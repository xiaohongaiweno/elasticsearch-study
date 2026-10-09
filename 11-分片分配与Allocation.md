# 11 · 分片分配与 Allocation

> 目标：搞懂「分片为什么在这个节点上」「为什么一个分片一直是红的」「怎么把它弄回去」。
>
> **为什么必须学**：`red`/`yellow` 状态、`unassigned_shards`、`disk watermark`、
> `shard allocation filtering`——这些日常运维的核心问题全部在这一章。
>
> ⚠️ 这是**ES 里最难啃的一章**（决策链很长），但也是**收益最高的一章**。

---

## 1. 全景图

```
【触发】节点上下线、索引创建/删除、分片启停、settings 变更、reroute API
                    │
                    ▼
┌───────────────────────────────────────────────────────────────────────────────┐
│  AllocationService（cluster/routing/allocation/AllocationService.java:66）     │
│                                                                               │
│   reroute(clusterState, reason)                        (:360/:369)            │
│     ├─ adaptAutoExpandReplicas                         (:243)  auto_expand    │
│     ├─ RoutingNodes（可变视图）                          (:372/:453)           │
│     ├─ new RoutingAllocation(deciders, nodes, state, clusterInfo, nanoTime)   │
│     └─ reroute(allocation)                             (:402)  ★ 三步走        │
│          ├─ ① gatewayAllocator.allocateUnassigned(allocation)   (:410)        │
│          │      ★ 处理 UNASSIGNED 分片：选节点 + 确定恢复来源                   │
│          ├─ ② shardsAllocator.allocate(allocation)              (:413)        │
│          │      ★ BalancedShardsAllocator：先 assign 再 balance                │
│          └─ ③ buildResultAndLogHealthChange → 新 ClusterState                  │
└───────────────────────────────────────────────────────────────────────────────┘
                    │
        ┌───────────┴────────────┐
        ▼                        ▼
┌───────────────────┐   ┌──────────────────────────────────────────────┐
│ GatewayAllocator  │   │ BalancedShardsAllocator                      │
│ gateway/          │   │ allocator/                                   │
│  ├ PrimaryShard   │   │  ├ allocateUnassigned()  → 分配未分配         │
│  │   Allocator    │   │  ├ moveShards()          → 搬迁（因过滤规则）  │
│  └ ReplicaShard   │   │  └ balance()             → 均衡（因倾斜）      │
│      Allocator    │   │                                              │
│ ★ 决定"从哪恢复"   │   │ ★ 决定"放哪个节点"                            │
│   和"哪个节点有数据"│   │   （通过 AllocationDeciders 逐一询问）        │
└───────────────────┘   └──────────────────────────────────────────────┘
```

**关键文件清单**

| 文件 | 路径 |
| --- | --- |
| `AllocationService` | `server/src/main/java/org/elasticsearch/cluster/routing/allocation/AllocationService.java` |
| `RoutingAllocation` | 同目录 |
| `RoutingNodes` | 同目录 |
| `AllocationDeciders` / `AllocationDecider` / `Decision` | 同目录 `decider/` |
| `BalancedShardsAllocator` | 同目录 `allocator/` |
| `GatewayAllocator` | `server/src/main/java/org/elasticsearch/gateway/GatewayAllocator.java` |
| `PrimaryShardAllocator` / `ReplicaShardAllocator` | 同目录 |
| `DiskThresholdSettings` / `DiskThresholdDecider` / `DiskThresholdMonitor` | `cluster/routing/allocation/` 与 `decider/` |
| `AllocationCommand` 家族 | 同目录 `command/` |
| `ClusterModule`（decider 注册） | `server/src/main/java/org/elasticsearch/cluster/ClusterModule.java:221` |

---

## 2. `AllocationService`：分配的总入口

### 2.1 字段（`:69-72`）

```java
private static final Logger logger = LogManager.getLogger(AllocationService.class);   // :67

private final AllocationDeciders allocationDeciders;                                  // :69
private GatewayAllocator gatewayAllocator;                                            // :70  ★ setter 注入（:88）
private final ShardsAllocator shardsAllocator;                                        // :71
private final ClusterInfoService clusterInfoService;                                  // :72  ★ 磁盘使用率来源
```

**★ `shardsAllocator` 是可插拔的**：`cluster.routing.allocation.type`（默认 `balanced`）。
见 `ClusterModule.SHARDS_ALLOCATOR_TYPE_SETTING`（`:97`）与 `createShardsAllocator`（`:256`）。

**★ `clusterInfoService` 的作用**：提供**节点磁盘使用率**（`ClusterInfo`），
是 `DiskThresholdDecider` 的数据来源。默认每 30s 采集一次（`cluster.info.update.interval`）。

### 2.2 五个公开入口（★ 排障时的"触发点"）

| 方法 | 行号 | 触发场景 |
| --- | --- | --- |
| `applyStartedShards(state, startedShards)` | `:98` | 分片启动成功（见 18 章） |
| `applyFailedShard(state, failedShard, markAsStale)` | `:152` | 单个分片失败（写入失败、恢复失败） |
| `applyFailedShards(state, failedShards, ...)` | `:169` | 批量分片失败 |
| `deassociateDeadNodes(state, reroute, reason)` | `:219` | ★ **节点掉线**（`NodesFaultDetection` → 见 10 章） |
| `reroute(state, commands, explain, retryFailed)` | `:329` | ★ **`POST /_cluster/reroute` API** |
| `reroute(state, reason)` | `:360` | 通用入口 |

### 2.3 ★ `reroute` 的"三步走"（`:402`）

```java
private void reroute(RoutingAllocation allocation) {
    // ★ 断言：dead node 必须先清理干净（否则会有不一致）
    assert hasDeadNodes(allocation) == false : "dead nodes should be explicitly cleaned up. See deassociateDeadNodes";
    // ★ 断言：auto_expand_replicas 必须与节点数一致
    assert AutoExpandReplicas.getAutoExpandReplicaChanges(allocation.metaData(), allocation.nodes()).isEmpty() :
        "auto-expand replicas out of sync with number of nodes in the cluster";

    // ★① 处理所有 UNASSIGNED 分片
    if (allocation.routingNodes().unassigned().size() > 0) {
        removeDelayMarkers(allocation);                    // :409  清理 delayed 标记
        gatewayAllocator.allocateUnassigned(allocation);   // :410  ★ 从上到下：primary 先，再 replica
    }

    // ★② BalancedShardsAllocator（内部三步：allocateUnassigned → moveShards → balance）
    shardsAllocator.allocate(allocation);                  // :413

    // ★③ 一致性断言
    assert RoutingNodes.assertShardStats(allocation.routingNodes());   // :414
}
```

**★ 顺序很关键**：
1. **GatewayAllocator 先跑**——因为**主分片必须先分配**，副本才有意义。
2. **BalancedShardsAllocator 后跑**——它处理"分片放哪更均衡"，也会分配剩余的。

### 2.4 ★ `reroute(state, reason, debug)`（`:369`）——入口的细节

```java
protected ClusterState reroute(ClusterState clusterState, String reason, boolean debug) {
    // ★ ① 先处理 auto_expand_replicas（可能会改 MetaData！）
    ClusterState fixedClusterState = adaptAutoExpandReplicas(clusterState);          // :370

    // ★ ② 构建可变的 RoutingNodes
    RoutingNodes routingNodes = getMutableRoutingNodes(fixedClusterState);           // :372
    // ★ ③ 打乱未分配分片顺序，避免"毒丸分片"总是排在前面导致其他分片分不出去
    routingNodes.unassigned().shuffle();                                             // :374

    // ★ ④ 构建 RoutingAllocation（含 ClusterInfo = 磁盘使用率）
    RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, routingNodes, fixedClusterState,
        clusterInfoService.getClusterInfo(), currentNanoTime());                     // :375
    allocation.debugDecision(debug);
    reroute(allocation);

    // ★ ⑤ 什么都没变 → 返回原状态（避免无意义的版本递增，见 09 章）
    if (fixedClusterState == clusterState && allocation.routingNodesChanged() == false) {
        return clusterState;
    }
    return buildResultAndLogHealthChange(clusterState, allocation, reason);
}
```

**★ `:374` 的 shuffle 是一个很妙的工程细节**：
如果不打乱，每次都从第一个 unassigned 分片开始尝试，
万一它因为某种原因（比如 `max_retries` 用尽）永远无法分配，
就会浪费时间反复算它；打乱后可以给其他分片机会。

**★ `:376` 的 `currentNanoTime()` 是可覆写的**（`:459` 注释：`override this to control time based decisions`）。
所以测试里可以"伪造时间流逝"，来验证 `delayed_timeout` 的行为。

### 2.5 ★ `deassociateDeadNodes`（`:417`）——节点掉线时做什么

```java
private void deassociateDeadNodes(RoutingAllocation allocation) {
    for (Iterator<RoutingNode> it = allocation.routingNodes().mutableIterator(); it.hasNext(); ) {
        RoutingNode node = it.next();
        if (allocation.nodes().getDataNodes().containsKey(node.nodeId())) {
            continue;        // 存活的节点，跳过
        }
        // ★ 该节点已经不在集群里了 → 把它的所有分片标记为未分配
        for (ShardRouting shardRouting : node.copyShards()) {
            final IndexMetaData indexMetaData = allocation.metaData().getIndexSafe(shardRouting.index());
            boolean delayed = INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.get(indexMetaData.getSettings()).nanos() > 0;
            UnassignedInfo unassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.NODE_LEFT,
                "node_left[" + node.nodeId() + "]", null, 0, allocation.getCurrentNanoTime(),
                System.currentTimeMillis(), delayed, AllocationStatus.NO_ATTEMPT);
            allocation.routingNodes().failShard(logger, shardRouting, unassignedInfo, indexMetaData, allocation.changes());
        }
        // ★ 注意注释：必须在 failShard 之后再 remove
        // since it relies on the fact that the RoutingNode exists in the list of nodes
        it.remove();
    }
}
```

**★ 两个关键点**：

1. **`delayed` 的计算**：来自 `INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING`
   （`cluster/routing/UnassignedInfo.java:58`）：
   ```java
   new Setting<>("index.unassigned.node_left.delayed_timeout", (s) -> TimeValue.timeValueMinutes(1).getStringRep(), ...)
   ```
   **默认 1 分钟**。所以节点掉线后，分片**不会立刻**重新分配，而是等 1 分钟
   （给节点"快速重启"的机会，避免不必要的数据拷贝）。

2. **顺序不可交换**：`failShard` 依赖 `RoutingNode` 还在列表里。

**★ 排障：`index.unassigned.node_left.delayed_timeout` 的两种极端**

| 值 | 效果 | 适用 |
| --- | --- | --- |
| `0` | ★ **立刻**重新分配 | 节点不会快速重启；或希望尽快恢复 |
| `1m`（默认） | 等 1 分钟 | 一般场景 |
| `30m` / `1h` | 等很久 | 长时间维护（可以避免大量数据搬迁） |

**★ 这个设置是"动态的 index 级设置"**：
```json
PUT /myindex/_settings
{ "index.unassigned.node_left.delayed_timeout": "5m" }
```

**★ 排障技巧**：如果你**确定**掉线的节点不会回来了，
把它设为 `0` 可以让分片**立刻**开始重新分配，加速恢复。

**★ `_cluster/allocation/explain` 里的 `delayed` 字段就是它**：
```json
"unassigned_info": { "reason": "NODE_LEFT", "delayed": true, ... }
```
`delayed: true` 时说明"分片其实可以分配，只是在等 timeout"。

### 2.6 `adaptAutoExpandReplicas`（`:243`）与 `_shrink` 的 `removeDelayMarkers`

```java
private ClusterState adaptAutoExpandReplicas(ClusterState clusterState) {
    final Map<String, List<String>> autoExpandReplicaChanges =
        AutoExpandReplicas.getAutoExpandReplicaChanges(clusterState.metaData(), clusterState.nodes());
    // ★ 如果 auto_expand_replicas 的目标副本数与节点数不匹配 → 改 MetaData
    if (autoExpandReplicaChanges.isEmpty()) {
        return clusterState;
    } else {
        final MetaData.Builder mdBuilder = MetaData.builder(clusterState.metaData());
        final RoutingTable.Builder rtBuilder = RoutingTable.builder(clusterState.routingTable());
        for (Map.Entry<String, List<String>> entry : autoExpandReplicaChanges.entrySet()) {
            final String index = entry.getKey();
            final int numberOfReplicas = getNumberOfReplicas(index, autoExpandReplicaChanges);
            logger.info("[{}] auto-expanding replicas from [{}] to [{}]", index, ...);     // ★ 你看到的日志
            ...
        }
    }
}
```

**★ `index.auto_expand_replicas: 0-2`** 的含义：
副本数在 0~2 之间自动调整，使 `(数据节点数 - 1)` 落在范围内。
例如 3 个数据节点 → 2 个副本；1 个数据节点 → 0 个副本。

**★ 你会看到这条日志**：
```
[INFO][o.e.c.r.a.AllocationService] [node1] [myindex] auto-expanding replicas from [1] to [2] based on [auto_expand_replicas:0-all] and number of nodes [3]
```

**★ 云服务注意**：`auto_expand_replicas` 是"自动扩容"的利器，
但**缩容时也会自动减副本**——这点容易被忽略（客户以为副本数固定，结果缩容后副本数变了）。

`removeDelayMarkers`（`:277`）的作用：如果某索引的 `delayed_timeout` 被改成 `0`，
就把已有的 `UnassignedInfo.delayed` 标记清掉，让分片立刻可分配。

---

## 3. ★ `AllocationDeciders`：决策的唯一入口

### 3.1 三种决策结果（`decider/Decision.java:44`）

```java
public static final Decision ALWAYS = new Single(Type.YES);       // :44
public static final Decision YES = new Single(Type.YES);          // :45
public static final Decision NO = new Single(Type.NO);            // :46
public static final Decision THROTTLE = new Single(Type.THROTTLE); // :47

public enum Type implements Writeable {
    YES(1),        // ★ 可以分配
    THROTTLE(2),   // ★ "可以，但现在太忙，等等再说" —— 不会永久阻止，只是本次跳过
    NO(0);         // ★ 不可以
}
```

**★ `THROTTLE` 是 ES 的一个精妙设计**：
它表示"**不是不行，而是现在不行**"。
所以 `_cluster/allocation/explain` 对 `THROTTLE` 的展示与 `NO` 不同——
`THROTTLE` 不会导致"分配失败计数"增加。

**★ `Decision` 的语义**（`decider/Decision.java:84` 的 `Type` README）：

```
Type.NO        → 不能分配
Type.THROTTLE  → 可以，但要等（比如恢复并发数已达上限）
Type.YES       → 可以
```

`Decision` 还携带 `explanation`（用于 explain API）：

```java
Decision.single(Type.YES, "shard is not allocated yet")
Decision.single(Type.NO, "no space left", allocations)
```

### 3.2 ★ 16 个内置 Decider（`ClusterModule.java:225-240`）

**注册顺序就是决策顺序**（`LinkedHashMap`，`:224`）：

| # | Decider | 行号 | 作用 | 关键配置 |
| --- | --- | --- | --- | --- |
| 1 | `MaxRetryAllocationDecider` | `:225` | ★ 分配失败次数超限则拒绝 | `index.allocation.max_retries`（默认 5） |
| 2 | `ResizeAllocationDecider` | `:226` | `_shrink`/`_split` 的特殊规则 | |
| 3 | `ReplicaAfterPrimaryActiveAllocationDecider` | `:227` | ★ **主分片没 active 时不分配副本** | |
| 4 | `RebalanceOnlyWhenActiveAllocationDecider` | `:228` | 只在 active 时才 rebalance | |
| 5 | `ClusterRebalanceAllocationDecider` | `:229` | ★ 何时允许 rebalance | `cluster.routing.allocation.allow_rebalance`（默认 `indices_all_active`） |
| 6 | `ConcurrentRebalanceAllocationDecider` | `:230` | ★ 并发 rebalance 数 | `cluster.routing.allocation.cluster_concurrent_rebalance`（默认 2） |
| 7 | `EnableAllocationDecider` | `:231` | ★ 总开关（enable/rebalance） | `cluster.routing.allocation.enable` / `cluster.routing.rebalance.enable` |
| 8 | `NodeVersionAllocationDecider` | `:232` | ★ 滚动升级时避免"降级"分配 | |
| 9 | `SnapshotInProgressAllocationDecider` | `:233` | 快照进行中的分片不移走 | |
| 10 | `RestoreInProgressAllocationDecider` | `:234` | 恢复进行中不移走 | |
| 11 | `FilterAllocationDecider` | `:235` | ★★ **`require`/`include`/`exclude` 过滤** | `index.routing.allocation.*` |
| 12 | `SameShardAllocationDecider` | `:236` | ★ 同一分片的不同副本不在同一节点 | `cluster.routing.allocation.same_shard.host`（默认 false） |
| 13 | `DiskThresholdDecider` | `:237` | ★★ **磁盘水位** | `cluster.routing.allocation.disk.*` |
| 14 | `ThrottlingAllocationDecider` | `:238` | ★★ **恢复并发数限流** | `cluster.routing.allocation.node_concurrent_recoveries`（默认 **2**）<br>`cluster.routing.allocation.node_initial_primaries_recoveries`（默认 **4**） |
| 15 | `ShardsLimitAllocationDecider` | `:239` | ★ 每节点分片数上限 | `cluster.routing.allocation.total_shards_per_node`（默认 -1）<br>`index.routing.allocation.total_shards_per_node` |
| 16 | `AwarenessAllocationDecider` | `:240` | ★★ **机架/可用区感知** | `cluster.routing.allocation.awareness.attributes`<br>`cluster.routing.allocation.awareness.force.*.values` |

**★ 决策结果的计算**（`decider/AllocationDeciders.java`）：

```java
@Override
public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
    return decideAll(shardRouting, node, allocation);          // 遍历所有 decider，取"最坏"结果
}
```

**规则**：只要有一个 decider 返回 `NO` → 结果就是 `NO`；
否则有 `THROTTLE` → `THROTTLE`；否则 `YES`。
**explanation 会聚合所有 decider 的理由**——这就是 explain API 的答案来源。

**★ 加解密（重要）**：
- **`FilterAllocationDecider` 在 `DiskThresholdDecider` 之前**，所以"过滤规则不匹配"会**先**被报出来
- **`DiskThresholdDecider` 在 `ThrottlingAllocationDecider` 之前**，所以"磁盘满了"优先于"太忙"

### 3.3 ★★ `FilterAllocationDecider`：过滤规则（热冷架构的核心）

**三组 + 一组初始恢复**（`IndexMetaData` 的 `AffixSetting`，见 08 章 4.1）：

| 设置 | 语义 | 优先级 |
| --- | --- | --- |
| `index.routing.allocation.require.<attr>` | ★ **硬性要求**（必须全部满足） | 最高（不可被覆盖） |
| `index.routing.allocation.include.<attr>` | 包含其一即可（OR） | 中 |
| `index.routing.allocation.exclude.<attr>` | **不允许**（不能有任何匹配） | 低 |
| `index.routing.allocation.initial_recovery._id` | ★ 只在**初始恢复**时生效（`_shrink` 用） | — |
| `cluster.routing.allocation.exclude.<attr>` | 集群级 exclude | — |

**真实案例（热冷架构）**：

```json
// 1) 节点上打标签
node.attr.box_type: hot      # 在 elasticsearch.yml 里
node.attr.box_type: warm
node.attr.box_type: cold

// 2) 新索引放热节点
PUT /hot-index/_settings
{ "index.routing.allocation.require.box_type": "hot" }

// 3) 用 ILM 或手工迁移到冷节点
PUT /hot-index/_settings
{ "index.routing.allocation.require.box_type": "cold" }
```

**★ 关键：改 `require` 会触发分片迁移**（`moveShards`）。
所以**云服务上"改 allocation 设置"是一个重操作**，会引发大量数据拷贝。

**★ `require` 与 `include` 的差别**（高频面试题）：

| | `require` | `include` |
| --- | --- | --- |
| 语义 | **必须**满足（AND，所有 `require.*` 都要满足） | 满足其一即可（OR） |
| 例子 | 同时 `require.box_type: hot` + `require.rack: r1` → 两个都要满足 | `include.rack: r1,r2` → 在 r1 或 r2 都行 |
| 不可分配时的行为 | **分片会留在原地**（不会移动），新分片**无法分配** | 同 |

**★ `_name` 与 `_ip` 特殊占位符**：

```json
{ "index.routing.allocation.include._name": "node1,node2" }
{ "index.routing.allocation.exclude._ip": "10.0.0.5" }
```

**★ 排障：改了 require 但分片不动？**
1. 检查目标节点是否真的有该属性：`GET /_cat/nodeattrs?v`
2. 检查是否有冲突的 `exclude`（`exclude` 优先？不，**`require` 必须满足，`exclude` 也不能匹配**）
3. 检查 `cluster.routing.allocation.enable` 是否为 `all`

### 3.4 ★★ `DiskThresholdDecider`：磁盘水位（最常见的原因之一）

**配置（`DiskThresholdSettings.java:39-60`）**：

| 设置 | 默认 | 语义 |
| --- | --- | --- |
| `cluster.routing.allocation.disk.threshold_enabled` | **true** | 总开关 |
| `cluster.routing.allocation.disk.watermark.low` | **85%** | ★ **低水位**：超过后**不再分配新分片**到该节点 |
| `cluster.routing.allocation.disk.watermark.high` | **90%** | ★ **高水位**：超过后**尝试搬迁**分片离开 |
| `cluster.routing.allocation.disk.watermark.flood_stage` | **95%** | ★★ **洪水位**：给索引打 `index.blocks.read_only_allow_delete` block！ |
| `cluster.routing.allocation.disk.include_relocations` | true | 计算时是否把"正在搬入的分片"算进使用量 |
| — | 30s | `cluster.info.update.interval`（`ClusterInfoService` 采集间隔） |

**★ 水位可以是百分比或绝对大小**：

```yaml
# 百分比（相对磁盘容量）
cluster.routing.allocation.disk.watermark.low: 85%
# 绝对大小（必须 >= 0，表示"剩余空间"）
cluster.routing.allocation.disk.watermark.low: 50gb
```

**★ 注意语义差异（一个经典坑）**：

| 形式 | 语义 |
| --- | --- |
| `"85%"` | **已用** 85%（超过就触发） |
| `"50gb"` | **剩余** 50gb（低于就触发） |

**★ `flood_stage`（95%）的连锁反应**（★ 必须知道）：

```
磁盘 > 95% → DiskThresholdMonitor 给所有索引加 index.blocks.read_only_allow_delete
          → 所有写入被拒！
          → 客户端看到：
             cluster_block_exception: blocked by: [FORBIDDEN/12/index read-only / allow delete (api)]
```

**★★ 排障黄金三步**：

```bash
# 1) 看水位状态
GET /_cat/allocation?v&h=node,disk.used_percent,disk.avail,shards

# 2) 看哪些索引被设了 read-only block
GET /_cat/indices?v&h=index,status,pri,rep&s=index
GET /*/_settings?filter_path=**.blocks.read_only_allow_delete&flat_settings=true

# 3) 清理磁盘后解除 block（★ 必须手工解除！ES 不会自动解除）
PUT /_all/_settings
{ "index.blocks.read_only_allow_delete": null }
```

**★ 关键认知**：`flood_stage` 触发的 read-only block **不会自动解除**。
清理完磁盘后**必须手工 `PUT .../_settings` 解除**。这是运维事故高发点。

**★ 云服务的处理方式**：
- 把 `flood_stage` 调高（如 98%）以留更多缓冲
- 或者监控磁盘并在 90% 时告警
- **关闭 `threshold_enabled` 是不可取的**（会写爆磁盘导致节点挂）

### 3.5 ★ `ThrottlingAllocationDecider`：恢复限流

| 设置 | 默认 | 语义 |
| --- | --- | --- |
| `cluster.routing.allocation.node_concurrent_recoveries` | **2** | 每节点**同时进行的副本恢复**数 |
| `cluster.routing.allocation.node_initial_primaries_recoveries` | **4** | 每节点**同时进行的初始主分片恢复**数 |

**★ 为什么要有这个限制？**

恢复分片要读磁盘、读网络（peer recovery）、写磁盘。
**不限制会导致 IO 饱和，把正常查询也拖死。**

**★ 排障：`unassigned_shards` 一直不降但节点有空间？**

```json
GET /_cluster/allocation/explain
{
  "unassigned_info": { "reason": "NODE_LEFT" },
  "node_allocation_decisions": [
    { "node_name": "node2", "decisions": [
        { "decider": "throttling", "decision": "THROTTLE",
          "explanation": "too many shards are currently recovering on this node ([2]/[2])" }
    ]}
  ]
}
```

**★ 看到 `THROTTLE` 说明"只是慢，不是错"**。

**★ 调优**：SSD + 万兆网可以调到 `4`~`8`；
但 `node_initial_primaries_recoveries: 4` **不建议随意调大**（可能会在集群恢复时把节点打爆）。

### 3.6 `AwarenessAllocationDecider`：感知机架/可用区

```yaml
# 1) 节点上打 zone 标签
node.attr.zone: az-a     # node1, node2
node.attr.zone: az-b     # node3, node4

# 2) 声明"感知"这个属性
cluster.routing.allocation.awareness.attributes: zone
```

**效果**：ES 会**尽量**让同一分片的副本分布在不同的 `zone`。

**★ `force` 变体（更强）**：

```yaml
cluster.routing.allocation.awareness.force.zone.values: az-a,az-b
```

**效果**：**强制**副本必须分布在 `az-a` 和 `az-b`（如果 `az-b` 挂了，`az-a` 里的副本**不会**被分配新副本，
从而避免"所有副本都在一个 AZ"的风险）。

**★ 云服务推荐**：
- 单 AZ 部署：**不用** `awareness`（没意义）
- 多 AZ 部署：**必须用**，`attributes: zone`（或 `rack_id`）
- 跨区（Region）：**用 `force`**，但要注意跨区带宽成本

**★ 排障**：`"decider": "awareness", "decision": "NO", "explanation": "there are already the maximum number of allowed shards/bytes on this node for this shard ID"`

### 3.7 `SameShardAllocationDecider`

| 设置 | 默认 | 语义 |
| --- | --- | --- |
| `cluster.routing.allocation.same_shard.host` | **false** | 是否禁止"同一分片的不同副本在同一物理主机上" |

**用途**：**多实例部署在同一台物理机**时使用（如一台机器跑 3 个 ES 实例）。
此时 **必须**设为 `true`，否则同一分片的副本可能都在这台机器上，
机器一挂就全丢。

**★ 判定方式**：比较 `hostname` 和 `hostAddress`。

### 3.8 `MaxRetryAllocationDecider`

| 设置 | 默认 | 语义 |
| --- | --- | --- |
| `index.allocation.max_retries` | **5** | 分片分配失败重试次数上限 |

**★ 超过后**：分片**永远不会被自动分配**，需要手工 `POST /_cluster/reroute?retry_failed=true`。

**★ 排障**：

```json
GET /_cluster/allocation/explain
{
  "unassigned_info": { "reason": "ALLOCATION_FAILED", "failure_count": 5 },
  "can_allocate": "no",
  "allocate_explanation": "shard has exceeded the maximum number of retries [5] on failed allocation attempts - manually call [/_cluster/reroute?retry_failed=true] to retry..."
}
```

**★★ 解法**：

```json
POST /_cluster/reroute?retry_failed=true
```

**★ 这是「分片明明有空间却分不出去」最常见的原因之一。**

### 3.9 `EnableAllocationDecider`

| 设置 | 默认值 | 可选值 | 语义 |
| --- | --- | --- | --- |
| `cluster.routing.allocation.enable` | `all` | `all`/`primaries`/`new_primaries`/`none` | ★ 是否允许分配 |
| `cluster.routing.rebalance.enable` | `all` | `all`/`primaries`/`replicas`/`none` | ★ 是否允许均衡 |
| `index.routing.allocation.enable` | — | 同上 | 索引级覆盖 |
| `index.routing.rebalance.enable` | — | 同上 | 索引级覆盖 |

**★ 这两个设置是运维的"刹车"**：

| 场景 | 设置 |
| --- | --- |
| **滚动重启** | `cluster.routing.allocation.enable: none`（停之前）/ `all`（起完之后） |
| **禁止所有分配** | `none` |
| **只允许分配主分片**（紧急恢复写入） | `primaries` |
| **只允许新索引的主分片** | `new_primaries` |
| **禁止均衡但允许分配** | `cluster.routing.rebalance.enable: none` |

**★ 云服务最佳实践（滚动重启）**：

```bash
# 1) 停止分片迁移
PUT /_cluster/settings
{ "transient": { "cluster.routing.allocation.enable": "none" } }

# 2) （可选）手动 flush 加速恢复
POST /_flush/synced

# 3) 重启节点 1（等它回来并恢复）
# 4) 恢复分配
PUT /_cluster/settings
{ "transient": { "cluster.routing.allocation.enable": "all" } }
```

**★ 注意**：`transient` 重启会丢（见 04 章），所以**一定要记得改回来**，
否则重启后 `enable` 回到 `all`，但你可能以为还是 `none`。

### 3.10 `ClusterRebalanceAllocationDecider` 与 `ConcurrentRebalanceAllocationDecider`

**`cluster.routing.allocation.allow_rebalance`**（默认 `indices_all_active`）：

| 值 | 语义 |
| --- | --- |
| `always` | ★ 总是允许 |
| `indices_primaries_active` | 所有**主分片** active 后允许 |
| `indices_all_active`（默认） | ★ 所有**分片（含副本）**active 后允许 |

**★ 默认值的含义**：集群刚从故障恢复时**不会**做 rebalance。
**为什么？** 因为恢复期间每分每秒都宝贵，不应该浪费带宽去搬迁。

**`cluster.routing.allocation.cluster_concurrent_rebalance`**（默认 **2**）：
全集群同时进行的 rebalance 数。`-1` 无限制。

**★ 排障**：为什么分片不均衡但不搬迁？
```json
GET /_cluster/allocation/explain
{ "decider": "cluster_rebalance", "decision": "NO",
  "explanation": "we have active shards only on some indices and all shards must be active, ..." }
```

### 3.11 `ShardsLimitAllocationDecider`

| 设置 | 默认 | 语义 |
| --- | --- | --- |
| `cluster.routing.allocation.total_shards_per_node` | `-1` | 全局：每节点分片数上限 |
| `index.routing.allocation.total_shards_per_node` | `-1` | 索引级 |

**★ 用途**：防止某索引把节点塞满（通常配合 `awareness` 使用）。

**★ 坑**：**设太小会导致分片无法分配**（`no space for shard` 之类的理由）。

---

## 4. `GatewayAllocator`：决定"从哪恢复"

文件：`server/src/main/java/org/elasticsearch/gateway/GatewayAllocator.java`

### 4.1 三大组件

```
GatewayAllocator
  ├── PrimaryShardAllocator     ★ 主分片：哪个节点有最新的数据？
  ├── ReplicaShardAllocator     ★ 副本：哪个节点已有数据（可以走 peer recovery）？
  └── AsyncShardFetch           ★ 异步收集各节点的分片信息（data path / allocation id）
```

**★ `AsyncShardFetch` 的用途**：向所有**有该分片数据的节点**并发问「你有哪些分片？
allocation id 是什么？」，然后把结果缓存起来。

**★ 这解释了 `GET /_cluster/allocation/explain` 里的 `fetch_in_progress` 字段**：
如果信息还没收集完，explain 会显示：
```json
{ "can_allocate": "fetch_in_progress", ... }
```

### 4.2 ★ 主分片分配：`PrimaryShardAllocator`

**核心问题**：一个未分配的主分片，哪个节点上的数据是"最新的"？

**判定方式**：

| 恢复类型 | 判定依据 |
| --- | --- |
| **`EXISTING_STORE`**（节点上有数据） | 比较各节点的 **`allocationId` 是否在 `inSyncAllocationIds` 里** + **`primaryTerm`** |
| **`PEER`**（从其他副本恢复） | 选一个 allocation id 也在 `inSyncAllocationIds` 里的**活跃副本** |
| **`EMPTY_STORE`**（全新） | 直接选节点（没有数据来源） |

**★ `inSyncAllocationIds` 与 `primaryTerm` 在这里登场**（见 08 章 4.5）：
这是 ES 保证"不会选到数据落后的节点当主分片"的机制。

**★ 你会看到的日志**：

```
[INFO][o.e.c.r.a.g.D.AsyncShardFetch] [node1][indices:admin/shards/...] ... 
[INFO][o.e.c.r.a.g.PrimaryShardAllocator] [node1] [myindex][0]: allocating primary shard with allocation id [...] with highest primary term [3]
[WARN][o.e.c.r.a.g.PrimaryShardAllocator] [node1] [myindex][0]: not allocating primary shard since no shard copy could be found...
```

**★ `_cluster/allocation/explain` 里最经典的两种主分片情况**：

| 情况 | explain 输出 |
| --- | --- |
| 有数据但不在 in-sync 里 | `allocation_status: "no_valid_shard_copy"` → **需要 `allocate_stale_primary` 强制分配（会丢数据！）** |
| 一个副本都没有 | `allocation_status: "no_valid_shard_copy"` + `has_valid_copy: false` |

### 4.3 ★ 副本分配：`ReplicaShardAllocator`

**核心问题**：副本该放哪？是否有节点已经有这个副本的数据（可以走 peer recovery 少拷数据）？

**判定顺序**：
1. 找一个**已有数据**的节点 → 走 `PEER` 恢复（快）
2. 没有的话，找一个**空的节点** → 走 `PEER` 恢复（慢，全量拷贝）

**★ 关键：副本恢复永远是 `PEER`**（从主分片或另一个副本拷数据）。

### 4.4 ★★ 关于"分配陈旧主分片"（`allocate_stale_primary`）

**场景**：主分片所在节点全挂了，只剩一个**数据不是最新**的副本。

**ES 的行为**：
- 默认**不会**自动分配（因为会丢数据）→ 分片一直是 `red`
- 必须**手工**执行：

```json
POST /_cluster/reroute
{
  "commands": [
    { "allocate_stale_primary": {
        "index": "myindex", "shard": 0, "node": "node2",
        "accept_data_loss": true
    }}
  ]
}
```

**★ 相关命令（`command/` 目录）**：

| 命令 | 类 | 用途 |
| --- | --- | --- |
| `allocate_empty_primary` | `AllocateEmptyPrimaryAllocationCommand` | ★ **放弃数据**，重新建一个空主分片 |
| `allocate_stale_primary` | `AllocateStalePrimaryAllocationCommand` | ★ **接受数据丢失**，用陈旧副本当主分片 |
| `allocate_replica` | `AllocateReplicaAllocationCommand` | 手工分配一个副本 |
| `cancel` | `CancelAllocationCommand` | 取消分片分配（`allow_primary` 才能取消主分片！） |
| `move` | `MoveAllocationCommand` | ★ 手工迁移分片 |

**★ `cancel` 的 `allow_primary`**：

```json
POST /_cluster/reroute
{ "commands": [ { "cancel": { "index": "myindex", "shard": 0, "node": "node2", "allow_primary": true } } ] }
```

不设 `allow_primary: true` 时，取消主分片会报错（防止误操作）。

**★ 排障黄金命令**：

```json
POST /_cluster/reroute?dry_run=true
{
  "commands": [
    { "allocate_stale_primary": { "index": "myindex", "shard": 0, "node": "node2", "accept_data_loss": true } }
  ]
}
```

`dry_run=true` 会**返回决策结果但不执行**——★ **强烈建议先 dry_run**。

---

## 5. ★ `BalancedShardsAllocator`：均衡分配

文件：`cluster/routing/allocation/allocator/BalancedShardsAllocator.java`

### 5.1 三个步骤（`:118` `allocate`）

```java
@Override
public void allocate(RoutingAllocation allocation) {
    if (allocation.routingNodes().size() == 0) {
        failAllocationOfNewPrimaries(allocation);       // ★ 没有任何数据节点 → 所有新主分片分配失败
        return;
    }
    final Balancer balancer = new Balancer(logger, allocation, weightFunction, threshold);
    balancer.allocateUnassigned();      // ★ ① 分配未分配的（unassigned → initializing）
    balancer.moveShards();              // ★ ② 搬迁（因过滤规则变化，必须移走的）
    balancer.balance();                 // ★ ③ 均衡（因倾斜而"想"移走的）
}
```

**★ 三者的区别（极重要）**：

| 步骤 | 触发原因 | 是否"必须" |
| --- | --- | --- |
| `allocateUnassigned` | 分片没有节点 | 必须 |
| `moveShards` | ★ **`canRemain` 返回 false**（如改了 `require`、磁盘水位超了）→ **必须搬** | 必须 |
| `balance` | ★ **`weight` 太高**（不均衡）→ **可以搬**（受 `threshold` 与 `rebalance.enable` 限制） | 可选 |

**★ 排障含义**：
- 「改了 `require.box_type` 后分片开始搬」→ `moveShards`（必须搬）
- 「集群没变化但分片在搬」→ `balance`（均衡）

### 5.2 ★★ `WeightFunction`：均衡的数学定义（`:191`）

**这是 ES 均衡算法的核心，务必理解。**

```java
public static class WeightFunction {
    private final float indexBalance;
    private final float shardBalance;
    private final float theta0;
    private final float theta1;

    public WeightFunction(float indexBalance, float shardBalance) {
        float sum = indexBalance + shardBalance;
        if (sum <= 0.0f) {
            throw new IllegalArgumentException("Balance factors must sum to a value > 0 but was: " + sum);
        }
        theta0 = shardBalance / sum;        // ★ 归一化：shard 权重占比
        theta1 = indexBalance / sum;        // ★ 归一化：index 权重占比
        this.indexBalance = indexBalance;
        this.shardBalance = shardBalance;
    }

    public float weight(Balancer balancer, ModelNode node, String index) {
        return weight(balancer, node, index, 0);
    }
    public float weightShardAdded(Balancer balancer, ModelNode node, String index) {
        return weight(balancer, node, index, 1);      // ★ 假设"再加一个分片"后的权重
    }
    public float weightShardRemoved(Balancer balancer, ModelNode node, String index) {
        return weight(balancer, node, index, -1);     // ★ 假设"减一个分片"后的权重
    }

    private float weight(Balancer balancer, ModelNode node, String index, int numAdditionalShards) {
        final float weightShard = node.numShards() + numAdditionalShards - balancer.avgShardsPerNode();
        final float weightIndex = node.numShards(index) + numAdditionalShards - balancer.avgShardsPerNode(index);
        return theta0 * weightShard + theta1 * weightIndex;
    }
}
```

**★ 公式（记住它）**：

```
weight(node, index) = θ0 * (node分片数 - 集群平均分片数)
                    + θ1 * (node上该索引分片数 - 该索引平均每节点分片数)

其中 θ0 = shardBalance / (indexBalance + shardBalance)   ← 默认 0.45/1.0 = 0.45
     θ1 = indexBalance / (indexBalance + shardBalance)   ← 默认 0.55/1.0 = 0.55
```

**★ 两个目标（这才是重点）**：

| 目标 | 数学项 | 含义 |
| --- | --- | --- |
| **集群级均衡** | `node.numShards() - avgShardsPerNode` | 每个节点的**总分片数**尽量相等 |
| **索引级均衡** | `node.numShards(index) - avgShardsPerNode(index)` | ★ **每个索引的分片**在节点间也尽量相等 |

**★ 为什么需要"索引级均衡"？**

假设 3 个节点，索引 A 有 3 个分片。
- 只看集群级：A 的 3 个分片可能全在 node1（如果 node1 恰好分片少）
- 加上索引级：A 的 3 个分片会分到 3 个不同节点

**★ 配置项**（`:85-90`）：

| 设置 | 默认 | 含义 |
| --- | --- | --- |
| `cluster.routing.allocation.balance.index` | **0.55** | ★ index balance 权重（越大越"打散同一个索引"） |
| `cluster.routing.allocation.balance.shard` | **0.45** | ★ shard balance 权重（越大越"总数量均衡"） |
| `cluster.routing.allocation.balance.threshold` | **1.0** | ★ **触发搬迁的最小收益**（越大越"懒得搬"） |

**★ 关于 `theta` 的一个"陷阱"**：

`theta0 + theta1 == 1`（归一化后）。
所以最终的 weight 是**加权平均**，量纲一致。

**★ 调优实战**：

| 需求 | 怎么调 |
| --- | --- |
| 减少搬迁（集群稳定） | 调大 `threshold`（如 `2.0`） |
| 更激进地均衡 | 调小 `threshold`（如 `0.5`） |
| 让同一索引的分片尽量分散 | 调大 `balance.index`（如 `0.7`）、调小 `balance.shard` |
| 只关心总数量均衡 | `balance.index: 0`、`balance.shard: 1` |

**★ 云服务经验**：**生产环境建议把 `threshold` 调大**（如 `2.0`~`3.0`）。
因为默认 `1.0` 会让集群为了"轻微不均衡"而搬数据，浪费带宽。
代价是均衡度略差——通常可以接受。

### 5.3 `Balancer` 的决策逻辑

| 方法 | 作用 |
| --- | --- |
| `allocateUnassigned()` | 遍历 unassigned，用 `decideAllocateUnassigned` 找节点 |
| `moveShards()` | 遍历所有分片，用 `decideMove` 找新节点（★ 只看 `canRemain`） |
| `balance()` | 用 `decideRebalance` 找"搬过去更好"的分片 |
| `decideAllocateUnassigned(shard, favoredNodes)` | → `AllocateUnassignedDecision` |
| `decideMove(shard)` | → `MoveDecision` |
| `decideRebalance(shard)` | → `MoveDecision` |

**★ "找最好的节点"的方式**：用**最小堆**（按 weight 排序）取 weight 最小的节点。
所以**"分片放到分片最少的节点上"**。

**★ `decideAllocateUnassigned` 的过程**：

```
for 候选节点（按 weight 从小到大排序）:
    decision = allocation.deciders().canAllocate(shard, node, allocation)
    ├─ YES     → 选它！（但记下 decision，用于 explain）
    └─ NO/THROTTLE → 尝试下一个
如果没有节点可以 → 生成 AllocateUnassignedDecision.no(...) 并带上**每个节点的理由**
```

**★ 这就是 explain API 的数据来源**：
```json
"node_allocation_decisions": [
  { "node_name": "node1", "node_decision": "no", "deciders": [ {...} ] },
  { "node_name": "node2", "node_decision": "no", "deciders": [ {...} ] }
]
```
★ **`explain` 会列出"最有可能成功的 3 个节点"的详细理由**（`explain` 时 `MAX_NODES` 限制）。

### 5.4 `ModelNode` / `ModelIndex`

```
Balancer 内部把 RoutingNodes 转成 Model：
  ModelNode   ← 一个数据节点 + 它上面的分片
  ModelIndex  ← 一个索引 + 它的分片在哪些节点
```

**★ 为什么要"模型化"？** 因为 balance 是**迭代优化**：
每次尝试搬一个分片（`weightShardAdded`/`weightShardRemoved`），
需要在"假设状态"下计算权重，所以要用可变模型。

---

## 6. `RoutingAllocation` 与 `RoutingNodes`

### 6.1 `RoutingAllocation`（`cluster/routing/allocation/RoutingAllocation.java`）

| 字段/方法 | 作用 |
| --- | --- |
| `routingNodes()` | 可变的分片视图 |
| `metaData()` | 元数据 |
| `nodes()` | `DiscoveryNodes` |
| `deciders()` | `AllocationDeciders` |
| `changes()` | ★ `RoutingChangesObserver`（记录变更，用于日志与通知） |
| `clusterInfo()` | ★ 磁盘使用率 |
| `getCurrentNanoTime()` | 时间（可覆写，便于测试） |
| `debugDecision(boolean)` | 是否记录详细决策（`_cluster/allocation/explain?include_yes_decisions=true`） |
| `isDebugDecision()` | |
| `hasPendingAsyncFetch()` | 是否有异步 fetch 未完成 |
| `routingNodesChanged()` | ★ 有没有变化（决定是否发布新状态） |
| `ignoreDisable()` | ★ 绕过 `cluster.routing.allocation.enable: none`（`reroute` API 用） |

**★ `ignoreDisable()` 的语义非常重要**：
**手工 `POST /_cluster/reroute` 会绕过 `allocation.enable: none`**。
这是"紧急手工分配"能工作的原因，也是"我设了 none 但手工 reroute 还是生效了"的原因。

### 6.2 `RoutingNodes`

★★ **`RoutingNodes` 是 `RoutingTable` 的"节点视角"重新组织**（见 08 章 2.5）。

| 方法/字段 | 作用 |
| --- | --- |
| `unassigned()` | ★ `UnassignedShards`（所有未分配分片，带 `shuffle()`） |
| `getNode(nodeId)` / `getNodes()` | 按节点取/遍历 |
| `node(nodeId)` | `RoutingNode` |
| `initializeShard(...)` | 分配：`UNASSIGNED → INITIALIZING` |
| `startShard(...)` | 启动：`INITIALIZING → STARTED` |
| `failShard(...)` | 失败：`→ UNASSIGNED`（并设 `UnassignedInfo`） |
| `relocateShard(...)` | 迁移：`STARTED → RELOCATING` |
| `cancelRelocation(...)` | 取消迁移 |
| `moveShard(...)` | 移动（`relocating` + `initializeTargetRelocatingShard`） |
| `assignShardToNode(...)` | |
| `mutableIterator()` | ★ 可删除的迭代器（`deassociateDeadNodes` 用） |
| `assertShardStats(nodes)` | ★ 一致性断言（统计各状态分片数是否自洽） |

**★ `assertShardStats` 是 ES 的一个"自检"**：
每次 reroute 后都会断言分片统计自洽（`.unassigned + initializing + started + relocating` 对得上）。
**如果这个断言失败，说明有 bug**（通常是插件改了路由表但没走正规 API）。

**★ 排障**：日志里出现
```
java.lang.AssertionError: shard stats mismatch ...
```
说明路由表被破坏了，需要重启节点恢复。

### 6.3 `RoutingChangesObserver`（`cluster/routing/allocation/RoutingChangesObserver.java`）

用于记录分配过程中的**每一次变更**，最终产出日志：

```
[INFO][o.e.c.r.a.AllocationService] [node1] Cluster health status changed from [YELLOW] to [GREEN] (reason: [shards started [[myindex][0]] ...]).
```

| 方法 | 触发 |
| --- | --- |
| `shardStarted(ShardRouting)` | 分片启动 |
| `shardFailed(ShardRouting, UnassignedInfo)` | 分片失败 |
| `shardInitialized(...)` | 分片被分配 |
| `shardAssigned(...)` | 分片被指派节点 |
| `relocationStarted(...)` / `relocationCompleted(...)` | 迁移开始/结束 |
| `shardsUpdated(Collection<ShardRouting>)` | 批量更新 |

**★ 你看到的那些 INFO 日志**：

```
[myindex][0] delivered as primary to node2, s[2], node[node2], [P], v[3], s[STARTED], a[id=xxx]
[myindex][0] moved to node3, s[3], node[node3], [P], v[4], s[RELOCATING], a[id=xxx], relocating [node2], [R], v[4], a[id=yyy]
[myindex][0] can't move to node3, ... 
```

**这些都是 `RoutingNodes` 的内部类打的**（`toString` 格式化）。
**`s[2]` 里的数字是"第几次 shuffle"**，`v[3]` 是分片版本。

---

## 7. ★ `_cluster/allocation/explain`：排障的终极武器

### 7.1 基本用法

```bash
# 解释当前第一个未分配分片
GET /_cluster/allocation/explain

# 解释指定分片
GET /_cluster/allocation/explain
{ "index": "myindex", "shard": 0, "primary": true }

# 包含"是"的决策（用于分析为什么没落到别的节点）
GET /_cluster/allocation/explain?include_yes_decisions=true

# 更详细（包含所有节点的决策）
GET /_cluster/allocation/explain?include_disk_info=true
```

**源码**：`action/admin/cluster/allocation/`（`TransportClusterAllocationExplainAction`）
+ `cluster/routing/allocation/explain/`（`AllocateUnassignedDecision` 等）。

### 7.2 ★ 完整响应解读

```json
{
  "index": "myindex",
  "shard": 0,
  "primary": true,
  "current_state": "unassigned",
  "unassigned_info": {
    "reason": "NODE_LEFT",                                  // ★ 见 08 章 5.6 的 Reason 枚举
    "at": "2026-01-01T00:00:00.000Z",
    "last_allocation_status": "no_attempt",
    "can_allocate": "no",                                   // ★ 能否分配
    "allocate_explanation": "cannot allocate because allocation is not permitted to any of the nodes",
    "node_allocation_decisions": [
      {
        "node_id": "node2id",
        "node_name": "node2",
        "transport_address": "10.0.0.2:9300",
        "node_attributes": { "box_type": "hot", "zone": "az-a" },   // ★ 节点属性
        "node_decision": "no",
        "weight_ranking": 1,                                         // ★ weight 排名（越小越好）
        "deciders": [
          {
            "decider": "disk_threshold",                              // ★ 哪个 decider 拒绝的
            "decision": "NO",
            "explanation": "the node is above the high watermark cluster setting [cluster.routing.allocation.disk.watermark.high=90%], using more disk space than the maximum allowed [90.0%], actual free: [5.2%]"
          }
        ]
      }
    ]
  },
  "can_remain_on_current_node": "no",
  "can_rebalance_cluster": "no",
  "can_rebalance_to_other_node": "no",
  "rebalance_explanation": "cannot rebalance as no target node exists that can both allocate this shard and improve the cluster balance"
}
```

### 7.3 ★★ 决策结果速查表

| `can_allocate` / `node_decision` | 含义 | 常见原因 |
| --- | --- | --- |
| `yes` | 可以分配 | — |
| `no` | 不可以 | 见 `deciders` 里的理由 |
| `throttle` | ★ 现在不行，等着 | 恢复并发数已满 |
| `fetch_in_progress` | ★ 信息还在收集中 | `AsyncShardFetch` 未完成 |
| `no_valid_shard_copy` | ★★ **没有任何节点有可用的数据副本** | 节点全丢 / 数据不在 in-sync 里 |
| `no_attempt` | 还没尝试分配过 | 正常（新建索引） |

### 7.4 ★★ 按 decider 名的排障对照表（★ 背下来）

| `decider` | 典型 `explanation` | 处理 |
| --- | --- | --- |
| `disk_threshold` | `the node is above the low watermark ... [85%]` | 清磁盘 / 调水位 / 换节点 |
| `disk_threshold` | `the node is above the high watermark ... [90%]` | 同上 |
| `throttling` | `too many shards are currently recovering on this node ([2]/[2])` | ★ **正常，等着**；或调大 `node_concurrent_recoveries` |
| `filter` | `node does not match index setting [index.routing.allocation.require.box_type] filters [box_type:"cold"]` | 检查节点属性 `GET /_cat/nodeattrs?v` |
| `filter` | `node does not match any of the required filter [...]` | 同上 |
| `same_shard` | `a copy of this shard is already allocated to this node` | ★ **正常**（副本本来就不该在同一节点） |
| `awareness` | `there are already the maximum number of allowed shards on this node for this shard ID` | 检查 `awareness.attributes` 与 `force` |
| `max_retry` | `shard has exceeded the maximum number of retries [5]` | ★ **`POST /_cluster/reroute?retry_failed=true`** |
| `enable` | `no allocations are allowed due to [cluster.routing.allocation.enable:none]` | 改回 `all` |
| `replica_after_primary_active` | `primary shard is not active yet` | ★ **正常**（主分片先恢复） |
| `node_version` | `cannot allocate replica shard to a node with version [...] since this is older than the primary version` | 滚动升级期间正常 |
| `snapshot_in_progress` / `restore_in_progress` | — | 等快照/恢复完成 |
| `rebalance_only_when_active` | — | 等所有分片 active（rebalance 场景） |
| `shards_limit` | `too many shards allocated on this node` | 检查 `total_shards_per_node` |
| `cluster_rebalance` | `we have active shards only on some indices...` | 正常；或改 `allow_rebalance: always` |
| `concurrent_rebalance` | `too many shards are currently relocating` | 等；或调大 `cluster_concurrent_rebalance` |
| `resize` | `shrink/split ...` | `_shrink`/`_split` 专属规则 |

**★★ 排障第一原则**：
**先跑 `GET /_cluster/allocation/explain`，按上面的表查 `decider` 名。**
90% 的分配问题能直接定位。

### 7.5 `_cluster/allocation/explain` 的 `weight_ranking`

`weight_ranking: 1` 表示该节点是**按 weight 排序后第 1 名**（最该放分片的节点）。
如果第 1 名因为磁盘满被拒，第 2 名可能可以——**explain 会按 ranking 列出所有节点**。

---

## 8. ★ 完整案例集

### 案例 1：新建索引后一直 yellow

```
原因：索引有 1 个副本，但只有 1 个数据节点。
分配器：ReplicaShardAllocationDecider? 不，是 SameShardAllocationDecider + 没有其他节点
explain 输出：
  "decider": "same_shard",
  "explanation": "a copy of this shard is already allocated to this node [[myindex][0], node[node1], [P], s[STARTED]]"
处理：
  - 加数据节点，或
  - PUT /myindex/_settings { "index.number_of_replicas": 0 }
```

### 案例 2：磁盘满导致 read-only

```
现象：写入报 cluster_block_exception: blocked by: [FORBIDDEN/12/index read-only / allow delete (api)]
原因：磁盘 > 95% → flood_stage → DiskThresholdMonitor 加 block
处理：
  1) GET /_cat/allocation?v  确认磁盘
  2) 删数据 / 加磁盘
  3) ★ PUT /_all/_settings { "index.blocks.read_only_allow_delete": null }
```

### 案例 3：改了 `require.box_type` 但分片不动

```
处理：
  1) GET /_cat/nodeattrs?v | grep box_type        → 目标节点有该属性吗？
  2) GET /_cluster/allocation/explain?include_yes_decisions=true  → 看 decider
  3) 检查 cluster.routing.allocation.enable 是否为 none
  4) 检查 cluster.routing.rebalance.enable（moveShards 不受它限制，但 balance 受）
  5) 检查目标节点磁盘水位
```

### 案例 4：`max_retries` 用尽

```
explain: "shard has exceeded the maximum number of retries [5] on failed allocation attempts
         - manually call [/_cluster/reroute?retry_failed=true] to retry"
处理：
  POST /_cluster/reroute?retry_failed=true
  或 PUT /myindex/_settings { "index.allocation.max_retries": 10 }
  ★ 但先要找到"为什么反复失败"的根因（别光重试）
```

### 案例 5：主分片全丢，只剩陈旧副本

```
explain: "allocation_status": "no_valid_shard_copy", "has_valid_copy": false
处理（会丢数据！）：
  # 先 dry_run
  POST /_cluster/reroute?dry_run=true
  { "commands": [ { "allocate_stale_primary": {
      "index": "myindex", "shard": 0, "node": "node2", "accept_data_loss": true } } ] }

  # 确认后执行
  POST /_cluster/reroute
  { "commands": [ { "allocate_stale_primary": {
      "index": "myindex", "shard": 0, "node": "node2", "accept_data_loss": true } } ] }
```

### 案例 6：`delayed` 导致分片不重分配

```
explain: "unassigned_info": { "reason": "NODE_LEFT", "delayed": true }
处理：
  # 确认节点不会回来了 → 立刻重分配
  PUT /_all/_settings { "index.unassigned.node_left.delayed_timeout": "0s" }
  # 或只对某个索引
  PUT /myindex/_settings { "index.unassigned.node_left.delayed_timeout": "0s" }
```

### 案例 7：集群缩容后分片不均衡

```
原因：balance 是"最好的努力"（best effort），受 threshold 和 rebalance.enable 限制
处理：
  1) 检查 cluster.routing.allocation.allow_rebalance（默认 indices_all_active，等所有分片 active）
  2) 检查 cluster.routing.rebalance.enable 是否 none
  3) 手工搬：POST /_cluster/reroute { "commands": [ { "move": {...} } ] }
  4) 或调小 balance.threshold 让它更积极
```

---

## 9. 动手实验

**实验 11-1：观察分配全过程**
单节点，创建 1 主 1 副的索引：
```bash
curl -XPUT 'localhost:9200/exp11' -H 'Content-Type: application/json' \
  -d '{"settings":{"number_of_shards":2,"number_of_replicas":1}}'
curl -s 'localhost:9200/_cluster/allocation/explain?pretty'
```
观察 `same_shard` decider 的解释。

**实验 11-2：观察 auto_expand_replicas**
```bash
curl -XPUT 'localhost:9200/exp11b' -H 'Content-Type: application/json' \
  -d '{"settings":{"number_of_shards":1,"number_of_replicas":0,"auto_expand_replicas":"0-all"}}'
# 启动第 2 个节点，观察日志里的 auto-expanding replicas
```

**实验 11-3：★ 制造磁盘水位触发**
```bash
# 把水位调得很低（实验环境）
PUT /_cluster/settings
{ "transient": {
    "cluster.routing.allocation.disk.watermark.low": "1%",
    "cluster.routing.allocation.disk.watermark.high": "2%",
    "cluster.routing.allocation.disk.watermark.flood_stage": "3%"
}}
# 创建索引 → 观察各种 decider 拒绝
GET /_cluster/allocation/explain?include_disk_info=true&pretty
# ★ 记得改回来！
```

**实验 11-4：观察 delayed_timeout**
```bash
curl -XPUT 'localhost:9200/exp11c' -H 'Content-Type: application/json' -d '{"settings":{"number_of_replicas":"1"}}'
# kill -9 掉一个数据节点
curl -s 'localhost:9200/_cluster/allocation/explain?pretty' | grep -E 'delayed|reason'
# 观察 delayed: true，等 1 分钟后变 false
```

**实验 11-5：观察 moveShards vs balance**
```bash
# 给节点打标签
# node1: node.attr.box_type: a   node2: node.attr.box_type: b
PUT /exp11c/_settings { "index.routing.allocation.require.box_type": "b" }
# ★ 观察分片全部搬到 node2（moveShards）
# 然后去掉 require
POST /_cluster/reroute { "commands": [ { "move": { "index":"exp11c","shard":0,"from_node":"node2","to_node":"node1" } } ] }
# 观察它是否又被 balance 搬回来（取决于 weight）
```

**实验 11-6：★ 观察 weight 与 threshold**
```bash
# 调大 threshold 让它"懒得搬"
PUT /_cluster/settings
{ "persistent": { "cluster.routing.allocation.balance.threshold": 10.0 } }
# 观察不均衡的集群是否停止搬迁
```

**实验 11-7：手工 reroute 绕过 enable=none**
```bash
PUT /_cluster/settings { "transient": { "cluster.routing.allocation.enable": "none" } }
# 创建索引 → 分片全 unassigned
# 手工分配（★ 会成功！）
POST /_cluster/reroute
{ "commands": [ { "allocate_replica": { "index":"exp11","shard":0,"node":"node2" } } ] }
```

**实验 11-8：观察 `max_retries`**
```bash
# 用一个无法满足的 require 强制分配失败
PUT /exp11/_settings { "index.routing.allocation.require.nonexistent_attr": "x" }
PUT /exp11/_settings { "index.allocation.max_retries": 2 }
# 观察 explain 的 failure_count 增长，最后报 max_retry
```

**实验 11-9：看 RoutingChangesObserver 的日志**
开 `logger.org.elasticsearch.cluster.routing.allocation: DEBUG`，
做一次 `move`，观察详细的 `s[N]`, `v[N]`, `a[id=xxx]` 日志。

---

## 10. 自测题

1. `AllocationService.reroute` 的"三步走"是哪三步？顺序能换吗？
2. `GatewayAllocator` 与 `BalancedShardsAllocator` 的分工？
3. `RoutingTable` 与 `RoutingNodes` 的区别？谁在分配时被修改？
4. `RoutingAllocation.ignoreDisable()` 的语义？谁会用？
5. `Decision` 的三种类型？`THROTTLE` 与 `NO` 的区别？
6. 16 个 decider 里，哪些是"硬性拒绝"、哪些是"临时等待"？
7. `require` 与 `include` 的语义差别？`exclude` 与它们的关系？
8. 磁盘水位的三个阈值各是什么？`flood_stage` 触发后会怎样？会**自动**解除吗？
9. `index.unassigned.node_left.delayed_timeout` 的作用？设 `0` 会怎样？
10. `index.allocation.max_retries` 用尽后的现象与解法？
11. `BalanceAllocator` 的 `allocateUnassigned` / `moveShards` / `balance` 三者区别？
12. `WeightFunction` 的公式？`indexBalance` 与 `shardBalance` 各控制什么？为什么还需要"索引级均衡"？
13. `cluster.routing.allocation.balance.threshold` 调大/调小分别有什么效果？
14. `index.auto_expand_replicas` 的行为？缩容时会怎样？
15. `deassociateDeadNodes` 里为什么要先 `failShard` 再 `it.remove()`？
16. `_cluster/allocation/explain` 里 `no_valid_shard_copy` 与 `no_attempt` 的区别？
17. `allocate_stale_primary` 与 `allocate_empty_primary` 的区别？各自的风险？
18. `GET /_cluster/allocation/explain` 里 `weight_ranking` 是什么意思？
19. 手工 `POST /_cluster/reroute` 会受 `cluster.routing.allocation.enable: none` 限制吗？
20. 滚动重启时应该怎么设置 `allocation.enable`？为什么用 `transient` 有风险？

---

## 11. 关键类速查

| 类 | 路径 | 一句话 |
| --- | --- | --- |
| `AllocationService` | `cluster/routing/allocation/AllocationService.java:66` | ★ 分配总入口（`:402` reroute） |
| `RoutingAllocation` | 同目录 | 一次分配过程的上下文 |
| `RoutingNodes` | 同目录 | ★ 可变的节点视角视图 |
| `RoutingChangesObserver` | 同目录 | 变更记录（日志来源） |
| `FailedShard` / `StaleShard` | 同目录 | 失败/陈旧分片描述 |
| `AllocationDecision` / `MoveDecision` / `AllocateUnassignedDecision` / `ShardAllocationDecision` | 同目录 | 决策结果 |
| `NodeAllocationResult` / `RoutingExplanations` / `RerouteExplanation` | 同目录 | explain 的数据结构 |
| `AllocationDeciders` | 同目录 `decider/` | ★ 所有 decider 的聚合 |
| `AllocationDecider` | 同上 | 决策接口 |
| `Decision` | 同上 | YES / NO / THROTTLE |
| `FilterAllocationDecider` | 同上 | ★ require/include/exclude |
| `DiskThresholdDecider` / `DiskThresholdSettings` / `DiskThresholdMonitor` | 同上 / 上级 | ★ 磁盘水位 |
| `ThrottlingAllocationDecider` | 同上 | ★ 恢复限流 |
| `EnableAllocationDecider` | 同上 | ★ 分配/均衡总开关 |
| `AwarenessAllocationDecider` | 同上 | ★ 机架/可用区感知 |
| `SameShardAllocationDecider` | 同上 | 副本不共物理机 |
| `ShardsLimitAllocationDecider` | 同上 | 每节点分片上限 |
| `MaxRetryAllocationDecider` | 同上 | 重试上限 |
| `ClusterRebalanceAllocationDecider` / `ConcurrentRebalanceAllocationDecider` | 同上 | rebalance 时机与并发 |
| `BalancedShardsAllocator` | 同目录 `allocator/` | ★ 均衡分配（`:191` WeightFunction） |
| `BalancedShardsAllocator.WeightFunction` | 同上 | ★ 权重公式 |
| `ShardsAllocator` | 同上 | 可插拔接口 |
| `GatewayAllocator` | `gateway/GatewayAllocator.java` | ★ 未分配分片的分配与恢复来源 |
| `PrimaryShardAllocator` / `ReplicaShardAllocator` | `gateway/` | ★ 主/副本的恢复来源判定 |
| `AsyncShardFetch` | `gateway/` | 异步收集节点分片信息 |
| `ClusterModule` | `cluster/ClusterModule.java:221` | ★ decider 注册顺序 |
| `AllocationCommand` 家族 | `cluster/routing/allocation/command/` | ★ reroute 命令 |

---

**上一篇**：[10-ZenDiscovery选主与节点发现.md](./10-ZenDiscovery选主与节点发现.md)　
**下一篇**：[12-索引管理与创建流程.md](./12-索引管理与创建流程.md)
