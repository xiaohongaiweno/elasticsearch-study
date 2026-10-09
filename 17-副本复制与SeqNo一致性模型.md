# 17 · 副本复制与 SeqNo 一致性模型

> 目标：把 **seq_no / primary_term / local checkpoint / global checkpoint / in-sync 集合**
> 这一整套 6.x 的复制一致性模型彻底讲透，并能在源码里逐行指出它们在哪里被计算、在哪里被推进、在哪里被持久化。
>
> **为什么必须学**：
> - 「主分片挂了，副本被提升，为什么没丢数据？」→ global checkpoint + primary term + resync。
> - 「为什么副本一直追不上（`global_checkpoint` 卡住不动）？」→ computeGlobalCheckpoint + pendingInSync。
> - 「为什么 `if_seq_no` 能替代 `version` 做乐观锁？」→ `seq_no` 是**全局单调的写序号**，不是文档级计数器。
> - 「为什么 translog 一直不删？」→ translog 只能 trim 到 global checkpoint 之前（第 14 章）。
> - 「为什么重启后副本要全量重传而不是增量？」→ 副本本地历史超出了 primary 的 history（msu 机制）。
> - 云服务常见的「副本落后」「分片卡在 recovery」「主副本数据不一致」全部要用本章的模型去定位。
>
> **前置**：第 13 章（写入链路）、第 14 章（Translog）、第 15 章（Engine）。
> **后续**：第 18 章 `18-分片恢复Recovery.md`。

---

## 1. 先建立直觉：三个数字与两个集合

```
                            ┌─────────────────────────────────────────────┐
                            │  写入顺序（主分片分配）                       │
                            │  seq_no: 0  1  2  3  4  5  6  7  8 ...       │
                            └─────────────────────────────────────────────┘
   Primary (P)              ✔  ✔  ✔  ✔  ✔  ✔  ✔  ✔  ✔
                                                    ↑ max_seq_no = 8
                                              ↑ local checkpoint = 6（7 还在并发写）
   Replica1 (R1)            ✔  ✔  ✔  ✔  ✔  ✔  ✗  ✔  ✗
                                        ↑ local checkpoint = 5
   Replica2 (R2)            ✔  ✔  ✔  ✔  ✔  ✔  ✔  ✗  ✗
                                              ↑ local checkpoint = 6

   global checkpoint = min(所有 in-sync 副本的 local checkpoint) = 5
```

**一句话总纲**：

| 概念 | 谁维护 | 含义 | 单调性 |
| --- | --- | --- | --- |
| `_seq_no` | 主分片写入时分配 | 该分片上的**写操作序号**（每个分片独立一套） | 主分片上严格 +1 递增 |
| `_primary_term` | 集群状态（master 递增） | **主分片代次**，每发生一次主分片切换 +1 | 只增不减 |
| `local checkpoint` | **每个分片副本自己算** | 本副本上「**所有 ≤ 该序号的操作都已处理**」的最高 seq_no | 单调递增 |
| `global checkpoint` | **主分片算，广播给副本** | 所有 in-sync 副本都处理完的最高 seq_no（= min(local ckpt)） | 单调递增 |
| `max_seq_no` | 每个分片自己算 | 已分配出去（可能还没处理完）的最大 seq_no | 单调递增 |
| `in-sync 集合` | master 维护，持久化在 `IndexMetaData` | 一组 allocationId：这些副本「保证持有全部已确认写入」 | 只会减少 / 重新加回 |

**核心不变式（背下来）**：

```
global_checkpoint ≤ min(所有 in-sync 副本的 local_checkpoint) ≤ local_checkpoint ≤ max_seq_no
```

- 只有 **≤ global checkpoint** 的操作才允许被 translog trim / 允许被 recovery 跳过。
- 只有 **in-sync 集合里的副本** 才有可能被选为新的主分片（第 11 章 `PrimaryShardAllocator`）。

---

## 2. 必读源码清单

| 类 | 路径 | 作用 |
| --- | --- | --- |
| `SequenceNumbers` | `index/seqno/SequenceNumbers.java` | ★ 常量定义（`-1/-2/-3`）与 min/max 工具 |
| `LocalCheckpointTracker` | `index/seqno/LocalCheckpointTracker.java` | ★ 单副本的 seqno 分配器 + bitmap 式 local checkpoint |
| `ReplicationTracker` | `index/seqno/ReplicationTracker.java` | ★★ 本章核心：global checkpoint、in-sync、primary/replica 模式 |
| `GlobalCheckpointSyncAction` | `index/seqno/GlobalCheckpointSyncAction.java` | 后台把 global checkpoint 同步给落后副本 |
| `SeqNoStats` | `index/seqno/SeqNoStats.java` | `(max_seq_no, local_checkpoint, global_checkpoint)` 三元组，Stats API 输出 |
| `ReplicationGroup` | `index/shard/ReplicationGroup.java` | 一次复制的「目标集合」计算 |
| `ReplicationOperation` | `action/support/replication/ReplicationOperation.java` | ★ 主 → 副本 的编排（先主后副、计数、失败处理） |
| `TransportReplicationAction` | `action/support/replication/TransportReplicationAction.java` | 复制框架中的 primary/replica transport handler |
| `TransportWriteAction` | `action/support/replication/TransportWriteAction.java` | 写类复制的公共逻辑（msu、失败副本、stale） |
| `PrimaryReplicaSyncer` | `index/shard/PrimaryReplicaSyncer.java` | 新主上任后对副本做的「补发 + 截断」 |
| `TransportResyncReplicationAction` | `action/resync/TransportResyncReplicationAction.java` | resync 的 transport 实现 |
| `GlobalCheckpointListeners` | `index/shard/GlobalCheckpointListeners.java` | 等待 global checkpoint 推进（CCR / `wait_for` 类语义） |
| `IndexShard` | `index/shard/IndexShard.java` | 上述一切的宿主（`:1864~2894`） |
| `IndexMetaData` | `cluster/metadata/IndexMetaData.java` | `in-sync allocation ids`（`:513`）与 `primaryTerm` 的持久化处 |

---

## 3. `SequenceNumbers`：先把常量记住

源码位置 `server/src/main/java/org/elasticsearch/index/seqno/SequenceNumbers.java`：

```java
public static final long PRE_60_NODE_CHECKPOINT = -3L;   // :34  5.x 节点的 checkpoint 占位
public static final long UNASSIGNED_SEQ_NO      = -2L;   // :38  尚未分配（如主分片执行前的 op）
public static final long NO_OPS_PERFORMED       = -1L;   // :42  初始值：一个操作都没做过
public static final long UNASSIGNED_PRIMARY_TERM = 0L;   // :47  未分配的主分片代次
```

**为什么这几个值重要**：它们频繁出现在断言里，而且**参与大小比较**（`-2 < -1 < 0` 都成立），
所以很多"取 min/max"的工具必须特殊处理它们。这就是 `SequenceNumbers.min`（`:85`）/`max`（`:109`）存在的原因：

```java
public static long min(final long minSeqNo, final long seqNo) {
    if (minSeqNo == NO_OPS_PERFORMED) return seqNo;       // 当前还是"没做过操作"，直接用新值
    else if (minSeqNo == UNASSIGNED_SEQ_NO) return seqNo; // 当前还是"未分配"，直接用新值
    else {
        if (seqNo == UNASSIGNED_SEQ_NO) throw new IllegalArgumentException("sequence number must be assigned");
        return Math.min(minSeqNo, seqNo);
    }
}
```

另外 `loadSeqNoInfoFromLuceneCommit`（`:55`）说明了一个关键事实：

> **`max_seq_no` 和 `local_checkpoint` 会被写进 Lucene 的 commit 数据里**（key = `max_seq_no` / `local_checkpoint`）。
> 所以分片重启后，从 `segments_N` 里就能恢复出「我曾经写到哪」，而不用扫 translog。

---

## 4. `LocalCheckpointTracker`：单副本的 seqno 分配与 local checkpoint

路径：`server/src/main/java/org/elasticsearch/index/seqno/LocalCheckpointTracker.java`

### 4.1 它要解决的问题

并发写入时，seq_no 是**先分配、后完成**的：线程 A 拿到 seq_no=7，线程 B 拿到 8，B 可能先写完。
所以「最大 seq_no」不等于「已完成的最大 seq_no」。我们需要一个结构来回答：

> 当前「**从 0 开始连续完成**」到哪了？

### 4.2 bitmap + 惰性清理

```29:41:index/seqno/LocalCheckpointTracker.java
```

```java
static final short BIT_SET_SIZE = 1024;                       // :35  每个 bitset 管 1024 个 seqno
final LongObjectHashMap<CountedBitSet> processedSeqNo = ...;  // :41  key = seqNo / 1024
volatile long checkpoint;                                     // :46  当前 local checkpoint
private volatile long nextSeqNo;                              // :51  下一个可分配的 seqno
```

- `generateSeqNo()`（`:80`）：`synchronized` 返回 `nextSeqNo++`。**分配是串行的，但完成是并发的**。
- `markSeqNoAsCompleted(seqNo)`（`:89`）：把对应 bit 置 1；若 `seqNo == checkpoint + 1`，触发 `updateCheckpoint()`。
- `updateCheckpoint()`（`:172`）：从 `checkpoint+1` 开始，**逐位向后推进**，直到遇到未完成的 bit；
  跨过一整个 bitset（1024 位）后把该 bitset 从 map 里删掉 —— 这就是注释里说的 "cleaning up while completed"，
  保证内存占用只与「在途操作数」相关，而不是与总写入量相关。
- `waitForOpsToComplete(seqNo)`（`:141`）：`while (checkpoint < seqNo) this.wait();`，被 `updateCheckpoint` 的 `notifyAll` 唤醒。

**一个容易踩的细节**（`:94`）：

```java
if (seqNo <= checkpoint) {
    // this is possible during recovery where we might replay an operation that was also replicated
    return;
}
```

恢复期间可能重放一个"已经被复制过"的操作，此时它的 seq_no 已经 ≤ checkpoint，直接忽略即可。

**一句话记忆**：`LocalCheckpointTracker` = **全局 seqno 发号器 + 一个"连续完成水位"bitmap**。

---

## 5. `ReplicationTracker`：整个一致性模型的心脏

路径：`server/src/main/java/org/elasticsearch/index/seqno/ReplicationTracker.java`（48KB，971 行，6.6.1 里最关键的类之一）

### 5.1 两种模式

```70:88:index/seqno/ReplicationTracker.java
```

```java
volatile boolean primaryMode;   // :88  true = 我是主分片，由我计算 global checkpoint
```

类注释（`:70-87`）明确写了三种「切到 primary mode」的时机：

1. **初始化中的主分片（非 relocation target）变 active** → `activatePrimaryMode`（`:509`）
2. **active 副本被提升为主** → 同样 `activatePrimaryMode`（在 `IndexShard.java:522`）
3. **主分片 relocation 的 target 在 handoff 期间** → `activateWithPrimaryContext`（`:827`）

> 注意：**任何分片刚初始化时都是 replica mode**（`primaryMode = false`，`:407`）。
> 也就是说副本分片**从不自己计算** global checkpoint，它只"被告知"。

### 5.2 核心数据结构：`checkpoints`

```124:131:index/seqno/ReplicationTracker.java
```

```java
final Map<String, CheckpointState> checkpoints;   // :129  allocationId -> 该副本的 checkpoint 状态
final Set<String> pendingInSync;                  // :142  正在"追赶 global checkpoint"的副本
volatile ReplicationGroup replicationGroup;       // :147  缓存的复制组
```

`CheckpointState`（`:149`）四个字段，务必分清楚：

| 字段 | 含义 |
| --- | --- |
| `localCheckpoint` | **该副本自己**报告上来的 local checkpoint |
| `globalCheckpoint` | **主分片对该副本的"已知 global checkpoint"**（用于判断副本是否落后） |
| `inSync` | 是否在 in-sync 集合里 → **是否参与 global checkpoint 计算** |
| `tracked` | 是否在复制组里 → **是否接收主分片发来的写操作** |

> `inSync ⇒ tracked`（不变式，`:362`），反之不然：
> 一个正在 recovery 的副本是 `tracked=true`（开始收增量写）但 `inSync=false`（还不拖慢 global checkpoint）。

### 5.3 `invariant()`：读这个类的"说明书"

```288:372:index/seqno/ReplicationTracker.java
```

`ReplicationTracker` 的每一个 public 方法**首尾都调用 `assert invariant()`**。
这是阅读这个类最快的方式 —— 它把所有不变式用断言写死了。摘录几条最重要的：

```java
// 副本模式下，别人（其它副本）的 checkpoint 信息必须是"未赋值"
assert primaryMode || checkpoints.values().stream()
    .allMatch(cps -> cps.localCheckpoint == UNASSIGNED_SEQ_NO || cps.localCheckpoint == PRE_60_NODE_CHECKPOINT);   // :293

// relocation handoff 只能在 primary 模式下发生，且期间 pendingInSync 必须为空
assert !handoffInProgress || primaryMode;                          // :309
assert !handoffInProgress || pendingInSync.isEmpty();              // :327

// primary 模式下，global checkpoint 必须恰好等于"重算一遍"的结果（不能是脏的缓存）
assert !primaryMode || getGlobalCheckpoint() == computeGlobalCheckpoint(pendingInSync, checkpoints.values(), getGlobalCheckpoint());  // :334

// global checkpoint 不允许超过任何 in-sync 副本的 local checkpoint
assert !primaryMode || getGlobalCheckpoint() <= inSyncCheckpointStates(checkpoints, CheckpointState::getLocalCheckpoint, LongStream::min);  // :340

// 路由表里已分配的副本，必须在 checkpoints 里有条目
assert routingTable == null || checkpoints.keySet().containsAll(routingTable.getAllAllocationIds());   // :354
```

**排障价值**：线上如果开了 `-ea`（测试集群/单元测试），任何对 `ReplicationTracker` 的非法状态变更都会立刻 AssertionError，
而不是等到数据不一致才暴露。这也是为什么 ES 的一致性 bug 大多在单元测试阶段就被抓出来。

### 5.4 global checkpoint 到底怎么算：`computeGlobalCheckpoint`

```720:740:index/seqno/ReplicationTracker.java
```

```java
private static long computeGlobalCheckpoint(final Set<String> pendingInSync,
                                            final Collection<CheckpointState> localCheckpoints,
                                            final long fallback) {
    long minLocalCheckpoint = Long.MAX_VALUE;
    if (pendingInSync.isEmpty() == false) {
        return fallback;                                    // ★ 有副本正在"追赶" → 冻结，不许推进
    }
    for (final CheckpointState cps : localCheckpoints) {
        if (cps.inSync) {                                   // ★ 只看 in-sync 集合
            if (cps.localCheckpoint == UNASSIGNED_SEQ_NO) {
                return fallback;                            // 有 in-sync 副本还没上报过 → 冻结
            } else if (cps.localCheckpoint == PRE_60_NODE_CHECKPOINT) {
                // 5.x 副本：忽略（不参与计算，混合版本集群里 global checkpoint 会退化）
            } else {
                minLocalCheckpoint = Math.min(cps.localCheckpoint, minLocalCheckpoint);
            }
        }
    }
    return minLocalCheckpoint;
}
```

三个结论：

1. **global checkpoint = in-sync 副本 local checkpoint 的最小值**。
2. 只要有副本在 `pendingInSync` 里，**global checkpoint 直接冻结**（返回 fallback = 旧值）。
3. **混合 5.x/6.x 集群**里，5.x 副本被忽略 → 6.x 副本们自己算最小值，这实际上**削弱了持久性保证**（所以升级要尽快完成）。

调用点是 `updateGlobalCheckpointOnPrimary`（`:745`）：

```java
private synchronized void updateGlobalCheckpointOnPrimary() {
    assert primaryMode;
    final CheckpointState cps = checkpoints.get(shardAllocationId);
    final long globalCheckpoint = cps.globalCheckpoint;
    final long computedGlobalCheckpoint = computeGlobalCheckpoint(pendingInSync, checkpoints.values(), getGlobalCheckpoint());
    assert computedGlobalCheckpoint >= globalCheckpoint : "...";      // ★ 只允许前进
    if (globalCheckpoint != computedGlobalCheckpoint) {
        cps.globalCheckpoint = computedGlobalCheckpoint;
        onGlobalCheckpointUpdated.accept(computedGlobalCheckpoint);   // ★ 回调 → IndexShard 的 listeners
    }
}
```

**什么时候会触发重算**？全类只有 4 处调用：

| 触发点 | 行号 | 场景 |
| --- | --- | --- |
| `activatePrimaryMode` | `:518` | 成为主分片 |
| `updateFromMaster` 且删掉了条目 | `:588` | master 把某个副本判死，从 in-sync 移除 → **global checkpoint 可能大步前进** |
| `markAllocationIdAsInSync` | `:659` | 新副本进入 in-sync |
| `updateLocalCheckpoint` | `:711` | 副本上报了新的 local checkpoint（**复制响应回来时**） |

**这就是"副本慢会拖慢 global checkpoint"的源码根源**：`updateLocalCheckpoint` 只在副本**上报更高值**时才可能推进。

### 5.5 `updateLocalCheckpoint`：复制响应回来后做了什么

```691:714:index/seqno/ReplicationTracker.java
```

```java
public synchronized void updateLocalCheckpoint(final String allocationId, final long localCheckpoint) {
    assert primaryMode;
    assert handoffInProgress == false;
    CheckpointState cps = checkpoints.get(allocationId);
    if (cps == null) {
        // 副本已从集群移除，但复制流程还不知道 → 忽略
        return;
    }
    boolean increasedLocalCheckpoint = updateLocalCheckpoint(allocationId, cps, localCheckpoint);
    boolean pending = pendingInSync.contains(allocationId);
    if (pending && cps.localCheckpoint >= getGlobalCheckpoint()) {
        pendingInSync.remove(allocationId);      // 追上了 → 移出 pending
        pending = false;
        cps.inSync = true;                       // 正式 in-sync
        replicationGroup = calculateReplicationGroup();
        notifyAllWaiters();                      // 唤醒 markAllocationIdAsInSync 里等待的线程
    }
    if (increasedLocalCheckpoint && pending == false) {
        updateGlobalCheckpointOnPrimary();       // ★ 只有"确实涨了且不在 pending"才重算
    }
}
```

配套的 `updateLocalCheckpoint(String, CheckpointState, long)`（`:665`）有一个很重要的保护：

```java
// a local checkpoint of PRE_60_NODE_CHECKPOINT cannot be overridden
assert cps.localCheckpoint != SequenceNumbers.PRE_60_NODE_CHECKPOINT || localCheckpoint == SequenceNumbers.PRE_60_NODE_CHECKPOINT;
```

### 5.6 `markAllocationIdAsInSync`：副本"转正"的阻塞点

```623:663:index/seqno/ReplicationTracker.java
```

recovery 完成、副本要被加进 in-sync 集合时调用（`IndexShard.java:1928`）：

```java
updateLocalCheckpoint(allocationId, cps, localCheckpoint);
if (cps.localCheckpoint < getGlobalCheckpoint()) {
    pendingInSync.add(allocationId);                 // 落后了 → 先挂起
    try {
        while (true) {
            if (pendingInSync.contains(allocationId)) {
                waitForLocalCheckpointToAdvance();   // synchronized wait()
            } else {
                break;
            }
        }
    } finally {
        pendingInSync.remove(allocationId);
    }
} else {
    cps.inSync = true;
    replicationGroup = calculateReplicationGroup();
    updateGlobalCheckpointOnPrimary();
}
```

**关键设计**：如果一个副本恢复完成时 local checkpoint **低于当前 global checkpoint**，
它会阻塞等待，直到追上为止，**期间 global checkpoint 被冻结**（因为 `pendingInSync` 非空）。

为什么要这样？因为 global checkpoint 的含义是"所有 in-sync 副本都有的最高 seq_no"。
如果允许一个"还没追上"的副本立刻 in-sync，那 global checkpoint 的含义就被破坏了 ——
后续 translog 会按这个错误的 checkpoint 去 trim，然后这个落后副本就永远补不上数据了。

> **线上现象**：一个副本卡的很久 → `global_checkpoint` 长时间不动 → translog 不 trim → 磁盘涨。
> 排障：`GET /_cat/shards?h=index,shard,prirep,sq,sqg` 对比各副本的 local/global checkpoint。

### 5.7 `updateFromMaster`：master 才是 in-sync 集合的最终权威

```530:594:index/seqno/ReplicationTracker.java
```

```java
if (applyingClusterStateVersion > appliedClusterStateVersion) {       // ★ 保证按序应用
    // primary 模式下，master 不能凭空造出新的 in-sync 条目
    assert !primaryMode || inSyncAllocationIds.stream()
        .allMatch(id -> checkpoints.containsKey(id) && checkpoints.get(id).inSync);   // :535

    // 删掉 master 那边已经不存在的条目
    boolean removedEntries = checkpoints.keySet().removeIf(
        aid -> !inSyncAllocationIds.contains(aid) && !initializingAllocationIds.contains(aid));   // :542

    if (primaryMode) {
        // 新增 initializing 副本（fresh copies，非 in-sync）
        for (String initializingId : initializingAllocationIds) { ... }   // :547
        if (removedEntries) {
            pendingInSync.removeIf(aId -> checkpoints.containsKey(aId) == false);
        }
    } else {
        // 副本模式：把 master 给的 in-sync 集合原样记下来
        for (String inSyncId : inSyncAllocationIds) { ... }   // :570
    }
    appliedClusterStateVersion = applyingClusterStateVersion;
    this.routingTable = routingTable;
    replicationGroup = calculateReplicationGroup();
    if (primaryMode && removedEntries) {
        updateGlobalCheckpointOnPrimary();      // ★ 副本被判死 → global checkpoint 可能前进
        notifyAllWaiters();
    }
}
```

`appliedClusterStateVersion`（`:120`）的存在理由写在注释 `:113-119`：

> 主分片 relocation handoff 之后，target 可能先应用一个**比 primary context 更旧**的集群状态。
> 如果允许这个旧状态影响 in-sync 集合，就可能**不安全地提前推进 global checkpoint**。
> 所以必须记录已应用的集群状态版本，保证 in-order。

调用链（谁把 master 的数据喂进来）：

```
IndicesClusterStateService.updateShard(...)          IndicesClusterStateService.java:567
  ├─ indexMetaData.primaryTerm(shardId.id())                              :576
  ├─ inSyncIds = indexMetaData.inSyncAllocationIds(shard.shardId().id())  :577
  └─ shard.updateShardState(shardRouting, primaryTerm,
         primaryReplicaSyncer::resync, clusterState.version(),
         inSyncIds, indexShardRoutingTable, pre60AllocationIds);          :592
                     ↓
IndexShard.updateShardState(...)                     IndexShard.java:410
  └─ if (newRouting.primary())
       replicationTracker.updateFromMaster(applyingClusterStateVersion, inSyncAllocationIds,
                                           routingTable, pre60AllocationIds);   :435
```

**持久化位置**：`IndexMetaData.inSyncAllocationIds`（`cluster/metadata/IndexMetaData.java:513`），
随 ClusterState 一起被 gateway 持久化（第 23 章）。**这就是 ES 6.x 能保证"不丢已确认写入"的根本所在**：
in-sync 集合是集群级的、持久化的、由 master 单点裁决的。

### 5.8 主分片 relocation handoff：三段式交接

这是 `ReplicationTracker` 里最绕的部分，但云服务里做迁移/扩缩容必看。

```
  源主分片 (source)                         目标分片 (target)
  ─────────────────────────────────────────────────────────────
  ① startRelocationHandoff()          :762
     handoffInProgress = true
     拷贝 {clusterStateVersion, checkpoints, routingTable}
     → PrimaryContext（可序列化）
  ──────────────── 通过 recovery 传输 PrimaryContext ───────────→
                                       ② activateWithPrimaryContext(ctx)   :827
                                          primaryMode = true
                                          checkpoints = 拷贝自 ctx
                                          appliedClusterStateVersion = ctx.clusterStateVersion
                                          updateGlobalCheckpointOnPrimary()
                                          runAfter.run()  // 重放可能漏掉的集群状态更新
  ←─────────────── handoff 成功 ────────────────────────────────
  ③ completeRelocationHandoff()       :795
     primaryMode = false;  relocated = true
     清空除"本分片 global checkpoint"外的所有 checkpoint 信息
```

为什么 handoff 期间禁止 `markAllocationIdAsInSync`（`:96` 的注释）？

> handoff 的目的是把 tracker 的全部内部状态转移给 target。
> 如果期间 in-sync 集合还能增长，target 就不知道这个新条目，从而**过早地推进 global checkpoint**。

而 `updateFromMaster` 是**允许**在 handoff 期间调用的（`:98`），因为 handoff 可能失败并 `abortRelocationHandoff`（`:784`），
此时源主分片不能错过期间发生的任何状态更新。

---

## 6. `ReplicationGroup`：一次复制要发给谁

路径：`server/src/main/java/org/elasticsearch/index/shard/ReplicationGroup.java`

```42:74:index/shard/ReplicationGroup.java
```

构造时把路由表里的分片分成三类：

| 集合 | 计算方式 | 用途 |
| --- | --- | --- |
| `unavailableInSyncShards` | `inSyncAllocationIds - routingTable.getAllAllocationIds()`（`:47`） | in-sync 里但在路由表已无分配 → 需要 **mark as stale** |
| `replicationTargets` | 已分配 且 `trackedAllocationIds.contains(allocationId)`（`:55`） | **真正要发写请求的目标**（含 relocation target，`:62-71`） |
| `skippedShards` | unassigned 或 initializing 但还没开 engine（`:51`、`:60`） | 只计入 `_shards.total`，不发请求 |

**为什么区分 `tracked` 和 `inSync`**：一个副本在 recovery 的 translog 阶段就已经 `tracked` 了（开始收增量写），
但还没 `inSync`。如果按 `inSync` 决定发不发，恢复期间的增量就丢了。

---

## 7. 一次写操作的完整复制编排：`ReplicationOperation`

路径：`server/src/main/java/org/elasticsearch/action/support/replication/ReplicationOperation.java`

### 7.1 `execute()`：顺序是灵魂

```90:129:action/support/replication/ReplicationOperation.java
```

```java
public void execute() throws Exception {
    final String activeShardCountFailure = checkActiveShardCount();        // ① wait_for_active_shards
    ...
    totalShards.incrementAndGet();
    pendingActions.incrementAndGet();          // ② 为"协调本身"占一个计数，防止过早结束
    primaryResult = primary.perform(request);  // ③ ★ 先在主分片上执行（分配 seq_no、写 Lucene+Translog）
    primary.updateLocalCheckpointForShard(primaryRouting.allocationId().getId(), primary.localCheckpoint());  // ④ 更新自己的 lcp
    final ReplicaRequest replicaRequest = primaryResult.replicaRequest();
    if (replicaRequest != null) {
        final long globalCheckpoint = primary.globalCheckpoint();                      // ⑤ ★ 先取 gcp
        final long maxSeqNoOfUpdatesOrDeletes = primary.maxSeqNoOfUpdatesOrDeletes();  // ⑥ ★ 再取 msu
        final ReplicationGroup replicationGroup = primary.getReplicationGroup();       // ⑦ ★ 最后取复制组
        markUnavailableShardsAsStale(replicaRequest, replicationGroup);                // ⑧
        performOnReplicas(replicaRequest, globalCheckpoint, maxSeqNoOfUpdatesOrDeletes, replicationGroup);  // ⑨
    }
    successfulShards.incrementAndGet();
    decPendingAndFinishIfNeeded();
}
```

**⑤⑥⑦ 的顺序有严格讲究**，源码注释（`:110-116`）逐条解释：

> - **必须在写主分片成功之后**才取复制组：这样能保证「恢复开始之后写进主分片的每个操作，都一定会被复制给恢复目标」。
> - **必须先取 global checkpoint，再取复制组**：保证这个 gcp 对这个复制组是有效的。
>   如果反过来取，gcp 可能是基于「复制组的**子集**」算出来的，会**比这个复制组实际允许的值更高**，
>   后果是某些副本会被告知一个**高于自己 local checkpoint 的 global checkpoint**（违反不变式）。

### 7.2 副本响应回来：`performOnReplica` 的 listener

```158:200:action/support/replication/ReplicationOperation.java
```

```java
public void onResponse(ReplicaResponse response) {
    successfulShards.incrementAndGet();
    try {
        primary.updateLocalCheckpointForShard(shard.allocationId().getId(), response.localCheckpoint());   // :171
        primary.updateGlobalCheckpointForShard(shard.allocationId().getId(), response.globalCheckpoint()); // :172
    } catch (final AlreadyClosedException e) {
        // 索引被删了 / relocation 后这个分片从未被激活 → 忽略
    } catch (final Exception e) {
        // 主分片失败，但让复制流程继续走完
        primary.failShard(message, e);
    }
    decPendingAndFinishIfNeeded();
}
```

注意：**副本的失败不会让整个写请求失败**（除非 `wait_for_active_shards` 不满足）。
副本失败走 `onFailure`（`:184`）→ `failShardIfNeeded` → `ShardStateAction.remoteShardFailed(..., markAsStale=true, ...)`（见 §10）。

### 7.3 `pendingActions` 的计数技巧

```67:67:action/support/replication/ReplicationOperation.java
```

```java
private final AtomicInteger pendingActions = new AtomicInteger();
```

注释（`:57-65`）说得很清楚：除了主分片操作和每个副本操作各占 1 个计数，**"协调本身"也占 1 个**。
否则会出现：主分片执行完、一个副本请求都还没发出去时，`pendingActions` 已经归零 → 请求提前返回。

---

## 8. `TransportReplicationAction`：复制框架的 transport 侧

路径：`server/src/main/java/org/elasticsearch/action/support/replication/TransportReplicationAction.java`

### 8.1 请求是怎么带上 seqno 信息的

| 类 | 行号 | 携带内容 |
| --- | --- | --- |
| `ConcreteShardRequest` | `:1244` | `targetAllocationID` + `primaryTerm` |
| `ConcreteReplicaRequest` | `:1324` | 上面两个 **+ `globalCheckpoint`（`:1326`）+ `maxSeqNoOfUpdatesOrDeletes`（`:1327`）** |
| `ReplicaResponse` | `:1090` | 回程：`localCheckpoint` + `globalCheckpoint`（副本自己的） |

序列化有版本兼容（`:1112-1133`）：对 `< 6.0.0` 的节点，读写成 `PRE_60_NODE_CHECKPOINT`。

### 8.2 主分片侧的入口：`AsyncPrimaryAction`

```348:374:action/support/replication/TransportReplicationAction.java
```

```java
protected void doRun() throws Exception {
    final IndexShard indexShard = getIndexShard(shardId);
    final ShardRouting shardRouting = indexShard.routingEntry();
    if (shardRouting.primary() == false) {
        throw new ReplicationOperation.RetryOnPrimaryException(shardId, "actual shard is not a primary " + shardRouting);
    }
    final String actualAllocationId = shardRouting.allocationId().getId();
    if (actualAllocationId.equals(targetAllocationID) == false) {
        throw new ShardNotFoundException(...);            // allocation id 不匹配 → 重试
    }
    final long actualTerm = indexShard.getPendingPrimaryTerm();
    if (actualTerm != primaryTerm) {
        throw new ShardNotFoundException(...);            // ★ primary term 不匹配 → 重试
    }
    acquirePrimaryOperationPermit(indexShard, request, ...);
}
```

> **这就是"主分片身份"的双重校验**：`allocationId`（哪一次分配）+ `primaryTerm`（第几代主）。
> 老主分片即使还活着，收到请求时 term 对不上，请求会被拒绝并触发重试（最终路由到新主）。

### 8.3 副本侧：`AsyncReplicaAction` 里 msu 与 gcp 的落地

```634:646:action/support/replication/TransportReplicationAction.java
```

```java
public void onResponse(Releasable releasable) {
    try {
        final ReplicaResult replicaResult = shardOperationOnReplica(request, replica);
        releasable.close();
        final TransportReplicationAction.ReplicaResponse response =
                new ReplicaResponse(replica.getLocalCheckpoint(), replica.getGlobalCheckpoint());   // :640
        replicaResult.respond(new ResponseListener(response));
    } catch (final Exception e) { ... }
}
```

真正的 gcp/msu 更新发生在**拿到 permit 之后、执行之前**（`IndexShard.java:2483-2506`）：

```java
public void onResponse(final Releasable releasable) {
    if (opPrimaryTerm < operationPrimaryTerm) {
        releasable.close();
        onPermitAcquired.onFailure(new IllegalStateException(
            "%s operation primary term [%d] is too old (current [%d])"));        // :2486  老主的请求直接拒
    } else {
        assert assertReplicationTarget();
        try {
            updateGlobalCheckpointOnReplica(globalCheckpoint, "operation");      // :2498 ★ 先更新 gcp
            advanceMaxSeqNoOfUpdatesOrDeletes(maxSeqNoOfUpdatesOrDeletes);       // :2499 ★ 再推进 msu
        } catch (Exception e) { ... }
        onPermitAcquired.onResponse(releasable);
    }
}
```

**顺序依然是"先 gcp 后 msu"**：`updateGlobalCheckpointOnReplica` 里有一个前置检查（见 §9.3），
如果先推进了 msu 再更新 gcp，可能导致 gcp 超过本地 msu 的判断出错。

### 8.4 写后同步 global checkpoint

```444:463:action/support/replication/TransportReplicationAction.java
```

```java
private ActionListener<Response> createResponseListener(final PrimaryShardReference primaryShardReference) {
    return new ActionListener<Response>() {
        public void onResponse(Response response) {
            if (syncGlobalCheckpointAfterOperation) {                  // :448  写类 action = true
                final IndexShard shard = primaryShardReference.indexShard;
                try {
                    shard.maybeSyncGlobalCheckpoint("post-operation");  // :451
                } catch (final Exception e) {
                    // 只记日志，不要让同步失败影响这次写入
                }
            }
            ...
```

`syncGlobalCheckpointAfterOperation` 在 `TransportReplicationAction` 构造函数（`:136`/`:150`）里设置，
`TransportWriteAction` 传的是 `true`（`TransportWriteAction.java:68`），
而 `GlobalCheckpointSyncAction`、`TransportRefreshAction` 之类传 `false`。

---

## 9. `max_seq_no_of_updates_or_deletes`（msu）：6.5 引入的关键优化

### 9.1 它要解决的问题

`Engine` 里的注释（`index/engine/Engine.java:150`）说明了设计意图：

```java
/**
 * 2. A replica never advances this marker by itself but only inherits from its primary (via advanceMaxSeqNoOfUpdatesOrDeletes).
 */
private final AtomicLong maxSeqNoOfUpdatesOrDeletes = new AtomicLong(UNASSIGNED_SEQ_NO);   // :152
```

背景（`:1889-1920` 的注释）：

> 在 Lucene 里，**"覆盖写同一 doc id" 和 "新增一个 doc" 在底层是不同的**（前者是 `updateDocument`，后者是 `addDocument`）。
> 如果一个操作在**主分片**上被判定为"新增"（append-only，走 `addDocument`），
> 但在**副本**上因为乱序/重试被判定成"更新"，那么主副本的**物理段结构**就不同了 ——
> 一旦这个副本被提升为主，后续行为可能与原主不一致。

msu 就是用来消除这种不确定性的：

> **主分片把它见过的"最高 seq_no（在 update/delete 类操作里）"随每次复制请求带给副本。**
> 副本据此知道："凡是 seq_no ≤ msu 且我没见过的 op，我不能当成 append-only 优化"。

### 9.2 主分片侧：什么时候推进

`InternalEngine.planIndexingAsPrimary`（`:1019`）末尾（`:1067-1070`）：

```java
final boolean toAppend = plan.indexIntoLucene && plan.useLuceneUpdateDocument == false;
if (toAppend == false) {
    advanceMaxSeqNoOfUpdatesOrDeletes(plan.seqNoForIndexing);     // :1069  非纯追加 → 推进 msu
}
```

也就是：**只要这次写不是"纯 append"，就把该 seq_no 记为 msu**。
`advanceMaxSeqNoOfUpdatesOrDeletes`（`Engine.java:1936`）是 `updateAndGet(curr -> Math.max(curr, seqNo))`，单调取最大。

取值时机：`ReplicationOperation.java:120` → `primary.maxSeqNoOfUpdatesOrDeletes()` →
`TransportReplicationAction.java:1078` → `IndexShard.getMaxSeqNoOfUpdatesOrDeletes()`（`:2868`）。

### 9.3 副本侧：怎么用它

`InternalEngine.planIndexingAsNonPrimary`（`:951`）：

```java
final boolean appendOnlyRequest = canOptimizeAddDocument(index);
if (appendOnlyRequest
        && mayHaveBeenIndexedBefore(index) == false
        && index.seqNo() > maxSeqNoOfNonAppendOnlyOperations.get()) {    // :955 ★ 关键比较
    plan = IndexingStrategy.optimizedAppendOnly(index.seqNo(), 1L);      // :965  可以走 addDocument
} else {
    if (appendOnlyRequest == false) {
        maxSeqNoOfNonAppendOnlyOperations.updateAndGet(curr -> Math.max(index.seqNo(), curr));  // :968
    }
    versionMap.enforceSafeAccess();
    ...
}
```

注释（`:956-963`）解释得很直白：

> 一个 append-only 请求在主分片上写完、可被搜索之后，用户可能立刻发起一个"针对它的后续操作"。
> 极少数情况下，后续操作会**先于**原始 append-only 到达副本。
> 此时不能盲目走 append-only 优化 —— 但如果副本见过的**所有非 append-only 操作的 seq_no 都小于**这个 append-only，
> 就能断言"副本不可能见过那条 append-only 的文档"，于是优化安全。

这就是 `maxSeqNoOfNonAppendOnlyOperations` 这个字段存在的全部意义，而它的种子值来自主分片同步过来的 msu。

---

## 10. 副本失败、stale 与 in-sync 收缩

### 10.1 写类复制的"狠手段"：`WriteActionReplicasProxy`

`TransportWriteAction.java:372`：

```java
class WriteActionReplicasProxy extends ReplicasProxy {
    public void failShardIfNeeded(ShardRouting replica, String message, Exception exception, ...) {
        if (TransportActions.isShardNotAvailableException(exception) == false) {
            logger.warn(new ParameterizedMessage("[{}] {}", replica.shardId(), message), exception);
        }
        shardStateAction.remoteShardFailed(replica.shardId(), replica.allocationId().getId(),
            primaryTerm, true /* markAsStale */, message, exception, ...);        // :384
    }

    public void markShardCopyAsStaleIfNeeded(ShardId shardId, String allocationId, ...) {
        shardStateAction.remoteShardFailed(shardId, allocationId, primaryTerm, true,
            "mark copy as stale", null, ...);                                      // :391
    }
}
```

对比基类 `ReplicasProxy`（`TransportReplicationAction.java:1180`/`:1190`）—— refresh、global checkpoint sync
这类**非写操作**的失败**不动**副本状态（注释：_"the replica should still be"_）。

### 10.2 `markAsStale` 的语义

`ShardStateAction.remoteShardFailed(..., markAsStale=true, ...)`（`ShardStateAction.java:164`）
→ 发 `internal:cluster/shard/failure`（`:83`）给 master
→ `ShardFailedClusterStateTaskExecutor`（`:305`）应用集群状态
→ 该 allocationId 从 `IndexMetaData.inSyncAllocationIds` 中移除。

**后果链条**：

```
副本被判死 → 从 in-sync 移除 → primary 的 updateFromMaster 删掉该条目 (removedEntries)
          → updateGlobalCheckpointOnPrimary() → global checkpoint 可能大步前进
          → translog 可以 trim 更多 → 磁盘释放
```

> **注意**：这里也能看出为什么 `ReplicationOperation.markUnavailableShardsAsStale`（`:131`）很重要 ——
> in-sync 集合里存在"路由表已无分配"的幽灵条目时，global checkpoint 会被**永久冻结**。
> 每次写操作都会顺手清理它们。

---

## 11. primary term：主分片代次与"脑裂"防线

### 11.1 两个 term 字段

| 字段 | 位置 | 含义 |
| --- | --- | --- |
| `pendingPrimaryTerm` | `IndexShard.java`（`getPendingPrimaryTerm():387`） | 集群状态里最新看到的 term |
| `operationPrimaryTerm` | `IndexShard.java` | 当前 engine 实际服务的 term |

`requirePrimaryTermUpdate(opPrimaryTerm, allPermits)`（`IndexShard.java:2553`）：

```java
return (opPrimaryTerm > pendingPrimaryTerm) || (allPermits && opPrimaryTerm > operationPrimaryTerm);
```

### 11.2 副本侧检测到新主：`bumpPrimaryTerm`

```2515:2546:index/shard/IndexShard.java
```

```java
if (requirePrimaryTermUpdate(opPrimaryTerm, allowCombineOperationWithPrimaryTermUpdate)) {
    synchronized (mutex) {
        if (requirePrimaryTermUpdate(opPrimaryTerm, allowCombineOperationWithPrimaryTermUpdate)) {
            final IndexShardState shardState = state();
            if (shardState != IndexShardState.POST_RECOVERY && shardState != IndexShardState.STARTED) {
                throw new IndexShardNotStartedException(shardId, shardState);   // 恢复中直接拒
            }
            bumpPrimaryTerm(opPrimaryTerm, () -> {
                updateGlobalCheckpointOnReplica(globalCheckpoint, "primary term transition");   // :2529
                final long currentGlobalCheckpoint = getGlobalCheckpoint();
                final long maxSeqNo = seqNoStats().getMaxSeqNo();
                logger.info("detected new primary with primary term [{}], global checkpoint [{}], max_seq_no [{}]", ...);  // :2532
                if (currentGlobalCheckpoint < maxSeqNo) {
                    resetEngineToGlobalCheckpoint();     // :2535 ★ 回滚到 global checkpoint
                } else {
                    getEngine().rollTranslogGeneration();// :2537 只是滚动 translog
                }
            }, allowCombineOperationWithPrimaryTermUpdate ? operationListener : null);
            ...
```

**这段话是"主副本如何收敛到一致"的核心**：

- 副本发现请求带来的 term 比自己的大 → 说明**换了新主**。
- 新主广播的 global checkpoint 才是"权威水位"。
- 如果副本自己的 `max_seq_no > global checkpoint`，说明副本上有**老主写入但从未被确认**的操作
  （这些操作在新主上可能不存在，因为新主可能没收到）→ **必须回滚**。

`resetEngineToGlobalCheckpoint()`（`IndexShard.java:2814`）：

```java
void resetEngineToGlobalCheckpoint() throws IOException {
    assert getActiveOperationsCount() == 0;
    sync();                                        // 先把 global checkpoint 持久化
    final SeqNoStats seqNoStats = seqNoStats();
    final TranslogStats translogStats = translogStats();
    flush(new FlushRequest().waitIfOngoing(true)); // flush 出一个 commit point
    ...
    final long globalCheckpoint = ...;
    newEngine.advanceMaxSeqNoOfUpdatesOrDeletes(globalCheckpoint);         // :2838
    newEngine.recoverFromTranslog(translogRunner, globalCheckpoint);       // :2843 ★ 只重放到 global checkpoint
}
```

> **这就是"未确认的写可能被丢弃"的源码证据**：
> 只有 ≤ global checkpoint 的操作会被保留。这也正是 ES 的语义 —— **响应返回给客户端 = 已确认 = 一定在 global checkpoint 之前 = 不会丢**。

### 11.3 新主上任：激活 + 补空洞 + resync

```514:577:index/shard/IndexShard.java
```

```java
bumpPrimaryTerm(newPrimaryTerm, () -> {
    replicationTracker.activatePrimaryMode(getLocalCheckpoint());        // :522
    final Engine engine = getEngine();
    if (getMaxSeqNoOfUpdatesOrDeletes() == UNASSIGNED_SEQ_NO) {
        engine.advanceMaxSeqNoOfUpdatesOrDeletes(seqNoStats().getMaxSeqNo());   // :536 老版本兼容
    }
    // 把之前 resetEngineToGlobalCheckpoint 回滚掉的操作从 translog 里恢复回来
    engine.restoreLocalHistoryFromTranslog((resettingEngine, snapshot) ->
        runTranslogRecovery(resettingEngine, snapshot, Engine.Operation.Origin.LOCAL_RESET, () -> {}));   // :538
    if (indexSettings.getIndexVersionCreated().onOrBefore(Version.V_6_0_0_alpha1)) {
        engine.flush(false, true);                                       // :546 老索引清 translog
    }
    engine.rollTranslogGeneration();                                     // :553
    engine.fillSeqNoGaps(newPrimaryTerm);                                // :554 ★ 用 noop 填 seqno 空洞
    replicationTracker.updateLocalCheckpoint(currentRouting.allocationId().getId(), getLocalCheckpoint());  // :555
    primaryReplicaSyncer.accept(this, new ActionListener<ResyncTask>() { ... });                            // :556
}, ...);
```

`fillSeqNoGaps(primaryTerm)`（`Engine.java:1839`）：把 `[local_checkpoint+1, max_seq_no]` 之间的"空洞"
用 `NoOp` 操作填上 —— 保证 local checkpoint 能一路推进到 max_seq_no。

> **为什么要填洞**：老主分配了 seq_no=7、8，但只完成了 8（比如 7 写主失败）。
> 新主必须显式地"声明 7 是一个 no-op"，否则 local checkpoint 永远卡在 6。

---

## 12. Primary-Replica Resync：新主把副本拉齐

路径：`server/src/main/java/org/elasticsearch/index/shard/PrimaryReplicaSyncer.java`

### 12.1 发什么

```84:93:index/shard/PrimaryReplicaSyncer.java
```

```java
public void resync(final IndexShard indexShard, final ActionListener<ResyncTask> listener) {
    final long startingSeqNo = indexShard.getGlobalCheckpoint() + 1;     // :87 ★ 从 gcp+1 开始
    final long maxSeqNo = indexShard.seqNoStats().getMaxSeqNo();         // :88
    snapshot = indexShard.getHistoryOperations("resync", startingSeqNo); // :93  读 translog history
    ...
    final long maxSeenAutoIdTimestamp = indexShard.getMaxSeenAutoIdTimestamp();  // :141
    resync(shardId, indexShard.routingEntry().allocationId().getId(), indexShard.getPendingPrimaryTerm(),
        wrappedSnapshot, startingSeqNo, maxSeqNo, maxSeenAutoIdTimestamp, resyncListener);   // :142
}
```

### 12.2 分块发送 + `trimmedAboveSeqNo`

```238:278:index/shard/PrimaryReplicaSyncer.java
```

```java
while ((operation = snapshot.next()) != null) {
    final long seqNo = operation.seqNo();
    if (startingSeqNo >= 0 && (seqNo == SequenceNumbers.UNASSIGNED_SEQ_NO || seqNo < startingSeqNo)) {
        totalSkippedOps.incrementAndGet();
        continue;                                          // 跳过 gcp 之前的
    }
    operations.add(operation);
    size += operation.estimateSize();
    if (size >= chunkSizeInBytes) break;                   // 默认 512KB（:62）
}
final long trimmedAboveSeqNo = firstMessage.get() ? maxSeqNo : SequenceNumbers.UNASSIGNED_SEQ_NO;   // :264
// have to send sync request even in case of there are no operations to sync - have to sync trimmedAboveSeqNo at least
if (!operations.isEmpty() || trimmedAboveSeqNo != SequenceNumbers.UNASSIGNED_SEQ_NO) {
    ResyncReplicationRequest request =
        new ResyncReplicationRequest(shardId, trimmedAboveSeqNo, maxSeenAutoIdTimestamp, operations.toArray(EMPTY_ARRAY));
    firstMessage.set(false);
    syncAction.sync(request, task, primaryAllocationId, primaryTerm, this);
}
```

**两个语义**：

1. **补发**：把 `[gcp+1, max_seq_no]` 的操作发给副本。
2. **截断**：`trimmedAboveSeqNo = maxSeqNo` 告诉副本「**你任何 seq_no > maxSeqNo 的本地操作都必须丢弃**」。
   这正好对应 §11.2 里副本的 `resetEngineToGlobalCheckpoint()`。

> 即使**一个操作都不用补发**，也要发一次请求（注释 `:265`），目的就是为了把 `trimmedAboveSeqNo` 送过去。

### 12.3 副本侧如何执行

`TransportResyncReplicationAction.performOnReplica`（`:135`）：

```java
if (request.getTrimmedAboveSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO) {
    replica.rollTranslogGeneration();   // 当前 translog 里可能有多余操作 → 先滚动
}
replica.updateMaxUnsafeAutoIdTimestamp(request.getMaxSeenAutoIdTimestampOnPrimary());
for (Translog.Operation operation : request.getOperations()) {
    final Engine.Result operationResult = replica.applyTranslogOperation(operation, Engine.Operation.Origin.REPLICA);  // :144
    if (operationResult.getResultType() == Engine.Result.Type.MAPPING_UPDATE_REQUIRED) {
        throw new TransportReplicationAction.RetryOnReplicaException(replica.shardId(),
            "Mappings are not available on the replica yet, triggered update: " + operationResult.getRequiredMappingUpdate());
    }
}
```

注意 `RetryOnReplicaException` 的处理（`TransportReplicationAction.java:650`）：
副本会 `observer.waitForNextChange(...)`，等下一个集群状态（mapping 到了）之后**把请求重发给自己**。

---

## 13. global checkpoint 的持久化与同步

### 13.1 三个"全局检查点"要分清

| 名称 | 获取方式 | 含义 |
| --- | --- | --- |
| `global checkpoint` | `IndexShard.getGlobalCheckpoint():1947` | 内存里的当前值 |
| `last synced global checkpoint` | `IndexShard.getLastSyncedGlobalCheckpoint():1954` | **已 fsync 进 translog checkpoint 文件**的值 |
| 各副本的"已知 gcp" | `ReplicationTracker.getInSyncGlobalCheckpoints():257` | 主分片对每个副本的 gcp 认知 |

### 13.2 `maybeSyncGlobalCheckpoint`：只在"确实落后"时才同步

```1973:1997:index/shard/IndexShard.java
```

```java
public void maybeSyncGlobalCheckpoint(final String reason) {
    verifyNotClosed();
    assert shardRouting.primary() : "only call maybeSyncGlobalCheckpoint on primary shard";
    if (replicationTracker.isPrimaryMode() == false) return;
    // only sync if there are not operations in flight
    final SeqNoStats stats = getEngine().getSeqNoStats(replicationTracker.getGlobalCheckpoint());
    if (stats.getMaxSeqNo() == stats.getGlobalCheckpoint()) {          // ① 没有在途写入
        final ObjectLongMap<String> globalCheckpoints = getInSyncGlobalCheckpoints();
        final String allocationId = routingEntry().allocationId().getId();
        final long globalCheckpoint = globalCheckpoints.get(allocationId);
        final boolean syncNeeded = StreamSupport.stream(globalCheckpoints.values().spliterator(), false)
            .anyMatch(v -> v.value < globalCheckpoint);                // ② 有副本落后
        if (syncNeeded) {
            globalCheckpointSyncer.run();                              // ③ 触发 GlobalCheckpointSyncAction
        }
    }
}
```

两个触发时机：

1. **每次写操作完成后**（`TransportReplicationAction.java:451`，reason = `"post-operation"`）
2. **主分片 term 变化时**（resync 过程中自然同步）

### 13.3 `GlobalCheckpointSyncAction`

`index/seqno/GlobalCheckpointSyncAction.java`：

```java
public static String ACTION_NAME = "indices:admin/seq_no/global_checkpoint_sync";   // :59

@Override
protected PrimaryResult<Request, ReplicationResponse> shardOperationOnPrimary(...) {
    maybeSyncTranslog(indexShard);                    // :122
    return new PrimaryResult<>(request, new ReplicationResponse());
}

@Override
protected ReplicaResult shardOperationOnReplica(final Request request, final IndexShard indexShard) throws Exception {
    maybeSyncTranslog(indexShard);                    // :128
    return new ReplicaResult();
}

private void maybeSyncTranslog(final IndexShard indexShard) throws IOException {    // :132
    if (indexShard.getTranslogDurability() == Translog.Durability.REQUEST &&
        indexShard.getLastSyncedGlobalCheckpoint() < indexShard.getGlobalCheckpoint()) {
        indexShard.sync();
    }
}
```

它其实**不携带任何 gcp 数值**。`gcp` 是通过 `ConcreteReplicaRequest.globalCheckpoint` 顺带过去的，
副本在 `innerAcquireReplicaOperationPermit` 里 `updateGlobalCheckpointOnReplica(globalCheckpoint, "operation")`（`IndexShard.java:2498`）。
这个 action 的真正价值是：**在没有任何写操作时，也能制造一次复制交互**，让落后的副本拿到最新的 gcp。

对 `< 6.0.0` 的节点（`:111`），直接伪造一个 `PRE_60_NODE_CHECKPOINT` 的响应，不去打扰它们。

### 13.4 副本侧更新 gcp 的保护

```2016:2036:index/shard/IndexShard.java
```

```java
public void updateGlobalCheckpointOnReplica(final long globalCheckpoint, final String reason) {
    assert assertReplicationTarget();
    final long localCheckpoint = getLocalCheckpoint();
    if (globalCheckpoint > localCheckpoint) {
        /*
         * 这可能在 recovery 期间发生：分片开了 engine 但 recovery 还没 finalize，就开始收到 gcp 更新。
         * 由于此时它还没参与 gcp 计算，主分片广播的 gcp 可能超前于它的 local checkpoint → 直接忽略。
         */
        assert state() != IndexShardState.POST_RECOVERY && state() != IndexShardState.STARTED : "...";
        return;
    }
    replicationTracker.updateGlobalCheckpointOnReplica(globalCheckpoint, reason);
}
```

`ReplicationTracker.updateGlobalCheckpointOnReplica`（`:457`）注释里补充了另一种情况：

> 主分片的信息可能滞后于副本（例如副本被提升为主，但它相对其它副本是陈旧的）。
> 此时副本本地的 gcp 可能比这个同步值更高 —— `updateGlobalCheckpoint`（`:497`）里有
> `if (cps != null && globalCheckpoint > cps.globalCheckpoint)` 的保护，只前进不后退。

### 13.5 等待 gcp 推进：`GlobalCheckpointListeners`

`index/shard/GlobalCheckpointListeners.java`（CCR、以及需要"等待写入在副本上完成"的场景用）：

```java
synchronized void add(final long waitingForGlobalCheckpoint, final GlobalCheckpointListener listener, final TimeValue timeout) {
    if (closed) { ...IndexShardClosedException... }
    if (lastKnownGlobalCheckpoint >= waitingForGlobalCheckpoint) {
        executor.execute(() -> notifyListener(listener, lastKnownGlobalCheckpoint, null));  // 已满足 → 立即通知
    } else {
        // 挂到 listeners 里，可带超时 → TimeoutException
    }
}
```

回调链：`IndexShard` 构造 `ReplicationTracker` 时传入 `globalCheckpointListeners::globalCheckpointUpdated`
（`IndexShard.java:310-311`）→ `ReplicationTracker.updateGlobalCheckpointOnPrimary`（`:755`）里 `onGlobalCheckpointUpdated.accept(...)`。

---

## 14. 乐观并发控制：`if_seq_no` + `if_primary_term`

传统 `version` 是**文档级**计数器：文档被删除再重建后 version 从 1 重新开始，
所以「我读到的 version=5」无法区分"第 5 次修改"和"重建后又改了 5 次"。

`seq_no` + `primary_term` 是**分片级 + 代次级**的，永不回退（除非分片被 reset），因此可以做严格的 CAS。

请求侧：`Engine.Index`（`Engine.java:1349-1366`）与 `Engine.Delete`（`:1447-1462`）都带 `ifSeqNo` / `ifPrimaryTerm`。
构造里有断言（`:1355-1360`）：

```java
assert ifPrimaryTerm == UNASSIGNED_PRIMARY_TERM || ifPrimaryTerm > 0 : "...";
assert ifSeqNo == UNASSIGNED_SEQ_NO || ifSeqNo >= 0 : "...";
assert (origin == Origin.PRIMARY) || (ifSeqNo == UNASSIGNED_SEQ_NO && ifPrimaryTerm == UNASSIGNED_PRIMARY_TERM) :
    "cas operations are only allowed if origin is primary";
```

> **CAS 只允许在主分片上判定**，副本只做重放。这保证"冲突与否"的裁决只有一个权威。

判定逻辑 `InternalEngine.planIndexingAsPrimary`（`:1019`）：

```java
final VersionValue versionValue = resolveDocVersion(index, index.getIfSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO);  // :1035
if (index.getIfSeqNo() != UNASSIGNED_SEQ_NO && versionValue == null) {
    // 文档不存在，但调用方期望它存在 → 冲突
    plan = IndexingStrategy.skipDueToVersionConflict(new VersionConflictEngineException(...), ...);       // :1048
} else if (index.getIfSeqNo() != UNASSIGNED_SEQ_NO &&
           (versionValue.seqNo != index.getIfSeqNo() || versionValue.term != index.getIfPrimaryTerm())) {  // :1049
    // ★ seq_no 或 primary_term 任一不匹配 → 冲突（409）
    plan = IndexingStrategy.skipDueToVersionConflict(new VersionConflictEngineException(
        shardId, index.type(), index.id(), index.getIfSeqNo(), index.getIfPrimaryTerm(),
        versionValue.seqNo, versionValue.term), ...);                                                     // :1054
} else if (index.versionType().isVersionConflictForWrites(currentVersion, index.version(), currentNotFoundOrDeleted)) {
    plan = IndexingStrategy.skipDueToVersionConflict(...);                                                // :1057
} else {
    plan = IndexingStrategy.processNormally(currentNotFoundOrDeleted,
        generateSeqNoForOperation(index), index.versionType().updateVersion(currentVersion, index.version()));  // :1061
}
```

**使用建议（云服务视角）**：
- 新业务一律用 `if_seq_no` + `if_primary_term`，不要用 `version`。
- 每次写响应都会带 `_seq_no` 和 `_primary_term`，客户端要用**最近一次读到的**那组值。
- 注意：主分片切换后 `_primary_term` 会变，此时旧值做 CAS 必然 409 —— 这是**正确的**行为。

---

## 15. 主分片选举为什么依赖 in-sync（回顾 + 加深）

第 11 章讲过 `PrimaryShardAllocator`，这里补上它与 seqno 模型的接口：

- **只有 in-sync 集合里的副本才有资格被选为新主**。
- in-sync 集合持久化在 `IndexMetaData.inSyncAllocationIds`（`IndexMetaData.java:513`），随 ClusterState 落盘。
- 副本进入 in-sync 的唯一途径是 `IndexShard.markAllocationIdAsInSync`（`:1928`）→ `ReplicationTracker.markAllocationIdAsInSync`（`:623`），
  而它**必须等副本 local checkpoint 追上 global checkpoint**。

由此推出整条保证链：

```
客户端收到 200 OK
  ⇒ 写操作已在主分片完成
  ⇒ 且（若 wait_for_active_shards 满足）已在至少一个副本完成
  ⇒ global checkpoint 只有在所有 in-sync 副本都完成后才前进
  ⇒ translog 只 trim 到 global checkpoint 之前
  ⇒ in-sync 集合持久化在磁盘（ClusterState）
  ⇒ master 只从 in-sync 集合里选新主
  ⇒ 新主一定持有所有已确认写入
```

**任何一环断了都会丢数据**，而源码里每一环都有 `assert` 把门。

---

## 16. 全景时序图（一次带 1 副本的 index 请求）

```
Client          协调节点              Primary                     Replica
  │               │                     │                           │
  ├─POST /i/d/1──►│                     │                           │
  │               ├─primaryAction──────►│                           │
  │               │              ① AsyncPrimaryAction.doRun         │
  │               │                 校验 allocationId + primaryTerm │
  │               │                 acquirePrimaryOperationPermit   │
  │               │              ② primary.perform(request)        │
  │               │                 generateSeqNo() → seq=42        │
  │               │                 planIndexingAsPrimary(...)      │
  │               │                 Lucene add/update + Translog    │
  │               │                 markSeqNoAsCompleted(42)        │
  │               │              ③ updateLocalCheckpointForShard    │
  │               │                 (primary 自己的 lcp)            │
  │               │              ④ gcp = primary.globalCheckpoint() │
  │               │                 msu = maxSeqNoOfUpdatesOrDeletes│
  │               │                 group = getReplicationGroup()   │
  │               │              ⑤ markUnavailableShardsAsStale    │
  │               │              ⑥ replicaAction(gcp, msu, seq=42)─►│
  │               │                                            ⑦ 校验 term/allocationId
  │               │                                               acquireReplicaOperationPermit
  │               │                                               updateGlobalCheckpointOnReplica(gcp)
  │               │                                               advanceMaxSeqNoOfUpdatesOrDeletes(msu)
  │               │                                               planIndexingAsNonPrimary(seq=42)
  │               │                                               Lucene + Translog
  │               │                                            ⑧ ReplicaResponse{lcp=42, gcp=?}
  │               │◄──────────────────────────────────────────────┤
  │               │              ⑨ updateLocalCheckpointForShard(replica, 42)
  │               │                 → updateGlobalCheckpointOnPrimary()
  │               │                 → onGlobalCheckpointUpdated (通知 listeners)
  │               │              ⑩ successfulShards++ / decPending → finish()
  │               │              ⑪ maybeSyncGlobalCheckpoint("post-operation")
  │               │◄──────────────┤
  │◄──200 OK──────┤  _seq_no=42, _primary_term=3, _shards{total=2,successful=2}
```

---

## 17. 线上排障手册

### 17.1 看什么指标

```bash
# 每个分片的 max_seq_no / local_checkpoint / global_checkpoint
GET /_cat/shards?h=index,shard,prirep,state,unassigned.reason,sq,sqg&v

# 详细（含 seq_no 三元组）
GET /my-index/_stats?level=shards
# 响应：shards[].seq_no.max / .local_checkpoint / .global_checkpoint
```

`_cat/shards` 的列定义在 `RestShardsAction.java:185`（`seq_no.global_checkpoint`，别名 `sqg`）
和 `:333-335`（三列分别取 `SeqNoStats::getMaxSeqNo` / `getLocalCheckpoint` / `getGlobalCheckpoint`）。

### 17.2 常见现象 → 源码定位

| 现象 | 可能原因 | 定位点 |
| --- | --- | --- |
| `global_checkpoint` 长期不动 | 某个 in-sync 副本落后 / 有副本卡在 pendingInSync | `computeGlobalCheckpoint`（`ReplicationTracker.java:720`）；`pendingInSync` 非空直接返回 fallback |
| translog 一直不删 | global checkpoint 不前进 | 同上；另见第 14 章 translog retention |
| 副本 `state=INITIALIZING` 很久 | 恢复中，`tracked=true, inSync=false` | `markAllocationIdAsInSync`（`:623`）在等 lcp 追上 gcp |
| 主副本 `_seq_no` 不一致 | 正常（副本可能落后）；若长期不一致看恢复 | `ReplicaResponse`（`TransportReplicationAction.java:1090`） |
| 写请求 409 且 `_primary_term` 变了 | 发生过主分片切换 | `bumpPrimaryTerm`（`IndexShard.java:2366`） |
| 副本被反复 mark stale | 副本写入失败 / GC 停顿 / 磁盘慢 | `WriteActionReplicasProxy.failShardIfNeeded`（`TransportWriteAction.java:379`） |
| 日志 `detected new primary with primary term [...]` | 正常的 term 提升 | `IndexShard.java:2532` |
| 日志 `global checkpoint sync failed` | post-operation 同步失败（不影响写入） | `GlobalCheckpointSyncAction.java:95` |

### 17.3 关键日志关键字

```
"updated global checkpoint to [{}]"                     ReplicationTracker.java:754
"marked [{}] as in-sync"                               ReplicationTracker.java:658 / :707
"detected new primary with primary term [{}], ..."     IndexShard.java:2532
"primary-replica resync completed with {} operations"  IndexShard.java:559
"syncing global checkpoint for [{}]"                   IndexShard.java:1993
"{} global checkpoint sync failed"                     GlobalCheckpointSyncAction.java:95
"[{}] failure while performing [{}] on replica {}"     ReplicationOperation.java:185
```

---

## 18. 动手实验

### 实验 1：观察 seq_no 与 primary_term

```bash
PUT /t_seqno
{"settings":{"number_of_shards":1,"number_of_replicas":1}}

PUT /t_seqno/_doc/1 {"a":1}
# → "_seq_no":0, "_primary_term":1

PUT /t_seqno/_doc/1 {"a":2}
# → "_seq_no":1, "_primary_term":1

# 用旧值做 CAS
PUT /t_seqno/_doc/1?if_seq_no=0&if_primary_term=1 {"a":3}
# → 409 VersionConflictEngineException（当前是 seq_no=1）
```

### 实验 2：观察 global checkpoint

```bash
GET /_cat/shards/t_seqno?h=index,shard,prirep,sq,sqg&v
# 停止一个副本节点后再写入，观察主分片 sqg 是否前进
```

### 实验 3：主分片切换 + resync

```bash
# 3 节点集群，停掉持有 primary 的节点
# 观察日志：
#   "detected new primary with primary term [N], global checkpoint [X], max_seq_no [Y]"
#   "primary-replica resync completed with N operations"
```

### 实验 4：断点清单（IDE 里打）

| 断点 | 目的 |
| --- | --- |
| `LocalCheckpointTracker.generateSeqNo`（`:80`） | 看 seq_no 何时分配 |
| `LocalCheckpointTracker.markSeqNoAsCompleted`（`:89`） | 看 local checkpoint 何时推进 |
| `ReplicationTracker.computeGlobalCheckpoint`（`:720`） | 看 gcp 怎么被"拖住" |
| `ReplicationOperation.execute`（`:102`） | 看主→副的编排顺序 |
| `IndexShard.innerAcquireReplicaOperationPermit`（`:2498`） | 看副本如何接收 gcp / msu |
| `IndexShard.resetEngineToGlobalCheckpoint`（`:2814`） | 看回滚到 gcp 的过程 |
| `PrimaryReplicaSyncer.SnapshotSender.doRun`（`:264`） | 看 resync 与 trimmedAboveSeqNo |

### 实验 5：单元测试（强烈建议）

```bash
./gradlew :server:test --tests "org.elasticsearch.index.seqno.ReplicationTrackerTests"
./gradlew :server:test --tests "org.elasticsearch.index.seqno.LocalCheckpointTrackerTests"
./gradlew :server:test --tests "org.elasticsearch.index.shard.PrimaryReplicaSyncerTests"
```

`ReplicationTrackerTests` 里有几百个 case，覆盖 handoff、pendingInSync、pre-6.0 兼容等边界，
是理解这个模型最省力的材料。

---

## 19. 自测题（答案都在本章）

1. `_seq_no` 是集群全局递增的还是分片级别的？主分片上它一定连续吗？
2. `local checkpoint` 与 `max_seq_no` 的区别是什么？为什么需要 bitmap？
3. `global checkpoint` 由谁计算？公式是什么？
4. 什么情况下 `computeGlobalCheckpoint` 会返回 fallback 而不推进？
5. `CheckpointState` 的 `inSync` 和 `tracked` 有什么区别？哪个决定"收不收写请求"？
6. 为什么 `ReplicationOperation.execute` 里必须**先取 gcp 再取复制组**？
7. `max_seq_no_of_updates_or_deletes`（msu）解决了什么问题？副本怎么用它？
8. 副本检测到新的 primary term 时，什么条件下会 `resetEngineToGlobalCheckpoint`？
9. resync 时为什么要传 `trimmedAboveSeqNo`？即使没有操作要补发也要发请求吗？
10. `markAllocationIdAsInSync` 为什么会阻塞？它阻塞期间 global checkpoint 会怎样？
11. primary relocation handoff 期间为什么禁止 `markAllocationIdAsInSync`，却允许 `updateFromMaster`？
12. translog 能 trim 到什么位置？这个位置由谁决定？
13. `if_seq_no` 相比 `version` 好在哪里？为什么 CAS 只允许在 origin=PRIMARY 时判定？
14. 写入返回 200 之后，能保证数据不丢吗？把保证链条按源码顺序复述一遍。
15. 一个副本长期落后，会对主分片产生哪些连带影响（至少 3 条）？
16. `pendingInSync` 里的条目什么时候会被移除？
17. 5.x 副本（`PRE_60_NODE_CHECKPOINT`）参与 global checkpoint 计算吗？后果是什么？
18. `GlobalCheckpointSyncAction` 的请求体里携带 global checkpoint 数值吗？那副本怎么拿到 gcp？
19. `maybeSyncGlobalCheckpoint` 的两个触发条件是什么？
20. `fillSeqNoGaps` 是干什么的？不做会怎样？
21. `ReplicationGroup` 的三类集合分别是什么？`skippedShards` 会被计入 `_shards.total` 吗？
22. 写类复制失败时副本会被 mark 为 stale，非写类（如 refresh）失败会吗？为什么？
23. `ReplicationOperation.pendingActions` 为什么除了主分片和副本各占 1 个，协调本身还要占 1 个？
24. `IndexShard` 里 `pendingPrimaryTerm` 和 `operationPrimaryTerm` 的区别？
25. 主分片收到 `opPrimaryTerm < operationPrimaryTerm` 的副本请求会怎么处理？

---

## 20. 关键类速查表

| 类 | 路径 | 一句话 / 关键行 |
| --- | --- | --- |
| `SequenceNumbers` | `index/seqno/SequenceNumbers.java` | `PRE_60=-3:34`、`UNASSIGNED=-2:38`、`NO_OPS=-1:42`、`min:85`、`max:109`、`loadSeqNoInfoFromLuceneCommit:55` |
| `LocalCheckpointTracker` | `index/seqno/LocalCheckpointTracker.java` | ★ 发号 + bitmap 水位：`BIT_SET_SIZE:35`、`generateSeqNo:80`、`markSeqNoAsCompleted:89`、`updateCheckpoint:172`、`waitForOpsToComplete:141` |
| `ReplicationTracker` | `index/seqno/ReplicationTracker.java` | ★★ 一致性核心：`primaryMode:88`、`checkpoints:129`、`pendingInSync:142`、`invariant:288`、`activatePrimaryMode:509`、`updateFromMaster:530`、`initiateTracking:602`、`markAllocationIdAsInSync:623`、`updateLocalCheckpoint:691`、`computeGlobalCheckpoint:720`、`updateGlobalCheckpointOnPrimary:745`、`startRelocationHandoff:762`、`completeRelocationHandoff:795`、`activateWithPrimaryContext:827` |
| `GlobalCheckpointSyncAction` | `index/seqno/GlobalCheckpointSyncAction.java` | `ACTION_NAME:59`、`updateGlobalCheckpointForShard:86`、`maybeSyncTranslog:132` |
| `SeqNoStats` | `index/seqno/SeqNoStats.java` | 三元组 + XContent（`:78`） |
| `ReplicationGroup` | `index/shard/ReplicationGroup.java` | `unavailableInSyncShards:47`、`replicationTargets:94`、`skippedShards:102` |
| `ReplicationOperation` | `action/support/replication/ReplicationOperation.java` | ★ `execute:90`、`markUnavailableShardsAsStale:131`、`performOnReplicas:143`、`performOnReplica:158`、`checkActiveShardCount:215`、`Primary:270`、`Replicas:348`、`ReplicaResponse:398` |
| `TransportReplicationAction` | `action/support/replication/TransportReplicationAction.java` | `syncGlobalCheckpointAfterOperation:117`、`newReplicasProxy:175`、`AsyncPrimaryAction.doRun:348`、`runWithPrimaryShardReference:376`、`createResponseListener:444`、`AsyncReplicaAction:597`、`PrimaryShardReference:1029`、`ReplicasProxy:1153`、`sendReplicaRequest:1231`、`ConcreteReplicaRequest:1324` |
| `TransportWriteAction` | `action/support/replication/TransportWriteAction.java` | `WritePrimaryResult:128`、`WriteReplicaResult:192`、`AsyncAfterWriteAction:286`、`WriteActionReplicasProxy:372` |
| `PrimaryReplicaSyncer` | `index/shard/PrimaryReplicaSyncer.java` | ★ `resync:84`、`startingSeqNo=gcp+1:87`、`chunkSize:62`、`SnapshotSender:187`、`trimmedAboveSeqNo:264` |
| `TransportResyncReplicationAction` | `action/resync/TransportResyncReplicationAction.java` | `ACTION_NAME:57`、`performOnReplica:135` |
| `GlobalCheckpointListeners` | `index/shard/GlobalCheckpointListeners.java` | `add:110`、`GlobalCheckpointListener:55` |
| `IndexShard` | `index/shard/IndexShard.java` | `ReplicationTracker 构造:310`、`updateShardState:410`、`activatePrimaryMode 调用:465/522`、`bumpPrimaryTerm:2366/2528`、`resetEngineToGlobalCheckpoint:2535/2814`、`maybeSyncGlobalCheckpoint:1973`、`updateGlobalCheckpointOnReplica:2016`、`innerAcquireReplicaOperationPermit:2470` |
| `Engine` | `index/engine/Engine.java` | `maxSeqNoOfUpdatesOrDeletes:152`、`getLocalCheckpoint:780`、`getSeqNoStats:793`、`getLastSyncedGlobalCheckpoint:798`、`fillSeqNoGaps:1839`、`restoreLocalHistoryFromTranslog:1831`、`advanceMaxSeqNoOfUpdatesOrDeletes:1936` |
| `InternalEngine` | `index/engine/InternalEngine.java` | `planIndexingAsNonPrimary:951`、`planIndexingAsPrimary:1019`、`planDeletionAsNonPrimary:1334`、`planDeletionAsPrimary:1382` |
| `IndexMetaData` | `cluster/metadata/IndexMetaData.java` | `inSyncAllocationIds:295/513`、`putInSyncAllocationIds:1020`、`primaryTerm(shardId)` |
| `ShardStateAction` | `cluster/action/shard/ShardStateAction.java` | `remoteShardFailed:164`、`localShardFailed:201`、`shardStarted:496` |
| `IndicesClusterStateService` | `indices/cluster/IndicesClusterStateService.java` | `updateShard:567`、`primaryTerm:576`、`inSyncIds:577`、`updateShardState 调用:592` |

---

**上一篇**：[16-RefreshFlush与Segment合并.md](./16-RefreshFlush与Segment合并.md)
**下一篇**：[18-分片恢复Recovery.md](./18-分片恢复Recovery.md)
