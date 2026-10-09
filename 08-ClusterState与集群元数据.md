# 08 · ClusterState 与集群元数据

> 目标：搞懂 ES 的「唯一真相源」——集群状态。
> **一张 `ClusterState` 同时装着：有哪些节点、有哪些索引、每个分片在哪、有哪些模板/别名、
> 有哪些 X-Pack 自定义元数据。** 集群里发生的任何"配置类"变更，本质都是「产生一个新版本的 ClusterState 并广播出去」。

---

## 1. 为什么要先学这一章

因为后面几乎每一章都会回到它：

| 章节 | 与 ClusterState 的关系 |
| --- | --- |
| 09 集群状态发布 | 就是「如何产生并同步 ClusterState」 |
| 10 选主 | 谁有权修改 ClusterState |
| 11 分片分配 | ★ 输入是 `RoutingTable`，输出是新 `RoutingTable` |
| 12 创建索引 | 在 `MetaData` 里加一个 `IndexMetaData` |
| 13 写入 | 每次写入提交都要拿 `ClusterState` 里的 `primaryTerm` / `inSyncAllocationIds` |
| 18 恢复 | 用 `RecoverySource` 和 `inSyncAllocationIds` 决定恢复方式 |

**一句话**：不理解 `ClusterState`，就无法理解 ES 任何「集群级」行为。

---

## 2. `ClusterState` 的结构（`ClusterState.java:84`）

### 2.1 五个核心字段 + 三个辅助字段

```java
public class ClusterState implements ToXContentFragment, Diffable<ClusterState> {
    public static final ClusterState EMPTY_STATE = builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)).build();  // :93

    public interface Custom extends NamedDiffable<Custom>, ToXContentFragment { }       // :106

    public static final String UNKNOWN_UUID = "_na_";                                   // :153
    public static final long UNKNOWN_VERSION = -1;                                      // :155

    private final long version;                                                          // :157
    private final String stateUUID;                                                      // :159  ★ 内容指纹
    private final RoutingTable routingTable;                                             // :161  ★ 分片路由
    private final DiscoveryNodes nodes;                                                  // :163  ★ 节点列表
    private final MetaData metaData;                                                     // :165  ★ 索引/模板/别名
    private final ClusterBlocks blocks;                                                  // :167  ★ 全局/索引级 block
    private final ImmutableOpenMap<String, Custom> customs;                              // :169  ★ 可扩展槽位
    private final ClusterName clusterName;                                               // :171
    private final boolean wasReadFromDiff;                                               // :173  仅用于日志/测试
}
```

| 字段 | 类型 | 装了什么 | 对应 API |
| --- | --- | --- | --- |
| `version` | `long` | ★ **单调递增**，每次发布 +1 | `GET /_cluster/state` 里的 `cluster_uuid` 旁边的 `version` |
| `stateUUID` | `String` | ★ **每次内容变化都换一个随机 UUID** | `state.stateUUID()`，用于 diff 校验 |
| `nodes` | `DiscoveryNodes` | 所有已知节点、master 是谁、集群名、节点版本 | `GET /_nodes` |
| `metaData` | `MetaData` | 索引元数据、模板、别名、持久化配置 | `GET /_cluster/state/metadata` |
| `routingTable` | `RoutingTable` | 每个索引的每个分片路由 | `GET /_cluster/state/routing_table` |
| `blocks` | `ClusterBlocks` | 全局/索引级 block（如 `read_only`） | `GET /_cluster/state/blocks` |
| `customs` | `ImmutableOpenMap<String, Custom>` | ★ 扩展点：快照、ILM、CCR、安全、Watcher… | `GET /_cluster/state?filter_path=customs` |

### 2.2 ★ `version` 与 `stateUUID` 的区别（极高频考点）

```
version： 1 → 2 → 3 → 4 → ...          单调递增，用来「判断新旧」
stateUUID：a1b2... → f9e3... → ...     每次重新随机，用来「判断内容是否相同」
```

**为什么两个都要？**

`stateUUID` 的用途（`:204` 的注释说明）：

```java
/**
 * This stateUUID is automatically generated for each version of cluster state. It is used to make sure that
 * we are applying diffs to the right previous state.
 */
```

`ClusterStateDiff` 的应用方（从节点）会带上「我基于哪个 stateUUID 算出的 diff」，
Master 校验 stateUUID 是否一致。**这是"乐观并发控制"**：
如果 Master 已经发了新版本，从节点的 diff 就会因为 stateUUID 不匹配而被拒绝，
从节点会改为**重新拉全量状态**（见 09 章）。

### 2.3 不可变性（Immutable）

`ClusterState` **完全不可变**。所有修改通过 `ClusterState.Builder` 进行：

```java
ClusterState newState = ClusterState.builder(oldState)
        .metaData(newMetaData)
        .routingTable(newRoutingTable)
        .version(oldState.version() + 1)      // 通常由 MasterService 自动设置
        .build();
```

**为什么不可变？**
- 并发读无需加锁（发布线程写、成千上万的读线程读）
- 可以做「事务」：一批变更要么全成功要么全放弃
- 便于 diff 与回滚

> **性能含义**：`ClusterState` 的对象创建本身是廉价的（只是字段赋值），
> 因为它**共享子对象**。比如只改了 `MetaData` 时，`routingTable`/`nodes` 是**同一个引用**。
> 这是为什么「每次集群状态发布不需要深拷贝整个状态」。

### 2.4 `customs`：扩展槽位

```java
public interface Custom extends NamedDiffable<Custom>, ToXContentFragment { }   // :106

public <T extends Custom> T custom(String type) { return (T) customs.get(type); }   // :252
```

`Custom` 的 `type` 字符串就是 key。已知的 custom：

| type | 来源 | 内容 |
| --- | --- | --- |
| `snapshots` | 内置 | `SnapshotsInProgress`（进行中的快照） |
| `snapshot_deletions` | 内置 | `SnapshotDeletionsInProgress` |
| `restore` | 内置 | `RestoreInProgress` |
| `index-graveyard` | 内置 | `IndexGraveyard`（已删除索引的"墓碑"） |
| `repository` | 内置 | `RepositoriesMetaData` |
| `persistent_tasks` | 内置 | 集群级持久化任务 |
| `licenses` / `security` / `ilm` / `ccr` / `watcher` / `rollup` / `machine_learning` … | X-Pack | 各功能的集群状态 |
| 你的插件 | 自定义 | `MergableCustomMetaData`（`:528` 附近） |

**★ 自定义 custom 的注册**：`ClusterPlugin.getClusterStateCustoms()`。
**关键差异**：
- `Custom` 的变更**会让整个 custom 重新序列化**（全量）
- `MergableCustomMetaData`（`cluster/MergableCustomMetaData.java`）★ 支持**差异合并**，只发变化的 part
  → **写 X-Pack 级别的功能必须用这个**，否则集群状态会爆炸

### 2.5 `getRoutingNodes()`

```java
public RoutingNodes getRoutingNodes() {                       // :268
    if (routingNodes != null) { return routingNodes; }
    routingNodes = new RoutingNodes(this);                    // ★ 惰性构建 + 缓存
    return routingNodes;
}
```

`RoutingNodes` 是 `RoutingTable` 的**「以节点为视角」的重排视图**
（`RoutingTable` 是「以分片为视角」）。

| 视图 | 类 | 组织方式 | 谁在用 |
| --- | --- | --- | --- |
| 分片视角 | `RoutingTable` | `index → IndexRoutingTable → shardId → ShardRouting` | 搜索（找 shard 在哪）、写入 |
| 节点视角 | `RoutingNodes` | `node → 该节点上的所有 ShardRouting` | ★ **分配器（AllocationService）** |

**记住这个区别，读 11 章的时候会豁然开朗。**

### 2.6 常用方法速查

| 方法 | 行号 | 作用 |
| --- | --- | --- |
| `version()` / `getVersion()` | `:196` / `:200` | 版本号 |
| `stateUUID()` | `:208` | 内容指纹 |
| `nodes()` / `getNodes()` | `:212` / `:216` | 节点列表 |
| `metaData()` / `getMetaData()` | `:220` / `:224` | 元数据 |
| `routingTable()` | `:228` | 路由表 |
| `blocks()` | `:236` | block |
| `customs()` | `:244` | 自定义状态 |
| `custom(String)` | `:252` | 取自定义状态 |
| `getRoutingNodes()` | `:268` | 惰性构建的节点视图 |
| `toString()` | `:277` | ★ 排障时非常有用，会打印**全部**内容 |
| `builder()` / `builder(ClusterState)` | — | 构造器 |
| `incrementVersion()` | — | 版本 +1 |

> **★ 排障技巧**：`ClusterState.toString()`（`:277`）输出的格式是
> ```
> cluster uuid: xxx
> version: 42
> state uuid: yyy
> from_diff: false
> meta data version: 40
>    index1: v[5], mv[3], sv[2]
>      0: p_term [4], isa_ids [xxx, yyy]
> ```
> 其中 `p_term` = `primaryTerm`（见 17 章）、`isa_ids` = `inSyncAllocationIds`。
> **这一行是排查「主分片选举 / 副本一致性」问题的第一手信息。**

---

## 3. `MetaData`：集群元数据（`MetaData.java:79`）

### 3.1 字段

```java
public class MetaData implements Iterable<IndexMetaData>, Diffable<MetaData> {
    public static final String ALL = "_all";                                          // :87
    public static final Setting<Integer> SETTING_CLUSTER_MAX_SHARDS_PER_NODE = ...;   // :128
    public static final Setting<Boolean> SETTING_READ_ONLY_SETTING = ...;             // :131
    public static final Setting<Boolean> SETTING_READ_ONLY_ALLOW_DELETE_SETTING = ...;// :138
    public static final MetaData EMPTY_META_DATA = builder().build();                 // :146

    public static final String CONTEXT_MODE_PARAM = "context_mode";                    // :148
    public static final String CONTEXT_MODE_SNAPSHOT = XContentContext.SNAPSHOT.toString();  // :150
    public static final String CONTEXT_MODE_GATEWAY = XContentContext.GATEWAY.toString();    // :152
    public static final String GLOBAL_STATE_FILE_PREFIX = "global-";                   // :154

    private final String clusterUUID;                                                  // :158
    private final long version;                                                        // :159  ★ 元数据独立版本号
    private final Settings transientSettings;                                          // :161
    private final Settings persistentSettings;                                         // :162
    private final Settings settings;                                                   // :163  ★ transient + persistent 合并后的结果
    private final ImmutableOpenMap<String, IndexMetaData> indices;                     // :164  ★
    private final ImmutableOpenMap<String, IndexTemplateMetaData> templates;           // :165
    private final ImmutableOpenMap<String, Custom> customs;                            // :166
    private final int totalNumberOfShards;                                             // :168
    private final int totalOpenIndexShards;                                            // :169
    private final int numberOfShards;                                                  // :170
    private final String[] allIndices;                                                 // :172  ★ 预计算的数组
    private final String[] allOpenIndices;                                             // :173
    private final String[] allClosedIndices;                                           // :174
    private final SortedMap<String, AliasOrIndex> aliasAndIndexLookup;                 // :176  ★ 预计算的别名/索引查找
}
```

**★ 三个设计要点**：

1. **`version` 是独立的**。`ClusterState.version` 和 `MetaData.version` 是两个计数器。
   `MetaData` 只在元数据变化时 +1，所以 `meta data version < cluster state version` 是常见的。
   **排障价值**：如果两者总是相等，说明每次状态变更都在改元数据（可能是异常）。

2. **`settings` 是 `transient` + `persistent` 合并的结果**（`transient` 优先）。
   读的时候用 `settings()`，写的时候要区分 `persistentSettings()` / `transientSettings()`。
   → **见 04 章 1.6 的优先级表。**

3. **预计算缓存**（`:172-176`）：`allIndices`、`allOpenIndices`、`allClosedIndices`、
   `aliasAndIndexLookup` 都是构造时算好的。
   **为什么？** 因为它们被极高频访问（每个搜索请求都要解析索引表达式），预计算避免每次遍历。

### 3.2 `aliasAndIndexLookup` 与 `AliasOrIndex`

```java
// metadata/AliasOrIndex.java
public abstract class AliasOrIndex {
    public abstract boolean isAlias();
    public static class Index extends AliasOrIndex { ... }
    public static class Alias extends AliasOrIndex {
        private final ImmutableOpenMap<String, AliasMetaData> indexToAliasesMap;
        ...
    }
}
```

**作用**：`GET /my_alias/_search` 时，`MetaData` 能**O(1)** 判断 `my_alias`
是索引还是别名、别名指向哪些索引。

`IndexNameExpressionResolver`（`metadata/IndexNameExpressionResolver.java`）就是用它 + 通配符匹配，
把 `logs-*`、`_all`、`my_alias`、`+a,-b` 解析成**具体索引列表**。

> **这也是「为什么别名搜索比索引搜索开销相似」的答案**：查表本身是 O(1)。

### 3.3 `MetaData.Custom`

和 `ClusterState.Custom` 类似但**挂在 MetaData 上**：

```java
public interface Custom extends NamedDiffable<Custom>, ToXContentFragment, Iterable<ObjectObjectCursor<String, Custom>> { }  // :240 附近
```

| type | 内容 |
| --- | --- |
| `repositories` | `RepositoriesMetaData` |
| `index-graveyard` | `IndexGraveyard`（已删索引的墓碑，防止旧节点"复活"索引） |
| `xpack` / `security` / `ilm` … | X-Pack |

**注册入口**：`ClusterPlugin.getMetadataCustoms()`。

**★ `IndexGraveyard` 的意义**（`metadata/IndexGraveyard.java`）：
删除索引后，Master 会记一个墓碑。如果一个「刚从分区恢复的旧节点」重新加入并声称有这个索引，
Master 会用墓碑把它清理掉。**这是防"僵尸索引"的关键机制。**

### 3.4 常用方法

| 方法 | 作用 |
| --- | --- |
| `index(String)` / `hasIndex(String)` | 单个索引查找 |
| `getIndices()` / `iterator()` | 遍历所有索引 |
| `getTemplates()` / `template(String)` | 模板 |
| `resolveIndex(...)` | ★ 见 `IndexNameExpressionResolver` |
| `getNumberOfShards()` / `getTotalNumberOfShards()` | 分片统计 |
| `settings()` / `transientSettings()` / `persistentSettings()` | 配置 |
| `clusterUUID()` | 集群 UUID |
| `version()` | 元数据版本 |
| `getAllIndices()` / `getAllOpenIndices()` / `getAllClosedIndices()` | ★ 预计算数组 |
| `concreteAllIndices()` / `concreteAllOpenIndices()` | 过滤掉隐藏索引 |

---

## 4. `IndexMetaData`：单个索引的元数据（`IndexMetaData.java`）

### 4.1 ★ 核心常量（务必记忆）

```java
public static final String INDEX_SETTING_PREFIX = "index.";                              // :147
public static final String SETTING_NUMBER_OF_SHARDS = "index.number_of_shards";          // :148
public static final Setting<Integer> INDEX_NUMBER_OF_SHARDS_SETTING = ...;               // :149
public static final String SETTING_NUMBER_OF_REPLICAS = "index.number_of_replicas";      // :150
public static final Setting<Integer> INDEX_NUMBER_OF_REPLICAS_SETTING = ...;             // :151
public static final String SETTING_ROUTING_PARTITION_SIZE = "index.routing_partition_size"; // :154
public static final String SETTING_AUTO_EXPAND_REPLICAS = "index.auto_expand_replicas";  // :177
public static final Setting<AutoExpandReplicas> INDEX_AUTO_EXPAND_REPLICAS_SETTING = ...; // :178

// block
public static final String SETTING_READ_ONLY = "index.blocks.read_only";                 // :179
public static final String SETTING_BLOCKS_READ = "index.blocks.read";                    // :183
public static final String SETTING_BLOCKS_WRITE = "index.blocks.write";                  // :187
public static final String SETTING_BLOCKS_METADATA = "index.blocks.metadata";            // :191
public static final String SETTING_READ_ONLY_ALLOW_DELETE = "index.blocks.read_only_allow_delete";  // :195

// version（★ 排障神器）
public static final String SETTING_VERSION_CREATED = "index.version.created";            // :199
public static final Setting<Version> SETTING_INDEX_VERSION_CREATED = ...;                // :201
public static final String SETTING_VERSION_CREATED_STRING = "index.version.created_string";  // :204
public static final String SETTING_VERSION_UPGRADED = "index.version.upgraded";          // :205
public static final String SETTING_VERSION_UPGRADED_STRING = "index.version.upgraded_string"; // :206
public static final String SETTING_CREATION_DATE = "index.creation_date";                // :207
public static final String SETTING_INDEX_PROVIDED_NAME = "index.provided_name";          // :212
public static final String SETTING_PRIORITY = "index.priority";                          // :213
public static final String SETTING_INDEX_UUID = "index.uuid";                            // :217
public static final String SETTING_DATA_PATH = "index.data_path";                        // :218

// ★ 分配过滤（热冷架构的核心，见 11 章）
public static final Setting.AffixSetting<String> INDEX_ROUTING_REQUIRE_GROUP_SETTING = ...;   // :226
public static final Setting.AffixSetting<String> INDEX_ROUTING_INCLUDE_GROUP_SETTING = ...;   // :229
public static final Setting.AffixSetting<String> INDEX_ROUTING_EXCLUDE_GROUP_SETTING = ...;   // :232
public static final Setting.AffixSetting<String> INDEX_ROUTING_INITIAL_RECOVERY_GROUP_SETTING = ...;  // :235

public static final int INDEX_FORMAT_DEFAULT = 6;                                        // :252 附近
public static final Setting<Integer> INDEX_FORMAT_SETTING = ...;                         // :253
```

**`Setting.AffixSetting` 是什么？** 它表示「**前缀可变**的配置」，例如：

```java
INDEX_ROUTING_REQUIRE_GROUP_SETTING = Setting.affixKeySetting(
    "index.routing.allocation.require.", "key",
    (name) -> Setting.simpleString(name, Property.Dynamic, Property.IndexScope))
```

所以 `index.routing.allocation.require.box_type: cold` 会被解析为
`AffixSetting = INDEX_ROUTING_REQUIRE_GROUP_SETTING, key = "box_type", value = "cold"`。

用 `getAllConcreteSettings(settings)` 可以取出所有匹配这个前缀的配置。
**这就是「为什么 `box_type` 可以随便叫，不叫 box_type 也行」的原因。**

### 4.2 序列化的 key 常量（★ 排障用）

```java
public static final String KEY_IN_SYNC_ALLOCATIONS = "in_sync_allocations";   // :256
static final String KEY_VERSION = "version";                                  // :257
static final String KEY_MAPPING_VERSION = "mapping_version";                  // :258
static final String KEY_SETTINGS_VERSION = "settings_version";                // :259
static final String KEY_ROUTING_NUM_SHARDS = "routing_num_shards";            // :260
static final String KEY_SETTINGS = "settings";                                // :261
static final String KEY_STATE = "state";                                      // :262  ★ open/close
static final String KEY_MAPPINGS = "mappings";                                // :263
static final String KEY_ALIASES = "aliases";                                  // :264
static final String KEY_ROLLOVER_INFOS = "rollover_info";                     // :265
public static final String KEY_PRIMARY_TERMS = "primary_terms";               // :266  ★
```

对应 `GET /_cluster/state/metadata` 的 JSON 结构：

```json
{
  "metadata": {
    "cluster_uuid": "xxx",
    "version": 42,
    "indices": {
      "myindex": {
        "state": "open",
        "settings": { "index.number_of_shards": "1", "index.version.created": "6060199", ... },
        "mappings": { "_doc": { "properties": { ... } } },
        "aliases": [ "myalias" ],
        "primary_terms": { "0": 3 },
        "in_sync_allocations": { "0": [ "allocId1" ] },
        "version": 12,
        "mapping_version": 3,
        "settings_version": 5,
        "routing_num_shards": 1024
      }
    }
  }
}
```

### 4.3 ★ 三个版本号的区别（高频考点）

| 字段 | getter | 什么时候 +1 |
| --- | --- | --- |
| `version` | `getVersion()` | **任何**元数据变化（最粗） |
| `mapping_version` | `getMappingVersion()` | 只有 **mapping** 变化 |
| `settings_version` | `getSettingsVersion()` | 只有 **settings** 变化 |

**为什么分这么细？** 为了**缓存失效的粒度**。

例如 `MapperService` 的缓存 key 是 `mapping_version`：
mapping 没变时，即使 `settings_version` 变了（如改了 `refresh_interval`），
**也不需要重建 mapping 解析器**。

反之 `IndexSettings` 的缓存 key 是 `settings_version`。

> **`ClusterState.toString()`（`:288`）打印的 `v[...], mv[...], sv[...]` 就是这三个值。**
> 排查「mapping 为什么不生效」时，先看 `mv` 有没有变。

### 4.4 `IndexMetaData.State`

```java
public enum State { OPEN, CLOSE }
```

`getState()` / `INDEX_STATE_SETTING`。
**关键**：`CLOSE` 的索引在 `MetaData` 里**仍然存在**，但 `RoutingTable` 里没有它的分片。

**排障**：
- `_cluster/health` 的 `indices` 里没有某个索引 → 它被 close 了
- `GET /_cat/indices` 默认不显示 close 的索引（要 `?expand_wildcards=all`）

### 4.5 `inSyncAllocationIds` 与 `primaryTerms`（★ 见 17 章）

```java
private final ImmutableOpenIntMap<Set<String>> inSyncAllocationIds;   // shardId → 该分片所有 In-Sync 副本的 allocationId
private final ImmutableOpenIntMap<Long> primaryTerms;                  // shardId → 主分片任期
```

| 字段 | 含义 | 为什么重要 |
| --- | --- | --- |
| `in_sync_allocations` | ★ 当前"主分片认可"的副本集合 | 决定副本请求发给谁；决定恢复时从哪些副本可以走 peer recovery |
| `primary_terms` | ★ 主分片的"任期号"（类似 Paxos 的 term） | 每次主分片切换 +1；**防止旧主分片复活后继续写入** |

**★ 这是 ES 6.x 一致性模型的核心**，务必记住这两个字段名。
（`GET /_cluster/state/metadata` 里能直接看到。）

### 4.6 三个独立版本与 `rolloverInfos` 等

| 字段 | 用途 |
| --- | --- |
| `rolloverInfos` | `_rollover` 的历史信息（记录上次 rollover 的索引名、时间） |
| `aliases` | `ImmutableOpenMap<String, AliasMetaData>` |
| `mappings` | `ImmutableOpenMap<String, MappingMetaData>`（6.x 还有 type 概念，虽然只允许一个） |
| `settings` | `Settings` |
| `activeAllocationIds(int shardId)` | `inSyncAllocationIds` + 正在 `INITIALIZING` 的 |
| `isSameUUID(...)` | 校验 `index.uuid` 是否一致（★ 见 18 章，防止恢复错索引） |

---

## 5. `RoutingTable`：分片路由（`RoutingTable.java`）

### 5.1 结构

```java
public class RoutingTable implements Iterable<IndexRoutingTable>, Diffable<RoutingTable> {
    public static final RoutingTable EMPTY_ROUTING_TABLE = builder().build();

    private final long version;                                          // 路由表独立版本号
    private final long indexVersion;                                     // ★ 从 MetaData.version 同步
    private final ImmutableOpenMap<String, IndexRoutingTable> indicesRouting;
    private final transient List<ShardRouting> shardsWithState;           // 未使用，见源码注释
}
```

| 字段 | 说明 |
| --- | --- |
| `version` | `RoutingTable` 自己的版本号 |
| `indexVersion` | ★ **必须等于 `MetaData.version`**。如果不等，说明 `RoutingTable` 与 `MetaData` **不一致** |

**★ `indexVersion` 的用途**：`ClusterState` 的构造器会断言/reconcile 两者。
如果 `MetaData` 改了索引但没有重建 `RoutingTable`，就会抛异常：

```
RoutingTable does not have the same index version as the metadata
```

**排障**：看到这个错说明某个 `ClusterStateUpdateTask` 改了 `MetaData` 却没重建 `RoutingTable`
（或者反之）。**这是写插件时最常见的错误之一。**

### 5.2 `IndexRoutingTable`

```java
public class IndexRoutingTable implements Iterable<IndexShardRoutingTable> {
    private final Index index;
    private final ImmutableOpenIntMap<IndexShardRoutingTable> shards;    // shardId → 该分片的所有副本
}
```

### 5.3 `IndexShardRoutingTable`（★ 最重要的路由结构）

```java
public class IndexShardRoutingTable implements Iterable<ShardRouting> {
    private final ShardId shardId;
    private final List<ShardRouting> shards;              // 所有副本（含主）
    private final List<ShardRouting> activeShards;        // ★ 活跃的
    private final List<ShardRouting> assignedShards;      // ★ 已分配的
    private final ShardRouting primary;                   // ★ 主分片
    private final List<ShardRouting> replicas;
    ...
    public Set<String> getAllAllocationIds();             // 所有 allocationId
    public boolean canBalance();                          // 是否可以再分配副本
}
```

**注意**：`ShardRouting` 是**不可变**的。分片状态变化 = 创建新的 `ShardRouting` 对象。

**★ 高频用途**：

```java
IndexShardRoutingTable shardTable = state.routingTable().shardRoutingTable("index", shardId);
ShardRouting primary = shardTable.primaryShard();               // 找主分片
List<ShardRouting> replicas = shardTable.replicaShards();       // 找副本
```

### 5.4 `ShardRouting`（`ShardRouting.java:46`）

```java
public class ShardRouting implements Writeable, ToXContentObject {
    private final ShardId shardId;                       // :44
    private final String currentNodeId;                  // :46  分片在哪个节点
    private final long expectedShardSize;                // :49  预期大小
    private final String relocatingNodeId;               // :51  ★ 迁移目标节点
    private final boolean primary;                       // :53
    private final ShardRoutingState state;               // :54
    private final RecoverySource recoverySource;         // :54  ★ 从哪恢复
    private final UnassignedInfo unassignedInfo;         // :55  ★ 为什么未分配
    private final AllocationId allocationId;             // :57  ★ 唯一标识"一次分配"
    private final ShardRouting targetRelocatingShard;    // :60  迁移目标分片
}
```

**断言反映的规则**（`:80-90`，读这些 assert 能快速理解不变量）：

```java
assert !(state == ShardRoutingState.UNASSIGNED && unassignedInfo == null) : "unassigned shard must be created with meta";
assert (state == ShardRoutingState.UNASSIGNED || state == ShardRoutingState.INITIALIZING) == (recoverySource != null) :
    "recovery source only available on unassigned or initializing shard but was " + state;
assert (currentNodeId == null) == (state == ShardRoutingState.UNASSIGNED) :
    "unassigned shard must not be assigned to a node " + this;
```

| 不变量 | 含义 |
| --- | --- |
| `UNASSIGNED` ⇒ `unassignedInfo != null` | 未分配的分片**必须**有原因（★ 这就是 `GET /_cluster/allocation/explain` 能工作的基础） |
| `UNASSIGNED`/`INITIALIZING` ⇔ `recoverySource != null` | `STARTED` 的分片不再持有恢复来源 |
| `currentNodeId == null` ⇔ `UNASSIGNED` | 未分配 = 没节点 |

### 5.5 ★ `ShardRoutingState`（4 个状态，必须背下来）

```
                    ┌──────────────┐
       (new index) │  UNASSIGNED   │  ← unassignedInfo 记录原因
                    └───────┬──────┘
                            │ 分配器选中一个节点
                            ▼
                    ┌──────────────┐
                    │ INITIALIZING │  ← recoverySource 记录来源
                    └───────┬──────┘
                            │ 恢复完成
                            ▼
                    ┌──────────────┐   （迁移）    ┌──────────────┐
                    │   STARTED    │ ───────────► │  RELOCATING  │
                    └───────┬──────┘               └───────┬──────┘
                            │                              │ 目标节点 INITIALIZING → STARTED
                            │                              ▼
                            │                      旧节点分片删除
                            │
                  (节点离线/分片失败)
                            ▼
                    ┌──────────────┐
                    │  UNASSIGNED   │
                    └──────────────┘
```

**`RELOCATING` 是「源分片」的状态；目标节点上是一个 `INITIALIZING` 的分片。**

```java
private ShardRouting initializeTargetRelocatingShard() {                       // :94
    if (state == ShardRoutingState.RELOCATING) {
        return new ShardRouting(shardId, relocatingNodeId, currentNodeId, primary, ShardRoutingState.INITIALIZING,
            PeerRecoverySource.INSTANCE, unassignedInfo, AllocationId.newTargetRelocation(allocationId), expectedShardSize);
    } else {
        return null;
    }
}
```

**辅助判断方法**（`:141-209`）：

| 方法 | 行号 | 含义 |
| --- | --- | --- |
| `unassigned()` | `:141` | 状态为 `UNASSIGNED` |
| `initializing()` | `:149` | 状态为 `INITIALIZING` |
| `active()` | `:159` | `started() || relocating()` |
| `started()` | `:166` | 状态为 `STARTED` |
| `relocating()` | `:176` | 状态为 `RELOCATING` |
| `relocatingNodeId()` | `:198` | 迁移目标节点 |
| `getTargetRelocatingShard()` | `:207` | 迁移的目标分片（断言必须 `relocating()`） |
| `unassignedInfo()` | `:217` | 未分配原因 |
| `primary()` | `:232` | 是否主分片 |
| `state()` | `:239` | 状态枚举 |
| `shardId()` | `:246` | 分片 ID |
| `newUnassigned(...)` | `:106` | 静态工厂：创建未分配分片 |

### 5.6 `UnassignedInfo`（★ 排障核心）

```java
public class UnassignedInfo implements ToXContentFragment, Writeable {
    public enum Reason {
        INDEX_CREATED,               // 索引刚创建
        CLUSTER_RECOVERED,           // 集群从元数据恢复
        INDEX_REOPENED,              // 索引重新打开
        DANGLING_INDEX_IMPORTED,     // 导入悬空索引
        NEW_INDEX_RESTORED,          // 快照恢复的新索引
        EXISTING_INDEX_RESTORED,     // 快照恢复的已有索引
        REPLICA_ADDED,               // 副本数增加
        ALLOCATION_FAILED,           // 分配失败
        NODE_LEFT,                   // ★ 节点离开
        REROUTE_CANCELLED,           // reroute 取消
        REINITIALIZED,               // 重新初始化
        REALLOCATED_REPLICA,         // 副本重新分配
        PRIMARY_FAILED,               // ★ 主分片失败
        FORCED_EMPTY_PRIMARY,        // ★ 强制空主分片
        MANUAL_ALLOCATION            // 手工分配
    }
    private final Reason reason;
    private final long unassignedTimeInNanos;
    private final String message;
    private final int failureCount;          // ★ 分配失败次数
    private final long lastAllocationFailureTimestamp;
    private final boolean delayed;
    private final boolean isStaleAllocation; // 是否过期的分配
}
```

**★ `Reason` 是 `GET /_cluster/allocation/explain` 输出的核心字段**：

```json
"unassigned_info": {
  "reason": "NODE_LEFT",
  "at": "2026-01-01T00:00:00.000Z",
  "delayed": false,
  "allocation_status": "no_valid_shard_copy",
  "details": "node_left [node1]"
}
```

**「`NODE_LEFT` + `delayed: false`」意味着：**
节点掉了，且**没有其他可用副本** → 只能等节点回来，或做 `reroute` 强制分配。

### 5.7 `RecoverySource`（`routing/RecoverySource.java:321`）

```java
public abstract class RecoverySource implements Writeable, ToXContentObject {
    public static final RecoverySource EMPTY_SNAPSHOT = new SnapshotRecoverySource(...);
    ...
    public static class SnapshotRecoverySource extends RecoverySource { ... }   // 从快照恢复
    public static class LocalShardsRecoverySource extends RecoverySource { ... } // shrink 时从本地分片恢复
    public static class PeerRecoverySource extends RecoverySource { ... }       // ★ 从其他副本恢复
}
```

| 类型 | 触发场景 |
| --- | --- |
| `EMPTY_STORE`（`PeerRecoverySource.INSTANCE` 的反面） | **新建主分片**：没有任何数据来源 |
| `EXISTING_STORE` | 索引已存在，本地磁盘上有数据 → 本地恢复 |
| `PEER` | 从其他副本（peer）恢复 |
| `SNAPSHOT` | 从快照恢复 |
| `LOCAL_SHARDS` | `_shrink` 时从源索引的本地分片恢复 |

**★ 读 18 章时会回来用这张表。**

---

## 6. `DiscoveryNodes`：节点列表（`cluster/node/`）

| 类 | 作用 |
| --- | --- |
| `DiscoveryNode` | 单个节点（name、id、address、attributes、roles、version） |
| `DiscoveryNodes` | 所有节点 + master 信息 + 各种过滤视图 |
| `ImmutableNodes` | 不可变集合包装 |
| `NodesStats` / `NodeStats` | 节点统计（MonitorService 用） |
| `NodeShutdownService` | 节点关闭状态（6.7+） |

`DiscoveryNodes` 的关键字段与方法：

```java
private final ImmutableOpenMap<String, DiscoveryNode> nodes;      // nodeId → node
private final String localNodeId;
private final String masterNodeId;
private final long version;                                        // ★ 节点列表也有版本号
private final long clusterStateVersion;

public boolean isLocalNodeElectedMaster();
public DiscoveryNode getLocalNode();
public DiscoveryNode getMasterNode();
public DiscoveryNodes getNodesByRole(DiscoveryNode.Role role);      // 如所有 data 节点
public boolean isAllNodesAtLeast(...);                              // 版本一致性检查
```

**★ `nodes` 也有 `version`**：这个版本号用来做**增量广播**（只发变化的节点）。
`ClusterStateDiff` 里 `nodes` 部分的 `CompactNodeInfo` 就是干这个的。

**★ 排障**：`GET /_cluster/state/nodes` 与 `GET /_cluster/state?version=N` 配合，
可以看某个版本的节点列表。

---

## 7. `Diff` 机制：集群状态的增量同步（★ 极重要）

### 7.1 为什么需要 diff

一个 100 节点的集群，`ClusterState` 可能有几 MB。
每秒发布几次 → 网络带宽直接爆炸。

**所以 ES 引入了 `Diff`**：从节点给 Master 一个「我当前的 stateUUID」，
Master 算出一个 diff（只有变更部分），只发 diff。

### 7.2 接口（`cluster/Diff.java`）

```java
public interface Diff<T> extends Writeable {
    T apply(T part);
}
```

**就这样，只有一个方法。** `apply` 把 diff 应用到旧状态上，返回新状态。

### 7.3 相关接口

| 接口 | 路径 | 含义 |
| --- | --- | --- |
| `Diffable<T>` | `cluster/Diffable.java` | `diff(T previousState)` + `readFrom(StreamInput)` |
| `NamedDiffable<T>` | `cluster/NamedDiffable.java` | ★ 带 `getWriteableName()`，用于 `customs`（因为 custom 是插件提供的，Master 不知道对方认不认识） |
| `AbstractDiffable<T>` | `cluster/AbstractDiffable.java` | 提供了 `serialize` / `readDiffFrom` 骨架 |
| `AbstractNamedDiffable<T>` | `cluster/AbstractNamedDiffable.java` | 同上 + name |
| `DiffableUtils` | `cluster/DiffableUtils.java` | ★ 提供 `ImmutableOpenMap` / `ImmutableOpenIntMap` 的 diff 工具 |

### 7.4 ★ `DiffableUtils`：最常被复用的工具

```java
// cluster/DiffableUtils.java
public static <K, V extends Diffable<V>> MapDiff<K, V> diff(Map<K, V> before, Map<K, V> after);
public static <K, V> MapDiff<K, V> diff(Map<K, V> before, Map<K, V> after, KeySerializer<K, V> keySerializer);
public static <V> IntMapDiff<V> diff(ImmutableOpenIntMap<V> before, ImmutableOpenIntMap<V> after);

// 值的序列化策略
public static <V> KeySerializer<String, V> getStringsKeySerializer()  // key 是 String，直接写
public static <V> KeySerializer<ShardId, V> getShardIdKeySerializer() // key 是 ShardId
```

**排障**：`--"Diff serialization failure"` 类异常几乎都来自 `DiffableUtils`
的 key 序列化器与实际的 key 类型不匹配。

### 7.5 `ClusterState` 的 diff

`ClusterState` 实现了 `Diffable<ClusterState>`（`ClusterState.java` 内部类 `ClusterStateDiff`）。

**它的 diff 内容**（按需组合，跳过相同项）：

| 组件 | 是否有 diff |
| --- | --- |
| `ClusterName` | 无（diff 里不包含） |
| `version` / `stateUUID` | 总是包含 |
| `RoutingTable` | ✅ |
| `DiscoveryNodes` | ✅（`CompactNodeInfo` 只发变化的） |
| `MetaData` | ✅（内部也做细分 diff） |
| `ClusterBlocks` | ✅ |
| `customs` | ✅（但 unknown 的 custom 会跳过 → 订阅不够就退化成全量） |

### 7.6 ★ diff 失败 → 全量回退

```
Master 收到 publish 请求，里面带 clusterStateVersion（从节点的版本）
  ├─ fromVersion == master.currentVersion  → 发 Diff
  └─ 否则                                  → 发全量 ClusterState
```

从节点侧：

```java
// IncompatibleClusterStateVersionException
throw new IncompatibleClusterStateVersionException(
    "Applied cluster state version [" + currentVersion + "] to cluster state of version [" + targetVersion + "]");
```

**排障**：日志里出现 `applying cluster state of version ... but have ...` 说明 diff 版本不匹配，
从节点会重新请求全量。**这本身不是错误**，只是低效；但如果**频繁**出现，
说明发布太快 / 网络抖动，值得关注。

### 7.7 `XContent` 与 `Diff` 是两套序列化

**★ 一定要区分清楚**：

| 场景 | 机制 | 层次 |
| --- | --- | --- |
| Transport 节点间同步 | **二进制 + Diff** | 性能关键 |
| 磁盘持久化（`global-*.st`） | **二进制 + Diff** | 见 23 章 |
| REST API 输出 | **XContent（JSON）** | 给人看 |
| 快照内容 | **XContent（JSON）** | 跨版本兼容 |

所以 `MetaData` 有 `writeTo`/`readFrom`（二进制）**和** `toXContent`/`fromXContent`（JSON），
两套都要维护。**这是读 ES 元数据代码时最容易混淆的地方。**

---

## 8. `ClusterState` 的构造：`ClusterState.Builder`

```java
public static Builder builder(ClusterName clusterName) { ... }
public static Builder builder(ClusterState state) { ... }        // ★ 从已有状态构造（拷贝）

public static class Builder {
    public Builder nodes(DiscoveryNodes nodes);
    public Builder routingTable(RoutingTable routingTable);
    public Builder metaData(MetaData metaData);
    public Builder blocks(ClusterBlocks blocks);
    public Builder incrementVersion();
    public Builder version(long version);
    public Builder stateUUID(String stateUUID);
    public Builder putCustom(String type, Custom custom);
    public Builder removeCustom(String type);
    public ClusterState build();
}
```

**★ `ClusterState.builder(state)` 会自动校验 `RoutingTable.indexVersion == MetaData.version`**
（见 5.1）。所以正确的写法是：

```java
MetaData newMetaData = MetaData.builder(oldState.metaData()).put(indexMetaData, false).build();
RoutingTable newRoutingTable = RoutingTable.builder(oldState.routingTable()).add(indexRoutingTable)
        .build();      // ★ 内部会把 indexVersion 设为 newMetaData.version
```

**或者用 `MetaData.Builder` + `RoutingTable.Builder` 的 `build(ClusterState)` 变体**。

---

## 9. ★ 完整示例：创建一个索引时 ClusterState 的变化

```
POST /myindex       → TransportCreateIndexAction → MetaDataCreateIndexService.createIndex(...)
   │
   ├─ ① 计算新分片数（含 auto_expand_replicas → 见 11 章）
   ├─ ② 构造 IndexMetaData
   │      IndexMetaData.builder("myindex")
   │        .settings(actualIndexSettings)
   │        .numberOfShards(numShards)
   │        .numberOfReplicas(0)                       # ← ★ 注意：先创建 0 副本！
   │        .primaryTerm(shardId, 1)
   │        .state(State.OPEN)
   │        .build()
   │
   ├─ ③ 构造 IndexRoutingTable（★ 所有分片初始为 UNASSIGNED）
   │      IndexRoutingTable.Builder indexRoutingTable = new IndexRoutingTable.Builder(index);
   │      for (int i = 0; i < numShards; i++) {
   │          indexRoutingTable.addShard(
   │              ShardRouting.newUnassigned(
   │                  new ShardId(index, i), true,
   │                  RecoverySource.EMPTY_STORE,                      # 主分片空恢复
   │                  new UnassignedInfo(Reason.INDEX_CREATED, null)
   │              )
   │          );
   │          for (int j = 0; j < numReplicas; j++) {                     # ★ 副本也是 UNASSIGNED
   │              indexRoutingTable.addShard(
   │                  ShardRouting.newUnassigned(
   │                      new ShardId(index, i), false,
   │                      PeerRecoverySource.INSTANCE,
   │                      new UnassignedInfo(Reason.INDEX_CREATED, null)
   │                  )
   │              );
   │          }
   │      }
   │
   ├─ ④ 提交 ClusterStateUpdateTask
   │      ClusterState newState = ClusterState.builder(currentState)
   │              .metaData(MetaData.builder(currentState.metaData()).put(indexMetaData, true).build())
   │              .routingTable(RoutingTable.builder(currentState.routingTable()).add(indexRoutingTable).build())
   │              .build();
   │
   ├─ ⑤ MasterService.submitStateUpdateTask → 发布（见 09 章）
   │
   └─ ⑥ 各节点 applyClusterState → AllocationService 选节点 → 分片变 INITIALIZING → 恢复（见 18 章）
```

**★ 为什么先创建 0 副本？**
1. **快速可用**：主分片先恢复，索引立刻可写
2. **避免浪费**：如果 3 节点集群创建 5 副本，先建 0 副本就不会卡在 `wait_for_active_shards`
3. 然后通过 `UpdateSettings` 把副本数设为目标值 → 分配器再分配副本

**★ 你会在 `MetaDataCreateIndexService` 里看到这段逻辑**（搜 `numberOfReplicas(0)`）。

---

## 10. 动手实验

**实验 8-1：观察 `version` 与 `stateUUID`**
```bash
curl -s 'localhost:9200/_cluster/state?pretty' | grep -E 'version|state_uuid'
curl -s -XPUT 'localhost:9200/myidx' -H 'Content-Type: application/json' -d '{"settings":{"number_of_shards":1,"number_of_replicas":0}}'
curl -s 'localhost:9200/_cluster/state?pretty' | grep -E 'version|state_uuid'
```
观察 `version` +1、`state_uuid` 变化。

**实验 8-2：观察三个版本号**
```bash
curl -s 'localhost:9200/_cluster/state/metadata/myidx?pretty' | grep -E '"version"|mapping_version|settings_version'
curl -s -XPUT 'localhost:9200/myidx/_settings' -H 'Content-Type: application/json' -d '{"index":{"refresh_interval":"5s"}}'
curl -s 'localhost:9200/_cluster/state/metadata/myidx?pretty' | grep -E '"version"|mapping_version|settings_version'
```
验证：**改 settings 时 `settings_version` +1，`mapping_version` 不变。**

**实验 8-3：观察 `in_sync_allocations` 与 `primary_terms`**
```bash
curl -s 'localhost:9200/_cluster/state/metadata?pretty' | python -m json.tool | grep -A5 -E 'in_sync|primary_terms'
```
然后 kill 一个副本节点，观察 `in_sync_allocations` 的变化。

**实验 8-4：观察 `RoutingTable` 与 `MetaData` 的一致性**
在 `ClusterState.Builder.build()` 打断点，查看 `routingTable.indexVersion()` 与
`metaData.version()` 是否相等。

**实验 8-5：观察 `Diff`**
两个节点集群，在 `ClusterStateDiff.writeTo` 打断点（或设 `logger.org.elasticsearch.cluster.service: DEBUG`），
创建一个索引，观察 diff 的字节数 vs 全量 ClusterState 的字节数。

**实验 8-6：找到 unassigned 的原因**
```bash
# 关掉一个副本节点
kill -9 <node2 pid>
curl -s 'localhost:9200/_cluster/allocation/explain?pretty'
```
观察 `unassigned_info.reason` 是 `NODE_LEFT`，`delayed: true`（因为 `index.unassigned.node_left.delayed_timeout` 默认 1m）。
把该设置改为 0，观察 `delayed` 变成 false。

**实验 8-7：观察 IndexGraveyard**
```bash
curl -s -XDELETE 'localhost:9200/myidx'
curl -s 'localhost:9200/_cluster/state/metadata?pretty' | grep -A10 graveyard
```

---

## 11. 自测题

1. `ClusterState.version` 与 `stateUUID` 各是什么？为什么两个都需要？
2. `ClusterState` 为什么设计成不可变？不可变后"修改"是怎么做的？性能上为什么不是问题？
3. `ClusterState.version` 与 `MetaData.version` 的关系？为什么它们可以不等？
4. `RoutingTable.indexVersion` 的作用？与 `MetaData.version` 不等会怎样？
5. `RoutingTable` 与 `RoutingNodes` 的区别？各自谁在用？
6. `ClusterState.Custom` 与 `MetaData.Custom` 的区别？`MergableCustomMetaData` 解决什么问题？
7. `IndexMetaData` 的三个版本号（`version`/`mapping_version`/`settings_version`）分别何时 +1？为什么要分开？
8. `ShardRoutingState` 有哪四个状态？`RELOCATING` 时源分片和目标分片分别是什么状态？
9. `ShardRouting` 的 `AllocationId` 与 `ShardId` 的区别？谁更"稳定"？
10. `UnassignedInfo.Reason` 有哪些常见值？如何用它排障？
11. `RecoverySource` 有哪几种？分别对应什么场景？
12. `Diff` 机制解决什么问题？如果 diff 失败会怎样？
13. `MetaData` 的 `allIndices` / `aliasAndIndexLookup` 为什么预计算？
14. 创建索引时为什么先创建 0 副本？
15. `IndexGraveyard` 解决什么问题？不给它会怎样？

---

## 12. 关键类速查

| 类 | 路径 | 一句话 |
| --- | --- | --- |
| `ClusterState` | `cluster/ClusterState.java:84` | ★ 集群唯一真相源 |
| `ClusterState.Builder` | 同上 | 构造/修改 |
| `ClusterStateDiff` | 同上（内部类） | 增量序列化 |
| `ClusterChangedEvent` | `cluster/ClusterChangedEvent.java` | ★ apply 时的"变更事件"（含 diff 结果） |
| `Diffable` / `Diff` / `NamedDiffable` | `cluster/` | diff 机制 |
| `DiffableUtils` | `cluster/DiffableUtils.java` | ★ diff 工具（Map/IntMap） |
| `MetaData` | `cluster/metadata/MetaData.java:79` | 集群元数据 |
| `IndexMetaData` | `cluster/metadata/IndexMetaData.java` | ★ 单索引元数据 |
| `IndexTemplateMetaData` | `cluster/metadata/IndexTemplateMetaData.java` | 索引模板 |
| `AliasMetaData` / `AliasOrIndex` | `cluster/metadata/` | 别名 |
| `IndexNameExpressionResolver` | `cluster/metadata/IndexNameExpressionResolver.java` | ★ 索引表达式解析 |
| `IndexGraveyard` | `cluster/metadata/IndexGraveyard.java` | 已删索引墓碑 |
| `RoutingTable` | `cluster/routing/RoutingTable.java` | 分片路由表 |
| `IndexRoutingTable` | `cluster/routing/IndexRoutingTable.java` | 单索引路由 |
| `IndexShardRoutingTable` | `cluster/routing/IndexShardRoutingTable.java` | ★ 单分片的所有副本 |
| `ShardRouting` | `cluster/routing/ShardRouting.java:46` | ★ 一个分片实例 |
| `ShardRoutingState` | `cluster/routing/ShardRoutingState.java` | 4 状态枚举 |
| `UnassignedInfo` | `cluster/routing/UnassignedInfo.java` | ★ 未分配原因 |
| `RecoverySource` | `cluster/routing/RecoverySource.java:321` | 恢复来源 |
| `AllocationId` | `cluster/routing/AllocationId.java` | 分配的全局唯一 ID |
| `RoutingNodes` | `cluster/routing/RoutingNodes.java` | ★ 节点视角的视图 |
| `DiscoveryNodes` / `DiscoveryNode` | `cluster/node/` | 节点列表/单个节点 |
| `ClusterBlocks` | `cluster/block/ClusterBlocks.java` | 全局/索引 block |
| `ClusterName` | `cluster/ClusterName.java` | 集群名 |

---

**上一篇**：[07-Action框架与请求处理链.md](./07-Action框架与请求处理链.md)　
**下一篇**：[09-ClusterService与MasterService集群状态发布.md](./09-ClusterService与MasterService集群状态发布.md)
