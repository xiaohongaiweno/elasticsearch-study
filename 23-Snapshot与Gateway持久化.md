# 23 · Snapshot 与 Gateway 持久化

> 目标：区分并讲清 ES 的两套"持久化"：
> **Gateway**（每个节点把自己那份集群元数据写到本地磁盘 + 集群启动时恢复）和
> **Snapshot**（把索引数据备份到外部仓库 + 可跨集群恢复）。
>
> **为什么必须学**：
> - 「改了 setting / 建了模板，重启后还在吗」→ Gateway（`GatewayMetaState.applyClusterState`）。
> - 「集群重启后索引全变红，或者整个索引消失了」→ Gateway 恢复的**法定票数**问题
>   （`Gateway.performStateRecovery:93` 的 `requiredAllocation`）。
> - 「`state not recovered / initialized`」→ `STATE_NOT_RECOVERED_BLOCK`（`GatewayService:70`）。
> - 「快照一直 IN_PROGRESS / 删不掉 / 恢复卡住」→ `SnapshotsInProgress` / `RestoreInProgress` 集群状态。
> - 「仓库里出现 `index-N` 一堆文件 / dangling index」→ 快照的世代机制。
> - 云服务里**备份恢复是刚需**，也是最容易出事故的环节。
>
> **前置**：第 08 章（ClusterState）、第 09 章（ClusterState 发布）、第 11 章（分配）、第 18 章（Recovery）。
> **本篇对应**：阶段 5（生态期）。

---

## 1. 一图总览：两套持久化

```
┌─────────────────────── Gateway（本地、自动、元数据）────────────────────────┐
│                                                                             │
│  ClusterState 变更                                                          │
│        ↓ ClusterStateApplier                                                │
│  GatewayMetaState.applyClusterState  (gateway/GatewayMetaState.java:118)     │
│        ├─ writeGlobalState()  → data/nodes/0/_state/global-<N>.st            │
│        └─ writeIndex()        → data/nodes/0/indices/<uuid>/_state/state-N.st│
│                                                                             │
│  启动时：                                                                    │
│  GatewayService.clusterChanged  (:147)                                      │
│        ├─ 有 STATE_NOT_RECOVERED_BLOCK 且本节点是 master → 开始恢复           │
│        └─ performStateRecovery (:202) → Gateway.performStateRecovery (:61)   │
│              ├─ 向所有 master 节点拉取本地元数据（TransportNodesListGatewayMetaState）│
│              ├─ 全局元数据：取 version 最大的                                 │
│              ├─ 每个索引元数据：各自取 version 最大的                          │
│              └─ found < max(1, minimumMasterNodes) → 恢复失败                │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────── Snapshot（外部、手动、数据）─────────────────────────┐
│                                                                             │
│  PUT /_snapshot/my_repo/my_snap                                             │
│        ↓ TransportCreateSnapshotAction → SnapshotsService.createSnapshot     │
│                                          (snapshots/SnapshotsService.java:234)│
│        ├─ 写 SnapshotsInProgress（ClusterState.Custom, TYPE="snapshots"）     │
│        ├─ BlobStoreRepository.initializeSnapshot (:387) 写全局/索引元数据      │
│        ├─ 每个 data 节点 SnapshotShardsService.snapshot (:390)               │
│        │     └─ 拷 segment 文件（复用 Lucene commit point，增量！）            │
│        └─ finalizeSnapshot (:538) 写 snap-<uuid>.dat + 更新 index-N          │
│                                                                             │
│  POST /_snapshot/my_repo/my_snap/_restore                                   │
│        ↓ RestoreService.restoreSnapshot (snapshots/RestoreService.java:175)  │
│              └─ 写 RestoreInProgress → 索引进入 recovery（复用第 18 章流程）    │
└─────────────────────────────────────────────────────────────────────────────┘
```

**一句话区别**：

| | Gateway | Snapshot |
| --- | --- | --- |
| 内容 | **只有元数据**（cluster settings、模板、index metadata） | **数据 + 元数据**（segment 文件） |
| 位置 | 每个节点的本地数据目录 | 外部仓库（fs / s3 / hdfs / url） |
| 触发 | 自动（每次 ClusterState 变更） | 手动 / SLM 定时 |
| 用途 | 节点/集群重启后恢复"集群长什么样" | 容灾、跨集群迁移、版本升级回滚 |

---

## 2. 必读源码清单

### Gateway

| 类 | 路径 | 作用 |
| --- | --- | --- |
| `MetaDataStateFormat` | `gateway/MetaDataStateFormat.java` | ★ 落盘格式（`write:97`、`loadLatestState:257`） |
| `MetaStateService` | `gateway/MetaStateService.java` | 读写封装（`loadIndexState:80`、`writeIndex:120`） |
| `GatewayMetaState` | `gateway/GatewayMetaState.java` | ★ `ClusterStateApplier`；决定写什么（`applyClusterState:118`） |
| `GatewayService` | `gateway/GatewayService.java` | ★ 恢复门控（`STATE_NOT_RECOVERED_BLOCK:70`、`clusterChanged:147`） |
| `Gateway` | `gateway/Gateway.java` | ★ 恢复逻辑（`performStateRecovery:61`） |
| `TransportNodesListGatewayMetaState` | `gateway/` | 从各 master 节点收集本地元数据 |
| `DanglingIndicesState` | `gateway/DanglingIndicesState.java` | 悬挂索引处理 |

### Snapshot

| 类 | 路径 | 作用 |
| --- | --- | --- |
| `Repository` | `repositories/Repository.java` | 仓库接口 |
| `BlobStoreRepository` | `repositories/blobstore/BlobStoreRepository.java` | ★ 对象存储实现（`initializeSnapshot:387`、`deleteSnapshot:418`、`finalizeSnapshot:538`、`snapshotShard:851`、`restoreShard:867`） |
| `ChecksumBlobStoreFormat` | `repositories/blobstore/ChecksumBlobStoreFormat.java` | 带校验和 + **原子写**的 blob 格式 |
| `RepositoryData` | `repositories/RepositoryData.java` | ★ 仓库的"索引文件"内容（`getGenId:107`、`addSnapshot:157`） |
| `RepositoriesService` | `repositories/RepositoriesService.java` | 仓库注册/管理（`registerRepository:96`、`applyClusterState:285`） |
| `RepositoriesModule` | `repositories/RepositoriesModule.java` | 装配 `RepositoryPlugin`（`:47`） |
| `FsRepository` | `repositories/fs/FsRepository.java` | `fs` 类型仓库 |
| `SnapshotsService` | `snapshots/SnapshotsService.java` | ★ 快照主流程（`createSnapshot:234`、`beginSnapshot:347`） |
| `SnapshotShardsService` | `snapshots/SnapshotShardsService.java` | ★ 数据节点侧的分片快照（`snapshot:390`） |
| `RestoreService` | `snapshots/RestoreService.java` | ★ 恢复主流程（`restoreSnapshot:175`） |
| `SnapshotsInProgress` | `cluster/SnapshotsInProgress.java` | `ClusterState.Custom`，`TYPE="snapshots"`（`:49`） |
| `RestoreInProgress` | `cluster/RestoreInProgress.java` | `ClusterState.Custom` |
| `SnapshotDeletionsInProgress` | `cluster/SnapshotDeletionsInProgress.java` | `ClusterState.Custom` |

---

## 3. Gateway：元数据怎么落盘

### 3.1 `MetaDataStateFormat`

`gateway/MetaDataStateFormat.java:64`：

```java
public static final XContentType FORMAT = XContentType.SMILE;          // :65  ★ 二进制 JSON
public static final String STATE_DIR_NAME = "_state";                  // :66
public static final String STATE_FILE_EXTENSION = ".st";               // :67
private static final String STATE_FILE_CODEC = "state";                // :69
private static final int MIN_COMPATIBLE_STATE_FILE_VERSION = 1;        // :70
private static final int STATE_FILE_VERSION = 1;                       // :71
private static final int BUFFER_SIZE = 4096;                           // :72
```

**文件格式**：`Lucene header(codec="state", version=1) + XContentType(SMILE) + XContent + Lucene footer(checksum)`

`write`（`:97`）—— **关键是"先写临时文件 + fsync + 原子改名"**：

```java
final long maxStateId = findMaxStateId(prefix, locations)+1;           // :104  递增编号
final String fileName = prefix + maxStateId + STATE_FILE_EXTENSION;
Path stateLocation = locations[0].resolve(STATE_DIR_NAME);
Files.createDirectories(stateLocation);
final Path tmpStatePath  = stateLocation.resolve(fileName + ".tmp");    // :109
final Path finalStatePath = stateLocation.resolve(fileName);
try {
    try (OutputStreamIndexOutput out = new OutputStreamIndexOutput(...)) {
        CodecUtil.writeHeader(out, STATE_FILE_CODEC, STATE_FILE_VERSION);
        out.writeInt(FORMAT.index());
        try (XContentBuilder builder = newXContentBuilder(FORMAT, new IndexOutputOutputStream(out) { ... })) {
            builder.startObject();
            { toXContent(builder, state); }
            builder.endObject();
        }
        CodecUtil.writeFooter(out);                                    // :130  ★ 校验和
    }
    IOUtils.fsync(tmpStatePath, false);        // fsync the state file
    Files.move(tmpStatePath, finalStatePath, StandardCopyOption.ATOMIC_MOVE);   // :133 ★ 原子改名
    IOUtils.fsync(stateLocation, true);        // fsync 目录                    // :134
    // 其余 data path：拷贝 + fsync + 原子改名                                   // :136-152
} finally {
    Files.deleteIfExists(tmpStatePath);                                // :154
}
cleanupOldFiles(prefix, fileName, locations);                          // :157  删旧文件
```

**三个可靠性要点**：

1. **tmp + ATOMIC_MOVE**：不会出现"半个文件"。断电后要么旧的、要么新的。
2. **fsync 文件 + fsync 目录**（`:132`/`:134`）：保证改名本身也持久化。
3. **Lucene footer 校验和**：读取时能检测损坏 → `CorruptStateException`。

其余关键方法：

| 方法 | 行号 |
| --- | --- |
| `toXContent` / `fromXContent`（抽象） | `:168` / `:174` |
| `read` | `:180` |
| `loadLatestState` | `:257` （读编号最大的那个） |
| `deleteMetaState` | `:334` |
| `cleanupOldFiles` | （`write` 末尾调用） |

### 3.2 谁决定"写什么"

`GatewayMetaState.applyClusterState`（`:118`）：

```java
final ClusterState state = event.state();
if (state.blocks().disableStatePersistence()) {                        // :121
    // reset the current metadata, we need to start fresh...
    this.previousMetaData = null;
    previouslyWrittenIndices = emptySet();
    return;
}

MetaData newMetaData = state.metaData();
if (state.nodes().getLocalNode().isMasterNode() || state.nodes().getLocalNode().isDataNode()) {   // :133
    ...
    // ① 全局状态变了才写
    if (previousMetaData == null || !MetaData.isGlobalStateEquals(previousMetaData, newMetaData)) {
        metaStateService.writeGlobalState("changed", newMetaData);     // :163
    }
    // ② 索引级：只写"相关的"索引
    relevantIndices = getRelevantIndices(event.state(), event.previousState(), previouslyWrittenIndices);  // :170
    final Iterable<IndexMetaWriteInfo> writeInfo = resolveStatesToBeWritten(...);
    for (IndexMetaWriteInfo indexMetaWrite : writeInfo) {
        metaStateService.writeIndex(indexMetaWrite.reason, indexMetaWrite.newMetaData);   // :176
    }
}

if (success) {
    previousMetaData = newMetaData;                                    // :184
    previouslyWrittenIndices = unmodifiableSet(relevantIndices);
}
```

**"相关索引"怎么定**（`getRelevantIndices:189`）：

```java
if (isDataOnlyNode(state)) {
    relevantIndices = getRelevantIndicesOnDataOnlyNode(state, previousState, previouslyWrittenIndices);  // :192
} else if (state.nodes().getLocalNode().isMasterNode()) {
    relevantIndices = getRelevantIndicesForMasterEligibleNode(state);  // :194
} else {
    relevantIndices = Collections.emptySet();
}
```

- **master 节点**：写**所有**索引的元数据（`getRelevantIndicesForMasterEligibleNode:362`）。
- **纯 data 节点**：只写**在本节点有分片**的索引（`getRelevantIndicesOnDataOnlyNode:335`）。

源码里有一段很关键的注释（`:136-141`）：

> 我们通过查看分片路由来决定是否在纯 data 节点上写元数据，
> 只有当该索引有分片分配在本节点时才写。
> **但是 closed 索引不会出现在分片路由里** —— 所以如果 closed 索引的元数据被更新，
> 而 `previouslyWrittenIndices` 为空（比如状态持久化被禁用或节点重启了），它就不会被写入。
> 因此我们必须在这里检查**磁盘上是否有分片**，并把它们的索引加进 `previouslyWrittenIndices`。

对应代码（`:142-155`）：只对 `CLOSE` 状态的索引去 `metaStateService.loadIndexState(...)`。

> **排障**：`"某节点重启后丢了 closed 索引的元数据"` 就来自这段逻辑的边界情况。

---

## 4. Gateway：集群启动恢复

### 4.1 `STATE_NOT_RECOVERED_BLOCK`

`gateway/GatewayService.java:70`：

```java
public static final ClusterBlock STATE_NOT_RECOVERED_BLOCK =
    new ClusterBlock(1, "state not recovered / initialized", true, true, ...);
```

恢复门控的设置（`:55-67`）：

| Setting | 行号 |
| --- | --- |
| `gateway.expected_nodes` | `:55` |
| `gateway.expected_data_nodes` | `:57` |
| `gateway.expected_master_nodes` | `:59` |
| `gateway.recover_after_time` | `:61` |
| `gateway.recover_after_nodes` | `:63` |
| `gateway.recover_after_data_nodes` | `:65` |
| `gateway.recover_after_master_nodes` | `:67` |

默认 `recover_after_time` 在设置了 expected 时为 **5 分钟**（`DEFAULT_RECOVER_AFTER_TIME_IF_EXPECTED_NODES_IS_SET:73`）。

### 4.2 是否开始恢复

`GatewayService.clusterChanged`（`:147`）：

```java
if (state.nodes().isLocalNodeElectedMaster() == false) return;         // :154  不是 master 不管
if (state.blocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK) == false) return;  // :158  已恢复

if (state.nodes().getMasterNodeId() == null) {
    logger.debug("not recovering from gateway, no master elected yet");
} else if (recoverAfterNodes != -1 && nodes.getMasterAndDataNodes().size() < recoverAfterNodes) {
    ... // 节点数不够
} else if (recoverAfterDataNodes != -1 && ...) {
} else if (recoverAfterMasterNodes != -1 && ...) {
} else {
    // 判断是否需要强制等待 recover_after_time
    if (expectedNodes == -1 && expectedMasterNodes == -1 && expectedDataNodes == -1) {
        enforceRecoverAfterTime = true;                                // :180
        reason = "recover_after_time was set to [" + recoverAfterTime + "]";
    } else {
        enforceRecoverAfterTime = false;
        if (expectedNodes != -1 && nodes.getMasterAndDataNodes().size() < expectedNodes) {
            enforceRecoverAfterTime = true;                            // :187  expected 未满足 → 等
            reason = "expecting [" + expectedNodes + "] nodes, but only have [...]";
        } ...
    }
    performStateRecovery(enforceRecoverAfterTime, reason);              // :198
}
```

`performStateRecovery`（`:202`）：

```java
if (enforceRecoverAfterTime && recoverAfterTime != null) {
    if (scheduledRecovery.compareAndSet(false, true)) {
        logger.info("delaying initial state recovery for [{}]. {}", recoverAfterTime, reason);
        threadPool.schedule(recoverAfterTime, ThreadPool.Names.GENERIC, () -> {   // :208
            if (recovered.compareAndSet(false, true)) {
                gateway.performStateRecovery(recoveryListener);                    // :211
            }
        });
    }
} else {
    if (recovered.compareAndSet(false, true)) {
        threadPool.generic().execute(new AbstractRunnable() { ... doRun → gateway.performStateRecovery ... });
    }
}
```

> **排障**：集群启动后所有请求报 `state not recovered / initialized`，
> 通常是 `gateway.expected_nodes` 设太大（节点没全起来）或 `recover_after_time` 太长。

### 4.3 `Gateway.performStateRecovery`（核心）

`gateway/Gateway.java:61`：

```java
String[] nodesIds = clusterService.state().nodes().getMasterNodes().keys().toArray(String.class);
TransportNodesListGatewayMetaState.NodesGatewayMetaState nodesState = listGatewayMetaState.list(nodesIds, null).actionGet();  // :64

int requiredAllocation = Math.max(1, minimumMasterNodes);              // :67 ★ 法定票数

ObjectFloatHashMap<Index> indices = new ObjectFloatHashMap<>();
MetaData electedGlobalState = null;
int found = 0;
for (NodeGatewayMetaState nodeState : nodesState.getNodes()) {
    if (nodeState.metaData() == null) continue;
    found++;
    if (electedGlobalState == null) {
        electedGlobalState = nodeState.metaData();
    } else if (nodeState.metaData().version() > electedGlobalState.version()) {
        electedGlobalState = nodeState.metaData();                     // :87 ★ 取 version 最大
    }
    for (ObjectCursor<IndexMetaData> cursor : nodeState.metaData().indices().values()) {
        indices.addTo(cursor.value.getIndex(), 1);                     // :90  统计每个索引出现在几个节点
    }
}

if (found < requiredAllocation) {
    listener.onFailure("found [" + found + "] metadata states, required [" + requiredAllocation + "]");   // :94
    return;                                                            // ★ 恢复失败！
}

// update the global state, and clean the indices, we elect them in the next phase
MetaData.Builder metaDataBuilder = MetaData.builder(electedGlobalState).removeAllIndices();   // :98

// 逐个索引选举
for (int i = 0; i < keys.length; i++) {
    ...
    for (NodeGatewayMetaState nodeState : nodesState.getNodes()) {
        IndexMetaData indexMetaData = nodeState.metaData().index(index);
        if (indexMetaData == null) continue;
        if (electedIndexMetaData == null || indexMetaData.getVersion() > electedIndexMetaData.getVersion()) {
            electedIndexMetaData = indexMetaData;                      // :117 ★ 取 version 最大
        }
        indexMetaDataCount++;
    }
    if (electedIndexMetaData != null) {
        if (indexMetaDataCount < requiredAllocation) {
            logger.debug("[{}] found [{}], required [{}], not adding", index, indexMetaDataCount, requiredAllocation);  // :124
        } // TODO if this logging statement is correct then we are missing an else here
        try {
            if (electedIndexMetaData.getState() == IndexMetaData.State.OPEN) {
                // verify that we can actually create this index - if not we recover it as closed with lots of warn logs
                indicesService.verifyIndexMetadata(electedIndexMetaData, electedIndexMetaData);   // :129
            }
        } catch (Exception e) {
            logger.warn("recovering index {} failed - recovering as closed", electedIndex);
            electedIndexMetaData = IndexMetaData.builder(electedIndexMetaData).state(IndexMetaData.State.CLOSE).build();  // :134
        }
        metaDataBuilder.put(electedIndexMetaData, false);
    }
}
final ClusterState.Builder builder = upgradeAndArchiveUnknownOrInvalidSettings(metaDataBuilder);   // :141
listener.onSuccess(builder.build());
```

**要点**：

1. **全局元数据和索引元数据是分开选举的** —— 全局看 `MetaData.version()`，每个索引看 `IndexMetaData.getVersion()`。
2. `requiredAllocation = max(1, minimumMasterNodes)`：
   如果收集到的有元数据节点数不足，**整个集群拒绝恢复**（索引会全部"消失"）。
   这是"集群重启后索引没了"的头号原因。
3. 索引元数据创建失败（比如缺插件）→ **降级为 CLOSED 状态恢复**（`:134`）。
   表现：索引还在，但是关着的，日志里有 `recovering index ... failed - recovering as closed`。
4. `upgradeAndArchiveUnknownOrInvalidSettings`（`:145`）：
   把**不认识的 / 非法的** setting 归档（archive）而不是直接失败 —— 保证能起来。

> **排障**：`"found [1] metadata states, required [2]"` → 只有 1 个 master 节点有元数据
> （比如另一个节点的 data 目录是空的 / 挂载没生效），
> 而 `discovery.zen.minimum_master_nodes=2`。
> **这就是为什么 minimum_master_nodes 必须正确设置**（`minimum_master_nodes = master/2 + 1`）。

### 4.4 恢复成功后

`GatewayService.GatewayRecoveryListener.onSuccess`（`:238`）：

```java
clusterService.submitStateUpdateTask("local-gateway-elected-state", new ClusterStateUpdateTask() {
    public ClusterState execute(ClusterState currentState) {
        assert currentState.metaData().indices().isEmpty();
        // remove the block, since we recovered from gateway
        ClusterBlocks.Builder blocks = ClusterBlocks.builder()
                .blocks(currentState.blocks())
                .blocks(recoveredState.blocks())
                .removeGlobalBlock(STATE_NOT_RECOVERED_BLOCK);              // :249 ★ 解除 block
        MetaData.Builder metaDataBuilder = MetaData.builder(recoveredState.metaData());
        metaDataBuilder.generateClusterUuidIfNeeded();                      // :253
        if (MetaData.SETTING_READ_ONLY_SETTING.get(...) || ...) {
            blocks.addGlobalBlock(MetaData.CLUSTER_READ_ONLY_BLOCK);        // :257
        }
        ...
        for (IndexMetaData indexMetaData : recoveredState.metaData()) {
            metaDataBuilder.put(indexMetaData, false);
            blocks.addBlocks(indexMetaData);                                // :266
        }
        ClusterState updatedState = ClusterState.builder(currentState)
                .blocks(blocks).metaData(metaDataBuilder).build();
        ...
    }
});
```

### 4.5 悬挂索引

`DanglingIndicesState` + `LocalAllocateDangledIndices`：
如果某个 data 节点磁盘上有索引数据（shard 目录），但**集群元数据里没有这个索引**，
它就是"dangling"。ES 会把它上报给 master，由 `LocalAllocateDangledIndices` 决定是否导入。

```bash
GET  /_dangling                                   # 列出
POST /_dangling/<index-uuid>?accept_data_loss=true  # 导入
DELETE /_dangling/<index-uuid>                      # 删除
```

> 常见于：误删了索引但数据还在 / 从别的集群拷贝了数据目录。

---

## 5. Snapshot：仓库与 Blob 布局

### 5.1 Repository 层次

```
Repository (interface)
   └── BlobStoreRepository (abstract)  ← 所有对象存储仓库的基类
          ├── FsRepository    (type="fs")     repositories/fs/
          ├── S3Repository    (type="s3")     plugins/repository-s3/
          ├── URLRepository   (type="url")    modules/repository-url/  (只读)
          └── ...（hdfs、azure、gcs 等插件）
```

`RepositoriesModule`（`:47`）把 `RepositoryPlugin.getRepositories()` 的结果装配进 `RepositoriesService`。
`RepositoriesService`（`:56`）本身也是 `ClusterStateApplier`，
仓库定义存在 `ClusterState` 的 `RepositoriesMetaData` 里 → **注册仓库是一次集群状态更新**。

### 5.2 Blob 命名

`BlobStoreRepository`（`:183-209`）：

```java
private static final String SNAPSHOT_PREFIX         = "snap-";                      // :183
private static final String SNAPSHOT_CODEC          = "snapshot";
private static final String INDEX_FILE_PREFIX       = "index-";                     // :187
private static final String INDEX_LATEST_BLOB       = "index.latest";               // :189
private static final String INCOMPATIBLE_SNAPSHOTS_BLOB = "incompatible-snapshots"; // :191
private static final String METADATA_NAME_FORMAT    = "meta-%s.dat";                // :195
private static final String METADATA_CODEC          = "metadata";
private static final String INDEX_METADATA_CODEC    = "index-metadata";
private static final String SNAPSHOT_NAME_FORMAT    = SNAPSHOT_PREFIX + "%s.dat";   // :201
private static final String SNAPSHOT_INDEX_PREFIX   = "index-";                     // :203
private static final String DATA_BLOB_PREFIX        = "__";                         // :209
```

仓库里的典型布局：

```
<repo>/
├── index-0                       ← RepositoryData（第 0 代）
├── index-1                       ← RepositoryData（第 1 代）……
├── index.latest                  ← 指向当前有效 index-N
├── incompatible-snapshots        ← 不兼容版本的快照列表
├── meta-<snapshot-uuid>.dat      ← 全局元数据（cluster settings/模板）
├── snap-<snapshot-uuid>.dat      ← SnapshotInfo（快照描述）
├── indices/
│   └── <index-id>/
│       ├── meta-<snapshot-uuid>.dat       ← 索引元数据
│       └── <shard-id>/
│           ├── snap-<snapshot-uuid>.dat   ← 该分片的文件清单
│           └── __<hash>                   ← 实际数据 blob（segment 文件）
```

**世代机制（generation）**：

- `RepositoryData.getGenId()`（`RepositoryData:107`）返回当前 `index-N` 的 N。
- 每次增/删快照 → 写 `index-(N+1)` → 更新 `index.latest`。
- `EMPTY_REPO_GEN = -1L`（`:54`）表示空仓库。

> **为什么用世代而不是覆盖**：多个节点可能并发写仓库。
> "写新文件 + 原子改名 index.latest"是无锁并发控制的标准做法（和 Gateway 的 `.st` 文件同理）。

`RepositoryData` 的内容（`snapshotsToXContent:311`）：`snapshots`（快照 id → 状态）、`indices`（索引名 → IndexId）、`incompatible-snapshots`。

### 5.3 `IndexId`：为什么索引有"仓库内 id"

`RepositoryData.resolveIndexId`（`:258`）/ `resolveIndices`（`:273`）/ `resolveNewIndices`（`:285`）：
仓库里用**不可变的 IndexId（uuid 形式）**而非索引名 —— 这样重命名索引后快照依然能对上。

---

## 6. Snapshot：创建流程

### 6.1 集群状态先行

`SnapshotsService.createSnapshot`（`:234`）：

```java
clusterService.submitStateUpdateTask(...)   // :243
    private SnapshotsInProgress.Entry newSnapshot = null;
    public ClusterState execute(ClusterState currentState) { ... }     // :246
    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) { ... }  // :282
```

`SnapshotsInProgress`（`cluster/SnapshotsInProgress.java`）：

```java
public static final String TYPE = "snapshots";                          // :49
public static final long UNDEFINED_REPOSITORY_STATE_ID = -2L;           // :53
public static class Entry { ... }                                       // :86
    // snapshot, includeGlobalState, partial, state, indices, startTime, repositoryStateId...
public static boolean completed(ObjectContainer<ShardSnapshotStatus> shards) { ... }   // :230
public static class ShardSnapshotStatus { ... }                         // :240
```

> **关键**：快照的进度存在 **ClusterState** 里（`SnapshotsInProgress`），
> 不是存在仓库里。所以 `GET /_snapshot/_status` 是从集群状态读的。

### 6.2 `initializeSnapshot`

`BlobStoreRepository.initializeSnapshot`（`:387`）：

```java
if (isReadOnly()) throw new RepositoryException(metadata.name(),
    "cannot create snapshot in a readonly repository");                 // :389

final RepositoryData repositoryData = getRepositoryData();
// ① 名字重复检查（两道）
if (repositoryData.getAllSnapshotIds().stream().anyMatch(s -> s.getName().equals(snapshotName))) {
    throw new InvalidSnapshotNameException(metadata.name(), snapshotId.getName(),
        "snapshot with the same name already exists");                  // :396
}
if (snapshotFormat.exists(blobContainer(), snapshotId.getUUID())) {
    throw new InvalidSnapshotNameException(...);                        // :399
}

// ② 写全局元数据
globalMetaDataFormat.write(clusterMetaData, blobContainer(), snapshotId.getUUID());   // :403

// ③ 逐个索引写元数据
for (IndexId index : indices) {
    final IndexMetaData indexMetaData = clusterMetaData.index(index.getName());
    final BlobPath indexPath = basePath().add("indices").add(index.getId());
    final BlobContainer indexMetaDataBlobContainer = blobStore().blobContainer(indexPath);
    indexMetaDataFormat.write(indexMetaData, indexMetaDataBlobContainer, snapshotId.getUUID());  // :410
}
```

### 6.3 数据节点侧：分片快照

`SnapshotShardsService`（`:99`）实现 `ClusterStateListener` + `IndexEventListener`：

```java
public static final String UPDATE_SNAPSHOT_STATUS_ACTION_NAME_V6 = "internal:cluster/snapshot/update_snapshot";       // :102
public static final String UPDATE_SNAPSHOT_STATUS_ACTION_NAME    = "internal:cluster/snapshot/update_snapshot_status";// :103
private volatile Map<Snapshot, Map<ShardId, IndexShardSnapshotStatus>> shardSnapshots = emptyMap();                  // :121
```

- `clusterChanged`（`:182`）→ `processIndexShardSnapshots`（`:233`）→ `snapshot(...)`（`:390`）。
- `syncShardStatsOnNewMaster`（`:427`）：新 master 上任时同步进度。
- `beforeIndexShardClosed`（`:203`）：分片关闭时中止快照。

**增量快照的原理**：快照复用 Lucene 的 **`IndexCommit`**（`snapshotShard:851` 的参数 `SnapshotId, IndexId, IndexCommit`）。
Lucene 的 segment 文件是**不可变**的，所以：

- 第二次快照时，已存在的 segment 文件**直接引用**，不重复上传。
- 只有新生成的（merge 后的）segment 需要上传。

> **这就是"频繁快照不占额外空间"的原因**，也是"删除旧快照后空间可能不立刻释放"的原因
> （其他快照可能还在引用同一个 blob）。

### 6.4 `finalizeSnapshot`

`BlobStoreRepository.finalizeSnapshot`（`:538`）：

```java
SnapshotInfo blobStoreSnapshot = new SnapshotInfo(snapshotId,
    indices.stream().map(IndexId::getName).collect(Collectors.toList()),
    startTime, failure, System.currentTimeMillis(), totalShards, shardFailures, includeGlobalState);   // :546
try {
    snapshotFormat.write(blobStoreSnapshot, blobContainer(), snapshotId.getUUID());   // :551
    final RepositoryData repositoryData = getRepositoryData();
    writeIndexGen(repositoryData.addSnapshot(snapshotId, blobStoreSnapshot.state(), indices), repositoryStateId);  // :553
} catch (FileAlreadyExistsException ex) {
    // if another master was elected and took over finalizing the snapshot, it is possible
    // that both nodes try to finalize the snapshot and write to the same blobs, so we just
    // log a warning here and carry on
    throw new RepositoryException(metadata.name(), "Blob already exists while " +
        "finalizing snapshot, assume the snapshot has already been saved", ex);       // :558
}
```

`blobStoreSnapshot.state()` 决定快照最终状态：

| state | 含义 |
| --- | --- |
| `SUCCESS` | 全部成功 |
| `PARTIAL` | 部分分片失败（`partial=true` 才允许） |
| `FAILED` | 失败 |
| `IN_PROGRESS` | 进行中 |

---

## 7. Snapshot：删除流程

`BlobStoreRepository.deleteSnapshot`（`:418`）：

```java
if (isReadOnly()) throw new RepositoryException(metadata.name(),
    "cannot delete snapshot from a readonly repository");               // :420

final RepositoryData repositoryData = getRepositoryData();
SnapshotInfo snapshot = null;
try {
    snapshot = getSnapshotInfo(snapshotId);
} catch (SnapshotMissingException ex) {
    throw ex;
} catch (IllegalStateException | SnapshotException | ElasticsearchParseException ex) {
    logger.warn("cannot read snapshot file [{}]", snapshotId);          // :430  ★ 读不出来也继续删
}

try {
    // Delete snapshot from the index file, since it is the maintainer of truth of active snapshots
    final RepositoryData updatedRepositoryData = repositoryData.removeSnapshot(snapshotId);   // :435
    writeIndexGen(updatedRepositoryData, repositoryStateId);                                  // :436
    ...
```

**顺序很重要**：**先从 `index-N` 里移除**（它是"真相来源"），再删 blob。
这样即使中途崩溃，也只是留下孤儿 blob（可清理），不会出现"index-N 里有快照但数据没了"。

> **注意**：`getSnapshotInfo` 失败只是 warn 并继续（`:430`）——
> 这是**故意的设计**，让损坏的快照也能被删掉。

---

## 8. Snapshot：恢复流程

`RestoreService.restoreSnapshot`（`snapshots/RestoreService.java:175`）：

```java
clusterService.submitStateUpdateTask(...)                             // :226
    public ClusterState execute(ClusterState currentState) { ... }
    private void checkAliasNameConflicts(...)                         // :390
    private void populateIgnoredShards(String index, IntSet ignoreShards)  // :398
    private boolean checkPartial(String index)                        // :406
    private void validateExistingIndex(IndexMetaData currentIndexMetaData,
        IndexMetaData snapshotIndexMetaData, String renamedIndex, boolean partial)   // :419
    private IndexMetaData updateIndexSettings(IndexMetaData indexMetaData,
        Settings changeSettings, String[] ignoreSettings)             // :441
```

不可修改的 setting（`UNMODIFIABLE_SETTINGS:122`、`UNREMOVABLE_SETTINGS:130`）：
比如 `index.number_of_shards`、`index.uuid` 等 —— 恢复时可以改副本数，不能改主分片数。

恢复状态：

- `RestoreInProgress`（`ClusterState.Custom`）
- `RestoreService.RestoreInProgressUpdater extends RoutingChangesObserver`（`:565`）
  监听 `shardStarted`（`:569`）和 `shardFailed`（`:582`）来推进恢复进度。

**恢复的本质**：创建索引（用快照里的 `IndexMetaData`）→ 分片进入 recovery →
从仓库下载 segment 文件（`BlobStoreRepository.restoreShard:867`）→ 分片 STARTED。
**复用了第 18 章的 recovery 框架**。

---

## 9. 限流与配置

`BlobStoreRepository`（`:250-251`）：

```java
snapshotRateLimiter = getRateLimiter(metadata.settings(),
    "max_snapshot_bytes_per_sec", new ByteSizeValue(40, ByteSizeUnit.MB));    // :250
restoreRateLimiter  = getRateLimiter(metadata.settings(),
    "max_restore_bytes_per_sec",  new ByteSizeValue(40, ByteSizeUnit.MB));    // :251
```

默认 **40MB/s**（快照和恢复分别限流）。
`getRateLimiter`（`:602`）：`<= 0` 时返回 null（不限流）。

其它：

| 配置 | 位置 | 默认 |
| --- | --- | --- |
| `chunk_size` | `FsRepository.CHUNK_SIZE_SETTING:59` | 见源码（默认较大） |
| `compress` | `FsRepository.COMPRESS_SETTING:63` | `false`（**只压缩元数据，不压缩数据**） |
| `location` | `FsRepository.LOCATION_SETTING:55` | 必填 |
| `repositories.fs.*` | `:57`/`:61`/`:64` | 全局默认值（必须在 `elasticsearch.yml` 里配，因为要过 `SecurityManager`） |

> **`path.repo` / `repositories.fs.location` 必须在配置文件里声明**，
> 这是安全沙箱的要求（防止快照写到任意路径）。
> 动态 `cluster settings` 设置不了 → 报错 `repository location [...] is not registered`。

---

## 10. 排障手册

### 10.1 Gateway

| 现象 / 报错 | 原因 | 位置 |
| --- | --- | --- |
| `state not recovered / initialized` | 未满足 `gateway.expected_*` 或 `recover_after_time` 未到 | `GatewayService.java:70`/`:158` |
| `found [N] metadata states, required [M]` | 有元数据的 master 节点数 < `minimum_master_nodes` | `Gateway.java:94` |
| `recovering index X failed - recovering as closed` | 索引元数据校验失败（常因缺插件） | `Gateway.java:133-134` |
| `CorruptStateException` | `.st` 文件校验和不匹配 | `MetaDataStateFormat.read` |
| 重启后 setting / 模板丢失 | 没有任何 master 节点写成功；或 `disableStatePersistence` | `GatewayMetaState.java:121` |
| closed 索引元数据在 data 节点丢失 | 见源码注释的边界情况 | `GatewayMetaState.java:136-155` |

```bash
# 看恢复进度
grep "recovering from gateway\|delaying initial state recovery\|successful state recovery" logs/*.log

# 看节点的本地元数据
ls data/nodes/0/_state/            # global-N.st
ls data/nodes/0/indices/*/_state/  # state-N.st

# 悬挂索引
GET /_dangling
```

### 10.2 Snapshot

| 现象 / 报错 | 原因 | 位置 |
| --- | --- | --- |
| `snapshot with the same name already exists` | 重名 | `BlobStoreRepository.java:396` |
| `cannot create snapshot in a readonly repository` | 仓库只读（`url` 类型） | `:389` |
| `found [2] metadata states, required ...`（快照的） | 见 Gateway | |
| 快照一直 `IN_PROGRESS` | `SnapshotsInProgress` 没被清理（master 切换等） | `SnapshotsInProgress.java:49` |
| `Blob already exists while finalizing snapshot` | 两个 master 同时 finalize | `:558` |
| `repository location [...] is not registered` | 未在 `elasticsearch.yml` 里声明 `path.repo` | `FsRepository.LOCATION_SETTING:55` |
| 恢复时 `cannot restore index [...] because it is open` | 同名索引已存在且未关闭 | `RestoreService.validateExistingIndex:419` |
| 删除快照后空间没释放 | blob 被其它快照引用（增量共享） | `deleteSnapshot:418` |
| `aborted snapshot` | 分片在快照期间被关闭/relocate | `SnapshotShardsService.beforeIndexShardClosed:203` |

```bash
# 状态
GET /_snapshot/_status
GET /_snapshot/my_repo/_all?verbose=false
GET /_cat/snapshots/my_repo?v

# 清理孤儿（6.6 用 _cleanup）
POST /_snapshot/my_repo/_cleanup

# 验证所有节点都能访问仓库
POST /_snapshot/my_repo/_verify

# 取消正在跑的快照
DELETE /_snapshot/my_repo/my_snap
```

### 10.3 备份策略建议（云服务）

1. **仓库要共享**：所有 master + data 节点都能访问（用 `_verify` 验证）。
2. **限流默认 40MB/s 很保守**，备份窗口紧可以适当调大，但要避免打满网络。
3. **SLM（快照生命周期管理）**：6.6 还没有内置 SLM（7.x 才有），
   需要用外部定时任务 + 脚本管理保留策略。
4. **删除旧快照要清理**：定期跑 `_cleanup` 回收孤儿 blob。
5. **跨大版本不能回滚**：快照只能恢复到**相同或更高**版本，且 6.x → 7.x 需要额外处理。
6. **恢复前先关索引**：恢复到已有索引必须先 `close` 或 `delete`。
7. **`partial` 快照慎用**：`partial=true` 的快照恢复时可能缺数据。

---

## 11. 动手实验

### 实验 1：观察 Gateway 落盘

```bash
# 启动单节点
./gradlew :distribution:archives:oss-zip:assemble   # 或直接用解压的发行版

# 建索引 / 改模板
curl -XPUT "localhost:9200/t1" -H 'Content-Type: application/json' -d '{"settings":{"number_of_shards":1}}'

# 看落盘
find data/nodes/0 -name "_state" -type d
find data/nodes/0/_state -type f          # global-N.st
find data/nodes/0/indices -name "*.st"    # state-N.st

# 用 xxd 看文件头（应该是 Lucene codec "state"）
xxd data/nodes/0/_state/global-1.st | head -3
```

### 实验 2：触发恢复失败

```bash
# 起 3 个 master 节点，minimum_master_nodes=2
# 停掉 2 个，删掉其中 1 个的 data 目录，启动
# → 日志里出现 "found [1] metadata states, required [2]"
```

### 实验 3：快照

```bash
# 1. 配置文件里声明仓库根目录
echo "path.repo: [/tmp/es-backup]" >> config/elasticsearch.yml

# 2. 注册仓库
PUT /_snapshot/my_repo
{"type":"fs","settings":{"location":"/tmp/es-backup","compress":true}}

# 3. 验证
POST /_snapshot/my_repo/_verify

# 4. 建快照（只快照索引，不带全局状态）
PUT /_snapshot/my_repo/snap1?wait_for_completion=true
{"indices":"t1","include_global_state":false}

# 5. 看仓库内容
ls -R /tmp/es-backup
cat /tmp/es-backup/index-0 | python -m json.tool

# 6. 恢复（改名）
POST /_snapshot/my_repo/snap1/_restore
{"indices":"t1","rename_pattern":"t1","rename_replacement":"t1_restored"}

# 7. 状态
GET /_snapshot/_status
```

### 实验 4：断点清单

| 断点 | 目的 |
| --- | --- |
| `MetaDataStateFormat.write`（`:104`） | 看 tmp + ATOMIC_MOVE + fsync |
| `GatewayMetaState.applyClusterState`（`:170`） | 看哪些索引被写 |
| `GatewayMetaState.getRelevantIndices`（`:189`） | 看 master/data 的差异 |
| `GatewayService.clusterChanged`（`:166`） | 看法定节点数判断 |
| `GatewayService.performStateRecovery`（`:208`） | 看定时恢复 |
| `Gateway.performStateRecovery`（`:87`/`:117`） | 看全局/索引元数据选举 |
| `GatewayService.GatewayRecoveryListener.onSuccess`（`:249`） | 看 block 解除 |
| `BlobStoreRepository.initializeSnapshot`（`:403`） | 看元数据写入 |
| `BlobStoreRepository.finalizeSnapshot`（`:551`） | 看 index-N 更新 |
| `BlobStoreRepository.deleteSnapshot`（`:435`） | 看"先改 index-N"的顺序 |
| `SnapshotShardsService.processIndexShardSnapshots`（`:233`） | 看分片级快照 |
| `RestoreService.restoreSnapshot`（`:226`） | 看恢复的状态更新 |

### 实验 5：单元测试

```bash
./gradlew :server:test --tests "org.elasticsearch.gateway.*"
./gradlew :server:test --tests "org.elasticsearch.repositories.blobstore.*"
./gradlew :server:test --tests "org.elasticsearch.snapshots.*"
./gradlew :server:test --tests "org.elasticsearch.gateway.GatewayMetaStateTests"
```

---

## 12. 自测题（答案都在本章）

1. Gateway 和 Snapshot 的本质区别是什么？各自持久化什么内容？
2. 元数据文件 `.st` 的格式是什么？用什么 XContent 类型？
3. `MetaDataStateFormat.write` 怎么保证"不会出现半个文件"？做了几次 fsync？
4. 多个 data path 时，元数据是怎么写的？
5. 哪些节点会写元数据？master 节点和纯 data 节点写的范围有什么不同？
6. closed 索引的元数据在 data 节点上有什么坑？源码为什么要特殊处理？
7. `STATE_NOT_RECOVERED_BLOCK` 是什么？什么时候加、什么时候解？
8. `gateway.expected_nodes` 和 `gateway.recover_after_nodes` 有什么区别？
9. 什么情况下 `enforceRecoverAfterTime` 会是 true？
10. `Gateway.performStateRecovery` 里 `requiredAllocation` 是怎么算的？不足会怎样？
11. 全局元数据和索引元数据是分别选举的吗？各自的依据是什么？
12. 索引元数据校验失败时会怎样？日志里会看到什么？
13. 集群重启后"索引全部消失"最可能的原因是什么？怎么排查？
14. 什么是 dangling index？怎么产生？怎么处理？
15. `BlobStoreRepository` 和 `Repository` 的关系？6.6.1 有哪几种内置仓库类型？
16. 仓库里的 `index-N`、`index.latest`、`snap-*.dat`、`meta-*.dat` 分别是什么？
17. 为什么要设计"世代（generation）"而不是覆盖同一个文件？
18. `IndexId` 是什么？为什么仓库里不用索引名？
19. 快照的进度存在哪里？`GET /_snapshot/_status` 的数据从哪来？
20. `initializeSnapshot` 做了哪几件事？重名检查做了几次？
21. 为什么说快照是"增量"的？底层依赖 Lucene 的什么特性？
22. 删除一个旧快照后，仓库空间为什么可能不减少？
23. `deleteSnapshot` 为什么"先从 index-N 移除，再删 blob"？反过来会有什么问题？
24. `getSnapshotInfo` 失败时删除会中断吗？为什么这样设计？
25. 快照有哪些最终状态（`state`）？`PARTIAL` 是怎么产生的？
26. 恢复流程的本质是什么？它复用了哪个章节的框架？
27. 恢复时哪些 setting 不能改？`UNMODIFIABLE_SETTINGS` 是什么？
28. `max_snapshot_bytes_per_sec` 和 `max_restore_bytes_per_sec` 默认多少？设为 0 会怎样？
29. `compress` 默认是什么？它压缩数据文件吗？
30. 为什么 `path.repo` 必须写在 `elasticsearch.yml` 里而不能动态设置？
31. 6.6.1 有内置 SLM（快照生命周期管理）吗？没有的话怎么办？
32. 快照能恢复到低版本吗？跨大版本要注意什么？
33. 生产上你会怎么设计备份策略？（仓库、频率、保留、限流、验证）

---

## 13. 关键类速查表

| 类 | 路径 | 关键行 |
| --- | --- | --- |
| `MetaDataStateFormat` | `gateway/MetaDataStateFormat.java` | `FORMAT(SMILE):65`、`STATE_DIR_NAME:66`、`STATE_FILE_EXTENSION:67`、`STATE_FILE_CODEC:69`、`STATE_FILE_VERSION:71`、**`write:97`**（maxStateId `:104`、tmp `:109`、header `:115`、footer `:130`、fsync `:132`、ATOMIC_MOVE `:133`、fsync 目录 `:134`、多路径 `:136-152`、清理 `:157`）、`toXContent:168`、`fromXContent:174`、`read:180`、`loadLatestState:257`、`deleteMetaState:334` |
| `MetaStateService` | `gateway/MetaStateService.java` | `ctor:47`、`loadIndexState:80`、`writeIndex:120` |
| `GatewayMetaState` | `gateway/GatewayMetaState.java` | `previousMetaData:69`、`previouslyWrittenIndices:71`、`loadMetaState:113`、**`applyClusterState:118`**（disableStatePersistence `:121`、master/data 判断 `:133`、closed 索引特判 `:142-155`、global `:161-167`、relevantIndices `:170`、writeIndex `:176`、更新缓存 `:184`）、**`getRelevantIndices:189`**、`isDataOnlyNode:202`、`ensureNoPre019State:209`、`upgradeMetaData:239`、`resolveStatesToBeWritten:314`、`getRelevantIndicesOnDataOnlyNode:335`、`getRelevantIndicesForMasterEligibleNode:362`、`IndexMetaWriteInfo:373` |
| `GatewayService` | `gateway/GatewayService.java` | `EXPECTED_NODES:55`、`EXPECTED_DATA_NODES:57`、`EXPECTED_MASTER_NODES:59`、`RECOVER_AFTER_TIME:61`、`RECOVER_AFTER_NODES:63`、`RECOVER_AFTER_DATA_NODES:65`、`RECOVER_AFTER_MASTER_NODES:67`、**`STATE_NOT_RECOVERED_BLOCK:70`**、`DEFAULT_RECOVER_AFTER_TIME:73`、**`clusterChanged:147`**（非 master `:154`、已恢复 `:158`、节点数 `:166-174`、enforce 判断 `:176-197`）、**`performStateRecovery:202`**（定时 `:208`）、`GatewayRecoveryListener:235`（解除 block `:249`、`generateClusterUuidIfNeeded:253`、read_only `:257`、加索引 block `:266`） |
| `Gateway` | `gateway/Gateway.java` | **`performStateRecovery:61`**（拉取 `:64`、**`requiredAllocation:67`**、全局选举 `:84-88`、统计 `:90`、**不足失败 `:94`**、removeAllIndices `:98`、索引选举 `:115-121`、票数不足 `:124`、verifyIndexMetadata `:129`、**降级 CLOSED `:134`**）、`upgradeAndArchiveUnknownOrInvalidSettings:145`、`GatewayStateRecoveredListener:171` |
| `BlobStoreRepository` | `repositories/blobstore/BlobStoreRepository.java` | `SNAPSHOT_PREFIX:183`、`INDEX_FILE_PREFIX:187`、`INDEX_LATEST_BLOB:189`、`INCOMPATIBLE_SNAPSHOTS_BLOB:191`、`METADATA_NAME_FORMAT:195`、`SNAPSHOT_NAME_FORMAT:201`、`SNAPSHOT_INDEX_PREFIX:203`、`DATA_BLOB_PREFIX:209`、**限流默认 40MB `:250/251`**、`createBlobStore:354`（抽象）、`basePath:359`（抽象）、`isCompress:366`、`chunkSize:377`、**`initializeSnapshot:387`**（只读 `:389`、重名 `:396/399`、全局元数据 `:403`、索引元数据 `:410`）、**`deleteSnapshot:418`**（只读 `:420`、warn 继续 `:430`、**先改 index-N `:435`**）、**`finalizeSnapshot:538`**（SnapshotInfo `:546`、写 `:551`、writeIndexGen `:553`、FileAlreadyExists `:558`）、`getSnapshotInfo:567`、`getSnapshotGlobalMetaData:578`、`getSnapshotIndexMetaData:589`、`getRateLimiter:602`、`getRepositoryData:663`、`writeIndexGen:721`、`snapshotShard:851`、`restoreShard:867`、`getShardSnapshotStatus:877`、`verify:886` |
| `RepositoryData` | `repositories/RepositoryData.java` | `EMPTY_REPO_GEN:54`、`EMPTY:58`、`getGenId:107`、`getSnapshotIds:114`、`getIncompatibleSnapshotIds:122`、`getAllSnapshotIds:130`、`getSnapshotState:142`、`getIndices:149`、`addSnapshot:157`、`removeSnapshot:191`、`getSnapshots:225`、`resolveIndexId:258`、`resolveIndices:273`、`resolveNewIndices:285`、`snapshotsToXContent:311`、`snapshotsFromXContent:347` |
| `RepositoriesService` | `repositories/RepositoriesService.java` | `registerRepository:96`、`unregisterRepository:179`、`verifyRepository:223`、`applyClusterState:285`、`repository:358`、`registerInternalRepository:370` |
| `RepositoriesModule` | `repositories/RepositoriesModule.java` | `ctor:47`（internalFactories `:61`、重名检查 `:65`） |
| `FsRepository` | `repositories/fs/FsRepository.java` | `TYPE:53`、`LOCATION_SETTING:55`、`REPOSITORIES_LOCATION_SETTING:57`、`CHUNK_SIZE_SETTING:59`、`REPOSITORIES_CHUNK_SIZE_SETTING:61`、`COMPRESS_SETTING:63`、`REPOSITORIES_COMPRESS_SETTING:64` |
| `SnapshotsService` | `snapshots/SnapshotsService.java` | `class:107`、`getRepositoryData:140`、`snapshot:154`、`snapshots:172`、`currentSnapshots:215`、**`createSnapshot:234`**（execute `:246`、clusterStateProcessed `:282`）、`validate:302/311`、`beginSnapshot:347`、`inProgressSnapshot:531`、`currentSnapshots:547`、`snapshotShards:604`、`applyClusterState:655`、`finalizeSnapshotDeletionFromPreviousMaster:684`、`removeFinishedSnapshotFromClusterState:702`、`processSnapshotsOnRemovedNodes:720`、`processStartedShards:798` |
| `SnapshotShardsService` | `snapshots/SnapshotShardsService.java` | `UPDATE_SNAPSHOT_STATUS_ACTION_NAME_V6:102`、`UPDATE_SNAPSHOT_STATUS_ACTION_NAME:103`、`shardSnapshots:121`、`clusterChanged:182`、`beforeIndexShardClosed:203`、`currentSnapshotShards:224`、`processIndexShardSnapshots:233`、**`snapshot:390`**、`syncShardStatsOnNewMaster:427` |
| `RestoreService` | `snapshots/RestoreService.java` | `UNMODIFIABLE_SETTINGS:122`、`UNREMOVABLE_SETTINGS:130`、**`restoreSnapshot:175`**（execute `:226`、`checkAliasNameConflicts:390`、`populateIgnoredShards:398`、`checkPartial:406`、`validateExistingIndex:419`、`updateIndexSettings:441`）、`updateRestoreStateWithDeletedIndices:512`、`RestoreCompletionResponse:541`、`RestoreInProgressUpdater:565`（`shardStarted:569`、`shardFailed:582`） |
| `SnapshotsInProgress` | `cluster/SnapshotsInProgress.java` | `TYPE:49`、`UNDEFINED_REPOSITORY_STATE_ID:53`、`Entry:86`、`completed:230`、`ShardSnapshotStatus:240` |

---

**上一篇**：[22-Ingest管道与Script脚本.md](./22-Ingest管道与Script脚本.md)
**下一篇**：[24-监控Stats与内存保护.md](./24-监控Stats与内存保护.md)
