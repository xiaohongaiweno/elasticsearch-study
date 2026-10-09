# 18 · 分片恢复 Recovery（peer / store / snapshot / local shards）

> 目标：把「一个 `INITIALIZING` 分片是怎么变成 `STARTED` 的」讲清楚 —— 从 `IndexShard.startRecovery` 的分派，
> 到 peer recovery 的 phase1/phase2/finalize 三步，再到**为什么 6.x 多数时候只传 translog 而不传 segment 文件**。
>
> **为什么必须学**：
> - 「节点重启后集群 yellow 了半小时」→ recovery 限速 / 全量文件拷贝 / translog 重放。
> - 「副本恢复为什么只传了增量？」→ sequence-number based recovery（第 17 章的 seqno 模型直接决定了它）。
> - 「为什么恢复中的副本已经开始收写请求了？」→ `initiateTracking` + `tracked ≠ inSync`。
> - 「恢复卡在 `stage=index` / `stage=translog` 很久」→ 需要看懂 `RecoveryState.Stage` 各阶段在干什么。
> - 「shrink / split / 快照恢复为什么走不同的路」→ `RecoverySource.Type` 五选一。
> - 云服务的**扩缩容、节点替换、快照回滚、主分片迁移**全部落在本章。
>
> **前置**：第 14 章（Translog）、第 15 章（Engine / Lucene）、第 **17 章**（SeqNo 一致性模型，本篇大量依赖）。
> **后续**：第 19 章 `19-检索流程-QueryThenFetch与DFS.md`（进入阶段 4：读取期）。

---

## 1. 一图总览：恢复的六种入口

```
                       IndexShard.startRecovery(recoveryState, ...)     IndexShard.java:2155
                                        │
                     switch (recoveryState.getRecoverySource().getType())   :2176
        ┌──────────────┬────────────────┼───────────────┬─────────────────┐
        │              │                │               │                 │
   EMPTY_STORE    EXISTING_STORE       PEER         SNAPSHOT        LOCAL_SHARDS
   (新建索引)      (重启/主恢复)      (从主分片)    (快照恢复)     (shrink/split)
        │              │                │               │                 │
        └──────────────┴── storeRecovery.recoverFromStore(this)             │
                       │   StoreRecovery.java:88                            │
                       │   └ internalRecoverFromStore(indexShard)  :357     │
                       │      ├ prepareForIndexRecovery()                   │
                       │      ├ openEngineAndRecoverFromTranslog() :1392    │
                       │      └ recoverFromTranslog(LOCAL_TRANSLOG_RECOVERY)│
                       │                                                    │
                       │   PeerRecoveryTargetService.startRecovery  :2194   │
                       │      └ doRecovery()  PeerRecoveryTargetService:165 │
                       │         → 源节点 RecoverySourceHandler             │
                       │  ┌────────────────────────────────────────────┐   │
                       │  │ recoverToTarget()  RecoverySourceHandler:135│   │
                       │  │  ① 校验 target 在复制组里                    │   │
                       │  │  ② 判定 seqno-based / file-based            │   │
                       │  │     · file-based → phase1（拷 segment 文件） │   │
                       │  │  ③ prepareTargetForTranslog（开 engine）     │   │
                       │  │  ④ initiateTracking（开始收增量写）           │   │
                       │  │  ⑤ phase2（发 translog 历史操作）             │   │
                       │  │  ⑥ finalizeRecovery（mark in-sync）          │   │
                       │  └────────────────────────────────────────────┘   │
                       │                                                    │
                       │   restoreFromRepository(repository)  :1692         │
                       │                                                    │
                       └───────── recoverFromLocalShards(...) :1662 ────────┘
```

---

## 2. 必读源码清单

| 类 | 路径 | 作用 |
| --- | --- | --- |
| `RecoverySource` | `cluster/routing/RecoverySource.java` | ★ 恢复来源类型（`:86` `Type` 枚举） |
| `IndexShard` | `index/shard/IndexShard.java` | 恢复入口 `startRecovery:2155`；`recoverFromStore:1683`、`recoverFromLocalShards:1662`、`restoreFromRepository:1692` |
| `StoreRecovery` | `index/shard/StoreRecovery.java` | 本地恢复（`:88`/`:101`/`:274`/`:357`） |
| `PeerRecoveryTargetService` | `indices/recovery/PeerRecoveryTargetService.java` | ★ 目标侧调度、重试、`getStartingSeqNo:358` |
| `RecoveryTarget` | `indices/recovery/RecoveryTarget.java` | 目标侧动作实现（接收文件块、重放 translog） |
| `PeerRecoverySourceService` | `indices/recovery/PeerRecoverySourceService.java` | 源侧 transport handler（`:110`） |
| `RecoverySourceHandler` | `indices/recovery/RecoverySourceHandler.java` | ★★ 源侧主流程（`:135` `recoverToTarget`） |
| `RecoveryState` | `indices/recovery/RecoveryState.java` | ★ 阶段机 `Stage:51` + 统计（`Index`/`Translog`/`VerifyIndex`/`Timer`） |
| `RecoverySettings` | `indices/recovery/RecoverySettings.java` | 限速 / chunk / 超时（`:38~88`） |
| `StartRecoveryRequest` | `indices/recovery/StartRecoveryRequest.java` | 目标→源的请求（携带本地 `MetadataSnapshot` + `startingSeqNo`） |
| `Store.MetadataSnapshot` | `index/store/Store.java` | 文件级 diff：`recoveryDiff:979`、`RecoveryDiff:1094` |
| `CombinedDeletionPolicy` | `index/engine/CombinedDeletionPolicy.java` | `findSafeCommitPoint:153`（找"安全" commit） |

---

## 3. 恢复来源：`RecoverySource.Type`

```86:92:cluster/routing/RecoverySource.java
```

| 类型 | 实现类 | 场景 |
| --- | --- | --- |
| `EMPTY_STORE` | `EmptyStoreRecoverySource` | 新索引刚创建、或分片是全新的（无本地数据） |
| `EXISTING_STORE` | `ExistingStoreRecoverySource` | 节点重启后主分片从本地磁盘恢复（`_state` + translog） |
| `PEER` | `PeerRecoverySource`（`:304`） | **副本从主分片恢复** / 主分片 relocation（最常见） |
| `SNAPSHOT` | `SnapshotRecoverySource`（`:211`） | 从快照仓库恢复（restore / 从快照分配副本） |
| `LOCAL_SHARDS` | `LocalShardsRecoverySource` | `shrink` / `split`：从同节点的其它分片硬链接/拷贝 |

分派代码 `IndexShard.startRecovery`（`IndexShard.java:2155-2266`）。几个细节：

- `EMPTY_STORE` / `EXISTING_STORE` / `SNAPSHOT` / `LOCAL_SHARDS` 都是 `markAsRecovering(...)` 之后扔到 **`threadPool.generic()`** 异步执行。
- `PEER` 是**同步**调 `recoveryTargetService.startRecovery(...)`（`:2194`），因为它本身内部就是异步的。
- `LOCAL_SHARDS` 会先检查源索引的**所有必需分片都已 STARTED**（`:2237`），否则抛
  `"not all required shards of index ... are started yet"`（`:2256`）—— shrink 时常见的报错。

---

## 4. 本地恢复（store recovery）

入口 `IndexShard.recoverFromStore()`（`IndexShard.java:1683`）→ `StoreRecovery.recoverFromStore(IndexShard)`（`StoreRecovery.java:88`）：

```java
boolean recoverFromStore(final IndexShard indexShard) {
    if (canRecover(indexShard)) {
        RecoverySource.Type recoveryType = indexShard.recoveryState().getRecoverySource().getType();
        assert recoveryType == EMPTY_STORE || recoveryType == EXISTING_STORE;      // :91
        return executeRecovery(indexShard, () -> {
            logger.debug("starting recovery from store ...");
            internalRecoverFromStore(indexShard);
        });
    }
    return false;
}
```

`canRecover`（`StoreRecovery.java:285` 附近）有个很关键的断言（`:293-296`）：

```java
if (indexShard.routingEntry().primary() == false) {
    throw new IndexShardRecoveryException(shardId, "Trying to recover when the shard is in backup state", null);
}
```

> **本地恢复只允许主分片**。副本永远走 `PEER`（或 `SNAPSHOT`），不会自己从本地 translog 重放。

### 4.1 `internalRecoverFromStore`（`StoreRecovery.java:357`）

```java
final boolean indexShouldExists = recoveryState.getRecoverySource().getType() != RecoverySource.Type.EMPTY_STORE;  // :359
indexShard.prepareForIndexRecovery();               // :360  stage = INDEX
try {
    store.failIfCorrupted();
    try {
        si = store.readLastCommittedSegmentsInfo(); // :369  读 segments_N
    } catch (Exception e) {
        if (indexShouldExists) {
            throw new IndexShardRecoveryException(shardId,
                "shard allocated for local recovery (post api), should exist, but doesn't, current files: " + files, e);   // :379
        }
    }
    if (si != null) {
        if (indexShouldExists) {
            version = si.getVersion();
        } else {
            // 本不该存在的数据（dangling / 残留）→ 直接清掉
            Lucene.cleanLuceneIndex(store.directory());   // :390
            si = null;
        }
    }
}
```

**排障点**：日志里的 `"shard allocated for local recovery ... should exist, but doesn't"` 意味着
`EXISTING_STORE` 类型但磁盘上读不到 `segments_N` —— 典型是**数据盘被清空 / 挂载点错误 / 索引目录被误删**。
此时该分片无法本地恢复，需要靠其它副本或快照。

### 4.2 打开 engine 并重放本地 translog

`IndexShard.openEngineAndRecoverFromTranslog()`（`IndexShard.java:1392`）：

```java
final RecoveryState.Translog translogRecoveryStats = recoveryState.getTranslog();
final Engine.TranslogRecoveryRunner translogRecoveryRunner = (engine, snapshot) -> {
    translogRecoveryStats.totalOperations(snapshot.totalOperations());
    translogRecoveryStats.totalOperationsOnStart(snapshot.totalOperations());
    return runTranslogRecovery(engine, snapshot, Engine.Operation.Origin.LOCAL_TRANSLOG_RECOVERY,
        translogRecoveryStats::incrementRecoveredOperations);
};
innerOpenEngineAndTranslog();                                  // :1400
final Engine engine = getEngine();
engine.initializeMaxSeqNoOfUpdatesOrDeletes();                 // :1402
engine.recoverFromTranslog(translogRecoveryRunner, Long.MAX_VALUE);   // :1403
```

`innerOpenEngineAndTranslog()`（`:1415`）内部有 4 个关键点：

```java
recoveryState.setStage(RecoveryState.Stage.VERIFY_INDEX);                          // :1419
if (Booleans.isTrue(checkIndexOnStartup) || "checksum".equals(checkIndexOnStartup)) {
    checkIndex();                                                                  // :1423
}
recoveryState.setStage(RecoveryState.Stage.TRANSLOG);                              // :1428
...
config.setEnableGcDeletes(false);                                                  // :1434
final String translogUUID = store.readLastCommittedSegmentsInfo().getUserData().get(Translog.TRANSLOG_UUID_KEY);
final long globalCheckpoint = Translog.readGlobalCheckpoint(translogConfig.getTranslogPath(), translogUUID);   // :1439
replicationTracker.updateGlobalCheckpointOnReplica(globalCheckpoint, "read from translog checkpoint");         // :1440
trimUnsafeCommits();                                                               // :1441 ★ 截断不安全 commit
```

**`trimUnsafeCommits()`（`:1460`）** 是本地恢复的"安全阀门"：

```java
final long globalCheckpoint = Translog.readGlobalCheckpoint(translogConfig.getTranslogPath(), translogUUID);
final long minRetainedTranslogGen = Translog.readMinTranslogGeneration(translogConfig.getTranslogPath(), translogUUID);
assertMaxUnsafeAutoIdInCommit();
store.trimUnsafeCommits(globalCheckpoint, minRetainedTranslogGen, indexSettings.getIndexVersionCreated());   // :1466
```

含义：**丢掉那些包含 `max_seq_no > global checkpoint` 的 commit** —— 因为那些操作没有经过集群确认，
一旦这个分片成为主分片，它们必须被丢弃（与第 17 章 §11.2 的 `resetEngineToGlobalCheckpoint` 是同一套逻辑）。

`assertSequenceNumbersInCommit()`（`:1469`）还要求 commit 里必须有
`local_checkpoint` / `max_seq_no` / `history_uuid` 三项（对应第 17 章 §3 的 `loadSeqNoInfoFromLuceneCommit`）。

---

## 5. Peer Recovery：目标侧（`PeerRecoveryTargetService`）

### 5.1 启动

`IndicesClusterStateService` 在集群状态变更里创建分片：

```
IndicesClusterStateService.createOrUpdateShards / applyNewShards
  └─ indicesService.createShard(shardRouting, recoveryState, recoveryTargetService,
        new RecoveryListener(shardRouting), repositoriesService, failedShardHandler, globalCheckpointSyncer);   :560
       └─ IndexShard.startRecovery(...)   →  case PEER: recoveryTargetService.startRecovery(...)   :2194
```

`PeerRecoveryTargetService.startRecovery`（`:135`）创建 `RecoveryTarget` 并注册到 `onGoingRecoveries`，
随后由 `RecoveryRunner` 在 `GENERIC` 线程池执行 `doRecovery(recoveryId)`（`:165`）。

### 5.2 `doRecovery`：先准备，再发请求

```java
request = getStartRecoveryRequest(recoveryTarget);                 // :180
recoveryTarget.indexShard().prepareForIndexRecovery();             // :182
...
cancellableThreads.execute(() -> responseHolder.set(
    transportService.submitRequest(request.sourceNode(),
        PeerRecoverySourceService.Actions.START_RECOVERY, request, ...).txGet()));   // :196
```

`prepareForIndexRecovery()`（`IndexShard.java:1282`）只是把 `recoveryState` 置为 `Stage.INDEX`。

### 5.3 关键决策：`getStartingSeqNo` —— 增量还是全量

```315:349:indices/recovery/PeerRecoveryTargetService.java
```

```java
final Store.MetadataSnapshot metadataSnapshot = getStoreMetadataSnapshot(recoveryTarget);   // :319
final long startingSeqNo;
if (metadataSnapshot.size() > 0) {
    startingSeqNo = getStartingSeqNo(logger, recoveryTarget);      // :324  本地有文件 → 试着算增量起点
} else {
    startingSeqNo = SequenceNumbers.UNASSIGNED_SEQ_NO;             // :326  空目录 → 只能全量
}
```

`getStartingSeqNo`（`:358`）是**整个 6.x recovery 里最精妙的一段**：

```java
final String translogUUID = store.readLastCommittedSegmentsInfo().getUserData().get(Translog.TRANSLOG_UUID_KEY);
final long globalCheckpoint = Translog.readGlobalCheckpoint(recoveryTarget.translogLocation(), translogUUID);  // :362
final List<IndexCommit> existingCommits = DirectoryReader.listCommits(store.directory());
final IndexCommit safeCommit = CombinedDeletionPolicy.findSafeCommitPoint(existingCommits, globalCheckpoint);  // :364
final SequenceNumbers.CommitInfo seqNoStats = Store.loadSeqNoInfo(safeCommit);                                // :365
if (seqNoStats.maxSeqNo <= globalCheckpoint) {
    assert seqNoStats.localCheckpoint <= globalCheckpoint;
    /*
     * Commit point 可用于 seqno-based recovery，因为它包含的最大 seqno 低于 global checkpoint
     * （即它不包含任何"可能不在主分片上"的操作）。恢复将从该 commit 里记录的 local checkpoint 之后开始。
     */
    return seqNoStats.localCheckpoint + 1;    // :381  ★ 增量起点
} else {
    return SequenceNumbers.UNASSIGNED_SEQ_NO; // :383  全量
}
```

**翻译成人话**：

1. 从本地 `translog.ckp` 里读出 `global checkpoint`（这是上次持久化下来的水位）。
2. 在所有本地 commit 里找一个 **safe commit** = `max_seq_no ≤ global_checkpoint` 的最新 commit
   （`CombinedDeletionPolicy.findSafeCommitPoint`，`:153`；判定见 `:179-192`）。
3. 如果找到了 → 本地数据里**没有**任何"未确认"的操作 → 可以只从 `local_checkpoint + 1` 补 translog。
4. 找不到 → 本地有"野"数据（比如上次写入没被确认就崩了）→ **必须全量拷文件**。

任何异常（`TranslogCorruptedException` / `IOException`）都退化为全量（`:385-392`），
注释里给的场景是：phase1 已经完成、translog 还没打开时发生网络分区，重试时看到磁盘上有文件就试图做增量 ——
此时 translog 可能已损坏，所以保守地退化为全量。

### 5.4 重试策略

`doRecovery` 的 `catch` 块（`PeerRecoveryTargetService.java:230-287`）按异常类型分别处理：

| 异常 | 行为 | 延迟 |
| --- | --- | --- |
| `ExecutionCancelledException` | 静默（恢复被取消） | — |
| `IllegalIndexShardStateException` / `IndexNotFoundException` / `ShardNotFoundException` | 重试 `"remote shard not ready"` | `retryDelayStateSync`（默认 500ms，`:47`） |
| `DelayRecoveryException` | 重试 | `retryDelayStateSync` |
| `ConnectTransportException` | 重试（网络抖动） | `retryDelayNetwork`（默认 5s，`:52`） |
| `AlreadyClosedException` | 失败，不重发 shard failure | — |
| 其它 | `failRecovery(..., true)` → 上报 master | — |

重试由 `retryRecovery`（`:140`）实现，且受 **`activityTimeout`**（默认 = `internal_action_long_timeout` = 30min，`:64-76`）限制。

### 5.5 目标侧动作：`RecoveryTarget`

| 方法 | 行号 | 做了什么 |
| --- | --- | --- |
| `prepareForTranslogOperations` | `:364` | ★ `indexShard().openEngineAndSkipTranslogRecovery()`（`:369`）—— 开 engine 但**跳过本地 translog 重放**（因为主分片会发过来） |
| `indexTranslogOperations` | `:392` | ★ 逐条 `applyTranslogOperation(..., Origin.PEER_RECOVERY)`（`:413`） |
| `receiveFileInfo` | `:429` | 记录 phase1 的文件明细到 `RecoveryState.Index` |
| `writeFileChunk` | `:491` | 写文件块到**带 recovery id 后缀的临时文件**（`:499` `openAndPutIndexOutput`） |
| `cleanFiles` | `:447` | `renameAllTempFiles()`（`:452`）→ `store.cleanupAndVerify(...)`（`:456`）→ 创建**空 translog**（`:458`） |
| `finalizeRecovery` | `:373` | ★ `updateGlobalCheckpointOnReplica(gcp, "finalizing recovery")` → `sync()` → `indexShard.finalizeRecovery()` |
| `handoffPrimaryContext` | `:387` | `indexShard.activateWithPrimaryContext(primaryContext)`（第 17 章 §5.8） |

`indexTranslogOperations` 里有两行很容易被忽略但非常重要（`:406`、`:411`）：

```java
indexShard().updateMaxUnsafeAutoIdTimestamp(maxSeenAutoIdTimestampOnPrimary);   // :406
indexShard().advanceMaxSeqNoOfUpdatesOrDeletes(maxSeqNoOfDeletesOrUpdatesOnPrimary);  // :411
```

注释（`:400-410`）解释了原因：主分片上的这两个值必须在副本上也"至少那么高"，
否则副本在重放这些操作时，append-only 优化 / msu 判定的依据就与主分片不符（见第 17 章 §9）。

`cleanFiles` 里还有一句 TODO（`:457`）：

```java
// TODO: Assign the global checkpoint to the max_seqno of the safe commit if the index version >= 6.2
final String translogUUID = Translog.createEmptyTranslog(
    indexShard.shardPath().resolveTranslog(), SequenceNumbers.UNASSIGNED_SEQ_NO, shardId,
    indexShard.getPendingPrimaryTerm());
```

**全量恢复时 translog 被重建为空**，且 `global checkpoint` 初始化为 `UNASSIGNED_SEQ_NO`。
这就是为什么全量恢复后，副本要等 `finalizeRecovery` 才拿到真正的 gcp。

如果 `cleanupAndVerify` 里 diff 不通过（`Store.java:667-690`），会抛
`"Files are missing on the recovery target"`（`:688`）或
`"local version ... is different from remote version after recovery"`（`:681`）——
**线上看到这两条，说明传输/校验出了问题**（如磁盘静默错误、网络篡改）。

---

## 6. Peer Recovery：源侧（`RecoverySourceHandler`）

源侧 transport handler：`PeerRecoverySourceService.StartRecoveryTransportRequestHandler`（`:110-116`）
→ `recover(request)`（`:90-108`）→ `new RecoverySourceHandler(shard, recoveryTarget, request, chunkSize)`（`:181`）
→ **`handler.recoverToTarget()`**（`RecoverySourceHandler.java:135`）。

### 6.1 第 0 步：校验目标

```java
final IndexShardRoutingTable routingTable = shard.getReplicationGroup().getRoutingTable();
ShardRouting targetShardRouting = routingTable.getByAllocationId(request.targetAllocationId());
if (targetShardRouting == null) {
    throw new DelayRecoveryException("source node does not have the shard listed in its state as allocated on the node");   // :142
}
assert targetShardRouting.initializing() : "expected recovery target to be initializing but was " + targetShardRouting;     // :144
```

这一段跑在 **`runUnderPrimaryPermit`**（`:240`）里 —— 必须持有主分片操作许可。
`runUnderPrimaryPermit` 在拿到 permit 之后还会再检查一次：

```java
if (primary.isRelocatedPrimary()) {
    throw new IndexShardRelocatedException(primary.shardId());      // :262
}
```

> **这是防止"旧主还在做 recovery"的关键**：主分片完成 relocation handoff 后 `isRelocatedPrimary()` 变 true，
> 任何在途恢复立刻失败。

### 6.2 第 1 步：判定增量 / 全量

```147:182:indices/recovery/RecoverySourceHandler.java
```

```java
try (Closeable ignored = shard.acquireRetentionLockForPeerRecovery()) {      // :147 ★ 保留锁
    final long startingSeqNo;
    final long requiredSeqNoRangeStart;
    final boolean isSequenceNumberBasedRecovery = request.startingSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO &&
        isTargetSameHistory() && shard.hasCompleteHistoryOperations("peer-recovery", request.startingSeqNo());   // :150-151
    if (isSequenceNumberBasedRecovery) {
        logger.trace("performing sequence numbers based recovery. starting at [{}]", request.startingSeqNo());
        startingSeqNo = request.startingSeqNo();
        requiredSeqNoRangeStart = startingSeqNo;
    } else {
        final Engine.IndexCommitRef phase1Snapshot;
        try {
            phase1Snapshot = shard.acquireSafeIndexCommit();                 // :159
        } catch (final Exception e) {
            throw new RecoveryEngineException(shard.shardId(), 1, "snapshot failed", e);
        }
        requiredSeqNoRangeStart =
            Long.parseLong(phase1Snapshot.getIndexCommit().getUserData().get(SequenceNumbers.LOCAL_CHECKPOINT_KEY)) + 1;   // :164-165
        startingSeqNo = shard.indexSettings().isSoftDeleteEnabled() ? requiredSeqNoRangeStart : 0;                        // :169
        try {
            final int estimateNumOps = shard.estimateNumberOfHistoryOperations("peer-recovery", startingSeqNo);
            phase1(phase1Snapshot.getIndexCommit(), () -> estimateNumOps);    // :172
        } catch (...) { throw new RecoveryEngineException(shard.shardId(), 1, "phase1 failed", e); }
        finally { IOUtils.close(phase1Snapshot); }
    }
    ...
```

**三个条件缺一不可**（`:150-151`）：

| 条件 | 含义 |
| --- | --- |
| `request.startingSeqNo() != UNASSIGNED` | 目标侧 `getStartingSeqNo` 算出了一个有效起点（§5.3） |
| `isTargetSameHistory()`（`:233`） | 目标的 `historyUUID` 与本分片一致 —— 保证是"同一条历史"，不是拼凑出来的数据 |
| `hasCompleteHistoryOperations("peer-recovery", startingSeqNo)`（`IndexShard.java:1769`） | 源上从 `startingSeqNo` 开始的**完整操作历史还在**（translog 没被 trim；靠 `:147` 的保留锁保证） |

`acquireRetentionLockForPeerRecovery()`（`IndexShard.java:1746`）→ `Engine.acquireRetentionLockForPeerRecovery()`
—— 它会**阻止 translog 被 trim**，从而保证整个 phase2 期间历史操作可读。

> **注意 `startingSeqNo = 0` 的分支（`:169`）**：软删除（`index.soft_deletes.enabled=true`）时，
> Lucene 里本身就保留了历史，所以只需要传 `local_checkpoint+1` 之后的操作；
> 否则要传 `startingSeqNo=0`，让目标**重建一个符合保留策略的 translog**（注释 `:166-168`）。

### 6.3 第 2 步：让目标准备好 engine

```java
prepareTargetForTranslog(isSequenceNumberBasedRecovery == false,
    shard.estimateNumberOfHistoryOperations("peer-recovery", startingSeqNo));      // :189-190
```

`prepareTargetForTranslog`（`:430`）→ 目标 `prepareForTranslogOperations` → `openEngineAndSkipTranslogRecovery`。
注意第一个参数：`fileBasedRecovery == (isSequenceNumberBasedRecovery == false)` ——
`true` 表示目标本地 translog 不可信，需要重建。

### 6.4 第 3 步：把目标加进复制组（★ 最重要的一步）

```195:214:indices/recovery/RecoverySourceHandler.java
```

```java
/*
 * add shard to replication group (shard will receive replication requests from this point on) now that engine is open.
 * This means that any document indexed into the primary after this will be replicated to this replica as well
 * make sure to do this before sampling the max sequence number in the next step, to ensure that we send
 * all documents up to maxSeqNo in phase2.
 */
runUnderPrimaryPermit(() -> shard.initiateTracking(request.targetAllocationId()),      // :201
    shardId + " initiating tracking of " + request.targetAllocationId(), shard, cancellableThreads, logger);

final long endingSeqNo = shard.seqNoStats().getMaxSeqNo();                             // :204
cancellableThreads.execute(() -> shard.waitForOpsToComplete(endingSeqNo));             // :209
```

这一小段是**恢复期间数据不丢的核心**，值得逐句拆：

1. `initiateTracking(allocationId)`（`IndexShard.java:1915`）→ `ReplicationTracker.initiateTracking`（`:602`）
   → 该副本变成 `tracked=true` —— **从此刻起，主分片的新写入会复制给它**（对应第 17 章 §5.2 的 `tracked ≠ inSync`）。
2. **必须在采样 `endingSeqNo` 之前**做这件事：因为恢复期间还有并发写入。
   如果先采样再 tracking，那么 `(endingSeqNo, 之后新写的]` 这段就既不通过 recovery 传、也不通过复制传 → **数据丢失**。
3. `waitForOpsToComplete(endingSeqNo)`（`LocalCheckpointTracker.java:141`）：
   等所有 ≤ `endingSeqNo` 的**在途操作**都完成，否则它们不在 translog 快照里、也还没写进 Lucene → 同样会丢。

> **记忆点**：`initiateTracking` 在前、`endingSeqNo` 采样在后、`waitForOpsToComplete` 兜底。
> 三者合起来保证了「恢复 + 复制」的交接没有缝隙。

### 6.5 第 4 步：phase2 —— 传 translog 历史

```216:227:indices/recovery/RecoverySourceHandler.java
```

```java
final long targetLocalCheckpoint;
try (Translog.Snapshot snapshot = shard.getHistoryOperations("peer-recovery", startingSeqNo)) {   // :217
    final long maxSeenAutoIdTimestamp = shard.getMaxSeenAutoIdTimestamp();                        // :220
    final long maxSeqNoOfUpdatesOrDeletes = shard.getMaxSeqNoOfUpdatesOrDeletes();                // :221
    targetLocalCheckpoint = phase2(startingSeqNo, requiredSeqNoRangeStart, endingSeqNo, snapshot,
        maxSeenAutoIdTimestamp, maxSeqNoOfUpdatesOrDeletes);                                      // :222
} catch (Exception e) {
    throw new RecoveryEngineException(shard.shardId(), 2, "phase2 failed", e);
}
```

`sendSnapshot`（`:546`）负责分批发送，有两个细节：

**(a) 按字节分批**（`chunkSizeInBytes`，默认 512KB，`RecoverySettings.java:78`）：

```java
operations.add(operation);
ops++;
size += operation.estimateSize();
totalSentOps++;
requiredOpsTracker.markSeqNoAsCompleted(seqNo);                 // :589
if (size >= chunkSizeInBytes) {
    cancellableThreads.executeIO(sendBatch);                     // :593
    ops = 0; size = 0; operations.clear();
}
```

即使一个操作都没发，也要发一次空请求（`:601-604`）：

```java
if (!operations.isEmpty() || totalSentOps == 0) {
    // send the leftover operations or if no operations were sent, request the target to respond with its local checkpoint
    cancellableThreads.executeIO(sendBatch);
}
```

**(b) 用 `requiredOpsTracker` 校验"必需区间"覆盖完整**（`:559`、`:610`）：

```java
final LocalCheckpointTracker requiredOpsTracker = new LocalCheckpointTracker(endingSeqNo, requiredSeqNoRangeStart - 1);   // :559
...
if (requiredOpsTracker.getCheckpoint() < endingSeqNo) {
    throw new IllegalStateException("translog replay failed to cover required sequence numbers" +
        " (required range [" + requiredSeqNoRangeStart + ":" + endingSeqNo + "). first missing op is ["
        + (requiredOpsTracker.getCheckpoint() + 1) + "]");                                                               // :610-613
}
```

> 这是防止"translog 被 trim 导致中间缺了几个 op，但两边都没发现"的最后一道防线。
> **线上看到 `translog replay failed to cover required sequence numbers`**，说明源上的历史确实不完整（通常是 retention 配置太小或保留锁失效）。

### 6.6 第 5 步：finalizeRecovery

```486:517:indices/recovery/RecoverySourceHandler.java
```

```java
/*
 * Before marking the shard as in-sync we acquire an operation permit. We do this so that there is a barrier between marking a
 * shard as in-sync and relocating a shard. If we acquire the permit then no relocation handoff can complete before we are done
 * marking the shard as in-sync. If the relocation handoff holds all the permits then after the handoff completes and we acquire
 * the permit then the state of the shard will be relocated and this recovery will fail.
 */
runUnderPrimaryPermit(() -> shard.markAllocationIdAsInSync(request.targetAllocationId(), targetLocalCheckpoint),   // :499
    shardId + " marking " + request.targetAllocationId() + " as in sync", shard, cancellableThreads, logger);
final long globalCheckpoint = shard.getGlobalCheckpoint();
cancellableThreads.executeIO(() -> recoveryTarget.finalizeRecovery(globalCheckpoint));                              // :502
runUnderPrimaryPermit(() -> shard.updateGlobalCheckpointForShard(request.targetAllocationId(), globalCheckpoint),   // :503
    shardId + " updating " + request.targetAllocationId() + "'s global checkpoint", shard, cancellableThreads, logger);

if (request.isPrimaryRelocation()) {
    // this acquires all IndexShard operation permits and will thus delay new recoveries until it is done
    cancellableThreads.execute(() -> shard.relocated(recoveryTarget::handoffPrimaryContext));                        // :509
}
```

**顺序同样是高度讲究的**：

1. `markAllocationIdAsInSync(target, targetLocalCheckpoint)`：副本正式 in-sync（第 17 章 §5.6，可能阻塞等 lcp 追上 gcp）。
2. 取 `globalCheckpoint` → 发给目标 → 目标 `updateGlobalCheckpointOnReplica` + `sync()` + `finalizeRecovery()`。
3. 主分片记录"该副本的已知 gcp"。
4. 如果是**主分片 relocation**，最后做 handoff：`shard.relocated(consumer)`（`IndexShard.java:635`）
   → `blockOperations(30, MINUTES, ...)` 拿走所有 permit → `startRelocationHandoff()`（第 17 章 §5.8）
   → 把 `PrimaryContext` 通过 `handoffPrimaryContext` 发给 target。

> 注释 `:494-498` 解释的 barrier 就是：
> 「mark in-sync」和「relocation handoff」互斥，二者不能交错，否则会出现"target 拿到过期的 tracker 状态"。

---

## 7. phase1 详解（只在全量恢复时执行）

### 7.1 文件 diff：`Store.MetadataSnapshot.recoveryDiff`

```979:1029:index/store/Store.java
```

返回 `RecoveryDiff`（`Store.java:1094`）：

| 集合 | 含义 |
| --- | --- |
| `identical` | 两边都有且一致 → **直接复用**（记入 `phase1ExistingFile*`） |
| `different` | 两边都有但不一致 → **重传** |
| `missing` | 源有目标没有 → **重传** |

判定粒度是 **segment**（`perSegment`，`Store.java:983-1001`）：同一 segment 的所有文件必须整体一致才算 identical，
避免"半个 segment"被复用。

`segments.gen`（`OLD_SEGMENTS_GEN`）被显式跳过（`:987`）。

### 7.2 sync id 短路

```316:331:indices/recovery/RecoverySourceHandler.java
```

```java
final boolean recoverWithSyncId = recoverySourceSyncId != null &&
        recoverySourceSyncId.equals(recoveryTargetSyncId);
if (recoverWithSyncId) {
    final long numDocsTarget = request.metadataSnapshot().getNumDocs();
    final long numDocsSource = recoverySourceMetadata.getNumDocs();
    if (numDocsTarget != numDocsSource) {
        throw new IllegalStateException("try to recover " + request.shardId() + " from primary shard with sync id but number " +
            "of docs differ: " + numDocsSource + " (...) vs " + numDocsTarget + "(...)");
    }
    // we shortcut recovery here because we have nothing to copy. but we must still start the engine on the target.
    // so we don't return here
    logger.trace("skipping [phase1]- identical sync id [{}] found on both source and target", recoverySourceSyncId);
}
```

**synced flush**（第 16 章）留下的 `sync_id` 一致 → **完全跳过文件拷贝**，直接进 phase2。
这是 5.x/6.x 里"重启集群后恢复飞快"的原因（7.x 之后被 soft-deletes 取代）。

### 7.3 发送 + 校验 + 清理

```java
// ① 先告诉目标要发哪些文件（用于进度统计）
cancellableThreads.execute(() -> recoveryTarget.receiveFileInfo(...));                       // :364-366

// ② 逐文件发送，包在 RateLimiter 里（RecoveryOutputStream），按 chunkSizeInBytes 分块
final Function<StoreFileMetaData, OutputStream> outputStreamFactories =
        md -> new BufferedOutputStream(new RecoveryOutputStream(md, translogOps), chunkSizeInBytes);   // :368-369
sendFiles(store, phase1Files.toArray(...), outputStreamFactories);                            // :370

// ③ 让目标改文件名 + 校验 + 删多余文件
cancellableThreads.executeIO(() -> recoveryTarget.cleanFiles(translogOps.get(), recoverySourceMetadata));   // :380-381
```

`sendFiles`（`:671`）：**先发小文件**（`ArrayUtil.timSort(files, comparingLong(length))`，`:674`），
这样进度条增长更快、也让"部分可用"更早达成。

### 7.4 校验失败后的自检（很漂亮的容错设计）

```382:418:indices/recovery/RecoverySourceHandler.java
```

目标在 `cleanFiles` 里发现 `CorruptIndexException` 时，源会**反过来检查自己本地的文件完整性**（`:394-402`）：

```java
for (StoreFileMetaData md : metadata) {          // 已按文件大小排序，小文件优先
    cancellableThreads.checkForCancel();
    if (store.checkIntegrityNoException(md) == false) {   // 主分片自己就坏了
        shard.failShard("recovery", corruptIndexException);
        logger.warn("Corrupted file detected {} checksum mismatch", md);
        throw corruptIndexException;
    }
}
// 主分片没问题 → 说明是传输过程中坏了
RemoteTransportException exception = new RemoteTransportException(
    "File corruption occurred on recovery but checksums are ok", null);
```

**排障意义**：看到 `"Corrupted file detected ... checksum mismatch"` → **源分片坏了**，要重建副本；
看到 `"File corruption occurred on recovery but checksums are ok"` → **网络/磁盘链路上有比特翻转**，查硬件。

---

## 8. `RecoveryState`：阶段机与可观测性

```51:74:indices/recovery/RecoveryState.java
```

| Stage | id | 谁设置 | 在干什么 |
| --- | --- | --- | --- |
| `INIT` | 0 | 创建时 | 排队、`getStartRecoveryRequest`、等 engine 准备 |
| `INDEX` | 1 | `IndexShard.prepareForIndexRecovery():1286` | 拷贝 Lucene 文件（phase1） |
| `VERIFY_INDEX` | 2 | `IndexShard.innerOpenEngineAndTranslog():1419` | 可选 `checkIndex`（`index.check_on_startup`） |
| `TRANSLOG` | 3 | `IndexShard.innerOpenEngineAndTranslog():1428` | 开 engine + 重放 translog（phase2） |
| `FINALIZE` | 4 | `IndexShard.finalizeRecovery():1531` | `refresh("recovery_finalization")` + 打开 GC deletes |
| `DONE` | 5 | `RecoveryState` 收尾 | 恢复完成 |

`setStage`（`:160-182`）用 `validateAndSetStage` 保证**只能前进不能回退**，并顺带 `start()/stop()` 各子计时器。

`RecoveryState` 的子统计对象：

- `Index`（`:105`）：文件数、`recoveredBytes` / `reusedBytes` —— `GET /_recovery` 里的主要输出。
- `Translog`（`:106`）：`totalOperations` / `totalOperationsOnStart` / `recoveredOperations`。
- `VerifyIndex`（`:107`）：`checkIndexTime`。
- `Timer`（`:108`，类定义在 `:350`）：整体耗时。

### 观测命令

```bash
# 实时看每个分片恢复到哪一步、传了多少字节
GET /_recovery?pretty&detailed=true
GET /my-index/_recovery?pretty

# 只看活跃恢复
GET /_cat/recovery/my-index?h=index,shard,time,type,stage,source_node,target_node,bytes,bytes_percent,files,files_percent&v
```

`_recovery` 里的关键字段：`type`（peer/snapshot/store/empty_store）、`stage`、`index.files.total/recovered/reused/percent`、
`translog.operations / percent`。

---

## 9. 主分片 relocation（特殊形态的 peer recovery）

`StartRecoveryRequest.isPrimaryRelocation()` 为 true 时，只多了一步（`:506-514`）：

```java
if (request.isPrimaryRelocation()) {
    cancellableThreads.execute(() -> shard.relocated(recoveryTarget::handoffPrimaryContext));
}
```

`IndexShard.relocated`（`IndexShard.java:635`）：

```java
indexShardOperationPermits.blockOperations(30, TimeUnit.MINUTES, () -> {
    assert indexShardOperationPermits.getActiveOperationsCount() == 0 : ...
    // ...
});
```

**要点**：

- `blockOperations` 会**等到所有在途操作结束并拿走全部 permit**，所以新的 recovery / 写入都会被阻塞（最多 30 分钟）。
- handoff 完成后，源分片 `primaryMode = false, relocated = true`（第 17 章 §5.8）。
- 注释 `:511-513` 提醒：**handoff 之后如果恢复失败，源和 target 都会被标记失败**（`failShard`），
  因为此时源已经不是主了，无法回退。

### 为什么 relocation 也要走 phase1/phase2？

因为 target 节点上通常没有该分片的数据（或只有旧数据），所以流程和副本恢复一致；
区别只在于最后一步的 handoff 和 `isPrimaryRelocation` 标记。

---

## 10. 快照恢复（`SNAPSHOT`）与 Local Shards（`LOCAL_SHARDS`）

### 10.1 `SNAPSHOT`

`IndexShard.restoreFromRepository(repository)`（`:1692`）→ `StoreRecovery.recoverFromRepository`（`:274`）：

```java
SnapshotRecoverySource recoverySource = (SnapshotRecoverySource) indexShard.recoveryState().getRecoverySource();   // :278
return executeRecovery(indexShard, () -> {
    logger.debug("restoring from {} ...", indexShard.recoveryState().getRecoverySource());
    restore(indexShard, repository, recoverySource);       // :281
});
```

流程与本地恢复类似，只是文件来源从本地磁盘换成 `Repository`（S3/HDFS/FS）。完成后同样走
`internalRecoverFromStore` → `openEngineAndRecoverFromTranslog`，但 translog 是从快照里的恢复点开始的。

### 10.2 `LOCAL_SHARDS`（shrink / split）

`StoreRecovery.recoverFromLocalShards`（`:101`）→ `addIndices`（`:148`）：

```java
Lucene.cleanLuceneIndex(target);                                       // :153 先清空目标目录
final Directory hardLinkOrCopyTarget = new HardlinkCopyDirectoryWrapper(target);   // :158 ★ 硬链接优先
IndexWriterConfig iwc = new IndexWriterConfig(null)
    .setSoftDeletesField(Lucene.SOFT_DELETES_FIELD)
    .setCommitOnClose(false)
    .setMergePolicy(NoMergePolicy.INSTANCE)                            // :166 恢复期间不合并
    .setOpenMode(IndexWriterConfig.OpenMode.APPEND);
try (IndexWriter writer = new IndexWriter(new StatsDirectoryWrapper(hardLinkOrCopyTarget, indexRecoveryStats), iwc)) {
    writer.addIndexes(sources);                                        // :173
    if (split) {
        writer.deleteDocuments(new ShardSplittingQuery(indexMetaData, shardId, hasNested));   // :175 按 routing 切分
    }
    writer.setLiveCommitData(() -> {
        liveCommitData.put(SequenceNumbers.MAX_SEQ_NO, Long.toString(maxSeqNo));               // :184
        liveCommitData.put(SequenceNumbers.LOCAL_CHECKPOINT_KEY, Long.toString(maxSeqNo));     // :185
        liveCommitData.put(Engine.MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID, Long.toString(maxUnsafeAutoIdTimestamp));
        return liveCommitData.entrySet().iterator();
    });
    writer.commit();
}
```

三个要点：

1. **`HardlinkCopyDirectoryWrapper`**（`:158`）：同节点内优先硬链接，几乎零拷贝 → shrink 极快。
2. **seqno 直接设为源分片的最大值**（`:184-185`）：因为这是新建的历史，从 `maxSeqNo` 开始继续。
3. **`ShardSplittingQuery`**（`:175`）是 split 的核心：按目标 shard 的 routing 范围**删除不属于自己的文档**。

`StatsDirectoryWrapper`（`:196`）的作用是**让硬链接/拷贝的字节数也能被 `RecoveryState.Index` 统计到**
（`:214` `index.addFileDetail`、`:252` `index.addRecoveredBytesToFile`）。

---

## 11. 调优参数（`RecoverySettings`）

| 设置 | 默认 | 行号 | 作用 |
| --- | --- | --- | --- |
| `indices.recovery.max_bytes_per_sec` | 40MB/s | `:38` | ★ 单节点**出方向**恢复总限速（共享 `RateLimiter`）。节点重启/扩容时最该调的一个 |
| `indices.recovery.retry_delay_state_sync` | 500ms | `:46` | 状态未就绪时的重试间隔 |
| `indices.recovery.retry_delay_network` | 5s | `:51` | 网络异常的重试间隔 |
| `indices.recovery.internal_action_timeout` | 15min | `:56` | 单个恢复 action 超时 |
| `indices.recovery.internal_action_long_timeout` | 30min | `:64` | 长 action 超时 |
| `indices.recovery.recovery_activity_timeout` | 30min | `:73` | ★ 整体不活跃超时（超时即失败） |
| `DEFAULT_CHUNK_SIZE` | 512KB | `:78` | 文件块 / translog 批次大小 |

全部是 **`Property.Dynamic`**，可热更新：

```bash
PUT /_cluster/settings
{
  "transient": {
    "indices.recovery.max_bytes_per_sec": "200mb",
    "indices.recovery.recovery_activity_timeout": "60m"
  }
}
```

**云服务经验值**：

- 节点替换 / 扩容：临时把 `max_bytes_per_sec` 抬到 200MB~1GB，完成后调回（避免影响在线流量）。
- 大分片（>50GB）频繁触发 `recovery_activity_timeout` → 抬高到 60m 甚至 120m。
- 磁盘 IO 打满导致写入超时 → **降低** `max_bytes_per_sec` 并抬高 `cluster.routing.allocation.node_concurrent_recoveries` 之外的 IO 余量。

还有几个在别处定义但同样重要的：

```bash
cluster.routing.allocation.node_concurrent_recoveries      # 单节点并发恢复数（默认 2）
cluster.routing.allocation.node_initial_primaries_recoveries  # 初始主恢复并发（默认 4）
index.unassigned.node_left.delayed_timeout                  # 节点离开后延迟分配（默认 1m，等节点回来可省一次全量恢复）
```

> **`delayed_timeout` 是运维最省事的优化**：默认给 5m~10m，节点短暂重启/网络抖动回来后分片直接复用本地数据，
> 走 seqno-based 增量恢复，而不是等它被踢掉后重新全量分配。

---

## 12. 排障手册

### 12.1 `GET /_recovery` 怎么看

| 字段含义 | 说明 |
| --- | --- |
| `type = peer` | 从主分片复制 |
| `type = store` / `empty_store` | 本地恢复（主分片） |
| `type = snapshot` | 从快照 |
| `stage = init` | 卡在排队/准备（常见：源节点未就绪、`DelayRecoveryException`） |
| `stage = index` | 在拷 segment 文件 → 看 `index.percent`；慢说明**走了全量** |
| `stage = translog` | 在重放 translog → 看 `translog.percent` |
| `index.files.reused > 0` | 复用了本地文件（好事） |
| `index.files.recovered == total` 且 `reused == 0` | 全量拷贝（慢） |

### 12.2 常见现象 → 源码定位

| 现象 | 原因 | 定位 |
| --- | --- | --- |
| 副本恢复总是**全量** | `getStartingSeqNo` 返回 `UNASSIGNED` | `PeerRecoveryTargetService.java:374-384`；检查 `maxSeqNo > globalCheckpoint`（本地有未确认数据）或 translog 损坏 |
| `DelayRecoveryException` 反复出现 | target 不在源的复制组路由表里 | `RecoverySourceHandler.java:139-143`；通常是集群状态还没同步到源节点 |
| `translog replay failed to cover required sequence numbers` | 源上历史不完整 | `RecoverySourceHandler.java:610`；检查 translog retention 与保留锁 |
| `File corruption occurred on recovery but checksums are ok` | 传输链路损坏 | `RecoverySourceHandler.java:408`；查网络/磁盘 |
| `Corrupted file detected ... checksum mismatch` | **源分片损坏** | `RecoverySourceHandler.java:399`；需要重建该分片 |
| `Files are missing on the recovery target` | 目标端文件校验失败 | `Store.java:688` |
| `not all required shards of index ... are started yet` | shrink 时源索引分片未全启动 | `IndexShard.java:2256` |
| 恢复卡住不动直到 30min 超时 | 网络/IO 极慢，或源端 permit 被 relocation 阻塞 | `RecoverySettings.java:73`、`IndexShard.java:639` |
| 恢复中副本磁盘占用翻倍 | phase1 用**临时文件名**（带 recovery id 后缀）写入 | `RecoveryTarget.java:499`；`cleanFiles` 后重命名（`:452`） |

### 12.3 关键日志

```
"starting recovery from store ..."                                  StoreRecovery.java:94
"recovery completed from [shard_store], took [{}]"                  StoreRecovery.java:330
"performing sequence numbers based recovery. starting at [{}]"      RecoverySourceHandler.java:153
"Calculate starting seqno based on global checkpoint [{}], safe commit [{}], existing commits [{}]"
                                                                    PeerRecoveryTargetService.java:371
"skipping [phase1]- identical sync id [{}] found on both source and target"
                                                                    RecoverySourceHandler.java:330
"recovery completed from [{}], took [{}]"                           PeerRecoveryTargetService.java:228
"finalizing recovery took [{}]"                                     RecoverySourceHandler.java:516
"performing relocation hand-off"                                    RecoverySourceHandler.java:507
"not all required shards of index {} are started yet, expected {} found {}"
                                                                    IndexShard.java:2256
```

---

## 13. 动手实验

### 实验 1：观察两种 peer recovery

```bash
# 1) 建一个带副本的索引，写入数据
PUT /rec_test {"settings":{"number_of_shards":1,"number_of_replicas":1}}
POST /rec_test/_doc/1 {"a":1}

# 2) 增量为 0 时停掉副本节点，主上继续写入，然后重启副本节点
#    观察 _recovery：type=peer，index.files.reused 应该 > 0（增量）
GET /_recovery?pretty&detailed=true

# 3) 删掉副本分片目录后重启节点 → 必然全量
#    观察 index.files.reused = 0
```

### 实验 2：看 `getStartingSeqNo` 的判定

在 `PeerRecoveryTargetService.getStartingSeqNo`（`:358`）打断点，
观察 `globalCheckpoint`、`safeCommit`、`seqNoStats.maxSeqNo` 三者关系。
把 `safeCommit` 的 `max_seq_no` 人为改大（测试代码里）会看到退化成全量。

### 实验 3：relocation handoff

```bash
PUT /_cluster/settings
{"transient":{"cluster.routing.allocation.exclude._ip":"<old-primary-ip>"}}
# 观察日志 "performing relocation hand-off"
```

### 实验 4：限速影响

```bash
PUT /_cluster/settings {"transient":{"indices.recovery.max_bytes_per_sec":"1mb"}}
# 全量恢复一个较大分片，观察 _recovery 里 bytes_percent 的爬升速度
```

### 实验 5：单元测试

```bash
./gradlew :server:test --tests "org.elasticsearch.indices.recovery.RecoverySourceHandlerTests"
./gradlew :server:test --tests "org.elasticsearch.indices.recovery.PeerRecoveryTargetServiceTests"
./gradlew :server:test --tests "org.elasticsearch.indices.recovery.IndexRecoveryTests"
./gradlew :server:test --tests "org.elasticsearch.index.shard.StoreRecoveryTests"
```

`RecoverySourceHandlerTests` 覆盖了 seqno-based vs file-based 的各种组合，`IndexRecoveryTests` 是端到端的
（含 `testTranslogHistoryTransferred`、`testRevertBulkDuringPeerRecovery` 等），非常值得逐条读。

---

## 14. 自测题（答案都在本章）

1. 6.x 有哪 5 种 `RecoverySource.Type`？分别在什么场景触发？
2. 恢复的分派入口是哪个方法？哪些类型是异步执行、哪些是同步发起？
3. 为什么本地恢复（store recovery）只允许主分片？
4. `trimUnsafeCommits()` 做什么？它和第 17 章的哪个方法思想一致？
5. 副本上执行 `openEngineAndSkipTranslogRecovery()` 而不是 `openEngineAndRecoverFromTranslog()`，为什么？
6. `getStartingSeqNo` 返回什么值表示"可以增量恢复"？判定条件是什么？
7. `CombinedDeletionPolicy.findSafeCommitPoint` 的"安全"是指什么？
8. 源侧判定 `isSequenceNumberBasedRecovery` 需要同时满足哪 3 个条件？
9. `acquireRetentionLockForPeerRecovery()` 的作用是什么？不加会怎样？
10. 为什么 `initiateTracking` 必须在采样 `endingSeqNo` **之前**执行？
11. `waitForOpsToComplete(endingSeqNo)` 解决什么问题？
12. `startingSeqNo` 在软删除关闭时为什么被设为 0，而不是 `requiredSeqNoRangeStart`？
13. `sendSnapshot` 里的 `requiredOpsTracker` 是干什么的？报错信息是什么？
14. `finalizeRecovery` 的三步顺序是什么？为什么 mark-in-sync 要包在 `runUnderPrimaryPermit` 里？
15. 主分片 relocation 与普通副本恢复的唯一区别是什么？`shard.relocated()` 做了什么？
16. `RecoveryState.Stage` 六个阶段分别由谁设置？
17. `RecoveryDiff` 的三个集合是什么？判定粒度是文件还是 segment？
18. sync id 一致时会发生什么？为什么 7.x 之后不再需要它？
19. `cleanFiles` 为什么会重建一个空的 translog？
20. 看到 `"Corrupted file detected ... checksum mismatch"` 和 `"File corruption occurred on recovery but checksums are ok"`，
    分别说明什么？该怎么处理？
21. `HardlinkCopyDirectoryWrapper` 在哪种恢复里使用？带来什么好处？
22. `ShardSplittingQuery` 的作用是什么？
23. 全量恢复期间副本磁盘会临时增大，源码上是怎么造成的？
24. `index.unassigned.node_left.delayed_timeout` 为什么能显著减少全量恢复？
25. 一个 100GB 的分片副本恢复特别慢，你会按什么顺序排查和调优？（至少 5 步）

---

## 15. 关键类速查表

| 类 | 路径 | 关键行 |
| --- | --- | --- |
| `RecoverySource` | `cluster/routing/RecoverySource.java` | `Type:86`、`PeerRecoverySource:304`、`SnapshotRecoverySource:211` |
| `IndexShard` | `index/shard/IndexShard.java` | ★ `startRecovery:2155`、`recoverFromStore:1683`、`recoverFromLocalShards:1662`、`restoreFromRepository:1692`、`prepareForIndexRecovery:1282`、`openEngineAndRecoverFromTranslog:1392`、`openEngineAndSkipTranslogRecovery:1410`、`innerOpenEngineAndTranslog:1415`、`trimUnsafeCommits:1460`、`assertSequenceNumbersInCommit:1469`、`finalizeRecovery:1530`、`performRecoveryRestart:1498`、`acquireSafeIndexCommit:1162`、`acquireRetentionLockForPeerRecovery:1746`、`estimateNumberOfHistoryOperations:1753`、`getHistoryOperations:1761`、`hasCompleteHistoryOperations:1769`、`initiateTracking:1915`、`markAllocationIdAsInSync:1928`、`relocated:635`、`isRelocatedPrimary:2271` |
| `StoreRecovery` | `index/shard/StoreRecovery.java` | `recoverFromStore:88`、`recoverFromLocalShards:101`、`addIndices:148`、`StatsDirectoryWrapper:196`、`recoverFromRepository:274`、`executeRecovery:302`、`internalRecoverFromStore:357` |
| `PeerRecoveryTargetService` | `indices/recovery/PeerRecoveryTargetService.java` | ★ `startRecovery:135`、`retryRecovery:140`、`doRecovery:165`、`getStoreMetadataSnapshot:296`、`getStartRecoveryRequest:315`、**`getStartingSeqNo:358`**、translog handler:449 |
| `RecoveryTarget` | `indices/recovery/RecoveryTarget.java` | `prepareForTranslogOperations:364`、`finalizeRecovery:373`、`handoffPrimaryContext:387`、`indexTranslogOperations:392`、`receiveFileInfo:429`、`cleanFiles:447`、`writeFileChunk:491` |
| `PeerRecoverySourceService` | `indices/recovery/PeerRecoverySourceService.java` | `recover:90`、`StartRecoveryTransportRequestHandler:110`、`new RecoverySourceHandler:181` |
| `RecoverySourceHandler` | `indices/recovery/RecoverySourceHandler.java` | ★★ **`recoverToTarget:135`**、`isTargetSameHistory:233`、`runUnderPrimaryPermit:240`、`phase1:288`、`prepareTargetForTranslog:430`、`phase2:459`、`finalizeRecovery:486`、`sendSnapshot:546`、`sendFiles:671` |
| `RecoveryState` | `indices/recovery/RecoveryState.java` | `Stage:51`、`setStage:160`、`Index/Translog/VerifyIndex/Timer:105-108`、`Timer 类:350` |
| `RecoverySettings` | `indices/recovery/RecoverySettings.java` | `max_bytes_per_sec:38`、`retry_delay_state_sync:46`、`retry_delay_network:51`、`internal_action_timeout:56`、`internal_action_long_timeout:64`、`recovery_activity_timeout:73`、`DEFAULT_CHUNK_SIZE:78` |
| `Store` | `index/store/Store.java` | `cleanupAndVerify:667`、`MetadataSnapshot.recoveryDiff:979`、`RecoveryDiff:1094` |
| `CombinedDeletionPolicy` | `index/engine/CombinedDeletionPolicy.java` | `findSafeCommitPoint:153`、`indexOfKeptCommits:179` |

---

**上一篇**：[17-副本复制与SeqNo一致性模型.md](./17-副本复制与SeqNo一致性模型.md)
**下一篇**：[19-检索流程-QueryThenFetch与DFS.md](./19-检索流程-QueryThenFetch与DFS.md)
