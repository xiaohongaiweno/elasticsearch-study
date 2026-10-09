# 15 · 存储引擎：InternalEngine 与 Lucene 集成

> 目标：搞清楚 ES 的 **Engine 抽象层**是怎么把「Lucene」包装成一个「支持 seqno、并发控制、
> 实时读、Translog 持久化」的存储引擎的。这是整个 ES 写/读路径的**心脏**。
>
> **为什么必须学**：
> - 第 13 章我们看到写入最终落到 `IndexShard.applyIndexOperationOnPrimary` → **`Engine.index()`**，
>   但没展开。本章把它打开。
> - 「为什么 GET 能立刻读到刚写的文档（realtime）」——答案在 `LiveVersionMap`。
> - 「为什么同样一条写入有时走 append-only 优化、有时要 update 旧文档」——答案在 `IndexingStrategy`。
> - 「版本冲突 / seqno 回放跳过 / 副本重放幂等」全部由 Engine 的 `plan*` 方法决定。
> - 第 16 章（Refresh/Flush/Merge）、第 17 章（SeqNo）、第 18 章（Recovery）都以此为基础。
>
> **前置**：第 13 章（写入链路）、第 14 章（Translog）。
> **后续**：第 16 章 `16-RefreshFlush与Segment合并.md`。

---

## 1. 一句话理解 Engine

> **Engine = Lucene 的 `IndexWriter`/`DirectoryReader` + Translog + 版本表（VersionMap） + seqno 追踪，
> 对外提供「带并发语义的 index/delete/get/flush」接口。**

Lucene 只懂「段」和「docId」，不懂：
- 文档的 **id/version**（倒排里没有）；
- **seqno**（复制一致性）；
- **实时读**（写进 buffer 但没 refresh 时读不到）；
- **持久化语义**（commit 才 fsync）。

`InternalEngine` 就是把这些补上。

```
                    ┌───────────────────────────────────────────────┐
                    │  Engine（抽象类, Engine.java:111）             │
                    │  index()/delete()/get()/refresh()/flush()...   │
                    └───────────────┬───────────────────────────────┘
                                    │ 实现
        ┌───────────────────────────┴────────────────────────────┐
        │  InternalEngine（InternalEngine.java:110）              │
        │                                                        │
        │  ┌────────────┐  ┌──────────────┐  ┌────────────────┐  │
        │  │ IndexWriter│  │LiveVersionMap│  │  Translog      │  │
        │  │ (Lucene)   │  │ current/old/ │  │ (第 14 章)     │  │
        │  │            │  │ tombstones   │  │                │  │
        │  └────────────┘  └──────────────┘  └────────────────┘  │
        │  ┌─────────────────────┐  ┌──────────────────────────┐ │
        │  │ SearcherManager×2   │  │ LocalCheckpointTracker   │ │
        │  │ internal / external │  │ (seqno 连续性追踪)       │ │
        │  └─────────────────────┘  └──────────────────────────┘ │
        └────────────────────────────────────────────────────────┘
```

---

## 2. 必读源码清单

| 类 | 路径 | 作用 |
| --- | --- | --- |
| `Engine` | `index/engine/Engine.java` | ★ 引擎抽象（接口 + Operation/Result/Searcher 定义） |
| `InternalEngine` | `index/engine/InternalEngine.java` | ★ 默认引擎，唯一生产实现 |
| `EngineConfig` | `index/engine/EngineConfig.java` | 引擎构造参数（含 `globalCheckpointSupplier`） |
| `LiveVersionMap` | `index/engine/LiveVersionMap.java` | ★ 实时版本表（realtime GET 的基础） |
| `VersionValue` / `IndexVersionValue` / `DeleteVersionValue` | `index/engine/VersionValue.java` | 版本文档 |
| `LocalCheckpointTracker` | `index/engine/LocalCheckpointTracker.java` | seqno 连续追踪 |
| `CombinedDeletionPolicy` | `index/engine/CombinedDeletionPolicy.java` | 决定哪些 commit/segments 可删 |
| `InternalEngineFactory` | `index/engine/InternalEngineFactory.java` | 工厂 |
| `EngineMergeScheduler` | `index/engine/EngineMergeScheduler.java` | 合并调度（第 16 章） |
| `IndexShard` | `index/shard/IndexShard.java` | ★ 引擎持有者（`getEngine()`/`applyIndexOperation*`） |
| `VersionsAndSeqNoResolver` | `index/engine/VersionsAndSeqNoResolver.java` | 从 Lucene reader 解出 version/seqno |
| `SeqNoStats` / `SequenceNumbers` | `index/seqno/` | seqno 常量与统计 |
| `IndexWriter` / `DirectoryReader` | Lucene | 底层 |

---

## 3. ★ `Engine` 抽象层

### 3.1 接口总览（`Engine.java`）

| 类别 | 方法 | 行号 |
| --- | --- | --- |
| 写入 | `index(Index)` | `:376` |
| | `delete(Delete)` | `:386` |
| | `noOp(NoOp)` | `:388` |
| 读 | `get(Get, searcherFactory)` | `:636` |
| 刷新 | `refresh(String)` | `:1033` |
| | `writeIndexingBuffer()` | `:1039` |
| | `maybeRefresh(...)`（见实现） | — |
| 提交 | `shouldPeriodicallyFlush()` | `:1045` |
| | `flush(boolean force, boolean waitIfOngoing)` | `:1055` |
| | `syncFlush(syncId, expectedCommitId)` | `:598` |
| Translog | `isTranslogSyncNeeded()` | `:712` |
| | `ensureTranslogSynced(Stream<Location>)` | `:717` |
| | `syncTranslog()` | `:719` |
| | `trimUnreferencedTranslogFiles()` | `:1074` |
| | `shouldRollTranslogGeneration()` | `:1082` |
| | `rollTranslogGeneration()` | `:1087` |
| | `trimOperationsFromTranslog(belowTerm, aboveSeqNo)` | `:335` |
| | `newChangesSnapshot(...)` | `:730` |
| 段 | `forceMerge(...)` | `:1099` |
| | `segments(boolean verbose)` | `:1004` |
| | `acquireLastIndexCommit(flushFirst)` | `:1108` |
| | `acquireSafeIndexCommit()` | `:1113` |
| seqno | `getLocalCheckpoint()` | `:780` |
| | `waitForOpsToComplete(seqNo)` | `:788` |
| | `getSeqNoStats(globalCheckpoint)` | `:793` |
| | `getLastSyncedGlobalCheckpoint()` | `:798` |
| 统计 | `getWritingBytes()` | `:201` |
| | `getIndexBufferRAMBytesUsed()` | `:924` |
| | `getIndexThrottleTimeInMillis()` / `isThrottled()` | `:323`/`:329` |
| 生命周期 | `closeNoLock(String, CountDownLatch)` | `:1662` |

> **抽象层的价值**：ES 只保留 `InternalEngine` 一个生产实现（`InternalEngineFactory`），
> 但抽象让测试能替换 MockEngine（如注入失败）。云服务二次开发时**一般不改 Engine**，
> 而是在 Shard/Store 层做扩展。

### 3.2 内部类家族

| 内部类 | 行号 | 说明 |
| --- | --- | --- |
| `Engine.Operation`（抽象） | `:1253` | 所有写操作的基类（uid/seqNo/primaryTerm/version/versionType/origin） |
| `Engine.Index` | `:1344` | 索引操作（含 parsedDoc / autoGeneratedIdTimestamp） |
| `Engine.Delete` | `:1443` | 删除操作（含 uid Term） |
| `Engine.NoOp` | `:1504` | 空操作（占 seqno） |
| `Engine.Result`（抽象） | `:395` | 结果基类（含 `getResultType()`） |
| `Engine.IndexResult` | `:514` | 索引结果（created/version/seqNo/term/`RequiredMappingUpdate`） |
| `Engine.DeleteResult` | `:546` | 删除结果 |
| `Engine.NoOpResult` | `:578` | 空操作结果 |
| `Engine.SyncedFlushResult` | `:600` | `SUCCESS` / `COMMIT_MISMATCH` / `PENDING_OPERATIONS` |
| `Engine.SearcherScope` | `:705` | ★ `EXTERNAL` / `INTERNAL` |
| `Engine.Searcher` | `:1207` | 包装 `IndexSearcher` + 关闭回调 |
| `Engine.Get` | `:1554` | 读请求（realtime / version / fromTranslog） |
| `Engine.GetResult` | `:1609` | 读结果（`NOT_EXISTS` 单例） |
| `Engine.CommitId` | `:1710` | Lucene commit 的 16 字节 id |
| `Engine.IndexCommitRef` | `:1765` | 持有 `IndexCommit` + 回调 |
| `Engine.EventListener` | `:1199` | 引擎事件（Merge/Refresh/Flush/Close/Fail） |
| `Engine.Warmer` | `:1807` | 搜索结果预热 |
| `Engine.TranslogRecoveryRunner` | `:1883` | Translog 回放回调 |
| `Engine.IndexThrottle` | `:271` | 写限流（合并落后时） |

### 3.3 `Engine.Operation`：写操作的统一描述（`:1253`）

```java
public abstract static class Operation {                    // :1253
    private final String uid;            // 6.x：uid = type + '#' + id（单 type 时可只 id）
    private final String routing;
    private final long seqNo;
    private final long primaryTerm;
    private final long version;
    private final VersionType versionType;
    private final Origin origin;
    private final long startTime;
    private final long autoGeneratedIdTimestamp;

    public enum Origin {                 // 写入来源
        PRIMARY,                         // primary 上的真实写入
        REPLICA,                         // 副本重放
        PEER_RECOVERY,                   // 从另一分片恢复
        LOCAL_TRANSLOG_RECOVERY,         // 本地 translog 恢复
        TRANSLOG_RECOVERY                // 旧版
    }
    public abstract String type();       // INDEX/DELETE/NOOP
    public abstract TYPE operationType();
}
```

**★ `Origin` 是 Engine 决策的输入之一**：
`planIndexingAsPrimary`（`InternalEngine:1019`）里：
```java
if (index.origin() == Operation.Origin.PRIMARY && index.seqNo() == SequenceNumbers.UNASSIGNED_SEQ_NO) {
    plan = IndexingStrategy.overrideExistingAsIfNotThere(generateSeqNoForOperation(index), 1L);
} else if (index.origin() == Operation.Origin.REPLICA && ...) { ... }
```

**只有 PRIMARY origin 才生成新 seqno**；REPLICA/PEER_RECOVERY 必须携带 seqno。

### 3.4 `Engine.Result` 与 `Result.Type`（`:395`）

```java
public abstract static class Result {
    public enum Type { SUCCESS, FAILURE, MAPPING_UPDATE_REQUIRED }
    public abstract Type getResultType();
    public abstract Exception getFailure();
    public abstract long getVersion();
    public abstract long getSeqNo();
    public abstract long getTerm();
    public Mapping getRequiredMappingUpdate();
}
```

**★★ `MAPPING_UPDATE_REQUIRED` 是第 13 章「动态 mapping」的源头**：
Engine 在解析文档时若发现新字段，不直接失败，而是返回一个「需要更新 mapping」的结果，
由 `TransportShardBulkAction.executeOnPrimaryWhileHandlingMappingUpdates`（第 13 章 `:482`）
提交 mapping 后重试。

---

## 4. ★★ `InternalEngine` 构造与关键字段

### 4.1 字段（`InternalEngine.java:110-166`）

```java
public class InternalEngine extends Engine {                     // :110
    private volatile long lastDeleteVersionPruneTimeMSec;        // :115 上次清理 tombstone 时间
    private final Translog translog;                             // :117 Translog（第 14 章）
    private final ElasticsearchConcurrentMergeScheduler mergeScheduler;   // :118 合并调度（第 16 章）
    private final IndexWriter indexWriter;                       // :120 ★ Lucene 写入器
    private final ExternalSearcherManager externalSearcherManager;  // :122 ★ 对外 searcher 管理器
    private final SearcherManager internalSearcherManager;          // :123 ★ 对内 searcher 管理器
    private final Lock flushLock = new ReentrantLock();          // :125 flush 串行锁
    private final ReentrantLock optimizeLock = new ReentrantLock(); // :126 forceMerge 锁
    private final LiveVersionMap versionMap = new LiveVersionMap();  // :130 ★ 实时版本表
    private volatile SegmentInfos lastCommittedSegmentInfos;     // :132 最后一次 commit 的 segments_N
    private final IndexThrottle throttle;                        // :134 写限流
    private final LocalCheckpointTracker localCheckpointTracker; // :136 ★ seqno 连续性
    private final String uidField;                               // :138 "_id" 或 "_uid"
    private final CombinedDeletionPolicy combinedDeletionPolicy; // :140 删除策略
    private final AtomicBoolean pendingTranslogRecovery;         // :146 ★ 恢复期间禁止 commit
    private final AtomicLong maxUnsafeAutoIdTimestamp;           // :147
    private final AtomicLong maxSeenAutoIdTimestamp;             // :148
    private final AtomicLong maxSeqNoOfNonAppendOnlyOperations;  // :149 ★ MSU 优化（见 §9）
    private final NumericDocValuesField softDeletesField;        // :156
    private final boolean softDeleteEnabled;                     // :157
    private final SoftDeletesPolicy softDeletesPolicy;           // :158
    private final LastRefreshedCheckpointListener lastRefreshedCheckpointListener;  // :159
    private final AtomicBoolean trackTranslogLocation;           // :161 是否记录 translog location
    private final KeyedLock<Long> noOpKeyedLock = new KeyedLock<>();  // :162 noOp 的 per-seqno 锁
    private final String historyUUID;                            // :165 分片历史 UUID（见第 17 章）
}
```

### 4.2 构造函数（`:171-247`）关键顺序

```java
InternalEngine(final EngineConfig engineConfig, BiFunction<...> localCheckpointTrackerSupplier) {
    super(engineConfig);
    if (engineConfig.isAutoGeneratedIDsOptimizationEnabled() == false) {
        updateAutoIdTimestamp(Long.MAX_VALUE, true);         // 关闭 append-only 优化
    }
    this.uidField = engineConfig.getIndexSettings().isSingleType() ? IdFieldMapper.NAME : UidFieldMapper.NAME;   // :178
    final TranslogDeletionPolicy translogDeletionPolicy = new TranslogDeletionPolicy(...);   // :179
    store.incRef();
    try {
        mergeScheduler = scheduler = new EngineMergeScheduler(shardId, indexSettings);       // :193
        throttle = new IndexThrottle();                                                     // :194
        translog = openTranslog(engineConfig, translogDeletionPolicy, globalCheckpointSupplier);  // :196
        this.softDeleteEnabled = indexSettings.isSoftDeleteEnabled();                        // :199
        this.softDeletesPolicy = newSoftDeletesPolicy();                                     // :200
        this.combinedDeletionPolicy = new CombinedDeletionPolicy(logger, translogDeletionPolicy,
            softDeletesPolicy, translog::getLastSyncedGlobalCheckpoint);                     // :201
        writer = createWriter();                                                             // :203 ★ Lucene IndexWriter
        bootstrapAppendOnlyInfoFromWriter(writer);                                           // :204
        historyUUID = loadHistoryUUID(writer);                                               // :205
        indexWriter = writer;                                                                // :206
        externalSearcherManager = createSearcherManager(new SearchFactory(...));              // :218
        internalSearcherManager = externalSearcherManager.internalSearcherManager;            // :219
        internalSearcherManager.addListener(versionMap);                                      // :222 ★ versionMap 监听 refresh
        pendingTranslogRecovery.set(true);                                                    // :225 ★ 恢复前禁止 commit
        this.localCheckpointTracker = createLocalCheckpointTracker(...);                      // :232
        this.lastRefreshedCheckpointListener = new LastRefreshedCheckpointListener(...);      // :234
        this.internalSearcherManager.addListener(lastRefreshedCheckpointListener);            // :235
        success = true;
    } finally {
        if (success == false) { IOUtils.closeWhileHandlingException(writer, translog, ...); }
    }
}
```

**★★ 三个必须记住的点**：

1. **`internalSearcherManager.addListener(versionMap)`（`:222`）**：
   `LiveVersionMap` 实现了 `ReferenceManager.RefreshListener`，
   **每次 refresh 时自动 `beforeRefresh()`/`afterRefresh()` 清理版本表**（见 §7）。
2. **`pendingTranslogRecovery.set(true)`（`:225`）**：引擎刚创建时**禁止 commit**，
   必须先把 translog 里未 commit 的操作回放完（`recoverFromTranslog`），
   否则会丢失数据、也会破坏 seqno 连续性。
3. **`historyUUID`（`:205`）**：分片历史标识，用于判断「副本是否与 primary 同源」，
   是第 17 章 `history_uuid` 校验的基础。

### 4.3 `createLocalCheckpointTracker`（`:249`）

```java
private static LocalCheckpointTracker createLocalCheckpointTracker(EngineConfig engineConfig,
        SegmentInfos lastCommittedSegmentInfos, ..., BiFunction<...> supplier) {
    final SequenceNumbers.CommitInfo seqNoStats =
        SequenceNumbers.loadSeqNoInfoFromLuceneCommit(lastCommittedSegmentInfos.userData.entrySet());  // 从 commit userData 读
    final long maxSeqNo = seqNoStats.maxSeqNo;
    final long localCheckpoint = seqNoStats.localCheckpoint;
    final LocalCheckpointTracker tracker = supplier.apply(maxSeqNo, localCheckpoint);
    if (localCheckpoint < maxSeqNo && engineConfig.getIndexSettings().isSoftDeleteEnabled()) {
        // soft-delete 开启时，需要扫 Lucene 把 [localCheckpoint+1, maxSeqNo] 标成已完成
        try (Searcher searcher = searcherSupplier.get()) {
            Lucene.scanSeqNosInReader(searcher.getDirectoryReader(), localCheckpoint + 1, maxSeqNo,
                tracker::markSeqNoAsCompleted);
        }
    }
    return tracker;
}
```

**★ `LocalCheckpointTracker` 是「seqno 连续性」的守门人**：
- `markSeqNoAsCompleted(seqNo)`：标记某 seqno 已完成。
- `getCheckpoint()`：返回**从 0 开始连续完成到的最大 seqno**（local checkpoint）。
- `waitForOpsToComplete(seqNo)`：等某个 seqno 完成（复制 `wait_for_active_shards` 相关）。

**local checkpoint 是「空洞敏感」的**：如果 seqno 5 完成了但 4 没完成，
checkpoint 仍是 3。这是第 17 章「为什么需要 global checkpoint」的前提。

---

## 5. ★★ 双 SearcherManager：internal vs external

```java
// Engine.java:705
public enum SearcherScope {
    EXTERNAL,   // 对用户可见的搜索（refresh 后更新）
    INTERNAL    // 引擎内部使用（realtime GET、索引缓冲写入、localCheckpoint 计算）
}
```

| 维度 | `internalSearcherManager` | `externalSearcherManager` |
| --- | --- | --- |
| 谁用 | Engine 内部（load version、realtime get、flush 后清 ver map） | `_search`/`_get`/聚合等用户查询 |
| 何时刷新 | `refresh(source, INTERNAL)`、`writeIndexingBuffer()` | `refresh(source, EXTERNAL)` |
| 可见性 | 可以看到「内部 refresh」产生的 NRT 段 | 只有显式 refresh 才更新 |

**为什么要有两个？**
- 用户查询必须看到一个**一致的、完整的** reader。如果内部操作（如 `writeIndexingBuffer`
  把内存索引缓冲刷到磁盘）能让用户查询看到，就会出现「同一个 search 看到一半新数据」的乱象。
- 而 realtime GET 需要能看到最新写入 —— 走 `INTERNAL` 并配合 `LiveVersionMap`。

**`SearchFactory`**（`InternalEngine` 内部类）实现了 `SearcherFactory`，
在 `newSearcher(IndexReader, IndexReader)` 里做**搜索预热**（`warmer.warm(...)`）。

---

## 6. ★★ `index()`：写入的决策中心（`:867`）

```java
@Override
public IndexResult index(Index index) throws IOException {
    assert Objects.equals(index.uid().field(), uidField) : index.uid().field();
    final boolean doThrottle = index.origin().isRecovery() == false;
    try (ReleasableLock releasableLock = readLock.acquire()) {          // ① 引擎读锁
        ensureOpen();
        assert assertIncomingSequenceNumber(index.origin(), index.seqNo());
        try (Releasable ignored = versionMap.acquireLock(index.uid().bytes());   // ② per-uid 锁
             Releasable indexThrottle = doThrottle ? throttle.acquireThrottle() : () -> {}) {
            lastWriteNanos = index.startTime();
            /* ③ 关键：计算 IndexingStrategy */
            final IndexingStrategy plan = indexingStrategyForOperation(index);       // :903

            final IndexResult indexResult;
            if (plan.earlyResultOnPreFlightError.isPresent()) {
                indexResult = plan.earlyResultOnPreFlightError.get();
                assert indexResult.getResultType() == Result.Type.FAILURE;
            } else if (plan.indexIntoLucene || plan.addStaleOpToLucene) {
                indexResult = indexIntoLucene(index, plan);                          // :1074
            } else {
                indexResult = new IndexResult(plan.versionForIndexing, plan.seqNoForIndexing, plan.currentNotFoundOrDeleted);
            }
            if (indexResult.getResultType() == Result.Type.SUCCESS) {
                final Translog.Location location;
                if (indexResult.getSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO
                        && indexResult.getSeqNo() != SequenceNumbers.NO_OPS_PERFORMED) {
                    location = translog.add(new Translog.Index(index, indexResult));      // ④ 写 translog
                } else if (indexResult.getSeqNo() == SequenceNumbers.NO_OPS_PERFORMED) {
                    location = translog.add(new Translog.NoOp(index.startTime(), indexResult.getSeqNo(), index.primaryTerm(), ...));
                } else {
                    location = null;
                }
                indexResult.setTranslogLocation(location);
            }
            if (!plan.currentNotFoundOrDeleted && plan.docIdInLucene != null) {
                // ⑤ 更新 version map
                versionMap.maybePutIndexUnderLock(index.uid().bytes(),
                    new IndexVersionValue(plan.docIdInLucene, location, indexResult.getVersion(),
                        indexResult.getSeqNo(), indexResult.getTerm()));
            }
            indexResult.setTook(System.nanoTime() - index.startTime());
            indexResult.freeze();
            return indexResult;
        }
    } catch (RuntimeException | IOException e) {
        try {
            maybeFailEngine("index", e);                       // ⑥ 判断是否要让引擎失效
        } catch (Exception inner) { e.addSuppressed(inner); }
        throw e;
    }
}
```

### 6.1 三层并发控制（★重点）

| 层 | 锁 | 目的 |
| --- | --- | --- |
| ① | `readLock`（引擎级） | 阻止 `flush`/`roll`/`forceMerge` 与写入并发 |
| ② | `versionMap.acquireLock(uid.bytes())` | **per-document 串行**：同一文档的写入不并发 |
| ③ | `throttle.acquireThrottle()` | 合并落后时**把并发写降为单线程** |

**★ 第 ② 层是关键**：不同的文档可以完全并发写；同一文档必须串行
（否则版本号 CAS 会乱）。`KeyedLock` 用 `uid` 做 key 保证这一点。

### 6.2 `indexingStrategyForOperation`（`:1010`）

```java
protected IndexingStrategy indexingStrategyForOperation(final Index index) throws IOException {
    if (index.origin() == Operation.Origin.PRIMARY) {
        return planIndexingAsPrimary(index);        // :1019
    } else {
        return planIndexingAsNonPrimary(index);     // :951 （REPLICA / PEER_RECOVERY / LOCAL_TRANSLOG_RECOVERY）
    }
}
```

#### `planIndexingAsPrimary`（`:1019`）：primary 上生成 seqno

```java
protected final IndexingStrategy planIndexingAsPrimary(Index index) throws IOException {
    assert index.origin() == Operation.Origin.PRIMARY : ...;
    final IndexingStrategy plan;
    VersionValue currentVersion = getVersionFromMap(index.uid().bytes());
    if (currentVersion == null) {
        if (canOptimizeAddDocument(index)) {                                  // ★ append-only 优化
            long seqNo = generateSeqNoForOperation(index);
            plan = IndexingStrategy.optimizedAppendOnly(seqNo, 1L);
        } else {
            // 需要检查 Lucene，看是否已存在
            plan = IndexingStrategy.processNormally(currentNotFoundOrDeleted,
                generateSeqNoForOperation(index), 1L, newVersion, currentVersion, ...);
        }
    } else if (currentVersion.isDelete() == false || currentVersion.term != getPrimaryTerm()) {
        // 已存在（未删除）→ 走 pathA 校验版本
        ...
        try {
            ...
            plan = IndexingStrategy.processNormally(...);
        } catch (VersionConflictEngineException e) {
            plan = IndexingStrategy.skipDueToVersionConflict(e, currentNotFoundOrDeleted, currentVersion, getPrimaryTerm());
        }
    }
    return plan;
}
```

#### `planIndexingAsNonPrimary`（`:951`）：副本按 seqno 判定

```java
protected final IndexingStrategy planIndexingAsNonPrimary(Index index) throws IOException {
    final IndexingStrategy plan;
    boolean hasUnassignedSeqNo = index.seqNo() == SequenceNumbers.UNASSIGNED_SEQ_NO;
    ...
    if (hasUnassignedSeqNo == false && index.origin().isRecovery() == false) {
        // ★ 操作在 recovery 之外进入且已有 seqno → 用 MSU 优化判断能否跳过
        final long maxSeqNoOfUpdatesOrDeletes = getMaxSeqNoOfUpdatesOrDeletes();
        if (maxSeqNoOfUpdatesOrDeletes < index.seqNo() && index.seqNo() > index.version()) {
            plan = IndexingStrategy.optimizedAppendOnly(index.seqNo(), index.version());
        } else {
            // 需要加载 Lucene 里的版本，走正常流程
            ...
            switch (opVsLucene) {
                case OP_NEWER:
                    plan = IndexingStrategy.processNormally(...);
                    break;
                case OP_STALE_OR_EQUAL:
                    plan = softDeleteEnabled ?
                        IndexingStrategy.processAsStaleOp(true, index.seqNo(), index.version()) :
                        IndexingStrategy.processButSkipLucene(false, index.seqNo(), index.version());
                    break;
                case LUCENE_DOC_NOT_FOUND:
                    // 副本上找不到 doc → 说明是「已被删除的旧操作」，标记为 stale
                    plan = IndexingStrategy.processNormally(true, index.seqNo(), index.version(), ...);
                    break;
            }
        }
    } else {
        // 无 seqno 或 recovery → 按「当前版本」判断
        ...
    }
    return plan;
}
```

### 6.3 `IndexingStrategy`（`:1166`）六种形态

| 工厂方法 | 行号 | 含义 | 干什么 |
| --- | --- | --- | --- |
| `optimizedAppendOnly(seqNo, version)` | `:1195` | ★ 文档不存在 + 自动生成 id → 直接 append | 跳过 version 检查，`addDocument` |
| `overrideExistingAsIfNotThere(seqNo, version)` | `:1214` | primary 上无 seqno 的写入（强制覆盖） | 不查 Lucene，直接用 `updateDocument(uid, doc)` |
| `processNormally(...)` | `:1208` | 正常处理（可能需要 delete+add） | 按 `currentNotFoundOrDeleted` 决定 |
| `processButSkipLucene(...)` | `:1220` | 只更新 seqno/version map，**不写 Lucene** | 已存在的旧操作（幂等跳过） |
| `processAsStaleOp(addToLucene, seqNo, version)` | `:1226` | soft-delete 模式下也要「占位」写 Lucene | 写入一条 stale 文档（保留 tombstone） |
| `skipDueToVersionConflict(e, ...)` | `:1200` | 版本冲突 → 直接失败 | `earlyResultOnPreFlightError` |

**★★ `optimizedAppendOnly` 是写吞吐的关键优化**：
- 条件（`canOptimizeAddDocument:772`）：`autoGeneratedIdTimestamp != UNKNOWN`（自动生成 id）
  且 `engineConfig.isAutoGeneratedIDsOptimizationEnabled()`。
- 因为自动生成的 id 一定唯一，所以**不需要检查是否已存在**，
  直接 `addDocument` + **不需要存 version map**（`maybePutIndexUnderLock` 里
  `if (maps.isSafeAccessMode())` 为假时就只 `markAsUnsafe`）。
- 效果：**日志/指标类场景（大量自动 id 小文档）内存占用大幅下降**。
- 代价：一旦某个 id 需要「安全检查」（比如按 id 写入），
  `enforceSafeAccess()` 会强制开启安全模式，并触发一次 refresh。

**`canOptimizeAddDocument:772`** 里还有 `assertPrimaryCanOptimizeAddDocument`：
如果 `index.autoGeneratedIdTimestamp() == UNKNOWN` 就不能走优化。

### 6.4 `indexIntoLucene`（`:1074`）

```java
private IndexResult indexIntoLucene(Index index, IndexingStrategy plan) throws IOException {
    try {
        if (plan.addStaleOpToLucene) {
            addStaleDocs(index.docs(), indexWriter);                       // 写入 stale 文档（soft-delete 保留 tombstone）
        } else if (plan.useLuceneUpdateDocument) {
            assert assertMaxSeqNoOfUpdatesIsAdvanced(index.uid(), plan.seqNoForIndexing, true, true);
            updateDocs(index.uid(), index.docs(), indexWriter);            // ★ delete + add
        } else {
            assert plan.currentNotFoundOrDeleted;
            if (index.origin() == Operation.Origin.LOCAL_TRANSLOG_RECOVERY
                    && index.getAutoGeneratedIdTimestamp() != Translog.UNSET_AUTO_GENERATED_TIMESTAMP
                    && index.getAutoGeneratedIdTimestamp() < maxUnsafeAutoIdTimestamp.get()) {
                // 恢复时同 id+时间戳的重复写入 → 直接跳过（幂等）
                plan = plan.withVersionAndSeqNo();
                return new IndexResult(plan.versionForIndexing, plan.seqNoForIndexing, plan.currentNotFoundOrDeleted);
            }
            addDocs(index.docs(), indexWriter);                            // ★ 新增
        }
        return new IndexResult(plan.versionForIndexing, plan.seqNoForIndexing, plan.currentNotFoundOrDeleted);
    } catch (Exception ex) { ... maybeFailEngine(index.operationType().getLowLevelName(), ex); throw ex; }
}
```

| 写入方式 | Lucene 调用 | 何时 |
| --- | --- | --- |
| `addDocs`（`:1145`） | `indexWriter.addDocument(doc)` | 新增（不存在或 append-only） |
| `updateDocs` | `indexWriter.updateDocument(uid, doc)` | 覆盖已有文档（内部 = delete + add） |
| `addStaleDocs` | `indexWriter.addDocument`（带 soft-delete 标记） | soft-delete 下的 stale 操作 |

**★ `updateDocument` 的语义**：Lucene 会「先用 uid Term 删除旧文档，再添加新文档」。
但因为删除是**逻辑删除**（写 tombstone），旧段里的数据仍占空间，直到 merge。
这就是「频繁更新会导致段膨胀」的原因（第 16 章）。

**★ `LOCAL_TRANSLOG_RECOVERY` + 相同 `autoGeneratedIdTimestamp` 的跳过逻辑（`:1085-1092`）**：
本地 translog 恢复时，同一个自动生成 id 的文档可能被写两次
（一次进 Lucene buffer、一次进 translog，重启后回放），这里靠时间戳判断并跳过，保证幂等。

---

## 7. ★★ `LiveVersionMap`：实时读的基石

### 7.1 三段结构（`LiveVersionMap.java:37`）

```
LiveVersionMap
├── maps (volatile Maps)
│   ├── current : VersionLookup   ← 自上次 refresh 以来的所有写入
│   └── old     : VersionLookup   ← refresh 期间被"换下来"的 current
└── tombstones : Map<BytesRef, DeleteVersionValue>   ← 删除记录（保留 gc_deletes 时间）
```

**查找顺序**（`getUnderLock:272`）：`current` → `old` → `tombstones`。

**为什么需要 `old`？**
```
时间线：
  T1: beforeRefresh()  → 建新的 current，旧的 current 变 old
  T2: Lucene 正在 reopen（慢）
  T3: 用户 GET → 新写入在 current，T1 之前的写在 old，都能查到 ✓
  T4: afterRefresh()   → 丢弃 old（此时新 reader 已包含 old 里的数据）
```

### 7.2 refresh 回调（`:240`/`:252`）

```java
@Override
public void beforeRefresh() throws IOException {                    // :240
    maps = maps.buildTransitionMap();       // current → new current, 旧 current 变 old
}

@Override
public void afterRefresh(boolean didRefresh) throws IOException {    // :252
    maps = maps.invalidateOldMap();         // 丢弃 old；tombstones 不动（按 gc_deletes 过期）
}
```

**★★ 这就是「为什么 refresh 后能搜到、又能清内存」的机制**：
- refresh 前：新写入在 `current`。
- refresh 时：Lucene 打开新 reader 包含这些写入；`old` 暂时保留以防并发 GET 漏读。
- refresh 后：`old` 丢弃，内存释放。

**注意**：即使 `didRefresh == false`（Lucene 认为没变化），也丢弃 `old` —— 因为在
`beforeRefresh` 里换了 map，有些并发写入可能「看起来新实际已被上一个 reader 包含」。

### 7.3 写入与不安全优化（`:311`/`:326`/`:340`）

```java
// 需要安全检查时（force）才写 version map
void putIndexUnderLock(BytesRef uid, IndexVersionValue version) {    // :326
    maps.put(uid, version);
    removeTombstoneUnderLock(uid);
}

// 不需要安全检查时：只删 tombstone + 标记 unsafe（★ 不存 version，省内存）
void maybePutIndexUnderLock(BytesRef uid, IndexVersionValue version) {   // :311
    Maps maps = this.maps;
    if (maps.isSafeAccessMode()) {
        putIndexUnderLock(uid, version);
    } else {
        removeTombstoneUnderLock(uid);
        maps.current.markAsUnsafe();
        assert putAssertionMap(uid, version);      // 只写断言用的 map
    }
}

void putDeleteUnderLock(BytesRef uid, DeleteVersionValue version) {   // :340
    putTombstone(uid, version);
    maps.remove(uid, version);
}
```

**★ tombstone（删除墓碑）的存在意义**：
删除一个文档后，Lucene 里的段还是旧的（删是逻辑删除）。
如果立刻「忘掉」这个删除，一个并发 GET 可能读到旧段里的老版本。
所以删除记录要保留 `index.gc_deletes`（默认 60s）时间。

**`enforceSafeAccess()`（`:300`）**：一旦某个 id 需要安全访问（按 id 写入），
就把整个 map 切成安全模式，此后所有写入都记 version map，并触发一次 refresh 让
之前「unsafe 期间」的写入先落地。

---

## 8. ★ `delete()`（`:1274`）与 `DeletionStrategy`

流程与 `index` 对称：

```java
public DeleteResult delete(Delete delete) throws IOException {
    ...
    try (ReleasableLock lock = readLock.acquire()) {
        ensureOpen();
        try (Releasable ignored = versionMap.acquireLock(delete.uid().bytes());
             Releasable indexThrottle = doThrottle ? throttle.acquireThrottle() : () -> {}) {
            final DeletionStrategy plan = deletionStrategyForOperation(delete);    // :1284
            if (plan.earlyResultOnPreflightError.isPresent()) {
                deleteResult = plan.earlyResultOnPreflightError.get();
            } else if (plan.deleteFromLucene || plan.addStaleOpToLucene) {
                deleteResult = deleteInLucene(delete, plan);                       // :1421
            } else {
                deleteResult = new DeleteResult(plan.versionOfDeletion, plan.seqNoOfDeletion, plan.currentlyDeleted == false);
            }
            if (deleteResult.getResultType() == Result.Type.SUCCESS) {
                final Translog.Location location;
                if (deleteResult.getSeqNo() != UNASSIGNED_SEQ_NO) {
                    location = translog.add(new Translog.Delete(delete, deleteResult));   // 写 translog
                } else { location = null; }
                deleteResult.setTranslogLocation(location);
            }
            if (deleteResult.getResultType() == Result.Type.SUCCESS) {
                versionMap.putDeleteUnderLock(delete.uid().bytes(),
                    new DeleteVersionValue(deleteResult.getVersion(), deleteResult.getSeqNo(),
                        delete.primaryTerm(), delete.startTime(), deleteResult.getTranslogLocation()));
            }
            deleteResult.setTook(...); deleteResult.freeze();
            return deleteResult;
        }
    } catch (RuntimeException | IOException e) { maybeFailEngine("delete", e); throw e; }
}
```

**`DeletionStrategy` 形态**（`:1334`/`planDeletionAsPrimary` 等）与 `IndexingStrategy` 类似：
- `optimizedAppendOnly`（删除也有 append-only？这里是「已知不存在」的快速路径）
- `processNormally`（正常删除）
- `processButSkipLucene`（幂等跳过）
- `processAsStaleOp`（soft-delete 保留）
- `skipDueToVersionConflict`

**★ 删除也要写 translog**（`Translog.Delete`）——因为 lucene 删除是逻辑删除，
必须能在恢复时重放。

---

## 9. ★★ seqno / version / primaryTerm 三元组

这是 ES 6.x 一致性的核心，Engine 层负责生成与校验。

```
每个文档的 VersionValue 有三元组：
  version      : 用户可见的版本号（乐观锁用）；primary 上生成，副本复制
  seqNo        : 全局单调递增序号，用于复制与恢复定位
  term         : primary term（主任期），用于区分"谁写的"
```

### 9.1 `compareOpToLuceneDocBasedOnSeqNo`（`:684`）

```java
private OpVsLuceneDocStatus compareOpToLuceneDocBasedOnSeqNo(final Operation op) throws IOException {
    assert op.seqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO : "resolving ops based on seq# but no seqNo is found";
    final OpVsLuceneDocStatus status;
    VersionValue versionValue = getVersionFromMap(op.uid().bytes());
    if (versionValue != null) {
        if (op.seqNo() > versionValue.seqNo) {
            status = OpVsLuceneDocStatus.OP_NEWER;
        } else if (op.seqNo() == versionValue.seqNo) {
            assert versionValue.term == op.primaryTerm() : "primary term not matched; ...";   // ★ 同 seqno 必须同 term
            status = OpVsLuceneDocStatus.OP_STALE_OR_EQUAL;
        } else {
            status = OpVsLuceneDocStatus.OP_STALE_OR_EQUAL;
        }
    } else {
        // 从 Lucene 里找
        try (Searcher searcher = acquireSearcher("load_seq_no", SearcherScope.INTERNAL)) {
            final DocIdAndSeqNo docAndSeqNo = VersionsAndSeqNoResolver.loadDocIdAndSeqNo(searcher.reader(), op.uid());
            if (docAndSeqNo == null) {
                status = OpVsLuceneDocStatus.LUCENE_DOC_NOT_FOUND;
            } else if (op.seqNo() > docAndSeqNo.seqNo) {
                status = docAndSeqNo.isLive ? OP_NEWER : LUCENE_DOC_NOT_FOUND;
            } else if (op.seqNo() == docAndSeqNo.seqNo) {
                status = OP_STALE_OR_EQUAL;
            } else {
                status = OP_STALE_OR_EQUAL;
            }
        }
    }
    return status;
}

enum OpVsLuceneDocStatus {          // :675
    OP_NEWER,              // 操作比 Lucene 里更新 → 要写
    OP_STALE_OR_EQUAL,     // 操作不比 Lucene 新 → 跳过（幂等）
    LUCENE_DOC_NOT_FOUND   // Lucene 里没有 → 新增
}
```

**★★ `assert versionValue.term == op.primaryTerm()`（`:693`）是硬不变量**：
**同一个 seqno 的操作，term 必须相同**。因为 seqno 是全局唯一的「位置」，
一个位置只能被一个 term 写入。反之，term 相同 seqno 也相同 → 是同一条操作的重放。

### 9.2 `planIndexingAsNonPrimary` 的三种判定结果

| `OpVsLuceneDocStatus` | 处理 |
| --- | --- |
| `OP_NEWER` | `processNormally`：真正写入 |
| `OP_STALE_OR_EQUAL` | `processButSkipLucene`（非 soft-delete）或 `processAsStaleOp`（soft-delete）：只推进 seqno |
| `LUCENE_DOC_NOT_FOUND` | 视情况 `processNormally(currentNotFoundOrDeleted=true)` |

**★ 这就是「副本重放幂等」的实现**：
primary 重发同一个操作，副本第一次写入（OP_NEWER），第二次发现 seqno 已存在（OP_STALE_OR_EQUAL），
直接跳过 Lucene 写入，只保证 seqno 推进 —— **不会产生重复文档**。

### 9.3 MSU 优化（max_seq_no_of_updates_or_deletes）

`maxSeqNoOfNonAppendOnlyOperations`（`InternalEngine:149`）+ `getMaxSeqNoOfUpdatesOrDeletes()`：
- 记录「所有**非** append-only 操作（即 update/delete 这类会查 Lucene 的操作）的最大 seqno」。
- **★ 如果某操作 `seqNo > maxSeqNoOfUpdatesOrDeletes` 且 `seqNo > version`，
  说明「比它小的所有操作都不是 update/delete」，因此它不可能与已有文档冲突 → 可以走 append-only。**
- 这是为了在副本上也能用 append-only 快路径，大幅提升复制吞吐。
- 第 17 章会把 `max_seq_no_of_updates` 作为副本请求的一部分传下去。

---

## 10. ★ `get()`：realtime 读取（`:616`）

```java
@Override
public GetResult get(Get get, BiFunction<String, SearcherScope, Searcher> searcherFactory) throws EngineException {
    try (ReleasableLock ignored = readLock.acquire()) {
        ensureOpen();
        SearcherScope scope;
        if (get.realtime()) {
            VersionValue versionValue = null;
            try (Releasable ignore = versionMap.acquireLock(get.uid().bytes())) {
                versionValue = getVersionFromMap(get.uid().bytes());          // ① 查版本表
            }
            if (versionValue != null) {
                if (versionValue.isDelete()) {
                    return GetResult.NOT_EXISTS;                              // ② 已被删除 → 不存在
                }
                if (get.versionType().isVersionConflictForReads(versionValue.version, get.version())) {
                    throw new VersionConflictEngineException(...);            // ③ 版本冲突
                }
                if (get.isReadFromTranslog()) {                               // ④ update 专用：直接从 translog 读最新 source
                    if (versionValue.getLocation() != null) {
                        Translog.Operation operation = translog.readOperation(versionValue.getLocation());
                        if (operation != null) {
                            Translog.Index index = (Translog.Index) operation;
                            TranslogLeafReader reader = new TranslogLeafReader(index, indexVersionCreated);
                            return new GetResult(new Searcher("realtime_get", new IndexSearcher(reader), reader::close),
                                new DocIdAndVersion(0, index.version(), index.seqNo(), index.primaryTerm(), reader, 0));
                        }
                    } else {
                        trackTranslogLocation.set(true);                      // 以后要记 location
                    }
                }
                refresh("realtime_get", SearcherScope.INTERNAL);              // ⑤ 让写入可见（内部 refresh）
            }
            scope = SearcherScope.INTERNAL;
        } else {
            scope = SearcherScope.EXTERNAL;     // 非 realtime → 走外部 reader（已 refresh 的）
        }
        return getFromSearcher(get, searcherFactory, scope);
    }
}
```

**★★ realtime GET 的完整逻辑（这是面试高频题）**：

```
get.realtime() == true（GET /idx/_doc/1、mget）?
  ├─ 查 LiveVersionMap
  │    ├─ 有删除记录 → 直接 NOT_EXISTS（不用碰 Lucene）
  │    ├─ 有写入记录 → 版本校验 → 需要时从 translog 读 source（update 用）
  │    │             → refresh(INTERNAL) → 从 internal reader 读
  │    └─ 没有 → 说明写入已经 refresh 过了（在 old → 已丢弃）或本来就没写过
  │             → refresh(INTERNAL) 后从 internal reader 读
  └─ 走 internal searcher
```

- **`get.isReadFromTranslog()`**（`Get:1554`）只有 `_update` 路径为 true，
  因为它需要拿到**最新 source**（可能还没 refresh 到 Lucene）。
  普通 `_get` 永远从 reader 读，保证「一致性」。
- **`refresh(INTERNAL)` 不是强刷**（`maybeRefreshBlocking` 只在有变化时真正 reopen），
  所以 realtime GET 通常不贵。
- **`getFromSearcher`**（`:740` 附近）从 reader 里找 `_id` 对应的 docId，
  再按 `_source` 过滤返回。

> **为什么 GET 比 search 快且强一致？** 因为 GET 走 version map + reader 定点查找，
> 不经过 query 分析、不依赖 refresh 的可见性判断；而 search 走 external reader，
> 需要 refresh 后才能看到新数据。

---

## 11. `refresh()` 与 `writeIndexingBuffer()`（`:1596`/`:1643`）

```java
public void refresh(String source) throws EngineException {
    refresh(source, SearcherScope.EXTERNAL);
}

final void refresh(String source, SearcherScope scope) throws EngineException {
    final long localCheckpointBeforeRefresh = getLocalCheckpoint();
    try (ReleasableLock lock = readLock.acquire()) {
        ensureOpen();
        if (store.tryIncRef()) {
            try {
                ReferenceManager<IndexSearcher> referenceManager = getReferenceManager(scope);   // internal 或 external
                referenceManager.maybeRefreshBlocking();       // ★ Lucene：把索引缓冲变成可搜索段
            } finally { store.decRef(); }
            lastRefreshedCheckpointListener.updateRefreshedCheckpoint(localCheckpointBeforeRefresh);
        }
    } catch (AlreadyClosedException e) { failOnTragicEvent(e); throw e; }
      catch (Exception e) { failEngine("refresh failed source[" + source + "]", e); throw new RefreshFailedEngineException(shardId, e); }
    maybePruneDeletes();          // 顺手清 tombstone
    mergeScheduler.refreshConfig();   // 应用新的 merge 设置
}

public void writeIndexingBuffer() throws EngineException {
    refresh("write indexing buffer", SearcherScope.INTERNAL);    // 只刷内部
}
```

| 概念 | 含义 |
| --- | --- |
| `maybeRefreshBlocking` | Lucene `ReferenceManager` 的「有变化才 reopen，否则立刻返回」 |
| `lastRefreshedCheckpointListener` | 记录「已经被 refresh 覆盖到的 local checkpoint」（第 17 章的 `lastRefreshedCheckpoint`） |
| `writeIndexingBuffer` | 把 Lucene 内存索引缓冲刷到磁盘（`IndexWriter` 的 RAM buffer），减少内存压力 |

**★ `lastRefreshedCheckpoint`（`InternalEngine:159`）的作用**：
global checkpoint 只能推进到「已被 refresh 的 local checkpoint」为止，
因为只有 refresh 后副本才能看到这批数据。这是第 17 章的关键约束。

---

## 12. `noOp()`（`:1525`）与 trim

```java
@Override
public NoOpResult noOp(final NoOp noOp) {
    NoOpResult noOpResult;
    try (Releasable ignored = noOpKeyedLock.acquire(noOp.getSeqNo())) {
        noOpResult = noOp(noOp);      // 内部：写 Translog.NoOp + localCheckpointTracker.markSeqNoAsCompleted
    } catch (final Exception e) {
        noOpResult = new NoOpResult(getPrimaryTerm(), noOp.getSeqNo(), e);
    }
    return noOpResult;
}
```

**`noOpKeyedLock`**：按 seqno 加锁，保证同一 seqno 的 noOp 不会并发写。

**`trimOperationsFromTranslog`（`Engine:335`）→ `Translog.trimOperations(belowTerm, aboveSeqNo)`（第 14 章）**：
peer recovery 时告诉 translog「这些操作不用再保留」。

---

## 13. 与 `IndexShard` 的接线

`IndexShard` 是 Engine 的持有者，二者关系：

```
TransportShardBulkAction
   → IndexShard.applyIndexOperationOnPrimary(version, versionType, source, ifSeqNo, ifPrimaryTerm, autoGenTs, isRetry)   (IndexShard:715)
        → applyIndexOperation(getEngine(), UNASSIGNED_SEQ_NO, operationPrimaryTerm, ..., Origin.PRIMARY, source)          (IndexShard:729)
             ├─ prepareIndex(...)                        // 解析文档、生成 uid、检查 mapping
             ├─ operation.parsedDoc().dynamicMappingsUpdate() != null → new Engine.IndexResult(update)   ← MAPPING_UPDATE_REQUIRED
             └─ index(engine, operation)  → Engine.index(...)                                             (IndexShard 内部)
   → IndexShard.applyIndexOperationOnReplica(seqNo, version, versionType, autoGenTs, isRetry, source)   (IndexShard:722)
        → applyIndexOperation(..., Origin.REPLICA, ...)
```

**★ 关键区别只在 `origin` 和 `seqNo`**：

```java
// IndexShard:715 primary：seqNo = UNASSIGNED_SEQ_NO（让 engine 生成）
public Engine.IndexResult applyIndexOperationOnPrimary(long version, VersionType versionType, SourceToParse sourceToParse,
        long ifSeqNo, long ifPrimaryTerm, long autoGeneratedTimestamp, boolean isRetry) {
    return applyIndexOperation(getEngine(), SequenceNumbers.UNASSIGNED_SEQ_NO, operationPrimaryTerm, version, versionType,
        ifSeqNo, ifPrimaryTerm, autoGeneratedTimestamp, isRetry, Engine.Operation.Origin.PRIMARY, sourceToParse);
}
// IndexShard:722 replica：seqNo 由 primary 传下来
public Engine.IndexResult applyIndexOperationOnReplica(long seqNo, long version, VersionType versionType,
        long autoGeneratedTimeStamp, boolean isRetry, SourceToParse sourceToParse) {
    return applyIndexOperation(getEngine(), seqNo, operationPrimaryTerm, version, versionType, UNASSIGNED_SEQ_NO, 0,
        autoGeneratedTimeStamp, isRetry, Engine.Operation.Origin.REPLICA, sourceToParse);
}
```

以及 `applyIndexOperation:733` 的断言：
```java
assert opPrimaryTerm <= this.operationPrimaryTerm : "op term [ " + opPrimaryTerm + " ] > shard term [" + this.operationPrimaryTerm + "]";
```
**★ 操作 term 不能超过 shard 当前 primary term**。超过说明这是「未来任期」的操作，
通常意味着旧 shard 收到了新 term 的请求（选主竞态），直接断言失败（生产中断言关闭，
由上层三重校验拦截）。

**`IndexShard.operationPrimaryTerm`**：在 `IndexShard` 的 primary 状态上设置，
与 `ReplicationTracker` 的 term 一致（第 17 章）。

---

## 14. 配置速查

| 设置 | 默认 | 含义 |
| --- | --- | --- |
| `index.gc_deletes` | `60s` | tombstone 保留时间（`getGcDeletesInMillis`） |
| `index.mapping.total_fields.limit` | `1000` | 字段上限（Engine 解析时校验） |
| `index.auto_generated_ids.optimization`（内部） | `true` | 是否启用 append-only 优化（`isAutoGeneratedIDsOptimizationEnabled`） |
| `index.soft_deletes.enabled` | `false`（6.x 默认，需显式开启） | soft-delete（保留历史操作，支持「操作级」恢复） |
| `index.soft_deletes.retention_lease.period` | `12h` | soft-delete 保留租约 |
| `index.merge.scheduler.max_thread_count` | `min(4, proc/2)` → 机械盘建议 1 | 合并线程数（第 16 章） |
| `index.translog.*` | — | 见第 14 章 |
| `indices.memory.index_buffer_size` | `10%` 堆 | Lucene 索引缓冲大小 |

**引擎级监控点**（`GET _nodes/stats/indices`）：

| 指标 | 含义 |
| --- | --- |
| `segments.count` / `segments.memory` | 段数量/内存 |
| `indexing.index_total` / `index_time_in_millis` | 索引计数/耗时 |
| `indexing.is_throttled` / `throttle_time_in_millis` | 是否被限流（合并落后） |
| `refresh.total` / `refresh_time_in_millis` | refresh 次数/耗时 |
| `merges.current` / `merges.total_time_in_millis` | 合并量（第 16 章） |
| `translog.uncommitted_size_in_bytes` | 未 commit translog（第 14 章：flush 触发） |
| `docs.deleted` | 逻辑删除数（merge 才能回收） |

---

## 15. 故障排查对照表

| 现象 | Engine 层根因 | 定位手段 |
| --- | --- | --- |
| 内存高（version map 撑爆） | 大量**按 id 写入**使 `enforceSafeAccess` 开启，version map 记录所有文档 | `GET _nodes/stats/indices/segments`，检查是否用了自定义 id |
| `version_conflict_engine_exception` | `skipDueToVersionConflict` | 确认 versionType / if_seq_no |
| 文档重复出现 | append-only 优化在「其实是 update」的场景被误用 | 检查 `autoGeneratedIdTimestamp`；见 `assertPrimaryCanOptimizeAddDocument` |
| 写被限流 `is_throttled` | merge 落后 / writeIndexingBuffer 慢 | 看 merge stats；加磁盘 IOPS / 调 `index.merge.scheduler.max_thread_count=1` |
| search 看不到刚写的数据 | external reader 未 refresh | 用 GET 或 `?refresh=true`；调小 `refresh_interval` |
| `translog` 恢复慢 | 需回放大量操作 | 见第 14 章 retention 设置 |
| `engine is closed` / shard 被 fail | `maybeFailEngine` 判定为致命 | 看日志中 `tragic event`；通常是磁盘错误 |
| 段数过多、merge 频繁 | 频繁 update（delete+add） | 减少 update、或调大 refresh_interval |
| `mapper_parsing_exception` / `MAPPING_UPDATE_REQUIRED` 反复 | 动态 mapping 抖动 | 提前定义 mapping |
| 长 GC / `live docs` 内存 | tombstones 或 deleted docs 过多 | `POST _forcemerge?only_expunge_deletes=true` |
| `No such file / checksum mismatch` | 存储损坏 | 见第 14 章 TranslogCorruptedException |

---

## 16. 动手实验

**15-1 观察 realtime GET 行为**
```bash
curl -XPUT "localhost:9200/rt/_settings" -H 'Content-Type: application/json' -d '{"index":{"refresh_interval":"-1"}}'
curl -XPUT "localhost:9200/rt/_doc/1" -H 'Content-Type: application/json' -d '{"v":"aaa"}'
curl -XGET "localhost:9200/rt/_doc/1"                       # 不 refresh 也能读到 → realtime
curl -XGET "localhost:9200/rt/_search?q=v:aaa"              # 搜不到 → 说明 search 依赖 refresh
curl -XPOST "localhost:9200/rt/_refresh"
curl -XGET "localhost:9200/rt/_search?q=v:aaa"              # 现在能搜到
```

**15-2 观察 version map 与删除墓碑**
```bash
curl -XDELETE "localhost:9200/rt/_doc/1"
curl -XGET "localhost:9200/rt/_doc/1"    # 立刻 404（墓碑生效，还没 merge）
curl -XGET "localhost:9200/rt/_stats/docs,store?pretty"  # deleted docs 仍在
curl -XPOST "localhost:9200/rt/_forcemerge?only_expunge_deletes=true&pretty"
curl -XGET "localhost:9200/rt/_stats/docs,store?pretty"  # deleted 归零
```

**15-3 append-only 优化：自动 id vs 指定 id 的内存差异**
```bash
# 自动 id（默认）→ unsafe 快速路径
for i in $(seq 1 10000); do curl -s -XPOST "localhost:9200/ao/_doc" -H 'Content-Type: application/json' -d '{"v":1}' >/dev/null; done
curl -XGET "localhost:9200/_nodes/stats/indices/segments?pretty" | head -50
# 指定 id → 触发安全模式
for i in $(seq 1 10000); do curl -s -XPUT "localhost:9200/ao2/_doc/$i" -H 'Content-Type: application/json' -d '{"v":1}' >/dev/null; done
curl -XGET "localhost:9200/_nodes/stats/indices/segments?pretty" | head -50
```

**15-4 观察 update 导致段膨胀**
```bash
curl -XPUT "localhost:9200/up/_doc/1" -H 'Content-Type: application/json' -d '{"v":0}'
for i in $(seq 1 200); do curl -s -XPOST "localhost:9200/up/_update/1" -H 'Content-Type: application/json' -d '{"doc":{"v":'"$i"'}}' >/dev/null; done
curl -XPOST "localhost:9200/up/_flush"
curl -XGET "localhost:9200/up/_stats/docs,store,segments?pretty"
# 观察 deleted docs > 0；`_forcemerge` 后回收
curl -XPOST "localhost:9200/up/_forcemerge?max_num_segments=1"
```

**15-5 seqno 与 version**
```bash
curl -XPUT "localhost:9200/sq/_doc/1?refresh=true" -H 'Content-Type: application/json' -d '{"v":1}'
curl -XGET "localhost:9200/sq/_doc/1?_source=false&pretty"   # 看 _seq_no / _primary_term
curl -XPUT "localhost:9200/sq/_doc/1?if_seq_no=<seq>&if_primary_term=<term>" -H 'Content-Type: application/json' -d '{"v":2}'
```

**15-6 副本重放幂等（需双节点）**
停掉副本节点 → 写几条 → 启动副本 → 观察日志里 recovery 与 translog 重放；
再用 `GET _cat/recovery?v&active_only` 观察。

**15-7 限流与合并**
```bash
curl -XPUT "localhost:9200/th/_settings" -H 'Content-Type: application/json' -d '{"index":{"merge.scheduler.max_thread_count":1}}'
# 大量写入并频繁 refresh，观察 indexing.is_throttled
curl -XGET "localhost:9200/_nodes/stats/indices/indexing,merges?pretty" | grep -i throttle
```

**15-8 观察 refresh 触发 version map 清理**
在 `LiveVersionMap.beforeRefresh:240` / `afterRefresh:252` 打断点，
执行 `POST /idx/_refresh`，观察 `maps.current` / `maps.old` 的切换。

**15-9 断点追踪 `index()` 的 plan 选择**
在 `InternalEngine.index:903`（`indexingStrategyForOperation` 返回处）打断点，分别：
- 自动 id 写入 → 期望 `optimizedAppendOnly`；
- 指定 id 首次写入 → 期望 `processNormally`；
- 同 id 再写（version 冲突）→ 期望 `skipDueToVersionConflict`。

**15-10 soft-delete 实验**
```bash
curl -XPUT "localhost:9200/sd" -H 'Content-Type: application/json' \
  -d '{"settings":{"index.soft_deletes.enabled":true}}'
# 写入并删除，观察 translog 中是否有 stale 操作（配合第 18 章）
```

---

## 17. 自测题

1. 为什么要抽象出 `Engine`？Lucene 本身缺了什么能力？
2. `Engine.SearcherScope.EXTERNAL` 和 `INTERNAL` 的区别是什么？为什么要两个 SearcherManager？
3. `Engine.Result.Type.MAPPING_UPDATE_REQUIRED` 有什么用？谁处理它？
4. `InternalEngine` 构造时为什么 `pendingTranslogRecovery.set(true)`？
5. `InternalEngine.index()` 里有哪三层锁？分别保护什么？
6. `indexingStrategyForOperation` 怎么区分 primary 和 non-primary？两条路径的 seqno 来源有何不同？
7. `IndexingStrategy.optimizedAppendOnly` 的适用条件是什么？它省掉了什么？代价是什么？
8. `canOptimizeAddDocument` 与 `enforceSafeAccess` 的关系？
9. `planIndexingAsNonPrimary` 里 `OP_STALE_OR_EQUAL` 为什么可以「跳过 Lucene 写入」而不出错？
10. `indexIntoLucene` 里 `useLuceneUpdateDocument` 与 `addStaleOpToLucene` 分别对应什么？
11. `LiveVersionMap` 的 `current`/`old`/`tombstones` 各是什么？为什么要有 `old`？
12. tombstone 为什么要保留一段时间？由哪个设置控制？
13. realtime GET 的完整路径是什么？为什么 `_update` 要从 translog 读？
14. `compareOpToLuceneDocBasedOnSeqNo` 的三个返回值分别对应什么处理？
15. 为什么「同 seqno 的操作 term 必须相同」？
16. `LocalCheckpointTracker` 的 local checkpoint 是怎么算的？为什么是「空洞敏感」的？
17. `lastRefreshedCheckpoint` 的作用是什么？它和 global checkpoint 有什么关系？
18. MSU（`max_seq_no_of_updates_or_deletes`）优化的原理是什么？
19. `IndexShard.applyIndexOperationOnPrimary` 与 `...OnReplica` 的唯一区别是什么？
20. `assert opPrimaryTerm <= operationPrimaryTerm` 为什么要存在？
21. `LUCENE_DOC_NOT_FOUND` 在副本上意味着什么？为什么也要写入？
22. `updateDocument` 的内部语义是什么？为什么频繁 update 会导致段膨胀？

---

## 18. 关键类速查表

| 类 | 路径 | 一句话 |
| --- | --- | --- |
| `Engine` | `index/engine/Engine.java` | ★ 引擎抽象（`:111`；`index:376`、`delete:386`、`get:636`、`flush:1055`） |
| `Engine.Operation/Index/Delete/NoOp` | 同上 `:1253`/`:1344`/`:1443`/`:1504` | 写操作 |
| `Engine.Result/IndexResult/DeleteResult` | 同上 `:395`/`:514`/`:546` | 结果 |
| `Engine.SearcherScope` | 同上 `:705` | EXTERNAL / INTERNAL |
| `Engine.Get/GetResult` | 同上 `:1554`/`:1609` | realtime 读 |
| `Engine.CommitId` / `IndexCommitRef` | 同上 `:1710`/`:1765` | commit 句柄 |
| `InternalEngine` | `index/engine/InternalEngine.java` | ★ 默认引擎（`:110`；`index:867`、`plan*:951/1019`、`indexIntoLucene:1074`、`delete:1274`、`get:616`、`refresh:1596`、`flush:1743`） |
| `InternalEngine.IndexingStrategy` | 同上 `:1166` | ★ 六种写入计划 |
| `InternalEngine.DeletionStrategy` | 同上（约 `:1450` 附近） | 删除计划 |
| `InternalEngine.OpVsLuceneDocStatus` | 同上 `:675` | 三种比较结果 |
| `InternalEngine.IndexThrottle` | `Engine.java:271` | 写限流 |
| `LiveVersionMap` | `index/engine/LiveVersionMap.java` | ★ 实时版本表（`:240`/`:252` refresh 回调、`:311` unsafe、`:340` tombstone） |
| `VersionValue` / `IndexVersionValue` / `DeleteVersionValue` | `index/engine/VersionValue.java` | 版本三元组 |
| `LocalCheckpointTracker` | `index/engine/LocalCheckpointTracker.java` | seqno 连续追踪 |
| `CombinedDeletionPolicy` | `index/engine/CombinedDeletionPolicy.java` | commit 保留策略 |
| `EngineConfig` | `index/engine/EngineConfig.java` | 引擎配置 |
| `VersionsAndSeqNoResolver` | `index/engine/VersionsAndSeqNoResolver.java` | 从 reader 解 version/seqno |
| `EngineMergeScheduler` | `index/engine/EngineMergeScheduler.java` | 合并调度（第 16 章） |
| `IndexShard` | `index/shard/IndexShard.java` | ★ 引擎持有者（`:715`/`:722`/`:729`、`refresh:918`、`acquireSearcher:1215`、`afterWriteOperation:2625`） |
| `Translog` | `index/translog/Translog.java` | 第 14 章 |
| `SequenceNumbers` / `SeqNoStats` | `index/seqno/` | seqno 常量/统计 |

---

**上一篇**：[14-Translog事务日志.md](./14-Translog事务日志.md)
**下一篇**：[16-RefreshFlush与Segment合并.md](./16-RefreshFlush与Segment合并.md)
