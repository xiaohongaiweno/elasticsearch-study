# 16 · Refresh、Flush 与 Segment 合并

> 目标：彻底分清 **Refresh / Flush / Merge** 三件事——它们经常被混为一谈，但对应
> 「可见性」「持久化」「空间回收」三个完全不同的目标，各自的触发时机、线程池、代价都不同。
>
> **为什么必须学**：
> - 「为什么写入后搜不到」→ Refresh。
> - 「为什么磁盘占用是数据的好几倍」→ Merge 落后 / delete 未回收。
> - 「为什么写入会抖动、被限流（`is_throttled`）」→ Merge 跟不上，`EngineMergeScheduler` 主动降速。
> - 「为什么 translog 会突然暴涨又突然消失」→ Flush。
> - 云服务里该调的参数（`refresh_interval`、`flush_threshold_size`、merge 线程数）
>   全部集中在本章。
>
> **前置**：第 14 章（Translog）、第 15 章（Engine）。
> **后续**：第 17 章 `17-副本复制与SeqNo一致性模型.md`。

---

## 1. 三件事，一张表说清

| 维度 | Refresh | Flush | Merge |
| --- | --- | --- | --- |
| **目的** | 让新写入**可见**（可搜索） | 让数据**持久化**（fsync 到段） | **合并段**，回收删除/降低段数 |
| **做什么** | Lucene buffer → 新 segment + 打开新 reader | `translog.rollGeneration` + Lucene `commit` + `trim` | 把多个小 segment 合成大 segment（物理删除 dedup） |
| **副作用** | 产生小 segment（后续要 merge） | 昂贵：fsync + 写 `segments_N` | 占用大量磁盘 IO/CPU |
| **触发** | 写请求 `refresh`、定时任务、`_refresh` API | translog 超阈值、`_flush` API、merge 后、关索引 | Lucene MergePolicy 自动、`_forcemerge` |
| **频率** | 高（默认每 1s） | 低（默认 512MB translog 才触发） | 中 |
| **线程池** | `REFRESH` | `FLUSH` | merge 专用线程（不走 ES 线程池） |

**一眼记忆**：
- **Refresh = 可见性**（近实时，默认 1s）
- **Flush = 持久性**（把 translog 里没落段的数据写进段，然后才能删 translog）
- **Merge = 空间与性能**（合并小段、回收 delete）

```
写入 ──→ Lucene buffer ──(refresh)──→ 小 segment ──(merge)──→ 大 segment
   └──→ Translog ──────────(flush)──────────────────────────→ 可删除(trim)
```

---

## 2. 必读源码清单

| 类 | 路径 | 作用 |
| --- | --- | --- |
| `IndexService` | `index/IndexService.java` | ★ 定时任务（`AsyncRefreshTask:829`、`AsyncTranslogFSync:807`） |
| `IndexShard` | `index/shard/IndexShard.java` | `refresh:918`、`isRefreshNeeded:2733`、`addRefreshListener:2744`、`afterWriteOperation:2625` |
| `InternalEngine` | `index/engine/InternalEngine.java` | `refresh:1596`、`flush:1743`、`shouldPeriodicallyFlush:1713`、`forceMerge:1929`、`EngineMergeScheduler:2300` |
| `Engine` | `index/engine/Engine.java` | `refreshNeeded:1006`、`flush:1055`、`IndexThrottle:271` |
| `ElasticsearchConcurrentMergeScheduler` | `index/engine/ElasticsearchConcurrentMergeScheduler.java` | 合并调度 + 统计（`:51`） |
| `MergeSchedulerConfig` | `index/MergeSchedulerConfig.java` | 合并线程/并发设置（`:55-65`） |
| `MergePolicyConfig` | `index/MergePolicyConfig.java` | 合并策略设置（`:119-197`） |
| `EsTieredMergePolicy` | `index/EsTieredMergePolicy.java` | 分层合并策略（`:35`） |
| `ElasticsearchMergePolicy` | `index/shard/ElasticsearchMergePolicy.java` | 段升级 + 包装（`:47`） |
| `RefreshListeners` | `index/shard/RefreshListeners.java` | `wait_for` 刷新监听（`:45`） |
| `OnGoingMerge` / `MergeStats` | `index/merge/` | 合并统计 |
| `Translog` | `index/translog/Translog.java` | roll/trim（第 14 章） |

---

## 3. ★ Refresh（刷新 / 近实时）

### 3.1 本质

Lucene 的 `IndexWriter` 把新文档放在**内存 buffer** 里，此时不可搜索。
`refresh` 调用 `IndexWriter` 把 buffer 里的内容生成一个新的 **segment**，
再让 `SearcherManager` 打开一个**新的 `DirectoryReader`**，从此新数据可搜索。

**ES 就是靠这一点实现「近实时（NRT）」的** —— 不是实时，而是「默认 1 秒后可见」。

### 3.2 `InternalEngine.refresh`（`:1596`）

```java
@Override
public void refresh(String source) throws EngineException {
    refresh(source, SearcherScope.EXTERNAL);        // 用户可见的 refresh → external
}

final void refresh(String source, SearcherScope scope) throws EngineException {
    final long localCheckpointBeforeRefresh = getLocalCheckpoint();
    try (ReleasableLock lock = readLock.acquire()) {          // ① 读锁：不与 flush 并发
        ensureOpen();
        if (store.tryIncRef()) {
            try {
                ReferenceManager<IndexSearcher> referenceManager = getReferenceManager(scope);
                referenceManager.maybeRefreshBlocking();      // ② ★ Lucene：有变化才 reopen，否则立刻返回
            } finally { store.decRef(); }
            lastRefreshedCheckpointListener.updateRefreshedCheckpoint(localCheckpointBeforeRefresh);  // ③ 记录已刷 checkpoint
        }
    } catch (...) { ... }
    maybePruneDeletes();          // ④ 顺手清理过期 tombstone
    mergeScheduler.refreshConfig();   // ⑤ 应用动态的 merge 设置
}
```

**★★ `maybeRefreshBlocking()` 的关键语义**：
- 如果**没有未提交的变化** → 立刻返回，**不产生新段**。
- 所以「高频 refresh 但无写入」几乎没有成本（`isRefreshNeeded()` 会为 false）。
- 这就是为什么「每秒 refresh 一次」不会把集群打爆。

**★ `lastRefreshedCheckpointListener`**：记录「最近一次 refresh 覆盖到的 local checkpoint」。
这是第 17 章 `global checkpoint` 推进的**上限**：global checkpoint 不能超过已 refresh 的 local checkpoint，
否则副本恢复时拿到「还没可见」的数据会产生不一致。

### 3.3 Refresh 的 4 个触发来源

```
① 写请求的 RefreshPolicy（第 13 章 TransportWriteAction.AsyncAfterWriteAction:274）
     IMMEDIATE  → indexShard.refresh("refresh_flag_index")       立即强制 refresh
     WAIT_UNTIL → indexShard.addRefreshListener(location, ...)   等下一次自然 refresh
     NONE       → 什么都不做（默认）

② 定时任务：IndexService.AsyncRefreshTask（IndexService:829）
     线程池 ThreadPool.Names.REFRESH，周期 = index.refresh_interval（默认 1s）
     → IndexService.maybeRefreshEngine()（:711）
         if (indexSettings.getRefreshInterval().millis() > 0)     ← -1 时不刷新
             for each shard: if (shard.isRefreshNeeded()) shard.refresh("schedule")

③ 手动 API：POST /idx/_refresh → TransportRefreshAction → IndexShard.refresh

④ Lucene 内部：IndexWriter 的 RAM buffer 满了（indices.memory.index_buffer_size 分摊）会自动 flush 到段，
   这一步会带来可搜索的段（但不会更新 ES 的 external reader，仍需 ② / ① 才对外可见）
```

**`IndexShard.isRefreshNeeded()`（`:2733`）**：

```java
public boolean isRefreshNeeded() {
    return getEngine().refreshNeeded()                       // external reader 不是最新的
        || (refreshListeners != null && refreshListeners.refreshNeeded());   // 有 wait_for 监听者
}
```

**`Engine.refreshNeeded()`（`Engine.java:1006`）**：

```java
public boolean refreshNeeded() {
    if (store.tryIncRef()) {
        try {
            try (Searcher searcher = acquireSearcher("refresh_needed", SearcherScope.EXTERNAL)) {
                return searcher.getDirectoryReader().isCurrent() == false;   // ★ 只有 reader 过期才算需要
            }
        } finally { store.decRef(); }
    }
    return false;
}
```

**★ 这是「refresh 是否必要」的精确判定**：直接问 Lucene「我手上的 external reader 还是最新的吗」。

### 3.4 `RefreshListeners`：`refresh=wait_for` 的实现（`:45`）

```java
public final class RefreshListeners implements ReferenceManager.RefreshListener, Closeable {  // :45
    private final Runnable forceRefresh;                                             // :47
    private final IntSupplier getMaxRefreshListeners;                                // 最大监听者数
    ...
    /** 返回 true 表示「因为监听者太多而被迫立刻 refresh」 */
    public boolean addOrNotify(Translog.Location location, Consumer<Boolean> listener) { ... }   // :87

    public boolean refreshNeeded() {   // :139  有等待者需要 refresh
        ...
    }

    @Override public void beforeRefresh()  { ... }   // :169
    @Override public void afterRefresh(boolean didRefresh) { ... }   // :174
}
```

**★ `wait_for` 的机制**：
1. 写入完成后，`AsyncAfterWriteAction` 调 `indexShard.addRefreshListener(location, listener)`。
2. `RefreshListeners` 记录「(location, 回调)」，**不阻塞线程**，等下一次 refresh。
3. 如果等待者数量超过 `index.max_refresh_listeners`（默认 1000），
   `addOrNotify` 会**强制触发一次 refresh**（给 write 请求方返回 true，表示被强制刷新）。
4. `beforeRefresh` 快照当前「已刷位置」，`afterRefresh` 对「位置 <= 已刷位置」的回调全部触发。

**为什么 `wait_for` 比 `true` 好？**
- `true`（IMMEDIATE）**每个请求**都强制 refresh → 产生海量微小段 → merge 压力巨大。
- `wait_for` 复用**同一次自然 refresh**，大量并发写可以共享一次 refresh → 成本摊薄。

> ⚠️ **`wait_for` 的坑**：如果索引**没有写入**（`refresh_interval=-1` 且无其他写入），
> `wait_for` 请求会一直等到 `?timeout=`（默认无超时 → 等到客户端超时）。
> 这是「`refresh=wait_for` 卡住」问题的根因。

### 3.5 `refresh_interval`（`IndexSettings.java`）

| 值 | 含义 |
| --- | --- |
| `1s`（默认） | 每秒检查一次，有变化才 refresh |
| `-1` | **禁止定时 refresh**（只能靠 `_refresh` / `refresh=true`） |
| `30s` / `60s` | 大批量导入时常用，减少段数量 |

> **★ 写入性能第一优化原则**：批量导入/日志场景把 `refresh_interval` 调大（如 `30s`）
> 或设为 `-1`（导入完再 `_refresh`），可以**减少 segment 数量、降低 merge 压力**，
> 吞吐可提升数倍。

---

## 4. ★ Flush（提交）

### 4.1 本质

`flush` 做的事（`InternalEngine.flush:1743`）：

```
① translog.rollGeneration()      ← 把当前代定格（第 14 章）
② commitIndexWriter(...)         ← Lucene commit：写 segments_N，并把 translog_generation 等写进 userData
③ refresh("version_table_flush", INTERNAL)   ← 清理版本表
④ translog.trimUnreferencedReaders()          ← 删掉 commit 之前的所有 translog 代
```

**★★ 为什么必须先 roll 再 commit**（第 14 章详细讲）：
Lucene commit 记录的是「translog 的**代**」；不 roll 的话 commit 指向的是「正在写的代」，
那一代永远删不掉（里面还有新数据）。roll 之后，commit 指向「已经封口的代」，
flush 完就能安全 trim。

### 4.2 `commitIndexWriter` 的 commit userData（`:2387`）

```java
protected void commitIndexWriter(final IndexWriter writer, final Translog translog, @Nullable final String syncId) throws IOException {
    ensureCanFlush();
    final long localCheckpoint = localCheckpointTracker.getCheckpoint();
    final Translog.TranslogGeneration translogGeneration = translog.getMinGenerationForSeqNo(localCheckpoint + 1);
    final String translogFileGeneration = Long.toString(translogGeneration.translogFileGeneration);
    final String translogUUID = translogGeneration.translogUUID;
    final String localCheckpointValue = Long.toString(localCheckpoint);

    writer.setLiveCommitData(() -> {
        final Map<String, String> commitData = new HashMap<>(6);
        commitData.put(Translog.TRANSLOG_GENERATION_KEY, translogFileGeneration);   // ★ 恢复起点
        commitData.put(Translog.TRANSLOG_UUID_KEY, translogUUID);                    // ★ 身份校验
        commitData.put(SequenceNumbers.LOCAL_CHECKPOINT_KEY, localCheckpointValue);  // ★ local checkpoint
        if (syncId != null) { commitData.put(Engine.SYNC_COMMIT_ID, syncId); }
        commitData.put(SequenceNumbers.MAX_SEQ_NO, Long.toString(localCheckpointTracker.getMaxSeqNo()));  // ★ max seqno
        commitData.put(MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID, Long.toString(maxUnsafeAutoIdTimestamp.get()));
        commitData.put(HISTORY_UUID_KEY, historyUUID);                              // ★ 分片历史
        if (softDeleteEnabled) { commitData.put(Engine.MIN_RETAINED_SEQNO, Long.toString(softDeletesPolicy.getMinRetainedSeqNo())); }
        return commitData.entrySet().iterator();
    });
    writer.commit();
}
```

**★★ 这一份 `commitData` 就是「持久化状态的完整描述」**，重启恢复时全靠它：

| key | 用途 |
| --- | --- |
| `translog_generation` | 从哪一代 translog 开始回放 |
| `translog_uuid` | 与 tlog header 校验（防文件错配，第 14 章） |
| `local_checkpoint` | 已连续完成的 seqno |
| `max_seq_no` | 已见过的最大 seqno（**必须 >= 已写入的 seqno**，否则会重用 seqno） |
| `max_unsafe_auto_id_timestamp` | append-only 优化的水位（第 15 章） |
| `history_uuid` | 分片历史（第 17 章副本校验） |
| `min_retained_seqno` | soft-delete 保留起点 |

> **★ 注释里的警告**：`max_seq_no` 必须在 `commit()` **之后**才能计算，
> 因为只有 commit 才知道哪些文档最终进了段。所以用 `setLiveCommitData` 的**迭代器延迟求值**。
> 如果 max_seq_no < 实际写入的 seqno，恢复后会**重用 seqno 导致文档重复**。

### 4.3 Flush 的触发来源

| 触发 | 代码 | 说明 |
| --- | --- | --- |
| translog 超阈值 | `IndexShard.afterWriteOperation:2625` → `shouldPeriodicallyFlush()`（`InternalEngine:1713`） | 默认 512MB（`index.translog.flush_threshold_size`） |
| `_flush` API | `TransportFlushAction` → `IndexShard.flush` | 手动同步 |
| merge 完成后 | `InternalEngine.EngineMergeScheduler.afterMerge:2319` | 见 §5.3 |
| 索引关闭前 | `IndexShard.close` | 保证数据落盘 |
| 副本 recovery 结束 | 第 18 章 | 恢复完成后 flush |
| `_forcemerge` 带 `flush=true` | `InternalEngine.forceMerge:1963` | 合并后提交 |

**`IndexShard.afterWriteOperation`（`:2625`）回顾**（第 14 章）：

```java
public void afterWriteOperation() {
    if (shouldPeriodicallyFlush() || shouldRollTranslogGeneration()) {
        if (flushOrRollRunning.compareAndSet(false, true)) {    // ★ 保证只有一个在跑
            if (shouldPeriodicallyFlush()) {
                flush(new FlushRequest());                      // 异步提交到 FLUSH 线程池
            } else if (shouldRollTranslogGeneration()) {
                rollTranslogGeneration();
            } else {
                flushOrRollRunning.compareAndSet(true, false);
            }
        }
    }
}
```

**★ 注意「写请求触发 flush/roll，但 flush/roll 在后台 `FLUSH` 线程池异步执行」**：
所以一次写请求的延迟里**不包含** flush 的耗时（除非 `?force`）。

### 4.4 `flush(force, waitIfOngoing)`（`:1743`）关键分支

```java
public CommitId flush(boolean force, boolean waitIfOngoing) throws EngineException {
    ...
    if (indexWriter.hasUncommittedChanges() || force || shouldPeriodicallyFlush()) {
        ensureCanFlush();                        // ★ pendingTranslogRecovery 期间不允许 flush
        try {
            translog.rollGeneration();
            commitIndexWriter(indexWriter, translog, null);
            refresh("version_table_flush", SearcherScope.INTERNAL);
            translog.trimUnreferencedReaders();
        } catch (AlreadyClosedException e) { throw e; }
        catch (Exception e) { throw new FlushFailedEngineException(shardId, e); }
        refreshLastCommittedSegmentInfos();
    }
    newCommitId = lastCommittedSegmentInfos.getId();
    ...
}
```

**`ensureCanFlush()`**：如果引擎还在 `pendingTranslogRecovery`（恢复未完成）就抛 `IllegalStateException`。
**这就是「恢复期间不能 flush」的保护**——恢复阶段 flush 会把未回放完的状态提交，丢数据。

---

## 5. ★★ Merge（段合并）

### 5.1 为什么必须合并

每次 refresh 产生一个小段，段太多会导致：
1. **搜索变慢**：每个段都要建 `Collector`、合并结果、消耗 file handle。
2. **删除不回收**：Lucene 的 delete 是逻辑删除，只有 merge 才会真正物理丢弃。
3. **版本信息膨胀**：老版本 doc 占空间。

Merge 把多个小段合成大段：**物理丢弃已删文档**、减少段数。

### 5.2 `EsTieredMergePolicy` + `MergePolicyConfig`（`MergePolicyConfig.java:119`）

ES 用 **TieredMergePolicy**（分层合并）：段按大小分层，同层内段数超过 `segments_per_tier` 就合并。

**默认参数（`MergePolicyConfig.java:124-131`）**：

| 参数 | 默认值 | 常量 | 含义 |
| --- | --- | --- | --- |
| `index.merge.policy.expunge_deletes_allowed` | `10.0`（%） | `DEFAULT_EXPUNGE_DELETES_ALLOWED` | 删除比例超过它，`_forcemerge?only_expunge_deletes` 才合并该段 |
| `index.merge.policy.floor_segment` | `2mb` | `DEFAULT_FLOOR_SEGMENT` | 小于它视为同层（避免频繁合并小段） |
| `index.merge.policy.max_merge_at_once` | `10` | `DEFAULT_MAX_MERGE_AT_ONCE` | 一次最多合并多少段 |
| `index.merge.policy.max_merge_at_once_explicit` | `30` | `DEFAULT_MAX_MERGE_AT_ONCE_EXPLICIT` | `_forcemerge` 时一次最多合并多少段 |
| `index.merge.policy.max_merged_segment` | `5gb` | `DEFAULT_MAX_MERGED_SEGMENT` | 单段最大（超过不再合并） |
| `index.merge.policy.segments_per_tier` | `10.0` | `DEFAULT_SEGMENTS_PER_TIER` | 每层最多段数 |
| `index.merge.policy.reclaim_deletes_weight` | `2.0` | — | （已废弃）删除回收权重 |
| `index.merge.policy.deletes_pct_allowed` | `33.0`（%） | `DEFAULT_DELETES_PCT_ALLOWED` | 删除比例超过它优先合并（**6.6 新增的重要设置**） |
| `index.compound_format` | Lucene 默认 | — | 是否用 compound file |

**★ 从 `MergePolicyConfig:181` 起**，构造时把这些值塞进 `EsTieredMergePolicy`：

```java
maxMergeAtOnce = adjustMaxMergeAtOnceIfNeeded(maxMergeAtOnce, segmentsPerTier);
mergePolicy.setNoCFSRatio(indexSettings.getValue(INDEX_COMPOUND_FORMAT_SETTING));
mergePolicy.setForceMergeDeletesPctAllowed(forceMergeDeletesPctAllowed);
mergePolicy.setFloorSegmentMB(floorSegment.getMbFrac());
mergePolicy.setMaxMergeAtOnce(maxMergeAtOnce);
mergePolicy.setMaxMergeAtOnceExplicit(maxMergeAtOnceExplicit);
mergePolicy.setMaxMergedSegmentMB(maxMergedSegment.getMbFrac());
mergePolicy.setSegmentsPerTier(segmentsPerTier);
mergePolicy.setDeletesPctAllowed(deletesPctAllowed);
```

**★ `EsTieredMergePolicy`（`EsTieredMergePolicy.java:35`）继承 `FilterMergePolicy`**，
只额外覆写 `size(segment, infos)`？其实它的主要作用是「禁止合并来自不同 shard 的段」等 ES 特有规则。
`ElasticsearchMergePolicy`（`index/shard/ElasticsearchMergePolicy.java:47`）再包一层，
负责**段升级**（`setUpgradeInProgress`，把旧版本的段在新版本上重写）。

### 5.3 `EngineMergeScheduler`（`InternalEngine:2300`）：★ 写限流的源头

```java
private final class EngineMergeScheduler extends ElasticsearchConcurrentMergeScheduler {   // :2300
    private final AtomicInteger numMergesInFlight = new AtomicInteger(0);
    private final AtomicBoolean isThrottling = new AtomicBoolean();

    @Override
    public synchronized void beforeMerge(OnGoingMerge merge) {            // :2308
        int maxNumMerges = mergeScheduler.getMaxMergeCount();
        if (numMergesInFlight.incrementAndGet() > maxNumMerges) {
            if (isThrottling.getAndSet(true) == false) {
                logger.info("now throttling indexing: numMergesInFlight={}, maxNumMerges={}", numMergesInFlight, maxNumMerges);
                activateThrottling();               // ★ 开始限流：把写入降为单线程
            }
        }
    }

    @Override
    public synchronized void afterMerge(OnGoingMerge merge) {             // :2319
        int maxNumMerges = mergeScheduler.getMaxMergeCount();
        if (numMergesInFlight.decrementAndGet() < maxNumMerges) {
            if (isThrottling.getAndSet(false)) {
                logger.info("stop throttling indexing: ...");
                deactivateThrottling();             // ★ 解除限流
            }
        }
        if (indexWriter.hasPendingMerges() == false &&
                System.nanoTime() - lastWriteNanos >= engineConfig.getFlushMergesAfter().nanos()) {
            // ★ 所有 merge 完成 + 一段时间没写入 → 异步 flush（回收刚合并段占用的临时磁盘）
            engineConfig.getThreadPool().executor(ThreadPool.Names.FLUSH).execute(new AbstractRunnable() {
                protected void doRun() throws Exception {
                    if (tryRenewSyncCommit() == false) { flush(); }
                }
            });
        }
    }

    @Override
    protected void handleMergeException(Directory dir, Throwable exc) {   // :2358
        // ★ 在另一个线程把 merge 异常升级为 failEngine（避免被 stack 上的 catch 吃掉）
        engineConfig.getThreadPool().generic().execute(new AbstractRunnable() {
            protected void doRun() { failEngine("merge failed", new MergePolicy.MergeException(exc, dir)); }
        });
    }
}
```

**★★ 这是「`is_throttled` 写入抖动」的完整解释**：

```
写入很快 → refresh 产生很多小段 → merge 排队
   → 同时进行的 merge 数 > index.merge.scheduler.max_merge_count
   → beforeMerge 触发 activateThrottling()
   → IndexThrottle 从 NoOpLock 切换成真锁（Engine.IndexThrottle:283）
   → 所有写入序列化（单线程）
   → merge 追上来后 afterMerge 触发 deactivateThrottling()
```

**`IndexThrottle`（`Engine.java:271-318`）的实现很巧妙**：

```java
protected static final class IndexThrottle {
    private static final ReleasableLock NOOP_LOCK = new ReleasableLock(new NoOpLock());   // 永远可获取
    private final ReleasableLock lockReference = new ReleasableLock(new ReentrantLock());
    private volatile ReleasableLock lock = NOOP_LOCK;

    public Releasable acquireThrottle() { return lock.acquire(); }
    public void activate()   { startOfThrottleNS = System.nanoTime(); lock = lockReference; }   // 换成真锁
    public void deactivate() { lock = NOOP_LOCK; /* 累计限流时长 */ }
    boolean isThrottled() { return lock != NOOP_LOCK; }
}
```

- **不限流时**：`acquireThrottle()` 拿的是 `NoOpLock`（几乎零成本）。
- **限流时**：切成 `ReentrantLock`，写入串行化。
- 这也解释了为什么 `indexing.is_throttled=true` 时**写入吞吐会骤降**。

### 5.4 `ElasticsearchConcurrentMergeScheduler`（`:51`）：统计与配置

```java
class ElasticsearchConcurrentMergeScheduler extends ConcurrentMergeScheduler {   // :51
    private final MeanMetric totalMerges = new MeanMetric();
    private final CounterMetric totalMergesNumDocs = new CounterMetric();
    private final CounterMetric totalMergesSizeInBytes = new CounterMetric();
    private final CounterMetric currentMerges = new CounterMetric();
    private final CounterMetric totalMergeStoppedTime = new CounterMetric();     // 被 STOPPED 暂停
    private final CounterMetric totalMergeThrottledTime = new CounterMetric();   // 被限流（IO 节流）时间
    private final Set<OnGoingMerge> onGoingMerges = ...;

    @Override protected void doMerge(IndexWriter writer, MergePolicy.OneMerge merge) {   // :83
        int totalNumDocs = merge.totalNumDocs();
        long timeNS = System.nanoTime();
        currentMerges.inc(); ...
        try { beforeMerge(onGoingMerge); super.doMerge(writer, merge); }   // ← 真正的合并在这里
        finally { /* 累计统计、记录耗时 > 20s 的 DEBUG 日志 */ }
    }

    @Override protected boolean maybeStall(IndexWriter writer) {    // :165
        // ★ 不做 Lucene 自带的 stall，因为 ES 用 IndexThrottle 自己做限流
        return true;
    }

    void refreshConfig() {    // :188 动态应用 max_merge_count / max_thread_count / auto_throttle
        if (getMaxMergeCount() != config.getMaxMergeCount() || getMaxThreadCount() != config.getMaxThreadCount()) {
            setMaxMergesAndThreads(config.getMaxMergeCount(), config.getMaxThreadCount());
        }
        ...enableAutoIOThrottle()/disableAutoIOThrottle()...
    }
}
```

| 监控指标 | 来源 | 含义 |
| --- | --- | --- |
| `merges.current` / `current_docs` / `current_size_in_bytes` | `currentMerges*` | 当前进行中的合并 |
| `merges.total` / `total_time_in_millis` | `totalMerges*` | 累计合并 |
| `merges.total_stopped_time_in_millis` | `totalMergeStoppedTime` | 被暂停（如 forcemerge 优先级） |
| `merges.total_throttled_time_in_millis` | `totalMergeThrottledTime` | **被 IO 节流的时间（关键！）** |
| `merges.total_auto_throttle_in_bytes` | `config.isAutoThrottle() ? getIORateLimitMBPerSec() : ∞` | 自动节流速率 |

### 5.5 `MergeSchedulerConfig`（`MergeSchedulerConfig.java:53`）

| 设置 | 默认 | 含义 |
| --- | --- | --- |
| `index.merge.scheduler.max_thread_count`（`:55`） | `max(1, min(4, 处理器数/2))` | 合并线程数。**机械盘建议设 1** |
| `index.merge.scheduler.max_merge_count`（`:60`） | `max_thread_count + 5` | 最多同时进行多少合并（超过就限流） |
| `index.merge.scheduler.auto_throttle`（`:64`） | `true` | 是否自动节流 merge 的 IO |

**★ 为什么机械盘要把 `max_thread_count` 设为 1？**
机械盘随机 IO 差，多个 merge 线程同时读写会**严重抖动**（seek 打架）。
SSD 上默认 4 是合理的。

**★ `max_merge_count = max_thread_count + 5` 的含义**：
允许最多 5 个 merge「排在后面等」，超过就**开始限流写入**（§5.3），
防止 merge 无限堆积把内存/磁盘打爆。

### 5.6 `forceMerge`（`InternalEngine:1929`）

```java
public void forceMerge(final boolean flush, int maxNumSegments, boolean onlyExpungeDeletes,
                       final boolean upgrade, final boolean upgradeOnlyAncientSegments) throws EngineException, IOException {
    ElasticsearchMergePolicy mp = (ElasticsearchMergePolicy) indexWriter.getConfig().getMergePolicy();
    optimizeLock.lock();                     // ★ 与其它 forceMerge 串行（一次只允许一个）
    try {
        ensureOpen();
        if (upgrade) { mp.setUpgradeInProgress(true, upgradeOnlyAncientSegments); }   // 段升级
        store.incRef();
        try {
            if (onlyExpungeDeletes) {
                indexWriter.forceMergeDeletes(true);          // 只清理删除（不减少段数）
            } else if (maxNumSegments <= 0) {
                indexWriter.maybeMerge();                     // 触发一次正常合并判断
            } else {
                indexWriter.forceMerge(maxNumSegments, true); // ★ 合并到 maxNumSegments 个段（阻塞）
            }
            if (flush) { if (tryRenewSyncCommit() == false) { flush(false, true); } }
        } finally { store.decRef(); }
    } finally { try { mp.setUpgradeInProgress(false, false); } finally { optimizeLock.unlock(); } }
}
```

**★ `_forcemerge` 的危险性**：
- **阻塞式**（`true` 参数）：会一直等到合并完成，可能几小时。
- **产生巨大段**：`max_num_segments=1` 会产生超大段，之后**很难再合并**（超过 `max_merged_segment` 5GB 后不再合并）。
- **网络/磁盘风暴**：一次合并会重写几乎所有数据。
- **官方建议**：生产中**只读索引**才 forcemerge，且用 `max_num_segments` 适度（不要盲目设 1）。

**`tryRenewSyncCommit()`**：如果上次是 synced flush，可以「续期」而不做完整 commit（便宜）。
见第 18 章 `_flush/synced`。

---

## 6. ★ 三者协同：一次写入的完整生命周期

```
t0  写请求到达 → InternalEngine.index() 写 Lucene buffer + Translog
t0  AsyncAfterWriteAction.run():
      ├─ refresh=IMMEDIATE → indexShard.refresh("refresh_flag_index")      ← Refresh
      ├─ refresh=WAIT_UNTIL → addRefreshListener(location, ...)             ← RefreshListener
      └─ durability=REQUEST → indexShard.sync(location)                     ← Translog fsync
t1  定时任务 AsyncRefreshTask（每 1s）→ maybeRefreshEngine → shard.refresh("schedule")  ← Refresh
      → Lucene 生成新 segment；LiveVersionMap 换 map（第 15 章）
t2  merge 排队：小段太多 → TieredMergePolicy 选中若干段 → EngineMergeScheduler.doMerge   ← Merge
      → 合并期间若并发 merge 超 max_merge_count → activateThrottling()（写入变慢）
t3  merge 完成后一段时间无写入 → 异步 flush()                             ← Flush
t4  translog 超 flush_threshold_size(512MB) → afterWriteOperation → flush()  ← Flush
      → rollGeneration + Lucene commit + refresh(INTERNAL) + trimUnreferencedReaders
```

---

## 7. 配置速查

| 设置 | 默认 | 动态 | 作用 |
| --- | --- | --- | --- |
| `index.refresh_interval` | `1s` | ✅ | 刷新间隔；`-1` 关闭 |
| `index.max_refresh_listeners` | `1000` | ❌ | `wait_for` 最大等待者数（超过强制刷新） |
| `indices.memory.index_buffer_size` | `10%` | ❌ | Lucene 索引缓冲（节点级） |
| `index.translog.flush_threshold_size` | `512mb` | ✅ | 触发 flush 的 translog 阈值 |
| `index.translog.durability` | `request` | ✅ | `request`/`async`（决定 fsync 频率） |
| `index.translog.sync_interval` | `5s` | ❌ | `async` 下 fsync 间隔 |
| `index.merge.scheduler.max_thread_count` | `min(4, proc/2)` | ✅ | 合并线程数（机械盘设 1） |
| `index.merge.scheduler.max_merge_count` | `thread+5` | ✅ | 最大并发合并数（超过限流） |
| `index.merge.scheduler.auto_throttle` | `true` | ✅ | merge IO 自动节流 |
| `index.merge.policy.segments_per_tier` | `10.0` | ✅ | 每层段数上限 |
| `index.merge.policy.max_merge_at_once` | `10` | ✅ | 单次合并段数 |
| `index.merge.policy.max_merged_segment` | `5gb` | ✅ | 单段最大 |
| `index.merge.policy.floor_segment` | `2mb` | ✅ | 小段下限 |
| `index.merge.policy.deletes_pct_allowed` | `33%` | ✅ | 删除比例阈值 |
| `index.merge.policy.expunge_deletes_allowed` | `10%` | ✅ | expunge 阈值 |
| `index.compound_format` | Lucene 默认 | ✅ | 是否 compound file |

**关键监控指标**：

| 指标路径 | 含义 |
| --- | --- |
| `indices.refresh.total` / `refresh_time_in_millis` | refresh 次数/耗时 |
| `indices.flush.total` / `periodic` / `flush_time_in_millis` | flush 次数（periodic 是自动触发的） |
| `indices.merges.current` / `total` / `total_time_in_millis` | 合并量 |
| `indices.merges.total_throttled_time_in_millis` | ★ 合并被 IO 节流的时间 |
| `indices.indexing.is_throttled` / `throttle_time_in_millis` | ★ 写入被限流的状态/时长 |
| `indices.segments.count` / `memory_in_bytes` | 段数/段内存 |
| `indices.docs.deleted` | 逻辑删除数（merge 才能回收） |
| `indices.store.size_in_bytes` | 索引占用（含未回收删除） |
| `indices.translog.uncommitted_size_in_bytes` | 未 flush 的 translog |

**`GET _cat/segments/<index>?v`**：查看每段的段数、大小、删除数。

---

## 8. 故障排查对照表

| 现象 | 根因 | 定位 / 解决 |
| --- | --- | --- |
| 写入后搜不到 | 未 refresh（`refresh_interval=-1` 或 interval 未到） | 用 `GET` / `?refresh=wait_for` / 调小 interval |
| `refresh=wait_for` 请求卡住 | 索引无其它写入 → 自然 refresh 永不发生 | 配 `refresh_interval`。这是已知语义，不是 bug |
| 磁盘占用远大于索引大小 | delete 未回收（`docs.deleted` 高） | `_forcemerge?only_expunge_deletes=true`（低峰做） |
| 段数持续增长（几百上千） | merge 落后 / `max_merged_segment` 太小 | 看 `merges.total_throttled_time`；调大 merge 线程或 `max_merged_segment` |
| 写入周期性抖动、`is_throttled=true` | 合并跟不上，`EngineMergeScheduler` 主动限流 | 提高磁盘 IOPS、机械盘设 `max_thread_count=1`、降低写入速率 |
| 写入 P99 高但平均正常 | 频繁 refresh（`refresh=true`）产生小段 → merge 抖动 | 改用 `refresh=wait_for` 或调大 interval |
| 合并线程占满 CPU | merge 线程数过多 / merge 任务重 | 调小 `max_thread_count`；错峰导入 |
| `flush` 耗时很长 | 大段 commit（fsync 慢） | 提升磁盘；减少单次 flush 的数据量 |
| translog 突然暴涨又消失 | 周期性 flush（正常现象） | 关注 `uncommitted_size_in_bytes` 峰值 |
| 索引大小持续缓慢增长不下降 | merge 策略阻止合并（超过 `max_merged_segment`） | 属正常；`_forcemerge` 到较小段数或接受 |
| `_forcemerge` 卡住几小时 | 阻塞式 + 数据量大 | 只在只读索引做；用 `wait_for_completion=false` |
| merge 失败导致 shard 挂 | 磁盘损坏/IO 错误 | `merge failed` 日志 + `failEngine`；检查磁盘 |
| 节点磁盘打满（flood stage） | merge/translog/flush 竞态 + 大段复制 | 见第 11 章 disk 水位；加盘或调水位 |

---

## 9. 动手实验

**16-1 观察 refresh 与可见性**
```bash
curl -XPUT "localhost:9200/rf/_settings" -H 'Content-Type: application/json' -d '{"index":{"refresh_interval":"-1"}}'
curl -XPUT "localhost:9200/rf/_doc/1" -H 'Content-Type: application/json' -d '{"a":1}'
curl -XGET "localhost:9200/rf/_search"            # 搜不到
curl -XGET "localhost:9200/rf/_doc/1"             # GET 能读到（realtime）
curl -XPOST "localhost:9200/rf/_refresh"
curl -XGET "localhost:9200/rf/_search"            # 能搜到了
```

**16-2 观察 refresh 产生的段**
```bash
curl -XPUT "localhost:9200/sg" -H 'Content-Type: application/json' -d '{"settings":{"refresh_interval":"-1"}}'
for i in $(seq 1 5); do curl -s -XPOST "localhost:9200/sg/_doc" -H 'Content-Type: application/json' -d '{"v":1}' >/dev/null; curl -s -XPOST "localhost:9200/sg/_refresh" >/dev/null; done
curl -XGET "localhost:9200/_cat/segments/sg?v"
# 每次 refresh 产生一个段 → 5 个段
```

**16-3 观察 merge**
```bash
curl -XPOST "localhost:9200/sg/_forcemerge?max_num_segments=1&pretty"
curl -XGET "localhost:9200/_cat/segments/sg?v"     # 段数变少
curl -XGET "localhost:9200/sg/_stats/merge?pretty" # 看 merge 统计
```

**16-4 观察 flush 与 translog trim**
```bash
curl -XGET "localhost:9200/rf/_stats?level=shards&filter_path=**.translog&pretty"
curl -XPOST "localhost:9200/rf/_flush?pretty"
curl -XGET "localhost:9200/rf/_stats?level=shards&filter_path=**.translog&pretty"
```

**16-5 delete 不回收 vs forcemerge 回收**
```bash
curl -XPUT "localhost:9200/dl/_doc/1?refresh=true" -H 'Content-Type: application/json' -d '{"a":1}'
for i in $(seq 1 100); do curl -s -XPOST "localhost:9200/dl/_update/1" -H 'Content-Type: application/json' -d '{"doc":{"a":'"$i"'}}' >/dev/null; done
curl -XPOST "localhost:9200/dl/_flush"
curl -XGET "localhost:9200/dl/_stats/docs,store?pretty"         # deleted docs 很多
curl -XPOST "localhost:9200/dl/_forcemerge?only_expunge_deletes=true&pretty"
curl -XGET "localhost:9200/dl/_stats/docs,store?pretty"         # deleted 归零、store 变小
```

**16-6 复现写限流（`is_throttled`）**
```bash
curl -XPUT "localhost:9200/th/_settings" -H 'Content-Type: application/json' \
  -d '{"index":{"refresh_interval":"100ms","merge.scheduler.max_thread_count":1,"translog.flush_threshold_size":"1gb"}}'
# 持续高速写入 + 大量小 refresh，同时观察
watch -n1 'curl -s "localhost:9200/_nodes/stats/indices/indexing,merges?pretty" | grep -E "is_throttled|throttle_time|throttled_time|current"'
```

**16-7 `refresh=wait_for` 的「卡住」现象**
```bash
curl -XPUT "localhost:9200/wf/_settings" -H 'Content-Type: application/json' -d '{"index":{"refresh_interval":"-1"}}'
time curl -XPUT "localhost:9200/wf/_doc/1?refresh=wait_for&timeout=5s" -H 'Content-Type: application/json' -d '{"a":1}'
# 期望 5s 后超时（因为没有其它写入触发 refresh）
```

**16-8 观察 `max_refresh_listeners` 强制刷新**
```bash
curl -XPUT "localhost:9200/mrl/_settings" -H 'Content-Type: application/json' \
  -d '{"index":{"refresh_interval":"-1","max_refresh_listeners":5}}'
# 并发发起 > 5 个 wait_for 写入 → 触发强制 refresh（返回后立即可见）
for i in $(seq 1 10); do curl -s -XPOST "localhost:9200/mrl/_doc/$i?refresh=wait_for" -H 'Content-Type: application/json' -d '{"a":1}' >/dev/null & done; wait
curl -XGET "localhost:9200/mrl/_count"
```

**16-9 观察 merge 参数影响**
```bash
# 把每层段数调小 → 更激进地合并
curl -XPUT "localhost:9200/mp/_settings" -H 'Content-Type: application/json' \
  -d '{"index":{"merge.policy.segments_per_tier":3,"merge.policy.max_merge_at_once":3}}'
# 写数据观察 merge 频率与段数
```

**16-10 断点追踪 flush 四步**
在 `InternalEngine.flush:1743`、`translog.rollGeneration`（第 14 章）、
`commitIndexWriter:2387`、`translog.trimUnreferencedReaders`（第 14 章）打断点，
执行 `POST /idx/_flush`，按顺序观察。

**16-11 观察 `commitData`**
在 `InternalEngine:2396`（`setLiveCommitData`）打断点执行 `_flush`，
检查 `commitData` 的 8 个 key。

**16-12 `_flush/synced`**
```bash
curl -XPOST "localhost:9200/rf/_flush/synced?pretty"
# 在只读索引上做，观察是否能成功（有写入时会 PENDING_OPERATIONS）
```

---

## 10. 自测题

1. Refresh / Flush / Merge 各自的目的是什么？三者的触发条件有何不同？
2. `maybeRefreshBlocking()` 为什么在「没有变化」时几乎零成本？
3. `Engine.refreshNeeded()` 怎么判断「需要 refresh」？为什么用 `isCurrent()==false`？
4. Refresh 的 4 个触发来源分别是什么？分别在哪个线程池执行？
5. `RefreshPolicy.WAIT_UNTIL` 为什么可能「卡住」？`max_refresh_listeners` 起什么作用？
6. `RefreshListeners.beforeRefresh/afterRefresh` 为什么要「快照 + 回调」两步？
7. `lastRefreshedCheckpoint` 是什么？它和 global checkpoint 是什么关系？
8. Flush 的四步顺序是什么？为什么不能颠倒？
9. `commitIndexWriter` 往 commitData 里写了哪些 key？各自的作用是什么？
10. 为什么 `max_seq_no` 必须用 `setLiveCommitData` 的迭代器「延迟计算」？
11. `ensureCanFlush()` 在什么情况下抛异常？为什么要有这个保护？
12. `shouldPeriodicallyFlush()` 的判定条件是什么？默认阈值多少？
13. `afterWriteOperation` 里 `flushOrRollRunning.compareAndSet` 的目的是什么？
14. 为什么必须 merge？不 merge 会有什么后果（三条）？
15. TieredMergePolicy 的核心思想是什么？`segments_per_tier` 默认多少？
16. `EngineMergeScheduler.beforeMerge/afterMerge` 在什么时候 `activateThrottling`？限流的后果是什么？
17. `IndexThrottle` 为什么用 `NoOpLock` 而不是直接用一个真实锁？
18. `index.merge.scheduler.max_merge_count` 默认值怎么算？超过它会怎样？
19. 为什么机械盘建议 `max_thread_count=1`？
20. `ElasticsearchConcurrentMergeScheduler.maybeStall` 为什么返回 true（不做 Lucene stall）？
21. `afterMerge` 里为什么要「merge 全完成后的一段时间没写入 → flush」？
22. `_forcemerge?max_num_segments=1` 有什么风险？官方为什么不建议在生产随机做？
23. `deletes_pct_allowed`（33%）的作用是什么？
24. `forceMergeDeletes` 和 `forceMerge` 的区别是什么？
25. `tryRenewSyncCommit` 在什么场景下能省掉一次完整 commit？

---

## 11. 关键类速查表

| 类 | 路径 | 一句话 |
| --- | --- | --- |
| `IndexService` | `index/IndexService.java` | ★ 定时任务宿主（`maybeRefreshEngine:711`、`AsyncRefreshTask:829`、`AsyncTranslogFSync:807`、`rescheduleRefreshTasks:669`） |
| `IndexShard` | `index/shard/IndexShard.java` | ★ `refresh:918`、`isRefreshNeeded:2733`、`addRefreshListener:2744`、`afterWriteOperation:2625`、`flushStats:942`、`refreshStats:937` |
| `InternalEngine` | `index/engine/InternalEngine.java` | ★ `refresh:1596`、`writeIndexingBuffer:1643`、`flush:1743`、`shouldPeriodicallyFlush:1713`、`forceMerge:1929`、`constructIndexWriter:2206`、`commitIndexWriter:2387`、`EngineMergeScheduler:2300` |
| `Engine` | `index/engine/Engine.java` | `refreshNeeded:1006`、`flush:1055`、`forceMerge:1099`、`IndexThrottle:271`、`SearcherScope:705` |
| `ElasticsearchConcurrentMergeScheduler` | `index/engine/ElasticsearchConcurrentMergeScheduler.java` | ★ 合并调度/统计（`:51`、`doMerge:83`、`maybeStall:165`、`refreshConfig:188`） |
| `MergeSchedulerConfig` | `index/MergeSchedulerConfig.java` | 合并线程设置（`:55`/`:60`/`:64`） |
| `MergePolicyConfig` | `index/MergePolicyConfig.java` | 合并策略设置（`:119-197`） |
| `EsTieredMergePolicy` | `index/EsTieredMergePolicy.java` | 分层合并（`:35`） |
| `ElasticsearchMergePolicy` | `index/shard/ElasticsearchMergePolicy.java` | 段升级 + 包装（`:47`） |
| `RefreshListeners` | `index/shard/RefreshListeners.java` | `wait_for` 实现（`:45`、`addOrNotify:87`、`refreshNeeded:139`、`beforeRefresh:169`、`afterRefresh:174`） |
| `OnGoingMerge` | `index/merge/OnGoingMerge.java` | 进行中的合并（`:30`） |
| `MergeStats` | `index/merge/MergeStats.java` | 合并统计（`:32`） |
| `SegmentsStats` | `index/engine/SegmentsStats.java` | 段统计 |
| `Translog` | `index/translog/Translog.java` | roll/trim（第 14 章） |
| `LiveVersionMap` | `index/engine/LiveVersionMap.java` | refresh 时切 map（第 15 章） |
| `TransportForceMergeAction` / `TransportRefreshAction` / `TransportFlushAction` | `action/admin/indices/` | REST 入口 |

---

**上一篇**：[15-存储引擎-InternalEngine与Lucene集成.md](./15-存储引擎-InternalEngine与Lucene集成.md)
**下一篇**：[17-副本复制与SeqNo一致性模型.md](./17-副本复制与SeqNo一致性模型.md)
