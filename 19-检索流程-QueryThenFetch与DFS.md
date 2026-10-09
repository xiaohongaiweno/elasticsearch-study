# 19 · 检索流程：Query-Then-Fetch 与 DFS

> 目标：把一次 `GET /index/_search` 从 REST 入口到最终 `SearchResponse` 的**完整 scatter-gather 链路**讲透，
> 并解释清楚 6.6.1 里那几个"为什么"：为什么要两阶段？DFS 多出来的那一轮在干什么？
> 为什么 `size=10` 但每个分片要算 `from+size` 条？为什么单分片会走"query AND fetch"优化？
>
> **为什么必须学**：
> - 「查询突然变慢 / 502」→ 要能分清是 **query 阶段**慢（分片上算 topN）、**fetch 阶段**慢（取 `_source`）、还是 **reduce** 慢（协调节点聚合）。
> - 「深分页 OOM / 超时」→ `from + size` 在每个分片都要算，且协调节点要持有 `shard_count × (from+size)` 个 `ScoreDoc`。
> - 「相关度打分不准」→ 分片级 IDF 偏差 → DFS（`dfs_query_then_fetch`）。
> - 「有的分片失败但查询还是返回了」→ `allow_partial_search_results`。
> - 「副本明明在，为什么查询还是打到主分片」→ `preference` / 自适应副本选择（ARS）。
> - 云服务的**查询性能优化、超时治理、慢查询治理**全部落在本章。
>
> **前置**：第 7 章（Action 框架）、第 11 章（分片分配）、第 15 章（Engine / Lucene）。
> **后续**：第 20 章 `20-聚合框架Aggregations.md`。

---

## 1. 一图总览：scatter-gather

```
                    协调节点（Coordinating Node）
  ┌──────────────────────────────────────────────────────────────────┐
  │ 0. TransportSearchAction.doExecute                  :177          │
  │    ├─ rewrite 查询（fetch 远端资源，如 terms lookup）                │
  │    ├─ 解析索引 / routing / alias filter / index boost              │
  │    ├─ OperationRouting.searchShards（preference / ARS 选副本）      │
  │    ├─ 分片数上限校验 / max_concurrent_shard_requests                │
  │    └─ [可选] CanMatchPreFilterSearchPhase（can_match 预过滤）       │
  │                                                                    │
  │ 1. QUERY 阶段   InitialSearchPhase.run()            :126           │
  │    ├─ 并发发 [phase/query] 给 N 个分片（默认并发 = min(256, 节点数×5)）│
  │    └─ 失败 → 换该分片的下一个副本（shardIt.nextOrNull()）            │
  │                       ↓ 每个分片返回 size 条 (docId, score, sort)   │
  │ 2. REDUCE      SearchPhaseController.reduce()        :687          │
  │    ├─ sortDocs + mergeTopDocs → 全局 topN（:153 / :233）            │
  │    ├─ 聚合 / suggest / profile 归约                                │
  │    └─ fillDocIdsToLoad → 每个分片要取哪些 doc（:286）                │
  │                       ↓                                            │
  │ 3. FETCH 阶段  FetchSearchPhase.innerRun()          :96            │
  │    └─ 只对"有命中的分片"发 [phase/fetch/id]                         │
  │                       ↓ 每个分片返回 _source / stored fields / ...  │
  │ 4. MERGE       SearchPhaseController.merge()         :305          │
  │    └─ 按全局顺序拼 hits → InternalSearchResponse                    │
  │ 5. [可选] ExpandSearchPhase（collapse 的 inner_hits）  :68          │
  │ 6. 响应        sendResponsePhase                     :209          │
  └──────────────────────────────────────────────────────────────────┘

  DFS 变体：在 1 之前多一轮
     1'. DFS 阶段  每个分片算本地 term/collection 统计 → DfsSearchResult
     1''.aggregateDfs 汇总成全局统计（SearchPhaseController:82）
     1'''.DfsQueryPhase 把全局统计发回各分片（带上 AggregatedDfs）→ 再走 QUERY
```

---

## 2. 必读源码清单

| 类 | 路径 | 作用 |
| --- | --- | --- |
| `TransportSearchAction` | `action/search/TransportSearchAction.java` | ★ 协调节点入口：`doExecute:177`、`executeSearch:291` |
| `SearchType` | `action/search/SearchType.java` | 检索类型枚举（`:27`），默认 `QUERY_THEN_FETCH`（`:51`） |
| `SearchPhase` / `SearchPhaseContext` | `action/search/SearchPhase.java`、`SearchPhaseContext.java` | 阶段抽象与上下文 |
| `AbstractSearchAsyncAction` | `action/search/AbstractSearchAsyncAction.java` | ★ 阶段机骨架：`start:112`、`executeNextPhase:124`、`onShardFailure:183` |
| `InitialSearchPhase` | `action/search/InitialSearchPhase.java` | ★ 第一轮扇出的并发控制（`run:126`） |
| `SearchQueryThenFetchAsyncAction` | `action/search/SearchQueryThenFetchAsyncAction.java` | Q_T_F 实现（`:59`） |
| `SearchDfsQueryThenFetchAsyncAction` | `action/search/SearchDfsQueryThenFetchAsyncAction.java` | DFS_Q_T_F 实现（`:61`） |
| `DfsQueryPhase` | `action/search/DfsQueryPhase.java` | DFS 第二轮（`run:64`） |
| `FetchSearchPhase` | `action/search/FetchSearchPhase.java` | ★ Fetch 阶段（`innerRun:96`） |
| `ExpandSearchPhase` | `action/search/ExpandSearchPhase.java` | collapse inner_hits（`:68`） |
| `CanMatchPreFilterSearchPhase` | `action/search/CanMatchPreFilterSearchPhase.java` | can_match 预过滤（`:44`） |
| `SearchPhaseController` | `action/search/SearchPhaseController.java` | ★★ reduce / merge / aggregateDfs |
| `SearchTransportService` | `action/search/SearchTransportService.java` | 各阶段的 transport action 名（`:73-83`） |
| `SearchService` | `search/SearchService.java` | ★ 数据节点侧：`executeQueryPhase:387`、`executeFetchPhase:562` |
| `QueryPhase` | `search/query/QueryPhase.java` | 分片级 query（`execute:95`） |
| `FetchPhase` | `search/fetch/FetchPhase.java` | 分片级 fetch（`execute:89`） |
| `OperationRouting` | `cluster/routing/OperationRouting.java` | 选副本：`searchShards:100`、`preferenceActiveShardIterator:145` |

---

## 3. 阶段 0：协调节点的准备工作

### 3.1 `doExecute`（`TransportSearchAction.java:177`）

```java
final long absoluteStartMillis = System.currentTimeMillis();
final long relativeStartNanos = System.nanoTime();
final SearchTimeProvider timeProvider =
        new SearchTimeProvider(absoluteStartMillis, relativeStartNanos, System::nanoTime);   // :180
ActionListener<SearchSourceBuilder> rewriteListener = ActionListener.wrap(source -> {
    ...
    if (remoteClusterIndices.isEmpty()) {
        executeSearch((SearchTask)task, timeProvider, searchRequest, localIndices, Collections.emptyList(),
            (clusterName, nodeId) -> null, clusterState, Collections.emptyMap(), listener,
            clusterState.getNodes().getDataNodes().size(), SearchResponse.Clusters.EMPTY);   // :193
    } else {
        remoteClusterService.collectSearchShards(...);  // 跨集群搜索（CCS）
    }
}, listener::onFailure);
if (searchRequest.source() == null) {
    rewriteListener.onResponse(searchRequest.source());
} else {
    Rewriteable.rewriteAndFetch(searchRequest.source(),
        searchService.getRewriteContext(timeProvider::getAbsoluteStartMillis), rewriteListener);   // :215
}
```

三个细节：

1. **`SearchTimeProvider`（`:137`）同时记录绝对时间（`System.currentTimeMillis()`）和单调时间（`nanoTime`）**。
   绝对时间用于索引名日期数学表达式和脚本里的 `$now`；单调时间用于计算 `took`（避免 NTP 校时导致负数）。
   **同一个 `timeProvider` 会被发到所有分片**，保证整个查询期间"现在"是一致的。
2. **`Rewriteable.rewriteAndFetch`**：有些查询在解析时需要异步取远端数据（如 terms lookup），
   所以先 rewrite 再继续。数据节点侧还会再做一次（第 5 节的 `rewriteShardRequest`）。
3. **CCS（跨集群搜索）** 会先并发向每个远端集群发 `_search/shards`，失败/超时的集群记为 `skipped`
   （`buildClusters:220`），体现在响应的 `_clusters` 字段里。

### 3.2 `executeSearch`（`:291`）—— 这一段的每一步都可能成为"慢"的来源

```java
clusterState.blocks().globalBlockedRaiseException(ClusterBlockLevel.READ);        // :297
final Index[] indices = resolveLocalIndices(localIndices, searchRequest.indicesOptions(), clusterState, timeProvider);
Map<String, AliasFilter> aliasFilter = buildPerIndexAliasFilter(searchRequest, clusterState, indices, remoteAliasMap);
Map<String, Set<String>> routingMap = indexNameExpressionResolver.resolveSearchRouting(clusterState, searchRequest.routing(),
    searchRequest.indices());
...
Map<String, Long> nodeSearchCounts = searchTransportService.getPendingSearchRequests();      // :310
GroupShardsIterator<ShardIterator> localShardsIterator = clusterService.operationRouting().searchShards(clusterState,
        concreteIndices, routingMap, searchRequest.preference(), searchService.getResponseCollectorService(), nodeSearchCounts);  // :311
GroupShardsIterator<SearchShardIterator> shardIterators = mergeShardsIterators(localShardsIterator, localIndices,
    remoteShardIterators);

failIfOverShardCountLimit(clusterService, shardIterators.size());                             // :316 ★
Map<String, Float> concreteIndexBoosts = resolveIndexBoosts(searchRequest, clusterState);

// optimize search type for cases where there is only one shard group to search on
if (shardIterators.size() == 1) {
    // if we only have one group, then we always want Q_T_F, no need for DFS, and no need to do THEN since we hit one shard
    searchRequest.searchType(QUERY_THEN_FETCH);                                               // :323 ★
}
if (searchRequest.allowPartialSearchResults() == null) {
   searchRequest.allowPartialSearchResults(searchService.defaultAllowPartialSearchResults());  // :327
}
if (searchRequest.isSuggestOnly()) {
    searchRequest.requestCache(false);                                                         // :331
    ...
}
...
if (searchRequest.isMaxConcurrentShardRequestsSet() == false) {
    searchRequest.setMaxConcurrentShardRequests(Math.min(256, nodeCount
        * IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.getDefault(Settings.EMPTY)));           // :354 ★
}
boolean preFilterSearchShards = shouldPreFilterSearchShards(searchRequest, shardIterators);    // :357
searchAsyncAction(task, searchRequest, shardIterators, timeProvider, connectionLookup, clusterState.version(),
    Collections.unmodifiableMap(aliasFilter), concreteIndexBoosts, routingMap, listener, preFilterSearchShards, clusters).start();  // :358
```

要点：

| 行 | 含义 |
| --- | --- |
| `:297` | 全局读阻塞检查（如集群级 `blocks.read`） |
| `:310` | 取"各节点在途搜索请求数" → 供 **ARS** 打分用 |
| `:311` | 选副本：`preference` 优先，其次 ARS 或随机 |
| `:316` | `action.search.shard_count.limit`（默认 `Long.MAX_VALUE`）→ 超限直接拒绝 |
| `:323` | **单分片组时强制降级为 Q_T_F**（DFS 无意义） |
| `:354` | 默认并发 = `min(256, 数据节点数 × 5)`（5 是默认分片数） |
| `:357` | can_match 预过滤的开关条件 |

### 3.3 选副本：`preference` 与 ARS

`OperationRouting.preferenceActiveShardIterator`（`:145`）：

```java
if (preference == null || preference.isEmpty()) {
    if (awarenessAttributes.isEmpty()) {
        if (useAdaptiveReplicaSelection) {
            return indexShard.activeInitializingShardsRankedIt(collectorService, nodeCounts);   // :152 ARS
        } else {
            return indexShard.activeInitializingShardsRandomIt();                               // :154 随机
        }
    } else {
        return indexShard.preferAttributesActiveInitializingShardsIt(awarenessAttributes, nodes);
    }
}
```

- **ARS（自适应副本选择）默认关闭**（`:53` `cluster.routing.use_adaptive_replica_selection = false`）。
  打开后会根据 `ResponseCollectorService` 收集的响应时间做**排序**，把请求发给"最快"的副本。
- 支持 `preference`：`_local`、`_prefer_nodes:xxx`、`_shards:2,3`、`_only_nodes:xxx`、自定义字符串（会 hash 成稳定值，用于**会话级结果一致性**）。

> **云服务建议**：读写分离场景给查询固定 `preference`（如用户 ID），既能打到副本减轻主分片压力，
> 又能保证同一用户翻页时结果稳定（不会因为随机选副本导致顺序抖动）。

### 3.4 can_match 预过滤（6.x 引入的"分片裁剪"）

```java
private boolean shouldPreFilterSearchShards(SearchRequest searchRequest, GroupShardsIterator<SearchShardIterator> shardIterators) {
    SearchSourceBuilder source = searchRequest.source();
    return searchRequest.searchType() == QUERY_THEN_FETCH && // we can't do this for DFS it needs to fan out to all shards all the time
            SearchService.canRewriteToMatchNone(source) &&
            searchRequest.getPreFilterShardSize() < shardIterators.size();          // :362-367
}
```

三个阶段（默认阈值 2 / 128，见 `pre_filter_shard_size`）：

1. 先发 `indices:data/read/search[can_match]`（`SearchTransportService.java:83`）给所有分片。
2. 分片用**索引级元数据**（如 `IndexSortConfig`、routing、`routing_partition`、日期范围）判断
   "这个查询在我这儿必然零命中"（`SearchService.canMatch:1076`）。
3. 匹配不上的分片直接 `iter.resetAndSkip()`（`CanMatchPreFilterSearchPhase.java:94`），不参与后续。

> **效果**：对时间序列索引（按日期分索引 + 范围查询）能砍掉大量分片，是最有效的查询优化之一。
> **限制**：不能用于 DFS（DFS 必须访问全部分片才能算准确 IDF）。

---

## 4. 阶段 1：QUERY 阶段（扇出）

### 4.1 阶段机：`AbstractSearchAsyncAction`

```java
public final void start() {                                  // :112
    if (getNumShards() == 0) {
        listener.onResponse(new SearchResponse(InternalSearchResponse.empty(), null, 0, 0, 0,
            buildTookInMillis(), ShardSearchFailure.EMPTY_ARRAY, clusters));
        return;
    }
    executePhase(this);
}
```

阶段推进的核心在 `executeNextPhase`（`:124`）：

```java
if (successfulOps.get() == 0) {                               // 全部分片失败
    ...
    onPhaseFailure(currentPhase, "all shards failed", cause);
} else {
    Boolean allowPartialResults = request.allowPartialSearchResults();
    if (allowPartialResults == false && shardFailures.get() != null) {
        onPhaseFailure(currentPhase, "Partial shards failure", null);      // :145
    } else {
        executePhase(nextPhase);
    }
}
```

> **这就是 `allow_partial_search_results` 的源码位置**：
> `true`（默认）时部分分片失败仍返回 200 + `_shards.failed > 0`；
> `false` 时直接抛 `SearchPhaseExecutionException`。

### 4.2 并发控制：`InitialSearchPhase.run()`

```126:169:action/search/InitialSearchPhase.java
```

```java
if (shardsIts.size() > 0) {
    int maxConcurrentShardRequests = Math.min(this.maxConcurrentShardRequests, shardsIts.size());
    final boolean success = shardExecutionIndex.compareAndSet(0, maxConcurrentShardRequests);
    assert success;
    if (request.allowPartialSearchResults() == false) {
        // Fail-fast：任何一个分片组没有任何可用副本 → 立刻失败
        for (int index = 0; index < shardsIts.size(); index++) {
            final SearchShardIterator shardRoutings = shardsIts.get(index);
            if (shardRoutings.size() == 0) {
                missingShards.append(shardRoutings.shardId());
            }
        }
        if (missingShards.length() > 0) {
            final String msg = "Search rejected due to missing shards ["+ missingShards +
                    "]. Consider using `allow_partial_search_results` setting to bypass this error.";
            throw new SearchPhaseExecutionException(getName(), msg, null, ShardSearchFailure.EMPTY_ARRAY);   // :152
        }
    }
    for (int index = 0; index < maxConcurrentShardRequests; index++) {
        final SearchShardIterator shardRoutings = shardsIts.get(index);
        assert shardRoutings.skip() == false;
        performPhaseOnShard(index, shardRoutings, shardRoutings.nextOrNull());
    }
}
```

**"窗口"语义**：先并发启动 `maxConcurrentShardRequests` 个，每完成一个就从 `shardExecutionIndex` 取下一个
（`maybeExecuteNext:163`）。这是典型的 **滑动窗口并发**，避免一个查询把集群打爆。

### 4.3 失败换副本：`onShardFailure`

```84:123:action/search/InitialSearchPhase.java
```

```java
if (totalOps.incrementAndGet() == expectedTotalOps) {
    onPhaseDone();                                    // 已经没有更多工作了
} else {
    final ShardRouting nextShard = shardIt.nextOrNull();
    final boolean lastShard = nextShard == null;
    if (!lastShard) {
        performPhaseOnShard(shardIndex, shardIt, nextShard);    // ★ 换下一个副本重试
    } else {
        maybeExecuteNext(); // move to the next execution if needed
        // no more shards active, add a failure
    }
}
```

> **注意**：这是**同一个分片组内的副本切换**，只会发生在"当前副本失败"时。
> 正常成功的情况下一个分片只发一次请求。

`performPhaseOnShard`（`:199`）里有一段注释值得记住（`:200-206`）：

> 完成后如果回调到**同一个线程**上，就 `fork()` 到线程池再继续，
> 否则会一路递归下去最终 StackOverflow。`maybeFork`（`:171`）就是干这个的。

### 4.4 构造 shard 请求

`AbstractSearchAsyncAction.buildShardSearchRequest`（`:321`）：

```java
String clusterAlias = shardIt.getClusterAlias();
AliasFilter filter = aliasFilter.get(shardIt.shardId().getIndex().getUUID());
float indexBoost = concreteIndexBoosts.getOrDefault(shardIt.shardId().getIndex().getUUID(), DEFAULT_INDEX_BOOST);
String indexName = shardIt.shardId().getIndex().getName();
final String[] routings = indexRoutings.getOrDefault(indexName, Collections.emptySet()).toArray(new String[0]);
return new ShardSearchTransportRequest(shardIt.getOriginalIndices(), request, shardIt.shardId(), getNumShards(),
    filter, indexBoost, timeProvider.getAbsoluteStartMillis(), clusterAlias, routings);
```

- `aliasFilter` 下发到数据节点（不是协调节点过滤）→ 别名过滤在分片上执行。
- `numberOfShards`（= `getNumShards()`）会带过去 —— **这就是"单分片走 query+fetch 优化"的判据**（第 5 节）。
- `absoluteStartMillis` 保证所有分片用同一个"现在"。

---

## 5. 数据节点：执行 QUERY

### 5.1 入口

`SearchService.executeQueryPhase(ShardSearchRequest, SearchTask, ActionListener)`（`SearchService.java:355`）
→ `rewriteShardRequest`（`:1101`，为了 BWC 和 alias filter）→ `executeQueryPhase(request, task)`（`:387`）：

```java
final SearchContext context = createAndPutContext(request);              // :388
final SearchOperationListener operationListener = context.indexShard().getSearchOperationListener();
context.incRef();
boolean queryPhaseSuccess = false;
try {
    context.setTask(task);
    operationListener.onPreQueryPhase(context);
    long time = System.nanoTime();
    contextProcessing(context);

    loadOrExecuteQueryPhase(request, context);                            // :398

    if (context.queryResult().hasSearchContext() == false && context.scrollContext() == null) {
        freeContext(context.id());                                        // :401 没有命中 → 立刻释放上下文
    } else {
        contextProcessedSuccessfully(context);
    }
    final long afterQueryTime = System.nanoTime();
    queryPhaseSuccess = true;
    operationListener.onQueryPhase(context, afterQueryTime - time);
    if (request.numberOfShards() == 1) {
        return executeFetchPhase(context, operationListener, afterQueryTime);   // :409 ★ 单分片优化
    }
    return context.queryResult();
} catch (Exception e) { ... } finally { cleanContext(context); }
```

**两个关键点**：

1. **单分片优化（`:408-410`）**：如果 `numberOfShards == 1`，query 之后**立刻本地执行 fetch**，
   返回 `QueryFetchSearchResult`。省掉一次网络往返，也省掉协调节点的 reduce。
   对应协调节点的 `queryAndFetchOptimization`（`FetchSearchPhase.java:102`）。
2. **上下文（SearchContext）**：保存了 `IndexSearcher`、docIdsToLoad、aggregators 等状态。
   只要 query 结果里还有"待 fetch 的 doc"（`hasSearchContext()`），上下文就必须留着直到 fetch 完成。
   **上下文泄漏是"search thread pool 打满 / 内存飙高"的常见原因**（见第 12 节）。

### 5.2 请求缓存

```java
private void loadOrExecuteQueryPhase(final ShardSearchRequest request, final SearchContext context) throws Exception {
    final boolean canCache = indicesService.canCache(request, context);      // :346
    context.getQueryShardContext().freezeContext();
    if (canCache) {
        indicesService.loadIntoContext(request, context, queryPhase);       // :349
    } else {
        queryPhase.execute(context);                                        // :351
    }
}
```

> **Request Cache 只在分片级别缓存"count/hits/aggs"，不缓存 `_source`**，
> 所以 fetch 阶段永远要走一次。这也是为什么 `size > 0` 的"取文档"查询无法被 request cache 命中
> （见 `canCache` 的判定：`request.requestCache()` 且 `source.size() == 0`）。

### 5.3 `QueryPhase.execute`（`QueryPhase.java:95`）

```java
if (searchContext.hasOnlySuggest()) {
    suggestPhase.execute(searchContext);
    searchContext.queryResult().topDocs(new TopDocs(0, Lucene.EMPTY_SCORE_DOCS, 0), new DocValueFormat[0]);
    return;
}
aggregationPhase.preProcess(searchContext);        // :112  聚合预处理（DFS 时已经在 dfs 阶段做过了）
final ContextIndexSearcher searcher = searchContext.searcher();
boolean rescore = execute(searchContext, searchContext.searcher(), searcher::setCheckCancelled);   // :114
if (rescore) {
    rescorePhase.execute(searchContext);           // :117
}
suggestPhase.execute(searchContext);               // :119
aggregationPhase.execute(searchContext);           // :120
if (searchContext.getProfilers() != null) { ... }
```

`QueryPhase.execute(searchContext, searcher, ...)`（`:134`）核心：

```java
queryResult.from(searchContext.from());
queryResult.size(searchContext.size());
Query query = searchContext.query();
assert query == searcher.rewrite(query); // already rewritten
// ... scroll 相关优化（MinDocQuery / SearchAfterSortedDocQuery / terminateAfter / trackTotalHits(false)）
final LinkedList<QueryCollectorContext> collectors = new LinkedList<>();
//  依次是：filtered → cancellable → minScore → earlyTermination → topDocs → multi
```

**collector 链**由 `QueryCollectorContext.create*CollectorContext` 静态工厂拼装（`:64-69`）：

| Collector | 作用 |
| --- | --- |
| `FilteredCollector` | 应用 alias filter / routing |
| cancellable（`:64`） | 支持取消（`_tasks/cancel`） |
| `MinScoreCollector` | `min_score` |
| early termination（`:65`） | `terminate_after` |
| `TopDocsCollector`（`TopDocsCollectorContext:69`） | 收集 `from + size` 条 |

> **深分页代价的源码根源就在这里**：每个分片都要收集 `from + size` 条并排序，
> 协调节点再对 `shard_count × (from+size)` 条做归并。所以 `from=10000, size=10` 在 100 个分片上
> 意味着 **1,000,000+ 个 ScoreDoc**。

---

## 6. 阶段 2：REDUCE（协调节点归约）

### 6.1 触发

`AbstractSearchAsyncAction.onPhaseDone()`（`:253`）→ `executeNextPhase(this, getNextPhase(results, this))`。
`getNextPhase` 由子类实现：

- `SearchQueryThenFetchAsyncAction.getNextPhase`（`:59`）：直接返回 `new FetchSearchPhase(...)`。
- `SearchDfsQueryThenFetchAsyncAction.getNextPhase`（`:61`）：返回 `new DfsQueryPhase(...)`
  → 它内部再 `nextPhaseFactory.apply(queryResult)` → `FetchSearchPhase`。

### 6.2 `SearchPhaseController.reduce()`

```java
public ReducedQueryPhase reduce() {                                       // :687
    return controller.reducedQueryPhase(results.asList(), getRemainingAggs(), getRemainingTopDocs(), topDocsStats,
        numReducePhases, false);
}
```

真正的实现 `reducedQueryPhase`（`:429`）做四件事：

1. **`numReducePhases++`**：记录归约轮数（大查询会分多轮 reduce 以控制内存，见 `:500` 附近的 `bufferedTopDocs`）。
2. **聚合归约**：`aggregationsList` → `InternalAggregation.reduce`（第 20 章详述）。
3. **suggest / profile 归约**。
4. **topDocs 归约**：`sortDocs(...) → mergeTopDocs(...)`（`:500`）。

```java
static TopDocs mergeTopDocs(Collection<TopDocs> results, int topN, int from) {     // :233
    if (results.isEmpty()) return null;
    final TopDocs topDocs = results.stream().findFirst().get();
    ...
    final TopDocs mergedTopDocs;
    // Lucene TopDocs.merge(start, topN, results, setShardIndex)
}
```

`sortDocs`（`:153`）负责：

- 按 sort 规则（score / field / `_doc`）比较；
- 加 **tie-breaker**：`shardIndex`（分片序号）保证同分时的**稳定排序**；
- 处理 `search_after`、collapse、`track_scores`。

> **常见误区**：`"_score 相同，翻页结果顺序不稳定"` ——
> 有了 `shardIndex` tie-breaker，同一次查询内部是稳定的；
> 但如果**不同副本的 maxDoc / 删除文档数不同**，同一个文档在两个副本上的 `_score` 可能不同，
> 导致换副本后顺序变化。解决：用 `preference` 固定副本，或改用显式 sort 字段。

### 6.3 `fillDocIdsToLoad`：算"要向谁取文档"

```286:304:action/search/SearchPhaseController.java
```

```java
public IntArrayList[] fillDocIdsToLoad(int numShards, ScoreDoc[] shardDocs) {
    IntArrayList[] docIdsToLoad = new IntArrayList[numShards];
    for (ScoreDoc shardDoc : shardDocs) {
        IntArrayList shardDocIdsToLoad = docIdsToLoad[shardDoc.shardIndex];
        if (shardDocIdsToLoad == null) {
            shardDocIdsToLoad = docIdsToLoad[shardDoc.shardIndex] = new IntArrayList();
        }
        shardDocIdsToLoad.add(shardDoc.doc);
    }
    return docIdsToLoad;
}
```

**结果**：`docIdsToLoad[i]` 为 `null` 表示第 i 个分片**没有任何文档进入全局 topN**
→ 后续不用给它发 fetch 请求（省一次网络 + 尽快释放其上下文）。

---

## 7. 阶段 3：FETCH

### 7.1 `FetchSearchPhase.innerRun`

```96:150:action/search/FetchSearchPhase.java
```

```java
final int numShards = context.getNumShards();
final boolean isScrollSearch = context.getRequest().scroll() != null;
List<SearchPhaseResult> phaseResults = queryResults.asList();
String scrollId = isScrollSearch ? TransportSearchHelper.buildScrollId(queryResults) : null;
final SearchPhaseController.ReducedQueryPhase reducedQueryPhase = resultConsumer.reduce();         // :101
final boolean queryAndFetchOptimization = queryResults.length() == 1;                              // :102
final Runnable finishPhase = () -> moveToNextPhase(searchPhaseController, scrollId, reducedQueryPhase,
    queryAndFetchOptimization ? queryResults : fetchResults);
if (queryAndFetchOptimization) {
    finishPhase.run();                                                                             // :110
} else {
    ScoreDoc[] scoreDocs = reducedQueryPhase.sortedTopDocs.scoreDocs;
    final IntArrayList[] docIdsToLoad = searchPhaseController.fillDocIdsToLoad(numShards, scoreDocs);  // :113
    if (scoreDocs.length == 0) { // no docs to fetch -- sidestep everything and return
        phaseResults.stream().map(SearchPhaseResult::queryResult)
            .forEach(this::releaseIrrelevantSearchContext);                                        // :117
        finishPhase.run();
    } else {
        final ScoreDoc[] lastEmittedDocPerShard = isScrollSearch ?
            searchPhaseController.getLastEmittedDocPerShard(reducedQueryPhase, numShards) : null;  // :121
        final CountedCollector<FetchSearchResult> counter = new CountedCollector<>(...);
        for (int i = 0; i < docIdsToLoad.length; i++) {
            IntArrayList entry = docIdsToLoad[i];
            SearchPhaseResult queryResult = queryResults.get(i);
            if (entry == null) { // no results for this shard ID
                if (queryResult != null) {
                    releaseIrrelevantSearchContext(queryResult.queryResult());                     // :134
                }
                counter.countDown();
            } else {
                ShardFetchSearchRequest fetchSearchRequest = createFetchRequest(queryResult.queryResult().getRequestId(), i, entry,
                    lastEmittedDocPerShard, queryResult.getSearchShardTarget().getOriginalIndices());
                executeFetch(i, searchShardTarget, counter, fetchSearchRequest, queryResult.queryResult(), connection);
            }
        }
    }
}
```

`run()`（`:79`）外面**先 fork 到线程池**：

```java
context.execute(new ActionRunnable<SearchResponse>(context) {
    public void doRun() throws IOException {
        // we do the heavy lifting in this inner run method where we reduce aggs etc. that's why we fork this phase
        // off immediately instead of forking when we send back the response to the user since there we only need
        // to merge together the fetched results which is a linear operation.
        innerRun();
    }
});
```

**为什么**：reduce（聚合归约、全局排序）是 CPU 密集的，不能占着 transport 线程。

### 7.2 `releaseIrrelevantSearchContext`：防止上下文泄漏

```187:199:action/search/FetchSearchPhase.java
```

```java
private void releaseIrrelevantSearchContext(QuerySearchResult queryResult) {
    // we only release search context that we did not fetch from if we are not scrolling
    // and if it has at lease one hit that didn't make it to the global topDocs
    if (context.getRequest().scroll() == null && queryResult.hasSearchContext()) {
        context.sendReleaseSearchContext(queryResult.getRequestId(), connection, ...);
    }
}
```

发送 `indices:data/read/search[free_context]`（`SearchTransportService.java:74`）。
**任何异常路径**（fetch 失败、线程池拒绝）都会补发这个请求（`:178`）。

> **排障**：`GET /_nodes/stats/indices/search?filter_path=**.open_contexts` 持续增长 = 上下文泄漏，
> 常见于**客户端发了查询就断开**（ES 会尽力清理，但大并发下可能来不及）。

### 7.3 数据节点的 fetch

`SearchService.executeFetchPhase(ShardFetchRequest, ...)`（`SearchService.java:562`）：

```java
final SearchContext context = findContext(request.id(), request);   // :564 按 requestId 找回 query 阶段的上下文
context.incRef();
try {
    context.setTask(task);
    contextProcessing(context);
    if (request.lastEmittedDoc() != null) {
        context.scrollContext().lastEmittedDoc = request.lastEmittedDoc();
    }
    ...
    context.docIdsToLoad(request.docIds(), 0, request.docIdsSize());
    fetchPhase.execute(context);
    ...
```

`FetchPhase.execute`（`FetchPhase.java:89`）核心循环：

```java
FetchSubPhase.HitContext hitContext = new FetchSubPhase.HitContext();
for (int index = 0; index < context.docIdsToLoadSize(); index++) {
    if (context.isCancelled()) throw new TaskCancelledException("cancelled");       // :146
    int docId = context.docIdsToLoad()[context.docIdsToLoadFrom() + index];
    int readerIndex = ReaderUtil.subIndex(docId, context.searcher().getIndexReader().leaves());   // :150
    LeafReaderContext subReaderContext = context.searcher().getIndexReader().leaves().get(readerIndex);
    int subDocId = docId - subReaderContext.docBase;
    final SearchHit searchHit;
    ...
    for (FetchSubPhase fetchSubPhase : fetchSubPhases) {
        fetchSubPhase.hitExecute(context, hitContext);                               // :167
    }
}
for (FetchSubPhase fetchSubPhase : fetchSubPhases) {
    fetchSubPhase.hitsExecute(context, hits);                                        // :175 批量阶段（如 inner_hits）
}
context.fetchResult().hits(new SearchHits(hits, context.queryResult().getTotalHits(), context.queryResult().getMaxScore()));
```

`fetchSubPhases` 由插件可插拔（`FetchPhase` 构造函数 `:79-82`），内置的有：
`ExplainFetchSubPhase`、`MatchedQueriesFetchSubPhase`、`ScriptFieldsPhase`、`FetchSourceSubPhase`
（`_source`）、`FetchDocValuesPhase`、`FetchFieldsPhase`、`StoredFieldsPhase`、
`HighlightPhase`、`FetchVersionPhase`、`SeqNoPrimaryTermPhase`、`MatchedQueriesFetchSubPhase`、
以及最后追加的 `InnerHitsFetchSubPhase`（`:81`）。

> **性能提示**：`_source` 的读取是 fetch 阶段最重的部分之一。
> 用 `stored_fields` + `docvalue_fields` 替代大 `_source`、或用 `_source_includes` 裁剪，直接减少这里的开销。

`docId → 具体 segment` 的映射用 `ReaderUtil.subIndex`（`:150`），
因为 `docIdsToLoad` 里的 docId 是**分片级（跨 segment 的全局编号）**。

---

## 8. 阶段 4~6：MERGE / expand / 响应

```java
private void moveToNextPhase(SearchPhaseController searchPhaseController,
                             String scrollId, SearchPhaseController.ReducedQueryPhase reducedQueryPhase,
                             AtomicArray<? extends SearchPhaseResult> fetchResultsArr) {
    final InternalSearchResponse internalResponse = searchPhaseController.merge(context.getRequest().scroll() != null,
        reducedQueryPhase, fetchResultsArr.asList(), fetchResultsArr::get);       // :204
    context.executeNextPhase(this, nextPhaseFactory.apply(internalResponse, scrollId));
}
```

`SearchPhaseController.merge`（`:305`）：

- 按 `reducedQueryPhase.sortedTopDocs.scoreDocs` 的全局顺序，从对应分片的 `SearchPhaseResult` 里取 `SearchHit`；
- 组装 `SearchHits`（`:397`：`new SearchHits(hits, reducedQueryPhase.totalHits, reducedQueryPhase.maxScore)`）；
- 组装 `InternalSearchResponse`（`:593`：`hits + aggregations + suggest + profile + timedOut + terminatedEarly + numReducePhases`）。

之后：

- `ExpandSearchPhase`（`:44`，name = `"expand"`）：仅当请求带 `collapse` 且有 `inner_hits` 时执行。
  它会对每个 collapse group **再发一次 msearch**（`:73-96`）—— 这是 collapse + inner_hits 昂贵的根本原因。
- `sendResponsePhase`（`FetchSearchPhase.java:209`）→ `context.buildSearchResponse(...)`（`:213`）。

`buildSearchResponse`（`AbstractSearchAsyncAction.java:278`）最后还会再检查一次
`allowPartialSearchResults == false && failures.length > 0`。

---

## 9. DFS：`dfs_query_then_fetch`

### 9.1 为什么要 DFS

默认 Q_T_F 下，**每个分片用自己的本地 IDF 计算 `_score`**：

```
分片 0：1000 个文档，"elasticsearch" 出现在 500 个 → IDF 低
分片 1：1000 个文档，"elasticsearch" 出现在 5 个 → IDF 高
```

同一个词在不同分片上"稀有度"不同 → 打分不可比。
**分片数多、数据分布不均时尤其明显**（这也是 `search_type` 在 6.x 仍保留 DFS 的原因）。

DFS 通过**先做一轮"分布式词频统计"**来解决：

1. **DFS 阶段**：每个分片把查询涉及的 term 的本地 `TermStatistics` / `CollectionStatistics` 返回。
2. **aggregateDfs**：协调节点汇总成全局统计（`SearchPhaseController.aggregateDfs:82`）。
3. **QUERY 阶段**：把 `AggregatedDfs` 发回各分片，各分片用全局统计重新打分。

### 9.2 源码

`SearchDfsQueryThenFetchAsyncAction.getNextPhase`（`:61`）：

```java
return new DfsQueryPhase(results.getAtomicArray(), searchPhaseController, (queryResults) ->
    new FetchSearchPhase(queryResults, searchPhaseController, context), context);
```

`DfsQueryPhase.run()`（`:64`）：

```java
final List<DfsSearchResult> resultList = dfsSearchResults.asList();
final AggregatedDfs dfs = searchPhaseController.aggregateDfs(resultList);          // :68
final CountedCollector<SearchPhaseResult> counter = new CountedCollector<>(queryResult::consumeResult,
    resultList.size(), () -> context.executeNextPhase(this, nextPhaseFactory.apply(queryResult)), context);
for (final DfsSearchResult dfsResult : resultList) {
    ...
    QuerySearchRequest querySearchRequest = new QuerySearchRequest(searchShardTarget.getOriginalIndices(),
            dfsResult.getRequestId(), dfs);                                        // :75-76 ★ 携带全局统计
    searchTransportService.sendExecuteQuery(connection, querySearchRequest, context.getTask(), ...);
}
```

数据节点侧（`SearchService.executeQueryPhase(QuerySearchRequest, ...)`，`SearchService.java:474`）：

```java
contextProcessing(context);
context.searcher().setAggregatedDfs(request.dfs());       // :483 ★ 用全局统计覆盖本地 IDF
operationListener.onPreQueryPhase(context);
queryPhase.execute(context);
```

`aggregateDfs`（`SearchPhaseController.java:82`）：

```java
ObjectObjectHashMap<Term, TermStatistics> termStatistics = HppcMaps.newNoNullKeysMap();
ObjectObjectHashMap<String, CollectionStatistics> fieldStatistics = HppcMaps.newNoNullKeysMap();
long aggMaxDoc = 0;
for (DfsSearchResult lEntry : results) {
    final Term[] terms = lEntry.terms();
    final TermStatistics[] stats = lEntry.termStatistics();
    for (int i = 0; i < terms.length; i++) {
        TermStatistics existing = termStatistics.get(terms[i]);
        if (existing != null) { /* 累加 docFreq / totalTermFreq */ }
        ...
    }
}
```

### 9.3 代价与建议

| | QUERY_THEN_FETCH | DFS_QUERY_THEN_FETCH |
| --- | --- | --- |
| 网络往返 | 2 轮（query、fetch） | 3 轮（dfs、query、fetch） |
| 打分准确度 | 分片级 IDF，可能不准 | 全局 IDF，准确 |
| 适用 | 绝大多数场景（默认） | 打分敏感 + 分片多 + 数据分布不均 |

**建议**：

- 默认别用 DFS（多一轮扇出，延迟明显上升）。
- 打分不准时**先考虑减少分片数 / 用 `dfs_query_then_fetch` 验证是否真的是 IDF 问题**。
- `size=0` 的聚合请求不需要 DFS。
- 单分片时 ES 会**强制降级**为 Q_T_F（`TransportSearchAction.java:323`）。
- can_match 预过滤**不能**用于 DFS（`:364` 注释明确说明）。

---

## 10. Scroll

- `FetchSearchPhase.innerRun:100`：`isScrollSearch ? TransportSearchHelper.buildScrollId(queryResults) : null`。
- scroll 期间 `SearchContext` **不释放**（`fetchPhaseShouldFreeContext:507` 返回 false），
  所以 scroll 会**长时间占用 search context 内存**。
- 每次 scroll 携带 `lastEmittedDocPerShard`（`:121` + `getLastEmittedDocPerShard:267`），
  数据节点据此跳过已返回的文档（`MinDocQuery` / `SearchAfterSortedDocQuery`，`QueryPhase.java:160-178`）。
- 相关的 transport action：`[phase/query/scroll]`、`[phase/fetch/id/scroll]`、`[free_context/scroll]`（`SearchTransportService.java:79-81`）。

> **生产建议**：深分页一律用 `search_after`（无状态）而不是 scroll；scroll 只用于**全量导出**，
> 且用完必须 `DELETE /_search/scroll`。并发 scroll 数受 `search.max_open_scroll_context` 限制（默认无限制，建议设）。

---

## 11. 容错与取消

| 机制 | 位置 | 说明 |
| --- | --- | --- |
| 换副本重试 | `InitialSearchPhase.onShardFailure:103-110` | 同一分片组内的下一个副本 |
| `allow_partial_search_results` | `AbstractSearchAsyncAction.executeNextPhase:137-145` | false 时任何分片失败即整体失败 |
| Fail-fast（无可用副本） | `InitialSearchPhase.run:136-154` | 同上，且提前抛错 |
| 忽略"分片不可用"类失败 | `AbstractSearchAsyncAction.onShardFailure:185` | `TransportActions.isShardNotAvailableException` 不计入 failures |
| 覆盖更有意义的异常 | `:203` `isReadOverrideException` | 不要被"illegal shard state"盖住真正的根因 |
| 整体失败时释放上下文 | `raisePhaseFailure:222-232` | 逐个发 `free_context` |
| 取消 | `FetchPhase:146` `context.isCancelled()`、`QueryPhase` cancellable collector | `POST _tasks/cancel`、`search.low_level_cancellation` |
| 超时 | `SearchContext.timeout`（默认无） | 分片级超时，返回部分结果 + `timed_out=true` |

---

## 12. 排障手册

### 12.1 先定位慢在哪个阶段

```bash
# 1) Profile API：看分片上 query/fetch 各子步骤耗时
GET /my-index/_search
{"profile":true,"query":{"match":{"title":"elasticsearch"}}}

# 2) 慢查询日志（按阶段分别记录）
PUT /my-index/_settings
{"index.search.slowlog.threshold.query.warn":"1s",
 "index.search.slowlog.threshold.fetch.warn":"500ms"}

# 3) 节点统计
GET /_nodes/stats/indices/search?filter_path=**.total,**.open_contexts
#   query_total / query_time_in_millis / fetch_total / fetch_time_in_millis
#   open_contexts 持续增长 = 上下文泄漏

# 4) 正在跑的任务
GET /_tasks?detailed=true&actions=*search
```

**判定表**：

| 现象 | 慢在哪 |
| --- | --- |
| `took` 大、`query_time_in_millis` 大 | 分片上 query（Lucene 搜索 + 聚合） |
| `fetch_time_in_millis` 大 | `_source` / highlight / 大文档 |
| `profile` 里 `rewrite_time` 大 | 查询 rewrite（如大量 term、正则、wildcard） |
| `num_reduce_phases > 1` 且耗时长 | 协调节点归约内存压力大（分片太多 / size 太大） |
| `open_contexts` 高 | 上下文没释放（scroll 未清 / 客户端断连 / fetch 失败） |

### 12.2 常见报错 → 源码定位

| 报错 | 原因 | 位置 |
| --- | --- | --- |
| `Search rejected due to missing shards [...]` | `allow_partial_search_results=false` 且有分片无可用副本 | `InitialSearchPhase.java:150` |
| `all shards failed` | 所有分片都失败 | `AbstractSearchAsyncAction.java:133` |
| `Partial shards failure` | 同上 + partial 关闭 | `:145` |
| `No search type for [...]` | `search_type` 拼错（只认 `query_then_fetch` / `dfs_query_then_fetch`） | `SearchType.java:99` |
| `SearchParseException` / `QueryPhaseExecutionException` | 分片上查询解析/执行失败 | `QueryPhase.java:95` |
| `TaskCancelledException("cancelled")` | 任务被取消 | `FetchPhase.java:147` |
| `trying to use more than ... shards` | 分片数超 `action.search.shard_count.limit` | `TransportSearchAction.java:430` |

### 12.3 优化清单（按性价比排序）

1. **控制分片扇出**：`size(from+size) × shard_count` 决定内存。能合并索引就合并。
2. **用 `search_after` 替代深分页**；或用 scroll 做导出。
3. **裁剪 `_source`**：`_source_includes` / `stored_fields` / `docvalue_fields`。
4. **合理 `preference`** 打到副本，减轻主分片。
5. **`size=0` 的聚合查询**关掉 `track_total_hits`（6.6 还没这个参数，但可 `terminate_after`）。
6. **时间范围查询**利用 can_match 预过滤（按时间建索引）；用 `index.sort` 让 ES 能提前终止。
7. **开启 ARS**（`cluster.routing.use_adaptive_replica_selection: true`）让请求避开慢节点。
8. **调 `max_concurrent_shard_requests`**：太大会打爆数据节点，太小会拉长延迟。
9. **聚合**用 `execution_hint`、`shard_size`、`composite` 分批（第 20 章）。
10. **Request Cache**：`size=0` 的聚合/统计查询默认可缓存；`?request_cache=true`。

---

## 13. 动手实验

### 实验 1：观察两阶段

```bash
PUT /s_test {"settings":{"number_of_shards":3,"number_of_replicas":0}}
POST /s_test/_doc/_bulk
{"index":{"_id":"1"}}\n{"t":"hello elasticsearch"}
{"index":{"_id":"2"}}\n{"t":"hello world"}
{"index":{"_id":"3"}}\n{"t":"elasticsearch search"}

GET /s_test/_search?pretty
{"query":{"match":{"t":"elasticsearch"}}}
# 观察 took / _shards{total,successful,skipped,failed} / hits.total

# 打开 profile
GET /s_test/_search
{"profile":true,"query":{"match":{"t":"elasticsearch"}}}
```

### 实验 2：DFS 与 Q_T_F 的打分差异

```bash
# 制造数据倾斜：一个分片塞很多 "elasticsearch"，另一个只塞少量
POST /s_test/_doc/_bulk?routing=a
... 大量含 elasticsearch 的文档 ...

GET /s_test/_search?search_type=query_then_fetch&explain=true  {"query":{"match":{"t":"elasticsearch"}}}
GET /s_test/_search?search_type=dfs_query_then_fetch&explain=true {"query":{"match":{"t":"elasticsearch"}}}
# 对比 explain 里的 idf
```

### 实验 3：partial results

```bash
# 停掉一个数据节点（有主分片）
GET /s_test/_search                                   # 默认返回 200 + _shards.failed
GET /s_test/_search?allow_partial_search_results=false  # 直接报错
```

### 实验 4：上下文泄漏

```bash
# 开一个 scroll 不清理
POST /s_test/_search?scroll=10m {"size":10,"query":{"match_all":{}}}
# 反复观察
GET /_nodes/stats/indices/search?filter_path=**.open_contexts
```

### 实验 5：断点清单

| 断点 | 目的 |
| --- | --- |
| `TransportSearchAction.executeSearch`（`:291`） | 看分片组是怎么算出来的 |
| `InitialSearchPhase.run`（`:126`） | 看并发窗口 |
| `InitialSearchPhase.onShardFailure`（`:103`） | 看换副本 |
| `SearchService.executeQueryPhase`（`:387`） | 数据节点入口 |
| `QueryPhase.execute`（`:95`） | collector 拼装 |
| `SearchPhaseController.reducedQueryPhase`（`:429`） | reduce |
| `FetchSearchPhase.innerRun`（`:96`） | fillDocIdsToLoad + 发 fetch |
| `FetchPhase.execute`（`:145`） | 逐 doc 取 `_source` |
| `SearchPhaseController.merge`（`:305`） | 最终组装 |
| `DfsQueryPhase.run`（`:64`） | DFS 第二轮 |

### 实验 6：单元测试

```bash
./gradlew :server:test --tests "org.elasticsearch.action.search.SearchPhaseControllerTests"
./gradlew :server:test --tests "org.elasticsearch.search.query.QueryPhaseTests"
./gradlew :server:test --tests "org.elasticsearch.search.fetch.FetchPhaseDocsIteratorTests"
./gradlew :server:test --tests "org.elasticsearch.search.basic.SearchWithRandomExceptionsIT"
```

---

## 14. 自测题（答案都在本章）

1. 6.6.1 支持哪几种 `search_type`？默认是什么？`QUERY_AND_FETCH` 还在吗？
2. 一次 Q_T_F 查询在协调节点上有哪几个阶段？各自由哪个类负责？
3. 为什么单分片时 ES 会强制把 `search_type` 改成 `QUERY_THEN_FETCH`？
4. `max_concurrent_shard_requests` 的默认值是多少？怎么算的？
5. `SearchTimeProvider` 为什么同时记录 `currentTimeMillis` 和 `nanoTime`？
6. 分片请求里带 `numberOfShards` 是干什么用的？
7. `aliasFilter` 是在协调节点过滤还是在数据节点过滤？为什么？
8. ARS 是什么？默认开启吗？它依赖什么数据？
9. can_match 预过滤的三个启用条件是什么？为什么 DFS 不能用它？
10. `InitialSearchPhase` 的并发模型是什么？"窗口"是怎么滑动的？
11. 一个分片查询失败后，ES 会怎么做？
12. `performPhaseOnShard` 里为什么要 `maybeFork`？不 fork 会怎样？
13. 数据节点上什么时候会走"query + fetch"合并执行？判据是什么？
14. `SearchContext` 什么时候被创建、什么时候被释放？泄漏的常见原因有哪些？
15. Request Cache 缓存什么？为什么不缓存 `_source`？
16. reduce 阶段做的四件事是什么？
17. `sortDocs` 用什么做 tie-breaker？为什么需要它？
18. 为什么 `_score` 相同但换副本后顺序可能变化？
19. `fillDocIdsToLoad` 返回 `null` 的元素意味着什么？协调节点怎么处理它？
20. `FetchSearchPhase.run()` 为什么先 fork 到线程池？
21. fetch 阶段 `docIdsToLoad` 里的 docId 是什么编号？怎么定位到具体 segment？
22. `FetchSubPhase` 有哪些？列举至少 5 个。
23. DFS 为什么要多一轮？`aggregateDfs` 汇总了什么？
24. 数据节点怎么把全局统计用起来？
25. 什么情况下应该用 DFS？它的代价是什么？
26. scroll 期间 `SearchContext` 为什么不释放？会带来什么风险？
27. `allow_partial_search_results` 在两个地方被检查？分别在哪些行？
28. 看到 `Search rejected due to missing shards` 该怎么处理？
29. 一个 200 分片、from=1000、size=10 的查询，协调节点最多要持有多少个 `ScoreDoc`？
30. 慢查询你会按什么顺序定位和优化？（至少 6 步）

---

## 15. 关键类速查表

| 类 | 路径 | 关键行 |
| --- | --- | --- |
| `TransportSearchAction` | `action/search/TransportSearchAction.java` | `SearchTimeProvider:137`、`doExecute:177`、`buildClusters:220`、`executeSearch:291`、**单分片降级 `:323`**、`shouldPreFilterSearchShards:362`、`searchAsyncAction:384`（DFS 分支 `:412`）、`failIfOverShardCountLimit:430` |
| `SearchType` | `action/search/SearchType.java` | `enum:27`、`DEFAULT:51`、`CURRENTLY_SUPPORTED:56`、`fromId:75`、`fromString:90` |
| `AbstractSearchAsyncAction` | `action/search/AbstractSearchAsyncAction.java` | `start:112`、**`executeNextPhase:124`**、`onShardFailure:183`、`raisePhaseFailure:222`、`onShardSuccess:237`、`onPhaseDone:253`、`buildSearchResponse:278`、`buildShardSearchRequest:321` |
| `InitialSearchPhase` | `action/search/InitialSearchPhase.java` | `onShardFailure:84`、**`run:126`**、`maybeExecuteNext:163`、`maybeFork:171`、`fork:179`、**`performPhaseOnShard:199`**、`onShardResult:234`、`successfulShardExecution:246` |
| `SearchQueryThenFetchAsyncAction` | `action/search/SearchQueryThenFetchAsyncAction.java` | `executePhaseOnShard:55`、`getNextPhase:59` |
| `SearchDfsQueryThenFetchAsyncAction` | `action/search/SearchDfsQueryThenFetchAsyncAction.java` | `getNextPhase:61` |
| `DfsQueryPhase` | `action/search/DfsQueryPhase.java` | `class:42`、`run:64`、`aggregateDfs 调用:68`、`QuerySearchRequest:75` |
| `FetchSearchPhase` | `action/search/FetchSearchPhase.java` | `run:79`、**`innerRun:96`**、`reduce:101`、`queryAndFetchOptimization:102`、`fillDocIdsToLoad:113`、`createFetchRequest:152`、`executeFetch:158`、`releaseIrrelevantSearchContext:187`、`moveToNextPhase:201`、`sendResponsePhase:209` |
| `ExpandSearchPhase` | `action/search/ExpandSearchPhase.java` | `class:44`、`run:68` |
| `CanMatchPreFilterSearchPhase` | `action/search/CanMatchPreFilterSearchPhase.java` | `class:44`、`resetAndSkip:94` |
| `SearchPhaseController` | `action/search/SearchPhaseController.java` | `aggregateDfs:82`、`sortDocs:153`、`mergeTopDocs:233`、`getLastEmittedDocPerShard:267`、`fillDocIdsToLoad:286`、`merge:305`、`reducedQueryPhase:413/429`、`ReducedQueryPhase:532`、`buildResponse:592`、`reduce():687` |
| `SearchTransportService` | `action/search/SearchTransportService.java` | `FREE_CONTEXT:74`、`DFS:76`、`QUERY:77`、`QUERY_ID:78`、`QUERY_SCROLL:79`、`FETCH_ID:82`、`CAN_MATCH:83` |
| `SearchService` | `search/SearchService.java` | `executeDfsPhase:306`、`loadOrExecuteQueryPhase:345`、`executeQueryPhase:355/387`、`executeFetchPhase(context):429`、`executeQueryPhase(QuerySearchRequest):474`、`executeFetchPhase(ShardFetchRequest):562`、`createContext:649`、`freeContext:729`、`canMatch:1076`、`canRewriteToMatchNone:1089`、`rewriteShardRequest:1101` |
| `QueryPhase` | `search/query/QueryPhase.java` | `class:76`、`execute:95`、**核心 `execute(searchContext, searcher, ...):134`**、`collectors:184` |
| `FetchPhase` | `search/fetch/FetchPhase.java` | `class:74`、`ctor:79`、`execute:89`、逐 doc 循环 `:145`、`hitExecute:167`、`hitsExecute:175` |
| `OperationRouting` | `cluster/routing/OperationRouting.java` | ARS setting `:53`、`searchShards:92/100`、`preferenceActiveShardIterator:145` |

---

**上一篇**：[18-分片恢复Recovery.md](./18-分片恢复Recovery.md)
**下一篇**：[20-聚合框架Aggregations.md](./20-聚合框架Aggregations.md)
