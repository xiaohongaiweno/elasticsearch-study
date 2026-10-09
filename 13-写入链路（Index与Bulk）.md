# 13 · 写入链路（Index 与 Bulk）

> 目标：把一条 `POST /idx/_doc` 或一次 `_bulk` 请求，从 REST 层一路追到
> **Lucene 写段 + Translog** 的完整路径搞清楚，并理解 ES 6.6.1 的
> **主副本复制（Primary-Replica）** 是怎么嵌在写入路径里的。
>
> **为什么必须学**：
> - 写入是云服务里最核心的 SLA 指标（P99 写延迟、bulk 拒绝率、写入放大）。
> - 线上 90% 的「写变慢 / 写失败」问题，都落在这一条链路的某一个环节上：
>   `wait_for_active_shards` 卡住、`delayed_timeout`、`es_rejected_execution`、
>   版本冲突、mapping 更新、translog sync 慢、refresh 慢。
> - 读完本章你才能看懂第 14 章（Translog）、15 章（Engine）、17 章（SeqNo 一致性）。
>
> **前置**：第 07 章（Action 框架）、第 08 章（ClusterState/RoutingTable）、第 12 章（索引创建）。
> **后续**：第 14 章 `14-Translog事务日志.md`。

---

## 1. 全景图：一次写入的七层穿越

```
┌────────────────────────────────────────────────────────────────────────────────┐
│ ① REST 层                                                                       │
│   RestIndexAction / RestBulkAction / RestDeleteAction / RestUpdateAction        │
│   → 解析 XContent、构建 IndexRequest / BulkRequest                              │
└───────────────────────────────────┬────────────────────────────────────────────┘
                                    │ client.index(...) / client.bulk(...)
┌───────────────────────────────────▼────────────────────────────────────────────┐
│ ② 协调节点（Coordinating Node）的 Action 层                                     │
│   IndexAction.NAME  = "indices:data/write/index"     （IndexAction.java:28）    │
│   BulkAction.NAME   = "indices:data/write/bulk"      （BulkAction.java:30）     │
│   ★ TransportBulkAction.doExecute:154                                           │
│      ├─ needToCheck() → 自动建索引（AutoCreateIndex）                           │
│      ├─ executeIngestAndBulk:226  → 先跑 Ingest pipeline                        │
│      └─ BulkOperation.doRun:335   → 按 shard 分组（:388-400）                   │
│           └─ 每个 shard 一个 BulkShardRequest，调 shardBulkAction(:420)         │
└───────────────────────────────────┬────────────────────────────────────────────┘
                                    │ indices:data/write/bulk[s]
┌───────────────────────────────────▼────────────────────────────────────────────┐
│ ③ 分片级复制入口（Primary 所在节点）                                            │
│   ★ TransportReplicationAction.doExecute:171 → ReroutePhase.doRun:776           │
│      ├─ resolveRequest（补 waitForActiveShards）                                │
│      ├─ primary(state) → 拿到 primary ShardRouting                              │
│      ├─ 本节点是 primary → performLocalAction:818                               │
│      └─ 否则 → performRemoteAction:828（转发到 primary 节点）                   │
│   AsyncPrimaryAction.doRun:349 → acquirePrimaryOperationPermit                  │
│      → runWithPrimaryShardReference:376 → createReplicatedOperation:486         │
└───────────────────────────────────┬────────────────────────────────────────────┘
                                    │
┌───────────────────────────────────▼────────────────────────────────────────────┐
│ ④ Primary 上的业务执行                                                          │
│   ★ TransportShardBulkAction.shardOperationOnPrimary:115                        │
│      → performOnPrimary:143 → executeBulkItemRequest:167                        │
│         ├─ UPDATE 先 UpdateHelper.prepare 翻译成 index/delete（:176）           │
│         ├─ executeIndexRequestOnPrimary:454（动态 mapping 处理 :482）            │
│         └─ executeDeleteRequestOnPrimary:470                                    │
│   → IndexShard.applyIndexOperationOnPrimary → InternalEngine.index()            │
│      → Lucene IndexWriter.addDocument + Translog.add()                          │
│   → WritePrimaryResult:128（携带 Translog.Location）                            │
└───────────────────────────────────┬────────────────────────────────────────────┘
                                    │ ReplicationOperation.execute()
┌───────────────────────────────────▼────────────────────────────────────────────┐
│ ⑤ 副本复制（ReplicationOperation，见第 17 章）                                  │
│   对每个 in-sync replica：ReplicasProxy.performOn:1162                          │
│      → "indices:data/write/bulk[s][r]" → ReplicaOperationTransportHandler       │
│      → TransportShardBulkAction.shardOperationOnReplica:379                     │
│         → performOpOnReplica:413 → applyIndexOperationOnReplica                 │
│   所有副本 ack（或超时）后 → PrimaryResult.respond(:530) 返回客户端             │
└───────────────────────────────────┬────────────────────────────────────────────┘
                                    │
┌───────────────────────────────────▼────────────────────────────────────────────┐
│ ⑥ 写后动作（AsyncAfterWriteAction，TransportWriteAction:274）                   │
│      ├─ refresh policy = IMMEDIATE → indexShard.refresh(...)                    │
│      ├─ refresh policy = WAIT_UNTIL → addRefreshListener（等可见）             │
│      └─ durability = REQUEST → indexShard.sync(location)（fsync translog）     │
└────────────────────────────────────────────────────────────────────────────────┘
```

**一句话记忆**：`Bulk/Index 请求 = 协调节点按 shard 分组 → primary 执行 → 同步复制到副本 → 写后 refresh/fsync`。

---

## 2. 必读源码清单

| 环节 | 类 | 路径 |
| --- | --- | --- |
| bulk 协调 | `TransportBulkAction` | `server/.../action/bulk/TransportBulkAction.java` |
| bulk 分片执行 | `TransportShardBulkAction` | `server/.../action/bulk/TransportShardBulkAction.java` |
| 主副本框架 | `TransportReplicationAction` | `server/.../action/support/replication/TransportReplicationAction.java` |
| 写操作框架 | `TransportWriteAction` | `server/.../action/support/replication/TransportWriteAction.java` |
| 复制协调 | `ReplicationOperation` | `server/.../action/support/replication/ReplicationOperation.java` |
| 单文档写 | `TransportIndexAction` / `TransportDeleteAction` / `TransportUpdateAction` | `server/.../action/{index,delete,update}/` |
| 请求对象 | `IndexRequest` / `BulkRequest` / `BulkItemRequest` / `DocWriteRequest` | `server/.../action/` + `action/bulk/` |
| 分片 | `IndexShard` | `server/.../index/shard/IndexShard.java` |
| 引擎 | `Engine` / `InternalEngine` | `server/.../index/engine/` |
| 复制跟踪 | `ReplicationTracker` | `server/.../index/seqno/ReplicationTracker.java` |
| 写后动作 | `Translog` | `server/.../index/translog/Translog.java` |

---

## 3. ★ REST → IndexRequest / BulkRequest

REST 层只做「解析 + 组装」，真正的分发逻辑都在 Action 层。以单文档写入为例：

```java
// RestIndexAction（rest/action/document/RestIndexAction.java）
//   prepareRequest → 解析 _index/_type/_id、?routing=、?version=、?refresh=
//   → new IndexRequest(index, type, id).source(...)
//   → client.index(indexRequest, listener)
```

对 `_bulk`，`RestBulkAction` 使用 `BulkRequestParser`（`rest/action/document/RestBulkAction.java`
+ `action/bulk/BulkRequestParser.java`）逐行解析 NDJSON，每行生成一个
`IndexRequest` / `DeleteRequest` / `UpdateRequest`，塞进 `BulkRequest`。

**注意 6.x 的 `_type`**：`IndexRequest` 必须带 type（默认 `_doc`）。
`DocWriteRequest.OpType` 有四个取值：`INDEX` / `CREATE` / `UPDATE` / `DELETE`，
它决定后续在 primary 上执行哪条分支（`TransportShardBulkAction.executeBulkItemRequest:353`）。

---

## 4. ★★ `TransportBulkAction`：协调节点的「总调度」

### 4.1 入口 `doExecute`（`:154`）：自动建索引

```java
protected void doExecute(Task task, BulkRequest bulkRequest, ActionListener<BulkResponse> listener) {
    final long startTime = relativeTime();
    final AtomicArray<BulkItemResponse> responses = new AtomicArray<>(bulkRequest.requests.size());

    if (needToCheck()) {                                    // AutoCreateIndex 是否开启
        // Step 1: 收集请求涉及的所有索引
        final Set<String> indices = bulkRequest.requests.stream()
                // ★ delete 请求默认不触发自动建索引（除非是 external 版本）
            .filter(request -> request.opType() != DocWriteRequest.OpType.DELETE
                    || request.versionType() == VersionType.EXTERNAL
                    || request.versionType() == VersionType.EXTERNAL_GTE)
            .map(DocWriteRequest::index)
            .collect(Collectors.toSet());
        // Step 2: 过滤出「不存在且可以创建」的索引
        ...
        // Step 3: 并发创建全部缺失索引；counter 归零后再执行 bulk
        if (autoCreateIndices.isEmpty()) {
            executeIngestAndBulk(task, bulkRequest, startTime, listener, responses, indicesThatCannotBeCreated);
        } else {
            final AtomicInteger counter = new AtomicInteger(autoCreateIndices.size());
            for (String index : autoCreateIndices) {
                createIndex(index, bulkRequest.timeout(), new ActionListener<CreateIndexResponse>() {
                    public void onResponse(CreateIndexResponse result) {
                        if (counter.decrementAndGet() == 0) { executeIngestAndBulk(...); }
                    }
                    public void onFailure(Exception e) {
                        // ★ 建索引失败 → 把涉及该索引的所有 item 标记为 Failure
                        if (!(ExceptionsHelper.unwrapCause(e) instanceof ResourceAlreadyExistsException)) {
                            for (int i = 0; i < bulkRequest.requests.size(); i++) {
                                DocWriteRequest request = bulkRequest.requests.get(i);
                                if (request != null && setResponseFailureIfIndexMatches(responses, i, request, index, e)) {
                                    bulkRequest.requests.set(i, null);   // ★ 置 null，后续跳过
                                }
                            }
                        }
                        if (counter.decrementAndGet() == 0) { executeIngestAndBulk(...); }
                    }
                });
            }
        }
    } else {
        executeIngestAndBulk(task, bulkRequest, startTime, listener, responses, emptyMap());
    }
}
```

**四个关键设计点**：

| 设计点 | 含义 | 线上意义 |
| --- | --- | --- |
| `needToCheck()` | 由 `action.auto_create_index` 控制 | 关闭后写不存在索引直接失败 |
| delete 不自动建索引 | 除非 external version | 避免误删/误建 |
| `indicesThatCannotBeCreated` | 创建失败的索引集合 | 后续统一报 `IndexNotFoundException` |
| `requests.set(i, null)` | 失败 item 置 null | 后续 `doRun` 用 `null` 判断跳过，**保证响应数组下标对齐** |

> ⚠️ **重要**：`AtomicArray<BulkItemResponse> responses` 的长度 == 原始请求条数，
> 每一项**必须**有值，否则 `responses.toArray()` 会抛 NPE。
> 所以任何提前失败的 item 都会当场写入 `responses.set(i, failure)`。
> 这是排查「bulk 响应条数不对」类问题的关键。

### 4.2 `executeIngestAndBulk`（`:226`）：Ingest 前置

```java
private void executeIngestAndBulk(Task task, BulkRequest bulkRequest, long startTimeNanos,
                                  ActionListener<BulkResponse> listener, AtomicArray<BulkItemResponse> responses,
                                  Map<String, IndexNotFoundException> indicesThatCannotBeCreated) {
    boolean hasIndexRequestsWithPipelines = false;
    // 遍历请求，为每个 IndexRequest 决定 pipeline：
    //   1) 请求显式带 ?pipeline=xxx   → 用它
    //   2) 否则用索引设置 index.default_pipeline
    //   3) 都没有 → NOOP_PIPELINE_NAME（"none"）
    ...
    if (hasIndexRequestsWithPipelines) {
        if (clusterService.localNode().isIngestNode()) {
            processBulkIndexIngestRequest(task, bulkRequest, listener);   // 本地跑 ingest
        } else {
            ingestForwarder.forwardIngestRequest(BulkAction.INSTANCE, bulkRequest, listener);  // 转发到 ingest 节点
        }
    } else {
        executeBulk(task, bulkRequest, startTimeNanos, listener, responses, indicesThatCannotBeCreated);
    }
}
```

- `IngestService.NOOP_PIPELINE_NAME == "none"`。
- Ingest 节点判断：`node.ingest`（默认 true）。
- Ingest 失败或 pipeline drop 掉文档时，通过 `BulkRequestModifier.markCurrentItemAsFailed/markCurrentItemAsDropped`
  （`:666` / `:653`）在响应里补占位，**保证 `responses` 下标对齐**。

### 4.3 `BulkOperation.doRun`（`:335`）：按 shard 分组

这是 bulk 最核心的一段。分三步：

**第一步：逐条 resolve（`:342-386`）**

```java
for (int i = 0; i < bulkRequest.requests.size(); i++) {
    DocWriteRequest docWriteRequest = bulkRequest.requests.get(i);
    if (docWriteRequest == null) { continue; }              // 上一步的失败项
    if (addFailureIfIndexIsUnavailable(docWriteRequest, i, concreteIndices, metaData)) { continue; }
    Index concreteIndex = concreteIndices.resolveIfAbsent(docWriteRequest);   // ★ 别名 → 具体索引
    switch (docWriteRequest.opType()) {
        case CREATE:
        case INDEX:
            IndexRequest indexRequest = (IndexRequest) docWriteRequest;
            final IndexMetaData indexMetaData = metaData.index(concreteIndex);
            MappingMetaData mappingMd = indexMetaData.mappingOrDefault(indexRequest.type());
            Version indexCreated = indexMetaData.getCreationVersion();
            indexRequest.resolveRouting(metaData);           // _routing 解析（含 join 字段）
            indexRequest.process(indexCreated, mappingMd, concreteIndex.getName());  // 生成 id、校验 type
            break;
        case UPDATE:
            TransportUpdateAction.resolveAndValidateRouting(...);
            break;
        case DELETE:
            docWriteRequest.routing(metaData.resolveWriteIndexRouting(...));
            if (docWriteRequest.routing() == null && metaData.routingRequired(...)) {
                throw new RoutingMissingException(...);
            }
            break;
    }
}
```

- `concreteIndices.resolveIfAbsent`（`ConcreteIndices:550`）内部用 `IndexNameExpressionResolver.concreteWriteIndex`，
  会处理**别名 → 写索引**、`<index>` 表达式、以及**索引不存在**的场景。
- `IndexRequest.process` 会：为没有 id 的文档生成 id（`autoGeneratedTimestamp`）、
  校验 type 与 mapping 是否匹配、解析 `parent/routing`。
- 任何 `ElasticsearchParseException | IllegalArgumentException | RoutingMissingException`
  都会被就地转成 `BulkItemResponse.Failure` 并置 null（`:378-385`）。

**第二步：构建 `Map<ShardId, List<BulkItemRequest>>`（`:388-400`）**

```java
Map<ShardId, List<BulkItemRequest>> requestsByShard = new HashMap<>();
for (int i = 0; i < bulkRequest.requests.size(); i++) {
    DocWriteRequest request = bulkRequest.requests.get(i);
    if (request == null) { continue; }
    String concreteIndex = concreteIndices.getConcreteIndex(request.index()).getName();
    ShardId shardId = clusterService.operationRouting()
        .indexShards(clusterState, concreteIndex, request.id(), request.routing()).shardId();
    requestsByShard.computeIfAbsent(shardId, shard -> new ArrayList<>())
                    .add(new BulkItemRequest(i, request));   // ★ 保存原始下标 i
}
```

> **★ `BulkItemRequest(i, request)` 里的 `i` 是原始 bulk 下标**。
> 整个响应回填链路靠它把「分片内顺序」映射回「原始顺序」。
> 这是理解 bulk 响应的钥匙：**分片并行执行 → 用 itemId 归位**。

**第三步：每个 shard 发一个 `BulkShardRequest`（`:408-454`）**

```java
final AtomicInteger counter = new AtomicInteger(requestsByShard.size());
for (Map.Entry<ShardId, List<BulkItemRequest>> entry : requestsByShard.entrySet()) {
    final ShardId shardId = entry.getKey();
    final List<BulkItemRequest> requests = entry.getValue();
    BulkShardRequest bulkShardRequest = new BulkShardRequest(shardId, bulkRequest.getRefreshPolicy(),
            requests.toArray(new BulkItemRequest[requests.size()]));
    bulkShardRequest.waitForActiveShards(bulkRequest.waitForActiveShards());
    bulkShardRequest.timeout(bulkRequest.timeout());
    shardBulkAction.execute(bulkShardRequest, new ActionListener<BulkShardResponse>() {
        public void onResponse(BulkShardResponse bulkShardResponse) {
            for (BulkItemResponse bulkItemResponse : bulkShardResponse.getResponses()) {
                if (bulkItemResponse.getResponse() != null) {
                    bulkItemResponse.getResponse().setShardInfo(bulkShardResponse.getShardInfo());  // 回填分片信息
                }
                responses.set(bulkItemResponse.getItemId(), bulkItemResponse);   // ★ 按下标归位
            }
            if (counter.decrementAndGet() == 0) { finishHim(); }
        }
        public void onFailure(Exception e) { /* 整个分片失败 → 该分片所有 item 都 Failure */ }
    });
}
```

**为什么按 shard 分组？**
- 一次 bulk 可能横跨几十个 shard。按 shard 分组后，每个 shard 一个请求，
  分片侧可以在**一个 permit 内顺序执行**一批操作（减少锁竞争、可批量刷 translog）。
- 分组后各 shard **并行**执行，整体吞吐受最慢 shard 制约（木桶效应）。

### 4.4 重试：`BulkOperation.retry`（`:471`）

```java
void retry(Exception failure) {
    if (observer.isTimedOut()) { onFailure(failure); return; }        // 超时后不再重试
    observer.waitForNextChange(new ClusterStateObserver.Listener() {
        public void onNewClusterState(ClusterState state) { run(); }  // 集群状态变化后再跑
        public void onClusterServiceClose() { onFailure(new NodeClosedException(clusterService.localNode())); }
        public void onTimeout(TimeValue timeout) { run(); }           // 超时再试最后一次
    });
}
```

触发重试的场景（`handleBlockExceptions:457`）：集群处于 `ClusterBlockLevel.WRITE` 全局 block
（如 `cluster.blocks.read_only`、磁盘水位触发的 `read_only_allow_delete`）。
**block 是 retryable 的 → 等集群状态变化重试；不可重试 → 直接失败。**

---

## 5. ★★ `TransportShardBulkAction`：Primary 上的批量执行

### 5.1 继承关系

```
TransportReplicationAction<BulkShardRequest, BulkShardRequest, BulkShardResponse>
        ▲
TransportWriteAction<BulkShardRequest, BulkShardRequest, BulkShardResponse>
        ▲
TransportShardBulkAction                      (TransportShardBulkAction.java:79)
```

- `ACTION_NAME = BulkAction.NAME + "[s]"` → `"indices:data/write/bulk[s]"`（`:81`）。
- 构造时把线程池名指定为 `ThreadPool.Names.WRITE`（`:94`），
  所以分片写入跑在 **`write` 线程池**（默认 `write: <处理器数> / 8` 的队列型线程池）。

### 5.2 `shardOperationOnPrimary`（`:115`）：Primary 主流程

```java
protected WritePrimaryResult<BulkShardRequest, BulkShardResponse> shardOperationOnPrimary(BulkShardRequest request, IndexShard primary)
    throws Exception {
    ClusterStateObserver observer = new ClusterStateObserver(clusterService, request.timeout(), logger, threadPool.getThreadContext());
    CheckedRunnable<Exception> waitForMappingUpdate = () -> {
        PlainActionFuture<Void> waitingFuture = new PlainActionFuture<>();
        observer.waitForNextChange(new ClusterStateObserver.Listener() {
            public void onNewClusterState(ClusterState state) { waitingFuture.onResponse(null); }
            public void onClusterServiceClose() { waitingFuture.onFailure(new NodeClosedException(clusterService.localNode())); }
            public void onTimeout(TimeValue timeout) {
                waitingFuture.onFailure(new MapperException("timed out while waiting for a dynamic mapping update"));
            }
        });
        waitingFuture.get();
    };
    return performOnPrimary(request, primary, updateHelper, threadPool::absoluteTimeInMillis,
        new ConcreteMappingUpdatePerformer(), waitForMappingUpdate);
}
```

**★ `waitForMappingUpdate` 是解决「动态 mapping 竞争」的关键**：
当某个写入引入了新字段，primary 需要先把新 mapping 提交 Master 并等本节点应用，
期间如果本节点还没看到新 mapping，就**阻塞等下一个集群状态**（最多等到请求超时）。

### 5.3 `performOnPrimary`（`:154`）与 `executeBulkItemRequest`（`:167`）

```java
private static WritePrimaryResult<BulkShardRequest, BulkShardResponse> performOnPrimary(
        BulkPrimaryExecutionContext context, ...) throws Exception {
    while (context.hasMoreOperationsToExecute()) {
        executeBulkItemRequest(context, updateHelper, nowInMillisSupplier, mappingUpdater, waitForMappingUpdate);
        assert context.isInitial();     // 要么完成、要么 reset
    }
    return new WritePrimaryResult<>(context.getBulkShardRequest(), context.buildShardResponse(),
        context.getLocationToSync(), null, context.getPrimary(), logger);
}
```

**★ 串行执行**：一个 `BulkShardRequest` 里的所有 item 在 primary 上是**顺序 for 循环**执行的
（不是并行）。这就是为什么「单 bulk 里同一 shard 的多个操作」延迟会累加。

`executeBulkItemRequest` 的分支逻辑：

```
opType == UPDATE ?
   ├─ UpdateHelper.prepare → 把 update 翻译成 index/delete/noop
   │     CREATED/UPDATED → setRequestToExecute(indexRequest)
   │     DELETED        → setRequestToExecute(deleteRequest)
   │     NOOP           → markOperationAsNoOp + markAsCompleted，直接跳过
   └─ 翻译失败 → Engine.IndexResult(failure,...) 记录失败
opType == DELETE ?
   └─ executeDeleteRequestOnPrimary:470
否则（INDEX/CREATE）
   └─ executeIndexRequestOnPrimary:454

若 requiresWaitingForMappingUpdate() → waitForMappingUpdate.run() → resetForExecutionForRetry()
若 UPDATE 且版本冲突且 retryCounter < retryOnConflict → resetForExecutionForRetry()   ← ★ 冲突重试
finalizePrimaryOperationOnCompletion(:244)
```

**★ 两种「重试」**：

| 触发 | 行为 | 关键行 |
| --- | --- | --- |
| 动态 mapping 未就绪 | 等集群状态后**整个 item 重跑** | `:219-227` |
| UPDATE 版本冲突（`VersionConflictEngineException`）且 `retry_on_conflict>0` | 重新取文档再试 | `:231-239` |

### 5.4 动态 mapping 处理：`executeOnPrimaryWhileHandlingMappingUpdates`（`:482`）

```java
private static <T extends Engine.Result> void executeOnPrimaryWhileHandlingMappingUpdates(
        BulkPrimaryExecutionContext context, CheckedSupplier<T, IOException> toExecute,
        Function<Exception, T> exceptionToResult, Consumer<T> onComplete, Consumer<Mapping> mappingUpdater) throws IOException {
    T result = toExecute.get();
    if (result.getResultType() == Engine.Result.Type.MAPPING_UPDATE_REQUIRED) {
        try {
            mappingUpdater.accept(result.getRequiredMappingUpdate());      // 提交 mapping 到 Master
        } catch (Exception e) {
            onComplete.accept(exceptionToResult.apply(e));                 // mapping 失败 → 该 item 失败
            return;
        }
        result = toExecute.get();                                          // 再试一次
        if (result.getResultType() == Engine.Result.Type.MAPPING_UPDATE_REQUIRED) {
            context.markAsRequiringMappingUpdate();                        // 还是不行 → 等集群状态后重试
        } else {
            onComplete.accept(result);
        }
    } else {
        onComplete.accept(result);
    }
}
```

配套的 `ConcreteMappingUpdatePerformer`（`:513`）直接调用
`mappingUpdatedAction.updateMappingOnMaster(shardId.getIndex(), type, update)` —— 即第 12 章的
`MappingUpdatedAction`（Master 上执行 `MetaDataMappingService`）。**注意是同步阻塞等 Master ack。**

### 5.5 `shardOperationOnReplica`（`:379`）与副本执行模式

```java
public WriteReplicaResult<BulkShardRequest> shardOperationOnReplica(BulkShardRequest request, IndexShard replica) throws Exception {
    final Translog.Location location = performOnReplica(request, replica);
    return new WriteReplicaResult<>(request, location, null, replica, logger);
}

public static Translog.Location performOnReplica(BulkShardRequest request, IndexShard replica) throws Exception {
    Translog.Location location = null;
    for (int i = 0; i < request.items().length; i++) {
        BulkItemRequest item = request.items()[i];
        switch (replicaItemExecutionMode(item, i)) {   // ★ 根据 primary 结果决定副本要做什么
            case NORMAL:   // primary 成功 → 副本正常执行
                operationResult = performOpOnReplica(item.getPrimaryResponse().getResponse(), docWriteRequest, replica);
                location = syncOperationResultOrThrow(operationResult, location);
                break;
            case NOOP:     // primary 未生成 seqno 或 noop → 副本啥也不做
                break;
            case FAILURE:  // primary 失败但已分配 seqno → 副本 markSeqNoAsNoop（占位）
                operationResult = replica.markSeqNoAsNoop(failure.getSeqNo(), failure.getMessage());
                location = syncOperationResultOrThrow(operationResult, location);
                break;
        }
    }
    return location;
}
```

**★ `replicaItemExecutionMode`（`:359`）是 6.x 副本一致性的精髓**：

```
primary 响应有 failure？
   ├─ failure.seqNo != UNASSIGNED_SEQ_NO → FAILURE（复制一个「空操作」占住 seqno）
   └─ failure.seqNo == UNASSIGNED_SEQ_NO → NOOP（不复制）
primary 成功？
   ├─ result != NOOP → NORMAL（正常复制）
   └─ result == NOOP → NOOP（不复制）
```

原因：**primary 和副本的 seqno 序列必须严格一致**。如果 primary 在失败前已经分配了 seqno，
副本必须「消耗同一个 seqno」（用 `markSeqNoAsNoop` 写一条空 translog），否则两边 seqno 会错位。

`performOpOnReplica`（`:413`）里还有一段注释解释**副本上 mapping 可能仍未应用**，
此时抛 `RetryOnReplicaException`（`:447`），由复制框架重试该副本。

---

## 6. ★ `TransportReplicationAction`：主副本框架的骨架

这是整个「写入 + 复制」的公共父类，Index/Delete/Update/Bulk/ShardBulk 全部继承它。

### 6.1 `doExecute`（`:171`）→ `ReroutePhase`（`:753`）

```java
protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
    new ReroutePhase((ReplicationTask) task, request, listener).run();
}
```

`ReroutePhase.doRun`（`:776`）流程：

```java
setPhase(task, "routing");
final ClusterState state = observer.setAndGetObservedState();
final String concreteIndex = concreteIndex(state, request);
// ① block 检查
// ② 索引必须存在且 OPEN（:791/:795）
// ③ resolveRequest(indexMetaData, request) —— 补 waitForActiveShards（:188）
final ShardRouting primary = primary(state);              // :866 → shardRoutingTable.primaryShard()
if (retryIfUnavailable(state, primary)) { return; }        // primary 未 active / 节点未知 → 重试
final DiscoveryNode node = state.nodes().get(primary.currentNodeId());
if (primary.currentNodeId().equals(state.nodes().getLocalNodeId())) {
    performLocalAction(state, primary, node, indexMetaData);   // :818 本地
} else {
    performRemoteAction(state, primary, node);                 // :828 远程转发
}
```

**`ReroutePhase` 的三个重试条件**（`:850 retryIfUnavailable`）：

| 条件 | 典型触发 | 结果 |
| --- | --- | --- |
| `primary == null \|\| !primary.active()` | 分片还在 recovery | `retryBecauseUnavailable`，等分片变 active |
| primary 节点不在 `DiscoveryNodes` | 节点刚挂，路由表未更新 | 重试 |
| 远程节点 `state.version() < request.routedBasedOnClusterVersion()` | 集群状态比发送方旧 | 重试（防重定向环路） |

**`performRemoteAction` 的「两跳」设计**：协调节点先把请求发给 primary 所在节点，
primary 节点上的 `PrimaryOperationTransportHandler`（`:312`）再执行 `AsyncPrimaryAction`。
这样 primary 节点能用**最新的集群状态**做校验（`targetAllocationID` + `primaryTerm`）。

### 6.2 `AsyncPrimaryAction.doRun`（`:349`）：三代校验

```java
final IndexShard indexShard = getIndexShard(shardId);
final ShardRouting shardRouting = indexShard.routingEntry();
if (shardRouting.primary() == false) { throw new RetryOnPrimaryException(...); }   // ① 不是 primary
final String actualAllocationId = shardRouting.allocationId().getId();
if (actualAllocationId.equals(targetAllocationID) == false) { throw new ShardNotFoundException(...); }  // ② allocationId 不符
final long actualTerm = indexShard.getPendingPrimaryTerm();
if (actualTerm != primaryTerm) { throw new ShardNotFoundException(...); }          // ③ primaryTerm 不符
acquirePrimaryOperationPermit(indexShard, request, ActionListener.wrap(
    releasable -> runWithPrimaryShardReference(new PrimaryShardReference(indexShard, releasable)),
    this::onFailure));
```

**★ 三重校验（allocationId + primaryTerm）是 6.x 防「写错分片」的核心**：
- 分片可能被 relocation、可能因故障重新选主。
- 旧请求带着旧的 `allocationId`/`primaryTerm` 到达，必须被拒绝并让协调节点重试。
- 这三行是排查 `ShardNotFoundException` 的第一现场。

### 6.3 `runWithPrimaryShardReference`（`:376`）：relocation 委托

```java
if (primaryShardReference.isRelocated()) {
    primaryShardReference.close();             // 尽快释放 shard 锁
    setPhase(replicationTask, "primary_delegation");
    // 把 primary 阶段转发给 relocation target
    transportService.sendRequest(relocatingNode, transportPrimaryAction,
        new ConcreteShardRequest<>(request, primary.allocationId().getRelocationId(), primaryTerm), ...);
} else {
    setPhase(replicationTask, "primary");
    final ActionListener<Response> listener = createResponseListener(primaryShardReference);
    createReplicatedOperation(request, ActionListener.wrap(result -> result.respond(listener), listener::onFailure),
            primaryShardReference).execute();
}
```

**★ Primary 迁移（relocation）期间的写入委托**：分片正在从 A 迁到 B 时，
A 上的 `IndexShard` 标记为 relocated，新到达的写请求被**原样转发**到 B。
这是「分片迁移不中断写入」的实现（详见第 11/18 章）。

### 6.4 `createReplicatedOperation`（`:486`）与 `PrimaryResult`

```java
protected ReplicationOperation<Request, ReplicaRequest, PrimaryResult<ReplicaRequest, Response>> createReplicatedOperation(
    Request request, ActionListener<PrimaryResult<ReplicaRequest, Response>> listener, PrimaryShardReference primaryShardReference) {
    return new ReplicationOperation<>(request, primaryShardReference, listener,
            newReplicasProxy(primaryTerm), logger, actionName);
}
```

- `PrimaryShardReference`（`:1029`）实现了 `ReplicationOperation.Primary`，
  暴露 `perform`（`:1050` → `shardOperationOnPrimary`）、`localCheckpoint`、`globalCheckpoint`、
  `getReplicationGroup` 等接口给复制框架。
- `ReplicaResult`（`:539`）只是「成功或失败」的包装：
  `respond` 时若 `finalFailure == null` 回 `TransportResponse.Empty`。
- `ReplicasProxy.performOn`（`:1162`）把 `ReplicaRequest` 包成 `ConcreteReplicaRequest`
  （带 `allocationId`/`primaryTerm`/`globalCheckpoint`/`maxSeqNoOfUpdatesOrDeletes`），
  再发给副本节点。

> 复制框架 `ReplicationOperation` 的**状态机**（新副本落后怎么办、`markShardCopyAsStale`、
> 副本失败是否 failShard、`waitForActiveShards` 判定）是第 17 章的重点，本章不展开。

---

## 7. ★ `TransportWriteAction`：写操作特有的两件事

它相对 `TransportReplicationAction` 只多了两点，但都很关键：

### 7.1 全局/索引级 block 级别 = `WRITE`（`:243`/`:248`）

```java
protected ClusterBlockLevel globalBlockLevel() { return ClusterBlockLevel.WRITE; }
protected ClusterBlockLevel indexBlockLevel()  { return ClusterBlockLevel.WRITE; }
```

所以只读 block、`index.blocks.write`、磁盘水位触发的 block 都会在这里把写**拦下来重试或失败**。

### 7.2 `AsyncAfterWriteAction`（`:274`）：写后三件事

```java
AsyncAfterWriteAction(IndexShard indexShard, WriteRequest<?> request,
                      @Nullable Translog.Location location, RespondingWriteResult respond, Logger logger) {
    boolean waitUntilRefresh = false;
    switch (request.getRefreshPolicy()) {
        case IMMEDIATE:
            indexShard.refresh("refresh_flag_index");   // ★ 立即 refresh（force）
            refreshed.set(true);
            break;
        case WAIT_UNTIL:
            if (location != null) { waitUntilRefresh = true; pendingOps.incrementAndGet(); }
            break;
        case NONE:
            break;
    }
    if ((sync = indexShard.getTranslogDurability() == Translog.Durability.REQUEST && location != null)) {
        pendingOps.incrementAndGet();                   // ★ durability=request → 每次写 fsync translog
    }
}

void run() {
    indexShard.afterWriteOperation();
    maybeFinish();                                      // 计数减一
    if (waitUntilRefresh) {
        indexShard.addRefreshListener(location, forcedRefresh -> { refreshed.set(forcedRefresh); maybeFinish(); });
    }
    if (sync) {
        indexShard.sync(location, (ex) -> { syncFailure.set(ex); maybeFinish(); });
    }
}
```

| RefreshPolicy | 语义 | 成本 |
| --- | --- | --- |
| `IMMEDIATE` / `?refresh=true` | 写完立即强制 refresh（本次写入可见） | **很高**，会生成小 segment |
| `WAIT_UNTIL` / `?refresh=wait_for` | 等下一次「自然 refresh」完成（不强制） | 中，受 `refresh_interval` 影响（默认 1s） |
| `NONE`（默认） | 不等，靠 `refresh_interval` | 低 |

**`Translog.Durability`**：

| 值 | 设置 | 含义 |
| --- | --- | --- |
| `REQUEST`（默认） | `index.translog.durability=request` | **每次写请求都 fsync translog**（最多丢已 ack 的最多... 其实是不丢） |
| `ASYNC` | `index.translog.durability=async` | 由后台线程每 `sync_interval` fsync，宕机可能丢这段时间数据 |

**★ 这是「写延迟」与「数据安全」的核心权衡**，也是 `sync` 慢会导致写变慢的直接原因。

---

## 8. 单文档写入（Index/Delete/Update）与 Bulk 的关系

| 入口 | 类 | 最终落到 |
| --- | --- | --- |
| `PUT /idx/_doc/1` | `TransportIndexAction` | `TransportReplicationAction` 子类，primary 上调 `IndexShard.applyIndexOperationOnPrimary` |
| `DELETE /idx/_doc/1` | `TransportDeleteAction` | 同上，`applyDeleteOperationOnPrimary` |
| `POST /idx/_doc/1/_update` | `TransportUpdateAction` | 用 `UpdateHelper` 先 GET + 脚本，再转成 index/delete |
| `POST /_bulk` | `TransportBulkAction` | 分组 → `TransportShardBulkAction`（一个 shard 一次 RPC 批量执行） |

**关键区别**：
- 单文档写入是「一次 RPC 一个操作」，走 `shardOperationOnPrimary` 的
  **单个** `PrimaryResult`。
- Bulk 是「一次 RPC N 个操作」，走 `BulkPrimaryExecutionContext` 的**循环**，
  最终把 N 个 `BulkItemResponse` 打成一个 `BulkShardResponse`。
- 单文档写入其实就是「bulk 里只有一个 item」的特例，但**没有走 bulk 的分组逻辑**，
  因此不会有 `TransportBulkAction` 的自动建索引批处理。

**`IndexRequest.process`（`TransportBulkAction:361` 调用）做了什么**：
1. 若 `id == null`：生成 UUID 或基于时间戳的 id，并设置 `autoGeneratedTimestamp`。
2. 若 `type == null`：使用 `index.mapping.single_type`（6.x 强制单 type，默认 `_doc`）。
3. 解析 `parent`/`routing`，校验 `_routing` 是否必需（`routingRequired`）。
4. 若带 `version` 且 `versionType` 非法则抛异常。

---

## 9. 配置与设置速查

### 9.1 写入相关设置

| 设置 | 默认 | 作用 | 注意 |
| --- | --- | --- | --- |
| `action.auto_create_index` | `true` | 自动建索引开关 | 可写成 `+idx*,-*` 白名单 |
| `index.number_of_shards` | 5（6.x）→ 1（7.x） | 建索引时确定，不可改 | |
| `index.refresh_interval` | `1s` | 自然 refresh 间隔 | `-1` 关闭；越大写越快但可见性越差 |
| `index.translog.durability` | `request` | 每次请求 fsync | `async` 更快但可能丢数据 |
| `index.translog.sync_interval` | `5s` | async 下 fsync 间隔 | 仅 `async` 生效 |
| `index.write.wait_for_active_shards` | `1` | 写等待的 active shard 数 | 建索引时可设 `all` |
| `index.unassigned.node_left.delayed_timeout` | `1m` | 节点掉线后延迟重分配 | 见第 11 章 |
| `index.mapping.total_fields.limit` | `1000` | 字段数上限 | 超了报 `Limit of total fields` |
| `index.max_result_window` | `10000` | 与写入无关，读取相关 | |
| `thread_pool.write.size` | `处理器数` | write 线程池 | Bulk 执行在此 |
| `thread_pool.write.queue_size` | `200` | 队列长度 | 满了 `es_rejected_execution` |

### 9.2 写入相关 REST 参数

| 参数 | 位置 | 含义 |
| --- | --- | --- |
| `?refresh=true\|wait_for\|false` | 写请求 | 刷新策略 |
| `?wait_for_active_shards=N\|all` | 写请求 | 至少 N 个分片 active 才写 |
| `?timeout=` | 写请求 | 整个请求超时（含重试） |
| `?version=` + `?version_type=` | 写请求 | 乐观并发；`internal/external/external_gte` |
| `?if_seq_no=` + `?if_primary_term=` | 写请求 | 基于 seqno 的 CAS（6.x 新特性） |
| `?retry_on_conflict=N` | `_update` | 版本冲突自动重试次数（默认 0） |
| `?routing=` | 写请求 | 指定路由分片 |
| `?pipeline=` | 写请求 | 指定 ingest pipeline |

---

## 10. 故障排查对照表

| 现象 | 根因 | 定位方法 |
| --- | --- | --- |
| `es_rejected_execution_exception` (bulk) | `write` 线程池队列满（默认 200） | 看 `thread_pool.write.queue`；增大 size/queue 或降批量 |
| 写延迟高、`sync` 慢 | translog fsync 受磁盘 IOPS 限制（durability=request） | `index.translog.durability=async`、换 SSD/调 `sync_interval` |
| `unavailable_shards_exception` | `wait_for_active_shards` 未满足 | `GET _cluster/health?level=indices`、查 recovery 卡在哪 |
| `ClusterBlockException`（retryable） | 磁盘水位触发的 `read_only_allow_delete` | `GET _settings` 查 `index.blocks.read_only_allow_delete` |
| `mapper_parsing_exception` | 字段类型冲突 / 超 `total_fields.limit` | 查 mapping；`GET _mapping` |
| `strict_dynamic_mapping_exception` | `dynamic=strict` 时写新字段 | 调整 `dynamic` 或先加 mapping |
| `version_conflict_engine_exception` | 并发写同 id（乐观锁） | 用 `if_seq_no/if_primary_term`，或 `retry_on_conflict` |
| `ShardNotFoundException` | 写请求带旧 `allocationId`/`primaryTerm` | 通常是 relocation/选主瞬间的正常重试 |
| `IndexNotFoundException` | 索引不存在且关闭了自动创建 | `action.auto_create_index` |
| `document_missing_exception` | `_update` 的 doc 不存在且未 `upsert` | 加 `doc_as_upsert` 或 `upsert` |
| bulk 返回条数少于请求条数 | 客户端解析问题 / NDJSON 格式错误 | 检查 `_bulk` 请求体末尾换行 |
| `too_many_requests` / 429 | 写线程池拒绝或磁盘水位 | 同上 |
| mapping 更新导致的抖动 | 动态 mapping 频繁提交 Master | 提前建 mapping，或关 `dynamic` |

---

## 11. 动手实验

> 实验基于单节点（见第 01 章）。多副本实验需 ≥2 节点。

**13-1 观察一次写入的分片归属**
```bash
curl -XGET "localhost:9200/myidx/_analyze" -H 'Content-Type: application/json' -d '{"text":"hello","analyzer":"standard"}'
curl -XPUT "localhost:9200/myidx/_doc/1?routing=abc" -H 'Content-Type: application/json' -d '{"f":"v"}'
# 用 ?explain 观察分片
curl -XGET "localhost:9200/myidx/_doc/1?routing=abc"
```

**13-2 bulk 响应与 itemId 对齐**
```bash
curl -XPOST "localhost:9200/_bulk?pretty" -H 'Content-Type: application/json' -d '
{"index":{"_index":"t","_id":"1"}}
{"f":1}
{"index":{"_index":"nope","_id":"2"}}
{"f":2}
{"delete":{"_index":"t","_id":"1"}}
{"index":{"_index":"t","_id":"3"}}
{"f":3}
'
# 观察 items 数组长度是否 == 5，失败项带 error，成功项带 _shard/_seq_no/_primary_term
```

**13-3 关闭自动建索引**
```bash
curl -XPUT "localhost:9200/_cluster/settings" -H 'Content-Type: application/json' \
  -d '{"persistent":{"action.auto_create_index":false}}'
curl -XPOST "localhost:9200/_bulk" -H 'Content-Type: application/json' -d '
{"index":{"_index":"nonexistent"}}
{"f":1}
'
# → 返回 index_not_found_exception
curl -XPUT "localhost:9200/_cluster/settings" -H 'Content-Type: application/json' \
  -d '{"persistent":{"action.auto_create_index":true}}'
```

**13-4 refresh 策略对比**
```bash
curl -XPUT "localhost:9200/rp/_doc/1?refresh=true"   -H 'Content-Type: application/json' -d '{"f":1}'
curl -XPUT "localhost:9200/rp/_doc/2?refresh=wait_for" -H 'Content-Type: application/json' -d '{"f":2}'
curl -XPUT "localhost:9200/rp/_doc/3"                -H 'Content-Type: application/json' -d '{"f":3}'
# 立即查
curl -XGET "localhost:9200/rp/_count"   # doc3 可能还看不到，等 ~1s 再看
```

**13-5 版本冲突与重试**
```bash
curl -XPUT "localhost:9200/vc/_doc/1" -H 'Content-Type: application/json' -d '{"v":1}'
curl -XPUT "localhost:9200/vc/_doc/1?version=1" -H 'Content-Type: application/json' -d '{"v":2}'  # ok
curl -XPUT "localhost:9200/vc/_doc/1?version=1" -H 'Content-Type: application/json' -d '{"v":3}'  # 409
curl -XPUT "localhost:9200/vc/_doc/1?if_seq_no=0&if_primary_term=1" -H 'Content-Type: application/json' -d '{"v":4}'
```

**13-6 观察写线程池**
```bash
curl -XGET "localhost:9200/_cat/thread_pool/write?v&h=node_name,active,queue,rejected,completed"
# 压测时看 queue/rejected
```

**13-7 动态 mapping 触发**
```bash
curl -XPUT "localhost:9200/dm/_doc/1" -H 'Content-Type: application/json' -d '{"new_field":1}'
curl -XGET "localhost:9200/dm/_mapping?pretty"
# → new_field 被自动映射为 long；看是否有 mapping 更新日志
```

**13-8 wait_for_active_shards 卡住复现（需多节点/单副本）**
```bash
curl -XPUT "localhost:9200/w/1" -H 'Content-Type: application/json' \
  -d '{"settings":{"number_of_replicas":1}}'
# 停掉一个节点前，用 all 写会等待
curl -XPOST "localhost:9200/w/_doc/1?wait_for_active_shards=all" -H 'Content-Type: application/json' -d '{"f":1}'
```

**13-9 在 Debugger 里断点追踪 bulk**
在 `TransportBulkAction.doExecute:154` 和 `TransportShardBulkAction.executeBulkItemRequest:167`
打上断点，发一个 3 个 item 的 bulk，观察：
- `requestsByShard` 的 size（是否 3 个 shard）。
- `AtomicArray responses` 最后是否每个下标都有值。

**13-10 调试副本复制（双节点）**
在 `TransportReplicationAction.ReplicasProxy.performOn:1162` 打断点，
观察 `request` 里是否包含 `globalCheckpoint`/`maxSeqNoOfUpdatesOrDeletes`。

---

## 12. 自测题

1. `TransportBulkAction.doExecute` 里，为什么 delete 请求默认不触发自动建索引？
2. `AtomicArray<BulkItemResponse> responses` 的长度由什么决定？为什么失败项也要 `set`？
3. `BulkItemRequest` 里的 `int id` 有什么用途？如果丢了会怎样？
4. 为什么 bulk 要先按 `ShardId` 分组再执行？不分组会怎样？
5. `BulkOperation` 的 `retry` 在什么条件下触发？`observer.isTimedOut()` 时为什么放弃重试？
6. `TransportShardBulkAction` 继承链是什么？它跑在哪个线程池？
7. `performOnPrimary` 里的 `while (context.hasMoreOperationsToExecute())` 是并行还是串行？证据是什么？
8. `executeOnPrimaryWhileHandlingMappingUpdates` 为什么要「提交 mapping 后再试一次」，还可能再 `markAsRequiringMappingUpdate`？
9. `replicaItemExecutionMode` 三种模式各在什么情况下产生？为什么 primary 失败还要在副本上 `markSeqNoAsNoop`？
10. `AsyncPrimaryAction.doRun` 里的三重校验（primary 判断、allocationId、primaryTerm）分别防什么问题？
11. `runWithPrimaryShardReference` 中 `isRelocated()` 分支做了什么？为什么？
12. `TransportWriteAction` 相比 `TransportReplicationAction` 多了哪两点关键改动？
13. `RefreshPolicy.IMMEDIATE` 与 `WAIT_UNTIL` 的语义差异是什么？各自的成本？
14. `Durability.REQUEST` 和 `ASYNC` 对数据安全的影响？
15. `wait_for_active_shards` 在哪里被解析？默认值来自哪里？
16. `IndexRequest.process` 在 bulk 路径的哪一步被调用？它对 id 做了什么？
17. 为什么「单 bulk 内同 shard 的多个操作」延迟是累加的？
18. `RetryOnReplicaException` 在什么情况下抛出？谁负责重试？

---

## 13. 关键类速查表

| 类 | 路径 | 一句话 |
| --- | --- | --- |
| `TransportBulkAction` | `action/bulk/TransportBulkAction.java` | ★ 协调节点 bulk 调度（`doExecute:154`、`doRun:335`、分组 `:388`） |
| `TransportBulkAction.BulkOperation` | 同上 `:309` | 按 shard 分组 + block 重试 |
| `TransportBulkAction.ConcreteIndices` | 同上 `:536` | 别名/表达式 → 具体索引 |
| `TransportBulkAction.BulkRequestModifier` | 同上 `:594` | ingest 失败/丢弃后的下标对齐 |
| `BulkAction` | `action/bulk/BulkAction.java` | `NAME="indices:data/write/bulk"`（`:30`） |
| `TransportShardBulkAction` | `action/bulk/TransportShardBulkAction.java` | ★ 分片级 bulk（`:115`/`:167`/`:379`） |
| `TransportShardBulkAction.ReplicaItemExecutionMode` | 同上 `:332` | NORMAL/NOOP/FAILURE |
| `TransportReplicationAction` | `action/support/replication/TransportReplicationAction.java` | ★ 主副本框架（`ReroutePhase:753`） |
| `TransportReplicationAction.AsyncPrimaryAction` | 同上 `:329` | primary 执行 + 三重校验 |
| `TransportReplicationAction.PrimaryShardReference` | 同上 `:1029` | primary 的受控句柄 |
| `TransportReplicationAction.ReplicasProxy` | 同上 `:1153` | 副本 RPC 代理 |
| `TransportReplicationAction.PrimaryResult/ReplicaResult` | 同上 `:494`/`:539` | 结果包装 |
| `TransportWriteAction` | `action/support/replication/TransportWriteAction.java` | ★ 写操作基类（`:243` block、`:274` 写后动作） |
| `TransportWriteAction.AsyncAfterWriteAction` | 同上 `:274` | refresh/sync 后置处理 |
| `TransportWriteAction.WritePrimaryResult/WriteReplicaResult` | 同上 `:128`/`:192` | 携带 `Translog.Location` |
| `ReplicationOperation` | `action/support/replication/ReplicationOperation.java` | 副本复制状态机（第 17 章） |
| `TransportIndexAction` | `action/index/TransportIndexAction.java` | 单文档 index |
| `TransportDeleteAction` | `action/delete/TransportDeleteAction.java` | 单文档 delete |
| `TransportUpdateAction` | `action/update/TransportUpdateAction.java` | update（GET+脚本+写回） |
| `UpdateHelper` | `action/update/UpdateHelper.java` | 把 update 翻译成 index/delete/noop |
| `IndexShard` | `index/shard/IndexShard.java` | ★ 分片运行时 |
| `Engine` / `InternalEngine` | `index/engine/Engine.java` / `InternalEngine.java` | ★ 引擎（第 15 章） |
| `Translog` | `index/translog/Translog.java` | ★ 事务日志（第 14 章） |
| `ReplicationTracker` | `index/seqno/ReplicationTracker.java` | ★ 副本跟踪（第 17 章） |
| `MappingUpdatedAction` | `action/admin/indices/mapping/put/MappingUpdatedAction.java` | 动态 mapping 提交 Master |
| `BulkRequest` / `BulkItemRequest` / `BulkShardRequest` | `action/bulk/` | 请求对象 |
| `DocWriteRequest.OpType` | `action/DocWriteRequest.java` | INDEX/CREATE/UPDATE/DELETE |
| `RefreshPolicy` | `action/support/WriteRequest.java` | IMMEDIATE/WAIT_UNTIL/NONE |

---

**上一篇**：[12-索引管理与创建流程.md](./12-索引管理与创建流程.md)
**下一篇**：[14-Translog事务日志.md](./14-Translog事务日志.md)
