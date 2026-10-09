# 附录 A · 核心类速查表

> 这是整个系列的**总索引**。三张表：
> ① **按问题找类**（你遇到现象 → 该看哪几个类）；
> ② **按篇章找类**（每篇的核心类 + 路径 + 行号）；
> ③ **按主题速查**（线程池 / 断路器 / 常用设置 / 常见异常 / 常用 API）。
>
> 所有路径默认相对 `server/src/main/java/org/elasticsearch/`，
> `x-pack/` 的会显式标注。行号基于 **ES 6.6.1**。

---

## 一、按问题找类（最常用）

| 你遇到的现象 | 先看这些类 | 篇章 |
| --- | --- | --- |
| 节点起不来（配置/目录/端口） | `Node`、`Bootstrap`、`Environment`、`BootstrapChecks` | 02 |
| 请求 404 / REST 路由不对 | `RestController`、`MethodHandlers`、`PathTrie`、`BaseRestHandler` | 04 |
| 请求 400（参数不对） | `RestRequest`、`BaseRestHandler.prepareRequest` | 04 |
| 请求被拒绝 `es_rejected_execution` | `ThreadPool`、`EsAbortPolicy`、`EsThreadPoolExecutor` | 04 |
| 节点间超时 / 连不上 | `TcpTransport`、`ConnectionManager`、`TransportService`、`NodeConnectionsService` | 05 |
| 集群状态更新卡住 | `MasterService`、`TaskBatcher`、`PublishClusterStateAction`、`GET /_cluster/pending_tasks` | 09 |
| 反复选主 / 脑裂 | `ZenDiscovery`、`ElectMasterService`、`MasterFaultDetection`、`NodesFaultDetection`、`MembershipAction` | 10 |
| 分片一直 UNASSIGNED | `AllocationService`、`AllocationDeciders`、`ClusterAllocationExplain` | 11 |
| 分片超过重试次数 | `max_retry` decider → `POST /_cluster/reroute?retry_failed=true` | 11 |
| 写入慢 | `TransportBulkAction`、`IndexShard`、`RefreshListener`、`merge.policy` | 13 / 16 |
| `version_conflict_engine_exception` | `Engine.planIndexingAsPrimary`（`if_seq_no` / `if_primary_term`）、`VersionConflictEngineException` | 15 / 17 |
| 数据可见有延迟 | `Engine.refresh`、`RefreshListeners`、`index.refresh_interval` | 15 / 16 |
| 副本落后 | `ReplicationTracker`、`PrimaryReplicaSyncer`、`GlobalCheckpointSyncAction` | 17 |
| 恢复慢 | `RecoverySourceHandler`、`RecoverySettings`、`PeerRecoveryTargetService` | 18 |
| 查询慢 | `SearchService`、`QueryPhase`、`FetchPhase`、`SearchPhaseController` | 19 |
| 分片结果不一致（打分） | `DfsQueryPhase`、`SearchType`（`dfs_query_then_fetch`） | 19 |
| 聚合慢 / 结果不准 | `AggregationPhase`、`InternalTerms.doReduce`、`BucketUtils`、`shard_size` | 20 |
| `too_many_buckets_exception` | `MultiBucketConsumerService` | 20 |
| `CircuitBreakingException` | `HierarchyCircuitBreakerService`、`ChildMemoryCircuitBreaker` | 24 |
| `[script] Too many dynamic script compilations` | `ScriptService.checkCompilationLimit` | 22 |
| pipeline 不生效 / 报错 | `IngestService`、`CompoundProcessor`、`ConfigurationUtils` | 22 |
| 快照失败 / 卡住 | `SnapshotsService`、`BlobStoreRepository`、`SnapshotShardsService`、`SnapshotsInProgress` | 23 |
| 重启后索引/配置丢失 | `Gateway`、`GatewayService`、`GatewayMetaState` | 23 |
| 插件加载失败（jar hell） | `PluginsService`、`PluginInfo` | 21 |
| 鉴权失败 / 权限不足 | `Security`、`AuthenticationService`、`AuthorizationService`（`x-pack/plugin/security`） | 25 |
| DLS/FLS 后查询变慢 | `Security.onIndexModule`、`SecurityIndexSearcherWrapper`、`OptOutQueryCache` | 25 |
| ILM 卡住 | `IndexLifecycleService`、`IndexLifecycleRunner`、`pending_tasks` | 25 |
| 内存持续走高 | `HierarchyCircuitBreakerService`、`CommonStats`、`JvmGcMonitorService` | 24 |

---

## 二、按篇章找类

### 01 · 源码环境搭建与调试指南

| 项 | 说明 |
| --- | --- |
| `./gradlew :server:test --tests "..."` | 跑单个测试 |
| `./gradlew :distribution:archives:oss-zip:assemble` | 打发行包 |
| `ESSingleNodeTestCase` | ★ 单节点测试基类（`test/framework/`） |
| `ESTestCase` / `ESTokenStreamTestCase` | 单元测试基类 |
| `ESIntegTestCase` / `ESRestTestCase` | 集成测试 / REST 测试基类 |
| `BootstrapForTesting` | 测试期的安全管理器与全局初始化 |
| `RandomizedRunner` | 随机化种子（`tests.seed`） |

### 02 · 整体架构与模块全景

| 目录 | 说明 |
| --- | --- |
| `server/` | 核心（`org.elasticsearch.*`） |
| `modules/` | 官方模块（transport-netty4、lang-painless、reindex、ingest-common…） |
| `plugins/` | 官方插件（analysis-icu、repository-s3、discovery-ec2、store-smb…） |
| `x-pack/` | 商业特性（security/monitoring/watcher/ml/ilm/ccr/sql/rollup…） |
| `client/` | REST client、transport client、benchmarks |
| `libs/` | 抽出的公共库（secure-sm、x-content、nio、dissect…） |
| `test/framework` | 测试基础设施 |
| `qa/` | 跨模块集成测试 |

### 03 · 启动流程与节点生命周期

| 类 | 路径 | 一句 |
| --- | --- | --- |
| `Elasticsearch` | `bootstrap/Elasticsearch.java` | main 入口 |
| `Bootstrap` | `bootstrap/Bootstrap.java` | 引导与安全管理器 |
| `Node` | `node/Node.java` | ★ 服务编排（插件装配行号见 21 篇） |
| `Lifecycle` / `LifecycleComponent` | `common/component/` | 生命周期（`start/stop/close`） |
| `Environment` | `env/Environment.java` | 目录与配置环境 |
| `BootstrapChecks` | `bootstrap/BootstrapChecks.java` | 生产环境强校验 |
| `NodeEnvironment` | `env/NodeEnvironment.java` | 数据目录、node lock、shard 锁 |

### 04 · 基础支撑层：Settings / 模块化 / 线程池

| 类 | 路径 | 一句 |
| --- | --- | --- |
| `Settings` | `common/settings/Settings.java` | 不可变配置 |
| `Setting` | `common/settings/Setting.java` | ★ 声明式设置（`Setting.Property`） |
| `ClusterSettings` / `IndexScopedSettings` | `common/settings/` | 动态设置的注册与校验 |
| `ModulesBuilder` | `common/inject/ModulesBuilder.java` | Guice 模块装配 |
| `ThreadPool` | `threadpool/ThreadPool.java` | ★ 线程池（`:69-85` 名称常量） |
| `EsExecutors` / `EsThreadPoolExecutor` | `threadpool/` | 线程池实现 |
| `EsAbortPolicy` | `threadpool/EsAbortPolicy.java` | ★ 拒绝策略 → `es_rejected_execution` |
| `ExecutorBuilder` / `ScalingExecutorBuilder` / `FixedExecutorBuilder` | `threadpool/` | 线程池声明 |
| `ThreadContext` | `common/util/concurrent/ThreadContext.java` | ★ 跨线程上下文（安全/ARS 的基础） |
| `BigArrays` | `common/util/BigArrays.java` | 页式大数组（配合断路器） |
| `PluginsService` / `Plugin` | `plugins/` | 见 21 篇 |
| `SettingsModule` | `common/settings/SettingsModule.java` | 设置注册中心 |

**线程池（全量，`threadpool/ThreadPool.java:69-85`）**

| 名称 | 行号 | 用途 | 关键度 |
| --- | --- | --- | --- |
| `same` | `:69` | 调用者线程（不切换） | |
| `generic` | `:70` | 通用后台任务 | ★ |
| `listener` | `:71` | listener 回调 | |
| `get` | `:72` | `GET` API | |
| `analyze` | `:73` | `_analyze` | |
| `index` | `:74` | 单文档 index/delete | |
| `write` | `:75` | ★ bulk / 单文档写 / **ingest** | ★★ |
| `search` | `:76` | ★ query/fetch（分片级 + 协调） | ★★ |
| `search_throttled` | `:77` | frozen 索引的查询 | |
| `management` | `:78` | 集群管理（如 `_cat`、ILM） | ★ |
| `flush` | `:79` | flush / translog fsync | ★ |
| `refresh` | `:80` | refresh | ★ |
| `warmer` | `:81` | 预热 | |
| `snapshot` | `:82` | 快照/恢复 | ★ |
| `force_merge` | `:83` | force_merge | |
| `fetch_shard_started` | `:84` | 分片启动 | |
| `fetch_shard_store` | `:85` | 分片元数据拉取 | |

### 05 · Transport 节点间通信层

| 类 | 路径 |
| --- | --- |
| `TransportService` | `transport/TransportService.java` |
| `TcpTransport` / `TcpChannel` | `transport/` |
| `ConnectionManager` / `Transport.Connection` | `transport/` |
| `TransportRequestOptions` | `transport/TransportRequestOptions.java`（`PING`/`RECOVERY`/`BULK`/`REG`） |
| `TransportStatus`/`StreamInput`/`StreamOutput` | `common/io/stream/` |
| `Netty4Transport` | `modules/transport-netty4/.../netty4/` |
| `TransportRequestHandler` / `RequestHandlerRegistry` | `transport/` |
| `TransportResponseHandler` | `transport/` |
| `ResponseCollectorService` | `action/support/`（ARS，第 19 章） |

### 06 · REST 层与 HTTP 请求入口

| 类 | 路径 | 关键行 |
| --- | --- | --- |
| `RestController` | `rest/RestController.java` | 注册 `:151`、分发 `:168` |
| `BaseRestHandler` | `rest/BaseRestHandler.java` | 模板方法 `:78` |
| `RestRequest` | `rest/RestRequest.java` | 参数解析 + 记账 |
| `RestStatusToXContentListener` | `rest/action/RestStatusToXContentListener.java` | ★ 最常用 |
| `RestResponseListener` | `rest/action/RestResponseListener.java` | ActionListener → RestChannel |
| `HttpServerTransport` | `http/HttpServerTransport.java` | HTTP 抽象 |
| `Netty4HttpServerTransport` | `modules/transport-netty4/.../netty4/` | Netty 实现 |
| `ExceptionsHelper` | `ExceptionsHelper.java` | 异常 → HTTP 状态码 |

### 07 · Action 框架与请求处理链

| 类 | 路径 | 关键行 |
| --- | --- | --- |
| `Action` / `GenericAction` | `action/` | 动作描述符 |
| `ActionRequest` / `ActionResponse` | `action/` | 请求/响应基类 |
| `ActionListener` | `action/ActionListener.java` | 异步回调 |
| `TransportAction` | `action/support/TransportAction.java` | 入口 `:127`、过滤器链 `:148` |
| `HandledTransportAction` | `action/support/HandledTransportAction.java` | 自动注册 `:57`/`:61` |
| `ActionFilter` | `action/ActionFilter.java` | 横切（x-pack 鉴权） |
| `ActionModule` | `action/ActionModule.java` | Action `:399`/`:514`、Filter `:525`、REST `:530` |
| `NodeClient` | `client/node/NodeClient.java` | 本地执行 `:87` |
| `AbstractClient` | `client/support/AbstractClient.java` | `client.index()` 等 |
| `TaskManager` / `Task` | `tasks/` | `_tasks` API |

### 08 · ClusterState 与集群元数据

| 类 | 路径 | 行 |
| --- | --- | --- |
| `ClusterState` | `cluster/ClusterState.java` | `:84` |
| `ClusterChangedEvent` | `cluster/ClusterChangedEvent.java` | `:43` |
| `Diffable` / `Diff` / `DiffableUtils` | `cluster/` | |
| `MetaData` | `cluster/metadata/MetaData.java` | `:79` |
| `IndexMetaData` | `cluster/metadata/IndexMetaData.java` | |
| `IndexTemplateMetaData` / `AliasMetaData` / `MappingMetaData` | `cluster/metadata/` | |
| `IndexNameExpressionResolver` | `cluster/metadata/IndexNameExpressionResolver.java` | |
| `IndexGraveyard` | `cluster/metadata/IndexGraveyard.java` | |
| `RoutingTable` / `IndexRoutingTable` / `IndexShardRoutingTable` | `cluster/routing/` | |
| `ShardRouting` | `cluster/routing/ShardRouting.java` | `:46` |
| `ShardRoutingState` / `UnassignedInfo` | `cluster/routing/` | |
| `RecoverySource` | `cluster/routing/RecoverySource.java` | `:321` |
| `AllocationId` | `cluster/routing/AllocationId.java` | |
| `RoutingNodes` | `cluster/routing/RoutingNodes.java` | |
| `DiscoveryNodes` / `DiscoveryNode` | `cluster/node/` | |
| `ClusterBlocks` | `cluster/block/ClusterBlocks.java` | |

### 09 · ClusterService / MasterService / 状态发布

| 类 | 路径 | 行 |
| --- | --- | --- |
| `ClusterService` | `cluster/service/ClusterService.java` | `:47` |
| `MasterService` | `cluster/service/MasterService.java` | `:69`、`runTasks:185`、`TaskOutputs:342` |
| `ClusterApplierService` | `cluster/service/ClusterApplierService.java` | `:69`、`runTask:378` |
| `TaskBatcher` | `cluster/service/TaskBatcher.java` | `:55`、`BatchedTask:165` |
| `ClusterStateTaskExecutor` | `cluster/ClusterStateTaskExecutor.java` | |
| `ClusterStateUpdateTask` / `AckedClusterStateUpdateTask` | `cluster/` | |
| `ClusterStateApplier` / `ClusterStateListener` | `cluster/` | |
| `SourcePrioritizedRunnable` | `cluster/service/` | |
| `PublishClusterStateAction` | `discovery/zen/PublishClusterStateAction.java` | ★ `:128`/`:181`/`:510` |
| `PendingClusterStatesQueue` | `discovery/zen/` | |
| `NodeConnectionsService` | `cluster/NodeConnectionsService.java` | |

### 10 · ZenDiscovery：发现与选主

| 类 | 路径 | 行 |
| --- | --- | --- |
| `ZenDiscovery` | `discovery/zen/ZenDiscovery.java` | `:83`、`innerJoinCluster:438`、`joinElectedMaster:507`、`rejoin:988` |
| `ZenPing` / `UnicastZenPing` | `discovery/zen/` | `:45` / — |
| `UnicastHostsProvider` | `discovery/zen/` | |
| `ElectMasterService` | `discovery/zen/ElectMasterService.java` | `compare:91`、`MasterCandidate:53` |
| `NodeJoinController` | `discovery/zen/NodeJoinController.java` | `ElectionContext:219` |
| `MembershipAction` | `discovery/zen/MembershipAction.java` | |
| `MasterFaultDetection` / `NodesFaultDetection` | `discovery/zen/` | |
| `DiscoverySettings` | `discovery/DiscoverySettings.java` | `no_master_block` |

### 11 · 分片分配与 Allocation

| 类 | 路径 |
| --- | --- |
| `AllocationService` | `cluster/routing/AllocationService.java` |
| `AllocationDeciders` / `AllocationDecider` | `cluster/routing/allocation/decider/` |
| `ClusterAllocationExplain` | `cluster/routing/allocation/command/` 相关 |
| `ClusterRerouteService` | `cluster/routing/` |
| `ShardsAllocator` / `BalancedShardsAllocator` | `cluster/routing/allocation/allocator/` |
| `ShardRoutingState` | `cluster/routing/ShardRoutingState.java` |

**决策结果速查**（`GET /_cluster/allocation/explain` 的 `can_allocate`/`node_decision`）：

| 值 | 含义 | 处理 |
| --- | --- | --- |
| `yes` | 可分配 | — |
| `no` | 不可分配 | 看 `deciders` 里的 explanation |
| `throttle` | ★ 现在不行，等着 | 恢复并发已满；**正常** |
| `fetch_in_progress` | ★ 信息收集中 | `AsyncShardFetch` 未完成 |
| `no_valid_shard_copy` | ★★ **没有任何可用副本** | 节点全丢 / 不在 in-sync |
| `no_attempt` | 还没试过 | 新建索引正常 |

**按 decider 名排障**：

| decider | 典型 explanation | 处理 |
| --- | --- | --- |
| `disk_threshold` | `above the low/high watermark` | 清磁盘 / 调水位 |
| `throttling` | `too many shards are currently recovering` | 等；或调 `node_concurrent_recoveries` |
| `filter` | `node does not match index setting [...] filters` | `GET /_cat/nodeattrs?v` |
| `same_shard` | `a copy of this shard is already allocated to this node` | ★ 正常 |
| `awareness` | `maximum number of allowed shards on this node` | 检查 awareness 配置 |
| `max_retry` | `shard has exceeded the maximum number of retries [5]` | ★ `POST /_cluster/reroute?retry_failed=true` |
| `enable` | `no allocations are allowed due to [enable:none]` | 改回 `all` |
| `replica_after_primary_active` | `primary shard is not active yet` | ★ 正常 |
| `shards_limit` | `too many shards allocated on this node` | `total_shards_per_node` |

### 12 · 索引管理与创建流程

| 类 | 路径 | 行 |
| --- | --- | --- |
| `MetaDataCreateIndexService` | `cluster/metadata/` | `:207`/`:225`/`:250`/`:284` |
| `MetaDataDeleteIndexService` | `cluster/metadata/` | |
| `MetaDataIndexStateService` | `cluster/metadata/` | `:107`/`:148`（close/open） |
| `MetaDataUpdateSettingsService` | `cluster/metadata/` | |
| `MetaDataMappingService` | `cluster/metadata/` | |
| `MetaDataIndexAliasesService` | `cluster/metadata/` | |
| `MetaDataIndexTemplateService` | `cluster/metadata/` | |
| `IndicesService` | `indices/IndicesService.java` | `createIndex:453`、`checkShardLimit:1406` |
| `IndexService` | `indices/IndexService.java` | |
| `IndicesClusterStateService` | `indices/cluster/IndicesClusterStateService.java` | ★ |
| `IndexSettings` | `index/IndexSettings.java` | |
| `MapperService` / `DocumentMapper` / `FieldMapper` | `index/mapper/` | |
| `MappingUpdatedAction` | `action/admin/indices/mapping/put/` | |

### 13 · 写入链路（Index 与 Bulk）

| 类 | 路径 |
| --- | --- |
| `TransportBulkAction` | `action/bulk/TransportBulkAction.java` |
| `BulkRequest` / `BulkItemResponse` | `action/bulk/` |
| `TransportIndexAction` / `IndexRequest` | `action/index/`、`action/` |
| `TransportReplicationAction` | `action/support/replication/` |
| `ReplicationOperation` | `action/support/replication/` |
| `TransportWriteAction` | `action/support/replication/` |
| `TransportShardBulkAction` | `action/bulk/` |
| `DocumentMapperForType` | `action/support/` |

**写入相关设置**：

| 设置 | 默认 | 作用 |
| --- | --- | --- |
| `action.auto_create_index` | `true` | 自动建索引（可写 `+idx*,-*`） |
| `index.number_of_shards` | 5（6.x） | 建索引确定，不可改 |
| `index.refresh_interval` | `1s` | `-1` 关闭 |
| `index.translog.durability` | `request` | `async` 更快但可能丢数据 |
| `index.translog.sync_interval` | `5s` | 仅 `async` 生效 |
| `index.write.wait_for_active_shards` | `1` | 可设 `all` |
| `index.unassigned.node_left.delayed_timeout` | `1m` | 节点掉线延迟重分配 |
| `index.mapping.total_fields.limit` | `1000` | 字段数上限 |
| `thread_pool.write.size` / `queue_size` | 处理器数 / 200 | 满了 → `es_rejected_execution` |

**写入 REST 参数**：

| 参数 | 含义 |
| --- | --- |
| `?refresh=true\|wait_for\|false` | 刷新策略 |
| `?wait_for_active_shards=N\|all` | 至少 N 个分片 active |
| `?timeout=` | 请求超时 |
| `?version=` + `?version_type=` | 乐观并发（`internal/external/external_gte`） |
| `?if_seq_no=` + `?if_primary_term=` | ★ 基于 seqno 的 CAS |
| `?retry_on_conflict=N` | `_update` 冲突重试 |
| `?routing=` | 指定路由 |
| `?pipeline=` | 指定 ingest pipeline |

### 14 · Translog 事务日志

| 类 | 路径 |
| --- | --- |
| `Translog` | `index/translog/Translog.java` |
| `TranslogWriter` / `TragicException` | `index/translog/` |
| `TranslogReader` / `BaseTranslogReader` | `index/translog/` |
| `TranslogConfig` | `index/translog/TranslogConfig.java` |
| `Checkpoint` | `index/translog/Checkpoint.java` |
| `Translog.Operation`（`Index`/`Delete`/`NoOp`） | `index/translog/Translog.java` |
| `MultiSnapshot` | `index/translog/` |
| `BufferedChecksumStreamInput` / `TruncatedTranslogException` | `index/translog/` |
| `TranslogDeletionPolicy` | `index/translog/` |

### 15 · 存储引擎：InternalEngine 与 Lucene 集成

| 类 | 路径 |
| --- | --- |
| `Engine` | `index/engine/Engine.java` |
| `InternalEngine` | `index/engine/InternalEngine.java` |
| `EngineConfig` | `index/engine/EngineConfig.java` |
| `IndexingStrategy` / `DeletionStrategy` | `InternalEngine.java`（`planIndexingAs*`） |
| `LiveVersionMap` / `VersionValue` | `index/engine/`（★ 版本并发控制） |
| `Searcher` / `Engine.SearcherSupplier` | `index/engine/Engine.java` |
| `ReadOnlyEngine` | `index/engine/ReadOnlyEngine.java`（frozen） |
| `TranslogHandler` | `index/engine/InternalEngine.java` |
| `CombinedDeletionPolicy` | `index/engine/CombinedDeletionPolicy.java` |
| `LocalCheckpointTracker` | `index/seqno/LocalCheckpointTracker.java` |
| `SequenceNumbers` / `SeqNoStats` | `index/seqno/` |

### 16 · Refresh / Flush / Segment 合并

| 设置 | 默认 | 动态 | 作用 |
| --- | --- | --- | --- |
| `index.refresh_interval` | `1s` | ✅ | `-1` 关闭 |
| `index.max_refresh_listeners` | `1000` | ❌ | `wait_for` 最大等待者 |
| `indices.memory.index_buffer_size` | `10%` | ❌ | Lucene 索引缓冲（节点级） |
| `index.translog.flush_threshold_size` | `512mb` | ✅ | 触发 flush |
| `index.translog.durability` | `request` | ✅ | `request`/`async` |
| `index.translog.sync_interval` | `5s` | ❌ | `async` 下 fsync |
| `index.merge.scheduler.max_thread_count` | `min(4, proc/2)` | ✅ | 机械盘设 1 |
| `index.merge.scheduler.auto_throttle` | `true` | ✅ | merge IO 节流 |
| `index.merge.policy.segments_per_tier` | `10.0` | ✅ | 每层段数 |
| `index.merge.policy.max_merge_at_once` | `10` | ✅ | 单次合并段数 |
| `index.merge.policy.max_merged_segment` | `5gb` | ✅ | 单段最大 |
| `index.merge.policy.floor_segment` | `2mb` | ✅ | 小段下限 |
| `index.merge.policy.deletes_pct_allowed` | `33%` | ✅ | 删除比例阈值 |

**监控**：

| 指标 | 含义 |
| --- | --- |
| `indices.refresh.total` / `refresh_time_in_millis` | refresh 次数/耗时 |
| `indices.flush.total` / `periodic` | flush（periodic = 自动） |
| `indices.merges.current` / `total` / `total_time_in_millis` | 合并量 |
| `indices.merges.total_throttled_time_in_millis` | ★ 合并被 IO 节流时间 |
| `indices.indexing.is_throttled` / `throttle_time_in_millis` | ★ 写入被限流 |

### 17 · 副本复制与 SeqNo 一致性模型

| 类 | 路径 | 关键 |
| --- | --- | --- |
| `ReplicationTracker` | `index/seqno/ReplicationTracker.java` | ★ checkpoint 记账 |
| `SequenceNumbers` | `index/seqno/SequenceNumbers.java` | `NO_OPS_PERFORMED` 等 |
| `LocalCheckpointTracker` | `index/seqno/LocalCheckpointTracker.java` | |
| `SeqNoStats` | `index/seqno/SeqNoStats.java` | |
| `GlobalCheckpointSyncAction` | `index/seqno/GlobalCheckpointSyncAction.java` | |
| `GlobalCheckpointListeners` | `index/shard/GlobalCheckpointListeners.java` | |
| `PrimaryReplicaSyncer` | `index/shard/PrimaryReplicaSyncer.java` | |
| `TransportResyncReplicationAction` | `action/resync/` | |
| `ReplicationGroup` | `index/shard/ReplicationGroup.java` | |
| `ShardStateAction` | `cluster/action/shard/ShardStateAction.java` | |

### 18 · 分片恢复 Recovery

| 类 | 路径 | 关键 |
| --- | --- | --- |
| `RecoveryState` | `indices/recovery/RecoveryState.java` | `Stage`：`INIT→INDEX→VERIFY_INDEX→TRANSLOG→FINALIZE→DONE` |
| `RecoverySourceHandler` | `indices/recovery/RecoverySourceHandler.java` | 源端 |
| `PeerRecoveryTargetService` / `RecoveryTarget` | `indices/recovery/` | 目标端 |
| `PeerRecoverySourceService` | `indices/recovery/` | |
| `StoreRecovery` (`IndexShardStoreRecovery`) | `index/shard/StoreRecovery.java` | 本地/快照恢复 |
| `RecoverySettings` | `indices/recovery/RecoverySettings.java` | 限速/并发 |
| `Store` / `StoreFileMetaData` | `index/store/Store.java` | 文件清单 diff |

### 19 · 检索流程（QueryThenFetch / DFS）

| 类 | 路径 | 关键 |
| --- | --- | --- |
| `TransportSearchAction` | `action/search/TransportSearchAction.java` | 入口 |
| `SearchType` | `action/search/SearchType.java` | `QUERY_THEN_FETCH` / `DFS_QUERY_THEN_FETCH` |
| `AbstractSearchAsyncAction` | `action/search/AbstractSearchAsyncAction.java` | 协调 |
| `InitialSearchPhase` / `FetchSearchPhase` | `action/search/` | |
| `SearchQueryThenFetchAsyncAction` / `SearchDfsQueryThenFetchAsyncAction` | `action/search/` | |
| `SearchPhaseController` | `action/search/SearchPhaseController.java` | ★ 归约/merge |
| `CanMatchPreFilterSearchPhase` | `action/search/` | 分片裁剪 |
| `ExpandSearchPhase` | `action/search/` | field collapsing |
| `SearchService` | `search/SearchService.java` | 数据节点侧 |
| `QueryPhase` / `FetchPhase` / `DfsQueryPhase` | `search/`、`search/fetch/` | |
| `SearchTransportService` | `action/search/SearchTransportService.java` | Action 名常量 |
| `ResponseCollectorService` | `action/support/` | ARS |

### 20 · 聚合框架

| 类 | 路径 | 关键行 |
| --- | --- | --- |
| `AggregationPhase` | `search/aggregations/AggregationPhase.java` | `preProcess:50`、`execute:79`、global 单独 search `:117`、pipeline 校验 `:143` |
| `Aggregator` / `AggregatorBase` | `search/aggregations/` | `getLeafCollector:164`、`postCollection:237` |
| `BucketsAggregator` | `search/aggregations/bucket/` | `grow:67`、`collectBucket:74`、`consumeBucketsAndMaybeBreak:131` |
| `InternalAggregation` | `search/aggregations/InternalAggregation.java` | `ReduceContext:56`、`reduce:134`、`doReduce:144` |
| `MultiBucketConsumerService` | `search/aggregations/MultiBucketConsumerService.java` | `MAX_BUCKET_SETTING:44`、`accept:112`（503 `:82`） |
| `TermsAggregatorFactory` | `search/aggregations/bucket/terms/` | `doCreateInternal:113`、`subAggCollectionMode:196`、`ExecutionMode:221` |
| `InternalTerms` | `search/aggregations/bucket/terms/InternalTerms.java` | `doReduce:220` |
| `BucketUtils` | `search/aggregations/bucket/BucketUtils.java` | `suggestShardSideQueueSize:38` |
| `DeferableBucketAggregator` / `BestBucketsDeferringCollector` | `search/aggregations/bucket/` | breadth_first |

### 21 · 插件机制与扩展点

| 类 | 路径 | 关键行 |
| --- | --- | --- |
| `PluginsService` | `plugins/PluginsService.java` | `ctor:103`、`sortBundles:413`、`loadBundles:456`、`checkBundleJarHell:474`、`loadBundle:522`、`reloadLuceneSPI:560`、`loadPlugin:581`、`filterPlugins:622` |
| `PluginInfo` | `plugins/PluginInfo.java` | `ES_PLUGIN_PROPERTIES:49`、`readFromProperties:156` |
| `Plugin` | `plugins/Plugin.java` | `createComponents:129`、`additionalSettings:140`、`onIndexModule:164` |
| `IndexModule` | `index/IndexModule.java` | `addIndexEventListener:199`、`setSearcherWrapper:281`、冻结 `frozen:119` |
| `SearchModule` | `search/SearchModule.java` | `ctor:292`、`registerFetchSubPhases:703` |

### 22 · Ingest 管道与 Script 脚本

| 类 | 路径 | 关键行 |
| --- | --- | --- |
| `IngestService` | `ingest/IngestService.java` | `applyClusterState:256`、`executeBulkRequest:377`、`innerExecute:455`、`innerUpdatePipelines:498` |
| `IngestMetadata` | `ingest/IngestMetadata.java` | `TYPE:45` |
| `Pipeline` / `CompoundProcessor` | `ingest/` | `Pipeline.create:68`；`CompoundProcessor.execute:117` |
| `IngestDocument` | `ingest/IngestDocument.java` | `setFieldValue:415`、`renderTemplate:557`、`MetaData:687` |
| `ConfigurationUtils` | `ingest/ConfigurationUtils.java` | `readStringProperty:72`、`compileTemplate:331` |
| `ScriptService` | `script/ScriptService.java` | `compile:283`、`checkCompilationLimit:390`、`CacheKey:340/575` |
| `ScriptModule` | `script/ScriptModule.java` | `CORE_CONTEXTS:40`、`ctor:68` |
| `ScriptType` | `script/ScriptType.java` | `INLINE:44`、`STORED:53` |
| `ScriptContext` | `script/ScriptContext.java` | `:57` |

### 23 · Snapshot 与 Gateway 持久化

| 类 | 路径 | 关键行 |
| --- | --- | --- |
| `MetaDataStateFormat` | `gateway/MetaDataStateFormat.java` | `FORMAT(SMILE):65`、`write:97`（ATOMIC_MOVE `:133`）、`loadLatestState:257` |
| `GatewayMetaState` | `gateway/GatewayMetaState.java` | `applyClusterState:118`、`getRelevantIndices:189` |
| `GatewayService` | `gateway/GatewayService.java` | `STATE_NOT_RECOVERED_BLOCK:70`、`clusterChanged:147` |
| `Gateway` | `gateway/Gateway.java` | `performStateRecovery:61`、`requiredAllocation:67`、失败 `:94` |
| `DanglingIndicesState` | `gateway/` | 悬挂索引 |
| `BlobStoreRepository` | `repositories/blobstore/BlobStoreRepository.java` | `initializeSnapshot:387`、`deleteSnapshot:418`、`finalizeSnapshot:538`、`snapshotShard:851` |
| `RepositoryData` | `repositories/RepositoryData.java` | `getGenId:107`、`addSnapshot:157` |
| `SnapshotsService` | `snapshots/SnapshotsService.java` | `createSnapshot:234` |
| `SnapshotShardsService` | `snapshots/SnapshotShardsService.java` | `snapshot:390` |
| `RestoreService` | `snapshots/RestoreService.java` | `restoreSnapshot:175` |
| `SnapshotsInProgress` | `cluster/SnapshotsInProgress.java` | `TYPE:49` |

### 24 · 监控 Stats 与内存保护

| 类 | 路径 | 关键行 |
| --- | --- | --- |
| `NodeStats` | `action/admin/cluster/node/stats/NodeStats.java` | `:51`、`getBreaker:190` |
| `NodeService` | `node/NodeService.java` | `stats:103`（breaker `:117`） |
| `CommonStats` | `action/admin/indices/stats/CommonStats.java` | 17 类指标 `:58-103` |
| `MonitorService` | `monitor/MonitorService.java` | `:35` |
| `JvmGcMonitorService` | `monitor/jvm/JvmGcMonitorService.java` | `GC_OVERHEAD_WARN:64`(50)、`INFO:66`(25)、`DEBUG:68`(10) |
| `CircuitBreaker` | `common/breaker/CircuitBreaker.java` | `FIELDDATA:43`、`REQUEST:51`、`IN_FLIGHT_REQUESTS:56`、`ACCOUNTING:62` |
| `HierarchyCircuitBreakerService` | `indices/breaker/HierarchyCircuitBreakerService.java` | `TOTAL:52`(70%)、`stats:215`、`checkParentLimit:240`、`registerBreaker:271` |
| `ChildMemoryCircuitBreaker` | `common/breaker/ChildMemoryCircuitBreaker.java` | `addEstimateBytesAndMaybeBreak:110`、`limit:149` |
| `CircuitBreakingException` | `common/breaker/CircuitBreakingException.java` | `status():71` → **503** |

**断路器默认值**：

| 设置 | 默认 | overhead |
| --- | --- | --- |
| `indices.breaker.total.limit`（PARENT） | 70% heap | 1.0 |
| `indices.breaker.request.limit` | 60% heap | 1.0 |
| `indices.breaker.fielddata.limit` | 60% heap | 1.03 |
| `indices.breaker.accounting.limit` | 100% heap | 1.0 |
| `network.breaker.inflight_requests.limit` | 100% heap | 1.0 |

### 25 · XPack 与商业特性

| 类 | 路径（`x-pack/`） | 关键 |
| --- | --- | --- |
| `XPackPlugin` | `plugin/core/.../core/XPackPlugin.java` | `:89`、`createComponents:248`、`onIndexModule:385` |
| `XPackLicenseState` | `plugin/core/.../license/XPackLicenseState.java` | `isDocumentAndFieldLevelSecurityAllowed:395`、`isIndexLifecycleAllowed:617`、`isCcrAllowed:704` 等 24 个开关 |
| `Security` | `plugin/security/.../security/Security.java` | `:261`、`onIndexModule:695`、`getRestHandlerWrapper:1022`、`getFieldFilter:1077` |
| `SecurityIndexSearcherWrapper` / `DocumentSubsetReader` | security / core | ★ DLS |
| `FieldPermissions` / `FieldPermissionsCache` | `plugin/core/.../core/security/authz/permission/` | ★ FLS |
| `OptOutQueryCache` | `plugin/core/.../core/security/authz/accesscontrol/` | FLS 时禁用缓存 |
| `IndexLifecycleService` / `IndexLifecycleRunner` | `plugin/ilm/.../indexlifecycle/` | ILM |
| `ShardFollowTasksExecutor` / `CcrRepository` | `plugin/ccr/.../ccr/` | CCR |
| `SqlPlugin` | `plugin/sql/.../sql/plugin/` | SQL |
| `MachineLearning` | `plugin/ml/.../ml/` | ML |
| `Monitoring` / `Watcher` | `plugin/monitoring/.../`、`plugin/watcher/.../` | 监控 / 告警 |

---

## 三、按主题速查

### 3.1 常见异常 → 源码定位

| 异常 / 报错 | 位置 | 篇章 |
| --- | --- | --- |
| `es_rejected_execution` / `EsRejectedExecutionException` | `ThreadPool` 的 `EsAbortPolicy` | 04 |
| `ClusterBlockException: state not recovered / initialized` | `GatewayService.java:70/158` | 23 |
| `found [N] metadata states, required [M]` | `Gateway.java:94` | 23 |
| `master_not_discovered_exception` | `DiscoverySettings.no_master_block` | 10 |
| `NoShardAvailableActionException` | 分片不可用 | 11 |
| `shard has exceeded the maximum number of retries [5]` | `max_retry` decider | 11 |
| `version_conflict_engine_exception` | `Engine.planIndexingAsPrimary` | 15/17 |
| `TooManyBucketsException` | `MultiBucketConsumerService.java:124`（**503**） | 20 |
| `CircuitBreakingException: Data too large` | `ChildMemoryCircuitBreaker:91` / `checkParentLimit:240`（**503**） | 24 |
| `[script] Too many dynamic script compilations within` | `ScriptService.java:407` | 22 |
| `pipeline with id [x] does not exist` | `IngestService.java:400` | 22 |
| `Merging/Reducing the aggregations failed ... two different types` | `InternalTerms.java:236` | 20 |
| `jar hell! duplicate codebases ...` | `PluginsService.java:511/518` | 21 |
| `TooManyClauses` / `query_shard_exception` | Lucene / `QueryShardContext` | 19 |
| `search_phase_execution_exception` | `AbstractSearchAsyncAction.onPhaseFailure` | 19 |

### 3.2 常用观测 API

```bash
# 健康与状态
GET /_cluster/health?pretty
GET /_cluster/state?pretty
GET /_cluster/pending_tasks
GET /_cluster/allocation/explain            # ★ 分片不分配必用
GET /_cat/nodes?v
GET /_cat/shards?v | grep UNASSIGNED
GET /_cat/allocation?v
GET /_cat/segments?v

# 性能
GET /_nodes/stats?pretty                    # 全量（重）
GET /_nodes/stats/breaker?pretty            # 断路器
GET /_nodes/stats/jvm?pretty                # GC/堆
GET /_nodes/stats/thread_pool               # rejected
GET /_nodes/stats/indices/segments,fielddata?pretty
GET /_cat/thread_pool?v
GET /_nodes/hot_threads?threads=10
GET /_tasks?detailed=true

# 数据路径
GET /<index>/_stats?pretty
GET /<index>/_segments?pretty
GET /<index>/_recovery?pretty
GET /<index>/_ilm/explain                   # ILM

# 安全与许可
GET /_security/_authenticate
GET /_xpack
GET /_xpack/usage
GET /_license
```

### 3.3 高频调优项（云服务 checklist）

```yaml
# 稳定性
discovery.zen.minimum_master_nodes: (master/2)+1
gateway.recover_after_master_nodes: (master/2)+1
cluster.routing.allocation.disk.watermark.low/high/flood_stage: 85%/90%/95%

# 内存
indices.fielddata.cache.size: 20%           # ★ 默认无限制，强烈建议设
indices.breaker.total.limit: 70%
search.max_buckets: 65536
index.max_result_window: 10000

# 写入
index.refresh_interval: 30s（日志场景）
index.translog.durability: async（能接受秒级丢失时）
thread_pool.bulk.queue_size: 适当加大

# 恢复
indices.recovery.max_bytes_per_sec: 100mb
cluster.routing.allocation.node_concurrent_recoveries: 4

# 快照
path.repo: [/mnt/backup]                    # ★ 必须写在 yml
```

### 3.4 全系列文档导航

| 篇 | 文档 |
| --- | --- |
| 01 | [01-源码环境搭建与调试指南.md](./01-源码环境搭建与调试指南.md) |
| 02 | [02-整体架构与模块全景.md](./02-整体架构与模块全景.md) |
| 03 | [03-启动流程与节点生命周期.md](./03-启动流程与节点生命周期.md) |
| 04 | [04-基础支撑层-Settings与模块化与线程池.md](./04-基础支撑层-Settings与模块化与线程池.md) |
| 05 | [05-Transport节点间通信层.md](./05-Transport节点间通信层.md) |
| 06 | [06-REST层与HTTP请求入口.md](./06-REST层与HTTP请求入口.md) |
| 07 | [07-Action框架与请求处理链.md](./07-Action框架与请求处理链.md) |
| 08 | [08-ClusterState与集群元数据.md](./08-ClusterState与集群元数据.md) |
| 09 | [09-ClusterService与MasterService集群状态发布.md](./09-ClusterService与MasterService集群状态发布.md) |
| 10 | [10-ZenDiscovery选主与节点发现.md](./10-ZenDiscovery选主与节点发现.md) |
| 11 | [11-分片分配与Allocation.md](./11-分片分配与Allocation.md) |
| 12 | [12-索引管理与创建流程.md](./12-索引管理与创建流程.md) |
| 13 | [13-写入链路（Index与Bulk）.md](./13-写入链路（Index与Bulk）.md) |
| 14 | [14-Translog事务日志.md](./14-Translog事务日志.md) |
| 15 | [15-存储引擎-InternalEngine与Lucene集成.md](./15-存储引擎-InternalEngine与Lucene集成.md) |
| 16 | [16-RefreshFlush与Segment合并.md](./16-RefreshFlush与Segment合并.md) |
| 17 | [17-副本复制与SeqNo一致性模型.md](./17-副本复制与SeqNo一致性模型.md) |
| 18 | [18-分片恢复Recovery.md](./18-分片恢复Recovery.md) |
| 19 | [19-检索流程-QueryThenFetch与DFS.md](./19-检索流程-QueryThenFetch与DFS.md) |
| 20 | [20-聚合框架Aggregations.md](./20-聚合框架Aggregations.md) |
| 21 | [21-插件机制与扩展点.md](./21-插件机制与扩展点.md) |
| 22 | [22-Ingest管道与Script脚本.md](./22-Ingest管道与Script脚本.md) |
| 23 | [23-Snapshot与Gateway持久化.md](./23-Snapshot与Gateway持久化.md) |
| 24 | [24-监控Stats与内存保护.md](./24-监控Stats与内存保护.md) |
| 25 | [25-XPack与商业特性源码结构.md](./25-XPack与商业特性源码结构.md) |
| A | 本文件 |

---

**返回**：[README.md](./README.md)
