# Elasticsearch 6.6.1 源码分析笔记（导航索引）

> 面向 **Elasticsearch 云服务开发工程师** 的源码学习资料。
> 目标：让你在 8~12 周内，从「能看懂目录」到「能定位线上问题、能改源码、能写插件」。

---

## 0. 本套文档的坐标

| 项目 | 取值 | 说明 |
| --- | --- | --- |
| 源码版本 | `6.6.1` | `server/src/main/java/org/elasticsearch/Version.java:206` → `CURRENT = V_6_6_1` |
| Wire 协议版本号 | `6060199` | `Version.java:203` `V_6_6_1_ID` |
| Lucene | `7.6.0` | `buildSrc/version.properties:2`，`Version.java:204` `LUCENE_7_6_0` |
| Netty | `4.1.32.Final` | `buildSrc/version.properties:18` |
| Jackson | `2.8.11` | `buildSrc/version.properties:7` |
| Log4j2 | `2.11.1` | `buildSrc/version.properties:12` |
| Joda-Time | `2.10.1` | `buildSrc/version.properties:19` |
| Gradle | `5.1.1` | `gradle/wrapper/gradle-wrapper.properties:3` |
| JDK | 8+（编译目标 1.8） | `Node.java:315` 打印的 JVM 信息可佐证 |

> **为什么强调 6.6.1**：6.x 与 7.x 在源码层面差异巨大。
> 例如 6.6.1 **仍然使用 Guice 做依赖注入**（`common/inject`，见 `Node.java:603` `modules.createInjector()`），
> 而 7.x 已改为手写 `BootstrapChecks` + 大量工厂方法；
> 6.6.1 的选主仍然是 **ZenDiscovery**（`discovery/zen`），而不是 7.x 的 `CoordinationService`。
> 如果你照着 7.x 的博客读 6.6.1 的代码，会处处对不上。

---

## 1. 文档清单

按推荐阅读顺序排列。**带 ★ 的是必读**。

| # | 文档 | 主题 | 对应阶段 |
| --- | --- | --- | --- |
| 00 | [学习路线.md](00学习路线.md) ★ | 全局学习路线、阶段划分、时间表、自测题 | 全程 |
| 01 | [01-源码环境搭建与调试指南.md](./01-源码环境搭建与调试指南.md) ★ | 环境、导 IDE、单节点启动、断点、日志、测试框架 | 阶段 0 |
| 02 | [02-整体架构与模块全景.md](./02-整体架构与模块全景.md) ★ | 三条主线、目录地图、模块依赖、请求全景图 | 阶段 0/1 |
| 03 | [03-启动流程与节点生命周期.md](./03-启动流程与节点生命周期.md) ★ | Bootstrap → Node 构造 → Node.start() → 关闭 | 阶段 1 |
| 04 | [04-基础支撑层-Settings与模块化与线程池.md](./04-基础支撑层-Settings与模块化与线程池.md) ★ | Settings、Guice Modules、ThreadPool、通用工具类 | 阶段 1 |
| 05 | [05-Transport节点间通信层.md](./05-Transport节点间通信层.md) ★ | TcpTransport/Netty4、TransportService、序列化协议 | 阶段 1 |
| 06 | [06-REST层与HTTP请求入口.md](./06-REST层与HTTP请求入口.md) ★ | Netty4HttpServerTransport、RestController、RestHandler | 阶段 1 |
| 07 | [07-Action框架与请求处理链.md](./07-Action框架与请求处理链.md) ★ | ActionModule、TransportAction、HandledTransportAction、线程池切换 | 阶段 1 |
| 08 | [08-ClusterState与集群元数据.md](./08-ClusterState与集群元数据.md) ★ | ClusterState、MetaData、IndexMetaData、RoutingTable、Diff | 阶段 2 |
| 09 | [09-ClusterService与MasterService集群状态发布.md](./09-ClusterService与MasterService集群状态发布.md) ★ | 两阶段提交、ClusterStateUpdateTask、Ack 机制 | 阶段 2 |
| 10 | [10-ZenDiscovery选主与节点发现.md](./10-ZenDiscovery选主与节点发现.md) ★ | ZenDiscovery、Ping、MasterFaultDetection、Join | 阶段 2 |
| 11 | [11-分片分配与Allocation.md](./11-分片分配与Allocation.md) ★ | AllocationService、Decider 链、ShardsAllocator | 阶段 2 |
| 12 | [12-索引管理与创建流程.md](./12-索引管理与创建流程.md) ★ | MetaDataCreateIndexService、模板/别名、MapperService、close/open、shrink/split/rollover | 阶段 3 |
| 13 | [13-写入链路（Index与Bulk）.md](./13-写入链路（Index与Bulk）.md) ★ | REST→Bulk→ReplicationAction→ShardBulk→Engine | 阶段 3 |
| 14 | [14-Translog事务日志.md](./14-Translog事务日志.md) ★ | Translog、Checkpoint、Durability、恢复语义 | 阶段 3 |
| 15 | [15-存储引擎-InternalEngine与Lucene集成.md](./15-存储引擎-InternalEngine与Lucene集成.md) ★ | Engine 接口、InternalEngine、版本并发控制、LiveVersionMap | 阶段 3 |
| 16 | [16-RefreshFlush与Segment合并.md](./16-RefreshFlush与Segment合并.md) ★ | RefreshListeners、Flush、MergePolicy、段合并 | 阶段 3 |
| 17 | [17-副本复制与SeqNo一致性模型.md](./17-副本复制与SeqNo一致性模型.md) ★ | ReplicationTracker、LocalCheckpointTracker、GlobalCheckpoint | 阶段 3 |
| 18 | [18-分片恢复Recovery.md](./18-分片恢复Recovery.md) ✅ | PeerRecovery、StoreRecovery、phase1/phase2 | 阶段 3 |
| 19 | [19-检索流程-QueryThenFetch与DFS.md](./19-检索流程-QueryThenFetch与DFS.md) ★ | TransportSearchAction、AsyncAction、Query/Fetch Phase | 阶段 4 |
| 20 | [20-聚合框架Aggregations.md](./20-聚合框架Aggregations.md) ✅ | Aggregator、Bucket/Metric、reduce、pipeline agg | 阶段 4 |
| 21 | [21-插件机制与扩展点.md](./21-插件机制与扩展点.md) ★ | Plugin、PluginsService、各 Module、自定义扩展点 | 阶段 5 |
| 22 | [22-Ingest管道与Script脚本.md](./22-Ingest管道与Script脚本.md) ✅ | IngestService、Processor、Painless | 阶段 5 |
| 23 | [23-Snapshot与Gateway持久化.md](./23-Snapshot与Gateway持久化.md) ✅ | GatewayMetaState、BlobStoreRepository、快照/恢复 | 阶段 5 |
| 24 | [24-监控Stats与内存保护.md](./24-监控Stats与内存保护.md) ✅ | MonitorService、Stats API、CircuitBreaker、Cache | 阶段 5 |
| 25 | [25-XPack与商业特性源码结构.md](./25-XPack与商业特性源码结构.md) | X-Pack 子模块、Security（DLS/FLS）、CCR、ILM、SQL、ML、Watcher、Monitoring | 阶段 5 |
| A | [附录A-核心类速查表.md](./附录A-核心类速查表.md) ★ | 类名 → 路径 → 职责 → 行号，随查随用 | 全程 |

---

## 2. 三条主线（先建立心智模型，再读细节）

读 ES 源码最大的陷阱是「按目录顺序读」，读到一半就迷路。正确的做法是**抓住三条主线**，其它所有代码都是这三条主线的支撑：

```
主线一：集群状态线（元数据）
  ZenDiscovery 选主 → MasterService 串行执行 Task → 生成新 ClusterState
  → ClusterStatePublisher 广播到所有节点 → ClusterApplierService 应用
  核心类：ClusterState / MetaData / RoutingTable / ClusterStateUpdateTask

主线二：写入线（数据）
  REST → TransportBulkAction → TransportReplicationAction(PrimaryPhase)
  → TransportShardBulkAction → IndexShard → InternalEngine → Lucene + Translog
  → ReplicaPhase 并发写副本 → 返回
  核心类：TransportBulkAction / TransportShardBulkAction / IndexShard / InternalEngine / Translog

主线三：读取线（查询）
  REST → TransportSearchAction → 解析并路由到 N 个分片
  → QueryPhase(打分/排序/取 docId 与 sort values) → 汇总(SearchPhaseController)
  → FetchPhase(取 _source / stored fields) → 返回
  核心类：TransportSearchAction / AbstractSearchAsyncAction / QueryPhase / FetchPhase
```

**读源码的顺序建议**：先「集群状态线」→ 再「写入线」→ 最后「读取线」。
原因：写入线和读取线都依赖 ClusterState 提供路由信息（哪个 shard 在哪个节点），
不先搞懂 ClusterState 的 `RoutingTable`/`RoutingNodes`，看分片路由代码会非常痛苦。

---

## 3. 如何使用这套文档

1. **不要只读文档**。每篇文档都给了「必读源码清单」，请打开 IDE 对照阅读。
2. **每篇文档末尾都有「动手实验」和「自测题」**。实验必须在本地跑一遍，否则记忆留存率低于 20%。
3. **遇到看不懂的类，先查 [附录A-核心类速查表.md](./附录A-核心类速查表.md)**，定位到路径再进去读。
4. **所有引用格式约定**：
   - 源码路径写法：`server/src/main/java/org/elasticsearch/node/Node.java`（相对仓库根目录）
   - 行号写法：`Node.java:685`（指该文件的第 685 行，6.6.1 版本）
   - 行号可能因你本地做过修改而漂移，**以类名和方法名为准，行号仅作导航锚点**。

---

## 4. 学习产出物清单（建议）

学完每个阶段，产出一份属于自己的笔记，建议至少包含：

- [ ] 一张时序图（用 PlantUML 或手绘）：例如「一次 bulk 写入的完整时序」
- [ ] 一张类图：例如「Engine 接口体系」
- [ ] 一份「源码疑问清单」：读不懂的地方记下来，下一阶段回头再看
- [ ] 至少一次源码级改造：例如给 `RestController` 加一个自定义 endpoint（参考 `plugins/examples/rest-handler`）

---

## 5. 快速跳转：常用源码顶层目录

```
server/src/main/java/org/elasticsearch/
├── bootstrap/        启动引导、安全检查、Native 调用（20 个文件）
├── node/             Node 生命周期、模块装配  ← 从这里出发
├── common/           基础设施：settings/io.stream/xcontent/inject/threadpool（483 个文件）
├── transport/        节点间 RPC 通信
├── http/             HTTP 服务端抽象（实现在 modules/transport-netty4）
├── rest/             REST 层：RestController、RestHandler、BaseRestHandler
├── action/           所有业务 Action（642 个文件，最大目录）
│   └── support/replication/  ← 主副本复制框架
├── cluster/          集群状态、元数据、路由、分配（160 个文件）
├── discovery/        发现与选主（zen）
├── indices/          IndicesService、恢复、字段数据、熔断
├── index/            单个索引/分片的一切：engine/translog/mapper/analysis/store（432 个文件）
├── search/           查询执行、聚合、排序、打分（624 个文件，第二大目录）
├── gateway/          元数据磁盘持久化、恢复
├── repositories/     快照仓库
├── snapshots/        快照服务
├── plugins/          插件框架
├── script/           脚本引擎框架
├── ingest/           Ingest 管道
├── monitor/          监控与 Stats
└── tasks/            任务管理（TaskManager）

modules/              官方"模块"（默认内置）：transport-netty4、analysis-common、lang-painless、
                      reindex、parent-join、percolator、aggs-matrix-stats ...
plugins/              官方"插件"（需手动安装）：analysis-icu、repository-s3、discovery-ec2 ...
x-pack/plugin/        商业功能：security、watcher、ml、ccr、ilm、sql、rollup、graph、monitoring
libs/                 独立公共库：elasticsearch-cli、ssl-config、secure-sm、grok、log4j ...
test/framework/       测试框架：ESIntegTestCase、InternalTestCluster、ESTestCase
```

---

## 6. 一句话记住 6.6.1 的关键结论

- **6.6.1 = Lucene 7.6 的分布式外壳**：ES 自己不实现倒排索引、不实现段合并，一切存储/检索能力来自 Lucene，ES 负责分片、副本、一致性、路由、恢复、聚合归并。
- **6.6.1 已经有完整的 seqno-based 复制**：`ReplicationTracker`（`index/seqno/ReplicationTracker.java`，约 47KB）已经包含 local/global checkpoint、in-sync allocation 跟踪、primary relocation handoff。
- **6.6.1 仍然用 Guice**：`Node.java:603` → `ModulesBuilder.createInjector()`，与 7.x 完全不同。
- **6.6.1 仍然用 ZenDiscovery**：`discovery/zen/ZenDiscovery.java`，ping 超时 `discovery.zen.fd.*`。

---

下一步：请打开 [学习路线.md](00学习路线.md)，先花 30 分钟看完学习路线，再按阶段开工。
