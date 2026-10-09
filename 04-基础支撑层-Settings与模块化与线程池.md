# 04 · 基础支撑层：Settings、依赖注入、线程池、通用工具

> 目标：搞懂 ES 的「地基」。这些代码本身不处理业务，但你在任何业务模块都会撞见它们。
> **不先搞懂这一章，后面每一章都会浪费大量时间在「这行代码什么意思」上。**

---

## 1. Settings：ES 的配置体系

### 1.1 三个层次

ES 的配置有严格的三层结构：

| 层 | 类 | 存储位置 | 能否动态修改 | 例子 |
| --- | --- | --- | --- | --- |
| **节点静态配置** | `Settings` | `elasticsearch.yml` + `-Dxxx` | ❌ | `path.data`、`node.name`、`network.host` |
| **集群动态配置** | `ClusterSettings` | ClusterState 的 `customs` 里 | ✅（`_cluster/settings`） | `cluster.routing.allocation.enable` |
| **索引级配置** | `IndexScopedSettings` | `IndexMetaData.settings` 里 | ✅（`_settings` API） | `index.number_of_replicas`、`index.refresh_interval` |

对应源码：

```
server/src/main/java/org/elasticsearch/common/settings/
├── Settings.java               ★ 不可变的配置容器（扁平 key-value）
├── Setting.java                ★ 单个配置项的元数据（默认值、类型、校验、Property）
├── AbstractScopedSettings.java ★ 管理"哪些配置项是合法的"以及动态更新
│   ├── ClusterSettings.java     集群级
│   └── IndexScopedSettings.java 索引级
├── SettingsModule.java         把上面三者注册进 Guice
├── SettingsFilter.java         过滤敏感配置（不返回给 REST）
├── SecureSetting.java          从 keystore 读取的配置
├── KeyStoreWrapper.java        elasticsearch.keystore 的实现
└── PropertyPlaceholder.java    ${...} 占位符替换
```

### 1.2 `Settings`（`Settings.java:84`）

**核心设计：不可变 + 扁平化**。

```java
public final class Settings implements ToXContentFragment {
    private final Map<String, String> settings;      // ★ 扁平化：所有 key 都是 "a.b.c" 形式
    ...
    public static final Settings EMPTY = new Settings(Collections.emptyMap());   // :86
}
```

**为什么是扁平 Map 而不是树？** 因为读写模式是「按完整 key 查」，扁平化让 `get("a.b.c")` 是 O(1)。

**从 YAML 到扁平 Map**（`Settings.Builder.loadFromSource` / `loadFromMap` 递归展开）：

```yaml
# elasticsearch.yml
cluster:
  routing:
    allocation:
      enable: all
```
```java
// 变成
{"cluster.routing.allocation.enable": "all"}
```

**常用 API**：

```java
Settings settings = Settings.builder()                  // :829 class Builder
        .put("a.b.c", 1)
        .put("a.b.d", true)
        .build();                                        // :1323 build()

settings.get("a.b.c");                                   // "1"
settings.getAsInt("a.b.c", 0);                           // 1
settings.getAsBoolean("a.b.d", false);                   // true
```

| API | 作用 | ES 里用在哪 |
| --- | --- | --- |
| `getAsGroups()` | 按「第一层前缀」分组为 `Map<String, Settings>` | 解析 `index.routing.allocation.require.*`、`discovery.zen.ping.unicast.hosts` |
| `getByPrefix(prefix)` | 去掉前缀，返回子 `Settings` | 插件只关心 `myplugin.*` |
| `filter(...)` | 只保留匹配的 key | `SettingsFilter` |
| `toXContent(builder, params)` | 序列化为 JSON | `_cluster/settings` 响应 |

### 1.3 `Setting<T>`——配置项的"元数据"

ES 里每一个配置项都是 `Setting` 的实例：

```java
// 真实风格（以 ClusterService 为例）
public static final Setting<TimeValue> CLUSTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING =
    Setting.positiveTimeSetting("cluster.service.slow_task_logging_threshold",
        TimeValue.timeValueSeconds(30), Property.Dynamic, Property.NodeScope);
```

| 信息 | 通过什么声明 | 作用 |
| --- | --- | --- |
| **类型与默认值** | `Setting.intSetting(...)` / `timeSetting(...)` | 类型安全 |
| **校验器** | `new Setting.SimpleKey(..., validator)` | 值域检查 |
| **作用域** | `Property.NodeScope` / `Property.IndexScope` / `Property.Dynamic` | ★ 决定能不能动态改 |
| **废弃信息** | `Property.Deprecated`、`SettingUpgrader` | 兼容老配置 |

**`Property` 枚举是理解配置体系的关键**：

| Property | 含义 | 影响 |
| --- | --- | --- |
| `NodeScope` | 节点级（写 `elasticsearch.yml`） | 不能放到索引上 |
| `IndexScope` | 索引级（写 `_settings`） | 不能放到集群上 |
| `Dynamic` | ★ **可以动态更新** | 没有它，API 改配置会报 `non-updatable setting` |
| `Final` | 不可覆盖（只能由 ES 内部设置） | |
| `Deprecated` | 已废弃 | 使用时会打 deprecation 日志 |
| `Filtered` | 不通过 REST 返回 | 敏感信息 |
| `Shared` | 多节点必须一致 | 例如 `cluster.name` |

> **排障价值**：客户问「为什么我改 `X` 报错」，答案通常是 `Property.Dynamic` 没声明。
> 搜 `Setting.simpleString("X"` 找到定义处，看它的 Property 就知道。

**常用工厂方法**（`Setting.java`）：

```java
Setting.intSetting(key, defaultValue, minValue, maxValue, properties...)
Setting.boolSetting(key, defaultValue, properties...)
Setting.simpleString(key, properties...)
Setting.timeSetting(key, defaultValue, minValue, properties...)
Setting.byteSizeSetting(key, defaultValue, properties...)
Setting.affixKeySetting(prefix, suffix, key, properties...)    // ★ 动态前缀，如 index.routing.allocation.*
Setting.groupSetting(key, properties...)                        // 前缀组，值是 Settings
Setting.listSetting(key, defaultValue, parser, properties...)   // 逗号分隔列表
```

### 1.4 `AbstractScopedSettings`：动态更新的实现

| 方法 | 作用 |
| --- | --- |
| `registerSetting(...)` | 注册合法 Setting（在 Module 里调用） |
| `updateSettings(...)` | ★ 校验并应用新配置，返回 diff |
| `validate(...)` | 只校验不应用 |
| `addSettingsUpdateConsumer(...)` | ★ 注册回调，配置变了通知你 |
| `get(...)` / `getSetting(...)` | 读当前值 |

**`_cluster/settings` 的完整链路**：

```
RestClusterUpdateSettingsAction
  → TransportClusterUpdateSettingsAction.doMasterOperation
      → ClusterStateUpdateTask 内调 clusterSettings.updateSettings(updated)   ← 校验 + 生成 diff
      → 新 ClusterState（customs 里 ClusterSettings 结果变了）
      → ZenDiscovery.publish → 广播 → 各节点 ClusterApplierService.applyClusterState
           → 触发所有注册过的 updateConsumer
```

> **这就是「动态配置也走集群状态线」的原因**：配置本身是集群状态的一部分。

注册回调的典型形式：

```java
clusterSettings.addSettingsUpdateConsumer(MAX_INDEXING_MEMORY_SETTING, this::setMaxIndexingMemory);
```

### 1.5 `SettingsModule`

```java
@Override
protected void configure() {
    bind(Settings.class).toInstance(settings);
    bind(IndexScopedSettings.class).toInstance(indexScopedSettings);
    bind(ClusterSettings.class).toInstance(clusterSettings);
    ...
    // ★ 检查 settings 里有没有"不认识的 key"
    // 写了 node.masterr: true → 启动报 unknown setting [node.masterr]
}
```

### 1.6 `_cluster/settings` 的 REST 字段

```bash
PUT /_cluster/settings
{
  "persistent": { "cluster.routing.allocation.enable": "none" },
  "transient":  { "logger.org.elasticsearch": "DEBUG" }
}
```

| 字段 | 落盘 | 重启后 | 对应常量 |
| --- | --- | --- | --- |
| `persistent` | ✅（MetaData.persistentSettings） | 保留 | `ClusterSettings.PERSISTENT_SETTINGS` |
| `transient` | ❌ | **丢失** | `ClusterSettings.TRANSIENT_SETTINGS` |
| `defaults` | ❌ | 丢失（优先级最低） | `ClusterSettings.DEFAULT_SETTINGS` |

优先级：**transient > persistent > elasticsearch.yml > Setting 默认值**

> **云服务建议**：不要用 `transient` 做正式配置（重启即丢），那是 6.x 的历史包袱，7.x 已移除。

---

## 2. 依赖注入：Guice 与 ModulesBuilder

### 2.1 ES 6.6.1 用的是 Guice（fork 版）

```
server/src/main/java/org/elasticsearch/common/inject/     ← Guice 的 fork（约 200 个文件）
server/src/main/java/org/elasticsearch/common/inject/ModulesBuilder.java
server/src/main/java/org/elasticsearch/plugins/PluginsService.java
```

> ⚠️ **不要花时间读 `common/inject` 下的代码**。那是 Guice 内部实现，读它对理解 ES 无帮助。
> 只需知道：`Module.configure()` 里 `bind(X.class).toInstance(y)`；`@Inject` 标注构造器；
> `injector.getInstance(Class)` 取实例。

### 2.2 `ModulesBuilder`

```java
public class ModulesBuilder implements Module {
    private final List<Module> modules = new ArrayList<>();

    public ModulesBuilder add(Module... newModules) {
        Collections.addAll(modules, newModules);
        return this;
    }

    @Override
    public void configure(Binder binder) {
        // 用 Elements.getElements 把多个 Module 合并，再一起 apply
    }

    public Injector createInjector() {
        return Guice.createInjector(Stage.PRODUCTION, this);   // ★ Stage.PRODUCTION
    }
}
```

**`Stage.PRODUCTION` 的含义**：`asEagerSingleton()` 的绑定会在 `createInjector()` 时**立即实例化**。
所以 `Node.java:603` 往往是启动过程中最慢的一行。

### 2.3 真实例子：`RestController` 怎么被注入的

```java
// ActionModule
public class ActionModule extends AbstractModule {
    private final Map<String, ActionHandler<?, ?>> actions;
    private final RestController restController;

    @Override
    protected void configure() {
        bind(RestController.class).toInstance(restController);   // ① 绑定实例
        for (Map.Entry<String, ActionHandler<?, ?>> entry : actions.entrySet()) {
            bind(...);                                            // ② 绑定所有 TransportAction
        }
    }

    public void initRestHandlers(Supplier<DiscoveryNodes> nodesInCluster) {
        // ③ ★ 延迟到这里才实例化 handler（因为 handler 需要 Client 等）
        List<RestHandler> handlers = ...;
        for (RestHandler handler : handlers) {
            registerHandler.accept(handler);      // → RestController.registerHandler（:151）
        }
    }
}

// NetworkModule
bind(RestController.class).toInstance(restController);   // 来自 ActionModule（同一个实例）
```

**关键理解**：`Node.java:478` 创建 `ActionModule` 时传入的 `RestController` 是**空的**，
`Node.java:620` 调 `initRestHandlers(...)` 才把路由注册满。

### 2.4 常用注解

| 注解 | 位置 | 作用 |
| --- | --- | --- |
| `@Inject` | 构造器 / 字段 / 方法 | 声明依赖 |
| `@Singleton` | 类 | 单例 |
| `@Named("xxx")` | 参数 | 同类型多实例区分 |

---

## 3. 线程池：ES 的并发骨架（★ 必须掌握）

### 3.1 `ThreadPool` 的构造（`ThreadPool.java:165`）

```java
public ThreadPool(final Settings settings, final ExecutorBuilder<?>... customBuilders) {
    // :174-193 注册所有内置线程池
    // :194-199 注册插件自定义线程池（重名直接抛异常）
    // :202     创建 ThreadContext
    // :204-213 逐个构建 ExecutorHolder
    // :215     Names.SAME → DIRECT_EXECUTOR（直接用当前线程）
    // :217     Scheduler（定时任务）
    // :219     启动 CachedTimeThread
}
```

**完整内置线程池（★ 参数直接取自代码）**：

> ⚠️ **6.6.1 没有名为 `bulk` 的线程池**。它在 6.3 改名为 `write`。
> `ThreadPool.java:176` 写作
> `new FixedExecutorBuilder(settings, Names.WRITE, "bulk", availableProcessors, 200)`——
> 第 3 个参数 `"bulk"` 是 **fallbackName**（见 `FixedExecutorBuilder.java:67`），
> 即 `thread_pool.bulk.*` 作为已废弃的兼容配置仍生效，但池的真名是 `write`。

| 池名 | 行号 | 类型 | 大小 / 队列 | 用途 |
| --- | --- | --- | --- | --- |
| `generic` | `:174` | scaling | 4 ~ `4*CPU`，keep 30s | 通用后台任务（`ClusterStateUpdateTask` 默认池） |
| `index` | `:175` | fixed | CPU 个，queue 200 | 索引/删除的分片级执行 |
| `write` | `:176` | fixed | CPU 个，queue 200 | 写入（bulk 的分片级执行） |
| `get` | `:177` | fixed | CPU 个，queue 1000 | realtime get / termvectors |
| `analyze` | `:178` | fixed | 1 个，queue 16 | `_analyze` |
| `search` | `:179` | auto-adjusting | `searchThreadPoolSize(CPU)`，queue 1000 | 查询执行 |
| `search_throttled` | `:181` | auto-adjusting | 1 个，queue 100 | 节流搜索（ML/rollup） |
| `management` | `:183` | scaling | 1 ~ 5，keep 5min | 集群管理 |
| `listener` | `:186` | fixed | `min(10, CPU/2)`，**queue -1（无界）** | listener 回调 |
| `flush` | `:187` | scaling | 1 ~ 5 | flush |
| `refresh` | `:188` | scaling | 1 ~ 10 | refresh |
| `warmer` | `:189` | scaling | 1 ~ 5 | IndexWarmer |
| `snapshot` | `:190` | scaling | 1 ~ 5 | 快照数据写入 |
| `fetch_shard_started` | `:191` | scaling | 1 ~ `2*CPU` | 恢复 phase1 |
| `force_merge` | `:192` | fixed | 1 个，**无界** | force merge |
| `fetch_shard_store` | `:193` | scaling | 1 ~ `2*CPU` | 恢复 store 阶段 |
| `same` | `:215` | direct | — | 当前线程执行 |

此外还有 `snapshot_meta`（串行 1 个）、`http_server_worker`、`transport_worker`（由 `NetworkModule` 注册）。

### 3.2 `ThreadPoolType`：三种类型（★ 排障关键）

| 类型 | 队列 | 拒绝行为 | 典型池 |
| --- | --- | --- | --- |
| `fixed` | 有界（除非 queueSize=-1） | 队列满 → 抛 `EsRejectedExecutionException`（HTTP 429） | `write`/`index`/`get`/`search` |
| `scaling` | `LinkedTransferQueue`（无界） | ★ **永远不会拒绝**，只会变慢 | `refresh`/`flush`/`warmer`/`snapshot` |
| `direct` | 无 | 无 | `same` |

> **★ 排障重点**：`scaling` 类型的池（`refresh`、`flush`）因为队列无界，
> **不会报 429**。所以「refresh 慢但没告警」是正常现象，要看 `_nodes/stats/thread_pool` 的 `queue`。

### 3.3 `Info` 与 `Stats`

```java
public static class Info  { String name; ThreadPoolType type; int min, max, queueSize; TimeValue keepAlive; ... }  // :585
public static class Stats { int threads, queue, active, largest; long rejected, completed; }                       // :269
```

对应 `_nodes/stats/thread_pool`：

```bash
GET /_nodes/stats/thread_pool?pretty
```

**排障口诀**：

| 观察 | 结论 |
| --- | --- |
| `queue` 持续 > 0 且 `active == max` | 线程池打满，**瓶颈在这里** |
| `rejected` 增长 | 已经在拒绝请求（客户端看到 429） |
| `largest` 远大于 `threads` | 历史上出现过突发，可据此调 `thread_pool.<name>.size` |

### 3.4 `EsThreadPoolExecutor`

```
common/util/concurrent/EsThreadPoolExecutor.java
common/util/concurrent/EsAbortPolicy.java
common/util/concurrent/EsRejectedExecutionException.java
```

**设计差异**：JDK 默认是「队列满了才扩容线程」，ES 改成「**先扩容到 max，再排队**」
（因为排队增加延迟）。队列满时 `EsAbortPolicy` 抛 `EsRejectedExecutionException`，
并在 `_nodes/stats` 里 `rejected++`。

### 3.5 `ThreadContext`：跨线程上下文的传递（★ 极重要）

文件：`server/src/main/java/org/elasticsearch/common/util/concurrent/ThreadContext.java`

保存内容：**headers**（`X-Opaque-Id`、`Authorization`）、**transient headers**、
**response headers**、**system context 标记**。

**核心问题**：`ThreadLocal` 不跨线程。所以 ES 提供：

```java
public StoredContext stashContext()                 // :132 ★ 打包上下文，返回可关闭对象
public StoredContext newRestorableContext(boolean)  // 可恢复的上下文（供响应回调恢复）
public void readHeaders(StreamInput in)              // :236 从 Transport 消息恢复
public void writeHeaders(StreamOutput out)           // 写入 Transport 消息
public String getHeader(String key)                  // :243
public void putHeader(String key, String value)      // :286
public void markAsSystemContext()                    // :375
public boolean isSystemContext()                     // :382
```

**标准范式（务必记住）**：

```java
try (ThreadContext.StoredContext ctx = threadContext.stashContext()) {
    threadContext.putHeader("X-Opaque-Id", someId);
    executor.execute(() -> doSomething());
}
```

**跨节点传递**：
- 发送：`TcpTransport.sendRequestToChannel` 里 `threadContext.writeHeaders(out)`
- 接收：`TcpTransport.messageReceived:1107` 里 `threadContext.readHeaders(streamIn)`

所以 `X-Opaque-Id` 能贯穿整个分布式调用链——排查分布式问题的最强工具。

### 3.6 `CachedTimeThread`（`:219`）

ES 有个 `[timer]` 线程每 200ms 更新一次时间，其他线程用 `relativeTimeInMillis()`（`:229`）读缓存。

**为什么？** `System.currentTimeMillis()` 在高并发下是热点。ES 用「精度换性能」。
**副作用**：ES 内部时间戳精度是 200ms（`ESTIMATED_TIME_INTERVAL_SETTING`，`:163`）。

### 3.7 线程池相关设置

| 配置 | 默认 | 含义 |
| --- | --- | --- |
| `thread_pool.<name>.size` | 见上表 | 线程数 |
| `thread_pool.<name>.queue_size` | 见上表 | 队列长度，`-1` 无界 |
| `thread_pool.<name>.keep_alive` | 见上表 | scaling 的空闲回收时间 |
| `processors` | CPU 核数 | ★ 覆盖自动探测的 CPU 数 |

> **云环境必看**：容器/cgroup 限制下 `Runtime.getRuntime().availableProcessors()` 可能是宿主机核数，
> 导致**所有线程池（以及 Netty 的 worker 数）都过大**。
> 这就是云厂商通常强制设置 `processors` 的原因。见 `Netty4Transport.java:112`。

---

## 4. 序列化体系：`StreamInput` / `StreamOutput`

### 4.1 为什么不用 Java 原生序列化

体积大、性能差、跨版本即失败、有反序列化漏洞。
ES 手写二进制协议，通过 `Version` 参数精确控制兼容性。

### 4.2 版本感知的序列化

```java
@Override
public void writeTo(StreamOutput out) throws IOException {
    super.writeTo(out);
    out.writeString(index);
    if (out.getVersion().onOrAfter(Version.V_6_4_0)) {     // ★ 版本分支
        out.writeLong(version);
    }
}
```

**这是滚动升级兼容的基础**：6.6 节点与 6.3 节点通信时，6.6 会读到 `Version.V_6_3_0`，
从而不发送 6.3 不认识的新字段。

版本从哪里来？见 05 章 5.2 的第 ⑦ 步：`TcpTransport.messageReceived:1103` 的 `streamIn.setVersion(version)`。

### 4.3 常用方法

| `StreamOutput` | `StreamInput` | 说明 |
| --- | --- | --- |
| `writeString` / `writeStringArray` | `readString` / `readStringArray` | UTF-8 + 长度前缀 |
| `writeInt` / `writeLong` / `writeVInt` / `writeVLong` | 对应 read | VInt 变长编码省空间 |
| `writeOptionalString` | `readOptionalString` | 支持 null |
| `writeGenericValue` | `readGenericValue` | 任意 JSON 值（`_source` 用） |
| `writeNamedWriteable` | `readNamedWriteable` | ★ 通过注册表反查类 |
| `writeException` | `readException` | ★ 异常跨网络传播 |
| `writeTimeValue` / `writeBytesReference` / `writeGeoPoint` | 对应 read | 常用类型 |
| `setVersion` / `getVersion` | 同 | ★ 版本感知 |

### 4.4 异常跨节点传播

```java
// 发送方
out.writeException(t);                              // 写类名 + message + stackTrace + 元数据
// 接收方
Exception e = in.readException();                   // 反射重建同类型异常
```

源码：`ElasticsearchException.writeTo` / `readFrom`、`ExceptionsHelper`。

**排障**：客户看到的 `RemoteTransportException ... Caused by: IndexNotFoundException`
就是这套机制把远端异常"搬"到了本地。

### 4.5 `NamedWriteableRegistry`

```java
// 注册（NetworkModule / IndicesModule / SearchModule 里）
new NamedWriteableRegistry.Entry(QueryBuilder.class, MatchQueryBuilder.NAME, MatchQueryBuilder::new),
new NamedWriteableRegistry.Entry(AggregationBuilder.class, TermsAggregationBuilder.NAME, TermsAggregationBuilder::new),
```

`NAME` 就是 JSON 里那个名字（`"match"`、`"terms"`）。

> **★ 写自定义 Query/Aggregation 必须注册**，否则跨节点传播时抛 `NamedWriteableNotFoundException`。
> 注册入口：`SearchPlugin.getQueries()` / `getAggregations()`。

---

## 5. `XContent`：JSON/YAML/SMILE 的统一抽象

```
common/xcontent/
├── XContent.java              ★ 抽象工厂
├── XContentType.java          JSON / YAML / SMILE / CBOR
├── XContentParser.java        ★ 解析器（token 流）
├── XContentBuilder.java       ★ 生成器
├── json/JsonXContent.java
├── yaml/YamlXContent.java
├── smile/ · cbor/
└── support/                   ★ 最常用的工具
    ├── XContentParserUtils.java
    ├── ConstructingObjectParser.java
    └── ObjectParser.java
```

### 5.1 ★ `ConstructingObjectParser`（现代解析模式）

```java
private static final ConstructingObjectParser<MatchQueryBuilder, Void> PARSER =
    new ConstructingObjectParser<>("match", true, args -> new MatchQueryBuilder((String) args[0], args[1]));

static {
    PARSER.declareString(constructorArg(), new ParseField("field"));
    PARSER.declareObject(OptionalConstructorArg(), (p, c) -> parseQuery(p), new ParseField("query"));
    PARSER.declareString(MatchQueryBuilder::lenient, new ParseField("lenient"));
}
```

**关键点**：
- `ParseField` 支持「当前名 + 已废弃别名」，实现字段重命名兼容（如 `fields` → `stored_fields`）
- `constructorArg()` / `OptionalConstructorArg()` / setter 三种绑定方式
- 所有 `QueryBuilder` / `AggregationBuilder` 都有这样的静态 `PARSER`

### 5.2 `XContentParser` 的 token 模型

```java
while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
    if (token == XContentParser.Token.FIELD_NAME) {
        String fieldName = parser.currentName();
    } else if (token == XContentParser.Token.START_OBJECT) { ... }
}
```

`Token`：`START_OBJECT` / `END_OBJECT` / `START_ARRAY` / `END_ARRAY` /
`FIELD_NAME` / `VALUE_STRING` / `VALUE_NUMBER` / `VALUE_BOOLEAN` / `VALUE_NULL`

### 5.3 不同场景用哪个 API

| 场景 | API |
| --- | --- |
| REST 请求解析 | `RestRequest.contentParser()` → `XContentParser` |
| REST 响应生成 | `XContentBuilder` → `RestResponse` |
| 集群状态序列化 | `.toXContent(builder, params)` / `fromXContent(parser)` |
| 磁盘持久化 | `XContentHelper.writeToPath` / `readFromPath` |
| Transport 二进制 | **不用 XContent**，用 `StreamInput/Output` |

---

## 6. 其它必知工具类

### 6.1 `BytesReference` 家族

```
common/bytes/BytesReference.java             抽象（类似 Netty ByteBuf 的角色）
common/bytes/BytesArray.java                 包装 byte[]
common/bytes/ReleasableBytesReference.java  ★ 带引用计数
common/bytes/CompositeBytesReference.java    零拷贝拼接
common/bytes/Netty4Utils.java                ES ↔ Netty ByteBuf 互转
```

**为什么不用 `byte[]`？** 零拷贝 + 引用计数。
`_source` 从 Lucene 读到 HTTP 响应全程不复制。

> ⚠️ **坑**：`ReleasableBytesReference` 必须 `decRef()`，否则内存泄漏。查内存泄漏重点看这里。

### 6.2 高效集合（读聚合/fielddata 代码必认）

```
common/collect/LongObjectPagedHashMap.java   long → Object，不装箱
common/util/LongHash.java / LongLongHash / LongObjectHash   开放寻址哈希
common/util/IntArray.java / LongArray.java / IntObjectHashMap.java
common/util/BitSet.java / CountedBitSet.java
```

**不要在普通业务代码里用**（可读性差），但读聚合代码时必须认识它们。

### 6.3 `BigArrays`：受熔断器管理的大数组

```java
long[] array = bigArrays.newLongArray(size);      // 会检查 parent breaker
bigArrays.adjustBreaker(size);                    // 手动调整
```

**内存保护核心**：所有可能很大的内存（fielddata、聚合桶、请求缓存）都必须走 `BigArrays`。

### 6.4 `Releasable` / `LifecycleComponent` / `Closeable`

| 接口 | 路径 | 语义 |
| --- | --- | --- |
| `Closeable` | JDK | 一次性资源，`close()` |
| `Releasable` | `common/lease/Releasable.java` | ★ **引用计数**，可多次持有 |
| `LifecycleComponent` | `common/component/` | 有状态组件（start/stop/close） |

> **`Releasable` 是 ES 最容易被忽略的坑**：`IndexShard`、`Engine`、`Store`、
> `ReleasableBytesReference`、`SearchContext` 都是 `Releasable`。
> 忘记 `decRef()` 会导致分片无法关闭、内存不释放。

### 6.5 `TimeValue` / `ByteSizeValue`

```
common/unit/TimeValue.java       "30s" / "5m" / "1d" 的解析与格式化
common/unit/ByteSizeValue.java   "10mb" / "1gb" 的解析与格式化
common/unit/SizeValue.java       无单位数值
```

「为什么 `10MB` 能被解析而 `10MiB` 不能」这类问题的答案就在这两个类里。

### 6.6 其它并发工具

```
common/util/concurrent/AsyncIOProcessor.java        ★ 批量异步 IO（translog、recovery 用）
common/util/concurrent/KeyedLock.java               ★ 按 key 加锁（IndexShard 内部大量使用）
common/util/concurrent/CountDown.java               一次性倒计时
common/util/concurrent/ExponentialBackoffPolicy.java 重试策略
common/util/concurrent/AbstractRunnable.java        ★ 能抛异常、有 onRejection/onFailure 的 Runnable
common/util/concurrent/ConcurrentCollections.java
```

`KeyedLock` 特别值得记住：ES 用它实现「按分片 ID 加锁」。

---

## 7. 动手实验

**实验 4-1：数一数 `IndexScopedSettings` 里注册了多少 setting**
在 `IndexScopedSettings.java` 里看 `registerSetting` 的调用数量，随便挑一个用 `PUT /myindex/_settings` 改它。

**实验 4-2：让动态设置的回调生效**
在 `ThreadPool` 或 `IndicesService` 里找一个 `addSettingsUpdateConsumer` 打断点，
用 `_cluster/settings` 改那个设置，观察回调被触发。

**实验 4-3：验证线程池拒绝**
把 `thread_pool.write.queue_size` 设为 1，压测写入，
观察 `EsRejectedExecutionException` 与 `_nodes/stats/thread_pool` 的 `rejected` 增长。

**实验 4-4：验证 `processors` 的影响**
分别用不设置 / `processors: 2` 启动，对比 `_nodes/stats/thread_pool` 里
`write` 与 `search` 的 `max` 值，以及 `jstack` 里 netty 线程数。

**实验 4-5：跨节点追踪 `X-Opaque-Id`**
在 `TransportService.sendRequestInternal` 和 `TcpTransport.messageReceived` 打断点，
带 `X-Opaque-Id: abc` 发请求，观察 header 的序列化/反序列化。

**实验 4-6：写一个自定义 Setting**
在你的插件里加 `Setting.simpleString("myplugin.foo", Property.NodeScope, Property.Dynamic)`，
在 Module 里注册，然后用 `_cluster/settings` 修改它。

---

## 8. 自测题

1. `Settings` 为什么设计成不可变的扁平 Map？`getAsGroups()` 有什么用？
2. `Setting.Property.Dynamic` 的作用？没有它会怎样？
3. 动态配置为什么必须走集群状态线？
4. `transient` 与 `persistent` 的优先级？它们和 `elasticsearch.yml` 谁优先？
5. `Stage.PRODUCTION` 下的 `createInjector()` 会做什么额外的事？
6. `fixed` 和 `scaling` 两种线程池在**拒绝行为**上的区别？对排障意味着什么？
7. `ThreadContext` 如何实现跨线程/跨节点传递？为什么 `stashContext()` 必须配 try-with-resources？
8. `EsThreadPoolExecutor` 与 JDK `ThreadPoolExecutor` 在扩容策略上的差异？
9. `Writeable.writeTo` 里为什么要判断 `out.getVersion().onOrAfter(...)`？
10. `NamedWriteableRegistry` 解决什么问题？不注册会怎样？
11. `Releasable` 与 `Closeable` 的区别？`ReleasableBytesReference` 忘记 `decRef()` 会怎样？
12. `BigArrays` 与熔断器的关系？为什么不能直接 `new long[size]`？

---

## 9. 关键类速查

| 类 | 路径 | 一句话 |
| --- | --- | --- |
| `Settings` | `common/settings/Settings.java:84` | 不可变配置容器 |
| `Setting` | `common/settings/Setting.java` | 配置项元数据 |
| `ClusterSettings` | `common/settings/ClusterSettings.java` | 集群动态配置 |
| `IndexScopedSettings` | `common/settings/IndexScopedSettings.java` | 索引级配置 |
| `AbstractScopedSettings` | `common/settings/AbstractScopedSettings.java` | 配置注册与动态更新 |
| `SettingsModule` | `common/settings/SettingsModule.java` | 配置体系注册到 Guice |
| `ModulesBuilder` | `common/inject/ModulesBuilder.java` | Guice 模块容器 |
| `ThreadPool` | `threadpool/ThreadPool.java:165` | 所有线程池 |
| `EsThreadPoolExecutor` | `common/util/concurrent/EsThreadPoolExecutor.java` | ES 的线程池语义 |
| `ThreadContext` | `common/util/concurrent/ThreadContext.java` | 跨线程/跨节点上下文 |
| `StreamInput` / `StreamOutput` | `common/io/stream/` | 二进制序列化 |
| `NamedWriteableRegistry` | `common/io/stream/NamedWriteableRegistry.java` | 序列化类名注册表 |
| `XContent` / `XContentBuilder` | `common/xcontent/` | JSON/YAML 抽象 |
| `BytesReference` | `common/bytes/BytesReference.java` | 零拷贝字节抽象 |
| `BigArrays` | `common/util/BigArrays.java` | 受熔断器管理的数组 |
| `Releasable` | `common/lease/Releasable.java` | 引用计数资源 |
| `TimeValue` | `common/unit/TimeValue.java` | 时间值解析 |

---

**上一篇**：[03-启动流程与节点生命周期.md](./03-启动流程与节点生命周期.md)　
**下一篇**：[05-Transport节点间通信层.md](./05-Transport节点间通信层.md)
