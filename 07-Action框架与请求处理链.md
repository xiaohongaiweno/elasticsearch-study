# 07 · Action 框架与请求处理链

> 目标：搞懂 ES 的「业务代码组织方式」——一个 `Action` 从 `client.xxx()` 到 `doExecute()` 的全过程，
> 以及 `ActionFilter`、`Task`、线程池、转发到 Master/其他节点的机制。
>
> 这是**读任何业务模块前的必须知识**：不理解 Action 框架，看 `TransportBulkAction` 会一头雾水。
>
> 本文第 8 节是重点：三条真实链路（本地 / 跨节点 / Master 转发）+ 装配链路 + 线程切换总表，
> 全部标注源码行号，可直接对着源码单步走。

---

## 1. Action 体系全景

```
┌──────────────────────────────────────────────────────────────────────────┐
│  客户端接口层                                                             │
│   Client（接口）→ AbstractClient（抽象）→ NodeClient（本节点执行）          │
│                                      → TransportClient（远程，已废弃）     │
│   · client.bulk(req, listener)  →  AbstractClient.bulk  →  doExecute()   │
└─────────────────────────────────┬────────────────────────────────────────┘
                                  │
┌─────────────────────────────────▼────────────────────────────────────────┐
│  Action 描述符层                                                          │
│   Action<Request, Response, RequestBuilder>（抽象类，含 name + 类型）      │
│     └── 例如 IndexAction.INSTANCE / BulkAction.INSTANCE / SearchAction.INSTANCE
│   GenericAction<Request, Response>（无 RequestBuilder 的版本）            │
│   ActionRequest（请求基类）· ActionResponse（响应基类）                     │
└─────────────────────────────────┬────────────────────────────────────────┘
                                  │
┌─────────────────────────────────▼────────────────────────────────────────┐
│  Action 执行层（★ 核心）                                                   │
│   TransportAction<Request, Response>（抽象基类，:40）                      │
│     ├─ execute(Request, ActionListener)      (:69)   ★ Client/REST 入口   │
│     ├─ execute(Request, TaskListener)        (:102)  需要 Task 句柄时用    │
│     ├─ execute(Task, Request, ActionListener)(:127)  ★ 统一执行入口        │
│     └─ doExecute(...)                        (:142/:146) 抽象             │
│                                                                           │
│   子类：                                                                   │
│     HandledTransportAction      ← 大多数 action 的基类（自动注册 handler）  │
│     TransportReplicationAction  ← 主副本复制 action（见 13 章）            │
│     TransportMasterNodeAction   ← 必须由 master 执行的 action              │
│     TransportBroadcastAction    ← 广播到所有分片执行的 action               │
│     TransportBroadcastReplicationAction                                    │
└─────────────────────────────────┬────────────────────────────────────────┘
                                  │
┌─────────────────────────────────▼────────────────────────────────────────┐
│  注册与装配层                                                              │
│   ActionModule（:340）· ActionHandler（action → TransportAction 的映射）    │
│   TransportActionNodeProxy（把一个 request 发到指定节点的 action）           │
│   ActionFilters / ActionFilter（横切逻辑，如 X-Pack Security、审计）        │
└──────────────────────────────────────────────────────────────────────────┘
```

**关键文件清单**

| 文件 | 路径 |
| --- | --- |
| `Action` | `server/src/main/java/org/elasticsearch/action/Action.java` |
| `GenericAction` | `server/src/main/java/org/elasticsearch/action/GenericAction.java` |
| `ActionRequest` | `server/src/main/java/org/elasticsearch/action/ActionRequest.java` |
| `ActionResponse` | `server/src/main/java/org/elasticsearch/action/ActionResponse.java` |
| `ActionListener` | `server/src/main/java/org/elasticsearch/action/ActionListener.java` |
| `TransportAction` | `server/src/main/java/org/elasticsearch/action/support/TransportAction.java` |
| `HandledTransportAction` | `server/src/main/java/org/elasticsearch/action/support/HandledTransportAction.java` |
| `TransportActionNodeProxy` | `server/src/main/java/org/elasticsearch/action/TransportActionNodeProxy.java` |
| `ActionListenerResponseHandler` | `server/src/main/java/org/elasticsearch/action/ActionListenerResponseHandler.java` |
| `ActionFilter` / `ActionFilterChain` | `server/src/main/java/org/elasticsearch/action/ActionFilter.java` |
| `ActionModule` | `server/src/main/java/org/elasticsearch/action/ActionModule.java` |
| `ActionHandler` | `server/src/main/java/org/elasticsearch/action/ActionModule.java`（内部类） |
| `NodeClient` | `server/src/main/java/org/elasticsearch/client/node/NodeClient.java` |
| `AbstractClient` | `server/src/main/java/org/elasticsearch/client/support/AbstractClient.java` |
| `Task` / `TaskManager` | `server/src/main/java/org/elasticsearch/tasks/` |
| `RequestHandlerRegistry` | `server/src/main/java/org/elasticsearch/transport/RequestHandlerRegistry.java` |
| `TaskTransportChannel` | `server/src/main/java/org/elasticsearch/transport/TaskTransportChannel.java` |

---

## 2. `Action` 描述符

### 2.1 三件套

```java
// ① Action：动作描述符（单例）
public class IndexAction extends Action<IndexRequest, IndexResponse, IndexRequestBuilder> {
    public static final IndexAction INSTANCE = new IndexAction();
    public static final String NAME = "indices:data/write/index";

    private IndexAction() { super(NAME); }

    @Override public IndexResponse newResponse() { return new IndexResponse(); }
    @Override public IndexRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new IndexRequestBuilder(client, this);
    }
}
```

```java
// ② ActionRequest：请求（可序列化、可校验）
public class IndexRequest extends ReplicatedWriteRequest<IndexRequest> implements DocWriteRequest<IndexRequest> {
    private String index; private String type; private String id; private BytesReference source;
    @Override public ActionRequestValidationException validate() { ... }     // ★ 参数校验
    @Override public void writeTo(StreamOutput out) throws IOException { ... }
}
```

```java
// ③ ActionResponse：响应
public class IndexResponse extends DocWriteResponse { ... }
```

**命名规范**：`<Xxx>Action` + `<Xxx>Request` + `<Xxx>Response` + `<Xxx>RequestBuilder`（可选）。

**`actionName` 的命名规范**（非常重要，日志/`_tasks`/权限都用它）：

| 前缀 | 含义 | 例子 |
| --- | --- | --- |
| `indices:data/write/*` | 数据写入 | `indices:data/write/index`、`indices:data/write/bulk` |
| `indices:data/read/*` | 数据读取 | `indices:data/read/search`、`indices:data/read/get` |
| `indices:admin/*` | 索引管理 | `indices:admin/create`、`indices:admin/mapping/put` |
| `cluster:monitor/*` | 集群监控 | `cluster:monitor/health`、`cluster:monitor/state` |
| `cluster:admin/*` | 集群管理 | `cluster:admin/settings/update`、`cluster:admin/reroute` |
| `internal:*` | ES 内部使用 | `internal:index/shard/recovery/start_recovery` |
| `cluster:admin/xpack/*` | X-Pack 功能 | |

> **X-Pack Security 的权限模型就是基于 `actionName` 前缀的**。
> 所以自定义 action 时**必须**遵循这个规范，否则没法用 RBAC 控制。
>
> `actionName` 还会被 `TransportService.registerRequestHandler` 用 `validateActionName(action)`
> 校验（`TransportService.java:789`），非法名字在注册阶段就报错。

### 2.2 `ActionListener`：异步回调

```java
public interface ActionListener<Response> {
    void onResponse(Response response);
    void onFailure(Exception e);

    // 静态工具
    static <Response> ActionListener<Response> wrap(...)
    static <Response> ActionListener<Response> runAfter(ActionListener<Response> listener, Runnable runnable)
    static <Response> ActionListener<Response> notifyOnce(ActionListener<Response> delegate)   // ★ 防止重复回调
}
```

**★ 核心约定**：`onResponse` 和 `onFailure` **只能被调用一次**。
违反这个约定会导致各种诡异的 bug（如响应重复发送、状态错乱）。
所以要包装 listener 时，务必用 `ActionListener.notifyOnce(...)` 或 `delegateFailure`。

常用方法：

| 方法 | 作用 |
| --- | --- |
| `wrap(Runnable onResponse, Consumer<Exception> onFailure)` | 包装 |
| `notifyOnce(listener)` | 保证只回调一次 |
| `runAfter(listener, runnable)` | 回调后执行一段代码 |
| `map(listener, Function)` | 转换响应类型（ES 6.6 里叫 `ActionListener.map`） |
| `delegateFailure(listener, ...)` | 失败转发 |

---

## 3. `TransportAction`：执行骨架（★ 核心）

文件：`server/src/main/java/org/elasticsearch/action/support/TransportAction.java`

### 3.1 字段与构造

```java
public abstract class TransportAction<Request extends ActionRequest, Response extends ActionResponse> extends AbstractComponent {
    protected final ThreadPool threadPool;                        // :42
    protected final String actionName;                            // :43
    private final ActionFilter[] filters;                         // :44
    protected final IndexNameExpressionResolver indexNameExpressionResolver;
    protected final TaskManager taskManager;                      // :46

    protected TransportAction(Settings settings, String actionName, ThreadPool threadPool, ActionFilters actionFilters,
                              IndexNameExpressionResolver indexNameExpressionResolver, TaskManager taskManager) {   // :48
        this.filters = actionFilters.filters();      // ★ 排好序的过滤器数组（:53）
    }
}
```

**`filters` 来自哪？** `ActionModule.setupActionFilters`（`ActionModule.java:525`）收集所有插件的
`ActionPlugin.getActionFilters()` 结果，装进 `ActionFilters`，在 `configure()` 里 `bind(ActionFilters.class).toInstance(actionFilters)`（`:673`），
由 Guice 注入每个 `TransportAction` 的构造器。

### 3.2 三个 `execute` 重载（★ 别搞混）

| 行号 | 签名 | 谁在调 | 说明 |
| --- | --- | --- | --- |
| `:58` | `execute(Request)` | 测试/同步场景 | 内部包一个 `PlainActionFuture`，阻塞取结果 |
| `:69` | `execute(Request, ActionListener<Response>)` | **★ `NodeClient.executeLocally`** | 注册 Task，用完即弃（listener 拿不到 task） |
| `:102` | `execute(Request, TaskListener<Response>)` | 需要 Task 句柄的调用方 | 同上，但回调里带回 task |
| `:127` | `execute(Task, Request, ActionListener<Response>)` | **★ 一切的最终汇聚点** | 不再创建 Task，直接校验 + 跑 filter 链 |

`:69` 的实现（原文误写成 TaskListener 版，这里以源码为准）：

```java
public final Task execute(Request request, ActionListener<Response> listener) {   // :69
    Task task = taskManager.register("transport", actionName, request);           // :77
    if (task == null) {
        execute(null, request, listener);        // 请求不要求跟踪 → 无 task 执行
    } else {
        execute(task, request, new ActionListener<Response>() {                   // :81
            @Override public void onResponse(Response response) {
                taskManager.unregister(task);     // ★ 本地路径：在这里注销 Task
                listener.onResponse(response);
            }
            @Override public void onFailure(Exception e) {
                taskManager.unregister(task);
                listener.onFailure(e);
            }
        });
    }
    return task;
}
```

> **注意 `task == null` 的分支**：`TaskManager.register` 是否返回 null 取决于
> `request.createTask(...)` 的实现（`TaskManager.java:115-118`）。
> `TaskAwareRequest.createTask` 的默认实现返回普通 `Task`（`TaskAwareRequest.java:52`），
> 子类可以覆盖返回 `CancellableTask`（如 `SearchTask`、`ReplicationTask`、`BulkByScrollTask`），
> 也可以返回 `null` 表示"这个请求不进 `_tasks`"。

### 3.3 `execute(task, request, listener)`（`:127`）——★★ 真正的入口

```java
public final void execute(Task task, Request request, ActionListener<Response> listener) {   // :127
    // ① ★ 参数校验（子类实现 validate()）
    ActionRequestValidationException validationException = request.validate();     // :128
    if (validationException != null) {
        listener.onFailure(validationException);
        return;
    }

    // ② 如果需要存储结果（异步 action），包装 listener
    if (task != null && request.getShouldStoreResult()) {                          // :134
        listener = new TaskResultStoringActionListener<>(taskManager, task, listener);   // :135
    }

    // ③ ★ 构造过滤器链并启动
    RequestFilterChain<Request, Response> requestFilterChain = new RequestFilterChain<>(this, logger);  // :138
    requestFilterChain.proceed(task, actionName, request, listener);               // :139
}
```

- `validate()` 是抽象方法（`ActionRequest.java:41`），**每次执行都会被调用**，包括跨节点入站时（对端收到请求后也会 validate 一次）。
- `getShouldStoreResult()` 默认 false（`ActionRequest.java:46`），只有 `_tasks` 需要回看结果的异步 action（如 reindex）才返回 true；
  此时 `TaskResultStoringActionListener`（`:182`）会在响应/失败时调 `taskManager.storeResult(...)`。

### 3.4 `RequestFilterChain`（`:148`）——过滤器链

```java
private static class RequestFilterChain<Request extends ActionRequest, Response extends ActionResponse>
        implements ActionFilterChain<Request, Response> {                          // :148
    private final TransportAction<Request, Response> action;
    private final AtomicInteger index = new AtomicInteger();       // ★ 用 AtomicInteger 做"指针"（:152）

    @Override
    public void proceed(Task task, String actionName, Request request, ActionListener<Response> listener) {   // :161
        int i = index.getAndIncrement();
        try {
            if (i < this.action.filters.length) {
                this.action.filters[i].apply(task, actionName, request, listener, this);   // ① 下一个 filter
            } else if (i == this.action.filters.length) {
                this.action.doExecute(task, request, listener);                            // ② ★ 链尾 → 业务
            } else {
                listener.onFailure(new IllegalStateException("proceed was called too many times"));  // ③ 防重复
            }
        } catch (Exception e) {
            logger.trace("Error during transport action execution.", e);
            listener.onFailure(e);
        }
    }
}
```

**设计要点**：
- 这是一个**非递归的链式调用**（用 `AtomicInteger` 模拟栈深度）。
  之所以不用递归，是因为链可能很长（X-Pack 会加很多 filter），递归有栈溢出风险。
- **防重入保护**：`proceed` 被多调用一次就会抛 `proceed was called too many times`。
  这是排障的一个重要信号——说明某个 filter 写错了。
- 链是**每次执行新建**的（`new RequestFilterChain<>(this, logger)`），不存在并发复用问题。

`ActionFilter` 接口：

```java
public interface ActionFilter {
    void apply(Task task, String action, ActionRequest request, ActionListener<?> listener, ActionFilterChain chain);
}
```

> **★ X-Pack Security 就是通过 `ActionFilter` 实现的**：
> 它在 `apply` 里做鉴权，通过则 `chain.proceed(...)`，不通过则 `listener.onFailure(...)`。
> 所以「鉴权发生在 `doExecute` 之前」——这是理解 ES 安全模型的关键。

---

## 4. `HandledTransportAction`：99% 的 action 的基类

文件：`server/src/main/java/org/elasticsearch/action/support/HandledTransportAction.java`

### 4.1 构造器（4 个重载）

```java
// :45 —— 用 Supplier<Request>
protected HandledTransportAction(Settings settings, String actionName, ThreadPool threadPool,
                                TransportService transportService, ActionFilters actionFilters,
                                IndexNameExpressionResolver indexNameExpressionResolver,
                                Supplier<Request> request) { ... }

// :51 —— 用 Writeable.Reader<Request>（6.x 新写法，配合 request 的构造函数引用）
protected HandledTransportAction(Settings settings, String actionName, ThreadPool threadPool,
                                TransportService transportService, ActionFilters actionFilters,
                                Writeable.Reader<Request> requestReader,
                                IndexNameExpressionResolver indexNameExpressionResolver) { ... }

// :57 —— 带 canTripCircuitBreaker（Supplier 版）
protected HandledTransportAction(Settings settings, String actionName, boolean canTripCircuitBreaker, ThreadPool threadPool,
                                 TransportService transportService, ActionFilters actionFilters,
                                 IndexNameExpressionResolver indexNameExpressionResolver, Supplier<Request> request) {
    super(settings, actionName, threadPool, actionFilters, indexNameExpressionResolver, transportService.getTaskManager());  // :60
    transportService.registerRequestHandler(actionName, request, ThreadPool.Names.SAME, false, canTripCircuitBreaker,
        new TransportHandler());                              // ★★ 关键：注册 handler（:61）
}

// :65 —— Reader 版 + canTripCircuitBreaker
protected HandledTransportAction(..., Writeable.Reader<Request> requestReader, ...) {
    super(...);
    transportService.registerRequestHandler(actionName, ThreadPool.Names.SAME, false, canTripCircuitBreaker, requestReader,
        new TransportHandler());                              // :69
}
```

**★ 两个极其重要的细节**：

1. **executor 是 `ThreadPool.Names.SAME`**
   → 意味着**这个 action 在自己的请求线程上开始执行**。
   - 对于从网络来的请求：就是 **transport 的工作线程**（见 8.3 节：`TcpTransport.java:1246` 按 `reg.getExecutor()` 分派，`SAME` 即不切换）
   - 对于本地调用：就是**调用方线程**（REST 场景是 `http_server_worker`，见 06 章）
   → **所以 `HandledTransportAction.doExecute` 里如果做重活，就会阻塞网络/HTTP 线程！**
   → 正确做法：在 `doExecute` 里立刻 `threadPool.executor("xxx").execute(...)` 切走
     （`TransportMasterNodeAction`、`TransportSearchAction`、`TransportBulkAction` 都是这么干的）。

2. **`forceExecution = false`、`canTripCircuitBreaker` 按需**
   → `false` 表示允许线程池拒绝；但 `SAME` 是 direct executor，实际不会拒绝，
     真正的拒绝风险在 `doExecute` 内部自己 `threadPool.executor(...).execute(...)` 时。
   → `canTripCircuitBreaker=true` 表示入站时会把请求字节数计入 in-flight 熔断器
     （`TcpTransport.java:1235-1239`）：超限直接 break，否则只累加不 break。

### 4.2 `TransportHandler`：只用三参方法

```java
class TransportHandler implements TransportRequestHandler<Request> {                    // :73
    @Override
    public final void messageReceived(Request request, TransportChannel channel) throws Exception {   // :75
        throw new UnsupportedOperationException("the task parameter is required for this operation");
    }

    @Override
    public final void messageReceived(final Request request, final TransportChannel channel, Task task) throws Exception {  // :80
        // We already got the task created on the network layer - no need to create it again on the transport layer
        execute(task, request, new ChannelActionListener<>(channel, actionName, request));  // :83
    }
}
```

**★ 关键点（原文遗漏）**：

- 两参版本**直接抛异常**——因为 6.x 要求所有 action 都能被 `_tasks` 跟踪，必须带 task。
- task **不是这里创建的**：它是网络层 `RequestHandlerRegistry.processMessageReceived`
  调 `taskManager.register(channel.getChannelType(), action, request)` 创建的（`RequestHandlerRegistry.java:60`），
  所以注释写的是 "We already got the task created on the network layer"。
  这也是为什么这里调的是 `execute(task, ...)`（`:127`）而不是 `execute(request, ...)`（`:69`）——
  **跨节点路径不会重复注册 Task**。
- 同理，`TransportMasterNodeAction` 把两参 `doExecute` 也禁用了（`TransportMasterNodeAction.java:134-137`）。

### 4.3 `ChannelActionListener`：把响应写回网络

```java
public static final class ChannelActionListener<Response extends TransportResponse, Request extends TransportRequest>
        implements ActionListener<Response> {                       // :87
    @Override
    public void onResponse(Response response) {
        try {
            channel.sendResponse(response);            // ★ 写回网络（:103）
        } catch (Exception e) {
            onFailure(e);
        }
    }

    @Override
    public void onFailure(Exception e) {
        try {
            channel.sendResponse(e);                   // ★ 异常也能写回（见 05 章异常跨节点，:112）
        } catch (Exception e1) {
            logger.warn(() -> new ParameterizedMessage(
                "Failed to send error response for action [{}] and request [{}]", actionName, request), e1);
        }
    }
}
```

注意这里的 `channel` 在网络路径上是被 `TaskTransportChannel` 包过的（见 8.3），
`sendResponse` 会顺带 `taskManager.unregister(task)`（`TaskTransportChannel.java:52-61`）。

### 4.4 子类只需实现 `doExecute`

```java
@Override
protected void doExecute(Task task, IndexRequest request, ActionListener<IndexResponse> listener) { ... }
```

`TransportAction.doExecute(Task, ...)` 的默认实现会转发到两参版本（`:142-144`），
所以只写两参 `doExecute(Request, ActionListener)` 也可以——但那样就拿不到 task 了。

---

## 5. 装配：`ActionModule` 与 `ActionHandler`

### 5.1 注册表：`setupActions`（`ActionModule.java:399`）

```java
static Map<String, ActionHandler<?, ?>> setupActions(List<ActionPlugin> actionPlugins) {   // :399
    class ActionRegistry extends NamedRegistry<ActionHandler<?, ?>> {
        public void register(ActionHandler<?, ?> handler) {
            register(handler.getAction().name(), handler);       // ★ key 就是 actionName
        }
        public <Request extends ActionRequest, Response extends ActionResponse> void register(
                GenericAction<Request, Response> action,
                Class<? extends TransportAction<Request, Response>> transportAction,
                Class<?>... supportTransportActions) {          // ★ 附属 action（如 [p]/[r]）
            register(new ActionHandler<>(action, transportAction, supportTransportActions));
        }
    }
    ActionRegistry actions = new ActionRegistry();

    actions.register(MainAction.INSTANCE, TransportMainAction.class);                       // :418
    actions.register(NodesInfoAction.INSTANCE, TransportNodesInfoAction.class);             // :419
    ...
    actions.register(FieldCapabilitiesAction.INSTANCE, TransportFieldCapabilitiesAction.class,
        TransportFieldCapabilitiesIndexAction.class);                                       // :506 ★ 带 support action
    ...
    actionPlugins.stream().flatMap(p -> p.getActions().stream()).forEach(actions::register); // :514 ★ 插件扩展点
    return unmodifiableMap(actions.getRegistry());                                          // :522
}
```

**`supportTransportActions` 的用途**：
`TransportReplicationAction` 需要注册**多个** action name：
- 顶层 `indices:data/write/index`
- 主分片 `indices:data/write/index[p]`
- 副本 `indices:data/write/index[r]`

这些"附属 action"类也要被实例化（否则不会向 `TransportService` 注册 handler），
所以 `ActionHandler` 会额外绑定它们（`ActionModule.java:689-691`）。

### 5.2 `configure()`：Guice MapBinder 装配（`:672`）

```java
@Override
protected void configure() {                                          // :672
    bind(ActionFilters.class).toInstance(actionFilters);               // :673
    bind(DestructiveOperations.class).toInstance(destructiveOperations);
    if (false == transportClient) {
        // register GenericAction -> transportAction Map used by NodeClient
        MapBinder<GenericAction, TransportAction> transportActionsBinder
                = MapBinder.newMapBinder(binder(), GenericAction.class, TransportAction.class);   // :683
        for (ActionHandler<?, ?> action : actions.values()) {
            bind(action.getTransportAction()).asEagerSingleton();       // :687 ★ 饿汉单例
            transportActionsBinder.addBinding(action.getAction()).to(action.getTransportAction()).asEagerSingleton();  // :688
            for (Class<?> supportAction : action.getSupportTransportActions()) {
                bind(supportAction).asEagerSingleton();                // :690
            }
        }
    }
}
```

**★ 为什么要 `asEagerSingleton`？**
因为 `TransportAction` 的构造器副作用就是**向 `TransportService` 注册 handler**。
如果不饿汉实例化，只在第一次被 `NodeClient` 用到时才创建，那这个节点就收不到别人发来的该 action 请求。
`bind(...).asEagerSingleton()` 保证节点启动即注册完毕。

### 5.3 `NodeClient` 如何拿到这张表

```java
// Node.java:615
client.initialize(injector.getInstance(new Key<Map<GenericAction, TransportAction>>() {}),
        () -> clusterService.localNode().getId(), transportService.getRemoteClusterService());
```

```java
// NodeClient.java
private Map<GenericAction, TransportAction> actions;                    // :46

public void initialize(Map<GenericAction, TransportAction> actions, Supplier<String> localNodeId,
                       RemoteClusterService remoteClusterService) {     // :58
    this.actions = actions;
}

@Override
public <...> void doExecute(Action<Request, Response, RequestBuilder> action, Request request, ActionListener<Response> listener) {
    // Discard the task because the Client interface doesn't use it.
    executeLocally(action, request, listener);                          // :76
}

public <...> Task executeLocally(GenericAction<Request, Response> action, Request request, ActionListener<Response> listener) {
    return transportAction(action).execute(request, listener);          // :87 ★ 直接调用本地 TransportAction
}

private <...> TransportAction<Request, Response> transportAction(GenericAction<Request, Response> action) {   // :114
    if (actions == null) {
        throw new IllegalStateException("NodeClient has not been initialized");     // :116
    }
    TransportAction<Request, Response> transportAction = actions.get(action);
    if (transportAction == null) {
        throw new IllegalStateException("failed to find action [" + action + "] to execute");   // :120
    }
    return transportAction;
}
```

> **★ 关键结论**：`client.xxx()` **不经过网络**，它直接在**当前线程**调用本节点的 `TransportAction.execute(...)`（`:69` 那个重载）。
> 这也解释了为什么 `HandledTransportAction` 注册用 `Names.SAME`——
> 本地路径根本不经过 `TcpTransport.handleRequest`，所以 `executor` 参数对本地调用无影响。

**排障**：看到 `failed to find action [...] to execute` → 说明某个 action 没在 `ActionModule` 注册
（插件没装好 / 用了 `NodeClient` 但没调 `initialize` / Guice 未实例化）。

---

## 6. 转发：`TransportActionNodeProxy`

当一个 action 需要在**别的节点**上执行（例如 `TransportReplicationAction` 要把 shard 请求发给主分片所在节点），
它用 `TransportActionNodeProxy`：

```java
// server/src/main/java/org/elasticsearch/action/TransportActionNodeProxy.java
public class TransportActionNodeProxy<Request extends ActionRequest, Response extends ActionResponse> {   // :30
    private final TransportService transportService;
    private final GenericAction<Request, Response> action;
    private final TransportRequestOptions transportOptions;                    // :34

    public TransportActionNodeProxy(Settings settings, GenericAction<Request, Response> action, TransportService transportService) {  // :36
        this.action = action;
        this.transportService = transportService;
        this.transportOptions = action.transportOptions(settings);             // :39 ★ 来自 Action 描述符
    }

    public void execute(final DiscoveryNode node, final Request request, final ActionListener<Response> listener) {  // :42
        ActionRequestValidationException validationException = request.validate();      // :43 ★ 发送前再校验一次
        if (validationException != null) {
            listener.onFailure(validationException);
            return;
        }
        transportService.sendRequest(node, action.name(), request, transportOptions,
            new ActionListenerResponseHandler<>(listener, action.getResponseReader()));  // :48-49
    }
}
```

**★ 与旧版实现的区别（重要）**：6.6 里 proxy **不再自定义 `TransportResponseHandler`**，
而是复用 `ActionListenerResponseHandler`（`ActionListenerResponseHandler.java:36`）：

```java
public class ActionListenerResponseHandler<Response extends TransportResponse> implements TransportResponseHandler<Response> {
    @Override public void handleResponse(Response response) { listener.onResponse(response); }   // :47
    @Override public void handleException(TransportException e) { listener.onFailure(e); }        // :52
    @Override public String executor() { return ThreadPool.Names.SAME; }                          // :57 ★
    @Override public Response read(StreamInput in) throws IOException { return reader.read(in); } // :62
}
```

**`executor()` 的语义**：告诉 Transport 层「**响应回来后丢到哪个线程池执行回调**」，
即 `TcpTransport.java:1206` 的 `threadPool.executor(handler.executor()).execute(...)`。
`ActionListenerResponseHandler` 返回 `SAME`，意思是**在收到响应的那个 IO 线程上直接回调**，
不再切换——所以回调里同样不能干重活。

需要指定超时/压缩等选项时，改的是 `action.transportOptions(settings)`（即 `TransportRequestOptions`），
而不是 `executor()`。典型值：复制类 action 用带超时的 options，管理类用默认 `EMPTY`。

---

## 7. Action 的四种常见形态

### 7.1 形态一：`HandledTransportAction`（最常见）

**特征**：无需转发，本地就能完成（或内部自己决定转发）。

**典型代表**：`TransportBulkAction`、`TransportSearchAction`、`TransportIndexAction`（继承自 `TransportReplicationAction`）。

```java
public class TransportXXXAction extends HandledTransportAction<XXXRequest, XXXResponse> {
    @Override
    protected void doExecute(Task task, XXXRequest request, ActionListener<XXXResponse> listener) {
        // 立刻切到合适的线程池
        threadPool.executor(ThreadPool.Names.XXX).execute(() -> { ... });
    }
}
```

### 7.2 形态二：`TransportMasterNodeAction`（★ 链路最复杂）

**特征**：必须由 **master 节点**执行。
- 如果本节点是 master → 本地线程池执行
- 否则 → `transportService.sendRequest` 转发给 master，并通过 `ClusterStateObserver` 等待/重试

**典型代表**：`TransportCreateIndexAction`、`TransportClusterUpdateSettingsAction`、
`TransportPutMappingAction`、`TransportClusterRerouteAction`。

**必须实现的抽象方法**：

```java
protected abstract String executor();                                                    // :101 用哪个线程池跑 masterOperation
protected abstract Response newResponse();                                               // :110（已 @Deprecated，推荐 read(StreamInput)）
protected abstract void masterOperation(Request request, ClusterState state,
                                        ActionListener<Response> listener) throws Exception;   // :118
protected abstract ClusterBlockException checkBlock(Request request, ClusterState state);      // :131
protected boolean localExecute(Request request) { return false; }                        // :127 强制本地执行的逃生门
```

**★ 执行链路（`doExecute` → `AsyncSingleAction`）**：

```java
@Override
protected final void doExecute(final Request request, ActionListener<Response> listener) {     // :134
    logger.warn("attempt to execute a master node operation without task");
    throw new UnsupportedOperationException("task parameter is required for this operation");
}

@Override
protected void doExecute(Task task, final Request request, ActionListener<Response> listener) {  // :140
    new AsyncSingleAction(task, request, listener).start();
}
```

`AsyncSingleAction` 构造时就把父子任务串起来（`:154-156`）：

```java
if (task != null) {
    request.setParentTask(clusterService.localNode().getId(), task.getId());   // :155 ★ 跨节点保持父子关系
}
```

`start()` 建一个带超时的状态观察器（`:160-165`）：

```java
public void start() {
    ClusterState state = clusterService.state();
    this.observer = new ClusterStateObserver(state, clusterService, request.masterNodeTimeout(),
                                             logger, threadPool.getThreadContext());   // :163 ★ master_timeout 在这里生效
    doStart(state);
}
```

`doStart()` 的三条分支（`:167-243`）：

```java
final Predicate<ClusterState> masterChangePredicate = MasterNodeChangePredicate.build(clusterState);   // :169
final DiscoveryNodes nodes = clusterState.nodes();                                                     // :170
if (nodes.isLocalNodeElectedMaster() || localExecute(request)) {                                       // :171
    // ============ 分支 A：本节点就是 master ============
    final ClusterBlockException blockException = checkBlock(request, clusterState);                     // :173
    if (blockException != null) {
        if (!blockException.retryable()) {
            listener.onFailure(blockException);                    // 不可重试的 block（如索引只读）→ 直接失败
        } else {
            retry(blockException, newState -> { ... });            // 可重试 block → 等集群状态变化（:179）
        }
    } else {
        ActionListener<Response> delegate = new ActionListener<Response>() {                            // :191
            @Override public void onFailure(Exception t) {
                if (t instanceof FailedToCommitClusterStateException || t instanceof NotMasterException) {
                    retry(t, masterChangePredicate);               // ★ 发布状态失败/已卸任 → 等新 master 再试（:202）
                } else {
                    listener.onFailure(t);
                }
            }
        };
        threadPool.executor(executor).execute(new ActionRunnable<Response>(delegate) {                 // :208 ★ 切到业务线程池
            @Override protected void doRun() throws Exception {
                masterOperation(task, request, clusterState, delegate);                                // :211
            }
        });
    }
} else {
    // ============ 分支 B/C：本节点不是 master ============
    if (nodes.getMasterNode() == null) {
        retry(null, masterChangePredicate);                        // :218 没有 master → 等
    } else {
        DiscoveryNode masterNode = nodes.getMasterNode();
        final String actionName = getMasterActionName(masterNode);                                     // :221 兼容改名
        transportService.sendRequest(masterNode, actionName, request,
            new ActionListenerResponseHandler<Response>(listener, TransportMasterNodeAction.this::read) {   // :222-223
                @Override public void handleException(final TransportException exp) {
                    Throwable cause = exp.unwrapCause();
                    if (cause instanceof ConnectTransportException) {
                        retry(cause, masterChangePredicate);       // ★ 连不上旧 master → 等新 master（:232）
                    } else {
                        listener.onFailure(exp);
                    }
                }
            });
    }
}
```

重试与最终失败（`:245-266`）：

```java
private void retry(final Throwable failure, final Predicate<ClusterState> statePredicate) {
    observer.waitForNextChange(new ClusterStateObserver.Listener() {
        @Override public void onNewClusterState(ClusterState state) { doStart(state); }   // :250 ★ 重新走一遍 doStart
        @Override public void onClusterServiceClose() {
            listener.onFailure(new NodeClosedException(clusterService.localNode()));      // :255
        }
        @Override public void onTimeout(TimeValue timeout) {
            listener.onFailure(new MasterNotDiscoveredException(failure));                // :262 ★ 超时 → MasterNotDiscoveredException
        }
    }, statePredicate);
}
```

**排障对照表**：

| 现象 | 源码位置 | 含义 |
| --- | --- | --- |
| `MasterNotDiscoveredException` | `:262` | 等 master 超时（受 `master_timeout` 控制，默认 30s） |
| `ClusterBlockException`（如 `index read-only`） | `:173-177` | `checkBlock` 判定不可重试，直接失败 |
| `FailedToCommitClusterStateException` → 自动重试 | `:199-202` | master 发布集群状态失败，等下一轮 |
| `ConnectTransportException` → 自动重试 | `:227-232` | 旧 master 已下线，等新 master 选举 |

### 7.3 形态三：`TransportReplicationAction`

**特征**：写入类 action，需要在主分片执行 + 并发复制到副本分片。

详见 [13-写入链路（Index与Bulk）.md](./13-写入链路（Index与Bulk）.md)。

关键点（先记住）：
```java
protected abstract PrimaryResult shardOperationOnPrimary(Request request, IndexShard primary) throws Exception;
protected abstract ReplicaResult shardOperationOnReplica(Request request, IndexShard replica) throws Exception;
```
注册三个 action name：`action`、`action[p]`、`action[r]`（`TransportReplicationAction.java:155-162`）。

### 7.4 形态四：广播类

| 基类 | 行为 | 典型代表 |
| --- | --- | --- |
| `TransportBroadcastAction` | 广播到**所有分片**执行 | `TransportClusterSearchShardsAction` |
| `TransportBroadcastReplicationAction` | 广播到**所有分片**，但每个分片走主副本复制 | `TransportFlushAction`、`TransportRefreshAction`、`TransportSyncedFlushAction` |
| `TransportNodesAction` | 广播到**所有节点**执行 | `TransportNodesStatsAction`、`TransportNodesHotThreadsAction` |

`TransportNodesAction` 的源码值得读——它是理解「广播 + 聚合」的最佳范例。

---

## 8. ★★ 重点链路

本节把三条真实链路逐段拆开，每段都给出源码位置与**线程切换点**。
（`SAME` = 不切换，在当前线程继续跑。）

### 8.0 装配链路（启动时，只跑一次）

```
Node 启动
  └─ new ActionModule(...)                                    ActionModule.java:357
       ├─ actions = setupActions(actionPlugins)               :368 → :399
       │    ├─ 内置 action 全量注册                            :418 ~ :520
       │    └─ actionPlugins.getActions()                      :514  ★ 插件扩展点
       ├─ actionFilters = setupActionFilters(actionPlugins)    :369 → :525  ★ 收集所有 ActionFilter
       └─ restController = new RestController(..., nodeClient, ...)  :390
  └─ Guice configure()                                        :672
       ├─ bind(action.getTransportAction()).asEagerSingleton()         :687
       │    └─ 构造器副作用：transportService.registerRequestHandler(...)   HandledTransportAction:61/:69
       │         └─ new RequestHandlerRegistry(action, reader, taskManager, handler, SAME, false, canTrip)  TransportService:830
       │              └─ transport.registerRequestHandler(reg)         TransportService:832  ★ 写进 serverHandlers map
       └─ MapBinder<GenericAction, TransportAction>            :683-688  ★ 供 NodeClient 查表
  └─ client.initialize(injector.getInstance(Key<Map<GenericAction, TransportAction>>), ...)  Node.java:615
```

**要点**：`TransportAction` 的实例化和 handler 注册是**同一个动作**；
`_tasks` 能看到的 action 名，和 `TransportService` 能接收的 action 名，出自同一张表。

### 8.1 链路一：本地调用（REST 请求落在本节点）

以 `POST /index/_doc` 为例：

```
【http_server_worker 线程】
 1. RestIndexAction 的 lambda
      client.index(indexRequest, new RestStatusToXContentListener<>(channel))
 2. AbstractClient.index(...)  →  execute(IndexAction.INSTANCE, request, listener)      # 类型化便利方法
 3. NodeClient.doExecute(action, request, listener)                                     NodeClient.java:74
 4. NodeClient.executeLocally(action, request, listener)                                NodeClient.java:87
 5. transportAction(action) → actions.get(IndexAction.INSTANCE)                         NodeClient.java:118
      └─ 找不到 → IllegalStateException("failed to find action [...]")                  NodeClient.java:120
 6. TransportAction.execute(request, listener)              ★ 与远程路径的分叉点        TransportAction.java:69
      ├─ taskManager.register("transport", actionName, request)                         TransportAction.java:77
      │    └─ request.createTask(...) → 默认 new Task(...)                              TaskAwareRequest.java:52
      └─ 包装 listener：响应/失败时 taskManager.unregister(task)                         TransportAction.java:84/:90
 7. TransportAction.execute(task, request, listener)                                    TransportAction.java:127
      ├─ request.validate()      ★ 失败直接 onFailure，不进 filter 链                    TransportAction.java:128
      ├─ (可选) TaskResultStoringActionListener                                         TransportAction.java:135
      └─ new RequestFilterChain<>(this, logger).proceed(...)                            TransportAction.java:138-139
 8. RequestFilterChain.proceed                                                          TransportAction.java:161
      ├─ i=0 → filters[0].apply(...)     # 如 X-Pack Security 鉴权
      ├─ ...
      └─ i==filters.length → doExecute(task, request, listener)                         TransportAction.java:167
 9. TransportIndexAction.doExecute(...)  → 内部走 TransportReplicationAction 流程（见 13 章）
      ├─ 通常会 threadPool.executor(...).execute(...) 切走            ★ 线程在此切换
      ├─ acquirePrimaryOperationPermit → shardOperationOnPrimary → InternalEngine.index
      └─ 并发发 ReplicaRequest 到 in-sync 副本（走 8.3 的跨节点链路）
10. listener.onResponse(IndexResponse)
      ├─ taskManager.unregister(task)                     ★ 步骤 6 包装的 listener      TransportAction.java:84
      └─ RestStatusToXContentListener.buildResponse → BytesRestResponse → Netty write
```

**要点**：
- 全程**没有网络往返**，也没经过 `TcpTransport`。
- Task 由 `TransportAction.execute(Request, ActionListener)` 创建并**由它自己包装的 listener 注销**。
- 一旦 `doExecute` 内部切了线程池，`_tasks` 里看到的 start time 与回调线程就对不上了——这是正常的。

### 8.2 链路二：跨节点入站（本节点收到别人的请求）

承接 8.1 第 9 步发出的副本请求，或 8.4 的 master 转发请求：

```
【transport 线程（netty worker / bulk 等，取决于 profile）】
 1. TcpTransport.messageReceived(...)
      └─ handleRequest(channel, profileName, streamIn, requestId, ...)                  TcpTransport.java:1215
 2. handleRequest 内部
      ├─ action = stream.readString()                                                   :1224
      ├─ reg = getRequestHandler(action)     ★ 就是装配时注册的那张表                     :1231
      │    └─ 找不到 → ActionNotFoundTransportException                                 :1233
      ├─ if (reg.canTripCircuitBreaker()) 计入 in-flight 熔断，否则只累加                  :1235-1239
      ├─ transportChannel = new TcpTransportChannel(...)                                :1240
      ├─ request = reg.newRequest(stream)    ★ 反序列化（reader 来自注册时）               :1242
      └─ threadPool.executor(reg.getExecutor()).execute(new RequestHandler(reg, request, transportChannel))
                                                                                        :1246 ★ 线程切换点（SAME 则不切）
 3. RequestHandler.doRun() → reg.processMessageReceived(request, transportChannel)      TcpTransport.java:1288
 4. RequestHandlerRegistry.processMessageReceived                                       :59
      ├─ task = taskManager.register(channel.getChannelType(), action, request)         :60 ★ Task 在网络层创建
      └─ handler.messageReceived(request, new TaskTransportChannel(taskManager, task, channel), task)   :66
           └─ channel 被包一层：sendResponse 时会 unregister(task)
 5. HandledTransportAction.TransportHandler.messageReceived(request, channel, task)     HandledTransportAction.java:80
      └─ execute(task, request, new ChannelActionListener<>(channel, actionName, request))   :83
           ★ 注意：走的是 :127 的三参版本，不会再创建 Task
 6. TransportAction.execute(task, request, listener)                                    TransportAction.java:127
      └─ validate → filter 链 → doExecute(task, request, listener)   # 与 8.1 第 7 步之后完全一致
 7. doExecute 结束 → listener.onResponse / onFailure
      └─ ChannelActionListener → channel.sendResponse(response)                         HandledTransportAction.java:103
           └─ TaskTransportChannel.sendResponse → endTask() → taskManager.unregister(task)   TaskTransportChannel.java:52-61
           └─ 底层 TcpTransportChannel 写回网络
```

**要点（与本地链路的三处差异）**：
1. **Task 在网络层创建**（`RequestHandlerRegistry:60`），`channel.getChannelType()` 作为 type（如 `"transport"` / `"netty"`），
   所以 `TransportHandler` 走三参 `execute(task, ...)`，不重复注册；
2. **Task 在响应写出时注销**（`TaskTransportChannel`），而不是由 `TransportAction` 包装的 listener 注销；
3. 是否切线程由 `reg.getExecutor()` 决定，而 `HandledTransportAction` 一律注册 `SAME`
   ——也就是说**入站请求的 `validate + filter 链 + doExecute` 开头都跑在 transport 线程上**。

### 8.3 链路三：Master 转发

承接 8.1/8.2 中「本节点不是 master」的情况（详见 7.2）：

```
【发起方线程】
 1. TransportMasterNodeAction.doExecute(task, request, listener)                        :140
      └─ new AsyncSingleAction(task, request, listener).start()                         :141
 2. AsyncSingleAction 构造：request.setParentTask(localNodeId, task.getId())              :155 ★ 父子任务关联
 3. start()
      ├─ new ClusterStateObserver(state, clusterService, request.masterNodeTimeout(), ...)  :163
      └─ doStart(state)                                                                 :164
 4. doStart：不是 master 且有 master
      └─ transportService.sendRequest(masterNode, actionName, request,
             new ActionListenerResponseHandler<>(listener, this::read))                 :222
           └─ TransportService.sendRequest → getConnection(node) → sendRequestInternal  TransportService.java:524/537/597
                ├─ responseHandlers.add(new ResponseContext<>(responseHandler, connection, action))  :609 ★ 按 requestId 记账
                ├─ (若有 timeout) threadPool.schedule(options.timeout(), GENERIC, timeoutHandler)     :625
                └─ connection.sendRequest(requestId, action, request, options)                        :627
                     └─ 若 node 是自己 → localNodeConnection（本地优化，不走网络）        :553-558
【master 节点】
 5. 走 8.2 的入站链路 → TransportMasterNodeAction.doExecute(task, ...)
      └─ doStart：isLocalNodeElectedMaster == true                                      :171
           ├─ checkBlock(...)                                                           :173
           └─ threadPool.executor(executor()).execute(ActionRunnable → masterOperation)  :208-211 ★ 切到业务线程池
【发起方线程（收到响应时）】
 6. TcpTransport 收到响应 → responseHandlers.onResponseReceived(requestId)              TcpTransport.java:1117
      └─ threadPool.executor(handler.executor()).execute(() -> handler.handleResponse(...))
           ActionListenerResponseHandler.executor() == SAME                             ActionListenerResponseHandler.java:57
      └─ listener.onResponse(response)  → 一路回到最外层的 REST listener
```

**要点**：
- 转发用的是 `transportService.sendRequest`，不是 `TransportActionNodeProxy`
  （proxy 是给「发到**指定分片所在节点**」这类场景用的，见第 6 节）。
- 父子 Task 通过 `request.setParentTask(...)` 传递，所以 `_tasks` 里能看到
  "协调节点上的 task → master 节点上的子 task" 的层级。
- 响应回调默认在 IO 线程（`SAME`），因此 master 分支里特意用 `threadPool.executor(executor())` 把重活切走。

### 8.4 线程切换总表

| 环节 | 线程 | 由什么决定 |
| --- | --- | --- |
| REST 解析与 `client.xxx()` 调用 | `http_server_worker` | 06 章 HTTP 层 |
| `NodeClient.executeLocally` → `doExecute` 开头 | **调用方线程**（不切换） | 本地直调 |
| `HandledTransportAction` 入站 handler 分派 | transport 线程（`SAME` 不切） | `registerRequestHandler(..., ThreadPool.Names.SAME, ...)` |
| `doExecute` 内部的业务执行 | 各 action 自己选（write/search/management…） | `threadPool.executor(xxx).execute(...)` |
| `TransportMasterNodeAction.masterOperation` | `executor()` 返回值 | 子类实现 `TransportMasterNodeAction.java:101` |
| 转发响应回调 | IO 线程（`SAME`） | `ActionListenerResponseHandler.java:57` |
| 普通请求响应回调（自定义 handler 时） | `handler.executor()` | `TcpTransport.java:1206` |

---

## 9. `Task` 与 `TaskManager`

### 9.1 概念

| 概念 | 说明 |
| --- | --- |
| `Task` | 一次 action 执行的可观测单元（有 id、父 id、startTime、action、描述） |
| `TaskManager` | 管理和注册所有 Task；支持 `_tasks` API、取消、结果存储 |
| 父子关系 | 分布式调用的父子关系通过 **Task ID 传递**（`request.setParentTask(nodeId, taskId)`） |
| `TaskAwareRequest` | 请求可携带 `parentTask`，并决定 `createTask` 返回什么 |
| `CancellableTask` | 可被 `_cancel` 取消的 task（`SearchTask`、`ReplicationTask`、`BulkByScrollTask`） |

### 9.2 ★ Task 的完整生命周期（两条路径）

| | 本地路径 | 跨节点入站路径 |
| --- | --- | --- |
| 创建者 | `TransportAction.execute(Request, ActionListener)` | `RequestHandlerRegistry.processMessageReceived` |
| 创建位置 | `TransportAction.java:77` | `RequestHandlerRegistry.java:60` |
| `type` 参数 | `"transport"` | `channel.getChannelType()` |
| Task 是否可能为 null | 是（`request.createTask` 返回 null） | 是（同上） |
| 注销者 | `TransportAction` 包装的匿名 listener | `TaskTransportChannel.sendResponse` |
| 注销位置 | `TransportAction.java:84` / `:90` | `TaskTransportChannel.java:73` |
| 结果存储 | `TaskResultStoringActionListener`（`getShouldStoreResult()==true` 时） | 同左（共用 `:134` 的逻辑） |

注册本体（`TaskManager.java:100`）：

```java
public Task register(String type, String action, TaskAwareRequest request) {   // :100
    ...收集 taskHeaders（含 X-Opaque-Id），超过 maxHeaderSize 直接抛 IllegalArgumentException...
    Task task = request.createTask(taskIdGenerator.incrementAndGet(), type, action, request.getParentTask(), headers);  // :115
    if (task == null) {
        return null;                       // ★ 请求选择不被跟踪
    }
    if (task instanceof CancellableTask) {
        registerCancellableTask(task);     // :125 可取消任务单独一张表
    } else {
        tasks.put(task.getId(), task);     // :127
    }
    return task;
}
```

### 9.3 相关 API

```bash
GET _tasks                                  # 所有任务
GET _tasks?actions=indices:data/write/bulk  # 按 action 过滤
GET _tasks?detailed=true                    # 带描述
GET _tasks/<taskId>                         # 单个任务
POST _tasks/<taskId>/_cancel                # 取消（只对 CancellableTask 有效）
```

源码：`server/src/main/java/org/elasticsearch/tasks/`（`TaskManager`、`Task`、`RestListTasksAction` 等）。

### 9.4 `RequestFilterChain` 与 Task 的关系

`TransportAction.execute(request, listener)` 创建 Task，
然后传进 `execute(task, request, listener)`，一路传到 `doExecute(task, ...)`。
子类可以覆写三参版本来使用 task：

```java
@Override
protected void doExecute(Task task, MyRequest request, ActionListener<MyResponse> listener) {
    String taskId = task.getId();     // 用于日志关联
    ...
}
```

> **注意**：`ActionFilter` 在 `doExecute` **之前**执行，但 Task 已经在 `execute(Request, ...)` 里注册了。
> 所以被鉴权拒绝的请求在 `_tasks` 里也会短暂出现——排查权限问题时别以为看到了 task 就代表请求执行了。

---

## 10. `ActionFilter`：横切逻辑

```java
public interface ActionFilter {
    void apply(Task task, String action, ActionRequest request,
               ActionListener<?> listener, ActionFilterChain chain);
}
```

**注册方式**：插件通过 `ActionPlugin.getActionFilters()` 返回 `List<ActionFilter>`，
由 `ActionModule.setupActionFilters`（`:525`）收集。

**真实的 ActionFilter 例子**（6.6 内置/X-Pack）：

| Filter | 来源 | 作用 |
| --- | --- | --- |
| X-Pack Security 的鉴权 filter | `x-pack/plugin/security` | 检查用户是否有该 action 的权限 |
| `x-pack/plugin/security` 的审计 filter | 同上 | 记录审计日志 |
| `x-pack/plugin/monitoring` 的采集 filter | `x-pack/plugin/monitoring` | 记录每个 action 的耗时分布 |
| `deprecation` filter | `x-pack/plugin/deprecation` | 记录废弃 API 调用 |

**实现范式**：

```java
public class MyFilter implements ActionFilter {
    @Override
    public void apply(Task task, String action, ActionRequest request,
                      ActionListener<?> listener, ActionFilterChain chain) {
        if (allowed(request)) {
            chain.proceed(task, action, request, listener);      // ★ 继续链
        } else {
            listener.onFailure(new SecurityException("denied"));  // ★ 中断
        }
    }
}
```

**⚠️ 坑**：`apply` 里必须**恰好调用一次** `chain.proceed(...)` 或 `listener.onFailure(...)`。
调用两次 → `proceed was called too many times`；
一次都不调 → 请求永远挂起（客户端超时）。

---

## 11. 读 Action 代码的"三步法"

看到任何一个 `TransportXxxAction`，按这三步读：

**第 1 步：看它继承谁**

| 父类 | 含义 | 读的重点 |
| --- | --- | --- |
| `HandledTransportAction` | 本地或自定义转发 | `doExecute` |
| `TransportReplicationAction` | 写入类，走主副本 | `shardOperationOnPrimary` / `shardOperationOnReplica` / `newResponse` |
| `TransportMasterNodeAction` | 必须 master 执行 | `masterOperation` + `checkBlock` + `executor()` |
| `TransportNodesAction` | 广播到所有节点 | `newResponse` / `nodeOperation` / `reduce` |
| `TransportBroadcastReplicationAction` | 广播到所有分片（每个分片走复制） | `newShardRequest` / `shardOperationOnPrimary` |

**第 2 步：看它注册了哪些 `actionName`**
在构造器里搜 `transportService.registerRequestHandler`，或看 `TransportReplicationAction` 的 `:155-162`。

**第 3 步：看它的线程池**
- 入站分派：`registerRequestHandler` 的 `executor` 参数（`HandledTransportAction` 恒为 `SAME`）；
- 业务执行：`doExecute` 内部的 `threadPool.executor(...)`；
- 响应回调：`TransportResponseHandler.executor()`。
对照 04 章的线程池表。

---

## 12. 动手实验

**实验 7-1：打印一次 action 的完整 filter 链**
在 `TransportAction.execute(Task, Request, ActionListener)`（`:127`）打断点，
查看 `this.filters` 数组的内容，确认装了哪些 filter。
（未装 X-Pack 时应该只有很少的 filter；装了 X-Pack 后会有 Security/Monitoring 的。）

**实验 7-2：观察 Task 的注册与注销**
发一个慢查询（如大聚合），同时 `GET /_tasks?detailed=true`，
观察 Task 的 action 名、父任务、耗时。
分别在 `TransportAction.java:77`（本地）和 `RequestHandlerRegistry.java:60`（入站）打断点，
对比 `type` 参数与注销时机（`TransportAction.java:84` vs `TaskTransportChannel.java:73`）。

**实验 7-3：观察 actionName 命名规范**
在 `ActionModule.setupActions`（`:399`）返回处打断点，列出所有注册的 actionName，
验证 2.1 节的命名规范；再故意用一个非法名字调 `registerRequestHandler`，看 `validateActionName`（`TransportService.java:789`）的报错。

**实验 7-4：验证 `validate()` 的作用**
发 `POST /_search` 但 body 里 `{"query":{"invalid":{}}}`，
观察是在 `TransportAction.execute`（`:128`）就被拒绝，还是在更深的解析层被拒绝。
再试 `POST /idx/_doc` 不带 body（`IndexRequest.validate` 会怎样）。

**实验 7-5：实现一个自定义 Action（★ 必做）**
参考 `plugins/examples`，实现：
```java
// 1) Request
public class MyRequest extends ActionRequest {
    String name;
    public MyRequest() {}                    // ★ 必须有空构造器（反序列化用）
    public MyRequest(String name) { this.name = name; }
    @Override public void writeTo(StreamOutput out) throws IOException { super.writeTo(out); out.writeString(name); }
    // 或实现 Writeable.Reader<MyRequest> 的构造器：public MyRequest(StreamInput in) { ... }
    @Override public ActionRequestValidationException validate() { return null; }
}

// 2) Response
public class MyResponse extends ActionResponse {
    String greeting;
    ...
}

// 3) Action
public class MyAction extends Action<MyRequest, MyResponse, MyRequestBuilder> {
    public static final MyAction INSTANCE = new MyAction();
    public static final String NAME = "cluster:admin/my/action";    // ★ 遵循命名规范
    ...
}

// 4) TransportAction
public class TransportMyAction extends HandledTransportAction<MyRequest, MyResponse> {
    @Inject
    public TransportMyAction(Settings settings, ThreadPool threadPool, TransportService transportService,
                             ActionFilters actionFilters, IndexNameExpressionResolver resolver) {
        super(settings, MyAction.NAME, threadPool, transportService, actionFilters, resolver, MyRequest::new);
    }
    @Override
    protected void doExecute(MyRequest request, ActionListener<MyResponse> listener) {
        listener.onResponse(new MyResponse("hello " + request.name));
    }
}

// 5) 在 ActionPlugin.getActions() 里注册
```

**实验 7-6：观察 ActionFilter 中断行为**
写一个 filter，在特定条件下 `listener.onFailure(...)` 而不调用 `chain.proceed`，
验证请求返回错误且 `doExecute` 没有被执行。

**实验 7-7：观察 Master 转发与重试**
在 `TransportMasterNodeAction.doStart`（`:167`）打断点，从非 master 节点发一个建索引请求，
观察走 `:222` 的转发分支；然后停掉 master，观察 `:227-232` 的 `ConnectTransportException` 重试
与 `:262` 的 `MasterNotDiscoveredException`。

---

## 13. 自测题

1. `TransportAction` 有哪几个 `execute` 重载？`NodeClient` 走的是哪个？为什么？
2. `RequestFilterChain`（`:148`）为什么用 `AtomicInteger` 而不是递归？`proceed was called too many times` 说明什么？
3. `ActionFilter` 在 `doExecute` **之前**还是**之后**执行？X-Pack 鉴权靠哪个机制？
4. `HandledTransportAction` 注册 handler 用 `ThreadPool.Names.SAME` 意味着什么？`doExecute` 里该怎么做？
5. `NodeClient.executeLocally` 会走网络吗？为什么？
6. 跨节点入站时，Task 是谁创建的？谁负责注销？和本地路径有什么不同？
7. `HandledTransportAction.TransportHandler` 的两参 `messageReceived` 为什么直接抛异常？
8. `TransportMasterNodeAction` 如何决定是自己执行还是转发？非 master 且无 master 时会怎样？
9. `ActionListenerResponseHandler.executor()` 返回什么？它对回调线程意味着什么？
10. `TransportReplicationAction` 注册了哪三个 action name？分别对应什么？
11. `ActionRequest.validate()` 在什么时机被调用？返回非 null 会怎样？
12. `ActionHandler` 的 `supportTransportActions` 参数是干什么的？为什么必须 `asEagerSingleton`？
13. 为什么 `actionName` 的命名规范（`indices:data/write/*`）很重要？
14. `master_timeout` 在 `TransportMasterNodeAction` 的哪一行生效？超时后抛什么异常？

---

## 14. 关键类速查

| 类 | 路径 | 一句话 |
| --- | --- | --- |
| `Action` / `GenericAction` | `action/` | 动作描述符（单例 + name + 类型） |
| `ActionRequest` / `ActionResponse` | `action/` | 请求/响应基类 |
| `ActionListener` | `action/ActionListener.java` | 异步回调（只能回调一次） |
| `TransportAction` | `action/support/TransportAction.java` | ★ 执行骨架（`:69` 本地入口、`:127` 统一入口、`:148` 过滤器链） |
| `HandledTransportAction` | `action/support/HandledTransportAction.java` | ★ 自动注册 handler（`:61`/`:69`），`TransportHandler` 只用三参方法（`:80`） |
| `TransportActionNodeProxy` | `action/TransportActionNodeProxy.java` | 把请求发到指定节点的指定 action（`:42`） |
| `ActionListenerResponseHandler` | `action/ActionListenerResponseHandler.java` | 响应回调适配器，`executor()` 恒为 `SAME`（`:57`） |
| `ActionFilter` | `action/ActionFilter.java` | 横切逻辑（X-Pack 鉴权） |
| `ActionModule` | `action/ActionModule.java` | ★ 注册表（`:399`）+ Guice 装配（`:672`）+ REST 注册（`:530`） |
| `NodeClient` | `client/node/NodeClient.java` | ★ 本地执行（`:87` `executeLocally`，`:114` 查表） |
| `AbstractClient` | `client/support/AbstractClient.java` | 类型化便利方法（`client.index()` 等） |
| `TransportService` | `transport/TransportService.java` | handler 注册（`:824`/`:845`）、`sendRequest`（`:524`） |
| `RequestHandlerRegistry` | `transport/RequestHandlerRegistry.java` | ★ 入站入口：创建 Task（`:60`）+ 调 handler（`:66`） |
| `TaskTransportChannel` | `transport/TaskTransportChannel.java` | 响应写出时注销 Task（`:73`） |
| `TcpTransport` | `transport/TcpTransport.java` | 入站分派（`:1215`、`:1246`） |
| `TaskManager` / `Task` | `tasks/` | 任务管理（`_tasks` API），`register`（`:100`） |

---

**上一篇**：[06-REST层与HTTP请求入口.md](./06-REST层与HTTP请求入口.md)　
**下一篇**：[08-ClusterState与集群元数据.md](./08-ClusterState与集群元数据.md)
