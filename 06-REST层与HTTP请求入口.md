# 06 · REST 层与 HTTP 请求入口

> 目标：搞懂「一个 HTTP 请求如何从 Netty 变成一次 `client.xxx()` 调用」。
> 这是所有排障的起点——客户给你一个 curl，你要知道它最终落到哪个类。

---

## 1. 分层结构

```
┌───────────────────────────── HTTP 层（modules/transport-netty4）───────────────────┐
│  Netty4HttpServerTransport                                                          │
│    ├─ Netty4HttpRequestHandler     （HTTP 请求 → RestRequest / RestChannel）         │
│    ├─ Netty4HttpResponseHandler    （RestResponse → HttpResponse）                   │
│    ├─ Netty4HeadersHandler                                                          │
│    └─ Netty4OpenChannelsHandler    （连接跟踪，用于 _nodes/stats/http）              │
└───────────────────────────────────┬────────────────────────────────────────────────┘
                                    │ HTTP 抽象（server/http）
┌───────────────────────────────────▼────────────────────────────────────────────────┐
│  HttpServerTransport  ·  HttpInfo  ·  HttpStats  ·  BoundHttpServerChannel           │
│  RestRequest  ·  RestChannel  ·  RestResponse  ·  BytesRestResponse                  │
└───────────────────────────────────┬────────────────────────────────────────────────┘
                                    │ REST 抽象（server/rest）
┌───────────────────────────────────▼────────────────────────────────────────────────┐
│  RestController        ★ 路由与分发                                                 │
│  RestHandler / BaseRestHandler  ★ 业务处理模板                                       │
│  RestRequest           ★ 参数解析                                                   │
│  RestResponse / BytesRestResponse  ★ 响应生成                                        │
│  MethodHandlers / PathTrie  ★ 路径匹配（支持 /{index}/{type}）                        │
└───────────────────────────────────┬────────────────────────────────────────────────┘
                                    │ 业务 Action（server/action/.../RestXxxAction）
                              client.xxx(...)
```

**关键文件清单**

| 文件 | 路径 |
| --- | --- |
| `RestController` | `server/src/main/java/org/elasticsearch/rest/RestController.java` |
| `RestHandler` | `server/src/main/java/org/elasticsearch/rest/RestHandler.java` |
| `BaseRestHandler` | `server/src/main/java/org/elasticsearch/rest/BaseRestHandler.java` |
| `RestRequest` | `server/src/main/java/org/elasticsearch/rest/RestRequest.java` |
| `RestChannel` | `server/src/main/java/org/elasticsearch/rest/RestChannel.java` |
| `BytesRestResponse` | `server/src/main/java/org/elasticsearch/rest/BytesRestResponse.java` |
| `PathTrie` / `MethodHandlers` | `server/src/main/java/org/elasticsearch/rest/` |
| `HttpServerTransport` | `server/src/main/java/org/elasticsearch/http/HttpServerTransport.java` |
| `Netty4HttpServerTransport` | `modules/transport-netty4/src/main/java/org/elasticsearch/http/netty4/Netty4HttpServerTransport.java` |
| `Netty4HttpRequestHandler` | `modules/transport-netty4/src/main/java/org/elasticsearch/http/netty4/Netty4HttpRequestHandler.java` |
| `ActionModule` | `server/src/main/java/org/elasticsearch/action/ActionModule.java` |

---

## 2. Netty 层：从 Socket 到 `RestRequest`

### 2.1 Pipeline

`Netty4HttpServerTransport` 的 channel pipeline：

```
"request_decoder"  → Netty4HttpRequestDecoder（继承 HttpRequestDecoder）
"request_aggregator" → HttpObjectAggregator（把 HttpChunk 聚合成 FullHttpRequest）
"chunk_aggregator"?  → 6.6 里用了 Netty4HttpRequestHandler 直接处理
"handler"          → Netty4HttpRequestHandler
"response_encoder" → Netty4HttpResponseEncoder
```

### 2.2 `Netty4HttpRequestHandler`

```java
@Override
protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
    // 1) 若开启 SSL/handshake，先处理
    // 2) 组装 Netty4HttpRequest / Netty4HttpChannel
    // 3) ★ 交给 HttpServerTransport 处理
    serverTransport.incomingRequest(request, channel);
}
```

### 2.3 `Netty4HttpServerTransport.incomingRequest`

关键点（`Netty4HttpServerTransport.java`）：

1. **构造 `RestRequest`**（`netty4Request` + `httpRequest`）
2. **★ 线程切换**：把处理交给 `http_server_worker` 线程池

```java
// 简化
if (HTTP_CORS_ENABLED...) { ... }
final Netty4HttpChannel nettyChannel = ...;
try {
    httpServerExecutor.execute(() -> {
        try {
            dispatchRequest(request, nettyChannel);
        } catch (...) { ... }
    });
} catch (EsRejectedExecutionException e) {
    // ★ 线程池满 → 429 Too Many Requests
    ...
}
```

**这就是 HTTP 层的背压机制**：`http_server_worker` 队列满了（默认队列 1000）会拒绝，
客户端收到 `429`。对应源码在 `Netty4HttpServerTransport.dispatchRequest` 的
`EsRejectedExecutionException` 分支。

3. **异常兜底**：
```java
} catch (Throwable t) {
    logger.warn("http request had unhandled exception", t);
    channel.sendResponse(new BytesRestResponse(channel, t));
}
```

### 2.4 相关设置

| 设置 | 默认 | 含义 |
| --- | --- | --- |
| `http.port` | 9200-9299 | 端口范围 |
| `http.host` / `network.host` | 127.0.0.1 | 绑定地址 |
| `http.max_content_length` | 100mb | ★ 请求体上限 |
| `http.max_initial_line_length` | 4kb | 请求行长度 |
| `http.max_header_size` | 8kb | header 总大小 |
| `http.compression` | true | 响应压缩（Accept-Encoding） |
| `http.compression_level` | 3 | 压缩级别 |
| `http.cors.enabled` | false | 跨域 |
| `http.cors.allow-origin` / `allow-methods` / `allow-headers` / `allow-credentials` / `max-age` | — | CORS 细节 |
| `http.detailed_errors.enabled` | true | 是否返回详细错误（`error_trace`） |
| `http.pipelining.max_events` | 10000 | HTTP pipelining 队列 |
| `http.tcp.no_delay` | true | |
| `http.tcp.keep_alive` | true | |
| `http.tcp.reuse_address` | true | |
| `http.tcp.send_buffer_size` / `receive_buffer_size` | -1 | |
| `http.content_type.required` | false | 是否强制 `Content-Type` |
| `http.publish_port` / `http.publish_host` | — | 对外公布的地址（NAT 场景） |
| `http.reset_on_close` | — | |

**排障**：客户端看到 `413 Request Entity Too Large` → `http.max_content_length` 太小。
在 `Netty4HttpServerTransport` 里搜这个设置，就能找到精确的判定位置。

---

## 3. `RestController`：路由与分发

### 3.1 数据结构

```java
public class RestController implements HttpServerTransport.Dispatcher {
    private final PathTrie<MethodHandlers> handlers = new PathTrie<>(RestUtils.REST_DECODER);   // ★ 路径树
    private final UnaryOperator<RestHandler> handlerWrapper;      // ★ handler 包装（X-Pack 安全在这里插入！）
    private final NodeClient client;
    private final CircuitBreakerService circuitBreakerService;
    private final UsageService usageService;
    ...
}
```

| 结构 | 作用 |
| --- | --- |
| `PathTrie<MethodHandlers>` | ★ 支持 `/{index}/{type}/_search` 这类带占位符的路径；`insertOrUpdate` 支持同一路径注册多种 HTTP 方法 |
| `MethodHandlers` | 「同一路径 + 多方法」的容器，内部是 `Map<RestRequest.Method, RestHandler>` |
| `handlerWrapper` | ★ **AOP 钩子**。X-Pack Security 用它给所有 handler 套上鉴权逻辑！ |

### 3.2 注册（`:151`）

```java
public void registerHandler(RestRequest.Method method, String path, RestHandler handler) {
    if (handler instanceof BaseRestHandler) {
        usageService.addRestHandler((BaseRestHandler) handler);       // 用于 _nodes/usage
    }
    handlers.insertOrUpdate(path, new MethodHandlers(path, handler, method), (mHandlers, newMHandler) -> {
        return mHandlers.addMethods(handler, method);                  // 已存在 → 追加方法
    });
}
```

**调用时机**：`Node.java:620` 的 `actionModule.initRestHandlers(...)`。
所以 **REST 路由在 `Node` 构造阶段就全部注册完毕**，`Node.start()` 时 HTTP 端口还没开。

**重复路径会怎样？** `insertOrUpdate` 会合并方法，
如果**同一路径同一方法**注册两次，会抛 `IllegalArgumentException`。

### 3.3 分发 `dispatchRequest`（`:168`）

```java
@Override
public void dispatchRequest(RestRequest request, RestChannel channel, ThreadContext threadContext) {
    if (request.rawPath().equals("/favicon.ico")) {                   // ① 特例
        handleFavicon(request, channel);
        return;
    }
    try {
        tryAllHandlers(request, channel, threadContext);              // ② 
    } catch (Exception e) {
        try {
            channel.sendResponse(new BytesRestResponse(channel, e));  // ③ 兜底
        } catch (Exception inner) {
            inner.addSuppressed(e);
            logger.error(() -> new ParameterizedMessage("failed to send failure response for uri [{}]", request.uri()), inner);
        }
    }
}
```

### 3.4 `tryAllHandlers`（`:316`）——逐段精读

```java
void tryAllHandlers(final RestRequest request, final RestChannel channel, final ThreadContext threadContext) throws Exception {
    // ① ★ 把 HTTP header 复制到 ThreadContext（配置项：http.tcp.* 之外，还有 headersToCopy）
    for (String key : headersToCopy) {
        String httpHeader = request.header(key);
        if (httpHeader != null) {
            threadContext.putHeader(key, httpHeader);
        }
    }

    boolean requestHandled = false;

    // ② error_trace 参数校验
    if (checkErrorTraceParameter(request, channel) == false) {
        channel.sendResponse(BytesRestResponse.createSimpleErrorResponse(channel, BAD_REQUEST,
            "error traces in responses are disabled."));
        return;
    }

    // ③ ★ 遍历所有匹配的 handler（PathTrie 可能返回多个候选）
    Iterator<MethodHandlers> allHandlers = getAllHandlers(request);
    for (Iterator<MethodHandlers> it = allHandlers; it.hasNext(); ) {
        final Optional<RestHandler> mHandler = Optional.ofNullable(it.next()).flatMap(mh -> mh.getHandler(request.method()));
        requestHandled = dispatchRequest(request, channel, client, mHandler);
        if (requestHandled) {
            break;
        }
    }

    // ④ 没有任何 handler 处理 → 400
    if (requestHandled == false) {
        handleBadRequest(request, channel);
    }
}
```

**`headersToCopy` 是什么？**
来自 `NetworkModule`/`RestController` 构造参数，默认包含
`X-Opaque-Id`、`traceparent` 等。实现由插件的 `ActionPlugin.getRestHeaders()` 提供。
**X-Pack Security 就是靠这个把 `Authorization` 头带进 ThreadContext 的。**

### 3.5 `dispatchRequest`（`:211`）——真正的处理

```java
boolean dispatchRequest(final RestRequest request, final RestChannel channel, final NodeClient client,
                        final Optional<RestHandler> mHandler) throws Exception {
    final int contentLength = request.hasContent() ? request.content().length() : 0;
    RestChannel responseChannel = channel;
    boolean requestHandled;

    if (contentLength > 0 && mHandler.map(h -> hasContentType(request, h) == false).orElse(false)) {
        // ① Content-Type 缺失或不支持 → 406 Not Acceptable
        sendContentTypeErrorMessage(request, channel);
        requestHandled = true;
    } else if (contentLength > 0 && mHandler.map(h -> h.supportsContentStream()).orElse(false) &&
        request.getXContentType() != XContentType.JSON && request.getXContentType() != XContentType.SMILE) {
        // ② 支持流式解析的 handler（如 _bulk、_msearch）只接受 JSON/SMILE
        channel.sendResponse(BytesRestResponse.createSimpleErrorResponse(channel, RestStatus.NOT_ACCEPTABLE,
            "Content-Type [" + request.getXContentType() + "] does not support stream parsing. Use JSON or SMILE instead"));
        requestHandled = true;
    } else if (mHandler.isPresent()) {
        try {
            // ③ ★★ 请求体也占熔断器额度！
            if (canTripCircuitBreaker(mHandler)) {
                inFlightRequestsBreaker(circuitBreakerService).addEstimateBytesAndMaybeBreak(contentLength, "<http_request>");
            } else {
                inFlightRequestsBreaker(circuitBreakerService).addWithoutBreaking(contentLength);
            }
            // ④ ★ 包装成 ResourceHandlingHttpChannel，响应时释放熔断额度
            responseChannel = new ResourceHandlingHttpChannel(channel, circuitBreakerService, contentLength);

            // ⑤ ★ 应用 handlerWrapper（X-Pack Security 的鉴权就在这里）
            final RestHandler wrappedHandler = mHandler.map(h -> handlerWrapper.apply(h)).get();
            wrappedHandler.handleRequest(request, responseChannel, client);
            requestHandled = true;
        } catch (Exception e) {
            responseChannel.sendResponse(new BytesRestResponse(responseChannel, e));
            requestHandled = true;
        }
    } else {
        // ⑥ 路径匹配上了但方法不对 → 405，并给出 Allow 头
        final Set<RestRequest.Method> validMethodSet = getValidHandlerMethodSet(request);
        if (validMethodSet.size() > 0 && validMethodSet.contains(request.method()) == false
            && request.method() != RestRequest.Method.OPTIONS) {
            handleUnsupportedHttpMethod(request, channel, validMethodSet);
            requestHandled = true;
        } else if (validMethodSet.contains(request.method()) == false && request.method() == RestRequest.Method.OPTIONS) {
            handleOptionsRequest(request, channel, validMethodSet);
            requestHandled = true;
        } else {
            requestHandled = false;
        }
    }
    return requestHandled;
}
```

**四个必须记住的点**：

| 标记 | 含义 | 排障价值 |
| --- | --- | --- |
| ① | 有请求体但没 `Content-Type` → 406 | 「为什么 PUT 报 406」 |
| ③ | **HTTP 请求体在解析前就占熔断额度** | `_nodes/stats/breaker` 的 `in_flight_requests`；大 body 的请求会被熔断 → 429 |
| ④ | `ResourceHandlingHttpChannel` 会在响应后**释放**额度 | 忘记释放会泄漏 → 熔断一直不下降 |
| ⑤ | `handlerWrapper` 是 X-Pack Security 的插入点 | 想知道「鉴权在哪做的」→ 搜 `RestController` 构造的 `handlerWrapper` |

### 3.6 `RestChannel` 家族

| 类 | 作用 |
| --- | --- |
| `RestChannel`（接口） | `sendResponse(RestResponse)`、`newBuilder()`、`request()`、`detailedErrorsEnabled()` |
| `Netty4HttpChannel` | Netty 实现（`modules/transport-netty4`） |
| `ResourceHandlingHttpChannel` | ★ 装饰器：响应时释放熔断额度 |
| `NullRestChannel` | 测试 / 丢弃响应时用 |

**`sendResponse` 的调用链**（`RestController.java:495`）：

```java
public void sendResponse(RestResponse restResponse) {
    // ... CORS 处理、日志
    channel.sendResponse(nettyResponse);       // → Netty writeAndFlush
}
```

---

## 4. `BaseRestHandler`：业务 handler 的模板

### 4.1 模板方法

```java
public abstract class BaseRestHandler extends AbstractComponent implements RestHandler {

    public static final Setting<Boolean> MULTI_ALLOW_EXPLICIT_INDEX =
        Setting.boolSetting("rest.action.multi.allow_explicit_index", true, Property.NodeScope);   // :56

    public abstract String getName();                                                   // :75

    @Override
    public final void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {   // :78
        // ① 预处理：各子类在这里解析参数、构造 XxxRequest
        final RestChannelConsumer action = prepareRequest(request, client);

        // ② ★ 参数校验：找出"没被消费"的参数
        final SortedSet<String> unconsumedParams = request.unconsumedParams().stream()
                .filter(p -> !responseParams().contains(p))
                .collect(Collectors.toCollection(TreeSet::new));

        if (!unconsumedParams.isEmpty()) {
            final Set<String> candidateParams = new HashSet<>();
            candidateParams.addAll(request.consumedParams());
            candidateParams.addAll(responseParams());
            throw new IllegalArgumentException(unrecognized(request, unconsumedParams, candidateParams, "parameter"));
        }

        usageCount.increment();                     // ③ 使用统计
        action.accept(channel);                     // ④ ★ 执行（发起 client.xxx()）
    }

    protected abstract RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException;  // :168

    protected Set<String> responseParams() { return Collections.emptySet(); }

    public boolean canTripCircuitBreaker() { return true; }

    public boolean supportsContentStream() { return false; }

    protected final void requestParamsConsumed(...) { ... }
}
```

### 4.2 ★ 参数校验机制（`unconsumedParams`）

这是 ES 一个很聪明的设计：

```java
// RestRequest 内部维护两个 Set
private final Set<String> consumedParams = new HashSet<>();
private final Set<String> unconsumedParams = new HashSet<>();

public String param(String key) {               // ★ 每次取参数都会"记账"
    consumedParams.add(key);
    unconsumedParams.remove(key);
    return params.get(key);
}
```

`handleRequest` 结束时检查 `unconsumedParams` 是否为空，非空就报：
```
request [/_search] contains unrecognized parameter: [foo]
```
（`BaseRestHandler.java:107` 的 `unrecognized` 方法）

**排障价值**：
客户写错参数名（如 `?refresh=truee`）报这个错，你能立刻告诉他「参数名拼错了」，
而不是去猜是哪个模块拒绝的。**这是 `BaseRestHandler` 而不是 `RestController` 做的**，
所以自定义 handler 只要继承 `BaseRestHandler` 就自动获得这个能力。

### 4.3 `prepareRequest` 的典型写法

以 `RestSearchAction` 为例：

```java
@Override
public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
    SearchRequest searchRequest = new SearchRequest();
    // ① 解析 URL 路径里的 index / type
    request.withContentOrSourceParamParserOrNull(parser -> parseSearchRequest(searchRequest, request, parser));
    // ② 解析 URL 查询参数（from/size/sort/...）—— 实际在 RestSearchAction.parseSearchRequest 里
    // ③ 解析 body（SearchSourceBuilder）
    // 返回一个 lambda，等 BaseRestHandler 调用它
    return channel -> client.search(searchRequest, new RestStatusToXContentListener<>(channel));
}
```

以 `RestBulkAction` 为例：

```java
@Override
public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
    BulkRequest bulkRequest = Requests.bulkRequest();
    String defaultIndex = request.param("index");
    String defaultType = request.param("type");
    String defaultRouting = request.param("routing");
    String waitForActiveShards = request.param("wait_for_active_shards");
    ...
    try {
        // ★ 流式解析 NDJSON（逐行读，不把整个 body 装进内存）
        if (request.hasContent()) {
            bulkRequest.add(request.content(), defaultIndex, defaultType, defaultRouting, null, ...);
        }
    } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(e.getMessage() + "...", e);
    }
    ...
    return channel -> client.bulk(bulkRequest, new RestStatusToXContentListener<>(channel));
}
```

**关键体会**：`prepareRequest` 只负责「把 HTTP 翻译成 `XxxRequest`」，
真正的执行是返回的 lambda。这是 Command 模式。

### 4.4 `supportsContentStream()`

只有 `true` 的 handler 才允许 `application/x-ndjson` 和流式解析。默认 `false`。
覆写为 `true` 的典型：`RestBulkAction`、`RestMultiSearchAction`、`RestMultiGetAction`（部分）。

**意义**：`_bulk` 可以处理 **GB 级** 的 body 而不 OOM，因为它逐行解析。
这也是为什么 ES 建议用 `_bulk` 而不是逐条 index。

---

## 5. `RestRequest`：参数解析

### 5.1 关键 API

| API | 说明 |
| --- | --- |
| `param(String)` | 取 URL 参数（会记账） |
| `paramAsInt/Boolean/Long/Float/Double(...)` | 类型化取参 |
| `param(String, String default)` | 带默认值 |
| `paramAsBoolean(String, boolean)` | |
| `hasParam(String)` | 是否存在 |
| **`param(String key, String defaultValue)` / `paramAsInt(key, default)`** | |
| `header(String)` | 取 HTTP header |
| `content()` | `BytesReference` 请求体 |
| `contentParser()` | ★ `XContentParser`（解析 body） |
| `withContentOrSourceParamParserOrNull(...)` | 有 body 或 `source` 参数时执行回调 |
| `source()` | `source` URL 参数（GET 带 body 的替代方案） |
| `getXContentType()` | `Content-Type` 对应的 `XContentType` |
| `rawPath()` / `path()` / `uri()` / `method()` | 路径与方法 |
| `consumedParams()` / `unconsumedParams()` | 参数记账（供 BaseRestHandler 校验） |
| `getAllHeaderValues(String name)` | 同名多值 header |
| `remoteAddress()` | 客户端地址（`RestRequest.remoteAddress()`） |

### 5.2 参数特殊规则

| 规则 | 说明 | 源码 |
| --- | --- | --- |
| **下划线参数** | 以 `_` 开头的参数会被**保留**（不报 unrecognized） | `RestUtils.checkUnrecognizedParameters` |
| **`error_trace`** | 控制是否返回堆栈 | `RestController.checkErrorTraceParameter:306` |
| **`filter_path`** | 响应字段过滤 | `RestResponse` / `FilterPathBasedFilter` |
| **`human` / `pretty`** | 人类可读格式 | `RestRequest` + `XContentBuilder` |
| **`format`** | 响应格式（json/yaml/smile/cbor） | `RestRequest` |
| **`v`** | 版本（已废弃） | |
| **`master_timeout` / `timeout`** | 超时 | `AcknowledgedRequest` |
| **`flat_settings`** | settings 是否扁平输出 | |

**`filter_path` 的实现**值得一看：`FilterPathBasedFilter`（`common/xcontent/`），
它能按 `a.b.c`、`a.*`、`**.name` 这样的模式过滤 JSON 输出。
这是「为什么 `_cluster/state?filter_path=x.y` 能只返回部分字段」的答案。

---

## 6. 响应生成

### 6.1 `RestStatusToXContentListener`（★ 最常用）

```java
// server/src/main/java/org/elasticsearch/rest/action/RestStatusToXContentListener.java
public class RestStatusToXContentListener<Response extends StatusToXContentObject> implements RestResponseListener<Response> {

    @Override
    public final RestResponse buildResponse(Response response, XContentBuilder builder) throws Exception {
        builder.startObject();
        response.toXContent(builder, getParams());
        builder.endObject();
        return new BytesRestResponse(response.status(), builder);
    }
}
```

**它做的事**：把 `XxxResponse` 的 `toXContent` 结果包成 JSON，HTTP 状态码取 `response.status()`。

**这就是「为什么 `_bulk` 里有失败 item 但 HTTP 还是 200」的答案**：
`BulkResponse.status()` 内部看 `hasFailures()`——只有**全部失败**才返回非 200。
（`BulkResponse.java` 的 `status()` 实现）

### 6.2 `BytesRestResponse`

| 构造器 | 用途 |
| --- | --- |
| `BytesRestResponse(RestStatus status)` | 空响应 |
| `BytesRestResponse(RestChannel channel, XContentBuilder)` | 带 body |
| `BytesRestResponse(RestChannel channel, Throwable)` | ★ **从异常生成响应** |
| `BytesRestResponse(RestStatus, String contentType, BytesReference)` | 原始字节 |

**异常 → HTTP 状态码的映射**在 `ExceptionsHelper.status(Throwable)`（`server/src/main/java/org/elasticsearch/ExceptionsHelper.java`）：

| 异常 | HTTP |
| --- | --- |
| `IndexNotFoundException` | 404 |
| `SearchPhaseExecutionException` | 400（或部分成功 200） |
| `VersionConflictEngineException` | 409 |
| `EsRejectedExecutionException` | 429 |
| `CircuitBreakingException` | 429 |
| `IllegalArgumentException` | 400 |
| `ParsingException` | 400 |
| `ElasticsearchTimeoutException` | 408（部分场景 504） |
| `ClusterBlockException` | 403 |
| `IndexShardNotStartedException` / `UnavailableShardsException` | 503 |
| `SecurityException`（X-Pack） | 401 / 403 |
| 其它 `ElasticsearchException` | 500 |

**这是排障第一课**：客户给你一个状态码，你能立刻缩小到几个异常类型，
再去搜异常的抛出点即可。

### 6.3 `RestResponseListener`

```java
public class RestResponseListener<Response> implements ActionListener<Response> {
    @Override
    public final void onResponse(Response response) {
        try {
            XContentBuilder builder = channel.newBuilder();
            RestResponse restResponse = buildResponse(response, builder);
            channel.sendResponse(restResponse);
        } catch (Exception e) {
            onFailure(e);
        }
    }
    @Override
    public final void onFailure(Exception exception) {
        try {
            channel.sendResponse(new BytesRestResponse(channel, exception));
        } catch (Exception inner) { ... }
    }
}
```

**设计要点**：`ActionListener` → `RestChannel` 的桥接。
所有 REST handler 都返回 `client.xxx(req, new RestXxxListener<>(channel))`，
异常最终被 `onFailure` 转成 `BytesRestResponse`。

---

## 7. ★ 一个 REST 请求的完整时序（串起 05 和 06）

以 `POST /myindex/_doc/1?refresh=true` 为例：

```
【Netty IO 线程】
1.  Netty4HttpRequestDecoder 解析 HTTP 字节流
2.  Netty4HttpRequestHandler.channelRead0
3.  Netty4HttpServerTransport.incomingRequest
      ├─ 构造 RestRequest + Netty4HttpChannel
      └─ httpServerExecutor.execute(() -> dispatchRequest(...))     ★ 切到 http_server_worker 池

【http_server_worker 线程】
4.  Netty4HttpServerTransport.dispatchRequest
5.  RestController.dispatchRequest(request, channel, threadContext)      (:168)
6.  RestController.tryAllHandlers                                       (:316)
      ├─ headersToCopy → threadContext.putHeader(...)                   # X-Opaque-Id 进上下文
      ├─ checkErrorTraceParameter
      └─ getAllHandlers(request) → PathTrie 匹配 "/myindex/_doc/{id}"
7.  RestController.dispatchRequest(request, channel, client, mHandler)   (:211)
      ├─ hasContentType 检查
      ├─ inFlightRequestsBreaker.addEstimateBytesAndMaybeBreak(...)      # 请求体占熔断
      ├─ responseChannel = new ResourceHandlingHttpChannel(...)
      ├─ handlerWrapper.apply(handler)                                   # X-Pack Security 鉴权
      └─ wrappedHandler.handleRequest(request, responseChannel, client)
8.  BaseRestHandler.handleRequest                                     (:78)
      ├─ prepareRequest(request, client)  → RestIndexAction 里构造 IndexRequest
      │     └─ 返回 RestChannelConsumer（lambda）
      ├─ 校验 unconsumedParams（拼错参数在此报错）
      └─ action.accept(channel)
9.  RestIndexAction 的 lambda:
      client.index(indexRequest, new RestStatusToXContentListener<>(channel))
10. NodeClient.index → AbstractClient.index → execute(IndexAction.INSTANCE, ...)
11. NodeClient.executeLocally(action, request, listener)
      └─ TransportAction.execute → doExecute                          ★ 见 07 章
```

之后进入 05 章描述的 Transport 流程（如果是本地 shard，走 `localNodeConnection`）。

**响应回来时**：

```
Transport 回包
  └─ ChannelActionListener.onResponse(IndexResponse)                （05 章 :101）
       └─ TcpTransportChannel.sendResponse(...)
            └─ Netty 写回 socket
或者（本地 action 失败）
  └─ RestStatusToXContentListener.onFailure(e)
       └─ new BytesRestResponse(channel, e)
            └─ RestController 里 sendResponse（:495）→ Netty writeAndFlush
```

---

## 8. 动手实验

**实验 6-1：数一数一共有多少 REST handler**
在 `Node.java:603`（`createInjector`）之后，遍历 `actionModule` 的 handlers 数量。
或者直接调 `GET /_nodes/usage`（需要 `UsageService`）观察所有已注册 handler 的名字。

**实验 6-2：验证 PathTrie 的匹配顺序**
注册一个 `GET /_my/{index}` 和 `GET /_my/static`，
请求 `/_my/static` 时观察哪个 handler 被调用（提示：静态优先）。

**实验 6-3：观察 handlerWrapper**
在 `RestController` 构造器里打印 `handlerWrapper` 的实现类。
装 X-Pack 后重启，观察它变成了什么（应该是 Security 的 wrapper）。

**实验 6-4：触发参数校验错误**
`GET /_search?foo=bar` → 应报 `request [/_search] contains unrecognized parameter: [foo]`。
在 `BaseRestHandler`（`:92`）打断点确认。

**实验 6-5：触发 405 与 406**
- `DELETE /_search` → 405，响应头里应有 `Allow: GET, POST`
- `PUT /myindex/_doc/1` 不带 `Content-Type` → 406

**实验 6-6：观察 HTTP 层熔断**
设置 `http.max_content_length=10mb`，发一个 20mb 的 body → 413。
再把 `network.breaker.inflight_requests.limit` 调很低，发一个大 body → 429（熔断）。

**实验 6-7：写一个自定义 REST handler（★ 必做）**
参考 `plugins/examples/rest-handler`：

```java
public class MyRestHandler extends BaseRestHandler {
    public MyRestHandler(Settings settings, RestController controller) {
        controller.registerHandler(RestRequest.Method.GET, "/_my/hello", this);
    }
    @Override public String getName() { return "my_hello_action"; }
    @Override protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        final String name = request.param("name", "world");
        return channel -> {
            XContentBuilder builder = channel.newBuilder();
            builder.startObject().field("hello", name).endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        };
    }
}
```
然后在 `Plugin` 里通过 `getRestHandlers(...)` 注册。

---

## 9. 自测题

1. HTTP 请求是在哪个线程池上被处理的？在哪里发生线程切换？
2. `RestController.handlers` 用 `PathTrie` 而不是 `Map`，为什么？
3. `handlerWrapper` 有什么用？谁会用？
4. `BaseRestHandler.handleRequest`（`:78`）里 `unconsumedParams` 校验解决什么问题？
5. `supportsContentStream()` 返回 true 的意义是什么？为什么 `_bulk` 要开它？
6. `ResourceHandlingHttpChannel` 解决了什么问题？不释放会怎样？
7. `filter_path` 的实现类叫什么？它支持哪些匹配语法？
8. `_bulk` 部分失败时 HTTP 状态码为什么是 200？逻辑在哪个类哪个方法？
9. `BytesRestResponse(channel, Throwable)` 是如何决定 HTTP 状态码的？
10. REST 路由是在 `Node` 的哪个阶段注册的？为什么不能更早或更晚？
11. 请求体为什么要占熔断器额度？是在解析**之前**还是之后？
12. 客户端拿到 406 和 400 分别代表什么？各自在源码的哪一行被抛出？

---

## 10. 关键类速查

| 类 | 路径 | 一句话 |
| --- | --- | --- |
| `RestController` | `rest/RestController.java` | 路由与分发（`:151` 注册、`:168` 分发） |
| `RestHandler` | `rest/RestHandler.java` | handler 接口 |
| `BaseRestHandler` | `rest/BaseRestHandler.java` | 模板方法 + 参数校验（`:78`） |
| `RestRequest` | `rest/RestRequest.java` | 参数解析 + 记账 |
| `RestChannel` / `BytesRestResponse` | `rest/` | 响应生成 |
| `PathTrie` / `MethodHandlers` | `rest/` | 路径匹配 |
| `RestStatusToXContentListener` | `rest/action/RestStatusToXContentListener.java` | Response → JSON（★ 最常用） |
| `RestResponseListener` | `rest/action/RestResponseListener.java` | ActionListener → RestChannel 桥接 |
| `HttpServerTransport` | `http/HttpServerTransport.java` | HTTP 抽象（`:dispatchRequest` / `dispatchBadRequest`） |
| `Netty4HttpServerTransport` | `modules/transport-netty4/.../netty4/Netty4HttpServerTransport.java` | Netty 实现 |
| `ExceptionsHelper` | `ExceptionsHelper.java` | 异常 → HTTP 状态码 |

---

**上一篇**：[05-Transport节点间通信层.md](./05-Transport节点间通信层.md)　
**下一篇**：[07-Action框架与请求处理链.md](./07-Action框架与请求处理链.md)
