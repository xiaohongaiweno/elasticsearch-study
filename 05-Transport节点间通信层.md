# 05 · Transport 节点间通信层

> 目标：彻底搞懂「一个 RPC 从节点 A 发到节点 B，再拿到响应」的每一步。
> 这是 ES 分布式能力的物理基础，也是排障时最常看的日志层。

---

## 1. 分层结构

```
┌──────────────────────────────────────────────────────────────────────────┐
│  业务层：TransportAction / TransportRequestHandler                        │
│     收：handler.messageReceived(request, channel, task)                   │
│     发：transportService.sendRequest(node, action, request, handler)      │
└─────────────────────────────────┬────────────────────────────────────────┘
                                  │
┌─────────────────────────────────▼────────────────────────────────────────┐
│  TransportService（server/transport/TransportService.java）               │
│   · 请求 ID 分配（responseHandlers）                                      │
│   · 超时管理（TimeoutHandler）                                            │
│   · 连接管理（ConnectionManager / getConnection）                          │
│   · 握手（Handshake）                                                     │
│   · 本地节点优化（localNodeConnection）                                    │
│   · 请求处理器注册表（requestHandlers / RequestHandlerRegistry）           │
│   · ThreadContext 恢复（ContextRestoreResponseHandler）                   │
└─────────────────────────────────┬────────────────────────────────────────┘
                                  │
┌─────────────────────────────────▼────────────────────────────────────────┐
│  Transport（抽象接口）→ TcpTransport（抽象实现）                            │
│   · header 编解码（TcpHeader）                                            │
│   · 压缩（CompressorFactory）                                             │
│   · 收发字节流（writeMessage / readMessage）                               │
│   · 连接建立/关闭（openConnection / CloseableChannel）                     │
│   · KeepAlive（TransportKeepAlive）                                       │
└─────────────────────────────────┬────────────────────────────────────────┘
                                  │
┌─────────────────────────────────▼────────────────────────────────────────┐
│  modules/transport-netty4                                                  │
│   Netty4Transport · Netty4MessageChannelHandler · Netty4TcpChannel         │
│   Netty4SizeHeaderFrameDecoder · Netty4OpenChannelsHandler                 │
│   底层：Netty NioEventLoopGroup（workerCount 个 IO 线程）                   │
└──────────────────────────────────────────────────────────────────────────┘
```

**关键文件清单**

| 文件 | 路径 |
| --- | --- |
| `Transport` | `server/src/main/java/org/elasticsearch/transport/Transport.java` |
| `TransportService` | `server/src/main/java/org/elasticsearch/transport/TransportService.java` |
| `TcpTransport` | `server/src/main/java/org/elasticsearch/transport/TcpTransport.java` |
| `TcpHeader` | `server/src/main/java/org/elasticsearch/transport/TcpHeader.java` |
| `TcpTransportChannel` | `server/src/main/java/org/elasticsearch/transport/TcpTransportChannel.java` |
| `TransportStatus` | `server/src/main/java/org/elasticsearch/transport/TransportStatus.java` |
| `ConnectionProfile` | `server/src/main/java/org/elasticsearch/transport/ConnectionProfile.java` |
| `TransportRequestOptions` | `server/src/main/java/org/elasticsearch/transport/TransportRequestOptions.java` |
| `RequestHandlerRegistry` | `server/src/main/java/org/elasticsearch/transport/RequestHandlerRegistry.java` |
| `TransportResponseHandler` | `server/src/main/java/org/elasticsearch/transport/TransportResponseHandler.java` |
| `RemoteTransportException` | `server/src/main/java/org/elasticsearch/transport/RemoteTransportException.java` |
| `Netty4Transport` | `modules/transport-netty4/src/main/java/org/elasticsearch/transport/netty4/Netty4Transport.java` |
| `Netty4MessageChannelHandler` | `modules/transport-netty4/src/main/java/org/elasticsearch/transport/netty4/Netty4MessageChannelHandler.java` |

---

## 2. 线协议（Wire Protocol）

### 2.1 消息结构

```
┌────────────────────────────────────────────────────────────────────────┐
│  Frame（Netty4SizeHeaderFrameDecoder 负责切分）                          │
│                                                                        │
│  ┌──────────────┬──────────────────────────────────────────────────┐   │
│  │ 长度前缀       │  Message Body                                    │   │
│  │ 4 bytes      │                                                  │   │
│  │ (int, 含标记) │  ┌──────────────────────────────────────────┐    │   │
│  └──────────────┘  │ TcpHeader（可变长）                       │    │   │
│                    │  ┌────────────────────────────────────┐   │    │   │
│                    │  │ requestId : 8 bytes (long)        │   │    │   │
│                    │  │ status    : 1 byte                │   │    │   │
│                    │  │ version   : 4 bytes (int)         │   │    │   │
│                    │  └────────────────────────────────────┘   │    │   │
│                    │  之后（仅请求）：                          │    │   │
│                    │   features : String[]      (≥6.3.0)      │    │   │
│                    │   action   : String         ★ 动作名      │    │   │
│                    │   ThreadContext headers（X-Opaque-Id 等）  │    │   │
│                    │   request 实体（由 action 对应的 Reader 解析）│   │   │
│                    └──────────────────────────────────────────┘    │   │
│                                                                        │
└────────────────────────────────────────────────────────────────────────┘
```

相关常量（`TcpHeader.java`）：

| 常量 | 含义 |
| --- | --- |
| `MARKER_BYTES_SIZE` | 帧标记（用于识别是否压缩 / 是否为 ES 协议） |
| `MESSAGE_LENGTH_SIZE` | 长度字段本身的大小（4 字节） |
| `HEADER_SIZE` | `requestId + status + version` 的总大小 |
| `REQUEST_ID_SIZE` = 8 | |
| `STATUS_SIZE` = 1 | |
| `VERSION_ID_SIZE` = 4 | |

### 2.2 `status` 字节的含义（`TransportStatus.java`）

| 常量 | 含义 |
| --- | --- |
| `STATUS_REQ` | 请求 |
| `STATUS_RES` | 正常响应 |
| `STATUS_ERROR` | 错误响应（body 是序列化的 Exception） |
| `STATUS_COMPRESSED` | 位标记，与上面按位组合 |

辅助判断方法：`TransportStatus.isRequest(status)`、`isResponse(status)`、
`isError(status)`、`isCompress(status)`、`isHandshake(status)`（带 `STATUS_REQ` 的握手特殊位）。

### 2.3 握手（Handshake）：为什么需要

问题：两个节点建立连接时，**双方可能版本不同**，但 header 里必须写 `version`。
如果 6.6 节点直接发 6.6 的 header，6.0 节点可能解析不了。

ES 的解法：
1. 建立连接后，双方先发一个 **HandshakeRequest**，其 `version` 字段填
   `currentVersion.minimumCompatibilityVersion()`（见 `TcpTransport.ensureVersionCompatibility:1150` 的注释）。
2. 收到握手包的一方回一个 `HandshakeResponse`，里面带上**自己的真实版本**和 features 集合。
3. 双方根据对端真实版本确定后续通信使用哪个版本。

```java
// TcpTransport.java:1311
public void executeHandshake(DiscoveryNode node, TcpChannel channel, ConnectionProfile profile,
                             ActionListener<Version> listener) {
    handshaker.sendHandshake(responseHandlers.newRequestId(), node, channel, profile.getHandshakeTimeout(), listener);
}
```

**版本兼容规则**（`TcpTransport.ensureVersionCompatibility:1150`）：

```java
final Version compatibilityVersion = isHandshake ? currentVersion.minimumCompatibilityVersion() : currentVersion;
if (version.isCompatible(compatibilityVersion) == false) {
    throw new IllegalStateException("Received ... message from unsupported version: [" + version + "] ...");
}
```

- 握手包：兼容到 **N-2**（`minimumCompatibilityVersion()`）
- 正常包：只兼容 **N-1** 和 **N**

> **排障**：日志里出现 `Received message from unsupported version` 就是这里抛的。
> 含义是「对端版本超出了滚动升级允许的范围」。

### 2.4 压缩

```java
// TcpTransport.messageReceived:1085
if (TransportStatus.isCompress(status) && hasMessageBytesToRead && streamIn.available() > 0) {
    Compressor compressor;
    try {
        final int bytesConsumed = TcpHeader.REQUEST_ID_SIZE + TcpHeader.STATUS_SIZE + TcpHeader.VERSION_ID_SIZE;
        compressor = CompressorFactory.compressor(reference.slice(bytesConsumed, reference.length() - bytesConsumed));
    } catch (NotCompressedException ex) {
        // 报了 compress 但实际不是 → 抛 IllegalStateException 并打印前 10 字节（方便排查）
        ...
    }
    streamIn = compressor.streamInput(streamIn);
}
```

- 开启方式：`transport.tcp.compress: true`（默认 false）
- 实现在 `server/src/main/java/org/elasticsearch/common/compress/`（`CompressorFactory`、`DeflateCompressor`）
- **注意**：压缩的是**整个 header 之后的 body**，不是 header。所以 `reference.slice(bytesConsumed, ...)`。
- 云建议：内网（同 AZ）不建议开，跨 AZ/跨区建议开（省带宽但耗 CPU）。

---

## 3. 连接模型：为什么有多个 Channel

### 3.1 `ConnectionProfile`

```java
// ConnectionProfile.java
public static final ConnectionProfile DEFAULT_PROFILE = ...;
// 6.6.1 的关键：按"用途"区分连接，避免互相阻塞
```

**6.6 定义了这些 profile**（见 `TransportService` / `ConnectionProfile` 的常量）：

| Profile 名 | 用途 | 默认连接数 |
| --- | --- | --- |
| `default` | 一般请求 | `transport.connections_per_node.default`（默认 6） |
| `bulk` | bulk 请求 | `transport.connections_per_node.bulk`（默认 3） |
| `ping` | 集群 ping | `transport.connections_per_node.ping`（默认 1） |
| `recovery` | 分片恢复 | `transport.connections_per_node.recovery`（默认 2） |
| `reg` | 注册/握手 | `transport.connections_per_node.reg`（默认 1） |
| `state` | 集群状态发布 | `transport.connections_per_node.state`（默认 1） |

**为什么要分开？** 因为一个 TCP 连接的写入是**有序**的。
如果 bulk 请求（大消息、慢）和 ping（小消息、必须快）共用一个 channel，
bulk 会把 ping 堵在后面，导致节点被误判为失联。

对应配置项（`TransportService.CONNECTIONS_PER_NODE_*`）：
```
transport.connections_per_node.recovery: 2
transport.connections_per_node.bulk: 3
transport.connections_per_node.reg: 1
transport.connections_per_node.state: 1
transport.connections_per_node.ping: 1
```

### 3.2 `TransportRequestOptions.Type` 与 profile 的对应

```java
// TransportRequestOptions.Type
RECOVERY → profile "recovery"
BULK     → profile "bulk"
REG      → profile "reg"
STATE    → profile "state"
PING     → profile "ping"
// 未指定 → "default"
```

源码：`TransportRequestOptions.java` 的 `type()` 与 `ConnectionProfile.buildDefaultConnectionProfile`。

### 3.3 `NodeChannels`（≈ 6.6 中的 `Connection` 内部持有）

`TcpTransport` 内部用一个 `Connection` 对象表示「到某个节点的连接集合」，
里面持有 `TcpChannel[] channels`（每个 profile 一个数组），
`channel(options.type())` 按类型选 channel：

```java
// TcpTransport.java:280
@Override
public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options)
    throws IOException, TransportException {
    if (isClosing.get()) {
        throw new NodeNotConnectedException(node, "connection already closed");
    }
    TcpChannel channel = channel(options.type());          // ★ 按类型选 channel
    sendRequestToChannel(this.node, channel, requestId, action, request, options, getVersion(), compress, (byte) 0);
}
```

### 3.4 打开连接

```java
// TcpTransport.java:297
@Override
public Releasable openConnection(DiscoveryNode node, ConnectionProfile profile,
                                 ActionListener<Transport.Connection> listener) {
    Objects.requireNonNull(profile, "connection profile cannot be null");
    if (node == null) {
        throw new ConnectTransportException(null, "can't open connection to a null node");
    }
    ConnectionProfile finalProfile = maybeOverrideConnectionProfile(profile);
    closeLock.readLock().lock();          // ★ 禁止在关闭过程中新建连接
    try {
        ensureOpen();
        List<TcpChannel> pendingChannels = initiateConnection(node, finalProfile, listener);
        return () -> CloseableChannel.closeChannels(pendingChannels, false);
    } finally {
        closeLock.readLock().unlock();
    }
}
```

`initiateConnection`（`:313`）会为 profile 里的每个 channel 建一条 TCP 连接，
并注册 `ChannelsConnectedListener`——**只有全部 channel 都连上，才认为连接建立成功**。

---

## 4. 发送一条 RPC 请求

### 4.1 `TransportService.sendRequest` 的三个重载

```java
// :512 —— 无 options
public <T extends TransportResponse> void sendRequest(final DiscoveryNode node, final String action,
                                                      final TransportRequest request,
                                                      final TransportResponseHandler<T> handler) {
    try {
        Transport.Connection connection = getConnection(node);          // ★ 先拿连接
        sendRequest(connection, action, request, TransportRequestOptions.EMPTY, handler);
    } catch (NodeNotConnectedException ex) {
        handler.handleException(ex);                                    // ★ 拿不到连接直接回调失败
    }
}

// :524 —— 带 options
public final <T extends TransportResponse> void sendRequest(final DiscoveryNode node, final String action,
                                                            final TransportRequest request,
                                                            final TransportRequestOptions options,
                                                            TransportResponseHandler<T> handler) { ... }

// :537 —— 直接给 connection
public final <T extends TransportResponse> void sendRequest(final Transport.Connection connection, final String action,
                                                            final TransportRequest request,
                                                            final TransportRequestOptions options,
                                                            TransportResponseHandler<T> handler) {
    try {
        asyncSender.sendRequest(connection, action, request, options, handler);
    } catch (NodeNotConnectedException ex) {
        handler.handleException(ex);
    }
}
```

`getConnection`（`:553`）：**本地节点优化**

```java
public Transport.Connection getConnection(DiscoveryNode node) {
    if (isLocalNode(node)) {
        return localNodeConnection;         // ★ 自己 → 内存直连，不走网络！
    } else {
        return connectionManager.getConnection(node);
    }
}
```

> **这是 ES 性能优化的关键点之一**：协调节点和数据节点是同一个进程时，
> shard 请求根本不经过 Netty，而是一个「内存 channel」。
> 所以「客户端直连数据节点」和「协调节点转发」在源码上是两条路。

### 4.2 `sendRequestInternal`：真正发出去（`TransportService.java:597`）

```java
private <T extends TransportResponse> void sendRequestInternal(final Transport.Connection connection, final String action,
                                                              final TransportRequest request,
                                                              final TransportRequestOptions options,
                                                              TransportResponseHandler<T> handler) {
    if (connection == null) { throw new IllegalStateException("can't send request to a null connection"); }
    DiscoveryNode node = connection.getNode();

    // 1) ★ 保存当前 ThreadContext，便于响应回来时恢复（X-Opaque-Id 等）
    Supplier<ThreadContext.StoredContext> storedContextSupplier = threadPool.getThreadContext().newRestorableContext(true);
    ContextRestoreResponseHandler<T> responseHandler = new ContextRestoreResponseHandler<>(storedContextSupplier, handler);

    // 2) ★ 分配 requestId，并登记 responseHandler（等待回包用）
    final long requestId = responseHandlers.add(new Transport.ResponseContext<>(responseHandler, connection, action));

    // 3) 超时管理
    final TimeoutHandler timeoutHandler;
    if (options.timeout() != null) {
        timeoutHandler = new TimeoutHandler(requestId, connection.getNode(), action);
        responseHandler.setTimeoutHandler(timeoutHandler);
    } else {
        timeoutHandler = null;
    }

    try {
        if (lifecycle.stoppedOrClosed()) {
            throw new TransportException("TransportService is closed stopped can't send request");
        }
        if (timeoutHandler != null) {
            timeoutHandler.future = threadPool.schedule(options.timeout(), ThreadPool.Names.GENERIC, timeoutHandler);   // ★ 超时任务
        }
        connection.sendRequest(requestId, action, request, options);   // 4) ★ 交给 TcpTransport.Connection
    } catch (final Exception e) {
        final Transport.ResponseContext contextToNotify = responseHandlers.remove(requestId);
        if (contextToNotify != null) {
            if (timeoutHandler != null) { timeoutHandler.cancel(); }
            // 通知调用方失败
            ...
        }
    }
}
```

**四个要点**：
1. **requestId 是全局自增的 long**，用于匹配请求与响应（`responseHandlers.newRequestId()`）。
2. **`responseHandlers`** 是一个 `Map<Long, ResponseContext>`（内部有超时清理），
   接收方回包时靠 requestId 找回 handler。
3. **超时是本地定时任务**（`threadPool.schedule`，`generic` 池）。
   超时后调 `handler.handleException(new NodeDisconnectedException(...))` 或 `ReceiveTimeoutTransportException`。
   → **注意：ES 不主动取消远端的执行**。超时只是本地放弃等待，远端可能还在跑。
4. **`ContextRestoreResponseHandler`** 保证响应回调在原来的 ThreadContext 上执行。

### 4.3 `Connection.sendRequest` → 序列化并写网络

```java
// TcpTransport.java:280（Connection 内部类）
@Override
public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options)
    throws IOException, TransportException {
    if (isClosing.get()) { throw new NodeNotConnectedException(node, "connection already closed"); }
    TcpChannel channel = channel(options.type());
    sendRequestToChannel(this.node, channel, requestId, action, request, options, getVersion(), compress, (byte) 0);
}
```

`sendRequestToChannel` 内部（`TcpTransport`）：

```java
private void sendRequestToChannel(DiscoveryNode node, TcpChannel channel, long requestId, String action,
                                 TransportRequest request, TransportRequestOptions options,
                                 Version version, boolean compress, byte status) throws IOException {
    ensureOpen();
    // 1) 计算 body 大小（先序列化到临时 buffer 或分两遍）
    int totalMessageSize = 0;
    ...
    try (BytesStreamOutput bytesStream = bytesStream()) {
        try (StreamOutput out = new TcpHeader...) { ... }
        // 2) 写 header
        // 3) ★ 写 ThreadContext headers
        threadPool.getThreadContext().writeHeaders(out);
        // 4) 写 request 主体
        request.writeTo(out);
        ...
        final long messageSize = bytesStream.size();
        ...
        // 5) 传给底层 channel（Netty4 → writeAndFlush）
        channel.sendMessage(headerBytes, bodyBytes);
    }
    // 6) 统计指标
    transmittedBytesMetric.add(messageSize);
}
```

底层 Netty 写入（`Netty4TcpChannel.java:139`）：

```java
channel.writeAndFlush(Netty4Utils.toByteBuf(reference), writePromise);
```

**注意**：ES 6.x 的 `TcpTransport` 对每个请求用 `writeAndFlush`，
**没有做批量合并（no batching）**。所以跨节点高 QPS 场景下小包会很多——
这是 6.x 的性能特征之一。

---

## 5. 接收一条 RPC 请求（★ 核心方法）

### 5.1 Netty 层

`Netty4MessageChannelHandler.channelRead`：

```java
@Override
protected void channelRead0(ChannelHandlerContext ctx, ByteBuf buffer) throws Exception {
    InetSocketAddress remoteAddress = (InetSocketAddress) ctx.channel().remoteAddress();
    TcpChannel channel = channel(ctx.channel());
    ...
    transport.inboundMessage(channel, Netty4Utils.toBytesReference(buffer));   // :53
}
```

Pipeline（`Netty4Transport.java:270-273`）：
```
"logging" → ESLoggingHandler
"size"    → Netty4SizeHeaderFrameDecoder   （按长度前缀切帧）
"dispatcher" → Netty4MessageChannelHandler
```

### 5.2 `TcpTransport.messageReceived`（`:1071`）逐段精读

```java
public final void messageReceived(BytesReference reference, TcpChannel channel) throws IOException {
    String profileName = channel.getProfile();
    InetSocketAddress remoteAddress = channel.getRemoteAddress();
    int messageLengthBytes = reference.length();
    final int totalMessageSize = messageLengthBytes + TcpHeader.MARKER_BYTES_SIZE + TcpHeader.MESSAGE_LENGTH_SIZE;
    readBytesMetric.inc(totalMessageSize);                                        // ① 读流量统计
    boolean hasMessageBytesToRead = (totalMessageSize - TcpHeader.HEADER_SIZE) > 0;
    StreamInput streamIn = reference.streamInput();
    boolean success = false;
    try (ThreadContext.StoredContext tCtx = threadPool.getThreadContext().stashContext()) {   // ② ★ 清理线程上下文
        long requestId = streamIn.readLong();                                     // ③ 读 header
        byte status = streamIn.readByte();
        Version version = Version.fromId(streamIn.readInt());
        if (TransportStatus.isCompress(status) && hasMessageBytesToRead && streamIn.available() > 0) {
            ... // ④ 解压（见 2.4）
            streamIn = compressor.streamInput(streamIn);
        }
        final boolean isHandshake = TransportStatus.isHandshake(status);
        ensureVersionCompatibility(version, this.version, isHandshake);           // ⑤ ★ 版本检查
        streamIn = new NamedWriteableAwareStreamInput(streamIn, namedWriteableRegistry);  // ⑥ 让 NamedWriteable 可解析
        streamIn.setVersion(version);                                             // ⑦ ★ 设置版本，后面所有 readXxx 都版本感知
        threadPool.getThreadContext().readHeaders(streamIn);                      // ⑧ ★ 恢复 ThreadContext（X-Opaque-Id！）
        threadPool.getThreadContext().putTransient("_remote_address", remoteAddress);
        if (TransportStatus.isRequest(status)) {                                  // ⑨ 分叉
            handleRequest(channel, profileName, streamIn, requestId, messageLengthBytes, version, remoteAddress, status);
        } else {
            // 响应路径（见 5.4）
        }
        success = true;
    } finally {
        if (success) { IOUtils.close(streamIn); } else { IOUtils.closeWhileHandlingException(streamIn); }
    }
}
```

**注意 ②**：`threadPool.getThreadContext().stashContext()` —— 处理新消息前**清空**当前线程的上下文，
否则会串味（比如 Netty 线程处理上一个请求时留下的 header）。
这就是为什么一个请求处理错误会导致「幽灵 header」。

### 5.3 `handleRequest`（`:1215`）逐段精读

```java
protected String handleRequest(TcpChannel channel, String profileName, final StreamInput stream, long requestId,
                               int messageLengthBytes, Version version, InetSocketAddress remoteAddress, byte status)
    throws IOException {
    final Set<String> features;
    if (version.onOrAfter(Version.V_6_3_0)) {                    // ① 6.3+ 才有 features
        features = Collections.unmodifiableSet(new TreeSet<>(Arrays.asList(stream.readStringArray())));
    } else {
        features = Collections.emptySet();
    }
    final String action = stream.readString();                    // ② ★ 读 action 名
    messageListener.onRequestReceived(requestId, action);          // ③ 通知 TaskManager（用于 _tasks）
    TransportChannel transportChannel = null;
    try {
        if (TransportStatus.isHandshake(status)) {                 // ④ 握手走另一条路
            handshaker.handleHandshake(version, features, channel, requestId, stream);
        } else {
            final RequestHandlerRegistry reg = getRequestHandler(action);   // ⑤ ★ 查注册表
            if (reg == null) {
                throw new ActionNotFoundTransportException(action);        //    → "No handler for action"
            }
            if (reg.canTripCircuitBreaker()) {                             // ⑥ ★ 请求体也走熔断器！
                getInFlightRequestBreaker().addEstimateBytesAndMaybeBreak(messageLengthBytes, "<transport_request>");
            } else {
                getInFlightRequestBreaker().addWithoutBreaking(messageLengthBytes);
            }
            transportChannel = new TcpTransportChannel(this, channel, transportName, action, requestId, version,
                features, profileName, messageLengthBytes, TransportStatus.isCompress(status));
            final TransportRequest request = reg.newRequest(stream);       // ⑦ ★ 反序列化请求
            request.remoteAddress(new TransportAddress(remoteAddress));
            validateRequest(stream, requestId, action);                    // ⑧ 校验消息被完整读取
            threadPool.executor(reg.getExecutor()).execute(new RequestHandler(reg, request, transportChannel));  // ⑨ ★★ 切线程池！
        }
    } catch (Exception e) {
        // ⑩ 熔断或任何异常 → 立即回错误响应
        if (transportChannel == null) {
            transportChannel = new TcpTransportChannel(this, channel, transportName, action, requestId, version,
                features, profileName, 0, TransportStatus.isCompress(status));
        }
        try {
            transportChannel.sendResponse(e);
        } catch (IOException inner) { ... }
    }
    return action;
}
```

**逐点说明**：

| 标记 | 说明 | 排障价值 |
| --- | --- | --- |
| ① | `features` 是 6.3 引入的能力协商（如 `xpack`、`dfe`） | 跨版本功能差异 |
| ② | `action` 名就是 `TransportAction` 里那个 `actionName`（如 `indices:data/write/bulk`） | 日志里出现 `action [...]` 就是它 |
| ⑤ | 找不到 handler → `ActionNotFoundTransportException`，客户端看到 `No handler for action [...]` | 典型原因：插件版本不一致 |
| ⑥ | **请求体在反序列化前就先占用熔断器额度** | `_nodes/stats/breaker` 里 `in_flight_requests` |
| ⑦ | `reg.newRequest(stream)` 是 `Writeable.Reader<Request>` 的产物 | 反序列化失败会在这里抛 |
| ⑧ | `validateRequest` 读一个字节确认 `-1`（EOF） | 失败 = `Message not fully read`，通常是版本不匹配 |
| ⑨ | **★ 线程池切换点**：用 `reg.getExecutor()` 指定的池 | 见下表 |

### 5.4 请求被分配到哪个线程池？（★ 高频考点）

`RequestHandlerRegistry.getExecutor()` 返回的值来自 `TransportService.registerRequestHandler(...)` 的 `executor` 参数。

**真实数据（从代码里查出来的）**：

| 注册方 | 注册的 action | executor | 代码位置 |
| --- | --- | --- | --- |
| `HandledTransportAction`（绝大多数 action 的基类） | 顶层 action，如 `indices:data/write/bulk` | **`ThreadPool.Names.SAME`** | `HandledTransportAction.java:61` / `:69` |
| `TransportReplicationAction` | 顶层 action | **`ThreadPool.Names.SAME`** | `TransportReplicationAction.java:155` |
| `TransportReplicationAction` | `action[p]`（主分片执行） | **构造器传入的 `executor`** | `:156` |
| `TransportReplicationAction` | `action[r]`（副本执行） | 同 `executor`，且 **`forceExecution=true`** | `:159-162` |
| `TransportShardBulkAction` | `indices:data/write/bulk[s]` | **`Names.WRITE`** | `TransportShardBulkAction.java:94` |
| `TransportIndexAction` | `indices:data/write/index[p]` / `[r]` | **`Names.INDEX`** | `TransportIndexAction.java:57` |
| `TransportDeleteAction` | `indices:data/write/delete[...]` | **`Names.INDEX`** | `TransportDeleteAction.java:49` |
| `TransportGetAction` | `indices:data/read/get[s]` | **`Names.GET`** | `TransportGetAction.java:52` |
| `TransportShardMultiGetAction` | `indices:data/read/mget[s]` | **`Names.GET`** | `TransportShardMultiGetAction.java:51` |
| `TransportTermVectorsAction` / `TransportShardMultiTermsVectorAction` | termvectors | **`Names.GET`** | `TransportTermVectorsAction.java:52` |
| `TransportUpdateAction` | — | **`Names.INDEX`**（`getExecutor()` 覆写） | `TransportUpdateAction.java:91` |
| `TransportResyncReplicationAction` | resync | **`Names.WRITE`** | `TransportResyncReplicationAction.java:65` |

**`Names.SAME` 的含义**：`ThreadPool.java:215` 里 `Names.SAME` 对应 `DIRECT_EXECUTOR`，
即**在当前线程上直接执行**。对于跨节点的请求，当前线程就是 **Netty 的 worker 线程**。

> 为什么可以这么做？因为顶层 action（如 `TransportBulkAction.doExecute`）只做「分组 + 转发」，
> 真正的重活会立刻被派发到 `write`/`index` 池（分片级 action）。
> 但如果某个顶层 action 做了重活，就会阻塞 Netty IO 线程 —— 这是 ES 6.x 的一个已知风险点。

**`forceExecution=true`（副本请求）**：`TcpTransport.RequestHandler.isForceExecution()`
返回 true 时，`EsThreadPoolExecutor` 即使队列满也会执行（`EsThreadPoolExecutor` 里
对 `forceExecution` 有特殊分支）。代码注释说得很直白：

```java
// TransportReplicationAction.java:158
// we must never reject on because of thread pool capacity on replicas
```

理由：副本请求被拒绝会导致副本落后 → 主分片要重新做 resync，代价更大。

### 5.5 `RequestHandler` 与最终的 `messageReceived`

```java
// TcpTransport.java:1274
class RequestHandler extends AbstractRunnable {
    @Override protected void doRun() throws Exception {
        reg.processMessageReceived(request, transportChannel);      // → handler.messageReceived(request, channel, task)
    }
    @Override public boolean isForceExecution() { return reg.isForceExecution(); }
    @Override public void onFailure(Exception e) { ... transportChannel.sendResponse(e); }
}
```

`reg.processMessageReceived`（`RequestHandlerRegistry.java:59`）会：
1. 用 `taskManager.register("transport", action, request)` 创建 `Task`
2. 调 `handler.messageReceived(request, channel, task)`

所以 `HandledTransportAction.TransportHandler.messageReceived`（`:80`）会被调用：

```java
@Override
public final void messageReceived(final Request request, final TransportChannel channel, Task task) throws Exception {
    // We already got the task created on the network layer - no need to create it again on the transport layer
    Logger logger = HandledTransportAction.this.logger;
    execute(task, request, new ChannelActionListener<>(channel, actionName, request));    // ★ 进入业务
}
```

`ChannelActionListener`（`:87`）负责把 `ActionListener` 的回调变成 `channel.sendResponse(...)`。

### 5.6 响应路径

```java
// TcpTransport.java:1163
private <T extends TransportResponse> void handleResponse(InetSocketAddress remoteAddress, final StreamInput stream,
                                                          final TransportResponseHandler<T> handler) {
    final T response;
    try {
        response = handler.read(stream);                       // ★ 反序列化响应
        response.remoteAddress(new TransportAddress(remoteAddress));
    } catch (Exception e) {
        handleException(handler, new TransportSerializationException(
            "Failed to deserialize response from handler [" + handler.getClass().getName() + "]", e));
        return;
    }
    threadPool.executor(handler.executor()).execute(new AbstractRunnable() {    // ★ 在 handler 指定的池上回调
        @Override public void onFailure(Exception e) {
            handleException(handler, new ResponseHandlerFailureTransportException(e));
        }
        @Override protected void doRun() throws Exception {
            handler.handleResponse(response);
        }
    });
}
```

**注意**：响应也是在**线程池**上回调的，池名来自 `handler.executor()`。
所以「响应在哪个线程被处理」取决于发送方给的 `TransportResponseHandler`。

错误响应（`handlerResponseError:1191`）：

```java
Exception error = stream.readException();                 // ★ 从流里"复活"异常
handleException(handler, error);
```

`handleException`（`:1201`）会把非 `RemoteTransportException` 的异常包一层：

```java
if (!(error instanceof RemoteTransportException)) {
    error = new RemoteTransportException(error.getMessage(), error);
}
```

> **这就是你在日志里看到 `RemoteTransportException` 的原因**：
> 它表示「这个异常是从远端节点传回来的」，`getCause()` 里才是真正的远端异常。

---

## 6. 请求处理器注册表

### 6.1 注册

```java
// TransportService.java:787 / :804 / :824 / :845（4 个重载）
public <Request extends TransportRequest> void registerRequestHandler(String action,
                                    Supplier<Request> requestFactory,
                                    String executor, TransportRequestHandler<Request> handler) { ... }

public <Request extends TransportRequest> void registerRequestHandler(String action,
                                    Supplier<Request> requestFactory,
                                    String executor, boolean forceExecution,
                                    boolean canTripCircuitBreaker,
                                    TransportRequestHandler<Request> handler) { ... }
```

内部 `requestHandlers.put(action, new RequestHandlerRegistry<>(action, requestFactory, handler, taskManager, executor, forceExecution, canTripCircuitBreaker))`。

`getRequestHandler(action)`（`TcpTransport.java:1506`）就是 `requestHandlers.get(action)`。

### 6.2 重复注册会怎样

`registerRequestHandler` 内部会检查 `requestHandlers.containsKey(action)`，
重复注册会抛 `IllegalArgumentException: transport action [xxx] is already registered`。

**排障价值**：插件装了两次 / 两个插件用了同一个 action 名，启动就会报这个错。

---

## 7. 本地节点优化（Local Node Optimization）

```java
// TransportService.java:553
public Transport.Connection getConnection(DiscoveryNode node) {
    if (isLocalNode(node)) {
        return localNodeConnection;
    }
    return connectionManager.getConnection(node);
}
```

`localNodeConnection` 是一个特殊实现：`sendRequest` 不走网络，
而是**直接在当前线程上调用 `onRequestReceived`**。

```java
// TransportService 的 localNodeConnection（简化）
private final Transport.Connection localNodeConnection = new Transport.Connection() {
    @Override
    public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options) {
        // 直接触发本地 handler
        onRequestReceived(requestId, action);
        try (StreamInput in = new NamedWriteableAwareStreamInput(StreamInput.wrap(new byte[0]), namedWriteableRegistry)) {
            // ★ 注意：仍然会"序列化→反序列化"一次，为了语义一致
            ...
        }
    }
};
```

**重要影响**：
1. 本地请求**不会经过 Netty**，也不会有网络开销。
2. 但**仍然会走一次序列化/反序列化**（`sendRequest` 时 `request.writeTo(out)`，
   `newRequest(stream)` 时再读）——这样既不引入特例分支，也能保证「本地调用与远程调用语义一致」。
3. 本地请求的 `executor` 是**调用方所在线程**（`Names.SAME` 不影响它，因为本地路径不经过 `handleRequest`）。

> **调优含义**：协调节点和数据节点同机部署（`coordinating only` 节点 + data 节点分开）时，
> 本地 shard 请求比远程快很多。这是「减少网络跳数」的源码级依据。

---

## 8. Transport 相关的设置与统计

### 8.1 关键设置

| 设置 | 默认 | 含义 |
| --- | --- | --- |
| `transport.tcp.port` | 9300-9305 | 端口范围 |
| `transport.tcp.compress` | false | 是否压缩 body |
| `transport.tcp.connect_timeout` | 30s | 连接超时 |
| `transport.ping_schedule` | 5s | keepalive ping 间隔（`-1` 关闭） |
| `transport.tcp.keep_alive` | true | TCP SO_KEEPALIVE |
| `transport.tcp.no_delay` | true | TCP_NODELAY |
| `transport.tcp.reuse_address` | true | SO_REUSEADDR |
| `transport.tcp.send_buffer_size` | -1 | SO_SNDBUF |
| `transport.tcp.receive_buffer_size` | -1 | SO_RCVBUF |
| `transport.connections_per_node.*` | 见 3.1 | 每种 profile 的连接数 |
| `transport.netty.worker_count` | `2 * CPU` | ★ Netty IO 线程数（`Netty4Transport.java:86`） |
| `transport.netty.max_cumulation_buffer_capacity` | 32MB | cumulation buffer 上限 |
| `transport.type` | `netty4` | 传输实现 |
| `transport.profiles.*` | — | ★ 自定义 profile（多网卡/多端口，用于冷热分离） |

**`transport.netty.worker_count` 的坑**（`Netty4Transport.java:112-131`）：

```java
Netty4Utils.setAvailableProcessors(EsExecutors.PROCESSORS_SETTING.get(settings));
this.workerCount = WORKER_COUNT.get(settings);
...
eventLoopGroup = new NioEventLoopGroup(workerCount, threadFactory);
```

`PROCESSORS_SETTING` 就是 `processors`。
**所以不设置 `processors` 时，容器里可能起出一个远超预期的 Netty 线程数**（每个都吃栈内存）。

### 8.2 统计指标

```bash
GET /_nodes/stats/transport?pretty
GET /_nodes/stats/transport,http?human
```

| 字段 | 含义 | 源码 |
| --- | --- | --- |
| `server_open` | 当前打开的连接数 | `TcpTransport` 的 `openChannels` |
| `total_outbound_connections` | 累计出向连接数 | |
| `rx_count` / `rx_size_in_bytes` | 接收请求数与字节 | `readBytesMetric`（`:1076`） |
| `tx_count` / `tx_size_in_bytes` | 发送请求数与字节 | `transmittedBytesMetric` |

**排障**：`rx_size_in_bytes` 突增但 `tx` 不变 → 可能是大聚合/fetch 拉数据；
`server_open` 持续增长不下降 → 连接泄漏（通常是某个 profile 的连接没释放）。

---

## 9. 完整调用链（务必背下来）

### 9.1 发送端

```
业务代码: transportService.sendRequest(node, action, request, options, responseHandler)
  └─ TransportService.getConnection(node)                       (:553)
       ├─ isLocalNode → localNodeConnection
       └─ else → connectionManager.getConnection(node)
  └─ TransportService.sendRequest(connection, ...)              (:537)
  └─ TransportService.sendRequestInternal                       (:597)
       ├─ threadContext.newRestorableContext(true)              # 保存上下文
       ├─ responseHandlers.add(...)  → 得到 requestId            # 登记等待
       ├─ threadPool.schedule(options.timeout(), timeoutHandler) # 超时任务
       └─ connection.sendRequest(requestId, action, request, options)
            └─ TcpTransport.Connection.sendRequest               (:280)
                 ├─ channel(options.type())                      # 按类型选 channel
                 └─ TcpTransport.sendRequestToChannel
                      ├─ 写 TcpHeader（requestId/status/version）
                      ├─ threadContext.writeHeaders(out)         # ★ 上下文跨节点
                      ├─ request.writeTo(out)                    # 序列化请求
                      └─ channel.sendMessage(header, bytes)
                           └─ Netty4TcpChannel: channel.writeAndFlush(...)   (:139)
```

### 9.2 接收端

```
Netty worker 线程
  └─ Netty4MessageChannelHandler.channelRead                      (:53)
       └─ Netty4Transport.inboundMessage → TcpTransport.messageReceived  (:1071)
            ├─ threadContext.stashContext()                       # ★ 清空上下文
            ├─ 读 requestId / status / version
            ├─ 解压（如果 compressed）
            ├─ ensureVersionCompatibility                         (:1150)
            ├─ setVersion(version)
            ├─ threadContext.readHeaders(streamIn)                # ★ 恢复上下文
            ├─ isRequest?
            │    └─ TcpTransport.handleRequest                    (:1215)
            │         ├─ 读 features / action
            │         ├─ messageListener.onRequestReceived(...)   # TaskManager
            │         ├─ getRequestHandler(action)                (:1506)
            │         ├─ inFlightRequestBreaker.addEstimateBytesAndMaybeBreak(...)
            │         ├─ new TcpTransportChannel(...)
            │         ├─ reg.newRequest(stream)                   # 反序列化请求
            │         ├─ validateRequest(stream, ...)
            │         └─ threadPool.executor(reg.getExecutor()).execute(new RequestHandler(...))   # ★★ 切线程
            │              └─ reg.processMessageReceived(request, channel)
            │                   └─ handler.messageReceived(request, channel, task)
            │                        └─ HandledTransportAction.execute(task, request, listener)
            │                             └─ TransportAction.execute                       (support/TransportAction.java:127)
            │                                  ├─ request.validate()
            │                                  ├─ RequestFilterChain.proceed(...)           # ActionFilter 链
            │                                  └─ doExecute(task, request, listener)         # ★ 业务逻辑
            └─ else（响应）
                 └─ responseHandlers.onResponseReceived(requestId, messageListener)
                      └─ handleResponse(...) / handlerResponseError(...)                (:1163 / :1191)
                           └─ threadPool.executor(handler.executor()).execute(...)
                                └─ handler.handleResponse(response) / handleException(...)
```

### 9.3 回包

```
业务代码: listener.onResponse(response)
  └─ ChannelActionListener.onResponse                                (:101)
       └─ TcpTransportChannel.sendResponse(response)                 (:62)
            └─ TcpTransport.sendResponse
                 ├─ 写 TcpHeader（相同 requestId，status=STATUS_RES）
                 ├─ response.writeTo(out)
                 └─ channel.sendMessage(...) → Netty writeAndFlush
```

---

## 10. 异常与常见错误

| 异常 | 含义 | 源码 |
| --- | --- | --- |
| `NodeNotConnectedException` | 目标节点没有可用连接 | `TransportService.getConnection` / `connectionManager` |
| `ConnectTransportException` | 建立连接失败（TCP 层） | `TcpTransport.initiateConnection:325/331` |
| `NodeDisconnectedException` | 连接在使用中中断 | `ConnectionManager` |
| `ReceiveTimeoutTransportException` | 本地超时（对端没回） | `TransportService.TimeoutHandler` |
| `RemoteTransportException` | ★ 远端返回的错误，`getCause()` 是真因 | `TcpTransport.handleException:1201` |
| `TransportSerializationException` | 反序列化失败（版本不匹配/数据损坏） | `handleResponse:1170` |
| `ActionNotFoundTransportException` | action 没有注册 | `handleRequest:1233` |
| `ResponseHandlerFailureTransportException` | 回包处理时抛出异常 | `handleResponse:1177` |
| `IllegalStateException: Message not fully read` | 请求/响应没被读完（几乎总是版本不匹配） | `validateRequest:1269` / `messageReceived:1135` |
| `IllegalStateException: Received ... unsupported version` | 版本超范围 | `ensureVersionCompatibility:1159` |
| `IllegalStateException: stream marked as compressed, but no compressor found` | 压缩标记与实际不符 | `messageReceived:1099` |

**排障速记**：
- 看到 `RemoteTransportException` → **往下看 `Caused by`**，真因在最后。
- 看到 `Message not fully read` → 检查节点版本是否在滚动升级中间态。
- 看到 `ActionNotFoundTransportException` → 检查插件是否所有节点都装了（version 不一致）。

---

## 11. 动手实验

**实验 5-1：抓一次完整的 requestId 生命周期**
在 `TransportService.sendRequestInternal`（`:609`，`responseHandlers.add`）打断点，
记下 `requestId`；在 `TcpTransport.handleRequest`（`:1215`）打断点，确认收到的是同一个 id；
在 `handleResponse`（`:1163`）打断点，确认还是同一个 id。

**实验 5-2：验证 NodeChannels 分类型**
3 节点集群，对每个节点 `netstat -an | grep 9300` 数一下连接数。
对照 `transport.connections_per_node.*` 的配置，理解每条连接是给哪个 profile 用的。

**实验 5-3：观察压缩的效果**
把 `transport.tcp.compress` 设为 `true`，重启节点，
在 `TcpTransport.messageReceived:1085` 打断点观察 `CompressorFactory.compressor(...)` 被调用。

**实验 5-4：验证 ThreadContext 跨节点**
带 `X-Opaque-Id: test-123` 发一个跨节点请求，
在接收方的 `TcpTransport.messageReceived:1107`（`readHeaders`）打断点，
检查 `threadPool.getThreadContext().getHeader("X-Opaque-Id")` 的值。

**实验 5-5：制造版本不兼容**
用 6.6 的节点去连一个 5.x 的节点（或用测试框架模拟），
观察 `ensureVersionCompatibility`（`:1150`）抛出的异常。

**实验 5-6：观察 netty worker 线程**
`jstack` 抓取节点，找到 `nioEventLoopGroup` 线程，数一下数量，
对照 `transport.netty.worker_count`（默认 `2*processors`）。

---

## 12. 自测题

1. Transport 消息头有哪三个必需字段？分别多少字节？
2. 为什么要有 `handshake`？为什么握手包的 version 填的是 `minimumCompatibilityVersion()`？
3. `ConnectionProfile` 为什么要按用途分多条 channel？不分会有什么问题？
4. `TransportService.sendRequestInternal`（`:597`）里 `newRestorableContext(true)` 的作用是什么？
5. 超时是怎么实现的？超时后**远端会停止执行**吗？
6. `TcpTransport.messageReceived`（`:1071`）开头为什么要 `stashContext()`？
7. `handleRequest`（`:1215`）里 `forceExecution` 是什么？哪些请求会设它？为什么？
8. `HandledTransportAction` 注册 handler 时用的是哪个线程池？这意味着请求在哪个线程上开始执行？
9. `TransportShardBulkAction`、`TransportGetAction`、`TransportIndexAction` 分别注册在哪个池上？
10. 本地节点优化（`localNodeConnection`）会不会跳过序列化？为什么？
11. `RemoteTransportException` 是怎么产生的？为什么说「真因在 `Caused by` 里」？
12. 请求体的熔断（`getInFlightRequestBreaker`）是在反序列化**之前**还是**之后**？为什么这样设计？

---

**上一篇**：[04-基础支撑层-Settings与模块化与线程池.md](./04-基础支撑层-Settings与模块化与线程池.md)　
**下一篇**：[06-REST层与HTTP请求入口.md](./06-REST层与HTTP请求入口.md)
