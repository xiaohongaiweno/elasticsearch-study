# 24 · 监控 Stats 与内存保护

> 目标：讲清两件在生产上天天打交道的事：
> **ES 的监控数据是怎么采集和聚合的**（`_nodes/stats`、`_cluster/stats`、`_cat/*`），
> 以及 **ES 是怎么防止自己被内存打爆的**（断路器体系）。
>
> **为什么必须学**：
> - 「`Data too large` / `CircuitBreakingException` / 429」→ 断路器。
>   这是 ES 最常见的写入/查询失败原因之一，而且报错信息需要会读。
> - 「节点 OOM 了，断路器为什么没拦住」→ 断路器的**估算**本质（它拦不住所有分配）。
> - 「该看哪些指标才算健康」→ `CommonStats` 里的 17 类指标，哪些是关键。
> - 「`_nodes/stats` 本身会不会被断路器拦」→ 会，所以源码里专门豁免了（见 §3.3）。
> - 云服务的**可观测性和稳定性**全落在这两件事上。
>
> **前置**：第 05 章（Transport）、第 06 章（线程池）、第 20 章（聚合的内存记账）。
> **本篇对应**：阶段 5（生态期）。

---

## 1. 一图总览

```
  监控（读）
  ┌─────────────────────────────────────────────────────────────────────┐
  │ GET /_nodes/stats?os&jvm&indices&breaker                             │
  │      ↓ RestNodesStatsAction.prepareRequest (:100)  解析 flags         │
  │      ↓ NodesStatsAction → TransportNodesStatsAction                  │
  │          （extends TransportNodesAction：向所有节点扇出，再聚合）        │
  │      ↓ NodeService.stats (:103)                                      │
  │          ├─ indicesService.stats(true, flags)      → NodeIndicesStats│
  │          ├─ monitorService.osService().stats()     → OsStats         │
  │          ├─ monitorService.processService().stats()→ ProcessStats    │
  │          ├─ monitorService.jvmService().stats()    → JvmStats        │
  │          ├─ monitorService.fsService().stats()     → FsInfo          │
  │          ├─ threadPool.stats()                     → ThreadPoolStats │
  │          ├─ transportService.stats() / httpServerTransport.stats()   │
  │          ├─ circuitBreakerService.stats()          → AllCircuitBreakerStats │
  │          ├─ scriptService.stats() / discovery.stats()                │
  │          ├─ ingestService.stats()                                    │
  │          └─ responseCollectorService.getAdaptiveStats(...)            │
  └─────────────────────────────────────────────────────────────────────┘

  内存保护（写）
  ┌─────────────────────────────────────────────────────────────────────┐
  │                    PARENT (total)  70% heap                          │
  │              ┌──────────┴──────────┬──────────┬──────────┐          │
  │           REQUEST 60%         FIELDDATA 60%  IN_FLIGHT  ACCOUNTING   │
  │           (overhead 1.0)      (overhead 1.03)  100%        100%      │
  │                                                                      │
  │  每次 addEstimateBytesAndMaybeBreak(bytes, label):                    │
  │    ① 子 breaker 检查：used*overhead > limit ? → circuitBreak           │
  │    ② parent 检查：Σ(used_i * overhead_i) > 70% ? → 抛 + 回滚           │
  └─────────────────────────────────────────────────────────────────────┘
```

---

## 2. 必读源码清单

### 监控

| 类 | 路径 | 作用 |
| --- | --- | --- |
| `NodeStats` | `action/admin/cluster/node/stats/NodeStats.java` | ★ 节点统计容器（`:51`） |
| `NodeService` | `node/NodeService.java` | ★ 采集入口（`stats:103`） |
| `TransportNodesStatsAction` | `action/admin/cluster/node/stats/TransportNodesStatsAction.java` | 扇出聚合（`:39`） |
| `RestNodesStatsAction` | `rest/action/admin/cluster/RestNodesStatsAction.java` | REST 层（`:100`、`canTripCircuitBreaker:210`） |
| `CommonStats` | `action/admin/indices/stats/CommonStats.java` | ★ 索引级 17 类统计（`:55`） |
| `CommonStatsFlags` | `action/admin/indices/stats/CommonStatsFlags.java` | 过滤标志 |
| `MonitorService` | `monitor/MonitorService.java` | os/process/jvm/fs 四个 service 的容器（`:35`） |
| `OsStats` / `OsProbe` | `monitor/os/` | CPU、load、mem、swap |
| `JvmStats` / `JvmInfo` | `monitor/jvm/` | 堆、GC、线程、buffer pools、classes |
| `ProcessStats` / `ProcessProbe` | `monitor/process/` | 进程 CPU、内存、fds |
| `FsInfo` / `FsProbe` | `monitor/fs/` | 磁盘 total/free/available、spins、io_stats |
| `HotThreads` | `monitor/jvm/HotThreads.java` | `/_nodes/hot_threads` |
| `JvmGcMonitorService` | `monitor/jvm/JvmGcMonitorService.java` | ★ 慢 GC 日志（`:44`） |
| `DeadlockAnalyzer` | `monitor/jvm/DeadlockAnalyzer.java` | 死锁检测 |

### 内存保护

| 类 | 路径 | 作用 |
| --- | --- | --- |
| `CircuitBreaker` | `common/breaker/CircuitBreaker.java` | ★ 接口 + 5 个名字 + Type（`:64`） |
| `HierarchyCircuitBreakerService` | `indices/breaker/HierarchyCircuitBreakerService.java` | ★ 父子断路器服务（`:45`） |
| `ChildMemoryCircuitBreaker` | `common/breaker/ChildMemoryCircuitBreaker.java` | ★ 子断路器实现（`addEstimateBytesAndMaybeBreak:110`） |
| `BreakerSettings` | `indices/breaker/BreakerSettings.java` | name/limit/overhead/type/durability |
| `CircuitBreakerStats` / `AllCircuitBreakerStats` | `indices/breaker/` | 统计输出 |
| `CircuitBreakingException` | `common/breaker/CircuitBreakingException.java` | 429（`:71`） |
| `NoopCircuitBreaker` | `common/breaker/NoopCircuitBreaker.java` | 空实现 |
| `BigArrays` | `common/util/BigArrays.java` | 页式大数组（配合 request breaker 记账） |

---

## 3. 监控：Stats 体系

### 3.1 `NodeStats` 有什么

`action/admin/cluster/node/stats/NodeStats.java:51`，字段（`:97` 的构造器）：

| 字段 | getter 行号 | 内容 |
| --- | --- | --- |
| `timestamp` | `:122` | 采集时间戳 |
| `hostname` | `:127` | |
| `NodeIndicesStats` | `:135` | 索引维度（docs/store/indexing/get/search/merge/refresh/flush/…） |
| `OsStats` | `:143` | CPU / load / mem / swap |
| `ProcessStats` | `:151` | 进程 CPU / mem / fds |
| `JvmStats` | `:159` | 堆 / 非堆 / GC / 线程 / buffer pools / classes |
| `ThreadPoolStats` | `:167` | 每个池的 threads/queue/active/rejected/completed |
| `FsInfo` | `:175` | 数据目录磁盘 |
| `TransportStats` | `:180` | 收发字节、连接数 |
| `HttpStats` | `:185` | HTTP 连接数 |
| `AllCircuitBreakerStats` | `:190` | ★ 断路器 |
| `ScriptStats` | `:195` | 脚本编译/执行/限流触发次数 |
| `DiscoveryStats` | `:200` | |
| `IngestStats` | `:205` | pipeline / processor 级别统计 |
| `AdaptiveSelectionStats` | `:210` | ARS（自适应副本选择） |

### 3.2 采集入口

`NodeService.stats`（`:103`）：

```java
// for indices stats we want to include previous allocated shards stats as well (it will
// only be applied to the sensible ones to use, like refresh/merge/flush/indexing stats)
return new NodeStats(transportService.getLocalNode(), System.currentTimeMillis(),
        indices.anySet() ? indicesService.stats(true, indices) : null,      // :109
        os ? monitorService.osService().stats() : null,                     // :110
        process ? monitorService.processService().stats() : null,           // :111
        jvm ? monitorService.jvmService().stats() : null,                   // :112
        threadPool ? this.threadPool.stats() : null,                        // :113
        ...
```

**注意**：每个维度都支持**开关**（`boolean os, process, jvm, ...`），
由 `RestNodesStatsAction` 根据 URL 参数决定。
所以 **`GET /_nodes/stats` 不带参数时是"全开"的，开销不小**；
生产采集应该显式指定需要的维度。

`indicesService.stats(true, indices)` 的第一个参数 `includePreviousShards=true`：
**包含已经从本节点迁走的分片的统计**（源码注释 `:106-107`）。
这解释了为什么 stats 里的 indexing/merge 计数看起来"只增不减"。

### 3.3 一个很贴心的设计

`RestNodesStatsAction.canTripCircuitBreaker`（`:210`）：

```java
@Override
public boolean canTripCircuitBreaker() {
    return false;
}
```

**含义**：节点 stats 请求**自身不会被断路器拦截**。

> 为什么重要：集群内存告急时，断路器会拒绝大部分请求；
> 如果 `_nodes/stats` 也被拦，你就**没法知道发生了什么** —— 监控系统全瞎。
> 这是个"留一条后路"的设计，值得在所有自建系统里借鉴。

### 3.4 `CommonStats`：索引级的 17 类指标

`action/admin/indices/stats/CommonStats.java:55`：

| 字段 | 行号 | 说明 |
| --- | --- | --- |
| `DocsStats docs` | `:58` | 文档数、删除文档数 |
| `StoreStats store` | `:61` | 索引大小（size_in_bytes）、throttle 时间 |
| `IndexingStats indexing` | `:64` | index/delete 的次数与耗时 |
| `GetStats get` | `:67` | get（含 realtime/missing） |
| `SearchStats search` | `:70` | query/fetch/suggest/scroll 的计数与耗时 |
| `MergeStats merge` | `:73` | 段合并（current/docs/bytes/total 及耗时） |
| `RefreshStats refresh` | `:76` | refresh 次数与耗时 |
| `FlushStats flush` | `:79` | flush 次数与耗时 |
| `WarmerStats warmer` | `:82` | 预热 |
| `QueryCacheStats queryCache` | `:85` | 节点级 query cache（hit/miss/eviction） |
| `FieldDataStats fieldData` | `:88` | ★ fielddata 内存占用（含 eviction） |
| `CompletionStats completion` | `:91` | completion suggester |
| `SegmentsStats segments` | `:94` | 段数量、各段内存（terms/norms/stored fields/doc values/…） |
| `TranslogStats translog` | `:97` | translog 操作数与大小 |
| `RequestCacheStats requestCache` | `:100` | 分片级 request cache |
| `RecoveryStats recoveryStats` | `:103` | 作为恢复源/目标的统计 |

`add(CommonStats)`（`:275`）负责把多个分片的统计**汇总**；
`getTotalMemory()`（`:491`）汇总段内存。

> **监控必看**（云服务经验）：
> `docs.count`、`segments.count`（段太多说明 merge 跟不上）、
> `store.size`、`indexing.index_time_in_millis`、`search.query_time_in_millis`、
> `merge.total_time_in_millis`、`refresh.total_time_in_millis`（压力大时 refresh 会变慢）、
> `fielddata.memory_size_in_bytes`、`query_cache.evictions`（eviction 高说明缓存太小）。

---

## 4. 监控：Probe 是怎么采集的

`monitor/MonitorService.java:35` 聚合 4 个 service：

```java
public OsService osService()      { return this.osService; }      // :53
public ProcessService processService() { return this.processService; }  // :57
public JvmService jvmService()    { return this.jvmService; }     // :61
public FsService fsService()      { return this.fsService; }      // :65
```

各 `Probe` 的实现方式：

| Probe | 数据来源 |
| --- | --- |
| `OsProbe` | `com.sun.management.OperatingSystemMXBean`（`/proc` 之外），拿 CPU 时间、load average、mem、swap |
| `JvmService` | `ManagementFactory.getMemoryMXBean()`、`getGarbageCollectorMXBeans()`、`getThreadMXBean()`、`getBufferPoolMXBeans()` |
| `ProcessProbe` | `OperatingSystemMXBean.getProcessCpuTime()`、`ProcessMXBean`、Linux 下读 `/proc/self/stat` |
| `FsProbe` | `FileStore.getUsableSpace()` + `NodeEnvironment` 的 data path |

> **`Probes`（`monitor/Probes.java`）** 提供静态工具方法，处理"拿不到 JMX 时降级"的情况。
> 这也是为什么某些容器环境下 `os.cpu.percent` 会显示 -1。

### 4.1 慢 GC 监控

`monitor/jvm/JvmGcMonitorService.java:44`：

| Setting | 行号 | 默认 | 说明 |
| --- | --- | --- | --- |
| `monitor.jvm.gc.enabled` | `:55` | `true` | |
| `monitor.jvm.gc.refresh_interval` | `:57` | `1s` | 采样间隔 |
| `monitor.jvm.gc.collector.*` | `:62` | — | 每个 GC 收集器的阈值（group setting） |
| `monitor.jvm.gc.overhead.warn` | `:64` | `50` | GC 时间占比 % |
| `monitor.jvm.gc.overhead.info` | `:66` | `25` | |
| `monitor.jvm.gc.overhead.debug` | `:68` | `10` | |

构造函数里校验阈值顺序（`:130`）：`warn > info > debug`，否则报
`"monitor.jvm.gc.overhead.warn [{}] is less or equal than monitor.jvm.gc.overhead.info [...]"`。

运行时（`:191-201`）：

```java
try { ... } catch (Exception e) {
    logger.debug("failed to monitor", e);
}
...
logSlowGc(logger, threshold, seq, slowGcEvent, JvmGcMonitorService::buildPools);   // :196
...
logGcOverhead(logger, threshold, current, elapsed, seq);                            // :201
```

> **日志里那句经典的**
> `[gc][young][123][45] duration [1.2s], collections [1]/[1.5s], total [1.2s]/[8.3s], memory [3.1gb]->[2.4gb]/[7.9gb]`
> 就来自这里（`SLOW_GC_LOG_MESSAGE:206`）。
> 它不是 error，但**是 OOM 的前兆**。

---

## 5. 内存保护：为什么需要断路器

ES 的内存压力来源：

| 来源 | 特性 |
| --- | --- |
| **JVM heap（用户可控）** | fielddata、聚合（第 20 章）、request cache、in-flight 请求、Lucene 段内存 |
| **堆外 / OS cache** | Lucene 依赖 OS page cache（**不消耗 heap**），所以 ES 建议 heap 不超过 32GB 且留一半给 OS |

断路器的作用：在**分配前**预估大小，超限就拒绝请求（而不是等 OOM）。

**关键认知**：断路器是**估算 + 主动记账**的，
只有**显式调用 `addEstimateBytesAndMaybeBreak` 的代码路径**才受保护。
Lucene 自身的一些分配、直接内存、栈上分配等不在其内 —— **所以有断路器依然可能 OOM**。

---

## 6. 断路器体系

### 6.1 5 个 breaker

`common/breaker/CircuitBreaker.java`：

| 名字 | 常量行号 | 跟踪什么 |
| --- | --- | --- |
| `REQUEST` | `:51` | ★ 单个请求的内存（聚合、cardinality、桶计数……）**请求结束后释放** |
| `FIELDDATA` | `:43` | ★ fielddata + parent/child id cache（**长期驻留**） |
| `IN_FLIGHT_REQUESTS` | `:56` | 网络层读写请求的字节 |
| `ACCOUNTING` | `:62` | **与请求生命周期无关**的内存（如 Lucene 段内存） |
| `PARENT` | （`HierarchyCircuitBreakerService` 里，`Type.PARENT`） | 总量上限，不是真实的 breaker，只在 `checkParentLimit` 里计算 |

`Type`（`:64`）：`MEMORY`（普通子断路器）、`PARENT`、`NOOP`（永不跳闸）。

### 6.2 默认阈值

`indices/breaker/HierarchyCircuitBreakerService.java`：

| Setting | 行号 | 默认 | overhead | 动态 |
| --- | --- | --- | --- | --- |
| `indices.breaker.total.limit`（PARENT） | `:52` | **70%** heap | 1.0 | ✅ |
| `indices.breaker.request.limit` | `:62` | **60%** heap | 1.0（`:64`） | ✅ |
| `indices.breaker.fielddata.limit` | `:55` | **60%** heap | **1.03**（`:57`） | ✅ |
| `indices.breaker.accounting.limit` | `:69` | **100%** heap | 1.0（`:71`） | ✅ |
| `network.breaker.inflight_requests.limit` | `:76` | **100%** heap | 1.0（`:78`） | ✅ |

`*_type` 设置（`:59`/`:66`/`:73`/`:80`）默认都是 `memory`，可以设成 `noop` 关闭。

**为什么 fielddata 的 overhead 是 1.03**：
源码注释（`:57-58`）没明说，但含义是"fielddata 的真实内存开销比估算值略高（约 3%）"，
用 overhead 系数把估算值放大以留出安全边际。

### 6.3 动态更新

`HierarchyCircuitBreakerService` 构造（`:131-140`）注册了 5 个 `addSettingsUpdateConsumer`：

```java
clusterSettings.addSettingsUpdateConsumer(TOTAL_CIRCUIT_BREAKER_LIMIT_SETTING, this::setTotalCircuitBreakerLimit,
    this::validateTotalCircuitBreakerLimit);                                            // :131
clusterSettings.addSettingsUpdateConsumer(FIELDDATA_CIRCUIT_BREAKER_LIMIT_SETTING,
    FIELDDATA_CIRCUIT_BREAKER_OVERHEAD_SETTING, this::setFieldDataBreakerLimit);        // :133
clusterSettings.addSettingsUpdateConsumer(IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_LIMIT_SETTING,
    IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_OVERHEAD_SETTING, this::setInFlightRequestsBreakerLimit);  // :135
clusterSettings.addSettingsUpdateConsumer(REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING,
    REQUEST_CIRCUIT_BREAKER_OVERHEAD_SETTING, this::setRequestBreakerLimit);            // :137
clusterSettings.addSettingsUpdateConsumer(ACCOUNTING_CIRCUIT_BREAKER_LIMIT_SETTING,
    ACCOUNTING_CIRCUIT_BREAKER_OVERHEAD_SETTING, this::setAccountingBreakerLimit);      // :139
```

**`registerBreaker` 是"替换"语义**（`:271`）：

```java
if (breakerSettings.getType() == CircuitBreaker.Type.NOOP) {
    CircuitBreaker breaker = new NoopCircuitBreaker(breakerSettings.getName());
    breakers.put(breakerSettings.getName(), breaker);
} else {
    ...
    breaker = new ChildMemoryCircuitBreaker(breakerSettings,
            (ChildMemoryCircuitBreaker)oldBreaker, ...);      // ★ 继承旧的 used / trippedCount
    if (breakers.replace(breakerSettings.getName(), oldBreaker, breaker)) return;
}
```

看 `ChildMemoryCircuitBreaker` 构造（`:66`）：

```java
if (oldBreaker == null) {
    this.used = new AtomicLong(0);
    this.trippedCount = new AtomicLong(0);
} else {
    this.used = oldBreaker.used;                 // ★ 继承已用量，不归零
    this.trippedCount = oldBreaker.trippedCount;
}
```

> **重要**：动态调大/调小 breaker **不会清零已用量**。
> 所以"内存已满 → 调大 breaker"只能临时救急，真正的占用还在。

### 6.4 `checkParentLimit`（父断路器）

`HierarchyCircuitBreakerService:240`：

```java
public void checkParentLimit(String label) throws CircuitBreakingException {
    long totalUsed = 0;
    for (CircuitBreaker breaker : this.breakers.values()) {
        totalUsed += (breaker.getUsed() * breaker.getOverhead());     // :243 ★ 注意 parent 不在 breakers 里
    }
    long parentLimit = this.parentSettings.getLimit();
    if (totalUsed > parentLimit) {
        this.parentTripCount.incrementAndGet();
        final StringBuilder message = new StringBuilder("[parent] Data too large, data for [" + label + "]" +
                " would be [" + totalUsed + "/" + new ByteSizeValue(totalUsed) + "]" +
                ", which is larger than the limit of [" + parentLimit + "/" + new ByteSizeValue(parentLimit) + "]");
        message.append(", usages [");
        message.append(String.join(", ", this.breakers.entrySet().stream().map(e -> {
            final CircuitBreaker breaker = e.getValue();
            final long breakerUsed = (long)(breaker.getUsed() * breaker.getOverhead());
            return e.getKey() + "=" + breakerUsed + "/" + new ByteSizeValue(breakerUsed);
        }).collect(Collectors.toList())));
        message.append("]");
        throw new CircuitBreakingException(message.toString(), totalUsed, parentLimit);   // :262
    }
}
```

> **`stats()` 里的 parent 计算是不同的**（`:215-228`）：
> `parentEstimated = Σ breaker.getUsed()`（**不乘 overhead**），
> 而 `checkParentLimit` 用 `Σ (used * overhead)`。
> **所以你看到的 `parent.estimated_size` 可能比真正触发跳闸的阈值小。**
> 排查时要留意这个差异。

### 6.5 `ChildMemoryCircuitBreaker.addEstimateBytesAndMaybeBreak`

`common/breaker/ChildMemoryCircuitBreaker.java:110`：

```java
// ① limit=0 → 立即跳闸
if (memoryBytesLimit == 0) {
    circuitBreak(label, bytes);
}

long newUsed;
if (this.memoryBytesLimit == -1) {
    newUsed = noLimit(bytes, label);                 // ② 无限制：只累加（RamAccountingTermsEnum 快路径）
} else {
    newUsed = limit(bytes, label);                   // ③ 有限制：CAS 循环 + 检查
}

// ④ 再检查 parent
try {
    parent.checkParentLimit(label);
} catch (CircuitBreakingException e) {
    // If the parent breaker is tripped, this breaker has to be
    // adjusted back down because the allocation is "blocked" but the
    // breaker has already been incremented
    this.addWithoutBreaking(-bytes);                 // ★ 回滚
    throw e;
}
return newUsed;
```

`limit`（`:149`）的 CAS 循环：

```java
do {
    currentUsed = this.used.get();
    newUsed = currentUsed + bytes;
    long newUsedWithOverhead = (long) (newUsed * overheadConstant);
    ...
    if (memoryBytesLimit > 0 && newUsedWithOverhead > memoryBytesLimit) {
        logger.warn("[{}] New used memory {} [{}] for data of [{}] would be larger than configured breaker: {} [{}], breaking",
                this.name, newUsedWithOverhead, ..., label, memoryBytesLimit, ...);
        circuitBreak(label, newUsedWithOverhead);
    }
    ...
} while (!this.used.compareAndSet(currentUsed, newUsed));
```

`circuitBreak`（`:91`）：

```java
this.trippedCount.incrementAndGet();
final String message = "[" + this.name + "] Data too large, data for [" + fieldName + "]" +
        " would be [" + bytesNeeded + "/" + new ByteSizeValue(bytesNeeded) + "]" +
        ", which is larger than the limit of [" +
        memoryBytesLimit + "/" + new ByteSizeValue(memoryBytesLimit) + "]";
logger.debug("{}", message);
throw new CircuitBreakingException(message, bytesNeeded, memoryBytesLimit);
```

**注意**：`circuitBreak` 只 `logger.debug` —— **断路器跳闸默认不打 WARN 日志**！
要看统计里的 `tripped` 计数，或开 `logger.org.elasticsearch.common.breaker=DEBUG`。

### 6.6 HTTP 状态码

`CircuitBreakingException.status()`（`:71`）返回 **`SERVICE_UNAVAILABLE`（503）**。

> 很多文章说断路器是 429，但 6.6.1 源码里是 **503**。
> 客户端重试策略要按 503 处理（而且**必须退避**，否则会把集群打死）。

---

## 7. 谁在往断路器记账

| 使用者 | 记账位置 | breaker |
| --- | --- | --- |
| 聚合（`AggregatorBase.addRequestCircuitBreakerBytes`） | `search/aggregations/AggregatorBase.java:121` | REQUEST |
| 聚合桶数（`BucketsAggregator.consumeBucketsAndMaybeBreak` → `MultiBucketConsumer`） | `MultiBucketConsumerService.java:112` | 独立计数（`search.max_buckets`） |
| `BigArrays` 分配 | `common/util/BigArrays.java` | REQUEST |
| fielddata 加载 | `indices/fielddata/` | FIELDDATA |
| 网络层收发 | `transport/` | IN_FLIGHT_REQUESTS |
| Lucene 段内存 | `indices/` | ACCOUNTING |

> 第 20 章 §9 讲过聚合的记账；这里补充：
> **`search.max_buckets` 和断路器是两套独立机制** ——
> 前者限"桶数量"（逻辑），后者限"字节数"（物理）。

---

## 8. 怎么读 `Data too large` 报错

真实报错长这样：

```json
{
  "type": "circuit_breaking_exception",
  "reason": "[parent] Data too large, data for [<http_request>] would be [12345678900/11.5gb],
             which is larger than the limit of [10995116277/10.2gb],
             usages [request=0/0b, fielddata=4567890123/4.2gb, in_flight_requests=123456/120.5kb,
                     accounting=7777777777/7.2gb]",
  "bytes_wanted": 12345678900,
  "bytes_limit": 10995116277
}
```

**读法**：

1. **`[parent]`** → 是父断路器（总限）触发的；
   如果是 `[request]` / `[fielddata]`，则是单个子断路器触发的。
2. **`data for [<http_request>]`** → label，通常是 `<http_request>`（网络层）或聚合名。
3. **`usages [...]`** → **这是关键**，告诉你每个 breaker 各占了多少。
4. 常见结论：
   - `fielddata=大` → 在 text 字段上做了聚合/排序 → 改用 keyword + doc_values，或清理 fielddata（`POST /_cache/clear?fielddata=true`）。
   - `accounting=大` → Lucene 段内存（segments.memory），**分片太多**或段太多。
   - `request=大` → 单个大聚合/大查询 → 见第 20 章优化清单。
   - `in_flight_requests=大` → 并发请求体太大 → 减小 bulk 批量 / 限制并发。

---

## 9. 为什么有断路器还是会 OOM

断路器的**三个盲区**：

1. **只覆盖显式记账的路径**。Lucene 内部的临时分配、JVM 自身开销、DirectByteBuffer（不归 heap 管）不在其中。
2. **是"估算"不是"实际"**。`overhead` 只是经验系数；低估就会漏。
3. **只保护 heap**。堆外的内存（native memory）持续增长会导致**容器被 OOM-killer 杀掉**，
   表现是"节点突然消失，日志里没有 OOM 异常"。

**其它 OOM 常见原因**：

- 分片数太多 → 每个分片都有 Lucene 段内存 + 文件句柄（`ACCOUNTING` 只覆盖部分）。
- `fielddata` 没有上限（`indices.fielddata.cache.size` 默认**无限制**！）。
- 深分页 / 大 `size` 的 fetch。
- 协调节点归约阶段的内存峰值（第 19/20 章）—— **这部分往往只部分记账**。

**防护建议**：

```yaml
# fielddata 缓存上限（默认无限制！强烈建议设置）
indices.fielddata.cache.size: 20%

# 断路器（保持默认或按业务调）
indices.breaker.total.limit: 70%
indices.breaker.request.limit: 60%
indices.breaker.fielddata.limit: 40%

# 分片级保护
search.max_buckets: 65536
index.max_result_window: 10000

# 清理
POST /_cache/clear?fielddata=true
```

---

## 10. 排障与观测清单

### 10.1 必看的 API

```bash
# 断路器（tripped 计数是关键）
GET /_nodes/stats/breaker
#   breakers.<name>.{limit_size, estimated_size, overhead, tripped}

# 内存全景
GET /_nodes/stats/jvm
#   jvm.mem.heap_used_percent / old_pool_used / gc.collectors.*

# 分片与段
GET /_cat/allocation?v          # 每节点分片数 + 磁盘
GET /_cat/shards?v              # 分片状态
GET /_cat/segments?v            # 段数量与内存
GET /_nodes/stats/indices/segments,fielddata?pretty

# 线程池（rejected 很重要）
GET /_cat/thread_pool?v
GET /_nodes/stats/thread_pool

# 聚合/脚本
GET /_nodes/stats/script
GET /_nodes/stats/ingest

# 慢 GC（看日志）
grep "gc\[" logs/*.log | tail -50

# 热点线程
GET /_nodes/hot_threads?threads=10
```

### 10.2 关键告警指标（云服务实践）

| 指标 | 阈值建议 | 说明 |
| --- | --- | --- |
| `jvm.mem.heap_used_percent` | > 75% 警告，> 85% 告警 | |
| `jvm.gc.collectors.old.collection_time_in_millis`（增量） | 单次 > 1s | 用 `JvmGcMonitorService` 日志更快 |
| `breakers.*.tripped` | 任何增长 | 说明已有请求被拒 |
| `thread_pool.*.rejected` | 任何增长 | 第 06 章 |
| `indices.segments.count` | 单分片 > 100 | merge 跟不上 |
| `indices.fielddata.memory_size_in_bytes` | 持续增长 | 可能没设上限 |
| `indices.query_cache.evictions` | 持续增长 | 缓存太小 |
| `fs.total.available_in_bytes` | 低于 watermark | 第 11 章 |
| `indices.search.query_time_in_millis` | P99 突增 | |

### 10.3 排查流程

```
1. 收到 503 circuit_breaking_exception
      ↓
2. GET /_nodes/stats/breaker —— 看是哪个 breaker 大
      ├─ fielddata 大 → 清理缓存 + 检查是否有 text 字段聚合 + 设上限
      ├─ accounting 大 → 分片/段太多 → 减分片、force_merge、扩容
      ├─ request 大   → 大聚合 → 见第 20 章
      └─ in_flight 大 → 减小 bulk 批量/并发
      ↓
3. GET /_nodes/stats/jvm —— heap 是否真的高
      ↓ 高
4. GET /_nodes/hot_threads —— 谁在烧 CPU
      ↓
5. GET /_tasks?detailed=true —— 谁在跑什么
      ↓
6. 临时止血：调大对应 breaker（注意不会清零已用量！）
   根治：优化查询 / 扩容 / 减分片
```

---

## 11. 动手实验

### 实验 1：看断路器统计

```bash
GET /_nodes/stats/breaker?pretty
# 关注每个 breaker 的 estimated_size / limit_size / tripped
```

### 实验 2：触发 request breaker

```bash
# 一个巨大的 cardinality（HLL 非常吃内存）
GET /big-index/_search
{"size":0,"aggs":{"c":{"cardinality":{"field":"high_cardinality_field","precision_threshold":40000}}}}

# 观察
GET /_nodes/stats/breaker?pretty
# → request.estimated_size 上升，tripped 可能 +1
```

### 实验 3：触发 fielddata breaker

```bash
# 在 text 字段上聚合（text 没有 doc_values → 走 fielddata）
GET /my-index/_search
{"size":0,"aggs":{"t":{"terms":{"field":"text_field"}}}}

# 看 fielddata 占用
GET /_nodes/stats/indices/fielddata?pretty

# 清理
POST /_cache/clear?fielddata=true
```

### 实验 4：验证动态更新不清零

```bash
# 1. 先让 request breaker 涨上去（跑个大聚合）
# 2. 动态调小再调大
PUT /_cluster/settings
{"transient":{"indices.breaker.request.limit":"10%"}}
# → 立刻 503

PUT /_cluster/settings
{"transient":{"indices.breaker.request.limit":"60%"}}
# → estimated_size 依然很高（因为 used 被继承了）
```

### 实验 5：验证 stats 不被断路器拦

```bash
# 把 total.limit 调到极低，让所有普通请求 503
PUT /_cluster/settings {"transient":{"indices.breaker.total.limit":"1%"}}
GET /_search                       # → 503
GET /_nodes/stats?pretty           # → 依然 200 ✔（canTripCircuitBreaker=false）
# 记得改回来
PUT /_cluster/settings {"transient":{"indices.breaker.total.limit":null}}
```

### 实验 6：断点清单

| 断点 | 目的 |
| --- | --- |
| `NodeService.stats`（`:109`） | 看各维度采集 |
| `RestNodesStatsAction.prepareRequest`（`:100`） | 看 flags 解析 |
| `CommonStats.add`（`:275`） | 看统计汇总 |
| `HierarchyCircuitBreakerService.checkParentLimit`（`:243`） | 看 parent 计算与报错拼装 |
| `ChildMemoryCircuitBreaker.addEstimateBytesAndMaybeBreak`（`:123`） | 看子 breaker 的 CAS 与 parent 回滚 |
| `ChildMemoryCircuitBreaker.circuitBreak`（`:93`） | 看报错文本 |
| `HierarchyCircuitBreakerService.registerBreaker`（`:289`） | 看"继承旧 used" |
| `AggregatorBase.addRequestCircuitBreakerBytes`（`:121`） | 看聚合记账 |

### 实验 7：单元测试

```bash
./gradlew :server:test --tests "org.elasticsearch.indices.breaker.*"
./gradlew :server:test --tests "org.elasticsearch.common.breaker.*"
./gradlew :server:test --tests "org.elasticsearch.monitor.*"
```

---

## 12. 自测题（答案都在本章）

1. `NodeStats` 包含哪些维度？分别由谁提供？
2. `_nodes/stats` 不带参数会采集哪些？为什么生产上应该显式指定？
3. `indicesService.stats(true, flags)` 的第一个参数 `true` 是什么意思？有什么副作用？
4. 为什么 `RestNodesStatsAction.canTripCircuitBreaker()` 返回 false？
5. `CommonStats` 有哪 17 类统计？挑 5 个说明用途。
6. 哪些监控指标是"云服务必看"的？（至少 6 个）
7. 为什么容器环境下 `os.cpu.percent` 可能显示 -1？
8. `JvmGcMonitorService` 的三个 overhead 阈值是多少？它们的顺序要求是什么？
9. 日志里 `[gc][young]...` 那行是谁打的？它意味着什么？
10. ES 有哪 5 个断路器？各自跟踪什么？
11. `PARENT` 断路器是真实的 breaker 吗？它在哪里被计算？
12. 各 breaker 的默认 limit 和 overhead 是多少？
13. 为什么 fielddata 的 overhead 是 1.03 而 request 是 1.0？
14. 动态修改 breaker limit 后，已用量会清零吗？为什么？
15. `checkParentLimit` 和 `stats()` 里 parent 的计算方式有什么不同？会带来什么困惑？
16. `addEstimateBytesAndMaybeBreak` 里为什么在 parent 失败时要 `addWithoutBreaking(-bytes)`？
17. `limit=0` 和 `limit=-1` 分别是什么语义？
18. `CircuitBreakingException` 的 HTTP 状态码是多少？客户端应该怎么重试？
19. 断路器跳闸会打 WARN 日志吗？怎么才能看到？
20. 聚合的内存记账走哪个 breaker？（给出源码位置）
21. `search.max_buckets` 和断路器是一回事吗？区别是什么？
22. 看到 `[parent] Data too large ... usages [fielddata=...]` 应该怎么排查？
23. 看到 `usages [accounting=...]` 很大说明什么？怎么处理？
24. 为什么有断路器还是可能 OOM？（至少 3 个原因）
25. `indices.fielddata.cache.size` 默认值是多少？为什么强烈建议设置？
26. 分片数太多为什么会吃内存？这部分被哪个 breaker 覆盖？
27. `tripped` 计数一直在涨说明什么？
28. 断路器保护的是堆内还是堆外？堆外出问题会有什么表现？
29. 生产上节点内存持续走高，你会按什么顺序排查？（至少 6 步）
30. 你会为 ES 集群配置哪些告警？（至少 8 项）

---

## 13. 关键类速查表

| 类 | 路径 | 关键行 |
| --- | --- | --- |
| `NodeStats` | `action/admin/cluster/node/stats/NodeStats.java` | `class:51`、字段 `:52-95`、`ctor:97`、`getTimestamp:122`、`getHostname:127`、`getIndices:135`、`getOs:143`、`getProcess:151`、`getJvm:159`、`getThreadPool:167`、`getFs:175`、`getTransport:180`、`getHttp:185`、**`getBreaker:190`**、`getScriptStats:195`、`getDiscoveryStats:200`、`getIngestStats:205`、`getAdaptiveSelectionStats:210`、`readNodeStats:214`、`toXContent:272` |
| `NodeService` | `node/NodeService.java` | 字段 `:48-62`、**`stats:103`**（indices `:109`、os `:110`、process `:111`、jvm `:112`、threadPool `:113`、fs `:114`、transport `:115`、http `:116`、**breaker `:117`**、script `:118`、discovery `:119`、ingest `:120`、ARS `:121`）、`getMonitorService:129` |
| `RestNodesStatsAction` | `rest/action/admin/cluster/RestNodesStatsAction.java` | `class:47`、`prepareRequest:100`、**`canTripCircuitBreaker:210`** |
| `TransportNodesStatsAction` | `action/admin/cluster/node/stats/TransportNodesStatsAction.java` | `class:39`、`nodeService:44`、`NodeStatsRequest:79` |
| `CommonStats` | `action/admin/indices/stats/CommonStats.java` | `class:55`、`docs:58`、`store:61`、`indexing:64`、`get:67`、`search:70`、`merge:73`、`refresh:76`、`flush:79`、`warmer:82`、`queryCache:85`、**`fieldData:88`**、`completion:91`、`segments:94`、`translog:97`、`requestCache:100`、`recoveryStats:103`、`ctor:105/109/171`、`add:275`、`getTotalMemory:491` |
| `MonitorService` | `monitor/MonitorService.java` | `class:35`、`osService:53`、`processService:57`、`jvmService:61`、`fsService:65` |
| `JvmGcMonitorService` | `monitor/jvm/JvmGcMonitorService.java` | `class:44`、`ENABLED:55`、`REFRESH_INTERVAL:57`、`GC_COLLECTOR_PREFIX:61`、`GC_SETTING:62`、**`GC_OVERHEAD_WARN:64`(50)**、`GC_OVERHEAD_INFO:66`(25)、`GC_OVERHEAD_DEBUG:68`(10)、`GcOverheadThreshold:71`、顺序校验 `:130`、阈值构造 `:154`、运行 `:191-201`、**`SLOW_GC_LOG_MESSAGE:206`**、`logSlowGc:228` |
| `CircuitBreaker` | `common/breaker/CircuitBreaker.java` | **`FIELDDATA:43`**、**`REQUEST:51`**、**`IN_FLIGHT_REQUESTS:56`**、**`ACCOUNTING:62`**、`Type:64`、`parseValue:72`、`circuitBreak:91`、`addEstimateBytesAndMaybeBreak:99`、`addWithoutBreaking:104`、`getUsed:109`、`getLimit:114`、`getOverhead:119`、`getDurability:126` |
| `HierarchyCircuitBreakerService` | `indices/breaker/HierarchyCircuitBreakerService.java` | `class:45`、`CHILD_LOGGER_PREFIX:48`、**`TOTAL:52`(70%)**、`FIELDDATA_LIMIT:55`(60%)、`FIELDDATA_OVERHEAD:57`(1.03)、`REQUEST_LIMIT:62`(60%)、`REQUEST_OVERHEAD:64`(1.0)、`ACCOUNTING_LIMIT:69`(100%)`ACCOUNTING_OVERHEAD:71`、`IN_FLIGHT_LIMIT:76`(100%)、`IN_FLIGHT_OVERHEAD:78`、`parentTripCount:90`、`ctor:92`（parent 构造 `:118`、注册 4 个 breaker `:126-129`、**动态消费者 `:131-140`**）、`setRequestBreakerLimit:143`、`setInFlightRequestsBreakerLimit:151`、`setFieldDataBreakerLimit:160`、`setAccountingBreakerLimit:172`、`validateTotalCircuitBreakerLimit:180`、`setTotalCircuitBreakerLimit:187`、`validateSettings:196`、`getBreaker:210`、**`stats:215`**、**`stats(name):231`**、**`checkParentLimit:240`**（报错 `:249-262`）、**`registerBreaker:271`**（继承旧 breaker `:289`） |
| `ChildMemoryCircuitBreaker` | `common/breaker/ChildMemoryCircuitBreaker.java` | `ctor:51/66`（**继承旧 used `:76-77`**）、**`circuitBreak:91`**（只 debug `:97`）、**`addEstimateBytesAndMaybeBreak:110`**（limit=0 `:112`、noLimit `:121`、limit `:123`、parent 检查 `:128`、**回滚 `:133`**）、`noLimit:139`、`limit:149`（CAS 循环 `:154-…`、跳闸 `:165-169`） |
| `CircuitBreakingException` | `common/breaker/CircuitBreakingException.java` | `ctor:37/49`、**`status():71`（503）** |
| `BreakerSettings` | `indices/breaker/BreakerSettings.java` | name / limit / overhead / type / durability |
| `CircuitBreakerStats` | `indices/breaker/CircuitBreakerStats.java` | `getLimit()`、`getEstimated()`、`getOverhead()`、`getTrippedCount()` |

---

**上一篇**：[23-Snapshot与Gateway持久化.md](./23-Snapshot与Gateway持久化.md)
**下一篇**：[25-XPack与商业特性源码结构.md](./25-XPack与商业特性源码结构.md)
