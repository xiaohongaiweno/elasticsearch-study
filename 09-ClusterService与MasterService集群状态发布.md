# 09 · ClusterService 与 MasterService：集群状态发布

> 目标：搞懂「集群状态是怎么被安全地改掉并同步到所有节点的」。
> 这是 ES 分布式一致性的**核心机制**，也是**排障最高频的一层**
> （`cluster state update task ... took 10s`、`failed to commit cluster state version` 都出自这里）。
>
> 一句话总结本章：**Master 单线程执行任务 → 两阶段提交（send + commit）→ 所有节点串行 apply。**

---

## 1. 三个服务的分工

```
┌───────────────────────────────────────────────────────────────────────────────┐
│  ClusterService（cluster/service/ClusterService.java:47）                      │
│    门面（Facade）。对外暴露：state()、submitStateUpdateTask()、                 │
│    addListener()、addStateApplier()、localNode()…                             │
│    内部持有 MasterService + ClusterApplierService                              │
└───────────────┬───────────────────────────────────────┬───────────────────────┘
                │                                       │
┌───────────────▼───────────────────────────────────────▼───────────────────────┐
│  MasterService（MasterService.java:69）   │  ClusterApplierService（:69）      │
│  ★ 只有 Master 用得上                      │  ★ 每个节点都有                    │
│                                            │                                  │
│   submitStateUpdateTask(...)               │   onNewClusterState(...)          │
│        ↓                                   │        ↓                         │
│   taskBatcher（按 batchingKey 批处理）      │   threadPoolExecutor             │
│        ↓                                   │        ↓                         │
│   runTasks(TaskInputs)  (:185)             │   runTask(UpdateTask)  (:378)     │
│        ↓                                   │        ↓                         │
│   calculateTaskOutputs → 新 ClusterState   │   ★ 串行调用所有 applier          │
│        ↓                                   │   ★ 串行调用所有 listener          │
│   clusterStatePublisher.accept(...)  (:230)│   nodeConnectionsService          │
│        ↓  （= ZenDiscovery.publish）        │                                   │
└────────────────────────────────────────────┴───────────────────────────────────┘
```

**★ 三者的核心区别**：

| | `MasterService` | `ClusterApplierService` |
| --- | --- | --- |
| 谁在用 | **只有 Master 节点** | **所有节点** |
| 干什么 | 计算新状态 + 发布 | 接收并应用新状态 |
| 线程名 | `masterService#updateTask`（`:72`） | `clusterApplierService#updateTask`（`:72`） |
| 线程数 | ★ **1 个**（`PrioritizedEsThreadPoolExecutor`） | ★ **1 个** |
| 关键约束 | **所有集群状态变更是串行的** | **所有 apply 是串行的** |
| 慢任务日志 | `warnAboutSlowTaskIfNeeded`（`:528`） | 同名方法 |

> **★ 最重要的一句话**：`masterService#updateTask` 是**单线程**的。
> 任何 `ClusterStateUpdateTask.execute()` 里的慢操作都会**阻塞整个集群的元数据变更**。
> 这就是「为什么创建索引会超时」「为什么 reroute 卡住」的根因。

---

## 2. 前置：集群状态变更的"两种语义"

| 场景 | API | 是否需要广播 |
| --- | --- | --- |
| 集群级变更（创建索引、改配置、reroute） | `ClusterService.submitStateUpdateTask(...)` | ✅ 需要 publish |
| 仅本节点的状态变更（如本地节点已知节点列表） | `ClusterService.runOnApplierThread(...)` | ❌ 不 publish |

**`ClusterService` 上的两组方法**：

```java
// ① 集群级（经 MasterService → 广播）
public <T extends ClusterStateTaskConfig & ClusterStateTaskExecutor<T> & ClusterStateTaskListener>
        void submitStateUpdateTask(String source, T updateTask);                                   // :245

public <T> void submitStateUpdateTask(String source, T task, ClusterStateTaskConfig config,
                                      ClusterStateTaskExecutor<T> executor,
                                      ClusterStateTaskListener listener);                          // :258 附近

// ② 本地（经 ClusterApplierService，不走网络）
public void runOnApplierThread(String source, Consumer<ClusterState> clusterStateConsumer,
                               ClusterApplierService.ClusterApplyListener listener, Priority priority);
```

---

## 3. `ClusterService`：门面（`ClusterService.java:47`）

### 3.1 字段与构造

```java
public class ClusterService extends AbstractLifecycleComponent {
    public static final Setting<TimeValue> CLUSTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING = ...;  // :59
    public static final Setting<String> USER_DEFINED_META_DATA = Setting.prefixKeySetting(              // :60
        "cluster.metadata.", (key) -> Setting.simpleString(key, Property.Dynamic, Property.NodeScope));

    private final Settings settings;                                       // :65
    private final ClusterName clusterName;                                 // :67
    private final OperationRouting operationRouting;                       // :69  ★ 路由计算（见 19 章）
    private final ClusterSettings clusterSettings;                         // :71
    private final String nodeName;                                         // :73
    private final Map<String, Supplier<ClusterState.Custom>> initialClusterStateCustoms;  // :75

    private final MasterService masterService;
    private final ClusterApplierService clusterApplierService;

    public ClusterService(Settings settings, ClusterSettings clusterSettings, ThreadPool threadPool,
                          Map<String, Supplier<ClusterState.Custom>> initialClusterStateCustoms) {   // :77
        this.masterService = new MasterService(nodeName, settings, threadPool);                     // :82
        this.operationRouting = new OperationRouting(settings, clusterSettings);                    // :83
        this.clusterSettings.addSettingsUpdateConsumer(CLUSTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING,
            this::setSlowTaskLoggingThreshold);                                                     // :86
        // ★ addAffixUpdateConsumer + 空回调 = 让 cluster.metadata.* 的变更被记录
        this.clusterSettings.addAffixUpdateConsumer(USER_DEFINED_META_DATA, (first, second) -> {}, (first, second) -> {});  // :89
        this.clusterApplierService = new ClusterApplierService(nodeName, settings, clusterSettings,
            threadPool, this::newClusterStateBuilder);                                              // :91
    }
}
```

**★ `:89` 的小技巧**：`addAffixUpdateConsumer` 配一个"空回调"，
作用不是做事情，而是让 **`cluster.metadata.*` 变成"已知配置"**（否则 `elasticsearch.yml` 里写它会报 unknown setting）。
这叫「**注册即合法**」。

### 3.2 生命周期

```java
@Override protected synchronized void doStart() {
    clusterApplierService.start();     // ★ 注意顺序：先启动 applier
    masterService.start();             //             再启动 master service
}
@Override protected synchronized void doStop() {
    masterService.stop();              // 逆序停止
    clusterApplierService.stop();
}
```

**为什么先 start applier？** 因为 MasterService 一启动就可能产生状态，需要 applier 立刻能接。

### 3.3 关键方法

| 方法 | 行号 | 说明 |
| --- | --- | --- |
| `newClusterStateBuilder()` | `:98` | ★ 用「集群名 + 所有 initial customs」初始化的 builder |
| `state()` | `:152` | ★ **返回的是"已应用"的状态**（来自 applier，不是 master service！） |
| `localNode()` | `:136` | 本地节点（状态里没有则抛异常） |
| `operationRouting()` | `:144` | 路由计算器（见 19 章） |
| `addHighPriorityApplier` / `addStateApplier` / `addLowPriorityApplier` | `:159`/`:173`/`:166` | 注册 applier |
| `addListener(ClusterStateListener)` | — | 注册变更监听 |
| `addLocalNodeMasterListener` | — | 只关心"我是不是 master 变了" |
| `addTimeoutListener(...)` | — | 带超时的监听 |

**★ 优先级**（`ClusterApplierService.java:84-87`）：

```java
private final Collection<ClusterStateApplier> highPriorityStateAppliers = new CopyOnWriteArrayList<>();
private final Collection<ClusterStateApplier> normalPriorityStateAppliers = new CopyOnWriteArrayList<>();
private final Collection<ClusterStateApplier> lowPriorityStateAppliers = new CopyOnWriteArrayList<>();
private final Iterable<ClusterStateApplier> clusterStateAppliers =
    Iterables.concat(highPriorityStateAppliers, normalPriorityStateAppliers, lowPriorityStateAppliers);
```

| 优先级 | 谁在用 |
| --- | --- |
| **high** | `IndicesClusterStateService`（★ 分片创建/删除必须最先，见 11/18 章） |
| **normal** | 大多数服务（`MetaDataMappingService`、`SnapshotsService`…） |
| **low** | 需要「其他人都处理完」的服务（`LocalAllocationService` 后置处理、监控类） |

> **★ 排障**：如果你写的 applier 依赖某个服务已经处理完，一定要用 `addLowPriorityApplier`。
> 用 `addStateApplier` 会和你依赖的服务处在同一阶段，"谁先谁后"取决于注册顺序。

---

## 4. `MasterService`：状态变更的执行者（`MasterService.java:69`）

### 4.1 字段

```java
public class MasterService extends AbstractLifecycleComponent {
    public static final String MASTER_UPDATE_THREAD_NAME = "masterService#updateTask";        // :72

    private final String nodeName;                                                            // :74
    private BiConsumer<ClusterChangedEvent, Discovery.AckListener> clusterStatePublisher;     // :76  ★
    private java.util.function.Supplier<ClusterState> clusterStateSupplier;                   // :78
    private volatile TimeValue slowTaskLoggingThreshold;                                      // :80
    protected final ThreadPool threadPool;                                                    // :82
    private volatile PrioritizedEsThreadPoolExecutor threadPoolExecutor;                      // :84
    private volatile Batcher taskBatcher;                                                     // :85  ★
}
```

**`clusterStatePublisher` 与 `clusterStateSupplier` 都是 `set` 进来的**（`:99`/`:103`）：

```java
public synchronized void setClusterStatePublisher(BiConsumer<ClusterChangedEvent, Discovery.AckListener> publisher);
public synchronized void setClusterStateSupplier(java.util.function.Supplier<ClusterState> clusterStateSupplier);
```

**在 `Node.java` 里**：`ClusterService` 构造后由 `Node` 调
`clusterService.getMasterService().setClusterStatePublisher(discovery::publish)`，
即 **`publisher = ZenDiscovery.publish`**（`:327`）。

> **这是「解耦」**：`MasterService` 不知道 ZenDiscovery 的存在，只知道"有个地方能发布"。
> 所以测试时可以换成假的 publisher。

### 4.2 `Batcher` 与 `TaskBatcher`（★ 批处理机制）

```java
class Batcher extends TaskBatcher {                                        // :119
    Batcher(Logger logger, PrioritizedEsThreadPoolExecutor threadExecutor) {
        super(logger, threadExecutor);
    }

    @Override
    protected void onTimeout(List<? extends BatchedTask> tasks, TimeValue timeout) {   // :126
        threadPool.generic().execute(() -> {
            for (BatchedTask task : tasks) {
                UpdateTask updateTask = (UpdateTask) task;
                updateTask.listener.onFailure(task.source,
                    new ProcessClusterEventTimeoutException(timeout, task.source));
            }
        });
    }

    @Override
    protected void run(Object batchingKey, List<? extends BatchedTask> tasks, String tasksSummary) {  // :134
        runTasks(new TaskInputs(threadPoolExecutor, tasks, tasksSummary));
    }

    class UpdateTask extends BatchedTask {                                 // :140
        final ClusterStateTaskListener listener;
        ...
    }
}
```

**`TaskBatcher`（`cluster/service/TaskBatcher.java:55`）的关键设计**：

```java
public void submitTasks(List<? extends BatchedTask> tasks, @Nullable TimeValue timeout) {
    ...
    // ① 同一个 batchingKey 的任务放进同一个 LinkedHashSet
    synchronized (tasksPerBatchingKey) {
        LinkedHashSet<BatchedTask> existingTasks = tasksPerBatchingKey.computeIfAbsent(firstTask.batchingKey, k -> new LinkedHashSet<>());
        existingTasks.addAll(tasks);
    }
    // ② 提交到线程池（同名任务会"合并"成一次执行）
    if (timeout != null) {
        threadExecutor.execute(firstTask, timeout, () -> onTimeoutInternal(tasks, timeout));
    } else {
        threadExecutor.execute(firstTask);
    }
}
```

**★ `batchingKey` 是什么？**

就是 `ClusterStateTaskExecutor` 实例本身（或它自己定义的东西）。
**当同一个 executor 有多个待执行任务时，它们会被"打包"成一次执行**：

```java
// TaskBatcher.runIfNotProcessed
LinkedHashSet<BatchedTask> pending = tasksPerBatchingKey.remove(updateTask.batchingKey);   // :130
// 一次性全部取出 → 传给 run(batchingKey, toExecute, tasksSummary)
```

**为什么这很重要？**

- 你**批量**创建 100 个索引 → `MetaDataCreateIndexService` 的 executor 只被调一次，
  `execute(currentState, List<Task>)` 拿到 100 个任务，一次算完新状态
  → **只发布 1 次集群状态，而不是 100 次**。这是 ES 能抗住高并发元数据操作的关键。
- 反过来，**不同 executor 的任务不会合并**。

**破坏这一点的行为**：在 `ClusterStateTaskExecutor` 里给每个任务创建**新的 executor 实例**
（每次 `new`）→ 无法批处理 → 集群状态发布次数暴增。
> **★ 写插件时的经典坑**：`ClusterStateTaskExecutor` 必须用**单例/常量**。

### 4.3 提交任务的两个重载

```java
// :307 —— 最简洁：task 自己就是 config + executor + listener
public <T extends ClusterStateTaskConfig & ClusterStateTaskExecutor<T> & ClusterStateTaskListener>
        void submitStateUpdateTask(String source, T updateTask) {
    submitStateUpdateTask(source, updateTask, updateTask, updateTask, updateTask);
}

// :332 —— 完整版
public <T> void submitStateUpdateTask(String source, T task,
                                      ClusterStateTaskConfig config,
                                      ClusterStateTaskExecutor<T> executor,
                                      ClusterStateTaskListener listener) { ... }
```

**`ClusterStateTaskConfig`**（`cluster/ClusterStateTaskConfig.java`）：

```java
public interface ClusterStateTaskConfig {
    @Nullable TimeValue timeout();     // 任务超时（超时后调 listener.onFailure(ProcessClusterEventTimeoutException)）
    Priority priority();               // URGENT / HIGH / NORMAL / LOW

    class Basic implements ClusterStateTaskConfig {
        public Basic(Priority priority, TimeValue timeout) { ... }     // :77
    }
}
```

**★ `Priority` 的实际影响**：`PrioritizedEsThreadPoolExecutor` 是一个**优先级队列**。
`URGENT` 的任务会插到队首。**谁用 URGENT？**

| 来源 | 优先级 | 理由 |
| --- | --- | --- |
| `ZenDiscovery` 的集群状态应用 | `URGENT` | 不能等 |
| `ClusterStateUpdateTask` 的默认 | `NORMAL` | |
| 索引创建/删除（`MetaDataCreateIndexService`） | `HIGH` | 用户等着的操作 |
| 各种后台任务 | `LOW` | 如 `IndexGraveyard` 清理 |

**★ 排障**：`GET /_cluster/pending_tasks` 能**看到队列里每个任务的 `priority` 和 `insert_order`**（`PendingClusterTask.java`）。
如果看到大量 `NORMAL` 排在前面、`HIGH` 在后面 → 就是被低优先级任务堵住了。

### 4.4 ★ `runTasks`（`:185`）逐段精读

```java
protected void runTasks(TaskInputs taskInputs) {
    final String summary = taskInputs.summary;
    if (!lifecycle.started()) {
        logger.debug("processing [{}]: ignoring, master service not started", summary);
        return;
    }

    logger.debug("processing [{}]: execute", summary);
    final ClusterState previousClusterState = state();

    // ★① 如果本节点已经不是 master，且任务只能在 master 执行 → 直接失败
    if (!previousClusterState.nodes().isLocalNodeElectedMaster() && taskInputs.runOnlyWhenMaster()) {
        logger.debug("failing [{}]: local node is no longer master", summary);
        taskInputs.onNoLongerMaster();                                // → listener.onNoLongerMaster(...)
        return;
    }

    long startTimeNS = currentTimeInNanos();
    // ★② 执行所有 executor，算出新状态
    TaskOutputs taskOutputs = calculateTaskOutputs(taskInputs, previousClusterState, startTimeNS);
    taskOutputs.notifyFailedTasks();                                  //   失败任务的回调

    // ★③ 状态没变 → 跳过 publish
    if (taskOutputs.clusterStateUnchanged()) {
        taskOutputs.notifySuccessfulTasksOnUnchangedClusterState();    //   仍然要通知"成功"
        TimeValue executionTime = ...;
        logger.debug("processing [{}]: took [{}] no change in cluster state", summary, executionTime);
        warnAboutSlowTaskIfNeeded(executionTime, summary);             //   ★ 慢任务告警
    } else {
        ClusterState newClusterState = taskOutputs.newClusterState;
        if (logger.isTraceEnabled()) {
            logger.trace("cluster state updated, source [{}]\n{}", summary, newClusterState);
        } else if (logger.isDebugEnabled()) {
            logger.debug("cluster state updated, version [{}], source [{}]", newClusterState.version(), summary);
        }
        try {
            // ★④ 构造 ClusterChangedEvent
            ClusterChangedEvent clusterChangedEvent = new ClusterChangedEvent(summary, newClusterState, previousClusterState);

            final DiscoveryNodes.Delta nodesDelta = clusterChangedEvent.nodesDelta();
            if (nodesDelta.hasChanges() && logger.isInfoEnabled()) {
                String nodeSummary = nodesDelta.shortSummary();
                if (nodeSummary.length() > 0) {
                    logger.info("{}, reason: {}", summary, nodeSummary);   // ★ 你在日志里看到的 "added {{node1}}, reason: ..."
                }
            }

            logger.debug("publishing cluster state version [{}]", newClusterState.version());
            try {
                // ★⑤★ 阻塞等待发布 + 提交
                clusterStatePublisher.accept(clusterChangedEvent,
                    taskOutputs.createAckListener(threadPool, newClusterState));
            } catch (Discovery.FailedToCommitClusterStateException t) {
                final long version = newClusterState.version();
                logger.warn(() -> new ParameterizedMessage(
                        "failing [{}]: failed to commit cluster state version [{}]", summary, version), t);
                taskOutputs.publishingFailed(t);
                return;
            }

            // ★⑥ 状态"处理过了"（即使别的 master 已经发布了更新的版本）
            taskOutputs.processedDifferentClusterState(previousClusterState, newClusterState);

            try {
                // ★⑦ 通知 executor「状态已发布」（ClusterStateTaskExecutor.clusterStatePublished）
                taskOutputs.clusterStatePublished(clusterChangedEvent);
            } catch (Exception e) {
                logger.error(() -> new ParameterizedMessage(
                        "exception thrown while notifying executor of new cluster state publication [{}]", summary), e);
            }
            ...
        } catch (Exception e) {
            // ⚠️ 注意这里的 TODO：publish 之后的异常不再回调 onFailure
            logger.warn(() -> new ParameterizedMessage(
                    "failed to publish updated cluster state in [{}]:\nversion [{}], uuid [{}], source [{}]\n{}", ...), e);
            // TODO: do we want to call updateTask.onFailure here?
        }
    }
}
```

**★ 三个必须理解的点**：

| 标记 | 含义 |
| --- | --- |
| ① | `runOnlyWhenMaster` 决定"失去 master 身份后任务失败还是继续" |
| ③ | **状态不变时不会 publish**——所以「改了配置但值没变」不会产生新版本 |
| ⑥ | `processedDifferentClusterState` 的语义：**即使最终集群状态和我算的不一样，我的任务也算"处理过了"** |

### 4.5 ★ `patchVersions`（`:278`）——版本号由 Master 独占控制

```java
private ClusterState patchVersions(ClusterState previousClusterState, ClusterTasksResult<?> executionResult) {
    ClusterState newClusterState = executionResult.resultingState;

    if (previousClusterState != newClusterState) {
        // only the master controls the version numbers
        Builder builder = ClusterState.builder(newClusterState).incrementVersion();                    // ★ 总是 +1
        if (previousClusterState.routingTable() != newClusterState.routingTable()) {
            builder.routingTable(RoutingTable.builder(newClusterState.routingTable())
                .version(newClusterState.routingTable().version() + 1).build());                       // ★ 只在变了时 +1
        }
        if (previousClusterState.metaData() != newClusterState.metaData()) {
            builder.metaData(MetaData.builder(newClusterState.metaData())
                .version(newClusterState.metaData().version() + 1));                                   // ★ 只在变了时 +1
        }
        newClusterState = builder.build();
    }
    return newClusterState;
}
```

**★ 这段代码解释了第 8 章的问题**：

- `ClusterState.version` **每次** publish 都 +1
- `RoutingTable.version` / `MetaData.version` **只在对应内容变了**才 +1

**判定"变了"用的是引用比较（`!=`）**，不是内容比较！
所以：
- 你在 executor 里**总是** new 一个 `MetaData`（即使内容一样）→ `MetaData.version` 会一直涨
- 你**正确**地复用原来的 `MetaData` 引用（`previousState.metaData()`）→ 版本不变

> **★ 这是写 `ClusterStateTaskExecutor` 的性能要点**：
> 没变化就**返回原来的 state**（`ClusterTasksResult.builder().successes(tasks).build(currentState)`），
> 不要 `ClusterState.builder(currentState).build()`。

### 4.6 `TaskOutputs`（`:342`）

```java
class TaskOutputs {
    public final TaskInputs taskInputs;
    public final ClusterState previousClusterState;
    public final ClusterState newClusterState;
    public final List<Batcher.UpdateTask> nonFailedTasks;
    public final Map<Object, ClusterStateTaskExecutor.TaskResult> executionResults;

    public void publishingFailed(Discovery.FailedToCommitClusterStateException t) { ... }         // :360
    public void processedDifferentClusterState(ClusterState previousClusterState, ClusterState newClusterState) { ... }  // :364
    public void clusterStatePublished(ClusterChangedEvent clusterChangedEvent) { ... }           // :368
    public Discovery.AckListener createAckListener(ThreadPool threadPool, ClusterState newClusterState) { ... }  // :372
    public boolean clusterStateUnchanged() { ... }                                                // :380
    public void notifyFailedTasks() { ... }                                                       // :384
    public void notifySuccessfulTasksOnUnchangedClusterState() { ... }                            // :395
}
```

**`createAckListener`（`:372`）的产物**：

```java
// :535 DelegatingAckListener + :558 AckCountDownListener
private static class AckCountDownListener implements Discovery.AckListener {
    private final AckedClusterStateTaskListener ackedTaskListener;
    private final CountDown countDown;
    private final DiscoveryNode masterNode;
    ...
}
```

它把「多少个节点 ack 了」转化成一个 `CountDown`，
全部 ack → `ackedTaskListener.onAllNodesAcked(null)`；
超时 → `ackedTaskListener.onAckTimeout()`。

### 4.7 ★ Ack 机制

`AckedClusterStateUpdateTask`（`cluster/AckedClusterStateUpdateTask.java`）：

```java
public abstract class AckedClusterStateUpdateTask<Response extends ClusterStateUpdateResponse, Request extends AckedRequest>
        extends ClusterStateUpdateTask {
    private final ActionListener<Response> listener;                                    // :34
    private final AckedRequest request;                                                 // :35

    public boolean mustAck(DiscoveryNode discoveryNode) {                               // :53
        return discoveryNode.isMasterNode();      // ★ 默认：只等 master 节点（子类可覆写）
    }
    public void onAllNodesAcked(@Nullable Exception e) { ... }                          // :63
    public void onAckTimeout() { ... }                                                  // :73
    public void onFailure(String source, Exception e) { ... }                           // :78
    public TimeValue ackTimeout() { ... }                                               // :85  → request.ackTimeout()
    public TimeValue timeout() { ... }                                                 // :90  → request.masterNodeTimeout()
}
```

`AckedClusterStateTaskListener`（`cluster/AckedClusterStateTaskListener.java`）：

```java
public interface AckedClusterStateTaskListener extends ClusterStateTaskListener {
    boolean mustAck(DiscoveryNode discoveryNode);          // :37  ★ 是否要等这个节点
    void onAllNodesAcked(@Nullable Exception e);           // :45  ★ 全部 ack
    void onAckTimeout();                                   // :51  ★ ack 超时
    TimeValue ackTimeout();                                //      ack 超时时间
}
```

**★ `mustAck` 的语义**（高频考点）：

| 覆写值 | 场景 | 意义 |
| --- | --- | --- |
| `discoveryNode.isMasterNode()`（默认） | 大多数 | 只要多数 master 节点看到就行 |
| `true` | 需要所有节点确认 | 例如 `TransportPutMappingAction`? 不，它用默认 |
| 自定义 | `TransportCreateIndexAction` 等 | 见具体实现 |

**★ 关键区分**：

| 概念 | 含义 | 配置 |
| --- | --- | --- |
| `masterNodeTimeout` / `timeout()` | Master 自己算状态 + 发布的最大时间 | `?master_timeout=30s` |
| `ackTimeout()` | 等节点 ack 的最大时间 | `?timeout=30s` |
| `wait_for_active_shards` | 等分片 active（不是 ack） | `?wait_for_active_shards=1` |

**★ 排障**：
- `process cluster event timeout while waiting for ack` → `ackTimeout`
- `Failed to acknowledge cluster state change` → ack 失败
- `master_timeout` → `ProcessClusterEventTimeoutException: cluster state update task ... timeout after [30s]`

---

## 5. ★★ 两阶段提交：`PublishClusterStateAction`

这是**整章最核心的部分**。文件：`discovery/zen/PublishClusterStateAction.java`

### 5.1 两个内部 action

```java
public static final String SEND_ACTION_NAME = "internal:discovery/zen/publish/send";       // :74
public static final String COMMIT_ACTION_NAME = "internal:discovery/zen/publish/commit";   // :75
```

**★ 阶段一（send）**：把状态（或 diff）发给所有节点，节点**只暂存到 `PendingClusterStatesQueue`，不 apply**。
**★ 阶段二（commit）**：告诉节点"这个版本可以 apply 了"，节点才真正 `applyClusterState`。

### 5.2 为什么要两阶段？

**经典的分布式问题**：Master 把状态发给 5 个节点，前 4 个收到了、第 5 个没收到，
这时 Master 自己挂了 → 前 4 个认为"新状态生效了"，第 5 个认为"还是旧状态"
→ **集群状态分裂（split brain）**。

**两阶段的解法**：
1. 所有人都**先拿着但不生效**（send）
2. Master 确认收到了**足够的 master 节点**的 ack 后（commit 条件），再广播 commit
3. 拿到了 commit 的节点才 apply

**★ 提交条件**（`SendingController` 构造器，`:510`）：

```java
private SendingController(ClusterState clusterState, int minMasterNodes, int totalMasterNodes,
                          BlockingClusterStatePublishResponseHandler publishResponseHandler) {
    this.neededMastersToCommit = Math.max(0, minMasterNodes - 1); // ★ we are one of the master nodes
    this.pendingMasterNodes = totalMasterNodes - 1;
    if (this.neededMastersToCommit > this.pendingMasterNodes) {
        throw new Discovery.FailedToCommitClusterStateException("not enough masters to ack sent cluster state." +
            "[{}] needed , have [{}]", neededMastersToCommit, pendingMasterNodes);
    }
    this.committed = neededMastersToCommit == 0;      // ★ 如果 minMasterNodes=1，立刻算 committed
    this.committedOrFailedLatch = new CountDownLatch(committed ? 0 : 1);
}
```

**所以**：

```
neededMastersToCommit = minMasterNodes - 1        (减掉自己)
即「除我之外，还需要至少 (minMasterNodes - 1) 个 master 节点 ack」
```

**`checkForCommitOrFailIfNoPending`（`:568`）**：

```java
private synchronized void checkForCommitOrFailIfNoPending(DiscoveryNode masterNode) {
    neededMastersToCommit--;
    if (neededMastersToCommit == 0) {
        if (markAsCommitted()) {
            // ★ 把「commit 之前就 ack 了的节点」补发 commit
            for (DiscoveryNode nodeToCommit : sendAckedBeforeCommit) {
                sendCommitToNode(nodeToCommit, clusterState, this);
            }
            sendAckedBeforeCommit.clear();
        }
    }
    decrementPendingMasterAcksAndChangeForFailure();
}
```

**`decrementPendingMasterAcksAndChangeForFailure`（`:583`）**：

```java
pendingMasterNodes--;
if (pendingMasterNodes == 0 && neededMastersToCommit > 0) {
    markAsFailed("no more pending master nodes, but failed to reach needed acks ([" + neededMastersToCommit + "] left)");
}
```

**★ 这就是 `discovery.zen.minimum_master_nodes` 的真正作用**：
如果没凑够 `minMasterNodes` 个 master 节点确认，**状态发布直接失败**，
Master 会 `rejoin("zen-disco-failed-to-publish")`（`ZenDiscovery.java:349`）。

> **云服务启示**：`minMasterNodes` 配置错误是**最常见的生产事故原因**。
> 正确值 = `master候选节点数 / 2 + 1`。
> 6 节点 master 候选 → `minMasterNodes = 4`。
> **设太小 → 可能脑裂；设太大 → 挂 1 个节点就无法改集群状态（连 `PUT /idx` 都做不了）。**

### 5.3 `publish`（`:128`）逐段精读

```java
public void publish(final ClusterChangedEvent clusterChangedEvent, final int minMasterNodes,
                    final Discovery.AckListener ackListener) throws Discovery.FailedToCommitClusterStateException {
    final DiscoveryNodes nodes;
    final SendingController sendingController;
    final Set<DiscoveryNode> nodesToPublishTo;
    final Map<Version, BytesReference> serializedStates;
    final Map<Version, BytesReference> serializedDiffs;
    final boolean sendFullVersion;
    try {
        nodes = clusterChangedEvent.state().nodes();
        nodesToPublishTo = new HashSet<>(nodes.getSize());
        DiscoveryNode localNode = nodes.getLocalNode();
        final int totalMasterNodes = nodes.getMasterNodes().size();
        for (final DiscoveryNode node : nodes) {
            if (node.equals(localNode) == false) {
                nodesToPublishTo.add(node);                          // ★ 除自己以外的所有节点
            }
        }
        sendFullVersion = !discoverySettings.getPublishDiff() || clusterChangedEvent.previousState() == null;
        serializedStates = new HashMap<>();
        serializedDiffs = new HashMap<>();

        // ★ 先序列化好（按版本缓存），避免每个节点都序列化一次
        buildDiffAndSerializeStates(clusterChangedEvent.state(), clusterChangedEvent.previousState(),
                nodesToPublishTo, sendFullVersion, serializedStates, serializedDiffs);

        final BlockingClusterStatePublishResponseHandler publishResponseHandler =
            new AckClusterStatePublishResponseHandler(nodesToPublishTo, ackListener);
        sendingController = new SendingController(clusterChangedEvent.state(), minMasterNodes,
            totalMasterNodes, publishResponseHandler);
    } catch (Exception e) {
        throw new Discovery.FailedToCommitClusterStateException("unexpected error while preparing to publish", e);
    }

    try {
        innerPublish(clusterChangedEvent, nodesToPublishTo, sendingController, ackListener, sendFullVersion,
            serializedStates, serializedDiffs);
    } catch (Discovery.FailedToCommitClusterStateException t) {
        throw t;
    } catch (Exception e) {
        if (sendingController.markAsFailed("unexpected error", e)) {
            throw new Discovery.FailedToCommitClusterStateException("unexpected error", e);
        } else {
            throw e;   // ★ 已经 committed 了 → 只是"出错了"，不是"没提交"
        }
    }
}
```

**★ 方法注释里写明了契约**（`:124`）：

```java
/**
 * The method is guaranteed to throw a {@link org.elasticsearch.discovery.Discovery.FailedToCommitClusterStateException}
 * if the change is not committed and should be rejected.
 * Any other exception signals the something wrong happened but the change is committed.
 */
```

**这句话极其重要**：
- 抛 `FailedToCommitClusterStateException` → **没提交，可以安全重试**
- 抛**别的**异常 → **已经提交了！不能重试**（否则会重复执行）

**这是 `MasterService.runTasks`（`:231`）里只对 `FailedToCommitClusterStateException` 特殊处理的原因。**

### 5.4 `innerPublish`（`:181`）逐段精读

```java
private void innerPublish(...) {
    final ClusterState clusterState = clusterChangedEvent.state();
    final ClusterState previousState = clusterChangedEvent.previousState();
    final TimeValue publishTimeout = discoverySettings.getPublishTimeout();
    final long publishingStartInNanos = System.nanoTime();

    // ★ 阶段一：send
    for (final DiscoveryNode node : nodesToPublishTo) {
        if (sendFullVersion || !previousState.nodes().nodeExists(node)) {
            sendFullClusterState(clusterState, serializedStates, node, publishTimeout, sendingController);       // :197
        } else {
            sendClusterStateDiff(clusterState, serializedDiffs, serializedStates, node, publishTimeout, sendingController);  // :199
        }
    }

    // ★ 关键：等 commit 条件满足（minMasterNodes 个 master 节点 ack）
    sendingController.waitForCommit(discoverySettings.getCommitTimeout());         // :203

    final long commitTime = System.nanoTime() - publishingStartInNanos;
    ackListener.onCommit(TimeValue.timeValueNanos(commitTime));                    // ★ 通知「已提交」

    try {
        long timeLeftInNanos = Math.max(0, publishTimeout.nanos() - commitTime);
        final BlockingClusterStatePublishResponseHandler publishResponseHandler = sendingController.getPublishResponseHandler();
        // ★ 阶段二后续：等所有节点 apply 完成
        sendingController.setPublishingTimedOut(!publishResponseHandler.awaitAllNodes(TimeValue.timeValueNanos(timeLeftInNanos)));
        if (sendingController.getPublishingTimedOut()) {
            DiscoveryNode[] pendingNodes = publishResponseHandler.pendingNodes();
            if (pendingNodes.length > 0) {
                logger.warn("timed out waiting for all nodes to process published state [{}] (timeout [{}], pending nodes: {})",
                    clusterState.version(), publishTimeout, pendingNodes);          // ★ 你在日志里看到的这条
            }
        }
        Set<DiscoveryNode> failedNodes = publishResponseHandler.getFailedNodes();
        if (failedNodes.isEmpty() == false) {
            logger.warn("publishing cluster state with version [{}] failed for the following nodes: [{}]",
                clusterChangedEvent.state().version(), failedNodes);                // ★ 另一条常见告警
        }
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
    }
}
```

**★ 时间线**：

```
t0  ──────── send 到所有节点 ────────► 各节点暂存（不 apply）
t1  ──────── 等 minMasterNodes 个 master 节点 ack ────────►
t2  ★ COMMIT 点：到这里为止"状态已提交，不可回滚"
    ──────── 向所有已 ack 的节点发 commit ────────► 节点 apply
    ackListener.onCommit(commitTime)
t3  ──────── 等所有节点 apply 完成（最长 publish_timeout）────────────────►
    ackListener.onNodeAck(node, null)  ← 每个节点 apply 完都会回调
```

**★ 三个超时设置**：

| 设置 | 默认 | 作用 |
| --- | --- | --- |
| `discovery.zen.publish_timeout` | 30s | ★ **整个 publish 的总超时**（含等所有节点 apply） |
| `discovery.zen.commit_timeout` | 30s | 等 `minMasterNodes` 个 master 节点 ack 的时间 |
| `discovery.zen.publish_diff.enable` | true | 是否使用 diff |

### 5.5 接收端（从节点）

```java
protected void handleIncomingClusterStateRequest(BytesTransportRequest request, TransportChannel channel) throws IOException {  // :376
    try (StreamInput in = request.data().streamInput()) {
        final ClusterState incomingState;
        synchronized (lastSeenClusterStateMutex) {
            incomingState = ClusterState.readFrom(in, lastSeenClusterState);         // ★ 基于"上一次看到的"读 diff
            lastSeenClusterState = incomingState;
        }
        logger.debug("received cluster state version {}", incomingState.version());
        incomingClusterStateListener.onIncomingClusterState(incomingState);          // ★ 暂存到 PendingClusterStatesQueue
        channel.sendResponse(TransportResponse.Empty.INSTANCE);                      // ★ 回 ack（表示"我收到了"）
    }
}

protected void handleCommitRequest(CommitClusterStateRequest request, final TransportChannel channel) {   // :418
    incomingClusterStateListener.onClusterStateCommitted(request.stateUUID, new ActionListener<Void>() {
        @Override
        public void onResponse(Void ignore) {
            try { channel.sendResponse(TransportResponse.Empty.INSTANCE); }          // ★ 回 ack（表示"我 apply 完了"）
            catch (Exception e) { logger.debug("failed to send response on cluster state processed", e); }
        }
        @Override
        public void onFailure(Exception e) {
            try { channel.sendResponse(e); }
            catch (Exception inner) { logger.debug("failed to send response on cluster state processed", inner); }
        }
    });
}
```

**★ 两次回 ack 的含义不同**：

| ack 时机 | 在 `SendingController` 里对应 |
| --- | --- |
| send 收到 | `onNodeSendAck(node)`（`:545`）→ 可能触发 commit |
| commit 收到并 apply 完 | `publishResponseHandler.onNodeAck(...)` → 结束等待 |

**★ 从节点的排队**：`PendingClusterStatesQueue`（`discovery/zen/PendingClusterStatesQueue.java`）
最多保留 **N 个**（默认 `discovery.zen.cluster_state_publish_timeout`? 实际是硬编码 25）。
如果 Master 发得太快，从节点会丢弃中间的（只保留最新的）。

---

## 6. `ClusterApplierService`：应用状态（`ClusterApplierService.java:69`）

### 6.1 字段

```java
public static final String CLUSTER_UPDATE_THREAD_NAME = "clusterApplierService#updateTask";     // :72

private final ClusterSettings clusterSettings;                                                  // :74
protected final ThreadPool threadPool;                                                          // :75
private volatile TimeValue slowTaskLoggingThreshold;                                            // :77
private volatile PrioritizedEsThreadPoolExecutor threadPoolExecutor;                            // :79

private final Collection<ClusterStateApplier> highPriorityStateAppliers = new CopyOnWriteArrayList<>();     // :84
private final Collection<ClusterStateApplier> normalPriorityStateAppliers = new CopyOnWriteArrayList<>();   // :85
private final Collection<ClusterStateApplier> lowPriorityStateAppliers = new CopyOnWriteArrayList<>();      // :86
private final Iterable<ClusterStateApplier> clusterStateAppliers = ...;                                     // :87

private final Collection<ClusterStateListener> clusterStateListeners = new CopyOnWriteArrayList<>();        // :90
private final Collection<TimeoutClusterStateListener> timeoutClusterStateListeners = ...;                   // :91
private final LocalNodeMasterListeners localNodeMasterListeners;                                            // :94
private final Queue<NotifyTimeout> onGoingTimeouts = ConcurrentCollections.newQueue();                      // :96
private final AtomicReference<ClusterState> state; // ★ last applied state                                // :98

private NodeConnectionsService nodeConnectionsService;                                                       // :102
private Supplier<ClusterState.Builder> stateBuilderSupplier;                                                 // :103
```

### 6.2 `runTask`（`:378`）——apply 的执行

```java
protected void runTask(UpdateTask task) {
    if (!lifecycle.started()) {
        logger.debug("processing [{}]: ignoring, cluster applier service not started", task.source);
        return;
    }
    logger.debug("processing [{}]: execute", task.source);
    final ClusterState previousClusterState = state.get();

    // ... 时间统计
    final ClusterState newClusterState;
    try {
        newClusterState = task.apply(previousClusterState);            // ★ 应用"更新函数"
    } catch (Exception e) {
        // ...
        return;
    }

    // ★ 有变化才继续
    if (previousClusterState != newClusterState) {
        // 校验版本单调递增
        if (newClusterState.version() < previousClusterState.version()) {
            throw new IllegalStateException("Updating cluster state from version [" + previousClusterState.version() +
                                            "] with a field into the past [" + newClusterState.version() + "]");
        }
        // ★ ① 先更新引用
        final long version = newClusterState.version();
        final String stateUUID = newClusterState.stateUUID();
        final String fullState = newClusterState.toString();
        // ... 日志

        // ★ ② 连接管理：先连上所有新节点（见 NodeConnectionsService）
        // ★ ③ 调用所有 applier（高 → 普通 → 低优先级）
        // ★ ④ 调用所有 listener
        // ★ ⑤ 更新 state 引用
        ...
    }
    // ... 慢任务告警
}
```

**★ apply 的顺序极其重要**：

```
1. nodeConnectionsService.connectToNodes(...)        ← 先把新节点连上（否则后续操作拿不到连接）
2. 校验（版本、UUID）
3. highPriorityStateAppliers                         ← 如 IndicesClusterStateService（创建/删除分片）
4. normalPriorityStateAppliers
5. lowPriorityStateAppliers
6. localNodeMasterListeners                          ← 「我是否成为/失去 master」
7. clusterStateListeners                             ← 各种业务监听
8. state.set(newClusterState)                        ← ★ 最后才更新 state 引用！
```

**★ 为什么最后才更新 `state` 引用？**
因为 `ClusterStateListener.clusterStateChanged(...)` 里可能会调 `clusterService.state()`，
此时应该拿到**新状态**（已 set）。但如果 applier 里抛异常，就不会走到 set。
→ 实际上 `state` 的更新是在 appliers **之前**还是**之后**，是 ES 里一个微妙的设计点：
**它先 set，再通知**（见源码 `runTask` 的实际实现），这样 listener 里 `state()` 拿到的就是新状态。

> **★ 排障**：如果某个 applier 抛异常，ES **不会**回滚状态，而是记录异常并继续。
> 所以「状态不一致」的 bug 往往来自 applier 里的异常被吞掉。

### 6.3 `StateApplier` vs `StateListener`

| 接口 | 路径 | 语义 | 何时用 |
| --- | --- | --- | --- |
| `ClusterStateApplier` | `cluster/ClusterStateApplier.java` | **改变状态的处理**（可改本地服务内部状态） | 服务内部逻辑（创建分片、更新路由） |
| `ClusterStateListener` | `cluster/ClusterStateListener.java` | **只读观察** | 业务逻辑（如 Snapshot 检查索引是否还在） |
| `LocalNodeMasterListener` | `cluster/LocalNodeMasterListener.java` | 只关心"我是不是 master" | 需要做 master-only 初始化的服务 |
| `TimeoutClusterStateListener` | `cluster/TimeoutClusterStateListener.java` | 带超时的监听 | 等待某个状态出现 |

**★ 写插件时的选择**：
- 要**改服务状态** → `addStateApplier` / `addHighPriorityApplier`
- 只是**知道有变化** → `addListener`
- 要看**新索引/新节点** → 用 `ClusterChangedEvent`（`:89` `routingTableChanged()`、`:111` `indicesCreated()`）

### 6.4 `ClusterChangedEvent`（`cluster/ClusterChangedEvent.java:43`）

```java
public class ClusterChangedEvent {
    private final String source;                          // :45  ★ 变更来源（就是 submitStateUpdateTask 的 source）
    private final ClusterState previousState;              // :47
    private final ClusterState state;                      // :49
    private final DiscoveryNodes.Delta nodesDelta;         // :51

    public String source()                                 // :66
    public ClusterState state()                            // :73
    public ClusterState previousState()                    // :80
    public boolean routingTableChanged()                   // :89
    public boolean indexRoutingTableChanged(String index)  // :97
    public List<String> indicesCreated()                   // :111  ★ 新创建的索引
    public List<Index> indicesDeleted()                    // :131  ★ 被删除的索引
    public boolean metaDataChanged()                       // :146
    public Set<String> changedCustomMetaDataSet()          // :155
    public boolean blocksChanged()                         // :193
    public boolean localNodeMaster()                       // :200  ★ 我是否是新 master
    public DiscoveryNodes.Delta nodesDelta()               // :208
    public boolean nodesRemoved() / nodesAdded() / nodesChanged()   // :215/:222/:229
    public boolean isNewCluster()                          // :240
}
```

**★ `source()` 的价值**：所有集群状态变更**都必须带一个 source 字符串**。
你会在日志里看到：

```
[2026-01-01T00:00:00,000][INFO ][o.e.c.s.MasterService    ] [node1] created-index, reason: created index [myindex]
```

前面的 `created-index` 就是 `source`。
**排障第一招：`grep MasterService` 看 source，就知道集群状态被谁改了。**

**`indicesDeleted()`（`:131`）特别注意**（`:248`/`:273`）：
它同时考虑 `MetaData` 里的删除**和** `IndexGraveyard` 的墓碑，
所以"删除索引"这个事件在墓碑存在期间会**重复报告**。

---

## 7. ★ 完整时序：创建一个索引的 5 个阶段

```
【客户端】POST /myindex
    ↓
【http_server_worker】REST → TransportCreateIndexAction（见 06/07 章）
    ↓ 这是 TransportMasterNodeAction
【任意节点】判断自己是否 master
    ├─ 是 → 继续
    └─ 否 → 转发给 master（master_timeout）
    ↓
【Master 的 masterService#updateTask 线程】★ 只有 1 个
┌─ 阶段 1：入队与批处理 ────────────────────────────────────────────────┐
│ masterService.submitStateUpdateTask("create-index", task,            │
│     ClusterStateTaskConfig.build(Priority.URGENT, masterTimeout),    │
│     createIndexTaskExecutor, listener)                               │
│ → TaskBatcher.submitTasks(...)                                       │
│ → 和其他同一 executor 的任务合并                                       │
└──────────────────────────────────────────────────────────────────────┘
┌─ 阶段 2：计算新状态（execute）────────────────────────────────────────┐
│ Batcher.run(batchingKey, tasks, summary) → runTasks(TaskInputs)      │
│   ├─ 检查还是不是 master（:195）                                      │
│   ├─ calculateTaskOutputs（:271）                                    │
│   │    └─ executor.execute(currentState, tasks)  ★ 你的业务代码       │
│   │         → MetaDataCreateIndexService.createIndex(...)            │
│   │         → 返回 ClusterTasksResult{resultingState, executionResults}│
│   └─ patchVersions（:278）→ ClusterState.version +1, MetaData.version +1│
└──────────────────────────────────────────────────────────────────────┘
┌─ 阶段 3：两阶段提交（send）──────────────────────────────────────────┐
│ clusterStatePublisher.accept(event, ackListener)                     │
│   = ZenDiscovery.publish（:327）                                     │
│     ├─ pendingStatesQueue.addPending(newState)                       │
│     └─ PublishClusterStateAction.publish（:128）                     │
│          ├─ buildDiffAndSerializeStates（:233）★ 按版本序列化一次      │
│          └─ innerPublish（:181）                                     │
│               └─ 对每个节点 sendFullClusterState / sendClusterStateDiff│
│                   （action = internal:discovery/zen/publish/send）    │
└──────────────────────────────────────────────────────────────────────┘
┌─ 阶段 4：等待 commit 条件 ───────────────────────────────────────────┐
│ 从节点收到 send → PendingClusterStatesQueue 暂存 → 回 ack             │
│ Master 收到 ack → onNodeSendAck（:545）                              │
│   └─ 如果 ack 来自 master 节点 → checkForCommitOrFailIfNoPending（:568）│
│         neededMastersToCommit-- （初始 = minMasterNodes - 1）          │
│         到 0 → markAsCommitted() → 向已 ack 的节点 sendCommitToNode    │
│                                   （action = .../publish/commit）     │
│ 若 pendingMasterNodes 耗尽仍不够 → markAsFailed → FailedToCommitClusterStateException│
│                                    → Master 调 rejoin() 重新加入集群   │
└──────────────────────────────────────────────────────────────────────┘
┌─ 阶段 5：apply ─────────────────────────────────────────────────────┐
│ 从节点收到 commit → IncomingClusterStateListener.onClusterStateCommitted│
│   → PendingClusterStatesQueue 取出 → processNextCommittedClusterState │
│      → ClusterApplierService.onNewClusterState(...)（:316）          │
│           → submitStateUpdateTask（:329）                            │
│           → 【clusterApplierService#updateTask 线程】runTask（:378）  │
│                ├─ nodeConnectionsService.connectToNodes              │
│                ├─ highPriorityStateAppliers                          │
│                │    └─ IndicesClusterStateService.applyClusterState  │
│                │         → 为新索引创建 IndexShard、启动恢复（见 18 章）│
│                ├─ normalPriorityStateAppliers                        │
│                ├─ lowPriorityStateAppliers                           │
│                ├─ localNodeMasterListeners                           │
│                └─ clusterStateListeners                              │
│      → 回 ack 给 master（表示"我 apply 完了"）                        │
│                                                                    │
│ Master 侧：ackListener.onNodeAck(node, null) → 全部 ack 后           │
│   → AckedClusterStateUpdateTask.onAllNodesAcked(null)               │
│      → 返回 IndexResponse 给客户端                                   │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 8. ★ 排障实战：四类高频问题

### 8.1 `cluster state update task ... took [Xs]`

**日志样例**：

```
[WARN][o.e.c.s.MasterService] [node1] took [31.2s], [5.1s] to execute cluster state update task, source: [create-index]
```

**根因**：`masterService#updateTask` 单线程被某个 executor 阻塞了。

**定位方法**：

1. **看 `GET /_cluster/pending_tasks`**：

```json
{
  "tasks": [
    { "insert_order": 1, "priority": "URGENT", "source": "create-index[myindex]", "executing": true, "time_in_queue_millis": 31000, "time_in_queue": "31s" },
    { "insert_order": 2, "priority": "NORMAL", "source": "update-mapping", "executing": false, "time_in_queue_millis": 30000 }
  ]
}
```

2. **看 `_tasks` API** 找正在执行的 action。
3. **看线程栈**：找 `masterService#updateTask` 线程的栈：

```bash
jstack <pid> | grep -A 30 "masterService#updateTask"
```

**常见根因**：

| 根因 | 表现 |
| --- | --- |
| 集群状态太大 | 序列化/反序列化慢（`Diff` 也救不了） |
| `minMasterNodes` 配错 | 每次 publish 都要等超时 |
| 网络问题 | 节点间 RTT 高 / 丢包 |
| 自定义 executor 里有慢操作 | 例如在 `execute()` 里做 HTTP 调用 |
| `MetaData` 里 custom 太多 | X-Pack 的 ILM/CCR/ML 状态膨胀 |
| 节点 GC 停顿 | Master 与从节点都在停 |

**★ 降低 `slow_task_logging_threshold` 可以更早看到问题**：

```yaml
cluster.service.slow_task_logging_threshold: 10s    # 默认 30s
```

### 8.2 `failed to commit cluster state version [N]`

**完整日志**：

```
[WARN][o.e.c.s.MasterService] [node1] failing [create-index]: failed to commit cluster state version [42]
org.elasticsearch.discovery.Discovery$FailedToCommitClusterStateException: failed to get enough masters to ack sent cluster state. [2] left
```

**含义**：`minMasterNodes` 个 master 节点没凑齐 ack。

**根因**：

| 现象 | 根因 | 处理 |
| --- | --- | --- |
| `[N] left` 且 `N` 很大 | `minMasterNodes` 设太大 | 调小到 `master候选/2 + 1` |
| 频繁但偶发 | 网络抖动 | 查网络 |
| 伴随 `rejoin` | Master 已经不认为自己有资格 | 检查 `minMasterNodes` 与 master 节点数 |
| 有节点频繁 GC | 从节点 ack 慢 | 查 GC |

**★ 注意**：抛 `FailedToCommitClusterStateException` 后，`ZenDiscovery.publish` 会：

```java
synchronized (stateMutex) {
    pendingStatesQueue.failAllStatesAndClear(new ElasticsearchException("failed to publish cluster state"));
    rejoin("zen-disco-failed-to-publish");      // ★ 触发重新加入（见 10 章）
}
```

**所以「发布失败」会导致 Master「重新加入集群」**，这会带来连锁反应（分片重路由等）。
**这是 ES 里最需要避免的状态之一。**

### 8.3 `timed out waiting for all nodes to process published state`

```
[WARN][o.e.d.z.PublishClusterStateAction] timed out waiting for all nodes to process published state [42] (timeout [30s], pending nodes: [[node5]])
```

**含义**：状态**已经 commit 了**（这是 commit 之后的等待），只是还有节点没 apply 完。

**★ 这不是致命错误**（状态是一致的），但说明有节点慢：

| 根因 | 处理 |
| --- | --- |
| 目标节点 GC 停顿 | 查 GC 日志 |
| 目标节点的 applier 慢 | 查该节点的 `clusterApplierService#updateTask` 栈 |
| 目标节点磁盘慢（apply 要写 `global-*.st`） | 查磁盘 IO |
| 网络带宽饱和（大 cluster state） | 减少 cluster state 大小 |

**调大 `discovery.zen.publish_timeout`** 只是掩盖问题，不解决。

### 8.4 `not enough masters to ack sent cluster state`

**在 `SendingController` 构造时就抛**（`:517`）：

```java
if (this.neededMastersToCommit > this.pendingMasterNodes) {
    throw new Discovery.FailedToCommitClusterStateException("not enough masters to ack sent cluster state." +
        "[{}] needed , have [{}]", neededMastersToCommit, pendingMasterNodes);
}
```

**含义**：`minMasterNodes - 1 > 当前 master 节点数 - 1`，
即**当前集群里的 master 节点数 < `minMasterNodes`**。

**根因**：`minMasterNodes` 配置错误（改了 `node.master` 但没改 `minMasterNodes`）。

> **正确算法**：`minMasterNodes = (master候选节点数 / 2) + 1`
> - 1 个 master 候选 → `1`
> - 2 个 → `2`
> - 3 个 → `2`
> - 5 个 → `3`
> - 6 个 → `4`
>
> **注意偶数的情况**：6 个候选 → 4，因为 `6/2+1 = 4`。
> 更通俗的规则是 **"超过半数"**：3 → 2, 4 → 3, 5 → 3, 6 → 4。

**6.7+ 的新机制**：`cluster.initial_master_nodes` + 自动管理，不再需要手工设 `minMasterNodes`。
6.6 还是老机制。

---

## 9. 动手实验

**实验 9-1：观察 source 与版本号**
```bash
curl -s 'localhost:9200/_cluster/state?pretty' | grep -E '"version"|state_uuid'
curl -s -XPUT 'localhost:9200/exp9' -H 'Content-Type: application/json' -d '{}'
curl -s 'localhost:9200/_cluster/state?pretty' | grep -E '"version"|state_uuid'
```
对照日志里的 `MasterService` 输出，确认 `source = create-index`。

**实验 9-2：观察批处理合并**
用一个脚本并发创建 20 个索引：
```bash
for i in $(seq 1 20); do curl -s -XPUT "localhost:9200/batch-$i" -H 'Content-Type: application/json' -d '{}' & done; wait
```
观察日志里 `MasterService` 的输出次数（应该远少于 20 次），
看 `source` 里是否有 `[batch-1, batch-2, ...]` 这样的批量 summary。

**实验 9-3：观察 pending_tasks**
把 `cluster.service.slow_task_logging_threshold` 设为 `1ms`，
同时发起大量元数据操作，`GET /_cluster/pending_tasks` 观察队列。

**实验 9-4：★ 制造发布失败**
3 节点集群，`minMasterNodes=3`，然后停掉 1 个 master 候选节点，
尝试创建索引 → 观察 `FailedToCommitClusterStateException` 与 `rejoin` 日志。
**（请在实验环境做，不要在测试环境之上做）**

**实验 9-5：观察两阶段提交**
在两个节点上，把 `logger.org.elasticsearch.discovery.zen: TRACE` 打开，
创建一个索引，观察日志顺序：
```
sending cluster state ...
received cluster state version 42        ← 阶段一
committing version [42]                  ← 阶段二
received commit for cluster state version 42
```

**实验 9-6：观察 applier 优先级**
在 `IndicesClusterStateService.applyClusterState` 和某个 `ClusterStateListener` 打日志，
观察 apply 顺序。

**实验 9-7：观察 apply 顺序与 `state()` 的关系**
在 `ClusterStateListener.clusterStateChanged(event)` 里打印
`clusterService.state().version()` 和 `event.state().version()`，验证两者一致。

**实验 9-8：观察 AckTimeout**
用一个很慢的 applier（在 applier 里 `Thread.sleep(5000)`），
用 `?timeout=1s` 发请求，观察 `ProcessClusterEventTimeoutException`。

---

## 10. 自测题

1. `MasterService` 与 `ClusterApplierService` 的职责区别？各自线程数是多少？
2. `masterService#updateTask` 是单线程的，这意味着什么？会有什么风险？
3. `batchingKey` 是什么？批处理解决什么问题？破坏批处理的常见写法是什么？
4. `ClusterStateTaskConfig.Priority` 有哪几个值？谁用 `URGENT`？
5. `runOnlyWhenMaster` 的作用？`onNoLongerMaster` 什么时候被调用？
6. `patchVersions`（`:278`）里判断"变了"用的是引用比较还是内容比较？这带来什么性能要求？
7. 两阶段提交的两个 action name 是什么？为什么需要两阶段？
8. `neededMastersToCommit` 的初始值怎么算？`pendingMasterNodes` 呢？
9. 什么情况下 `PublishClusterStateAction.publish` 会抛 `FailedToCommitClusterStateException`？抛其他异常意味着什么？
10. `discovery.zen.publish_timeout` 与 `discovery.zen.commit_timeout` 的区别？
11. `AckedClusterStateUpdateTask.mustAck` 默认是什么？`ackTimeout` 与 `timeout` 的区别？
12. `ClusterApplierService.runTask` 里 applier 的调用顺序？为什么要 `nodeConnectionsService.connectToNodes` 在最前面？
13. `ClusterStateApplier` 与 `ClusterStateListener` 的区别？写插件时如何选择？
14. `ClusterChangedEvent.source()` 从哪里来？为什么排障要看它？
15. `minMasterNodes` 的正确算法？设太大会怎样？设太小会怎样？
16. 「发布失败」后 Master 会做什么？为什么会引发连锁反应？

---

## 11. 关键类速查

| 类 | 路径 | 一句话 |
| --- | --- | --- |
| `ClusterService` | `cluster/service/ClusterService.java:47` | ★ 门面 |
| `MasterService` | `cluster/service/MasterService.java:69` | ★ Master 侧：算状态 + 发布（`:185` runTasks） |
| `MasterService.TaskOutputs` | 同上 `:342` | 一批任务的结果与回调 |
| `ClusterApplierService` | `cluster/service/ClusterApplierService.java:69` | ★ 所有节点：apply（`:378` runTask） |
| `TaskBatcher` | `cluster/service/TaskBatcher.java:55` | ★ 批处理框架 |
| `Batcher` / `BatchedTask` | `MasterService.java:119` / `TaskBatcher.java:165` | |
| `SourcePrioritizedRunnable` | `cluster/service/SourcePrioritizedRunnable.java` | 带 source 与优先级的 Runnable |
| `PendingClusterTask` | `cluster/service/PendingClusterTask.java` | `_cluster/pending_tasks` 的元素 |
| `ClusterStateTaskExecutor` | `cluster/ClusterStateTaskExecutor.java` | ★ 你实现的核心接口 |
| `ClusterTasksResult` / `TaskResult` | 同上 `:70` / `:129` | 执行结果 |
| `ClusterStateTaskConfig` | `cluster/ClusterStateTaskConfig.java` | 优先级 + 超时 |
| `ClusterStateTaskListener` | `cluster/ClusterStateTaskListener.java:30` | `onFailure` |
| `ClusterStateUpdateTask` | `cluster/ClusterStateUpdateTask.java` | 常用基类 |
| `AckedClusterStateUpdateTask` | `cluster/AckedClusterStateUpdateTask.java` | ★ 需要 ack 的任务 |
| `AckedClusterStateTaskListener` | `cluster/AckedClusterStateTaskListener.java` | ★ `mustAck` / `onAllNodesAcked` / `onAckTimeout` |
| `LocalClusterUpdateTask` | `cluster/LocalClusterUpdateTask.java` | 只本地更新 |
| `ClusterChangedEvent` | `cluster/ClusterChangedEvent.java:43` | ★ apply 时的变更事件 |
| `ClusterStateApplier` | `cluster/ClusterStateApplier.java` | 改状态 |
| `ClusterStateListener` | `cluster/ClusterStateListener.java` | 观察状态 |
| `LocalNodeMasterListener` | `cluster/LocalNodeMasterListener.java` | 只关心 master 变更 |
| `PublishClusterStateAction` | `discovery/zen/PublishClusterStateAction.java` | ★★ 两阶段提交（`:128`/`:181`/`:510`） |
| `PendingClusterStatesQueue` | `discovery/zen/PendingClusterStatesQueue.java` | 从节点暂存待提交状态 |
| `ElectMasterService` | `discovery/zen/ElectMasterService.java` | 选主比较规则（见 10 章） |
| `NodeConnectionsService` | `cluster/NodeConnectionsService.java` | 维护到所有节点的连接 |

---

**上一篇**：[08-ClusterState与集群元数据.md](./08-ClusterState与集群元数据.md)　
**下一篇**：[10-ZenDiscovery选主与节点发现.md](./10-ZenDiscovery选主与节点发现.md)
