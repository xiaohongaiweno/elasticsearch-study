# 10 · ZenDiscovery：节点发现与选主

> 目标：搞懂 ES 6.x 的集群成员管理机制——**节点怎么找到彼此、怎么选出 master、怎么发现故障、怎么重新加入**。
>
> **为什么必须学**：生产环境 90% 的「集群挂了」事故，根因都在这一层
> （`master_not_discovered`、脑裂、`minMasterNodes` 配错、节点被踢出）。
>
> ⚠️ 本章讲的是 **6.x 的 Zen1**。7.x 换成了完全重写的 **Zen2**（`cluster.initial_master_nodes`、
> 基于 Raft 的 `Coordinator`）。**6.6 是 Zen1 的最后一个大版本之一**。

---

## 1. 全景图

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                            ZenDiscovery（1319 行，本章主角）                      │
│                                                                                  │
│  ┌────────────────────┐  ┌────────────────────┐  ┌──────────────────────────┐   │
│  │ UnicastZenPing     │  │ ElectMasterService │  │ NodeJoinController       │   │
│  │ ★ 发现节点（ping）  │  │ ★ 选主与多数派检查  │  │ ★ 处理加入请求、等待被选为  │   │
│  │ 665 行             │  │ 223 行             │  │   master                 │   │
│  └────────────────────┘  └────────────────────┘  └──────────────────────────┘   │
│                                                                                  │
│  ┌────────────────────┐  ┌────────────────────┐  ┌──────────────────────────┐   │
│  │ MasterFaultDetection│ │ NodesFaultDetection│  │ MembershipAction         │   │
│  │ ★ 从节点监控 master │  │ ★ Master 监控从节点 │  │ ★ join/leave 的 RPC 定义  │   │
│  └────────────────────┘  └────────────────────┘  └──────────────────────────┘   │
│                                                                                  │
│  ┌────────────────────┐  ┌────────────────────┐  ┌──────────────────────────┐   │
│  │ PublishClusterState│  │ PendingCluster     │  │ JoinThreadControl        │   │
│  │ ★ 两阶段提交（09章）│  │ StatesQueue        │  │ ★ 管理 join 线程          │   │
│  └────────────────────┘  └────────────────────┘  └──────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────────────────┘
                                     │
                        action = internal:discovery/zen/*
```

**关键文件清单**

| 文件 | 路径 | 行数 |
| --- | --- | --- |
| `ZenDiscovery` | `server/src/main/java/org/elasticsearch/discovery/zen/ZenDiscovery.java` | 1319 |
| `UnicastZenPing` | 同目录 | 665 |
| `ZenPing` | 同目录 | 178 |
| `ElectMasterService` | 同目录 | 223 |
| `NodeJoinController` | 同目录 | 527 |
| `MembershipAction` | 同目录 | 294 |
| `MasterFaultDetection` | 同目录 | 443 |
| `NodesFaultDetection` | 同目录 | 369 |
| `FaultDetection` | 同目录 | 116 |
| `PublishClusterStateAction` | 同目录 | 661（见 09 章） |
| `PendingClusterStatesQueue` | 同目录 | 309 |
| `DiscoverySettings` | `server/src/main/java/org/elasticsearch/discovery/DiscoverySettings.java` | — |

---

## 2. `ZenDiscovery` 的结构（`ZenDiscovery.java:83`）

### 2.1 配置项（★ 全部要记）

```java
private static final Logger logger = LogManager.getLogger(ZenDiscovery.class);                    // :90

public static final Setting<TimeValue> PING_TIMEOUT_SETTING =                                     // :92
    Setting.positiveTimeSetting("discovery.zen.ping_timeout", TimeValue.timeValueSeconds(3), ...);
public static final Setting<TimeValue> JOIN_TIMEOUT_SETTING =                                     // :94
    Setting.positiveTimeSetting("discovery.zen.join_timeout",
        TimeValue.timeValueMillis(DEFAULT_JOIN_TIMEOUT), ...);     // 默认 = ping_timeout * 20
public static final Setting<Integer> JOIN_RETRY_ATTEMPTS_SETTING =                                // :98
    Setting.intSetting("discovery.zen.join_retry_attempts", 3, ...);
public static final Setting<TimeValue> JOIN_RETRY_DELAY_SETTING =                                 // :100
    Setting.positiveTimeSetting("discovery.zen.join_retry_delay", TimeValue.timeValueMillis(100), ...);
public static final Setting<Integer> MAX_PINGS_FROM_ANOTHER_MASTER_SETTING =                      // :102
    Setting.intSetting("discovery.zen.max_pings_from_another_master", 3, ...);
public static final Setting<Boolean> SEND_LEAVE_REQUEST_SETTING =                                 // :104
    Setting.boolSetting("discovery.zen.send_leave_request", true, ...);
public static final Setting<TimeValue> MASTER_ELECTION_WAIT_FOR_JOINS_TIMEOUT_SETTING =           // :106
    Setting.positiveTimeSetting("discovery.zen.master_election.wait_for_joins_timeout",
        TimeValue.timeValueMillis(5000), ...);
public static final Setting<Boolean> MASTER_ELECTION_IGNORE_NON_MASTER_PINGS_SETTING =            // :110
    Setting.boolSetting("discovery.zen.master_election.ignore_non_master_pings", false, ...);
public static final Setting<Integer> MAX_PENDING_CLUSTER_STATES_SETTING =                         // :112
    Setting.intSetting("discovery.zen.max_pending_cluster_states", 25, 1, ...);

public static final String DISCOVERY_REJOIN_ACTION_NAME = "internal:discovery/zen/rejoin";        // :115
```

**★ 关键配置速查表**：

| 配置 | 默认 | 作用 | 调优建议 |
| --- | --- | --- | --- |
| `discovery.zen.ping.unicast.hosts` | `["127.0.0.1", "[::1]"]` | ★ **种子节点地址列表** | 云上用**内网 IP / DNS 名**，列出至少 3 个（不要列全部） |
| `discovery.zen.ping.unicast.hosts.resolve_timeout` | 5s | DNS 解析超时 | 云上 DNS 慢时调大 |
| `discovery.zen.ping.unicast.concurrent_connects` | 10 | 并发 ping 数 | — |
| `discovery.zen.ping_timeout` | **3s** | ★ ping 超时 | 跨 AZ/跨区部署时**必须调大**（如 30s） |
| `discovery.zen.join_timeout` | `ping_timeout * 20` = 60s | join 超时 | |
| `discovery.zen.join_retry_attempts` | 3 | join 重试次数 | |
| `discovery.zen.join_retry_delay` | 100ms | 重试间隔 | |
| `discovery.zen.minimum_master_nodes` | 1 | ★★ **多数派门槛** | ★ 必须 = `master候选/2 + 1` |
| `discovery.zen.master_election.wait_for_joins_timeout` | 5s | 被选为 master 后等 join 的时间 | |
| `discovery.zen.master_election.ignore_non_master_pings` | false | 是否忽略非 master 候选的 ping | 建议 **true**（减少 ping 流量与噪音） |
| `discovery.zen.fd.ping_interval` | **1s** | ★ 故障检测 ping 间隔 | |
| `discovery.zen.fd.ping_timeout` | **30s** | ★ 故障检测超时 | 云上网络抖动时调大 |
| `discovery.zen.fd.ping_retries` | 3 | 故障检测重试 | |
| `discovery.zen.max_pings_from_another_master` | 3 | 发现"另一个 master"的容忍次数 | |
| `discovery.zen.send_leave_request` | true | 关闭时是否发 leave | |
| `discovery.zen.no_master_block` | `write` | ★ **没有 master 时的行为** | `write`（只挡写）/ `all`（全挡） |
| `discovery.zen.publish_timeout` | 30s | 见 09 章 | |
| `discovery.zen.commit_timeout` | 30s | 见 09 章 | |
| `discovery.zen.publish_diff.enable` | true | 是否用 diff | |
| `discovery.zen.max_pending_cluster_states` | 25 | 从节点暂存的待提交状态数 | |

### 2.2 字段（`:117-156`）

```java
private final TransportService transportService;                                  // :117
private final MasterService masterService;                                        // :118
private final DiscoverySettings discoverySettings;                                // :119
protected final ZenPing zenPing; // protected to allow tests access                // :120
private final MasterFaultDetection masterFD;                                      // :121  ★ 从节点视角
private final NodesFaultDetection nodesFD;                                        // :122  ★ Master 视角
private final PublishClusterStateAction publishClusterState;                      // :123
private final MembershipAction membership;                                        // :124
private final ClusterName clusterName;                                            // :125
private final ThreadPool threadPool;                                              // :126

private final TimeValue pingTimeout;                                              // :128
private final TimeValue joinTimeout;                                              // :129
private final int joinRetryAttempts;                                             // :132
private final TimeValue joinRetryDelay;                                          // :134
private final int maxPingsFromAnotherMaster;                                     // :137
private final boolean sendLeaveRequest;                                          // :140
private final ElectMasterService electMaster;                                    // :142  ★
private final boolean masterElectionIgnoreNonMasters;                            // :144
private final TimeValue masterElectionWaitForJoinsTimeout;                       // :145
private final JoinThreadControl joinThreadControl;                               // :147  ★
private final PendingClusterStatesQueue pendingStatesQueue;                      // :149  ★
private final NodeJoinController nodeJoinController;                             // :151  ★
private final NodeRemovalClusterStateTaskExecutor nodeRemovalExecutor;           // :152
private final ClusterApplier clusterApplier;                                     // :153
private final AtomicReference<ClusterState> committedState; // ★ last committed   // :154
private final Object stateMutex = new Object();                                  // :155  ★ 大锁
private final Collection<BiConsumer<DiscoveryNode, ClusterState>> onJoinValidators;  // :156
```

**★ `stateMutex`（`:155`）是 ZenDiscovery 的全局锁**。
几乎所有的状态变更（rejoin、handleMasterGone、handleAnotherMaster）都要先拿它。
**这是 Zen1 的一个架构缺陷**：`ZenDiscovery` 内的锁竞争会成为瓶颈。
（Zen2 用 `Coordinator` + `ClusterStateUpdateTask` 彻底重构了这一点。）

**★ 两个 FaultDetection 的对称性**：

| 类 | 谁运行 | 监控谁 | 用哪个 action |
| --- | --- | --- | --- |
| `MasterFaultDetection` | **非 master 节点** | master | `internal:discovery/zen/fd/master_ping` |
| `NodesFaultDetection` | **master** | 所有从节点 | `internal:discovery/zen/fd/ping` |

**为什么是双向的？**
- Master 需要知道哪些从节点活着 → 才能决定分片分配
- 从节点需要知道 master 活着 → 才能知道谁来协调

### 2.3 `doStart`（`:249`）——初始状态

```java
protected void doStart() {
    DiscoveryNode localNode = transportService.getLocalNode();
    assert localNode != null;
    synchronized (stateMutex) {
        // set initial state
        assert committedState.get() == null;
        ClusterState.Builder builder = clusterApplier.newClusterStateBuilder();
        ClusterState initialState = builder
            .blocks(ClusterBlocks.builder()
                .addGlobalBlock(STATE_NOT_RECOVERED_BLOCK)          // ★ 全局 block：状态未恢复
                .addGlobalBlock(discoverySettings.getNoMasterBlock())  // ★ 全局 block：无 master
                .build())
            .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()))
            .build();
        committedState.set(initialState);
        clusterApplier.setInitialState(initialState);
        nodesFD.setLocalNode(localNode);
        joinThreadControl.start();
    }
    zenPing.start();
}
```

**★ 两个初始 block 极其重要**：

| block | cluster block 名字 | 效果 |
| --- | --- | --- |
| `STATE_NOT_RECOVERED_BLOCK` | `state not recovered / initialized` | 所有操作被拒（集群还没准备好） |
| `getNoMasterBlock()` | `no master` | 按 `discovery.zen.no_master_block` 配置：阻止写（`write`）或全部（`all`） |

**★ 你看到的错误**：

```
cluster_block_exception: blocked by: [SERVICE_UNAVAILABLE/1/state not recovered / initialized]
cluster_block_exception: blocked by: [SERVICE_UNAVAILABLE/2/no master]
```

前者 = 集群还在启动恢复到一半（或者 `gateway.recover_after_nodes` 还没满足）；
后者 = 没有 master。**这两个是启动阶段最常见的错误。**

### 2.4 `startInitialJoin`（`:272`）

```java
@Override
public void startInitialJoin() {
    // start the join thread from a cluster state update. See {@link JoinThreadControl} for details.
    synchronized (stateMutex) {
        // do the join on a different thread, the caller of this method waits for 30s anyhow till it is discovered
        joinThreadControl.startNewThreadIfNotRunning();
    }
}
```

**调用时机**：`Node.start()` 里 `discovery.startInitialJoin()`（见 03 章）。
**注意**：它是**异步**的——启动线程不会等 join 完成。

**所以「节点启动后 `GET /_cluster/health` 显示 `master_not_discovered`」是正常现象，
需要等几秒。**

### 2.5 `doStop`（`:281`）——优雅退出

```java
protected void doStop() {
    joinThreadControl.stop();
    masterFD.stop("zen disco stop");
    nodesFD.stop();
    Releasables.close(zenPing); // stop any ongoing pinging
    DiscoveryNodes nodes = clusterState().nodes();
    if (sendLeaveRequest) {
        if (nodes.getMasterNode() == null) {
            // if we don't know who the master is, nothing to do here
        } else if (!nodes.isLocalNodeElectedMaster()) {
            // ★ 从节点：告诉 master「我要走了」
            membership.sendLeaveRequestBlocking(nodes.getMasterNode(), nodes.getLocalNode(), TimeValue.timeValueSeconds(1));
        } else {
            // ★ 我是 master：通知最多 5 个潜在 master 候选，让他们立刻发起选举
            DiscoveryNode[] possibleMasters = electMaster.nextPossibleMasters(nodes.getNodes().values(), 5);
            for (DiscoveryNode possibleMaster : possibleMasters) {
                if (nodes.getLocalNode().equals(possibleMaster)) { continue; }
                membership.sendLeaveRequest(nodes.getLocalNode(), possibleMaster);
            }
        }
    }
}
```

**★ 这就是「优雅停止节点能加快 master 选举」的原因**：
不等 30s 的故障检测超时，master 立刻知道节点走了并触发 reroute。

**★ 云服务启示**：
- **滚动重启时一定要给节点发送 `SIGTERM` 并等它退出**（不要 `kill -9`）
- `systemctl stop elasticsearch` 会发 `SIGTERM` → 触发这个逻辑
- 如果容器编排的 `terminationGracePeriodSeconds` 太短（如 5s），
  ES 来不及优雅退出 → 从节点的视角就是"突然消失"

---

## 3. ★ `innerJoinCluster`（`:438`）——加入流程的核心

```java
private void innerJoinCluster() {
    DiscoveryNode masterNode = null;
    final Thread currentThread = Thread.currentThread();
    nodeJoinController.startElectionContext();                     // ① 标记「我开始参与选举」
    while (masterNode == null && joinThreadControl.joinThreadActive(currentThread)) {
        masterNode = findMaster();                                 // ② 不断 ping，直到找到 master 候选
    }

    if (!joinThreadControl.joinThreadActive(currentThread)) {
        logger.trace("thread is no longer in currentJoinThread. Stopping.");
        return;
    }

    if (transportService.getLocalNode().equals(masterNode)) {
        // ★★ 情况 A：我自己被选为 master
        final int requiredJoins = Math.max(0, electMaster.minimumMasterNodes() - 1); // we count as one
        logger.debug("elected as master, waiting for incoming joins ([{}] needed)", requiredJoins);
        nodeJoinController.waitToBeElectedAsMaster(requiredJoins, masterElectionWaitForJoinsTimeout,
            new NodeJoinController.ElectionCallback() {
                @Override public void onElectedAsMaster(ClusterState state) {
                    synchronized (stateMutex) { joinThreadControl.markThreadAsDone(currentThread); }
                }
                @Override public void onFailure(Throwable t) {
                    logger.trace("failed while waiting for nodes to join, rejoining", t);
                    synchronized (stateMutex) { joinThreadControl.markThreadAsDoneAndStartNew(currentThread); }
                }
            }
        );
    } else {
        // ★★ 情况 B：别人是 master，我去 join 它
        nodeJoinController.stopElectionContext(masterNode + " elected");       // ③ 停止接受 join 请求
        final boolean success = joinElectedMaster(masterNode);                  // ④ 发 join 请求
        synchronized (stateMutex) {
            if (success) {
                DiscoveryNode currentMasterNode = this.clusterState().getNodes().getMasterNode();
                if (currentMasterNode == null) {
                    logger.debug("no master node is set, despite of join request completing. retrying pings.");
                    joinThreadControl.markThreadAsDoneAndStartNew(currentThread);
                } else if (currentMasterNode.equals(masterNode) == false) {
                    joinThreadControl.stopRunningThreadAndRejoin("master_switched_while_finalizing_join");
                }
                joinThreadControl.markThreadAsDone(currentThread);
            } else {
                joinThreadControl.markThreadAsDoneAndStartNew(currentThread);
            }
        }
    }
}
```

**★ 三条必须理解的点**：

1. **`:452`**：`requiredJoins = minMasterNodes - 1`（"we count as one"）。
   即**我被选为 master 后，还要等 `minMasterNodes - 1` 个其他 master 候选节点加入我**。
   这与 09 章的 `neededMastersToCommit = minMasterNodes - 1` 是**同一个常数**。

2. **`:451`**：判断"我是 master"用的是 `transportService.getLocalNode().equals(masterNode)`。
   所以 `findMaster()` 返回 `null` 时循环会继续 ping。

3. **`:482-491`**："join 成功但 `currentMasterNode == null` 或不是同一个" → **重新开始**
   （说明选主期间集群又变了）。**这是 ES 里"启动慢"的常见原因之一。**

### 3.1 `joinElectedMaster`（`:507`）——发 join 请求

```java
private boolean joinElectedMaster(DiscoveryNode masterNode) {
    try {
        // first, make sure we can connect to the master
        transportService.connectToNode(masterNode);                          // ① 先建 TCP 连接
    } catch (Exception e) {
        logger.warn(() -> new ParameterizedMessage("failed to connect to master [{}], retrying...", masterNode), e);
        return false;
    }
    int joinAttempt = 0; // we retry on illegal state if the master is not yet ready
    while (true) {
        try {
            logger.trace("joining master {}", masterNode);
            membership.sendJoinRequestBlocking(masterNode, transportService.getLocalNode(), joinTimeout);   // ② 阻塞发 join
            return true;
        } catch (Exception e) {
            final Throwable unwrap = ExceptionsHelper.unwrapCause(e);
            if (unwrap instanceof NotMasterException) {                       // ③ master 还不是 master
                if (++joinAttempt == this.joinRetryAttempts) {
                    logger.info("failed to send join request to master [{}], reason [{}], tried [{}] times",
                        masterNode, ExceptionsHelper.detailedMessage(e), joinAttempt);
                    return false;
                }
                // 否则继续重试
            } else {
                logger.info("failed to send join request to master [{}], reason [{}]",
                    masterNode, ExceptionsHelper.detailedMessage(e));
                return false;
            }
        }
        try { Thread.sleep(this.joinRetryDelay.millis()); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}
```

**★ `NotMasterException` 是关键词**：
它表示"你 ping 到的这个节点自己还不是 master（可能正在等 join）"。
**这是启动阶段日志里最常见的异常**：

```
failed to send join request to master [{node2}{...}], reason [RemoteTransportException[...];
nested: NotMasterException[Node [{node2}] not the master]]
```

**这不是错误**，只是"master 还没准备好"，会重试 `join_retry_attempts` 次。

### 3.2 `findMaster`：从 ping 结果推出 master

```java
// 简化结构（:900 附近）
private DiscoveryNode findMaster() {
    ZenPing.PingCollection fullPingResponses = pingAndWait(pingTimeout);        // ★ 发起一轮 ping
    ...
    // filter responses
    final List<ZenPing.PingResponse> pingResponses = filterPingResponses(fullPingResponses, masterElectionIgnoreNonMasters, logger);

    List<DiscoveryNode> activeMasters = new ArrayList<>();
    for (ZenPing.PingResponse pingResponse : pingResponses) {
        // ★ 收集"别人认为的 master"（排除自己）
        if (pingResponse.master() != null && !localNode.equals(pingResponse.master())) {
            activeMasters.add(pingResponse.master());
        }
    }

    // nodes discovered during pinging
    List<ElectMasterService.MasterCandidate> masterCandidates = new ArrayList<>();
    for (ZenPing.PingResponse pingResponse : pingResponses) {
        if (pingResponse.node().isMasterNode()) {
            masterCandidates.add(new ElectMasterService.MasterCandidate(
                pingResponse.node(), pingResponse.getClusterStateVersion()));
        }
    }

    if (activeMasters.isEmpty()) {
        if (electMaster.hasEnoughCandidates(masterCandidates)) {                 // ★ 多数派检查
            final ElectMasterService.MasterCandidate winner = electMaster.electMaster(masterCandidates);
            logger.trace("candidate {} won election", winner);
            return winner.getNode();
        } else {
            // ★ 你会在日志里看到这一条！
            logger.warn("not enough master nodes discovered during pinging (found [{}], but needed [{}]), pinging again",
                        masterCandidates, electMaster.minimumMasterNodes());
            return null;                                                          // → 循环继续 ping
        }
    } else {
        assert !activeMasters.contains(localNode) :
            "local node should never be elected as master when other nodes indicate an active master";
        // lets tie break between discovered nodes
        return electMaster.tieBreakActiveMasters(activeMasters);                   // ★ 已有 master，选最"好"的那个
    }
}
```

**★ 两条路径**：

| 情况 | 行为 |
| --- | --- |
| **没人认为有 master** | 用 `electMaster.electMaster(...)` 从候选里选一个（需要够多数派） |
| **有人报告了 master** | `tieBreakActiveMasters(...)` 选一个最优的（不需要多数派！） |

**这解释了「为什么集群已经有 master 时，新节点加入不需要多数派」。**

---

## 4. ★ `ElectMasterService`：谁更适合当 master

文件：`discovery/zen/ElectMasterService.java`

### 4.1 比较规则（★ 高频考点）

```java
public static class MasterCandidate {                                       // :53
    public static final long UNRECOVERED_CLUSTER_VERSION = -1;              // :55
    private final DiscoveryNode node;
    private final long clusterStateVersion;

    /**
     * compares two candidates to indicate which the a better master.
     * A higher cluster state version is better
     *
     * @return -1 if c1 is a batter candidate, 1 if c2.
     */
    public static int compare(MasterCandidate c1, MasterCandidate c2) {     // :91
        // we explicitly swap c1 and c2 here. the code expects "better" is lower in a sorted
        // list, so if c2 has a higher cluster state version, it needs to come first.
        int ret = Long.compare(c2.clusterStateVersion, c1.clusterStateVersion);   // ① ★ 集群状态版本高的优先
        if (ret == 0) {
            ret = compareNodes(c1.getNode(), c2.getNode());                     // ② ★ 版本相同 → 比节点
        }
        return ret;
    }
}

private static int compareNodes(DiscoveryNode o1, DiscoveryNode o2) {        // :214
    if (o1.isMasterNode() && !o2.isMasterNode()) { return -1; }              // ① 是 master 候选的优先
    if (!o1.isMasterNode() && o2.isMasterNode()) { return 1; }
    return o1.getId().compareTo(o2.getId());                                // ② ★ 否则按节点 ID 字典序（最小的赢）
}
```

**★★ 选主优先级（务必背下来）**：

```
第 1 优先：clusterStateVersion 最大（掌握最新集群状态的节点最合适）
第 2 优先：是 master 候选节点（node.master: true）
第 3 优先：node ID 字典序最小（tie-break，保证确定性）
```

**★ 第 3 条极其重要**：它保证了**所有节点对"谁应该当 master"得出相同结论**（确定性算法）。
如果没有这条，可能出现"每个节点都认为自己该当 master"的脑裂。

**★ 排障技巧**：想知道下一次会选出谁，可以同时 `GET /_nodes` 拿到所有节点 ID，
按字典序最小的 + 是 master 候选 + 状态版本最高的即可预测。

### 4.2 多数派检查

```java
public boolean hasEnoughCandidates(Collection<MasterCandidate> candidates) {   // :125
    if (candidates.isEmpty()) { return false; }
    if (minimumMasterNodes < 1) { return true; }                               // ★ 配成 0/-1 会"关闭保护"
    assert candidates.stream().map(MasterCandidate::getNode).collect(Collectors.toSet()).size() == candidates.size() :
        "duplicates ahead: " + candidates;
    return candidates.size() >= minimumMasterNodes;                            // ★★ 核心判断
}

public boolean hasEnoughMasterNodes(Iterable<DiscoveryNode> nodes) {           // :153
    final int count = countMasterNodes(nodes);
    return count > 0 && (minimumMasterNodes < 0 || count >= minimumMasterNodes);
}

public boolean hasTooManyMasterNodes(Iterable<DiscoveryNode> nodes) {          // :158
    final int count = countMasterNodes(nodes);
    return count > 1 && minimumMasterNodes <= count / 2;                       // ★ 配置过小 → "太多 master" → 有脑裂风险
}

public void logMinimumMasterNodesWarningIfNecessary(ClusterState oldState, ClusterState newState) {   // :163
    // check if min_master_nodes setting is too low and log warning
    if (hasTooManyMasterNodes(oldState.nodes()) == false && hasTooManyMasterNodes(newState.nodes())) {
        logger.warn("minimum_master_nodes was incorrectly set to [{}] - the cluster may lose data if this node...", minimumMasterNodes);
    }
}
```

**★ 你会看到这条告警**（`:163` 的产物）：

```
[WARN][o.e.d.z.ElectMasterService] minimum_master_nodes was incorrectly set to [1]
- the cluster may lose data if this node is partitioned from the rest of the cluster.
- the minimum_master_nodes setting is deprecated and should be set to the number of master nodes / 2 + 1
```

**★ `hasTooManyMasterNodes` 的逻辑**：`minimumMasterNodes <= count / 2` → "配得太小"。
- 3 个 master 候选，`minMasterNodes = 1` → `1 <= 1` → **告警** ✅
- 3 个 master 候选，`minMasterNodes = 2` → `2 <= 1` false → 正常 ✅
- 4 个 master 候选，`minMasterNodes = 2` → `2 <= 2` → **告警** ✅（应该是 3）

### 4.3 其他方法

| 方法 | 行号 | 作用 |
| --- | --- | --- |
| `minimumMasterNodes(int)` / `minimumMasterNodes()` | `:107` / `:111` | 读写（★ **可动态修改**） |
| `countMasterNodes(Iterable<DiscoveryNode>)` | `:115` | 数 master 候选 |
| `electMaster(Collection<MasterCandidate>)` | `:141` | 排序后取第一个 |
| `tieBreakActiveMasters(Collection<DiscoveryNode>)` | `:149` | 有多个 active master 时选最优 |
| `sortByMasterLikelihood(Iterable<DiscoveryNode>)` | `:177` | 按"当 master 的概率"排序 |
| `nextPossibleMasters(ObjectContainer<DiscoveryNode>, int)` | `:186` | 取 N 个最可能的 master（`doStop` 用） |

**`minimumMasterNodes` 是动态的**（`:107` 的 setter）：
对应 `MockZenPings`/`DiscoverySettings` 里注册的 `addSettingsUpdateConsumer`。
所以可以用 `PUT /_cluster/settings` 改：

```json
PUT /_cluster/settings
{ "persistent": { "discovery.zen.minimum_master_nodes": 2 } }
```

---

## 5. 节点发现：`UnicastZenPing`

文件：`discovery/zen/UnicastZenPing.java`（665 行）

### 5.1 为什么是 "Unicast"

| 方式 | 说明 | ES 支持 |
| --- | --- | --- |
| **Unicast** | 主动向配置的种子节点发请求 | ✅ `discovery.zen.ping.unicast.hosts` |
| Multicast | 向组播地址广播 | ❌ 5.x 起移除 |
| Custom | 插件实现 `UnicastHostsProvider` | ✅ `discovery.zen.hosts_provider` |

**★ 取消组播的原因**：云环境普遍不支持组播；组播不可靠且容易被误配置导致跨集群污染。

### 5.2 配置

```java
public static final Setting<Integer> DISCOVERY_ZEN_PING_UNICAST_CONCURRENT_CONNECTS_SETTING =   // :98
    Setting.intSetting("discovery.zen.ping.unicast.concurrent_connects", 10, 0, ...);
public static final Setting<TimeValue> DISCOVERY_ZEN_PING_UNICAST_HOSTS_RESOLVE_TIMEOUT =       // :100
    Setting.positiveTimeSetting("discovery.zen.ping.unicast.hosts.resolve_timeout", TimeValue.timeValueSeconds(5), Property.NodeScope);
```

**`discovery.zen.ping.unicast.hosts` 的格式**：

```yaml
# 方式 1：纯地址（默认端口 9300）
discovery.zen.ping.unicast.hosts: ["10.0.0.1", "10.0.0.2", "10.0.0.3"]

# 方式 2：带端口
discovery.zen.ping.unicast.hosts: ["10.0.0.1:9300", "10.0.0.2:9300"]

# 方式 3：DNS 名（推荐，方便扩缩容）
discovery.zen.ping.unicast.hosts: ["es-master.internal", "es-data.internal"]

# 方式 4：如果用 transport.profiles 自定义端口
discovery.zen.ping.unicast.hosts: ["10.0.0.1:9301"]
```

**★ 云服务最佳实践**：
1. **只列一部分种子节点**（如 3 个），不要列全部。因为 ES 通过种子节点**传染式**发现其他节点。
2. **用 DNS 名**而不是硬编码 IP——扩缩容时不用改配置。
3. **列 master 候选节点**，不要列 data-only 节点。
4. **解析超时要关注**：云上 DNS 若走 VPC DNS 可能慢，`resolve_timeout` 调到 10-30s。

### 5.3 `ZenPing.PingResponse`（`ZenPing.java:47`）

```java
class PingResponse implements Writeable {
    private final DiscoveryNode node;              // ① 回复者的节点信息
    private final DiscoveryNode master;            // ② ★ 回复者认为的 master（可能为 null）
    private final ClusterName clusterName;         // ③ ★ 集群名（用于隔离不同集群）
    private final long clusterStateVersion;        // ④ ★ 集群状态版本（选主的关键）

    public DiscoveryNode node()                    // :117
    public DiscoveryNode master()
    public ClusterName clusterName()
    public long getClusterStateVersion()           // :130
    // toString(): "cluster_name[...], cluster_state_version[...]"
}
```

**★ `PingResponse` 是理解"节点发现"的核心数据结构**：每个 ping 回复都携带这 4 个信息。
`findMaster()` 就是把这些回复聚合成"谁是 master"。

**★ `clusterName` 的作用**：**防止跨集群误连**。
如果节点收到不同 `clusterName` 的 ping 回复，会丢弃并告警：

```
received cluster state from [node] which is also master but with a different cluster name [xxx]
```
（`ZenDiscovery.java` 里有这条日志）

**★ 这是"同一个网段里有两个集群互相打架"的根因**——集群名配错了。

### 5.4 一个 ping 轮次做了什么

```
ZenPing.ping(...)（UnicastZenPing 实现）
  ├─ ① 解析 discovery.zen.ping.unicast.hosts → 得到一批地址
  │    （或调 UnicastHostsProvider.getHosts() 插件实现）
  ├─ ② 对每个地址：可能需要先建 Transport 连接（concurrent_connects 限流）
  ├─ ③ 发送 action = internal:discovery/zen/unicast_ping 请求
  │    请求体：本节点的 DiscoveryNode + 本集群名 + 本节点认为的 master + 本节点 clusterStateVersion
  ├─ ④ 收到 ping 请求的节点 → 回 PingResponse（把自己的情况告诉对方）
  ├─ ⑤ 收集所有 PingResponse
  │    ★ 关键：PingResponse 里还包含"对方知道的其他节点"
  │    → 下一轮 ping 会把这些新地址也加进来（★ 传染式发现）
  └─ ⑥ 返回 PingCollection
```

**★ "传染式发现"是最容易忽略的一点**：
`ZenPing` 的响应里会带回**它知道的其他节点地址**，
所以**只要连上 1 个种子节点，就能发现整个集群**。
这也是「种子节点不需要列全」的原因。

> **排障**：新节点能发现旧节点，但旧节点发现不了新节点？
> 这不可能——因为 ping 是**双向带回**的。
> 如果真的有这种不对称，通常是**网络 ACL 只单向放行**。

### 5.5 `ZenPing.PingCollection`

`ZenPing.PingCollection` 收集并去重 `PingResponse`，提供：
- `addPingResponseToCollection(...)`：★ 如果对同一个节点收到多个回复，保留 `clusterStateVersion` **最大**的
- `toList()` / `iterator()`

---

## 6. 故障检测：`FaultDetection`

文件：`FaultDetection.java`（116 行）是基类；`MasterFaultDetection`（443）和 `NodesFaultDetection`（369）是子类。

### 6.1 基类（`FaultDetection.java`）

| 字段/方法 | 作用 |
| --- | --- |
| `threadPool` / `transportService` | |
| `clusterName` | ★ 校验对端集群名 |
| `node` | 被监控的节点 |
| `connectOnNetworkDisconnect` | 断连后是否重连 |
| `pingRetryCount` / `pingInterval` / `pingRetryTimeout` | ping 参数 |
| `notifyMasterFailure(...)` / `notifyNodeFailure(...)` | 抽象回调 |
| `onPingFailure(...)` | ping 失败处理（重试计数） |
| `start()` / `stop(reason)` / `close()` | 生命周期 |
| `FailureDetection` 内部类 | 一个"检测任务" |

**★ 核心逻辑**：

```
定时（ping_interval）发一个 ping
  ├─ 收到响应 → 重置失败计数
  └─ 未收到 → 重试（最多 ping_retries 次）
       └─ 达到 ping_retries → 累计等待时间到 ping_timeout → 判定节点失败 → 回调 notifyXxxFailure
```

### 6.2 `MasterFaultDetection`（从节点侧）

**作用**：非 master 节点监控 master 是否活着。

**关键回调** → `ZenDiscovery.handleMasterGone`：

```java
FaultDetection.masterFailureHandler = (node, reason, cause) -> handleMasterGone(node, cause, reason);
```

**`handleMasterGone`（`ZenDiscovery.java` 约 `:711`）做了什么**：

```java
private void handleMasterGone(final DiscoveryNode masterNode, final Throwable cause, final String reason) {
    synchronized (stateMutex) {
        if (localNodeMaster()) {
            return; // we are the master, no need to do anything
        }
    }
    logger.info("master_left [{}], reason: {}", masterNode, reason);       // ★ 你在日志里看到的
    synchronized (stateMutex) {
        if (localNodeMaster()) { return; }
        if (clusterState().nodes().getMasterNodeId() == null) {
            logger.debug("master left, but we have no master node id. ignoring");
            return;
        }
        rejoin(reason);                                                     // ★ 触发重新 join
    }
}
```

### 6.3 `NodesFaultDetection`（Master 侧）

**作用**：master 监控所有从节点。

**关键回调** → `ZenDiscovery.handleNodeFailure`：

```java
FaultDetection.nodeFailureHandler = (node, reason) -> handleNodeFailure(node, reason);
```

**`handleNodeFailure`（`ZenDiscovery.java:679`）**：

```java
private void handleNodeFailure(final DiscoveryNode node, final String reason) {
    if (lifecycle.started() == false) { ... return; }
    synchronized (stateMutex) {
        if (localNodeMaster() == false) { ... return; }
        removeNode(node, "zen-disco-node-failed", reason);        // ★ 提交一个 ClusterStateUpdateTask 移除节点
    }
}
```

`removeNode` 会提交 `NodeRemovalClusterStateTaskExecutor`（`ZenDiscovery.java:565`）：

```java
public static class NodeRemovalClusterStateTaskExecutor
        implements ClusterStateTaskExecutor<NodeRemovalClusterStateTaskExecutor.Task>,
                   ClusterStateTaskListener {
    @Override
    public ClusterTasksResult<Task> execute(final ClusterState currentState, final List<Task> tasks) throws Exception {
        final DiscoveryNodes.Builder remainingNodesBuilder = DiscoveryNodes.builder(currentState.nodes());
        boolean removed = false;
        for (final Task task : tasks) {
            // ...
            if (currentState.nodes().nodeExists(task.node())) {
                remainingNodesBuilder.remove(task.node());
                removed = true;
            } else {
                logger.debug("node [{}] does not exist in cluster state, ignoring", task.node());
            }
        }
        if (!removed) {
            // no nodes to remove, no need to return a cluster state with a new version
            return ClusterTasksResult.<Task>builder().successes(tasks).build(currentState);     // ★ 复用旧状态！
        }

        // ★ 关键：先移除节点，再跑分配器
        final ClusterState clusterState = ClusterState.builder(currentState).nodes(remainingNodesBuilder).build();
        final ClusterState maybeUpdatedClusterState = allocationService.disassociateDeadNodes(clusterState, true, describeTasks(tasks));
        if (clusterState == maybeUpdatedClusterState) {
            // no nodes were removed from the routing table...
            return ClusterTasksResult.<Task>builder().successes(tasks).build(currentState);
        }
        return ClusterTasksResult.<Task>builder().successes(tasks).build(maybeUpdatedClusterState);
    }
}
```

**★ 三层"没有变化就复用旧状态"的优化**（`:611` 之后的几处）：
这正是 09 章 4.5 提到的性能要点——**避免无意义的版本号递增与广播**。

**★ `allocationService.disassociateDeadNodes(...)`** 就是「节点掉线后分片重新分配」的入口（见 11 章）。

### 6.4 三种"节点消失"的对比

| 事件 | 触发者 | 走哪条路 | 日志 |
| --- | --- | --- | --- |
| **节点优雅关闭** | 节点自己发 leave request | `handleLeaveRequest` → `removeNode` | `node_left [node2], reason: ...` |
| **节点崩溃（master 发现）** | `NodesFaultDetection` | `handleNodeFailure` → `removeNode` | `master_left` / `node_failed` |
| **master 崩溃（从节点发现）** | `MasterFaultDetection` | `handleMasterGone` → `rejoin` | `master_left [{master}], reason: ...` |

**★ 排障**：

```bash
grep -E "master_left|node_left|node_failed|rejoin|zen-disco" /var/log/elasticsearch/*.log
```

**这是排查"分片为什么重新分配"的第一条命令。**

---

## 7. 重新加入：`rejoin`（`:988`）

```java
protected void rejoin(String reason) {
    assert Thread.holdsLock(stateMutex);
    ClusterState clusterState = committedState.get();

    logger.warn("{}, current nodes: {}", reason, clusterState.nodes());        // ★ 你看到的 "zen-disco-xxx, current nodes: {...}"
    nodesFD.stop();
    masterFD.stop(reason);

    joinThreadControl.startNewThreadIfNotRunning();                            // ★ 启动新的 join 线程

    if (clusterState.nodes().getMasterNodeId() != null) {
        // ★ 加上 no_master_block
        ClusterBlocks clusterBlocks = ClusterBlocks.builder().blocks(clusterState.blocks())
            .addGlobalBlock(discoverySettings.getNoMasterBlock())
            .build();
        // ★ 清掉 master
        DiscoveryNodes discoveryNodes = new DiscoveryNodes.Builder(clusterState.nodes()).masterNodeId(null).build();
        clusterState = ClusterState.builder(clusterState).blocks(clusterBlocks).nodes(discoveryNodes).build();

        committedState.set(clusterState);
        clusterApplier.onNewClusterState(reason, this::clusterState, (source, e) -> {}); // don't wait for state to be applied
    }
}
```

**★ `rejoin` 做了 4 件事**：

1. **停止两个 fault detection**（避免重复告警）
2. **启动新的 join 线程**（重新走 `innerJoinCluster`）
3. **清空 `masterNodeId`** → 状态变成"无 master"
4. **加 `no_master_block`** → ★ **写的请求会立刻被拒**（`ClusterBlockException: no master`）

**★ 触发 `rejoin` 的所有场景**（★ 排障必须记住）：

| 触发点 | reason 字符串 | 场景 |
| --- | --- | --- |
| `MasterFaultDetection` | `master_left` | master 掉线 |
| 发布了新版本但版本比当前低 | `zen-disco-receive(from master [...])` | 收到旧状态 |
| 发现另一个 master（版本更新） | `zen-disco-discovered another master with a new cluster_state [...]` | 脑裂恢复 |
| 集群状态发布失败 | `zen-disco-failed-to-publish` | minMasterNodes 不够 |
| `min_master_nodes` 调整 | `zen-disco-node-failed` / `zen-disco-join` | |
| 主动移除节点 | `node_removed` | |
| `currentMasterNode` 变了 | `master_switched_while_finalizing_join` | 选主期间变化 |
| 节点被 master 要求 rejoin | `zen-disco-master_rejoin` | |

**★ `zen-disco-` 前缀是 ZenDiscovery 所有 reason 的约定**。
看到 `current nodes: {...}` 的 WARN 日志，就是 `rejoin` 的 `:992` 打的。

**★★ 云服务最重要的调优含义**：

`rejoin` 是一个**昂贵**的操作（要重新 ping、重新 join、重新 apply 状态）。
频繁 `rejoin` = 集群不稳定。

**降低误触发的方法**：
1. 调大 `discovery.zen.fd.ping_timeout`（默认 30s → 云上可到 60s）
2. 调大 `discovery.zen.publish_timeout`
3. 保证 GC 停顿小于 `ping_timeout`（★ 这是最关键的！大堆 + CMS/G1 的长时间 GC 会导致误判）

---

## 8. `NodeJoinController`：加入请求的处理（`:57`）

### 8.1 核心状态

```java
public class NodeJoinController {
    private final MasterService masterService;                     // :61
    private final JoinTaskExecutor joinTaskExecutor;               // :62
    private ElectionContext electionContext = null;                // :66  ★
    ...
}
```

### 8.2 关键方法

| 方法 | 行号 | 作用 |
| --- | --- | --- |
| `startElectionContext()` | `:149` | ★ 开始"我在竞选 master"（开始接受 join 请求） |
| `stopElectionContext(String reason)` | `:158` | 停止竞选（如"别人已经是 master"） |
| `handleJoinRequest(node, callback)` | `:172` | ★ 处理一个 join 请求 |
| `checkPendingJoinsAndElectIfNeeded()` | `:187` | ★ 检查是否攒够 join，够就"当选" |
| `waitToBeElectedAsMaster(...)` | `:87` | 等待当选（`innerJoinCluster` 调用） |
| `failContextIfNeeded(...)` | `:139` | 竞选失败 |

### 8.3 `ElectionContext`（`:219` 内部类）

```java
private class ElectionContext {
    private ElectionCallback callback = null;                          // :220
    private int requiredMasterJoins = -1;                              // :221  ★ 还需要多少个 join
    private final Map<DiscoveryNode, List<MembershipAction.JoinCallback>> joinRequestAccumulator = new HashMap<>();  // :222

    public synchronized void onAttemptToBeElected(int requiredMasterJoins, ElectionCallback callback) {  // :226
        ensureOpen();
        if (this.callback != null) {
            throw new IllegalStateException("already attempting to be elected as master");
        }
        this.requiredMasterJoins = requiredMasterJoins;
        this.callback = callback;
    }

    public synchronized void addIncomingJoin(DiscoveryNode node, MembershipAction.JoinCallback callback) {  // :234
        ensureOpen();
        joinRequestAccumulator.computeIfAbsent(node, n -> new ArrayList<>()).add(callback);
    }

    public synchronized boolean isEnoughPendingJoins(int pendingMasterJoins) {        // :240
        return requiredMasterJoins == -1 || pendingMasterJoins >= requiredMasterJoins;
    }
    ...
    public synchronized void closeAndBecomeMaster() { ... }                          // :268
    public synchronized void closeAndProcessPending(String reason) { ... }            // :283
}
```

**★ 特殊常量**（`:382` / `:395`）：

```java
public static final DiscoveryNode BECOME_MASTER_TASK = new DiscoveryNode("_BECOME_MASTER_TASK_", ...);   // :382
public static final DiscoveryNode FINISH_ELECTION_TASK = new DiscoveryNode("_FINISH_ELECTION_", ...);    // :395
```

**这两个"伪节点"是很有技巧的设计**：
它们被塞进 join 请求的批处理里，作为"特殊任务"触发状态变更：

- `BECOME_MASTER_TASK` → 触发 `JoinTaskExecutor` 把本节点设为 master
- `FINISH_ELECTION_TASK` → 触发一次状态提交来"结束选举"

**★ 你会看到这种日志**：

```
zen-disco-elected-as-master ([1] nodes joined)[{node1}{...}], reason: after join[1] nodes, elected as master
```

或者发现 `_BECOME_MASTER_TASK_` 出现在 `_tasks` 里（这是正常的）。

### 8.4 `JoinTaskExecutor`（`NodeJoinController` 内部，`:345` 附近）

**它的 `execute` 做三件事**：

```java
@Override
public ClusterTasksResult<Task> execute(final ClusterState currentState, final List<Task> joinTasks) {
    // ① 检查自己还是不是 master
    if (currentState.nodes().isLocalNodeElectedMaster() == false) {
        throw new NotMasterException("Node " + localNode + " not the master");      // ★ 这就是 NotMasterException 的来源！
    }

    final ClusterTasksResult.Builder<Task> resultBuilder = ClusterTasksResult.<Task>builder();

    // ② 对每个 join 请求做校验
    for (Task joinTask : joinTasks) {
        if (joinTask.node().isMasterNode()) {
            // 如果是 master 候选，检查是否够多数派
            final int mastersCount = nodesBuilder.masterNodesCount();
            if (mastersCount == electMaster.minimumMasterNodes()) {
                logger.debug("not adding node [{}] as there are already [{}] master nodes",
                    joinTask.node(), mastersCount);
                resultBuilder.failure(joinTask, new IllegalStateException("Not added to cluster since the node won't be part of the minimum master nodes quorum"));
                continue;
            }
        }
        // ★ 检查是否是同一集群、版本兼容等
        validateNodeJoin(joinTask.node());
        ...
    }
    ...
    for (BiConsumer<DiscoveryNode, ClusterState> joinValidator : onJoinValidators) {
        joinValidator.accept(node, currentState);                                     // ③ 插件校验（如 license 检查）
    }

    // ④ 把节点加到 nodes 里，并把状态设为"有 master"
    return resultBuilder.successes(...).build(ClusterState.builder(currentState).nodes(nodesBuilder).build());
}
```

**★ `validateNodeJoin` 里的检查**（`ZenDiscovery.addBuiltInJoinValidators`，`:231`）：

```java
static Collection<BiConsumer<DiscoveryNode, ClusterState>> addBuiltInJoinValidators(...) {
    final Collection<BiConsumer<DiscoveryNode, ClusterState>> validators = new ArrayList<>();
    // ① 检查集群名
    validators.add((node, state) -> { ... checkClusterJoin(node, state); });
    // ② 检查节点版本兼容
    validators.add((node, state) -> { ... checkNodeVersion(node, state); });
    return validators;
}
```

**★ 所以"节点加入被拒"的三种原因**：

| 原因 | 异常/日志 |
| --- | --- |
| 集群名不一致 | `This node previously joined a cluster ... but was told to join a different cluster` |
| 版本不兼容 | `can't join existing cluster as the node version [...] is too old/new` |
| master 候选太多 | `Not added to cluster since the node won't be part of the minimum master nodes quorum` |

**第三种是很有趣的设计**：如果已经有 `minMasterNodes` 个 master 节点了，
**新的 master 候选节点会被拒绝加入集群状态**（但连接是建立的，只是不当 master）。

> **★ 这解释了「为什么加了 master 节点后必须调大 `minMasterNodes`」**：
> 不调大，新节点会被拒绝加入。

---

## 9. `MembershipAction`：join/leave 的 RPC

文件：`discovery/zen/MembershipAction.java`（294 行）

| action name | 方向 | 用途 |
| --- | --- | --- |
| `internal:discovery/zen/join` | 从节点 → master | ★ 请求加入 |
| `internal:discovery/zen/join/validate` | master → 待加入节点 | 反向验证（校验集群名/版本） |
| `internal:discovery/zen/leave` | 从节点 → master | ★ 优雅退出 |

**关键方法**：

| 方法 | 用途 |
| --- | --- |
| `sendJoinRequest(DiscoveryNode masterNode, DiscoveryNode node)` | 异步发 join |
| `sendJoinRequestBlocking(DiscoveryNode masterNode, DiscoveryNode node, TimeValue timeout)` | ★ **同步**发 join（`joinElectedMaster` 用） |
| `sendLeaveRequest(DiscoveryNode sender, DiscoveryNode masterNode)` | 异步发 leave |
| `sendLeaveRequestBlocking(...)` | 同步发 leave（`doStop` 用） |

**★ `internal:discovery/zen/join/validate` 的作用**：
Master 收到 join 请求后，会**反向连接**请求节点，
向它发一个 `validate` 请求，让对方也校验一次"我们是一个集群、版本兼容"。

**为什么需要反向验证？** 防止单向网络问题导致的"半个集群"。
**这也是 master 启动变慢的一个原因**（每个 join 都要一次额外的往返）。

---

## 10. ★ 完整时序：集群从零启动

```
【3 节点，node1/node2/node3，minMasterNodes=2，hosts=[node1,node2,node3]】

t0  三个节点同时启动
    └─ Node.start() → discovery.startInitialJoin() → 启动 join 线程
        └─ innerJoinCluster() → findMaster() → pingAndWait(pingTimeout)

t1  第一轮 ping（可能大家都是 UNRECOVERED，clusterStateVersion = -1）
    ├─ node1: 收到 node2/node3 的回复 → masterCandidates = [n1, n2, n3]（version 都是 -1）
    │         hasEnoughCandidates(3 >= 2) ✅ → electMaster → 按 nodeId 比较 → 最小 ID 的赢
    │         假设 node1 赢了 → masterNode = node1 → 是自己！
    │         → waitToBeElectedAsMaster(requiredJoins = 2 - 1 = 1, wait_for_joins_timeout=5s)
    ├─ node2: 同样的 ping → 也认为 node1 应该当 master → masterNode = node1
    │         → joinElectedMaster(node1)
    │           └─ membership.sendJoinRequestBlocking(...)     ← 报错 NotMasterException（node1 还没准备好）
    │               按 join_retry_attempts=3 重试，间隔 join_retry_delay=100ms
    └─ node3: 同 node2

t2  node1 收到 node2、node3 的 join 请求
    └─ NodeJoinController.handleJoinRequest → ElectionContext.addIncomingJoin
        └─ checkPendingJoinsAndElectIfNeeded
             pendingMasterJoins = 2 >= requiredMasterJoins = 1 ✅
             → closeAndProcessPending → 提交 JoinTaskExecutor
                  ├─ 校验 node2/node3（集群名/版本）
                  └─ 新状态：nodes = {n1, n2, n3}, master = n1
             → 发布（两阶段提交，见 09 章）
             → node1 成为 master ✅

t3  node1 成为 master 后 → ZenDiscovery.doStart 里注册的回调
    ├─ 启动 nodesFD（监控 node2/node3）
    └─ 加载/恢复集群状态（gateway，见 23 章）

t4  node2/node3 的 join 请求成功返回
    └─ 收到 master 发布的集群状态 → apply
        ├─ 状态里有 master = node1 → 记录
        └─ 启动 masterFD（监控 node1）

t5  集群形成，无 master block 被解除
    └─ STATE_NOT_RECOVERED_BLOCK 也在 gateway 恢复完成后解除

t6  正常状态：master 定期 ping 所有节点；每个节点定期 ping master
```

**★ 关键日志序列（正常启动）**：

```
[node1] [INFO ][o.e.d.z.ZenDiscovery     ] [node1] master_left ...          ← 初始无 master
[node1] [INFO ][o.e.d.z.ZenDiscovery     ] [node1] zen-disco-elected-as-master ([0] nodes joined), reason: ...
[node2] [INFO ][o.e.d.z.ZenDiscovery     ] [node2] master_left ... [node1]  ← 选择
[node2] [INFO ][o.e.d.z.ZenDiscovery     ] [node2] zen-disco-join (elected_node_id [node1])
[node1] [INFO ][o.e.d.z.ZenDiscovery     ] [node1] zen-disco-node-join[{node2}{...}], reason: added {{node2}}
[node1] [INFO ][o.e.c.s.MasterService    ] [node1] zen-disco-node-join, reason: added {{node2}}   ← 09 章的 source
```

**★ 关键日志序列（异常启动）**：

```
not enough master nodes discovered during pinging (found [[]], but needed [2]), pinging again     ← ping 不到足够节点
failed to send join request to master [{node1}], reason [NotMasterException[...]]                 ← master 还没好
zen-disco-failed-to-publish ... current nodes: {...}                                              ← 发布失败
```

---

## 11. ★ 排障实战：五类问题

### 11.1 `master_not_discovered_exception` / `MasterNotDiscoveredException`

**含义**：没有找到 master，且 `no_master_block` 生效。

**完整错误**：
```json
{
  "error": {
    "type": "master_not_discovered_exception",
    "reason": null
  },
  "status": 503
}
```

**排查清单**：

| 检查项 | 命令/方法 |
| --- | --- |
| 1. 有 `minMasterNodes` 个 master 候选活着吗？ | `GET /_cat/nodes?v&h=name,id,master` |
| 2. `discovery.zen.ping.unicast.hosts` 对吗？ | 看配置文件 / `GET /_nodes/settings` |
| 3. 节点间 9300 端口通吗？ | `nc -zv other-node 9300` |
| 4. 集群名一致吗？ | `GET /_nodes/settings?filter_path=**.cluster.name` |
| 5. 有 master 候选节点在反复 rejoin 吗？ | `grep "zen-disco" *.log` |
| 6. GC 停顿超过 `ping_timeout` 吗？ | GC 日志 |
| 7. `minMasterNodes` 是奇数配置还是偶数？ | 见 11.2 |

**★ 最常用的诊断命令**：

```bash
# 看所有节点的 master 状态和版本
GET /_cat/nodes?v&h=id,name,master,node.role,version

# 看集群状态里的 master
GET /_cluster/state/master_node?pretty

# 看 discovery 的统计
GET /_nodes/stats/discovery?pretty
```

`GET /_nodes/stats/discovery` 的字段：

```json
"discovery": {
  "cluster_state_queue": {
    "total": 0,          // 收到的待处理状态总数
    "pending": 0,        // ★ 当前积压
    "committed": 0,      // 已提交
    "uncommitted": 0     // ★ 未提交（应该总是 0；>0 说明有 publish 卡住）
  },
  "published_cluster_states": {
    "full_states": 10,               // 全量发布次数
    "incompatible_diffs": 2,         // ★ diff 不兼容次数（越小越好）
    "compatible_diffs": 100          // diff 发布次数
  }
}
```

**★ `uncommitted > 0` 是重要信号**：说明有 publish 卡在阶段一，Master 在等 ack。

### 11.2 脑裂（Split Brain）

**表现**：两个节点都认为自己是 master。

**日志特征**：

```
[node2] discovered [{node1}{...}] which is also master but with a newer cluster_state, rejoining
[node2] zen-disco-discovered another master with a new cluster_state [{node1}][...], current nodes: {...}
```

或者在**两个节点上**都能看到：

```
zen-disco-elected-as-master
```

**根因**：

| 根因 | 说明 |
| --- | --- |
| ★ **`minMasterNodes` 太小** | 最常见的根因。3 个候选配 `1` → 任一节点分区后都能自选 master |
| 网络分区 | 一半节点看不到另一半 |
| 长时间 GC | 节点被误判为"死了" |
| 配置错误 | 部分节点 `node.master: false` 而 `minMasterNodes` 没跟着调 |

**★ 检查 method**：

```json
// 如果两个节点都返回自己，就是脑裂
GET /_cluster/state/master_node?pretty
```

**★ 恢复方法**：

```
1. 【止血】立刻停掉其中一个"少数派" master（或全部停掉）
2. 【诊断】确认哪个 master 有最新的数据（看 /_cluster/state 的 version 和索引数据）
3. 【修复】把旧的 master 节点停掉 → 让它重新 join
4. 【预防】把 minMasterNodes 改成正确的值（master候选/2 + 1），并设为 persistent
```

**★ 6.6 的一个细节**：
`ZenDiscovery.handleAnotherMaster`（`:1023`）会自动处理"发现另一个 master"：

```java
private void handleAnotherMaster(ClusterState localClusterState, final DiscoveryNode otherMaster,
                                 long otherClusterStateVersion, String reason) {
    assert localClusterState.nodes().isLocalNodeElectedMaster() : "handleAnotherMaster called but current node is not a master";
    assert Thread.holdsLock(stateMutex);

    if (otherClusterStateVersion > localClusterState.version()) {
        // ★ 对方的状态更新 → 我主动 rejoin（放弃 master 身份）
        rejoin("zen-disco-discovered another master with a new cluster_state [" + otherMaster + "][" + reason + "]");
    } else {
        // ★ 我的状态更新 → 让对方 rejoin（我继续当 master）
        logger.warn("discovered [{}] which is also master but with an older cluster_state, telling [{}] to rejoin the cluster ([{}])",
            otherMaster, otherMaster, reason);
        try {
            transportService.connectToNode(otherMaster);
            transportService.sendRequest(otherMaster, DISCOVERY_REJOIN_ACTION_NAME,
                new RejoinClusterRequest(localClusterState.nodes().getLocalNodeId()),
                new EmptyTransportResponseHandler(ThreadPool.Names.SAME) { ... });
        } catch (Exception e) { ... }
    }
}
```

**★ 这段代码是"脑裂自愈"机制**：比较 `clusterStateVersion`，**版本低的那个主动放弃 master 身份**。

**⚠️ 但这有一个致命前提**：**两个节点必须能通信**（才能比较版本）。
如果网络完全分区，两边都升到各自的最高版本 → 谁也说服不了谁 → **真正的脑裂**。

**这就是 `minMasterNodes` 存在的根本原因**：
**通过"必须在 ping 阶段就凑够多数派才能竞选 master"来避免分区后双方都能当 master。**

### 11.3 节点被频繁踢出

**日志**：

```
[node1] [INFO][o.e.d.z.ZenDiscovery] [node1] master_left [{node2}{...}], reason: ... timed out
[node2] [INFO][o.e.d.z.ZenDiscovery] [node2] master_left [{node1}{...}], reason: ...
```

**根因与处理**：

| 根因 | 判断 | 处理 |
| --- | --- | --- |
| ★ **GC 停顿超过 `fd.ping_timeout`（30s）** | GC 日志里有 > 30s 的 Full GC | 调优 GC / 减少堆 / 调大 `ping_timeout` |
| 网络抖动 | `netstat -s` 有重传 | 调大 `ping_timeout` 和 `ping_retries` |
| Master 侧线程池堵塞 | `masterService#updateTask` 栈 | 见 09 章 8.1 |
| 磁盘 IO 饱和（写 translog 阻塞 IO 线程） | iostat | 换 SSD / 分离存储 |
| `vm.max_map_count` 不足 | 启动日志 | 见 01 章 |

**★ 调优**：

```yaml
discovery.zen.fd.ping_interval: 1s
discovery.zen.fd.ping_timeout: 60s     # ★ 从 30s 调到 60s（云上常见）
discovery.zen.fd.ping_retries: 3
```

**代价**：故障检测变慢 → master 挂掉后 30-60s 才能选出新 master；
分片重新分配也变慢。**这是"稳定性 vs 恢复速度"的权衡。**

### 11.4 `NotMasterException`

```
org.elasticsearch.discovery.MasterNotDiscoveredException
nested: NotMasterException[Node [{node2}] not the master]
```

**两种来源**：

| 来源 | 位置 | 含义 |
| --- | --- | --- |
| `ZenDiscovery.joinElectedMaster` | `:523` | 我 ping 到的 master 自己还不是 master（正在等 join） |
| `NodeJoinController.JoinTaskExecutor.execute` | `:357` 附近 | Master 收到请求时发现自己已不是 master |

**处理**：**通常不用处理**（会自动重试）。持续出现说明集群在反复选主。

### 11.5 `This node previously joined a cluster ... but was told to join a different cluster`

```
java.lang.IllegalStateException: This node previously joined a cluster with UUID [...] but was told to join a different cluster with UUID [...]
```

**含义**：节点的**数据目录里已经有集群 UUID**，但被要求加入不同 UUID 的集群。

**根因**：`path.data` 复用了（把旧集群的数据目录挂到了新集群）。

**处理**：清空 `path.data` 或用正确的集群（★ **注意：清空会丢数据！**）。

---

## 12. 动手实验

**实验 10-1：观察选主全过程**
3 节点集群，全部停掉，设 `logger.org.elasticsearch.discovery: DEBUG`，
依次启动 → 观察 `innerJoinCluster` 的完整日志流。

**实验 10-2：预测选主结果**
`GET /_nodes?filter_path=nodes.*.name,nodes.*.version` 拿到所有 nodeId，
按字典序最小的预测下一个 master，然后 kill 掉 master 验证预测。

**实验 10-3：★ 制造并观察脑裂**
3 节点，`minMasterNodes=1`，用 iptables 隔离 master：
```bash
# 在 node1 上
iptables -A OUTPUT -p tcp --dport 9300 -d <node2_ip> -j DROP
iptables -A OUTPUT -p tcp --dport 9300 -d <node3_ip> -j DROP
```
观察 node1 是否还认为自己是 master，node2/node3 是否选出新 master。
**（实验环境做，做完 `iptables -F`）**

**实验 10-4：观察 rejoin**
`kill -9` master → 在其他节点观察：
```
master_left [{node1}{...}], reason: ...
zen-disco-... , current nodes: {...}
```
统计从 kill 到新 master 选出的时间（应该 ≈ `fd.ping_timeout`）。

**实验 10-5：观察优雅退出加速**
对比两种停止方式：
```bash
# A. 优雅
kill <pid>              # SIGTERM
# B. 暴力
kill -9 <pid>
```
对比两种情况下其他节点发现"master 不在了"的时间。

**实验 10-6：观察 minMasterNodes 动态修改**
```bash
PUT /_cluster/settings
{ "persistent": { "discovery.zen.minimum_master_nodes": 3 } }
```
然后在 `ElectMasterService.minimumMasterNodes(int)` 打断点验证。
（★ 记得改回来！）

**实验 10-7：观察 ping 的传染式发现**
4 节点，`hosts` 只配 1 个节点，观察其余 3 个是否都能加入集群。

**实验 10-8：观察 `_BECOME_MASTER_TASK_`**
启动瞬间 `GET /_tasks?actions=internal:*&detailed=true`（可能抓不到，可以用日志）。

**实验 10-9：观察 join 被拒**
设 `minMasterNodes=1`，启动 3 个 master 候选节点，
观察是否只有 1 个成功成为 master 候选加入（另 2 个被 `Not added to cluster since the node won't be part of the minimum master nodes quorum` 拒绝）。

---

## 13. 自测题

1. 为什么 ES 5.x 起移除了组播（multicast）发现？
2. `discovery.zen.ping.unicast.hosts` 为什么不需要列全所有节点？
3. 选主的三个优先级规则是什么？为什么需要"node ID 字典序"这一条？
4. `hasEnoughCandidates` 与 `hasTooManyMasterNodes` 的判断逻辑分别是什么？
5. `minMasterNodes` 的正确算法？为什么偶数个候选时要更谨慎？
6. `doStart`（`:249`）里加了哪两个全局 block？各自会导致什么错误？
7. `innerJoinCluster` 里 `requiredJoins = minMasterNodes - 1` 为什么减 1？
8. `joinElectedMaster` 遇到 `NotMasterException` 为什么是"正常的"？
9. `findMaster` 在"有 activeMaster"和"没有 activeMaster"两种情况下的逻辑差异？
10. `MasterFaultDetection` 和 `NodesFaultDetection` 分别由谁运行、监控谁？
11. `rejoin` 做了哪 4 件事？为什么它很"贵"？
12. `handleAnotherMaster`（`:1023`）的脑裂自愈逻辑？它有什么前提条件？
13. `_BECOME_MASTER_TASK_` 和 `_FINISH_ELECTION_` 这两个"伪节点"是干什么的？
14. 为什么"master 候选节点太多"时新节点会被拒绝加入集群状态？
15. `internal:discovery/zen/join/validate` 这个反向验证解决什么问题？
16. 节点被频繁踢出的 5 个常见根因？
17. `GET /_nodes/stats/discovery` 的 `uncommitted` 字段 > 0 意味着什么？

---

## 14. 关键类速查

| 类 | 路径 | 一句话 |
| --- | --- | --- |
| `ZenDiscovery` | `discovery/zen/ZenDiscovery.java:83` | ★ 本章主角（`:438` innerJoinCluster、`:507` joinElectedMaster、`:988` rejoin） |
| `ZenDiscovery.NodeRemovalClusterStateTaskExecutor` | 同上 `:565` | 移除节点（master 掉线/离开） |
| `ZenPing` / `ZenPing.PingResponse` | `discovery/zen/ZenPing.java:45` / `:47` | ping 接口与响应 |
| `UnicastZenPing` | `discovery/zen/UnicastZenPing.java` | ★ 单播发现 |
| `UnicastHostsProvider` / `SettingsBasedHostsProvider` / `FileBasedUnicastHostsProvider` | 同目录 | 种子节点提供者 |
| `ElectMasterService` | `discovery/zen/ElectMasterService.java` | ★ 选主规则（`:91` compare） |
| `ElectMasterService.MasterCandidate` | 同上 `:53` | 候选（node + clusterStateVersion） |
| `NodeJoinController` | `discovery/zen/NodeJoinController.java` | ★ join 处理与等待当选 |
| `NodeJoinController.ElectionContext` | 同上 `:219` | 竞选上下文 |
| `MembershipAction` | `discovery/zen/MembershipAction.java` | ★ join/leave/validate RPC |
| `FaultDetection` | `discovery/zen/FaultDetection.java` | 故障检测基类 |
| `MasterFaultDetection` | `discovery/zen/MasterFaultDetection.java` | ★ 从节点监控 master |
| `NodesFaultDetection` | `discovery/zen/NodesFaultDetection.java` | ★ master 监控从节点 |
| `PublishClusterStateAction` | `discovery/zen/PublishClusterStateAction.java` | ★ 两阶段提交（见 09 章） |
| `PendingClusterStatesQueue` | `discovery/zen/PendingClusterStatesQueue.java` | 待提交状态队列 |
| `DiscoverySettings` | `discovery/DiscoverySettings.java` | `no_master_block` 等 |
| `Discovery` / `DiscoveryModule` | `discovery/` | 接口与模块装配 |

---

**上一篇**：[09-ClusterService与MasterService集群状态发布.md](./09-ClusterService与MasterService集群状态发布.md)　
**下一篇**：[11-分片分配与Allocation.md](./11-分片分配与Allocation.md)
