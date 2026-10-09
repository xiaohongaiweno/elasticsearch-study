# 25 · XPack 与商业特性源码结构

> 目标：把 x-pack 的**模块划分、装配方式、License 机制**讲清楚，
> 并以 **Security** 为重点剖析它是怎么用第 21 章那些扩展点"无侵入"地接进 ES 核心的。
>
> **为什么必须学**：
> - x-pack 是**官方给出的最佳扩展实践**：它几乎把 ES 的所有扩展点都用了一遍，
>   是写自研插件时最好的参考。
> - 云服务里的**鉴权、审计、监控、告警、生命周期管理**都对应 x-pack 的模块。
> - 「为什么开了 DLS/FLS 查询就变慢」→ `Security.onIndexModule:699` 的 `setSearcherWrapper` + `OptOutQueryCache`。
> - 「为什么某个功能突然不可用」→ `XPackLicenseState.isXxxAllowed()`。
> - 「能不能不改 core 就加功能」→ x-pack 证明了：**可以**。
>
> **前置**：第 21 章（插件机制与扩展点）—— 本篇是它的"实战案例"。
> **本篇对应**：阶段 5（生态期）收官。

---

## 1. 一图总览

```
x-pack/
├── plugin/
│   ├── core/            ← x-pack 核心：XPackPlugin、License、SSL、各 FeatureSet 的公共部分
│   │                       同时也放 core 层的 DTO（如 DocumentSubsetReader、FieldPermissions）
│   ├── security/        ← 安全（认证/授权/审计/TLS/DLS/FLS）
│   ├── monitoring/      ← 监控采集与导出
│   ├── watcher/         ← 告警
│   ├── ml/              ← 机器学习（+ ml-cpp 原生进程）
│   ├── ilm/             ← 索引生命周期管理
│   ├── ccr/             ← 跨集群复制
│   ├── sql/             ← SQL / JDBC
│   ├── rollup/          ← 预聚合 rollup
│   ├── graph/           ← 图探索
│   ├── upgrade/         ← 升级助手
│   ├── deprecation/     ← 弃用检查
│   └── logstash/        ← Logstash 管理
├── transport-client/    ← transport client 用的 x-pack 版本
├── qa/                  ← 集成测试
└── dev-tools/, docs/, license-tools/, test/

每个子目录都是一个**独立插件**（有自己的 plugin-descriptor.properties），
通过 extended.plugins 依赖 core。
```

**分层原则**：

| 层 | 内容 | 为什么分开 |
| --- | --- | --- |
| `plugin/core` | 不依赖"服务端运行时"的类（DTO、常量、License 状态、FeatureSet 接口） | transport client 也能用 |
| `plugin/<feature>` | 依赖服务端运行时（Node、ClusterService、IndexService） | 只在数据节点/master 节点加载 |

> **这个分层对你的启示**：如果你想写一个既能在服务端又能在客户端用的插件，
> 应该照抄这个结构 —— "core（纯数据/接口）" + "plugin（运行时）"。

---

## 2. 必读源码清单

| 类 | 路径 | 作用 |
| --- | --- | --- |
| `XPackPlugin` | `plugin/core/.../core/XPackPlugin.java` | ★ x-pack 主插件（`:89`） |
| `XPackClientPlugin` | `plugin/core/.../core/XPackClientPlugin.java` | 客户端基类 |
| `XPackFeatureSet` | `plugin/core/.../core/XPackFeatureSet.java` | 各功能的"能力描述"接口 |
| `XPackSettings` | `plugin/core/.../core/XPackSettings.java` | 全局设置 |
| `XPackLicenseState` | `plugin/core/.../license/XPackLicenseState.java` | ★ License 开关（`:345` 起） |
| `LicenseService` | `plugin/core/.../license/LicenseService.java` | License 管理与过期 |
| `Security` | `plugin/security/.../security/Security.java` | ★ 安全插件（`:261`） |
| `AuthenticationService` | `plugin/security/.../security/authc/AuthenticationService.java` | 认证 |
| `AuthorizationService` | `plugin/security/.../security/authz/AuthorizationService.java` | 授权 |
| `Realms` / `InternalRealms` | `plugin/security/.../security/authc/` | Realm 链 |
| `SecurityServerTransportInterceptor` | `plugin/security/.../security/transport/` | 节点间鉴权 |
| `SecurityIndexSearcherWrapper` | `plugin/security/.../security/authz/` | ★ **DLS**（文档级安全） |
| `DocumentSubsetReader` | `plugin/core/.../core/security/authz/accesscontrol/` | DLS 的 Lucene 层实现 |
| `FieldPermissions` / `FieldPermissionsCache` | `plugin/core/.../core/security/authz/permission/` | ★ **FLS**（字段级安全） |
| `IndicesAndAliasesResolver` | `plugin/security/.../security/authz/` | 解析请求里的索引名 |
| `OptOutQueryCache` | `plugin/core/.../core/security/authz/accesscontrol/` | FLS 时禁用 query cache |
| `SecurityIndexManager` | `plugin/security/.../security/support/` | `.security` 索引管理 |
| `Monitoring` / `MonitoringService` | `plugin/monitoring/.../monitoring/` | 监控 |
| `Watcher` / `WatcherService` | `plugin/watcher/.../watcher/` | 告警 |
| `MachineLearning` | `plugin/ml/.../ml/MachineLearning.java` | 机器学习 |
| `IndexLifecycle` / `IndexLifecycleService` | `plugin/ilm/.../indexlifecycle/` | ILM |
| `SqlPlugin` | `plugin/sql/.../sql/plugin/SqlPlugin.java` | SQL |
| `ShardFollowTasksExecutor` / `CcrRepository` | `plugin/ccr/.../ccr/` | CCR |
| `Rollup` / `RollupJobIdentifierUtils` | `plugin/rollup/.../rollup/` | Rollup |
| `SecurityExtension` | `plugin/core/.../core/security/SecurityExtension.java` | 安全扩展点（给闭源/自研用） |

---

## 3. 装配：x-pack 是怎么接进 ES 的

### 3.1 `XPackPlugin`

`plugin/core/src/main/java/org/elasticsearch/xpack/core/XPackPlugin.java:89`：

```java
public class XPackPlugin extends XPackClientPlugin
        implements ScriptPlugin, ExtensiblePlugin, RepositoryPlugin, EnginePlugin {
```

只实现了 4 个接口 —— 因为**大部分扩展点由各子插件自己实现**。

| 方法 | 行号 | 作用 |
| --- | --- | --- |
| `createGuiceModules()` | `:235` | License / SSL 的 Guice 模块 |
| `createComponents(...)` | `:248` | LicenseService、SSLService 等核心组件 |
| `getSettings()` | `:377` | 全局 setting |
| `onIndexModule(IndexModule)` | `:385` | 索引级钩子 |

`ExtensiblePlugin` 意味着**别的插件可以 `extended.plugins: x-pack` 来扩展它**
（回顾第 21 章 §3.6 —— 被扩展的插件必须先加载）。

### 3.2 `Security`：扩展点用得最全的一个

`plugin/security/.../security/Security.java:261`：

```java
public class Security extends Plugin
        implements ActionPlugin, IngestPlugin, NetworkPlugin, ClusterPlugin, DiscoveryPlugin, MapperPlugin,
                   ExtensiblePlugin {
```

**一个人实现了 8 个接口** —— 这就是"深度集成"的样子：

| 扩展点 | 行号 | x-pack 用它做什么 |
| --- | --- | --- |
| `createComponents` | `:394` | 构造 Realms、Authc/Authz 服务、审计、Token 服务 |
| **`onIndexModule`** | `:695` | ★ DLS（setSearcherWrapper）+ FLS（forceQueryCacheProvider）+ scroll 保护 |
| `getActions` | `:729` | 用户/角色/token/权限的 Action |
| `getRestHandlers` | `:777` | `/_security/*` REST |
| **`getRestHandlerWrapper`** | `:1022` | ★ 全局 REST 鉴权入口 |
| **`getTransportInterceptors`** | `:974` | ★ 节点间请求鉴权 |
| **`getHttpTransports`** | `:1008` | Security 版 HTTP（TLS / 双向认证） |
| **`getFieldFilter`** | `:1077` | ★ FLS：过滤 mappings / `_source` 字段 |
| `getJoinValidator`（DiscoveryPlugin） | `:1114-1157` | 节点入群校验（TLS、License、安全索引版本） |
| `getExecutorBuilders` | `:1033` | Token 服务专用线程池 |
| `getIndexTemplateMetaDataUpgrader` | `:1042` | 注入 `.security`、审计索引模板 |
| `getProcessors`（IngestPlugin） | — | `set_security_user` processor |

### 3.3 REST 鉴权的入口

`Security.java:1022`：

```java
public UnaryOperator<RestHandler> getRestHandlerWrapper(ThreadContext threadContext) {
    if (enabled == false || transportClientMode) {
        return null;
    }
    final boolean ssl = HTTP_SSL_ENABLED.get(settings);
    final SSLConfiguration httpSSLConfig = getSslService().getHttpTransportSSLConfiguration();
    boolean extractClientCertificate = ssl && getSslService().isSSLClientAuthEnabled(httpSSLConfig);
    return handler -> new SecurityRestFilter(getLicenseState(), threadContext, authcService.get(),
            handler, extractClientCertificate);
}
```

> **回顾第 21 章 §5.1**：`getRestHandlerWrapper` **只允许一个插件实现**。
> 所以：**只要你启用了 x-pack security，就不能再挂别的 REST wrapper**。
> 自研鉴权必须走 x-pack 的扩展点（`SecurityExtension`）或在它前面加代理。

### 3.4 节点间鉴权

`SecurityServerTransportInterceptor` + `ServerTransportFilter`：
拦截所有 transport 请求，从 `ThreadContext` 里取认证信息并校验。

> 这是 `NetworkPlugin.getTransportInterceptors()`（第 21 章 §5.7）的典型用法。
> 也是"为什么开启 security 后节点间通信有一点额外开销"的原因。

---

## 4. License 机制

### 4.1 `XPackLicenseState`：功能开关

`plugin/core/.../license/XPackLicenseState.java`。所有功能在使用前都要问它：

| 方法 | 行号 |
| --- | --- |
| `isAuthAllowed()` | `:345` |
| `isIpFilteringAllowed()` | `:356` |
| `isAuditingAllowed()` | `:366` |
| `isStatsAndHealthAllowed()` | `:379` |
| **`isDocumentAndFieldLevelSecurityAllowed()`** | `:395` |
| `isCustomRoleProvidersAllowed()` | `:436` |
| `isAuthorizationRealmAllowed()` | `:447` |
| `isWatcherAllowed()` | `:466` |
| `isMonitoringAllowed()` | `:489` |
| `isMonitoringClusterAlertsAllowed()` | `:499` |
| `isUpdateRetentionAllowed()` | `:512` |
| `isGraphAllowed()` | `:528` |
| `isMachineLearningAllowed()` | `:550` |
| `isRollupAllowed()` | `:564` |
| `isLogstashAllowed()` | `:572` |
| `isBeatsAllowed()` | `:581` |
| `isDeprecationAllowed()` | `:591` |
| `isUpgradeAllowed()` | `:603` |
| **`isIndexLifecycleAllowed()`** | `:617` |
| `isSqlAllowed()` | `:629` |
| `isJdbcAllowed()` | `:638` |
| `isOdbcAllowed()` | `:652` |
| **`isCcrAllowed()`** | `:704` |

注意：**大部分是 `synchronized` 的**（因为 license 状态会在运行时更新）。

> **排障**：`"current license is non-compliant for [security]"` /
> `"this feature is not available with the current license"` →
> 就是这些 `isXxxAllowed()` 返回 false。
> 用 `GET /_license` 和 `GET /_xpack` 查看当前 license 与功能状态。

### 4.2 `LicenseService`

`plugin/core/.../license/LicenseService.java`：

- 从 `.licenses` 索引（或 `license` 文件）加载 license；
- 注册 `ClusterStateListener`，license 变化时更新 `XPackLicenseState`；
- 到期后按 grace period 降级（会打 WARN 日志）。

> **工程启示**：这是"**运行时功能开关**"的一个成熟实现 ——
> 一个中心状态对象 + 各处 `if (allowed)`。
> 自研的多租户/分级功能完全可以照抄。

### 4.3 `XPackFeatureSet`

`plugin/core/.../core/XPackFeatureSet.java`：每个功能（security/ml/watcher/...）
实现一个 FeatureSet，暴露 `name()`、`available()`、`enabled()`、`usage()`。

`GET /_xpack/usage` 就是聚合所有 FeatureSet 的输出 —— **这是 Usage 统计的标准做法**。

---

## 5. Security 深度剖析

### 5.1 认证（Authentication）

```
HTTP/TCP 请求
    ↓ SecurityRestFilter / ServerTransportFilter
    ↓ AuthenticationService.authenticate(...)
    ↓ Realms（链）
         ├─ reserved（内置用户 elastic/kibana/logstash_system/beats_system/apm_system/remote_monitoring_user）
         ├─ native（.security 索引）
         ├─ file（users / users_roles 文件）
         ├─ ldap / active_directory
         ├─ pki（客户端证书）
         ├─ saml
         ├─ kerberos
         └─ custom（SecurityExtension 提供）
    ↓ 得到 User（username, roles, metadata, 是否 superuser）
    ↓ 写入 ThreadContext（后续所有操作从 ThreadContext 取）
```

| 类 | 路径 |
| --- | --- |
| `AuthenticationService` | `security/authc/AuthenticationService.java` |
| `Realms` / `InternalRealms` | `security/authc/Realms.java` / `InternalRealms.java` |
| `TokenService` | `security/authc/TokenService.java`（token 的签发/失效） |
| `CachingUsernamePasswordRealm` | `security/authc/support/`（缓存，减轻 LDAP 压力） |
| `DnRoleMapper` / `UserRoleMapper` | `security/authc/support/`（证书 DN → 角色映射） |

**关键设计**：认证结果放 **`ThreadContext`**。
ES 的 `ThreadContext` 会**自动跨线程传播**（第 05 章），
所以后续在 search/write 线程里都能拿到当前用户 —— 这是 DLS/FLS 能工作的前提。

> **排障**：`"unable to authenticate user [x] for REST request"` → Realm 链里没人认得这个用户。
> 用 `GET /_security/_authenticate` 验证。

### 5.2 授权（Authorization）

`security/authz/`：

| 类 | 作用 |
| --- | --- |
| `AuthorizationService` | ★ 入口：判断是否允许 |
| `AuthorizationUtils` | 工具 |
| `AuthorizedIndices` | 该用户能访问哪些索引 |
| `IndicesAndAliasesResolver` | ★ 把请求里的索引名（`logs-*`、`_all`）解析成具体索引 |
| `SecuritySearchOperationListener` | 搜索前后钩子（scroll 校验） |
| `accesscontrol/` | DLS/FLS 的 Lucene 实现 |
| `store/` | 角色存储（`NativeRolesStore`、`FileRolesStore`、`ReservedRolesStore`） |

权限模型（RBAC）：

```
Role = cluster privileges + indices privileges + applications + run_as
   indices privileges: [names, privileges(read/write/...), field_security, query, allow_restricted_indices]
User → roles（来自 Realm 或 role_mapping）
```

核心类在 `plugin/core/.../core/security/authz/permission/`：

| 类 | 作用 |
| --- | --- |
| `IndicesPermission` | 索引权限集合 |
| **`FieldPermissions`** | ★ FLS：允许的字段（include/exclude）|
| `FieldPermissionsCache` | FLS 缓存（**性能关键**）|
| `FieldPermissionsDefinition` | FLS 定义 |

### 5.3 DLS（文档级安全）

`Security.onIndexModule:695-718`：

```java
if (XPackSettings.DLS_FLS_ENABLED.get(settings)) {
    module.setSearcherWrapper(indexService ->
            new SecurityIndexSearcherWrapper(
                    shardId -> indexService.newQueryShardContext(shardId.id(),
                            // we pass a null index reader, which is legal and will disable rewrite optimizations
                            // based on index statistics, which is probably safer...
                            null,
                            () -> { throw new IllegalArgumentException(
                                "permission filters are not allowed to use the current timestamp"); },
                            null),
                    indexService.cache().bitsetFilterCache(),
                    indexService.getThreadPool().getThreadContext(), getLicenseState(),
                    indexService.getScriptService()));
    /* We need to forcefully overwrite the query cache implementation to use security's opt out query cache
     * implementation. This impl. disabled the query cache if field level security is used for a particular
     * request. If we wouldn't do forcefully overwrite the query cache implementation then we leave the system
     * vulnerable to leakages of data to unauthorized users. */
    module.forceQueryCacheProvider(
            (settings, cache) -> new OptOutQueryCache(settings, cache, threadContext.get(), getLicenseState()));
}
```

**三个要点**：

1. **`setSearcherWrapper`**：包装 `IndexSearcher`，在每次 `search()` 时
   把用户的角色查询（`query` 字段，支持模板）作为 `BooleanQuery(FILTER)` 注入。
   → **每个查询都要多算一遍过滤条件**，这是 DLS 变慢的根本原因。
2. **`OptOutQueryCache`**：**FLS 生效时禁用 query cache**。
   否则"用户 A 的查询结果缓存"可能被"用户 B"命中 → **数据泄露**。
   这也是 FLS 让查询变慢的第二个原因（缓存失效）。
3. **`null` index reader**：禁用基于索引统计的 rewrite 优化（源码注释说是"更安全"）。

`DocumentSubsetReader`（`plugin/core/.../core/security/authz/accesscontrol/`）：
Lucene 层面的 `FilterDirectoryReader`，只暴露有权访问的文档。

> **这是"用 Lucene 扩展点实现安全"的教科书案例**：
> 不改 ES 一行代码，靠 `IndexModule.setSearcherWrapper` + Lucene 的 `DirectoryReader` 包装就实现了。

### 5.4 FLS（字段级安全）

两处生效：

1. **`getFieldFilter`（`:1077`）**：过滤 mappings 和 `_source` 里暴露的字段。
2. **DLS/FLS 开启时**：`SecurityIndexSearcherWrapper` + `FieldPermissions` 在 Lucene 层过滤。

```java
public Function<String, Predicate<String>> getFieldFilter() {
    if (enabled) {
        return index -> {
            if (indicesAdminFilteredFields == false || getLicenseState().isDocumentAndFieldLevelSecurityAllowed() == false) {
                return MapperPlugin.NOOP_FIELD_PREDICATE;
            }
            ...
            throw new IllegalStateException("unexpected call to getFieldFilter for index [" + index + "] which is not granted");
        };
    }
    return MapperPlugin.super.getFieldFilter();
}
```

> **注意最后那句 `IllegalStateException`**（`:1090`）：
> 如果对"未授权"的索引还要过滤字段，说明**前面已经出错了** —— 这属于"最小惊讶"的防御式断言。

### 5.5 Scroll 保护

`Security.java:720-724`：

```java
// in order to prevent scroll ids from being maliciously crafted and/or guessed, a listener is added that
// attaches information to the scroll context so that we can validate the user that created the scroll against
// the user that is executing a scroll operation
module.addSearchOperationListener(
        new SecuritySearchOperationListener(threadContext.get(), getLicenseState(), auditTrailService.get()));
```

> 防止"猜到别人的 scroll id 就能读到数据"。

### 5.6 审计

`security/audit/`：`AuditTrailService` + `IndexAuditTrail`（写 `.security-audit` 索引）+ `LoggingAuditTrail`。
通过 `xpack.security.audit.enabled` 开启。

> **合规场景必开**。注意审计索引会持续增长，需要配合 ILM 清理。

### 5.7 `.security` 索引

`SecurityIndexManager`（`security/support/`）：

- 存储 native 用户、角色、role_mapping、token 的**系统索引**；
- 有 `TEMPLATE_VERSION_PATTERN`，版本不匹配时**拒绝服务**（防止读到旧结构）；
- 有专门的 `State` 枚举（如 `UNRECOVERABLE_STATE`）来反映健康度。

> **排障**：`"Security index is not available"` / `.security` 索引变红 →
> 所有 native 用户无法登录。此时只能用 `file` realm 或 `reserved` 用户（如果还有）。

---

## 6. 其它商业特性速览

### 6.1 Monitoring（监控）

`plugin/monitoring/.../monitoring/`：

| 类/包 | 作用 |
| --- | --- |
| `Monitoring` | 插件入口 |
| `MonitoringService` | 调度采集与导出周期 |
| `collector/` | 各类采集器（cluster、node、index、shard、job…） |
| `exporter/` | 导出器（local：写 `.monitoring-*` 索引；http：推到远集群） |
| `cleaner/` | 清理过期的 `.monitoring-*` 索引 |
| `rest/` | REST |

> 采集的就是第 24 章讲的那些 stats，然后写进 `.monitoring-es-*` 索引，供 Kibana 展示。
> **注意**：监控本身会消耗资源（采集 + 索引写入），
> `xpack.monitoring.collection.interval` 默认 10s，压力大的集群可调大。

### 6.2 Watcher（告警）

`plugin/watcher/.../watcher/`：

```
Watch = trigger(schedule) + input(search/http/simple/chain) + condition + actions(email/webhook/index/logging/...)
                                                                  ↓
                                                           .watches 索引
     执行：TriggerEngine → ExecutionService → 写 .watcher-history-*
```

关键类：`Watcher`、`WatcherService`、`WatcherLifeCycleService`、`execution/`、`condition/`、`input/`、`actions/`、`notification/`。

> 它本质上是"**定时 + 查询 + 条件 + 动作**"，
> 用 `PersistentTask`-like 的机制把 watch 存在索引里，靠 master 节点调度。

### 6.3 ML（机器学习）

`plugin/ml/.../ml/`：

| 类/包 | 作用 |
| --- | --- |
| `MachineLearning` | 插件入口（注册 Job、Datafeed、Autodetect 进程管理） |
| `MlLifeCycleService` / `MlInitializationService` | 启停 |
| `job/` | Job 配置与任务 |
| `datafeed/` | 数据供给（定期查询 ES 喂给 autodetect） |
| `process/` | 与 **C++ 进程（autodetect）** 通信 |
| `action/` / `rest/` | API |
| `notifications/` | 异常通知 |
| `MachineLearningPainlessExtension` | 扩展 Painless 白名单 |
| `MlDailyMaintenanceService` | 每日维护（删旧模型快照） |

> **注意**：ML 依赖 `ml-cpp`（原生进程，非 JVM）。
> 这解释了"为什么 ML 需要 native controller"和"为什么 ML 节点要单独规划内存"。

### 6.4 ILM（索引生命周期管理）

`plugin/ilm/.../indexlifecycle/`：

| 类 | 作用 |
| --- | --- |
| `IndexLifecycle` | 插件入口 |
| **`IndexLifecycleService`** | 主调度器（master 节点上跑） |
| **`IndexLifecycleRunner`** | 执行当前 step |
| `PolicyStepsRegistry` | policy → steps 的解析缓存 |
| `MoveToNextStepUpdateTask` / `MoveToErrorStepUpdateTask` / `ExecuteStepsUpdateTask` / `SetStepInfoUpdateTask` | ★ 各种 ClusterStateUpdateTask |
| `OperationModeUpdateTask` | ILM 运行模式（RUNNING/STOPPING/STOPPED） |
| `TimeValueSchedule` | 时间触发 |

**实现要点**：ILM 的进度存在 **`index.lifecycle.*` 索引 setting + `IndexMetaData` 的 custom** 里，
推进靠 **ClusterStateUpdateTask** —— 和第 09 章的机制完全一致。

> 这就是为什么 ILM 卡顿时要看 `GET /_cluster/pending_tasks` 和 `GET /<index>/_ilm/explain`。

### 6.5 CCR（跨集群复制）

`plugin/ccr/.../ccr/`：

| 类 | 作用 |
| --- | --- |
| `ShardFollowTask` / `ShardFollowNodeTask` | follower 分片的任务定义 |
| **`ShardFollowTasksExecutor`** | ★ `PersistentTasksExecutor` 的实现（第 21 章 §5.12） |
| `ShardChangesAction` | 从 leader 拉取变更（**按 seqno 增量**，见第 17 章） |
| `AutoFollowCoordinator` | 自动跟随新索引 |
| `repositories/CcrRepository` | 用"虚拟仓库"实现恢复（复用第 18 章 recovery 框架！） |
| `action/bulk/` | 批量写 follower |

> **最精妙的一点**：CCR 的**初始全量同步复用了 snapshot/restore 的框架**
> （`CcrRepository` 是一个"假的"仓库，数据直接从 leader 节点拉）。
> 这是"**用已有原语组合出新功能**"的典范。

### 6.6 SQL

`plugin/sql/.../sql/`：

| 类 | 作用 |
| --- | --- |
| `SqlPlugin` | 插件入口（ActionPlugin + SearchPlugin） |
| `RestSqlQueryAction` / `TransportSqlQueryAction` | 查询 |
| `RestSqlTranslateAction` | 把 SQL 翻译成 ES DSL（**调试神器**） |
| `RestSqlClearCursorAction` | 游标（分页） |
| `SqlLicenseChecker` | License 校验 |
| `SqlPainlessExtension` | 扩 Painless |

> `POST /_sql/translate` 是学习 ES DSL 的好工具。

### 6.7 Rollup

`plugin/rollup/.../rollup/`：

- `Rollup`（插件入口）、`RollupJobIdentifierUtils`、
  `RollupRequestTranslator` / `RollupResponseTranslator`（**把普通 agg 请求改写到 rollup 索引**）、
  `job/`（rollup job 与 persistent task）、`rest/`。
- 也是 `PersistentTasksExecutor` 的典型用法。

### 6.8 其它

| 插件 | 作用 |
| --- | --- |
| `graph` | 图探索（基于 significant_terms） |
| `upgrade` | 升级助手（`GET /_xpack/migration_assistance`） |
| `deprecation` | 弃用检查（`GET /_xpack/deprecation`） |
| `logstash` | Logstash pipeline 管理 |

---

## 7. 从 x-pack 学到的 8 条工程经验

1. **core / plugin 分层**：纯数据 + 接口放 core，运行时放 plugin —— 客户端也能复用。
2. **能力协商用 Feature / FeatureSet**：`GET /_xpack/usage` 是标准做法。
3. **运行时功能开关**：`XPackLicenseState` 一个中心对象 + 各处 `if`。
4. **安全边界靠 `ThreadContext`**：认证结果自动跨线程传播，是 DLS/FLS 的前提。
5. **Lucene 层才是终极扩展点**：`setSearcherWrapper` + `FilterDirectoryReader` 就能做 DLS。
6. **复用已有原语**：CCR 用 snapshot/restore 做全量同步；ILM 用 ClusterStateUpdateTask。
7. **缓存要能被安全策略关闭**：`OptOutQueryCache` —— 正确性优先于性能。
8. **防御式断言**：`getFieldFilter` 里对"不该发生"的情况直接抛 IllegalStateException。

---

## 8. 排障手册

### 8.1 Security

| 报错 | 原因 | 处理 |
| --- | --- | --- |
| `unable to authenticate user [x]` | Realm 链没匹配 | `GET /_security/_authenticate`；检查 realm 顺序 |
| `action [indices:data/read/search] is unauthorized for user [x]` | 缺权限 | 检查角色的 indices privileges |
| `current license is non-compliant for [security]` | License 不支持 | `GET /_license` |
| `Security index is not available` | `.security` 索引不可用 | 检查 `.security` 健康度 |
| `unexpected call to getFieldFilter for index [...] which is not granted` | 授权逻辑异常 | `Security.java:1090`；检查角色配置 |
| 开了 DLS/FLS 后查询变慢 | 每个查询多一层过滤 + 禁用 query cache | 简化角色 query；用 `OptOutQueryCache` 的行为预期 |
| `circuit_breaking_exception` 在 security 下更容易 | DLS 的 bitset 缓存占内存 | 调 `indices.requests.cache` / 断路器 |

```bash
# 常用 API
GET  /_security/_authenticate
GET  /_security/user
GET  /_security/role
GET  /_security/role_mapping
POST /_security/user/<name>/_password
GET  /_xpack/usage
GET  /_license
```

### 8.2 各功能的状态

```bash
GET /_xpack                 # 所有 feature 的 available/enabled
GET /_xpack/usage           # 使用统计
GET /_license

# ILM
GET /<index>/_ilm/explain
GET /_ilm/policy

# CCR
GET /_ccr/stats
GET /_ccr/auto_follow

# ML
GET /_ml/anomaly_detectors
GET /_ml/datafeeds

# Watcher
GET /_watcher/stats
```

### 8.3 云服务落地建议

1. **Security 必开**：至少要有 `native` realm + 传输层 TLS。
2. **DLS/FLS 谨慎使用**：正确性优先，但性能代价明显；先在测试环境压测。
3. **Monitoring 的 `.monitoring-*` 索引要用 ILM 清理**，否则会持续增长。
4. **ILM 卡顿先看 pending_tasks**：`GET /_cluster/pending_tasks`。
5. **CCR 依赖 seqno**：leader 的软删除/translog 保留策略要配置好（6.6 的 CCR 是 beta）。
6. **ML 节点独立规划**：native 进程不吃 JVM heap，但吃机器内存。
7. **License 到期要有告警**：`xpack.license.self_generated` / 正式 license 的到期时间。

---

## 9. 动手实验

### 实验 1：看 Security 用了哪些扩展点

```bash
cd x-pack/plugin/security
grep -n "@Override" src/main/java/org/elasticsearch/xpack/security/Security.java | head -40
# 对照第 21 章的扩展点表
```

### 实验 2：开启 DLS 观察查询变化

```bash
# 1. 建一个带 query 的角色
PUT /_security/role/only_a
{"indices":[{"names":["my-*"],"privileges":["read"],"query":"{\"term\":{\"team\":\"a\"}}"}]}

# 2. 建用户
PUT /_security/user/u1
{"password":"changeme","roles":["only_a"]}

# 3. 用 u1 查询，用 _search 的 profile 看是否多了一层 filter
curl -u u1:changeme 'localhost:9200/my-index/_search?pretty'
# → DLS 的过滤条件会体现在 profile 的 query 结构里
```

### 实验 3：SQL translate

```bash
POST /_sql/translate
{"query":"SELECT team, COUNT(*) FROM my-index GROUP BY team"}
# → 看它被翻译成什么 ES DSL
```

### 实验 4：观察 license 开关

```bash
GET /_xpack/usage
GET /_license
# 对照 XPackLicenseState 里的 isXxxAllowed 表
```

### 实验 5：断点清单

| 断点 | 目的 |
| --- | --- |
| `Security.onIndexModule`（`:695`） | 看 DLS/FLS 的装配 |
| `Security.getRestHandlerWrapper`（`:1022`） | 看 REST 鉴权入口 |
| `Security.getFieldFilter`（`:1077`） | 看 FLS 的字段过滤 |
| `Security.getJoinValidator`（`:1114-1157`） | 看节点入群校验 |
| `AuthenticationService.authenticate` | 看认证与 ThreadContext |
| `AuthorizationService` | 看授权判断 |
| `SecurityIndexSearcherWrapper` | 看 DLS 的查询注入 |
| `OptOutQueryCache` | 看 FLS 时缓存如何被禁用 |
| `IndexLifecycleService` / `IndexLifecycleRunner` | 看 ILM 如何用 ClusterStateUpdateTask |
| `ShardFollowTasksExecutor` | 看 PersistentTask 的实际用法 |
| `XPackLicenseState.isXxxAllowed` | 看运行时功能开关 |
| `MonitoringService` | 看采集调度 |

### 实验 6：跑 x-pack 测试

```bash
./gradlew :x-pack:plugin:security:test
./gradlew :x-pack:plugin:ilm:test
./gradlew :x-pack:plugin:sql:test
./gradlew :x-pack:qa:multi-cluster-search:integTestRunner
```

---

## 10. 自测题（答案都在本章）

1. x-pack 里的 `plugin/core` 和 `plugin/<feature>` 是怎么分层的？为什么要这样？
2. `XPackPlugin` 实现了哪 4 个接口？为什么它不像 Security 那样实现很多？
3. `Security` 实现了哪些扩展点？挑 4 个说明它用它们做了什么。
4. 为什么说"启用 x-pack security 后不能再挂别的 REST wrapper"？
5. 节点间请求是怎么鉴权的？用了哪个扩展点？
6. `XPackLicenseState` 的作用是什么？举 5 个它控制的开关。
7. 大部分 `isXxxAllowed()` 为什么是 `synchronized` 的？
8. `XPackFeatureSet` 是做什么的？`GET /_xpack/usage` 的数据从哪来？
9. x-pack 支持哪些 Realm？认证结果存在哪里？为什么这样设计？
10. `ThreadContext` 在 security 里扮演什么角色？
11. DLS 是怎么实现的？它依赖 `IndexModule` 的哪个扩展点？
12. `OptOutQueryCache` 是做什么的？为什么必须用它？（安全性角度）
13. 为什么开了 DLS/FLS 后查询会变慢？（至少 2 个原因）
14. FLS 有哪两处生效？
15. `getFieldFilter` 里那句 `IllegalStateException` 说明什么设计思想？
16. Scroll id 为什么需要额外保护？怎么保护的？
17. `.security` 索引有什么特殊性？它挂了会怎样？
18. Monitoring 采集的是什么数据？导出到哪？有什么副作用？
19. Watcher 的基本模型是什么（4 要素）？
20. ML 为什么要 native controller？它的进程叫什么？
21. ILM 的进度存在哪里？推进靠什么机制？
22. ILM 卡顿时应该看什么 API？
23. CCR 的全量同步复用了什么框架？这体现了什么设计思想？
24. CCR 的增量同步依赖第几章讲的什么机制？
25. `POST /_sql/translate` 有什么用？
26. Rollup 和 PersistentTask 的关系？
27. `SecurityExtension` 是给谁用的？
28. 从 x-pack 学到的 8 条工程经验里，哪一条对你最有启发？为什么？
29. 如果你要自研一个"租户隔离"插件，你会用哪些扩展点？（至少 4 个）
30. 如果要在 ES 之外再包一层网关做鉴权，和 x-pack 的方案各有什么优劣？

---

## 11. 关键类速查表

| 类 | 路径（`x-pack/`） | 关键行 |
| --- | --- | --- |
| `XPackPlugin` | `plugin/core/.../core/XPackPlugin.java` | `class:89`、`createGuiceModules:235`、`createComponents:248`、`getSettings:377`、`onIndexModule:385` |
| `XPackFeatureSet` | `plugin/core/.../core/XPackFeatureSet.java` | `name()`、`available()`、`enabled()`、`usage()` |
| `XPackLicenseState` | `plugin/core/.../license/XPackLicenseState.java` | `isAuthAllowed:345`、`isIpFilteringAllowed:356`、`isAuditingAllowed:366`、`isStatsAndHealthAllowed:379`、**`isDocumentAndFieldLevelSecurityAllowed:395`**、`isCustomRoleProvidersAllowed:436`、`isAuthorizationRealmAllowed:447`、`isWatcherAllowed:466`、`isMonitoringAllowed:489`、`isMonitoringClusterAlertsAllowed:499`、`isUpdateRetentionAllowed:512`、`isGraphAllowed:528`、`isMachineLearningAllowed:550`、`isRollupAllowed:564`、`isLogstashAllowed:572`、`isBeatsAllowed:581`、`isDeprecationAllowed:591`、`isUpgradeAllowed:603`、`isIndexLifecycleAllowed:617`、`isSqlAllowed:629`、`isJdbcAllowed:638`、`isOdbcAllowed:652`、`isCcrAllowed:704` |
| `LicenseService` | `plugin/core/.../license/LicenseService.java` | License 加载/更新/过期 |
| `Security` | `plugin/security/.../security/Security.java` | `class:261`、`createComponents:394`、**`onIndexModule:695`**（DLS `:699`、OptOutQueryCache `:716`、scroll listener `:723`）、`getActions:729`、`getRestHandlers:777`、`getTransportInterceptors:974`、`getHttpTransports:1008`、**`getRestHandlerWrapper:1022`**、`getExecutorBuilders:1033`、`getIndexTemplateMetaDataUpgrader:1042`、**`getFieldFilter:1077`**、`ValidateTLSOnJoin:1114`、`ValidateUpgradedSecurityIndex:1133`、`ValidateLicenseCanBeDeserialized:1146`、`ValidateLicenseForFIPS:1157` |
| `AuthenticationService` | `plugin/security/.../security/authc/AuthenticationService.java` | 认证入口 |
| `AuthorizationService` | `plugin/security/.../security/authz/AuthorizationService.java` | 授权入口 |
| `IndicesAndAliasesResolver` | `plugin/security/.../security/authz/` | 索引名解析 |
| `SecuritySearchOperationListener` | `plugin/security/.../security/authz/` | scroll 校验 |
| `SecurityIndexSearcherWrapper` | `plugin/security/.../security/authz/` | ★ DLS |
| `DocumentSubsetReader` | `plugin/core/.../core/security/authz/accesscontrol/` | DLS 的 Lucene 实现 |
| `FieldPermissions` | `plugin/core/.../core/security/authz/permission/` | ★ FLS |
| `FieldPermissionsCache` | 同上 | FLS 缓存 |
| `FieldPermissionsDefinition` | 同上 | FLS 定义 |
| `IndicesPermission` | 同上 | 索引权限 |
| `OptOutQueryCache` | `plugin/core/.../core/security/authz/accesscontrol/` | FLS 时禁用缓存 |
| `SecurityIndexManager` | `plugin/security/.../security/support/` | `.security` 索引 |
| `SecurityServerTransportInterceptor` | `plugin/security/.../security/transport/` | 节点间鉴权 |
| `SecurityExtension` | `plugin/core/.../core/security/SecurityExtension.java` | 安全扩展点 |
| `Monitoring` / `MonitoringService` | `plugin/monitoring/.../monitoring/` | 采集调度；`collector/`、`exporter/`、`cleaner/` |
| `Watcher` / `WatcherService` | `plugin/watcher/.../watcher/` | 告警；`execution/`、`condition/`、`input/`、`actions/`、`notification/` |
| `MachineLearning` | `plugin/ml/.../ml/MachineLearning.java` | ML；`job/`、`datafeed/`、`process/`、`MachineLearningPainlessExtension` |
| `IndexLifecycle` / `IndexLifecycleService` / `IndexLifecycleRunner` | `plugin/ilm/.../indexlifecycle/` | ILM；`PolicyStepsRegistry`、`MoveToNextStepUpdateTask`、`ExecuteStepsUpdateTask`、`OperationModeUpdateTask`、`TimeValueSchedule` |
| `ShardFollowTasksExecutor` / `ShardFollowTask` / `ShardChangesAction` / `AutoFollowCoordinator` | `plugin/ccr/.../ccr/action/` | CCR |
| `CcrRepository` | `plugin/ccr/.../ccr/repositories/` | CCR 的"虚拟仓库" |
| `SqlPlugin` / `RestSqlTranslateAction` | `plugin/sql/.../sql/plugin/` | SQL |
| `Rollup` / `RollupRequestTranslator` | `plugin/rollup/.../rollup/` | Rollup |

---

## 12. 系列收官：25 篇 + 附录

到这里，ES 6.6.1 的主链路与生态全部打通：

| 阶段 | 篇章 | 主题 |
| --- | --- | --- |
| **基础期** | 01–04 | 模块全景、启动流程、插件与 SPI、HTTP/REST |
| **核心期** | 05–09 | Transport、线程池、Action 框架、线程池实践、Index/Shard 模型 |
| **集群期** | 10–14 | Mapping、分配、ClusterState 与选主、写入链路、Translog |
| **引擎期** | 15–18 | Engine/Lucene、Refresh/Flush/Merge、复制与 SeqNo、Recovery |
| **读取期** | 19–20 | 检索流程（QTF/DFS）、聚合框架 |
| **生态期** | 21–25 | 插件与扩展点、Ingest 与 Script、Snapshot 与 Gateway、监控与内存保护、x-pack |

**下一步建议**：

1. **读测试代码**：ES 的测试质量极高，`ReplicationTrackerTests`、`IndexRecoveryTests`、
   `SearchPhaseControllerTests`、`TermsAggregatorTests`、`PluginsServiceTests` 都是理解边界条件最快的材料。
2. **跑起来调试**：`./gradlew :server:test --tests ...`，或单机起节点打断点。
3. **读 x-pack**：它是"如何在不改 core 的前提下扩展 ES"的完整答案。
4. **对照上游**：遇到线上问题时，用本系列的行号去 GitHub 找对应的改动历史与修复版本。

---

**上一篇**：[24-监控Stats与内存保护.md](./24-监控Stats与内存保护.md)
**下一篇（附录）**：[附录A-核心类速查表.md](./附录A-核心类速查表.md)
