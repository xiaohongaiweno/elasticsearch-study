# 22 · Ingest 管道与 Script 脚本

> 目标：讲清 **Ingest pipeline**（写入前的数据预处理）和 **Script**（Painless 等脚本引擎）的实现，
> 包括它们**如何在集群里存储**、**如何编译/缓存**、以及**为什么脚本是"性能杀手"**。
>
> **为什么必须学**：
> - 「`[script] Too many dynamic script compilations within`」→ 编译限流（`ScriptService.checkCompilationLimit:390`）；
>   这在生产上**极其常见**，且会让写/查全部失败。
> - 「pipeline 改了没生效 / 有的节点生效有的不生效」→ pipeline 存在 `ClusterState` 里，靠 `ClusterStateApplier` 同步。
> - 「bulk 突然变慢」→ 你可能给索引加了 pipeline，`grok` 是 CPU 杀手。
> - 「pipeline 里 `drop` 之后 bulk 返回了空响应」→ `CompoundProcessor.execute:124` 的 null 语义。
> - 云服务里**日志采集、字段规整、GeoIP 富化**几乎全靠 ingest pipeline。
>
> **前置**：第 08 章（ClusterState）、第 09 章（ClusterState 发布）、第 13 章（写入链路）、第 21 章（插件扩展点）。
> **本篇对应**：阶段 5（生态期）。

---

## 1. 一图总览

```
  写入请求（BulkRequest，带 ?pipeline=xxx 或 index.default_pipeline）
        ↓
  TransportBulkAction.processBulkIndexIngestRequest  (:564)
        ↓
  IngestService.executeBulkRequest  (:377)      ← 在 WRITE 线程池执行
        │
        ├─ 对每个 IndexRequest：pipelines.get(pipelineId)
        │      └─ innerExecute  (:455)
        │            ├─ 构造 IngestDocument(index, type, id, routing, ..., sourceAsMap)
        │            ├─ pipeline.execute(ingestDocument)   ← CompoundProcessor.execute (:117)
        │            │      → 返回 null 表示 drop
        │            └─ 回写 indexRequest（index/type/id/routing/version/source）
        ↓
  继续走正常的 bulk（分片路由 → 主分片 → 副本）

  ─────────────────────────────────────────────────────
  Pipeline 从哪来？
  PUT /_ingest/pipeline/xxx
        → IngestService.putPipeline (:212) → ClusterStateUpdateTask
        → ClusterState.metaData().custom("ingest") = IngestMetadata
        → 所有节点 IngestService.applyClusterState (:256)
        → innerUpdatePipelines (:498) 重新构造 Pipeline 对象
```

---

## 2. 必读源码清单

### Ingest

| 类 | 路径 | 作用 |
| --- | --- | --- |
| `IngestService` | `ingest/IngestService.java` | ★ 中心服务（`executeBulkRequest:377`、`innerExecute:455`、`innerUpdatePipelines:498`） |
| `IngestMetadata` | `ingest/IngestMetadata.java` | `MetaData.Custom`，`TYPE="ingest"`（`:47`） |
| `PipelineConfiguration` | `ingest/PipelineConfiguration.java` | ClusterState 里的 pipeline 配置（**不持有 Processor 对象**） |
| `Pipeline` | `ingest/Pipeline.java` | 可执行 pipeline（`create:68`、`execute:93`） |
| `Processor` | `ingest/Processor.java` | 处理器接口 + `Factory` + `Parameters` |
| `CompoundProcessor` | `ingest/CompoundProcessor.java` | ★ 处理器链 + `on_failure`（`execute:117`） |
| `AbstractProcessor` | `ingest/AbstractProcessor.java` | tag/type 的基类 |
| `ConditionalProcessor` | `ingest/ConditionalProcessor.java` | `if` 条件包装器 |
| `PipelineProcessor` | `ingest/PipelineProcessor.java` | pipeline 里调另一个 pipeline |
| `DropProcessor` | `ingest/DropProcessor.java` | 丢文档（返回 null） |
| `TrackingResultProcessor` | `ingest/TrackingResultProcessor.java` | 统计包装器 |
| `IngestDocument` | `ingest/IngestDocument.java` | ★ 文档模型（`getFieldValue:114`、`setFieldValue:415`、`appendFieldValue:384`） |
| `ConfigurationUtils` | `ingest/ConfigurationUtils.java` | ★ 配置读取 + 报错（`readStringProperty:72` 等） |
| `ValueSource` | `ingest/ValueSource.java` | 值来源（常量 or 模板） |
| `IngestMetric` / `IngestStats` | `ingest/` | 统计 |

### Script

| 类 | 路径 | 作用 |
| --- | --- | --- |
| `ScriptService` | `script/ScriptService.java` | ★ 编译 + 缓存 + 限流（`compile:283`、`checkCompilationLimit:390`） |
| `ScriptModule` | `script/ScriptModule.java` | ★ 装配引擎与上下文（`CORE_CONTEXTS:40`、`ctor:68`） |
| `ScriptEngine` | `script/ScriptEngine.java` | 脚本引擎接口（Painless 实现它） |
| `ScriptContext` | `script/ScriptContext.java` | ★ 脚本"用途"类型（`class:57`） |
| `ScriptType` | `script/ScriptType.java` | `INLINE`（`:44`） / `STORED`（`:53`） |
| `Script` | `script/Script.java` | 脚本描述（type/lang/idOrCode/options/params） |
| `ScriptMetaData` | `script/ScriptMetaData.java` | 存储在 ClusterState 的脚本 |
| `StoredScriptSource` | `script/StoredScriptSource.java` | stored script 的 source/lang/options |
| `ScriptMetrics` / `ScriptStats` | `script/` | 编译/执行计数 |

---

## 3. Ingest：Pipeline 存在哪里

### 3.1 `IngestMetadata`（ClusterState 的 Custom）

`ingest/IngestMetadata.java:45`：

```java
public final class IngestMetadata implements MetaData.Custom {
    public static final String TYPE = "ingest";      // :47
    ...
}
```

**关键设计**（源码注释 `:56`，值得原文记住）：

> 我们**不能**把 `Pipeline` 类直接放进集群状态，因为
> 应用集群状态时**我们手上没有 processor factories**。

所以 ClusterState 里存的是 `PipelineConfiguration`（纯配置 Map），
由**每个节点**在 `applyClusterState` 时用自己本地的 `processorFactories` 构造出 `Pipeline` 对象。

> **这解释了一个经典现象**：
> 如果某个节点**缺少某个插件**（比如 `ingest-attachment`），
> 那么在该节点上 pipeline 会构造失败 → `substitutePipeline`（`:319`）把它替换成一个**永远抛异常的 pipeline**，
> 并且 `ExceptionsHelper.rethrowAndSuppress`（`:528`）会把错误抛出来。
> 表现就是"别的节点能写入，这个节点报 `processor [attachment] not found`"。

### 3.2 Pipeline 的 JSON 结构

`Pipeline`（`ingest/Pipeline.java:37`）识别的 key：

```java
public static final String DESCRIPTION_KEY = "description";    // :39
public static final String PROCESSORS_KEY  = "processors";     // :40
public static final String VERSION_KEY     = "version";        // :41
public static final String ON_FAILURE_KEY  = "on_failure";     // :42
```

### 3.3 变更如何生效

`IngestService.applyClusterState`（`:256`）：

```java
public void applyClusterState(final ClusterChangedEvent event) {
    ClusterState state = event.state();
    Map<String, Pipeline> originalPipelines = pipelines;
    innerUpdatePipelines(event.previousState(), state);
    // pipelines changed, so add the old metrics to the new metrics
    if (originalPipelines != pipelines) { ... 迁移统计 ... }
}
```

`innerUpdatePipelines`（`:498`）：

```java
if (state.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
    return;                                                    // :499  网关未恢复 → 跳过
}
IngestMetadata ingestMetadata = state.getMetaData().custom(IngestMetadata.TYPE);
IngestMetadata previousIngestMetadata = previousState.getMetaData().custom(IngestMetadata.TYPE);
if (Objects.equals(ingestMetadata, previousIngestMetadata)) {
    return;                                                    // :505  没变 → 直接返回
}
Map<String, Pipeline> pipelines = new HashMap<>();
List<ElasticsearchParseException> exceptions = new ArrayList<>();
for (PipelineConfiguration pipeline : ingestMetadata.getPipelines().values()) {
    try {
        pipelines.put(pipeline.getId(),
            Pipeline.create(pipeline.getId(), pipeline.getConfigAsMap(), processorFactories, scriptService));  // :515
    } catch (ElasticsearchParseException e) {
        pipelines.put(pipeline.getId(), substitutePipeline(pipeline.getId(), e));   // :518
        exceptions.add(e);
    } catch (Exception e) {
        ...
    }
}
this.pipelines = Collections.unmodifiableMap(pipelines);       // :527
ExceptionsHelper.rethrowAndSuppress(exceptions);               // :528
```

要点：

- `pipelines` 是 `volatile Map`（`:79`），**整体替换**而非原地修改 → 读无锁、天然线程安全。
- 只比较 `IngestMetadata`（配置），**不等则全部重建** —— 改一条 pipeline 会重建所有 pipeline。
  所以"频繁 PUT pipeline"是有成本的。

---

## 4. Ingest：执行流程

### 4.1 Bulk 中的调用点

`TransportBulkAction.processBulkIndexIngestRequest`（`:564`）：

```java
long ingestStartTimeInNanos = System.nanoTime();
BulkRequestModifier bulkRequestModifier = new BulkRequestModifier(original);
ingestService.executeBulkRequest(() -> bulkRequestModifier,
    (indexRequest, exception) -> {                       // 单条失败
        bulkRequestModifier.markCurrentItemAsFailed(exception);
    }, (exception) -> {                                  // 全部完成
        long ingestTookInMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - ingestStartTimeInNanos);
        BulkRequest bulkRequest = bulkRequestModifier.getBulkRequest();
        if (bulkRequest.requests().isEmpty()) {
            // 全部被 drop 或失败 → 直接返回空响应，不再走分片
            actionListener.onResponse(new BulkResponse(new BulkItemResponse[0], 0));    // :585
        } else {
            doExecute(task, bulkRequest, actionListener);
        }
    },
    indexRequest -> bulkRequestModifier.markCurrentItemAsDropped());   // :591
```

> **`ingestTookInMillis` 就是响应里的 `ingest_took` 字段**。
> 如果看到 `took` 很小但 `ingest_took` 很大 —— 瓶颈在 pipeline，不在分片写入。

### 4.2 `executeBulkRequest`（`:377`）

```java
threadPool.executor(ThreadPool.Names.WRITE).execute(new AbstractRunnable() {   // :381 ★ WRITE 线程池
    protected void doRun() {
        for (DocWriteRequest<?> actionRequest : actionRequests) {
            IndexRequest indexRequest = TransportBulkAction.getIndexWriteRequest(actionRequest);
            if (indexRequest == null) continue;                     // 非 index 请求跳过
            String pipelineId = indexRequest.getPipeline();
            if (NOOP_PIPELINE_NAME.equals(pipelineId) == false) {   // :396
                try {
                    Pipeline pipeline = pipelines.get(pipelineId);
                    if (pipeline == null) {
                        throw new IllegalArgumentException(
                            "pipeline with id [" + pipelineId + "] does not exist");   // :400
                    }
                    innerExecute(indexRequest, pipeline, itemDroppedHandler);
                    // this shouldn't be needed here but we do it for consistency with index api
                    // which requires it to prevent double execution
                    indexRequest.setPipeline(NOOP_PIPELINE_NAME);   // :405 ★ 防重复执行
                } catch (Exception e) {
                    itemFailureHandler.accept(indexRequest, e);
                }
            }
        }
        completionHandler.accept(null);
    }
});
```

**注意点**：

1. Ingest 跑在 **WRITE 线程池**（`:381`），和写入共用。
   pipeline 很重（如 grok）会**占满 WRITE 线程池 → 所有写入排队**。
2. `NOOP_PIPELINE_NAME = "_none"`（`:70`）用于**幂等**：pipeline 执行完立刻把请求的 pipeline 置为 `_none`，
   防止同一请求被重复处理（比如 retry 或 `default_pipeline` 与 `pipeline` 同时命中）。
3. `pipeline does not exist` 是**单条 item 失败**（`itemFailureHandler`），不是整个 bulk 失败。

### 4.3 `innerExecute`（`:455`）

```java
if (pipeline.getProcessors().isEmpty()) return;                     // :456

long startTimeInNanos = System.nanoTime();
try {
    totalMetrics.preIngest();
    String index = indexRequest.index();
    ...
    Map<String, Object> sourceAsMap = indexRequest.sourceAsMap();
    IngestDocument ingestDocument = new IngestDocument(index, type, id, routing, null, version, versionType, sourceAsMap);  // :472
    if (pipeline.execute(ingestDocument) == null) {                 // :473 ★ null = drop
        itemDroppedHandler.accept(indexRequest);
    } else {
        Map<IngestDocument.MetaData, Object> metadataMap = ingestDocument.extractMetadata();
        // it's fine to set all metadata fields all the time, as ingest document holds their starting values
        indexRequest.index((String) metadataMap.get(IngestDocument.MetaData.INDEX));
        indexRequest.type((String) metadataMap.get(IngestDocument.MetaData.TYPE));
        indexRequest.id((String) metadataMap.get(IngestDocument.MetaData.ID));
        indexRequest.routing((String) metadataMap.get(IngestDocument.MetaData.ROUTING));
        indexRequest.version(((Number) metadataMap.get(IngestDocument.MetaData.VERSION)).longValue());
        if (metadataMap.get(IngestDocument.MetaData.VERSION_TYPE) != null) {
            indexRequest.versionType(VersionType.fromString(...));
        }
        indexRequest.source(ingestDocument.getSourceAndMetadata());  // :487
    }
} catch (Exception e) {
    totalMetrics.ingestFailed();
    throw e;
} finally {
    long ingestTimeInMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeInNanos);
    totalMetrics.postIngest(ingestTimeInMillis);                     // :494
}
```

> **关键**：pipeline 可以修改 `_index` / `_id` / `routing` / `version`。
> 这也是 `date_index_name` processor 能做到"按日期写到不同索引"的原理 —— **在路由之前**改写请求。

---

## 5. Processor 模型

### 5.1 `CompoundProcessor`（处理器链）

`ingest/CompoundProcessor.java:39`：

```java
public static final String ON_FAILURE_MESSAGE_FIELD       = "on_failure_message";         // :40
public static final String ON_FAILURE_PROCESSOR_TYPE_FIELD = "on_failure_processor_type"; // :41
public static final String ON_FAILURE_PROCESSOR_TAG_FIELD  = "on_failure_processor_tag";  // :42

private final boolean ignoreFailure;              // :44
private final List<Processor> processors;         // :45
private final List<Processor> onFailureProcessors;// :46
private final List<Tuple<Processor, IngestMetric>> processorsWithMetrics;  // :47
```

`execute`（`:117`）—— **null 语义是"丢弃文档"**：

```java
for (Tuple<Processor, IngestMetric> processorWithMetric : processorsWithMetrics) {
    Processor processor = processorWithMetric.v1();
    IngestMetric metric = processorWithMetric.v2();
    long startTimeInNanos = relativeTimeProvider.getAsLong();
    try {
        metric.preIngest();
        if (processor.execute(ingestDocument) == null) {
            return null;                                       // :125 ★ drop，整条链终止
        }
    } catch (Exception e) {
        metric.ingestFailed();
        if (ignoreFailure) {
            continue;                                          // :130  忽略，继续下一个
        }
        ElasticsearchException compoundProcessorException =
            newCompoundProcessorException(e, processor.getType(), processor.getTag());
        if (onFailureProcessors.isEmpty()) {
            throw compoundProcessorException;                  // :136  没有 on_failure → 抛出
        } else {
            if (executeOnFailure(ingestDocument, compoundProcessorException) == false) {
                return null;                                   // :139  on_failure 里 drop
            }
            break;                                             // :141  执行完 on_failure 就结束
        }
    } finally {
        long ingestTimeInMillis = ...;
        metric.postIngest(ingestTimeInMillis);
    }
}
return ingestDocument;
```

**语义总结**：

| 情况 | 结果 |
| --- | --- |
| 所有 processor 正常 | 返回文档，继续落盘 |
| 某个 processor 返回 `null`（`drop`） | **整条 pipeline 终止**，文档被丢弃 |
| 抛异常 + `ignore_failure: true` | 跳过该 processor，继续下一个 |
| 抛异常 + 有 `on_failure` | 执行 `on_failure` 链，然后**结束**（不继续后续 processor） |
| 抛异常 + 无 `on_failure` | 抛出 → 该 bulk item 失败 |

`on_failure` 期间的元数据（`putFailureMetadata:172`）：

```java
ingestMetadata.put(ON_FAILURE_MESSAGE_FIELD,        cause.getRootCause().getMessage());
ingestMetadata.put(ON_FAILURE_PROCESSOR_TYPE_FIELD, failedProcessorType);
ingestMetadata.put(ON_FAILURE_PROCESSOR_TAG_FIELD,  failedProcessorTag);
```

即 `on_failure` 里可以用 `{{_ingest.on_failure_message}}` 等模板变量。
`removeFailureMetadata`（`:183`）在 `finally` 里清理。

### 5.2 特殊 Processor

| 类 | 作用 |
| --- | --- |
| `ConditionalProcessor` | 包装真实 processor，配 `if` 条件（脚本）；`getProcessor()` 拿被包装者 |
| `PipelineProcessor` | 调用另一个 pipeline（可递归）；注意 `getPipelineName()` |
| `DropProcessor` | 直接返回 null → 丢弃 |
| `FailProcessor`（ingest-common） | 主动抛异常，配合 `on_failure` 做数据校验 |
| `TrackingResultProcessor` | 统计包装器（tracks 成功/失败/耗时） |

`getProcessorName`（`IngestService:434`）说明了统计名字的规则：

```java
if (processor instanceof ConditionalProcessor) {
    processor = ((ConditionalProcessor) processor).getProcessor();   // :437 剥掉条件包装
}
sb.append(processor.getType());
if (processor instanceof PipelineProcessor) {
    sb.append(":").append(((PipelineProcessor) processor).getPipelineName());
}
String tag = processor.getTag();
if (tag != null && !tag.isEmpty()) {
    sb.append(":").append(tag);
}
```

→ 统计里看到的名字形如 `grok`、`set:my_tag`、`pipeline:inner`。

### 5.3 写一个 Processor

```java
public class MyProcessor extends AbstractProcessor {
    public static final String TYPE = "my_processor";

    private final String field;
    private final TemplateScript.Factory valueTemplate;

    public MyProcessor(String tag, String field, TemplateScript.Factory valueTemplate) {
        super(tag);
        this.field = field;
        this.valueTemplate = valueTemplate;
    }

    @Override
    public IngestDocument execute(IngestDocument doc) throws Exception {
        doc.setFieldValue(field, doc.renderTemplate(valueTemplate));
        return doc;                     // ★ 返回 null 表示 drop
    }

    @Override
    public String getType() { return TYPE; }

    public static final class Factory implements Processor.Factory {
        @Override
        public MyProcessor create(Map<String, Processor.Factory> factories, String tag,
                                  Map<String, Object> config) throws Exception {
            String field = ConfigurationUtils.readStringProperty(TYPE, tag, config, "field");
            TemplateScript.Factory value = ConfigurationUtils.compileTemplate(TYPE, tag, "value",
                ConfigurationUtils.readStringProperty(TYPE, tag, config, "value"),
                scriptService);
            return new MyProcessor(tag, field, value);
        }
    }
}
```

注册（第 21 章的 `IngestPlugin`）：

```java
public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
    return Collections.singletonMap(MyProcessor.TYPE, new MyProcessor.Factory(parameters));
}
```

> **重名会报错**：`IngestService.processorFactories`（`:101`）里：
> `throw new IllegalArgumentException("Ingest processor [" + key + "] is already registered")`。

### 5.4 `ConfigurationUtils`

`ingest/ConfigurationUtils.java`：

| 方法 | 行号 |
| --- | --- |
| `readOptionalStringProperty` | `:60` |
| `readStringProperty`（含默认值版本） | `:72` / `:83` |
| `readStringOrIntProperty` | `:111` |
| `readOptionalStringOrIntProperty` | `:142` |
| `readBooleanProperty` | `:151` |
| `readIntProperty` | `:178` |
| `readOptionalList` / `readList` | `:197` / `:212` |
| `readMap` / `readOptionalMap` | `:239` / `:254` |
| `readObject` | `:278` |
| `newConfigurationException` | `:287` / `:300` |
| `readProcessorConfigs` | `:307` |
| `compileTemplate` | `:331` ★ 模板编译 |
| `readProcessor` | `:368` / `:383` |

`TAG_KEY = "tag"`（`:50`）—— 每个 processor 都可以有 `tag`，用于统计和 `on_failure` 定位。

---

## 6. `IngestDocument`

`ingest/IngestDocument.java:51`，`INGEST_KEY = "_ingest"`（`:53`）。

### 6.1 字段模型

| 方法 | 行号 | 说明 |
| --- | --- | --- |
| `getFieldValue(path, clazz)` | `:114` | 按**点号路径**取值，类型不符抛异常 |
| `getFieldValue(path, clazz, ignoreMissing)` | `:133` | 可忽略缺失 |
| `getFieldValue(TemplateScript.Factory, clazz)` | `:153` | 路径本身是模板 |
| `getFieldValueAsBytes` | `:166` / `:180` | |
| `hasField(path)` | `:210` / `:221`（`failOutOfRange`） | |
| `removeField(path)` | `:289` / `:298` | |
| `appendFieldValue(path, value)` | `:384` / `:400` | 追加成 list |
| `setFieldValue(path, value)` | `:415` / `:428` | ★ 设置 |
| `renderTemplate(TemplateScript.Factory)` | `:557` | 渲染 mustache 模板 |
| `extractMetadata()` | `:574` | 取出 `_index`/`_type`/`_id`/... |
| `getSourceAndMetadata()` | `:606` | 最终 `_source` |
| `executePipeline(Pipeline)` | `:651` | 供 `PipelineProcessor` 使用 |
| `deepCopyMap` | `:611` | 深拷贝 |

`MetaData` 枚举（`:687`）：`INDEX`、`TYPE`、`ID`、`ROUTING`、`VERSION`、`VERSION_TYPE`。

> **`_ingest` 元数据**：processor 可以往 `_ingest` 下写临时数据（如 `_ingest.timestamp`），
> 这些数据**不会**进入最终 `_source`（因为 `getSourceAndMetadata()` 只返回 source+metadata 部分，
> `ingestMetadata` 是独立的 map）。

### 6.2 模板（mustache）

`ConfigurationUtils.compileTemplate`（`:331`）把 `"{{field}}"` 编译成 `TemplateScript.Factory`，
运行时由 `IngestDocument.renderTemplate`（`:557`）渲染。
模板引擎是 **`lang-mustache` 模块**（也是一个 `ScriptPlugin`）。

```json
{"set": {"field": "full_name", "value": "{{first}} {{last}}"}}
```

---

## 7. 内置 Processor（modules/ingest-common）

`modules/ingest-common/src/main/java/org/elasticsearch/ingest/common/`：

| 分类 | Processor |
| --- | --- |
| 字段增删改 | `set`、`append`、`remove`、`rename`、`dot_expander`、`foreach` |
| 字符串 | `uppercase`、`lowercase`、`trim`、`gsub`、`split`、`join`、`sort`、`url_decode`、(`AbstractStringProcessor` 基类) |
| 类型转换 | `convert`、`bytes` |
| 时间 | `date`、`date_index_name`（`DateFormat`） |
| 结构化解析 | `grok`、`dissect`、`kv`、`json` |
| 控制流 | `fail`、`drop`（core）、`if`/`on_failure`（语法） |
| 脚本 | `script` |

> **性能提示**：`grok` 用正则，**是 CPU 杀手**；
> 能用 `dissect` 就别用 `grok`（dissect 是纯字符串分割，快一个数量级）；
> `json` processor 每次都要解析；`script` processor 每次执行脚本。

---

## 8. Ingest 节点

```java
// node/Node.java:199
Setting.boolSetting("node.ingest", true, Property.NodeScope);

// cluster/node/DiscoveryNode.java:69
public static boolean isIngestNode(Settings settings) { ... }
public boolean isIngestNode() { ... }                       // :322
```

**架构建议**：

- 高吞吐写入场景（日志）→ **独立的 ingest 节点池**（`node.master: false`、`node.data: false`、`node.ingest: true`），
  让 CPU 密集的 pipeline 不影响 data 节点的写入与查询。
- 客户端要连到 ingest 节点（或让协调节点转发）。
- 注意：**ingest 发生在 bulk 到达的节点上**（协调/ingest 节点），
  不是在 data 节点上 —— 所以 pipeline 的 CPU 成本不在 data 节点。

---

## 9. Script 总览

### 9.1 四个核心概念

| 概念 | 类 | 说明 |
| --- | --- | --- |
| **脚本内容** | `Script` | `{ "source": "...", "lang": "painless", "params": {...} }` |
| **来源类型** | `ScriptType` | `INLINE`（内联） / `STORED`（存在集群里） |
| **用途上下文** | `ScriptContext` | `script_score`、`update`、`ingest` 等 |
| **引擎** | `ScriptEngine` | `painless`、`expression`、`mustache`、插件提供 |

`ScriptType`（`script/ScriptType.java:35`）：

```java
INLINE ( 0 , new ParseField("source", "inline") , false ),   // :44
STORED ( 1 , new ParseField("id",     "stored") , false );   // :53
```

### 9.2 `ScriptContext`：脚本的"用途"

`script/ScriptContext.java:57`。`ScriptModule.CORE_CONTEXTS`（`script/ScriptModule.java:40`）列出 6.6.1 的全部内建上下文：

| Context | 用途 |
| --- | --- |
| `SearchScript.CONTEXT` | 查询里 `script` / `script_fields` |
| `SearchScript.AGGS_CONTEXT` | 聚合里的脚本 |
| `ScoreScript.CONTEXT` | `script_score` 自定义打分 |
| `SearchScript.SCRIPT_SORT_CONTEXT` | `_script` 排序 |
| `TermsSetQueryScript.CONTEXT` | `terms_set` 查询 |
| `UpdateScript.CONTEXT` | `_update` 更新脚本 |
| `BucketAggregationScript.CONTEXT` | `bucket_script` 类 |
| `BucketAggregationSelectorScript.CONTEXT` | `bucket_selector` |
| `SignificantTermsHeuristicScoreScript.CONTEXT` | significant_terms 自定义启发 |
| `IngestScript.CONTEXT` | ingest `script` processor |
| `IngestConditionalScript.CONTEXT` | ingest `if` 条件 |
| `FilterScript.CONTEXT` | `script` query |
| `SimilarityScript.CONTEXT` / `SimilarityWeightScript.CONTEXT` | 自定义相似度 |
| `TemplateScript.CONTEXT` | 模板渲染（mustache） |
| `MovingFunctionScript.CONTEXT` | `moving_fn` pipeline |
| `ScriptedMetricAggContexts.{Init,Map,Combine,Reduce}Script.CONTEXT` | `scripted_metric` |

**为什么要有 Context**：

1. **安全边界**：可以用 `script.allowed_contexts` 禁用某类脚本（如禁止 `update`）。
2. **编译产物不同**：同一段源码在不同 context 下编译出的"工厂类"不同（如 `SearchScript.Factory` vs `UpdateScript.Factory`）。
3. **沙箱白名单不同**：Painless 对 `update` 脚本暴露的可访问成员与 `filter` 不同。

### 9.3 `ScriptModule` 装配

`script/ScriptModule.java:68`：

```java
public ScriptModule(Settings settings, List<ScriptPlugin> scriptPlugins) {
    Map<String, ScriptEngine> engines = new HashMap<>();
    Map<String, ScriptContext<?>> contexts = new HashMap<>(CORE_CONTEXTS);
    for (ScriptPlugin plugin : scriptPlugins) {
        for (ScriptContext<?> context : plugin.getContexts()) {
            ScriptContext<?> oldContext = contexts.put(context.name, context);
            if (oldContext != null) {
                throw new IllegalArgumentException("Context name [" + context.name + "] defined twice");   // :75
            }
        }
    }
    for (ScriptPlugin plugin : scriptPlugins) {
        ScriptEngine engine = plugin.getScriptEngine(settings, contexts.values());
        if (engine != null) {
            ScriptEngine existing = engines.put(engine.getType(), engine);
            if (existing != null) {
                throw new IllegalArgumentException("scripting language [" + engine.getType()
                    + "] defined for engine [...] and [...]");                                             // :84
            }
        }
    }
    scriptService = new ScriptService(settings,
        Collections.unmodifiableMap(engines), Collections.unmodifiableMap(contexts));                        // :89
}
```

> **两个"重复定义"检查**：同名 context 和同 lang 的引擎都会启动失败。
> 这是插件冲突的另一个高发点（第 21 章）。

---

## 10. `ScriptService`：编译、缓存、限流

### 10.1 关键设置

`script/ScriptService.java`：

| Setting | 行号 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `script.cache.max_size` | `:99` | **100** | 编译缓存条目数 |
| `script.cache.expire` | `:101` | `0`（不过期） | 缓存过期时间 |
| `script.max_size_in_bytes` | `:103` | 65535 | inline/stored 脚本大小上限（**动态**） |
| `script.max_compilations_rate` | `:105` | **`75/5m`** | ★ 编译限流（**动态**） |
| `script.allowed_types` | `:110` | 空（= 全允许） | 允许的 ScriptType |
| `script.allowed_contexts` | `:112` | 空（= 全允许） | 允许的 Context |

`DISABLE_DYNAMIC_SCRIPTING_SETTING = "script.disable_dynamic"`（`:65`）。

### 10.2 `compile`（`:283`）

```java
if (type == ScriptType.STORED) {
    // * lang and options will both be null when looking up a stored script,
    //   so we must get the source to retrieve them before checking if the context is supported
    // * a stored script must be pulled from the cluster state every time in case
    //   the script has been updated since the last compilation
    StoredScriptSource source = getScriptFromClusterState(id);      // :300 ★ 每次都从 ClusterState 取
    lang    = source.getLang();
    idOrCode = source.getSource();
    options = source.getOptions();
}
...
ScriptEngine scriptEngine = getEngine(lang);                        // :315
if (isTypeEnabled(type) == false)  throw new IllegalArgumentException("cannot execute [" + type + "] scripts");   // :318
if (contexts.containsKey(context.name) == false) throw new IllegalArgumentException("script context [...] not supported");  // :322
if (isContextEnabled(context) == false) throw new IllegalArgumentException("cannot execute scripts using [...] context");  // :326

if (type == ScriptType.INLINE) {
    if (idOrCode.getBytes(UTF_8).length > maxSizeInBytes) {
        throw new IllegalArgumentException("exceeded max allowed inline script size in bytes [...]");   // :331
    }
}

CacheKey cacheKey = new CacheKey(lang, idOrCode, context.name, options);   // :340
Object compiledScript = cache.get(cacheKey);
if (compiledScript != null) return context.factoryClazz.cast(compiledScript);   // :344 命中缓存

// Synchronize so we don't compile scripts many times during multiple shards all compiling a script
synchronized (this) {                                               // :348 ★ 全局锁
    compiledScript = cache.get(cacheKey);                           // 双重检查
    if (compiledScript == null) {
        try {
            checkCompilationLimit();                                // :362 ★ 限流
            compiledScript = scriptEngine.compile(id, idOrCode, context, options);   // :363
        } catch (ScriptException good) {
            throw good;
        } catch (Exception exception) {
            throw new GeneralScriptException("Failed to compile " + type + " script [" + id
                + "] using lang [" + lang + "]", exception);        // :368
        }
        scriptMetrics.onCompilation();
        cache.put(cacheKey, compiledScript);                        // :374
    }
    return context.factoryClazz.cast(compiledScript);
}
```

**三个重点**：

1. **`synchronized (this)`**：编译是**全局串行**的。
   大量不命中缓存的脚本 → **所有脚本编译互相阻塞**。
2. **`CacheKey = (lang, idOrCode, context, options)`**（`:575`）。
   注意 **key 里没有 `params`** —— 所以"同一段脚本 + 不同 params"只编译一次。
   **这就是"参数化脚本"能救你的原因**。
3. **STORED 脚本每次都从 ClusterState 取**（源码注释 `:298`），
   所以更新 stored script 会立刻生效；但也意味着 stored script 每次都要查集群状态。

### 10.3 编译限流：token bucket

`checkCompilationLimit`（`:390`）：

```java
// This is a variant of the token bucket algorithm
long now = System.nanoTime();
long timePassed = now - lastInlineCompileTime;
lastInlineCompileTime = now;

scriptsPerTimeWindow += (timePassed) * compilesAllowedPerNano;

// It's been over the time limit anyway, readjust the bucket to be level
if (scriptsPerTimeWindow > rate.v1()) {
    scriptsPerTimeWindow = rate.v1();                       // 桶满了，多余的丢弃
}

// If there is enough tokens in the bucket, allow the request and decrease the tokens by 1
if (scriptsPerTimeWindow >= 1) {
    scriptsPerTimeWindow -= 1.0;
} else {
    throw new CircuitBreakingException("[script] Too many dynamic script compilations within, max: ["
        + rate.v1() + "/" + rate.v2() +"]; please use indexed, or scripts with parameters instead; "
        + "this limit can be changed by the [" + SCRIPT_MAX_COMPILATIONS_RATE.getKey() + "] setting");   // :407
}
```

> **这是生产上最常见的脚本报错。** 默认 **5 分钟内最多 75 次编译**。
>
> **根因**：脚本里**硬编码了变量**（如日期、ID、阈值），导致每次请求都是"新脚本"：
>
> ```json
> // ❌ 错：每换一天就是新脚本
> {"script": {"source": "doc['price'].value * 1.2 > 100"}}
> // ✅ 对：只有 1 个脚本
> {"script": {"source": "doc['price'].value * params.rate > params.threshold",
>             "params": {"rate": 1.2, "threshold": 100}}}
> ```
>
> **处理**：
> - 临时：`PUT /_cluster/settings {"transient":{"script.max_compilations_rate":"200/1m"}}`（可动态改）。
> - 根治：**参数化脚本**；或用 **stored script**（也要注意 stored 脚本改内容同样会重新编译）。

### 10.4 Stored Script

```java
// ScriptService:430
Map<String, StoredScriptSource> getScriptsFromClusterState() {
    ScriptMetaData scriptMetadata = clusterState.metaData().custom(ScriptMetaData.TYPE);   // :435
    ...
}

// :444
StoredScriptSource getScriptFromClusterState(String id) {
    ...
    throw new ResourceNotFoundException("unable to find script [" + id + "] in cluster state");   // :448
}
```

`putStoredScript`（`:460`）还有大小检查（`:462`）。

> stored script 存在 `ClusterState` 的 `ScriptMetaData`（`MetaData.Custom`）里，
> 所以**改脚本 = 一次集群状态更新**，会全集群广播。别高频写。

---

## 11. Painless 简介

`modules/lang-painless` + `modules/lang-expression` + `modules/lang-mustache`：

| 引擎 | lang 名 | 用途 |
| --- | --- | --- |
| Painless | `painless` | 通用；**默认、最安全、最快** |
| Lucene expressions | `expression` | 只能做数值计算，非常快；不支持 update/ingest context（见 `ScriptService:308-313` 的特判） |
| Mustache | `mustache` | 只做模板渲染（非通用脚本） |

Painless 的特点：

- **AST + ASM 直接生成字节码**，不走 Groovy 那种动态派发，性能好。
- **沙箱白名单**：只允许访问白名单里的类/方法（`PainlessLookup`）。
  这也是为什么 `System.exit()` 之类的调用会被拒绝。
- 6.6.1 里 Painless 的 `ProcessorsWhitelistExtension`（在 `modules/ingest-common` 里）说明
  **白名单可以被插件扩展**。

**排序**：性能上 `expression` > `painless` >> 任何"每文档解释执行"的方案。

---

## 12. 排障手册

### 12.1 常见报错

| 报错 | 原因 | 位置 |
| --- | --- | --- |
| `[script] Too many dynamic script compilations within` | 编译限流；脚本没参数化 | `ScriptService.java:407` |
| `pipeline with id [x] does not exist` | pipeline 未创建 / 名字错 | `IngestService.java:400` |
| `Ingest processor [x] is already registered` | 两个插件注册同名 processor | `IngestService.java:101`附近 |
| `processor [x] not found` | 该节点缺少对应插件 | `ConfigurationUtils.readProcessor:368` |
| `Error updating pipeline with id [x]` | pipeline 配置解析失败 | `IngestService.java:521` |
| `exceeded max allowed inline script size in bytes` | 脚本超过 `script.max_size_in_bytes` | `ScriptService.java:331` |
| `script context [x] not supported` | context 名不存在 | `ScriptService.java:322` |
| `cannot execute scripts using [x] context` | 被 `script.allowed_contexts` 禁用 | `ScriptService.java:326` |
| `cannot execute [stored] scripts` | 被 `script.allowed_types` 禁用 | `ScriptService.java:318` |
| `Failed to compile inline script [x] using lang [painless]` | 语法/白名单错误 | `ScriptService.java:368` |
| `unable to find script [x] in cluster state` | stored script 不存在 | `ScriptService.java:448` |
| `Context name [x] defined twice` | 两个 ScriptPlugin 定义同名 context | `ScriptModule.java:75` |
| `scripting language [x] defined for engine [...] and [...]` | 两个插件注册同 lang 引擎 | `ScriptModule.java:84` |

### 12.2 观测

```bash
# ingest 统计（每个 pipeline / processor 的 count、time、failed）
GET /_nodes/stats/ingest?pretty

# 脚本统计（compilations、cache_evictions、compilation_limit_triggered）
GET /_nodes/stats/script?pretty

# 查看 pipeline
GET /_ingest/pipeline?pretty
GET /_ingest/pipeline/my_pipeline?pretty

# 模拟执行（不写数据）——排查 pipeline 逻辑的神器
POST /_ingest/pipeline/my_pipeline/_simulate
{"docs":[{"_source":{"message":"127.0.0.1 - - [01/Jan/2020] \"GET /\" 200"}}]}

# bulk 响应里的 ingest_took
#   ingest_took 大 → 瓶颈在 pipeline
```

### 12.3 优化清单

**Ingest**：

1. 能用 `dissect` 就不用 `grok`（正则 vs 字符串分割）。
2. 高吞吐场景用**独立 ingest 节点**。
3. pipeline 里避免 `script` processor（改为内建 processor 或 Painless 预编译）。
4. 用 `_simulate` 提前验证，别在生产上试。
5. 需要丢弃数据时用 `drop`，但注意返回空 bulk 的情况（`:585`）。
6. 别高频 PUT pipeline（会重建全部 pipeline）—— `innerUpdatePipelines:509`。
7. 注意 pipeline 会改 `_index`，可能导致路由到意外索引。

**Script**：

1. **参数化脚本**（最重要）—— 直接消除编译限流。
2. 数值计算用 `expression` 引擎（更快，但有 context 限制）。
3. `script_score` 尽量用 `function_score` 内建函数替代。
4. 排序/聚合用脚本 → 考虑改成写入时预计算字段（ingest 里算好）。
5. 调 `script.cache.max_size`（默认 100 偏小，脚本种类多时容易 evict）。
6. `script.max_compilations_rate` 只在应急时调大，**不是根治手段**。
7. 别在脚本里做 IO / 复杂循环 —— 它是**逐文档**执行的。

---

## 13. 动手实验

### 实验 1：pipeline 基础

```bash
# 创建
PUT /_ingest/pipeline/my-pipeline
{
  "description": "add timestamp + parse",
  "processors": [
    {"set": {"field": "_source.ingest_time", "value": "{{_ingest.timestamp}}"}},
    {"convert": {"field": "price", "type": "double", "ignore_missing": true}},
    {"rename": {"field": "msg", "target_field": "message", "ignore_missing": true}}
  ]
}

# 模拟
POST /_ingest/pipeline/my-pipeline/_simulate
{"docs":[{"_source":{"msg":"hello","price":"12.5"}}]}

# 使用
POST /my-index/_doc?pipeline=my-pipeline
{"msg":"hello","price":"12.5"}

# 默认 pipeline
PUT /my-index/_settings {"index.default_pipeline":"my-pipeline"}
```

### 实验 2：on_failure 与 ignore_failure

```bash
PUT /_ingest/pipeline/err
{
  "processors": [
    {"convert": {"field": "num", "type": "integer"}},
    {"fail": {"message": "always fail"}}
  ],
  "on_failure": [
    {"set": {"field": "error", "value": "{{_ingest.on_failure_message}}"}},
    {"set": {"field": "failed_processor", "value": "{{_ingest.on_failure_processor_type}}"}}
  ]
}
POST /_ingest/pipeline/err/_simulate
{"docs":[{"_source":{"num":"abc"}}]}
```

### 实验 3：触发编译限流

```bash
# 连续发 100 个"不同"的内联脚本
for i in $(seq 1 100); do
  curl -s -XPOST "localhost:9200/my-index/_search" -H 'Content-Type: application/json' -d \
    "{\"query\":{\"script\":{\"script\":{\"source\":\"doc['n'].value > $i\"}}}}" > /dev/null
done
# → 第 76 个开始报 [script] Too many dynamic script compilations

# 改成参数化后不再触发
curl -XPOST "localhost:9200/my-index/_search" -H 'Content-Type: application/json' -d '
{"query":{"script":{"script":{"source":"doc['"'"'n'"'"'].value > params.t","params":{"t":1}}}}}'
```

### 实验 4：观察统计

```bash
GET /_nodes/stats/ingest?pretty     # 对比 grok vs dissect 的 time_in_millis
GET /_nodes/stats/script?pretty     # compilations / cache_evictions / compilation_limit_triggered
```

### 实验 5：断点清单

| 断点 | 目的 |
| --- | --- |
| `TransportBulkAction.processBulkIndexIngestRequest`（`:564`） | 看 ingest 在 bulk 里的位置 |
| `IngestService.executeBulkRequest`（`:381`） | 看 WRITE 线程池与逐条处理 |
| `IngestService.innerExecute`（`:472`） | 看 IngestDocument 构造与结果回写 |
| `CompoundProcessor.execute`（`:117`） | 看 null 语义、ignore_failure、on_failure |
| `CompoundProcessor.putFailureMetadata`（`:172`） | 看 `_ingest.on_failure_*` 从哪来 |
| `IngestService.innerUpdatePipelines`（`:498`） | 看 ClusterState → Pipeline 的重建 |
| `IngestService.substitutePipeline`（`:319`） | 看缺插件时的降级行为 |
| `ScriptService.compile`（`:340`） | 看 CacheKey 与双重检查锁 |
| `ScriptService.checkCompilationLimit`（`:390`） | 看 token bucket 限流 |
| `ScriptModule.<init>`（`:68`） | 看引擎与 context 注册 |

### 实验 6：单元测试

```bash
./gradlew :server:test --tests "org.elasticsearch.ingest.*"
./gradlew :modules:ingest-common:test
./gradlew :server:test --tests "org.elasticsearch.script.ScriptServiceTests"
./gradlew :modules:lang-painless:test
```

---

## 14. 自测题（答案都在本章）

1. Pipeline 存在哪里？为什么不能把 `Pipeline` 对象直接放进 ClusterState？
2. 改一条 pipeline 会导致什么？（想想 `innerUpdatePipelines` 的重建逻辑）
3. 某个节点缺少插件时，pipeline 会怎样？表现是什么？
4. Ingest 跑在哪个线程池？为什么这很关键？
5. `NOOP_PIPELINE_NAME` 是什么？为什么要把它设回请求上？
6. `pipeline does not exist` 会让整个 bulk 失败吗？
7. `innerExecute` 里 pipeline 可以修改请求的哪些元数据？`date_index_name` 是怎么工作的？
8. `CompoundProcessor.execute` 返回 null 意味着什么？
9. `ignore_failure` 和 `on_failure` 的行为有什么区别？
10. 有 `on_failure` 时，失败之后还会继续执行后续 processor 吗？
11. `_ingest.on_failure_message` 这些变量是哪里写进去的？什么时候被清理？
12. `ConditionalProcessor`、`PipelineProcessor`、`DropProcessor` 各自干什么？
13. 统计里 processor 的名字是怎么拼出来的？（tag / pipeline 名）
14. 两个插件注册同名 processor 会怎样？
15. 写一个自定义 processor 需要实现什么？怎么注册？
16. `ConfigurationUtils` 是做什么的？`compileTemplate` 用的哪个引擎？
17. `_ingest` 下的临时数据会进入最终 `_source` 吗？
18. 列举 5 个常用的 ingest processor，并指出哪个最耗 CPU。
19. 为什么高吞吐日志场景建议用独立 ingest 节点？
20. Ingest 是在协调节点还是 data 节点执行？
21. `ScriptType` 有哪两种？区别是什么？
22. 什么是 `ScriptContext`？为什么要区分 context？（至少 2 点）
23. 列出 5 个内建 `ScriptContext`。
24. 两个插件注册同名的 context 或同 lang 的引擎会怎样？
25. `script.max_compilations_rate` 的默认值是多少？它用的是什么算法？
26. 为什么"脚本里硬编码变量"会触发编译限流？请用 `CacheKey` 解释。
27. 参数化脚本为什么能解决这个问题？
28. `compile` 里的 `synchronized (this)` 意味着什么？
29. stored script 为什么要"每次都从 ClusterState 取"？代价是什么？
30. `script.cache.max_size` 默认值是多少？脚本种类多时会怎样？
31. `expression` 引擎有什么限制？（看 `ScriptService:308`）
32. Painless 为什么安全？它怎么生成代码？
33. bulk 响应里 `ingest_took` 很大说明什么？怎么定位是哪个 processor 慢？
34. 生产上遇到 `[script] Too many dynamic script compilations` 你会怎么处理？（临时 + 根治）
35. 想把"写入时算好一个字段"替代"查询时用脚本排序"，该怎么组合使用本篇的两个主题？

---

## 15. 关键类速查表

| 类 | 路径 | 关键行 |
| --- | --- | --- |
| `IngestService` | `ingest/IngestService.java` | `NOOP_PIPELINE_NAME:70`、`ctor:83`、**`processorFactories:101`**、`delete:126`、`innerDelete:142`、`getPipelines:176`、`putPipeline:212`、`getPipeline:234`、`info:242`、**`applyClusterState:256`**、`substitutePipeline:319`、`innerPut:338`、**`executeBulkRequest:377`**（WRITE 池 `:381`、pipeline 不存在 `:400`、防重复 `:405`）、`stats:416`、`getProcessorName:434`、**`innerExecute:455`**（IngestDocument `:472`、drop `:473`、回写 `:479-487`）、**`innerUpdatePipelines:498`**（skip `:499`、重建 `:515`、降级 `:518`、rethrow `:528`） |
| `IngestMetadata` | `ingest/IngestMetadata.java` | `TYPE:45`、注释 `:56`、`getPipelines:78`、`fromXContent:100`、`diff:125` |
| `Pipeline` | `ingest/Pipeline.java` | `DESCRIPTION_KEY:39`、`PROCESSORS_KEY:40`、`VERSION_KEY:41`、`ON_FAILURE_KEY:42`、`create:68`、`execute:93`、`getCompoundProcessor:135`、`getMetrics:165` |
| `CompoundProcessor` | `ingest/CompoundProcessor.java` | `ON_FAILURE_MESSAGE_FIELD:40`、`ON_FAILURE_PROCESSOR_TYPE_FIELD:41`、`ON_FAILURE_PROCESSOR_TAG_FIELD:42`、`ignoreFailure:44`、**`execute:117`**（drop `:125`、ignore `:130`、抛 `:136`、on_failure `:138-141`）、**`executeOnFailure:154`**、`putFailureMetadata:172`、`removeFailureMetadata:183`、`newCompoundProcessorException:190` |
| `IngestDocument` | `ingest/IngestDocument.java` | `INGEST_KEY:53`、`ctor:65/92/101`、`getFieldValue:114/133/153`、`getFieldValueAsBytes:166/180`、`hasField:200/210/221`、`removeField:289/298`、`appendFieldValue:384/400`、**`setFieldValue:415/428`**、`renderTemplate:557`、`extractMetadata:574`、`getSourceAndMetadata:606`、`deepCopyMap:611`、`executePipeline:651`、`MetaData:687` |
| `ConfigurationUtils` | `ingest/ConfigurationUtils.java` | `TAG_KEY:50`、`readOptionalStringProperty:60`、`readStringProperty:72/83`、`readStringOrIntProperty:111`、`readOptionalStringOrIntProperty:142`、`readBooleanProperty:151`、`readIntProperty:178`、`readOptionalList:197`、`readList:212`、`readMap:239`、`readOptionalMap:254`、`readObject:278`、`newConfigurationException:287/300`、`readProcessorConfigs:307`、**`compileTemplate:331`**、`readProcessor:368/383` |
| `TransportBulkAction` | `action/bulk/TransportBulkAction.java` | `ingestService:94`、**`processBulkIndexIngestRequest:564`**（调用 `:567`、空响应 `:585`、dropped `:591`）、`BulkRequestModifier:594`、`default pipeline:247-255` |
| `ScriptService` | `script/ScriptService.java` | `DISABLE_DYNAMIC:65`、`MAX_COMPILATION_RATE_FUNCTION:69`、`SCRIPT_CACHE_SIZE_SETTING:99`（100）、`SCRIPT_CACHE_EXPIRE_SETTING:101`、`SCRIPT_MAX_SIZE_IN_BYTES:103`（65535）、**`SCRIPT_MAX_COMPILATIONS_RATE:105`（75/5m）**、`TYPES_ALLOWED_SETTING:110`、`CONTEXTS_ALLOWED_SETTING:112`、`cache:122`、`ctor:135`、`getEngine:242`、**`compile:283`**（stored `:300`、expression 特判 `:308`、类型/context 校验 `:317-327`、大小 `:330`、**`CacheKey:340`**、缓存命中 `:344`、**`synchronized:348`**、**`checkCompilationLimit:362`**、`scriptEngine.compile:363`）、**`checkCompilationLimit:390`**（抛 `:407`）、`isTypeEnabled:418`、`isContextEnabled:422`、`getScriptsFromClusterState:430`、`getScriptFromClusterState:444`、`putStoredScript:460`、`deleteStoredScript:514`、`stats:545`、`applyClusterState:550`、`CacheKey:569/575` |
| `ScriptModule` | `script/ScriptModule.java` | **`CORE_CONTEXTS:40`**（列表 `:42-63`）、`ctor:68`（context 重复 `:75`、engine 重复 `:84`、构造 ScriptService `:89`）、`getScriptService:95`、`registerClusterSettingsListeners:102` |
| `ScriptType` | `script/ScriptType.java` | `INLINE:44`、`STORED:53`、`readFrom:60`、`getName:104`、`getParseField:111` |
| `ScriptContext` | `script/ScriptContext.java` | `class:57` |
| `ScriptEngine` | `script/ScriptEngine.java` | `getType()`、`compile(...)` |
| `DiscoveryNode` | `cluster/node/DiscoveryNode.java` | `isIngestNode(Settings):69`、`isIngestNode():322` |
| `Node` | `node/Node.java` | `node.ingest:199`、`IngestService 构造:385`、`ScriptModule:361` |

---

**上一篇**：[21-插件机制与扩展点.md](./21-插件机制与扩展点.md)
**下一篇**：[23-Snapshot与Gateway持久化.md](./23-Snapshot与Gateway持久化.md)
