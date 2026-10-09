# 20 · 聚合框架 Aggregations

> 目标：把聚合的**四层抽象**（Builder → Factory → Aggregator → InternalAggregation）、
> **bucket ordinal 模型**、**三层归约**、以及 terms 聚合的**近似性根源**讲清楚。
>
> **为什么必须学**：
> - 「terms 聚合结果不准 / 少了几条」→ `shard_size` + `doc_count_error_upper_bound`（分布式 top-N 的固有问题）。
> - 「`too_many_buckets_exception`」→ `search.max_buckets` 熔断（`MultiBucketConsumerService`）。
> - 「聚合把节点打 OOM」→ 分片级桶数 × 分片数 × 子聚合深度，`BigArrays` + 断路器。
> - 「`circuit_breaking_exception`」→ 聚合是 ES 里最吃内存的操作，没有之一。
> - 「为什么加个 `collect_mode: breadth_first` 就好了」→ `BestBucketsDeferringCollector` 延迟收集。
> - 云服务的**报表、监控、BI 场景**全部落在本章；aggs 是最容易把集群打挂的功能。
>
> **前置**：第 15 章（Engine / Lucene）、第 **19 章**（检索流程；聚合是 query 阶段的一部分）。
> **本阶段收尾**：学完这篇，20 篇源码分析系列全部完成。

---

## 1. 一图总览

```
          请求 JSON                                            分片执行                        协调节点归约
  ┌─────────────────────┐                        ┌────────────────────────────┐        ┌──────────────────────┐
  │ AggregationBuilder  │  build(context,parent) │ AggregatorFactory          │        │ InternalAggregation  │
  │  (解析/校验/序列化)   │ ─────────────────────► │  .create(parent, single?)  │        │  .doReduce(list,ctx) │
  └─────────────────────┘                        └────────────┬───────────────┘        └──────────┬───────────┘
                                                              │                                     ▲
                                                 ┌────────────▼───────────────┐                    │
                                                 │ Aggregator（每分片实例）      │  buildAggregation │
                                                 │  preCollection()           │ ────────────────────┘
                                                 │  getLeafCollector(ctx)     │
                                                 │    └ collect(doc, bucketOrd)  ←─ 每命中一个 doc 调一次
                                                 │  postCollection()          │
                                                 │  buildAggregation(0)       │
                                                 └────────────────────────────┘

  AggregationPhase（search/aggregations/AggregationPhase.java）
     preProcess:50   → 建 aggregator 树 + 挂到 queryCollectors
     execute:79      → global 单独跑一轮 → postCollection + buildAggregation → 组装 InternalAggregations
```

---

## 2. 必读源码清单

| 类 | 路径 | 作用 |
| --- | --- | --- |
| `AggregationBuilder` / `AggregatorFactory` / `Aggregator` / `InternalAggregation` | `search/aggregations/` | ★ 四层抽象 |
| `AggregationPhase` | `search/aggregations/AggregationPhase.java` | ★ 聚合生命周期的驱动者（`preProcess:50`、`execute:79`） |
| `AggregatorFactories` | `search/aggregations/AggregatorFactories.java` | 聚合树的工厂容器（`:212` / `:194`） |
| `AggregatorBase` | `search/aggregations/AggregatorBase.java` | Aggregator 模板实现（`getLeafCollector:164`、`postCollection:237`） |
| `BucketsAggregator` | `search/aggregations/bucket/BucketsAggregator.java` | ★ bucket ordinal 记账（`grow:67`、`collectBucket:74`、`bucketDocCount:115`） |
| `LeafBucketCollector` | `search/aggregations/LeafBucketCollector.java` | `collect(doc, bucket)` 抽象（`:78`） |
| `InternalAggregation` | `search/aggregations/InternalAggregation.java` | ★ `reduce:134` / `doReduce:144` / `ReduceContext:56` |
| `MultiBucketConsumerService` | `search/aggregations/MultiBucketConsumerService.java` | `search.max_buckets` 熔断（`:44`、`:98`） |
| `TermsAggregatorFactory` | `search/aggregations/bucket/terms/TermsAggregatorFactory.java` | ★ terms 的执行模式选择（`doCreateInternal:113`） |
| `InternalTerms` | `search/aggregations/bucket/terms/InternalTerms.java` | ★ terms 归约与误差计算（`doReduce:220`） |
| `BucketUtils` | `search/aggregations/bucket/BucketUtils.java` | `shard_size` 启发式（`:38`） |
| `DeferableBucketAggregator` | `search/aggregations/bucket/DeferableBucketAggregator.java` | breadth_first（`shouldDefer:95`、`runDeferredCollections:99`） |
| `BestBucketsDeferringCollector` | `search/aggregations/bucket/BestBucketsDeferringCollector.java` | 延迟收集的"只跑 top 桶"（`prepareSelectedBuckets:142`） |
| `SearchContextAggregations` | `search/aggregations/SearchContextAggregations.java` | 分片级聚合上下文（`:37`） |

---

## 3. 四层抽象

| 层 | 职责 | 生命周期 | 线程 |
| --- | --- | --- | --- |
| `AggregationBuilder` | 解析 JSON、校验、序列化（XContent） | 每个请求 1 个 | 协调节点 + 数据节点（反序列化） |
| `AggregatorFactory` | 依据 `ValuesSourceConfig` / mapping 决定**用哪种实现** | 每个请求每分片 | 数据节点 |
| `Aggregator` | **真正遍历文档、记账** | 每个请求每分片 1 个实例 | 数据节点 search 线程 |
| `InternalAggregation` | 分片内结果 + **可归约（reduce）**的容器 | 每个分片 1 个 → 协调节点归约成 1 个 | 两处 |

**为什么要四层**：`Aggregator` 是"有状态的收集器"，必须在**数据节点、持有 `IndexSearcher`** 的环境里创建；
而 `AggregationBuilder` 只是配置。中间插一个 `AggregatorFactory`，是因为**同一个聚合在不同 mapping 下要走不同实现**
（典型：terms 聚合在 keyword 字段上用 global ordinals，在 numeric 上用 `LongTermsAggregator`，
在 unmapped 字段上用 `NonCollectingAggregator`）。

### 3.1 unmapped 字段的处理（很能说明设计）

`TermsAggregatorFactory.createUnmapped`（`:82-99`）：

```java
final InternalAggregation aggregation = new UnmappedTerms(name, order,
        bucketCountThresholds.getRequiredSize(), bucketCountThresholds.getMinDocCount(), pipelineAggregators, metaData);
return new NonCollectingAggregator(name, context, parent, factories, pipelineAggregators, metaData) {
    {
        // even in the case of an unmapped aggregator, validate the order
        InternalOrder.validate(order, this);
    }
    public InternalAggregation buildEmptyAggregation() { return aggregation; }
};
```

> **关键**：字段不存在时**不是报错、也不是跳过**，而是返回一个 `UnmappedTerms`。
> 这样归约时它和其它 `InternalTerms` 能正常合并（见 `InternalTerms.doReduce:228-233` 对 `UnmappedTerms` 的特判）。
> 这也是"跨类型索引聚合不报错、但多索引同名字段不同类型会报错"的根源 —— 见 §7.3。

---

## 4. `AggregationPhase`：驱动生命周期

### 4.1 `preProcess`（`:50`）

```java
AggregatorFactories factories = context.aggregations().factories();
aggregators = factories.createTopLevelAggregators();                 // :55
for (int i = 0; i < aggregators.length; i++) {
    if (aggregators[i] instanceof GlobalAggregator == false) {
        collectors.add(aggregators[i]);                              // ★ global 聚合不挂进去
    }
}
context.aggregations().aggregators(aggregators);
if (!collectors.isEmpty()) {
    Collector collector = MultiBucketCollector.wrap(collectors);
    ((BucketCollector)collector).preCollection();
    ...
    context.queryCollectors().put(AggregationPhase.class, collector);   // :70
}
```

**这段解释了聚合是"搭便车"跑的**：聚合 collector 被挂进 `queryCollectors`，
由 `QueryPhase` 在 Lucene `search()` 时和 topDocs collector 一起执行 —— **同一次文档遍历，不额外扫一遍**。
（第 19 章 §5.3 的 collector 链。）

### 4.2 `execute`（`:79`）

```java
// ① global 聚合单独跑一轮（它不受主查询约束）
if (!globals.isEmpty()) {
    BucketCollector globalsCollector = MultiBucketCollector.wrap(globals);
    Query query = context.buildFilteredQuery(Queries.newMatchAllQuery());   // :101
    globalsCollector.preCollection();
    context.searcher().search(query, collector);                            // :117 ★ 又一次完整搜索
}

// ② 普通聚合：postCollection + buildAggregation
context.aggregations().resetBucketMultiConsumer();
for (Aggregator aggregator : context.aggregations().aggregators()) {
    aggregator.postCollection();                                            // :129
    aggregations.add(aggregator.buildAggregation(0));                       // :130
}
context.queryResult().aggregations(new InternalAggregations(aggregations));

// ③ pipeline 聚合：只有 sibling 允许在顶层
List<PipelineAggregator> pipelineAggregators = context.aggregations().factories().createPipelineAggregators();
for (PipelineAggregator pipelineAggregator : pipelineAggregators) {
    if (pipelineAggregator instanceof SiblingPipelineAggregator) {
        siblingPipelineAggregators.add((SiblingPipelineAggregator) pipelineAggregator);
    } else {
        throw new AggregationExecutionException("Invalid pipeline aggregation named [...] . " +
            "Only sibling pipeline aggregations are allowed at the top level");               // :143
    }
}
context.queryResult().pipelineAggregators(siblingPipelineAggregators);

// ④ 关掉聚合，避免 scroll 下一页重复计算
context.aggregations(null);                                                  // :154
context.queryCollectors().remove(AggregationPhase.class);
```

> **`global` 聚合的代价**：`:117` 又跑了一次 `search()`（match_all）。
> 大索引上一个 `global` 桶 = **两遍扫描**。

---

## 5. `Aggregator` 的生命周期

### 5.1 五个回调

| 阶段 | 方法 | 时机 | 典型实现 |
| --- | --- | --- | --- |
| 建树 | `AggregatorFactory.create(parent, collectsFromSingleBucket)` | `AggregationPhase.preProcess` | 选实现、建子聚合 |
| 准备 | `preCollection()`（`AggregatorBase:184`） | 搜索开始前 | `doPreCollection()`：建 deferring collector / 分配数组 |
| 收集 | `getLeafCollector(LeafReaderContext)`（`AggregatorBase:164`）→ `collect(doc, bucket)` | **每个命中的文档 × 每个 segment** | 记账到 `bucketOrd` |
| 收尾 | `postCollection()`（`AggregatorBase:237`）→ `doPostCollection()` | 搜索结束后 | 排 topN、跑 deferred 子聚合 |
| 产出 | `buildAggregation(long bucket)` | `AggregationPhase.execute:130` | 生成 `InternalAggregation` |
| 释放 | `close()`（`AggregatorBase:245`）→ `doClose()` | `SearchContext` 释放 | 归还 `BigArrays` |

`AggregatorBase.getLeafCollector`（`:164`）的 final 模板非常关键：

```java
public final LeafBucketCollector getLeafCollector(LeafReaderContext ctx) throws IOException {
    preGetSubLeafCollectors();                                     // :174 钩子
    final LeafBucketCollector sub = collectableSubAggregators.getLeafCollector(ctx);   // 子聚合的 collector
    return getLeafCollector(ctx, sub);                             // 子类实现
}
```

**递归结构**：父聚合的 collector 里包着子聚合的 collector，一次 `collect(doc, bucketOrd)` 调用会
沿着聚合树**递归下发**到所有子聚合。这就是"聚合树越深越慢"的源码原因。

### 5.2 bucket ordinal：`BucketsAggregator`

聚合内部**不用对象表示桶**，而是用一个 **`long` 编号（bucket ord）**。
`BucketsAggregator`（`:39`）负责这套账本：

| 方法 | 行号 | 作用 |
| --- | --- | --- |
| `maxBucketOrd()` | `:60` | 当前已分配的最大 ord |
| `grow(long maxBucketOrd)` | `:67` | 扩容（分配 docCounts / 子聚合空间） |
| `collectBucket(sub, doc, bucketOrd)` | `:74` | **ord 首次出现**时调用（会 grow + 计数） |
| `collectExistingBucket(sub, doc, bucketOrd)` | `:82` | ord 已存在（快路径，不 grow） |
| `incrementBucketDocCount(ord, inc)` | `:107` | 该桶的 doc 数 |
| `bucketDocCount(ord)` | `:115` | 读取 |
| `consumeBucketsAndMaybeBreak(count)` | `:131` | ★ 桶数熔断 |
| `bucketAggregations(bucket)` | `:138` | 收集该桶的子聚合结果 |
| `mergeBuckets(mergeMap, newNumBuckets)` | `:87` | 归并（如 significant_terms 的裁剪） |

**为什么用 long 不用对象**：一个 `terms` 聚合可能有上百万个桶，
对象化会带来巨大的 GC 压力。`long ord` + `BigArrays`（页式数组）让聚合可以**超出 JVM 堆**地扩展，
同时由断路器统一记账。

---

## 6. 三层归约

```
   第 1 层（分片内）  每个 Aggregator.buildAggregation(0) → 1 个 InternalAggregation
        ↓（网络）
   第 2 层（协调节点 partial reduce）
        SearchPhaseController.reducedQueryPhase:429
        → InternalAggregation.reduce(aggs, ctx)   ctx.isFinalReduce() == false
        ↓（分片太多时，先局部归约，控制内存）
   第 3 层（final reduce）
        → InternalAggregation.reduce(aggs, ctx)   ctx.isFinalReduce() == true
        → 应用 pipeline 聚合（InternalAggregation.reduce:136-139）
        → 应用 minDocCount / 裁剪到 requiredSize
```

### 6.1 `InternalAggregation.reduce`（`:134`）

```java
public final InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
    InternalAggregation aggResult = doReduce(aggregations, reduceContext);
    if (reduceContext.isFinalReduce()) {
        for (PipelineAggregator pipelineAggregator : pipelineAggregators) {
            aggResult = pipelineAggregator.reduce(aggResult, reduceContext);     // :138 ★ 只在最后一轮
        }
    }
    return aggResult;
}
```

`ReduceContext`（`:56`）的三个能力：

```java
public boolean isFinalReduce() { return isFinalReduce; }             // :66
public BigArrays bigArrays() { return bigArrays; }                   // :70
public ScriptService scriptService() { return scriptService; }       // :74
public void consumeBucketsAndMaybeBreak(int size) {                  // :82
    multiBucketConsumer.accept(size);
}
```

`isFinalReduce()` 的注释（`:61-65`）值得原文记住：

> 指示当前归约是否为**最终**归约。这决定了 pipeline 聚合是否应该应用、
> `minDocCount` 之类的特性是否应该被考虑。
> **任何可能丢失信息的操作只能在最终归约阶段执行。**

**原因**：partial reduce 的结果还要继续和别的 partial 结果归约，
此时裁剪（`minDocCount`、截断到 `size`）会丢信息 → 最终结果错误。

### 6.2 多轮 reduce（大聚合的内存保护）

`SearchPhaseController.reducedQueryPhase`（`:429`）里 `numReducePhases` 会递增；
当结果过多时，`bufferedTopDocs != null` 的分支会**分批处理**，
每批处理完再与下一批归约（`:500` 附近的 `sortDocs` 调用）。
结果里 `num_reduce_phases` 就是这个计数（`InternalSearchResponse`）。

> **观测**：`profile` 里 `num_reduce_phases > 1` 说明协调节点内存压力大，
> 要么分片太多，要么 `size` / 桶数太大。

---

## 7. Terms 聚合深度剖析（最重要、也最容易出错）

### 7.1 执行模式选择

`TermsAggregatorFactory.doCreateInternal`（`:113`）：

```java
if (collectsFromSingleBucket == false) {
    return asMultiBucketAggregator(this, context, parent);                 // :116  嵌套在别的桶里 → 退化
}
BucketCountThresholds bucketCountThresholds = new BucketCountThresholds(this.bucketCountThresholds);
if (InternalOrder.isKeyOrder(order) == false
        && bucketCountThresholds.getShardSize() == DEFAULT_BUCKET_COUNT_THRESHOLDS.getShardSize()) {
    // 用户没指定 shard_size → 用启发式，避免分布式计数导致排序错误
    bucketCountThresholds.setShardSize(BucketUtils.suggestShardSideQueueSize(
        bucketCountThresholds.getRequiredSize(), context.numberOfShards() == 1));         // :124
}
bucketCountThresholds.ensureValidity();
if (valuesSource instanceof ValuesSource.Bytes) {
    ExecutionMode execution = null;
    if (executionHint != null) execution = ExecutionMode.fromString(executionHint, DEPRECATION_LOGGER);  // :131
    if (valuesSource instanceof ValuesSource.Bytes.WithOrdinals == false) {
        execution = ExecutionMode.MAP;                                     // :135  没有 ordinals 只能 map
    }
    final long maxOrd = getMaxOrd(valuesSource, context.searcher());
    if (execution == null) execution = ExecutionMode.GLOBAL_ORDINALS;      // :139  默认
    ...
```

**6.6.1 只剩两种 `execution_hint`**（`:308-323`）：`map` 和 `global_ordinals`。
`global_ordinals_hash` / `global_ordinals_low_cardinality` 已废弃，都会被映射为 `global_ordinals`。

| 模式 | 适用 | 特点 |
| --- | --- | --- |
| `global_ordinals`（默认） | keyword / 有 doc_values 的 string | 用 Lucene 的**全局 ordinal** 做数组下标，**快且省内存**；代价是构建 global ords（首次较贵，会被缓存） |
| `map` | 无 ordinals（如脚本、某些 fielddata 场景） | 用 `ObjectObjectHashMap`，**慢且吃内存** |

### 7.2 `shard_size`：为什么 terms 结果是近似的

`BucketUtils.suggestShardSideQueueSize`（`:38`）：

```java
if (singleShard) {
    return finalSize;                                     // 单分片不需要多取
}
// Request 50% more buckets on the shards in order to improve accuracy
// as well as a small constant that should help with small values of 'size'
final long shardSampleSize = (long) (finalSize * 1.5 + 10);
return (int) Math.min(Integer.MAX_VALUE, shardSampleSize);
```

**问题本质**：terms 是分布式 top-N。
每个分片只能返回它自己的 top `shard_size`，协调节点再从中取全局 top `size`。
如果某个词在**分片 A 排第 100**（没进 shard_size=65 的队列）、但在**分片 B 排第 1**，
协调节点就看不到它在 A 的存在 → **`doc_count` 偏小，甚至整个词被漏掉**。

**误差指标**：`InternalTerms.doReduce`（`:220`）计算两个东西：

```java
// ① 每个分片自己的误差上界
if (terms.getBuckets().size() < getShardSize() || InternalOrder.isKeyOrder(order)) {
    thisAggDocCountError = 0;                       // :243  桶没满 / 按 key 排序 → 无误差
} else if (InternalOrder.isCountDesc(order)) {
    thisAggDocCountError = terms.getDocCountError() > 0
        ? terms.getDocCountError()
        : terms.getBuckets().get(terms.getBuckets().size() - 1).docCount;   // :252  用最后一名桶的 doc_count
} else {
    thisAggDocCountError = -1;                      // :255  无法估计（如按子聚合排序）
}

// ② 桶级误差：sum of shard errors
bucket.docCountError -= thisAggDocCountError;       // :273
...
b.docCountError += sumDocCountError;                // :290
```

对应响应里两个字段：

| 字段 | 含义 |
| --- | --- |
| `doc_count_error_upper_bound` | 聚合级：所有分片误差上界之和；**`-1` 表示无法估计** |
| `sum_other_doc_count` | 未进入结果的所有词的 doc 数之和 |

**实践建议**：

- 需要精确结果 → **提高 `shard_size`**（如 `size` 的 3~10 倍），或 **减少分片数**（最有效）。
- 判断结果是否可信 → 看 `doc_count_error_upper_bound`；非 0 就要警惕。
- 按 **子聚合排序**（`order: {sub_agg > _count}`）时误差无法估计（`-1`）—— 这是最常见的"结果忽对忽错"。
- `singleShard` 时 `shard_size == size`（`:42-44`），所以**单分片结果永远精确**。

### 7.3 跨索引同名字段类型冲突

`InternalTerms.doReduce:231-239`：

```java
if (referenceTerms != null &&
        !referenceTerms.getClass().equals(terms.getClass()) &&
        !terms.getClass().equals(UnmappedTerms.class)) {
    throw new AggregationExecutionException("Merging/Reducing the aggregations failed when computing the aggregation ["
        + referenceTerms.getName() + "] because the field you gave in the aggregation query existed as two different "
        + "types in two different indices");
}
```

> **经典报错**：`Merging/Reducing the aggregations failed ... existed as two different types in two different indices`。
> 原因：一个字段在 index A 是 `keyword`，在 index B 是 `text`（不可聚合）或 `long`。
> 解决：统一 mapping；或显式指定 `field.keyword`；或只查其中一个索引。

### 7.4 `collect_mode` / `breadth_first`

`TermsAggregatorFactory.subAggCollectionMode`（`:196`）：

```java
if (expectedSize == Integer.MAX_VALUE) {
    return SubAggCollectionMode.DEPTH_FIRST;                 // 要全部桶 → 只能深度优先
}
if (maxOrd == -1 || maxOrd > expectedSize) {
    return SubAggCollectionMode.BREADTH_FIRST;               // ★ 基数大于期望桶数 → 广度优先
}
return SubAggCollectionMode.DEPTH_FIRST;
```

| 模式 | 行为 | 内存 |
| --- | --- | --- |
| `depth_first`（默认） | 边收集边为每个桶跑子聚合 | `桶数 × 子聚合内存`，**可能爆炸** |
| `breadth_first` | 先只算桶的优先级队列，`postCollection` 阶段**只对 top 桶**跑子聚合 | 只与 `size` 相关 |

实现：`DeferableBucketAggregator.doPreCollection`（`:45`）判断是否 `shouldDefer`（`:95`），
若需要则创建 `BestBucketsDeferringCollector`，
`postCollection` 时调 `prepareSelectedBuckets(...)`（`BestBucketsDeferringCollector:142`）+ `runDeferredCollections`（`DeferableBucketAggregator:99`）。

> **排障**：`"aggs 里 terms + 子聚合，桶几百万，直接 OOM"` → 加 `collect_mode: breadth_first`。
> 注意：**`breadth_first` 只对"能先算出 top 桶"的聚合有效**，
> 且 `order` 依赖子聚合时不能用（鸡生蛋问题）。

---

## 8. 桶数熔断：`search.max_buckets`

`MultiBucketConsumerService`（`:41`）：

```java
public static final int DEFAULT_LIMIT_MAX_BUCKETS = -1;                 // :42  默认不限
public static final int SOFT_LIMIT_MAX_BUCKETS = 10000;                 // :43  软限（只打日志）
public static final Setting<Integer> MAX_BUCKET_SETTING =
    Setting.intSetting("search.max_buckets", DEFAULT_LIMIT_MAX_BUCKETS, -1,
        Setting.Property.NodeScope, Setting.Property.Dynamic);          // :44  ★ 可动态更新
```

`MultiBucketConsumer.accept`（`:112`）：

```java
public void accept(int value) {
    count += value;
    if (hasLogged == false && count > SOFT_LIMIT_MAX_BUCKETS) {
        logger.debug("New bucket count [{}] exceeds soft limit of [{}]...", count, SOFT_LIMIT_MAX_BUCKETS);
        hasLogged = true;
    }
    if (limit >= 0 && count > limit) {
        throw new TooManyBucketsException("Trying to create too many buckets. Must be less than or equal to: ["
            + limit + "] but was [" + count + "]. This limit can be set by changing the ["
            + MAX_BUCKET_SETTING.getKey() + "] cluster level setting.", limit);       // :124
    }
}
```

`TooManyBucketsException`（`:58`）的 HTTP 状态是 **503**（`:82`）。

**调用点**：

- 分片侧：`BucketsAggregator.consumeBucketsAndMaybeBreak`（`:131`）→ `context.aggregations().multiBucketConsumer()`
- 归约侧：`InternalAggregation.ReduceContext.consumeBucketsAndMaybeBreak`（`:82`），
  例如 `InternalTerms.doReduce:296/298`

> **注意**：这个计数是**分片级 + 归约级分别统计**的。
> 所以 `search.max_buckets=10000` 并不意味着最终响应最多 1 万个桶 —— 它限制的是**每个分片**和**归约过程**各自的上限。

**云服务建议**：`search.max_buckets` 建议设为一个合理值（如 65536），
否则一条 `date_histogram(1s) × terms(1万值)` 就能把协调节点打挂。

---

## 9. 内存与断路器

### 9.1 `BigArrays`

聚合里所有大数组都用 `BigArrays`（`common/util/BigArrays.java`）分配：
页式（`ByteArray`/`IntArray`/`LongArray`/`ObjectArray`），**按需分页**，避免一次性大对象。

### 9.2 request circuit breaker

`AggregatorBase.addRequestCircuitBreakerBytes`（`:121`）：

```java
protected long addRequestCircuitBreakerBytes(long bytes) {
    return context.bigArrays().adjustBreaker(bytes);      // 向 request breaker 记账
}
```

所有 `Aggregator` 在分配大数组前都应调用它。超出 `indices.breaker.request.limit`（默认 60% heap）
会抛 `CircuitBreakingException`（HTTP 429）。

`AggregatorBase.DEFAULT_WEIGHT`（`:43`）：

```java
public static final int DEFAULT_WEIGHT = 1024 * 5; // 5kb
```

这是聚合器自身的**预估固定开销**，用于断路器记账和 `AggregatorFactories.Builder.mustVisitAllDocs()` 之类的判断。

> **排障**：`"aggs 查询把节点打挂"` 通常是 request breaker 没兜住（如 `fielddata` + 多层嵌套），
> 或者是**协调节点**归约时内存峰值（不在 request breaker 管辖范围，属于 `BigArrays` 直接分配）。

---

## 10. Metrics 聚合

`search/aggregations/metrics/` 下的聚合**不产生桶**，只在一个桶内累计数值。

| 类/包 | 说明 |
| --- | --- |
| `MetricsAggregator` | metrics 聚合基类 |
| `NumericMetricsAggregator.SingleValue` | 产出单个数值（`sum`/`avg`/`min`/`max`/`value_count`） |
| `NumericMetricsAggregator.MultiValue` | 产出多值（`stats`/`extended_stats`/`percentiles`） |
| `avg` / `sum` / `min` / `max` / `valuecount` | 直接从 doc_values 累加，O(n) 且内存 O(1) |
| `cardinality` | **HyperLogLog++**（`HyperLogLogPlusPlus`），近似去重计数，`precision_threshold` 控制精度/内存 |
| `percentiles` | **TDigest**（默认）或 **HDRHistogram**（`hdr` 参数），近似分位数 |
| `mad` | 中位数绝对偏差（`median_absolute_deviation`） |
| `stats` / `weighted_avg` / `geobounds` / `geocentroid` / `scripted` | 其余 |
| `tophits` | 特殊：它其实是"桶内取 top 文档"，走 `TopDocsCollector` |

**要点**：

- `sum`/`avg` 这类是**精确**的（浮点精度除外）。
- `cardinality` / `percentiles` 是**近似**的，误差受参数控制，且**不可跨分片精确合并**（HLL 可合并，TDigest 可合并但结果仍是近似）。
- `tophits` 很贵（每个桶都要维护一个 top-N 队列 + 之后 fetch），慎用在大基数桶下。

---

## 11. Pipeline 聚合

`search/aggregations/pipeline/`：

| 类型 | 包 | 执行位置 |
| --- | --- | --- |
| **Parent**（parent） | `derivative`、`cumulativesum`、`moving_avg`/`movavg`、`movfn`、`bucketselector`、`serialdiff` | **分片上**（在 `buildAggregation` 时对桶序列做计算） |
| **Sibling**（sibling） | `bucketsort`、`bucketmetrics`（`avg_bucket`/`sum_bucket`/...）、`bucketscript` | **协调节点 final reduce**（`InternalAggregation.reduce:138`） |

这也是 §4.2 里那段报错的原因：

> 顶层只允许 sibling pipeline（`AggregationPhase.java:143`）——
> 因为 parent pipeline 必须挂在某个桶聚合"内部"。

**注意**：`bucket_sort`（`bucketsort`）在 6.x 里是 sibling，
意味着它**裁剪桶数和分页发生在协调节点**，所以 `bucket_sort` 的 `truncate`/`size` 不会减少分片侧的工作量。

---

## 12. 其它 bucket 聚合速览

| 聚合 | 包 | 要点 |
| --- | --- | --- |
| `date_histogram` / `histogram` | `bucket/histogram/` | 按 interval 分桶；`min_doc_count`、extended_bounds、`offset`；**空桶也会建**（可用 `min_doc_count` 过滤） |
| `range` / `date_range` / `ip_range` | `bucket/range/` | 用户给定区间 |
| `filters` / `adjacency_matrix` | `bucket/filter/`、`bucket/adjacency/` | 多 filter 桶 |
| `nested` / `reverse_nested` | `bucket/nested/` | 需要 `nested` 类型；内部跑 `NestedDocsQuery` + 子聚合 |
| `significant_terms` / `significant_text` | `bucket/significant/` | 显著性检验；内部用 `mergeBuckets` 裁剪（`BucketsAggregator:87`） |
| `sampler` / `diversified_sampler` | `bucket/sampler/` | 抽样后聚合（大幅提速，结果是近似） |
| `composite` | `bucket/composite/` | ★ 6.5+ **分页聚合**（`after_key`），替代"大 terms + 深分页" |
| `geohash_grid` / `geotile_grid` | `bucket/geogrid/` | 地理网格 |
| `global` | `bucket/global/` | 见 §4.2 —— **会额外跑一次 match_all** |
| `missing` | `bucket/missing/` | `missing` 值的桶 |

### `composite` 聚合为什么重要

传统"取全部 terms 桶"的做法：

- 必须 `size` 设得很大 → 分片侧维护巨大优先队列 → 内存爆炸；
- 或者用 `partition` 分片取（不准确、不好用）。

`composite` 的做法：**按 (source1, source2, ...) 的组合键全量排序，用 `after_key` 游标翻页**。
每页只取 `size` 个，分片侧只需维护 `size × 分片数` 的队列 —— **内存恒定、结果精确**。

> **生产建议**："导出全部聚合结果"一律用 `composite`，不要用大 `size` 的 terms。

---

## 13. 排障手册

### 13.1 观测手段

```bash
# 1) Profile：看每个聚合的耗时
GET /my-index/_search
{"size":0,"profile":true,"aggs":{"by_k":{"terms":{"field":"k.keyword"}}}}
# 响应里 profile.shards[].aggregations[]

# 2) 慢日志
PUT /my-index/_settings {"index.search.slowlog.threshold.query.warn":"2s"}

# 3) 断路器
GET /_nodes/stats/breaker
#   request / fielddata / parent 的 limit / estimated / tripped

# 4) 正在跑的任务
GET /_tasks?detailed=true&actions=*search
```

### 13.2 常见报错 → 源码定位

| 报错 | 原因 | 位置 |
| --- | --- | --- |
| `Trying to create too many buckets` | 超 `search.max_buckets` | `MultiBucketConsumerService.java:124`（503） |
| `CircuitBreakingException` | request/fielddata 断路器 | `AggregatorBase.java:121` |
| `Merging/Reducing the aggregations failed ... two different types` | 跨索引同名字段类型不一致 | `InternalTerms.java:236` |
| `Invalid pipeline aggregation ... Only sibling pipeline aggregations are allowed at the top level` | 顶层放了 parent pipeline | `AggregationPhase.java:143` |
| `Unknown 'execution_hint': [...] , expected any of [map, global_ordinals]` | hint 写错 | `TermsAggregatorFactory.java:321` |
| `terms aggregation cannot be applied to field [...]` | 字段类型不支持 | `TermsAggregatorFactory.java:190` |
| `Could not initialize aggregators` | 建聚合器失败（IO/映射问题） | `AggregationPhase.java:73` |
| `Failed to build aggregation [...]` | `buildAggregation` 抛异常 | `AggregationPhase.java:132` |
| `num_reduce_phases` 很大 | 协调节点多轮归约，内存压力 | `SearchPhaseController.java:429` |

### 13.3 优化清单（按性价比）

1. **减少分片数** —— 对 terms 精度、reduce 内存、网络量都是"一箭三雕"。
2. **`collect_mode: breadth_first`** —— terms + 子聚合且基数高时必用。
3. **用 `composite`** 替代"大 size terms"。
4. **调 `shard_size`** —— 提高精度；同时看 `doc_count_error_upper_bound` 验证。
5. **`sampler` / `diversified_sampler`** —— 允许近似时大幅提速（如"热门词"报表）。
6. **避免 `tophits` 在大量桶下** —— 它是每个桶一个优先队列 + fetch。
7. **`cardinality` 的 `precision_threshold`** —— 默认 3000，超过后精度下降但内存可控；调大 = 更多内存。
8. **避免 `global` 桶** —— 它会额外跑一次 match_all（`:117`）。
9. **`execution_hint: global_ordinals`**（默认）—— 确认 keyword 字段有 doc_values。
10. **设置 `search.max_buckets`** —— 保护集群，别让它无限。
11. **`size: 0`** —— 只要聚合不要 hits，省掉 fetch 阶段（第 19 章）。
12. **别在聚合里用脚本** —— 脚本会退化为 `map` 模式且逐文档执行。

---

## 14. 动手实验

### 实验 1：观察 shard_size 与误差

```bash
PUT /agg_test {"settings":{"number_of_shards":5,"number_of_replicas":0},
  "mappings":{"doc":{"properties":{"k":{"type":"keyword"}}}}}

# 造倾斜数据：让某些词集中在少数分片
POST /agg_test/_doc/_bulk
... 大量文档 ...

GET /agg_test/_search
{"size":0,"aggs":{"by_k":{"terms":{"field":"k","size":10}}}}
# 看 doc_count_error_upper_bound 和 sum_other_doc_count

GET /agg_test/_search
{"size":0,"aggs":{"by_k":{"terms":{"field":"k","size":10,"shard_size":100,"show_term_doc_count_error":true}}}}
# 对比误差变化
```

### 实验 2：execution_hint

```bash
GET /agg_test/_search
{"size":0,"aggs":{"by_k":{"terms":{"field":"k","execution_hint":"map"}}}}
GET /agg_test/_search
{"size":0,"aggs":{"by_k":{"terms":{"field":"k","execution_hint":"global_ordinals"}}}}
# 用 profile 对比耗时
```

### 实验 3：max_buckets 熔断

```bash
PUT /_cluster/settings {"transient":{"search.max_buckets":100}}
GET /agg_test/_search
{"size":0,"aggs":{"by_k":{"terms":{"field":"k","size":1000}}}}
# → 503 TooManyBucketsException
```

### 实验 4：breadth_first 救 OOM

```bash
# 高基数字段 + 子聚合
GET /agg_test/_search
{"size":0,"aggs":{"by_k":{"terms":{"field":"k","size":10},
   "aggs":{"s":{"sum":{"field":"v"}}}}}}

GET /agg_test/_search
{"size":0,"aggs":{"by_k":{"terms":{"field":"k","size":10,"collect_mode":"breadth_first"},
   "aggs":{"s":{"sum":{"field":"v"}}}}}}
# 对比 profile 里的时间与断路器用量
```

### 实验 5：composite 分页

```bash
GET /agg_test/_search
{"size":0,"aggs":{"c":{"composite":{"size":2,"sources":[{"k":{"terms":{"field":"k"}}}]}}}}
# 用返回的 after_key 翻下一页
GET /agg_test/_search
{"size":0,"aggs":{"c":{"composite":{"size":2,"after":{"k":"xx"},"sources":[{"k":{"terms":{"field":"k"}}}]}}}}
```

### 实验 6：断点清单

| 断点 | 目的 |
| --- | --- |
| `AggregationPhase.preProcess`（`:50`） | 看聚合树怎么建、怎么挂到 queryCollectors |
| `AggregatorBase.getLeafCollector`（`:164`） | 看父子聚合 collector 的递归 |
| `BucketsAggregator.collectBucket`（`:74`） | 看 bucketOrd 是怎么增长和记账的 |
| `BucketsAggregator.consumeBucketsAndMaybeBreak`（`:131`） | 看熔断 |
| `AggregationPhase.execute`（`:129`） | 看 postCollection + buildAggregation |
| `InternalAggregation.reduce`（`:134`） | 看 pipeline 只在 final reduce 执行 |
| `InternalTerms.doReduce`（`:220`） | 看误差计算 |
| `MultiBucketConsumer.accept`（`:112`） | 看 max_buckets 熔断 |
| `BestBucketsDeferringCollector.prepareSelectedBuckets`（`:142`） | 看 breadth_first |

### 实验 7：单元测试

```bash
./gradlew :server:test --tests "org.elasticsearch.search.aggregations.bucket.terms.TermsAggregatorTests"
./gradlew :server:test --tests "org.elasticsearch.search.aggregations.AggregationsIntegrationIT"
./gradlew :server:test --tests "org.elasticsearch.search.aggregations.metrics.*"
./gradlew :server:test --tests "org.elasticsearch.search.aggregations.MetaDataIT"
```

---

## 15. 自测题（答案都在本章）

1. 聚合的四层抽象分别是什么？各自在哪里创建、在哪里使用？
2. 为什么需要 `AggregatorFactory` 这一层？举个"同一聚合不同实现"的例子。
3. 字段未映射（unmapped）时 terms 聚合会怎样？为什么要这样设计？
4. 聚合是在 query 阶段执行的吗？它是怎么"搭便车"复用文档遍历的？
5. `global` 聚合为什么要单独跑一次 `search()`？代价是什么？
6. `Aggregator` 的生命周期有哪几个回调？分别在什么时候被调用？
7. 什么叫 bucket ordinal？为什么用 `long` 而不是对象表示桶？
8. `collectBucket` 和 `collectExistingBucket` 的区别是什么？
9. 聚合的"三层归约"分别发生在哪里？
10. `ReduceContext.isFinalReduce()` 有什么用？为什么 `minDocCount` 只能在 final reduce 应用？
11. pipeline 聚合为什么只在 final reduce 执行？
12. `num_reduce_phases > 1` 说明什么？
13. `search.max_buckets` 的默认值是多少？软限是多少？超限后 HTTP 状态码是什么？
14. 这个计数是全局的还是分片级的？设置 10000 是否意味着最终响应最多 1 万个桶？
15. `shard_size` 的启发式公式是什么？单分片时呢？
16. 为什么分布式 terms 聚合是近似的？`doc_count_error_upper_bound` 怎么算？
17. 什么时候 `doc_count_error_upper_bound` 会是 `-1`？
18. `sum_other_doc_count` 是什么意思？
19. 想让 terms 结果更精确，有哪些办法？最有效的是哪个？
20. `execution_hint` 有哪两种？各适用什么场景？6.6 里哪些 hint 已废弃？
21. `depth_first` 与 `breadth_first` 的区别？什么时候该用 breadth_first？有什么限制？
22. `subAggCollectionMode` 的自动判定规则是什么？
23. 跨索引聚合时报 "two different types" 是怎么回事？怎么解决？
24. `cardinality` 和 `percentiles` 分别是用什么算法？精确吗？
25. parent pipeline 和 sibling pipeline 分别在哪里执行？各举 2 个例子。
26. 为什么顶层只允许 sibling pipeline？
27. `composite` 聚合解决了什么问题？相比"大 size terms"好在哪？
28. `bucket_sort` 是 parent 还是 sibling？这意味着什么？
29. 聚合为什么会触发 `CircuitBreakingException`？`DEFAULT_WEIGHT` 是什么？
30. 一条聚合查询把集群打挂了，你会按什么顺序排查和优化？（至少 7 步）

---

## 16. 关键类速查表

| 类 | 路径 | 关键行 |
| --- | --- | --- |
| `AggregationPhase` | `search/aggregations/AggregationPhase.java` | **`preProcess:50`**（global 排除 `:57`、挂 collector `:70`）、**`execute:79`**（global 单独 search `:117`、postCollection `:129`、buildAggregation `:130`、pipeline 校验 `:143`、清理 `:154`） |
| `AggregatorFactories` | `search/aggregations/AggregatorFactories.java` | `VALID_AGG_NAME:55`、`createPipelineAggregators:182`、`createSubAggregators:194`、`createTopLevelAggregators:212`、`Builder:243`、`mustVisitAllDocs:284` |
| `Aggregator` | `search/aggregations/Aggregator.java` | `class:40`、`Parser:48`、`name:77`、`buildAggregation:97`、`buildEmptyAggregation:102`、`SubAggCollectionMode:105` |
| `AggregatorBase` | `search/aggregations/AggregatorBase.java` | `DEFAULT_WEIGHT:43`、`addRequestCircuitBreakerBytes:121`、`getLeafCollector 模板:164`、`preGetSubLeafCollectors:174`、`preCollection:184`、`postCollection:237`、`close:245`、`doPostCollection:259` |
| `BucketCollector` | `search/aggregations/BucketCollector.java` | `NO_OP_COLLECTOR:33`、`getLeafCollector:54`、`preCollection:59`、`postCollection:64` |
| `LeafBucketCollector` | `search/aggregations/LeafBucketCollector.java` | `NO_OP_COLLECTOR:34`、`wrap:45`、`collect(doc,bucket):78`、`collect(doc):81` |
| `BucketsAggregator` | `search/aggregations/bucket/BucketsAggregator.java` | `maxBucketOrd:60`、`grow:67`、`collectBucket:74`、`collectExistingBucket:82`、`mergeBuckets:87`、`incrementBucketDocCount:107`、`bucketDocCount:115`、`consumeBucketsAndMaybeBreak:131`、`bucketAggregations:138` |
| `InternalAggregation` | `search/aggregations/InternalAggregation.java` | `ReduceContext:56`、`isFinalReduce:66`、`consumeBucketsAndMaybeBreak:82`、**`reduce:134`**、**`doReduce:144`**、`isMapped:150`、`getProperty:161` |
| `InternalAggregations` | `search/aggregations/InternalAggregations.java` | 归约集合，`reduce(...)` |
| `MultiBucketConsumerService` | `search/aggregations/MultiBucketConsumerService.java` | `DEFAULT_LIMIT:42`、`SOFT_LIMIT:43`、`MAX_BUCKET_SETTING:44`、`TooManyBucketsException:58`（status 503 `:82`）、`MultiBucketConsumer:98`、`accept:112` |
| `SearchContextAggregations` | `search/aggregations/SearchContextAggregations.java` | `factories:42`、`aggregators:46/55`、`multiBucketConsumer:63` |
| `TermsAggregator` | `search/aggregations/bucket/terms/TermsAggregator.java` | `BucketCountThresholds:57`、`ensureValidity:93`、`getShardSize:138`、`descendsFromNestedAggregator:216` |
| `TermsAggregatorFactory` | `search/aggregations/bucket/terms/TermsAggregatorFactory.java` | `createUnmapped:82`、`doCreateInternal:113`（shard_size 启发式 `:124`）、`subAggCollectionMode:196`、`getMaxOrd:212`、`ExecutionMode:221`（`MAP:223`、`GLOBAL_ORDINALS:244`、`fromString:308`） |
| `InternalTerms` | `search/aggregations/bucket/terms/InternalTerms.java` | `Bucket:50`、`doReduce:220`（类型冲突 `:236`、误差 `:242-290`、裁剪 `:283`） |
| `BucketUtils` | `search/aggregations/bucket/BucketUtils.java` | **`suggestShardSideQueueSize:38`** |
| `DeferableBucketAggregator` | `search/aggregations/bucket/DeferableBucketAggregator.java` | `doPreCollection:45`、`getDeferringCollector:76`、`shouldDefer:95`、`runDeferredCollections:99` |
| `BestBucketsDeferringCollector` | `search/aggregations/bucket/BestBucketsDeferringCollector.java` | `class:49`、`getLeafCollector:107`、**`prepareSelectedBuckets:142`**、`wrap:200` |

---

## 17. 阶段收尾：读取期（19-20 篇）学完之后

到这里，ES 6.6.1 的**写入期 + 读取期 + 集群管理**主链路已经全部打通：

| 阶段 | 篇章 | 你现在应该能回答 |
| --- | --- | --- |
| 模块骨架 | 01 | 每个 module / plugin 干什么的 |
| 启动与生命周期 | 02 | 节点怎么起、服务怎么编排 |
| 插件与 SPI | 03 | 扩展点在哪、怎么写插件 |
| HTTP / REST | 04 | REST 请求怎么到 Transport |
| Transport | 05 | 节点间通信、连接管理 |
| 线程池 | 06 | 各线程池职责、被拒绝意味着什么 |
| Action 框架 | 07 | Action 是怎么注册和执行的 |
| 线程池实践 | 08 | 拒绝/堆积怎么定位 |
| Index / Shard 模型 | 09 | `IndexService` / `IndexShard` 的层次 |
| Mapping | 10 | 映射怎么解析、怎么影响写入与查询 |
| 分片分配 | 11 | master 怎么决定分片放哪 |
| 集群状态与选主 | 12 | ClusterState 怎么传播、怎么选主 |
| 写入链路 | 13 | 一次 index 请求走完哪些阶段 |
| Translog | 14 | 持久化的边界在哪 |
| Engine / Lucene | 15 | refresh / flush / segment 的真相 |
| Refresh / Flush / Merge | 16 | 为什么写入可见有延迟、段为什么越合越大 |
| 复制与 SeqNo | 17 | 数据为什么不丢、副本为什么落后 |
| 恢复 | 18 | 分片怎么从 INITIALIZING 到 STARTED |
| 检索 | 19 | query / fetch / DFS 各在干什么 |
| 聚合 | 20 | 桶、ordinal、归约、误差与熔断 |

**推荐的下一步**：

1. **读测试**：`ReplicationTrackerTests`、`IndexRecoveryTests`、`SearchPhaseControllerTests`、`TermsAggregatorTests`
   —— ES 的单元测试质量极高，是理解边界条件最快的材料。
2. **跑起来调试**：`./gradlew :server:test --tests ...` 或单机起节点打断点。
3. **对照官方 issue / PR**：遇到线上问题时，用本文的行号去 GitHub 上找对应的改动历史。
4. **读 x-pack**：`x-pack/` 下的 Security / Monitoring / SQL 是很好的"如何扩展 ES"的范例。

---

**上一篇**：[19-检索流程-QueryThenFetch与DFS.md](./19-检索流程-QueryThenFetch与DFS.md)
**下一篇**：[21-插件机制与扩展点.md](./21-插件机制与扩展点.md)（进入阶段 5：生态期）
