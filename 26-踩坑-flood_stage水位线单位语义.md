# flood_stage 水位线：百分比与字节数的语义相反

> 源码基于 Elasticsearch 6.6.1。

## 结论

`cluster.routing.allocation.disk.watermark.flood_stage` 默认 `95%`：

| 写法 | 触发条件 | 危险方向 |
| --- | --- | --- |
| `95%` | 已用 / 总容量 > 95%（剩余不足 5%） | 值越**大**越危险 |
| `10gb` | 剩余可用空间 < 10GB | 值越**小**越危险 |

即：**百分比是"已用比例"，字节数是"剩余空间下限"**。官方文档承认这很别扭：

```82:85:docs/reference/modules/cluster/disk_allocator.asciidoc
NOTE: Percentage values refer to used disk space, while byte values refer to
free disk space. This can be confusing, since it flips the meaning of high and
low. For example, it makes sense to set the low watermark to 10gb and the high
watermark to 5gb, but not the other way around.
```

触发后动作一致：给该节点上所有分片所属的索引加 `index.blocks.read_only_allow_delete=true`（索引级，非分片级；禁写但允许删除；6.6.1 不自动解除）。

## 源码：一个配置值拆成两套阈值

赋值时同一个字符串被同时解析为百分比和字节，各存一份：

```233:239:server/src/main/java/org/elasticsearch/cluster/routing/allocation/DiskThresholdSettings.java
    private void setFloodStageRaw(String floodStageRaw) {
        // Watermark is expressed in terms of used data, but we need "free" data watermark
        this.floodStageRaw = floodStageRaw;
        this.freeDiskThresholdFloodStage = 100.0 - thresholdPercentageFromWatermark(floodStageRaw);
        this.freeBytesThresholdFloodStage = thresholdBytesFromWatermark(floodStageRaw,
            CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey());
    }
```

百分比做了 `100.0 - x` 翻转（已用 → 空闲），字节原样存。这就是语义差异的根源。

解析失败不抛异常，而是返回兜底值：百分比失败 → `100.0`（阈值变 `0.0`），字节失败 → `0b`：

```311:322:server/src/main/java/org/elasticsearch/cluster/routing/allocation/DiskThresholdSettings.java
    private static double thresholdPercentageFromWatermark(String watermark, boolean lenient) {
        try {
            return RatioValue.parseRatioValue(watermark).getAsPercent();
        } catch (ElasticsearchParseException ex) {
            // NOTE: this is not end-user leniency, since up above we check that it's a valid byte or percentage, and then store the two
            // cases separately
            if (lenient) {
                return 100.0;
            }
            throw ex;
        }
    }
```

```341:352:server/src/main/java/org/elasticsearch/cluster/routing/allocation/DiskThresholdSettings.java
    private static ByteSizeValue thresholdBytesFromWatermark(String watermark, String settingName, boolean lenient) {
        try {
            return ByteSizeValue.parseBytesSizeValue(watermark, settingName);
        } catch (ElasticsearchParseException ex) {
            if (lenient) {
                return ByteSizeValue.parseBytesSizeValue("0b", settingName);
            }
            throw ex;
        }
    }
```

因此两套阈值天然互斥：配 `10gb` 时"空闲百分比 < 0"恒假，配 `95%` 时"freeBytes < 0"恒假。单测对此有直接断言：

```67:68:server/src/test/java/org/elasticsearch/cluster/routing/allocation/DiskThresholdSettingsTests.java
        assertEquals(ByteSizeValue.parseBytesSizeValue("250mb", "test"), diskThresholdSettings.getFreeBytesThresholdFloodStage());
        assertEquals(0.0D, diskThresholdSettings.getFreeDiskThresholdFloodStage(), 0.0D);
```

## 判定与数据来源

```116:117:server/src/main/java/org/elasticsearch/cluster/routing/allocation/DiskThresholdMonitor.java
                if (usage.getFreeBytes() < diskThresholdSettings.getFreeBytesThresholdFloodStage().getBytes() ||
                    usage.getFreeDiskAsPercentage() < diskThresholdSettings.getFreeDiskThresholdFloodStage()) {
```

两个条件是"或"，但只有配置对应的那条可能为真；均为严格小于，等于阈值不触发。

百分比的分母是**数据路径所在文件系统的总容量**（非索引占用、非数据目录大小）：

```105:112:server/src/main/java/org/elasticsearch/cluster/DiskUsage.java
    public double getFreeDiskAsPercentage() {
        if (totalBytes == 0) {
            return 100.0;
        }
        return 100.0 * ((double)freeBytes / totalBytes);
    }
```

`freeBytes` 取自节点**所有数据路径中可用空间最小的那条**的 `available`（非 `free`，即扣除 root 保留块后的可用量，对应 `df -h` 的 Avail 列）：

```391:402:server/src/main/java/org/elasticsearch/cluster/InternalClusterInfoService.java
                for (FsInfo.Path info : nodeStats.getFs()) {
                    if (leastAvailablePath == null) {
                        assert mostAvailablePath == null;
                        mostAvailablePath = leastAvailablePath = info;
                    } else if (leastAvailablePath.getAvailable().getBytes() > info.getAvailable().getBytes()){
                        leastAvailablePath = info;
```

```417:418:server/src/main/java/org/elasticsearch/cluster/InternalClusterInfoService.java
                    newLeastAvaiableUsages.put(nodeId, new DiskUsage(nodeId, nodeName, leastAvailablePath.getPath(),
                        leastAvailablePath.getTotal().getBytes(), leastAvailablePath.getAvailable().getBytes()));
```

多路径节点按最差的盘判定，一块满即整节点触发。

## 校验：排序方向反转

三个水位必须同为百分比或同为字节，混用报错 `unable to consistently parse ... as percentage or bytes`：

```178:202:server/src/main/java/org/elasticsearch/cluster/routing/allocation/DiskThresholdSettings.java
        if (lowWatermarkThreshold > highWatermarkThreshold) { ... }
        if (highWatermarkThreshold > floodThreshold) { ... }
...
        if (lowWatermarkBytes.getBytes() < highWatermarkBytes.getBytes()) { ... }
        if (highWatermarkBytes.getBytes() < floodStageBytes.getBytes()) { ... }
```

百分比要求 `low ≤ high ≤ flood_stage`，字节要求 `low ≥ high ≥ flood_stage`（如 `100gb / 50gb / 10gb`）。ES 不读节点磁盘容量，只有统一成"空闲空间"语义才能做一次方向无关的排序校验。

## 换算

设总容量 `total`，可用 `free`：

- 百分比 `P%`：`100 × free/total < 100 - P` ⟺ `used/total > P%`，触发时剩余 `(100-P)% × total`
- 字节 `B`：`free < B`，触发时使用率 `1 - B/total`

| 磁盘容量 | `95%` 触发时剩余 | `10gb` 触发时使用率 |
| --- | --- | --- |
| 500 GB | 25 GB | 98.0% |
| 2 TB | 100 GB | 99.5% |
| 8 TB | 400 GB | 99.88% |

同一配置在异构集群里表现悬殊：8TB 盘上 `95%` 会在剩 400GB 时锁索引（看起来"过早"），而 `10gb` 要到 99.88% 才有动作（等于放弃保护）。

## 易错点

- `10gb` 不是"用了 10GB 就锁"，是"剩不到 10GB 才锁"；`95%` 不是"剩 95%"。
- 日志打印的是阈值本身 `[10gb] exceeded`，容易被读成"用了 10GB"：

```71:73:server/src/main/java/org/elasticsearch/cluster/routing/allocation/DiskThresholdMonitor.java
        if (usage.getFreeBytes() < diskThresholdSettings.getFreeBytesThresholdFloodStage().getBytes()) {
            logger.warn("flood stage disk watermark [{}] exceeded on {}, all indices on this node will be marked read-only",
                diskThresholdSettings.getFreeBytesThresholdFloodStage(), usage);
```

- 加的是索引级块，影响该索引全部分片。
- flood stage 不参与分片分配决策（`DiskThresholdDecider` 中无此逻辑），搬分片由 high 水位负责。
- 6.6.1 无自动解块逻辑。

## 排查处置

```bash
GET _cluster/settings?include_defaults=true&filter_path=*.disk.watermark*
GET _cat/allocation?v          # disk.avail / disk.total
GET _nodes/stats/fs            # 每 path 的 available，取最小者比对阈值
```

master 日志搜 `flood stage disk watermark [...] exceeded ... marked read-only` 确认触发。

解除前**必须先腾空间**：`DiskThresholdMonitor` 按 `cluster.info.update.interval`（默认 30s）持续巡检，条件仍满足就会重新加块（已有块的索引被 `removeIf` 过滤，不重复刷但也不会自动摘除）。

```bash
PUT */_settings
{
  "index.blocks.read_only_allow_delete": null
}
```

## 配置建议

- 磁盘规格统一：用百分比，默认 `85% / 90% / 95%` 即可。
- 磁盘规格混杂：用字节，按"至少保留多少空闲"取值，下限覆盖单分片恢复 + 段合并临时空间 + 余量，不要用 `10gb` 这类过小值；并把 `cluster.info.update.interval` 适当调大。
- 三个水位单位必须统一，改动时一起提交。
- 记录配置说明时把字节值写成"至少保留 10GB 空闲"，别写成"10GB 触发"。

## 版本差异

较新版本为百分比水位补充了绝对空间上限类参数（`flood_stage.max_headroom`），并在磁盘回落至 high 水位以下时自动释放写入块。6.6.1 均不具备——只加块，不摘块。跨版本运维需按对应版本源码复核。
