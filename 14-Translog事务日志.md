# 14 · Translog 事务日志

> 目标：搞懂 **Translog 是什么、为什么必须有它、它和 Lucene commit 的关系**，
> 以及它是怎样同时承担「崩溃恢复」和「实时 GET/近实时检索」两件事的。
>
> **为什么必须学**：
> - Translog 是 ES **唯一**的「写入持久化凭证」——Lucene 只有 commit 才 fsync，而 commit 很贵。
> - 一个索引最占磁盘的东西，往往不是 Lucene segment，而是**没被 flush 掉的 translog**。
> - `index.translog.*` 的每一个设置都直接对应「写延迟」与「宕机丢数据量」的权衡。
> - 第 15 章（Engine）、18 章（Recovery）、17 章（SeqNo）全部建立在 Translog 之上。
>
> **前置**：第 13 章（写入链路）、第 12 章（索引）。
> **后续**：第 15 章 `15-存储引擎-InternalEngine与Lucene集成.md`。

---

## 1. 一句话理解 Translog

> **Lucene 是「批量的、昂贵的、按段提交的」存储；Translog 是「逐条的、廉价的、每次请求就落盘的」日志。**
> 写入时同时写两者；宕机重启时用 Translog 把 Lucene 缺失的那部分补回来。

```
写请求 → ┌────────────────────┐
        │ 1. Lucene IndexWriter │ ← 只写内存 buffer，不 fsync（快）
        │    (in-memory buffer) │
        └────────────────────┘
        │ 2. Translog.add()     │ ← 顺序追加到 translog-N.tlog（可选 fsync）
        └────────────────────┘
              ↓ (周期性 / 手动 flush)
        ┌────────────────────┐
        │ Lucene commit        │ ← 真正 fsync 段文件；之后 translog 才能被清理
        └────────────────────┘
```

- **Lucene 侧**：`IndexWriter.addDocument` 只写内存 buffer，`refresh` 生成可搜索段，
  `commit` 才把段持久化到磁盘（fsync）。
- **Translog 侧**：每条写操作**立即**追加到 translog 文件；`durability=request` 时每次都 fsync。
- **恢复时**：找到最后一个 Lucene commit（`segments_N`），把 translog 中**该 commit 之后**的操作重放一遍。

---

## 2. 全景：文件结构与命名

```
data/nodes/0/indices/<uuid>/<shard>/translog/
├── translog.ckp            ← 当前 checkpoint（元数据：offset/numOps/generation/seqno/globalCP/...）
├── translog-1.tlog         ← generation=1 的日志（可能已被 trim 删除）
├── translog-1.ckp          ← generation=1 的「已固化」checkpoint
├── translog-2.tlog
├── translog-2.ckp
├── ...
└── translog-5.tlog         ← 当前正在写的 generation（current writer）
```

关键常量（`index/translog/Translog.java`）：

| 常量 | 值 | 行号 |
| --- | --- | --- |
| `TRANSLOG_FILE_PREFIX` | `"translog-"` | `:112` |
| `TRANSLOG_FILE_SUFFIX` | `".tlog"` | `:113` |
| `CHECKPOINT_SUFFIX` | `".ckp"` | `:114` |
| `CHECKPOINT_FILE_NAME` | `"translog.ckp"` | `:115` |
| `TRANSLOG_GENERATION_KEY` | `"translog_generation"` | `:110` |
| `TRANSLOG_UUID_KEY` | `"translog_uuid"` | `:111` |

**★ `translogUUID` 的作用**：它是分片的「身份标识」，写在**每个 tlog 文件的 header** 里，
也写在 Lucene commit 的 `userData` 里。恢复时如果发现 tlog 的 uuid 与 Lucene commit 里的不一致，
说明文件被误拷/误删，直接报 `TranslogCorruptedException`。
这也是**为什么不能手工拷贝单个 shard 目录**的根本原因。

### 2.1 文件命名规则

- `translog-<generation>.tlog`：第 generation 代日志（`getFilename:745`）。
- `translog.ckp`：当前 checkpoint（`Checkpoint.write` 目标，`TranslogWriter:401`）。
- `translog-<generation>.ckp`：roll 时把 `translog.ckp` 复制一份固化下来（`getCommitCheckpointFileName:749`）。

> **注意**：`translog-N.ckp` 的存在表示「第 N 代已经被固化，可以安全地在 flush 后删除」。

---

## 3. ★ `Translog` 类结构

```java
public class Translog extends AbstractIndexShardComponent implements IndexShardComponent, Closeable {   // :98
    // 已关闭的 generations（按 generation 升序，读操作要同时读 readers + current）
    private final List<TranslogReader> readers = new ArrayList<>();        // :121
    private BigArrays bigArrays;                                          // :122
    protected final ReleasableLock readLock;                              // :123  读锁
    protected final ReleasableLock writeLock;                             // :124  写锁（roll/trim 用它）
    private final Path location;                                          // :125
    private TranslogWriter current;                                       // :126  ★ 当前正在写的那一代
    protected final TragicExceptionHolder tragedy = new TragicExceptionHolder();  // :128 致命异常
    private final AtomicBoolean closed = new AtomicBoolean();              // :129
    private final TranslogConfig config;                                  // :130
    private final LongSupplier globalCheckpointSupplier;                  // :131  全局检查点来源（来自 IndexShard）
    private final LongSupplier primaryTermSupplier;                       // :132  primary term 来源
    private final String translogUUID;                                    // :133
    private final TranslogDeletionPolicy deletionPolicy;                  // :134  ★ 删除策略
}
```

**★★ 一个 Translog = 一个 `current` writer + 一组只读 `readers`**。

- 新写入总是进 `current`。
- `rollGeneration()` 把 `current` 转成 `TranslogReader` 加入 `readers`，再新建一个 `current`。
- 读（snapshot/recovery）要**同时遍历 `readers` + `current`**。

**锁模型**：

| 锁 | 类型 | 保护 |
| --- | --- | --- |
| `readLock` | 读锁 | `add`、`sync`、`ensureSynced`、`newSnapshot` |
| `writeLock` | 写锁 | `rollGeneration`、`trimUnreferencedReaders`、`trimOperations` |

因为 `ReentrantReadWriteLock`，**roll/trim 会阻塞所有写**——这是「translog 文件特别多时写入卡顿」的原因之一。

---

## 4. ★★ `add`：写入一条操作（`:524`）

```java
public Location add(final Operation operation) throws IOException {
    final ReleasableBytesStreamOutput out = new ReleasableBytesStreamOutput(bigArrays);
    try {
        final long start = out.position();
        out.skip(Integer.BYTES);                                        // 预留 4 字节放 size
        writeOperationNoSize(new BufferedChecksumStreamOutput(out), operation);   // 序列化 + 计算 checksum
        final long end = out.position();
        final int operationSize = (int) (end - Integer.BYTES - start);
        out.seek(start);
        out.writeInt(operationSize);                                    // 回填 size
        out.seek(end);
        final ReleasablePagedBytesReference bytes = out.bytes();
        try (ReleasableLock ignored = readLock.acquire()) {             // ★ 读锁
            ensureOpen();
            if (operation.primaryTerm() > current.getPrimaryTerm()) {   // ★★ 拒绝「未来 term」的操作
                assert false : "Operation term is newer than the current term; ...";
                throw new IllegalArgumentException("Operation term is newer than the current term; ...");
            }
            return current.add(bytes, operation.seqNo());               // 追加到 current writer，返回 Location
        }
    } catch (final AlreadyClosedException | IOException ex) {
        closeOnTragicEvent(ex);
        throw ex;
    } catch (final Exception ex) {
        closeOnTragicEvent(ex);
        throw new TranslogException(shardId, "Failed to write operation [" + operation + "]", ex);
    } finally {
        Releasables.close(out);
    }
}
```

**逐条操作的磁盘格式**（`writeOperationNoSize:1661`）：

```
┌──────────────┬───────────────────────────┬──────────────┐
│ size (4B)    │ operation bytes           │ checksum(4B) │
└──────────────┴───────────────────────────┴──────────────┘
   ↑ 不含自身     ↑ 含操作类型 + 字段数据        ↑ CRC32，单独计算
```

- `size` **不参与** checksum（`readOperation:1614` 里 `resetDigest()`）。
- 读取时先 `mark(opSize)` → `skip(opSize-4)` → `verifyChecksum` → `reset` → 真正读（`:1615-1624`）。
  这样能**先校验再解析**，避免解析损坏数据时抛诡异异常。

**★★ `operation.primaryTerm() > current.getPrimaryTerm()` 校验**：
Translog header 记录了 roll 时的 primary term。如果一条操作的 term 比 header 里的还新，
说明出现了不一致（比如脑裂后旧 primary 收到新 term 的写），必须拒绝。
这是 6.x seqno-based 复制的重要防御（详见第 17 章）。

---

## 5. `Operation`：translog 里能存什么

```java
public interface Operation {                       // :1018
    enum Type {                                    // :1019
        @Deprecated CREATE((byte) 1),              //   历史遗留（6.x 不再用）
        INDEX((byte) 2),
        DELETE((byte) 3),
        NO_OP((byte) 4);                           //   ★ 占位操作！
        ...
    }
    Type opType();
    long estimateSize();
    Source getSource();
    long seqNo();
    long primaryTerm();
    static Operation readOperation(StreamInput input) {...}    // :1066
    static void writeOperation(StreamOutput output, Operation operation) {...}  // :1085
}
```

| 实现类 | 行号 | 内容 |
| --- | --- | --- |
| `Index` | `:1120` | id/type/source/routing/parent/version/versionType/seqNo/primaryTerm/autoGeneratedIdTimestamp |
| `Delete` | `:1327` | id/type/version/versionType/seqNo/primaryTerm |
| `NoOp` | `:1488` | seqNo/primaryTerm/reason（**不携带数据**） |
| `Source` | `:1106` | source + routing + parent |

**★★ `NoOp`（`:1488`）是理解 6.x 一致性的关键**：
- 当 primary 上某个操作**生成了 seqno 但最终失败**时，副本必须「消耗掉同一个 seqno」，
  否则两边 seqno 序列错位。
- 做法：在副本上写一条 `NoOp`（`IndexShard.markSeqNoAsNoop`，见第 13 章 `replicaItemExecutionMode` 的 `FAILURE` 分支）。
- 恢复时重放 `NoOp` 只推进 seqno，不写数据。

**`Index` 的序列化格式**（`:1122`）：

| 常量 | 值 | 含义 |
| --- | --- | --- |
| `FORMAT_2_X` | 6 | 2.0 起 |
| `FORMAT_AUTO_GENERATED_IDS` | 7 | 5.0 起（自动生成 id 时间戳） |
| `FORMAT_SEQ_NO` | 8 | **6.0 起（加入 seqNo/primaryTerm）** ← `SERIALIZATION_FORMAT` |

**同一 seqno 的「覆盖」规则**：如果两条操作 seqno 相同，**term 更大的胜出**
（`Snapshot.overriddenOperations:951` 明确写了这一点）。
这是「primary 切换后旧 primary 的写入被新 primary 覆盖」的机制。

---

## 6. ★ `Location`：一个写入的「坐标」（`:875`）

```java
public static class Location implements Comparable<Location> {     // :875
    public final long generation;         // 第几代文件
    public final long translogLocation;   // 文件内偏移
    public final int size;                // 操作字节数

    @Override
    public int compareTo(Location o) {    // :892
        if (generation == o.generation) {
            return Long.compare(translogLocation, o.translogLocation);
        }
        return Long.compare(generation, o.generation);
    }
}
```

**★ 注意 `compareTo` 故意忽略 `size`**（`:892`），而 `equals` 比较 `size`（`:900`）。
原因见 `getLastWriteLocation:575` 的注释：为了让「上一次写的位置」严格小于「下一次写的位置」。

**`Location` 有三大用途**：
1. `WritePrimaryResult` 携带它 → `AsyncAfterWriteAction` 用它做 `sync`/`addRefreshListener`（第 13 章）。
2. `seqno → location` 映射（`LocalCheckpointTracker`/`IndexShard` 的 `deleteShard` 校验）。
3. 副本恢复时用 `minTranslogGeneration` + `Location` 判断增量起点。

**`getLastWriteLocation()`（`:575`）**：返回 `new Location(current.generation, current.sizeInBytes()-1, Integer.MAX_VALUE)`。
这是一个「排在最后一个已写之后、下一个写入之前」的哨兵位置。

---

## 7. ★ 同步（fsync）：`sync` 与 `ensureSynced`

```java
public void sync() throws IOException {                       // :724
    try (ReleasableLock lock = readLock.acquire()) {
        if (closed.get() == false) {
            current.sync();                                   // → TranslogWriter.sync() → syncUpTo(Long.MAX_VALUE)
        }
    } catch (final Exception ex) {
        closeOnTragicEvent(ex);
        throw ex;
    }
}

public boolean ensureSynced(Location location) throws IOException {   // :795
    try (ReleasableLock lock = readLock.acquire()) {
        if (location.generation == current.getGeneration()) {         // 若是旧 generation，说明 roll 时已 sync
            ensureOpen();
            return current.syncUpTo(location.translogLocation + location.size);
        }
    } catch (final Exception ex) {
        closeOnTragicEvent(ex);
        throw ex;
    }
    return false;
}
```

- `sync()` 把整个 `current` fsync 到最新（用于 `durability=async` 的定时 sync、
  以及 flush 前的强制 sync）。
- `ensureSynced(Location)` 只 fsync「到这个位置」（`syncUpTo`，`TranslogWriter:338`），
  用于 `durability=request` 的逐请求 fsync。
- `ensureSynced(Stream<Location>)`（`:815`）**只 sync 最大那个**——因为 translog 是追加的「磁带」，
  sync 到最远的位置自然把前面的都覆盖了（`:89` 的注释）。

**`TranslogWriter.syncUpTo(offset)`（`TranslogWriter:338`）的内部逻辑**（简述）：
- 获取 sync 锁，若已 sync 到 >= offset 直接返回 false。
- 否则 `channel.force(false)`（fsync 数据，不 fsync metadata）。
- **★ 只有在本次 fsync 覆盖了 checkpoint 的更新时才写 `.ckp`**（`writeCheckpoint:397`）——
  避免每次写都写 checkpoint 文件（那是随机 IO）。

---

## 8. ★★ 代（generation）管理：roll 与 trim

### 8.1 为什么要分「代」

- 一个 tlog 文件不能无限增长：**flush 只能删除「整个文件」级别的数据**
  （Lucene commit 记录的是 `translog_generation`，而不是 offset）。
- 所以 ES 把 translog 切成「一代一代」，每代一个文件；
  flush 后只要「某代的所有操作都已被 commit」就能整代删除。

### 8.2 `shouldRollGeneration`（`:565`）

```java
public boolean shouldRollGeneration() {
    final long size = this.current.sizeInBytes();
    final long threshold = this.indexSettings.getGenerationThresholdSize().getBytes();   // 默认 64MB
    return size > threshold;
}
```

### 8.3 `rollGeneration`（`:1702`）

```java
public void rollGeneration() throws IOException {
    try (Releasable ignored = writeLock.acquire()) {                    // ★ 写锁，阻塞所有写
        try {
            final TranslogReader reader = current.closeIntoReader();     // ① current 转为只读 reader
            readers.add(reader);
            assert Checkpoint.read(location.resolve(CHECKPOINT_FILE_NAME)).generation == current.getGeneration();
            copyCheckpointTo(location.resolve(getCommitCheckpointFileName(current.getGeneration())));  // ② 固化 checkpoint
            current = createWriter(current.getGeneration() + 1);         // ③ 新建下一代 writer
            logger.trace("current translog set to [{}]", current.getGeneration());
        } catch (final Exception e) {
            tragedy.setTragicException(e);
            closeOnTragicEvent(e);
            throw e;
        }
    }
}
```

**★★ 三个动作的顺序极其重要**：`closeIntoReader` → `copyCheckpointTo` → `createWriter(gen+1)`。
如果在 ② 和 ③ 之间宕机，`translog.ckp` 已经指向 gen+1 但 gen+1 文件还没建立——
构造函数里的「孤儿文件清理」逻辑（`:181-188`）会处理这个状态。

### 8.4 `trimUnreferencedReaders`（`:1724`）：删除不再需要的代

```java
public void trimUnreferencedReaders() throws IOException {
    try (ReleasableLock ignored = writeLock.acquire()) {
        if (closed.get()) { return; }                                    // 关闭中不删
        long minReferencedGen = deletionPolicy.minTranslogGenRequired(readers, current);   // ★ 问策略
        ...
        for (Iterator<TranslogReader> iterator = readers.iterator(); iterator.hasNext(); ) {
            // 删除 generation < minReferencedGen 的 reader 及其 .tlog/.ckp 文件
        }
    }
}
```

**触发点**：
- `InternalEngine.flush` 之后（`:1778`）；
- `rollTranslogGeneration` 之后（`:1835`）；
- `trimUnreferencedTranslogFiles`（`:1853`，后台任务）；
- 释放 retention lock 之后（`Translog:715`）。

### 8.5 `TranslogDeletionPolicy`（`TranslogDeletionPolicy.java:33`）：什么时候能删

```java
synchronized long minTranslogGenRequired(List<TranslogReader> readers, TranslogWriter writer) throws IOException {  // :156
    long minByLocks = getMinTranslogGenRequiredByLocks();                        // ① 被快照/恢复占用
    long minByAge   = getMinTranslogGenByAge(readers, writer, retentionAgeInMillis, currentTime());   // ② age
    long minBySize  = getMinTranslogGenBySize(readers, writer, retentionSizeInBytes);                 // ③ size
    final long minByAgeAndSize;
    if (minBySize == Long.MIN_VALUE && minByAge == Long.MIN_VALUE) {
        minByAgeAndSize = Long.MAX_VALUE;            // age/size 都关闭（-1）→ 不因它们保留
    } else {
        minByAgeAndSize = Math.max(minByAge, minBySize);   // ★ 取更保守（更大）的那个
    }
    return Math.min(minByAgeAndSize, Math.min(minByLocks, minTranslogGenerationForRecovery));
}
```

**四道闸门，取最小值（最保守）**：

| 闸门 | 字段 | 含义 | 默认 |
| --- | --- | --- | --- |
| `minByLocks` | `translogRefCounts` | 有快照/恢复在读这代 → 不能删 | — |
| `minByAge` | `retentionAgeInMillis` | 最近 N 时间内不删（提高 ops-based recovery 成功率） | 12h |
| `minBySize` | `retentionSizeInBytes` | 至少保留 N 字节 | 512MB |
| `minTranslogGenerationForRecovery` | 同名字段 | 上次 flush 指向的恢复基准代 | — |

> ⚠️ **这是云服务调优的重灾区**：`retention.size/age` 设太大 → translog 占盘爆炸；
> 设太小 → 副本恢复只能用「文件级 recovery」（传整个 segment），恢复变慢。
> 见第 18 章。

**`acquireTranslogGen`（`:107`）**：`Translog.acquireRetentionLock()`（`:701`）会给当前最小代加引用计数，
返回的 `Closeable` 关闭时才允许删除。这就是「快照期间 translog 不被删」的实现。

---

## 9. `Checkpoint`：translog 的元数据（`Checkpoint.java:39`）

```java
final class Checkpoint {
    final long offset;              // 当前写入偏移
    final int numOps;               // 操作数
    final long generation;          // 当前代
    final long minSeqNo;            // 最小 seqno
    final long maxSeqNo;            // 最大 seqno
    final long globalCheckpoint;    // ★ 全局检查点（见第 17 章）
    final long minTranslogGeneration;  // 当前 translog 引用的最小代
    final long trimmedAboveSeqNo;   // ★ 被 trim 的上界（超过它的一律忽略）

    private static final int INITIAL_VERSION = 1;
    private static final int VERSION_6_0_0 = 2;      // 引入 global checkpoint
    private static final int CURRENT_VERSION = 3;    // 引入 trimmed above seq#
}
```

**★ `minTranslogGeneration`**：写在 checkpoint 里，也在 Lucene commit 的 `userData` 里。
它告诉恢复流程「从哪一代开始重放是安全的」。

**★ `trimmedAboveSeqNo`**：配合 `trimOperations(belowTerm, aboveSeqNo)`（`Translog:757`）
实现「peer recovery 的 trim」——当副本从 primary 做「基于操作」的恢复时，
primary 会把副本「丢失的那段」之外的操作标记为可跳过（见第 18 章）。

**恢复时的 checkpoint 读取**（`Translog` 构造函数 `:170-205`）：

```java
final Checkpoint checkpoint = readCheckpoint(location);
final Path nextTranslogFile = location.resolve(getFilename(checkpoint.generation + 1));
final Path currentCheckpointFile = location.resolve(getCommitCheckpointFileName(checkpoint.generation));
// ★ 处理「上一代已固化，但下一代只建了空文件就宕机」的状态
if (Files.exists(currentCheckpointFile) && Files.deleteIfExists(nextTranslogFile)) {
    logger.warn("deleted previously created, but not yet committed, next generation [{}]...", nextTranslogFile.getFileName());
}
this.readers.addAll(recoverFromFiles(checkpoint));
if (readers.isEmpty()) { throw new IllegalStateException("at least one reader must be recovered"); }
current = createWriter(checkpoint.generation + 1, getMinFileGeneration(), checkpoint.globalCheckpoint);
```

---

## 10. Snapshot 与恢复读取

```java
public Snapshot newSnapshot() throws IOException {                    // :601
    try (ReleasableLock ignored = readLock.acquire()) {
        return newSnapshotFromGen(new TranslogGeneration(translogUUID, getMinFileGeneration()), Long.MAX_VALUE);
    }
}

public Snapshot newSnapshotFromGen(TranslogGeneration fromGeneration, long upToSeqNo) throws IOException {  // :607
    try (ReleasableLock ignored = readLock.acquire()) {
        ensureOpen();
        final long fromFileGen = fromGeneration.translogFileGeneration;
        if (fromFileGen < getMinFileGeneration()) {
            throw new IllegalArgumentException("requested snapshot generation [" + fromFileGen + "] is not available. ...");
        }
        TranslogSnapshot[] snapshots = Stream.concat(readers.stream(), Stream.of(current))
            .filter(reader -> reader.getGeneration() >= fromFileGen && reader.getCheckpoint().minSeqNo <= upToSeqNo)
            .map(BaseTranslogReader::newSnapshot).toArray(TranslogSnapshot[]::new);
        final Snapshot snapshot = newMultiSnapshot(snapshots);
        ...
    }
}
```

- `Snapshot`（`:932`）接口：`totalOperations` / `skippedOperations` / `overriddenOperations` / `next`。
- `SeqNoFilterSnapshot`（`:967`）：只返回 `[fromSeqNo, toSeqNo]` 范围内的操作，
  其他计入 `skippedOperations`。**这就是「按 seqno 做 peer recovery」的读取入口。**
- `MultiSnapshot`：把多个 reader 的 snapshot 串成一个流。

**谁在用 Snapshot？**
- `IndexShard.recoverFromTranslog`（本地恢复，第 18 章）。
- `RecoverySource.PeerRecoverySource` 的 phase1（从 primary 拉 translog 操作）。
- `InternalEngine` 的 ops-based 恢复。

---

## 11. 与 Engine / Flush 的联动

Translog 自己**不决定**何时 flush，是 `InternalEngine` 在写后判断的。

### 11.1 `shouldPeriodicallyFlush`（`InternalEngine.java:1713`）

```java
public boolean shouldPeriodicallyFlush() {
    ensureOpen();
    final long translogGenerationOfLastCommit =
        Long.parseLong(lastCommittedSegmentInfos.userData.get(Translog.TRANSLOG_GENERATION_KEY));   // ★ 上次 commit 的 gen
    final long flushThreshold = config().getIndexSettings().getFlushThresholdSize().getBytes();      // 默认 512MB
    if (translog.sizeInBytesByMinGen(translogGenerationOfLastCommit) < flushThreshold) {
        return false;
    }
    final long translogGenerationOfNewCommit =
        translog.getMinGenerationForSeqNo(localCheckpointTracker.getCheckpoint() + 1).translogFileGeneration;
    return translogGenerationOfLastCommit < translogGenerationOfNewCommit              // 新 commit 指向更后的代
        || localCheckpointTracker.getCheckpoint() == localCheckpointTracker.getMaxSeqNo();
}
```

**判定条件**：`未 commit 的 translog 大小 > flush_threshold_size`（默认 **512MB**）。
注意最后一行：如果 flush 后条件仍成立会**死循环 flush**，所以额外要求「新 commit 指向更新的代」。

### 11.2 `afterWriteOperation`（`IndexShard.java:2625`）：写后异步触发

```java
public void afterWriteOperation() {
    if (shouldPeriodicallyFlush() || shouldRollTranslogGeneration()) {
        if (flushOrRollRunning.compareAndSet(false, true)) {       // ★ 保证只有一个在跑
            if (shouldPeriodicallyFlush()) {
                // 提交到 ThreadPool.Names.FLUSH 线程池异步执行
                flush(new FlushRequest());
            } else if (shouldRollTranslogGeneration()) {
                rollTranslogGeneration();
            } else {
                flushOrRollRunning.compareAndSet(true, false);
            }
        }
    }
}
```

**★ 注意**：`afterWriteOperation` 由 `AsyncAfterWriteAction.run()`（第 13 章 `TransportWriteAction:339`）
**在每次写的最后**调用。所以「写触发 flush/roll，但 flush/roll 在后台 FLUSH 线程池异步跑」。

### 11.3 `flush`（`InternalEngine.java:1743`）：真正提交

```java
public CommitId flush(boolean force, boolean waitIfOngoing) throws EngineException {
    ...
    if (indexWriter.hasUncommittedChanges() || force || shouldPeriodicallyFlush()) {
        ensureCanFlush();
        try {
            translog.rollGeneration();                        // ① roll：把当前代固化
            commitIndexWriter(indexWriter, translog, null);   // ② Lucene commit（写 segments_N，把 gen 记进 userData）
            refresh("version_table_flush", SearcherScope.INTERNAL);   // ③ refresh 清掉旧版本值
            translog.trimUnreferencedReaders();               // ④ 删掉不再需要的代
        } catch (AlreadyClosedException e) { throw e; }
        catch (Exception e) { throw new FlushFailedEngineException(shardId, e); }
        refreshLastCommittedSegmentInfos();
    }
    newCommitId = lastCommittedSegmentInfos.getId();
    ...
}
```

**★★ flush 的四步顺序是 ES 持久化正确性的基石**：

```
rollGeneration  ← 先把"当前写的位置"定格成一个完整的代
commit          ← Lucene 提交，并在 commit userData 里记下 translog_generation / translog_uuid
refresh         ← 清理旧版本（version map）
trim            ← 现在可以安全删除 commit 之前的所有代
```

**为什么必须先 roll 再 commit？** 因为 Lucene commit 记录的是「代」，
如果不 roll，commit 记录的代 = 当前正在写的代，那样**这一代永远不能被删除**（里面还有新写的数据）。

### 11.4 其他 flush 触发点

| 触发 | 入口 | 说明 |
| --- | --- | --- |
| `_flush` API | `RestFlushAction` → `TransportFlushAction` → `IndexShard.flush` | 手动 |
| translog 超阈值 | `afterWriteOperation` | 默认 512MB |
| 索引关闭前 | `IndexShard.close` | 保证数据落盘 |
| 副本 recovery 结束时 | `IndexShard` recovery | 见第 18 章 |
| 定时（`index.translog.flush_threshold_period` 已废弃） | — | 6.x 用 size 触发 |

---

## 12. 配置速查

| 设置 | 默认 | 动态 | 含义 | 行号 |
| --- | --- | --- | --- | --- |
| `index.translog.durability` | `request` | ✅ | `request`=每次请求 fsync；`async`=定时 fsync | `IndexSettings.java:80` |
| `index.translog.sync_interval` | `5s`（最小 100ms） | ❌ | `async` 下的 fsync 间隔 | `IndexSettings.java:77` |
| `index.translog.flush_threshold_size` | `512mb` | ✅ | 未 commit translog 超过它触发 flush | `IndexSettings.java:192` |
| `index.translog.retention.age` | `12h` | ✅ | flush 后仍保留多久（提高恢复成功率） | `IndexSettings.java:208` |
| `index.translog.retention.size` | `512mb` | ✅ | flush 后仍保留多少字节 | `IndexSettings.java:217` |
| `index.translog.generation_threshold_size` | `64mb` | ✅ | 单代多大就 roll | `IndexSettings.java:225` |
| Translog 写缓冲 | `8kb` | ❌ | `TranslogConfig.DEFAULT_BUFFER_SIZE` | `TranslogConfig.java:37` |

**相关监控指标**（`GET _stats?level=shards`）：

| 指标 | 含义 |
| --- | --- |
| `translog.size_in_bytes` | 当前 translog 总大小 |
| `translog.uncommitted_size_in_bytes` | 未 commit 的大小（关键！） |
| `translog.uncommitted_operations` | 未 commit 的操作数 |
| `translog.earliest_last_modified_age` | 最老 translog 文件年龄 |

**磁盘占用诊断**：
```
GET /_cat/allocation?v
GET /idx/_stats?level=shards&filter_path=**.translog
```

---

## 13. 故障排查对照表

| 现象 | 根因 | 定位 / 解决 |
| --- | --- | --- |
| 磁盘持续增长，且 segment 不多 | `retention.size/age` 过大，translog 不删 | 调小 `index.translog.retention.size`；`POST _flush` |
| `TranslogCorruptedException` | tlog header uuid 与 commit uuid 不匹配；文件损坏/被误拷 | 数据无法自动恢复，需从快照/副本恢复 |
| `TruncatedTranslogException` | tlog 文件尾部被截断（掉电/diskcache 丢失） | 6.x 会自动截断到最后一条完整记录；继续报错则数据损坏 |
| 写延迟高 | `durability=request` 且磁盘 IOPS 低 | 看 `fsync` 耗时；考虑 `async`（权衡丢数据） |
| 写延迟周期性尖刺 | `rollGeneration`/`trimUnreferencedReaders` 持写锁 | 调大 `generation_threshold_size` 减少 roll 频率 |
| flush 频繁、CPU 高 | `flush_threshold_size` 太小 | 调大（默认 512MB 已较合理） |
| `flush` 卡住不返回 | 大段 commit 慢 / 磁盘慢 | 看 FLUSH 线程池；`GET _nodes/stats/thread_pool` |
| 恢复很慢（recovery 长时间 transferring translog） | translog 保留太少 → 只能文件级恢复 | 调大 `retention.size`（第 18 章） |
| `translog` 目录下出现孤儿文件 | roll/commit 之间宕机 | 重启时自动清理并打 warn 日志 |
| `already closed` 后 shard 失效 | `tragedy` 被置位（写 translog 时 IO 异常） | 见 `closeOnTragicEvent`；通常需要 fail shard |
| `Operation term is newer than the current term` | 旧 primary / 旧 term 的写入到达 | 通常是选主瞬间；若持续说明集群有问题 |

---

## 14. 动手实验

**14-1 直接观察 translog 目录**
```bash
curl -XPUT "localhost:9200/tl/_doc/1?refresh=true" -H 'Content-Type: application/json' -d '{"a":1}'
ls -l $(find /path/to/data/nodes/0/indices -name translog -type d | head -1)
# 期望看到 translog.ckp 和 translog-N.tlog
```

**14-2 让 translog 增长**
```bash
for i in $(seq 1 2000); do curl -s -XPUT "localhost:9200/tl/_doc/$i" -H 'Content-Type: application/json' -d "{\"a\":$i}" >/dev/null; done
curl -XGET "localhost:9200/tl/_stats?level=shards&filter_path=**.translog&pretty"
# 观察 uncommitted_size_in_bytes / uncommitted_operations
```

**14-3 flush 后观察 translog 变小**
```bash
curl -XPOST "localhost:9200/tl/_flush?pretty"
curl -XGET "localhost:9200/tl/_stats?level=shards&filter_path=**.translog&pretty"
# 期望 uncommitted_operations 归零、文件数量减少（旧代被 trim）
```

**14-4 roll generation**
```bash
curl -XPOST "localhost:9200/tl/_flush/synced?pretty"   # 或手动 _flush
ls -l .../translog/   # 观察 generation 递增：translog-1.tlog → translog-2.tlog
```

**14-5 调整 generation_threshold_size 让 roll 更频繁**
```bash
curl -XPUT "localhost:9200/tl/_settings" -H 'Content-Type: application/json' \
  -d '{"index":{"translog.generation_threshold_size":"1mb"}}'
# 写入若干数据，观察 .tlog 文件快速增多；再调回 64mb
```

**14-6 对比 durability=request vs async 的写延迟**
```bash
curl -XPUT "localhost:9200/tl/_settings" -H 'Content-Type: application/json' \
  -d '{"index":{"translog.durability":"async","translog.sync_interval":"5s"}}'
# 用压测（esrally / 自写脚本）对比 P99 写延迟
curl -XPUT "localhost:9200/tl/_settings" -H 'Content-Type: application/json' \
  -d '{"index":{"translog.durability":"request"}}'
```

**14-7 模拟「translog 未 flush 时重启」的恢复**
```bash
# 关闭刷新，写入数据（此时数据只在 translog）
curl -XPUT "localhost:9200/tl/_settings" -H 'Content-Type: application/json' -d '{"index":{"refresh_interval":"-1"}}'
curl -XPUT "localhost:9200/tl/_doc/999" -H 'Content-Type: application/json' -d '{"a":999}'
# 重启 ES 节点
curl -XGET "localhost:9200/tl/_doc/999"   # 数据仍在 → 说明是从 translog 恢复的
```

**14-8 观察 translog trim 的日志**
在 `log4j2.properties` 把 `org.elasticsearch.index.translog` 设为 `TRACE`，
执行 `_flush`，观察 `trimUnreferencedReaders` 删除了哪些 generation。

**14-9 断点调试 `add`**
在 `Translog.add:524` 和 `copyCheckpointTo`（`:1708` 调用）打断点，
写一条文档，观察 `Location`（generation/translogLocation/size）。

**14-10 观察 checkpoint 内容**
用随包工具或直接 `xxd` 看 `translog.ckp`（Lucene `ckp` codec 格式）：
```bash
xxd .../translog/translog.ckp | head -20
```

---

## 15. 自测题

1. 为什么 ES 需要 Translog？只靠 Lucene 不行吗？
2. 一条 translog 记录在磁盘上的格式是什么？`size` 为什么不计入 checksum？
3. `translog.ckp` 和 `translog-<gen>.ckp` 有什么区别？
4. translogUUID 的作用是什么？为什么不能手工拷贝 shard 目录？
5. `add` 里「操作 term > current term」为什么要抛异常？
6. `NoOp` 操作什么时候产生？它携带数据吗？
7. 同一 seqno 的两条操作，恢复时以哪条为准？在哪段代码体现？
8. `Location.compareTo` 为什么忽略 `size`？
9. `sync()` 与 `ensureSynced(Location)` 有什么区别？`ensureSynced(Stream)` 为什么只 sync 最大的？
10. 为什么要把 translog 切成多代？如果不分代会怎样？
11. `rollGeneration` 的三个动作顺序是什么？为什么不能颠倒？
12. `TranslogDeletionPolicy.minTranslogGenRequired` 有哪四道闸门？取 min 还是 max？为什么？
13. `trimUnreferencedReaders` 持的是读锁还是写锁？对写入有什么影响？
14. `flush` 的四步顺序是什么？为什么必须先 `rollGeneration` 再 `commit`？
15. `flush_threshold_size` 默认多少？超过它会怎样？在哪个方法里判断的？
16. `afterWriteOperation` 跑在哪个线程池？它和写请求是否同步？
17. `durability=request` / `async` 分别怎么 fsync？宕机各可能丢多少数据？
18. `retention.size/age` 调大调小分别有什么利弊？
19. `timmedAboveSeqNo` / `trimOperations` 是给谁用的？
20. `TranslogCorruptedException` 与 `TruncatedTranslogException` 有什么区别？各怎么处理？

---

## 16. 关键类速查表

| 类 | 路径 | 一句话 |
| --- | --- | --- |
| `Translog` | `index/translog/Translog.java` | ★ 事务日志核心（`:524` add、`:724` sync、`:1702` roll、`:1724` trim） |
| `Translog.Location` | 同上 `:875` | 写入坐标（generation/offset/size） |
| `Translog.Operation` / `.Type` | 同上 `:1018`/`:1019` | INDEX/DELETE/NO_OP |
| `Translog.Index/Delete/NoOp` | 同上 `:1120`/`:1327`/`:1488` | 三种操作 |
| `Translog.Snapshot` / `SeqNoFilterSnapshot` | 同上 `:932`/`:967` | 只读遍历（恢复用） |
| `Translog.Durability` | 同上 `:1571` | ASYNC / REQUEST |
| `TranslogWriter` | `index/translog/TranslogWriter.java` | ★ 当前代的写入器（`:251` sync、`:338` syncUpTo、`:397` writeCheckpoint） |
| `TranslogReader` / `BaseTranslogReader` | `index/translog/TranslogReader.java` | 只读的已固化代 |
| `TranslogDeletionPolicy` | `index/translog/TranslogDeletionPolicy.java` | ★ 删除策略（`:156` minTranslogGenRequired、`:107` acquire） |
| `Checkpoint` | `index/translog/Checkpoint.java` | translog 元数据（`:100` 构造） |
| `TranslogConfig` | `index/translog/TranslogConfig.java` | 配置（`:37` 8KB buffer） |
| `TranslogHeader` | `index/translog/TranslogHeader.java` | 文件头（含 uuid/primaryTerm） |
| `MultiSnapshot` | `index/translog/MultiSnapshot.java` | 多代合成一个 snapshot |
| `TranslogCorruptedException` / `TruncatedTranslogException` | `index/translog/` | 损坏异常 |
| `TranslogToolCli` / `TruncateTranslogAction` | `index/translog/` | 离线修复工具（`elasticsearch-shard`） |
| `InternalEngine` | `index/engine/InternalEngine.java` | ★ 引擎（`:1713` shouldPeriodicallyFlush、`:1743` flush） |
| `IndexShard` | `index/shard/IndexShard.java` | ★ 分片（`:2625` afterWriteOperation） |
| `IndexSettings` | `index/IndexSettings.java` | translog 相关设置（`:77-234`） |
| `IndexShardComponent` | `index/shard/` | 提供 shardId/indexSettings |

---

**上一篇**：[13-写入链路（Index与Bulk）.md](./13-写入链路（Index与Bulk）.md)
**下一篇**：[15-存储引擎-InternalEngine与Lucene集成.md](./15-存储引擎-InternalEngine与Lucene集成.md)
