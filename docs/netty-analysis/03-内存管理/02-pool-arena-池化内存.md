# PoolArena 池化内存管理深度分析

## 一、概述

PoolArena 是 Netty 池化内存分配器的核心调度层。它借鉴了 jemalloc 的设计理念，通过多层级的数据结构实现了高效的内存分配与回收。整个分配体系由以下核心组件构成：

- **PoolArena** -- 内存分配的顶层调度器，管理 Chunk 列表和 SubPage 池
- **PoolChunk** -- 16MB 的大内存块，内部通过位图管理页级别的分配
- **PoolSubpage** -- 将单个 Page 进一步切分为小块，用于 SubPage 级别的分配
- **PoolChunkList** -- 按使用率分组管理 Chunk 的链表，实现 Chunk 的动态迁移
- **PoolThreadCache** -- 线程本地缓存，减少锁竞争

默认配置下，chunkSize = 8192 << 9 = 16MB，pageSize = 8192 (8KB)，每个 Chunk 包含 2048 个 Page。

## 二、整体架构

### 2.1 架构总览

```
PooledByteBufAllocator
    |
    +-- heapArenas[]          (默认 2*CPU核数 个 HeapArena)
    +-- directArenas[]        (默认 2*CPU核数 个 DirectArena)
    +-- PoolThreadLocalCache  (FastThreadLocal, 每线程一个 PoolThreadCache)
         |
         +-- PoolThreadCache
              +-- heapArena  -> 指向 leastUsedArena 选中的某个 HeapArena
              +-- directArena -> 指向 leastUsedArena 选中的某个 DirectArena
              +-- smallSubPageHeapCaches[]
              +-- smallSubPageDirectCaches[]
              +-- normalHeapCaches[]
              +-- normalDirectCaches[]
```

### 2.2 Arena 轮转选择

线程首次分配时，通过 `leastUsedArena()` 选择当前绑定线程数最少的 Arena，而非简单轮转。这保证了各 Arena 之间的负载均衡：

```java
// PooledByteBufAllocator.PoolThreadLocalCache
private <T> PoolArena<T> leastUsedArena(PoolArena<T>[] arenas) {
    if (arenas == null || arenas.length == 0) {
        return null;
    }
    PoolArena<T> minArena = arenas[0];
    if (minArena.numThreadCaches.get() == CACHE_NOT_USED) {
        return minArena; // 优化：首次直接返回，避免遍历
    }
    for (int i = 1; i < arenas.length; i++) {
        PoolArena<T> arena = arenas[i];
        if (arena.numThreadCaches.get() < minArena.numThreadCaches.get()) {
            minArena = arena;
        }
    }
    return minArena;
}
```

每个 Arena 维护 `numThreadCaches` 原子计数器，当 PoolThreadCache 创建时 +1，释放时 -1。通过将线程分散到多个 Arena，每个 Arena 内部的锁竞争被降低为 1/N。

### 2.3 Arena 内部结构

```
PoolArena
    |
    +-- smallSubpagePools[]        // SubPage 池，按 elemSize 分组的双向链表头数组
    |     +-- [0] -> SubPage <-> SubPage <-> SubPage <-> head  (elemSize=16)
    |     +-- [1] -> SubPage <-> SubPage <-> head              (elemSize=32)
    |     +-- ...
    |
    +-- qInit   [0%, 25%]         // 使用率 0~25% 的 Chunk 链表
    +-- q000    [1%, 50%]         // 使用率 1~50%
    +-- q025    [25%, 75%]        // 使用率 25~75%
    +-- q050    [50%, 100%]       // 使用率 50~100%
    +-- q075    [75%, 100%]       // 使用率 75~100%
    +-- q100    [100%, MAX]       // 使用率 100%
```

## 三、核心类分析

### 3.1 PoolArena -- 抽象基类

#### 3.1.1 分配入口：allocate()

```java
private void allocate(PoolThreadCache cache, PooledByteBuf<T> buf, final int reqCapacity) {
    final int sizeIdx = sizeClass.size2SizeIdx(reqCapacity);

    if (sizeIdx <= sizeClass.smallMaxSizeIdx) {
        tcacheAllocateSmall(cache, buf, reqCapacity, sizeIdx);   // <= 一个 Page
    } else if (sizeIdx < sizeClass.nSizes) {
        tcacheAllocateNormal(cache, buf, reqCapacity, sizeIdx);  // 多个 Page
    } else {
        allocateHuge(buf, normCapacity);                         // 超过 chunkSize
    }
}
```

三级分配策略：
- **Small** (sizeIdx <= smallMaxSizeIdx)：小于等于 pageSize 的分配，走 SubPage 机制
- **Normal** (sizeIdx < nSizes)：大于 pageSize 但不超过 chunkSize 的分配，走 Run/Page 机制
- **Huge** (sizeIdx >= nSizes)：超过 chunkSize 的分配，直接向操作系统申请非池化内存

#### 3.1.2 Small 分配流程：tcacheAllocateSmall()

```java
private void tcacheAllocateSmall(PoolThreadCache cache, PooledByteBuf<T> buf,
                                  final int reqCapacity, final int sizeIdx) {
    // 第一步：尝试线程本地缓存
    if (cache.allocateSmall(this, buf, reqCapacity, sizeIdx)) {
        return;
    }

    // 第二步：从 Arena 的 smallSubpagePools 中获取已有的 SubPage
    final PoolSubpage<T> head = smallSubpagePools[sizeIdx];
    head.lock();
    try {
        final PoolSubpage<T> s = head.next;
        if (s != head) {
            // 有可用的 SubPage，直接分配
            long handle = s.allocate();
            s.chunk.initBufWithSubpage(buf, null, handle, reqCapacity, cache, false);
            return;
        }
    } finally {
        head.unlock();
    }

    // 第三步：没有可用 SubPage，加锁分配新的 Normal 区域并切分为 SubPage
    lock();
    try {
        allocateNormal(buf, reqCapacity, sizeIdx, cache);
    } finally {
        unlock();
    }
}
```

三级降级策略：ThreadCache -> SubPagePool -> Arena 全局分配。

#### 3.1.3 Normal 分配流程：allocateNormal()

```java
private void allocateNormal(PooledByteBuf<T> buf, int reqCapacity, int sizeIdx,
                             PoolThreadCache threadCache) {
    // 按使用率从高到低尝试，优先使用利用率较高的 Chunk
    if (q050.allocate(buf, reqCapacity, sizeIdx, threadCache) ||
        q025.allocate(buf, reqCapacity, sizeIdx, threadCache) ||
        q000.allocate(buf, reqCapacity, sizeIdx, threadCache) ||
        qInit.allocate(buf, reqCapacity, sizeIdx, threadCache) ||
        q075.allocate(buf, reqCapacity, sizeIdx, threadCache)) {
        return;
    }

    // 所有 ChunkList 都无法满足，申请新的 Chunk
    PoolChunk<T> c = newChunk(sizeClass.pageSize, sizeClass.nPSizes,
                               sizeClass.pageShifts, sizeClass.chunkSize);
    boolean success = c.allocate(buf, reqCapacity, sizeIdx, threadCache);
    assert success;
    qInit.add(c);  // 新 Chunk 放入 qInit
}
```

分配顺序的设计意图：优先使用已有 Chunk（q050 -> q025 -> q000 -> qInit -> q075），减少新 Chunk 的创建。只有所有现有 Chunk 都无法满足时才创建新 Chunk。

#### 3.1.4 释放流程：free()

```java
void free(PoolChunk<T> chunk, ByteBuffer nioBuffer, long handle,
          int normCapacity, PoolThreadCache cache) {
    chunk.decrementPinnedMemory(normCapacity);
    if (chunk.unpooled) {
        // Huge 分配，直接销毁
        destroyChunk(chunk);
        activeBytesHuge.add(-size);
    } else {
        SizeClass sizeClass = sizeClass(handle);
        // 优先放入线程本地缓存
        if (cache != null && cache.add(this, chunk, nioBuffer, handle, normCapacity, sizeClass)) {
            return;
        }
        // 缓存已满，归还到 Chunk
        freeChunk(chunk, handle, normCapacity, sizeClass, nioBuffer, false);
    }
}
```

释放时优先将内存区域缓存到线程本地，避免频繁的全局锁竞争。

#### 3.1.5 HeapArena vs DirectArena

| 特性 | HeapArena | DirectArena |
|------|-----------|-------------|
| 内存类型 | `byte[]` | `ByteBuffer` (direct) |
| 底层分配 | `PlatformDependent.allocateUninitializedArray()` | `PlatformDependent.allocateDirect()` |
| 内存拷贝 | `System.arraycopy()` | Unsafe `copyMemory()` 或 NIO `put()` |
| Chunk 复用 | 通过 `lastDestroyedChunk` 缓存一个已销毁的 Chunk | 无复用，依赖 GC |
| ByteBuf 类型 | `PooledUnsafeHeapByteBuf` / `PooledHeapByteBuf` | `PooledUnsafeDirectByteBuf` / `PooledDirectByteBuf` |

### 3.2 PoolChunk -- 内存块管理

#### 3.2.1 数据结构

PoolChunk 管理一块连续的内存（默认 16MB），将其划分为多个 Page（默认 8KB）进行管理。

核心数据结构：
- **runsAvailMap** (`LongLongHashMap`)：记录每个可用 Run 的首尾页偏移量到 handle 的映射，用于合并相邻空闲 Run
- **runsAvail** (`IntPriorityQueue[]`)：按页数索引的优先队列数组，每个队列管理相同大小的可用 Run
- **subpages** (`PoolSubpage[]`)：按页偏移索引的 SubPage 数组
- **freeBytes**：当前空闲字节数

#### 3.2.2 Handle 位布局

handle 是一个 long 值，64 位的布局如下：

```
 63       49 48       34 33  33  32  32  31                          0
+-----------+-----------+---+---+---+---------------------------------+
| runOffset |   size    | u | e |        bitmapIdx                  |
|  (15bit)  |  (15bit)  |(1)|(1)|           (32bit)                 |
+-----------+-----------+---+---+---+---------------------------------+
```

- **runOffset** (bit 63-49)：Run 在 Chunk 中的页偏移量
- **size** (bit 48-34)：Run 包含的页数
- **isUsed** (bit 33)：是否已分配
- **isSubpage** (bit 32)：是否为 SubPage 分配
- **bitmapIdx** (bit 31-0)：SubPage 内的位图索引（仅 SubPage 有效）

#### 3.2.3 Chunk 内存布局

```
 PoolChunk (16MB = 2048 pages)
 +----------+----------+----------+----------+----------+----------+
 | Page 0   | Page 1   | Page 2   | ...      | Page 2046| Page 2047|
 +----------+----------+----------+----------+----------+----------+
 |<--  Run A (4 pages) -->|<-Run B->|<--  Run C (8 pages) --------->|
 |        allocated       |  free   |         free                  |
 +------------------------+---------+-------------------------------+

 runsAvail[3]:  [RunC(8页)]
 runsAvail[0]:  [RunB(1页)]
 runsAvailMap:  { RunC.offset -> RunC.handle, RunC.end -> RunC.handle,
                  RunB.offset -> RunB.handle, RunB.end -> RunB.handle }
```

#### 3.2.4 allocate() 分配算法

```java
boolean allocate(PooledByteBuf<T> buf, int reqCapacity, int sizeIdx, PoolThreadCache cache) {
    final long handle;
    if (sizeIdx <= arena.sizeClass.smallMaxSizeIdx) {
        // === Small: SubPage 分配 ===
        PoolSubpage<T> head = arena.smallSubpagePools[sizeIdx];
        head.lock();
        try {
            PoolSubpage<T> nextSub = head.next;
            if (nextSub != head) {
                // 有现成的 SubPage，直接从中分配一个槽位
                handle = nextSub.allocate();
                nextSub.chunk.initBufWithSubpage(buf, null, handle, reqCapacity, cache, false);
                return true;
            }
            // 没有现成 SubPage，分配一个新的 Run 并初始化为 SubPage
            handle = allocateSubpage(sizeIdx, head);
        } finally {
            head.unlock();
        }
    } else {
        // === Normal: Run/Page 分配 ===
        int runSize = arena.sizeClass.sizeIdx2size(sizeIdx);
        handle = allocateRun(runSize);
    }
    ByteBuffer nioBuffer = cachedNioBuffers != null ? cachedNioBuffers.pollLast() : null;
    initBuf(buf, nioBuffer, handle, reqCapacity, cache, false);
    return true;
}
```

#### 3.2.5 allocateRun() -- 页级分配

```java
private long allocateRun(int runSize) {
    int pages = runSize >> pageShifts;
    int pageIdx = arena.sizeClass.pages2pageIdx(pages);

    runsAvailLock.lock();
    try {
        // 1. 查找第一个足够大的可用 Run
        int queueIdx = runFirstBestFit(pageIdx);
        if (queueIdx == -1) {
            return -1;  // 没有可用空间
        }

        // 2. 从该队列中取出偏移量最小的 Run
        IntPriorityQueue queue = runsAvail[queueIdx];
        long handle = queue.poll();
        handle <<= BITMAP_IDX_BIT_LENGTH;

        removeAvailRun0(handle);

        // 3. 如果 Run 大于需求，拆分并保存剩余部分
        handle = splitLargeRun(handle, pages);

        int pinnedSize = runSize(pageShifts, handle);
        freeBytes -= pinnedSize;
        return handle;
    } finally {
        runsAvailLock.unlock();
    }
}
```

**runFirstBestFit()** 的策略：
- 如果 Chunk 完全空闲（freeBytes == chunkSize），直接返回最大页索引
- 否则从请求的 pageIdx 开始向后搜索，找到第一个非空队列

**splitLargeRun()** 的拆分逻辑：如果找到的 Run 有 16 页但只需要 4 页，则拆分为两个 Run：前 4 页标记为已使用，后 12 页插入回 runsAvail 供后续分配。

#### 3.2.6 allocateSubpage() -- SubPage 分配

```java
private long allocateSubpage(int sizeIdx, PoolSubpage<T> head) {
    // 1. 计算 SubPage 需要的 Run 大小（必须是 pageSize 的倍数）
    int runSize = calculateRunSize(sizeIdx);

    // 2. 先分配一个 Run
    long runHandle = allocateRun(runSize);
    if (runHandle < 0) {
        return -1;
    }

    int runOffset = runOffset(runHandle);
    int elemSize = arena.sizeClass.sizeIdx2size(sizeIdx);

    // 3. 将 Run 初始化为 SubPage（计算可容纳的元素数量，初始化位图）
    PoolSubpage<T> subpage = new PoolSubpage<T>(head, this, pageShifts, runOffset,
            runSize(pageShifts, runHandle), elemSize);
    subpages[runOffset] = subpage;

    // 4. 从 SubPage 中分配一个元素
    return subpage.allocate();
}
```

**calculateRunSize()** 的目标是找到 pageSize 和 elemSize 的最小公倍数，确保 Run 能被 elemSize 整除，最大化空间利用率。

#### 3.2.7 free() -- 释放与合并

```java
void free(long handle, int normCapacity, ByteBuffer nioBuffer) {
    if (isSubpage(handle)) {
        // SubPage 释放：将槽位归还给 SubPage
        int sIdx = runOffset(handle);
        PoolSubpage<T> subpage = subpages[sIdx];
        PoolSubpage<T> head = subpage.chunk.arena.smallSubpagePools[subpage.headIndex];
        head.lock();
        try {
            if (subpage.free(head, bitmapIdx(handle))) {
                return;  // SubPage 仍有其他元素在使用，不释放 Run
            }
            // SubPage 所有元素都已释放，需要释放整个 Run
            subpages[sIdx] = null;
        } finally {
            head.unlock();
        }
    }

    // Run 释放：合并相邻空闲 Run
    runsAvailLock.lock();
    try {
        long finalRun = collapseRuns(handle);
        finalRun &= ~(1L << IS_USED_SHIFT);     // 标记为未使用
        finalRun &= ~(1L << IS_SUBPAGE_SHIFT);   // 清除 SubPage 标记
        insertAvailRun(runOffset(finalRun), runPages(finalRun), finalRun);
        freeBytes += runSize;
    } finally {
        runsAvailLock.unlock();
    }
}
```

**collapseRuns()** 的合并算法：
1. `collapsePast(handle)` -- 向前合并：检查当前 Run 偏移量 -1 处是否有相邻的空闲 Run
2. `collapseNext(handle)` -- 向后合并：检查当前 Run 结束位置 +1 处是否有相邻的空闲 Run
3. 合并时从 runsAvail 和 runsAvailMap 中移除被合并的 Run

### 3.3 PoolSubpage -- 子页管理

#### 3.3.1 数据结构

```
PoolSubpage
    +-- elemSize       // 每个元素的大小（如 16, 32, 48, ...）
    +-- runSize        // 这个 SubPage 管理的总字节数
    +-- maxNumElems    // 最大元素数 = runSize / elemSize
    +-- numAvail       // 当前可用元素数
    +-- bitmap[]       // 位图，每位标记一个槽位是否已分配
    +-- nextAvail      // 上次释放的槽位索引（快速路径优化）
    +-- prev / next    // 双向链表指针
```

位图示例（假设 maxNumElems = 16）：
```
bitmap[0] = 0b 0 0 0 0 0 0 0 0 0 0 0 0 1 1 0 1
                                  bit 0  1  2 ... 15
                                  状态  1  1  0  1  -> 槽位 0,1,3 已分配，槽位 2 可用
```

#### 3.3.2 allocate() -- 位图分配

```java
long allocate() {
    if (numAvail == 0 || !doNotDestroy) {
        return -1;
    }

    // 1. 获取下一个可用槽位（优先使用上次释放的 nextAvail）
    final int bitmapIdx = getNextAvail();
    if (bitmapIdx < 0) {
        removeFromPool();
        throw new AssertionError("No next available bitmap index found");
    }

    // 2. 在位图中标记为已分配
    int q = bitmapIdx >>> 6;   // bitmap 数组索引
    int r = bitmapIdx & 63;    // long 内的位偏移
    bitmap[q] |= 1L << r;

    // 3. 如果所有槽位都已分配，从链表中移除
    if (--numAvail == 0) {
        removeFromPool();
    }

    return toHandle(bitmapIdx);
}
```

**getNextAvail()** 的两级查找：
1. 快速路径：检查 `nextAvail` 缓存（上次 free 时记录的索引）
2. 慢速路径：`findNextAvail()` 遍历 bitmap 数组，找到第一个有空闲位的 long 值，再逐位查找

#### 3.3.3 free() -- 位图释放

```java
boolean free(PoolSubpage<T> head, int bitmapIdx) {
    int q = bitmapIdx >>> 6;
    int r = bitmapIdx & 63;

    // 1. 清除位图中的标记
    bitmap[q] ^= 1L << r;

    // 2. 记录下次可快速分配的索引
    setNextAvail(bitmapIdx);

    // 3. 如果之前全满，重新加入链表
    if (numAvail++ == 0) {
        addToPool(head);
        if (maxNumElems > 1) {
            return true;
        }
    }

    // 4. 如果所有槽位都空闲，标记为可销毁（从链表中移除）
    if (numAvail != maxNumElems) {
        return true;
    } else {
        if (prev == next) {
            return true;  // 链表中只剩自己，保留
        }
        doNotDestroy = false;
        removeFromPool();
        return false;  // 通知调用方释放整个 Run
    }
}
```

#### 3.3.4 双向链表管理

每个 `smallSubpagePools[sizeIdx]` 维护一个循环双向链表：

```
head <-> SubPage_A <-> SubPage_B <-> SubPage_C <-> head
          (numAvail=3)  (numAvail=1)  (numAvail=5)
```

分配时从 head.next 获取第一个有空闲槽位的 SubPage。释放时如果 SubPage 从全满变为有空闲，重新插入链表头部。如果 SubPage 全部空闲且链表中还有其他 SubPage，则从链表中移除并标记为可销毁。

### 3.4 PoolChunkList -- Chunk 动态迁移

#### 3.4.1 使用率区间

六个 ChunkList 按使用率区间形成一个链表结构：

```
qInit  [0%, 25%]  -->  q000  [1%, 50%]  -->  q025  [25%, 75%]
                                                  |
q100  [100%, MAX] <-- q075  [75%, 100%] <-- q050  [50%, 100%]
```

注意 `q075` 的 nextList 指向 `q100`（不是 `q050`），`q050` 的 nextList 也指向 `q100`。

每个 ChunkList 的 `prevList` 指针用于释放时向下迁移：
```
q100.prevList = q075
q075.prevList = q050
q050.prevList = q025
q025.prevList = q000
q000.prevList = null      (usage==0 的 Chunk 直接销毁)
qInit.prevList = qInit    (qInit 比较特殊，usage==0 也不销毁)
```

#### 3.4.2 分配时的向上迁移

```java
boolean allocate(PooledByteBuf<T> buf, int reqCapacity, int sizeIdx, PoolThreadCache threadCache) {
    int normCapacity = arena.sizeClass.sizeIdx2size(sizeIdx);
    if (normCapacity > maxCapacity) {
        return false;  // 该 ChunkList 中的 Chunk 无法满足请求
    }

    for (PoolChunk<T> cur = head; cur != null; cur = cur.next) {
        if (cur.allocate(buf, reqCapacity, sizeIdx, threadCache)) {
            // 分配成功后，检查是否需要向上迁移
            if (cur.freeBytes <= freeMinThreshold) {
                remove(cur);
                nextList.add(cur);  // 移入使用率更高的 ChunkList
            }
            return true;
        }
    }
    return false;
}
```

#### 3.4.3 释放时的向下迁移

```java
boolean free(PoolChunk<T> chunk, long handle, int normCapacity, ByteBuffer nioBuffer) {
    chunk.free(handle, normCapacity, nioBuffer);
    // 释放后检查是否需要向下迁移
    if (chunk.freeBytes > freeMaxThreshold) {
        remove(chunk);
        return move0(chunk);  // 沿 prevList 向下寻找合适的 ChunkList
    }
    return true;
}
```

`move0()` 递归地沿 prevList 向下查找，直到找到使用率匹配的 ChunkList 或到达最底层（usage==0 时销毁 Chunk）。

#### 3.4.4 阈值计算

```java
// maxUsage == 100 时 freeMinThreshold == 0，表示不会向上迁移（已经是最高使用率区间）
freeMinThreshold = (maxUsage == 100) ? 0 : (int) (chunkSize * (100.0 - maxUsage + 0.99999999) / 100L);
// minUsage == 100 时 freeMaxThreshold == 0
freeMaxThreshold = (minUsage == 100) ? 0 : (int) (chunkSize * (100.0 - minUsage + 0.99999999) / 100L);
```

`0.99999999` 的偏移是为了对齐 `usage()` 方法中的取整逻辑，避免边界值错误。

### 3.5 PoolThreadCache -- 线程本地缓存

#### 3.5.1 缓存结构

```
PoolThreadCache
    +-- heapArena / directArena       // 绑定的 Arena
    +-- smallSubPageHeapCaches[]      // Small 堆缓存，每个 sizeIdx 一个队列
    +-- smallSubPageDirectCaches[]    // Small 直接内存缓存
    +-- normalHeapCaches[]            // Normal 堆缓存
    +-- normalDirectCaches[]          // Normal 直接内存缓存
    +-- freeSweepAllocationThreshold  // 触发 trim 的分配次数阈值 (默认 8192)
```

每个 `MemoryRegionCache` 内部是一个 MPSC (Multi-Producer Single-Consumer) 队列，存储 `Entry` 对象（包含 chunk + handle）。

#### 3.5.2 缓存分配

```java
// 从缓存分配
boolean allocateSmall(PoolArena<?> area, PooledByteBuf<?> buf, int reqCapacity, int sizeIdx) {
    return allocate(cacheForSmall(area, sizeIdx), buf, reqCapacity);
}

private boolean allocate(MemoryRegionCache<?> cache, PooledByteBuf buf, int reqCapacity) {
    if (cache == null) {
        return false;
    }
    boolean allocated = cache.allocate(buf, reqCapacity, this);
    // 每次分配后检查是否需要 trim
    if (++allocations >= freeSweepAllocationThreshold) {
        allocations = 0;
        trim();  // 释放不常使用的缓存条目
    }
    return allocated;
}
```

#### 3.5.3 缓存回收（释放 ByteBuf 时）

```java
// PooledByteBuf.deallocate()
protected final void deallocate() {
    if (handle >= 0) {
        final long handle = this.handle;
        this.handle = -1;
        memory = null;
        // free() 内部会先尝试放入 threadCache
        chunk.arena.free(chunk, tmpNioBuf, handle, maxLength, cache);
        tmpNioBuf = null;
        chunk = null;
        cache = null;
        this.recyclerHandle.unguardedRecycle(this);  // ByteBuf 对象本身也被回收
    }
}
```

#### 3.5.4 Trim 机制

```java
// MemoryRegionCache.trim()
public final void trim() {
    int free = size - allocations;  // 本次周期内未使用的缓存容量
    allocations = 0;
    if (free > 0) {
        free(free, false);  // 释放 free 个缓存条目
    }
}
```

trim 的触发条件有两个：
1. 每 `freeSweepAllocationThreshold` (默认 8192) 次分配触发一次
2. 定时任务 `DEFAULT_CACHE_TRIM_INTERVAL_MILLIS` 触发（默认 0，即不启用）

trim 的效果：如果某个缓存槽位在一段时间内没有被分配使用，就将其缓存的 Chunk/handle 归还给 Arena。

### 3.6 PooledByteBuf -- 池化字节缓冲区

#### 3.6.1 核心字段

```java
protected PoolChunk<T> chunk;    // 所属的 Chunk
protected long handle;           // 分配句柄（编码了 offset/size/isUsed/isSubpage/bitmapIdx）
protected T memory;              // 底层内存（byte[] 或 ByteBuffer）
protected int offset;            // 在 memory 中的起始偏移
protected int length;            // 逻辑容量（用户可见的 capacity）
int maxLength;                   // 实际分配的最大容量
PoolThreadCache cache;           // 绑定的线程缓存
ByteBuffer tmpNioBuf;            // 缓存的 NIO ByteBuffer 视图
```

#### 3.6.2 init() 与 initUnpooled()

```java
void init(PoolChunk<T> chunk, ByteBuffer nioBuffer, long handle, int offset,
          int length, int maxLength, PoolThreadCache cache, boolean threadLocal) {
    chunk.incrementPinnedMemory(maxLength);
    this.chunk = chunk;
    memory = chunk.memory;
    this.handle = handle;
    this.offset = offset;
    this.length = length;
    this.maxLength = maxLength;
    this.cache = cache;
    // ...
}

void initUnpooled(PoolChunk<T> chunk, int length) {
    // Huge 分配：offset=0, maxLength=length, 无 threadCache
    init0(chunk, null, 0, 0, length, length, null, false, false);
}
```

#### 3.6.3 capacity() 扩容/缩容

```java
public final ByteBuf capacity(int newCapacity) {
    if (!chunk.unpooled) {
        if (newCapacity > length) {
            if (newCapacity <= maxLength) {
                length = newCapacity;  // 在 maxLength 范围内，直接调整长度
                return this;
            }
        } else if (newCapacity > maxLength >>> 1 && ...) {
            length = newCapacity;  // 缩容但不浪费太多空间
            return this;
        }
    }
    // 需要重新分配
    chunk.arena.reallocate(this, newCapacity);
    return this;
}
```

maxLength 与 length 的区别：分配时可能获得比请求更大的内存区域（因为 sizeClass 对齐），maxLength 记录实际分配的大小，length 记录用户请求的大小。扩容时如果不超过 maxLength，只需调整 length 字段，无需重新分配。

## 四、ASCII 内存布局图

### 4.1 完整分配层次

```
PooledByteBufAllocator
 +-- Arena[0] (HeapArena)     Arena[1] (DirectArena)     ...Arena[N]
      |                            |
      +-- smallSubpagePools[]      +-- smallSubpagePools[]
      |   [0]: head <-> SP_A       |   [0]: head <-> SP_X
      |   [1]: head <-> SP_B       |   [1]: head
      |   ...                      |   ...
      |                            |
      +-- qInit  (0~25%)           +-- qInit
      |   +-- Chunk_1              |   +-- Chunk_5
      |   +-- Chunk_2              |
      +-- q000   (1~50%)           +-- q000
      |   +-- Chunk_3              |   +-- Chunk_6
      +-- q025   (25~75%)          +-- q025
      |   +-- Chunk_4              |
      +-- q050   (50~100%)         +-- q050
      +-- q075   (75~100%)         +-- q075
      +-- q100   (100%)            +-- q100


PoolChunk (16MB)
+--------+--------+--------+--------+-----+--------+
| Page 0 | Page 1 | Page 2 | Page 3 | ... |Page2047|
+--------+--------+--------+--------+-----+--------+
|<-- Run A (4 pages, used) -->|
                               |<- Run B (1p, free) ->|
                                        |<-- Run C (8 pages, free) ------->|

Run A 内部作为 SubPage 使用：
+------+------+------+------+------+------+------+------+------+
|elem 0|elem 1|elem 2|elem 3|elem 4|elem 5|elem 6|elem 7| ...  |
+------+------+------+------+------+------+------+------+------+
  used   free   used   free   free   used   free   free

bitmap: [0b 0 0 0 0 0 0 0 0 0 0 0 0 0 1 0 1 0 1 ]
                                bit: 7  6  5  4  3  2  1  0
                                     f  f  f  f  f  u  f  u   (u=used, f=free)
```

### 4.2 分配路径决策树

```
请求分配 N 字节
    |
    v
size2SizeIdx(N) -> sizeIdx
    |
    +-- sizeIdx <= smallMaxSizeIdx? --- YES --> Small 分配
    |                                          |
    |                                          +-- ThreadCache 命中? --> 直接返回
    |                                          |
    |                                          +-- smallSubpagePools[sizeIdx] 有空闲?
    |                                          |   |
    |                                          |   YES --> SubPage.allocate() -> bitmap 分配
    |                                          |   |
    |                                          |   NO --> Arena.lock() -> allocateNormal()
    |                                          |           -> allocateRun() -> allocateSubpage()
    |                                          |
    +-- sizeIdx < nSizes? ------------ YES --> Normal 分配
    |                                          |
    |                                          +-- ThreadCache 命中? --> 直接返回
    |                                          |
    |                                          +-- 遍历 q050/q025/q000/qInit/q075
    |                                          |   |
    |                                          |   命中 --> Chunk.allocateRun() -> splitLargeRun()
    |                                          |   |
    |                                          |   全部未命中 --> newChunk() -> qInit.add()
    |                                          |
    +-- 否则 -----------------------------> Huge 分配
                                               |
                                               +-- 直接 newUnpooledChunk()
```

## 五、设计思想

### 5.1 分层缓存策略

```
请求 --> [ThreadCache] --命中--> 返回
              |
            未命中
              |
              v
         [SubPagePool] --命中--> 返回
              |
            未命中
              |
              v
         [ChunkList] --命中--> 返回
              |
            未命中
              |
              v
         [new Chunk] --> 返回
```

每一层缓存都减少了对下一层的访问频率。ThreadCache 完全无锁，SubPagePool 只需锁定单个链表头，ChunkList 需要锁定整个 Arena。

### 5.2 伙伴系统思想

PoolChunk 的 Run 分配采用了类似伙伴系统（Buddy System）的思想：
- 分配时：从可用 Run 中找到最小满足需求的 Run，如果太大则拆分
- 释放时：检查相邻 Run 是否空闲，如果空闲则合并

但与经典伙伴系统不同的是，PoolChunk 使用 `runsAvailMap`（HashMap）而非固定数组来管理空闲 Run 的合并，这使得合并操作更加灵活。

### 5.3 Slab 分配器思想

PoolSubpage 的设计类似于 Linux 内核的 Slab 分配器：
- 将一个 Page 切分为固定大小的小对象
- 使用位图跟踪每个对象的分配状态
- 按对象大小分组管理（smallSubpagePools 数组），减少内部碎片

### 5.4 渐进式内存管理

PoolChunkList 的设计实现了渐进式内存管理：
- 使用率低的 Chunk 集中在 qInit/q000，便于查找和复用
- 使用率高的 Chunk 集中在 q075/q100，避免在接近满载的 Chunk 中浪费搜索时间
- Chunk 在 ChunkList 之间动态迁移，自动适应使用率变化

## 六、模块交互

### 6.1 分配时序图

```
PooledByteBufAllocator          PoolThreadCache         PoolArena           PoolChunk
       |                             |                     |                   |
       |-- newDirectBuffer() ------->|                     |                   |
       |   cache = threadCache.get() |                     |                   |
       |                             |                     |                   |
       |-- directArena.allocate() ----------------------->|                   |
       |                             |                     |                   |
       |                             |-- tcacheAllocateNormal()               |
       |                             |   cache.allocateNormal()               |
       |                             |   |                                     |
       |                             |   +-- cache 命中 -> return              |
       |                             |   |                                     |
       |                             |   +-- cache 未命中                      |
       |                             |       Arena.lock()                      |
       |                             |       allocateNormal()                  |
       |                             |           |                             |
       |                             |           +-- q050.allocate() -------->|
       |                             |           |       Chunk.allocate()     |
       |                             |           |           allocateRun()    |
       |                             |           |           splitLargeRun()  |
       |                             |           |       <----- handle -------|
       |                             |           |                             |
       |                             |           +-- initBuf() ------------->|
       |                             |           |       buf.init()          |
       |                             |           |       <------- buf --------|
       |                             |           |                             |
       |                             |           +-- cache.add()              |
       |   <---- return buf ---------+-----------+                             |
```

### 6.2 释放时序图

```
PooledByteBuf                    PoolArena              PoolThreadCache        PoolChunk
    |                               |                       |                    |
    |-- deallocate() ------------->|                       |                    |
    |   chunk.arena.free()         |                       |                    |
    |                               |                       |                    |
    |                               |-- free() ----------->|                    |
    |                               |   cache.add()        |                    |
    |                               |   |                  |                    |
    |                               |   +-- 缓存未满 -> 放入缓存，return        |
    |                               |   |                  |                    |
    |                               |   +-- 缓存已满 -> freeChunk()            |
    |                               |       Arena.lock()                        |
    |                               |       chunk.parent.free() ------------->|
    |                               |           chunk.free()                   |
    |                               |               collapseRuns()             |
    |                               |               insertAvailRun()           |
    |                               |           <-------------------------------|
    |                               |       检查是否需要 ChunkList 迁移         |
    |   recyclerHandle.recycle()    |                       |                    |
    |   (ByteBuf 对象回收)          |                       |                    |
```

## 七、关键流程详解

### 7.1 小内存分配完整流程 (Small Allocation)

以请求分配 24 字节为例（假设 pageSize=8192, smallMaxSizeIdx 对应 elemSize=4096）：

1. `size2SizeIdx(24)` 返回 sizeIdx，对应的 normalizeSize 为 32
2. 进入 `tcacheAllocateSmall()`
3. 尝试 `cache.allocateSmall()` -- 从 ThreadCache 的 `smallSubPageDirectCaches[sizeIdx]` 队列中 poll 一个 Entry
4. 如果缓存未命中，加锁 `smallSubpagePools[sizeIdx]` 的 head
5. 检查 `head.next != head`，即是否有活跃的 SubPage
6. 如果有，调用 `SubPage.allocate()` 从位图中找到空闲槽位
7. `initBufWithSubpage()` 计算实际内存偏移：`offset = (runOffset << pageShifts) + bitmapIdx * elemSize`
8. 如果没有活跃 SubPage，需要分配新的 Run 并初始化 SubPage

### 7.2 Run 合并算法详解

释放一个 Run 时的合并过程：

```
释放前状态：
  Run_A(offset=0, pages=4, used)    Run_B(offset=4, pages=2, free)
  Run_C(offset=6, pages=3, used)    Run_D(offset=9, pages=1, free)

释放 Run_C (offset=6, pages=3)：

1. collapsePast(handle):
   - 检查 offset-1=5 处 -> 找到 Run_B(offset=4, pages=2)
   - Run_B 结束位置 4+2=6 == 当前 offset 6 -> 连续！
   - 合并：新 handle = (offset=4, pages=2+3=5)
   - 继续检查 offset-1=3 -> Run_A 是 used -> 停止

2. collapseNext(handle):
   - 检查 offset+pages=4+5=9 处 -> 找到 Run_D(offset=9, pages=1)
   - 当前结束位置 4+5=9 == Run_D offset 9 -> 连续！
   - 合并：新 handle = (offset=4, pages=5+1=6)
   - 继续检查 offset+pages=4+6=10 -> 无 Run -> 停止

最终结果：
  Run_A(offset=0, pages=4, used)    Run_Merged(offset=4, pages=6, free)
```

### 7.3 ChunkList 迁移策略

迁移方向：
- **分配导致使用率上升**：从当前 ChunkList 移到 nextList（使用率更高的区间）
- **释放导致使用率下降**：从当前 ChunkList 移到 prevList（使用率更低的区间）

特殊规则：
- `qInit` 的 `prevList` 指向自己，意味着使用率降到 0 也不会被销毁（作为"保底"Chunk）
- `q000` 的 `prevList` 为 null，使用率降到 0 时 Chunk 会被销毁并释放内存

这确保了系统不会在低负载时持有过多空闲内存，同时保留最少一个 Chunk 供快速分配。

## 八、学习要点

### 8.1 核心设计模式

1. **分层缓存模式**：ThreadCache -> SubPagePool -> ChunkList -> OS，每一层都减少对下层的访问
2. **Slab 分配模式**：SubPage 将 Page 切分为固定大小的槽位，用位图管理
3. **伙伴系统变体**：Run 的分配与合并采用类似伙伴系统的思想
4. **Copy-on-Write 思想**：maxLength 与 length 的设计允许在一定范围内避免重新分配

### 8.2 锁优化策略

| 数据结构 | 锁类型 | 粒度 |
|---------|--------|------|
| PoolThreadCache | 无锁 | 线程独占 |
| smallSubpagePools[sizeIdx] | ReentrantLock | 单个 size 分类 |
| PoolChunk.runsAvail | ReentrantLock | 单个 Chunk |
| PoolArena | ReentrantLock | 单个 Arena |

锁的范围从细到粗：ThreadCache 无锁 -> SubPage 链表头锁 -> Chunk 级锁 -> Arena 级锁。越热的路径使用的锁越细粒度。

### 8.3 关键参数与调优

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `io.netty.allocator.numHeapArenas` | 2*CPU核数 | Heap Arena 数量 |
| `io.netty.allocator.numDirectArenas` | 2*CPU核数 | Direct Arena 数量 |
| `io.netty.allocator.pageSize` | 8192 | Page 大小 |
| `io.netty.allocator.maxOrder` | 9 | 最大阶数，chunkSize = pageSize << maxOrder = 16MB |
| `io.netty.allocator.smallCacheSize` | 256 | ThreadCache Small 缓存队列深度 |
| `io.netty.allocator.normalCacheSize` | 64 | ThreadCache Normal 缓存队列深度 |
| `io.netty.allocator.maxCachedBufferCapacity` | 32768 | ThreadCache 缓存的最大缓冲区容量 |
| `io.netty.allocator.cacheTrimInterval` | 8192 | 触发 trim 的分配次数 |

### 8.4 常见问题与排查

1. **内存泄漏**：PooledByteBuf 未调用 `release()` 会导致 Chunk 中的 Run/SubPage 无法回收
2. **内存碎片化**：频繁分配释放不同大小的内存可能导致 Chunk 内部碎片化
3. **Arena 锁争用**：Arena 数量不足时，多个线程会竞争同一个 Arena 的锁
4. **ThreadCache 膨胀**：大量短生命周期线程会导致 ThreadCache 频繁创建和销毁
