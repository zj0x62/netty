# 数据读写：Gathering/Scattering IO

## 一、概述

Netty 的数据读写是其作为网络框架的核心能力。与连接建立的"一次性"特性不同，数据读写是持续、高频的操作，其性能直接决定了框架的吞吐量。

本文深入分析 Netty NIO 传输层的数据读写实现，重点关注：

- **Scattering Read**：将数据从 Channel 读入多个 Buffer（Netty 中为单次循环多次读取）
- **Gathering Write**：将多个 Buffer 的数据一次性写出到 Channel
- **自适应缓冲区**：根据历史读取行为动态调整缓冲区大小
- **写缓冲区管理**：`ChannelOutboundBuffer` 的链表结构与流量控制

## 二、架构图

### 2.1 数据读取类体系

```
AbstractChannel
  |
  +-- AbstractNioChannel
       |
       +-- AbstractNioByteChannel (字节流 Channel)
            |
            +-- NioByteUnsafe (内部类，负责读取)
            |
            +-- NioSocketChannel
                 |
                 +-- doReadBytes()  -- 实际字节读取
                 +-- doWriteBytes() -- 实际字节写入
                 +-- doWrite()      -- 写入流程控制 (Gathering Write)
```

### 2.2 数据读取流程

```
Selector 检测到 OP_READ
  |
  +-- NioByteUnsafe.read()
       |
       +-- allocHandle.reset(config)          -- 重置自适应缓冲区
       |
       +-- do {                                -- 读取循环
       |     byteBuf = allocHandle.allocate()  -- 分配 ByteBuf
       |     doReadBytes(byteBuf)              -- SocketChannel.read()
       |     allocHandle.lastBytesRead(bytes)  -- 记录读取量
       |     pipeline.fireChannelRead(byteBuf) -- 触发事件
       |   } while (allocHandle.continueReading())
       |
       +-- allocHandle.readComplete()          -- 自适应调整
       +-- pipeline.fireChannelReadComplete()  -- 通知读取完成
```

### 2.3 数据写入流程

```
用户代码: ctx.write(msg)
  |
  +-- ChannelOutboundBuffer.addMessage(msg)
       |
用户代码: ctx.flush()
  |
  +-- ChannelOutboundBuffer.addFlush()
  |
  +-- NioSocketChannel.doWrite(ChannelOutboundBuffer)
       |
       +-- in.nioBuffers(1024, maxBytes)   -- 聚合为 NIO ByteBuffer[]
       |
       +-- switch (nioBufferCnt)
       |     case 0: doWrite0()            -- 非 ByteBuf 消息
       |     case 1: ch.write(buffer)      -- 单 Buffer 写入
       |     default: ch.write(buffers)    -- Gathering Write
       |
       +-- adjustMaxBytesPerGatheringWrite() -- 动态调整写入阈值
       +-- incompleteWrite() / clearOpWrite()
```

### 2.4 ChannelOutboundBuffer 数据结构

```
ChannelOutboundBuffer (环形链表)
  |
  +-- flushedEntry --> Entry --> Entry --> null
  |                  (已 flush, 待写出)
  +-- unflushedEntry --> Entry --> Entry --> null
  |                    (已添加, 未 flush)
  +-- tailEntry --> 指向最后一个 Entry
  |
  每个 Entry:
  +-- msg: Object (ByteBuf / FileRegion)
  +-- promise: ChannelPromise
  +-- progress: long (已写入进度)
  +-- total: long (总字节数)
  +-- pendingSize: int (占用内存大小)
  +-- cancelled: boolean
```

## 三、核心类分析

### 3.1 NioByteUnsafe.read() -- 数据读取引擎

**路径**: `transport/src/main/java/io/netty/channel/nio/AbstractNioByteChannel.java`

```java
@Override
public final void read() {
    final ChannelConfig config = config();
    if (shouldBreakReadReady(config)) {
        clearReadPending();
        return;
    }
    final ChannelPipeline pipeline = pipeline();
    final ByteBufAllocator allocator = config.getAllocator();
    final RecvByteBufAllocator.Handle allocHandle = recvBufAllocHandle();
    allocHandle.reset(config);

    ByteBuf byteBuf = null;
    boolean close = false;
    try {
        do {
            byteBuf = allocHandle.allocate(allocator);
            allocHandle.lastBytesRead(doReadBytes(byteBuf));
            if (allocHandle.lastBytesRead() <= 0) {
                byteBuf.release();
                byteBuf = null;
                close = allocHandle.lastBytesRead() < 0;
                if (close) {
                    readPending = false;
                }
                break;
            }

            allocHandle.incMessagesRead(1);
            readPending = false;
            pipeline.fireChannelRead(byteBuf);
            byteBuf = null;
        } while (allocHandle.continueReading());

        allocHandle.readComplete();
        pipeline.fireChannelReadComplete();

        if (close) {
            closeOnRead(pipeline);
        }
    } catch (Throwable t) {
        handleReadException(pipeline, byteBuf, t, close, allocHandle);
    } finally {
        if (!readPending && !config.isAutoRead()) {
            removeReadOp();
        }
    }
}
```

这是 Netty 读取数据的核心方法，逐行深度解析：

#### 前置检查

```java
if (shouldBreakReadReady(config)) {
    clearReadPending();
    return;
}
```

`shouldBreakReadReady()` 检查输入端是否已关闭（`shutdownInput`）。如果输入已关闭且不允许半关闭（`allowHalfClosure=false`），或者输入已关闭且之前已经看到过错误，则跳过读取。

#### 读取循环

```java
do {
    byteBuf = allocHandle.allocate(allocator);
    allocHandle.lastBytesRead(doReadBytes(byteBuf));
    if (allocHandle.lastBytesRead() <= 0) {
        byteBuf.release();
        byteBuf = null;
        close = allocHandle.lastBytesRead() < 0;
        break;
    }
    allocHandle.incMessagesRead(1);
    readPending = false;
    pipeline.fireChannelRead(byteBuf);
    byteBuf = null;
} while (allocHandle.continueReading());
```

循环的每一轮：

1. **`allocHandle.allocate(allocator)`**：通过自适应缓冲区分配器分配一个 `ByteBuf`。初始大小由 `AdaptiveRecvByteBufAllocator` 根据历史数据决定（默认 2048 字节）。

2. **`doReadBytes(byteBuf)`**：调用底层 `SocketChannel.read(ByteBuffer)` 将数据读入 `ByteBuf`。在 `NioSocketChannel` 中：
   ```java
   protected int doReadBytes(ByteBuf byteBuf) throws Exception {
       final RecvByteBufAllocator.Handle allocHandle = unsafe().recvBufAllocHandle();
       allocHandle.attemptedBytesRead(byteBuf.writableBytes());
       return byteBuf.writeBytes(javaChannel(), allocHandle.attemptedBytesRead());
   }
   ```
   - `attemptedBytesRead` 记录本次尝试读取的字节数
   - `byteBuf.writeBytes(javaChannel(), ...)` 调用 JDK `SocketChannel.read(ByteBuffer)`，返回实际读取的字节数

3. **返回值判断**：
   - `> 0`：成功读取了数据
   - `== 0`：没有数据可读（非阻塞模式下的正常情况），跳出循环
   - `< 0`：收到 EOF（对端关闭连接），标记 `close=true`

4. **`pipeline.fireChannelRead(byteBuf)`**：每读取到一批数据就立即触发 `channelRead` 事件，让 Pipeline 中的 Handler 及时处理。注意这里不是积累所有数据后再触发，而是**边读边触发**，降低延迟。

5. **`allocHandle.continueReading()`**：决定是否继续读取。这由自适应策略控制，通常会继续读取直到没有更多数据。

#### 读取完成

```java
allocHandle.readComplete();
pipeline.fireChannelReadComplete();
```

- **`readComplete()`**：通知自适应分配器本轮读取结束，用于更新缓冲区大小预测
- **`fireChannelReadComplete()`**：触发 `channelReadComplete` 事件。Handler 通常在此方法中执行一些"本轮读取完成后"的逻辑，如 flush 写缓冲区

#### 资源安全

`byteBuf = null` 的赋值出现在两个关键位置：
- 读取成功并触发 `channelRead` 后：此时 Pipeline 中的 Handler 已接管 ByteBuf 的引用计数
- 读取失败时：在 `break` 之前释放 ByteBuf

这确保了 ByteBuf 不会泄漏。

### 3.2 NioSocketChannel.doReadBytes() -- 字节读取实现

**路径**: `transport/src/main/java/io/netty/channel/socket/nio/NioSocketChannel.java`

```java
@Override
protected int doReadBytes(ByteBuf byteBuf) throws Exception {
    final RecvByteBufAllocator.Handle allocHandle = unsafe().recvBufAllocHandle();
    allocHandle.attemptedBytesRead(byteBuf.writableBytes());
    return byteBuf.writeBytes(javaChannel(), allocHandle.attemptedBytesRead());
}
```

这个方法虽然只有三行，但包含了关键的设计：

1. **`attemptedBytesRead(byteBuf.writableBytes())`**：记录尝试读取的字节数。这个值会被 `AdaptiveRecvByteBufAllocator` 用于判断是否需要增大缓冲区。
2. **`byteBuf.writeBytes(javaChannel(), ...)`**：`ByteBuf.writeBytes(ScatteringByteChannel, int)` 是 Netty `ByteBuf` 的方法，底层调用 `SocketChannel.read(ByteBuffer)`，将数据直接读入 `ByteBuf` 的可写区域。
3. **返回值**：实际读取的字节数，可能小于 `attemptedBytesRead`。

### 3.3 RecvByteBufAllocator -- 自适应缓冲区分配

**路径**: `transport/src/main/java/io/netty/channel/RecvByteBufAllocator.java`

`RecvByteBufAllocator` 是缓冲区分配策略的顶层接口：

```java
public interface RecvByteBufAllocator {
    Handle newHandle();

    interface Handle {
        ByteBuf allocate(ByteBufAllocator alloc);
        int guess();
        void reset(ChannelConfig config);
        void incMessagesRead(int numMessages);
        void lastBytesRead(int bytes);
        int lastBytesRead();
        void attemptedBytesRead(int bytes);
        int attemptedBytesRead();
        boolean continueReading();
        void readComplete();
    }

    interface ExtendedHandle extends Handle {
        boolean continueReading(UncheckedBooleanSupplier maybeMoreDataSupplier);
    }
}
```

`Handle` 接口的方法贯穿整个读取循环的生命周期：

| 方法 | 调用时机 | 作用 |
|------|---------|------|
| `reset(config)` | 读取循环开始前 | 重置状态，准备新一轮读取 |
| `allocate(alloc)` | 每次读取前 | 根据预测值分配 ByteBuf |
| `guess()` | 由 `allocate()` 内部调用 | 预测合适的缓冲区大小 |
| `attemptedBytesRead()` | `doReadBytes()` 中 | 记录尝试读取量 |
| `lastBytesRead()` | `doReadBytes()` 后 | 记录实际读取量 |
| `incMessagesRead()` | 每次成功读取后 | 累加消息计数 |
| `continueReading()` | 循环判断 | 决定是否继续读取 |
| `readComplete()` | 读取循环结束后 | 触发自适应调整 |

### 3.4 AdaptiveRecvByteBufAllocator -- 自适应策略

**路径**: `transport/src/main/java/io/netty/channel/AdaptiveRecvByteBufAllocator.java`

```java
public class AdaptiveRecvByteBufAllocator extends DefaultMaxMessagesRecvByteBufAllocator {

    public static final int DEFAULT_MINIMUM = 64;
    public static final int DEFAULT_INITIAL = 2048;
    public static final int DEFAULT_MAXIMUM = 65536;

    private final class HandleImpl extends MaxMessageHandle {
        private final AdaptiveCalculator calculator;

        HandleImpl(int minimum, int initial, int maximum) {
            calculator = new AdaptiveCalculator(minimum, initial, maximum);
        }

        @Override
        public void lastBytesRead(int bytes) {
            if (bytes == attemptedBytesRead()) {
                calculator.record(bytes);
            }
            super.lastBytesRead(bytes);
        }

        @Override
        public int guess() {
            return calculator.nextSize();
        }

        @Override
        public void readComplete() {
            calculator.record(totalBytesRead());
        }
    }
}
```

自适应策略的核心逻辑：

1. **初始值**：默认 2048 字节（大于常见 MTU 1500，避免首次读取就需要多次系统调用）
2. **增长条件**：当 `lastBytesRead == attemptedBytesRead` 时，说明缓冲区被填满，可能存在更多数据。`calculator.record(bytes)` 会记录这一事实，下次分配更大的缓冲区。
3. **缩减条件**：当连续两次读取的数据量低于缓冲区容量的一定比例时，缩小缓冲区
4. **边界**：最小 64 字节，最大 65536 字节
5. **`readComplete()`**：使用 `totalBytesRead()`（本轮读取的总字节数）更新预测器

`lastBytesRead()` 方法中的 `if (bytes == attemptedBytesRead())` 判断非常精妙：
- 如果实际读取量等于尝试读取量，说明缓冲区可能太小了（可能还有数据没读完），应该增大
- 如果实际读取量小于尝试读取量，说明数据已经读完，不需要调整（此时由 `readComplete()` 的整体统计来决定是否缩小）

### 3.5 ChannelOutboundBuffer -- 写缓冲区

**路径**: `transport/src/main/java/io/netty/channel/ChannelOutboundBuffer.java`

`ChannelOutboundBuffer` 是 Netty 写操作的核心数据结构，管理所有待写出的消息。

#### 核心数据结构

```java
// 链表结构:
// Entry(flushedEntry) --> ... Entry(unflushedEntry) --> ... Entry(tailEntry)
private Entry flushedEntry;     // 已 flush 链表的头节点
private Entry unflushedEntry;   // 未 flush 链表的头节点
private Entry tailEntry;        // 链表尾节点
private int flushed;            // 已 flush 的消息数量

private volatile long totalPendingSize;  // 总待写入字节数
private volatile int unwritable;         // 不可写标志位
```

`ChannelOutboundBuffer` 内部维护一个 **Entry 单向链表**，逻辑上分为两段：

```
[已 Flush 区域]                    [未 Flush 区域]
flushedEntry --> ... --> unflushedEntry --> ... --> tailEntry
                      |
                      +-- addFlush() 时将此区域标记为已 Flush
```

#### addMessage() -- 添加写消息

```java
public void addMessage(Object msg, int size, ChannelPromise promise) {
    Entry entry = Entry.newInstance(msg, size, total(msg), promise);
    if (tailEntry == null) {
        flushedEntry = null;
    } else {
        Entry tail = tailEntry;
        tail.next = entry;
    }
    tailEntry = entry;
    if (unflushedEntry == null) {
        unflushedEntry = entry;
    }

    // Touch for leak detection
    if (msg instanceof AbstractReferenceCountedByteBuf) {
        ((AbstractReferenceCountedByteBuf) msg).touch();
    } else {
        ReferenceCountUtil.touch(msg);
    }

    incrementPendingOutboundBytes(entry.pendingSize, false);
}
```

逐行解析：

1. **`Entry.newInstance()`**：从 `Recycler` 对象池中获取一个 `Entry` 对象，避免频繁创建对象。`total(msg)` 计算消息的总字节数。
2. **链表插入**：新 Entry 追加到链表尾部。如果链表为空，`flushedEntry` 设为 `null`（因为新消息是 unflushed 的）。
3. **`unflushedEntry` 维护**：如果这是第一条消息，`unflushedEntry` 指向它。
4. **`touch()`**：标记 ByteBuf 的访问，用于泄漏检测。
5. **`incrementPendingOutboundBytes()`**：增加待写入字节计数，可能触发不可写状态。

#### addFlush() -- 标记 Flush

```java
public void addFlush() {
    Entry entry = unflushedEntry;
    if (entry != null) {
        if (flushedEntry == null) {
            flushedEntry = entry;
        }
        do {
            flushed++;
            if (!entry.promise.setUncancellable()) {
                int pending = entry.cancel();
                decrementPendingOutboundBytes(pending, false, true);
            }
            entry = entry.next;
        } while (entry != null);
        unflushedEntry = null;
    }
}
```

`addFlush()` 将所有未 flush 的消息标记为已 flush：

1. **移动 flushedEntry**：如果 `flushedEntry` 为 null，将其指向当前 `unflushedEntry`
2. **遍历标记**：将 `unflushedEntry` 到 `tailEntry` 之间的所有 Entry 标记为已 flush（`flushed++`）
3. **处理取消**：如果某个 Entry 的 Promise 已被取消，释放其消息并减少待写入计数
4. **重置 unflushedEntry**：设为 null，表示没有未 flush 的消息

#### nioBuffers() -- 聚合 NIO Buffer

```java
public ByteBuffer[] nioBuffers(int maxCount, long maxBytes) {
    long nioBufferSize = 0;
    int nioBufferCount = 0;
    final InternalThreadLocalMap threadLocalMap = InternalThreadLocalMap.get();
    ByteBuffer[] nioBuffers = NIO_BUFFERS.get(threadLocalMap);
    Entry entry = flushedEntry;
    while (isFlushedEntry(entry) && entry.msg instanceof ByteBuf) {
        if (!entry.cancelled) {
            ByteBuf buf = (ByteBuf) entry.msg;
            final int readerIndex = buf.readerIndex();
            final int readableBytes = buf.writerIndex() - readerIndex;

            if (readableBytes > 0) {
                if (maxBytes - readableBytes < nioBufferSize && nioBufferCount != 0) {
                    break;
                }
                nioBufferSize += readableBytes;
                int count = entry.count;
                if (count == -1) {
                    entry.count = count = buf.nioBufferCount();
                }
                int neededSpace = min(maxCount, nioBufferCount + count);
                if (neededSpace > nioBuffers.length) {
                    nioBuffers = expandNioBufferArray(nioBuffers, neededSpace, nioBufferCount);
                    NIO_BUFFERS.set(threadLocalMap, nioBuffers);
                }
                if (count == 1) {
                    ByteBuffer nioBuf = entry.buf;
                    if (nioBuf == null) {
                        entry.buf = nioBuf = buf.internalNioBuffer(readerIndex, readableBytes);
                    }
                    nioBuffers[nioBufferCount++] = nioBuf;
                } else {
                    nioBufferCount = nioBuffers(entry, buf, nioBuffers, nioBufferCount, maxCount);
                }
                if (nioBufferCount >= maxCount) {
                    break;
                }
            }
        }
        entry = entry.next;
    }
    this.nioBufferCount = nioBufferCount;
    this.nioBufferSize = nioBufferSize;
    return nioBuffers;
}
```

这个方法是 **Gathering Write** 的关键。它将 `ChannelOutboundBuffer` 中的多个 `ByteBuf` 聚合为一个 `ByteBuffer[]` 数组，供 JDK `SocketChannel.write(ByteBuffer[])` 使用：

1. **ThreadLocal 缓冲区数组**：`NIO_BUFFERS` 是线程本地的 `ByteBuffer[]` 数组，避免每次写入都创建新数组。初始容量 1024。
2. **遍历已 Flush 的 Entry**：只处理 `flushedEntry` 到 `unflushedEntry` 之间的 Entry，且只处理 `ByteBuf` 类型的消息。
3. **大小限制**：
   - `maxCount`：最多聚合的 Buffer 数量（默认 1024）
   - `maxBytes`：最大聚合字节数（由 `SO_SNDBUF * 2` 决定）
4. **NIO Buffer 缓存**：`entry.buf` 缓存了转换后的 `ByteBuffer`，避免重复创建。对于复合 ByteBuf（`count > 1`），则缓存在 `entry.bufs` 数组中。
5. **数组扩容**：当需要的空间超过当前数组长度时，通过 `expandNioBufferArray()` 扩容（每次翻倍）。

#### removeBytes() -- 处理已写入数据

```java
public void removeBytes(long writtenBytes) {
    for (;;) {
        Object msg = current();
        if (!(msg instanceof ByteBuf)) {
            assert writtenBytes == 0;
            break;
        }

        final ByteBuf buf = (ByteBuf) msg;
        final int readerIndex = buf.readerIndex();
        final int readableBytes = buf.writerIndex() - readerIndex;

        if (readableBytes <= writtenBytes) {
            if (writtenBytes != 0) {
                progress(readableBytes);
                writtenBytes -= readableBytes;
            }
            remove();
        } else {
            if (writtenBytes != 0) {
                buf.readerIndex(readerIndex + (int) writtenBytes);
                progress(writtenBytes);
            }
            break;
        }
    }
    clearNioBuffers();
}
```

Gathering Write 完成后，`removeBytes()` 处理已写入的数据：

1. **完全写入的 ByteBuf**：`readableBytes <= writtenBytes` 时，整个 ByteBuf 已写入，调用 `remove()` 释放并通知 Promise。
2. **部分写入的 ByteBuf**：`readableBytes > writtenBytes` 时，更新 `readerIndex` 跳过已写入的部分，下次继续写入剩余数据。
3. **`clearNioBuffers()`**：清空 NIO Buffer 缓存，允许 GC 回收。

#### 流量控制 -- 写水位线

```java
private void incrementPendingOutboundBytes(long size, boolean invokeLater) {
    if (size == 0) return;
    long newWriteBufferSize = TOTAL_PENDING_SIZE_UPDATER.addAndGet(this, size);
    if (newWriteBufferSize > channel.config().getWriteBufferHighWaterMark()) {
        setUnwritable(invokeLater);
    }
}

private void decrementPendingOutboundBytes(long size, boolean invokeLater, boolean notifyWritability) {
    if (size == 0) return;
    long newWriteBufferSize = TOTAL_PENDING_SIZE_UPDATER.addAndGet(this, -size);
    if (notifyWritability && newWriteBufferSize < channel.config().getWriteBufferLowWaterMark()) {
        setWritable(invokeLater);
    }
}
```

Netty 通过高低水位线机制实现背压（Backpressure）：

- **高水位线（WriteBufferHighWaterMark）**：当 `totalPendingSize` 超过高水位线时，Channel 变为不可写，`isWritable()` 返回 `false`。
- **低水位线（WriteBufferLowWaterMark）**：当 `totalPendingSize` 降到低水位线以下时，Channel 恢复可写。
- **双水位线设计**：避免在阈值附近频繁切换状态（hysteresis）。
- **线程安全**：使用 `AtomicLongFieldUpdater` 进行原子更新，`isWritable()` 方法可以安全地从任意线程调用。

### 3.6 NioSocketChannel.doWrite() -- Gathering Write 实现

**路径**: `transport/src/main/java/io/netty/channel/socket/nio/NioSocketChannel.java`

```java
@Override
protected void doWrite(ChannelOutboundBuffer in) throws Exception {
    SocketChannel ch = javaChannel();
    int writeSpinCount = config().getWriteSpinCount();
    do {
        if (in.isEmpty()) {
            clearOpWrite();
            return;
        }

        int maxBytesPerGatheringWrite = ((NioSocketChannelConfig) config).getMaxBytesPerGatheringWrite();
        ByteBuffer[] nioBuffers = in.nioBuffers(1024, maxBytesPerGatheringWrite);
        int nioBufferCnt = in.nioBufferCount();

        switch (nioBufferCnt) {
            case 0:
                writeSpinCount -= doWrite0(in);
                break;
            case 1: {
                ByteBuffer buffer = nioBuffers[0];
                int attemptedBytes = buffer.remaining();
                final int localWrittenBytes = ch.write(buffer);
                if (localWrittenBytes <= 0) {
                    incompleteWrite(true);
                    return;
                }
                adjustMaxBytesPerGatheringWrite(attemptedBytes, localWrittenBytes, maxBytesPerGatheringWrite);
                in.removeBytes(localWrittenBytes);
                --writeSpinCount;
                break;
            }
            default: {
                long attemptedBytes = in.nioBufferSize();
                final long localWrittenBytes = ch.write(nioBuffers, 0, nioBufferCnt);
                if (localWrittenBytes <= 0) {
                    incompleteWrite(true);
                    return;
                }
                adjustMaxBytesPerGatheringWrite((int) attemptedBytes, (int) localWrittenBytes,
                        maxBytesPerGatheringWrite);
                in.removeBytes(localWrittenBytes);
                --writeSpinCount;
                break;
            }
        }
    } while (writeSpinCount > 0);

    incompleteWrite(writeSpinCount < 0);
}
```

这是 Netty 写入数据的核心方法，逐段解析：

#### 写入循环

```java
int writeSpinCount = config().getWriteSpinCount();  // 默认 16
do {
    // ... 写入操作
} while (writeSpinCount > 0);
```

Netty 采用**自旋写入**策略：在一次 `doWrite()` 调用中尝试写入多次（最多 `writeSpinCount` 次），避免每次写入都返回 EventLoop 再重新调度的开销。默认 16 次自旋。

#### Buffer 聚合

```java
int maxBytesPerGatheringWrite = ((NioSocketChannelConfig) config).getMaxBytesPerGatheringWrite();
ByteBuffer[] nioBuffers = in.nioBuffers(1024, maxBytesPerGatheringWrite);
int nioBufferCnt = in.nioBufferCount();
```

- `maxBytesPerGatheringWrite`：控制单次 Gathering Write 的最大字节数。初始值为 `SO_SNDBUF * 2`，后续根据实际写入情况动态调整。
- `in.nioBuffers()`：将已 flush 的 ByteBuf 聚合为 `ByteBuffer[]` 数组。

#### 三种写入路径

**Case 0: `nioBufferCnt == 0`**

```java
writeSpinCount -= doWrite0(in);
```

当没有 ByteBuf 可聚合时（例如队列中只有 `FileRegion`），回退到 `doWrite0()` 逐消息写入。

**Case 1: `nioBufferCnt == 1`**

```java
ByteBuffer buffer = nioBuffers[0];
int attemptedBytes = buffer.remaining();
final int localWrittenBytes = ch.write(buffer);
```

只有一个 ByteBuf 时，使用普通的 `SocketChannel.write(ByteBuffer)` 写入，避免 Gathering Write 的额外开销。

**Default: `nioBufferCnt >= 2` (Gathering Write)**

```java
long attemptedBytes = in.nioBufferSize();
final long localWrittenBytes = ch.write(nioBuffers, 0, nioBufferCnt);
```

多个 ByteBuf 时，使用 JDK 的 **Gathering Write**：`SocketChannel.write(ByteBuffer[], int, int)`。这会将多个 Buffer 的数据一次性交给操作系统，减少系统调用次数。

#### 自适应调整

```java
adjustMaxBytesPerGatheringWrite(attemptedBytes, localWrittenBytes, maxBytesPerGatheringWrite);
```

```java
private void adjustMaxBytesPerGatheringWrite(int attempted, int written, int oldMaxBytesPerGatheringWrite) {
    if (attempted == written) {
        if (attempted << 1 > oldMaxBytesPerGatheringWrite) {
            ((NioSocketChannelConfig) config).setMaxBytesPerGatheringWrite(attempted << 1);
        }
    } else if (attempted > MAX_BYTES_PER_GATHERING_WRITE_ATTEMPTED_LOW_THRESHOLD && written < attempted >>> 1) {
        ((NioSocketChannelConfig) config).setMaxBytesPerGatheringWrite(attempted >>> 1);
    }
}
```

`maxBytesPerGatheringWrite` 的动态调整逻辑：

- **全部写入** (`attempted == written`)：说明操作系统能处理更多数据，将阈值翻倍 (`attempted << 1`)
- **写入不足一半** (`written < attempted >>> 1`) 且尝试量大于 4KB：说明操作系统缓冲区已满，将阈值减半 (`attempted >>> 1`)
- **其他情况**：保持不变

这种自适应策略使 Netty 能够在不同负载和操作系统环境下自动找到最优的写入大小。

#### 写入未完成处理

```java
if (localWrittenBytes <= 0) {
    incompleteWrite(true);
    return;
}
```

当 `SocketChannel.write()` 返回 0 或负数时，说明操作系统发送缓冲区已满（`EAGAIN`），无法继续写入：

```java
protected final void incompleteWrite(boolean setOpWrite) {
    if (setOpWrite) {
        setOpWrite();
    } else {
        clearOpWrite();
        eventLoop().execute(flushTask);
    }
}
```

- **`setOpWrite()`**：向 Selector 注册 `OP_WRITE` 事件，当发送缓冲区有空间时 Selector 会通知
- **另一种路径** (`setOpWrite=false`)：清除 `OP_WRITE`，通过 `eventLoop().execute(flushTask)` 调度一次 flush 重试

```java
do {
    // ...
} while (writeSpinCount > 0);

incompleteWrite(writeSpinCount < 0);
```

当自旋次数用完但仍有数据未写出时，也会调用 `incompleteWrite()`，注册 `OP_WRITE` 等待下次写出机会。

### 3.7 AbstractNioByteChannel.doWrite() -- 非 Socket 场景的写入

**路径**: `transport/src/main/java/io/netty/channel/nio/AbstractNioByteChannel.java`

```java
@Override
protected void doWrite(ChannelOutboundBuffer in) throws Exception {
    int writeSpinCount = config().getWriteSpinCount();
    do {
        Object msg = in.current();
        if (msg == null) {
            clearOpWrite();
            return;
        }
        writeSpinCount -= doWriteInternal(in, msg);
    } while (writeSpinCount > 0);

    incompleteWrite(writeSpinCount < 0);
}
```

这是 `AbstractNioByteChannel` 层的默认 `doWrite()` 实现。`NioSocketChannel` 覆盖了此方法以实现 Gathering Write。基类版本逐消息写入：

```java
private int doWriteInternal(ChannelOutboundBuffer in, Object msg) throws Exception {
    if (msg instanceof ByteBuf) {
        ByteBuf buf = (ByteBuf) msg;
        if (!buf.isReadable()) {
            in.remove();
            return 0;
        }
        final int localFlushedAmount = doWriteBytes(buf);
        if (localFlushedAmount > 0) {
            in.progress(localFlushedAmount);
            if (!buf.isReadable()) {
                in.remove();
            }
            return 1;
        }
    } else if (msg instanceof FileRegion) {
        FileRegion region = (FileRegion) msg;
        if (region.transferred() >= region.count()) {
            in.remove();
            return 0;
        }
        long localFlushedAmount = doWriteFileRegion(region);
        if (localFlushedAmount > 0) {
            in.progress(localFlushedAmount);
            if (region.transferred() >= region.count()) {
                in.remove();
            }
            return 1;
        }
    }
    return WRITE_STATUS_SNDBUF_FULL;  // Integer.MAX_VALUE
}
```

返回值语义：
- `0`：空消息，跳过
- `1`：写入操作已执行（无论是否完全写入）
- `WRITE_STATUS_SNDBUF_FULL`：发送缓冲区满，无法写入

### 3.8 NioSocketChannel.doWriteBytes() -- 字节写入实现

```java
@Override
protected int doWriteBytes(ByteBuf buf) throws Exception {
    final int expectedWrittenBytes = buf.readableBytes();
    return buf.readBytes(javaChannel(), expectedWrittenBytes);
}
```

`ByteBuf.readBytes(WritableByteChannel, int)` 调用 JDK `SocketChannel.write(ByteBuffer)`，将 ByteBuf 中的可读字节写入 Channel。

### 3.9 filterOutboundMessage() -- 出站消息过滤

```java
@Override
protected final Object filterOutboundMessage(Object msg) {
    if (msg instanceof ByteBuf) {
        ByteBuf buf = (ByteBuf) msg;
        if (buf.isDirect()) {
            return msg;
        }
        return newDirectBuffer(buf);
    }
    if (msg instanceof FileRegion) {
        return msg;
    }
    throw new UnsupportedOperationException(
            "unsupported message type: " + StringUtil.simpleClassName(msg) + EXPECTED_TYPES);
}
```

在消息进入 `ChannelOutboundBuffer` 之前，`filterOutboundMessage()` 进行过滤：

1. **堆内存 ByteBuf**：转换为直接内存 ByteBuf。因为 NIO 的 `SocketChannel.write()` 需要直接内存 Buffer，使用堆内存会导致额外的拷贝。
2. **直接内存 ByteBuf**：直接使用
3. **FileRegion**：零拷贝文件传输，直接支持
4. **其他类型**：抛出 `UnsupportedOperationException`

`newDirectBuffer()` 方法（在 `AbstractNioChannel` 中）：

```java
protected final ByteBuf newDirectBuffer(ByteBuf buf) {
    final int readableBytes = buf.readableBytes();
    if (readableBytes == 0) {
        ReferenceCountUtil.safeRelease(buf);
        return Unpooled.EMPTY_BUFFER;
    }

    final ByteBufAllocator alloc = alloc();
    if (alloc.isDirectBufferPooled()) {
        ByteBuf directBuf = alloc.directBuffer(readableBytes);
        directBuf.writeBytes(buf, buf.readerIndex(), readableBytes);
        ReferenceCountUtil.safeRelease(buf);
        return directBuf;
    }

    final ByteBuf directBuf = ByteBufUtil.threadLocalDirectBuffer();
    if (directBuf != null) {
        directBuf.writeBytes(buf, buf.readerIndex(), readableBytes);
        ReferenceCountUtil.safeRelease(buf);
        return directBuf;
    }

    return buf;  // 池化直接内存不可用且线程本地分配也不可用，返回原始 buf
}
```

优先使用池化的直接内存，其次使用线程本地的直接内存，最后回退到原始 buf（此时 NIO 会自行拷贝）。

## 四、设计思想

### 4.1 读写分离的类层次

Netty 将 Channel 按读写模式分为两种：

| 类型 | 基类 | 特征 | 典型实现 |
|------|------|------|---------|
| 字节流 Channel | `AbstractNioByteChannel` | 数据以字节流形式读写 | `NioSocketChannel` |
| 消息 Channel | `AbstractNioMessageChannel` | 数据以消息为单位读写 | `NioServerSocketChannel`、`NioDatagramChannel` |

字节流 Channel 使用 `doReadBytes()`/`doWriteBytes()` 操作连续的字节流。消息 Channel 使用 `doReadMessages()`/`doWriteMessage()` 操作离散的消息。

### 4.2 边读边触发 vs 批量触发

读取流程的一个重要设计决策是**边读边触发 ChannelRead**：

```java
do {
    byteBuf = allocHandle.allocate(allocator);
    allocHandle.lastBytesRead(doReadBytes(byteBuf));
    // ...
    pipeline.fireChannelRead(byteBuf);  // 每次读取立即触发
} while (allocHandle.continueReading());
```

对比 `NioMessageUnsafe` 的批量触发模式，`NioByteUnsafe` 选择在每次读取后立即触发 `channelRead`。这带来了：

- **更低的延迟**：数据不必等到整轮读取完成才被处理
- **更好的流式处理**：Handler 可以边接收边处理
- **内存效率**：不需要在 readBuf 列表中积累数据

### 4.3 自旋写入

```java
int writeSpinCount = config().getWriteSpinCount();  // 默认 16
```

Netty 采用自旋写入策略的原因：

1. **减少上下文切换**：一次 `doWrite()` 调用中尝试多次写入，避免每次写入都经历 `EventLoop -> Selector -> EventLoop` 的调度周期
2. **避免饥饿**：限制自旋次数（默认 16），防止单个 Channel 的写入占用过多 EventLoop 时间
3. **优雅退出**：当自旋次数用完但仍有数据时，通过 `incompleteWrite()` 注册 `OP_WRITE`，在下一次 Selector 循环中继续

### 4.4 零拷贝与聚合写

Gathering Write 的核心价值：

```
传统方式（N 次系统调用）:
  write(buf1) -> syscall
  write(buf2) -> syscall
  write(buf3) -> syscall

Gathering Write（1 次系统调用）:
  write(buf1, buf2, buf3) -> syscall
```

Netty 通过 `ChannelOutboundBuffer.nioBuffers()` 将多个 ByteBuf 聚合为 `ByteBuffer[]`，然后调用 `SocketChannel.write(ByteBuffer[])`，减少系统调用次数。

### 4.5 背压（Backpressure）机制

```
写入速度 >> 发送速度
  |
  +-- totalPendingSize > WriteBufferHighWaterMark
  |     |
  |     +-- Channel.isWritable() = false
  |     +-- Handler 应该暂停写入
  |
  +-- totalPendingSize < WriteBufferLowWaterMark
        |
        +-- Channel.isWritable() = true
        +-- Handler 可以恢复写入
```

典型的背压使用模式：

```java
public void channelRead(ChannelHandlerContext ctx, Object msg) {
    ctx.write(msg, ctx.voidPromise());
    if (!ctx.channel().isWritable()) {
        // 暂停读取上游数据
        ctx.channel().config().setAutoRead(false);
    }
}

public void channelWritabilityChanged(ChannelHandlerContext ctx) {
    if (ctx.channel().isWritable()) {
        // 恢复读取上游数据
        ctx.channel().config().setAutoRead(true);
    }
}
```

## 五、模块交互

### 5.1 读取过程中的模块交互

```
NioEventLoop
  |
  +-- Selector 检测到 OP_READ
  +-- AbstractNioUnsafe.handle(READ)
  |     |
  |     +-- NioByteUnsafe.read()
  |           |
  |           +-- RecvByteBufAllocator.Handle     -- 缓冲区管理
  |           |     +-- allocate() -> ByteBufAllocator  -- 内存分配
  |           |     +-- guess() -> AdaptiveCalculator   -- 大小预测
  |           |
  |           +-- NioSocketChannel.doReadBytes()  -- 实际 I/O
  |           |     +-- SocketChannel.read(ByteBuffer)
  |           |
  |           +-- Pipeline.fireChannelRead()      -- 事件传播
  |           |     +-- HeadContext
  |           |     +-- Decoder
  |           |     +-- BusinessHandler
  |           |
  |           +-- Pipeline.fireChannelReadComplete()
```

### 5.2 写入过程中的模块交互

```
用户代码: ctx.write(msg)
  |
  +-- AbstractChannel.write()
  |     |
  |     +-- Pipeline.filterOutboundMessage()      -- 消息过滤
  |     |     +-- 堆内存 -> 直接内存转换
  |     |
  |     +-- TailContext.write()                   -- Pipeline 传播
  |           +-- Encoder
  |           +-- HeadContext
  |                 |
  |                 +-- ChannelOutboundBuffer.addMessage()
  |
用户代码: ctx.flush()
  |
  +-- AbstractChannel.flush()
  |     +-- HeadContext.flush()
  |           +-- ChannelOutboundBuffer.addFlush()
  |           +-- AbstractUnsafe.flush0()
  |                 +-- NioSocketChannel.doWrite()
  |                       |
  |                       +-- ChannelOutboundBuffer.nioBuffers()  -- 聚合
  |                       +-- SocketChannel.write(ByteBuffer[])   -- Gathering Write
  |                       +-- ChannelOutboundBuffer.removeBytes() -- 清理
  |
  +-- 若未完全写出: 注册 OP_WRITE
  +-- Selector 检测到 OP_WRITE: 重新调用 doWrite()
```

### 5.3 ChannelOutboundBuffer 的生命周期

```
write(msg1)  --> addMessage(msg1) --> [unflushed: msg1]
write(msg2)  --> addMessage(msg2) --> [unflushed: msg1 -> msg2]
write(msg3)  --> addMessage(msg3) --> [unflushed: msg1 -> msg2 -> msg3]

flush()      --> addFlush()       --> [flushed: msg1 -> msg2 -> msg3]
                doWrite()
                  nioBuffers()    --> ByteBuffer[]{buf1, buf2, buf3}
                  ch.write()      --> 写入 300 字节
                  removeBytes(300)
                    msg1 全部写入  --> remove(), 通知 Promise
                    msg2 部分写入  --> 更新 readerIndex

下次 OP_WRITE --> doWrite()
                  nioBuffers()    --> ByteBuffer[]{msg2剩余, buf3}
                  ch.write()      --> 写入剩余
                  removeBytes()
                    msg2 全部写入  --> remove()
                    msg3 全部写入  --> remove()
                  clearOpWrite()
```

## 六、关键流程详解

### 6.1 完整的读取流程

以一次数据到达为例：

**阶段一：事件触发**
1. 网卡接收到数据，操作系统通知 Selector
2. `NioEventLoop.run()` 中 `Selector.select()` 返回
3. `processSelectedKey()` 检测到 `OP_READ`
4. 调用 `AbstractNioUnsafe.handle()`，分发到 `read()`

**阶段二：循环读取**
1. `allocHandle.reset(config)` 重置本轮读取状态
2. 第一轮：`allocate()` 分配 2048 字节 ByteBuf，`doReadBytes()` 读取 1500 字节（一个 MSS）
3. `continueReading()` 返回 true，继续读取
4. 第二轮：分配 2048 字节 ByteBuf，读取 800 字节
5. `continueReading()` 返回 true，继续读取
6. 第三轮：`doReadBytes()` 返回 0（没有更多数据），跳出循环

**阶段三：完成处理**
1. `allocHandle.readComplete()`：更新自适应计算器
2. `pipeline.fireChannelReadComplete()`：通知 Pipeline
3. `allocHandle.guess()` 可能调整下次分配大小（如果两次都读满了缓冲区，可能增大到 4096）

### 6.2 完整的写入流程

以发送一个 HTTP 响应为例：

**阶段一：Write**
```java
ctx.write(responseHead);  // HTTP 响应头
ctx.write(responseBody);  // HTTP 响应体
```

每次 `write()` 调用：
1. `filterOutboundMessage()` 检查消息类型，必要时转换为直接内存
2. `ChannelOutboundBuffer.addMessage()` 将消息加入链表
3. `incrementPendingOutboundBytes()` 更新待写入计数

**阶段二：Flush**
```java
ctx.flush();
```

1. `ChannelOutboundBuffer.addFlush()` 将所有消息标记为已 flush
2. `doWrite()` 开始写入循环
3. `in.nioBuffers(1024, maxBytes)` 聚合 ByteBuf 为 `ByteBuffer[]`
4. 如果有 2+ 个 ByteBuffer，执行 `SocketChannel.write(ByteBuffer[])`
5. `adjustMaxBytesPerGatheringWrite()` 根据写入结果调整阈值
6. `in.removeBytes(written)` 处理已写入的数据
7. 如果自旋次数用完，`incompleteWrite()` 注册 `OP_WRITE`

**阶段三：异步续写**
1. Selector 检测到 `OP_WRITE`（发送缓冲区有空间）
2. 再次调用 `doWrite()`，继续写入剩余数据
3. 所有数据写入完成后，`clearOpWrite()` 移除 `OP_WRITE`

### 6.3 自适应缓冲区的调整过程

```
轮次 1: guess=2048, attempted=2048, read=2048 (填满)
         --> lastBytesRead == attemptedBytesRead
         --> calculator.record(2048), 预测增长
         --> readComplete(), totalBytesRead=2048

轮次 2: guess=4096, attempted=4096, read=3000
         --> lastBytesRead < attemptedBytesRead (未填满)
         --> readComplete(), totalBytesRead=3000
         --> 如果连续未填满，预测缩小

轮次 3: guess=2048, attempted=2048, read=100
         --> readComplete(), totalBytesRead=100
         --> 持续缩小直到 DEFAULT_MINIMUM(64)

高吞吐场景:
         guess 持续增长直到 DEFAULT_MAXIMUM(65536)
         减少系统调用次数，提高吞吐量
```

### 6.4 异常处理

#### 读取异常

```java
private void handleReadException(ChannelPipeline pipeline, ByteBuf byteBuf,
        Throwable cause, boolean close, RecvByteBufAllocator.Handle allocHandle) {
    if (byteBuf != null) {
        if (byteBuf.isReadable()) {
            readPending = false;
            pipeline.fireChannelRead(byteBuf);  // 尝试传递已有数据
        } else {
            byteBuf.release();
        }
    }
    allocHandle.readComplete();
    pipeline.fireChannelReadComplete();
    pipeline.fireExceptionCaught(cause);

    if (close || cause instanceof OutOfMemoryError
            || cause instanceof LeakPresenceDetector.AllocationProhibitedException
            || cause instanceof IOException) {
        closeOnRead(pipeline);
    }
}
```

读取异常时的处理策略：
1. 如果 ByteBuf 中有已读取的数据，先传递给 Pipeline（尽可能不丢失数据）
2. 触发 `readComplete` 和 `exceptionCaught`
3. 对于严重错误（OOM、IO 异常），关闭连接
4. 对于 `allowHalfClosure`，只关闭输入端（`shutdownInput`），保持输出端开放

#### 写入异常 -- 关闭时的处理

```java
void close(final Throwable cause, final boolean allowChannelOpen) {
    inFail = true;
    // 释放所有未 flush 的消息
    Entry e = unflushedEntry;
    while (e != null) {
        int size = e.pendingSize;
        TOTAL_PENDING_SIZE_UPDATER.addAndGet(this, -size);
        if (!e.cancelled) {
            ReferenceCountUtil.safeRelease(e.msg);
            safeFail(e.promise, cause);
        }
        e = e.unguardedRecycleAndGetNext();
    }
    inFail = false;
}
```

Channel 关闭时，`ChannelOutboundBuffer.close()` 释放所有未 flush 的消息，通知所有 Promise 失败。

## 七、学习要点

### 7.1 性能优化技巧

| 优化点 | 实现方式 | 效果 |
|--------|---------|------|
| Gathering Write | 聚合多个 ByteBuf 为 `ByteBuffer[]` | 减少系统调用次数 |
| 自旋写入 | 一次 `doWrite()` 中多次尝试写入 | 减少 EventLoop 调度开销 |
| 自适应缓冲区 | 根据历史读取调整缓冲区大小 | 平衡内存占用和系统调用次数 |
| maxBytesPerGatheringWrite | 根据实际写入动态调整阈值 | 适配不同操作系统的写入特性 |
| 直接内存 | NIO 写入时避免堆内存拷贝 | 减少一次内存拷贝 |
| Entry 对象池 | `Recycler` 复用 Entry 对象 | 减少 GC 压力 |
| ThreadLocal ByteBuffer[] | 缓存 NIO Buffer 数组 | 避免频繁创建数组 |

### 7.2 写入路径的选择

```
doWrite() 被调用
  |
  +-- in.nioBuffers() 聚合
  |
  +-- nioBufferCnt == 0
  |     |
  |     +-- 非 ByteBuf 消息 (FileRegion)
  |     +-- doWrite0() 逐消息写入
  |
  +-- nioBufferCnt == 1
  |     |
  |     +-- SocketChannel.write(ByteBuffer)     -- 普通写入
  |
  +-- nioBufferCnt >= 2
        |
        +-- SocketChannel.write(ByteBuffer[])   -- Gathering Write
```

### 7.3 流量控制的工程实践

1. **不要忽略 `isWritable()` 事件**：这是 Netty 提供背压信号的唯一途径
2. **在 `channelWritabilityChanged()` 中恢复/暂停读取**：避免 OOM
3. **`write()` + `voidPromise()` 批量写入**：减少 Promise 创建开销
4. **在 `channelRead()` 中直接 write**：减少 EventLoop 线程切换

### 7.4 关键数字

| 参数 | 默认值 | 含义 |
|------|--------|------|
| `WriteSpinCount` | 16 | 单次 `doWrite()` 的最大自旋次数 |
| `maxBytesPerGatheringWrite` | `SO_SNDBUF * 2` | 单次 Gathering Write 的最大字节数 |
| `DEFAULT_INITIAL` (RecvByteBuf) | 2048 | 自适应缓冲区的初始大小 |
| `DEFAULT_MINIMUM` (RecvByteBuf) | 64 | 自适应缓冲区的最小值 |
| `DEFAULT_MAXIMUM` (RecvByteBuf) | 65536 | 自适应缓冲区的最大值 |
| `WriteBufferHighWaterMark` | 65536 | 写高水位线（默认 64KB） |
| `WriteBufferLowWaterMark` | 32768 | 写低水位线（默认 32KB） |
| `NIO_BUFFERS` 初始容量 | 1024 | ThreadLocal ByteBuffer[] 数组大小 |

### 7.5 常见陷阱

1. **忘记 flush**：`write()` 只是将消息加入缓冲区，不调用 `flush()` 消息不会被发送
2. **堆内存 ByteBuf 直接 write**：会触发 `newDirectBuffer()` 拷贝，应尽量使用直接内存
3. **不处理背压**：大量写入不检查 `isWritable()` 会导致 OOM
4. **在非 EventLoop 线程调用 write**：虽然线程安全，但会增加调度延迟
5. **FileRegion 和 ByteBuf 混用**：`ChannelOutboundBuffer.nioBuffers()` 只处理 ByteBuf，遇到 FileRegion 会回退到单消息写入
