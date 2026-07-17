# Linux io_uring 原生传输

## 概述

Netty 的 io_uring 传输是基于 Linux 5.1+ 内核引入的新一代异步 I/O 接口 `io_uring` 的实现。与 epoll 的"就绪通知"模型不同，io_uring 采用"完成通知"模型——提交 I/O 请求后内核异步执行，完成后通过完成队列通知应用程序。这种模型可以实现真正的零系统调用开销（批量提交/批量收割），并原生支持零拷贝发送。

**核心模块**: `transport-classes-io_uring`

**最低内核要求**: Linux 5.1+（推荐 6.x+ 以获得完整特性支持）

**关键特性**:
- 提交队列 (SQ) / 完成队列 (CQ) 共享内存，减少系统调用
- 支持 `IORING_OP_SEND_ZC` / `IORING_OP_SENDMSG_ZC` 零拷贝发送
- 支持 Multi-shot 模式（一次提交持续产生完成事件）
- 支持 Buffer Ring（预注册缓冲区，减少内存分配）
- 支持 Splice 管道操作
- 支持 TCP Fast Open
- 全部 I/O 操作异步化（connect、accept、read、write、close 等）

## 架构图

```
                    +-------------------------------+
                    |       EventLoopGroup          |
                    |  (IoUringEventLoopGroup)       |
                    +----------+--------------------+
                               |
                    +----------v--------------------+
                    |     IoUringEventLoop           |
                    |  (SingleThreadIoEventLoop)     |
                    +----------+--------------------+
                               |
                    +----------v--------------------+
                    |     IoUringIoHandler           |
                    |  (IoHandler 实现)               |
                    |                                |
                    |  ringBuffer: RingBuffer        |
                    |    +-- SubmissionQueue (SQ)     |
                    |    +-- CompletionQueue  (CQ)    |
                    |  eventfd    : FileDescriptor   |
                    |  iovArray   : IovArray         |
                    |  registrations: IntObjectMap   |
                    |  pendingOps : PendingOpMap     |
                    +----------+--------------------+
                               |
            +------------------+------------------+
            |                                     |
  +---------v----------+            +-------------v-----------+
  |IoUringSocketChannel|            |IoUringServerSocketChannel|
  +---------+----------+            +-------------+-----------+
            |                                     |
  +---------v----------+            +-------------v-----------+
  |AbstractIoUringChannel|          |AbstractIoUringServerChannel|
  +---------+----------+            +-------------+-----------+
            |                                     |
  +---------v-------------------------------------v----------+
  |                      LinuxSocket                          |
  +----------------------------------------------------------+
            |
  +---------v------------------------------------------------+
  |                      Native                               |
  |  io_uring_setup / io_uring_enter / io_uring_register     |
  +----------------------------------------------------------+
            |
  +---------v------------------------------------------------+
  |            netty_transport_native_io_uring.so             |
  |  (C/JNI 实现, 调用 io_uring 系统调用)                     |
  +----------------------------------------------------------+
```

## 核心类分析

### 1. IoUringIoHandler — 事件循环核心

`IoUringIoHandler` 是 io_uring 传输的 I/O 处理器，管理提交队列和完成队列。

**关键字段**:

```java
public final class IoUringIoHandler implements IoHandler {
    private final RingBuffer ringBuffer;           // SQ + CQ 的共享内存环
    private final IntObjectMap<IoUringBufferRing> registeredIoUringBufferRing; // 预注册缓冲区环
    private final IntObjectMap<DefaultIoUringIoRegistration> registrations;     // 注册表
    private final FileDescriptor eventfd;          // 唤醒用 eventfd
    private final IovArray iovArray;               // scatter/gather I/O 数组
    private final MsgHdrMemoryArray msgHdrMemoryArray; // sendmsg 头数组
    private final PendingOpMap pendingOps;         // 慢路径操作映射
    private int nextRegistrationId;                // 下一个注册 ID
}
```

**SQ/CQ 环形缓冲区模型**:

```
+------------------------------------------+
|           RingBuffer (共享内存)            |
|                                          |
|  +------------------------------------+  |
|  | SubmissionQueue (SQ)               |  |
|  |  SQE[0] SQE[1] SQE[2] ... SQE[n]  |  |
|  |  head ────────────────────> tail    |  |
|  +------------------------------------+  |
|                                          |
|  +------------------------------------+  |
|  | CompletionQueue (CQ)               |  |
|  |  CQE[0] CQE[1] CQE[2] ... CQE[m]  |  |
|  |  head ────────────────────> tail    |  |
|  +------------------------------------+  |
|                                          |
+------------------------------------------+

SQ: 应用程序写入 SQE (Submission Queue Entry)
CQ: 内核写入 CQE (Completion Queue Entry)
两者共享内存，通过 mmap 映射到用户空间
```

**初始化过程**:

```java
IoUringIoHandler(ThreadAwareExecutor executor, IoUringIoHandlerConfig config) {
    IoUring.ensureAvailability();
    int setupFlags = Native.setupFlags(config.singleIssuer());

    // 创建 RingBuffer (SQ + CQ)
    int cqSize = 2 * config.getRingSize();
    if (config.needSetupCqeSize()) {
        setupFlags |= Native.IORING_SETUP_CQSIZE;
        cqSize = config.getCqSize();
    }
    this.ringBuffer = Native.createRingBuffer(config.getRingSize(), cqSize, setupFlags);

    // 注册 Buffer Ring（预分配缓冲区）
    for (IoUringBufferRingConfig bufferRingConfig : config.getInternBufferRingConfigs()) {
        IoUringBufferRing ring = newBufferRing(ringBuffer.fd(), bufferRingConfig);
        registeredIoUringBufferRing.put(bufferRingConfig.bufferGroupId(), ring);
    }

    // 创建 eventfd 用于唤醒
    eventfd = Native.newBlockingEventFd();
}
```

**核心事件循环 `run()` 方法**:

```java
public int run(IoHandlerContext context) {
    SubmissionQueue submissionQueue = ringBuffer.ioUringSubmissionQueue();
    CompletionQueue completionQueue = ringBuffer.ioUringCompletionQueue();

    if (!completionQueue.hasCompletions() && context.canBlock()) {
        // 没有待处理的完成事件且可以阻塞
        if (eventfdReadSubmitted == 0) {
            submitEventFdRead();  // 提交 eventfd 读请求，用于唤醒
        }
        // 提交所有待处理的 SQE 并等待
        submitAndWaitWithTimeout(submissionQueue, false, timeoutNanos);
    } else {
        // 有完成事件或不能阻塞，立即提交并返回
        submitAndClearNow(submissionQueue);
    }

    // 处理完成队列中的事件
    int ioCompletions = processCompletionsAndHandleOverflow(
            submissionQueue, completionQueue, this::handle);
    return ioCompletions;
}
```

**完成事件处理**:

```java
private boolean handle(int res, int flags, long udata, ByteBuffer extraCqeData) {
    if (udata == EVENTFD_TOKEN) {
        handleEventFdRead();   // eventfd 唤醒事件
        return false;
    }
    if (udata == RINGFD_TOKEN) {
        return false;          // 超时/NOP 事件
    }
    if (udata >= 0) {
        handleFastPath(res, flags, udata, extraCqeData);   // 快速路径
        return true;
    }
    handleSlowPath(res, flags, udata, extraCqeData);       // 慢速路径
    return true;
}
```

**快速路径 vs 慢速路径**:

```
submit(IoOps)
    |
    +--> canUseFastPath(userData)?
    |       |
    |       +--> YES: UserData.encode(id, op, userData)
    |       |         直接将 id + op + userData 打包为 64 位
    |       |         写入 SQE 的 user_data 字段
    |       |         CQE 返回时直接解包
    |       |
    |       +--> NO: pendingOps.nextToken()
    |                 注册到 PendingOpMap
    |                 token 写入 SQE 的 user_data 字段
    |                 CQE 返回时通过 PendingOpMap 查找
```

**唤醒机制**:

```java
public void wakeup() {
    if (!executor.isExecutorThread(Thread.currentThread()) &&
        !eventfdAsyncNotify.getAndSet(true)) {
        // 使用 AtomicBoolean 防止重复唤醒
        // 使用 wakeupWriters 计数器防止关闭时竞态
        int s;
        do {
            s = wakeupWriters.get();
            if ((s & WAKEUP_CLOSED) != 0) return;  // 已关闭，放弃唤醒
        } while (!wakeupWriters.compareAndSet(s, s + 1));
        try {
            Native.eventFdWrite(eventfd.intValue(), 1L);
        } finally {
            wakeupWriters.decrementAndGet();
        }
    }
}
```

### 2. AbstractIoUringChannel — 通道基类

io_uring 通道的核心特点是**全部操作异步化**:

```java
abstract class AbstractIoUringChannel extends AbstractChannel implements UnixChannel {
    final LinuxSocket socket;

    // I/O 状态位掩码
    private static final int POLL_IN_SCHEDULED  = 1;
    private static final int POLL_OUT_SCHEDULED = 1 << 2;
    private static final int POLL_RDHUP_SCHEDULED = 1 << 3;
    private static final int WRITE_SCHEDULED    = 1 << 4;
    private static final int READ_SCHEDULED     = 1 << 5;
    private static final int CONNECT_SCHEDULED  = 1 << 6;

    private byte ioState;                // 当前 I/O 状态组合
    private short numOutstandingWrites;  // 待完成的写操作数
    private short numOutstandingReads;   // 待完成的读操作数（-1 表示 multi-shot）
}
```

**与 epoll 的根本区别**:

| 操作 | epoll (就绪通知) | io_uring (完成通知) |
|------|-----------------|-------------------|
| 读 | 通知"可读" -> 应用 read() | 提交 RECV -> 内核完成后通知 |
| 写 | 通知"可写" -> 应用 write() | 提交 SEND -> 内核完成后通知 |
| 接受连接 | 通知"有连接" -> 应用 accept() | 提交 ACCEPT -> 内核完成后通知 |
| 连接 | 应用 connect() -> 通知完成 | 提交 CONNECT -> 内核完成后通知 |
| 关闭 | 应用 close() | 提交 CLOSE -> 内核完成后通知 |

**异步关闭机制**:

```java
protected void doClose() throws Exception {
    active = false;
    if (registration != null) {
        if (socket.markClosed()) {
            int fd = fd().intValue();
            // 提交异步关闭操作，而非直接 close()
            IoUringIoOps ops = IoUringIoOps.newClose(fd, (byte) 0, nextOpsId());
            registration.submit(ops);
        }
    } else {
        // 未注册则直接关闭
        socket.close();
    }
}
```

**延迟关闭机制**:

io_uring 中可能存在未完成的读/写操作，关闭通道需要等待这些操作完成:

```java
protected void close(ChannelPromise promise, Throwable cause, ClosedChannelException closeCause) {
    // 记录延迟关闭信息
    delayedClose = new DelayedClose(promise, cause, closeCause);

    // 取消所有未完成的操作
    cancelOps(cancelConnect);

    // 检查是否可以立即关闭
    if (canCloseNow()) {
        closeNow();
    }
}

private boolean canCloseNow() {
    return canCloseNow0() && (ioState & (WRITE_SCHEDULED | READ_SCHEDULED)) == 0;
}
```

### 3. IoUringSocketChannel — 零拷贝写入

`IoUringSocketChannel` 实现了基于 io_uring 的零拷贝写入，这是 epoll/kqueue 无法提供的能力。

**零拷贝写入核心逻辑**:

```java
// IoUringSocketUnsafe
private static final Object ZC_BATCH_MARKER = new Object(); // 批次标记

protected int scheduleWriteSingle(Object msg) {
    if (IoUring.isSendZcSupported() && msg instanceof ByteBuf) {
        ByteBuf buf = (ByteBuf) msg;
        int length = buf.readableBytes();
        if (config.shouldWriteZeroCopy(length)) {
            long address = IoUring.memoryAddress(buf) + buf.readerIndex();
            // 使用 IORING_OP_SEND_ZC 替代普通 write
            IoUringIoOps ops = IoUringIoOps.newSendZc(fd().intValue(), address, length, 0, nextOpsId(), 0);
            writeId = registration().submit(ops);
            writeOpCode = ops.opcode();
            return 1;
        }
    }
    return super.scheduleWriteSingle(msg);
}
```

**零拷贝写入的双通知机制**:

io_uring 的零拷贝发送会产生两个 CQE:
1. 第一个 CQE: 发送完成（`IORING_CQE_F_MORE` 标志表示还有后续通知）
2. 第二个 CQE: 内核释放了对缓冲区的引用（`IORING_CQE_F_NOTIF` 标志）

```java
boolean writeComplete0(byte op, int res, int flags, short data, int outstanding) {
    if (op == Native.IORING_OP_SEND_ZC || op == Native.IORING_OP_SENDMSG_ZC) {
        return handleWriteCompleteZeroCopy(op, channelOutboundBuffer, res, flags);
    }
    return super.writeComplete0(op, res, flags, data, outstanding);
}

private boolean handleWriteCompleteZeroCopy(byte op, ChannelOutboundBuffer out, int res, int flags) {
    if ((flags & Native.IORING_CQE_F_NOTIF) == 0) {
        // 第一个通知：发送完成
        boolean more = (flags & Native.IORING_CQE_F_MORE) != 0;
        if (more) {
            // 保留缓冲区，等待第二个通知
            if (zcWriteQueue == null) {
                zcWriteQueue = new ArrayDeque<>(8);
            }
            // 将已发送的 ByteBuf 加入队列并 retain
            do {
                ByteBuf currentBuffer = (ByteBuf) out.current();
                zcWriteQueue.add(currentBuffer);
                currentBuffer.retain();  // 防止被释放
                // ... 处理已发送字节数
            } while (res > 0);
            zcWriteQueue.add(ZC_BATCH_MARKER);  // 批次结束标记
        } else {
            // 不会有第二个通知，直接释放
            out.removeBytes(res);
        }
    } else {
        // 第二个通知：内核已释放缓冲区引用
        if (zcWriteQueue != null) {
            for (;;) {
                Object queued = zcWriteQueue.remove();
                if (queued == ZC_BATCH_MARKER) break;
                ((ByteBuf) queued).release();  // 现在可以安全释放
            }
        }
    }
    return true;
}
```

**零拷贝 vs 普通发送对比**:

```
普通发送 (write/send):
  用户空间 Buffer --copy--> 内核 Socket Buffer --copy--> 网卡
  两次内存拷贝

零拷贝发送 (send_zc):
  用户空间 Buffer ------> 网卡 (内核直接引用用户缓冲区)
  零次内存拷贝
  但需要双 CQE 通知来管理缓冲区生命周期
```

### 4. Multi-shot 模式

io_uring 支持 Multi-shot 模式，一次提交可以持续产生完成事件:

**Multi-shot Accept**:
```
提交 ACCEPT (带 IORING_ACCEPT_MULTISHOT)
    |
    +--> 客户端 A 连接 -> CQE (res=fdA)
    +--> 客户端 B 连接 -> CQE (res=fdB)
    +--> 客户端 C 连接 -> CQE (res=fdC)
    +--> ... 持续产生，直到被取消
```

**Multi-shot Recv**:
```
提交 RECV (带 multi-shot 标志)
    |
    +--> 数据到达 -> CQE (res=N bytes, IORING_CQE_F_MORE)
    +--> 更多数据 -> CQE (res=M bytes, IORING_CQE_F_MORE)
    +--> 连接关闭 -> CQE (res=0, 无 IORING_CQE_F_MORE)
    |
    IORING_CQE_F_MORE: 还会有更多 CQE
    无此标志: 本次 multi-shot 结束，需要重新提交
```

**Buffer Ring**:

Buffer Ring 是 io_uring 的高级特性，预注册一组缓冲区给内核，内核在接收数据时直接选择空闲缓冲区:

```
+-------------------+
|   Buffer Ring      |
|                    |
|  buf[0]: 已使用    |  <-- 内核自动选择
|  buf[1]: 空闲      |
|  buf[2]: 空闲      |
|  buf[3]: 已使用    |
|  ...               |
+-------------------+

优势:
- 不需要每次 recv 前分配缓冲区
- 内核直接写入预注册缓冲区
- 通过 bgId 标识不同的缓冲区组
```

### 5. IoUring — 特性检测

`IoUring` 类在静态初始化时检测内核支持的所有特性:

```java
public final class IoUring {
    static {
        // 检测内核版本
        kernelVersion = Native.kernelVersion();
        Native.checkKernelVersion(kernelVersion);

        // 创建探测用 ring buffer
        ringBuffer = Native.createRingBuffer(1, 0);

        // 探测各项特性
        socketNonEmptySupported = Native.isCqeFSockNonEmptySupported(probe);
        spliceSupported = Native.isSpliceSupported(probe);
        sendZcSupported = Native.isSendZcSupported(probe);
        sendmsgZcSupported = Native.isSendmsgZcSupported(probe);
        acceptMultishotSupported = Native.isAcceptMultishotSupported(probe);
        recvMultishotSupported = Native.isRecvMultishotSupported();
        recvsendBundleSupported = (features & Native.IORING_FEAT_RECVSEND_BUNDLE) != 0;
        // ... 更多特性检测
    }
}
```

**关键特性列表**:

| 特性 | 说明 | 最低内核版本 |
|------|------|------------|
| `FEAT_SUBMIT_STABLE` | SQE 提交后内容稳定 | 5.4 |
| `SEND_ZC` | 零拷贝发送 | 6.0 |
| `SENDMSG_ZC` | 零拷贝 sendmsg | 6.0 |
| `ACCEPT_MULTISHOT` | 多次接受连接 | 6.0 |
| `RECV_MULTISHOT` | 多次接收数据 | 6.0 |
| `RECVSEND_BUNDLE` | 批量收发 | 6.10 |
| `POLL_ADD_MULTISHOT` | 多次轮询 | 6.0 |
| `REGISTER_BUFFER_RING` | 缓冲区环 | 6.0 |
| `SETUP_SINGLE_ISSUER` | 单一提交者优化 | 6.0 |
| `SETUP_DEFER_TASKRUN` | 延迟任务运行 | 6.1 |
| `CQE_F_SOCK_NONEMPTY` | socket 非空标志 | 6.0 |
| `FEAT_NO_IOWAIT` | 不计入 iowait | 6.6 |

## 设计思想

### 1. 就绪通知 vs 完成通知

```
epoll (就绪通知模型):
  应用 --[关注事件]--> epoll
  epoll --[事件就绪]--> 应用
  应用 --[执行 I/O]--> 内核
  内核 --[I/O 结果]--> 应用

  总计: 2 次系统调用 (epoll_wait + read/write)

io_uring (完成通知模型):
  应用 --[提交 SQE]--> SQ
  内核 --[执行 I/O]--> 内核
  内核 --[写入 CQE]--> CQ
  应用 --[收割 CQE]--> CQ

  总计: 可以 0 次系统调用 (SQ/CQ 共享内存)
  实际: io_uring_enter() 批量提交/收割
```

### 2. 批量处理减少系统调用

io_uring 的核心优势是批量操作:
- **批量提交**: 多个 SQE 写入 SQ 后，一次 `io_uring_enter()` 提交
- **批量收割**: 多个 CQE 在 CQ 中，直接读取无需系统调用
- **内核轮询**: `IORING_SETUP_SQPOLL` 模式下内核线程轮询 SQ，完全无需系统调用

### 3. 异步一切

与 epoll 只能异步等待"就绪"不同，io_uring 将所有 I/O 操作异步化:
- `IORING_OP_ACCEPT`: 异步接受连接
- `IORING_OP_CONNECT`: 异步建立连接
- `IORING_OP_RECV` / `IORING_OP_SEND`: 异步收发数据
- `IORING_OP_CLOSE`: 异步关闭 fd
- `IORING_OP_SPLICE`: 异步 splice 管道操作
- `IORING_OP_SEND_ZC`: 异步零拷贝发送

### 4. 注册机制的差异

io_uring 使用 `int registrationId` 而非 fd 作为注册表键，因为:
- 同一个 fd 可能有多个并发 I/O 操作
- 需要区分同一个 fd 的不同操作（读/写/accept 等）
- 使用 UserData 打包 `id + op + userData` 实现高效路由

### 5. 安全的缓冲区生命周期管理

零拷贝写入需要确保缓冲区在内核使用期间不被释放:
```
发送请求 -> retain(ByteBuf)
    |
第一个 CQE (F_MORE): 内核正在使用缓冲区
    |
第二个 CQE (F_NOTIF): 内核已释放引用 -> release(ByteBuf)
```

## 模块交互

```
+------------------------------------------+
|        IoUringEventLoopGroup             |
+------+------+-+--------------------------+
       |      |
       v      v
+------+  +---+------+
| Loop |  |  Loop    |  每个线程独立的 io_uring 实例
|  #0  |  |  #1     |
+--+---+  +---+------+
   |          |
   v          v
IoUringIoHandler  IoUringIoHandler
   |                  |
   v                  v
RingBuffer_0     RingBuffer_1
   |                  |
   +-- SQ (提交队列)  +-- SQ
   +-- CQ (完成队列)  +-- CQ
   |
   +--- registrations (id -> Channel 映射)
   |
   +--- pendingOps (慢路径操作映射)
   |
   +--- registeredBufferRing (预注册缓冲区)
   |
   +--- processCompletions()
         |
         +---> handle(res, flags, udata)
               |
               +---> DefaultIoUringIoRegistration.handle()
                     |
                     +---> AbstractUringUnsafe.handle()
                           |
                           +-- RECV/ACCEPT -> readComplete()
                           +-- SEND/WRITE  -> writeComplete()
                           +-- POLL_ADD    -> pollAddComplete()
                           +-- CONNECT     -> connectComplete()
                           +-- CLOSE       -> 关闭完成
```

## 关键流程

### 接受连接流程（Multi-shot Accept）

```
IoUringServerSocketChannel.doBind()
    |
    +--> socket.listen(backlog)
    +--> [EventLoop 注册后]
    |
AbstractIoUringServerChannel.doBeginRead()
    |
    +--> 提交 IORING_OP_ACCEPT (带 MULTISHOT 标志)
    |
    [内核异步等待连接]
    |
    [客户端 A 连接]
CQE: (res=fdA, IORING_CQE_F_MORE)
    |
    +--> newChildChannel(fdA, address)
    +--> pipeline.fireChannelRead(childChannelA)
    |
    [客户端 B 连接]
CQE: (res=fdB, IORING_CQE_F_MORE)
    |
    +--> newChildChannel(fdB, address)
    +--> pipeline.fireChannelRead(childChannelB)
    |
    ... 持续产生，不需要重新提交
```

### 异步读取流程

```
用户调用 channel.read()
    |
    +--> doBeginRead()
    |       +--> readPending = true
    |       +--> isPollInFirst()?
    |               |
    |               +--> YES: schedulePollIn()
    |               |       提交 POLL_ADD(POLLIN)
    |               |       等待 POLLIN 事件后提交 RECV
    |               |
    |               +--> NO: scheduleFirstRead()
    |                       直接提交 RECV
    |
    [内核异步接收数据]
    |
CQE: (res=N bytes)
    |
    +--> readComplete()
    |       +--> ByteBuf.writerIndex += N
    |       +--> pipeline.fireChannelRead(byteBuf)
    |       +--> pipeline.fireChannelReadComplete()
    |
    +--> 用户是否继续读?
            |
            +--> YES: 继续提交 RECV
            +--> NO: 不再提交，等待下次 read()
```

### 异步连接流程

```
用户调用 channel.connect(remote)
    |
    +--> 有 TFO 数据?
    |       |
    |       +--> YES: 提交 SENDMSG (MSG_FASTOPEN)
    |       |         在 SYN 中携带数据
    |       |
    |       +--> NO: 提交 CONNECT
    |
    +--> ioState |= CONNECT_SCHEDULED
    +--> scheduleConnectTimeout()
    |
    [内核异步建立连接]
    |
CQE: (res == 0, 连接成功)
    |
    +--> connectComplete()
    |       +--> fulfillConnectPromise()
    |       +--> schedulePollRdHup()  // 监听半关闭
    |       +--> pipeline.fireChannelActive()
    |
CQE: (res == EINPROGRESS, 连接进行中)
    |
    +--> schedulePollOut()  // 等待 POLLOUT 后检查连接状态
```

### 零拷贝发送流程

```
用户调用 channel.write(largeBuf).flush()
    |
    +--> scheduleWriteSingle(largeBuf)
    |       |
    |       +--> shouldWriteZeroCopy(length)?
    |               |
    |               +--> YES: 提交 IORING_OP_SEND_ZC
    |               |         地址 = ByteBuf.memoryAddress()
    |               |
    |               +--> NO: 提交普通 IORING_OP_SEND
    |
    [内核异步零拷贝发送]
    |
CQE #1: (res=bytesSent, IORING_CQE_F_MORE)
    |      发送完成，但内核仍持有缓冲区
    |      retain(ByteBuf) 加入 zcWriteQueue
    |
CQE #2: (IORING_CQE_F_NOTIF)
    |      内核释放缓冲区引用
    |      release(ByteBuf) 从 zcWriteQueue 移除
    |
    +--> outboundBuffer.removeBytes(bytesSent)
    +--> 刷出剩余数据（如有）
```

## 学习要点

1. **完成通知模型的优势**: io_uring 的核心优势是将 I/O 操作的"提交"和"完成"分离，实现了真正的异步 I/O。epoll 只是通知"就绪"，实际 I/O 还是同步的。

2. **零拷贝的代价**: 零拷贝发送 (`SEND_ZC`) 需要双 CQE 通知机制来管理缓冲区生命周期。第一个 CQE 通知发送完成（缓冲区仍被内核引用），第二个 CQE 通知内核释放了引用。

3. **Multi-shot 的价值**: 一次提交持续产生完成事件，避免了反复提交的系统调用开销。特别适合 accept 和 recv 这类高频操作。

4. **Buffer Ring 的预分配**: 通过 `io_uring_register(IORING_REGISTER_BUFFERS)` 预注册缓冲区，内核可以直接使用，避免了每次 I/O 的缓冲区分配。

5. **快慢路径分离**: io_uring 的 UserData 打包机制 (`id + op + userData`) 实现了零额外开销的完成事件路由。当 userData 超出 short 范围时才走慢路径的 PendingOpMap。

6. **延迟关闭**: io_uring 的异步特性使得关闭操作也需要异步化。必须等待所有未完成的 I/O 操作完成（或取消）后才能安全关闭 fd。

7. **特性检测**: io_uring 的特性集随内核版本不断扩展，Netty 在初始化时通过 `io_uring_probe` 检测所有可用特性，按需启用。

8. **与 epoll/kqueue 的对比**:
   - epoll: "告诉你 fd 可以读了，你自己去读"（就绪通知）
   - kqueue: "告诉你过滤器满足条件了，你自己去操作"（就绪通知）
   - io_uring: "你把请求给我，我帮你做完了告诉你结果"（完成通知）

9. **Java 9+ 要求**: io_uring 传输要求 Java 9+，因为需要使用 `Buffer.wrapMemoryAddress` 等直接内存操作 API。

10. **初始化开销**: io_uring 的初始化比 epoll 复杂得多，包括创建 RingBuffer、探测内核特性、注册 Buffer Ring 等。但运行时的 I/O 效率远高于 epoll。
