# Netty SSL/TLS 处理器深度分析

## 一、概述

Netty 的 SSL/TLS 子系统是其网络安全通信的核心基础设施，位于 `io.netty.handler.ssl` 包下。该子系统以 `SslHandler` 为核心处理器，通过 `SslContext` 抽象工厂模式屏蔽了 JDK SSL 和 OpenSSL 两种底层实现的差异，为上层应用提供统一的加密通信接口。

核心设计目标：
- **零拷贝优化**：充分利用 Netty 的 `ByteBuf` 体系，减少内存拷贝
- **异步非阻塞**：握手、加解密全程异步，不阻塞 EventLoop 线程
- **多实现可切换**：通过 `SslProvider` 枚举实现 JDK SSL / OpenSSL / Conscrypt 的无缝切换
- **协议协商**：内置 ALPN/NPN 支持，为 HTTP/2 等协议协商提供基础
- **StartTLS 支持**：允许在明文连接中途升级为加密连接

## 二、架构总览

### 2.1 类层次结构

```
SslContext (抽象工厂)
├── JdkSslContext                    -- JDK 原生 SSL 实现
│   ├── JdkSslClientContext
│   └── JdkSslServerContext
├── OpenSslContext                   -- OpenSSL 实现 (Finalizer 回收)
│   ├── OpenSslClientContext
│   └── OpenSslServerContext
└── ReferenceCountedOpenSslContext   -- OpenSSL 实现 (引用计数回收)
    ├── ReferenceCountedOpenSslClientContext
    └── ReferenceCountedOpenSslServerContext

SslHandler (核心处理器)
├── extends ByteToMessageDecoder     -- 入站解码
└── implements ChannelOutboundHandler -- 出站加密

SSLEngine (JDK 标准接口)
├── JdkSslEngine                     -- JDK 引擎装饰器
├── ReferenceCountedOpenSslEngine    -- OpenSSL 引擎
└── ConscryptAlpnSslEngine           -- Conscrypt 引擎

SslClientHelloHandler<T>            -- ClientHello 拦截器 (SNI 路由)
├── extends ByteToMessageDecoder
└── implements ChannelOutboundHandler
```

### 2.2 Pipeline 中的位置

```
┌─────────────────────────────────────────────────────┐
│                    ChannelPipeline                    │
│                                                       │
│  Head → ... → SslHandler → [业务 Handler] → ... → Tail│
│                                                       │
│  SslHandler 同时作为:                                  │
│    - ByteToMessageDecoder (入站: unwrap 解密)          │
│    - ChannelOutboundHandler (出站: wrap 加密)          │
└─────────────────────────────────────────────────────┘
```

### 2.3 数据流向

```
写入方向 (出站):
  应用数据 ByteBuf
    → SslHandler.write()        -- 暂存到 pendingUnencryptedWrites
    → SslHandler.flush()        -- 触发 wrap 操作
    → SSLEngine.wrap()          -- 加密为 TLS 记录
    → ctx.write(encryptedBuf)   -- 写入下层传输

读取方向 (入站):
  网络数据 ByteBuf
    → SslHandler.decode()       -- 触发 unwrap 操作
    → SSLEngine.unwrap()        -- 解密 TLS 记录
    → ctx.fireChannelRead()     -- 传递明文数据给上层
```

## 三、核心类分析

### 3.1 SslHandler -- 核心处理器

**文件位置**: `handler/src/main/java/io/netty/handler/ssl/SslHandler.java`

SslHandler 是 Netty SSL 子系统的核心，它同时实现了入站解码和出站加密两个方向的处理。其类声明体现了这一双重角色：

```java
public class SslHandler extends ByteToMessageDecoder
                        implements ChannelOutboundHandler {
```

#### 3.1.1 状态管理

SslHandler 使用位掩码 (`short state`) 管理复杂的状态机：

```java
private static final int STATE_SENT_FIRST_MESSAGE     = 1;       // StartTLS 首消息已发送
private static final int STATE_FLUSHED_BEFORE_HANDSHAKE = 1 << 1; // 握手前有 flush 请求
private static final int STATE_READ_DURING_HANDSHAKE   = 1 << 2;  // 握手期间有 read 请求
private static final int STATE_HANDSHAKE_STARTED       = 1 << 3;  // 握手已启动
private static final int STATE_NEEDS_FLUSH             = 1 << 4;  // 需要 flush
private static final int STATE_OUTBOUND_CLOSED         = 1 << 5;  // 出站已关闭
private static final int STATE_CLOSE_NOTIFY            = 1 << 6;  // close_notify 已处理
private static final int STATE_PROCESS_TASK            = 1 << 7;  // 正在处理委托任务
private static final int STATE_FIRE_CHANNEL_READ       = 1 << 8;  // 需要触发 channelRead
private static final int STATE_UNWRAP_REENTRY          = 1 << 9;  // unwrap 重入标记
```

状态操作方法：
```java
private boolean isStateSet(int bit) { return (state & bit) == bit; }
private void setState(int bit)      { state |= bit; }
private void clearState(int bit)    { state &= ~bit; }
```

使用位掩码而非 `boolean` 标志的好处：
1. **原子性**：单个 `short` 变量即可表示所有状态
2. **内存效率**：10 个布尔标志只需 2 字节
3. **组合查询**：可同时检查多个状态位

#### 3.1.2 SslEngineType 策略枚举

`SslEngineType` 是策略模式的经典应用，它封装了不同 SSLEngine 实现的差异：

```java
private enum SslEngineType {
    TCNATIVE(true, COMPOSITE_CUMULATOR) { ... },   // OpenSSL
    CONSCRYPT(true, COMPOSITE_CUMULATOR) { ... },   // Conscrypt
    JDK(false, MERGE_CUMULATOR) { ... };            // JDK 原生
}
```

每种类型需要实现以下抽象方法：

| 方法 | 职责 |
|------|------|
| `unwrap()` | 解密操作，不同引擎对多 ByteBuffer 的支持不同 |
| `allocateWrapBuffer()` | 分配加密输出缓冲区，OpenSSL 可精确计算大小 |
| `calculateRequiredOutBufSpace()` | 计算加密所需输出空间 |
| `calculatePendingData()` | 计算待处理数据量，OpenSSL 可查询 SSL 层缓冲 |
| `jdkCompatibilityMode()` | 是否使用 JDK 兼容模式解析记录头 |

关键差异：

```java
// OpenSSL: 支持 scatter/gather I/O，使用 CompositeByteBuf 避免拷贝
TCNATIVE(true, COMPOSITE_CUMULATOR) {
    SSLEngineResult unwrap(...) {
        // 直接使用 nioBuffers() 数组，无需合并
        result = opensslEngine.unwrap(in.nioBuffers(...), handler.singleBuffer);
    }
}

// JDK: 只支持单 ByteBuffer，必须使用 MERGE_CUMULATOR 合并碎片
JDK(false, MERGE_CUMULATOR) {
    SSLEngineResult unwrap(...) {
        // 必须合并为单个 ByteBuffer
        ByteBuffer inNioBuffer = toByteBuffer(in, ...);
        result = handler.engine.unwrap(inNioBuffer, toByteBuffer(out, ...));
    }
}
```

#### 3.1.3 关键字段

```java
private final SSLEngine engine;                          // 底层 SSL 引擎
private final SslEngineType engineType;                  // 引擎类型策略
private final Executor delegatedTaskExecutor;            // 委托任务执行器
private final boolean jdkCompatibilityMode;              // JDK 兼容模式标志
private final boolean startTls;                          // StartTLS 模式标志
private final ResumptionController resumptionController; // 会话恢复控制器

private SslHandlerCoalescingBufferQueue pendingUnencryptedWrites; // 待加密写入队列
private Promise<Channel> handshakePromise;               // 握手完成 Promise
private final LazyChannelPromise sslClosePromise;        // SSL 关闭 Promise

private volatile long handshakeTimeoutMillis = 10000;    // 握手超时 (默认 10 秒)
private volatile long closeNotifyFlushTimeoutMillis = 3000;  // close_notify 刷新超时
private volatile long closeNotifyReadTimeoutMillis;      // close_notify 读取超时
volatile int wrapDataSize = MAX_PLAINTEXT_LENGTH;        // 单次 wrap 数据大小 (16KB)
```

#### 3.1.4 构造函数

```java
SslHandler(SSLEngine engine, boolean startTls, Executor delegatedTaskExecutor,
           ResumptionController resumptionController) {
    this.engine = ObjectUtil.checkNotNull(engine, "engine");
    this.delegatedTaskExecutor = ObjectUtil.checkNotNull(delegatedTaskExecutor, "delegatedTaskExecutor");
    engineType = SslEngineType.forEngine(engine);  // 根据引擎类型选择策略
    this.startTls = startTls;
    this.jdkCompatibilityMode = engineType.jdkCompatibilityMode(engine);
    setCumulator(engineType.cumulator);  // 设置累积器 (MERGE 或 COMPOSITE)
    this.resumptionController = resumptionController;
}
```

`SslEngineType.forEngine()` 通过 `instanceof` 检测引擎类型：
```java
static SslEngineType forEngine(SSLEngine engine) {
    return engine instanceof ReferenceCountedOpenSslEngine ? TCNATIVE :
           engine instanceof ConscryptAlpnSslEngine ? CONSCRYPT : JDK;
}
```

### 3.2 握手流程

#### 3.2.1 握手启动

握手在以下时机启动：

1. **客户端模式**：`channelActive()` 或 `handlerAdded()` 时自动启动
2. **服务端模式**：收到客户端 ClientHello 后由 SSLEngine 驱动
3. **手动重协商**：调用 `renegotiate()` 方法

```java
@Override
public void channelActive(final ChannelHandlerContext ctx) throws Exception {
    setOpensslEngineSocketFd(ctx.channel());
    if (!startTls) {
        startHandshakeProcessing(true);  // 非 StartTLS 模式立即启动握手
    }
    ctx.fireChannelActive();
}

private void startHandshakeProcessing(boolean flushAtEnd) {
    if (!isStateSet(STATE_HANDSHAKE_STARTED)) {
        setState(STATE_HANDSHAKE_STARTED);
        if (engine.getUseClientMode()) {
            handshake(flushAtEnd);  // 客户端主动发起握手
        }
        applyHandshakeTimeout();    // 设置握手超时
    }
}
```

#### 3.2.2 handshake() 方法

```java
private void handshake(boolean flushAtEnd) {
    if (engine.getHandshakeStatus() != HandshakeStatus.NOT_HANDSHAKING) {
        return;  // 已在握手中，避免重复调用
    }
    if (handshakePromise.isDone()) {
        return;  // 握手已完成
    }

    try {
        engine.beginHandshake();       // 触发 JDK SSLEngine 开始握手
        wrapNonAppData(ctx, false);    // 生成握手消息 (如 ClientHello)
    } catch (Throwable e) {
        setHandshakeFailure(ctx, e);
    } finally {
        if (flushAtEnd) {
            forceFlush(ctx);           // 确保握手消息被发送
        }
    }
}
```

#### 3.2.3 wrapNonAppData() -- 非应用数据的 wrap

此方法专门用于握手过程中的控制消息交换：

```java
private boolean wrapNonAppData(final ChannelHandlerContext ctx, boolean inUnwrap)
        throws SSLException {
    ByteBuf out = null;
    ByteBufAllocator alloc = ctx.alloc();
    try {
        outer: while (!ctx.isRemoved()) {
            if (out == null) {
                out = allocateOutNetBuf(ctx, 2048, 1);  // 握手消息通常较小
            }
            SSLEngineResult result = wrap(alloc, engine, Unpooled.EMPTY_BUFFER, out);
            if (result.bytesProduced() > 0) {
                ctx.write(out).addListener(future -> {
                    Throwable cause = future.cause();
                    if (cause != null) {
                        setHandshakeFailureTransportFailure(ctx, cause);
                    }
                });
                out = null;
            }

            HandshakeStatus status = result.getHandshakeStatus();
            switch (status) {
                case FINISHED:
                    setHandshakeSuccess();
                    return false;
                case NEED_TASK:
                    if (!runDelegatedTasks(inUnwrap)) break outer;
                    break;
                case NEED_UNWRAP:
                    if (inUnwrap || unwrapNonAppData(ctx) <= 0) return false;
                    break;
                case NEED_WRAP:
                    break;  // 继续循环
                case NOT_HANDSHAKING:
                    setHandshakeSuccess();
                    return true;
            }

            if (result.bytesProduced() == 0 && status != HandshakeStatus.NEED_TASK) {
                break;
            }
        }
    } finally {
        if (out != null) out.release();
    }
    return false;
}
```

#### 3.2.4 握手状态机流转

SSLEngine 的握手状态机通过 `HandshakeStatus` 驱动：

```
                    ┌───────────────────┐
                    │  NOT_HANDSHAKING  │ ← 初始状态 / 完成状态
                    └─────────┬─────────┘
                              │ beginHandshake()
                              ▼
                    ┌───────────────────┐
              ┌────→│    NEED_WRAP      │────┐
              │     └───────────────────┘    │
              │              │ wrap()        │
              │              ▼               │
              │     ┌───────────────────┐    │
              │     │   NEED_UNWRAP     │←───┘
              │     └────────┬──────────┘
              │              │ unwrap()
              │              ▼
              │     ┌───────────────────┐
              │     │    NEED_TASK      │ ← 需要执行委托任务
              │     └────────┬──────────┘
              │              │ runDelegatedTasks()
              │              ▼
              │     ┌───────────────────┐
              └─────│     FINISHED      │ ← 握手完成
                    └───────────────────┘
```

### 3.3 wrap 操作 -- 出站加密

#### 3.3.1 write() 方法

`write()` 方法将应用数据暂存到 `pendingUnencryptedWrites` 队列：

```java
@Override
public void write(final ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
        throws Exception {
    if (!(msg instanceof ByteBuf)) {
        UnsupportedMessageTypeException exception = new UnsupportedMessageTypeException(msg, ByteBuf.class);
        ReferenceCountUtil.safeRelease(msg);
        promise.setFailure(exception);
    } else if (pendingUnencryptedWrites == null) {
        ReferenceCountUtil.safeRelease(msg);
        promise.setFailure(newPendingWritesNullException());
    } else {
        pendingUnencryptedWrites.add((ByteBuf) msg, promise);  // 暂存，不立即加密
    }
}
```

#### 3.3.2 flush() 方法

`flush()` 触发实际的加密操作：

```java
@Override
public void flush(ChannelHandlerContext ctx) throws Exception {
    // StartTLS 模式：首条消息明文发送
    if (startTls && !isStateSet(STATE_SENT_FIRST_MESSAGE)) {
        setState(STATE_SENT_FIRST_MESSAGE);
        pendingUnencryptedWrites.writeAndRemoveAll(ctx);  // 明文直出
        forceFlush(ctx);
        startHandshakeProcessing(true);  // 然后启动握手
        return;
    }

    if (isStateSet(STATE_PROCESS_TASK)) {
        return;  // 正在处理委托任务，暂不 flush
    }

    try {
        wrapAndFlush(ctx);
    } catch (Throwable cause) {
        setHandshakeFailure(ctx, cause);
        PlatformDependent.throwException(cause);
    }
}
```

#### 3.3.3 wrap() 核心加密方法

```java
private void wrap(ChannelHandlerContext ctx, boolean inUnwrap) throws SSLException {
    ByteBuf out = null;
    ByteBufAllocator alloc = ctx.alloc();
    try {
        final int wrapDataSize = this.wrapDataSize;
        outer: while (!ctx.isRemoved()) {
            ChannelPromise promise = ctx.newPromise();
            // 从队列取出数据，按 wrapDataSize 分片
            ByteBuf buf = wrapDataSize > 0 ?
                    pendingUnencryptedWrites.remove(alloc, wrapDataSize, promise) :
                    pendingUnencryptedWrites.removeFirst(promise);
            if (buf == null) break;

            SSLEngineResult result;
            if (buf.readableBytes() > MAX_PLAINTEXT_LENGTH) {
                // 大块数据：分片加密，减少内存分配
                out = allocateOutNetBuf(ctx, readableBytes, buf.nioBufferCount() + numPackets);
                result = wrapMultiple(alloc, engine, buf, out);
            } else {
                // 小块数据：单次加密
                out = allocateOutNetBuf(ctx, buf.readableBytes(), buf.nioBufferCount());
                result = wrap(alloc, engine, buf, out);
            }

            // 处理加密结果...
            if (result.getStatus() == Status.CLOSED) {
                pendingUnencryptedWrites.releaseAndFailAll(ctx, exception);
                return;
            }

            switch (result.getHandshakeStatus()) {
                case NEED_TASK:
                    if (!runDelegatedTasks(inUnwrap)) break outer;
                    break;
                case FINISHED:
                case NOT_HANDSHAKING:
                    setHandshakeSuccess();
                    break;
                case NEED_WRAP:
                    // 继续循环
                    break;
                case NEED_UNWRAP:
                    readIfNeeded(ctx);
                    return;  // 需要更多数据
            }
        }
    } finally {
        if (out != null) out.release();
        if (inUnwrap) setState(STATE_NEEDS_FLUSH);
    }
}
```

#### 3.3.4 wrap(alloc, engine, in, out) -- 底层加密

```java
private SSLEngineResult wrap(ByteBufAllocator alloc, SSLEngine engine,
                              ByteBuf in, ByteBuf out) throws SSLException {
    ByteBuf newDirectIn = null;
    try {
        int readerIndex = in.readerIndex();
        int readableBytes = in.readableBytes();

        final ByteBuffer[] in0;
        if (in.isDirect() || !engineType.wantsDirectBuffer) {
            // 直接缓冲区或 JDK 引擎：直接使用内部 ByteBuffer
            if (!(in instanceof CompositeByteBuf) && in.nioBufferCount() == 1) {
                in0 = singleBuffer;
                in0[0] = in.internalNioBuffer(readerIndex, readableBytes);
            } else {
                in0 = in.nioBuffers();  // CompositeByteBuf 的多个 ByteBuffer
            }
        } else {
            // 堆缓冲区 + OpenSSL 引擎：拷贝到直接缓冲区
            newDirectIn = alloc.directBuffer(readableBytes);
            newDirectIn.writeBytes(in, readerIndex, readableBytes);
            in0 = singleBuffer;
            in0[0] = newDirectIn.internalNioBuffer(newDirectIn.readerIndex(), readableBytes);
        }

        for (;;) {
            ByteBuffer out0 = toByteBuffer(out, out.writerIndex(), out.writableBytes());
            SSLEngineResult result = engine.wrap(in0, out0);
            in.skipBytes(result.bytesConsumed());
            out.writerIndex(out.writerIndex() + result.bytesProduced());

            if (result.getStatus() == Status.BUFFER_OVERFLOW) {
                out.ensureWritable(engine.getSession().getPacketBufferSize());
            } else {
                return result;
            }
        }
    } finally {
        singleBuffer[0] = null;
        if (newDirectIn != null) newDirectIn.release();
    }
}
```

### 3.4 unwrap 操作 -- 入站解密

#### 3.4.1 decode() 入口

```java
@Override
protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out)
        throws SSLException {
    if (isStateSet(STATE_PROCESS_TASK)) {
        return;  // 正在处理委托任务
    }
    if (jdkCompatibilityMode) {
        decodeJdkCompatible(ctx, in);    // JDK 模式：按记录头解析
    } else {
        decodeNonJdkCompatible(ctx, in); // OpenSSL 模式：直接解密
    }
}
```

#### 3.4.2 decodeJdkCompatible() -- JDK 兼容模式

```java
private void decodeJdkCompatible(ChannelHandlerContext ctx, ByteBuf in)
        throws NotSslRecordException {
    int packetLength = this.packetLength;
    if (packetLength > 0) {
        // 已知包长度，等待足够数据
        if (in.readableBytes() < packetLength) return;
    } else {
        // 解析 TLS 记录头获取包长度
        final int readableBytes = in.readableBytes();
        if (readableBytes < SslUtils.SSL_RECORD_HEADER_LENGTH) return;

        packetLength = getEncryptedPacketLength(in, in.readerIndex(), true);
        if (packetLength == SslUtils.NOT_ENCRYPTED) {
            // 非 SSL/TLS 数据，抛出异常
            NotSslRecordException e = new NotSslRecordException(...);
            setHandshakeFailure(ctx, e);
            throw e;
        }
        if (packetLength == NOT_ENOUGH_DATA) return;
        if (packetLength > readableBytes) {
            this.packetLength = packetLength;  // 缓存包长度
            return;
        }
    }

    this.packetLength = 0;
    try {
        final int bytesConsumed = unwrap(ctx, in, packetLength);
        if (bytesConsumed != packetLength && !engine.isInboundDone()) {
            throw new NotSslRecordException();
        }
    } catch (Throwable cause) {
        handleUnwrapThrowable(ctx, cause);
    }
}
```

TLS 记录头格式 (5 字节)：
```
+------+--------+--------+--------+--------+
| Type | Major  | Minor  | Length (16 bit)  |
+------+--------+--------+--------+--------+
|  1B  |  1B    |  1B    |      2B          |
+------+--------+--------+--------+--------+

Type: 20=ChangeCipherSpec, 21=Alert, 22=Handshake, 23=ApplicationData
```

#### 3.4.3 unwrap() 核心解密方法

```java
private int unwrap(ChannelHandlerContext ctx, ByteBuf packet, int length)
        throws SSLException {
    final int originalLength = length;
    boolean wrapLater = false;
    boolean notifyClosure = false;
    ByteBuf decodeOut = allocate(ctx, length);
    try {
        do {
            final SSLEngineResult result = engineType.unwrap(this, packet, length, decodeOut);
            final Status status = result.getStatus();
            final HandshakeStatus handshakeStatus = result.getHandshakeStatus();
            final int produced = result.bytesProduced();
            final int consumed = result.bytesConsumed();

            packet.skipBytes(consumed);
            length -= consumed;

            // 握手完成通知
            if (handshakeStatus == HandshakeStatus.FINISHED ||
                handshakeStatus == HandshakeStatus.NOT_HANDSHAKING) {
                wrapLater |= (decodeOut.isReadable() ?
                        setHandshakeSuccessUnwrapMarkReentry() : setHandshakeSuccess()) || ...;
            }

            // 传递解密数据给上层
            if (decodeOut.isReadable()) {
                setState(STATE_FIRE_CHANNEL_READ);
                if (isStateSet(STATE_UNWRAP_REENTRY)) {
                    executedRead = true;
                    executeChannelRead(ctx, decodeOut);  // 延迟到栈展开后执行
                } else {
                    ctx.fireChannelRead(decodeOut);
                }
                decodeOut = null;
            }

            // 处理各种状态
            if (status == Status.CLOSED) {
                notifyClosure = true;
            } else if (status == Status.BUFFER_OVERFLOW) {
                // 重新分配更大的缓冲区
                decodeOut = allocate(ctx, applicationBufferSize - produced);
                continue;
            }

            if (handshakeStatus == HandshakeStatus.NEED_TASK) {
                boolean pending = runDelegatedTasks(true);
                if (!pending) break;
            } else if (handshakeStatus == HandshakeStatus.NEED_WRAP) {
                if (wrapNonAppData(ctx, true) && length == 0) break;
            }

            if (status == Status.BUFFER_UNDERFLOW ||
                handshakeStatus != HandshakeStatus.NEED_TASK &&
                (consumed == 0 && produced == 0 || ...)) {
                break;
            }
        } while (!ctx.isRemoved());

        if (wrapLater) {
            wrap(ctx, true);  // 握手完成后，处理排队的写入
        }
    } finally {
        if (decodeOut != null) decodeOut.release();
        if (notifyClosure) notifyClosePromise(null);
    }
    return originalLength - length;
}
```

### 3.5 委托任务处理

SSLEngine 在握手过程中可能产生需要在特定线程执行的委托任务 (如证书验证、密钥计算)。

#### 3.5.1 runDelegatedTasks()

```java
private boolean runDelegatedTasks(boolean inUnwrap) {
    if (delegatedTaskExecutor == ImmediateExecutor.INSTANCE || inEventLoop(delegatedTaskExecutor)) {
        // 同步执行：在 EventLoop 线程直接运行
        for (;;) {
            Runnable task = engine.getDelegatedTask();
            if (task == null) return true;
            setState(STATE_PROCESS_TASK);
            if (task instanceof AsyncRunnable) {
                // 异步任务：支持异步完成回调
                AsyncRunnable asyncTask = (AsyncRunnable) task;
                AsyncTaskCompletionHandler completionHandler = new AsyncTaskCompletionHandler(inUnwrap);
                asyncTask.run(completionHandler);
                pending = completionHandler.resumeLater();
                if (pending) return false;
            } else {
                task.run();
            }
            clearState(STATE_PROCESS_TASK);
        }
    } else {
        // 异步执行：提交到委托执行器
        executeDelegatedTask(inUnwrap);
        return false;
    }
}
```

#### 3.5.2 SslTasksRunner -- 异步任务执行器

```java
private final class SslTasksRunner implements Runnable {
    private final boolean inUnwrap;

    @Override
    public void run() {
        Runnable task = engine.getDelegatedTask();
        if (task == null) return;
        if (task instanceof AsyncRunnable) {
            AsyncRunnable asyncTask = (AsyncRunnable) task;
            asyncTask.run(runCompleteTask);  // 异步完成回调
        } else {
            task.run();
            runComplete();  // 同步完成后恢复处理
        }
    }

    void runComplete() {
        // 跳回 EventLoop 线程继续处理
        ctx.executor().execute(this::resumeOnEventExecutor);
    }

    private void resumeOnEventExecutor() {
        clearState(STATE_PROCESS_TASK);
        HandshakeStatus status = engine.getHandshakeStatus();
        switch (status) {
            case NEED_TASK:   executeDelegatedTask(this); break;  // 继续下一个任务
            case FINISHED:    /* 通知握手成功 */ break;
            case NOT_HANDSHAKING: /* 恢复处理 */ break;
            case NEED_UNWRAP: unwrapNonAppData(ctx); break;
            case NEED_WRAP:   wrapNonAppData(ctx, false); break;
        }
    }
}
```

### 3.6 握手完成与超时

#### 3.6.1 setHandshakeSuccess()

```java
private boolean setHandshakeSuccess() throws SSLException {
    final SSLSession session = engine.getSession();
    if (resumptionController != null && !handshakePromise.isDone()) {
        // 验证会话恢复的证书
        resumptionController.validateResumeIfNeeded(engine);
    }
    final boolean notified = !handshakePromise.isDone() &&
                              handshakePromise.trySuccess(ctx.channel());
    if (notified) {
        logger.debug("{} HANDSHAKEN: protocol:{} cipher suite:{}",
            ctx.channel(), session.getProtocol(), session.getCipherSuite());
        ctx.fireUserEventTriggered(SslHandshakeCompletionEvent.SUCCESS);
    }
    if (isStateSet(STATE_READ_DURING_HANDSHAKE)) {
        clearState(STATE_READ_DURING_HANDSHAKE);
        if (!ctx.channel().config().isAutoRead()) {
            ctx.read();  // 恢复握手期间暂停的读操作
        }
    }
    return notified;
}
```

#### 3.6.2 applyHandshakeTimeout()

```java
private void applyHandshakeTimeout() {
    final Promise<Channel> localHandshakePromise = this.handshakePromise;
    final long handshakeTimeoutMillis = this.handshakeTimeoutMillis;
    if (handshakeTimeoutMillis <= 0 || localHandshakePromise.isDone()) return;

    final Future<?> timeoutFuture = ctx.executor().schedule(() -> {
        if (localHandshakePromise.isDone()) return;
        SSLException exception = new SslHandshakeTimeoutException(
            "handshake timed out after " + handshakeTimeoutMillis + "ms");
        if (localHandshakePromise.tryFailure(exception)) {
            SslUtils.handleHandshakeFailure(ctx, exception, true);
        } finally {
            releaseAndFailAll(ctx, exception);
        }
    }, handshakeTimeoutMillis, TimeUnit.MILLISECONDS);

    // 握手完成时取消超时定时器
    localHandshakePromise.addListener(f -> timeoutFuture.cancel(false));
}
```

### 3.7 关闭流程

#### 3.7.1 closeOutboundAndChannel()

```java
private void closeOutboundAndChannel(final ChannelHandlerContext ctx,
                                      final ChannelPromise promise,
                                      boolean disconnect) throws Exception {
    setState(STATE_OUTBOUND_CLOSED);
    engine.closeOutbound();  // 生成 close_notify

    if (!ctx.channel().isActive()) {
        if (disconnect) ctx.disconnect(promise);
        else ctx.close(promise);
        return;
    }

    ChannelPromise closeNotifyPromise = ctx.newPromise();
    try {
        flush(ctx, closeNotifyPromise);  // 发送 close_notify
    } finally {
        if (!isStateSet(STATE_CLOSE_NOTIFY)) {
            setState(STATE_CLOSE_NOTIFY);
            // 等待 close_notify 发送完成后关闭 Channel
            safeClose(ctx, closeNotifyPromise,
                      PromiseNotifier.cascade(false, ctx.newPromise(), promise));
        }
    }
}
```

#### 3.7.2 safeClose() -- 安全关闭

```java
private void safeClose(final ChannelHandlerContext ctx,
                        final ChannelFuture flushFuture,
                        final ChannelPromise promise) {
    if (!ctx.channel().isActive()) {
        ctx.close(promise);
        return;
    }

    // 设置 close_notify 刷新超时
    if (!flushFuture.isDone() && closeNotifyFlushTimeoutMillis > 0) {
        timeoutFuture = ctx.executor().schedule(() -> {
            if (!flushFuture.isDone()) {
                logger.warn("Last write attempt timed out; force-closing the connection.");
                ctx.close(ctx.newPromise());
            }
        }, closeNotifyFlushTimeoutMillis, TimeUnit.MILLISECONDS);
    }

    // close_notify 发送成功后
    flushFuture.addListener(f -> {
        if (timeoutFuture != null) timeoutFuture.cancel(false);
        if (closeNotifyReadTimeoutMillis <= 0) {
            ctx.close(ctx.newPromise());
        } else {
            // 等待对端的 close_notify 响应
            sslClosePromise.addListener(future -> {
                ctx.close(ctx.newPromise());
            });
        }
    });
}
```

### 3.8 异常处理

#### 3.8.1 ignoreException() -- 可忽略异常判断

```java
private boolean ignoreException(Throwable t) {
    if (!(t instanceof SSLException) && t instanceof IOException && sslClosePromise.isDone()) {
        String message = t.getMessage();
        // 匹配 "connection reset" / "broken pipe" 等常见网络错误
        if (message != null && IGNORABLE_ERROR_MESSAGE.matcher(message).matches()) {
            return true;
        }
        // 检查堆栈中的 SocketChannel.read() 调用
        for (StackTraceElement element: t.getStackTrace()) {
            if ("read".equals(element.getMethodName()) &&
                isIgnorableClassInStack(element.getClassName())) {
                return true;
            }
        }
    }
    return false;
}
```

设计意图：在 SSL 关闭后，对端可能发送 RST 而非正常的 close_notify 响应，这类错误应被静默处理。

## 四、SslContext -- SSL 上下文抽象

### 4.1 核心职责

`SslContext` 是 SSL 配置的抽象工厂，负责：
1. 创建 `SSLEngine` 实例
2. 创建 `SslHandler` 实例
3. 管理会话上下文
4. 提供证书和密钥管理接口

### 4.2 关键抽象方法

```java
public abstract class SslContext {
    // 创建 SSLEngine
    public abstract SSLEngine newEngine(ByteBufAllocator alloc);
    public abstract SSLEngine newEngine(ByteBufAllocator alloc, String peerHost, int peerPort);

    // 获取会话上下文
    public abstract SSLSessionContext sessionContext();

    // 创建 SslHandler (模板方法模式)
    public final SslHandler newHandler(ByteBufAllocator alloc) {
        return newHandler(alloc, startTls);
    }

    protected SslHandler newHandler(ByteBufAllocator alloc, boolean startTls, Executor executor) {
        return new SslHandler(newEngine(alloc), startTls, executor, resumptionController);
    }

    // 端点验证算法 (Netty 4.2 默认启用 HTTPS 验证)
    protected static final String defaultEndpointVerificationAlgorithm;
    static {
        // HTTPS -- DNS 主机名验证 (默认)
        // LDAP -- LDAP 身份验证
        // NONE -- 禁用验证 (Netty 4.1 行为)
    }
}
```

### 4.3 SslContextBuilder -- 构建器

`SslContextBuilder` 提供流式 API 配置 SslContext：

```java
// 服务端
SslContext sslCtx = SslContextBuilder.forServer(certChainFile, keyFile)
    .sslProvider(SslProvider.OPENSSL)
    .ciphers(Arrays.asList("TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"))
    .applicationProtocolConfig(new ApplicationProtocolConfig(
        Protocol.ALPN,
        SelectorFailureBehavior.FATAL_ALERT,
        SelectedListenerFailureBehavior.ACCEPT,
        "h2", "http/1.1"))
    .build();

// 客户端
SslContext sslCtx = SslContextBuilder.forClient()
    .trustManager(trustCertCollectionFile)
    .sslProvider(SslProvider.JDK)
    .build();
```

`build()` 方法根据配置委托给具体实现：

```java
public SslContext build() throws SSLException {
    if (forServer) {
        return SslContext.newServerContextInternal(provider, ...);
    } else {
        return SslContext.newClientContextInternal(provider, ...);
    }
}
```

`newServerContextInternal()` 内部通过 `switch(provider)` 分发：

```java
switch (provider) {
    case JDK:
        return new JdkSslServerContext(...);
    case OPENSSL:
        return new OpenSslServerContext(...);
    case OPENSSL_REFCNT:
        return new ReferenceCountedOpenSslServerContext(...);
}
```

## 五、SslProvider -- 提供者枚举

### 5.1 三种提供者

```java
public enum SslProvider {
    JDK,              // JDK 原生 javax.net.ssl 实现
    OPENSSL,          // OpenSSL 实现 (Finalizer 回收)
    OPENSSL_REFCNT;   // OpenSSL 实现 (引用计数回收，推荐)
}
```

### 5.2 能力查询

```java
// ALPN 支持检测
public static boolean isAlpnSupported(final SslProvider provider) {
    switch (provider) {
        case JDK:     return JdkAlpnApplicationProtocolNegotiator.isAlpnSupported();
        case OPENSSL:
        case OPENSSL_REFCNT: return OpenSsl.isAlpnSupported();
    }
}

// TLS 1.3 支持检测
public static boolean isTlsv13Supported(final SslProvider sslProvider) { ... }

// 默认提供者选择 (优先 OpenSSL)
private static SslProvider defaultProvider() {
    if (OpenSsl.isAvailable()) return SslProvider.OPENSSL;
    else return SslProvider.JDK;
}
```

## 六、JDK SSL vs OpenSSL 实现对比

### 6.1 JdkSslEngine -- JDK 引擎装饰器

```java
class JdkSslEngine extends SSLEngine implements ApplicationProtocolAccessor {
    private final SSLEngine engine;                    // 委托给 JDK 原生引擎
    private volatile String applicationProtocol;       // 协商的应用协议

    // 所有方法直接委托给 engine
    @Override
    public SSLEngineResult wrap(ByteBuffer src, ByteBuffer dst) throws SSLException {
        return engine.wrap(src, dst);
    }
    @Override
    public SSLEngineResult unwrap(ByteBuffer src, ByteBuffer dst) throws SSLException {
        return engine.unwrap(src, dst);
    }
}
```

JdkSslEngine 是一个轻量级装饰器，仅添加了 `applicationProtocol` 属性用于 ALPN 协商结果的存储。

### 6.2 ReferenceCountedOpenSslEngine -- OpenSSL 引擎

```java
public class ReferenceCountedOpenSslEngine extends SSLEngine
        implements ReferenceCounted, ApplicationProtocolAccessor {

    // OpenSSL 原生状态
    private long ssl;         // SSL* 指针
    private long networkBIO;  // BIO* 指针

    private enum HandshakeState {
        NOT_STARTED,
        STARTED_IMPLICITLY,   // 通过 unwrap/wrap 隐式启动
        STARTED_EXPLICITLY,   // 通过 beginHandshake() 显式启动
        FINISHED
    }
}
```

### 6.3 核心差异对比

| 特性 | JDK SSL | OpenSSL |
|------|---------|---------|
| **内存管理** | GC 自动回收 | 引用计数 / Finalizer |
| **缓冲区支持** | 仅单 ByteBuffer | 支持 ByteBuffer[] scatter/gather |
| **累积器** | MERGE_CUMULATOR (必须合并碎片) | COMPOSITE_CUMULATOR (零拷贝) |
| **输出缓冲区** | 堆缓冲区 (减少原生内存使用) | 直接缓冲区 (零拷贝) |
| **待处理数据查询** | 不支持 | `sslPending()` 精确查询 |
| **输出大小计算** | `getPacketBufferSize()` 估算 | `calculateOutNetBufSize()` 精确计算 |
| **委托任务** | 同步执行 | 支持异步 AsyncRunnable |
| **性能** | 较低 (额外内存拷贝) | 较高 (零拷贝、原生优化) |
| **依赖** | JDK 内置 | 需要 netty-tcnative 本地库 |
| **会话恢复** | JDK SessionCache | OpenSSL Session Cache |

### 6.4 性能差异根因

```java
// JDK: 堆缓冲区 + 额外拷贝
JDK(false, MERGE_CUMULATOR) {
    ByteBuf allocateWrapBuffer(...) {
        // 使用堆缓冲区，因为 JDK 内部操作 byte[]
        return allocator.heapBuffer(Math.max(pendingBytes,
            engine.getSession().getPacketBufferSize()));
    }
}

// OpenSSL: 直接缓冲区 + 零拷贝
TCNATIVE(true, COMPOSITE_CUMULATOR) {
    ByteBuf allocateWrapBuffer(...) {
        // 使用直接缓冲区，避免堆外拷贝
        return allocator.directBuffer(
            ((ReferenceCountedOpenSslEngine) handler.engine)
                .calculateOutNetBufSize(pendingBytes, numComponents));
    }
}
```

## 七、ALPN 协商

### 7.1 ApplicationProtocolConfig

```java
public final class ApplicationProtocolConfig {
    public enum Protocol {
        NONE,         // 禁用协议协商
        NPN,          // Next Protocol Negotiation (已过时)
        ALPN,         // Application-Layer Protocol Negotiation (推荐)
        NPN_AND_ALPN  // 同时支持
    }

    public enum SelectorFailureBehavior {
        FATAL_ALERT,            // 无匹配时发送致命告警
        NO_ADVERTISE,           // 不广告扩展
        CHOOSE_MY_LAST_PROTOCOL // 回退到最后一个协议
    }

    public enum SelectedListenerFailureBehavior {
        ACCEPT,                 // 接受任何选择
        FATAL_ALERT,            // 不匹配时发送致命告警
        CHOOSE_MY_LAST_PROTOCOL // 回退到最后一个协议
    }
}
```

### 7.2 HTTP/2 协商示例

```java
ApplicationProtocolConfig alpnConfig = new ApplicationProtocolConfig(
    Protocol.ALPN,
    SelectorFailureBehavior.FATAL_ALERT,       // 服务端：无匹配则失败
    SelectedListenerFailureBehavior.FATAL_ALERT, // 客户端：不匹配则失败
    "h2",        // HTTP/2 优先
    "http/1.1"   // HTTP/1.1 回退
);

SslContext sslCtx = SslContextBuilder.forServer(certFile, keyFile)
    .applicationProtocolConfig(alpnConfig)
    .build();
```

协商流程：
1. 客户端在 ClientHello 中发送 `extension: application_layer_protocol_negotiation`，包含支持的协议列表
2. 服务端在 ServerHello 中返回选中的协议
3. `SslHandler.applicationProtocol()` 返回协商结果

## 八、SslClientHelloHandler -- SNI 路由

### 8.1 设计目的

`SslClientHelloHandler` 允许在 TLS 握手前拦截 ClientHello 消息，提取 SNI (Server Name Indication) 扩展，实现基于域名的动态证书选择。

### 8.2 核心实现

```java
public abstract class SslClientHelloHandler<T> extends ByteToMessageDecoder
        implements ChannelOutboundHandler {

    public static final int MAX_CLIENT_HELLO_LENGTH = 0xFFFFFF;  // 16MB (RFC 限制)
    static final int DEFAULT_MAX_CLIENT_HELLO_LENGTH = 64 * 1024; // 64KB 默认限制

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out)
            throws Exception {
        // 解析 TLS 记录头
        while (readableBytes >= SslUtils.SSL_RECORD_HEADER_LENGTH) {
            final int contentType = in.getUnsignedByte(readerIndex);
            switch (contentType) {
                case SSL_CONTENT_TYPE_HANDSHAKE:
                    // 检查是否为 ClientHello (handshakeType == 1)
                    final int handshakeType = in.getUnsignedByte(readerIndex + SSL_RECORD_HEADER_LENGTH);
                    if (handshakeType != 1) {
                        select(ctx, null);  // 非 ClientHello，跳过
                        return;
                    }
                    // 解析 Handshake Length
                    handshakeLength = in.getUnsignedMedium(readerIndex + SSL_RECORD_HEADER_LENGTH + 1);
                    // 提取 ClientHello 数据
                    ByteBuf clientHello = handshakeBuffer.setIndex(0, handshakeLength);
                    select(ctx, clientHello);
                    return;
            }
        }
    }

    // 子类实现：从 ClientHello 中提取 SNI 并选择证书
    protected abstract Future<T> lookup(ChannelHandlerContext ctx, ByteBuf clientHello);

    // 子类实现：查找完成后配置 SSLEngine
    protected abstract void onLookupComplete(ChannelHandlerContext ctx, Future<T> future);
}
```

### 8.3 ClientHello 数据格式

```
ClientHello:
  +-------------------+
  | client_version (2B)|
  | random (32B)       |
  | session_id (var)   |
  | cipher_suites (var)|
  | compression (var)  |
  | extensions (var)   |  ← SNI 在此
  +-------------------+

SNI Extension (type=0x0000):
  +-------------------+
  | server_name_list  |
  |   name_type (1B)  |  0=hostname
  |   name_length (2B)|
  |   name (var)      |  例如 "example.com"
  +-------------------+
```

### 8.4 异步查找机制

```java
private void select(final ChannelHandlerContext ctx, ByteBuf clientHello) throws Exception {
    final Future<T> future = lookup(ctx, clientHello);
    if (future.isDone()) {
        onLookupComplete(ctx, future);  // 同步完成
    } else {
        suppressRead = true;  // 暂停读取，等待查找完成
        future.addListener(future1 -> {
            suppressRead = false;
            onLookupComplete(ctx, future1);
            if (readPending) {
                readPending = false;
                ctx.read();  // 恢复读取
            }
        });
    }
}
```

## 九、事件通知机制

### 9.1 SslCompletionEvent 继承体系

```
SslCompletionEvent (抽象基类)
├── SslHandshakeCompletionEvent  -- 握手完成事件
│   ├── SUCCESS (单例)
│   └── new SslHandshakeCompletionEvent(Throwable cause)  -- 失败
└── SslCloseCompletionEvent      -- SSL 关闭事件
    ├── SUCCESS (单例)
    └── new SslCloseCompletionEvent(Throwable cause)  -- 失败
```

### 9.2 事件触发时机

```java
// 握手成功
ctx.fireUserEventTriggered(SslHandshakeCompletionEvent.SUCCESS);

// 握手失败
ctx.fireUserEventTriggered(new SslHandshakeCompletionEvent(cause));

// SSL 关闭成功
ctx.fireUserEventTriggered(SslCloseCompletionEvent.SUCCESS);

// SSL 关闭失败
ctx.fireUserEventTriggered(new SslCloseCompletionEvent(cause));
```

### 9.3 使用示例

```java
public class SslEventHandler extends ChannelInboundHandlerAdapter {
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        if (evt == SslHandshakeCompletionEvent.SUCCESS) {
            SslHandler sslHandler = ctx.pipeline().get(SslHandler.class);
            String protocol = sslHandler.applicationProtocol();
            if ("h2".equals(protocol)) {
                // 配置 HTTP/2 处理器
            }
        } else if (evt instanceof SslHandshakeCompletionEvent) {
            Throwable cause = ((SslHandshakeCompletionEvent) evt).cause();
            logger.error("SSL handshake failed", cause);
        }
        ctx.fireUserEventTriggered(evt);
    }
}
```

## 十、模块交互关系

### 10.1 组件协作图

```
┌──────────────────────────────────────────────────────────────┐
│                        用户代码                               │
│  SslContextBuilder.forServer().sslProvider(OPENSSL).build()   │
└────────────────────────┬─────────────────────────────────────┘
                         │ 创建
                         ▼
┌──────────────────────────────────────────────────────────────┐
│                      SslContext                              │
│  ┌─────────────┐  ┌──────────────┐  ┌──────────────────────┐│
│  │ JdkSslContext│  │  OpenSslCtx  │  │RefCountedOpenSslCtx ││
│  └──────┬──────┘  └──────┬───────┘  └──────────┬───────────┘│
│         │                │                      │            │
│         │   newEngine()  │   newEngine()        │ newEngine()│
│         ▼                ▼                      ▼            │
│  ┌─────────────┐  ┌──────────────┐  ┌──────────────────────┐│
│  │ JdkSslEngine│  │OpenSslEngine │  │RefCountedOpenSslEngin││
│  └──────┬──────┘  └──────┬───────┘  └──────────┬───────────┘│
└─────────┼────────────────┼──────────────────────┼────────────┘
          │                │                      │
          └────────────────┼──────────────────────┘
                           │ newHandler()
                           ▼
┌──────────────────────────────────────────────────────────────┐
│                      SslHandler                              │
│  ┌──────────────────────────────────────────────────────────┐│
│  │  ByteToMessageDecoder + ChannelOutboundHandler           ││
│  │                                                          ││
│  │  ┌─────────────┐  ┌──────────────┐  ┌────────────────┐  ││
│  │  │ SSLEngine   │  │SslEngineType │  │  状态机管理     │  ││
│  │  │ wrap/unwrap │  │  策略选择     │  │  位掩码 state   │  ││
│  │  └─────────────┘  └──────────────┘  └────────────────┘  ││
│  └──────────────────────────────────────────────────────────┘│
└──────────────────────────────────────────────────────────────┘
```

### 10.2 数据处理流水线

```
出站数据流:
  App → write(ByteBuf) → pendingUnencryptedWrites 队列
      → flush() → wrap() → SSLEngine.wrap() → encrypted ByteBuf
      → ctx.write() → 下层传输

入站数据流:
  Network → ByteBuf → decode() → unwrap() → SSLEngine.unwrap()
      → decrypted ByteBuf → ctx.fireChannelRead() → 上层 Handler

握手控制流:
  握手启动 → beginHandshake() → wrapNonAppData() → NEED_UNWRAP
      → unwrap() → NEED_TASK → runDelegatedTasks()
      → NEED_WRAP → wrapNonAppData() → FINISHED
      → setHandshakeSuccess() → SslHandshakeCompletionEvent
```

## 十一、关键流程详解

### 11.1 完整 TLS 握手流程 (客户端视角)

```
客户端 (SslHandler)                    服务端
    │                                    │
    │  1. channelActive()                │
    │  → startHandshakeProcessing()      │
    │  → handshake()                     │
    │  → engine.beginHandshake()         │
    │  → wrapNonAppData()                │
    │  → SSLEngine.wrap() 产生 ClientHello│
    │  → ctx.write(ClientHello) ────────→│
    │                                    │
    │                                    │  处理 ClientHello
    │                                    │  选择密码套件
    │                                    │  生成证书
    │                                    │
    │  ←───────────── ServerHello ───────│
    │  ←───────────── Certificate ───────│
    │  ←───────────── ServerHelloDone ───│
    │                                    │
    │  2. decode() → unwrap()            │
    │  → NEED_TASK (证书验证)             │
    │  → runDelegatedTasks()             │
    │  → NEED_WRAP                       │
    │  → wrapNonAppData()                │
    │  → SSLEngine.wrap() 产生:          │
    │    ClientKeyExchange               │
    │    ChangeCipherSpec ──────────────→│
    │    Finished ──────────────────────→│
    │                                    │
    │                                    │  验证 Finished
    │                                    │
    │  ←───────── ChangeCipherSpec ──────│
    │  ←───────── Finished ─────────────│
    │                                    │
    │  3. unwrap() → FINISHED            │
    │  → setHandshakeSuccess()           │
    │  → fireUserEventTriggered(SUCCESS) │
    │                                    │
    │  4. 开始加密通信                     │
    │  wrap(应用数据) ←─────────────────→│
```

### 11.2 StartTLS 流程

```
1. 客户端发送明文 StartTLS 请求
2. 服务端回复明文 StartTLS 响应
3. 创建 SslHandler(startTls=true) 并插入 Pipeline
4. 首次 flush() 时，STATE_SENT_FIRST_MESSAGE 检查:
   - 未设置：明文直出，然后启动握手
   - 已设置：正常加密流程
```

```java
@Override
public void flush(ChannelHandlerContext ctx) throws Exception {
    if (startTls && !isStateSet(STATE_SENT_FIRST_MESSAGE)) {
        setState(STATE_SENT_FIRST_MESSAGE);
        pendingUnencryptedWrites.writeAndRemoveAll(ctx);  // 明文直出
        forceFlush(ctx);
        startHandshakeProcessing(true);  // 启动握手
        return;
    }
    // ... 正常加密流程
}
```

### 11.3 TLS 重协商流程

```java
public Future<Channel> renegotiate(final Promise<Channel> promise) {
    // 必须在 EventLoop 线程执行
    if (!executor.inEventLoop()) {
        executor.execute(() -> renegotiateOnEventLoop(promise));
        return promise;
    }
    renegotiateOnEventLoop(promise);
    return promise;
}

private void renegotiateOnEventLoop(final Promise<Channel> newHandshakePromise) {
    final Promise<Channel> oldHandshakePromise = handshakePromise;
    if (!oldHandshakePromise.isDone()) {
        // 已在握手中，合并 Promise
        PromiseNotifier.cascade(oldHandshakePromise, newHandshakePromise);
    } else {
        handshakePromise = newHandshakePromise;
        handshake(true);          // 重新开始握手
        applyHandshakeTimeout();  // 重新设置超时
    }
}
```

## 十二、学习要点

### 12.1 设计模式总结

| 模式 | 应用位置 | 说明 |
|------|---------|------|
| **策略模式** | `SslEngineType` | 封装不同 SSLEngine 实现的差异 |
| **工厂方法** | `SslContext.newEngine()` | 多态创建不同类型的 SSLEngine |
| **建造者模式** | `SslContextBuilder` | 流式 API 构建复杂配置 |
| **装饰器模式** | `JdkSslEngine` | 包装 JDK SSLEngine 添加 ALPN 支持 |
| **模板方法** | `SslClientHelloHandler` | 定义 ClientHello 解析骨架，子类实现查找 |
| **状态模式** | SslHandler 状态机 | 位掩码驱动的复杂状态转换 |
| **观察者模式** | Promise/Event 机制 | 异步通知握手完成、关闭等事件 |

### 12.2 Netty 编程技巧

1. **位掩码状态管理**：用 `short` 的各个位表示独立状态，避免多个 `boolean` 字段
2. **策略枚举**：将可变行为封装在枚举常量中，通过 `instanceof` 自动选择
3. **Promise 链式通知**：使用 `PromiseNotifier.cascade()` 实现 Promise 级联
4. **延迟事件触发**：通过 `executeChannelRead()` 将事件延迟到栈展开后执行，避免重入问题
5. **累积器选择**：根据引擎能力选择 `MERGE_CUMULATOR` 或 `COMPOSITE_CUMULATOR`
6. **缓冲区零拷贝**：利用 `internalNioBuffer()` 和 `nioBuffers()` 避免不必要的内存拷贝

### 12.3 性能优化要点

1. **选择合适的 SslProvider**：优先使用 `OPENSSL_REFCNT`，避免 JDK SSL 的额外内存拷贝
2. **合理设置 wrapDataSize**：根据网络 MTU 和应用特点调整单次加密数据量
3. **使用 CompositeByteBuf**：OpenSSL 引擎支持 scatter/gather I/O，避免数据合并
4. **直接缓冲区**：OpenSSL 引擎偏好直接缓冲区，减少堆内堆外拷贝
5. **会话恢复**：合理配置 SessionCache，减少完整握手的开销
6. **委托任务异步化**：将证书验证等 CPU 密集型任务委托到专用线程池

### 12.4 常见陷阱

1. **Handler 移除后写入**：`pendingUnencryptedWrites` 在 `handlerRemoved0()` 后为 null
2. **握手超时**：默认 10 秒，高延迟网络可能需要调整
3. **close_notify 超时**：设置过短可能导致连接被强制关闭
4. **重入问题**：`unwrap()` 过程中可能触发 `wrap()`，需要 `STATE_UNWRAP_REENTRY` 保护
5. **Android 兼容性**：某些 Android 版本的 SSLEngine 存在已知 Bug，Netty 内置了 Workaround
6. **JDK 22-24 Bug**：`JDK-8357268` 导致直接缓冲区 unwrap 问题，Netty 自动拷贝到堆缓冲区

### 12.5 配置建议

```java
// 生产环境推荐配置
SslContext sslCtx = SslContextBuilder.forServer(certFile, keyFile)
    .sslProvider(SslProvider.OPENSSL_REFCNT)  // 使用 OpenSSL 引用计数版本
    .sslContextProvider(null)                  // 使用默认 Provider
    .ciphers(Arrays.asList(                    // 明确指定密码套件
        "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
        "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384"))
    .applicationProtocolConfig(new ApplicationProtocolConfig(
        Protocol.ALPN,
        SelectorFailureBehavior.FATAL_ALERT,
        SelectedListenerFailureBehavior.ACCEPT,
        "h2", "http/1.1"))
    .clientAuth(ClientAuth.NONE)               // 按需配置双向认证
    .sessionCacheSize(0)                       // 使用默认缓存大小
    .sessionTimeout(0)                         // 使用默认超时
    .build();

// 创建 Handler 时可调整参数
SslHandler sslHandler = sslCtx.newHandler(alloc);
sslHandler.setHandshakeTimeoutMillis(30000);      // 30 秒握手超时
sslHandler.setCloseNotifyFlushTimeoutMillis(5000); // 5 秒 close_notify 超时
sslHandler.setWrapDataSize(8192);                  // 8KB 单次加密块
```

## 十三、核心源码文件索引

| 文件 | 职责 |
|------|------|
| `SslHandler.java` | 核心处理器，wrap/unwrap、握手状态机、生命周期管理 |
| `SslContext.java` | SSL 上下文抽象工厂，创建 SSLEngine 和 SslHandler |
| `SslContextBuilder.java` | 建造者模式，流式 API 配置 SslContext |
| `SslProvider.java` | 提供者枚举 (JDK / OPENSSL / OPENSSL_REFCNT) |
| `JdkSslContext.java` | JDK SSL 上下文实现 |
| `JdkSslEngine.java` | JDK SSLEngine 装饰器，添加 ALPN 支持 |
| `ReferenceCountedOpenSslContext.java` | OpenSSL 上下文实现 (引用计数) |
| `ReferenceCountedOpenSslEngine.java` | OpenSSL SSLEngine 实现 |
| `ApplicationProtocolConfig.java` | ALPN/NPN 协议协商配置 |
| `SslClientHelloHandler.java` | ClientHello 拦截器，SNI 路由基础 |
| `SslHandshakeCompletionEvent.java` | 握手完成事件 |
| `SslCloseCompletionEvent.java` | SSL 关闭事件 |
