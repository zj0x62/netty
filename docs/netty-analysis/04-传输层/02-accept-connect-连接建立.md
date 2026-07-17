# 连接建立：Accept/Connect 流程

## 一、概述

Netty 作为基于 NIO 的高性能网络框架，其连接建立流程分为两大场景：**服务端 Accept** 和**客户端 Connect**。本文从源码层面深度剖析这两个流程的完整实现，揭示 Netty 如何在 JDK NIO 的基础上构建了一套高效、可扩展的连接管理体系。

Accept 流程的核心挑战在于：如何将 JDK `ServerSocketChannel.accept()` 返回的原始 `SocketChannel` 无缝融入 Netty 的 Channel 体系，并完成 Pipeline 初始化、EventLoop 注册等一系列生命周期操作。Connect 流程则需要处理非阻塞连接的异步特性，包括连接超时、连接取消等边界情况。

## 二、架构图

### 2.1 类继承体系

```
                          Channel
                            |
                     AbstractChannel
                            |
                     AbstractNioChannel
                       /          \
          AbstractNioMessageChannel  AbstractNioByteChannel
                   |                        |
        NioServerSocketChannel      NioSocketChannel
        (服务端 Accept Channel)      (客户端/已连接 Channel)
```

### 2.2 Accept 流程全景

```
NioEventLoop.run()
  |
  +-- Selector.select()
  |
  +-- processSelectedKey(OP_ACCEPT)
       |
       +-- NioMessageUnsafe.read()
            |
            +-- NioServerSocketChannel.doReadMessages()
            |     |
            |     +-- ServerSocketChannel.accept()
            |     +-- new NioSocketChannel(parent, socketChannel)
            |
            +-- pipeline.fireChannelRead(nioSocketChannel)
                 |
                 +-- ServerBootstrapAcceptor.channelRead()
                      |
                      +-- child.pipeline().addLast(childHandler)
                      +-- setChannelOptions(child, childOptions)
                      +-- setAttributes(child, childAttrs)
                      +-- childGroup.register(child)
```

### 2.3 Connect 流程全景

```
用户代码: channel.connect(remoteAddress)
  |
  +-- AbstractNioChannel.AbstractNioUnsafe.connect()
       |
       +-- NioSocketChannel.doConnect()
       |     |
       |     +-- SocketChannel.connect(remoteAddress)
       |     +-- 若未立即连接: addAndSubmit(NioIoOps.CONNECT)
       |
       +-- 若立即连接成功: fulfillConnectPromise()
       +-- 若异步等待: 保存 connectPromise, 设置超时
            |
            NioEventLoop.run()
              |
              +-- Selector.select() 检测到 OP_CONNECT
              +-- AbstractNioUnsafe.handle(CONNECT)
                   |
                   +-- finishConnect()
                        |
                        +-- doFinishConnect()
                        +-- fulfillConnectPromise()
                             +-- pipeline.fireChannelActive()
```

## 三、核心类分析

### 3.1 NioServerSocketChannel

**路径**: `transport/src/main/java/io/netty/channel/socket/nio/NioServerSocketChannel.java`

`NioServerSocketChannel` 是服务端监听 Channel 的 NIO 实现，继承自 `AbstractNioMessageChannel`，表示这是一个面向**消息**（而非字节流）的 Channel。每一条 "消息" 就是一个新建立的连接。

#### 构造方法

```java
public NioServerSocketChannel(ServerSocketChannel channel) {
    super(null, channel, SelectionKey.OP_ACCEPT);
    config = new NioServerSocketChannelConfig(this, javaChannel().socket());
}
```

关键设计点：
- **parent 为 null**：服务端 Channel 没有父 Channel，它是连接的源头
- **SelectionKey.OP_ACCEPT**：注册的感兴趣事件为 ACCEPT，当有新连接到来时 Selector 会通知
- `NioServerSocketChannel` 的父类构造链会调用 `ch.configureBlocking(false)`，将底层 JDK Channel 设为非阻塞模式

#### doReadMessages() -- Accept 的核心

```java
@Override
protected int doReadMessages(List<Object> buf) throws Exception {
    SocketChannel ch = SocketUtils.accept(javaChannel());

    try {
        if (ch != null) {
            buf.add(new NioSocketChannel(this, ch));
            return 1;
        }
    } catch (Throwable t) {
        logger.warn("Failed to create a new channel from an accepted socket.", t);
        try {
            ch.close();
        } catch (Throwable t2) {
            logger.warn("Failed to close a socket.", t2);
        }
    }

    return 0;
}
```

逐行解析：

1. **`SocketUtils.accept(javaChannel())`**：调用 JDK `ServerSocketChannel.accept()`，这是一个非阻塞调用。如果没有待接受的连接，返回 `null`。
2. **`new NioSocketChannel(this, ch)`**：将 JDK `SocketChannel` 包装为 Netty 的 `NioSocketChannel`，并设置 `this`（服务端 Channel）为 parent。构造过程中会自动将新 Channel 设为非阻塞模式。
3. **`buf.add(...)`**：将新 Channel 加入消息列表，后续由 `NioMessageUnsafe.read()` 遍历该列表，逐个触发 `fireChannelRead`。
4. **异常处理**：如果创建 Channel 失败，确保关闭底层 JDK SocketChannel，避免资源泄漏。
5. **返回值语义**：返回 1 表示成功读取一条消息（一个连接），返回 0 表示没有新连接。

#### 不支持的方法

```java
@Override
protected boolean doConnect(SocketAddress remoteAddress, SocketAddress localAddress) throws Exception {
    throw new UnsupportedOperationException();
}

@Override
protected void doWriteMessage(Object msg, ChannelOutboundBuffer in) throws Exception {
    throw new UnsupportedOperationException();
}
```

`NioServerSocketChannel` 明确不支持 `connect`、`write` 等操作，因为它只负责监听和接受连接。这种设计通过抛出 `UnsupportedOperationException` 提供了明确的错误语义。

### 3.2 NioMessageUnsafe.read() -- 消息读取引擎

**路径**: `transport/src/main/java/io/netty/channel/nio/AbstractNioMessageChannel.java`

`NioMessageUnsafe` 是 `AbstractNioMessageChannel` 的内部类，负责驱动整个消息读取循环。

```java
@Override
public void read() {
    assert eventLoop().inEventLoop();
    final ChannelConfig config = config();
    final ChannelPipeline pipeline = pipeline();
    final RecvByteBufAllocator.Handle allocHandle = unsafe().recvBufAllocHandle();
    allocHandle.reset(config);

    boolean closed = false;
    Throwable exception = null;
    try {
        try {
            do {
                int localRead = doReadMessages(readBuf);
                if (localRead == 0) {
                    break;
                }
                if (localRead < 0) {
                    closed = true;
                    break;
                }
                allocHandle.incMessagesRead(localRead);
            } while (continueReading(allocHandle));
        } catch (Throwable t) {
            exception = t;
        }

        int size = readBuf.size();
        for (int i = 0; i < size; i ++) {
            readPending = false;
            pipeline.fireChannelRead(readBuf.get(i));
        }
        readBuf.clear();
        allocHandle.readComplete();
        pipeline.fireChannelReadComplete();
        // ... 异常处理和关闭逻辑
    } finally {
        if (!readPending && !config.isAutoRead()) {
            removeReadOp();
        }
    }
}
```

核心流程：

1. **断言 EventLoop 线程**：所有 Channel 操作必须在绑定的 EventLoop 线程中执行，保证线程安全
2. **循环读取**：通过 `do-while` 循环不断调用 `doReadMessages()`（即 `NioServerSocketChannel.doReadMessages()`），尽可能多地接受新连接
3. **`continueReading()`**：由 `RecvByteBufAllocator.Handle` 决定是否继续读取。默认情况下，对于服务端 Channel，每次事件循环会尽量多接受连接
4. **批量触发 ChannelRead**：读取完成后，遍历 `readBuf` 列表，逐个触发 `pipeline.fireChannelRead()`。注意这里将读取和事件触发分离，避免在读取循环中触发用户代码可能导致的重入问题
5. **`fireChannelReadComplete()`**：所有消息处理完毕后，触发一次 ReadComplete 事件，通知 Pipeline 本轮读取结束
6. **AutoRead 控制**：如果 `autoRead=false` 且没有显式调用 `read()`，则移除 OP_ACCEPT 注册，停止接受新连接

### 3.3 ServerBootstrap.init() 与 ServerBootstrapAcceptor

**路径**: `transport/src/main/java/io/netty/bootstrap/ServerBootstrap.java`

#### init() -- 服务端 Channel 初始化

```java
@Override
void init(Channel channel) throws Throwable {
    setChannelOptions(channel, newOptionsArray(), logger);
    setAttributes(channel, newAttributesArray());

    ChannelPipeline p = channel.pipeline();

    final EventLoopGroup currentChildGroup = childGroup;
    final ChannelHandler currentChildHandler = childHandler;
    final Entry<ChannelOption<?>, Object>[] currentChildOptions = newOptionsArray(childOptions);
    final Entry<AttributeKey<?>, Object>[] currentChildAttrs = newAttributesArray(childAttrs);
    final Collection<ChannelInitializerExtension> extensions = getInitializerExtensions();

    p.addLast(new ChannelInitializer<Channel>() {
        @Override
        public void initChannel(final Channel ch) {
            final ChannelPipeline pipeline = ch.pipeline();
            ChannelHandler handler = config.handler();
            if (handler != null) {
                pipeline.addLast(handler);
            }

            ch.eventLoop().execute(new Runnable() {
                @Override
                public void run() {
                    pipeline.addLast(new ServerBootstrapAcceptor(
                            ch, currentChildGroup, currentChildHandler,
                            currentChildOptions, currentChildAttrs, extensions));
                }
            });
        }
    });
}
```

关键设计：

1. **ChannelInitializer 模式**：使用 `ChannelInitializer` 作为占位 Handler，在 Channel 注册到 EventLoop 后才添加真正的 Handler。这是因为注册过程中会触发 `handlerAdded` 和 `channelRegistered` 事件。
2. **延迟添加 ServerBootstrapAcceptor**：通过 `ch.eventLoop().execute()` 将 `ServerBootstrapAcceptor` 的添加推迟到 EventLoop 线程中执行。这确保了 Pipeline 的修改在正确的线程中进行。
3. **快照子配置**：在 `init()` 时就捕获 `childGroup`、`childHandler`、`childOptions`、`childAttrs` 的快照，避免后续修改影响已初始化的 Channel。

#### ServerBootstrapAcceptor.channelRead() -- 接受新连接

```java
@Override
public void channelRead(ChannelHandlerContext ctx, Object msg) {
    final Channel child = (Channel) msg;

    child.pipeline().addLast(childHandler);

    try {
        setChannelOptions(child, childOptions, logger);
    } catch (Throwable cause) {
        forceClose(child, cause);
        return;
    }
    setAttributes(child, childAttrs);

    try {
        childGroup.register(child).addListener(future -> {
            if (!future.isSuccess()) {
                forceClose(child, future.cause());
            }
        });
    } catch (Throwable t) {
        forceClose(child, t);
    }
}
```

这是 Accept 流程中最为关键的一环。当 `NioServerSocketChannel` 接受了一个新连接后，会触发 `fireChannelRead(nioSocketChannel)`，由 `ServerBootstrapAcceptor` 处理。完整步骤：

1. **添加用户 Handler**：将用户通过 `childHandler()` 配置的 Handler 添加到子 Channel 的 Pipeline 中。这些 Handler 通常包含业务编解码器和业务逻辑处理器。
2. **设置 Channel 选项**：将 `childOptions` 中配置的选项（如 `TCP_NODELAY`、`SO_KEEPALIVE` 等）应用到子 Channel。
3. **设置属性**：将 `childAttrs` 中的自定义属性绑定到子 Channel。
4. **注册到 childGroup**：调用 `childGroup.register(child)` 将子 Channel 注册到工作 EventLoopGroup。这是 Accept 流程的最后一步，注册完成后 Channel 就能开始读写数据。
5. **错误处理**：注册失败时通过 `forceClose()` 强制关闭 Channel，确保不会泄漏资源。

#### exceptionCaught() -- 异常恢复机制

```java
@Override
public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    final ChannelConfig config = ctx.channel().config();
    if (config.isAutoRead()) {
        config.setAutoRead(false);
        ctx.channel().eventLoop().schedule(enableAutoReadTask, 1, TimeUnit.SECONDS);
    }
    ctx.fireExceptionCaught(cause);
}
```

当 Accept 操作出现异常（如文件描述符耗尽），Netty 会临时关闭 autoRead，暂停接受新连接 1 秒钟，给系统恢复的时间。这是对 [Netty issue #1328](https://github.com/netty/netty/issues/1328) 的修复，防止在系统资源不足时进入 CPU 空转的死循环。

### 3.4 AbstractNioChannel.connect() -- 连接发起

**路径**: `transport/src/main/java/io/netty/channel/nio/AbstractNioChannel.java`

```java
@Override
public final void connect(
        final SocketAddress remoteAddress, final SocketAddress localAddress, final ChannelPromise promise) {
    if (promise.isDone() || !ensureOpen(promise)) {
        return;
    }

    try {
        if (connectPromise != null) {
            throw new ConnectionPendingException();
        }

        boolean wasActive = isActive();
        if (doConnect(remoteAddress, localAddress)) {
            fulfillConnectPromise(promise, wasActive);
        } else {
            connectPromise = promise;
            requestedRemoteAddress = remoteAddress;

            final int connectTimeoutMillis = config().getConnectTimeoutMillis();
            if (connectTimeoutMillis > 0) {
                connectTimeoutFuture = eventLoop().schedule(new Runnable() {
                    @Override
                    public void run() {
                        ChannelPromise connectPromise = AbstractNioChannel.this.connectPromise;
                        if (connectPromise != null && !connectPromise.isDone()
                                && connectPromise.tryFailure(new ConnectTimeoutException(
                                        "connection timed out after " + connectTimeoutMillis + " ms: " +
                                                remoteAddress))) {
                            close(voidPromise());
                        }
                    }
                }, connectTimeoutMillis, TimeUnit.MILLISECONDS);
            }

            promise.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) {
                    if (future.isCancelled()) {
                        if (connectTimeoutFuture != null) {
                            connectTimeoutFuture.cancel(false);
                        }
                        connectPromise = null;
                        close(voidPromise());
                    }
                }
            });
        }
    } catch (Throwable t) {
        promise.tryFailure(annotateConnectException(t, remoteAddress));
        closeIfClosed();
    }
}
```

核心逻辑：

1. **防止重复连接**：通过检查 `connectPromise != null` 确保同一时间只有一个连接操作在进行中。如果已有一个连接正在进行，抛出 `ConnectionPendingException`。
2. **尝试立即连接**：调用 `doConnect()`。在 NIO 非阻塞模式下，`SocketChannel.connect()` 可能立即返回 `true`（本地连接或回环地址），也可能返回 `false`（需要等待网络握手完成）。
3. **异步连接处理**（`doConnect()` 返回 `false`）：
   - 保存 `connectPromise`，后续在 `finishConnect()` 中使用
   - 启动连接超时定时器，超时后自动关闭连接并通知 Promise 失败
   - 监听 Promise 的取消事件，如果用户取消了连接操作，关闭底层 Channel

### 3.5 NioSocketChannel.doConnect() -- NIO 连接实现

**路径**: `transport/src/main/java/io/netty/channel/socket/nio/NioSocketChannel.java`

```java
@Override
protected boolean doConnect(SocketAddress remoteAddress, SocketAddress localAddress) throws Exception {
    if (localAddress != null) {
        doBind0(localAddress);
    }

    boolean success = false;
    try {
        boolean connected = SocketUtils.connect(javaChannel(), remoteAddress);
        if (!connected) {
            addAndSubmit(NioIoOps.CONNECT);
        }
        success = true;
        return connected;
    } finally {
        if (!success) {
            doClose();
        }
    }
}
```

逐行分析：

1. **本地地址绑定**：如果指定了 `localAddress`，先绑定本地地址。这在多网卡环境下很有用。
2. **`SocketUtils.connect()`**：调用 JDK `SocketChannel.connect()`。在非阻塞模式下：
   - 返回 `true`：连接已立即建立（如同机连接、回环地址）
   - 返回 `false`：连接正在进行中（TCP 三次握手未完成）
3. **`addAndSubmit(NioIoOps.CONNECT)`**：如果连接未立即完成，向 Selector 注册 `OP_CONNECT` 事件。当 TCP 握手完成时，Selector 会通知 Channel。
4. **异常时关闭**：如果 `connect()` 抛出异常，确保在 `finally` 块中关闭 Channel。

### 3.6 AbstractNioChannel.finishConnect() -- 完成连接

**路径**: `transport/src/main/java/io/netty/channel/nio/AbstractNioChannel.java`

```java
@Override
public final void finishConnect() {
    assert eventLoop().inEventLoop();

    try {
        boolean wasActive = isActive();
        doFinishConnect();
        fulfillConnectPromise(connectPromise, wasActive);
    } catch (Throwable t) {
        fulfillConnectPromise(connectPromise, annotateConnectException(t, requestedRemoteAddress));
    } finally {
        if (connectTimeoutFuture != null) {
            connectTimeoutFuture.cancel(false);
        }
        connectPromise = null;
    }
}
```

当 Selector 检测到 `OP_CONNECT` 事件时，会调用 `finishConnect()`：

1. **`doFinishConnect()`**：调用 JDK `SocketChannel.finishConnect()`。这个方法会阻塞（在非阻塞 Channel 上应该立即返回）直到连接完成，或者如果连接失败则抛出异常。
2. **`fulfillConnectPromise()`**：通知连接结果。
3. **清理资源**：取消超时定时器，清除 `connectPromise`。

#### fulfillConnectPromise() -- 通知连接结果

```java
private void fulfillConnectPromise(ChannelPromise promise, boolean wasActive) {
    if (promise == null) {
        return;
    }

    boolean active = isActive();
    boolean promiseSet = promise.trySuccess();

    if (!wasActive && active) {
        pipeline().fireChannelActive();
    }

    if (!promiseSet) {
        close(voidPromise());
    }
}
```

- **`promise.trySuccess()`**：通知连接成功。如果用户已经取消了连接（`trySuccess` 返回 `false`），则关闭 Channel。
- **`fireChannelActive()`**：如果 Channel 从非活跃变为活跃状态，触发 ChannelActive 事件。这会启动 Pipeline 中的自动读取逻辑。

### 3.7 NioSocketChannel.doFinishConnect()

```java
@Override
protected void doFinishConnect() throws Exception {
    if (!javaChannel().finishConnect()) {
        throw new UnsupportedOperationException(
                "finishConnect is not supported for " + getClass().getName());
    }
}
```

直接调用 JDK `SocketChannel.finishConnect()`。如果在非阻塞模式下 `finishConnect()` 返回 `false`（理论上不应该发生），则抛出异常。

### 3.8 事件分发：AbstractNioUnsafe.handle()

**路径**: `transport/src/main/java/io/netty/channel/nio/AbstractNioChannel.java`

```java
@Override
public void handle(IoRegistration registration, IoEvent event) {
    try {
        NioIoEvent nioEvent = (NioIoEvent) event;
        NioIoOps nioReadyOps = nioEvent.ops();

        if (nioReadyOps.contains(NioIoOps.CONNECT)) {
            removeAndSubmit(NioIoOps.CONNECT);
            unsafe().finishConnect();
        }

        if (nioReadyOps.contains(NioIoOps.WRITE)) {
            forceFlush();
        }

        if (nioReadyOps.contains(NioIoOps.READ_AND_ACCEPT) || nioReadyOps.equals(NioIoOps.NONE)) {
            read();
        }
    } catch (CancelledKeyException ignored) {
        close(voidPromise());
    }
}
```

这是 NIO 事件分发的枢纽方法。事件处理的优先顺序：

1. **CONNECT 优先**：先处理 CONNECT 事件（如果有），因为需要先确认连接状态才能进行读写
2. **WRITE 次之**：处理写事件，释放可能占用的内存
3. **READ/ACCEPT 最后**：处理读取/接受事件

对于 CONNECT 事件：
- 首先移除 `OP_CONNECT` 注册（否则 Selector 会持续返回，导致空转，参见 [Netty issue #924](https://github.com/netty/netty/issues/924)）
- 然后调用 `finishConnect()` 完成连接握手

## 四、设计思想

### 4.1 服务端/客户端 Channel 分离

Netty 将 Channel 分为两大类：

| 类型 | 类 | 职责 |
|------|-----|------|
| 服务端 Channel | `NioServerSocketChannel` | 监听端口、接受连接 |
| 客户端 Channel | `NioSocketChannel` | 数据读写 |

`NioServerSocketChannel` 继承 `AbstractNioMessageChannel`（面向消息），`NioSocketChannel` 继承 `AbstractNioByteChannel`（面向字节流）。这种分离使得每种 Channel 只关注自己的职责，不需要处理不支持的操作。

### 4.2 Acceptor 模式 -- 工厂与流水线

`ServerBootstrapAcceptor` 是典型的 **Acceptor 模式**实现：

1. **工厂角色**：接收原始连接（`Channel` 对象），完成初始化（添加 Handler、设置选项）
2. **流水线角色**：将初始化后的 Channel 注册到工作 EventLoopGroup

这种设计将"接受连接"和"处理连接"完全解耦，acceptor EventLoop 专注于接受新连接，不会被业务逻辑阻塞。

### 4.3 非阻塞连接的异步处理

Netty 的 Connect 流程完美体现了异步编程思想：

```
connect() 调用
  |
  +-- 立即成功 --> 直接通知 Promise, 触发 ChannelActive
  |
  +-- 异步等待 --> 注册 OP_CONNECT, 返回等待
       |
       +-- Selector 通知 OP_CONNECT --> finishConnect() --> 通知 Promise
       |
       +-- 超时 --> ConnectTimeoutException --> 关闭 Channel
       |
       +-- 用户取消 --> 关闭 Channel
```

所有路径都有明确的处理，没有遗漏的状态。

### 4.4 防御性编程

源码中处处体现防御性编程：

- **`doConnect()` 的 try-finally**：异常时确保关闭 Channel
- **`fulfillConnectPromise()` 的 `trySuccess()`**：使用 `trySuccess` 而非 `setSuccess`，避免与 `cancel()` 的竞态条件
- **`connectPromise != null` 检查**：防止重复连接
- **`CancelledKeyException` 捕获**：处理 SelectionKey 被取消的情况

## 五、模块交互

### 5.1 Bootstrap 与 Channel 的交互

```
ServerBootstrap
  |
  +-- bind() --> 创建 NioServerSocketChannel --> init() --> 注册到 EventLoop
  |
  +-- init() 中添加 ChannelInitializer
       |
       +-- ChannelInitializer.initChannel() 在注册时被调用
            |
            +-- 添加用户 Handler
            +-- 添加 ServerBootstrapAcceptor
```

### 5.2 EventLoop 与 Channel 的交互

```
NioEventLoop
  |
  +-- register(AbstractNioUnsafe) --> 返回 IoRegistration
  |
  +-- run()
       |
       +-- Selector.select() 获取就绪事件
       +-- processSelectedKey(SelectionKey)
            |
            +-- AbstractNioUnsafe.handle(IoEvent)
                 |
                 +-- CONNECT --> finishConnect()
                 +-- WRITE --> flush()
                 +-- READ/ACCEPT --> read()
```

### 5.3 Accept 完整交互时序

```
1. Boss EventLoop: Selector 检测到 OP_ACCEPT
2. Boss EventLoop: NioMessageUnsafe.read()
3. Boss EventLoop: NioServerSocketChannel.doReadMessages() -> accept()
4. Boss EventLoop: 创建 NioSocketChannel
5. Boss EventLoop: pipeline.fireChannelRead(nioSocketChannel)
6. Boss EventLoop: ServerBootstrapAcceptor.channelRead()
7. Boss EventLoop: 添加 childHandler
8. Boss EventLoop: childGroup.register(child) --> 提交到 Worker EventLoop
9. Worker EventLoop: doRegister() -- 注册 OP_READ
10. Worker EventLoop: fireChannelRegistered()
11. Worker EventLoop: pipeline 初始化 (handlerAdded, channelRegistered)
12. Worker EventLoop: fireChannelActive()
13. Worker EventLoop: beginRead() -- 注册读事件，开始接收数据
```

## 六、关键流程详解

### 6.1 一次完整的 Accept 流程

以一个典型的 TCP 服务端为例：

```java
ServerBootstrap b = new ServerBootstrap();
b.group(bossGroup, workerGroup)
 .channel(NioServerSocketChannel.class)
 .childHandler(new ChannelInitializer<SocketChannel>() {
     @Override
     protected void initChannel(SocketChannel ch) {
         ch.pipeline().addLast(new MyHandler());
     }
 });
b.bind(8080);
```

**阶段一：服务端启动**
1. `ServerBootstrap.bind()` 触发 `init()`，创建 `NioServerSocketChannel`
2. `ChannelInitializer` 将 `ServerBootstrapAcceptor` 添加到 Pipeline
3. 注册到 Boss EventLoopGroup，绑定端口，开始监听

**阶段二：接受连接**
1. 客户端发起连接，Boss EventLoop 的 Selector 检测到 `OP_ACCEPT`
2. `NioMessageUnsafe.read()` 被调用
3. `doReadMessages()` 中 `accept()` 返回 JDK `SocketChannel`
4. 包装为 `NioSocketChannel`，加入 `readBuf` 列表
5. `pipeline.fireChannelRead(nioSocketChannel)` 触发 `ServerBootstrapAcceptor`

**阶段三：初始化子 Channel**
1. `ServerBootstrapAcceptor.channelRead()` 将 `childHandler` 添加到子 Channel
2. 应用 `childOptions` 和 `childAttrs`
3. `childGroup.register(child)` 将子 Channel 注册到 Worker EventLoopGroup

**阶段四：子 Channel 就绪**
1. Worker EventLoop 完成 Channel 注册
2. Pipeline 初始化完成
3. `fireChannelActive()` 触发，Channel 进入活跃状态
4. 如果 `autoRead=true`，自动注册 `OP_READ`，开始接收数据

### 6.2 一次完整的 Connect 流程

```java
Bootstrap b = new Bootstrap();
b.group(group)
 .channel(NioSocketChannel.class)
 .handler(new MyHandler());
ChannelFuture f = b.connect("127.0.0.1", 8080).sync();
```

**阶段一：发起连接**
1. `Bootstrap.connect()` 触发 `init()` 和 `doConnect()`
2. `NioSocketChannel.doConnect()` 调用 `SocketChannel.connect()`
3. 若为非本地连接，返回 `false`，注册 `OP_CONNECT`

**阶段二：连接完成**
1. EventLoop Selector 检测到 `OP_CONNECT`
2. `handle()` 方法中移除 `OP_CONNECT`，调用 `finishConnect()`
3. `doFinishConnect()` 完成 JDK 层连接
4. `fulfillConnectPromise()` 通知连接成功
5. `fireChannelActive()` 触发，Channel 进入活跃状态

**阶段三：异常路径**
- **连接超时**：`connectTimeoutFuture` 触发，通知 `ConnectTimeoutException`，关闭 Channel
- **连接拒绝**：`doFinishConnect()` 抛出 `ConnectException`，通过 `annotateConnectException()` 增强错误信息
- **用户取消**：Promise 的 cancel listener 触发，关闭 Channel

### 6.3 readPending 与 AutoRead 的协调

Netty 的读取控制是一个精妙的协调机制：

```
autoRead=true  --> 每次 readComplete 后自动重新注册 OP_READ/OP_ACCEPT
autoRead=false --> 需要显式调用 channel.read() 才会注册读事件
```

在 `NioMessageUnsafe.read()` 的 finally 块中：

```java
if (!readPending && !config.isAutoRead()) {
    removeReadOp();
}
```

这意味着：
- 如果 `autoRead=true`，`readComplete` 后 Pipeline 会自动触发下一轮读取
- 如果 `autoRead=false`，需要用户在 Handler 中显式调用 `ctx.read()` 来触发读取
- 如果用户在 `channelRead()` 或 `channelReadComplete()` 中调用了 `read()`，`readPending` 会设为 `true`，不会移除读事件

## 七、学习要点

### 7.1 设计模式

| 模式 | 应用场景 | 体现类 |
|------|---------|--------|
| **Acceptor 模式** | 连接接受与初始化 | `ServerBootstrapAcceptor` |
| **模板方法** | Channel 操作的骨架 | `doConnect()`、`doReadMessages()` |
| **工厂模式** | Channel 创建 | `ServerBootstrap.init()` |
| **异步回调** | 连接结果通知 | `ChannelPromise` + `ChannelFutureListener` |
| **观察者模式** | 事件传播 | `Pipeline.fireChannelRead()` |

### 7.2 线程模型理解

- **Boss 线程**：负责 Accept 新连接，通常只需 1 个线程
- **Worker 线程**：负责已连接 Channel 的 I/O 读写，通常为 CPU 核心数 * 2
- **所有 Channel 操作必须在 EventLoop 线程中执行**：通过 `assert eventLoop().inEventLoop()` 保证
- **register 操作可以跨线程**：`childGroup.register()` 可以从 Boss 线程提交到 Worker 线程

### 7.3 非阻塞 I/O 的关键点

1. **`accept()` 可能返回 null**：非阻塞模式下没有待接受连接时返回 null
2. **`connect()` 可能不立即完成**：需要注册 `OP_CONNECT` 等待就绪
3. **`finishConnect()` 必须被调用**：否则 `OP_CONNECT` 事件会持续触发
4. **处理完 `OP_CONNECT` 后必须移除**：否则 Selector 会空转

### 7.4 异常处理策略

- **资源泄漏防护**：异常时确保关闭底层 Channel（`forceClose()`、`doClose()`）
- **优雅降级**：Accept 异常时暂停 1 秒再恢复，避免 CPU 空转
- **错误信息增强**：`annotateConnectException()` 在异常消息中附加远程地址信息
- **Promise 通知**：所有异步操作的结果都通过 Promise 明确通知调用方

### 7.5 与 JDK NIO 的对比

| 特性 | JDK NIO | Netty |
|------|---------|-------|
| 连接管理 | 手动管理 `SelectionKey` | Channel + EventLoop 自动管理 |
| 连接超时 | 需自行实现 | 内置 `connectTimeout` 机制 |
| 线程模型 | 需自建 Selector 循环 | EventLoop 线程模型 |
| 资源释放 | 手动 close | Pipeline 生命周期管理 |
| 错误处理 | 各自实现 | 统一的异常传播机制 |
