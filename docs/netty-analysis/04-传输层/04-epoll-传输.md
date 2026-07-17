# Linux epoll 原生传输

## 概述

Netty 的 epoll 传输是 Linux 平台上性能最优的网络 I/O 实现，通过 JNI 直接调用 Linux 内核的 `epoll` 系统调用，绕过了 JDK NIO `Selector` 的抽象层。相比 JDK NIO，epoll 传输避免了 JDK 中的 bug（如臭名昭著的 epoll bug 导致的 CPU 100% 空轮询），并暴露了更多 Linux 特有的 TCP 选项（如 `TCP_FASTOPEN`、`SO_REUSEPORT`、`TCP_CORK` 等）。

**核心模块**: `transport-classes-epoll`

**关键特性**:
- 通过 JNI 直接调用 `epoll_create`/`epoll_ctl`/`epoll_wait`
- 支持 Linux 特有的 TCP 优化选项
- 支持 `sendmmsg`/`recvmmsg` 批量系统调用
- 支持 `sendfile` 零拷贝文件传输
- 基于 `eventfd` 实现线程唤醒
- 基于 `timerfd` 实现精确超时控制

## 架构图

```
                    +-----------------------------+
                    |      EventLoopGroup         |
                    |  (EpollEventLoopGroup)       |
                    +----------+------------------+
                               |
                    +----------v------------------+
                    |      EpollEventLoop          |
                    |  (SingleThreadIoEventLoop)   |
                    +----------+------------------+
                               |
                    +----------v------------------+
                    |     EpollIoHandler           |
                    |  (IoHandler 实现)             |
                    |                              |
                    |  epollFd   : FileDescriptor  |
                    |  eventFd   : FileDescriptor  |
                    |  timerFd   : FileDescriptor  |
                    |  events    : EpollEventArray |
                    |  registrations: IntObjectMap |
                    +----------+------------------+
                               |
            +------------------+------------------+
            |                                     |
  +---------v---------+              +------------v-----------+
  | EpollSocketChannel|              | EpollServerSocketChannel|
  | (客户端通道)       |              | (服务端通道)             |
  +---------+---------+              +------------+-----------+
            |                                     |
  +---------v---------+              +------------v-----------+
  |AbstractEpollChannel|             |AbstractEpollServerChannel|
  | (基类)             |              | (服务端基类)             |
  +---------+---------+              +------------+-----------+
            |                                     |
  +---------v-------------------------------------v----------+
  |                     LinuxSocket                          |
  |  (JNI 封装, 提供 native socket 操作)                      |
  +----------------------------------------------------------+
            |
  +---------v------------------------------------------------+
  |                      Native                               |
  |  epollCreate / epollCtl / epollWait / splice / sendmmsg  |
  +----------------------------------------------------------+
            |
  +---------v------------------------------------------------+
  |            netty_transport_native_epoll.so                |
  |  (C/JNI 实现, 直接调用 Linux 系统调用)                     |
  +----------------------------------------------------------+
```

## 核心类分析

### 1. EpollIoHandler — 事件循环核心

`EpollIoHandler` 是 epoll 传输的 I/O 处理器，实现了 `IoHandler` 接口，是整个 epoll 事件驱动机制的核心。

**关键字段**:

```java
public class EpollIoHandler implements IoHandler {
    private FileDescriptor epollFd;     // epoll 实例的文件描述符
    private FileDescriptor eventFd;     // 用于线程唤醒的 eventfd
    private FileDescriptor timerFd;     // 用于精确超时的 timerfd
    private final EpollEventArray events;              // epoll 事件数组
    private final IntObjectMap<DefaultEpollIoRegistration> registrations; // fd -> registration 映射
    private final SelectStrategy selectStrategy;       // 选择策略
    private final AtomicLong nextWakeupNanos;          // 唤醒时间控制
    private boolean pendingWakeup;                     // 是否有待处理的唤醒
}
```

**三个核心文件描述符的职责**:

| 文件描述符 | 用途 | epoll 事件 |
|-----------|------|-----------|
| `epollFd` | epoll 实例，管理所有注册的 fd | N/A |
| `eventFd` | 线程唤醒机制 | `EPOLLIN \| EPOLLET`（边沿触发） |
| `timerFd` | 精确定时器，替代 epoll_wait 的 timeout | `EPOLLIN \| EPOLLET`（边沿触发） |

**初始化过程** (`openFileDescriptors`):

```java
public void openFileDescriptors() {
    this.epollFd = Native.newEpollCreate();    // 创建 epoll 实例
    this.eventFd = Native.newEventFd();        // 创建 eventfd
    // eventFd 使用边沿触发模式，只在写入时通知一次
    Native.epollCtlAdd(epollFd.intValue(), eventFd.intValue(),
                       Native.EPOLLIN | Native.EPOLLET);
    this.timerFd = Native.newTimerFd();        // 创建 timerfd
    // timerFd 也使用边沿触发模式
    Native.epollCtlAdd(epollFd.intValue(), timerFd.intValue(),
                       Native.EPOLLIN | Native.EPOLLET);
}
```

**核心事件循环 `run()` 方法**:

```
run(IoHandlerContext context)
    |
    +--> selectStrategy.calculateStrategy() -- 选择策略
    |       |
    |       +--> CONTINUE: 跳过本轮
    |       +--> BUSY_WAIT: epollBusyWait() (忙等待)
    |       +--> SELECT: epollWait() (阻塞等待)
    |
    +--> processReady(events, ready) -- 处理就绪事件
    |       |
    |       +--> 遍历 events 数组
    |       +--> 区分 eventFd / timerFd / 普通 fd
    |       +--> registration.handle(ev) -- 分发事件
    |
    +--> events.increase() -- 动态扩容（如需）
```

**唤醒机制**:

```java
public void wakeup() {
    if (!executor.isExecutorThread(Thread.currentThread())
            && nextWakeupNanos.getAndSet(AWAKE) != AWAKE) {
        // 写入 eventfd 唤醒 epoll_wait
        Native.eventFdWrite(eventFd.intValue(), 1L);
    }
}
```

唤醒流程:
1. 外部线程调用 `wakeup()`
2. 将 `nextWakeupNanos` 设为 `AWAKE`
3. 向 `eventfd` 写入数据
4. `epoll_wait` 因 `eventfd` 上的 `EPOLLIN` 事件返回
5. `processReady` 检测到 `eventFd` 的 fd，清除 `pendingWakeup` 标记

**Registration 状态机**:

```
           submit(ops)
  Pending ---------> Added
     |                  |
     | epollCtlAdd      | epollCtlMod / epollCtlDel
     v                  v
  Cancelled <-------- Cancelled
     |
     | epollCtlDel
     v
  从 registrations map 中移除
```

### 2. AbstractEpollChannel — 通道基类

`AbstractEpollChannel` 是所有 epoll 通道的抽象基类，封装了基于 epoll 的事件处理逻辑。

**关键设计**:

```java
abstract class AbstractEpollChannel extends AbstractChannel implements UnixChannel {
    protected final LinuxSocket socket;      // Linux 原生 socket
    private IoRegistration registration;      // epoll 注册
    private EpollIoOps ops;                   // 当前关注的事件掩码
    protected volatile boolean active;        // 是否活跃
}
```

**事件标志管理**:

```java
// 设置事件标志（如 EPOLLIN）
protected void setFlag(int flag) throws IOException {
    if (ops.contains(flag)) return;  // 已设置，跳过 syscall
    ops = ops.with(EpollIoOps.valueOf(flag));
    if (isRegistered()) {
        registration.submit(ops);     // 通过 epollCtlMod 更新
    }
}

// 清除事件标志
void clearFlag(int flag) throws IOException {
    if (!ops.contains(flag)) return;  // 未设置，跳过 syscall
    ops = ops.without(EpollIoOps.valueOf(flag));
    registration.submit(ops);         // 通过 epollCtlMod 更新
}
```

**AbstractEpollUnsafe 事件处理**:

```java
public void handle(IoRegistration registration, IoEvent event) {
    int ops = epollEvent.ops().value;

    // 处理顺序至关重要，不可随意调整！
    // 1. 先处理 EPOLLOUT（完成连接 / 刷出数据）
    if ((ops & EPOLL_ERR_OUT_MASK) != 0) {
        epollOutReady();
    }
    // 2. 再处理 EPOLLIN（读取数据）
    if ((ops & EPOLL_ERR_IN_MASK) != 0) {
        epollInReady();
    }
    // 3. 最后处理 EPOLLRDHUP（对端关闭写入）
    if ((ops & EPOLL_RDHUP_MASK) != 0) {
        epollRdHupReady();
    }
}
```

**连接完成流程**:

```
doConnect(remote)
    |
    +--> socket.connect(remote)
    |       |
    |       +--> 成功: 返回 true
    |       +--> EINPROGRESS: setFlag(EPOLLOUT), 返回 false
    |
    +--> [等待 EPOLLOUT 事件]
    |
    +--> epollOutReady()
    |       |
    |       +--> finishConnect()
    |               |
    |               +--> socket.finishConnect()
    |               +--> clearFlag(EPOLLOUT)
    |               +--> fulfillConnectPromise()
    |
    +--> fireChannelActive()
```

### 3. EpollSocketChannel / EpollServerSocketChannel

**EpollSocketChannel** 继承自 `AbstractEpollStreamChannel`，实现客户端 TCP 连接。

关键特性 - **TCP Fast Open 客户端**:

```java
boolean doConnect0(SocketAddress remote) throws Exception {
    if (IS_SUPPORTING_TCP_FASTOPEN_CLIENT && config.isTcpFastOpenConnect()) {
        ChannelOutboundBuffer outbound = unsafe().outboundBuffer();
        outbound.addFlush();
        Object curr;
        if ((curr = outbound.current()) instanceof ByteBuf) {
            ByteBuf initialData = (ByteBuf) curr;
            // 在 SYN 包中携带初始数据
            long localFlushedAmount = doWriteOrSendBytes(
                    initialData, (InetSocketAddress) remote, true);
            if (localFlushedAmount > 0) {
                outbound.removeBytes(localFlushedAmount);
                return true;
            }
        }
    }
    return super.doConnect0(remote);
}
```

**EpollServerSocketChannel** 在 `doBind` 中配置 TCP Fast Open 和监听:

```java
protected void doBind(SocketAddress localAddress) throws Exception {
    super.doBind(localAddress);
    if (IS_SUPPORTING_TCP_FASTOPEN_SERVER && (tcpFastopen = config.getTcpFastopen()) > 0) {
        socket.setTcpFastOpen(tcpFastopen);  // 设置 TFO backlog
    }
    socket.listen(config.getBacklog());       // 开始监听
    active = true;
    submitCurrentOps();                       // 注册 EPOLLIN 等待连接
}
```

### 4. LinuxSocket — JNI 桥梁

`LinuxSocket` 继承自 `Socket`，是 Netty 与 Linux 内核 socket 操作之间的 JNI 桥梁。

**封装的 Linux 特有功能**:

| 功能 | 方法 | 对应内核选项 |
|------|------|------------|
| TCP Fast Open | `setTcpFastOpen()` | `TCP_FASTOPEN` |
| TCP Defer Accept | `setTcpDeferAccept()` | `TCP_DEFER_ACCEPT` |
| TCP Quick ACK | `setTcpQuickAck()` | `TCP_QUICKACK` |
| TCP Cork | `setTcpCork()` | `TCP_CORK` |
| TCP Keep-Alive 参数 | `setTcpKeepIdle/Intvl/Cnt()` | `TCP_KEEPIDLE/INTVL/CNT` |
| TCP User Timeout | `setTcpUserTimeout()` | `TCP_USER_TIMEOUT` |
| TCP Not Sent Low At | `setTcpNotSentLowAt()` | `TCP_NOTSENT_LOWAT` |
| IP Free Bind | `setIpFreeBind()` | `IP_FREEBIND` |
| IP Transparent | `setIpTransparent()` | `IP_TRANSPARENT` |
| TCP MD5 签名 | `setTcpMd5Sig()` | `TCP_MD5SIG` |
| UDP GRO | `setUdpGro()` | `UDP_GRO` |
| sendfile | `sendFile()` | `sendfile(2)` |
| sendmmsg/recvmmsg | `sendmmsg()/recvmmsg()` | `sendmmsg(2)/recvmmsg(2)` |

### 5. EpollChannelConfig — 配置管理

**ET vs LT 模式**（已废弃）:

```java
// Netty 4.2 默认使用水平触发模式，不再支持切换
@Deprecated
public EpollMode getEpollMode() {
    return EpollMode.LEVEL_TRIGGERED;  // 固定返回 LT
}

@Deprecated
public EpollChannelConfig setEpollMode(EpollMode mode) {
    // no-op
    return this;
}
```

Netty 在早期版本中支持 Edge-Triggered (ET) 模式，但由于 ET 模式要求一次性读完所有数据，否则不会再收到通知，这与 Netty 的读取控制模型（autoRead / readPending）配合时容易出错。因此 Netty 4.x 最终改为仅使用 Level-Triggered (LT) 模式。

**关键配置选项**:

```java
// 最大每次聚合写入的字节数
private volatile long maxBytesPerGatheringWrite = SSIZE_MAX;

// 要求 RecvByteBufAllocator 必须实现 ExtendedHandle
public EpollChannelConfig setRecvByteBufAllocator(RecvByteBufAllocator allocator) {
    if (!(allocator.newHandle() instanceof RecvByteBufAllocator.ExtendedHandle)) {
        throw new IllegalArgumentException("...");
    }
    super.setRecvByteBufAllocator(allocator);
    return this;
}
```

## 设计思想

### 1. JNI 直接调用 vs JDK Selector

**JDK NIO Selector 的问题**:
- 中间抽象层增加了开销
- epoll bug（JDK-6670302）导致 CPU 空轮询
- 无法使用 Linux 特有的 TCP 优化选项
- `Selector.selectedKeys()` 使用 HashSet 产生大量迭代器

**Netty epoll 的优势**:
- 直接调用内核系统调用，零额外开销
- 通过 `eventfd` + `timerfd` 实现精确唤醒和超时
- 暴露 Linux 全部 TCP 选项
- 事件数组使用直接内存，避免 GC 压力

### 2. eventfd 唤醒机制

传统 Selector 的 `wakeup()` 存在竞态条件，Netty 使用 `eventfd` 解决:

```
外部线程                    EventLoop 线程
   |                            |
   |-- eventFdWrite(1) ------->| epoll_wait 返回
   |   (原子写入)               | (eventfd EPOLLIN)
   |                            |
   |<-- nextWakeupNanos=AWAKE --| 清除 pendingWakeup
```

### 3. timerfd 精确超时

Netty 使用 `timerfd` 而非 `epoll_wait` 的 timeout 参数，原因:
- `timerfd` 可以在 `epoll_wait` 阻塞期间被外部修改
- 避免了调整 timeout 值时的竞态条件
- 支持纳秒级精度

### 4. 操作顺序的重要性

事件处理中 EPOLLOUT -> EPOLLIN -> EPOLLRDHUP 的顺序是硬性要求:
- 先处理 EPOLLOUT: 连接建立可能在 EPOLLIN 之前完成
- 再处理 EPOLLIN: 读取所有可用数据
- 最后处理 EPOLLRDHUP: 确保数据读完后再处理半关闭

## 模块交互

```
+------------------------------------------+
|           EpollEventLoopGroup            |
| 创建 N 个 EpollEventLoop 线程            |
+------+------+-+--------------------------+
       |      |
       v      v
+------+  +---+------+
| Loop |  |  Loop    |
|  #0  |  |  #1     |  ... 每个线程独立的 epoll 实例
+--+---+  +---+------+
   |          |
   v          v
EpollIoHandler  EpollIoHandler
   |              |
   v              v
epollFd_0      epollFd_1      每个 EventLoop 独立的 epoll fd
   |
   +--- registrations (fd -> Channel 映射)
   |
   +--- processReady()
         |
         +---> AbstractEpollChannel.AbstractEpollUnsafe.handle()
               |
               +---> epollInReady()  -> pipeline.fireChannelRead()
               +---> epollOutReady() -> flush0()
               +---> epollRdHupReady() -> shutdownInput()
```

## 关键流程

### 服务端接受连接流程

```
EpollServerSocketChannel.doBind()
    |
    +--> socket.setTcpFastOpen(tcpFastopen)  // 可选
    +--> socket.listen(backlog)
    +--> submitCurrentOps()                   // 注册 EPOLLIN
    |
    [等待 EPOLLIN 事件]
    |
EpollIoHandler.processReady()
    |
    +--> registration.handle(EPOLLIN)
    |
AbstractEpollUnsafe.epollInReady()
    |
    +--> socket.accept() -> 新 fd
    +--> newChildChannel(fd, address)
    |       |
    |       +--> new EpollSocketChannel(this, new LinuxSocket(fd), remoteAddr)
    |
    +--> pipeline.fireChannelRead(childChannel)
    +--> pipeline.fireChannelReadComplete()
```

### 读取数据流程

```
用户调用 channel.read() 或 autoRead=true
    |
    +--> doBeginRead()
    |       +--> setFlag(EPOLLIN)             // 注册读事件
    |
    [等待 EPOLLIN 事件]
    |
epollInReady()
    |
    +--> recvBufAllocHandle.allocate(alloc)   // 分配 ByteBuf
    +--> doReadBytes(byteBuf)                 // socket.recv()
    |       |
    |       +--> 成功: 更新 writerIndex
    |       +--> -1 / EAGAIN: 对端关闭
    |
    +--> pipeline.fireChannelRead(byteBuf)
    +--> pipeline.fireChannelReadComplete()
    |
    +--> shouldStopReading()?
            |
            +--> yes: clearEpollIn0()         // 取消 EPOLLIN
            +--> no: 继续下一轮读取
```

### 写数据流程

```
用户调用 channel.write(msg)
    |
    +--> ChannelOutboundBuffer 缓冲
    |
用户调用 channel.flush()
    |
    +--> doWrite(outboundBuffer)
    |       |
    |       +--> doWriteBytes(): socket.send()
    |       +--> 成功: removeBytes()
    |       +--> EAGAIN: setFlag(EPOLLOUT)    // 等待可写
    |
    [等待 EPOLLOUT 事件]
    |
epollOutReady()
    |
    +--> super.flush0()                       // 继续刷出
    |
    +--> 全部写完: clearFlag(EPOLLOUT)
```

## 学习要点

1. **JNI 性能优化**: 直接调用系统调用比通过 JDK 抽象层快，但代价是需要维护平台相关的本地代码。

2. **eventfd 的妙用**: eventfd 是 Linux 特有的轻量级事件通知机制，比 pipe 更高效（只需一个 fd，无额外缓冲区）。

3. **timerfd 的精确定时**: 与 epoll_wait 的 timeout 相比，timerfd 支持纳秒精度且可被动态修改。

4. **ET vs LT 的取舍**: Netty 选择 LT 模式是因为它与 Netty 的读取控制模型更兼容。ET 要求一次性读完数据，否则永远不会再收到通知。

5. **操作顺序**: epoll 事件处理顺序是经过大量 bug 修复后确定的，不可随意调整。

6. **SO_REUSEPORT**: 通过 `LinuxSocket` 可以设置 `SO_REUSEPORT`，允许多个进程/线程绑定同一端口，内核自动负载均衡。

7. **TCP Fast Open**: 在 `doConnect0` 中实现客户端 TFO，在 `doBind` 中实现服务端 TFO，可以在 SYN/SYN-ACK 中携带数据，减少一个 RTT。

8. **4.2 架构变化**: `EpollEventLoop` 已标记为 `@Deprecated`，推荐使用 `SingleThreadIoEventLoop` + `EpollIoHandler`，这是 Netty 4.2 统一 IoHandler 抽象的一部分。
