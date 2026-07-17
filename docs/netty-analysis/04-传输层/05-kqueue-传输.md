# macOS kqueue 原生传输

## 概述

Netty 的 kqueue 传输是 macOS/BSD 平台上的原生高性能 I/O 实现，通过 JNI 直接调用 BSD 内核的 `kqueue`/`kevent` 系统调用，绕过 JDK NIO `Selector` 的抽象层。kqueue 与 Linux 的 epoll 功能类似，但 API 设计更统一，使用 `kevent` 结构体来同时描述"要监控的变化"和"返回的事件"。

**核心模块**: `transport-classes-kqueue`

**支持平台**: macOS、FreeBSD、OpenBSD 等 BSD 系统

**关键特性**:
- 通过 JNI 直接调用 `kqueue()`/`kevent()`
- 使用 `EVFILT_READ`/`EVFILT_WRITE` 过滤器代替 `EPOLLIN`/`EPOLLOUT`
- 支持 `EVFILT_USER` 用户事件实现线程唤醒
- 支持 `connectx()` 实现 TCP Fast Open（macOS 特有）
- 统一的事件模型，过滤器可独立添加/删除

## 架构图

```
                    +-------------------------------+
                    |       EventLoopGroup          |
                    |  (KQueueEventLoopGroup)        |
                    +----------+--------------------+
                               |
                    +----------v--------------------+
                    |      KQueueEventLoop           |
                    |  (SingleThreadIoEventLoop)     |
                    +----------+--------------------+
                               |
                    +----------v--------------------+
                    |      KQueueIoHandler           |
                    |  (IoHandler 实现)               |
                    |                                |
                    |  kqueueFd  : FileDescriptor    |
                    |  changeList: KQueueEventArray  |
                    |  eventList : KQueueEventArray  |
                    |  registrations: LongObjectMap  |
                    +----------+--------------------+
                               |
            +------------------+------------------+
            |                                     |
  +---------v----------+            +-------------v-----------+
  |KQueueSocketChannel |            |KQueueServerSocketChannel |
  | (客户端通道)        |            | (服务端通道)              |
  +---------+----------+            +-------------+-----------+
            |                                     |
  +---------v----------+            +-------------v-----------+
  |AbstractKQueueChannel|           |AbstractKQueueServerChannel|
  +---------+----------+            +-------------+-----------+
            |                                     |
  +---------v-------------------------------------v----------+
  |                      BsdSocket                          |
  |  (JNI 封装, BSD socket 操作)                             |
  +----------------------------------------------------------+
            |
  +---------v------------------------------------------------+
  |                     Native                                |
  |  kqueue / kevent / keventAddUserEvent / keventTrigger...  |
  +----------------------------------------------------------+
            |
  +---------v------------------------------------------------+
  |            netty_transport_native_kqueue.dylib            |
  |  (C/JNI 实现, 直接调用 BSD 系统调用)                      |
  +----------------------------------------------------------+
```

## 核心类分析

### 1. KQueueIoHandler — 事件循环核心

`KQueueIoHandler` 是 kqueue 传输的 I/O 处理器，实现了 `IoHandler` 接口。

**关键字段**:

```java
public final class KQueueIoHandler implements IoHandler {
    private final FileDescriptor kqueueFd;                // kqueue 实例
    private final KQueueEventArray changeList;            // 待提交的变更列表
    private final KQueueEventArray eventList;             // 返回的事件列表
    private final LongObjectMap<DefaultKqueueIoRegistration> registrations; // id -> registration
    private final Queue<DefaultKqueueIoRegistration> cancelledRegistrations; // 取消队列
    private int nextId;                                   // 下一个分配的 ID
    private volatile int wakenUp;                         // 唤醒标记

    private static final int KQUEUE_WAKE_UP_IDENT = 0;   // 唤醒事件的 ident (保留值)
}
```

**与 epoll 的关键区别**:

| 特性 | epoll | kqueue |
|------|-------|--------|
| 核心 API | `epoll_create`/`epoll_ctl`/`epoll_wait` | `kqueue()`/`kevent()` |
| 事件标识 | fd (int) | ident + filter (long + short) |
| 注册方式 | `epoll_ctl(ADD/MOD/DEL)` | 往 changeList 添加 kevent |
| 唤醒机制 | `eventfd` | `EVFILT_USER` 用户事件 |
| 定时机制 | `timerfd` | kevent 内置 timeout |
| 事件方向 | EPOLLIN/EPOLLOUT 标志位 | EVFILT_READ/EVFILT_WRITE 过滤器 |
| 注册表键 | fd (int) | 生成的 id (long) |

**初始化过程**:

```java
private KQueueIoHandler(ThreadAwareExecutor executor, int maxEvents, SelectStrategy strategy) {
    this.kqueueFd = Native.newKQueue();           // 创建 kqueue 实例
    this.changeList = new KQueueEventArray(maxEvents);
    this.eventList = new KQueueEventArray(maxEvents);

    // 添加用户事件用于线程唤醒
    int result = Native.keventAddUserEvent(kqueueFd.intValue(), KQUEUE_WAKE_UP_IDENT);
    if (result < 0) {
        destroy();
        throw new IllegalStateException("kevent failed to add user event");
    }
}
```

**核心事件循环 `run()` 方法**:

```java
public int run(IoHandlerContext context) {
    int strategy = selectStrategy.calculateStrategy(selectNowSupplier, !context.canBlock());
    switch (strategy) {
        case SelectStrategy.CONTINUE:
            return 0;
        case SelectStrategy.BUSY_WAIT:
            // kqueue 不支持忙等待，fall-through 到 SELECT
        case SelectStrategy.SELECT:
            strategy = kqueueWait(context, WAKEN_UP_UPDATER.getAndSet(this, 0) == 1);
            // 处理竞态条件：如果在 select 前被唤醒，需要再次唤醒
            if (wakenUp == 1) {
                wakeup0();
            }
    }

    if (strategy > 0) {
        handled = processReady(strategy);
    }
    return handled;
}
```

**事件处理 `processReady()`**:

```java
private int processReady(int ready) {
    int ioCount = 0;
    for (int i = 0; i < ready; ++i) {
        final short filter = eventList.filter(i);
        final short flags = eventList.flags(i);
        final int ident = eventList.ident(i);

        // 跳过用户事件和错误事件
        if (filter == Native.EVFILT_USER || (flags & Native.EV_ERROR) != 0) {
            continue;
        }

        ioCount++;
        long id = eventList.udata(i);
        DefaultKqueueIoRegistration registration = registrations.get(id);
        if (registration != null) {
            registration.handle(ident, filter, flags, fflags, data, id);
        }
    }
    return ioCount;
}
```

**唤醒机制**:

```java
public void wakeup() {
    if (!executor.isExecutorThread(Thread.currentThread())
            && WAKEN_UP_UPDATER.compareAndSet(this, 0, 1)) {
        wakeup0();
    }
}

private void wakeup0() {
    // 触发 EVFILT_USER 事件，唤醒 kevent
    Native.keventTriggerUserEvent(kqueueFd.intValue(), KQUEUE_WAKE_UP_IDENT);
}
```

与 epoll 的 `eventfd` 不同，kqueue 使用原生的 `EVFILT_USER` 机制:
- `keventAddUserEvent`: 注册用户事件过滤器
- `keventTriggerUserEvent`: 触发用户事件（等价于写 eventfd）
- 优点是不需要额外的文件描述符

### 2. Registration 机制

kqueue 的注册机制与 epoll 有本质区别:

**epoll 模式**: 通过 `epoll_ctl(ADD/MOD/DEL)` 直接修改内核状态
**kqueue 模式**: 将 kevent 添加到 `changeList`，在下一次 `kevent_wait` 时批量提交

```java
// DefaultKqueueIoRegistration.submit()
public long submit(IoOps ops) {
    KQueueIoOps kQueueIoOps = cast(ops);
    short filter = kQueueIoOps.filter();
    short flags = kQueueIoOps.flags();

    if (executor.isExecutorThread(Thread.currentThread())) {
        evSet(filter, flags, fflags, data);  // 直接添加到 changeList
    } else {
        executor.execute(() -> evSet(filter, flags, fflags, data));  // 异步添加
    }
    return 0;
}

private void evSet(short filter, short flags, int fflags, long data) {
    changeList.evSet(handle.ident(), filter, flags, fflags, data, id);
}
```

**取消机制**:

```java
public boolean cancel() {
    if (!canceled.compareAndSet(false, true)) return false;
    // 延迟到事件处理完成后才从 map 中移除
    cancellationPending = true;
    cancelledRegistrations.offer(this);
    return true;
}

// 在 processCancelledRegistrations() 中统一处理
private void processCancelledRegistrations() {
    for (;;) {
        DefaultKqueueIoRegistration reg = cancelledRegistrations.poll();
        if (reg == null) return;
        registrations.remove(reg.id);
        reg.handle.unregistered();
    }
}
```

### 3. KQueueSocketChannel / KQueueServerSocketChannel

**KQueueSocketChannel** 的 TCP Fast Open 实现使用 macOS 独有的 `connectx()` 系统调用:

```java
protected boolean doConnect0(SocketAddress remoteAddress, SocketAddress localAddress) throws Exception {
    if (config.isTcpFastOpenConnect()) {
        ChannelOutboundBuffer outbound = unsafe().outboundBuffer();
        outbound.addFlush();
        Object curr;
        if ((curr = outbound.current()) instanceof ByteBuf) {
            ByteBuf initialData = (ByteBuf) curr;
            if (initialData.isReadable()) {
                IovArray iov = new IovArray(config.getAllocator().directBuffer());
                try {
                    iov.add(initialData, initialData.readerIndex(), initialData.readableBytes());
                    // 使用 macOS 的 connectx() 实现 TCP Fast Open
                    int bytesSent = socket.connectx(
                            (InetSocketAddress) localAddress,
                            (InetSocketAddress) remoteAddress, iov, true);
                    writeFilter(true);
                    outbound.removeBytes(Math.abs(bytesSent));
                    return bytesSent > 0;
                } finally {
                    iov.release();
                }
            }
        }
    }
    return super.doConnect0(remoteAddress, localAddress);
}
```

**KQueueServerSocketChannel** 的 bind 过程:

```java
protected void doBind(SocketAddress localAddress) throws Exception {
    super.doBind(localAddress);
    socket.listen(config.getBacklog());
    if (config.isTcpFastOpen()) {
        socket.setTcpFastOpen(true);  // BSD 风格的 TFO 开启
    }
    active = true;
}
```

### 4. BsdSocket — BSD Socket 封装

`BsdSocket` 对应 epoll 的 `LinuxSocket`，封装 BSD 特有的 socket 操作:
- `connectx()`: macOS 的 TCP Fast Open 连接
- `setTcpFastOpen()`: BSD 风格的 TFO 开关（布尔值，非 backlog）
- 标准 BSD socket 操作

## 设计思想

### 1. 过滤器模型 vs 事件模型

**epoll 事件模型**:
```
一个 fd 关注 EPOLLIN | EPOLLOUT | EPOLLRDHUP 组合
通过 epoll_ctl(EPOLL_CTL_MOD) 修改整个关注集合
```

**kqueue 过滤器模型**:
```
每个过滤器（EVFILT_READ / EVFILT_WRITE / EVFILT_USER）独立管理
可以单独添加/删除某个过滤器
一个 kevent 结构体描述一个过滤器的监控规则
```

kqueue 的过滤器模型更加灵活，可以精确控制每个方向的事件订阅。

### 2. Change List + Event List 双缓冲

kqueue 使用 `changelist` 和 `eventlist` 双缓冲设计:
- `changelist`: 应用程序提交要监控的变化（添加/删除/修改过滤器）
- `eventlist`: 内核返回的就绪事件
- 一次 `kevent()` 调用同时处理两者，减少系统调用次数

### 3. 基于 ID 的注册表

epoll 使用 fd 作为注册表的键，kqueue 使用生成的 long 类型 ID:
- 优点: 避免了 fd 复用时的映射冲突
- 缺点: 需要额外维护 id -> registration 的映射

### 4. 取消队列的延迟处理

kqueue 的取消是延迟的，已取消的 registration 先放入 `cancelledRegistrations` 队列，在事件处理完成后统一清理。这避免了在遍历事件时修改注册表导致的 `ConcurrentModificationException`。

## 模块交互

```
+------------------------------------------+
|         KQueueEventLoopGroup             |
+------+------+-+--------------------------+
       |      |
       v      v
+------+  +---+------+
| Loop |  |  Loop    |  每个线程独立的 kqueue 实例
|  #0  |  |  #1     |
+--+---+  +---+------+
   |          |
   v          v
KQueueIoHandler  KQueueIoHandler
   |                 |
   v                 v
kqueueFd_0       kqueueFd_1
   |
   +--- registrations (id -> Channel 映射)
   |
   +--- changeList (待提交的 kevent 变更)
   |
   +--- eventList (返回的就绪事件)
         |
         +---> DefaultKqueueIoRegistration.handle()
               |
               +---> AbstractKQueueUnsafe.handle()
                     |
                     +---> filter == EVFILT_READ  -> epollInReady 等价
                     +---> filter == EVFILT_WRITE -> epollOutReady 等价
                     +---> filter == EVFILT_USER  -> 唤醒处理
```

## 关键流程

### kevent 事件处理流程

```
KQueueIoHandler.run(context)
    |
    +--> kqueueWait(context, oldWakeup)
    |       |
    |       +--> Native.keventWait(kqueueFd, changeList, eventList, timeout)
    |       |       |
    |       |       +--> 内核处理 changeList 中的变更
    |       |       +--> 内核将就绪事件写入 eventList
    |       |       +--> changeList.clear()
    |       |
    |       +--> return eventList 中的事件数量
    |
    +--> processReady(ready)
            |
            +--> 遍历 eventList
            |       |
            |       +--> filter == EVFILT_USER?  跳过（唤醒事件）
            |       +--> flags & EV_ERROR?      跳过（错误事件）
            |       |
            |       +--> 根据 udata(id) 查找 registration
            |       +--> registration.handle(ident, filter, flags, fflags, data, id)
            |
            +--> processCancelledRegistrations()  // 清理已取消的注册
```

## 学习要点

1. **kqueue vs epoll 的 API 设计差异**: kqueue 使用过滤器模型，epoll 使用事件标志模型。kqueue 的 `kevent()` 结构体同时处理"变更提交"和"事件获取"，epoll 需要分别调用 `epoll_ctl` 和 `epoll_wait`。

2. **EVFILT_USER 的唤醒机制**: kqueue 原生支持用户事件过滤器，不需要像 epoll 那样创建额外的 `eventfd` 文件描述符。

3. **macOS TCP Fast Open**: macOS 使用 `connectx()` 系统调用实现 TFO，与 Linux 的 `sendto(MSG_FASTOPEN)` 不同。BSD 的 `setTcpFastOpen` 接受布尔值而非 backlog。

4. **注册表键的选择**: epoll 用 fd 做键天然唯一（fd 是进程级别的唯一标识），kqueue 用生成的 long ID 做键更灵活但需要额外管理。

5. **延迟取消策略**: kqueue 的取消是延迟的——先标记 `cancellationPending`，事件处理完成后再从 map 中移除。这避免了并发修改问题。

6. **平台兼容性**: kqueue 传输仅在 BSD 系统上可用（macOS、FreeBSD 等），Linux 上应使用 epoll，不支持的平台 fallback 到 JDK NIO。

7. **Busy-wait 不支持**: kqueue 没有类似 `epoll_wait` 的 `epollBusyWait` 对应物，`BUSY_WAIT` 策略会降级为 `SELECT`。

8. **与 epoll 的对称性**: `KQueueIoHandler` 和 `EpollIoHandler` 实现了相同的 `IoHandler` 接口，结构高度对称，可以互换使用。
