# 04-传输层 01 - Netty Bootstrap 启动引导机制深度分析

> **分析范围**：`io.netty.bootstrap` 包核心类，涵盖服务端与客户端的 Channel 创建、初始化、注册、绑定全流程。
>
> **源码版本**：Netty 4.2（分支 `4.2`）

---

## 一、概述

Bootstrap 是 Netty 应用的入口门面，负责将所有离散的配置（EventLoopGroup、Channel 类型、Pipeline Handler、ChannelOption 等）组装为一个可运行的 Channel。整个 `bootstrap` 包的核心设计围绕两条主线展开：

1. **服务端**：`ServerBootstrap` -- 创建 `ServerChannel`（监听连接），接受连接后创建子 Channel 处理 I/O。
2. **客户端**：`Bootstrap` -- 创建普通 `Channel`，发起远程连接。

两条主线共享同一个抽象基类 `AbstractBootstrap`，通过模板方法模式实现「创建 -> 初始化 -> 注册 -> 绑定/连接」的统一流程。

---

## 二、类层次结构

```
AbstractBootstrap<B, C>                    (抽象基类，模板方法)
    |
    +-- Bootstrap                           (客户端启动器)
    |       +-- BootstrapConfig             (只读配置视图)
    |
    +-- ServerBootstrap                     (服务端启动器)
            +-- ServerBootstrapConfig       (只读配置视图)
            +-- ServerBootstrapAcceptor     (内部类，处理新连接)

ChannelFactory<T>                           (Channel 工厂接口)
    |
    +-- ReflectiveChannelFactory<T>         (反射创建 Channel)

ChannelInitializerExtension                 (扩展点，注入全局 Pipeline 策略)
```

---

## 三、核心类逐一分析

### 3.1 AbstractBootstrap -- 模板基类

**职责**：定义 Channel 从创建到注册的完整骨架，封装所有 Bootstrap 共有的配置项。

#### 3.1.1 核心字段

```java
volatile EventLoopGroup group;                              // Channel 绑定的 EventLoopGroup
private volatile ChannelFactory<? extends C> channelFactory; // Channel 创建工厂
private volatile SocketAddress localAddress;                 // 绑定地址
private final Map<ChannelOption<?>, Object> options;         // Channel 选项（LinkedHashMap 保序）
private final Map<AttributeKey<?>, Object> attrs;            // Channel 属性（ConcurrentHashMap 线程安全）
private volatile ChannelHandler handler;                     // 业务 Handler
```

**设计细节**：
- `options` 使用 `LinkedHashMap` 而非 `ConcurrentHashMap`，因为选项之间可能存在验证依赖，需要按配置顺序依次应用，通过 `synchronized` 块保证线程安全。
- `attrs` 使用 `ConcurrentHashMap`，因为属性之间互不依赖，需要更高的并发性能。

#### 3.1.2 channel() -- 指定 Channel 类型

```java
public B channel(Class<? extends C> channelClass) {
    return channelFactory(new ReflectiveChannelFactory<C>(
            ObjectUtil.checkNotNull(channelClass, "channelClass")
    ));
}
```

这是最常用的 Channel 配置方式。内部将 `Class` 包装为 `ReflectiveChannelFactory`，后续通过反射的无参构造器创建实例。如果 Channel 实现没有无参构造器，则需要直接使用 `channelFactory()` 方法传入自定义工厂。

#### 3.1.3 initAndRegister() -- 核心启动骨架

这是整个 Bootstrap 最关键的方法，抽象出了所有 Bootstrap 共享的「创建-初始化-注册」三阶段：

```java
final ChannelFuture initAndRegister() {
    Channel channel = null;
    try {
        // 阶段1：通过工厂反射创建 Channel 实例
        channel = channelFactory.newChannel();
        // 阶段2：子类模板方法，配置 Pipeline / Option / Attribute
        init(channel);
    } catch (Throwable t) {
        // 创建失败兜底：强制关闭并返回失败 Future
        if (channel != null) {
            channel.unsafe().closeForcibly();
            return new DefaultChannelPromise(channel, GlobalEventExecutor.INSTANCE).setFailure(t);
        }
        return new DefaultChannelPromise(new FailedChannel(), GlobalEventExecutor.INSTANCE).setFailure(t);
    }

    // 阶段3：向 EventLoopGroup 注册 Channel
    final ChannelFuture regFuture = config().group().register(channel);
    if (regFuture.cause() != null) {
        if (channel.isRegistered()) {
            channel.close();
        } else {
            channel.unsafe().closeForcibly();
        }
    }
    return regFuture;
}
```

**执行模型解析**：
- `register()` 是异步操作，返回的 `regFuture` 可能已经完成（在 EventLoop 线程中调用时），也可能尚未完成（从外部线程调用时，任务被投递到 EventLoop 的任务队列）。
- 无论哪种情况，后续的 `bind()` / `connect()` 操作都会通过 `channel.eventLoop().execute()` 投递，从而保证在注册完成之后才执行——因为 `register()`、`bind()`、`connect()` 都绑定到同一个 EventLoop 线程的任务队列中，FIFO 顺序执行。

#### 3.1.4 doBind() -- 绑定流程

```java
private ChannelFuture doBind(final SocketAddress localAddress) {
    final ChannelFuture regFuture = initAndRegister();
    final Channel channel = regFuture.channel();

    if (regFuture.isDone()) {
        // 注册已完成，直接执行绑定
        ChannelPromise promise = channel.newPromise();
        doBind0(regFuture, channel, localAddress, promise);
        return promise;
    } else {
        // 注册尚未完成，添加监听器等待注册完成后再绑定
        final PendingRegistrationPromise promise = new PendingRegistrationPromise(channel);
        regFuture.addListener(future -> {
            if (future.cause() != null) {
                promise.setFailure(future.cause());
            } else {
                promise.registered();
                doBind0(regFuture, channel, localAddress, promise);
            }
        });
        return promise;
    }
}
```

`doBind0()` 的关键设计是将 `channel.bind()` 操作投递到 EventLoop 执行，而非直接在当前线程执行：

```java
private static void doBind0(...) {
    channel.eventLoop().execute(() -> {
        if (regFuture.isSuccess()) {
            channel.bind(localAddress, promise).addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
        } else {
            promise.setFailure(regFuture.cause());
        }
    });
}
```

**为什么这么做**：注释明确说明——"This method is invoked before channelRegistered() is triggered. Give user handlers a chance to set up the pipeline in its channelRegistered() implementation." 通过投递到 EventLoop 队列，bind 操作排在 register 回调之后执行，确保用户在 `channelRegistered()` 中添加的 Handler 在 bind 之前已经就位。

#### 3.1.5 PendingRegistrationPromise -- 注册期间的 Promise 委托

```java
static final class PendingRegistrationPromise extends DefaultChannelPromise {
    private volatile boolean registered;

    void registered() { registered = true; }

    @Override
    protected EventExecutor executor() {
        if (registered) {
            return super.executor();  // 使用 Channel 的 EventLoop
        }
        return GlobalEventExecutor.INSTANCE;  // 注册失败时的兜底
    }
}
```

**设计原因**：在 Channel 注册到 EventLoop 之前，`channel.eventLoop()` 返回 `null`。如果注册失败，需要一个始终可用的 `EventExecutor` 来通知监听器，`GlobalEventExecutor.INSTANCE` 充当这个角色。注册成功后切换为 Channel 自身的 EventLoop，保证后续通知在正确的线程执行。

#### 3.1.6 setChannelOptions() / setAttributes() -- 配置应用

```java
static void setChannelOptions(Channel channel, Map.Entry<ChannelOption<?>, Object>[] options,
                               InternalLogger logger) throws Throwable {
    for (Map.Entry<ChannelOption<?>, Object> e: options) {
        setChannelOption(channel, e.getKey(), e.getValue(), logger);
    }
}

private static void setChannelOption(Channel channel, ChannelOption<?> option,
                                      Object value, InternalLogger logger) throws Throwable {
    try {
        if (!channel.config().setOption((ChannelOption<Object>) option, value)) {
            logger.warn("Unknown channel option '{}' for channel '{}' of type '{}'", ...);
        }
    } catch (Throwable t) {
        logger.warn("Failed to set channel option '{}' with value '{}' ...", ...);
        if (CLOSE_ON_SET_OPTION_FAILURE) {
            throw t;  // 默认 true，设置选项失败会导致 Channel 关闭
        }
    }
}
```

`CLOSE_ON_SET_OPTION_FAILURE` 系统属性（默认 `true`）控制设置选项失败时是否关闭 Channel。在生产环境中建议保持默认值，避免 Channel 处于半配置状态。

---

### 3.2 ServerBootstrap -- 服务端启动器

**职责**：创建 `ServerChannel` 监听端口，接受连接后创建子 Channel 并分配给 worker EventLoopGroup。

#### 3.2.1 双 EventLoopGroup 模型

```java
// 父类中的 group（acceptor / boss）
volatile EventLoopGroup group;

// ServerBootstrap 独有的 childGroup（worker）
private volatile EventLoopGroup childGroup;
```

这是 Netty 服务端最核心的架构设计：

| EventLoopGroup | 别名 | 职责 | 管理的 Channel |
|---|---|---|---|
| `group`（parentGroup） | boss | 监听端口，接受 TCP 连接（accept） | `ServerChannel`（如 `NioServerSocketChannel`） |
| `childGroup` | worker | 处理已建立连接的 I/O 读写 | 子 `Channel`（如 `NioSocketChannel`） |

**单 group 简写**：

```java
public ServerBootstrap group(EventLoopGroup group) {
    return group(group, group);  // parent 和 child 共用同一个 group
}
```

对于低连接数场景可以简化配置，但生产环境建议分离——因为 accept 操作如果和 I/O 操作共享线程池，高负载时 accept 可能被 I/O 任务饿死。

**childGroup 默认值**：

```java
@Override
public ServerBootstrap validate() {
    super.validate();
    if (childHandler == null) {
        throw new IllegalStateException("childHandler not set");
    }
    if (childGroup == null) {
        logger.warn("childGroup is not set. Using parentGroup instead.");
        childGroup = config.group();  // 未设置时自动降级为 parentGroup
    }
    return this;
}
```

#### 3.2.2 option vs childOption

```java
// 父类的 option -- 应用于 ServerChannel 本身
public <T> B option(ChannelOption<T> option, T value) { ... }

// ServerBootstrap 独有的 childOption -- 应用于每个新创建的子 Channel
public <T> ServerBootstrap childOption(ChannelOption<T> childOption, T value) { ... }
```

**典型用法**：

```java
new ServerBootstrap()
    .option(ChannelOption.SO_BACKLOG, 128)          // ServerSocket 的 backlog
    .childOption(ChannelOption.SO_KEEPALIVE, true)   // 每个连接的 keepalive
    .childOption(ChannelOption.TCP_NODELAY, true);   // 每个连接的 Nagle 算法
```

同理，`attr()` 设置在 ServerChannel 上的属性，`childAttr()` 设置在每个子 Channel 上。

#### 3.2.3 init() -- ServerChannel 初始化

`ServerBootstrap.init()` 是整条启动链路中最具深度的方法：

```java
@Override
void init(Channel channel) throws Throwable {
    // 1. 将 option 应用到 ServerChannel
    setChannelOptions(channel, newOptionsArray(), logger);
    setAttributes(channel, newAttributesArray());

    ChannelPipeline p = channel.pipeline();

    // 2. 快照当前配置（防御性复制）
    final EventLoopGroup currentChildGroup = childGroup;
    final ChannelHandler currentChildHandler = childHandler;
    final Entry<ChannelOption<?>, Object>[] currentChildOptions = newOptionsArray(childOptions);
    final Entry<AttributeKey<?>, Object>[] currentChildAttrs = newAttributesArray(childAttrs);
    final Collection<ChannelInitializerExtension> extensions = getInitializerExtensions();

    // 3. 添加 ChannelInitializer（一次性 Handler，初始化完成后自动移除）
    p.addLast(new ChannelInitializer<Channel>() {
        @Override
        public void initChannel(final Channel ch) {
            final ChannelPipeline pipeline = ch.pipeline();
            ChannelHandler handler = config.handler();
            if (handler != null) {
                pipeline.addLast(handler);  // 用户设置的 Server Channel Handler
            }

            // 4. 异步添加 ServerBootstrapAcceptor
            ch.eventLoop().execute(() -> {
                pipeline.addLast(new ServerBootstrapAcceptor(
                        ch, currentChildGroup, currentChildHandler,
                        currentChildOptions, currentChildAttrs, extensions));
            });
        }
    });
}
```

**关键设计**：

- **防御性快照**：在 `init()` 被调用时，将 `childGroup`、`childHandler` 等配置复制为 `final` 局部变量。这是因为 `init()` 在当前线程执行，但 `ChannelInitializer.initChannel()` 可能在 EventLoop 线程异步执行，需要保证读取到的是调用时刻的配置值。
- **Acceptor 异步添加**：`ServerBootstrapAcceptor` 通过 `ch.eventLoop().execute()` 异步添加到 Pipeline 末尾。这确保它排在用户 Handler 之后，不会干扰 `channelRegistered()` 回调的触发顺序。

#### 3.2.4 ServerBootstrapAcceptor -- 连接接受器

这是 Netty 服务端接受新连接的核心 Handler，是理解 Boss/Worker 分工的关键。

```java
private static class ServerBootstrapAcceptor extends ChannelInboundHandlerAdapter {
    private final EventLoopGroup childGroup;
    private final ChannelHandler childHandler;
    private final Entry<ChannelOption<?>, Object>[] childOptions;
    private final Entry<AttributeKey<?>, Object>[] childAttrs;
    private final Runnable enableAutoReadTask;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        final Channel child = (Channel) msg;  // msg 就是新接受的子 Channel

        // 步骤1：将用户设置的 childHandler 添加到子 Channel 的 Pipeline
        child.pipeline().addLast(childHandler);

        // 步骤2：应用 childOption 到子 Channel
        try {
            setChannelOptions(child, childOptions, logger);
        } catch (Throwable cause) {
            forceClose(child, cause);
            return;
        }

        // 步骤3：应用 childAttr 到子 Channel
        setAttributes(child, childAttrs);

        // 步骤4：将子 Channel 注册到 childGroup 的某个 EventLoop
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
}
```

**完整数据流**：

```
ServerChannel (boss EventLoop)
    |
    | accept() -> 创建子 Channel 对象
    |
    v
ServerBootstrapAcceptor.channelRead()
    |
    +-- child.pipeline().addLast(childHandler)   // 注入业务 Handler
    +-- setChannelOptions(child, childOptions)    // 设置连接选项
    +-- setAttributes(child, childAttrs)          // 设置连接属性
    +-- childGroup.register(child)                // 注册到 worker EventLoop
            |
            v
        子 Channel 在 worker EventLoop 上开始处理 I/O
```

**异常处理策略**：

```java
@Override
public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    final ChannelConfig config = ctx.channel().config();
    if (config.isAutoRead()) {
        config.setAutoRead(false);  // 暂停接受新连接
        ctx.channel().eventLoop().schedule(enableAutoReadTask, 1, TimeUnit.SECONDS);
    }
    ctx.fireExceptionCaught(cause);
}
```

当 accept 操作持续失败（如文件描述符耗尽）时，暂时关闭 `autoRead` 1 秒，给系统恢复的时间。这与 `enableAutoReadTask` 的预创建配合——在构造时就创建 Runnable，避免在 URLClassLoader 场景下因文件描述符耗尽而无法加载类。

---

### 3.3 Bootstrap -- 客户端启动器

**职责**：创建客户端 Channel，发起 TCP 连接（或用于 UDP 等无连接协议的 bind）。

#### 3.3.1 init() -- 客户端 Channel 初始化

客户端初始化比服务端简单得多：

```java
@Override
void init(Channel channel) throws Throwable {
    ChannelPipeline p = channel.pipeline();
    p.addLast(config.handler());  // 直接添加用户 Handler

    setChannelOptions(channel, newOptionsArray(), logger);
    setAttributes(channel, newAttributesArray());

    // 扩展点回调
    Collection<ChannelInitializerExtension> extensions = getInitializerExtensions();
    if (!extensions.isEmpty()) {
        for (ChannelInitializerExtension extension : extensions) {
            extension.postInitializeClientChannel(channel);
        }
    }
}
```

**对比服务端**：
- 不需要 `ServerBootstrapAcceptor`（客户端不接受连接）
- 不需要 `childOption` / `childAttr` / `childHandler`
- Handler 直接添加而非通过 `ChannelInitializer` 包装

#### 3.3.2 connect() -- 连接流程

`connect()` 是客户端最核心的操作，包含 DNS 解析和实际连接两个阶段：

```java
public ChannelFuture connect(SocketAddress remoteAddress) {
    ObjectUtil.checkNotNull(remoteAddress, "remoteAddress");
    validate();
    return doResolveAndConnect(remoteAddress, config.localAddress());
}
```

`doResolveAndConnect()` 完整流程：

```java
private ChannelFuture doResolveAndConnect(final SocketAddress remoteAddress, final SocketAddress localAddress) {
    // 阶段1：创建 Channel 并注册到 EventLoop
    final ChannelFuture regFuture = initAndRegister();
    final Channel channel = regFuture.channel();

    if (regFuture.isDone()) {
        if (!regFuture.isSuccess()) { return regFuture; }
        // 阶段2：解析地址并连接
        return doResolveAndConnect0(channel, remoteAddress, localAddress, channel.newPromise());
    } else {
        // 等待注册完成后再连接
        final PendingRegistrationPromise promise = new PendingRegistrationPromise(channel);
        regFuture.addListener(future -> {
            if (future.cause() != null) {
                promise.setFailure(future.cause());
            } else {
                promise.registered();
                doResolveAndConnect0(channel, remoteAddress, localAddress, promise);
            }
        });
        return promise;
    }
}
```

`doResolveAndConnect0()` 处理 DNS 解析：

```java
private ChannelFuture doResolveAndConnect0(final Channel channel, SocketAddress remoteAddress,
                                            final SocketAddress localAddress, final ChannelPromise promise) {
    // 1. 解析器被禁用 -> 直接连接
    if (disableResolver) {
        doConnect(remoteAddress, localAddress, promise);
        return promise;
    }

    // 2. 获取解析器
    final EventLoop eventLoop = channel.eventLoop();
    AddressResolver<SocketAddress> resolver =
        ExternalAddressResolver.getOrDefault(externalResolver).getResolver(eventLoop);

    // 3. 地址已解析或不支持 -> 直接连接
    if (!resolver.isSupported(remoteAddress) || resolver.isResolved(remoteAddress)) {
        doConnect(remoteAddress, localAddress, promise);
        return promise;
    }

    // 4. 异步解析域名
    final Future<SocketAddress> resolveFuture = resolver.resolve(remoteAddress);
    if (resolveFuture.isDone()) {
        // 同步完成（缓存命中或阻塞查询）
        if (resolveFuture.cause() != null) {
            channel.close();
            promise.setFailure(resolveFuture.cause());
        } else {
            doConnect(resolveFuture.getNow(), localAddress, promise);
        }
        return promise;
    }

    // 5. 异步等待解析完成
    resolveFuture.addListener(future -> {
        if (future.cause() != null) {
            channel.close();
            promise.setFailure(future.cause());
        } else {
            doConnect(future.getNow(), localAddress, promise);
        }
    });
    return promise;
}
```

`doConnect()` 将连接操作投递到 EventLoop：

```java
private static void doConnect(final SocketAddress remoteAddress, final SocketAddress localAddress,
                               final ChannelPromise connectPromise) {
    final Channel channel = connectPromise.channel();
    channel.eventLoop().execute(() -> {
        if (localAddress == null) {
            channel.connect(remoteAddress, connectPromise);
        } else {
            channel.connect(remoteAddress, localAddress, connectPromise);
        }
        connectPromise.addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
    });
}
```

**connect 流程总结**：

```
connect("example.com", 8080)
    |
    +-- initAndRegister()
    |       |
    |       +-- channelFactory.newChannel()    // 反射创建 Channel
    |       +-- init(channel)                   // 添加 Handler、设置 Option
    |       +-- group.register(channel)          // 注册到 EventLoop
    |
    +-- doResolveAndConnect0()
            |
            +-- resolver.resolve("example.com")  // DNS 解析
            |
            +-- doConnect(resolvedAddress)
                    |
                    +-- channel.eventLoop().execute(() -> {
                    |       channel.connect(remoteAddress, promise)
                    |   })
                    |
                    +-- CLOSE_ON_FAILURE 监听器
```

---

### 3.4 ReflectiveChannelFactory -- 反射创建 Channel

```java
public class ReflectiveChannelFactory<T extends Channel> implements ChannelFactory<T> {
    private final Constructor<? extends T> constructor;

    public ReflectiveChannelFactory(Class<? extends T> clazz) {
        ObjectUtil.checkNotNull(clazz, "clazz");
        try {
            this.constructor = clazz.getConstructor();  // 缓存无参构造器
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("Class " + StringUtil.simpleClassName(clazz) +
                    " does not have a public non-arg constructor", e);
        }
    }

    @Override
    public T newChannel() {
        try {
            return constructor.newInstance();  // 反射调用
        } catch (Throwable t) {
            throw new ChannelException("Unable to create Channel from class " +
                    constructor.getDeclaringClass(), t);
        }
    }
}
```

**设计要点**：
- 构造器在工厂创建时就通过 `getConstructor()` 查找并缓存，而非每次 `newChannel()` 都查找，提升性能。
- 错误前置：如果 Channel 类没有公共无参构造器，在 `new ReflectiveChannelFactory()` 时就抛出异常，而非等到运行时 `newChannel()` 才失败。
- `toString()` 输出友好的类名信息，方便日志和调试。

**ChannelFactory 接口层次**：

```java
// 顶层接口（bootstrap 包，已废弃）
public interface ChannelFactory<T extends Channel extends io.netty.bootstrap.ChannelFactory<T> {
    T newChannel();
}

// 当前使用的接口（channel 包）
public interface ChannelFactory<T extends Channel> extends io.netty.bootstrap.ChannelFactory<T> {
    @Override
    T newChannel();
}
```

存在两个同名接口是历史遗留——`io.netty.channel.ChannelFactory` 取代了 `io.netty.bootstrap.ChannelFactory`，后者标记为 `@Deprecated`。

---

### 3.5 ChannelInitializerExtension -- 全局扩展点

这是一个 4.2 版本引入的扩展机制，允许在不修改应用代码的前提下，向所有 Netty Channel 的 Pipeline 注入全局策略：

```java
public abstract class ChannelInitializerExtension {
    public static final String EXTENSIONS_SYSTEM_PROPERTY = "io.netty.bootstrap.extensions";

    public double priority() { return 0; }                           // 优先级（升序执行）
    public void postInitializeClientChannel(Channel channel) {}      // 客户端 Channel 初始化后
    public void postInitializeServerListenerChannel(ServerChannel ch) {} // ServerChannel 初始化后
    public void postInitializeServerChildChannel(Channel channel) {} // 子 Channel 初始化后
}
```

**使用场景**：
- 应用级防火墙规则注入
- 全局监控 / 追踪 Handler 注入
- 安全审计 Handler 注入

**启用方式**：需要通过系统属性 `io.netty.bootstrap.extensions=serviceload` 显式启用，默认禁用。通过 Java `ServiceLoader` 机制发现并加载所有扩展实现。

---

## 四、设计思想总结

### 4.1 模板方法模式（Template Method）

`AbstractBootstrap` 定义了 `initAndRegister()` -> `doBind()` 的骨架流程，将 `init()` 声明为抽象方法由子类实现：

| 方法 | AbstractBootstrap 定义 | 子类实现 |
|---|---|---|
| `initAndRegister()` | 创建 + init + register | -- |
| `init(Channel)` | 抽象方法声明 | ServerBootstrap / Bootstrap 各自实现 |
| `validate()` | 检查 group / factory | 子类追加检查（childHandler / handler） |

### 4.2 Boss-Worker 线程模型

```
                     EventLoopGroup (boss)
                          |
            +-------------+-------------+
            |             |             |
        EventLoop1    EventLoop2    EventLoop3
            |             |             |
        ServerChannel   (闲置)        (闲置)
            |
            | accept()
            v
         子 Channel  ----register---->  EventLoopGroup (worker)
                                            |
                                +-----------+-----------+
                                |           |           |
                            EventLoopA  EventLoopB  EventLoopC
                                |           |           |
                           子Channel1   子Channel2   子Channel3
```

- **boss EventLoopGroup**：通常只需 1 个 EventLoop（1 个线程即可处理 accept）
- **worker EventLoopGroup**：默认 `2 * CPU核心数` 个 EventLoop，每个 EventLoop 绑定多个子 Channel
- 子 Channel 一旦注册到某个 worker EventLoop，后续所有 I/O 事件都在该 EventLoop 线程上处理，无需加锁

### 4.3 异步分层 + 事件驱动

整个启动过程是异步的，但通过 `EventLoop.execute()` 的队列机制保证了正确的执行顺序：

```
initAndRegister() 的 register() -> 投递到 EventLoop 队列 [任务A: register]
doBind0() 的 channel.bind()     -> 投递到 EventLoop 队列 [任务A, 任务B: bind]
```

由于任务 A 在任务 B 之前入队，EventLoop 按 FIFO 执行，天然保证 register 在 bind 之前完成。

### 4.4 防御性编程

- **配置快照**：`init()` 中将可变配置快照为 `final` 局部变量，避免跨线程可见性问题
- **失败兜底**：`FailedChannel` 作为 `newChannel()` 失败时的占位对象，保证 `Future` 链路不中断
- **选项失败策略**：`CLOSE_ON_SET_OPTION_FAILURE` 控制选项设置失败时是否关闭 Channel
- **accept 异常退避**：accept 失败时暂停 1 秒，防止 CPU 空转

---

## 五、模块交互全景

```
 用户代码
    |
    | new ServerBootstrap().group(boss, worker).channel(NioServerSocketChannel.class)
    |       .option(SO_BACKLOG, 128).childHandler(new MyHandler()).bind(8080)
    v
 ServerBootstrap
    |
    | bind(8080)
    v
 AbstractBootstrap.doBind()
    |
    | initAndRegister()
    |   |
    |   +-- ReflectiveChannelFactory.newChannel()      // 反射创建 NioServerSocketChannel
    |   +-- ServerBootstrap.init(channel)               // 设置 option, 添加 ChannelInitializer
    |   +-- bossGroup.register(channel)                 // 注册到 boss EventLoop (OP_ACCEPT)
    |
    | doBind0() -> channel.bind(8080)                   // 绑定端口，开始监听
    v
 NioServerSocketChannel (boss EventLoop 线程)
    |
    | 有新连接到达 -> 触发 OP_ACCEPT 事件
    v
 ServerBootstrapAcceptor.channelRead()
    |
    +-- NioSocketChannel 子 Channel 创建
    +-- childHandler 添加到子 Pipeline
    +-- childOption / childAttr 应用
    +-- workerGroup.register(child)                     // 注册到 worker EventLoop
    v
 NioSocketChannel (worker EventLoop 线程)
    |
    | 开始处理 OP_READ / OP_WRITE 事件
    v
 用户业务 Handler 处理请求
```

---

## 六、关键流程详解

### 6.1 服务端启动流程（bind）

```
1. ServerBootstrap.bind(port)
2.   -> validate()                    // 检查 group, channelFactory, childHandler
3.   -> doBind(localAddress)
4.      -> initAndRegister()
5.         -> channelFactory.newChannel()   // 创建 NioServerSocketChannel
6.            -> NioServerSocketChannel 构造器
7.               -> 创建 JDK ServerSocketChannel
8.               -> 创建 Pipeline, Unsafe 等
9.         -> init(channel)                 // ServerBootstrap.init()
10.           -> setChannelOptions()        // 应用 SO_BACKLOG 等
11.           -> pipeline.addLast(ChannelInitializer)
12.        -> bossGroup.register(channel)   // 异步注册
13.           -> EventLoop.execute(registerTask)
14.              -> AbstractChannel.register0()
15.                 -> doRegister()          // JDK Selector.register(serverChannel, OP_ACCEPT)
16.                 -> pipeline.fireChannelRegistered()
17.                    -> ChannelInitializer.initChannel()
18.                       -> 添加用户 Handler
19.                       -> 异步添加 ServerBootstrapAcceptor
20.      -> doBind0()
21.         -> channel.eventLoop().execute(bindTask)
22.            -> channel.bind(localAddress)
23.               -> JDK ServerSocketChannel.bind(port, backlog)
24.               -> pipeline.fireChannelActive()
25.                  -> 开始调用 accept() 接受连接
```

### 6.2 客户端连接流程（connect）

```
1. Bootstrap.connect("host", port)
2.   -> validate()
3.   -> doResolveAndConnect(remoteAddress, localAddress)
4.      -> initAndRegister()
5.         -> channelFactory.newChannel()   // 创建 NioSocketChannel
6.         -> init(channel)                 // 添加 Handler, 设置 Option
7.         -> group.register(channel)        // 注册到 EventLoop (但还未 connect)
8.      -> doResolveAndConnect0()
9.         -> resolver.resolve("host")       // DNS 解析
10.        -> doConnect(resolvedAddress)
11.           -> channel.eventLoop().execute(connectTask)
12.              -> channel.connect(remoteAddress)
13.                 -> JDK SocketChannel.connect(remoteAddress)
14.                 -> 注册 OP_CONNECT 到 Selector
15.                 -> 连接完成时触发 channelActive
```

### 6.3 新连接接受流程

```
1. boss EventLoop 检测到 OP_ACCEPT 事件
2.   -> NioServerSocketChannel.doReadMessages()
3.      -> JDK ServerSocketChannel.accept()
4.      -> 创建 NioSocketChannel 包装
5.      -> 返回 List<Channel> (单元素)
6.   -> pipeline.fireChannelRead(childChannel)
7.      -> ServerBootstrapAcceptor.channelRead()
8.         -> child.pipeline().addLast(childHandler)
9.         -> setChannelOptions(child, childOptions)
10.        -> childGroup.register(child)
11.           -> worker EventLoop.execute(registerTask)
12.              -> child 的 register0() / doRegister()
13.                 -> JDK Selector.register(socketChannel, OP_READ)
14.                 -> pipeline.fireChannelRegistered()
15.                 -> pipeline.fireChannelActive()
16.        -> 设置完成监听器，失败时 forceClose
```

---

## 七、学习要点

### 7.1 核心设计模式

| 模式 | 应用位置 | 作用 |
|---|---|---|
| **模板方法** | `AbstractBootstrap.initAndRegister()` | 定义启动骨架，子类实现 `init()` |
| **工厂方法** | `ChannelFactory.newChannel()` | 解耦 Channel 创建与使用 |
| **Builder（流式 API）** | `Bootstrap` / `ServerBootstrap` 的链式调用 | 优雅的配置 API |
| **观察者模式** | `ChannelFuture` + `Listener` | 异步操作结果通知 |
| **责任链** | `ChannelPipeline` + `ChannelHandler` | I/O 事件处理链 |

### 7.2 线程安全策略

| 策略 | 应用位置 |
|---|---|
| **volatile 读** | `group`、`childGroup`、`handler` 等字段的跨线程可见性 |
| **synchronized 块** | `options` / `childOptions` 的读写（LinkedHashMap 非线程安全） |
| **ConcurrentHashMap** | `attrs` / `childAttrs`（属性之间无依赖，允许更高并发） |
| **EventLoop 单线程** | Channel 的所有 I/O 操作绑定到同一个 EventLoop 线程，无需加锁 |
| **防御性快照** | `init()` 中将可变配置复制为 final 局部变量 |

### 7.3 异步执行保证

Netty 通过 `EventLoop.execute()` 的 FIFO 队列机制，隐式保证了操作的执行顺序：

- `register()` 先入队 -> 先执行
- `bind()` / `connect()` 后入队 -> 后执行
- 因此 register 总是在 bind/connect 之前完成，不需要显式的同步屏障

### 7.4 常见陷阱

1. **忘记设置 childHandler**：`ServerBootstrap.validate()` 会抛出 `IllegalStateException`
2. **option 与 childOption 混淆**：`option` 设置在 ServerChannel 上（如 SO_BACKLOG），`childOption` 设置在子 Channel 上（如 SO_KEEPALIVE）
3. **在非 EventLoop 线程操作 Channel**：所有 Channel 操作都应通过 EventLoop 执行，否则可能产生线程安全问题
4. **忽略 ChannelFuture**：`bind()` / `connect()` 返回的 `ChannelFuture` 必须添加监听器或同步等待，否则可能错过错误

### 7.5 与其他模块的关系

```
Bootstrap（启动引导）
    |
    +-- ChannelFactory -> 创建 Channel 实例
    +-- EventLoopGroup -> 提供 EventLoop（线程 + Selector）
    +-- ChannelPipeline -> Handler 链（业务逻辑）
    +-- ChannelOption -> 底层 Socket 选项
    +-- AddressResolver -> DNS 解析（客户端）
    +-- ChannelInitializerExtension -> 全局 Pipeline 扩展
```

---

## 八、类关系速查表

| 类 | 包 | 核心职责 |
|---|---|---|
| `AbstractBootstrap` | `bootstrap` | 启动骨架，initAndRegister/doBind 模板 |
| `Bootstrap` | `bootstrap` | 客户端启动器，connect + DNS 解析 |
| `ServerBootstrap` | `bootstrap` | 服务端启动器，Boss-Worker 模型 |
| `ServerBootstrapAcceptor` | `bootstrap`（内部类） | 接受新连接，注册到 worker |
| `ReflectiveChannelFactory` | `channel` | 反射创建 Channel 实例 |
| `ChannelFactory` | `channel` | Channel 工厂接口 |
| `ChannelInitializerExtension` | `bootstrap` | 全局 Pipeline 扩展点 |
| `AbstractBootstrapConfig` | `bootstrap` | Bootstrap 配置只读视图基类 |
| `BootstrapConfig` | `bootstrap` | 客户端 Bootstrap 配置视图 |
| `ServerBootstrapConfig` | `bootstrap` | 服务端 Bootstrap 配置视图 |
| `PendingRegistrationPromise` | `bootstrap`（内部类） | 注册期间的 Promise 委托 |
