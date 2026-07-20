# CLAUDE.md

本文件为 Claude Code (claude.ai/code) 在本仓库中工作时提供指引。

## 项目概述

Netty 是一个异步事件驱动的网络应用框架，用于快速开发高性能、可维护的协议服务器和客户端。4.2 版本要求 Java 8+（io_uring 需要 Java 9+）。

## 构建命令

### Docker 构建（推荐，与 CI 一致）
```bash
docker compose -f docker/docker-compose.yaml -f docker/docker-compose.centos-7.111.yaml run build
```

### 本地构建
```bash
# 完整构建，跳过测试
./mvnw -B -ntp clean install -DskipTests

# 完整构建并运行测试
./mvnw -B -ntp clean install

# 仅构建指定模块
./mvnw -B -ntp -pl buffer -am clean install

# 开启泄漏检测构建
./mvnw -B -ntp -Pleak clean install
```

### 运行测试
```bash
# 运行模块内所有测试
./mvnw -B -ntp -pl transport test

# 运行单个测试类
./mvnw -B -ntp -pl transport test -Dtest=NioEventLoopTest

# 运行单个测试方法
./mvnw -B -ntp -pl transport test -Dtest=NioEventLoopTest#testScheduledTask

# 使用 BoringSSL 运行
./mvnw -B -ntp -Pboringssl clean install
```

### 常用 Maven 参数
- `-B` — 批处理模式
- `-ntp` — 隐藏传输进度
- `-pl <module>` — 指定构建模块
- `-am` — 同时构建依赖模块
- `-Pleak` — 启用泄漏检测 profile
- `-Pboringssl` — 启用 BoringSSL profile
- `-DskipTests` — 跳过测试
- `-Dcheckstyle.skip=true` — 跳过 checkstyle
- `-Dforbiddenapis.skip=true` — 跳过 forbidden API 检查
- `-Drevapi.skip=true` — 跳过 API 兼容性检查

## 模块结构

### 核心模块（自底向上）
- `common` — 基础工具：并发（`HashedWheelTimer`、`Recycler`、`FastThreadLocal`）、日志、引用计数、资源泄漏检测
- `buffer` — `ByteBuf` 抽象与内存池（`PooledByteBufAllocator`、`PoolArena`、`PoolChunk`）
- `transport` — `Channel`、`EventLoop`、`Bootstrap`、`Pipeline` — I/O 主干
- `transport-native-unix-common` — 原生传输共享基础（JNI、Unix 域套接字）
- `transport-native-epoll` — Linux epoll 原生传输（边缘触发）
- `transport-native-kqueue` — macOS kqueue 原生传输
- `transport-native-io_uring` — Linux io_uring 原生传输（需 Java 9+）

### 编解码模块
- `codec-base` — 编解码基础框架：`ByteToMessageDecoder`、`MessageToByteEncoder`
- `codec` — 通用编解码器：长度字段、分隔符、定长帧解码器
- `codec-http` — HTTP/1.1 编解码
- `codec-http2` — HTTP/2 编解码（HPACK、流控、流管理）
- `codec-http3` — HTTP/3 + QUIC 编解码
- `codec-compression` — 压缩编解码（zlib、brotli、lz4、zstd、snappy）
- `codec-dns`、`codec-haproxy`、`codec-memcache`、`codec-mqtt`、`codec-redis`、`codec-smtp`、`codec-socks`、`codec-stomp` — 各协议编解码

### 处理器与解析器
- `handler` — SSL/TLS（`SslHandler`）、空闲检测（`IdleStateHandler`）、日志、流量整形、IP 过滤
- `handler-proxy` — SOCKS/HTTP 代理处理器
- `handler-ssl-ocsp` — OCSP 装订支持
- `resolver` — 地址解析器框架
- `resolver-dns` — DNS 名称解析器

### 测试与示例
- `testsuite` — 跨传输层测试套件
- `testsuite-http2` — HTTP/2 专项测试
- `testsuite-native-image` — GraalVM 原生镜像测试
- `microbench` — JMH 基准测试
- `example` — 示例实现（echo、HTTP、HTTP/2、MQTT、Redis、WebSocket 等）

## 核心架构

### 五大抽象
1. **ByteBuf**（`buffer/`）— 引用计数字节缓冲区，读写索引独立。支持池化/非池化分配器，通过 `CompositeByteBuf`、`SlicedByteBuf`、`DuplicatedByteBuf` 实现零拷贝。
2. **Channel**（`transport/`）— 网络连接抽象。生命周期：register → active → inactive → deregister。每个 Channel 绑定一个 EventLoop。
3. **EventLoop**（`transport/`）— 单线程 I/O 事件循环（继承 `EventExecutor`）。处理 I/O 事件和定时任务。`MultithreadEventLoopGroup` 将 Channel 分配到各线程。
4. **ChannelPipeline**（`transport/`）— `ChannelHandlerContext` 双向链表。入站事件 head→tail 传播，出站事件 tail→head 传播。
5. **ChannelHandler**（`transport/`）— `ChannelInboundHandler`（读事件）和 `ChannelOutboundHandler`（写事件）。`ChannelDuplexHandler` 合并两者。

### 启动流程
```
ServerBootstrap.bind()
  → ChannelFactory 创建 NioServerSocketChannel
  → EventLoopGroup.register(channel)
  → ChannelPipeline.addLast(handlers)
  → channel.bind(localAddress)
```

### 入站数据流
```
Selector OP_READ
  → NioEventLoop.processSelectedKey()
  → NioByteUnsafe.read()
  → 分配 ByteBuf → SocketChannel.read(buf)
  → Pipeline.fireChannelRead(msg)
  → Handler 链顺序处理
```

### 出站数据流
```
Channel.write(msg)
  → Pipeline.write() tail→head 传播
  → HeadContext.write() → ChannelOutboundBuffer.add(msg)
  → Channel.flush() → SocketChannel.write(bufs)
```

## 编码规范

- 所有公开 API 类必须有 Javadoc
- 内部实现类放在 `*.internal` 包中
- 线程安全：EventLoop 绑定的对象无需同步；共享对象使用 `synchronized` 或 `java.util.concurrent`
- 引用计数：实现 `ReferenceCounted`，使用 `ReferenceCountUtil.release()` / `retain()`
- 测试命名：`*Test.java` 为单元测试，`*IT.java` 为集成测试（通过 failsafe 执行）
- 代码库使用 Java 8 语言级别 — 不支持 record、sealed class、pattern matching

## 关键设计模式

- **Reactor 模式**：`EventLoop` = 单线程 reactor，`EventLoopGroup` = 线程池
- **责任链模式**：`Pipeline` + `Handler` 链
- **建造者模式**：`Bootstrap` / `ServerBootstrap` 配置 Channel
- **对象池模式**：`Recycler` 跨线程复用对象
- **享元模式**：`AttributeKey` 作为 Channel 属性键

## 原生传输注意事项

- 原生传输（epoll/kqueue/io_uring）基于 JNI，需要平台特定的本地库
- 使用 `Epoll.isAvailable()` / `Kqueue.isAvailable()` / `IoUring.isAvailable()` 运行时检查可用性
- 原生传输支持边缘触发模式和 `SO_REUSEPORT`
- 生产环境推荐使用 BoringSSL 作为 SSL 提供者（通过 `tcnative`）

## 常见陷阱

- 禁止在 EventLoop 线程中阻塞 — 使用 `EventLoop.execute()` 或卸载到独立线程池
- 用完 `ByteBuf` 必须释放 — 使用 try-with-resources 或 `ReferenceCountUtil.safeRelease()`
- `ChannelHandler` 实例默认跨 Channel 共享 — 需要 `@Sharable` 注解
- Handler 内部使用 `ctx.writeAndFlush()` 而非 `channel().writeAndFlush()`（避免额外的 Pipeline 遍历）
