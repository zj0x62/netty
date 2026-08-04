# Netty 4.2 源码深度分析

> 系统学习 Netty 的设计思想与实现原理，覆盖全部 40+ 模块

> **初学者入口**：如果你刚开始学 Netty，请先阅读 [学习计划](学习计划.md)，按"先写 → 再读 → 最后再写"的三段式执行；学习状态记录在 [学习进度](学习进度.md)，流程规则见 [学习系统操作手册](学习系统操作手册.md)，任何 agent 均可据此随时接续。

## 项目信息

| 项目 | 说明 |
|------|------|
| Netty 版本 | 4.2.17.Final-SNAPSHOT |
| 分析日期 | 2026-07-17 |
| 文档总数 | 66 篇（正文 64 + 导航 2） |
| 覆盖模块 | 40+ |
| 分支 | 4.2 |
| JDK 要求 | Java 8+（io_uring 需 Java 9+） |
| 许可证 | Apache License 2.0 |

## 推荐阅读顺序

### 第一阶段：建立全局视野

在深入源码之前，先建立对 Netty 的整体认知。

| 序号 | 文档 | 内容 |
|------|------|------|
| 1 | [Netty 概述](00-概述与导航/01-netty-概述.md) | Netty 是什么、核心特性、版本演进、模块全景图 |
| 2 | [模块依赖关系](00-概述与导航/02-模块依赖关系.md) | 40+ 模块的依赖关系图与各模块定位 |
| 3 | [核心架构概述](00-概述与导航/03-架构概述.md) | 架构分层图、核心接口设计、职责划分 |
| 4 | [源码阅读地图](00-概述与导航/04-源码阅读地图.md) | 启动、事件循环、数据读写等核心流程的精确代码路径 |

### 第二阶段：核心抽象

| 序号 | 文档 | 内容 |
|------|------|------|
| 5 | [ByteBuf 体系](01-核心抽象/01-bytebuf-体系.md) | ByteBuf 抽象层次、读写索引、派生 Buffer |
| 6 | [Channel 抽象](01-核心抽象/02-channel-抽象.md) | Channel 生命周期、Unsafe 实现、状态管理 |
| 7 | [ChannelHandler 体系](01-核心抽象/03-channel-handler-体系.md) | Handler 类型、生命周期回调、适配器模式 |
| 8 | [EventLoop 体系](01-核心抽象/04-eventloop-体系.md) | EventLoop 模型、线程绑定、任务调度 |

### 第三阶段：流水线与事件驱动

| 序号 | 文档 | 内容 |
|------|------|------|
| 9 | [Pipeline 架构](02-流水线与事件驱动/01-pipeline-架构.md) | DefaultChannelPipeline、HandlerContext 链表 |
| 10 | [事件传播机制](02-流水线与事件驱动/02-事件传播机制.md) | inbound/outbound 传播方向、fire* 方法 |
| 11 | [Future/Promise 体系](02-流水线与事件驱动/03-future-promise-体系.md) | 异步结果模型、GenericFutureListener |

### 第四阶段：内存管理

| 序号 | 文档 | 内容 |
|------|------|------|
| 12 | [Buffer 分配器](03-内存管理/01-buffer-分配器.md) | Pooled/Unpooled 分配策略 |
| 13 | [PoolArena 池化内存](03-内存管理/02-pool-arena-池化内存.md) | Arena/Chunk/Page/SubPage 四级管理 |
| 14 | [Recycler 对象池](03-内存管理/03-recycler-对象池.md) | 跨线程对象回收、WeakOrderQueue |
| 15 | [堆外内存与泄漏检测](03-内存管理/04-堆外内存与泄漏检测.md) | DirectBuffer、ResourceLeakDetector |

### 第五阶段：传输层

| 序号 | 文档 | 内容 |
|------|------|------|
| 16 | [Bootstrap 启动引导](04-传输层/01-bootstrap-启动引导.md) | ServerBootstrap/Bootstrap 配置组装、Channel 创建/注册/绑定流程 |
| 17 | [Accept/Connect](04-传输层/02-accept-connect-连接建立.md) | 服务端 Accept、客户端 Connect 流程 |
| 18 | [Read/Write](04-传输层/03-read-write-数据读写.md) | 数据读写流程、WriteBufferWaterMark |
| 19 | [Epoll 传输](04-传输层/04-epoll-传输.md) | Linux epoll 原生传输、边缘触发 |
| 20 | [Kqueue 传输](04-传输层/05-kqueue-传输.md) | macOS kqueue 原生传输 |
| 21 | [io_uring 传输](04-传输层/06-io_uring-传输.md) | Linux io_uring 异步 I/O |
| 22 | [其他传输](04-传输层/07-其他传输.md) | OIO、Embedded、Local 传输 |

### 第六阶段：编解码框架

| 序号 | 文档 | 内容 |
|------|------|------|
| 23 | [编解码器基础](05-编解码框架/01-编解码器基础.md) | ByteToMessageDecoder、MessageToByteEncoder |
| 24 | [消息编解码](05-编解码框架/02-消息编解码.md) | MessageToMessage 编解码、类型转换 |
| 25 | [帧解码器](05-编解码框架/03-帧解码器.md) | LengthField、Delimiter、FixedLength 帧解码 |
| 26 | [序列化方案](05-编解码框架/04-序列化方案.md) | Protobuf、MessagePack、JBoss Marshalling |

### 第七阶段：协议实现

| 序号 | 文档 | 内容 |
|------|------|------|
| 27 | [HTTP/1.1](06-协议实现/01-http1.1-编解码.md) | HttpRequestDecoder、HttpObjectAggregator |
| 28 | [HTTP/2](06-协议实现/02-http2-编解码.md) | 帧编解码、流管理、HPACK |
| 29 | [HTTP/3 + QUIC](06-协议实现/03-http3-quic.md) | QUIC 传输、HTTP/3 帧映射 |
| 30 | [WebSocket](06-协议实现/04-websocket-编解码.md) | 握手、帧编解码、Ping/Pong |
| 31 | [MQTT](06-协议实现/05-mqtt-编解码.md) | MQTT 3.1.1/5.0 编解码 |
| 32 | [Redis](06-协议实现/06-redis-编解码.md) | RESP 协议编解码 |
| 33 | [DNS](06-协议实现/07-dns-编解码.md) | DNS 协议编解码 |
| 34 | [Memcache](06-协议实现/08-memcache-编解码.md) | Memcache 二进制/文本协议 |
| 35 | [STOMP](06-协议实现/09-stomp-编解码.md) | STOMP 消息帧编解码 |
| 36 | [SMTP/SOCKS/HAProxy](06-协议实现/10-smtp-socks-haproxy-编解码.md) | SMTP、SOCKS 代理、HAProxy 协议 |
| 37 | [压缩编解码](06-协议实现/11-压缩编解码.md) | JdkZlib、Brotli、Lz4、Zstd、Snappy |

### 第八阶段：处理器与扩展

| 序号 | 文档 | 内容 |
|------|------|------|
| 38 | [SSL/TLS](07-处理器与扩展/01-ssl-tls-处理器.md) | SslHandler、SslContext、SNI |
| 39 | [空闲检测](07-处理器与扩展/02-空闲检测.md) | IdleStateEvent、读写空闲超时 |
| 40 | [日志处理器](07-处理器与扩展/03-日志处理器.md) | LoggingHandler、日志级别控制 |
| 41 | [代理处理器](07-处理器与扩展/04-代理处理器.md) | HTTP/SOCKS 代理、ProxyHandler |
| 42 | [流量整形](07-处理器与扩展/05-流量整形.md) | ChannelTrafficShaping、GlobalTrafficShaping |
| 43 | [OCSP](07-处理器与扩展/06-ocsp-证书验证.md) | 在线证书状态协议支持 |

### 第九阶段：域名解析

| 序号 | 文档 | 内容 |
|------|------|------|
| 44 | [基础解析器](08-域名解析/01-基础解析器.md) | AddressResolverGroup、轮询策略 |
| 45 | [DNS 解析器](08-域名解析/02-dns-解析器.md) | DnsNameResolver、递归解析 |

### 第十阶段：工具与基础设施

| 序号 | 文档 | 内容 |
|------|------|------|
| 46 | [通用工具](09-工具与基础设施/01-通用工具.md) | HashedWheelTimer、StringUtil、NetUtil |
| 47 | [AttributeKey](09-工具与基础设施/02-attribute-key.md) | Channel 属性绑定、ConcurrentMap |
| 48 | [资源泄漏检测](09-工具与基础设施/03-资源泄漏检测.md) | ResourceLeakDetector 四级检测 |
| 49 | [并发工具](09-工具与基础设施/04-并发工具.md) | FastThreadLocal、PlatformDependent |

### 第十一阶段：原生集成

| 序号 | 文档 | 内容 |
|------|------|------|
| 50 | [JNI 集成](10-原生集成/01-jni-集成.md) | JNI 调用机制、NativeLibraryLoader |
| 51 | [原生传输](10-原生集成/02-原生传输.md) | Epoll/Kqueue/io_uring 底层实现 |
| 52 | [原生 SSL](10-原生集成/03-原生-ssl.md) | OpenSSL/BoringSSL 集成 |

### 第十二阶段：测试体系

| 序号 | 文档 | 内容 |
|------|------|------|
| 53 | [测试框架](11-测试体系/01-测试框架.md) | EmbeddedChannel、TestUtils |
| 54 | [测试套件](11-测试体系/02-测试套件.md) | 传输层测试套件、协议测试 |
| 55 | [基准测试](11-测试体系/03-基准测试.md) | JMH 基准测试、性能对比 |

### 第十三阶段：扩展话题

| 序号 | 文档 | 内容 |
|------|------|------|
| 56 | [设计模式](12-扩展话题/01-设计模式.md) | Netty 中使用的设计模式总结 |
| 57 | [性能调优](12-扩展话题/02-性能调优.md) | 关键参数、调优策略 |
| 58 | [GraalVM](12-扩展话题/03-graalvm-native-image.md) | Native Image 编译支持 |
| 59 | [IO 模型对比](12-扩展话题/04-io-模型对比.md) | NIO/epoll/io_uring 性能对比 |
| 60 | [框架对比](12-扩展话题/05-框架对比.md) | 与 Mina/Grpc/NIO 对比 |
| 61 | [版本演进](12-扩展话题/06-版本演进.md) | Netty 3.x → 4.x → 5.x 演进 |

### 全链路追踪（强烈推荐阅读）

| 文档 | 内容 |
|------|------|
| [Server Accept 流程](附录-全链路追踪/01-server-accept-流程.md) | 从 ServerBootstrap.bind() 到 accept 新连接的完整链路 |
| [Request Read 流程](附录-全链路追踪/02-request-read-流程.md) | 从 Selector 就绪到业务 Handler 收到数据的完整链路 |
| [Response Write 流程](附录-全链路追踪/03-response-write-流程.md) | 从 Channel.write() 到数据发送至网络的完整链路 |

## 核心设计原则

1. **单线程模型**：一个 EventLoop 绑定一个线程，一个 Channel 绑定一个 EventLoop
2. **异步非阻塞**：基于 Future/Promise 的异步编程模型
3. **零拷贝**：CompositeByteBuf / SlicedByteBuf / DuplicatedByteBuf
4. **内存池化**：Arena / Chunk / Page / SubPage 四级内存管理
5. **对象池化**：Recycler 跨线程对象回收
6. **责任链**：Pipeline / Handler 链式事件处理

## 源码目录结构

```
netty/
├── common/                  ← 基础工具：并发、日志、内存管理
├── buffer/                  ← ByteBuf 抽象与内存池
├── transport/               ← Channel、EventLoop、Bootstrap
├── codec-base/              ← 编解码器基础框架
├── codec/                   ← 常用编解码器集合
├── codec-http/              ← HTTP/1.x 协议
├── codec-http2/             ← HTTP/2 协议
├── codec-http3/             ← HTTP/3 协议
├── handler/                 ← SSL、流量控制等高级 Handler
├── handler-proxy/           ← SOCKS/HTTP 代理支持
├── resolver/                ← DNS 解析器
├── transport-native-epoll/  ← Linux epoll 传输
├── transport-native-kqueue/ ← macOS kqueue 传输
├── transport-native-io_uring/ ← Linux io_uring 传输
└── example/                 ← 示例代码
```
