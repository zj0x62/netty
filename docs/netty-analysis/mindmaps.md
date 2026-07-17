# Netty 分析思维导图

> 使用缩进列表展示模块间的层级与关联关系，帮助快速回顾知识点。

---

## 阶段一：概述与核心

```
Netty 概述
├── 核心特性
│   ├── 异步非阻塞 I/O
│   ├── 事件驱动模型
│   ├── 高性能零拷贝
│   └── 丰富的协议支持
├── 模块全景（40+ 模块）
│   ├── 核心层：common / buffer / transport
│   ├── 协议层：codec / codec-http / codec-http2 / codec-http3
│   ├── 扩展层：handler / handler-proxy / resolver
│   └── 原生层：transport-native-epoll / kqueue / io_uring
├── 核心架构
│   ├── Reactor 线程模型
│   │   ├── 单 Reactor 单线程
│   │   ├── 单 Reactor 多线程
│   │   └── 主从 Reactor（Netty 默认）
│   ├── 核心接口
│   │   ├── Channel — 网络连接抽象
│   │   ├── ChannelHandler — 事件处理
│   │   ├── ChannelPipeline — Handler 链
│   │   ├── EventLoop — I/O 线程
│   │   └── ByteBuf — 数据缓冲区
│   └── 启动流程
│       ├── ServerBootstrap.bind()
│       ├── Channel 初始化
│       ├── EventLoop 注册
│       └── Pipeline 组装
└── 源码阅读地图
    ├── 服务端启动链路
    ├── 客户端连接链路
    ├── 数据读取链路
    └── 数据写入链路
```

---

## 阶段二：内存管理与传输层

```
内存管理
├── ByteBuf 体系
│   ├── 抽象层次
│   │   ├── ByteBuf（顶层抽象）
│   │   ├── AbstractByteBuf（读写索引管理）
│   │   └── AbstractReferenceCountedByteBuf（引用计数）
│   ├── 实现分类
│   │   ├── 堆内 Buffer：HeapByteBuf
│   │   ├── 堆外 Buffer：DirectByteBuf
│   │   ├── 复合 Buffer：CompositeByteBuf（零拷贝）
│   │   ├── 切片 Buffer：SlicedByteBuf（零拷贝）
│   │   └── 包装 Buffer：WrappedByteBuf
│   └── 分配器
│       ├── PooledByteBufAllocator（池化，高性能）
│       └── UnpooledByteBufAllocator（非池化，简单场景）
├── 池化内存（PoolArena）
│   ├── 四级结构
│   │   ├── Arena — 分配入口，线程绑定
│   │   ├── Chunk — 大块内存（默认 16MB）
│   │   ├── Page — 内存页（默认 8KB）
│   │   └── SubPage — 小对象分配
│   ├── 分配策略
│   │   ├── Tiny（< 512B）→ SubPage 分配
│   │   ├── Small（512B ~ 8KB）→ SubPage 分配
│   │   └── Normal（8KB ~ 16MB）→ Page 分配
│   └── 线程本地缓存
│       └── PoolThreadCache → 减少锁竞争
├── Recycler 对象池
│   ├── ThreadLocal 栈 → 本线程快速分配
│   ├── WeakOrderQueue → 跨线程回收
│   └── Handle 接口 → 对象借还句柄
└── 泄漏检测
    ├── ResourceLeakDetector 四级检测
    │   ├── DISABLED → 关闭
    │   ├── SIMPLE → 采样检测
    │   ├── ADVANCED → 详细追踪
    │   └── PARANOID → 全量检测（开发环境推荐）
    └── 堆外内存释放
        └── Cleaner / Unsafe.freeMemory

传输层
├── NIO 传输（JDK NIO）
│   ├── NioEventLoop
│   │   ├── Selector 轮询
│   │   ├── 任务队列处理
│   │   └── 定时任务调度
│   ├── NioServerSocketChannel → 服务端
│   ├── NioSocketChannel → 客户端
│   └── SelectionKey 事件处理
├── Accept / Connect 流程
│   ├── 服务端 Accept
│   │   ├── OP_ACCEPT 就绪
│   │   ├── JavaChannel.accept()
│   │   ├── 创建 NioSocketChannel
│   │   └── 注册到 Worker EventLoop
│   └── 客户端 Connect
│       ├── OP_CONNECT 就绪
│       ├── finishConnect()
│       └── 触发 channelActive
├── Read / Write 流程
│   ├── 读流程
│   │   ├── OP_READ 就绪
│   │   ├── ByteBuf 分配
│   │   ├── SocketChannel.read()
│   │   └── Pipeline.fireChannelRead()
│   └── 写流程
│       ├── Channel.write() → 待写队列
│       ├── 刷新触发
│       ├── SocketChannel.write()
│       └── WriteBufferWaterMark 背压
├── Epoll 传输（Linux 原生）
│   ├── 边缘触发（ET）→ 更高性能
│   ├── Native Datagram 支持
│   └── SO_REUSEPORT 支持
├── Kqueue 传输（macOS 原生）
│   └── kqueue + kevent 系统调用
├── io_uring 传输（Linux 5.1+）
│   ├── 提交队列（SQ）/ 完成队列（CQ）
│   ├── 零拷贝发送
│   └── 注册缓冲区
└── 其他传输
    ├── EmbeddedChannel → 测试用
    ├── LocalChannel → JVM 内通信
    └── OIO 传输 → 已废弃
```

---

## 阶段三：编解码与协议

```
编解码框架
├── 核心抽象
│   ├── ByteToMessageDecoder → 字节→消息
│   ├── MessageToByteEncoder → 消息→字节
│   ├── ByteToMessageCodec → 双向字节编解码
│   └── MessageToMessageCodec → 消息间转换
├── 帧解码器
│   ├── LengthFieldBasedFrameDecoder → 长度字段
│   ├── DelimiterBasedFrameDecoder → 分隔符
│   ├── FixedLengthFrameDecoder → 固定长度
│   └── LineBasedFrameDecoder → 换行符
├── MessageToMessage 编解码
│   ├── MessageToMessageDecoder → 消息转换
│   └── MessageToMessageEncoder → 消息转换
└── 序列化方案
    ├── Protobuf（ProtobufDecoder/Encoder）
    ├── MessagePack
    ├── JBoss Marshalling
    └── JDK Serializable

协议实现
├── HTTP/1.1
│   ├── HttpRequestDecoder → 请求解码
│   ├── HttpResponseEncoder → 响应编码
│   ├── HttpObjectAggregator → 消息聚合
│   ├── HttpContentCompressor → 内容压缩
│   └── HttpContentDecompressor → 内容解压
├── HTTP/2
│   ├── 帧类型
│   │   ├── DATA / HEADERS / PRIORITY
│   │   ├── RST_STREAM / SETTINGS
│   │   ├── PUSH_PROMISE / GOAWAY
│   │   └── WINDOW_UPDATE / PING
│   ├── 流管理
│   │   ├── Http2Stream 生命周期
│   │   ├── 流量控制（WINDOW_UPDATE）
│   │   └── 并发流限制
│   ├── HPACK 头部压缩
│   │   ├── 静态表
│   │   ├── 动态表
│   │   └── Huffman 编码
│   └── 连接管理
│       ├── Http2ConnectionHandler
│       └── Http2FrameCodec
├── HTTP/3 + QUIC
│   ├── QUIC 传输层
│   │   ├── 连接迁移
│   │   ├── 多路复用
│   │   └── 0-RTT 握手
│   └── HTTP/3 帧映射
├── WebSocket
│   ├── 握手流程
│   ├── 帧类型（Text/Binary/Ping/Pong/Close）
│   └── 压缩扩展（permessage-deflate）
├── MQTT
│   ├── MQTT 3.1.1
│   └── MQTT 5.0
├── Redis（RESP 协议）
├── DNS 协议
├── Memcache
│   ├── 二进制协议
│   └── 文本协议
├── STOMP 消息帧
├── SMTP / SOCKS / HAProxy
└── 压缩编解码
    ├── JdkZlib（Deflate/Gzip/Zlib）
    ├── Brotli
    ├── Lz4
    ├── Zstd
    └── Snappy
```

---

## 阶段四：处理器与工具

```
处理器与扩展
├── SSL/TLS
│   ├── SslContext → SSL 上下文工厂
│   ├── SslHandler → Pipeline 中的 SSL 处理
│   ├── SNI 支持 → 多域名证书
│   ├── 协议版本
│   │   ├── TLS 1.2
│   │   └── TLS 1.3
│   └── 后端实现
│       ├── JDK SSL
│       ├── OpenSSL（原生）
│       └── BoringSSL
├── 空闲检测
│   ├── IdleStateHandler
│   │   ├── readerIdleTime → 读空闲
│   │   ├── writerIdleTime → 写空闲
│   │   └── allIdleTime → 全空闲
│   └── IdleStateEvent → 触发事件
├── 日志处理器
│   └── LoggingHandler → 事件级日志输出
├── 代理处理器
│   ├── HttpProxyHandler → HTTP 代理
│   ├── Socks4ProxyHandler → SOCKS4
│   ├── Socks5ProxyHandler → SOCKS5
│   └── ProxyHandler → 通用代理抽象
├── 流量整形
│   ├── ChannelTrafficShaping → 单通道限速
│   ├── GlobalTrafficShaping → 全局限速
│   └── GlobalChannelTrafficShaping → 全局+通道限速
└── OCSP
    └── 在线证书状态协议

域名解析
├── 基础解析器
│   ├── AddressResolverGroup → 解析器组
│   ├── 轮询策略（RoundRobin / Sequential）
│   └── 缓存机制
└── DNS 解析器
    ├── DnsNameResolver → UDP/TCP DNS 查询
    ├── 递归解析
    ├── 多 DNS 服务器
    └── 搜索域（search domains）

工具与基础设施
├── 通用工具
│   ├── HashedWheelTimer → 时间轮定时器
│   ├── StringUtil → 字符串处理
│   ├── NetUtil → 网络工具
│   └── ResourceLeakDetector → 泄漏检测
├── AttributeKey
│   ├── 类型安全的 Key-Value
│   ├── Channel 级属性绑定
│   └── 线程安全实现
├── 资源泄漏检测
│   ├── 四级检测策略
│   └── 堆转储分析
└── 并发工具
    ├── FastThreadLocal → 优化的 ThreadLocal
    ├── PlatformDependent → 平台相关操作
    ├── RecyclableArrayList → 可回收 List
    └── IntegerHolder / BooleanHolder
```

---

## 阶段五：原生/测试/扩展

```
原生集成
├── JNI 集成
│   ├── NativeLibraryLoader → 库加载
│   ├── JNI 调用约定
│   └── 本地方法注册
├── 原生传输
│   ├── Epoll（Linux）
│   │   ├── epoll_create / epoll_ctl / epoll_wait
│   │   ├── 边缘触发模式
│   │   └── SO_REUSEPORT
│   ├── Kqueue（macOS）
│   │   ├── kqueue / kevent
│   │   └── EVFILT_READ / EVFILT_WRITE
│   └── io_uring（Linux 5.1+）
│       ├── io_uring_setup / io_uring_enter
│       ├── SQE / CQ 队列
│       └── 注册文件/缓冲区
└── 原生 SSL
    ├── OpenSSL 集成
    ├── BoringSSL 集成
    └── ALPN 协议协商

测试体系
├── 测试框架
│   ├── EmbeddedChannel → 内嵌通道测试
│   ├── TestUtils → 测试工具
│   └── 测试基类
├── 测试套件
│   ├── 传输层测试套件
│   │   ├── SocketTestPermutation
│   │   └── 跨传输一致性验证
│   └── 协议测试
│       ├── HTTP/1.1 测试
│       ├── HTTP/2 测试
│       └── WebSocket 测试
└── 基准测试
    ├── JMH 基准测试
    ├── ByteBuf 分配性能
    ├── 编解码性能
    └── 传输层吞吐量

扩展话题
├── 设计模式
│   ├── Reactor 模式 → EventLoop
│   ├── 责任链模式 → Pipeline / Handler
│   ├── 装饰器模式 → WrappedByteBuf
│   ├── 工厂模式 → Channel / Allocator
│   ├── 建造者模式 → Bootstrap
│   ├── 观察者模式 → Future / Listener
│   ├── 享元模式 → AttributeKey
│   └── 对象池模式 → Recycler
├── 性能调优
│   ├── 线程模型配置
│   ├── 内存分配参数
│   ├── TCP 参数调优
│   │   ├── SO_BACKLOG
│   │   ├── TCP_NODELAY
│   │   ├── SO_SNDBUF / SO_RCVBUF
│   │   └── WriteBufferWaterMark
│   └── 监控指标
├── GraalVM Native Image
│   ├── 反射配置
│   ├── JNI 配置
│   └── 资源配置
├── IO 模型对比
│   ├── BIO → 同步阻塞
│   ├── NIO → 同步非阻塞（多路复用）
│   ├── epoll → 事件驱动（Linux）
│   └── io_uring → 异步 I/O（Linux 5.1+）
├── 框架对比
│   ├── Netty vs Mina
│   ├── Netty vs gRPC
│   ├── Netty vs 原生 NIO
│   └── Netty vs Vert.x
└── 版本演进
    ├── Netty 3.x → 4.0
    │   ├── ChannelBuffer → ByteBuf
    │   │   └── 从 Channel 中独立出来
    │   ├── ChannelHandler 重构
    │   │   └── 拆分 Inbound / Outbound
    │   └── 引用计数机制
    ├── Netty 4.0 → 4.1
    │   ├── ResourceLeakDetector
    │   └── Recycler 改进
    └── Netty 4.1 → 4.2
        ├── io_uring 支持
        ├── HTTP/3 + QUIC
        └── API 清理与优化
```
