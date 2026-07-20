# 更新记录

> 本文件由 Claude Code 自动维护，记录每次代码与配置的实质性变更。
> 禁止修改或删除已有记录行。

| 日期 | 改动模块 | 改动摘要 |
| ---- | -------- | -------- |
| 2026-07-17 | docs/netty-analysis | 新增 Future/Promise 体系深度分析文档，涵盖接口体系、DefaultPromise 核心实现、Listener 通知机制、状态机设计等内容 |
| 2026-07-17 | docs/netty-analysis/01-核心抽象 | 新增 ByteBuf 体系深度分析文档，涵盖三指针模型、类继承体系、引用计数机制、堆内/堆外/池化/非池化实现、零拷贝设计、17 个源码文件逐类分析 |
| 2026-07-17 | docs/netty-analysis/附录-全链路追踪 | 新增 TCP 连接全链路追踪文档（ServerBootstrap.bind 到 Channel 激活），覆盖 bootstrap/transport 模块 12 个关键步骤 |
| 2026-07-17 | docs/netty-analysis/附录-全链路追踪 | 新增数据读取全链路追踪文档（网卡到业务 Handler），覆盖 transport/buffer/codec 模块的 ByteBuf 分配、NIO 读取、Pipeline 入站传播、解码器处理 |
| 2026-07-17 | docs/netty-analysis/附录-全链路追踪 | 新增数据写出全链路追踪文档（业务代码到网卡），覆盖 transport/buffer/codec 模块的编码器处理、ChannelOutboundBuffer 缓冲、Gathering Write、背压控制 |
| 2026-07-17 | docs/netty-analysis/03-内存管理 | 新增堆外内存与资源泄漏检测深度分析文档，涵盖 UnsafeByteBufUtil、PlatformDependent/0、ResourceLeakDetector、Cleaner 体系等核心类分析 |
| 2026-07-17 | docs/netty-analysis/03-内存管理 | 新增堆外内存与资源泄漏检测深度分析文档，涵盖 PlatformDependent0/PlatformDependent 平台适配、UnsafeByteBufUtil 堆外读写、ResourceLeakDetector 四级检测、五种 Cleaner 实现演进、12 个源码文件逐类分析 |
| 2026-07-17 | docs/netty-analysis/06-协议实现 | 新增压缩编解码器源码分析文档，涵盖 Zlib/Gzip/Snappy/LZ4/Zstd/Bzip2 等 9 种算法的编解码器实现模式、状态机设计、内存安全防护、零拷贝优化等内容 |
| 2026-07-17 | docs/netty-analysis/06-协议实现 | 重写 DNS 协议实现深度分析文档，新增 DNS Header 位布局图、TCP DNS RFC 7766 长度前缀机制详解、域名压缩指针解码示例、UDP/TCP/服务端 Pipeline 架构图、编码时序图等内容 |
| 2026-07-17 | docs/netty-analysis/08-域名解析 | 新增基础解析器深度分析文档，涵盖 NameResolver/AddressResolver 双层抽象、AddressResolverGroup 生命周期管理、模板方法模式、类型参数匹配机制等 8 个核心类分析 |
| 2026-07-17 | docs/netty-analysis/08-域名解析 | 新增 DNS 解析器深度分析文档，涵盖 DnsNameResolver 异步查询、DnsResolveContext 递归查询状态机、搜索域处理、CNAME 跟踪与环路检测、三层缓存体系、DNS 服务器地址轮询策略等 6 个核心类分析 |
| 2026-07-17 | docs/netty-analysis | 新增 Netty 学习计划文档，覆盖 13 个阶段 60+ 篇文档的系统学习路径 |
| 2026-07-20 | CLAUDE.md | 新增 Claude Code 协作指南（中文），涵盖构建命令、模块结构、核心架构、编码约定 |
| 2026-07-20 | CLAUDE.md | 将 CLAUDE.md 全文改为中文 |
| 2026-07-20 | transport/ChannelFuture | 为 ChannelFuture 接口添加中文 Javadoc 注释，替换原有英文注释，涵盖异步语义、状态机图示、await 与 addListener 对比、死锁警告、超时说明等 |
| 2026-07-20 | .claude/skills/netty-chinese-javadoc | 新增 netty-chinese-javadoc 项目级技能，封装 Netty 源码中文注释添加规范 |
| 2026-07-20 | common/Future | 为 Future 接口添加中文 Javadoc 注释，涵盖异步语义、监听器机制、等待/同步方法、getNow 非阻塞获取等 |
| 2026-07-20 | transport/EventLoopGroup | 为 EventLoopGroup 接口添加中文 Javadoc 注释，涵盖事件循环组概念、Channel 注册机制、典型用法示例 |
| 2026-07-20 | transport/IoHandler | 为 IoHandler 接口添加中文 Javadoc 注释，涵盖 I/O 事件分发、线程约束、IoHandle 注册与 IoOps 提交机制 |
| 2026-07-20 | transport/ChannelFutureListener | 为 ChannelFutureListener 接口添加中文 Javadoc 注释，涵盖三个内置监听器（CLOSE、CLOSE_ON_FAILURE、FIRE_EXCEPTION_ON_FAILURE）的功能说明 |
| 2026-07-20 | .gitignore | 添加 .obsidian/ 目录到 git 忽略规则 |
