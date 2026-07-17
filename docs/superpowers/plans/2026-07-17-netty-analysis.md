# Netty 全量深度分析 — 执行计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 对 Netty 4.2 全部 40+ 模块进行深度分析，生成 60 篇中文文档，包含架构图、源码分析、设计思想和全链路追踪。

**Architecture:** 按学习路径分 5 个阶段执行，每阶段内使用 parallel agents 并行分析独立模块。每个 agent 负责 2-4 篇相关文档，通过阅读源码、提取设计模式、绘制架构图来生成结构化的中文分析文档。

**Tech Stack:** Java (Netty 4.2 源码)、Markdown、Mermaid (流程图/架构图)、PlantUML (类图)、ASCII (简单示意图)

---

## 文件结构

所有文档输出到 `docs/netty-analysis/` 目录：

```
docs/netty-analysis/
├── README.md
├── 00-概述与导航/
├── 01-核心抽象/
├── 02-流水线与事件驱动/
├── 03-内存管理/
├── 04-传输层/
├── 05-编解码框架/
├── 06-协议实现/
├── 07-处理器与扩展/
├── 08-域名解析/
├── 09-工具与基础设施/
├── 10-原生集成/
├── 11-测试体系/
├── 12-扩展话题/
└── 附录-全链路追踪/
```

## 每篇文档统一结构

```markdown
# 标题

## 概述
- 这个模块/概念解决什么问题
- 在 Netty 整体架构中的位置

## 架构图
- 本模块的类关系图、流程图（Mermaid/PlantUML/ASCII 混合）

## 核心类分析
- 每个关键类的职责、字段、方法逐一讲解
- 关键代码片段（带中文注释）
- 核心链路附带源码行号引用（如 ByteBuf.java:123）

## 设计思想
- 为什么这样设计
- 体现了哪些设计模式或原则

## 与其他模块的交互
- 本模块如何与其他模块协作

## 关键流程
- 核心流程的时序图/流程图

## 学习要点
- 需要重点理解的关键点
- 常见面试问题/思考题
```

## 深度策略

| 层次 | 深度 | 说明 |
|------|------|------|
| 核心链路 | 逐类逐方法 | 每个字段、每个方法都分析 |
| 传输层 | 逐类分析 + 实现对比 | 重点讲清差异和选型依据 |
| 编解码框架 | 框架逐类 + 协议聚焦状态机 | 协议实现聚焦编解码逻辑和状态机 |
| 处理器/解析器/工具 | 讲清用途和设计 | 重点类分析，辅助类简述 |
| 原生集成 | 机制分析 | JNI 调用链、C 代码关键结构 |
| 测试/扩展 | 概述 + 关键实现 | 重点讲框架和模式 |

---

## 阶段一：概述与核心（13 篇）

> 建立全局视野，理解 Netty 最核心的抽象和事件驱动模型

### Task 1: 00-概述与导航（4 篇）

**并行执行：** 单 agent 处理全部 4 篇

**Files:**
- Create: `docs/netty-analysis/README.md`
- Create: `docs/netty-analysis/00-概述与导航/01-netty-overview.md`
- Create: `docs/netty-analysis/00-概述与导航/02-module-dependency-map.md`
- Create: `docs/netty-analysis/00-概述与导航/03-architecture-overview.md`
- Create: `docs/netty-analysis/00-概述与导航/04-source-reading-map.md`

- [ ] **Step 1: 分析 Netty 概述信息**

阅读以下文件获取项目概况：
- `README.md`
- `pom.xml`（头部 100 行）
- `common/src/main/java/io/netty/util/Version.java`（版本信息）
- `common/src/main/java/io/netty/util/NettyRuntime.java`

提取：Netty 定义、核心特性、版本演进历史（3.x → 4.x → 4.2）

- [ ] **Step 2: 分析模块依赖关系**

阅读根 `pom.xml` 中的 `<modules>` 部分，提取所有模块及其依赖关系。重点关注：
- 核心模块（transport、buffer、common、codec、handler）的依赖方向
- 协议模块对核心模块的依赖
- 原生模块（native-epoll、native-kqueue、native-io_uring）的分层关系

绘制 Mermaid 依赖关系图。

- [ ] **Step 3: 分析核心架构分层**

阅读以下核心类理解架构分层：
- `transport/src/main/java/io/netty/channel/Channel.java`
- `transport/src/main/java/io/netty/channel/EventLoopGroup.java`
- `transport/src/main/java/io/netty/channel/ChannelPipeline.java`
- `buffer/src/main/java/io/netty/buffer/ByteBuf.java`
- `common/src/main/java/io/netty/util/concurrent/Future.java`

绘制架构分层图（从上到下：用户代码 → Handler → Pipeline → Channel → EventLoop → Selector/IO）

- [ ] **Step 4: 构建源码阅读地图**

基于前面的分析，梳理核心流程的精确类和方法路径：
- 启动流程：ServerBootstrap.bind() → ... → Selector.register()
- 事件循环：SingleThreadEventExecutor.run() → NioEventLoop.run()
- 数据读写：Pipeline.fireChannelRead() → ... → HeadContext
- Pipeline 传播：DefaultChannelPipeline 的完整调用链

- [ ] **Step 5: 生成 4 篇文档**

按照统一结构生成 4 篇文档，包含 Mermaid 架构图和源码行号引用。

- [ ] **Step 6: 验证并提交**

验证每篇文档完整性（所有章节都有内容、架构图可渲染、源码引用准确），提交到 git。

---

### Task 2: 核心抽象 — ByteBuf（1 篇）

**并行执行：** 与其他核心抽象文档并行

**Files:**
- Create: `docs/netty-analysis/01-核心抽象/01-bytebuf.md`

- [ ] **Step 1: 分析 ByteBuf 接口体系**

阅读以下文件（逐类逐方法分析）：
- `buffer/src/main/java/io/netty/buffer/ByteBuf.java` — 抽象基类，所有字段和方法
- `buffer/src/main/java/io/netty/buffer/ByteBufHolder.java`
- `buffer/src/main/java/io/netty/buffer/ByteBufAllocator.java`
- `buffer/src/main/java/io/netty/buffer/CompositeByteBuf.java`
- `buffer/src/main/java/io/netty/buffer/ReadOnlyByteBuf.java`
- `buffer/src/main/java/io/netty/buffer/SlicedByteBuf.java`
- `buffer/src/main/java/io/netty/buffer/DuplicatedByteBuf.java`

重点关注：readerIndex/writerIndex 机制、引用计数、读写方法分类。

- [ ] **Step 2: 分析 ByteBuf 实现类**

阅读具体实现：
- `buffer/src/main/java/io/netty/buffer/AbstractByteBuf.java`
- `buffer/src/main/java/io/netty/buffer/UnpooledDirectByteBuf.java`
- `buffer/src/main/java/io/netty/buffer/UnpooledHeapByteBuf.java`
- `buffer/src/main/java/io/netty/buffer/PooledByteBuf.java`
- `buffer/src/main/java/io/netty/buffer/PooledDirectByteBuf.java`
- `buffer/src/main/java/io/netty/buffer/PooledHeapByteBuf.java`

- [ ] **Step 3: 分析 Unpooled 工厂类**

阅读 `buffer/src/main/java/io/netty/buffer/Unpooled.java`，分析静态工厂方法。

- [ ] **Step 4: 绘制类图**

使用 PlantUML 绘制 ByteBuf 完整类继承图（接口 → 抽象类 → 具体实现）。

- [ ] **Step 5: 生成文档**

按照统一结构生成文档，重点覆盖：readerIndex/writerIndex/capacity 三指针模型、引用计数机制、堆内存 vs 堆外内存、池化 vs 非池化。

- [ ] **Step 6: 验证并提交**

---

### Task 3: 核心抽象 — Channel（1 篇）

**并行执行：** 与 ByteBuf、ChannelHandler、EventLoop 文档并行

**Files:**
- Create: `docs/netty-analysis/01-核心抽象/02-channel.md`

- [ ] **Step 1: 分析 Channel 接口**

阅读以下文件（逐类逐方法）：
- `transport/src/main/java/io/netty/channel/Channel.java` — 核心接口
- `transport/src/main/java/io/netty/channel/ServerChannel.java`
- `transport/src/main/java/io/netty/channel/AbstractChannel.java` — 抽象实现
- `transport/src/main/java/io/netty/channel/AbstractServerChannel.java`

重点关注：Channel 生命周期（isActive/isOpen/isRegistered/isWritable）、Unsafe 内部接口。

- [ ] **Step 2: 分析 NIO 实现**

阅读：
- `transport/src/main/java/io/netty/channel/nio/AbstractNioChannel.java`
- `transport/src/main/java/io/netty/channel/nio/NioSocketChannel.java`
- `transport/src/main/java/io/netty/channel/nio/NioServerSocketChannel.java`
- `transport/src/main/java/io/netty/channel/nio/AbstractNioByteChannel.java`
- `transport/src/main/java/io/netty/channel/nio/AbstractNioMessageChannel.java`

- [ ] **Step 3: 分析 Unsafe 实现**

阅读 `AbstractChannel.AbstractUnsafe` 内部类，理解 register/connect/disconnect/close/unsafe 的实现。

- [ ] **Step 4: 绘制 Channel 类图和生命周期图**

PlantUML 类图 + Mermaid 状态机图（Channel 生命周期状态转换）。

- [ ] **Step 5: 生成文档**

- [ ] **Step 6: 验证并提交**

---

### Task 4: 核心抽象 — ChannelHandler（1 篇）

**并行执行：** 与 ByteBuf、Channel、EventLoop 文档并行

**Files:**
- Create: `docs/netty-analysis/01-核心抽象/03-channel-handler.md`

- [ ] **Step 1: 分析 Handler 接口体系**

阅读：
- `transport/src/main/java/io/netty/channel/ChannelHandler.java`
- `transport/src/main/java/io/netty/channel/ChannelInboundHandler.java`
- `transport/src/main/java/io/netty/channel/ChannelOutboundHandler.java`
- `transport/src/main/java/io/netty/channel/ChannelInboundHandlerAdapter.java`
- `transport/src/main/java/io/netty/channel/ChannelOutboundHandlerAdapter.java`
- `transport/src/main/java/io/netty/channel/ChannelDuplexHandler.java`
- `transport/src/main/java/io/netty/channel/SimpleChannelInboundHandler.java`

重点关注：入站 vs 出站的区分、@Sharable 注解、适配器模式。

- [ ] **Step 2: 分析 Handler 生命周期回调**

梳理 ChannelHandler 的回调方法顺序：handlerAdded → channelRegistered → channelActive → channelRead → channelInactive → channelUnregistered → handlerRemoved

- [ ] **Step 3: 绘制 Handler 类图和入站/出站流程图**

PlantUML 类图 + Mermaid 流程图（入站事件和出站事件的处理方向）。

- [ ] **Step 4: 生成文档**

- [ ] **Step 5: 验证并提交**

---

### Task 5: 核心抽象 — EventLoop（1 篇）

**并行执行：** 与 ByteBuf、Channel、ChannelHandler 文档并行

**Files:**
- Create: `docs/netty-analysis/01-核心抽象/04-eventloop.md`

- [ ] **Step 1: 分析 EventLoop 接口体系**

阅读：
- `common/src/main/java/io/netty/util/concurrent/EventExecutor.java`
- `common/src/main/java/io/netty/util/concurrent/EventExecutorGroup.java`
- `transport/src/main/java/io/netty/channel/EventLoop.java`
- `transport/src/main/java/io/netty/channel/EventLoopGroup.java`
- `common/src/main/java/io/netty/util/concurrent/SingleThreadEventExecutor.java`
- `transport/src/main/java/io/netty/channel/SingleThreadEventLoop.java`
- `transport/src/main/java/io/netty/channel/nio/NioEventLoop.java`
- `transport/src/main/java/io/netty/channel/nio/NioEventLoopGroup.java`

重点关注：线程模型（一个 EventLoop 绑定一个线程）、任务队列、Selector 轮询机制。

- [ ] **Step 2: 分析 NioEventLoop.run() 事件循环**

深入分析 `NioEventLoop.run()` 的三阶段：select → processSelectedKeys → runAllTasks。理解 ioRatio 的作用。

- [ ] **Step 3: 分析任务调度**

阅读 `SingleThreadEventExecutor` 的任务队列实现（LinkedBlockingQueue、MpscQueue）、schedule 机制。

- [ ] **Step 4: 绘制 EventLoop 架构图和事件循环流程图**

Mermaid 流程图（事件循环三阶段）+ 组件关系图（EventLoopGroup → EventLoop → Thread → Selector）。

- [ ] **Step 5: 生成文档**

- [ ] **Step 6: 验证并提交**

---

### Task 6: 流水线 — Pipeline 与事件传播（2 篇）

**并行执行：** 与 Future/Promise 文档并行

**Files:**
- Create: `docs/netty-analysis/02-流水线与事件驱动/01-pipeline.md`
- Create: `docs/netty-analysis/02-流水线与事件驱动/02-event-propagation.md`

- [ ] **Step 1: 分析 Pipeline 核心类**

阅读：
- `transport/src/main/java/io/netty/channel/ChannelPipeline.java` — 接口
- `transport/src/main/java/io/netty/channel/DefaultChannelPipeline.java` — 实现
- `transport/src/main/java/io/netty/channel/ChannelHandlerContext.java`
- `transport/src/main/java/io/netty/channel/AbstractChannelHandlerContext.java`
- `transport/src/main/java/io/netty/channel/DefaultChannelHandlerContext.java`

重点关注：HeadContext 和 TailContext 的角色、Handler 链的双向链表结构。

- [ ] **Step 2: 分析事件传播机制**

深入分析：
- 入站事件传播方向（Head → Tail）：fireChannelRead、fireChannelActive、fireExceptionCaught
- 出站事件传播方向（Tail → Head）：write、flush、bind、connect
- 异常传播机制：pipeline.fireExceptionCaught() 和 ctx.fireExceptionCaught() 的区别

- [ ] **Step 3: 分析 add/remove/replace 操作**

分析 Pipeline 动态修改 Handler 链的操作及其线程安全性。

- [ ] **Step 4: 绘制 Pipeline 结构图和事件传播图**

ASCII 示意图（Head → H1 → H2 → Tail 双向链表）+ Mermaid 时序图（入站/出站事件传播流程）。

- [ ] **Step 5: 生成 2 篇文档**

- [ ] **Step 6: 验证并提交**

---

### Task 7: 流水线 — Future/Promise（1 篇）

**并行执行：** 与 Pipeline 文档并行

**Files:**
- Create: `docs/netty-analysis/02-流水线与事件驱动/03-future-promise.md`

- [ ] **Step 1: 分析 Future/Promise 体系**

阅读：
- `common/src/main/java/io/netty/util/concurrent/Future.java`
- `common/src/main/java/io/netty/util/concurrent/GenericFutureListener.java`
- `transport/src/main/java/io/netty/channel/ChannelFuture.java`
- `transport/src/main/java/io/netty/channel/ChannelPromise.java`
- `common/src/main/java/io/netty/util/concurrent/DefaultPromise.java`
- `transport/src/main/java/io/netty/channel/DefaultChannelPromise.java`

重点关注：Future vs Promise 的区别、listener 通知机制、sync() vs await()。

- [ ] **Step 2: 分析 GlobalEventExecutor 和通知机制**

阅读 `common/src/main/java/io/netty/util/concurrent/GlobalEventExecutor.java`，理解 listener 回调的线程选择。

- [ ] **Step 3: 绘制类图和状态转换图**

PlantUML 类图 + Mermaid 状态机（Future 的状态：未完成 → 成功/失败/取消）。

- [ ] **Step 4: 生成文档**

- [ ] **Step 5: 验证并提交**

---

### Task 8: 附录 — 全链路追踪（3 篇）

**并行执行：** 单 agent 处理全部 3 篇（需要跨模块串联，保持上下文连贯）

**Files:**
- Create: `docs/netty-analysis/附录-全链路追踪/01-server-accept-flow.md`
- Create: `docs/netty-analysis/附录-全链路追踪/02-request-read-flow.md`
- Create: `docs/netty-analysis/附录-全链路追踪/03-response-write-flow.md`

- [ ] **Step 1: 追踪 Server Accept 全链路**

精确阅读以下类的方法调用链：
- `AbstractBootstrap.bind()` → `doBind()` → `initAndRegister()`
- `NioServerSocketChannel` 构造和 `doBind()`
- `NioEventLoop.register()` → `processSelectedKeys()`
- `NioServerSocketChannel.doReadMessages()` → 创建 `NioSocketChannel`
- `ServerBootstrap.ServerBootstrapAcceptor.channelRead()`
- `childGroup.register()` → `pipeline.fireChannelActive()`

每个步骤记录：类名、方法名、所在模块、关键代码行号。

- [ ] **Step 2: 追踪 Request Read 全链路**

精确阅读：
- `NioEventLoop.run()` → `processSelectedKeys()`
- `NioSocketChannel.NioSocketChannelUnsafe.read()`
- `ByteBufAllocator.ioBuffer()` → ByteBuf 分配
- `SocketChannel.read(ByteBuf)` → NIO 读取
- `Pipeline.fireChannelRead(byteBuf)` → Handler 链传播
- `SimpleChannelInboundHandler.channelRead0()` → 业务处理

- [ ] **Step 3: 追踪 Response Write 全链路**

精确阅读：
- `ctx.writeAndFlush(msg)` → `TailContext.write()`
- 出站 Handler 链反向传播
- `MessageToByteEncoder.write()` → 编码
- `HeadContext.write(byteBuf)` → `NioSocketChannel.unsafe.write()`
- `ChannelOutboundBuffer.addMessage()` → 缓冲
- `NioEventLoop.run()` flush 阶段 → `SocketChannel.write(ByteBuf)`

- [ ] **Step 4: 绘制全链路时序图**

每篇文档绘制一个完整的 Mermaid 时序图，标注每个步骤涉及的模块。

- [ ] **Step 5: 生成 3 篇文档**

- [ ] **Step 6: 验证并提交**

---

## 阶段二：内存管理与传输层（11 篇）

> 深入理解 Netty 的内存管理和 IO 传输机制

### Task 9: 内存管理 — Buffer 分配器（1 篇）

**并行执行：** 与 PoolArena、Recycler、NativeMemory 文档并行

**Files:**
- Create: `docs/netty-analysis/03-内存管理/01-buffer-allocator.md`

- [ ] **Step 1: 分析分配器接口和实现**

阅读：
- `buffer/src/main/java/io/netty/buffer/ByteBufAllocator.java`
- `buffer/src/main/java/io/netty/buffer/AbstractByteBufAllocator.java`
- `buffer/src/main/java/io/netty/buffer/UnpooledByteBufAllocator.java`
- `buffer/src/main/java/io/netty/buffer/PooledByteBufAllocator.java`

重点：directDefault（默认使用堆外内存）、Pooled vs Unpooled 的选择策略。

- [ ] **Step 2: 分析 Unpooled 分配器**

阅读 `UnpooledByteBufAllocator` 和 `Unpooled.directBuffer()` / `heapBuffer()` 实现。

- [ ] **Step 3: 分析 Pooled 分配器入口**

阅读 `PooledByteBufAllocator` 的构造方法、`newDirectBuffer()` / `newHeapBuffer()` 入口。

- [ ] **Step 4: 绘制分配器类图和选择策略图**

- [ ] **Step 5: 生成文档**

- [ ] **Step 6: 验证并提交**

---

### Task 10: 内存管理 — PoolArena（1 篇）

**并行执行：** 与 BufferAllocator、Recycler、NativeMemory 文档并行

**Files:**
- Create: `docs/netty-analysis/03-内存管理/02-pool-arena.md`

- [ ] **Step 1: 分析 Arena 体系**

阅读：
- `buffer/src/main/java/io/netty/buffer/PoolArena.java`（抽象类）
- `buffer/src/main/java/io/netty/buffer/PoolArena.HeapArena.java`
- `buffer/src/main/java/io/netty/buffer/PoolArena.DirectArena.java`
- `buffer/src/main/java/io/netty/buffer/PoolChunk.java`
- `buffer/src/main/java/io/netty/buffer/PoolChunkList.java`
- `buffer/src/main/java/io/netty/buffer/PoolSubpage.java`
- `buffer/src/main/java/io/netty/buffer/PoolThreadCache.java`

重点：Chunk 的二叉树内存管理、SubPage 的小内存分配、ThreadCache 的线程本地缓存。

- [ ] **Step 2: 分析内存分配算法**

深入 `PoolArena.allocate()` → `PoolChunk.allocate()` 的二叉树分配算法，理解 order 和 depthLevel 的含义。

- [ ] **Step 3: 分析内存释放和复用**

`PoolChunk.free()` 和 `PoolArena.free()` 的实现，chunkList 的动态迁移机制。

- [ ] **Step 4: 绘制 Arena 架构图和内存布局图**

ASCII 内存布局图（Chunk → Page → SubPage）+ Mermaid 分配流程图。

- [ ] **Step 5: 生成文档**

- [ ] **Step 6: 验证并提交**

---

### Task 11: 内存管理 — Recycler（1 篇）

**并行执行：** 与 BufferAllocator、PoolArena、NativeMemory 文档并行

**Files:**
- Create: `docs/netty-analysis/03-内存管理/03-recycler.md`

- [ ] **Step 1: 分析 Recycler 实现**

阅读：
- `common/src/main/java/io/netty/util/Recycler.java`
- `common/src/main/java/io/netty/util/Recycler.WeakOrderQueue.java`
- `common/src/main/java/io/netty/util/Recycler.LocalPool.java`

重点：ThreadLocal 缓存、WeakOrderQueue 跨线程回收、Handle 接口。

- [ ] **Step 2: 分析对象获取和归还流程**

`get()` → `LocalPool.claim()` → 新对象创建 / 缓存命中
`handle.recycle()` → `LocalPool.release()` / `WeakOrderQueue` 跨线程

- [ ] **Step 3: 绘制 Recycler 架构图**

- [ ] **Step 4: 生成文档**

- [ ] **Step 5: 验证并提交**

---

### Task 12: 内存管理 — 堆外内存与泄漏检测（1 篇）

**并行执行：** 与 BufferAllocator、PoolArena、Recycler 文档并行

**Files:**
- Create: `docs/netty-analysis/03-内存管理/04-native-memory.md`

- [ ] **Step 1: 分析堆外内存操作**

阅读：
- `buffer/src/main/java/io/netty/buffer/UnsafeByteBufUtil.java`
- `common/src/main/java/io/netty/util/internal/PlatformDependent.java`
- `common/src/main/java/io/netty/util/internal/PlatformDependent0.java`

重点：Unsafe 直接内存操作、DirectByteBuffer 的分配和释放。

- [ ] **Step 2: 分析资源泄漏检测**

阅读：
- `common/src/main/java/io/netty/util/ResourceLeakDetector.java`
- `common/src/main/java/io/netty/util/ResourceLeakTracker.java`
- `common/src/main/java/io/netty/util/ResourceLeakHint.java`

重点：采样率控制、泄漏报告的调用栈捕获、四种检测级别（DISABLED/SIMPLE/ADVANCED/PARANOID）。

- [ ] **Step 3: 分析 Cleaner 机制**

阅读 `buffer/src/main/java/io/netty/buffer/CleanerJava9.java` 和 `CleanerJava6.java`，理解 JDK 9+ 和 JDK 8 的 Cleaner 差异。

- [ ] **Step 4: 绘制内存管理全景图**

- [ ] **Step 5: 生成文档**

- [ ] **Step 6: 验证并提交**

---

### Task 13: 传输层 — NIO 传输（1 篇）

**并行执行：** 与其他传输层文档并行

**Files:**
- Create: `docs/netty-analysis/04-传输层/01-nio-transport.md`

- [ ] **Step 1: 分析 Bootstrap 体系**

阅读：
- `transport/src/main/java/io/netty/bootstrap/Bootstrap.java`
- `transport/src/main/java/io/netty/bootstrap/ServerBootstrap.java`
- `transport/src/main/java/io/netty/bootstrap/AbstractBootstrap.java`

重点：ServerBootstrap 的 group(parentGroup, childGroup) 模型、init() 方法、option/childOption 的区别。

- [ ] **Step 2: 分析 Channel 注册流程**

`AbstractBootstrap.initAndRegister()` → `EventLoop.register()` → `AbstractNioChannel.doRegister()` → `javaChannel().register(eventLoop().unwrappedSelector(), 0, this)`

- [ ] **Step 3: 分析 ChannelFactory 和反射创建**

`ReflectiveChannelFactory` 如何通过反射创建 Channel 实例。

- [ ] **Step 4: 绘制 Bootstrap 启动流程图**

- [ ] **Step 5: 生成文档**

- [ ] **Step 6: 验证并提交**

---

### Task 14: 传输层 — Accept/Connect 与 Read/Write（2 篇）

**并行执行：** 与其他传输层文档并行

**Files:**
- Create: `docs/netty-analysis/04-传输层/02-accept-connect.md`
- Create: `docs/netty-analysis/04-传输层/03-read-write.md`

- [ ] **Step 1: 分析 Accept 流程**

阅读 `NioServerSocketChannel.doReadMessages()` 和 `ServerBootstrap.ServerBootstrapAcceptor`，理解新连接的接受和 child channel 的注册。

- [ ] **Step 2: 分析 Connect 流程**

阅读 `NioSocketChannel.doConnect()` 和 `AbstractNioChannel.finishConnect()`。

- [ ] **Step 3: 分析 Read 流程**

阅读 `NioSocketChannel.NioSocketChannelUnsafe.read()`，理解：
- `allocHandle` 的自适应缓冲区大小调整
- Gathering IO（循环读取直到无数据）
- `autoRead` 机制

- [ ] **Step 4: 分析 Write 流程**

阅读 `NioSocketChannel.doWrite()` 和 `ChannelOutboundBuffer`，理解：
- `ChannelOutboundBuffer` 的缓冲链表
- Scattering IO（批量写出）
- `isWritable` 的高低水位线

- [ ] **Step 5: 绘制 Accept/Read/Write 时序图**

- [ ] **Step 6: 生成 2 篇文档并提交**

---

### Task 15: 传输层 — 原生传输（4 篇）

**并行执行：** 单 agent 处理 4 篇（对比分析需要统一上下文）

**Files:**
- Create: `docs/netty-analysis/04-传输层/04-epoll-transport.md`
- Create: `docs/netty-analysis/04-传输层/05-kqueue-transport.md`
- Create: `docs/netty-analysis/04-传输层/06-io_uring-transport.md`
- Create: `docs/netty-analysis/04-传输层/07-other-transport.md`

- [ ] **Step 1: 分析 Epoll 传输**

阅读：
- `transport-classes-epoll/src/main/java/io/netty/channel/epoll/EpollEventLoop.java`
- `transport-classes-epoll/src/main/java/io/netty/channel/epoll/EpollSocketChannel.java`
- `transport-classes-epoll/src/main/java/io/netty/channel/epoll/EpollServerSocketChannel.java`
- `transport-classes-epoll/src/main/java/io/netty/channel/epoll/LinuxSocket.java`

重点：JNI 调用 native epoll、ET vs LT 模式、SO_REUSEPORT。

- [ ] **Step 2: 分析 Kqueue 传输**

阅读：
- `transport-classes-kqueue/src/main/java/io/netty/channel/kqueue/KQueueEventLoop.java`
- `transport-classes-kqueue/src/main/java/io/netty/channel/kqueue/KQueueSocketChannel.java`

重点：macOS kqueue 的 EVFILT_READ/EVFILT_WRITE。

- [ ] **Step 3: 分析 io_uring 传输**

阅读：
- `transport-classes-io_uring/src/main/java/io/netty/channel/uring/IOUringEventLoop.java`
- `transport-classes-io_uring/src/main/java/io/netty/channel/uring/IOUringSocketChannel.java`

重点：io_uring 的 SQ/CQ 机制、零拷贝支持。

- [ ] **Step 4: 分析其他传输**

简述 SCTP、UDT、RXTX 传输的用途和实现。

- [ ] **Step 5: 绘制三种 IO 模型对比图**

用表格或对比图展示 epoll/kqueue/io_uring 的差异。

- [ ] **Step 6: 生成 4 篇文档并提交**

---

## 阶段三：编解码框架与协议实现（15 篇）

> 理解 Netty 的编解码框架和各协议的具体实现

### Task 16: 编解码框架（4 篇）

**并行执行：** 单 agent 处理全部 4 篇（框架逻辑紧密关联）

**Files:**
- Create: `docs/netty-analysis/05-编解码框架/01-codec-framework.md`
- Create: `docs/netty-analysis/05-编解码框架/02-message-codec.md`
- Create: `docs/netty-analysis/05-编解码框架/03-codec-base.md`
- Create: `docs/netty-analysis/05-编解码框架/04-serialization.md`

- [ ] **Step 1: 分析 ByteToMessage / MessageToByteEncoder**

阅读：
- `codec-base/src/main/java/io/netty/handler/codec/ByteToMessageDecoder.java`
- `codec-base/src/main/java/io/netty/handler/codec/MessageToByteEncoder.java`
- `codec-base/src/main/java/io/netty/handler/codec/CodecOutputList.java`

重点：cumulation 缓冲区管理、isSingleDecode、release 语义。

- [ ] **Step 2: 分析 MessageToMessage 系列**

阅读：
- `codec-base/src/main/java/io/netty/handler/codec/MessageToMessageDecoder.java`
- `codec-base/src/main/java/io/netty/handler/codec/MessageToMessageEncoder.java`
- `codec-base/src/main/java/io/netty/handler/codec/MessageToMessageCodec.java`
- `codec-base/src/main/java/io/netty/handler/codec/ByteToMessageCodec.java`

- [ ] **Step 3: 分析帧解码器**

阅读：
- `codec-base/src/main/java/io/netty/handler/codec/DelimiterBasedFrameDecoder.java`
- `codec-base/src/main/java/io/netty/handler/codec/FixedLengthFrameDecoder.java`
- `codec-base/src/main/java/io/netty/handler/codec/LengthFieldBasedFrameDecoder.java`
- `codec-base/src/main/java/io/netty/handler/codec/LineBasedFrameDecoder.java`

重点：LengthFieldBasedFrameDecoder 的 lengthFieldOffset/lengthFieldLength/lengthAdjustment 参数。

- [ ] **Step 4: 分析序列化方案**

简述 Protobuf 和 Marshalling 编解码器的使用方式和实现要点。

- [ ] **Step 5: 绘制编解码器类图和帧结构图**

- [ ] **Step 6: 生成 4 篇文档并提交**

---

### Task 17: 协议实现 — HTTP/WebSocket（2 篇）

**并行执行：** 与其他协议实现并行

**Files:**
- Create: `docs/netty-analysis/06-协议实现/01-http-codec.md`
- Create: `docs/netty-analysis/06-协议实现/04-websocket-codec.md`

- [ ] **Step 1: 分析 HTTP/1.1 编解码**

阅读：
- `codec-http/src/main/java/io/netty/handler/codec/http/HttpRequestDecoder.java`
- `codec-http/src/main/java/io/netty/handler/codec/http/HttpResponseEncoder.java`
- `codec-http/src/main/java/io/netty/handler/codec/http/HttpObjectDecoder.java`
- `codec-http/src/main/java/io/netty/handler/codec/http/HttpObjectAggregator.java`

重点：HttpObjectDecoder 的状态机（SKIP_CONTROL_CHARS → READ_INITIAL → READ_HEADER → READ_CONTENT）。

- [ ] **Step 2: 分析 WebSocket 编解码**

阅读：
- `codec-http/src/main/java/io/netty/handler/codec/http/websocketx/WebSocketServerProtocolHandler.java`
- `codec-http/src/main/java/io/netty/handler/codec/http/websocketx/WebSocketFrameDecoder.java`
- `codec-http/src/main/java/io/netty/handler/codec/http/websocketx/WebSocketFrameEncoder.java`

重点：WebSocket 握手（HTTP Upgrade）、帧格式、分片。

- [ ] **Step 3: 生成 2 篇文档并提交**

---

### Task 18: 协议实现 — HTTP/2 和 HTTP/3（2 篇）

**并行执行：** 与其他协议实现并行

**Files:**
- Create: `docs/netty-analysis/06-协议实现/02-http2-codec.md`
- Create: `docs/netty-analysis/06-协议实现/03-http3-quic.md`

- [ ] **Step 1: 分析 HTTP/2 帧和流**

阅读：
- `codec-http2/src/main/java/io/netty/handler/codec/http2/Http2FrameCodec.java`
- `codec-http2/src/main/java/io/netty/handler/codec/http2/Http2ConnectionHandler.java`
- `codec-http2/src/main/java/io/netty/handler/codec/http2/Http2Connection.java`
- `codec-http2/src/main/java/io/netty/handler/codec/http2/Http2Stream.java`
- `codec-http2/src/main/java/io/netty/handler/codec/http2/DefaultHttp2FrameWriter.java`

重点：帧类型（DATA/HEADERS/RST_STREAM/SETTINGS/PING）、流状态机、HPACK 头部压缩。

- [ ] **Step 2: 分析 HTTP/2 连接管理**

阅读 `Http2Connection` 和 `Http2Connection.Endpoint`，理解单连接多流复用。

- [ ] **Step 3: 分析 HTTP/3 和 QUIC**

阅读：
- `codec-http3/src/main/java/io/netty/handler/codec/http3/Http3Codec.java`
- `codec-classes-quic/src/main/java/io/netty/handler/codec/quic/QuicChannel.java`

重点：QUIC 的多路复用、0-RTT 连接建立。

- [ ] **Step 4: 生成 2 篇文档并提交**

---

### Task 19: 协议实现 — MQTT/Redis/DNS（3 篇）

**并行执行：** 与其他协议实现并行

**Files:**
- Create: `docs/netty-analysis/06-协议实现/05-mqtt-codec.md`
- Create: `docs/netty-analysis/06-协议实现/06-redis-codec.md`
- Create: `docs/netty-analysis/06-协议实现/07-dns-codec.md`

- [ ] **Step 1: 分析 MQTT 协议实现**

阅读 `codec-mqtt/src/main/java/io/netty/handler/codec/mqtt/` 目录下的关键类：
- `MqttDecoder.java` — 解码器状态机
- `MqttEncoder.java` — 编码器
- `MqttMessage.java` — 消息类型体系

重点：MQTT 固定头/可变头/负载的结构、QoS 级别、CONNECT/SUBSCRIBE/PUBLISH 流程。

- [ ] **Step 2: 分析 Redis 协议实现**

阅读 `codec-redis/src/main/java/io/netty/handler/codec/redis/`：
- `RedisDecoder.java` — RESP 协议解码
- `RedisEncoder.java`
- `RedisMessage.java` — 消息类型（SimpleString/Error/Integer/BulkString/Array）

重点：RESP 协议的行分隔符、批量字符串的长度前缀、Pipeline 批量命令。

- [ ] **Step 3: 分析 DNS 协议实现**

阅读 `codec-dns/src/main/java/io/netty/handler/codec/dns/`：
- `DnsQueryDecoder.java`
- `DnsResponseDecoder.java`
- `DnsRecord.java` — DNS 记录类型

重点：DNS 报文结构（Header/Question/Answer/Authority/Additional）、TCP DNS（长度前缀）。

- [ ] **Step 4: 生成 3 篇文档并提交**

---

### Task 20: 协议实现 — Memcache/STOMP/SMTP/SOCKS/HAProxy（3 篇）

**并行执行：** 与其他协议实现并行

**Files:**
- Create: `docs/netty-analysis/06-协议实现/08-memcache-codec.md`
- Create: `docs/netty-analysis/06-协议实现/09-stomp-codec.md`
- Create: `docs/netty-analysis/06-协议实现/10-smtp-socks-haproxy.md`

- [ ] **Step 1: 分析 Memcache 协议**

阅读 `codec-memcache/src/main/java/io/netty/handler/codec/memcache/`，重点：二进制协议和文本协议的帧结构、CAS 操作。

- [ ] **Step 2: 分析 STOMP 协议**

阅读 `codec-stomp/src/main/java/io/netty/handler/codec/stomp/`，重点：STOMP 帧结构（COMMAND/Headers/Body）、SUBSCRIBE/SEND/MESSAGE。

- [ ] **Step 3: 分析 SMTP/SOCKS/HAProxy**

简述三种协议的编解码实现要点：
- SMTP：命令/响应模式
- SOCKS：SOCKS4/5 握手流程
- HAProxy：PROXY 协议 v1/v2

- [ ] **Step 4: 生成 3 篇文档并提交**

---

### Task 21: 协议实现 — 压缩编解码（1 篇）

**并行执行：** 与其他协议实现并行

**Files:**
- Create: `docs/netty-analysis/06-协议实现/11-compression.md`

- [ ] **Step 1: 分析压缩编解码器**

阅读 `codec-compression/src/main/java/io/netty/handler/codec/compression/`：
- `JdkZlibDecoder/Encoder.java` — JDK Zlib
- `JZlibDecoder/Encoder.java` — JZlib
- `SnappyFrameDecoder/Encoder.java` — Snappy
- `Lz4FrameDecoder/Encoder.java` — LZ4
- `ZstdOptions.java` — Zstd

- [ ] **Step 2: 生成文档并提交**

---

## 阶段四：处理器、解析与工具（12 篇）

> 理解 Netty 的各种 Handler 扩展和基础设施

### Task 22: 处理器 — SSL/TLS（1 篇）

**并行执行：** 与其他处理器文档并行

**Files:**
- Create: `docs/netty-analysis/07-处理器与扩展/01-ssl-handler.md`

- [ ] **Step 1: 分析 SSL 体系**

阅读：
- `handler/src/main/java/io/netty/handler/ssl/SslHandler.java`
- `handler/src/main/java/io/netty/handler/ssl/SslContext.java`
- `handler/src/main/java/io/netty/handler/ssl/SslContextBuilder.java`
- `handler/src/main/java/io/netty/handler/ssl/JdkSslContext.java`
- `handler/src/main/java/io/netty/handler/ssl/OpenSslContext.java`

重点：SslHandler 的握手流程、SSLEngine 的 wrap/unwrap、ALPN 协商。

- [ ] **Step 2: 生成文档并提交**

---

### Task 23: 处理器 — Idle/Logging/Proxy/Traffic/OCSP（5 篇）

**并行执行：** 单 agent 处理（每篇较短，合并处理提高效率）

**Files:**
- Create: `docs/netty-analysis/07-处理器与扩展/02-idle-handler.md`
- Create: `docs/netty-analysis/07-处理器与扩展/03-logging-handler.md`
- Create: `docs/netty-analysis/07-处理器与扩展/04-proxy-handler.md`
- Create: `docs/netty-analysis/07-处理器与扩展/05-traffic-shaping.md`
- Create: `docs/netty-analysis/07-处理器与扩展/06-ocsp.md`

- [ ] **Step 1: 分析 IdleStateHandler**

阅读 `handler/src/main/java/io/netty/handler/timeout/IdleStateHandler.java`，重点：readerIdleTime/writerIdleTime/allIdleTime、定时检测机制。

- [ ] **Step 2: 分析 LoggingHandler**

阅读 `handler/src/main/java/io/netty/handler/logging/LoggingHandler.java`，重点：ByteBuf 的日志格式化。

- [ ] **Step 3: 分析 ProxyHandler**

阅读 `handler-proxy/src/main/java/io/netty/handler/proxy/ProxyHandler.java` 及其子类，重点：SOCKS4/5、HTTP CONNECT 代理握手流程。

- [ ] **Step 4: 分析 TrafficShapingHandler**

阅读 `handler/src/main/java/io/netty/handler/traffic/TrafficShapingHandler.java`，重点：限流算法、ChannelTrafficShapingHandler vs GlobalTrafficShapingHandler。

- [ ] **Step 5: 分析 OCSP**

阅读 `handler-ssl-ocsp/` 模块，简述 OCSP 证书验证的实现。

- [ ] **Step 6: 生成 5 篇文档并提交**

---

### Task 24: 域名解析（2 篇）

**并行执行：** 与工具文档并行

**Files:**
- Create: `docs/netty-analysis/08-域名解析/01-resolver-basics.md`
- Create: `docs/netty-analysis/08-域名解析/02-dns-resolver.md`

- [ ] **Step 1: 分析基础解析器**

阅读 `resolver/src/main/java/io/netty/resolver/` 目录：
- `AddressResolverGroup.java`
- `InetSocketAddressResolver.java`
- `DefaultAddressResolverGroup.java`

- [ ] **Step 2: 分析 DNS 解析器**

阅读 `resolver-dns/src/main/java/io/netty/resolver/dns/`：
- `DnsNameResolver.java` — 核心解析器
- `DnsCache.java` — DNS 缓存
- `DnsServerAddresses.java` — DNS 服务器地址管理
- `DnsAddressResolverGroup.java`

重点：递归查询、搜索域、CNAME 跟踪、缓存策略。

- [ ] **Step 3: 生成 2 篇文档并提交**

---

### Task 25: 工具与基础设施（4 篇）

**并行执行：** 与域名解析文档并行

**Files:**
- Create: `docs/netty-analysis/09-工具与基础设施/01-common-utilities.md`
- Create: `docs/netty-analysis/09-工具与基础设施/02-attribute-key.md`
- Create: `docs/netty-analysis/09-工具与基础设施/03-resource-leak.md`
- Create: `docs/netty-analysis/09-工具与基础设施/04-concurrency-utils.md`

- [ ] **Step 1: 分析通用工具**

阅读 `common/src/main/java/io/netty/util/` 下的关键类：
- `HashedWheelTimer.java` — 时间轮定时器
- `InternalLoggerFactory.java` — 日志抽象
- `AttributeMap.java` / `AttributeKey.java` — Channel 属性

- [ ] **Step 2: 分析 AttributeKey 机制**

深入 `AttributeKey` 的 ConcurrentHashMap 实现、`@Deprecated` 的 `valueOf()` vs `newInstance()`。

- [ ] **Step 3: 分析并发工具**

阅读：
- `common/src/main/java/io/netty/util/concurrent/NonStickyEventExecutorGroup.java`
- `common/src/main/java/io/netty/util/concurrent/UnorderedThreadPoolEventExecutor.java`
- `common/src/main/java/io/netty/util/concurrent/AbstractEventExecutor.java`

- [ ] **Step 4: 生成 4 篇文档并提交**

---

## 阶段五：原生集成、测试与扩展（9 篇）

> 理解 Netty 的原生代码集成、测试体系和高级主题

### Task 26: 原生集成（3 篇）

**并行执行：** 与测试/扩展文档并行

**Files:**
- Create: `docs/netty-analysis/10-原生集成/01-native-jni.md`
- Create: `docs/netty-analysis/10-原生集成/02-native-transport.md`
- Create: `docs/netty-analysis/10-原生集成/03-native-ssl.md`

- [ ] **Step 1: 分析 JNI 集成机制**

阅读：
- `common/src/main/java/io/netty/util/internal/NativeLibraryLoader.java`
- `transport-native-unix-common/src/main/java/io/netty/channel/unix/` 目录

重点：NativeLibraryLoader 的加载策略、JNI 方法签名。

- [ ] **Step 2: 分析原生传输层 C 代码**

阅读 `transport-native-epoll/src/main/c/` 下的 C 代码：
- `netty_epoll_native.c` — epoll JNI 实现
- `netty_unix_socket.c` — socket 操作

- [ ] **Step 3: 分析原生 SSL**

阅读：
- `handler/src/main/java/io/netty/handler/ssl/ReferenceCountedOpenSslContext.java`
- `handler/src/main/java/io/netty/handler/ssl/ReferenceCountedOpenSslEngine.java`

重点：BoringSSL/OpenSSL 的 JNI 调用、SSL_CTX 配置。

- [ ] **Step 4: 生成 3 篇文档并提交**

---

### Task 27: 测试体系（3 篇）

**并行执行：** 与原生集成/扩展文档并行

**Files:**
- Create: `docs/netty-analysis/11-测试体系/01-test-framework.md`
- Create: `docs/netty-analysis/11-测试体系/02-testsuite.md`
- Create: `docs/netty-analysis/11-测试体系/03-benchmark.md`

- [ ] **Step 1: 分析测试基础设施**

阅读：
- `transport/src/main/java/io/netty/channel/embedded/EmbeddedChannel.java`
- `transport/src/main/java/io/netty/channel/embedded/EmbeddedEventLoop.java`

重点：EmbeddedChannel 如何模拟 Channel 行为、writeInbound/writeOutbound。

- [ ] **Step 2: 分析 testsuite 模块**

阅读 `testsuite/src/main/java/io/netty/testsuite/` 和 `testsuite-common/`，理解跨传输层的测试策略。

- [ ] **Step 3: 分析微基准测试**

阅读 `microbench/src/main/java/io/netty/microbench/`，理解 JMH 基准测试的编写方式。

- [ ] **Step 4: 生成 3 篇文档并提交**

---

### Task 28: 扩展话题（6 篇）

**并行执行：** 单 agent 处理（分析性文档，需要综合视角）

**Files:**
- Create: `docs/netty-analysis/12-扩展话题/01-design-patterns.md`
- Create: `docs/netty-analysis/12-扩展话题/02-performance-tuning.md`
- Create: `docs/netty-analysis/12-扩展话题/03-graalvm-native.md`
- Create: `docs/netty-analysis/12-扩展话题/04-nio-vs-epoll-vs-io_uring.md`
- Create: `docs/netty-analysis/12-扩展话题/05-netty-vs-other-frames.md`
- Create: `docs/netty-analysis/12-扩展话题/06-version-evolution.md`

- [ ] **Step 1: 分析设计模式**

从前面的分析中提取 Netty 使用的设计模式：
- 责任链模式（Pipeline/Handler 链）
- Builder 模式（Bootstrap）
- 装饰器模式（WrappedByteBuf）
- 观察者模式（Future/Listener）
- 工厂模式（ChannelFactory/ByteBufAllocator）
- 策略模式（EventLoopChooser）
- 模板方法（AbstractChannel）

- [ ] **Step 2: 分析性能调优要点**

整理 Netty 的性能优化手段：
- 内存池化
- 对象池化（Recycler）
- 零拷贝（CompositeByteBuf/SlicedByteBuf）
- 无锁串行化（EventLoop 单线程模型）
- 自适应缓冲区大小（AdaptiveRecvByteBufAllocator）
- writev/gathering write

- [ ] **Step 3: 分析 GraalVM Native Image 支持**

阅读 `testsuite-native-image/` 模块，理解 Netty 对 GraalVM 的适配。

- [ ] **Step 4: 生成 6 篇文档并提交**

---

### Task 29: README 和思维导图（收尾）

**Files:**
- Create: `docs/netty-analysis/README.md`

- [ ] **Step 1: 编写 README.md**

包含：
- 分析项目总览
- 阅读指南（推荐阅读顺序）
- 文档索引（带链接）
- 各阶段思维导图

- [ ] **Step 2: 生成各阶段思维导图**

为 5 个阶段各生成一份 Markdown 格式的思维导图。

- [ ] **Step 3: 最终验证并提交**

验证所有 60 篇文档完整性、链接正确性、架构图可渲染。

---

## 执行方式

**推荐：Subagent-Driven Development**

每个 Task 分派一个独立的 subagent 执行，Task 之间有 review 检查点。

**并行策略：**
- 阶段一：Task 1 (概述) | Task 2-5 (核心抽象 ×4) | Task 6-7 (流水线 ×2) | Task 8 (全链路)
- 阶段二：Task 9-12 (内存管理 ×4) | Task 13-15 (传输层 ×3)
- 阶段三：Task 16 (编解码框架) | Task 17-21 (协议实现 ×5)
- 阶段四：Task 22-23 (处理器 ×2) | Task 24-25 (解析+工具 ×2)
- 阶段五：Task 26-28 (原生+测试+扩展 ×3) | Task 29 (收尾)
