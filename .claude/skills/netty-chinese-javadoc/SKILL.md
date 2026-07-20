---
name: netty-chinese-javadoc
description: Use when adding Chinese Javadoc comments to Netty source code — covers class/interface/method annotations with accurate networking terminology, Javadoc format compliance, and code immutability constraints
---

# Netty 中文 Javadoc 注释

为 Netty 源码添加高质量中文注释，替换原有英文 Javadoc。

## 核心原则

**仅修改注释，严禁改动代码逻辑、包名、类名、方法签名、变量名。**

## 专业术语规范

Netty 专有名词保持英文原意，必要时采用 `English(中文解释)` 形式：

| 英文 | 中文 |
|------|------|
| EventLoopGroup | 事件循环组 |
| ChannelHandler | 通道处理器 |
| ChannelPipeline | 通道管道 |
| ByteBuf | 字节缓冲区 |
| Bootstrap | 引导类 |
| ChannelFuture | 通道异步结果 |
| ChannelOutboundBuffer | 通道出站缓冲区 |
| ReferenceCount | 引用计数 |

理解 Netty 的异步非阻塞、Reactor 线程模型，注释描述须符合技术原理。

## 注释格式

### 类/接口

```java
/**
 * {@link Channel} I/O 操作的异步结果。
 * <p>
 * Netty 中所有的 I/O 操作都是异步的……
 *
 * <h3>子标题</h3>
 * 说明文字……
 *
 * <pre>
 * // 代码示例
 * </pre>
 */
```

- 说明该类的作用、在 Netty 架构中的位置（Inbound/Outbound）
- 包含典型应用场景
- 涉及状态机时使用 ASCII 图示

### 方法

```java
/**
 * 方法功能说明。
 *
 * @param name 参数说明
 * @return 返回值说明（特别是 ChannelFuture 等异步回调返回值）
 * @throws ExceptionType 异常说明
 */
```

### 行内注释

涉及 Netty 核心机制的关键代码行添加 `//` 注释：

```java
// pipeline 入站传播：从 HeadContext 向 TailContext 方向触发
ctx.fireChannelRead(msg);

// 引用计数减 1，归零时释放底层内存
ReferenceCountUtil.release(msg);
```

## 覆盖范围

- 类声明
- 关键成员变量
- 所有公开方法
- 核心私有方法

## 工作流程

1. 读取目标文件
2. 逐个替换英文 Javadoc 为中文
3. 补充缺失的 `@param`、`@return`、`@throws`
4. 对核心机制添加行内注释
5. 更新 `CHANGELOG.md`
