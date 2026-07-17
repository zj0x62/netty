# STOMP 协议编解码实现

## 1. 概述

Netty 的 `codec-stomp` 模块实现了 STOMP（Simple Text Oriented Messaging Protocol）协议的编解码。STOMP 是一种基于文本的消息协议，广泛用于消息中间件（如 ActiveMQ、RabbitMQ）。其帧结构简单：COMMAND 行 + Headers 键值对 + 空行 + Body + NUL 终止符。

Netty 对 STOMP 的实现采用了与 HTTP 类似的 Subframe 模式——将帧拆分为 HeadersSubframe 和 ContentSubframe 进行流式处理，再通过 Aggregator 聚合为完整的 StompFrame。

**源码路径**：`codec-stomp/src/main/java/io/netty/handler/codec/stomp/`

## 2. 架构图

```
                    StompSubframe (marker)
                    /               \
     StompHeadersSubframe      StompContentSubframe
            |                        |
  DefaultStompHeadersSubframe   DefaultStompContentSubframe
            |                        |
            |              LastStompContentSubframe
            |                        |
            +--------+---------------+
                     |
               StompFrame (= StompHeadersSubframe + LastStompContentSubframe)
                     |
             DefaultStompFrame
```

**Pipeline 结构**：

```
StompSubframeDecoder  →  StompSubframeAggregator  →  StompSubframeEncoder
       解码                  聚合（可选）                  编码
```

## 3. 核心类分析

### 3.1 STOMP 命令枚举

**StompCommand**（`StompCommand.java`）：定义所有 STOMP 命令：

```java
public enum StompCommand {
    STOMP, CONNECT, CONNECTED,   // 连接阶段
    SEND, SUBSCRIBE, UNSUBSCRIBE, // 消息发送/订阅
    ACK, NACK,                     // 消息确认
    BEGIN, ABORT, COMMIT,          // 事务
    DISCONNECT,                    // 断开
    MESSAGE, RECEIPT, ERROR,       // 服务端响应
    UNKNOWN                        // 未知命令
}
```

### 3.2 帧结构常量

**StompConstants**（`StompConstants.java`）：

```java
static final byte CR = 13;    // \r
static final byte LF = 10;    // \n
static final byte NUL = 0;    // 帧终止符
static final byte COLON = 58; // : 分隔 header name 和 value
```

STOMP 帧的文本格式：

```
COMMAND\n
header1:value1\n
header2:value2\n
\n
body\0
```

### 3.3 Subframe 接口体系

**StompSubframe**（`StompSubframe.java`）：所有子帧的标记接口，仅继承 `DecoderResultProvider`。

**StompHeadersSubframe**（`StompHeadersSubframe.java`）：头部子帧接口，提供 `command()` 和 `headers()` 方法。

**StompContentSubframe**（`StompContentSubframe.java`）：内容子帧接口，继承 `ByteBufHolder`。

**LastStompContentSubframe**：最后一个内容子帧，标识内容结束。

**StompFrame**（`StompFrame.java`）：完整的 STOMP 帧，同时继承 `StompHeadersSubframe` 和 `LastStompContentSubframe`。

### 3.4 Header 系统

**StompHeaders**（`StompHeaders.java`）：继承 `Headers<CharSequence, CharSequence, StompHeaders>`，定义所有标准 Header 名称常量：

```java
AsciiString ACCEPT_VERSION  // 客户端支持的 STOMP 版本
AsciiString HOST            // 目标主机
AsciiString LOGIN           // 登录用户名
AsciiString PASSCODE        // 密码
AsciiString HEART_BEAT      // 心跳配置
AsciiString VERSION         // 协商后的版本
AsciiString SESSION         // 会话 ID
AsciiString DESTINATION     // 目的地
AsciiString MESSAGE_ID      // 消息 ID
AsciiString SUBSCRIPTION    // 订阅 ID
AsciiString CONTENT_LENGTH  // 内容长度
AsciiString CONTENT_TYPE    // 内容类型
```

### 3.5 解码器 StompSubframeDecoder

**StompSubframeDecoder**（`StompSubframeDecoder.java`）：继承 `ReplayingDecoder<State>`，核心解码状态机：

```java
enum State {
    SKIP_CONTROL_CHARACTERS,  // 跳过前导 CR/LF
    READ_HEADERS,             // 读取 COMMAND 行 + Header 行
    READ_CONTENT,             // 读取 Body 内容
    FINALIZE_FRAME_READ,      // 读取 NUL 终止符
    BAD_FRAME,                // 错误帧，跳过所有数据
    INVALID_CHUNK
}
```

**解码流程**：

1. **SKIP_CONTROL_CHARACTERS**：跳过前导的 CR/LF 空白字符
2. **READ_HEADERS**：
   - 调用 `readCommand()` 逐字节解析 COMMAND 行（到 LF 为止）
   - 调用 `readHeaders()` 循环解析 `name:value\n` 格式的 header
   - 遇到空行（连续 LF）表示 header 结束
   - 若存在 `content-length` header，进入 `READ_CONTENT` 按长度读取
   - 否则按 NUL 终止符读取
3. **READ_CONTENT**：按 `maxChunkSize` 分块读取 body，最后一块为 `LastStompContentSubframe`
4. **FINALIZE_FRAME_READ**：读取 NUL 字节，输出最后一块内容，重置解码器

**UTF-8 解析**：内部类 `Utf8LineParser` 实现逐字节 UTF-8 解码，支持 1/2/3 字节编码，同时进行行长度限制检查。

**Header 解析与反转义**：内部类 `HeaderParser` 继承 `Utf8LineParser`，额外处理：

- 以冒号 `:` 分割 header name 和 value
- 对 CONNECT/CONNECTED 以外的命令，header 中的特殊字符需要反转义：
  - `\c` → `:`
  - `\n` → 换行
  - `\r` → 回车
  - `\\` → `\`
- CONNECT/CONNECTED 命令不做反转义，但会验证不包含非法字符（`\r`、`\n`、`:`、`\0`）

**content-length 处理**：如果 header 中包含 `content-length`，解码器按精确长度读取 body，支持二进制内容。否则通过 NUL 字节确定 body 边界。

### 3.6 聚合器 StompSubframeAggregator

**StompSubframeAggregator**（`StompSubframeAggregator.java`）：继承 `MessageAggregator`，将 `StompHeadersSubframe` + 多个 `StompContentSubframe` 聚合为 `StompFrame`。

```java
protected StompFrame beginAggregation(StompHeadersSubframe start, ByteBuf content) {
    StompFrame ret = new DefaultStompFrame(start.command(), content);
    ret.headers().set(start.headers());
    return ret;
}
```

支持 `maxContentLength` 限制，超过时抛出 `TooLongFrameException`。通过 `content-length` header 预检查内容长度。

### 3.7 编码器 StompSubframeEncoder

**StompSubframeEncoder**（`StompSubframeEncoder.java`）：继承 `MessageToMessageEncoder<StompSubframe>`，处理三种消息类型：

1. **StompFrame**（完整帧）：一次编码 header + content + NUL
2. **StompHeadersSubframe**：只编码 header 部分
3. **StompContentSubframe**：只编码 content 部分，`LastStompContentSubframe` 追加 NUL

**编码 header 的转义处理**：

```java
// 对 CONNECT/CONNECTED 以外的命令，header key/value 需要转义
'\\' → '\\\\'
':'  → '\\c'
'\n' → '\\n'
'\r' → '\\r'
'\0' → 非法，抛出异常
```

编码器使用 `FastThreadLocal<LinkedHashMap>` 缓存标准 header key 的转义结果（最多 32 条，LRU 淘汰），避免重复转义。

**扩展点**：提供三个 `convert*()` 方法供子类重写，将编码后的 `ByteBuf` 转换为其他消息类型：

```java
protected Object convertFullFrame(StompFrame original, ByteBuf encoded)
protected Object convertHeadersSubFrame(StompHeadersSubframe original, ByteBuf encoded)
protected Object convertContentSubFrame(StompContentSubframe original, ByteBuf encoded)
```

## 4. 设计思想

### 4.1 Subframe 流式处理

与 HTTP 的 `HttpObjectAggregator` 模式一致，STOMP 将帧拆分为 Subframe 进行流式处理：

- 不需要聚合时，直接使用 `StompSubframeDecoder`，逐块处理 content
- 需要完整帧时，在 decoder 后添加 `StompSubframeAggregator`

这种设计对大消息体（如二进制附件）非常友好，避免一次性加载整个 body 到内存。

### 4.2 ReplayingDecoder

`StompSubframeDecoder` 继承 `ReplayingDecoder`，利用其"回放"机制简化字节不足时的处理。当缓冲区数据不够时，`ReplayingDecoder` 自动抛出 `Signal` 并在下次数据到达时从 checkpoint 位置重新执行。

### 4.3 Header 转义的性能优化

编码器对标准 header key 使用 `FastThreadLocal` + LRU 缓存，避免每次编码都进行转义操作。值不缓存因为值变化频繁。

### 4.4 content-length 驱动的双模式解码

- 有 `content-length`：按精确字节数读取 body，支持二进制内容
- 无 `content-length`：按 NUL 字节扫描确定 body 边界，仅支持文本内容

## 5. 模块交互

```
         入站 ByteBuf
              |
              v
    StompSubframeDecoder (ReplayingDecoder)
     ├─ SKIP_CONTROL_CHARACTERS
     ├─ READ_HEADERS → 输出 StompHeadersSubframe
     ├─ READ_CONTENT → 输出 StompContentSubframe × N
     └─ FINALIZE_FRAME_READ → 输出 LastStompContentSubframe
              |
              v (可选)
    StompSubframeAggregator (MessageAggregator)
              |
              v
         StompFrame (完整帧)
              |
              v
    业务 Handler 处理
              |
              v
    StompSubframeEncoder (MessageToMessageEncoder)
     ├─ StompFrame → 编码 header + content + NUL
     ├─ StompHeadersSubframe → 编码 header
     └─ StompContentSubframe → 编码 content
              |
              v
         出站 ByteBuf
```

## 6. 关键流程

### 6.1 CONNECT 帧解码

```
输入: "CONNECT\naccept-version:1.2\nhost:localhost\n\n\0"

1. SKIP_CONTROL_CHARACTERS：无前导字符
2. READ_HEADERS：
   - readCommand() → "CONNECT" → StompCommand.CONNECT
   - readHeaders()：
     - "accept-version:1.2" → headers.add("accept-version", "1.2")
     - "host:localhost" → headers.add("host", "localhost")
     - 空行 → header 结束
   - 无 content-length → 状态转为 FINALIZE_FRAME_READ
3. FINALIZE_FRAME_READ：
   - 读取 NUL 字节
   - lastContent 为 null → LastStompContentSubframe.EMPTY_LAST_CONTENT
4. 输出: StompHeadersSubframe(CONNECT, {accept-version:1.2, host:localhost})
       + LastStompContentSubframe(EMPTY)
```

### 6.2 SEND 帧编码

```
输入: StompFrame(SEND, {destination:/queue/test}, "Hello World")

编码过程:
1. 写入 "SEND\n"
2. 遍历 headers:
   - "destination" → 写入 "destination:/queue/test\n"
3. 写入 "\n"（空行分隔 header 和 body）
4. 写入 "Hello World"
5. 写入 NUL(0x00)

输出: "SEND\ndestination:/queue/test\n\nHello World\0"
```

### 6.3 Header 转义示例

```
原始 header value: "server:1.0"
CONNECT/CONNECTED 命令: 验证失败（包含非法字符 ':'）
其他命令: 转义为 "server\c1.0"

原始 header value: "line1\nline2"
其他命令: 转义为 "line1\nline2"（\n 字面量）
```

## 7. 学习要点

1. **文本协议帧结构**：COMMAND + Headers + 空行 + Body + NUL，理解 STOMP 的简洁文本协议设计
2. **Subframe 模式**：与 HTTP 一致的流式处理模式，StompHeadersSubframe + StompContentSubframe → StompFrame
3. **ReplayingDecoder 应用**：STOMP 解码器是学习 `ReplayingDecoder` 状态机的优秀范例
4. **Header 转义机制**：STOMP 1.1+ 的 header 转义规则（`\c`、`\n`、`\r`、`\\`），以及 CONNECT/CONNECTED 命令的特殊处理
5. **UTF-8 逐字节解析**：`Utf8LineParser` 在字节层面处理 UTF-8 多字节编码，避免先转 String 再解析的额外开销
6. **content-length 驱动的双模式**：有 content-length 时按长度读取（支持二进制），无则按 NUL 扫描（仅文本）
7. **FastThreadLocal 缓存**：编码器对标准 header key 的转义结果使用线程本地 LRU 缓存，是性能优化的典型手法
