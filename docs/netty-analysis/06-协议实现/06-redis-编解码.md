# Netty Redis 协议（RESP）实现深度分析

## 1. 概述

Redis 使用 RESP（REdis Serialization Protocol）作为客户端与服务器之间的通信协议。RESP 是一种文本协议，具有简单、高效、易于解析的特点。Netty 的 `codec-redis` 模块提供了完整的 RESP 协议编解码实现。

该模块的核心设计特点：

- **类型前缀驱动**：RESP 协议通过首字节（`+`、`-`、`:`、`$`、`*`）区分不同的数据类型
- **流式解码**：支持大 Bulk String 的分块传输，避免一次性加载大量数据到内存
- **聚合器模式**：通过 `RedisArrayAggregator` 和 `RedisBulkStringAggregator` 将流式消息聚合为完整消息
- **消息池优化**：使用 `FixedRedisMessagePool` 缓存常用消息，减少对象创建

## 2. 架构图

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Redis Codec 模块架构                              │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌────────────────────────────────────────────────────────────┐    │
│  │              RESP 数据类型                                   │    │
│  │  Simple String  '+'  简单字符串，以 \r\n 结尾               │    │
│  │  Error          '-'  错误信息，以 \r\n 结尾                 │    │
│  │  Integer        ':'  整数，以 \r\n 结尾                     │    │
│  │  Bulk String    '$'  二进制安全字符串，长度前缀              │    │
│  │  Array          '*'  数组，长度前缀                         │    │
│  │  Inline Command      内联命令（非标准 RESP）                │    │
│  └────────────────────────────────────────────────────────────┘    │
│                                                                     │
│  ┌─────────────────┐      ┌─────────────────┐      ┌────────────┐ │
│  │  RedisDecoder    │      │RedisArrayAggregator│   │RedisBulkStr│ │
│  │  (状态机)        │ ──►  │  (数组聚合)       │ ──► │Aggregator  │ │
│  │                  │      │                   │     │(字符串聚合)│ │
│  │  DECODE_TYPE     │      │  聚合 ArrayHeader │     │            │ │
│  │  DECODE_INLINE   │      │  + 后续元素为      │     │            │ │
│  │  DECODE_LENGTH   │      │  ArrayRedisMessage │     │            │ │
│  │  DECODE_BULK_*   │      │                   │     │            │ │
│  └─────────────────┘      └─────────────────┘      └────────────┘ │
│                                                                     │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │  RedisEncoder (MessageToMessageEncoder)                      │   │
│  │  ├─ writeSimpleStringMessage()                               │   │
│  │  ├─ writeErrorMessage()                                      │   │
│  │  ├─ writeIntegerMessage()                                    │   │
│  │  ├─ writeFullBulkStringMessage()                             │   │
│  │  ├─ writeBulkStringHeader() + writeBulkStringContent()       │   │
│  │  └─ writeArrayMessage() / writeArrayHeader()                 │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                                                                     │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │  FixedRedisMessagePool (消息池)                              │   │
│  │  缓存常用消息（如 +OK、-ERR、:0 等）减少 GC 压力            │   │
│  └─────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

## 3. 核心类分析

### 3.1 RedisDecoder — RESP 协议解码器

`RedisDecoder` 继承自 `ByteToMessageDecoder`，通过状态机解析 RESP 协议。

```java
public final class RedisDecoder extends ByteToMessageDecoder {
    private enum State {
        DECODE_TYPE,                // 阶段1: 读取类型前缀字节
        DECODE_INLINE,              // 阶段2: 读取内联命令/简单字符串/错误/整数
        DECODE_LENGTH,              // 阶段3: 读取 Bulk String 或 Array 的长度
        DECODE_BULK_STRING_EOL,     // 阶段4: 读取空 Bulk String 的结尾 \r\n
        DECODE_BULK_STRING_CONTENT, // 阶段5: 读取 Bulk String 的内容
    }
}
```

**状态机流转过程**：

```
                    ┌──────────────────────────────────────────┐
                    │                                          │
                    ▼                                          │
             DECODE_TYPE ──────────────────┐                   │
                │                          │                   │
                │ (inline 类型)            │ (非 inline 类型)  │
                ▼                          ▼                   │
          DECODE_INLINE             DECODE_LENGTH              │
                │                     │       │                │
                │                     │       │ (Bulk String)  │
                ▼                     │       ▼                │
           (输出消息) ────────────────┤  DECODE_BULK_STRING_EOL│
                    │                 │       │                │
                    │                 │       │ (长度>0)        │
                    │                 │       ▼                │
                    │                 │  DECODE_BULK_STRING_CONTENT
                    │                 │       │                │
                    │                 │       │ (读取完成)      │
                    │                 │       ▼                │
                    │                 │  (输出消息) ───────────┘
                    │                 │
                    │                 │ (Array)
                    │                 ▼
                    │           (输出 ArrayHeaderRedisMessage)
                    │                 │
                    └─────────────────┘
```

### 3.2 RedisMessageType — 消息类型枚举

```java
public enum RedisMessageType {
    INLINE_COMMAND(null, true),     // 内联命令（非标准 RESP）
    SIMPLE_STRING((byte) '+', true), // 简单字符串
    ERROR((byte) '-', true),         // 错误
    INTEGER((byte) ':', true),       // 整数
    BULK_STRING((byte) '$', false),  // 批量字符串
    ARRAY_HEADER((byte) '*', false); // 数组头
}
```

**类型判断逻辑**：

```java
public static RedisMessageType readFrom(ByteBuf in, boolean decodeInlineCommands) {
    final int initialIndex = in.readerIndex();
    final RedisMessageType type = valueOf(in.readByte());
    if (type == INLINE_COMMAND) {
        if (!decodeInlineCommands) {
            throw new RedisCodecException("Decoding of inline commands is disabled");
        }
        // 内联命令不消耗类型字节，重置读指针
        in.readerIndex(initialIndex);
    }
    return type;
}
```

### 3.3 BulkStringHeaderRedisMessage — 批量字符串头

Bulk String 是 RESP 协议中最常用的数据类型，支持二进制安全的数据传输。

```java
public class BulkStringHeaderRedisMessage implements RedisMessage {
    private final int bulkStringLength;  // 字符串长度，-1 表示 NULL

    public boolean isNull() {
        return bulkStringLength == RedisConstants.NULL_VALUE;  // -1
    }
}
```

**Bulk String 的三种情况**：

1. **正常字符串**：`$<length>\r\n<data>\r\n`，如 `$5\r\nhello\r\n`
2. **空字符串**：`$0\r\n\r\n`
3. **NULL 值**：`$-1\r\n`

### 3.4 ArrayHeaderRedisMessage — 数组头

```java
public class ArrayHeaderRedisMessage implements RedisMessage {
    private final long length;  // 数组长度，-1 表示 NULL

    public boolean isNull() {
        return length == RedisConstants.NULL_VALUE;  // -1
    }
}
```

**Array 的两种情况**：

1. **正常数组**：`*<count>\r\n<element1><element2>...`
2. **NULL 数组**：`*-1\r\n`

### 3.5 RedisEncoder — 编码器

`RedisEncoder` 使用 `instanceof` 链进行消息类型分派：

```java
private void writeRedisMessage(ByteBufAllocator allocator, RedisMessage msg, List<Object> out) {
    if (msg instanceof InlineCommandRedisMessage) {
        writeInlineCommandMessage(allocator, (InlineCommandRedisMessage) msg, out);
    } else if (msg instanceof SimpleStringRedisMessage) {
        writeSimpleStringMessage(allocator, (SimpleStringRedisMessage) msg, out);
    } else if (msg instanceof ErrorRedisMessage) {
        writeErrorMessage(allocator, (ErrorRedisMessage) msg, out);
    } else if (msg instanceof IntegerRedisMessage) {
        writeIntegerMessage(allocator, (IntegerRedisMessage) msg, out);
    } else if (msg instanceof FullBulkStringRedisMessage) {
        writeFullBulkStringMessage(allocator, (FullBulkStringRedisMessage) msg, out);
    } else if (msg instanceof BulkStringRedisContent) {
        writeBulkStringContent(allocator, (BulkStringRedisContent) msg, out);
    } else if (msg instanceof BulkStringHeaderRedisMessage) {
        writeBulkStringHeader(allocator, (BulkStringHeaderRedisMessage) msg, out);
    } else if (msg instanceof ArrayHeaderRedisMessage) {
        writeArrayHeader(allocator, (ArrayHeaderRedisMessage) msg, out);
    } else if (msg instanceof ArrayRedisMessage) {
        writeArrayMessage(allocator, (ArrayRedisMessage) msg, out);
    }
}
```

### 3.6 RedisConstants — 协议常量

```java
final class RedisConstants {
    static final int TYPE_LENGTH = 1;              // 类型前缀长度
    static final int EOL_LENGTH = 2;               // \r\n 长度
    static final int NULL_LENGTH = 2;              // "-1" 长度
    static final int NULL_VALUE = -1;              // NULL 值
    static final int REDIS_MESSAGE_MAX_LENGTH = 512 * 1024 * 1024; // 512MB
    static final int REDIS_INLINE_MESSAGE_MAX_LENGTH = 64 * 1024;  // 64KB
    static final short NULL_SHORT = makeShort('-', '1');   // "-1"
    static final short EOL_SHORT = makeShort('\r', '\n');  // "\r\n"
}
```

## 4. 设计思想

### 4.1 流式解码与聚合

RESP 协议的 Bulk String 可能非常大（最大 512MB），Netty 采用流式解码避免内存溢出：

```
解码器输出（流式）:
  BulkStringHeaderRedisMessage(length=1000000)
  BulkStringRedisContent(chunk1)
  BulkStringRedisContent(chunk2)
  ...
  LastBulkStringRedisContent(finalChunk)

聚合器输出:
  FullBulkStringRedisMessage(content=完整数据)
```

**RedisBulkStringAggregator** 将流式消息聚合为完整的 `FullBulkStringRedisMessage`。

**RedisArrayAggregator** 将 `ArrayHeaderRedisMessage` 和后续的元素聚合为 `ArrayRedisMessage`。

### 4.2 行分隔符处理

RESP 协议使用 `\r\n`（CRLF）作为行分隔符：

```java
private static void readEndOfLine(final ByteBuf in) {
    final short delim = in.readShort();
    if (RedisConstants.EOL_SHORT == delim) {
        return;
    }
    throw new RedisCodecException("delimiter: [" + bytes[0] + "," + bytes[1] + "] (expected: \\r\\n)");
}
```

### 4.3 数值解析优化

```java
private static final class ToPositiveLongProcessor implements ByteProcessor {
    private long result;

    @Override
    public boolean process(byte value) throws Exception {
        if (value < '0' || value > '9') {
            throw new RedisCodecException("bad byte in number: " + value);
        }
        result = result * 10 + (value - '0');
        return true;
    }
}
```

使用 `ByteProcessor` 接口进行高效的数值解析，避免创建临时字符串。

### 4.4 消息池化

`FixedRedisMessagePool` 缓存常用的小整数和简单字符串消息：

```java
// 缓存 -128 到 127 的整数
// 缓存 "OK"、"PONG" 等常用简单字符串
// 缓存常见的错误消息
```

### 4.5 内联命令支持

Redis 协议还支持非标准的内联命令格式：

```
标准 RESP:  *2\r\n$3\r\nGET\r\n$3\r\nkey\r\n
内联命令:   GET key\r\n
```

通过 `decodeInlineCommands` 参数控制是否启用内联命令解码。

## 5. 模块交互

### 5.1 解码器在 Pipeline 中的位置

```
ChannelPipeline (典型配置):
  ┌──────────────────────────────────────────────────────────────┐
  │  RedisEncoder (出站)                                          │
  │    ↑                                                          │
  │  业务 Handler                                                 │
  │    ↑                                                          │
  │  RedisArrayAggregator (入站，聚合数组)                        │
  │    ↑                                                          │
  │  RedisBulkStringAggregator (入站，聚合批量字符串，可选)        │
  │    ↑                                                          │
  │  RedisDecoder (入站)                                          │
  └──────────────────────────────────────────────────────────────┘
```

### 5.2 聚合器的状态管理

**RedisArrayAggregator** 使用 `Deque<AggregateState>` 管理嵌套数组：

```java
private static final class AggregateState {
    private final int length;                    // 期望的元素数量
    private final List<RedisMessage> children;   // 已收集的元素
}
```

当收到 `ArrayHeaderRedisMessage` 时，压入新的 `AggregateState`；当元素收集完成时，弹出并创建 `ArrayRedisMessage`。

### 5.3 消息类型层次结构

```
RedisMessage (接口)
├── SimpleStringRedisMessage      (+OK)
├── ErrorRedisMessage             (-ERR ...)
├── IntegerRedisMessage           (:1000)
├── InlineCommandRedisMessage     (GET key)
├── FullBulkStringRedisMessage    (完整的批量字符串)
├── BulkStringHeaderRedisMessage  (批量字符串头，流式)
├── BulkStringRedisContent        (批量字符串内容，流式)
│   ├── DefaultBulkStringRedisContent
│   └── DefaultLastBulkStringRedisContent
├── LastBulkStringRedisContent    (最后一个分块)
├── ArrayHeaderRedisMessage       (数组头)
└── ArrayRedisMessage             (完整的数组)
```

## 6. 关键流程

### 6.1 Simple String 解码流程

```
输入: +OK\r\n

1. DECODE_TYPE
   └─ 读取 '+' → type = SIMPLE_STRING, inline = true
   └─ 状态转换 → DECODE_INLINE

2. DECODE_INLINE
   └─ readLine(in) 查找 \r\n
   └─ 读取 "OK"
   └─ 创建 SimpleStringRedisMessage("OK")
   └─ resetDecoder() → DECODE_TYPE
```

### 6.2 Bulk String 解码流程

```
输入: $5\r\nhello\r\n

1. DECODE_TYPE
   └─ 读取 '$' → type = BULK_STRING, inline = false
   └─ 状态转换 → DECODE_LENGTH

2. DECODE_LENGTH
   └─ readLine(in) 读取 "5"
   └─ parseRedisNumber() → length = 5
   └─ remainingBulkLength = 5
   └─ 调用 decodeBulkString()

3. DECODE_BULK_STRING_CONTENT
   └─ 输出 BulkStringHeaderRedisMessage(5)
   └─ 状态转换 → DECODE_BULK_STRING_CONTENT
   └─ 读取 5 字节内容 + \r\n
   └─ 输出 DefaultLastBulkStringRedisContent("hello")
   └─ resetDecoder() → DECODE_TYPE
```

### 6.3 大 Bulk String 流式解码流程

```
输入: $1000000\r\n<data chunk1><data chunk2>...\r\n

1. DECODE_TYPE → DECODE_LENGTH
   └─ 读取长度 1000000

2. DECODE_BULK_STRING_CONTENT (第一次)
   └─ 输出 BulkStringHeaderRedisMessage(1000000)
   └─ readableBytes < remainingBulkLength + EOL_LENGTH
   └─ 读取部分数据 → DefaultBulkStringRedisContent(chunk1)
   └─ remainingBulkLength -= chunk1.length

3. DECODE_BULK_STRING_CONTENT (后续)
   └─ 继续读取 → DefaultBulkStringRedisContent(chunk2)
   └─ ...

4. DECODE_BULK_STRING_CONTENT (最后一次)
   └─ readableBytes >= remainingBulkLength + EOL_LENGTH
   └─ 读取剩余数据 → DefaultLastBulkStringRedisContent(finalChunk)
   └─ 读取 \r\n
   └─ resetDecoder() → DECODE_TYPE
```

### 6.4 Array 解码流程

```
输入: *2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n

1. DECODE_TYPE → DECODE_LENGTH
   └─ 读取 '*' → type = ARRAY_HEADER
   └─ 读取长度 2
   └─ 输出 ArrayHeaderRedisMessage(2)

2. 后续元素解码
   └─ $3\r\nfoo\r\n → BulkStringHeaderRedisMessage(3) + LastBulkStringRedisContent("foo")
   └─ $3\r\nbar\r\n → BulkStringHeaderRedisMessage(3) + LastBulkStringRedisContent("bar")

3. RedisArrayAggregator 聚合
   └─ 收到 ArrayHeaderRedisMessage(2) → 创建 AggregateState(length=2)
   └─ 收到元素1 → children.add(element1)
   └─ 收到元素2 → children.add(element2)
   └─ children.size() == length → 创建 ArrayRedisMessage(children)
```

### 6.5 NULL 值解码流程

```
输入: $-1\r\n (NULL Bulk String)

1. DECODE_TYPE → DECODE_LENGTH
   └─ 读取 '$' → type = BULK_STRING
   └─ 读取长度 -1

2. decodeBulkString()
   └─ remainingBulkLength == NULL_VALUE (-1)
   └─ 输出 FullBulkStringRedisMessage.NULL_INSTANCE
   └─ resetDecoder()

类似地: *-1\r\n (NULL Array)
   └─ 输出 ArrayHeaderRedisMessage(-1)
   └─ RedisArrayAggregator → ArrayRedisMessage.NULL_INSTANCE
```

## 7. 学习要点

### 7.1 状态机解码模式

Redis 解码器展示了如何设计一个清晰的状态机：

- **状态定义**：每个状态对应协议的一个解析阶段
- **状态转换**：明确的转换条件和目标状态
- **错误恢复**：异常时重置到初始状态

### 7.2 流式处理大数据

- 大 Bulk String 通过 `BulkStringHeaderRedisMessage` + `BulkStringRedisContent` 流式传输
- 聚合器将流式消息组装为完整消息
- 可以选择不使用聚合器，直接处理流式消息以节省内存

### 7.3 行分隔符协议的解析技巧

- 使用 `indexOf()` 查找 `\n` 的位置
- 验证 `\n` 前一个字节是 `\r`
- 使用 `readShort()` 一次性读取并验证 `\r\n`

### 7.4 数值解析优化

- 使用 `ByteProcessor` 避免创建临时字符串
- 先检查长度是否超过 `POSITIVE_LONG_MAX_LENGTH` (19)，避免无效解析
- 支持负数解析

### 7.5 消息池化策略

- 缓存常用的小整数消息（-128 到 127）
- 缓存常用的简单字符串（如 "OK"、"PONG"）
- 减少短生命周期对象的创建，降低 GC 压力

### 7.6 编码器的设计

- 使用 `instanceof` 链进行类型分派（适合类型较多的场景）
- 预计算缓冲区大小，减少扩容
- 使用 `ioBuffer()` 而非 `heapBuffer()`，优化 I/O 操作

### 7.7 常见 RESP 命令的编码示例

```
GET key → *2\r\n$3\r\nGET\r\n$3\r\nkey\r\n
SET key value → *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
+OK → +OK\r\n
:1000 → :1000\r\n
-ERR unknown command → -ERR unknown command\r\n
```
