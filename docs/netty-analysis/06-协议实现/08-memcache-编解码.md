# Memcache 协议编解码实现

## 1. 概述

Netty 的 `codec-memcache` 模块实现了 Memcached 的二进制协议编解码。该模块遵循 Netty 统一的 `Message → ByteBuf` 编码和 `ByteBuf → Message` 解码模式，将二进制协议的固定头部（24 字节）、可选 extras、可选 key 以及变长 value body 映射为类型安全的 Java 对象模型。

**源码路径**：`codec-memcache/src/main/java/io/netty/handler/codec/memcache/`

## 2. 架构图

```
                    MemcacheObject (marker)
                    /                \
          MemcacheMessage      MemcacheContent
          /           \              |
BinaryMemcacheMessage  ...    LastMemcacheContent
      /         \                    |
BinaryMemcacheRequest  BinaryMemcacheResponse   FullMemcacheMessage
      |                    |
DefaultBinaryMemcacheRequest  DefaultBinaryMemcacheResponse
      |                    |
DefaultFullBinaryMemcacheRequest  DefaultFullBinaryMemcacheResponse
```

**Pipeline 编解码器层次**：

```
[客户端 Pipeline]
  BinaryMemcacheRequestEncoder    → 编码请求
  BinaryMemcacheResponseDecoder   → 解码响应
  BinaryMemcacheObjectAggregator  → 聚合分块内容

[服务端 Pipeline]
  BinaryMemcacheRequestDecoder    → 解码请求
  BinaryMemcacheResponseEncoder   → 编码响应
```

使用 `CombinedChannelDuplexHandler` 组合：

- `BinaryMemcacheClientCodec` = ResponseDecoder + RequestEncoder
- `BinaryMemcacheServerCodec` = RequestDecoder + ResponseEncoder

## 3. 核心类分析

### 3.1 接口体系

**MemcacheMessage**（`MemcacheMessage.java`）：所有 Memcache 消息的标记接口，继承 `ReferenceCounted`，提供引用计数管理。

**MemcacheContent**（`MemcacheContent.java`）：内容块接口，继承 `ByteBufHolder`。当 value 体较大时，解码器会将其拆分为多个 `MemcacheContent` 发送上 pipeline，最后一个为 `LastMemcacheContent`。

**BinaryMemcacheMessage**（`binary/BinaryMemcacheMessage.java`）：定义二进制协议消息的全部头部字段：

```java
byte magic();           // 魔数字节，标识请求(0x80)或响应(0x81)
byte opcode();          // 操作码，如 GET(0x00)、SET(0x01)
short keyLength();      // key 长度
byte extrasLength();    // extras 长度
byte dataType();        // 数据类型
int totalBodyLength();  // 总 body 长度 = key + extras + value
int opaque();           // 不透明字段，原样回传
long cas();             // CAS 标识符
ByteBuf key();          // key 内容
ByteBuf extras();       // extras 内容
```

**BinaryMemcacheRequest**：在 `BinaryMemcacheMessage` 基础上增加 `short reserved()` 字段。

**BinaryMemcacheResponse**：在 `BinaryMemcacheMessage` 基础上增加 `short status()` 字段。

### 3.2 消息实现类

**DefaultBinaryMemcacheRequest / DefaultBinaryMemcacheResponse**：基础实现，只包含头部和可选的 key/extras，不包含 value body。用于流式场景，配合 `MemcacheContent` 分块传输 value。

**DefaultFullBinaryMemcacheRequest / DefaultFullBinaryMemcacheResponse**：完整消息实现，在头部 + key + extras 基础上额外持有 `ByteBuf content`。构造时自动计算 `totalBodyLength = keyLength + extrasLength + content.readableBytes()`。

### 3.3 编码器

**AbstractBinaryMemcacheEncoder\<M\>**（`binary/AbstractBinaryMemcacheEncoder.java`）：

```java
protected ByteBuf encodeMessage(ChannelHandlerContext ctx, M msg) {
    ByteBuf buf = ctx.alloc().buffer(MINIMUM_HEADER_SIZE + msg.extrasLength() + msg.keyLength());
    encodeHeader(buf, msg);    // 子类实现，写入 24 字节头部
    encodeExtras(buf, msg.extras());
    encodeKey(buf, msg.key());
    return buf;
}
```

子类 `BinaryMemcacheRequestEncoder` 和 `BinaryMemcacheResponseEncoder` 分别实现 `encodeHeader()`，唯一区别在于请求写 `reserved` 字段，响应写 `status` 字段。

**AbstractMemcacheObjectEncoder**：基类编码器，处理 `MemcacheContent` 分块的透传编码。使用 `expectingMoreContent` 标记确保消息序列正确（必须先发 MemcacheMessage，再发 MemcacheContent 块）。

### 3.4 解码器

**AbstractBinaryMemcacheDecoder\<M\>**（`binary/AbstractBinaryMemcacheDecoder.java`）：核心解码逻辑，基于状态机：

```
READ_HEADER → READ_EXTRAS → READ_KEY → READ_CONTENT → (回到 READ_HEADER)
                                                          ↘ BAD_MESSAGE
```

关键解码流程：

1. **READ_HEADER**：等待至少 24 字节，调用 `decodeHeader()` 读取固定头部
2. **READ_EXTRAS**：根据 `extrasLength` 读取 extras，使用 `in.readRetainedSlice()` 零拷贝
3. **READ_KEY**：根据 `keyLength` 读取 key
4. **READ_CONTENT**：计算 `valueLength = totalBodyLength - keyLength - extrasLength`，按 `chunkSize`（默认 8192）分块读取。最后一块包装为 `DefaultLastMemcacheContent`，中间块为 `DefaultMemcacheContent`
5. **BAD_MESSAGE**：解码出错时跳过所有可读字节

子类 `BinaryMemcacheRequestDecoder` 和 `BinaryMemcacheResponseDecoder` 实现 `decodeHeader()`，分别读取请求头（含 reserved）和响应头（含 status）。

### 3.5 聚合器

**BinaryMemcacheObjectAggregator**（`binary/BinaryMemcacheObjectAggregator.java`）：

继承 `AbstractMemcacheObjectAggregator`，将 `BinaryMemcacheMessage` + 多个 `MemcacheContent` 聚合为 `FullBinaryMemcacheRequest` 或 `FullBinaryMemcacheResponse`。聚合时将所有元数据从 start message 复制到 Full 对象。

### 3.6 操作码与状态码

**BinaryMemcacheOpcodes**：定义所有二进制协议操作码常量：

| 操作码 | 值 | 说明 |
|--------|------|------|
| GET | 0x00 | 获取 |
| SET | 0x01 | 设置 |
| ADD | 0x02 | 添加（不存在时） |
| REPLACE | 0x03 | 替换（存在时） |
| DELETE | 0x04 | 删除 |
| INCREMENT | 0x05 | 递增 |
| DECREMENT | 0x06 | 递减 |
| GETK | 0x0c | 获取并返回 key |
| APPEND | 0x0e | 追加 |
| PREPEND | 0x0f | 前置 |
| SASL_AUTH | 0x21 | SASL 认证 |

带 `Q` 后缀的操作码（如 `SETQ`、`GETQ`）为"quiet"变体，表示静默模式，不返回响应。

**BinaryMemcacheResponseStatus**：响应状态码，如 `SUCCESS(0x00)`、`KEY_ENOENT(0x01)`、`KEY_EEXISTS(0x02)`、`DELTA_BADVAL(0x06)` 等。

### 3.7 CAS 操作

CAS（Compare-And-Swap）是 Memcached 实现乐观锁的核心机制。在二进制协议中，CAS 值存储在消息头部的最后 8 个字节（`long cas` 字段）。

- **SET with CAS**：客户端在 SET 请求中携带 `cas` 值，服务端仅当该 key 的当前 CAS 值匹配时才执行写入，否则返回 `KEY_EEXISTS(0x02)`
- **GET with CAS**：GET 响应中包含当前 key 的 CAS 值，客户端在后续 SET 请求中携带此值实现条件更新
- **DELTA_BADVAL(0x06)**：INCREMENT/DECREMENT 操作中，当目标值非数字时返回此状态

## 4. 设计思想

### 4.1 消息与内容分离

借鉴 HTTP 的 `HttpMessage` + `HttpContent` 模式，将协议消息拆分为头部消息和内容块。对于小消息，使用 `Full*` 合并类型避免分块开销；对于大消息，分块传输避免内存峰值。

### 4.2 模板方法模式

`AbstractBinaryMemcacheDecoder` 和 `AbstractBinaryMemcacheEncoder` 定义了编解码骨架，请求与响应的差异由子类实现：

- `decodeHeader()`：解码请求头 vs 响应头
- `encodeHeader()`：编码请求头 vs 响应头
- `buildInvalidMessage()`：构建错误消息占位

### 4.3 零拷贝读取

解码器大量使用 `in.readRetainedSlice(length)` 读取 extras 和 key，避免内存复制，仅增加引用计数。

### 4.4 CombinedChannelDuplexHandler 组合

`BinaryMemcacheClientCodec` 和 `BinaryMemcacheServerCodec` 使用 `CombinedChannelDuplexHandler` 将编码器和解码器组合为单个 handler，同时保持各自的独立性。客户端 Codec 还增加了请求/响应计数器，在连接关闭时检测未响应的请求。

## 5. 模块交互

```
                    Pipeline
                       |
   +-------------------+-------------------+
   |                                       |
   v                                       v
BinaryMemcacheRequestEncoder      BinaryMemcacheResponseDecoder
   |                                       |
   v                                       v
AbstractMemcacheObjectEncoder     AbstractBinaryMemcacheDecoder
   |                                       |
   | encodeMessage()               decode() 状态机
   | encodeAndRetain()             READ_HEADER → READ_EXTRAS
   |                                       → READ_KEY → READ_CONTENT
   v                                       v
  ByteBuf                           MemcacheMessage + MemcacheContent
                                            |
                                            v
                                  BinaryMemcacheObjectAggregator
                                            |
                                            v
                                  FullBinaryMemcacheRequest/Response
```

## 6. 关键流程

### 6.1 编码流程（SET 请求）

```
1. 创建 DefaultBinaryMemcacheRequest，设置 opcode=SET(0x01)
2. 设置 key ByteBuf 和 extras ByteBuf（包含 flags 和 expiry）
3. 发送 request 到 pipeline
4. 发送 MemcacheContent（value body，可能分多块）
5. 最后一块为 LastMemcacheContent

编码器处理：
6. AbstractMemcacheObjectEncoder.encode() 识别 MemcacheMessage
7. 调用 BinaryMemcacheRequestEncoder.encodeHeader() 写入 24 字节头部
8. 写入 extras 和 key
9. 后续 MemcacheContent 块直接透传 ByteBuf
```

### 6.2 解码流程（响应）

```
1. BinaryMemcacheResponseDecoder 接收 ByteBuf
2. READ_HEADER：读取 24 字节，解析 magic(0x81)、opcode、keyLength、
   extrasLength、dataType、status、totalBodyLength、opaque、cas
3. READ_EXTRAS：读取 extrasLength 字节 → setExtras(readRetainedSlice)
4. READ_KEY：读取 keyLength 字节 → setKey(readRetainedSlice)
5. 输出 BinaryMemcacheResponse 到 pipeline
6. READ_CONTENT：计算 valueLength，按 chunkSize 分块读取
7. 中间块 → DefaultMemcacheContent
8. 最后一块 → DefaultLastMemcacheContent
9. 全部读完后 resetDecoder()，回到 READ_HEADER
```

### 6.3 客户端 Codec 的请求响应对账

`BinaryMemcacheClientCodec` 内部的 `Encoder` 在发送 `LastMemcacheContent` 时递增计数器，`Decoder` 在收到 `LastMemcacheContent` 时递减。连接关闭时若计数器不为零，抛出 `PrematureChannelClosureException`。

## 7. 学习要点

1. **二进制协议帧结构**：24 字节固定头部 + 可变长度 extras + key + value body，理解每个字段的含义和编码顺序
2. **状态机解码器**：`AbstractBinaryMemcacheDecoder` 是学习 Netty `ByteToMessageDecoder` 状态机模式的典型范例
3. **消息与内容分离模式**：`MemcacheMessage` + `MemcacheContent` + `FullMemcacheMessage` 三层结构，与 HTTP 的 `HttpMessage` + `HttpContent` + `FullHttpRequest` 模式一致
4. **模板方法模式**：`AbstractBinaryMemcacheEncoder/Decoder` 的抽象方法设计，请求和响应共享编解码骨架
5. **CAS 机制**：8 字节 `cas` 字段实现乐观并发控制，是 Memcached 区别于简单 KV 存储的关键特性
6. **CombinedChannelDuplexHandler**：组合编解码器的 Netty 惯用模式，保持 handler 独立性的同时减少 pipeline 编排复杂度
7. **Quiet 操作码**：带 `Q` 后缀的变体操作码用于批量操作场景，减少不必要的响应开销
