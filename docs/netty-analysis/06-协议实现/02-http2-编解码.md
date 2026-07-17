# Netty HTTP/2 协议实现深度分析：帧、流、HPACK、连接管理

> 基于 Netty 4.2 源码 (`codec-http2` 模块)，聚焦帧格式编解码、流状态机、HPACK 头部压缩以及单连接多流复用机制。

---

## 一、概述

HTTP/2 (RFC 7540 / RFC 9113) 的核心创新在于：在单个 TCP 连接上通过**帧（Frame）**实现多条**流（Stream）**的并发复用，并通过 **HPACK** (RFC 7541) 头部压缩大幅降低冗余传输。Netty 的 `codec-http2` 模块对上述三大机制均有完整实现。

模块核心职责：

- **帧编解码**：将 HTTP/2 二进制帧格式与 Java 对象（`Http2Frame` 体系）双向转换
- **流管理**：维护流状态机 (IDLE -> OPEN -> HALF_CLOSED -> CLOSED)，管理流的生命周期
- **HPACK**：实现静态表 + 动态表的头部压缩/解压，支持 Huffman 编码
- **连接管理**：处理连接前言、SETTINGS 协商、GOAWAY 优雅关闭、流控窗口

---

## 二、整体架构

```
┌─────────────────────────────────────────────────────────────────┐
│                    Application Layer                             │
│   Http2DataFrame / Http2HeadersFrame / Http2ResetFrame ...      │
├─────────────────────────────────────────────────────────────────┤
│                    Http2MultiplexHandler                         │
│   为每条 HTTP/2 流创建独立的 Http2StreamChannel (子 Channel)      │
├─────────────────────────────────────────────────────────────────┤
│                    Http2FrameCodec                               │
│   Http2Frame <-> Java Object 双向转换层                          │
│   ├─ FrameListener (入站：帧 -> Http2Frame 对象)                │
│   └─ write()      (出站：Http2Frame -> 二进制帧)                │
├─────────────────────────────────────────────────────────────────┤
│                    Http2ConnectionHandler                        │
│   连接生命周期管理：Preface、SETTINGS、GOAWAY、错误处理           │
│   ├─ PrefaceDecoder  (连接前言阶段)                              │
│   └─ FrameDecoder    (常规帧解码阶段)                            │
├─────────────────────────────────────────────────────────────────┤
│                    Encoder / Decoder 组合层                      │
│   ┌──────────────────────┐  ┌──────────────────────┐           │
│   │ Http2ConnectionEncoder│  │ Http2ConnectionDecoder│           │
│   │ └─ DefaultHttp2Frame  │  │ └─ DefaultHttp2Frame  │          │
│   │    Writer             │  │    Reader             │          │
│   │ └─ HpackEncoder       │  │ └─ HpackDecoder       │          │
│   └──────────────────────┘  └──────────────────────┘           │
├─────────────────────────────────────────────────────────────────┤
│                    Http2Connection                               │
│   连接状态核心：流表、端点信息、流控控制器、GOAWAY 状态           │
│   ├─ Endpoint (local)  ─ 奇数流 ID                              │
│   └─ Endpoint (remote) ─ 偶数流 ID                              │
└─────────────────────────────────────────────────────────────────┘
```

**关键类分层关系**：

| 层级 | 类名 | 职责 |
|------|------|------|
| 用户 API | `Http2FrameCodec` | 将 HTTP/2 帧映射为 `Http2Frame` Java 对象，支持 Channel 读写语义 |
| 连接管理 | `Http2ConnectionHandler` | 处理连接前言、帧解码分发、GOAWAY/RST_STREAM 错误处理 |
| 帧写入 | `DefaultHttp2FrameWriter` | 将各类 `Http2Frame` 编码为二进制格式写入 `ByteBuf` |
| 帧读取 | `DefaultHttp2FrameReader` | 解析二进制帧头 + 载荷，分发到 `Http2FrameListener` |
| 头部压缩 | `HpackEncoder` / `HpackDecoder` | HPACK 头部编码/解码，管理动态表 |
| 流管理 | `Http2Connection` / `Http2Stream` | 流状态机、流表、端点管理 |
| 多路复用 | `Http2MultiplexHandler` | 每条流对应一个子 Channel，实现 Channel 级别的流复用 |

---

## 三、核心类深度分析

### 3.1 Http2FrameCodec —— 帧与对象的桥梁

`Http2FrameCodec` 继承自 `Http2ConnectionHandler`，是 Netty HTTP/2 的核心编解码入口。

**入站路径**（收到二进制帧 -> 产生 Java 对象）：

```
TCP ByteBuf
    → Http2ConnectionHandler.decode()
    → PrefaceDecoder / FrameDecoder.decode()
    → DefaultHttp2FrameReader.readFrame()
    → DefaultHttp2FrameReader.processPayloadState()
    → FrameListener (Http2FrameListener 回调)
    → Http2FrameCodec.onHttp2Frame()
    → ctx.fireChannelRead(Http2Frame)
```

**出站路径**（写入 Java 对象 -> 编码为二进制帧）：

```
Application writes Http2Frame
    → Http2FrameCodec.write()
    → encoder().writeData() / writeHeaders() / writeRstStream() ...
    → DefaultHttp2FrameWriter 写入 ByteBuf
    → ctx.write(ByteBuf)
```

核心源码中 `write()` 方法对每种帧类型做了分派：

```java
// Http2FrameCodec.write() 核心分派逻辑
if (msg instanceof Http2DataFrame) {
    encoder().writeData(ctx, dataFrame.stream().id(), dataFrame.content(),
            dataFrame.padding(), dataFrame.isEndStream(), promise);
} else if (msg instanceof Http2HeadersFrame) {
    writeHeadersFrame(ctx, (Http2HeadersFrame) msg, promise);
} else if (msg instanceof Http2ResetFrame) {
    encoder().writeRstStream(ctx, rstFrame.stream().id(), rstFrame.errorCode(), promise);
} else if (msg instanceof Http2PingFrame) {
    encoder().writePing(ctx, frame.ack(), frame.content(), promise);
} else if (msg instanceof Http2SettingsFrame) {
    encoder().writeSettings(ctx, ((Http2SettingsFrame) msg).settings(), promise);
} else if (msg instanceof Http2GoAwayFrame) {
    writeGoAwayFrame(ctx, (Http2GoAwayFrame) msg, promise);
}
// ...
```

**流初始化机制**：新建出站流时，先在 `frameStreamToInitializeMap` 中记录待初始化的 `DefaultHttp2FrameStream`，当 `Http2Connection.Listener.onStreamAdded()` 回调时完成属性绑定：

```java
// ConnectionListener.onStreamAdded()
public void onStreamAdded(Http2Stream stream) {
    DefaultHttp2FrameStream frameStream = frameStreamToInitializeMap.remove(stream.id());
    if (frameStream != null) {
        frameStream.setStreamAndProperty(streamKey, stream);
    }
}
```

### 3.2 Http2ConnectionHandler —— 连接生命周期管理

`Http2ConnectionHandler` 继承 `ByteToMessageDecoder` 并实现 `Http2LifecycleManager`，是 HTTP/2 连接的全权管理者。

**连接前言协议阶段**（两阶段解码器设计）：

```
handlerAdded() / channelActive()
    → PrefaceDecoder 创建
    → sendPrefaceIfNeeded()
        ├─ 客户端：写入 clientPrefaceString ("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
        └─ 写入初始 SETTINGS 帧

PrefaceDecoder.decode()
    → readClientPrefaceString()   // 服务端验证客户端前言
    → verifyFirstFrameIsSettings() // 验证第一帧为非 ACK 的 SETTINGS
    → 切换到 FrameDecoder

FrameDecoder.decode()
    → decoder.decodeFrame()       // 常规帧解码
```

**GOAWAY 优雅关闭**：

```java
// close() 方法中的优雅关闭逻辑
public void close(ChannelHandlerContext ctx, ChannelPromise promise) {
    // 如果尚未发送 GOAWAY，则发送一个
    ChannelFuture f = connection().goAwaySent()
        ? ctx.write(EMPTY_BUFFER)
        : goAway(ctx, null, ctx.newPromise());
    ctx.flush();
    doGracefulShutdown(ctx, f, promise);
}

// doGracefulShutdown：等待所有活跃流关闭后才真正关闭连接
private void doGracefulShutdown(ChannelHandlerContext ctx, ChannelFuture future, ChannelPromise promise) {
    if (isGracefulShutdownComplete()) {
        future.addListener(newClosingChannelFutureListener(ctx, promise));
    } else {
        // 有活跃流存在，记录 closeListener，等流全部关闭后再触发
        closeListener = listener;
    }
}
```

**错误分级处理**：

- **连接级错误** -> `onConnectionError()` -> 发送 GOAWAY 帧 -> 关闭连接
- **流级错误** -> `onStreamError()` -> 发送 RST_STREAM 帧 -> 关闭对应流
- **头部过大特殊处理** -> 发送 431 响应 (REQUEST_HEADER_FIELDS_TOO_LARGE)

### 3.3 Http2Connection / Http2Stream —— 流状态机

#### 流状态定义

```java
// Http2Stream.State 枚举
enum State {
    IDLE(false, false),               // 初始状态
    RESERVED_LOCAL(false, false),      // 保留（本地推送）
    RESERVED_REMOTE(false, false),     // 保留（远端推送）
    OPEN(true, true),                  // 双向开放
    HALF_CLOSED_LOCAL(false, true),    // 本地已关闭，远端仍可发
    HALF_CLOSED_REMOTE(true, false),   // 远端已关闭，本地仍可发
    CLOSED(false, false);              // 完全关闭
}
```

#### 流状态转换图

```
                          发送 HEADERS
    IDLE ─────────────────────────────────> OPEN
     │                                        │
     │ 发送 HEADERS (END_STREAM)               │ 收到 END_STREAM
     │                                        ▼
     │                               HALF_CLOSED_REMOTE
     │                                        │
     │                                        │ 收到 END_STREAM / RST_STREAM
     ▼                                        ▼
    HALF_CLOSED_LOCAL                       CLOSED
     │                                        ▲
     │ 收到 END_STREAM / RST_STREAM           │
     └────────────────────────────────────────┘
```

#### 端点（Endpoint）设计

`Http2Connection` 维护两个 `Endpoint`：`local()` 和 `remote()`，分别代表连接的两端。

```java
interface Endpoint<F extends Http2FlowController> {
    int incrementAndGetNextStreamId();   // 获取下一个流 ID（奇/偶数交替）
    boolean isValidStreamId(int streamId);
    boolean canOpenStream();             // 是否可新建流（受限于 maxActiveStreams）
    Http2Stream createStream(int streamId, boolean halfClosed);
    int numActiveStreams();
    int maxActiveStreams();              // SETTINGS_MAX_CONCURRENT_STREAMS
    F flowController();                 // 流控控制器
}
```

**关键设计点**：
- 客户端发起的流 ID 为奇数 (1, 3, 5, ...)
- 服务端发起的流 ID 为偶数 (2, 4, 6, ...)
- 流 ID 0 保留给连接级控制（connection stream）
- `maxActiveStreams` 限制并发流数量，由对端通过 SETTINGS 帧设置

#### 属性扩展机制

`Http2Connection.PropertyKey` 允许在流对象上附加应用层数据：

```java
// 在 Http2FrameCodec 中使用
streamKey = connection().newKey();
// 关联 Http2FrameStream 到 Http2Stream
stream.setProperty(streamKey, frameStream);
// 读取关联数据
Http2FrameStream frameStream = stream.getProperty(streamKey);
```

### 3.4 DefaultHttp2FrameWriter —— 帧编码

`DefaultHttp2FrameWriter` 实现了所有 HTTP/2 帧类型的编码逻辑。

#### HTTP/2 帧头格式 (9 字节)

```
+-----------------------------------------------+
|                 Length (24)                    |
+---------------+---------------+---------------+
|   Type (8)    |   Flags (8)   |
+-+-------------+---------------+---------------+
|R|                 Stream Identifier (31)       |
+=+=============================================+
|                   Frame Payload ...            |
+-----------------------------------------------+
```

#### 各帧类型编码示例

**DATA 帧** —— 传输请求/响应体：

```java
// writeData() 核心逻辑
// 1. 大数据分片：如果数据超过 maxFrameSize，拆分为多个 DATA 帧
if (remainingData > maxFrameSize) {
    writeFrameHeaderInternal(frameHeader, maxFrameSize, DATA, flags, streamId);
    ctx.write(frameHeader.retainedSlice(), promiseAggregator.newPromise());
    ctx.write(data.readRetainedSlice(maxFrameSize), promiseAggregator.newPromise());
}
// 2. 最后一帧设置 END_STREAM 标志
flags.endOfStream(endStream);
writeFrameHeaderInternal(frameHeader2, remainingData, DATA, flags, streamId);
```

**HEADERS 帧** —— 传输头部 + HPACK 编码后的 header block：

```java
// writeHeadersInternal() 核心逻辑
// 1. HPACK 编码整个头部块
headerBlock = ctx.alloc().buffer();
headersEncoder.encodeHeaders(streamId, headers, headerBlock);

// 2. 计算首帧可容纳的 fragment 大小
int maxFragmentLength = maxFrameSize - nonFragmentBytes;
ByteBuf fragment = headerBlock.readRetainedSlice(
    min(headerBlock.readableBytes(), maxFragmentLength));

// 3. 如果还有剩余，用 CONTINUATION 帧续传
if (!flags.endOfHeaders()) {
    writeContinuationFrames(ctx, streamId, headerBlock, promiseAggregator);
}
```

**CONTINUATION 帧** —— 头部块过大时的续传帧：

```java
// writeContinuationFrames() 循环写入
do {
    fragmentReadableBytes = min(headerBlock.readableBytes(), maxFrameSize);
    ByteBuf fragment = headerBlock.readRetainedSlice(fragmentReadableBytes);
    // 最后一帧设置 END_HEADERS 标志
    if (!headerBlock.isReadable()) {
        flags = flags.endOfHeaders(true);
    }
    writeFrameHeaderInternal(buf, fragmentReadableBytes, CONTINUATION, flags, streamId);
    ctx.write(buf, promiseAggregator.newPromise());
    ctx.write(fragment, promiseAggregator.newPromise());
} while (headerBlock.isReadable());
```

### 3.5 DefaultHttp2FrameReader —— 帧解码

`DefaultHttp2FrameReader` 实现了完整的帧解码逻辑，采用两阶段处理：

**阶段一：帧头解析** (`preProcessFrame`)：

```java
private boolean preProcessFrame(ByteBuf in) throws Http2Exception {
    if (in.readableBytes() < FRAME_HEADER_LENGTH) {  // 9 字节
        return false;
    }
    payloadLength = in.readUnsignedMedium();  // 3 字节长度
    frameType = in.readByte();                // 1 字节类型
    flags = new Http2Flags(in.readUnsignedByte());  // 1 字节标志
    streamId = readUnsignedInt(in);           // 4 字节流 ID (含 1 bit R)
    readingHeaders = false;
    return true;
}
```

**阶段二：按类型分派** (`processPayloadState`)：

```java
switch (frameType) {
    case DATA:       readDataFrame(ctx, in, listener); break;
    case HEADERS:    readHeadersFrame(ctx, in, listener); break;
    case PRIORITY:   readPriorityFrame(ctx, in, listener); break;
    case RST_STREAM: readRstStreamFrame(ctx, in, listener); break;
    case SETTINGS:   readSettingsFrame(ctx, in, listener); break;
    case PING:       readPingFrame(ctx, in.readLong(), listener); break;
    case GO_AWAY:    readGoAwayFrame(ctx, in, listener); break;
    case WINDOW_UPDATE: readWindowUpdateFrame(ctx, in, listener); break;
    case CONTINUATION:  readContinuationFrame(in, listener); break;
    default:         readUnknownFrame(ctx, in, listener); break;
}
```

**HEADERS + CONTINUATION 跨帧组装**：

HEADERS 帧的头部块可能跨越多个 CONTINUATION 帧。`HeadersContinuation` + `HeadersBlockBuilder` 实现了跨帧拼接：

```java
// HeadersBlockBuilder.addFragment()
final void addFragment(ByteBuf fragment, int len, ByteBufAllocator alloc, boolean endOfHeaders) {
    if (headerBlock == null) {
        if (endOfHeaders) {
            // 优化：只有一个 fragment 时直接使用，避免拷贝
            headerBlock = fragment.readRetainedSlice(len);
        } else {
            headerBlock = alloc.buffer(len).writeBytes(fragment, len);
        }
        return;
    }
    // 追加到已有 buffer
    if (headerBlock.isWritable(len)) {
        headerBlock.writeBytes(fragment, len);
    } else {
        ByteBuf buf = alloc.buffer(headerBlock.readableBytes() + len);
        buf.writeBytes(headerBlock).writeBytes(fragment, len);
        headerBlock.release();
        headerBlock = buf;
    }
}
```

### 3.6 HpackEncoder —— HPACK 头部编码

`HpackEncoder` 实现了 RFC 7541 定义的头部压缩算法。

#### 编码策略选择

```java
private void encodeHeader(ByteBuf out, CharSequence name, CharSequence value,
                           boolean sensitive, long headerSize) {
    // 1. 敏感头部：永远不索引 (NEVER indexed)
    if (sensitive) {
        encodeLiteral(out, name, value, IndexType.NEVER, nameIndex);
        return;
    }
    // 2. 动态表大小为 0：只使用静态表
    if (maxHeaderTableSize == 0) {
        int staticTableIndex = HpackStaticTable.getIndexInsensitive(name, value);
        if (staticTableIndex != NOT_FOUND) {
            encodeInteger(out, 0x80, 7, staticTableIndex);  // Indexed
        } else {
            encodeLiteral(out, name, value, IndexType.NONE, nameIndex);  // 不索引
        }
        return;
    }
    // 3. 头部字段大于动态表容量：不索引
    if (headerSize > maxHeaderTableSize) {
        encodeLiteral(out, name, value, IndexType.NONE, nameIndex);
        return;
    }
    // 4. 动态表中已存在完全匹配：直接索引
    NameValueEntry headerField = getEntryInsensitive(name, nameHash, value, valueHash);
    if (headerField != null) {
        encodeInteger(out, 0x80, 7, getIndexPlusOffset(headerField.counter));
    } else {
        // 5. 静态表完全匹配
        int staticTableIndex = HpackStaticTable.getIndexInsensitive(name, value);
        if (staticTableIndex != NOT_FOUND) {
            encodeInteger(out, 0x80, 7, staticTableIndex);
        } else {
            // 6. 新头部：增量索引 (INCREMENTAL indexing)
            ensureCapacity(headerSize);
            encodeAndAddEntries(out, name, nameHash, value, valueHash);
            size += headerSize;
        }
    }
}
```

#### 动态表管理

`HpackEncoder` 使用两个哈希表加速查找：
- `nameEntries[]`：按名称索引的哈希链表
- `nameValueEntries[]`：按名称+值索引的哈希链表，同时通过 `after` 指针构成 LRU 双向链表

```java
// 驱逐旧条目
private void remove() {
    NameValueEntry eldest = head.after;  // 最旧的条目
    removeNameValueEntry(eldest);
    removeNameEntryMatchingCounter(eldest.name, eldest.counter);
    head.after = eldest.after;
    eldest.unlink();
    size -= eldest.size();
}
```

#### Huffman 编码

当字符串长度超过阈值 (512) 且 Huffman 编码后更短时，自动使用 Huffman 编码：

```java
private void encodeStringLiteral(ByteBuf out, CharSequence string) {
    int huffmanLength;
    if (string.length() >= huffCodeThreshold
        && (huffmanLength = hpackHuffmanEncoder.getEncodedLength(string)) < string.length()) {
        encodeInteger(out, 0x80, 7, huffmanLength);  // Huffman 标志位 = 1
        hpackHuffmanEncoder.encode(out, string);
    } else {
        encodeInteger(out, 0x00, 7, string.length()); // Huffman 标志位 = 0
        out.writeCharSequence(string, CharsetUtil.ISO_8859_1);
    }
}
```

### 3.7 HpackDecoder —— HPACK 头部解码

`HpackDecoder` 实现头部块的解码，采用状态机驱动的解析方式。

#### 解码状态机

```
READ_HEADER_REPRESENTATION (初始/回到起点)
    ├── b < 0          → READ_INDEXED_HEADER       (索引头部)
    ├── b & 0x40 == 1  → INCREMENTAL 索引编码
    ├── b & 0x20 == 1  → Dynamic Table Size Update
    └── otherwise      → Literal (NONE / NEVER)

READ_INDEXED_HEADER         → 读取完整索引值 -> 输出头部
READ_INDEXED_HEADER_NAME    → 读取名称索引 -> 读值
READ_LITERAL_HEADER_NAME_*  → 读取字面量名称
READ_LITERAL_HEADER_VALUE_* → 读取字面量值 -> 输出头部
```

#### 安全性设计

`Http2HeadersSink` 延迟验证机制：先收集所有头部，最后统一验证，防止解码中途抛异常导致动态表状态损坏：

```java
void finish() throws Http2Exception {
    if (exceededMaxLength) {
        headerListSizeExceeded(streamId, maxHeaderListSize, true);
    } else if (validationException != null) {
        throw validationException;
    }
}
```

头部验证规则：
- 伪头部 (`:method`, `:path` 等) 必须出现在常规头部之前
- 请求伪头部和响应伪头部不可混用
- 禁止出现 `Connection` 等逐跳头部
- `TE` 头部只允许值为 `trailers`

### 3.8 Http2MultiplexHandler —— 子 Channel 多路复用

`Http2MultiplexHandler` 是实现 HTTP/2 流复用的核心，它将每条 HTTP/2 流映射为一个独立的 `Http2StreamChannel`（子 Channel）。

#### 子 Channel 创建

```java
// 当流状态变为 OPEN / HALF_CLOSED 时，创建子 Channel
case OPEN:
case HALF_CLOSED_REMOTE:
    if (stream.attachment != null) {
        break;  // 已创建，忽略
    }
    final AbstractHttp2StreamChannel ch;
    if (stream.id() == HTTP_UPGRADE_STREAM_ID && !isServer(ctx)) {
        ch = new Http2MultiplexHandlerStreamChannel(stream, upgradeStreamHandler);
        ch.closeOutbound();
    } else {
        ch = new Http2MultiplexHandlerStreamChannel(stream, inboundStreamHandler);
    }
    // 注册到 EventLoop
    ctx.channel().eventLoop().register(ch);
    break;
```

#### 帧分发到子 Channel

```java
public void channelRead(ChannelHandlerContext ctx, Object msg) {
    if (msg instanceof Http2StreamFrame) {
        Http2StreamFrame streamFrame = (Http2StreamFrame) msg;
        AbstractHttp2StreamChannel channel = (AbstractHttp2StreamChannel)
            ((DefaultHttp2FrameStream) streamFrame.stream()).attachment;

        if (msg instanceof Http2ResetFrame || msg instanceof Http2PriorityFrame) {
            // 非流控帧通过 userEvent 传播（不被 read() 抑制）
            channel.pipeline().fireUserEventTriggered(msg);
        } else {
            // 流控帧通过 channelRead 传播
            channel.fireChildRead(streamFrame);
        }
        return;
    }
    // 连接级事件（如 GOAWAY）向下传播
    ctx.fireChannelRead(msg);
}
```

#### ReadComplete 批处理优化

```java
// 避免每个子 Channel 都单独 flush，统一在 channelReadComplete 中批量处理
public void channelReadComplete(ChannelHandlerContext ctx) {
    processPendingReadCompleteQueue();
    ctx.fireChannelReadComplete();
}

private void processPendingReadCompleteQueue() {
    parentReadInProgress = true;
    AbstractHttp2StreamChannel childChannel = readCompletePendingQueue.poll();
    if (childChannel != null) {
        try {
            do {
                childChannel.fireChildReadComplete();
                childChannel = readCompletePendingQueue.poll();
            } while (childChannel != null);
        } finally {
            parentReadInProgress = false;
            readCompletePendingQueue.clear();
            ctx.flush();  // 统一 flush，优化系统调用
        }
    }
}
```

---

## 四、HTTP/2 帧类型全景

| 帧类型 | 值 | 流 ID | 核心作用 | 关键标志 |
|--------|-----|-------|---------|---------|
| DATA | 0x0 | 非零 | 传输请求/响应体 | END_STREAM, PADDED |
| HEADERS | 0x1 | 非零 | 传输头部 (含 HPACK 编码) | END_STREAM, END_HEADERS, PADDED, PRIORITY |
| PRIORITY | 0x2 | 非零 | 设置流优先级 | - |
| RST_STREAM | 0x3 | 非零 | 终止单条流 | - |
| SETTINGS | 0x4 | 0 | 协商连接参数 | ACK |
| PUSH_PROMISE | 0x5 | 非零 | 服务端推送 | END_HEADERS, PADDED |
| PING | 0x6 | 0 | 连接活性检测 | ACK |
| GOAWAY | 0x7 | 0 | 优雅关闭连接 | - |
| WINDOW_UPDATE | 0x8 | 0/非零 | 流量控制窗口更新 | - |
| CONTINUATION | 0x9 | 非零 | HEADERS 续传 | END_HEADERS |

---

## 五、关键流程详解

### 5.1 连接建立流程

```
客户端                                        服务端
  │                                              │
  │──── TCP 连接建立 ────────────────────────────>│
  │                                              │
  │──── Client Preface ("PRI * HTTP/2.0...") ───>│
  │──── SETTINGS (初始参数) ─────────────────────>│
  │                                              │
  │<──── SETTINGS (初始参数) ─────────────────────│
  │                                              │
  │──── SETTINGS ACK ───────────────────────────>│
  │<──── SETTINGS ACK ───────────────────────────│
  │                                              │
  │         连接就绪，可开始发送 HEADERS          │
```

Netty 实现中的关键代码路径：
1. `PrefaceDecoder.sendPrefaceIfNeeded()`：客户端写入 `connectionPrefaceBuf()` + 初始 SETTINGS
2. `PrefaceDecoder.verifyFirstFrameIsSettings()`：服务端验证第一帧必须是非 ACK 的 SETTINGS
3. 连接建立完成后切换到 `FrameDecoder` 进入常规帧处理

### 5.2 请求-响应流程

```
客户端                                        服务端
  │                                              │
  │──── HEADERS (stream=1, :method=GET) ────────>│
  │──── DATA (stream=1, END_STREAM) ────────────>│
  │                                              │
  │<──── HEADERS (stream=1, :status=200) ────────│
  │<──── DATA (stream=1, body, END_STREAM) ──────│
  │                                              │
```

在 Netty 中：
- **客户端**：通过 `Http2StreamChannel` 写入 `Http2HeadersFrame` -> `Http2FrameCodec.writeHeadersFrame()` -> `encoder().writeHeaders()` -> `HpackEncoder` 编码
- **服务端**：`DefaultHttp2FrameReader.readHeadersFrame()` -> `HpackDecoder.decode()` -> `FrameListener.onHeadersRead()` -> `Http2FrameCodec.onHttp2Frame()` -> 子 Channel `channelRead`

### 5.3 流量控制

HTTP/2 的流控基于**信用机制**：接收方通过 WINDOW_UPDATE 帧告知发送方可发送的字节数。

```
发送方                                      接收方
  │                                           │
  │── DATA (16KB) ───────────────────────────>│ 窗口 -16KB
  │── DATA (16KB) ───────────────────────────>│ 窗口 -16KB
  │                                           │
  │<── WINDOW_UPDATE (+32KB) ─────────────────│ 窗口 +32KB
  │                                           │
```

Netty 中 `Http2FrameCodec` 自动管理流控窗口：
- **入站**：`onDataRead()` 返回 0（不自动消费），由应用层通过 `Http2WindowUpdateFrame` 显式消费
- **出站**：通过 `Http2RemoteFlowController` 管理，根据窗口大小决定何时发送数据

---

## 六、设计思想

### 6.1 编解码分离

`Http2ConnectionEncoder` 和 `Http2ConnectionDecoder` 作为独立接口，可以灵活组合不同的帧读写器和 HPACK 编解码器：

```
Http2ConnectionEncoder
    ├─ Http2FrameWriter (帧格式化)
    ├─ HpackEncoder (头部压缩)
    ├─ Http2RemoteFlowController (出站流控)
    └─ Http2LifecycleManager (生命周期回调)

Http2ConnectionDecoder
    ├─ Http2FrameReader (帧解析)
    ├─ HpackDecoder (头部解压)
    ├─ Http2LocalFlowController (入站流控)
    └─ Http2LifecycleManager (生命周期回调)
```

### 6.2 子 Channel 抽象

`Http2MultiplexHandler` 将每条 HTTP/2 流映射为 Netty 的 `Channel`，使得：
- 每条流可以独立设置 Pipeline
- 每条流可以独立进行流控 (可暂停/恢复读取)
- 每条流的生命周期独立管理
- 复用 Netty 已有的 ChannelHandler 体系

### 6.3 防御性编程

- `verifyFrameState()` 在解码前验证帧格式合法性
- `verifyNotProcessingHeaders()` 防止在 HEADERS 处理中穿插其他帧
- `verifyAssociatedWithAStream()` 确保帧关联到有效流
- 头部大小限制防止 OOM 攻击
- `maxSmallContinuationFrames` 防止 CONTINUATION 帧洪泛攻击（ENHANCE_YOUR_CALM）

### 6.4 引用计数安全

HTTP/2 帧中的 `ByteBuf` 需要正确的引用计数管理：
- 入站帧在 `retain()` 后传递给下游
- 出站帧的 `ByteBuf` 所有权转移给编码器
- 错误路径中确保释放已分配的缓冲区

---

## 七、模块交互图

```
┌───────────────┐    write(Http2Frame)    ┌───────────────────┐
│  Application  │ ───────────────────────>│  Http2FrameCodec   │
│  (子Channel)  │ <───────────────────────│  (编解码)          │
│               │    fireChannelRead      │                   │
└───────────────┘    (Http2Frame)         └───────┬───────────┘
                                                  │
                                    ┌─────────────┴──────────────┐
                                    ▼                            ▼
                         ┌──────────────────┐       ┌──────────────────┐
                         │ HpackEncoder     │       │ HpackDecoder     │
                         │ (头部压缩)       │       │ (头部解压)       │
                         └────────┬─────────┘       └────────┬─────────┘
                                  │                          │
                                  ▼                          ▼
                         ┌──────────────────┐       ┌──────────────────┐
                         │DefaultHttp2Frame │       │DefaultHttp2Frame │
                         │Writer            │       │Reader            │
                         │ (帧编码)         │       │ (帧解码)         │
                         └────────┬─────────┘       └────────┬─────────┘
                                  │                          │
                                  ▼                          ▼
                         ┌──────────────────────────────────────────┐
                         │          TCP ByteBuf (9字节帧头+载荷)      │
                         └──────────────────────────────────────────┘
```

---

## 八、学习要点

1. **帧格式是基础**：9 字节帧头 (Length + Type + Flags + Stream ID) 是所有帧的共同前缀，理解它就理解了 HTTP/2 的二进制编码基础

2. **流状态机是核心**：IDLE -> OPEN -> HALF_CLOSED -> CLOSED 的转换规则直接决定了连接行为，`Http2Stream.State` 枚举用两个布尔值 (`localSideOpen`, `remoteSideOpen`) 精确描述每个状态

3. **HPACK 是性能关键**：静态表 (61 个预定义头部) + 动态表 (LRU 淘汰) + Huffman 编码的三层优化，使 HTTP/2 头部传输效率远超 HTTP/1.1

4. **子 Channel 是创新**：`Http2MultiplexHandler` 将每条流映射为 `Channel`，让开发者像使用普通 TCP 连接一样使用 HTTP/2 流，这是 Netty HTTP/2 API 的最大亮点

5. **流控不可忽视**：HTTP/2 的两级流控 (连接级 + 流级) 需要应用层正确处理 WINDOW_UPDATE 帧，否则会导致数据传输停滞

6. **CONTINUATION 帧陷阱**：HEADERS 帧的头部块可能跨越多个 CONTINUATION 帧，解码器必须在收到 END_HEADERS 之前保持状态（`HeadersContinuation`）

7. **错误分级处理**：连接级错误发送 GOAWAY 关闭整个连接，流级错误发送 RST_STREAM 只关闭单条流，这个区分对系统稳定性至关重要
