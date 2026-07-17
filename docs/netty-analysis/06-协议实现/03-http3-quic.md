# HTTP/3 + QUIC 协议实现

> 基于 Netty 4.2 源码 (`codec-http3` 和 `codec-classes-quic` 模块)，聚焦 QUIC 多路复用、0-RTT 连接建立、HTTP/3 帧格式以及 QPACK 头部压缩机制。

---

## 一、概述

### HTTP/3 + QUIC 解决什么问题

HTTP/3 (RFC 9114) 是 HTTP 协议的第三个版本，运行在 QUIC (RFC 9000) 传输协议之上，而非 TCP。这解决了 HTTP/2 在 TCP 上面临的**队头阻塞 (Head-of-Line Blocking)** 问题：

| 挑战 | HTTP/2 的问题 | HTTP/3 + QUIC 的解决方案 |
|------|-------------|------------------------|
| TCP 队头阻塞 | 单个 TCP 连接上，一个丢包阻塞所有流 | QUIC 在 UDP 上实现独立流，丢包只影响对应流 |
| 连接建立延迟 | TCP + TLS 需要 2-3 个 RTT | QUIC 1-RTT 握手，支持 0-RTT 恢复 |
| 连接迁移 | TCP 连接绑定四元组，IP 变化即断开 | QUIC 使用 Connection ID，支持无缝迁移 |
| 头部压缩 | HPACK 依赖 TCP 有序交付 | QPACK 解耦，允许乱序到达的头部块 |
| 握手安全性 | TLS 可选 | QUIC 内置 TLS 1.3，强制加密 |

### 在 Netty 整体架构中的位置

Netty 的 HTTP/3 实现分布在两个模块中：

```
┌─────────────────────────────────────────────────────────────────┐
│                      codec-http3                                 │
│   HTTP/3 帧编解码、QPACK、连接管理                               │
│   ├── Http3ConnectionHandler (连接生命周期)                      │
│   ├── Http3ServerConnectionHandler (服务端)                      │
│   ├── Http3ClientConnectionHandler (客户端)                      │
│   ├── Http3FrameCodec (帧编解码)                                 │
│   ├── QpackEncoder / QpackDecoder (头部压缩)                     │
│   └── Http3ControlStream*Handler (控制流处理)                    │
├─────────────────────────────────────────────────────────────────┤
│                      codec-classes-quic                          │
│   QUIC 传输层抽象                                                │
│   ├── QuicChannel (QUIC 连接，类似 SocketChannel)               │
│   ├── QuicStreamChannel (QUIC 流，类似子 Channel)                │
│   ├── QuicStreamType (UNIDIRECTIONAL / BIDIRECTIONAL)           │
│   └── QuicChannelBootstrap / QuicStreamChannelBootstrap         │
├─────────────────────────────────────────────────────────────────┤
│                      codec-native-quic                           │
│   QUIC 原生实现（基于 Quiche —— Cloudflare 的 Rust QUIC 库）     │
└─────────────────────────────────────────────────────────────────┘
```

**与 HTTP/2 的关键区别**：

| 特性 | HTTP/2 (Netty) | HTTP/3 (Netty) |
|------|---------------|----------------|
| 传输层 | TCP (`Channel`) | QUIC (`QuicChannel`) |
| 流实现 | `Http2MultiplexHandler` 创建子 Channel | QUIC 原生流 (`QuicStreamChannel`) |
| 头部压缩 | HPACK (`HpackEncoder`/`HpackDecoder`) | QPACK (`QpackEncoder`/`QpackDecoder`) |
| 帧格式 | 9 字节帧头 (length + type + flags + stream) | 变长整数帧头 (type + length) |
| 控制流 | 共用连接上的 SETTINGS/PING 等帧 | 独立的单向控制流 |
| 流复用 | TCP 上的逻辑流 | UDP 上的原生独立流 |

---

## 二、整体架构

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Application Layer                            │
│   Http3HeadersFrame / Http3DataFrame / Http3SettingsFrame ...       │
├─────────────────────────────────────────────────────────────────────┤
│                         Http3ConnectionHandler                       │
│   HTTP/3 连接管理：控制流创建、QPACK 初始化、流分类                   │
│   ├─ Http3ServerConnectionHandler (服务端)                           │
│   └─ Http3ClientConnectionHandler (客户端)                           │
├─────────────────────────────────────────────────────────────────────┤
│                         Http3FrameCodec                              │
│   HTTP/3 帧编解码：Type + Length + Payload (可变长整数)               │
│   ├─ QpackEncoder (QPACK 头部编码)                                  │
│   └─ QpackDecoder (QPACK 头部解码)                                  │
├─────────────────────────────────────────────────────────────────────┤
│                         QuicChannel                                  │
│   QUIC 连接通道：管理所有流、TLS 握手、拥塞控制                      │
│   ├─ QuicStreamChannel (双向流)                                      │
│   └─ QuicStreamChannel (单向流)                                      │
├─────────────────────────────────────────────────────────────────────┤
│                         Quiche (Rust JNI)                            │
│   QUIC 协议引擎：帧处理、加密、丢包恢复、流控                        │
├─────────────────────────────────────────────────────────────────────┤
│                         UDP DatagramChannel                          │
│   UDP 传输层                                                          │
└─────────────────────────────────────────────────────────────────────┘
```

**关键类分层关系**：

| 层级 | 类名 | 职责 |
|------|------|------|
| 连接管理 | `Http3ConnectionHandler` | HTTP/3 连接生命周期：创建控制流、初始化 QPACK、流分类 |
| 服务端连接 | `Http3ServerConnectionHandler` | 服务端特定逻辑：请求流处理、单向流初始化 |
| 帧编解码 | `Http3FrameCodec` | HTTP/3 帧的编解码，使用 QUIC 可变长整数格式 |
| QUIC 连接 | `QuicChannel` | QUIC 连接通道，管理所有流和 TLS 状态 |
| QUIC 流 | `QuicStreamChannel` | 单条 QUIC 流，支持双向/单向、流控、优先级 |
| 头部压缩 | `QpackEncoder` / `QpackDecoder` | QPACK 头部压缩/解压，支持动态表 |
| 控制流 | `Http3ControlStreamInboundHandler` / `Http3ControlStreamOutboundHandler` | 处理 SETTINGS、GOAWAY 等控制帧 |

---

## 三、核心类深度分析

### 3.1 QuicChannel -- QUIC 连接通道

`QuicChannel` 是 QUIC 连接的 Netty Channel 抽象，对应一个 QUIC 连接（而非单条流）。

**核心 API 设计**：

```java
public interface QuicChannel extends Channel {
    // 获取 TLS 引擎（QUIC 内置 TLS 1.3）
    @Nullable SSLEngine sslEngine();

    // 查询对端允许创建的流数量
    long peerAllowedStreams(QuicStreamType type);

    // 查询连接是否因空闲超时而关闭
    boolean isTimedOut();

    // 获取对端传输参数
    @Nullable QuicTransportParameters peerTransportParameters();

    // 创建新流
    Future<QuicStreamChannel> createStream(QuicStreamType type, @Nullable ChannelHandler handler);

    // 应用层关闭（带错误码和原因）
    ChannelFuture close(boolean applicationClose, int error, ByteBuf reason);

    // 收集连接统计信息
    Future<QuicConnectionStats> collectStats();

    // 创建客户端 QuicChannel
    static QuicChannelBootstrap newBootstrap(Channel channel);
}
```

**QUIC 连接与 TCP 连接的关键差异**：

| 特性 | TCP SocketChannel | QUIC QuicChannel |
|------|------------------|-----------------|
| 数据传输 | 单一字节流 | 多条独立流 |
| 流创建 | 不适用 | `createStream()` 动态创建 |
| 加密 | TLS 可选 | 内置 TLS 1.3，强制加密 |
| 连接关闭 | FIN/RST | 应用关闭 / 传输关闭两种语义 |
| 连接迁移 | 不支持 | Connection ID 机制支持 |
| 统计信息 | 有限 | `collectStats()` 丰富统计 |

### 3.2 QuicStreamChannel -- QUIC 流通道

`QuicStreamChannel` 继承自 `DuplexChannel`，表示一条 QUIC 流。每条流都有自己的流控和生命周期。

**流类型**：

```java
public enum QuicStreamType {
    UNIDIRECTIONAL,  // 单向流：只能单向发送数据
    BIDIRECTIONAL    // 双向流：两端都可以发送数据
}
```

QUIC 流 ID 使用 62 位整数（最低 2 位编码类型和发起方）：

```
位 0: 发起方 (0=客户端, 1=服务端)
位 1: 流类型 (0=双向, 1=单向)
位 2-61: 流序号

客户端双向流: 0x00, 0x04, 0x08, ...
服务端双向流: 0x01, 0x05, 0x09, ...
客户端单向流: 0x02, 0x06, 0x0a, ...
服务端单向流: 0x03, 0x07, 0x0b, ...
```

**流关闭语义**：

QUIC 流支持精细的关闭控制，与 TCP 的全双工关闭不同：

```java
// 正常关闭：发送 FIN
channel.shutdownOutput();

// 异常关闭：发送 RESET_STREAM，携带错误码
channel.shutdownOutput(errorCode, promise);

// 停止接收：发送 STOP_SENDING，通知对端停止发送
channel.shutdownInput(errorCode, promise);

// 同时关闭输入输出
channel.shutdown(errorCode, promise);
```

**核心 API**：

```java
public interface QuicStreamChannel extends DuplexChannel {
    long streamId();                    // 获取流 ID
    QuicStreamType type();              // 流类型
    boolean isLocalCreated();           // 是否由本地创建

    // 关闭输入（发送 STOP_SENDING）
    ChannelFuture shutdownInput(int error, ChannelPromise promise);
    // 关闭输出（发送 RESET_STREAM 或 FIN）
    ChannelFuture shutdownOutput(int error, ChannelPromise promise);
    // 更新流优先级
    ChannelFuture updatePriority(QuicStreamPriority priority, ChannelPromise promise);
}
```

### 3.3 Http3ConnectionHandler -- 连接生命周期

`Http3ConnectionHandler` 是 HTTP/3 连接管理的抽象基类，负责：

1. **初始化 QPACK 属性**（连接级别，所有流共享）
2. **创建控制流**（第一条创建的流）
3. **分发新流到子类处理**

**handlerAdded() 和 channelActive()**：

```java
@Override
public void handlerAdded(ChannelHandlerContext ctx) {
    QuicChannel channel = (QuicChannel) ctx.channel();
    // 初始化 QPACK 属性（连接级别，流之间共享）
    Http3.setQpackAttributes(channel, new QpackAttributes(channel, disableQpackDynamicTable));
    if (ctx.channel().isActive()) {
        createControlStreamIfNeeded(ctx);
    }
}

@Override
public void channelActive(ChannelHandlerContext ctx) {
    createControlStreamIfNeeded(ctx);
    ctx.fireChannelActive();
}
```

**控制流创建**：

```java
private void createControlStreamIfNeeded(ChannelHandlerContext ctx) {
    QuicChannel channel = (QuicChannel) ctx.channel();
    // 创建单向控制流，发送 SETTINGS 帧
    channel.createStream(QuicStreamType.UNIDIRECTIONAL, remoteControlStreamHandler)
            .addListener(f -> {
                if (!f.isSuccess()) {
                    ctx.fireExceptionCaught(new Http3Exception(
                        Http3ErrorCode.H3_STREAM_CREATION_ERROR, "Unable to open control stream"));
                    ctx.close();
                } else {
                    Http3.setLocalControlStream(channel, (QuicStreamChannel) f.getNow());
                }
            });
}
```

**新流分发**：

```java
@Override
public void channelRead(ChannelHandlerContext ctx, Object msg) {
    if (msg instanceof QuicStreamChannel) {
        QuicStreamChannel channel = (QuicStreamChannel) msg;
        switch (channel.type()) {
            case BIDIRECTIONAL:
                initBidirectionalStream(ctx, channel);   // 请求流
                break;
            case UNIDIRECTIONAL:
                initUnidirectionalStream(ctx, channel);  // 控制流/QPACK流/Push流
                break;
        }
    }
    ctx.fireChannelRead(msg);
}
```

### 3.4 Http3ServerConnectionHandler -- 服务端流初始化

`Http3ServerConnectionHandler` 继承自 `Http3ConnectionHandler`，为服务端的每种流类型配置 Pipeline。

**双向流（请求流）初始化**：

```java
@Override
void initBidirectionalStream(ChannelHandlerContext ctx, QuicStreamChannel streamChannel) {
    ChannelPipeline pipeline = streamChannel.pipeline();
    // 状态验证器
    Http3RequestStreamEncodeStateValidator encodeStateValidator = new Http3RequestStreamEncodeStateValidator();
    Http3RequestStreamDecodeStateValidator decodeStateValidator = new Http3RequestStreamDecodeStateValidator();
    // 帧编解码器
    pipeline.addLast(newCodec(encodeStateValidator, decodeStateValidator));
    // 状态校验
    pipeline.addLast(encodeStateValidator);
    pipeline.addLast(decodeStateValidator);
    // 请求流验证
    pipeline.addLast(newRequestStreamValidationHandler(streamChannel, encodeStateValidator, decodeStateValidator));
    // 用户提供的业务 Handler
    pipeline.addLast(requestStreamHandler);
}
```

**单向流初始化**：

```java
@Override
void initUnidirectionalStream(ChannelHandlerContext ctx, QuicStreamChannel streamChannel) {
    streamChannel.pipeline().addLast(
        new Http3UnidirectionalStreamInboundServerHandler(
            codecFactory,
            nonStandardSettingsValidator,
            localControlStreamHandler,       // 处理对端控制流
            remoteControlStreamHandler,      // 本地控制流
            unknownInboundStreamHandlerFactory,
            () -> new QpackEncoderHandler(maxTableCapacity, qpackDecoder),
            () -> new QpackDecoderHandler(qpackEncoder)));
}
```

HTTP/3 中的单向流通过流类型标识符区分：

| 流类型 ID | 用途 | 说明 |
|-----------|------|------|
| 0x00 | 控制流 | 传输 SETTINGS、GOAWAY 等控制帧 |
| 0x01 | Push 流 | 服务端推送 |
| 0x02 | QPACK 编码器流 | 动态表更新指令 |
| 0x03 | QPACK 解码器流 | 确认和流取消指令 |

### 3.5 Http3FrameCodec -- 帧编解码核心

`Http3FrameCodec` 是 HTTP/3 帧编解码的核心类，同时实现了入站解码（`ByteToMessageDecoder`）和出站编码（`ChannelOutboundHandler`）。

**HTTP/3 帧格式**：

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                       Type (i)                            ...
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                      Length (i)                           ...
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                       Frame Payload (*)                    ...
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

(i): 可变长整数 (Variable-Length Integer, 1/2/4/8 字节)
(*): 可变长度
```

与 HTTP/2 的 9 字节固定帧头不同，HTTP/3 使用 QUIC 的可变长整数编码，帧类型和长度字段更紧凑：

```
高 2 位为 00: 1 字节编码 (0-63)
高 2 位为 01: 2 字节编码 (0-16383)
高 2 位为 10: 4 字节编码 (0-1073741823)
高 2 位为 11: 8 字节编码 (0-4611686018427387903)
```

**HTTP/3 帧类型常量**：

```java
// Http3CodecUtils.java
static final int HTTP3_DATA_FRAME_TYPE          = 0x0;   // 数据帧
static final int HTTP3_HEADERS_FRAME_TYPE        = 0x1;   // 头部帧
static final int HTTP3_CANCEL_PUSH_FRAME_TYPE    = 0x3;   // 取消推送
static final int HTTP3_SETTINGS_FRAME_TYPE       = 0x4;   // 设置帧
static final int HTTP3_PUSH_PROMISE_FRAME_TYPE   = 0x5;   // 推送承诺
static final int HTTP3_GO_AWAY_FRAME_TYPE        = 0x7;   // GoAway
static final int HTTP3_MAX_PUSH_ID_FRAME_TYPE    = 0xd;   // 最大推送 ID
```

**解码流程**：

```java
@Override
protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
    // 1. 读取帧类型（可变长整数）
    if (type == -1) {
        int typeLen = numBytesForVariableLengthInteger(in.getByte(in.readerIndex()));
        long localType = readVariableLengthInteger(in, typeLen);
        // 校验：拒绝 HTTP/2 保留类型
        if (Http3CodecUtils.isReservedHttp2FrameType(localType)) {
            connectionError(ctx, Http3ErrorCode.H3_FRAME_UNEXPECTED, ...);
            return;
        }
        // 校验：类型对当前流是否合法
        validator.validate(localType, firstFrame);
        type = localType;
        firstFrame = false;
    }

    // 2. 读取载荷长度（可变长整数）
    if (payLoadLength == -1) {
        int payloadLen = numBytesForVariableLengthInteger(in.getByte(in.readerIndex()));
        payLoadLength = (int) readVariableLengthInteger(in, payloadLen);
    }

    // 3. 按类型解码帧
    int read = decodeFrame(ctx, type, payLoadLength, in, out);
    if (read >= 0) {
        if (read == payLoadLength) {
            type = -1;         // 重置状态，准备下一帧
            payLoadLength = -1;
        } else {
            payLoadLength -= read;  // 帧数据还未读完
        }
    }
}
```

**帧类型分发**：

```java
private int decodeFrame(ChannelHandlerContext ctx, int type, int payLoadLength, ByteBuf in, List<Object> out) {
    switch (type) {
        case 0x0:  // DATA
            int length = Math.min(in.readableBytes(), payLoadLength);
            out.add(new DefaultHttp3DataFrame(in.readRetainedSlice(length)));
            return length;

        case 0x1:  // HEADERS
            // QPACK 解码（可能需要等待动态表更新）
            if (!qpackAttributes.dynamicTableDisabled() && !qpackAttributes.decoderStreamAvailable()) {
                readResumptionListener.suspended();  // 暂停读取
                return 0;
            }
            Http3HeadersFrame headersFrame = new DefaultHttp3HeadersFrame();
            if (decodeHeaders(ctx, headersFrame.headers(), in, payLoadLength, ...)) {
                out.add(headersFrame);
                return payLoadLength;
            }
            return -1;  // 解码未完成

        case 0x4:  // SETTINGS
            Http3SettingsFrame settingsFrame = decodeSettings(ctx, in, payLoadLength);
            if (settingsFrame != null) out.add(settingsFrame);
            return payLoadLength;

        case 0x7:  // GO_AWAY
            out.add(new DefaultHttp3GoAwayFrame(readVariableLengthInteger(in, idLen)));
            return payLoadLength;

        default:
            if (!Http3CodecUtils.isReservedFrameType(longType)) {
                return skipBytes(in, payLoadLength);  // 未知类型，跳过
            }
            return decodeUnknownFrame(in, payLoadLength, out);  // 保留类型
    }
}
```

**出站编码 -- "先预留空间再回填"技巧**：

HTTP/3 帧编码使用一种巧妙的编码策略：先在 Buffer 前部预留 16 字节空间，写入载荷后，再回填 type 和 length 字段：

```java
private static <T extends Http3Frame> void writeDynamicFrame(
        ChannelHandlerContext ctx, long type, T frame,
        BiFunction<T, ByteBuf, Boolean> writer, ChannelPromise promise) {
    ByteBuf out = ctx.alloc().directBuffer();
    int payloadStartIndex = out.writerIndex() + 16;  // 预留 16 字节给 type + length
    out.writerIndex(payloadStartIndex);

    if (writer.apply(frame, out)) {  // 写入载荷（如 QPACK 编码的头部）
        int payloadLength = out.writerIndex() - payloadStartIndex;

        // 回填 length 字段
        int len = numBytesForVariableLengthInteger(payloadLength);
        out.writerIndex(payloadStartIndex - len);
        writeVariableLengthInteger(out, payloadLength, len);

        // 回填 type 字段
        int typeLength = numBytesForVariableLengthInteger(type);
        out.writerIndex(payloadStartIndex - len - typeLength);
        writeVariableLengthInteger(out, type, typeLength);

        // 调整 Buffer 索引
        out.setIndex(payloadStartIndex - len - typeLength, out.writerIndex());
        ctx.write(out, promise);
    } else {
        out.release();  // 编码失败，释放 Buffer
    }
}
```

**数据帧编码（零拷贝）**：

```java
private static void writeDataFrame(ChannelHandlerContext ctx, Http3DataFrame frame, ChannelPromise promise) {
    ByteBuf out = ctx.alloc().directBuffer(16);
    writeVariableLengthInteger(out, frame.type());                  // 写入类型 0x0
    writeVariableLengthInteger(out, frame.content().readableBytes());  // 写入长度
    ByteBuf content = frame.content().retain();
    // 零拷贝组合：帧头和载荷不需要合并
    ctx.write(Unpooled.wrappedUnmodifiableBuffer(out, content), promise);
}
```

### 3.6 QPACK 头部压缩

HTTP/3 使用 QPACK (RFC 9204) 替代 HTTP/2 的 HPACK。两者核心区别在于：

| 特性 | HPACK (HTTP/2) | QPACK (HTTP/3) |
|------|---------------|----------------|
| 流依赖 | 依赖 TCP 有序交付 | 解耦，允许乱序 |
| 动态表更新 | 在同一连接上同步 | 通过独立编码器/解码器流异步 |
| 阻塞处理 | 不需要 | 支持阻塞流等待动态表更新 |
| 编码策略 | 绝对索引 | 相对索引 + 后缀 |

**QPACK 流架构**：

```
请求流 (双向):
    [HEADERS帧: QPACK编码的头部块] → [DATA帧: 载荷]

QPACK 编码器流 (单向, 0x02):
    [动态表插入指令] → [容量更新指令]

QPACK 解码器流 (单向, 0x03):
    [确认指令] → [流取消指令]
```

**QPACK 编解码流程**：

```
编码端                              解码端
  |                                   |
  |── Encoder Stream ────────────────>|  动态表指令
  |   (INSERT, SET_CAPACITY 等)       |
  |                                   |
  |── Request Stream ────────────────>|  编码后的头部块
  |   (HEADERS 帧)                    |  (可能引用动态表条目)
  |                                   |
  |<── Decoder Stream ────────────────|  确认指令
  |   (INSERT_COUNT_INCREMENT 等)     |
```

**QpackAttributes -- 连接级共享状态**：

`QpackAttributes` 存储在 `QuicChannel` 的属性中，被同一条连接上的所有流共享：

```java
// Http3ConnectionHandler.handlerAdded()
Http3.setQpackAttributes(channel, new QpackAttributes(channel, disableQpackDynamicTable));

// Http3FrameCodec.handlerAdded()
qpackAttributes = Http3.getQpackAttributes(ctx.channel().parent());
```

**读取恢复机制**：

QPACK 解码可能需要等待动态表更新，`Http3FrameCodec` 使用 `ReadResumptionListener` 实现暂停/恢复：

```java
private static final class ReadResumptionListener
        implements Runnable, GenericFutureListener<Future<? super QuicStreamChannel>> {
    private static final int STATE_SUSPENDED = 0b1000_0000;           // 已暂停
    private static final int STATE_READ_PENDING = 0b0100_0000;        // 有待处理的 read()
    private static final int STATE_READ_COMPLETE_PENDING = 0b0010_0000;  // 有待处理的 readComplete()

    // 当 HEADERS 帧引用的动态表条目尚未到达时暂停
    void suspended() { setState(STATE_SUSPENDED); }

    // 当 QPACK 解码器流就绪时恢复
    @Override
    public void run() { resume(); }

    private void resume() {
        unsetState(STATE_SUSPENDED);
        codec.channelRead(ctx, Unpooled.EMPTY_BUFFER);  // 触发继续解码
        if (hasState(STATE_READ_COMPLETE_PENDING)) {
            codec.channelReadComplete(ctx);
        }
        if (hasState(STATE_READ_PENDING)) {
            codec.read(ctx);
        }
    }
}
```

同样，写入也可能需要等待 QPACK 编码器流就绪，`WriteResumptionListener` 使用 `PendingWriteQueue` 缓存待写入消息。

### 3.7 控制流管理

**入站控制流处理** (`Http3ControlStreamInboundHandler`)：

```java
void channelRead(ChannelHandlerContext ctx, Http3ControlStreamFrame frame) {
    // 1. 第一帧必须是 SETTINGS
    if (!firstFrameRead && !isSettingsFrame) {
        connectionError(ctx, H3_MISSING_SETTINGS, "Missing settings frame.");
        return;
    }
    // 2. 不允许收到第二个 SETTINGS
    if (firstFrameRead && isSettingsFrame) {
        connectionError(ctx, H3_FRAME_UNEXPECTED, "Second settings frame received.");
        return;
    }
    firstFrameRead = true;

    // 3. 处理各类控制帧
    if (isSettingsFrame) {
        handleHttp3SettingsFrame(ctx, settingsFrame);      // 初始化 QPACK 流
    } else if (frame instanceof Http3GoAwayFrame) {
        handleHttp3GoAwayFrame(ctx, goAwayFrame);           // 验证 GOAWAY ID
    }
}
```

**出站控制流处理** (`Http3ControlStreamOutboundHandler`)：

```java
public void channelActive(ChannelHandlerContext ctx) {
    // 1. 写入流类型标识 (0x00)
    ByteBuf buffer = ctx.alloc().buffer(8);
    writeVariableLengthInteger(buffer, HTTP3_CONTROL_STREAM_TYPE);
    ctx.write(buffer);
    // 2. 添加编解码器
    ctx.pipeline().addFirst(codec);
    // 3. 发送本地 SETTINGS
    closeOnFailure(ctx.writeAndFlush(localSettings));
    localSettings = null;
    ctx.fireChannelActive();
}
```

---

## 四、HTTP/3 帧类型全景

| 帧类型 | 值 | 流类型 | 核心作用 |
|--------|-----|-------|---------|
| DATA | 0x0 | 请求流 | 传输请求/响应体 |
| HEADERS | 0x1 | 请求流 | 传输头部 (QPACK 编码) |
| CANCEL_PUSH | 0x3 | 控制流 | 取消服务端推送 |
| SETTINGS | 0x4 | 控制流 | 协商连接参数 |
| PUSH_PROMISE | 0x5 | 请求流 | 服务端推送预告 |
| GOAWAY | 0x7 | 控制流 | 优雅关闭连接 |
| MAX_PUSH_ID | 0xd | 控制流 | 设置最大推送 ID |

**HTTP/2 保留帧类型**（HTTP/3 中禁止使用，收到会触发 `H3_FRAME_UNEXPECTED` 错误）：

| 值 | HTTP/2 帧类型 | HTTP/3 处理 |
|----|-------------|------------|
| 0x2 | PRIORITY | 禁止，返回 `H3_FRAME_UNEXPECTED` |
| 0x6 | PING | 禁止，返回 `H3_FRAME_UNEXPECTED` |
| 0x8 | WINDOW_UPDATE | 禁止，返回 `H3_FRAME_UNEXPECTED` |
| 0x9 | CONTINUATION | 禁止，返回 `H3_FRAME_UNEXPECTED` |

---

## 五、关键流程详解

### 5.1 QUIC 连接建立（1-RTT）

```
客户端                                          服务端
  |                                               |
  |── Initial (ClientHello) ────────────────────>|
  |                                               |
  |<── Initial (ServerHello) ─────────────────────|
  |<── Handshake (EncryptedExtensions, etc.) ─────|
  |                                               |
  |── Handshake (Finished) ─────────────────────>|
  |── 1-RTT (HTTP/3 请求) ──────────────────────>|
  |                                               |
  |       连接就绪，可发送 HTTP/3 帧              |
```

**0-RTT 恢复**（缓存 TLS 会话后）：

```
客户端                                          服务端
  |                                               |
  |── Initial (ClientHello + 0-RTT 数据) ───────>|
  |   (可立即发送 HTTP/3 请求，无需等待响应)      |
  |                                               |
  |<── Initial (ServerHello) ─────────────────────|
  |<── 1-RTT (0-RTT 处理结果) ───────────────────|
  |                                               |
```

QUIC 内置 TLS 1.3 的 0-RTT 能力意味着客户端可以在第一个数据包中就携带 HTTP/3 请求，这是 TCP + TLS 无法实现的。

### 5.2 HTTP/3 连接建立

```
客户端                                            服务端
  |                                                  |
  |  ──── QUIC 握手 (1-RTT) ────>                    |
  |  <─── QUIC 握手响应 ────────                     |
  |                                                  |
  |  ──── 创建单向控制流 (stream_id=0x02) ────>       |
  |        [类型=0x00] [SETTINGS帧]                   |
  |                                                  |
  |  <─── 创建单向控制流 (stream_id=0x03) ────        |
  |        [类型=0x00] [SETTINGS帧]                   |
  |                                                  |
  |  ──── 创建单向流 (stream_id=0x06) ────>           |
  |        [类型=0x02] (QPACK编码器流)                 |
  |                                                  |
  |  <─── 创建单向流 (stream_id=0x07) ────            |
  |        [类型=0x02] (QPACK编码器流)                 |
  |                                                  |
  |  ──── 创建单向流 (stream_id=0x0a) ────>           |
  |        [类型=0x03] (QPACK解码器流)                 |
  |                                                  |
  |  <─── 创建单向流 (stream_id=0x0b) ────            |
  |        [类型=0x03] (QPACK解码器流)                 |
  |                                                  |
  |       HTTP/3 连接就绪                             |
```

Netty 实现中的关键代码路径：
1. `Http3ConnectionHandler.createControlStreamIfNeeded()` -- 创建本地控制流
2. `Http3ControlStreamOutboundHandler.channelActive()` -- 写入流类型 `0x00` + `SETTINGS` 帧
3. `Http3ControlStreamInboundHandler.channelRead()` -- 处理远端 SETTINGS，创建 QPACK 流

### 5.3 请求-响应流程

```
客户端                                            服务端
  |                                                  |
  |── 创建双向流 (BIDIRECTIONAL) ────────────────>   |
  |── HEADERS (:method=GET, :path=/) ────────────>   |
  |── DATA (END_STREAM) ────────────────────────>    |
  |                                                  |
  |<── HEADERS (:status=200) ────────────────────    |
  |<── DATA (response body) ─────────────────────    |
  |<── DATA (END_STREAM) ────────────────────────    |
  |                                                  |
```

### 5.4 GOAWAY 优雅关闭

```
服务端                                          客户端
  |                                               |
  |── Control Stream: GOAWAY (id=N) ────────────>|
  |                                               |
  |   客户端停止创建 id > N 的新流               |
  |   已有流继续处理直到完成                      |
  |                                               |
  |── Control Stream: GOAWAY (id=M, M<=N) ──────>|  可选：进一步缩小
  |                                               |
```

**GOAWAY ID 规则**：
- 服务端发送的 GOAWAY ID 必须是 4 的倍数（`id % 4 == 0`）
- 客户端发送的 GOAWAY ID 必须不是 4 的倍数
- 后续 GOAWAY ID 只能减小，不能增大

---

## 六、设计思想

### 6.1 QUIC 流复用替代 TCP 多路复用

HTTP/2 在单一 TCP 连接上模拟多流复用，TCP 层面的丢包仍会导致所有流阻塞。HTTP/3 使用 QUIC 原生流，每条流独立可靠传输：

```
HTTP/2:  [Stream1][Stream2][Stream3] → TCP → 网络
         (TCP 丢包影响所有流)

HTTP/3:  Stream1 → QUIC Packet1 → UDP → 网络
         Stream2 → QUIC Packet2 → UDP → 网络
         Stream3 → QUIC Packet3 → UDP → 网络
         (丢包只影响对应流)
```

### 6.2 Channel 抽象的一致性

Netty 在 HTTP/2 和 HTTP/3 中都使用子 Channel 抽象来表示流，但底层实现完全不同：

| 特性 | HTTP/2 子 Channel | HTTP/3 子 Channel |
|------|------------------|------------------|
| 父 Channel | TCP Channel | QuicChannel |
| 流创建 | HTTP/2 帧触发 | QUIC 协议原生 |
| 流控 | HTTP/2 WINDOW_UPDATE | QUIC 内置流控 |
| 关闭 | RST_STREAM 帧 | QUIC RESET_STREAM / STOP_SENDING |

### 6.3 控制流与数据流分离

HTTP/3 将控制信息（SETTINGS、GOAWAY）放在独立的单向控制流中，而非像 HTTP/2 那样在连接流（stream 0）上发送。这确保了控制帧不会被大量数据帧阻塞。

### 6.4 QPACK 异步解码

QPACK 的核心创新是解码可以延迟：当 HEADERS 帧引用的动态表条目尚未到达时，解码器可以暂停并等待 Encoder Stream 的指令。Netty 通过 `ReadResumptionListener` 和 `WriteResumptionListener` 实现了这一机制：

- **ReadResumptionListener**：HEADERS 帧解码时，如果 QPACK 动态表未就绪，暂停读取；当 decoder stream 可用后自动恢复
- **WriteResumptionListener**：HEADERS 帧编码时，如果 encoder stream 未就绪，消息入队；当 stream 可用后批量发送

### 6.5 帧类型的可扩展性

HTTP/3 的帧类型使用可变长整数编码，并设计了保留类型区间：

```java
// 保留帧类型区间: 0x21 到 0x3fffffffffffffff
static final long MIN_RESERVED_FRAME_TYPE = 0x1f * 1 + 0x21;
static final long MAX_RESERVED_FRAME_TYPE = 0x1f * (long) Integer.MAX_VALUE + 0x21;
```

接收端对未知帧类型必须跳过（而不是报错），这允许未来扩展新帧类型而无需协商。Netty 的 `Http3FrameCodec` 对未知类型执行 `skipBytes()`，对保留类型创建 `Http3UnknownFrame` 传递给上层。

### 6.6 错误处理策略

HTTP/3 的错误处理与 HTTP/2 有显著差异：

| 错误类型 | HTTP/2 处理 | HTTP/3 处理 |
|---------|------------|------------|
| 连接级错误 | GOAWAY 帧 | QUIC CONNECTION_CLOSE |
| 流级错误 | RST_STREAM 帧 | QUIC RESET_STREAM |
| 头部解码错误 | 连接级错误 | 流级错误 (H3_MESSAGE_ERROR) |
| 控制流关闭 | N/A | H3_CLOSED_CRITICAL_STREAM |

---

## 七、模块交互图

```
┌───────────────────────────────────────────────────────────────────────┐
│                        QuicChannel (父 Channel)                       │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐      │
│  │ StreamChannel   │  │ StreamChannel   │  │ StreamChannel   │      │
│  │ (Control 本地)  │  │ (Control 远端)  │  │ (Request 双向)  │      │
│  │                 │  │                 │  │                 │      │
│  │ OutboundHandler │  │ InboundHandler  │  │ FrameCodec      │      │
│  │ → Settings      │  │ ← Settings      │  │ ← Headers       │      │
│  │ → GoAway        │  │ ← GoAway        │  │ ← Data          │      │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘      │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐      │
│  │ StreamChannel   │  │ StreamChannel   │  │ StreamChannel   │      │
│  │ (QPACK Encoder) │  │ (QPACK Decoder) │  │ (Push 单向)     │      │
│  │                 │  │                 │  │                 │      │
│  │ QpackEncoder    │  │ QpackDecoder    │  │ PushHandler     │      │
│  │ Handler         │  │ Handler         │  │                 │      │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘      │
└───────────────────────────────────────────────────────────────────────┘
                                   │
                                   v
┌───────────────────────────────────────────────────────────────────────┐
│                    QuicheQuicChannel (JNI 绑定)                       │
│                    ├── QuicheQuicConnection                           │
│                    ├── Quiche (Rust 库)                               │
│                    └── UDP DatagramChannel                            │
└───────────────────────────────────────────────────────────────────────┘
```

---

## 八、学习要点

### 关键设计模式

1. **Channel 抽象**：QUIC 连接和流都抽象为 Netty Channel，复用 Pipeline/EventLoop/Handler 体系
2. **工厂模式**：`Http3FrameCodecFactory` 创建帧编解码器，`Http3Codec` 提供静态工厂方法
3. **状态机模式**：`Http3FrameCodec` 的解码器使用 type/payLoadLength 双状态机
4. **观察者模式**：`ReadResumptionListener` / `WriteResumptionListener` 监听 QPACK 流就绪事件
5. **策略模式**：`Http3FrameTypeValidator` 校验帧类型对当前流的合法性

### 性能优化技巧

1. **零拷贝编码**：数据帧使用 `Unpooled.wrappedUnmodifiableBuffer()` 组合帧头和载荷
2. **预留空间**：动态帧编码在 Buffer 前部预留 16 字节，避免额外的 Buffer 分配和复制
3. **直连 Buffer**：编码输出使用 `ctx.alloc().directBuffer()`，减少堆外复制
4. **异步 QPACK**：读写暂停/恢复机制避免阻塞事件循环，同时避免不必要的上下文切换
5. **可变长整数**：QUIC 的可变长整数编码 (1/2/4/8 字节) 比 HTTP/2 的固定 4 字节整数更紧凑

### 核心学习要点

1. **QUIC 流是原生概念**：与 HTTP/2 在 TCP 上模拟流不同，QUIC 流是协议原生支持的，每条流有独立的流控和错误处理，不存在 TCP 层面的队头阻塞

2. **可变长整数是基础**：HTTP/3 和 QUIC 统一使用可变长整数编码（1/2/4/8 字节），取代了 HTTP/2 的固定长度字段，`Http3CodecUtils` 中的 `readVariableLengthInteger()` / `writeVariableLengthInteger()` 是最基础的编解码工具

3. **控制流必须优先建立**：HTTP/3 要求控制流是第一批创建的流，SETTINGS 帧必须是控制流的第一帧，Netty 通过 `createControlStreamIfNeeded()` 和 `firstFrameRead` 标志确保这一约束

4. **QPACK 解码可以延迟**：与 HPACK 的同步解码不同，QPACK 允许解码器在等待动态表更新时暂停，Netty 通过 `ReadResumptionListener` 和 `WriteResumptionListener` 实现了异步暂停/恢复机制

5. **单向流类型由首字节标识**：HTTP/3 的单向流通过第一个可变长整数标识流类型（0x00=控制、0x01=推送、0x02=QPACK 编码器、0x03=QPACK 解码器）

6. **子 Channel 模式延续**：HTTP/3 沿用了 HTTP/2 的子 Channel 抽象（`QuicStreamChannel`），使得应用层代码可以像使用普通 Channel 一样处理每条流

7. **HTTP/2 帧类型被禁止**：HTTP/3 明确禁止使用 PRIORITY(0x2)、PING(0x6)、WINDOW_UPDATE(0x8)、CONTINUATION(0x9) 等 HTTP/2 帧类型，收到这些类型会触发 `H3_FRAME_UNEXPECTED` 错误

8. **QUIC 连接迁移**：QUIC 基于 Connection ID 而非 IP:Port 识别连接，支持网络切换时的无缝迁移，这是 HTTP/2 over TCP 无法实现的能力

---

## 参考资料

- [RFC 9000 - QUIC: A UDP-Based Multiplexed and Secure Transport](https://datatracker.ietf.org/doc/html/rfc9000)
- [RFC 9114 - HTTP/3](https://datatracker.ietf.org/doc/html/rfc9114)
- [RFC 9204 - QPACK: Field Compression for HTTP/3](https://datatracker.ietf.org/doc/html/rfc9204)
- Netty 源码: `codec-http3/`, `codec-classes-quic/`
- 关键文件: `Http3FrameCodec.java`, `Http3ConnectionHandler.java`, `Http3ServerConnectionHandler.java`, `QuicChannel.java`, `QuicStreamChannel.java`
