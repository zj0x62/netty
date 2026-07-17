# WebSocket 编解码

## 概述

### WebSocket 解决什么问题

WebSocket (RFC 6455) 是一种在单个 TCP 连接上提供**全双工通信**的协议，解决了 HTTP 轮询带来的高延迟和资源浪费问题。Netty 的 `codec-http` 模块对 WebSocket 协议进行了完整实现，核心挑战与解决方案如下：

| 挑战 | 具体问题 | Netty 的解决方案 |
|------|---------|-----------------|
| 协议升级 | WebSocket 连接通过 HTTP Upgrade 建立 | `WebSocketServerHandshaker` 完成握手流程，动态替换 Pipeline |
| 帧格式解析 | 二进制帧头包含 FIN/RSV/opcode/mask/payload 等多层字段 | `WebSocket08FrameDecoder` 基于有限状态机逐步解析 |
| 帧格式编码 | 需按协议规范组装帧头并处理 payload 掩码 | `WebSocket08FrameEncoder` 负责编码，支持零拷贝优化 |
| 掩码处理 | 客户端->服务端的帧必须掩码以防止缓存投毒 | 解码器中 `unmask()` 方法使用 long/int 批量 XOR 优化 |
| 分片消息 | 大消息可拆分为多个帧传输 | `fragmentedFramesCount` 跟踪分片状态，ContinuationFrame 承接 |
| 多版本兼容 | WebSocket 有 V00/V07/V08/V13 等多个版本 | `WebSocketServerHandshakerFactory` 根据 `Sec-WebSocket-Version` 头自动选择 |
| 安全防护 | 控制帧长度限制、保留 opcode 检查、UTF-8 验证 | `WebSocketDecoderConfig` 提供 `maxFramePayloadLength` 等多级保护 |

### 在 Netty 整体架构中的位置

WebSocket 编解码器在 HTTP 握手完成后替换 Pipeline 中的 HTTP 编解码器，实现协议切换：

```
握手阶段 Pipeline：
    HttpRequestDecoder / HttpServerCodec
        → WebSocketServerProtocolHandshakeHandler
            → WebSocketServerProtocolHandler

握手完成后 Pipeline：
    WebSocket08FrameDecoder (替换 HttpRequestDecoder)
        → Utf8FrameValidator (可选)
            → WebSocketServerProtocolHandler
                → WebSocket08FrameEncoder (在 HttpServerCodec 之前插入)
                    → 用户业务 Handler
```

---

## 架构图

### WebSocket 编解码器类继承体系

```
                          WebSocketFrame (抽象基类)
                              |
              +---------------+---------------+---------------+
              |               |               |               |
     TextWebSocketFrame  BinaryWebSocketFrame  CloseWebSocketFrame
              |               |               |
     PingWebSocketFrame  PongWebSocketFrame  ContinuationWebSocketFrame

  ─────────────────────────────────────────────────────────────────

  编码器:
    MessageToMessageEncoder<WebSocketFrame>
        └── WebSocket08FrameEncoder (implements WebSocketFrameEncoder)

  解码器:
    ByteToMessageDecoder
        └── WebSocket08FrameDecoder (implements WebSocketFrameDecoder)

  ─────────────────────────────────────────────────────────────────

  握手器:
    WebSocketServerHandshaker (抽象)
        ├── WebSocketServerHandshaker00  (V00, 无版本头)
        ├── WebSocketServerHandshaker07  (V07)
        ├── WebSocketServerHandshaker08  (V08)
        └── WebSocketServerHandshaker13  (V13, RFC 6455)

  ─────────────────────────────────────────────────────────────────

  Pipeline 管理:
    WebSocketProtocolHandler (抽象基类, MessageToMessageDecoder + ChannelOutboundHandler)
        ├── WebSocketServerProtocolHandler
        │       └── 通过 handlerAdded() 插入 WebSocketServerProtocolHandshakeHandler
        └── WebSocketClientProtocolHandler
                └── 通过 handlerAdded() 插入 WebSocketClientProtocolHandshakeHandler
```

### 核心类职责表

| 类名 | 职责 |
|------|------|
| `WebSocketServerProtocolHandler` | 服务端 WebSocket 协议总入口，管理握手、控制帧、Pipeline 动态切换 |
| `WebSocketServerProtocolHandshakeHandler` | 处理 HTTP Upgrade 请求，触发握手流程 |
| `WebSocketServerHandshaker` | 握手抽象基类，定义 `handshake()` / `newHandshakeResponse()` / `close()` |
| `WebSocketServerHandshaker13` | V13 (RFC 6455) 握手实现，使用 SHA-1 + GUID 计算 `Sec-WebSocket-Accept` |
| `WebSocketServerHandshakerFactory` | 根据 `Sec-WebSocket-Version` 头自动创建对应版本的 Handshaker |
| `WebSocket08FrameDecoder` | 基于状态机的帧解码器，处理 FIN/RSV/opcode/mask/payload |
| `WebSocket08FrameEncoder` | 帧编码器，支持掩码和零拷贝优化 |
| `WebSocketFrame` | 所有 WebSocket 帧的抽象基类，携带 `finalFragment` 和 `rsv` 属性 |
| `WebSocketProtocolHandler` | 处理 Ping/Pong 自动回复、Close 帧关闭、写关闭保护 |
| `WebSocketDecoderConfig` | 解码器配置：`maxFramePayloadLength`、`expectMaskedFrames`、`allowExtensions` 等 |

---

## 核心类深度分析

### 3.1 WebSocket08FrameDecoder -- 帧解码状态机

`WebSocket08FrameDecoder` 继承自 `ByteToMessageDecoder`，是 WebSocket 帧解码的核心。它实现了一个六状态有限状态机来逐步解析帧的各个字段。

**状态机流转**：

```
READING_FIRST ──→ READING_SECOND ──→ READING_SIZE ──→ MASKING_KEY ──→ PAYLOAD ──→ READING_FIRST
   (1 byte)         (1 byte)        (0/2/8 bytes)    (0/4 bytes)    (N bytes)      (下一帧)
     |                  |                |                |              |
   FIN+RSV          MASK+LEN1       扩展长度          掩码密钥       载荷数据
   +opcode
```

**第一个字节解析** (`READING_FIRST`)：

```java
byte b = in.readByte();
frameFinalFlag = (b & 0x80) != 0;   // bit 7: FIN 标志
frameRsv = (b & 0x70) >> 4;         // bit 6-4: RSV1/RSV2/RSV3 扩展位
frameOpcode = b & 0x0F;             // bit 3-0: opcode 操作码
```

WebSocket 帧第一个字节的位布局：

```
 7 6 5 4 3 2 1 0
+-+-+-+-+-------+
|F|R|R|R| opcode|
|I|S|S|S|  (4)  |
|N|V|V|V|       |
| |1|2|3|       |
+-+-+-+-+-------+
```

**第二个字节解析** (`READING_SECOND`)：

```java
b = in.readByte();
frameMasked = (b & 0x80) != 0;      // bit 7: MASK 标志
framePayloadLen1 = b & 0x7F;        // bit 6-0: 初始载荷长度
```

```
 7 6 5 4 3 2 1 0
+-------+-------+
| MASK  |  len  |
|  (1)  | (7)   |
+-------+-------+
```

**载荷长度解析** (`READING_SIZE`)：

| `framePayloadLen1` 值 | 含义 | 后续读取 |
|----------------------|------|---------|
| 0-125 | 直接就是实际长度 | 无 |
| 126 | 后续 2 字节为实际长度 (unsigned short) | 2 字节 |
| 127 | 后续 8 字节为实际长度 (long) | 8 字节 |

```java
if (framePayloadLen1 == 126) {
    framePayloadLength = in.readUnsignedShort();
    // 校验：必须 >= 126（最小编码原则）
} else if (framePayloadLen1 == 127) {
    framePayloadLength = in.readLong();
    // 校验：必须 >= 65536（最小编码原则），且非负
} else {
    framePayloadLength = framePayloadLen1;
}
```

**掩码解码** (`MASKING_KEY`)：

如果 `frameMasked` 为 true，读取 4 字节掩码密钥。载荷解码时使用 XOR 操作还原数据：

```java
private void unmask(ByteBuf frame) {
    int i = frame.readerIndex();
    int end = frame.writerIndex();
    long longMask = intMask & 0xFFFFFFFFL;
    longMask |= longMask << 32;

    // 8 字节批量 XOR（优化路径）
    for (int lim = end - 7; i < lim; i += 8) {
        frame.setLong(i, frame.getLong(i) ^ longMask);
    }
    // 4 字节 XOR
    if (i < end - 3) {
        frame.setInt(i, frame.getInt(i) ^ (int) longMask);
        i += 4;
    }
    // 逐字节处理剩余
    for (; i < end; i++) {
        frame.setByte(i, frame.getByte(i) ^ WebSocketUtil.byteAtIndex(intMask, maskOffset++ & 3));
    }
}
```

**帧类型分发** (`PAYLOAD`)：

| opcode | 值 | 帧类型 | 说明 |
|--------|-----|--------|------|
| 0x0 | 0 | `ContinuationWebSocketFrame` | 分片续帧 |
| 0x1 | 1 | `TextWebSocketFrame` | 文本数据帧 |
| 0x2 | 2 | `BinaryWebSocketFrame` | 二进制数据帧 |
| 0x8 | 8 | `CloseWebSocketFrame` | 关闭控制帧 |
| 0x9 | 9 | `PingWebSocketFrame` | Ping 控制帧 |
| 0xA | 10 | `PongWebSocketFrame` | Pong 控制帧 |

**协议违规校验**：

解码器在多个层级执行严格校验：

1. **RSV 位校验**：如果未协商扩展，RSV 必须为 0
2. **掩码校验**：服务端期望掩码帧，客户端期望非掩码帧
3. **控制帧约束**：控制帧不能分片、载荷不能超过 125 字节、不能使用保留 opcode
4. **数据帧分片状态**：收到 Continuation 前必须有初始帧，收到非 Continuation 时不能处于分片状态
5. **最小编码**：长度必须使用最小编码方式（如 125 以内不能用 126 编码）
6. **Close 帧校验**：载荷长度为 1 是协议违规；状态码必须有效；消息体必须是合法 UTF-8

### 3.2 WebSocket08FrameEncoder -- 帧编码

`WebSocket08FrameEncoder` 继承自 `MessageToMessageEncoder<WebSocketFrame>`，负责将 WebSocket 帧对象编码为二进制格式。

**编码流程**：

```java
protected void encode(ChannelHandlerContext ctx, WebSocketFrame msg, List<Object> out) {
    byte opcode = getOpCode(msg);
    int length = data.readableBytes();

    // 第一个字节: FIN + RSV + opcode
    int b0 = 0;
    if (msg.isFinalFragment()) b0 |= 1 << 7;
    b0 |= (msg.rsv() & 0x07) << 4;
    b0 |= opcode & 0x7F;

    // 根据长度选择编码方式
    if (length <= 125) {
        // 2 字节头 + 4 字节掩码（如果有）
    } else if (length <= 0xFFFF) {
        // 4 字节头（126 + 2字节长度）
    } else {
        // 10 字节头（127 + 8字节长度）
    }

    // 掩码处理（客户端 -> 服务端必须掩码）
    if (maskGenerator != null) {
        int mask = maskGenerator.nextMask();
        buf.writeInt(mask);
        // 批量 XOR 编码（与解码器对称的优化）
    }
}
```

**性能优化**：

- **GATHERING_WRITE_THRESHOLD (1024)**：非掩码消息小于阈值时合并为单个 Buffer 发送，减少系统调用
- **掩码为 0 时跳过 XOR**：如果随机掩码恰好为 0，直接复制数据
- **long/int 批量 XOR**：与解码器使用相同的 8/4 字节批量处理优化

### 3.3 WebSocketServerHandshaker -- 握手核心

`WebSocketServerHandshaker` 是握手的抽象基类，定义了完整的握手流程。

**handshake() 方法核心逻辑**：

```java
public final ChannelFuture handshake(Channel channel, FullHttpRequest req,
                                      HttpHeaders responseHeaders, final ChannelPromise promise) {
    // 1. 生成握手响应
    FullHttpResponse response = newHandshakeResponse(req, responseHeaders);

    // 2. 移除不再需要的 HTTP 处理器
    p.remove(HttpObjectAggregator.class);
    p.remove(HttpContentCompressor.class);

    // 3. 替换编解码器（关键：HTTP -> WebSocket 协议切换）
    if (使用 HttpServerCodec) {
        p.addBefore(ctx.name(), "wsencoder", newWebSocketEncoder());
        p.addBefore(ctx.name(), "wsdecoder", newWebsocketDecoder());
        encoderName = ctx.name();  // 记住旧编码器名称
    } else {
        p.replace(ctx.name(), "wsdecoder", newWebsocketDecoder());
        p.addBefore(encoderName, "wsencoder", newWebSocketEncoder());
    }

    // 4. 发送握手响应，成功后移除旧的 HTTP 编码器
    channel.writeAndFlush(response).addListener(future -> {
        if (future.isSuccess()) {
            p.remove(encoderName);  // 移除 HttpResponseEncoder / HttpServerCodec
            promise.setSuccess();
        }
    });
}
```

**WebSocketServerHandshaker13 的握手响应**：

V13 (RFC 6455) 握手使用 SHA-1 哈希计算 `Sec-WebSocket-Accept`：

```java
// 固定 GUID（RFC 6455 规定）
public static final String WEBSOCKET_13_ACCEPT_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

// 计算 Accept 值
String key = reqHeaders.get(HttpHeaderNames.SEC_WEBSOCKET_KEY);
MessageDigest digestSha1 = WebSocketUtil.sha1();
digestSha1.update(key.getBytes(StandardCharsets.US_ASCII));
digestSha1.update(GUID_BYTES);
String accept = WebSocketUtil.base64(digestSha1.digest());
```

握手请求/响应示例：

```
客户端请求:                          服务端响应:
GET /chat HTTP/1.1                  HTTP/1.1 101 Switching Protocols
Host: server.example.com            Upgrade: websocket
Upgrade: websocket                  Connection: Upgrade
Connection: Upgrade                 Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=
Sec-WebSocket-Key: dGhlIHN...       Sec-WebSocket-Protocol: chat
Sec-WebSocket-Protocol: chat, superchat
Sec-WebSocket-Version: 13
```

### 3.4 WebSocketServerHandshakerFactory -- 版本路由

`WebSocketServerHandshakerFactory` 根据客户端请求中的 `Sec-WebSocket-Version` 头自动选择对应版本的 Handshaker：

```java
private static WebSocketServerHandshaker resolveHandshaker0(...) {
    CharSequence version = req.headers().get(HttpHeaderNames.SEC_WEBSOCKET_VERSION);
    if (version != null) {
        if (version.equals(WebSocketVersion.V13.toHttpHeaderValue())) {
            return new WebSocketServerHandshaker13(...);  // RFC 6455
        } else if (version.equals(WebSocketVersion.V08.toHttpHeaderValue())) {
            return new WebSocketServerHandshaker08(...);  // HyBi-10
        } else if (version.equals(WebSocketVersion.V07.toHttpHeaderValue())) {
            return new WebSocketServerHandshaker07(...);  // HyBi-07
        } else {
            return null;  // 不支持的版本
        }
    } else {
        return new WebSocketServerHandshaker00(...);  // 无版本头，假设 V00
    }
}
```

如果版本不受支持，`sendUnsupportedVersionResponse()` 会返回 `426 Upgrade Required` 并在 `Sec-WebSocket-Version` 头中告知客户端支持的版本。

### 3.5 WebSocketServerProtocolHandler -- Pipeline 管理

`WebSocketServerProtocolHandler` 是服务端 WebSocket 的总入口，继承自 `WebSocketProtocolHandler`。

**handlerAdded() -- 动态插入处理器**：

```java
@Override
public void handlerAdded(ChannelHandlerContext ctx) {
    ChannelPipeline cp = ctx.pipeline();
    // 在自己之前插入握手处理器
    if (cp.get(WebSocketServerProtocolHandshakeHandler.class) == null) {
        cp.addBefore(ctx.name(), WebSocketServerProtocolHandshakeHandler.class.getName(),
                new WebSocketServerProtocolHandshakeHandler(serverConfig));
    }
    // 可选：在自己之前插入 UTF-8 验证器
    if (serverConfig.decoderConfig().withUTF8Validator() && cp.get(Utf8FrameValidator.class) == null) {
        cp.addBefore(ctx.name(), Utf8FrameValidator.class.getName(),
                new Utf8FrameValidator(serverConfig.decoderConfig().closeOnProtocolViolation()));
    }
}
```

**decode() -- 控制帧处理**：

```java
@Override
protected void decode(ChannelHandlerContext ctx, WebSocketFrame frame, List<Object> out) {
    if (serverConfig.handleCloseFrames() && frame instanceof CloseWebSocketFrame) {
        WebSocketServerHandshaker handshaker = getHandshaker(ctx.channel());
        if (handshaker != null) {
            frame.retain();
            ChannelPromise promise = ctx.newPromise();
            closeSent(promise);
            handshaker.close(ctx, (CloseWebSocketFrame) frame, promise);
        } else {
            ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
        }
        return;
    }
    super.decode(ctx, frame, out);  // 交给 WebSocketProtocolHandler 处理 Ping/Pong
}
```

### 3.6 WebSocketProtocolHandler -- 控制帧基类

`WebSocketProtocolHandler` 是 `WebSocketServerProtocolHandler` 和 `WebSocketClientProtocolHandler` 的共同基类，处理通用的控制帧逻辑。

**Ping/Pong 自动回复**：

```java
@Override
protected void decode(ChannelHandlerContext ctx, WebSocketFrame frame, List<Object> out) {
    if (frame instanceof PingWebSocketFrame) {
        frame.content().retain();
        ctx.writeAndFlush(new PongWebSocketFrame(frame.content()));  // 自动回复 Pong
        readIfNeeded(ctx);
        return;
    }
    if (frame instanceof PongWebSocketFrame && dropPongFrames) {
        readIfNeeded(ctx);  // 默认丢弃 Pong 帧
        return;
    }
    out.add(frame.retain());  // 其他帧传递给下一个 Handler
}
```

**关闭保护**：

```java
@Override
public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
    if (closeSent != null) {
        // 已发送 Close 帧后，拒绝所有后续写入
        ReferenceCountUtil.release(msg);
        promise.setFailure(new ClosedChannelException());
    } else if (msg instanceof CloseWebSocketFrame) {
        closeSent(promise.unvoid());
        ctx.write(msg).addListener(new PromiseNotifier<>(false, closeSent));
    } else {
        ctx.write(msg, promise);
    }
}
```

---

## 设计思想

### 4.1 Pipeline 动态编排

Netty WebSocket 实现最核心的设计是**握手完成后动态替换 Pipeline**。这不是简单的 handler 添加/删除，而是精确的协议切换：

1. **握手阶段**：Pipeline 中是 HTTP 编解码器（`HttpRequestDecoder` + `HttpResponseEncoder`）
2. **握手完成**：用 `WebSocket08FrameDecoder` 替换 `HttpRequestDecoder`，在 `HttpResponseEncoder` 之前插入 `WebSocket08FrameEncoder`
3. **响应发送成功**：移除 `HttpResponseEncoder`（因为不再需要 HTTP 编码）

这种设计使得 WebSocket 连接可以在同一个 TCP 连接上从 HTTP 无缝升级。

### 4.2 安全第一的设计

WebSocket 协议在设计时充分考虑了安全问题，Netty 的实现严格遵循：

- **掩码强制**：服务端默认要求客户端帧必须掩码（`expectMaskedFrames=true`），防止中间代理缓存投毒
- **最小编码**：拒绝非最小编码的长度字段，防止解析歧义
- **载荷限制**：`maxFramePayloadLength` 默认 65536，防止内存耗尽攻击
- **控制帧约束**：控制帧不能分片、载荷 <= 125 字节，减少攻击面
- **UTF-8 验证**：Text 帧和 Close 帧消息体必须是合法 UTF-8

### 4.3 版本兼容策略

通过 `WebSocketServerHandshakerFactory` 的工厂模式，Netty 支持多个 WebSocket 版本，同时保持向后兼容：

- **V13** (RFC 6455)：当前标准版本，使用 SHA-1 + GUID
- **V08** (HyBi-10)：旧版本兼容
- **V07** (HyBi-07)：旧版本兼容
- **V00** (Hixie-76)：最老版本，无 `Sec-WebSocket-Version` 头时的回退

对于不支持的版本，返回 `426 Upgrade Required` 并告知支持的版本号。

### 4.4 分片消息处理

WebSocket 支持将大消息拆分为多个帧：

```
[TextFrame(FIN=false)] → [ContinuationFrame(FIN=false)] → [ContinuationFrame(FIN=true)]
```

解码器通过 `fragmentedFramesCount` 计数器跟踪分片状态：

- 收到 Text/Binary 帧（非 FIN）：计数器 +1
- 收到 Continuation 帧（FIN=true）：计数器归零
- 在分片中收到非 Continuation 帧：协议违规
- 在非分片中收到 Continuation 帧：协议违规

---

## 模块交互

### 服务端 WebSocket 完整 Pipeline 交互

```
客户端                    服务端 Pipeline
  |                            |
  |--- HTTP Upgrade Request -->|
  |                            |  HttpRequestDecoder → 解码为 HttpRequest
  |                            |  → WebSocketServerProtocolHandshakeHandler
  |                            |      → WebSocketServerHandshakerFactory.resolveHandshaker()
  |                            |      → handshaker.handshake()
  |                            |          → newHandshakeResponse() (生成 101 响应)
  |                            |          → Pipeline 替换: HTTP → WebSocket
  |                            |          → writeAndFlush(101 响应)
  |<-- 101 Switching Protocols-|
  |                            |
  |--- WebSocket Frame ------->|
  |                            |  WebSocket08FrameDecoder → 解码为 WebSocketFrame
  |                            |  → Utf8FrameValidator (可选验证)
  |                            |  → WebSocketServerProtocolHandler
  |                            |      → Ping? 自动回复 Pong
  |                            |      → Close? 执行关闭握手
  |                            |      → Text/Binary? 传递给业务 Handler
  |                            |
  |<-- WebSocket Frame --------|
  |                            |  业务 Handler → WebSocket08FrameEncoder → 字节流
```

---

## 关键流程

### 流程一：服务端握手

```
1. 客户端发送 HTTP GET 请求，包含:
   - Upgrade: websocket
   - Connection: Upgrade
   - Sec-WebSocket-Key: <base64 随机值>
   - Sec-WebSocket-Version: 13

2. WebSocketServerProtocolHandshakeHandler.channelRead()
   ├── 检查 URI 是否匹配配置的 websocketPath
   ├── WebSocketServerHandshakerFactory.resolveHandshaker()
   │   └── 根据 Sec-WebSocket-Version 选择 Handshaker 版本
   ├── WebSocketServerProtocolHandler.setHandshaker() (保存到 Channel 属性)
   ├── 移除 WebSocketServerProtocolHandshakeHandler 自身
   └── handshaker.handshake()
       ├── newHandshakeResponse()
       │   ├── 验证: 必须是 GET 方法
       │   ├── 验证: Connection 头包含 "Upgrade"
       │   ├── 验证: Upgrade 头包含 "websocket"
       │   ├── 验证: Sec-WebSocket-Key 存在
       │   └── 计算: SHA1(key + GUID) → Base64 → Sec-WebSocket-Accept
       ├── Pipeline 替换操作
       │   ├── 移除 HttpObjectAggregator
       │   ├── 移除 HttpContentCompressor
       │   ├── 添加 wsdecoder (WebSocket08FrameDecoder)
       │   └── 添加 wsencoder (WebSocket08FrameEncoder)
       └── writeAndFlush(101 响应)
           └── 成功后移除 HTTP 编码器

3. 握手完成事件
   ├── ServerHandshakeStateEvent.HANDSHAKE_COMPLETE (兼容旧版)
   └── HandshakeComplete (新版，携带 requestUri/requestHeaders/selectedSubprotocol)
```

### 流程二：帧解码

```
WebSocket08FrameDecoder.decode():

1. READING_FIRST: 读取 1 字节
   ├── FIN (bit 7), RSV (bit 6-4), opcode (bit 3-0)
   └── 校验: 控制帧 opcode > 7

2. READING_SECOND: 读取 1 字节
   ├── MASK (bit 7), payloadLen1 (bit 6-0)
   ├── 校验: RSV 位与扩展协商
   ├── 校验: 掩码与期望匹配
   └── 校验: 控制帧不能分片、载荷 <= 125、opcode 有效

3. READING_SIZE: 根据 payloadLen1 读取扩展长度
   ├── 0-125: 直接使用
   ├── 126: 读取 2 字节 unsigned short
   └── 127: 读取 8 字节 long
   └── 校验: 最小编码原则、不超过 maxFramePayloadLength

4. MASKING_KEY: 如果 masked，读取 4 字节掩码

5. PAYLOAD:
   ├── 读取 payloadLength 字节数据
   ├── 如果 masked，执行 unmask() (XOR 解码)
   ├── 根据 opcode 创建对应帧对象
   └── 处理分片状态跟踪
```

### 流程三：帧编码

```
WebSocket08FrameEncoder.encode():

1. 确定 opcode (根据帧类型)
2. 组装第一个字节: FIN + RSV + opcode
3. 根据 payload 长度选择编码方式:
   ├── <= 125: 直接编码
   ├── <= 0xFFFF: 126 + 2 字节
   └── > 0xFFFF: 127 + 8 字节
4. 如果需要掩码:
   ├── 生成 4 字节随机掩码
   ├── 写入掩码到输出
   └── 批量 XOR 编码 payload
5. 输出: 如果 payload 小于阈值，合并为单个 Buffer
```

---

## 学习要点

### 关键设计模式

1. **工厂模式**：`WebSocketServerHandshakerFactory` 根据请求自动选择 Handshaker 版本
2. **模板方法**：`WebSocketServerHandshaker` 定义握手骨架，子类实现 `newHandshakeResponse()` / `newWebsocketDecoder()` / `newWebSocketEncoder()`
3. **状态机模式**：`WebSocket08FrameDecoder` 使用六状态有限状态机解析帧
4. **责任链模式**：Pipeline 中的 Handler 链式处理，握手完成后动态替换

### 性能优化技巧

1. **批量 XOR 解码**：使用 long (8字节) 和 int (4字节) 批量操作代替逐字节 XOR，显著提升掩码处理性能
2. **零拷贝编码**：小 payload 合并到帧头 Buffer，大 payload 使用 gathering write 分别发送
3. **掩码为 0 快捷路径**：随机掩码为 0 时跳过 XOR 操作
4. **最小编码校验**：拒绝非最小编码的长度字段，减少不必要的内存分配

### 与其他协议的对比

| 特性 | HTTP/1.1 | WebSocket | HTTP/2 |
|------|----------|-----------|--------|
| 通信模式 | 请求-响应 | 全双工 | 多路复用全双工 |
| 协议格式 | 文本 | 二进制帧 | 二进制帧 |
| 连接建立 | 直接 | HTTP Upgrade | 直接/TLS-ALPN |
| 头部压缩 | 无 | 无 | HPACK |
| 服务端推送 | 不支持 | 不支持 | 支持 |
| 流控 | 无 | 无 | 窗口机制 |

### 常见使用陷阱

1. **忘记处理握手完成事件**：应监听 `HandshakeComplete` 用户事件确认握手成功
2. **未配置 `maxFramePayloadLength`**：默认 65536，对于大消息场景需要调整
3. **掩码不匹配**：客户端必须掩码，服务端必须验证掩码
4. **Close 帧处理**：发送 Close 帧后不应再发送其他帧
5. **分片消息中的 Ping**：Ping 帧可以在分片消息中间插入，但不能自身分片

---

## 参考资料

- [RFC 6455 - The WebSocket Protocol](https://tools.ietf.org/html/rfc6455)
- Netty 源码: `codec-http/src/main/java/io/netty/handler/codec/http/websocketx/`
- 关键文件: `WebSocket08FrameDecoder.java`, `WebSocketServerHandshaker.java`, `WebSocketServerProtocolHandler.java`
