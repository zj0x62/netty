# Netty MQTT 协议实现深度分析

## 1. 概述

MQTT（Message Queuing Telemetry Transport）是一种轻量级的消息传输协议，专为低带宽、高延迟或不可靠的网络环境设计。Netty 的 `codec-mqtt` 模块提供了完整的 MQTT v3.1、v3.1.1 和 v5.0 协议编解码实现。

该模块的核心设计特点：

- **状态机驱动的解码器**：基于 `ReplayingDecoder`，通过三阶段状态机（固定头 -> 可变头 -> 负载）逐步解析消息
- **版本自适应**：根据 CONNECT 消息中的协议版本号，动态切换 v3.1/v3.1.1/v5.0 的编解码逻辑
- **丰富的消息类型体系**：15 种 MQTT 消息类型，每种都有对应的消息类和编解码路径
- **QoS 级别支持**：完整支持 AT_MOST_ONCE(0)、AT_LEAST_ONCE(1)、EXACTLY_ONCE(2) 三个服务质量级别

## 2. 架构图

```
┌─────────────────────────────────────────────────────────────────┐
│                    MQTT Codec 模块架构                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐    │
│  │ MqttMessage  │◄────│MqttFixedHeader│    │ MqttQoS      │    │
│  │  (基类)       │     │  messageType │     │ AT_MOST_ONCE │    │
│  │  fixedHeader │     │  isDup       │     │ AT_LEAST_ONCE│    │
│  │  variableHdr │     │  qosLevel    │     │ EXACTLY_ONCE │    │
│  │  payload     │     │  isRetain    │     │ FAILURE      │    │
│  │  decoderResult│    │  remainingLen│     └──────────────┘    │
│  └──────┬───────┘     └──────────────┘                         │
│         │                                                       │
│         ▼                                                       │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │              MqttMessageType (15种)                       │  │
│  │  CONNECT(1) CONNACK(2) PUBLISH(3) PUBACK(4) PUBREC(5)   │  │
│  │  PUBREL(6) PUBCOMP(7) SUBSCRIBE(8) SUBACK(9)            │  │
│  │  UNSUBSCRIBE(10) UNSUBACK(11) PINGREQ(12) PINGRESP(13)  │  │
│  │  DISCONNECT(14) AUTH(15)                                 │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                 │
│  ┌─────────────────────┐      ┌─────────────────────┐         │
│  │    MqttDecoder       │      │    MqttEncoder       │         │
│  │  (ReplayingDecoder)  │      │  (MessageToMessage)  │         │
│  │                      │      │                      │         │
│  │  READ_FIXED_HEADER ──┼──►───│  encodeConnect()     │         │
│  │  READ_VARIABLE_HEADER│      │  encodePublish()     │         │
│  │  READ_PAYLOAD        │      │  encodeSubscribe()   │         │
│  │  BAD_MESSAGE         │      │  encodeProperties()  │         │
│  └─────────────────────┘      └─────────────────────┘         │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │              MqttVersion (版本适配)                        │  │
│  │  MQTT_3_1  ("MQIsdp", level=3)                           │  │
│  │  MQTT_3_1_1("MQTT",   level=4)                           │  │
│  │  MQTT_5    ("MQTT",   level=5)                           │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

## 3. 核心类分析

### 3.1 MqttFixedHeader — 固定头

`MqttFixedHeader` 是所有 MQTT 消息的固定头部，对应 MQTT 协议规范中的 Fixed Header 部分。

```java
public final class MqttFixedHeader {
    private final MqttMessageType messageType;  // 消息类型 (4 bit)
    private final boolean isDup;                // 重发标志 (1 bit)
    private final MqttQoS qosLevel;            // 服务质量 (2 bit)
    private final boolean isRetain;             // 保留标志 (1 bit)
    private final int remainingLength;          // 剩余长度 (可变字节整数)
}
```

**关键设计点**：

- 固定头的第一个字节包含消息类型（高4位）和标志位（低4位）
- `remainingLength` 使用 MQTT 特有的可变字节整数编码（Variable Byte Integer），最多 4 个字节，最大值为 268,435,455
- 不同消息类型对标志位有不同的约束（如 PUBLISH 的所有标志位都有效，而 CONNECT 的标志位必须全为 0）

### 3.2 MqttDecoder — 解码器状态机

`MqttDecoder` 继承自 `ReplayingDecoder<DecoderState>`，是整个解码模块的核心。

```java
public final class MqttDecoder extends ReplayingDecoder<DecoderState> {
    enum DecoderState {
        READ_FIXED_HEADER,      // 阶段1: 读取固定头
        READ_VARIABLE_HEADER,   // 阶段2: 读取可变头
        READ_PAYLOAD,           // 阶段3: 读取负载
        BAD_MESSAGE,            // 异常状态: 丢弃所有数据
    }
}
```

**状态机流转过程**：

```
READ_FIXED_HEADER ──► READ_VARIABLE_HEADER ──► READ_PAYLOAD ──► (输出消息)
       │                      │                      │
       │ (解析失败)           │ (超出长度限制)        │ (解析失败)
       ▼                      ▼                      ▼
   BAD_MESSAGE ◄──────────────────────────────────────┘
       │
       ▼
   (持续丢弃数据直到断开连接)
```

**解码流程详解**：

1. **READ_FIXED_HEADER**：调用 `decodeFixedHeader()` 解析第一个字节，提取消息类型、QoS、DUP、Retain 标志，然后通过 `parseRemainingLength()` 解析可变长度编码的剩余长度字段

2. **READ_VARIABLE_HEADER**：根据消息类型分派到不同的解码方法：
   - `decodeConnectionVariableHeader()` — CONNECT 消息
   - `decodeConnAckVariableHeader()` — CONNACK 消息
   - `decodePublishVariableHeader()` — PUBLISH 消息
   - `decodeMessageIdAndPropertiesVariableHeader()` — SUBSCRIBE/SUBACK 等
   - `decodePubReplyMessage()` — PUBACK/PUBREC/PUBREL/PUBCOMP

3. **READ_PAYLOAD**：根据消息类型解码负载数据，如 CONNECT 的客户端标识符和遗嘱消息、SUBSCRIBE 的主题列表等

**长度保护机制**：

```java
// 在 READ_VARIABLE_HEADER 阶段检查消息总长度
if (bytesRemainingBeforeVariableHeader > maxBytesInMessage) {
    buffer.skipBytes(actualReadableBytes());
    throw new TooLongFrameException("message length exceeds " + maxBytesInMessage);
}
```

### 3.3 MqttEncoder — 编码器

`MqttEncoder` 是一个 `@Sharable` 的 `MessageToMessageEncoder`，采用单例模式。

```java
@ChannelHandler.Sharable
public final class MqttEncoder extends MessageToMessageEncoder<MqttMessage> {
    public static final MqttEncoder INSTANCE = new MqttEncoder();
}
```

**编码方法分派**：

```java
switch (message.fixedHeader().messageType()) {
    case CONNECT:   return encodeConnectMessage(ctx, (MqttConnectMessage) message);
    case CONNACK:   return encodeConnAckMessage(ctx, (MqttConnAckMessage) message);
    case PUBLISH:   return encodePublishMessage(ctx, (MqttPublishMessage) message);
    case SUBSCRIBE: return encodeSubscribeMessage(ctx, (MqttSubscribeMessage) message);
    // ... 其他消息类型
}
```

**编码策略**：

- 先计算所有字段的总长度，一次性分配足够大的 `ByteBuf`
- 使用 `writeVariableLengthInt()` 编码可变长度整数
- UTF-8 字符串编码有两种策略：`writeExactUTF8String()`（已知长度）和 `writeEagerUTF8String()`（需要先编码再写入长度）

### 3.4 MqttConnectVariableHeader — CONNECT 可变头

CONNECT 消息的可变头包含连接参数：

```java
public final class MqttConnectVariableHeader {
    private final String name;                // 协议名 ("MQIsdp" 或 "MQTT")
    private final int version;                // 协议级别 (3/4/5)
    private final boolean hasUserName;        // 用户名标志
    private final boolean hasPassword;        // 密码标志
    private final boolean isWillRetain;       // 遗嘱保留标志
    private final int willQos;                // 遗嘱 QoS
    private final boolean isWillFlag;         // 遗嘱标志
    private final boolean isCleanSession;     // 清理会话标志
    private final int keepAliveTimeSeconds;   // 保活时间（秒）
    private final MqttProperties properties;  // MQTT 5.0 属性
}
```

### 3.5 MqttVersion — 版本适配

```java
public enum MqttVersion {
    MQTT_3_1("MQIsdp", (byte) 3),
    MQTT_3_1_1("MQTT", (byte) 4),
    MQTT_5("MQTT", (byte) 5);
}
```

版本信息在 CONNECT 消息解码时确定，通过 `MqttCodecUtil.setMqttVersion(ctx, version)` 存储到 ChannelHandlerContext 中，后续消息解码时通过 `getMqttVersion(ctx)` 获取。

## 4. 设计思想

### 4.1 ReplayingDecoder 的应用

MQTT 解码器使用 `ReplayingDecoder` 而非普通的 `ByteToMessageDecoder`，这是一个关键的设计决策：

**优势**：
- 解码代码可以像读取完整数据一样编写，无需手动检查可读字节数
- 当数据不足时，`ReplayingDecoder` 会自动抛出 `Signal` 并等待更多数据
- 简化了多阶段解码的状态管理

**注意事项**：
- `ReplayingDecoder` 的性能略低于 `ByteToMessageDecoder`，因为 `Signal` 异常的创建和捕获有开销
- 解码器在每个阶段都通过 `checkpoint()` 记录状态，确保数据不足时能正确回退

### 4.2 消息类型的层次结构

```
MqttMessage (基类)
├── MqttConnectMessage      (CONNECT)
├── MqttConnAckMessage      (CONNACK)
├── MqttPublishMessage      (PUBLISH)
├── MqttPubAckMessage       (PUBACK)
├── MqttSubscribeMessage    (SUBSCRIBE)
├── MqttSubAckMessage       (SUBACK)
├── MqttUnsubscribeMessage  (UNSUBSCRIBE)
├── MqttUnsubAckMessage     (UNSUBACK)
└── (PINGREQ/PINGRESP/DISCONNECT 使用基类)
```

### 4.3 MQTT 5.0 属性编码

MQTT 5.0 引入了属性系统，Netty 通过 `MqttProperties` 类统一管理：

```java
// 属性类型包括：
// - IntegerProperty: 1/2/4 字节整数
// - StringProperty: UTF-8 字符串
// - BinaryProperty: 二进制数据
// - UserProperty: 键值对字符串
```

属性编码时，先编码属性内容到临时缓冲区，再写入属性长度前缀：

```java
private static ByteBuf encodeProperties(ByteBufAllocator byteBufAllocator, MqttProperties mqttProperties) {
    ByteBuf propertiesHeaderBuf = byteBufAllocator.buffer();
    ByteBuf propertiesBuf = byteBufAllocator.buffer();
    // 编码所有属性到 propertiesBuf
    // ...
    writeVariableLengthInt(propertiesHeaderBuf, propertiesBuf.readableBytes());
    propertiesHeaderBuf.writeBytes(propertiesBuf);
    return propertiesHeaderBuf;
}
```

## 5. 模块交互

### 5.1 编解码器在 Pipeline 中的位置

```
ChannelPipeline:
  ┌─────────────────────────────────────┐
  │  MqttDecoder (入站)                  │
  │    ↓                                │
  │  业务 Handler (处理 MqttMessage)     │
  │    ↓                                │
  │  MqttEncoder (出站)                  │
  └─────────────────────────────────────┘
```

### 5.2 版本信息的传递

```
客户端发送 CONNECT ──► MqttDecoder 解析协议版本
                         │
                         ▼
                    MqttCodecUtil.setMqttVersion(ctx, version)
                         │
                         ▼
                    后续消息解码时 getMqttVersion(ctx)
                         │
                         ▼
                    根据版本决定是否解析 Properties
```

### 5.3 错误处理机制

```java
private MqttMessage invalidMessage(Throwable cause) {
    checkpoint(DecoderState.BAD_MESSAGE);  // 切换到 BAD_MESSAGE 状态
    return MqttMessageFactory.newInvalidMessage(mqttFixedHeader, variableHeader, cause);
}
```

当解码失败时：
1. 创建一个带有 `DecoderResult.failure()` 的无效消息
2. 切换到 `BAD_MESSAGE` 状态，持续丢弃后续数据
3. 业务层可以通过 `message.decoderResult().isFailure()` 检测错误

## 6. 关键流程

### 6.1 CONNECT 消息解码流程

```
1. READ_FIXED_HEADER
   └─ 解析消息类型 = CONNECT (1)
   └─ 解析 remainingLength

2. READ_VARIABLE_HEADER
   └─ decodeConnectionVariableHeader()
      ├─ 读取协议名 ("MQIsdp" / "MQTT")
      ├─ 读取协议级别 (3/4/5)
      ├─ 读取连接标志字节
      │   ├─ hasUserName (bit 7)
      │   ├─ hasPassword (bit 6)
      │   ├─ willRetain (bit 5)
      │   ├─ willQos (bit 4-3)
      │   ├─ willFlag (bit 2)
      │   └─ cleanSession (bit 1)
      ├─ 读取 keepAlive 时间
      └─ (MQTT 5.0) 读取 Properties

3. READ_PAYLOAD
   └─ decodeConnectionPayload()
      ├─ 读取 clientId
      ├─ (如有遗嘱) 读取 willTopic 和 willMessage
      ├─ (如有用户名) 读取 userName
      └─ (如有密码) 读取 password
```

### 6.2 PUBLISH 消息解码流程

```
1. READ_FIXED_HEADER
   └─ 解析消息类型 = PUBLISH (3)
   └─ 解析 QoS (0/1/2)、DUP、Retain 标志

2. READ_VARIABLE_HEADER
   └─ decodePublishVariableHeader()
      ├─ 读取主题名 (UTF-8 字符串)
      ├─ (QoS > 0) 读取报文标识符
      └─ (MQTT 5.0) 读取 Properties

3. READ_PAYLOAD
   └─ decodePublishPayload()
      └─ 读取剩余字节作为消息体 (ByteBuf)
```

### 6.3 SUBSCRIBE 消息解码流程

```
1. READ_FIXED_HEADER
   └─ 解析消息类型 = SUBSCRIBE (8)
   └─ 验证: QoS 必须为 1，DUP 必须为 0，Retain 必须为 0

2. READ_VARIABLE_HEADER
   └─ decodeMessageIdAndPropertiesVariableHeader()
      ├─ 读取报文标识符 (2 字节)
      └─ (MQTT 5.0) 读取 Properties

3. READ_PAYLOAD
   └─ decodeSubscribePayload()
      └─ 循环读取主题过滤器
         ├─ 主题名 (UTF-8 字符串)
         └─ 订阅选项 (1 字节)
             ├─ QoS (bit 1-0)
             ├─ No Local (bit 2) [MQTT 5.0]
             ├─ Retain As Published (bit 3) [MQTT 5.0]
             └─ Retain Handling (bit 5-4) [MQTT 5.0]
```

### 6.4 可变长度整数编解码

MQTT 协议使用特殊的可变长度整数编码（Variable Byte Integer），每个字节的最高位表示是否还有后续字节：

**解码**：
```java
private static int parseRemainingLength(ByteBuf buffer, MqttMessageType messageType) {
    int remainingLength = 0;
    int multiplier = 1;
    for (int i = 0; i < 4; i++) {
        short digit = buffer.readUnsignedByte();
        remainingLength += (digit & 127) * multiplier;
        if ((digit & 128) == 0) {
            return remainingLength;
        }
        multiplier *= 128;
    }
    throw new DecoderException("remaining length exceeds 4 digits");
}
```

**编码**：
```java
private static void writeVariableLengthInt(ByteBuf buf, int num) {
    do {
        int digit = num & 0x7F;
        num >>>= 7;
        if (num > 0) {
            digit |= 0x80;
        }
        buf.writeByte(digit);
    } while (num > 0);
}
```

## 7. 学习要点

### 7.1 ReplayingDecoder 的使用模式

- 状态机模式：通过 `checkpoint()` 切换状态，`state()` 获取当前状态
- `Signal` 异常机制：数据不足时自动回退
- 适用于多阶段、变长协议的解码

### 7.2 MQTT 协议的核心概念

- **固定头 + 可变头 + 负载** 的三层消息结构
- **QoS 级别**：0（最多一次）、1（至少一次）、2（恰好一次）
- **报文标识符**：用于 QoS 1/2 的消息确认
- **遗嘱消息**：客户端异常断开时由服务器发布

### 7.3 版本兼容性设计

- 通过 `MqttVersion` 枚举统一管理版本差异
- 版本信息在 ChannelHandlerContext 中传递，避免在消息对象中重复存储
- MQTT 5.0 的 Properties 系统通过条件编解码实现向后兼容

### 7.4 性能优化技巧

- `MqttEncoder` 使用单例模式，避免重复创建
- 使用 `readRetainedSlice()` 而非 `readBytes()` 减少内存拷贝
- UTF-8 编码时使用 `reserveAndWriteUtf8()` 预计算长度，避免二次遍历

### 7.5 错误处理最佳实践

- 区分协议错误和解码错误
- 使用 `BAD_MESSAGE` 状态持续丢弃无效数据
- 通过 `DecoderResult` 向业务层传递错误信息，而非直接抛出异常
