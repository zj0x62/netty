# SMTP / SOCKS / HAProxy 协议编解码实现

## 1. 概述

本文档分析 Netty 中三个轻量级协议编解码器：SMTP（邮件传输）、SOCKS（代理协议）、HAProxy（PROXY 协议）。这三个模块的共同特点是协议结构相对简单，编解码器代码量精简，但各自体现了不同的设计取舍。

| 模块 | 协议类型 | 编码格式 | 核心特征 |
|------|---------|---------|---------|
| `codec-smtp` | 邮件传输 | 文本（CRLF 分隔） | 命令/响应模式，DATA 阶段支持二进制内容 |
| `codec-socks` | 网络代理 | 二进制 | 多阶段握手，自移除解码器 |
| `codec-haproxy` | 负载均衡代理信息传递 | v1 文本 / v2 二进制 | 仅解析连接头部，解析后自移除 |

## 2. SMTP 协议编解码

### 2.1 架构图

```
SmtpRequest                    SmtpResponse
    |                               |
DefaultSmtpRequest             DefaultSmtpResponse
(command + parameters)         (code + details)

Pipeline:
  SmtpResponseDecoder (入站)  ←→  SmtpRequestEncoder (出站)
         ↑                              ↑
  LineBasedFrameDecoder        SmtpContent + LastSmtpContent
```

### 2.2 核心类分析

#### SmtpCommand

**SmtpCommand**（`SmtpCommand.java`）：使用不可变类（非 enum）表示 SMTP 命令，内置标准命令缓存：

```java
public static final SmtpCommand EHLO = new SmtpCommand(AsciiString.cached("EHLO"));
public static final SmtpCommand HELO = new SmtpCommand(AsciiString.cached("HELO"));
public static final SmtpCommand MAIL = new SmtpCommand(AsciiString.cached("MAIL"));
public static final SmtpCommand RCPT = new SmtpCommand(AsciiString.cached("RCPT"));
public static final SmtpCommand DATA = new SmtpCommand(AsciiString.cached("DATA"));
public static final SmtpCommand QUIT = new SmtpCommand(AsciiString.cached("QUIT"));
```

`isContentExpected()` 方法返回 `true` 当命令为 `DATA`，标识后续需要传输邮件内容。

`valueOf()` 方法先查缓存 HashMap，未命中则创建新实例，支持非标准扩展命令。

#### SmtpRequest / SmtpResponse

**SmtpRequest**：`command()` + `parameters()`（`List<CharSequence>`）。

**SmtpResponse**：`code()`（int，100-599）+ `details()`（`List<CharSequence>`）。

**SmtpRequests** 工厂类提供便捷构造方法：

```java
SmtpRequests.ehlo("mail.example.com")    // EHLO mail.example.com
SmtpRequests.mail("sender@example.com")  // MAIL FROM:<sender@example.com>
SmtpRequests.rcpt("recv@example.com")    // RCPT TO:<recv@example.com>
SmtpRequests.data()                      // DATA
SmtpRequests.quit()                      // QUIT
```

#### SmtpRequestEncoder

**SmtpRequestEncoder**（`SmtpRequestEncoder.java`）：编码 `SmtpRequest` 和 `SmtpContent`。

编码逻辑：
1. 写入命令名（ASCII）
2. 写入参数（空格分隔）
3. 写入 CRLF
4. DATA 命令后进入内容模式，透传 `SmtpContent` 的 ByteBuf
5. `LastSmtpContent` 之后追加 `.CRLF`（邮件内容结束标记，行首单点转义）

```java
if (msg instanceof LastSmtpContent) {
    out.add(DOT_CRLF_BUFFER.retainedDuplicate()); // ".\r\n"
    contentExpected = false;
}
```

**安全防护**：`SmtpUtils.validateSMTPParameters()` 验证参数不包含 CRLF，防止 SMTP 命令注入攻击。

#### SmtpResponseDecoder

**SmtpResponseDecoder**（`SmtpResponseDecoder.java`）：继承 `LineBasedFrameDecoder`，逐行解析 SMTP 响应。

响应格式：
```
250-First line\n     ← 多行响应（'-' 分隔符）
250-Second line\n
250 Last line\n      ← 最后一行（' ' 分隔符）
```

解析逻辑：
- 前 3 字节为状态码（百位 + 十位 + 个位）
- 第 4 字节为分隔符：`'-'` 表示多行继续，`' '` 表示响应结束
- 使用实例变量 `details` 累积多行详情
- 遇到 `' '` 时输出完整 `DefaultSmtpResponse`

### 2.3 设计要点

1. **命令/响应分离**：请求编码和响应解码使用独立的 handler，天然支持客户端和服务端
2. **DATA 阶段的内容流**：`SmtpContent` + `LastSmtpContent` 支持大邮件的流式传输
3. **反注入验证**：`SmtpUtils` 在构造请求时验证参数不含 CRLF

## 3. SOCKS 协议编解码

### 3.1 架构图

```
SocksMessage (abstract)
    ├── SocksRequest (abstract)
    │     ├── SocksInitRequest      — 初始握手请求
    │     ├── SocksAuthRequest      — 认证请求
    │     └── SocksCmdRequest       — 命令请求（CONNECT/BIND/UDP）
    └── SocksResponse (abstract)
          ├── SocksInitResponse     — 初始握手响应
          ├── SocksAuthResponse     — 认证响应
          └── SocksCmdResponse      — 命令响应

解码器（各自独立，解码后自移除）:
  SocksInitRequestDecoder   SocksInitResponseDecoder
  SocksAuthRequestDecoder   SocksAuthResponseDecoder
  SocksCmdRequestDecoder    SocksCmdResponseDecoder

编码器:
  SocksMessageEncoder（通用，@Sharable）
```

### 3.2 SOCKS5 握手流程

```
Client                          Server
  |                               |
  |--- SocksInitRequest -------->|  版本(5) + 支持的认证方案列表
  |<-- SocksInitResponse --------|  版本(5) + 选定的认证方案
  |                               |
  |  [若需认证]                    |
  |--- SocksAuthRequest -------->|  版本(1) + 用户名 + 密码
  |<-- SocksAuthResponse --------|  版本(1) + 认证状态
  |                               |
  |--- SocksCmdRequest --------->|  版本(5) + CMD + 地址类型 + 地址 + 端口
  |<-- SocksCmdResponse ---------|  版本(5) + 状态 + 地址类型 + BND 地址 + 端口
  |                               |
  |=== 数据透传 ====>              |
```

### 3.3 核心类分析

#### SocksMessage 基类

**SocksMessage**（`SocksMessage.java`）：所有 SOCKS 消息的基类，持有 `SocksMessageType`（REQUEST/RESPONSE/UNKNOWN）和协议版本（固定为 SOCKS5）。提供已废弃的 `encodeAsByteBuf()` 方法用于编码。

#### 认证方案枚举

**SocksAuthScheme**：
- `NO_AUTH(0x00)` — 无需认证
- `AUTH_GSSAPI(0x01)` — GSSAPI 认证
- `AUTH_PASSWORD(0x02)` — 用户名/密码认证

#### 命令类型

**SocksCmdType**：
- `CONNECT(0x01)` — TCP 连接代理
- `BIND(0x02)` — 反向连接
- `UDP(0x03)` — UDP 关联

**SocksCmdStatus**：命令响应状态码，如 `SUCCESS(0x00)`、`FAILURE(0x01)`、`NETWORK_UNREACHABLE(0x03)`、`HOST_UNREACHABLE(0x04)` 等。

#### 地址类型

**SocksAddressType**：支持 IPv4、IPv6、域名三种地址类型，在 `SocksCmdRequest/Response` 的编码解码中按类型处理不同长度的地址。

#### 自移除解码器

SOCKS 解码器的核心设计特征是**解码完成后自动从 Pipeline 移除**：

```java
// SocksInitRequestDecoder.decode() 末尾
ctx.pipeline().remove(this);
```

这是因为 SOCKS 握手是多阶段过程，每个阶段的消息结构不同。使用多个专用解码器按阶段替换，而不是一个状态机处理所有阶段。Pipeline 中的编排示例：

```java
// 初始阶段
pipeline.addLast(new SocksInitRequestDecoder());
pipeline.addLast(new SocksMessageEncoder());

// 收到 InitRequest 后，替换为认证阶段解码器
// pipeline: SocksInitRequestDecoder 已自移除
pipeline.addLast(new SocksAuthRequestDecoder());
// ... 以此类推
```

#### SocksCmdRequestDecoder

`SocksCmdRequestDecoder` 是最复杂的解码器，状态机：

```
CHECK_PROTOCOL_VERSION → READ_CMD_HEADER → READ_CMD_ADDRESS
```

`READ_CMD_ADDRESS` 根据地址类型分支：
- IPv4：4 字节 IP + 2 字节端口
- 域名：1 字节长度 + 域名字符串 + 2 字节端口
- IPv6：16 字节 IP + 2 字节端口

#### SocksMessageEncoder

**SocksMessageEncoder**（`SocksMessageEncoder.java`）：`@Sharable` 的通用编码器，直接调用 `msg.encodeAsByteBuf(out)`。所有 SocksMessage 子类自行实现编码逻辑（在 `encodeAsByteBuf` 方法中）。

### 3.4 设计要点

1. **自移除解码器**：每个解码器只处理一种消息类型，解码完成后 `ctx.pipeline().remove(this)` 自动移除。这是 SOCKS 多阶段握手的典型处理模式
2. **消息自编码**：编码逻辑内置于消息类的 `encodeAsByteBuf()` 方法中，编码器仅做代理调用
3. **@Sharable 编码器**：`SocksMessageEncoder` 无状态，可在多个 Channel 间共享
4. **IPv4/IPv6/域名统一处理**：`SocksCmdRequest/Response` 通过 `SocksAddressType` 枚举统一处理三种地址格式

## 4. HAProxy PROXY 协议编解码

### 4.1 架构图

```
HAProxyMessage (extends AbstractReferenceCounted)
    ├── protocolVersion: V1 | V2
    ├── command: LOCAL | PROXY
    ├── proxiedProtocol: TCP4/TCP6/UDP4/UDP6/UNIX_STREAM/UNIX_DGRAM/UNKNOWN
    ├── sourceAddress / destinationAddress
    ├── sourcePort / destinationPort
    └── tlvs: List<HAProxyTLV>  (仅 V2)

Pipeline:
  HAProxyMessageDecoder (入站，解码后自移除)
  HAProxyMessageEncoder (出站，@Sharable 单例)
```

### 4.2 PROXY 协议概述

PROXY 协议由 HAProxy 开发，用于在 TCP 连接建立时传递客户端的真实 IP 地址。当请求经过多层代理/负载均衡器时，后端服务器可通过此协议获取原始客户端地址。

- **V1**：文本格式，如 `PROXY TCP4 192.168.1.1 192.168.1.2 56324 80\r\n`
- **V2**：二进制格式，以 `\x0D\x0A\x0D\x0A\x00\x0D\x0A\x51\x55\x49\x54\x0A`（12 字节魔数）开头

### 4.3 核心类分析

#### 版本与命令枚举

**HAProxyProtocolVersion**：`V1(0x10)` / `V2(0x20)`，高 4 位编码版本号。

**HAProxyCommand**：`LOCAL(0x00)` / `PROXY(0x01)`，低 4 位编码命令。LOCAL 表示代理自身发起的连接，PROXY 表示代理客户端的连接。

版本和命令共用一个字节：`高4位版本 | 低4位命令`。

#### 代理协议枚举

**HAProxyProxiedProtocol**：编码地址族 + 传输协议的组合：

| 枚举值 | 说明 | 地址族 | 传输协议 |
|--------|------|--------|---------|
| TCP4 | IPv4 TCP | AF_IPv4 | STREAM |
| TCP6 | IPv6 TCP | AF_IPv6 | STREAM |
| UDP4 | IPv4 UDP | AF_IPv4 | DGRAM |
| UDP6 | IPv6 UDP | AF_IPv6 | DGRAM |
| UNIX_STREAM | Unix 域套接字流 | AF_UNIX | STREAM |
| UNIX_DGRAM | Unix 域套接字数据报 | AF_UNIX | DGRAM |
| UNKNOWN | 未知 | AF_UNSPEC | UNSPEC |

地址族和传输协议也共用一个字节：`高4位地址族 | 低4位传输协议`。

#### HAProxyMessageDecoder

**HAProxyMessageDecoder**（`HAProxyMessageDecoder.java`）：继承 `ByteToMessageDecoder`，解码后自移除。

**版本检测**：`detectProtocol()` 静态方法通过前缀匹配判断版本：
- V2：匹配 12 字节二进制魔数 `\x0D\x0A\x0D\x0A\x00\x0D\x0A\x51\x55\x49\x54\x0A`
- V1：匹配文本前缀 `PROXY `

**V1 解码**：基于 `LineHeaderExtractor`，按 CRLF 分割行，然后调用 `HAProxyMessage.decodeHeader(String)` 按空格分割字段。

**V2 解码**：基于 `StructHeaderExtractor`，读取 16 字节固定头部，然后根据第 15-16 字节的地址信息长度读取地址块。

V2 头部结构：

```
字节 0-11:  12 字节魔数
字节 12:    版本 + 命令 (高4位版本, 低4位命令)
字节 13:    地址族 + 传输协议 (高4位地址族, 低4位传输协议)
字节 14-15: 地址信息长度 (unsigned short)
字节 16+:   地址信息（源地址 + 目标地址 + 端口 + TLV）
```

地址信息根据地址族解析：
- AF_IPv4：4 字节源 IP + 4 字节目标 IP + 2 字节源端口 + 2 字节目标端口 = 12 字节
- AF_IPv6：16 字节源 IP + 16 字节目标 IP + 2 字节源端口 + 2 字节目标端口 = 36 字节
- AF_UNIX：108 字节源路径 + 108 字节目标路径 = 216 字节

#### TLV 扩展

**HAProxyTLV**（`HAProxyTLV.java`）：V2 协议支持的 Type-Length-Value 扩展数据。

TLV 类型（`HAProxyTLV.Type`）：
- `PP2_TYPE_ALPN(0x01)` — ALPN 协议协商
- `PP2_TYPE_AUTHORITY(0x02)` — SNI 主机名
- `PP2_TYPE_SSL(0x20)` — SSL/TLS 信息
- `PP2_TYPE_SSL_VERSION(0x21)` — SSL 版本
- `PP2_TYPE_SSL_CN(0x22)` — SSL 证书 CN
- `PP2_TYPE_NETNS(0x30)` — 网络命名空间

**HAProxySSLTLV**（`HAProxySSLTLV.java`）：`PP2_TYPE_SSL` 的特殊 TLV，内部嵌套其他 TLV，包含：
- `client` 字节：客户端位字段（PP2_CLIENT_SSL / PP2_CLIENT_CERT_CONN / PP2_CLIENT_CERT_SESS）
- `verify` 整数：SSL 验证结果
- `encapsulatedTLVs`：嵌套的 TLV 列表

TLV 解析支持最大 128 层嵌套深度限制，防止恶意数据导致栈溢出。

#### HAProxyMessageEncoder

**HAProxyMessageEncoder**（`HAProxyMessageEncoder.java`）：`@Sharable` 单例（`INSTANCE`），编码 `HAProxyMessage` 为 V1 文本或 V2 二进制格式。

V1 编码：`PROXY TCP4 srcAddr dstAddr srcPort dstPort\r\n`

V2 编码：12 字节魔数 + 版本命令字节 + 协议字节 + 地址长度 + 地址信息 + TLV 列表。

### 4.4 设计要点

1. **协议自动检测**：`detectProtocol()` 通过前缀匹配自动识别 V1/V2，可用于 Pipeline 的条件初始化
2. **自移除解码器**：PROXY 协议头只在连接建立时发送一次，解码完成后立即移除，不影响后续数据流
3. **TLV 嵌套与递归解析**：SSL TLV 内部可嵌套其他 TLV，解析时带嵌套深度限制
4. **引用计数与泄漏检测**：`HAProxyMessage` 继承 `AbstractReferenceCounted`，使用 `ResourceLeakDetector` 跟踪 TLV 列表的释放
5. **字节位操作**：版本/命令和地址族/传输协议各共用一个字节，通过位掩码分离

## 5. 三个模块的设计对比

| 维度 | SMTP | SOCKS | HAProxy |
|------|------|-------|---------|
| **协议用途** | 邮件传输 | 网络代理 | 传递客户端真实地址 |
| **编码格式** | 纯文本 | 二进制 | V1 文本 / V2 二进制 |
| **生命周期** | 持续（多个请求/响应） | 握手阶段（3-5 个来回） | 连接建立时（一次性） |
| **解码器基类** | LineBasedFrameDecoder | ReplayingDecoder | ByteToMessageDecoder |
| **自移除** | 否 | 是 | 是 |
| **编码器** | 独立类 | 消息自编码 | 独立类 + 单例 |
| **内容流** | SmtpContent | 无 | 无（TLV 非流式） |
| **@Sharable** | 否（有状态） | 是 | 是 |

## 6. 学习要点

1. **LineBasedFrameDecoder 应用**：SMTP 解码器展示了基于行的文本协议如何复用 Netty 的行解码器
2. **自移除解码器模式**：SOCKS 和 HAProxy 的解码器都在完成解码后自移除，适合一次性握手/头部解析场景
3. **消息自编码 vs 编码器编码**：SOCKS 将编码逻辑放在消息类中（`encodeAsByteBuf`），SMTP/HAProxy 使用独立编码器。前者减少类数量但耦合度高，后者职责分离更清晰
4. **协议自动检测**：HAProxy 的 `detectProtocol()` 是协议嗅探的典型实现，可用于 Pipeline 的动态组装
5. **二进制位字段编码**：HAProxy V2 的版本/命令、地址族/传输协议各占半字节，是紧凑二进制协议的常见手法
6. **TLV 扩展机制**：HAProxy V2 的 TLV 结构支持协议扩展，嵌套深度限制是防御恶意数据的必要措施
7. **SMTP 命令注入防护**：`SmtpUtils.validateSMTPParameters()` 验证参数不含 CRLF，是文本协议安全编码的重要实践
8. **CombinedChannelDuplexHandler 的缺席**：SOCKS 模块未使用组合 handler，因为握手阶段的编解码不对称（每阶段使用不同解码器）
