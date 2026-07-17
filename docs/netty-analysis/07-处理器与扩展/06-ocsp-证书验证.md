# OCSP 证书验证 — OcspServerCertificateValidator 源码分析

## 概述

OCSP（Online Certificate Status Protocol，在线证书状态协议）模块为 Netty 的 SSL/TLS 连接提供实时证书吊销状态验证。与传统的 CRL（证书吊销列表）相比，OCSP 可以实时查询证书状态，避免下载庞大的吊销列表。

**源码位置**: `handler-ssl-ocsp/src/main/java/io/netty/handler/ssl/ocsp/`

## 架构图

```
Pipeline
┌─────────────────────────────────────────────────────────────┐
│                OcspServerCertificateValidator                │
│                (ByteToMessageDecoder + ChannelOutboundHandler)│
│                                                             │
│  SslHandshakeCompletionEvent                                │
│         │                                                   │
│         ▼                                                   │
│  ┌─────────────────────────────────────────────┐            │
│  │  OcspClient.query()                         │            │
│  │  ┌───────────────────────────────────────┐  │            │
│  │  │ 1. 解析证书 AIA 扩展获取 OCSP URL     │  │            │
│  │  │ 2. 构建 OCSP Request (含 Nonce)       │  │            │
│  │  │ 3. HTTP POST 发送到 OCSP Responder    │  │            │
│  │  │ 4. 验证 OCSP Response                 │  │            │
│  │  └───────────────────────────────────────┘  │            │
│  └─────────────────────────────────────────────┘            │
│         │                                                   │
│         ▼                                                   │
│  OcspValidationEvent ──► 传递给下游 Handler                  │
│  [若无效且 closeAndThrowIfNotValid]                          │
│         │                                                   │
│         ▼                                                   │
│  触发异常 + 关闭连接                                         │
└─────────────────────────────────────────────────────────────┘
```

## 核心类分析

### OcspServerCertificateValidator

核心验证器，同时实现 `ByteToMessageDecoder`（入站）和 `ChannelOutboundHandler`（出站）。

**双重角色设计**:
- 作为 `ByteToMessageDecoder`：在 OCSP 查询期间缓冲收到的数据，`decode()` 方法为空实现
- 作为 `ChannelOutboundHandler`：在查询期间拦截 `read()` 操作，暂停读取直到验证完成

```java
@Override
protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
    // 缓冲数据直到 OCSP 处理完成
}

@Override
public void read(ChannelHandlerContext ctx) throws Exception {
    if (ocspQueryInProgress) {
        readPending = true;  // 暂存读请求
    } else {
        ctx.read();
    }
}
```

**验证触发时机**: 监听 `SslHandshakeCompletionEvent`，仅在 TLS 握手成功后执行验证。

```java
if (sslHandshakeCompletionEvent.isSuccess()) {
    Certificate[] certificates = ctx.pipeline().get(SslHandler.class)
            .engine().getSession().getPeerCertificates();
    // certificates[0] = 服务器证书, certificates[1] = 颁发者证书
    OcspClient.query(certificates[0], certificates[1], ...);
}
```

**验证结果处理**:

```java
// 解析证书状态
if (response.getCertStatus() == null) {
    status = OcspResponse.Status.VALID;       // null 表示证书有效
} else if (response.getCertStatus() instanceof RevokedStatus) {
    status = OcspResponse.Status.REVOKED;     // 已吊销
} else {
    status = OcspResponse.Status.UNKNOWN;     // 未知状态
}

// 触发事件供下游处理
ctx.fireUserEventTriggered(new OcspValidationEvent(new OcspResponse(status, ...)));

// 根据配置决定是否关闭连接
if (status != OcspResponse.Status.VALID && closeAndThrowIfNotValid) {
    ctx.fireExceptionCaught(new OCSPException("Certificate not valid"));
    ctx.close();
}
```

### OcspClient

OCSP 查询客户端，负责构建请求、发送查询、验证响应。

**查询流程**:

1. **解析 OCSP URL**: 从证书的 Authority Information Access (AIA) 扩展中提取 OCSP Responder 地址
2. **构建 OCSP Request**: 使用 BouncyCastle 库构建请求，包含证书序列号和 16 字节随机 Nonce
3. **HTTP 发送**: 通过 Netty 的 `Bootstrap` 建立到 OCSP Responder 的 HTTP 连接，发送 POST 请求
4. **验证响应**: 校验 Nonce、签名、证书链

```java
// 构建 OCSP 请求
CertificateID certificateID = new CertificateID(digestCalculatorProvider.get(HASH_SHA1),
        new JcaX509CertificateHolder(issuer), x509Certificate.getSerialNumber());

OCSPReqBuilder builder = new OCSPReqBuilder();
builder.addRequest(certificateID);

// 添加 16 字节 Nonce 防重放
byte[] nonce = new byte[16];
SECURE_RANDOM.nextBytes(nonce);
builder.setRequestExtensions(new Extensions(
        new Extension(id_pkix_ocsp_nonce, false, new DEROctetString(nonce))));
```

**响应验证** (`validateResponse()`):

1. **响应数量检查**: 只请求了一个证书，响应数必须为 1
2. **CertID 匹配**: 验证响应中的 CertID 与请求的证书一致
3. **Nonce 验证**: 比对响应中的 Nonce 与请求中发送的 Nonce
4. **签名验证**: 验证 OCSP 响应的数字签名

```java
// 签名验证：区分有无独立 responder 证书的情况
if (certs != null && certs.length > 0) {
    // 有独立 responder 证书，验证签名并检查证书链
    ContentVerifierProvider responderVerifier = providerBuilder.build(responderCert);
    if (!resp.isSignatureValid(responderVerifier)) {
        throw new OCSPException("OCSP response signature is not valid");
    }
    validateCertificateChain(responderCert, certs, issuerCertificate);
} else {
    // 无独立 responder 证书，直接用 issuer 证书验证
    ContentVerifierProvider issuerVerifier = providerBuilder.build(issuerCertificate);
    if (!resp.isSignatureValid(issuerVerifier)) {
        throw new OCSPException("OCSP response signature is not valid");
    }
}
```

### OcspHttpHandler

处理与 OCSP Responder 的 HTTP 通信，继承 `ChannelDuplexHandler`。

- 收到响应后验证 `Content-Type: application/ocsp-response` 和 HTTP 状态码 200
- 写请求时启动超时定时器（默认 10 秒）
- 响应成功后通过 `Promise<OCSPResp>` 回调

### OcspResponse / OcspValidationEvent

数据模型类：

```java
public class OcspResponse {
    public enum Status { VALID, REVOKED, UNKNOWN }
    private final Status status;
    private final Date thisUpdate;
    private final Date nextUpdate;
}
```

### IoTransport

封装 OCSP 查询所需的 I/O 基础设施（EventLoop、SocketChannel、DatagramChannel），提供默认 NIO 实现。

## 设计思想

1. **Pipeline 嵌入式验证**: 验证器作为 Pipeline 的一部分，与 SSL 握手流程无缝集成
2. **异步非阻塞**: OCSP 查询通过 Netty 的 `Bootstrap` 发起异步 HTTP 请求，不阻塞 IO 线程
3. **读暂停保护**: 查询期间暂停读取，避免在证书状态未确认前处理业务数据
4. **Nonce 防重放**: 16 字节随机 Nonce 防止 OCSP 响应被重放攻击
5. **可配置的严格程度**: `closeAndThrowIfNotValid` 参数允许灵活选择"严格模式"（关闭连接）或"宽松模式"（仅通知）

## 关键流程

```
TLS 握手完成
    │
    ▼
SslHandshakeCompletionEvent.isSuccess()
    │
    ▼
获取 peerCertificates (server cert + issuer cert)
    │
    ▼
OcspClient.query()
    │
    ├─ 解析 AIA 扩展 ──► 获取 OCSP Responder URL
    │
    ├─ 构建 OCSP Request (CertificateID + Nonce)
    │
    ├─ Bootstrap.connect(ocspResponder)
    │       │
    │       ▼
    │   HTTP POST /ocsp (application/ocsp-request)
    │       │
    │       ▼
    │   OcspHttpHandler.channelRead()
    │       │
    │       ▼
    │   Promise<OCSPResp> 完成
    │
    ▼
validateResponse()
    ├─ 检查响应数量 == 1
    ├─ 检查 CertID 匹配
    ├─ 验证 Nonce
    └─ 验证签名 + 证书链
        │
        ▼
    Promise<BasicOCSPResp> 完成
        │
        ▼
    解析证书状态 (VALID / REVOKED / UNKNOWN)
        │
        ▼
    触发 OcspValidationEvent
        │
        ├─ VALID ──► 正常继续
        │
        └─ 非 VALID 且 closeAndThrowIfNotValid
                │
                ▼
            触发异常 + 关闭连接
```

## 学习要点

- **OCSP Stapling vs OCSP 查询**: Netty 的 `OcspClientHandler` 处理的是 OCSP Stapling（服务端主动附带 OCSP 响应），而 `OcspServerCertificateValidator` 是客户端主动查询 OCSP Responder
- **ByteToMessageDecoder 的妙用**: 继承它不是为了解码，而是利用其数据缓冲能力，在 OCSP 查询期间暂存收到的数据
- **ChannelOutboundHandler 的 read() 拦截**: 通过拦截 `read()` 操作暂停数据读取，这是 Netty 中控制流量的常用模式
- **BouncyCastle 依赖**: OCSP 模块依赖 BouncyCastle 加密库进行 ASN.1 编解码、证书操作和签名验证
- **OCSP 响应时效性检查**: 验证 `thisUpdate` 和 `nextUpdate` 确保响应未过期
