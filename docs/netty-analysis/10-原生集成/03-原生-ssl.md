# 原生 SSL（BoringSSL/OpenSSL）

## 概述

Netty 通过 `netty-tcnative` 库实现了对 OpenSSL/BoringSSL 的 JNI 集成，提供了比 JDK SSL 更高性能的 TLS 实现。`ReferenceCountedOpenSslContext` 和 `ReferenceCountedOpenSslEngine` 是这一集成的核心 Java 类，它们通过 `io.netty.internal.tcnative` 包中的 JNI 桥接类（`SSL`、`SSLContext`、`Buffer`）与底层 C 库交互。

**核心模块**: `handler`（SSL 处理器）、`netty-tcnative`（JNI 桥接）

**关键特性**:
- 支持 OpenSSL 和 BoringSSL 两种后端
- 完整的 TLS 1.3 支持
- ALPN/NPN 协议协商
- OCSP 装订支持
- 引用计数管理原生内存生命周期
- 异步私钥操作支持

## 架构图

```
+------------------------------------------------------------------+
|                      Java 应用层                                  |
+------------------------------------------------------------------+
                              |
+-----------------------------v------------------------------------+
|                    SslHandler (Channel Handler)                   |
+-----------------------------+------------------------------------+
                              |
         +--------------------+--------------------+
         |                                         |
+--------v--------+                    +-----------v-----------+
| SslContext       |                    | SSLEngine              |
| (抽象基类)       |                    | (JDK 或 OpenSSL)       |
+--------+--------+                    +-----------+-----------+
         |                                         |
+--------v--------+                    +-----------v-----------+
| ReferenceCounted |                    | ReferenceCounted       |
| OpenSslContext   |                    | OpenSslEngine          |
+--------+--------+                    +-----------+-----------+
         |                                         |
         |   JNI 桥接层 (netty-tcnative)            |
         |                                         |
+--------v--------+                    +-----------v-----------+
| SSLContext       |                    | SSL                    |
| .make()          |                    | .newSSL()              |
| .setCipherSuite()|                    | .read() / .write()     |
| .setAlpnProtos() |                    | .getHandshakeStatus()  |
+--------+--------+                    +-----------+-----------+
         |                                         |
+--------v--------+                    +-----------v-----------+
| BIO (内存缓冲区) |                    | SSL_CTX / SSL 对象     |
+--------+--------+                    +-----------+-----------+
         |                                         |
+--------v-----------------------------------------v-----------+
|              OpenSSL / BoringSSL C 库                         |
|  (libssl.so / libcrypto.so / libssl.a / libcrypto.a)         |
+--------------------------------------------------------------+
```

## 核心类分析

### 1. OpenSsl — OpenSSL 可用性检测与初始化

`OpenSsl` 是 SSL 原生集成的入口类，负责检测和初始化 OpenSSL/BoringSSL。

**文件位置**: `handler/src/main/java/io/netty/handler/ssl/OpenSsl.java`

**初始化流程**:

```java
static {
    Throwable cause = null;

    // 1. 检查是否显式禁用
    if (SystemPropertyUtil.getBoolean("io.netty.handler.ssl.noOpenSsl", false)) {
        cause = new UnsupportedOperationException("OpenSSL was explicit disabled");
    }

    // 2. 检查 netty-tcnative 是否在 classpath
    if (cause == null) {
        try {
            Class.forName("io.netty.internal.tcnative.SSLContext", false,
                    PlatformDependent.getClassLoader(OpenSsl.class));
        } catch (ClassNotFoundException t) {
            cause = t;
        }
    }

    // 3. 加载原生库并初始化
    if (cause == null) {
        try {
            loadTcNative();           // 加载 JNI 库
            initializeTcNative(engine); // 初始化引擎
        } catch (Throwable t) {
            cause = t;
        }
    }

    UNAVAILABILITY_CAUSE = cause;
}
```

**后端检测**:

```java
// 检测当前使用的 SSL 后端
String versionString = versionString();
IS_BORINGSSL = "BoringSSL".equals(versionString);
IS_AWSLC = versionString != null && versionString.startsWith("AWS-LC");
```

**密码套件探测**:

```java
// 创建临时 SSL_CTX 探测支持的密码套件
final long sslCtx = SSLContext.make(SSL.SSL_PROTOCOL_ALL, SSL.SSL_MODE_SERVER);
SSLContext.setCipherSuite(sslCtx, "ALL", false);

final long ssl = SSL.newSSL(sslCtx, true);
for (String c : SSL.getCiphers(ssl)) {
    availableOpenSslCipherSuites.add(c);
}
```

### 2. ReferenceCountedOpenSslContext — SSL 上下文

`ReferenceCountedOpenSslContext` 是 OpenSSL SSL_CTX 对象的 Java 封装，管理 TLS 连接的全局配置。

**文件位置**: `handler/src/main/java/io/netty/handler/ssl/ReferenceCountedOpenSslContext.java`

**核心字段**:

```java
public abstract class ReferenceCountedOpenSslContext extends SslContext implements ReferenceCounted {
    protected long ctx;  // OpenSSL SSL_CTX 指针（原生内存）

    final ReadWriteLock ctxLock = new ReentrantReadWriteLock();
    final ConcurrentMap<Long, ReferenceCountedOpenSslEngine> engines = new ConcurrentHashMap<>();
    final List<OpenSslCredential> credentials = new ArrayList<>();

    final Certificate[] keyCertChain;
    final ClientAuth clientAuth;
    final String[] protocols;
    final String[] groups;
    final boolean enableOcsp;
}
```

**构造函数中的 SSL_CTX 配置**:

```java
ReferenceCountedOpenSslContext(...) throws SSLException {
    // 1. 创建 SSL_CTX
    ctx = SSLContext.make(protocolOpts, mode);

    // 2. 配置密码套件
    SSLContext.setCipherSuite(ctx, cipherBuilder.toString(), false);      // TLS 1.2 及以下
    SSLContext.setCipherSuite(ctx, cipherTLSv13Builder.toString(), true); // TLS 1.3

    // 3. 设置协议选项
    int options = SSLContext.getOptions(ctx)
        | SSL.SSL_OP_NO_SSLv2 | SSL.SSL_OP_NO_SSLv3
        | SSL.SSL_OP_NO_TLSv1 | SSL.SSL_OP_NO_TLSv1_1
        | SSL.SSL_OP_CIPHER_SERVER_PREFERENCE
        | SSL.SSL_OP_NO_COMPRESSION | SSL.SSL_OP_NO_TICKET;
    SSLContext.setOptions(ctx, options);

    // 4. 配置 ALPN/NPN
    switch (apn.protocol()) {
        case ALPN:
            SSLContext.setAlpnProtos(ctx, appProtocols, selectorBehavior);
            break;
        case NPN:
            SSLContext.setNpnProtos(ctx, appProtocols, selectorBehavior);
            break;
    }

    // 5. 配置 OCSP
    if (enableOcsp) {
        SSLContext.enableOcsp(ctx, isClient());
    }

    // 6. 配置椭圆曲线/组
    SSLContext.setCurvesList(ctx, groups);

    // 7. 配置私钥方法（可选）
    if (privateKeyMethod != null) {
        SSLContext.setPrivateKeyMethod(ctx, new PrivateKeyMethod(engines, privateKeyMethod));
    }
}
```

**引用计数管理**:

```java
private final AbstractReferenceCounted refCnt = new AbstractReferenceCounted() {
    @Override
    protected void deallocate() {
        try {
            destroy();  // 释放 SSL_CTX
        } finally {
            if (leak != null) {
                leak.close(ReferenceCountedOpenSslContext.this);
            }
        }
    }
};

private void destroy() {
    ctxLock.writeLock().lock();
    try {
        SSLContext.free(ctx);  // 释放原生内存
        ctx = 0;
    } finally {
        ctxLock.writeLock().unlock();
    }
}
```

### 3. ReferenceCountedOpenSslEngine — SSL 引擎

`ReferenceCountedOpenSslEngine` 是 OpenSSL SSL 对象的 Java 封装，代表一个 TLS 连接。

**文件位置**: `handler/src/main/java/io/netty/handler/ssl/ReferenceCountedOpenSslEngine.java`

**核心字段**:

```java
public class ReferenceCountedOpenSslEngine extends SSLEngine implements ReferenceCounted {
    private long ssl;       // OpenSSL SSL 指针
    private long networkBIO; // 网络 BIO 指针

    private HandshakeState handshakeState = HandshakeState.NOT_STARTED;
    private boolean receivedShutdown;
    private volatile boolean destroyed;
    private volatile String applicationProtocol;
    private volatile boolean needTask;
}
```

**握手状态机**:

```java
private enum HandshakeState {
    NOT_STARTED,        // 未开始
    STARTED_IMPLICITLY, // 通过 unwrap/wrap 隐式开始
    STARTED_EXPLICITLY, // 通过 beginHandshake() 显式开始
    FINISHED            // 完成
}
```

**wrap 操作**（加密出站数据）:

```java
public SSLEngineResult wrap(ByteBuffer[] srcs, int offset, int length, ByteBuffer dst) {
    // 1. 检查状态
    if (destroyed) {
        return CLOSED_NOT_HANDSHAKING;
    }

    // 2. 执行 SSL 写入
    int bytesProduced = 0;
    for (ByteBuffer src : srcs) {
        int len = src.remaining();
        if (len > 0) {
            int result = SSL.write(ssl, src);
            if (result > 0) {
                bytesProduced += result;
            }
        }
    }

    // 3. 从 BIO 读取加密数据到 dst
    int pending = SSL.bioLengthPending(networkBIO);
    if (pending > 0) {
        bytesProduced = SSL.bioRead(networkBIO, dst);
    }

    // 4. 返回结果
    return new SSLEngineResult(getEngineStatus(), getHandshakeStatus(), bytesConsumed, bytesProduced);
}
```

**unwrap 操作**（解密入站数据）:

```java
public SSLEngineResult unwrap(ByteBuffer src, ByteBuffer[] dsts, int offset, int length) {
    // 1. 将加密数据写入 BIO
    int bytesConsumed = SSL.bioWrite(networkBIO, src);

    // 2. 执行 SSL 读取
    int bytesProduced = 0;
    for (ByteBuffer dst : dsts) {
        int result = SSL.read(ssl, dst);
        if (result > 0) {
            bytesProduced += result;
        }
    }

    // 3. 返回结果
    return new SSLEngineResult(getEngineStatus(), getHandshakeStatus(), bytesConsumed, bytesProduced);
}
```

### 4. netty-tcnative JNI 桥接类

`netty-tcnative` 提供了以下 JNI 桥接类：

| Java 类 | 功能 | 对应 C 结构 |
|---------|------|------------|
| `SSLContext` | SSL 上下文管理 | `SSL_CTX` |
| `SSL` | SSL 连接操作 | `SSL` |
| `Buffer` | BIO 操作 | `BIO` |
| `Library` | 库版本查询 | N/A |

**关键 JNI 方法**:

```java
// SSLContext 类
public class SSLContext {
    public static native long make(int protocols, int mode);
    public static native void free(long ctx);
    public static native void setCipherSuite(long ctx, String cipher, boolean tlsv13);
    public static native void setAlpnProtos(long ctx, String[] protocols, int selectorBehavior);
    public static native void setOptions(long ctx, int options);
    public static native void enableOcsp(long ctx, boolean client);
    public static native boolean setCurvesList(long ctx, String curves);
    public static native void setPrivateKeyMethod(long ctx, PrivateKeyMethod method);
}

// SSL 类
public class SSL {
    public static native long newSSL(long ctx, boolean server);
    public static native void free(long ssl);
    public static native int read(long ssl, ByteBuffer dst);
    public static native int write(long ssl, ByteBuffer src);
    public static native int bioLengthPending(long bio);
    public static native int bioRead(long bio, ByteBuffer dst);
    public static native String[] getCiphers(long ssl);
    public static native String getVersionString();
    public static native int getHandshakeStatus(long ssl);
}
```

## 设计思想

### 1. BIO 内存抽象

OpenSSL 使用 BIO（Basic I/O）抽象进行数据 I/O。Netty 通过创建内存 BIO 对，实现了 Java ByteBuffer 与 OpenSSL 之间的数据传递：

```
+------------------+     BIO_write      +------------------+
| 网络数据 (入站)   | ----------------> | SSL 对象          |
| (ByteBuffer)     |                    | (解密处理)        |
+------------------+                    +------------------+
                                               |
                                          SSL_read
                                               |
                                               v
+------------------+                    +------------------+
| 应用数据 (出站)   | <---------------- | 明文数据          |
| (ByteBuffer)     |                    | (ByteBuffer)     |
+------------------+     BIO_read       +------------------+
```

### 2. 引用计数与原生内存管理

OpenSSL 的 SSL_CTX 和 SSL 对象是原生内存，必须手动释放。Netty 使用引用计数模式管理生命周期：

```java
// ReferenceCountedOpenSslContext
- retain()  → 引用计数 +1
- release() → 引用计数 -1，当计数为 0 时调用 SSLContext.free(ctx)

// ReferenceCountedOpenSslEngine
- retain()  → 引用计数 +1
- release() → 引用计数 -1，当计数为 0 时调用 SSL.free(ssl)
```

**生命周期约束**:
- Engine 必须在 Context 之前释放
- Engine 持有 Context 的引用，释放时自动调用 `parentContext.release()`

### 3. 协议版本控制

通过位掩码控制支持的 TLS 版本：

```java
// 选项常量
SSL.SSL_OP_NO_SSLv2   = 0x01000000
SSL.SSL_OP_NO_SSLv3   = 0x02000000
SSL.SSL_OP_NO_TLSv1   = 0x04000000
SSL.SSL_OP_NO_TLSv1_1 = 0x10000000
SSL.SSL_OP_NO_TLSv1_2 = 0x08000000
SSL.SSL_OP_NO_TLSv1_3 = 0x20000000

// 默认禁用不安全版本
int options = SSL.SSL_OP_NO_SSLv2 | SSL.SSL_OP_NO_SSLv3
            | SSL.SSL_OP_NO_TLSv1 | SSL.SSL_OP_NO_TLSv1_1;
```

### 4. BoringSSL 特性支持

BoringSSL 相比 OpenSSL 提供了额外特性：

```java
if (IS_BORINGSSL) {
    // 支持后量子密钥交换
    defaultConvertedNamedGroups.add("X25519MLKEM768");

    // 额外的 TLS 1.3 密码套件
    EXTRA_SUPPORTED_TLS_1_3_CIPHERS = new String[] {
        "TLS_AES_128_GCM_SHA256",
        "TLS_AES_256_GCM_SHA384",
        "TLS_CHACHA20_POLY1305_SHA256"
    };
}
```

## 模块交互

### SSL 上下文创建流程

```
SslContextBuilder.build()
    │
    ├── new ReferenceCountedOpenSslContext(...)
    │       │
    │       ├── SSLContext.make(protocolOpts, mode)
    │       │       └── 创建 SSL_CTX 对象
    │       │
    │       ├── SSLContext.setCipherSuite(ctx, ciphers, false)
    │       │       └── 设置 TLS 1.2 密码套件
    │       │
    │       ├── SSLContext.setCipherSuite(ctx, tlsv13Ciphers, true)
    │       │       └── 设置 TLS 1.3 密码套件
    │       │
    │       ├── SSLContext.setOptions(ctx, options)
    │       │       └── 禁用不安全协议
    │       │
    │       ├── SSLContext.setAlpnProtos(ctx, protocols, behavior)
    │       │       └── 配置 ALPN 协议列表
    │       │
    │       └── SSLContext.setCurvesList(ctx, groups)
    │               └── 配置椭圆曲线
    │
    └── 返回 ReferenceCountedOpenSslContext 实例
```

### SSL 握手流程

```
SslHandler.handshake()
    │
    ├── engine.beginHandshake()
    │       └── SSL.doHandshake(ssl)
    │
    ├── [需要数据] engine.wrap(appData)
    │       │
    │       ├── SSL.write(ssl, src)        — 写入明文
    │       ├── BIO_read(networkBIO, dst)  — 读取密文
    │       └── 发送到网络
    │
    ├── [收到数据] engine.unwrap(ciphertext)
    │       │
    │       ├── BIO_write(networkBIO, src) — 写入密文
    │       ├── SSL.read(ssl, dst)         — 读取明文
    │       └── 返回给应用
    │
    └── [握手完成]
            ├── 获取协商的协议 (ALPN)
            ├── 获取协商的密码套件
            └── 触发 HandshakeComplete 事件
```

### 引用计数生命周期

```
创建 Context (refCnt = 1)
    │
    ├── 创建 Engine 1 (context.retain() → refCnt = 2)
    ├── 创建 Engine 2 (context.retain() → refCnt = 3)
    │
    ├── Engine 1 关闭 (engine.release() → context.release() → refCnt = 2)
    ├── Engine 2 关闭 (engine.release() → context.release() → refCnt = 1)
    │
    └── Context 关闭 (context.release() → refCnt = 0 → SSLContext.free(ctx))
```

## 关键流程

### TLS 1.3 密码套件配置

```
OpenSsl 静态初始化
    │
    ├── 探测 TLS 1.3 支持
    │       ├── 创建临时 SSL_CTX
    │       ├── 设置 TLS 1.3 密码套件
    │       └── 成功则 tlsv13Supported = true
    │
    ├── 构建密码套件字符串
    │       ├── TLS 1.2: "ECDHE-RSA-AES128-GCM-SHA256:..."
    │       └── TLS 1.3: "TLS_AES_128_GCM_SHA256:..."
    │
    └── 应用到 SSL_CTX
            ├── SSLContext.setCipherSuite(ctx, tls12Ciphers, false)
            └── SSLContext.setCipherSuite(ctx, tls13Ciphers, true)
```

### 证书加载流程

```
ReferenceCountedOpenSslContext 构造
    │
    ├── [PEM 格式]
    │       ├── 读取证书文件 → byte[]
    │       ├── 创建 BIO (toBIO)
    │       ├── SSL.parseX509Chain(certBio) → long certChain
    │       └── SSLContext.setCertificateChainFile(ctx, certFile)
    │
    ├── [JKS/PKCS12 格式]
    │       ├── KeyManagerFactory.getKeyManagers()
    │       ├── X509KeyManager.getCertificateChain(alias)
    │       └── 转换为 PEM 后加载
    │
    └── [OpenSslX509KeyManagerFactory]
            ├── 直接使用 OpenSSL 原生证书加载
            └── SSLContext.setCertificate(ctx, certBio, keyBio)
```

## 学习要点

1. **BIO 桥接模式**：OpenSSL 的 BIO 抽象是 Java ByteBuffer 与原生 SSL 之间的桥梁，通过 `BIO_write`/`BIO_read` 实现数据传递，避免了 JNI 数组拷贝

2. **引用计数管理**：原生内存（SSL_CTX/SSL）必须手动释放，Netty 使用引用计数确保所有 Engine 释放后才释放 Context，防止 Use-After-Free

3. **协议版本位掩码**：使用 `SSL_OP_NO_*` 位掩码精确控制支持的 TLS 版本，默认禁用 SSLv2/v3 和 TLS 1.0/1.1 等不安全版本

4. **BoringSSL 差异处理**：BoringSSL 与 OpenSSL 在 API 行为上有差异（如 `SSL.getCiphers()` 不返回 TLS 1.3 套件），Netty 通过 `IS_BORINGSSL` 标志进行适配

5. **线程安全设计**：`ReferenceCountedOpenSslContext` 使用 `ReadWriteLock` 保护 `ctx` 指针，读操作（创建 Engine）使用读锁，写操作（销毁）使用写锁
