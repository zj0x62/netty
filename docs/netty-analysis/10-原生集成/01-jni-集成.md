# JNI 集成机制

## 概述

Netty 通过 JNI（Java Native Interface）实现了对操作系统底层能力的直接访问，这是其高性能网络编程的基石。JNI 集成层包含三个核心部分：原生库加载器（`NativeLibraryLoader`）、Unix 通用原生操作（`transport-native-unix-common`）、以及平台特定的传输实现（epoll/kqueue/io_uring）。

**核心模块**: `common`（NativeLibraryLoader）、`transport-native-unix-common`（Unix JNI 基础）

**关键特性**:
- 三级原生库加载策略：System.loadLibrary → classpath 资源 → 临时文件
- 支持 Maven Shade 插件的包前缀重定位（shading）
- 跨平台的文件描述符和 Socket 操作抽象
- 直接内存（DirectByteBuffer）与原生指针的高效桥接

## 架构图

```
+------------------------------------------------------------------+
|                        Java 应用层                                |
+------------------------------------------------------------------+
                              |
+-----------------------------v------------------------------------+
|                    NativeLibraryLoader                            |
|  加载策略: System.loadLibrary → classpath → 临时文件              |
+-----------------------------+------------------------------------+
                              |
         +--------------------+--------------------+
         |                    |                    |
+--------v--------+  +--------v--------+  +-------v---------+
| Unix 通用原生    |  | epoll 原生       |  | kqueue 原生      |
| (unix-common)   |  | (linux)          |  | (macOS/BSD)      |
+--------+--------+  +--------+--------+  +-------+---------+
         |                    |                    |
+--------v--------+  +--------v--------+  +-------v---------+
| FileDescriptor  |  | Native          |  | Native           |
| Socket          |  | (epoll_ctl等)   |  | (kevent等)       |
| IovArray        |  | LinuxSocket     |  |                  |
| Buffer          |  +--------+--------+  +-------+---------+
+--------+--------+           |                    |
         |           +--------v--------+  +-------v---------+
         |           | netty_epoll_     |  | netty_kqueue_   |
         |           | native.c         |  | native.c        |
         |           +--------+--------+  +-------+---------+
         |                    |                    |
+--------v--------------------v--------------------v---------+
|                   Linux / macOS 内核                        |
|          epoll / kqueue / socket / file I/O                |
+------------------------------------------------------------+
```

## 核心类分析

### 1. NativeLibraryLoader — 原生库加载器

`NativeLibraryLoader` 是 Netty 加载 JNI 原生库的核心工具类，实现了三级降级加载策略。

**文件位置**: `common/src/main/java/io/netty/util/internal/NativeLibraryLoader.java`

**加载策略** (`load` 方法):

```java
public static void load(String originalName, ClassLoader loader) {
    String mangledPackagePrefix = calculateMangledPackagePrefix();
    String name = mangledPackagePrefix + originalName;

    // 第一优先级：从 java.library.path 加载
    try {
        loadLibrary(loader, name, false);
        return;
    } catch (Throwable ex) { /* 降级 */ }

    // 第二优先级：从 classpath 资源 META-INF/native/ 加载
    String libname = System.mapLibraryName(name);
    String path = NATIVE_RESOURCE_HOME + libname;  // "META-INF/native/"
    URL url = getResource(path, loader);

    // 第三优先级：解压到临时文件后加载
    tmpFile = PlatformDependent.createTempFile(prefix, suffix, WORKDIR);
    // 从 URL 拷贝到临时文件
    // 尝试修补 macOS shading ID（仅 macOS）
    loadLibrary(loader, tmpFile.getPath(), true);
}
```

**三级加载策略详解**:

| 优先级 | 策略 | 适用场景 | 实现方式 |
|-------|------|---------|---------|
| 1 | `System.loadLibrary` | 系统已安装原生库 | `java.library.path` 搜索 |
| 2 | classpath 资源 | JAR 内嵌原生库 | `META-INF/native/` 目录 |
| 3 | 临时文件 | classpath 中有但无法直接加载 | 解压到 `WORKDIR` 后加载 |

**Shading 支持**:

```java
private static String calculateMangledPackagePrefix() {
    String maybeShaded = NativeLibraryLoader.class.getName();
    String expected = "io.netty.util.internal.NativeLibraryLoader";
    // 计算 shading 前缀，将 _ 转为 _1，. 转为 _
    return maybeShaded.substring(0, maybeShaded.length() - expected.length())
                      .replace("_", "_1")
                      .replace('.', '_');
}
```

**配置项**:

| 系统属性 | 默认值 | 说明 |
|---------|-------|------|
| `io.netty.native.workdir` | 系统临时目录 | 原生库临时文件存放目录 |
| `io.netty.native.deleteLibAfterLoading` | `true` | 加载后是否删除临时文件 |
| `io.netty.native.tryPatchShadedId` | `true` | macOS 上是否修补 shading ID |
| `io.netty.native.detectNativeLibraryDuplicates` | `true` | 是否检测重复原生库 |

### 2. FileDescriptor — 文件描述符抽象

`FileDescriptor` 是 Netty 对 Unix 文件描述符的 Java 封装，是所有原生 I/O 操作的基础。

**文件位置**: `transport-native-unix-common/src/main/java/io/netty/channel/unix/FileDescriptor.java`

**状态管理**（位图）:

```java
// 状态位定义
private static final int STATE_CLOSED_MASK = 1;           // bit 0: 已关闭
private static final int STATE_INPUT_SHUTDOWN_MASK = 1 << 1;  // bit 1: 输入关闭
private static final int STATE_OUTPUT_SHUTDOWN_MASK = 1 << 2; // bit 2: 输出关闭

volatile int state;  // 位图状态
final int fd;        // 底层文件描述符整数值
```

**核心 native 方法**:

| Java 方法 | JNI 签名 | 对应 C 函数 | 功能 |
|-----------|---------|------------|------|
| `open(String)` | `(Ljava/lang/String;)I` | `netty_unix_filedescriptor_open` | 打开文件 |
| `close(int)` | `(I)I` | `netty_unix_filedescriptor_close` | 关闭文件描述符 |
| `write(int, ByteBuffer, int, int)` | `(ILjava/nio/ByteBuffer;II)I` | `netty_unix_filedescriptor_write` | 写入数据 |
| `read(int, ByteBuffer, int, int)` | `(ILjava/nio/ByteBuffer;II)I` | `netty_unix_filedescriptor_read` | 读取数据 |
| `writev(int, ByteBuffer[], int, int, long)` | `(I[Ljava/nio/ByteBuffer;IIJ)J` | `netty_unix_filedescriptor_writev` | 聚合写入 |
| `newPipe()` | `()J` | `netty_unix_filedescriptor_newPipe` | 创建管道 |

**writev 的高效实现**（直接内存映射 iovec）:

```c
// netty_unix_filedescriptor.c - 直接构造 struct iovec 数组
static jlong netty_unix_filedescriptor_writev(JNIEnv* env, jclass clazz,
        jint fd, jobjectArray buffers, const jint offset, jint length, jlong maxBytesToWrite) {
    struct iovec iov[length];
    for (i = offset; i < num; ++i) {
        jobject bufObj = (*env)->GetObjectArrayElement(env, buffers, i);
        jint pos = (*env)->GetIntField(env, bufObj, posFieldId);
        jint limit = (*env)->GetIntField(env, bufObj, limitFieldId);
        iovptr->iov_base = (*env)->GetDirectBufferAddress(env, bufObj) + pos;
        iovptr->iov_len = (size_t)(limit - pos);
        (*env)->DeleteLocalRef(env, bufObj);  // 避免本地引用泄漏
    }
    return _writev(env, clazz, fd, iov, length);
}
```

### 3. Socket — Socket 操作 JNI 桥接

`Socket` 继承自 `FileDescriptor`，提供完整的 socket 操作 JNI 桥接。

**文件位置**: `transport-native-unix-common/src/main/java/io/netty/channel/unix/Socket.java`

**继承关系**:

```
FileDescriptor (fd 管理 + read/write)
    └── Socket (socket 专属操作: bind/listen/connect/accept/send/recv)
        └── LinuxSocket (Linux 特有选项: TCP_FASTOPEN/SO_REUSEPORT)
```

**核心 native 方法分类**:

| 类别 | 方法 | 功能 |
|------|------|------|
| 连接管理 | `bind`, `listen`, `connect`, `accept`, `shutdown` | TCP 连接生命周期 |
| 数据传输 | `send`, `recv`, `sendTo`, `recvFrom` | 数据收发 |
| 地址查询 | `remoteAddress`, `localAddress` | 获取对端/本端地址 |
| Socket 选项 | `setKeepAlive`, `setTcpNoDelay`, `setReusePort` 等 | 选项设置 |
| FD 传递 | `sendFd`, `recvFd` | Unix domain socket FD 传递 |

**JNI 方法注册表**（`netty_unix_socket.c`）:

```c
static const JNINativeMethod fixed_method_table[] = {
  { "shutdown", "(IZZ)I", (void *) netty_unix_socket_shutdown },
  { "bind", "(IZ[BII)I", (void *) netty_unix_socket_bind },
  { "listen", "(II)I", (void *) netty_unix_socket_listen },
  { "connect", "(IZ[BII)I", (void *) netty_unix_socket_connect },
  { "accept", "(I[B)I", (void *) netty_unix_socket_accept },
  // ... 约 50 个方法
};
```

### 4. IovArray — 聚合写入优化

`IovArray` 直接在直接内存中构造 `struct iovec` 数组，避免 JNI 调用中的数组拷贝开销。

**文件位置**: `transport-native-unix-common/src/main/java/io/netty/channel/unix/IovArray.java`

```java
public final class IovArray implements MessageProcessor {
    private static final int ADDRESS_SIZE = Buffer.addressSize();  // 8 (64位) 或 4 (32位)
    public static final int IOV_SIZE = 2 * ADDRESS_SIZE;          // struct iovec 大小
    private static final int MAX_CAPACITY = IOV_MAX * IOV_SIZE;   // 最大容量

    private final long memoryAddress;  // 直接内存地址
    private int count;                 // 当前 iov 条目数
    private long size;                 // 总字节数
}
```

**内存布局**（直接映射 struct iovec）:

```
+------------------+------------------+
|   iov_base (8B)  |   iov_len  (8B)  |  <- 单个 iovec 条目 (16 字节)
+------------------+------------------+
|   iov_base (8B)  |   iov_len  (8B)  |  <- 下一个条目
+------------------+------------------+
|        ...       |       ...        |
```

## 设计思想

### 1. 分层抽象与平台隔离

Netty 的 JNI 层采用了清晰的分层设计：

```
平台无关层 (transport-native-unix-common)
    ├── FileDescriptor  — 文件描述符抽象
    ├── Socket          — Socket 操作抽象
    ├── IovArray        — 聚合 I/O 抽象
    └── Buffer          — 直接内存操作抽象

平台相关层 (transport-native-epoll / transport-native-kqueue)
    ├── Native          — 平台特定 I/O 多路复用
    └── LinuxSocket     — 平台特定 Socket 选项
```

### 2. 错误码传递机制

Netty 的 JNI 层使用负数 errno 传递错误，避免 JNI 异常处理的开销：

```c
// C 层：返回负数 errno
static jint netty_unix_socket_bind(...) {
    if (bind(fd, (struct sockaddr*) &addr, addrSize) == -1) {
        return -errno;  // 返回负数错误码
    }
    return 0;
}
```

```java
// Java 层：根据错误码抛出异常
public static int ioResult(String method, int err) throws IOException {
    if (err == ERRNO_EAGAIN_NEGATIVE || err == ERRNO_EWOULDBLOCK_NEGATIVE) {
        return 0;  // 非阻塞，无数据可读
    }
    if (err == ERRNO_EINTR_NEGATIVE) {
        return 0;  // 被中断，重试
    }
    throw newIOException(method, err);  // 其他错误抛异常
}
```

### 3. 直接内存零拷贝

通过 `GetDirectBufferAddress` 获取 `DirectByteBuffer` 的原生地址，实现 Java 与 C 之间的零拷贝数据传递：

```c
// 直接操作 DirectByteBuffer 的底层内存
static jint netty_unix_filedescriptor_write(JNIEnv* env, jclass clazz,
        jint fd, jobject jbuffer, jint pos, jint limit) {
    return _write(env, clazz, fd,
        (*env)->GetDirectBufferAddress(env, jbuffer), pos, limit);
}
```

## 模块交互

### JNI 生命周期管理

```
JNI_OnLoad (库加载时)
    │
    ├── netty_jni_util_register_natives()  — 注册 native 方法
    ├── NETTY_JNI_UTIL_LOAD_CLASS()        — 加载 Java 类引用
    ├── NETTY_JNI_UTIL_GET_METHOD()        — 缓存方法 ID
    ├── NETTY_JNI_UTIL_GET_FIELD()         — 缓存字段 ID
    └── 检测系统能力 (epoll_pwait2 等)
    │
JNI_OnUnload (库卸载时)
    │
    ├── netty_jni_util_unregister_natives() — 注销 native 方法
    └── 释放全局引用和缓存的 ID
```

### 动态方法签名机制

对于返回自定义类型的 native 方法（如 `recvFrom` 返回 `DatagramSocketAddress`），Netty 使用动态签名构建：

```c
static JNINativeMethod* createDynamicMethodsTable(const char* packagePrefix) {
    // 构建包含 shading 前缀的完整类名
    NETTY_JNI_UTIL_PREPEND(packagePrefix,
        "io/netty/channel/unix/DatagramSocketAddress;", dynamicTypeName, error);
    NETTY_JNI_UTIL_PREPEND("(ILjava/nio/ByteBuffer;II)L",
        dynamicTypeName, dynamicMethod->signature, error);
    dynamicMethod->name = "recvFrom";
    dynamicMethod->fnPtr = (void *) netty_unix_socket_recvFrom;
}
```

## 关键流程

### 原生库加载流程

```
NativeLibraryLoader.load("netty_transport_native_epoll", classLoader)
    │
    ├── 1. calculateMangledPackagePrefix()
    │       └── 处理 shading 前缀（如 "com.foo.shaded_"）
    │
    ├── 2. 尝试 System.loadLibrary(name)
    │       └── 失败则继续
    │
    ├── 3. 从 classpath 加载 META-INF/native/lib{name}.so
    │       ├── macOS: 尝试 .jnilib 和 .dynlib 两种后缀
    │       └── 使用 SHA-256 检测重复库内容
    │
    ├── 4. 解压到临时文件
    │       ├── WORKDIR = io.netty.native.workdir || System.tmpdir
    │       └── macOS: 尝试修补 shading ID + 重新签名
    │
    ├── 5. loadLibrary(loader, tmpFile, absolute=true)
    │       ├── 优先通过目标 ClassLoader 的 helper 类加载
    │       └── 降级到 NativeLibraryUtil.loadLibrary
    │
    └── 6. 清理
            ├── DELETE_NATIVE_LIB_AFTER_LOADING=true: 立即删除
            └── 否则: 注册 JVM 退出时删除
```

### Socket 操作 JNI 调用链

```
Java: socket.send(buf, pos, limit)
    │
    ├── Socket.java: send(intValue(), buf, pos, limit)  [native]
    │
    ├── JNI 桥接: GetDirectBufferAddress(buf) + pos
    │
    ├── C: _send(env, clazz, fd, buffer, pos, limit)
    │       │
    │       └── do {
    │               res = send(fd, buffer + pos, limit - pos, 0);
    │           } while (res == -1 && errno == EINTR);
    │
    └── 返回: res >= 0 ? res : -errno
```

## 学习要点

1. **JNI 加载策略的降级设计**：三级加载策略确保在各种部署环境（系统安装、嵌入 JAR、容器环境）下都能正确加载原生库

2. **Shading 兼容性**：Maven Shade 插件会重命名包名，Netty 通过 `calculateMangledPackagePrefix()` 动态计算前缀，确保原生库能找到正确的 Java 类

3. **错误码而非异常**：JNI 层使用负数 errno 传递错误，比抛出 Java 异常更高效，适合高频调用的 I/O 操作

4. **直接内存操作**：`GetDirectBufferAddress` 获取原生指针后直接操作，避免了 JNI 中 `GetByteArrayElements` 等方法的拷贝开销

5. **本地引用管理**：在循环中频繁创建 JNI 对象时（如 `writev` 中遍历 ByteBuffer 数组），必须手动调用 `DeleteLocalRef` 释放本地引用，否则可能超出 JNI 本地引用表限制（默认 16 个）
