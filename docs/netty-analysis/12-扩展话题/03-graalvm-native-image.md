# GraalVM Native Image 支持

## 概述

GraalVM 的 Native Image 技术可以将 Java 应用编译为本地可执行文件，实现更快的启动速度和更低的内存占用。Netty 通过多种机制适配 GraalVM，确保框架在 AOT 编译环境下正常工作。

---

## 一、GraalVM Native Image 挑战

### 传统 JVM vs Native Image

| 特性 | JVM | Native Image |
|------|-----|--------------|
| 编译方式 | JIT（即时编译） | AOT（提前编译） |
| 启动速度 | 慢（类加载、JIT 预热） | 快（本地代码直接执行） |
| 内存占用 | 高（JVM 元数据） | 低（精简运行时） |
| 反射支持 | 原生支持 | 需要配置 |
| 动态代理 | 原生支持 | 需要配置 |
| 类初始化 | 运行时 | 构建时或运行时 |

### Netty 面临的问题

1. **反射调用**：Netty 大量使用反射创建 Channel、访问私有字段
2. **Unsafe 操作**：直接内存访问需要重新计算字段偏移
3. **静态初始化**：某些类在构建时初始化会失败
4. **JNI 调用**：原生传输层（epoll/io_uring）的 JNI 绑定

---

## 二、Netty 的 GraalVM 适配策略

### 2.1 Substitution 机制

Netty 使用 GraalVM 的 `@TargetClass` 和 `@Substitute` 注解，在 AOT 编译时替换特定实现。

#### 字段偏移重计算

```java
// PlatformDependent0Substitution.java
@TargetClass(className = "io.netty.util.internal.PlatformDependent0")
final class PlatformDependent0Substitution {
    @Alias
    @RecomputeFieldValue(
        kind = RecomputeFieldValue.Kind.FieldOffset,
        declClassName = "java.nio.Buffer",
        name = "address")
    private static long ADDRESS_FIELD_OFFSET;
}
```

**原理**：Native Image 中的对象布局与 JVM 不同，字段偏移需要重新计算。

#### 数组基址重计算

```java
// PlatformDependentSubstitution.java
@TargetClass(className = "io.netty.util.internal.PlatformDependent")
final class PlatformDependentSubstitution {
    @Alias
    @RecomputeFieldValue(
        kind = RecomputeFieldValue.Kind.ArrayBaseOffset,
        declClass = byte[].class)
    private static long BYTE_ARRAY_BASE_OFFSET;
}
```

#### 引用计数偏移重计算

```java
// RefCntSubstitution.java
@TargetClass(className = "io.netty.util.internal.RefCnt$UnsafeRefCnt")
final class RefCntSubstitution {
    @Alias
    @RecomputeFieldValue(
            kind = RecomputeFieldValue.Kind.FieldOffset,
            declClassName = "io.netty.util.internal.RefCnt",
            name = "value")
    public static long VALUE_OFFSET;
}
```

#### JCTools 队列适配

```java
// UnsafeRefArrayAccessSubstitution.java
@TargetClass(className = "io.netty.util.internal.shaded.org.jctools.util.UnsafeRefArrayAccess")
final class UnsafeRefArrayAccessSubstitution {
    @Alias
    @RecomputeFieldValue(
        kind = RecomputeFieldValue.Kind.ArrayIndexShift,
        declClass = Object[].class)
    public static int REF_ELEMENT_SHIFT;
}
```

### 2.2 延迟初始化

```java
// NetUtilSubstitutions.java
@TargetClass(NetUtil.class)
final class NetUtilSubstitutions {
    // 使用 Initialization-on-demand holder 模式
    // 将静态字段初始化延迟到运行时
    private static final class NetUtilLocalhost4LazyHolder {
        private static final Inet4Address LOCALHOST4 = NetUtilInitializations.createLocalhost4();
    }

    @Alias
    @InjectAccessors(NetUtilLocalhost4Accessor.class)
    public static Inet4Address LOCALHOST4;
}
```

**原理**：Native Image 构建时可能无法访问网络接口，需要延迟到运行时获取。

---

## 三、Native Image 配置文件

### 3.1 native-image.properties

```properties
# buffer/src/main/resources/META-INF/native-image/io.netty/netty-buffer/native-image.properties
Args = --initialize-at-run-time=io.netty.buffer.PooledByteBufAllocator,io.netty.buffer.AdaptiveByteBufAllocator,io.netty.buffer.ByteBufAllocator,io.netty.buffer.ByteBufUtil,io.netty.buffer.AbstractReferenceCountedByteBuf
```

**作用**：指定哪些类需要在运行时初始化（而非构建时）。

### 3.2 reflect-config.json

```json
// buffer/src/main/resources/META-INF/native-image/io.netty/netty-buffer/reflect-config.json
[
  {
    "name": "io.netty.buffer.AbstractByteBufAllocator",
    "queryAllDeclaredMethods": true
  },
  {
    "name": "io.netty.buffer.AbstractReferenceCountedByteBuf",
    "fields": [
      {
        "name": "refCnt"
      }
    ]
  },
  {
    "name": "io.netty.buffer.AdaptivePoolingAllocator$Chunk",
    "fields": [
      {
        "name": "refCnt"
      }
    ]
  }
]
```

**作用**：声明需要通过反射访问的类、方法和字段。

### 3.3 jni-config.json

```json
// codec-native-quic/src/main/resources/META-INF/native-image/io.netty/netty-codec-native-quic/jni-config.json
[
  {
    "name": "io.netty.handler.codec.quic.QuicCodec"
  }
]
```

**作用**：声明 JNI 调用的本地方法。

### 3.4 resource-config.json

```json
// 声明需要包含的资源文件
{
  "resources": {
    "includes": [
      {"pattern": "io/netty/.*\\.properties$"},
      {"pattern": "META-INF/native-image/.*"}
    ]
  }
}
```

---

## 四、testsuite-native-image 模块

### 模块结构

```
testsuite-native-image/
├── pom.xml
├── verify-native-image.sh
└── src/main/java/io/netty/testsuite/svm/
    ├── HttpNativeServer.java
    ├── HttpNativeClient.java
    ├── HttpNativeServerHandler.java
    └── HttpNativeServerInitializer.java
```

### 测试覆盖

```java
// HttpNativeServer.java — 测试所有传输类型和分配器组合
public static void main(String[] args) throws Exception {
    for (TransportType value : TransportType.values()) {
        for (AllocatorType allocatorType : AllocatorType.values()) {
            testTransport(value, allocatorType);
        }
    }
}

enum TransportType {
    NIO,      // Java NIO
    EPOLL,    // Linux epoll
    IO_URING  // Linux io_uring
}

enum AllocatorType {
    POOLED,    // PooledByteBufAllocator
    UNPOOLED,  // UnpooledByteBufAllocator
    ADAPTIVE   // AdaptiveByteBufAllocator
}
```

### Maven 配置

```xml
<!-- 使用 native-maven-plugin 编译 Native Image -->
<plugin>
    <groupId>org.graalvm.buildtools</groupId>
    <artifactId>native-maven-plugin</artifactId>
    <version>0.10.6</version>
    <executions>
        <execution>
            <id>http-server</id>
            <configuration>
                <imageName>native-image-http-server</imageName>
                <mainClass>io.netty.testsuite.svm.HttpNativeServer</mainClass>
                <buildArgs>
                    <arg>--report-unsupported-elements-at-runtime</arg>
                    <arg>--allow-incomplete-classpath</arg>
                    <arg>-Ob</arg>
                </buildArgs>
            </configuration>
        </execution>
    </executions>
</plugin>
```

### 验证脚本

```bash
#!/bin/bash
# verify-native-image.sh
# 执行编译后的 Native Image 并验证功能正确性

./native-image-http-server
if [ $? -eq 0 ]; then
    echo "Native Image HTTP Server test passed"
else
    echo "Native Image HTTP Server test failed"
    exit 1
fi
```

---

## 五、各模块的 GraalVM 支持

### 已适配模块

| 模块 | 配置文件 | 主要适配点 |
|------|----------|-----------|
| netty-common | reflect-config.json, native-image.properties | PlatformDependent 字段偏移 |
| netty-buffer | reflect-config.json, native-image.properties | ByteBuf 分配器运行时初始化 |
| netty-transport | reflect-config.json | Channel 反射创建 |
| netty-handler | reflect-config.json | SSL Provider 选择 |
| netty-codec-http | reflect-config.json | HTTP Handler 反射 |
| netty-codec-http2 | reflect-config.json | HTTP/2 Handler 反射 |
| netty-codec-native-quic | reflect-config.json, jni-config.json, resource-config.json | QUIC JNI 绑定 |
| netty-transport-classes-epoll | native-image.properties | Epoll 类运行时初始化 |
| netty-transport-classes-io_uring | native-image.properties | io_uring 类运行时初始化 |

### 配置生成方式

1. **手动编写**：针对已知的反射/JNI 使用
2. **Tracing Agent**：运行应用时自动收集
   ```bash
   java -agentlib:native-image-agent=config-output-dir=./native-image-configs -jar app.jar
   ```
3. **Build-time 生成**：通过 Maven/Gradle 插件自动收集

---

## 六、最佳实践

### 开发者指南

1. **使用延迟初始化**：
   ```java
   // 不推荐：构建时初始化
   private static final InetAddress LOCALHOST = InetAddress.getLocalHost();

   // 推荐：延迟到运行时
   private static final class LazyHolder {
       private static final InetAddress LOCALHOST = InetAddress.getLocalHost();
   }
   ```

2. **声明反射使用**：
   ```json
   // reflect-config.json
   {
     "name": "com.example.MyClass",
     "allDeclaredMethods": true,
     "allDeclaredFields": true
   }
   ```

3. **配置类初始化时机**：
   ```properties
   # native-image.properties
   Args = --initialize-at-run-time=com.example.MyClass
   ```

4. **测试 Native Image**：
   ```bash
   # 使用 testsuite-native-image 验证
   mvn -pl testsuite-native-image -Pnative-image-testsuite package
   ```

### 常见问题排查

| 问题 | 原因 | 解决方案 |
|------|------|----------|
| `UnresolvedElementException` | 方法在构建时不可用 | 添加 `--report-unsupported-elements-at-runtime` |
| `FieldOffsetError` | 字段偏移未重新计算 | 添加 `@RecomputeFieldValue` Substitution |
| `ClassNotFoundException` | 类未包含在 Native Image | 添加 reflect-config.json |
| `NoClassDefFoundError` | 类初始化失败 | 配置 `--initialize-at-run-time` |

---

## 学习要点

1. **Substitution 是核心**：通过 `@TargetClass` + `@RecomputeFieldValue` 适配 Unsafe 操作
2. **延迟初始化**：构建时无法完成的操作延迟到运行时
3. **配置文件声明**：反射、JNI、资源使用需要显式声明
4. **全面测试**：`testsuite-native-image` 模块测试所有传输类型和分配器组合
5. **持续演进**：随着 GraalVM 版本更新，适配策略也在不断改进
