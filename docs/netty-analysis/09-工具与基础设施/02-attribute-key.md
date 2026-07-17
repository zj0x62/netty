# AttributeKey 与 Channel 属性机制分析

## 概述

Netty 的属性系统允许在 `Channel`、`ChannelHandlerContext` 等对象上附加自定义的键值对数据，类似于 HTTP Header 或 Servlet Attribute 的概念。这套机制由以下核心组件构成：

- `AttributeKey<T>` -- 类型安全的属性键，全局唯一
- `Attribute<T>` -- 属性值容器，支持原子操作
- `AttributeMap` -- 属性存储接口
- `DefaultAttributeMap` -- 基于排序数组 + CAS 的高性能实现

与 Java 的 `ConcurrentHashMap` 方案不同，Netty 的属性系统采用了 **Copy-On-Write + 二分查找** 的设计，在读多写少的场景下提供了极高的并发性能。

## 架构图

```
┌─────────────────────────────────────────────────────────┐
│                    用户代码                               │
│  AttributeKey<String> KEY = AttributeKey.valueOf("key") │
│  channel.attr(KEY).set("value")                         │
└────────────────────────┬────────────────────────────────┘
                         │
          ┌──────────────┴──────────────┐
          ▼                             ▼
   ┌─────────────┐             ┌──────────────────┐
   │ AttributeKey │             │  AttributeMap    │
   │ (Constant)   │◄────────────│  (Interface)     │
   └──────┬──────┘             └────────┬─────────┘
          │                             │
   ┌──────┴──────┐             ┌────────┴─────────┐
   │ConstantPool │             │DefaultAttributeMap│
   │ (ConcurrentMap)│           │ (sorted array    │
   └─────────────┘             │  + CAS)          │
                               └──────────────────┘
                                        │
                               ┌────────┴─────────┐
                               │ DefaultAttribute  │
                               │ (AtomicReference) │
                               └──────────────────┘
```

## 核心类分析

### 1. AttributeKey -- 类型安全的属性键

`AttributeKey<T>` 继承自 `AbstractConstant`，通过 `ConstantPool` 保证全局唯一性。

```java
public final class AttributeKey<T> extends AbstractConstant<AttributeKey<T>> {
    private static final ConstantPool<AttributeKey<Object>> pool =
        new ConstantPool<AttributeKey<Object>>() {
            @Override
            protected AttributeKey<Object> newConstant(int id, String name) {
                return new AttributeKey<Object>(id, name);
            }
        };
}
```

#### valueOf vs newInstance 的区别

```java
// valueOf -- 获取或创建，幂等操作
AttributeKey<String> key1 = AttributeKey.valueOf("myKey");
AttributeKey<String> key2 = AttributeKey.valueOf("myKey");
// key1 == key2，始终返回同一个实例

// newInstance -- 必须是新名称，否则抛异常
AttributeKey<String> key3 = AttributeKey.newInstance("myKey");
// 抛出 IllegalArgumentException: 'myKey' is already in use
```

| 方法 | 行为 | 适用场景 |
|------|------|----------|
| `valueOf` | 已存在则返回，不存在则创建 | 通用场景，推荐使用 |
| `newInstance` | 必须不存在，否则抛异常 | 防止名称冲突，强制唯一性 |
| `exists` | 仅检查是否存在 | 探测性检查 |

#### ConstantPool 的实现

`ConstantPool` 使用 `ConcurrentHashMap` 存储已创建的常量：

```java
private final ConcurrentMap<String, T> constants = new ConcurrentHashMap<>();
private final AtomicInteger nextId = new AtomicInteger(1);

private T getOrCreate(String name) {
    T constant = constants.get(name);
    if (constant == null) {
        final T tempConstant = newConstant(nextId(), name);
        constant = constants.putIfAbsent(name, tempConstant);
        if (constant == null) {
            return tempConstant;  // 新创建成功
        }
    }
    return constant;  // 返回已有实例
}
```

注意：`nextId()` 分配的 `id` 在 `DefaultAttributeMap` 的二分查找中作为排序依据，因此即使 `putIfAbsent` 失败 (并发创建)，被丢弃的 id 也不会被回收，这保证了已分配 id 的单调递增性。

### 2. Attribute -- 属性值接口

`Attribute<T>` 是线程安全的属性值容器，提供原子操作：

```java
public interface Attribute<T> {
    AttributeKey<T> key();
    T get();                    // 读取当前值
    void set(T value);          // 设置值
    T getAndSet(T value);       // 原子交换
    T setIfAbsent(T value);     // 仅未设置时写入
    boolean compareAndSet(T oldValue, T newValue);  // CAS
    T getAndRemove();           // @Deprecated, 从 Map 中移除
    void remove();              // @Deprecated, 从 Map 中移除
}
```

### 3. DefaultAttributeMap -- 高性能属性存储

`DefaultAttributeMap` 是 Netty 4.2 中 `AttributeMap` 的默认实现，采用了与早期版本 (基于 `ConcurrentHashMap`) 完全不同的设计。

#### 核心数据结构

```java
public class DefaultAttributeMap implements AttributeMap {
    // 排序数组，按 AttributeKey.id() 升序排列
    private volatile DefaultAttribute[] attributes = EMPTY_ATTRIBUTES;

    // CAS 更新器
    private static final AtomicReferenceFieldUpdater<DefaultAttributeMap, DefaultAttribute[]>
        ATTRIBUTES_UPDATER = AtomicReferenceFieldUpdater.newUpdater(...);
}
```

#### 二分查找

```java
private static int searchAttributeByKey(DefaultAttribute[] sortedAttributes, AttributeKey<?> key) {
    int low = 0, high = sortedAttributes.length - 1;
    while (low <= high) {
        int mid = low + high >>> 1;
        DefaultAttribute midVal = sortedAttributes[mid];
        if (midVal.key == key) return mid;       // 引用比较，O(1)
        int midValKeyId = midVal.key.id();
        int keyId = key.id();
        if (midValKeyId < keyId) low = mid + 1;
        else high = mid - 1;
    }
    return -(low + 1);  // 未找到，返回插入点
}
```

查找优化点：
- 使用 `key.id()` (int) 比较而非字符串比较
- 使用 `key ==` 引用比较作为快速路径 (同一 `AttributeKey` 实例必然相同)

#### 属性写入 -- Copy-On-Write

```java
public <T> Attribute<T> attr(AttributeKey<T> key) {
    for (;;) {
        final DefaultAttribute[] attributes = this.attributes;
        final int index = searchAttributeByKey(attributes, key);

        if (index >= 0) {
            // 已存在：检查是否已移除
            DefaultAttribute attribute = attributes[index];
            if (!attribute.isRemoved()) return attribute;
            // 已移除：创建新属性替换
            newAttributes = Arrays.copyOf(attributes, count);
            newAttributes[index] = newAttribute;
        } else {
            // 不存在：创建新数组，有序插入
            newAttributes = new DefaultAttribute[count + 1];
            orderedCopyOnInsert(attributes, count, newAttributes, newAttribute);
        }

        // CAS 更新
        if (ATTRIBUTES_UPDATER.compareAndSet(this, attributes, newAttributes)) {
            return newAttribute;
        }
        // CAS 失败则重试
    }
}
```

#### 有序插入优化

```java
private static void orderedCopyOnInsert(DefaultAttribute[] sortedSrc, int srcLength,
                                         DefaultAttribute[] copy, DefaultAttribute toInsert) {
    final int id = toInsert.key.id();
    int i;
    // 从后向前遍历，因为新 key 的 id 通常更大
    for (i = srcLength - 1; i >= 0; i--) {
        if (sortedSrc[i].key.id() < id) break;
        copy[i + 1] = sortedSrc[i];
    }
    copy[i + 1] = toInsert;
    System.arraycopy(sortedSrc, 0, copy, 0, i + 1);
}
```

从后向前遍历是一个微优化：新创建的 `AttributeKey` 通常拥有更大的 `id` (因为 `nextId` 单调递增)，所以从尾部开始比较可以更快找到插入位置。

#### 属性移除

```java
// DefaultAttribute.remove()
public void remove() {
    // 1. 将 attributeMap 置 null，标记为已移除
    boolean removed = MAP_UPDATER.compareAndSet(this, attributeMap, null);
    set(null);
    // 2. 从数组中移除
    if (removed) {
        attributeMap.removeAttributeIfMatch(key, this);
    }
}
```

`removeAttributeIfMatch` 同样使用 CAS + Copy-On-Write：创建一个长度减1的新数组，通过两次 `System.arraycopy` 跳过被移除的元素。

### 4. DefaultAttribute -- 原子属性值

`DefaultAttribute` 是 `Attribute` 接口的实现，同时继承了 `AtomicReference<T>`：

```java
private static final class DefaultAttribute<T> extends AtomicReference<T>
        implements Attribute<T> {
    private volatile DefaultAttributeMap attributeMap;  // null 表示已移除
    private final AttributeKey<T> key;

    public T setIfAbsent(T value) {
        while (!compareAndSet(null, value)) {
            T old = get();
            if (old != null) return old;  // 已有值
        }
        return null;  // 设置成功
    }
}
```

利用 `AtomicReference` 的 CAS 能力实现无锁的线程安全操作。

## 设计思想

### 1. 为什么不用 ConcurrentHashMap？

早期 Netty 版本使用 `ConcurrentHashMap` 存储属性，但存在以下问题：

- 每个 `Channel` 都有一个属性 Map，大量 Channel 时内存开销大
- `ConcurrentHashMap` 的读操作虽然无锁，但需要计算 hash 和处理冲突
- 属性数量通常很少 (1-5个)，`ConcurrentHashMap` 的容量和负载因子设计不匹配

新方案使用 **排序数组 + 二分查找**：
- 数组内存紧凑，无额外节点对象开销
- 读操作 (二分查找) 无锁且缓存友好
- 写操作虽然需要 CAS + 数组复制，但属性数量少时开销可控

### 2. Copy-On-Write 的权衡

| 操作 | 复杂度 | 说明 |
|------|--------|------|
| 读 (attr/get) | O(log n) | 二分查找，无锁 |
| 写 (set) | O(1) | AtomicReference CAS |
| 新增属性 | O(n) | 数组复制 + 有序插入 |
| 移除属性 | O(n) | 数组复制 |

适用于 **读多写少** 的场景 -- 属性通常在连接建立时设置，之后只读取。

### 3. 延迟清理

属性移除分为两步：
1. `DefaultAttribute.attributeMap = null` -- 标记为已移除 (快速)
2. 从数组中物理移除 -- 在下次 `attr()` 调用时惰性完成

这避免了移除操作时的数组复制开销，因为后续的 `attr()` 调用无论如何都需要创建新数组。

## 模块交互

```
Channel / ChannelHandlerContext
    └── DefaultAttributeMap (implements AttributeMap)
            └── DefaultAttribute[] (sorted by AttributeKey.id)
                    └── DefaultAttribute extends AtomicReference<T>

AttributeKey
    └── ConstantPool<AttributeKey<Object>>
            └── ConcurrentHashMap<String, AttributeKey<Object>>

AbstractConstant
    └── id (from ConstantPool.nextId)
    └── uniquifier (from AtomicLong)
```

## 关键流程

### 属性读写完整流程

```
读取属性:
channel.attr(KEY).get()
    │
    ├── DefaultAttributeMap.attr(KEY)
    │   ├── 二分查找 sortedAttributes 数组
    │   ├── 找到 → 检查 isRemoved()
    │   │   ├── 未移除 → 返回已有 DefaultAttribute
    │   │   └── 已移除 → 创建新 DefaultAttribute, CAS 替换
    │   └── 未找到 → 创建新 DefaultAttribute, 有序插入, CAS 更新数组
    │
    └── DefaultAttribute.get()
        └── AtomicReference.get() -- 无锁读取

设置属性:
channel.attr(KEY).set("value")
    │
    └── DefaultAttribute.set("value")
        └── AtomicReference.set("value") -- volatile 写

CAS 设置:
channel.attr(KEY).setIfAbsent("value")
    │
    └── DefaultAttribute.setIfAbsent("value")
        ├── compareAndSet(null, "value")
        │   ├── 成功 → return null
        │   └── 失败 → get() 检查当前值
        │       ├── 非 null → return 当前值
        │       └── null → 重试 CAS
```

## 学习要点

1. **ConstantPool 模式** -- 使用 `ConcurrentHashMap` + `AtomicInteger` 实现全局唯一的常量池，`AttributeKey`、`ChannelHandlerMask` 等都使用了这种模式
2. **Copy-On-Write 思想** -- 读操作完全无锁，写操作通过数组复制保证线程安全，适合读多写少场景
3. **valueOf vs newInstance** -- 幂等获取 vs 强制创建，体现了"获取或创建"与"必须新建"两种设计意图
4. **排序数组 + 二分查找** -- 替代 `ConcurrentHashMap` 的轻量级方案，在元素数量少时更高效
5. **延迟清理** -- 属性移除分两步完成，减少不必要的数组复制
6. **引用比较优化** -- `AttributeKey` 的 `equals` 使用引用比较 (`==`)，因为同一名称的 key 始终是同一实例
