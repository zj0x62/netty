# Netty Recycler 对象池深度分析

## 一、概述

`Recycler` 是 Netty 自研的轻量级对象池框架，核心目标是**通过对象复用来降低 GC 压力**。在高吞吐网络场景中，诸如 `ByteBuf`、`WriteTask`、`ChannelOutboundBuffer.Entry` 等对象会被频繁创建和销毁，`Recycler` 通过线程本地栈 + 跨线程回收队列的两级架构，在几乎无锁的前提下实现了高效的对象复用。

**核心设计哲学**：

- 同线程获取/归还走线程本地路径，零竞争
- 跨线程归还走 MPSC 队列，由拥有者线程批量消费
- 通过采样比率（ratio）控制池化比例，避免冷启动时的过度池化
- 提供 Guarded / Unguarded 两种模式，在安全性和性能之间做权衡

**系统属性配置**：

| 属性 | 默认值 | 说明 |
|---|---|---|
| `io.netty.recycler.maxCapacityPerThread` | 4096 | 每线程最大池化对象数 |
| `io.netty.recycler.ratio` | 8 | 采样比率，每 8 次 get 才池化 1 个新对象 |
| `io.netty.recycler.chunkSize` | 32 | 批量数组（batch）的大小 |
| `io.netty.recycler.blocking` | false | 是否使用阻塞队列（调试用） |
| `io.netty.recycler.batchFastThreadLocalOnly` | true | 是否仅对 FastThreadLocalThread 启用批量优化 |

## 二、架构图

### 2.1 整体类层次

```
Recycler<T>  (抽象类，对象池的入口)
│
├── get()                          -- 获取对象
├── newObject(Handle<T>)           -- 抽象方法，子类实现对象创建
│
├── [内部接口] Handle<T>            -- 对象回收句柄
│   └── [内部类] EnhancedHandle<T>  -- 增强句柄（增加 unguardedRecycle）
│       └── [内部类] DefaultHandle<T>   -- Guarded 模式的句柄（引用计数 + 状态校验）
│       └── [内部类] LocalPoolHandle<T> -- Unguarded 模式的句柄（轻量级）
│
├── [内部类] LocalPool<H, T>        -- 线程本地池（抽象基类）
│   ├── GuardedLocalPool<T>         -- 带状态校验的本地池
│   └── UnguardedLocalPool<T>       -- 无状态校验的本地池
│
└── [内部类] BlockingMessageQueue<T> -- 阻塞队列实现（调试用）
```

### 2.2 数据流架构

```
                    ┌─────────────────────────────────────────────┐
                    │              Thread A (Owner)                │
                    │                                             │
                    │   ┌───────────────────────────────────┐     │
                    │   │         LocalPool<H, T>           │     │
                    │   │                                   │     │
  get() ──────────► │   │  batch[] (栈式数组，chunkSize)     │     │
                    │   │  ┌─────┬─────┬─────┬─────┐        │     │
                    │   │  │ H[0]│ H[1]│ H[2]│ ... │        │     │
                    │   │  └─────┴─────┴─────┴─────┘        │     │
                    │   │  batchSize = 3                     │     │
                    │   │         ▲                         │     │
                    │   │         │ acquire: pop from top   │     │
                    │   │         │ release:  push to top   │     │
                    │   │         ▼                         │     │
                    │   │  pooledHandles (MPSC Queue)       │     │
                    │   │  ┌───┬───┬───┬───┐                │     │
                    │   │  │   │   │   │   │  (溢出存储)     │     │
                    │   │  └───┴───┴───┴───┘                │     │
                    │   └───────────────────────────────────┘     │
                    └─────────────────┬───────────────────────────┘
                                      │
                    ┌─────────────────▼───────────────────────────┐
                    │              Thread B (Other)                │
                    │                                             │
  recycle() ──────► │   归还对象 → 放入 Thread A 的 pooledHandles  │
  (跨线程)          │   (MPSC Queue 支持多生产者并发写入)           │
                    └─────────────────────────────────────────────┘
```

### 2.3 get() 获取流程

```
Recycler.get()
    │
    ├─ localPool != null ?  (共享模式 / Owner 绑定模式)
    │   └── localPool.getWith(this)
    │       ├── GuardedLocalPool:
    │       │   1. acquire() → 尝试从 batch[] 或 pooledHandles 取出 DefaultHandle
    │       │   2. handle != null → handle.claim() → 返回缓存对象
    │       │   3. handle == null → canAllocatePooled() ?
    │       │       ├─ true  → new DefaultHandle → recycler.newObject(handle)
    │       │       └─ false → recycler.newObject(NOOP_HANDLE)  (不池化)
    │       │
    │       └── UnguardedLocalPool:
    │           1. acquire() → 尝试从 batch[] 或 pooledHandles 取出对象 T
    │           2. obj != null → 直接返回
    │           3. obj == null → canAllocatePooled() ?
    │               ├─ true  → recycler.newObject(handle)  (复用 LocalPoolHandle)
    │               └─ false → recycler.newObject(NOOP_HANDLE)  (不池化)
    │
    └─ localPool == null ?  (ThreadLocal 模式)
        ├── 非 FastThreadLocalThread → newObject(NOOP_HANDLE) (直接创建，不池化)
        └── FastThreadLocalThread → threadLocalPool.get().getWith(this) (同上)
```

## 三、核心类分析

### 3.1 Recycler<T> —— 对象池主类

`Recycler` 是一个泛型抽象类，使用者通过继承它并实现 `newObject(Handle<T>)` 方法来定义对象的创建逻辑。

#### 构造函数体系

`Recycler` 提供了丰富的构造函数，覆盖不同的使用场景：

```java
// 1. 标准 ThreadLocal 模式（最常用）
protected Recycler()
protected Recycler(int maxCapacityPerThread)
protected Recycler(int maxCapacityPerThread, int interval, int chunkSize)

// 2. 共享池模式（所有线程共享一个池）
protected Recycler(int maxCapacity, boolean unguarded)

// 3. Owner 绑定模式（绑定到特定线程）
protected Recycler(Thread owner, boolean unguarded)
protected Recycler(int chunkSize, int maxCapacityPerThread, Thread owner, boolean unguarded)
```

**关键设计决策**：在私有构造函数中，通过 `useThreadLocalStorage` 参数决定使用 `FastThreadLocal` 还是直接持有一个 `LocalPool` 实例：

```java
private Recycler(int maxCapacityPerThread, int ratio, int chunkSize,
                 boolean useThreadLocalStorage, Thread owner, boolean unguarded) {
    if (maxCapacityPerThread > 0 && useThreadLocalStorage) {
        // 使用 FastThreadLocal，每个线程独立的 LocalPool
        threadLocalPool = new FastThreadLocal<LocalPool<?, T>>() { ... };
        localPool = null;
    } else {
        // 直接持有一个共享的 LocalPool（共享模式或 Owner 绑定模式）
        threadLocalPool = null;
        localPool = ...;
    }
}
```

#### get() 方法

```java
public final T get() {
    if (localPool != null) {
        // 共享模式 / Owner 绑定模式：直接使用实例级 localPool
        return localPool.getWith(this);
    } else {
        // ThreadLocal 模式
        if (!FastThreadLocalThread.currentThreadWillCleanupFastThreadLocals()) {
            // 非 FastThreadLocalThread，直接创建不池化
            return newObject((Handle<T>) NOOP_HANDLE);
        }
        // FastThreadLocalThread，从线程本地池获取
        return threadLocalPool.get().getWith(this);
    }
}
```

**设计要点**：对于非 `FastThreadLocalThread`，Netty 判断该线程退出时不会清理 ThreadLocal，如果池化会导致内存泄漏，因此直接返回 `NOOP_HANDLE`（对象不被池化）。

#### newObject() 抽象方法

```java
protected abstract T newObject(Handle<T> handle);
```

这是使用者唯一需要实现的方法。`Handle` 参数必须传递给创建出的对象，后续通过 `handle.recycle(object)` 归还对象。

### 3.2 Handle 体系 —— 对象回收句柄

#### Handle<T> 接口

```java
public interface Handle<T> extends ObjectPool.Handle<T> { }
```

最顶层的句柄接口，继承自 `ObjectPool.Handle<T>`，仅定义了 `recycle(T self)` 方法。

#### EnhancedHandle<T> 抽象类

```java
public abstract static class EnhancedHandle<T> implements Handle<T> {
    public abstract void unguardedRecycle(Object object);
}
```

增加了 `unguardedRecycle()` 方法，用于不校验对象状态的快速回收路径。

#### DefaultHandle<T> —— Guarded 模式句柄

```java
private static final class DefaultHandle<T> extends EnhancedHandle<T> {
    private static final int STATE_CLAIMED = 0;   // 已被获取使用
    private static final int STATE_AVAILABLE = 1;  // 可用（在池中）

    private volatile int state;
    private final GuardedLocalPool<T> localPool;
    private T value;
}
```

**核心方法**：

```java
// 回收对象
public void recycle(Object object) {
    if (object != value) {
        throw new IllegalArgumentException("object does not belong to handle");
    }
    toAvailable();          // CAS 设置状态为 AVAILABLE，防止重复回收
    localPool.release(this);
}

// 获取对象（从池中取出时调用）
T claim() {
    assert state == STATE_AVAILABLE;
    STATE_UPDATER.lazySet(this, STATE_CLAIMED);  // 设置为已使用
    return value;
}

// 状态转换：CAS 保证线程安全
private void toAvailable() {
    int prev = STATE_UPDATER.getAndSet(this, STATE_AVAILABLE);
    if (prev == STATE_AVAILABLE) {
        throw new IllegalStateException("Object has been recycled already.");
    }
}
```

**设计精髓**：

- 使用 `AtomicIntegerFieldUpdater` 而非 `AtomicInteger`，避免额外的对象包装开销
- `toAvailable()` 使用 `getAndSet` 原子操作，确保不会被重复回收
- `claim()` 使用 `lazySet` 而非 `set`，牺牲极小的可见性延迟换取更好的写入性能（安全的，因为有 happens-before 保证）

#### LocalPoolHandle<T> —— Unguarded 模式句柄

```java
private static final class LocalPoolHandle<T> extends EnhancedHandle<T> {
    private final UnguardedLocalPool<T> pool;

    public void recycle(T object) {
        pool.release(object);   // 直接归还，无状态校验
    }
}
```

**Unguarded 模式特点**：

- 所有从同一个 `UnguardedLocalPool` 获取的对象共享同一个 `LocalPoolHandle` 实例
- 不校验对象归属（不检查 `object != value`）
- 不做重复回收检测
- 性能更高，但要求调用者保证正确使用

### 3.3 LocalPool<H, T> —— 线程本地池

`LocalPool` 是整个 Recycler 的核心存储结构，采用了**两级存储**设计。

#### 字段解析

```java
private abstract static class LocalPool<H, T> {
    private final int ratioInterval;          // 采样间隔
    private final H[] batch;                  // 第一级：栈式数组（快速路径）
    private int batchSize;                    // 当前 batch 中的元素数量
    private Thread owner;                     // 拥有者线程
    private MessagePassingQueue<H> pooledHandles;  // 第二级：MPSC 队列（溢出/跨线程）
    private int ratioCounter;                 // 采样计数器
}
```

#### 两级存储机制

**第一级：batch[] 栈式数组**

```java
// 获取：从栈顶弹出
protected final H acquire() {
    int size = batchSize;
    if (size == 0) {
        // batch 为空，降级到第二级
        return pooledHandles.relaxedPoll();
    }
    int top = size - 1;
    final H h = batch[top];
    batchSize = top;
    batch[top] = null;  // 帮助 GC
    return h;
}

// 归还：压入栈顶
protected final void release(H handle) {
    Thread owner = this.owner;
    if (owner != null && Thread.currentThread() == owner && batchSize < batch.length) {
        // 快速路径：归还到 batch 栈
        batch[batchSize] = handle;
        batchSize++;
    } else if (owner != null && isTerminated(owner)) {
        // 拥有者线程已终止，清理资源
        pooledHandles = null;
        this.owner = null;
    } else {
        // 慢速路径：放入 MPSC 队列
        pooledHandles.relaxedOffer(handle);
    }
}
```

**设计要点**：

- `batch[]` 是一个固定大小的数组（chunkSize，默认 32），作为线程本地的快速栈
- 只有 owner 线程才能操作 batch[]，因此无需任何同步
- 当 batch 满时，溢出到 `pooledHandles`（MPSC 队列）
- 当 batch 空时，从 `pooledHandles` 批量补充（虽然当前代码只 poll 一个）

**第二级：pooledHandles（MPSC Queue）**

- 使用 JCTools 的 MPSC（Multi-Producer Single-Consumer）无锁队列
- 支持多线程并发写入（跨线程回收场景）
- 只有 owner 线程读取（单消费者）
- 当 owner 线程终止时，队列被清空释放

#### canAllocatePooled() —— 采样控制

```java
boolean canAllocatePooled() {
    if (ratioInterval < 0) return false;   // 禁用池化
    if (ratioInterval == 0) return true;   // 总是池化
    if (++ratioCounter >= ratioInterval) {
        ratioCounter = 0;
        return true;                        // 每 ratioInterval 次允许一次池化
    }
    return false;
}
```

**采样策略**：默认 `ratio = 8`，意味着每调用 8 次 `get()` 才允许 1 次将新对象放入池中。这样做的目的是：

- **避免冷启动过度池化**：系统刚启动时大量对象创建，如果全部池化会浪费内存
- **渐进扩容**：池的容量随使用量自然增长
- **自适应**：高频使用的对象类型自然获得更多池化配额

### 3.4 GuardedLocalPool<T> 与 UnguardedLocalPool<T>

#### GuardedLocalPool

```java
private static final class GuardedLocalPool<T> extends LocalPool<DefaultHandle<T>, T> {

    public T getWith(Recycler<T> recycler) {
        DefaultHandle<T> handle = acquire();
        T obj;
        if (handle == null) {
            // 池中无可用 handle
            handle = canAllocatePooled() ? new DefaultHandle<>(this) : null;
            if (handle != null) {
                obj = recycler.newObject(handle);
                handle.set(obj);        // 将对象绑定到 handle
            } else {
                obj = recycler.newObject((Handle<T>) NOOP_HANDLE);  // 不池化
            }
        } else {
            obj = handle.claim();       // 从 handle 中取出已有对象
        }
        return obj;
    }
}
```

**Guarded 模式特点**：

- 每个对象都有独立的 `DefaultHandle`，通过 `handle.set(value)` 绑定
- 回收时校验 `object == value`，防止错误归还
- 通过 CAS 状态机防止重复回收
- 存储在池中的是 `DefaultHandle`，而非裸对象

#### UnguardedLocalPool

```java
private static final class UnguardedLocalPool<T> extends LocalPool<T, T> {
    private final EnhancedHandle<T> handle;  // 共享的单一 handle

    public T getWith(Recycler<T> recycler) {
        T obj = acquire();
        if (obj == null) {
            obj = recycler.newObject(canAllocatePooled() ? handle : (Handle<T>) NOOP_HANDLE);
        }
        return obj;
    }
}
```

**Unguarded 模式特点**：

- 池中直接存储对象 T，而非 Handle 包装
- 所有对象共享同一个 `LocalPoolHandle` 实例
- 无需状态管理，回收路径更短
- 适合对性能极致敏感且能保证正确使用的场景

### 3.5 BlockingMessageQueue<T> —— 阻塞队列（调试用）

```java
private static final class BlockingMessageQueue<T> implements MessagePassingQueue<T> {
    private final Queue<T> deque;
    private final int maxCapacity;

    // 所有操作都 synchronized
    public synchronized boolean offer(T e) { ... }
    public synchronized T poll() { ... }
}
```

当 `io.netty.recycler.blocking = true` 时使用，基于 `ArrayDeque` + `synchronized` 实现，**仅用于调试目的**。选用 `ArrayDeque` 而非 `ArrayBlockingQueue` 的原因是后者会预分配最大容量的内存，而这些队列通常数量多（每线程一个）但实际使用量小。

## 四、设计思想

### 4.1 ThreadLocal 亲和性设计

Recycler 的核心思想是**线程亲和性**：对象在哪条线程创建，就尽量在哪条线程复用。

```
Thread A 创建对象 → 对象存入 Thread A 的 LocalPool → Thread A 再次 get() 时直接复用
                                                ↑
Thread B 使用完毕 → 对象放入 Thread A 的 pooledHandles ──┘
```

这种设计将锁竞争降到了最低：

- 同线程路径：完全无锁，仅操作线程本地的 batch[] 数组
- 跨线程路径：使用 MPSC 无锁队列，多线程可并发写入

### 4.2 两级存储的性能权衡

| 层级 | 数据结构 | 访问特性 | 适用场景 |
|---|---|---|---|
| batch[] | 固定大小数组，栈式操作 | 极快（数组下标访问） | 同线程高频存取 |
| pooledHandles | MPSC 无锁队列 | 较快（CAS 操作） | 溢出存储 + 跨线程归还 |

batch[] 的大小由 `chunkSize`（默认 32）控制，这意味着：

- 大多数场景下，同线程的获取/归还都在 batch[] 上完成，完全避免了队列操作
- 只有 batch 满/空时才与 pooledHandles 交互

### 4.3 Guarded vs Unguarded 的权衡

| 特性 | Guarded (DefaultHandle) | Unguarded (LocalPoolHandle) |
|---|---|---|
| 对象归属校验 | 是（`object != value` 检查） | 否 |
| 重复回收检测 | 是（CAS 状态机） | 否 |
| 内存开销 | 每对象一个 DefaultHandle 实例 | 所有对象共享一个 Handle |
| 性能 | 略低（CAS 操作） | 更高 |
| 适用场景 | 通用场景 | 性能极致敏感且能保证正确使用 |

### 4.4 采样比率的自适应扩容

ratio 机制实现了池的**渐进式扩容**：

```
初始状态：池为空
第 1 次 get() → 创建新对象，NOOP_HANDLE（不池化）
第 2 次 get() → 创建新对象，NOOP_HANDLE
...
第 8 次 get() → 创建新对象，放入池中（ratioCounter 达到 ratio）
第 9 次 get() → 创建新对象，NOOP_HANDLE
...
第 16 次 get() → 创建新对象，放入池中
...
```

这确保了池的增长速度与实际使用频率成正比，避免了一次性分配过多内存。

### 4.5 FastThreadLocalThread 的特殊处理

```java
if (!FastThreadLocalThread.currentThreadWillCleanupFastThreadLocals()) {
    return newObject((Handle<T>) NOOP_HANDLE);
}
```

Netty 的 `FastThreadLocalThread` 会在退出时主动清理所有 `FastThreadLocal` 关联的数据，因此可以安全地池化。而普通线程（如 JDK 线程池中的线程）使用 `ThreadLocal` 可能导致内存泄漏，所以直接使用 `NOOP_HANDLE` 放弃池化。

## 五、模块交互

### 5.1 Recycler 与 ByteBuf 分配器的协作

Recycler 在 Netty 中最核心的应用是 `PooledByteBuf` 的对象复用：

```java
// PooledDirectByteBuf.java
private static final Recycler<PooledDirectByteBuf> RECYCLER =
        new Recycler<PooledDirectByteBuf>() {
            @Override
            protected PooledDirectByteBuf newObject(Handle<PooledDirectByteBuf> handle) {
                return new PooledDirectByteBuf(handle, 0);
            }
        };

static PooledDirectByteBuf newInstance(int maxCapacity) {
    PooledDirectByteBuf buf = RECYCLER.get();
    // 重新初始化 buf 的字段...
    return buf;
}
```

`PooledByteBuf` 在 `deallocate()` 时通过 `recyclerHandle.recycle(this)` 将自身归还池中：

```java
// PooledByteBuf.java
protected final void deallocate() {
    if (handle >= 0) {
        final long h = this.handle;
        this.handle = -1;
        memory = null;
        // 归还 ByteBuf 对象到 Recycler
        recyclerHandle.recycle(this);
        // 归还底层内存到 PoolArena（另一套池化机制）
        chunk.arena.free(chunk, h, maxLength, cache);
    }
}
```

### 5.2 Recycler 与 WriteTask 的协作

网络写操作中频繁创建的 `WriteTask` 也通过 Recycler 池化：

```java
// AbstractChannelHandlerContext.java
static final class WriteTask implements Runnable {
    private static final Recycler<WriteTask> RECYCLER = new Recycler<WriteTask>() {
        @Override
        protected WriteTask newObject(Handle<WriteTask> handle) {
            return new WriteTask(handle);
        }
    };

    static WriteTask newInstance(...) {
        WriteTask task = RECYCLER.get();  // 从池中获取或新建
        init(task, ctx, msg, promise, flush);
        return task;
    }

    public void run() {
        // ... 执行写操作 ...
        // run 结束后，WriteTask 可被回收
    }
}
```

### 5.3 RecyclableArrayList 的使用模式

```java
public final class RecyclableArrayList extends ArrayList<Object> {
    private static final Recycler<RecyclableArrayList> RECYCLER = ...;

    public static RecyclableArrayList newInstance() {
        RecyclableArrayList ret = RECYCLER.get();
        ret.insertSinceRecycled = false;  // 重置状态
        return ret;
    }

    public boolean recycle() {
        if (insertSinceRecycled) {
            clear();  // 清空数据
        }
        return RECYCLER.recycle(this, handle);  // 归还到池中
    }
}
```

**典型使用模式总结**：

1. 定义 `static final Recycler<X> RECYCLER` 实例
2. 实现 `newObject(Handle<X>)` 构造对象，将 handle 保存到对象中
3. 提供 `static X newInstance()` 工厂方法，从池中获取并重置状态
4. 对象使用完毕后，调用 `handle.recycle(this)` 归还

## 六、关键流程详解

### 6.1 完整的 get() + recycle() 生命周期

以 `GuardedLocalPool` + `FastThreadLocalThread` 模式为例：

```
=== 首次获取 ===
1. recycler.get()
2. threadLocalPool.get() → 创建 GuardedLocalPool（FastThreadLocal.initialValue()）
3. localPool.getWith(recycler)
4. acquire() → batchSize=0, pooledHandles=null → 返回 null
5. canAllocatePooled() → ratioCounter(8) >= ratioInterval(8) → true, 重置为 0
6. new DefaultHandle<>(this)
7. recycler.newObject(handle) → 创建对象 obj
8. handle.set(obj)
9. 返回 obj

=== 后续获取（池中有对象）===
1. recycler.get()
2. threadLocalPool.get() → 获取已有 GuardedLocalPool
3. localPool.getWith(recycler)
4. acquire() → batchSize > 0 → 从 batch[] 栈顶弹出 handle
5. handle.claim() → lazySet STATE_CLAIMED → 返回缓存的 value

=== 回收对象（同线程）===
1. handle.recycle(obj)
2. 校验 object == value
3. toAvailable() → CAS 设置 state = STATE_AVAILABLE
4. localPool.release(handle)
5. Thread.currentThread() == owner → true
6. batchSize < batch.length → true
7. batch[batchSize] = handle; batchSize++
8. 完成（对象在 batch[] 中等待下次 get）

=== 回收对象（跨线程）===
1. handle.recycle(obj)
2. 校验 object == value
3. toAvailable() → CAS 设置 state = STATE_AVAILABLE
4. localPool.release(handle)
5. Thread.currentThread() == owner → false（其他线程）
6. pooledHandles.relaxedOffer(handle)  // 放入 MPSC 队列
7. 完成（对象在队列中等待 owner 线程下次 get 时消费）
```

### 6.2 FastThreadLocal 的 onRemoval 回调

当 `FastThreadLocalThread` 退出时：

```java
@Override
protected void onRemoval(LocalPool<?, T> value) throws Exception {
    super.onRemoval(value);
    MessagePassingQueue<?> handles = value.pooledHandles;
    value.pooledHandles = null;    // 置空引用
    value.owner = null;            // 清除 owner
    if (handles != null) {
        handles.clear();           // 清空队列，帮助 GC
    }
}
```

这确保了线程退出时不会泄漏池化对象。

### 6.3 Owner 线程终止检测

```java
protected final void release(H handle) {
    Thread owner = this.owner;
    if (owner != null && Thread.currentThread() == owner && batchSize < batch.length) {
        // 快速路径：同线程归还到 batch
        batch[batchSize] = handle;
        batchSize++;
    } else if (owner != null && isTerminated(owner)) {
        // Owner 线程已终止，清理资源
        pooledHandles = null;
        this.owner = null;
    } else {
        // 慢速路径：放入 MPSC 队列
        pooledHandles.relaxedOffer(handle);
    }
}

private static boolean isTerminated(Thread owner) {
    // J9 JVM 的 Thread.getState() 有性能问题，特殊处理
    return PlatformDependent.isJ9Jvm()
        ? !owner.isAlive()
        : owner.getState() == Thread.State.TERMINATED;
}
```

当检测到 owner 线程已终止时，会清空队列引用，防止已终止线程的池数据继续占用内存。

## 七、配置参数详解

### 7.1 maxCapacityPerThread

控制每个线程的 LocalPool 最大容量。设为 0 则完全禁用池化（退化为 `NOOP_LOCAL_POOL`）。

```java
if (maxCapacityPerThread <= 0) {
    maxCapacityPerThread = 0;
    chunkSize = 0;
} else {
    maxCapacityPerThread = max(4, maxCapacityPerThread);  // 最小为 4
    chunkSize = max(2, min(chunkSize, maxCapacityPerThread >> 1));
}
```

注意 chunkSize 被限制在 `[2, maxCapacityPerThread/2]` 范围内。

### 7.2 ratio（采样比率）

```java
RATIO = max(0, SystemPropertyUtil.getInt("io.netty.recycler.ratio", 8));
```

- 值为 0：每次 get 都尝试池化
- 值为 8（默认）：每 8 次 get 池化 1 个
- 值越大，池化率越低，内存占用越小

### 7.3 chunkSize（批处理大小）

```java
DEFAULT_QUEUE_CHUNK_SIZE_PER_THREAD = SystemPropertyUtil.getInt("io.netty.recycler.chunkSize", 32);
```

控制 `batch[]` 数组的大小，即线程本地快速栈的深度。较大的值减少与 MPSC 队列的交互频率，但增加内存占用。

### 7.4 BATCH_FAST_TL_ONLY

```java
BATCH_FAST_TL_ONLY = SystemPropertyUtil.getBoolean("io.netty.recycler.batchFastThreadLocalOnly", true);
```

当为 true 时，只有 `FastThreadLocalThread` 才会设置 `LocalPool.owner`，从而启用 batch[] 快速路径。普通线程只能使用 MPSC 队列路径。

## 八、学习要点

### 8.1 设计模式的运用

| 模式 | 应用位置 |
|---|---|
| **模板方法** | `Recycler.get()` 定义获取流程，子类通过 `newObject()` 定制创建逻辑 |
| **工厂方法** | `newObject(Handle<T>)` 是经典的工厂方法 |
| **对象池** | 整个 Recycler 就是对象池模式的实现 |
| **享元** | `NOOP_HANDLE` 和 `LocalPoolHandle` 被多个对象共享 |

### 8.2 并发编程技巧

1. **AtomicIntegerFieldUpdater 替代 AtomicInteger**：避免为每个 Handle 额外包装一个 AtomicInteger 对象
2. **lazySet 替代 set**：在 `claim()` 中使用 `lazySet`，因为后续操作有更强的 happens-before 保证
3. **MPSC 队列**：JCTools 的无锁队列实现，充分利用"单消费者"约束来优化性能
4. **relaxedOffer / relaxedPoll**：使用放宽语义的操作，进一步减少内存屏障开销

### 8.3 内存管理考量

1. **帮助 GC**：`batch[top] = null` 在弹出时主动清空数组引用
2. **线程退出清理**：`FastThreadLocal.onRemoval()` 回调确保资源释放
3. **Owner 终止检测**：`isTerminated()` 检测避免向已终止线程的队列写入
4. **NOOP 模式**：对不适合池化的场景（非 FastThreadLocalThread、maxCapacity=0），优雅退化

### 8.4 性能优化总结

| 优化手段 | 效果 |
|---|---|
| ThreadLocal 亲和性 | 消除同线程路径的锁竞争 |
| batch[] 栈式数组 | 用数组下标访问替代队列操作 |
| MPSC 无锁队列 | 跨线程归还时无锁 |
| 采样比率控制 | 避免过度池化，内存可控 |
| lazySet | 减少内存屏障，提升吞吐 |
| Guarded / Unguarded 双模式 | 按需选择安全性或性能 |

### 8.5 常见陷阱

1. **忘记调用 `handle.recycle()`**：对象不会被归还池中，等同于普通对象创建，且可能引起 `DefaultHandle` 状态泄漏
2. **重复调用 `handle.recycle()`**：Guarded 模式会抛出 `IllegalStateException`；Unguarded 模式行为未定义
3. **归还错误对象**：`object != value` 检查会抛出 `IllegalArgumentException`
4. **在非 FastThreadLocalThread 上使用**：不会池化，每次都创建新对象（退化为工厂方法）
5. **过度依赖池化**：池化仅适用于创建成本高、生命周期短的对象，不适合长生命周期对象
