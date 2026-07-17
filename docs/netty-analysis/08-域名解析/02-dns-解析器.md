# Netty 域名解析（二）：DNS 解析器

## 概述

Netty 的 `resolver-dns` 模块实现了一套完全异步、非阻塞的 DNS 解析器，替代了 JDK 内置的阻塞式 `InetAddress.getByName()`。它直接基于 Netty 的 DatagramChannel 发送和接收 DNS 协议报文，支持递归查询、搜索域、CNAME 跟踪、多级缓存等完整的 DNS 解析能力。

本文的核心关注点：

- `DnsNameResolver` 如何通过 UDP/TCP 通道实现异步 DNS 查询
- `DnsResolveContext` 如何实现递归查询、搜索域扩展和 CNAME 跟踪
- 三层缓存体系（解析结果缓存、CNAME 缓存、权威服务器缓存）的设计
- DNS 服务器地址的管理与轮询策略

## 架构图

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         DnsAddressResolverGroup                             │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │  DnsNameResolverBuilder  ← 共享缓存配置                              │  │
│  │  ConcurrentMap<String, Promise> resolvesInProgress  ← 去重并发查询    │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                              │                                              │
│                    newResolver(EventLoop)                                    │
│                              │                                              │
│                              ▼                                              │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │  InflightNameResolver  ← 包装 DnsNameResolver，合并并发查询           │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           DnsNameResolver                                    │
│                           (extends InetNameResolver)                         │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ 核心组件                                                            │    │
│  │  - DnsCache resolveCache           ← 解析结果缓存                   │    │
│  │  - DnsCnameCache cnameCache        ← CNAME 映射缓存                 │    │
│  │  - AuthoritativeDnsServerCache     ← 权威 DNS 服务器缓存            │    │
│  │  - DnsServerAddressStreamProvider  ← DNS 服务器地址提供者            │    │
│  │  - HostsFileEntriesResolver        ← hosts 文件解析                  │    │
│  │  - DnsQueryContextManager          ← 查询上下文管理                  │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ 传输层                                                              │    │
│  │  ┌──────────────────────┐    ┌──────────────────────┐               │    │
│  │  │   DatagramChannel    │    │    SocketChannel     │               │    │
│  │  │   (UDP 主通道)       │    │    (TCP 回退)        │               │    │
│  │  └──────────┬───────────┘    └──────────┬───────────┘               │    │
│  │             │                           │                           │    │
│  │  DatagramDnsQueryEncoder   DnsQueryContext (TCP)                    │    │
│  │  DatagramDnsResponseDecoder                                        │    │
│  │  DnsResponseHandler                                                │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ 解析策略                                                            │    │
│  │  - ResolvedAddressTypes (IPV4_ONLY / IPV4_PREFERRED / ...)         │    │
│  │  - searchDomains[] (搜索域列表)                                     │    │
│  │  - ndots (绝对查询阈值)                                             │    │
│  │  - maxQueriesPerResolve (最大查询次数)                               │    │
│  │  - queryTimeoutMillis (查询超时)                                    │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                    ┌───────────────┼───────────────┐
                    ▼               ▼               ▼
           ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
           │DnsRecordResolve│DnsAddressRes.│ │ DnsResolve   │
           │   Context     │ │   Context    │ │   Context    │
           │(单条记录查询) │ │(地址解析)    │ │(抽象基类)    │
           └──────────────┘ └──────────────┘ └──────────────┘
                    │               │
                    ▼               ▼
           ┌──────────────────────────────┐
           │       DnsResolveContext       │
           │  ┌────────────────────────┐  │
           │  │ 递归查询状态机          │  │
           │  │  - allowedQueries      │  │
           │  │  - queriesInProgress   │  │
           │  │  - finalResult         │  │
           │  │  - triedCNAME          │  │
           │  │  - completeEarly       │  │
           │  └────────────────────────┘  │
           └──────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                         DNS 服务器地址管理                                   │
│                                                                             │
│  DnsServerAddresses (抽象)                                                  │
│      ├── SequentialDnsServerAddressStream  ← 顺序轮询                       │
│      ├── ShuffledDnsServerAddressStream    ← 随机洗牌                       │
│      ├── RotationalDnsServerAddresses      ← 轮转起始点                     │
│      └── SingletonDnsServerAddresses       ← 单地址                         │
│                                                                             │
│  DnsServerAddressStreamProvider (接口)                                      │
│      ├── DefaultDnsServerAddressStreamProvider  ← 系统 DNS 服务器           │
│      └── UnixResolverDnsServerAddressStreamProvider ← /etc/resolv.conf     │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 核心类分析

### 1. DnsNameResolver -- 核心 DNS 解析器

`DnsNameResolver` 继承自 `InetNameResolver`，是整个 DNS 解析模块的核心。它通过 Netty 的 `DatagramChannel` 发送 DNS 查询报文，并异步接收响应。

#### 1.1 初始化与配置

```java
// resolver-dns/src/main/java/io/netty/resolver/dns/DnsNameResolver.java
public class DnsNameResolver extends InetNameResolver {
    private final DnsCache resolveCache;
    private final AuthoritativeDnsServerCache authoritativeDnsServerCache;
    private final DnsCnameCache cnameCache;
    private final DnsServerAddressStream queryDnsServerAddressStream;
    private final long queryTimeoutMillis;
    private final int maxQueriesPerResolve;
    private final ResolvedAddressTypes resolvedAddressTypes;
    private final boolean recursionDesired;
    private final int maxPayloadSize;
    private final HostsFileEntriesResolver hostsFileEntriesResolver;
    private final DnsServerAddressStreamProvider dnsServerAddressStreamProvider;
    private final String[] searchDomains;
    private final int ndots;
    private final DnsRecordType[] resolveRecordTypes;
    private final SocketProtocolFamily preferredAddressType;
    private final boolean completeOncePreferredResolved;
    private final DnsResolveChannelProvider resolveChannelProvider;
    private final Bootstrap socketBootstrap;
    private final boolean retryWithTcpOnTimeout;
    private final int maxNumConsolidation;
    private final Map<DnsQuestion, Promise<AddressedEnvelope<...>>> inflightLookups;
}
```

构造函数中，根据 `ResolvedAddressTypes` 决定查询哪些记录类型：

```java
switch (this.resolvedAddressTypes) {
    case IPV4_ONLY:
        resolveRecordTypes = IPV4_ONLY_RESOLVED_RECORD_TYPES;    // [A]
        break;
    case IPV4_PREFERRED:
        resolveRecordTypes = IPV4_PREFERRED_RESOLVED_RECORD_TYPES; // [A, AAAA]
        break;
    case IPV6_ONLY:
        resolveRecordTypes = IPV6_ONLY_RESOLVED_RECORD_TYPES;    // [AAAA]
        break;
    case IPV6_PREFERRED:
        resolveRecordTypes = IPV6_PREFERRED_RESOLVED_RECORD_TYPES; // [AAAA, A]
        break;
}
```

#### 1.2 传输层初始化

构造函数中创建 UDP 通道的 `Bootstrap`：

```java
Bootstrap bootstrap = new Bootstrap()
        .channelFactory(channelFactory)
        .group(eventLoop)
        .handler(new ChannelInitializer<DatagramChannel>() {
            protected void initChannel(DatagramChannel ch) {
                ch.config().setRecvByteBufAllocator(new FixedRecvByteBufAllocator(maxPayloadSize));
                ch.pipeline().addLast(DATAGRAM_ENCODER, DATAGRAM_DECODER, responseHandler);
            }
        });
this.resolveChannelProvider = newProvider(datagramChannelStrategy, bootstrap, localAddress);
```

通道策略有两种：

- `ChannelPerResolver`：所有解析共享一个 DatagramChannel（默认）
- `ChannelPerResolution`：每次解析创建独立的 DatagramChannel

#### 1.3 解析流程 -- doResolve

```java
protected void doResolve(String inetHost, Promise<InetAddress> promise) throws Exception {
    doResolve(inetHost, EMPTY_ADDITIONALS, promise, resolveCache);
}

protected void doResolve(String inetHost, DnsRecord[] additionals,
                         Promise<InetAddress> promise, DnsCache resolveCache) {
    // 1. 空主机名 → 返回 loopback
    if (inetHost == null || inetHost.isEmpty()) {
        promise.setSuccess(loopbackAddress());
        return;
    }

    // 2. IP 地址字符串 → 直接转换，无需 DNS 查询
    final InetAddress address = NetUtil.createInetAddressFromIpAddressString(inetHost);
    if (address != null) {
        promise.setSuccess(address);
        return;
    }

    // 3. IDN 编码转换 (punycode)
    final String hostname = hostname(inetHost);

    // 4. 检查 hosts 文件
    InetAddress hostsFileEntry = resolveHostsFileEntry(hostname);
    if (hostsFileEntry != null) {
        promise.setSuccess(hostsFileEntry);
        return;
    }

    // 5. 检查缓存
    if (!doResolveCached(hostname, additionals, promise, resolveCache)) {
        // 6. 缓存未命中，发起 DNS 查询
        ChannelFuture f = resolveChannelProvider.nextResolveChannel(promise);
        doResolveNow(f, hostname, additionals, promise, resolveCache);
    }
}
```

这个方法展示了 Netty DNS 解析的优化路径：

1. **快速路径**：空主机名、IP 地址字符串直接返回，避免不必要的 DNS 查询
2. **本地优先**：先检查 hosts 文件（零网络开销）
3. **缓存命中**：从 `DnsCache` 获取缓存结果
4. **实际查询**：只有以上路径都未命中时，才发起网络查询

#### 1.4 缓存读取 -- doResolveCached

```java
private boolean doResolveCached(String hostname, DnsRecord[] additionals,
                                Promise<InetAddress> promise, DnsCache resolveCache) {
    final List<? extends DnsCacheEntry> cachedEntries = resolveCache.get(hostname, additionals);
    if (cachedEntries == null || cachedEntries.isEmpty()) {
        return false;
    }

    Throwable cause = cachedEntries.get(0).cause();
    if (cause == null) {
        // 成功缓存：按 preferred address type 选择最佳结果
        for (SocketProtocolFamily f : resolvedInternetProtocolFamilies) {
            for (int i = 0; i < numEntries; i++) {
                final DnsCacheEntry e = cachedEntries.get(i);
                if (addressType(f).isInstance(e.address())) {
                    trySuccess(promise, e.address());
                    return true;
                }
            }
        }
        return false;
    } else {
        // 失败缓存：直接返回缓存的异常
        tryFailure(promise, cause);
        return true;
    }
}
```

缓存按照 `resolvedInternetProtocolFamilies` 的优先顺序选择地址类型。例如 `IPV4_PREFERRED` 模式下，先查找 IPv4 地址，没有才用 IPv6。

#### 1.5 查询合并 -- inflightLookups

```java
// 在 doQuery 方法中
if (inflightLookups != null && (additionals == null || additionals.length == 0)) {
    Promise<...> inflight = inflightLookups.get(question);
    if (inflight != null) {
        // 已有相同查询在进行中，复用其结果
        inflight.addListener(f -> {
            if (f.isSuccess()) {
                ReferenceCountUtil.retain(result);
                promise.setSuccess(result);
            } else {
                if (isTimeoutError(cause)) {
                    // 超时则重新发起查询
                    doQueryNow(...);
                } else {
                    promise.setFailure(cause);
                }
            }
        });
        return castPromise;
    }
}
```

当 `maxNumConsolidation > 0` 时，相同 DNS 问题（相同域名、相同记录类型）的并发查询会被合并为一次实际查询。后续查询通过监听同一个 `Promise` 来获取结果。这在高并发场景下大幅减少了 DNS 查询次数。

#### 1.6 响应处理

内部类 `DnsResponseHandler` 处理 UDP 响应：

```java
private static final class DnsResponseHandler extends ChannelInboundHandlerAdapter {
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        final DatagramDnsResponse res = (DatagramDnsResponse) msg;
        final int queryId = res.id();

        // 根据 sender 地址和 queryId 查找对应的查询上下文
        final DnsQueryContext qCtx = queryContextManager.get(res.sender(), queryId);
        if (qCtx == null || qCtx.isDone()) {
            res.release();
            return;
        }

        // 截断标记处理：如果响应被截断，后续可回退到 TCP
        qCtx.finishSuccess(res, res.isTruncated());
    }
}
```

`DnsQueryContextManager` 通过 `(senderAddress, queryId)` 二元组来匹配请求和响应。每个发出的 DNS 查询都有一个唯一的 queryId，响应到达时根据 queryId 找到对应的 `DnsQueryContext`。

### 2. DnsNameResolverBuilder -- 构建器

```java
// resolver-dns/src/main/java/io/netty/resolver/dns/DnsNameResolverBuilder.java
public final class DnsNameResolverBuilder {
    volatile EventLoop eventLoop;
    private ChannelFactory<? extends DatagramChannel> datagramChannelFactory;
    private ChannelFactory<? extends SocketChannel> socketChannelFactory;
    private DnsCache resolveCache;
    private DnsCnameCache cnameCache;
    private AuthoritativeDnsServerCache authoritativeDnsServerCache;
    private Integer minTtl;
    private Integer maxTtl;
    private Integer negativeTtl;
    private long queryTimeoutMillis = -1;
    private ResolvedAddressTypes resolvedAddressTypes = DnsNameResolver.DEFAULT_RESOLVE_ADDRESS_TYPES;
    private boolean recursionDesired = true;
    private int maxQueriesPerResolve = -1;
    private int maxPayloadSize = 4096;
    private boolean optResourceEnabled = true;
    private HostsFileEntriesResolver hostsFileEntriesResolver = HostsFileEntriesResolver.DEFAULT;
    private DnsServerAddressStreamProvider dnsServerAddressStreamProvider;
    private String[] searchDomains;
    private int ndots = -1;
    private int maxNumConsolidation;
    // ...
}
```

关键配置项说明：

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `queryTimeoutMillis` | `/etc/resolv.conf` 的 timeout 值或 5s | 单次 DNS 查询超时 |
| `maxQueriesPerResolve` | `/etc/resolv.conf` 的 attempts 值或 8 | 单次解析的最大查询次数 |
| `maxPayloadSize` | 4096 | UDP 载荷大小，影响 EDNS0 |
| `recursionDesired` | true | 是否设置 RD 标志 |
| `ndots` | `/etc/resolv.conf` 的 ndots 值或 1 | 决定先尝试绝对查询还是搜索域查询 |
| `searchDomains` | 系统搜索域 | 搜索域列表 |

`build()` 方法中的缓存处理逻辑：

```java
public DnsNameResolver build() {
    // 如果用户设置了自定义缓存，TTL 参数被忽略
    if (resolveCache != null && (minTtl != null || maxTtl != null || negativeTtl != null)) {
        logger.debug("resolveCache and TTLs are mutually exclusive. TTLs are ignored.");
    }

    DnsCache resolveCache = getOrNewCache();    // 无自定义 → DefaultDnsCache(minTtl, maxTtl, negativeTtl)
    DnsCnameCache cnameCache = getOrNewCnameCache();
    AuthoritativeDnsServerCache authoritativeDnsServerCache = getOrNewAuthoritativeDnsServerCache();

    return new DnsNameResolver(...);
}
```

### 3. DnsCache 与 DefaultDnsCache -- DNS 缓存

#### 3.1 DnsCache 接口

```java
// resolver-dns/src/main/java/io/netty/resolver/dns/DnsCache.java
public interface DnsCache {
    void clear();
    boolean clear(String hostname);
    List<? extends DnsCacheEntry> get(String hostname, DnsRecord[] additionals);
    DnsCacheEntry cache(String hostname, DnsRecord[] additionals,
                        InetAddress address, long originalTtl, EventLoop loop);
    DnsCacheEntry cache(String hostname, DnsRecord[] additionals,
                        Throwable cause, EventLoop loop);
}
```

缓存接口设计的关键点：

- `get()` 方法接受 `additionals` 参数，`DefaultDnsCache` 的实现中，如果有 additional 记录则不使用缓存（因为 additional 可能影响结果）
- `cache()` 方法接受 `originalTtl`（DNS 服务器返回的 TTL）和 `EventLoop`（用于注册过期定时器）
- 同时支持缓存成功结果和失败结果

#### 3.2 DefaultDnsCache 实现

```java
// resolver-dns/src/main/java/io/netty/resolver/dns/DefaultDnsCache.java
public class DefaultDnsCache implements DnsCache {
    private final Cache<DefaultDnsCacheEntry> resolveCache = new Cache<DefaultDnsCacheEntry>() { ... };
    private final int minTtl;
    private final int maxTtl;
    private final int negativeTtl;

    public DnsCacheEntry cache(String hostname, DnsRecord[] additionals,
                               InetAddress address, long originalTtl, EventLoop loop) {
        DefaultDnsCacheEntry e = new DefaultDnsCacheEntry(hostname, address);
        if (maxTtl == 0 || !emptyAdditionals(additionals)) {
            return e;  // 不缓存
        }
        // TTL 裁剪：max(minTtl, min(maxTtl, originalTtl))
        resolveCache.cache(appendDot(hostname), e,
                Math.max(minTtl, (int) Math.min(maxTtl, originalTtl)), loop);
        return e;
    }
}
```

TTL 裁剪策略：DNS 服务器返回的 TTL 会被限制在 `[minTtl, maxTtl]` 范围内。这使得应用可以：
- 设置 `minTtl` 避免过于频繁的 DNS 查询
- 设置 `maxTtl` 确保 DNS 变更能及时生效
- 设置 `negativeTtl` 控制失败结果的缓存时长

缓存键统一追加尾部点号（`appendDot`），确保 `example.com` 和 `example.com.` 映射到同一缓存条目。

### 4. DnsServerAddresses -- DNS 服务器地址管理

```java
// resolver-dns/src/main/java/io/netty/resolver/dns/DnsServerAddresses.java
public abstract class DnsServerAddresses {
    // 顺序轮询
    public static DnsServerAddresses sequential(Iterable<? extends InetSocketAddress> addresses);

    // 随机洗牌
    public static DnsServerAddresses shuffled(Iterable<? extends InetSocketAddress> addresses);

    // 轮转起始点
    public static DnsServerAddresses rotational(Iterable<? extends InetSocketAddress> addresses);

    // 单地址
    public static DnsServerAddresses singleton(InetSocketAddress address);

    // 每次调用返回一个新的地址流
    public abstract DnsServerAddressStream stream();
}
```

三种主要策略的区别：

- **sequential**：严格顺序遍历，所有 `stream()` 实例从第一个地址开始
- **shuffled**：每次 `stream()` 返回时对地址列表做随机洗牌
- **rotational**：类似 sequential，但每个 `stream()` 从不同的起始位置开始（通过原子计数器实现），实现负载均衡

所有地址在创建时必须是已解析的（`!isUnresolved()`），未解析的地址会被拒绝。

### 5. DnsAddressResolverGroup -- 解析器组

```java
// resolver-dns/src/main/java/io/netty/resolver/dns/DnsAddressResolverGroup.java
public class DnsAddressResolverGroup extends AddressResolverGroup<InetSocketAddress> {
    private final DnsNameResolverBuilder dnsResolverBuilder;
    private final ConcurrentMap<String, Promise<InetAddress>> resolvesInProgress = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Promise<List<InetAddress>>> resolveAllsInProgress = new ConcurrentHashMap<>();

    public DnsAddressResolverGroup(DnsNameResolverBuilder dnsResolverBuilder) {
        this.dnsResolverBuilder = withSharedCaches(dnsResolverBuilder.copy());
    }

    private static DnsNameResolverBuilder withSharedCaches(DnsNameResolverBuilder dnsResolverBuilder) {
        // 所有 DnsNameResolver 实例共享同一份缓存
        return dnsResolverBuilder.resolveCache(dnsResolverBuilder.getOrNewCache())
                .cnameCache(dnsResolverBuilder.getOrNewCnameCache())
                .authoritativeDnsServerCache(dnsResolverBuilder.getOrNewAuthoritativeDnsServerCache());
    }

    protected NameResolver<InetAddress> newNameResolver(EventLoop eventLoop, ...) {
        DnsNameResolverBuilder builder = dnsResolverBuilder.copy();
        return builder.eventLoop(eventLoop).datagramChannelFactory(channelFactory)
                .nameServerProvider(nameServerProvider).build();
    }
}
```

关键设计：

1. **缓存共享**：`withSharedCaches()` 确保所有 EventLoop 上的 `DnsNameResolver` 实例共享同一份缓存。这是因为 DNS 解析结果与 EventLoop 无关，共享缓存可以最大化缓存命中率。

2. **InflightNameResolver 包装**：在 `newResolver()` 中，`DnsNameResolver` 被 `InflightNameResolver` 包装，后者使用 `resolvesInProgress` 和 `resolveAllsInProgress` 两个 `ConcurrentMap` 合并跨 EventLoop 的并发查询。

### 6. DnsResolveContext -- 递归查询状态机

`DnsResolveContext` 是 DNS 递归查询的核心引擎。它是一个抽象类，有两个具体子类：

- `DnsAddressResolveContext`：解析地址（A/AAAA 记录）
- `DnsRecordResolveContext`：解析任意 DNS 记录

#### 6.1 状态字段

```java
abstract class DnsResolveContext<T> {
    final DnsNameResolver parent;
    private final Channel channel;
    private final Promise<?> originalPromise;
    private final DnsServerAddressStream nameServerAddrs;
    private final String hostname;
    private final DnsRecordType[] expectedTypes;

    private final Set<Future<...>> queriesInProgress;  // 当前进行中的查询
    private List<T> finalResult;                        // 已收集的结果
    private int allowedQueries;                         // 剩余允许的查询次数
    private boolean triedCNAME;                         // 是否已尝试 CNAME
    private boolean completeEarly;                      // 是否可以提前完成
}
```

#### 6.2 搜索域处理

```java
void resolve(final Promise<List<T>> promise) {
    final String[] searchDomains = parent.searchDomains();
    if (searchDomains.length == 0 || parent.ndots() == 0 || StringUtil.endsWith(hostname, '.')) {
        // 无搜索域 / ndots=0 / FQDN（以点结尾） → 直接绝对查询
        internalResolve(hostname, promise);
    } else {
        // 判断是否先尝试绝对查询
        final boolean startWithoutSearchDomain = hasNDots();
        final String initialHostname = startWithoutSearchDomain
                ? hostname
                : hostname + '.' + searchDomains[0];

        // 按顺序尝试搜索域
        searchDomainPromise.addListener(new FutureListener<List<T>>() {
            private int searchDomainIdx = initialSearchDomainIdx;
            public void operationComplete(Future<List<T>> future) {
                if (future.isSuccess()) {
                    promise.trySuccess(future.getNow());
                } else if (searchDomainIdx < searchDomains.length) {
                    // 尝试下一个搜索域
                    doSearchDomainQuery(hostname + '.' + searchDomains[searchDomainIdx++], nextPromise);
                } else if (!startWithoutSearchDomain) {
                    // 所有搜索域都失败，最后尝试绝对查询
                    internalResolve(hostname, promise);
                } else {
                    promise.tryFailure(...);
                }
            }
        });
        doSearchDomainQuery(initialHostname, searchDomainPromise);
    }
}
```

搜索域处理逻辑遵循 `/etc/resolv.conf` 的行为规范：

1. 如果域名以 `.` 结尾（FQDN），直接进行绝对查询
2. 如果域名中的点数 >= `ndots`，先尝试绝对查询，失败后再逐个尝试搜索域
3. 如果域名中的点数 < `ndots`，先逐个尝试搜索域，最后尝试绝对查询

例如，配置 `searchDomains = ["example.com", "corp.local"]`，`ndots = 1`：

- 查询 `www`（0 个点 < 1）：先查 `www.example.com`，再查 `www.corp.local`，最后查 `www`
- 查询 `www.sub`（1 个点 >= 1）：先查 `www.sub`，再查 `www.sub.example.com`，再查 `www.sub.corp.local`
- 查询 `www.example.com.`（FQDN）：直接查 `www.example.com.`

#### 6.3 递归查询核心 -- query 方法

```java
private void query(final DnsServerAddressStream nameServerAddrStream,
                   final int nameServerAddrStreamIndex,
                   final DnsQuestion question,
                   final DnsQueryLifecycleObserver queryLifecycleObserver,
                   final boolean flush,
                   final Promise<List<T>> promise,
                   final Throwable cause) {
    // 终止条件检查
    if (completeEarly || nameServerAddrStreamIndex >= nameServerAddrStream.size() ||
            allowedQueries == 0 || originalPromise.isCancelled() || promise.isCancelled()) {
        tryToFinishResolve(...);
        return;
    }

    --allowedQueries;  // 消耗一次查询配额

    final InetSocketAddress nameServerAddr = nameServerAddrStream.next();
    if (nameServerAddr.isUnresolved()) {
        // DNS 服务器地址本身未解析 → 先解析服务器地址
        queryUnresolvedNameServer(...);
        return;
    }

    // 发送 DNS 查询
    final Future<AddressedEnvelope<DnsResponse, InetSocketAddress>> f =
            parent.doQuery(channel, nameServerAddr, question, ...);

    queriesInProgress.add(f);

    f.addListener(future -> {
        queriesInProgress.remove(future);
        if (queryCause == null) {
            onResponse(nameServerAddrStream, index, question, future.getNow(), ...);
        } else {
            // 服务器未响应，尝试下一个
            query(nameServerAddrStream, index + 1, question, ...);
        }
    });
}
```

查询逻辑的关键点：

1. **查询配额控制**：每次查询消耗 `allowedQueries`，防止无限递归
2. **服务器轮询**：当前服务器无响应时，自动尝试下一个
3. **未解析的服务器地址**：如果 DNS 服务器地址本身是域名（如 NS 记录返回的），需要先递归解析它
4. **超时/取消检查**：每次迭代检查 promise 是否已完成

#### 6.4 响应处理 -- onResponse

```java
private void onResponse(DnsServerAddressStream nameServerAddrStream, int index,
                        DnsQuestion question, AddressedEnvelope<DnsResponse, InetSocketAddress> envelope,
                        DnsQueryLifecycleObserver queryLifecycleObserver, Promise<List<T>> promise) {
    final DnsResponse res = envelope.content();
    final DnsResponseCode code = res.code();

    if (code == DnsResponseCode.NOERROR) {
        // 1. 检查是否是重定向（NS 记录响应）
        if (handleRedirect(question, envelope, queryLifecycleObserver, promise)) {
            return;
        }
        // 2. CNAME 类型查询
        if (type == DnsRecordType.CNAME) {
            onResponseCNAME(question, buildAliasMap(...), ...);
            return;
        }
        // 3. A/AAAA 类型查询
        onExpectedResponse(question, envelope, ...);
        return;
    }

    // 非 NOERROR 响应
    if (code != NXDOMAIN) {
        // 非 NXDOMAIN → 尝试下一个服务器
        query(nameServerAddrStream, index + 1, question, ...);
    } else {
        // NXDOMAIN → 仅在非权威响应时尝试下一个服务器
        if (!res.isAuthoritativeAnswer()) {
            query(nameServerAddrStream, index + 1, question, ...);
        } else {
            // 权威 NXDOMAIN → 域名不存在
            tryToFinishResolve(...);
        }
    }
}
```

#### 6.5 重定向处理 -- handleRedirect

当 DNS 服务器不直接返回答案，而是返回 NS 记录指向其他权威服务器时，触发重定向：

```java
private boolean handleRedirect(DnsQuestion question, AddressedEnvelope<DnsResponse, InetSocketAddress> envelope,
                               DnsQueryLifecycleObserver queryLifecycleObserver, Promise<List<T>> promise) {
    if (res.count(DnsSection.ANSWER) == 0) {
        // 提取 AUTHORITY 段中的 NS 记录
        AuthoritativeNameServerList serverNames = extractAuthoritativeNameServers(question.name(), res);
        if (serverNames != null) {
            // 从 ADDITIONAL 段提取 NS 服务器的 IP 地址
            for (int i = 0; i < additionalCount; i++) {
                serverNames.handleWithAdditional(parent, r, authoritativeDnsServerCache);
            }
            // 处理没有 ADDITIONAL 记录的 NS（需要额外解析）
            serverNames.handleWithoutAdditionals(parent, resolveCache(), authoritativeDnsServerCache);

            // 构建新的服务器地址流并继续查询
            DnsServerAddressStream serverStream = parent.newRedirectDnsServerStream(question.name(), addresses);
            query(serverStream, 0, question, ...);
            return true;
        }
    }
    return false;
}
```

重定向过程中会缓存权威 DNS 服务器地址到 `AuthoritativeDnsServerCache`，下次查询相同域名时可以直接从缓存获取权威服务器，跳过根服务器的迭代。

#### 6.6 CNAME 跟踪

CNAME 跟踪是 DNS 解析中最复杂的部分之一。Netty 的实现包含两个层面：

**响应内的 CNAME 链**（`buildAliasMap`）：

```java
private static Map<String, String> buildAliasMap(String queryName, DnsResponse response,
                                                  DnsCnameCache cache, EventLoop loop) {
    Map<String, String> cnames = null;
    for (int i = 0; i < answerCount; i++) {
        final DnsRecord r = response.recordAt(DnsSection.ANSWER, i);
        if (r.type() != DnsRecordType.CNAME) continue;

        String name = r.name().toLowerCase(Locale.US);
        String mapping = domainName.toLowerCase(Locale.US);

        // 缓存 CNAME 映射
        cache.cache(nameWithDot, mappingWithDot, r.timeToLive(), loop);
        cnames.put(name, mapping);
    }
    return cnames;
}
```

一次 DNS 响应中可能包含完整的 CNAME 链（如 `www → www.cdn → cdn.host`），`buildAliasMap` 将其构建为映射表，后续的 `onExpectedResponse` 通过遍历映射表来验证响应中的地址记录是否与查询域名匹配。

**跨查询的 CNAME 跟踪**（`followCname`）：

```java
private void followCname(DnsQuestion question, String cname,
                         DnsQueryLifecycleObserver queryLifecycleObserver, Promise<List<T>> promise) {
    // 先从 CNAME 缓存中解析到最终域名
    cname = cnameResolveFromCache(cnameCache(), cname);
    // 获取最终域名的权威 DNS 服务器
    DnsServerAddressStream stream = getNameServers(cname);
    // 创建新的 DNS 问题并查询
    DnsQuestion cnameQuestion = new DefaultDnsQuestion(cname, question.type(), dnsClass);
    query(stream, 0, cnameQuestion, ...);
}
```

CNAME 缓存中的环路检测使用了 Floyd 龟兔算法：

```java
static String cnameResolveFromCacheLoop(DnsCnameCache cnameCache, String hostname,
                                         String first, String mapping) {
    boolean advance = false;
    String name = mapping;
    while ((mapping = cnameCache.get(hostnameWithDot(name))) != null) {
        checkCnameLoop(hostname, first, mapping);
        name = mapping;
        if (advance) {
            first = cnameCache.get(first);  // 慢指针每两步走一步
        }
        advance = !advance;
    }
    return name;
}
```

#### 6.7 查询完成判定 -- tryToFinishResolve

```java
private void tryToFinishResolve(DnsServerAddressStream nameServerAddrStream, int index,
                                DnsQuestion question, DnsQueryLifecycleObserver observer,
                                Promise<List<T>> promise, Throwable cause) {
    // 仍有查询在进行中 → 等待
    if (!completeEarly && !queriesInProgress.isEmpty()) {
        return;
    }

    if (finalResult == null) {
        // 无结果
        if (index < nameServerAddrStream.size()) {
            // 还有服务器可以尝试
            query(nameServerAddrStream, index + 1, question, ...);
            return;
        }

        // 最后手段：尝试 CNAME 查询
        if (TRY_FINAL_CNAME_ON_ADDRESS_LOOKUPS && !triedCNAME &&
                (question.type() == DnsRecordType.A || question.type() == DnsRecordType.AAAA)) {
            triedCNAME = true;
            query(hostname, DnsRecordType.CNAME, getNameServers(hostname), true, promise);
            return;
        }
    }

    finishResolve(promise, cause);
}
```

当所有服务器都尝试完毕且没有结果时，作为最后手段会尝试查询 CNAME 记录。这是因为某些 DNS 服务配置不规范，A/AAAA 查询可能返回 NXDOMAIN，但 CNAME 查询可能成功。这个行为通过 `io.netty.resolver.dns.tryCnameOnAddressLookups` 系统属性控制。

## 设计思想

### 1. 异步非阻塞的 DNS 协议栈

Netty 的 DNS 解析器不依赖 JDK 的 `InetAddress.getByName()`，而是直接实现了 DNS 协议（RFC 1035）的编解码：

```
应用层: resolve("www.example.com")
    │
    ▼
DnsNameResolver: 构建 DNS 查询报文
    │
    ▼
DatagramDnsQueryEncoder: 编码为 UDP 数据报
    │
    ▼
DatagramChannel: 发送 UDP 包到 DNS 服务器
    │
    ▼  (异步)
DnsResponseHandler.channelRead(): 接收响应
    │
    ▼
DatagramDnsResponseDecoder: 解码响应报文
    │
    ▼
DnsQueryContext: 匹配请求与响应，通知 Promise
```

### 2. 三层缓存体系

```
┌─────────────────────────────────────────────┐
│           DnsCache (resolveCache)            │  ← 解析结果缓存
│  key: hostname.  → [DnsCacheEntry, ...]     │     (A/AAAA 地址)
│  TTL: 受 minTtl/maxTtl 裁剪                 │
├─────────────────────────────────────────────┤
│           DnsCnameCache (cnameCache)         │  ← CNAME 映射缓存
│  key: alias.  → canonical.                  │     (域名别名)
│  TTL: 来自 CNAME 记录的 TTL                 │
├─────────────────────────────────────────────┤
│     AuthoritativeDnsServerCache              │  ← 权威服务器缓存
│  key: domain.  → DnsServerAddressStream      │     (NS 记录)
│  TTL: 来自 NS 记录的 TTL                    │
└─────────────────────────────────────────────┘
```

三层缓存各司其职：

- **解析结果缓存**：避免对同一域名的重复查询
- **CNAME 缓存**：避免对 CNAME 链的重复追踪
- **权威服务器缓存**：避免从根服务器开始的重复迭代

### 3. 传输层回退机制

Netty DNS 解析器支持 UDP 到 TCP 的回退（RFC 7766）：

1. 默认使用 UDP 发送查询
2. 当响应被标记为 `truncated` 时，使用 TCP 重试
3. 当配置 `retryWithTcpOnTimeout = true` 时，UDP 超时也会回退到 TCP

TCP 回退通过 `socketBootstrap` 创建 `SocketChannel` 实现。

### 4. 查询生命周期观察者

```java
DnsQueryLifecycleObserverFactory dnsQueryLifecycleObserverFactory
```

`DnsQueryLifecycleObserver` 接口允许外部监控每次 DNS 查询的生命周期事件：

- `querySucceed()` - 查询成功
- `queryFailed(cause)` - 查询失败
- `queryCancelled(allowedQueries)` - 查询被取消
- `queryRedirected(nameservers)` - 查询被重定向
- `queryCNAMEd(question)` - 查询遇到 CNAME
- `queryNoAnswer(code)` - 查询无答案

通过 `LoggingDnsQueryLifeCycleObserverFactory` 可以启用详细的查询日志，便于调试。

## 模块交互

### 解析器创建流程

```
DnsAddressResolverGroup(eventLoop, channelType, nameServerProvider)
    │
    │  构造时: withSharedCaches(builder.copy())
    │  → 所有 EventLoop 共享同一份缓存实例
    │
    ▼
newResolver(eventLoop)
    │
    ├── newNameResolver(eventLoop, channelFactory, nameServerProvider)
    │   │
    │   ├── builder.copy()  ← 复制配置
    │   ├── builder.eventLoop(eventLoop)
    │   └── builder.build()  → new DnsNameResolver(...)
    │
    ├── new InflightNameResolver(eventLoop, dnsNameResolver, resolvesInProgress, resolveAllsInProgress)
    │   └── 包装层：合并跨 EventLoop 的并发查询
    │
    └── new InetSocketAddressResolver(eventLoop, inflightNameResolver)
        └── 最终暴露为 AddressResolver<InetSocketAddress>
```

### 完整解析流程

```
resolve("www.example.com")
    │
    ├── 1. inetHost 非空，非 IP 字符串
    │
    ├── 2. hostname("www.example.com")
    │      → IDN.toASCII() 转换
    │
    ├── 3. resolveHostsFileEntry("www.example.com")
    │      → 检查 /etc/hosts → 未命中
    │
    ├── 4. doResolveCached("www.example.com.", ...)
    │      → 检查 DnsCache → 未命中
    │
    └── 5. doResolveUncached(channel, "www.example.com.", ...)
           │
           ├── 创建 DnsAddressResolveContext
           │
           └── ctx.resolve(promise)
               │
               ├── 搜索域处理 (ndots, searchDomains)
               │
               └── internalResolve("www.example.com", promise)
                   │
                   ├── cnameResolveFromCache(cnameCache, "www.example.com.")
                   │   → 无缓存 CNAME
                   │
                   ├── getNameServers("www.example.com")
                   │   → 检查 AuthoritativeDnsServerCache → 未命中
                   │   → 使用 DnsServerAddressStreamProvider 获取系统 DNS 服务器
                   │
                   ├── query("www.example.com", A, servers, ...)
                   │   │
                   │   ├── 构建 DnsQuestion("www.example.com", A)
                   │   ├── --allowedQueries
                   │   ├── parent.doQuery(channel, server, question, ...)
                   │   │   └── DatagramDnsQueryContext.writeQuery()
                   │   │       → UDP 发送 DNS 查询报文
                   │   │
                   │   └── f.addListener(future -> onResponse(...))
                   │
                   ├── [等待 UDP 响应]
                   │
                   └── onResponse(...)
                       │
                       ├── code == NOERROR
                       │   ├── handleRedirect() → 可能触发重定向查询
                       │   └── onExpectedResponse()
                       │       ├── buildAliasMap() → 提取 CNAME 链
                       │       ├── 遍历 ANSWER 段
                       │       ├── convertRecord() → InetAddress
                       │       ├── cache() → 缓存结果
                       │       └── trySuccess(promise, result)
                       │
                       └── code == NXDOMAIN
                           └── 尝试下一个服务器 / 失败
```

## 关键流程

### DNS 递归查询状态机

```
                    ┌─────────────┐
                    │   resolve()  │
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
                    │搜索域处理    │
                    │(ndots 判断)  │
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
                    │internalResolve│
                    │CNAME缓存检查  │
                    └──────┬──────┘
                           │
              ┌────────────┼────────────┐
              ▼            ▼            ▼
        ┌──────────┐ ┌──────────┐ ┌──────────┐
        │query(A)  │ │query(AAAA│ │query(... │
        └────┬─────┘ └────┬─────┘ └────┬─────┘
             │            │            │
             └────────────┼────────────┘
                          │
                   ┌──────▼──────┐
                   │ onResponse() │
                   └──────┬──────┘
                          │
           ┌──────────────┼──────────────┐
           ▼              ▼              ▼
    ┌────────────┐ ┌────────────┐ ┌────────────┐
    │  NOERROR   │ │  NXDOMAIN  │ │  其他错误   │
    └─────┬──────┘ └─────┬──────┘ └─────┬──────┘
          │              │              │
          ▼              ▼              ▼
    ┌───────────┐  ┌───────────┐  ┌───────────┐
    │有 ANSWER? │  │权威响应?  │  │下一服务器  │
    └─────┬─────┘  └─────┬─────┘  └───────────┘
     是/│  \否      是/│  \否
        ▼    ▼         ▼    ▼
    ┌──────┐┌─────┐ ┌────┐┌──────────┐
    │提取  ││重定向│ │失败││下一服务器 │
    │结果  ││查询  │ └────┘└──────────┘
    └──────┘└─────┘
        │
        ▼
    ┌───────────┐
    │CNAME需要? │
    │followCname│
    └───────────┘
        │
        ▼
    ┌───────────┐
    │tryToFinish│
    │Resolve()  │
    └───────────┘
        │
   ┌────┼────┐
   ▼    ▼    ▼
 有结果 无结果 还有服务器
   │     │    │
   ▼     ▼    ▼
 完成  尝试CNAME  继续查询
```

### 缓存层次查找顺序

```
DnsNameResolver.doResolve(hostname)
    │
    ├── hostsFileEntriesResolver.address(hostname)
    │   └── /etc/hosts 或 Windows hosts 文件
    │
    ├── resolveCache.get(hostname, additionals)
    │   └── DefaultDnsCache → ConcurrentMap
    │
    ├── cnameCache.get(hostname)
    │   └── DefaultDnsCnameCache → 跟踪 CNAME 链到最终域名
    │
    ├── authoritativeDnsServerCache.get(hostname)
    │   └── 从缓存获取权威 DNS 服务器，避免从根服务器迭代
    │
    └── dnsServerAddressStreamProvider.nameServerAddressStream(hostname)
        └── 系统 DNS 服务器 (/etc/resolv.conf 或平台默认)
```

## 学习要点

1. **完整 DNS 协议实现**：Netty 的 DNS 解析器是 DNS 协议的完整实现，而非简单封装 JDK。它直接编解码 DNS 报文（RFC 1035），支持 UDP/TCP 传输（RFC 7766），处理 EDNS0 扩展（RFC 6891），实现搜索域（/etc/resolv.conf 行为）和 CNAME 跟踪。

2. **异步递归查询状态机**：`DnsResolveContext` 通过 `queriesInProgress` 集合和回调链实现了异步递归。每次查询发起后不阻塞，而是注册回调。当所有进行中的查询都完成或超时后，通过 `tryToFinishResolve` 汇总结果。这种模式将传统的递归算法转换为了事件驱动的状态机。

3. **缓存一致性**：三层缓存共享同一份实例（通过 `DnsAddressResolverGroup.withSharedCaches`），确保不同 EventLoop 上的解析器看到一致的缓存状态。缓存条目的过期通过 EventLoop 的定时任务实现，避免了额外的清理线程。

4. **CNAME 环路检测**：使用 Floyd 龟兔算法检测 CNAME 缓存中的环路，时间复杂度 O(n)，空间复杂度 O(1)。这是一种经典的链表环路检测算法在 DNS 场景的应用。

5. **查询去重与合并**：`inflightLookups` 机制将相同 DNS 问题的并发查询合并为一次实际查询。这在微服务场景下（大量连接同时解析同一服务名）可以显著减少 DNS 查询量。

6. **Bailiwick 检查**：在缓存权威 DNS 服务器和 CNAME 记录时，Netty 实现了 bailiwick 检查（RFC 2181），只缓存属于查询域名子域的 NS 记录，防止缓存投毒攻击。

7. **可扩展的生命周期观察**：`DnsQueryLifecycleObserverFactory` 模式允许用户插入自定义的监控逻辑，例如 Prometheus 指标采集或分布式追踪。这是 Netty 中常见的可扩展性设计模式。
