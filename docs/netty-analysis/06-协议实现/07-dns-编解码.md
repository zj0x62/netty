# Netty DNS 协议实现深度分析

## 1. 概述

DNS（Domain Name System，域名系统）是互联网的基础设施协议，负责将人类可读的域名解析为机器可识别的 IP 地址。Netty 的 `codec-dns` 模块提供了完整的 DNS 协议编解码实现，覆盖 RFC 1035（基础 DNS）、RFC 2929（DNS IANA 考虑事项）、RFC 7766（DNS over TCP）等核心规范。

该模块的核心设计特点：

- **五段式报文结构**：Header + Question + Answer + Authority + Additional，每段通过 `DnsSection` 枚举独立管理
- **域名压缩指针**：支持 RFC 1035 Section 4.1.4 定义的域名压缩机制，通过指针引用减少报文大小
- **记录类型丰富**：支持 A、AAAA、CNAME、MX、NS、PTR、SRV、OPT 等 40+ 种记录类型
- **UDP/TCP 双模式**：UDP 使用 `DatagramPacket` 编解码，TCP 使用 2 字节长度前缀帧（RFC 7766）
- **引用计数管理**：所有 DNS 消息和记录都实现 `ReferenceCounted` 接口，配合 `ResourceLeakDetector` 防止内存泄漏
- **单记录优化**：每个 Section 在仅有一条记录时直接存储对象而非列表，减少内存开销

## 2. 架构图

### 2.1 模块整体架构

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         codec-dns 模块架构                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                       消息类型层次                                    │  │
│  │                                                                       │  │
│  │  DnsMessage (接口, extends ReferenceCounted)                          │  │
│  │  ├── DnsQuery (接口) ──┬── DefaultDnsQuery (TCP)                     │  │
│  │  │                     └── DatagramDnsQuery (UDP, 带地址信息)         │  │
│  │  └── DnsResponse (接口) ─┬── DefaultDnsResponse (TCP)                │  │
│  │                          └── DatagramDnsResponse (UDP, 带地址信息)    │  │
│  │                                                                       │  │
│  │  DnsRecord (接口)                                                    │  │
│  │  ├── DnsQuestion (接口) ──► DefaultDnsQuestion                       │  │
│  │  ├── DnsRawRecord (接口) ──► DefaultDnsRawRecord                     │  │
│  │  ├── DnsPtrRecord (接口) ──► DefaultDnsPtrRecord                     │  │
│  │  └── DnsOptPseudoRecord (接口) ──► DefaultDnsOptEcsRecord            │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                       DNS 报文结构                                    │  │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐   │  │
│  │  │  Header  │ │ Question │ │  Answer  │ │Authority │ │ Addition-│   │  │
│  │  │ (12 字节) │ │  (0~N)   │ │  (0~N)   │ │  (0~N)   │ │  al(0~N) │   │  │
│  │  └──────────┘ └──────────┘ └──────────┘ └──────────┘ └──────────┘   │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                       UDP 编解码层                                    │  │
│  │  DatagramDnsQueryEncoder   ──► DatagramPacket (出站)                 │  │
│  │  DatagramPacket            ──► DatagramDnsResponseDecoder (入站)     │  │
│  │  DatagramDnsQueryDecoder   ──► DatagramDnsQuery (入站, 服务端)       │  │
│  │  DatagramDnsResponseEncoder──► DatagramPacket (出站, 服务端)         │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                       TCP 编解码层                                    │  │
│  │  TcpDnsQueryEncoder        ──► 2字节长度前缀 + DNS 报文 (出站)       │  │
│  │  TcpDnsResponseDecoder     ──► LengthFieldBasedFrameDecoder (入站)   │  │
│  │  TcpDnsResponseEncoder     ──► 2字节长度前缀 + DNS 报文 (服务端出站) │  │
│  │  TcpDnsQueryDecoder        ──► LengthFieldBasedFrameDecoder (服务端入站)│ │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                       核心编解码器层                                  │  │
│  │  DnsQueryEncoder          ──► 编码 Header + Questions + Additionals  │  │
│  │  DnsResponseDecoder       ──► 解码 Header + 所有 Section             │  │
│  │  DnsMessageUtil           ──► 共享的编解码逻辑 (Query 解码/Response 编码)│ │
│  │  DefaultDnsRecordEncoder  ──► 编码域名 + 资源记录                    │  │
│  │  DefaultDnsRecordDecoder  ──► 解码域名 + 资源记录                    │  │
│  │  DnsCodecUtil             ──► 域名编码/解码/压缩解压工具             │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 DNS Header 16-bit Flags 布局

```
  15 14 13 12 11 10  9  8  7  6  5  4  3  2  1  0
+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
|QR|    OPCODE   |AA|TC|RD|RA|   Z    |   RCODE  |
+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+

QR     (1 bit) : 0=查询, 1=响应
OPCODE (4 bit) : 0=QUERY, 1=IQUERY, 2=STATUS, 4=NOTIFY, 5=UPDATE
AA     (1 bit) : 权威回答标志 (仅响应)
TC     (1 bit) : 截断标志 (报文超过 512 字节)
RD     (1 bit) : 期望递归
RA     (1 bit) : 可用递归 (仅响应)
Z      (3 bit) : 保留字段 (应为 0)
RCODE  (4 bit) : 响应码 (仅响应: 0=NoError, 3=NXDomain, ...)
```

## 3. 核心类分析

### 3.1 DnsMessage — DNS 消息顶层接口

`DnsMessage` 是所有 DNS 消息的顶层接口，继承自 `ReferenceCounted`，定义了报文头核心字段和 Section 操作：

```java
public interface DnsMessage extends ReferenceCounted {
    int id();                              // 报文标识符 (16 bit)
    DnsMessage setId(int id);
    DnsOpCode opCode();                    // 操作码 (4 bit)
    DnsMessage setOpCode(DnsOpCode opCode);
    boolean isRecursionDesired();          // RD 标志位
    DnsMessage setRecursionDesired(boolean recursionDesired);
    int z();                               // 保留字段 (3 bit)
    DnsMessage setZ(int z);

    // Section 统一操作接口
    int count(DnsSection section);
    <T extends DnsRecord> T recordAt(DnsSection section, int index);
    DnsMessage addRecord(DnsSection section, DnsRecord record);
    DnsMessage setRecord(DnsSection section, DnsRecord record);
    <T extends DnsRecord> T removeRecord(DnsSection section, int index);
    DnsMessage clear(DnsSection section);
}
```

`DnsQuery` 和 `DnsResponse` 分别扩展了 `DnsMessage`，其中 `DnsResponse` 额外定义了权威回答（AA）、截断（TC）、可用递归（RA）标志以及响应码（RCODE）。

### 3.2 DnsSection — 报文段枚举

```java
public enum DnsSection {
    QUESTION,    // 查询问题段 (QDCOUNT)
    ANSWER,      // 回答段 (ANCOUNT)
    AUTHORITY,   // 授权段 (NSCOUNT)
    ADDITIONAL   // 附加段 (ARCOUNT)
}
```

四个枚举值的 `ordinal()` 与 DNS Header 中的计数字段顺序一致，`AbstractDnsMessage` 直接使用 ordinal 值作为数组索引来定位存储字段。

### 3.3 AbstractDnsMessage — 消息基类的内存优化

`AbstractDnsMessage` 是消息的核心实现基类，继承自 `AbstractReferenceCounted`。其最显著的设计特点是**单记录/列表二态存储**：

```java
public abstract class AbstractDnsMessage extends AbstractReferenceCounted implements DnsMessage {
    // 每个 Section 的存储：null / DnsRecord 单对象 / List<DnsRecord> 列表
    private Object questions;
    private Object answers;
    private Object authorities;
    private Object additionals;

    private void addRecord(int section, DnsRecord record) {
        final Object records = sectionAt(section);
        if (records == null) {
            setSection(section, record);              // 第一条：直接存储对象
            return;
        }
        if (records instanceof DnsRecord) {
            final List<DnsRecord> recordList = new ArrayList<>(2);
            recordList.add(castRecord(records));      // 第二条：升级为列表
            recordList.add(record);
            setSection(section, recordList);
            return;
        }
        ((List<DnsRecord>) records).add(record);      // 后续：追加到列表
    }
}
```

这种设计在大多数 DNS 查询只有一条 Question 记录的场景下，避免了创建 `ArrayList` 的开销。`count()` 方法通过 `instanceof` 判断来返回正确的计数：

```java
private int count(int section) {
    final Object records = sectionAt(section);
    if (records == null) return 0;
    if (records instanceof DnsRecord) return 1;  // 单对象
    return ((List<DnsRecord>) records).size();    // 列表
}
```

引用计数管理方面，`deallocate()` 会遍历所有 Section 释放记录：

```java
@Override
protected void deallocate() {
    clear();  // 释放所有 Section 中的记录
    final ResourceLeakTracker<DnsMessage> leak = this.leak;
    if (leak != null) {
        boolean closed = leak.close(this);
        assert closed;
    }
}
```

### 3.4 AbstractDnsRecord — 资源记录基类

```java
public abstract class AbstractDnsRecord implements DnsRecord {
    private final String name;        // 域名 (已标准化)
    private final DnsRecordType type; // 记录类型
    private final short dnsClass;     // 记录类 (通常为 IN=0x0001)
    private final long timeToLive;    // TTL
}
```

构造时会执行两个关键操作：`IDN.toASCII()` 国际化域名转 ASCII 编码，以及 `appendTrailingDot()` 确保域名以 `.` 结尾。`equals()` 和 `hashCode()` 基于域名（忽略大小写）、类型和类三个字段。

### 3.5 DnsRecord / DnsQuestion — 记录接口层次

```java
public interface DnsRecord {
    int CLASS_IN    = 0x0001;  // 互联网类 (最常用)
    int CLASS_CSNET = 0x0002;
    int CLASS_CHAOS = 0x0003;
    int CLASS_HESIOD= 0x0004;
    int CLASS_NONE  = 0x00fe;
    int CLASS_ANY   = 0x00ff;

    String name();
    DnsRecordType type();
    int dnsClass();
    long timeToLive();
}

public interface DnsQuestion extends DnsRecord {
    @Override
    long timeToLive();  // Question 没有 TTL，固定返回 0
}
```

`DnsQuestion` 是 `DnsRecord` 的特殊子类型，位于 QUESTION 段，不含 TTL 和 RDATA。`AbstractDnsMessage.checkQuestion()` 确保 QUESTION 段只能添加 `DnsQuestion` 类型的记录。

### 3.6 DnsResponseDecoder — 响应解码核心

`DnsResponseDecoder` 是 DNS 响应解码的核心抽象类，通过模板方法模式让子类决定具体的 `DnsResponse` 实现：

```java
abstract class DnsResponseDecoder<A extends SocketAddress> {
    private final DnsRecordDecoder recordDecoder;

    final DnsResponse decode(A sender, A recipient, ByteBuf buffer) throws Exception {
        // 第一步：解析 12 字节 Header
        final int id = buffer.readUnsignedShort();
        final int flags = buffer.readUnsignedShort();

        // QR 位校验：必须为响应
        if (flags >> 15 == 0) {
            throw new CorruptedFrameException("not a response");
        }

        // 模板方法：子类创建具体的 DnsResponse 实例
        final DnsResponse response = newResponse(sender, recipient, id,
            DnsOpCode.valueOf((byte) (flags >> 11 & 0xf)),   // OPCODE [bit 14..11]
            DnsResponseCode.valueOf((byte) (flags & 0xf)));  // RCODE  [bit 3..0]

        // 解析标志位
        response.setRecursionDesired((flags >> 8 & 1) == 1);     // RD [bit 8]
        response.setAuthoritativeAnswer((flags >> 10 & 1) == 1); // AA [bit 10]
        response.setTruncated((flags >> 9 & 1) == 1);            // TC [bit 9]
        response.setRecursionAvailable((flags >> 7 & 1) == 1);   // RA [bit 7]
        response.setZ(flags >> 4 & 0x7);                         // Z  [bit 6..4]

        // 第二步：读取各段记录数
        final int questionCount = buffer.readUnsignedShort();         // QDCOUNT
        final int answerCount = buffer.readUnsignedShort();           // ANCOUNT
        final int authorityRecordCount = buffer.readUnsignedShort();  // NSCOUNT
        final int additionalRecordCount = buffer.readUnsignedShort(); // ARCOUNT

        // 第三步：依次解析四个段
        decodeQuestions(response, buffer, questionCount);
        if (!decodeRecords(response, DnsSection.ANSWER, buffer, answerCount)) {
            return response;  // 截断：提前返回已解析数据
        }
        if (!decodeRecords(response, DnsSection.AUTHORITY, buffer, authorityRecordCount)) {
            return response;  // 截断
        }
        decodeRecords(response, DnsSection.ADDITIONAL, buffer, additionalRecordCount);
        return response;
    }

    // 模板方法：由子类实现
    protected abstract DnsResponse newResponse(A sender, A recipient, int id,
                                               DnsOpCode opCode, DnsResponseCode responseCode);

    // 截断处理：decodeRecord 返回 null 表示数据不足
    private boolean decodeRecords(DnsResponse response, DnsSection section,
                                   ByteBuf buf, int count) throws Exception {
        for (int i = count; i > 0; i--) {
            final DnsRecord r = recordDecoder.decodeRecord(buf);
            if (r == null) return false;  // 截断
            response.addRecord(section, r);
        }
        return true;
    }
}
```

关键设计点：
- **泛型地址参数** `<A extends SocketAddress>` 让同一个解码逻辑同时支持 `InetSocketAddress`（UDP）和 `SocketAddress`（TCP）
- **模板方法** `newResponse()` 让 UDP 使用 `DatagramDnsResponse`（携带地址），TCP 使用 `DefaultDnsResponse`
- **截断感知**：当 `recordDecoder.decodeRecord()` 返回 `null` 时，解码器提前返回已成功解析的记录，而非抛出异常

### 3.7 DnsQueryEncoder — 查询编码器

```java
final class DnsQueryEncoder {
    private final DnsRecordEncoder recordEncoder;

    void encode(DnsQuery query, ByteBuf out) throws Exception {
        encodeHeader(query, out);                     // 12 字节 Header
        encodeQuestions(query, out);                  // Question 段
        encodeRecords(query, DnsSection.ADDITIONAL, out); // Additional 段
    }

    private static void encodeHeader(DnsQuery query, ByteBuf buf) {
        buf.writeShort(query.id());       // ID (16 bit)
        int flags = 0;
        flags |= (query.opCode().byteValue() & 0xFF) << 14; // OPCODE
        if (query.isRecursionDesired()) {
            flags |= 1 << 8;              // RD
        }
        buf.writeShort(flags);
        buf.writeShort(query.count(DnsSection.QUESTION));     // QDCOUNT
        buf.writeShort(0);                                    // ANCOUNT = 0
        buf.writeShort(0);                                    // NSCOUNT = 0
        buf.writeShort(query.count(DnsSection.ADDITIONAL));   // ARCOUNT
    }
}
```

注意查询编码器只编码 Header + Question + Additional 三部分，因为查询报文的 Answer 和 Authority 段始终为空。编码使用委托模式，将记录级别的编码委托给 `DnsRecordEncoder`。

### 3.8 DnsMessageUtil — 共享编解码逻辑

`DnsMessageUtil` 是一个工具类，集中了 Query 解码和 Response 编码的共享逻辑，被 UDP 和 TCP 的 Handler 复用：

```java
final class DnsMessageUtil {

    // 解码 DNS Query（UDP/TCP 共用）
    static DnsQuery decodeDnsQuery(DnsRecordDecoder decoder, ByteBuf buf,
                                    DnsQueryFactory supplier) throws Exception {
        DnsQuery query = newQuery(buf, supplier);  // 解析 Header
        // 解析四个段
        int questionCount = buf.readUnsignedShort();
        int answerCount = buf.readUnsignedShort();
        int authorityRecordCount = buf.readUnsignedShort();
        int additionalRecordCount = buf.readUnsignedShort();
        decodeQuestions(decoder, query, buf, questionCount);
        decodeRecords(decoder, query, DnsSection.ANSWER, buf, answerCount);
        decodeRecords(decoder, query, DnsSection.AUTHORITY, buf, authorityRecordCount);
        decodeRecords(decoder, query, DnsSection.ADDITIONAL, buf, additionalRecordCount);
        return query;
    }

    // 编码 DNS Response（UDP/TCP 共用）
    static void encodeDnsResponse(DnsRecordEncoder encoder, DnsResponse response, ByteBuf buf) {
        encodeHeader(response, buf);            // 12 字节 Header
        encodeQuestions(encoder, response, buf); // Question 段
        encodeRecords(encoder, response, DnsSection.ANSWER, buf);
        encodeRecords(encoder, response, DnsSection.AUTHORITY, buf);
        encodeRecords(encoder, response, DnsSection.ADDITIONAL, buf);
    }

    // DnsQueryFactory 工厂接口：让调用方决定创建哪种 Query 实现
    interface DnsQueryFactory {
        DnsQuery newQuery(int id, DnsOpCode dnsOpCode);
    }
}
```

Query 解码使用工厂模式 (`DnsQueryFactory`)：UDP 创建 `DatagramDnsQuery`（携带 sender/recipient），TCP 创建 `DefaultDnsQuery`。Response 编码直接操作 `ByteBuf`，因为响应头的编码逻辑比查询头更复杂（包含 AA/TC/RA/RCODE 等字段）。

### 3.9 DefaultDnsRecordDecoder — 资源记录解码器

```java
public class DefaultDnsRecordDecoder implements DnsRecordDecoder {

    // 解码 Question 记录
    public final DnsQuestion decodeQuestion(ByteBuf in) throws Exception {
        String name = decodeName(in);           // 域名 (支持压缩指针)
        DnsRecordType type = DnsRecordType.valueOf(in.readUnsignedShort()); // QTYPE
        int qClass = in.readUnsignedShort();    // QCLASS
        return new DefaultDnsQuestion(name, type, qClass);
    }

    // 解码资源记录 (Answer/Authority/Additional)
    public final <T extends DnsRecord> T decodeRecord(ByteBuf in) throws Exception {
        final int startOffset = in.readerIndex();
        final String name = decodeName(in);

        // 安全检查：剩余字节是否足够 (TYPE 2 + CLASS 2 + TTL 4 + RDLENGTH 2 = 10)
        if (endOffset - in.readerIndex() < 10) {
            in.readerIndex(startOffset);  // 回退读指针
            return null;                  // 数据不足，返回 null 表示截断
        }

        final DnsRecordType type = DnsRecordType.valueOf(in.readUnsignedShort());
        final int aClass = in.readUnsignedShort();
        final long ttl = in.readUnsignedInt();
        final int length = in.readUnsignedShort();  // RDLENGTH

        // RDATA 完整性检查
        if (endOffset - offset < length) {
            in.readerIndex(startOffset);
            return null;
        }

        T record = decodeRecord(name, type, aClass, ttl, in, offset, length);
        in.readerIndex(offset + length);  // 跳过 RDATA
        return record;
    }
}
```

`decodeRecord()` 的安全设计值得注意：当数据不足时回退 `readerIndex` 并返回 `null`，而不是抛出异常。这使得上层 `DnsResponseDecoder` 能够优雅地处理截断响应（TC 标志）。

### 3.10 DefaultDnsRecordEncoder — 资源记录编码器

```java
public class DefaultDnsRecordEncoder implements DnsRecordEncoder {

    public void encodeRecord(DnsRecord record, ByteBuf out) throws Exception {
        if (record instanceof DnsQuestion)       encodeQuestion(...);
        else if (record instanceof DnsPtrRecord)  encodePtrRecord(...);
        else if (record instanceof DnsOptEcsRecord) encodeOptEcsRecord(...);
        else if (record instanceof DnsOptPseudoRecord) encodeOptPseudoRecord(...);
        else if (record instanceof DnsRawRecord)  encodeRawRecord(...);
        else throw new UnsupportedMessageTypeException(record);
    }

    // 通用记录头编码
    private void encodeRecord0(DnsRecord record, ByteBuf out) {
        encodeName(record.name(), out);           // 域名
        out.writeShort(record.type().intValue()); // TYPE
        out.writeShort(record.dnsClass());        // CLASS
        out.writeInt((int) record.timeToLive());  // TTL (4 bytes)
    }
}
```

编码器通过 `instanceof` 链实现多态编码，支持 `DnsQuestion`、`DnsPtrRecord`、`DnsOptEcsRecord`、`DnsOptPseudoRecord`、`DnsRawRecord` 五种记录类型。PTR 记录需要先预留 2 字节 RDLENGTH 空间，编码完 hostname 后回填长度。

### 3.11 DnsCodecUtil — 域名编解码工具

`DnsCodecUtil` 是域名处理的核心工具类，实现了 RFC 1035 的域名编码规则和压缩指针机制。

**域名编码**（写入字节流）：

```java
static void encodeDomainName(String name, ByteBuf buf) {
    if (ROOT.equals(name)) {
        buf.writeByte(0);  // 根域名: 单个零字节
        return;
    }
    String[] labels = name.split("\\.");
    for (String label : labels) {
        int labelLen = label.length();
        if (labelLen > 63) throw new IllegalArgumentException(...); // RFC 限制
        if (label.contains("\0")) throw new IllegalArgumentException(...);
        buf.writeByte(labelLen);                // 标签长度 (1 byte)
        ByteBufUtil.writeAscii(buf, label);     // 标签内容 (ASCII)
    }
    buf.writeByte(0);  // 域名结束标记
}
```

**域名解码**（从字节流读取，支持压缩指针）：

```java
static String decodeDomainName(ByteBuf in) {
    int position = -1;   // 指针之后的恢复位置
    int checked = 0;     // 循环检测计数器

    while (in.isReadable()) {
        final int len = in.readUnsignedByte();
        final boolean pointer = (len & 0xc0) == 0xc0;  // 高2位为 11 表示指针

        if (pointer) {
            if (position == -1) {
                position = in.readerIndex() + 1;  // 记录指针后的位置
            }
            final int next = (len & 0x3f) << 8 | in.readUnsignedByte(); // 14位偏移
            if (next >= end) throw new CorruptedFrameException("out-of-range pointer");
            in.readerIndex(next);  // 跳转到指针目标

            checked += 2;
            if (checked >= end) throw new CorruptedFrameException("name contains a loop");
        } else if (len != 0) {
            if (len > 63) throw new TooLongFrameException("label must <= 63");
            name.append(in.toString(in.readerIndex(), len, CharsetUtil.UTF_8)).append('.');
            in.skipBytes(len);
            if (name.length() > 255) throw new TooLongFrameException("name must <= 255");
        } else {
            break;  // 长度 0 = 域名结束
        }
    }

    if (position != -1) in.readerIndex(position);  // 恢复到指针之后
    return name.toString();
}
```

关键安全机制：
- **循环检测**：`checked` 计数器防止指针循环（恶意构造的报文可能导致无限循环）
- **边界检查**：指针偏移不能超过 `writerIndex`，标签长度不能超过 63，域名总长度不能超过 255
- **指针恢复**：遇到指针后保存恢复位置，解码完毕后将 `readerIndex` 恢复到指针之后，使后续字段能正常读取

## 4. 设计思想

### 4.1 UDP 与 TCP 的统一抽象

DNS 协议支持 UDP（默认，端口 53）和 TCP（大报文、区域传输，RFC 7766）两种传输方式。Netty 通过分层设计实现编解码逻辑复用：

```
                    共享层
┌────────────────────────────────────────────────┐
│  DnsQueryEncoder        编码查询报文            │
│  DnsResponseDecoder     解码响应报文            │
│  DnsMessageUtil         Query 解码/Response 编码│
│  DefaultDnsRecordEncoder/Decoder  记录编解码    │
└────────────────────────────────────────────────┘
           ▲                    ▲
           │                    │
    UDP 适配层            TCP 适配层
┌──────────────────┐  ┌──────────────────────────┐
│ DatagramDnsQuery │  │ TcpDnsQueryEncoder       │
│ Encoder          │  │ (MessageToByteEncoder)   │
│ (MessageToMessage│  │                          │
│  Encoder)        │  │ 帧格式:                   │
│                  │  │ +--------+----------+     │
│ DatagramDnsRes-  │  │ | Length | DNS Msg  |     │
│ ponseDecoder     │  │ | 2字节  |  变长    |     │
│ (MessageToMessage│  │ +--------+----------+     │
│  Decoder)        │  │                          │
└──────────────────┘  │ TcpDnsResponseDecoder    │
                      │ (LengthFieldBasedFrame-  │
                      │  Decoder)                │
                      └──────────────────────────┘
```

**UDP 模式**：
- 编码器继承 `MessageToMessageEncoder<AddressedEnvelope<DnsQuery, InetSocketAddress>>`
- 解码器继承 `MessageToMessageDecoder<DatagramPacket>`
- 消息对象 (`DatagramDnsQuery/Response`) 同时是 `AddressedEnvelope`，携带 sender/recipient 地址

**TCP 模式**：
- 编码器继承 `MessageToByteEncoder<DnsQuery>`，直接输出带长度前缀的字节流
- 解码器继承 `LengthFieldBasedFrameDecoder`，自动处理 TCP 粘包/拆包

### 4.2 TCP DNS 长度前缀机制（RFC 7766）

RFC 7766 Section 8 定义了 DNS over TCP 的帧格式：每个 DNS 报文前附加 2 字节网络字节序长度字段。这解决了 TCP 流式传输的帧定界问题。

**TCP 编码流程**：

```java
// TcpDnsQueryEncoder
protected void encode(ChannelHandlerContext ctx, DnsQuery msg, ByteBuf out) {
    // 1. 预留 2 字节给长度字段
    out.writerIndex(out.writerIndex() + 2);
    // 2. 编码 DNS 报文 (Header + Question + Additional)
    encoder.encode(msg, out);
    // 3. 回填长度 = 报文总长度 - 2 (不含长度字段自身)
    out.setShort(0, out.readableBytes() - 2);
}
```

**TCP 解码流程**：

```java
// TcpDnsResponseDecoder extends LengthFieldBasedFrameDecoder
public TcpDnsResponseDecoder(DnsRecordDecoder recordDecoder, int maxFrameLength) {
    // maxFrameLength=64KB, lengthOffset=0, lengthSize=2, adjust=0, strip=2
    super(maxFrameLength, 0, 2, 0, 2);
}

protected Object decode(ChannelHandlerContext ctx, ByteBuf in) {
    ByteBuf frame = (ByteBuf) super.decode(ctx, in);  // 基类自动拆帧
    if (frame == null) return null;  // 数据不足，等待更多字节
    try {
        return responseDecoder.decode(remoteAddr, localAddr, frame.slice());
    } finally {
        frame.release();
    }
}
```

`LengthFieldBasedFrameDecoder` 的参数 `super(maxFrameLength, 0, 2, 0, 2)` 含义：
- `lengthFieldOffset=0`：长度字段在帧起始位置
- `lengthFieldLength=2`：长度字段占 2 字节
- `lengthAdjustment=0`：长度值不含自身
- `initialBytesToStrip=2`：传递给 `decode()` 的帧已去除长度前缀

### 4.3 域名压缩指针机制

DNS 报文中域名可能大量重复（如 Answer 段的 NAME 通常与 Question 段的 QNAME 相同）。RFC 1035 Section 4.1.4 定义了压缩机制：

```
普通标签: 00xxxxxx xxxxxxxx (高2位为00, 低6位为标签长度)
压缩指针: 11xxxxxx xxxxxxxx (高2位为11, 低14位为报文内偏移量)

示例报文 (偏移量标注):
[0]  \x03www\x07example\x03com\x00  ← 完整域名 "www.example.com."
[19] \x03ftp\xc0\x04                 ← "ftp." + 指针指向偏移4 = "example.com."
                                      结果: "ftp.example.com."
```

解码时使用 `in.duplicate().setIndex(offset, offset + length)` 而非 `slice()` 来处理 RDATA 中的压缩域名，因为压缩指针的偏移量是相对于整个 DNS 报文开头的，而非相对于 RDATA 的起始位置。

对于包含压缩域名的记录类型（PTR、CNAME、NS、MX），Netty 使用 `DnsCodecUtil.decompressDomainName()` 将压缩域名解码为字符串后重新编码为未压缩格式：

```java
static ByteBuf decompressDomainName(ByteBuf compression) {
    String domainName = decodeDomainName(compression);  // 解码压缩域名
    ByteBuf result = compression.alloc().buffer(domainName.length() << 1);
    encodeDomainName(domainName, result);  // 重新编码为未压缩格式
    return result;
}
```

### 4.4 记录类型的特殊解码处理

不同记录类型的 RDATA 结构不同，`DefaultDnsRecordDecoder.decodeRecord()` 对四种情况做了特殊处理：

```java
protected DnsRecord decodeRecord(String name, DnsRecordType type, int dnsClass,
                                  long timeToLive, ByteBuf in, int offset, int length) {
    // PTR: RDATA = 域名 (可能压缩)
    if (type == DnsRecordType.PTR) {
        return new DefaultDnsPtrRecord(name, dnsClass, timeToLive,
            decodeName0(in.duplicate().setIndex(offset, offset + length)));
    }

    // CNAME/NS: RDATA = 域名 (可能压缩)
    if (type == DnsRecordType.CNAME || type == DnsRecordType.NS) {
        ByteBuf decompressed = DnsCodecUtil.decompressDomainName(
            in.duplicate().setIndex(offset, offset + length));
        return new DefaultDnsRawRecord(name, type, dnsClass, timeToLive, decompressed);
    }

    // MX: RDATA = preference(2字节) + exchange(压缩域名)
    if (type == DnsRecordType.MX) {
        if (length < 3) throw new CorruptedFrameException("MX RDATA too short");
        final int pref = in.getUnsignedShort(offset);
        ByteBuf exchange = DnsCodecUtil.decompressDomainName(
            in.duplicate().setIndex(offset + 2, offset + length));
        // 重建未压缩 RDATA
        ByteBuf out = in.alloc().buffer(2 + exchange.readableBytes());
        out.writeShort(pref);
        out.writeBytes(exchange);
        return new DefaultDnsRawRecord(name, type, dnsClass, timeToLive, out);
    }

    // 其他类型: RDATA 保持原样 (retainedDuplicate 避免拷贝)
    ByteBuf content = in.retainedDuplicate();
    content.setIndex(offset, offset + length);
    return new DefaultDnsRawRecord(name, type, dnsClass, timeToLive, content);
}
```

### 4.5 引用计数与资源管理

DNS 消息继承 `AbstractReferenceCounted`，所有操作都遵循 Netty 的引用计数协议：

```java
// 解码失败时的安全释放 (DnsResponseDecoder)
boolean success = false;
try {
    decodeQuestions(response, buffer, questionCount);
    // ... 解码各段 ...
    success = true;
    return response;
} finally {
    if (!success) response.release();  // 解码失败时释放
}
```

`AbstractDnsMessage.deallocate()` 会遍历所有 Section 释放记录，配合 `ResourceLeakDetector` 在开发阶段检测未释放的消息。

### 4.6 Sharable Handler 设计

UDP 编解码器标注了 `@ChannelHandler.Sharable`，因为它们不持有任何状态：

```java
@ChannelHandler.Sharable
public class DatagramDnsQueryDecoder extends MessageToMessageDecoder<DatagramPacket> { ... }

@ChannelHandler.Sharable
public class DatagramDnsResponseDecoder extends MessageToMessageDecoder<DatagramPacket> { ... }
```

这意味着同一个 Decoder/Encoder 实例可以在多个 Channel 的 Pipeline 中共享，减少对象创建开销。TCP 编解码器同样标注了 `@ChannelHandler.Sharable`。

## 5. 模块交互

### 5.1 UDP 客户端模式 Pipeline

```
ChannelPipeline (UDP DNS 客户端):
  ┌────────────────────────────────────────────────────────────────┐
  │  出站方向 (发送查询)                                           │
  │                                                                │
  │  DatagramDnsQueryEncoder (@Sharable)                           │
  │    输入: AddressedEnvelope<DnsQuery, InetSocketAddress>        │
  │    输出: DatagramPacket                                        │
  │    委托: DnsQueryEncoder → DnsRecordEncoder                    │
  ├────────────────────────────────────────────────────────────────┤
  │  入站方向 (接收响应)                                           │
  │                                                                │
  │  DatagramDnsResponseDecoder (@Sharable)                        │
  │    输入: DatagramPacket                                        │
  │    输出: DatagramDnsResponse (extends AddressedEnvelope)       │
  │    委托: DnsResponseDecoder → DnsRecordDecoder                 │
  └────────────────────────────────────────────────────────────────┘
```

### 5.2 TCP 客户端模式 Pipeline

```
ChannelPipeline (TCP DNS 客户端):
  ┌────────────────────────────────────────────────────────────────┐
  │  出站方向                                                      │
  │                                                                │
  │  TcpDnsQueryEncoder (@Sharable)                                │
  │    输入: DnsQuery                                              │
  │    输出: ByteBuf (2字节长度前缀 + DNS报文)                     │
  │    内部: writerIndex+2 → encode() → setShort(0, len-2)         │
  ├────────────────────────────────────────────────────────────────┤
  │  入站方向                                                      │
  │                                                                │
  │  TcpDnsResponseDecoder                                         │
  │    继承: LengthFieldBasedFrameDecoder(64KB, 0, 2, 0, 2)        │
  │    输入: ByteBuf (TCP 字节流)                                  │
  │    输出: DefaultDnsResponse                                    │
  │    流程: 自动拆帧 → slice → DnsResponseDecoder.decode()        │
  └────────────────────────────────────────────────────────────────┘
```

### 5.3 UDP 服务端模式 Pipeline

```
ChannelPipeline (UDP DNS 服务端):
  ┌────────────────────────────────────────────────────────────────┐
  │  入站方向                                                      │
  │                                                                │
  │  DatagramDnsQueryDecoder (@Sharable)                           │
  │    输入: DatagramPacket                                        │
  │    输出: DatagramDnsQuery                                      │
  │    委托: DnsMessageUtil.decodeDnsQuery() → DnsRecordDecoder    │
  ├────────────────────────────────────────────────────────────────┤
  │  出站方向                                                      │
  │                                                                │
  │  DatagramDnsResponseEncoder (@Sharable)                        │
  │    输入: AddressedEnvelope<DnsResponse, InetSocketAddress>     │
  │    输出: DatagramPacket                                        │
  │    委托: DnsMessageUtil.encodeDnsResponse() → DnsRecordEncoder │
  └────────────────────────────────────────────────────────────────┘
```

### 5.4 编解码器的委托关系

```
消息级编解码器                    记录级编解码器
┌──────────────────────┐         ┌──────────────────────┐
│  DnsQueryEncoder     │────────►│  DnsRecordEncoder    │
│  (Header + Section)  │         │  encodeQuestion()    │
├──────────────────────┤         │  encodeRecord()      │
│  DnsResponseDecoder  │────────►├──────────────────────┤
│  (Header + Section)  │         │  DnsRecordDecoder    │
├──────────────────────┤         │  decodeQuestion()    │
│  DnsMessageUtil      │────────►│  decodeRecord()      │
│  (共享编解码逻辑)    │         └──────────┬───────────┘
└──────────────────────┘                    │
                                            ▼
                              ┌──────────────────────┐
                              │  DefaultDnsRecord-    │
                              │  Encoder / Decoder    │
                              │  (具体实现)           │
                              ├──────────────────────┤
                              │  DnsCodecUtil         │
                              │  encodeDomainName()   │
                              │  decodeDomainName()   │
                              │  decompressDomainName()│
                              └──────────────────────┘
```

## 6. 关键流程

### 6.1 DNS 查询编码完整流程

```
1. Header 编码 (12 字节)
   ├─ writeShort(query.id())                    // ID
   ├─ 构建 Flags:
   │   ├─ OpCode << 14                          // [bit 15..12]
   │   └─ RD ? (1 << 8) : 0                    // [bit 8]
   ├─ writeShort(flags)                         // Flags
   ├─ writeShort(questionCount)                 // QDCOUNT
   ├─ writeShort(0)                             // ANCOUNT = 0
   ├─ writeShort(0)                             // NSCOUNT = 0
   └─ writeShort(additionalCount)               // ARCOUNT

2. Question 段编码
   └─ 对每个 DnsQuestion:
      ├─ encodeDomainName(name)                 // QNAME (标签长度+内容+零结尾)
      ├─ writeShort(type.intValue())            // QTYPE
      └─ writeShort(dnsClass)                   // QCLASS (通常 IN=1)

3. Additional 段编码
   └─ 对每条附加记录 (如 EDNS OPT):
      ├─ encodeDomainName(name)                 // NAME
      ├─ writeShort(type.intValue())            // TYPE
      ├─ writeShort(dnsClass)                   // CLASS
      ├─ writeInt((int)timeToLive)              // TTL
      ├─ writeShort(rdLength)                   // RDLENGTH
      └─ writeBytes(rdata)                      // RDATA
```

### 6.2 DNS 响应解码完整流程

```
1. Header 解析 (12 字节)
   ├─ readUnsignedShort() → id
   ├─ readUnsignedShort() → flags
   │   ├─ QR  = flags >> 15                    // 必须为 1
   │   ├─ Opcode = (flags >> 11) & 0xf
   │   ├─ AA  = (flags >> 10) & 1
   │   ├─ TC  = (flags >> 9) & 1
   │   ├─ RD  = (flags >> 8) & 1
   │   ├─ RA  = (flags >> 7) & 1
   │   ├─ Z   = (flags >> 4) & 7
   │   └─ RCODE = flags & 0xf
   └─ readUnsignedShort() x4 → 四段计数

2. Question 段解析
   └─ 循环 QDCOUNT 次:
      ├─ decodeDomainName() → QNAME
      ├─ readUnsignedShort() → QTYPE
      └─ readUnsignedShort() → QCLASS

3. Answer/Authority/Additional 段解析
   └─ 对每条记录:
      ├─ decodeDomainName() → NAME
      ├─ 检查剩余字节 >= 10 (TYPE+CLASS+TTL+RDLENGTH)
      ├─ readUnsignedShort() → TYPE
      ├─ readUnsignedShort() → CLASS
      ├─ readUnsignedInt() → TTL
      ├─ readUnsignedShort() → RDLENGTH
      ├─ 检查剩余字节 >= RDLENGTH
      ├─ 根据 TYPE 创建记录对象:
      │   ├─ PTR    → DefaultDnsPtrRecord (解压域名)
      │   ├─ CNAME/NS → DefaultDnsRawRecord (解压域名)
      │   ├─ MX     → DefaultDnsRawRecord (preference + 解压域名)
      │   └─ 其他   → DefaultDnsRawRecord (RDATA 原样)
      └─ readerIndex = offset + RDLENGTH

4. 截断处理
   ├─ decodeRecord() 返回 null → 提前返回已解析数据
   └─ 最终通过 finally 块确保失败时释放 response
```

### 6.3 域名压缩指针解码示例

```
报文内容 (十六进制):
偏移: 00 01 02 03 04 05 06 07 08 09 0A 0B 0C 0D 0E 0F
数据: 03 77 77 77 07 65 78 61 6D 70 6C 65 03 63 6F 6D 00
       |  w  w  w  |  e  x  a  m  p  l  e  |  c  o  m  \0

解码 "www.example.com." (无指针):
1. 读取 0x03 → 读3字节 "www" → name = "www."
2. 读取 0x07 → 读7字节 "example" → name = "www.example."
3. 读取 0x03 → 读3字节 "com" → name = "www.example.com."
4. 读取 0x00 → 结束

带指针的情况:
偏移: 00 01 02 03 04 05 06 07 08 09 0A 0B 0C 0D 0E 0F 10 11 12 13
数据: 03 77 77 77 07 65 78 61 6D 70 6C 65 03 63 6F 6D 00 03 66 74 70 C0 04
       ←── "www.example.com." ──────────────────────────→ ← ftp. → ←指针→

解码 "ftp.example.com." (偏移 0x11):
1. 读取 0x03 → 读3字节 "ftp" → name = "ftp."
2. 读取 0xC0 → 指针! 计算偏移: (0xC0 & 0x3F) << 8 | 0x04 = 4
3. 跳转到偏移4: 07 65 78 61 6D 70 6C 65 03 63 6F 6D 00
4. 继续解码: "example.com."
5. 恢复 readerIndex 到偏移 0x13 (指针字节之后)
6. 最终结果: "ftp.example.com."
```

### 6.4 TCP DNS 编码时序

```
TcpDnsQueryEncoder.encode(ctx, query, out):

  ByteBuf 状态:
  初始:  [                              ] (writerIndex=0)

  步骤1: [__                              ] (writerIndex=2, 预留长度字段)
         ↑ 2字节预留空间

  步骤2: [__|ID|Flags|QD|AN|NS|AR|QNAME|QTYPE|QCLASS|Additional...]
         ↑ Header (12字节)  ↑ Question 段         ↑ Additional 段

  步骤3: [12|ID|Flags|QD|AN|NS|AR|QNAME|QTYPE|QCLASS|Additional...]
         ↑ 回填长度 = readableBytes - 2
```

## 7. 学习要点

### 7.1 DNS 报文结构要点

- **Header 固定 12 字节**：ID(2) + Flags(2) + QDCOUNT(2) + ANCOUNT(2) + NSCOUNT(2) + ARCOUNT(2)
- **Question 段**：QNAME(变长) + QTYPE(2) + QCLASS(2)，查询报文至少包含一条 Question
- **资源记录 (Answer/Authority/Additional)**：NAME(变长) + TYPE(2) + CLASS(2) + TTL(4) + RDLENGTH(2) + RDATA(变长)
- **QR 位区分查询与响应**：解码时首先检查 `flags >> 15`，查询为 0，响应为 1

### 7.2 域名编码规则

- 域名由多个标签组成，每个标签以**长度字节**开头（非分隔符）
- 标签长度范围 0~63（6 bit 限制），域名总长度不超过 255 字节
- 以长度为 0 的字节结尾（根域名单独表示为 `\x00`）
- 压缩指针格式：`11xxxxxx xxxxxxxx`，高 2 位为 11 标识指针，低 14 位为报文内偏移量
- 指针可出现在域名的任意位置，且指针可以指向另一个指针（链式引用）

### 7.3 UDP vs TCP 设计差异

| 维度 | UDP | TCP |
|------|-----|-----|
| 传输层 | 无连接数据报 | 面向连接流 |
| 帧定界 | DatagramPacket 天然边界 | 2 字节长度前缀 (RFC 7766) |
| 报文大小 | 默认 512 字节 (EDNS 可扩展) | 理论无限制 (Netty 默认 64KB) |
| 消息类型 | DatagramDnsQuery/Response | DefaultDnsQuery/Response |
| Handler 基类 | MessageToMessageEncoder/Decoder | MessageToByteEncoder / LengthFieldBasedFrameDecoder |
| 地址信息 | 携带 sender/recipient | 通过 Channel 获取 |
| 典型场景 | 普通查询 | 区域传输 (AXFR/IXFR)、大报文 |

### 7.4 编码器/解码器设计模式

- **模板方法**：`DnsResponseDecoder.newResponse()` 让子类决定创建哪种 Response 实例
- **委托模式**：消息级编解码器将记录级编解码委托给 `DnsRecordEncoder`/`DnsRecordDecoder`
- **工厂模式**：`DnsMessageUtil.DnsQueryFactory` 让调用方决定 Query 的具体实现
- **策略模式**：`DnsRecordEncoder`/`DnsRecordDecoder` 可替换自定义实现
- **Sharable Handler**：UDP 编解码器无状态，可在多个 Channel 间共享

### 7.5 安全与健壮性

- **截断响应**：`decodeRecord()` 返回 `null` 而非抛异常，上层优雅处理
- **指针循环**：`DnsCodecUtil` 通过 `checked` 计数器检测循环引用
- **边界检查**：标签长度 <= 63，域名总长 <= 255，指针偏移不超报文范围
- **引用计数**：解码失败时通过 `finally` 块释放已创建的 `DnsResponse`
- **内存泄漏检测**：`ResourceLeakDetector` 在开发阶段追踪未释放的消息

### 7.6 值得借鉴的设计技巧

- **单记录/列表二态存储**：`AbstractDnsMessage` 的 `Object` 类型字段在单记录时不创建 List，减少内存分配
- **保留读指针回退**：解码时保存 `startOffset`，数据不足时回退而非抛异常
- **retainedDuplicate vs slice**：处理压缩域名时使用 `retainedDuplicate()` 保持引用计数
- **先预留后回填**：TCP 编码时先跳过长度字段，编码完成后回填，避免两遍扫描
