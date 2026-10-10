# 操作格式 v1 与网络 envelope v1

`lfs-protocol` 实现 canonical encoder、有界 decoder 和纯 full-row MV register。LFS1 payload 固定操作身份与字节；网络通过 LFSE envelope 携带 schema fingerprint、附件声明与原始 payload。固定十六进制向量保护兼容性。

SHA-256 只校验完整性，不认证来源。没有远端 SQL、DDL、时间戳覆盖、字段合并、raw changeset replication 或 `sqlite3changeset_apply`。

## 精确二进制布局

整数均 big-endian。`bytes` 是 u32 字节长度加原字节；`text` 为 UTF-8 bytes。没有 padding、map、平台端序或 JSON 数字。header 顺序如下：

| 字段                         | 编码                                            |
| ---------------------------- | ----------------------------------------------- |
| magic / protocol             | `4c 46 53 31 00 00 00 01`（LFS1 + u32 1）       |
| dataset, device, incarnation | 各一个 length-prefixed UTF-8 字符串，1–128 字节 |
| business schema version      | u32，非零                                       |
| transaction sequence         | u64，非零；本地存储实际限制 `1..i64::MAX-1`     |
| predecessor                  | u8 0/1；1 后跟前一包 SHA-256 的 32 原始字节     |
| operation count              | u32，1–1000                                     |
| operations                   | 按 `(table_id, key UTF-8 bytes)` 严格升序       |

每个 operation 顺序为：`table_id:u32`（非零）；typed key（目前只允许 TEXT tag）；`parent_count:u32`；每个 parent 的 length-prefixed canonical OpId；`row_present:u8`；存在行则跟 `column_count:u32` 和逐列 typed value，缺失行即 tombstone。

OpId 的现有编码携带 `(dataset, device, incarnation, seq, index, schema)`；语义身份是前五项，schema 为内容元数据，同一语义身份不同 schema/content 不能成为另一个合法版本。编码元组不变；前三项 length-prefixed UTF-8，然后 u64 seq、u32 index、u32 schema。包中自身 OpId 由 header + 从 0 开始的 operation 位置无歧义推导；不会重复存一份。父 OpId 存完整编码。parents 按编码 bytes 升序且不重复，最多 256 个、每个最多 512 字节；普通单头写产生零或一个父版本，resolve 可引用多个同记录已存在版本。包的列顺序由登记 Schema 定义，table ID 不随 SQL 表名排序变化。

| 值      | tag | 后续编码                         |
| ------- | --: | -------------------------------- |
| NULL    |   0 | 无                               |
| INTEGER |   1 | i64 二进制补码 8 字节            |
| REAL    |   2 | finite IEEE-754 f64 bits，8 字节 |
| TEXT    |   3 | length-prefixed 严格 UTF-8       |
| BLOB    |   4 | length-prefixed 原始 bytes       |

NULL、空 TEXT、空 BLOB 和缺失行各不相同。SQLite STRICT affinity 后的实际存储值为真值；不承诺保留 SQLite 自己规范化的输入表示。非有限 real、非法 UTF-8、NULL 主键拒绝；绑定参数也先拒 NaN/Infinity。完整行第一列必须等于 typed key。fixed-vector 测试包含负 i64、finite real、NULL、TEXT 与二进制值；SQLite API 测试另外覆盖 i64::MAX。

`hash = SHA256(exact_packet_bytes)`。`PendingPacket.object_id = decimal_seq_padded_to_20 + '-' + lowercase_hash_hex`，身份另外在 `PendingPacket.identity` 与包 header 中；**object_id 不是跨 dataset/device/incarnation 的全局对象路径**。安全对象路径见下文；不能直接把任意 identity 当文件路径。

## 严格操作与因果规则

每包恰好包含每个 typed record key 的一个净事务操作；当前支持的 typed key 只有 TEXT，不隐式转数值。主键移动是旧 key 墓碑与新 key 完整行；keys 按 table ID、UTF-8 字节严格升序。op index 必须等于此序列中的 0-based 位置；wire 中隐含该位置，不能编码独立重复 index。encoder 拒绝错误 index/重复 key，decoder 拒绝重复/逆序 key、重复/逆序 parents、非规范 tags、尾随字节、非法 UTF-8、非有限 REAL、列数/主键不符。

seq 为 `1..i64::MAX-1`。seq1 必须无 predecessor，其他必须有前包 hash。predecessor 只在相同 dataset/device/incarnation 流中寻找 `seq-1`；即使上一包操作另一个记录，也不能跳过。父 OpId 必须是同 dataset、当前支持的 schema、相同 table+typed key 的已有节点；自己流中的父 seq 必须更早。同包不同 key 不能互为记录父版本。跨流父版本缺失持久等待；scope 错误明确 Failed。

纯 `Register` 接受已校验的、父节点已存在的操作，永久保留全部节点。heads 是没有被其他已知版本直接引用的节点；完整祖先集合的边不会删除。SQLite 的 `_lfs_nodes/_lfs_edges/_lfs_tips` 实现相同规则，保留全部 `_lfs_packets` 字节。重复包幂等；旧祖先因已存在而不再次成为 head。**不存在 device max-seq 代替记录因果的路径。**

投影固定选取 heads 中 **解析后 `(dataset, device, incarnation, numeric seq, numeric index, schema)` 最大的一整个版本**；字符串用 Rust Ord（UTF-8 字典序），数字按数值。schema 不能区分同身份的两个版本，因此最后字段不改变身份选择。不能用 length-prefixed OpId bytes 排序代替此规则；`aa` / `b` 的测试覆盖这个区别。最大版本是墓碑时业务表无该行，但所有其他 heads 仍可通过 `heads/list_conflicts` 检查。

普通事务对 multihead 记录产生净变化会返回 `ConflictRequiresResolution`，整事务连同无关修改一起回滚。`resolve(table,key,observed_ids,full_row_or_none)` 引用调用者明确给出的、非空且唯一的已存在同记录版本，规范排序后生成一个新操作；不要求 observed 等于当下 heads。未见子分支和同时生成的 resolution 保持并发。无字段自动合并。

## 同步对象、收件与身份

`PacketKey = (Identity {dataset,device,incarnation}, seq, hash)` 是完整持久 outbox/inbox 身份。`Package` 包含此 key、canonical schema fingerprint（版本 + 按 table ID 排序的 canonical DDL）、exact packet bytes 和附件引用数组。早期无附件 MODEL 包的引用数组为空。fingerprint 是对象元数据，不重写 payload/hash，也不是签名；当前附件扩展规则见下文。

接收顺序：先检查字节长度与 envelope 边界，完整性失败留存独立 attempt；核验 hash、header/key identity、protocol/schema/fingerprint/table whitelist、完整列数/类型/NOT NULL、key=row PK、canonical order、父 scope、附件声明后记录 Validated。缺前包或父节点转 WaitingDependencies。单个 apply 事务包含 packet/DAG/heads/业务投影/inbox Applied；`_lfs_packets` 的完整已应用集合同时是 applied progress，失败不能留下半包。不存在 remote Session 或 echo outbox。

只有通过完整 envelope/hash、canonical payload、schema、表/键/值和已知父记录作用域校验的不同内容，才能建立同流同 seq 分叉。父节点未到达时先等待，不能把尚未验证的 scope 当作有效竞争内容。无效哈希、decode 或元数据不会停止合法流；真实分叉写入 `_lfs_stopped` 并冻结已有合法投影，只隔离该流。独立流仍可上传、下载和重访，总体状态保持不健康。

`Retryable` 表示可重新下载的完整性失败；Failed 表示不兼容 schema/不支持语义/约束等永久阻塞。retryable 对象不进入永久 known 集合。`failed_attempts` 追加保存每次有界失败的 ObjectKey、错误、retryable 标记、完整原始 wire（OpenDAL fetch）或含所有 fingerprint/attachment 字段的 LFSE 证据（MODEL Package）。原始证据不覆盖；正确重传可替换工作收件的 Retryable 状态并原子应用。无字节的网络错误保留键和分类，超过接管上限的输入仍由宿主负责。旧 `retained_variants` 仅用于读取旧库 payload-only 历史，不再作为新失败的写入口；`retained_bytes` 保留收件 payload 读取用途。历史失败证据不会冒充竞争有效版本，也不会自行清除永久阻塞。

`_lfs_download_debt` 持久保存按完整 ObjectKey 去重的下载欠账、轮转次序、失败和 blocked 状态，最多 10,000 项。页面进入欠账后才推进扫描游标；单对象缺失/损坏消耗预算并移到队尾，其他对象可继续，缺失对象即使后续 listing 不再出现也不会丢失。每次最多处理 downloads 项，另受共享 retries 预算限制；Auth/Cancelled 终止本次网络调用但保留欠账。永久阻塞不自动重试为成功，只有具体内容修正且重新校验或未来显式恢复工作流才能改变状态。`Status` 暴露 `blocked_streams` 身份和 `outstanding_objects`；存在任何欠账、Failed/Blocked/Waiting/Retryable 或停止流时不会报告 healthy。

等待队列采用持久 rowid 游标，每次最多检查 1000 个候选，依赖补齐后重访；新到达、`revisit`、`sync_once` 和重开均触发检查。有限预算用尽后保留状态和游标，下一次继续。枚举游标与不完整页标记也持久保存；扫描结束回到 0，下一轮重新发现旧缺口，不用最大 seq 跳过。已接收/应用的完整 PacketKey 去重，新 hash 必须重新下载检查。接收历史和祖先永不自动删除。

## 内部元数据 v1 → v2

在 `open` 的同一个初始化事务中创建 metadata v2（`_lfs_meta.version=2`），按序读取旧 outbox（包括 acknowledged），验证 bytes/hash/schema，重建 DAG 和完整身份的 delivery。操作身份、bytes、hash、next_seq、predecessor、ack 均保持。迁移失败整体回滚；升级后的数据库不能由旧 metadata v1 代码打开。

旧 `_lfs_versions/_lfs_heads/_lfs_outbox` 表保留为只读迁移来源，当前正常路径不再读写它们；不是第二套运行中的 head/outbox。新库中这些 legacy 表为空以保持明确的 schema 识别。生产数据变更机制只有 `_lfs_packets/_lfs_nodes/_lfs_edges/_lfs_tips/_lfs_delivery`。已有 baseline 操作直接迁入，不重新赋身份。

## 预算与边界

合法 packet ≤8 MiB、1–1000 操作、每行 1–64 列、每 cell/key ≤512 KiB；每 op ≤256 parents，单 parent ≤512 bytes；schema ≤64 表。decoder 在分配各计数对应内容前检查剩余输入与这些边界，先检查整个 packet 长度。

`Store::receive` 入口额外允许 **最多 16 MiB 的拒绝证据输入**：8–16 MiB 不解码、以 ≤8 MiB chunks 原样保存并标 Failed；超过 16 MiB 的 host input 在接管前返回 `Invalid(package limit)`，调用者仍负责原输入，不会存储或声称已接收。MODEL transport 本身拒绝 >8 MiB 的对象。不能用该入口把无界输入存入数据库。

`SyncLimits` 默认 upload100、page100×page_size100、download1000、全调用 retry2；硬上限分别 1000、1000×1000、10000、8。上传成功必须再下载完整对象比较后 ack（因此至多另有 upload 数量的 readback downloads）。每次失败 budget 只对 Transient/Timeout/RateLimit 消耗重试；Auth/Cancelled/Corrupt/LostAck 不自动重试。Fault queue 和事件各最多64项。状态/冲突/历史读取返回 owned 集合，历史长期增长仍可能有大内存开销；未提供历史 GC 或生产级大规模性能保证。

Session native changeset 先分配再检查净操作数，Rust callback 自身也不是内存/实时沙箱。保留同样边界；SDK 自身不会在事务中 await 网络。

## 完整网络 envelope v1

网络不是裸 `Packet`。`OutgoingEnvelope { key: ObjectKey, bytes }` 在产生本地操作的**同一个 SQLite 事务**写入 `_lfs_outgoing_wire`；表保存 payload hash、envelope hash 和完整字节。`pending_envelopes` 读取这份持久字节并校验；`Transport::upload` 接收该实例，OpenDAL 原样发送，不在上传时从当前 Schema/附件状态重新构造元数据。未来不识别的 envelope 版本拒绝；升级不能静默重写已提交 outbox。`Package::from_pending` 仅是 内存 MODEL/手工构包便利接口，不是 SDK 上传路径。

LFSE v1 使用与 inner payload 相同的 big-endian 长度规则：

| 顺序 | 字段                                                                                                |
| ---- | --------------------------------------------------------------------------------------------------- |
| 1    | 8 bytes `4c 46 53 45 00 00 00 01`（LFSE + u32 version 1）                                           |
| 2    | `u32 length + UTF-8 canonical schema fingerprint`，最大 `MAX_FINGERPRINT = 393216` bytes（384 KiB） |
| 3    | `u32 attachment count` ≤256，然后每项 `u32 length + UTF-8 reference` ≤512 bytes                     |
| 4    | 32 bytes `payload_hash = SHA256(exact LFS1 packet bytes)`                                           |
| 5    | `u32 payload length + exact LFS1 bytes`，≤8 MiB；禁止尾随 bytes                                     |

附件使用此字段，引用为严格排序、无重复的64位小写 SHA256 hex（最多256项）；非法路径/hash拒绝。旧无附件 wire 字节保持不变。解码先检查 envelope 总长 ≤`MAX_ENVELOPE = 8,913,984` bytes，再逐字段检查长度/UTF-8/hash，最后严格 decode inner Packet；身份、seq、predecessor、op ID 与因果校验沿用上述规则。`envelope_hash = SHA256(exact complete LFSE object)`，包含 fingerprint、附件字段、payload hash 和 payload。外部路径携带 envelope hash，不在 envelope 内对自身递归求 hash。SHA256 只提供完整性。

精确路径为：

```text
lfs1/<base64url_nopad(dataset)>/<base64url_nopad(device)>/<base64url_nopad(incarnation)>/<20-digit-seq>-<64-lowerhex-payload-hash>-<64-lowerhex-envelope-hash>.lfs
```

三个身份原始 UTF-8 字符串沿用 1–128 bytes，编码 segment ≤171 ASCII bytes；文件名为固定有界长度。接收路径必须严格逆解码并重新生成相等，拒绝非规范编码、空/多余组件、非法 seq/hash、特殊文件、越出 dataset root 的条目。包内身份/seq/hash 必须与路径精确一致。前包字段仍指向**同流 seq−1 的 inner payload hash**，不会变成 envelope hash；因此既有 OpId 与 ancestry 不变。

`ObjectKey = { packet: PacketKey, envelope_hash: Some(full_hash) }` 是生产枚举/下载键；None 只用于 内存 MODEL。去重使用完整 ObjectKey。相同 payload 的不同 envelope 仍需检查，不能被 payload-key 去重隐藏；错误 fingerprint/附件属于失败证据，不能因此停流。`_lfs_received_wire` 保存通过完整性及本地语义校验的完整对象；`Store/Sdk::received_envelopes` 与 `failed_attempts` 分别暴露有效 wire 和失败尝试。OpenDAL `Transport::fetch` 把有界原始 wire 交给核心校验和留证；原 `download` 仍提供严格验证后的 Package，用于上传回读及兼容已有 transport。自定义 transport 的默认 fetch 包装 download，只有 transport 实際交回的 bytes 才能被留存。

## 发布、扫描、迁移

发布先读取已有对象，核对实际长度、完整 SHA256 和字节相等；精确重试成功不改内容。已有对象若能解码成有效 envelope 却身份/内容不同，拒绝覆盖。只有缺失对象或不能解码的部分/损坏对象可用已提交固定字节补写；超过上限的对象 fail closed，需宿主调查。PUT/write 结果不确定时仍尝试完整回读，只有一致才成功；SDK 再回读一次后 ack。Fs 文件与父目录直到专用 root 执行 sync_all；没有全局 rename/条件创建依赖，没有共享 manifest/latest，没有远端删除。

OpenDAL lister 在 native 页到达时逐条消费为 ≤1000 个 ObjectKey 的 SDK 页，不用 `Operator::list` 一次收集整个 namespace。S3 采用声明的 recursive list；Fs/WebDAV 无 native recursive-list 时逐目录遍历，并对目录去重，最多4096目录/1,000,000文件位置。持久 cursor 是扫描位置而非 maxseq；中断重建 lister 并重放扫描位置，完成后归零，下轮重扫补齐晚到/顺序变化的对象。目录原生 listing 不承诺一致性快照；健康表示当前扫描与已知依赖齐全，不证明未来/隐藏对象不存在。缺前包及跨流依赖持久等待，不按最大 seq 跳过。

HTTP 响应流有上限：PROPFIND ≤4 MiB，其他响应 ≤MAX_ENVELOPE；上限适用于缺少 Content-Length 的 chunked body。WebDAV 0.59.4 底层仍会缓冲一份 PROPFIND 响应，不宣称服务器分页或无界目录支持。Fs 在 lister 前检查当前目录子项类型，避免上游对 symlink 条目的处理不一致。可取消 Tokio deadline 默认为每调用15秒，允许 `(0,120s]`；SQLite 执行锁/事务不跨 await。已有 SyncLimits 有界重试仅针对 Transient/Timeout/RateLimit；实际 HTTP 401/403→Auth、429→RateLimit，Unsupported/Limit/Corrupt 不重试。不输出原始后端错误、URL 或凭据。

旧的无 wire 表数据库在 `Store::open` 的初始化事务里一次性增加 `_lfs_wire_version=1`、outgoing/received wire 表；遍历已有 delivery（含 acked）用已锁定旧 schema_spec 对应的 Schema 固定 LFSE。只有这个**明确的预发布迁移**为旧 packet 首次补 envelope；之后不重新生成。inner bytes/hash、OpId、next_seq、predecessor、ack 不变。新代码存在独立 wire version，旧 schema 校验会拒绝新表，不能降级打开。迁移、同事务 outbox 故障回滚、取消后重试均已有真实 SQLite 测试。

## fingerprint 边界与兼容性

fingerprint 格式保持完整 canonical DDL，不切换 digest、不改变 LFSE/LFS1 版本、已提交字节、hash、OpId 或 pending outbox。64 表 ×64 列、63 字符名称、u32 最大 schema/table ID 的最坏 fingerprint 为 `11 + 64 × (11 + 5471) + 63 = 350922` 字节；其中主键必须 TEXT，其余最长类型 INTEGER，全非空。Schema.validate、编码、解码、receive 共用 384 KiB 上限；MAX_ENVELOPE 与 SQLite/HTTP/文件分配限制随之更新，pending_envelopes 单次总 wire 最多32 MiB。旧合法 wire 字节继续可读；旧二进制无法接收新增大 schema 的合法 wire，应先升级接收端，不能改写旧 outbox 来兼容它。

open 的初始化事务增加 attempts/chunks/download-debt 表，旧 schema 校验仍拒绝陌生表，不能降级打开；旧 wire/outbox 和停止标记不会改写。历史的 `_lfs_variants` / `_lfs_rejected` 保留读取。无 GC、自动迁移业务 schema 或自动清除真实分叉。

## 附件与恢复扩展

引用是 packet 级文件闭包，与该批记录操作和完整 wire 同事务冻结。接收实际 apply 同时保存历史引用与 Missing 元数据；保留全部祖先/冲突引用，不按当前投影清除。附件远端独立命名空间为 `lfs-attachments/<base64url_nopad(dataset)>/<64-lowerhex-sha256>`，不混入 lfs1 记录扫描。上传完成且完整 SHA256 回读通过才发布记录；上传失败不得 ack 或重写引用。

本地 write / resolve 每个显式引用都要求先可靠安装为 Available，并在业务修改前重新校验完整 SHA256；历史中已有 hash 若再次显式传入也适用。已知 Missing 返回 Rust Invalid / Node `Invalid` / CLI invalid(exit2)，业务、序号、DAG、outbox 不提交，Node `status()` 不变；失败事件可记录，原 Available 文件校验损坏可降为 Missing，I/O 按对应错误返回。宿主先 add/download 再 write/resolve，受控事务不联网。此为既有前置条件澄清，不改变 LFSE/LFS1、schema 或 API 行为。

已有记录保持原引用的普通编辑可不提供新的显式引用：全行保留原 hash，因果父包保留历史引用，无需每次重列所有 hash，本体 Missing 时仍可离线提交。第三端须收到相关因果历史，才能保有该历史引用并按需下载；已有实际 Node22/Linux Fs 三端闭环验证。不得据此推断任意字符串、新无关记录或缺失祖先历史的附件归属。这些验证不扩展其他平台或后端资格。

新增 attachment metadata version=1；已有库在 open 初始化事务添加表，历史 packet/wire 不改。restore 仅支持带当前完整 manifest 的备份，新的操作流随机轮换 incarnation 并从seq1开始，旧 delivery 按原身份/seq/hash确认。它不是业务 schema/全协议迁移，也不支持raw copy降级。详细状态机、文件闭包和兼容性限制见 [DESKTOP](DESKTOP.md)。
