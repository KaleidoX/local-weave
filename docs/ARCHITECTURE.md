# 架构与运行契约

本地路径：宿主受控 callback → 唯一 SDK connection / 新 Session → 净键 → 完整末态/墓碑 → multihead 拒写检查 → DAG、投影、next_seq、exact outbox 原子提交。远端路径：枚举/下载 → 有界校验与 receipt → 缺依赖持久等待 → 单包 DAG/投影/inbox Applied/完整 applied identity 集合原子提交。远端没有 Session、echo outbox 或任意 SQL 路径。

## 目录与依赖方向

根 `Cargo.toml` / `Cargo.lock` 是唯一 workspace。默认成员仅 `core/protocol` 和 `core/sqlite`；显式选择 `--workspace` 才包含适配器、CLI 和 Node native。

| 路径                     | package / 职责                                         |
| ------------------------ | ------------------------------------------------------ |
| `core/protocol`          | `lfs-protocol`：编码与纯因果寄存器                     |
| `core/sqlite`            | `lfs-sqlite`：Session、Store/Sdk、同步状态、附件与恢复 |
| `integrations/opendal`   | `lfs-storage`：可选宿主存储适配                        |
| `bin/lfs`                | `lfs-cli`：`lfs` JSON 命令                             |
| `bindings/nodejs`        | JS/TS API、测试、示例和构建入口                        |
| `bindings/nodejs/native` | `lfs-node`：Rust Node-API 实现                         |

依赖从 CLI/native 指向 adapter 和核心，adapter 指向核心，SQLite 核心指向 protocol。核心不依赖 OpenDAL/Tokio/HTTP；不另建 facade、service 或 layer crate。各成员保留自己的 examples/tests，包名和 import 不随目录变化。

## 模块与公开 API

| 模块                        | 当前职责                                                                                                                                              |
| --------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| `lfs-protocol`              | `Packet::encode/decode`、typed values/OpId、canonical hash、纯 `Register::insert/heads/projection`                                                    |
| `lfs-sqlite::schema/access` | typed canonical STRICT schema、fingerprint、受限 `Read::query` / `Write::execute/query`、authorizer                                                   |
| `lfs-sqlite::Store`         | `open/read/write_transaction`、`heads/list_conflicts/resolve`、`pending_packets/acknowledge`、`receive/revisit/status/applied_packets/retained_bytes` |
| `lfs-sqlite::causal`        | metadata v2 升级、持久 packet/nodes/edges/tips、完整流身份 delivery/inbox、failed/blocked receipts、依赖扫描游标                                      |
| `lfs-sqlite::attempts`      | 追加失败证据、原始 wire chunks、持久完整对象下载欠账与轮转                                                                                            |
| `lfs-sqlite::envelope`      | 固定 LFSE 编解码、同事务 outgoing wire、完整 hash 去重及 received wire、旧库预发布迁移                                                                |
| `lfs-storage`               | OpenDAL 0.59.4 Fs/WebDAV/S3 构造、安全路径、native lister、固定字节发布回读、有界 HTTP 与 deadline                                                    |
| `lfs-sqlite::sync`          | `Sdk` facade、异步 `sync_once`、`Transport` trait、`MemoryTransport` MODEL/FAULT、`Cancellation/SyncLimits/SyncReport/Event`                          |

`Sdk::open/read/write_transaction/sync_once/status/list_conflicts/resolve/close/events` 都执行真实逻辑。Store 是同步低层入口；Sdk 用同一个 Store 的 mutex 串行连接访问，锁忙明确返回 `Busy`，没有第二条 SDK 写连接。没有裸 writable Connection。宿主提供 async runtime；SDK 不创建 runtime/thread/background task，也不默认常驻。

## 事务、网络和关闭

`sync_once` 的独立 atomic guard 覆盖整个同步调用；并发调用或同步期间 close 返回 `SyncInProgress`。数据库访问使用短期 `try_lock`。取 committed owned bytes 后释放 mutex，再 await upload；上传后完整 download readback 一致才 ack。list/download 均在数据库 mutex 和 SQL transaction 之外；远端 atomic apply 中没有 await。

File 所有权锁与 SQLite `locking_mode=EXCLUSIVE` 生命周期锁仍在整个 Store 存活期保留，以持续拒绝外部 SQLite 连接和第二 SDK handle；这里的“网络不占 DB 锁”指不占 SDK 执行 mutex、不保持 SQL 事务，**不释放数据库文件的排他所有权**。网络 future 挂起期间通过公开 facade 成功写入的测试验证执行锁已经释放。

取消 token 会唤醒并取消挂起的 transport future；同时在网络前后、packet 之间和等待队列的每次单包处理前检查。已经进入的单包 SQLite commit 不从中间打断。future 被宿主 drop 会释放 sync guard；已持久 receipt/outbox 保留，下一次重新处理。每 token 最多64个并行 await 注册，drop 会清理。OpenDAL 使用宿主 Tokio runtime 的每调用 deadline，SDK 处理 Timeout 与总重试预算；不创建独立 runtime。Fs 验收同时包含真实 loopback HTTP 挂起超时；分类/重试故障注入仍明确标为 MODEL。

`close` 和 sync 使用同一 guard，串行关闭；返回当前状态然后 drop connection，未确认 outbox 不会删除或静默 ack。重开继续使用相同身份与 committed bytes；丢 ack 测试包含远端完整对象和本地重开逐字节比较。

## 因果与持久状态归属

纯 register 与 SQLite 都保留完整祖先，单 op 只描述一个 full row 或 tombstone。deterministic projection 选解析后 identity 最大的完整 head；删除 head 也可获胜且仍可检查。普通本地净修改 multihead 返回 `ConflictRequiresResolution`，整个 SQL callback 所有修改回滚。resolve 引用 caller-observed、已存在同记录版本；不要求等于最新 heads，不删除未知并发分支。详情见 [PROTOCOL](PROTOCOL.md)。

`_lfs_packets` 保存已应用 packet 的完整 stream+seq+hash 与 bytes；这是 applied progress 的事实集合。`_lfs_nodes/_lfs_edges/_lfs_tips` 保存永久 DAG 和全部 heads；`_lfs_delivery` 保存完整身份 pending/ack。`_lfs_inbox` 保存 exact receipt、状态及错误；同流分叉写 `_lfs_stopped`，不能继续上传/应用为健康。`_lfs_cursors` 保存枚举与等待扫描位置以及不完整页标记；`_lfs_rejected` 保存有界超限拒绝证据，`_lfs_variants` 仅保留旧证据读取，新失败写入 `_lfs_attempts/_lfs_attempt_bytes`，完整字节与元数据不可覆盖；`_lfs_download_debt` 保存下载欠账、次序和阻塞原因。无 max-device-seq 因果推断，无自动 GC。

metadata v1→v2 在 open 的初始化事务中迁移旧 outbox，包括 acked 历史；保留旧业务/身份/序号/bytes，旧单头表与 outbox 只留作迁移来源，不参与运行。基线 Import 的操作身份不改。迁移、重开、正常写、远端 apply 与 outbox 故障都在测试中覆盖。业务 schema migration 未实现；backup/restore/clone 见桌面契约。

## 收件与错误

Downloaded、Validated、WaitingDependencies、Applied、Retryable、Failed、Blocked 分别持久保存。hash、protocol、dataset、schema fingerprint/version、whitelist、列数/typed value/NOT NULL、PK、parents scope、canonical order、附件声明必须符合规则。Downloaded 在重开也会重新校验；缺前包或父操作持久等待。单包 commit 失败回滚所有 DAG/heads/业务/applied 状态，随后保留 Failed receipt。已记录的失败不自动“跳过即成功”。open 会尝试重访队列，持久 failed/blocked 数据仍可通过 status 检查。

SDK errors 包含 SQLite/IO、Schema/Invalid/IdentityMismatch、DatabaseInUse/ExternalChange/Poisoned、ConflictRequiresResolution、MissingDependencies、StreamFork、Busy、SyncInProgress、Closed 和结构化 NetworkError。网络错误有 Transient/Auth/RateLimit/Timeout/Cancelled/Missing/LostAck/Corrupt/Limit/Unsupported。lfs CLI 有固定 JSON 错误分类与退出码，见 DESKTOP。

Status 暴露 inbox、pending outbox、停止流数量与具体身份、outstanding_objects、不完整扫描和进程内最后同步错误。`healthy()` 表示没有已知 blocked/waiting/failed/未完成扫描；不等同于远端已完整备份。`SyncReport::healthy()` 还要求枚举完整且无 pending outbox。并发远端修改和远端谎报枚举完整无法由本地 SDK 证明不存在。模型明确标记缺失/延迟/替换重复页为 incomplete，持久保留此标记直到一轮完整重扫；预算用尽也不会称完整。普通重复 key 去重；新 hash 不会被最大 seq 或旧 key 忽略。

事件保留最近64项，不含凭据、SQL 参数或 packet 内容；完整持久状态以 SQLite 为准。历史状态接口返回集合，未实现大规模分页历史浏览或性能保证。预算详见协议。

## schema 与连接边界

Canonical STRICT 普通表，首列显式 TEXT NOT NULL PRIMARY KEY，最多64表/64列；拒绝其他 key 类型、复合 PK、额外 index/UNIQUE、trigger/view/FK/generated/custom collation/DDL。`Table::create_sql` 是受支持 canonical DDL。非空已有库必须 `Baseline::Import`（最多1000行/8 MiB），不捕获历史。

Authorizer 默认拒绝，只允许登记 main 表 SELECT/DML 和函数白名单；拒内部表、其他 schema、事务控制、ATTACH/DETACH、任意 PRAGMA 等。仅在已授权 statement 的执行阶段允许 Session 自己的 `table_xinfo`；callback 直接调用仍拒绝。新 Session 每次销毁；Err/panic 清权限、回滚；SQLite `OR ROLLBACK` 自行结束事务后不能吞错逃逸到自动提交。

活动期 file+SQLite 排他锁拒正常外写；data_version/schema_version 和 canonical schema 检查是有限检测。raw file 篡改、关闭期业务行改写、绕锁或任意复制数据库不受支持，不能当备份/恢复。回调不得联网、必须有界；任意 Rust closure 无法被 SDK 沙箱控制。SQL progress hook 约5秒，不是硬 realtime；native Session changeset 分配先于净键限制。

## 事务错误与同步失败

受控 Write 使用 callback 范围的错误锁存。任意 statement/parameter/query/authorizer 错误都会 taint 本次事务，后续操作拒绝，callback 返回 Ok 仍全量回滚；新 callback 使用新锁存。Session sqlite3_step 内部的 table_xinfo 授权窗口仍保留，caller PRAGMA 不获得权限。没有 SQL 文本正则特例。

接收先校验再判定分叉，未知父 scope 等待；只停止实际 fork 流，保留首先合法应用的投影。revisit 单包错误持久记录后继续其他候选；receive 自己的失败仍返回错误。sync 对缺失/损坏单对象返回 unhealthy 报告并继续其他对象；Auth/Cancelled 和数据库运行故障仍终止调用。持久欠账与扫描游标独立，下载失败不把扫描重置到第一项。完整性失败可有界重下载，永久 schema/constraint 阻塞不转成虚假的同步完成。

## 附件、恢复与 CLI

`lfs-sqlite::attachments` 管持久状态、SDK 自有文件根与事务引用；`backup` 管 SQLite Backup、一致文件闭包、manifest 校验和 incarnation 轮换。`lfs-storage` 的增量 writer/reader 在事务外传输和验 hash；`lfs-cli` 只通过 Sdk facade 操作用户 SQL，不获得裸写连接。附件根按完整 DB 文件名追加 `.attachments`，无丢失扩展名的目录碰撞。备份和恢复不携带 backend 配置或秘密。完整接口、失败与恢复约定见 [DESKTOP](DESKTOP.md)。

## Node 所有权

`bindings/nodejs/index.js` 进行有界 tagged JSON 编解码，直接调用 `lfs-node` Node-API8 addon。每 handle 一个 native actor 和一个 current-thread Tokio runtime，全部 SQLite/文件/同步任务在 actor 串行执行，共享核心 `Sdk` 保留 Session/因果/outbox/backup 职责。16 个 process handles，16 个未完成请求/handle，额外一个 close 保留槽；重复 sync Busy；close 先拒新请求再排空。AsyncTask 在 libuv 等待回复，Arc/channel 生命周期不暴露 Rust 借用或可写连接。核心 Cancellation 通过 native token 由 AbortSignal 驱动。详细 API 与取消时机见 [Node README](../bindings/nodejs/README.md)。
