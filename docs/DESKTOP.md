# 桌面附件、备份与 lfs CLI

当前独立 SDK 默认使用宿主引擎，构建前须满足 [宿主依赖指南](HOST_DEPENDENCIES.md) 的头文件、ABI、loader 和 Session 条件；bundled 需 `--features bundled` 显式选择。`lfs help`、`lfs diagnostics`、`lfs doctor` 不需要 DEVICE_DIR；其他设备操作需要目录，restore 则为 BACKUP_DIR NEW_DIR。diagnostics 是静态元信息，doctor 才执行引擎兼容检查，但也不验证后端。

## 公开 SDK 与文件所有权

`Sdk::add_attachment(reader, limit, cancellation)` 按 64 KiB 拉取同步 `Read`；宿主不得提供无限阻塞的 reader。`download_attachment(transport, hash, limit, cancellation)` 按需下载；`open_attachment(hash, cancellation)` 返回从头定位、重新验过完整 SHA256 的只读 File；`attachment_status` 返回 Missing/Installed/Available 的持久状态。默认同步附件上限 256 MiB，add/download 可配置 `1..=1 TiB`，`sync_once_with_attachment_limit` 为上传设置同样的范围。限制是总字节上限，不是对耗时或所有元数据内存的承诺。

对数据库 `notes.db`，SDK 专用根是 `notes.db.attachments/`，子目录 `staging/objects/quarantine`；调用者必须独占管理这些目录，不能放入其他数据。哈希只接收 64 位小写 SHA256 hex，绝不按远端路径拼接；本地所有路径组件拒绝 `..`、symlink 和特殊文件。路径检查不能防御恶意本地进程同时换链；未提供 openat 沙箱。活动数据库的文件锁拒绝重复打开；原始拷贝不构成受支持恢复。

落盘顺序为 staging 行与新文件 → 分块写入并验 hash → file fsync → 记录完成的 staging hash/size → hard-link 安装到 hash 唯一路径 → objects 父目录 fsync → Installed 元数据 → 短 SQLite 事务切到 Available 并移除 staging 行 → 清除临时链接并 fsync staging 目录。同一 SDK 根内要求文件系统支持 hard link；不跨盘安装。Filesystem 与 SQLite 没有联合原子事务。

打开库时执行 reconciliation：完整已安装对象重验后可用，丢失/损坏对象标 Missing；未完成 staging 与无法归属的对象移到 quarantine。它不删因果引用，不做远端 GC；quarantine 保留人工恢复可能性，当前无自动回收配额或按字节续传。`add_attachment_observed` 在真实持久化屏障回调，仅用于宿主观测/故障验证，不可重入 SDK。进程恢复测试由测试控制进程在四个屏障 SIGKILL 子进程后重开验证；这不等于硬件掉电测试。

引用通过 `write_transaction_with_attachments(sorted_unique_hashes, callback)` 或 `resolve_with_attachments(..., sorted_unique_hashes)` 显式绑定。每项必须是已落盘可用的本地内容，事务无净记录变化时拒绝携带引用。业务操作、引用元数据、序号、DAG 和完整 outbox wire 在同一 SQLite 事务提交。引用是 **packet 级保守闭包**，该批次涉及的每条记录共享附件集合；需要精确记录归属时将记录分批写入。`record_attachment_references(table, key)` 返回全部历史版本引用，包含已覆盖祖先、冲突与墓碑；普通写或 resolve 不隐式根据行里的字符串猜测附件。

上述 write / resolve 的**每个显式引用**（包括重复传入历史 hash）都在业务 callback / resolve 前调用 `open_attachment`：要求本机 `Available` 且完整 SHA256 校验通过。已知 Missing 当前返回 Rust `Error::Invalid`、Node `SdkError.code='Invalid'`、CLI `invalid` / exit2；业务行、序号、DAG、outbox 不提交，Node `status()` 不变。失败事件可记录；若原 Available 文件校验损坏，附件状态可降为 Missing，I/O 等失败使用对应错误类型。宿主须先 add / download 成功，再显式引用；CLI 对应 `attachment-add` / `attachment-download` → `write` / `resolve`。受控事务不负责联网。

已有记录保持原附件引用、只更新其他字段的普通离线编辑，可省略显式引用（Rust 普通 write、Node 省略第二参数或 `[]`、CLI `attachments:[]`）；本体 Missing 不阻止这种编辑。全行原 hash 与因果祖先包的引用历史保留，无需每次重复所有 hash。已有 Linux Node/Fs 三端验证覆盖离线编辑、重开 pending、历史引用和精确文件字节；不能据此推断任意字符串或无关记录的附件关联。

LFSE v1 的预留引用字段现在接收严格排序的 hash；LFS1 payload、已提交旧 wire、OpId 和 predecessor 不变。接收方先把完整引用集持久保存到 `_lfs_refsets`；实际 apply 事务同时写永久引用和 Missing 文件元数据。相同 payload 的有效引用变体视为分叉，停止该流。非法路径/hash 不得触碰文件系统。旧程序不能理解新元数据/附件，需全体升级；没有全协议 epoch/schema 转换。

同步对每个引用先上传附件并完整流式回读 SHA256，再发布原始固定 envelope，再核对完整 envelope 才 ack。附件失败保留 exact outbox；失败/cancel 的下载不标 Available。Fs 与 S3 用 OpenDAL writer 分块写入，缓冲上限按 5 MiB chunk、并发 1；S3 走 multipart（小文件允许有界单次 PutObject）。reader 使用 64 KiB chunk。每调用默认 15 秒 timeout，可通过 Options 调到最多 120 秒；丢弃/取消 S3 future 可能留下远端未完成 multipart，本 SDK 不自动远端 GC。WebDAV 的 OpenDAL OneShotWriter 不能满足流式上传，附件明确 Unsupported，记录同步继续可用。

## 备份与恢复

`Sdk::backup(new_dir, kind, BackupOptions, cancellation)` 使用 SQLite Backup API，默认每步64页、最多100000步、Busy/Locked 最多重试10次（各10ms）。执行时持 SDK mutex，不联网；步间检查取消。目标必须不存在；失败清理本次新目录。存在 active/stale staging 或 Installed 中间状态时返回 Busy，须等传输完成或关闭重开进行 reconciliation 后再导出。禁止复制活跃 DB 文件充当备份。

- `LocalDisaster` 保留业务、DAG、墓碑、收件状态、旧身份与所有 pending exact payload/wire；包含全部历史引用和所有 Available 本地缓存（包括未被记录引用的私有文件）。使用一致 SQLite snapshot 决定不可变文件闭包。
- `SharedBootstrap` 只在 pending outbox=0、没有停止流/未完成因果状态/下载欠账/不完整扫描时导出。同行 published p 后写 unpublished q 也拒绝；不删除 outbox 来伪造共享切面。冲突本身可完整备份。仅包含全部已发布历史引用的附件，覆盖祖先、已覆盖版本、分支和墓碑，不包含无关本地缓存文件或其 `_lfs_files` 元数据。

两类都要求 **全部已应用历史引用** 文件可用；远端引用尚未下载时先下载再备份，否则拒绝。它比仅要求 pending 引用更保守。staging 文件/行、quarantine、CLI 配置和凭据不进入备份。目录包含 `store.db`、`objects/<sha256>`、最后写入并 fsync 的 `manifest.json`；清单按路径排序，含文件大小十进制字符串/hash、完整 schema、协议、元数据和备份格式版本。manifest 无墙钟、无随机排序，不是签名。

共享副本先设置 `PRAGMA secure_delete=ON`，删除无关 `_lfs_files` 行，再执行 `VACUUM`，重建 SQLite 页面，去除被丢弃的元数据及旧空闲页残留；只处理 snapshot，不修改源 DB/cache，也不删除源对象。备份通过副本 Available 状态和复制后 SHA256 校验文件，失败/取消不将源文件改标 Missing。清理 SQL/VACUUM 每约 1000 条 SQLite VM 指令检查取消，阶段前后和 manifest 发布前也检查；页步预算只约束 Backup API，不是 VACUUM 的耗时/磁盘预算，取消不保证打断正在执行的 OS I/O。普通失败清理新目录；强制终止可能留下含未清理数据的未完成目录（无最终 manifest），宿主应仅分享成功完成的目录并清理自己拥有的失败目录。未提供硬件掉电或安全擦除磁盘介质保证。

`Sdk::restore(backup_dir, new_destination_dir, cancellation)` 验证完整 manifest、路径、文件 SHA256、SQLite integrity/canonical schema、packet/predecessor、DAG/heads/投影、引用和 outbox；失败不安装可用库，原库/备份不改。恢复仅支持当前 `format=1/protocol=1/internal=[2,1,1,1]` 的完整备份，不能覆盖目录。成功库位于 `new_destination/store.db`，使用 256-bit OS 随机新 incarnation，后续 NEW 操作 seq 从1开始；所有旧操作 ID 和 pending payload/wire 不变，ack 按旧包实际身份定位。恢复两个副本得到不同新流，可同步并产生正常并发冲突。

旧时点备份只包含该时点已知数据；后续远端历史通过正常同步补回。不能把旧备份文件覆盖正在运行的库，也不能手改 incarnation/next_seq。错 dataset/schema、损坏/缺文件、缺 DAG 节点均拒绝。原始 DB 文件复制/离线修改、完整协议迁移、历史重构、published-only 重构和远端历史被删后的补救均不支持。验证会读完整历史元数据，无大历史性能保证。

## lfs 命令与 JSON

构建：`cargo build -p lfs-cli --offline --locked`；可执行文件 `target/debug/lfs`。单次命令退出，没有默认 daemon。所有 stdout 恰好一行 JSON：成功 `{"ok":true,"result":...}`，失败 `{"ok":false,"error":{"kind":"invalid","code":2}}`；错误不打印 SQL、路径、URL、配置或底层凭据。`diagnostics` 输出固定能力信息。

初始化使用显式 schema 定义，不接收任意 DDL：

```bash
mkdir -p "$PWD/target/stage4-tmp/remote"
./target/debug/lfs init "$PWD/target/stage4-tmp/device-a" <<EOF_CONFIG
{"identity":["demo","a","unique-first-install"],"schema":{"version":1,"tables":[{"id":1,"name":"notes","columns":[{"name":"id","kind":"Text","nullable":false},{"name":"body","kind":"Text","nullable":false}]}]},"backend":{"kind":"Fs","root":"$PWD/target/stage4-tmp/remote"}}
EOF_CONFIG
./target/debug/lfs write "$PWD/target/stage4-tmp/device-a" <<'EOF_WRITE'
{"statements":[{"sql":"INSERT INTO notes VALUES (?1,?2)","params":[{"type":"text","value":"x"},{"type":"text","value":"offline"}]}],"attachments":[]}
EOF_WRITE
```

`config DEVICE_DIR` 返回身份/schema和后端类型；初始化时保存 `config.json`，没有后台配置发现。WebDAV 只支持显式 Basic 来源：`{"kind":"WebDAV","endpoint":"https://host/path/","username":"host-user","password_env":"HOST_SELECTED_PASSWORD"}`。S3 为 `kind/endpoint/region/bucket/access_key_id_env/secret_access_key_env/session_token_env`；最后一项可省略。只在同步/下载时读取指定变量，无默认名称、目录扫描或 credential chain；原始 secret 字段被拒绝。`allow_loopback_http:true` 仅供显式 localhost 测试。初始化不登录或验证云端。Google Drive 记录配置为 `kind: "GoogleDrive"`、`root_folder_id`、`access_token_env`，可选显式 `owned_loopback_origin` 仅用于模型测试；详见 [DRIVE](DRIVE.md)。该配置仅为保留原型：Drive DEFERRED / PROTOTYPE_NOT_ACCEPTED，当前不属于可用后端；真实 Drive 未验证，附件 Unsupported。

| 命令                               | 输入/结果                                                                                                      |
| ---------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| `status DIR`                       | pending、blocked、conflicts、healthy；计数十进制字符串                                                         |
| `read DIR`                         | stdin `{"sql":"SELECT ...","params":[]}`；typed rows                                                           |
| `write DIR`                        | stdin `{"statements":[{"sql":"...","params":[]}],"attachments":[]}`；单受控事务，多条语句分别提交给 authorizer |
| `sync-once DIR`                    | 显式使用该设备后端；不完整/阻塞以 exit7 退出                                                                   |
| `conflicts DIR`                    | 每个 head 的 canonical OpId hex 与完整 typed row/tombstone                                                     |
| `resolve DIR`                      | stdin `{"table":1,"key":"x","observed":["<head-id-hex>"],"row":[...],"attachments":[]}`；row=null 表示删除     |
| `attachment-add DIR FILE`          | 流式导入宿主指定普通文件，返回 hash/size/Available                                                             |
| `attachment-download DIR HASH`     | 该后端按需下载到 SDK 自有目录，完整校验后返回 Available                                                        |
| `backup DIR NEW_DIR local\|shared` | 返回 manifest；默认 local                                                                                      |
| `restore BACKUP_DIR NEW_DIR`       | stdin 为显式 backend 对象；恢复后保存新 config，无旧凭据复制                                                   |
| `diagnostics` / `help`             | 无需 DEVICE_DIR；静态资格/用法，不证明引擎或后端运行                                                           |
| `doctor`                           | 无需 DEVICE_DIR；真实私有内存 Session I/U/D 与 rollback，无用户库/配置读取                                     |

Typed 值：`{"type":"null"}`、`{"type":"integer","value":"9223372036854775807"}`、`{"type":"real","value":"3ff4000000000000"}`（16位小写hex IEEE754 f64 bits）、`{"type":"text","value":"..."}`、`{"type":"blob","value":"00ff"}`。不把 i64 转为 JSON number；拒绝非有限 f64。表/列的 u32 标识仍可安全使用 JSON number。SQL 写入仍受 SQLite STRICT affinity 规范化。

退出码固定为：0 success、2 invalid（含 corrupt/missing/schema）、3 conflict、4 busy、5 auth、6 transient/timeout/rate-limit、7 blocked、8 cancelled、9 unsupported action/backend。`--cancel` 请求开始即取消；`--timeout-ms=N`（1..120000）由宿主给操作设置取消计时，网络 await/流式分块/备份步骤检测 token；它不是阻塞第三方 Read 的强制线程终止器。SIGINT/SIGTERM 由 OS 默认终止进程，不承诺 JSON exit8；恢复状态机处理留下的 staging。

## 可复现入口

```bash
export TMPDIR="$PWD/target/stage4-tmp"
mkdir -p "$TMPDIR"
cargo fmt --all --check
cargo check --workspace --all-targets --locked
cargo test --workspace --locked
cargo run -p lfs-storage --features fs --example two_devices --offline --locked
cargo build -p lfs-cli --offline --locked
python3 bin/lfs/tests/cli_smoke.py
```

`two_devices` 只使用 public facade + OpenDAL Fs，自建/清理 TMPDIR 子目录，覆盖离线新增/编辑/删除、附件内容、按需可用性、完整行冲突和 caller-observed resolve、两端读回相等及 shared backup/restore。CLI smoke 使用真实 lfs 和 JSON，另测 int64/f64/blob、退出码、脱敏与 authorizer。测试 fault HTTP 只证明错误分类，不能替代 [真实后端服务测试](testing/TEST_BACKENDS.md)。
