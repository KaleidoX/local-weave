# Host-provided dependencies

`lfs-sqlite` 默认不内嵌 SQLite。workspace 固定 `rusqlite = 0.40.2`、`libsqlite3-sys = 0.38.2`，保留 Session/hooks/limits/backup 所需功能。核心不依赖 OpenDAL、Tokio 或 HTTP；下方适配器部分说明当前存储适配器及消费者的 feature 选择。

## 构建与运行是两种要求

从源码构建默认 profile 时，宿主须提供相互匹配的 SQLite 头文件和可链接库，以及 Rust/C 工具链和 Session bindings 使用的 libclang/bindgen 环境。按宿主平台配置库搜索路径；Linux 可显式设置 `SQLITE3_INCLUDE_DIR`、`SQLITE3_LIB_DIR` 和 `LIBSQLITE3_SYS_USE_PKG_CONFIG=1`，运行动态产物时使用相应加载器路径。只有头文件可见或编译成功，并不能证明实际加载的引擎具备可用 Session。

运行已构建的动态产物需要相容 ABI 的实际 SQLite 动态库及其依赖，不需要头文件、Rust 或 sqlite3 CLI。需要 `SQLITE_ENABLE_SESSION` 和 `SQLITE_ENABLE_PREUPDATE_HOOK`；SDK 还会通过真实受控捕获与回滚检查行为。缺少链接符号时，加载器可能在程序启动前失败，这种失败无法由 SDK 的 Rust `Result` 捕获。

便捷内嵌构建须显式选择 feature：

```sh
cargo test --locked --offline -p lfs-protocol -p lfs-sqlite --features lfs-sqlite/bundled
cargo build --locked --offline -p lfs-cli --features lfs-cli/bundled
# Node 仅声明转发；本次没有重建或验收 Node 制品：
cargo build --locked --offline -p lfs-node --features lfs-node/bundled
```

`lfs-sqlite`、`lfs-cli`、`lfs-node` 均为 `default = []`。CLI/Node 的 `bundled` 转发至 `lfs-sqlite/bundled`，后者启用两项 SQLite binding 的 bundled feature。Cargo 会统一 features；同一构建图中任何依赖启用 bundled 都会影响引擎选择，应结合 `cargo tree -e features` 和产物链接信息检查。Session bundled 构建仍需要构建工具，不能把它理解为零构建依赖。

## 真实启动诊断

```rust,ignore
let compatibility = lfs_sqlite::check_sqlite_compatibility()?;
assert!(compatibility.controlled_capture && compatibility.rollback);
```

公开 `SqliteCompatibility` 保持三个字段：`runtime: RuntimeInfo`、`controlled_capture: bool`、`rollback: bool`。现有 `RuntimeInfo` 不变。检查在独立私有内存连接中创建 canonical 普通表，复用生产 authorizer 与 `Write::execute` 的 step 保护，逐项核对 Session INSERT/UPDATE/DELETE 的主键、旧值/新值、操作数，以及真实回滚后的行和 changeset。成功标志仅在全部检查成功后返回；失败使用现有 `Error`。诊断不接受用户路径、不创建用户数据库，也不开放 SDK 用户内存数据库模式。

`Store::open` 在创建用户数据库文件、取得文件锁及初始化用户数据库之前执行同一检查。CLI `lfs doctor` 无需 `DEVICE_DIR`，不读取设备配置或凭据，成功输出例如：

```json
{"ok":true,"result":{"sqlite_version":"3.46.1","session":true,"preupdate_hook":true,"controlled_capture":true,"rollback":true}}
```

失败返回非零退出码，stdout 保持现有 JSON 错误结构，stderr 提供 SQLite 兼容检查错误。doctor 不证明后端、账户或同步服务可用。

Session 3.46.1 内部使用 `table_info`，3.53.2 使用 `table_xinfo`。两者仅在内部 `Write.execute` stepping、write=true 且目标为注册主库表时获准。调用者直接 PRAGMA、表值 PRAGMA、其他 schema、内部表、DDL、事务控制及不安全函数继续拒绝；拒绝后事务整体回滚。不提供任意 native UDF/schema plugin 入口，不捕获外部 ORM 连接的写入。

## 引擎与文件路径范围

Linux 已验证 system SQLite 3.46.1、独立 shared 3.53.2 和 bundled 3.53.2；这是具体引擎配置的运行证据，不代表任意 SQLite、SQLCipher、VFS 或其他 OS。当前路径入口在文件操作前拒绝原始、大小写敏感的 `file:` 前缀；`./file:basename` 和绝对普通字面路径允许。doctor 的私有内存检查保持不变。旧 Node 制品未重建，不继承这些 Rust 源码修复资格。

## 宿主 OpenDAL adapter

`lfs-storage` 是核心之外的可选 adapter，`default = []`。workspace 的 OpenDAL 0.59.4 只固定 `executors-tokio`，不再强制选择服务或 HTTP transport；`lfs-sqlite` 仍不依赖 OpenDAL。以下两种构造 profile 的网络策略归属不同。

**Native host profile** 消费宿主已配置的 `opendal::Operator`：

```rust,ignore
// 非 Fs：保留宿主 Operator、Layers、HTTP transport 和 executor。
let transport = OpenDalTransport::from_operator(op, Options::default())?;
// Fs：宿主启用 opendal/services-fs，无需启用 lfs-storage/fs。
let transport = OpenDalTransport::from_fs_operator(&absolute_root, op, Options::default())?;
```

两个入口不调用 `with_context`，不另建 HTTP client，不重放 Layers；构造不联网、不探测凭据、不写文件。必要 read/write/stat/list 能力不足返回 `NetworkError::Unsupported`；timeout 为零或大于 120 秒返回 `NetworkError::Limit`。调用期继续执行 adapter 的包校验、大小限制、取消与操作 timeout，但网络 endpoint、TLS、proxy、redirect、凭据来源和刷新归宿主。`allow_loopback_http` 不配置 native Operator 的网络策略。

Fs 必须使用 root-bound 入口。root 是已存在的绝对普通目录，拒绝 `.`/`..` 绕行及任一祖先 symlink；Operator scheme 必须是 `fs`，`op.info().root()` 也须通过相同检查并 canonicalize 到同一目录。保存实际根用于后续 `safe_path`、遍历异常拒绝、回读校验及 file/dir `sync_all`。OpenDAL Fs builder 会自行 canonicalize 输入；若它已消除原始 symlink 拼写，adapter 无法恢复该历史，只校验宿主显式 root 和 Operator 当前报告的实际 root。宿主必须独占管理根目录；检查不是针对恶意宿主 Layer、Context 或并发目录替换的沙箱。generic 入口拒绝 Fs，不能借此绕过根绑定。

**Strict convenience factory profile** 保留原 `fs`、`webdav`、`s3` 工厂签名及行为，通过 feature 显式选择。WebDAV/S3 工厂继续配置 bounded HTTP、no proxy、no redirect、endpoint 与凭据检查；不套用宿主 native Context 的策略。

| Adapter feature      | 启用内容                                                                                        |
| -------------------- | ----------------------------------------------------------------------------------------------- |
| `fs`                 | 原 Fs 便利工厂与 `opendal/services-fs`                                                          |
| `webdav`             | 原 WebDAV 工厂与服务，optional url/http/reqwest/OpenDAL reqwest transport                       |
| `s3`                 | 原 S3 工厂与服务，optional url/http/reqwest/OpenDAL reqwest transport                           |
| `drive-experimental` | 仅保留旧 DEFERRED Drive 原型、导出及其 optional HTTP/serde/serde_json 依赖；没有新增 Drive 资格 |

base64、sha2、futures-util 和 Tokio 用于 adapter 数据面，继续是必需依赖。serde_json 另作为既有 stage4 测试的 dev dependency；核心和 OpenDAL 的传递依赖仍可能包含 serde/serde_json/url/http，不能把关闭 adapter 的直接 optional 依赖误读成整张依赖图没有这些库。默认 native adapter 与只选宿主 Fs 的消费者图中没有 reqwest、S3、WebDAV 或 OpenDAL reqwest transport。CLI 显式启用四项 factory features，Node 显式启用 Fs/WebDAV/S3；Cargo feature 统一仍适用。

旧测试和示例通过 `required-features` 保留：fs/services 测试因共用 fixture/HTTP 检查需要 fs+webdav+s3，stage4/two_devices 需要 fs，旧 Drive 测试需要 fs+drive-experimental。未选择 feature 而跳过的目标不计为通过。

真实 native Fs 验证覆盖记录、附件、exact envelope 回读重试和 no echo；Context/Layer 的 Memory 测试属于模型。真实 WebDAV 仅验证指定 native Operator 场景，strict 工厂仍拒绝非 loopback HTTP，详见 [能力矩阵](CAPABILITIES.md)。能力检查不代表任意后端的同步语义已验证。

`lfs diagnostics` 仅输出固定元信息；实际引擎检查使用 `lfs doctor`。示例从根目录执行：

```sh
export TMPDIR="$(mktemp -d)"
cargo run --locked -p lfs-storage --features fs --example two_devices
rmdir "$TMPDIR"
```

服务目标需要 `--features fs,webdav,s3`，被 feature gate 跳过的零用例不能算通过。完整检查方法见 [TESTING](TESTING.md)。

## Android 宿主边界

Android 的数据库文件所有权锁使用 NDK `flock(LOCK_EX | LOCK_NB)`，随 SDK 持有的文件句柄关闭而释放。只有锁竞争返回 `DatabaseInUse`；其他系统锁错误保留为 `Error::Io`。其他平台仍使用标准库文件锁。Rust 1.96 的 Android 标准库文件锁不受支持，不能直接替代此适配。

宿主传入应用私有目录的真实路径；Android 返回的数据目录可能包含系统符号链接，应先解析已存在的应用目录，再构造数据库路径。SDK 仍拒绝符号链接，不能靠关闭该检查适配移动平台。

使用当前 reqwest/rustls HTTPS transport 时，Android 宿主必须在首次网络操作前初始化 `rustls-platform-verifier` 的 JVM、应用 Context 与类加载器，并打包与 Cargo.lock 中 `rustls-platform-verifier-android` 版本匹配的 Kotlin/Maven 组件。当前锁定 verifier 0.7.1 对应 Android 组件 0.2.0，二者版本号不同。传入 Application Context，初始化失败必须阻止网络使用；进程重启后重新初始化。此工作归应用宿主，SDK 不持有 Android Activity。测试 CA 只能进入调试夹具，不放宽正式应用的证书或主机名验证。

该 verifier 的 Android 组件显式读取 AndroidCAStore，不把应用 Network Security Config 中的 `@raw` 自定义 CA 自动作为信任锚。测试应使用独立虚拟机的临时系统/用户信任项，并在测试后移除；不能通过关闭 TLS 校验使测试通过。
