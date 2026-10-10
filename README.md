# LocalWeave

LocalWeave 是 SQLite 本地优先同步 SDK：设备离线读写自己的数据库，通过对象存储交换记录、保留并发版本，并显式解决冲突。它不是数据库服务器或协调服务器，也不能捕获任意 ORM、其他连接或进程的写入。

**LocalWeave is a SQLite local-first sync SDK with controlled Session capture, atomic durable outbox, causal conflict tracking, attachments, and backup/restore. It does not require a coordination server.**

SDK 在自己拥有的单写连接上使用 SQLite Session，将净变化转换成完整行或墓碑；业务数据、持久序号、因果 DAG 和 exact outbox 字节在同库一次提交。远端包先校验再原子应用，不产生回声 outbox。网络操作不占 SQL 事务；冲突通过 `resolve` 明确处理，不依赖墙钟或自动字段合并。

## 从源码开始

仓库地址为 [KaleidoX/local-weave](https://github.com/KaleidoX/local-weave)。当前提供源码接入，**未发布 crate、npm 包或预编译安装包**；`lfs-*` crate 与 `lfs` CLI 名称保持不变；Node 包名为 `@local-weave/sqlite`，仍为 private 包。

```sh
git clone https://github.com/KaleidoX/local-weave.git
cd local-weave
cargo build --locked -p lfs-cli
cargo run --locked -p lfs-cli -- doctor
export TMPDIR="$(mktemp -d)"
cargo run --locked -p lfs-storage --features fs --example two_devices
rmdir "$TMPDIR"
```

已验证环境为 Linux x86_64 / Rust 1.96。默认使用**宿主 SQLite**：源码构建需要匹配的 SQLite 头文件/库、C 工具链和 libclang；实际加载的库必须 ABI 相容，支持 Session 与 preupdate hook。`doctor` 会在私有内存连接中实际检查 Session I/U/D 和回滚；编译成功或 `diagnostics` 的静态输出不能替代它。无需 sqlite3 CLI 或额外 OpenDAL 服务进程。配置和运行边界见 [宿主依赖](docs/HOST_DEPENDENCIES.md)。

明确选择内嵌 SQLite 时使用 `cargo build --locked -p lfs-cli --features bundled`。`bundled` 是 opt-in，仍需要 C/libclang 构建工具。依赖已缓存时可在 Cargo 命令中加 `--offline`。

根 workspace 按职责分为 `core/protocol`、`core/sqlite`、`integrations/opendal`、`bin/lfs` 和 `bindings/nodejs/native`；Node JS/TS 位于 `bindings/nodejs`。根 `cargo test` 默认只选 protocol/sqlite，完整检查见 [贡献指南](CONTRIBUTING.md)。

## Rust 最小接入

假设消费者工程与检出的 `local-weave` 目录同级：

```toml
[dependencies]
lfs-sqlite = { path = "../local-weave/core/sqlite", default-features = false }
```

下面的 `src/main.rs` 使用同步 `Store`，在新的 `notes.db` 中写入一行；重复运行前请使用新的数据库文件或更换记录键。

```rust
use lfs_sqlite::{Baseline, Column, ColumnType, Identity, Schema, Store, Table, Value};

fn main() -> lfs_sqlite::Result<()> {
    let schema = Schema { version: 1, tables: vec![Table {
        id: 1, name: "notes".into(), columns: vec![
            Column { name: "id".into(), kind: ColumnType::Text, nullable: false },
            Column { name: "body".into(), kind: ColumnType::Text, nullable: false },
        ],
    }] };
    let identity = Identity {
        dataset: "notes-demo".into(), device: "device-a".into(),
        incarnation: "first-install-a".into(),
    };
    let mut store = Store::open("./notes.db", identity, schema, Baseline::Reject)?;
    store.write_transaction(|tx| {
        tx.execute("INSERT INTO notes VALUES (?1, ?2)", &[
            Value::Text("unique-note-id".into()), Value::Text("offline note".into()),
        ])
    })?;
    println!("{:?}", store.read(|db| db.query("SELECT * FROM notes", &[]))?);
    Ok(())
}
```

同一设备重开必须保持 identity/schema；不同设备应使用不同 device/incarnation。已有非空数据库只能显式 `Baseline::Import`，不会补捕获历史。这里只产生本地持久 outbox；完整重开示例见 [local_loop.rs](core/sqlite/examples/local_loop.rs)。

异步同步、附件和恢复使用 `Sdk` facade；`lfs-storage` 提供可选 OpenDAL adapter，核心不依赖 OpenDAL、Tokio 或 HTTP。宿主可传入已有 Operator，或显式启用 `fs` / `webdav` / `s3` 工厂。接入方式见 [宿主 Operator](docs/HOST_DEPENDENCIES.md)，双设备完整流程见 [two_devices.rs](integrations/opendal/examples/two_devices.rs)。[桌面指南](docs/DESKTOP.md) 提供 CLI/JSON 和备份 API，[Node 指南](bindings/nodejs/README.md) 提供源码原生绑定用法。

## 使用边界

支持受限 canonical STRICT 普通表，首列为 `TEXT NOT NULL PRIMARY KEY`；额外 UNIQUE、FK、触发器、任意 DDL/迁移不受支持。共享快照拒绝未发布状态并排除无关私有缓存；本地灾备保留 pending exact bytes，恢复为新操作轮换 incarnation。哈希用于完整性，凭据由宿主提供；未实现签名、端到端加密或自动远端 GC。

Fs 记录与附件有真实 I/O 验证；WebDAV 有限定的真实服务记录验证，附件仍 `Unsupported`；S3 记录/multipart 仅验证本地 rclone 服务；Drive 是延期原型。旧 Node 制品未重建，不能继承最新 Rust URI 修复资格。移动端及其他 OS 未验证。备份/恢复裸相对目标 `backup` 存在 empty-parent `NotFound`，请使用 `./backup` 或绝对路径。完整边界见 [能力矩阵](docs/CAPABILITIES.md) 与 [已知限制](docs/KNOWN_LIMITATIONS.md)。

## 文档与贡献

工程工具与模块测试支持脚本的用途和入口见[开发指南](docs/DEVELOPMENT.md#工程工具与测试支持)。

[开发指南](docs/DEVELOPMENT.md) · [文档目录](docs/README.md) · [架构](docs/ARCHITECTURE.md) · [协议](docs/PROTOCOL.md) · [测试方法](docs/TESTING.md) · [验证范围](docs/TEST_REPORT.md) · [路线](docs/ROADMAP.md)

问题和改进建议通过仓库 [Issues](https://github.com/KaleidoX/local-weave/issues) 与 [Pull requests](https://github.com/KaleidoX/local-weave/pulls) 交流；贡献步骤见 [CONTRIBUTING.md](CONTRIBUTING.md)。

本项目采用 [Apache-2.0](LICENSE)。第三方依赖保留各自许可，见 [第三方声明](docs/THIRD_PARTY.md)。
