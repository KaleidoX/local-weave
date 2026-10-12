# 第三方依赖与许可

LocalWeave 自身使用 [Apache-2.0](../LICENSE)。第三方组件保留各自许可；项目许可证不替换其版权、许可或 NOTICE 义务。当前按源码接入，未发布二进制包。

根 [Cargo.lock](../Cargo.lock) 固定 Rust 依赖，成员 manifests 固定直接版本与 features；[DEPENDENCIES.tsv](DEPENDENCIES.tsv) 提供依赖许可清单。[Node lock](../bindings/nodejs/package-lock.json) 与 [NODE_DEPENDENCIES.tsv](NODE_DEPENDENCIES.tsv) 对应开发依赖；Node 没有运行时 npm 依赖。

Rust 清单生成入口为 [dependency_inventory.py](../scripts/dependency_inventory.py)。从仓库根目录执行，metadata 文件放在自选临时目录：

```sh
cargo metadata --locked --offline --format-version 1 > /tmp/local-weave-metadata.json
python3 scripts/dependency_inventory.py /tmp/local-weave-metadata.json
```

生成器读取输入 JSON 的全部 `packages`（包括 workspace 成员与传递依赖），按名称和版本排序，输出许可、最低 Rust 声明及来源；不自行解析 features、筛选平台或补齐缺失包。上述 metadata 使用默认 feature 选择，不传 `--filter-platform` 或 `--no-deps`；全目标依赖元数据不代表每个目标已构建。其他 feature/profile 应显式记录对应 metadata 命令并审查依赖集差异。脚本以当前工作目录定位并覆盖 `docs/DEPENDENCIES.tsv`，缺少 license 和 license_file 元数据时返回失败；试运行应在源码副本进行，更新正式清单前审阅差异。本次入口整理不重新定义既有清单的许可或依赖集。

| 直接组件                                 | 锁定版本                | 用途 / 许可                                   |
| ---------------------------------------- | ----------------------- | --------------------------------------------- |
| rusqlite / libsqlite3-sys                | 0.40.2 / 0.38.2         | Session、hooks、limits、backup；MIT           |
| SQLite（bundled opt-in）                 | 3.53.2                  | 内嵌引擎；public domain；默认由宿主提供引擎   |
| fallible-streaming-iterator              | 0.1.9                   | Session 迭代；MIT OR Apache-2.0               |
| sha2                                     | 0.11.0                  | 完整性哈希；MIT OR Apache-2.0                 |
| libc                                     | 0.2.190                 | Android 文件所有权锁 flock；MIT OR Apache-2.0 |
| getrandom                                | 0.4.3                   | 新 incarnation 随机源；MIT OR Apache-2.0      |
| opendal / opendal-http-transport-reqwest | 0.59.4                  | 可选存储适配与显式 HTTP transport；Apache-2.0 |
| tokio                                    | 1.53.2                  | adapter/native runtime；MIT                   |
| reqwest                                  | 0.13.5                  | 显式 rustls/stream HTTP；MIT OR Apache-2.0    |
| napi / napi-derive / napi-build          | 3.14.2 / 3.6.12 / 2.6.0 | Node-API8；MIT                                |
| TypeScript / @types/node                 | 6.0.3 / 22.20.1         | Node 开发类型检查；Apache-2.0 / MIT           |

完整传递图还包含 serde、serde_json、url、http、base64、futures-util 等，精确版本及许可见清单。核心不依赖 OpenDAL/Tokio/HTTP；默认 SQLite 不 bundled；OpenDAL 默认服务及 HTTP transport 关闭，由宿主 Operator 或明确的 adapter features 选择。Drive 原型没有获得生产支持资格。

本项目实际工具链为 Rust 1.96.0；单个依赖的最低 Rust 声明不能作为整个 SDK 的最低版本承诺。源码构建需要 C 工具链、libclang 和匹配的头文件/库，运行动态制品只需要 ABI 相容的运行库，详见 [宿主依赖](HOST_DEPENDENCIES.md)。无需 sqlite3 CLI 或额外 OpenDAL 服务进程。

rclone 1.75.1（MIT）仅用于可选本地 HTTP 验证，不是 SDK 运行依赖。未来二进制分发应按实际目标、feature 和打包内容收集第三方 LICENSE/NOTICE 全文并验证动态依赖；当前没有已完成该分发工作的声明。

开发静态门禁新增 Prettier、ESLint、@eslint/js、typescript-eslint、eslint-config-prettier、globals（MIT）及 YAML（ISC），精确直接版本见 [package.json](../bindings/nodejs/package.json)，传递版本/许可见 NODE_DEPENDENCIES.tsv。Python 私有开发工具 Ruff 0.17.0 使用 MIT，锁定见 [requirements-dev.txt](../scripts/requirements-dev.txt)。这些工具不进入 SDK 运行依赖。
