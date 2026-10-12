# 验证范围

本文区分已验证行为与复现方法，不作为所有平台或服务的支持承诺。可执行命令见 [TESTING](TESTING.md)。

| 范围     | 已有运行结果与限制                                                                                                                                                                                                                                  |
| -------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| SQLite   | Linux system 3.46.1、shared 3.53.2、bundled 3.53.2 的真实 Session、受控写、回滚与路径契约通过                                                                                                                                                       |
| 因果同步 | 公共 API 覆盖多设备、排列/重复/缺依赖、分叉、显式 resolve、失败原子性及 exact outbox 重试                                                                                                                                                           |
| Fs       | 真实文件、完整 envelope、附件、native Operator、快照与恢复；含真实子进程中止后重开                                                                                                                                                                  |
| WebDAV   | rclone 本地服务；另有特定真实 endpoint 的 Rust native Operator/system SQLite 场景，64 KiB raw I/O、三设备同步与五包解码；不推广至 Node 或全部服务                                                                                                   |
| S3       | 仅本地 rclone 1.75.1 的记录/multipart 与 HTTP 故障注入；未验证 AWS/STS                                                                                                                                                                              |
| Node     | 既有 Linux Node 22.23.3 / N-API8 的 Fs/WebDAV/S3 本地原生验证；源码目录重排后另行新构建 system SQLite addon，在隔离副本实际加载，Fs 10 passed/0 failed/0 skipped 及双设备示例通过；未重跑 Node HTTP 服务，仓库旧 addon 未覆盖，不能继承新构建的资格 |
| Drive    | 原型模型测试不能抵消三项未解决反例；延期                                                                                                                                                                                                            |
| 其他平台 | macOS/Windows/Android/iOS 未验证，浏览器排除                                                                                                                                                                                                        |

路径契约提前拒绝原始 case-sensitive `file:`，允许 `./file:basename` 及绝对普通字面路径；doctor 私有内存检查保持不变。裸相对 backup 目标 empty-parent `NotFound` 仍存在，请使用 `./backup` 或绝对路径。

迁移前 system/bundled 工作区各 103 个唯一入口通过、1 ignored，shared 核心 57 个入口通过。这些是历史基线，不是当前目录或后续修改的自动验收结果；不同 profile 不相加为独立测试数量。运行新的检查后应以该次命令、原退出码和实际非零入口为依据。

## 职责分层迁移后的独立复核

- 默认核心测试在 system SQLite 下实际 57 个唯一入口通过；system 工作区与 bundled `scripts/check.py --offline` 各 103 个唯一入口通过、1 ignored。ignored 服务不计通过，嵌套子进程入口去重。
- 新的目录依赖路径下，system/shared/bundled 独立消费者均通过原始路径拒绝、零副作用、普通文件持久化、exact outbox、备份恢复与新 incarnation 契约。
- bundled 检查入口的 fmt/check/strict Clippy/tests，以及 system strict Clippy 均返回 0；CLI 完整 smoke、共享快照隐私检查、Fs 双设备示例和 README 原样 Rust 示例实际运行通过。
- 新编译 Node addon 的隔离 Fs 公共测试 10 passed/0 failed/0 skipped，Node 双设备示例与类型检查通过；没有覆盖仓库中的旧 addon，旧 addon 不能继承新构建的资格；未重跑 Node HTTP 服务，没有新增 Node 云端或其他平台资格。
- 本机复用已有 stable 别名，其实际 Rust/Cargo 为 1.96.0，并显式提供任务私有 SQLite/libclang/Clippy 环境；没有安装命名 1.96.0 工具链或全局组件。GitHub CI 尚未执行，本次没有新增真实远端服务请求。

## `local-weave` 定名与本地目录改名后的复核

- 从新目录实际完成 Cargo metadata、fmt、CLI/Node 构建、CLI smoke、Fs 双设备示例、README Rust 示例与独立持久化/路径/备份消费者；Node 隔离副本实际加载新构建 addon，Fs 10 passed/0 failed/0 skipped、双设备示例和类型检查通过。没有覆盖旧 addon 或重跑 HTTP/云端服务。
- 三个服务 runner 的 `--help` 返回 0；缺少必填 `--tool` 时返回 2，不创建 evidence/fixture，不启动服务或加载环境文件。
- **首次默认并行核心回归返回 101**：`stage1::reopen_exact_retry_acknowledgement_and_sequence` 在 `Store::open` 收到 `DatabaseInUse`。单独执行该用例返回 0；核心全组以 `--test-threads=1` 诊断执行返回 0，57 个唯一入口通过。串行与单例结果不覆盖或取消并行失败。
- 该次 Rust 源码、测试和 Cargo.lock 保持迁移前字节不变。并行锁竞争是待查假设，根因未确认，未修改 SDK、测试断言或 CI 并行方式；不能宣称最终并行回归全绿。见 [已知限制](KNOWN_LIMITATIONS.md)。

MODEL/FAULT 仅证明协议及失败路径；本地 HTTP 不等于云服务。真实 WebDAV 对象清理曾得到 DELETE 204 与后续 PROPFIND 404，严格工厂仍拒绝非 loopback HTTP，附件仍 Unsupported。所有资格边界见 [能力矩阵](CAPABILITIES.md)，风险见 [已知限制](KNOWN_LIMITATIONS.md)。

## 2026-10-10 Android 文件锁适配

Rust 1.96 Android 的 `File::try_lock` 返回 Unsupported；旧实现把所有锁错误映射为 DatabaseInUse。新 Android 路径使用 NDK flock，其他平台保留标准库实现。只有 WouldBlock 映射为 DatabaseInUse，其他错误为 Error::Io。文件所有权与 SQLite 连接仍由 Store 生命周期管理。

Linux x86_64 bundled profile 执行 `python3 scripts/check.py format-check lint type test --scope rust --offline`，整体退出码 0；包括默认并行 workspace 回归。既有 ignored 服务用例不计作通过。

Android-x86 9.0-r2 / API 28 上，NDK 28.2.13676358 编译的 CardWeave 私有公开 API consumer 实测同进程竞争拒绝、显式 close 后重开、独立进程持锁竞争拒绝、持锁进程终止后重开、身份不符导致 open 失败后正常重开，退出码 0。Flutter 3.47.7 / FRB 2.13.0 原生 APK 实测字段保真、SQLite Session、Fs 双端同步、并发编辑/删除冲突及显式解决；新的应用进程读回两端已解决数据与零 outbox。此结果仅适用于 Android x86_64、bundled SQLite 和指定本地夹具，不代表 ARM/iOS、系统 SQLite、GPU、硬件 KeyStore 或生产云资格。

同一 Android APK 另通过本地 rclone 1.75.1 HTTPS WebDAV 的双端同步、并发编辑/删除冲突、显式解决与独立进程重开。使用 AndroidCAStore 中的临时用户 CA 验证合法证书；错误主机名和未知 CA 均失败且 outbox 不被确认，回到正确 endpoint 后恢复；新应用进程重新进行 HTTPS 请求也通过。JNI 初始化使用 Application Context，Kotlin AAR 0.2.0 与 verifier 0.7.1 配套。该结果属于本地服务，不提升为生产云支持。
