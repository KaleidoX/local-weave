# 本地检查与 CI

工具准备和分项命令见[开发指南](DEVELOPMENT.md)。本地与 [CI](../.github/workflows/ci.yml) 复用 `scripts/check.py`；配置存在不代表 GitHub 远端已通过。

## 基础门禁与资格矩阵

| scope  | format-check                | lint                                             | type                                      | test                                                      |
| ------ | --------------------------- | ------------------------------------------------ | ----------------------------------------- | --------------------------------------------------------- |
| rust   | cargo fmt stable            | bundled workspace all-targets Clippy -D warnings | bundled workspace all-targets cargo check | bundled workspace cargo test，默认并行                    |
| node   | Prettier JS/TS              | ESLint flat config，推荐错误规则                 | strict tsc，包含全部类型用例              | 新 cargo bundled addon，在隔离目录运行 native/Fs API 测试 |
| python | Ruff format                 | Ruff 基础错误规则                                | 不适用                                    | 不适用                                                    |
| docs   | Prettier Markdown/JSON/YAML | JSON/YAML/TOML 解析、公开相对链接/工程引用       | 不适用                                    | 不适用                                                    |

默认入口按 format-check/lint/type/test 执行；每条命令打印 exit，失败停止。不自动修复、下载、忽略锁失败或重试直到绿。Node 仅运行无远端依赖的 `native.test.mjs`；隔离副本从本次 Cargo build 输出安装 addon，不读取源码目录旧 `.node`。公开 npm build 仍为宿主策略；门禁显式 bundled 只影响验收构建。

宿主 SQLite core、共享库与 bundled 是不同资格，按下面入口和宿主指南独立运行；不使用 all-features 代替矩阵。CLI 既有依赖启用 Drive 原型 feature，其模型测试不改变延期状态。WebDAV/S3/Drive 服务测试需显式准备与授权，不包含在默认门禁；ignored 或零用例不算通过。

## 失败准入

所有要求的门禁通过才可报告整体通过。已知默认并行 `stage1::reopen_exact_retry_acknowledgement_and_sequence` 曾发生 DatabaseInUse，详见[限制](KNOWN_LIMITATIONS.md)和[证据](TEST_REPORT.md)。保留原始失败，单例/串行诊断不能抵消，不为规范工作修改锁、断言或 CI 并行策略。超出变更范围的 SDK 语义修复单独评审。

## 核心与运行入口

```sh
cargo test --locked -p lfs-protocol -p lfs-sqlite
# 或默认成员，仅 protocol/sqlite：
cargo test --locked
export TMPDIR="$(mktemp -d)"
cargo run --locked -p lfs-storage --features fs --example two_devices
cargo build --locked -p lfs-cli
python3 bin/lfs/tests/cli_smoke.py
python3 bin/lfs/tests/shared_snapshot.py
rmdir "$TMPDIR"
```

CLI 脚本按 `CARGO_TARGET_DIR`（缺省 `target`）定位 `debug/lfs`；这些入口使用本地夹具，smoke 的 HTTP 仅为 loopback 故障服务。测试真实 Session、原子回滚、乱序与重复、冲突、schema 拒绝、附件、备份恢复和进程重开；进程中止不等于硬件断电。Node 构建、类型和运行入口见 [Node 指南](../bindings/nodejs/README.md)。

## 结果解释

每个 profile 单独计数，以测试二进制和 test name 为唯一身份；嵌套子进程摘要不重复加总。明确 ignored 的服务入口不能计通过，未启用 feature 而跳过的目标也不能计通过。模型、真实文件、本地 HTTP 和特定真实服务证据分别记录，详见 [后端测试](testing/TEST_BACKENDS.md) 和 [验证范围](TEST_REPORT.md)。
