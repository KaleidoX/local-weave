# 开发指南

[文档目录](README.md) · [贡献流程](../CONTRIBUTING.md) · [代码规范](CODING_STANDARDS.md) · [测试资格](TESTING.md)

## 工具准备

从仓库根目录执行。Rust 使用 [rust-toolchain.toml](../rust-toolchain.toml) 的 1.96.0、rustfmt 和 Clippy stable。C 工具链、libclang 与 SQLite profile 由[宿主依赖指南](HOST_DEPENDENCIES.md)规定。统一门禁显式使用 bundled，不改变 SDK 默认宿主 SQLite 策略。

开发门禁需要 Node 22.13+（Node 22 系列）和 Python 3.11+（标准库 TOML 解析）。SDK Node runtime 下限仍由 package.json 的 engines 定义。npm 与现有模块 lock 保留，不建立根 npm workspace。工具版本以 [Node package.json](../bindings/nodejs/package.json) / lock 和 [Python 开发工具锁](../scripts/requirements-dev.txt) 为准。

```sh
npm ci --prefix bindings/nodejs
python3 -m venv scripts/.venv
scripts/.venv/bin/python -m pip install -r scripts/requirements-dev.txt
python3 scripts/check.py --help
```

安装是显式准备步骤；检查入口不会下载工具或执行修复。可用 `--ruff /path/to/private/venv/bin/ruff` 指向同版本私有工具。精确版本、第三方许可见[声明](THIRD_PARTY.md)。

## 统一入口

```sh
python3 scripts/check.py
python3 scripts/check.py format-check lint type
python3 scripts/check.py test --scope node
python3 scripts/check.py lint --scope rust --offline
# 下列入口会写入受控源码集合；先检查本地改动：
python3 scripts/check.py format
```

默认依次运行 format-check、lint、type、test。检查不会改写源码；编译缓存和临时测试产物会写入并按用途保留或清理。`format` 不能与检查混用。scope 支持 all/rust/node/python/docs；不适用组合、缺失工具与零文件均失败。命令逐条打印退出码，任一失败不算通过。CI 复用这些入口，测试组合与失败准入只在 [TESTING](TESTING.md) 维护。

格式集合在 [scripts/project_files.py](../scripts/project_files.py) 明确限定：手写 Rust、Node JS/TS、工程工具及模块测试支持 Python、公开 Markdown/JSON/YAML；不遍历秘密、内部任务、依赖、构建缓存、二进制、第三方、生成清单或 golden。全部公开 Python 共用 `scripts/ruff.toml`。TOML 只解析，不增加格式插件。Prettier 不格式化 Markdown 内嵌代码，避免改变示例语义；示例变更须真实消费验证。

## 工程工具与测试支持

根 `scripts/` 保留跨模块工程工具；测试支持脚本归所属模块。以下路径均相对仓库根目录。

| 入口                                                 | 保留用途                                                                                   |
| ---------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| `scripts/check.py`                                   | 本地与 CI 的格式、lint、类型和测试统一入口                                                 |
| `scripts/check_docs.py`                              | 公开文档链接、工程引用与配置解析检查                                                       |
| `scripts/project_files.py`                           | 门禁与交付清单共用的精确公开文件集合；作为模块导入                                         |
| `scripts/source_manifest.py`                         | 不依赖 Git index 的交付快照生成与校验                                                      |
| `scripts/dependency_inventory.py`                    | 从 Cargo metadata 生成依赖许可表；必须从仓库根执行，输入范围见[第三方声明](THIRD_PARTY.md) |
| `bin/lfs/tests/cli_smoke.py`                         | CLI 进程、JSON、Fs 同步、附件、备份恢复、锁和错误退出码回归                                |
| `bin/lfs/tests/shared_snapshot.py`                   | 共享快照、未发布 outbox 边界与恢复身份回归                                                 |
| `integrations/opendal/tests/http_services.py`        | 显式启动本地 rclone WebDAV/S3 服务并运行 Rust 服务回归                                     |
| `integrations/opendal/tests/fixtures/drive_model.py` | Rust Drive 测试启动的合成 HTTP 夹具，不代表真实云验证                                      |
| `bindings/nodejs/test/webdav_services.py`            | Node addon 的本地 WebDAV 认证、失败与恢复验证                                              |
| `bindings/nodejs/test/s3_services.py`                | Node addon 的本地 S3 服务、故障与截断验证                                                  |

CLI 用法见[测试指南](TESTING.md)，本地服务准备见[后端测试](testing/TEST_BACKENDS.md)和 [Node 指南](../bindings/nodejs/README.md)。Node 的 `build.mjs`、`check-yaml.mjs`、`example.mjs` 与 `eslint.config.mjs` 分别承担构建、配置校验、使用示例和 lint 配置职责，继续保留。

## 交付快照

[SOURCE_MANIFEST.json](../SOURCE_MANIFEST.json) 是交付快照，不是每次贡献必须手填的文件。准备源码交付或独立验收候选时运行：

```sh
python3 scripts/source_manifest.py generate
python3 scripts/source_manifest.py check
# 可先生成到外部候选，不覆盖现有快照：
python3 scripts/source_manifest.py generate --output /tmp/local-weave-manifest.json
python3 scripts/source_manifest.py check --output /tmp/local-weave-manifest.json
```

生成与校验使用同一公开源码集合，不依赖 Git index，支持 unborn 仓库。包含公开 AGENTS、docs、锁文件及手写源文件；排除 `.agents`、凭据、node_modules、target、native 二进制和清单自身。新增目录或交付文件种类须审查集合规则。日常源码修改后旧快照可过期，默认门禁不要求每次重建；交付必须校验。哈希证明完整性，不证明来源可信或测试资格。
