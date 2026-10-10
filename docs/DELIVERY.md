# 源码交付说明

[文档目录](README.md) · [能力矩阵](CAPABILITIES.md) · [测试报告](TEST_REPORT.md)

LocalWeave 当前提供 Rust workspace、CLI、Node 原生绑定源码、锁文件、示例与测试；尚未发布 crate、npm 包或跨平台预编译安装包。workspace 版本为 `0.1.0`，保留既有 `lfs-*` crate 与 `lfs` CLI 名称；Node 包名为 `@local-weave/sqlite`，仍为 private 包。

| 入口                                                          | 用途                                                     |
| ------------------------------------------------------------- | -------------------------------------------------------- |
| [lfs-protocol](../core/protocol/Cargo.toml)                   | canonical packet、typed values、纯 MV register           |
| [lfs-sqlite](../core/sqlite/Cargo.toml)                       | 受控 Session、Store/Sdk、因果状态、附件和恢复            |
| [lfs-storage](../integrations/opendal/Cargo.toml)             | 可选 OpenDAL adapter、显式服务 features、延期 Drive 原型 |
| [lfs-cli](../bin/lfs/Cargo.toml)                              | 单次 JSON 命令 `lfs`                                     |
| [Node 源码包](../bindings/nodejs/README.md)                   | `lfs-node` Node-API addon 与 JS/TS API                   |
| [双设备示例](../integrations/opendal/examples/two_devices.rs) | Fs、冲突、附件、共享快照和恢复闭环                       |

默认宿主 SQLite 与显式 bundled 的构建/运行要求见 [HOST_DEPENDENCIES](HOST_DEPENDENCIES.md)。路径依赖接入见 [README](../README.md)。Node 构建脚本目前只验证 Linux，旧二进制未重建，不能继承最新 Rust 修复资格。

应用必须通过 SDK 写连接修改受同步表，提供稳定设备身份与受支持 schema，独占管理数据库及附件目录。凭据由宿主提供，不随备份交付。恢复只接受完整校验的受支持 manifest，新的操作轮换 incarnation，旧 pending bytes 不变。裸相对备份/恢复目的目录存在已知问题，请使用 `./backup` 或绝对路径。

项目采用 [Apache-2.0](../LICENSE)；第三方依赖各自保留原许可，见 [THIRD_PARTY](THIRD_PARTY.md)。未来二进制分发须按实际目标收集适用 LICENSE/NOTICE 并执行目标平台运行验证；当前文档不构成此类分发或验证的声明。
