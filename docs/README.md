# LocalWeave 文档

LocalWeave is a source-distributed SQLite local-first sync SDK. 文档面向接入者和贡献者，当前能力不等于所有目标已完成。

| 分层               | 阅读入口                                                                                                                                                                                      |
| ------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 快速开始与源码接入 | [项目 README](../README.md)、[交付说明](DELIVERY.md)                                                                                                                                          |
| 架构与状态归属     | [架构](ARCHITECTURE.md)、[不变量](INVARIANTS.md)                                                                                                                                              |
| API 与使用         | [Rust 示例](../core/sqlite/examples/local_loop.rs)、[双设备 Fs 示例](../integrations/opendal/examples/two_devices.rs)、[桌面/CLI](DESKTOP.md)、[Node JS/native](../bindings/nodejs/README.md) |
| 协议与决策         | [Protocol](PROTOCOL.md)、[Session ADR](adr/0001-session-to-logical-operations.md)、[Drive 原型 ADR](adr/0002-drive-record-ids.md)                                                             |
| 宿主与依赖         | [SQLite/OpenDAL 接入](HOST_DEPENDENCIES.md)、[第三方声明](THIRD_PARTY.md)                                                                                                                     |
| 测试               | [本地检查与 CI](TESTING.md)、[验证范围](TEST_REPORT.md)、[后端测试](testing/TEST_BACKENDS.md)                                                                                                 |
| 能力与限制         | [能力矩阵](CAPABILITIES.md)、[已知限制](KNOWN_LIMITATIONS.md)、[延期 Drive 原型](DRIVE.md)                                                                                                    |
| 目标与贡献         | [需求](REQUIREMENTS.md)、[后续路线](ROADMAP.md)、[贡献指南](../CONTRIBUTING.md)                                                                                                               |

开发入口：[开发指南](DEVELOPMENT.md) · [代码规范](CODING_STANDARDS.md) · [变更记录](../CHANGELOG.md)
