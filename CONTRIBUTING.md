# 参与 LocalWeave

通过 [Issues](https://github.com/KaleidoX/local-weave/issues) 报告可复现问题、讨论接口和后端语义，通过 [Pull requests](https://github.com/KaleidoX/local-weave/pulls) 提交改进。项目当前按源码接入，尚未发布 crate/npm 或预编译安装包。

先阅读[项目入口](README.md)、[架构](docs/ARCHITECTURE.md)、[协议](docs/PROTOCOL.md)和[不变量](docs/INVARIANTS.md)。准备工具和运行检查的唯一操作参考是[开发指南](docs/DEVELOPMENT.md)；命名、依赖、错误和兼容约定见[代码规范](docs/CODING_STANDARDS.md)，验证组合与失败准入见[测试方法](docs/TESTING.md)。

## 问题与变更

报告源码版本、OS/架构、工具与 SQLite 版本、feature/profile、最小复现、预期/实际行为和脱敏退出码。不要上传凭据、私人数据库内容或带认证信息的 URL。

使用目的单一的分支和变更；提交消息为 `type(scope): 简体中文说明`，type 可用 feat/fix/docs/style/refactor/test/build/ci/chore。大范围纯格式化独立于逻辑变更审阅；本约定不自动授权 agent 提交或推送。

PR 说明问题与结果、接口/协议/持久状态影响、兼容或迁移策略、实际执行命令与退出码以及未验证范围。用户可见重要变化记录到 [CHANGELOG](CHANGELOG.md) 的 Unreleased；纯内部琐碎调整不强制增条目。没有配置远程分支保护或自动发布的承诺。

项目采用 [Apache-2.0](LICENSE)，第三方许可独立保留，见[第三方声明](docs/THIRD_PARTY.md)。交付前重新生成源码快照、核验许可、消费者与包名权属；发布另行评审。[文档目录](docs/README.md)提供进一步导航。
