# 代码规范

[开发指南](DEVELOPMENT.md)规定工具和命令；[架构](ARCHITECTURE.md)规定目录与依赖方向；[不变量](INVARIANTS.md)和[协议](PROTOCOL.md)规定 SDK 行为。本文只维护代码与变更约定。

## 命名与格式

| 范围               | 命名                                                             | 格式权威                                             |
| ------------------ | ---------------------------------------------------------------- | ---------------------------------------------------- |
| Rust               | 模块/函数 snake_case，类型 PascalCase，常量 SCREAMING_SNAKE_CASE | rustfmt.toml：edition 2024，4 空格，100 列，stable   |
| JS/TS              | 函数/变量 camelCase，类型 PascalCase                             | Prettier：2 空格，singleQuote，semi；ESLint 不管排版 |
| Python             | 模块/函数/变量 snake_case，类型 PascalCase                       | scripts/ruff.toml：4 空格，100 列，基础错误 lint     |
| Markdown/JSON/YAML | 文件名遵循所在目录现有约定                                       | 共享 Prettier 配置                                   |

已有公共 crate/CLI/API 标识不因规范重命名。大范围纯格式化与逻辑修改分开供审阅，不夹带黄金向量、wire 字符串或测试断言调整。

## 依赖与边界

Rust 共享版本在 workspace 声明，模块 feature 由模块控制；npm 开发工具精确锁定在现有 Node 模块，Python 工具在项目私有 venv。依赖变更同时审阅 lock、许可、宿主兼容及默认 feature 行为，禁止用 all-features 替代资格矩阵。核心不得通过工具或适配器间接引入网络/runtime 依赖。第三方信息统一维护在[依赖声明](THIRD_PARTY.md)。

## 错误、异步与日志

输入、IO、网络和持久化失败返回现有结构化错误，不吞错为健康，不以重试直到成功掩盖失败。新增生产 panic/unwrap/unsafe 必须说明可审查的不变量理由；测试可用断言和 unwrap。取消、关闭、锁和事务归属依照架构与不变量，不借 lint 改生命周期。

日志不得输出凭据、SQL 参数、业务数据库内容或 packet 原文。验证记录给出命令、退出码、feature/profile、数据来源和边界，不将 compile-only、mock 或模型升级为真实后端资格。

## 兼容与评审

公开 API、wire/schema、身份、持久 outbox 或恢复语义变更须先说明兼容策略、迁移/回滚和反例验证。任何静态工具暴露的真实逻辑缺陷单独登记并评审，不使用 unsafe fix。提交与 PR 约定见[贡献指南](../CONTRIBUTING.md)，失败准入见[测试方法](TESTING.md)。
