# Changelog

记录面向使用者的重要变化，按 Added / Changed / Deprecated / Removed / Fixed / Security 分类。版本计划遵循 Semantic Versioning；当前 0.1.0 尚未发布，不代表已存在发布标签或安装包。

## Unreleased

### Added

- 提供 SQLite local-first SDK、桌面 CLI、双设备 Fs 示例与 Node 原生绑定源码；已验证能力与未完成项见[能力矩阵](docs/CAPABILITIES.md)。
- 增加公共开发规范、分项 lint/format/type/test 入口和源码交付快照工具。

### Changed

- 统一本地与 CI 静态检查及 Linux bundled Node 新构建验证；实际资格以[测试报告](docs/TEST_REPORT.md)为准。

### Fixed

- 修复 Android 上标准库文件锁不受支持导致数据库无法打开的问题；仅真实锁竞争返回 `DatabaseInUse`，其余锁错误保留 I/O 类型。Android x86_64 验证范围见[测试报告](docs/TEST_REPORT.md)。
