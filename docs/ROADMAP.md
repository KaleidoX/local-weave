# 后续路线

[文档目录](README.md) · [当前能力](CAPABILITIES.md) · [已知限制](KNOWN_LIMITATIONS.md)

以下是未完成方向，没有承诺发布日期。Rust/CLI、Node 与各后端的验证分别记录，不从一个平台推广到其他平台。

1. **本地可用性与新制品验证**：修复 backup/restore 裸相对目标的 empty-parent 问题；重建并验证 Node 的最新宿主引擎与路径拒绝行为；补充大历史、资源压力和故障恢复边界。
2. **后端兼容性**：分别验证更多 WebDAV 服务与真实 S3/AWS/STS，保留 strict 工厂和 native Operator 的策略边界；WebDAV 附件需要真正满足流式与取消语义的方案。没有自动远端 GC 计划可被当前调用者依赖。
3. **Google Drive — DEFERRED**：保留原型，三项反例未修复，记录与附件不属于已支持能力。后续必须重新评估对象布局、根目录约束及扫描进度，详见 [Drive](DRIVE.md)。
4. **Android — PLANNED / UNVERIFIED**：Android SDK/NDK 许可尚未接受，没有设备构建/运行资格。需要独立验证 ABI、SQLite Session、绑定、宿主安全凭据、取消、进程恢复和后台生命周期。
5. **iOS — PLANNED / UNVERIFIED**：需要独立 Apple 工具链与设备验证、绑定、SQLite 构建、宿主凭据和前后台生命周期，不能用 Android 或 Linux 结果代替。
6. **其他桌面平台与分发**：macOS/Windows 构建、动态依赖、文件锁、路径和 fsync/恢复语义须在对应平台实测，再考虑预编译包与发布流程。

浏览器不在当前范围。签名、E2EE、多用户权限、业务 schema/协议 epoch 迁移需单独设计及验证；没有占位 API 可被视为现有能力。
