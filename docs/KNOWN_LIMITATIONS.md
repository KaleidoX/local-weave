# 已知限制

[文档目录](README.md) · [能力矩阵](CAPABILITIES.md) · [路线](ROADMAP.md)

当前验证范围为 Linux x86_64 / Rust 1.96；Node 既有验证使用 Node 22.23.3、Node-API 8。macOS、Windows、其他 Node 版本、Bun、Android/iOS 未验证；浏览器不在当前范围。

## 数据库路径与宿主引擎

当前 Rust 源码在 `Store::open`、`Sdk::open` 以及 backup/restore 派生数据库路径的文件操作之前拒绝原始、大小写敏感的 `file:` 前缀，防止将 URI 当作持久文件入口。普通 `./file:basename` 和绝对路径中的字面 `file:` 文件名允许；普通文件名中的 `?`、`#` 不因此禁用。doctor 的私有内存诊断不受此变更影响，也不向调用者开放 URI/memory 数据库。旧 Node 二进制没有重建，不能声称已经具有该修复。

**backup/restore 的裸相对目标如 `backup` 存在 empty-parent `sync_dir` 返回 `NotFound` 的缺陷。请使用 `./backup` 或绝对路径。该问题尚未修复。**

宿主 SQLite 必须匹配头文件、ABI 和 Session/preupdate 行为。SQLCipher、自定义 VFS、原生 schema 扩展和任意 SQLite 版本没有资格保证；加载器缺符号可能在 Rust 返回错误之前终止启动。

本地目录定名为 `local-weave` 后，首次默认并行核心回归中的关闭重开用例收到 `DatabaseInUse`；单例与整组串行诊断通过，但不能抵消并行失败。源码与锁文件未因此修改，根因尚未确认，也未证明仅为测试问题。当前不能声称默认并行回归全绿，详见 [验证范围](TEST_REPORT.md)。

## schema、事务和因果历史

只有 SDK 所有者连接参与捕获；其他 ORM/连接/进程写库不受支持。canonical STRICT 普通表要求首列非空 TEXT 主键；不支持复合键、额外 UNIQUE/FK、触发器、视图、虚拟表、生成列、自定义排序规则或任意 DDL。非空旧库须显式有界 `Baseline::Import`，没有自动业务 schema 迁移。

全行冲突保留所有并发 heads；普通写不能静默覆盖冲突，调用者须显式 resolve。历史、墓碑、祖先和失败记录持续增长，没有自动远端 GC 或大历史性能保证。原生 Session changeset 分配发生在净操作数检查之前；Rust callback 必须自行有界，不能被 SDK 当作内存/实时沙箱。

## 附件、快照和取消

显式附件 hash 在 write/resolve 前必须已经本机 Available，并重新通过完整 SHA256 校验；Missing 返回 `Invalid`。失败不能提交业务行、序号、DAG 或 outbox；损坏文件可以被重新标记 Missing。已有记录保留祖先引用的普通离线编辑可省略显式 refs，本体 Missing 不阻止编辑；不能从任意字符串、新无关记录或缺失祖先历史推断附件关联。

共享快照拒绝未发布或阻塞状态，包含全部已发布历史引用，排除无关私有缓存；本地灾备保留全部 Available 缓存与 pending exact bytes。两者均要求全部已应用历史引用文件可用。强制终止可能留下没有最终 manifest 的不完整目录；不能分享或恢复这些目录。未验证硬件断电、安全擦除介质或精确 mid-VACUUM 取消。

取消是协作式；单 SQLite statement 受约五秒 progress budget 约束，已经提交的事务不能因之后的取消而撤销。OS/HTTP 已发起工作不保证立即停止；S3 可能留下未完成 multipart staging，SDK 不自动远端清理。路径预检查不能抵抗恶意宿主并发替换路径，附件安装要求同根文件系统支持 hard link。

## 后端、Node 与信任

WebDAV 附件始终 `Unsupported`。S3 仅有本地 rclone 服务记录/multipart 资格；AWS/STS、凭据刷新和广泛云兼容性未验证。真实 WebDAV 只覆盖 [能力矩阵](CAPABILITIES.md) 中指定的 native Operator 场景，strict 工厂仍拒绝非 loopback HTTP。Drive 原型延期，三项模型反例见 [DRIVE](DRIVE.md)。

Node 事务是结构化 SQL 数组，不是异步 JS callback 或 ORM bridge；整数必须用 BigInt helper 或 canonical tagged 十进制字符串。有限 f64 以 SQLite 读出值为准，不承诺保留 affinity 规范化前的表示。绑定拒绝 SQL 文本中所有分号，限制每进程 16 handles、每 handle 16 个未完成普通请求；close 排空已接收工作。AsyncTask 等待 native 回复会占用 libuv pool 槽，可能延迟其他 libuv 工作。没有广泛吞吐基准或 Node 独立进程崩溃专项；详情见 [Node 指南](../bindings/nodejs/README.md)。

SHA256 只校验完整性，不认证发送者。需由宿主管理可信私有远端、传输策略与凭据；未实现签名、E2EE、多用户权限和自动 OAuth。当前没有发布安装包，源码与单机测试不能证明所有平台生产可用。
