# 核心不变量 / Core invariants

这些不变量约束当前实现；产品范围见 [REQUIREMENTS](REQUIREMENTS.md)，精确编码见 [PROTOCOL](PROTOCOL.md)。

- **I01 连接所有权**：每库只有 SDK 所有的串行写入口，不暴露裸可写 Connection。Session 只捕获该连接。外部写入不受支持；通过 data_version/schema 指纹尽可能检测并拒绝。已有数据须显式基线导入。
- **I02 事务隔离**：每事务新建 Session，只 attach 登记的普通业务表，成功、失败或回滚均销毁。authorizer 拒绝内部表修改、事务控制、ATTACH、DDL、危险 PRAGMA、未登记表写入和触发器副作用。
- **I03 本地原子性**：业务状态、因果版本、持久 next_seq、exact outbox 字节、身份与哈希同库一次提交。任一步失败均无部分提交。无净变化不消耗序号、不产生包；主键移动为旧键墓碑与新键完整行。
- **I04 网络边界**：数据库事务和串行执行锁内不联网。只上传已提交的固定字节；上传成功而本地 ack 丢失后，重开仍重发相同身份与字节。数据库文件所有权锁的生命周期不因此缩短。
- **I05 全行 MV register**：操作是完整末态行或墓碑，parents 属于同一 typed record key。持久祖先防止旧版本复活；heads 与确定投影取决于有效操作集合，不取决于到达顺序或墙钟。同 OpId 不同内容是错误。
- **I06 显式冲突解决**：普通写多头记录返回 ConflictRequiresResolution。resolve 只覆盖调用者提供的 observed 版本，未见并发继续保留；删除/编辑与同时 resolve 仍可并发，不自动逐字段合并。
- **I07 依赖**：缺前包或记录父版本时持久等待并重访；设备最大序号不能代替记录因果，不能跳过缺口。流身份/序号分叉停止该流，不信任文件名替代内容校验。
- **I08 远端原子应用**：先校验大小、hash、protocol/dataset/schema、列/键/表/操作/父版本/附件，再原子更新 inbox、版本、投影和 applied progress。失败不推进进度、不生成 echo outbox；下载、校验、等待、应用、失败状态分别持久保存。不兼容或约束失败必须可见。
- **I09 编码**：确定性 canonical 编码、稳定 table ID 与非空 typed key，保留 NULL/i64/finite f64/text/BLOB，不经 JSON number 丢精度。拒绝非有限数、非法 UTF-8 和不支持的键；尽可能在分配和反序列化前检查公开上限。
- **I10 存储**：按 device/incarnation 与 seq/hash 保存不可变对象，包含前包与依赖；共享 latest/manifest 不能成为唯一事实。分页枚举、去重并处理全部缺口，检查后端实际能力。Drive 必须保留 parent/file ID 与所有同名对象语义，否则不能声称支持。
- **I11 附件**：临时流、hash/大小校验、fsync、安装及持久状态完整后才可标 Available；崩溃重开进行 reconciliation。引用包发布前先上传并完整回读附件；接收可先显示 Missing，只有校验下载并可靠安装后可用。不自动远端 GC。
- **I12 备份恢复**：使用 SQLite 一致 Backup API，不复制活跃文件。本地灾备保留 exact outbox、因果状态及所有 Available 缓存。共享快照拒绝未发布状态，包含全部已发布历史引用（祖先、旧版本、分支、墓碑），排除无关缓存与元数据并清理副本残页；只改变副本，失败/取消不改源。恢复/克隆为新操作轮换 incarnation，保留旧身份与字节；损坏备份在安装前拒绝，不能覆盖原库。
- **I13 信任边界**：宿主管理可信私有远端、TLS 与凭据，SHA256 只校验完整性。远端不执行 SQL/DDL。OAuth 交互、签名、E2EE、多用户权限及移动生命周期不因接口存在而被视为已实现。

验证须覆盖 Session 运行时捕获与 rollback、净零事务、连续 Session、原子 outbox、authorizer、重开、排列/重复/缺依赖、并发与 stale resolve、schema/hash/大小/分叉拒绝、附件中断和恢复、快照隔离与新 incarnation。MODEL/FAULT、本地 I/O、本地 HTTP 和真实服务分别报告；移动平台需要独立目标构建与运行，详见 [TESTING](TESTING.md)。
