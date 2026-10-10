# LocalWeave 需求与范围

以下是产品目标与验收范围；未实现方向以 [能力矩阵](CAPABILITIES.md) 和 [路线](ROADMAP.md) 为准。目标不等于支持承诺。

## 1. 项目目标

Rust 共享核心，SQLite 为每台设备本地业务数据层，SQLite Session Extension 捕获受控事务变化，Apache OpenDAL 连接 WebDAV、S3 兼容存储和 Google Drive。结构化记录、删除状态、附件；离线可查询修改，恢复网络后可靠同步；远端仅存储及交换，不依赖自建协调服务器。个人多设备；首版非多人权限、实时文本协作、任意数据库零改造。三个后端分阶段、逐项真实验证状态。先桌面 Rust SDK、CLI、双设备示例，再 Node；Android/iOS 独立里程碑；浏览器不保证。

## 2. 调查与 ADR

核验 Rust SQLite 绑定、Session API、OpenDAL 服务能力、MSRV、许可证，受维护稳定依赖，锁定版本/特性，第三方许可说明。SQLite 实测 SQLITE_ENABLE_SESSION 和 SQLITE_ENABLE_PREUPDATE_HOOK。设计决策须解释：直接 changeset vs Session 转逻辑记录操作，默认后者；UPDATE changeset 不含完整行，时间戳覆盖不能保证多写端收敛，冲突回调不等于同步协议。

## 3. 数据库边界

SDK 管连接和事务生命周期，每库串行写入口；受控写、只读查询、同步、状态、冲突解决、附件；所有同步写入经 SDK。Session 只捕获所属连接，.db 路径不能捕获其他连接/ORM/进程；ORM 显式适配；不暴露裸可写连接。authorizer 等机制阻止内部表修改、调用者提交、ATTACH、任意 DDL、危险 PRAGMA、未注册表写入。外部进程写不支持且尽可能检测。普通业务表，显式非空 PK；示例全局 ID，非设备自增。拒虚拟表及未验证触发器、生成列、复杂排序规则、额外 UNIQUE、跨表约束；派生索引/视图本地重建；非任意 schema。

## 4. 本地事务

每事务新 Session，仅 attach 注册表。一个 SQLite 事务：业务修改；changeset 主键；末态完整行/墓碑；逻辑操作和版本；持久设备序号；确定包字节/身份/哈希入同库 outbox；提交。任一步失败全回滚，提交后上传，网络不占事务。changeset 不清 Session，成功/失败/回滚都销毁；无净变化无假操作；PK 变化=旧键删除+新键插入；Session 非审计日志。业务/序号/版本/outbox 原子；上传成功未确认崩溃后重发相同包。已有 DB 明确基线导入，不捕获历史。

## 5. 收敛

记录级全行多值寄存器。稳定表 ID+类型安全 PK；每版本完整值或墓碑。操作含 dataset_id/device_id/incarnation/事务 seq/包内 op seq/schema 版本/因果上下文。显式父版本+持久祖先 DAG 可行；覆盖旧 heads，并发全部保留；晚到旧包不复活；设备最大 seq 不替代逐记录因果。缺依赖等待补拉。有效操作集合相同则顺序/重复不改变 heads、删除和投影。固定确定性全版本投影，无墙钟；显示非删除其他分支；查询识别冲突。普通写多 heads 返回 ConflictRequiresResolution。resolve 带观察 heads+选定或合并完整值，只覆盖所见分支，未见并发保留，同时 resolve 收敛。删除并发修改保留冲突；非字段自动合并。

## 6. 接收/schema

远端受控不捕获写入。下载校验后外层事务内去重、合并、投影、inbox、已应用进度，失败回滚，无 echo。预验 dataset/protocol/schema/白名单/PK/列/数量/大小/依赖。若用 sqlite3changeset_apply 必预验兼容及覆盖范围，SQLITE_OK 不证明全包应用。约束失败全包回滚，阻塞/隔离；区分 downloaded/validated/waiting/applied/failed，失败不推进进度。宿主显式业务迁移；协议/业务/内部元数据独立版本；不兼容包保留阻塞，不跳过称同步；远端不执行 SQL/DDL。旧包转换/历史重建/new epoch 另行设计测试。

## 7. 远端协议

dataset 下 device incarnation 分区，追加不可变包。文件名 seq+hash，含前包 ID/因果依赖/附件引用。确定序列化、NULL/i64/f64/text/BLOB，无 JSON 数字损失；特殊值、大小、类型限制。身份/seq 持久；同身份 seq 不同内容=分叉停止。无全局 manifest/latest 覆盖；可索引但非唯一事实。分页枚举、重复去重、缺批补拉、乱序缓冲、依赖；不按最大 seq 跳过缺口。统一 OpenDAL 传输，运行时能力验证；不假定三后端支持 rename/条件创建/唯一路径。Drive 验 parent ID/file ID/重上传/枚举，同目录同名按身份内容去重，非随便取文件。OpenDAL 不足可小补适配记录原因或明确未完成。

## 8. 附件

内容寻址，SQLite 元数据/引用，本体文件。临时文件->校验->可靠落盘->DB 引用，崩溃恢复状态机和孤立文件清理。远端附件上传完整性确认后发布引用包；接收记录可先到，本体下载验哈希落盘后 available。流式、取消、失败重试、按需下载、大小、路径穿越防护；非包内大附件。

## 9. 安全/备份/恢复

凭证宿主/安全存储，OAuth 交互宿主，SDK 接口；无真实凭证/绕过授权。SHA256 非来源认证，可信私有远端+TLS；签名/E2EE/密钥另设计，未实现不可宣称。关闭自动远端 GC；不简单时间删除墓碑/日志/附件破坏长期离线设备。一致性备份含业务/版本/墓碑/一致因果切面，不复制活跃 db。共享新设备快照不含 unpublished outbox；本地灾难备份保留 outbox/恢复元数据，区分用途。恢复/克隆新操作 new incarnation，旧操作身份保留；误恢复/重复实例/损坏备份明确处理。

## 10. 工程/API

Rust workspace 分协议/合并、Session 事务、inbox outbox 引擎、OpenDAL、附件恢复、facade、CLI、Node、示例测试。API 至少 open/read/write_transaction/sync_once/status/list_conflicts/resolve/add attachment/download attachment。互斥、线程、异步 runtime 所有权、取消边界、状态事件、错误、重试、关闭。CLI 初始化/状态/单次 sync/冲突查看解决/脱敏 diagnostics，机器输出稳定退出码，非默认常驻。

## 11. 交付顺序

阶段一：能力调查、协议 ADR、不变量、SQLite 编译与 Session 捕获证明、同事务 outbox 最小闭环。
阶段二：逻辑操作、版本 DAG、持久 outbox/inbox、内存传输、双设备冲突收敛。
阶段三：本地目录和故障注入、恢复，再逐个 WebDAV/S3/Google Drive；逐后端独立状态。
阶段四：附件、CLI、示例、备份恢复、Node.js；桌面完成再 Node。
阶段五：目标移动平台绑定、凭据存储、后台生命周期；Android/iOS 独立验证，未完成规划中。
不得等全部写完才测，每阶段运行测试后继续下一阶段。

## 12. 验收测试

至少覆盖：本地提交/回滚/连续 Session 隔离；业务与 outbox 原子；上传成功未确认崩溃后同包重发；包重复/乱序/缺失/延迟/依赖补齐；双设备和三设备离线并发；同行多字段/删除编辑并发；resolve 期间新并发；旧版本晚到/克隆/恢复序号；schema 不兼容/约束失败/损坏/超大包；附件上传下载中断/孤立文件/状态恢复；Drive 同名/重复上传/分页；网络超时/限流/认证过期/取消。
性质或模型测试：同有效操作集合在不同排列及重复下相同 heads/墓碑/投影/已应用结果。不是只断言 upload success。
可运行双设备示例：离线新增/修改/删除/附件/同步/冲突查看/显式解决。

## 13. 源码交付

提供锁定依赖、README、架构/协议/ADR、能力矩阵、示例、测试方法与已知限制。分别记录模型、真实 SQLite/文件、本地服务和真实后端结果；不以模拟或编译通过代替运行证据。浏览器不在当前范围；Android 与 iOS 分别验证。
