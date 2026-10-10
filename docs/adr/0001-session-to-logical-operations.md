# ADR 0001：Session 转逻辑全行因果操作

状态：已采用。默认协议使用 Session 转逻辑记录操作，需求见 [REQUIREMENTS](../REQUIREMENTS.md)。

SQLite Session 能捕获同一连接上的净变化，但 UPDATE 只带主键和发生变化的列。固定版 Session 源码说明 UPDATE 省略未变化列；公开 API 测试实际验证只改 title 后其他列仍保留，并在同一事务里重新查询完整末态。Session 不记录完整审计历史，也不因提取 changeset 自动清空。[官方 Session 说明](https://sqlite.org/sessionintro.html)、[固定版 rusqlite API](https://docs.rs/rusqlite/0.40.2/rusqlite/session/index.html)。

| 选择                                         | 适用点                                                                       | 对本需求的不足/成本                                                                                              |
| -------------------------------------------- | ---------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| 直接复制 changeset + apply conflict callback | 兼容起点与 schema 间的差异传送；体积通常较小                                 | 不携带全行版本/逐记录因果；回调本身不能保证乱序、重复、长期离线下的收敛；不能把 SQLITE_OK 当作每行都已应用的证据 |
| Session → 完整行/墓碑 → 持久因果 MV register | 显式保存并发 heads、祖先关系，按集合而非到达顺序合并；可检测冲突并由用户解决 | 更多存储、查询和协议验证；需限制 schema，维护 ancestry、inbox/outbox 和恢复语义；不是无成本的通用 SQLite 复制    |

当前实现采用第二项。事务内由 Session 得到变化键，对活跃记录读完整末态、缺失键写墓碑；PK 更新规范为旧键删除+新键插入。业务值、持久序号、版本关系、规范包 bytes 和 outbox 一次提交，提交后联网。接收方验证后原子合并/投影/进度，不捕获 remote apply，不产生 echo。

每版本含 dataset、table、类型化 PK、device/incarnation、事务和操作序号、明确父版本及完整行/墓碑。保留祖先关系；缺依赖等待，旧版本晚到不能复活。并发 heads 全保留，以固定版本 ID 排序产生确定投影，墙钟不参与。普通写多 heads 拒绝；显式 resolve 仅覆盖用户观察的 heads，未见并发继续保留。全行寄存器不自动字段合并。

线上格式需区分 NULL、i64、f64、TEXT 和 BLOB，不能经过 JSON/JS Number 丢失 >2^53 整数。采用固定类型标记与长度的二进制规范；有限 f64 保留 SQLite 末态 bits，非有限值拒绝。基线、schema 迁移、附件、共享快照、恢复新 incarnation 均属于完整协议，不因本 ADR 而声称已完成。

精确格式与兼容性以 [PROTOCOL](../PROTOCOL.md) 为准；验证方法见 [TESTING](../TESTING.md)。
