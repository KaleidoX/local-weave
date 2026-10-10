# Google Drive 原型：延期

**DEFERRED / PROTOTYPE_NOT_ACCEPTED**。保留的记录适配器不属于支持能力，真实 Google Drive 未验证，附件返回 `Unsupported`，Node 不导出 Drive 构造器。

三项公开 SDK 模型反例仍未修复：

1. 同名慢对象 ID 会阻塞已经验证成功的上传。
2. 已枚举祖先移出配置根后，后续创建可能越出原根。
3. 小 page budget 下反复替换 transport 会使后面的 key 无法取得进展。

越界副作用仅发生在合成 loopback 夹具，没有访问 Google 或用户数据。现有六个模型测试通过不覆盖这些反例，不能据此声明可用。

原型 `GoogleDriveTransport` 位于 `integrations/opendal`，由 `drive-experimental` feature 控制。它复用锁定 OpenDAL 的公共 HTTP transport，以有限的 ID-aware REST 补充处理 parent/file ID 与同名对象；未实例化 path-based Gdrive Operator，设计背景见 [ADR 0002](adr/0002-drive-record-ids.md)。保留原 `lfs1` 对象格式，未改变协议、schema、操作身份或 exact outbox。

宿主传入已有专用 folder ID 与短期 token 快照；没有 OAuth、默认凭据发现或自动 token 刷新。CLI 中保留的 `GoogleDrive` 配置也是原型入口，不是支持声明。仅合成测试使用 `owned_loopback`；模型脚本 [drive_model.py](../integrations/opendal/tests/fixtures/drive_model.py) 是测试支持工具，不是协调服务器。

后续需重新评估根目录约束、对象定位、重试与扫描进度，并分别验证真实服务语义；当前没有已批准的新布局或修复承诺。其他后端范围见 [能力矩阵](CAPABILITIES.md)。
