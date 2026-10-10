# ADR 0002：Drive 记录必须保留对象 ID

状态：延期原型的设计记录，不构成支持承诺。当前反例与状态见 [Drive](../DRIVE.md)。

Drive 的 parent/file ID 与同名对象语义不能安全简化成唯一路径。只选择一个同名 ID、按路径去重或覆盖既有 ID，不能满足不可变同步包的发现、重试及分叉验证需求。

保留原型使用 OpenDAL 0.59.4 的公共 HttpTransport/ReqwestTransport，加上有限 ID-aware REST。枚举须保留同名候选，核对 parent、MIME、trash、完整内容与哈希；对象 ID 仅为 adapter 定位信息，不取代 PacketKey/OpId。未知 create 结果仍保留固定 outbox，重试重新枚举和比较内容；不执行 PATCH/DELETE。

凭据快照和专用 folder ID 由宿主提供。模型使用显式 loopback，不能验证 Google 兼容性。根目录移动、慢重复候选与小预算重建 transport 的反例说明原设计还不足以获得资格；附件及真实服务接入延期。核心 wire/schema/因果契约保持不变。
