# LocalWeave Node.js 绑定

真实 Node-API 原生插件，共享 Rust `lfs-sqlite::Sdk` 拥有 SQLite 连接、Session、因果状态和 exact outbox。验证范围为 Linux x86_64、Node **22.23.3**（宿主 Node-API 10）、插件目标 **Node-API 8**、Rust **1.96.0**。macOS、Windows、其他 Node 版本和 Bun 未验证；不表示支持浏览器、React Native、Android 或 iOS。包按源码交付（package 标记 private，避免误发布），未发布 npm 或预编译安装包。

## 构建与运行

从仓库根目录执行；需要 Rust 1.96、C/C++ 构建工具和 libclang。`TMPDIR` 指向宿主提供的可写 scratch 目录。宿主应准备 SQLite Session/preupdate 引擎与匹配头文件，按需配置 `LIBCLANG_PATH`，见 [宿主依赖](../../docs/HOST_DEPENDENCIES.md)。

```sh
# 在已设置 TMPDIR 的 shell 中
npm ci --prefix bindings/nodejs --ignore-scripts --no-audit --no-fund --cache "$TMPDIR/npm-cache"
node bindings/nodejs/build.mjs
node --test bindings/nodejs/test/native.test.mjs
npm --prefix bindings/nodejs run typecheck
node bindings/nodejs/example.mjs
```

`build.mjs` 仅在构建时执行 `cargo build --locked -p lfs-node`，将 `target/debug/liblfs_node.so` 复制为 `bindings/nodejs/lfs_node.node`（尊重 `CARGO_TARGET_DIR`）。运行时 `index.js` 直接加载该插件，运行 API 不启动 CLI 或任何子进程，不提供 JavaScript SQLite fallback。必须从该 workspace 构建；直接复制 JS 包不能代替共享 Rust 源码和锁文件。

JS/TS 接口与测试位于 `bindings/nodejs`，Rust Node-API 实现在 `bindings/nodejs/native`。二者与核心共用根 Cargo workspace/lock；包名为 `@local-weave/sqlite`（`private: true`），native crate 仍为 `lfs-node`。项目采用 [Apache-2.0](../../LICENSE)。

旧本地 `lfs_node.node` 没有重建，不能继承最新 Rust 宿主引擎或 URI 拒绝修复资格。`build.mjs` 会替换该文件；需要保留旧制品时先另外备份。只检查当前 native 编译可从根执行 `cargo check --locked -p lfs-node`，但编译成功不证明 Node 运行通过。显式 bundled 构建可先执行 `cargo build --locked -p lfs-node --features bundled`，再将所选 target 的 `debug/liblfs_node.so` 安装到绑定目录；普通 `build.mjs` 使用默认宿主 SQLite。

## 最小用法

```js
import {open, value as v, decodeValue} from './bindings/nodejs/index.js';
const db = await open({
  path: '/host/private/device.db',
  identity: ['dataset', 'device-a', 'incarnation-first'],
  schema: {version: 1, tables: [{id: 1, name: 'notes', columns: [
    {name: 'id', kind: 'Text', nullable: false},
    {name: 'number', kind: 'Integer', nullable: true}
  ]}]},
  backend: {kind: 'Fs', root: '/host/private/remote'}
});
try {
  await db.writeTransaction([{sql: 'INSERT INTO notes VALUES (?, ?)',
    params: [v.text('global-id'), v.integer(9223372036854775807n)]}]);
  const rows = await db.read('SELECT * FROM notes');
  console.log(decodeValue(rows[0][1])); // 9223372036854775807n
  const result = await db.syncOnce();
  // Check result.healthy and result.status; incomplete scans do not imply convergence.
} finally { await db.close(); }
```

数据库父目录须存在，Fs root 须为绝对路径且同步前存在；SDK 不替用户创建远端目录。数据库和 Fs 目录由宿主管理，禁止其他程序同时修改。路径拒绝 `..` 和现有 symlink；不保证抵抗宿主恶意并发替换路径。`baseline_import: true` 显式导入已存在 canonical schema 的有界数据，不捕获历史。错误 schema/未知字段拒绝，不忽略未支持的 UNIQUE、FK、trigger 等要求。只支持首列非空 TEXT 主键、STRICT 普通表，完整限制见 [核心契约](../../docs/REQUIREMENTS.md)。

## 值与事务边界

每个 SQL 参数和返回值均为 tagged object；[index.d.ts](index.d.ts) 为公开声明。[test/types.ts](test/types.ts) 检查 TypeScript 契约；[原生测试](test/native.test.mjs) 实际往返 i64::MAX、finite f64、BLOB、NULL。

| SQLite 值  | wire 表达                                      | helper / JS 结果                                      |
| ---------- | ---------------------------------------------- | ----------------------------------------------------- |
| NULL       | `{type:'null'}`                                | `v.null()` / `null`                                   |
| signed i64 | `{type:'integer',value:'9223372036854775807'}` | `v.integer(bigint或canonical十进制string)` / `bigint` |
| finite f64 | `{type:'real',value:'3ff2000000000000'}`       | `v.real(1.125)` / `number`                            |
| TEXT       | `{type:'text',value:'hello'}`                  | `v.text(string)` / `string`                           |
| BLOB       | `{type:'blob',value:'00ff'}`                   | `v.blob(Buffer)` / `Buffer`                           |

禁止用 JS Number 表示整数参数（包括安全整数），也不接受裸 JSON Number。整数拒溢出、非 canonical 字符串；real 位串必须解出有限值，NaN/Infinity 拒绝。编码保持 SQLite **读出的** binary64 位，SQLite affinity 可规范化 -0 或整型/实型表示，不承诺恢复存储前已丢失的信息。TEXT 与 BLOB 单值最大 512 KiB；请求 JSON 最大 8 MiB。JSON/Buffer 编解码在 JS 线程有有界成本，SQLite/文件/网络工作全部在 native worker。

`writeTransaction(statements, attachmentHashes?, {signal}?)` 是 1–1000 条受控 SQL 的同步 native 原子批次。Session 只在共享核心拥有的连接上捕获；业务行、序号、因果信息和 outbox 一起提交。任何 SQL、参数、约束、authorizer 或提交失败回滚整个批次。**不是异步 JS 事务 callback，也不是 ORM 连接适配器**。SQL 最多 65536 bytes；此绑定保守拒绝 SQL 文本中的所有分号（包括注释/字面量中的分号，请使用参数），从而没有隐式多语句尾部。`COMMIT`、DDL、ATTACH、危险 PRAGMA 和内部表访问由核心 authorizer 拒绝。

## 附件引用与离线编辑

`writeTransaction(statements, attachmentHashes)` 和 `resolve({..., attachments})` 中**每个显式 hash** 都必须已在本机可靠安装、状态为 `Available`，并在本次调用中通过完整 SHA256 校验。即使该 hash 已存在于记录历史中，只要再次显式传入，也执行同样检查。已知 `Missing` 的显式引用当前返回 `SdkError.code === 'Invalid'`（不是 `Missing`），在业务 SQL / resolve 执行前拒绝，业务行、序号、因果状态和 outbox 不提交，`status()` 保持不变。失败会产生操作失败事件；若本地文件原标 Available 但校验发现损坏，附件状态可降为 Missing，文件 I/O 等错误按对应 code 返回。

宿主新增显式引用时，先 `await addAttachment(path)` 或 `await downloadAttachment(hash)`，成功安装后再 write / resolve。事务不会自行联网下载；不要在受控事务中执行网络操作。引用为 packet 级集合，同批记录共享该集合，并不从任意 SQL 字符串推断附件。

**已有记录保持原附件引用的普通离线编辑**可以省略 `attachmentHashes`（或传 `[]`），仅更新其他字段，即使本机附件本体仍为 Missing。全行中的原 hash 保留，祖先包的引用历史继续保存；不要求每次编辑重复所有 hash。已有场景在 Linux / Node 22 / Fs 上实际验证：端点不可用时编辑并重开保留 pending，重连后新第三端取得编辑版本及因果父包、历史附件引用并下载完全相同的字节。这不承诺新建无关记录、仅复制 hash 字符串或缺失祖先历史时自动建立附件关联。

```js
// Existing row already has a reference introduced by an ancestor packet.
await db.writeTransaction([{sql: 'UPDATE notes SET body=? WHERE id=?',
  params: [v.text('offline edit'), v.text('existing-id')]}]);
// To supply an explicit reference, install it before the transaction.
await db.downloadAttachment(hash); // Or await db.addAttachment(hostFile).
await db.writeTransaction(statements, [hash]);
```

## API、线程与生命周期

- `read(sql, params?)` 返回 `SqlValue[][]`；`status()` 返回有界字段计数的十进制字符串和健康状态；`info()` 返回 identity/schema，不返回路径或凭据。
- `syncOnce({signal}?)` 通过 native worker 选择真实 OpenDAL Fs、WebDAV 或 S3 同步。`listConflicts()` 返回各版本头及完整行/墓碑；`resolve({table,key,observed,row,attachments?})` 接受原样 head IDs 和选择的完整行或 `null`。普通冲突写返回 `ConflictRequiresResolution`，不会隐式覆盖。
- `addAttachment(path,{signal}?)` 读取普通文件并流式 hash/install；`downloadAttachment(hash,{signal}?)` 按需下载、验 hash、安装；`attachmentStatus(hash)` 查询持久状态。`exportAttachment(hash,newPath,{signal}?)` 校验后分块写入新文件，失败删除自己创建的输出。默认附件上限 256 MiB，不把本体装进 JSON。
- `backup(newDirectory,'LocalDisaster'|'SharedBootstrap',{signal}?)` 返回 manifest。SharedBootstrap 要求没有未发布/阻塞记录，保留所有已发布历史附件引用（即使对应记录已删除），排除无关私有缓存；LocalDisaster 保留私有缓存和 pending exact bytes。
- `restore({backup,destination,backend},{signal}?)` 返回新 handle，destination 必须为新目录，数据库位于 `destination/store.db`。核心校验快照并旋转 incarnation；重开时使用 `info()` 返回的 identity/schema。原有 pending 包保持旧身份，新写入使用新 incarnation。
- `events()` 返回最近 64 条 native 操作完成事件，字段 `operation/success/error`；轮询获取，非持久审计日志，不调用跨线程 JS callback。同步完整性另外查看返回报告的 `healthy`。不输出核心 debug 错误、SQL、参数、凭据或私有路径。

每实例一个 FIFO Rust 线程，线程独占 `Sdk` 和 current-thread Tokio runtime；Tokio 最多 2 个 blocking threads。每进程最多 16 个打开中/已打开 handle，每 handle 最多 16 个尚未完成的普通请求，超限 `Busy`；close 有额外保留槽。多调用方的写按 native 接收顺序串行，包含同步期间的写；同实例前一 sync Promise 未完成时第二个 sync 明确 `Busy`。

`close()` 同步关闭新请求准入，然后排空已接受请求、关闭核心连接并完成 Promise；后续操作及重复 close 为 `Closed`。需要取消正在进行的同步/附件/备份时先 abort 对应 signal，再 await close。丢弃 JS handle 后，已接受任务持有引用直到完成，channel 关闭后 worker 在线程内销毁 SDK；应用应显式 close，GC 时机无保证。未发布 outbox 不会因 close 被确认或清除。

`AsyncTask` 仅用于异步等待 native worker 回复；没有把普通 napi abort 当作运行中任务的停止机制。AbortSignal 通过 `CancelToken` 调用核心 `Cancellation`。预取消在副作用前拒绝；运行中取消由核心流复制/hash/backup 检查、sync future 边界及批次 SQL 之间协作处理。单次 SQLite statement 受核心约 5 秒 progress budget 约束，无法立即由 signal 打断；提交完成后的取消不会撤销事务。Fs/OS/HTTP 已发起工作不保证立即中断，close 也不是硬截止时限。

错误类型为 `SdkError`，稳定 `code` 包括 `Invalid/Schema/Forbidden/Constraint/Busy/Closed/Cancelled/ConflictRequiresResolution/Blocked/Missing/Corrupt/Limit/Io/Unsupported/Auth/Transient`，message 仅为 code。底层信息脱敏；未识别失败不反射调用参数。

## 支持边界和来源

Node 导出 Fs、WebDAV 与 S3 backend。Basic 记录链路通过独立 rclone 1.75.1 loopback 验证（LOCAL_SERVICE）；Bearer 通过合成认证代理的真实 HTTP 验证（MODEL_HTTP），不等于 rclone 原生 Bearer 或通用 WebDAV 兼容性。WebDAV 流式附件保持 Unsupported，S3 records 与流式附件已本地验证，Drive DEFERRED。真实云端均未验证；没有账号、OAuth、云端或移动端动作。Android 和 iOS 各自规划见 [ROADMAP](../../docs/ROADMAP.md)，具体限制见 [KNOWN_LIMITATIONS](../../docs/KNOWN_LIMITATIONS.md)。

napi **3.14.2**（关闭 defaults，`napi8,dyn-symbols`）、napi-derive **3.6.12**、napi-build **2.6.0** 均锁定；下载源码声明 MIT/MSRV1.88，完整 SDK 当前验证工具链为 Rust 1.96.0。核验 [AsyncTask](https://napi.rs/docs/concepts/async-task)、[async/concurrency](https://napi.rs/docs/more/async-concurrency)、[class](https://napi.rs/docs/concepts/class) 及锁定版本 Task/AsyncTask 源码。其普通 abort 只取消未运行的 libuv 工作；此绑定采用独立核心 token。

## WebDAV 配置契约与本地资格

`Backend = FsBackend | WebdavBackend | S3Backend`；`open` 与 `restore` 接受同一类型。
WebDAV 配置为 `{kind:'Webdav', endpoint, credentials, allow_loopback_http?}`；凭据为
`{kind:'Basic', username, password}` 或 `{kind:'Bearer', token}`。endpoint 与每个凭据字符串
均须非空且最多 **8192 UTF-8 bytes**。Basic username 不得含 `:`；password 可含 `:` 和 Unicode。
Bearer 遵循 RFC 6750 的 `1*(ALPHA / DIGIT / "-" / "." / "_" / "~" / "+" / "/") *"="`，
不接受空白、控制字符或中间出现的 `=`。未知字段、未知标签和非 boolean flag 返回 `Invalid`。
字符串/凭据格式错误为 `Invalid`，工厂拒绝的 endpoint 为 `Unsupported`，实际 HTTP 认证失败为 `Auth`；
这些错误只包含稳定 code，不包含配置。非网络配置在创建数据库或执行 restore 前验证。

默认仅 HTTPS。URL userinfo、query、fragment 拒绝。只有显式 `allow_loopback_http: true` 才允许
`http://127.0.0.1` / `http://[::1]`，供宿主自建本地测试服务使用；不使用隐式代理或跟随重定向。
每个 handle 使用宿主在 open/restore 前从 secure store 取得的不可变凭据快照；更新凭据须 close，
再以相同 path、identity、schema 和新快照 reopen。SDK 不读取凭据环境变量/文件、不 OAuth、无刷新 callback。
JS wrapper 会把输入序列化为暂态内存 JSON 交给 native；凭据只在 JS/native 内存使用，不写入 SDK 配置、
SQLite、backup、日志或 diagnostics，不承诺内存 zeroization。restore 的 backup 仍是本地文件，凭据由当前调用提供。

网络工作在 native worker 中复用共享 OpenDAL 工厂与默认 15 秒单操作边界及取消；这不是完整 sync 的总期限。
WebDAV attachment streaming 不支持，引用附件的 pending packet 在发布记录前以 `Unsupported` 拒绝并保持 pending。
下载入口在创建附件 staging 前检查工厂能力并拒绝 Unsupported，拒绝后仍可立即 LocalDisaster backup。
实际 sync 失败会令当前 handle 的 `healthy` 为 false；持久计数、身份和 outbox 字节保持，close/reopen 重置内存错误状态。

宿主接入示例（`snapshot` 由宿主 secure store 提供，不由 SDK 获取）：

```js
const db = await open({path, identity, schema, backend: {
  kind: 'Webdav', endpoint: 'https://dav.example.invalid/private/',
  credentials: {kind: 'Basic', username: snapshot.username, password: snapshot.password}
}});
// To replace expired credentials, close this handle, then open the same path/identity/schema
// with a fresh host snapshot. Unacknowledged operations retain their original wire identity/bytes.
```

Node HTTP 验证可从仓库根目录复跑；宿主通过 `RCLONE_BIN` 提供 rclone 1.75.1 的绝对路径。
每次使用新的 evidence 目录，runner 保留合成 fixture 和失败日志，只回收自己启动的进程；无需云端凭据。

```sh
python3 bindings/nodejs/test/webdav_services.py --tool "$RCLONE_BIN" --bearer --pattern 'H[1-6] ' \
  --evidence "$TMPDIR/nh1-webdav-new-evidence"
```

六项 WebDAV 测试覆盖三端 typed 全行/墓碑/显式解决/无回声、Basic/Bearer HTTP、严格配置、认证重开原字节重试、
restore 身份轮换、附件拒绝和实际 HTTP 中途取消。独立 Fs replay 只作为本地 backup 的 opaque exact-byte oracle；
Node WebDAV 的业务上传/下载始终经真实 addon 与 HTTP，未用 CLI 代替。

## S3 配置与 streaming 本地资格

```ts
const backend: S3Backend = {
  kind: 'S3', endpoint: 'https://s3.example.invalid', region: 'host-region', bucket: 'host-bucket',
  credentials: {
    access_key_id: snapshot.access_key_id,
    secret_access_key: snapshot.secret_access_key,
    // Omit session_token for static credentials; supply a nonempty string for a temporary snapshot.
    ...(snapshot.session_token === undefined ? {} : {session_token: snapshot.session_token})
  }
};
```

S3 credentials 没有 `kind` 字段。endpoint、region、bucket、key、secret 和出现的 token 均须为
1–8192 UTF-8 bytes 字符串；token 缺省可接受，显式 `null`、空串或非字符串拒绝为 `Invalid`。
未知字段拒绝，校验在 SQLite open / restore destination 副作用前完成。HTTPS / 显式严格 loopback
策略与 WebDAV 共用；拒绝 URL userinfo、query、fragment。open 和 restore 均接收此 backend，
restore 的当前凭据不从 backup 取得。

每个 native handle 复制宿主凭据快照，修改 JS 对象不会刷新它；close 后以相同 path、identity、schema
和新 snapshot reopen 可重试原 pending wire。复用共享 Rust S3 工厂的显式 key/secret/token，保持
`disable_config_load` 与 `disable_ec2_metadata`；没有隐式 host credential discovery、provider 或 callback。
凭据类型不提供 Debug / Serialize，不写入 SQLite、backup、diagnostics、events 或日志。
JSON bridge 为暂态内存交接，不承诺 zeroization。合成 token 的实际 HTTP header 转发已观测，
不代表 AWS STS 授权、有效期或刷新已验证。

`addAttachment(path)` 增量安装本地文件；显式 hash 写入后，sync 在既有 native worker / Rust 路径中
按 5 MiB writer chunks、concurrent=1 上传，并完整回读 SHA256 后才发布引用记录。
本地 8,388,625-byte fixture 实际产生 initiate、两片 PUT、complete 与全体 range GET；peer 可先收到行而
本体为 Missing，`downloadAttachment(hash)` 成功后才 Available，`exportAttachment(hash,path)` 输出完整字节。
Missing 显式引用仍拒绝 Invalid；既有记录保持历史引用的普通离线编辑不变。

LOCAL_SERVICE_TESTED 仅覆盖 owned rclone 1.75.1 experimental basic S3；真实 HTTP 截断代理标为 MODEL_HTTP。
截断验证失败不发布引用记录、不确认 pending，重开恢复保持原 wire；SharedBootstrap 包含已发布附件闭包，
LocalDisaster 保留 pending，restore 为新操作轮换 incarnation。现有 FIFO/16 handles/16 pending/close drain/
AbortSignal 映射与默认单操作超时未改。该 fixture 不证明任意大文件内存上界或整个 S3 生态兼容。
真实云、AWS/STS、移动端和其他平台均未验证；旧制品资格与当前源码编译结果分别记录。

```sh
python3 bindings/nodejs/test/s3_services.py --tool "$RCLONE_BIN" --evidence "$TMPDIR/nh2-s3-new-evidence"
# A single scenario, with the prefix anchored to avoid matching S3 in other test titles:
python3 bindings/nodejs/test/s3_services.py --tool "$RCLONE_BIN" --pattern '^S4 ' --evidence "$TMPDIR/nh2-s3-multipart-evidence"
# The package's existing typecheck stays unchanged; check the additive S3 type contract too:
bindings/nodejs/node_modules/.bin/tsc --noEmit --types node --typeRoots bindings/nodejs/node_modules/@types \
  --strict --target ES2022 --module NodeNext --moduleResolution NodeNext bindings/nodejs/test/s3-types.ts
```

runner 在 serve 前准备测试 bucket，使用签名 root / exact bucket HTTP200 就绪检查，并保留 fixture、HTTP 摘要和实际退出码。
