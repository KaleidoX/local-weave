# 后端测试

MemoryTransport 与故障注入属于 MODEL/FAULT；Fs 是真实本地 I/O；rclone 是独立本地 HTTP 服务。特定真实 WebDAV 结果单独记录在 [能力矩阵](../CAPABILITIES.md)，不能推广至所有服务。Drive 原型延期，模型通过不抵消未解决反例。

## 可选本地 HTTP 场景

宿主自行准备 rclone 1.75.1 和 Rust/SQLite 编译环境。从仓库根目录执行，`RCLONE_BIN` 是宿主指定的工具绝对路径；每次 evidence 使用新目录。

```sh
export TMPDIR="$(mktemp -d)"
python3 integrations/opendal/tests/http_services.py webdav --tool "$RCLONE_BIN" --evidence "$TMPDIR/webdav-evidence"
python3 integrations/opendal/tests/http_services.py s3 --tool "$RCLONE_BIN" --evidence "$TMPDIR/s3-evidence"
```

runner 只启动自己的 loopback 服务，显式 `--config /dev/null`，生成测试身份，不读取真实云配置；WebDAV 用 PROPFIND 207、S3 用签名 GET 200 检查就绪，再通过真实 SDK 执行三端 typed rows、冲突/resolve/no echo 和错误认证场景。它保存命令、测试退出码与清理结果，并回收自己启动的服务。运行后保留 evidence 供检查，由调用者决定保留期。

常规 workspace 测试中的 `real_local_http_sdk_journey` 明确 ignored；runner 以所需 `fs,webdav,s3` features 和 `--ignored` 运行它。缺少服务工具不能算通过。Node HTTP 入口见 [Node 指南](../../bindings/nodejs/README.md)，必须配套新构建且已验证的 native 制品。

## 证据解释

Fs lost-ack、缺页、延迟和重复通过真实文件上的故障包装注入；HTTP 错误分类与截断使用合成故障服务。它们验证失败路径，不等于真实云端。子进程重启不等于硬件掉电；子进程的嵌套摘要不重复计数。S3 multipart 资格仅限本地 rclone；WebDAV 附件 Unsupported；AWS/STS、移动与其他 OS 未验证。
