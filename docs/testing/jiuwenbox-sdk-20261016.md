# Jiuwenbox / Conch 完整 ADX 单机部署与 SDK 验收 — 2026-10-16

完整单机部署与公开 SDK 验收 **11/11 通过**。supervisor/conch 均跑通创建、
exec、二进制及 UTF-8 文件读写、独立 Execd EROFS、删除及进程故障恢复。
最终 5 个执行记录均已 Deleted，实例、进程、挂载和网络资源审计无残留。
Jiuwenbox 和 Conch 源码保持不变。

## 版本与部署

ADX master 基线为 `fb77fc4195580c958235c99254f782aaf18a0c63`，分支为
`feat/jiuwenbox-runtime-p0`。部署二进制来自
`efb190cfde7c399042a6c589152f64e86796a7fa`，本轮新增验收脚本和记录，未修改生产 Rust 代码。
使用仓库 `build/release/package.py` 的 assemble/verify 生成并校验 x86_64 GNU debug 包；
包含 8 个 ADX 二进制、静态 musl Execd、独立 EROFS、SDK wheel，以及固定 Redis 7.2.5。
这份开发验收包没有作为正式 release 发布。

外部依赖与 guest/宿主环境沿用 [Linux 运行时验收记录](jiuwenbox-linux-20261016.md)：
Jiuwenbox develop `9e0569f4fd047e510f4ee4ff28c26c54aaec5066`、Conch master
`a9a756d86e4e48305c4a19cc3a63bd53ae972659`。验收后再次比对原始 archive，
97/248 个源码文件均无内容变化。仅生成隔离的服务配置和测试 policy。

`adxctl validate → render → run` 启动独立 Redis、Coordinator、ADXlet（内嵌 Relay）、
API Server 和独立 Ingress。ADXlet 直接接 Jiuwenbox，使用 Auto 容量采样。
公开 API 入口在本机 loopback 上启用 TLS/API key，内部 RPC 和 Ingress → Relay 使用 mTLS。
命令和文件调用经过公开 Ingress → Relay → Execd，未使用直接运行时驱动完成业务断言。

SDK 为 `adx-environment 0.1.0rc1`，安装来自同包 wheel。
客户端使用独立 `sdk-venv/bin/python -I` 进程；记录的模块位置在该 venv 的
site-packages，未通过 PYTHONPATH 导入源码。

Execd EROFS SHA256 为
`425d38ba8a38de1b1ffbccb53fd7e952f33ac8bc2223a9b9ad264febf9c356b2`。
宿主 `findmnt` 确认 EROFS/ro；两个 guest 内的 Execd 摘要与包内静态二进制一致，
向 `/__adx` 写入均失败。Conch 经现有 virtiofs volume 暴露该只读目录。

## 业务与故障场景

| 场景 | supervisor | conch |
|---|---|---|
| SDK 创建、查询 Running、继承业务 sleep 入口 | 通过 | 通过 |
| exec stdout=`adx-sdk`、stderr=`stderr-sdk`、exit=7 | 通过 | 通过 |
| 170,000 字节二进制文件往返、UTF-8 文件读写 | 通过 | 通过 |
| 独立 Execd EROFS 摘要及只读性 | 通过 | 通过 |
| ADXlet SIGKILL，adxctl 拉起后接管原执行 | 通过 | 通过 |
| Coordinator SIGKILL，从 Redis 恢复控制状态 | 通过 | 通过 |
| API Server / Ingress 分别 SIGKILL 后恢复公开调用 | 通过 | 通过 |
| Jiuwenbox SIGKILL 后清理旧执行并自动冷重建 | 通过 | 通过 |
| conchd SIGKILL 后清理旧 VM 并自动冷重建 | 原执行持续可用 | 通过 |
| SDK kill，重新 from_id 返回 EnvironmentNotFound | 通过 | 通过 |

机器结果按 2 个创建综合用例、6 类组件重启、2 个删除及 1 个原生资源审计计为 11 项。
各次重启后都使用原 SDK handle 重新执行命令、文件读取及 EROFS 检查。

控制/数据面进程重启时，两个 Environment 的 runtime ID 和 ownership generation 保持原值。
Jiuwenbox 重启后，Environment ID/assignment 保持，两个 runtime ID 均变更，
restart_attempts 从 0 变为 1；conchd 重启后仅 Conch 再换 runtime ID，attempts 变为 2。
创建时显式配置 `RestartPolicy(max_attempts=5, initial_backoff_seconds=1, max_backoff_seconds=2)`，
未启用 failover 或快照。

测试 policy 将宿主验收目录以可写卷挂到 `/workspace`。170,000 字节二进制的 SHA256
始终为 `7d925f11bf5d15fdc4edd15d7b8f4c09f474fadd7aa0bb915e068e26f88a9daa`，
在 runtime 替换后仍可通过 SDK 读取。公开 SDK 的 Mount 目前只支持只读 image/S3 源；
本结果证明已有卷在重建后的持久性，不代表新增了公开可写 Local 挂载参数，也不承诺临时根目录持久化。

最终审计确认所有 5 个原始/替换执行均 Deleted，Conch 原生查询均 404；无任务 Execd、
VM、virtiofsd、supervisor cgroup/netns/veth、ADX 防火墙链或 EROFS loop 残留。
停止服务后，Conch 网络池命名空间及 bridge 端口也为空。原有 AKernel 容器继续运行。

## 证据与复现

入口为 [full_sdk.py](../../build/e2e/jiuwenbox/full_sdk.py) 和
[隔离的公开 SDK 客户端](../../build/e2e/jiuwenbox/sdk_client.py)，准备步骤见
[复现说明](../../build/e2e/jiuwenbox/README.md)。

本地证据在 `out/jiuwenbox-linux-20261016/sdk-evidence/`；成功轮为 `runs/sdk003`。
包含结果、各重启前后控制记录、安装位置、组件日志、runtime/宿主审计和 package manifest。
可分享证据包 `sdk-evidence.tar.gz` 的 SHA256 为
`3f04f5f0b1595ea015347b656797cfab248dd78ba90031de7500455d148f8cf7`。
包内不包含部署凭证、私钥、完整 deployment YAML 或渲染的 Redis 密码配置。

前两轮的失败日志也保留：sdk001 在配置扩展名校验处失败；sdk002 的未认证公共
health 请求使 readiness 等待超时。修正验收脚本的 YAML 文件名、认证 resources 检查，
并将 Relay target CIDR 配到 Jiuwenbox 使用的地址池后，标准包的 sdk003 完整通过。
最终轮启动早期一次 Redis 尚未 ready 的连接拒绝被正常重试，全部场景及清理终态通过。

上述为 Linux 单机进程部署验收。后续已完成独立 VM 宿主强制 OS 重启与 Redis 故障恢复，
见 [2026-10-19 验收记录](jiuwenbox-recovery-20261019.md)。宿主断电/丢盘、多节点、资源限额与
网络隔离矩阵仍需独立验证；checkpoint/restore 不在本阶段范围内。
