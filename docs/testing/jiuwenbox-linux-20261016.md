# Jiuwenbox / Conch Linux runtime acceptance — 2026-10-16

生产 Jiuwenbox RuntimeDriver 与 LinuxHost 在专用 Linux x86_64 KVM 主机上完成
9/9 单机真实运行时场景。全部逐例清理审计及服务停止后的宿主审计通过。
Jiuwenbox 和 Conch 源码均保持不变，未验证 checkpoint/restore。

## 源码与运行环境

- ADX 基于 master `fb77fc4195580c958235c99254f782aaf18a0c63`，分支
  `feat/jiuwenbox-runtime-p0`；本轮在初始提交 `75c8ba0` 上修复实际接口约束。
- Jiuwenbox develop `9e0569f4fd047e510f4ee4ff28c26c54aaec5066`，
  Conch master `a9a756d86e4e48305c4a19cc3a63bd53ae972659`。
  与原始源码 archive 比对：分别 97、248 个文件，无内容变化。
- 宿主 Linux `6.8.0-137-generic`，cgroup v2，KVM API 12；实际创建/关闭 KVM VM fd 成功。
- Cloud Hypervisor v52.0-conch；virtiofsd 1.13.3；erofs-utils 1.9。
- guest 为 Linux 6.12，PVH、EROFS、overlayfs、virtiofs/FUSE、virtio-pmem 和
  virtio-vsock 内建；Conch initramfs 从同一份未修改源码构建。
- Ubuntu 24.04 转换为内容寻址的 Conch template；入口业务为 sleep，未预装 Execd。
  Execd 使用当前 ADX 构建的静态 musl 二进制，通过独立 EROFS 只读挂载。

| 制品 | SHA256 |
|---|---|
| Execd EROFS | `425d38ba8a38de1b1ffbccb53fd7e952f33ac8bc2223a9b9ad264febf9c356b2` |
| guest vmlinux | `d79d291dea332380b17ee486fedec241a2fabdda967ed178b43ac47a74b739a8` |

完整环境和二进制摘要保存在验收制品的 `artifacts/environment.json`。

## 场景与证据

| 后端 | 场景 | 结果 |
|---|---|---|
| supervisor | 创建、Execd readiness/命令、独立 EROFS、删除 | 通过 |
| supervisor | 驱动进程 SIGKILL 后打开原所有权目录并清理 | 通过 |
| supervisor | Jiuwenbox SIGKILL 后重启、对账清理 | 通过 |
| supervisor | 实际创建成功，响应晚于 caller deadline，删除等待创建完成 | 通过 |
| conch | 创建、Execd readiness/命令、独立 EROFS 经 virtiofs 暴露、删除 | 通过 |
| conch | 驱动进程 SIGKILL 后打开原所有权目录并清理 | 通过 |
| conch | Jiuwenbox SIGKILL 后重启、经 Conch 原生接口清理 | 通过 |
| conch | 实际 VM 创建成功，响应晚于 caller deadline，删除等待创建完成 | 通过 |
| conch | conchd SIGKILL 后重启、清理旧执行 | 通过 |

宿主 `findmnt` 确认 EROFS/ro；guest 从 `/__adx` 执行的 Execd 二进制摘要与独立
构建产物一致，向该目录写入失败。Conch guest 的传递层为 virtiofs。
入口配置另以只读 volume 挂在 `/__adx_launch`。

超时测试使用独立 UDS 代理延迟成功创建响应，真实 backend 始终实际运行。
调用方得到 DeadlineExceeded；并发删除在响应释放前保持等待，随后完成物理清理。
真实 Jiuwenbox access log 的两个 late sandbox 均无 Execd background 启动请求。

每例 `audit.json` 检查 journal 已 Deleted、Conch 原生实例不存在、Execd/VM/virtiofsd
进程、supervisor cgroup/netns/veth、ADX 防火墙链和 Execd loop 挂载；9 份
`leftovers` 均为空。最终服务停止后的 `host-audit.json` 确认 Conch 网络池命名空间、
其 bridge 端口和任务后端进程也无残留。保留源码、构建/模板缓存与日志。

验收入口为 [accept.py](../../build/e2e/jiuwenbox/accept.py) 和
[Rust 生产驱动桥接程序](../../platform/adxlet/examples/jiuwenbox_acceptance.rs)。
使用方法见 [复现说明](../../build/e2e/jiuwenbox/README.md)。原始证据保留在本轮
`out/jiuwenbox-linux-20261016/evidence/`，成功轮为 `runs/run006`；先前失败轮同样保留。

## 本轮修复与检查

真实 Linux 验证发现并修复以下 ADX 问题：

- supervisor 请求映射为 Jiuwenbox 接受的 `bwrap`。
- 后端 sandbox ID 使用共同接受的 32 字符格式；清理按 journal 中保存的 ID 定位原目录。
- 将入口配置挂载移出 Conch 禁止的 `/run`，并在副作用前拒绝保留目录。
- 所有权库仅在进程独占 lease 下初始化 WAL/schema；更新使用 IMMEDIATE 事务，消除并发锁升级失败。
- 修正部署说明：当前 Conch SDK 固定连接 `/var/run/conch/conchd.sock`。

完整 Linux adxlet 测试：238 通过，0 失败，0 跳过。
工作区 `cargo clippy --workspace --all-targets --all-features -- -D warnings` 通过。
初次发现的数据库并发失败、请求约束失败和缺少 PVH 的 guest 内核启动失败均保留日志；
最终成功轮使用修复后的 ADX 和补齐 PVH 的验收内核。

上述 9 项为生产驱动与真实后端的集成验证。后续完整 ADX 单机部署和公开 SDK
验收另有 11/11 场景通过，见 [完整部署与 SDK 验收记录](jiuwenbox-sdk-20261016.md)。
资源限制/网络隔离矩阵、宿主重启及多节点仍需独立验证。
