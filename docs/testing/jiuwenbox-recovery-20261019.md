# Jiuwenbox / Conch 宿主重启及 Redis 故障验收 — 2026-10-19

supervisor/conch 的 Redis 故障恢复、真实 Linux VM 宿主重启恢复、卷文件保留及最终
孤儿资源审计均通过。本轮发现并修复一处 ADX 启动恢复缺口；Jiuwenbox、Conch 源码未修改，
未使用 checkpoint/restore。

## 版本与环境

分支 `feat/jiuwenbox-runtime-p0`，沿用 ADX master 基线 `fb77fc419558`。
原部署二进制 `efb190c`，修复后标准包源码提交
`4fa09b59182f4a8119bed304a4b4d740eaeb23fd`；assemble/verify 校验通过。
包为 x86_64 GNU debug，使用 Redis 7.2.5、静态 musl Execd 和独立只读 EROFS，
安装后的 `adx-environment 0.1.0rc1` wheel 经 `python -I` 运行。
Execd EROFS 摘要与 [上轮部署验收](jiuwenbox-sdk-20261016.md) 相同。

外部代码继续固定为 Jiuwenbox develop `9e0569f4fd047e510f4ee4ff28c26c54aaec5066`
和 Conch master `a9a756d86e4e48305c4a19cc3a63bd53ae972659`。
在父机与独立 VM 分别比对原始 archive：97/248 个源码文件均无变化。

共享开发机上已有 ADX/AFS 和 AKernel 服务，因此没有重启共享开发机。为真实宿主重启
创建独立 Ubuntu 24.04 VM（4 vCPU、8 GiB、持久化 qcow2 磁盘），kernel 为
`6.8.0-147-generic`。VM 内 KVM API 12 和 CREATE_VM ioctl 通过，且实际成功创建
Conch 的内层 VM；不是仅重启容器或模拟 backend。Ubuntu 基础镜像 SHA256 为
`04e8f2461a16a44e78a370284471f29616cdd9baf1893df25807b71f985b6c94`。

ADX、Jiuwenbox、conchd 通过验收专用 systemd units 在 VM 开机时启动。
Redis AOF（appendfsync=always）、ADX 所有权 SQLite、原配置和 `/workspace` 均位于持久化磁盘。
创建环境时启用有界 RestartPolicy，未启用 failover。单位中断保留期望状态；显式
`adxctl stop` 的 drain/delete 语义不变。本次注入 `/sbin/reboot -f`，跳过部署优雅删除。

## 结果

| 故障/检查 | supervisor | conch |
|---|---|---|
| Redis SIGKILL，AOF 重放后恢复 | 原执行保持、文件通过 | 原执行保持、文件通过 |
| Redis SIGSTOP 35 秒，再 SIGCONT | 原执行保持、文件通过 | 原执行保持、文件通过 |
| Redis 恢复后再重启 Coordinator | 原执行保持、文件通过 | 原执行保持、文件通过 |
| VM 强制 OS 重启，公开 SDK 按原 ID 重连 | 冷重建通过 | 冷重建通过 |
| 重启后 exec stdout/stderr/exit=7、UTF-8 读写 | 通过 | 通过 |
| 重启前 170,000 字节卷文件及 SHA256 | 保留 | 保留 |
| 独立 Execd EROFS 摘要及只读检查 | 通过 | 通过 |
| 公开 SDK 删除、原生与宿主资源审计 | 通过 | 通过 |

Redis 成功轮 `redis002` 为 4/4 综合用例通过。整个故障序列前后，Environment ID、
assignment 和 runtime ID 均保持。期间独立读取 ADX 所有权 journal，两个执行仍为 Ready。

宿主成功轮为 `reboot002`，kernel boot ID 从
`8f028e35-d337-485e-ba96-06fcefcb0c8e` 变为
`1b6532a7-16e5-43db-a043-7ebed5ab043e`。两个 Environment 的 ID/assignment 保持，
执行 ID 分别从原来的 `-1`、`-2` 结尾变为 `-1-r4`、`-2-r4`，restart_attempts 均由 0 变为 1。
SDK 使用 `Environment.from_id()` 重新建立本地句柄，并通过公开 Ingress/Relay 完成业务检查。

卷文件始终为 170,000 字节，SHA256 为
`7d925f11bf5d15fdc4edd15d7b8f4c09f474fadd7aa0bb915e068e26f88a9daa`。
`/workspace` 来自验收 policy 的可写卷；本轮没有新增公开 SDK Local 挂载参数，也不承诺
临时 rootfs 内容在冷重建后保留。

最终 Redis 轮的 2 条执行和宿主重启轮的 4 条原始/替换执行均 Deleted，Conch 原生查询
均不存在；无任务 Execd/VM/virtiofsd、supervisor cgroup/netns/veth、ADX 防火墙链、
EROFS loop 或 Conch 网络池/bridge 端口残留。所有验收 services/units 已停用，独立 VM
已关机。共享开发机 boot ID 保持原值，原 ADX/AFS、AKernel 容器继续运行。

## ADX 修复及失败证据

原逻辑在启动对账确认 Running 执行丢失时，仅发布 Failed，未设置 restart_pending。
新增单测先失败；原包 `reboot001` 的真实 OS 重启也复现：两环境都 Failed，配置仍有
重启策略，但 pending=false、attempts=0。

修复在确认旧执行物理清理完成后，共用现有重启调度逻辑；保留预算、Environment ID 和
assignment，后续 tick 执行冷重建。未配置策略、预算耗尽、后端存量无法确认或清理失败时
不会据此启动新执行。新回归覆盖冷重建、耗尽预算、清理屏障，原无重启策略的行为保持。
完整 ADXlet 测试 **241/241**，无跳过；workspace all-targets/all-features Clippy、格式检查通过。

修复包的 `reboot002` 首次 check 已完成恢复/卷文件检查，后因测试客户端对 from_id 句柄
调用 kill 未删除远端而失败。按 SDK 既有句柄语义改用公开 `Environment.delete()`，保留首轮
日志后继续检查同一实际重启事件，恢复、删除及最终审计全部通过。SDK 产品代码未修改。

## 证据与复现

入口为 [Redis 驱动](../../build/e2e/jiuwenbox/redis_recovery.py)、
[宿主重启分阶段驱动](../../build/e2e/jiuwenbox/host_recovery.py)；用法及准备条件见
[复现说明](../../build/e2e/jiuwenbox/README.md)。

本地证据根目录为 `out/jiuwenbox-recovery-20261019/`：

- `evidence/parent/runs/redis002/`：4 项结果、各阶段执行身份、文件摘要、最终审计。
- `evidence/vm/runs/reboot002/`：boot ID、重启前后记录、公开 SDK 结果及原生/宿主审计。
- `reboot001` 和 `redis001`、SDK 删除修正前日志、Rust red/green/Clippy 与打包日志也保留。
- `recovery-parent-evidence.tar.gz` SHA256：
  `f84d2ec4da154f3c198557ff8cfc4df3a8c41e74ec73ee58e0ead70fd80098f3`。
- `recovery-vm-evidence.tar.gz` SHA256：
  `cb0a7ca7e361a86a9d63cb4e5f91448e449dbad3233fb3fc148da4670b976c96`。

证据包只包含选定结果与日志；排除凭证、私钥和渲染配置，并对本轮生成的 API/Redis key
进行内容扫描确认无命中。源码、包、VM 磁盘、构建缓存和私有配置保留在各自隔离目录中。

本结果覆盖保留磁盘/AOF/所有权库的强制 OS 重启与 Redis 进程/连接中断，不代表断电丢写、
磁盘丢失、Redis 数据丢失或主从 HA 验收。多节点和 checkpoint/restore 仍不在本轮范围内。
