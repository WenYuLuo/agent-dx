# ADXlet 周期对账与启动恢复拆分 — 2026-10-19

本次基于 `feat/jiuwenbox-runtime-p0@e6bb99a` 的已有检视工作树修改公共 ADXlet 对账。
Coordinator 是归属权威；sandboxd、supervisor、Conch 继续使用同一公共周期入口。

## 行为

- 启动恢复使用 `ReconciliationMode::Recover`，重建本地控制器、资源和路由。
- 周期入口使用 `Compare`。已发布本地记录与权威记录一致，且后端执行身份和状态一致时，
  不执行 readiness、路由 activate、状态 commit 或整表绑定替换，不重置实例计时。
- 目录中缺少本地控制器时恢复对应实例；后端执行丢失时继续发布失败并释放资源。
  多余执行、无引用终态的迟到执行继续清理，删除失败在后续周期重试。
- 版本检查经过实例串行队列。目录落后于已发布本地结果时，在 inventory/adoption/删除前
  返回 StaleCatalog/Aborted，保留已有生命周期和路由，下轮获取新目录；不触发注册重握手。
  权威确认的新终态仍清理执行，保留已发布的终态控制器承接显式删除，不重复 commit。
- 周期目录获取阶段仅关闭新分配，已有生命周期调用继续；实际比对持有写锁，新调用等待，
  避免健康扫描期间偶发返回 `node is reconciling`。
- 完成启动恢复的内存标记与暂时准入状态分开。删除失败、ready 上报失败后的重试仍是比对。
  Coordinator 重启后的注册握手独立重试，不强制重做健康实例恢复；Relay 实际重启仍可恢复绑定。

Jiuwenbox/Conch 的 wheel、RPM、SDK、VMM 保持原内容。Execd 使用原独立只读 EROFS，
共享挂载按既有约定保留；本次没有引入本地归属持久化或快照能力。

## TDD 与组件验证

本地证据目录：`out/periodic-comparison-20261019/`。

- `red02.log`：一致实例与孤儿删除重试场景均复现重复恢复副作用。
- `red-stale-terminal.log`：旧终态目录会错误进入删除路径，修正版本检查顺序后通过。
- `red-handshake-gate.log`：复现 Coordinator 重新握手阻塞和目录获取期间已有生命周期被关闭。
- `red-pending-race03.log`：复现 Pending 目录落后于已发布 Running 导致关门禁和误握手。
- `red-authority-expiry.log`、`red-terminal-delete.log`：复现权威过期清理及终态显式删除边界。
- `red-retired-cache.log`：复现控制器已回收、删除已发布后，旧目录错误重建控制器。
- `green09.log`：macOS 三套公共流程测试 **43/43**，包括孤儿、晚到创建、清理失败重试、
  执行丢失、旧目录、ready 重试、Coordinator 握手和有效执行无恢复副作用。
- 最终 Linux 六套测试 **131/131**：ADXlet lib、reconciliation、node_reconciliation、routes、
  sandboxd_rpc、jiuwenbox_runtime；workspace 全 targets/features Clippy `-D warnings` 通过。
- macOS 完整 ADXlet 测试在 3 项已有 managed_mounts 用例失败，该实现依赖 Linux `/proc/self/fd`；
  macOS workspace Clippy 缺少 pkg-config/FUSE。未修改这些路径；Linux 对应 lib 与 workspace gate 通过。

## 最终部署身份

远端最终根目录：`47.83.171.126:/root/adx-periodic-closure-20261019/`。

| 项目 | SHA256 |
|---|---|
| 最终源码清单 `source-manifest-03.json` | `893f0c79421c8887c9577f417b8b47093b89a0cc4970a6ca4c60c2fd07ee67cd` |
| ADX 包 `package-sdk/manifest.json` | `ff2d6d7f7e04b792ce63100230ecaa4968b435e9a2f7ce951cd8f914923885a7` |
| Jiuwenbox/Conch 独立 Execd EROFS | `dd787079e646e692379a4a61668c8f074a1f69cdceed3734a414d40e6d3cc11d` |

包为 Linux GNU debug，复制后剥离 debug 符号。只有 ADXlet 二进制相对上一验收包改变；
其余服务、SDK 和运行时镜像复用此前校验过的内容。镜像启动后逐项校验包及运行中二进制。

## 真实部署回归

下文保留本轮原始验收结果；剩余 runc PTY 断连的后续诊断与修复见
[PTY 关闭竞争回归](runc-pty-close-20261019.md)。

最终 `ff2d6d…` 包的 supervisor/Conch 完整 ADX 部署与公开安装 SDK **11/11 通过**，
run 为 `periodic-sdk004`。覆盖创建、exec、二进制与 UTF-8 文件、只读独立 EROFS、
ADXlet/Coordinator/API Server/Ingress/Jiuwenbox/conchd 六项服务重启、两种后端删除与原生资源审计。
恢复后 Environment ID 和卷内文件保留；ADXlet/Coordinator 重启使用原执行，后端服务重启按契约冷重建。

`runtime/audit.json` 和 `host-audit.json` 的 leftovers 均为空，Conch bridge ports 为空，
独立只读 EROFS `/dev/loop1` 保留。对 8 个固定缓存文件、55 个已安装 wheel 文件、35 个 RPM 文件
再次核对，内容均保持原样；服务 policy 和运行目录仍是独立 fixture 配置。

sandboxd 最终包的 runsc **18/18 组通过**，连续短命令 PTY **3/3**；16 秒流和 16 MiB
文件校验通过，Redis 资源与后端物理清单为空。两种运行时均核对 `report_interval_seconds=5`，
没有用延长周期代替默认配置验收。

最终包 runc 的连续 PTY **2/3**：第二轮在 14.742 秒报
`PTY WebSocket connection failed: no close frame received or sent`。其余 **11/11 适用 E2E 组**、
基础运行时集成 **7/7**、16 秒流和 16 MiB 文件校验通过；最终 Redis/后端为空，所属容器均删除，
既有 protected 部署保持运行。runc 脚本仍返回 1，不将额外 PTY 失败折算为全绿。
对应环境在失败前保持 Running，Aborted/旧目录警告发生在测试清理删除之后。
不能据此继续把该次断连归因于周期对账，也不能宣称 PTY 回归全部通过；根因仍待定位。

此前 `bb2abf…` 中间包的 runsc 为 18/18、PTY 3/3；runc 为 11/11 适用组、
基础运行时 7/7，额外连续 PTY 2/3。中间包结果与最终包记录分开，不替代最终版本验收。

中间轮失败保留在 `adx-periodic-comparison-20261019`、`adx-periodic-final-20261019` 及
当前根目录的 `periodic-sdk003`。这些轮次分别复现提前关门禁、Coordinator 握手及 Pending/Running、
已回收控制器/旧目录之间的竞争；未计为最终通过。被取消的过期运行记录了自有容器删除和旧部署保护状态。

## 验证范围

本轮是 Linux x86_64 单机进程/容器验收。宿主重启、Redis 故障、多节点、Kata、Firecracker
未重跑；Conch 快照不在当前范围。runc 原生不支持的 ACL 不列入本轮正向适用组。

## 交付证据

本地目录 `out/periodic-comparison-20261019/` 保存 RED/GREEN 日志、构建源码清单、
本轮补丁 `periodic-only.patch` 和脱敏后的 Linux 证据包。远端最终验收为
`runs/periodic-sdk004/`、`akernel-runsc-final/`、`akernel-runc-final/`；失败尝试单独保留。
最终镜像为 `akernel-local/adx-periodic-closure:20261019-final`，ID 为
`sha256:a369a6939e2b065774ff737e8280e7ba6fded587854ec7ed5dc9b08c17de62cd`。

本轮代码保留在原 worktree，尚未提交。构建快照之后仅补写验收文档，Rust/proto/Cargo.lock
与最终构建清单核对一致。
