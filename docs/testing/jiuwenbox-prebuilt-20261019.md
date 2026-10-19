# Jiuwenbox / Conch 预编译产物验收 — 2026-10-19

Linux x86_64 单机进程部署验收通过：真实运行时 9/9，完整 ADX 部署与公开 SDK 11/11。
Jiuwenbox、Conch 服务和 SDK 使用原始 wheel/RPM，未修改源文件或二进制。
Conch 使用 RPM 配套的 StratoVirt、guest kernel/initrd；Execd 继续由 ADX 独立 EROFS 提供。

## 最新 ADX 包复验（PTY 修复后）

使用原始预编译 JiuwenSwarm wheel、Conch RPM/SDK，以及与 sandboxd 回归相同的
ADX 包重新完整验收，`final-runtime003` **9/9**、`final-sdk003` **11/11** 通过。
未修改 Jiuwenbox、Conch 或其 SDK；原始 archive/member 与辅助程序身份保持不变。

本次固定 `feat/jiuwenbox-runtime-p0@e6bb99a` 的当前未提交源码，复用 Linux x86_64
GNU debug 服务与 musl release Execd。当前工作树 520 个 Rust/proto/Cargo 文件与构建清单相符。
Execd 包含 [PTY 关闭握手修复](runc-pty-close-20261019.md)，继续使用独立只读 EROFS。

| 本次部署身份 | SHA256 |
|---|---|
| ADX 包 manifest | `254dcb9bb9279aacd42bc11ffbe421cb8845f905ff77ceb4f9c47c0ac094ddeb` |
| ADX 构建源码 manifest | `9a4b01aa89e09f3dfdbd0f0009034c3638e94752ebfb319dd9172bdd27996524` |
| Execd | `65986e4419ff533aaab0fbeee91e4a8812b6519d0af27f1deaad3928bfb8fd1b` |
| 独立 Execd EROFS | `b177a7fa2ee5147aa2cb41dd29ac4031d4c42812c5fc66cef4babd92d46cfd8a` |
| 安装的公开 SDK wheel | `9ce96591e1f3b1b7c10e47390ef6dccd4d5df6e3346c9d9ab33904643125f71a` |

完整部署仍通过 `adxctl validate → render → run`，公开 `adx-environment 0.1.0rc1`
安装到独立 sdk-venv 并以 Python `-I` 执行，21 个 SDK 包内文件与 wheel 逐一相同。
两种后端覆盖创建、stdout/stderr/非零退出、二进制和 UTF-8 文件读写、guest Execd 摘要、
只读 EROFS、公开删除及不存在查询。ADXlet、Coordinator、API Server、Ingress 单进程重启后
继续使用原 Environment；Jiuwenbox/conchd 重启按冷重建契约恢复，卷内内容保留。
运行时套件另覆盖驱动重启、创建响应超时晚到，以及 conchd 重启后的原生资源清理。
本 fixture 的节点上报周期为 1 秒，保持验收脚本原配置。

两轮前置失败保留：`final-runtime001` 受任务外层宿主 TMPDIR 影响，路径在 supervisor
沙箱内不存在，Execd 无法创建临时输出文件；撤掉该变量后运行时 9/9 通过。
`final-sdk002` 的重启前路径断言不接受符号链接包目录，改为内容相同的独立副本后完整重跑。
两次仅调整任务夹具，产品二进制、EROFS、SDK 和外部 wheel/RPM 内容均未改变。

最终 `runtime/audit.json` 与两层 host audit 的 leftovers 均为空，Conch bridge ports 为空，
测试服务停止，既有 protected 部署仍运行。按约定保留只读 bootstrap EROFS 挂载（loop2）；
独立挂载不计为孤儿。8 个固定缓存文件、55 个外部 wheel 文件、35 个 RPM payload 文件全部复核通过。
锁文件已更新本次验收身份，并补充实际 conch/conchd 二进制摘要；源码 revision 仍标为未知。

远端证据根目录 `/root/adx-jbx-prebuilt-final-20261019/`，本地对应
`out/jiuwenbox-prebuilt-final-20261019/`。脱敏包 `evidence.tar.gz` 含 151 个文件，SHA256：
`37908220f3c34e698e2af2b9b93193e3503d7e4dd656a83c7806cf0bdb538bb3`。
归档排除配置/密钥/后端状态，并核查 4 个本轮 API/Redis 密钥未泄漏。
系统盘无普通用户可用空间，运行目录使用任务专属 tmpfs；原始固定产物缓存仍在持久磁盘，
脱敏证据已回传本地。验收服务已停止，承载共享 EROFS 的 fixture 挂载保留。
本次为 Linux x86_64 单机验收；宿主重启、Redis 故障、多节点和 aarch64 未重跑，快照暂不做。

下文保留首次预编译产物验收的构建、修正及证据，当前验收身份以上述复验和锁文件为准。

## 固定产物

完整来源、archive/member 摘要、依赖及验收身份见
[预编译产物锁文件](../../build/e2e/jiuwenbox/prebuilt-lock.json)。

| 组件 | 包版本 | SHA256 |
|---|---|---|
| Jiuwenbox 服务端（JiuwenSwarm wheel） | `1` | `37f8ef116ffc46cf1753a5388e44a02aebffee7b8644d075398895a73fb94fbe` |
| Conch RPM | `0.1.0-10.oe2403sp4` | `fa07d014e3f9cadc5b30301c307de6145ad66b112d464af82524cc6bf6af5757` |
| Conch SDK | `0.2.0` | `da54ff97e32e01670c4610801e65cf88d72202013e66e3089f722ff5026303e0` |
| StratoVirt RPM | `2.4.0-14.oe2403sp4` | `23acdb5f47715c7b6975e10a09af55432c761318b105827aa77406fdd8d3e77e` |

JiuwenSwarm 来源为 OBS `daily/agentos_b050/last_successful_build`，Conch 来源为
OBS `release/agentos_b050/last_successful_build`。保留原渠道属性，按摘要固定本次内容。
JiuwenSwarm wheel 版本号只有 `1`；Conch 自报 `unknown`，StratoVirt RPM 标注 2.4.0，
二进制自报 2.5.0。锁文件分别记录包标识、运行时版本与摘要，不凭这些标签推断源码 commit。
两个服务包均未提供可确认的源码 revision，字段保留为 null；guest kernel 提供了独立 revision。

原始 archive、选中的 wheel/RPM、Conch SDK 和 virtiofsd 已按 SHA256 保存在验收主机
`47.83.171.126:/root/adx-jbx-prebuilt-20261019/pinned/`。
锁文件的 `cache_path` 相对此目录。复用缓存时校验摘要；上游滚动 URL 发生变化时不能
用新内容覆盖本次固定版本。aarch64 产物虽包含在原始 archive 内，本轮只验收 x86_64。

## 部署与未修改校验（首次验收）

ADX 使用 `feat/jiuwenbox-runtime-p0@e6bb99a` 加当前工作树修改，以 Linux GNU debug
配置构建；打包前对复制的 ADX 二进制去除 debug 符号，未改动 Cargo 缓存中的输出。
部署源码快照为 `adx-source-manifest-03.json`：

- 源码清单 SHA256：`93196e99e320e28ca9f23b276ef8f5ba7b5f7685414087d7fed932fd0071b4bc`。
- ADX 包 manifest SHA256：`3ff1cd2e01843b2a06ec62631762b0e63c83dff232121d7dd2f717f50540af23`。
- Execd EROFS SHA256：`dd787079e646e692379a4a61668c8f074a1f69cdceed3734a414d40e6d3cc11d`。

Conch RPM 解包到独立 fixture，使用原始 conchd、CLI、SDK、kernel、initrd 和
StratoVirt；未执行全局 RPM 安装脚本。JiuwenSwarm wheel 安装到独立 venv，
公开 `adx-environment 0.1.0rc1` wheel 安装到另一 sdk-venv，客户端以 Python `-I` 运行。
virtiofsd 复用此前构建的 1.13.3，并单独固定二进制副本和摘要。

结束后重新比对原始 wheel 内的 Jiuwenbox / Conch SDK / 生成 RPC 文件，共 55 个文件；
重新解析三个 x86_64 RPM payload，共 35 个常规文件逐一比对，symlink 也核对目标，均无变化。
服务 policy、Conch work/state 目录和 CNI 参数属于 fixture 配置，未写回包内源文件。

## 验收结果（首次验收）

| 验证层 | 场景 | 结果 |
|---|---|---|
| 真实运行时 `prebuilt-runtime003` | 两种后端创建、Execd、只读 EROFS、删除；驱动 / Jiuwenbox 重启；晚到创建；conchd 重启清理 | 9/9 |
| 完整部署 `prebuilt-sdk002` | `adxctl validate → render → run`，Redis、Coordinator、adxlet/Relay、API Server、Ingress | 通过 |
| 公开安装后 SDK | 两种后端创建、stdout/stderr/非零退出码、二进制与 UTF-8 文件读写、EROFS 摘要与只读验证 | 通过 |
| 进程恢复 | ADXlet、Coordinator、API Server、Ingress 重启；Jiuwenbox 和 conchd 冷重建后工作卷内容保留 | 通过 |
| 删除与审计 | SDK 删除及查询不存在；5 个历史执行对应的原生资源清理；无进程、cgroup/netns/veth 或 Conch bridge 端口残留 | 通过 |

公开 SDK 共 11/11 场景通过，原始 Environment ID 在恢复后仍可使用。
ADXlet / Coordinator 重启采用原 runtime；Jiuwenbox / conchd 重启按契约冷重建。
共享 Execd EROFS 继续保留 `/dev/loop1`，不作为孤儿资源。
所有验收服务已停止，保留源码/模板/产物缓存、证据和约定的只读挂载。

## ADX 修正与回归（首次验收）

`prebuilt-runtime001` 发现 Jiuwenbox 清单字段是 `sandbox_runtime`，ADX 原来读取
`runtime`，导致驱动重启对账失败。修正字段后，补充按实际 API 字段构造的回归。
这份 wheel 的 Conch policy 只接受 `template_id`，因此新增显式内容 ID image 绑定，
与 template_name 互斥；只发送所选字段，外部服务保持不变。

`prebuilt-sdk001` 在 UTF-8 文件下载时收到截断响应。周期对账重复撤下 Coordinator 路由、
清空 Relay 绑定，导致现有数据流中断。修正公共流程：有效会话刷新时关闭新分配并保持
路由可见；InspectNode 接受当前会话在分配关闭时检查目录；已有 Relay 会话按绑定版本同步。
首次启动、会话变更及 Relay 重启仍走完整同步。

两项真实 Redis 7.2.5 / mTLS / HTTP2 回归先复现 Inspector 拒绝及既有流中断，再验证通过；
同时验证旧会话 fencing、分配开放时拒绝 Inspect，以及同一流跨 3 次绑定对账继续收发。
macOS 7 套定向测试 103/103 通过；Linux ADXlet 完整测试 263/263 通过；
workspace 全 targets/features Clippy `-D warnings` 通过。
格式、文档和 diff 检查通过；文档检查覆盖 154 份文档、748 个本地链接、45 个 JSON 示例。

## 证据与边界（首次验收）

本地证据根目录：`out/jiuwenbox-prebuilt-20261019/`；远端根目录：
`/root/adx-jbx-prebuilt-20261019/`。关键证据包括：

- `rebuild-adx-03.log`、`prebuilt-runtime003.log`、`prebuilt-sdk002.log`。
- 两个成功 run 的 `results.json`、`host-audit.json`，SDK 的 `runtime/audit.json` 与安装身份。
- `rpc-route-red.log`、`red-periodic-routes*.log`、`green-periodic-routes.log`。
- `linux-route-gates-final.log`，四个生产源码文件的跑前/跑后摘要校验。
- `pin.log`、`artifacts/post-run-integrity.json` 和固定清单。失败轮日志继续保留。

8 个固定缓存文件全部通过 `SHA256SUMS` 复核。包含 166 个文件的证据包已回传本地，
排除了配置/密钥文件，并检查未包含本轮 API / Redis 密钥或 PEM 私钥。
证据包 `prebuilt-evidence.tar.gz` 的 SHA256 为
`bc5df4e26738a681967e715ad1538dde8026ccb22092d1d5698dca3b5930647c`。

本轮覆盖单机真实后端和完整公开 SDK；宿主重启、Redis 故障、资源隔离/ACL 矩阵、
多节点和 aarch64 未重跑，快照不在本阶段范围。未增加 Jiuwenbox 本地所有权数据库。
准备和复现接口见 [验收说明](../../build/e2e/jiuwenbox/README.md)。
