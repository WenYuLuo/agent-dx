# Jiuwenbox runtime P0

本实现基于 ADX master `fb77fc419558`，适配 Jiuwenbox develop
`9e0569f4fd047e510f4ee4ff28c26c54aaec5066` 和 Conch master
`a9a756d86e4e48305c4a19cc3a63bd53ae972659` 的现有接口。Conch dev
`a83e40fc3bf29792eeeafbd61585afad86426e36` 的 SDK 和 agent proto 与该 master 相同。
本变更仅修改 ADX，不修改外部仓库，不提供此驱动的 checkpoint/restore。

## 启动和配置

配置例子为 [node-jiuwenbox.json](../../build/config/examples/node-jiuwenbox.json)。
它是 `adxlet --config <path>` 接收的完整 JSON；也可以把字段放入部署 YAML
对应 adxlet service 的 `config`。统一部署的 runtime_profile 必须同步给 API Server。
`sandboxd_socket` 可以省略；保留它时，同一节点还可以提供原有 sandboxd runtime classes。
容量使用现有 Auto/File 源，同一宿主机容量只记一次。证书、地址、数据面允许的 CIDR
和目录须按实际部署填写。

Jiuwenbox 和 conchd 必须独立部署，Jiuwenbox 使用单 worker，且由该进程自己绑定 UDS，不能使用保留监听 socket 的进程热替换或 socket activation。Jiuwenbox 所在 Python 环境须安装匹配的
Conch SDK。该版本 SDK 固定连接 `/var/run/conch/conchd.sock`，不读取
`CONCH_SDK_CONFIG`；conchd 的 work_dir 及 ADX 的 conch_socket 必须对应这个 socket。
Jiuwenbox 的 idle_timeout 必须为 null。
ADX 通过现有 `/api/v1/timeout` 验证 idle reaper 已关闭，不修改其设置。
可选 `token_file` 包含 Jiuwenbox Bearer token；该 token 不写入所有权数据库。

节点上安装 mount/umount、iproute2、iptables-save/iptables-restore，使用 cgroup v2，
加载 br_netfilter 并配置 `net.bridge.bridge-nf-call-iptables=1`。adxlet 需要执行
只读 EROFS 挂载和节点防火墙操作的权限；其宿主挂载、网络和 PID/cgroup 视图须与
Jiuwenbox/conchd 一致。容器部署还需要正确的挂载传播，不能仅在 adxlet 私有命名空间
看到 bootstrap。Conch guest 必须包含现有 virtiofs volume 支持。

bootstrap.image 是独立 Execd EROFS，sha256 必须替换为安装产物的真实摘要，不能使用
示例中的零值。启动入口必须位于 bootstrap.target 下。ADX 检查内容摘要和 EROFS magic，
只读挂载后再次确认 loop backing image；再转换为 supervisor bind 或 Conch volume。
有未确认清理的实例时保留挂载；全部执行确定不存在后才卸载。

classes 的键只允许 supervisor/conch。每个 classes.images 显式绑定公开 Environment
image 名称；它不会自动拉取 OCI image。supervisor 的程序和依赖由宿主及 Jiuwenbox
原有 policy 提供，conch 的程序和依赖来自对应 template_name。Conch 模板名须由部署
约束为不可变版本。process 定义继承镜像启动时的 argv/cwd/user，ADX 将生成 Execd
既有 ProcessSpec 配置并独立只读挂载。业务镜像无需预装 Execd。入口配置通过只读 `/__adx_launch` 挂载；Conch 的 `/run`、`/proc`、`/sys`、`/dev` 及其子目录不接受 volume 目标。

class.policy 是可信节点配置，使用现有 append policy 模式合并到 Jiuwenbox 基础 policy。
ADX 接管 cgroup、网络、bootstrap、声明的工作目录挂载和执行身份。
Jiuwenbox 基础 policy 的网络默认应允许所需数据面和控制流量，bind 目标不得与 ADX 保留路径冲突；基础 policy 中保留的限制仍会生效。
示例 supervisor 以 root 运行 Execd，由 Execd 按 ProcessSpec 切换业务用户；实际配置
必须保证目标用户、UID/GID、目录权限及原有隔离策略相容。

Conch CPU limit 要求整 vCPU，memory limit 要求整 MiB，不能静默舍入。设备分配、
磁盘配额、只读业务 rootfs、extra_config、非 Local bind 以及 DNS/IPv6 网络策略会在创建前拒绝。
已有 IPv4 traffic 策略由 ADX 在宿主 iptables 中执行，包括协议、端口范围、优先级、
默认动作及控制端口例外；内置后端网络策略不承担这些细粒度规则。允许动作返回给
其余宿主链，不能绕过同一包另一方向的限制。策略替换使用单次 restore transaction。

## 生命周期和故障边界

state_dir 是节点专属持久化目录，不能使用 /tmp 或在后端重启时清空。
文件锁防止两个 adxlet 同时使用它，SQLite FULL/WAL 在外部操作前记录所有权和创建意图。
同一 runtime ID 的规格变化或删除后重用返回冲突。后端 sandbox ID 是完整 runtime ID
的固定摘要（adx- 加 28 位 SHA256 十六进制，总长 32，满足两种后端共同限制），不截断原始 ID；数据库同时检查其唯一性。

调用方 deadline 不取消已经发送的创建请求。驱动拥有独立任务，继续等待响应；
若成功晚到，只清理，不发布为成功、不再启动业务。删除等待同一执行的创建任务，
不以 Jiuwenbox 404 代替物理删除证明。传输结果丢失且同一后端代次返回 404 时，
状态保持待清理，节点关闭新准入；需要后端重新启动以终止旧请求，再完成对账。

Linux 通过 socket、SO_PEERCRED PID、进程 start time 和 boot ID 识别后端代次。
Jiuwenbox 重启后不热接管后台 job：supervisor 按原有 die-with-parent 行为退出，
ADX 确认 cgroup 为空；Conch 通过其原生 list/get/delete 完成存量清理后重建。
正常创建和执行仍经 Jiuwenbox。原生 Conch 请求只操作本地所有权记录中的 ID，
发现不在本地记录中的 ADX 前缀实例时拒绝准入，不删除未知所有者的实例。Conch 列表显式请求 5000 条；达到该接口上限时按可能截断处理，拒绝准入。

存活判断优先使用 Execd 自身控制状态并验证完整执行身份，健康检查断连不是退出。
驱动不采信 ConchRuntime 推断的 0/-1 退出码。Execd 不可达时，cgroup 非空或 Conch 原生 running 列表确认后端存活，继续由控制器 readiness 失败阈值处理；确定不存在才报告退出，后端也无法确认时返回观测失败。配置示例将 Execd 连续失败阈值设为 3。
原有 Execd readiness、业务探针及路由启用流程仍由 ADX 生命周期控制器负责。

启动对账确认非 failover Environment 的原 Running 执行不存在时，在物理清理成功后继续应用显式配置的
RestartPolicy；保持 Environment ID/assignment，使用新 runtime ID，沿用已消耗的尝试次数。
未配置重启、预算耗尽、存量无法确认或清理失败时不会据此创建新执行。
这是节点本地冷重建，不依赖快照；卷文件是否保留取决于原卷及宿主存储的持久性。

## 验证

`platform/adxlet/tests/jiuwenbox_runtime.rs` 使用真实 HTTP/UDS，替换特权 host 边界，
覆盖挂载请求、身份注入、重复创建、deadline 晚到、删除失败、后端重启清理、未知状态
和删除屏障。它不启动 bubblewrap/VM，也不证明 virtiofs、EROFS 或内核网络运行成功。

```sh
cargo test -p adxlet --test jiuwenbox_policy --test jiuwenbox_runtime --test jiuwenbox_network -j 2
cargo test -p adxlet -j 2
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
python3 build/docs/check.py
```

本次 Linux x86_64 验证中，完整 adxlet 测试 238 项通过，无跳过；其中包括
16 项 Jiuwenbox 运行时合同、5 项策略测试和混合后端删除屏障测试。
workspace all-targets/all-features Clippy、格式和文档检查通过。
生产驱动与真实 Jiuwenbox/Conch 的单机运行时集成验收 9/9 通过，
独立 EROFS、超时晚到和重启后清理的逐例/宿主审计均无残留。
版本、边界与制品摘要见 [Linux 验收记录](jiuwenbox-linux-20261016.md)，
复现入口见 [验收驱动](../../build/e2e/jiuwenbox/README.md)。

本驱动暂未上报后端实际资源用量和精确作业退出码；节点容量仍由 Auto/File 采样，
业务故障由 Execd 探针和现有恢复策略处理。

完整 ADX 单机部署与安装后公开 SDK 验收 11/11 通过，覆盖两个后端的创建、命令、文件、
删除、独立 EROFS，以及 ADXlet/Coordinator/API Server/Ingress/Jiuwenbox/conchd 进程恢复。
原 SDK handle 在重启后继续可用；可写卷文件在冷重建后保留。具体身份变化、存储配置
和证据见 [完整部署与 SDK 验收记录](jiuwenbox-sdk-20261016.md)。

Redis SIGKILL/35 秒中断恢复，以及独立 Linux VM 宿主强制 OS 重启后的冷重建、
卷文件保留和无孤儿审计均已通过，见 [宿主重启与 Redis 验收](jiuwenbox-recovery-20261019.md)。
CPU/内存实际约束、IPv4 双向及跨沙箱隔离、显式 WebSocket 矩阵、跨节点、断电/丢盘及
Redis 数据丢失/HA 仍须独立验证。
