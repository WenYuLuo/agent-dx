# adxlet 生命周期与降级

adxlet 拥有实例串行状态机。Coordinator 提供归属校验、Redis 持久化和路由发布；暂停等节点操作由 API Server 缓存命中后直达节点；手动恢复与 Ingress 流量唤醒统一调用 Coordinator `EnsureRunning`。

## 运行策略

`EnvironmentSpec.lifecycle` 包含 `idle_timeout_seconds`、`idle_action`（默认 delete）、可选 `pause` 和可选 `restart`。HTTP 对应 `idleTimeoutSeconds`、`idleAction`、`pauseOptions`、`restartPolicy`；Python SDK 对应 `idle_timeout`、`IdleAction`、`PausePolicy`、`RestartPolicy`。自动和手动暂停后的鉴权业务流量均可唤醒，无需额外开关。

- 空闲超时为 0 时不回收。启用后，运行实例的 Execd 活跃客户端请求数与 Relay 流计数必须同时为 0，且活动版本在整个空闲窗口内未改变。后台命令仍计入 Execd 状态与活动版本，但客户端退出后不会仅因命令仍运行而阻止回收。采样失败或活动版本改变会重置窗口。adxlet 默认执行删除；`idle_action=pause` 在同一窗口结束后进入自动暂停，要求正数空闲超时和可用的 checkpoint 能力。
- RuntimeDriver 确认执行退出后，节点先退役路由、确认旧执行清理，再标记 Failed。启用重启时，按照指数退避和累计重试上限重新申请本机资源，并分配新的执行 ID，Environment ID 与归属代次保持。
- `restart_attempts` 和 `restart_pending` 随实例结果写入 Redis／降级日志。进程重启不重置次数。显式删除取消重启。清理未确认时保留资源，禁止启动替代执行。
- 可配置 `execd_health_failure_threshold`；省略时不以健康探测失败触发重启。连续失败达到阈值后，仍须先确认旧执行清理。健康检查只覆盖 Execd。
- 初次 Start 失败、超时或应答丢失均判定创建 Failed；不会触发运行中退出的自动重启策略。创建进入 Failed 即释放资源预留并提交结果；清理失败或超时不阻止释放。Failed 控制器继续清理残留及迟到执行，不转成 Running。节点重启按权威目录对账。

## 周期性权威对账

ADXlet 启动及每个 `report_interval_seconds` 周期统一调用公共
`node_reconciliation::reconcile_node`，覆盖所有配置的 RuntimeDriver；调度流程不按
sandboxd、supervisor 或 Conch 分支，也没有后端专用的周期对账开关。

每轮先关闭新分配准入，再获取完整 `InspectNode` 目录。首次启动尚未完成恢复时上报
`reconciling=true`；已完成恢复的有效会话做周期刷新时保持 `reconciling=false`、
`accepting_allocations=false`，保留已有路由。Coordinator 只允许当前会话在分配准入
关闭时检查目录。已有降级结果按既有契约补写后重新读取目录；随后校验归属、排空操作、
比对本地控制器和后端清单，并清理无有效归属的资源。成功后在本轮立即重新发布准入，不等待下次心跳。
耗时清理期间继续发送带递增序号且准入关闭的心跳，避免节点会话因对账耗时失效。

启动恢复调用完整恢复入口。周期对账对归属、已发布记录和后端执行状态均一致的实例只做
比对，不重新检查 readiness、不调用路由激活、不提交实例状态，也不重置空闲或重启计时。
本地缺少控制器或后端状态确实改变时才处理对应实例；已发布本地结果领先于目录时，本轮
返回 Aborted 并关闭新分配，保留已有生命周期和路由，等待下轮新目录，不触发注册重握手。
权威版本更新且已退休的执行仍须清理；保留终态控制器承接显式删除，不重复恢复或提交。
删除失败或 ready 上报失败后的重试仍沿用周期比对路径。

获取周期目录时仅关闭新分配，已有实例生命周期调用仍可继续。健康节点的完整目录与本机归属、
规格一致且只有 Running/Paused 时，例行核对非阻塞地申请本地写锁；已有操作仍持锁时返回
Aborted，保留既有生命周期和路由，等待下轮新目录，不排队阻塞其他实例的 pause/resume。
取得独占锁后再完成比对与清理；新分配在完整核对成功后恢复。启动恢复、归属变更、权威终态
以及已关闭生命周期的路径仍排空已接纳操作。Coordinator 重启要求重新握手时重新上报
reconciling，但不因此重做健康控制器的启动恢复。

周期对账不启动完整绑定替换；Relay 会话仍有效时同步检查没有绑定写入。只有 Relay
实际重启才从本地路由缓存恢复完整绑定，新恢复的控制器单独激活自己的路由。执行删除、归属
变化及目标变化继续通过路由版本撤销旧流量。

一次空扫描不证明未来不会有资源。响应丢失或超时的创建在扫描后才出现时，下一轮仍会
发现并删除；无需存在本地控制器。删除失败会阻止新分配，下轮重复权威对账并重试。
有效目录中的执行保留原身份与资源占用，不因周期扫描重新创建。

目录不可用不是空目录，不扫描并删除未知资源。已运行节点在收到新目录前遇到传输失败，
保留已有归属控制器的降级操作路径，同时阻止新 Environment/本地预留；首次启动、明确
的归属拒绝、已收到目录后的核对失败（目录前进或健康节点忙时的延后重试除外）以及 ready
发布失败时，生命周期保持关闭。延后重试不授权清理快照或释放运行中的引用。
周期调度与对账状态仅在内存中协调，不增加本地所有权数据库或日志。

组件测试 `node_reconciliation.rs`、`reconciliation.rs`、`sandboxd_rpc.rs` 和
`jiuwenbox_runtime.rs` 经真实 Coordinator gRPC/运行时 UDS 连接验证孤儿、晚到创建、
失败重试、有效执行无恢复副作用、旧目录防护、忙时并发准入、归属撤销排空、心跳、准入发布和已有降级语义；后端进程/VM 是可控夹具，
不是新的真实运行时或公开 SDK 验收结论。

## 休眠与按需恢复

`pauseOptions.retention` 为 `{"kind":"ttl","ttlSeconds":90000}`（默认）或 `{"kind":"forever"}`。
有限 TTL 从制品发布完成开始计算，与空闲窗口分别计时；永久恢复点的过期时间为空，
保留至显式删除或替换。`timeoutSeconds` 默认 300（1–3600），约束后端捕获 RPC，
不覆盖 OBS 上传等后续步骤的整体耗时。`storage` 为 configured（默认）、local 或 shared；
SDK 使用 `CheckpointStorage` 枚举。shared 要求节点已配置共享对象存储，发布失败不能返回暂停成功。

自动暂停先通过 Relay 的活动版本原子关闭新流准入，再通过 Execd 的活动版本关闭
客户端操作准入并复核需求。已接纳请求不会被当成空闲；捕获前的新需求取消本次暂停，
捕获开始后只排队等待，不取消不确定的 checkpoint，随后由同一恢复入口恢复。
后台命令仍沿用原先的空闲检测语义，可由后端冻结保存。

Ingress 先完成租户和数据面策略校验，再发出 `EnsureRunning`；被动订阅、health/metrics
不产生唤醒需求。Coordinator 在 Redis CAS 中记录恢复意图和固定 operation ID，并按
Environment 串行驱动；多个 Ingress、手动恢复和后台重试共用它。先尝试原节点的本机
资源准入；只有明确 NoCapacity 或旧执行已失效且有共享恢复点，才选择兼容节点。
RPC 超时、应答丢失和发布失败保留当前归属，避免启动第二份执行。全部节点拒绝时，
一个容量轮次排除已经拒绝的节点，30 秒后重试资源，不在节点间持续改写归属。

节点失效将 Paused 标记为归属失效并释放资源，仍保留 Paused；不进入 Running/Failed
对象的主动故障恢复。自动故障恢复过程中的中间 Paused/recovery.pending 并非用户休眠，目标再次失效仍续走原有故障恢复。首次需求再触发休眠对象跨节点恢复。Coordinator/adxlet 重启按 Redis
意图、generation 和节点进程会话对账，清理未提交执行后才能恢复。TTL 到期或显式
删除与恢复提交通过串行节点任务及 Redis 版本校验竞争；失联休眠对象由 Coordinator
退役，迟到结果不能复活它。原节点回来对账时，先排空旧控制器操作并清理执行，再根据
完整全局引用目录回收无人引用的 checkpoint；其他节点或待完成恢复仍在引用的制品必须保留。
仅本机目录不构成删除共享制品的依据。对象删除失败可在下次对账重试。

组件测试 `on_demand.rs` 使用真实 Redis、Coordinator/Node gRPC 和多个真实 Ingress；
执行内存与对象存储为可控夹具。Execd idle admission 测试启动生产二进制，用 FIFO
模拟 checkpoint handoff。它们不构成此次真实 Firecracker/OBS 部署验收。

## 资源与指标

`resource_source` 选择以下之一，也可保留既有 `capacity_file`。两者不能同时配置。

```json
{"kind":"sandboxd","socket":"/run/sandboxd/resource.sock","valid_for_seconds":30}
```

sandboxd 的 `/resource` 通过 Unix Socket 提供 CPU 核数、内存／存储字节数和整卡设备 ID；节点先验证 sandboxd 健康 RPC，再转换为调度器毫核单位。健康检查覆盖 sandboxd 内部资源刷新有效期。

```json
{"kind":"auto","disk_path":"/opt/adx/data","valid_for_seconds":30}
```

自动模式读取当前 Linux 进程的 CPU 可用并行度、cgroup v1/v2 层级限制、cpuset、内存上限与目标磁盘可用空间。它不虚构 GPU/NPU 清单；需要设备清单的部署选择 sandboxd 源或带设备信息的观测文件。

采集只更新容量，不重置已有资源预留。CPU、内存和磁盘容量均允许为 0，
也允许低于已有预留；成功读取的零容量样本立即替换旧容量并刷新有效期。
首次收到零容量样本也可注册节点和维持心跳。节点本地与 Coordinator 使用同一容量口径，
可分配量为 `max(capacity - reserved, 0)`，资源匹配自然拒绝无法满足的新请求。
容量下降不终止已有执行，也不修改已有预留；后续容量增加或业务退出释放预留后，
按新的可分配量恢复调度。Environment 请求的 CPU 和内存仍必须为正数。

首次无有效样本时等待；后续采集失败沿用旧样本至其原有效期结束，失败不续期，
过期后关闭新准入。负数、溢出和格式错误仍属于无效输入。资源观测新鲜和准入标志
分别表示样本有效期与节点是否允许资源匹配；CPU 或内存为零的新鲜样本仍可保持这两个
标志为真，但不满足正 CPU／内存请求。磁盘为零时仍可匹配不请求磁盘的实例。
`pressure` 提供内存和磁盘高／低阈值，任一达到高阈值关闭准入，两者都降到低阈值以下才恢复；压力采集失败也关闭准入。

配置 `metrics_listen` 后提供 `/metrics`。实例 CPU／内存经 RuntimeDriver Stats 定时采样，并携带采样年龄；资源预留和节点准入也作为指标导出。日志由部署环境收集。

## SQLite 降级契约

`degradation_journal` 为可选本地 SQLite 路径。

1. 正常操作完成后，adxlet 经 Coordinator 提交结果。没有待补写日志时不创建本地数据库。
2. 只有 Coordinator 不可用才尝试 SQLite 落盘，WAL 使用 FULL 同步。明确的权限、归属和版本拒绝不会降级。落盘失败不会报告 Journaled。
3. SQLite 保存有序、去重的待补写结果，不保存完整实例目录。暂停后恢复等连续操作必须按序补写，不能只保留最后状态。
4. 本地操作已完成不等于集群可见。内部 RPC 返回 Journaled；公共暂停／恢复 API 返回暂不可用并说明集群发布待完成。Ingress 仍使用缓存路由，由 Relay 复核本机绑定。
5. 当前节点进程在已对账的状态下可继续处理生命周期操作，心跳恢复后补写日志。补写成功后清理旧制品。
6. 节点进程重启后，先等 Coordinator 完整目录。日志记录仅在归属／请求规格匹配且版本更新时补写；已提交结果和退役归属被清除。补写后再次读取目录，再与真实 backend 对账，最后开放生命周期和新准入。
7. Coordinator 不可用时不能以 SQLite 缺记录为由清理或重建运行实例。SQLite 损坏也不能解释为空目录。

本地目录是降级可用性的条件；删除日志意味着丢失其中尚未进入 Redis 的结果。日志不是 Ingress 的发现或路由来源。

## 验证

Rust 用例位于 adxlet 的 `journal.rs`、`resources.rs`、`pause_resume.rs` 与 Coordinator 的 `storage.rs` 测试。真实 Redis 用例覆盖 Redis 进程中断、节点持久化组件重建、有序暂停／恢复补写与重启次数保护。

`build/e2e/firecracker/sdk_node_lifecycle.py` 在显式指定的 Linux 测试部署中通过公共 SDK 验证后端异常退出重启、Coordinator 失联空闲删除、SQLite 日志、节点进程重启等待、恢复补写和资源源断开。脚本中的 Redis／backend 查询是验收证据，生命周期正常操作仍使用公共 API。

后端首次启动使用独立的 `runtime_ready_timeout_seconds`（默认 120 秒）作为单轮就绪探测窗口。连接失败或一轮探测超时后，adxlet 按 `report_interval_seconds` 继续等待外部 sandboxd；等待期间不注册节点、不启动 RPC 服务，也不开放实例准入。配置错误仍立即退出。日常 `rpc_timeout_seconds` 只控制单次 RPC，避免后端资源初始化时间挤占日常故障降级预算。

## 本地验收（2026-09-16）

独立 Lima `adx-fc`，运行目录 `/var/lib/adx-pause-r7`，ARM64 原生 KVM + Firecracker，`package-v6`，源码包含未提交修改。继承同节点 checkpoint 5 项用例全部通过，新增 5 项真实运行时故障用例全部通过：

- 后端执行实例被删除后，按策略自动重启并使用新的执行身份。
- Coordinator 被暂停时，节点空闲删除写入 SQLite，Redis 保留旧状态，未冒充全局提交成功。
- 此时重启 adxlet，等待 Coordinator 对账，保留已有运行实例。
- Coordinator 恢复后，重放删除日志并恢复已有实例路由，SQLite 待同步记录归零。
- 资源采集 socket 暂时不可访问，超过有效期关闭准入；采集恢复后重新开放，已有实例继续运行。

同一轮最终目录中实例全部 Deleted，资源已释放，sandboxd 清单与 checkpoint 目录为空，服务正常停止。
证据位于 `out/ci/pause-resume/fc-r7/evidence/`，包括 `sdk/result.json`、`lifecycle/result.json`、`catalog-final.json`、`inventory-final.txt` 和 `result.json`；构建/部署完整日志位于 `out/ci/stage-2/{linux-2,package-2,fc-r7}.log`。回归日志为 `rust-5.log`（141 项测试，Clippy 通过）及 `clients-4.log`（Go 检查及 138 项 SDK 测试）。这些是本地证据，Kubernetes 验收尚未包含新增生命周期场景。
