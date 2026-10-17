# Agent FS（AFS） 测试与验收

Agent FS（AFS） 在目标仓中的验收按“目标仓源码、目标仓二进制、目标仓环境证据”登记。DMS/AFS 历史证据保留来源版本、环境和判据，可用于追溯和风险判断，但不自动升级为 Agent DX 当前候选结论。

## 平台与入口矩阵

Rust 构建、Cargo metadata、格式、Clippy、单元测试、FUSE 运行、mount、权限、runc、workspace bind 和性能结论都只在 Linux 上确认。macOS 可用于文档检查和 Python 静态工具核对，不能作为 Rust/文件系统验收通过证据。

| 入口 | 当前可执行平台 | 能形成的结论 |
| --- | --- | --- |
| 根构建／组件包 | Linux x86_64 与 Linux ARM64 | 对应架构的编译、清单和安装合同；ARM 默认不运行全部测试 |
| `acceptance/runner.py --list` 与工具单测 | 任意受支持 Python 环境（平台相关项可 skip） | 清单/调度/拒绝路径，不是文件系统运行通过 |
| `acceptance/runner.py` case 执行 | **专用 Linux ARM64 root 环境** | 仅本轮 lock、case、matrix 与 profile 的结果 |
| `scripts/ownerfs/bind-two-node-localfile.py`、`scripts/dfs/three_node_core.py` | 满足脚本 preflight 的 Linux root/FUSE 环境 | 单 VM 小规模定向功能证据，不等于通用 runner 或 Full |
| Full profile | Linux ARM64 runner 且完整 FROZEN lock；当前通用 ENV verifier 未完成 | 当前仍 BLOCKED，不得以 smoke/定向 driver 替代 |

通用 runner 的 ARM64 guard 是其现有探针、rootfs、LTP 和实验环境合同，不代表
AFS 产品二进制只支持 ARM64，也不代表 x86_64 包或定向 Linux driver 已通过 Full。
在逐项验证 runner 依赖前不得删除该 guard。

开跑前固定并记录：源码 commit、目标二进制 SHA、配置 SHA、测试套件版本、挂载点、容量门禁、数据规模、并发、缓存状态、持久屏障、计时边界、对照系统和适用副本语义。基础自检、工具就绪、功能通过、性能改善和最终达标分别登记。

维护中的验收工具在 [build/e2e/afs/acceptance/](../../build/e2e/afs/acceptance/)。过程日志、原始大证据、VM 镜像和历史归档放在源码树外；仓内只保留紧凑索引、命令、版本、校验和和必要夹具。

测试 runner 执行必须显式传入仓外本轮 `--lock`；`--list` 无需环境。仓内 `acceptance.lock.example.json` 不含个人 VM 状态和候选身份，复制后仍不能通过完整发布门禁。旧固定实验室判定器、原始观测及固定 3FS 编排已完整归档仓外；仓内使用参数化探针与合成负例。通用完整 ENV verifier 未实现，full 明确 BLOCKED；smoke 仍须执行对应真实 driver，工具单测不替代功能或性能验收。有效的 mmap 新鲜度探针保留为 `acceptance/probes/mmap_freshness.py`。

## 功能优先级

阶段二的当前顺序：

1. OwnerFs workspace bind ON 的必要功能闭环和试用交付；
2. OwnerFs 远端读写，以及 bind ON 与远端 FUSE 访问协同；
3. DFS 一写多读核心场景；
4. 普通 OwnerFs 本地 FUSE 访问及性能优化。

大规模、长时间、复杂可靠性、多 Meta、etcd、Redis 和完整故障矩阵继续后置。pjdfstest、固定 LTP filesystem 子集和短 FSx 用于兜底功能完备性；项目自定义 case 只补充 OwnerFs bind、远端访问、DFS 复制、Meta 恢复和权限语义。

## 性能目标

这些目标属于后续产品演进，不是当前候选的既有能力：

| 范围 | 目标 |
| --- | --- |
| OwnerFs workspace bind | 选定核心 case 接近 native ext4，吞吐按既定 `>=0.90x` native ext4 判据验收 |
| 普通 OwnerFs 本地读写 | 吞吐 `>=1.2x` 同条件 MooseFS，独立操作时延 `<=0.8x` 同条件 MooseFS；两项都满足才算达标 |
| 普通 OwnerFs 远端读写 | 吞吐 `>=1.2x` 同条件 MooseFS，独立操作时延 `<=0.8x` 同条件 MooseFS；两项都满足才算达标 |
| DFS | 同接口、三份同步持久副本条件下持平 3FS |
| 删除 | 正确性必须通过，并保留性能对照报告；不新增硬比例 |

时延必须先固定操作范围、计时边界和判定分位数，记录 p50/p95/p99。不得成绩出来后选择有利指标，也不得由吞吐反推时延。不具备同条件对照的结果只能作为摸底。

## 当前候选版本工程验收

每个准备交付的 AFS 候选版本至少核对：

1. 默认 OFF 的构建、测试和包清单验证；
2. 显式 ON 的 AFS crates 构建、严格 all-features Clippy、受影响单测和真实带组件包验证；
3. 从本轮 ON artifact 完成安装、配置、启动、健康检查、正常停止和卸载；
4. 用目标仓实际二进制完成 OwnerFs bind ON＋远端双向访问、权限/清位/errno、中心 local-file 正常重启恢复，以及小规模 DFS 一写多读；
5. 验证 direct-I/O mmap 协商及正常卸载/受管引用排空；测试辅助程序只由验收流程显式构建，不进入普通包。

单个出口未完成时如实登记待验收或阻塞。性能、完整 POSIX 和复杂可靠性继续后置；
工具检查、文档同步和旧候选结果不等于当前候选的功能或性能通过。带日期的历史结果
见 [迁移报告](../migration/2026-10-09-afs-snapshot.md)。

## DFS 外部 S3 闭环

外部后端的基本流程为 A 写入 → 同步确认 S3 对象和 Meta 持久提交 → rename 发布并
同步父目录 → B 独立打开读取 → 重复读取。先使用64KiB／64MiB数据验证必要功能，
再按任务范围扩大；内容、功能、测量完成及性能达标分别登记。
SeaweedFS、PostgreSQL、JuiceFS 的部署、凭据和环境编排留在产品仓外。

### 发布、回源与恢复

| 边界 | 必要覆盖 |
| --- | --- |
| S3签名策略 | 省略配置默认signed；显式unsigned仍有SigV4 Authorization，普通PUT路径／Content-Length／完整正文不变；HTTP错误不能生成成功receipt；完整BLAKE3和fsync屏障保持 |
| 持久发布 | PUT 失败不发布新 head／长度；确认成功后另一节点读取完整内容；外部模式不新增 Node 耐久副本 |
| 窗口与重放 | 真实4MiB满块跨 write 预上传，所有命令共享四路上限且完成一路立即补位；乱序回执不串窗口，部分失败保留身份与成功回执，Meta 未知 ACK 精确重放并阻挡后续 mutation |
| 同步与修改 | Full／DataOnly、同步写、可写 flush、覆盖／稀疏写／截断和正常退出保持；成功 fsync 等待入口之前已接纳的整个 inode 序号（包括其它 FD）的所有窗口对象回执和 Meta 持久确认。外部本机 dirty read 先完成同一有序发布屏障，再经已发布读取；发布失败报错且不交付旧或部分输出。无 dirty 不增加 Meta 写事务，非外部 DFS 保留原 overlay |
| 跨窗口与预算 | 旧 PUT 进行时普通顺序 write 可接纳下一窗口；旧 ACK 不丢 active 内容／killpriv；readonly fsync 可同步同inode本地writer，readonly flush不代提交；未知 ACK 冻结重放；flush／drain／O_SYNC 不只提交首窗。成功 PUT 可退休正文，但最后 partial read／HTTP／retry pin 释放才退账，提交描述跨未知 Meta ACK 保留；Uploaded 仍计逻辑64MiB窗口而不是空洞。pending 分配前准入、publisher 转换拒绝不吸收未接纳正文、全部释放后归零 |
| 读正确性 | 完整 SHA 一致、完整长度／摘要拒绝短长响应及内容损坏、错误不能发布部分输出；cache 命中不能替代权限、当前版本和期限检查 |
| 已发布数据恢复 | A 正常退出后 B 清空客户端内容缓存并从 S3 回源；local-file Meta 正常全停重启后重新打开读取 |
| 满窗准入 | 正文已退休且前窗尚 Preparing 时，满 active 窗口不得以1字节继续接纳；推进原提交链后再冻结。连续33个4MiB块应保持完整分块，每次 publication delta 不超过64MiB；无进展或失败保留错误与已接纳序号 |
| 写会话恢复边界 | 新会话立即复写与退出后读取分开验收；旧写租约有效时可能返回 EBUSY，不能通过修改租约或只测读取宣布恢复写权通过 |

正常重启恢复不等于物理崩溃恢复、HA 或复杂可靠性。cache OFF 与 ON 分别绑定候选，
历史 debug／release 或不同后端的功能通过不自动继承。当前支持与剩余缺口见
[演进计划](../development/afs-plan.md#支持范围)。

### 缓存、授权与资源生命周期

| 边界 | 必要覆盖 |
| --- | --- |
| 缓存安全 | 队列／pending／磁盘额度、私有文件检查、损坏回源、填充失败、store/generation 隔离、重开与正常排空；资源不足跳过填充，S3／Meta 错误仍传播 |
| 不可变载荷 | GET 验证后、Pending、后台填充按完整 chunk 身份共享同一分配；错误长度／摘要／身份不能准入或修改输出；写侧私有 StagedChunk 经 transport／retry 交接不重复整块复制或校验 |
| 字节预算 | 分配前准入，dirty／stage／transport／retry 的租约持有到最后 owner；cache 按实际 Vec 容量去重计费，pending／worker／单驻留槽及读者仍持有的旧 pin 均不得提前退账 |
| 驻留语义 | Node 共享最多4MiB已验证对象，预算不足回退；每次 open 在线观察最新 head；每 callback 仍检查有界授权、身份、范围和期限。RAM 命中允许延后发现磁盘副本变化，新磁盘加载必须再检查权限和完整摘要 |
| 有界版本授权 | 同一 Meta read view 核验版本归属、live session、对象／范围，按需建立最长5秒上下文；单调截止从 RPC 发出计时，命中／心跳不续期，超过期限不可交付输出 |
| 期限与生命周期 | 有效范围复用、过期在线核验、断链有效／到期、Meta 持久重启后新准入；已知失效与输出串行化，停止后不得通过心跳复活；128项／1MiB保守预留含在途引用 |
| 拒绝与回退 | 错误 inode／namespace／layout、MAC、epoch、过期及越界拒绝；新版本接口的权限拒绝／timeout／其他错误不回退或同次重签；保留旧接口原拒绝语义、不 GET、不修改输出 |
| 原路径隔离 | 空洞／dirty 写状态、P2P 保留原路径；一致版本视图的 EOF 不要求逐读 head RPC；外部 S3 实验不改变 OwnerFs bind 语义 |
| 校验线程 | 官方串行／并行完整摘要一致，请求范围外损坏仍拒绝；GET／Ready 共用私有两线程池、单对象 gate；停止后拒绝新校验、实际 worker panic 传播并显式 join，失败不被退出成功掩盖 |
| Ready对象内流水线 | 固定1MiB段边界、空对象／短尾完整BLAKE3一致，保留原allocation；受控验证三处相邻段read／hash重叠，并核对真实trace；中间／尾段损坏、短读／超长、后段读取和worker错误不生成已验证载荷，报错前等待已接纳工作；raw分配前预留、与pending／pin共用原预算并安全转移；恢复不递归取锁，预算不足不删文件或绕道GET；退出等待已接纳任务 |
| 独占Ready复用／owned结果 | 只回收唯一载荷，reader／fill pin阻止回收；原capacity跨raw／Verified交接连续计费，短尾、较大对象回退和失败退账；损坏／短读／超长不能交付；owned结果覆盖多range／空洞／短尾与预算拒绝，最终过期／停止／失效检查与借用路径共用，借用输出失败不变；FUSE直接使用owned回复，其他后端默认适配保留行为 |

入口满时应真实等待；timeout、ticket 放弃及 Drop 不留下独立客户端任务。HTTP
连接复用不能替代并发／字节准入。载荷预算、cache 内容额度和进程 RSS 是不同指标，
不能用事后 RSS 或四路并发证明字节上限。旧 generation、索引及前台临时读取缓冲
须另计环境预算，不能把 cache 额度当成整个目录或文件系统配额。

Linux 受影响回归可从现有模块入口执行；按改动选择过滤项，不为文档变化重跑：

```sh
cargo test --locked -p afs --lib --no-default-features --features dfs node::external_chunk::tests::
cargo test --locked -p afs --lib --no-default-features --features dfs node::dfs_read::tests::
cargo test --locked -p afs --lib --no-default-features --features dfs external_version
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
```

Node 复制、Meta 条件事务、精确重放及 local-file 恢复的已有回归按受影响范围复用；
上面的两个模块命令不代表这些其他模块、完整 POSIX 或 Full 已通过。

### 性能与实验边界

外部存储对接的功能验收不包含性能达标承诺。性能测试与产品功能分别登记；
实验部署、比较环境、原始数据和临时探针保存在产品源码树外。

### 限定白盒诊断

DFS 外部后端路径复用已有 OpenTelemetry tracing。普通运行保持 tracing 默认关闭；
诊断可显式配置 `trace_enabled=true`、OTLP gRPC `trace_endpoint` 和
`trace_sample_ratio=1.0`。采集前固定输出预算，检查导出失败／丢弃计数，正常停止
AFS 并排空导出后再停止 collector。全采样结果用于归因，不能直接替代关闭 tracing
时的正式性能成绩。

使用管理员已有的兼容 OTLP gRPC collector；产品不提供或部署实验接收器。

观测范围包括 FUSE 排队和请求大小、dirty 预算触发的准备／上传／发布、S3 worker
排队、读端授权／缓存分支／整块装载／摘要校验／分段复制，以及 Meta 合并提交的
fork／clone／sort／编码字节／etcd CAS。跨线程和 RPC 保留 trace 关联，合并提交通过
links 关联多个请求；不能把多请求共有批次、嵌套 span 或并行 PUT 时间重复相加。
处理字节是逻辑读取／哈希／复制量，不代表物理磁盘 I/O；不同 VM 的时间戳不能直接
相减来计算单向网络时延。

需要细分载荷交接时，使用 `node.fuse.read/write.execute` 与对应的
`allocate/input_copy/reply`，区分请求执行、分配／复制和内核回复。
写侧 `node.dfs.chunk.stage` 下分别记录 `vec_to_arc` 与完整 `hash`；
Ready 装载分别记录私有文件检查、预算／分配、verifier gate 和同对象分段读／校验。
`pipeline_scope` 包含并行读／hash及scope等待，不能当作纯等待或CPU时间。
span引用可能被异步后代持有，尤其写请求的外层关联span不等于callback返回时延；
按短寿命执行／回复边界和应用syscall计时核对，不直接累加父子或重叠区间。


## DFS 外部缓存扫描与回源观测

缓存策略的必要回归包括：任务／pending拒绝时旧Ready文件保持、可选resident
释放与caller pin继续计费、两候选取较旧者、唯一／无候选及Pending排除、删除
失败不扣容量且可重试。空body分片后仍须等待真实EOF，短读、超长、流错误和
BLAKE3错误不得产生成功载荷；并发观测快照不能倒退累计计数。以上受影响检查
在Linux运行，完整POSIX、复杂故障或正式性能须单独验收。

正常性能轮通过`/metrics`采集以下固定标签指标，无需开启全量OTLP：

| 指标 | 口径 |
| --- | --- |
| `afs_dfs_s3_get_stage_seconds{phase}` | queue、headers_sdk_aggregate、body_first_poll_wait、body_poll_wait_total、body_poll_wait_max、body_copy、body_final_eof_wait；first／max是body等待的子集，不能再次相加 |
| `afs_dfs_external_verify_seconds{source}` | get包含gate＋完整hash；ready包含gate＋加载＋完整hash，均为wall时间 |
| `afs_dfs_external_verify_bytes_total{source}` | 成功完整校验字节，source为get／ready；失败不计为成功校验 |
| `afs_dfs_s3_request_reply_seconds{operation}` | 调用者等待worker回复，包含嵌套工作，不与GET阶段再次相加 |
| `afs_dfs_s3_cache_admit_seconds{result}` | attempted是尝试准入而非保证接受，另有no_cache |
| `afs_dfs_s3_cache_activity_total{event}` | admissions、evictions、task_rejects、pending_budget_rejects、capacity_rejects；capacity包括零长度或单对象超过配置限额，pending为聚合内存不足 |
| `afs_dfs_s3_get_body_bytes_total{result}` | 成功GET完整收流字节，不是应用全部读取字节 |
| `afs_dfs_s3_slow_gets_total{result}` | 所有达到50ms慢GET阈值的成功请求数；日志每60秒最多16条，因此条数可能更少 |
| `afs_dfs_s3_get_failures_total{stage,result}` | headers／body／metadata／verify错误及read timeout；计数只证明失败阶段，不提供完整失败耗时 |

原生Peer读另外提供`afs_dfs_read_scan_bytes_total`、`afs_dfs_read_scans_total`和
`afs_dfs_read_copy_bytes_total{stage}`。scan记录实际主读验证循环的逻辑文件读取／BLAKE3输入，
包含失败前已扫描字节，不等于物理磁盘IO；每次scan汇总一次计数。copy固定六个阶段：
scan_to_range、pinned_range_to_output、range_to_frame、frame_to_attempt、attempt_to_staging、
staging_to_output；只统计这些自有载荷交接，不包含codec、TLS或内核复制，也不是内存峰值。
计数不能换算为E2E时延或与嵌套阶段累计耗时直接相加。
原生单连续范围采用A单次完整扫描和B私有最终结果交付；复杂布局保留原回退路径。
完整BLAKE3、授权和错误传播保持。原生DFS新open仍取得fresh inode；已打开句柄
复用已提交读视图最多5秒，截止从fresh请求前开始并受原Node/session期限约束，
命中和心跳不延长本次视图。到期重新取得head，本机发布、truncate、getattr等已有
刷新点使旧视图失效；不承诺旧句柄每次read立即看到其他客户端修改，也不是永久快照。
read开始和最终交付均检查session/epoch、停止及原截止，borrowed输出在成功前不修改。
普通Meta断链不会伪造撤销，但新open、head刷新、source miss或授权刷新需要Meta时
失败直接报错。read/grant拒绝清除受影响上下文，Node注册拒绝、epoch变化或停止才
停止共享授权。source缓存500ms、A授权缓存最多5秒及原grant期限不延长。

A端Peer服务仅复用当前被请求的
已验证不可变对象，不预读下一个对象；B的可选纯磁盘缓存单独验收。对象首次完整BLAKE3通过后
才可入单槽；命中仍核对catalog、Durable、device/epoch、record revision及当前物理文件的
dev/ino/size/mtime/ctime，变化则失效重验。命中交付的是已验证的内存字节，不能声称每次
命中都重新检测磁盘后来发生的静默损坏；缓存不是读授权，逐range授权与范围checksum保留。

A端每个LocalChunkStore共用8MiB Peer读载荷额度，当前对象槽最多4MiB。
填充、驻留、淘汰后仍被发送流持有的对象、范围回退正文和RDMA packed正文，均按实际
capacity计费至最后一个载荷所有者释放；额度不足返回结构化容量错误，不暗增内存。
原8个read slots、每流4个64KiB待发送帧及扫描scratch仍各受原上限约束，需单独计入
进程资源预算；8MiB不是总RSS，也不是每个请求额外获得8MiB。大于4MiB对象保留范围
读取，协议允许的最大请求不代表当前共享额度一定能准入。fsync与持久提交路径不变。

`afs_dfs_peer_read_payload_bytes`和`afs_dfs_peer_read_payload_peak_bytes`分别报告当前
计费载荷及历史峰值；`afs_dfs_peer_read_payload_reserved_bytes_total`／
`afs_dfs_peer_read_payload_released_bytes_total`记录准入和最终释放容量。
`afs_dfs_peer_read_object_cache_total{result="hit|miss"}`仅记录固定标签的槽查询结果。
这些指标不授予权限、不执行准入；槽中4MiB驻留并非泄漏，取消／排空及最终引用退账
须用针对性回归分别确认，不能仅看RSS推断。

原生gRPC正文帧使用Bytes持有原PeerReadPayload：保留Data frame后取消流、淘汰
缓存槽，必须继续计费直到最后一个frame／对象引用释放；范围fallback遵守同一规则。
授权与帧生产共用一个blocking任务，授权拒绝仍在返回stream之前发生；取消发生在
不可中断授权期间，permit必须保持到任务真实退出，已观察取消后不再读取正文。
回归应同时覆盖slice地址共享、最后引用退账、预算耗尽、损坏无Header、wire兼容、
遗漏Completion及错误checksum拒绝。帧仍为64KiB、队列仍为4，完整校验不省略。

`afs_dfs_peer_read_events_total{event}`按固定标签记录blocking_authorize、
blocking_produce、blocking_combined任务提交，以及data_frame、body_vec_allocation、
shared_frame正文事件。旧独立阶段标签在合并路径上为0；任务提交不代表已经完成，
帧事件不代表消费方成功交付。与copy／scan／RPC计数同窗口核对，不从计数换算耗时，
也不能以正文帧共享宣称codec、TLS或内核复制消失。

`afs_dfs_read_rpc_calls_total{method}`记录客户端get_inode、get_file_version、
get_chunk_sources、validate_read_grants和peer_read_ranges调用尝试，包含失败。
按Node和操作窗口取增量；get_inode还包含open/getattr等调用，不能把其总数当作
read-head次数，也不能把客户端尝试当作服务端成功次数。指标不含所有DFS控制RPC。

64MiB验证保留全缓存重复读；数据规模超过既定磁盘缓存时，第二遍称repeat，
不能继续称全暖缓存。双方预读关闭，固定接口、数据、并发、缓存、持久屏障及
p95判据，分时运行；实际GET次数／字节、整体时延、调用p50／p95／p99和阶段
指标分别记录。不通过挑种子、增加缓存或单边trace扣减来宣称性能收益。

缓存淘汰回归须覆盖同秒／跨秒两候选选择、命中刷新、首次Ready及重启索引初始化、
128对象／64容量扫描、预算与pin、删除失败回滚。新策略只使用1秒单调访问桶，
不改变完整校验、最终授权或硬准入额度。独立ON诊断沿用lookup／admit／fill spans；
新增桶与选择原因仅记录每cache前1024次成功淘汰，复用原计数并输出截断数，
不分配事件缓冲。fill的written_bytes只统计成功完整写入，outcome区分失败，
不能把写入成功等同持久保证或将接纳当Ready完成。诊断不替代OFF完整E2E对照。

## 原生 Peer 磁盘读缓存验收

开启 `dfs_peer_cache` 后需分别验证：miss 回填→关闭重开命中、覆盖写后新open读取新版本、
截断与partial grant不扩大读取范围、授权到期／已知session拒绝、损坏与短文件安全回源、
容量淘汰与重启恢复、unsafe目录拒绝、正常停止不再接纳fill。缓存不能增加持久副本数，
必须核对B的durable chunk目录为空、Meta仍只有A的单持久副本。失败不改公开输出。

功能验证之后，固定数据／并发／缓存容量／预读0／持久屏障及冷读到重复读的间隔，再做
分时对照；若同guest两读之间不采样，Peer与工作量只报告两读合计，不虚构逐阶段计数。
独立功能测量可证明重复读Peer正文为0。完整对齐1MiB命中需核对实际数据读取attempt与显式copy；直读应无scratch→输出复制，partial／跨段／尾段保留强校验及保守fallback，短读／EINTR的重试单独计真实调用，不由逻辑段数冒充系统调用数。直读后hash／身份／授权失败仍不能公开私有结果。首次获取和重启恢复必须完整验证可信Meta根；稳定磁盘命中逐完整1MiB分段BLAKE3验证，不跳过当前请求覆盖段。另验crosssegment／尾段、段内损坏、身份变化后的完整重验、metadata未变的段外损坏延后、独立1MiB元信息额度／淘汰／停止退账。完整接收证明必须核对Meta根及全部帧身份，并在Completion与实际EOF后才可交接；late Status／额外帧不得产生证明。
`afs_dfs_peer_disk_cache_events_total`按固定结果标签记录命中与填充；
`afs_dfs_read_hash_bytes_total{stage}`按实际BLAKE3执行记录本地根、范围stream、接收根、fallback根、分段摘要生成及磁盘段／全文校验字节；根和分段各执行一次即各计一次，不能把新建摘要当免费。旧`afs_dfs_peer_disk_cache_hash_bytes_total`已由按实际执行计数的新指标替代；历史数据保持原指标口径。失败／部分扫描不代表完整验证成功。`afs_dfs_peer_disk_cache_metadata_bytes`与`afs_dfs_peer_disk_cache_metadata_peak_bytes`观察独立元信息额度，不能冒充正文驻留或Node RSS。 `afs_dfs_peer_disk_cache_data_read_calls_total`记录实际正文区间read尝试（含失败／中断，不含header及尾部EOF探测）；`afs_dfs_read_copy_bytes_total{stage="cache_to_output"}`记录实际cache私有结果复制，直读为0，失败时已发生的工作也计入。`afs_dfs_peer_disk_cache_stage_nanoseconds_total{stage}`与`afs_dfs_peer_disk_cache_stage_observations_total{stage}`按operation聚合准备、直读、段hash、fallback流式读hash及结束检查；不逐64KiB取时钟。固定低频计数／计时属于产品观测，trace导出OFF运行仍包含其开销，不称零扰动、不换算或扣减E2E，也不伪拆两读合计为逐阶段。fill当前／峰值和private峰值指标仅观察真实生命周期，不执行准入；
B的临时fill与A的8MiB共享载荷额度分别报告，不当成整个Node RSS或全局B读输出池。
应用阶段之和须明确排除等待和控制采样，不冒称连续单时钟E2E。历史通过不自动继承。

填充链另有固定低频`afs_dfs_peer_disk_cache_fill_stage_nanoseconds_total{stage}`／`afs_dfs_peer_disk_cache_fill_stage_observations_total{stage}`，仅实际进入完整对象fill后报告lock_wait、lookup、prepare、receive_verify、store、deliver和total。lock_wait覆盖cache.operation准入及首次root检查，不是纯锁争用；lookup不包含此前Meta sources RPC。receive_verify包含完整Peer调用、解码／复制、根及段校验、Completion与真实EOF，不是纯网络时间。旧完整缓冲路径store为store_verified完整调用；流式路径receive_verify含接收／校验与私有写盘，store只覆盖最后Ready提交，deliver只覆盖其后最终交付检查。流式写盘与接收重叠，不将其累计子耗时重复加入父阶段，也不直接对旧store毫秒承诺节省；prepare可由多个互斥片段组成，观察次数不等于fill次数。total是父阶段，不与子阶段相加，也不是完整FUSE回调计时。失败已发生的阶段也计入，不改变错误／重试／缓存准入。

`afs_dfs_peer_disk_cache_fill_stage_thread_cpu_nanoseconds_total{stage}`及`afs_dfs_peer_disk_cache_fill_stage_thread_cpu_observations_total{stage}`只统计同步调用线程的有效CPU样本，排除远端、producer根校验和h2／TLS等后台线程；跨线程或clock失败缺少CPU观察，不冒充零CPU。墙钟覆盖CPU采样区间，但wall−threadCPU不能直接命名为网络／磁盘等待。每阶段低频聚合，无逐64KiB取clock；正式测量包含实际观测开销，不承诺零扰动。诊断若采用cold后单独采样，其repeat间隔与固定1秒正式三轮不同，重复读只能核对正确性／0Peer等机制，不继承正式性能口径。

同对象流式填充需针对性验证：实际EOF／late Status／坏根不产生Ready；文件完成凭证不能跨FD／对象或复用被修改的私有输出；缓存创建、配额和写盘失败仍以同次完整校验RPC交付合法区间；最终授权失效不能提交Ready或公开输出；满队列取消／超时必须真实join后退账；current-thread、过大窗口或小批预算保留原路径。复读仍只用磁盘cache，不引入驻留正文RAM。固定块另需验证超过两块后的真实复用、跨1MiB段／不足256KiB尾块、Header／Completion／额外帧错误；空闲块等待／归还及满队列取消受绝对期限约束，错误attempt不混入旧尾块或摘要。
根校验归producer的双块路径还须验证：consumer不重复计算根，producer按块恰扫描完整正文一次；尾块／control前flush顺序及根／段摘要均正确。consumer的Completion只登记待确认状态，实际EOF／成功join后的内部回执必须匹配可信Meta根、Completion摘要／长度以及私有sink／输出绑定，再允许完成凭证。错误长度／摘要／绑定、自洽正文但错误Meta根、Completion后late Status、取消／超时均不得提前放行；legacy／非流水路径保持原完整校验。

`afs_dfs_peer_disk_cache_stream_reserved_bytes`／`stream_reserved_peak_bytes`记录原4MiB额度的保守流式reservation；`stream_private_peak_bytes`另含既有私有输出，不代表实际分配或Node RSS。`afs_dfs_peer_disk_cache_stream_frame_bytes`／`stream_frame_peak_bytes`仅记录两个固定载荷块的真实capacity，归还空闲池仍占账，最后owner销毁才释放；控制字段另有独立有界余量，不含transport buffer。历史规范化帧版本保留原观测范围，不以新旧峰值直接推断RSS变化。旧`fill_bytes`系列只观察旧完整Vec路径，两者互斥，不能相加当同一填充峰值。wire帧上限、两个固定块、单cache操作、窗口／解码约束才执行准入，指标不授予额度。`peer_frame_to_block`统计当前帧到块的实际复制，`peer_frame_detach`保留历史／规范化路径计数，`fill_to_output`统计请求区间提取；必须按实测工作量判断是否减少复制，不能因没有完整Vec就宣称零复制。

64MiB本地对照可在预授权控制连接建立后固定等待30秒，由A单调时钟从create前覆盖发布及B首次完整读校验ACK。完整双读总时延若含固定1秒复读等待必须明确标注；阶段之和仅作辅助。测试专用控制适配器在本地测试方案维护，不进入产品包。

`afs_dfs_peer_read_events_total`的`stream_frame`记录接收到的wire帧（含控制帧）；`stream_block_alloc`记录固定块实际分配，`stream_block_handoff`记录成功正文块交接，`stream_block_write`记录consumer对正文块进入私有sink的调用，不等于物理write系统调用或缓存落盘成功。64MiB完整4MiB对象对照应分别核对1024个Data帧、256次正文块交接和写入入口；尾块按实际长度计数，不从事件数推算性能。

## 原生写 buffer 验证

以下是受影响定向检查入口；通过数与候选身份以各轮实际 Linux 记录为准，不替代完整 POSIX、
可靠性、正式安装包或性能验收。

```sh
cargo test --locked -p afs --lib write_buffer_tests
cargo test --locked -p afs --lib native_builder_
cargo test --locked -p afs --lib dispatch_tests
cargo test --locked -p afs --test config_contract
cargo test --locked -p afs --bin afs-node allocator::tests
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
```

真实 FUSE 使用本地 owner、NodeReplicated R1、无 S3：六次应用1MiB写入合计6MiB，记录内核实际
回调大小和中途提交轨迹；同批原提交入口冻结4MiB＋2MiB有效正文，复制入口合计6MiB、地址保持，
远端重新打开核对 SHA／长度／EOF。随后偏移8192覆盖4KiB，验证小 Chunk、新布局和旧 Chunk 不变；
另验证未冻结时同位置原地覆盖。若后台中途提交，功能结果与“同批两正文”结构验收必须分开登记。

`afs_dfs_write_buffer_*` 记录实际 live/idle/peak capacity、分配、复用、释放和输入 copy；
`input_copy_bytes_total`／`input_copies_total` 来自实际 slice-copy/extend 入口，不能当端到端总复制。
诊断轮设 `log_level="debug"`，固定 `fuse_write_admission` 和 `dfs.write_buffer.*` 事件关联真实回调、
buffer offset、世代、冻结前后指针与最后引用；不记录正文。hash、网络、SDK、读快照和 profiler 抽样
分别记账，诊断时延不作正常性能收益。常规回收轮保持日志低等级、trace OFF。

`afs_node_allocator_*` 将 jemalloc 的 allocated/active/resident/retained、后台开关、decay 和观测有效位
分别公开；它们不等于 AFS live/idle 或 Linux RSS。峰值测试测前固定池基线、实际触页 live 峰值、
RSS允许偏差和观测窗口，完成原提交入口、释放所有引用后核对 live下降、idle≤基线、超额释放和RSS回落；
重复负载不得逐轮累积。慢副本、读取引用和未知提交必须继续持有正文，不能为回落提前退账。

必须补充不对齐／跨块／稀疏／截断扩展／并发覆盖、合法短写与同步标志、prepare失败恢复和
未知 Meta ACK 精确重试、停止排空，以及受影响 OwnerFs／RN／RDMA／S3 回归。完整BLAKE3、授权、
成功 fsync 和 close-to-open 不因这些优化放松；RDMA仍按现有独立注册 endpoint 缓冲合同验证。

## 原生读粗粒度诊断

原生读粗诊断由默认OFF的`dfs_read_diagnostics_enabled`显式开启，使用固定有界对象聚合。根／段双块诊断分别记录producer根校验和consumer的写入／提取／段校验，成功join后计算两组有序区间的真实交集；空、时钟失败或固定区间容量溢出不能记成有效零重叠。它不保存逐帧事件或正文，不增加载荷额度。ON时延不作OFF成绩，聚合hash／交集不从完整E2E扣减；同步线程CPU降低也不等于全Node CPU降低。


通过现有Node管理HTTP的 `GET /v1/diagnostics/dfs-read-timing` 在测量窗口外读取固定快照。
先检查schema、容量、overflow、clock_failures及每条success/completed；该进程前256条之后不轮转，
因此长时测量记录不完整时只能登记缺口。正文收齐只表示最后预期Data帧已解码，不能当作真实EOF、
校验完成或授权交付。Meta、FUSE及对象父阶段按实际嵌套与重叠核对，不能直接相加；
consumer线程CPU不含producer或远端CPU。快照不保留正文／路径／授权信息，也不是业务授权来源。
开启配置及监听范围见[部署说明](../deployment/afs.md#原生读粗粒度诊断)。
