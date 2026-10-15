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
