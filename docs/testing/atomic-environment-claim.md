# 创建确认、对象持久化与可选本地 Claim

默认 `create_mode: "central"`。所有组件使用同一版本的 Environment RPC；公开 Request ID 随创建、转发和可选 Claim 传递。

## 中心创建

Coordinator 在内存中排队、选择节点和预留资源。adxlet 完成本机准入、sandboxd 启动、Execd 就绪和路由绑定后，Coordinator 才用一次 Redis 原子事务发布初次 Running 结果和请求回执。中心路径不预写 Pending 或 Assignment。快照恢复引用、后续生命周期和节点故障处理有各自的持久化步骤，不计入普通无快照创建的一次成功发布。

同一进程按租户/Request ID、Environment ID 串行协调创建，等待锁也受原始创建期限约束。Redis 和节点 RPC 不占用普通创建的调度锁；快照与异常恢复仍需要单独协调。未确认请求到期后释放内存预留；重试和清理与成功发布共用该 Environment 的互斥锁，成功发布后先同步内存目录及资源账本，再返回确认。已经确认的结果保留；写入应答丢失后通过原身份查询和重放复用。

对尚未成功确认的创建，连接失败、节点恢复中或旧 session 拒绝允许重新选点。候选节点必须完成恢复对账、心跳有效并开放准入；换一个 session 字符串不构成就绪。每次尝试保留原绝对 deadline。运行中的已确认实例继续遵循生命周期和恢复协议。

Coordinator 重启可以丢失未确认调度；新的尝试使用新的 generation。旧节点上的未确认执行由后续权威对账回收。允许暂时存在多个未确认执行，最终只有已确认结果进入有效目录。

## 两种成功身份

在同一租户内，已有成功结果命中 Environment ID **或** Request ID，就复用已有结果及其规格，不允许迟到候选覆盖。不同 Request ID 命中同一 Environment 时，为新 Request ID 登记相同结果的回执。不同 API Server 和重启后的查询使用 Redis 回执收敛；API Server 的 600 秒响应缓存只是加速层。

Environment 与 Request ID 回执在同一次事务中写入。节点 process session 和 Coordinator epoch 同时校验；同对象的读改写比较该对象原始字节。删除移除主目录，已完成 Request ID 仍绑定原 Environment 生命周期：重放查询返回其当前终态，不复活已删除实例，也不把旧请求指向同名新执行。初始归属 generation 同时作为持久化的 creation_generation，跨节点恢复保留此值；Request ID 回执据此读取当前执行，删除后同名重建使用新的生命周期标识。命中已确认 Environment ID 时，先复用原规格，再决定是否需要处理新的 Snapshot 输入。请求回执随命名空间保留，当前没有自动过期策略；需要回收时应先定义公共重试保留期。

## 存储边界

| 数据 | 更新时机与一致性 |
|---|---|
| 节点注册、地址和进程 session | 首次注册、新进程或地址变更时持久化；超时失效后，同进程完成恢复对账时重新启用成功发布标记 |
| 心跳序号、容量、设备能力、ready 状态 | 内存更新；普通心跳不写 Redis；重启后重新上报和对账 |
| Coordinator 地址发现 | 定期续约带 epoch 和 TTL 的发现键，独立于节点心跳 |
| 管理员暂停调度 | 设置改变时持久化，心跳不能清除 |
| 初次中心排队、选点和 Pending | 仅内存 |
| 初次成功及 Request ID 回执 | 原子条件发布；按任一成功身份复用 |
| 已确认实例的生命周期、恢复、快照、引用、凭据 | 各自对象的条件更新；保持既有持久化语义 |
| 节点超时失效 | 校验目标节点、受影响实例和该节点归属集合的版本后批量退役；并发新发布不会漏掉 |
| 发布序号 | 本进程内存；用于目录增量和缺口检测，不参与 Redis 对象写入条件 |

全局 header 负责 schema、分片数和 Coordinator 启动 epoch。普通对象写入不推进或比较整个控制表版本。Environment、Snapshot 等比较自己的旧值；故障批量更新只比较受影响对象。节点归属集合的计数只用于防止“扫描后新增执行”逃过节点失效，不会让不同节点互相竞争。generation 使用 epoch 划分的 `u64` 范围，由进程内分配器递增，重启后不复用旧执行身份。

目录发布器首次发送完整快照，后续按内存序号发送增量。序号缺口或订阅积压时重建全量视图；新 epoch 重新建立订阅基线。快照读取验证读取期间的发布序号，避免把旧快照标成新版本而漏掉事件。

## 可选本地优先

`create_mode: "local_first"` 启用入口节点暂留资源和 `ClaimEnvironment`。Coordinator 检查心跳、会话、硬约束，在最多 256 条、最长 1 ms 的微批中登记可选持久化 Claim，再导入调度账本。本地不足时用原身份和剩余期限回退中心路径。Pack/Spread、软评分和队列公平性属于中心路径；Snapshot 创建始终进入中心路径。

批量 Claim 中失效或缺失节点只影响对应项，事务仅校验实际参与写入的节点 session。Claim 的同一环境原子占位与 Snapshot 引用比较独立于其他对象更新。写入未知后，仅推进 Claim 自己的恢复屏障，再重载权威状态；普通生命周期写入不比较该屏障。迟到失败候选只能撤销自己尚无成功结果的 Claim，不能删除成功或替代执行。

## 对账和资源观测

容量采集失败保留最后一次有效容量和现有资源扣减，不因此关闭准入。尚无首次有效容量、明确 draining、管理员禁用和启动恢复仍可限制准入。

普通周期对账采用最终一致：获取失败保持原有目录和准入，不把失败解释为空目录。正常比较通过每个 Environment 的串行任务清理失去归属的执行；抓取目录期间新接纳的工作受比较基线保护。启动/进程替换则必须恢复控制器、账本并清理孤儿后才开放准入。

资源采样新鲜度与准入状态分别导出指标：旧样本过期会令 observation_fresh 为 0，但 accepting_allocations 继续反映实际准入判断。周期性对账失败保留最近已验证状态，必要的准入变化尽力上报；明确的 session fencing 仍触发恢复。

## 验证入口

- `python3 build/ci/run.py storage --jobs 2`：真实 Redis/AOF、对象并发、成功身份复用、崩溃重启、旧 epoch 与节点失效竞争。
- `python3 build/ci/run.py control-rpc --jobs 2`：真实 Redis/mTLS、中心与可选本地路径、心跳、重启、会话和资源账本。
- `cargo test -p adxlet --test node_reconciliation`：容量观测失效、周期获取失败、启动孤儿清理和清理重试。
- `cargo test -p adx-apiserver`：公开请求缓存、重放、剩余期限与错误契约。

这些组件/RPC 夹具使用 RuntimeDriver 测试实现，不能替代真实 sandboxd、Execd、多 VM 或 Kubernetes 验收。
