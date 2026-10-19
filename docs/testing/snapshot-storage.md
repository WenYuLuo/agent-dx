# Checkpoint 存储与快照生命周期

`adx-checkpoint-storage` 提供节点文件访问接口 `CheckpointAccess` 和 Coordinator
共享回收接口 `ArtifactReclaimer`。adxlet 处理捕获、恢复和本机缓存；Coordinator
管理 Environment/Snapshot 的保留关系并回收共享文件。默认 ADX 构建不依赖 AFS/FUSE。

## 三种模式

| 模式 | 捕获 | 恢复 | 物理回收 |
| --- | --- | --- | --- |
| local only | 写节点私有目录，完成可读性校验 | 直接读取本地目录 | 原节点；不支持跨节点恢复 |
| S3 + 本地缓存 | 写本地 staging，文件上传完成后发布 manifest | 命中缓存直接用；否则下载并校验后使用 | Coordinator 删除 S3；节点独立淘汰缓存 |
| AFS / distributed_fs | 直接写挂载目录中的独立捕获目录，逐文件同步后发布 manifest | 直接使用同一共享路径，不预下载整个制品 | Coordinator 删除共享 manifest 和目录 |

每个共享后端使用稳定 `alias`。同一 alias 的所有节点与 Coordinator 必须配置同一
存储 namespace。调度通过 `ADX_CHECKPOINT_STORAGE=<alias>` 选择兼容节点。
S3/AFS 支持跨节点恢复；本地恢复仍受源节点约束。

### adxlet 配置

`checkpoint_dir` 是本地目录简写；与 `checkpoint_storage` 互斥。
以下片段分别放入节点配置：

```json
{"checkpoint_storage":{"kind":"local","root":"/opt/adx/data/checkpoints"}}
```

```json
{"checkpoint_storage":{"kind":"s3","alias":"obs","bucket":"adx-checkpoints","region":"cn-north-4","endpoint":"https://obs.cn-north-4.myhuaweicloud.com","allow_http":false,"prefix":"production/checkpoints","root":"/opt/adx/data/checkpoints","cache_max_entries":500,"cache_budget_bytes":53687091200}}
```

```json
{"checkpoint_storage":{"kind":"distributed_fs","alias":"afs-checkpoints","mount_root":"/mnt/afs-dfs","prefix":"adx/checkpoints"}}
```

S3 通过标准 AWS 环境变量/身份机制取得凭据。记录只保存 alias、制品 ID、大小，
不保存端点或密钥。缓存默认 500 条、50 GiB，活跃恢复/执行文件受到 pin 保护。
上传完成并提交权威结果后，原 staging 直接进入缓存，不复制或重新下载。

AFS 要求 Linux 上实际存在来源为 `afs-dfs` 的 FUSE 挂载。启动及每次访问均检查挂载，
缺失时返回错误，不落到挂载点下面的普通本地目录。Coordinator 也需要挂载同一 namespace。
各进程使用自己的 AFS 身份与权限；`store-id` 核验 prefix 与 alias 一致。

外部 sandboxd 必须能看到传入的绝对路径。运行时的可写文件应放到私有执行目录，
不能修改已发布的 checkpoint。若希望避免跨文件系统复制，sandboxd 的运行目录也应配置
在同一 AFS 文件系统中；ADX 目前不通过单次 restore RPC 改写 sandboxd 的全局存储根。
此配置下需选择该文件系统支持的磁盘 I/O 模式；本仓存储验证使用 sandboxd
`writable_io_engine = "Sync"`、`writable_cache_type = "Writeback"`，S3 对照采用相同配置。
当前 AFS 不提供 `AsyncDirect` 所需的 direct I/O 对齐信息，不能直接沿用该运行时默认值。
DFS 的 `copy_file_range` 通过独立 inode / FileVersion 共享不可变 chunk 的 extent，
支持分段大文件、稀疏空洞和目标范围覆盖，不预读或重新上传源文件数据。
Meta 校验固定源版本、同 namespace、源/目标范围及完整布局；副本后续写入不会改变源快照。
源与私有运行文件需要处于同一 AFS 文件系统，且目标写归属在本客户端；
同 inode 或远端目标写归属返回不支持，调用方可能走普通复制回退。

锁定 sandboxd `efc2015` / Firecracker `v1.16.1-akernel.3` 的 SharedFile 路径
使用上述标准文件操作创建私有 `memory.live`，无需修改外部运行时协议。
验收同时检查 COW 提交日志、系统调用和延迟触页的远端数据增量；
`copy_file_range` 返回逻辑字节数或 guest RSS 增长本身都不代表实际数据已按需读取。

### Coordinator 配置

本地模式使用默认空列表。共享模式必须配置对应的回收后端，配置中没有节点缓存目录：

```json
{"checkpoint_reclaimers":[{"kind":"s3","alias":"obs","bucket":"adx-checkpoints","region":"cn-north-4","endpoint":"https://obs.cn-north-4.myhuaweicloud.com","allow_http":false,"prefix":"production/checkpoints"}]}
```

```json
{"checkpoint_reclaimers":[{"kind":"distributed_fs","alias":"afs-checkpoints","mount_root":"/mnt/afs-dfs","prefix":"adx/checkpoints"}]}
```

Coordinator 的 S3 身份需要 prefix 下的 List/Delete；AFS 身份需要目录访问与删除权限。
缺少回收器时删除保持待完成并报告错误，不转交给源节点删除共享数据。

## 文件接口与流程

节点侧：

1. `begin_capture()` 分配运行时写入目录；`capture_references()` 给出捕获预留身份。
2. adxlet 在调用运行时前持久化文件预留。`size_bytes=0` 仅用于未完成捕获的保护，不能恢复。
3. runtime 写入完成后，`seal()` 完成该后端的发布合同并返回完整 artifact。
   S3 上传并发布 manifest；AFS 同步文件并发布 manifest；local 校验文件可读性。
4. adxlet 提交恢复点/Snapshot 的权威元数据，随后调用 `committed()` 完成本地缓存登记。
5. 恢复时 `prepare_restore()` 返回 `RestoreAccess`；S3 取得缓存 pin，AFS/local 返回原目录。
   adxlet 在执行仍依赖文件时保留访问对象及持久化输入记录，确认停止后释放。
6. 节点 `release()` 只处理本地文件/缓存，AFS 不在此删除共享文件。

`seal()` 完成不代表 Environment/Snapshot 元数据已经提交；对外成功需要两者完成。
local-only 沿用异步写回语义，不强制 fsync，也不承诺主机断电/丢失后的文件耐久性。
AFS restore 检查 manifest、路径、文件类型和长度，不扫描完整文件内容；数据读取由 AFS 执行。

## 匿名快照跟随 Environment

`EnvironmentRecord.checkpoint` 是该 Environment 的匿名恢复点。
显式有限 TTL 或永久保留沿用 pause 的 retention；永久保留持续到替换或删除 Environment。
到期、替换或删除会移除对应保留关系。旧文件若仍被运行时映射，需等该执行停止后才能回收。

`pauseOptions.storage` 的 typed 枚举值为 `configured`（默认）、`local`、`shared`。
`configured` 使用配置后端，`shared` 要求共享后端；S3 节点可用 `local` 保留本地恢复点。
AFS 节点显式 `local` 会拒绝，需选择本地存储节点。该选项不会把 AFS 目录当本地磁盘。

## 可复用 Snapshot

创建 Snapshot 直接捕获独立制品，不需要先创建匿名恢复点或复制已有恢复点。
从 Snapshot 创建 Environment 时借用该不可变制品，并创建独立运行身份和可写运行状态；
不自动复制第二份快照。`snapshot_source` 与匿名 `checkpoint` 分开记录。
目标暂停生成自己的匿名恢复点，且停止原执行后，才释放旧 Snapshot 的使用关系。

Snapshot 使用中删除立即返回 HTTP 409：

```json
{"code":409,"message":"snapshot is in use","error":{"code":"SNAPSHOT_IN_USE","retry":"never","outcome":"not_started","requestId":"request-id","sandbox_ids":["sandbox-a","sandbox-b"],"template_ids":["node/base"]}}
```

保持 Snapshot Ready，不排队等待 Sandbox 退出。调用方处理完列表中的使用者后重新删除。
列表包含已分配恢复、仍映射文件的源/目标执行，以及失联节点未确认清理的旧执行。
SDK 的 `EnvironmentHTTPError.sandbox_ids` / `template_ids` 提供这些列表。

无使用者时，Redis CAS 将 Snapshot 置为 Deleting，关闭新引用。后台执行物理删除并提交 Deleted；
存储失败或应答丢失会幂等重试。共享删除不要求源 adxlet 在线。

## 共享回收与故障

Coordinator 在 Redis 持久化共享制品清单。每轮检查 Environment 恢复点、执行输入、
Snapshot 和旧执行保护，随后原子写入删除标记，拒绝迟到提交重新引用该身份。
物理删除成功后移除清单项；删除标记跨 Coordinator 重启保留。

节点失联只让执行归属失效，不证明进程已停止。旧执行文件保护在 Redis 中保留；
节点重新完成停止旧执行及对账，并用当前会话报告可路由后，才释放该保护。
无法恢复的节点会保守保留文件，需要运维确认隔离/清理后恢复该节点的对账，不能只按心跳 TTL 删除。
Paused 保持休眠，不因为节点失效批量唤醒。

Coordinator 重启后，adxlet 排空已接纳的恢复操作期间仍上报心跳，标记正在对账且停止接纳新任务。
关闭准入不能阻塞心跳，否则共享存储下载较慢时会把健康恢复节点误判失效。

节点已不运行共享存储删除扫描。无权威记录的历史对象不自动推断归属；
S3 未完成 multipart upload 由部署配置过期中止规则。
停止写入且已无保留关系的捕获预留（`size_bytes=0`）也由 Coordinator 回收；
这种记录仍禁止用于恢复，回收不要求先形成完整 manifest。

## 验证边界

定向测试包含存储目录直读、缺失挂载、manifest/符号链接拒绝、缓存 pin、
匿名恢复点生命周期、真实 Redis 的重启/引用竞争/旧执行保护、HTTP/SDK 使用中删除契约。
目录 fixture 仅验证文件访问适配逻辑。真实 AFS/FUSE、双 Ingress、Firecracker 与
S3/OBS 的单独验证由 `build/e2e/firecracker/storage.py` 执行，日志和逐项证据保存在
工作树 `out/afs-fc/`；原有组件测试日志保留在 `out/snapshot-storage/`。
测试拓扑为同一专用 KVM 主机的两个隔离执行节点，不代表两台物理机器故障域。
AFS COW 与同版本 S3 对比的本轮日志另保存在工作树 `out/afs-cow/`；故障用例记录
Coordinator 重启后的目录进度与排空心跳，并检查目标节点未被误判失效、旧 VMM 退出早于替代 VMM 启动。

## 分段 profile 与性能对比

在已准备好的 Firecracker / AFS fixture 中，为 Meta 进程及其启动父进程设置
`AFS_META_PROFILE=1` 可记录批次提交、排队、旧状态释放及后台完整检查点的分段耗时。先保留整份脱敏 `export/`
（包括两个 Ingress 的服务日志），再汇总：

```bash
python3 build/e2e/firecracker/profile_storage.py /path/to/export/evidence --output /path/to/stages.json
```

汇总按 Environment、Runtime 和首个成功请求的 ID 关联恢复阶段，包括 adxlet 准备、
sandboxd open / prepare / instantiate / VMM ready / load / agent、应用就绪探测、
路由激活和 Ingress 等待。COW 排队与执行计时取同一请求组前后指标增量，属于
sandboxd 阶段内部开销，不能与父阶段相加；RPC 与首 HTTP 的外层差值单独保留。
汇总保留提交数量和编码字节区间；local-file 的日常编码是本批次增量，不能把它的编码字节
当成完整权威状态大小，与旧版完整快照的大小区间直接比较。前后对比应同时核对
`state_counts`（实体、幂等结果和事件数量）、请求操作及负载；后台完整检查点另计，
避免把后台续租数量变化或编码格式改变误当成相同工作量。

Meta 增量持久化回归由 `cargo test -p afs --all-features` 覆盖：小提交不重写历史快照、
后台检查点保留并发提交、跨检查点重开不丢幂等结果／watch 事件、提交成功后应答丢失、
不完整尾帧截断、完整帧校验和损坏、版本缺口／重复、检查点安装中断后恢复及失败后的权威关闭。
另验证后台等待到期保留原 pending 请求，正常关闭期间的迟到心跳不重新开启读授权。
这些进程／文件测试不代表物理断电或跨物理主机的存储故障验证。

正式后端对比关闭 Meta profile 和 syscall trace；`compare_storage.py` 会拒绝带此类
诊断的轮次，并核对发布包、外部运行时、guest 镜像和工作负载身份。每个后端保留
三次冷恢复、三次热恢复及生命周期验证，使用相同拓扑和顺序。独立的带 trace 轮次
验证内存文件 COW、首次响应前未整份读取、延迟触页后远端读取增加，不计入性能样本。
业务 HTTP 失败时，fixture 在删除 Environment 前保留请求身份、Ingress 副本、状态码、
响应正文及一次 guest 日志／网络状态采集；guest 采集失败也保留原错误响应。
只有明确的 `503 ENV_RESUMING` 使用等待重试，502 等执行结果不确定的失败直接终止验收。

OBS 的恢复中重启用例在目标 fixture 的网络 namespace 内，临时阻断 Execd readiness
端口 50090。确认真实目标 VMM 已恢复且控制面仍有 pending recovery 后，才重启
Coordinator；观察目标节点至少两次成功的排空心跳后解除阻断。异常退出也移除规则，
且最终验证目标未失效、旧 VMM 先退出后再启动替代 VMM。该阻断只用于性能样本之后的
故障用例，上传、下载、VMM 和业务请求仍使用真实实现。

## 原生 AFS 回收验收

设置 `ADX_STORAGE_GC=1` 后，真实 AFS/Firecracker 驱动把专用 Meta 的结果窗口设为 30 秒、
Watch 窗口设为 16 个 revision，保留生产的 60 秒引用保护期。
驱动先验证 COW/open-unlink 数据在回收期间仍可读取，替换一个 AFS Node session 后再检查
剩余读者及最终物理清理。之后执行双 Ingress 流量唤醒和 Coordinator／执行节点故障场景，
再对同一个 Environment 做两批各三次暂停恢复，每次 12 个并发请求且只允许一次物理恢复。

`gc-observations.json` 记录每阶段的 Meta 实体、幂等结果、Watch 水位，以及各 Node 的
实际 chunk 数量、字节和 catalog 大小。两批之间等待保护期结束，比较稳定存量；删除
Environment 后必须回到初始存量附近，同时 inode、目录项、版本、布局、引用、写租约和 chunk
实体不能超过初始数量。初始存量只允许 CheckpointStore 的三字节 `store-id` 及其元数据，
不允许上轮执行文件或写租约。只看到目录删除或 SDK 成功不能判定物理回收通过。
执行拓扑仍是一台 KVM 主机上的两个隔离执行节点，不代表两个物理故障域。

## AFS unlink 并发回归

`afs/tests/dfs_commit_contention.rs` 在 unlink 读取与提交之间执行真实 DfsService
元数据写回／同父目录的另一个 unlink，检查冲突后重新校验和提交。回归同时覆盖
权限变化、名字被替换、目录新增子项、持续竞争返回 EAGAIN，以及提交成功但应答
丢失后的幂等重放。测试不把预设返回值代替真实的 Meta 比较提交。

真实 AFS/Firecracker 验证可开启 `AFS_META_NAMESPACE_TRACE=1` 关联失败条件和
相邻写入事务；关联必须使用 inode 及版本／head，不能只凭相近时间认定竞争双方。
删除验收还要检查私有运行文件、匿名恢复点、Meta 实体和物理数据块均已收敛。
