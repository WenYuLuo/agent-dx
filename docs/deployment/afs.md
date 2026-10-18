# Agent FS（AFS） 部署与配置

Agent FS（AFS） 是 Agent DX 的可选文件系统组件。默认部署配置、默认发布包和默认 supervisor 服务都不包含 AFS 二进制、配置示例或 FUSE 依赖；只有显式设置 `ADX_WITH_AFS=1` 的文件系统包和部署入口才包含这些内容。

## 发布包合同

默认包不得包含：

- `bin/afs-meta`
- `bin/afs-node`
- `etc/examples/afs/`
- Agent FS（AFS） 专用端口、数据目录、挂载点或探针程序

显式文件系统包必须在 `manifest.json` 中记录 `with_afs: true`，并包含 `afs-meta`、`afs-node` 和 `etc/examples/afs/*` 的摘要。测试辅助程序只由验收流程按需构建和放入测试环境，不加入普通试用包。
组包完成后的校验与安装前／暂存后的校验共用随包
`lib/package_manifest.py` 合同；AFS ON、默认 OFF、旧 `with_dfs`、文件清单、摘要和
主机架构不会由两份独立规则分别解释。

## 配置示例

发布包配置示例来自 [build/config/examples/afs/](../../build/config/examples/afs/)，源码级示例保留在 [afs/examples/](../../afs/examples/)。部署层只负责把 AFS TOML 交给 `afs-meta` 和 `afs-node`，以及进程启动、健康查询、正常停止和卸载；它不改写 AFS 自有配置 schema。

Meta 后端优先级：

1. `memory`：一次性演示，重启不保留状态；
2. `local-file`：当前可靠性基线，中心节点可正常重启恢复；
3. `etcd`：资源和可靠性专题后置；
4. `redis`：最低优先级，后置。

当前试用建议使用 `local-file`。发布包内 `etc/examples/afs/meta.toml` 已显式设置 `meta_store = "local-file"`，并把 Meta 状态放在 `/opt/adx/data/afs/meta`，避免因代码默认值误连 etcd。使用 `memory` 时，验收报告必须明确“Meta 重启后不保留状态”。

单机试用前先准备配置、数据、运行时、挂载目录和本机临时证书：

```sh
sudo install -d -m 0755 /opt/adx/config/afs /opt/adx/run/afs /mnt/adx/ownerfs /mnt/adx/dfs
sudo install -d -m 0700 /opt/adx/data/afs/meta /opt/adx/data/afs/node-a
sudo cp /opt/adx/current/etc/examples/afs/meta.toml /opt/adx/config/afs/meta.toml
sudo cp /opt/adx/current/etc/examples/afs/node.toml /opt/adx/config/afs/node.toml
sudo /opt/adx/current/etc/examples/afs/prepare-local-tls.sh
sudo cp /opt/adx/current/etc/examples/afs/deployment-ownerfs-local.yaml \
  /opt/adx/config/deployment.yaml
sudo adxctl validate
```

证书脚本只创建 30 天有效的本机隔离试用 CA、Meta 和 `node-a` 身份，并在已有
任一目标文件时拒绝覆盖；它不分发固定私钥，也不适用于生产或多主机扩容。生产部署
必须由自己的 CA 为每个 Node 签发独立身份，并在 Meta 和 peer 的
`trusted_node_certs` 中把证书精确映射到该 Node ID。不要复制 Node 私钥到其他主机。
证书绑定表不能替代 TLS 配置；启用 OwnerFs 或 DFS 权威的 Meta 缺少 mTLS 时，
必须在监听端口和打开持久后端前拒绝启动。

示例 Node 配置使用 mTLS `https://` endpoint、`/opt/adx/data/afs/node-a` 和
`/opt/adx/run/afs/node-a.sock`。前台运行 `sudo adxctl run` 后，在另一终端用
`sudo adxctl status` 检查 Meta 与 Node 都返回 `status=ready`，并用
`sudo adxctl stop` 正常停止。Supervisor 按 Meta→Node 排序启动；Node 在尚未挂载或
暴露本地 API 前对首次 Meta 注册做有界、可取消重试。预算耗尽、证书/身份拒绝或运行后
心跳/排空失败仍然失败关闭，不会无限重试或盲目重启。

如果启用 FUSE 挂载，先确认 `/mnt/adx/ownerfs` 与 `/mnt/adx/dfs` 为空目录，再在
`node.toml` 中启用对应 `ownerfs_mount` 或 `dfs_mount`。挂载所有者捕获的 canonical
path、mount ID 和 source 同时作为 readiness 身份；挂载被替换时健康检查继续失败关闭。

## OwnerFs workspace bind ON

普通发行默认关闭 workspace bind。当前实际试用场景需要显式开启，示例字段以 `build/config/examples/afs/node.toml` 为准；历史 `native_workspace` 命名只作为兼容入口，不能作为新的核心能力名称。

开启条件：

- 先通过 OwnerFs 正常创建并授权 workspace，再开启 bind；不要在 Home/root/epoch 尚未固定时直接打开开关；
- workspace 必须是已授权的本地 Home workspace；
- bind 源必须是 Home 上的底层真实目录；
- bind 目标必须是 OwnerFs FUSE 根下对应的一级目录；
- 同一 Node 只启用管理员控制的 host bind entry；
- 停机、root/epoch 变化或授权失效前，先停止受管用户并排空引用。

## 运维边界

启动前一次性检查二进制身份、配置 SHA、端口、TLS、数据目录、运行目录、日志目录、挂载目录、FUSE 设备、`fusermount3`/`umount`、旧 PID、旧 UDS、旧挂载和磁盘余量。遇到环境阻塞时保存证据并停止受影响项，不把环境修补混入产品结论。

正常停止应记录进程退出状态，确认 FUSE mount 和 bind mount 已卸载，UDS/PID/临时 claim 无遗留。`local-file` Meta 的恢复检查至少要覆盖写入、sync/close、正常停止、重启、重新打开和内容校验；这不等于 crash recovery、HA、多 Meta 或复杂可靠性通过。

## DFS 外部 S3 连接

这是实验接入，先验证小规模发布／回源闭环，尚不承诺性能或完整故障验收。
在新 Meta 和参与的 Node 中添加相同的身份与后端连接配置；Node 另需凭据。
保留默认 DFS 单副本计数，任何外部后端与多 Node 副本的组合均在启动前拒绝。

```toml
[dfs_external_backend]
store_id = "filesystem-a"
generation = "generation-1"
endpoint = "https://s3.example.com"
bucket = "afs-chunks"
region = "us-east-1"
allow_http = false
unsigned_payload = false
# Node-only; omit on Meta. Must be absolute, owned by the Node process UID,
# regular file without symlink, and mode 0600 or stricter.
credentials_file = "/opt/adx/config/afs/s3-credentials.json"
```

凭据 JSON 包含 `access_key_id`、`secret_access_key` 和可选 `session_token`。
不在示例中提供真实凭据，不把秘密直接放 TOML 或命令行。未指定文件的 Node
显式读取 `AWS_ACCESS_KEY_ID`、`AWS_SECRET_ACCESS_KEY` 和可选
`AWS_SESSION_TOKEN`；缺失凭据启动失败。`--print-config` 只显示文件路径。
默认 `unsigned_payload=false`，保留 S3 载荷签名。只有管理员确认后端和安全策略支持时才显式设置 `unsigned_payload=true`；该项仍保留 SigV4 请求
认证，只省略 SDK 的载荷签名 SHA256，不替代 AFS 完整 BLAKE3／长度检查，也不
改变成功 fsync 等待对象回执和 Meta 持久确认的要求。它不自动探测 SeaweedFS，
不因 `allow_http=true` 自动启用；泛 S3 后端和既有配置保持原默认。

默认 HTTPS；`allow_http=true` 仅供隔离测试端点。外部服务的认证、bucket
权限、数据同步落盘及数据库持久配置由管理员在服务端保证；HTTP 成功本身
不能证明物理持久性。产品包不部署或打包 SeaweedFS、PostgreSQL 或 JuiceFS。

Meta 的 `meta_store` 是另一项独立配置：程序默认 etcd，现有试用示例明确选择
local-file。本接入不增加 PostgreSQL Meta，也不改变原默认或后端选择。
对象 key 身份不允许变更后重新使用同一 Meta 数据目录；配置错误及 GET/PUT
错误向调用方传播。正常停机仍需先排空工作负载和 FUSE 引用。

S3 适配层共用一个 worker、运行时和客户端，固定最多四个实际 PUT；完整
4MiB 块可以提前上传，完成后滚动补位。最多32个活动命令和一个排队命令，
队满等待回压；共享客户端最多保留四个空闲连接。空闲连接池大小本身不是并发限制。

顺序追加以64MiB为一个发布单元，冻结窗口上传期间可按预算余量接纳下一窗口；
普通 write 返回表示已接纳，中间版本稍后发布。独立只读节点读取已发布版本，
同挂载和已有 owner 代理的 dirty read 先完成读入口前已接纳序号的有序发布，
发布失败报错，不能回退旧内容。fsync、fdatasync、可写 flush、同步写
和必要排空必须等待有效对象回执及 Meta 持久确认，不能仅凭 PUT 成功返回。
覆盖写、稀疏写和截断先收口相关窗口，再使用原有提交路径。Meta ACK 未知时保留
原请求身份并阻挡后续 mutation；PUT 失败保留脏数据和可复用的成功回执。

Node 写流水线采用分配前预算：普通数据128MiB，发布保留32MiB，总计160MiB；
共享内容只计一次，失败、准备提交和未知 ACK 期间仍存活的内容不能提前退账。
PUT 直接持有已封口的不可变载荷及其预算租约，不再复制整块 HTTP body；
发布构块使用独立串行保护，仍存活的 transport／retry owner 释放前不得退还额度。
这项预算与缓存容量、缓存 pending、读取缓冲及进程其它开销分开核算，不能作为
总 RSS 上限。失败或超时可能留下未引用的不可变对象，不能直接删除可能被引用的对象。

外部模式的当前恢复范围是读取已经发布的版本；写节点重启后重新取得写权
另行验收。旧写租约有效时，新会话立即复写原文件可能返回 EBUSY，不能将
“退出后可回源读取”理解为“新会话立即恢复写入”。

### 可选的异步内容缓存

仅在 Node 配置中增加以下子表，省略时保持 cache OFF；Meta 不使用此缓存配置。
缓存目录必须专用、绝对路径、由 Node 进程用户持有且权限为0700；父目录须已存在。
不要与 Node 耐久 chunk 目录、源码共享目录或其它服务数据目录混用。

```toml
[dfs_external_backend.cache]
directory = "/opt/adx/data/afs/s3-cache"
capacity_bytes = 268435456
pending_bytes = 37748736
cache_large_write = false
```

`capacity_bytes` 约束当前 store/generation 的 ready 文件及临时文件预留的内容字节，
非零且不超过1TiB；`pending_bytes` 约束被缓存接纳的排队、正在落盘、单个已验证
驻留槽及读者仍持有的分配容量，共享分配只计一次，非零且不大于
capacity。它们不是文件系统配额或进程RSS上限，索引、文件系统块分配、读写工作缓冲
及其它历史目录需另计环境预算。普通填充资源不足时跳过缓存，不阻塞后端确认。

完整GET校验成功后可异步填充，读取优先复用Node共享的单个已验证对象（最多4MiB），
再查pending或加载完整缓存文件。驻留不足时回退原路径，不扩充额度。
外部只读每次 open 在线获取最新版本，版本对象授权可复用最长5秒，同时受
session／grant 截止限制；每次 read 仍检查合法范围、期限和已知失效。Node
停止／epoch 变化关闭复用；Meta 断链时新 open、新对象及到期续期正常失败，
已授权范围只可继续到原截止。未知 Meta 重启不保证瞬时撤销，恢复后重新在线
准入。此固定上限不是新增配置开关；授权元数据最多128项／1MiB，与载荷分账。
来自磁盘的新内容仍安全检查并完整BLAKE3校验。RAM命中
不检查磁盘副本后来发生的变化，下次磁盘加载负责发现损坏并回源。小于4MiB的写入块默认允许填充，完整块
仅在`cache_large_write=true`时填充；后台写盘可与上传重叠，但不产生持久回执。
缓存损坏或读取失败回源S3；S3请求、授权和持久提交错误仍向调用者传播。
正常退出排空并join后台缓存工作，不要求cache fsync。

cache ON 必须按[验收说明](../testing/afs.md#dfs-外部-s3-闭环)登记对应版本的实际 Linux 结果；
不能由配置可解析或工具就绪推导功能／性能通过。SeaweedFS部署继续归外部测试方案。

## FUSE 接收与 DFS 页缓存配置

以下是Node顶层TOML键，也可通过同名连字符CLI选项覆盖；Meta不使用这些挂载参数。

| 参数 | 默认 | 范围与作用 |
| --- | --- | --- |
| `fuse_threads` | `4` | `1..=64`，OwnerFs／DFS接收线程 |
| `fuse_clone_fd` | `true` | 仅多接收线程时生效 |
| `fuse_workers` | `32` | `1..=1024`，有界callback executor，队列容量为worker数×8 |
| `dfs_fuse_keep_cache` | `true` | 仅原生DFS只读、已核对提交身份的open允许保留页面 |
| `dfs_fuse_entry_ttl_ms` | `0` | `0..=60000`，路径entry TTL |
| `dfs_fuse_attr_ttl_ms` | `0` | `0..=60000`，属性TTL；CREATE的单TTL回复取两者较小值 |
| `fuse_max_readahead_bytes` | `1048576` | `0..=16777216`，仅DFS的内核read-ahead请求上限，实际值受协商限制 |
| `dfs_version_cache_entries` | `4096` | `1..=65536`，不可变版本／布局的resident元数据条目数 |
| `dfs_version_cache_bytes` | `33554432`（32MiB） | 标量范围`1..=1073741824`；具备DFS能力的Node还校验与entries相关的最低结构预算 |
| `dfs_outboard_work_wait_ms` | `2` | `0..=50`，共享 proof 工作额度不足时的最长准入等待；0 表示不等待，仍保留完整校验和原全扫回退 |
| `dfs_peer_serve_resident_objects` | `2` | `1..=16`，A端已验证完整对象的LRU条目上限，每对象至多4MiB |
| `dfs_peer_serve_payload_bytes` | `16777216`（16MiB） | `4194304..=67108864`，A端驻留、填充、退役pin及packed/RDMA共用的实际正文capacity上限 |
| `dfs_source_cache_max_ms` | `5000` | `0..=60000`，源元数据缓存的最长保留时间，另受grant安全截止约束；0不保留且不前瞻 |
| `dfs_source_lookahead_chunks` | `64` | `0..=128`，仅后续源元数据；与mandatory合并请求至多128项；0仅关闭前瞻 |
| `dfs_source_cache_bytes` | `4194304`（4MiB） | `0..=4194304`，最多1024条源元数据的保留估算上限；0不保留且不前瞻 |

TTL均保持0才属于严格路径新鲜度配置；正值是显式宽松模式，rename替换等路径
可能在TTL内继续解析为旧inode，不能宣传严格close-to-open。OwnerFs原Home缓存、TTL、
direct-I/O与mmap策略不由上述DFS参数覆盖。read-ahead不等于对象预取；测试必须
记录有效`read_ahead_kb`和应用open flags，不能用配置请求值冒充协商结果。

DFS复用以完整提交版本ID及长度为准；新open仍在线检查权限和Meta。dirty、provider、
可写或外部S3句柄不取得此复用资格，身份表超限时保守不保留。已经交给内核的页面
不再次经过用户态授权，不能解释成可立即撤回的远端载荷。

提高worker数会增加可同时存活的私有读取结果。原读配置是每操作上限，不是Node全局
RSS配额；A共享正文、B填充、帧、元数据、allocator和内核页缓存须分别核算。
默认线程数也不构成吞吐或时延达标保证。

A端serve正文默认从8MiB调整为16MiB，不能将旧资源上限的结果直接标为新配置证据。
最多8个对象key同时填充，同对象完整结果可共享，partial仍走原范围路径；容量不足
立即返回结构化容量错误，不增加等待队列。控制元数据固定不超过64KiB，独立于正文和
原proof账本。停止后不接受新对象或packed准入，正常排空等待最后引用，首次关闭起
10秒仍未完成则报错。此配置不改变B端磁盘缓存、S3预算或每RPC的独立授权。

源缓存的1秒grant安全余量固定，期限由请求发送前的单调时钟与墙钟锚定，Meta处理／传输
时间不会在回复或安装时被补回。显式配置max超过5000ms不代表授权也获得同样延长；
原grant、Node/session和每个读取上下文的原截止仍分别生效。native与external路径的源元数据
共用这份账本；external授权的原1MiB和S3正文／descriptor预算独立保持。
4MiB不包括当前RPC reply／读取clone或正文。低字节限可能缩小有效条目容量；超大单项
当前仍可依法使用，但不进入缓存，不提供保底例外项。仅可选ID跳过512B以上的值，
最多128项的每份新增advisory字符串正文≤64KiB，String槽与请求／集合副本须另计。

未发布接口统一为`dfs_source_cache_max_ms`，旧`dfs_read_source_cache_ttl_ms`／
`--dfs-read-source-cache-ttl-ms`已移除，会明确报未知字段／参数；请更新为新名，不能继续
依赖旧配置或静默回落。新名按默认值＜TOML＜CLI解析，最终组合再校验。


版本缓存不保存权限或正文，不代替新open的在线检查。entries与bytes双上限均在启动时
生效；bytes按版本、布局、索引的容器capacity、各ID字符串capacity和缓存容器包装
保守估算，不只统计extent数量。超大布局仍能被当前操作使用，但不被缓存保留；淘汰后
在途读者仍可持有其Arc。此上限不等于所有活跃引用、加载临时分配或整个Node RSS上限，
不能据命中率推断新鲜度或授权有效。

超大版本未被保留时，后续需要该布局的每次用户态读取可能重新执行Meta
`get_file_version`、传输完整布局并重建索引，存在比原单项缓存更高的控制成本；
内核页缓存命中不进入该路径。缓存还预扣与entries相关的保守容器开销T，只有
`T + 单项保留估算 <= dfs_version_cache_bytes`时才可能接纳该项。当前64位构建中
T约为`2048 + 384 × dfs_version_cache_entries`字节：默认4096项约1.50MiB，65536项
约24.00MiB；这是实现估算，非实测RSS，也不是固定文件大小阈值。具备DFS能力的Node
按CLI／文件最终合并值校验`bytes >= T + sizeof(ReadOnlyVersion) + 2*sizeof(usize)`，
不足时拒绝启动，并报告该entries配置的minimum。表开销与运行缓存复用同一公式；
这里只是固定字段与Arc头的结构下界，不假定ID字符串长度，也不保证任何真实布局都能缓存。
真实字符串、extent和索引capacity仍逐项计费；超大／碎片化布局超过上限时不新增例外槽，
每次需要该布局的用户态读可能重新加载Meta。可同时减小entries或增大bytes，
并按实际布局估算单项成本；Meta进程及未编译DFS的OwnerFs-only进程不分配该DFS缓存。

## 原生 Peer 可丢弃磁盘读缓存

仅供未配置外部后端的 DFS Node；配置在读者 B 上即可，普通缺省 OFF。
目录必须是独立绝对路径，与 `data_dir`、FUSE挂载点互不嵌套，不含 `..`，由Node进程
独占使用；初始化遇到不明文件或不安全身份应报错，不清理其它目录。

```toml
[dfs_peer_cache]
directory = "/opt/adx/cache/afs/peer"
capacity_bytes = 268435456
```

容量可配置，非零且不超过1TiB；256MiB只是示例。缓存保存不可变对象的临时副本，
近似LRU自动淘汰，条目最多4096；v2文件为4096B固定header、正文和至多8192B的
Bao outboard trailer。ready与pending实际分配受磁盘容量限制，目录及marker开销另计。
旧v1专属缓存条目可退休重建；这不是持久数据格式迁移，不得删除A的唯一持久副本。
回滚到只识别v1的旧Node时，不能直接复用包含v2 ready文件的缓存目录；应先正常停止
Node，再按专属缓存目录的清理合同退休可丢弃条目，或配置新的空缓存目录／禁用可选缓存。
此操作仅针对B的可再生缓存，不得清理A的持久副本、catalog或Meta数据。

`dfs_fetch_max_inflight_objects`默认8，范围1..=8，可由CLI覆盖TOML；仅限制原生Peer
磁盘缓存的metadata flights，不适用于S3，也不是正文并发数。一个active fill仍共用原
4MiB逻辑额度；不同缓存hit与fill可并行，冷对象之间仍受该单fill限制。这个4MiB
是准入和观测的保守包络，不是常驻正文内存或Node RSS上限。流式分支的两个256KiB
载荷块、解码／控制帧和私有输出须按各自所有权分别核算，不能把它们重复相加为
额外可用额度；legacy完整对象缓冲受同一fill许可约束，单批仍校验原读取预算。

原生回填只有一个固定pending路径和一份文件身份／磁盘预留；ready和pending实际
分配共同受`capacity_bytes`约束。外部S3缓存示例的`pending_bytes = 37748736`
（36MiB）仅属于外部缓存，不是原生Peer已有额度，不能用来开启多对象并发回填。
后续若支持并发回填或预读，须先明确原生总字节准入、逐flight临时文件及身份、
私有输出和在途帧的实际capacity与最后引用退账；当前不新增配置、额度或多fill能力。

表内indexed、retired与删除中条目合计最多4096，实际文件删除前磁盘占用不退还；被pin的退休条目
也占容量，同key不能在旧文件仍被使用时重建。容量或控制元数据不足保留安全回退。

索引、entry及proof使用同一原proof ledger的resident子限；每个operation的8KiB
快照控制包络与有界flight使用该ledger的瞬时额度，128个operation是结构上限，
不保证都同时取得快路。该包络按有界容器和字符串容量保守计量，不等于allocator或RSS。
无控制额度的命中可pin并完整核根；不能将安全回退写成命中失败或损坏。
正常停止保留健康ready供恢复；失败pending删除仍可能留到下次独占恢复处理，
不得手动删除身份不明的文件或把它们当作已经释放的空间。

原1MiB proof ledger由同一Node的A store、原生写pool和B cache共用；计入proof实际
capacity、Arc／包装、构建／encode／decode临时分配及证明索引（当前64位构建为32KiB），
在途引用直到最后owner释放才退账。总额的一半（默认512KiB）作为常驻保留子限，
A pinned／Peer单槽和B proof／索引包装共同受限；不是额外额度或另一池。条目淘汰但
引用仍活跃时不能提前退账。临时验证／写入builder共享剩余总账，超过可用额度允许安全
回退，不保证默认32 workers都取得快路径。容量不足不能当作健康proof损坏并触发反复
重建或写盘；对应准入失败、原生写无proof和范围全扫回退需要分别观察。不挪用正文额度。它不代表进程总RAM；codec、TLS、HTTP/2及内核pagecache
仍按原独立预算核算。这里没有新增正文RAM驻留缓存或预取。

本机读、A端Peer范围服务及B磁盘命中统一使用标准BLAKE3根与32KiB覆盖组outboard。
proof包含64B格式／长度／根header和parent CV，4MiB对象最多8192B；命中仍读取、
校验覆盖组；连续对齐范围直读私有输出后用官方range validator验证，仍有32KiB
scratch复制；至多128个无别名范围且有完整中部时，非对齐／多范围直读私有结果并补齐覆盖组边界。其它情况保留原codec，不承诺零复制。本机规范ChunkId的根／长度
须与Meta布局及catalog一致，无需新增Meta RPC；legacy ID或持久副本缺失、旧／坏sidecar
走原完整根扫描。sidecar是可选优化：正文、chunks目录和catalog必需屏障完成后，
安装工作进入有界后台队列，不等待sidecar文件／目录sync再返回ACK。
`dfs_sidecar_install_queue`默认64、范围1..=64，限制queued＋active；每批最多32项。
proof引用和控制容器／ID／路径容量都计入原1MiB ledger，控制容量空闲时仍计费，
最后引用及真实释放后才退账；不新增正文缓存或Node全局写额度。
队满、瞬时元数据不足、停止或安装失败只放弃可选项并计数；安装前的读可全根校验并
按需修复。健康proof逐字节匹配后不重写／rename／file sync，后台批末独立目录sync。
可选proof失败不改变正文、catalog、receipt及成功fsync屏障。该队列不是文件版本提交入口。
停止时关闭入队且固定10秒截止；超时不启动新的可选目录sync，不取消已开始I/O或提前
释放引用；真实join若卡住仍由既有15秒watchdog以124结束，不能当正常排空。
`afs_dfs_sidecar_install_items{state="queued|active|limit"}`与
`afs_dfs_sidecar_install_control_bytes`分别报告项数和有界idle控制容量；
`afs_dfs_outboard_events_total{event="sidecar_install_dropped|sidecar_install_failed"}`
报告可选工作处置事件，不表示写入持久化失败或精确对象完成数。
读侧也可同步修复缺失／损坏的sidecar，安装可能产生文件与目录sync。健康sidecar仅
在固定共享修复锁内比较而不write／rename／sync，避免同对象重复安装；已验证Peer正文
单槽命中不再访问sidecar。修复不持
Peer正文单槽锁；这条读修复仍同步执行，与写ACK后的可选installer区分。缺容量时走完整根安全读取，不能借修复改变
原正文、catalog或成功fsync合同。

并发范围验证的快路径受原proof总账约束：对齐单范围工作票据36KiB（32KiB scratch
加4KiB元数据余量），非对齐／多范围仍为76KiB。resident接近512KiB且没有其他临时
占用时，剩余约512KiB分别容纳14张／6张票据，实际取决于占用及调度重叠。
准入使用入口固定的截止时间，退账可唤醒等待者，伪唤醒不延长等待。停止时唤醒
等待者，不提前退还仍被引用的分配；builder和常驻proof准入仍不等待。
超过截止时间仍拿不到票据时保留安全全根扫描，例如128KiB范围读4MiB对象可产生
32倍正文读取／hash放大；默认32worker不等于32路均走范围快路径。此退化不重建或
落盘sidecar。性能／容量规划须同时采集全账、resident和全扫事件，不能只看缓存命中率。

符合multi-thread runtime、有界HTTP/2窗口及原读预算条件时，gRPC使用单producer／
consumer与两个256KiB固定载荷块，wire帧仍至多64KiB，块归还后才能复用。
首正文前固定唯一核根者：有预算支持的完整Tree（远端proof或本机构建）时由consumer
计算标准根，并与Meta锚及Completion核对；旧Peer无proof不要求重复计算正文根。
Tree不可用时移除root-only builder，由producer（同步入口为原checksum路径）独占完整
核根。模式在Header固定，不在正文中途切换；实际EOF、join、私有输出绑定和最终授权保持。
`afs_dfs_peer_read_events_total{event="root_fallback"}`统计回退选主次数，
`afs_dfs_read_hash_bytes_total{stage="root_fallback"}`仍是另一条回退扫描的字节，不能混用。
合法缓存覆盖组校验是单独工作，不能从正文唯一核根推断总hash等于应用长度。
有完整中部且无别名的至多128个范围可直接读入私有结果，再补齐覆盖组边界校验；
源／输出别名或小范围保持codec。原77,824B非对齐票据与完整校验不变，
这不新增可配置正文RAM缓存；正式收益需要同条件E2E测量。
两块仍按原预算交接，是否重叠以实际阶段观测为准。原4MiB填充额度与本次私有输出不能超过原单批预算；
不满足条件沿用完整对象缓冲回退，不增加队列、连接数、窗口或线程池。

整个当前chunk获授权时才请求整对象proof；partial保持精确授权正文与range checksum，
由A核对本地可信根覆盖组，不宣称B取得独立整对象证明。
旧Peer缺少可选字段时完整根安全回退；格式、长度或对象绑定错误拒绝。
实际EOF、成功join、完整根／CV、身份和最终授权都通过前，不产生Ready或公开输出。
缓存创建／配额／写盘失败仍以同次RPC完成强校验和合法区间交付；Meta／Peer真实错误仍传播。
文件身份变化使证明失效，未请求覆盖组的静默损坏在读取该组时检测。

正常退出等待活动operation结束，停止后不接纳fill；可丢弃缓存不要求fsync，成功应用
fsync依赖原持久副本与Meta屏障。S3原有窗口、载荷／descriptor预算独立保持，外部写不生成
这份原生写proof；外部S3与Peer缓存协同仍后置，不同时配置两种后端缓存。
这些是实现及部署边界，不表示本候选已完成运行、回归或性能验收。

## 原生写 buffer 与 Node allocator

`dfs_write_buffer_idle_bytes` 为 Node 顶层配置，默认134217728（128MiB），允许0..=1TiB；
0禁用空闲保留。它只限制池中可复用的空闲实际容量，不预分配，也不是 live、RSS 或进程内存硬上限。
未命中池时按需申请，最后引用释放才归还，超基线分配释放给 allocator；停止清空池。
该配置不改变单 inode 原16MiB累计接纳阈值、后台写回或 S3 的任何预算上限。
多 inode／慢提交可增加 live；明确分配拒绝尽力返回 ENOMEM 或合法短写，Linux OOM 杀进程
仍按崩溃合同处理，不能保证始终正常返回错误。

Linux Node 默认 feature `node-jemalloc` 安装固定官方 allocator，作用于同一 Node 进程内的 OwnerFs 和 DFS；
AFS Meta 和 ADX 其他进程不受强制替换。依赖许可随 ON 包中的 `third_party/afs-source/NOTICE` 提供。
后台线程在初始化后开启，dirty decay1000ms、muzzy decay0，配置失败启动报错。
`/metrics` 提供写 buffer 与 allocator 的独立统计，allocator每1秒采样；采样失败只使观测无效，
不改变业务行为。GNU Linux 与 musl 的后台线程支持不同，未验证目标不能继承回收结论。

同机制 system allocator 对照仅用于显式 Linux 验证：

```sh
cargo build --locked -p afs --release --bin afs-node
cargo build --locked -p afs --release --no-default-features --features ownerfs,dfs --bin afs-node
```

必须分开保存并核对两个 ELF，不能用后一次构建覆盖前者身份。统一发行仍沿用 ADX 构建入口。
空闲容量基线不能直接当作 RSS 验收值；回收窗口、触页峰值和允许偏差需在测前固定。

## 原生读粗粒度诊断

Node 顶层 `dfs_read_diagnostics_enabled` 默认为 `false`。仅在限定诊断轮显式开启：

```toml
dfs_read_diagnostics_enabled = true
```

使用节点现有 `rest_listen` 地址读取 `GET /v1/diagnostics/dfs-read-timing`；例如 `afs/examples/node.toml` 示例的
`http://127.0.0.1:7501/v1/diagnostics/dfs-read-timing`。未开启时路由返回404。
该入口复用现有管理HTTP监听，不新增鉴权层；监听地址应限于原受控管理网络。

诊断只保留当前进程前256条记录，不轮转、不存正文、路径、inode、用户身份或授权token。
检查 `schema_version`、`overflow`、`clock_failures` 及每项完成／成功状态后再解释数据；
溢出或缺失记录不能用于完整归因。根／outboard工作区间及临时元数据有固定上限，不增加正文缓存或载荷预算。
关闭时不分配诊断journal、不采诊断时钟，仍有轻量状态判断及原有指标，不承诺零指令开销。
正式性能轮关闭该选项，诊断轮单独保存；父子阶段、重叠区间和累计hash耗时不能从E2E直接扣减。
详细边界见[测试说明](../testing/afs.md#原生读粗粒度诊断)。

## Meta 快照升级与回滚边界

OwnerFs 与 DFS 共享 Meta store 的持久化机制，业务记录分开。当前每个持久化批次保存完整权威快照，
DFS 状态增长也会增加共用 Meta 的快照处理成本；本次不改变该机制，不承诺增长成本已解决。

本版本新增 DFS durability policy、外部对象回执等持久化表示。旧二进制可能无法解析新快照；
不能把替换为旧二进制直接当成可用的回滚。升级前应正常停止相关写入、保留并核验原 Meta 和
数据后端的匹配恢复点及版本身份。回滚只能使用与旧版本匹配的完整恢复点，并明确恢复点之后
写入的处理；不得把旧 Meta 快照与另一代对象数据混用。本版本不提供快照降级转换。

## Node 注册租约期限

Meta 注册回复提供实际已持久化租约的相对剩余期限 `lease_remaining_ms`；Node 从请求开始的
单调时刻计算本地截止时间，保守计入建连和应答等待，最长不超过请求的30秒。
绝对到期时间仍供诊断，不用 Node 与 Meta 的墙钟相减决定读授权。启动与心跳使用同一规则。
缺失／耗尽期限、已过期的读授权或 epoch 变化不能恢复旧授权；真实 session 失效仍按原策略
关停 Node，混合模式的 OwnerFs 也会正常排空。本次不引入 DFS 局部故障隔离。

需配套升级 Meta 与 Node。旧 Meta 未返回相对期限时，新 Node 明确拒绝注册授权，
不回退到跨机墙钟或任意30秒授权。Meta persistence readiness 的判定保持独立。
