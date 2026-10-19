# AKernel / sandboxd 回归 — 2026-10-19


后续公共对账拆分及最终包回归见 [周期对账修改记录](periodic-comparison-20261019.md)。
本文保留初轮部署身份、失败及对照结果。

当前 Jiuwenbox 工作树的公共对账改动完成 Linux 真实 AKernel SDK 回归，**尚未全绿**。
runsc 为 25/26 组通过；runc 的适用用例为 10/11 组通过，另有 7/7 项基础运行时测试通过。
两种运行时均出现短命令 PTY 连续建连失败，这是本轮需要处理的回退。
runc 原始执行的 12 组还包含本就不支持的 ACL 正向用例；该项不适用，不计为新增回归。
本轮只调整验收镜像和夹具，未修改 ADX、AKernel、sandboxd、Jiuwenbox 或 Conch 生产源码。

## 身份与部署

验收主机为 `47.83.171.126`，Ubuntu 24.04.4 / Linux 6.8.0-137-generic / x86_64。
使用独立的 `akernel-node` 容器与私有数据目录，控制面端口 50443、数据面端口 50080。
原有 `akernel-adx152-standalone-20261013` 始终保持运行。

| 输入 | 本次固定值 |
|---|---|
| ADX | `feat/jiuwenbox-runtime-p0@e6bb99a1b53940edff6f9d1441f653826288c632` 加当前未提交修改 |
| ADX 源码快照 SHA256 | `93196e99e320e28ca9f23b276ef8f5ba7b5f7685414087d7fed932fd0071b4bc` |
| ADX 包 manifest SHA256 | `3ff1cd2e01843b2a06ec62631762b0e63c83dff232121d7dd2f717f50540af23` |
| AKernel SDK 与用例 | `develop@b686cf025074284c93b643db07eba1b4d1a0a9b5`，`akernel-sdk==0.2.0rc1` |
| 容器基础镜像源码 | AKernel `9cd50bd3aa4ddff80fb1c6e13779bc2d4cb1fde4` |
| sandboxd | `02386fea2050c4a77cfaa9df5edfe72fd35fc987`，与最新 AKernel develop 的 gitlink 相同 |
| gVisor / runc | `release-20260817.0-akernel.4` / `1.5.1` |
| 当前 ADX SDK | `adx-environment==0.1.0rc1`，使用本轮完整包中的 wheel |
| Python WebSocket 依赖 | `websockets==17.2`，当前、旧包与长周期对照共用同一客户端环境 |
| 验收镜像 | `akernel-local/adx-jbx-sandboxd:20261019-erofs17` |
| 验收镜像 ID | `sha256:5179fa629531bbb89ab463a1e0cbe79aabab1db25c10772626004e58ae4d214a` |
| AKernel runtime EROFS SHA256 | `f539222b3d313a250453a355629d29a0ec1e8a6987b2dc8a36a0fe897b9c14b9` |

复用已有 AKernel 容器依赖并覆盖完整 ADX 包，保留其原有 Ubuntu runtime rootfs 内容、
替换为同包 Execd 后重打 EROFS。该 AKernel runtime EROFS 与 ADX 包内的最小 EROFS 是两个产物。
ADX 为 Linux GNU debug 构建，打包副本已去除 debug 符号；所有包文件和运行中的
sandboxd、Coordinator、adxlet、API Server 二进制逐一校验 SHA256。
默认部署使用嵌入式 Relay / Ingress，后两者分别由 adxlet / API Server 承载。
AKernel SDK 安装到独立 venv，正式回归按仓库原有 runner 执行，并通过 PYTHONPATH 加载该版本 SDK 源码。

## 正式回归结果

| 运行时 / 层次 | 组结果 | unittest 计数 | 跳过 |
|---|---:|---:|---:|
| runsc standalone | 12/13 组通过 | 包含在下方合计内 | 0 |
| runsc Full 单节点适用组 | 12/12 组通过 | 包含在下方合计内 | 0 |
| runsc 进程强杀恢复 | 1/1 组通过 | 2 项通过 | 0 |
| runsc 合计 | **25/26 组通过** | 59 项，58 通过、1 错误；另有 4 个脚本组 | 0 |
| runc standalone 适用组 | 8/9 组通过；原始执行为 8/10 | 包含在下方合计内 | 0 |
| runc 进程强杀恢复与删除故障 | 2/2 组通过 | 3 项通过 | 0 |
| runc runner 合计 | **10/11 适用组通过**；原始执行为 10/12 | 原始 31 项，29 通过、2 错误，其中 ACL 不适用 | 0 |
| runc 基础运行时类，独立执行 | 7/7 项通过 | 7 项通过 | 0 |

通过项覆盖创建、幂等删除、命令退出码和后台进程、文件读写/目录复制/二进制摘要、普通 PTY、
端口访问与同端口隔离、反向隧道。runsc 还覆盖动态网络策略及矩阵、存储配额、现有 checkpoint/reload、
OCI/Dockerfile、创建结果未知、真实 TCP 应答丢失、并发同名创建、Redis 生命周期、租户隔离、
密钥生命周期、准入和调度维护。两个进程恢复测试均检查原 Sandbox ID、卷内 marker 与命令可用性。
sandboxd 使用运行稳定 25 秒后的 SIGKILL；adxlet 也通过 SIGKILL 触发恢复。
runc 删除故障用例通过真实 runc 的单次失败注入验证保留资源归属并自动重试清理。

两种运行时另有同一项定向验证：持续输出 16 秒，跨越默认 5 秒的多轮对账，再读取并校验
16 MiB 二进制文件；均通过，文件 SHA256 为
`341aacac661ccb210720bedaa9ead5d668fe5ea41a73532fc147c71e34040df1`。
这证明被测长流可存活，不能代替连续新建 PTY 会话的验证。

## 需要处理的结果

### 1. 连续短命令 PTY 与周期性对账相关的回退

失败用例为 `tests.e2e.standalone.test_pty_lifecycle.PtyLifecycleIntegrationTest.test_completed_pty_sessions_do_not_accumulate`。
原用例连续创建 100 个 `/bin/sh -c 'exit 7'` PTY，逐次等待退出码 7，并检查客户端会话表清空。
当前 runsc 和 runc 均在 `session.wait(timeout=10)` 提前报错：

```text
PTY WebSocket connection failed: no close frame received or sent
```

定向对照使用相同 AKernel SDK、ADX SDK、WebSocket 依赖、sandboxd 二进制及 runsc：

| 对照 | 100 会话用例重复结果 | 耗时 |
|---|---|---|
| 当前 ADX 包，默认 5 秒对账 | **0/3 通过**；每次提前失败 | 约 1.77 / 4.97 / 5.02 秒 |
| 原 AKernel 镜像中的旧 ADX `af7d82f04a52d4604ea7ca4c550f09c6114dee4d`，clean | **3/3 通过**；共完整完成 300 次会话 | 约 26.45 秒/轮 |
| 同一当前 ADX 二进制，对账周期设为 300 秒的诊断夹具 | **3/3 通过**；共完整完成 300 次会话 | 约 26.65 / 26.80 / 26.80 秒 |

长周期夹具同步延长 Coordinator 心跳超时和容量有效期至 3600 秒，以保证配置有效；
80 秒左右的用例窗口未覆盖下一次周期性对账。因长周期也延长启动连接重试，夹具在 Coordinator
监听后重启一次尚未注册的 adxlet，再等待正式 readiness 和身份校验后开始用例。
它是定位对照，不能算默认配置下的修复或验收通过。

结果指向周期性对账期间的连接处理，具体的关闭位置尚未定位。旧包与当前包之间还有其他代码差异，
不能仅凭旧包对照把问题归到某一个函数。默认 5 秒配置下的失败保留为本轮回归阻塞项；
尚未修改生产代码，也未用长周期结果覆盖首轮失败。

### 2. runc 的 ACL 能力限制

`network-policy` 组在动态配置 block 策略时失败。固定 sandboxd 的
`internal/server/server.go` 明确拒绝 runc 的创建期和动态 ACL，返回 FailedPrecondition：

```text
network ACL is not supported by runtime runc
```

这是当前 sandboxd/runc 的既有能力限制。将 ACL 正向用例放入 runc 的验收范围不合适，
应从适用用例中排除；原始失败日志和计数继续保留，不计为新增回归。ADX 目前将此动态配置错误包装为
HTTP 503，客户端重试三次后抛错；不能把该用例记为通过，也不应把它当成网络策略已生效。
runc 的配额、checkpoint 以及硬编码默认 runsc 的网络策略示例未纳入 runc 的正向验收。

## 清理与证据

正式 runsc、runc 及三组 PTY 对照结束后均确认：sandboxd 实例清单为空，Redis
`states={}`、`held_ids=[]`、`failed_held_ids=[]`。任务容器已移除，原有受保护容器仍运行。
正式回归的 `cleanup.exit_code=1` 反映用例失败；容器删除和保护容器检查分别有独立证据。
未将日志、镜像缓存或合法共享 EROFS 挂载当成未清理的沙箱。

本地证据目录为 `out/akernel-sandboxd-20261019/`，远端为
`47.83.171.126:/root/adx-akernel-sandboxd-20261019/`：

- `runsc-03/`、`runc-02/`：各组日志、summary、进程重启事件、删除故障证据、长流/文件校验及清理记录。
- `pty-current-01/`、`pty-baseline-01/`、`pty-slow-02/`：三种对照的逐次日志、身份和 focus-summary。
- `regression-summary.json`：结果汇总、实际容器审计、配置周期和运行二进制摘要比对。
- `evidence.tar.gz`、`evidence-audit.json`：排除私有配置和密钥后的证据包及 SHA256。
- `pip-freeze.txt`、`image-erofs17.json`、包和源码清单：产物与客户端身份。

202 个文件的证据包已回传本地并重新校验，SHA256 为
`d8e3072f298ddd14f43563731a28b6bfd0ef298e80c62514a394abad021390d4`。
收集时核对 13 个实际 secret 值，所选证据文件未包含这些值或 PEM 私钥。

先前准备失败的证据单独保留：`runsc-01` 的校验脚本错误要求独立 Relay/Ingress；
`runsc-02` 的 EROFS 使用 1.9 工具生成，runsc 拒绝不兼容特性位 `0x1`。
改用 AKernel 原有 erofs-utils 1.7.1 后特性位为 `0x0`，再完整执行 `runsc-03`。
第一轮长周期夹具也因启动重试等待超过 readiness 预算而失败，未运行 PTY，不计入对照成绩。

本轮覆盖 Linux 单机 sandboxd 的 runsc / runc 真实后端；Kata、Firecracker、GPU、S3、
跨节点和宿主重启未执行。Jiuwenbox / Conch 的快照不属于本轮范围。
