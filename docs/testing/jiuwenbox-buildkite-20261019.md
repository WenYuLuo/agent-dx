# Jiuwenbox supervisor 验收记录 — 2026-10-19

本文保留手工验收与 Buildkite 试验记录。当前 package pipeline 已撤下 Jiuwenbox
冒烟入口，独立运行方式见 [Supervisor SDK smoke](../../build/e2e/jiuwenbox/README.md#supervisor-sdk-smoke)。
Jiuwenbox 继续作为固定摘要的外部依赖。

## 正式 CI 跟进

以下原始本地验收记录已随 `efaf577` 提交。随后已推送同一分支并执行正式 CI：

- [#163](https://buildkite.com/agent-dx/agent-dx/builds/163)：ARM secret 权限失败，
  Linux builder 长期 reserved；取消该轮后，`3a48934` 恢复 #162 已验证的固定 builder，
  后续构建显式设置 `ADX_ARM_BUILD=0`。
- [#164](https://buildkite.com/agent-dx/agent-dx/builds/164)：x86 构建和组装通过；
  supervisor 在 Python 镜像的 `git` 缺失处失败。`9b4acab` 将工具安装移至 checkout 检查前，
  缺 Git 的回归用例已先失败、后通过。
- [#165](https://buildkite.com/agent-dx/agent-dx/builds/165)：构建、发布、镜像和 Kubernetes
  E2E 均通过；supervisor 在本仓库夹具的 v2-only preflight 失败，未执行 SDK case。
  原始 wheel 的 `supervisor/cgroup.py` 已核对：`detect_backend` 同时支持 v1/v2；
  该轮没有证明后端不支持当前 worker。夹具检查与两种层级的进程/孤儿审计均已修正。

pip 改用华为云 HTTPS 镜像，固定五项依赖及当前两个大依赖均有对应版本。
同一 `cryptography-50.0.2` Linux wheel（4,752,576 bytes）本机镜像下载 1.157 秒，
SHA256 `9dab55f57c74c3cad24c323bacbbd04be4705ba6eb0d92e920b1fc4837ed5079` 与镜像索引一致。
这是本机镜像连通性/完整性证据，CI 速度和 supervisor 通过结果仍待新提交重跑。
服务端和 SDK 的 pip 缓存放在既有 `/mnt/paas/build-cache/adx/pip/jiuwenbox`。

[#166](https://buildkite.com/agent-dx/agent-dx/builds/166) 已证明镜像源在 CI 生效：
pydantic_core 下载 71.9 MB/s、cryptography 106.4 MB/s，且 cgroup v1 前置检查通过。
后续 ADXlet 未就绪，日志明确为 EROFS `failed to setup loop device`，SDK cases 仍为空。
Pod 原先没有显式映射宿主设备目录；已补充 `/dev` hostPath，并在下载前运行只读
`losetup --find` 检查，实际 EROFS 挂载及 SDK 结果仍需修正后的提交重跑。
后续 [#167](https://buildkite.com/agent-dx/agent-dx/builds/167) 通过 loop 设备预检查及包校验，
SDK 阶段仍失败且 `cases=[]`，没有形成公开 SDK 通过结果。该轮已确认 canceled。本阶段结束 CI 试验，
package pipeline 的 step 与专用 bootstrap wrapper 已移除，保留手工验收工具及全部原始记录。
Jiuwenbox archive/wheel 的固定摘要保持原样。

## 试验期间的接入行为

- `.buildkite/jiuwenbox-smoke.sh` 限定 clean checkout 和当前 `BUILDKITE_COMMIT`，
  通过当前 `BUILDKITE_BUILD_ID` 下载 `platform-build` 包及 checksum；校验后部署。
- `prepare_supervisor.py` 使用 `prebuilt-lock.json` 固定的原始 JiuwenSwarm archive/wheel，
  对缓存和下载均校验 SHA256 与大小，创建独立服务端 venv；上游 wheel 内容保持原样。
  上游 daily URL 内容变化时校验失败，不自动采用新版本。
- `smoke.py` 校验包 manifest、commit、target、clean identity，安装包内公开 SDK wheel，
  调用 `full_sdk.py --runtime supervisor`。原有不带 `--runtime` 的完整验收仍默认覆盖两个后端。
- Execd 使用发布包内独立 EROFS。环境删除后先检查共享挂载仍保留，最后销毁本 job
  的临时节点时才卸载这个私有 fixture 的精确挂载点。
- 必须通过八个 case、SDK 文件校验、Jiuwenbox 文件前后校验、原生资源审计和 fixture 清理。
  导出 summary、JUnit、审计及脱敏日志；密钥、渲染配置、venv 和后端状态不在 artifact glob 内。

## 已执行验证

在 Linux 开发机的独立 privileged Ubuntu 24.04.5 容器执行，使用 host cgroup namespace
和可写 cgroup v2 挂载；网络使用该容器的独立 namespace，无宿主端口映射。新后端 venv
未安装 Conch SDK。测试部署独立 ADX 进程和 Redis，通过公开安装后的 SDK 操作：

| Case | 结果 |
| --- | --- |
| supervisor-create-exec-files-erofs | PASS：创建、exec、二进制/UTF-8 文件及独立只读 EROFS |
| adxlet-restart | PASS |
| coordinator-restart | PASS |
| apiserver-restart | PASS |
| ingress-restart | PASS |
| jiuwenbox-restart | PASS |
| supervisor-delete | PASS |
| native-resource-audit | PASS |

最终 `smoke002/summary.json`：`passed=true`、8/8、`error=null`，SDK wheel 21 个文件
一致；Jiuwenbox 47 个包内文件及 2 个锁定产物前后校验一致。host/runtime 的
`leftovers=[]`，fixture 挂载和私有目录清理结果均为 true；本次容器退出后已删除。

最终 Linux 脚本测试共 **35/35**：新增 smoke 单测 10、既有 pipelines 测试 9、
pipeline contracts 测试 16。远端的九个关键脚本/配置与本地 SHA256 全部一致。

CI 配置镜像为既有 `adx-python-build`：
`sha256:12afbd81cde9ec5931c9db8aaf955720fdfd1e1e267545a44430ce79a4d81854`。
开发机拉取该 SWR 镜像被鉴权拒绝，因此实际容器验证使用本地既有 Ubuntu 镜像
`akernel-local/adx-runc-pty:20261019-fixed`，镜像 ID 为
`sha256:403fdc67f0bcdba9c9767b7d639cab2c8b23dcb67bfa261f27d1a8333e70d942`，
显式使用 Bash entrypoint。正式 CI 镜像、Kubernetes worker 的内核/准入条件以及
Buildkite 当前构建产物下载链路仍需一次提交后的 CI 验证。

## 固定产物与本次包身份

| 产物 | 版本或 SHA256 |
| --- | --- |
| JiuwenSwarm wheel | 版本 `1`；`37f8ef116ffc46cf1753a5388e44a02aebffee7b8644d075398895a73fb94fbe` |
| JiuwenSwarm daily archive | `d8960ecd5ee06187686b6a038dfb99517990a936535e2828c2b1d6bc6a1aa004` |
| prebuilt-lock.json | `f5417ced9a186768c0b67fcfa7fe76e75c061885ed529fc13444b6f7f261107d` |
| ADX package manifest | `254dcb9bb9279aacd42bc11ffbe421cb8845f905ff77ceb4f9c47c0ac094ddeb` |
| SDK wheel | `0.1.0rc1`；`9ce96591e1f3b1b7c10e47390ef6dccd4d5df6e3346c9d9ab33904643125f71a` |
| Execd | `65986e4419ff533aaab0fbeee91e4a8812b6519d0af27f1deaad3928bfb8fd1b` |
| Execd EROFS | `b177a7fa2ee5147aa2cb41dd29ac4031d4c42812c5fc66cef4babd92d46cfd8a` |

真实运行验证复用了此前完整验收的 dirty 发布包，因此手动运行未提供 `--commit`，
原始 dirty 身份保留在 summary。Buildkite wrapper 总是传入 `--commit` 并拒绝 dirty 包，
这一拒绝路径有单测。运行验证证明 supervisor 脚本和原始 wheel 可执行；新 commit 的发布包
仍由正式 CI 构建并验收。

## 失败记录与证据

保留全部尝试日志：`container001.log` 为 SWR 鉴权失败；`container002.log` 为开发机
磁盘空间导致 apt 校验失败，随后为本次容器配置独立 tmpfs apt 缓存和临时目录；
`container003.log` 进入运行时但 pinned policy 引用的 `/etc/pki` 在 Ubuntu 中缺失，
由 CI bootstrap 创建该目录后，`container004.log` 8/8 通过。没有改 wheel 或上游 policy。
首个运行失败的 `smoke001/` 也保留在脱敏证据内，其审计无残留。

本地证据位于 `out/jiuwenbox-ci-20261019/supervisor-evidence.tar.gz`，63 个文件、25,377 bytes，
SHA256 为 `548f3bed685f65a78e73d8f60aaa340984c6c2ae962ea159b5176ebc1514168a`。
对应 `supervisor-evidence-audit.json` 和 `supervisor-code-identity.json` 已下载并复验。
远端工作目录为 `/root/adx-jbx-supervisor-ci-20261019`；失败 fixture 的私有诊断目录保留在
该任务目录中，不包含在公开证据归档内。

本次没有重跑宿主机重启、Redis 故障或快照验收；相关既有报告保留各自的验证边界。
