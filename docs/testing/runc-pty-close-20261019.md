# runc PTY 断连诊断与回归 — 2026-10-19

本轮基于 `feat/jiuwenbox-runtime-p0@e6bb99a` 的已有未提交工作树，继续定位
[公共对账拆分回归](periodic-comparison-20261019.md) 中剩余的 runc PTY 断连。

## 先复现

远端 `47.83.171.126:/root/adx-runc-pty-20261019/` 保存本轮证据。
旧包 manifest 为 `ff2d6d7f…`，镜像 `sha256:a369a693…`，节点周期为原默认 5 秒。
公开安装 SDK 和 AKernel 用例保持原内容，串行重复
`PtyLifecycleIntegrationTest.test_completed_pty_sessions_do_not_accumulate`：
前三轮各 100 个通过，第四轮 7.423 秒出现
`PTY WebSocket connection failed: no close frame received or sent`。
Environment 在清理删除前一直 Running。

`reproduce-runc-01/` 保留首次失败。开启 WebSocket DEBUG 的独立诊断
`diagnose-runc-01/` 为 12/12 通过；该带额外日志的结果不能代替原失败。

## 根因证据

Execd 在子进程退出后等待最后输出，接着发送 `exited`、Close，立即释放 TCP。
SDK 在连接建立后会发送首个 resize；若子进程先退出，这个输入可能在最后输出等待期间
到达而未被读取。Linux 关闭有未读输入的 TCP socket 会发送 RST。
Relay 因 TCP reset 结束 H2 CONNECT，已经写出的终态帧可能尚未到达 SDK。

在旧镜像中仅把首个 resize 的到达推迟 100 ms，未修改命令、服务、SDK 包或对账周期。
`late-resize-runc-01/` 第一个 100 次用例在第 21 个会话失败；
`causal-runc-01/` 前三轮通过，第四轮在第 27 个会话失败，抓包完整包含最后失败流：

- 会话 `pty-7-27`；Guest `10.88.0.10:50090`，Relay 端口 `53948`。
- `1792403030.424902`：客户端 resize 到达 Guest。
- `1792403030.535605`：Guest 发出 `exited(exit_code=7)`。
- `1792403030.535620`：Guest 发出 Close。
- `1792403030.535679`：Guest 发出 TCP RST；无客户端 Close 应答。
- 同一顺序测试的公开 SDK 返回上述断连错误，且后端环境仍运行。

抓包无丢包。`causal-runc-01/pcap-summary.json` 保存经过过滤的帧时间与类型；
原始 pcap 包含内部请求头，仅保留在远端私有任务目录，不作为公开交付文件。
这些证据把问题定位到 PTY 关闭流程；对账周期无需作为规避参数调整。

真实 TCP 单测先复现同一边界：晚到 resize 后客户端关闭握手得到 Linux
`ConnectionReset`（errno 104），`red-late-resize.log` 为行为 RED。

## 修改

仅调整 Execd PTY 连接结束：先发送终态和 Close，再读取到客户端 Close 应答后释放 TCP；
没有应答时最多保留 5 秒。客户端先 Close 的分支也 flush 自动应答。
HTTP 升级结束仍退出整个连接，不回到 HTTP keep-alive。

新增真实 TCP 回归覆盖晚到 resize、终态与 Close 后正常 EOF，以及不应答 Close 的超时回收。
原有输出测试改用 `/bin/sh -c` 执行同一命令，避免读取本次宿主仅兼容 Bash 的登录 profile；
原输出和退出码断言保留。没有更改公开 SDK 的等待或关闭超时。

Jiuwenbox、Conch、sandboxd、ADXlet、Coordinator、Gateway 和 SDK 均保持原内容。
新包仅替换静态 Execd 和其独立 EROFS；AKernel 运行时 rootfs 内的 Execd 同步更新。

## 构建验证

- Linux Execd lib：97/97，包含晚到输入、TCP EOF、无 Close 应答的超时回收。
- Workspace 全 targets/features Clippy `-D warnings` 通过。
- 单线程与默认并行两种配置均为 97/97，0 忽略；默认并行耗时 5.29 秒。
- 最初全 lib 暴露登录 profile 语法错误及一次监听器恢复用例短超时，失败日志保留。
  修正测试命令后，默认并行复跑时监听器恢复用例也通过。

构建日志：本地 `out/runc-pty-20261019/linux-green-build-03.log`；
默认并行日志：远端 `lib-default-parallel.log`。
静态 Execd 为 `x86_64-unknown-linux-musl` release，其余 ADX 服务复用上一轮固定包。

| 产物 | SHA256 |
|---|---|
| Execd | `65986e4419ff533aaab0fbeee91e4a8812b6519d0af27f1deaad3928bfb8fd1b` |
| 独立 Execd EROFS | `b177a7fa2ee5147aa2cb41dd29ac4031d4c42812c5fc66cef4babd92d46cfd8a` |
| AKernel 运行时 rootfs | `65b21aad33fc38857798cae79e327fa71ed6575d5d7d2c011a8ad904cde66692` |
| 包 manifest | `254dcb9bb9279aacd42bc11ffbe421cb8845f905ff77ceb4f9c47c0ac094ddeb` |
| 构建源码 manifest | `9a4b01aa89e09f3dfdbd0f0009034c3638e94752ebfb319dd9172bdd27996524` |

镜像 `akernel-local/adx-runc-pty:20261019-fixed`：
`sha256:403fdc67f0bcdba9c9767b7d639cab2c8b23dcb67bfa261f27d1a8333e70d942`。
源码清单校验 1229 文件，包校验 34 文件；构建之后补写当前文档与 API 关闭语义说明。

AKernel 验收工作树为 `/root/akernel-worktrees/adx-jbx-sandboxd-20261019`，
SDK 源码固定 `b686cf025074284c93b643db07eba1b4d1a0a9b5`；sandboxd 固定
`02386fea2050c4a77cfaa9df5edfe72fd35fc987`，运行二进制 SHA256
`ffa46d926400065dee4958bc88e659868cb78b8d25b95c869b62dd859eb019fd`。
Jiuwenbox/Conch 的外部完整性核对仍为 8 个固定缓存文件、55 个 wheel 文件、35 个 RPM 文件
均保持原样，原共享 EROFS 继续为 `dd787079…`。

## 真实 sandboxd 回归

验收使用原默认 5 秒节点周期与原安装 SDK，最终固定镜像为上述 `403fdc67…`。

| 后端 | 默认连续 PTY | 100 ms 晚到 resize | 关闭抓包 | 长 PTY | E2E |
|---|---|---|---|---|---|
| runc | 12×100 全过 | 3×100 全过 | 300 流，零 RST | 21.86 秒、21 行、exit 7 | 11/11 适用组 |
| runsc | 3×100 全过 | 1×100 全过 | 100 流，零 RST | 21.83 秒、21 行、exit 7 | 18/18 组 |

runc 的基础运行时集成另为 7/7；runsc 的最终 E2E 为 45 项 unittest、0 跳过。
两种后端的 16 秒流、16 MiB 文件校验、进程重启，以及 runc 删除失败重试均通过。
最终 driver 均退出 0，Redis states/held_ids 及后端执行清单均为空；
自有容器已删除，既有 protected 部署保持运行。

真实执行文件摘要也经过校验：runc 首轮通过宿主进程视图核对；runsc 最终通过公开 SDK
在 guest 内枚举 `/proc/*/exe` 并读取摘要，均为 `65986e44…`。

首轮 runsc 的整体脚本返回 1：长 PTY 前置夹具试图从宿主 `/proc` 查找 Execd，
在 runsc 进程视图下得到空集合，因此未执行该长 PTY。该轮其余 18/18 组与 PTY 压力均通过。
修正夹具后，保持产品代码、SDK 和镜像不变，完整重跑 runsc，取得上述整体退出 0 的结果。
首轮证据 `accept-runsc-01/` 保留，最终采用 `accept-runsc-02/`；runc 采用 `accept-runc-01/`。

本轮覆盖 Linux x86_64 单机进程/容器。runc 原生不支持的 ACL 不列入正向适用组。

## 交付

本地 `out/runc-pty-20261019/` 保存构建日志、检视补丁 `runc-pty-only.patch`、源码清单及
脱敏 Linux 证据包。完整验收、默认并行 lib 测试和初始失败日志分别保留，原始 pcap 留在
远端 `private-pcaps/`。临时编译缓存和运行 fixture 使用任务专属 tmpfs，收尾时卸载；
固定包、镜像与脱敏证据保留。

脱敏证据包 `out/runc-pty-20261019/linux-evidence.tar.gz` 的 SHA256：
`9503fefecacbe1dba049ab742c664af43867cb26ccd62138abc4c52ef242301f`。
`artifact-cleanup.json` 确认本任务 9 处 tmpfs 已卸载、既有部署仍运行。

改动保留在原 worktree，尚未提交。构建之后仅更新文档，Rust 文件与构建源码清单一致。
