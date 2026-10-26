# Sandbox 可观测本地验证记录

验证日期：2026-10-26。实现分支：`feat/sandbox-observability-20261026`。
组件和端到端运行基于 `f7aaee0fc50a2c6a5d8dcf525a59fbac6babf00d` 加本分支实现；
本记录描述本地验证，不表示测试或生产集群已部署。
指标口径见 [Sandbox 指标合同](sandbox-observability.md)。

## PR #125 复核修复验证

基于 `c8f5091` 加本次复核修复，2026-10-26 在独立 worktree 验证：

- 真实 Redis 存储：62 passed，覆盖未发布 Running 后终态归并、同进程重连后第二次事故、
  reconciliation 期间回放去重，以及 CAS 返回完整摘要和删除回放。
- adxlet：139 passed（journal 6、lifecycle 39、pause_resume 54、reconciliation 40），
  覆盖 reload 清理结果不确定时保留意图、后续确认缺失、重连归并和独立 OOM 不被屏蔽。
- mTLS RPC：串行 33 passed，另有 ignored lib 2、background_traces 1 passed。
  默认并行首轮有 Redis 写暂停故障注入超时、Coordinator 启动等待超时各 1 项；
  配置相同证书串行复测全部通过，保留首轮失败日志。
- `make rust-check JOBS=2 ADX_WITH_AFS=0`、文档检查和 diff 检查通过。
- native Prometheus：Coordinator/API Server 各 1 项实际采集及查询通过。

三项原始行为缺陷先通过失败断言复现，再加入正式回归测试。共享 Cargo 缓存曾命中其他
worktree 的旧类型产物；最终验证使用独立缓存，未清理共享缓存。日志与采集证据位于
`out/observability/review-fixes/`。本次未重跑完整 Docker E2E，未部署测试或生产集群；
下文端到端结果及外部 sandboxd 已知失败仍来自初版验证。

## 组件与采集

| 检查 | 结果 |
| --- | --- |
| core/protocol/adxlet/apiserver/coordinator 常规测试 | 565 passed，124 ignored |
| 真实 Redis 存储测试 | 59 passed |
| Coordinator ignored lib/background trace | 2 + 1 passed |
| 真实 Redis + mTLS RPC | 33 passed |
| native Prometheus 实际采集与查询 | Coordinator/API 各 1 项通过 |
| Linux ARM64 managed_mounts | 5 passed |
| Rust fmt/Clippy | `make rust-check JOBS=2 ADX_WITH_AFS=0` 通过 |
| Prometheus 配置与 Grafana 表达式 | 配置与 18 条表达式语法通过 |
| 调度超时 E2E 脚本定向单测 | 1 passed；旧合同断言先失败，修订后通过 |

上述测试类别存在重叠，不相加为一个总测试数。macOS 上完整 all-features 检查受既有
Linux AFS eventfd 依赖限制；未将排除 AFS 的检查描述为完整 all-features 通过。
`promtool check metrics` 报缺少 HELP 文本并退出 3，该 lint 检查未通过。
实际 Prometheus 抓取与 PromQL 查询通过；Grafana UI 未验证。

## 双节点端到端

本地 Docker Desktop Linux ARM64，两个隔离节点，真实 Redis 7.2.5、sandboxd/runc、
安装后的 Python SDK 和从实现工作树统一构建的 ADX debug 二进制。
sandboxd 固定版本为 `efc201531d7e2e9d69505da151eb66084b61eebf`。
Prometheus v3.2.1 每秒采集 API、Coordinator 和两个 adxlet，共 4 个 target。

**18 类场景中 17 类通过，sandboxd 强杀重启失败，两次复现；完整 E2E 未全绿。**

通过：sdk、data-plane、lifecycle、auth、capacity、placement、local-first、node-failure、
runtime-exit、workload-probes、create-response-cut、create-unknown-query、schedule-deadline、
coordinator-restart、redis-restart、restart、stop。
除 stop 关闭服务后不再采集外，通过场景均执行实际指标采集。
所有场景 cleanup_errors 为空，最终检查无本轮残留容器或网络。

| 业务事实 | 实测结果 |
| --- | --- |
| 调度排队 | queued=1，最老等待约 2 秒；释放后归零 |
| 确认调度超时 | 调用 failure=1，stage=scheduling/reason=timeout；队列排空、无 assignment，owned creation=0 |
| 无成功耗时样本 | histogram count=0，P95 为 NaN |
| worker 失联 | 1 次执行中断、1 次 worker 事故；当前影响 1 env / 1 worker |
| 运行期异常与恢复 | runtime_exit=4，recovered=2，unrecoverable=2；当前影响最多 2 env / 1 worker |
| readiness 失败 | 创建 failure=1，stage=readiness/reason=probe_failed；未混入运行中断 |
| 健康探针失败 | probe_failed 中断=2，recovered=1，unrecoverable=1 |
| 响应丢失/unknown 后查询 | 两组各 2 次成功调用、各 1 个成功新建耗时样本，重放未重复采样 |
| Coordinator/Redis 重启 | 统计覆盖起点不变 |
| 正常删除与空闲回收 | 常规场景未增加异常中断累计数 |

响应丢失用例的 unknown 属于客户端视角，API 已完成成功；不能据此宣称验证了 API
unknown 计数。API unknown 分类与成功样本排除由上表 native 采集测试另行验证。
调度超时脚本已对齐确认取消排队后的 `never / not_started` 合同，并在真实链路复验通过。

## 未通过项与边界

`sandboxd-restart` 在新建后不到 1 秒强杀 daemon，重启后原 runtime 消失、退出码 137。
日志与锁定版本源码指向 cgroup 使用关系的持久化窗口：使用集合每 5 秒保存，恢复时磁盘
使用记录为空但实际 cgroup 仍存在，未登记的 cgroup 进入清理路径并被杀掉。
独立复测再次出现相同结果。不能仅凭 137 归为 OOM；本改动不包含外部 sandboxd 修复。

本轮未验证真实内核 OOM、runsc/Firecracker、KVM checkpoint、生产网络与生产规模。
真实 worker 失联覆盖一个运行 env，多对象批量失联仍需专项验证。
直接恢复 Running 保留旧异常、正常操作意图竞态与重复提交去重有组件测试，不能视为
这些路径已全部完成真实故障注入。当前影响摘要不提供任意历史窗口完整去重。

本地完整日志、原始 scrape、PromQL、影响范围和包身份保存在执行者工作树的
`out/observability/`，端到端机器可读汇总为 `e2e/regression-summary.json`。
这些输出不随仓库发布；本页提供可审阅的验证范围和结果摘要。
