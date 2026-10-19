# Jiuwenbox 检视修改验证 — 2026-10-19

基线为 `feat/jiuwenbox-runtime-p0@e6bb99a`。本次修改仅在 ADX 内实现：

- 移除 Jiuwenbox 所有权 SQLite，使用进程内协调状态；恢复关联来自 Coordinator 完整权威目录。
- 每个节点上报周期执行对账；未知 ADX 资源及失败创建的晚到资源持续清理，删除失败后重试。
- 资源发现包含 Jiuwenbox 注册表、Conch 原生列表与 supervisor cgroup/netns。
- 网络使用 Jiuwenbox 原生 ACL 创建/更新接口；无法保留 ADX 语义的配置明确返回 Invalid。
- 独立 Execd EROFS 首次校验并挂载后持续复用，环境归零时保留挂载。

## 已执行检查

日志位于本 worktree 的 `out/jiuwenbox-review-20261019/`。

- 修改前的两个新增合同测试均失败：未知实例使 inventory 报错；驱动仍创建 ownership.sqlite。
  对应 `red-orphans.log`、`red-mount.log`。
- macOS 定向测试 80/80 通过：Jiuwenbox runtime 23、network 4、policy 2、
  reconciliation 12、sandboxd_rpc 39。对应 `focused-03.log`。
- `cargo clippy -p adxlet --all-targets --all-features -j 2 -- -D warnings` 通过，
  对应 `clippy-adxlet-05.log`。
- 完整 macOS ADXlet 测试尝试在 3 项既有 managed_mounts 测试失败：该路径依赖
  Linux `/proc/self/fd`，本轮没有修改 managed_mounts。对应 `adxlet-02.log`。
- macOS workspace all-features Clippy 在 fuser 构建阶段缺少 pkg-config/FUSE，
  对应 `clippy-03.log`；不能把组件 Clippy 通过当作 workspace gate 通过。

前一轮 Linux arm64 容器（Rust 1.95.0）中，`cargo test --locked -p adxlet -j 2`
完整 249 项测试通过，0 失败；随后
`cargo clippy --locked --workspace --all-targets --all-features -j 2 -- -D warnings`
通过。对应 `linux-gates-01.log`。构建镜像为本地 `agentos-rust-check:local`，
目标缓存为独立 Docker volume `adx-jiuwenbox-review-linux-target`。

`cargo fmt --all -- --check`、`python3 build/docs/check.py` 及 `git diff --check`
通过；文档检查覆盖 153 份文档、742 个本地链接、45 个 JSON 示例，无错误。
architecture Markdown 对应 HTML 已重新生成。

## 公共周期性权威对账

后续检视将周期触发提升至 ADXlet 公共 `node_reconciliation::reconcile_node`，
启动与每个节点上报周期共用相同流程；移除 RuntimeDriver/RuntimeRouter/Jiuwenbox
的后端专用周期开关。成功后本轮立即发布 ready，清理耗时期间继续心跳。
目录不可用时不虚构空目录；获取新目录前的临时通信失败保留已有控制器的降级操作路径，
同时禁止新分配。部分对账失败、节点会话失效或 ready 上报失败时保持生命周期门禁关闭。
没有新增本地持久化。

本轮日志位于 `out/jiuwenbox-public-reconcile-20261019/`。保留后端特判时，新增的
6 项公共流程测试全部失败（`red.log`）；删除特判后 6 项通过（`green-01.log`）。
后续增加 sandboxd、supervisor、Conch 三种适配路径的孤儿/晚到创建测试，以及 RPC 超时、
清理失败重试、既有降级行为和有效执行保留测试。

- macOS 定向测试 86/86 通过：Jiuwenbox runtime 24、公共对账 7、
  reconciliation 14、sandboxd_rpc 41。对应 `focused-04.log`。
- Linux arm64 容器中完整 ADXlet 测试 260/260 通过；workspace 全 targets/features
  Clippy 通过。对应 `linux-gates-02.log`。
- 最后补充恢复入口的门禁后，Linux reconciliation 14/14 再次通过，workspace
  全 targets/features Clippy 再次通过。对应 `linux-final.log`。
- 格式、文档和 diff 检查通过，文档检查覆盖 153 份文档、742 个本地链接及
  45 个 JSON 示例。对应 `fmt-01.log`、`docs-01.log`。

## 验证边界

HTTP/UDS 组件测试使用真实连接，宿主挂载、cgroup 和 VM 明确替换为测试实现。
覆盖无本地数据库的权威目录恢复、未发布实例回收、后端重启、晚到实例、删除失败重试、
原生策略更新与不支持规则的拒绝，以及共享挂载保留。

验收脚本已同步使用原生资源审计与测试提供的权威目录，不再读取所有权 SQLite；
宿主审计把保留的 Execd EROFS loop 与孤儿实例分开记录。Python 语法检查通过。
后续 [预编译产物验收](jiuwenbox-prebuilt-20261019.md) 已针对这些修改及实际 API 契约修正
重新完成真实 Linux supervisor/Conch、完整部署与公开 SDK 验收。宿主重启及 Redis 故障
场景仍是修改前版本的证据，未延用其通过结论。
