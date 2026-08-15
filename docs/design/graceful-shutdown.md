# wf-runtime 优雅关闭设计（graceful shutdown）

> 记录 wfusion daemon 在 SIGTERM 后长时间不退的根因与修复，以及对照 warp-parse
> 的退出机制。相关代码在 `crates/wf-runtime`（spawn/sink 在 lifecycle，规则任务在 engine_task）。

## 背景：SIGTERM 后 daemon 5s+ 不退

现象：`wfusion daemon` 收到 SIGTERM 后 5s+ 不退出，被外部 `kill_daemon` 的
SIGKILL 兜底砍掉；此时 TCP listener 已关闭但进程仍满负荷（644% CPU）烧资源，
孤儿化后污染后续测量。

## 根因链

1. **信号链本身是通的**：`lifecycle/signal.rs` 的 `SignalKind::terminate()`
   会 cancel root `CancellationToken` → `Reactor::run()` 控制循环 break →
   `wait()` 逐个 join 任务组。
2. **卡点 A（rule task）**：`engine_task/mod.rs` 的 `run_push_loop` 用
   `tokio::select!{ biased; }`，原先 `rx.recv()` 排在 `cancel.cancelled()`
   之前——push channel 被 ingest burst 灌满时，cancel 分支被饿死；
   `process_batch`（`engine_task/rule_task.rs`）对 batch 内每个 event 做同步
   `machine.advance_at_with`，一个 batch 内不可中断。
3. **卡点 B（大头：sink consumer）**：`alert_task.rs` 原有的
   `run_sink_consumer` **没有 cancel token**，退出只依赖 channel 关闭
   （所有 producer drop）+ **把积压的全部 alert 排空写完**。30M 事件 → 约
   88 万条 alert 写盘，5s+ 排不完。`sink.send_records().await` 写盘是主耗时。
4. rule 的 `flush_alerts` 里 blocking send（`tx.send(batch).await`）在 sink
   慢/关闭时阻塞 rule 的 process，使任何"task 内部预算"都失效。

## 修复（当前实现）

原则：**不在 task 内部做复杂时间预算**（预算依赖 task 回到 select 才能检查，
process 内部的 await 不可中断 → 预算失效）。改为 warp-parse 模式：
**task 只响应 stop 信号（尽力优雅），join 层用 timeout + abort 硬兜底**。

1. `lifecycle/types.rs` `TaskGroup::wait`：整个 group 用**共享 deadline**
   `GROUP_JOIN_TIMEOUT = 3s`，每个 handle 用剩余时间 join；超时
   `handle.abort()` + 记录 aborted 数。整个 group 一个上界，避免逐 handle
   累积。退出时打 `task group {name} shutdown complete: tasks=.. aborted=..`。
2. `lifecycle/alert_task.rs` `run_sink_consumer`：加 `cancel:
   CancellationToken`；cancel 后带 `SINK_DRAIN_BUDGET = 1s` 排空 channel，
   超时 drop 剩余 batch + `sink.stop()`。抽 `dispatch_batch` helper 共享
   正常/关闭路径。
3. `engine_task/mod.rs` `run_push_loop`：`cancel.cancelled()` 提到 biased
   select **第一位**（stop 命令优先，防 channel 满饿死 cancel 分支）。
4. `lifecycle/spawn.rs` `spawn_alert_task` 加 `cancel` 参数，sink consumer 用
   `cancel.child_token()`；`lifecycle/mod.rs` 调用处同步，`Reactor::run()` 的
   `wait()` 后加 `reactor shutdown complete` 汇总日志。

### 明确回退的过度工程

- `drain_push_channel_bounded` / `pull_and_advance_bounded`：rule 内部 drain
  预算——select 只在 process_push 完成后才 poll sleep，process 内部阻塞时
  预算形同虚设，还引入 race。
- `ALERT_SEND_TIMEOUT`：给 flush_alerts 的 blocking send 加超时——治标且
  正常路径也可能误丢，删掉。
- 逐条诊断日志（drain slow batch 等）：定位用，已清理。

## 对照 warp-parse（`wparse/wp-motor`）

退出机制精髓 = **join 层 timeout+abort 硬兜底，task 内部只响应 stop**：

| warp-parse | wf-runtime 对应 |
|---|---|
| `runtime/actor/group.rs` `wait_grace_down_with_timeout`：广播 stop 命令 → 逐个 join `timeout(wait_timeout, handle)`，超时 `h.abort()` | `lifecycle/types.rs` `TaskGroup::wait`：共享 deadline + `handle.abort()` |
| `orchestrator/engine/service.rs` `wait_processing_drained(deadline: Instant)`：drain 带 deadline | （sink 的 `SINK_DRAIN_BUDGET` 预算排空） |
| `force_stop_processing`：`ShutdownCmd::Immediate` 停 Parser/Sink/Infra/Maintainer 所有 role | join 层 abort 等价于 force stop |
| `shutdown_with_signal(policy_kind, initial_signal_received)`：signal 分段优雅 | `wait_for_signal` + `run()` control loop |

## 验证状态与待办

- ✅ 编译通过；`cargo test -p wf-runtime --lib` 140 通过（含 3 个 `TaskGroup` 测试）。
- ✅ 10M（`./bench.sh q1 cont 10m`）：无 SIGKILL、无残留、9800 释放。
- ✅ **30M 已验证（2026-08-15，两轮连续）**：SIGTERM → `reactor shutdown complete`
  ~4.3-4.6s、无 SIGKILL、无残留、9800 释放、30M 全量摄取（EPS 4.8-6.0M）。
  bench.sh `kill_daemon` 宽限已从 5s 调到 10s（3s join + 0.5s abort 确认 +
  unwind/flush 1-2s + 进程拆解 ~0.5-1s，5s 余量不足）。
- ✅ 测试 `wait_aborts_task_that_never_exits` 卡住已修复：paused clock 下
  `advance()` 与 `wait()` 首次 poll 存在竞态（wait 未注册 sleep 时钟就先推进，
  新 sleep 永不被触发）；advance 前加 `yield_now()` 保证确定性。

## 回归记录：join 超时从启动时刻起算，daemon 开机 3s 自杀（2026-08-15 修复）

**现象**：30M cont 跑到 ~26% 摄取即停摆（EPS 78k），日志显示启动 3s 后全部
任务组被 `aborting task`。

**根因**：`watch_group`/`watch_receiver_group` 在**启动时**就调用
`TaskGroup::wait()`，而 `GROUP_JOIN_TIMEOUT`（3s）deadline 从 wait 调用时刻
起算——所有长驻任务在**开机 3s 后**被强制 abort。warp-parse 原设计是
"先广播 stop，再带 timeout join"两阶段，这里漏了第一阶段。receiver 被 abort 后
watcher 误判"输入完成"信号 EOS flush，规则 flush 的告警写入已关闭的 alert 通道
全部丢弃。10M 验证没暴露是因为摄取 <3s 完成，赶在自杀前结束。

**修复（`lifecycle/types.rs`）**：
1. `TaskGroup::wait` 改**两阶段**：cancel 前无时限 join；cancel 后才 arm 共享
   deadline（3s）。新增回归测试 `wait_does_not_abort_before_shutdown`（无 cancel
   时任务活过 3× timeout 不被 abort）。
2. grace 到期后**先一次性 abort 全部剩余 handle，再逐个确认**（原来逐个
   abort+等待，把 N 个任务的 unwind 串行化成 N×500ms）。
3. abort 确认有界（`ABORT_CONFIRM_TIMEOUT = 500ms`）：abort 只在任务下次 yield
   生效，同步批处理/阻塞 syscall 中的任务可能长时间不 yield；进程退出时 runtime
   drop 会回收，不得让关闭流程无限等。

**30M 关闭时间线（修复后）**：SIGTERM → metrics/evictor 秒退 → 3s grace 到期
批量 abort（alert 8 / rules 6 / receiver 9）→ +0.5-1.2s unwind →
`reactor shutdown complete` ≈ 4.3-4.6s。
