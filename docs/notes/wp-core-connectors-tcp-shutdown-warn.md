# wp-core-connectors: TCP source 停机误报 WARN + 停机路径触发无谓重连

> **已提交**：https://github.com/wp-labs/wp-core-connectors/issues/22
>
> 来源：在 `warp-fusion/examples/wp-pipeline/streaming_ipv6/run.sh` 中观察到，单独立项给上游
> `wp-core-connectors`（本文基于 0.8.4 源码；行号为该版本）。

## 问题摘要

优雅停机时，TCP source 会打出：

```
WARN TCP source 'nginx_tcp' connection channel closed; no further connectors will arrive
```

这条日志描述的是**监听循环已停**（即「不再接受新连接」的正常终止信号），却以 WARN 级别、
且措辞像故障输出。同时 `receive()` 在此情况下返回可重连类错误，导致停机期间多一次重试、
再多打一条同样的 WARN（实测两条相隔 5s）。

## 复现

`warp-fusion/examples/wp-pipeline/streaming_ipv6/run.sh`（wfusion daemon + wparse daemon，
wparse 的 arrow_tcp sink 连到 wfusion 的 `nginx_tcp` :9802），脚本 kill 掉 wfusion 时出现：

```
2026-09-19T09:58:53.688752Z WARN TCP source 'nginx_tcp' connection channel closed; no further connectors will arrive  log.file=.../wp-core-connectors-0.8.4/src/sources/tcp/source.rs log.line=305
2026-09-19T09:58:58.691689Z WARN TCP source 'nginx_tcp' connection channel closed; no further connectors will arrive  log.file=... log.line=305
```

## 根因

1. **WARN 的实际语义 = 监听器停机**，与客户端断开无关：
   - `sources/tcp/source.rs:292` `wait_for_connection()` 在 `connection_rx.recv()` 返回 `None`
     时打这条 WARN（`:305`）。该通道由 acceptor 的监听循环持有。
   - `sources/tcp/acceptor.rs:38-70`：`ctrl_rx` 收到 `Stop` / `Isolate(true)` → `stop_tx.send(())`。
   - `sources/tcp/worker/listener_loop.rs:58-73`：`run()` 只在 `stop_rx.recv()` 时 `break`；
     循环结束、sender（`instance_reg_txs`）被 drop → source 侧 `recv()` 得到 `None`。
   - 客户端断开走的是另一条 INFO 路径：`conn ... closed during try_read` /
     `deregistered connection (reason=peer ... closed)`（同文件 `:102` / `:178` 附近）。
2. **停机被当成可重连故障**：`source.rs:340-344`，`wait_for_connection` 返回 false 后
   `receive()` 立即返回 `SourceReason::disconnect("TCP source 'x' no active connections")`；
   下游（如 `wf-runtime/src/source/mod.rs:315`）把 `Disconnect | SupplierError` 映射为
   **Connect**（可重连），于是外层重试 → 再进 `receive()` → 再打一条同样的 WARN
   （实测间隔 5s）。
3. 与实例数无关：`sources/tcp/config.rs:13` `DEFAULT_TCP_SOURCE_INSTANCES = 1`。

### 证据时间线（同一次运行）

| 时间（local） | 事件 |
|---|---|
| 17:58:53.093 | `TCP listener loop 'tcp_1' stopped` |
| 17:58:53.184 | 该进程全部 role group `await end` |
| 17:58:53.688 | WARN #1（+0.5s） |
| 17:58:58.691 | WARN #2（+5s，重试） |

## 影响

- 每次优雅停机都会产生 2 条 WARN（+ 一次无谓重试），示例 / CI 日志噪音大、易被误读为故障；
- 排障时无法从等级上区分「正常停机」与「监听器异常提前退出」——而后者是真问题
  （source 不再接受任何新连接）。

## 建议修复

1. **区分停机与异常**：监听循环停止时把「原因」传给 source（例如在关闭 `connection_rx`
   之前发送一个 `Shutdown` 哨兵，或让通道返回 `Result<ConnectionRegistration, SourceReason>`），
   source 据此返回 `SourceReason::EOF`（正常结束）而非 `Disconnect`。
2. **日志分级**：正常停机走 INFO/debug（如 `TCP source 'x' listener stopped; no new connections`）；
   仅在**非停机**状态下通道意外关闭才 WARN/ERROR。
3. **避免停机引发重试**：消费侧收到 EOF 应结束而非重连（配合 1 即可；否则上层仍会按
   Connect 重试一次）。

## 验收标准

- 优雅停机时不再出现该 WARN；正常结束路径为 INFO，且**不产生第二条**（无重试）；
- 监听循环因错误/意外结束（非停机）时仍有 WARN/ERROR 提示，且能区分来源；
- `wait_for_connection` 的 channel-closed 分支有对应单测（当前仓库内**没有**断言
  `"connection channel closed"` / `"no active connections"` 的测试，改动不会破坏既有断言）；
- `sources/tcp/` 内既有多实例分发用例（`factory.rs` 内 `instances = 3` 的用例）保持通过。

## 证据索引

| 项 | 位置（wp-core-connectors 0.8.4） |
|---|---|
| WARN 输出 | `src/sources/tcp/source.rs:305`（`wait_for_connection`，`:292`） |
| 停机后 `receive()` 报 disconnect | `src/sources/tcp/source.rs:340-344` |
| 监听循环退出条件 | `src/sources/tcp/worker/listener_loop.rs:58-73` |
| 通道 sender 归属/传递 | `src/sources/tcp/acceptor.rs:38-70`、`listener_loop.rs:18` |
| 默认实例数 | `src/sources/tcp/config.rs:13` |
| 下游把 Disconnect 当可重连 | `wp-reactor/crates/wf-runtime/src/source/mod.rs:315` |
| 复现脚本 | `warp-fusion/examples/wp-pipeline/streaming_ipv6/run.sh` |
| 触发该 source 的配置 | `warp-fusion/examples/wp-pipeline/streaming_ipv6/wfusion/topology/sources/netflow_tcp.toml`（`key = "nginx_tcp"`） |
