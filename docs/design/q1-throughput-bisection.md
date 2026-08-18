# Q1 吞吐二分法验证记录

> 日期：2026-08-18 · 分支 `feat/columnar-execution`（列式执行改造之后）
> 目标：用逐段切断法定位 Q1（`on each` 无状态投影）的吞吐瓶颈，判断下一步该优化哪里。

## 0. 基准锚点

- 命令：`./bench.sh q1 cont 100m`（`wf-examples/performance/nexmark_pk`）
- 配置：`parse_buffer_bytes=2GB`、`CONNECTIONS=4`、`SHARD_KEYS=bid_events:auction`、
  `p=10 r=10`、100k 帧、`instances=4`（source read+decode 并行度）
- 机器：Apple M3 Max，16 核（12 P-core + 4 E-core），**双峰相位 ±8%**，A/B 需按相位配对
- Q1 规则：`events { b : bid_events } on each b -> score(1.0) entity(digit, b.auction)
  yield nexmark_alerts(id=b.auction, alert_type="q1_passthrough", detail="bid", request_count=1)`
- 正确性口径：`EMIT q1_bid_passthrough = 92,000,000`（bid 流 ≈ 92% × 100M），全跑批 `[clean]`

## 1. 逐刀切断结果

「切掉某一步」= 把该步改成 no-op（只保留上一段的可测计数），其余不变。EPS 为
`window append_total / 墙钟`（bench.sh 口径）。

| 切法 | EPS | 较上一级掉的 | 备注 |
|---|---|---|---|
| **只 receive（第 1 步退出）** | **14.05M** | — | parse worker 里只 count + drop，无 route/window/rule/sink |
| + parse + window actor + broadcast（切规则） | 12.17M | **-13%** | `process_batch` no-op |
| + rule `process_batch`（切 sink） | 7.52M / 8.18M | **-38%** | `flush_alerts` no-op |
| + sink（全量基线） | 7.17M | **-4%** | 完整链路 |

```text
ingress(只 receive)                14.05M
  └─ parse/window/broadcast       -13%  → 12.17M
       └─ rule process_batch      -38%  → ~7.5M   ← 主墙
            └─ sink                -4%  → ~7.17M
```

### 各刀实现（均已回退，仅记录口径）

1. **只 receive**：`wf-runtime/lifecycle/parse_pool.rs::run_parse_worker_direct` 里，拿到
   `ParseItem` 后 `metrics.add_window_append("bid_events", batch.num_rows())` 然后
   `drop(permits); continue;`——跳过 `route_parse` / `dispatch_parsed`。
2. **切规则**：`wf-runtime/engine_task/rule_task.rs::process_batch` 开头直接 `return;`。
3. **切 sink**：`rule_task.rs::flush_alerts` 里 `take` 掉 pending 后直接 `return;`（不
   `try_send`/`send` 给 sink 通道）。`emitted_total` 在 emit 路径已计数，故 EMIT 仍 92M。

## 2. 旁路窗口 actor（已回退，非干净收益）

把无状态 `on each` 窗口的 raw batch 直接广播到规则任务、跳过窗口 actor 的
append/reorder/evict（`router.rs` 加 `bypassable` 集 + `dispatch_parsed` 短路）。

| 口径 | EPS | EMIT | 结论 |
|---|---|---|---|
| 领先记 append（广播后即记） | 10.16M | 84.5M ❌ | append 速率提升，但 EMIT 欠计 7.5M |
| 滞后记 append（规则处理完记） | 3.83M | 92M ✅ | 端到端退化 |

- 领先口径的 10.16M 是「喂给规则通道」的速率；滞后口径显示端到端只有 3.83M，且规则
  任务 CPU 仅 ~14%——**并发广播（10 parse worker 直推规则通道）造成退化**，不是 sink。
- 结论：旁路窗口 actor 对 Q1 端到端**无收益**，已回退。

## 3. ingress（只 receive）天花板定位

只 receive 的天花板分三阶段挖出（每步都有实证）：

1. **热点分片限制 14.05M**：旧 `SHARD_KEYS=bid_events:auction` 只分 bid，auction/person
   全进 s0（s0=2.47GB vs 其余 1.76GB），最大分片完成时间卡住 ingress。
2. **均匀分片 → 20.2M**：`SHARD_KEYS="bid_events:auction,auction_events:id,person_events:id"`
   三个流各自按键分，8×970MB 均匀 → ingress 14.05M → 20.2M。
3. **客户端 copy 缓冲 → 43.9M**：`send-arrow` 用 `tokio::io::copy`（8KiB 栈缓冲），
   100M 数据约 100 万次文件读（syscall + spawn_blocking 交接）→ 卡 20.2M；改
   1MiB 大缓冲 → **43.9M EPS（~3.4GB/s）**。

连接数 / `instances` 缩放（旧分片 + 旧客户端）：

| 引擎 `instances` | CONNECTIONS | EPS |
|---|---|---|
| 4 | 1 | 6.15M |
| 4 | 4 | **14.05M** |
| 4 | 8 | 5.48M |
| 4 | 16 | 5.48M |
| **8** | **8** | **14.05M** |
| **8** | **16** | **14.08M** |

- **连接数 > `instances` 会退化**：C=8/16 配 instances=4 时，4 个 decode 循环在 2/4 条
  连接间 async 切换，掉到 5.48M（C=8 与 C=16 完全相同 = 撞到同一个 `instances=4` 上限）。
- **把 `instances` 提到 8 配 C=8，ingress 回到 14.05M**；再配均匀分片 + 大缓冲客户端，
  ingress 逐级涨到 43.9M。
- 8 分片 vs 16 分片（均匀 + 优化客户端）同为 ~20M 之后涨到 43.9M：分片数不影响，
  瓶颈在客户端 copy 缓冲，改完 1MiB/4MiB 持平（43.9/43.4M）。

## 4. 分片文件不均与修复（benchmark 工具口径）

旧 `shard-frames --shard-keys bid_events:auction` 只按 `bid_events.auction` 分片，导致
`auction_events`（~6M）与 `person_events`（~2M）**全部落进 s0**，s0 永远是热点分片：

| 分片数 | s0 大小 | 其余分片大小 | s0 占比 |
|---|---|---|---|
| C=4 | 2.47 GB | 1.76 GB × 3 | ~32% |
| C=8 | 1.59 GB | 0.88 GB × 7 | ~21% |
| C=16 | 1.15 GB | 0.44 GB × 15 | ~15% |

- **修复**：`SHARD_KEYS="bid_events:auction,auction_events:id,person_events:id"` 三个流
  各自按键分 → 8×970MB / 16×485MB 均匀（偏差 <0.1%）。
- 光分片不均解释不了 5.48M 暴跌（C=8 的 s0 比 C=4 的 s0 更小却更慢）；5.48M 的根因是
  `instances` 与连接数错配（§3）。
- **send-arrow 优化**（warp-fusion `wfgen`，commit `db39f81`）：`tokio::io::copy` 的
  8KiB 栈缓冲 → 1MiB 大缓冲，只 receive 20.2M → 43.9M（+2.2×）。

## 5. 结论

1. **Q1 主墙是 rule `process_batch`（~38%）**：物化 Event + each 循环 +
   `emit_each_direct_batch`。这是下一步最该打的地方——把 `on each` 规则任务列式化
   （不产生 Event HashMap，直接对 `RecordBatch` 求值）。
2. 其次 parse/window/broadcast（~13%）；sink 基本不是（~4%）。
3. **ingress 已不是瓶颈**：均匀分片 + 优化客户端后 43.9M，远超全链路 7.17M。
4. 旁路窗口 actor、切 sink 都已回退，不是干净收益。
5. benchmark 工具侧：`SHARD_KEYS` 已修（三流分片）、`send-arrow` copy 缓冲已优化；
   连接数需与 `instances` 匹配（`instances` 默认 4，建议改文档）。

## 6. 下一步

- **Q1**：rule `process_batch` 列式化（打 ~38%）。
- **Q2**：match 状态机 `scan_expired_at` + `advance_at_with_masks` 分段计时（summary 里
  Q2 actor broadcast 73.3% 的证据指向这里）。
- **benchmark 工具**：把 `SHARD_KEYS` 三流分片、`instances=连接数` 写进 bench.sh 默认
  配置；`send-arrow` 大缓冲已提交（warp-fusion `alpha` `db39f81`）。
