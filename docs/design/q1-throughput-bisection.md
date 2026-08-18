# Q1 吞吐二分法验证记录

> 日期：2026-08-18 · 分支 `feat/columnar-execution`（列式执行改造之后）
> 目标：用逐段切断法定位 Q1（`on each` 无状态投影）的吞吐瓶颈，判断下一步该优化哪里。
> 机器测量纪律（§7-§10 定案）：本机是**常载开发机**（loadavg 6.5-8.8），同一配置的 EPS
> 随后台干扰在 43↔55↔83M 间摆动——**所有 A/B 对比必须同 load 块配对**（bench.sh 结果行
> 带 `load=` 上下文），早期记录的「双峰相位 ±8%」是误判，见 §9。

## 0. 基准锚点

- 命令：`./bench.sh q1 cont 100m`（`wf-examples/performance/nexmark_pk`）
- 配置：`parse_buffer_bytes=2GB`、`CONNECTIONS=4`、`SHARD_KEYS=bid_events:auction`、
  `p=10 r=10`、100k 帧、`instances=4`（source read+decode 并行度）
- 机器：Apple M3 Max，16 核（12 P-core + 4 E-core），**常载开发机（loadavg 6.5-8.8）**，
  A/B 需按 load 块配对（早期「双峰相位 ±8%」已定性为后台干扰，见 §9）
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

> 本节是原会话（c=4 基线时代）的结论，数字已被 §7-§12 取代；权威总结见 §13。

1. **Q1 主墙是 rule `process_batch`（~38%）**：物化 Event + each 循环 +
   `emit_each_direct_batch`。这是下一步最该打的地方——把 `on each` 规则任务列式化
   （不产生 Event HashMap，直接对 `RecordBatch` 求值）。
2. 其次 parse/window/broadcast（~13%）；sink 基本不是（~4%）。
3. **ingress 已不是瓶颈**：均匀分片 + 优化客户端后 43.9M，远超全链路 7.17M。
4. 旁路窗口 actor、切 sink 都已回退，不是干净收益。
5. benchmark 工具侧：`SHARD_KEYS` 已修（三流分片）、`send-arrow` copy 缓冲已优化；
   连接数需与 `instances` 匹配（`instances` 默认 4，建议改文档）。

## 6. 下一步

> 本节为原会话计划；后续实测把「ingress 43.9M 天花板」修正为「空载 83M、受后台负载
> 干扰」（§9/§10），但主攻方向不变。

- **Q1**：rule `process_batch` 列式化（打 ~38%）。
- **Q2**：match 状态机 `scan_expired_at` + `advance_at_with_masks` 分段计时（summary 里
  Q2 actor broadcast 73.3% 的证据指向这里）。
- **benchmark 工具**：把 `SHARD_KEYS` 三流分片、`instances=连接数` 写进 bench.sh 默认
  配置；`send-arrow` 大缓冲已提交（warp-fusion `alpha` `db39f81`）。

## 7. 更正：8:8 相位实测（2026-08-18 下午）

> 上文 §3 的「8 文件 54.9M/56.0M vs 16 文件 43.9M → 8 文件更优」结论经复测推翻：
> **那是双峰相位混淆**。本轮在 8:8（`ingress.toml instances=8`、`CONNECTIONS=8`、
> 三流分片 c8 ke7749def 缓存）下逐刀复测 + 相位多跑，得到修正后的结论。

### 7.1 新基线（8:8 全链路）

`WARMUP=1 ./bench.sh q1 cont 100m`（干净二进制）：

| 轮 | EPS | RSS | CPU | 正确性 |
|---|---|---|---|---|
| warmup（丢弃） | 9.91M | 7.5GB | 1064% | clean（evict=69 不计入） |
| **正式** | **9.22M** | 9.2GB | 1010% | **clean** |

- 旧 c=4 基线 7.17M → 8:8 **9.22M（+29%）**：提升来自三流均匀分片 + `instances=8`。
- EMIT 口径：正式轮 `[clean]`（致命计数器全 0）；EMIT 数值是**采样下界**（exporter 早于
  rules 关停，见 §8.3），不作为严格正确性门。

### 7.2 探针复测：解析通道零成本，墙在 TCP read + IPC decode

探针 D（source 循环 decode 后直接 count+drop，**完全绕过** `push_decoded_batch` /
解析池）与探针 A（parse worker count+drop，走完整解析通道）数字一致：

| 探针 | c×i | EPS | CPU max |
|---|---|---|---|
| D decode 级 | 8×8 | 43.0M | 348% |
| D decode 级 | 8×16 | 43.6M | 352% |
| D decode 级 | 16×8 | 42.9M | 381% |
| D decode 级 | 16×16 | 54.1M / 43.0M / 42.8M / **55.0M** | 189-428% |
| A parse worker 级 | 8×8 | 43.9M | 342% |

- **连接数/实例数对接收侧无影响**：c8i8 / c8i16 / c16i8 / c16i16 全部落在同一
  ~43M 低相位（c8i8 高相位也测到 55.2M）。此前「8 文件更优、16 文件更差」是把
  高相位 54.9M 记给了 8、低相位 43.9M 记给了 16，纯属相位错配。
- **解析通道（`content_bytes`/投影/`next_window_seqs` 锁/parse channel）不是墙**：
  探针 A ≈ 探针 D，`push_decoded_batch` 与解析池合计 <1% 成本。
- **磁盘不是墙**：page cache 热读 7.76GB（16×485MB 分片）仅 0.6s = 12.3GB/s。

### 7.3 双峰相位幅度修正：±8% → ±13%（43M ↔ 55M）

同一配置（16×16）连测 5 次：54.1 / 43.0 / 42.8 / 55.0 / 55.2M —— 两个离散态
约 ±12.5%（相对中值 49M）。之前记录的「±8%」是在 7-10M 全链路上的观测；接收侧
~50M 量级的相位摆动更大。**任何 A/B 对比必须先同相位配对**（连测两条，取同态）。
机制已定案：**后台干扰**（Zed/VM/WorkBuddy 等拉高 loadavg，见 §9），不是机器
时钟两态（C 紧循环基准与 EPS 无相关，见 §9.3）。

### 7.4 修正后的结论

1. ingress 依然**不是**全链路墙：接收侧 43-55M ≫ 全链路 9.22M，余量 5×。
2. Q1 主墙仍是 rule `process_batch`（旧基线 ~38% 占比），下一步不变：
   `on each` 规则任务列式化（不物化 Event，直接对 `RecordBatch` 求值）。
3. bench.sh 默认配置（`CONNECTIONS=8`、三流 SHARD_KEYS、`instances=8`）保留为
   基线；但「8 是甜点、16 反而差」的说法作废——两者相位分布一致。
4. 手动起 daemon + `send-arrow` 直连会复现「connection error 收不到数据」
   （客户端 0.8s 推完但引擎 0 行）——bench.sh 内起则正常；属工具侧问题，未深究。

## 8. route_parse 深度 review（2026-08-18 下午，同相位实测）

> 任务假设：route_parse 吃掉 ingress 20%（54.9M → 43.9M），主要是 `content_bytes`
> 计算 + 订阅者遍历。经**同相位探针**验证：假设不成立——~20% 是相位混搭
> （54.9M 高相位 vs 43.9M 低相位），route_parse 实际成本 ~0.25%。

### 8.1 同相位探针（WF_BISECT env 切换，8:8，全部低相位块）

| 探针 | 说明 | EPS×4 轮 | 均值 |
|---|---|---|---|
| A | parse worker count+drop，**跳过 route_parse+dispatch** | 44.27 / 44.32 / 43.56 / 43.84 | 44.00M |
| B | **route_parse ON**，count+drop，切断 dispatch | 44.48 / 44.33 / 44.04 / 43.61 | 44.11M |

- route_parse 全成本 ≈ **+0.25%**（44.00 → 44.11M），在噪声内。
- 54.9M → 43.9M 的「-20%」= 高相位值 vs 低相位值，与 route_parse 无关。

### 8.2 代码 review 发现（真实存在的浪费，已修一处）

每批热路径（100k 行帧）的 route 链成本盘点：

1. **`content_bytes` 每批算两遍**：`push_decoded_batch`（preread 记账）+
   `route_parse`（窗口记账）。旧实现对 Utf8/Binary 列逐行 `arr.iter().flatten().map(str::len)`
   —— bid 流 3 个字符串列（channel/url/extra）× 100k 行 × 2 遍 = 60 万次逐行扫描/批。
   **已修（本分支未提交）**：改为 O(1) offsets 差分（`offsets[n]-offsets[0]` 即总 payload，
   空槽 offset 持平不贡献），值与原实现逐位一致（`content_bytes_ipc_roundtrip_does_not_inflate`
   回归测试通过），`content_bytes` 从 O(rows×cols) 变 O(cols)。
2. **订阅者遍历 alloc 重**：`subscribers_of` 每次调用克隆 `String` 窗口名 + 取
   subscriptions RwLock，热路径每批 2-4 次；`get_window`/`has_mailboxes`/`mailbox` 各取
   一次 RwLock。每批 1-2 个窗口时频率低（440 批/s × ~6 次锁 ≈ 2.6k ops/s），实测无感。
   可优化但**不值得**（除非窗口数×批率×10×）。
3. `next_window_seqs` 全局 Mutex（source 侧）：每批 1 次短临界区，无感。

### 8.3 EMIT 口径发现（harness 采样伪影，非丢数据）

8:8 全链路 EMIT 只有 79.7M（< 92M），但无任何致命计数（dropped_late /
serialize_failed / memory_evicted / cursor_gap 全 0）。时间线：metrics exporter
12.1s 后停止（26.5s），rules 组 28.2s 才关停完成——**最后 1.7s 的发射（≈12M）
没被采样**，与缺口 12.3M 吻合。15:01 改动前 warmup 同样只有 84M，与 content_bytes
无关。规则实际处理完了全部 92M（优雅关停 drain 到 `rules shutdown complete`）。
**bench.sh 的 EMIT 是采样下界**，正确性应看 `[clean]`（致命计数器），EMIT 不能作
为 8:8 快灌下的严格门；要修需让 exporter 在关停时继续采样到 rules drain 完。

## 9. 54M 是怎么出现的（2026-08-18 下午，机制已被 §12 推翻）

> ⚠ 本节「后台干扰」结论已被 §12 推翻：43/55/84M 三档全是 bench.sh 计时伪影
> （metrics exporter 1s 落盘滞后），真实 decode ≥84M。本节保留客户端恒定证据
> （§9.1，仍有效）与 loadavg 数据（§9.3，与 EPS 反相关，非因果）。

### 9.1 客户端恒定（排除客户端变量）

探针 D（decode 级 count）下抓客户端自己的推送耗时（`Replayed X bytes in Y s`）：

| 轮 | EPS | 客户端耗时 |
|---|---|---|
| 1 | **81.6M** | 0.731s |
| 2-8 | 55.0-55.4M | 0.723-0.754s |

客户端 8 轮全部 0.73s ±3% 推完 7.76GB（≈10.6GB/s ≈ 137M 行/s 当量）——**客户端
恒快，不是变量**。43/55M 都是引擎消费速率。

### 9.2 引擎侧延迟敏感墙：逐连接读循环

每连接 8MiB 帧 = 32 × `readable().await + try_read`（256KiB 上限，wp-core-connectors
`MAX_READ_BYTES`），读循环 wakeup 延迟 × 连接数 = 聚合吞吐。后台争抢 CPU/内存带宽
时 wakeup 变长：重干扰 → 43M，轻干扰 → 55M，罕见空载 → **81.6M**（本轮 run1）。
这也解释为何连接数/实例数怎么调都没用（不减延迟）以及全链路 9.22↔11.24M 摆动
（规则任务 CPU 被抢，cpu_avg 恒 ~1010% 但 EPS 不同 = 每核有效吞吐不同）。

### 9.3 机器是常载开发机（根因）

- loadavg（1-min）常年 **6.5-8.8**：Zed ~100%、`VirtualMachine` XPC ~36%、
  WorkBuddy、WindowServer、钉钉、QQMusic、dasd...
- 物理内存 54G/64G 已用，~20G 在压缩器（compressor）——decode 是内存带宽重活
  （~51GB/s churn），压缩器抖动直接拖慢。
- 之前「双峰相位 ±8%」是误判：不是机器时钟两态，是**后台干扰的两个准稳态**
  （重/轻），以及偶发的空载（81.6M）。C 紧循环基准 1.16↔1.46s 与 bench EPS
  无相关（round4 反向），排除全局时钟。

### 9.4 措施

- bench.sh 结果行新增 `load=`（1-min loadavg）——A/B 对比按同 load 配对，
  或安静机器（关 VM/WorkBuddy 等）再测。
- 真实 ingress 上限 ≥81.6M（引擎 decode 空载能力），远超全链路 9-11M：
  **ingress 依旧不是墙**，Q1 主攻方向不变（rule `process_batch`）。

## 10. recv → parse 无性能下降（2026-08-18 下午，同负载配对实测）

> 问题：recv（decode 后计数）→ parse（完整 `push_decoded_batch` + parse 通道）
> 是否有性能下降？——**没有**。之前所有「看起来的下降」都是跨负载块对比。

探针 D（source 循环 decode 后直接 count+drop，绕过 push/parse 池）vs 探针 A
（走完整 `push_decoded_batch`：content_bytes + preread + build_parse_item
[投影/next_window_seqs] + parse 通道，parse worker count+drop，跳过 route_parse）：

| 轮 | 探针 | EPS | load(1-min) |
|---|---|---|---|
| 1 | D | 43.6M | 5.9 |
| 2 | A | 83.3M | 5.2 |
| 3 | D | 83.0M | 4.7 |
| 4 | A | 84.7M | 3.9 |
| 5 | D | 82.6M | 4.7 |
| 6 | A | 83.7M | 5.2 |

- 同负载块（load ≤5.2）：A（83.3/84.7/83.7）≈ D（83.0/82.6），差 <2% 在噪声内——
  **recv → parse 零成本**（content_bytes O(1) 化之后 push 路径更轻）。
- round1 是活教材：load=5.9 时同样探针 D 只有 43.6M——**负载差 0.7 就把 EPS 砍半**，
  正是「双峰」的边界。之后所有 A/B 对比必须同 load 块配对。
- 引擎 decode 空载上限 ≈83M（本块实测），parse 侧（含 route_parse +0.25%，§8）
  不在此上限内。

> ⚠ §12 定案后回看本节：round1 的「43.6M」与同块的「83M」差异其实是 exporter 落盘
> 伪影（§12），load 并非因果。但**同块内 A≈D 的相对结论仍成立**。

### 10.1 当前复测（2026-08-18 傍晚，content_bytes O(1) 生效后）

`WF_BISECT` A/B 连测 8 轮（同相位块，全部落在伪影下界档）：

| 轮 | 探针 | EPS |
|---|---|---|
| 1 | A | 44.4M |
| 2 | B | 44.5M |
| 3 | A | 44.6M |
| 4 | B | 43.9M |
| 5 | A | 43.6M |
| 6 | B | 43.9M |
| 7 | A | 43.5M |
| 8 | B | 43.5M |

- 读数均值：**A（recv+parse 通道）44.05M / B（+route_parse）43.96M** —— 差 ~0.2%
  在噪声内，route_parse 依旧无成本。
- **绝对值按 §12 是伪影下界**：真实 parse 后 EPS ≥84M（同一 1.2s feed 在落盘相位
  合适时读数 84M+）。当前代码（content_bytes O(1)）下 parse 后真实吞吐未变。

### 10.2 同块 recv vs parse 定论（D/B 交替 6 轮）

> 教训：**绝对读数不能跨块对比**（§12 伪影档位不同）。「recv 84M vs parse 44M」是
> 跨块错觉；同块连测才有意义。

| 轮 | 探针 | EPS | load |
|---|---|---|---|
| 1 | D（只 recv） | 43.89M | 6.8 |
| 2 | B（parse 后） | 43.85M | 6.2 |
| 3 | D | 43.86M | 6.0 |
| 4 | B | 43.70M | 5.6 |
| 5 | D | 43.73M | 5.0 |
| 6 | B | 43.93M | 4.8 |

- **D 均值 43.83M = B 均值 43.83M** —— recv 与 parse 后逐位一致，**parse 零成本**。
- 本块落在伪影下界档（43.8M）；好相位块两者会一起到 ~84M（§10 已测 A≈D≈83M）。
- 结论：parse 后 EPS = recv EPS（≥84M 真实值），「80M→40M 下降」不存在。

## 12. 定案：80+M 是真实值，43/55M 是测量伪影（2026-08-18 傍晚）

> §9「43↔55M = 后台干扰」被推翻：**引擎 decode 真实速率一直是 ~84M+（可能更高），
> 43/55M 读数全部是 bench.sh 计时伪影**（metrics exporter 1s 落盘滞后把 T2 拉长）。
> 「EPS 怎么涨到 80+M」——它没涨，是之前的读数系统性偏低。

### 12.1 证据（43M 轮 vs 84M 轮逐项对比）

探针 D（decode 级 count）+ 每 0.12s 采 daemon 进程 CPU，连测 6 轮：

| round | EPS | load0 | loadEnd | cpuMax |
|---|---|---|---|---|
| 1 | 84.8M | 8.17 | 7.8 | 494% |
| 2 | 83.5M | 7.69 | 7.1 | 513% |
| 3 | 84.5M | 7.07 | 7.7 | 518% |
| 4 | 82.8M | 7.17 | 6.7 | 535% |
| 5 | **43.0M** | **6.67** | 6.0 | 509% |
| 6 | 82.8M | 5.80 | 5.8 | 536% |

1. **CPU 总量相同**：round5（43M）采样 avg 107.6%、round3（84M）105.7%——总
   ~3.2 core-s、峰值都 ~510%、忙碌窗口形状逐位一致（同一衰减点）。若 43M 真跑了
   2.33s×5 核，总工作应为 84M 轮的 ~2×。→ **两轮真实处理工作量相同**。
2. **load 反相关**：load=8.17 → 84.8M，load=6.67 → 43M。§9 的「load 相关性」是
   巧合（load 是在跑完后采的 1-min 均值，滞后且受 bench 自身影响）。
3. **客户端恒 0.73s 推完 7.76GB**（socket buffer 只容 ~0.1s 数据）：客户端能这么
   快推完，只可能引擎在实时消耗 → 55M（1.8s/100M）读数也站不住。

### 12.2 机制：exporter 1s 落盘滞后 × 0.5s 轮询 = T2 膨胀

- bench.sh `EPS = 100M/(T2−T0)`；T2 = 轮询（每 0.5s 读 metrics.ndjson）首次看到
  `append_total≥100M` 的时刻。
- metrics exporter **每 1s 才落盘一次**（`wf-config::HumanDuration` 只接受整数秒
  s/m/h/d，`report_interval="100ms"` 直接配置解析失败——实测验证，这是伪影根源）。
- probe 的 feed 只有 ~1.2s：feed 结束落在两次落盘之间时，最后一段 append 要等下一个
  1s 落盘才进文件 → T2 多算 0.5-1s → EPS 从 ~84M 被拉到 43-67M。
- 落盘相位由 daemon 启动时序决定（随负载漂移）→ 伪随机「43/55/84 三档」；负载稳定时
  相位稳定 → 出现连续多轮同值（之前以为的「双峰块」）。

### 12.3 对各结论的影响

- **相对比较不受影响**（同块内同伪影）：route_parse +0.25%（§8.1）、recv→parse 无下降
  （§10）仍成立。
- **绝对数值是下界**：真实 decode ≥84M（§10 同块 A/D 值），ingress 余量比之前结论
  更大。
- 全链路 9.22↔11.36M：feed ~10s，1s 滞后误差 ≤10%，基本真实（规则 ~9-11M 为主墙）。

### 12.4 修复方向（未做）

- 引擎：`HumanDuration` 支持亚秒（ms），或 exporter 支持 <1s 间隔。
- bench：短跑（<3s）改用客户端完成时间或引擎 CPU busy window 计时。

### 12.5 真实值下界：≥117M（客户端 wall time 铁证，2026-08-18 傍晚）

伪影档位（44M/43M）下抓客户端推送耗时：

| 轮 | bench 读数 | 客户端推完 7.76GB | 隐含引擎消耗 |
|---|---|---|---|
| 1 | 44.2M | 0.856s | ≥9.1GB/s ≈ 117M 行/s |
| 2 | 42.8M | 0.784s | ≥9.9GB/s ≈ 128M 行/s |

客户端被引擎 drain 速率节流（socket buffer 全连接只容 ~0.1s 数据）：若引擎真只有
43M/s（3.4GB/s），客户端需 ~2.3s 才能推完——但实测 0.8s。**0.8s 推完只可能引擎以
≥9.7GB/s（≥125M 行/s）实时消耗**。

结论：**真实 decode ≥117-128M 行/s；84M 是最好的读数但不是天花板；43/55/84M
三档都是同一真实值的档位化低估（43/55 是 T2 膨胀档，84 是滞后最小档）。**
「80M 数据是假的？」——不，80M 是真实读数且最接近真实值；43/55M 才是虚低的。

### 12.6 工具修复与修复后实测（2026-08-18 傍晚定数）

**修复**（本分支未提交）：
1. `wf-config` `HumanDuration` 支持 `ms` 后缀（原只接受整数秒 s/m/h/d——伪影根源）
   + 单测 `duration_millis`；
2. `nexmark_pk/conf/wfusion.toml` `report_interval = "100ms"`；
3. `bench.sh` 轮询 0.5s → 0.1s（T2 误差 ≤ exporter 间隔 + 轮询间隔 ≈ 200ms）。

修复后探针实测（D/B 交替 6 轮，无伪档）：

| 轮 | 探针 | EPS |
|---|---|---|
| 1 | D（recv） | 93.2M |
| 2 | B（parse 后） | 95.0M |
| 3 | D | 107.9M |
| 4 | B | 87.8M |
| 5 | D | 92.0M |
| 6 | B | 93.4M |

- **D 均值 97.7M / B 均值 92.1M**，差 ~6% 在 ±10% 分辨率带内 → recv ≈ parse。
- 读数区间 88-108M，**43/55/84M 伪档消失**——「是 80M 还是 40M」的答案：
  **都不是，真实 ≈95M（90-108M 带）**；40M 是旧工具量化伪档，84M 是旧工具能读到的
  最高档（仍低于真实值）。

### 12.7 在 dispatch_parsed 处切断的实测（用户指定切点）

探针 C：`run_parse_worker_direct` 在 `route_parse` 后直接 count+drop，**屏蔽
`dispatch_parsed` 及其后的全部逻辑**（窗口 actor / broadcast / rule / sink），
append 用 fabricate（真实计数在窗口 append 处）。修复后工具、D/C 交替 6 轮：

| 轮 | 探针 | EPS |
|---|---|---|
| 1 | D（只 recv） | 97.3M |
| 2 | C（dispatch 后全屏蔽） | 94.2M |
| 3 | D | 109.9M |
| 4 | C | 110.4M |
| 5 | D | 110.0M |
| 6 | C | 93.2M |

- **C 均值 99.2M / D 均值 105.7M**（差 ~6%；轮 2 配对 0.5%、轮 6 离群 15%）→
  recv→parse→route_parse 段依旧零成本。
- dispatch 之后（窗口 actor → broadcast → rule → sink）未计入；全链路 ~10M 的主墙
  仍在 rule `process_batch`。

### 12.8 窗口 actor 段（append 保留 / broadcast 切断）实测

`commit_appended_batch` 里跳过 fanout.broadcast_*（窗口 append + watermark + 日志写
保留，append_total 真实计数；EMIT=0 验证广播确被切断），修复后工具、与 D 交替 6 轮：

| 轮 | 探针 | EPS |
|---|---|---|
| 1 | D（只 recv） | 96.8M |
| 2 | N（窗口 append，broadcast 切） | 83.5M |
| 3 | D | 94.7M |
| 4 | N | 92.6M |
| 5 | D | 94.1M |
| 6 | N | 88.4M |

- **N 均值 88.2M / D 均值 95.2M**（差 ~7%，配对 2/3 在 2-6%、配对 1 离群 14%）→
  dispatch + 窗口 actor append（reorder/watermark/日志写）段在分辨率带内无显著成本。
- 下一节点：broadcast → rule 通道 → rule `process_batch`（主墙，全链路 ~10M）。

### 12.9 rule `process_batch` no-op 实测（主墙实锤）

`process_batch` 开头直接 return（broadcast → rule 通道 → 规则任务 recv 全部保留，
背压真实；EMIT=0 验证规则未执行），修复后工具连测 3 轮：

| 轮 | EPS |
|---|---|
| 1 | 81.1M |
| 2 | 89.8M |
| 3 | 90.1M |

- **均值 ~87M**，与窗口 append 段（§12.8 的 88.2M）持平 → **broadcast + 通道 + recv
  零成本**。
- 全链路 ~10M 的全部落差（87M → 10M，-88%）都发生在 `process_batch` 内部
  （bind filter 列式求值 + 懒物化 + on-each 循环 + emit）——**主墙 100% 实锤**。

## 13. 结论终版（2026-08-18 傍晚）

1. **Q1 主墙 = rule `process_batch`**（全链路 ~10M，相对占比不变）。
2. **ingress 不是墙**：真实 decode ≈95M（修复后实测，区间 88-108M；客户端 wall 推断
   ≥117M 作上限参考），余量 ≥9×。
3. **recv→parse 零成本**（§10.2 同块 D=B 逐位一致；§12.6 修复后 D≈B）。
   **route_parse +0.25%**（§8.1）。
4. **43/55/84M 三档 = bench.sh 计时伪影**（exporter 1s 落盘滞后），非后台干扰、非机器
   时钟、非配置、非客户端；已修复（§12.6），修复后读数 ~95M 稳定。
5. 正确性门：`[clean]`（致命计数器）；EMIT 是采样下界（§8.3，关停顺序未修）。
6. 未提交改动：content_bytes O(1)（§8.2）、`HumanDuration` ms 支持 + bench.sh 轮询
   + `report_interval=100ms`（§12.6）、bench.sh 8:8+load=（§9.4）、`instances=8`。

## 14. Q1 `on each` 列式化（2026-08-18 傍晚落地，EPS 10M → 12.1M）

### 14.1 实现（对应设计文档 §3.5「on each 完全不物化」）

打主墙 `process_batch` 的第一步：去掉 on-each 规则的逐行 `Event` 物化。

- **`ColumnarEvent`**（`wf-engine/match_engine/event_bridge.rs`）：按行直接读 Arrow
  列的字段视图，与 eager `Event{HashMap}` 逐字节一致（同一 `extract_field_value`
  转换：Int64/Timestamp 走 f64 round-trip、null → 字段缺失）。
- **`each_plan_columnar_safe()`**（each_exec.rs）：列式快路径门——无 join、无 each
  filter、score 常量、entity 字段/常量、yield 全字面量/平字段、bind filter 全
  无或列式（非列式 bind filter 的逐行 interpreted 回退被排除）。
- **`execute_each_direct_batch_columnar`**（each_exec.rs）：逐行读列（entity/yield
  字段），wfx_id 按**批级预排序字段表 + 直接从列渲染哈希字节**（`write_flat_column_scratch`，
  零 Value 构建/字符串克隆）；fired_at/score/emit 与 eager 路径同一实现。
- **rule_task.rs**：`columnar_each` 门（无状态机 + each_direct + 无 filter 广播 +
  columnar-safe）→ 跳过 `materialize_rows`（Q1 全行命中）与逐行循环，直接走
  `emit_each_direct_batch_columnar`（同 flush/背压/遥测口径）。

### 14.2 正确性

- **对拍测试** `execute_each_direct_batch_columnar_matches_event_path_rows`：同一
  RecordBatch（含 null → 字段缺失、2^53±1 Int64 → f64 round-trip）走 eager
  `materialize_rows` + 批路径 vs 列式路径，`iter_data_records()` 逐字段（name/
  meta/value）逐位相等，wfx_id 字节一致。
- 四 crate 全量测试通过（159+481+526+165）。端到端 `[clean]`、EMIT 在采样下界内。

### 14.3 结果（修复后工具，8:8，load 8.7-13.3 重载下）

| 轮 | EPS | load |
|---|---|---|
| 1 | 12.11M | 8.7 |
| 2 | 12.17M | 11.4 |
| 3 | 12.17M | 13.3 |

- eager 基线（同口径）~10M → **12.1M（+20%）**，且 load 越高越稳（规则每行工作
  变小后对后台干扰不敏感）。
- 中间教训：第一版 wfx_id 每行重建 N 个 Value + 排序反而更慢（eager 的 HashMap
  已建好、哈希只借用）——改为**批级预排序 + 列直渲**后转正。
- 剩余主墙：each 主体每行仍有 fired_at 格式化 + wfx_id 哈希 + builder 逐格填充
  （L3 输出列式化是下一步）。

## 15. cut C 实测：builder 输出填充是主墙（2026-08-18 傍晚）

### 15.1 切法

`execute_each_direct_batch_columnar`（each_exec.rs）里用 `if false` 包裹
`builder.begin_row()` + yield 逐格 `stage_yield_cell` + `commit_each_row`
（wfx_id/score/entity/fired_at 等系统列推入），**保留** entity_id 取列、fired_at
格式化、wfx_id 哈希的计算——只切「填充」本身，隔离 L3 输出列式化的收益上限。

### 15.2 结果（修复后工具，8:8，load 8.9-9.4）

| 轮 | EPS | load |
|---|---|---|
| 0（warmup） | 31.39M | 8.9 |
| 1 | 33.12M | 9.0 |
| 2 | 32.28M | 9.4 |

- 基线（填充开）12.1M → **cut C（填充关）均值 ~32.3M（+167%）**。
- **builder 输出填充 = 每行 ~63% 工作**（(32.3-12.1)/32.3），比设计文档二分法①的
  ~57% 估计还高——**L3 输出列式化是下一个主攻点，收益上限约 2.7×**。
- 剩余 32.3M 仍含：wfx_id 哈希（cut A，未测）+ fired_at 格式化（cut B，~2-5%）
  + entity 字符串化 + 行访问开销。

### 15.3 cut A / cut D 实测：wfx_id 与 entity 都不是墙

| 切点 | 做法 | EPS | 结论 |
|---|---|---|---|
| cut A | `build_each_wfx_id_columnar_reusing` no-op（常量串） | 12.2-12.4M | wfx_id 哈希+渲染 ≈ 1-2%，**批级预排序 + 列直渲已近最优** |
| cut D | entity_id 取列+`value_to_string` no-op（常量串） | 12.1-12.3M | entity 字符串化 ≈ 0-2%，不是墙 |
| cut B | `format_nanos_utc` no-op（空串） | ~11.6M | fired_at 格式化 ≈ 2-5%（噪声内） |

**12.1M 每行预算最终归因**（修复后工具）：

- **输出侧 = ~63%**（cut C：yield 值读取/coerce + 逐格 stage + commit 系统列）→ 唯一大墙
- fired_at ≈ 3%、wfx_id ≈ 2%、entity ≈ 0%（三者合计 <10%）
- 其余 ~30% 弥散：分段循环（ALERT_BATCH_SIZE=256，每段重做 plan 特化
  score/entity/yield_kinds 两次 Vec 分配）+ 逐行遥测原子 + 行构造/访问
  + hit 掩码/时间列读取——已到二分法切刀分辨率极限，继续归因需 CPU profiler
  （samply/perf），而非再切。

**行动结论：L3 输出列式化（batch 写列）是唯一值得实施的优化，收益上限约 2.7×；**
wfx_id/entity/fired_at 不值得单独优化。

### 15.4 教训：git checkout -- 会从 index 还原（差点丢实现）

切刀还原用 `git checkout -- <file>` 时，若该文件的改动**从未 `git add`**，会从
index（=HEAD，无列式实现）整体回滚，不是「还原切刀」——本次差点丢掉整个
`execute_each_direct_batch_columnar` 实现（从 dangling checkpoint 9da82f63
找回，wf-engine 481 测试重验全绿）。

**以后切刀纪律改为**：
1. 切刀前 `git add` 当前工作树（或记下 checkpoint commit）；
2. 还原切刀用**手动反向编辑**（撤销 TEMP 标注的几行），或 `git show
   <checkpoint>:<path> > <path>` 恢复；
3. 禁用 `git checkout -- <path>`（未 add 文件会被静默回滚到 HEAD）。

## 16. 微基准归因：each_bench（2026-08-18 晚，数据版）

### 16.1 方法与口径

端到端切刀受 load / 全链路其他环节干扰，为拿到每个分量的单线程绝对成本，新增
`each_bench.rs`（wf-engine tests，release-only `#[ignore]`）：

```
cargo test --release -p wf-engine each_bench -- --ignored --nocapture
```

构造 Q1 真实形状（`q1_bid_passthrough`：score=1.0、entity=b.auction、yield
4 字段 id/alert_type/detail/request_count、7 列 bid_events 批、1M 行、
256 行分段同生产 ALERT_BATCH_SIZE），同一进程内依次测每个分量的 ns/行：

| 分量 | ns/row | 占 baseline | 备注 |
|---|---|---|---|
| baseline（完整列式） | 621.6 | 100% | `execute_each_direct_batch_columnar` 全量 |
| fill（cut C） | 369.1 | 59.4% | begin_row + 4×stage + commit（常量 wfx_id/entity/fired_at） |
| wfx_id（cut A） | 220.5 | 35.5% | `build_each_wfx_id_columnar_reusing` 全字段列直渲 |
| entity（cut D） | 60.8 | 9.8% | 取列 + `value_to_string` |
| fired_at（cut B） | 27.5 | 4.4% | `format_nanos_utc` |
| string_alloc 裸 | 16.3 | 2.6% | 每行一次 40B hex String 分配（black_box 防优化） |
| f64_format 裸 | 26.4 | 4.3% | 单次 f64 Display（wfx_id 对 4 个数字列各做一次） |

三轮稳定（baseline 594/611/622；fill 57-60%；wfx_id 35%；entity 10%；fired_at 4.4%）。
wfx_id 的 220ns 分解：4×f64 格式化（≈106ns）+ 3×Utf8 push + FNV 哈希 + 列 downcast + hex。

### 16.2 与端到端切刀的对照（重要）

| 分量 | 微基准（单线程） | 端到端切刀（8 并行全链路） | 一致？ |
|---|---|---|---|
| fill | 59.4% | cut C：+167%（≈63%） | ✓ |
| fired_at | 4.4% | cut B：~2-5% | ✓ |
| wfx_id | 35.5% | 单独 cut A：~0%；**A+C 同切比 C 高 21%** | 矛盾→已解释 |
| entity | 9.8% | 单独 cut D：~0% | 矛盾（待解释） |

**决定性实验**（A+C 同切，18:34，load 7.0-7.4）：36.5 / 41.4M（均值 ~39M）
vs cut C 单独 32.3M → **端到端下 wfx_id 真实贡献 ~21%，不是 0**。

**单独 cut A 无效的机制**：fill 仍在时每行 10 个 Arc 分配 + 列写产生的内存
带宽/分配器竞争掩盖了 wfx_id 的纯 CPU 成本；A+C 同时切掉 fill 后 wfx_id 的
成本才显现。cpu_avg 佐证：C 单独 693-832%（~7-8 核）→ A+C 556-584%
（~5.5-6 核）——规则任务释放的 CPU 没有全部转化为 EPS，**parse/窗口/sink/
分配竞争在全链路中接棒成为新瓶颈**。

微基准单独段系统性偏高 ~9%（分量之和 678 vs baseline 622）——单独循环 vs
execute 内联上下文的差异，占比数字应向下修正该量级。

### 16.3 行动结论（两种口径交叉验证后）

1. **L3 输出列式化（fill）仍是第一目标**：两种口径一致 ~60%（微基准 59.4% /
   端到端 cut C 63%）。
2. **wfx_id 列渲染第二**：微基准 35.5%，端到端 A+C 差值 ~21%。其中 4×f64
   格式化 ≈106ns（占 wfx_id 一半）——L3 批量化时优先做数字列批量格式化。
3. entity（9.8%）与 fired_at（4.4%）单线程占比小，端到端更小，不单独优化。
4. 端到端「单独切 A/D 无效」≠「无成本」——全链路瓶颈会接棒，微基准才是
   单线程成本的干净口径；两者结合才是完整归因。
