# q13 内存峰值随数据总量增长（30M 14GB / 100M 26GB）

> 状态：**待修复** · 2026-08-25 · 优先级：高（违反"流式内存与总量无关"目标）
> 关联：`notes/q13-side-input-join-progress.md`（M-13 分片根治段，性能达标态）
> 复现场景：`wf-examples/performance/nexmark_pk` → `./bench.sh q13 replay 30m|100m`
> 前置修复（已 commit `53aca64`）：广播按订阅类型裁剪 + q13b 列式化
> 类比先例：`window-overload-drop-vs-backpressure.md` §3.3（事件时间压缩放大）

---

## 1. 症状

q13（q13a→bid_mod→q13b 双规则链，分片态）内存峰值**随数据总量线性增长**：
30M 峰值 14GB、100M 峰值 26GB。流式处理下应与总量无关。

性能本身已达标（EPS 3.2–4.1M > ingest 3M/s，正确性 `[clean]`）——**纯内存问题**。

## 2. 复现证据（footprint 整程采样，macOS 权威口径）

### 2.1 峰值 ∝ 数据总量（锯齿形积累/释放）

`footprint -p wfusion --sample 1 --sample-duration N`（footprint = dirty +
compressed，**不含** reclaimable，是 macOS 内存权威口径）：

| 规模 | footprint 曲线（GB，每 1–2s 采样） | 峰值 | bench RSS_peak |
|---|---|---|---|
| 30M | 5.9→7.9→9.0→10→12→13→**14**→10→6.1 | **14GB** | 14.4GB |
| 100M | 5.4→8.3→11→15→19→21→25→**26**→19→7.5→9.2→11→13→17→**20**→10→7.2→5.6→3.5 | **26GB** | 26.3GB |

**结论**：footprint 与 ps RSS 吻合 → **RSS 没有虚高，内存确实随总量增长**。
形态是**单调爬升 → 批量释放**（100M 出现两轮波峰），即消费追不上摄入期间的
持续积累，不是稳态占用。

> ⚠ **已纠正的误判**：先前单点 footprint 采样（30M 3355MB dirty / 100M 3407MB
> dirty）曾被解读为"真实占用与总量无关、RSS 虚高"。那是**撞上释放低谷**的
> 采样偏差——整程采样证明峰值确实 14GB / 26GB。**勿再用单点采样下结论。**

### 2.2 成分拆解：**窗口有界，非窗口部分才随总量翻倍**

`metrics.ndjson` window.memory_bytes 峰值实测（非估算）：

| 成分 | 30M | 100M | 随总量增长？ |
|---|---|---|---|
| bid_mod 窗（over=1h） | 1.69GB | 2.81GB | 弱（1.7×） |
| bid_events 窗（over=30m） | 1.52GB | 2.92GB | 弱（1.9×） |
| auction + person 窗 | 0.66GB | 0.72GB | 否 |
| **窗口合计** | **3.87GB** | **5.7GB** | **弱（1.5×）** |
| footprint 峰值 | 14GB | 26GB | — |
| **非窗口部分（差值）** | **~10GB** | **~20GB** | **是（2×）** |

**关键结论**：窗口保留量由 `over × 事件时间速率` 决定，**已被有界**——数据是
官方 NEXMark 口径（`INTER_EVENT_DELAY_NS = 100µs/事件`），事件时间跨度 =
N × 100µs：

| 规模 | 事件时间跨度 | bid_events(over=30m) 保留 | bid_mod(over=1h) 保留 |
|---|---|---|---|
| 10M | 16.7 min | 全部（跨度 < over，**无法老化**） | 全部 |
| 30M | 50 min | 30/50 → 18M 行 | 全部 30M 行（跨度 < 1h） |
| 100M | 167 min | 30/167 → 18M 行 | 60/167 → 36M 行 |

两个规模的窗口保留行数量级相同（18M + 30~36M 行）→ 与实测 3.87GB / 5.7GB 吻合。
**所以 12GB 的峰值差额几乎全在窗口之外**，H1 不是主因。

- bid_mod `acked_lag` 峰值：30M **369** 批 vs 100M **765** 批（~2×，与非窗口
  内存 10GB→20GB 的 2× 同步）
- 段区（footprint 分类）：IOAccelerator reclaimable 100M 23GB / 30M 12GB
  （= mimalloc 已 purge 未还 OS 的段，**不计入 footprint**，只抬高 ps RSS 瞬时
  读数——不是问题主体）

### 2.3 其他事实

- 正确性始终 `[clean]`（memory_evicted_total=0），EPS 3.2–4.1M > ingest 3M/s
- 时间驱逐在工作：evict=248（100M）/ 35–86（30M）
- RSS 随机器负载波动明显：同 30M 在 load 9.3–9.6 时 8.4–9.9GB、load 10.6–10.8 时
  14–15GB → **负载越高 → 消费越滞后 → 积累峰值越高**（与积压假说一致）

## 3. 根因（已收窄到缓冲预算尺寸，**非泄漏**）

### 已排除：H1 窗口无法老化
事件时间跨度算术（§2.2）+ 窗口实测证明窗口保留有界（30M 3.87GB / 100M 5.7GB），
只解释 1.8GB 差额，**不是 12GB 峰值差的主因**。
（注：10M 跨度 16.7min < 两个 over，窗口全量保留——小规模下反而是窗口主导。）

### 主因（高度可信，待测试验证）：**在途通道缓冲预算过大 + Arc 共享使其隐形**

代码常量：`RULE_CHANNEL_CAPACITY = 256`（`lifecycle/spawn.rs:38`）是
**每个规则分片通道**的容量。q13b 有 10 个 RoundRobin 分片（bench r=10）：

```
10 分片 × 256 批/分片 = 2560 批在途
× bid_mod 批 ~2MB（实测：1.69GB / ~845 批）
= ~5.1GB 单 q13b 通道预算
```

另：sink alert 通道 `SINK_CHANNEL_CAPACITY = 2048` 批 ×
`ALERT_BATCH_SIZE = 4096` 条/批 = 8.4M 条在途 × ~150B ≈ **1.3GB/sink**
（q13 每行 bid 一条 alert，通道长期接近满）。

**为何窗口会计看不见**：`flush_pipes` 把**同一批次** append 到窗口并广播，
Arrow 缓冲经 `Arc` 共享（`batch.clone()` / `Arc::new(b.clone())` 都是浅拷）。
窗口驱逐后，**只要通道里还有在途条目持有 `Arc`，这些缓冲仍然活着，但已不计入
`window_bytes`**——正是那 10→20GB。

**为何增长是次线性**（3.3× 数据 → 1.86× 内存）：有界缓冲在 30M（ingest ~9s）
没填满，100M（~30s 持续满载）才填到接近上限 → **渐近于预算总和，而非无界增长**。
推推：300M 应仍 ≈ 26–30GB（预算封顶）。

次要因素：mimalloc 分配峰值水位（高频 alert 构建 × 10 worker），受机器负载
放大（同 30M：load 8.9→13.8GB、load 9.3→9.9GB）。

## 4. 已尝试 / 已排除

| 方案 | 结果 | 结论 |
|---|---|---|
| 广播无条件裁剪 batch-only | 3 测试 FAIL（`push.events` 契约） | 必须按订阅类型 |
| **广播按订阅类型裁剪**（`53aca64`） | 30M EPS 1.52M→4.06M、RSS 28.8→9.9GB | **保留，勿回退** |
| 回退 q13a 单 worker | 1.52M EPS、5.9GB | 性能倒退，不做 |
| 单点 footprint 采样判定"RSS 虚高" | 误判（撞低谷） | 必须整程采样 |
| **`bid_mod over = 1h → 1m`** | **无效**：bid_mod 峰值 1.69GB→2.03GB（噪声内略高）、窗口合计 3.87→4.25GB、RSS 13.8→13.5GB 不变 | **已否决**：时间驱逐被 ack floor 门控（`evictor.rs:130`），未被 q13b ack 的批次（lag 峰值 369–765）`over` 再小也删不掉——**保留量由消费滞后决定，不由 over 决定** |

## 5. 下一步（测试驱动，已备好度量设施）

### 已落地：分配器级内存度量（`wf-runtime/src/memory_probe.rs`）
把内存度量从"bench + 外部 footprint 采样"变成确定性 `cargo test`：
- `CountingAlloc`：测试构建的 `#[global_allocator]`（包装 `System`，原子计数
  current/peak）；生产二进制不受影响（wfusion CLI 用 mimalloc）。
- `MemoryProbe::exclusive()`：取全局锁串行化 + 重置 peak 基线；
  `peak_growth()` = 相对基线的峰值增量（**规模对比断言用这个**）。
- 自测：`counts_allocations_and_tracks_peak`、
  `peak_reflects_concurrent_not_cumulative`。
- 口径：统计**请求字节**（`Layout::size()`），不含分配器元数据/碎片，
  系统性低于 RSS——用于**同一测试内的 N vs 3N 对比**，不与 RSS 绝对值对齐。

### 待做：用探针验证预算假说
1. **在途通道占比**：驱动 q13a→bid_mod→q13b 链（消费故意慢于生产），
   N 与 3N 两档比 `peak_growth()`。预期：峰值封顶于通道预算（不随 N 线性）
   → 坐实"预算尺寸问题"；若线性增长 → 另有无界项。
2. **消融实验（ablation）**：分别只跑 ① 窗口 append、② +each 发 alert
   （sink 正常 drain）、③ +中间窗链慢消费，比各档 `peak_growth()` 差值
   → 归因到具体路径。
3. **验证修复**：调小 `RULE_CHANNEL_CAPACITY`（如 256→32）与/或
   `SINK_CHANNEL_CAPACITY`，先在测试里看 `peak_growth()` 降幅，再跑 bench 确认
   EPS 不降（通道是吸波器，过小会损失突发吸收能力——**预期有 EPS/内存
   权衡，用数据定点**）。
4. 目标：100M 峰值 < 10GB，EPS 维持 3.2M+。
5. 修完回归其余高内存 Q（§6）——若确为通道预算，则 q5/q14/q16/q18/q19/q22
   很可能**同源**（分片数 × 256 批），一处修复多处受益。

## 6. 关联

- `notes/q13-side-input-join-progress.md`（M-13 分片根治 + q13a/q13b 列式化）
- `issues/window-overload-drop-vs-backpressure.md`（§3.3 事件时间压缩放大，
  §7 源反压设计——H2 若需背压可复用）
- 全局 100M 内存盘点（2026-08-25 全量跑批，RSS>10G 判定为问题）：
  q5 17.5GB / q13 26GB / q14 18GB / q16 22.9GB / q18 24.2GB / q19 32.9GB /
  q22 30GB——**各 Q 可能同源（H1/H2），修 q13 后回归验证其余**
