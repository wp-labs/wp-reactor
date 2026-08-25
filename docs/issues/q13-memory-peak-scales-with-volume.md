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

## 3. 根因（已收窄到"非窗口部分"，待定位具体持有者）

### 已排除：H1 窗口无法老化
事件时间跨度算术（§2.2）+ 窗口实测证明窗口保留有界（30M 3.87GB / 100M 5.7GB），
只解释 1.8GB 差额，**不是 12GB 峰值差的主因**。
（注：10M 跨度 16.7min < 两个 over，窗口全量保留——小规模下反而是窗口主导。）

### 待定位：非窗口部分 10GB→20GB

候选持有者（按 q13 特征排序）：

1. **alert 输出路径 ∝ N**（最可疑）：q13 语义 = **每行 bid 产出一条 alert**
   （30M 事件 → ~14M alerts；100M → ~46M alerts）。输出基数随总量线性增长，
   若 alert 构建/序列化/dispatch 落后于产出，`AlertColumnBuilder` 批与通道
   积压 ∝ N。旁证：`alert.channel_depth` 峰值 629；早前 `sample` 的 malloc
   热点正是 `AlertColumnBuilder` + `RawVec<DataType,Value>`。
2. **q13b 消费滞后的在途批次**：bid_mod acked_lag 369→765。但 pull 模型下未
   ack 批次仍计在 window_bytes 内，可能与窗口成分重叠、非独立增量。
3. **mimalloc 分配峰值水位**：高频 alert 构建 × 10 worker 并发 → 段区借入。
   受机器负载放大（同 30M：load 8.9→13.8GB、load 9.3→9.9GB）。

**下一步定位手段**：峰值时刻 `vmmap <pid>` 的分类明细（footprint 摘要不够细），
看 MALLOC_LARGE / MALLOC_HUGE 区与 Arrow 缓冲的占比；配合 `alert.channel_depth`
与 `alert.dispatch_total` 的时间曲线判断 alert 路径是否为积压源。

## 4. 已尝试 / 已排除

| 方案 | 结果 | 结论 |
|---|---|---|
| 广播无条件裁剪 batch-only | 3 测试 FAIL（`push.events` 契约） | 必须按订阅类型 |
| **广播按订阅类型裁剪**（`53aca64`） | 30M EPS 1.52M→4.06M、RSS 28.8→9.9GB | **保留，勿回退** |
| 回退 q13a 单 worker | 1.52M EPS、5.9GB | 性能倒退，不做 |
| 单点 footprint 采样判定"RSS 虚高" | 误判（撞低谷） | 必须整程采样 |

## 5. 下一步（先数据，后动手）

1. **定位非窗口内存持有者**（唯一阻塞项）：100M 跑到峰值时刻抓 `vmmap <pid>`
   分类明细，对比 30M 同时刻——差额落在哪个区（MALLOC_LARGE/HUGE vs Arrow
   缓冲 vs 通道）。**勿用单点 footprint 摘要**（§2.1 已踩坑）。
2. **验证 alert 路径假说**：`alert.channel_depth` / `dispatch_total` /
   `emitted_total` 的时间曲线——积压是否全程增长且 ∝ N。若成立，修复方向是
   alert 侧背压或 dispatch 吞吐（不是扩大缓冲）。
3. **顺带可做的安全优化**：`bid_mod over = 1h` 对 q13 语义**无用**——q13b 的
   join 目标是 `side_input` provider **静态表**，bid_mod 只是链路中转窗，
   没有历史行需求。调小 over（如 1m）可省 1.7–2.8GB，**但需先确认无其他
   规则把 bid_mod 当 join 目标**（当前只有 q13b 消费）。
4. 目标：100M 峰值 < 10GB，EPS 维持 3.2M+。
5. 修完回归其余高内存 Q（§6），确认是否同源。

## 6. 关联

- `notes/q13-side-input-join-progress.md`（M-13 分片根治 + q13a/q13b 列式化）
- `issues/window-overload-drop-vs-backpressure.md`（§3.3 事件时间压缩放大，
  §7 源反压设计——H2 若需背压可复用）
- 全局 100M 内存盘点（2026-08-25 全量跑批，RSS>10G 判定为问题）：
  q5 17.5GB / q13 26GB / q14 18GB / q16 22.9GB / q18 24.2GB / q19 32.9GB /
  q22 30GB——**各 Q 可能同源（H1/H2），修 q13 后回归验证其余**
