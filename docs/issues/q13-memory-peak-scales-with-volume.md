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

### 候选主因（† **已自我推翻一半，待探针消融定论**）：在途通道缓冲预算

代码常量：`RULE_CHANNEL_CAPACITY = 256`（`lifecycle/spawn.rs:38`）是
**每个规则分片通道**的容量。q13b 有 10 个 RoundRobin 分片（bench r=10）：

```
10 分片 × 256 批/分片 = 2560 批在途
× bid_mod 批 3.22MB（实测，见下）
= ~8.2GB 名义预算
```

**批次尺寸实测**（峰值时刻 `window.memory_bytes / batches / rows`，30M）：

| 窗口 | 每批字节 | 每批行数 | 每行字节 |
|---|---|---|---|
| bid_mod | **3.22MB** | 35,360 | 91B |
| bid_events | 7.43MB | 36,199 | 205B |

bid_mod 声明仅 6 个 int64（应 48B/行），实测 91B/行——差额是**中间窗自带的
meta 列**：`__wfu_rule_name` / `__wfu_entity_type` / `__wfu_entity_id`（字符串
+ offsets）、`__wfu_score`、`__wf_pipe_ts`。

> † **这个加法很可能是错的（勿直接当结论用）**：通道里 `RulePush` 持有的是
> `Arc<RecordBatch>`，与窗口里那份是**同一批 Arrow 缓冲**（`flush_pipes` 的
> append 与 broadcast 都是浅拷）。而窗口驱逐又被 ack floor 门控——**未 ack 的
> 批次正是通道里那些，窗口本来就在留**。所以通道条目大概率**不产生额外
> 内存**，已计在 `window_bytes` 内。旁证：30M lag 峰值 534 批 × 3.22MB =
> 1.7GB ≈ bid_mod 窗峰值 1.81GB（两者重叠，非相加）。

### 另一个强信号：RSS 跑批间波动巨大 → 疑瞬时 churn 而非结构持有

**同一 30M 配置**多次跑批：RSS_peak 8.4 / 9.9 / 11.6 / 13.5 / 13.8 / 15.3GB
（load 8.0–10.8）。这种幅度不像稳定结构体，更像**分配器瞬时 churn / 碎片**
（alert 构建、parse 缓冲、列式临时量）叠加负载影响。

### ✅ 已定案（分配器分账 + 堆归因）：**在途 Arrow 数据量，非泄漏、非分配器**

#### 第一步：分账（`alloc.*` 指标，30M）
| 指标 | 值 |
|---|---|
| `peak_commit` | 16.75GB |
| `peak_rss` | 15.38GB |
| 窗口合计 | 3.60GB |
| **peak_commit − 窗口** | **13.16GB** |

#### 第二步：分配器**被排除**（关掉 mimalloc 对照，q13a-only 30M）
| 分配器 | RSS_peak |
|---|---|
| mimalloc（基线） | 18,521MB |
| mimalloc + `PURGE_DELAY=0 ABANDONED_RECLAIM_ON_FREE=1` | 18,345MB |
| **系统分配器**（mimalloc 关掉） | **17,916MB** |

三者几乎相等 → **mimalloc / arena / abandoned 页都不是因**（虽然 stats 确实显示
98.7% 页 abandoned、reserved 20GiB/14 arenas，但换分配器后内存不变 → 那是
**现象而非原因**）。

#### 第三步：同条件对照定位到 pipe 路径（30M）
| 规则 | peak_commit | 窗口 | 非窗口 | EPS |
|---|---|---|---|---|
| **q1**（each → **sink**） | 4.30GB | 3.21GB | **1.09GB** | 18.1M |
| **q13a-only**（each → **中间窗**） | ~21GB | 3.87GB | **~17GB** | 6.3M |
| q13a+q13b | 16.75GB | 3.60GB | 13.16GB | 3.3M |

同一 bid_events 输入、同样 each 形状、同样 10 分片——差异只在输出去向。
**注意：EPS 越低 → 内存越高**（这是下面结论的关键伏笔）。

#### 第四步：堆归因（`heap` 尺寸分级 + `malloc_history` 调用栈）
全速 30M 采样（`heap -s`）：2270 万个活跃分配，其中 16B×1509万 +
32B×756万——看似骇人，但按字节只 ~0.5GB。

重度插桩跑（`MallocStackLogging` + `malloc_history -allBySize`）的成分：
| 尺寸 | 数量 | 合计 | 归因（调用栈） |
|---|---|---|---|
| ~6.9–7.5MB | **337** | **~2.4GB** | `decode_ipc_trusted` → `MutableBuffer::from_len_zeroed`（IPC 帧体） |
| 80KB–528KB | ~11k | ~2.0GB | Arrow 列缓冲（含 `flush_pipes → take_batch → Int64Array`） |
| 16B/32B | 5.6M | ~0.15GB | 零碎小对象（**非主体**） |

**关键反证**：该插桩跑 EPS 仅 118k（极慢）时，footprint 4.9GB / malloc 活跃
5.16GB——**缺口几乎消失**。而全速时是 14–18GB vs 窗口 3.9GB。

#### 结论（已根据四个数据点修正：**不是在途积压，是 pipe 写入分配速率**）

四个数据点放一起，内存随 **pipe 写入速率**单调上升：

| 配置 | pipe 写入速率 | 非窗口内存 |
|---|---|---|
| q1（**不走 pipe**） | 0 | **1.09GB** |
| 插桩慢跑 q13（MallocStackLogging） | 0.12M 行/s | ~1GB |
| q13 全链（被 q13b 限速） | 3.3M 行/s | 13.16GB |
| **q13a-only**（无下游拖慢） | **6.3M 行/s** | **17GB** |

**关键反例**：q13a-only 是最快的 q13 变体、**没有任何下游积压**，却最费内存；
插桩慢跑最慢反而最省。所以"下游慢 → 积压 → 内存"**不成立**。

正确关系：**内存 ≈ pipe 路径分配速率 × 分配存活时间**。所以方向不是"加快下游"
也不是"扩大/缩小缓冲预算"（两者已分别被 q13a-only 与 parse 预算实验否决），
而是**降低 pipe 写入路径的单行分配足迹**。

→ 这把下方那条 `Vec<Option<i64>>` 优化从"顺带"提升为**主线候选修复**。

#### 下一步（指标已成必需品：所有猜测均已被实测否决）

已逐一否决：窗口保留、1over 参数、通道条目（Arc 共享）、分配器/arena、
小对象（0.15GB）、alert 输出基数（q1 反例）、`parse_buffer_bytes` 预算。
所以不能再猜，必须先把在途量变成可观测量：

1. **`parse.inflight_bytes`（已用/预算）** —— 预算 2GB 但调到 256MB 内存不变，
   必须看**实际已用值**才知道预算是否真是约束（若已用远小于预算 →
   矶颈在其他环节）。
2. **`receiver.decode_inflight_bytes` + 解码膚胀率** —— 7MB 帧体 × 337 活跃是
   唯一被实证的大块，但并存深度上界未知。
3. **`window.mailbox_permits_used`（每窗）** —— 确认 64MiB×7 是否真的只 0.45GB。
4. **未归因残差** = `alloc.peak_commit − Σ窗口 − Σ以上` —— 唯一能证明"账对上了"
   的量。

#### 主线候选修复（已实证的分配足迹浪费）
`malloc_history` 抓到 `flush_pipes → take_batch → Int64Array::from(Vec<Option<i64>>)`：
`PipeCol` 用 `Vec<Option<i64>>` 暂存（**16B/值**，35k 行×11 列 ≈ 6.2MB/批），再
**拷一份**进 Arrow 缓冲（8B/值 + null bitmap）。改用 Arrow builder 直写（或
`Vec<i64>` + 独立 null buffer）可同时省一半暂存与一次全量拷贝。

按上面的关系（内存 ∝ 分配速率），这是**直接作用于每行分配足迹**的修复，
预期比调缓冲参数有效（后者已被否决）。验证方式：先用 `memory_probe`
（计数分配器）在单测里量化 take_batch 的 peak_growth 降幅，再跑 q13a-only
30M 看 RSS（当前基线 18.3–18.5GB）。

## 4. 已尝试 / 已排除

| 方案 | 结果 | 结论 |
|---|---|---|
| 广播无条件裁剪 batch-only | 3 测试 FAIL（`push.events` 契约） | 必须按订阅类型 |
| **广播按订阅类型裁剪**（`53aca64`） | 30M EPS 1.52M→4.06M、RSS 28.8→9.9GB | **保留，勿回退** |
| 回退 q13a 单 worker | 1.52M EPS、5.9GB | 性能倒退，不做 |
| 单点 footprint 采样判定"RSS 虚高" | 误判（撞低谷） | 必须整程采样 |
| **`bid_mod over = 1h → 1m`** | **无效**：bid_mod 峰值 1.69GB→2.03GB（噪声内略高）、窗口合计 3.87→4.25GB、RSS 13.8→13.5GB 不变 | **已否决**：时间驱逐被 ack floor 门控（`evictor.rs:130`），未被 q13b ack 的批次（lag 峰值 369–765）`over` 再小也删不掉——**保留量由消费滞后决定，不由 over 决定** |
| **`parse_buffer_bytes` 2GB → 256MB** | **无效**：RSS 15.2GB（原 14–15GB）不变，EPS 3.3M→2.87M 反而变差。已核实 `/tmp/bench_conf.toml` 生效 | **已否决**：虽然参数注释写明"在途 = 预算 × IPC 解码膚胀 ~10×，下游卡顿时 RSS 可近 10×预算"，但实测不成立——本数据集的膚胀约 1×（bid_events 内容 7.43MB/批 ≈ IPC 帧体 6.9–7.5MB） |
| **`window_buffer_bytes`**（默认 64MiB/窗） | 未单独测 | 名义上只 7×64MiB≈0.45GB，量级不对应 |

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
- 全局 100M 内存盘点（2026-08-25 全量跑批，RSS>10G 判定为问题，**共 8 个**）：
  q19 32.9GB / q22 30.0GB / q13 26.4GB / q18 24.2GB / q16 22.9GB / q14 18.0GB /
  q5 17.5GB / q17 14.9GB
  
  **是否同源：目前不能断定，且有硬反例**。
  - 反例：**q4 也是双规则链 + 中间窗**，100M 仅 8.68GB → "pipe 链/中间窗/
    广播携带 events" **不是**其他 Q 高内存的驱动因素；本次修复（广播裁剪）
    只作用于中间窗生产者（q4/q13），**对其余 6 个无帮助**。
  - 性质可能不同：**无规则状态类**（q13：stateless each + 静态表 join）的
    非窗口内存是**真异常**；**重状态类**（q16/q18/q19：stats/dedup/top-N）的
    非窗口内存可能是**合法规则状态**（不计入 window_bytes，且键基数随
    nexmark 的 auction/person id 增长）——需先区分"合法状态"与"异常占用"。
  - **分账已就绪**：`alloc.*` 指标接入后，一轮 `./bench.sh all replay 100m`
    就能给 22 个 Q 全部分账（window_bytes / peak_commit / peak_rss），一次看清
    哪些是引擎真持有、哪些是合法状态。
