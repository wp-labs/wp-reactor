<!-- 角色：引擎架构师 | 状态：设计方案 v2.2（评审后修正并源码实证：WFL 语法形态、持久化与 spill 解耦、消费侧聚合语义、yield 读取聚合值写法均已闭环验证；①dur=半衰期 ②存储=独立持久基线库(非spill) ③WARMUP=30 ④周期=一等公民） | 创建：2026-08-30 | 更新：2026-08-30 -->

# baseline() 在线基线能力设计方案

> 目标：把 `baseline(expr, dur, method)` 从"占位能力"（dur 被忽略、无持久化、语义夸大）升级为**可信的在线行为基线原语**，使 wfusion 从"检测已知模式/硬阈值"扩展到"检测每个实体自身历史的偏离"（UEBA 级）。
>
> 基线模型态落本地 `redb` 持久化（进程重启可 warm start）；引擎 CEP 匹配态仍纯内存、无 checkpoint 屏障。

---

## 1. 当前实现事实（已核对源码）

| 项 | 位置 | 现状 |
|---|---|---|
| `eval_baseline` 入口 | `wp-reactor/crates/wf-engine/src/match_engine/match_engine/eval/mod.rs:187` | 只读 `args[0]`（expr）与 `args.get(2)`（method，默认 `"mean"`）；**`args[1]`（dur）从未读取** |
| 状态键 | `mod.rs:207` | `format!("{:?}:{}", args[0], method)` —— **不含 dur**，不同 dur 撞同一 tracker |
| `RollingStats` | `types.rs:375` | 字段 `count/sum/sum_sq/method/ewma/ewma_alpha(0.3)/values(Vec 上限1000)`；`mean` 为实例启动以来全量累积；`ewma` 固定 α=0.3；`median` 最近 1000 值环形缓冲 |
| 状态挂载 | `state.rs:138` | `baselines: EngineHashMap<String, RollingStats>` 挂在 **match 实例** 上；实例创建默认空（`:178`）；淘汰/重启即 `clear()`（`:304`） |
| 内存记账 | `state.rs:244` | `size += self.baselines.len() * 128` |

**结论**：当前 `baseline(x, 30d, "ewma")` 的 `30d` 不产生任何"过去 30 天"语义，实为"实例启动以来累积统计量"；实例淘汰即从零冷启动。对外文档此前据此夸大，本方案收敛之。

---

## 2. 设计总览

基线能力分「主路径」与「辅助路径」两条，全用引擎现有机制，不新造特例状态机。主路径又由生产与消费两环串成：

- **主路径① 基线数据生产**：用 `stats` 窗口产出基线聚合记录 `BaselineRecord`（含 `n/sum/sum_sq`），经 `yield` + 现有 sink 落独立持久存储（本地 redb 或外部 DB）；重启经 `StaticWindowSchema`/`join snapshot` 重载。
- **主路径② 消费侧 API**：`baseline(近期窗口, 周期, 方法, 值)` 读取生产出的历史同相位记录，按 `周期` 取同相位集合、按 `方法` 合并，返回偏离度。
- **辅助路径 内联 `baseline()` 热路径糖**：个别场景需逐事件在线偏离（ewma 响应式、median 抗离群），保留内联 `baseline(expr, dur, method)` 管逐事件热路径，stats 窗口产出的持久化记录供 warm start。

两层分工：`stats` 窗口出**精确硬窗口**记录（无衰减、适合精确窗口/周期对比）；半衰期只作用于**消费侧跨周期合并**与**内联辅路径的逐事件衰减**，不产生同一参数的双义基线值。

---

## 3. 关键设计决策（D1–D6）

| # | 决策 | 默认 | 备选 |
|---|---|---|---|
| **D1** | 时间基准 | **事件时间**（与引擎窗口模型一致，支持重放/乱序） | 处理时间（退化，等同现状） |
| **D2** | 持久化 | **基线模型态落独立持久存储**（本地 redb 基线库或外部 sink）；CEP 匹配态仍纯内存。**不可复用窗口 spill**（无持久化语义，见 §4.5） | 仅内存（重启冷启动） |
| **D3** | 衰减模型 | **指数遗忘（O(1) 内存）**，非全量样本存储 | 真滚动窗口存样本（内存随 dur 增长） |
| **D4** | 预热保护 | `warmup < 30` 时 deviation 返回 0（`warmup` 为**整数样本计数**，非衰减权重 `count`） | 无保护 |
| **D5** | 状态键含 dur | `format!("{:?}:{}:{}", args[0], dur, method)` | 不变（会撞键） |
| **D6** | 实体隔离范围 | 在带 key 的 `match` 内 → 按 key 值隔离；否则 → 规则级单 tracker | 仅规则级 |

已闭环决策：① `dur`=半衰期（`lambda=LN2/dur`，权重 `0.5^(age/dur)` 平滑衰减从不为 0，非硬截断）；② 存储=**独立持久基线库**（本地 redb 或外部 sink，**严禁复用窗口 spill**，§4.5）；③ `WARMUP`=30（内置常量，不暴露为规则级参数）；④ `周期` 为 `baseline(近期窗口, 周期, 方法, 值)` 第二参数一等公民。

---

## 4. 基线数据生成与持久化（主方案：五原语复用）

### 4.1 生成：产出基线记录的 stats 窗口

形态参照两处已编译/在用的真实代码：stats 子句形态参照
`wf-examples/performance/nexmark_pk/models/queries/q19.wfl`；yield 读取聚合值的写法参照
`wf-lang/src/compiler/tests/coverage_extra.rs:272-289`（`stats<10s:fixed> group by (e.sip) { e | count as n; }`
+ `yield out (y = fmt("{}", stat.value(final(n))))`）。

```
rule baseline_producer {
  events { e: metrics_stream && e.value != null }
  stats<1h:fixed> group by (e.entity, e.metric) {   # 1h 固定窗(tumbling)，每窗收盘产出一条记录
    e | count          as n;                        # count 不接受字段参数
    e | sum(e.value)   as s;
    e | sumsq(e.value) as ss;                       # 需新增聚合，见 4.2
  }
  entity(str, e.entity)
  yield baseline_out (
    entity    = e.entity,
    metric    = e.metric,
    win_start = @window_start_time,
    win_end   = @window_end_time,
    n       = stat.value(final(n)),          # 读 measure 终值，见下
    sum     = stat.value(final(s)),
    sum_sq  = stat.value(final(ss))
  )
}
```

**语法约束（已核对 parser）**

- **`stats<1h:fixed>`，不能用 `hop`**：stats 子句只接受 `duration[:fixed|session]`（`wfl_parser/stats_p.rs:79-95`），**无 `hop`、无 `(size, slide)`**；`hop(10s, 2s)` 是 `match<...>` 的窗口规格。故基线的"30 天"**不由单个窗口承载**，而由消费侧按 §5.3 对 ~720 条小时记录做半衰期加权合并形成。
- **measure 语法** = `alias | agg(field) as label`，多条以 `;` 分隔（`stats_p.rs:160`）。
- **`count` 不接受字段**（`count as n`）；`sum/avg/min/max/distinct_count/last` 需 `(field)`（`stats_p.rs:191-221`）。
- **`group by` 必须带括号**：`group by (e.entity, e.metric)`（`stats_p.rs:97-113`）。
- **窗口系统变量**是 `@window_start_time` / `@window_end_time`（`expr.rs:265-266`），不是 `$window_start`。

**measure 标签如何进 yield（已验证可行，无需语言扩展）**

`yield` 中读取 measure 值须用 **`stat.value(final(label))` 选择器**，不是裸标识符（`n = n` 无效）。

- 作用域注入：`populate_stats_measure_labels`（`checker/rules/mod.rs:291`）把每条 stats measure 的标签写入 `scope.stat_labels`，且**全部标记为 `StatLabelStage::Close`**——即"度量在窗口关闭时产出终值"，正合基线"每窗收盘产出一条记录"的语义。
- 语法佐证：`compiler/tests/basic.rs:460`、`coverage_extra.rs:277/300` 均有 `stat.value(final(label))` 的编译通过用例；`coverage_extra.rs:754` 注明 "`stat.value(final(label))` reads no event fields"。
- 故 `final(...)` 不是可选修饰：stats measure 一律落在 Close 阶段，必须写 `final`。

> 附注：q19 之所以**没有**用到 `stat.value(...)`，是因为它的需求形态不同——`top(10, b.price)` 是**选择器**（挑出 top-10 行），载荷是这些行的原始字段，经 `field_values` 注入 `b.*` 即可，聚合标签本身无需进 yield。基线恰好相反：**聚合结果本身就是载荷**（`n/sum/sum_sq` 就是要存的东西），因此必须走 `stat.value(final(...))`。q19 是"不需要"而非"不支持"，不构成先例。

- 周期分桶**不需要**写进 `group by`：消费侧按记录的 `win_start` 筛同相位窗口即可（如"此前各周一 10:00–14:00"）。
- `sumsq` 是为基线新增的极小聚合（见 4.2）。

### 4.2 基线的数据结构 = 这条 yield 出来的记录

```
BaselineRecord {
  entity, metric,             # 隔离键（单体/群体由 group by 维度决定）
  win_start, win_end,        # 时间窗（季节基线再按 period_bucket 分桶）
  n: u64, sum: f64, sum_sq: f64
}
```

重载时：`mean = sum/n`，`std = sqrt(sum_sq/n - mean²)`，z-score 直接算——**不需要 stats 原生支持 stddev**。

**为什么存 `sum_sq` 而非 `std`**（已核对源码）：`StatsAggPlan`（`wf-lang/src/plan.rs:204`）只有 `Count/Sum/Avg/Min/Max/DistinctCount/Last/Top`，**无 `StdDev`/`Percentile`**。存 `n+sum+sum_sq` 复用已有 `Sum`/`Count` 聚合，只需给 stats 补一个极小的 `SumSq` 聚合（或退化存 `sum/count` 用 min/max 近似）。

`SumSq` 需在三处落地：① parser 的 `stats_agg`（`wfl_parser/stats_p.rs:191`）——按 `sum` 同形接受 `(field)`；② `StatsAggPlan`（`plan.rs:204`）枚举 + 求值实现；③ checker 的 `populate_stats_measure_labels`（`checker/rules/mod.rs:298`）补 `StatsAgg::SumSq => Measure::Sum` 映射（当前兜底分支会落到 `Measure::Count`，仅影响 `stat.value(...)` 的类型校验精度，不影响取值）。

### 4.3 完整闭环（全用现有机制，无新状态机）

| 环节 | 复用机制 | 说明 |
|---|---|---|
| 生成 | `stats<dur:fixed>` + `group by` | 1h 固定窗（tumbling），每窗每键产出一行聚合（`Rows` 输出已支持） |
| 产出 | `yield baseline_out` | 基线作为**普通输出记录**经现有 sink 落库；聚合值经 `stat.value(final(label))` 读出（§4.1） |
| 持久化 | sink 落库 / 独立持久基线库 | 1h 窗口内存有界（每窗每键仅一行聚合），**不需要 spill**；长周期基线由消费侧合并历史记录形成，而非靠单个大窗口 |
| 重启重载 | `StaticWindowSchema`(`over=ZERO`) / `join snapshot` | 把最近基线记录当参考表载回引擎 |
| 消费 | `join baseline_table snapshot on e.entity == baseline_table.entity` | 取最新基线，`x > mean + 3*std` 即异常（模式关键字在源名**之后**，见 `clauses.rs:471-473`） |

### 4.4 五原语自洽与痛点消除

- **Redis 误用** → 基线走自有 sink/存储，不经过 Redis 知识库（`wp_knowledge` 仅读）；
- **持久化** → `yield`→`sink` 是引擎现成的持久化路径：走外部 sink 时**无需新模块**；走本地 redb 时需一个极小的基线存储模块（复用 redb **库依赖**，不是 spill 机制），见 §4.5；
- **季节/周期** → 每条基线记录自带 `win_start`，消费侧按其筛同相位窗口即可（§5.1），**无需**在 `group by` 里加时间桶；
- **可解释性** → 基线是看得见的记录，不是引擎内部 opaque 状态；
- **五原语自洽** → Stats(生成) / Yield(产出) / Bind+Join(重载) / Match(检测) 全在语义内核内，没有"特例函数"。

### 4.5 持久化后端：独立持久存储（严禁与 spill 混用）

基线记录需落存储——不落盘则"过去 30 天"无从谈起。但**不能复用窗口 spill**：

> **spill 无持久化语义**（`wf-runtime/src/lifecycle/spawn.rs:238-302`）：spill 文件 `spill_{rule}_{pid}.rb` 在"窗口 close 后 `cleanup` 删除，下一窗口复用同一路径（create 删旧建新）"；启动时 `cleanup_leftover_spill_files()` 删除目录下全部 `spill_*.rb/.rbr`，注释明写"**spill 无持久化语义，重启 = 重新 ingest**"。
> `SpillMode::Redb`（`wf-lang/src/compiler/mod.rs:1357`）是 stats 窗口在内存压力下的**中间态换出**通道，不是存储。若基线落 spill 目录或沿用 `spill_` 前缀，warm start 必然失效。

- **首选：独立 redb 基线库**。redb 已是引擎依赖，复用它作**库**（而非 spill 机制）成本最低，但必须**与 spill 生命周期彻底解耦**：
  - 独立目录（如 `WF_BASELINE_DIR`，默认 `baseline/`），**不与 `WF_SPILL_DIR` 同目录**；
  - 独立文件名前缀（如 `baseline_{rule}.rb`）——**绝不可用 `spill_` 前缀**，否则会被启动清理删除；
  - 不做启动清理；按 `(entity, metric, win_start)` 主键 upsert。
  - 落盘不落网，热路径零网络依赖，无 checkpoint 屏障。
- **或外部 sink**：规则级 sink 直接写外部 DB/对象存储（基线本就是普通 yield 记录，走现有 sink 即可）。
- **降级**：存储不可用时退回内存冷启动（仅失历史，不影响在线检测）。

据此，§3 D2 的"基线模型态落本地 redb"应理解为**独立持久基线库**，**不是** `disk_provider=redb` 的窗口 spill。

---

## 5. 消费侧 API：baseline(近期窗口, 周期, 方法, 值)

### 5.1 签名与模型

```
baseline(近期窗口, 周期, 方法, 值)
baseline(4小时, 周,  "mean",     qps)      # 当前 4h 窗口 vs 此前各周同时段 4h 窗口均值
baseline(1h,    日,  "median",   latency)  # 当前 1h vs 此前每日同时段 1h 中位数
baseline(4小时, none,"ewma",     qps)      # 无周期：单滚动窗口内连续指数衰减（aperiodic 流）
```

- **近期窗口**：当前要评估的聚合窗口（如 `4小时`），等价于一个 `stats` 窗口的产出；
- **周期**：对比周期（`日`/`周`/`月`），决定取哪些历史**同相位**窗口（如"此前各周一 10:00–14:00"）参与对比；省略=`none`（退化为单滚动窗口）；
- **方法**：如何合并历史同相位样本 → `mean` / `ewma`（近期周期加权）/ `median`（抗离群）/ `percentile(pN)`；
- **值**：被评估表达式。

**返回值语义**：`mean` → z-score `(v - μ) / σ`（σ=0 返回 0）；`ewma` / `median` / `percentile` → 相对偏离 `(v - baseline) / |baseline|`（baseline=0 返回 0）。非数值表达式返回 `None`。

**当前窗口如何聚合（补齐定义）**：`方法` 同时决定**两个环节**的统计量——(a) 当前近期窗口内，把多条基线记录聚成一个当前值 `v`；(b) 历史同相位窗口集合，合并成基线 `baseline`。两侧必须用同一统计量，否则不可比：

| 方法 | 当前窗口聚合（得 `v`） | 历史同相位合并（得 `baseline`） |
|---|---|---|
| `mean` | 算术均值 | 半衰期加权均值（§5.3） |
| `median` | 中位数 | 各同相位窗口值的中位数 |
| `percentile(pN)` | 分位数 | 各同相位窗口值的分位数 |
| `ewma` | 退化为时间加权均值 | 指数加权（近期周期权重高） |

- `v` 的来源：`近期窗口` 内 §4 产出的基线记录（如 4h 窗口 = 最近 4 条 1h 记录）按上表聚合。
- 例：`baseline(4小时, 周, "mean", qps)` = 最近 4 条小时记录的均值，对比"此前各周一同一 4h 相位窗口均值"的半衰期加权合并值。
- **非对称比较（当前用 mean、历史用 p95）MVP 不支持**——须拆成两个 `baseline()` 调用后自行比较。

### 5.2 范式区分与周期语义

滚动基线抓"水平位移/突发"；周期基线（同比=同相位跨周期、环比=上一周期同相位）抓"周期内异常"。二者机制不同，非同一旋钮。

**flash sale 例**：规则 `baseline(4小时, 周, "mean", qps)` 评估本周一 10:00–14:00 QPS 窗口，对比此前各周一同时段窗口均值。
- 往周同段均值=1000，本周 flash sale 同段=5000 → 偏离 5× → **正确告警**；
- 若成常态，下周一同时段也≈5000 → 基线吸收 → **自动停报**。

关键点：对比对象是"**那天上午的数据**"（同相位历史），而非被 30 天稀释的均值——用 30d 均值基线≈(1000×N+5000)/... 被稀释，首周即误报且难自适应。

**ewma 的定位**：在周期模型内对历史同相位样本做"近期周期加权"（如最近一周比上上月权重高）；或用于无清晰周期的流（`周期=none`，单窗口连续指数衰减）。其"逐事件 O(1) 在线偏离"能力由内联 `baseline()` 保留（§6）。

**未规划**：真·同比（去年同日）需长周期历史留存，可能涉及磁盘状态后端，与纯内存定位冲突；"周/日"周期在内存 + 本地持久化范围内可行。

### 5.3 衰减数学（半衰期加权矩，作用在消费侧跨周期合并）

令 `lambda = LN2 / 近期窗口`，对同相位历史记录按事件时间做**半衰期加权**——权重 `0.5^(age/近期窗口)`，近周期高、远周期仍留长程背景，使基线追随近期常态但不丢历史。跨周期合并的方差由**衰减加权矩**统一计算（各同相位记录的衰减矩按 `0.5^(age/近期窗口)` 累加），无需存原始样本。

---

## 6. 内联 baseline() 热路径糖（辅：逐事件 ewma/median）

用于 `stats` 窗口覆盖不了的逐事件在线偏离（ewma 响应式、median 抗离群，见 §7）。

### 6.1 数据结构变更

`types.rs:375` 的 `RollingStats` 扩展：

```rust
pub(crate) struct RollingStats {
    method: String,
    count: f64,          // 改为有效权重（衰减后可非整数）
    sum: f64,
    sum_sq: f64,
    // ewma
    ewma: f64,
    // median
    values: Vec<(f64, f64)>, // (value, event_time)，上限 MEDIAN_CAP
    // 时间追踪
    last_event_time: f64,    // 上一样本事件时间，用于算 dt
    half_life: f64,          // = dur（秒）
    warmup: u64,             // 已吸收样本数（整数计数，用于预热）
}
```

- 新增 `new_with_params(method, dur_secs, warmup)`。
- `update(&mut self, value: f64, event_time: f64)`：按 §6.2 计算 `dt` 并衰减/累加，更新 `last_event_time`，`warmup += 1`。
- `deviation(&self, value: f64) -> f64`：逻辑同现状，但 `mean/stddev` 走衰减矩；`warmup < 30` 时返回 `0.0`。

### 6.2 衰减数学（逐事件）

令 `lambda = LN2 / dur`（半衰期 = dur），`dt = 当前事件时间 - 上一样本事件时间`（乱序 `dt<0` 钳为 0）。

**mean / stddev（衰减矩）**
```
首样本:  sum=v; sum_sq=v*v; count=1
后续:    w = exp(-lambda * dt)
         sum    = w*sum    + v
         sum_sq = w*sum_sq + v*v
         count  = w*count  + 1
μ = sum / count
σ = sqrt(max(0, sum_sq/count - μ*μ))
```
- `dt == 0` 时 `w = 1` → 退化为现状的"全量累积均值"，**向后兼容**。
- 内存 O(1)，契合引擎"内存精确控制"优势。

**ewma（连续时间）**
```
首样本:  ewma = v
后续:    w = exp(-lambda * dt)
         alpha = if dt > 0 { 1 - w } else { ALPHA_FALLBACK }   // dt=0 仍要吸收新样本
         ewma = (1 - alpha) * ewma + alpha * v
```
- `ALPHA_FALLBACK` 默认 `0.1`（处理时间退化路径下的固定平滑）。

**median（带时间戳环形缓冲）**
- 维持 `(value, event_time)` 元组；`update` 后 prune `event_time < now - RETENTION`，其中 **`RETENTION = 3 * dur`**（半衰期语义下此时权重已降至 12.5%，更旧样本对中位数影响可忽略）。**不可把半衰期 `dur` 直接当硬截断阈值**——那会只保留 50% 权重就丢弃。容量上限 `MEDIAN_CAP = 1000`（超出丢弃最旧）。
- `dt==0` 时退化为现状的 1000 值缓冲。
- 窗口内样本超过 `MEDIAN_CAP` 时为**近似中位数**（文档注明）。

### 6.3 求值流程变更

`eval/mod.rs:187` 新签名（线程化事件时间 + 规则级基线表）：

```rust
fn eval_baseline(
    args: &[Expr],
    event: &dyn FieldSource,
    event_time: f64,                              // 新增：由调用方从事件解析（窗口已算过）
    baselines: &mut RuleBaselines,               // 变更：规则级（见 §6.4），非实例级
    entity_key: &str,                            // 新增：match key 值或 ""（D6）
) -> Option<Value>
```

- 解析 `args[1]` 为 `dur_secs`（必须；缺失/非数值 → `None`）。
- 状态键：`format!("{:?}:{}:{}:{}", entity_key, args[0], dur_secs, method)`（含实体 + dur，D5/D6）。
- 流程不变：先 `deviation(current_val)`，再 `update(current_val, event_time)`。

> `event_time` 来源：调用方（match 实例/规则求值上下文）已持有事件时间（窗口算子依赖它），直接下传，不在 `eval_baseline` 内重新解析。

### 6.4 状态归属变更（规则级续命）

现状 `baselines` 挂在 match 实例、`clear()` 于淘汰（state.rs:304）导致冷启动。改为：

- 在**规则级**引入 `RuleBaselines`（`EngineHashMap<BaselineKey, RollingStats>`），由规则任务持有，生命周期长于单个 match 实例。
- match 实例通过 `entity_key` 在 `RuleBaselines` 中查找/创建自己的 tracker。
- **实例淘汰不清 `RuleBaselines`**；仅规则卸载时整体清空。
- 内存记账迁移到规则级 map（state.rs:244 改为统计 `RuleBaselines.len()`，并加 `median` 元组容量 `MEDIAN_CAP * 16` 字节/项）。

> 此改动使"实体重新活跃"可从已有基线续命（warm restart）；配合 §4.5 的 redb 持久化，进程重启亦可 warm start。

### 6.5 边界与正确性

- **非数值 expr** → `None`（现状已处理）。
- **NaN 值** → 跳过 `update`（不污染基线）；`deviation` 对含 NaN 的 median 已 `filter(!is_nan)`（现状）。
- **预热保护** → `warmup < 30`：`deviation` 返回 `0.0`，避免冷启动首样本大偏离误报。
- **baseline=0**（ewma/median）→ 返回 `0.0`，不除零。
- **乱序事件** → `dt < 0` 钳为 0（不反向遗忘）；如需严格事件时间语义可改为跳过更新，留作后续选项。
- **`dur` 缺失/非法** → 返回 `None`，不静默用默认（避免"看起来生效其实没生效"的陷阱）。

### 6.6 性能与内存

- `mean`/`ewma`：每次 `update` O(1)，每 tracker 内存 O(1)（除 median）。
- `median`：prune + 排序 O(cap log cap)，cap=1000，百万 EPS 下每 tracker 摊销极小；内存有界。
- 整体契合引擎高 EPS 定位；基线状态纳入现有内存记账（§6.4）。
- 注意：每 (实体, expr, dur, method) 组合一个 tracker；高基数实体 × 多基线规则需关注 tracker 总数（已在 size 记账中可见）。

### 6.7 测试计划

| 用例 | 验证点 |
|---|---|
| 合成固定均值流 | `mean` 偏离在稳态趋近 0；注入阶跃后偏离符号正确 |
| `dt>0` 遗忘 | 同值流隔 `dur` 后基线衰减至约半（验证 λ 正确） |
| `dt==0` 兼容 | 行为与现状全量累积一致（回归） |
| 两不同 dur 同 expr | 键不碰撞，各自独立追踪（D5） |
| 预热保护 | 前 `30` 样本 deviation 恒为 0 |
| 乱序流 | `dt<0` 钳 0，不反向衰减 |
| 跨淘汰续命 | 实例淘汰后同实体再活跃，基线非冷启动（D2） |
| median 超 cap | 窗口内 >1000 样本时为近似中位数，不 panic |
| 非数值 expr | 返回 `None` |

---

## 7. 方法缺口与示例（ewma / median / percentile）

`stats` 窗口原生覆盖 mean/std（经 `sum_sq`）。三种 method 语义不在窗口能力内，由内联 `baseline()` 的 ewma/median 或新增 Percentile 聚合补（§6 的"辅"）。以下用具体数据说明为何需要。

### 7.1 抗离群基线（median）—— 单次尖峰不该污染基线

**场景**：每用户会话数据传输量（DLP 外泄检测）。99% 会话 1–10MB，但有个合法备份任务偶尔 5GB。
样本(MB)：`[3, 5, 4, 6, 8, 5000(备份), 4, 5, 3, 7]`

| 统计量 | 值 | 对"100MB 可疑外泄"的判断 |
|---|---|---|
| mean | **504.5MB**（和 5045 / n=10，被 5000 拉爆） | 100<504.5 → "正常" → **漏报** |
| median | **5MB** | 100/5=20× → **命中** |

mean 被单点离群污染 → 真异常淹没。median 抗离群，稳稳代表"典型会话"。窗口方案无 Median 聚合（仅 Top 近似），需内联 `baseline(..., "median")` 或新增 Median 聚合。

### 7.2 AIOps 延迟类（percentile p95/p99）—— SLA 看尾部不看均值

**场景**：支付网关延迟 SLO = "p99 < 200ms"。
100 次延迟(ms)：**95 次落在 30–60（均值约 45）**，**尾部 5 次** `180, 190, 210, 240, 300`。按 nearest-rank 口径（第 ⌈p·N⌉ 位）：

| 统计量 | 值 | SLO 状态 |
|---|---|---|
| mean | **~54ms**（(95×45 + 1120)/100） | "健康" → 误导 |
| p95 | **~60ms**（第 95 位，仍在正常区） | 达标 |
| p99 | **240ms**（第 99 位） | **超标（>200ms）→ 该报警** |

SRE/AIOps 的 SLA 几乎都是百分位定义，不看均值。`StatsAggPlan` 无 Percentile 聚合，需新增 Percentile 聚合（维护 quantile sketch）或内联 `baseline(..., "percentile")`。

### 7.3 响应式近期基线（ewma）—— 让基线跟上"新常态"

**场景**：API 网关 QPS。平时 1000/s；10:00 起大促合法涨到 5000/s 并持续。
**正确解法**：用**同比/环比**（同星期、同时段，见 §5.2），不是 30d 均值，也不是 ewma 追平滑值。规则 `baseline(4小时, 周, "mean", qps)`：
- 往周同段均值=1000，本周 flash sale 同段=5000 → 偏离 5× → **正确告警**；
- 若成常态，下周一同时段也≈5000 → 基线吸收 → **自动停报**。
- ewma 在此是**方法之一**：在周期模型内对历史同相位样本做"近期周期加权"，或用于**无清晰周期的流**（`周期=none`，单窗口连续指数衰减，见 §6.2）。

### 7.4 三者的共同点

都是"均值/固定窗口"表达不了的统计语义 → 正是 §4 窗口方案的三个 method 缺口，由内联 `baseline()` 的 ewma/median 或新增 Percentile 聚合补（§6 的"辅"）。

---

## 8. 现实基线需求分类与范围决策

> 把"到底要支持哪些基线场景"从拍脑袋变成可决策的分类表。所有需求按五个维度拆开，再与引擎定位（单机、高吞吐、纯内存、无 checkpoint、可解释、按实体隔离）对照，划分 MVP / MVP+ / 不推荐。

### 8.1 基线需求的五个维度

任何一条"基线类"需求都能映射到这五个维度，先拆再决策才不会混为一谈：

| 维度 | 取值 | 含义 |
|---|---|---|
| **隔离 (isolation)** | 全局 / 单体(按实体) / 群体(同侪·分群) | 基线归属：一条全局、每个实体各一份(单体)、或每个 cohort(部门/角色/地域…)一份群体分布(群体) |
| **当前值形态 (current)** | 点值 / 窗口聚合 | 比的是单个事件值，还是最近 N 的聚合 |
| **基线形态 (shape)** | 水平衰减 / 周期(季节) / 相对(环比同比) | 基线自身是标量平滑值、按相位分桶、还是同相位跨周期对比 |
| **统计量 (stat)** | mean(z) / ewma / median / percentile / ratio | 偏离的度量方式 |
| **留存 (retention)** | 短(进程内) / 长(跨重启持久化) | 状态是否需要落盘 |

### 8.2 各领域的现实基线需求

| 领域 | 典型需求 | 隔离 | 形态 | 统计量 | 周期? | 备注 |
|---|---|---|---|---|---|---|
| **安全/UEBA** | 登录频次/失败率 per user | 按实体 | 水平衰减 | ewma/z | 否 | 核心场景 |
| 安全 | 出站流量/连接数 per host | 按实体 | 水平衰减 | ewma | 昼夜(弱) | 周期可选 |
| 安全 | 数据外泄体积 per user | 按实体 | 水平衰减 | z | 否 | 长基线更佳 |
| 安全 | 失败登录**率**(fail/(fail+succ)) | 按实体 | 水平衰减 | **ratio** | 否 | 组合基线 |
| **风控/反欺诈** | 交易额/频次 per user | 按实体 | 水平衰减 | ewma/z | 否 | 核心 |
| 风控 | 设备切换频次 per account | 按实体 | 水平衰减 | ewma | 否 | |
| 风控 | 成功交易**率** per user | 按实体 | 水平衰减 | **ratio** | 否 | 组合基线 |
| **AIOps/可观测** | QPS/RPS per service | 按实体 | **周期(昼夜)** | z/ewma | **是(1d)** | 周期强需求 |
| AIOps | 错误率 per service | 按实体 | 周期(昼夜) | z | 是(1d) | |
| AIOps | 延迟 p99 per service | 按实体 | 水平衰减 | **percentile** | 否 | p95/p99 |
| AIOps | 日志量 per service | 按实体 | 周期(昼夜+周) | z | 是(1d/7d) | |
| **IoT/工业** | 温/振 per device | 按实体 | 水平衰减(慢) | z/ewma | 否 | 长基线 |
| IoT | 能耗 per 设备/楼宇 | 按实体 | **周期(昼夜+季节)** | z | 是(1d) | 季节弱 |
| **交易监察**(呼应股票) | 报撤单频次 per account | 按实体 | 水平衰减 | ewma/z | 否 | |
| 交易 | 成交量 per symbol | 按实体 | 水平衰减 | z | 否 | |
| 交易 | 买卖**比** per symbol | 按实体 | 水平衰减 | **ratio** | 否 | 组合基线 |
| **业务监测** | GMV/订单/活跃 per 业务线 | 全局或按实体 | **周期(时+周)** | z | **是(1d/7d)** | 周期强需求 |
| 业务 | 转化率(conv/visit) | 全局或按实体 | 水平衰减 | **ratio** | 否 | 组合基线 |
| **网络/流量** | 带宽 per IP | 按实体 | 水平衰减 | ewma | 昼夜(弱) | |
| 网络 | DNS 查询率 per IP | 按实体 | 周期(昼夜) | z | 是(1d) | |

### 8.3 范围决策矩阵

把 8.2 收敛成能力项，按"价值 × 与定位契合度"分级：

| 能力项 | 覆盖的需求 | 范围 | 理由 |
|---|---|---|---|
| **按实体水平衰减基线** (mean/ewma/median + dur) | 安全/风控/IoT/交易/网络 的 per-entity 类（占表约 60%） | **MVP（必做）** | 本设计已覆盖；引擎"按实体隔离 + O(1) 内存"天然契合 |
| **周期(季节)基线** (baseline 第二参 `周期` + 分桶) | AIOps 昼夜、业务时/周、IoT 能耗、网络昼夜（约 30%） | **MVP（必做）** | 周期误报问题严重且高价值领域(AIOps/业务)强依赖；`周期` 已是签名一等公民（§5），键加 bucket，内存随 `period/粒度` 放大但可控 |
| **组合/比率基线** (ratio，如 fail/(fail+succ)) | 失败率、转化率、成功交易率、买卖比 | **MVP+（高价值后续）** | 风控/业务高频；可由 `baseline(a)/baseline(b)` 近似，但比值 ≠ 比值的基线（相关性问题），精确支持需 2-arg 联合维护 |
| **分位基线** (p95/p99) | AIOps 延迟 | **MVP+（中价值）** | 仅 p50(median) 已在 MVP；p95/p99 需额外 quantile 结构，需求面窄 |
| **群体/同侪基线** (实体 vs 所属 cohort 分布) | UEBA 高级威胁狩猎（Exabeam 式 peer analytics）、"异于同侪"检测 | **MVP+（高价值，纯内存可行）** | 仅需维护 cohort 级聚合(mean/var/quantile sketch)，O(1)/cohort，纯内存可行；需定义 cohort 键与"相对群体分位"语义；与单体互补不替代 |
| **跨重启基线持久化** (日/周周期) | 进程重启后 warm start | **MVP（已支持，§4.5）** | 独立持久基线库（本地 redb 或外部 sink）；落盘不落网，无 checkpoint 屏障，不牺牲高吞吐 |
| **真·同比** (去年同日) | 长周期行为画像、年度同比 | **不推荐（超出定位）** | 需留存 365 天量级历史，存储与冷启动成本远超收益；事件时间衰减可近似缓解 |
| **多周期叠加** (时 of day + 周 of week 同时) | 复杂业务节奏 | **不推荐(MVP)**，可组合 | 可逐 period 叠加近似，完整支持复杂；优先级低 |
| **多元/相关基线** (volume 与 price 联合) | 量价联动异常 | **不推荐（超出定位）** | 需矩阵/相关运算，引擎无此数学能力 |

### 8.4 建议

1. **MVP 实打实覆盖"按实体 + 水平衰减 + 周期"两件套**：这俩合起来已覆盖表中约 90% 的现实需求（安全/风控/IoT/交易/网络/AIOps/业务 的绝大多数 per-entity 与周期场景）。
2. **`周期` 是一等公民（MVP 必做）**：理由不是"锦上添花"，而是 AIOps/业务/IoT 这些大市场若无周期基线会严重误报，且扩展代价小（键加 bucket）。`周期` 已作为 `baseline(近期窗口, 周期, 方法, 值)` 第二参数固化（§5）。
3. **组合/比率基线列为 MVP+ 第一优先**：风控失败率、业务转化率是高频刚需，能用 `baseline(a)/baseline(b)` 临时顶，但精确语义应补。
4. **一不碰**：多元相关（违背数学能力边界）——用 `external()` 调外部或明确告知不在域内。跨重启基线持久化已通过独立持久基线库支持（§4.5、§8.3），**仅真·同比（去年同日）不支持**；群体基线 MVP+（§8.5）。
5. **对外诚实口径**：落地后可写"在线基线（dur 生效、昼夜/周周期、单体+群体基线、按实体/分群隔离、跨淘汰续命、**跨重启本地持久化**）"；**不写**"真·同比(去年同日)/多元相关"——那是后续或不支持项。

### 8.5 单体 vs 群体：互补而非替代

基线"和谁比"有**两层正交维度**，常被混为一谈——

- **单体基线 (self / per-entity)**：实体与**自己的历史**比。"该用户今天的登录数，相对他自己过去 30 天是否异常？" → 抓住**自身行为漂移**。当前 `baseline()` 默认即单体（状态按 entity 隔离）。
- **群体基线 (peer / cohort)**：实体与**同侪群体的分布**比。"该用户的登录数，在全体/同部门用户里处于什么分位？" → 抓住**异于同侪**，即便他自身历史很稳。

二者**互补不可替代**：
- 单体漏报：某员工一直每天登录 50 次（自身稳定），但全部门均值才 5 次 → 单体基线判正常，群体基线立刻抓出 99 分位异常。
- 群体误报：新入职员工登录数天然高于老员工 → 群体基线误判，单体基线（他自己的近期）反而宽容。

**落地形态（纯内存可行）**：单体已在 MVP；群体基线只需维护 **cohort 级聚合**（均值/方差/分位数 sketch），内存 O(1)/cohort，不存原始实体值，与"纯内存单机"定位**不冲突**。需三件事：(a) 定义 cohort 键（如 `entity.dept`/`region`/设备型）；(b) 维护 cohort 聚合状态；(c) 返回"相对群体"的偏离（z vs 群体均值，或群体分位 rank）。建议语法独立于 `baseline()`（如 `peer_dev(expr, cohort_key, dur)`），避免污染单体语义。

群体基线与组合/比率基线同列 **MVP+ 第一优先**，是 wfusion 从"行为异常检测"迈向"真 UEBA"的关键一跳；但需先定义 cohort 语义，且它依赖"同侪分布"这一额外输入（非 `baseline()` 单点可解）。

---

## 9. 里程碑

| 阶段 | 内容 | 估时 |
|---|---|---|
| **MVP（推荐）** | §4 主方案(1h 固定窗生成 + 独立持久基线库) + §5 消费 API(mean/周期) + §6 内联糖(mean/ewma + 规则级续命 + 键含 dur + WARMUP=30) | 3–5 天 |
| 增强 | §6.2 median 时间戳 prune + §6.3 事件时间线程化完整落地 | +2–3 天 |
| **MVP+** | 组合/比率基线、群体/同侪基线(`peer_dev`)、Percentile 聚合(quantile sketch) | +2–4 天 |

### 9.1 可行性验证结论（已闭环）

| 原疑点 | 结论 | 依据 |
|---|---|---|
| stats measure 标签能否进 `yield` 作用域？ | **能，无需语言扩展**。写法为 `stat.value(final(label))`（裸标识符无效） | `checker/rules/mod.rs:291` 把 measure 标签注入 `scope.stat_labels`（全部 `Close` 阶段）；`compiler/tests/basic.rs:460`、`coverage_extra.rs:277/300` 为编译通过用例 |
| 是否需要新增持久存储模块？ | 走外部 sink 则**不需要**；走本地 redb 需一个极小模块（复用 redb 库依赖） | §4.5 |
| `sumsq` 是否需语言扩展？ | 需新增聚合，但为确定性工作量（parser + `StatsAggPlan` + checker 映射三处） | §4.2 |

> 备注：`q19.wfl` 未使用 `stat.value(...)` 属**需求形态不同**（聚合作选择器、载荷为原始行字段），不构成"标签不可进 yield"的证据。基线的聚合即载荷，必须走 `stat.value(final(...))`（§4.1）。

---

## 10. 文档影响

- **`wfl-dsl-comparison.md`**：将原"内置基线 + 持久化（规划/夸大）"行升级为"在线基线（dur 生效、按实体隔离、事件时间、可跨淘汰续命、**跨重启本地持久化**）"——如实区分两种持久化层级。
- **`wfl-design.md`**：新增 `baseline()` 语义小节（API + 数学）。
- **README**：本方案落地前**不**在 README 写 baseline（延续"先不写基线"决定）；落地且经测试后，可在一行差异化中恢复"内置在线基线"表述。
