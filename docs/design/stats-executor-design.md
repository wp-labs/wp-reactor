# WFL `stats` 声明式窗口统计执行器 — 完整设计

> 状态：设计评审稿 v6（2026-08-22；v4 修 D3/D6/D7/D8/N1/N2/N3 补 D4/D9，v5 增分档矩阵，v6 统一桶键模型+行/列 pivot）
> 配套：`wf-examples/performance/nexmark_pk/NEXMARK_AUTHORITATIVE_SEMANTICS.md`
> 动机数据：`close_bench`（q15 close 累积 550ns/evt；guard 33% + distinct 23% + close 循环 19%）

---

## 1. 背景与动机

### 1.1 现状：窗口统计被 CEP 状态机承载，模型错配

Q1~Q22 权威语义中一部分是窗口统计（见 §2.2 精确清单），当前用 `match<...>`（CEP 状态机）实现：

```
CEP 逐事件状态机:                        窗口统计真正需要:
  advance_at 实例推进(匹配进度/顺序)      无匹配进度、无序列——只累计
  每 branch guard 独立评估               一次分档共享
  satisfied_flags / current_step          无匹配状态概念
  close 累积 × 每事件                    窗口结束时一次性汇总
```

q15 热点实测：`accumulate_close_steps` 334 次/事件 + foldhash 338 次/事件 +
guard mask 33% —— 全部是「用匹配引擎做统计」的错配代价。

### 1.2 目标

为**窗口内可交换结合统计**提供一等执行模型（与 `match`/`on each`/`conv` 平级）：

- 语义诚实：用户写 `stats`，声明"我要统计"，不伪装成匹配
- 优化对齐：语言结构（一次分档 × 多度量）× 执行结构（一次列扫描 × 归并）对应
- 可并行：可交换结合 → 批间/桶间并行是**数学保证**
- 不污染 CEP：`match` 语义完全不动

### 1.3 关键澄清（v2 修订 R3）

`stats` 无的是**匹配进度/序列**（无 seq/negation/多步骤/accu）。
它**保留事件时间有序性**：fixed/sliding/session 窗口的推进都需要按事件时间
有序处理（出窗、gap 合并）。「无顺序」仅指不依赖事件间的**匹配/序列关系**。

---

## 2. 适用边界（什么走 `stats`）

### 2.1 判定条件（编译期，结构可判，非启发式）

规则声明为 `stats` 形态即天然满足；不满足的规则必须用 `match`/`on each`：

| # | 条件 | 判定方式 |
|---|---|---|
| 1 | 无匹配进度（无 seq/negation/多步骤/accu） | 语法保证 |
| 2 | 全部度量是窗口统计函数 | 度量枚举检查 |
| 3 | guard 是行过滤（不参与匹配进度） | 语法保证 |
| 4 | yield 只读度量最终值 | 标签引用检查 |
| 5 | 窗口类型 ∈ {fixed, sliding, session} | 语法保证 |

### 2.2 覆盖查询（v2 修订 R1：精确归位）

**第一梯队 — 纯条件统计（`stats` 直接命中，收益最大）**

| Q | 权威语义 | 分组 | 度量 |
|---|---|---|---|
| Q15 | price 分档 × count/distinct | 空键（`day`，30m 恰 1 天） | tier + count/distinct_count |
| Q16 | channel × price 分档统计 | **复合键 (channel, day)** | tier + count/distinct_count |
| Q17 | auction × price 分档 + min/max/avg/sum | **复合键 (auction, day)** | tier + count/min/max/avg/sum |

**第二梯队 — 纯窗口计数（无 join 无 top，`stats` 直接命中）**

| Q | 权威语义 | 分组 | 度量 |
|---|---|---|---|
| Q11 | session(10s) 按 bidder 计数 | bidder | count + session 窗口 |
| Q12 | TUMBLE(10s) 按 bidder 计数 | bidder | count + fixed 窗口 |

**第三梯队 — 需 join/两级聚合/top 扩展（`stats` 部分命中或需扩展，见 §10）**

| Q | 权威语义 | 为何不能纯 stats | 形态 |
|---|---|---|---|
| Q4 | inner max per auction → outer avg per category | **bid⋈auction join + 两级聚合** | stats+stats 管线 + join |
| Q5 | 10s 窗 bid 数最多的 auction | count 后**跨 auction top-1** | stats + top 扩展 |
| Q7 | 10s 窗**全局**最高价 bid | 全局 max + **join 回原始 bid 行** | stats + join |
| Q9 | 每 auction 生命周期 [dateTime,expires] 胜出 bid | **auction⋈bid join + 非固定窗口 + argmax 整行** | stats + join + argmax |
| Q18 | 每 (bidder,auction) 最后一条 bid | `last(整行)` 非归并 | stats + last 扩展 |
| Q19 | 每 auction price top-10 | per-key top-N 非归并 | stats + top 扩展 |

**不在覆盖范围（v2 明确排除）**

| Q | 语义 | 原因 |
|---|---|---|
| Q8 | person⋈auction 增量 join | **当前是能力面测试（未对齐权威）**，非"待优化"——不做 stats 化 |
| Q6 | 每 seller 10 行滑动 avg | join-then-key + ROWS 滑动窗，语义与 stats 窗口模型不同，暂留 match |
| Q1/Q2/Q10/Q21/Q22 | 无状态变换 | `on each` |
| Q14 | 无状态变换 + 离散枚举分桶(dayTime/nightTime) | `on each`（枚举投影）；stats 枚举分桶用 `group by (daytime(...))`, 见 §4.1.1 |
| Q3/Q13/Q20 | 单行 join | `on each`+join / match 富化 |

---

## 2.5 引擎执行形态全景（stats 之外的其它能力缺口）

> v3 新增：从 NEXMark 需求盘点引擎完整能力图谱，明确各形态现状与优先级。
> stats 是其中一块；本节约定的其它形态是 stats 落地后（或并行）的第二、三块短板。

### 2.5.1 形态总览（当前已有关键能力）

| 形态 | 语法 | 执行模型 | 现状 |
|---|---|---|---|
| 无状态变换 | `on each` | 逐行投影/过滤 | ✅ 已有（Q1/Q2/Q10/Q14/Q21/Q22） |
| CEP 匹配 | `match<...>` | 逐事件状态机 | ✅ 已有（Q3/Q6/Q9/Q22） |
| 窗口收口变换 | `conv { sort/top/dedup/where }` | 收口批内变换 | ✅ 已有（Q7 top-1） |
| 快照 join | `join ... snapshot` | 主键查快照 | ✅ 已有（Q3/Q20） |
| Asof join | `join ... asof` | 时间戳对齐查最近 | ✅ 已有（Q22） |
| 窗口统计 | `stats` | 列式归并 | 🔶 本文档（P0） |

### 2.5.2 缺口清单（按 NEXMark 收益排序）

**P1 — Interval window join（时间窗口 join）**

```
两流在各自事件时间窗口内关联（bid ⋈ auction WHERE A.id=B.auction
AND B.dateTime BETWEEN A.dateTime AND A.expires）。
```

| 项 | 值 |
|---|---|
| NEXMark | Q7/Q8/Q9（3 个长期未对齐查询的根因） |
| 现状 | ❌ 缺失（仅 Snapshot/Asof） |
| 形态 | `join <left> on <cond> interval(<left_dur>, <right_dur>)` |
| 执行 | 两侧按时间桶索引 + 窗口内扫描匹配；与 CEP join 富化管线可复用 |
| 对齐后 | Q8（增量 join）、Q9（生命周期胜出）从能力面测试 → 权威语义 |

**P2 — Per-key Top-N / dedup 度量**

```
每 key 各自排序取前 N（Q19），或每 key 排序取第 1（Q9/Q18）。
```

| 项 | 值 |
|---|---|
| NEXMark | Q19（每 auction top-10）、Q9/Q18（ROW_NUMBER=1） |
| 现状 | ⚠️ conv `top(N)` 是**收口批全局**，无 per-key；`last(alias)`/`argmax` 度量缺失 |
| 形态 | stats 扩展度量 `top(N, field)` / `last(alias)` / `argmax(field)`；或独立 Rank 算子 |
| 执行 | 每 key 维护有界堆/最新行（非归并状态）；`argmax` 需 tie-break（权威 Q9：price DESC, dateTime ASC） |

**P3 — Side input join（侧输入维度流）**

```
外部维度流（文件/慢表）与主流关联（Q13：bid ⋈ side_input）。
```

| 项 | 值 |
|---|---|
| NEXMark | Q13 |
| 现状 | ❌ 缺失 |
| 形态 | `join ... from <side_table>` 静态/周期刷新快照 |

**P4 — Hop 窗口（滑动重叠窗）✅ 已落地（2026-08-23）**

```
HOP(bid, 2s, 10s)：每 2s 推进、10s 跨度重叠窗口（Q5 权威形状）。
```

| 项 | 值 |
|---|---|
| NEXMark | Q5（已由 `match<auction:hop(10s, 2s)>` 精确对齐） |
| 现状 | ✅ 已实现（`WindowSpec::Hop { size, slide }`，CEP match 路径；stats 执行器按 slide 推进待扩展） |
| 形态 | `match<key:hop(size, slide)>`：每事件扇入 size/slide 个覆盖窗口，`w_start + size` 收口（slide 对齐）；`hop(size,size)` 等价 `fixed(size)` |

**P5 — stats → join 回查管线**

```
统计结果 join 回原始流（Q5/Q7：窗口 top/max 后回查原始 bid 行）。
```

| 项 | 值 |
|---|---|
| NEXMark | Q5/Q7 |
| 现状 | ❌ 缺失（stats 输出 → join 的组合顺序未定义） |
| 形态 | stats 输出流作为 join 左侧，原始流快照作右侧；复用 P1 interval join 的执行 |

**其它缺口（低优先 / 能力面）**

| 缺口 | NEXMark | 现状 | 说明 |
|---|---|---|---|
| Watermark/乱序处理 | — | ✅ 已有 | 事件时间水位推进 |
| 触发策略（allowedLateness/trigger） | — | ❌ 缺失 | 延迟数据再触发 |
| UDF/外部函数 | Q14（count_char） | ⚠️ external | 能力面可用 |
| Session 窗口 | Q11 | ✅ 已有 | stats P3 复用 |
| Sliding（步进出窗） | Q6 | ⚠️ 已有 | 语义复杂，暂留 match |

### 2.5.3 优先级总表

```
P0  stats 执行器（本文档）        → Q11/12/15/16/17     直接命中 5 个
P1  Interval window join          → Q7/Q8/Q9 对齐       未对齐查询的根因
P2  Per-key Top-N / dedup 度量    → Q19 + Q9/Q18        有界堆/最新行
P3  Side input join               → Q13                 维度流注入
P4  Hop 窗口                      → Q5 形状精确化
P5  stats → join 回查管线          → Q5/Q7 精确化
```

> 关键结论：NEXMark 22 个查询里 **7 个涉及 join**，而现有仅 Snapshot/Asof 两种——
> interval/side-input/self-join 是第二短板（Q8/Q9 长期未对齐的根因）；
> stats 与 join 是**正交能力**，组合覆盖 Q5/Q7/Q9 这类「统计+回查」。

---

## 4. 语法设计（BNF）（v2 修订 R2/R6）

```ebnf
rule            := 'rule' name '{' events lets? stats_body entity yield limits? '}'

stats_body      := 'stats' window_spec '{' tier_decl? stats_block '}'
                 (* tier_decl 是语法糖, ≡ group by 桶键 + output columns, 见 §4.1.1 *)

window_spec     := '<' duration [':' window_mode] '>'      (* 分组独立声明, 见 group_by *)
window_mode     := 'fixed' | 'sliding' | 'session'         (* 默认 fixed *)

group_by        := 'group by' '(' group_key (',' group_key)* ')'   (* 桶键表达式列表; 缺省 = 空键全局 *)
group_key       := expr                                     (* 任意表达式; tier/bucket 是内置桶键函数 *)
output_shape    := 'output' ('rows' | 'columns')            (* 缺省 rows; columns = 输出时 pivot 转置 *)

tier_decl       := 'tier' field '[' boundary (',' boundary)* ']'  (* ≡ group by (tier(field,b..)) output columns *)
time_unit       := 'second' | 'minute' | 'hour' | 'day' | 'week' | 'month'
boundary        := '<' number | '<=' number                (* 升序常量 *)

stats_block     := measure_decl (';' measure_decl)*
measure_decl    := event_ref '|' agg_func 'as' label
                 | event_ref '|' agg_func 'as' label 'where' expr   (* 行过滤, 与 tier 叠加 *)
event_ref       := alias

agg_func        := 'count'
                 | 'sum(' field ')'
                 | 'avg(' field ')'
                 | 'min(' field ')'
                 | 'max(' field ')'
                 | 'distinct_count(' field ')'
                 | 'last(' alias ')'          (* 扩展: 保留最新整行, Q18; yield 取 row.price *)
                 | 'top(' N ',' field ')'     (* 扩展: per-key top-N, Q19 *)
```

**v3 语法变更（N1）**：
- 空键写法统一为 `stats<30m:fixed>`——**无前导冒号**；空键 = 不写 `group by`。
  v2 残留的 `stats<:30m:fixed>`（前导 `:`）与 BNF 不兼容（`<:30m` 非合法 duration），已全部清除。
- 分组统一为 `group by (f1, f2, ...)`（复合键），window_spec 不再内嵌 key（R2）
- guard 从 `b && expr`（CEP 残留）改为 `where expr`（声明式行过滤）（R6）
- `last(alias)` 保留整行（R7）

### 4.1 语义要点

- `tier b.price [ <10000, <1000000 ]` → 3 个**条件档** + 1 个**无条件档**（total）：

  | 档 | 条件（权威 SQL 对照） | 覆盖 |
  |---|---|---|
  | total | 无条件（= `count(*)`） | 全部行 |
  | rank1 | `price < 10000`（= `FILTER (WHERE price < 10000)`） | 开区间 |
  | rank2 | `10000 <= price < 1000000` | 半开区间 |
  | rank3 | `price >= 1000000` | 闭区间 |

  （v2 修订 R5：边界映射与权威 SQL 严格对齐；tier 字段为 null 的行**只计入 total 档**）

- `group by (b.channel, bucket(b.dateTime, 'day'))`（Q16 形态）：复合键 + 时间粒度分桶
- 每个 measure 的 `where` 与 tier 叠加 = 行过滤后按档归并

### 4.1.1 分档统一模型：分桶键函数 + 输出形状（v6 重构，替代 v5 三种并列语法）

**核心抽象：所有分档 = 「分桶键函数」——每行计算一个桶键(索引/标签/下界)，值相同的行进同一桶。**
`tier`/`bucket`/枚举表达式只是三种「桶键计算方式」，统一进 `group by`：

```
① 区间分档:   group by (tier(b.price, 10000, 1000000))   // 返回桶索引 0/1/2
② 时间粒度:   group by (bucket(b.dateTime, 'day'))        // 返回粒度下界
③ 枚举分桶:   group by (daytime(b.dateTime))              // 返回标签
④ 复合:       group by (b.channel, bucket(b.dateTime,'day'), tier(b.price, 10000, 1000000))
                // 多键 = 多分桶的叉乘; 每键一个累加器维度
```

**分桶键函数库（内置，后续可扩展）：**

| 函数 | 返回 | 列式可优化 | 适用 |
|---|---|---|---|
| `tier(f, b1, b2, ...)` | 桶索引（边界数+1，含 total 档） | ✅ 区间列扫描 | Q15/Q16/Q17 |
| `bucket(f, unit)` | 粒度下界（epoch 对齐） | ✅ 时间列取整 | Q15(day)/Q16(day) |
| 任意比较/表达式 | 标签/索引 | ⚠️ 可列式则列式,否则逐行 | Q14(dayTime) |

**语法统一（用户视角）：**

```ebnf
group_key       := expr                    (* 桶键表达式; tier/bucket 是内置函数 *)
group_by        := 'group by' '(' group_key (',' group_key)* ')'
output_shape    := 'rows' | 'columns'      (* 缺省: 单键 rows; 多键/声明 columns 时转置 *)
```

**保留的差异：输出形状（行展开 vs 列展开）——不是新语法，是同一分桶的 pivot 选择。**

```wfl
// 行展开（每档一行, 缺省）——Q12 形态
stats<30m:fixed> group by (tier(b.price, 10000, 1000000)) { b | count as bids; }
// → 3 行: (rank1,1) (rank2,1) (rank3,1)

// 列展开（每档一列, 单行）——Q15 形态: 权威输出 1 行 12 列
stats<30m:fixed> tier b.price [ <10000, <1000000 ] { b | count as bids; }
// → 1 行 4 列: total=3, rank1=1, rank2=1, rank3=1
// 语法糖: `tier f [b1,b2]` ≡ `group by (tier(f,b1,b2)) output columns`
```

**执行层**：分桶→累加只有一条路径；列展开在输出阶段做一次 **pivot（行转列）**。
列序必须固定（与权威 SQL 列序一致）作为对拍契约：`total, rank1, rank2, rank3, ...`。

**优化分派保留（实现视角）**：

```
语法统一 ≠ 放弃列式优化:
  tier()/bucket() 是编译器「已知桶键函数」→ 识别后走列式扫描(§6.2 段1b)
  其它表达式 → 通用逐行求值
  → 统一的是声明语法, 保留的是优化分派(函数特例)
```

**多分档组合（group by × tier × where 叠加）：**

```wfl
// Q16 完整形态: channel+day 分组 × price 区间档 × 无条件 count
stats<30m:fixed>
    group by (b.channel, bucket(b.dateTime, 'day'))
    tier b.price [ <10000, <1000000 ] {
        b | count as total_bids;
        b | distinct_count(b.bidder) as total_bidders;
        b | distinct_count(b.auction) as total_auctions;
    }
// → 桶数 = 分组键值 × 4 档; 每桶 9 个度量(3 无条件 × 3 档展开)
```

### 4.2 q15 重写示例（与权威 SQL 逐列对应）

```wfl
rule q15_bidding_stats {
    events { b : bid_events }
    stats<30m:fixed> tier b.price [ <10000, <1000000 ] {
        b | count as total_bids;
        b | distinct_count(b.bidder) as total_bidders;
        b | distinct_count(b.auction) as total_auctions;
        // rank1/2/3 由 tier 自动展开, 无需重复写 guard
    }
    -> entity(digit, 1)
    yield nexmark_alerts (
        id = 1,
        alert_type = "q15_stats",
        detail = fmt("{} {} {} {} ...",
            stat.total(total_bids),
            stat.tier(1, total_bids), stat.tier(2, total_bids), stat.tier(3, total_bids),
            stat.total(total_bidders), stat.tier(1, total_bidders), ...),
        request_count = 1
    )
    limits { max_memory = "8GB" }
}
```

- `tier` 声明**一次**（编译器展开 4 档 × 3 度量 = 12 列）
- yield 用 `stat.total(label)` / `stat.tier(n, label)` 引用——不再有 `stat.value(final(...))`

### 4.3 各查询重写形态（概要，v2 修订 R1/R10）

| Q | stats 形态 | 说明 |
|---|---|---|
| Q11 | `stats<10s:session> group by (bidder) { b | count as bid_count }` | session 窗口 |
| Q12 | `stats<10s:fixed> group by (bidder) { b | count as bid_count }` | 固定窗口 |
| Q15 | `stats<30m:fixed> tier b.price [<10000,<1000000] { ... }` | 空键 |
| Q16 | `stats<30m:fixed> group by (b.channel, bucket(b.dateTime,'day')) tier b.price [...] { ... }` | 复合键 + 时间粒度 |
| Q17 | `stats<30m:fixed> group by (b.auction, bucket(b.dateTime,'day')) tier b.price [...] { ... }` | 复合键 + 时间粒度 |
| Q14 | `stats<30m:fixed> group by (daytime(b.dateTime)) { b \| count }` | 离散枚举分桶（on each 投影改造） |
| Q4 | `stats` inner(max) → stats outer(avg) 管线 + bid⋈auction join | 两级（§10.1） |
| Q5 | `stats<10s:fixed> group by (auction) { count }` + `top(1, num)` 跨 auction | 全局 top 扩展 |
| Q7 | `stats<10s:fixed> { max(price) }` + join 回 bid | 全局 max 回查 |
| Q9 | auction⋈bid join + `stats` `argmax(price)` 生命周期窗 | 扩展 argmax |
| Q18 | `stats<30m:fixed> group by (bidder, auction) { b \| last(b) }` | last 整行 |
| Q19 | `stats<30m:fixed> group by (auction) { b \| top(10, price) }` | per-key top-N |

> 注（2026-08-23 更新）：Q5 权威 hop(2s,10s) 滑动形状已由 CEP `match<auction:hop(10s, 2s)>`
> 精确实现（见 docs/design/wfl-design.md window_spec），stats 执行器的 hop 形态仍为
> 后续扩展。

---

## 5. 计划结构（wf-lang `StatsPlan`）

```rust
// wf-lang/src/plan.rs 新增
pub struct StatsPlan {
    pub window_spec: WindowSpec,             // Fixed/Sliding/Session
    pub keys: Vec<ExprPlan>,                 // 桶键表达式列表(group by 统一, v6); 空 = 空键全局
    pub output_shape: OutputShape,           // Row | Column(v6: 行展开/列展开 pivot)
    pub measures: Vec<StatsMeasurePlan>,
    pub tracked_bind_fields: HashMap<String, HashSet<String>>, // 物化字段
}

pub enum OutputShape {
    Row,        // 每桶一行(缺省)
    Column,     // 每桶一列, 输出时 pivot 转置(单行多列)—— tier 语法糖的目标
}

// v6: TierPlan 并入 keys —— `tier f [b1,b2]` ≡ keys.push(Expr::Tier(f,[b1,b2])) + Column
// 桶键函数识别留在编译/执行(已知函数列式化), 不再有独立 plan 结构。

pub struct StatsMeasurePlan {
    pub label: String,
    pub source_alias: String,
    pub where_expr: Option<ExprPlan>,        // 行过滤(与桶键叠加)
    pub agg: StatsAgg,                       // count/sum/avg/min/max/distinct_count/last/top
    pub field: Option<FieldRef>,             // sum(field) 等
    pub arg: Option<usize>,                  // top(N) 的 N
}
```

### 5.1 编译器职责（v6 更新）

1. **桶键编译**：每个 `group by` 表达式 → 桶键函数分派——`tier()`/`bucket()` 标记为
   「已知桶键函数」走列式扫描(§6.2 段1b)；其它表达式走通用逐行求值。
   `tier f [b1,b2]` 语法糖展开为 `keys += tier(f, b1, b2)` + `output_shape = Column`。
2. **字段物化投影**：`tracked_bind_fields` = 所有 `field` 引用 + `where` 字段 + 桶键表达式字段
   （复用现有 field_usage 机制）
3. **window 挂载**：复用现有 `WindowRegistry`/`Router`
4. **yield 解析**：`stat.total(label)` / `stat.tier(n, label)` → 桶索引（列展开）；
   行展开时 label 直接是输出列

---

## 6. 执行器设计（wf-engine `StatsExecutor`）

### 6.1 状态结构（无匹配进度，纯累加）

```rust
// 每 (key 桶 × tier 档) 一个累加器
pub struct StatsAccum {
    pub count: u64,
    pub sum_i128: i128,                            // 整数累加（金额/计数, D8）; 展示时转 f64
    pub min: Option<i128>, pub max: Option<i128>,  // 整数极值（同精度敏感）
    pub distinct_set: Option<HashSet<DistinctKey>>, // distinct_count 用（D7: 原生 i64/timestamp key）
    pub last_row: Option<Arc<Event>>,              // last() 用（Q18）
    pub top_heap: Option<BinaryHeap<...>>,         // top(N) 用（Q19）
}

// 执行器实例：窗口 × key 桶
pub struct StatsWindowState {
    pub buckets: HashMap<ScopeKey, Vec<StatsAccum>>,  // [key][bucket_idx]; bucket_idx 由桶键表达式组合
    pub window_start: i64,
    pub last_event_nanos: i64,                       // session gap / sliding 出窗用
}
```

> v6：无独立 tier 维度——`buckets[key][bucket_idx]` 的 `bucket_idx` 由全部桶键
> 表达式（分组键 × 分档键）组合而成；`tier` 只是其中一个桶键函数。
> 列展开（Column）时, 输出阶段对 bucket_idx 做 pivot(行转列)。

> **D6/D8 精度约定**：归并状态只用 `count` + `sum_i128`（及整数 min/max）；
> **avg 不作为状态**——它永远在输出时由 `sum_i128 / count` 求得（见 §5.4）。
> 金额/计数字段（NEXMark `price` 为整数分）一律整数累加，不用 f64，避免
> `>2^53` 丢精度（与 fanout.rs:1103 记载的分歧敏感度一致）。

### 6.2 批处理流程（每 RecordBatch 8192 行）

**分两段：列式段（整列归并）+ 行式段（distinct/last/top 逐行）**

```
段 1 — 列式（整列归并, 不逐行）:
  a. 分组键列 → 列式 ScopeKey 分桶
       复用 fanout.rs:559 `pub(crate) fn scope_key_columnar`（或同模块
       `partition_rows_by_key` 提级为 pub(crate)）——**不是** 523 行的私有
       `scope_key_from_column`（D3）
  b. 分档键(tier/bucket) → 列式求桶键
       tier: 价格列 vs 边界数组一次扫描 → 桶索引列
       bucket: 时间列粒度取整 → 下界列
       stats 自行实现（D2/N2：不复用 CEP 的私有 `compare_int`/`cmp_vec`）；
       非已知桶键函数的表达式 → 通用逐行求值(段 2 前置)
  c. where 涉及的列 → 列式 mask（复用 eval_guard_columnar, pub）
  d. 纯归并度量（count/sum/min/max）按 key×桶键**整列累加**:
       对每列切片, 按 (key, bucket_idx) 分组做 count/sum_i128/min/max —— 无逐行解释器

段 2 — 行式（仅非归并度量 / 非列式桶键, 逐行）:
  for (row in batch):
     如果该桶只有 count/sum/min/max（无 distinct/last/top）→ 段 1 已算完, 跳过
     key = scope_key[row]; bucket = bucket_idx[row]
     if where_mask[row] == false: continue
     acc = buckets[key][bucket]
     if distinct: acc.distinct_set.insert(DistinctKey::from_raw(i64/timestamp))
                   // D7: 从列式原生值构造 key, **禁止** ValueKey::from_value 的
                   // f64 化（fanout.rs:1103 记载 >2^53 分歧）——bidder/auction
                   // 为 i64, 直接域内构造, 与列式路径字节一致
     if last:     acc.last_row = Some(row)
     if top:      acc.top_heap.push((row[field], row)); trim to N

输出(pivot):
  Row   → 每桶一行 (key, bucket_idx, measure...) 直接输出
  Column→ 按 bucket_idx 转置: 单行 [total, rank1, rank2, ...] × 每个度量
          列序固定为权威 SQL 列序(对拍契约): total, rank1, rank2, rank3, ...
```

> 关键：**段 1 覆盖纯归并规则（Q11/Q12/Q15 大部分度量），整批无逐行循环**；
> 段 2 只在存在 distinct/last/top 时逐行。这是 v2 对 R4 的落实——「每批列式」对
> count/sum/min/max 严格成立,distinct 降级为每行一次哈希（不可回避）。

```
窗口推进（需要事件时间有序, 见 §1.3）:
  fixed:   窗口到点 → 冻结快照 → 输出 → 清桶
  sliding: 按事件时间增量出窗（过期行从累加器减去——sum/count 可减, distinct 不可减,
           滑动窗 + distinct 需保留窗口内集合或退化为逐事件 → v2 明确: P3 暂不支持
           sliding+distinct 组合, 文档明示）
  session: gap 合并（last_event_nanos 与 gap 比较）
```

### 6.3 与 CEP 的关键差异（为什么快）

| 维度 | CEP（现状） | StatsExecutor |
|---|---|---|
| 循环 | 每事件 12 branch | 纯归并度量**整列一次**；distinct 每行 1 次哈希 |
| guard | 9 个独立表达式 | **1 次 tier 分档** |
| distinct | 每事件 2 次独立哈希 | **每行 1 次哈希**（批内预去重可再降） |
| 实例 | created/expiry/satisfied | 无 |
| 并行 | 单实例串行 | **桶间/批间并行（可交换结合）** |

### 6.4 并行与分片（D6：avg 归并以 (sum,count) 对结合）

- 空键全局：内部按批归并到单桶（并行路径：分片预归并 → 合并）
- 带 key：复用现有 `Sharded` fanout 按 key 分片 → 每片独立 StatsWindowState →
  close 时各片输出（Q12/Q16/Q17 形态）

**归并单位（分片/批间合并必须遵守）**：

| 度量 | 结合方式 | 说明 |
|---|---|---|
| count | `c1 + c2` | 直接相加 |
| sum | `s1 + s2`（i128） | 直接相加 |
| avg | **`(s1+s2, c1+c2)` 结合, 输出时 `(s1+s2)/(c1+c2)`** | ⚠️ `avg(avg1, avg2) ≠ avg(a∪b)`, **绝不直接合并 avg 值** |
| min/max | `min(m1,m2)` / `max(m1,m2)` | 取极值 |
| distinct | 集合**并** | 可交换结合 |

> **avg 不是状态**：StatsAccum 只有 `sum_i128`+`count`,avg 仅最终输出时计算。
> 任何分片/批间归并都以 (sum, count) 对为单位, 避免实现者照字面直接合并 avg 导致算错。

- **sliding+distinct 例外**：滑动窗的 distinct 需要「窗口内集合」（行可出窗），
  不可简单归并——P3 阶段文档明示不支持，需专用结构

---

## 7. 接线（window → 执行器）（v3 修订 N3：真实架构）

**stats 作为 fanout 订阅者（与 rule_task 并列），走既有投递机制——不存在「router 内联直调」**。

```
window/router.rs（现有）
   └─ Router 解析批次 → ParsedWindow（defer: events=None, shard_rows）
        └─ 窗口 actor / fanout 投递（经 mpsc channel, 复用现有 RulePush 机制）
             ├─ rule_task（CEP, 现有）
             └─ StatsExecutor（新订阅者）
                   └─ 收到 raw RecordBatch → process_batch(batch)
                      （空键: broadcast_batch_only; 带 key: register_sharded 分片）
                   └─ StatsWindowState 维护, close 时经现有 entity/yield 下沉
```

### 集成点（与代码对齐）

- **订阅方式**：空键单实例 → `rule_fanout.register(window, tx)`（单订阅者）；
  带 key → `register_sharded(...)`（按 key 分片, 每片独立 StatsWindowState）。
  与 rule_task 相同的 `RulePush` 通道——**复用投递/ack 机制, 不新造同步内联钩子**。
- **批次形态**：走 `broadcast_batch_only`（raw RecordBatch, events=None,
  defer_materialization 路径）——stats 执行器消费原始列, 无需事件物化。
- **为什么不是 router 内联**：router.rs:335 的 defer 分支只产出
  `ParsedWindow { events: None, shard_rows }` 并 continue, 不在解析循环里调任何
  执行器；真正消费方是 fanout/窗口 actor（经 channel 投递）。

### backpressure / ack（必须显式处理）

- stats 执行器是窗口 buffer 的**共享下游**：若 stats 不 ack, 会让底层窗口缓冲的
  cursor-gap 卡住（与 evictor cursor-gap 教训一致）。
- **必须参与 ack floor**：stats 处理完批次后 ack, 进度推进与 rule_task 对齐；
  否则慢 stats 会阻塞驱逐, 影响共享同窗口的 CEP 规则。
- 若 stats 未来需要同步内联（极端场景），必须评估 backpressure：同步执行会阻塞
  router 分发, 连共享 fanout 的 CEP rule_task 一起卡住——**默认走 channel, 不做同步内联**。

---

## 8. 语义等价与测试

### 8.1 对拍（oracle）（v2 修订 R12）

- **Q15/Q16/Q17/Q11/Q12**：对拍用**现有 CEP 执行器跑等价规则**作 ground truth
  （q15 已有锚点 `EMIT=1` + 12 列数值），stats 输出逐列一致
- **Q18/Q19/Q4/Q9**：当前实现是近似或未实现（Q19 未实现、Q18 无 last 值）——
  **不能以 CEP 作 ground truth**，改用**权威 SQL 期望值**（独立计算器）对拍

### 8.2 判定器单测

- 语法/计划层：tier 展开数、复合键桶索引、yield 解析
- 执行层：批内/批间边界、where null 语义、tier 边界值（`price = 10000` 归属 rank2）、
  session gap、分片合并、sliding 出窗

### 8.3 三值逻辑对齐

- where null → 与 CEP `eval_expr_ext` 一致的 permissive 语义（null 不 block）
- tier 字段 null → **只计入 total 档**（v2 修订 R5，与 `count(*)` 一致；条件档统计
  仅对非 null 有效，与 Flink `FILTER (WHERE price < 10000)` 对 null 的行为一致）

---

## 9. 渐进落地路线

| 阶段 | 内容 | 查询 | 里程碑 |
|---|---|---|---|
| P1 | 语法 + 计划 + 空键 fixed count/distinct | Q15 | 对拍通过, EPS 见 §11 推导 |
| P2 | 复合键分组 + sum/avg/min/max | Q12/Q16/Q17 | 分片并行 |
| P3 | session/sliding 窗口 | Q11 | session 归并；sliding+distinct 明示不支持 |
| P4 | last/top 扩展度量 | Q18/Q19 | 非归并状态 + 权威 SQL 对拍 |
| P5 | stats + join / 两级管线 | Q4/Q5/Q7/Q9 | 与 join 算子组合 |

**每阶段门槛**：oracle 对拍一致 + 相对 CEP 版本 EPS 提升可测 + 回归全绿。

---

## 10. 风险与开放问题（v3 修订 D9：Q4 两级聚合结构已给出）

1. **Q4 两级聚合（D9，已框架化）**：inner max per auction → outer avg per category。
   结构：`stats` inner 输出（每 auction 一行 `(auction, category, final=max)`）作为
   **中间流**进入第二个 `stats`（group by category, `avg(final)`）。两级 stats 之间
   复用现有 join 管线的中间流投递；或单 stats 内两级 group-by（P2 前给出
   `StatsPlan` 嵌套结构：`measures[].group_by` 二级分组）。当前取前者（管线）优先。
2. **Q9 胜出行**：`argmax(price)` 需保留整行 + 平手规则（权威 SQL：`price DESC, dateTime ASC`）
   —— 扩展度量要带 tie-break 语义
3. **Q7/Q5 全局 top 回查**：`stats` 输出全局 max/top-1 后 join 回原始 bid 行——
   统计结果 → join 的组合顺序需定义
4. **sliding + distinct**：滑动窗 distinct 需窗口内集合（可出窗），与归并模型冲突——
   P3 明示不支持，需专用结构（如滑动窗口位图/时序集合）
5. **是否保留 CEP 降级优化**：旧 `match` 写法的统计规则是否透明走 stats（判定器）——
   可作为 P6 可选（v2 强调：仅对 §2.2 第一/二梯队安全）
6. **`stats` 与 `conv` 的关系**：Q19 per-key top-N 现有 conv **无 per-key 分组**（未实现
   权威语义），stats 的 `top(N)` 是补能力而非吸收 conv；conv 仍保留（Q7 全局 top-1 用）

---

## 10.5 窗口推进触发信号源与 ack（D4，钉死）

| 窗口 | 触发信号 | 说明 |
|---|---|---|
| fixed | **内部事件时间水印**推进到窗口上界 | 复用现有 watermark/progress 机制；窗口冻结 → 输出 → 清桶 |
| sliding | 水印推进触发步进出窗 | 过期行从累加器扣除（count/sum/min/max 可扣；distinct 不可，见 §5.4） |
| session | 水印推进触发 gap 合并 | last_event_nanos + gap 与水印比较 |

- **ack 集成**（N3 第 3 点）：stats 输出/清理完成后向窗口进度报告 ack——必须与
  rule_task 共用同一 ack floor（§7 backpressure 节），否则驱逐（evictor）会因
  stats 未 ack 而 cursor-gap 卡住。
- 触发信号源**统一为内部水印**，不引入窗口关闭的外部事件通道。

---

## 11. 验收标准（v2 修订 R9：目标有推导）

**EPS 目标推导**（基于 close_bench 组件数据）：

```
现状 CEP q15:  550 ns/evt（close 累积）+ 外围 ≈ 590ns/evt → 1.7M EPS
stats 预计:    tier 分档 ~30ns + 列式 count ~10ns + distinct 35ns（i64 免包装）
               ≈ 75-100 ns/evt（无 12-branch 循环、无 guard 重复）
理论上限:     ~10-13M EPS（单核）
保守验收:     P1 Q15 10M EPS ≥ 3M（5× 于现状 1.7M, 留足 distinct 合并余量）
```

- [ ] Q15 用 `stats` 重写，oracle 对拍 12 列一致（含 avg 归并以 (sum,count) 对、
      distinct key 从列式原生 i64 构造——D6/D7 精度验证）
- [ ] Q15 10M EPS ≥ 3M（推导见上；实测以正常二进制为准，排除插桩/负载）
- [ ] Q12/Q16/Q17 至少 2 个 stats 化，全部对拍一致
- [ ] 判定器/边界单测覆盖（tier 边界含 `=`、null、复合键、session gap、分片合并、sliding 出窗、
      sum>2^53 精度、分片 avg 归并）
- [ ] stats 参与 ack floor，共享窗口的 CEP 规则无 cursor-gap（N3 backpressure 验证）
- [ ] CEP 路径零改动，`match` 规则全部回归通过
