# Join 算子族设计（interval / side-input / 回查）

> 状态：设计评审稿 v2（2026-08-22；v2 吸收 review R1/R2/R3/R4/R5/R6：emit at 显式 deferred、
> as-label 改 object+Path 注入、bucket_end 内建、oracle 跨仓库、读路径复用 asof_candidates、origin 语义）
> 前置：`docs/stats-executor-design.md`（v6 统一桶键模型）——stats 与 join 是**正交能力**
> 配套评审：`docs/design/join-family-design-review.md`（v1，2026-08-22）
> 痛点：NEXMark join 家族涉及 **Q3/Q8/Q9/Q13/Q20/Q21/Q22 共 7 个**（snapshot/asof/anti/interval），
> 现有仅 Snapshot/Asof/Anti 三种——Q8/Q9 长期未对齐的根因是 **interval（时间窗口 join）缺失**。
> （口径：Q5/Q7 移出归 stats P5 桶内回查，Q4 归 stats 两级聚合；Q21 anti 计入但已有）

---

## 1. 设计结论（TL;DR）

**所有 join = 同一条原语：「时态键查找」**，三根轴参数化：

```
lookup(right_window, key, time_pred) → matched_rows
        × mode（输出选择器：挑/归约/判断）
        × 触发（eager / deferred，`emit at` 显式声明）
```

代码层证据——interval 的**读路径 90% 已存在**：

| 已有能力 | 位置 |
|---|---|
| `JoinIndex` 存每键带时间戳的列式行定位 | `crates/wf-engine/src/window/buffer/mod.rs:41-68` |
| `lookup_timestamped` 已返回 `Vec<(i64, JoinRow)>` | `buffer/mod.rs:124` → interval = 在这上面 retain 时间谓词 |
| join 键索引自动配置 | `crates/wf-runtime/src/lifecycle/bootstrap.rs:239` `set_join_key` |
| provider 窗口（表背） | `crates/wf-engine/src/window/registry.rs:237` `provider_snapshot` → side input 现成（调用点在 `window_lookup.rs:136`） |
| join 后过滤 / 时序查找接口 | `WindowLookup` trait（snapshot / asof_candidates / join_lookup） |

**真正的缺口只有三个：**

1. **前瞻（lookforward）**：Q8/Q9 需要右行 ts > 左事件 ts，现有 eager 模型只能回看 → 需 **deferred 触发**。
2. **输出形状只有 1:1 富化**：缺「集合→归约」（Q9 maxrow）与「存在判断」（Q8）。
3. **右窗保留期**：interval 跨度可能超 `over=10m`（Q9 auction 生命周期）→ 编译期校验。

**范围收缩（关键决策）**：Q5/Q7 的「统计后回查」**不需要新 join 算子**——是 stats close 时从**桶内行**过滤（Q7：`price == 桶 max` 扇出），归 stats P5（桶内行访问），不入 join 家族。

---

## 2. 统一模型

### 2.1 原语

```
lookup(right_window, key, time_pred) → matched_rows
```

| 模式 | 时间谓词 | 输出 | 触发 | NEXMark | 现状 |
|---|---|---|---|---|---|
| snapshot | 无 | 1:1 首匹配 | eager | Q3/Q13/Q20 | ✅ |
| asof | `ts ≤ t`（within 下界） | 1:1 最新 | eager | Q22 | ✅ |
| anti | — | 有匹配则丢 | eager | Q21 | ✅ |
| **（缺省 inner）** | 无 | 存在则输出 | eager | — | 🔶 |
| **interval（回看）** | `ts ∈ [t-a, t]` | 1:1 | eager | 通用 | 🔶 P2 |
| **interval（前瞻）** | `ts ∈ [t, t+b]`（行内上界） | reduce / 存在 | **deferred** | **Q8/Q9** | ❌ P3 |
| **side input** | 无 | 1:1 | eager | Q13 | ✅ 90% |

### 2.2 触发（`emit at` 显式声明）

| 形态 | 声明 | 触发 |
|---|---|---|
| 回看 | 无 `emit at` | **eager**——现有 execute_joins 路径 |
| lookforward | **`emit at <expr>`**（显式 deferred 标记） | **deferred**——挂起至 watermark ≥ 触发点，到期评估 |

> ⚠ review R1：现有 on-each **无延迟承载点**（`EachPlan` 无 window/deadline，`rule_task` 的
> `scan_timeouts`/`flush` 均 `machine else return`）。deferred join 规则是一条**新的 rule task 分支**
> （挂起队列 + watermark 到期扫描，复用 `scan_timeouts` 机制），不是 on-each 即时路径的扩展。
> `emit at` 既是 deferred 标记也是触发点。
> 校验：**`emit_at ≥ within 上界`**（否则 `[lo, hi]` 内的行在触发时未到齐，会漏匹配）。

---

## 3. 语法（定稿 BNF）

```ebnf
join_clause := 'join' target join_mode
               ['within' within_spec]
               'on' cond ('&&' cond)*
               ['reduce' measure ['as' label]]
               ['emit' 'at' expr]

join_mode   := 'snapshot' | 'asof' | 'anti' | ∅    (* 缺省 = 纯存在(inner)；reduce 见下 *)
within_spec := '[' bound ',' bound ']' | dur              (* within 10s 糖 ≡ within [-10s, 0s] *)
bound       := ['<' | '<='] (dur | field_ref)             (* '<' 前缀 = 开区间；缺省闭 *)
cond        := field_ref '==' field_ref
measure     := 'maxrow' '(' field ')' ['tie' '(' field 'asc'|'desc' ')']
             | 'minrow' '(' field ')' ['tie' ...]
             | 'last' '(' field ')' | 'top' '(' N ',' field ')'
```

要点：

- **mode = 从匹配集里怎么选**；**`within` = 匹配集的时间界**——两轴正交，`interval` 不是一个独立 mode。
- **缺省 inner**：join miss → 丢事件。`optional` 保留（LEFT：miss 保留、字段为空），但 v1 无现实用例，只留语法位。
- **`reduce` + `as label`**：归约结果为一行，`label.field` 引用（Q9 `winner.bidder`）。
  ⚠ review R2：`as label` 不是裸名——reduce 整行以**裸键 object value** 注入 eval context
  （`ctx.fields["winner"] = Value::Object{...}`），`label.field` 编译为 `FieldRef::Path(["label","field"])`。
  `field_ref_name` 会丢限定词（`key.rs:338-349`），裸名在多行冲突时取错（Q9 的 dateTime/extra 重名）——不能依赖。
- **`emit at <expr>`**：deferred 标记 + 触发点（review R1）。expr 为驱动行字段/表达式（如 `a.expires`）；
  无 `emit at` 即 eager。校验 `emit_at ≥ within 上界`。
- **`within` 开闭记号**复用 stats tier 的 `<` 前缀：
  | 写法 | 谓词 |
  |---|---|
  | `[lo, hi]` | `lo ≤ ts ≤ hi`（缺省闭，对齐 SQL BETWEEN） |
  | `[lo, <hi]` | `lo ≤ ts < hi`（上开，窗口对齐常用） |
  | `[<lo, hi]` | `lo < ts ≤ hi` |
  | `[<lo, <hi]` | `lo < ts < hi` |
- 界 = 相对时长（相对左事件 ts）或**左行字段引用**（绝对时间，行内区间）。

---

## 4. 语义与决策记录

### 4.1 关键决策

| # | 决策 | 理由 |
|---|---|---|
| D1 | interval 不是新 mode，是 `within` 时间谓词 × mode 选择器 | 正交化：snapshot/asof/anti/reduce 全是「键查找 + 时间谓词 + 选择器」的参数组合 |
| D2 | `reduce` 归约留在 join 子句，不塞进 stats | stats 列式批模型无法承载"按 key 到期组装的行集合"；join 侧 keyed 状态更诚实 |
| D3 | 右窗 `over ≥ interval 跨度`（编译期校验 / wfs 显式声明） | 到期查找时行必须仍在窗内，否则漏行 |
| D4 | 缺省 inner（miss → 丢） | 全部现有 NEXMark join 规则实际都是 inner；顺带修掉 Q13 join miss 仍输出空字段的潜在坑 |
| D5 | **`emit at <expr>` 显式声明 deferred**（review R1） | 现有 on-each 无延迟承载点，deferred 需新执行路径 + 显式标记；`emit at` 既是标记也是触发点 |
| D6 | 归约 measure 用 `maxrow` 而非 `argmax` | 直观 + 与代码库 row 词汇一致（`JoinRow`/`IndexedRow`）；`tie` 表平手规则 |
| D7 | `as label` 注入为 object value，`label.field` 编译为 `FieldRef::Path`（review R2） | `field_ref_name` 丢限定词，裸名会冲突（Q9 的 dateTime/extra 重名会取到驱动行） |

### 4.2 `maxrow` 与 `tie` 语义

```wfl
reduce maxrow(price) tie(dateTime asc)
```

- `maxrow(price)` = 返回 price 最大的**那一行**（非标量）；`max(price)` = 标量值，两者并存。
- `tie(dateTime asc)` = 平手规则：同价时取 dateTime 最早。≡ 权威 SQL `ORDER BY price DESC, dateTime ASC` 拆成主次两键。
- 同价同 dateTime 仍并列时需第三个确定性破平（行序）——P3 对拍时钉死。
- 返回行 family：`maxrow` / `minrow` / `last` / `top(N)`；返回值 family：`max` / `min` / `sum` / `avg` / `count`。

---

## 5. 执行设计

### 5.1 回看 interval（P2，eager）

`execute_joins` 内加时间谓词分支：`asof_candidates(right, key)` → `retain(ts ∈ [lo, hi])` → 按 mode 取首/最新/存在/归约。零新状态。（R5：读路径复用 trait 层 `asof_candidates` / `snapshot_with_timestamps`，无需新开）

### 5.2 前瞻 interval（P3，deferred）——唯一的新机器

```
事件流 → rule_task（deferred 分支，非 on-each 即时路径）
  回看（无 emit at）       : 现有 execute_joins + 时间谓词
  deferred（emit at e）     : 挂起到 keyed buffer, expiry = e
                               watermark ≥ e → 触发:
                                 asof_candidates → 时间过滤 → reduce/exists → 输出
```

- **新执行路径**（review R1）：现有 on-each 无延迟承载点（`EachPlan` 无 window/deadline，
  `rule_task` 的 `scan_timeouts`/`flush` 均 `machine else return`）。deferred 规则在 rule_task 增加
  **挂起队列 + watermark 到期扫描**，复用 `scan_timeouts` 的到期机制。
  「互斥（二选一）」指**输出路径**：join 带 `emit at` → 整条规则转 deferred 输出路径（绕过即时输出）；
  无 `emit at` → eager。`each <alias>` 仅声明驱动事件 + entity/yield，不决定输出路径——§6 的 `each p`/`each a`
  是 deferred 规则的驱动声明，不是 on-each 即时规则。
- **挂起结构**：每左行一实例，expiry = `emit at` 字段值（行内表达式），watermark 驱动到期。
- **归约在到期时对 `Vec<JoinRow>` 执行**（`asof_candidates` 已给全部 `(ts, row)`，见 R5）；空集 → 不输出（Q9 无 bid 的 auction 恰不输出）。
- **输出语义**（review R6）：`origin = AlertOrigin::Close{reason}`（复用 close 路径）或新增；
  `fired_at = 到期 watermark`。P3 实现钉死，影响对拍断言。
- **Q9 的 auction 驱动天然正确**：auction 先于其 bids 到达（event-time 序），挂起到 expires 时 bids 已全量入窗，无乱序问题。

### 5.3 抽象缺口：keyed-state store

**Q9 长期难的根因**：引擎没有通用的「按 key 的有状态存储 + 行派生 TTL + 到期触发」（Flink 的 KeyedState + Timers）。deferred interval 需要一个**最小实现**：按 key 的 TTL 状态 + watermark 到期。这是本设计最该承认并单独投入的部分，而不是在两个现成执行器里挑一个硬塞。

### 5.4 保留期（D3）

| 界 | 校验 |
|---|---|
| 常量界 | 编译期校验 `right_window.over ≥ lo + hi`，不满足报错 |
| 行内界（Q9 `a.expires`） | wfs 显式声明 `over ≥ max auction 生命周期`，运行时超界告警 |

---

## 6. NEXMark 落地形态

### 6.1 Q8 —— 同桶存在性（deferred + 纯存在，上开）

```wfl
rule q8_monitor_new_user {
    events { p : person_events }
    each p
    join auction_events within [p.dateTime, <bucket_end(p.dateTime, 10s)]
        on p.id == auction_events.seller
        emit at bucket_end(p.dateTime, 10s)
    entity(digit, p.id)
    yield nexmark_alerts (id = p.id, alert_type = "q8_new_user", ...)
}
// deferred（emit at = 桶末）：watermark ≥ 桶末 → 桶内有 seller==p.id 的 auction → 输出，否则不输出
// 上开对齐 TUMBLE 桶 [B, B+10s)：恰在桶边界 B+10s 的 auction 归下桶，不误匹配
// review R3/N4：bucket_end() 为 P1 新增内建；算术替代 time_bucket(p.dateTime, 10) + 10s 可避免新内建，
//   但依赖 time+duration 加法算子（待验证），故保留 bucket_end 作 P1 依赖
```

### 6.2 Q9 —— 生命周期胜者（deferred + reduce maxrow）

```wfl
rule q9_winning_bid {
    events { a : auction_events }
    each a
    join bid_events reduce maxrow(price) tie(dateTime asc)
        within [a.dateTime, a.expires]
        on a.id == bid_events.auction as winner
        emit at a.expires
    entity(digit, a.id)
    yield nexmark_alerts (id = a.id, alert_type = "q9_win",
        detail = fmt("winner {}", winner.bidder), ...)
}
// 权威 SQL 逐段对应：
//   on a.id == bid_events.auction        → A.id = B.auction
//   within [a.dateTime, a.expires]       → B.dateTime BETWEEN A.dateTime AND A.expires（闭）
//   reduce maxrow(price) tie(dateTime asc) → ROW_NUMBER() OVER (PARTITION BY A.id
//                                            ORDER BY B.price DESC, B.dateTime ASC) = 1
// review R2：`winner` 以裸键 object value 注入（ctx.fields["winner"] = {bidder, price, ...}），
//   `winner.bidder` 编译为 FieldRef::Path——否则裸名 dateTime/extra 会与驱动 auction 冲突取错
// review R1：deferred 规则（emit at）走新 rule task 分支，非 on-each 即时输出
// 依赖：bid_events.over ≥ max auction 生命周期（D3）
```

### 6.3 现有规则迁移（缺省 inner）

| 规则 | 现状 | 新语法下 |
|---|---|---|
| Q20 | `join auction_events snapshot on ...` + `where category == 10` | 不变；缺省 inner（miss 丢，同现状 where 抑制） |
| Q13 | `join person_events snapshot on b.bidder == person_events.id` | 不变；缺省 inner **修掉** join miss 仍输出空字段的坑 |
| Q3 | `join person_events snapshot on ...` + `where state in (...)` | 不变 |
| Q22 | `join ... asof within 10s on ...` | 写法不变 |

显式 LEFT 用 `optional`（v1 无现实用例，只留语法位）。

### 6.4 移出 join 家族

- **Q5**（每 10s 桶 count 最多 auction）、**Q7**（每 10s 桶全局最高价 bid）：
  「统计后回查」= stats close 时从桶内行过滤（`price == 桶 max` 扇出 / count 后桶内 top-1），
  归 **stats P5 桶内行访问**，不需要新 join 算子。

---

## 7. 与 stats 的关系

| 能力 | 归属 | 查询 |
|---|---|---|
| 窗口统计（count/sum/.../distinct） | stats P1-P3 | Q11/Q12/Q15/Q16/Q17 |
| per-key top/last/maxrow 度量 | stats P4 | Q18/Q19 |
| **interval join（回看）** | **join P2** | 通用 |
| **interval join（前瞻 deferred + reduce）** | **join P3** | Q8/Q9 |
| **stats close 桶内回查** | **stats P5** | Q5/Q7 |
| side input | join（provider 窗口） | Q13 |

---

## 8. 分期

| 阶段 | 内容 | 查询 | 复用 | 门槛 |
|---|---|---|---|---|
| **P1** | 语法+计划：`within`/`reduce`/`as label` AST+parser + over 校验 | — | wf-lang | 单测 |
| **P2** | 回看 interval eager 执行（时间谓词 + 存在/首/最新） | 通用面 | `lookup_timestamped` + `execute_joins` | 单测 + oracle |
| **P3** | **前瞻 deferred**（keyed TTL 状态 + watermark 到期 + reduce） | **Q8/Q9** | match expiry + `lookup_timestamped` | Q8/Q9 权威对拍 |
| **P4** | side input 补齐（provider 窗口声明 + 文档） | Q13 精确化 | `provider_snapshot` | Q13 文档 |
| — | stats close 桶内回查 | Q5/Q7 | conv | stats P5 |

> oracle 已有窗口状态 + WindowLookup（join-field-as-key-design P2 建好）——interval = oracle 侧加同样 `lookup_interval`，对拍可行。
> ⚠ review R4：oracle（wfgen）在**独立仓库** `warp-fusion/crates/wfgen`——P3 的 oracle interval + deferred
> 评估是跨仓库工作，验收对拍脚本需跨仓库。

---

## 9. 风险与开放问题

1. **P3 deferred 分支的具体形态**：rule_task 内挂起队列 + `scan_timeouts` 复用（review R1 定方向，最小改动）vs 独立 `JoinExecutor`——v1 建议前者；deferred 规则不走列式热路径（`execute_each_direct*`），实现差异需标注。
2. **tie 后仍并列的确定性破平**：需追加行序键，P3 对拍钉死。
3. **join 可见性非确定**（engine replay append 超前，join-field-as-key §7.3 已知）——interval 继承，对拍归 known-diff。
4. **内联 test 块**：join 规则禁内联 test（harness 无 WindowLookup），验证锚点 = oracle/引擎对拍（同 join-field-as-key §7.7）。
5. **分片互斥**：deferred 的 keyed 状态与 rule-sharding 冲突（同 join-then-key §7.5）——v1 单连接。
6. **Q8 下界依赖数据假设**：`[p.dateTime, <bucket_end]` 而非 `[bucket_start, <bucket_end]`——依赖"auction 必晚于其 seller 的 person"这一 NEXMark 生成器保证；通用语义下应写 `[bucket_start(p.dateTime, 10s), <bucket_end(...)]`。
7. **缺省 inner 是行为变更**：需 audit 现有 examples/ 中把 join 当可选富化用的规则（`optional` 位先留，确有需要再启用）。

---

## 10. 相关文档

- 前置：`docs/stats-executor-design.md`（v6，stats+join 正交能力）
- 既有 join：`docs/design/join-field-as-key-design.md`（snapshot join-then-key，P0-P2 已完成）
- 语法示例：`wf-examples/performance/nexmark_pk/models/queries/q8.wfl` / `q9.wfl`（现状近似版）

---

## 11. P1 实施记录（2026-08-22，wf-lang 语法层）

P1（语法+计划+over 校验）已落地，`cargo test -p wf-lang` 598 通过（含 28 个新用例）。

### 11.1 已实现

- **AST**（`ast/join.rs`）：`JoinClause` 新增 `within`/`reduce`/`emit_at`；新增 `WithinSpec`、
  `Bound{open, val}`、`BoundVal::Dur{dur, neg} | Expr(Expr)`、`ReduceClause`、
  `ReduceMeasure::Maxrow|Minrow|Last|Top`、`TieSpec`。
- **`JoinMode::Inner`**（缺省 mode，设计 D4）：Q8/Q9 示例无 mode 关键字，必须表达缺省 inner。
  引擎 `execute_joins` 的 `_ =>` 兜底天然承接（P2 再接执行）。
- **Parser**（`wfl_parser/clauses.rs`）：`within [lo, hi]` / `within dur` 糖、`reduce ... [tie]`、
  `as label`（reduce 后或 on 后两种位置）、`emit at <expr>`；`within`/`reduce` 任一顺序；
  `asof within [..]` 经 asof_within 去 cut 回溯后落入 interval within（既有 `asof within DUR` 不变）。
- **Plan/Compiler**（`plan.rs`/`compiler/mod.rs`）：`JoinPlan` 透传新字段；`as label` 引用
  （`winner.bidder`）编译为 `FieldRef::Path`（review R2）——score/entity/yield/where/emit_at/
  within 界全部重写。
- **Checker**（`checker/rules/joins.rs` + `scope.rs` + `scope_build.rs`）：
  - over ≥ 区间跨度（D3，常量界）；lo ≤ hi；lo/hi 类型一致；interval 需右窗 time field；
  - `emit at`：需 `within`；上界须绝对时间表达式；同为字段时须 ≥ 上界（Q9 同字段）；
  - reduce：度量/tie 字段右窗存在性、top N ≥ 1、度量非结构化；
  - reduce 标签注册为 object 别名（`label.field` 解析）+ 与事件别名冲突报错；
  - 同规则内 `as label` 唯一（跨 join 重复报错）；
  - within 界 / emit at 表达式只能引用驱动事件字段（左行），不能引用 join 右窗；
  - `asof within DUR` 与 `within [...]` 互斥；
  - `bucket_end(time, seconds)` 内建（checker + infer 返回 Time）。
- **Explain**（`explain/sections.rs`）：新语法字符串化（含 `as label`/`emit at`）。

### 11.1b review 修复（同轮）

- `emit_at` 字段比较改叶子名比较（裸名 vs 限定名同字段不再误报）。
- 跨 join 重复 `as label` 报错（运行时同标签会互相覆盖注入）。
- within 界 / emit at 表达式限驱动事件字段（左行），引 join 右窗报错。
- pipeline 规则 final stage 的 reduce 标签注册（pipeline 分支手工建 scope，补 `register_reduce_labels`）。
- reduce 标签叶子类型从 `Ok(Some(Object))` 改为 `Ok(None)`（运行时确定，与嵌套 Path 一致）——
  使 `where winner.bidder > 0`、`winner.bidder` 在任意表达式位置都能通过静态检查。

### 11.2 已知边界（P1 遗留，P2/P3 处理）

- P2 已实现 eager interval（见 §12）；`reduce` 执行与 `emit at` deferred → P3。
- `emit_at ≥ 上界`仅静态字段等值校验；更一般的关系（如 `emit at bucket_end(...)+5s`）留 P3
  运行时 watermark 语义钉死。
- `winner.bidder` 叶子类型运行时确定（`resolve_field_ref` 返回 `Ok(None)`）；`field_ref_name` 对
  Path 取根段，bind tracking 会多记一个 label 根名（无执行影响）。
- `reduce`/`within` 分片互斥（设计 §9 风险 5）与 deferred 单连接约束延续至 P3。

---

## 12. P2 实施记录（2026-08-22，eager interval join）

P2（回看 interval eager 执行：时间谓词 + 存在/首/最新）已落地。
`cargo test -p wf-engine -p wf-runtime` 全绿（新增 8 引擎单测 + 2 bucket_end 单测 + 2 端到端集成测试）。

### 12.1 已实现

- **`execute_joins` interval 分支**（`crates/wf-engine/src/match_engine/executor/context.rs`）：
  - `join.within` 存在 → `execute_interval_join`：`asof_candidates`（R5 复用 trait 层）→
    `retain(ts ∈ [lo, hi])`（复刻 `find_matching_row` 逐条件复核）→ 按 mode 选择；
  - mode 选择：**Inner/Snapshot = 区间内最早（ts 最小，确定性）**、**Asof = 最新（ts 最大）**、
    **Anti = 区间内有匹配则丢**；
  - 界求值（`eval_interval_bound`）：`Dur`（相对左事件 ts，可负）→ 常量界；`Expr`（左行
    绝对时间字段/函数）→ `eval_expr` + `normalize_epoch_timestamp_float_nanos`；
  - 开闭记号：`Bound.open` → 开区间（`[lo, <hi]` 上开桶形态）；
  - **缺省 inner（D4）**：interval 与非 interval 路径均实现「命中富化、miss 丢事件」；
  - **deferred 跳过**：`join.emit_at.is_some()` 的 join 在 eager 路径跳过（P3 rule_task
    deferred 分支处理，设计 §2.2 互斥）。
- **`bucket_end(time, interval_seconds)` 内建**：`= time_bucket(t) + interval`（Q8 上开桶形态），
  引擎两套 eval 路径（`match_engine/eval/funcs.rs` 与 `executor/eval/builtins.rs`）都实现。
- **富化提取**：`enrich_join_row`（限定名 `window.field` + 裸名），interval 与既有 mode 共用。

### 12.2 测试

- 引擎单测（`context.rs`）：inner 命中/越界丢、snapshot 取最早、asof 取最新、anti 区间命中丢、
  上开界排除边界、行内字段界（绝对时间）、多条件逐条复核、plain inner miss 丢。
- `bucket_end` 单测（`l2/expr.rs`）：桶内偏移与桶边界移入下桶。
- 端到端（`wf-runtime/engine_task/where_integration_tests.rs`）：interval inner 命中富化输出、
  miss 丢（真实 Router + Window + RuleTask）。

### 12.2b review 修复（同轮）

- **close 路径尊重 join 返回值**（`close_exec.rs`）：原 `execute_joins(...)` 忽略返回值，
  导致缺省 inner / interval inner miss 与 anti 命中在 close 输出路径不抑制——与
  match/on-each 路径不一致。改为 `if !execute_joins(...) { return Ok(None); }`。
  既有 close+join 测试均为 snapshot hit（返回 true）不受影响；Q21（anti）走 on-each。
- 补测试：
  - `l2/joins.rs`：close + interval inner miss 抑制、close + interval inner hit 富化输出、
    close + plain inner miss 抑制；
  - `context.rs`：闭区间包含 lo/hi 边界、snapshot interval miss 保留不富化、
    interval join 后接 plain join 混合富化。

### 12.2c 性能基准（`match_engine/tests/interval_bench.rs`）

热路径分解（release，`cargo test --release -p wf-engine interval_bench -- --ignored`）：

| 路径 | ns/event | vs snapshot |
|---|---|---|
| snapshot（Q3/Q13/Q20 路径） | 450 | 100% |
| asof within 1800s（Q22 fallback） | 492 | 109% |
| **interval 常量界**（`within 10s` 糖） | 484 | **108%** |
| **interval 行内字段界**（`eval_expr` 求值） | 529 | **118%** |
| interval 候选 8 行 | 494 | 110% |
| interval 候选 64 行 | 832 | 185% |

结论：interval 分支相对 snapshot 只增加 ~8%（常量界）/~18%（字段界），与 asof 相当；
候选行数线性影响区间过滤成本（64 行 ≈ 2×）。设计 R5「读路径复用 asof_candidates」成立，
无灾难性开销。另附 debug 常规回归测试 `interval_join_overhead_bounded`（interval ≤ snapshot × 8 + 20ns）。

### 12.3 边界（P3 处理）

- `reduce` 执行（maxrow/minrow/last/top + `as label` object 注入）→ P3。
- 前瞻（lookforward）与 deferred 触发（`emit at` + keyed TTL 状态 + watermark 到期）→ P3。
- eager interval 对「未来行」（行内上界 > 当前 watermark）天然看不见——lookforward 语义需
  deferred（Q8/Q9 正是如此）。
