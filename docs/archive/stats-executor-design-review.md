# Review：`stats-executor-design.md`（设计评审稿 v1, 2026-08-22）

> 审查方式：逐条比对设计文档的"复用现有…/对等官方…"硬承诺与
> `wp-reactor/crates` 实际源码（`wf-engine` / `wf-lang`），并交叉核对
> `wf-examples/performance/nexmark_pk/NEXMARK_AUTHORITATIVE_SEMANTICS.md`。
> 所有代码引用均带 `文件:行号`，可独立复核。

---

## 0. 总体判断

**方向正确、复用承诺基本落地、查询映射经权威语义验证无误。** 但有 **1 处接线缺口（致命）、3 处符号/职责引用错误（会误导实现）、5 处设计空白/不严谨（影响正确性）**，以及若干内部不一致。建议 v2 先修 §6 接线缺口与 §6 的符号名再进 P1。

---

## 1. 已核实、落地靠谱的部分（不要动）

| 设计承诺 | 实际代码 | 结论 |
|---|---|---|
| §6 `RuleFanout` 订阅机制可加 stats 订阅者 | `fanout.rs:111` `RuleFanout`；`register`/`register_sharded`/`register_round_robin` 三态 `Subscription` 枚举（`fanout.rs:59`） | ✅ 新增 `StatsExecutor` 作为并列订阅者，复用 `register()`/`register_sharded()` 即可 |
| §5.2 "raw batch 广播已支持（defer materialization）" | `fanout.rs:271 broadcast_batch_only` + `fanout.rs:245 broadcast_with_batch`；生产路径 `commit.rs:76/91` 已实际调用；`defer_materialization` 标志 `buffer/mod.rs:267` | ✅ 能力真实存在且已接线 |
| §5.2 列式 key 分桶 | `fanout.rs:559 scope_key_columnar`(`pub(crate)`)、`fanout.rs:586 partition_rows_by_key` | ✅ 列式 ScopeKey 路径真实可用 |
| §5.2 列式 guard | `columnar.rs:253 eval_guard_columnar` 真实存在，三值语义与解释器对拍（`columnar.rs:664 assert_equiv`） | ✅ 可复用 |
| §4 `WindowSpec` / `Fixed/Sliding/Session` | `plan.rs:185 enum WindowSpec { Sliding, Fixed, Session }`；编译器 `compiler/mod.rs:357` 已映射 `WindowMode`→`WindowSpec` | ✅ 计划层类型真实存在 |
| §1.1 动机数据（q15 close 550ns，guard/distinct/close 占比） | `match_engine/tests/close_bench.rs` 真实存在，含 `distinct_valkey`/`distinct_i64` 分量拆解 | ✅ 动机数据有出处，非编造 |
| §2.2 / §3.3 查询→度量映射 | 权威语义文档确认：Q18=**Find last bid**→`last(b)`、Q19=**Auction TOP-10 Price**→`top(10,price)`、Q9=**Winning Bids**→`max`+守发行、Q4=**Average Price for a Category**→两级 | ✅ **映射全部正确**（初审时的"Q18/Q19 疑似错配"被权威名证伪，文档是对的） |

---

## 2. 符号/职责引用错误（会误导实现，必须改）

### 🔴 D1 — §6 引用的 `RuleFanout::broadcast_batch` 不存在
文档原文（§6 图）：
```
RuleFanout::broadcast_batch（defer，raw RecordBatch）
     └─ StatsExecutor::process_batch(batch)
```
实际 `fanout.rs` **没有 `broadcast_batch`**，正确符号是：
- `broadcast_batch_only(batch, materialize_fields, shard_rows, seq)`（`fanout.rs:271`）—— 只发 raw `batch`、`events=None`，正是 stats 执行器"直接消费 raw batch"要用的；
- `broadcast_with_batch(events, batch, …)`（`fanout.rs:245`）—— 同时带 `events` 与 `batch`。

**修复**：把 §6 改成 `broadcast_batch_only`（或说明何时用 `broadcast_with_batch`）。此错误与上轮 nexmark 审查的"引用错源"同一类，应按精确符号名落地。

### 🟡 D2 — §5.2 "复用 **GuardMasks** 的 eval_guard_columnar" 职责写错
`eval_guard_columnar` 是 `columnar.rs:253` 的**自由函数**，不是 `GuardMasks` 的方法。
`GuardMasks`（`columnar.rs:116`）是 **CEP 分支 guard 掩码缓存**（keyed by `(step,branch)` 的 event/close/neg 三表），是匹配引擎专用结构，**stats 不应复用 GuardMasks**。
**修复**：改成"复用 `match_engine::columnar::eval_guard_columnar(expr, &view)`"。

### 🟡 D3 — §5.2 "复用 columnar `scope_key_from_column`" 引用了私有符号
`scope_key_from_column`（`fanout.rs:523`）是 **`fn`（私有）**，stats 执行器（另一模块）不可见。
可复用的公开符号是 `scope_key_columnar`（`fanout.rs:559`，`pub(crate)`）与 `partition_rows_by_key`（`fanout.rs:586`）。
**修复**：改成 `scope_key_columnar` / `partition_rows_by_key`（如需按 key 分区）。

---

## 3. 关键接线缺口（致命，P1 前必须解决）

### 🔴 D4 — 窗口"到点→出窗→emit"信号如何送达 stats 执行器，文档完全没说
§5.2 step 3 写 "fixed: 窗口到点 → 冻结快照 → 输出"，但**整个 fanout 只有 `RulePush`（events/batch），没有任何 close/freeze 信号**（`fanout.rs` 全文无 `close` 推送；grep `WindowClose|on_close|freeze` 在 `window/` 下零命中）。
现有 CEP 的 close 由 `RuleTask` 内部挂的 **match engine 自己驱动**（`advance_at`），**不经 fanout**。
→ stats 执行器没有 match engine，就没有人告诉它"这个窗口结束了，请出窗"。
**这是 §6 图最大的洞**：图里只画了 `fanout → StatsExecutor::process_batch`，缺一条 close/emit 触发路径。

**要求文档补充**：谁在何时触发 stats 执行器的 emit？两种可行路线，需明确其一：
- (a) 复用现有窗口边界检测（`commit.rs`/`evictor.rs` 已算窗口边界），新增一个"窗口关闭事件"经 fanout 或独立 channel 送达 stats 订阅者；
- (b) stats 执行器自己按 `WindowSpec` 维护水印，在收到超过窗口上界的事件时间戳时冻结并 emit。
无论哪条，都必须在 §6 画出，并与现有 `WindowProgress`/`ack floor`（`fanout.rs:23-25`）的反压语义对齐（否则慢 stats 执行器会让窗口驱逐卡住，重演 evictor 的 cursor-gap 教训）。

---

## 4. 设计空白 / 不严谨（影响正确性）

### 🟡 D5 — sliding / session 的"非单调聚合"回撤完全没处理
§5.2 step 3 把 sliding 写成 "增量滑动（按到达时间出窗）"，§5.4 又宣称 "count/sum/avg/min/max/distinct 全部可交换结合 → 分片归并数学安全"。
但 **sliding 窗口 + distinct/top/last 需要"事件滑出窗口时回撤"**，回撤 ≠ 归并：
- `distinct_count`：事件滑出要**从集合删除**，集合无减法逆运算（除非精确集合或近似 HLL）；
- `top(N)` / `last(b)`：滑出要**从堆/最新行剔除或回退**， Heap 无逆运算。

§5.4 的"可交换结合"只论证了**已完成桶的跨分片归并**，完全没覆盖 **窗口内滑动回撤**。Q5（sliding）只用了 `count`（单调、可回撤为计数减一），所以 Q5 没事；但文档 §2.2 把 Q11(session)/Q5 标为 P3，并未限定 sliding 只能用 count，**一旦在 sliding 上用 distinct/top/last 就会错**。
**要求**：在 §5.2/§5.4 显式声明滑动/会话窗口对 distinct/top/last 的回撤策略（精确集合回撤？窗口内保留全量事件副本？还是限定 sliding 仅支持单调度量），否则实现者会踩坑。

### 🟡 D6 — `avg` 的"可交换结合"表述不严谨（实现若直接归并 avg 值会算错）
§5.4 列 "avg…可交换结合"。但 `avg(avg(a),avg(b)) ≠ avg(a∪b)`。**只有当累加器存 `(sum,count)` 对、归并 sum 与 count、最后再除时才是结合的**。`StatsAccum`（`§5.1`）确实存了 `sum: f64` 与 `count: u64`，结构是对的；但文档文字没说"跨分片归并传 (sum,count) 而非 avg"，实现者容易直接合并 avg。
**要求**：§5.4 明确"分片归并以 `(sum,count)` 为单位结合，avg 仅在最终输出时由 sum/count 求得"。

### 🟡 D7 — `distinct_count` 的 key 精度：别用 `ValueKey::from_value`（f64 丢精度）
§5.1 `distinct_set: Option<HashSet<ValueKey>>`，§5.2 `acc.distinct_set.insert(Value(ValueKey(row[field])))`。
但 `ValueKey::from_value` 对数值走 **f64**（`close_bench.rs:373` 用 `ValueKey::from_value`；同文件 `:19` 把 `distinct_i64`（原生 i64 key）列为**优化方向**，说明基线确实 f64 四舍五入）。而 `fanout.rs:1103-1105` 已记载 >2^53 的 i64 行式 `Value::Number(f64)` 丢精度。
→ 若 `bidder`/`auction` id > 2^53，stats 的 `distinct_count` 会和列式 key 路径（原生 i64）结果**不一致**。
**要求**：distinct key 必须从列式原生值构造（i64/timestamp），禁用 `ValueKey::from_value` 的 f64 化；可借鉴 `close_bench` 的 `distinct_i64` 方向直接域内哈希。

### 🟡 D8 — `sum`/`avg` 用 `f64` 对"价格（分）"有溢出风险
权威数据生成文档载明 `initialBid`/`reserve`/`price` 单位为**分（整数）**。Q4/Q9/Q16 的 `sum(price)`/`avg(price)` 若用 `f64` 累加，在 > 2^53 分（≈9e15 分 ≈ 9e13 元）时丢精度。100M bids × ~5e5 分 ≈ 5e13 分，仍在 2^53 内；但更高规模或更大基数会越界。
**要求**：金额/计数类 `sum` 至少用 `i128`（或整数累加 + 仅在展示时转 f64），与现有 `>2^53` 敏感度一致。

### 🟡 D9 — Q4 两级聚合（inner max → outer avg）只有"开放问题"，无结构方案
§2.2 把 Q4 列为一/二梯队"必覆盖"，但 §9 风险 #1 仅说"需定义中间流形态"。`stats` 的 `tier`/measure 模型是**单级**的，无法表达"按 auction 求 max，再按 category 求 avg(max)"。这是覆盖 Q4 的结构性缺口，不是边角。
**要求**：v2 给出 Q4 的具体计划结构（stats→stats 管线？或单 `stats` 内 group-by auction 求 max 后再 group-by category 求 avg？），否则 Q4 在 P2 无法落地。

### 🟡 D10 — 分组键被定义了两遍（window_spec key 与 group by），会冲突
§3 BNF 同时有 `window_spec := '<' [key_field ':'] duration … '>'` **和** `group_by := 'group by' field`。§3.1 又写 "group by none（缺省）：空键全局"。但 §3.3 所有示例都用 **window_spec 里的 key**（如 `stats<bidder:10s>`、`stats<auction:10m>`），从不出现 `group by`。
→ 两个机制语义重叠且示例只用其一，实现者无法判断"到底哪个是分组键"。
**要求**：合并为单一分组键来源（建议仅保留 `window_spec` 里的 `[key_field]`，删掉冗余的 `group by` 语法），并在 §2.1 判定条件里一致表述。

### 🟡 D11 — 全局键（空键）的"并行路径"描述自相矛盾
§5.4 "空键全局：内部按批归并到单桶（并行路径：分片预归并 → 合并）"。空键**无法按 key 分片**；全局并行只能是**批内列式向量化**（SIMD 式），不是分片。且 "foldhash 归并安全" 中的 foldhash 是 CEP 实例哈希，与 stats 累加器无关。
**要求**：把"空键并行"改为"批内列式扫描 + 单桶串行归并"，删掉"分片预归并/foldhash"措辞。

---

## 5. 内部不一致（小，但评审稿不该有）

- **§1.1 "12 个是窗口统计" 但列出 13 个**：Q4/Q5/Q6/Q7/Q8/Q9/Q11/Q12/Q15/Q16/Q17/Q18/Q19 实为 **13** 项。应改为 13 或调整清单。
- **§3.1 tier 计数措辞**："4 档：`(-∞,10000)`、`[10000,1e6)`、`[1e6,∞)`、`无条件`" 与 §4 `TierPlan` 注释 "档数 = boundaries.len()+1（含无条件档 total）" 一致（4 档 = 3 边界+total），但 §3.1 又把 4 档并列、未显式说 total 是第 4 档且总由编译器恒跟踪。建议在 §3.1 直接写"编译器恒为每度量额外展开 1 个 total 档"。

---

## 6. 给作者的修订清单（按优先级）

| 优先级 | 项 | 位置 |
|---|---|---|
| P0 | 补 §6 窗口关闭/emit 触发路径（D4） | §5.2 step3 / §6 |
| P0 | 修正符号名 `broadcast_batch`→`broadcast_batch_only`（D1） | §6 |
| P1 | `GuardMasks` 改 `eval_guard_columnar` 自由函数（D2）；`scope_key_from_column` 改 `scope_key_columnar`（D3） | §5.2 |
| P1 | 显式声明 sliding/session 对 distinct/top/last 的回撤策略（D5） | §5.2/§5.4 |
| P1 | 明确 avg 归并以 `(sum,count)` 而非 avg 值（D6） | §5.4 |
| P1 | distinct key 走列式原生值、禁用 `ValueKey::from_value` f64 化（D7） | §5.1/§5.2 |
| P2 | sum/avg 金额用 i128 防 2^53 溢出（D8） | §5.1 |
| P2 | 给出 Q4 两级聚合的具体计划结构（D9） | §2.2/§9 |
| P2 | 合并重复的分组键定义（D10） | §3 BNF |
| P2 | 修正空键并行描述（D11） | §5.4 |
| P3 | §1.1 "12"→"13"；§3.1 tier/total 措辞（D12） | §1.1/§3.1 |

**一句话**：这份设计的"复用现有基础设施"判断是准的（这是我特意逐文件核过的，不是空话），但它把"复用 X"的 X 写错了一半的符号名，且漏了让 stats 执行器**知道何时出窗**这条最关键的接线。先补 D4 和 D1–D3 再开 P1，否则实现会卡在"收不到 close 信号"上。
