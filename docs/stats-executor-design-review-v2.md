# Review v2：`stats-executor-design.md`（设计评审稿 v2, 2026-08-22）

> 审查方式：逐条比对 v1 review 的 12 条意见在 v2 文档中的落实情况，并**重新核对当前
> `wp-reactor/crates` 源码符号**（防引用错源/过期）。所有代码引用带 `文件:行号`，
> 可独立复核。配套权威语义：`wf-examples/performance/nexmark_pk/NEXMARK_AUTHORITATIVE_SEMANTICS.md`。
>
> 符号已用 grep 重新坐实（2026-08-22）：
> - `fanout.rs:523 fn scope_key_from_column`（**私有**）/ `fanout.rs:559 pub(crate) fn scope_key_columnar`
> - `fanout.rs:271 pub async fn broadcast_batch_only` / `fanout.rs:245 broadcast_with_batch`
> - `columnar.rs:253 pub fn eval_guard_columnar`（自由函数）/ `columnar.rs:116 struct GuardMasks`（CEP 专用）
> - `columnar.rs:547 fn compare_int` / `columnar.rs:447 fn cmp_vec`（**均私有**）
> - `buffer/mod.rs:267 defer_materialization` / `router.rs:335` 走 defer 分支产出 `ParsedWindow`
> - `plan.rs:185 pub enum WindowSpec`

---

## 0. 总体判断

v2 修掉了 v1 一半以上的问题，方向是**对的**、复用承诺**基本落地**。但：

- **v1 的 4 条"会产出错误结果 / 编不过"的问题（D3/D6/D7/D8）一条都没修**——其中 D3 是编译级符号错、D6/D7/D8 是正确性/精度错。
- **D10 的修复（key 移出 window_spec）没同步到示例**，引入了一个新的**语法解析错误**（N1）。
- **§6 接线描述与 router 真实架构打架**（N3），D4 的"谁触发出窗"仍未真正闭合。
- 又新增 1 处**复用私有符号**的错引（N2，`compare_int/cmp_vec`）。

建议：**先修 D3 + N1 + N2（三处都是"符号/语法写错，实现者照做会卡住或编不过"），再修 D6/D7/D8（正确性），最后补 D4/N3 接线**。

---

## 1. v1 意见落实清单

| 编号 | v1 问题 | v2 状态 | 说明 |
|---|---|---|---|
| D1 | §6 `broadcast_batch` 不存在 | ✅ **已修** | v2 §6 不再出现该符号，改用"defer_materialization 路径"（buffer/mod.rs:267 真实存在） |
| D2 | `GuardMasks` 方法误用 | ✅ **已修** | v2 §5.2 段1c 改为"复用 eval_guard_columnar"（columnar.rs:253 自由函数） |
| D3 | `scope_key_from_column` 私有 | ❌ **未修** | v2 §5.2 段1a **仍写**"复用 scope_key_from_column"（fanout.rs:523 私有 `fn`）。正确是 `scope_key_columnar`（fanout.rs:559 `pub(crate)`） |
| D4 | 窗口 close/emit 触发路径缺失 | 🟡 **部分** | v2 §5.2 新增"窗口推进"policy（fixed/sliding/session 逻辑），但**触发机制仍含糊**；且与 §6/router 架构打架（见 N3）；未提与 `WindowProgress`/ack floor 的关系 |
| D5 | sliding/session 非单调聚合回撤 | ✅ **已修** | v2 §5.2/§5.4 显式声明 P3 不支持 sliding+distinct，并给出原因 |
| D6 | avg "可交换结合"不严谨 | ❌ **未修** | v2 §5.4 仍写"avg…可交换结合归并安全"，**未补 (sum,count) 归并说明**。直接合并 avg 值会算错 |
| D7 | distinct key 走 `ValueKey::from_value` f64 丢精度 | ❌ **未修** | v2 §5.1/§5.2 仍 `HashSet<ValueKey>` + `ValueKey(row[field])`，f64 化 >2^53 丢精度未解决 |
| D8 | sum/avg 用 f64 对金额(分)溢出 | ❌ **未修** | v2 §5.1 仍 `sum: f64`。金额/计数类应 i128 |
| D9 | Q4 两级聚合无结构方案 | 🟡 **部分** | v2 §9 #1 扩写（inner→outer 管线），仍开放但已框架化 |
| D10 | 分组键定义了两遍 | ✅ **已修** | v2 BNF 去掉 window_spec 内 key，统一 `group by`；示例已改用 group by（**但示例语法引入 N1 错误**） |
| D11 | 空键并行自相矛盾 | 🟡 **部分** | v2 删掉 foldhash 措辞，但"分片预归并 → 合并"对空键仍有歧义（见下备注） |
| D12 | §1.1 "12" 实为 13 | ✅ **已修** | v2 §1.1 不再列数字；补 R1–R12 修订标记 |

> 备注 D11：空键"分片预归并 → 合并"若理解为"按流分片、每片空键累加器、窗口关闭时合并各片累加器"是合法的（与 key 分片不同，是按流分片）。但文档未解释，易与"按 key 分片"混淆。建议一句话点明"空键按**流**分片预归并、关闭时合并累加器"。

---

## 2. 仍未修复、且会导致错误结果 / 编不过的问题（最高优先级）

### 🔴 D3（未修）— §5.2 段1a 复用私有符号 `scope_key_from_column`
当前代码 `fanout.rs:523` 是 `fn scope_key_from_column(...)`（**无 `pub`**），stats 执行器（另一模块）不可见。可复用的是 `fanout.rs:559 pub(crate) fn scope_key_columnar` 与 `fanout.rs:586 fn partition_rows_by_key`（同模块，需提级）。
**修复**：§5.2 段1a 改为 `scope_key_columnar` / `partition_rows_by_key`。这是 v1 已指出、v2 漏改的同一条。

### 🔴 D6（未修）— avg 归并表述会诱导错误实现
v2 §5.4：「count/sum/avg/min/max **可交换结合**归并安全」。但 `avg(avg(a),avg(b)) ≠ avg(a∪b)`。
`StatsAccum`（§5.1）结构上存了 `sum: f64` + `count: u64`（正确），但**文档文字没说"跨分片归并以 (sum,count) 为单位、avg 仅在最终输出时由 sum/count 求得"**。实现者照字面"avg 可交换结合"会直接合并 avg 值 → 算错。
**修复**：§5.4 明确"分片归并以 `(sum,count)` 对结合，avg 仅最终输出时计算"。

### 🔴 D7（未修）— distinct_count key 精度
v2 §5.1 `distinct_set: Option<HashSet<ValueKey>>`、§5.2 `acc.distinct_set.insert(ValueKey(row[field]))`。
`ValueKey::from_value`（match_engine/match_engine/key.rs:21/112，`pub(crate)`）对数值走 **f64**，>2^53 四舍五入（`fanout.rs:1103` 已记载该分歧）。若 `bidder`/`auction` id > 2^53，stats 的 distinct 结果会与列式原生 i64 key 路径**不一致**。
**修复**：distinct key 必须从列式原生值（i64/timestamp）域内哈希构造，禁用 `ValueKey::from_value` 的 f64 化；可借鉴 `close_bench.rs` 的 `distinct_i64` 方向。

### 🔴 D8（未修）— sum/avg 金额用 f64 溢出
权威数据生成文档载明 `price` 单位为**分（整数）**。v2 §5.1 仍 `sum: f64`。`sum(price)` 在 > 2^53 分（≈9e15 分）时丢精度（100M bids × ~5e5 分 ≈ 5e13 分，当前规模尚安全，但更高基数越界）。
**修复**：金额/计数类 `sum` 至少用 `i128`（或整数累加、仅展示时转 f64），与现有 `>2^53` 敏感度一致。

---

## 3. v2 新引入的问题

### 🔴 N1（新增）— `<:30m:fixed>` 示例语法与 BNF 不兼容（解析错误）
v2 BNF：`window_spec := '<' duration [':' window_mode] '>'`。
但 §3.2/§3.3 示例混用两种写法：
- `stats<:30m:fixed>`（Q15/Q16/Q17，行 260/289/290/291）—— 带**前导冒号**；
- `stats<30m:fixed>`（Q18/Q19，行 296/297）—— 无前导冒号。

`<:30m:fixed>` 按 BNF 解析为 `<` + `:30m`（非合法 duration）+ `:` + `fixed`，**无法解析**。前导 `:` 是把 v1 的 `[key_field ':' ]` 删掉后残留的占位符，BNF 未编码。
**修复（二选一）**：
- 推荐：示例统一为 `stats<30m:fixed>`，"空键 = 不写 group by"；BNF 维持 `window_spec := '<' duration [':' window_mode] '>'`。
- 或：BNF 显式编码空键占位 `window_spec := '<' [ ':' ] duration [':' window_mode] '>'`，并说明前导 `:` 表示空键。

### 🟡 N2（新增）— §5.2 段1b "复用 compare_int/cmp_vec" 复用私有符号
`compare_int`（`columnar.rs:547` `fn`，私有）、`cmp_vec`（`columnar.rs:447` `fn`，私有）都在 `match_engine/columnar.rs`，stats 执行器（新模块）不可见，且它们是 **CEP 列式 guard 求值**的底层助手，并非通用"列 vs 标量比较"API。
**修复**：tier_idx 计算应复用现有列式比较基础设施的 `pub` 入口，或 stats 自行实现"价格列 vs 边界数组"的列式比较（推荐后者，避免耦合 CEP guard 语义）；文档标注清楚。

### 🟡 N3（新增）— §6 接线描述与 router 真实架构矛盾，且 backpressure 未提
v2 §6：「rule_fanout **注册 stats 订阅**」→「Router 解析批次时**内联直调** StatsExecutor::process_batch（…零拷贝，**不走 mpsc channel**）」。
两处与代码不符：
1. **"注册订阅" 与 "不走 channel" 自相矛盾**：fanout 订阅者经 `broadcast_batch_only`/`broadcast_with_batch` 投递，rule_task 走的就是 mpsc channel（`RulePush`，events=None 仍经 channel）。"注册订阅"即意味走 channel。
2. **"Router 内联直调" 不符 router 架构**：真实 `router.rs:335` 分支只产出 `ParsedWindow { events: None, shard_rows }` 并 `continue`，**不在解析循环里调用任何执行器**；真正消费的是 fanout/窗口 actor（经 channel 投递）。不存在"router 内联直调外部执行器"的钩子。
3. **同步内联的 backpressure 风险未提**：若 stats 真在 router/解析路径同步内联执行（段1 列式 + 段2 行式 distinct/last/top），慢 stats 规则会**阻塞整条 router 分发**，连共享 fanout 的 CEP rule_task 一起卡住。

**修复**：明确 stats 的真实集成点——
- 推荐：stats 作为 **fanout 订阅者**（与 rule_task 并列），走 `broadcast_batch_only`（raw batch，events=None，defer_materialization 路径），在窗口 actor 内调用 `process_batch`；空键单实例、带 key 走 `register_sharded`。这复用现有投递/ack 机制，无需新造"内联直调"。
- 并补一句：stats 执行器是否参与 `WindowProgress`/ack floor（若共享底层 window buffer，慢 stats 不 ack 会让驱逐卡住——即 evictor cursor-gap 教训），需显式说明。

---

## 4. 仍开放、但已框架化的问题（沿用 v1，非阻塞）

- **D9（部分）** Q4 两级聚合（inner max → outer avg）仍只在 §9 列为开放问题，无具体 `StatsPlan` 结构。P2 前需给出（stats→stats 管线？还是单 stats 内两级 group-by？）。
- **D4（部分）** "窗口推进"policy 已写（fixed/sliding/session），但触发信号源（内部水印 vs 窗口关闭事件）与 ack 集成（N3 第3点）仍待钉死。

---

## 5. 给作者的修订清单（按优先级，可直接照改）

| 优先级 | 项 | 位置 | 改法 |
|---|---|---|---|
| P0 | D3 私有符号 | §5.2 段1a | `scope_key_from_column` → `scope_key_columnar` / `partition_rows_by_key` |
| P0 | N1 语法错 | §3.2/§3.3 示例 | `<:30m:fixed>` → `<30m:fixed>`（空键=无 group by），或 BNF 编码空键占位 |
| P0 | N2 私有符号 | §5.2 段1b | tier_idx 改用 stats 自有列式比较或 `pub` 入口，删 `compare_int/cmp_vec` 引用 |
| P0 | N3 接线矛盾 | §6 | 改为"fanout 订阅者 + 窗口 actor 内 process_batch"，删"内联直调/不走 channel"，补 ack 语义 |
| P1 | D6 avg 归并 | §5.4 | 明确"以 (sum,count) 结合，avg 仅输出时算" |
| P1 | D7 distinct 精度 | §5.1/§5.2 | distinct key 走列式原生值，禁 `ValueKey::from_value` f64 |
| P1 | D8 溢出 | §5.1 | sum 金额用 i128 |
| P2 | D4 触发信号 | §5.2/§6 | 钉死水印/关闭事件来源 + WindowProgress/ack 关系 |
| P2 | D9 两级结构 | §2.2/§9 | 给 Q4 具体 StatsPlan |
| P3 | D11 空键并行措辞 | §5.4 | 点明"按流分片预归并、关闭时合并累加器" |

**一句话**：v2 把"复用符号写错名""sliding+distinct 没声明""分组键重复""计数错"都修了，方向值得肯定；但**最该先动的 D3/N1/N2（符号与语法写错，实现者会卡住或编不过）+ N3（接线与真实架构打架）还没动**，D6/D7/D8（会算错/丢精度）也还在。先清这 7 条再开 P1。
