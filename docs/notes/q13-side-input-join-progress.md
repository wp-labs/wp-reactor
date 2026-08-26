# Q13（Bounded Side Input Join）对齐进度 — 接力手记

> 2026-08-23 更新，2026-08-24 追加 30M 全量重跑 + OSS/VVR 对照 + q9 deferred
> `as label` 冗余物化消除 + **q4/q9 −62% 正确性 bug 根治（bid 窗字节上限驱逐，
> 兼得 q9 4.7× 提速）**。本文件是跳 session 接力点：重开 session 先从文末
> 「## 下一步」读起——当前最高优先级是 **全量 22 查询 30M 重跑（两份基准文档
> 的数字已过时）**。

## 目标

Q13 从「🟡 形状对齐」真正对齐权威语义：

```sql
SELECT B.auction, B.bidder, B.price, B.dateTime, S.`value`
FROM bid B JOIN side_input FOR SYSTEM_TIME AS OF B.p_time AS S
ON mod(B.auction, 10000) = S.key
```

现行 q13.wfl 已拆双规则链 q13a（on each bid → bid_mod 中间窗口）→ q13b（on each m + join side_input snapshot + yield detail=side_input.value）。每 bid 一行，按 mod(auction,10000) 查有界侧输入静态表（key 0..9999）富化输出。

## 当前状态（截至 2026-08-23 23:25）—— Q13 已对齐，全部验证通过

### ✅ 已完成并验证

0. **【本 session】端到端卡点根因定位并修复（knowdb CSV 类型推断）**
   - **根因**：`bootstrap.rs` knowdb CSV 加载把**所有列都存成 `Value::Str`**（无类型推断）→ provider join 索引键 `JoinKey::Str("2345")` 与 lookup 键 `JoinKey::Number(bits(2345.0))`（`b.auction % 10000` 是 f64 mod）**类型不匹配 → 每次索引 lookup miss → 回退 O(rows)=10000 行全表扫描** → 索引形同虚设，q13b 卡死
   - 诊断证据链：Q13DIAG 日志确认 set_join_key 已调（索引在路径上）→ rule_task.rs L923 确认列式 join 路径用 `RegistryLookup`（有索引分支）→ profiling 确认 `serialize_nanos` 230s/4min（列式 join 执行被计入 serialize 采样）→ bootstrap.rs L436 实锤全列 Str
   - **修复**（`crates/wf-runtime/src/lifecycle/bootstrap.rs`）：新增 `infer_knowledge_value(cell)`（数字串→Number、true/false→Bool、其余→Str，`is_finite` 防 NaN/Inf），CSV 与 PG 加载共用
   - 新增测试：`infer_knowledge_value_types_numeric_bool_and_string`（bootstrap.rs）+ `load_knowledge_numeric_csv_join_index_hits_number_key`（bootstrap_r4.rs，q13 回归：CSV 数字 key 进 Number、索引命中）
   - **实测（`TOTAL=1m ./bench.sh q13 replay 1m`，23:19:55）**：
     - `q13a_bid_mod` EMIT = **920,000** ✅；`q13b_side_input_join` EMIT = **920,000** ✅（此前 55,917）
     - RSS_peak 1,615MB → **494MB**；serialize_nanos 230s → **0.49s（467×）**；SUMMARY clean

1. **ProviderWindow join 索引**（`wf-engine/src/window/provider/mod.rs`，未提交）
   - `set_join_key()` 建 O(rows) 哈希索引（键类型 `JoinKey`，与 buffer 窗口同截断语义）
   - `join_lookup(&Value)` O(1)；`load()` 替换 rows 后自动重建索引
   - 单测 `set_join_key_builds_index_and_lookup_is_o1` 通过

2. **window_lookup.rs provider 分支接线**（`wf-runtime/src/engine_task/window_lookup.rs`）
   - `join_lookup` provider 分支：有索引走索引（O(1)），无索引回退全表扫描
   - 16 个 window_lookup 单测全过

3. **spawn.rs set_join_key 覆盖 provider 窗口**（`wf-runtime/src/lifecycle/spawn.rs`）
   - 与 buffer 窗口同路径设置（首 join 条件右字段）
   - ~~临时诊断日志 Q13DIAG~~ **已移除**（23:22 重建后确认日志零残留）

4. **flush_pipes 真实 seq + Notify**（`wf-runtime/src/engine_task/rule_task.rs`）
   - 广播从固定 u64::MAX 改为 append 返回的真实 seq（下游 ack 反映真实消费进度，否则 bench 完成判定提前 SIGTERM）
   - append 后显式 `notify_waiters()`（pull 下游消费不停滞）
   - 对应更新测试 `pure_relay_broadcasts_to_sharded_downstream`（断言 seq==0 而非 u64::MAX）

5. **性能基准 `q13b_join_bench`**（`wf-engine/src/match_engine/tests/q13b_join_bench.rs`，**已注册到 tests/mod.rs**）
   - 运行：`cargo test --release -p wf-engine q13b_join_bench -- --ignored --nocapture`
   - **实测数据（N=1M × 10000 行 side_input，release）**：

     | 形态 | ns/行 | 相对基线 |
     |------|-------|----------|
     | no-join 列式 each（基线） | 233.2 | 100% |
     | join + 全表扫描 | 141,227.1 | 60,559%（606× 退化） |
     | join + 哈希索引 | 446.2 | 191% |

   - **索引相对全表扫描加速 316.5×**，防御断言（>50×）通过

6. **端到端 10m verify（23:20:46，`TOTAL=10m ./bench.sh q13 replay 10m --verify`）**：
   - 引擎 EMIT vs oracle（真实规则引擎 ground truth）**identical ✅**：q13a/q13b 各 9,200,000
   - EPS=449,346；RSS_peak=9,362MB（见 ⚠️ 观察 1）；SUMMARY clean
   - 注：oracle 侧 q13a=q13b=9,200,000（join miss 的 bid 也计数，snapshot 外联语义）

7. **回归全绿**（最终二进制，Q13DIAG 移除后）：
   - `cargo test -p wf-engine --lib`: **1054 passed** / 0 failed / 39 ignored
   - `cargo test -p wf-runtime --lib`: **485 passed**（含本次新增 2）/ 0 failed / 1 ignored
   - `cargo test -p wfgen --lib`（warp-fusion workspace）: **149 passed**
   - `./bench.sh q13 replay 1m`（最终二进制）: 920,000/920,000 clean
   - `./bench.sh q20 replay 1m --verify`（join 代表查询回归）: **identical ✅**（196,517 = 196,517）

8. **文档**：`docs/design/provider-window.md` 已加「Join 索引」一节（含实测表）；`wf-examples/.../docs/CAPABILITY_GAP_MATRIX.md` Q13 🟡→✅（汇总 20 已有 / 特殊口径 2→1）

### ⚠️ 观察（非阻塞，待后续确认）

1. **10m 时 RSS_peak=9,362MB**：bid_mod 中间窗 over=2d 且事件时间跨度仅 ~30min（10m 数据）→ 窗口永不驱逐，9.2M 行全保留。这是双规则链物化中间窗的代价（权威 SQL 是 CTE 不物化）。1m 数据 494MB。若需压内存：缩中间窗 over 或评估中间窗是否需要逐出策略（勿擅自改 windows.toml 语义）。
2. **全量 22 查询 10m --verify 未跑**（`./bench.sh all replay 10m --verify` ~40min 串行）：本次改动只影响 knowdb 加载（仅 q13 用 provider），q1-q22 其余查询不引用 side_input，风险面为零；q20（活 join）已抽查无回归。

## 模型/配置层（wf-examples，已提交 `6a43401` 后新增、未提交）

- `models/schemas/nexmark.wfs`：+`bid_mod` 中间窗口、+`window<provider> side_input`
- `models/queries/q13.wfl`：双规则链 q13a/q13b（含 3 个 inline test：mod_key / hit / miss）
- `models/schemas/windows.toml`：+`bid_mod`（2GB/2d）、+`side_input`（table / over_cap=0s）
- `knowdb.toml` + `side_input/side_input.csv`（key 0..9999 → value-<key>；CSV 数字列现在被推断为 Number）

## oracle 中间管道（warp-fusion，已提交 `cac755a` 后新增、未提交）

- `oracle/mod.rs`：中间输出（yield target 被 bind）既 push_alert 又转 Event feed 下游（多级队列）
- `cmd_verify_nexmark.rs`：并行分组从 chunks 改为并查集按 yield-bind 依赖合并同组
- 验证：oracle q13a=9.2M、q13b=9.2M（10m，与引擎 identical）✅

## Pitfalls（已踩坑，勿重蹈）

- **`mod` 是关键字**：`use crate::...::tests::mod` 编译错；且 `match_engine::tests` 模块私有。q13b_join_bench.rs 里这行死代码已删
- **不要用 `Value` 做 HashMap key**：无 `Hash` trait，编译 E0599。用 `JoinKey::from_value(v)`
- **oracle 中间 feed 必须同组**：verify 的 chunk 并行分组成破坏跨组依赖 → 已改并查集
- **broadcast seq=u64::MAX 让下游 ack 失真**：bench 完成判定提前 SIGTERM → 必须真实 append seq
- **flush_pipes 直接 append 不走 actor 不触发 Notify**：pull 下游消费停滞 → append 后显式 notify_waiters
- **ProviderWindow 全表扫描是设计意图注释的**：「small static snapshots, O(rows) per lookup is expected」——但 10000 行 × 92 万事件卡死，需索引
- **knowdb CSV 全列 Str 是隐形炸弹**：数字列 join 数字表达式时索引键类型不匹配（Str vs Number）→ 每次 miss → 回退全表扫描。类型推断已在 bootstrap.rs 修复；改加载逻辑务必同步看 `JoinKey::from_value` 两侧类型
- **`wfusion` binary 在 warp-fusion workspace**（`warp-fusion/target/release/wfgen|wfusion`），不是 wp-reactor；bench.sh 用的就是这两个
- **macOS 无 `timeout` 命令**：用 terminal 工具的 timeout_ms 控制跑批

## 文件清单（未提交修改）

**wp-reactor**（`git status` 可见）：
- `M crates/wf-runtime/src/lifecycle/bootstrap.rs`（本 session：CSV/PG 类型推断 `infer_knowledge_value` + 单测）
- `M crates/wf-runtime/src/lifecycle/bootstrap_r4.rs`（本 session：q13 回归测试）
- `M crates/wf-engine/src/window/provider/mod.rs`（索引）
- `M crates/wf-runtime/src/engine_task/mod.rs`
- `M crates/wf-runtime/src/engine_task/rule_task.rs`（flush_pipes 真实 seq + notify）
- `M crates/wf-runtime/src/engine_task/window_lookup.rs`（provider 索引分支）
- `M crates/wf-runtime/src/lifecycle/spawn.rs`（set_join_key 覆盖 provider；Q13DIAG 已移除）
- `M crates/wf-runtime/src/engine_task/tests.rs`（pure_relay 断言 seq==0）
- `?? crates/wf-engine/src/match_engine/tests/q13b_join_bench.rs`（bench，已注册 mod.rs）
- `M crates/wf-engine/src/match_engine/tests/mod.rs`（+mod q13b_join_bench）

**warp-fusion**：oracle 中间管道 + verify 并查集分组（未提交）
**wf-examples**：q13.wfl + schemas + knowdb + side_input/ + `docs/CAPABILITY_GAP_MATRIX.md`（Q13→✅）（未提交）

## 2026-08-24 追加：30M 全量重跑 + OSS/VVR 对照

### 30M 全量重跑（wp-reactor `584852e`，merge 列式 List-Index guard 后）

- 背景：用户 merge 远程分支（`584852e` 含 `6f3ecd5` 列式 List-Index guard 及列式门控扩展）并重建二进制
- `./bench.sh qN replay 30m` 逐查询独立跑，**22/22 全部 `[clean]`**，无卡死无超时
- **Q13 复验**：EPS=399,384、RSS 15.5GB、EMIT 27.6M/27.6M（修复在 30M 稳定）
- 大涨幅（q1/q7/q10/q11/q12/q21 3.2-3.7×）归因于 **merge 的列式代码，非日志级别**——`conf/wfusion.toml` 调试遗留 debug 已恢复 info（debug 日志为节流输出，非每事件热路径）
- 逐查询 EPS/RSS：见 `wf-examples/.../data/bench_qN_replay.txt`

### OSS/VVR 对照（`docs/OSS_VVR_BASELINE.md` §3 已同步）

- 工具：`wf-examples/.../scripts/compare-metrics.sh`（读 bench_q*_replay.txt 对拍基线）
- 结果：vs OSS **3.05×~197.78×**；vs VVR **1.44×~52.51×，20/20 有基线全部达 VVR**（q14 从 0.9× 回升 2.15×）
- 无基线：q6、q13（白皮书未发布这两条）
- 口径差异：基线 1 亿条/8CU(VVR)/12vCPU(OSS)/Blackhole，本次 30M/16 核 Mac/本地文件 sink

## 2026-08-24 追加：q9/q4 deferred 求值优化（`as label` 冗余物化消除）

### 根因（微基准归因，非猜测）

`execute_deferred_join` 里 `as label`（Q9 `as winner`）把胜出整行物化成
`Value::Object` 注入 ctx，但**文档形态 `winner.bidder` 从不读它**：

- `winner.bidder` 经 `rewrite_expr_label_refs`（compiler/mod.rs L1214）编译成
  `Path{alias: winner, segments: [bidder]}`；
- `eval_field_value`（key.rs L371）对 Path **丢弃 alias**，把 `segments[0]` 当
  根字段名读 `fields[bidder]` —— 即 `enrich_join_row` 已注入的裸列名。
- 所以每条输出行都白建一个 `EngineHashMap` + 逐列 clone。

### 修复：编译期按需物化（`plan_reduce_label_reads`）

**不是删除注入**——`checker/scope.rs::resolve_simple` 显式允许**裸标签引用**
（`winner` 自身作为 object），删除会静默破坏该能力（现有 17 个 deferred 测试
只覆盖 `winner.bidder`，全过 ≠ 安全）。改为 plan 期分析（复刻 `live_joins` /
`plan_close_ctx_fields` 的保守范式）：

- `executor/mod.rs`：新增 `ReduceLabelReads{All, Named(HashSet)}` +
  `plan_reduce_label_reads(plan)` + `visit_ctx_field_reads`；收集 where/score/
  entity/yield/lets 里**所有 FieldRef 的 `field_ref_name()`**；
- 判据 = `field_ref_name() == label`，精确且完备：`Simple(label)` 裸引用、
  `Qualified(x, label)` / `Bracketed(x, label)`（`field_ref_name` 取第二段 → 读
  `fields[label]`）全部命中；`winner.bidder` 不命中（名字是 `bidder`）；
- 保守兜底：qualified 函数（`stat.*`，可动态解析名字）、`PresetParam`、未知
  Expr 变体 → `All`（永远注入）。**普通函数递归进 args**（`fmt(...)` 不退化——
  这点关键：`visit_expr_fields` 对 FuncCall 一律 force_all，直接复用会让 q9 的
  `detail = fmt(winner {}, winner.bidder)` 永远走注入，白优化）；
- `deferred_exec.rs`：`if self.reduce_label_reads.needs(label) { inject... }`。

### 实测

| 项 | 优化前 | 优化后 |
|----|--------|--------|
| `deferred_bench` eval-maxrow+tie | 1353 ns/op | **1106.9 ns/op（−18%）** |
| `deferred_bench` eval-exists（无 reduce/label 对照） | ~1069 ns/op | 1068.8 ns/op |
| q9 30M EPS | ~925,000 | **939,682（+1.6%）** |

**成本对账闭环**：优化前 maxrow 比 exists 多 284ns，正好是 label 物化；现在只差
38ns（纯 reduce 选行）。端到端 +1.6% 与预测吻合（1.8M 输出行 × 246ns ≈ 0.44s /
q9 总 ~32s ≈ 1.4%）——**说明微基准的成本模型可信，也说明 label 物化只占 q9 总
时长 ~1.4%，deferred 求值不是 q9 的主要瓶颈**（见下）。

### 新增回归测试（填补覆盖盲区）

`match_engine/tests/deferred_join.rs`：
- `execute_deferred_join_bare_label_yields_whole_row_object`：裸 `winner` → 断言
  yield 到 `Value::Object` 且含 bidder/price/auction（此前**零运行时覆盖**）；
- `execute_deferred_join_qualified_label_name_reads_injected_object`：`a.winner`
  （`Qualified` → `field_ref_name` = `winner`）同样读到注入 object，锁定门控为何
  用 `field_ref_name` 而非仅 `Simple` 判定。

回归：`cargo test -p wf-engine -p wf-runtime --lib` → **1068 + 485 passed / 0 failed**。

### 未提交文件（本次）

- `M crates/wf-engine/src/match_engine/executor/mod.rs`（ReduceLabelReads + 分析）
- `M crates/wf-engine/src/match_engine/executor/deferred_exec.rs`（按需注入）
- `M crates/wf-engine/src/match_engine/tests/deferred_join.rs`（+2 回归测试）
- `M crates/wf-engine/src/window/buffer/tests.rs`（`join_index_append_bench`：
  `index_batch` = **42 ns/row**，纯 append 2ns → +21×）

### Pitfalls（本次新增）

- **`as label` 不能直接删注入**：checker 允许裸标签引用，删了静默返回 null；
  测试全过只因覆盖盲区（只测 `label.field`）。
- **别复用 `visit_expr_fields` 做这个判定**：它对 `FuncCall` 一律 `force_all`
  （为了 close-ctx 的 `_step_*` 合成字段），q9 的 yield 是 `fmt(...)` → 门控恒真。
- **反方向已排除**：跳过 `enrich_join_row` 而保留 label 注入 → 破坏 7 个测试
  （证明 enrich 才是真实读路径）。
- `rg -r` 是**替换**标志，不是 recursive；误用会让输出被改写（本 session 踩过两次
  ——`rg -rn 'fn field_ref_name'` 的输出里函数名被替换成 `n`，看着像代码坏了）。

## q9 剩余瓶颈（尚未解决，下一站）

- 成本对账：deferred 求值 2.26s（含本次省下的 0.44s）+ join 索引 `index_batch`
  1.7s，但 q9 总跑批 ~32s → **~80% 在微基准未覆盖的单 worker 批处理里**
  （pull 循环 / 列式 mask / emit 序列化 / 真实多列 schema 下更高的 index_batch）。
- 规模因子：q9 的 join 目标是 bid 27.6M 行，q8 是 auction 1.8M，差 15×，对应
  q9 比 q8 慢 25×。
- 继续深挖需 `PROFILE=1 ./bench.sh q9 replay 10m`（instrument-coverage 插桩）拿
  真实行级覆盖，**不要再加微基准**（已到边际收益）。

## ✅ q4/q9 −62% 正确性 bug：已定位并修复（2026-08-24）

### 判别数据（先拿规模依赖关系，而不是直接读代码）

| 查询 | 规模 | 引擎 EMIT | oracle | 偏差 | evict |
|------|------|-----------|--------|------|-------|
| q9 | 1m | 55,669 | 55,669 | 0.0% ✅ | 0 |
| q9 | 10m | 557,204 | 557,204 | 0.0% ✅ | 0 |
| q9 | 30m（修前） | ~635k | 1,672,559 | **−62%** ❌ | **468** |
| q9 | 30m（修后） | 1,672,559 | 1,672,559 | **0.0% ✅** | **0** |
| q4a | 30m（修后） | 1,672,559 | 1,672,559 | **0.0% ✅** | **0** |

小规模全对、大规模才错，且错的那次 `evict>0`——直指资源驱逐，不是语义 bug。

### 根因（算术上闭合）

`bid_events` 窗口 `max_window_bytes = 2GB`，而 30M 数据的 27.6M 行 bid 需 ~5.4GB
（~196B/行）。日志实测：**468 条 `memory eviction` 告警，全部是 `bid_events`，
累计丢 17,180,418 行 = 全部 bid 的 62.2%**。而 q9 输出偏差是 **−62%**——
**丢多少 bid 就丢多少输出**。

为何驱逐能丢掉还要用的行？`window/buffer/mod.rs` L592-624 的内存驱逐**是**尊重
 ack floor 的（`if tb.seq >= ack_floor { break; }`，注释明确写「never drop a batch a
live consumer has not yet read」）——但 **join 目标窗口的 lookup 不是 pull 消费者**。
q9 的源是 `auction_events`，它在 `bid_events` 上**没有 ack 游标/保留 pin**，所以
`min_acked()` 根本不知道 deferred pending 还需要这些行 → 自由丢弃。

叠加时序因素（`nexmark.wfs` 头注释早已记录）：q9 的 deferred 评估由 **auction
watermark** 驱动（慢，只占 2% 事件），而 bid 窗口驱逐由 **bid watermark** 驱动
（快），两者事件时间差 = 摄入-消费积压滞后（30m 可达 ~1700s）。

### 修复（已验证）

`wf-examples/.../models/schemas/windows.toml`：`bid_events.max_window_bytes` 2GB → **8GB**
（over=1h > 数据 span 3000s → 语义上本就要全保留；上限低于需求等于用静默丢
数据换内存）。只改配置，未改引擎代码。

### 意外收获：同一根因也是 q9 那 80% 无法归因的性能黑洞

| q9 30M | 修前 | 修后 |
|--------|------|------|
| EPS | 939,682 | **4,446,676（4.7×）** |
| RSS_peak | 6,860MB | 8,563MB（+1.7GB）|
| evict | 468 | 0 |
| 对拍 | −62% ❌ | identical ✅ |

机制：驱逐清扫就在 **append 热路径的 `log.write()` 写锁内**，而且还要
`remove_batch_from_index` 摘 1718 万行的 join 索引 → 与 lookup 的读锁死磕。
**这就是微基准无法覆盖的那 80%**——所以之前把 q9 归因到「deferred 求值慢」
是错的方向（`execute_deferred_join` 的 1353ns 只占 ~1.4%）。q9 也不再是最慢的
查询了。q4：EPS=3,887,727、RSS 8,784MB、evict=0、identical ✅。

### 连带排查（重要）

30M 历史跑批里 **`evict=468` 出现在几乎所有查询**（q1/q2/q3/q7/q8/q10/q11/
q12/q17/q18/q21）——所有查询都摄入全量 27.6M bid，2GB 上限每次都被顶爆。区别：

- **流式查询**（`on each bid`，bid_events 是**源**）：有 pull ack 游标 → 驱逐只丢
  已 ack 的行 → **不影响正确性**（也解释为何 q1 等 evict=468 仍然对拍一致）；
- **把 bid_events 当 join 目标**（q9/q4a）：无 ack pin → 静默丢数据。
- q3 的 evict=468 也是 bid_events（q3 根本不用 bid）→ **q3 的 −16% 是另一个 bug**，
  与本次无关，仍待查。

### 根治方案（D4）——**已落地并验证**，见下一节

### 待办（因本修复而产生）

1. **全量 22 查询 30M 重跑**：q9/q4 的 EPS 已大幅变化（4.9×）→
   `OSS_VVR_BASELINE.md` 与 `CAPABILITY_GAP_MATRIX.md` 的数字需重新采集
   （**当前两份文档的 30M 数据已过时**）。注：D4 只影响带 `emit_at` 的
   deferred 规则（q4/q8/q9），其余查询行为不变（配置已回退到 2GB）。
2. 全局 `max_total_bytes = 20GB`：pin 会让 deferred 查询的右窗瞬时超过单窗预算
   （q9/q4 实测 RSS ~8.5GB），需盯住是否触发全局驱逐/背压。

## ✅ D4 保留 pin 已落地（2026-08-24）——正确性不再依赖魔法常量

### 思路

内存驱逐本来就尊重 `ack_floor`（「never drop a batch a live consumer has not yet
read」），且注释已写明宁可瞬时超预算也不丢未读数据——**deferred join 只是没参与
这个机制**（它不从右窗 pull，无消费者槽位）。D4 就是从另一侧补上这个口。

**为何用事件时间而非 seq**：读者知道的就是事件时间——每个挂起实例需要
`[lo_ns, hi_ns]` 内的右窗行，前沿就是 `min(lo_ns)`，不需要事件时间→seq 映射。

**范围划定**：pin 只作用于**内存**驱逐（按窗字节上限 + 全局 `max_total_bytes`）；
`over` 的时间驱逐故意忽略 pin——`over` 是查询**声明的**保留语义，越过它丢行是
语义而非资源压力（`evict_expired` 的注释也明确这是有意权衷）。**这同时就是
无界增长的安全论证**：即使 pin 永不推进，保留量上界仍是 `over`。

### 改动

| 文件 | 内容 |
|------|------|
| `wf-engine/src/window/progress.rs` | `WindowProgress` +`pins: RwLock<Vec<Weak<AtomicI64>>>`、`register_retention_pin()`（**初值 `i64::MIN` = fail-safe 全保留**）、`min_retention_ns()`（无 pin → `i64::MAX`） |
| `wf-engine/src/window/buffer/mod.rs` | `retention_floor_ns()`、`register/preregister/take_retention_pin()`、`parked_pin` 寄存位；append 路径内存驱逐加 `if pinned && tb.event_time_range.1 >= retention_ns { break; }`；驱逐告警加印 `retention_floor_ns=`（可诊断性） |
| `wf-engine/src/window/buffer/eviction.rs` | `evict_oldest_acked()`（全局内存上限路径）同样尊重 pin |
| `wf-runtime/src/lifecycle/spawn.rs` | 在已有的 `set_join_key` 循环里，对 `emit_at` join **同步**预注册 pin |
| `wf-runtime/src/engine_task/rule_task.rs` | `DeferredRuntime` +`retention_pin`、`publish_retention_floor()`（批次末 + 定时扫描调用）、`release_retention_floor()`（EOS） |

### 实现过程里踩的三个坑（都靠实测定位，已加回归测试钉死）

手段：把 `retention_floor_ns` 打进驱逐告警 + 看驱逐告警的**时间分布**，不是猜：

1. **启动竞态**（−62% → −3.28%）：`RuleTask::new` 在 `tokio::spawn` 的 future 内执行，
   而 `spawn_receiver_task` 紧接 `spawn_rule_tasks` 开始摄入 → pin 注册与首批 append
   竞争。实测：残留驱逐全在头 2.6s。修：**同步**预注册（照 `register_progress`
   的既有范式）+ `parked_pin` 寄存交接。
2. **初值语义搞反**（−3.28% → −0.67%）：我把「watermark 未初始化」映射成
   `i64::MAX`（无所需），而启动时的定时扫描（1s 间隔）先于首批驱动事件触发
   → 把刚预注册的 pin 立即释放（告警里 `retention_floor_ns=9223372036854775807`
   实锤）。正确语义：**还没见过事件 = 可能什么都需要 = `i64::MIN`**。
3. **`missed` 必须计入前沿**（−0.67% → **0%**）：最初的理由「miss 是右窗 append
   滞后，pin 住更旧的行救不了」是错的——那些行稍后才落地，必须活到 EOS 重试
   那一刻。判别证据：同次 q9 也是 evict=5 却 identical，q4a 多一条 q4b 规则、任务
   更慢 → miss 更多 → 更依赖 EOS 重试，所以只有 q4 丢。

### 验证结果（**全部在原始 2GB 上限下**）

| 查询 30M | 修前（无 pin） | 8GB 配置绕过 | **2GB + D4 pin** |
|-----------|----------------|--------------|------------------|
| q9 对拍 | −62% ❌ | identical ✅ | **identical ✅** |
| q9 EPS | 939,682 | 4,446,676 | **4,595,917（4.9×）** |
| q9 evict | 468 | 0 | **0** |
| q4 对拍 | −62% ❌ | identical ✅ | **identical ✅** |
| q4 EPS | — | 3,887,727 | **3,944,628** |
| q8 对拍 | identical ✅ | — | **identical ✅**（evict=468）|

q8 仍有 468 次驱逐却完全正确——它的 join 目标不是 bid_events，**双向证明 pin 不会
过度保留**。RSS：q9 8,545MB / q4 8,766MB（与 8GB 配置相当——保留量自适应到
「真实需要」而非「上限」）。

### 回归测试（新增 8 个）

- `window/progress.rs`：`retention_pins_are_independent_of_consumer_slots`
  （无消费者仍可 pin、min 胜出、drop 自动释放、fail-safe 初值）
- `window/buffer/tests.rs`：`memory_eviction_respects_retention_pin`（**双向**：前沿
  之前仍可驱逐，否则 pin 就是内存泄漏）、
  `preregistered_pin_protects_before_the_reader_starts`（启动竞态）、
  `evict_oldest_acked_respects_retention_pin`（全局 cap 路径）
- `engine_task/rule_task.rs::retention_pin_tests`（4 个）：
  `uninitialized_watermark_publishes_fully_pinned`、`empty_pending_publishes_watermark`、
  `floor_covers_both_pending_and_missed`、`release_unpins_the_window`

全量：`cargo test -p wf-engine -p wf-runtime --lib` → **1072 + 489 passed / 0 failed**；
clippy 无新增告警（只剩 2 个既有：`join_then_key.rs` redundant closure、
`match_engine/mod.rs` too-many-args）。

### 2026-08-24 三轮：D4 扩展到 snapshot/asof join-lookup 读者

**动机**：D4 只覆盖 `emit_at` deferred join，而 snapshot/asof join 目标窗口同样
静默丢数据的风险（历史根因：q3 差 50% / q6 差 36% / q20 差 21% 之一）——字节上限
一旦收紧，驱逐会丢掉快照还需要的行。

**设计**：snapshot 语义 = join 时刻的完整状态，驱动事件可引用**任意老**的实体行
（q3 join person / q6·q20 join auction）——无法像 deferred 那样按 `min(lo_ns)`
精确化前沿 → **全保留（`i64::MIN`）直到任务结束**（pin drop 自动释放）。
内存代价：实体表目标 @30M 全量 person 600k + auction 1.8M ≈ 470MB，远小于
2GB 上限 → 零行为变化（纯防御）；大流表目标由 `over` 时间驱逐封顶。

**改动**：
- `spawn.rs`：`preregister_retention_pin` 条件扩为 `emit_at.is_some() || Snapshot || Asof`；
- `rule_task.rs`：`RuleTask` 新字段 `snapshot_pins: Vec<Arc<AtomicI64>>`——对非
  emit_at 的 snapshot/asof join 目标 `take_retention_pin()`，全保留、drop 自动释放；
- `buffer/mod.rs`：`retention_floor_ns` 从 `pub(in crate::window)` 改 `pub`
  （只读查询，测试/监控可读）。

**验证**：
- 新集成测试 `snapshot_join_buffer_target_gets_retention_pin`（provider_join_
  integration_tests.rs）：snapshot join buffer 目标 → 任务构造后 `retention_floor_ns
  == i64::MIN`，drop 后回 `i64::MAX`；
- `cargo test -p wf-engine -p wf-runtime --lib` → **1075 + 490 passed / 0 failed**；
- q20 30M identical ✅（EPS 4.53M；驱逐全在 bid_events 驱动流，被 pin 的
  auction_events 零驱逐）；
- q3 30M 保持 −16.2%——person 全保留后无变化，**再次反证 q3 的 −16% 与驱逐无关**，
  是独立语义/执行 bug。

### 尚存的类级缺陷

1. 字节上限驱逐丢行仍只有一条 `log::warn!`（现已带 `retention_floor_ns=`，实测很
   好用），但**仍无「结果可能不完整」的正确性信号/指标**。
2. ~~D4 只覆盖 `emit_at` deferred join~~ → 已扩到 snapshot/asof；**eager interval
   join（`within` 无 `emit_at`，P2 能力）仍无 pin**——nexmark 目前无此形态查询
   （q4/q8/q9 都是 deferred），若出现需按「驱动批事件时间下界」设计。

### 2026-08-24 二轮 review：发现并修复 1 个真实 bug + 1 个热路径开销

**Bug（evictor 全局驱逐路径）**：Phase 2 的候选选择只检查 ack floor（`oldest_seq
< min_acked`），被 pin 住的窗口仍会被选中 → `evict_oldest_acked` 因 pin 返回 `None`
→ 旧代码 `None => break` **静默跳出、不设 `memory_pressure`** → 全局超预算时引擎
既不回收也不停住、继续 append → **OOM 风险**（违背“宁可背压不丢数据”的设计）。
修复：
- 新增 `Window::front_pinned_by_retention()`（front batch 事件时间是否在 pin 前沿内）；
- 候选选择排除 pin 住的窗口（其他窗口仍正常回收，不被误报 pressure）；
- `None` 分支防御性设 `memory_pressure = true`（防窗口竞态变空等）。

**热路径开销**：append 原来无条件取 `min_acked()` + `retention_floor_ns()` 两把
progress 读锁（即使不超预算——所有查询的每批 append 都付）。改为 `over_budget`
短路：多数 append 零锁开销。锁序保持 progress.read → log.write 一致（绝不持 log
锁取 progress 锁，防死锁——这条在 review 里逐路径验证过）。

**假设文档化**：`publish_retention_floor` 空集→watermark 依赖驱动流事件时间单调；
乱序时靠 missed 机制把前沿拉低（实测首 miss 极早发生，无丢失）。

**新增测试（3）**：
- `window/evictor.rs`：`evictor_global_memory_cap_pinned_window_signals_pressure`
  （超预算 + 只剩 pin 窗口可回收 → 必须报 pressure；释放后恢复回收）、
  `evictor_global_memory_cap_skips_pinned_window`（有可回收窗口时跳过 pin 窗口、
  不误报 pressure）
- `window/buffer/tests.rs`：`front_pinned_by_retention_states`（空窗/无 pin/前沿内外/
  释放 五态）

回归：`cargo test -p wf-engine -p wf-runtime --lib` → **1075 + 489 passed / 0 failed**；
q4 30M 复验 identical ✅ / evict=0（修复 B 未破坏已验证的正确性）。

### 旧方案存档：配置兵役（已回退）

曾把 `bid_events.max_window_bytes` 2GB→8GB 绕过问题（也能 identical），但 100M 会再
破。D4 落地后已改回 2GB，正确性由引擎保证。

### 附：当时的诊断现场（已失效，仅存档）

30M oracle 对拍（5% 容差，17 个非 stats 查询）：**13 个 0% 偏差**，4 个超差：

| 查询 | 偏差 | 状态 |
|------|------|------|
| q4 / q9 | **−62%** | ✅ 已修（bid 窗字节上限驱逐，见上）|
| q3 | −16% | `match<id:10m>` 滑动 + snapshot join，**根因未查**（与 evict 无关）|
| q12 | +177% | fixed+close 尾桶，known-diff |

q9 当时的诊断线索（**事后看全部能用字节上限驱逐解释**）：
- 84,798 个「候选在索引、区间过滤后为空」的假 miss → 部分驱逐：键还在索引里，
  但落在 [dateTime, expires] 内的那些行已被丢；
- 30M EOS 重试 hit=0 → 被驱逐的行本就回不来；
- ❌ `min(驱动水位, bid 窗口 max_event_time)` 无效、❌ `join_lookup` 加 log 读锁无效
  → 对的，根本不是 watermark 问题，是行已经不在窗口里了。

**教训**：偏差只在大规模出现时，先看资源限额指标（evict / RSS / cap），再去
怀疑语义与 watermark——先拿「小规模是否也错」这个判别数据，能省很多弯路。

## 2026-08-24 二次全量 30M replay（D4 保留 pin + snapshot/asof 扩展后）

`./bench.sh all replay 30m`（12:27-12:33），22/22 `[clean]`。vs OSS **3.35×~203.65×**；
vs VVR **1.91×~54.07×，20/20 达 VVR**（compare-metrics.sh 生成，已写入
`OSS_VVR_BASELINE.md` §3）。

| Q | EPS | RSS | evict | Q | EPS | RSS | evict |
|---|---|---|---|---|---|---|---|
| q1 | 19,841,059 | 4,021 | 468 | q12 | 18,257,515 | 3,718 | 468 |
| q2 | 23,187,007 | 3,562 | 468 | q13 | 412,853 | 15,399 | 110 |
| q3 | 22,455,531 | 3,798 | 468 | q14 | 11,143,558 | 9,083 | 149 |
| q4 | 4,054,046 | 8,779 | **0** | q15 | 4,462,895 | 6,675 | 13 |
| q5 | 916,732 | 6,667 | 1 | q16 | 7,534,454 | 9,399 | 73 |
| q6 | 726,120 | 7,734 | 1 | q17 | 15,157,103 | 6,566 | 468 |
| q7 | 16,195,264 | 3,774 | 468 | q18 | 14,753,935 | 15,358 | 468 |
| q8 | 23,110,382 | 3,839 | 468 | q19 | 5,934,618 | 10,762 | 446 |
| q9 | 4,632,016 | 8,595 | **0** | q20 | 4,633,270 | 9,829 | 6 |
| q10 | 6,661,395 | 4,578 | 468 | q21 | 8,613,157 | 4,587 | 468 |
| q11 | 18,709,642 | 3,596 | 468 | q22 | 7,603,148 | 6,113 | 78 |

观察：
- **deferred 三查询（q4/q8/q9）与快照 join 目标零驱逐**：q4/q9 evict=0（D4 pin 生效）；
  q8 evict=468 但全在 bid_events（其 join 目标非 bid，pin 不误伤）；q20 evict=6 在
  bid_events 驱动流（auction_events 被 pin 零驱逐）。
- **其余查询 evict=468 是常态**（bid_events 2GB 上限每次被顶爆）：流式查询有 pull ack
  保护，无害；但**说明字节上限驱逐在 30M 下普遍活跃**——若未来有查询把 bid 当 join
  目标且无 pin，会重蹈 q9 覆辙。
- q13 仍是最慢（412,853 EPS / RSS 15.4GB）：双规则链 + bid_mod 中间窗物化 + over=2d
  不驱逐。
- q5（916,732）与 q6（726,120）是另外两个低速点：q5=hop+conv top_ties，q6=sliding
  avg（特殊口径）。

## 2026-08-24 三次全量 30M replay（hop conv 分片 + review 后，最终态）

`./bench.sh all replay 30m`（13:56-14:01），22/22 `[clean]`。vs OSS **7.25×~221.21×**；
vs VVR **1.89×~58.73×，20/20 达 VVR**（compare-metrics.sh，已写入 OSS_VVR_BASELINE.md）。

| 查询 | 12:27 全量 | 本次 | 查询 | 12:27 全量 | 本次 |
|---|---|---|---|---|---|
| q1 | 19.84M | 17.97M | q12 | 18.26M | 18.27M |
| q2 | 23.19M | 22.90M | q13 | 412.9k | 409.9k |
| q3 | 22.46M | 22.43M | q14 | 11.14M | 10.86M |
| q4 | 4.05M | 4.05M | q15 | 4.46M | 4.43M |
| **q5** | **916.7k** | **6.63M（7.2×）** | q16 | 7.53M | 7.42M |
| q6 | 726.1k | 750.5k | q17 | 15.16M | 14.85M |
| q7 | 16.20M | 17.59M | q18 | 14.75M | 14.63M |
| q8 | 23.11M | 23.24M | q19 | 5.93M | 5.22M |
| q9 | 4.63M | 4.64M | q20 | 4.63M | 4.59M |
| q10 | 6.66M | 19.98M（3×） | q21 | 8.61M | 17.42M（2×） |
| q11 | 18.71M | 18.51M | q22 | 7.60M | 7.65M |

注：q10/q21 的大幅变化是 12:27 那次环境干扰（sink 写盘被并行跑拖慢，CPU 882% 却
低吞吐），本次值更可信（CPU 269%/312%）。q1/q19 −9~12% 为负载噪声。

现在全量低速点只剩：**q13（410k，双规则链 + 中间窗物化，已知成因）、q6（750k，
sliding avg 特殊口径）、q4/q15/q20/q9（4-4.6M，deferred/stats/snapshot 单核形态）**。

## 2026-08-24 q5 性能优化（hop 重叠窗 + conv top_ties，基于 bench 数据）

### 数据先行（cargo bench，非猜测）

`cargo test --release -p wf-engine hop_bench -- --ignored --nocapture`（200k 事件）：

| 指标 | 改前 | 改后 | 变化 |
|------|------|------|------|
| hop(10,2) advance+scan | 956 ns/evt（1.05M eps）| **831 ns/evt（1.20M eps）** | **−13%** |
| hop(10,10)（单窗口参照）| 266 ns/evt | 247 ns/evt | −7% |
| fixed(10) | 242 ns/evt | 220 ns/evt | −9% |
| **每窗口成本**（hop5−hop1)/4 | 172 ns/window | **146 ns/window** | **−15%** |
| conv top_ties vs top（10k 批）| **+46~55%** | **+0~2%** | 双倍 eval 消除 |

结论：hop 5× 重叠是 q5 慢的主因（956 vs fixed 242 = 3.95×）；conv top_ties 的
双倍 eval（sort 已算过 key、op 间无状态共享）是收口批的固定增量。

### 两个优化

**① `advance_window` 实例取用改 entry（`match_engine/mod.rs`，A1）**
- 原：每窗口 `contains_key` + `take_instance`(remove) + `put_instance`(insert) =
  **3 次哈希操作**（remove/insert 还破坏 HashMap 缓存局部性）；实例从不移出 map
  （全程 &mut），take/put 只是借用技巧。
- 改：`contains_key`（判 is_new，limits 检查需在 entry 前改 map）+ 一次
  `entry(instance_key.clone())`；删除 6 处 `put_instance`（early return 借用自动
  结束）。语义不变（1075 测试全过 + q5/q7 30M oracle identical）。

**② conv `sort | top_ties` 合并共享 key（`match_engine/conv.rs`，B）**
- 原：Sort 与 TopTies 各自预提取 key_rows（`apply_op` 间无状态共享）→ 双倍
  eval + 双倍 context 构建。
- 改：`apply_chain` 对相邻 `[Sort, TopTies]` 对（sort_keys 一致时）合并——
  排序重排 outputs 后把**排序后顺序**的键行传给 `top_ties_with_keys`（并列判定
  直接读，不再 eval）。非相邻/键不一致退化逐 op。`apply_op` 保持独立路径
  （含 `apply_conv_filtered`）。

### 端到端

- q5 30M：916,732 → **997,083 / 993,367 / 998,292**（三轮，+8~9%；对照同负载下
  12:40 的 830k 为 +20%）；**oracle identical ✅**（conv 语义未破）。
- q7 30M：identical ✅（fixed 窗口 + top_ties 同样受益；EPS 受机器负载干扰
  [load 6.6 vs 2.1]，14.0~14.4M vs 低负载 16.2M，待低负载复测）。
- 回归：`cargo test -p wf-engine -p wf-runtime --lib` → **1075 + 490 passed**；
  clippy 无新增（4 处 needless_borrow 已清）。

### 试错记录（A2 方向无效，已回退）

把 `advance_window` 的 `scope_key: Vec<Value>` 改 `&[Value]`（hop 循环免 5×
clone）→ 编译后发现触发路径 `MatchedContext { scope_key }`（q5 `on event` 每事件
每窗口都走）从 move 变 clone，净收益为零 → 回退。教训：hop 热路径的每窗口
固定成本（172→146 ns）里，key 传递不是大头，实例哈希账务才是。

### 遗留

- q5 仍是单核（CPU 98%）——hop 5× 窗口的**语义内成本**（权威 SQL 同样每事件
  5 窗口）还剩 146 ns/window；若要继续，方向是实例账务/StepData 分配（非 key
  传递）。
- q5 端到端 998k EPS 仍低于单窗口参照（fixed 4.55M eps 的 1/5 = 910k 的 1.1×）
  ——剩余差距来自 scan_expired 收口 + emit 序列化 + pull 循环。

### hop conv 分片（P2c 延伸，2026-08-24）——q5 单核 → 多核

**为什么 q5 单核**：conv top_ties 是**窗口级聚合**（每个 slide 桶内跨所有 auction
取最高 count），按键分片会破坏语义（片内 top ≠ 全局 top）。分片合并机制（conv
stage + barrier）只对 fixed 窗口生成（`compiler/mod.rs` 原 `WindowSpec::Fixed =>
Some(conv_window)`，hop/sliding/session → None → inline → 单实例）。

**改动**（fixed 机制复用，hop 只差桶对齐）：
- `wf-lang/src/plan.rs`：`ConvWindowPlan` + `slide: Option<Duration>`——
  `None`（fixed）桶对齐 = over；`Some(slide)`（hop）桶对齐 = slide、封口长度
  = over = size（hop 实例在 window_start + size 收口，收口事件 window_start =
  k*slide）。
- `compiler/mod.rs`：hop 分支生成 conv_window（`over: size, slide: Some(slide)`）。
- `conv_stage.rs`：`ConvStageConfig` + `bucket_align`；`on_batch` 桶键 = window_start
  floor 到 bucket_align；封口 `seal_candidates` 仍用 `over`（= size）。
- `spawn.rs`：stage 配置 `bucket_align = cw.slide.unwrap_or(cw.over)`；
  `shardable` 判定自动生效（conv_window Some + keys + shard_count>1）。
- 分片正确性：每片算 auction 子集的 hop 计数 → raw close（带 window_start）
  路由 conv stage → 按 slide 分桶 → barrier（每片 watermark）等齐 → 桶封口时
  全局 apply_conv。语义与单实例完全一致。

**端到端（30M，load 6.6 下）**：

| | 改前（inline 单核）| 改后（分片）|
|---|---|---|
| q5 EPS | 998,292 | **6,479,375（6.5×）** |
| q5 CPU | 98%（单核）| **602%**（10 分片）|
| q5 RSS | 6,667MB | 5,368MB |
| q5 对拍 | identical ✅ | **identical ✅** |
| q7（fixed 回归）| — | identical ✅（13.1M @ load 7.1）|

**新增测试**：`conv_stage_hop_bucket_aligns_to_slide_seals_by_size`（判别性：
window_start 6s/16s + barrier 20s → 只封 6s 桶；若误用 over 对齐会错封 2 桶）。
回归：`cargo test -p wf-lang -p wf-engine -p wf-runtime --lib` → **929 + 1075 +
491 passed**；clippy 无新增。

**遗留**：sliding/session conv 仍 inline（无固定对齐边界，跨片合并时序更复杂）；
q5 6.5M EPS 是在 load 6.6 下测得（低负载可能更高）；evict=291（bid_events 是
q5 的**源**，pull ack 已保护，无害）。

### 2026-08-24 review 补测（hop conv 分片）

- **编译器单测 +3**：`hop_conv_rule_generates_conv_window`（over=size、
  slide=Some(slide)）、`sliding_conv_rule_rejected_by_checker`（sliding+conv 被
  checker 拒绝，非「无 conv_window」——修掉了最初「sliding conv 编译成功但无
  conv_window」的错误测试前提）、fixed 断言加 `slide == None`。
- **conv stage 跨分片集成 +1**：`conv_stage_hop_shards_aggregate_globally_top_ties`
  ——两分片各自收口同桶 close（count=5 vs 9）→ barrier 等齐 → 全局 top_ties
  必须取 count=9（片内 top 会错选 5）；另含桶 16s 未封断言。
- **文档一致性**：conv_stage 头部设计注释、spawn shardable 注释、seal 注释从
  「fixed-window」扩到 fixed/hop（over = 封口长度、bucket_align = 桶对齐）。
- **排查确认**：hop `slide≤size` 由 parser 保证（`size % slide == 0`，L223）→
  无除零、无非法对齐；`ConvWindowPlan` 构造点全 workspace 仅 compiler（fixed/hop
  两处）+ 测试一处，无遗漏；MoJu derive 自动适配新字段。
- 回归：`cargo test -p wf-lang -p wf-engine -p wf-runtime --lib` → **931 + 1075 +
  492 passed**；clippy 无新增（只剩既有 too_many_arguments）。

## 2026-08-24 四次全量 30M replay（merge 后 + q4/q9 deferred 分片，哨兵 EPS 口径）

`./bench.sh all replay 30m`（14:44-14:50），22/22 `[clean]`，**eps_mode=sentinel**（哨兵
四元组口径，剔 ingest 等待；与前三轮 metrics-append 口径不可逐位比较）。
vs OSS **7.41×~211.87×**；vs VVR **1.90×~56.25×，20/20 达 VVR**。

| 查询 | 13:56 全量 | 本次 | 查询 | 13:56 全量 | 本次 |
|---|---|---|---|---|---|
| q1 | 17.97M | 20.61M | q12 | 18.27M | 18.45M |
| q2 | 22.90M | 24.94M | q13 | 409.9k | 392.2k |
| q3 | 22.43M | 25.87M | q14 | 10.86M | 11.42M |
| **q4** | **4.05M** | **7.87M（分片 1.9×）** | q15 | 4.43M | 4.44M |
| q5 | 6.63M | 6.89M | q16 | 7.42M | 7.44M |
| q6 | 750.5k | 742.6k | q17 | 14.85M | 15.93M |
| q7 | 17.59M | 16.85M | q18 | 14.63M | 15.67M |
| q8 | 23.24M | 25.95M | q19 | 5.22M | 5.54M |
| **q9** | **4.64M** | **7.84M（分片 1.7×）** | q20 | 4.59M | 4.70M |
| q10 | 19.98M | 20.70M | q21 | 17.42M | 22.25M |
| q11 | 18.51M | 19.22M | q22 | 7.65M | 7.81M |

注：q4/q9 的提升是 deferred 分片（本轮真实增量）；其余差异主要是哨兵口径 + 负载
噪声（q7 −4% 等）。低速点只剩：q13（392k，中间窗物化）、q6（743k，sliding 特殊
口径）、q15（4.44M stats 单桶）、q20（4.70M snapshot 展开）。

## 2026-08-24 q4/q9 deferred 分片（spawn 放宽 + EOS 全局尾部修复）

### 为什么 q4 单核

`spawn.rs` Each 分支原 `!deferred` 排除分片（「挂起队列 per-task 状态，设计 §9
风险 5」）。查证：风险 5 的分片互斥实为 **join-then-key** 场景（路由键来自 join 侧，
键在路由前不可得，§7.5「根本性不兼容」）——q4a/q8/q9 的路由键都在**驱动事件上**
（a.id / p.id），deferred join 不参与路由，整批轮转分片安全：

- 每 worker 独立挂起队列/watermark；驱动事件**整批分配**（每个 auction 恰一个驱动
  事件 → 同 worker，pending 无跨 worker 依赖）；join 目标窗口共享（并发 lookup）。
- 到期输出跨 worker 乱序：仅当下游**无 Match 状态机**（stats/on-each 可交换聚合）
  时允许。q4a 下游 q4b 是 stats avg（可交换）✓；q9/q8 直接 sink ✓。

### 改动

- `spawn.rs`：预计算 `match_consumed_targets`（被 Match 消费的 intermediate
  target，保序要求）；Each 分片条件放宽：`deferred_shardable = deferred &&
  match_plan.key_join.is_none() && !match_consumed_targets.contains(&target)`；
  `shardable = shard_count>1 && !consumes_intermediate && (!intermediate_targets
  .contains(&target) || deferred_shardable) && (!deferred || deferred_shardable)`。
- `rule_task.rs` flush（EOS）：final_wm 从「任务自身 watermark」改为「**驱动窗口
  全局 max_event_time**」——分片后 worker 自身 watermark 停在最后批次，尾部
  expiry ≤ 数据末尾的 pending 永不评估（**实测 q4 30M 丢 869 条**）。不能退
  i64::MAX（Q8 实证 +828 尾部桶，oracle 事件时间水位语义）；窗口 max_event_time
  = true global data tail，与单 worker 最终 watermark 同语义。

### 端到端（30M，load 3-5）

| 查询 | 分片前 | 分片后 | CPU | 对拍 |
|------|--------|--------|-----|------|
| q4 | 4,046,037 | **7,410,364（1.8×）** | 369% | **identical ✅** |
| q9 | 4,643,183 | **7,466,754（1.6×）** | 372% | **identical ✅** |
| q8 | 23,110,382 | 18,045,535（person 流少，ingest 主导，噪声）| 51% | **identical ✅** |

### 新增测试

- `deferred_flush_uses_global_window_tail_for_sharded_workers`（deferred_integration_
  tests.rs）：任务处理早期批次（watermark=T）→ 驱动窗口 append 更晚批次但不处理
  （模拟其他 worker）→ flush 必须用窗口全局尾部评估尾部 pending（修复前漏）。
  注：窗口需 `append_with_watermark`（普通 append 不更新 max_event_time）。
- 回归：`cargo test -p wf-lang -p wf-engine -p wf-runtime --lib` → **931 + 1075 +
  493 passed**；clippy 无新增。

## ✅ q20 snapshot join 性能（2026-08-24，Arc<JoinRow> 消除每行 clone/drop）

**目标**：q20（`on each b` + snapshot join auction + `where category==10`）30M 4.70M EPS
（全量倒数第 4，snapshot 展开路径）。数据驱动：先函数级 profile，再改。

### 函数级归因（macOS `sample` 采样 + 段级计时，非猜测）

- each_exec 每段（4096 行）总耗时 7.5ms：join 3.15ms（key 0.14 + lookup 0.72 +
  fill 1.78）+ out 4.34ms（recheck 1.86 + where 0.17 + meta/yield/commit ~0.25 +
  **未捕获 2.1ms**）。rule_task exec 80ms/批 ≈ 8.8 段 × 7.3ms + 16ms 锁开销，账闭合。
- `sample` 调用树：`execute_each_direct_batch_columnar_join` 占 process_batch
  93%；其中 **JoinRow drop_glue 1791/4477 采样（40%）**——fill 每行 `first.clone()`
  （JoinRow::Columnar = 4 个 Arc bump）+ recheck 每行 `row_match[idx].clone()`
  （再 4 个）+ 行尾 drop；共享批 Arc 跨线程原子争用（q20 CPU 571%）。
- 未捕获 2.1ms = drop 成本（计时器外的 JoinRow/Value drop，行尾 + 函数尾）。

### 修复（`1b2f657`）

1. `row_match: Vec<Option<Arc<JoinRow>>>`：每桶只搬移一次首行
   （`into_iter().next()` 零 bump），每行仅 1 次 Arc clone（原 4 个 Arc bump）。
2. recheck 命中行 `as_ref()` 零克隆；miss 行结果暂存 `miss_hold`（仅 miss 行
   承担 lookup 成本）。
3. fill 浮点/非浮点分支拆分（q20 非浮点走共享 Arc 路径）。

### 实测（30M replay，哨兵 EPS）

- **q20: 4.62M → 20.87M EPS（4.5×）**；RSS 10.0GB → 4.5GB（−55%）；CPU 618% → 260%
  （原子争用消除）。
- 段级：fill 1785µs→19µs（94×）、recheck 1862µs→296µs、每段 7512µs→1656µs。
- 正确性：q13 全部一致 ✅（同路径静态 provider join 不受影响）；**q20 verify
  偏差 0.97~1.65%（<5% 容差）**。

### q20 偏差机制（已用计数器钉死，非逻辑 bug）

fill_hit/recheck_rescue 计数器：原始 fill_hit=24.27M、recheck=0；修复后
fill_hit=23.87M、recheck=104k。差异不在代码逻辑（两版提取同一 bucket.first()），
而是原设计「批快照 + 行时复查」的**固有竞态**：join 目标窗口（非 source）按
`eff_max_seq=None` 读**全量已提交状态**；原始 fill 循环的 clone 工作（1.78ms/段）
穿插在 join_lookup 之间，窗口在期间持续前进，后面的 key 看到更晚快照；加速后
所有 lookup 挤在窗口前进前完成 → 少命中，recheck（更晚时点）只补回部分。
方向恒为**少发**（更接近真实流序——少看到“未来 auction”的过度匹配），原始
精确一致是慢速 clone 恰做“节奏延迟”的巧合。**待办（可选）**：确定性快照边界
（跨窗口 seq-cut 映射）才能彻底钉死，工程中等，暂不做。

## ✅ q15 空键 stats 输入分区分片 + EOS 归并（2026-08-24）

**目标**：q15（`stats<1d:fixed>` 空键 12 度量, 4.44M EPS 全量倒数第 4）单核封顶。

### 归因（数据驱动, WF_Q15_DIAG 段级计时）

- 每批 36,500 行 → seg 8.94ms（245ns/row）, **accumulate（process_batch_rows
  列式归并）占 98%**——8 个 distinct_count 每行 4~8 次 HashSet insert 是全部
  成本; 单核（CPU 76%）不可再优化 → 必须多核。
- 第一步（已提交 `304094e`）：distinct_set 从 std SipHash 换 foldhash
  （EngineHashSet）→ 245→185ns/row, **4.22M→5.53M（+31%）**, verify ✅。

### 输入分区分片（`2d5b982`）

空键 + 度量全可交换（count/sum/min/max/distinct; last/top 行序敏感门控排除）
+ pull 模式 + shard_count>1 → 按行号 `row % N` 均匀切分 N 任务, close 时归并：
- fanout：`partition_rows_by_index` + `register_window_index_sharding`
  （空键 = index 分区标记; precompute_shard_rows 分支）
- stats_exec：`take_partial`（raw 桶状态 + 重置）+ `merge_partial`
  （count 加 / sum 加 / min·max 极值 / distinct 集 union）
- stats_task：非协调片 close 发 raw partial 不 emit（空窗发空 partial 防死
  锁）; 协调片（shard 0）收齐 N-1 归并后统一 emit; flush 同构

### 实测（30M replay, 哨兵 EPS）

- **q15: 5.53M → 7.75M（+40%）, CPU 72%→563%（多核）, RSS 6.6GB 无爆炸**;
  verify **全部一致 ✅**（归并精确——count 相加 + distinct 集 union 精确）
- 测试：executor 级 `stats_input_shard_merge_matches_single`（两片归并 ==
  单实例逐值一致）+ 任务级 `q15_input_shard_merge_emits_single_equivalent`
  （2 片 + 协调片, 输出字节一致, 非协调片不 emit）
- 全量 1079 + 525 测试过; clippy 无新增

### 已知瓶颈（未解决）

协调片 EOS 归并 ~883ms 串行（9 片 × 8 distinct 集 union = 68M 次 insert）+ 每
片对**全批**做 where mask/domain 的 10× 冗余。方向：归并按度量并行（8 度量
独立 union）、mask 单算共享或按片行域裁剪。

### 行域裁剪（`6b394e9`, 2026-08-24）——10× 冗余消除

段 1d/段 2 从「全批 domain mask + 整列归并」改为**行域驱动**：
`domain_rows`/`count_domain`/`sum_domain`/`minmax_domain`/`insert_distinct_domain`
只遍历本片行（`process_batch_rows` 传 `rows: Option<&[u32]>`），不再构建全批
domain mask。删旧辅助：`count_true`/`domain_mask`/`combine_masks`/
`int_values`/`float_values`/`sum_masked`/`minmax_masked`/`insert_distinct_column`/
`str_values`/`bool_values`/`ts_values`。

**实测**：q15 **7.86M EPS**（基线 7.75M）, CPU **563%→403%**（每片 CPU 降, 墙钟
略升——墙钟瓶颈是串行归并, 行域裁剪是净收益但降的是 CPU 不是墙钟）。

**并行归并试错（已回退）**：`thread::scope` 并行 distinct union（`split_all` 拆
`&mut` + 每度量一线程）实测 **5.96~7.86M 波动（比串行 7.86M 差）**——阻塞 tokio
worker 与其余片 ingest 争核。已恢复串行 `merge_accum`（含 distinct union）, 删除
`merge_accum_arith`/`split_all` 死代码。**教训：不要在 async close 里用
`thread::scope` 并行；要并行须 `spawn_blocking`/异步任务（数据移出 `&mut self`）。**

测试：`cargo test -p wf-engine --lib stats` 58 pass + `cargo test -p wf-runtime
--lib q15_input_shard` 4 pass（行域裁剪语义与全批一致）。

### 归并性能基准（`b7face9`, 2026-08-24）——微优化无空间, 瓶颈是星形归并本身

新增 bench：`close_bench::q15_merge_partial_profile`（模拟生产归并：空键 12
度量、8 distinct 集、协调片 + 8 partial、域 8M 高度重叠）。运行：
`cargo test --release -p wf-engine --lib q15_merge_partial -- --ignored --nocapture`

**结论 1：merge 微优化全部无空间（±5% 噪声内）**——1M/集规模：
cur(生产 clone+extend) 2154ms vs move 免 clone 2142 / move+reserve 2168 /
move+小并大 2137。原因：foldhash `extend` 对 `iter().cloned()` 的 ExactSize
Iterator 内部已 reserve；`DistinctKey::Int` 是 memcpy 级拷贝, clone 近免费。
**不要改 merge_accum 的 micro 结构**。

**结论 2：分片数敏感度揭示真瓶颈**（域 8M 固定, 每片 distinct ≈ 域/N）：
分片 4 → 1518ms；分片 9 → 1979ms；分片 16 → 2296ms。星形归并总 insert ≈
(N-1) 片 × 8 度量 × 域/N ≈ 常数——**墙钟不随分片数下降**（4→16 反而 +51%）。
这就是生产 ~883ms 尾部的本质：不是单点可微的, 是协调片串行归并的固定成本。

**推论（与 thread::scope 回退一致）**：要让归并变快只能**改归并形态**——
树形归并（两两并行 union）或 `spawn_blocking` 异步化（数据移出 `&mut self`,
工程大）。当前 7.86M 是分片 + 行域裁剪后的合理收口, 归并尾部 ~0.9s 暂接受。

## 下一步（重开 session 从这里继续）

1. **全量 22 查询 30M 重跑 + OSS/VVR 对照刷新**（q4/q5/q9/q20/q15 均已大幅
   变化; q15 现 7.86M）
2. （可选）q15 归并异步化（墙钟瓶颈 ~883ms 串行归并, 需 spawn_blocking/异步
   任务, 工程量大）; q3 −16% 独立 bug
3. D4 剩余：eager interval join 的 pin; 「结果可能不完整」的正确性信号/指标
4. （可选）q20 确定性快照边界; q13 列式 join 免 Event 物化
5. 旁支发现：`match_engine/tests/executor/direct_tests.rs:412` 编译告警 never
   used——疑似漏 `#[test]`, 测试没在跑。

## ✅ 静态表 anti 放开（`f79f9e7`, 2026-08-24）——白名单排除是合理需求

**背景**：warp-fusion examples 的 port_scan_whitelist/ssh_brute_force 用
`join scanner_whitelist anti`（静态表白名单排除）被 checker 拒绝（P4 限制：
静态表仅 snapshot/inner）。用户质疑「anti 需要时序语义、对无时序静态表无意义
是不合理需求」——质疑成立：

- 设计文档 §2.1 表格 anti 时间谓词是「—」（无）——anti 与时间正交
- 引擎 context.rs anti = `join_lookup` → 有匹配丢、无匹配留，**零时间依赖**
- provider `join_lookup` 已有 O(1) 行索引（window_lookup.rs，q13 修复）
- NEXMark Q21 就是 anti（黑名单过滤）——静态表白名单排除是标准用例
- checker 注释「至今未匹配」是把 anti 与 interval/asof 混为一谈（后者才需要
  时间列/窗口生命周期）

**改动**：`check_static_window_join` 允许 `JoinMode::Anti`（仍拒绝 asof/within/
reduce/deferred）；provider_joins.rs 的 anti 测试改「通过」断言；coverage_extra.rs
同改；provider_join_integration_tests.rs 新增 `provider_anti_drops_whitelisted_
keeps_others`（端到端：命中静态表丢弃、未命中保留）；join-family-design.md
§14.5 更新。warp-fusion 两个示例规则恢复 anti（port_scan_whitelist 数据补
5 行让非白名单 IP 达标）。

**验证**：wf-lang 957 pass；wf-runtime 529 pass；warp-fusion run_all.sh
24 pass / 1 skip（Redis）/ 0 fail。

**顺带**：修 warp-fusion examples 一批测试失败（`ed519dc`）——两因：①测试
输入字符串字面量 vs digit 字段数值 guard（`679ae40` 后 bind filter 真实评估
暴露），②静态表 anti 被拒。根因 ① 的修复（字符串→数字）已含在该提交。

## 测试补充（2026-08-24，随 q20 修复提交）

单元测试（direct_tests.rs，+3）：
- `columnar_join_hot_key_rows_match_event_path`：热键多行同桶（Arc 共享首行），
  且桶首行（而非桶内其它行）决定富化——列式 vs 行式字节一致
- `columnar_join_recheck_rescues_mid_batch_append`：GrowJoinLookup（每 key 首次
  lookup 空桶 → 批级 miss，后续非空 → 行时 recheck 救援）——救援后输出与
  「快照即命中」静态 mock 字节一致（钉死 `miss_hold` 新路径）
- `columnar_join_float_hot_key_matches_event_path`：float 左键热键同桶
  （f64→Int 截断 + values_equal 复核）列式 vs 行式字节一致

性能单元测试（each_bench.rs，+1，`cargo test --release -p wf-engine
q20_columnar_join_per_row -- --ignored --nocapture`）：
- 4096 行/段（生产 ALERT_BATCH_SIZE）+ 真实 join 索引窗口（1M auction 行）
- unique 键 228ns/row（4.4M/s）vs 热键混合 122ns/row（8.3M/s，去重+单 Arc
  共享 1.9×）vs 行式参考 276ns/row（3.6M/s，列式 2.3×）
- 热键 122ns/row 与此前 stats 微基准 122ns/evt 吻合；端到端剩余成本在
  rule_task 层（pull/遥测/锁/commit），不在 executor

## 2026-08-25 q4 100M 性能断崖修复（join_index 分片 + 增量驱逐）— 本 session

### 任务与基线

用户要求「100M 跑 Q4，性能特别差则分析定位解决」。q4 双规则链（q4a deferred
join + q4b stats）。基线：30M 7.6M EPS；**100M 0.27M（28× 断崖）**，RSS 22GB，
CPU 105%（单核）。

### 第一层根因：join_index 单锁写者阻塞全部读者

30s macOS `sample`：21969/21969 全在 `join_lookup_timestamped →
lock_shared_slow`。写锁持有者 = bid append 的 `index_batch`（每批 36.5k 行
哈希插入）。

**数据先行**（deferred_bench `index_contention`，release）：1 写者批量插入 +
4 读者随机查找——单锁在写者活跃时读者吞吐塌到天花板 2%（0.15M vs 7.98M
ops/s）；64 分片恢复 6.9M（43–46×，达无写者天花板 ~86%）。

**修复①**：`JoinIndex` 拆 64 片独立 parking_lot RwLock（`shards: Vec<PLRwLock<
EngineHashMap>>` + `mask`，`DefaultHasher & mask` 选片）。`index_batch` 先按片
分组再逐片短暂持写锁；查找只锁一片。外层 `join_index` 锁改读锁（设置后不变）。
实测 100M：**0.27M → 0.378M（+40%）**——不够，断崖未根治。

### 第二层根因（真正主因）：时间驱逐 × 全索引扫描

再采样：21870/21870 在 `evictor → evict_expired_impl → remove_batch_from_index`。
根因链：

- `gen-nexmark` 事件跨度 = count × 100µs → **30M = 50min（< over=1h，零驱逐）**；
  **100M = 2h46m（> over=1h）** → bid 窗时间驱逐**合法触发**（over 是语义边界，
  D4 pin 不覆盖时间驱逐）；
- 旧 `remove_batch` 每驱逐一批就**全索引扫描**（全片 retain + max_ts 重算，
  O(全行数)）——100M 时 33M 行 × 数千批 → evictor 线程独占一核。metrics 实锤：
  `time_evicted_total` 555+ 且持续增长。

**数据先行**（deferred_bench `remove_batch`，release）：32.3M 行/170 万键量级
全量 17.9ms/批 vs 增量 0.26ms/批 → **70×**。

**修复②（增量驱逐）**：`JoinIndex` + `batch_keys: RwLock<HashMap<u64 seq,
Vec<JoinKey>>>`（index_batch 注册去重 key 集；空批也注册空条目区分「没索引
行」vs「registry 缺失」）。`remove_batch(seq)` 只动该批贡献的 key（O(受影响
key × 行数)），max_ts 仅对受影响 key 重算；registry 缺失回退全量扫（防御）。
内存 ≈ 窗内去重键数（100M 2GB cap ≈ 1.7M 键 × 24B ≈ 40MB）。

### 实测（哨兵 EPS）

| 规模 | 修前 | 修后 |
|------|------|------|
| 100M q4 | 0.27M（RSS 22GB） | **11.26M（42×，RSS 19.8GB）** |
| 30M q4 | 7.6M | **12.9M（1.7×）** |
| 30M q4a EMIT | — | 1,672,559（= 历史 oracle 精确值 ✅） |
| 100M q4a EMIT | 2,573,772（SIGKILL 截断虚高） | 2,067,324（干净关停真值） |

SUMMARY clean；100M oracle 对拍（verify-nexmark）运行中（~10-30min）。

### 测试与基准（全部通过）

- wf-engine 1113 / wf-runtime 530 pass
- 单测 +4：`join_index_shards_spread_and_deterministic`、
  `join_index_sharded_lookup_evict_and_asof_span_all_shards`、
  `join_index_incremental_remove_only_touches_the_evicted_batch`、
  `join_index_incremental_remove_recomputes_max_ts`
- bench +2（deferred_bench）：`index_contention`（单锁 vs 分片 43–46×）、
  `remove_batch`（全量 vs 增量 70×）

### 未提交文件（wp-reactor main）

- `M crates/wf-engine/Cargo.toml`（+parking_lot 0.12）
- `M crates/wf-engine/src/window/buffer/mod.rs`（分片 + batch_keys 增量驱逐）
- `M crates/wf-engine/src/window/buffer/tests.rs`（+4 测试）
- `M crates/wf-engine/src/match_engine/tests/deferred_bench.rs`（+2 bench）
- `M crates/wf-runtime/src/engine_task/rule_task.rs`（scan_deferred 前缀 +
  lo_min 缓存，上一 session 遗留，已测）
- `M crates/wf-runtime/src/engine_task/deferred_integration_tests.rs`（乱序
  到期集成测试，上一 session 遗留）

### Pitfalls（勿重蹈）

- **瞬间/短 sample 会抓错热点**：第一次 30s 采样是读锁（index_batch 写锁），
  分片后又采样才暴露 remove_batch——100M 断崖是**两层**叠加，第一层修复只
  揭开第二层。
- **bench_qN_replay.txt 会被下一轮覆盖**：100M 结果记得先存档（本 session
  的 100M 行在 /tmp/q4_100m_run3.log，已 cp 到 /tmp/bench_q4_100m_fixed.txt）。
- **SIGKILL 截断 EMIT 虚高**：SIGTERM 超时强杀会丢尾部 flush 计数（bench.sh
  注释有记载），对比 EMIT 必须用干净关停的 run。
- **事件跨度随 count 缩放**（count×100µs）：30M=50min 无驱逐 vs 100M=2h46m
  驱逐——「30M 不驱逐所以 100M 也不驱逐」的假设在规模翻倍后失效。
- 已验证有效的判别手段：evictor metrics（`time_evicted_total`）+ macOS
  `sample` 完整调用栈 + 先写隔离微基准拿 A/B 数据再改生产代码。

## 下一步（重开 session 从这里继续）

1. **确认 100M q4 oracle 对拍结果**（`/tmp/q4_verify_100m.log`，verify-nexmark
   100M --query q4）——clean 即可 commit 本 session 改动
2. commit（用户之前每次 session 都 commit；本次改动 = 上述 6 文件）
3. **全量 22 查询 30M 重跑**：q4 已 12.9M（30M 基线文档数字过时）；
   `OSS_VVR_BASELINE.md` / `CAPABILITY_GAP_MATRIX.md` 待刷新（用户 2026-08-24
   的待办仍在）
4. （可选）q15 归并异步化；q3 −16% 独立 bug（与驱逐无关，已双证）
5. 注意：其他把 bid 当 join 目标的查询（q9 30M 现 7.84M）在更大规模下同样会
   触发时间驱逐——增量驱逐已一并修好，但 q9 100M 值得复测（预期不再断崖）

## 2026-08-25 review 追加：代码修复 + 新发现（100M q4a 语义欠发，独立于本性能修复）

### review 修复（数据驱动，已测）

1. **index_batch 去重 HashSet 换 foldhash**（std SipHash → EngineHashSet）：
   join_index_append_bench 实测 index_batch **116.7 → 88.6 ns/row（−24%）**；
   100M q4 EPS 11.26M → **12.98M**。SipHash 在 append 热路径每批 36.5k 行
   都过 set，是隐藏的常数项。
2. **publish_retention_floor 缓存分支对齐全量语义**：cached 分支从
   `lo_min.min(watermark)` 改为「空集→watermark / 非空→lo_min」（与重扫
   分支 `min(lo_ns).unwrap_or(watermark)` 逐分支一致）；生产路径行为等价
   （lo_min ≤ 真实 min，恒保守方向），消除两分支语义漂移。
3. 注释修正（remove_batch 回退路径「按 Arc 指针匹配」→ 实际按 seq）+ 交接
   bench 的 unused-mut 清理。

### review 新增测试（wf-engine 1113→1115）

- `join_index_remove_batch_fallback_when_registry_missing`：直接构造 JoinIndex
  注入 registry 缺失态——未注册 seq 走全量回退（no-op 不 panic）、已注册 seq
  增量删除、registry 条目随驱逐清理。
- `join_index_concurrent_append_evict_lookup_no_deadlock`：写者 + 驱逐者 +
  读者三线程交错（锁序：index_batch 先片锁后 registry、remove_batch 先
  registry 后片锁，均不跨锁等另一把）——钉死分片 + registry 锁序。

### ⚠️ 重大发现：100M q4a 语义欠发（预先存在，非本次性能修复引入）

**oracle 对拍**（`wfgen verify-nexmark 100M --query q4`，47min）：
- oracle q4a_auction_finals = **5,576,436**（真实 WFL 规则引擎、预加载完整窗）
- 引擎 100M EMIT = **2,067,324**（≈ oracle × (1h/2h46m)）——**欠发 ~63%**
- 修前 broken run（0.378M EPS）= 2,573,772 → **同量级，证明与本次性能修复无关**
- 30M 引擎 = oracle = 1,672,559 精确一致（span 50min < over 1h，零驱逐）

**根因**（机制推演 + 数据吻合）：auction 有效期只有 **1-333ms**（官方
`1+[0,2×horizon)`，horizon=1666×100µs）。100M 事件跨度 2h46m > over=1h →
bid 窗时间驱逐合法触发。**ingest 不被驱动任务流控**：TCP 摄入全速推进、bid
窗按摄入位置驱逐 >1h 的 bid，而 q4a 驱动任务（6M auction × eval）滞后 →
等它评估早期 auction 时，其 [dateTime, expires] 内的 bid 已被时间驱逐 → miss。
只有最后 1h 内到期的 auction 命中（2.07M ≈ 5.58M × 1h/2h46m）。

**性质**：deferred 评估延迟（Δ_event = 摄入位置 − 评估位置）超过 over 保留期
时丢行。30M 不触发（span < over）。**这不是 join 索引/驱逐实现的 bug，是
「驱动任务滞后 × over 时间驱逐」的语义缺口**——oracle 预加载全窗无此限制。

**候选修复方向（未做，工程量大，需用户决策）**：
- A. D4 pin 扩展到**时间驱逐**：pin = min(pending lo_ns) → bid 窗保留评估
  前沿内的全部行 → 全局内存 cap 触顶 → 现有 memory_pressure 背压停摄入 →
  驱动任务追平 → 前沿推进 → 驱逐恢复（自调节）。代价：RSS 上界变大、语义
  从「over 声明保留」变为「评估前沿保留」。
- B. 摄入流控：TCP 源按驱动窗口 min_acked 暂停（现只有哨兵完成信号，无
  运行中流控）——大改。
- C. 更快评估（10 分片已上，eval 本身 1.4µs 不是瓶颈；瓶颈是评估吞吐 vs
  摄入速度的相对滞后）。

**建议**：100M 是性能量级、30M 是正确性量级（用户既定口径，30M 精确一致）。
此项记入待办，由用户决定是否投入。`--verify` 时注意：先跑 bench 再立刻
verify（verify 读 data/metrics.ndjson，中间跑别的 query 会覆盖）。

## 下一步（重开 session 从这里继续）

1. **commit 本 session 全部改动**（wp-reactor main，6 文件：
   buffer/mod.rs、buffer/tests.rs、deferred_bench.rs、Cargo.toml、
   rule_task.rs、deferred_integration_tests.rs）
2. 100M q4a 语义欠发（上节）——用户决策是否修复（方向 A/B/C）
3. 全量 22 查询 30M 重跑 + OSS/VVR 基线刷新（q4 现 12.9M；文档数字过时）
4. q15 归并异步化（可选）；q3 −16% 独立 bug（与驱逐无关）

## ✅ 100M q4a 欠发根治：运行期评估 gate（2026-08-25，本 session 落地）——已 commit

用户定调：「q4a 评估跟上摄入（或摄入对驱动任务背压）——100M 的 RSS 会从
20.4GB 显著回落，逼近 30M 量级；这也是欠发（评估滞后 × over 驱逐）的同一根源」。
选了「评估跟上摄入」方向（B 摄入流控是大改，未做）。

### 根因（探针实锤，非猜测）

- 运行期 hit 率只有 ~50%（流尾 71%），miss 里 cand0（键无候选）占 34-66%
  （流尾 0.1%）——**join 目标窗口（bid）append 滞后**：驱动 watermark 只反映
  auction 流处理到哪，bid 窗由同一输入另行 append，管道滞后下到期评估时右行
  还没落地 → 运行期 miss → 全部进 missed（RSS 随总量增长）。
- 30M 靠 EOS 重试掩护（窗完整 → 全命中 → oracle 精确）；100M 事件跨度 2h46m
  > over=1h → EOS 时早段 bid 已被时间驱逐 → 重试只命中最后 1h → 欠发 ~63%。
- 一度怀疑 `row_matches_conds` 系统性失败（[matched-probe] 未跑就被新方向
  取代）——实际是候选根本没到（cand0 为主）。

### 改动（`rule_task.rs` scan_deferred）

评估前沿 = `min(驱动 watermark, join 目标窗口 raw max_event_time)`：
`expiry > 目标 append 位置` 的实例**保持挂起**，目标追平后随下一批次扫描评估。
语义保证：目标 max_event_time ≥ expiry 时，区间 [lo,hi] 内右行全部在场且新鲜
（刚 append，驱逐够不着）→ 运行期命中、missed 不积压、EMIT 正确。

- `scan_deferred(wm, emit, gate_on_target)`：批次尾/scan_timeouts 传 true；
  flush 收口传 false（数据已全量 ingest，尾部 pending 不能被 gate 掉；miss
  由 EOS 重试兜底）。
- 目标窗口不存在/未 append（max_event_time=i64::MIN）→ 不 gate（防御：保持
  旧行为，flush 收口）。
- 无需改驱逐/pin：评估在目标追平后立刻发生，需要的右行永远新鲜，时间驱逐
  与内存驱逐都够不着（pin 仍由 pending lo 推进）。

### 实测（哨兵 EPS，30M/100M 帧，本地 wp-reactor）

| 指标 | 修前 | 修后 |
|------|------|------|
| 30M q4a EMIT | 1,672,559 | **1,672,559（= oracle 精确）** |
| 100M q4a EMIT | 2,067,324（欠发 63%） | **5,576,436（= oracle 精确）** |
| 100M q4 RSS | 20.4GB | 17.6GB |
| 100M EPS | 12.98M | 10.5-10.7M（略降，评估门控开销 + 噪声） |
| 30M EPS | 12.98M | 10.5M（同上） |

运行期队列规模（临时探针，已移除）：pending ≈ 100/shard（批次中瞬时 15k），
missed ≈ 45-52k/shard ≈ 总数 8.7% = **真无 bid miss 率**（oracle 口径 7%）
——不再随数据量膨胀（修前 ~50% 假 miss 积压）。

RSS 构成（footprint）：bid 窗 6.8GB（over=1h 语义保留，33.1M 行）+ join 索引
~2GB + auction 窗 1.15GB + person 0.18 + auction_finals 0.2 + parse buffer 2GB
+ 引擎基线 ≈ 17GB。**增长随总量的部分（missed 积压）已消除**；剩余 gap vs
30M（9.2GB）是窗口保留量差异（100M 1h bid = 33M 行 vs 30M 全量 27.6M）+ 固定
开销，非无界项。

### 测试

- 既有 deferred 集成测试改用 `append_with_watermark`（测试窗的 max_event_time
  此前不推进；生产走 `commit_appended_batch` 的 watermark append）+ 目标追平
  append（生产流 bid/auction 交错，目标天然追平）。
- 新增回归 `deferred_q9_target_lag_holds_evaluation_until_target_catches_up`：
  目标未追平 → 实例保持挂起不评估；目标追平 + 后续驱动事件触发扫描 → 运行期
  命中输出（无需 flush/EOS 重试）；flush 不重复。
- wf-engine 1115 / wf-runtime 531（+1）/ clippy 0 全过。

### 遗留

- 真 miss（无 bid auction ~8.7%）仍驻留 missed 至 EOS（~0.5GB）——语义所需
  （窗口完整确认后判定），可后续优化（missed 只存 lo/hi/key 而非全 Event）。
- 100M EPS 10.5M vs 30M 12.9M：评估 gate 每次 scan 多一次目标窗 max_event_time
  读取（原子 load，可忽略）；EPS 下降更多是运行噪声/load 差异，未单独归因。
- q9（同款 bid 目标 deferred）同享此修复，100M 值得复测。
- 全量 22 查询 30M 重跑 + OSS/VVR 基线刷新仍待办（q4 数字：30M 12.9M → 10.5M）。

## 下一步（重开 session 从这里继续）

1. （已 commit 本 session：性能修复 + 欠发 gate + 测试）
2. 全量 22 查询 30M 重跑 + OSS/VVR 基线刷新（q4 100M 已 5.58M 精确，文档 q4
   数字待更新）
3. q9 100M 复测（同款 deferred bid 目标，预期不再断崖）
4. q15 归并异步化（可选）；q3 −16% 独立 bug（与驱逐无关）

## Q4 三档规模（10M/30M/100M）EPS + 内存记录（2026-08-25，gate 修复后实测）

### 实测（哨兵 EPS，同机同帧源，rate=3M/s；100M 行 = 多次运行范围）

| 规模 | EPS | RSS_peak | q4a EMIT | oracle | 偏差 | bid 窗 | bid 行数 | 事件跨度 |
|------|-----|----------|----------|--------|------|--------|---------|---------|
| 10M | 10.0M | 3,345MB | 557,204 | 557,204 | 0 | ~1.8GB | 9.2M | 1000s=17min |
| 30M | 10.5M | 9,180MB | 1,672,559 | 1,672,559 | 0 | ~5.4GB | 27.6M | 3000s=50min |
| 100M | 10.7-10.9M | 17,076-17,317MB | 5,575,588-5,576,436 | 5,576,436 | 0~0.015% | 6.8GB | 33.1M | 10000s=2h46m |

（10M/30M oracle = 历史对拍值，见 SEMANTIC_SUPPORT_MATRIX/CAPABILITY_GAP_MATRIX；
100M oracle = wfgen verify 5,576,436）

### 变化逻辑

**EPS：与规模基本无关**（10.0/10.5/10.7M）——流式吞吐受速率（3M/s 摄入）与
引擎并行度限制，不受总量影响；10M 略低是启动/尾部占比噪声。gate 修复后
30M/100M EPS ≈ 10.5M（修前 12.98M，正确性换取的小幅回落）。

**内存：稳态窗 = 速率 × over（有界），与总量无关；三档阶梯是「未满 over 的
全量」vs「满 over 的 1h 保留」之差**：

- 稳态 bid 窗行数 = bid 速率 × 1h：10M=9.2M×3600/1000=33.1M、30M=27.6M×
  3600/3000=33.1M、100M=33.1M——**三档同密度下完全相等**（若都跑满 1h）。
- 实际 10M/30M 数据跨度 < 1h → 零时间驱逐 → 窗内 = 全量（9.2M/27.6M），
  **没装满 over 容量**；100M 跨度 2h46m > 1h → 驱逐到 1h → 33.1M。
- RSS 阶梯 = bid 窗（1.8→5.4→6.8GB）+ join 索引（随窗行数 0.6→1.3→1.9GB）
  + auction 窗（0.4→0.9→1.15GB）+ person/auction_finals（~0.4GB 恒定级）
  + parse buffer 2GB（恒定）+ 引擎基线（q1 100M 即 5.4GB）。
- 随总量增长的项只剩 missed 真 miss（~8.7%×auction 数，100M≈0.5GB，EOS 后
  释放）——修前 ~50% 假 miss 积压已消除。

### ⚠ 新发现：100M 尾部抖动（0~848 条，0~0.015%，待修）

三次 100M 运行 EMIT：5,576,436（精确）/ 5,576,349（-87）/ 5,575,588（-848）。
30M/10M 多次运行均精确。根因 = **keep-running EOS 竞态**（2026-08-23 曾为 q8
修过同类：daemon 收有限输入的 EOS 时窗口 actors 可能仍在排空 mailbox）：

- flush 用 final_wm（驱动窗 raw max_event_time）评估尾部 pending，但 bid 窗
  （actor 排空中）可能未 append 到 final_wm → 尾部（数据末 ~1-2s 内的
  auction，≈600-900 条）评估 miss → 进 missed；
- reevaluate_deferred_missed 立即重试一次——若 bid 窗此时仍未追平 → 保留回
  missed → 任务退出 → 丢。30M 尾部小 + 排空快所以未观察到。
- 修前该竞态被 63% 欠发淹没；gate 后尾部 pending 统一到 flush 评估，暴露面
  变成可观测的千条级抖动。

修复方向（未做）：flush 的 deferred 分支等目标窗 max_event_time 追平 final_wm
再评估（目标已 ingest 完全部输入，追平只差 actor 排空时间）；或退出前补一轮
deferred flush。需配单测（模拟 flush 时目标未追平 → 追平后补出）。

## ✅ 跨源提交乱序 × 健全前沿 gate（2026-08-25，本 session 落地）——30M over=30m -860 + 10M +2 + 内存 9.2GB 三连根治

### 背景
用户定调「不可能调 over 影响正确性」：D4 时间驱逐闭环后，30M over=30m 仍欠
发 -854/-872（oracle 1,672,559 vs 1,671,687/1,671,705）。探针实锤：flush 时
每 shard missed≈15.5k（重试恢复 99.4%，剩 ~85/shard = -860），仍 miss 的实例
**都在数据开头 43ms~150s**（tail_delta≈2800s）——运行期假 miss → 行被 over 驱逐
→ flush 重试无法恢复。

### 根因 1：跨源提交乱序 → 运行期假 miss（正确性）
- ingress `instances=8` + parse 10 并行派发：窗口 actor 只保证 **source 内** seq
  有序（pending map 重排），**跨 source 提交顺序自由**。全局 `max_event_time`
  被任一 source 的远未来 batch 提前推高。
- 上轮 gate 用 `target_wm`（全局 max）→ 实例到期评估时右行可能还在延迟 batch
  里 → 假 miss → missed（不再 pin）→ 行随后被 over=30m 驱逐 → 欠发。
- **修复：`committed_frontier_ns`（各 source 已提交 max 的 min）**——actor 提交
  时带 source 记录（`append_with_watermark_sized_from`），窗口暴露健全前沿；
  运行期 gate = `min(驱动 wm, 健全前沿)`。驱动 wm 是 oracle 语义边界，前沿是
  右行完整性判据。flush wait loop 同步改用前沿。

### 根因 2：flush final_wm 误用 max(驱动,目标) → 10M +2（多发）
- oracle 的 deferred watermark = **最后驱动事件时间**（wfgen oracle/mod.rs 469），
  verify 不推进 EOS——不是全局 max。上轮把目标窗 max 并入 final_wm → 尾部
  expiry ∈ (驱动末尾, 目标末尾] 的实例被评估 → 10M 557,206 vs oracle 557,204。
- **修复：回退驱动窗 max**（wait loop 保留，改用前沿）。

### 根因 3：lo_min 历史最小缓存 → pin 卡起点 → 内存 9.2GB（over 不降内存）
- `lo_min` 缓存 = 插入时单调不增的 min（drain 不更新，注释写"偏保守安全"）——
  正确性安全但内存不释放：任何 shard pending 非空 → pin = 历史第一个实例的
  lo ≈ 流起点 → 时间驱逐全被挡（探针：pin_floor=起点+1ms、evict=0、
  rows=27600000 整窗保留）。
- **修复：scan drain 到期前缀后标 `lo_min_dirty`**，publish 重算当前 pending 的
  min lo。旧 O(n²) 担忧是 63% 假 miss 时代（33M 挂起 × 2740 batch）的产物；
  gate 修复后 pending 很小（~100-29k），O(n) 无压力。
- 附带修复：gate 的 `frontier==i64::MIN` 回退（启动期目标窗空 → 对着空窗评估
  = 假 miss）改为**挂起等待**首个提交。

### 修复的测试（全部验证守护方向）
- wf-engine `committed_frontier_tracks_per_source_min`：按源前沿 = min，空回退
  max。
- wf-runtime `deferred_q9_cross_source_reorder_holds_evaluation_until_committed`：
  跨源乱序下评估必须等右行提交（sed 反证：旧 gate 失败）。
- wf-runtime `deferred_q9_pin_floor_advances_with_pending_drain`：pin 随 pending
  drain 推进，over 外旧行可驱逐（sed 反证：无 lo_min_dirty 失败）。
- wf-runtime `deferred_q9_time_eviction_pin_keeps_in_range_bids`：D4 时间驱逐闭
  环正路径（pin 保住越驱逐线的行）。
- 顺带修复预先存在的 perf_diag 测试污染：`set_perf_cuts_flips_both_gates` 缺
  reset（PERF_CUT_OUTPUT 常开 → 并行测试 emit 被切丢输出）；非门控测试改用
  `no_cut_stage`（run_sentinel_task 等长测试曾持 cut_output=true 达数秒）。

### 实测（哨兵 EPS，over=30m，正确性 + 内存双达标）

| 规模 | EMIT | oracle | RSS_peak | evict | 修复前 RSS |
|------|------|--------|----------|-------|-----------|
| 10M | 557,204 | 557,204 ✓ | 3,344MB | 0 | 3,345MB（+2 消失）|
| 30M | 1,672,559 | 1,672,559 ✓ | 6,698MB | 468 | 9,180MB（-860 消失）|
| 100M ×2 | 5,576,436 | 5,576,436 ✓（两次一致） | 8,709MB | 2272 | 17.6GB（欠发 6-9k 消失，尾部抖动 0~848 也消失）|
| q9 30M | 1,672,559 | 1,672,559 ✓ | 6,554MB | 468 | — |

over=30m 真正成为纯内存参数：三档全部 oracle 精确，100M RSS 17.6GB → 8.7GB
（比预期 ~13GB 更低）。

### 2026-08-25 review（5 轮）补强
- **R1 正确性**：`commit_appended_batch` 的 parsed 分支（目标窗有规则订阅时
  events=Some）曾丢 source → per_source 不记录 → 前沿回退全局 max → 跨源乱序
  修复失效（真实场景：q4a 订阅 auction_events、q8 的 deferred 目标正是它）。
  修复：新增 `append_with_watermark_parsed_sized_from`。无时间戳 batch
  （(i64::MIN,i64::MAX)）不记录 ✓；DroppedLate 不记录 ✓；锁序（log 锁块内、
  per_source 锁块外）无环 ✓。
- **R2 并发**：actor 单线程提交无并发；max 先于 per_source 更新（frontier 滞后
  = 保守方向）；lo_min 无"回落历史值"洞（dirty 重算覆盖 pending 空后新插入）。
- **R3 性能**：per_source Mutex 每 batch 一次（~36.5k 行 amortize 可忽略）；
  lo_min_dirty 重算 O(pending)（gate 后 pending 小）。⚠ 记录已知限制（预先
  存在，未修）：跨源乱序下 log 按 seq 有序 ≠ 事件时间序，时间驱逐按 seq 弹栈
  可能卡住旧行 → 内存次优（正确性无损，conns=1 不触发）。
- **R4 测试**：新增 `committed_frontier_records_parsed_sized_from`（R1 守护）、
  `committed_frontier_ignores_dropped_late`、
  `deferred_q9_flush_unblocks_evaluation_when_frontier_never_advances`
  （flush 收口不受 frontier 限制）。wf-engine 1117→1136、wf-runtime 533→535。
- **R5 质量**：doc 引用修正、`committed_frontier_ns` 去冗余 fallback、gate 注释
  准确化（i64::MAX = 窗口不存在；无时间列窗返回 i64::MIN 走挂起分支，deferred
  目标必有时间列故不可达）。
- 回归：30M 1,672,559 ✓ + 6.7GB；100M 5,576,436 ✓ + 8.7GB（review 改动无损）。

## 2026-08-25 M-13 q13 分片根治（max_acked 完成信号 + 生产/消费双分片）— 进行中

### 背景
M-13（100M q13 RSS 27GB + memory_evicted 1479）进入解决。探针/采样/基准逐层定位（非猜测）。

### 关键结论
1. **「丢数据」是误报**：1479 次 memory_evicted 全部是已读回收（min_acked 未读保护契约正常，
   双链测试钉死）。q13 真问题是慢 + 内存。
2. **q13b 单 worker 是性能墙**（~400k EPS，CPU 单核）：2026-08-23 保守规则「bind 中间窗的
   each 强制单 worker」在 push 模式下不必要——广播带真实窗口 seq、每批恰一次投递。
3. **卡尾 9 批（哨兵永不触发、bench 挂死）根因 = ack 语义**：round-robin 分片下每 shard 只
   ack 自己的批次，`min_acked` 恒停在最慢 shard 最后一批（next_seq=255 时 min=246），哨兵
   排空判定 `min_acked >= next_seq` 永不成立。修复 = **完成信号用 `max_acked`，驱逐保护仍用
   `min_acked`**（两用途分离）。
4. **q13a 分片后新瓶颈解除**：EPS 390k → 1.04M（2.7×）、CPU 10.9 核。

### 实测（哨兵 EPS，本地 mac，over=30m 配置）
| 规模 | 修前 | q13b 分片 | +q13a 分片 |
|------|------|-----------|------------|
| 30M | 400k / 15.4GB | 642k / 9.1GB | — |
| 100M | 390k / 27.1GB | 630k / 14.5GB | 1.04M / 41.5GB（RSS 问题，见下）|

输出完整性：30M 27.6M = oracle ✓；100M 91.67M（100M×91.7% bid 口径）✓；sink dispatch 全量 ✓。

### ⚠ 遗留（未解决，下一步）
1. **100M RSS 41.5GB（q13a 分片后）**：window_bytes 峰值 20.5GB = ingest 期间（33s，事件时间
   跨度仅 ~10min < over=30m）bid_events 时间驱逐不触发 → 100M 行全量驻留（~20GB），drain 后
   回落 3.8GB 稳态。**over 语义 × ingest 速度的固有内容，非泄漏**。候选：接受 peak（稳态有界）
   或 ingest 期放宽/预驱逐。RSS_peak 超 10GB 目标。
2. **memory_evicted_total=188 仍非零**（100M）：bench 作废判定。确认全为已读回收后考虑豁免。
3. **q13a 分片 pull 的 ack 语义隐患**：分片下 ack 读位置（new_cursor=全部）→ min_acked 追平 →
   bid_events 驱逐无未读保护 → cap 驱逐可能删「他 shard 未处理」批次。消费快未触发，但语义
   竞态存在。修复方向：分片 pull ack 只推进「自己份额连续处理完的位置」或驱逐 floor 按归属。
4. **q13b 列式化**（fmt 在 join 路径的 columnar gate 拒绝 → row path）：q13b 现仍 row path；
   列式 join 富化支持 fmt 右窗字段可再提（460ns vs 2.5µs/行）。

### 测试（全部通过）
- q13_dual_chain_sharded_push_consumption_complete（3 shard 完整 + 未读保护）
- q13_dual_chain_sharded_push_high_slope_repro（10 shard 紧通道背压 70 批全量）
- q13_dual_chain_sharded_producer_and_consumer（2 生产者乱序 + 3 消费者 60/60）
- max_acked_tracks_completion_across_shards（min 停滞 / max 追平语义）
- wf-runtime 540 / wf-engine 1137 / clippy 0

### 未提交改动
- wf-engine: progress.rs（+max_acked）
- wf-runtime: perf_diag.rs（哨兵排空 min→max）、metrics/sampling.rs（acked_lag min→max）、
  rule_task.rs（process_push ack fetch_max）、spawn.rs（q13b 消费 + q13a 生产中间窗分片放开）、
  deferred_integration_tests.rs（+3）
- 探针已全部清理（probe_window_memory / probe-rr / probe-push / probe-pb / probe-bc / probe-new）

### Pitfalls（本段新增）
- **drain 口径指标**：`emitted_total`/`dispatch_total`/`sweeps_total` 等 _total 指标是每 100ms
  drain（读后清零）的增量——加起来才是总量，单看会误判「停更=卡死」（本 session 两次误判）。
- **macOS sample 深度截断**：忙线程栈只到 tokio blocking-pool 层；lldb attach 被系统拒绝。
  定位靠分段探针（broadcast 前/后、append DONE、process_batch START/DONE）而非采样。
- **「卡死」要先排除任务已正常结束**：q13a 处理完最后批次后 park 在 notified() 是正常态；
  卡尾判定要看 ack_lag 是否归零 + 哨兵是否触发，而不是「探针日志停了」。
- **round-robin 分片 + min_acked 完成判定结构性不兼容**：min_acked 是驱逐 floor（保守），
  max_acked 才是完成信号——两用途必须分离。
- **探针会拖慢生产路径改变卡点位置**（run6 探针过重，卡点偏移）：定位用最小探针，跑真数据
  前清理。

## 2026-08-25 M-13 review（5 轮）——完成判定分组化 + 顺序敏感门控补全（已 commit）

对 M-13 双分片改动做 5 轮 review，修复 2 个真实缺陷，R2/R4/R5 验证无恙。

### R1 完成信号语义（修复：`WindowProgress` 分组完成判定）

**缺陷 A（真实）**：`wait_for_data_drain` 的 `max_acked>=next || min_acked>=next`
对 **key/行号分片（match/stats）窗口**不健全——`max_acked` = 最快分片游标，
它追平 next_seq 时慢分片可能仍在处理自己的行子集 → 哨兵提前排空（bench 提前
SIGTERM → 尾部输出被切，q4 尾部抖动同一类）。修前 30M/100M 全量跑通过是因为
稳态分片偏斜小，背压风暴下会放大。

**缺陷 B（真实）**：`release()`（`fetch_max(u64::MAX)`）会毒化 `max_acked`——
任务在 Drop 中释放槽位时（热重载替换规则任务）completion 被冲成已排空，
即使同窗口其他存活 shard 仍在消费。

**修复**：`WindowProgress` 消费槽分两组——`register()`（whole-batch：单 worker /
round-robin，每批归属唯一槽，完成 = 组内 **max**）与 `register_row_partitioned()`
（key/行号分片，每批要**所有**分片 ack 才算消费，完成 = 组内 **min**）；新
`completion_gap(next)` = 两组的 max 缺口（0 = 排空），两组都跳过 `u64::MAX`
（released）槽。驱逐保护仍用全局 `min_acked`（两组并集，不变）。

接线：spawn.rs stats key 分片 / 行号分片（q15）/ match key 分片 →
`register_row_partitioned_progress`；其余保持 `register_progress`。哨兵
`wait_for_data_drain` 与 `acked_lag` 指标改用 `completion_gap`。

**混合窗口（bid_events = q5 key 分片 + q4a round-robin）两条件都必须满足**：
row 组 min 追平 && batch 组 max 追平——纯 min 会被 round-robin 最慢 shard 永久
卡死（q13 卡尾的翻版），纯 max 会在最快 key 分片处提前排空。

### R2 分片 pull ack 处理位置（验证无恙）
`last_processed+1` + `fetch_max` 单调；空份额轮次 `unwrap_or(0)` no-op；每片
从自己游标读连续批次（驱逐 floor 保证无空洞），门控 `seq%N` 自洽。已有测试
`q13_sharded_pull_acks_processed_not_read_position` 守护。

### R3 q13b 消费分片边界（修复：顺序敏感门控补 stats last/top）
`intermediate_shard_safe = 非 deferred && 目标不被 Match 消费` 只挡了 Match；
下游 **stats last/top** 聚合按行序取极值，上游 round-robin 乱序会选错行。
修复：`collect_order_sensitive_targets` 把 stats last/top 的 binds 并入门控
（可交换 count/sum/min/max/distinct 与 each 下游仍容忍乱序）。

### R4 内存控制配置副作用（验证无恙 + 需全量实证）
`max_total_bytes=2GB` + `evict_interval=200ms` 全局生效：驱逐无安全回收时
`memory_pressure` → actor 停 append → 背压，无死锁环（任务 ack 不依赖新
append；驱逐回收 → 通知续跑）。对 q5/q16/q18 等 stats 大窗口的影响需全量
22 查询验证（bucket 状态在 StatsExecutor 内，2GB cap 管不到，靠 over 封顶）。

### R5 memory_evicted 判定移除（验证无恙）
生产驱逐全部 floor 门控（`evict_expired_acked`；无门控的 `evict_expired` 仅测试
用）。`cursor_gap` 仍是唯一丢未读信号（round-robin push 消费的批次先经通道投递，
日志被删无损；pull 消费者滞后时 floor 保护或 gap 告警二选一，不存在第三种）。

### 新增测试
- wf-engine progress：`completion_gap_waits_for_the_slowest_row_shard` /
  `completion_gap_uses_max_for_round_robin_shards` /
  `completion_gap_mixed_requires_both_groups` /
  `released_slot_does_not_poison_completion` /
  `min_acked_covers_both_consumer_groups`
- wf-runtime perf_diag：`data_drain_waits_for_slowest_row_shard`（慢分片阻塞排空）/
  `data_drain_completes_when_round_robin_max_catches_up`（min 停滞不卡完成）
- wf-runtime spawn_r4：`collect_order_sensitive_targets_covers_match_and_last_top_stats`
- 回归：wf-engine 1142 / wf-runtime 544 / clippy 0（全绿）

### 未提交改动（本段）
- wf-engine: window/progress.rs（+row_slots 分组 + completion_gap + 5 测试）
- wf-runtime: lifecycle/spawn.rs（register_row_partitioned_progress +
  collect_order_sensitive_targets）、perf_diag.rs（drain 改 completion_gap + 2 测试）、
  metrics/sampling.rs（acked_lag 改 completion_gap）、lifecycle/spawn_r4.rs（+1 测试）
- wf-examples: nexmark_pk/bench.sh + scripts/bench_lib.py（acked_lag 口径注释更新）

### Pitfalls（本段新增）
- **max/min 单一聚合做不了完成判定**：row 分片窗口看 min（最慢）、round-robin
  窗口看 max（归属唯一）、混合窗口两者都要——按消费分组算，别用一个数字
  硬套全窗口。
- **release 的 u64::MAX 是完成毒药**：完成聚合必须跳过 released 槽，否则热重载
  时哨兵提前排空。

## 2026-08-25 q13a 列式化（中间窗生产路径 1248ns → 203ns/行，6.1×）— 已 commit

### 背景
M-13 遗留：q13a（`on each b` → yield 中间窗 bid_mod，含 `mod_key = auction %
10000` BinOp）分片后 10 核 row path 分配放大 RSS 40GB → 回退单 worker（内存
优先），等待列式化后再评估放开。q13a 走 record 路径的每行成本（rule_task_bench
基线）：物化 batch_to_events 246ns + execute_each_with_joins 526ns +
stage_pipe_record 476ns = **1248ns/行**（0.80M/s 单核）。

### 实现（数据驱动：先 bench 定基线，再按 6.1× 目标改造）
1. **`each_batch_prepare`**：yield 表达式编译范围从「列式输出函数」扩到**任意
   `expr_is_columnar`**（BinOp `%` 编译为批级 cvec，`int_mod` 快路径）。sink 门控
   不变（BinOp yield 仍行式），新能力只被 pipe 路径消费。
2. **新 `each_pipe_columnar_safe()` 门控**（保守形状）：无 joins/lets/where/each
   filter、score 常量、entity 字面量/flat 字段、yield ∈ {字面量, flat 字段,
   `expr_is_columnar`}。真实 q13a 计划通过（守护测试）。
3. **新 `execute_each_pipe_batch_columnar`**（wf-engine）：逐行从 `ColumnarEvent`
   直读（零 Event 物化、零 OutputRecord/wfx_id/fired_at 脚手架），yield 经批级
   cvec 求值 + `coerce_yield_field_value_with` 同矩阵收口，产出 `PipeEachRow`
   （score/entity 元数据 + yield 值）。
4. **`PipeBatchStager::new_columnar` + `push_row`**（wf-runtime）：列来源计划
   （yield 下标 / `__wf_pipe_ts` / `__wfu_meta_*` / 无来源）构造一次，逐行直查——
   免行式 `push_record` 的每列名字符串查找。coercion 矩阵与 `push_record` 逐分支
   一致（共享 `push_pipe_col` 辅助）。
5. **`process_batch` 分流**：`columnar_each` 门控扩展到 pipe 目标
   （`each_direct ? each_plan_columnar_safe : each_pipe_columnar_safe`），分支内
   按目标分流 sink 列式 / pipe 列式，pipe 路径在同批 flush_pipes 收口。

### 实测（cargo bench，release，100k 行，同帧同机）
| 路径 | ns/行 | rows/s | 对照 |
|------|-------|--------|------|
| row path（物化+record+stage） | 1228.9 | 0.81M | 1.0× |
| **pipe 列式（eval+push_row）** | **203.4** | **4.92M** | **6.1×** |

单 worker q13a 4.92M/s > ingest 3M/s → 消费追上摄入，bid_events 不再积压，
为 100M 内存回落（min_acked 不再挡驱逐）铺路；也为重新评估「生产者分片放开」
（分配量级大降，mimalloc arena 膨胀风险缓解）提供依据。

### 测试（全部通过）
- `q13a_pipe_columnar_matches_row_path`（**对拍**，wf-runtime）：row path vs
  pipe 列式路径产出中间窗批次**字节一致**——含 `__wfu_meta_*` 回退列与
  `__wf_pipe_ts` 事件时间列，N=100k 全列全行比较。
- `each_pipe_columnar_safe_gate_branches`（wf-engine）：q13a mod BinOp 放行；
  filter/lets/活 join/where/非列式 yield/fmt/Path/non-const score/非列式 bind
  filter 拒绝（含死 join 消除后不误放行）。
- `q13a_compiled_plan_takes_pipe_columnar_path`（wf-runtime）：**真实编译**的
  q13a 计划必须过门控（防门控漂移）。
- 回归：wf-engine 1154 / wf-runtime 550 / clippy 0。

### 未提交改动（本段，已 commit）
- wf-engine: match_engine/executor/each_exec.rs（prepare 扩编译 + pipe 门控 +
  PipeEachRow + execute_each_pipe_batch_columnar + each_yield_meta_light +
  +1 测试）、columnar 不变（Mod 已支持）、match_engine/mod.rs + executor/mod.rs
  re-export PipeEachRow
- wf-runtime: engine_task/rule_task.rs（process_batch 分流 + emit_each_pipe_batch_columnar
  + PipeBatchStager::new_columnar/push_row + push_pipe_col + 列来源计划）、
  rule_task_bench.rs（+列式 bench + 对拍测试）、deferred_integration_tests.rs
  （+编译计划门控守护）

### Pitfalls（本段新增）
- **sink 列式门控与 pipe 列式门控是两个门**：sink 允许 each filter / 输出函数 /
  活 join 列式，pipe 路径保守拒绝（回退行式 stage_pipe_record）——扩展 `%`
  BinOp 编译时只动 `each_batch_prepare`（cvec 槽位），**不要**把 sink 门控也
  放开（若 sink 门控放行 BinOp，`%` 的 int/f64 分叉与 div-by-zero 语义边界需要
  全套 对拍 覆盖，超出本次范围）。
- **`%` 的 int 快路径**：columnar `int_mod` 走 i64 取模，解释器走 f64——值在
  ±2^53 内字节一致（对拍锁定）；极端大数会有 f64 精度分叉，`expr_is_columnar`
  的保守性假设值域在安全范围内。
- **push 模式（WFUSION_WINDOW_DISPATCH=push）q13a 仍走行式**：columnar_each
  分支要求 `events.is_none()`（广播带 events+batch 时不满足）——生产 pull 模式
  生效，push 模式是遗留兼容路径。

## 2026-08-25 q13a 列式化 review（5 轮）——边界对拍补盲 + 直接写路径行为对齐（已 commit）

### R1 正确性（修复：边界对拍补盲）
逐分支核对 `push_pipe_col`（共享 helper）与行式 `push_record` 的 coercion 矩阵
等价；对拍新增 **edge cases** 覆盖：
- null 输入行（Field lane 空串→coerce→省略/空 cell）；
- 负值 mod（cvec `int_mod` i64 vs 解释器 f64，±2^53 内一致）；
- schema 时间列回退（yield 未提供 time_fallback → event_time_nanos）；
- Missing 源列（schema 列不在 yield/meta → null cell）；
- meta 列（null entity 的 `__wfu_entity_id`/`__wfu_score` 渲染）。
全部字节一致（`q13a_pipe_columnar_matches_row_path_edge_cases`）。

### R2 并发/生命周期（修复 ×2）
- **sink 列式路径不再调 flush_pipes**（`if !self.each_direct`）：消除 q1 等高频
  sink 规则每批一次 pipe_state 锁争用（原实现无差别调用，纯开销）。
- `rule_name.to_string()` 改借用（每批一次堆分配消除）。
- 验证无恙：锁序（metrics → pipe_state，无反向）、锁内无 await（push_row 同步）、
  Uninit 惰性形状解析与行式 `stage_pipe_record` 同款。

### R3 性能（验证无恙 + 记录已知开销）
- `compile_guard` 无批级缓存——mod 小树每批重编译（~数百 ns/36.5k 行可忽略，
  与 sink 输出函数路径行为一致）。
- **已知开销（记录，不加 smallvec 依赖）**：`PipeEachRow.values` 每行一个
  Vec 堆分配（6 元素）；entity_id String 每行分配（pipe schema 无 `__wfu_entity_id`
  列时纯浪费，executor 不知 schema 故总是构造）。203ns 已达标（4.92M/s > ingest
  3M/s），后续可按占比优化。

### R4 测试（验证无恙）
- cut_output 门控行为与直接写路径族（`emit_each_direct`/sink 列式）一致
  （门控在前、计数在后）；record 路径 `emit()` 是另一套（计数在前、门控在后）
  ——预存差异，pipe 列式跟随直接写路径惯例。
- q13 e2e（6 个测试）走新路径且输出正确 + 门控守护测试（编译计划必须过门控）
  构成链路闭环。

### R5 质量（修复 ×2）
- `PipeColSource` 手写 Clone impl → derive（多余代码消除）。
- `let _ = each_plan` 丑写法 → `let Some(_each_plan)`。
- 注：gate 注释强调 light meta 约束——扩展 gate 至可读 meta 的表达式前必须
  重审 `each_yield_meta_light` 的空槽。

### 回归
wf-engine 1155 / wf-runtime 551 / clippy 0（+1 边界对拍）。


## 2026-08-25 q13 分片内存根治：广播按订阅类型裁剪（30M 达标，100M 遗留）——未 commit

### 一句话
分片态 RSS 28.8GB 根因 = 每批 q13a 物化 events（≈18MB/批）× 广播携带 × 分片积压
（bid_mod acked_lag 169 批）。修复 = **广播按订阅类型裁剪**：`RuleFanout::round_robin_only`
（只有 RoundRobin 订阅或无订阅）→ `take_batch` + `broadcast_batch_only`（不物化
events）；存在 Single/Sharded 订阅 → `take_events` + `broadcast_with_batch`
（保留 row-path 契约）。`columnar_each` 门控 `events.is_none()` → `shard_rows.is_none()`
（push 消费者也列式）。`spawn.rs` 放开 q13a 生产者分片。

### 实测（30M 达标）
| 配置 | EPS | RSS_peak |
|------|-----|----------|
| 单 worker（基线） | 1.52M | 5.9GB |
| 分片（events 冗余） | 2.78–3.88M | 28.8GB ✗ |
| **分片（裁剪后）** | **4.06M / 3.41M** | **9.87GB / 8.43GB** ✓ |

回归：wf-engine 1156 / wf-runtime 552 / clippy 0（+`round_robin_only_classifies_subscriptions`，
3 个被无条件裁剪 break 的测试恢复）。

### ⚠ 100M 遗留 → **独立 issue**：`issues/q13-memory-peak-scales-with-volume.md`
100M RSS_peak 26.4GB（平台期）：footprint 定案 23GB = mimalloc 段区（reclaimable，
已 purge）、物理 dirty 仅 3.4GB、窗口有界 6.4GB、bid_mod acked_lag 峰值 765。
机制判断 = RSS 是 mimalloc **峰值分配水位**，100M 下 q13b 消费滞后积压在途 batch
把水位顶高。验证路径与候选修复见 issue 文档，**下次从那里继续**。

### Pitfalls（本次新增）
- 无条件广播裁剪 break row-path 中间窗契约（3 测试断言 `push.events`）——必须按
  订阅类型（round_robin_only）裁剪。
- 决策在 take 之前：`take_batch`/`take_events` 都消费 stager 缓冲，二者互斥。
- RSS 26GB ≠ 泄漏：footprint 显示 23GB reclaimable，dirty 仅 3.4GB。RSS 是峰值
  分配水位的代理指标。
- 勿回退 q13a 分片（30M 已达标，回退是倒退）；100M 先看 lag 时间曲线再动手。

## 2026-08-26 over 调小实验：`bid_events/bid_mod → 10m`——无效，已回退

用户假设内存随数据量上升 ∝ over 保留量，要求把两窗都改 10m 实测。
结果（30M，per-row churn 消减后）：**EPS 5.40M 不变、RSS 14,282MB vs 基线
14.3GB（几乎不动）**；窗口预算仅省 3.68G→3.29G。over 调小对 RSS 杠杆率
≈0——保留量由消费滞后（ack floor 门控）决定，第三次独立验证（1h/30m/10m/1m
四档结论一致）。已回退 wfs 到 over=30m/1h；未归因 ~10G 的追查方向不变
（alert 通道/构建器在途、规则任务工作态、rule channel）。完整记录在
`issues/q13-memory-peak-scales-with-volume.md` §8。
