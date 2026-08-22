# stats 执行器 P1 实施进展记录

> 状态：实施中（2026-08-22）
> 设计依据：`docs/stats-executor-design.md`（v6）
> 目标：P1 = 语法 + 计划 + 空键 fixed count/distinct + Q15 对拍

---

## 1. 实施范围（P1 分解）

| 步骤 | 内容 | 状态 |
|---|---|---|
| ① 语法层 | AST + parser（stats 子句） | ✅ 已提交 `444778d`（wf-lang 557 全绿） |
| ② 计划层 | StatsPlan 编译（桶键/度量/输出形状） | ✅ 已提交 `d09fdb0`（wf-lang 559 + wf-engine 557 + wf-runtime 186） |
| ③ 执行层 | StatsExecutor（空键 fixed count/distinct/sum/avg/min/max） | ✅ 已提交 `8b68e8a`（5 单测 + 全量回归） |
| ④a 执行器对拍 | Q15 12 度量对拍（逐度量 where + 独立参考实现） | ✅ 已提交 `7b7b885`（wf-engine 565 全绿） |
| ④b 引擎层接线 | checker stats 标签 + 编译→归并→alert 全链路对拍 | ✅ 已提交（wf-lang 562 / wf-engine 568 全绿） |
| ④c daemon 接线 | fanout/pull 投递 + 窗口 close 信号 + ack floor | 🔶 下一步（见 §6） |

---

## 2. 已完成改动（步骤 ①）

### 2.1 新增文件

| 文件 | 内容 |
|---|---|
| `crates/wf-lang/src/ast/stats.rs` | AST：`StatsClause` / `StatsWindow` / `StatsWindowMode` / `StatsOutputShape` / `StatsMeasure` / `StatsAgg` |
| `crates/wf-lang/src/wfl_parser/stats_p.rs` | 解析器：`stats_clause_only`（stats<dur[:mode]> + group by + tier + measure + where） |
| `crates/wf-lang/src/wfl_parser/tests/stats.rs` | 4 个解析测试：空键 count / tier 列展开 / group by / where 过滤 |

### 2.2 修改文件

| 文件 | 改动 |
|---|---|
| `crates/wf-lang/src/ast/mod.rs` | 注册 `mod stats` / `pub use stats::*` |
| `crates/wf-lang/src/ast/rule.rs` | `RuleDecl` 加 `stats_clause: Option<StatsClause>` |
| `crates/wf-lang/src/wfl_parser/mod.rs` | 注册 `mod stats_p` |
| `crates/wf-lang/src/wfl_parser/rule.rs` | rule 解析加 stats 分支（`match stats_p::stats_clause_only` 先尝试，失败回退 pattern/stage） |

### 2.3 语法设计要点（对齐 v6 统一桶键模型）

```wfl
stats<30m:fixed> {                        // 空键 fixed 窗口
    b | count as total_bids;              // count 度量
    b | distinct_count(b.bidder) as total_bidders;
}

stats<30m:fixed> tier b.price [ <10000, <1000000 ] {   // tier 语法糖 → tier() 桶键 + 列展开
    b | count as total_bids;
}
// ≡ group by (tier(b.price, 10000, 1000000)) output columns

stats<30m:fixed> group by (b.channel) {   // 分组键
    b | count as bids;
}

stats<30m:fixed> {
    b | count as google_bids where b.channel == "google";   // 行过滤
}
```

### 2.4 已解决的 parser 关键点

1. **measures 尾部分号**：`separated(1.., m, sep)` 语义要求 sep 后必有 item，
   `m1; }` 会失败——改为**手写循环**（`opt(";")` 分号分隔，无分号则要求 `}`）。
2. **tier 边界空格**：`[ <b1, <b2 ]` 中 `[` 后、`<` 前的空格——同样手写循环，
   `ws_skip` + `opt("]")` 终止判断。
3. **`where` 后空格**：`expr::parse_expr` 不跳前导空白——`preceded(kw("where"),
   preceded(ws_skip, expr))`。
4. **stats 规则无 `-> score`**：stats 分支不消耗 `->`（score 缺省 0），
   entity 直接跟在 stats 块后。

---

## 3. 测试状态

| 测试 | 断言 | 状态 |
|---|---|---|
| `stats_empty_key_count` | 空键 / fixed / Rows / count | ✅ 通过 |
| `stats_tier_columns` | tier → `tier()` FuncCall / Columns | ✅ 通过 |
| `stats_group_by` | group by 键解析 | ✅ 通过 |
| `stats_where_filter` | where 表达式解析 | ✅ 修复后单测通过（`debug_stats_measure` 验证 OK=true） |

> ⚠️ 完整 wf-lang 回归测试**未跑完**：系统文件句柄耗尽（`Too many open files`，
> shell 无法启动）。恢复后需：
> 1. `cargo test -p wf-lang --lib` 全量回归
> 2. 确认 `stats_where_filter` 在完整规则上下文中通过

---

## 4. 已提交记录

| 提交 | 内容 |
|---|---|
| `444778d` | 步骤① 语法层: AST + parser |
| `d09fdb0` | 步骤② 计划层: StatsPlan 编译 |
| `8b68e8a` | 步骤③ 执行层核心: StatsExecutor（count/distinct/sum/avg/min/max） |

## 5. Q15 stats 性能 profile（2026-08-22, release, N=500k 同数据同机）

`cargo test --release -p wf-engine close_bench -- --ignored --nocapture`
（`q15_stats_executor_profile`, 与 CEP 对照同一次运行）:

**优化后（where 内建求值 + 去重共享, 提交 `497e095`）**:

| 路径 | ns/evt | M evt/s | 相对 CEP engine_full |
|---|---|---|---|
| CEP engine_full(advance) | 600 | 1.67 | 1.00× |
| CEP accumulate_close | 510 | 1.96 | 0.85× |
| CEP prod_row_full(生产) | 339 | 2.95 | 0.57× |
| **stats 行式全量（内建共享 where）** | **450** | **2.22** | **1.31×（快）** |
| **stats 列式全量（P1.5）** | **115** | **8.73** | **5.14×（快）** |
| 分量 count(×1) | 1.4 | 695 | — |
| 分量 where9（1× build + 9 eval, 未共享） | 406 | 2.46 | — |
| 分量 where3（1× build + 3 eval, 共享分档） | 205 | 4.89 | — |
| 分量 distinct(×4, DistinctKey) | 91 | 11.0 | — |

**优化前基线（P1 行式, where 逐度量 build+eval）: 1387 ns/evt（0.72M/s）= 0.44×（慢）**

**结论**：

1. **where 共享 ctx + 去重共享已落地**: `StatsExecutor::new` 预计算
   `unique_wheres`（q15 9 度量 where → 3 唯一表达式）+ `measure_where` 映射;
   `process_rows` 每行 1 次 Event 构建 + 唯一条件求值, 同条件度量共享结果。
   **1387 → 450 ns/evt（3.1×）, 快于 CEP engine_full 1.31×**。
2. **列式段（P1.5）已落地**: `process_batch(&RecordBatch) -> bool`——where 列式
   mask（`eval_guard_columnar`, 去重后唯一条件每批一次）+ count/sum/min/max
   整列归并（无逐行循环）+ distinct 行式段（按 mask true 行读原生列值插入）。
   **450 → 115 ns/evt（3.9×）, 相对 CEP engine_full 5.14×、生产路径 ~3×**。
   列式前置不满足（where 不可列式化 / distinct 字段类型不支持）时返回 `false`,
   调用方必须回退 `process_rows`（语义等价, 对拍测试锁定）。
3. 列式剩余开销 ≈ 115 − distinct 87 ≈ 28 ns/evt（mask 摊还 + 度量循环 + 列解析）;
   distinct 每行 1-4 次哈希不可回避（批内预去重为后续优化）。
4. 已实测否决: 共享 baselines 不可行（`RollingStats` 为 cfg(test) 类型）。

## 6. daemon 接线（步骤 ④c, 已落地）

**StatsTask 已实现**（`wf-runtime/src/engine_task/stats_task.rs`, 提交 xxx）:

- `RunRuleKind::Stats` + `build_run_rules` 分支（stats_plan + time_field 解析）+
  `spawn_rule_tasks` 分支（空键单实例 `register`; 带 key 分片为 P2）
- StatsTask 消费 fanout 投递的 raw RecordBatch: push 通道
  （`WFUSION_WINDOW_DISPATCH=push`）或 pull window log（默认 M1）
- 归并: 列式 `process_batch` 优先, 前置不满足回退行式 `process_rows`
  （batch_to_events 物化; 语义等价）
- 窗口 close: fixed 桶对齐 CEP（`bucket_start = (t/dur)*dur`）, 按批次最大事件
  时间（单调 watermark）越过边界触发; 空桶不产出（与 CEP 无实例一致）;
  EOS/取消/通道关闭时 `flush` 关闭残留窗口
- close 时: `close_window()` → 合成 CloseOutput（④b 路径）→
  `execute_close_with_joins` → AlertColumnBuilder 单条 → sink_fanout 投递
- **ack floor**: 处理完 ack 进度 slot（push_seq+1 / 读位置）, 与 rule_task 对齐

**接线 review 修复**: `advance_window` 死循环 bug——`while watermark >= window_end`
用循环外绑定的局部 `window_end`（close 后不更新）→ watermark 越界时无限 close。
改为循环内重读 `self.window_end` + 极短 dur 防呆（测试捕获）。

**测试**（stats_task_tests, 4 个）: 跨窗口 close 触发 + alert 值 + ack floor;
单窗口 flush 收尾; 多窗口跳变（空桶跳过）; 不可列式 where 回退行式路径。

## 7. P1 完整状态

P1（语法 → 计划 → 执行 → 对拍 → 接线）已全部落地。全量回归:
wf-lang 567 / wf-engine 577 / wf-runtime 190 全绿。

**性能全景**（Q15, N=500k 同数据同机）:
CEP engine_full 589 → stats 行式 450 → **stats 列式 115 ns/evt（8.7M/s）**
= 相对 CEP 生产路径 ~3×、engine_full ~5×。

## 8. 阻塞项

- ~~系统文件句柄耗尽~~ ✅ 已恢复
- ~~checker 不感知 stats 标签~~ ✅ 已修（`populate_stats_measure_labels`, 3 测试）
- daemon 投递层（④c）✅ 已落地（本 §）

## 8. review 发现与修复（2026-08-22, 提交 xxx）

1. **🔴 部分应用 bug（已修）**: `process_batch` 在段 1（count/sum 累加）**之后**
   段 2 才发现 distinct 字段类型不支持返回 `false` → 调用方回退 `process_rows`
   会把已累加的值重复计算。修复: 前置检查 `distinct_fields_columnar_safe`
   （在**任何副作用之前**一次性判定类型支持集）。回归测试
   `stats_columnar_partial_apply_rolls_back_cleanly`。
2. **🔴 avg count 缺失（已修）**: 段 1d 对 `Sum/Avg/Min/Max` 只累加 sum/极值,
   不累加 `count`——avg 输出 `sum/count` 时 count=0 → 恒 0。修复: 段 1d 对所有
   度量先按 mask 累加 `count`（对齐行式「count++ 在字段读取前」）。测试
   `stats_columnar_sum_avg_min_max_matches_row_based`。
3. **🟡 checker 不校验 stats 度量字段（已修）**: 度量 field/where 的字段拼写错误
   在运行时静默失效（eval None → 不累计, 无告警）。新增 `check_stats_measures`:
   source alias 存在 + field 引用可解析（`resolve_field_ref`）+ where 为 bool
   表达式。5 测试（未知字段 / where 未知字段 / where 非 bool / 未知 alias / 合法）。
4. 测试补充: 列式 sum/avg/min/max（含 null + where）、Float64/Utf8 列 distinct、
   多批累积、列式接线路径（process_batch → alert 与行式逐字符一致）。

---

## 8. 相关文档

- 设计：`docs/stats-executor-design.md`（v6 统一桶键模型）
- review：`docs/stats-executor-design-review.md` / `-v2.md`
- 语法示例：`wf-examples/performance/nexmark_pk/models/queries/q15.wfl`（现状 CEP 版）

---

## 9. P2 实施进展（复合键分组 + sum/avg/min/max + 分片并行）

> 状态：已实现 + 端到端验证（Q12/Q16 100% 对拍一致）；Q17 部分验证（见 §9.5）
> 目标：`stats<dur:fixed> group by (keys) { count/sum/avg/min/max/distinct_count }`，
> 覆盖 Q12/Q16/Q17；带 key 规则按键分片并行（fanout `register_sharded` / pull 行子集）。

### 9.1 已实现（wp-reactor `feat/columnar-execution` 分支, 未提交）

| 模块 | 内容 |
|---|---|
| `stats_exec.rs` | 多桶 `HashMap<ScopeKey, Vec<StatsAccum>>`（空键单桶快路径）; 桶键求值 `eval_row_key`/`eval_row_bucket_key`（Field / `bucket(f,'day'|...)` / `tier(f,b1,...)`）; `final_measure_values_by_bucket`（桶按 ScopeKey 升序, 确定性对拍契约）; `process_batch_keyed`（列式桶键 + mask 逐行归并）; `process_batch_rows(batch, rows: Option<&[u32]>)`（P2 分片行域, 见 §9.2） |
| `stats_task.rs` | `close_current_window` 每桶一条 alert（桶键拆解为 scope_key + 键字段值注入 field_values）; pull 分片（`read_since_with_shard(cursor, shard_index)`）; **批内窗口切段**（见 §9.3） |
| `spawn.rs` | stats 分片分支: 桶键全为简单字段且 `shard_count>1` → 按键分片多任务; 空键/含函数键 → 单实例 |
| `ScopeKey`/`field_ref_name` | `pub(crate)` → `pub`（wf-runtime 跨 crate） |

### 9.2 Blocker 1 — 分片行子集重复归并（已修）

**Bug**: 带 key 规则分片后, `process_batch` 忽略 shard_rows, 每片处理全批 →
每个键被 N 片各算一遍, close 重复输出 N 倍（Q16 实测 EMIT 152,318 = 10×）。

**修复**: `process_batch_rows(batch, rows)`——`rows` = 本片行索引子集:
- 空键路径: 行域转列 mask（`domain_mask`）与 where mask 逐位 AND（`combine_masks`）,
  整列归并原语（count_true/sum_masked/minmax_masked/insert_distinct_column）无需改动;
- 带 key 路径: `process_batch_keyed` 只遍历行域内行;
- 任务侧: pull 传 `shard_rows_per_batch`（`read_since_with_shard`）; push 传
  `RulePush.shard_rows`; 行式回退只物化行域（`materialize_rows`）。

**测试**: `stats_columnar_row_subset_empty_key_counts_domain_only` /
`stats_columnar_row_subset_keyed_disjoint_partition` /
`stats_sharded_task_processes_only_own_rows`。

**端到端验证**（30M v5 数据, rule_parallelism=10）: Q16 EMIT 152,318 → 20,008
（0 重复行）; Q12 849,682 条 alert 全唯一（(bidder, window) 组合零重复）。

### 9.3 窗口归属 — 批内切段（已修, 新发现）

**Bug**（Q12 对拍暴露, 44% 组合发散）: 原实现先按批次最大事件时间推进窗口,
再整批归并到推进后的窗口——批跨窗口边界时尾部 ~17k 行错归到下一窗
（Q12 每窗总量 74k/111k 交替 vs CEP 恒 92k; 总计数守恒）。

**修复**: `process_batch_from` 按事件时间（v5 数据排序保证）扫时间列**切段**, 每段
归并到其所属窗口后再推进（对齐 CEP 逐事件归属; 分片子集/整批统一走行段）。

**测试**: `stats_task_segments_keyed_batch_across_window_boundary` /
`stats_task_segments_empty_key_batch_across_window_boundary`。

**端到端验证**（30M v5 数据）:
- **Q12**: 849,372/849,372 组合逐值一致（0 差异, 修复前 44% 发散）; 300 窗口
  总量全等（每窗 92,000）。
- **Q16**: 10004 channel × 2 窗口按序逐值一致（0 差异; 仅尾部窗 fired_at 标签
  不同: stats=01:00:00 桶边界 vs CEP=00:49:59.998 关窗 watermark——元数据, 值相等）。

### 9.4 分片语义

- 桶键全为简单字段且 `shard_count>1` → fanout 按键分区（同 key 同片, 片内桶不跨
  片拆分）; 空键/含函数键（bucket/tier）→ 单实例。
- 片内窗口推进独立（每片只对自己的行切段/推进）; 键缺失行 → shard 0
  （fanout 分片口径, 归并仍按完整键）。

### 9.5 Q17 对拍分析（值一致; 尾窗产出受 bench 关停窗口限制）

用帧数据（pyarrow 解析 RFC6587 帧）建立 ground truth: 30M v5 数据窗口 1 [0,30m)
= 16.56M bids / **1,079,512 distinct auctions**; 窗口 2 [30m,60m) = 11.04M bids /
**719,753 distinct auctions**。

- **stats 窗口 1 EMIT = 1,079,512 = ground truth 精确一致**（10 片全部桶正确产出,
  metrics 计数）→ 累积/分片/close/输出链路正确。
- 窗口 2（end 60m > 数据 span 50m）只能由 EOS **flush** 关闭——bench.sh 追平后
  `sleep 3` → `kill_daemon`（SIGKILL +13s）, 720k 条逐桶 alert 的 flush 被截断
  （debug 文件只落 ~80k 条, 每次跑略有差异 = 关停竞态）。**CEP 同样丢失窗口 2**
  （其 290k 尾部为窗口 1 空闲实例经墙钟 sweep 延迟关闭, 非窗口 2）。
- 与 CEP 逐组合比较: 共享组合值 100% 一致（849,564）; 差异全部来自 (a) CEP
  空闲实例墙钟 close 语义（fired_at 归到 ~50m 尾部）与 (b) 关停竞态截断尾窗。

**结论**: stats 累积正确（窗口 1 精确匹配 ground truth + Q12 全窗口 100%）;
Q17 尾窗完整验证需 bench 关停前给足 flush 时间（bench.sh 追平后等待
`emitted_total` 稳定再采集/kill）, 属 bench 基建跟进项。

### 9.6 全量回归

wf-engine 586 / wf-runtime 196 / wf-lang 567 全绿（含新增 5 测试）。
端到端: Q16 EPS 6.9M（CEP 3.4M, 2.1×）RSS 3.9GB（CEP 10.2GB）; Q12 stats
EPS 2.5M（CEP 12M, stats 逐桶 close 为热点）; Q17 stats EPS 1.9M（CEP 6.8M）。
