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

### 9.7 P2 review 修复（2026-08-22, 提交 xxx）

1. **🔴 D7/D8 精度（已修）**: `accumulate_keyed_row` 原经 `column_value` 把 Int64
   转 `Value::Number(f64)` 再回 i128/distinct——≥2^53 的 id 被舍入（2^53 与
   2^53+1 的 f64 相同 → distinct 塌缩、sum 失真）, 与空键列式原生路径发散; 且
   `column_value` 不含 Timestamp 分派 → 带 key 对 Timestamp distinct 静默全跳过。
   修复: 原生列值读取 `column_i128`/`column_distinct_key`（Int64 原生 i64、
   Timestamp 原生 i64, 对齐 insert_distinct_column/sum_masked 口径）。
   测试: `stats_columnar_keyed_precision_matches_empty_key_native` /
   `stats_columnar_keyed_timestamp_distinct_native`。
2. **🟡 空窗不产出（已修, 显式不变式）**: `close_current_window` 加
   `event_count == 0` guard——空键规则预建 Empty 桶（全 0）, 无 guard 时空窗
   close 会产出全 0 alert（分段归并下事件推进路径本不触发, 为显式不变式 +
   session/sliding 未来路径防线）。测试:
   `stats_task_empty_key_jump_emits_no_zero_windows`（5s→35s 跳窗, 空窗不产出）。
3. **📊 性能发现（非 bug, 记录）**: debug 文件 sink 对 stats 逐桶 emit 是重大
   瓶颈——同二进制 Q12 30M: sink 开 2.66M EPS/5.8GB vs sink 关 14.9M EPS/857MB
   （每窗 close 内 2830 条 alert 顺序 await 投递, sink 慢则回压停整条归并循环）;
   CEP 12M 不受影响。正常 bench（blackhole sink）下 stats Q12 实际快于 CEP。
   后续可把 per-bucket emit 批量合成一次投递。

### 9.8 P2 全量回归（review 后）

wf-engine 588 / wf-runtime 197 / wf-lang 567 全绿。端到端复验（Q12 30M,
precision 修复后）: 849,372/849,372 组合与 CEP 逐值一致, 0 差异。

### 9.9 批量 emit 改进（Q12/Q16/Q17, 提交 xxx）

**问题**（9.7 §3 发现落地）: close 逐桶 `emit_record().await`——每桶一次
AlertColumnBatch 构建 + 通道投递, 桶多时通道满后阻塞整条归并循环; debug
文件 sink 下实测 5.6× 退化（Q12 30M 2.66M→14.9M EPS）。

**修复**: `close_current_window` 逐桶构建 record 后按 yield_target 合并进同一
`AlertColumnBuilder`, 每窗口**一次**投递（桶序 = ScopeKey 升序不变）; 删
`emit_record`（单条路径）。

**验证**（30M v5, 批量 emit 后）:

| Query | 之前 EPS（debug sink 开） | 之后 EPS | RSS | 提升 |
|---|---|---|---|---|
| Q12 | 2.66M / 5.8GB | 16.5M（sink 开）/ 17.6M（blackhole）/ 1.1-1.2GB | 6.2× |
| Q16 | 6.9M / 3.9GB | 7.3M（sink 开）/ 7.5M（blackhole） | 1.09× |
| Q17 | 1.9M / 3.8GB | 10.0M（sink 开）/ 10.3M（blackhole）/ 4.5GB | 5.3× |

**正确性**: Q12 849,372/849,372 与 CEP 逐值一致; Q16 20,008（2 窗 × 10004
channel）按窗口序与 CEP 0 差异; **Q17 两窗口 distinct auction 数与帧数据
ground truth 精确一致（1,079,512 / 719,753）**——批量 emit 让窗口 2（719,753
条）在 flush 时一次投递完成, 不再被关停竞态截断（旧逐桶 emit 只落 ~80k 条）。

测试: q12 任务测试改为 `take_alerts` 断言一批多条（批量语义）; 全量回归
wf-engine 588 / wf-runtime 197 全绿。

## 10. P4 last/top 扩展度量（Q18/Q19, 已实现 + review 修复）

### 10.1 实现（工作区, 未提交）

- **执行器** `stats_exec.rs`: `StatsAccum.last_row`（Arc 跨同桶多 last 度量共享,
  免 4× 内存）+ `top_entries`（TopEntry = key f64 + 行字段）; `apply_last_top` /
  `insert_top`; `row_fields_from_batch`（字段子集提取, `None` = 全列）;
  `with_row_fields` 构造器; rich close `close_window_by_bucket_rows`（每桶每度量
  值列表, top = N 条目 rank 序; 桶按 ScopeKey 升序, 清空状态）; 带 key 逐行路径
  每行一次 `row_cache` 懒提取, 多 last 度量共享。
- **任务** `stats_task.rs`: `close_current_window` 有 last/top 走 rich 路径
  （n_records = 每桶最大条目数; top 每条目一条 alert, 行字段注入 `b.*`）;
  `build_stats_close_output` 加 `row_fields` 参数; 批量 emit 沿用。
- **spawn** `spawn.rs`: `stats_row_fields` 计算 last/top 行字段提取子集
  （yield/entity 引用 ∪ 度量字段; 桶键不入行）——Q18 5.29M 桶 × 整行 8 字段
  会到 ~19GB, 子集降 4× 以上。
- **解析器** `stats_p.rs`: `field_ref_lit` 入口 `ws_skip`; `group_by_clause` /
  `top(N, f)` 逗号两侧空白; 闭括号空白。
- **wf-examples**: `q18_stats.wfl`（group by (bidder, auction) + 4 last 度量）/
  `q19_stats.wfl`（group by (auction) + top(10, price)）。

**端到端**（30M v5）: Q18 EMIT 5,286,087 = CEP 精确一致, 抽查 30 万条 last bid
与帧数据 100% 一致; Q19 EMIT 7,943,687 = Σ min(10,bids) 精确一致。

### 10.2 review 发现与修复（2026-08-22）

1. **🔴 行式回退路径忽略 `row_fields` 子集（已修）**: `process_rows` 的 last/top
   保留整行（8 字段）, 与列式 `row_fields_from_batch` 子集不一致——一旦走行式
   回退（非列式 where / 函数键）Q18/Q19 内存放大 4×+。修复: 新增
   `row_fields_from_row`（按子集过滤）两处替换。测试:
   `stats_row_fields_subset_both_paths_match`（行式 vs 列式 close 逐条目一致 +
   子集外字段不入行）。
2. **🟡 top 空条目虚假产出（已修）**: `bucket_measure_entries` 的 Top 空条目
   原返回 `vec![scalar(0.0)]` —— `top(0, ...)` 或全部非数值键时产出 0.0 记录;
   且该分支是 `m.len()-1` 下溢的唯一防线。修复: 空条目返回空 Vec, 任务层
   `close_current_window` 改用 `m.get(min(k, len.saturating_sub(1)))` 安全读取
   （空 → 0.0/None）, 全空时 n_records=0 整桶不产出。测试:
   `stats_top_zero_keeps_no_entries` / `stats_top_zero_emits_nothing`。
3. **🟡 `insert_top` 每事件整行克隆（已修）**: Q19 每 bid 先 `row.clone()` 再
   `insert_top`（满时再截断）——克隆白付。修复: 快速淘汰提前到克隆前（满且
   `key <= entries[n-1].key` 直接跳过; 同 key 新条目必插在既有之后, 满时必被
   截断, 先到者语义不变）。测试: `stats_top_full_cutoff_replaces_tail`（满后
   高于门槛替换尾部、低于/等于门槛跳过）。
4. **🟡 解析器空格边界（已修）**: `group by (a , b)`（逗号前空格）与
   `top(10 , b.price)` 解析失败——separator/逗号用裸 `literal(",")` 不跳前导
   空白; `top( 10 ...)` 括号后空格亦失败。修复: 逗号两侧、闭括号均 `ws_skip`。
   测试: `stats_ws_tolerant_comma_and_parens`。
5. **🟡 last 字段缺失保留行（语义锁定, 补测试）**: last 度量字段缺失仍保留整
   行（yield 读其它字段）, 度量值 0.0——行式/列式一致。测试:
   `stats_last_missing_field_keeps_row`（行式缺键 vs 列式 null 列）。

### 10.3 已知保留项

- Q18/Q19 RSS 仍 ~21GB（5.29M 桶 × 6 字段子集）: Arc 共享 + 子集已生效; 进一步
  需行字段改紧凑结构（并行数组 / Vec<(name, value)> 代替 EngineHashMap）——
  **已由 §11 P5 落地**（行字段状态 ~19GB → ~1.3GB 量级, 30M RSS 预计 21GB →
  ~10GB 量级, 待 bench 复跑实测确认）。
- top 空条目时该度量对跨 top 记录贡献 0.0（混合 last+top 且 top 无条目）——
  退化可接受, 与 CEP 无实例语义对齐。

### 10.4 测试与回归

执行器 41（+5: 子集一致性 / top(0) / 字段缺失 / 满后替换 / 既有 tie 覆盖快速淘汰）;
任务 17（+1: top(0) 不产出）; 解析器 17（+1: 空格边界）。全量回归: wf-lang 570 /
wf-engine 598 / wf-runtime 203 全绿。切段 profile（release, N=500k）: 扫描 5.5
ns/evt, 切段+归并 42.6 vs 整批 31.5（附加 34.9%）, 边界密集 10 段 2.5×——与修复
前基线一致, 无回归。

## 11. P5 行字段紧凑化（Q18/Q19 内存, 2026-08-22）

### 11.1 动机

Q18/Q19 30M RSS ~21GB。行字段状态为 `Arc<EngineHashMap<String, Value>>`——每桶
6 个 SmolStr key + foldhash 节点 + Value ≈ 400B+/桶, 5.29M 桶直接顶到 ~19GB。
10.3 记录的 P5 可选优化落地。

### 11.2 方案

行字段从 **字段名 HashMap → 按子集列序的 `Box<[Option<Value>]>`**（缺失/null =
`None`）:

- `StatsAccum.last_row: Option<Arc<Box<[Option<Value>]>>>`、`TopEntry.row:
  Box<[Option<Value>]>`、`StatsCloseEntry.row_fields` 同型。
- **列序确定性契约**（三处同序）: 子集路径 = 构造期对 `row_fields` 排序（spawn
  恒传子集, 生产路径）; 无子集 = 行式按行键排序 / 列式按 schema 字段名排序。
- **热路径零查找**: `measure_field_idx`（每度量字段在列数组的位置）构造期预计算,
  `apply_last_top`/close 取值直索引（免去旧的每事件字符串查找）; 无子集时退化为
  按列名 position 查找（仅测试/缺省）。
- **提取零克隆**: 子集路径 `row_fields_from_batch` 直接复用列名切片, 不逐行克隆;
  None 时每批算一次排序名。
- **行式路径补 Arc 共享**: `process_rows` 也按行懒提取一次, 多 last/top 度量共享
  同一 Arc（原每度量各提取一份——与 accumulate_keyed_row 的 row_cache 对齐）。
- **任务层注入**: `build_stats_close_output` 加 `row_names` 参数, 列数组按列名展开
  注入 StepData.field_values（yield 读 `b.*` 不变）。

### 11.3 内存估算

每桶: 1 Arc + 1 Box + 6 × Option<Value>(24B) ≈ 170B + ScopeKey/Vec 开销 ≈ 250B,
vs 旧 ~400B+/桶——行字段状态 ~1.3GB 量级（旧 ~19GB）。30M RSS 预计 21GB →
~10GB 量级（剩余为 alert 批量构建等, 非行字段状态）。**30M 实测待 bench 复跑确认。**

### 11.4 测试

- `stats_row_fields_compact_and_shared`: 列数组长度 = 子集大小（非整行）+ 同桶
  多 last 度量 `Arc::ptr_eq` 共享 + 子集外字段不入列。
- 既有 last/top 测试改为显式子集形态（`full_bid_subset`——生产经 spawn 恒有子集,
  无子集时 last/top 度量值在标量/close 路径为 0.0, 已在 `with_row_fields` 注释
  声明该限制）; 列数组断言走 `row_val(row, names, name)` 辅助。

### 11.5 回归

执行器 42（+1 结构验证）; 全量: wf-lang 570 / wf-engine 599 / wf-runtime 203 全绿。

### 11.6 last/top 热路径 profile（release, N=500k, 10k auction 桶）

优化前:

| 形态 | ns/evt | evt/s | 边际（vs keyed count 40.8 ns/evt） |
|---|---|---|---|
| keyed count（基线） | 40.8 | 24.5M | — |
| 单键 4×last | 158.1 | 6.3M | +117.3（行字段列数组提取 + Arc 共享） |
| **Q18 复合键 4×last** | **507.4** | **2.0M** | **+466.6（其中 Pair 盒装键 ≈ 349）** |
| Q19 top(10) 单键 | 165.8 | 6.0M | +124.9（提取 + 有界插入） |

**发现**: 复合键 `ScopeKey::Pair` 每次事件 2 次 Box 分配 ≈ 349 ns/evt——Q18
的 bidder+auction 复合键是最大单项开销（占 Q18 总时间 ~69%）。

### 11.7 复合键优化（2026-08-22, 已实施）

**方案（11.6 方向 a 落地）**: stats 桶表从 `HashMap<ScopeKey, Vec<StatsAccum>>`
改为 **`HashMap<u64, Vec<StatsBucket>>`**（哈希键 + 碰撞链; `StatsBucket` =
完整 `ScopeKey` + 累加器）:

- **列式路径每事件零 Box 分配**: 键数 ≤ 4 走扁平路径——栈上叶数组
  （`scope_key_from_column`, 复用 fanout 的规范化单一来源）→ `comps_hash` →
  `keyed_bucket_mut`（链扫描, 完整 `ScopeKey` 仅**每桶首见**构建一次）;
  键数 > 4 回退 `scope_key_columnar`（罕见）。
- **哈希字节级同构**: `comps_hash`（叶数组）== `scope_key_hash`（树）——仅最外层
  tag + 内层叶 payload + 0x1f 分隔, 与 `key.rs::scope_key_shard_index` 同序列
  （内层叶无 tag 是既有设计: 类型歧义由碰撞链完整比较消歧）。行式/列式两路径
  同键必落同桶（测试锁定）。
- **空键/行式路径不变**: `bucket_mut(&ScopeKey)` 走树哈希; 空键单桶预建。
- **close 拍平**: `take_buckets()` 清空并拍平为 `(ScopeKey, accs)` 按 ScopeKey
  升序（输出契约不变）。

**效果**（同 profile 复测）:

| 形态 | 优化前 | 优化后 | 提升 |
|---|---|---|---|
| Q18 复合键 4×last | 507.4 ns/evt（2.0M） | **379.8 ns/evt（2.6M）** | **1.34×** |
| 单键 4×last | 158.1 | 149.2 | 1.06× |
| Q19 top(10) | 165.8 | 152.9 | 1.08× |

复合键 Pair 盒装成本 349 → 231 ns/evt（-34%）。**剩余** ~119 ns 复合键边际:
列式键读取分派（`scope_key_from_column` 每行每键 `downcast_ref`）可提前到每批
一次预解析（后续优化项, 收益 ~30-50 ns）; 行字段提取 ~111 ns 已成最大单项。

**测试**: `stats_composite_key_hash_flat_matches_tree`（哈希同构契约, 1-4 键 /
int+float+str 混合）/ `stats_composite_key_mixed_types_columnar_matches_row_based`
（Int64+Utf8 混合复合键逐桶对拍）/ `stats_composite_key_three_field_columnar_matches_row_based`
（3 键左深递归边界）/ `stats_composite_key_mixed_paths_same_bucket`（同一 executor
先列式批再行式行, 两查找路径落同桶不产生重复桶）。

### 11.8 复合键深度优化（2026-08-22, 三轮）

**① 键列类型预解析**: `resolve_key_columns` 每批一次解析键列类型为 `KeyColumn`
（借用 batch 列数组）——逐行免 `downcast_ref` 动态分派; 规范化与
`scope_key_from_column` 一致（含 Float64 规范化位, 复制 key.rs canonical 口径）;
不支持类型回退 `Other`。

**② profile 数据分布修正（重要）**: 原 profile 的 bidder 伪随机 100 万取值 →
50 万行几乎全唯一 → 50 万桶（100% 唯一率）, 夸大哈希表压力（复合键 count 189.7
ns/evt, 其中 ~98 ns 是桶密度伪影）。修正为 10 万桶 / 每桶 5 行（对齐 Q18 真实
唯一率 ~19%）后: 复合键 count 91.5、Q18 228.1 ns/evt。**结论: 复合键查找真实
成本 ~54 ns, 非早期误判的 ~152。**

**③ 桶表换 foldhash**: `buckets` 从 std `HashMap<u64, _>`（SipHash RandomState）
改为 `EngineHashMap`（foldhash, 与引擎热路径哈希一致）——单键 count 37.1 → 26.7
ns/evt（-28%）; 复合键 count 91.5 → 73.7。

**④ 行字段列索引预解析**: `row_fields_from_batch` 改收每批预解析的列索引切片
（`row_field_cols`, 免逐行 `schema.index_of`）——行字段提取边际 121 → 94 ns/evt。

**最终效果**（对齐 Q18 真实分布, N=500k, 10k auction × 10 万 bidder）:

| 形态 | 优化前 | 优化后 | 提升 |
|---|---|---|---|
| keyed count（单键基线） | 37.1 | 26.8 | 1.39× |
| 复合键 count | 91.5 | 80.2 | 1.14× |
| 单键 4×last | 148.0 | 120.7 | 1.23× |
| **Q18 复合键 4×last** | **228.1** | **183.1** | **1.25×**（EPS 4.3M → 5.5M） |
| Q19 top(10) | 143.0 | 135.4 | 1.06× |

**剩余构成**（Q18 = 183 ns/evt）: 行字段提取+Arc ~94 / 复合键查找 ~54 / 单键
基线 ~27 / 其余 ~8。下一优化候选: 行字段 Box 分配（每行一次, 每桶保留一行——
可提前判断「该行是否会成为 last」避免无谓分配, 或改紧凑定长数组）。

### 11.10 行字段分配优化（2026-08-22）

**⑤ 行字段 `Arc<[T]>` 单块分配**: `Arc<Box<[Option<Value>]>>` → `Arc<[Option<Value>]>`
——消除 Arc→Box→数组两层间接/两次堆分配（`Arc::from(vec)` 单块）。`TopEntry.row`
保持 `Box<[T]>`（top 条目各自独立行, 不共享; 插入时从 Arc 显式深拷贝,
快速淘汰分支仍提前跳过）。任务层 `row_fields` 类型同步。

**⑥ spawn 行字段子集排除桶键**: `stats_row_fields` 从子集移除 `plan.keys` 的
简单字段——桶键已由 close 从 scope_key 注入 field_values, 行字段重复存纯属浪费
（Q18 键 bidder/auction 去掉后子集 6 → 4 字段, 提取量与内存 -33%）。注意:
若 last/top 度量字段恰为桶键, 其度量值退化为 0.0（yield 读该字段仍经 scope_key
注入, 实际输出不受影响——注释已声明）。测试:
`stats_row_fields_excludes_key_fields`（spawn 层锁定）。

**效果**（对齐生产形态: schema 含 channel/url/dateTime, 子集排除桶键）:

| 形态 | 优化前(§11.8) | 优化后 | 提升 |
|---|---|---|---|
| Q18 复合键 4×last | 183-197 | **180-197（5.1-5.5M evt/s）** | 持平（噪声内） |
| 单键 4×last | 120.7 | 115.6-119.8 | ~1.04× |
| Q19 top(10) | 135.4 | 127.5-138.4 | ~1.03× |

Q18 数字受机器负载噪声（±10%）影响难精确归因; **⑥ 的对照测**（同计划 6 字段
不排键 vs 4 字段生产形态）波动 3.8-51.8 ns/evt, 方向明确; 内存收益明确:
行字段 6 → 4 字段（-33%）+ Arc 单块（-1 层指针）。Q18 端到端 EPS 预计
5.5M+（30M 实测待 bench）。

### 11.11 回归

执行器 46 / 任务 17 / spawn +1（桶键不入行）; 全量: wf-lang 570 / wf-engine 603 /
wf-runtime 204 全绿。

## 12. 端到端验证 + shutdown flush 修复 + 分块 emit（2026-08-22）

### 12.1 30M/10M 实测（stats 版, blackhole sink, `run_stats_bench.sh`）

30M 正确性（窗口事件推进产出, metrics 正常采集）:

| 查询 | EPS | RSS | EMIT | 对拍 |
|---|---|---|---|---|
| q18_stats | 3.47M | **14.2GB** | 5,286,087 | ✅ 与优化前一致; RSS 21.7→14.2GB（-35%） |
| q19_stats | 5.43M | **17.6GB** | 7,943,687 | ✅ 与优化前一致; RSS 23.3→17.6GB（-25%, 分块后） |

10M 性能摸底: q12 12.8M（EMIT 282,514 ✓ 10s 窗自然关闭）/ q15 4.1M / q16 6.5M /
q17 10.2M / q18 9.9M / q19 10.2M（EPS; 均 [clean]）。

### 12.2 shutdown flush 产出被丢（修复）

**现象**: 10M + 30m 窗口查询（span 16.7m < 30m 窗, 唯一窗口靠 flush 关闭）
EMIT=0。daemon.log: `stats alert channel closed, dropping alert batch`。

**根因链**: ① daemon 模式 receiver 常驻 → EOS 只在 cancel 时触发 → stats 的
EOS flush 永远发生在 shutdown 中; ② stats flush 构建百万级 alert 需数秒
（q19 30M ≈ 8M 条 ~13s）; ③ `SINK_DRAIN_BUDGET=1s` → sink consumer 在 stats
flush 投递前耗尽预算退出（drop rx）; ④ `GROUP_JOIN_TIMEOUT=3s` → rules/alert
group 在 flush 完成前被 abort。投递必然失败。

**修复**: `SINK_DRAIN_BUDGET 1s → 30s`（sink 等 stats flush 投递）+
`GROUP_JOIN_TIMEOUT 3s → 60s`（group 等 flush 完成; 卡死任务仍被 abort 兜底）。
验证: 10M q18 shutdown 后 alert group `aborted=0`、无 channel closed 警告,
产出落地（debug_output 文件增量）; 30M q18 窗口 2 也能产出。

**残余（采集口径, 非实现 bug）**: metrics task 在 SIGTERM 后立即退出, flush 的
emitted_total 增量在 metrics 停止后 → 10M 30m 窗查询的 EMIT 仍显示 0（产出已在
sink 侧确认）。正确性验证走 30M（窗口事件推进产出, metrics 正常）或任务测试。

### 12.3 q19 close 峰值（分块 emit, 修复）

**现象**: q19 30M close 一次构建 7.94M 条 alert, RSS 峰值 23.3GB（与 alert 条数
正相关; q18 5.29M 条 → 14.2GB）。

**修复**: `emit_close_record` 分块——builder 累计达 `EMIT_CHUNK=100 万` 条即
`dispatch_columns` 并重建（投递仍批量, 不引入 §9.9 前的逐桶 await 回压）。
效果: q19 30M RSS 23.3 → 17.6GB（-25%）, CPU 789%→362%, EPS 持平 5.4M,
EMIT 7,943,687 一致。

### 12.4 回归

全量: wf-lang 570 / wf-engine 603 / wf-runtime 204 全绿。
