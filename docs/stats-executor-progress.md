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

## 5. 下一步（步骤 ④c daemon 接线）

**引擎层数据路径已锁定**（④b）: 编译 stats 规则 → StatsExecutor 归并（编译出的
where_expr 逐行求值）→ 合成 CloseOutput → `execute_close_with_joins` → OutputRecord,
与 CEP q15 锚点逐字符一致。剩余为 daemon 投递层：

- **StatsTask**（wf-runtime）: 类比 rule_task 的新任务类型
  - 消费 `broadcast_batch_only` 的 raw RecordBatch（空键 `register`; 带 key 后续
    `register_sharded`）; pull 模型（M1）需从 window log 直接拉批次
  - 窗口 close 信号: fixed 窗口按事件时间 watermark 越过窗口边界触发（镜像 CEP
    的固定窗口 bucket close, 见 match_engine close/scan_expired）
  - close 时: `close_window()` → 合成 CloseOutput（④b 已验证的路径）→
    `execute_close_with_joins` → 经 sink_fanout 下沉
  - **必须参与 ack floor**（progress slot, push_seq+1, 与 rule_task 对齐, 否则
    卡驱逐 cursor-gap）
- **spawn 接线**: `RunRuleKind::Stats` 变体 + `spawn_rule_tasks` 分支
- **P1.5 列式段**: process_rows 升级为 RecordBatch 列式读取（复用
  `scope_key_columnar` + `eval_guard_columnar`; count/sum/min/max 整列归并）

## 6. 阻塞项

- ~~系统文件句柄耗尽~~ ✅ 已恢复
- ~~checker 不感知 stats 标签~~ ✅ 已修（`populate_stats_measure_labels`, 3 测试）
- daemon 投递层（④c）为剩余接线面, 见 §5

---

## 6. 相关文档

- 设计：`docs/stats-executor-design.md`（v6 统一桶键模型）
- review：`docs/stats-executor-design-review.md` / `-v2.md`
- 语法示例：`wf-examples/performance/nexmark_pk/models/queries/q15.wfl`（现状 CEP 版）
