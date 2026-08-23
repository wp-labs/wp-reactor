# 覆盖率补测期间发现的 Bug 清单与处置（2026-08-23）

> 四波并行 agent 补测共报告 12 处潜在 bug / 语义不一致。
> 交叉验证 + 源码确认后：**3 处真实 bug 已修复**（含回归测试），
> 其余为防御性死代码 / 调用方契约 / 文档不一致，判定不修并记录理由。

## 🔴 已修复（真实 bug，含回归测试）

### B1 — 列式 close 失败行仍被提交（正确性）✅
- **位置**：`wf-engine/src/match_engine/executor/close_exec.rs::execute_close_direct_batch_columnar`
- **现象**：单行 yield coerce/export 失败时 `stats.failed += 1; builder.take_staged(); break;`——
  `break` 只退出 yield 字段循环，之后 `staged_rows.push(...)` / `wfx_ids.push(...)` /
  `stats.appended += 1` 照常执行，失败行带着空字段被提交进批次。
- **契约对照**：on-each 批量路径（`each_exec.rs`）是「失败行 `continue`，不触碰任何列、不计 appended」。
- **影响**：失败行产出含 gap 填充的残缺 alert；`failed` 与 `appended` 同时 +1 语义矛盾。
- **交叉验证**：两个独立 agent 各自发现并复现（coverage_extra.rs 与 close_coverage_more.rs 均固化该行为）。
- **修复**：`break` → `continue 'close`（外层循环加标签），跳过该行的全部提交。
- **测试**：更新 2 处固化错误行为的断言（`failed=1, appended=0, builder.is_empty()`）；
  新增混合行隔离回归测试 `close_direct_batch_columnar_skips_failed_row_keeps_rest`。

### B2 — stats `group by (...)` 与 `tier` 之间缺 `ws_skip`（解析）✅
- **位置**：`wf-lang/src/wfl_parser/stats_p.rs`
- **现象**：`stats<30m:fixed> group by (a.sip) tier a.count [...]`（带空格）解析失败；
  只有零空格 `)tier` 才能解析。`tier_clause` 以 `kw("tier")` 开头（不跳空白），
  `group_by_clause` 结束 `)` 后没有 `ws_skip`，`opt(tier_clause)` 遇到前导空格即失败。
- **修复**：`opt(tier_clause)` 前补 `ws_skip`。
- **测试**：新增 `stats_group_by_then_tier_with_whitespace` 回归测试。

### B3 — `rewrite_expr_label_refs` 不递归 Object/Array（编译）✅
- **位置**：`wf-lang/src/compiler/mod.rs::rewrite_expr_label_refs`
- **现象**：`Object` / `Array` 分支直接 `expr.clone()` 不递归，而
  `collect_bind_tracking` 对两者是递归的。`object { bidder = winner.bidder; }` 内的
  `winner.bidder`（`as label` 归约结果的 Qualified 引用）不会被重写为 `FieldRef::Path`；
  运行期归约行以裸键 object 注入 eval context，Qualified 引用会取错行。
- **修复**：Object/Array 分支递归调用 `rewrite_expr_label_refs`（注意 `ObjectItem` 字段为
  `targets`/`type_hint`/`value`）。
- **测试**：新增 `reduce_label_refs_inside_object_and_array_are_rewritten`（object 成员 /
  array 成员 / 顶层裸引用三种形态均断言重写为 Path）。

## 🟡 判定不修（记录理由）

### B4 — rule_task `PIPE_EVENT_TIME_FIELD` 的 `unreachable!`
- 事件时间列若非 Timestamp 会 panic。**判定**：pipeline schema 由 wf-lang 编译器生成，
  事件时间字段恒为 Timestamp，`unreachable!` 是编译器保证前置条件后的防御断言，非实际缺陷。

### B5 — `within [-10s, 0s]` 括号负时长无法解析
- 文档/错误消息示例与实际语法不一致（`duration_value` 不接受 `-` 前缀）。
- **判定**：负下界已有 `within 10s` 糖形式（`lo=neg:true`）可表达，括号负时长属文档示例
  过时；低优先级，后续修文档或补解析。

### B6 — untyped 数字顶层产 Float、嵌套产 Digit
- `export_untyped_value` 对整数 Number 恒产 `Float`，嵌套在 Object/Array 走
  `rule_value_to_model_value` 产 `Digit`。JSON 输出相同，仅列元数据不同。
- **判定**：展示层轻微不一致，无查询引用；记录待评估。

## 🟢 死代码 / 防御臂 / 调用方契约（非 bug）

| # | 位置 | 说明 |
|---|---|---|
| B7 | `ColumnarEvent::value_at` 越界 panic | 调用方契约（`build_field_index` 只含 schema 内列） |
| B8 | `visit_expr_fields` 兜底 `_ => force_all` | `Expr` 是 `#[non_exhaustive]`，防未来新增变体 |
| B9 | `advance_at_with_diagnostics` L788 分支 | AND/OR 短路使该臂不可达 |
| B10 | `binop_symbol`/`cmp_symbol` 的 `_ => "?"` | 枚举已穷尽 |
| B11 | joins/compiler 中 Path 分支 | parser 在语法层拒绝嵌套路径，不可达 |
| B12 | `check_funcs::unify_mvappend_element_type` compatible 臂 | 元素类型只可能 Base/ArrayAny |

## 验证状态

- 3 处修复 + 新增测试全部通过 `diagnostics` 编译检查（0 error / 0 warning）。
- ⚠️ 环境：宿主 fd 耗尽（`Too many open files`）导致终端不可用，`cargo test` 尚未执行。
  终端恢复后需跑：
  1. `cargo test -p wf-lang`（B2/B3 回归）
  2. `cargo test -p wf-engine --lib`（B1 回归）
  3. 全量 `cargo test --workspace --tests` + 重跑覆盖率 + 提交第四波。
