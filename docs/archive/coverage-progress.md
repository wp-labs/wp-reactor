# 覆盖率提升实施进展（目标 90% Line）

> 会话日期：2026-08-22 · 分支：feat/columnar-execution

## 目标

wp-reactor 仓库 Line 覆盖率从基线 76.84% 提升到 **90%**。

## 基线（改造前）

`cargo llvm-cov --workspace --tests --summary-only -- --skip match_engine::tests::deferred_bench::deferred_join_overhead_bounded`

| crate | 行覆盖 | 说明 |
|---|---|---|
| wf-config | 92.4% ✅ | |
| wf-data | 74.1% | |
| wf-lang | 71.7% | |
| wf-engine | 70.7% | |
| wf-runtime | 84.3% | |
| **TOTAL** | **76.84%** | 缺 16,160 行 |

## 第一波（已提交 8475993）—— +1.81pt → 78.65%

三个并行 agent 的测试补写：

- **wf-lang**：`checker/tests/coverage_extra.rs`(74) + `compiler/tests/coverage_extra.rs`(32) + `wfl_parser/tests/coverage_extra.rs`(17) + explain/diagnostics/field_usage/yield_preset/preprocess 内嵌（~43）。`cargo test -p wf-lang` → 784 passed（基线 618）。
- **wf-engine eval**：`executor/eval/tests.rs` +2608 行 38 测（62 builtin 全覆盖、L3 aggregate、stat selector、mv/regex/sha1）。735 passed。
- **wf-engine core**：`match_engine/tests/core_coverage.rs` 51 测 + `eval_coverage.rs` 11 测。735 passed。

提交：`8475993 test(wf-lang,wf-engine): 覆盖率补测第一波 ...`（15 files, +10967 行）。

## 第二波（已提交 10d7cc6）—— 82.76%

4 个并行 agent 分片补测（41 files, +11277 行）：
- wf-engine executor（53 测：coverage_extra/eval/builtins/stats）
- wf-engine match_engine/alert/window（55 测：match_engine 核心/event_bridge/alert 列批/actor）
- wf-lang（37 测：coverage_more ×3）
- wf-runtime（110 测：cli 从 0% 起步/spawn/bootstrap/rule_task/stats_task/metrics/tracing）

全量测试通过（wf-engine 843 / wf-lang 821 / wf-runtime 325）→ **82.76%**

## 第三波（已提交 a100a1f）—— 待重测

4 个并行 agent 深度补测（31 files, +6859 行）：
- wf-engine match_engine（32 测：OR/AND/Any 模式/expiry heap/conv/event_bridge）
- wf-engine builtins/alert/fanout/context/close（36 测）
- wf-lang（39 测：coverage_more2 ×3，stats 全聚合/limits/stat 选择器）
- wf-runtime（55 测：rule_task/stats_task/spawn/bootstrap/cli 深度分支）

全量测试通过（wf-engine 911 / wf-lang 860 / wf-runtime 380）→ **82.76%**（第三波后重测，Line cover 82.76% / 缺 14152 行）

## 第四波（进行中）—— 基于行级清单精准补测

生成 `target/profile-cov/uncovered_lines.txt`（158 文件行级未覆盖清单），4 个并行 agent：
- wf-engine match_engine（74 测：builtins L3 调度/builtins_r4/event_bridge_r4/executor_r4/eval_r4）
- wf-engine alert/window/sink/contract（coverage_r4 ×4，文件已落盘）
- wf-lang（53 测：coverage_r4 ×3，scope/joins/rules/check_funcs/compile/parser）
- wf-runtime（72 测：metrics/cli/receiver/stats_task/rule_task/spawn/bootstrap/tracing 的 *_r4.rs）

⚠️ 环境事件：并行 agent 运行期间宿主 fd 耗尽（`Too many open files`），终端不可用数十分钟；
agent 无法跑 cargo 验证。已手工修复 wf-runtime 5 个 r4 文件的编译错误（
AggPlan/BranchPlan/StepPlan import 路径、`with_filter` 需 Layer trait、`EvictionGate::new(max_total_bytes)`、
`BaseType::Chars`（无 Str）、`StructArray::try_new` 需 Fields、`TaskGroup` 非 Debug 用 match 替代 expect_err、
`StatsWindowState` re-export 补到 match_engine/mod.rs、unused imports 清理）。
diagnostics 确认全部编译错误清零。待终端恢复后：
1. `cargo test --workspace --tests` 全量验证
2. 重跑覆盖率
3. 提交第四波

## 坑 / 注意

- **llvm-cov 插桩会使计时测试必挂**：跑覆盖率必须 `--skip match_engine::tests::deferred_bench::deferred_join_overhead_bounded`（墙钟阈值：eval ≤ eager×8+50ns、pending < 2µs）。普通 `cargo test` 偶发抖动失败，单跑必过。
- llvm-cov 与普通 cargo test 共用 target 有竞争，不要并行跑两者。
- 覆盖率目标为 **Line 列**，非 Region/Branch。
- 提交不要 `git add -A`，显式列出文件。
