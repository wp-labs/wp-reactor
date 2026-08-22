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

## 第二波（进行中）—— 目标 90%

剩余缺口 top 文件（`scripts/cov_top.py /tmp/cov_report.txt`）：

| 文件 | 缺行 | 优先级 |
|---|---|---|
| `wf-engine/src/match_engine/match_engine/mod.rs` | 1111 | 高 |
| `wf-engine/src/alert/types.rs` | 862 | 高 |
| `wf-engine/src/match_engine/executor/eval/builtins.rs` | 836 | 高 |
| `wf-lang/src/compiler/mod.rs` | 846 | 高 |
| `wf-lang/src/checker/rules/mod.rs` | 697 | 高 |
| `wf-engine/src/match_engine/event_bridge.rs` | 672 | 高 |
| `wf-runtime/src/engine_task/rule_task.rs` | 671 | 中 |
| `wf-engine/src/window/fanout.rs` | 615 | 高 |
| `wf-runtime/src/lifecycle/spawn.rs` | 604 | 中 |
| `wf-runtime/src/cli/mod.rs` | 249（0%） | 中 |
| `wf-engine/src/match_engine/executor/*`（mod/context/close_exec/each_exec/stats） | ~1500 | 高 |
| `wf-runtime/src/lifecycle/bootstrap.rs` / `metrics/mod.rs` / `stats_task.rs` | ~600 | 中 |

策略：4 个并行 agent 按 crate/目录分片补测，写范围不相交。

## 坑 / 注意

- **llvm-cov 插桩会使计时测试必挂**：跑覆盖率必须 `--skip match_engine::tests::deferred_bench::deferred_join_overhead_bounded`（墙钟阈值：eval ≤ eager×8+50ns、pending < 2µs）。普通 `cargo test` 偶发抖动失败，单跑必过。
- llvm-cov 与普通 cargo test 共用 target 有竞争，不要并行跑两者。
- 覆盖率目标为 **Line 列**，非 Region/Branch。
- 提交不要 `git add -A`，显式列出文件。
