//! match_engine 测试桶：单元 / 两后端一致性 / 回归 / 基准。
//!
//! - `l2/`（语言层特性）、`l3/`（conv 变换）：**解释 vs 列式两后端一致性对拍**，
//!   与 `eval_coverage.rs` / `executor/eval/coverage_*` 一起守护双后端语义不漂移；
//! - `regression/`：P0/P1 bug 修复回归（22–34）；
//! - `*_bench.rs`：热路径基准（`#[ignore]` 或 bench 专用，不进常规单测）；
//! - CEP 状态机内部细节测试在同级 `cep/tests/`。

mod accu;
mod advance_count_bench;
mod any_l2;
mod close_bench;
mod columnar_bench;
mod columnar_close_seq;
mod columnar_fieldview;
mod columnar_wiring;
mod core_coverage;
mod deferred_bench;
mod deferred_join;
mod each_bench;
mod eval_coverage;
mod event_bridge_coverage;
mod event_bridge_coverage_more;
mod event_bridge_r4;
mod guard_bench;
mod helpers;
mod hop_bench;
mod interval_bench;
mod join_key;
mod match_bench;
mod nexmark_hotpath_bench;
mod perf;
mod q13b_join_bench;
mod scope_key_bench;
mod seq_l2;
mod spill_write_bench;
mod stats_soa_bench;

mod cep_core;
mod close;
mod executor;
mod l2;
mod l3;
mod regression;
mod seq_order;
