//! 直发（on-each）执行路径：逐事件/逐批推进 step 分支，命中即组装输出行
//! （alert 列构建复用），覆盖 deferred / pipe 等变体；与 stats_exec（窗口
//! 统计）正交。引擎内执行器组织见 `executor/mod.rs`。
//!
//! 子模块：`plan`（列式门控/join plan 解析）、`direct`（行式直发 + 安全门）、
//! `col_exec`（列式批执行器）、`col_join`（列式 join + 输出组装）。共享执行原语
//! （`EntityCol`/`I64Col`、`E1Profiler`、`passes_each_filter` 等）留本层收口。
use std::sync::OnceLock;
use std::time::Instant;

use arrow::array::{Array, Int64Array, StringArray, TimestampNanosecondArray};
use smol_str::SmolStr;

use crate::match_engine::cep::{Event, Value};

use super::eval::eval_bool_expr;
#[path = "col_exec.rs"]
mod col_exec;
#[path = "col_join.rs"]
mod col_join;
#[path = "direct.rs"]
mod direct;
#[path = "plan.rs"]
mod plan;

// re-export 面（executor/mod.rs 以 `pub use each_exec::{EachDirectBatchStats,
// PipeEachRow, PipeRowSink}` 转发 → match_engine → wf-runtime；EachJoinPlan /
// parse_each_join_columnar 经 executor/mod.rs 的 each_exec:: 路径消费）。
pub use col_join::{EachDirectBatchStats, PipeEachRow, PipeRowSink};
// 供同层子模块（direct / col_exec）经 `use super::*` 使用。
pub(crate) use plan::{
    EachJoinPlan, ScorePlan, ScoreShape, entity_is_general, expr_refs_let, fmt_identity_field,
    parse_each_join_columnar, score_is_general, score_shape,
};

fn passes_each_filter(filter: Option<&wf_lang::ast::Expr>, event: &Event) -> bool {
    match filter.and_then(|expr| eval_bool_expr(expr, event)) {
        Some(result) => result,
        None => filter.is_none(),
    }
}

/// Env-gated per-row segment profiler for the columnar on-each execute path
/// (Q1 bisection). Defaults to off with one `OnceLock`-cached `Instant`-free
/// check; `E1_TIMER=1` breaks the per-row budget into entity / fired_at /
/// wfx_id / begin+stage / commit buckets and prints ns/row after the batch.
/// Intended for `each_bench` and end-to-end profiling, never shipped hot-path.
struct E1Profiler {
    on: bool,
    buckets: [u64; 5],
}

#[inline(always)]
fn e1_bucket_entity() -> usize {
    0
}
#[inline(always)]
fn e1_bucket_fired() -> usize {
    1
}
#[inline(always)]
fn e1_bucket_wfx() -> usize {
    2
}
#[inline(always)]
fn e1_bucket_stage() -> usize {
    3
}
#[inline(always)]
fn e1_bucket_commit() -> usize {
    4
}

impl E1Profiler {
    fn maybe() -> Self {
        static ENABLED: OnceLock<bool> = OnceLock::new();
        let on = *ENABLED.get_or_init(|| {
            std::env::var("E1_TIMER").is_ok() && std::env::var("E1_TIMER").as_deref() != Ok("0")
        });
        E1Profiler {
            on,
            buckets: [0; 5],
        }
    }
    #[inline(always)]
    fn enabled(&self) -> bool {
        self.on
    }
    #[inline(always)]
    fn add(&mut self, bucket: usize, start: Instant) {
        if self.on {
            self.buckets[bucket] += start.elapsed().as_nanos() as u64;
        }
    }
    fn report(&self, rows: usize) {
        if !self.on || rows == 0 {
            return;
        }
        let total: u64 = self.buckets.iter().sum();
        let n = rows as f64;
        eprintln!(
            "[E1-profiler] rows={rows} total={:.1}ns/row",
            total as f64 / n
        );
        let names = [
            "\u{7c} entity  ",
            "\u{7c} fired_at",
            "\u{7c} wfx_id  ",
            "\u{7c} stage   ",
            "\u{7c} commit  ",
        ];
        for (name, ns) in names.iter().zip(self.buckets.iter()) {
            eprintln!(
                "  {} {:>7.1} ns/row  ({:>4.1}% of segment total)\n",
                name,
                *ns as f64 / n,
                if total > 0 {
                    *ns as f64 / total as f64 * 100.0
                } else {
                    0.0
                }
            );
        }
    }
}

/// The null / missing-column entity fallback on the columnar on-each path:
/// the Event reference path routes a missing entity field through the yield
/// empty-string fallback, so the row still appends with `entity_id = ""` and
/// a shared-column yield reads the empty string too.
#[inline(always)]
fn empty_entity_pair() -> (String, Option<Value>) {
    (String::new(), Some(Value::Str(SmolStr::default())))
}

/// Whether `export_yield_f64` handles the target type natively (no `Value`
/// fallback), so the entity==yield numeric fast lane can stage the raw number
/// directly and stay byte-identical to the `Value::Number` coerce+export path.
#[inline(always)]
fn is_numeric_yield_type(field_type: Option<&wf_lang::FieldType>) -> bool {
    matches!(
        field_type,
        None | Some(wf_lang::FieldType::Base(wf_lang::BaseType::Digit))
            | Some(wf_lang::FieldType::Base(wf_lang::BaseType::Float))
            | Some(wf_lang::FieldType::Base(wf_lang::BaseType::Chars))
    )
}

/// Batch-resolved typed entity column (P2): ONE downcast per batch, direct
/// typed reads per row — replaces the per-row `value_at` +
/// `write_flat_column_scratch` double dynamic dispatch on the entity path.
enum EntityCol<'a> {
    /// Int64 / Timestamp(ns) — physically i64 arrays; one typed read feeds
    /// both the `write_int64_value` rendering and the `Value` held for
    /// shared-column yield reuse.
    I64(I64Col<'a>),
    /// Plain (non-structured) Utf8 — `&str` read pushed directly (the qradar
    /// entity shape: sip / source_ip / user). Structured Utf8 columns must
    /// stay [`EntityCol::Generic`] — their values JSON-parse in
    /// `extract_field_value`.
    Utf8(&'a StringArray),
    /// Everything else keeps the existing `value_at` + `value_to_string` lane.
    Generic,
}

/// The two physically-i64 column flavors an [`EntityCol::I64`] can hold.
enum I64Col<'a> {
    Int64(&'a Int64Array),
    TsNanos(&'a TimestampNanosecondArray),
}

impl I64Col<'_> {
    /// Typed read with the same null gate as `ColumnarEvent::value_at`
    /// (`None` on a null slot → the shared entity-failure branch).
    #[inline(always)]
    fn read(&self, row: usize) -> Option<i64> {
        match self {
            I64Col::Int64(a) => {
                if a.is_null(row) {
                    None
                } else {
                    Some(a.value(row))
                }
            }
            I64Col::TsNanos(a) => {
                if a.is_null(row) {
                    None
                } else {
                    Some(a.value(row))
                }
            }
        }
    }
}
