//! Round-2 coverage-fill tests for `executor/context.rs` — the branches the
//! in-module `tests` and `executor/coverage_extra.rs` do not reach:
//!
//! - `execute_interval_join` bound-eval failure (lo / hi `Expr` bound missing
//!   on the left row) → inner drops, optional modes keep;
//! - `execute_interval_join` with no `asof_candidates` → inner drops, optional
//!   modes keep;
//! - `eval_interval_bound`: `Duration` overflow saturates to `i64::MAX`; an
//!   `Expr` number that fails epoch normalization → `None`;
//! - `enrich_join_row` over a columnar row with a null cell (skipped);
//! - `build_eval_context` narrow build requesting the `_bind_<alias>_count`
//!   synthetic field by its full name, and a step whose
//!   `satisfied_branch_index` is out of range (no `_step_*_source`).
use std::sync::Arc;

use std::collections::HashSet;
use std::time::Duration;

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use wf_lang::ast::{Bound, BoundVal, Expr, FieldRef, JoinMode, WithinSpec};
use wf_lang::plan::{BranchPlan, JoinCondPlan, JoinPlan, StepPlan};

use crate::match_engine::cep::{BindData, EngineHashMap, Event, StepData, Value, WindowLookup};
use crate::match_engine::executor::context::{eval_interval_bound, in_interval};
use crate::match_engine::executor::{CloseCtxFields, build_eval_context, execute_joins};
use crate::match_engine::{JoinRow, columnar_join_rows};

fn num(n: f64) -> Value {
    Value::Number(n)
}

fn str_val(s: &str) -> Value {
    Value::Str(s.to_string().into())
}

fn event(pairs: Vec<(&str, Value)>) -> Event {
    Event {
        fields: pairs.into_iter().map(|(k, v)| (k.into(), v)).collect(),
    }
}

struct EmptyLookup;
impl WindowLookup for EmptyLookup {
    fn snapshot_field_values(&self, _w: &str, _f: &str) -> Option<HashSet<String>> {
        None
    }
    fn snapshot(&self, _w: &str) -> Option<Vec<JoinRow>> {
        None
    }
}

fn timed_row(ts: i64, id: f64) -> (i64, JoinRow) {
    let mut fields = EngineHashMap::default();
    fields.insert("id".into(), num(id));
    (ts, JoinRow::Event(Arc::new(Event { fields })))
}

struct TimedLookup(Vec<(i64, JoinRow)>);
impl WindowLookup for TimedLookup {
    fn snapshot_field_values(&self, _w: &str, _f: &str) -> Option<HashSet<String>> {
        None
    }
    fn snapshot(&self, _w: &str) -> Option<Vec<JoinRow>> {
        Some(self.0.iter().map(|(_, r)| r.clone()).collect())
    }
    fn asof_candidates(&self, _w: &str, _kf: &str, _k: &Value) -> Option<Vec<(i64, JoinRow)>> {
        Some(self.0.clone())
    }
}

/// `within [lo, hi]` with the given bounds (both closed).
fn within_spec(lo: Bound, hi: Bound) -> WithinSpec {
    WithinSpec { lo, hi }
}

fn dur_bound(secs: u64, neg: bool) -> Bound {
    Bound {
        open: false,
        val: BoundVal::Dur {
            dur: Duration::from_secs(secs),
            neg,
        },
    }
}

fn expr_bound(expr: Expr) -> Bound {
    Bound {
        open: false,
        val: BoundVal::Expr(expr),
    }
}

/// A single-cond `aid == right.id` interval join.
fn interval_join(within: WithinSpec, mode: JoinMode) -> JoinPlan {
    JoinPlan {
        right_window: "bid_events".into(),
        mode,
        conds: vec![JoinCondPlan {
            left: FieldRef::Simple("aid".into()),
            right: FieldRef::Qualified("w".into(), "id".into()),
        }],
        within: Some(within),
        reduce: None,
        emit_at: None,
    }
}

#[test]
fn interval_bound_eval_failure_semantics() {
    // lo bound is a left-row field that is missing → inner drops, snapshot keeps.
    let within = within_spec(
        expr_bound(Expr::Field(FieldRef::Simple("lo_f".into()))),
        dur_bound(0, false),
    );
    let lookup = TimedLookup(vec![timed_row(495_000_000_000, 1.0)]);
    let mut ctx = event(vec![("aid", num(1.0))]);
    assert!(
        !execute_joins(
            &[interval_join(within.clone(), JoinMode::Inner)],
            &mut ctx,
            &lookup,
            500_000_000_000
        ),
        "inner drops when the interval bound cannot be evaluated"
    );
    let mut ctx = event(vec![("aid", num(1.0))]);
    assert!(
        execute_joins(
            &[interval_join(within.clone(), JoinMode::Snapshot)],
            &mut ctx,
            &lookup,
            500_000_000_000
        ),
        "snapshot keeps when the interval bound cannot be evaluated"
    );
    let mut ctx = event(vec![("aid", num(1.0))]);
    assert!(
        execute_joins(
            &[interval_join(within.clone(), JoinMode::Anti)],
            &mut ctx,
            &lookup,
            500_000_000_000
        ),
        "anti keeps when the interval bound cannot be evaluated"
    );
    assert!(
        !ctx.fields.contains_key("bid_events.id"),
        "no enrichment on bound failure"
    );

    // hi bound missing → same semantics (lo evaluated fine).
    let within = within_spec(
        dur_bound(10, true),
        expr_bound(Expr::Field(FieldRef::Simple("hi_f".into()))),
    );
    let mut ctx = event(vec![("aid", num(1.0))]);
    assert!(
        !execute_joins(
            &[interval_join(within.clone(), JoinMode::Inner)],
            &mut ctx,
            &lookup,
            500_000_000_000
        ),
        "inner drops when the hi bound cannot be evaluated"
    );
    let mut ctx = event(vec![("aid", num(1.0))]);
    assert!(
        execute_joins(
            &[interval_join(within, JoinMode::Snapshot)],
            &mut ctx,
            &lookup,
            500_000_000_000
        ),
        "snapshot keeps when the hi bound cannot be evaluated"
    );
}

#[test]
fn interval_join_without_candidates_falls_back_per_mode() {
    // No candidates at all (window empty / missing): inner drops, others keep.
    let within = within_spec(dur_bound(10, true), dur_bound(0, false));
    let mut ctx = event(vec![("aid", num(1.0))]);
    assert!(
        !execute_joins(
            &[interval_join(within.clone(), JoinMode::Inner)],
            &mut ctx,
            &EmptyLookup,
            500_000_000_000
        ),
        "inner interval with no candidates drops"
    );
    let mut ctx = event(vec![("aid", num(1.0))]);
    assert!(
        execute_joins(
            &[interval_join(within.clone(), JoinMode::Snapshot)],
            &mut ctx,
            &EmptyLookup,
            500_000_000_000
        ),
        "snapshot interval with no candidates keeps"
    );
    let mut ctx = event(vec![("aid", num(1.0))]);
    assert!(
        execute_joins(
            &[interval_join(within, JoinMode::Anti)],
            &mut ctx,
            &EmptyLookup,
            500_000_000_000
        ),
        "anti interval with no candidates keeps"
    );
}

#[test]
fn eval_interval_bound_duration_overflow_and_bad_expr() {
    let ctx = event(vec![]);
    // Duration::MAX.as_nanos() overflows i64 → saturates to i64::MAX.
    let huge = Bound {
        open: false,
        val: BoundVal::Dur {
            dur: Duration::MAX,
            neg: false,
        },
    };
    assert_eq!(eval_interval_bound(&huge, &ctx, 1_000), Some(i64::MAX));
    let huge_neg = Bound {
        open: false,
        val: BoundVal::Dur {
            dur: Duration::MAX,
            neg: true,
        },
    };
    // Negative overflow: -i64::MAX + event time (saturating on the offset).
    assert_eq!(
        eval_interval_bound(&huge_neg, &ctx, 1_000),
        Some(-i64::MAX + 1_000)
    );
    // A numeric Expr bound that fails epoch normalization (out of range) → None.
    let bad = expr_bound(Expr::Number(1e300));
    assert_eq!(eval_interval_bound(&bad, &ctx, 0), None);
    // A numeric Expr bound that is too small → None.
    let tiny = expr_bound(Expr::Number(-1e300));
    assert_eq!(eval_interval_bound(&tiny, &ctx, 0), None);
}

#[test]
fn in_interval_open_both_sides() {
    // (lo, hi) open: boundaries excluded, interior included.
    assert!(!in_interval(100, 100, 200, true, true));
    assert!(!in_interval(200, 100, 200, true, true));
    assert!(in_interval(150, 100, 200, true, true));
    // Open lo / closed hi combos.
    assert!(in_interval(200, 100, 200, true, false));
    assert!(!in_interval(100, 100, 200, true, false));
}

#[test]
fn enrich_join_row_skips_null_columnar_cells() {
    // A columnar row with a null cell: the null field is skipped, the present
    // fields are enriched (qualified + plain).
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, true),
        Field::new("note", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![Some("a"), Some("b")])) as ArrayRef,
            Arc::new(StringArray::from(vec![None, Some("n")])) as ArrayRef,
        ],
    )
    .unwrap();
    let rows = columnar_join_rows(vec![batch], None);
    let mut ctx = event(vec![("key", str_val("a"))]);
    let join = JoinPlan {
        right_window: "w".to_string(),
        mode: JoinMode::Inner,
        conds: vec![],
        within: None,
        reduce: None,
        emit_at: None,
    };
    crate::match_engine::executor::context::enrich_join_row(&mut ctx, &join, &rows[0]);
    assert_eq!(ctx.fields.get("w.id"), Some(&str_val("a")));
    assert_eq!(ctx.fields.get("id"), Some(&str_val("a")));
    // Null `note` cell → field_value None → skipped entirely.
    assert!(!ctx.fields.contains_key("w.note"));
    assert!(!ctx.fields.contains_key("note"));
}

#[test]
fn build_eval_context_named_bind_count_and_out_of_range_branch() {
    let keys = vec![FieldRef::Simple("sip".into())];
    let scope_key = vec![str_val("10.0.0.1")];

    // Narrow build requesting the synthetic `_bind_<alias>_count` name.
    let bd = BindData {
        alias: "win".to_string(),
        count: 5,
        field_values: EngineHashMap::from_iter([("amount".to_string(), vec![num(1.0), num(2.0)])]),
    };
    let named = CloseCtxFields::Named(HashSet::from(["_bind_win_count".to_string()]));
    let ctx = build_eval_context(&keys, &scope_key, &[], &[bd], &[], None, &named, None);
    assert_eq!(ctx.fields.get("_bind_win_count"), Some(&num(5.0)));
    // `amount` was not requested → absent (only the count field is wanted).
    assert!(!ctx.fields.contains_key("amount"));

    // Step whose satisfied_branch_index is out of range → no `_step_0_source`.
    let sd = StepData {
        satisfied_branch_index: 9,
        label: Some("login".to_string()),
        measure_value: 3.0,
        event_first_time_nanos: None,
        event_last_time_nanos: None,
        collected_values: Vec::new(),
        field_values: EngineHashMap::default(),
    };
    let plan = StepPlan {
        branches: vec![BranchPlan {
            label: Some("login".into()),
            source: "e".into(),
            field: None,
            guard: None,
            agg: wf_lang::plan::AggPlan {
                transforms: vec![],
                measure: wf_lang::ast::Measure::Count,
                cmp: wf_lang::ast::CmpOp::Ge,
                threshold: Expr::Number(1.0),
            },
        }],
    };
    let ctx = build_eval_context(
        &keys,
        &scope_key,
        &[sd],
        &[],
        &[&plan],
        None,
        &CloseCtxFields::All,
        None,
    );
    assert_eq!(ctx.fields.get("login"), Some(&num(3.0)));
    assert_eq!(ctx.fields.get("_step_0_measure"), Some(&num(3.0)));
    assert!(
        !ctx.fields.contains_key("_step_0_source"),
        "out-of-range satisfied_branch_index must not set a source field"
    );
}
