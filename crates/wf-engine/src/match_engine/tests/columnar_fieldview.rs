//! P3 FieldView — differential conformance between the eager `Event` path and
//! the columnar `ColumnarEvent` path feeding `CepStateMachine::advance_at_with_masks`.
//!
//! The whole point of the columnar entry is byte-identical behavior with zero
//! per-hit-row HashMap materialization. These tests run the *same* batch
//! through both sources (`Event` materialized via `batch_to_events` vs
//! `ColumnarEvent` read straight from the columns) and assert identical
//! per-row `StepResult`, identical instance counts, and identical close/expiry
//! outputs — covering the interpreted-guard fallback (`masks = None`), the
//! columnar-mask path (`masks = Some`), measure/branch-field reads, seq
//! negation, and `to_event()` byte parity.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use wf_lang::ast::{BinOp, CloseMode, Expr, FieldRef, FieldSelector, MatchMode, Measure, CmpOp};
use wf_lang::plan::{AggPlan, BranchPlan, MatchPlan, SeqPlan, SeqSkipPlan, SeqStepPlan};

use crate::match_engine::{
    ColumnarEvent, FieldSource, GuardMasks, RuleExecutor, batch_event_time_nanos_at,
    batch_time_col_index, batch_to_events, build_field_index, materialize_rows_filtered,
};
use crate::match_engine::match_engine::{CepStateMachine, CloseReason};

use super::helpers::{branch, count_ge, plan_with_close, simple_key, simple_plan, simple_rule_plan, step};

fn eq_str(field: &str, val: &str) -> Expr {
    Expr::BinOp {
        op: BinOp::Eq,
        left: Box::new(Expr::Field(FieldRef::Simple(field.to_string()))),
        right: Box::new(Expr::StringLit(val.to_string())),
    }
}

/// Branch with a guard (used for event/close/neg guard coverage).
fn guarded_branch(source: &str, guard: Expr, agg: AggPlan) -> BranchPlan {
    BranchPlan {
        label: None,
        source: source.to_string(),
        field: None,
        guard: Some(guard),
        agg,
    }
}

/// Branch with a sum measure over a field selector (exercises
/// `extract_branch_field` + `update_measure(Sum)` + `collect_event_fields`).
fn sum_dport_branch(source: &str, threshold: f64) -> BranchPlan {
    BranchPlan {
        label: None,
        source: source.to_string(),
        field: Some(FieldSelector::Dot("dport".into())),
        guard: None,
        agg: AggPlan {
            transforms: vec![],
            measure: Measure::Sum,
            cmp: CmpOp::Ge,
            threshold: Expr::Number(threshold),
        },
    }
}

/// `(sip, action, dport, ts)` rows → a batch with a `ts` time column and a
/// `wp_src_ip` machine-id column.
fn sip_batch(rows: &[(&str, Option<&str>, Option<i64>, i64)]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("sip", DataType::Utf8, true),
        Field::new("action", DataType::Utf8, true),
        Field::new("dport", DataType::Int64, true),
        Field::new("ts", DataType::Int64, false),
        Field::new("wp_src_ip", DataType::Utf8, true),
    ]));
    let sip: Vec<Option<&str>> = rows.iter().map(|r| Some(r.0)).collect();
    let action: Vec<Option<&str>> = rows.iter().map(|r| r.1).collect();
    let dport: Vec<Option<i64>> = rows.iter().map(|r| r.2).collect();
    let ts: Vec<i64> = rows.iter().map(|r| r.3).collect();
    let wp_src_ip: Vec<Option<&str>> = rows.iter().map(|_r| Some("10.0.0.1")).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(sip)) as ArrayRef,
            Arc::new(StringArray::from(action)) as ArrayRef,
            Arc::new(Int64Array::from(dport)) as ArrayRef,
            Arc::new(Int64Array::from(ts)) as ArrayRef,
            Arc::new(StringArray::from(wp_src_ip)) as ArrayRef,
        ],
    )
    .unwrap()
}

/// Run a batch through both the eager `Event` path and the columnar
/// `ColumnarEvent` path, asserting identical per-row `StepResult` and instance
/// counts, then identical `close_all` / expiry outputs.
fn assert_columnar_matches(
    plan: MatchPlan,
    batch: &RecordBatch,
    aliases: &[&str],
    masks: Option<&GuardMasks>,
) {
    assert_eq!(aliases.len(), batch.num_rows());
    let events = batch_to_events(batch);
    let index = build_field_index(batch);
    let time_col = batch_time_col_index(batch, Some("ts"));
    let mut sm_e = CepStateMachine::new("c".into(), plan.clone(), Some("ts".into()));
    let mut sm_c = CepStateMachine::new("c".into(), plan, Some("ts".into()));

    for row in 0..batch.num_rows() {
        let ts = time_col
            .map(|c| batch_event_time_nanos_at(batch, c, row))
            .unwrap_or(0);
        let r_e = sm_e.advance_at_with_masks(aliases[row], &events[row], ts, None, row, masks);
        let col = ColumnarEvent::with_index(batch, row, Arc::clone(&index));
        let r_c = sm_c.advance_at_with_masks(aliases[row], &col, ts, None, row, masks);
        assert_eq!(r_e, r_c, "row {row} StepResult");
        assert_eq!(
            sm_e.instance_count(),
            sm_c.instance_count(),
            "row {row} instance count"
        );
    }

    // Expiry scan then close-all must agree.
    let watermark = batch
        .column(batch_time_col_index(batch, Some("ts")).unwrap())
        .as_any()
        .downcast_ref::<Int64Array>()
        .map(|a| (0..batch.num_rows()).map(|i| a.value(i)).max().unwrap_or(0))
        .unwrap_or(0);
    assert_eq!(
        sm_e.scan_expired_at(watermark).len(),
        sm_c.scan_expired_at(watermark).len(),
        "scan_expired_at count"
    );
    let close_e = sm_e.close_all(CloseReason::Eos);
    let close_c = sm_c.close_all(CloseReason::Eos);
    assert_eq!(close_e.len(), close_c.len(), "close_all count");
    for (i, (ce, cc)) in close_e.iter().zip(close_c.iter()).enumerate() {
        assert_eq!(ce.close_ok, cc.close_ok, "close {i} close_ok");
        let me = ce.close_step_data.iter().map(|s| s.measure_value).collect::<Vec<_>>();
        let mc = cc.close_step_data.iter().map(|s| s.measure_value).collect::<Vec<_>>();
        assert_eq!(me, mc, "close {i} measures");
    }
}

/// Plan: one guarded event step (`a`) + one guarded close step (`a`). Feeding
/// alias `a` exercises key extraction, the interpreted-guard fallback (when
/// `masks=None`), `extract_branch_field`, `collect_event_fields`, close
/// accumulation, and the `trigger_event` `to_event()` on emit.
fn event_close_plan() -> MatchPlan {
    let mut plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![guarded_branch(
            "a",
            eq_str("sip", "10.0.0.1"),
            count_ge(1.0),
        )])],
    );
    plan.close_steps = vec![step(vec![guarded_branch(
        "a",
        eq_str("action", "blocked"),
        count_ge(1.0),
    )])];
    plan.close_mode = CloseMode::And;
    plan
}

fn event_close_rows() -> Vec<(&'static str, Option<&'static str>, Option<i64>, i64)> {
    vec![
        ("10.0.0.2", None, Some(80), 0),            // event guard false → no instance
        ("10.0.0.1", None, Some(443), 1),           // event guard true → Matched (trigger to_event)
        ("10.0.0.1", Some("blocked"), None, 2),     // close: accumulate (action blocked)
        ("10.0.0.1", Some("login"), None, 3),       // close: explicit false → blocked
        ("10.0.0.1", None, None, 4),                // close: action null → permissive → accumulate
    ]
}

#[test]
fn event_close_columnar_matches_event_path_interpreted() {
    // `masks = None` forces the interpreted `eval_expr_ext` fallback — the
    // `Expr::Field` reads go through `eval_field_value_src` on both sources.
    let plan = event_close_plan();
    let batch = sip_batch(&event_close_rows());
    let aliases: Vec<&str> = vec!["a"; batch.num_rows()];
    assert_columnar_matches(plan, &batch, &aliases, None);
}

#[test]
fn event_close_columnar_matches_event_path_masked() {
    // `masks = Some(branch_guard_masks)` exercises the columnar-mask lookup +
    // columnar field reads composed.
    let plan = event_close_plan();
    let executor = RuleExecutor::new(simple_rule_plan(
        "ev_close",
        plan.clone(),
        Expr::Number(5.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    ));
    let batch = sip_batch(&event_close_rows());
    let masks = executor.branch_guard_masks(&batch);
    let aliases: Vec<&str> = vec!["a"; batch.num_rows()];
    assert_columnar_matches(plan, &batch, &aliases, Some(&masks));
}

#[test]
fn measure_sum_columnar_matches_event_path() {
    // Any-mode plan with a sum(dport) close branch: exercises
    // `extract_branch_field` (field selector), `update_measure(Sum)`,
    // `apply_transforms`, and `collect_event_fields` over the columnar view.
    let mut plan = plan_with_close(
        vec![simple_key("sip")],
        vec![step(vec![branch("a", count_ge(1.0))])],
        vec![step(vec![sum_dport_branch("a", 100.0)])],
        Duration::from_secs(300),
    );
    plan.match_mode = MatchMode::Any;
    let batch = sip_batch(&[
        ("10.0.0.1", None, Some(50), 0),
        ("10.0.0.1", None, Some(30), 1),
        ("10.0.0.1", None, Some(40), 2),
        ("10.0.0.1", None, None, 3), // null dport → skipped by extract_branch_field
    ]);
    let aliases: Vec<&str> = vec!["a"; batch.num_rows()];
    assert_columnar_matches(plan, &batch, &aliases, None);
}

#[test]
fn seq_negation_columnar_matches_event_path() {
    // Chain a → neg c(guard sip=="10.0.0.2") → b. The `c` row (alias `c`)
    // within the window marks the chain violated; `b` is suppressed. Exercises
    // `scan_negations` over the columnar view.
    let mut plan = simple_plan(
        vec![simple_key("sip")],
        vec![
            step(vec![branch("a", count_ge(1.0))]),
            step(vec![branch("b", count_ge(1.0))]),
        ],
    );
    plan.seq = Some(SeqPlan {
        consec: false,
        skip: SeqSkipPlan::PastLast,
        steps: vec![
            SeqStepPlan {
                neg: false,
                within: None,
                branch: branch("a", count_ge(1.0)),
            },
            SeqStepPlan {
                neg: true,
                within: Some(Duration::from_secs(300)),
                branch: guarded_branch("c", eq_str("sip", "10.0.0.2"), count_ge(1.0)),
            },
            SeqStepPlan {
                neg: false,
                within: None,
                branch: branch("b", count_ge(1.0)),
            },
        ],
    });
    let batch = sip_batch(&[
        ("10.0.0.1", None, None, 0), // a
        ("10.0.0.2", None, None, 1), // c (neg, within window)
        ("10.0.0.1", None, None, 2), // b → suppressed by the violation
    ]);
    let aliases = ["a", "c", "b"];
    assert_columnar_matches(plan, &batch, &aliases, None);
}

#[test]
fn columnar_to_event_matches_batch_to_events() {
    // `to_event()` on the columnar view must reproduce the eager materialized
    // Event byte-for-byte (same `extract_field_value` conversions, nulls
    // dropped from the map).
    let batch = sip_batch(&[
        ("10.0.0.1", None, Some(80), 0),
        ("10.0.0.1", Some("blocked"), None, 1),
        ("10.0.0.1", None, Some(443), 2),
    ]);
    let events = batch_to_events(&batch);
    let index = build_field_index(&batch);
    for (row, event) in events.iter().enumerate() {
        let col = ColumnarEvent::with_index(&batch, row, Arc::clone(&index));
        assert_eq!(col.to_event(), *event, "row {row} to_event parity");
    }

    // Projected to_event: with the `materialize_fields` projection, to_event
    // must reproduce `materialize_rows_filtered` exactly (the emit-path trigger
    // event on the deferred columnar path).
    let projection = Arc::new(std::collections::HashSet::from([
        "sip".to_string(),
        "dport".to_string(),
        "ts".to_string(),
    ]));
    let indices: Vec<u32> = (0..batch.num_rows() as u32).collect();
    let projected = materialize_rows_filtered(&batch, &indices, &projection);
    for (row, ev) in projected.iter().enumerate() {
        let col = ColumnarEvent::with_index_projected(
            &batch,
            row,
            Arc::clone(&index),
            Some(Arc::clone(&projection)),
        );
        assert_eq!(col.to_event(), *ev, "row {row} projected to_event parity");
    }
}

/// `Event` and `ColumnarEvent` both implement `FieldSource`; the trait surface
/// itself must agree on a mixed row (null + present + missing).
#[test]
fn fieldsource_field_value_matches_event() {
    let batch = sip_batch(&[("10.0.0.1", None, Some(80), 7)]);
    let event = &batch_to_events(&batch)[0];
    let index = build_field_index(&batch);
    let col = ColumnarEvent::with_index(&batch, 0, Arc::clone(&index));
    assert_eq!(col.field_value("sip"), event.field_value("sip"));
    assert_eq!(col.field_value("action"), event.field_value("action")); // null → None
    assert_eq!(col.field_value("dport"), event.field_value("dport"));
    assert_eq!(col.field_value_str("sip"), event.field_value_str("sip"));
    assert_eq!(col.field_value("missing"), event.field_value("missing"));
}
