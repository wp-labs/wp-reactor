//! Columnar branch-guard wiring for close-step accumulation guards and seq
//! negation guards (the "branch guard 收尾" complement to event-step guards).
//!
//! Event steps use "must be true" semantics (null → false); close steps use
//! **permissive** semantics (only an explicit `false` blocks, null passes); seq
//! negation steps use "must be true" semantics. The differential tests feed the
//! same events through `advance_at_with_masks` (columnar) and `advance_at_with`
//! (interpreted) and assert identical state-machine results.
use std::sync::Arc;

use std::time::Duration;

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use wf_lang::ast::{BinOp, CloseMode, Expr, FieldRef};
use wf_lang::plan::{AggPlan, BranchPlan, MatchPlan, SeqPlan, SeqSkipPlan, SeqStepPlan};

use crate::match_engine::RuleExecutor;
use crate::match_engine::cep::{CepStateMachine, CloseReason, StepResult};

use super::helpers::{
    branch, count_ge, event, plan_with_close, simple_key, simple_plan, simple_rule_plan, step,
    str_val,
};

fn eq_str(field: &str, val: &str) -> Expr {
    Expr::BinOp {
        op: BinOp::Eq,
        left: Box::new(Expr::Field(FieldRef::Simple(field.to_string()))),
        right: Box::new(Expr::StringLit(val.to_string())),
    }
}

fn guarded_branch(source: &str, guard: Expr, agg: AggPlan) -> BranchPlan {
    BranchPlan {
        label: None,
        source: source.to_string(),
        field: None,
        guard: Some(guard),
        agg,
    }
}

/// Plan with an event guard, a close-step guard (permissive), and a seq
/// negation guard. Event steps are the two use steps `a` → `b`; `c` is the
/// negation step in the chain.
fn close_neg_plan() -> MatchPlan {
    let mut plan = simple_plan(
        vec![simple_key("sip")],
        vec![
            step(vec![guarded_branch(
                "a",
                eq_str("sip", "10.0.0.1"),
                count_ge(1.0),
            )]),
            step(vec![branch("b", count_ge(1.0))]),
        ],
    );
    plan.close_steps = vec![step(vec![guarded_branch(
        "resp",
        eq_str("action", "blocked"),
        count_ge(1.0),
    )])];
    plan.close_mode = CloseMode::And;
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
    plan
}

fn sip_action_batch(rows: Vec<(&str, Option<&str>)>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("sip", DataType::Utf8, true),
        Field::new("action", DataType::Utf8, true),
    ]));
    let sip: Vec<Option<&str>> = rows.iter().map(|(s, _)| Some(*s)).collect();
    let action: Vec<Option<&str>> = rows.iter().map(|(_, a)| *a).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(sip)) as ArrayRef,
            Arc::new(StringArray::from(action)) as ArrayRef,
        ],
    )
    .unwrap()
}

#[test]
fn branch_guard_masks_covers_close_and_neg() {
    let plan = close_neg_plan();
    let executor = RuleExecutor::new(simple_rule_plan(
        "close_neg_col",
        plan,
        Expr::Number(5.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    ));

    let batch = sip_action_batch(vec![
        ("10.0.0.1", None),            // event guard true; close action null → permissive
        ("10.0.0.2", Some("login")),   // neg guard true; close action != "blocked" → false
        ("10.0.0.3", Some("blocked")), // event/neg false; close action == "blocked" → true
    ]);
    let masks = executor.branch_guard_masks(&batch);

    // Event guard (sip == "10.0.0.1"), must-be-true.
    assert_eq!(masks.event_value(0, 0, 0), Some(true));
    assert_eq!(masks.event_value(0, 0, 1), Some(false));
    assert_eq!(masks.event_value(0, 0, 2), Some(false));

    // Close guard (action == "blocked"), permissive null-aware.
    assert_eq!(masks.close_value(0, 0, 0), Some(None)); // null → permissive
    assert_eq!(masks.close_value(0, 0, 1), Some(Some(false)));
    assert_eq!(masks.close_value(0, 0, 2), Some(Some(true)));

    // Neg guard (sip == "10.0.0.2"), must-be-true.
    assert_eq!(masks.neg_value(0, 0, 0), Some(false));
    assert_eq!(masks.neg_value(0, 0, 1), Some(true));
    assert_eq!(masks.neg_value(0, 0, 2), Some(false));
}

#[test]
fn close_step_guard_columnar_matches_interpreted() {
    // Close step `resp` count>=2 guarded on `action == "blocked"` (permissive):
    // only an explicit false (action present and != "blocked") blocks.
    let plan = plan_with_close(
        vec![simple_key("sip")],
        vec![step(vec![branch("req", count_ge(1.0))])],
        vec![step(vec![guarded_branch(
            "resp",
            eq_str("action", "blocked"),
            count_ge(2.0),
        )])],
        Duration::from_secs(300),
    );
    let executor = RuleExecutor::new(simple_rule_plan(
        "close_col",
        plan.clone(),
        Expr::Number(5.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    ));
    let batch = sip_action_batch(vec![
        ("10.0.0.1", None),            // req: action null (irrelevant)
        ("10.0.0.1", Some("blocked")), // resp: accumulate (count 1)
        ("10.0.0.1", Some("login")),   // resp: explicit false → block
        ("10.0.0.1", None),            // resp: action null → permissive → accumulate (count 2)
    ]);
    let masks = executor.branch_guard_masks(&batch);

    let req = event(vec![("sip", str_val("10.0.0.1"))]);
    let resp_blocked = event(vec![
        ("sip", str_val("10.0.0.1")),
        ("action", str_val("blocked")),
    ]);
    let resp_login = event(vec![
        ("sip", str_val("10.0.0.1")),
        ("action", str_val("login")),
    ]);
    let resp_missing = event(vec![("sip", str_val("10.0.0.1"))]);

    let mut masked = CepStateMachine::new("close_col".into(), plan.clone(), None);
    let mut plain = CepStateMachine::new("close_col".into(), plan, None);

    assert_eq!(
        masked.advance_at_with_masks("req", &req, 0, None, 0, Some(&masks)),
        StepResult::Advance
    );
    assert_eq!(
        plain.advance_at_with("req", &req, 0, None),
        StepResult::Advance
    );

    for (row, e) in [
        (1usize, &resp_blocked),
        (2, &resp_login),
        (3, &resp_missing),
    ] {
        assert_eq!(
            masked.advance_at_with_masks("resp", e, 0, None, row, Some(&masks)),
            plain.advance_at_with("resp", e, 0, None),
            "resp row {row}"
        );
    }

    let out_m = masked
        .close(&[str_val("10.0.0.1")], CloseReason::Timeout)
        .unwrap();
    let out_p = plain
        .close(&[str_val("10.0.0.1")], CloseReason::Timeout)
        .unwrap();
    assert_eq!(out_m.close_ok, out_p.close_ok);
    assert_eq!(
        out_m.close_step_data[0].measure_value,
        out_p.close_step_data[0].measure_value
    );
    // count == 2 (blocked + missing) >= 2.
    assert!(out_m.close_ok);
    assert_eq!(out_m.close_step_data[0].measure_value, 2.0);
}

#[test]
fn negation_guard_columnar_matches_interpreted() {
    // Chain: a(use) → c(neg, guard action=="blocked") → b(use). All events share
    // scope key sip=10.0.0.1 (the neg guard is on `action`, not the key), so a
    // matching `c` within the window violates the chain and suppresses `b`.
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
                branch: guarded_branch("c", eq_str("action", "blocked"), count_ge(1.0)),
            },
            SeqStepPlan {
                neg: false,
                within: None,
                branch: branch("b", count_ge(1.0)),
            },
        ],
    });

    let executor = RuleExecutor::new(simple_rule_plan(
        "neg_col",
        plan.clone(),
        Expr::Number(5.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    ));
    let batch = sip_action_batch(vec![
        ("10.0.0.1", Some("login")),   // a
        ("10.0.0.1", Some("blocked")), // c → neg guard true (violation)
        ("10.0.0.1", Some("login")),   // b
    ]);
    let masks = executor.branch_guard_masks(&batch);
    assert_eq!(masks.neg_value(0, 0, 1), Some(true), "neg mask for c@row1");

    let e_a = event(vec![
        ("sip", str_val("10.0.0.1")),
        ("action", str_val("login")),
    ]);
    let e_c = event(vec![
        ("sip", str_val("10.0.0.1")),
        ("action", str_val("blocked")),
    ]);
    let e_b = event(vec![
        ("sip", str_val("10.0.0.1")),
        ("action", str_val("login")),
    ]);

    let mut masked = CepStateMachine::new("neg_col".into(), plan.clone(), None);
    let mut plain = CepStateMachine::new("neg_col".into(), plan, None);

    assert_eq!(
        masked.advance_at_with_masks("a", &e_a, 0, None, 0, Some(&masks)),
        StepResult::Advance
    );
    assert_eq!(
        plain.advance_at_with("a", &e_a, 0, None),
        StepResult::Advance
    );

    assert_eq!(
        masked.advance_at_with_masks("c", &e_c, 60_000_000_000, None, 1, Some(&masks)),
        plain.advance_at_with("c", &e_c, 60_000_000_000, None)
    );
    // `b` must be suppressed by the negation violation in both paths.
    assert_eq!(
        masked.advance_at_with_masks("b", &e_b, 120_000_000_000, None, 2, Some(&masks)),
        StepResult::Accumulate
    );
    assert_eq!(
        plain.advance_at_with("b", &e_b, 120_000_000_000, None),
        StepResult::Accumulate
    );
}
