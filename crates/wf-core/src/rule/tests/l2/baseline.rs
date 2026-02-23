use super::*;

// ---------------------------------------------------------------------------
// baseline() stateful evaluation — Issue #8
// ---------------------------------------------------------------------------

/// baseline() must accumulate state across events in the same instance.
/// After feeding a series of events, the deviation should change (not be 0).
#[test]
fn baseline_accumulates_state_in_guard() {
    use wf_lang::ast::{BinOp, CmpOp, Expr, FieldRef, FieldSelector, Measure};
    use wf_lang::plan::{AggPlan, BranchPlan};

    // Guard: baseline(fail.score, 300) > 2.0
    let guard = Expr::BinOp {
        op: BinOp::Gt,
        left: Box::new(Expr::FuncCall {
            qualifier: None,
            name: "baseline".to_string(),
            args: vec![
                Expr::Field(FieldRef::Simple("score".to_string())),
                Expr::Number(300.0),
            ],
        }),
        right: Box::new(Expr::Number(2.0)),
    };

    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![BranchPlan {
            label: None,
            source: "fail".to_string(),
            field: Some(FieldSelector::Dot("score".to_string())),
            guard: Some(guard),
            agg: AggPlan {
                transforms: vec![],
                measure: Measure::Count,
                cmp: CmpOp::Ge,
                threshold: Expr::Number(1.0),
            },
        }])],
    );

    let mut sm = CepStateMachine::new("baseline_test".into(), plan, None);

    // Feed events with varying scores around 50 (need variance for stddev > 0).
    // Alternating 45 and 55 gives mean=50, stddev=5.
    for i in 0..20 {
        let score = if i % 2 == 0 { 45.0 } else { 55.0 };
        let e = event(vec![("sip", str_val("10.0.0.1")), ("score", num(score))]);
        sm.advance("fail", &e);
    }

    // Now feed an outlier: score=200. mean≈50, stddev≈5,
    // deviation = (200-50)/5 = 30 which is >> 2.0. Guard should pass → match.
    let outlier = event(vec![("sip", str_val("10.0.0.1")), ("score", num(200.0))]);
    let result = sm.advance("fail", &outlier);
    assert!(
        matches!(result, StepResult::Matched(_)),
        "outlier should trigger baseline guard; got {:?}",
        result
    );
}

/// baseline() should return 0.0 on the first event (no history yet).
#[test]
fn baseline_returns_zero_on_first_event() {
    use crate::rule::match_engine::eval_expr;

    let expr = Expr::FuncCall {
        qualifier: None,
        name: "baseline".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("x".to_string())),
            Expr::Number(300.0),
        ],
    };
    let e = event(vec![("x", num(100.0))]);
    // eval_expr uses a temp baselines map — first call should return 0.0
    let result = eval_expr(&expr, &e);
    assert_eq!(result, Some(Value::Number(0.0)));
}
