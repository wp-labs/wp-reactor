use super::*;

// ---------------------------------------------------------------------------
// String min/max + compound threshold (26–27)
// ---------------------------------------------------------------------------

#[test]
fn string_min_max() {
    // min(hostname) >= "beta" with string values
    let agg = AggPlan {
        transforms: vec![],
        measure: Measure::Min,
        cmp: CmpOp::Ge,
        threshold: Expr::StringLit("beta".to_string()),
    };
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![BranchPlan {
            label: None,
            source: "dns".to_string(),
            field: Some(FieldSelector::Dot("hostname".to_string())),
            guard: None,
            agg,
        }])],
    );
    let mut sm = CepStateMachine::new("rule26".to_string(), plan, None);

    let mk = |h: &str| event(vec![("sip", str_val("10.0.0.1")), ("hostname", str_val(h))]);

    // "alpha" < "beta" → min="alpha", min >= "beta" is false
    assert_eq!(sm.advance("dns", &mk("alpha")), StepResult::Accumulate);
    // "gamma" → min stays "alpha", still < "beta"
    assert_eq!(sm.advance("dns", &mk("gamma")), StepResult::Accumulate);
    // "delta" → min stays "alpha", still < "beta"
    assert_eq!(sm.advance("dns", &mk("delta")), StepResult::Accumulate);

    // Now test max with same data
    let agg_max = AggPlan {
        transforms: vec![],
        measure: Measure::Max,
        cmp: CmpOp::Ge,
        threshold: Expr::StringLit("delta".to_string()),
    };
    let plan2 = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![BranchPlan {
            label: None,
            source: "dns".to_string(),
            field: Some(FieldSelector::Dot("hostname".to_string())),
            guard: None,
            agg: agg_max,
        }])],
    );
    let mut sm2 = CepStateMachine::new("rule26b".to_string(), plan2, None);

    // "alpha" → max="alpha", max >= "delta" is false
    assert_eq!(sm2.advance("dns", &mk("alpha")), StepResult::Accumulate);
    // "delta" → max="delta", max >= "delta" is true → matched
    assert!(matches!(
        sm2.advance("dns", &mk("delta")),
        StepResult::Matched(_)
    ));
}

#[test]
fn compound_threshold() {
    // count >= (2 + 1) should require 3 events, not fire at 0 (old buggy behavior)
    let agg = AggPlan {
        transforms: vec![],
        measure: Measure::Count,
        cmp: CmpOp::Ge,
        threshold: Expr::BinOp {
            op: BinOp::Add,
            left: Box::new(Expr::Number(2.0)),
            right: Box::new(Expr::Number(1.0)),
        },
    };
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![BranchPlan {
            label: None,
            source: "fail".to_string(),
            field: None,
            guard: None,
            agg,
        }])],
    );
    let mut sm = CepStateMachine::new("rule27".to_string(), plan, None);
    let e = event(vec![("sip", str_val("10.0.0.1"))]);

    // Should NOT match on first event (old bug: threshold=0.0 → immediate match)
    assert_eq!(sm.advance("fail", &e), StepResult::Accumulate);
    assert_eq!(sm.advance("fail", &e), StepResult::Accumulate);

    // Third event → count=3 >= 3 → matched
    if let StepResult::Matched(ctx) = sm.advance("fail", &e) {
        assert_eq!(ctx.step_data[0].measure_value, 3.0);
    } else {
        panic!("expected Matched at count=3");
    }
}
