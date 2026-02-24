use super::*;

// ---------------------------------------------------------------------------
// Close step min/max + threshold safety (32–34)
// ---------------------------------------------------------------------------

#[test]
fn close_step_string_min_max() {
    // Close step: min(hostname) >= "beta"
    // Verifies value-based comparison works in close path (not f64 ±INF).
    let agg_min = AggPlan {
        transforms: vec![],
        measure: Measure::Min,
        cmp: CmpOp::Ge,
        threshold: Expr::StringLit("beta".to_string()),
    };
    let plan = plan_with_close(
        vec![simple_key("sip")],
        vec![step(vec![branch("req", count_ge(1.0))])],
        vec![step(vec![BranchPlan {
            label: None,
            source: "dns".to_string(),
            field: Some(FieldSelector::Dot("hostname".to_string())),
            guard: None,
            agg: agg_min,
        }])],
        Duration::from_secs(60),
    );
    let base: i64 = 1_700_000_000 * NANOS_PER_SEC;

    // Scenario A: min="alpha" < "beta" → close_ok=false
    let mut sm_a = CepStateMachine::new("rule32a".to_string(), plan.clone(), None);
    let req = event(vec![("sip", str_val("10.0.0.1"))]);
    sm_a.advance_at("req", &req, base);

    let mk_dns = |h: &str| event(vec![("sip", str_val("10.0.0.1")), ("hostname", str_val(h))]);
    sm_a.advance_at("dns", &mk_dns("gamma"), base);
    sm_a.advance_at("dns", &mk_dns("alpha"), base); // min becomes "alpha"
    sm_a.advance_at("dns", &mk_dns("delta"), base);

    let out_a = sm_a
        .close(&[str_val("10.0.0.1")], CloseReason::Timeout)
        .unwrap();
    assert!(out_a.event_ok);
    assert!(!out_a.close_ok); // min="alpha" < "beta" → not satisfied

    // Scenario B: min="beta" >= "beta" → close_ok=true
    let mut sm_b = CepStateMachine::new("rule32b".to_string(), plan.clone(), None);
    sm_b.advance_at("req", &req, base);
    sm_b.advance_at("dns", &mk_dns("gamma"), base);
    sm_b.advance_at("dns", &mk_dns("beta"), base); // min becomes "beta"

    let out_b = sm_b
        .close(&[str_val("10.0.0.1")], CloseReason::Timeout)
        .unwrap();
    assert!(out_b.event_ok);
    assert!(out_b.close_ok); // min="beta" >= "beta" → satisfied

    // Scenario C: max(hostname) >= "delta" on close step
    let agg_max = AggPlan {
        transforms: vec![],
        measure: Measure::Max,
        cmp: CmpOp::Ge,
        threshold: Expr::StringLit("delta".to_string()),
    };
    let plan_max = plan_with_close(
        vec![simple_key("sip")],
        vec![step(vec![branch("req", count_ge(1.0))])],
        vec![step(vec![BranchPlan {
            label: None,
            source: "dns".to_string(),
            field: Some(FieldSelector::Dot("hostname".to_string())),
            guard: None,
            agg: agg_max,
        }])],
        Duration::from_secs(60),
    );
    let mut sm_c = CepStateMachine::new("rule32c".to_string(), plan_max, None);
    sm_c.advance_at("req", &req, base);
    sm_c.advance_at("dns", &mk_dns("alpha"), base);
    sm_c.advance_at("dns", &mk_dns("gamma"), base); // max becomes "gamma"

    let out_c = sm_c
        .close(&[str_val("10.0.0.1")], CloseReason::Timeout)
        .unwrap();
    assert!(out_c.event_ok);
    assert!(out_c.close_ok); // max="gamma" >= "delta" → true
}

#[test]
fn non_constant_threshold_no_silent_zero() {
    // Threshold is a field ref expression (non-constant).
    // Previously this would silently evaluate to 0.0, causing false positives
    // when count >= 0 (always true after any event).
    // Now try_eval_expr_to_f64 returns None → check_threshold returns false.
    let agg = AggPlan {
        transforms: vec![],
        measure: Measure::Count,
        cmp: CmpOp::Ge,
        // Threshold is a field reference — not a constant
        threshold: Expr::Field(FieldRef::Simple("limit".to_string())),
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
    let mut sm = CepStateMachine::new("rule33".to_string(), plan, None);

    // Even after many events, the threshold can't be resolved, so it never matches
    let e = event(vec![("sip", str_val("10.0.0.1")), ("limit", num(5.0))]);
    for _ in 0..10 {
        assert_eq!(sm.advance("fail", &e), StepResult::Accumulate);
    }
    // Should never fire — threshold is not a static constant
}

#[test]
fn min_max_non_constant_threshold_no_false_positive() {
    // min(hostname) > field_ref — threshold is a non-constant expression.
    // Previously eval_expr_to_value returned Value::Str(""), which could
    // produce false positives via cross-type ordering (Number < Str < Bool).
    // Now try_eval_expr_to_value returns None → check_threshold returns false.

    // Event-step path
    let agg_min = AggPlan {
        transforms: vec![],
        measure: Measure::Min,
        cmp: CmpOp::Gt,
        threshold: Expr::Field(FieldRef::Simple("baseline".to_string())),
    };
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![BranchPlan {
            label: None,
            source: "dns".to_string(),
            field: Some(FieldSelector::Dot("hostname".to_string())),
            guard: None,
            agg: agg_min,
        }])],
    );
    let mut sm = CepStateMachine::new("rule34a".to_string(), plan, None);

    let e = event(vec![
        ("sip", str_val("10.0.0.1")),
        ("hostname", str_val("zebra")),
        ("baseline", str_val("alpha")),
    ]);
    // Even though "zebra" > "alpha" in string ordering, the threshold is a
    // field ref and cannot be statically resolved → must not fire.
    for _ in 0..5 {
        assert_eq!(sm.advance("dns", &e), StepResult::Accumulate);
    }

    // Close-step path
    let agg_max = AggPlan {
        transforms: vec![],
        measure: Measure::Max,
        cmp: CmpOp::Lt,
        threshold: Expr::Field(FieldRef::Simple("upper_bound".to_string())),
    };
    let plan_close = plan_with_close(
        vec![simple_key("sip")],
        vec![step(vec![branch("req", count_ge(1.0))])],
        vec![step(vec![BranchPlan {
            label: None,
            source: "dns".to_string(),
            field: Some(FieldSelector::Dot("hostname".to_string())),
            guard: None,
            agg: agg_max,
        }])],
        Duration::from_secs(60),
    );
    let mut sm2 = CepStateMachine::new("rule34b".to_string(), plan_close, None);
    let base: i64 = 1_700_000_000 * NANOS_PER_SEC;
    let req = event(vec![("sip", str_val("10.0.0.1"))]);
    sm2.advance_at("req", &req, base);

    let dns_ev = event(vec![
        ("sip", str_val("10.0.0.1")),
        ("hostname", str_val("alpha")),
    ]);
    sm2.advance_at("dns", &dns_ev, base);

    let out = sm2
        .close(&[str_val("10.0.0.1")], CloseReason::Timeout)
        .unwrap();
    assert!(out.event_ok);
    assert!(!out.close_ok); // threshold is non-constant → must not satisfy
}

// ---------------------------------------------------------------------------
// Cross-type min/max threshold rejection (35)
// ---------------------------------------------------------------------------

#[test]
fn min_max_cross_type_threshold_rejected() {
    // min(hostname) > 1 — hostname is Str, threshold is Number.
    // Cross-type comparison must return false, not use arbitrary ordering.
    let agg = AggPlan {
        transforms: vec![],
        measure: Measure::Min,
        cmp: CmpOp::Gt,
        threshold: Expr::Number(1.0),
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
    let mut sm = CepStateMachine::new("rule35a".to_string(), plan, None);

    // String values — old behavior: Str > Number in cross-type ordering → false positive
    let e = event(vec![
        ("sip", str_val("10.0.0.1")),
        ("hostname", str_val("alpha")),
    ]);
    for _ in 0..5 {
        assert_eq!(sm.advance("dns", &e), StepResult::Accumulate);
    }

    // max(score) > "high" — score is Number, threshold is Str.
    let agg_max = AggPlan {
        transforms: vec![],
        measure: Measure::Max,
        cmp: CmpOp::Gt,
        threshold: Expr::StringLit("high".to_string()),
    };
    let plan2 = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![BranchPlan {
            label: None,
            source: "alert".to_string(),
            field: Some(FieldSelector::Dot("score".to_string())),
            guard: None,
            agg: agg_max,
        }])],
    );
    let mut sm2 = CepStateMachine::new("rule35b".to_string(), plan2, None);

    let e2 = event(vec![("sip", str_val("10.0.0.1")), ("score", num(999.0))]);
    for _ in 0..5 {
        assert_eq!(sm2.advance("alert", &e2), StepResult::Accumulate);
    }
}
