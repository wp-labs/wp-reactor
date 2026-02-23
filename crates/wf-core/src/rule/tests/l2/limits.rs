use super::*;

// ===========================================================================
// Limits: max_cardinality with Throttle
// ===========================================================================

#[test]
fn limits_max_cardinality_throttle() {
    // Use count >= 2 so instances stay alive after the first event
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(2.0))])],
    );
    let limits = LimitsPlan {
        max_state_bytes: None,
        max_cardinality: Some(2),
        max_emit_rate: None,
        on_exceed: ExceedAction::Throttle,
    };
    let mut sm = CepStateMachine::with_limits("rule_lim".to_string(), plan, None, Some(limits));

    let e1 = event(vec![("sip", str_val("10.0.0.1"))]);
    let e2 = event(vec![("sip", str_val("10.0.0.2"))]);
    let e3 = event(vec![("sip", str_val("10.0.0.3"))]);

    // First two keys create instances
    assert_eq!(sm.advance("fail", &e1), StepResult::Accumulate);
    assert_eq!(sm.advance("fail", &e2), StepResult::Accumulate);
    assert_eq!(sm.instance_count(), 2);

    // Third key is throttled — max_cardinality reached
    assert_eq!(sm.advance("fail", &e3), StepResult::Accumulate);
    assert_eq!(sm.instance_count(), 2);

    // Existing keys still advance normally
    assert!(matches!(sm.advance("fail", &e1), StepResult::Matched(_)));
}

// ===========================================================================
// Limits: max_cardinality with DropOldest
// ===========================================================================

#[test]
fn limits_max_cardinality_drop_oldest() {
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(3.0))])],
    );
    let limits = LimitsPlan {
        max_state_bytes: None,
        max_cardinality: Some(2),
        max_emit_rate: None,
        on_exceed: ExceedAction::DropOldest,
    };
    let mut sm = CepStateMachine::with_limits("rule_lim".to_string(), plan, None, Some(limits));

    let e1 = event(vec![("sip", str_val("10.0.0.1"))]);
    let e2 = event(vec![("sip", str_val("10.0.0.2"))]);
    let e3 = event(vec![("sip", str_val("10.0.0.3"))]);

    // Create 2 instances at different timestamps
    assert_eq!(sm.advance_at("fail", &e1, 100), StepResult::Accumulate);
    assert_eq!(sm.advance_at("fail", &e2, 200), StepResult::Accumulate);
    assert_eq!(sm.instance_count(), 2);

    // Third key evicts the oldest (10.0.0.1, created at t=100)
    assert_eq!(sm.advance_at("fail", &e3, 300), StepResult::Accumulate);
    assert_eq!(sm.instance_count(), 2);

    // 10.0.0.1 was evicted — re-inserting it evicts the next oldest (10.0.0.2)
    assert_eq!(sm.advance_at("fail", &e1, 400), StepResult::Accumulate);
    assert_eq!(sm.instance_count(), 2);
}
