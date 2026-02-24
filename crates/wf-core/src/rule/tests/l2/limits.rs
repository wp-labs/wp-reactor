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

// ===========================================================================
// Limits: max_state_bytes with Throttle
// ===========================================================================

#[test]
fn limits_max_state_bytes_throttle() {
    // Each instance: ~128 base + ~32 key ("10.0.0.x" = 8 chars + 24) + 80 branch ≈ 240 bytes
    // Two instances ≈ 480 bytes. Set limit to 450 so 2 instances exceed threshold
    // and the 3rd new-key event is throttled.
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(2.0))])],
    );
    let limits = LimitsPlan {
        max_state_bytes: Some(450),
        max_cardinality: None,
        max_emit_rate: None,
        on_exceed: ExceedAction::Throttle,
    };
    let mut sm = CepStateMachine::with_limits("rule_state".to_string(), plan, None, Some(limits));

    let e1 = event(vec![("sip", str_val("10.0.0.1"))]);
    let e2 = event(vec![("sip", str_val("10.0.0.2"))]);
    let e3 = event(vec![("sip", str_val("10.0.0.3"))]);

    // First two keys create instances — total estimated ~484 bytes
    assert_eq!(sm.advance("fail", &e1), StepResult::Accumulate);
    assert_eq!(sm.advance("fail", &e2), StepResult::Accumulate);
    assert_eq!(sm.instance_count(), 2);

    // Third key is throttled — total exceeds max_state_bytes
    assert_eq!(sm.advance("fail", &e3), StepResult::Accumulate);
    assert_eq!(sm.instance_count(), 2);

    // Existing keys still advance normally
    assert!(matches!(sm.advance("fail", &e1), StepResult::Matched(_)));
}

// ===========================================================================
// Limits: max_emit_rate with Throttle
// ===========================================================================

#[test]
fn limits_max_emit_rate_throttle() {
    // count >= 1 so every event for a key triggers a match
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(1.0))])],
    );
    let limits = LimitsPlan {
        max_state_bytes: None,
        max_cardinality: None,
        max_emit_rate: Some(RateSpec {
            count: 2,
            per: Duration::from_secs(60),
        }),
        on_exceed: ExceedAction::Throttle,
    };
    let mut sm = CepStateMachine::with_limits("rule_rate".to_string(), plan, None, Some(limits));

    let e1 = event(vec![("sip", str_val("10.0.0.1"))]);

    // First two matches succeed (within rate limit)
    assert!(matches!(
        sm.advance_at("fail", &e1, 1_000_000_000),
        StepResult::Matched(_)
    ));
    assert!(matches!(
        sm.advance_at("fail", &e1, 2_000_000_000),
        StepResult::Matched(_)
    ));

    // Third match is throttled — rate limit reached
    assert_eq!(
        sm.advance_at("fail", &e1, 3_000_000_000),
        StepResult::Accumulate
    );
}

// ===========================================================================
// Limits: max_emit_rate window reset
// ===========================================================================

#[test]
fn limits_max_emit_rate_resets_window() {
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(1.0))])],
    );
    let limits = LimitsPlan {
        max_state_bytes: None,
        max_cardinality: None,
        max_emit_rate: Some(RateSpec {
            count: 1,
            per: Duration::from_secs(10),
        }),
        on_exceed: ExceedAction::Throttle,
    };
    let mut sm =
        CepStateMachine::with_limits("rule_rate_reset".to_string(), plan, None, Some(limits));

    let e1 = event(vec![("sip", str_val("10.0.0.1"))]);

    // First match at t=1s succeeds
    assert!(matches!(
        sm.advance_at("fail", &e1, 1_000_000_000),
        StepResult::Matched(_)
    ));

    // Second match at t=5s is throttled (within the 10s window)
    assert_eq!(
        sm.advance_at("fail", &e1, 5_000_000_000),
        StepResult::Accumulate
    );

    // Advance time past the 10s window boundary (t=12s) — counter should reset
    assert!(matches!(
        sm.advance_at("fail", &e1, 12_000_000_000),
        StepResult::Matched(_)
    ));
}
