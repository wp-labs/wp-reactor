use super::*;

// ===========================================================================
// Fixed: events in the same bucket share a single instance
// ===========================================================================

#[test]
fn fixed_same_bucket_single_instance() {
    let dur = Duration::from_secs(10);
    let plan = fixed_plan(
        vec![simple_key("sip")],
        dur,
        vec![step(vec![branch("fail", count_ge(5.0))])],
    );
    let mut sm = CepStateMachine::new("r_fixed".to_string(), plan, None);

    let e = event(vec![("sip", str_val("10.0.0.1"))]);

    // All three events at t=1s, t=3s, t=7s fall in bucket [0, 10s)
    sm.advance_at("fail", &e, 1_000_000_000);
    sm.advance_at("fail", &e, 3_000_000_000);
    sm.advance_at("fail", &e, 7_000_000_000);

    assert_eq!(sm.instance_count(), 1);
}

// ===========================================================================
// Fixed: events in different buckets create separate instances
// ===========================================================================

#[test]
fn fixed_different_buckets_separate_instances() {
    let dur = Duration::from_secs(10);
    let plan = fixed_plan(
        vec![simple_key("sip")],
        dur,
        vec![step(vec![branch("fail", count_ge(5.0))])],
    );
    let mut sm = CepStateMachine::new("r_fixed".to_string(), plan, None);

    let e = event(vec![("sip", str_val("10.0.0.1"))]);

    // t=5s → bucket [0, 10s), t=15s → bucket [10s, 20s)
    sm.advance_at("fail", &e, 5_000_000_000);
    sm.advance_at("fail", &e, 15_000_000_000);

    assert_eq!(sm.instance_count(), 2);
}

// ===========================================================================
// Fixed: bucket expires when watermark reaches bucket_start + duration
// ===========================================================================

#[test]
fn fixed_bucket_expiry() {
    let dur = Duration::from_secs(10);
    let plan = fixed_plan(
        vec![simple_key("sip")],
        dur,
        vec![step(vec![branch("fail", count_ge(5.0))])],
    );
    let mut sm = CepStateMachine::new("r_fixed".to_string(), plan, None);

    let e = event(vec![("sip", str_val("10.0.0.1"))]);

    // Feed event at t=3s → bucket [0, 10s) with created_at=0
    sm.advance_at("fail", &e, 3_000_000_000);
    assert_eq!(sm.instance_count(), 1);

    // Watermark at 9s: not expired yet (9 - 0 = 9 < 10)
    let expired = sm.scan_expired_at(9_000_000_000);
    assert!(expired.is_empty());
    assert_eq!(sm.instance_count(), 1);

    // Watermark at 10s: expired (10 - 0 = 10 >= 10)
    let expired = sm.scan_expired_at(10_000_000_000);
    assert_eq!(expired.len(), 1);
    assert_eq!(sm.instance_count(), 0);
}

// ===========================================================================
// Fixed: on close fires at expiry
// ===========================================================================

#[test]
fn fixed_on_close_fires_at_expiry() {
    let dur = Duration::from_secs(10);
    let plan = fixed_plan_with_close(
        vec![simple_key("sip")],
        dur,
        // on event: count >= 1 (satisfied immediately)
        vec![step(vec![branch("fail", count_ge(1.0))])],
        // on close: count >= 1 (satisfied if any events accumulated)
        vec![step(vec![branch("fail", count_ge(1.0))])],
    );
    let mut sm = CepStateMachine::new("r_fixed_close".to_string(), plan, None);

    let e = event(vec![("sip", str_val("10.0.0.1"))]);

    // Feed 3 events into bucket [0, 10s)
    sm.advance_at("fail", &e, 1_000_000_000);
    sm.advance_at("fail", &e, 2_000_000_000);
    sm.advance_at("fail", &e, 5_000_000_000);

    assert_eq!(sm.instance_count(), 1);

    // Expire at watermark = 10s
    let expired = sm.scan_expired_at(10_000_000_000);
    assert_eq!(expired.len(), 1);
    assert!(expired[0].event_ok);
    assert!(expired[0].close_ok);
    assert_eq!(sm.instance_count(), 0);
}

// ===========================================================================
// Fixed: match without close steps (on event only)
// ===========================================================================

#[test]
fn fixed_match_no_close() {
    let dur = Duration::from_secs(10);
    let plan = fixed_plan(
        vec![simple_key("sip")],
        dur,
        // on event: count >= 3
        vec![step(vec![branch("fail", count_ge(3.0))])],
    );
    let mut sm = CepStateMachine::new("r_fixed_evt".to_string(), plan, None);

    let e = event(vec![("sip", str_val("10.0.0.1"))]);

    // Events in bucket [0, 10s)
    assert_eq!(
        sm.advance_at("fail", &e, 1_000_000_000),
        StepResult::Accumulate
    );
    assert_eq!(
        sm.advance_at("fail", &e, 2_000_000_000),
        StepResult::Accumulate
    );
    // Third event satisfies count >= 3 → Matched
    assert!(matches!(
        sm.advance_at("fail", &e, 3_000_000_000),
        StepResult::Matched(_)
    ));
}

// ===========================================================================
// Fixed: same scope key in different buckets accumulates independently
// ===========================================================================

#[test]
fn fixed_independent_bucket_aggregation() {
    let dur = Duration::from_secs(10);
    let plan = fixed_plan(
        vec![simple_key("sip")],
        dur,
        // on event: count >= 3
        vec![step(vec![branch("fail", count_ge(3.0))])],
    );
    let mut sm = CepStateMachine::new("r_fixed_indep".to_string(), plan, None);

    let e = event(vec![("sip", str_val("10.0.0.1"))]);

    // 2 events in bucket [0, 10s) — not enough to match
    sm.advance_at("fail", &e, 1_000_000_000);
    sm.advance_at("fail", &e, 5_000_000_000);

    // 1 event in bucket [10s, 20s) — should NOT carry over count from previous bucket
    assert_eq!(
        sm.advance_at("fail", &e, 12_000_000_000),
        StepResult::Accumulate
    );

    // Two separate instances exist
    assert_eq!(sm.instance_count(), 2);
}

// ===========================================================================
// Fixed: after match+reset, created_at stays at bucket_start (not event time)
// so the instance expires correctly at bucket_start + duration.
// ===========================================================================

#[test]
fn fixed_reset_preserves_bucket_start() {
    let dur = Duration::from_secs(10);
    let plan = fixed_plan(
        vec![simple_key("sip")],
        dur,
        // count >= 2 — will match after two events
        vec![step(vec![branch("fail", count_ge(2.0))])],
    );
    let mut sm = CepStateMachine::new("r_fixed_reset".to_string(), plan, None);

    let e = event(vec![("sip", str_val("10.0.0.1"))]);

    // Two events at t=1s, t=2s in bucket [0, 10s) → match + reset
    sm.advance_at("fail", &e, 1_000_000_000);
    assert!(matches!(
        sm.advance_at("fail", &e, 2_000_000_000),
        StepResult::Matched(_)
    ));

    // Instance still exists (reset, not removed) — feed more events
    assert_eq!(sm.instance_count(), 1);
    sm.advance_at("fail", &e, 8_000_000_000);

    // Bucket [0, 10s) should expire at watermark=10s.
    // Bug: if reset sets created_at=2s (event time), expiry would need
    // watermark >= 12s instead of 10s.
    let expired = sm.scan_expired_at(10_000_000_000);
    assert_eq!(
        expired.len(),
        1,
        "bucket should expire at bucket_start + dur = 10s"
    );
    assert_eq!(sm.instance_count(), 0);
}

// ===========================================================================
// Fixed: close(scope_key) finds the bucket instance
// ===========================================================================

#[test]
fn fixed_close_by_scope_key() {
    let dur = Duration::from_secs(10);
    let plan = fixed_plan_with_close(
        vec![simple_key("sip")],
        dur,
        vec![step(vec![branch("fail", count_ge(1.0))])],
        vec![step(vec![branch("fail", count_ge(1.0))])],
    );
    let mut sm = CepStateMachine::new("r_fixed_close_key".to_string(), plan, None);

    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    sm.advance_at("fail", &e, 3_000_000_000);
    assert_eq!(sm.instance_count(), 1);

    // close() with scope key should find the fixed-window instance
    let out = sm.close(&[str_val("10.0.0.1")], CloseReason::Flush);
    assert!(out.is_some(), "close() should find fixed-window instance");
    assert_eq!(sm.instance_count(), 0);
}

// ===========================================================================
// Fixed: close(scope_key) closes oldest bucket first when multiple exist
// ===========================================================================

#[test]
fn fixed_close_oldest_bucket_first() {
    let dur = Duration::from_secs(10);
    let plan = fixed_plan_with_close(
        vec![simple_key("sip")],
        dur,
        vec![step(vec![branch("fail", count_ge(1.0))])],
        vec![step(vec![branch("fail", count_ge(1.0))])],
    );
    let mut sm = CepStateMachine::new("r_fixed_close_multi".to_string(), plan, None);

    let e = event(vec![("sip", str_val("10.0.0.1"))]);

    // Create two bucket instances: [0, 10s) and [10s, 20s)
    sm.advance_at("fail", &e, 3_000_000_000);
    sm.advance_at("fail", &e, 15_000_000_000);
    assert_eq!(sm.instance_count(), 2);

    // First close gets the oldest bucket [0, 10s)
    let out1 = sm.close(&[str_val("10.0.0.1")], CloseReason::Flush);
    assert!(out1.is_some());
    assert_eq!(sm.instance_count(), 1);

    // Second close gets bucket [10s, 20s)
    let out2 = sm.close(&[str_val("10.0.0.1")], CloseReason::Flush);
    assert!(out2.is_some());
    assert_eq!(sm.instance_count(), 0);

    // Third close → None
    let out3 = sm.close(&[str_val("10.0.0.1")], CloseReason::Flush);
    assert!(out3.is_none());
}

// ===========================================================================
// Fixed: scope key containing '@' does not cause cross-scope ambiguity
// ===========================================================================

#[test]
fn fixed_key_with_at_sign_no_ambiguity() {
    let dur = Duration::from_secs(10);
    let plan = fixed_plan_with_close(
        vec![simple_key("user")],
        dur,
        vec![step(vec![branch("fail", count_ge(1.0))])],
        vec![step(vec![branch("fail", count_ge(1.0))])],
    );
    let mut sm = CepStateMachine::new("r_fixed_at".to_string(), plan, None);

    // Two distinct scope keys: "user@host" and "user"
    let e1 = event(vec![("user", str_val("user@host"))]);
    let e2 = event(vec![("user", str_val("user"))]);

    // Both in bucket [0, 10s)
    sm.advance_at("fail", &e1, 3_000_000_000);
    sm.advance_at("fail", &e2, 5_000_000_000);
    assert_eq!(sm.instance_count(), 2);

    // close("user") must NOT accidentally close the "user@host" instance
    let out = sm.close(&[str_val("user")], CloseReason::Flush);
    assert!(out.is_some());
    assert_eq!(sm.instance_count(), 1);

    // The remaining instance is "user@host"
    let out2 = sm.close(&[str_val("user@host")], CloseReason::Flush);
    assert!(out2.is_some());
    assert_eq!(sm.instance_count(), 0);
}
