use super::*;

// ===========================================================================
// Limits: max_instances with Throttle
// ===========================================================================

#[test]
fn limits_max_instances_throttle() {
    // Use count >= 2 so instances stay alive after the first event
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(2.0))])],
    );
    let limits = LimitsPlan {
        max_memory_bytes: None,
        max_instances: Some(2),
        max_throttle: None,
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

    // Third key is throttled — max_instances reached
    assert_eq!(sm.advance("fail", &e3), StepResult::Accumulate);
    assert_eq!(sm.instance_count(), 2);

    // Existing keys still advance normally
    assert!(matches!(sm.advance("fail", &e1), StepResult::Matched(_)));
}

// ===========================================================================
// Limits: max_instances with DropOldest
// ===========================================================================

#[test]
fn limits_max_instances_drop_oldest() {
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(3.0))])],
    );
    let limits = LimitsPlan {
        max_memory_bytes: None,
        max_instances: Some(2),
        max_throttle: None,
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
// Limits: max_memory_bytes with Throttle
// ===========================================================================

#[test]
fn limits_max_memory_bytes_throttle() {
    // Each instance: ~128 base + ~32 key ("10.0.0.x" = 8 chars + 24) + 80 branch ≈ 240 bytes
    // base_estimated_bytes for a new instance: also ~240 bytes.
    // Set limit to 500: allows 2 instances (480 < 500) but blocks a 3rd
    // because 2 existing (480) + new base (240) = 720 >= 500.
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(2.0))])],
    );
    let limits = LimitsPlan {
        max_memory_bytes: Some(500),
        max_instances: None,
        max_throttle: None,
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

    // Third key is throttled — total exceeds max_memory_bytes
    assert_eq!(sm.advance("fail", &e3), StepResult::Accumulate);
    assert_eq!(sm.instance_count(), 2);

    // Existing keys still advance normally
    assert!(matches!(sm.advance("fail", &e1), StepResult::Matched(_)));
}

// ===========================================================================
// Limits: max_memory_bytes blocks first instance when base cost exceeds limit
// ===========================================================================

#[test]
fn limits_max_memory_bytes_blocks_first_instance() {
    // base_estimated_bytes for a new instance ≈ 240 bytes.
    // Set limit to 100: even the first instance's base cost exceeds the limit.
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(1.0))])],
    );
    let limits = LimitsPlan {
        max_memory_bytes: Some(100),
        max_instances: None,
        max_throttle: None,
        on_exceed: ExceedAction::Throttle,
    };
    let mut sm = CepStateMachine::with_limits("rule_tiny".to_string(), plan, None, Some(limits));

    let e1 = event(vec![("sip", str_val("10.0.0.1"))]);

    // Even the first event is throttled — new instance base cost exceeds limit
    assert_eq!(sm.advance("fail", &e1), StepResult::Accumulate);
    assert_eq!(sm.instance_count(), 0);
}

// ===========================================================================
// Limits: max_throttle with Throttle
// ===========================================================================

#[test]
fn limits_max_throttle_throttle() {
    // count >= 1 so every event for a key triggers a match
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(1.0))])],
    );
    let limits = LimitsPlan {
        max_memory_bytes: None,
        max_instances: None,
        max_throttle: Some(RateSpec {
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
// Limits: max_throttle window reset
// ===========================================================================

#[test]
fn limits_max_throttle_resets_window() {
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(1.0))])],
    );
    let limits = LimitsPlan {
        max_memory_bytes: None,
        max_instances: None,
        max_throttle: Some(RateSpec {
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

// ===========================================================================
// Limits: max_throttle enforced on close-path alerts
// ===========================================================================

#[test]
fn limits_max_throttle_close_path() {
    // Rule with close steps: event count >= 1, close count >= 0 (always passes).
    // Rate limit: 1 emit per 60s. Create 3 instances, satisfy event steps,
    // then close_all. Only the first close should produce an alert;
    // the other two should be suppressed (close_ok = false).
    let plan = plan_with_close(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(1.0))])],
        vec![step(vec![branch("fail", count_ge(0.0))])],
        Duration::from_secs(300),
    );
    let limits = LimitsPlan {
        max_memory_bytes: None,
        max_instances: None,
        max_throttle: Some(RateSpec {
            count: 1,
            per: Duration::from_secs(60),
        }),
        on_exceed: ExceedAction::Throttle,
    };
    let mut sm =
        CepStateMachine::with_limits("rule_close_rate".to_string(), plan, None, Some(limits));

    let e1 = event(vec![("sip", str_val("10.0.0.1"))]);
    let e2 = event(vec![("sip", str_val("10.0.0.2"))]);
    let e3 = event(vec![("sip", str_val("10.0.0.3"))]);

    // Each event creates an instance and satisfies the event step (count >= 1).
    // With close steps present, advance returns Advance (deferred to close path).
    let base = 1_000_000_000i64;
    assert_eq!(sm.advance_at("fail", &e1, base), StepResult::Advance);
    assert_eq!(sm.advance_at("fail", &e2, base + 1), StepResult::Advance);
    assert_eq!(sm.advance_at("fail", &e3, base + 2), StepResult::Advance);
    assert_eq!(sm.instance_count(), 3);

    // Close all instances at EOF. Rate limit is 1/60s.
    let closes = sm.close_all(CloseReason::Eos);
    assert_eq!(closes.len(), 3);

    // Count how many would actually emit an alert (event_ok && close_ok)
    let alert_count = closes.iter().filter(|c| c.event_ok && c.close_ok).count();
    assert_eq!(
        alert_count, 1,
        "expected only 1 close alert due to rate limiting, got {}",
        alert_count
    );
}

// ===========================================================================
// Limits: max_memory_bytes + DropOldest evicts enough instances
// ===========================================================================

#[test]
fn limits_max_memory_bytes_drop_oldest_evicts_enough() {
    // Each instance ≈ 240 bytes. Create 3 instances (≈720 bytes total).
    // Then set limit to 300 — requires evicting at least 2 of the 3 existing
    // instances to make room for the new one.
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(5.0))])],
    );
    let limits = LimitsPlan {
        max_memory_bytes: Some(300),
        max_instances: None,
        max_throttle: None,
        on_exceed: ExceedAction::DropOldest,
    };
    let mut sm =
        CepStateMachine::with_limits("rule_drop_multi".to_string(), plan, None, Some(limits));

    let e1 = event(vec![("sip", str_val("10.0.0.1"))]);
    let e2 = event(vec![("sip", str_val("10.0.0.2"))]);
    let e3 = event(vec![("sip", str_val("10.0.0.3"))]);
    let e4 = event(vec![("sip", str_val("10.0.0.4"))]);

    // Create 3 instances — all fit initially (no limit hit until total >= 300
    // is checked with incoming new instance cost included)
    sm.advance_at("fail", &e1, 100);
    sm.advance_at("fail", &e2, 200);
    sm.advance_at("fail", &e3, 300);

    // Fourth key arrives: total existing ≈720 + new ≈240 = 960 >= 300.
    // DropOldest should evict enough to get under 300 before creating new.
    sm.advance_at("fail", &e4, 400);

    // After evictions + new creation, total estimated should be < 300
    // and we should have at most 1 instance (the new one).
    assert!(
        sm.instance_count() <= 1,
        "expected at most 1 instance after drop_oldest eviction loop, got {}",
        sm.instance_count()
    );
}

// ===========================================================================
// Limits: max_memory_bytes + DropOldest with no instances to evict
// ===========================================================================

#[test]
fn limits_max_memory_bytes_drop_oldest_no_instances() {
    // Set limit extremely small (10 bytes). Even a single instance base cost
    // exceeds it. With DropOldest, there's nothing to evict, so the event
    // should be skipped (Accumulate) rather than creating an instance.
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(1.0))])],
    );
    let limits = LimitsPlan {
        max_memory_bytes: Some(10),
        max_instances: None,
        max_throttle: None,
        on_exceed: ExceedAction::DropOldest,
    };
    let mut sm =
        CepStateMachine::with_limits("rule_drop_empty".to_string(), plan, None, Some(limits));

    let e1 = event(vec![("sip", str_val("10.0.0.1"))]);

    // No instances exist, but new instance cost > limit.
    // DropOldest has nothing to evict → should not create instance.
    assert_eq!(sm.advance("fail", &e1), StepResult::Accumulate);
    assert_eq!(sm.instance_count(), 0);
}

// ===========================================================================
// Limits: scan_expired_at deterministic rate limiting order
// ===========================================================================

#[test]
fn limits_scan_expired_rate_limit_deterministic() {
    // Create 5 instances at staggered times, all with close steps.
    // Rate limit: 2 per 60s. Expire all at once via scan_expired_at.
    // Regardless of HashMap iteration order, the 2 earliest-created
    // instances should always be the ones that emit alerts.
    let plan = plan_with_close(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(1.0))])],
        vec![step(vec![branch("fail", count_ge(0.0))])],
        Duration::from_secs(10),
    );
    let limits = LimitsPlan {
        max_memory_bytes: None,
        max_instances: None,
        max_throttle: Some(RateSpec {
            count: 2,
            per: Duration::from_secs(60),
        }),
        on_exceed: ExceedAction::Throttle,
    };
    let mut sm = CepStateMachine::with_limits(
        "rule_scan_rate".to_string(),
        plan,
        None,
        Some(limits),
    );

    // Create 5 instances at distinct creation times (1s apart).
    // Each satisfies event step (count >= 1) → Advance (deferred to close).
    let base = 1_000_000_000i64;
    for i in 0..5 {
        let e = event(vec![("sip", str_val(&format!("10.0.0.{}", i + 1)))]);
        assert_eq!(
            sm.advance_at("fail", &e, base + i * 1_000_000_000),
            StepResult::Advance
        );
    }
    assert_eq!(sm.instance_count(), 5);

    // Expire all instances at once (watermark well past all creation + 10s).
    let expire_wm = base + 20_000_000_000;
    let results = sm.scan_expired_at(expire_wm);
    assert_eq!(results.len(), 5);

    let alert_count = results.iter().filter(|c| c.event_ok && c.close_ok).count();
    assert_eq!(
        alert_count, 2,
        "expected exactly 2 alerts due to rate limit 2/60s, got {}",
        alert_count
    );
}

// ===========================================================================
// Limits: max_memory_bytes + DropOldest evicts the current key when oldest
// ===========================================================================

#[test]
fn limits_max_memory_bytes_drop_oldest_evicts_current_key() {
    // Use a 2-step plan: each step needs count >= 1.
    // After step1 completes, completed_steps grows and estimated_bytes
    // increases from ~320 (base for 2-step) to ~384 bytes per instance.
    // With 2 grown instances: 384 + 384 = 768. Set limit=750 so the check
    // triggers only when both instances have grown, not during creation.
    // (During B's creation: A.384 + B.base.320 = 704 < 750 → OK.)
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![
            step(vec![branch("fail", count_ge(1.0))]),
            step(vec![branch("fail", count_ge(1.0))]),
        ],
    );
    let limits = LimitsPlan {
        max_memory_bytes: Some(750),
        max_instances: None,
        max_throttle: None,
        on_exceed: ExceedAction::DropOldest,
    };
    let mut sm = CepStateMachine::with_limits(
        "rule_drop_current".to_string(),
        plan,
        None,
        Some(limits),
    );

    let e1 = event(vec![("sip", str_val("10.0.0.1"))]);
    let e2 = event(vec![("sip", str_val("10.0.0.2"))]);

    // Event 1 for A: step1 completes (count=1 >= 1). A grows to ~384 bytes.
    assert_eq!(sm.advance_at("fail", &e1, 100), StepResult::Advance);
    // Event 2 for B: step1 completes. B grows to ~384 bytes.
    assert_eq!(sm.advance_at("fail", &e2, 200), StepResult::Advance);
    assert_eq!(sm.instance_count(), 2);
    // Total = 384 + 384 = 768 >= 750. Next event for existing key triggers check.

    // Event 3 for A (oldest key, created_at=100):
    // Memory check: 768 >= 750 → DropOldest.
    // A is the oldest → evicted. Re-creation base cost ~320 is budgeted:
    //   768 - 384(A) + 320(base) = 704 < 750 → fits.
    // Fresh A processes event: step1 count=1 >= 1 → Advance (step2 remains).
    let result = sm.advance_at("fail", &e1, 300);
    assert_eq!(
        result,
        StepResult::Advance,
        "expected Advance (step1 on fresh re-created instance), got {:?}",
        result
    );
    // B (384 bytes) + fresh A (320 base) = 704: both instances alive.
    assert_eq!(sm.instance_count(), 2);
}

// ===========================================================================
// Limits: close_all deterministic rate limiting order
// ===========================================================================

#[test]
fn limits_close_all_rate_limit_deterministic() {
    // Create 5 instances at staggered times with close steps.
    // Rate limit: 2 per 60s. close_all should process in created_at order,
    // so the 2 earliest instances always get their alerts through.
    let plan = plan_with_close(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(1.0))])],
        vec![step(vec![branch("fail", count_ge(0.0))])],
        Duration::from_secs(300),
    );
    let limits = LimitsPlan {
        max_memory_bytes: None,
        max_instances: None,
        max_throttle: Some(RateSpec {
            count: 2,
            per: Duration::from_secs(60),
        }),
        on_exceed: ExceedAction::Throttle,
    };
    let mut sm = CepStateMachine::with_limits(
        "rule_close_all_det".to_string(),
        plan,
        None,
        Some(limits),
    );

    let base = 1_000_000_000i64;
    for i in 0..5 {
        let e = event(vec![("sip", str_val(&format!("10.0.0.{}", i + 1)))]);
        assert_eq!(
            sm.advance_at("fail", &e, base + i * 1_000_000_000),
            StepResult::Advance
        );
    }
    assert_eq!(sm.instance_count(), 5);

    let results = sm.close_all(CloseReason::Eos);
    assert_eq!(results.len(), 5);

    let alert_count = results.iter().filter(|c| c.event_ok && c.close_ok).count();
    assert_eq!(
        alert_count, 2,
        "expected exactly 2 alerts due to rate limit 2/60s, got {}",
        alert_count
    );

    // The first 2 results (sorted by created_at) should be the ones with alerts
    let first_two_ok: Vec<bool> = results.iter().take(2).map(|c| c.event_ok && c.close_ok).collect();
    assert_eq!(first_two_ok, vec![true, true], "earliest-created instances should get alerts");
}
