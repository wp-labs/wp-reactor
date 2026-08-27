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
        spill: None,
        max_disk_bytes: None,
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
        spill: None,
        max_disk_bytes: None,
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
        spill: None,
        max_disk_bytes: None,
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
        spill: None,
        max_disk_bytes: None,
    };
    let mut sm = CepStateMachine::with_limits("rule_tiny".to_string(), plan, None, Some(limits));

    let e1 = event(vec![("sip", str_val("10.0.0.1"))]);

    // Even the first event is throttled — new instance base cost exceeds limit
    assert_eq!(sm.advance("fail", &e1), StepResult::Accumulate);
    assert_eq!(sm.instance_count(), 0);
}

#[test]
fn limits_max_memory_bytes_counts_tracked_source_alias_state() {
    let mut plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(1.0))])],
    );
    plan.tracked_bind_aliases = std::collections::HashSet::from(["fail".to_string()]);

    let limits = LimitsPlan {
        max_memory_bytes: Some(300),
        max_instances: None,
        max_throttle: None,
        on_exceed: ExceedAction::Throttle,
        spill: None,
        max_disk_bytes: None,
    };
    let mut sm =
        CepStateMachine::with_limits("rule_tracked_mem".to_string(), plan, None, Some(limits));

    let e1 = event(vec![("sip", str_val("10.0.0.1"))]);

    // Base branch state fits under 300 bytes, but adding tracked source-alias
    // bind state for the first event pushes the admission estimate over limit.
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
        spill: None,
        max_disk_bytes: None,
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
        spill: None,
        max_disk_bytes: None,
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
        spill: None,
        max_disk_bytes: None,
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
        spill: None,
        max_disk_bytes: None,
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
        spill: None,
        max_disk_bytes: None,
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
        spill: None,
        max_disk_bytes: None,
    };
    let mut sm =
        CepStateMachine::with_limits("rule_scan_rate".to_string(), plan, None, Some(limits));

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
    // `estimated_memory_bytes` tracks only the O(1) per-instance base cost on
    // insert/remove; state growth (completed_steps after step1) is invisible
    // until `recalibrate_memory()` re-anchors to the exact sum. So: create the
    // two instances, re-anchor (A+B grow to ~384 each = 768), then a further
    // event exceeds the 750 limit and exercises DropOldest.
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
        spill: None,
        max_disk_bytes: None,
    };
    let mut sm =
        CepStateMachine::with_limits("rule_drop_current".to_string(), plan, None, Some(limits));

    let e1 = event(vec![("sip", str_val("10.0.0.1"))]);
    let e2 = event(vec![("sip", str_val("10.0.0.2"))]);

    // Event 1 for A: step1 completes (count=1 >= 1).
    assert_eq!(sm.advance_at("fail", &e1, 100), StepResult::Advance);
    // Event 2 for B: step1 completes.
    assert_eq!(sm.advance_at("fail", &e2, 200), StepResult::Advance);
    assert_eq!(sm.instance_count(), 2);
    // Re-anchor to exact: each instance grew to ~384 bytes, 384 + 384 = 768.
    sm.recalibrate_memory();

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

#[test]
fn recalibrate_memory_reanchors_after_state_growth() {
    // base×N accounting tracks only creation-time cost; instance state growth
    // (completed_steps) is invisible until recalibrate_memory() re-anchors to
    // the exact sum of live instance state.
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![
            step(vec![branch("fail", count_ge(1.0))]),
            step(vec![branch("fail", count_ge(1.0))]),
        ],
    );
    let limits = LimitsPlan {
        max_memory_bytes: Some(100_000),
        max_instances: None,
        max_throttle: None,
        on_exceed: ExceedAction::Throttle,
        spill: None,
        max_disk_bytes: None,
    };
    let mut sm = CepStateMachine::with_limits("rule_recal".to_string(), plan, None, Some(limits));

    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    // Instance created (base ~320) then step1 completes (grows to ~384).
    assert_eq!(sm.advance_at("fail", &e, 100), StepResult::Advance);

    // Running estimate is the O(1) base cost only — below the exact size.
    let base_estimate = sm.estimated_memory_bytes_for_test();
    sm.recalibrate_memory();
    let exact = sm.estimated_memory_bytes_for_test();
    assert!(
        exact >= base_estimate,
        "exact estimate ({exact}) should be >= base-only estimate ({base_estimate}) after growth"
    );
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
        spill: None,
        max_disk_bytes: None,
    };
    let mut sm =
        CepStateMachine::with_limits("rule_close_all_det".to_string(), plan, None, Some(limits));

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
    let first_two_ok: Vec<bool> = results
        .iter()
        .take(2)
        .map(|c| c.event_ok && c.close_ok)
        .collect();
    assert_eq!(
        first_two_ok,
        vec![true, true],
        "earliest-created instances should get alerts"
    );
}

// ===========================================================================
// P2b: shared limits across shards (with_limits_shared)
// ===========================================================================

#[test]
fn shared_max_instances_capped_collectively_across_shards() {
    // count >= 2 so instances stay alive after the first event.
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(2.0))])],
    );
    let limits = LimitsPlan {
        max_memory_bytes: None,
        max_instances: Some(2),
        max_throttle: None,
        on_exceed: ExceedAction::Throttle,
        spill: None,
        max_disk_bytes: None,
    };
    let shared = SharedLimits::new();
    let mut sm1 = CepStateMachine::with_limits_shared(
        "rule_lim".to_string(),
        plan.clone(),
        None,
        Some(limits.clone()),
        std::sync::Arc::clone(&shared),
    );
    let mut sm2 = CepStateMachine::with_limits_shared(
        "rule_lim".to_string(),
        plan,
        None,
        Some(limits),
        std::sync::Arc::clone(&shared),
    );

    let e1 = event(vec![("sip", str_val("10.0.0.1"))]);
    let e2 = event(vec![("sip", str_val("10.0.0.2"))]);
    let e3 = event(vec![("sip", str_val("10.0.0.3"))]);

    // Two shards collectively hold 2 instances (1 each).
    assert_eq!(sm1.advance("fail", &e1), StepResult::Accumulate);
    assert_eq!(sm2.advance("fail", &e2), StepResult::Accumulate);
    assert_eq!(shared.instance_count(), 2);

    // The collective cap of 2 is reached — shard2's third key is throttled
    // even though shard2 alone has only 1 local instance.
    assert_eq!(sm2.advance("fail", &e3), StepResult::Accumulate);
    assert_eq!(shared.instance_count(), 2);
    assert_eq!(sm1.instance_count(), 1);
    assert_eq!(sm2.instance_count(), 1);

    // Existing keys still advance normally (collective budget is not frozen).
    assert!(matches!(sm1.advance("fail", &e1), StepResult::Matched(_)));
}

#[test]
fn shared_throttle_capped_collectively_across_shards() {
    // count >= 1 so every event for a key triggers a match.
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
        spill: None,
        max_disk_bytes: None,
    };
    let shared = SharedLimits::new();
    let mut sm1 = CepStateMachine::with_limits_shared(
        "rule_rate".to_string(),
        plan.clone(),
        None,
        Some(limits.clone()),
        std::sync::Arc::clone(&shared),
    );
    let mut sm2 = CepStateMachine::with_limits_shared(
        "rule_rate".to_string(),
        plan,
        None,
        Some(limits),
        std::sync::Arc::clone(&shared),
    );

    let e1 = event(vec![("sip", str_val("10.0.0.1"))]);

    // Two shards collectively get 2 emits per window (previously 2×2 = 4).
    assert!(matches!(
        sm1.advance_at("fail", &e1, 1_000_000_000),
        StepResult::Matched(_)
    ));
    assert!(matches!(
        sm2.advance_at("fail", &e1, 2_000_000_000),
        StepResult::Matched(_)
    ));
    // Third collective emit is throttled.
    assert_eq!(
        sm1.advance_at("fail", &e1, 3_000_000_000),
        StepResult::Accumulate
    );
}

#[test]
fn shared_fail_rule_latches_all_shards() {
    // One shard exceeding the instance cap with FailRule fails the whole rule:
    // the other shard must reject events too (shared failed latch).
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(2.0))])],
    );
    let limits = LimitsPlan {
        max_memory_bytes: None,
        max_instances: Some(1),
        max_throttle: None,
        on_exceed: ExceedAction::FailRule,
        spill: None,
        max_disk_bytes: None,
    };
    let shared = SharedLimits::new();
    let mut sm1 = CepStateMachine::with_limits_shared(
        "rule_fail".to_string(),
        plan.clone(),
        None,
        Some(limits.clone()),
        std::sync::Arc::clone(&shared),
    );
    let mut sm2 = CepStateMachine::with_limits_shared(
        "rule_fail".to_string(),
        plan,
        None,
        Some(limits),
        std::sync::Arc::clone(&shared),
    );

    let e1 = event(vec![("sip", str_val("10.0.0.1"))]);
    let e2 = event(vec![("sip", str_val("10.0.0.2"))]);

    // shard1 fills the single slot; shard2's key exceeds → FailRule latches.
    assert_eq!(sm1.advance("fail", &e1), StepResult::Accumulate);
    assert_eq!(sm2.advance("fail", &e2), StepResult::Accumulate);
    assert!(shared.is_failed());

    // Both shards reject all further events.
    assert_eq!(sm1.advance("fail", &e1), StepResult::Accumulate);
    assert_eq!(sm2.advance("fail", &e1), StepResult::Accumulate);
}

// ===========================================================================
// P1②: exact max_instances reservation across shards (DropOldest paths)
// ===========================================================================

#[test]
fn shared_max_instances_drop_oldest_evicts_local_and_rereserves() {
    // max=1, DropOldest: a shard with a local instance evicts it and re-reserves
    // so the shared count stays exactly 1 (the new key swaps in).
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(2.0))])],
    );
    let limits = LimitsPlan {
        max_memory_bytes: None,
        max_instances: Some(1),
        max_throttle: None,
        on_exceed: ExceedAction::DropOldest,
        spill: None,
        max_disk_bytes: None,
    };
    let shared = SharedLimits::new();
    let mut sm = CepStateMachine::with_limits_shared(
        "rule_drop".to_string(),
        plan,
        None,
        Some(limits),
        std::sync::Arc::clone(&shared),
    );
    let e1 = event(vec![("sip", str_val("10.0.0.1"))]);
    let e2 = event(vec![("sip", str_val("10.0.0.2"))]);

    assert_eq!(sm.advance("fail", &e1), StepResult::Accumulate);
    assert_eq!(shared.instance_count(), 1);
    // New key exceeds the shared cap: DropOldest evicts the local oldest and
    // re-reserves, so the count stays exactly 1 and the new key is created.
    assert_eq!(sm.advance("fail", &e2), StepResult::Accumulate);
    assert_eq!(shared.instance_count(), 1);
    assert_eq!(sm.instance_count(), 1);
}

#[test]
fn shared_max_instances_drop_oldest_rejects_when_no_local_to_evict() {
    // max=1, DropOldest, two shards. Shard1 holds the only slot; shard2 has no
    // local instance to evict, so its new key must be REJECTED (shared budget
    // held by the other shard) — the previous check-then-act would have created
    // it and overshot the cap.
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(2.0))])],
    );
    let limits = LimitsPlan {
        max_memory_bytes: None,
        max_instances: Some(1),
        max_throttle: None,
        on_exceed: ExceedAction::DropOldest,
        spill: None,
        max_disk_bytes: None,
    };
    let shared = SharedLimits::new();
    let mut sm1 = CepStateMachine::with_limits_shared(
        "rule_drop".to_string(),
        plan.clone(),
        None,
        Some(limits.clone()),
        std::sync::Arc::clone(&shared),
    );
    let mut sm2 = CepStateMachine::with_limits_shared(
        "rule_drop".to_string(),
        plan,
        None,
        Some(limits),
        std::sync::Arc::clone(&shared),
    );
    let e1 = event(vec![("sip", str_val("10.0.0.1"))]);
    let e2 = event(vec![("sip", str_val("10.0.0.2"))]);

    // Shard1 reserves the single slot.
    assert_eq!(sm1.advance("fail", &e1), StepResult::Accumulate);
    assert_eq!(shared.instance_count(), 1);
    // Shard2's new key: shared cap reached, no local instance to evict →
    // rejected (no overshoot).
    assert_eq!(sm2.advance("fail", &e2), StepResult::Accumulate);
    assert_eq!(shared.instance_count(), 1);
    assert_eq!(sm2.instance_count(), 0);
}

#[test]
fn shared_max_instances_released_on_close_all() {
    // Instances closed (permanent remove) release their shared slots, so the
    // count returns to zero exactly.
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(2.0))])],
    );
    let limits = LimitsPlan {
        max_memory_bytes: None,
        max_instances: Some(3),
        max_throttle: None,
        on_exceed: ExceedAction::Throttle,
        spill: None,
        max_disk_bytes: None,
    };
    let shared = SharedLimits::new();
    let mut sm = CepStateMachine::with_limits_shared(
        "rule_close".to_string(),
        plan,
        None,
        Some(limits),
        std::sync::Arc::clone(&shared),
    );
    let e1 = event(vec![("sip", str_val("10.0.0.1"))]);
    let e2 = event(vec![("sip", str_val("10.0.0.2"))]);
    assert_eq!(sm.advance("fail", &e1), StepResult::Accumulate);
    assert_eq!(sm.advance("fail", &e2), StepResult::Accumulate);
    assert_eq!(shared.instance_count(), 2);
    // close_all permanently removes both → shared count back to zero.
    let closes = sm.close_all(CloseReason::Flush);
    assert_eq!(closes.len(), 2);
    assert_eq!(shared.instance_count(), 0);
}

#[test]
fn shared_slot_released_when_memory_throttle_rejects_new_key() {
    // N1 regression: with BOTH max_instances and max_memory_bytes configured,
    // admission's CAS reservation must be released when the memory check
    // throttles the new key. Before the fix each throttled key leaked one
    // shared slot until the whole budget was burnt (all shards then rejected
    // every new key).
    // Instance ≈ 240 bytes: limit 500 admits 2 instances; a 3rd key sees
    // 480 + 240 = 720 >= 500 and is throttled by the memory check.
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(2.0))])],
    );
    let limits = LimitsPlan {
        max_memory_bytes: Some(500),
        max_instances: Some(10),
        max_throttle: None,
        on_exceed: ExceedAction::Throttle,
        spill: None,
        max_disk_bytes: None,
    };
    let shared = SharedLimits::new();
    let mut sm = CepStateMachine::with_limits_shared(
        "rule_n1".to_string(),
        plan,
        None,
        Some(limits),
        std::sync::Arc::clone(&shared),
    );

    assert_eq!(
        sm.advance("fail", &event(vec![("sip", str_val("10.0.0.1"))])),
        StepResult::Accumulate
    );
    assert_eq!(
        sm.advance("fail", &event(vec![("sip", str_val("10.0.0.2"))])),
        StepResult::Accumulate
    );
    assert_eq!(shared.instance_count(), 2);

    // 8 distinct new keys: each passes the (generous) instance admission —
    // reserving a shared slot — then hits the memory Throttle and returns.
    // All 8 reservations must be released: without the fix the shared count
    // would climb to 2 + 8 = 10 and burn the entire instance budget.
    for i in 3..11 {
        let e = event(vec![("sip", str_val(&format!("10.0.0.{}", i)))]);
        assert_eq!(sm.advance("fail", &e), StepResult::Accumulate);
    }
    assert_eq!(sm.instance_count(), 2);
    assert_eq!(
        shared.instance_count(),
        2,
        "throttled new keys must not leak shared instance slots"
    );

    // Budget intact: after closing both instances a new key fits again.
    let closes = sm.close_all(CloseReason::Flush);
    assert_eq!(closes.len(), 2);
    assert_eq!(shared.instance_count(), 0);
    assert_eq!(
        sm.advance("fail", &event(vec![("sip", str_val("10.0.0.9"))])),
        StepResult::Accumulate
    );
    assert_eq!(shared.instance_count(), 1);
}

#[test]
fn shared_slot_released_when_memory_fail_rule_rejects_new_key() {
    // N1 regression, FailRule variant: the memory check's FailRule arm must
    // also release the un-consumed admission reservation before latching.
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(2.0))])],
    );
    let limits = LimitsPlan {
        max_memory_bytes: Some(500),
        max_instances: Some(10),
        max_throttle: None,
        on_exceed: ExceedAction::FailRule,
        spill: None,
        max_disk_bytes: None,
    };
    let shared = SharedLimits::new();
    let mut sm = CepStateMachine::with_limits_shared(
        "rule_n1f".to_string(),
        plan,
        None,
        Some(limits),
        std::sync::Arc::clone(&shared),
    );

    assert_eq!(
        sm.advance("fail", &event(vec![("sip", str_val("10.0.0.1"))])),
        StepResult::Accumulate
    );
    assert_eq!(
        sm.advance("fail", &event(vec![("sip", str_val("10.0.0.2"))])),
        StepResult::Accumulate
    );
    assert_eq!(shared.instance_count(), 2);

    // Third key trips the memory FailRule: latch set AND slot released.
    assert_eq!(
        sm.advance("fail", &event(vec![("sip", str_val("10.0.0.3"))])),
        StepResult::Accumulate
    );
    assert!(shared.is_failed());
    assert_eq!(
        shared.instance_count(),
        2,
        "FailRule must release the un-consumed reservation"
    );
}

#[test]
fn shared_slot_inherited_when_memory_drop_oldest_recreates_current() {
    // N2 regression: when the memory DropOldest loop evicts the incoming key's
    // OWN instance, the re-created fresh instance inherits its shared slot.
    // Releasing it (old behavior) under-counted by one and over-admitted
    // later keys.
    // Mirror of limits_max_memory_bytes_drop_oldest_evicts_current_key with
    // shared limits: 2 instances re-anchored to ~384 each = 768 >= 750 →
    // DropOldest evicts the oldest, which IS the current key (A).
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![
            step(vec![branch("fail", count_ge(1.0))]),
            step(vec![branch("fail", count_ge(1.0))]),
        ],
    );
    let limits = LimitsPlan {
        max_memory_bytes: Some(750),
        max_instances: Some(10),
        max_throttle: None,
        on_exceed: ExceedAction::DropOldest,
        spill: None,
        max_disk_bytes: None,
    };
    let shared = SharedLimits::new();
    let mut sm = CepStateMachine::with_limits_shared(
        "rule_n2".to_string(),
        plan,
        None,
        Some(limits),
        std::sync::Arc::clone(&shared),
    );

    let e1 = event(vec![("sip", str_val("10.0.0.1"))]);
    let e2 = event(vec![("sip", str_val("10.0.0.2"))]);
    assert_eq!(sm.advance_at("fail", &e1, 100), StepResult::Advance);
    assert_eq!(sm.advance_at("fail", &e2, 200), StepResult::Advance);
    assert_eq!(shared.instance_count(), 2);
    sm.recalibrate_memory();

    // A (oldest, current) evicted and re-created: local count 2, shared
    // count must still be 2 — the fresh A inherited the evicted slot.
    let result = sm.advance_at("fail", &e1, 300);
    assert_eq!(result, StepResult::Advance);
    assert_eq!(sm.instance_count(), 2);
    assert_eq!(
        shared.instance_count(),
        2,
        "re-created current-key instance must inherit its shared slot"
    );
}
