//! M15 close step / timeout tests (12–21).

use std::time::Duration;

use wf_lang::ast::{CmpOp, Expr, FieldSelector, Measure};
use wf_lang::plan::{AggPlan, BranchPlan};

use crate::match_engine::match_engine::{CepStateMachine, CloseReason, StepResult};

use super::helpers::*;

const NANOS_PER_SEC: i64 = 1_000_000_000;

#[test]
fn no_close_steps_preserves_m14() {
    // Empty close_steps → advance() returns Matched (backward compat)
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(2.0))])],
    );
    let mut sm = CepStateMachine::new("rule12".to_string(), plan, None);
    let e = event(vec![("sip", str_val("10.0.0.1"))]);

    assert_eq!(sm.advance("fail", &e), StepResult::Accumulate);
    if let StepResult::Matched(ctx) = sm.advance("fail", &e) {
        assert_eq!(ctx.rule_name, "rule12");
        assert_eq!(ctx.step_data[0].measure_value, 2.0);
    } else {
        panic!("expected Matched with no close steps");
    }
}

#[test]
fn close_missing_detection() {
    // A → NOT B: req count≥1, resp count==0 with timeout guard → event_ok && close_ok
    let plan = plan_with_close(
        vec![simple_key("sip")],
        // event step: req count >= 1
        vec![step(vec![branch("req", count_ge(1.0))])],
        // close step: resp count == 0, guarded on timeout
        vec![step(vec![BranchPlan {
            label: Some("no_resp".to_string()),
            source: "resp".to_string(),
            field: None,
            guard: Some(close_reason_guard("timeout")),
            agg: AggPlan {
                transforms: vec![],
                measure: Measure::Count,
                cmp: CmpOp::Eq,
                threshold: Expr::Number(0.0),
            },
        }])],
        Duration::from_secs(60),
    );
    let mut sm = CepStateMachine::new("rule13".to_string(), plan, None);
    let base: i64 = 1_700_000_000 * NANOS_PER_SEC;

    // Send a request → event step completes, returns Advance (not Matched, close steps present)
    let req = event(vec![("sip", str_val("10.0.0.1"))]);
    let result = sm.advance_at("req", &req, base);
    assert_eq!(result, StepResult::Advance);
    assert_eq!(sm.instance_count(), 1);

    // No response arrives. Timeout triggers close.
    let close_time = base + 61 * NANOS_PER_SEC;
    let expired = sm.scan_expired_at(close_time);
    assert_eq!(expired.len(), 1);

    let out = &expired[0];
    assert_eq!(out.rule_name, "rule13");
    assert_eq!(out.scope_key, vec![str_val("10.0.0.1")]);
    assert_eq!(out.close_reason, CloseReason::Timeout);
    assert!(out.event_ok);
    assert!(out.close_ok);
    assert_eq!(out.event_step_data.len(), 1);
    assert_eq!(out.close_step_data.len(), 1);
    assert_eq!(out.close_step_data[0].label, Some("no_resp".to_string()));
}

#[test]
fn int_key_close_preserves_number_scope_key() {
    // Regression: the close path's scope_key used to flatten Int keys to
    // `Value::Str`, so digit-typed yield/entity fields referencing the key
    // (e.g. `id = b.auction` on `on close` / conv rules) received a string and
    // failed digit coercion — every close alert was dropped with `data format
    // error` (Q12/Q14 hit this). A close over an Int key must produce a
    // `Number` scope_key, byte-identical to the event path.
    let plan = plan_with_close(
        vec![simple_key("auction")],
        vec![step(vec![branch("b", count_ge(1.0))])],
        vec![step(vec![branch("b", count_ge(1.0))])],
        Duration::from_secs(60),
    );
    let mut sm = CepStateMachine::new("int_key_close".to_string(), plan, None);
    let base: i64 = 1_700_000_000 * NANOS_PER_SEC;

    let bid = event(vec![("auction", num(421_762.0))]);
    assert_eq!(sm.advance_at("b", &bid, base), StepResult::Advance);

    let expired = sm.scan_expired_at(base + 61 * NANOS_PER_SEC);
    assert_eq!(expired.len(), 1);
    assert_eq!(expired[0].scope_key, vec![num(421_762.0)]);
}

#[test]
fn close_sum_measure_fires_at_window_end() {
    // Q16 regression probe: `and close { b.price | sum >= T }` on a fixed window.
    // Count-close fires (q12min), but sum/avg close returned 0 — verify the
    // engine accumulates the branch field's sum across the window and fires.
    use wf_lang::ast::FieldSelector;

    let sum_price = BranchPlan {
        label: Some("total".to_string()),
        source: "b".to_string(),
        field: Some(FieldSelector::Dot("price".into())),
        guard: None,
        agg: AggPlan {
            transforms: vec![],
            measure: Measure::Sum,
            cmp: CmpOp::Ge,
            threshold: Expr::Number(10.0),
        },
    };
    let plan = plan_with_close(
        vec![simple_key("auction")],
        vec![step(vec![branch("b", count_ge(1.0))])],
        vec![step(vec![sum_price])],
        Duration::from_secs(60),
    );
    let mut sm = CepStateMachine::new("close_sum".to_string(), plan, None);
    let base: i64 = 1_700_000_000 * NANOS_PER_SEC;

    // Three bids, prices 5/5/5 → window sum 15 >= 10. The event step fires once
    // (AND mode, event_ok kept), close accumulates every bid's price.
    for (i, price) in [5.0, 5.0, 5.0].into_iter().enumerate() {
        let ns = base + (i as i64) * NANOS_PER_SEC;
        let result = sm.advance_at(
            "b",
            &event(vec![("auction", num(1.0)), ("price", num(price))]),
            ns,
        );
        assert!(
            matches!(result, StepResult::Advance | StepResult::Accumulate),
            "AND close should not emit per event"
        );
    }

    let expired = sm.scan_expired_at(base + 61 * NANOS_PER_SEC);
    assert_eq!(expired.len(), 1, "one window closes");
    let out = &expired[0];
    assert!(out.event_ok);
    assert!(out.close_ok, "close must qualify (sum >= 10)");
    assert_eq!(out.close_step_data.len(), 1);
    assert_eq!(out.close_step_data[0].label, Some("total".to_string()));
    assert_eq!(
        out.close_step_data[0].measure_value, 15.0,
        "window sum of prices must be 15"
    );
}

#[test]
fn maxspan_expiry_resets() {
    // Instance past maxspan → scan_expired removes it, returns CloseOutput
    let plan = plan_with_close(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(5.0))])],
        vec![],
        Duration::from_secs(30),
    );
    let mut sm = CepStateMachine::new("rule14".to_string(), plan, None);
    let base: i64 = 1_700_000_000 * NANOS_PER_SEC;

    // Create an instance
    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    sm.advance_at("fail", &e, base);
    assert_eq!(sm.instance_count(), 1);

    // Scan before expiry — nothing removed
    let before = base + 29 * NANOS_PER_SEC;
    let expired = sm.scan_expired_at(before);
    assert!(expired.is_empty());
    assert_eq!(sm.instance_count(), 1);

    // Scan after expiry — instance removed
    let after = base + 31 * NANOS_PER_SEC;
    let expired = sm.scan_expired_at(after);
    assert_eq!(expired.len(), 1);
    assert_eq!(expired[0].close_reason, CloseReason::Timeout);
    assert!(!expired[0].event_ok); // event steps not complete
    assert_eq!(sm.instance_count(), 0);
}

#[test]
fn on_close_trigger_eval() {
    // Close step with no guard, resp count≥2 accumulated → close_ok
    let plan = plan_with_close(
        vec![simple_key("sip")],
        vec![step(vec![branch("req", count_ge(1.0))])],
        // close step: resp count >= 2 (no guard)
        vec![step(vec![branch("resp", count_ge(2.0))])],
        Duration::from_secs(60),
    );
    let mut sm = CepStateMachine::new("rule15".to_string(), plan, None);
    let base: i64 = 1_700_000_000 * NANOS_PER_SEC;

    let req = event(vec![("sip", str_val("10.0.0.1"))]);
    let resp = event(vec![("sip", str_val("10.0.0.1"))]);

    // req → Advance (event step done)
    assert_eq!(sm.advance_at("req", &req, base), StepResult::Advance);

    // 2 resp events → accumulate close step data
    assert_eq!(sm.advance_at("resp", &resp, base), StepResult::Accumulate);
    assert_eq!(sm.advance_at("resp", &resp, base), StepResult::Accumulate);

    // Close explicitly
    let out = sm
        .close(&[str_val("10.0.0.1")], CloseReason::Flush)
        .unwrap();
    assert!(out.event_ok);
    assert!(out.close_ok);
    assert_eq!(out.close_step_data[0].measure_value, 2.0);
}

#[test]
fn close_on_incomplete_instance() {
    // event_ok=false (steps not done), close_ok=true → output reflects both
    let plan = plan_with_close(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(5.0))])],
        // close step: any count == 0 (trivially true if no events matched)
        vec![step(vec![BranchPlan {
            label: None,
            source: "resp".to_string(),
            field: None,
            guard: None,
            agg: AggPlan {
                transforms: vec![],
                measure: Measure::Count,
                cmp: CmpOp::Eq,
                threshold: Expr::Number(0.0),
            },
        }])],
        Duration::from_secs(60),
    );
    let mut sm = CepStateMachine::new("rule16".to_string(), plan, None);
    let base: i64 = 1_700_000_000 * NANOS_PER_SEC;

    // One event — event step not yet satisfied (needs 5)
    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    sm.advance_at("fail", &e, base);
    assert_eq!(sm.instance_count(), 1);

    // Close — event_ok=false, close_ok=true (resp count == 0 is true)
    let out = sm.close(&[str_val("10.0.0.1")], CloseReason::Eos).unwrap();
    assert!(!out.event_ok);
    assert!(out.close_ok);
    assert_eq!(out.close_reason, CloseReason::Eos);
}

#[test]
fn close_step_accumulation() {
    // sum(bytes) accumulated during advance, verified at close time
    let plan = plan_with_close(
        vec![simple_key("sip")],
        vec![step(vec![branch("req", count_ge(1.0))])],
        // close step: sum(bytes) >= 1000
        vec![step(vec![BranchPlan {
            label: None,
            source: "traffic".to_string(),
            field: Some(FieldSelector::Dot("bytes".to_string())),
            guard: None,
            agg: AggPlan {
                transforms: vec![],
                measure: Measure::Sum,
                cmp: CmpOp::Ge,
                threshold: Expr::Number(1000.0),
            },
        }])],
        Duration::from_secs(60),
    );
    let mut sm = CepStateMachine::new("rule17".to_string(), plan, None);
    let base: i64 = 1_700_000_000 * NANOS_PER_SEC;

    let req = event(vec![("sip", str_val("10.0.0.1"))]);
    sm.advance_at("req", &req, base);

    // Accumulate traffic
    let mk = |bytes: f64| event(vec![("sip", str_val("10.0.0.1")), ("bytes", num(bytes))]);
    sm.advance_at("traffic", &mk(400.0), base);
    sm.advance_at("traffic", &mk(700.0), base);

    // Close: sum = 1100 >= 1000 → close_ok
    let out = sm
        .close(&[str_val("10.0.0.1")], CloseReason::Timeout)
        .unwrap();
    assert!(out.event_ok);
    assert!(out.close_ok);
    assert!((out.close_step_data[0].measure_value - 1100.0).abs() < f64::EPSILON);
}

#[test]
fn close_reason_guard_filters() {
    // timeout guard passes for Timeout, fails for Flush
    let plan = plan_with_close(
        vec![simple_key("sip")],
        vec![step(vec![branch("req", count_ge(1.0))])],
        // close step guarded on close_reason == "timeout"
        vec![step(vec![BranchPlan {
            label: None,
            source: "resp".to_string(),
            field: None,
            guard: Some(close_reason_guard("timeout")),
            agg: AggPlan {
                transforms: vec![],
                measure: Measure::Count,
                cmp: CmpOp::Eq,
                threshold: Expr::Number(0.0),
            },
        }])],
        Duration::from_secs(60),
    );

    // Scenario 1: close with Timeout → guard passes → close_ok
    let mut sm1 = CepStateMachine::new("rule18a".to_string(), plan.clone(), None);
    let base: i64 = 1_700_000_000 * NANOS_PER_SEC;
    let req = event(vec![("sip", str_val("10.0.0.1"))]);
    sm1.advance_at("req", &req, base);
    let out1 = sm1
        .close(&[str_val("10.0.0.1")], CloseReason::Timeout)
        .unwrap();
    assert!(out1.close_ok);

    // Scenario 2: close with Flush → guard fails → close_ok=false
    let mut sm2 = CepStateMachine::new("rule18b".to_string(), plan, None);
    sm2.advance_at("req", &req, base);
    let out2 = sm2
        .close(&[str_val("10.0.0.1")], CloseReason::Flush)
        .unwrap();
    assert!(!out2.close_ok);
}

#[test]
fn scan_expired_only_removes_expired() {
    let plan = plan_with_close(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(5.0))])],
        vec![],
        Duration::from_secs(60),
    );
    let mut sm = CepStateMachine::new("rule19".to_string(), plan, None);
    let base: i64 = 1_700_000_000 * NANOS_PER_SEC;

    // Create two instances at different times
    let e1 = event(vec![("sip", str_val("10.0.0.1"))]);
    let e2 = event(vec![("sip", str_val("10.0.0.2"))]);
    sm.advance_at("fail", &e1, base);
    sm.advance_at("fail", &e2, base + 40 * NANOS_PER_SEC);
    assert_eq!(sm.instance_count(), 2);

    // At base+61s: only first instance is expired (created at base, 61s ago)
    // Second instance was created at base+40s, only 21s ago → not expired
    let scan_time = base + 61 * NANOS_PER_SEC;
    let expired = sm.scan_expired_at(scan_time);
    assert_eq!(expired.len(), 1);
    assert_eq!(expired[0].scope_key, vec![str_val("10.0.0.1")]);
    assert_eq!(sm.instance_count(), 1);

    // At base+101s: second instance now expired too (created at base+40s, 61s ago)
    let scan_time2 = base + 101 * NANOS_PER_SEC;
    let expired2 = sm.scan_expired_at(scan_time2);
    assert_eq!(expired2.len(), 1);
    assert_eq!(expired2[0].scope_key, vec![str_val("10.0.0.2")]);
    assert_eq!(sm.instance_count(), 0);
}

#[test]
fn close_removes_instance() {
    let plan = plan_with_close(
        vec![simple_key("sip")],
        vec![step(vec![branch("req", count_ge(1.0))])],
        vec![step(vec![branch("resp", count_ge(0.0))])],
        Duration::from_secs(60),
    );
    let mut sm = CepStateMachine::new("rule20".to_string(), plan, None);
    let base: i64 = 1_700_000_000 * NANOS_PER_SEC;

    let req = event(vec![("sip", str_val("10.0.0.1"))]);
    sm.advance_at("req", &req, base);
    assert_eq!(sm.instance_count(), 1);

    // Close removes the instance
    let out = sm.close(&[str_val("10.0.0.1")], CloseReason::Flush);
    assert!(out.is_some());
    assert_eq!(sm.instance_count(), 0);

    // Closing again → None
    let out2 = sm.close(&[str_val("10.0.0.1")], CloseReason::Flush);
    assert!(out2.is_none());
}

#[test]
fn multiple_close_steps_all_must_pass() {
    // Two close steps, both must satisfy for close_ok=true
    let plan = plan_with_close(
        vec![simple_key("sip")],
        vec![step(vec![branch("req", count_ge(1.0))])],
        vec![
            // close step 1: resp count >= 1
            step(vec![branch("resp", count_ge(1.0))]),
            // close step 2: error count == 0
            step(vec![BranchPlan {
                label: None,
                source: "error".to_string(),
                field: None,
                guard: None,
                agg: AggPlan {
                    transforms: vec![],
                    measure: Measure::Count,
                    cmp: CmpOp::Eq,
                    threshold: Expr::Number(0.0),
                },
            }]),
        ],
        Duration::from_secs(60),
    );

    let base: i64 = 1_700_000_000 * NANOS_PER_SEC;
    let req = event(vec![("sip", str_val("10.0.0.1"))]);
    let resp = event(vec![("sip", str_val("10.0.0.1"))]);

    // Scenario A: resp arrives, no error → both close steps pass
    let mut sm_a = CepStateMachine::new("rule21a".to_string(), plan.clone(), None);
    sm_a.advance_at("req", &req, base);
    sm_a.advance_at("resp", &resp, base);
    let out_a = sm_a
        .close(&[str_val("10.0.0.1")], CloseReason::Timeout)
        .unwrap();
    assert!(out_a.event_ok);
    assert!(out_a.close_ok);

    // Scenario B: no resp → close step 1 fails (count 0 < 1)
    let mut sm_b = CepStateMachine::new("rule21b".to_string(), plan.clone(), None);
    sm_b.advance_at("req", &req, base);
    let out_b = sm_b
        .close(&[str_val("10.0.0.1")], CloseReason::Timeout)
        .unwrap();
    assert!(out_b.event_ok);
    assert!(!out_b.close_ok);

    // Scenario C: resp arrives + error arrives → close step 2 fails (error count 1 != 0)
    let mut sm_c = CepStateMachine::new("rule21c".to_string(), plan, None);
    sm_c.advance_at("req", &req, base);
    sm_c.advance_at("resp", &resp, base);
    let err = event(vec![("sip", str_val("10.0.0.1"))]);
    sm_c.advance_at("error", &err, base);
    let out_c = sm_c
        .close(&[str_val("10.0.0.1")], CloseReason::Timeout)
        .unwrap();
    assert!(out_c.event_ok);
    assert!(!out_c.close_ok);
}

/// Regression: when tracked_bind_aliases contains the same alias as the step
/// branches, collect_alias_event is called on every event, but the step
/// evaluation should still complete normally.
#[test]
fn tracked_alias_same_as_branch_source_still_matches() {
    let mut plan = plan_with_close(
        vec![simple_key("sip")],
        // event step: count >= 2
        vec![step(vec![branch("c", count_ge(2.0))])],
        // close step: count >= 2
        vec![step(vec![branch("c", count_ge(2.0))])],
        Duration::from_secs(10),
    );
    // Simulate the compiler fix: tracked_bind_aliases contains "c"
    plan.tracked_bind_aliases = std::collections::HashSet::from(["c".to_string()]);

    let mut sm = CepStateMachine::new("tracked_alias_test".to_string(), plan, None);
    let base: i64 = 1_700_000_000 * NANOS_PER_SEC;

    let e = event(vec![("sip", str_val("10.0.0.1"))]);

    // First event: count=1, not enough
    assert_eq!(sm.advance_at("c", &e, base), StepResult::Accumulate);

    // Second event: count=2, event step matches, returns Advance in AND mode
    assert_eq!(sm.advance_at("c", &e, base + 1), StepResult::Advance);

    // Event step should be complete (event_ok=true)
    // Close the instance — close step should pass (count=2 >= 2)
    let out = sm
        .close(&[str_val("10.0.0.1")], CloseReason::Timeout)
        .unwrap();
    assert!(out.event_ok, "event_ok should be true");
    assert!(out.close_ok, "close_ok should be true");
}
