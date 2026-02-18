//! M15 close step / timeout tests (12–21).

use std::time::{Duration, Instant};

use wf_lang::ast::{CmpOp, Expr, FieldSelector, Measure};
use wf_lang::plan::{AggPlan, BranchPlan};

use crate::rule::match_engine::{CepStateMachine, CloseReason, StepResult};

use super::helpers::*;

#[test]
fn no_close_steps_preserves_m14() {
    // Empty close_steps → advance() returns Matched (backward compat)
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(2.0))])],
    );
    let mut sm = CepStateMachine::new("rule12".to_string(), plan);
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
    let mut sm = CepStateMachine::new("rule13".to_string(), plan);
    let now = Instant::now();

    // Send a request → event step completes, returns Advance (not Matched, close steps present)
    let req = event(vec![("sip", str_val("10.0.0.1"))]);
    let result = sm.advance_with_instant("req", &req, now);
    assert_eq!(result, StepResult::Advance);
    assert_eq!(sm.instance_count(), 1);

    // No response arrives. Timeout triggers close.
    let close_time = now + Duration::from_secs(61);
    let expired = sm.scan_expired(close_time);
    assert_eq!(expired.len(), 1);

    let out = &expired[0];
    assert_eq!(out.rule_name, "rule13");
    assert_eq!(out.scope_key, vec!["10.0.0.1"]);
    assert_eq!(out.close_reason, CloseReason::Timeout);
    assert!(out.event_ok);
    assert!(out.close_ok);
    assert_eq!(out.event_step_data.len(), 1);
    assert_eq!(out.close_step_data.len(), 1);
    assert_eq!(out.close_step_data[0].label, Some("no_resp".to_string()));
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
    let mut sm = CepStateMachine::new("rule14".to_string(), plan);
    let now = Instant::now();

    // Create an instance
    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    sm.advance_with_instant("fail", &e, now);
    assert_eq!(sm.instance_count(), 1);

    // Scan before expiry — nothing removed
    let before = now + Duration::from_secs(29);
    let expired = sm.scan_expired(before);
    assert!(expired.is_empty());
    assert_eq!(sm.instance_count(), 1);

    // Scan after expiry — instance removed
    let after = now + Duration::from_secs(31);
    let expired = sm.scan_expired(after);
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
    let mut sm = CepStateMachine::new("rule15".to_string(), plan);
    let now = Instant::now();

    let req = event(vec![("sip", str_val("10.0.0.1"))]);
    let resp = event(vec![("sip", str_val("10.0.0.1"))]);

    // req → Advance (event step done)
    assert_eq!(sm.advance_with_instant("req", &req, now), StepResult::Advance);

    // 2 resp events → accumulate close step data
    assert_eq!(sm.advance_with_instant("resp", &resp, now), StepResult::Accumulate);
    assert_eq!(sm.advance_with_instant("resp", &resp, now), StepResult::Accumulate);

    // Close explicitly
    let out = sm.close(&["10.0.0.1".to_string()], CloseReason::Flush).unwrap();
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
    let mut sm = CepStateMachine::new("rule16".to_string(), plan);
    let now = Instant::now();

    // One event — event step not yet satisfied (needs 5)
    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    sm.advance_with_instant("fail", &e, now);
    assert_eq!(sm.instance_count(), 1);

    // Close — event_ok=false, close_ok=true (resp count == 0 is true)
    let out = sm.close(&["10.0.0.1".to_string()], CloseReason::Eos).unwrap();
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
    let mut sm = CepStateMachine::new("rule17".to_string(), plan);
    let now = Instant::now();

    let req = event(vec![("sip", str_val("10.0.0.1"))]);
    sm.advance_with_instant("req", &req, now);

    // Accumulate traffic
    let mk = |bytes: f64| {
        event(vec![
            ("sip", str_val("10.0.0.1")),
            ("bytes", num(bytes)),
        ])
    };
    sm.advance_with_instant("traffic", &mk(400.0), now);
    sm.advance_with_instant("traffic", &mk(700.0), now);

    // Close: sum = 1100 >= 1000 → close_ok
    let out = sm.close(&["10.0.0.1".to_string()], CloseReason::Timeout).unwrap();
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
    let mut sm1 = CepStateMachine::new("rule18a".to_string(), plan.clone());
    let now = Instant::now();
    let req = event(vec![("sip", str_val("10.0.0.1"))]);
    sm1.advance_with_instant("req", &req, now);
    let out1 = sm1.close(&["10.0.0.1".to_string()], CloseReason::Timeout).unwrap();
    assert!(out1.close_ok);

    // Scenario 2: close with Flush → guard fails → close_ok=false
    let mut sm2 = CepStateMachine::new("rule18b".to_string(), plan);
    sm2.advance_with_instant("req", &req, now);
    let out2 = sm2.close(&["10.0.0.1".to_string()], CloseReason::Flush).unwrap();
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
    let mut sm = CepStateMachine::new("rule19".to_string(), plan);
    let now = Instant::now();

    // Create two instances at different times
    let e1 = event(vec![("sip", str_val("10.0.0.1"))]);
    let e2 = event(vec![("sip", str_val("10.0.0.2"))]);
    sm.advance_with_instant("fail", &e1, now);
    sm.advance_with_instant("fail", &e2, now + Duration::from_secs(40));
    assert_eq!(sm.instance_count(), 2);

    // At now+61: only first instance is expired (created at now, 61s ago)
    // Second instance was created at now+40, only 21s ago → not expired
    let scan_time = now + Duration::from_secs(61);
    let expired = sm.scan_expired(scan_time);
    assert_eq!(expired.len(), 1);
    assert_eq!(expired[0].scope_key, vec!["10.0.0.1"]);
    assert_eq!(sm.instance_count(), 1);

    // At now+101: second instance now expired too (created at now+40, 61s ago)
    let scan_time2 = now + Duration::from_secs(101);
    let expired2 = sm.scan_expired(scan_time2);
    assert_eq!(expired2.len(), 1);
    assert_eq!(expired2[0].scope_key, vec!["10.0.0.2"]);
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
    let mut sm = CepStateMachine::new("rule20".to_string(), plan);
    let now = Instant::now();

    let req = event(vec![("sip", str_val("10.0.0.1"))]);
    sm.advance_with_instant("req", &req, now);
    assert_eq!(sm.instance_count(), 1);

    // Close removes the instance
    let out = sm.close(&["10.0.0.1".to_string()], CloseReason::Flush);
    assert!(out.is_some());
    assert_eq!(sm.instance_count(), 0);

    // Closing again → None
    let out2 = sm.close(&["10.0.0.1".to_string()], CloseReason::Flush);
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

    let now = Instant::now();
    let req = event(vec![("sip", str_val("10.0.0.1"))]);
    let resp = event(vec![("sip", str_val("10.0.0.1"))]);

    // Scenario A: resp arrives, no error → both close steps pass
    let mut sm_a = CepStateMachine::new("rule21a".to_string(), plan.clone());
    sm_a.advance_with_instant("req", &req, now);
    sm_a.advance_with_instant("resp", &resp, now);
    let out_a = sm_a.close(&["10.0.0.1".to_string()], CloseReason::Timeout).unwrap();
    assert!(out_a.event_ok);
    assert!(out_a.close_ok);

    // Scenario B: no resp → close step 1 fails (count 0 < 1)
    let mut sm_b = CepStateMachine::new("rule21b".to_string(), plan.clone());
    sm_b.advance_with_instant("req", &req, now);
    let out_b = sm_b.close(&["10.0.0.1".to_string()], CloseReason::Timeout).unwrap();
    assert!(out_b.event_ok);
    assert!(!out_b.close_ok);

    // Scenario C: resp arrives + error arrives → close step 2 fails (error count 1 != 0)
    let mut sm_c = CepStateMachine::new("rule21c".to_string(), plan);
    sm_c.advance_with_instant("req", &req, now);
    sm_c.advance_with_instant("resp", &resp, now);
    let err = event(vec![("sip", str_val("10.0.0.1"))]);
    sm_c.advance_with_instant("error", &err, now);
    let out_c = sm_c.close(&["10.0.0.1".to_string()], CloseReason::Timeout).unwrap();
    assert!(out_c.event_ok);
    assert!(!out_c.close_ok);
}
