//! Regression tests for P0/P1 bug fixes (22–34).

use std::time::Duration;

use wf_lang::ast::{BinOp, CmpOp, Expr, FieldRef, FieldSelector, Measure};
use wf_lang::plan::{AggPlan, BranchPlan};

use crate::rule::match_engine::{CepStateMachine, CloseReason, StepResult};

use super::helpers::*;

const NANOS_PER_SEC: i64 = 1_000_000_000;

// ---------------------------------------------------------------------------
// Guards: InList / FuncCall (22–25)
// ---------------------------------------------------------------------------

#[test]
fn guard_in_list() {
    // Guard `action in ("login", "logout")` should pass; "upload" should fail.
    let guard = Expr::InList {
        expr: Box::new(Expr::Field(FieldRef::Simple("action".to_string()))),
        list: vec![
            Expr::StringLit("login".to_string()),
            Expr::StringLit("logout".to_string()),
        ],
        negated: false,
    };

    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![BranchPlan {
            label: None,
            source: "auth".to_string(),
            field: None,
            guard: Some(guard),
            agg: count_ge(2.0),
        }])],
    );
    let mut sm = CepStateMachine::new("rule22".to_string(), plan, None);

    // "upload" not in list → skipped
    let upload = event(vec![
        ("sip", str_val("10.0.0.1")),
        ("action", str_val("upload")),
    ]);
    assert_eq!(sm.advance("auth", &upload), StepResult::Accumulate);
    assert_eq!(sm.advance("auth", &upload), StepResult::Accumulate);

    // "login" in list → counted
    let login = event(vec![
        ("sip", str_val("10.0.0.1")),
        ("action", str_val("login")),
    ]);
    assert_eq!(sm.advance("auth", &login), StepResult::Accumulate);
    assert!(matches!(sm.advance("auth", &login), StepResult::Matched(_)));
}

#[test]
fn guard_not_in_list() {
    // Guard `action not in ("success")` — should count everything except "success".
    let guard = Expr::InList {
        expr: Box::new(Expr::Field(FieldRef::Simple("action".to_string()))),
        list: vec![Expr::StringLit("success".to_string())],
        negated: true,
    };

    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![BranchPlan {
            label: None,
            source: "auth".to_string(),
            field: None,
            guard: Some(guard),
            agg: count_ge(1.0),
        }])],
    );
    let mut sm = CepStateMachine::new("rule23".to_string(), plan, None);

    // "success" → filtered out by NOT IN
    let ok = event(vec![
        ("sip", str_val("10.0.0.1")),
        ("action", str_val("success")),
    ]);
    assert_eq!(sm.advance("auth", &ok), StepResult::Accumulate);

    // "failed" → passes NOT IN → matched
    let fail = event(vec![
        ("sip", str_val("10.0.0.1")),
        ("action", str_val("failed")),
    ]);
    assert!(matches!(sm.advance("auth", &fail), StepResult::Matched(_)));
}

#[test]
fn guard_func_call_contains() {
    // Guard: `contains(cmd, "powershell")`
    let guard = Expr::FuncCall {
        qualifier: None,
        name: "contains".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("cmd".to_string())),
            Expr::StringLit("powershell".to_string()),
        ],
    };

    let plan = simple_plan(
        vec![simple_key("host")],
        vec![step(vec![BranchPlan {
            label: None,
            source: "proc".to_string(),
            field: None,
            guard: Some(guard),
            agg: count_ge(1.0),
        }])],
    );
    let mut sm = CepStateMachine::new("rule24".to_string(), plan, None);

    // cmd without "powershell" → skipped
    let notepad = event(vec![
        ("host", str_val("srv1")),
        ("cmd", str_val("notepad.exe")),
    ]);
    assert_eq!(sm.advance("proc", &notepad), StepResult::Accumulate);

    // cmd with "powershell" → matched
    let ps = event(vec![
        ("host", str_val("srv1")),
        ("cmd", str_val("powershell -enc abc")),
    ]);
    assert!(matches!(sm.advance("proc", &ps), StepResult::Matched(_)));
}

#[test]
fn guard_func_lower_in_list() {
    // Guard: `lower(proto) in ("tcp", "udp")` — tests nested FuncCall + InList
    let guard = Expr::InList {
        expr: Box::new(Expr::FuncCall {
            qualifier: None,
            name: "lower".to_string(),
            args: vec![Expr::Field(FieldRef::Simple("proto".to_string()))],
        }),
        list: vec![
            Expr::StringLit("tcp".to_string()),
            Expr::StringLit("udp".to_string()),
        ],
        negated: false,
    };

    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![BranchPlan {
            label: None,
            source: "conn".to_string(),
            field: None,
            guard: Some(guard),
            agg: count_ge(1.0),
        }])],
    );
    let mut sm = CepStateMachine::new("rule25".to_string(), plan, None);

    // "ICMP" → lower → "icmp" → not in list → skipped
    let icmp = event(vec![
        ("sip", str_val("10.0.0.1")),
        ("proto", str_val("ICMP")),
    ]);
    assert_eq!(sm.advance("conn", &icmp), StepResult::Accumulate);

    // "TCP" → lower → "tcp" → in list → matched
    let tcp = event(vec![
        ("sip", str_val("10.0.0.1")),
        ("proto", str_val("TCP")),
    ]);
    assert!(matches!(sm.advance("conn", &tcp), StepResult::Matched(_)));
}

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

// ---------------------------------------------------------------------------
// Close guard field access + scan_expired safety (28–31)
// ---------------------------------------------------------------------------

#[test]
fn close_guard_event_field() {
    // Close step guard referencing event field: `status == "error"`
    // Events with status != "error" should be filtered during accumulation.
    let plan = plan_with_close(
        vec![simple_key("sip")],
        vec![step(vec![branch("req", count_ge(1.0))])],
        vec![step(vec![BranchPlan {
            label: Some("errors_only".to_string()),
            source: "resp".to_string(),
            field: None,
            guard: Some(Expr::BinOp {
                op: BinOp::Eq,
                left: Box::new(Expr::Field(FieldRef::Simple("status".to_string()))),
                right: Box::new(Expr::StringLit("error".to_string())),
            }),
            agg: count_ge(2.0),
        }])],
        Duration::from_secs(60),
    );
    let mut sm = CepStateMachine::new("rule28".to_string(), plan, None);
    let base: i64 = 1_700_000_000 * NANOS_PER_SEC;

    // Event step: req arrives → Advance
    let req = event(vec![("sip", str_val("10.0.0.1"))]);
    assert_eq!(sm.advance_at("req", &req, base), StepResult::Advance);

    // Close step accumulation: success responses should be filtered by guard
    let resp_ok = event(vec![
        ("sip", str_val("10.0.0.1")),
        ("status", str_val("success")),
    ]);
    sm.advance_at("resp", &resp_ok, base);
    sm.advance_at("resp", &resp_ok, base);
    sm.advance_at("resp", &resp_ok, base);

    // Only errors should count
    let resp_err = event(vec![
        ("sip", str_val("10.0.0.1")),
        ("status", str_val("error")),
    ]);
    sm.advance_at("resp", &resp_err, base);
    sm.advance_at("resp", &resp_err, base);

    // Close: guard `status == "error"` references event field.
    // At close time, status is missing from synthetic event → permissive (passes).
    // 2 error events were accumulated (3 success filtered) → count=2 >= 2 → close_ok
    let out = sm
        .close(&[str_val("10.0.0.1")], CloseReason::Timeout)
        .unwrap();
    assert!(out.event_ok);
    assert!(out.close_ok);
    assert_eq!(out.close_step_data[0].measure_value, 2.0);
    assert_eq!(
        out.close_step_data[0].label,
        Some("errors_only".to_string())
    );
}

#[test]
fn close_guard_mixed_event_and_close_reason() {
    // Guard: `status == "error" && close_reason == "timeout"`
    // During accumulation: close_reason missing → None → permissive, status filters.
    // At close time: status missing → None → permissive, close_reason filters.
    let guard = Expr::BinOp {
        op: BinOp::And,
        left: Box::new(Expr::BinOp {
            op: BinOp::Eq,
            left: Box::new(Expr::Field(FieldRef::Simple("status".to_string()))),
            right: Box::new(Expr::StringLit("error".to_string())),
        }),
        right: Box::new(Expr::BinOp {
            op: BinOp::Eq,
            left: Box::new(Expr::Field(FieldRef::Simple("close_reason".to_string()))),
            right: Box::new(Expr::StringLit("timeout".to_string())),
        }),
    };

    let plan = plan_with_close(
        vec![simple_key("sip")],
        vec![step(vec![branch("req", count_ge(1.0))])],
        vec![step(vec![BranchPlan {
            label: None,
            source: "resp".to_string(),
            field: None,
            guard: Some(guard),
            agg: count_ge(1.0),
        }])],
        Duration::from_secs(60),
    );

    let base: i64 = 1_700_000_000 * NANOS_PER_SEC;
    let req = event(vec![("sip", str_val("10.0.0.1"))]);

    // Scenario A: error events + Timeout close → both parts satisfied → close_ok
    let mut sm_a = CepStateMachine::new("rule29a".to_string(), plan.clone(), None);
    sm_a.advance_at("req", &req, base);
    let resp_err = event(vec![
        ("sip", str_val("10.0.0.1")),
        ("status", str_val("error")),
    ]);
    sm_a.advance_at("resp", &resp_err, base);
    let out_a = sm_a
        .close(&[str_val("10.0.0.1")], CloseReason::Timeout)
        .unwrap();
    assert!(out_a.close_ok);

    // Scenario B: error events + Flush close → close_reason != "timeout" → close_ok=false
    let mut sm_b = CepStateMachine::new("rule29b".to_string(), plan.clone(), None);
    sm_b.advance_at("req", &req, base);
    sm_b.advance_at("resp", &resp_err, base);
    let out_b = sm_b
        .close(&[str_val("10.0.0.1")], CloseReason::Flush)
        .unwrap();
    assert!(!out_b.close_ok);

    // Scenario C: success events + Timeout → status filter blocks accumulation → count=0 < 1
    let mut sm_c = CepStateMachine::new("rule29c".to_string(), plan, None);
    sm_c.advance_at("req", &req, base);
    let resp_ok = event(vec![
        ("sip", str_val("10.0.0.1")),
        ("status", str_val("success")),
    ]);
    sm_c.advance_at("resp", &resp_ok, base);
    let out_c = sm_c
        .close(&[str_val("10.0.0.1")], CloseReason::Timeout)
        .unwrap();
    assert!(!out_c.close_ok);
}

#[test]
fn scan_expired_no_panic_on_future_created_at() {
    // Instance created at a time after watermark (out-of-order clocks) — must not panic.
    let plan = plan_with_close(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(5.0))])],
        vec![],
        Duration::from_secs(60),
    );
    let mut sm = CepStateMachine::new("rule30".to_string(), plan, None);
    let base: i64 = 1_700_000_000 * NANOS_PER_SEC;
    let future = base + 100 * NANOS_PER_SEC;
    let past = base;

    // Create instance at future time
    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    sm.advance_at("fail", &e, future);

    // Scan with `past` < `future` — should not panic, and instance should survive
    let expired = sm.scan_expired_at(past);
    assert!(expired.is_empty());
    assert_eq!(sm.instance_count(), 1);
}

#[test]
fn close_guard_close_reason_only_permissive() {
    // Guard that references ONLY close_reason should still accumulate all events
    // (close_reason not in event → None → permissive during accumulation)
    let plan = plan_with_close(
        vec![simple_key("sip")],
        vec![step(vec![branch("req", count_ge(1.0))])],
        vec![step(vec![BranchPlan {
            label: None,
            source: "resp".to_string(),
            field: None,
            guard: Some(close_reason_guard("timeout")),
            agg: count_ge(3.0),
        }])],
        Duration::from_secs(60),
    );
    let mut sm = CepStateMachine::new("rule31".to_string(), plan, None);
    let base: i64 = 1_700_000_000 * NANOS_PER_SEC;

    let req = event(vec![("sip", str_val("10.0.0.1"))]);
    sm.advance_at("req", &req, base);

    // 3 resp events — guard has close_reason which is missing from events,
    // so it should NOT block accumulation (permissive None)
    let resp = event(vec![("sip", str_val("10.0.0.1"))]);
    sm.advance_at("resp", &resp, base);
    sm.advance_at("resp", &resp, base);
    sm.advance_at("resp", &resp, base);

    // Close with Timeout → guard passes at close time → count=3 >= 3 → close_ok
    let out = sm
        .close(&[str_val("10.0.0.1")], CloseReason::Timeout)
        .unwrap();
    assert!(out.close_ok);
    assert_eq!(out.close_step_data[0].measure_value, 3.0);

    // Scenario B: same but close with Flush → close_reason guard fails
    let mut sm2 = CepStateMachine::new(
        "rule31b".to_string(),
        plan_with_close(
            vec![simple_key("sip")],
            vec![step(vec![branch("req", count_ge(1.0))])],
            vec![step(vec![BranchPlan {
                label: None,
                source: "resp".to_string(),
                field: None,
                guard: Some(close_reason_guard("timeout")),
                agg: count_ge(3.0),
            }])],
            Duration::from_secs(60),
        ),
        None,
    );
    sm2.advance_at("req", &req, base);
    sm2.advance_at("resp", &resp, base);
    sm2.advance_at("resp", &resp, base);
    sm2.advance_at("resp", &resp, base);
    let out2 = sm2
        .close(&[str_val("10.0.0.1")], CloseReason::Flush)
        .unwrap();
    assert!(!out2.close_ok);
}

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
