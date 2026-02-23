use super::*;

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
