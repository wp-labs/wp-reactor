use super::*;

fn secs(n: i64) -> i64 {
    n * 1_000_000_000
}

fn session_plan(gap_secs: u64) -> wf_lang::plan::MatchPlan {
    wf_lang::plan::MatchPlan {
        keys: vec![FieldRef::Simple("k".to_string())],
        key_map: None,
        window_spec: wf_lang::plan::WindowSpec::Session(Duration::from_secs(gap_secs)),
        event_steps: vec![wf_lang::plan::StepPlan {
            branches: vec![wf_lang::plan::BranchPlan {
                label: None,
                source: "e".to_string(),
                field: None,
                guard: None,
                agg: wf_lang::plan::AggPlan {
                    transforms: vec![],
                    measure: wf_lang::ast::Measure::Count,
                    cmp: wf_lang::ast::CmpOp::Ge,
                    // Keep the instance alive (do not match on event path).
                    threshold: Expr::Number(100.0),
                },
            }],
        }],
        close_steps: vec![],
        close_mode: CloseMode::Or,
    }
}

#[test]
fn session_gap_uses_last_event_time_for_expiry() {
    let mut sm = CepStateMachine::new("r_session".to_string(), session_plan(10), None);
    let e = crate::rule::tests::helpers::event(vec![("k", Value::Str("a".to_string()))]);

    let _ = sm.advance_at("e", &e, secs(0));
    let _ = sm.advance_at("e", &e, secs(9));

    // If created_at were used, this would expire at t=10s. Session gap should keep it alive.
    assert!(sm.scan_expired_at(secs(10)).is_empty());
    assert!(sm.scan_expired_at(secs(18)).is_empty());

    let expired = sm.scan_expired_at(secs(19));
    assert_eq!(expired.len(), 1);
    assert_eq!(expired[0].close_reason, CloseReason::Timeout);
    assert_eq!(expired[0].watermark_nanos, secs(19)); // last_event(9s) + gap(10s)
}

#[test]
fn session_scan_expired_sorted_by_last_event_time() {
    let mut sm = CepStateMachine::new("r_session_sort".to_string(), session_plan(10), None);

    let a = crate::rule::tests::helpers::event(vec![("k", Value::Str("a".to_string()))]);
    let b = crate::rule::tests::helpers::event(vec![("k", Value::Str("b".to_string()))]);

    let _ = sm.advance_at("e", &a, secs(0));
    let _ = sm.advance_at("e", &b, secs(0));
    let _ = sm.advance_at("e", &a, secs(4));
    let _ = sm.advance_at("e", &b, secs(8));

    let expired = sm.scan_expired_at(secs(30));
    assert_eq!(expired.len(), 2);

    // Session windows sort by last_event_nanos, so key a (4s) comes before b (8s).
    assert_eq!(expired[0].scope_key, vec![Value::Str("a".to_string())]);
    assert_eq!(expired[1].scope_key, vec![Value::Str("b".to_string())]);
    assert_eq!(expired[0].watermark_nanos, secs(14)); // 4 + 10
    assert_eq!(expired[1].watermark_nanos, secs(18)); // 8 + 10
}
