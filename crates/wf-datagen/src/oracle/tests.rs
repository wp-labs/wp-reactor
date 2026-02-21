use std::time::Duration;

use chrono::Utc;
use wf_lang::ast::{CmpOp, Expr, FieldRef, Measure};
use wf_lang::plan::{
    AggPlan, BindPlan, BranchPlan, EntityPlan, MatchPlan, RulePlan, ScorePlan, StepPlan,
    WindowSpec, YieldPlan,
};

use crate::datagen::stream_gen::GenEvent;
use crate::oracle::run_oracle;

fn make_simple_rule_plan() -> RulePlan {
    RulePlan {
        name: "brute_force".to_string(),
        binds: vec![BindPlan {
            alias: "fail".to_string(),
            window: "LoginWindow".to_string(),
            filter: None,
        }],
        match_plan: MatchPlan {
            keys: vec![FieldRef::Simple("sip".to_string())],
            window_spec: WindowSpec::Sliding(Duration::from_secs(300)),
            event_steps: vec![StepPlan {
                branches: vec![BranchPlan {
                    label: Some("fail_count".to_string()),
                    source: "fail".to_string(),
                    field: None,
                    guard: None,
                    agg: AggPlan {
                        transforms: vec![],
                        measure: Measure::Count,
                        cmp: CmpOp::Ge,
                        threshold: Expr::Number(3.0),
                    },
                }],
            }],
            close_steps: vec![],
        },
        joins: vec![],
        entity_plan: EntityPlan {
            entity_type: "ip".to_string(),
            entity_id_expr: Expr::Field(FieldRef::Simple("sip".to_string())),
        },
        yield_plan: YieldPlan {
            target: "alerts".to_string(),
            fields: vec![],
        },
        score_plan: ScorePlan {
            expr: Expr::Number(85.0),
        },
        conv_plan: None,
    }
}

fn make_event(alias: &str, window: &str, sip: &str, ts: &str) -> GenEvent {
    let mut fields = serde_json::Map::new();
    fields.insert(
        "sip".to_string(),
        serde_json::Value::String(sip.to_string()),
    );
    fields.insert(
        "timestamp".to_string(),
        serde_json::Value::String(ts.to_string()),
    );

    GenEvent {
        stream_alias: alias.to_string(),
        window_name: window.to_string(),
        timestamp: ts.parse().unwrap(),
        fields,
    }
}

#[test]
fn hit_cluster_triggers_alert() {
    let plan = make_simple_rule_plan();
    let start: chrono::DateTime<Utc> = "2024-01-01T00:00:00Z".parse().unwrap();
    let duration = Duration::from_secs(3600);

    // 3 events with same key → should trigger
    let events = vec![
        make_event("s1", "LoginWindow", "10.0.0.1", "2024-01-01T00:01:00Z"),
        make_event("s1", "LoginWindow", "10.0.0.1", "2024-01-01T00:02:00Z"),
        make_event("s1", "LoginWindow", "10.0.0.1", "2024-01-01T00:03:00Z"),
    ];

    let result = run_oracle(&events, &[plan], &start, &duration, None).unwrap();
    assert_eq!(result.alerts.len(), 1);
    assert_eq!(result.alerts[0].rule_name, "brute_force");
    assert_eq!(result.alerts[0].entity_id, "10.0.0.1");
    assert!((result.alerts[0].score - 85.0).abs() < f64::EPSILON);
    assert!(result.alerts[0].close_reason.is_none());
}

#[test]
fn near_miss_no_alert() {
    let plan = make_simple_rule_plan();
    let start: chrono::DateTime<Utc> = "2024-01-01T00:00:00Z".parse().unwrap();
    let duration = Duration::from_secs(3600);

    // 2 events (threshold is 3) → should NOT trigger
    let events = vec![
        make_event("s1", "LoginWindow", "10.0.0.1", "2024-01-01T00:01:00Z"),
        make_event("s1", "LoginWindow", "10.0.0.1", "2024-01-01T00:02:00Z"),
    ];

    let result = run_oracle(&events, &[plan], &start, &duration, None).unwrap();
    assert_eq!(result.alerts.len(), 0);
}

#[test]
fn different_keys_isolated() {
    let plan = make_simple_rule_plan();
    let start: chrono::DateTime<Utc> = "2024-01-01T00:00:00Z".parse().unwrap();
    let duration = Duration::from_secs(3600);

    // 2 events each for two different IPs → neither triggers (threshold=3)
    let events = vec![
        make_event("s1", "LoginWindow", "10.0.0.1", "2024-01-01T00:01:00Z"),
        make_event("s1", "LoginWindow", "10.0.0.2", "2024-01-01T00:01:30Z"),
        make_event("s1", "LoginWindow", "10.0.0.1", "2024-01-01T00:02:00Z"),
        make_event("s1", "LoginWindow", "10.0.0.2", "2024-01-01T00:02:30Z"),
    ];

    let result = run_oracle(&events, &[plan], &start, &duration, None).unwrap();
    assert_eq!(result.alerts.len(), 0);
}

#[test]
fn empty_events_no_alerts() {
    let plan = make_simple_rule_plan();
    let start: chrono::DateTime<Utc> = "2024-01-01T00:00:00Z".parse().unwrap();
    let duration = Duration::from_secs(3600);

    let result = run_oracle(&[], &[plan], &start, &duration, None).unwrap();
    assert_eq!(result.alerts.len(), 0);
}

#[test]
fn multi_alias_same_window_both_receive_events() {
    // Rule with two binds on the same window: "a" and "b" both reference LoginWindow.
    // Step 1 uses "a" (count >= 2), step 2 uses "b" (count >= 2).
    // All events come from LoginWindow, so both aliases must receive them.
    let plan = RulePlan {
        name: "multi_bind".to_string(),
        binds: vec![
            BindPlan {
                alias: "a".to_string(),
                window: "LoginWindow".to_string(),
                filter: None,
            },
            BindPlan {
                alias: "b".to_string(),
                window: "LoginWindow".to_string(),
                filter: None,
            },
        ],
        match_plan: MatchPlan {
            keys: vec![FieldRef::Simple("sip".to_string())],
            window_spec: WindowSpec::Sliding(Duration::from_secs(300)),
            event_steps: vec![
                StepPlan {
                    branches: vec![BranchPlan {
                        label: Some("step_a".to_string()),
                        source: "a".to_string(),
                        field: None,
                        guard: None,
                        agg: AggPlan {
                            transforms: vec![],
                            measure: Measure::Count,
                            cmp: CmpOp::Ge,
                            threshold: Expr::Number(2.0),
                        },
                    }],
                },
                StepPlan {
                    branches: vec![BranchPlan {
                        label: Some("step_b".to_string()),
                        source: "b".to_string(),
                        field: None,
                        guard: None,
                        agg: AggPlan {
                            transforms: vec![],
                            measure: Measure::Count,
                            cmp: CmpOp::Ge,
                            threshold: Expr::Number(2.0),
                        },
                    }],
                },
            ],
            close_steps: vec![],
        },
        joins: vec![],
        entity_plan: EntityPlan {
            entity_type: "ip".to_string(),
            entity_id_expr: Expr::Field(FieldRef::Simple("sip".to_string())),
        },
        yield_plan: YieldPlan {
            target: "alerts".to_string(),
            fields: vec![],
        },
        score_plan: ScorePlan {
            expr: Expr::Number(90.0),
        },
        conv_plan: None,
    };

    let start: chrono::DateTime<Utc> = "2024-01-01T00:00:00Z".parse().unwrap();
    let duration = Duration::from_secs(3600);

    // 4 events to LoginWindow → alias "a" gets 4, alias "b" gets 4.
    // Step 1 (a >= 2) triggers after event 2, step 2 (b >= 2) triggers after event 4.
    let events = vec![
        make_event("s1", "LoginWindow", "10.0.0.1", "2024-01-01T00:01:00Z"),
        make_event("s1", "LoginWindow", "10.0.0.1", "2024-01-01T00:02:00Z"),
        make_event("s1", "LoginWindow", "10.0.0.1", "2024-01-01T00:03:00Z"),
        make_event("s1", "LoginWindow", "10.0.0.1", "2024-01-01T00:04:00Z"),
    ];

    let result = run_oracle(&events, &[plan], &start, &duration, None).unwrap();

    // With the old single-alias map, alias "b" would never receive events
    // and the rule would never fully match. With the fix, both aliases
    // receive events and the multi-step rule completes.
    assert!(
        !result.alerts.is_empty(),
        "multi-alias same-window rule should trigger when both aliases receive events"
    );
    assert_eq!(result.alerts[0].rule_name, "multi_bind");
}

#[test]
fn sc7_uninjected_rule_skipped() {
    let plan = make_simple_rule_plan(); // name = "brute_force"
    let start: chrono::DateTime<Utc> = "2024-01-01T00:00:00Z".parse().unwrap();
    let duration = Duration::from_secs(3600);

    // 3 events that would trigger the rule
    let events = vec![
        make_event("s1", "LoginWindow", "10.0.0.1", "2024-01-01T00:01:00Z"),
        make_event("s1", "LoginWindow", "10.0.0.1", "2024-01-01T00:02:00Z"),
        make_event("s1", "LoginWindow", "10.0.0.1", "2024-01-01T00:03:00Z"),
    ];

    // With injected_rules containing "brute_force" → alert generated
    let injected: std::collections::HashSet<String> =
        ["brute_force".to_string()].into_iter().collect();
    let result = run_oracle(&events, &[plan.clone()], &start, &duration, Some(&injected)).unwrap();
    assert_eq!(result.alerts.len(), 1);

    // With injected_rules NOT containing "brute_force" → no alert (SC7)
    let other: std::collections::HashSet<String> =
        ["some_other_rule".to_string()].into_iter().collect();
    let result = run_oracle(&events, &[plan], &start, &duration, Some(&other)).unwrap();
    assert_eq!(result.alerts.len(), 0);
}
