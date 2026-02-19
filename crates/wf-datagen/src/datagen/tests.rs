use std::time::Duration;

use wf_lang::ast::{CmpOp, Expr, FieldRef, Measure};
use wf_lang::plan::{
    AggPlan, BindPlan, BranchPlan, EntityPlan, MatchPlan, RulePlan, ScorePlan, StepPlan,
    WindowSpec, YieldPlan,
};
use wf_lang::{BaseType, FieldDef, FieldType, WindowSchema};

use super::generate;
use crate::wfg_parser::parse_wfg;

fn make_login_schema() -> WindowSchema {
    WindowSchema {
        name: "LoginWindow".to_string(),
        streams: vec!["login_events".to_string()],
        time_field: Some("timestamp".to_string()),
        over: Duration::from_secs(300),
        fields: vec![
            FieldDef {
                name: "timestamp".to_string(),
                field_type: FieldType::Base(BaseType::Time),
            },
            FieldDef {
                name: "src_ip".to_string(),
                field_type: FieldType::Base(BaseType::Ip),
            },
            FieldDef {
                name: "username".to_string(),
                field_type: FieldType::Base(BaseType::Chars),
            },
            FieldDef {
                name: "success".to_string(),
                field_type: FieldType::Base(BaseType::Bool),
            },
            FieldDef {
                name: "attempts".to_string(),
                field_type: FieldType::Base(BaseType::Digit),
            },
            FieldDef {
                name: "score".to_string(),
                field_type: FieldType::Base(BaseType::Float),
            },
            FieldDef {
                name: "request_id".to_string(),
                field_type: FieldType::Base(BaseType::Hex),
            },
        ],
    }
}

#[test]
fn test_same_seed_same_output() {
    let input = r#"
scenario deterministic seed 42 {
    time "2024-01-01T00:00:00Z" duration 1h
    total 100
    stream s1 : LoginWindow 10/s
}
"#;
    let wfg = parse_wfg(input).unwrap();
    let schemas = vec![make_login_schema()];

    let result1 = generate(&wfg, &schemas, &[]).unwrap();
    let result2 = generate(&wfg, &schemas, &[]).unwrap();

    assert_eq!(result1.events.len(), result2.events.len());
    for (e1, e2) in result1.events.iter().zip(result2.events.iter()) {
        assert_eq!(e1.timestamp, e2.timestamp);
        assert_eq!(e1.fields, e2.fields);
    }
}

#[test]
fn test_different_seed_different_output() {
    let input1 = r#"
scenario seed_a seed 42 {
    time "2024-01-01T00:00:00Z" duration 1h
    total 50
    stream s1 : LoginWindow 10/s
}
"#;
    let input2 = r#"
scenario seed_b seed 99 {
    time "2024-01-01T00:00:00Z" duration 1h
    total 50
    stream s1 : LoginWindow 10/s
}
"#;
    let wsc1 = parse_wfg(input1).unwrap();
    let wsc2 = parse_wfg(input2).unwrap();
    let schemas = vec![make_login_schema()];

    let result1 = generate(&wsc1, &schemas, &[]).unwrap();
    let result2 = generate(&wsc2, &schemas, &[]).unwrap();

    assert_eq!(result1.events.len(), result2.events.len());
    // At least some fields should differ
    let mut any_different = false;
    for (e1, e2) in result1.events.iter().zip(result2.events.iter()) {
        if e1.fields != e2.fields {
            any_different = true;
            break;
        }
    }
    assert!(
        any_different,
        "different seeds should produce different output"
    );
}

#[test]
fn test_correct_event_count() {
    let input = r#"
scenario count_test seed 1 {
    time "2024-01-01T00:00:00Z" duration 1h
    total 200
    stream s1 : LoginWindow 10/s
}
"#;
    let wfg = parse_wfg(input).unwrap();
    let schemas = vec![make_login_schema()];

    let result = generate(&wfg, &schemas, &[]).unwrap();
    assert_eq!(result.events.len(), 200);
}

#[test]
fn test_field_types_correct() {
    let input = r#"
scenario types_test seed 7 {
    time "2024-01-01T00:00:00Z" duration 1h
    total 10
    stream s1 : LoginWindow 10/s
}
"#;
    let wfg = parse_wfg(input).unwrap();
    let schemas = vec![make_login_schema()];

    let result = generate(&wfg, &schemas, &[]).unwrap();
    assert!(!result.events.is_empty());

    let event = &result.events[0];
    // Time field should be a string (ISO8601)
    assert!(event.fields["timestamp"].is_string());
    // IP field should be a string
    assert!(event.fields["src_ip"].is_string());
    // Chars field should be a string
    assert!(event.fields["username"].is_string());
    // Bool field should be a boolean
    assert!(event.fields["success"].is_boolean());
    // Digit field should be a number
    assert!(event.fields["attempts"].is_number());
    // Float field should be a number
    assert!(event.fields["score"].is_number());
    // Hex field should be a string
    assert!(event.fields["request_id"].is_string());
}

#[test]
fn test_events_sorted_by_time() {
    let input = r#"
scenario sorted_test seed 42 {
    time "2024-01-01T00:00:00Z" duration 1h
    total 100
    stream s1 : LoginWindow 10/s
}
"#;
    let wfg = parse_wfg(input).unwrap();
    let schemas = vec![make_login_schema()];

    let result = generate(&wfg, &schemas, &[]).unwrap();
    for window in result.events.windows(2) {
        assert!(window[0].timestamp <= window[1].timestamp);
    }
}

#[test]
fn test_multiple_streams_distribution() {
    let schema2 = WindowSchema {
        name: "DnsWindow".to_string(),
        streams: vec!["dns_events".to_string()],
        time_field: Some("timestamp".to_string()),
        over: Duration::from_secs(300),
        fields: vec![
            FieldDef {
                name: "timestamp".to_string(),
                field_type: FieldType::Base(BaseType::Time),
            },
            FieldDef {
                name: "query".to_string(),
                field_type: FieldType::Base(BaseType::Chars),
            },
        ],
    };

    let input = r#"
scenario multi_stream seed 42 {
    time "2024-01-01T00:00:00Z" duration 1h
    total 300

    stream s1 : LoginWindow 20/s
    stream s2 : DnsWindow 10/s
}
"#;
    let wfg = parse_wfg(input).unwrap();
    let schemas = vec![make_login_schema(), schema2];

    let result = generate(&wfg, &schemas, &[]).unwrap();
    assert_eq!(result.events.len(), 300);

    let login_count = result
        .events
        .iter()
        .filter(|e| e.window_name == "LoginWindow")
        .count();
    let dns_count = result
        .events
        .iter()
        .filter(|e| e.window_name == "DnsWindow")
        .count();

    // Rate ratio is 2:1, so login should get ~200, dns ~100
    assert_eq!(login_count + dns_count, 300);
    assert!(
        login_count > dns_count,
        "LoginWindow should have more events"
    );
}

#[test]
fn test_enum_named_values_arg() {
    let input = r#"
scenario enum_values seed 7 {
    time "2024-01-01T00:00:00Z" duration 1h
    total 50
    stream s1 : LoginWindow 10/s {
        username = enum(values: "alice,bob,charlie")
    }
}
"#;
    let wfg = parse_wfg(input).unwrap();
    let schemas = vec![make_login_schema()];

    let result = generate(&wfg, &schemas, &[]).unwrap();
    assert!(!result.events.is_empty());
    for event in &result.events {
        let user = event.fields["username"].as_str().unwrap();
        assert!(matches!(user, "alice" | "bob" | "charlie"));
    }
}

// ---------------------------------------------------------------------------
// Inject generation tests
// ---------------------------------------------------------------------------

fn make_brute_force_plan() -> RulePlan {
    RulePlan {
        name: "brute_force".to_string(),
        binds: vec![BindPlan {
            alias: "fail".to_string(),
            window: "LoginWindow".to_string(),
            filter: None,
        }],
        match_plan: MatchPlan {
            keys: vec![FieldRef::Simple("src_ip".to_string())],
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
                        threshold: Expr::Number(5.0),
                    },
                }],
            }],
            close_steps: vec![],
        },
        joins: vec![],
        entity_plan: EntityPlan {
            entity_type: "ip".to_string(),
            entity_id_expr: Expr::Field(FieldRef::Simple("src_ip".to_string())),
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

#[test]
fn test_inject_hit_cluster_correctness() {
    let input = r#"
scenario inject_hit seed 42 {
    time "2024-01-01T00:00:00Z" duration 1h
    total 1000
    stream s1 : LoginWindow 100/s
    inject for brute_force on [s1] {
        hit 50%;
    }
}
"#;
    let wfg = parse_wfg(input).unwrap();
    let schemas = vec![make_login_schema()];
    let plans = vec![make_brute_force_plan()];

    let result = generate(&wfg, &schemas, &plans).unwrap();
    assert_eq!(result.events.len(), 1000);

    // Hit clusters: 1000 * 50% = 500 events / 5 per cluster = 100 clusters
    // Count events that are inject hit events by checking src_ip pattern
    let hit_events: Vec<_> = result
        .events
        .iter()
        .filter(|e| {
            e.fields
                .get("src_ip")
                .and_then(|v| v.as_str())
                .map(|s| s.starts_with("10."))
                .unwrap_or(false)
                && e.fields
                    .get("src_ip")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .len()
                    <= 15 // typical inject IP pattern
        })
        .collect();

    // At minimum we should have some hit events
    assert!(
        hit_events.len() >= 100,
        "expected at least 100 hit events, got {}",
        hit_events.len()
    );

    // All events should be sorted by timestamp
    for w in result.events.windows(2) {
        assert!(w[0].timestamp <= w[1].timestamp);
    }
}

#[test]
fn test_inject_near_miss_no_trigger() {
    // near-miss events should produce N-1 events per cluster (not enough to trigger)
    let input = r#"
scenario inject_nm seed 42 {
    time "2024-01-01T00:00:00Z" duration 1h
    total 1000
    stream s1 : LoginWindow 100/s
    inject for brute_force on [s1] {
        near_miss 40%;
    }
}
"#;
    let wfg = parse_wfg(input).unwrap();
    let schemas = vec![make_login_schema()];
    let plans = vec![make_brute_force_plan()];

    let result = generate(&wfg, &schemas, &plans).unwrap();
    assert_eq!(result.events.len(), 1000);

    // Run oracle — near-miss clusters should NOT produce alerts
    use crate::oracle::run_oracle;
    let start = "2024-01-01T00:00:00Z".parse().unwrap();
    let duration = Duration::from_secs(3600);
    let oracle = run_oracle(&result.events, &plans, &start, &duration).unwrap();
    assert_eq!(
        oracle.alerts.len(),
        0,
        "near-miss clusters should not trigger any alerts"
    );
}

#[test]
fn test_inject_hit_triggers_oracle() {
    let input = r#"
scenario inject_oracle seed 42 {
    time "2024-01-01T00:00:00Z" duration 1h
    total 1000
    stream s1 : LoginWindow 100/s
    inject for brute_force on [s1] {
        hit 50%;
    }
}
"#;
    let wfg = parse_wfg(input).unwrap();
    let schemas = vec![make_login_schema()];
    let plans = vec![make_brute_force_plan()];

    let result = generate(&wfg, &schemas, &plans).unwrap();

    // Run oracle — hit clusters should produce alerts
    use crate::oracle::run_oracle;
    let start = "2024-01-01T00:00:00Z".parse().unwrap();
    let duration = Duration::from_secs(3600);
    let oracle = run_oracle(&result.events, &plans, &start, &duration).unwrap();

    // 1000 events * 50% = 500 hit events / 5 per cluster = 100 clusters → 100 alerts
    assert_eq!(
        oracle.alerts.len(),
        100,
        "expected 100 alerts from 100 hit clusters, got {}",
        oracle.alerts.len()
    );

    // All alerts should have correct rule name and score
    for alert in &oracle.alerts {
        assert_eq!(alert.rule_name, "brute_force");
        assert!((alert.score - 85.0).abs() < f64::EPSILON);
        assert_eq!(alert.entity_type, "ip");
    }
}

#[test]
fn test_inject_budget_allocation() {
    // hit% + near_miss% + non_hit% should be accounted for; rest is background
    let input = r#"
scenario inject_budget seed 42 {
    time "2024-01-01T00:00:00Z" duration 1h
    total 1000
    stream s1 : LoginWindow 100/s
    inject for brute_force on [s1] {
        hit 30%;
        near_miss 10%;
        non_hit 20%;
    }
}
"#;
    let wfg = parse_wfg(input).unwrap();
    let schemas = vec![make_login_schema()];
    let plans = vec![make_brute_force_plan()];

    let result = generate(&wfg, &schemas, &plans).unwrap();
    // Total should still be 1000
    assert_eq!(result.events.len(), 1000);
}

#[test]
fn test_inject_deterministic() {
    let input = r#"
scenario inject_det seed 42 {
    time "2024-01-01T00:00:00Z" duration 1h
    total 500
    stream s1 : LoginWindow 100/s
    inject for brute_force on [s1] {
        hit 30%;
    }
}
"#;
    let wfg = parse_wfg(input).unwrap();
    let schemas = vec![make_login_schema()];
    let plans = vec![make_brute_force_plan()];

    let result1 = generate(&wfg, &schemas, &plans).unwrap();
    let result2 = generate(&wfg, &schemas, &plans).unwrap();

    assert_eq!(result1.events.len(), result2.events.len());
    for (e1, e2) in result1.events.iter().zip(result2.events.iter()) {
        assert_eq!(e1.timestamp, e2.timestamp);
        assert_eq!(e1.fields, e2.fields);
    }
}

#[test]
fn test_no_inject_backward_compat() {
    // Without inject blocks, generate() behaves identically with or without rule_plans
    let input = r#"
scenario compat seed 42 {
    time "2024-01-01T00:00:00Z" duration 1h
    total 100
    stream s1 : LoginWindow 10/s
}
"#;
    let wfg = parse_wfg(input).unwrap();
    let schemas = vec![make_login_schema()];
    let plans = vec![make_brute_force_plan()];

    let result_with_plans = generate(&wfg, &schemas, &plans).unwrap();
    let result_without = generate(&wfg, &schemas, &[]).unwrap();

    assert_eq!(result_with_plans.events.len(), result_without.events.len());
    for (e1, e2) in result_with_plans.events.iter().zip(result_without.events.iter()) {
        assert_eq!(e1.timestamp, e2.timestamp);
        assert_eq!(e1.fields, e2.fields);
    }
}

// ---------------------------------------------------------------------------
// Fault generation tests
// ---------------------------------------------------------------------------

use crate::datagen::fault_gen::apply_faults;
use crate::wfg_ast::{FaultLine, FaultType, FaultsBlock};
use rand::SeedableRng;
use rand::rngs::StdRng;

fn make_clean_events(count: usize) -> Vec<super::stream_gen::GenEvent> {
    let input = format!(
        r#"
scenario fault_helper seed 42 {{
    time "2024-01-01T00:00:00Z" duration 1h
    total {}
    stream s1 : LoginWindow 10/s
}}
"#,
        count
    );
    let wfg = parse_wfg(&input).unwrap();
    let schemas = vec![make_login_schema()];
    generate(&wfg, &schemas, &[]).unwrap().events
}

fn faults_block(faults: Vec<(FaultType, f64)>) -> FaultsBlock {
    FaultsBlock {
        faults: faults
            .into_iter()
            .map(|(ft, pct)| FaultLine {
                fault_type: ft,
                percent: pct,
            })
            .collect(),
    }
}

#[test]
fn test_fault_drop_removes_events() {
    let events = make_clean_events(100);
    let faults = faults_block(vec![(FaultType::Drop, 10.0)]);
    let mut rng = StdRng::seed_from_u64(42);
    let result = apply_faults(events, &faults, &mut rng);

    assert!(
        result.events.len() < 100,
        "drop should reduce event count, got {}",
        result.events.len()
    );
    assert!(result.stats.dropped > 0);
    assert_eq!(
        result.stats.dropped + result.stats.clean,
        100,
        "dropped + clean should equal original count"
    );
}

#[test]
fn test_fault_duplicate_adds_events() {
    let events = make_clean_events(100);
    let faults = faults_block(vec![(FaultType::Duplicate, 10.0)]);
    let mut rng = StdRng::seed_from_u64(42);
    let result = apply_faults(events, &faults, &mut rng);

    assert!(
        result.events.len() > 100,
        "duplicate should increase event count, got {}",
        result.events.len()
    );
    assert!(result.stats.duplicate > 0);
    // Each duplicate adds 1 extra event
    assert_eq!(result.events.len(), 100 + result.stats.duplicate);
}

#[test]
fn test_fault_out_of_order_preserves_count() {
    let events = make_clean_events(100);
    let faults = faults_block(vec![(FaultType::OutOfOrder, 20.0)]);
    let mut rng = StdRng::seed_from_u64(42);
    let result = apply_faults(events, &faults, &mut rng);

    // OutOfOrder swaps pairs but doesn't change count
    assert_eq!(result.events.len(), 100);
    assert!(result.stats.out_of_order > 0);

    // Verify some timestamps are out of order
    let mut has_disorder = false;
    for w in result.events.windows(2) {
        if w[0].timestamp > w[1].timestamp {
            has_disorder = true;
            break;
        }
    }
    assert!(has_disorder, "out_of_order should produce timestamp disorder");
}

#[test]
fn test_fault_late_preserves_count() {
    let events = make_clean_events(100);
    let faults = faults_block(vec![(FaultType::Late, 10.0)]);
    let mut rng = StdRng::seed_from_u64(42);
    let result = apply_faults(events, &faults, &mut rng);

    // Late moves events later in the output but doesn't change count
    assert_eq!(result.events.len(), 100);
    assert!(result.stats.late > 0);
}

#[test]
fn test_fault_deterministic() {
    let events1 = make_clean_events(100);
    let events2 = make_clean_events(100);
    let faults = faults_block(vec![
        (FaultType::OutOfOrder, 10.0),
        (FaultType::Late, 5.0),
        (FaultType::Duplicate, 3.0),
        (FaultType::Drop, 2.0),
    ]);
    let mut rng1 = StdRng::seed_from_u64(99);
    let mut rng2 = StdRng::seed_from_u64(99);

    let result1 = apply_faults(events1, &faults, &mut rng1);
    let result2 = apply_faults(events2, &faults, &mut rng2);

    assert_eq!(result1.events.len(), result2.events.len());
    for (e1, e2) in result1.events.iter().zip(result2.events.iter()) {
        assert_eq!(e1.timestamp, e2.timestamp);
        assert_eq!(e1.fields, e2.fields);
    }
}

#[test]
fn test_fault_combined_stats() {
    let events = make_clean_events(200);
    let faults = faults_block(vec![
        (FaultType::OutOfOrder, 10.0),
        (FaultType::Late, 5.0),
        (FaultType::Duplicate, 3.0),
        (FaultType::Drop, 2.0),
    ]);
    let mut rng = StdRng::seed_from_u64(42);
    let result = apply_faults(events, &faults, &mut rng);
    let s = &result.stats;

    // Every input event is accounted for exactly once
    // Note: out_of_order consumes 1 event but also counts the partner as clean
    // So total assignments = out_of_order + late + duplicate + dropped + clean
    // But out_of_order also increments clean for the partner
    // Total input events = out_of_order + late + duplicate + dropped + clean - out_of_order
    //                    = late + duplicate + dropped + clean
    // Actually: each out_of_order event contributes (1 out_of_order + 1 clean for partner)
    // But the partner was already in the input. So:
    // input_count = out_of_order + clean_from_partner + late + duplicate + dropped + other_clean
    //            = out_of_order + late + duplicate + dropped + clean
    // where clean includes the partners
    assert_eq!(
        s.out_of_order + s.late + s.duplicate + s.dropped + s.clean,
        200,
        "stats should account for all input events (including out_of_order partners)"
    );
}

#[test]
fn test_empty_faults_passthrough() {
    let events = make_clean_events(50);
    let faults = FaultsBlock { faults: vec![] };
    let mut rng = StdRng::seed_from_u64(1);
    let result = apply_faults(events, &faults, &mut rng);

    assert_eq!(result.events.len(), 50);
    assert_eq!(result.stats.clean, 50);
    assert_eq!(result.stats.dropped, 0);
    assert_eq!(result.stats.duplicate, 0);
    assert_eq!(result.stats.out_of_order, 0);
    assert_eq!(result.stats.late, 0);
}
