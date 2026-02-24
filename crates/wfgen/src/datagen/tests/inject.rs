use super::*;
use crate::oracle::run_oracle;

#[test]
fn test_inject_hit_cluster_correctness() {
    let input = r#"
scenario inject_hit seed 42 {
    time "2024-01-01T00:00:00Z" duration 1h
    total 1000
    stream fail : LoginWindow 100/s
    inject for brute_force on [fail] {
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
    stream fail : LoginWindow 100/s
    inject for brute_force on [fail] {
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
    let start = "2024-01-01T00:00:00Z".parse().unwrap();
    let duration = Duration::from_secs(3600);
    let oracle = run_oracle(&result.events, &plans, &start, &duration, None).unwrap();
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
    stream fail : LoginWindow 100/s
    inject for brute_force on [fail] {
        hit 50%;
    }
}
"#;
    let wfg = parse_wfg(input).unwrap();
    let schemas = vec![make_login_schema()];
    let plans = vec![make_brute_force_plan()];

    let result = generate(&wfg, &schemas, &plans).unwrap();

    // Run oracle — hit clusters should produce alerts
    let start = "2024-01-01T00:00:00Z".parse().unwrap();
    let duration = Duration::from_secs(3600);
    let oracle = run_oracle(&result.events, &plans, &start, &duration, None).unwrap();

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
    stream fail : LoginWindow 100/s
    inject for brute_force on [fail] {
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
    stream fail : LoginWindow 100/s
    inject for brute_force on [fail] {
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
