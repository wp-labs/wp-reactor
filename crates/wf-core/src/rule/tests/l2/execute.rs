use super::*;

// ===========================================================================
// Limits: no limits plan → unlimited instances
// ===========================================================================

#[test]
fn no_limits_allows_unlimited_instances() {
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(5.0))])],
    );
    let mut sm = CepStateMachine::new("rule_nolim".to_string(), plan, None);

    // Create 100 different keys
    for i in 0..100 {
        let e = event(vec![("sip", str_val(&format!("10.0.0.{}", i)))]);
        sm.advance("fail", &e);
    }
    assert_eq!(sm.instance_count(), 100);
}

// ===========================================================================
// Execute match without joins (backward compat)
// ===========================================================================

#[test]
fn execute_match_without_joins_still_works() {
    let plan = simple_rule_plan(
        "r_compat",
        simple_plan(
            vec![simple_key("sip")],
            vec![step(vec![branch("fail", count_ge(1.0))])],
        ),
        Expr::Number(50.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    );
    let exec = RuleExecutor::new(plan);
    let matched = MatchedContext {
        rule_name: "r_compat".to_string(),
        scope_key: vec![str_val("10.0.0.1")],
        step_data: vec![StepData {
            satisfied_branch_index: 0,
            label: None,
            measure_value: 1.0,
            collected_values: Vec::new(),
        }],
        event_time_nanos: 0,
    };

    // Old API still works
    let alert = exec.execute_match(&matched).unwrap();
    assert_eq!(alert.entity_id, "10.0.0.1");
    assert!((alert.score - 50.0).abs() < f64::EPSILON);
}
