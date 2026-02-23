use super::*;

// ===========================================================================
// Key mapping: extract_key with key_map
// ===========================================================================

#[test]
fn key_map_extracts_from_alias_field() {
    use std::time::Duration;

    let key_map = vec![
        KeyMapPlan {
            logical_name: "ip".to_string(),
            source_alias: "login".to_string(),
            source_field: "src_ip".to_string(),
        },
        KeyMapPlan {
            logical_name: "ip".to_string(),
            source_alias: "dns".to_string(),
            source_field: "client_ip".to_string(),
        },
    ];

    let plan = MatchPlan {
        keys: vec![FieldRef::Simple("ip".to_string())],
        key_map: Some(key_map),
        window_spec: WindowSpec::Sliding(Duration::from_secs(300)),
        event_steps: vec![step(vec![
            branch("login", count_ge(1.0)),
            branch("dns", count_ge(1.0)),
        ])],
        close_steps: vec![],
    };

    let mut sm = CepStateMachine::new("rule_km".to_string(), plan, None);

    // "login" event with "src_ip" field — should extract key from src_ip
    let e1 = event(vec![("src_ip", str_val("10.0.0.1"))]);
    let result = sm.advance("login", &e1);
    assert!(matches!(result, StepResult::Matched(_)));
    if let StepResult::Matched(ctx) = result {
        assert_eq!(ctx.scope_key, vec![str_val("10.0.0.1")]);
    }
}
