use std::time::Duration;

use wf_lang::{BaseType, FieldDef, FieldType, WindowSchema};

use crate::rule::contract::run_contract;

/// Schema for auth_events window.
fn auth_events_schema() -> WindowSchema {
    WindowSchema {
        name: "auth_events".to_string(),
        streams: vec!["auth_stream".to_string()],
        time_field: Some("event_time".to_string()),
        over: Duration::from_secs(3600),
        fields: vec![
            FieldDef {
                name: "sip".to_string(),
                field_type: FieldType::Base(BaseType::Ip),
            },
            FieldDef {
                name: "action".to_string(),
                field_type: FieldType::Base(BaseType::Chars),
            },
            FieldDef {
                name: "user".to_string(),
                field_type: FieldType::Base(BaseType::Chars),
            },
            FieldDef {
                name: "count".to_string(),
                field_type: FieldType::Base(BaseType::Digit),
            },
            FieldDef {
                name: "event_time".to_string(),
                field_type: FieldType::Base(BaseType::Time),
            },
        ],
    }
}

fn security_alerts_schema() -> WindowSchema {
    WindowSchema {
        name: "security_alerts".to_string(),
        streams: vec![],
        time_field: None,
        over: Duration::from_secs(3600),
        fields: vec![
            FieldDef {
                name: "sip".to_string(),
                field_type: FieldType::Base(BaseType::Ip),
            },
            FieldDef {
                name: "fail_count".to_string(),
                field_type: FieldType::Base(BaseType::Digit),
            },
        ],
    }
}

/// Parse a WFL source with rule + contract, compile the rule, and run the contract.
fn run_contract_from_source(source: &str) -> crate::rule::contract::ContractResult {
    let schemas = vec![auth_events_schema(), security_alerts_schema()];
    let wfl_file = wf_lang::parse_wfl(source).expect("parse should succeed");
    let plans = wf_lang::compile_wfl(&wfl_file, &schemas).expect("compile should succeed");

    assert!(
        !wfl_file.contracts.is_empty(),
        "expected at least one contract"
    );
    let test = &wfl_file.contracts[0];

    let plan = plans
        .iter()
        .find(|p| p.name == contract.rule_name)
        .unwrap_or_else(|| panic!("rule `{}` not found in plans", contract.rule_name));

    let time_field = schemas
        .iter()
        .find(|s| plan.binds.iter().any(|b| b.window == s.name))
        .and_then(|s| s.time_field.clone());

    run_contract(contract, plan, time_field).expect("run_contract should succeed")
}

#[test]
fn contract_match_five_events() {
    let source = r#"
rule brute_force {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 5; } } -> score(70.0)
    entity(ip, e.sip)
    yield security_alerts (sip = e.sip, fail_count = 5)
}

test five_hits for brute_force {
    given {
        row(e, sip = "10.0.0.1", action = "failed");
        row(e, sip = "10.0.0.1", action = "failed");
        row(e, sip = "10.0.0.1", action = "failed");
        row(e, sip = "10.0.0.1", action = "failed");
        row(e, sip = "10.0.0.1", action = "failed");
    }
    expect {
        hits == 1;
        hit[0].score >= 70;
        hit[0].entity_type == "ip";
    }
}
"#;
    let result = run_contract_from_source(source);
    assert!(result.passed, "failures: {:?}", result.failures);
    assert_eq!(result.alert_count, 1);
}

#[test]
fn contract_below_threshold() {
    let source = r#"
rule brute_force {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 5; } } -> score(70.0)
    entity(ip, e.sip)
    yield security_alerts (sip = e.sip, fail_count = 5)
}

test below_threshold for brute_force {
    given {
        row(e, sip = "10.0.0.1", action = "failed");
        row(e, sip = "10.0.0.1", action = "failed");
        row(e, sip = "10.0.0.1", action = "failed");
    }
    expect {
        hits == 0;
    }
}
"#;
    let result = run_contract_from_source(source);
    assert!(result.passed, "failures: {:?}", result.failures);
    assert_eq!(result.alert_count, 0);
}

#[test]
fn contract_close_trigger_timeout() {
    let source = r#"
rule timeout_rule {
    events { e : auth_events }
    match<sip:5m> {
        on event { e | count >= 1; }
        on close { close_count: e | count >= 1; }
    } -> score(80.0)
    entity(ip, e.sip)
    yield security_alerts (sip = e.sip, fail_count = 1)
}

test test_timeout for timeout_rule {
    given {
        row(e, sip = "10.0.0.1", action = "failed");
        row(e, sip = "10.0.0.1", action = "failed");
        tick(6m);
    }
    expect {
        hits == 1;
    }
    options {
        close_trigger = timeout;
    }
}
"#;
    let result = run_contract_from_source(source);
    assert!(result.passed, "failures: {:?}", result.failures);
    assert_eq!(result.alert_count, 1);
}

#[test]
fn contract_close_trigger_eos() {
    let source = r#"
rule eos_rule {
    events { e : auth_events }
    match<sip:5m> {
        on event { e | count >= 1; }
        on close { close_count: e | count >= 1; }
    } -> score(80.0)
    entity(ip, e.sip)
    yield security_alerts (sip = e.sip, fail_count = 1)
}

test test_eos for eos_rule {
    given {
        row(e, sip = "10.0.0.1", action = "failed");
        row(e, sip = "10.0.0.1", action = "failed");
    }
    expect {
        hits >= 1;
    }
    options {
        close_trigger = eos;
    }
}
"#;
    let result = run_contract_from_source(source);
    assert!(result.passed, "failures: {:?}", result.failures);
}

#[test]
fn contract_score_assertion_fail() {
    let source = r#"
rule brute_force {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 5; } } -> score(70.0)
    entity(ip, e.sip)
    yield security_alerts (sip = e.sip, fail_count = 5)
}

test score_fail for brute_force {
    given {
        row(e, sip = "10.0.0.1");
        row(e, sip = "10.0.0.1");
        row(e, sip = "10.0.0.1");
        row(e, sip = "10.0.0.1");
        row(e, sip = "10.0.0.1");
    }
    expect {
        hit[0].score >= 90;
    }
}
"#;
    let result = run_contract_from_source(source);
    assert!(!result.passed, "expected failure but test passed");
    assert!(
        result.failures.iter().any(|f| f.contains("score")),
        "expected score failure, got: {:?}",
        result.failures
    );
}

#[test]
fn contract_entity_id_check() {
    let source = r#"
rule brute_force {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 5; } } -> score(70.0)
    entity(ip, e.sip)
    yield security_alerts (sip = e.sip, fail_count = 5)
}

test entity_check for brute_force {
    given {
        row(e, sip = "10.0.0.1");
        row(e, sip = "10.0.0.1");
        row(e, sip = "10.0.0.1");
        row(e, sip = "10.0.0.1");
        row(e, sip = "10.0.0.1");
    }
    expect {
        hit[0].entity_id == "10.0.0.1";
    }
}
"#;
    let result = run_contract_from_source(source);
    assert!(result.passed, "failures: {:?}", result.failures);
}

#[test]
fn contract_hits_ge() {
    let source = r#"
rule brute_force {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 5; } } -> score(70.0)
    entity(ip, e.sip)
    yield security_alerts (sip = e.sip, fail_count = 5)
}

test hits_ge for brute_force {
    given {
        row(e, sip = "10.0.0.1");
        row(e, sip = "10.0.0.1");
        row(e, sip = "10.0.0.1");
        row(e, sip = "10.0.0.1");
        row(e, sip = "10.0.0.1");
    }
    expect {
        hits >= 1;
    }
}
"#;
    let result = run_contract_from_source(source);
    assert!(result.passed, "failures: {:?}", result.failures);
}
