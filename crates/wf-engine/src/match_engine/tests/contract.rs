use std::time::Duration;

use wf_lang::{BaseType, FieldDef, FieldType, WindowSchema};

use crate::match_engine::contract::run_test;

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
                name: "roles_obj".to_string(),
                field_type: FieldType::Object,
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
            FieldDef {
                name: "uid".to_string(),
                field_type: FieldType::Base(BaseType::Chars),
            },
        ],
    }
}

/// Parse a WFL source with rule + contract, compile the rule, and run the contract.
fn run_contract_from_source(source: &str) -> crate::match_engine::contract::TestResult {
    let schemas = vec![auth_events_schema(), security_alerts_schema()];
    let wfl_file = wf_lang::parse_wfl(source).expect("parse should succeed");
    let plans = wf_lang::compile_wfl(&wfl_file, &schemas).expect("compile should succeed");

    assert!(!wfl_file.tests.is_empty(), "expected at least one test");
    let test = &wfl_file.tests[0];

    let plan = plans
        .iter()
        .find(|p| p.name == test.rule_name)
        .unwrap_or_else(|| panic!("rule `{}` not found in plans", test.rule_name));

    let time_field = schemas
        .iter()
        .find(|s| plan.binds.iter().any(|b| b.window == s.name))
        .and_then(|s| s.time_field.clone());

    run_test(test, plan, time_field).expect("run_test should succeed")
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
    input {
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
    assert_eq!(result.output_count, 1);
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
    input {
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
    assert_eq!(result.output_count, 0);
}

#[test]
fn contract_match_can_use_source_alias_aggregates_without_runtime_task() {
    let source = r#"
rule source_alias_agg {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(avg(e.count))
    entity(user, last(e.user))
    yield security_alerts (sip = e.sip, fail_count = 1)
}

test source_alias_agg_contract for source_alias_agg {
    input {
        row(e, sip = "10.0.0.1", user = "alice", count = 42, event_time = 1);
    }
    expect {
        hits == 1;
        hit[0].score == 42;
        hit[0].entity_type == "user";
        hit[0].entity_id == "alice";
    }
}
"#;
    let result = run_contract_from_source(source);
    assert!(result.passed, "failures: {:?}", result.failures);
    assert_eq!(result.output_count, 1);
}

#[test]
fn contract_close_trigger_timeout() {
    let source = r#"
rule timeout_rule {
    events { e : auth_events }
    match<sip:5m> {
        on event { e | count >= 1; }
        and close { close_count: e | count >= 1; }
    } -> score(80.0)
    entity(ip, e.sip)
    yield security_alerts (sip = e.sip, fail_count = 1)
}

test test_timeout for timeout_rule {
    input {
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
    assert_eq!(result.output_count, 1);
}

#[test]
fn contract_close_trigger_eos() {
    let source = r#"
rule eos_rule {
    events { e : auth_events }
    match<sip:5m> {
        on event { e | count >= 1; }
        and close { close_count: e | count >= 1; }
    } -> score(80.0)
    entity(ip, e.sip)
    yield security_alerts (sip = e.sip, fail_count = 1)
}

test test_eos for eos_rule {
    input {
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
    input {
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
    input {
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
    input {
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

#[test]
fn contract_shuffle_permutation_exposes_order_sensitive_rule() {
    let source = r#"
rule ordered_ab {
    events {
        a : auth_events
        b : auth_events
    }
    match<sip:5m> {
        on event {
            a | count >= 1;
            b | count >= 1;
        }
    } -> score(60.0)
    entity(ip, a.sip)
    yield security_alerts (sip = a.sip, fail_count = 1)
}

test shuffle_order_check for ordered_ab {
    input {
        row(a, sip = "10.0.0.1", action = "A");
        row(b, sip = "10.0.0.1", action = "B");
    }
    expect {
        hits == 1;
    }
    options {
        permutation = shuffle;
        runs = 6;
    }
}
"#;
    let result = run_contract_from_source(source);
    assert!(!result.passed, "expected shuffle run to expose ordering");
    assert!(
        result.failures.iter().any(|f| f.contains("run")),
        "expected run-scoped failure messages, got: {:?}",
        result.failures
    );
}

// =========================================================================
// Nested field paths (wp-labs/warp-fusion#64)
// =========================================================================

#[test]
fn contract_yield_nested_path_with_nested_input() {
    let source = r#"
rule brute_force {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(70.0)
    entity(ip, e.sip)
    yield security_alerts (sip = e.sip, fail_count = 1, uid = e.roles_obj.source.process.uid)
}

test nested_uid for brute_force {
    input {
        row(e, sip = "10.0.0.1", roles_obj = object {
            source = object {
                process = object {
                    uid = "d22b3fbcb9e77cb86834f6a18e2e0f68";
                };
            };
        });
    }
    expect {
        hits == 1;
    }
}
"#;
    let result = run_contract_from_source(source);
    assert!(result.passed, "failures: {:?}", result.failures);
    assert_eq!(
        result.output_count, 1,
        "nested path must not suppress the alert"
    );
}

#[test]
fn contract_yield_nested_path_missing_input_still_fires() {
    // Without `roles_obj` in the input, the nested path degrades to an omitted
    // field and the alert still fires (issue #64).
    let source = r#"
rule brute_force {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(70.0)
    entity(ip, e.sip)
    yield security_alerts (sip = e.sip, fail_count = 1, uid = e.roles_obj.source.process.uid)
}

test missing_roles for brute_force {
    input {
        row(e, sip = "10.0.0.1", action = "failed");
    }
    expect {
        hits == 1;
    }
}
"#;
    let result = run_contract_from_source(source);
    assert!(result.passed, "failures: {:?}", result.failures);
    assert_eq!(
        result.output_count, 1,
        "missing nested path must not suppress the alert"
    );
}

// =========================================================================
// on event<accu> — within-window accumulation (wp-labs/warp-fusion#65)
// =========================================================================

#[test]
fn contract_accu_fires_every_subsequent_event() {
    // 5 events in a 100s window, threshold 2: with accu the block fires on the
    // 2nd, 3rd, 4th and 5th event (running count), instead of resetting after
    // the 2nd (which would fire only on the 2nd and 4th).
    let source = r#"
rule accu_rule {
    events { s : auth_events }
    match<sip:100s> {
        on event<accu> { s | count >= 2; }
    } -> score(70.0)
    entity(ip, s.sip)
    yield security_alerts (sip = s.sip, fail_count = count(s))
}

test accu_five for accu_rule {
    input {
        row(s, sip = "10.0.0.1", action = "failed");
        row(s, sip = "10.0.0.1", action = "failed");
        row(s, sip = "10.0.0.1", action = "failed");
        row(s, sip = "10.0.0.1", action = "failed");
        row(s, sip = "10.0.0.1", action = "failed");
    }
    expect {
        hits == 4;
    }
}
"#;
    let result = run_contract_from_source(source);
    assert!(result.passed, "failures: {:?}", result.failures);
    assert_eq!(result.output_count, 4, "accu must fire on events 2..5");
}

#[test]
fn contract_default_resets_after_fire() {
    // Same input without `<accu>`: fires on the 2nd and 4th event only.
    let source = r#"
rule reset_rule {
    events { s : auth_events }
    match<sip:100s> {
        on event { s | count >= 2; }
    } -> score(70.0)
    entity(ip, s.sip)
    yield security_alerts (sip = s.sip, fail_count = count(s))
}

test reset_five for reset_rule {
    input {
        row(s, sip = "10.0.0.1", action = "failed");
        row(s, sip = "10.0.0.1", action = "failed");
        row(s, sip = "10.0.0.1", action = "failed");
        row(s, sip = "10.0.0.1", action = "failed");
        row(s, sip = "10.0.0.1", action = "failed");
    }
    expect {
        hits == 2;
    }
}
"#;
    let result = run_contract_from_source(source);
    assert!(result.passed, "failures: {:?}", result.failures);
    assert_eq!(
        result.output_count, 2,
        "default reset fires on events 2 and 4 only"
    );
}

#[test]
fn contract_join_key_rule_rejected_by_guard() {
    // join-then-key rules can't be asserted by inline tests: the harness runs
    // advance_at without a WindowLookup, so every event is skipped as a join
    // miss — an `expect hits == 0` would pass vacuously. The guard must fail
    // the test instead of green-lighting it.
    let bid = WindowSchema {
        name: "bid_events".to_string(),
        streams: vec!["bid_stream".to_string()],
        time_field: Some("event_time".to_string()),
        over: Duration::from_secs(3600),
        fields: vec![
            FieldDef {
                name: "auction".to_string(),
                field_type: FieldType::Base(BaseType::Digit),
            },
            FieldDef {
                name: "bidder".to_string(),
                field_type: FieldType::Base(BaseType::Digit),
            },
            FieldDef {
                name: "price".to_string(),
                field_type: FieldType::Base(BaseType::Digit),
            },
            FieldDef {
                name: "event_time".to_string(),
                field_type: FieldType::Base(BaseType::Time),
            },
        ],
    };
    let auction = WindowSchema {
        name: "auction_events".to_string(),
        streams: vec!["auction_stream".to_string()],
        time_field: Some("event_time".to_string()),
        over: Duration::from_secs(3600),
        fields: vec![
            FieldDef {
                name: "id".to_string(),
                field_type: FieldType::Base(BaseType::Digit),
            },
            FieldDef {
                name: "category".to_string(),
                field_type: FieldType::Base(BaseType::Digit),
            },
            FieldDef {
                name: "event_time".to_string(),
                field_type: FieldType::Base(BaseType::Time),
            },
        ],
    };
    let out = WindowSchema {
        name: "out".to_string(),
        streams: vec![],
        time_field: None,
        over: Duration::from_secs(3600),
        fields: vec![FieldDef {
            name: "id".to_string(),
            field_type: FieldType::Base(BaseType::Digit),
        }],
    };
    let schemas = vec![bid, auction, out];
    let source = r#"
rule r {
    events { b : bid_events }
    match<category:10m> {
        on event { b | count >= 1; }
    } -> score(50.0)
    join auction_events snapshot on b.auction == auction_events.id
    entity(digit, b.auction)
    yield out (id = b.auction)
}

test t for r {
    input {
        row(b, auction = 1, bidder = 2, price = 3);
    }
    expect {
        hits == 0;
    }
}
"#;
    let wfl_file = wf_lang::parse_wfl(source).expect("parse should succeed");
    let plans = wf_lang::compile_wfl(&wfl_file, &schemas).expect("compile should succeed");
    let test = &wfl_file.tests[0];
    let plan = plans
        .iter()
        .find(|p| p.name == test.rule_name)
        .expect("rule present");
    let result =
        crate::match_engine::contract::run_test(test, plan, None).expect("run_test should succeed");
    assert!(
        !result.passed,
        "join-key inline test must be rejected by the guard"
    );
    assert!(
        result.failures[0].contains("join-then-key"),
        "failure must explain the join-then-key guard, got: {:?}",
        result.failures
    );
}
