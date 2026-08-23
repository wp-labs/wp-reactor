//! Round-4 coverage tests for `match_engine/contract.rs` — the inline
//! contract harness. Covers the guards and harness branches the earlier
//! suites skip:
//!
//! - on-each-with-joins rejection guard;
//! - `expr_refs_window` over every Expr form (Neg / BinOp / InList /
//!   IfThenElse / FuncCall / Array / Object / FieldRef variants) plus the
//!   joined-window `where` rejection;
//! - bind-level (`events { e : win && guard }`) row rejection in the harness;
//! - close-trigger `timeout` / `flush` loop bodies;
//! - compare-op lanes for `hits` / `score` and the failure-message
//!   `cmp_op_str` forms;
//! - `hit[i].origin` / `entity_type` / `entity_id` / `field(...)` assertion
//!   failure lanes;
//! - shuffle with fewer than two rows (early return).
//!
//! Only test code lives here — no production logic is modified.

use std::time::Duration;

use wf_lang::ast::{BinOp, Expr, FieldRef, ObjectItem, PathSegment};
use wf_lang::{BaseType, FieldDef, FieldType, WindowSchema};

use crate::match_engine::contract::run_test;

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

fn geo_lookup_schema() -> WindowSchema {
    WindowSchema {
        name: "geo_lookup".to_string(),
        streams: vec![],
        time_field: None,
        over: Duration::from_secs(3600),
        fields: vec![
            FieldDef {
                name: "sip".to_string(),
                field_type: FieldType::Base(BaseType::Ip),
            },
            FieldDef {
                name: "region".to_string(),
                field_type: FieldType::Base(BaseType::Chars),
            },
            FieldDef {
                name: "price".to_string(),
                field_type: FieldType::Base(BaseType::Digit),
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

/// Compile `source` and return `(plan, test, time_field)` for the first test.
fn compile_first(
    source: &str,
) -> (
    wf_lang::plan::RulePlan,
    wf_lang::ast::TestBlock,
    Option<String>,
) {
    let schemas = vec![
        auth_events_schema(),
        geo_lookup_schema(),
        security_alerts_schema(),
    ];
    let wfl_file = wf_lang::parse_wfl(source).expect("parse should succeed");
    let plans = wf_lang::compile_wfl(&wfl_file, &schemas).expect("compile should succeed");
    let test = wfl_file.tests[0].clone();
    let plan = plans
        .iter()
        .find(|p| p.name == test.rule_name)
        .unwrap_or_else(|| panic!("rule `{}` not found in plans", test.rule_name))
        .clone();
    let time_field = schemas
        .iter()
        .find(|s| plan.binds.iter().any(|b| b.window == s.name))
        .and_then(|s| s.time_field.clone());
    (plan, test, time_field)
}

// ===========================================================================
// Guard: on-each rules with joins cannot be asserted inline
// ===========================================================================

#[test]
fn contract_rejects_on_each_with_joins() {
    let source = r#"
rule each_join {
    events { e : auth_events }
    on each e -> score(10.0)
    join geo_lookup snapshot on e.sip == geo_lookup.sip
    entity(ip, e.sip)
    yield security_alerts (sip = e.sip, fail_count = 1)
}

test t for each_join {
    input {
        row(e, sip = "10.0.0.1");
    }
    expect {
        hits == 1;
    }
}
"#;
    let (plan, test, time_field) = compile_first(source);
    let result = run_test(&test, &plan, time_field).expect("run_test should succeed");
    assert!(
        !result.passed,
        "on-each + join inline test must be rejected by the guard"
    );
    assert!(
        result.failures[0].contains("on-each rule with joins"),
        "failure must explain the on-each guard, got: {:?}",
        result.failures
    );
}

// ===========================================================================
// expr_refs_window: every expression form + the joined-window rejection
// ===========================================================================

/// A `where` expression that touches every `Expr` variant `expr_refs_window`
/// recurses into but never references the joined window (`geo_lookup`), so the
/// harness runs normally while the whole tree is traversed.
fn where_expr_with_all_forms() -> Expr {
    Expr::IfThenElse {
        cond: Box::new(Expr::BinOp {
            op: BinOp::And,
            left: Box::new(Expr::Neg(Box::new(Expr::Field(FieldRef::Simple(
                "count".into(),
            ))))),
            right: Box::new(Expr::InList {
                expr: Box::new(Expr::Field(FieldRef::Path {
                    alias: "e".into(),
                    segments: vec![
                        PathSegment::Field("roles_obj".into()),
                        PathSegment::Field("uid".into()),
                    ],
                })),
                list: vec![Expr::StringLit("x".into()), Expr::Number(1.0)],
                negated: false,
            }),
        }),
        then_expr: Box::new(Expr::FuncCall {
            qualifier: None,
            name: "lower".into(),
            args: vec![Expr::Field(FieldRef::Qualified(
                "other_win".into(),
                "f".into(),
            ))],
        }),
        else_expr: Box::new(Expr::Object(vec![ObjectItem {
            targets: vec!["k".into()],
            type_hint: None,
            value: Expr::Array(vec![Expr::Field(FieldRef::Bracketed(
                "other_win".into(),
                "g".into(),
            ))]),
        }])),
    }
}

#[test]
fn contract_where_all_expr_forms_run_through_expr_refs_window() {
    let source = r#"
rule enriched {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(70.0)
    join geo_lookup snapshot on e.sip == geo_lookup.sip
    entity(ip, e.sip)
    yield security_alerts (sip = e.sip, fail_count = 1)
}

test t for enriched {
    input {
        row(e, sip = "10.0.0.1");
    }
    expect {
        hits == 1;
    }
}
"#;
    let (mut plan, test, time_field) = compile_first(source);
    // A where that references no joined window → the check traverses every
    // expr form and finds nothing; the rule runs normally.
    plan.r#where = Some(where_expr_with_all_forms());
    let result = run_test(&test, &plan, time_field).expect("run_test should succeed");
    assert!(result.passed, "failures: {:?}", result.failures);
    assert_eq!(result.output_count, 1);
}

#[test]
fn contract_where_referencing_joined_window_is_rejected() {
    let source = r#"
rule enriched {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(70.0)
    join geo_lookup snapshot on e.sip == geo_lookup.sip
    entity(ip, e.sip)
    yield security_alerts (sip = e.sip, fail_count = 1)
}

test t for enriched {
    input {
        row(e, sip = "10.0.0.1");
    }
    expect {
        hits == 1;
    }
}
"#;
    let (mut plan, test, time_field) = compile_first(source);
    // Qualified reference to the joined window (e.g. `geo_lookup.region`).
    plan.r#where = Some(Expr::Field(FieldRef::Qualified(
        "geo_lookup".into(),
        "region".into(),
    )));
    let result = run_test(&test, &plan, time_field).expect("run_test should succeed");
    assert!(!result.passed, "joined-window where must be rejected");
    assert!(
        result.failures[0].contains("references joined window"),
        "failure must explain the where guard, got: {:?}",
        result.failures
    );
}

#[test]
fn contract_where_path_and_simple_field_forms_are_not_window_refs() {
    // Path (`e.roles_obj.source.process.uid`) and simple (`count`) field
    // references must not be mistaken for joined-window references.
    let source = r#"
rule enriched {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(70.0)
    join geo_lookup snapshot on e.sip == geo_lookup.sip
    entity(ip, e.sip)
    yield security_alerts (sip = e.sip, fail_count = 1)
}

test t for enriched {
    input {
        row(e, sip = "10.0.0.1");
    }
    expect {
        hits == 1;
    }
}
"#;
    let (mut plan, test, time_field) = compile_first(source);
    plan.r#where = Some(Expr::BinOp {
        op: BinOp::Eq,
        left: Box::new(Expr::Field(FieldRef::Simple("count".into()))),
        right: Box::new(Expr::Number(1.0)),
    });
    let result = run_test(&test, &plan, time_field).expect("run_test should succeed");
    assert!(result.passed, "failures: {:?}", result.failures);
    assert_eq!(result.output_count, 1);
}

// ===========================================================================
// Bind-level filter: `events { e : auth_events && guard }` row rejection
// ===========================================================================

#[test]
fn contract_bind_filter_rejects_non_matching_rows() {
    let source = r#"
rule filtered {
    events { e : auth_events && e.action == "failed" }
    match<sip:5m> { on event { e | count >= 1; } } -> score(70.0)
    entity(ip, e.sip)
    yield security_alerts (sip = e.sip, fail_count = 1)
}

test t for filtered {
    input {
        row(e, sip = "10.0.0.1", action = "ok");
        row(e, sip = "10.0.0.1", action = "failed");
    }
    expect {
        hits == 1;
    }
}
"#;
    let (plan, test, time_field) = compile_first(source);
    let result = run_test(&test, &plan, time_field).expect("run_test should succeed");
    assert!(result.passed, "failures: {:?}", result.failures);
    assert_eq!(result.output_count, 1, "only the guard-passing row fires");
}

// ===========================================================================
// Close triggers: timeout / flush loop bodies
// ===========================================================================

fn close_rule_source(trigger: &str) -> String {
    format!(
        r#"
rule close_rule {{
    events {{ e : auth_events }}
    match<sip:5m> {{
        on event {{ e | count >= 1; }}
        and close {{ c: e | count >= 1; }}
    }} -> score(80.0)
    entity(ip, e.sip)
    yield security_alerts (sip = e.sip, fail_count = 1)
}}

test t for close_rule {{
    input {{
        row(e, sip = "10.0.0.1");
    }}
    expect {{
        hits >= 1;
    }}
    options {{
        close_trigger = {trigger};
    }}
}}
"#
    )
}

#[test]
fn contract_close_trigger_timeout_fires_pending_close() {
    // No tick in the input: the window is still open when the input ends, so
    // the `timeout` trigger's +24h scan is what expires it — the loop body
    // (`scan_expired_at_with_conv` → `execute_close`) runs at the trigger.
    let (plan, test, time_field) = compile_first(&close_rule_source("timeout"));
    let result = run_test(&test, &plan, time_field).expect("run_test should succeed");
    assert!(result.passed, "failures: {:?}", result.failures);
    assert!(
        result.output_count >= 1,
        "timeout trigger must fire the close"
    );
}

#[test]
fn contract_close_trigger_flush_fires_pending_close() {
    let (plan, test, time_field) = compile_first(&close_rule_source("flush"));
    let result = run_test(&test, &plan, time_field).expect("run_test should succeed");
    assert!(result.passed, "failures: {:?}", result.failures);
    assert!(
        result.output_count >= 1,
        "flush trigger must fire the close"
    );
}

// ===========================================================================
// Compare-op lanes + assertion failure messages
// ===========================================================================

#[test]
fn contract_hits_and_score_compare_op_lanes() {
    let source = r#"
rule brute_force {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 5; } } -> score(70.0)
    entity(ip, e.sip)
    yield security_alerts (sip = e.sip, fail_count = 5)
}

test ops for brute_force {
    input {
        row(e, sip = "10.0.0.1");
        row(e, sip = "10.0.0.1");
        row(e, sip = "10.0.0.1");
        row(e, sip = "10.0.0.1");
        row(e, sip = "10.0.0.1");
    }
    expect {
        hits != 1;
        hits == 2;
        hits < 1;
        hits > 1;
        hits <= 0;
        hits >= 2;
        hit[0].score < 10;
        hit[0].score > 100;
        hit[0].score <= 10;
        hit[0].score >= 100;
    }
}
"#;
    let (plan, test, time_field) = compile_first(source);
    let result = run_test(&test, &plan, time_field).expect("run_test should succeed");
    assert!(!result.passed, "expect to collect failure messages");
    // All ten assertions fail; the failure messages exercise cmp_op_str for
    // every CmpOp variant.
    assert_eq!(result.failures.len(), 10, "failures: {:?}", result.failures);
    let joined = result.failures.join("\n");
    for op in ["==", "!=", "<", ">", "<=", ">="] {
        assert!(joined.contains(op), "missing op {op} in {joined}");
    }
    assert_eq!(result.output_count, 1);
}

#[test]
fn contract_hit_assert_failure_lanes() {
    let source = r#"
rule brute_force {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 5; } } -> score(70.0)
    entity(ip, e.sip)
    yield security_alerts (sip = e.sip, fail_count = 5)
}

test asserts for brute_force {
    input {
        row(e, sip = "10.0.0.1");
        row(e, sip = "10.0.0.1");
        row(e, sip = "10.0.0.1");
        row(e, sip = "10.0.0.1");
        row(e, sip = "10.0.0.1");
    }
    expect {
        hit[0].origin == "bogus";
        hit[0].entity_type == "user";
        hit[0].entity_id == "nope";
        hit[0].field("missing") == 1;
        hit[0].field("sip") == "wrong";
        hit[0].field("sip") > 1;
        hit[0].field("sip") == e.sip;
    }
}
"#;
    let (plan, test, time_field) = compile_first(source);
    let result = run_test(&test, &plan, time_field).expect("run_test should succeed");
    assert!(!result.passed, "expect to collect hit-assert failures");
    assert_eq!(result.failures.len(), 7, "failures: {:?}", result.failures);
    let joined = result.failures.join("\n");
    assert!(joined.contains("origin"), "{joined}");
    assert!(joined.contains("entity_type"), "{joined}");
    assert!(joined.contains("entity_id"), "{joined}");
    assert!(joined.contains("no such yield field"), "{joined}");
    assert!(joined.contains("only ==/!="), "{joined}");
    assert!(joined.contains("unsupported expected value"), "{joined}");
}

// ===========================================================================
// Shuffle with fewer than two rows: early return
// ===========================================================================

#[test]
fn contract_shuffle_single_row_is_not_shuffled() {
    let source = r#"
rule simple {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(70.0)
    entity(ip, e.sip)
    yield security_alerts (sip = e.sip, fail_count = 1)
}

test single for simple {
    input {
        row(e, sip = "10.0.0.1");
    }
    expect {
        hits == 1;
    }
    options {
        permutation = shuffle;
        runs = 3;
    }
}
"#;
    let (plan, test, time_field) = compile_first(source);
    let result = run_test(&test, &plan, time_field).expect("run_test should succeed");
    assert!(result.passed, "failures: {:?}", result.failures);
    assert_eq!(result.output_count, 1);
}
