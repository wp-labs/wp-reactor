use std::time::Duration;

use crate::ast::*;
use crate::compiler::compile_wfl;
use crate::plan::*;
use crate::schema::{BaseType, FieldDef, FieldType, WindowSchema};
use crate::wfl_parser::parse_wfl;

// ---------------------------------------------------------------------------
// Schema helpers
// ---------------------------------------------------------------------------

fn bt(b: BaseType) -> FieldType {
    FieldType::Base(b)
}

fn make_window(name: &str, streams: Vec<&str>, fields: Vec<(&str, FieldType)>) -> WindowSchema {
    WindowSchema {
        name: name.to_string(),
        streams: streams.into_iter().map(String::from).collect(),
        time_field: Some("event_time".to_string()),
        over: Duration::from_secs(3600),
        fields: fields
            .into_iter()
            .map(|(n, ft)| FieldDef {
                name: n.to_string(),
                field_type: ft,
            })
            .collect(),
    }
}

fn make_output_window(name: &str, fields: Vec<(&str, FieldType)>) -> WindowSchema {
    WindowSchema {
        name: name.to_string(),
        streams: vec![],
        time_field: None,
        over: Duration::from_secs(3600),
        fields: fields
            .into_iter()
            .map(|(n, ft)| FieldDef {
                name: n.to_string(),
                field_type: ft,
            })
            .collect(),
    }
}

fn auth_events_window() -> WindowSchema {
    make_window(
        "auth_events",
        vec!["auth_stream"],
        vec![
            ("sip", bt(BaseType::Ip)),
            ("dip", bt(BaseType::Ip)),
            ("action", bt(BaseType::Chars)),
            ("user", bt(BaseType::Chars)),
            ("count", bt(BaseType::Digit)),
            ("event_time", bt(BaseType::Time)),
        ],
    )
}

fn fw_events_window() -> WindowSchema {
    make_window(
        "fw_events",
        vec!["fw_stream"],
        vec![
            ("sip", bt(BaseType::Ip)),
            ("dip", bt(BaseType::Ip)),
            ("dport", bt(BaseType::Digit)),
            ("event_time", bt(BaseType::Time)),
        ],
    )
}

/// Generic window used by many tests as "win".
fn generic_window() -> WindowSchema {
    make_window(
        "win",
        vec!["stream"],
        vec![
            ("sip", bt(BaseType::Ip)),
            ("dip", bt(BaseType::Ip)),
            ("dport", bt(BaseType::Digit)),
            ("action", bt(BaseType::Chars)),
            ("host", bt(BaseType::Chars)),
            ("active", bt(BaseType::Bool)),
            ("detail.sha256", bt(BaseType::Hex)),
            ("event_time", bt(BaseType::Time)),
        ],
    )
}

/// Second generic window used by tests as "win2".
fn generic_window2() -> WindowSchema {
    make_window(
        "win2",
        vec!["stream2"],
        vec![
            ("sip", bt(BaseType::Ip)),
            ("dport", bt(BaseType::Digit)),
            ("event_time", bt(BaseType::Time)),
        ],
    )
}

fn dns_query_window() -> WindowSchema {
    make_window(
        "dns_query",
        vec!["dns_stream"],
        vec![
            ("query_id", bt(BaseType::Chars)),
            ("sip", bt(BaseType::Ip)),
            ("domain", bt(BaseType::Chars)),
            ("event_time", bt(BaseType::Time)),
        ],
    )
}

fn dns_response_window() -> WindowSchema {
    make_window(
        "dns_response",
        vec!["dns_stream"],
        vec![
            ("query_id", bt(BaseType::Chars)),
            ("sip", bt(BaseType::Ip)),
            ("close_reason", bt(BaseType::Chars)),
            ("event_time", bt(BaseType::Time)),
        ],
    )
}

fn output_window() -> WindowSchema {
    make_output_window(
        "out",
        vec![
            ("x", bt(BaseType::Ip)),
            ("y", bt(BaseType::Chars)),
            ("n", bt(BaseType::Digit)),
        ],
    )
}

fn security_alerts_window() -> WindowSchema {
    make_output_window(
        "security_alerts",
        vec![
            ("sip", bt(BaseType::Ip)),
            ("fail_count", bt(BaseType::Digit)),
            ("port_count", bt(BaseType::Digit)),
            ("message", bt(BaseType::Chars)),
        ],
    )
}

// ---------------------------------------------------------------------------
// Compile helper
// ---------------------------------------------------------------------------

/// Compile a WFL source string with given schemas, asserting parse + compile
/// both succeed.
fn compile_with(src: &str, schemas: &[WindowSchema]) -> Vec<RulePlan> {
    let file = parse_wfl(src).expect("parse should succeed");
    compile_wfl(&file, schemas).expect("compile should succeed")
}

// =========================================================================
// 1. compile_brute_force
// =========================================================================

#[test]
fn compile_brute_force() {
    let schemas = [auth_events_window(), security_alerts_window()];
    let plans = compile_with(
        r#"
rule brute_force {
    events {
        fail : auth_events && action == "failed"
    }
    match<sip:5m> {
        on event {
            fail | count >= 3;
        }
    } -> score(70.0)
    entity(ip, fail.sip)
    yield security_alerts (
        sip = fail.sip,
        fail_count = count(fail),
        message = fmt("{} failed", fail.sip)
    )
}
"#,
        &schemas,
    );
    assert_eq!(plans.len(), 1);
    let p = &plans[0];

    // name
    assert_eq!(p.name, "brute_force");

    // 1 bind
    assert_eq!(p.binds.len(), 1);
    assert_eq!(p.binds[0].alias, "fail");
    assert_eq!(p.binds[0].window, "auth_events");
    assert!(p.binds[0].filter.is_some());

    // match: 1 key, Sliding(300s), 1 event step, no close
    assert_eq!(p.match_plan.keys, vec![FieldRef::Simple("sip".into())]);
    assert_eq!(
        p.match_plan.window_spec,
        WindowSpec::Sliding(Duration::from_secs(300))
    );
    assert_eq!(p.match_plan.event_steps.len(), 1);
    assert!(p.match_plan.close_steps.is_empty());

    // event step: 1 branch
    let branch = &p.match_plan.event_steps[0].branches[0];
    assert_eq!(branch.source, "fail");
    assert!(branch.field.is_none());
    assert_eq!(branch.agg.measure, Measure::Count);
    assert_eq!(branch.agg.cmp, CmpOp::Ge);
    assert_eq!(branch.agg.threshold, Expr::Number(3.0));

    // entity
    assert_eq!(p.entity_plan.entity_type, "ip");
    assert_eq!(
        p.entity_plan.entity_id_expr,
        Expr::Field(FieldRef::Qualified("fail".into(), "sip".into()))
    );

    // score
    assert_eq!(p.score_plan.expr, Expr::Number(70.0));

    // yield: 3 fields
    assert_eq!(p.yield_plan.target, "security_alerts");
    assert_eq!(p.yield_plan.fields.len(), 3);
    assert_eq!(p.yield_plan.fields[0].name, "sip");
    assert_eq!(p.yield_plan.fields[1].name, "fail_count");
    assert_eq!(p.yield_plan.fields[2].name, "message");

    // L1 empties
    assert!(p.joins.is_empty());
    assert!(p.conv_plan.is_none());
}

// =========================================================================
// 2. compile_multi_source_multi_step
// =========================================================================

#[test]
fn compile_multi_source_multi_step() {
    let schemas = [auth_events_window(), fw_events_window(), output_window()];
    let plans = compile_with(
        r#"
rule multi {
    events {
        fail : auth_events && action == "failed"
        scan : fw_events
    }
    match<sip:5m> {
        on event {
            fail | count >= 3;
            scan.dport | distinct | count > 10;
        }
    } -> score(80.0)
    entity(ip, fail.sip)
    yield out (x = fail.sip)
}
"#,
        &schemas,
    );
    let p = &plans[0];

    // 2 binds
    assert_eq!(p.binds.len(), 2);
    assert_eq!(p.binds[0].alias, "fail");
    assert_eq!(p.binds[1].alias, "scan");

    // 2 event steps
    assert_eq!(p.match_plan.event_steps.len(), 2);

    // step[1]: field = Dot("dport"), transforms = [Distinct]
    let step1 = &p.match_plan.event_steps[1].branches[0];
    assert_eq!(step1.field, Some(FieldSelector::Dot("dport".into())));
    assert_eq!(step1.agg.transforms, vec![Transform::Distinct]);
    assert_eq!(step1.agg.measure, Measure::Count);
    assert_eq!(step1.agg.cmp, CmpOp::Gt);
}

// =========================================================================
// 3. compile_on_close
// =========================================================================

#[test]
fn compile_on_close() {
    let schemas = [dns_query_window(), dns_response_window(), output_window()];
    let plans = compile_with(
        r#"
rule dns_timeout {
    events {
        req : dns_query
        resp : dns_response
    }
    match<query_id:30s> {
        on event {
            req | count >= 1;
        }
        on close {
            resp && close_reason == "timeout" | count == 0;
        }
    } -> score(50.0)
    entity(ip, req.sip)
    yield out (x = req.sip)
}
"#,
        &schemas,
    );
    let p = &plans[0];

    assert_eq!(p.match_plan.event_steps.len(), 1);
    assert_eq!(p.match_plan.close_steps.len(), 1);

    let close_branch = &p.match_plan.close_steps[0].branches[0];
    assert_eq!(close_branch.source, "resp");
    assert!(close_branch.guard.is_some());
    assert_eq!(close_branch.agg.measure, Measure::Count);
    assert_eq!(close_branch.agg.cmp, CmpOp::Eq);
    assert_eq!(close_branch.agg.threshold, Expr::Number(0.0));
}

// =========================================================================
// 4. compile_or_branches
// =========================================================================

#[test]
fn compile_or_branches() {
    let schemas = [generic_window(), generic_window2(), output_window()];
    let plans = compile_with(
        r#"
rule or_rule {
    events { a : win  b : win2 }
    match<sip:5m> {
        on event {
            a | count >= 3 || b | count >= 5;
        }
    } -> score(60.0)
    entity(ip, a.sip)
    yield out (x = a.sip)
}
"#,
        &schemas,
    );
    let step = &plans[0].match_plan.event_steps[0];
    assert_eq!(step.branches.len(), 2);
    assert_eq!(step.branches[0].source, "a");
    assert_eq!(step.branches[1].source, "b");
}

// =========================================================================
// 5. compile_no_key
// =========================================================================

#[test]
fn compile_no_key() {
    let schemas = [generic_window(), output_window()];
    let plans = compile_with(
        r#"
rule nokey {
    events { e : win }
    match<:5m> {
        on event { e | count >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#,
        &schemas,
    );
    assert!(plans[0].match_plan.keys.is_empty());
}

// =========================================================================
// 6. compile_compound_keys
// =========================================================================

#[test]
fn compile_compound_keys() {
    let schemas = [generic_window(), output_window()];
    let plans = compile_with(
        r#"
rule compound {
    events { e : win }
    match<sip,dport:5m> {
        on event { e | count >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#,
        &schemas,
    );
    let keys = &plans[0].match_plan.keys;
    assert_eq!(keys.len(), 2);
    assert_eq!(keys[0], FieldRef::Simple("sip".into()));
    assert_eq!(keys[1], FieldRef::Simple("dport".into()));
}

// =========================================================================
// 7. compile_entity_type_normalization
// =========================================================================

#[test]
fn compile_entity_type_normalization() {
    let schemas = [generic_window(), output_window()];

    // Ident form: ip
    let plans_ident = compile_with(
        r#"
rule r1 {
    events { e : win }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#,
        &schemas,
    );
    assert_eq!(plans_ident[0].entity_plan.entity_type, "ip");

    // StringLit form: "ip"
    let plans_str = compile_with(
        r#"
rule r2 {
    events { e : win }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity("ip", e.sip)
    yield out (x = e.sip)
}
"#,
        &schemas,
    );
    assert_eq!(plans_str[0].entity_plan.entity_type, "ip");

    // Both normalize to the same string
    assert_eq!(
        plans_ident[0].entity_plan.entity_type,
        plans_str[0].entity_plan.entity_type
    );
}

/// Uppercase entity type is lowercased during compilation.
#[test]
fn compile_entity_type_case_normalization() {
    let schemas = [generic_window(), output_window()];

    let plans = compile_with(
        r#"
rule r {
    events { e : win }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(IP, e.sip)
    yield out (x = e.sip)
}
"#,
        &schemas,
    );
    assert_eq!(plans[0].entity_plan.entity_type, "ip");

    let plans2 = compile_with(
        r#"
rule r {
    events { e : win }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity("IP", e.sip)
    yield out (x = e.sip)
}
"#,
        &schemas,
    );
    assert_eq!(plans2[0].entity_plan.entity_type, "ip");
}

// =========================================================================
// 8. compile_multiple_rules
// =========================================================================

#[test]
fn compile_multiple_rules() {
    let schemas = [generic_window(), output_window()];
    let plans = compile_with(
        r#"
rule r1 {
    events { e : win }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}

rule r2 {
    events { e : win }
    match<:1h> { on event { e.dport | sum >= 100; } } -> score(30.0)
    entity(host, e.host)
    yield out (y = e.host)
}
"#,
        &schemas,
    );
    assert_eq!(plans.len(), 2);
    assert_eq!(plans[0].name, "r1");
    assert_eq!(plans[1].name, "r2");
}

// =========================================================================
// 9. compile_yield_fields
// =========================================================================

#[test]
fn compile_yield_fields() {
    let schemas = [auth_events_window(), output_window()];
    let plans = compile_with(
        r#"
rule r {
    events { fail : auth_events }
    match<sip:5m> { on event { fail | count >= 3; } } -> score(70.0)
    entity(ip, fail.sip)
    yield out (
        x = fail.sip,
        n = count(fail)
    )
}
"#,
        &schemas,
    );
    let yp = &plans[0].yield_plan;
    assert_eq!(yp.target, "out");
    assert_eq!(yp.fields.len(), 2);

    assert_eq!(yp.fields[0].name, "x");
    assert_eq!(
        yp.fields[0].value,
        Expr::Field(FieldRef::Qualified("fail".into(), "sip".into()))
    );

    assert_eq!(yp.fields[1].name, "n");
    assert!(matches!(
        &yp.fields[1].value,
        Expr::FuncCall { name, args, .. } if name == "count" && args.len() == 1
    ));
}

// =========================================================================
// 10. compile_score_arithmetic
// =========================================================================

#[test]
fn compile_score_arithmetic() {
    let schemas = [generic_window(), output_window()];
    let plans = compile_with(
        r#"
rule r {
    events { e : win }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0 + 20.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#,
        &schemas,
    );
    assert!(matches!(
        &plans[0].score_plan.expr,
        Expr::BinOp {
            op: BinOp::Add,
            ..
        }
    ));
}

// =========================================================================
// 11. compile_bracket_field_ref
// =========================================================================

#[test]
fn compile_bracket_field_ref() {
    // The bracket key e["detail.sha256"] is not type-checked for L1 (checker
    // skips bracket-ref field existence), so generic_window suffices.
    let schemas = [generic_window(), output_window()];
    let plans = compile_with(
        r#"
rule r {
    events { e : win }
    match<e["detail.sha256"]:5m> {
        on event { e | count >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#,
        &schemas,
    );
    let keys = &plans[0].match_plan.keys;
    assert_eq!(keys.len(), 1);
    assert_eq!(
        keys[0],
        FieldRef::Bracketed("e".into(), "detail.sha256".into())
    );
}

// =========================================================================
// 12. compile_event_filter
// =========================================================================

#[test]
fn compile_event_filter() {
    let schemas = [generic_window(), output_window()];
    let plans = compile_with(
        r#"
rule r {
    events { e : win && action == "failed" }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#,
        &schemas,
    );
    let bind = &plans[0].binds[0];
    assert!(bind.filter.is_some());
    assert!(matches!(
        bind.filter.as_ref().unwrap(),
        Expr::BinOp {
            op: BinOp::Eq,
            ..
        }
    ));
}

// =========================================================================
// 13. compile_labeled_branch
// =========================================================================

#[test]
fn compile_labeled_branch() {
    let schemas = [generic_window(), generic_window2(), output_window()];
    let plans = compile_with(
        r#"
rule r {
    events { a : win  b : win2 }
    match<sip:5m> {
        on event {
            lbl: a | count >= 3;
        }
    } -> score(50.0)
    entity(ip, a.sip)
    yield out (x = a.sip)
}
"#,
        &schemas,
    );
    let branch = &plans[0].match_plan.event_steps[0].branches[0];
    assert_eq!(branch.label, Some("lbl".into()));
}

// =========================================================================
// 14. compile_empty_file
// =========================================================================

#[test]
fn compile_empty_file() {
    let plans = compile_with("", &[]);
    assert!(plans.is_empty());
}

// =========================================================================
// 15. compile_rejects_semantic_errors
// =========================================================================

#[test]
fn compile_rejects_semantic_errors() {
    let file = parse_wfl(
        r#"
rule r {
    events { e : nonexistent_window }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#,
    )
    .unwrap();
    let err = compile_wfl(&file, &[output_window()]).unwrap_err();
    assert!(
        err.to_string().contains("nonexistent_window"),
        "error should mention the bad window: {err}",
    );
}
