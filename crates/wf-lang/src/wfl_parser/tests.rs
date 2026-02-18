use std::time::Duration;

use super::parse_wfl;
use crate::ast::*;

// -----------------------------------------------------------------------
// use declarations
// -----------------------------------------------------------------------

#[test]
fn parse_use_decl() {
    let file = parse_wfl(r#"use "security.wfs""#).unwrap();
    assert_eq!(file.uses.len(), 1);
    assert_eq!(file.uses[0].path, "security.wfs");
}

#[test]
fn parse_multiple_uses() {
    let file = parse_wfl(
        r#"
use "security.wfs"
use "dns.wfs"
"#,
    )
    .unwrap();
    assert_eq!(file.uses.len(), 2);
}

// -----------------------------------------------------------------------
// Minimal rule
// -----------------------------------------------------------------------

#[test]
fn parse_minimal_rule() {
    let input = r#"
use "security.wfs"

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
        message = fmt("{} failed {} times", fail.sip, count(fail))
    )
}
"#;
    let file = parse_wfl(input).unwrap();
    assert_eq!(file.uses.len(), 1);
    assert_eq!(file.rules.len(), 1);

    let rule = &file.rules[0];
    assert_eq!(rule.name, "brute_force");
    assert!(rule.meta.is_none());
    assert_eq!(rule.events.decls.len(), 1);
    assert_eq!(rule.events.decls[0].alias, "fail");
    assert_eq!(rule.events.decls[0].window, "auth_events");
    assert!(rule.events.decls[0].filter.is_some());
}

// -----------------------------------------------------------------------
// Meta block
// -----------------------------------------------------------------------

#[test]
fn parse_meta_block() {
    let input = r#"
rule test_rule {
    meta {
        description = "Test rule"
        mitre = "T1110"
    }
    events { e : win }
    match<:5m> {
        on event { e | count >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    let meta = file.rules[0].meta.as_ref().unwrap();
    assert_eq!(meta.entries.len(), 2);
    assert_eq!(meta.entries[0].key, "description");
    assert_eq!(meta.entries[0].value, "Test rule");
    assert_eq!(meta.entries[1].key, "mitre");
    assert_eq!(meta.entries[1].value, "T1110");
}

// -----------------------------------------------------------------------
// Events block
// -----------------------------------------------------------------------

#[test]
fn parse_events_with_filter() {
    let input = r#"
rule r {
    events {
        fail : auth_events && action == "failed"
        scan : fw_events
    }
    match<sip:5m> {
        on event { fail | count >= 1; }
    } -> score(50.0)
    entity(ip, fail.sip)
    yield out (x = fail.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    let events = &file.rules[0].events;
    assert_eq!(events.decls.len(), 2);
    assert_eq!(events.decls[0].alias, "fail");
    assert!(events.decls[0].filter.is_some());
    assert_eq!(events.decls[1].alias, "scan");
    assert!(events.decls[1].filter.is_none());
}

// -----------------------------------------------------------------------
// Match clause
// -----------------------------------------------------------------------

#[test]
fn parse_match_single_key() {
    let input = r#"
rule r {
    events { e : win }
    match<sip:5m> {
        on event { e | count >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    let mc = &file.rules[0].match_clause;
    assert_eq!(mc.keys, vec![FieldRef::Simple("sip".into())]);
    assert_eq!(mc.duration, Duration::from_secs(300));
}

#[test]
fn parse_match_compound_keys() {
    let input = r#"
rule r {
    events { e : win }
    match<sip,dport:5m> {
        on event { e | count >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    let mc = &file.rules[0].match_clause;
    assert_eq!(mc.keys.len(), 2);
    assert_eq!(mc.keys[0], FieldRef::Simple("sip".into()));
    assert_eq!(mc.keys[1], FieldRef::Simple("dport".into()));
}

#[test]
fn parse_match_no_keys() {
    let input = r#"
rule r {
    events { e : win }
    match<:5m> {
        on event { e | count >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    assert!(file.rules[0].match_clause.keys.is_empty());
}

// -----------------------------------------------------------------------
// Match steps and OR branches
// -----------------------------------------------------------------------

#[test]
fn parse_multiple_steps() {
    let input = r#"
rule r {
    events { fail : auth_events  scan : fw_events }
    match<sip:5m> {
        on event {
            fail | count >= 3;
            scan.dport | distinct | count > 10;
        }
    } -> score(80.0)
    entity(ip, fail.sip)
    yield out (x = fail.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    let steps = &file.rules[0].match_clause.on_event;
    assert_eq!(steps.len(), 2);
    assert_eq!(steps[0].branches[0].source, "fail");
    assert!(steps[0].branches[0].field.is_none());
    assert_eq!(steps[0].branches[0].pipe.measure, Measure::Count);
    assert_eq!(steps[0].branches[0].pipe.cmp, CmpOp::Ge);

    assert_eq!(steps[1].branches[0].source, "scan");
    assert_eq!(
        steps[1].branches[0].field,
        Some(FieldSelector::Dot("dport".into()))
    );
    assert_eq!(
        steps[1].branches[0].pipe.transforms,
        vec![Transform::Distinct]
    );
    assert_eq!(steps[1].branches[0].pipe.measure, Measure::Count);
    assert_eq!(steps[1].branches[0].pipe.cmp, CmpOp::Gt);
}

#[test]
fn parse_or_branches() {
    let input = r#"
rule r {
    events { a : win  b : win2 }
    match<sip:5m> {
        on event {
            a | count >= 3 || b | count >= 5;
        }
    } -> score(60.0)
    entity(ip, a.sip)
    yield out (x = a.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    let step = &file.rules[0].match_clause.on_event[0];
    assert_eq!(step.branches.len(), 2);
    assert_eq!(step.branches[0].source, "a");
    assert_eq!(step.branches[1].source, "b");
}

// -----------------------------------------------------------------------
// on close block
// -----------------------------------------------------------------------

#[test]
fn parse_on_close() {
    let input = r#"
rule r {
    events { req : dns_query  resp : dns_response }
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
"#;
    let file = parse_wfl(input).unwrap();
    let mc = &file.rules[0].match_clause;
    assert_eq!(mc.on_event.len(), 1);
    assert!(mc.on_close.is_some());
    let close_steps = mc.on_close.as_ref().unwrap();
    assert_eq!(close_steps.len(), 1);
    assert_eq!(close_steps[0].branches[0].source, "resp");
    assert!(close_steps[0].branches[0].guard.is_some());
    assert_eq!(close_steps[0].branches[0].pipe.measure, Measure::Count);
    assert_eq!(close_steps[0].branches[0].pipe.cmp, CmpOp::Eq);
}

// -----------------------------------------------------------------------
// Score
// -----------------------------------------------------------------------

#[test]
fn parse_score_number() {
    let input = r#"
rule r {
    events { e : win }
    match<:5m> {
        on event { e | count >= 1; }
    } -> score(80.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    assert_eq!(file.rules[0].score.expr, Expr::Number(80.0));
}

// -----------------------------------------------------------------------
// Entity
// -----------------------------------------------------------------------

#[test]
fn parse_entity_ident_type() {
    let input = r#"
rule r {
    events { e : win }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    assert_eq!(
        file.rules[0].entity.entity_type,
        EntityTypeVal::Ident("ip".into())
    );
    assert_eq!(
        file.rules[0].entity.id_expr,
        Expr::Field(FieldRef::Qualified("e".into(), "sip".into()))
    );
}

#[test]
fn parse_entity_string_type() {
    let input = r#"
rule r {
    events { e : win }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity("process", e.name)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    assert_eq!(
        file.rules[0].entity.entity_type,
        EntityTypeVal::StringLit("process".into())
    );
}

// -----------------------------------------------------------------------
// Yield
// -----------------------------------------------------------------------

#[test]
fn parse_yield_clause() {
    let input = r#"
rule r {
    events { fail : auth_events }
    match<sip:5m> { on event { fail | count >= 3; } } -> score(70.0)
    entity(ip, fail.sip)
    yield security_alerts (
        sip = fail.sip,
        fail_count = count(fail),
        message = fmt("{} failed {} times", fail.sip, count(fail))
    )
}
"#;
    let file = parse_wfl(input).unwrap();
    let y = &file.rules[0].yield_clause;
    assert_eq!(y.target, "security_alerts");
    assert_eq!(y.args.len(), 3);
    assert_eq!(y.args[0].name, "sip");
    assert_eq!(y.args[1].name, "fail_count");
    assert_eq!(y.args[2].name, "message");
}

// -----------------------------------------------------------------------
// Expressions
// -----------------------------------------------------------------------

#[test]
fn parse_expr_comparison() {
    let input = r#"
rule r {
    events { e : win && count > 5 }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    let filter = file.rules[0].events.decls[0].filter.as_ref().unwrap();
    assert!(matches!(filter, Expr::BinOp { op: BinOp::Gt, .. }));
}

#[test]
fn parse_expr_logical_and() {
    let input = r#"
rule r {
    events { e : win && action == "failed" && result == "error" }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    let filter = file.rules[0].events.decls[0].filter.as_ref().unwrap();
    assert!(matches!(filter, Expr::BinOp { op: BinOp::And, .. }));
}

#[test]
fn parse_expr_arithmetic() {
    let input = r#"
rule r {
    events { e : win }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0 + 20.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    let score_expr = &file.rules[0].score.expr;
    assert!(matches!(score_expr, Expr::BinOp { op: BinOp::Add, .. }));
}

#[test]
fn parse_expr_function_call() {
    let input = r#"
rule r {
    events { fail : auth_events }
    match<sip:5m> { on event { fail | count >= 3; } } -> score(70.0)
    entity(ip, fail.sip)
    yield out (
        n = count(fail),
        msg = fmt("{} failed", fail.sip)
    )
}
"#;
    let file = parse_wfl(input).unwrap();
    let y = &file.rules[0].yield_clause;

    // count(fail)
    match &y.args[0].value {
        Expr::FuncCall {
            qualifier,
            name,
            args,
        } => {
            assert!(qualifier.is_none());
            assert_eq!(name, "count");
            assert_eq!(args.len(), 1);
        }
        other => panic!("expected FuncCall, got {other:?}"),
    }

    // fmt(...)
    match &y.args[1].value {
        Expr::FuncCall { name, args, .. } => {
            assert_eq!(name, "fmt");
            assert_eq!(args.len(), 2);
        }
        other => panic!("expected FuncCall, got {other:?}"),
    }
}

#[test]
fn parse_expr_field_refs() {
    let input = r#"
rule r {
    events { e : win }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (
        a = sip,
        b = e.sip,
        c = e["detail.sha256"]
    )
}
"#;
    let file = parse_wfl(input).unwrap();
    let y = &file.rules[0].yield_clause;
    assert_eq!(y.args[0].value, Expr::Field(FieldRef::Simple("sip".into())));
    assert_eq!(
        y.args[1].value,
        Expr::Field(FieldRef::Qualified("e".into(), "sip".into()))
    );
    assert_eq!(
        y.args[2].value,
        Expr::Field(FieldRef::Bracketed("e".into(), "detail.sha256".into()))
    );
}

#[test]
fn parse_expr_unary_neg() {
    let input = r#"
rule r {
    events { e : win }
    match<:5m> { on event { e | count >= 1; } } -> score(-1.0 + 100.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    match &file.rules[0].score.expr {
        Expr::BinOp {
            op: BinOp::Add,
            left,
            ..
        } => {
            assert!(matches!(left.as_ref(), Expr::Neg(_)));
        }
        other => panic!("expected BinOp Add, got {other:?}"),
    }
}

#[test]
fn parse_expr_bool_literal() {
    let input = r#"
rule r {
    events { e : win && active == true }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    let filter = file.rules[0].events.decls[0].filter.as_ref().unwrap();
    match filter {
        Expr::BinOp { right, .. } => {
            assert_eq!(right.as_ref(), &Expr::Bool(true));
        }
        other => panic!("expected BinOp, got {other:?}"),
    }
}

#[test]
fn parse_expr_in_list() {
    let input = r#"
rule r {
    events { e : win && action in ("a", "b", "c") }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    let filter = file.rules[0].events.decls[0].filter.as_ref().unwrap();
    match filter {
        Expr::InList { negated, list, .. } => {
            assert!(!negated);
            assert_eq!(list.len(), 3);
        }
        other => panic!("expected InList, got {other:?}"),
    }
}

#[test]
fn parse_expr_not_in() {
    let input = r#"
rule r {
    events { e : win && action not in ("x", "y") }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    let filter = file.rules[0].events.decls[0].filter.as_ref().unwrap();
    match filter {
        Expr::InList { negated, .. } => assert!(negated),
        other => panic!("expected InList, got {other:?}"),
    }
}

#[test]
fn parse_expr_parenthesized() {
    let input = r#"
rule r {
    events { e : win }
    match<:5m> { on event { e | count >= 1; } } -> score((50.0 + 30.0) * 1.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    match &file.rules[0].score.expr {
        Expr::BinOp { op: BinOp::Mul, .. } => {}
        other => panic!("expected Mul, got {other:?}"),
    }
}

// -----------------------------------------------------------------------
// Full examples from design docs
// -----------------------------------------------------------------------

#[test]
fn parse_brute_force_then_scan() {
    let input = r#"
use "security.wfs"

rule brute_force_then_scan {
    meta {
        description = "Login failures followed by port scan from same IP"
        mitre       = "T1110, T1046"
    }

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

    yield security_alerts (
        sip        = fail.sip,
        fail_count = count(fail),
        port_count = distinct(scan.dport),
        message    = fmt("{}: brute force then port scan detected", fail.sip)
    )
}
"#;
    let file = parse_wfl(input).unwrap();
    assert_eq!(file.uses.len(), 1);
    assert_eq!(file.rules.len(), 1);

    let rule = &file.rules[0];
    assert_eq!(rule.name, "brute_force_then_scan");
    assert!(rule.meta.is_some());
    assert_eq!(rule.meta.as_ref().unwrap().entries.len(), 2);
    assert_eq!(rule.events.decls.len(), 2);
    assert_eq!(rule.match_clause.keys, vec![FieldRef::Simple("sip".into())]);
    assert_eq!(rule.match_clause.duration, Duration::from_secs(300));
    assert_eq!(rule.match_clause.on_event.len(), 2);
    assert!(rule.match_clause.on_close.is_none());
    assert_eq!(rule.score.expr, Expr::Number(80.0));
    assert_eq!(rule.entity.entity_type, EntityTypeVal::Ident("ip".into()));
    assert_eq!(rule.yield_clause.target, "security_alerts");
    assert_eq!(rule.yield_clause.args.len(), 4);
}

#[test]
fn parse_dns_no_response() {
    let input = r#"
use "dns.wfs"

rule dns_no_response {
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
    yield security_alerts (
        sip = req.sip,
        domain = req.domain,
        message = fmt("{} query {} no response", req.sip, req.domain)
    )
}
"#;
    let file = parse_wfl(input).unwrap();
    let rule = &file.rules[0];
    assert_eq!(rule.name, "dns_no_response");
    assert!(rule.match_clause.on_close.is_some());
    let close = rule.match_clause.on_close.as_ref().unwrap();
    assert_eq!(close.len(), 1);
    assert_eq!(close[0].branches[0].source, "resp");
}

// -----------------------------------------------------------------------
// Multiple rules
// -----------------------------------------------------------------------

#[test]
fn parse_multiple_rules() {
    let input = r#"
use "sec.wfs"

rule r1 {
    events { e : win }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}

rule r2 {
    events { e : win }
    match<:1h> { on event { e | sum >= 100; } } -> score(30.0)
    entity(host, e.host)
    yield out (x = e.host)
}
"#;
    let file = parse_wfl(input).unwrap();
    assert_eq!(file.rules.len(), 2);
    assert_eq!(file.rules[0].name, "r1");
    assert_eq!(file.rules[1].name, "r2");
}

// -----------------------------------------------------------------------
// Empty file
// -----------------------------------------------------------------------

#[test]
fn parse_empty_file() {
    let file = parse_wfl("").unwrap();
    assert!(file.uses.is_empty());
    assert!(file.rules.is_empty());
    assert!(file.contracts.is_empty());
}

// -----------------------------------------------------------------------
// Error cases
// -----------------------------------------------------------------------

#[test]
fn reject_missing_events() {
    let input = r#"
rule r {
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert!(parse_wfl(input).is_err());
}

#[test]
fn reject_missing_score() {
    let input = r#"
rule r {
    events { e : win }
    match<:5m> { on event { e | count >= 1; } }
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert!(parse_wfl(input).is_err());
}

#[test]
fn reject_digit_leading_ident() {
    // rule name starting with digit
    let input = r#"
rule 1bad {
    events { e : win }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert!(parse_wfl(input).is_err());
}

#[test]
fn reject_empty_events() {
    let input = r#"
rule r {
    events { }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert!(parse_wfl(input).is_err());
}

#[test]
fn reject_empty_on_event() {
    let input = r#"
rule r {
    events { e : win }
    match<:5m> { on event { } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert!(parse_wfl(input).is_err());
}

#[test]
fn parse_match_key_bracket_ref() {
    let input = r#"
rule r {
    events { e : win }
    match<e["detail.sha256"]:5m> {
        on event { e | count >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    let mc = &file.rules[0].match_clause;
    assert_eq!(
        mc.keys,
        vec![FieldRef::Bracketed("e".into(), "detail.sha256".into())]
    );
}

#[test]
fn reject_contract_decimal_hit_index() {
    let input = r#"
contract ct for r {
    given { row(e, x = 1); }
    expect {
        hit[0.5].score == 50.0;
    }
}
"#;
    assert!(parse_wfl(input).is_err());
}

#[test]
fn reject_contract_decimal_hits_count() {
    let input = r#"
contract ct for r {
    given { row(e, x = 1); }
    expect {
        hits == 1.5;
    }
}
"#;
    assert!(parse_wfl(input).is_err());
}

// -----------------------------------------------------------------------
// Contract blocks
// -----------------------------------------------------------------------

#[test]
fn parse_contract_full() {
    let input = r#"
contract dns_no_response_timeout for dns_no_response {
    given {
        row(req,
            query_id = "q-1",
            sip = "10.0.0.8",
            domain = "evil.test",
            event_time = "2026-02-17T10:00:00Z"
        );
        tick(31s);
    }
    expect {
        hits == 1;
        hit[0].score == 50.0;
        hit[0].close_reason == "timeout";
        hit[0].entity_type == "ip";
        hit[0].entity_id == "10.0.0.8";
        hit[0].field("domain") == "evil.test";
    }
    options {
        close_trigger = timeout;
        eval_mode = strict;
    }
}
"#;
    let file = parse_wfl(input).unwrap();
    assert_eq!(file.contracts.len(), 1);

    let c = &file.contracts[0];
    assert_eq!(c.name, "dns_no_response_timeout");
    assert_eq!(c.rule_name, "dns_no_response");

    // given
    assert_eq!(c.given.len(), 2);
    match &c.given[0] {
        GivenStmt::Row { alias, fields } => {
            assert_eq!(alias, "req");
            assert_eq!(fields.len(), 4);
            assert_eq!(fields[0].name, "query_id");
            assert_eq!(fields[1].name, "sip");
            assert_eq!(fields[2].name, "domain");
            assert_eq!(fields[3].name, "event_time");
        }
        other => panic!("expected Row, got {other:?}"),
    }
    assert_eq!(c.given[1], GivenStmt::Tick(Duration::from_secs(31)));

    // expect
    assert_eq!(c.expect.len(), 6);
    assert_eq!(
        c.expect[0],
        ExpectStmt::Hits {
            cmp: CmpOp::Eq,
            count: 1
        }
    );
    match &c.expect[1] {
        ExpectStmt::HitAssert { index, assert } => {
            assert_eq!(*index, 0);
            assert_eq!(
                *assert,
                HitAssert::Score {
                    cmp: CmpOp::Eq,
                    value: 50.0
                }
            );
        }
        other => panic!("expected HitAssert, got {other:?}"),
    }
    match &c.expect[2] {
        ExpectStmt::HitAssert { assert, .. } => {
            assert_eq!(
                *assert,
                HitAssert::CloseReason {
                    value: "timeout".into()
                }
            );
        }
        other => panic!("expected HitAssert, got {other:?}"),
    }
    match &c.expect[5] {
        ExpectStmt::HitAssert { assert, .. } => {
            assert!(
                matches!(assert, HitAssert::Field { name, cmp: CmpOp::Eq, .. } if name == "domain")
            );
        }
        other => panic!("expected HitAssert field, got {other:?}"),
    }

    // options
    let opts = c.options.as_ref().unwrap();
    assert_eq!(opts.close_trigger, Some(CloseTrigger::Timeout));
    assert_eq!(opts.eval_mode, Some(EvalMode::Strict));
}

#[test]
fn parse_contract_no_options() {
    let input = r#"
contract simple_test for my_rule {
    given {
        row(e, action = "failed");
        tick(5m);
    }
    expect {
        hits >= 1;
    }
}
"#;
    let file = parse_wfl(input).unwrap();
    let c = &file.contracts[0];
    assert_eq!(c.name, "simple_test");
    assert_eq!(c.rule_name, "my_rule");
    assert!(c.options.is_none());
    assert_eq!(c.given.len(), 2);
    assert_eq!(
        c.expect[0],
        ExpectStmt::Hits {
            cmp: CmpOp::Ge,
            count: 1
        }
    );
}

#[test]
fn parse_contract_options_only_close_trigger() {
    let input = r#"
contract ct for r {
    given { row(e, x = 1); }
    expect { hits == 0; }
    options { close_trigger = flush; }
}
"#;
    let file = parse_wfl(input).unwrap();
    let opts = file.contracts[0].options.as_ref().unwrap();
    assert_eq!(opts.close_trigger, Some(CloseTrigger::Flush));
    assert_eq!(opts.eval_mode, None);
}

#[test]
fn parse_contract_options_only_eval_mode() {
    let input = r#"
contract ct for r {
    given { row(e, x = 1); }
    expect { hits == 0; }
    options { eval_mode = lenient; }
}
"#;
    let file = parse_wfl(input).unwrap();
    let opts = file.contracts[0].options.as_ref().unwrap();
    assert_eq!(opts.close_trigger, None);
    assert_eq!(opts.eval_mode, Some(EvalMode::Lenient));
}

#[test]
fn parse_contract_options_eos() {
    let input = r#"
contract ct for r {
    given { row(e, x = 1); }
    expect { hits == 1; }
    options { close_trigger = eos; }
}
"#;
    let file = parse_wfl(input).unwrap();
    let opts = file.contracts[0].options.as_ref().unwrap();
    assert_eq!(opts.close_trigger, Some(CloseTrigger::Eos));
}

#[test]
fn parse_contract_field_assert_expr() {
    let input = r#"
contract ct for r {
    given { row(e, count = 10); }
    expect {
        hits == 1;
        hit[0].field("count") >= 5 + 3;
    }
}
"#;
    let file = parse_wfl(input).unwrap();
    match &file.contracts[0].expect[1] {
        ExpectStmt::HitAssert {
            index,
            assert: HitAssert::Field { name, cmp, value },
        } => {
            assert_eq!(*index, 0);
            assert_eq!(name, "count");
            assert_eq!(*cmp, CmpOp::Ge);
            assert!(matches!(value, Expr::BinOp { op: BinOp::Add, .. }));
        }
        other => panic!("expected field assert, got {other:?}"),
    }
}

#[test]
fn parse_contract_string_field_name() {
    let input = r#"
contract ct for r {
    given {
        row(e, "detail.sha256" = "abc123");
    }
    expect { hits == 1; }
}
"#;
    let file = parse_wfl(input).unwrap();
    match &file.contracts[0].given[0] {
        GivenStmt::Row { fields, .. } => {
            assert_eq!(fields[0].name, "detail.sha256");
        }
        other => panic!("expected Row, got {other:?}"),
    }
}

#[test]
fn parse_multiple_contracts() {
    let input = r#"
contract ct1 for r1 {
    given { row(e, x = 1); }
    expect { hits == 1; }
}
contract ct2 for r2 {
    given { row(e, x = 2); tick(10s); }
    expect { hits == 0; }
}
"#;
    let file = parse_wfl(input).unwrap();
    assert_eq!(file.contracts.len(), 2);
    assert_eq!(file.contracts[0].name, "ct1");
    assert_eq!(file.contracts[1].name, "ct2");
}

#[test]
fn parse_rules_and_contracts() {
    let input = r#"
use "security.wfs"

rule brute_force {
    events { fail : auth_events && action == "failed" }
    match<sip:5m> {
        on event { fail | count >= 3; }
    } -> score(70.0)
    entity(ip, fail.sip)
    yield security_alerts (sip = fail.sip)
}

contract brute_test for brute_force {
    given {
        row(fail, action = "failed", sip = "1.2.3.4");
        row(fail, action = "failed", sip = "1.2.3.4");
        row(fail, action = "failed", sip = "1.2.3.4");
        tick(6m);
    }
    expect {
        hits == 1;
        hit[0].score == 70.0;
        hit[0].entity_id == "1.2.3.4";
    }
    options {
        close_trigger = timeout;
    }
}
"#;
    let file = parse_wfl(input).unwrap();
    assert_eq!(file.uses.len(), 1);
    assert_eq!(file.rules.len(), 1);
    assert_eq!(file.contracts.len(), 1);
    assert_eq!(file.contracts[0].rule_name, "brute_force");
    assert_eq!(file.contracts[0].given.len(), 4);

    // 3 rows + 1 tick
    let rows: Vec<_> = file.contracts[0]
        .given
        .iter()
        .filter(|s| matches!(s, GivenStmt::Row { .. }))
        .collect();
    assert_eq!(rows.len(), 3);
    assert_eq!(
        file.contracts[0].given[3],
        GivenStmt::Tick(Duration::from_secs(360))
    );
}

#[test]
fn parse_contract_multiple_rows() {
    let input = r#"
contract ct for r {
    given {
        row(req, query_id = "q-1", sip = "10.0.0.1");
        row(resp, query_id = "q-1", sip = "10.0.0.1");
        tick(31s);
    }
    expect {
        hits == 0;
    }
}
"#;
    let file = parse_wfl(input).unwrap();
    let c = &file.contracts[0];
    assert_eq!(c.given.len(), 3);
    match &c.given[0] {
        GivenStmt::Row { alias, fields } => {
            assert_eq!(alias, "req");
            assert_eq!(fields.len(), 2);
        }
        other => panic!("expected Row, got {other:?}"),
    }
    match &c.given[1] {
        GivenStmt::Row { alias, fields } => {
            assert_eq!(alias, "resp");
            assert_eq!(fields.len(), 2);
        }
        other => panic!("expected Row, got {other:?}"),
    }
}

#[test]
fn parse_contract_hit_score_cmp() {
    let input = r#"
contract ct for r {
    given { row(e, x = 1); }
    expect {
        hit[0].score >= 50.0;
        hit[0].score <= 100.0;
        hit[1].score != 0;
    }
}
"#;
    let file = parse_wfl(input).unwrap();
    let stmts = &file.contracts[0].expect;
    assert_eq!(stmts.len(), 3);
    match &stmts[0] {
        ExpectStmt::HitAssert {
            index: 0,
            assert: HitAssert::Score { cmp, value },
        } => {
            assert_eq!(*cmp, CmpOp::Ge);
            assert_eq!(*value, 50.0);
        }
        other => panic!("expected score >= 50, got {other:?}"),
    }
    match &stmts[2] {
        ExpectStmt::HitAssert {
            index: 1,
            assert: HitAssert::Score { cmp, value },
        } => {
            assert_eq!(*cmp, CmpOp::Ne);
            assert_eq!(*value, 0.0);
        }
        other => panic!("expected score != 0, got {other:?}"),
    }
}
