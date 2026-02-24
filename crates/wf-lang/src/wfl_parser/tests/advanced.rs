use std::time::Duration;

use crate::ast::*;
use crate::parse_wfl;

// -----------------------------------------------------------------------
// Bracket field refs in match keys
// -----------------------------------------------------------------------

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

// -----------------------------------------------------------------------
// L2: limits block
// -----------------------------------------------------------------------

#[test]
fn parse_limits_block() {
    let input = r#"
rule r {
    events { e : win }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    limits {
        max_cardinality = 10000;
        max_emit_rate = "1000/min";
        on_exceed = drop_oldest;
    }
}
"#;
    let file = parse_wfl(input).unwrap();
    let rule = &file.rules[0];
    let limits = rule.limits.as_ref().unwrap();
    assert_eq!(limits.items.len(), 3);
    assert_eq!(limits.items[0].key, "max_cardinality");
    assert_eq!(limits.items[0].value, "10000");
    assert_eq!(limits.items[1].key, "max_emit_rate");
    assert_eq!(limits.items[1].value, "1000/min");
    assert_eq!(limits.items[2].key, "on_exceed");
    assert_eq!(limits.items[2].value, "drop_oldest");
}

#[test]
fn parse_rule_without_limits() {
    let input = r#"
rule r {
    events { e : win }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    assert!(file.rules[0].limits.is_none());
}

// -----------------------------------------------------------------------
// L2: key mapping block
// -----------------------------------------------------------------------

#[test]
fn parse_key_mapping() {
    let input = r#"
rule r {
    events {
        a : win1
        b : win2
    }
    match<:5m> {
        key {
            user_id = a.uid;
            user_id = b.user_name;
        }
        on event {
            a | count >= 1;
        }
    } -> score(50.0)
    entity(ip, a.sip)
    yield out (x = a.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    let mc = &file.rules[0].match_clause;
    let km = mc.key_mapping.as_ref().unwrap();
    assert_eq!(km.len(), 2);
    assert_eq!(km[0].logical_name, "user_id");
    assert_eq!(
        km[0].source_field,
        FieldRef::Qualified("a".into(), "uid".into())
    );
    assert_eq!(km[1].logical_name, "user_id");
    assert_eq!(
        km[1].source_field,
        FieldRef::Qualified("b".into(), "user_name".into())
    );
}

#[test]
fn parse_match_without_key_mapping() {
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
    assert!(file.rules[0].match_clause.key_mapping.is_none());
}

// -----------------------------------------------------------------------
// L2: join clause
// -----------------------------------------------------------------------

#[test]
fn parse_join_snapshot() {
    let input = r#"
rule r {
    events { e : win }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    join ip_repdb snapshot on sip == ip_repdb.ip
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    let rule = &file.rules[0];
    assert_eq!(rule.joins.len(), 1);
    assert_eq!(rule.joins[0].target_window, "ip_repdb");
    assert_eq!(rule.joins[0].mode, JoinMode::Snapshot);
    assert_eq!(rule.joins[0].conditions.len(), 1);
    assert_eq!(
        rule.joins[0].conditions[0].left,
        FieldRef::Simple("sip".into())
    );
    assert_eq!(
        rule.joins[0].conditions[0].right,
        FieldRef::Qualified("ip_repdb".into(), "ip".into())
    );
}

#[test]
fn parse_join_asof_with_within() {
    let input = r#"
rule r {
    events { e : win }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    join geo_log asof within 10m on sip == geo_log.src_ip
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    let rule = &file.rules[0];
    assert_eq!(rule.joins.len(), 1);
    assert_eq!(rule.joins[0].target_window, "geo_log");
    assert_eq!(
        rule.joins[0].mode,
        JoinMode::Asof {
            within: Some(Duration::from_secs(600))
        }
    );
}

#[test]
fn parse_join_asof_no_within() {
    let input = r#"
rule r {
    events { e : win }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    join geo_log asof on sip == geo_log.src_ip
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    assert_eq!(file.rules[0].joins[0].mode, JoinMode::Asof { within: None });
}

#[test]
fn parse_multiple_joins() {
    let input = r#"
rule r {
    events { e : win }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    join rep_db snapshot on sip == rep_db.ip
    join geo_log asof within 5m on sip == geo_log.src_ip
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    assert_eq!(file.rules[0].joins.len(), 2);
}

#[test]
fn parse_join_multi_cond() {
    let input = r#"
rule r {
    events { e : win }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    join t snapshot on sip == t.ip && dport == t.port
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    assert_eq!(file.rules[0].joins[0].conditions.len(), 2);
}

// -----------------------------------------------------------------------
// L2: baseline() with duration argument
// -----------------------------------------------------------------------

#[test]
fn parse_baseline_func_call() {
    let input = r#"
rule r {
    events { e : win }
    match<sip:5m> { on event { e | count >= 1; } }
    -> score(baseline(e.bytes, 30m))
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    let score = &file.rules[0].score.expr;
    match score {
        Expr::FuncCall { name, args, .. } => {
            assert_eq!(name, "baseline");
            assert_eq!(args.len(), 2);
            // Duration 30m = 1800 seconds
            assert_eq!(args[1], Expr::Number(1800.0));
        }
        other => panic!("expected baseline FuncCall, got {other:?}"),
    }
}

// -----------------------------------------------------------------------
// L2: window.has() — already supported by qualified func call
// -----------------------------------------------------------------------

#[test]
fn parse_window_has_call() {
    let input = r#"
rule r {
    events { e : win && threat_list.has(e.domain) }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    let filter = file.rules[0].events.decls[0].filter.as_ref().unwrap();
    match filter {
        Expr::FuncCall {
            qualifier,
            name,
            args,
        } => {
            assert_eq!(qualifier.as_deref(), Some("threat_list"));
            assert_eq!(name, "has");
            assert_eq!(args.len(), 1);
        }
        other => panic!("expected FuncCall, got {other:?}"),
    }
}

#[test]
fn parse_window_has_two_args() {
    let input = r#"
rule r {
    events { e : win && ip_repdb.has(e.sip, "ip") }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    let filter = file.rules[0].events.decls[0].filter.as_ref().unwrap();
    match filter {
        Expr::FuncCall { args, .. } => {
            assert_eq!(args.len(), 2);
        }
        other => panic!("expected FuncCall, got {other:?}"),
    }
}

// -----------------------------------------------------------------------
// L2: if-then-else expression
// -----------------------------------------------------------------------

#[test]
fn parse_if_expr_basic() {
    let input = r#"
rule r {
    events { e : win }
    match<sip:5m> { on event { e | count >= 1; } }
    -> score(if e.action == "failed" then 80.0 else 40.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    let score = &file.rules[0].score.expr;
    match score {
        Expr::IfThenElse {
            cond,
            then_expr,
            else_expr,
        } => {
            assert!(matches!(cond.as_ref(), Expr::BinOp { op: BinOp::Eq, .. }));
            assert_eq!(then_expr.as_ref(), &Expr::Number(80.0));
            assert_eq!(else_expr.as_ref(), &Expr::Number(40.0));
        }
        other => panic!("expected IfThenElse, got {other:?}"),
    }
}

#[test]
fn parse_if_expr_nested() {
    let input = r#"
rule r {
    events { e : win }
    match<sip:5m> { on event { e | count >= 1; } }
    -> score(if e.action == "a" then 80.0 else if e.action == "b" then 60.0 else 40.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    let score = &file.rules[0].score.expr;
    match score {
        Expr::IfThenElse { else_expr, .. } => {
            assert!(
                matches!(else_expr.as_ref(), Expr::IfThenElse { .. }),
                "else branch should be a nested IfThenElse"
            );
        }
        other => panic!("expected IfThenElse, got {other:?}"),
    }
}

#[test]
fn parse_if_expr_in_yield() {
    let input = r#"
rule r {
    events { e : win }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (
        x = e.sip,
        y = if e.action == "ok" then "good" else "bad"
    )
}
"#;
    let file = parse_wfl(input).unwrap();
    let y = &file.rules[0].yield_clause;
    assert!(matches!(y.args[1].value, Expr::IfThenElse { .. }));
}

// -----------------------------------------------------------------------
// L2: yield @vN
// -----------------------------------------------------------------------

#[test]
fn parse_yield_with_version() {
    let input = r#"
rule r {
    events { e : win }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out@v2 (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    let y = &file.rules[0].yield_clause;
    assert_eq!(y.target, "out");
    assert_eq!(y.version, Some(2));
    assert_eq!(y.args.len(), 1);
}

#[test]
fn parse_yield_without_version() {
    let input = r#"
rule r {
    events { e : win }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    assert_eq!(file.rules[0].yield_clause.version, None);
}
