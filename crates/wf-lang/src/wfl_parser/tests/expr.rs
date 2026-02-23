use crate::ast::*;
use crate::parse_wfl;

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
