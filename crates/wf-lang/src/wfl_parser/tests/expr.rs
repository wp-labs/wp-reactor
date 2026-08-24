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
fn parse_expr_logical_not_keyword() {
    // `not <cond>`：events 过滤里对整组条件取逻辑非（Sigma t1571 形态）。
    let input = r#"
rule r {
    events { e : win && not (e.state == "closed") }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    let filter = file.rules[0].events.decls[0].filter.as_ref().unwrap();
    match filter {
        Expr::Not(inner) => assert!(matches!(
            inner.as_ref(),
            Expr::BinOp { op: BinOp::Eq, .. }
        )),
        other => panic!("expected Not(Eq), got {other:?}"),
    }
}

#[test]
fn parse_expr_logical_not_bang() {
    // `!<cond>` 符号否定。
    let input = r#"
rule r {
    events { e : win && !(e.state == "closed") }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    let filter = file.rules[0].events.decls[0].filter.as_ref().unwrap();
    match filter {
        Expr::Not(inner) => assert!(matches!(
            inner.as_ref(),
            Expr::BinOp { op: BinOp::Eq, .. }
        )),
        other => panic!("expected Not(Eq), got {other:?}"),
    }
}

#[test]
fn parse_expr_not_binds_looser_than_comparison() {
    // `not a == b` 解析为 `not (a == b)`（优先级：逻辑 NOT > 比较）。
    let input = r#"
rule r {
    events { e : win && not e.state == "closed" }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    let filter = file.rules[0].events.decls[0].filter.as_ref().unwrap();
    match filter {
        Expr::Not(inner) => assert!(matches!(
            inner.as_ref(),
            Expr::BinOp { op: BinOp::Eq, .. }
        )),
        other => panic!("expected Not(Eq), got {other:?}"),
    }
}

#[test]
fn parse_expr_not_in_remains_in_list_negation() {
    // `x not in (...)` 仍是列表成员否定（InList negated），不被 not_expr 抢占。
    let input = r#"
rule r {
    events { e : win && e.state not in ("open", "closing") }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    let filter = file.rules[0].events.decls[0].filter.as_ref().unwrap();
    match filter {
        Expr::InList { negated: true, .. } => {}
        other => panic!("expected InList(negated), got {other:?}"),
    }
}

/// 解析 `events { e : win && <filter> }`，返回 filter 表达式。
fn filter_of(input: &str) -> Expr {
    let file = parse_wfl(input).unwrap();
    file.rules[0]
        .events
        .decls[0]
        .filter
        .clone()
        .unwrap_or(Expr::Bool(true))
}

#[test]
fn parse_expr_not_binds_tighter_than_or() {
    // `not a || b` → `(not a) || b`（逻辑 NOT 比 `||` 紧）。
    let filter = filter_of(
        r#"
rule r {
    events { e : win && not e.private || e.state == "closed" }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#,
    );
    match filter {
        Expr::BinOp {
            op: BinOp::Or,
            left,
            right,
        } => {
            assert!(matches!(left.as_ref(), Expr::Not(_)));
            assert!(matches!(
                right.as_ref(),
                Expr::BinOp { op: BinOp::Eq, .. }
            ));
        }
        other => panic!("expected Or(Not, Eq), got {other:?}"),
    }
}

#[test]
fn parse_expr_not_nested_double() {
    // `not not a` → Not(Not(a))。
    let filter = filter_of(
        r#"
rule r {
    events { e : win && not not e.private }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#,
    );
    match filter {
        Expr::Not(inner) => assert!(matches!(inner.as_ref(), Expr::Not(_))),
        other => panic!("expected Not(Not), got {other:?}"),
    }
}

#[test]
fn parse_expr_bang_before_neq_is_not_of_comparison() {
    // `!a != b` → `not (a != b)`（`!` 前缀 + `!=` 比较）。
    let filter = filter_of(
        r#"
rule r {
    events { e : win && !e.state != "closed" }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#,
    );
    match filter {
        Expr::Not(inner) => assert!(matches!(
            inner.as_ref(),
            Expr::BinOp { op: BinOp::Ne, .. }
        )),
        other => panic!("expected Not(Ne), got {other:?}"),
    }
}

#[test]
fn parse_expr_not_without_whitespace_before_paren() {
    // `not(x)`（无空格）也应解析。
    let filter = filter_of(
        r#"
rule r {
    events { e : win && not(e.state == "closed") }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#,
    );
    assert!(matches!(filter, Expr::Not(_)));
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
fn parse_structured_object_and_array_literals() {
    let input = r#"
rule r {
    events { e : win }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (
        risk_context = object {
            score: float = @score;
            source = e.sip;
            tags: array = array ["bruteforce", "ssh", e.action];
            geo: object = object {
                country = e.country;
            };
        }
    )
}
"#;
    let file = parse_wfl(input).unwrap();
    let value = &file.rules[0].yield_clause.args[0].value;
    let Expr::Object(items) = value else {
        panic!("expected object literal, got {value:?}");
    };
    assert_eq!(items.len(), 4);
    assert_eq!(items[0].targets, vec!["score"]);
    assert!(matches!(
        items[0].type_hint,
        Some(crate::schema::FieldType::Base(
            crate::schema::BaseType::Float
        ))
    ));
    assert_eq!(items[1].targets, vec!["source"]);
    assert!(matches!(
        items[2].type_hint,
        Some(crate::schema::FieldType::ArrayAny)
    ));
    assert!(matches!(items[2].value, Expr::Array(_)));
    assert!(matches!(
        items[3].type_hint,
        Some(crate::schema::FieldType::Object)
    ));
    assert!(matches!(items[3].value, Expr::Object(_)));
}

#[test]
fn parse_array_literal_allows_whitespace_around_commas_and_trailing_comma() {
    let input = r#"
rule r {
    events { e : win }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (
        tags = array [
            "bruteforce" ,
            e.action ,
        ]
    )
}
"#;
    let file = parse_wfl(input).unwrap();
    let value = &file.rules[0].yield_clause.args[0].value;
    let Expr::Array(items) = value else {
        panic!("expected array literal, got {value:?}");
    };
    assert_eq!(items.len(), 2);
}

#[test]
fn parse_expr_field_refs() {
    let input = r##"
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
"##;
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
fn parse_expr_nested_field_paths() {
    let input = r##"
rule r {
    events { e : win }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (
        a = e.roles_obj.source.process.uid,
        b = e.roles_obj.related[0].process.name
    )
}
"##;
    let file = parse_wfl(input).unwrap();
    let y = &file.rules[0].yield_clause;
    assert_eq!(
        y.args[0].value,
        Expr::Field(FieldRef::Path {
            alias: "e".into(),
            segments: vec![
                PathSegment::Field("roles_obj".into()),
                PathSegment::Field("source".into()),
                PathSegment::Field("process".into()),
                PathSegment::Field("uid".into()),
            ],
        })
    );
    assert_eq!(
        y.args[1].value,
        Expr::Field(FieldRef::Path {
            alias: "e".into(),
            segments: vec![
                PathSegment::Field("roles_obj".into()),
                PathSegment::Field("related".into()),
                PathSegment::Index(0),
                PathSegment::Field("process".into()),
                PathSegment::Field("name".into()),
            ],
        })
    );
}

#[test]
fn parse_expr_nested_path_shapes() {
    // FieldRef shape matrix: single-level stays Qualified, quoted bracket stays
    // Bracketed, and only multi-level dot/index access becomes Path.
    let input = r##"
rule r {
    events { e : win }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (
        a = e.sip,
        b = e["detail.sha256"],
        c = e.roles_obj.related[0],
        d = e.a[0][1],
        e = e.a.b[0].c
    )
}
"##;
    let file = parse_wfl(input).unwrap();
    let y = &file.rules[0].yield_clause;
    // Single-level: backward-compatible Qualified (no Path).
    assert_eq!(
        y.args[0].value,
        Expr::Field(FieldRef::Qualified("e".into(), "sip".into()))
    );
    // Quoted bracket: unchanged Bracketed (flat dotted name).
    assert_eq!(
        y.args[1].value,
        Expr::Field(FieldRef::Bracketed("e".into(), "detail.sha256".into()))
    );
    // member + index → Path.
    assert_eq!(
        y.args[2].value,
        Expr::Field(FieldRef::Path {
            alias: "e".into(),
            segments: vec![
                PathSegment::Field("roles_obj".into()),
                PathSegment::Field("related".into()),
                PathSegment::Index(0),
            ],
        })
    );
    // consecutive indices.
    assert_eq!(
        y.args[3].value,
        Expr::Field(FieldRef::Path {
            alias: "e".into(),
            segments: vec![
                PathSegment::Field("a".into()),
                PathSegment::Index(0),
                PathSegment::Index(1),
            ],
        })
    );
    // mixed members and indices.
    assert_eq!(
        y.args[4].value,
        Expr::Field(FieldRef::Path {
            alias: "e".into(),
            segments: vec![
                PathSegment::Field("a".into()),
                PathSegment::Field("b".into()),
                PathSegment::Index(0),
                PathSegment::Field("c".into()),
            ],
        })
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

#[test]
fn parse_expr_system_score_ref() {
    let input = r#"
rule r {
    events { e : win }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (risk_score = @score)
}
"#;
    let file = parse_wfl(input).unwrap();
    assert_eq!(
        file.rules[0].yield_clause.args[0].value,
        Expr::SystemVar(SystemVar::Score)
    );
}

#[test]
fn parse_expr_wfu_meta_refs() {
    let input = r#"
rule r {
    events { e : win }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (
        rule_name = @__wfu_rule_name,
        score = @__wfu_score
    )
}
"#;
    let file = parse_wfl(input).unwrap();
    assert_eq!(
        file.rules[0].yield_clause.args[0].value,
        Expr::WfuMeta(crate::wfu_meta::WfuMetaField::RuleName)
    );
    assert_eq!(
        file.rules[0].yield_clause.args[1].value,
        Expr::WfuMeta(crate::wfu_meta::WfuMetaField::Score)
    );
}

#[test]
fn parse_expr_time_system_vars() {
    let input = r#"
rule r {
    events { e : win }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (
        first_seen = @event_first_time,
        last_seen = @event_last_time,
        evidence_start_time = @evidence_start_time,
        evidence_end_time = @evidence_end_time,
        rule_window_start = @window_start_time,
        rule_window_end = @window_end_time,
        latest_analysis_time = @emit_time
    )
}
"#;
    let file = parse_wfl(input).unwrap();
    let vars: Vec<_> = file.rules[0]
        .yield_clause
        .args
        .iter()
        .map(|arg| &arg.value)
        .collect();
    assert_eq!(
        vars,
        vec![
            &Expr::SystemVar(SystemVar::EventFirstTime),
            &Expr::SystemVar(SystemVar::EventLastTime),
            &Expr::SystemVar(SystemVar::EvidenceStartTime),
            &Expr::SystemVar(SystemVar::EvidenceEndTime),
            &Expr::SystemVar(SystemVar::WindowStartTime),
            &Expr::SystemVar(SystemVar::WindowEndTime),
            &Expr::SystemVar(SystemVar::EmitTime),
        ]
    );
}
