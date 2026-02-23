use crate::ast::*;
use crate::parse_wfl;

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
