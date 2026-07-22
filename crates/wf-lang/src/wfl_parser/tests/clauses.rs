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

#[test]
fn parse_yield_preset_and_refs() {
    let input = r#"
yield preset base_alerts (
    y = "wfl",
    n = 1
)

yield preset severity_fields (
    y = "medium"
)

rule r {
    events { fail : auth_events }
    match<sip:5m> { on event { fail | count >= 3; } } -> score(70.0)
    entity(ip, fail.sip)
    yield out : base_alerts, severity_fields (
        x = fail.sip
    )
}
"#;
    let file = parse_wfl(input).unwrap();
    assert_eq!(file.yield_presets.len(), 2);
    assert_eq!(file.yield_presets[0].name, "base_alerts");
    assert_eq!(file.yield_presets[0].args.len(), 2);
    assert_eq!(file.yield_presets[1].name, "severity_fields");

    let y = &file.rules[0].yield_clause;
    assert_eq!(y.target, "out");
    assert_eq!(y.presets, vec!["base_alerts", "severity_fields"]);
    assert_eq!(y.args.len(), 1);
    assert_eq!(y.args[0].name, "x");
}

#[test]
fn parse_yield_version_with_preset_and_empty_args() {
    let input = r#"
yield preset base_alerts (
    y = "wfl"
)

rule r {
    events { fail : auth_events }
    match<sip:5m> { on event { fail | count >= 3; } } -> score(70.0)
    entity(ip, fail.sip)
    yield out@v2 : base_alerts ()
}
"#;
    let file = parse_wfl(input).unwrap();
    let y = &file.rules[0].yield_clause;
    assert_eq!(y.target, "out");
    assert_eq!(y.version, Some(2));
    assert_eq!(y.presets, vec!["base_alerts"]);
    assert!(y.args.is_empty());
}

#[test]
fn parse_yield_presets_and_patterns_can_be_interleaved_before_rules() {
    let input = r#"
yield preset base_alerts (
    y = "wfl"
)

pattern burst(alias, key, win, threshold) {
    match<${key}:${win}> {
        on event { ${alias} | count >= ${threshold}; }
    } -> score(50.0)
}

yield preset severity_fields (
    y = "medium"
)

rule brute_force {
    events { e : auth_events }
    burst(e, sip, 5m, 5)
    entity(ip, e.sip)
    yield out : base_alerts, severity_fields (
        x = e.sip
    )
}
"#;
    let file = parse_wfl(input).unwrap();
    assert_eq!(file.patterns.len(), 1);
    assert_eq!(file.yield_presets.len(), 2);
    assert_eq!(file.rules[0].yield_clause.presets.len(), 2);
    assert!(file.rules[0].pattern_origin.is_some());
}
