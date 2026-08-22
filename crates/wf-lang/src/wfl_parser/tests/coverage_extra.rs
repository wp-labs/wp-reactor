//! Extra coverage tests for the WFL parser: clause-level parses (let, join
//! error paths, bound markers, yield versions) and top-level entry points.

use crate::ast::*;
use crate::parse_wfl;

// -----------------------------------------------------------------------
// let clause
// -----------------------------------------------------------------------

#[test]
fn parse_let_clause() {
    let input = r#"
rule r {
    events { e : win }
    let parts = split(e.action, "/")
    let first = mvindex(parts, 0)
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = first)
}
"#;
    let file = parse_wfl(input).unwrap();
    let lets = &file.rules[0].lets;
    assert_eq!(lets.len(), 2);
    assert_eq!(lets[0].name, "parts");
    assert!(matches!(
        &lets[0].expr,
        Expr::FuncCall { name, .. } if name == "split"
    ));
    assert_eq!(lets[1].name, "first");
}

#[test]
fn reject_let_without_binding_expr() {
    let input = r#"
rule r {
    events { e : win }
    let x =
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert!(parse_wfl(input).is_err());
}

// -----------------------------------------------------------------------
// join clause: error paths + bound markers
// -----------------------------------------------------------------------

#[test]
fn parse_join_missing_on_rejected() {
    let input = r#"
rule r {
    events { e : win }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    join other snapshot
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert!(parse_wfl(input).is_err());
}

#[test]
fn parse_join_condition_missing_eq_rejected() {
    let input = r#"
rule r {
    events { e : win }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    join other on e.sip other.sip
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert!(parse_wfl(input).is_err());
}

#[test]
fn parse_join_reduce_unknown_measure_rejected() {
    let input = r#"
rule r {
    events { e : win }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    join other reduce bogus(other.sip) on e.sip == other.sip
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert!(parse_wfl(input).is_err());
}

#[test]
fn parse_join_asof_without_duration() {
    let input = r#"
rule r {
    events { e : win }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    join other asof on e.sip == other.sip
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    let j = &file.rules[0].joins[0];
    assert_eq!(j.mode, JoinMode::Asof { within: None });
}

#[test]
fn parse_join_multi_condition_and_emit_at() {
    let input = r#"
rule r {
    events { e : win }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    join other on e.sip == other.sip && e.user == other.user emit at e.expires
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    let j = &file.rules[0].joins[0];
    assert_eq!(j.conditions.len(), 2);
    assert_eq!(
        j.conditions[0],
        JoinCondition {
            left: FieldRef::Qualified("e".into(), "sip".into()),
            right: FieldRef::Qualified("other".into(), "sip".into()),
        }
    );
    assert!(j.emit_at.is_some());
}

#[test]
fn parse_join_reduce_top_and_label() {
    let input = r#"
rule r {
    events { e : win }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    join other reduce top(3, other.price) as winner on e.sip == other.sip
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    let j = &file.rules[0].joins[0];
    let rc = j.reduce.as_ref().expect("reduce");
    assert_eq!(
        rc.measure,
        ReduceMeasure::Top {
            n: 3,
            field: FieldRef::Qualified("other".into(), "price".into()),
        }
    );
    assert_eq!(rc.label.as_deref(), Some("winner"));
}

#[test]
fn parse_join_reduce_minrow_last() {
    let input = r#"
rule r {
    events { e : win }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    join a reduce minrow(a.price) on e.sip == a.sip
    join b reduce last(b.price) on e.sip == b.sip
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    let joins = &file.rules[0].joins;
    assert!(matches!(
        &joins[0].reduce.as_ref().unwrap().measure,
        ReduceMeasure::Minrow { .. }
    ));
    assert!(matches!(
        &joins[1].reduce.as_ref().unwrap().measure,
        ReduceMeasure::Last { .. }
    ));
}

// -----------------------------------------------------------------------
// yield clause: version without presets
// -----------------------------------------------------------------------

#[test]
fn parse_yield_version_without_presets() {
    let input = r#"
rule r {
    events { e : win }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out@v1 (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    let y = &file.rules[0].yield_clause;
    assert_eq!(y.version, Some(1));
    assert!(y.presets.is_empty());
}

// -----------------------------------------------------------------------
// limits block: bare tokens and quoted values
// -----------------------------------------------------------------------

#[test]
fn parse_limits_quoted_and_bare_values() {
    let input = r#"
rule r {
    events { e : win }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    limits {
        max_memory = "256MB";
        max_instances = 100;
        max_throttle = 500/min;
        on_exceed = throttle;
    }
}
"#;
    let file = parse_wfl(input).unwrap();
    let limits = file.rules[0].limits.as_ref().expect("limits");
    assert_eq!(limits.items.len(), 4);
    assert_eq!(limits.items[0].value, "256MB");
    assert_eq!(limits.items[1].value, "100");
    assert_eq!(limits.items[2].value, "500/min");
    assert_eq!(limits.items[3].value, "throttle");
}

#[test]
fn reject_empty_limits_block() {
    let input = r#"
rule r {
    events { e : win }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    limits { }
}
"#;
    assert!(parse_wfl(input).is_err());
}

// -----------------------------------------------------------------------
// top-level entry points (wfl_parser/mod.rs)
// -----------------------------------------------------------------------

#[test]
fn parse_empty_input() {
    let file = parse_wfl("").unwrap();
    assert!(file.rules.is_empty());
    assert!(file.uses.is_empty());
    assert!(file.patterns.is_empty());
    assert!(file.yield_presets.is_empty());
}

#[test]
fn parse_use_declaration() {
    let file = parse_wfl(r#"use "security.wfs""#).unwrap();
    assert_eq!(file.uses.len(), 1);
    assert_eq!(file.uses[0].path, "security.wfs");
}

#[test]
fn reject_use_without_string_path() {
    assert!(parse_wfl("use security.wfs").is_err());
}

#[test]
fn parse_interleaved_patterns_presets_rules_and_tests() {
    let input = r#"
yield preset base (y = "x")

pattern p(alias) {
    match<:5m> { on event { ${alias} | count >= 1; } } -> score(10.0)
}

rule r {
    events { e : win }
    p(e)
    entity(ip, e.sip)
    yield out : base (x = e.sip)
}

test t for r {
    input {
        row(e, sip = "1.2.3.4");
    }
    expect {
        hits == 1;
    }
}
"#;
    let file = parse_wfl(input).unwrap();
    assert_eq!(file.yield_presets.len(), 1);
    assert_eq!(file.patterns.len(), 1);
    assert_eq!(file.rules.len(), 1);
    assert_eq!(file.tests.len(), 1);
    assert!(file.rules[0].pattern_origin.is_some());
}

// -----------------------------------------------------------------------
// conv clause: dedup / where chains
// -----------------------------------------------------------------------

#[test]
fn parse_conv_dedup_where() {
    let input = r#"
rule r {
    events { e : win }
    match<sip:1h:fixed> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    conv { dedup(sip) | where(count >= 2) ; }
}
"#;
    let file = parse_wfl(input).unwrap();
    let conv = file.rules[0].conv.as_ref().expect("conv");
    assert_eq!(conv.chains[0].steps.len(), 2);
    assert!(matches!(conv.chains[0].steps[0], ConvStep::Dedup(_)));
    assert!(matches!(conv.chains[0].steps[1], ConvStep::Where(_)));
}
