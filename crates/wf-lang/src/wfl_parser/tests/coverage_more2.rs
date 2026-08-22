//! Third-wave parser coverage tests for wfl_parser/rule.rs: the pipeline-stage
//! score rejection, missing entity/yield cut paths, a full rule exercising
//! every optional block (meta / let / conv / limits), pattern-invocation
//! cut errors, and closing-brace recovery.

use crate::parse_wfl;

// ---------------------------------------------------------------------------
// rule_decl_with_patterns — cut-error branches
// ---------------------------------------------------------------------------

#[test]
fn reject_non_final_pipeline_stage_with_score() {
    // A non-final stage carrying `-> score(...)` is a Cut error (v1).
    let input = r#"
rule r {
    events { e : win }
    match<sip:5m> { on event { e | count >= 1; } } -> score(1.0)
    |> match<sip:5m> { on event { _in | count >= 1; } } -> score(1.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert!(
        parse_wfl(input).is_err(),
        "non-final stage `score` must be rejected"
    );
}

#[test]
fn reject_missing_entity_clause() {
    let input = r#"
rule r {
    events { e : win }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    yield out (x = e.sip)
}
"#;
    assert!(
        parse_wfl(input).is_err(),
        "missing entity clause must be rejected"
    );
}

#[test]
fn reject_missing_yield_clause() {
    let input = r#"
rule r {
    events { e : win }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
}
"#;
    assert!(
        parse_wfl(input).is_err(),
        "missing yield clause must be rejected"
    );
}

#[test]
fn reject_pattern_invocation_missing_open_paren() {
    // Known pattern name but no `(` → Cut error (not a backtrack to the
    // regular stage parser).
    let input = r#"
pattern burst(alias, key) {
    match<${key}:5m> {
        on event { ${alias} | count >= 1; }
    } -> score(50.0)
}

rule r {
    events { e : win }
    burst e, sip
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert!(
        parse_wfl(input).is_err(),
        "missing '(' after pattern name must be a cut error"
    );
}

#[test]
fn reject_missing_closing_brace() {
    let input = r#"
rule r {
    events { e : win }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
"#;
    assert!(parse_wfl(input).is_err(), "unclosed rule must be rejected");
}

// ---------------------------------------------------------------------------
// rule_decl_with_patterns — full optional-block rule
// ---------------------------------------------------------------------------

#[test]
fn parse_rule_with_all_optional_blocks() {
    let input = r#"
rule r {
    meta {
        description = "coverage"
        owner = "qa"
    }
    events { e : win }
    let bound = e.sip
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    conv { sort(-e.count) | top(5); }
    limits { max_instances = 10; }
}
"#;
    let file = parse_wfl(input).expect("parse should succeed");
    let rule = &file.rules[0];

    let meta = rule.meta.as_ref().expect("meta block");
    assert_eq!(meta.entries.len(), 2);
    assert_eq!(meta.entries[0].key, "description");

    assert_eq!(rule.lets.len(), 1);
    assert_eq!(rule.lets[0].name, "bound");

    let conv = rule.conv.as_ref().expect("conv block");
    assert_eq!(conv.chains.len(), 1);
    assert_eq!(conv.chains[0].steps.len(), 2);

    let limits = rule.limits.as_ref().expect("limits block");
    assert_eq!(limits.items.len(), 1);
    assert_eq!(limits.items[0].key, "max_instances");
}

#[test]
fn parse_rule_without_optional_blocks() {
    let input = r#"
rule r {
    events { e : win }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).expect("parse should succeed");
    let rule = &file.rules[0];
    assert!(rule.meta.is_none());
    assert!(rule.lets.is_empty());
    assert!(rule.conv.is_none());
    assert!(rule.limits.is_none());
}

// ---------------------------------------------------------------------------
// stage_clause — on-each with filter and score (final stage)
// ---------------------------------------------------------------------------

#[test]
fn parse_each_stage_with_filter_and_score() {
    let input = r#"
rule r {
    events { e : win }
    on each e where e.sip == "1.2.3.4" -> score(1.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).expect("parse should succeed");
    let rule = &file.rules[0];
    let each = rule.each_clause.as_ref().expect("each clause");
    assert_eq!(each.alias, "e");
    assert!(each.filter.is_some(), "each filter must be parsed");
    assert_eq!(rule.score.expr, crate::ast::Expr::Number(1.0));
}
