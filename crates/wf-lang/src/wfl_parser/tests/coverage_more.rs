//! Second-wave parser coverage tests for wfl_parser/rule.rs: pattern
//! invocation with trailing `where`/`|>`, non-final pipeline stages carrying
//! `where`, each/pipeline stage shapes, meta-block edge cases and the
//! non-backtracking stats error path.

use crate::parse_wfl;

// ---------------------------------------------------------------------------
// pattern invocation constraints (rule.rs)
// ---------------------------------------------------------------------------

#[test]
fn reject_pattern_invocation_with_where() {
    let input = r#"
pattern burst(alias, key) {
    match<${key}:5m> {
        on event { ${alias} | count >= 1; }
    } -> score(50.0)
}

rule r {
    events { e : win }
    burst(e, sip)
    where e.sip == "1.2.3.4"
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert!(parse_wfl(input).is_err(), "`where` after a pattern invocation must be rejected");
}

#[test]
fn reject_pattern_invocation_with_pipeline() {
    let input = r#"
pattern burst(alias, key) {
    match<${key}:5m> {
        on event { ${alias} | count >= 1; }
    } -> score(50.0)
}

rule r {
    events { e : win }
    burst(e, sip)
    |> match<sip:5m> { on event { _in | count >= 1; } } -> score(1.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert!(parse_wfl(input).is_err(), "`|>` after a pattern invocation must be rejected");
}

#[test]
fn reject_non_final_pipeline_stage_with_where() {
    // `where` is only allowed on the final stage of a `|>` chain.
    let input = r#"
rule r {
    events { e : win }
    match<sip:5m> { on event { e | count >= 1; } where e.sip == "1.2.3.4" }
    |> match<sip:5m> { on event { _in | count >= 1; } } -> score(1.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert!(parse_wfl(input).is_err(), "non-final stage `where` must be rejected");
}

// ---------------------------------------------------------------------------
// each/pipeline stage shapes (rule.rs loop)
// ---------------------------------------------------------------------------

#[test]
fn parse_each_final_stage_after_match_stage() {
    // `on each` as the final stage of a chain: the match stage goes into
    // pipeline_stages and the each clause lands on the rule itself.
    let input = r#"
rule r {
    events { e : win }
    match<sip:5m> { on event { e | count >= 1; } }
    |> on each e -> score(1.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).expect("parse should succeed");
    let rule = &file.rules[0];
    assert!(rule.each_clause.is_some(), "final stage should carry the each clause");
    assert_eq!(rule.pipeline_stages.len(), 1);
    assert!(rule.pipeline_stages[0].each_clause.is_none());
}

#[test]
fn parse_stage_each_before_pipeline() {
    // A non-final `on each` stage is pushed into pipeline_stages.
    let input = r#"
rule r {
    events { e : win }
    on each e
    |> match<sip:5m> { on event { _in | count >= 1; } } -> score(1.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).expect("parse should succeed");
    let rule = &file.rules[0];
    assert!(rule.each_clause.is_none());
    assert_eq!(rule.pipeline_stages.len(), 1);
    assert!(
        rule.pipeline_stages[0].each_clause.is_some(),
        "the non-final each stage should be preserved in pipeline_stages"
    );
}

// ---------------------------------------------------------------------------
// meta block edge cases (rule.rs meta_block)
// ---------------------------------------------------------------------------

#[test]
fn parse_empty_meta_block() {
    let input = r#"
rule r {
    meta { }
    events { e : win }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).expect("parse should succeed");
    let meta = file.rules[0].meta.as_ref().expect("meta block");
    assert!(meta.entries.is_empty());
}

#[test]
fn reject_meta_block_missing_value() {
    // `meta { description }` — key without `=`/value is a cut error.
    let input = r#"
rule r {
    meta { description }
    events { e : win }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert!(parse_wfl(input).is_err(), "meta key without a value must be rejected");
}

// ---------------------------------------------------------------------------
// stats rule parse error propagation (rule.rs outer match)
// ---------------------------------------------------------------------------

#[test]
fn reject_malformed_stats_rule() {
    // `stats<...>` cut-parses; a malformed measure must fail the whole rule
    // via the non-backtracking error path rather than backtracking into the
    // stage parser.
    let input = r#"
rule r {
    events { e : win }
    stats<10s:fixed> {
        e | nonsense_measure;
    }
    entity(digit, 1)
    yield out (y = "x")
}
"#;
    assert!(parse_wfl(input).is_err(), "malformed stats measure must be rejected");
}
