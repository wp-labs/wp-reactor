//! Fourth-wave parser coverage tests (coverage_r4) for wfl_parser/rule.rs and
//! wfl_parser/clauses.rs:
//!
//! - rule.rs: non-final pipeline stage `where` rejection, final stage missing
//!   `score` rejection, pattern invocation followed by `where` / `|>` cut
//!   errors.
//! - clauses.rs: yield presets with `<...>` parameters and reference arguments
//!   (including strings / parens / brackets / braces / line comments inside the
//!   angle body — `find_angle_close` internals), open/closed `within` bound
//!   markers, and unknown `reduce` measure rejection.

use crate::parse_wfl;

// ---------------------------------------------------------------------------
// rule.rs — pipeline stage cut errors
// ---------------------------------------------------------------------------

#[test]
fn reject_non_final_pipeline_stage_with_where() {
    // A non-final stage carrying `where <expr>` is a Cut error (v1: where is
    // supported on the final stage only).
    let input = r#"
rule r {
    events { e : win }
    match<sip:5m> { on event { e | count >= 1; } } where e.count > 1
    |> match<sip:5m> { on event { _in | count >= 1; } } -> score(1.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert!(
        parse_wfl(input).is_err(),
        "non-final stage `where` must be rejected"
    );
}

#[test]
fn reject_pipeline_final_stage_without_score() {
    let input = r#"
rule r {
    events { e : win }
    match<sip:5m> { on event { e | count >= 1; } }
    |> match<sip:5m> { on event { _in | count >= 1; } }
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert!(
        parse_wfl(input).is_err(),
        "pipeline final stage without `score` must be rejected"
    );
}

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
    where e.count > 1
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert!(
        parse_wfl(input).is_err(),
        "`where` after a pattern invocation must be a cut error"
    );
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
    assert!(
        parse_wfl(input).is_err(),
        "`|>` after a pattern invocation must be a cut error"
    );
}

// ---------------------------------------------------------------------------
// clauses.rs — yield presets with parameters / reference arguments
// ---------------------------------------------------------------------------

#[test]
fn parse_yield_preset_params_and_ref_args() {
    // The `<...>` angle bodies exercise find_angle_close: a string with
    // parens (skip_string), function-call parens, array brackets, object
    // braces, and a trailing line comment.
    let input = r#"
yield preset base <p = fmt("a(b)"), q = array [1], r = object { k = 1; }, s = 2 // note
> (x = p, y = q)
rule r {
    events { e : win }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out : base <e.sip> (extra = "ok")
}
"#;
    let file = parse_wfl(input).expect("preset with params + ref args should parse");
    assert_eq!(file.yield_presets.len(), 1);
    let preset = &file.yield_presets[0];
    assert_eq!(preset.params.len(), 4, "four preset parameters");
    assert_eq!(preset.params[0].name, "p");
    assert!(preset.params[0].default.is_some(), "param default");
    assert_eq!(preset.params[2].name, "r");

    let rule = &file.rules[0];
    assert_eq!(rule.yield_clause.presets.len(), 1);
    let preset_ref = &rule.yield_clause.presets[0];
    assert_eq!(preset_ref.name, "base");
    assert_eq!(preset_ref.args.len(), 1, "positional ref argument");
}

#[test]
fn parse_empty_preset_ref_args() {
    let input = r#"
yield preset base (x = 1)
rule r {
    events { e : win }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out : base <> (y = "ok")
}
"#;
    let file = parse_wfl(input).expect("empty preset ref args should parse");
    let rule = &file.rules[0];
    let preset_ref = &rule.yield_clause.presets[0];
    assert!(
        preset_ref.args.is_empty(),
        "`<>` yields no positional arguments"
    );
}

#[test]
fn parse_empty_preset_params() {
    let input = r#"
yield preset base < > (x = 1)
rule r {
    events { e : win }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out : base (y = "ok")
}
"#;
    let file = parse_wfl(input).expect("empty preset params should parse");
    let preset = &file.yield_presets[0];
    assert!(preset.params.is_empty(), "`< >` yields no parameters");
}

// ---------------------------------------------------------------------------
// clauses.rs — within bound markers / reduce measures
// ---------------------------------------------------------------------------

#[test]
fn parse_within_open_closed_bound_markers() {
    let input = r#"
rule r {
    events { a : win }
    on each a -> score(1.0)
    join bid_events within [<10s, <=20s] on a.id == bid_events.auction
    entity(digit, a.id)
    yield out (n = a.id)
}
"#;
    let file = parse_wfl(input).expect("open/closed within bounds should parse");
    let join = &file.rules[0].joins[0];
    let wspec = join.within.as_ref().expect("within spec");
    assert!(wspec.lo.open, "`<10s` is an open lower bound");
    assert!(!wspec.hi.open, "`<=20s` is a closed upper bound");
}

#[test]
fn reject_unknown_reduce_measure() {
    let input = r#"
rule r {
    events { a : win }
    on each a -> score(1.0)
    join bid_events reduce bogus(price) on a.id == bid_events.auction
    entity(digit, a.id)
    yield out (n = a.id)
}
"#;
    assert!(
        parse_wfl(input).is_err(),
        "unknown reduce measure must be a cut error"
    );
}
