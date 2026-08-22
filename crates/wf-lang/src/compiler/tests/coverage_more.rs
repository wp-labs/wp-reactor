//! Second-wave compiler coverage tests: compile_rule pipeline/each rejection
//! branches, compile_match session/any window modes, key-mapping key dedup,
//! resolve_join_key None paths, entity type normalization and pattern-origin
//! plan carry-over that the first-wave coverage_extra.rs does not reach.

use std::time::Duration;

use crate::ast::{Expr, FieldRef, MatchMode};
use crate::compiler::{collect_rule_bind_tracking, compile_wfl};
use crate::plan::{PatternOriginPlan, WindowSpec};
use crate::wfl_parser::parse_wfl;

use super::*;

/// Driver window for join-then-key tests.
fn auction_events_window() -> WindowSchema {
    make_window(
        "auction_events",
        vec!["auction_stream"],
        vec![
            ("id", bt(BaseType::Digit)),
            ("category", bt(BaseType::Chars)),
            ("event_time", bt(BaseType::Time)),
        ],
    )
}

/// Snapshot-join target carrying `auction` / `bidder` fields.
fn bid_events_window() -> WindowSchema {
    make_window(
        "bid_events",
        vec!["bid_stream"],
        vec![
            ("auction", bt(BaseType::Digit)),
            ("bidder", bt(BaseType::Chars)),
            ("event_time", bt(BaseType::Time)),
        ],
    )
}

// ---------------------------------------------------------------------------
// compile_rule rejection branches
// ---------------------------------------------------------------------------

#[test]
fn compile_rejects_each_combined_with_pipeline_stages() {
    // `on each` as the final stage of a `|>` chain parses into
    // rule.each_clause = Some + non-empty pipeline_stages.
    let file = parse_wfl(
        r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } }
    |> on each e -> score(1.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#,
    )
    .expect("parse should succeed");
    let err = compile_wfl(&file, &[auth_events_window(), output_window()])
        .expect_err("`on each` together with pipeline stages must be rejected");
    assert!(
        err.to_string().contains("not supported together with pipeline stages"),
        "unexpected error: {err}"
    );
}

#[test]
fn compile_rejects_pipeline_stage_with_each_clause() {
    // A non-final `on each` stage (inside pipeline_stages) is rejected.
    let file = parse_wfl(
        r#"
rule r {
    events { e : auth_events }
    on each e
    |> match<sip:5m> { on event { _in | count >= 1; } } -> score(1.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#,
    )
    .expect("parse should succeed");
    let err = compile_wfl(&file, &[auth_events_window(), output_window()])
        .expect_err("`on each` pipeline stages must be rejected");
    assert!(
        err.to_string().contains("`on each` pipeline stages are not supported yet"),
        "unexpected error: {err}"
    );
}

// ---------------------------------------------------------------------------
// compile_match window modes / match modes
// ---------------------------------------------------------------------------

#[test]
fn compile_session_window_mode() {
    let plans = compile_with(
        r#"
rule r {
    events { e : auth_events }
    match<sip:session(10m)> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#,
        &[auth_events_window(), output_window()],
    );
    assert_eq!(
        plans[0].match_plan.window_spec,
        WindowSpec::Session(Duration::from_secs(600))
    );
}

#[test]
fn compile_any_match_mode() {
    let plans = compile_with(
        r#"
rule r {
    events { a : auth_events  b : auth_events }
    match<sip:5m> { on event any { a | count >= 1;  b | count >= 1; } } -> score(50.0)
    entity(ip, a.sip)
    yield out (x = a.sip)
}
"#,
        &[auth_events_window(), output_window()],
    );
    assert_eq!(plans[0].match_plan.match_mode, MatchMode::Any);
}

#[test]
fn compile_accu_flag_carried_into_plan() {
    let plans = compile_with(
        r#"
rule r {
    events { s : auth_events }
    match<:5m> { on event<accu> { s | count >= 2; } } -> score(50.0)
    entity(ip, s.sip)
    yield out (x = s.sip)
}
"#,
        &[auth_events_window(), output_window()],
    );
    assert!(plans[0].match_plan.accu, "accu flag must be carried into the plan");
}

// ---------------------------------------------------------------------------
// compile_match key mapping
// ---------------------------------------------------------------------------

#[test]
fn compile_key_mapping_dedups_logical_keys() {
    let plans = compile_with(
        r#"
rule r {
    events { a : auth_events  b : fw_events }
    match<:5m> {
        key { user_id = a.sip;  user_id = b.sip; }
        on event { a | count >= 1; }
    } -> score(50.0)
    entity(ip, a.sip)
    yield out (x = a.sip)
}
"#,
        &[auth_events_window(), fw_events_window(), output_window()],
    );
    // Keys are the deduplicated logical names; the key map keeps both sources.
    assert_eq!(plans[0].match_plan.keys, vec![FieldRef::Simple("user_id".into())]);
    let key_map = plans[0].match_plan.key_map.as_ref().expect("key map");
    assert_eq!(key_map.len(), 2);
    assert_eq!(key_map[0].logical_name, "user_id");
    assert_eq!(key_map[0].source_alias, "a");
    assert_eq!(key_map[1].source_alias, "b");
}

// ---------------------------------------------------------------------------
// resolve_join_key None paths
// ---------------------------------------------------------------------------

#[test]
fn compile_key_join_none_for_key_mapping_and_multi_key_rules() {
    // Key mapping present → resolve_join_key returns None.
    let plans = compile_with(
        r#"
rule km {
    events { a : auction_events }
    match<:5m> {
        key { kid = a.id; }
        on event { a | count >= 1; }
    } -> score(1.0)
    join bid_events snapshot on a.id == bid_events.auction
    entity(digit, a.id)
    yield out (n = a.id)
}
rule mk {
    events { a : auction_events }
    match<id, category:5m> { on event { a | count >= 1; } } -> score(1.0)
    join bid_events snapshot on a.id == bid_events.auction
    entity(digit, a.id)
    yield out (n = a.id)
}
"#,
        &[auction_events_window(), bid_events_window(), output_window()],
    );
    assert!(
        plans.iter().all(|p| p.match_plan.key_join.is_none()),
        "neither rule should resolve a join key: {:?}",
        plans.iter().map(|p| &p.name).collect::<Vec<_>>()
    );
}

// ---------------------------------------------------------------------------
// compile_entity / compile_regular_rule
// ---------------------------------------------------------------------------

#[test]
fn compile_entity_type_lowercased() {
    let plans = compile_with(
        r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity("IP Address", e.sip)
    yield out (x = e.sip)
}
"#,
        &[auth_events_window(), output_window()],
    );
    assert_eq!(plans[0].entity_plan.entity_type, "ip address");
}

#[test]
fn compile_pattern_origin_carried_into_plan() {
    let schemas = [auth_events_window(), output_window()];
    let plans = compile_with(
        r#"
pattern burst(alias, key, win, threshold) {
    match<${key}:${win}> {
        on event { ${alias} | count >= ${threshold}; }
    } -> score(50.0)
}

rule r {
    events { e : auth_events }
    burst(e, sip, 5m, 3)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#,
        &schemas,
    );
    assert_eq!(
        plans[0].pattern_origin,
        Some(PatternOriginPlan {
            pattern_name: "burst".to_string(),
            args: vec!["e".to_string(), "sip".to_string(), "5m".to_string(), "3".to_string()],
        })
    );
}

// ---------------------------------------------------------------------------
// needs-field-history / bind tracking helpers
// ---------------------------------------------------------------------------

#[test]
fn bind_tracking_stat_count_inner_selector_forms() {
    // `stat.count(window_event(x))` tracks the alias; `stat.count(match_event(x))`
    // inside a score expression must not crash and keeps plain-field tracking
    // for other references.
    let score_expr = Expr::FuncCall {
        qualifier: Some("stat".into()),
        name: "count".into(),
        args: vec![Expr::FuncCall {
            qualifier: None,
            name: "match_event".into(),
            args: vec![Expr::Field(FieldRef::Simple("lbl".into()))],
        }],
    };
    let entity_expr = Expr::Number(1.0);
    let yield_fields = vec![super::YieldField {
        name: "n".into(),
        value: Expr::Field(FieldRef::Simple("lbl".into())),
    }];
    let tracking = collect_rule_bind_tracking(&score_expr, &entity_expr, &yield_fields);
    // match_event selector args are not window aliases → no alias tracked.
    assert!(tracking.aliases.is_empty());
    assert!(tracking.plain_fields.contains("lbl"));
}
