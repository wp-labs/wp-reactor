//! Fourth-wave compiler coverage tests (coverage_r4):
//!
//! - `compile_rule` error paths (on-each + pipeline combinations) exercised
//!   through `compile_wfl_after_semantic_checks` (the checker rejects these
//!   first, so the compile-side errors are only reachable bypassing check_wfl).
//! - `compile_limits` defensive arms (unknown key / bare byte size / bad rate
//!   unit) via the same direct-call path.
//! - `compile_match` key-mapping `None` arm, join-then-key resolution
//!   (`resolve_join_key`), `SeqSkip::ToNext`, pipeline qualified-key output
//!   naming, and `rewrite_expr_label_refs` negation recursion.
//! - `explain` formatting branches: branch guards, asof-without-within, inner
//!   joins, reduce variant formatting, negative within bounds, dedup conv ops,
//!   plain-field lineage and system-variable lineage.

use std::time::Duration;

use crate::ast::{Expr, FieldRef};
use crate::compiler::{compile_wfl, compile_wfl_after_semantic_checks};
use crate::explain::explain_rules;
use crate::plan::{JoinKeyPlan, SeqSkipPlan};
use crate::schema::{BaseType, FieldType, WindowSchema};
use crate::wfl_parser::parse_wfl;

use super::*;

// ---------------------------------------------------------------------------
// Local schemas
// ---------------------------------------------------------------------------

fn auction_events_window() -> WindowSchema {
    make_window(
        "auction_events",
        vec!["auction_stream"],
        vec![
            ("id", bt(BaseType::Digit)),
            ("category", bt(BaseType::Chars)),
            ("dateTime", bt(BaseType::Time)),
            ("expires", bt(BaseType::Time)),
        ],
    )
}

fn bid_events_window() -> WindowSchema {
    make_window(
        "bid_events",
        vec!["bid_stream"],
        vec![
            ("auction", bt(BaseType::Digit)),
            ("bidder", bt(BaseType::Chars)),
            ("price", bt(BaseType::Digit)),
            ("dateTime", bt(BaseType::Time)),
        ],
    )
}

fn rich_window() -> WindowSchema {
    make_window(
        "rich",
        vec!["rich_stream"],
        vec![
            ("sip", bt(BaseType::Ip)),
            ("dip", bt(BaseType::Ip)),
            ("action", bt(BaseType::Chars)),
            ("user", bt(BaseType::Chars)),
            ("count", bt(BaseType::Digit)),
            ("roles_obj", FieldType::Object),
            ("event_time", bt(BaseType::Time)),
        ],
    )
}

fn rich_out_window() -> WindowSchema {
    make_output_window(
        "out",
        vec![
            ("n0", bt(BaseType::Digit)),
            ("f0", bt(BaseType::Float)),
            ("s0", bt(BaseType::Chars)),
            ("b0", bt(BaseType::Bool)),
            ("t0", bt(BaseType::Time)),
            ("arr0", FieldType::Array(BaseType::Chars)),
        ],
    )
}

// ---------------------------------------------------------------------------
// compile_rule / compile_limits error paths (bypassing check_wfl)
// ---------------------------------------------------------------------------

#[test]
fn compile_rule_rejects_on_each_with_pipeline() {
    // `on each` + pipeline stages: the compile-side error only fires when
    // semantic checks are bypassed (check_wfl rejects this combination first).
    let src = r#"
rule r {
    events { e : rich }
    match<:5m> { on event { e | count >= 1; } }
    |> on each _in -> score(1.0)
    entity(ip, e.sip)
    yield out (s0 = "x")
}
"#;
    let file = parse_wfl(src).expect("parse should succeed");
    let err = compile_wfl_after_semantic_checks(&file, &[rich_window(), rich_out_window()])
        .expect_err("`on each` + pipeline must fail to compile");
    assert!(
        err.to_string()
            .contains("`on each` is not supported together with pipeline stages yet"),
        "unexpected error: {err}"
    );
}

#[test]
fn compile_rule_rejects_on_each_pipeline_stage() {
    let src = r#"
rule r {
    events { e : rich }
    on each e
    |> match<:5m> { on event { _in | count >= 1; } } -> score(1.0)
    entity(ip, e.sip)
    yield out (s0 = "x")
}
"#;
    let file = parse_wfl(src).expect("parse should succeed");
    let err = compile_wfl_after_semantic_checks(&file, &[rich_window(), rich_out_window()])
        .expect_err("`on each` pipeline stage must fail to compile");
    assert!(
        err.to_string()
            .contains("`on each` pipeline stages are not supported yet"),
        "unexpected error: {err}"
    );
}

#[test]
fn compile_limits_defensive_arms_via_direct_call() {
    // Unknown limits key → the `_ => {}` arm; bare byte size → the plain-parse
    // arm; invalid rate unit → parse_rate_spec's `_ => None`.
    let src = r#"
rule r {
    events { e : rich }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (s0 = "x")
    limits { bogus = 1; }
}
rule r2 {
    events { e : rich }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (s0 = "x")
    limits { max_memory = 1024; }
}
rule r3 {
    events { e : rich }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (s0 = "x")
    limits { max_throttle = "5/fortnight"; }
}
"#;
    let file = parse_wfl(src).expect("parse should succeed");
    let plans = compile_wfl_after_semantic_checks(&file, &[rich_window(), rich_out_window()])
        .expect("direct compile should succeed");
    let by_name = |n: &str| plans.iter().find(|p| p.name == n).expect("plan");

    let lim = by_name("r").limits_plan.as_ref().expect("limits");
    assert_eq!(lim.max_memory_bytes, None);
    assert_eq!(lim.max_instances, None);
    assert_eq!(lim.max_throttle, None);

    let lim2 = by_name("r2").limits_plan.as_ref().expect("limits");
    assert_eq!(lim2.max_memory_bytes, Some(1024), "bare byte size parses");

    let lim3 = by_name("r3").limits_plan.as_ref().expect("limits");
    assert_eq!(lim3.max_throttle, None, "unknown rate unit → None");
}

// ---------------------------------------------------------------------------
// compile_match — key mapping / join-then-key / seq skip
// ---------------------------------------------------------------------------

#[test]
fn key_mapping_unqualified_source_skipped() {
    // A key mapping whose source is a bare alias falls into the `None` arm of
    // the compiler's KeyMapPlan filter (the checker rejects it, so this runs
    // via the direct-call path).
    let src = r#"
rule r {
    events { e : rich }
    match<:5m> {
        key { uid = e; }
        on event { e | count >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (s0 = "x")
}
"#;
    let file = parse_wfl(src).expect("parse should succeed");
    let plans = compile_wfl_after_semantic_checks(&file, &[rich_window(), rich_out_window()])
        .expect("direct compile");
    let plan = &plans[0];
    let key_map = plan.match_plan.key_map.as_ref().expect("key map");
    assert!(
        key_map.is_empty(),
        "unqualified source produces no KeyMapPlan"
    );
}

#[test]
fn join_then_key_resolves_join_key_plan() {
    // `auction` is absent from the driver but present on the snapshot join
    // target → the compiler routes the key through the join (Path A).
    let src = r#"
rule r {
    events { a : auction_events }
    match<auction:10m> { on event { a | count >= 1; } } -> score(1.0)
    join bid_events snapshot on a.id == bid_events.auction
    entity(digit, a.id)
    yield out (n0 = a.id)
}
"#;
    let plans = compile_with(
        src,
        &[
            auction_events_window(),
            bid_events_window(),
            rich_out_window(),
        ],
    );
    let plan = &plans[0];
    let kj: &JoinKeyPlan = plan
        .match_plan
        .key_join
        .as_ref()
        .expect("join-then-key must produce a JoinKeyPlan");
    assert_eq!(kj.join_idx, 0);
    assert_eq!(kj.right_window, "bid_events");
    assert_eq!(kj.key_name, "auction");
    assert_eq!(kj.left_field, FieldRef::Qualified("a".into(), "id".into()));
    assert_eq!(kj.right_field, "auction");
}

#[test]
fn seq_skip_to_next_compiles_to_plan() {
    let src = r#"
rule r {
    events { a : rich  b : rich }
    match<sip:30m> {
        on event seq skip = to_next {
            has a;
            has b;
        }
    } -> score(70.0)
    entity(ip, a.sip)
    yield out (s0 = "x")
}
"#;
    let plans = compile_with(src, &[rich_window(), rich_out_window()]);
    let seq = plans[0].match_plan.seq.as_ref().expect("seq plan");
    assert_eq!(seq.skip, SeqSkipPlan::ToNext);
}

#[test]
fn pipeline_qualified_key_uses_field_name_for_stage_output() {
    // `e.sip` as a stage key exercises `key_output_name`'s qualified arm and
    // the auto stage yield / pipeline entity naming.
    let src = r#"
rule pipe {
    events { e : rich }
    match<e.sip:5m> { on event { e | count >= 1; } }
    |> match<:5m> { on event { _in | count >= 1; } } -> score(1.0)
    entity(ip, _in.sip)
    yield out (s0 = _in.sip)
}
"#;
    let plans = compile_with(src, &[rich_window(), rich_out_window()]);
    assert_eq!(plans.len(), 2);
    let stage1 = &plans[0];
    assert_eq!(stage1.name, "__wf_pipe_pipe_s1");
    let names: Vec<&str> = stage1
        .yield_plan
        .fields
        .iter()
        .map(|f| f.name.as_str())
        .collect();
    assert!(
        names.contains(&"sip"),
        "qualified key `e.sip` must be emitted as `sip`, got {names:?}"
    );
    assert_eq!(
        stage1.entity_plan.entity_id_expr,
        Expr::Field(FieldRef::Simple("sip".into())),
        "pipeline entity reads the key output field"
    );
}

// ---------------------------------------------------------------------------
// rewrite_expr_label_refs — negation recursion
// ---------------------------------------------------------------------------

#[test]
fn label_ref_rewritten_inside_negation() {
    let src = r#"
rule r {
    events { a : auction_events }
    on each a -> score(1.0)
    join bid_events reduce maxrow(price) on a.id == bid_events.auction as winner
    entity(digit, a.id)
    yield out (n0 = -winner.price)
}
"#;
    let plans = compile_with(
        src,
        &[
            auction_events_window(),
            bid_events_window(),
            rich_out_window(),
        ],
    );
    let field = plans[0]
        .yield_plan
        .fields
        .iter()
        .find(|f| f.name == "n0")
        .expect("n0 field");
    assert!(
        matches!(
            &field.value,
            Expr::Neg(inner) if matches!(
                inner.as_ref(),
                Expr::Field(FieldRef::Path { alias, segments }) if alias == "winner"
                    && segments.len() == 1
            )
        ),
        "label ref inside Neg must be rewritten to a Path: {:?}",
        field.value
    );
}

// ---------------------------------------------------------------------------
// explain — formatting branches (guard / asof / inner / reduce / conv / within)
// ---------------------------------------------------------------------------

#[test]
fn explain_formats_guard_negation_and_reduce_variants() {
    let src = r#"
rule r {
    events { e : rich }
    match<sip:5m> {
        on event { e && e.count > 1 | count >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (s0 = "x")
}
rule r2 {
    events { a : auction_events }
    on each a -> score(1.0)
    join bid_events snapshot reduce maxrow(price) on a.id == bid_events.auction
    join bid_events snapshot reduce minrow(price) on a.id == bid_events.auction
    join bid_events snapshot reduce last(price) on a.id == bid_events.auction
    join bid_events snapshot reduce top(3, price) on a.id == bid_events.auction
    entity(digit, a.id)
    yield out (n0 = a.id)
}
rule r3 {
    events { a : auction_events }
    on each a -> score(1.0)
    join bid_events asof on a.id == bid_events.auction
    join bid_events on a.id == bid_events.auction
    join bid_events within 10s on a.id == bid_events.auction
    entity(digit, a.id)
    yield out (n0 = a.id)
}
"#;
    let schemas = [
        rich_window(),
        rich_out_window(),
        auction_events_window(),
        bid_events_window(),
    ];
    let file = parse_wfl(src).expect("parse should succeed");
    let plans = compile_wfl(&file, &schemas).expect("compile");
    let expls = explain_rules(&plans, &schemas);

    // Guard formatting.
    let steps = &expls[0].match_expl.event_steps;
    assert!(
        steps.iter().any(|s| s.contains("&&")),
        "guard must be formatted, got: {steps:?}"
    );

    // Reduce variants (maxrow/minrow/last/top, all label-less).
    let joined = expls[1].joins.join("\n");
    assert!(joined.contains("maxrow(price)"), "got: {joined}");
    assert!(joined.contains("minrow(price)"), "got: {joined}");
    assert!(joined.contains("last(price)"), "got: {joined}");
    assert!(joined.contains("top(3, price)"), "got: {joined}");

    // asof without within / inner mode / duration-within negative bound.
    let joined3 = expls[2].joins.join("\n");
    assert!(
        joined3.contains("join bid_events asof"),
        "asof without within: got: {joined3}"
    );
    assert!(
        joined3.contains("join bid_events inner"),
        "default inner mode: got: {joined3}"
    );
    assert!(
        joined3.contains("within [-10s , 0s]") || joined3.contains("within [-10s, 0s]"),
        "duration within sugar formats a negative bound: got: {joined3}"
    );
}

#[test]
fn explain_conv_dedup_and_lineage_shapes() {
    let src = r#"
rule r {
    events { e : rich }
    match<sip:1h:fixed> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (s0 = sip, f0 = @score)
    conv { sort(-e.count) | dedup(e.sip) | top(5); }
}
"#;
    let schemas = [rich_window(), rich_out_window()];
    let file = parse_wfl(src).expect("parse should succeed");
    let plans = compile_wfl(&file, &schemas).expect("compile");
    let expl = &explain_rules(&plans, &schemas)[0];

    let conv = expl.conv.as_ref().expect("conv");
    assert!(
        conv[0].contains("sort(-e.count) | dedup(e.sip) | top(5)"),
        "dedup conv op must be formatted: got: {}",
        conv[0]
    );

    // Plain (unaliased) field lineage + system-var lineage.
    let mut by_name: std::collections::HashMap<&str, &str> = std::collections::HashMap::new();
    for (name, origin) in &expl.lineage {
        by_name.insert(name.as_str(), origin.as_str());
    }
    assert_eq!(
        by_name.get("s0").copied(),
        Some("field `sip`"),
        "plain field lineage: {:?}",
        by_name
    );
    assert_eq!(
        by_name.get("f0").copied(),
        Some("@score"),
        "system var lineage: {:?}",
        by_name
    );
}

// ---------------------------------------------------------------------------
// stats compile — session window plan (regression guard)
// ---------------------------------------------------------------------------

#[test]
fn stats_session_window_plan_roundtrip() {
    let src = r#"
rule stats_r {
    events { a : rich }
    stats<30s:session> tier a.count [<100, <1000] {
        a | count as total;
    }
    entity(digit, 1)
    yield out (s0 = "x")
}
"#;
    let plans = compile_with(src, &[rich_window(), rich_out_window()]);
    let stats = plans[0].stats_plan.as_ref().expect("stats plan");
    assert_eq!(
        stats.window_spec,
        crate::plan::WindowSpec::Session(Duration::from_secs(30))
    );
}
