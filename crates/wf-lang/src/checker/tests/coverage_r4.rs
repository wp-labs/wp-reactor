//! Fourth-wave checker coverage tests (coverage_r4):
//!
//! - `scope.rs` field-ref resolution edges: bracketed refs, reduce-label path
//!   / bare access, conflicting cross-source field types, qualified missing
//!   field.
//! - `joins.rs` remaining branches: right-side missing field, within bound
//!   kind-mix / span-over-window, emit-at constraints (requires within, requires
//!   absolute upper bound, must match the upper-bound field), reduce field
//!   qualification and structured-measure rejection, and driver-alias-only
//!   recursion shapes for within / emit-at expressions.
//! - `rules/mod.rs`: pipeline stage `on each` / seq / any rejection, stats
//!   `where`-expression shape recursion (`collect_expr_field_refs`), on-each
//!   neg / allowed-func recursion, accu single-branch constraint, chain `not`
//!   field aggregation rejection, `skip = to_next` warning.
//! - `check_funcs.rs` builtin argument validation: valid usages plus wrong
//!   arity / wrong type errors across the scalar, string, array and L3
//!   families.

use std::time::Duration;

use super::*;
use crate::check_wfl;
use crate::checker::Severity;
use crate::schema::{BaseType, FieldDef, FieldType, WindowSchema};
use crate::wfl_parser::parse_wfl;

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

/// Static side-input window (provider projection): no stream, no time field,
/// `over` = 0.
fn static_win_window() -> WindowSchema {
    WindowSchema {
        name: "static_win".to_string(),
        streams: vec![],
        time_field: None,
        over: Duration::ZERO,
        fields: vec![
            FieldDef {
                name: "code".to_string(),
                field_type: bt(BaseType::Chars),
            },
            FieldDef {
                name: "val".to_string(),
                field_type: bt(BaseType::Digit),
            },
        ],
    }
}

/// Driver window with structured columns (object / array) for builtin-type and
/// reduce-measure checks.
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
            ("dport", bt(BaseType::Digit)),
            ("roles_obj", FieldType::Object),
            ("tags", FieldType::Array(BaseType::Chars)),
            ("event_time", bt(BaseType::Time)),
        ],
    )
}

/// Driver window whose `shared` column conflicts with another source.
fn shared_chars_window() -> WindowSchema {
    make_window(
        "shared_chars",
        vec!["sc_stream"],
        vec![
            ("shared", bt(BaseType::Chars)),
            ("event_time", bt(BaseType::Time)),
        ],
    )
}

fn shared_digit_window() -> WindowSchema {
    make_window(
        "shared_digit",
        vec!["sd_stream"],
        vec![
            ("shared", bt(BaseType::Digit)),
            ("event_time", bt(BaseType::Time)),
        ],
    )
}

/// Window whose only structured column is a join-reduce target.
fn struct_join_window() -> WindowSchema {
    make_window(
        "struct_join",
        vec!["sj_stream"],
        vec![
            ("id", bt(BaseType::Digit)),
            ("payload", FieldType::Object),
            ("event_time", bt(BaseType::Time)),
        ],
    )
}

/// Output window covering every base type plus array/object shapes. Field names
/// mirror the test usages (`b*` bool, `s*` chars, `n*` digit, `t*` time,
/// `f*` float, `h0` hex, `arr*` arrays, `obj`, `arrn`).
fn rich_out_window() -> WindowSchema {
    let mut fields: Vec<FieldDef> = Vec::new();
    for i in 0..=10 {
        fields.push(FieldDef {
            name: format!("b{i}"),
            field_type: bt(BaseType::Bool),
        });
    }
    for i in 0..=18 {
        fields.push(FieldDef {
            name: format!("s{i}"),
            field_type: bt(BaseType::Chars),
        });
    }
    for i in 0..=3 {
        fields.push(FieldDef {
            name: format!("n{i}"),
            field_type: bt(BaseType::Digit),
        });
    }
    for i in 0..=3 {
        fields.push(FieldDef {
            name: format!("t{i}"),
            field_type: bt(BaseType::Time),
        });
    }
    for i in 0..=14 {
        fields.push(FieldDef {
            name: format!("f{i}"),
            field_type: bt(BaseType::Float),
        });
    }
    for i in 0..=4 {
        fields.push(FieldDef {
            name: format!("arr{i}"),
            field_type: FieldType::Array(BaseType::Chars),
        });
    }
    fields.push(FieldDef {
        name: "h0".to_string(),
        field_type: bt(BaseType::Hex),
    });
    fields.push(FieldDef {
        name: "arrn".to_string(),
        field_type: FieldType::Array(BaseType::Digit),
    });
    fields.push(FieldDef {
        name: "obj".to_string(),
        field_type: FieldType::Object,
    });
    WindowSchema {
        name: "out".to_string(),
        streams: vec![],
        time_field: None,
        over: Duration::from_secs(3600),
        fields,
    }
}

// ===========================================================================
// scope.rs — field-ref resolution edges
// ===========================================================================

#[test]
fn bracket_field_ref_resolves() {
    // `e["sip"]` (FieldRef::Bracketed) resolves through the qualified path.
    let input = r#"
rule r {
    events { e : rich }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e["sip"])
    yield out (s0 = e["action"])
}
"#;
    assert_no_errors(input, &[rich_window(), rich_out_window()]);
}

#[test]
fn reduce_label_path_and_bare_refs_resolve() {
    // `winner.bidder` (Path into a reduce label) and the bare `winner` label
    // both resolve to a set-level object value with no static scalar type.
    let input = r#"
rule r {
    events { a : auction_events }
    on each a -> score(1.0)
    join bid_events reduce maxrow(price) on a.id == bid_events.auction as winner
    entity(digit, a.id)
    yield out (s0 = winner.bidder, obj = winner)
}
"#;
    assert_no_errors(
        input,
        &[
            auction_events_window(),
            bid_events_window(),
            rich_out_window(),
        ],
    );
}

#[test]
fn bare_field_conflicting_types_across_sources_rejected() {
    let input = r#"
rule r {
    events { a : shared_chars  b : shared_digit }
    match<:5m> { on event { a | count >= 1; } } -> score(50.0)
    entity(ip, a.shared)
    yield out (s0 = shared)
}
"#;
    assert_has_error(
        input,
        &[
            shared_chars_window(),
            shared_digit_window(),
            rich_out_window(),
        ],
        "conflicting types across event sources",
    );
}

#[test]
fn qualified_missing_field_rejected() {
    let input = r#"
rule r {
    events { e : rich }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (s0 = e.nope)
}
"#;
    assert_has_error(
        input,
        &[rich_window(), rich_out_window()],
        "field `nope` not found in window `rich`",
    );
}

// ===========================================================================
// joins.rs — static side-input window joins
// ===========================================================================

#[test]
fn static_window_snapshot_join_ok_and_asof_rejected() {
    // Snapshot (and default inner) joins against a provider/static side-input
    // window are the supported shape...
    let ok = r#"
rule r {
    events { a : auction_events }
    on each a -> score(1.0)
    join static_win snapshot on a.id == static_win.val
    entity(digit, a.id)
    yield out (n0 = a.id)
}
"#;
    assert_no_errors(
        ok,
        &[
            auction_events_window(),
            static_win_window(),
            rich_out_window(),
        ],
    );

    // ...while asof is meaningless on a static table (no time column).
    let asof = r#"
rule r {
    events { a : auction_events }
    on each a -> score(1.0)
    join static_win asof within 10s on a.id == static_win.val
    entity(digit, a.id)
    yield out (n0 = a.id)
}
"#;
    assert_has_error(
        asof,
        &[
            auction_events_window(),
            static_win_window(),
            rich_out_window(),
        ],
        "provider/静态窗口",
    );
}

// ===========================================================================
// joins.rs — within / emit-at / reduce / condition branches
// ===========================================================================

#[test]
fn join_right_side_qualified_missing_field_rejected() {
    let input = r#"
rule r {
    events { a : auction_events }
    match<id:10m> { on event { a | count >= 1; } } -> score(1.0)
    join bid_events on a.id == bid_events.nope
    entity(digit, a.id)
    yield out (n0 = a.id)
}
"#;
    assert_has_error(
        input,
        &[
            auction_events_window(),
            bid_events_window(),
            rich_out_window(),
        ],
        "field `nope` not found in window `bid_events`",
    );
}

#[test]
fn join_within_mixed_bound_kinds_rejected() {
    // One duration bound + one expression bound is inconsistent.
    let input = r#"
rule r {
    events { a : auction_events }
    match<id:10m> { on event { a | count >= 1; } } -> score(1.0)
    join bid_events within [10s, a.dateTime] on a.id == bid_events.auction
    entity(digit, a.id)
    yield out (n0 = a.id)
}
"#;
    assert_has_error(
        input,
        &[
            auction_events_window(),
            bid_events_window(),
            rich_out_window(),
        ],
        "必须同为相对时长或同为绝对时间表达式",
    );
}

#[test]
fn join_within_constant_span_valid_and_over_limit_rejected() {
    // Small constant span (within over) is fine...
    let ok = r#"
rule r {
    events { a : auction_events }
    match<id:10m> { on event { a | count >= 1; } } -> score(1.0)
    join bid_events within [1s, 5s] on a.id == bid_events.auction
    entity(digit, a.id)
    yield out (n0 = a.id)
}
"#;
    assert_no_errors(
        ok,
        &[
            auction_events_window(),
            bid_events_window(),
            rich_out_window(),
        ],
    );

    // ...but a span wider than the right window's `over` is rejected (D3).
    let over_limit = r#"
rule r {
    events { a : auction_events }
    match<id:10m> { on event { a | count >= 1; } } -> score(1.0)
    join bid_events within [1h, 3h] on a.id == bid_events.auction
    entity(digit, a.id)
    yield out (n0 = a.id)
}
"#;
    assert_has_error(
        over_limit,
        &[
            auction_events_window(),
            bid_events_window(),
            rich_out_window(),
        ],
        "超过右窗 over",
    );

    // lo > hi is also rejected.
    let inverted = r#"
rule r {
    events { a : auction_events }
    match<id:10m> { on event { a | count >= 1; } } -> score(1.0)
    join bid_events within [10s, 1s] on a.id == bid_events.auction
    entity(digit, a.id)
    yield out (n0 = a.id)
}
"#;
    assert_has_error(
        inverted,
        &[
            auction_events_window(),
            bid_events_window(),
            rich_out_window(),
        ],
        "下界必须 ≤ 上界",
    );
}

#[test]
fn join_emit_at_requires_within() {
    let input = r#"
rule r {
    events { a : auction_events }
    on each a -> score(1.0)
    join bid_events on a.id == bid_events.auction emit at a.expires
    entity(digit, a.id)
    yield out (n0 = a.id)
}
"#;
    assert_has_error(
        input,
        &[
            auction_events_window(),
            bid_events_window(),
            rich_out_window(),
        ],
        "`emit at` 需要 `within` 区间",
    );
}

#[test]
fn join_emit_at_requires_absolute_upper_bound() {
    // A duration upper bound cannot guarantee the trigger time.
    let input = r#"
rule r {
    events { a : auction_events }
    on each a -> score(1.0)
    join bid_events within [1s, 5s] on a.id == bid_events.auction emit at a.id
    entity(digit, a.id)
    yield out (n0 = a.id)
}
"#;
    assert_has_error(
        input,
        &[
            auction_events_window(),
            bid_events_window(),
            rich_out_window(),
        ],
        "要求 within 上界为绝对时间表达式",
    );
}

#[test]
fn join_emit_at_must_match_within_upper_bound() {
    // Same field as the upper bound → OK (Q9 form).
    let ok = r#"
rule r {
    events { a : auction_events }
    on each a -> score(1.0)
    join bid_events within [a.dateTime, a.expires] on a.id == bid_events.auction
        emit at a.expires
    entity(digit, a.id)
    yield out (n0 = a.id)
}
"#;
    assert_no_errors(
        ok,
        &[
            auction_events_window(),
            bid_events_window(),
            rich_out_window(),
        ],
    );

    // A different field than the upper bound → rejected (emit_at must be >= hi).
    let mismatch = r#"
rule r {
    events { a : auction_events }
    on each a -> score(1.0)
    join bid_events within [a.dateTime, a.expires] on a.id == bid_events.auction
        emit at a.dateTime
    entity(digit, a.id)
    yield out (n0 = a.id)
}
"#;
    assert_has_error(
        mismatch,
        &[
            auction_events_window(),
            bid_events_window(),
            rich_out_window(),
        ],
        "必须 ≥ within 上界",
    );
}

#[test]
fn join_reduce_qualified_missing_field_rejected() {
    let input = r#"
rule r {
    events { a : auction_events }
    on each a -> score(1.0)
    join bid_events reduce last(bid_events.nope) on a.id == bid_events.auction
    entity(digit, a.id)
    yield out (n0 = a.id)
}
"#;
    assert_has_error(
        input,
        &[
            auction_events_window(),
            bid_events_window(),
            rich_out_window(),
        ],
        "reduce measure field `bid_events.nope` not found in window `bid_events`",
    );
}

#[test]
fn join_reduce_structured_measure_rejected() {
    let input = r#"
rule r {
    events { a : auction_events }
    on each a -> score(1.0)
    join struct_join reduce maxrow(payload) on a.id == struct_join.id
    entity(digit, a.id)
    yield out (n0 = a.id)
}
"#;
    assert_has_error(
        input,
        &[
            auction_events_window(),
            struct_join_window(),
            rich_out_window(),
        ],
        "must be scalar",
    );
}

#[test]
fn join_within_expr_may_only_reference_driver_aliases() {
    // A BinOp bound whose right operand references the join target window is
    // rejected by the driver-alias-only check (also recurses into the BinOp).
    let input = r#"
rule r {
    events { a : auction_events }
    on each a -> score(1.0)
    join bid_events within [a.dateTime + bid_events.price, a.expires]
        on a.id == bid_events.auction
    entity(digit, a.id)
    yield out (n0 = a.id)
}
"#;
    assert_has_error(
        input,
        &[
            auction_events_window(),
            bid_events_window(),
            rich_out_window(),
        ],
        "只能引用驱动事件字段（左行），不能引用 join 右窗 `bid_events`",
    );
}

#[test]
fn join_emit_at_func_call_expr_resolves() {
    // emit-at as a function call (FuncCall recursion in driver-alias check).
    let input = r#"
rule r {
    events { a : auction_events }
    on each a -> score(1.0)
    join bid_events within [a.dateTime, a.expires] on a.id == bid_events.auction
        emit at fmt("{}", a.expires)
    entity(digit, a.id)
    yield out (n0 = a.id)
}
"#;
    assert_no_errors(
        input,
        &[
            auction_events_window(),
            bid_events_window(),
            rich_out_window(),
        ],
    );
}

// ===========================================================================
// rules/mod.rs — pipeline stage constraints / stats where shapes / on-each
// ===========================================================================

#[test]
fn pipeline_stage_with_on_each_rejected() {
    let input = r#"
rule r {
    events { a : auction_events }
    on each a
    |> match<id:10m> { on event { _in | count >= 1; } } -> score(1.0)
    entity(digit, a.id)
    yield out (n0 = a.id)
}
"#;
    assert_has_error(
        input,
        &[auction_events_window(), rich_out_window()],
        "`on each` pipeline stages are not supported yet",
    );
}

#[test]
fn pipeline_stage_with_seq_or_any_rejected() {
    let seq_stage = r#"
rule r {
    events { a : auction_events  b : auction_events }
    match<id:10m> { on event seq { has a; has b; } }
    |> match<id:10m> { on event { _in | count >= 1; } } -> score(1.0)
    entity(digit, a.id)
    yield out (n0 = a.id)
}
"#;
    assert_has_error(
        seq_stage,
        &[auction_events_window(), rich_out_window()],
        "not supported in pipeline stages",
    );

    let any_stage = r#"
rule r {
    events { a : auction_events  b : auction_events }
    match<id:10m> { on event any { a | count >= 1; b | count >= 1; } }
    |> match<id:10m> { on event { _in | count >= 1; } } -> score(1.0)
    entity(digit, a.id)
    yield out (n0 = a.id)
}
"#;
    assert_has_error(
        any_stage,
        &[auction_events_window(), rich_out_window()],
        "not supported in pipeline stages",
    );
}

#[test]
fn stats_where_expr_shapes_collect_fields() {
    // InList / IfThenElse / FuncCall / Neg shapes in stats measure `where`
    // expressions recurse through collect_expr_field_refs without error.
    let ok = r#"
rule stats_r {
    events { a : rich }
    stats<10s:fixed> {
        a | count as m1 where a.dport in (1, 2);
        a | count as m2 where if a.count > 1 then true else false;
        a | count as m3 where startswith(a.action, "x");
        a | count as m4 where -a.count < 0;
    }
    entity(ip, a.sip)
    yield out (s0 = "x")
}
"#;
    assert_no_errors(ok, &[rich_window(), rich_out_window()]);

    // Object / Array literal shapes still recurse, but the where is non-bool.
    let non_bool = r#"
rule stats_r {
    events { a : rich }
    stats<10s:fixed> {
        a | count as m1 where object { v = a.count; };
        a | count as m2 where array [a.count];
    }
    entity(ip, a.sip)
    yield out (s0 = "x")
}
"#;
    let errors = check_errors(non_bool, &[rich_window(), rich_out_window()]);
    assert_eq!(
        errors
            .iter()
            .filter(|m| m.contains("where expression must be bool"))
            .count(),
        2,
        "both non-bool where shapes must be rejected, got: {:?}",
        errors
    );
}

#[test]
fn on_each_neg_and_allowed_func_expressions_recursion() {
    // Unary neg and an allowed function call recurse through
    // check_on_each_expr without errors.
    let input = r#"
rule r {
    events { e : rich }
    on each e -> score(1.0)
    entity(ip, e.sip)
    yield out (n0 = -e.count, s0 = lower(e.action))
}
"#;
    assert_no_errors(input, &[rich_window(), rich_out_window()]);
}

#[test]
fn accu_requires_single_branch_step() {
    let input = r#"
rule r {
    events { e : rich }
    match<sip:5m> {
        on event<accu> { e | count >= 1 || e | count >= 2; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (n0 = e.count)
}
"#;
    assert_has_error(
        input,
        &[rich_window(), rich_out_window()],
        "exactly one step with a single branch",
    );
}

#[test]
fn seq_neg_with_field_aggregation_rejected() {
    let input = r#"
rule r {
    events { a : rich  b : rich }
    match<sip:30m> {
        on event seq {
            not a.count | count >= 1;
            has b;
        }
    } -> score(70.0)
    entity(ip, a.sip)
    yield out (n0 = a.count)
}
"#;
    assert_has_error(
        input,
        &[rich_window(), rich_out_window()],
        "references a field aggregation",
    );
}

#[test]
fn seq_skip_to_next_warns() {
    let input = r#"
rule r {
    events { a : rich  b : rich }
    match<sip:30m> {
        on event seq skip = to_next {
            has a;
            has b;
        }
    } -> score(70.0)
    entity(ip, a.sip)
    yield out (n0 = a.count)
}
"#;
    let file = parse_wfl(input).expect("parse should succeed");
    let errs = check_wfl(&file, &[rich_window(), rich_out_window()]);
    let warnings: Vec<&str> = errs
        .iter()
        .filter(|e| e.severity == Severity::Warning)
        .map(|e| e.message.as_str())
        .collect();
    assert!(
        warnings
            .iter()
            .any(|m| m.contains("`skip = to_next` is deferred to L3")),
        "expected skip=to_next warning, got: {:?}",
        warnings
    );
}

// ===========================================================================
// check_funcs.rs — builtin argument validation
// ===========================================================================

#[test]
fn builtin_scalar_functions_valid_usage() {
    let input = r#"
rule r {
    events { e : rich }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (
        b0 = regex_match(e.action, "x.*"),
        b1 = contains(e.action, "x"),
        b2 = startswith(e.action, "x"),
        b3 = endswith(e.action, "x"),
        b4 = startswith_any(e.action, "x", "y"),
        b5 = endswith_any(e.action, "x", "y"),
        b6 = isnull(e.sip),
        b7 = isnotnull(e.sip),
        b8 = is_blank(e.action),
        b9 = is_finite(e.count),
        b10 = has(e, "action"),
        s0 = substr(e.action, 0, 3),
        s1 = replace(e.action, "a", "b"),
        s2 = replace_plain(e.action, "a", "b"),
        s3 = trim(e.action),
        s4 = ltrim(e.action),
        s5 = rtrim(e.action),
        s6 = concat(e.action, e.user),
        s7 = upper(e.action),
        s8 = lower(e.action),
        s9 = md5(e.action),
        s10 = sha1(e.action),
        s11 = sha256(e.action),
        s12 = sha1_n(e.action, 8),
        s13 = stable_id("p", e.sip, e.user),
        s14 = join(e.action, e.user),
        s15 = join_by("-", e.action, e.user),
        s16 = default_if_blank(e.action, "d"),
        s17 = null_if_blank(e.action),
        s18 = strftime(e.event_time, "%Y"),
        n0 = count_char(e.action, "a"),
        n1 = indexof(e.action, "x"),
        n2 = len(e.action),
        n3 = now_s(),
        t0 = strptime(e.action, "%Y-%m-%d"),
        t1 = time_bucket(e.event_time, 60),
        t2 = bucket_end(e.event_time, 60),
        t3 = now(),
        h0 = hex(e.action)
    )
}
"#;
    assert_no_errors(input, &[rich_window(), rich_out_window()]);
}

#[test]
fn builtin_math_functions_valid_usage() {
    let input = r#"
rule r {
    events { e : rich }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (
        n0 = abs(e.count),
        f0 = ceil(e.count),
        f1 = floor(e.count),
        f2 = round(e.count, 1),
        f3 = sqrt(e.count),
        f4 = exp(e.count),
        f5 = sign(e.count),
        f6 = trunc(e.count),
        f7 = pow(e.count, 2),
        f8 = log(e.count, 2),
        f9 = clamp(e.count, 0, 100),
        f10 = time_diff(e.event_time, e.event_time),
        f11 = avg(e.count),
        f12 = stddev(e.count),
        f13 = percentile(e.count, 50),
        f14 = baseline(e.count, 10, "mean")
    )
}
"#;
    assert_no_errors(input, &[rich_window(), rich_out_window()]);
}

#[test]
fn builtin_array_functions_valid_usage() {
    let input = r#"
rule r {
    events { e : rich }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (
        arr0 = split(e.action, ","),
        arr1 = mvdedup(split(e.action, ",")),
        arr2 = mvsort(split(e.action, ",")),
        arr3 = mvreverse(split(e.action, ",")),
        arr4 = mvappend(e.action, "x"),
        s0 = mvjoin(split(e.action, ","), "-"),
        s1 = mvindex(split(e.action, ","), 0),
        n0 = mvcount(split(e.action, ",")),
        arrn = mvappend(e.count, 1)
    )
}
"#;
    assert_no_errors(input, &[rich_window(), rich_out_window()]);
}

#[test]
fn builtin_aggregates_valid_usage() {
    let input = r#"
rule r {
    events { e : rich }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (
        n0 = sum(e.count),
        n1 = min(e.count),
        n2 = max(e.count),
        f0 = avg(e.count),
        s0 = first(e.user),
        s1 = last(e.user),
        arr0 = collect_set(e.action),
        arr1 = collect_list(e.action)
    )
}
"#;
    assert_no_errors(input, &[rich_window(), rich_out_window()]);
}

#[test]
fn builtin_stat_selectors_valid_usage() {
    // stat.count(window_event(e)) + stat.value(trigger(t)) (event stage) and
    // stat.value(final(t)) (close stage) all pass.
    let input = r#"
rule r {
    events { e : rich }
    match<sip:5m> {
        on event { t: e | count >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (
        n0 = stat.count(window_event(e)),
        n1 = stat.count(match_event(t)),
        f0 = stat.value(trigger(t))
    )
}
rule r2 {
    events { e : rich }
    match<sip:5m> {
        on event { e | count >= 1; }
        and close { t: e | count >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (f0 = stat.value(final(t)))
}
"#;
    assert_no_errors(input, &[rich_window(), rich_out_window()]);
}

#[test]
fn builtin_wrong_arg_count_rejected() {
    let cases: &[(&str, &str)] = &[
        (
            "regex_match(e.action)",
            "regex_match() requires exactly 2 arguments",
        ),
        (
            "regex_match(e.action, \"a\", \"b\")",
            "regex_match() requires exactly 2 arguments",
        ),
        (
            "time_diff(e.event_time)",
            "time_diff() requires exactly 2 arguments",
        ),
        (
            "time_bucket(e.event_time)",
            "time_bucket() requires exactly 2 arguments",
        ),
        ("abs()", "abs() requires exactly 1 numeric argument"),
        ("round()", "round() requires 1 or 2 arguments"),
        ("sqrt()", "sqrt() requires exactly 1 numeric argument"),
        ("pow(1.0)", "pow() requires exactly 2 numeric arguments"),
        ("log()", "log() requires 1 or 2 numeric arguments"),
        ("clamp(1.0)", "clamp() requires exactly 3 numeric arguments"),
        ("coalesce()", "coalesce() requires at least 1 argument"),
        ("merge()", "merge() requires at least 1 argument"),
        ("isnull()", "isnull() requires exactly 1 argument"),
        ("is_blank()", "is_blank() requires exactly 1 argument"),
        (
            "default_if_blank(e.action)",
            "default_if_blank() requires exactly 2 arguments",
        ),
        ("now(1)", "now() requires no arguments"),
        (
            "count_char(e.action)",
            "count_char() requires exactly 2 arguments",
        ),
        ("strftime()", "strftime() requires 1 or 2 arguments"),
        (
            "strptime(e.action)",
            "strptime() requires exactly 2 arguments",
        ),
        (
            "contains(e.action)",
            "contains() requires exactly 2 arguments",
        ),
        (
            "startswith(e.action)",
            "startswith() requires exactly 2 arguments",
        ),
        (
            "startswith_any(e.action)",
            "startswith_any() requires at least 2 arguments",
        ),
        ("md5()", "md5() requires exactly 1 argument"),
        ("sha1_n(e.action)", "sha1_n() requires exactly 2 arguments"),
        (
            "stable_id(\"p\")",
            "stable_id() requires at least 2 arguments",
        ),
        ("join()", "join() requires at least 1 argument"),
        ("join_by(\"-\")", "join_by() requires at least 2 arguments"),
        ("substr(e.action)", "substr() requires 2 or 3 arguments"),
        (
            "replace(e.action, \"a\")",
            "replace() requires exactly 3 arguments",
        ),
        (
            "replace_plain(e.action, \"a\")",
            "replace_plain() requires exactly 3 arguments",
        ),
        ("trim()", "trim() requires exactly 1 argument"),
        ("ltrim()", "ltrim() requires exactly 1 argument"),
        ("concat()", "concat() requires at least 1 argument"),
        (
            "indexof(e.action)",
            "indexof() requires exactly 2 arguments",
        ),
        ("mvcount()", "mvcount() requires exactly 1 argument"),
        ("mvjoin(e.action)", "mvjoin() requires exactly 2 arguments"),
        ("split(e.action)", "split() requires exactly 2 arguments"),
        ("mvdedup()", "mvdedup() requires exactly 1 argument"),
        ("mvsort()", "mvsort() requires exactly 1 argument"),
        ("mvindex(e.action)", "mvindex() requires 2 or 3 arguments"),
        ("len()", "len() requires exactly 1 argument"),
        ("collect_set()", "collect_set() requires exactly 1 argument"),
        ("first()", "first() requires exactly 1 argument"),
        ("stddev()", "stddev() requires exactly 1 argument"),
        (
            "percentile(e.count)",
            "percentile() requires exactly 2 arguments",
        ),
        ("baseline(e.count)", "baseline() requires 2 or 3 arguments"),
        ("has()", "has() expects 1 or 2 arguments"),
    ];
    for (expr, msg) in cases {
        let src = format!(
            "rule r {{ events {{ e : rich }} match<:5m> {{ on event {{ e | count >= 1; }} }} -> score(50.0) entity(ip, e.sip) yield out (b0 = {expr}) }}"
        );
        assert!(
            check_errors(&src, &[rich_window(), rich_out_window()])
                .iter()
                .any(|m| m.contains(msg)),
            "expected {:?} from {expr}",
            msg
        );
    }
}

#[test]
fn builtin_wrong_arg_type_rejected() {
    let cases: &[(&str, &str)] = &[
        // sum/avg require numeric fields.
        ("sum(e.sip)", "requires a numeric field"),
        ("avg(e.user)", "requires a numeric field"),
        // min/max require orderable fields.
        ("min(e.sip)", "requires an orderable field"),
        ("max(e.roles_obj)", "requires an orderable field"),
        // sum over a set-level alias is a projection error.
        ("sum(e)", "requires a field projection"),
        ("min(e)", "requires a field projection"),
        // regex_match / contains / startswith require chars.
        (
            "regex_match(e.count, \"x\")",
            "first argument must be chars",
        ),
        (
            "regex_match(e.action, 5)",
            "second argument must be a string literal",
        ),
        ("contains(e.count, \"x\")", "argument 1 must be chars"),
        ("startswith(e.count, \"x\")", "argument 1 must be chars"),
        ("trim(e.count)", "argument must be chars"),
        ("lower(e.count)", "argument must be chars"),
        ("substr(e.count, 0)", "first argument must be chars"),
        ("split(e.count, \",\")", "first argument must be chars"),
        (
            "strftime(e.action)",
            "first argument must be time or numeric",
        ),
        // time_diff / time_bucket accept only time or numeric.
        (
            "time_diff(e.action, e.action)",
            "argument 1 must be time or numeric",
        ),
        (
            "time_bucket(e.action, 60)",
            "first argument must be time or numeric",
        ),
        // baseline / math require numeric.
        (
            "baseline(e.user, 10)",
            "baseline() first argument must be numeric",
        ),
        (
            "baseline(e.count, 0)",
            "baseline() second argument must be a positive duration",
        ),
        (
            "baseline(e.count, 10, 1)",
            "baseline() method must be a string literal",
        ),
        ("abs(e.user)", "argument must be numeric"),
        ("pow(e.user, 2)", "argument 1 must be numeric"),
        ("sqrt(e.user)", "argument must be numeric"),
        ("clamp(e.user, 0, 1)", "argument 1 must be numeric"),
        ("round(e.user)", "first argument must be numeric"),
        // mv* require arrays.
        ("mvcount(e.action)", "argument must be an array expression"),
        (
            "mvjoin(e.action, \"-\")",
            "first argument must be an array expression",
        ),
        ("mvdedup(e.action)", "argument must be an array expression"),
        ("mvsort(e.action)", "argument must be an array expression"),
        (
            "mvindex(e.count, 0)",
            "first argument must be an array expression",
        ),
        // L3 collection/statistical functions require column projections.
        ("collect_set(e)", "argument must be a column projection"),
        ("first(e)", "argument must be a column projection"),
        ("stddev(e)", "argument must be a column projection"),
        ("percentile(e, 50)", "field must be a column projection"),
        ("stddev(e.user)", "requires a numeric field"),
        ("percentile(e.user, 50)", "field must be numeric"),
        (
            "percentile(e.count, 101)",
            "p must be a number literal 0-100",
        ),
        // merge requires object args.
        ("merge(e.count, e.user)", "argument 1 must be object"),
        // count() rejects field projections.
        ("count(e.action)", "count() expects a set-level argument"),
        // count_char / default_if_blank require chars.
        ("count_char(e.count, \"a\")", "argument 1 must be chars"),
        (
            "default_if_blank(e.count, \"d\")",
            "argument 1 must be chars",
        ),
        // replace pattern must be a string literal.
        (
            "replace(e.action, 5, \"b\")",
            "second argument must be a string literal",
        ),
        // sha1_n length range.
        (
            "sha1_n(e.action, 99)",
            "length must be an integer from 1 to 40",
        ),
    ];
    for (expr, msg) in cases {
        let src = format!(
            "rule r {{ events {{ e : rich }} match<:5m> {{ on event {{ e | count >= 1; }} }} -> score(50.0) entity(ip, e.sip) yield out (b0 = {expr}) }}"
        );
        assert!(
            check_errors(&src, &[rich_window(), rich_out_window()])
                .iter()
                .any(|m| m.contains(msg)),
            "expected {:?} from {expr}",
            msg
        );
    }
}

#[test]
fn coalesce_type_mixing_rules() {
    // Mixed types in score context (allow_mixed_coalesce = false) → rejected.
    let mixed = r#"
rule r {
    events { e : rich }
    match<:5m> { on event { e | count >= 1; } } -> score(coalesce(e.count, "x"))
    entity(ip, e.sip)
    yield out (s0 = "x")
}
"#;
    assert_has_error(
        mixed,
        &[rich_window(), rich_out_window()],
        "is not compatible with",
    );

    // Compatible types pass.
    let ok = r#"
rule r {
    events { e : rich }
    match<:5m> { on event { e | count >= 1; } } -> score(coalesce(e.count, 1))
    entity(ip, e.sip)
    yield out (s0 = "x")
}
"#;
    assert_no_errors(ok, &[rich_window(), rich_out_window()]);
}

#[test]
fn mvappend_element_shapes() {
    // Valid mixed scalar + array append.
    let ok = r#"
rule r {
    events { e : rich }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (arr0 = mvappend(e.action, "x"))
}
"#;
    assert_no_errors(ok, &[rich_window(), rich_out_window()]);

    // Empty array literal is skipped.
    let empty = r#"
rule r {
    events { e : rich }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (arr0 = mvappend(array [], e.action))
}
"#;
    assert_no_errors(empty, &[rich_window(), rich_out_window()]);

    // Incompatible element types → rejected.
    let incompatible = r#"
rule r {
    events { e : rich }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (arr0 = mvappend(e.tags, e.count))
}
"#;
    assert_has_error(
        incompatible,
        &[rich_window(), rich_out_window()],
        "not compatible with",
    );

    // Non scalar/array argument → rejected.
    let non_scalar = r#"
rule r {
    events { e : rich }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (arr0 = mvappend(e.roles_obj, "x"))
}
"#;
    assert_has_error(
        non_scalar,
        &[rich_window(), rich_out_window()],
        "must be scalar or array expression",
    );
}

#[test]
fn l3_funcs_rejected_in_guard_expressions() {
    let input = r#"
rule r {
    events { e : rich }
    match<sip:5m> {
        on event { e.action && collect_set(e.action) | count >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (s0 = "x")
}
"#;
    assert_has_error(
        input,
        &[rich_window(), rich_out_window()],
        "is not allowed in guard expressions",
    );
}

#[test]
fn stat_count_bad_selector_and_value_rejected() {
    // stat.count(match_distinct(...)) with a distinct non-count label → error.
    let bad_distinct = r#"
rule r {
    events { e : rich }
    match<:5m> { on event { t: e.count | distinct | sum >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n0 = stat.count(match_distinct(t)))
}
"#;
    assert_has_error(
        bad_distinct,
        &[rich_window(), rich_out_window()],
        "requires step label `t` to use distinct | count",
    );

    // stat.value() with an unknown selector.
    let bad_selector = r#"
rule r {
    events { e : rich }
    match<:5m> { on event { t: e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n0 = stat.value(window_event(e)))
}
"#;
    assert_has_error(
        bad_selector,
        &[rich_window(), rich_out_window()],
        "stat.value() accepts trigger(...) or final(...)",
    );
}
