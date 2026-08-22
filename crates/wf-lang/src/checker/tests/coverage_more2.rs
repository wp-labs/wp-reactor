//! Third-wave checker coverage tests: rules/mod.rs remaining branches
//! (on-each expression shapes — qualified close_reason, system/wfu vars,
//! in-list and if-then-else recursion, undeclared each alias; chain step
//! warnings), check_funcs.rs stat.* selector validation arms, and joins.rs
//! reduce/condition error branches not reached by earlier waves.

use std::time::Duration;

use super::*;
use crate::check_wfl;
use crate::schema::{BaseType, FieldDef, WindowSchema};
use crate::wfl_parser::parse_wfl;

// ---------------------------------------------------------------------------
// Local windows used only by these tests
// ---------------------------------------------------------------------------

fn auction_events_window() -> WindowSchema {
    WindowSchema {
        name: "auction_events".to_string(),
        streams: vec!["auction".to_string()],
        time_field: Some("dateTime".to_string()),
        over: Duration::from_secs(1800),
        fields: vec![
            FieldDef {
                name: "id".to_string(),
                field_type: bt(BaseType::Digit),
            },
            FieldDef {
                name: "category".to_string(),
                field_type: bt(BaseType::Chars),
            },
            FieldDef {
                name: "dateTime".to_string(),
                field_type: bt(BaseType::Time),
            },
            FieldDef {
                name: "expires".to_string(),
                field_type: bt(BaseType::Time),
            },
        ],
    }
}

fn bid_events_window() -> WindowSchema {
    WindowSchema {
        name: "bid_events".to_string(),
        streams: vec!["bid".to_string()],
        time_field: Some("dateTime".to_string()),
        over: Duration::from_secs(1800),
        fields: vec![
            FieldDef {
                name: "auction".to_string(),
                field_type: bt(BaseType::Digit),
            },
            FieldDef {
                name: "bidder".to_string(),
                field_type: bt(BaseType::Chars),
            },
            FieldDef {
                name: "price".to_string(),
                field_type: bt(BaseType::Digit),
            },
            FieldDef {
                name: "dateTime".to_string(),
                field_type: bt(BaseType::Time),
            },
        ],
    }
}

/// Driver window carrying a `close_reason` column (for on-each rejections).
fn dns_response_window() -> WindowSchema {
    make_window(
        "dns_response",
        vec!["dns_stream"],
        vec![
            ("query_id", bt(BaseType::Chars)),
            ("sip", bt(BaseType::Ip)),
            ("close_reason", bt(BaseType::Chars)),
            ("event_time", bt(BaseType::Time)),
        ],
    )
}

/// Output window with float + chars fields (system/wfu var yields).
fn meta_out_window() -> WindowSchema {
    make_output_window(
        "out",
        vec![
            ("s", bt(BaseType::Float)),
            ("r", bt(BaseType::Chars)),
        ],
    )
}

// ===========================================================================
// rules/mod.rs — check_on_each_expr / check_each_clause / check_seq
// ===========================================================================

#[test]
fn on_each_qualified_close_reason_rejected() {
    // `e.close_reason` (qualified) hits the qualified-field branch of
    // check_on_each_expr, distinct from the bare `close_reason` case.
    let input = r#"
rule r {
    events { e : dns_response }
    on each e where e.close_reason == "timeout" -> score(1.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[dns_response_window(), output_window()],
        "close_reason is not available in `on each`",
    );
}

#[test]
fn on_each_in_list_with_disallowed_func_rejected() {
    // The disallowed aggregate sits inside the `in (...)` list head — the
    // InList arm must recurse into the expression.
    let input = r#"
rule r {
    events { e : auth_events }
    on each e -> score(1.0)
    entity(ip, e.sip)
    yield out (b = avg(e.count) in (1.0, 2.0))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "function `avg` is not allowed in `on each`",
    );
}

#[test]
fn on_each_if_then_else_with_disallowed_func_rejected() {
    let input = r#"
rule r {
    events { e : auth_events }
    on each e -> score(1.0)
    entity(ip, e.sip)
    yield out (n = if avg(e.count) > 1.0 then 1 else 0)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "function `avg` is not allowed in `on each`",
    );
}

#[test]
fn on_each_stat_qualified_func_rejected() {
    // `stat.count` / `stat.value` are disallowed in on-each expressions.
    let input = r#"
rule r {
    events { e : auth_events }
    on each e -> score(1.0)
    entity(ip, e.sip)
    yield out (n = stat.count(window_event(e)))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "function `count` is not allowed in `on each`",
    );

    let input2 = r#"
rule r {
    events { e : auth_events }
    on each e -> score(1.0)
    entity(ip, e.sip)
    yield out (n = stat.value(final(total)))
}
"#;
    assert_has_error(
        input2,
        &[auth_events_window(), output_window()],
        "function `value` is not allowed in `on each`",
    );
}

#[test]
fn on_each_system_and_wfu_vars_allowed() {
    // SystemVar / WfuMeta arms of check_on_each_expr are pass-through.
    let input = r#"
rule r {
    events { e : auth_events }
    on each e -> score(1.0)
    entity(ip, e.sip)
    yield out (s = @score, r = @__wfu_rule_name)
}
"#;
    assert_no_errors(input, &[auth_events_window(), meta_out_window()]);
}

#[test]
fn on_each_undeclared_alias_rejected() {
    let input = r#"
rule r {
    events { e : auth_events }
    on each nope -> score(1.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "`on each` references undeclared event alias `nope`",
    );
}

#[test]
fn chain_first_step_neg_warns_and_within_over_window_warns() {
    // `not` as the first chain step warns; `within` exceeding the match window
    // duration also warns (both are check_seq warning branches).
    let input = r#"
rule r {
    events { a : auth_events  b : auth_events }
    match<sip:30m> {
        on event seq {
            not has a;
            has b within 40m;
        }
    } -> score(70.0)
    entity(ip, a.sip)
    yield out (x = a.sip)
}
"#;
    let file = parse_wfl(input).expect("parse should succeed");
    let errs = check_wfl(&file, &[auth_events_window(), output_window()]);
    let warnings: Vec<&str> = errs
        .iter()
        .filter(|e| e.severity == Severity::Warning)
        .map(|e| e.message.as_str())
        .collect();
    assert!(
        warnings
            .iter()
            .any(|m| m.contains("anchors to the window start")),
        "expected first-step-neg warning, got: {:?}",
        warnings
    );
    assert!(
        warnings
            .iter()
            .any(|m| m.contains("exceeds the match window duration")),
        "expected within-over-window warning, got: {:?}",
        warnings
    );
}

// ===========================================================================
// check_funcs.rs — stat.* selector validation arms
// ===========================================================================

#[test]
fn stat_count_window_event_join_target_rejected() {
    // stat.count(window_event(<join window>)) must be rejected — the symbol is
    // a join window, not a driver event alias.
    let input = r#"
rule r {
    events { a : auction_events }
    match<id:10m> { on event { a | count >= 1; } } -> score(1.0)
    join bid_events snapshot on a.id == bid_events.auction
    entity(digit, a.id)
    yield out (n = stat.count(window_event(bid_events)))
}
"#;
    assert_has_error(
        input,
        &[auction_events_window(), bid_events_window(), output_window()],
        "references unknown event alias `bid_events`",
    );
}

#[test]
fn stat_count_wrong_arity_and_bad_selector_forms() {
    // Zero arguments.
    let input0 = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = stat.count())
}
"#;
    assert_has_error(
        input0,
        &[auth_events_window(), output_window()],
        "stat.count() requires exactly 1 stat selector argument",
    );

    // Non-selector argument.
    let input1 = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = stat.count(e))
}
"#;
    assert_has_error(
        input1,
        &[auth_events_window(), output_window()],
        "stat functions require a selector",
    );

    // Unknown selector name.
    let input2 = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = stat.count(bogus(e)))
}
"#;
    assert_has_error(
        input2,
        &[auth_events_window(), output_window()],
        "unknown stat selector `bogus(...)`",
    );

    // Selector with zero symbol args.
    let input3 = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = stat.count(window_event()))
}
"#;
    assert_has_error(
        input3,
        &[auth_events_window(), output_window()],
        "requires exactly 1 symbol argument",
    );

    // Selector symbol not a bare field.
    let input4 = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = stat.count(window_event(e.sip)))
}
"#;
    assert_has_error(
        input4,
        &[auth_events_window(), output_window()],
        "requires a static symbol argument, without quotes",
    );
}

#[test]
fn stat_count_trigger_selector_rejected() {
    // `trigger` is only valid for stat.value.
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { t: e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = stat.count(trigger(t)))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "stat.count() accepts window_event(...), match_event(...), or match_distinct(...)",
    );
}

#[test]
fn stat_value_match_event_selector_rejected() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { t: e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = stat.value(match_event(t)))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "stat.value() accepts trigger(...) or final(...)",
    );
}

#[test]
fn stat_count_match_distinct_stage_and_measure_variants() {
    // Label declared on `and close` — wrong stage for match_distinct.
    let close_label = r#"
rule r {
    events { e : auth_events }
    match<:5m> {
        on event { e | count >= 1; }
        and close { t: e.sip | distinct | count >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = stat.count(match_distinct(t)))
}
"#;
    assert_has_error(
        close_label,
        &[auth_events_window(), output_window()],
        "requires step label `t` to come from on event",
    );

    // Distinct transform but non-count measure.
    let non_count = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { t: e.count | distinct | sum >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = stat.count(match_distinct(t)))
}
"#;
    assert_has_error(
        non_count,
        &[auth_events_window(), output_window()],
        "requires step label `t` to use distinct | count",
    );

    // Label without distinct.
    let no_distinct = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { t: e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = stat.count(match_distinct(t)))
}
"#;
    assert_has_error(
        no_distinct,
        &[auth_events_window(), output_window()],
        "requires step label `t` to use distinct",
    );

    // Unknown label.
    let unknown = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = stat.count(match_distinct(nope)))
}
"#;
    assert_has_error(
        unknown,
        &[auth_events_window(), output_window()],
        "references unknown step label `nope`",
    );
}

#[test]
fn stat_count_match_event_non_count_measure_rejected() {
    // The trailing `match_event` check requires the label's measure to be count.
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { t: e.count | sum >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = stat.count(match_event(t)))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "requires step label `t` to use count",
    );
}

#[test]
fn stat_value_trigger_on_close_label_rejected() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> {
        on event { e | count >= 1; }
        and close { t: e | count >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = stat.value(trigger(t)))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "requires step label `t` to come from on event",
    );
}

// ===========================================================================
// joins.rs — reduce / join-condition error branches
// ===========================================================================

#[test]
fn join_reduce_top_zero_rejected() {
    let input = r#"
rule r {
    events { a : auction_events }
    on each a -> score(1.0)
    join bid_events reduce top(0, price) on a.id == bid_events.auction
    entity(digit, a.id)
    yield out (n = a.id)
}
"#;
    assert_has_error(
        input,
        &[auction_events_window(), bid_events_window(), output_window()],
        "reduce top(N) N must be ≥ 1",
    );
}

#[test]
fn join_condition_left_side_resolve_error() {
    let input = r#"
rule r {
    events { a : auction_events }
    match<id:10m> { on event { a | count >= 1; } } -> score(1.0)
    join bid_events on a.bogus == bid_events.auction
    entity(digit, a.id)
    yield out (n = a.id)
}
"#;
    assert_has_error(
        input,
        &[auction_events_window(), bid_events_window(), output_window()],
        "join condition left side:",
    );
}

#[test]
fn join_condition_unqualified_right_rejected() {
    let input = r#"
rule r {
    events { a : auction_events }
    match<id:10m> { on event { a | count >= 1; } } -> score(1.0)
    join bid_events on a.id == auction
    entity(digit, a.id)
    yield out (n = a.id)
}
"#;
    assert_has_error(
        input,
        &[auction_events_window(), bid_events_window(), output_window()],
        "join condition right side must be qualified with window name",
    );
}

#[test]
fn join_reduce_last_and_minrow_ok() {
    // `reduce last` and `reduce minrow ... tie` both pass their checks.
    let input = r#"
rule r {
    events { a : auction_events }
    on each a -> score(1.0)
    join bid_events reduce last(price) on a.id == bid_events.auction
    join bid_events reduce minrow(price) tie(dateTime asc) on a.id == bid_events.auction
    entity(digit, a.id)
    yield out (n = a.id)
}
"#;
    assert_no_errors(input, &[auction_events_window(), bid_events_window(), output_window()]);
}

#[test]
fn join_reduce_top_n_and_last_field_missing_rejected() {
    let top_missing = r#"
rule r {
    events { a : auction_events }
    on each a -> score(1.0)
    join bid_events reduce top(3, nope) on a.id == bid_events.auction
    entity(digit, a.id)
    yield out (n = a.id)
}
"#;
    assert_has_error(
        top_missing,
        &[auction_events_window(), bid_events_window(), output_window()],
        "reduce measure field `nope` not found in window `bid_events`",
    );

    let last_missing = r#"
rule r {
    events { a : auction_events }
    on each a -> score(1.0)
    join bid_events reduce last(nope) on a.id == bid_events.auction
    entity(digit, a.id)
    yield out (n = a.id)
}
"#;
    assert_has_error(
        last_missing,
        &[auction_events_window(), bid_events_window(), output_window()],
        "reduce measure field `nope` not found in window `bid_events`",
    );
}
