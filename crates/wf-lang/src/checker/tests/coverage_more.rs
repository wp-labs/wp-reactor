//! Second-wave checker coverage tests: error branches in rules/mod.rs
//! (on-each recursion, pipeline/each combos, seq stat labels, undeclared
//! on-each alias), joins.rs (join-condition and reduce-field validation),
//! scope.rs (conflicting bare field types) and check_funcs.rs
//! (regex_match/lower/upper, stat window_event vs join target) that the
//! first-wave coverage_extra.rs does not reach.

use std::time::Duration;

use super::*;
use crate::check_wfl;
use crate::checker::Severity;
use crate::schema::{BaseType, FieldDef, WindowSchema};
use crate::wfl_parser::parse_wfl;

// ---------------------------------------------------------------------------
// Local windows used only by these tests
// ---------------------------------------------------------------------------

/// Driver window with `id` / `category` / `expires` fields (join-then-key
/// and within/emit-at shapes).
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

/// Join target window with `auction` / `bidder` / `price` fields.
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

/// Output window with id / n / y / b fields.
fn out_window() -> WindowSchema {
    make_output_window(
        "out",
        vec![
            ("id", bt(BaseType::Digit)),
            ("n", bt(BaseType::Digit)),
            ("y", bt(BaseType::Chars)),
            ("b", bt(BaseType::Bool)),
        ],
    )
}

/// Two windows carrying the same field name `sip` with different types.
fn ip_sip_win() -> WindowSchema {
    make_window(
        "ip_sip",
        vec!["s1"],
        vec![
            ("sip", bt(BaseType::Ip)),
            ("event_time", bt(BaseType::Time)),
        ],
    )
}

fn chars_sip_win() -> WindowSchema {
    make_window(
        "chars_sip",
        vec!["s2"],
        vec![
            ("sip", bt(BaseType::Chars)),
            ("event_time", bt(BaseType::Time)),
        ],
    )
}

// ===========================================================================
// check_funcs.rs — regex_match / lower / upper / stat window_event
// ===========================================================================

#[test]
fn regex_match_first_arg_must_be_chars() {
    // `regex_match(e.count, ...)` passes a Digit field where Chars is required.
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (b = regex_match(e.count, "x"))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), out_window()],
        "regex_match() first argument must be chars",
    );
}

#[test]
fn lower_upper_wrong_arg_count_and_type() {
    // lower() with 2 args → count error.
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (y = lower(e.action, "extra"))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), out_window()],
        "lower() requires exactly 1 argument",
    );

    // upper() on a Digit field → type error.
    let input2 = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (y = upper(e.count))
}
"#;
    assert_has_error(
        input2,
        &[auth_events_window(), out_window()],
        "upper() argument must be chars",
    );
}

#[test]
fn is_blank_family_wrong_arg_count() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (y = is_blank(e.user, e.action))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), out_window()],
        "is_blank() requires exactly 1 argument",
    );

    let input2 = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (y = null_if_blank())
}
"#;
    assert_has_error(
        input2,
        &[auth_events_window(), out_window()],
        "null_if_blank() requires exactly 1 argument",
    );
}

#[test]
fn mvappend_bool_elements_unify() {
    // Bool scalars unify to a Bool element type (ValType::Bool arm); the
    // result array is stringified so no target-array field is needed.
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (y = fmt("{}", mvappend(true, false)))
}
"#;
    assert_no_errors(input, &[auth_events_window(), out_window()]);
}

#[test]
fn lower_upper_valid() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (y = lower(upper(e.action)))
}
"#;
    assert_no_errors(input, &[auth_events_window(), out_window()]);
}

#[test]
fn stat_count_window_event_rejects_join_target_window() {
    // `window_event(bid_events)` names a join target (registered in
    // scope.join_windows but not in scope.aliases) — must be rejected.
    let input = r#"
rule r {
    events { a : auction_events }
    match<id:10m> { on event { a | count >= 1; } } -> score(1.0)
    join bid_events on a.id == bid_events.auction
    entity(digit, a.id)
    yield out (n = stat.count(window_event(bid_events)))
}
"#;
    assert_has_error(
        input,
        &[auction_events_window(), bid_events_window(), out_window()],
        "references unknown event alias `bid_events`",
    );
}

#[test]
fn stat_count_window_event_valid_alias_passes() {
    // Control: the same selector with a real event alias passes.
    let input = r#"
rule r {
    events { a : auction_events }
    match<id:10m> { on event { a | count >= 1; } } -> score(1.0)
    join bid_events on a.id == bid_events.auction
    entity(digit, a.id)
    yield out (n = stat.count(window_event(a)))
}
"#;
    assert_no_errors(
        input,
        &[auction_events_window(), bid_events_window(), out_window()],
    );
}

// ===========================================================================
// rules/mod.rs — on-each expression recursion and pipeline/each combos
// ===========================================================================

#[test]
fn on_each_undeclared_alias_rejected() {
    let input = r#"
rule r {
    events { e : auth_events }
    on each zzz -> score(1.0)
    entity(ip, e.sip)
    yield out (y = "x")
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), out_window()],
        "references undeclared event alias `zzz`",
    );
}

#[test]
fn on_each_if_then_else_recursion_rejects_close_reason() {
    // The `if ... then ... else` arm of check_on_each_expr must recurse into
    // the else branch, where the bare `close_reason` reference is rejected.
    let input = r#"
rule r {
    events { e : auth_events }
    on each e -> score(1.0)
    entity(ip, e.sip)
    yield out (y = if e.action == "failed" then e.sip else close_reason)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), out_window()],
        "close_reason is not available in `on each`",
    );
}

#[test]
fn on_each_in_list_recursion_rejects_aggregate_func() {
    // An aggregate function nested inside an `in (...)` list must be caught by
    // the InList recursion of check_on_each_expr.
    let input = r#"
rule r {
    events { e : auth_events }
    on each e -> score(1.0)
    entity(ip, e.sip)
    yield out (b = e.count in (1, avg(e.count)))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), out_window()],
        "function `avg` is not allowed in `on each`",
    );
}

#[test]
fn each_with_pipeline_stages_rejected() {
    // rule.each_clause is set (final stage is `on each`) while pipeline stages
    // exist — rejected by check_rule's first guard.
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } }
    |> on each e -> score(1.0)
    entity(ip, e.sip)
    yield out (y = e.sip)
}
"#;
    let errs = check_errors(input, &[auth_events_window(), out_window()]);
    assert!(
        errs.iter()
            .any(|m| m.contains("not supported together with pipeline stages")),
        "expected each+pipeline rejection, got: {:?}",
        errs
    );
}

#[test]
fn pipeline_stage_with_each_rejected() {
    // A non-final `on each` stage (pushed into pipeline_stages) must be
    // rejected by check_rule's per-stage guard.
    let input = r#"
rule r {
    events { e : auth_events }
    on each e
    |> match<sip:5m> { on event { _in | count >= 1; } } -> score(1.0)
    entity(ip, e.sip)
    yield out (y = e.sip)
}
"#;
    let errs = check_errors(input, &[auth_events_window(), out_window()]);
    assert!(
        errs.iter()
            .any(|m| m.contains("`on each` pipeline stages are not supported yet")),
        "expected pipeline-each rejection, got: {:?}",
        errs
    );
}

#[test]
fn seq_distinct_label_supports_match_distinct_stat() {
    // populate_stat_labels seq branch: a labeled step with `distinct` marks
    // uses_distinct=true so stat.count(match_distinct(label)) type-checks.
    let input = r#"
rule seq_stat {
    events { a : fw_events }
    match<sip:30m> {
        on event seq {
            uniq: a.dport | distinct | count >= 2;
        }
    } -> score(80.0)
    entity(ip, a.sip)
    yield out (n = stat.count(match_distinct(uniq)))
}
"#;
    assert_no_errors(input, &[fw_events_window(), out_window()]);
}

// ===========================================================================
// rules/joins.rs — join condition and reduce field validation
// ===========================================================================

#[test]
fn join_condition_right_wrong_qualifier_rejected() {
    let input = r#"
rule r {
    events { a : auction_events }
    match<id:10m> { on event { a | count >= 1; } } -> score(1.0)
    join bid_events on a.id == auction_events.id
    entity(digit, a.id)
    yield out (id = a.id)
}
"#;
    assert_has_error(
        input,
        &[auction_events_window(), bid_events_window(), out_window()],
        "must be qualified with target window `bid_events`",
    );
}

#[test]
fn join_condition_right_field_not_found_rejected() {
    let input = r#"
rule r {
    events { a : auction_events }
    match<id:10m> { on event { a | count >= 1; } } -> score(1.0)
    join bid_events on a.id == bid_events.nope
    entity(digit, a.id)
    yield out (id = a.id)
}
"#;
    assert_has_error(
        input,
        &[auction_events_window(), bid_events_window(), out_window()],
        "field `nope` not found in window `bid_events`",
    );
}

#[test]
fn reduce_measure_wrong_qualifier_rejected() {
    let input = r#"
rule r {
    events { a : auction_events }
    on each a -> score(1.0)
    join bid_events reduce maxrow(a.price) on a.id == bid_events.auction
    entity(digit, a.id)
    yield out (id = a.id)
}
"#;
    assert_has_error(
        input,
        &[auction_events_window(), bid_events_window(), out_window()],
        "reduce measure field `a.price` must be qualified with target window `bid_events`",
    );
}

#[test]
fn reduce_tie_wrong_qualifier_rejected() {
    let input = r#"
rule r {
    events { a : auction_events }
    on each a -> score(1.0)
    join bid_events reduce maxrow(price) tie(a.price asc) on a.id == bid_events.auction
    entity(digit, a.id)
    yield out (id = a.id)
}
"#;
    assert_has_error(
        input,
        &[auction_events_window(), bid_events_window(), out_window()],
        "reduce tie field `a.price` must be qualified with target window `bid_events`",
    );
}

// ===========================================================================
// checker/scope.rs — bare field resolution across sources
// ===========================================================================

#[test]
fn bare_field_conflicting_types_across_sources_rejected() {
    // `sip` resolves to Ip in ip_sip and Chars in chars_sip → resolve_simple
    // must reject the conflicting bare reference.
    let input = r#"
rule r {
    events { a : ip_sip  b : chars_sip }
    match<:5m> { on event { a | count >= 1; } } -> score(50.0)
    entity(ip, a.sip)
    yield out (y = sip)
}
"#;
    assert_has_error(
        input,
        &[ip_sip_win(), chars_sip_win(), out_window()],
        "field `sip` has conflicting types across event sources",
    );
}

// ===========================================================================
// checker/mod.rs — multi-error display and severity plumbing
// ===========================================================================

#[test]
fn check_wfl_reports_test_context_errors_with_test_name() {
    // contracts::check_tests errors carry `test: Some(...)`; Display must
    // prefix with `test` rather than `rule`.
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (y = e.sip)
}
test ct for r {
    input { row(bad, x = 1); }
    expect { hits == 1; }
}
"#;
    let file = parse_wfl(input).unwrap();
    let errs = check_wfl(&file, &[auth_events_window(), out_window()]);
    let display: Vec<String> = errs
        .iter()
        .filter(|e| e.severity == Severity::Error)
        .map(|e| e.to_string())
        .collect();
    assert!(
        display.iter().any(|s| s.contains("test `ct`")),
        "expected a test-context diagnostic, got: {:?}",
        display
    );
}

// ===========================================================================
// issue #90 — 规则级字符串字面量 let（正则复用）在 events 条件中的引用
// ===========================================================================

#[test]
fn events_regex_match_accepts_const_string_rule_let_reused_across_fields() {
    // 同一正则 let 复用于两个字段的 events 条件（规则文本中 let 位于 events 之后）。
    let input = r#"
rule r {
    events { e : auth_events && (regex_match(e.action, re) || regex_match(e.user, re)) }
    let re = "\\b62\\d{14,17}\\b"
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (y = e.action)
}
"#;
    assert_no_errors(input, &[auth_events_window(), out_window()]);
}

#[test]
fn events_regex_match_accepts_leading_const_string_let_before_events() {
    // let 声明可置于 events 块之前（issue #90 示例形态）；解析后与尾部 let 合并。
    let input = r#"
rule r {
    let re = "\\b62\\d{14,17}\\b"
    events { e : auth_events && regex_match(e.action, re) }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (y = e.action)
}
"#;
    assert_no_errors(input, &[auth_events_window(), out_window()]);
}

#[test]
fn events_regex_match_rejects_non_literal_rule_let() {
    // events 条件在绑定阶段求值，早于 per-event let 注入：仅字符串字面量 let
    // 可引用，非字面量（字段派生）let 显式报错并带变量名。
    let input = r#"
rule r {
    events { e : auth_events && regex_match(e.action, pat) }
    let pat = e.user
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (y = e.action)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), out_window()],
        "rule-level let `pat` used in the events condition must be a string literal",
    );
}

#[test]
fn events_regex_match_undefined_pattern_name_reports_name() {
    // 未声明的正则变量：静态检查报出变量名（第二参必须为字面量，且说明引用名）。
    let input = r#"
rule r {
    events { e : auth_events && regex_match(e.action, card_re) }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (y = e.action)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), out_window()],
        "reference to `card_re`",
    );
}

#[test]
fn events_regex_match_const_let_chain_alias_across_leading_trailing() {
    // 前置 `let base`（常量）＋ 尾部 `let re = base`（间接别名），events 条件
    // 引用 re —— 链式展开后仍应内联为字面量（跨前置/尾部边界的 let 引用链）。
    let input = r#"
rule r {
    let base = "\\b62\\d{14,17}\\b"
    events { e : auth_events && regex_match(e.action, re) }
    let re = base
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (y = e.action)
}
"#;
    assert_no_errors(input, &[auth_events_window(), out_window()]);
}

#[test]
fn events_regex_match_const_let_pattern_still_regex_validated() {
    // 内联后 pattern 仍走编译期正则校验：let 中携带非法正则须报错（与直接
    // 内联字面量同语义）。
    let input = r#"
rule r {
    events { e : auth_events && regex_match(e.action, re) }
    let re = "("
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (y = e.action)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), out_window()],
        "not valid regex",
    );
}

#[test]
fn events_filter_const_let_reusable_in_plain_comparison() {
    // 常量字符串 let 不限于 regex 槽位：events 条件中的普通比较同样内联。
    let input = r#"
rule r {
    events { e : auth_events && e.action == code }
    let code = "A1"
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (y = e.action)
}
"#;
    assert_no_errors(input, &[auth_events_window(), out_window()]);
}

#[test]
fn events_filter_non_literal_let_rejected_outside_regex_match() {
    // 非字面量 let 引用在 events 条件任意位置（非仅 regex 第二参）都拒绝并带名。
    let input = r#"
rule r {
    events { e : auth_events && e.action == p2 }
    let p2 = e.user
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (y = e.action)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), out_window()],
        "rule-level let `p2` used in the events condition must be a string literal",
    );
}
