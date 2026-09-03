//! 表达式/作用域/管道/lint 校验分支：types/check_expr.rs 表达式形状、scope.rs 解析
//! （含段内散落的 check_funcs 分支补测）、types/pipe.rs、lint 警告分支（2026-09-04
//! 自 coverage_extra.rs 拆出；经 `use super::*` 复用父模块 window harness）。

use super::*;

// ===========================================================================
// types/check_expr.rs — expression shape validation
// ===========================================================================

#[test]
fn object_literal_duplicate_field_rejected() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let _ = input;
    let dup = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = object { a = 1; a = 2; })
}
"#;
    assert_has_error(
        dup,
        &[auth_events_window(), output_window()],
        "duplicate object field `a`",
    );
}

#[test]
fn object_type_hint_incompatible_rejected() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = object { hint: ip = "not-an-ip-string"; })
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "object field type hint",
    );
}

#[test]
fn if_then_else_type_errors() {
    let branches = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = if e.action == "x" then e.sip else 5)
}
"#;
    assert_has_error(
        branches,
        &[auth_events_window(), output_window()],
        "if-then-else branches have incompatible types",
    );

    let cond = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = if e.sip then 1 else 2)
}
"#;
    assert_has_error(
        cond,
        &[auth_events_window(), output_window()],
        "if-then-else condition must be bool",
    );
}

#[test]
fn match_expr_ok_and_unknown_field_rejected() {
    // issue #79 Issue 2：severity 枚举归一化——多模式 `|` + 默认 `_` 通过 checker。
    let ok = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (y = case e.action {
        "emerg" | "alert" | "crit" => "CRITICAL",
        "error" => "HIGH",
        "warning" => "MEDIUM",
        _ => e.action,
    })
}
"#;
    assert_no_errors(ok, &[auth_events_window(), output_window()]);

    // match 的 subject/模式/分支值引用未知字段 → 报错（递归检查）。
    let bad = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (y = case e.action {
        "x" => missing_field,
        _ => "none",
    })
}
"#;
    assert_has_error(
        bad,
        &[auth_events_window(), output_window()],
        "`missing_field` not found",
    );
}

#[test]
fn logical_not_requires_bool_operand() {
    // `not <非 bool 字段>`（guard 上下文）→ 报错。
    let bad = r#"
rule r {
    events { e : auth_events && not e.sip }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        bad,
        &[auth_events_window(), output_window()],
        "logical `not` requires a bool operand",
    );

    // `not <bool 比较>` → 通过（无错误）。
    let ok = r#"
rule r {
    events { e : auth_events && not (e.action == "failed") }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_no_errors(ok, &[auth_events_window(), output_window()]);
}

#[test]
fn negation_and_arithmetic_require_numeric() {
    let neg = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(-e.sip)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        neg,
        &[auth_events_window(), output_window()],
        "unary negation requires numeric operand",
    );

    let arith = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(e.sip + 1)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        arith,
        &[auth_events_window(), output_window()],
        "arithmetic `+` requires numeric operands",
    );

    let ordering = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (b = e.sip > 1)
}
"#;
    assert_has_error(
        ordering,
        &[auth_events_window(), wide_output_window()],
        "ordering `>` requires numeric operands",
    );
}

#[test]
fn preset_param_outside_preset_rejected() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = $foo)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "can only be used inside a yield preset",
    );
}

#[test]
fn system_var_outside_yield_rejected() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(@score)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "system variables are only allowed in `yield` expressions",
    );
}

// ===========================================================================
// scope.rs — resolution branches
// ===========================================================================

#[test]
fn join_key_with_multi_bind_rejected() {
    // join-then-key requires a single driver bind.
    let schemas = vec![bid_win(), auction_win(), output_window()];
    let input = r#"
rule r {
    events { b : bid_events  d : bid_events }
    match<id:10m> { on event { b | count >= 1; } } -> score(50.0)
    join auction_events snapshot on b.auction == auction_events.id
    entity(ip, b.bidder)
    yield out (y = b.bidder)
}
"#;
    // The yield references an undeclared alias too; assert the join-key error
    // appears regardless.
    let errors = check_errors(input, &schemas);
    assert!(
        errors
            .iter()
            .any(|e| e.contains("join-then-key requires a single event bind")),
        "got: {errors:?}"
    );
}

#[test]
fn join_key_missing_from_every_source_rejected() {
    // A simple key absent from both driver events and join windows is reported
    // against every driver source.
    let schemas = vec![auth_events_window(), bid_win(), output_window()];
    let input = r#"
rule r {
    events { e : auth_events }
    match<missing_key:5m> { on event { e | count >= 1; } } -> score(50.0)
    join bid_events on e.sip == bid_events.bidder
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &schemas,
        "match key `missing_key` not found in event source `e` (window `auth_events`)",
    );
}

#[test]
fn bare_field_not_found_rejected() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = nonexistent)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "field `nonexistent` not found in any event source",
    );
}

#[test]
fn func_percentile_p_must_be_literal() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = percentile(e.count, e.sip))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), wide_output_window()],
        "percentile() p must be a number literal 0-100",
    );
}

#[test]
fn func_empty_arg_validation() {
    let coalesce = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (y = coalesce())
}
"#;
    assert_has_error(
        coalesce,
        &[auth_events_window(), wide_output_window()],
        "coalesce() requires at least 1 argument",
    );

    let merge = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = merge())
}
"#;
    assert_has_error(
        merge,
        &[auth_events_window(), wide_output_window()],
        "merge() requires at least 1 argument",
    );

    let default_count = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (y = default_if_blank(e.user))
}
"#;
    assert_has_error(
        default_count,
        &[auth_events_window(), wide_output_window()],
        "default_if_blank() requires exactly 2 arguments",
    );

    let substr_count = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (y = substr(e.user, 1, 2, 3))
}
"#;
    assert_has_error(
        substr_count,
        &[auth_events_window(), wide_output_window()],
        "substr() requires 2 or 3 arguments",
    );

    let replace_count = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (y = replace(e.user))
}
"#;
    assert_has_error(
        replace_count,
        &[auth_events_window(), wide_output_window()],
        "replace() requires exactly 3 arguments",
    );
}

#[test]
fn func_round_and_strftime_wrong_counts() {
    let round_first = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = round(e.sip))
}
"#;
    assert_has_error(
        round_first,
        &[auth_events_window(), wide_output_window()],
        "round() first argument must be numeric",
    );

    let strftime_empty = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (y = strftime())
}
"#;
    assert_has_error(
        strftime_empty,
        &[auth_events_window(), wide_output_window()],
        "strftime() requires 1 or 2 arguments",
    );
}

#[test]
fn func_isnull_valid_and_join_scalar_ok() {
    let ok = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (b = isnull(e.user), y = join(e.user, e.action))
}
"#;
    assert_no_errors(ok, &[auth_events_window(), wide_output_window()]);
}

// ===========================================================================
// types/pipe.rs — match step pipe chain branches
// ===========================================================================

#[test]
fn pipe_sum_without_field_selector_rejected() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> {
        on event { e | sum >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "sum() requires a field selector",
    );
}

#[test]
fn pipe_min_without_field_selector_rejected() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> {
        on event { e | min >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "min() requires a field selector",
    );
}

#[test]
fn pipe_threshold_type_mismatch_rejected() {
    // count() result is Digit; a Chars threshold is incompatible.
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> {
        on event { e | count >= "high"; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "threshold type Base(Chars) is not compatible with count() result type",
    );
}

#[test]
fn pipe_min_on_non_orderable_field_rejected() {
    // action is Chars (orderable), but an object-typed field is not.
    let input = r#"
rule r {
    events { e : obj_win }
    match<sip:5m> {
        on event { e.obj | min >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), obj_win(), output_window()],
        "min() requires an orderable field",
    );
}

#[test]
fn pipe_duplicate_step_label_rejected() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> {
        on event {
            fail: e | count >= 1;
            fail: e | count >= 2;
        }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "duplicate step label `fail`",
    );
}

#[test]
fn pipe_step_source_undeclared_alias_rejected() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> {
        on event { missing | count >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "match step source `missing` is not a declared event alias",
    );
}

// ===========================================================================
// lint — extra warning branches
// ===========================================================================

#[test]
fn lint_w001_alias_used_only_in_each_filter() {
    use crate::checker::lint::lint_wfl;
    let input = r#"
rule r {
    events { e : auth_events }
    on each e where e.action == "failed" -> score(1.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = crate::wfl_parser::parse_wfl(input).unwrap();
    let warnings = lint_wfl(&file, &[auth_events_window(), output_window()]);
    assert!(
        warnings
            .iter()
            .all(|w| !w.message.contains("[W001] event alias `e`")),
        "alias used in each filter should not warn, got: {:?}",
        warnings
            .iter()
            .map(|w| w.message.clone())
            .collect::<Vec<_>>()
    );
}

#[test]
fn lint_w004_close_step_zero_threshold_warns() {
    use crate::checker::lint::lint_wfl;
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> {
        on event { e | count >= 1; }
        on close { e | count >= 0; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = crate::wfl_parser::parse_wfl(input).unwrap();
    let warnings = lint_wfl(&file, &[auth_events_window(), output_window()]);
    assert!(
        warnings
            .iter()
            .any(|w| w.message.contains("[W004] step threshold is 0")),
        "close-step zero threshold should warn, got: {:?}",
        warnings
            .iter()
            .map(|w| w.message.clone())
            .collect::<Vec<_>>()
    );
}

#[test]
fn lint_w001_alias_used_only_in_seq_steps() {
    use crate::checker::lint::lint_wfl;
    let input = r#"
rule r {
    events { a : auth_events }
    match<:5m> {
        on event seq {
            a | count >= 1;
        }
    } -> score(50.0)
    entity(ip, a.sip)
    yield out (x = a.sip)
}
"#;
    let file = crate::wfl_parser::parse_wfl(input).unwrap();
    let warnings = lint_wfl(&file, &[auth_events_window(), output_window()]);
    assert!(
        warnings
            .iter()
            .all(|w| !w.message.contains("[W001] event alias `a`")),
        "alias used in seq step should not warn, got: {:?}",
        warnings
            .iter()
            .map(|w| w.message.clone())
            .collect::<Vec<_>>()
    );
}
