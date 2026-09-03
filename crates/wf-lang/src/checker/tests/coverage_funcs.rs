//! check_funcs.rs 参数/分支校验（前半）：字符串 / join / hash / 时间 / 数值等
//! 内建函数族的错误分支覆盖（2026-09-04 自 coverage_extra.rs 拆出；`#[path]`
//! sibling 子模块，经 `use super::*` 复用父模块 window harness 与共享断言）。

use super::*;

// ===========================================================================
// check_funcs.rs — argument validation branches
// ===========================================================================

#[test]
fn func_count_char_wrong_arg_count_and_type() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = count_char(e.user))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), wide_output_window()],
        "count_char() requires exactly 2 arguments",
    );

    let input2 = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = count_char(e.sip, "a"))
}
"#;
    assert_has_error(
        input2,
        &[auth_events_window(), wide_output_window()],
        "count_char() argument 1 must be chars",
    );
}

#[test]
fn func_concat_requires_arguments() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (y = concat())
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), wide_output_window()],
        "concat() requires at least 1 argument",
    );
}

#[test]
fn func_indexof_rejects_non_chars() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = indexof(e.sip, "x"))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), wide_output_window()],
        "indexof() argument 1 must be chars",
    );
}

#[test]
fn func_replace_plain_rejects_non_chars_arg() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (y = replace_plain(e.user, e.sip, "x"))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), wide_output_window()],
        "replace_plain() argument 2 must be chars",
    );
}

#[test]
fn func_trim_family_rejects_non_chars() {
    for (name, src) in [
        ("trim", "trim(e.sip)"),
        ("ltrim", "ltrim(e.sip)"),
        ("rtrim", "rtrim(e.sip)"),
    ] {
        let input = format!(
            r#"
rule r {{
    events {{ e : auth_events }}
    match<:5m> {{ on event {{ e | count >= 1; }} }} -> score(50.0)
    entity(ip, e.sip)
    yield out (y = {src})
}}
"#
        );
        assert_has_error(
            &input,
            &[auth_events_window(), wide_output_window()],
            &format!("{name}() argument must be chars"),
        );
    }
}

#[test]
fn func_isnull_family_wrong_arg_count() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (b = isnull())
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), wide_output_window()],
        "isnull() requires exactly 1 argument",
    );

    let input2 = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (b = isnotnull(e.user, e.user))
}
"#;
    assert_has_error(
        input2,
        &[auth_events_window(), wide_output_window()],
        "isnotnull() requires exactly 1 argument",
    );
}

#[test]
fn func_first_last_require_column_projection() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = first(e))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), wide_output_window()],
        "first() argument must be a column projection",
    );

    let input2 = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = last())
}
"#;
    assert_has_error(
        input2,
        &[auth_events_window(), wide_output_window()],
        "last() requires exactly 1 argument: alias.field",
    );
}

#[test]
fn func_has_arg_validation() {
    let zero = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (b = has())
}
"#;
    assert_has_error(
        zero,
        &[auth_events_window(), wide_output_window()],
        "has() expects 1 or 2 arguments",
    );

    let too_many = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (b = has(e, e, e))
}
"#;
    assert_has_error(
        too_many,
        &[auth_events_window(), wide_output_window()],
        "has() expects 1 or 2 arguments",
    );

    let non_literal = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (b = has(e, e.sip))
}
"#;
    assert_has_error(
        non_literal,
        &[auth_events_window(), wide_output_window()],
        "has() second argument must be a string literal",
    );
}

#[test]
fn func_stable_id_validation() {
    let too_few = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (y = stable_id("p"))
}
"#;
    assert_has_error(
        too_few,
        &[auth_events_window(), wide_output_window()],
        "stable_id() requires at least 2 arguments",
    );

    let bad_prefix = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (y = stable_id(e.sip, e.sip))
}
"#;
    assert_has_error(
        bad_prefix,
        &[auth_events_window(), wide_output_window()],
        "stable_id() prefix must be chars",
    );
}

#[test]
fn func_join_family_arg_validation() {
    let empty_join = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (y = join())
}
"#;
    assert_has_error(
        empty_join,
        &[auth_events_window(), wide_output_window()],
        "join() requires at least 1 argument",
    );

    let join_by_few = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (y = join_by(","))
}
"#;
    assert_has_error(
        join_by_few,
        &[auth_events_window(), wide_output_window()],
        "join_by() requires at least 2 arguments",
    );

    let join_by_sep = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (y = join_by(e.sip, "x"))
}
"#;
    assert_has_error(
        join_by_sep,
        &[auth_events_window(), wide_output_window()],
        "join_by() separator must be chars",
    );

    let non_scalar = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (y = join(e.user, split(e.user, ",")))
}
"#;
    assert_has_error(
        non_scalar,
        &[auth_events_window(), wide_output_window()],
        "join() argument 2 must be scalar",
    );
}

#[test]
fn func_sha1_n_validation() {
    let wrong_count = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (y = sha1_n("x"))
}
"#;
    assert_has_error(
        wrong_count,
        &[auth_events_window(), wide_output_window()],
        "sha1_n() requires exactly 2 arguments",
    );

    for bad in ["0", "41", "1.5"] {
        let input = format!(
            r#"
rule r {{
    events {{ e : auth_events }}
    match<:5m> {{ on event {{ e | count >= 1; }} }} -> score(50.0)
    entity(ip, e.sip)
    yield out (y = sha1_n("x", {bad}))
}}
"#
        );
        assert_has_error(
            &input,
            &[auth_events_window(), wide_output_window()],
            "sha1_n() length must be an integer from 1 to 40",
        );
    }
}

#[test]
fn func_baseline_validation() {
    let wrong_count = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = baseline(e.count))
}
"#;
    assert_has_error(
        wrong_count,
        &[auth_events_window(), wide_output_window()],
        "baseline() requires 2 or 3 arguments",
    );

    let non_numeric = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = baseline(e.sip, 10))
}
"#;
    assert_has_error(
        non_numeric,
        &[auth_events_window(), wide_output_window()],
        "baseline() first argument must be numeric",
    );

    let non_positive = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = baseline(e.count, 0))
}
"#;
    assert_has_error(
        non_positive,
        &[auth_events_window(), wide_output_window()],
        "baseline() second argument must be a positive duration",
    );
}

#[test]
fn func_time_bucket_family_validation() {
    let wrong_count = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = time_bucket(e.event_time))
}
"#;
    assert_has_error(
        wrong_count,
        &[auth_events_window(), wide_output_window()],
        "time_bucket() requires exactly 2 arguments",
    );

    let bad_first = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = time_bucket(e.sip, 60))
}
"#;
    assert_has_error(
        bad_first,
        &[auth_events_window(), wide_output_window()],
        "time_bucket() first argument must be time or numeric",
    );

    let bad_second = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = time_bucket(e.event_time, e.sip))
}
"#;
    assert_has_error(
        bad_second,
        &[auth_events_window(), wide_output_window()],
        "time_bucket() second argument must be numeric",
    );

    let bucket_end_count = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = bucket_end(e.event_time))
}
"#;
    assert_has_error(
        bucket_end_count,
        &[auth_events_window(), wide_output_window()],
        "bucket_end() requires exactly 2 arguments",
    );
}

#[test]
fn func_round_validation() {
    let wrong_count = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = round())
}
"#;
    assert_has_error(
        wrong_count,
        &[auth_events_window(), wide_output_window()],
        "round() requires 1 or 2 arguments",
    );

    let bad_precision = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = round(1.5, e.sip))
}
"#;
    assert_has_error(
        bad_precision,
        &[auth_events_window(), wide_output_window()],
        "round() second argument must be numeric",
    );
}

#[test]
fn func_pow_log_clamp_validation() {
    let pow_count = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = pow(2))
}
"#;
    assert_has_error(
        pow_count,
        &[auth_events_window(), wide_output_window()],
        "pow() requires exactly 2 numeric arguments",
    );

    let pow_type = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = pow(e.sip, 2))
}
"#;
    assert_has_error(
        pow_type,
        &[auth_events_window(), wide_output_window()],
        "pow() argument 1 must be numeric",
    );

    let log_count = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = log())
}
"#;
    assert_has_error(
        log_count,
        &[auth_events_window(), wide_output_window()],
        "log() requires 1 or 2 numeric arguments",
    );

    let log_type = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = log(e.sip))
}
"#;
    assert_has_error(
        log_type,
        &[auth_events_window(), wide_output_window()],
        "log() argument 1 must be numeric",
    );

    let clamp_count = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = clamp(1, 2))
}
"#;
    assert_has_error(
        clamp_count,
        &[auth_events_window(), wide_output_window()],
        "clamp() requires exactly 3 numeric arguments",
    );

    let clamp_type = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = clamp(e.sip, 1, 2))
}
"#;
    assert_has_error(
        clamp_type,
        &[auth_events_window(), wide_output_window()],
        "clamp() argument 1 must be numeric",
    );
}

#[test]
fn func_unary_math_wrong_arg_count() {
    let abs_count = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = abs())
}
"#;
    assert_has_error(
        abs_count,
        &[auth_events_window(), wide_output_window()],
        "abs() requires exactly 1 numeric argument",
    );

    let sqrt_type = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = sqrt(e.sip))
}
"#;
    assert_has_error(
        sqrt_type,
        &[auth_events_window(), wide_output_window()],
        "sqrt() argument must be numeric",
    );
}

#[test]
fn func_strftime_strptime_validation() {
    let bad_second = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (y = strftime(e.event_time, e.sip))
}
"#;
    assert_has_error(
        bad_second,
        &[auth_events_window(), wide_output_window()],
        "strftime() second argument must be chars",
    );

    let strptime_count = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = strptime("x"))
}
"#;
    assert_has_error(
        strptime_count,
        &[auth_events_window(), wide_output_window()],
        "strptime() requires exactly 2 arguments",
    );
}

#[test]
fn func_string_helpers_wrong_arg_count() {
    let contains = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (b = contains("a"))
}
"#;
    assert_has_error(
        contains,
        &[auth_events_window(), wide_output_window()],
        "contains() requires exactly 2 arguments",
    );

    let startswith = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (b = startswith("a"))
}
"#;
    assert_has_error(
        startswith,
        &[auth_events_window(), wide_output_window()],
        "startswith() requires exactly 2 arguments",
    );

    let any_few = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (b = startswith_any("a"))
}
"#;
    assert_has_error(
        any_few,
        &[auth_events_window(), wide_output_window()],
        "startswith_any() requires at least 2 arguments",
    );

    let md5_count = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (y = md5())
}
"#;
    assert_has_error(
        md5_count,
        &[auth_events_window(), wide_output_window()],
        "md5() requires exactly 1 argument",
    );
}
