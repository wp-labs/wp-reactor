//! Extra coverage tests for the checker: error branches of check_funcs,
//! rule-level checks (rules/mod.rs), joins, keys, limits, expr type-checking
//! and scope resolution that the focused test files do not reach.

use super::*;
use crate::schema::FieldType;

// ---------------------------------------------------------------------------
// Extra windows used only by these tests
// ---------------------------------------------------------------------------

/// Window with a float field (non-key scalar excluded from join keys).
pub(super) fn float_win() -> WindowSchema {
    make_window(
        "float_win",
        vec!["float_stream"],
        vec![
            ("f", bt(BaseType::Float)),
            ("sip", bt(BaseType::Chars)),
            ("event_time", bt(BaseType::Time)),
        ],
    )
}

/// Window with structured (object/array) and bool fields.
pub(super) fn obj_win() -> WindowSchema {
    make_window(
        "obj_win",
        vec!["obj_stream"],
        vec![
            ("sip", bt(BaseType::Chars)),
            ("obj", FieldType::Object),
            ("arr", FieldType::Array(BaseType::Chars)),
            ("active", bt(BaseType::Bool)),
            ("event_time", bt(BaseType::Time)),
        ],
    )
}

/// Static provider window (side input): no streams, no time field, over = 0.
pub(super) fn provider_win() -> WindowSchema {
    WindowSchema {
        name: "prov".to_string(),
        streams: vec![],
        time_field: None,
        over: Duration::ZERO,
        fields: vec![
            FieldDef {
                name: "sip".to_string(),
                field_type: bt(BaseType::Chars),
            },
            FieldDef {
                name: "category".to_string(),
                field_type: bt(BaseType::Chars),
            },
        ],
    }
}

/// Window with a time field for asof/within join tests.
pub(super) fn bid_win() -> WindowSchema {
    make_window(
        "bid_events",
        vec!["bid_stream"],
        vec![
            ("auction", bt(BaseType::Digit)),
            ("bidder", bt(BaseType::Chars)),
            ("price", bt(BaseType::Digit)),
            ("category", bt(BaseType::Chars)),
            ("event_time", bt(BaseType::Time)),
        ],
    )
}

/// Snapshot-join target carrying `id` / `category` for join-then-key tests.
pub(super) fn auction_win() -> WindowSchema {
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

/// Output window with a broad field set (n, y, x, b, f).
pub(super) fn wide_output_window() -> WindowSchema {
    make_output_window(
        "out",
        vec![
            ("x", bt(BaseType::Ip)),
            ("y", bt(BaseType::Chars)),
            ("n", bt(BaseType::Digit)),
            ("b", bt(BaseType::Bool)),
            ("f", bt(BaseType::Float)),
        ],
    )
}

/// Two windows that both carry a field named `sip` but with different types.
pub(super) fn ip_sip_win() -> WindowSchema {
    make_window(
        "ip_sip",
        vec!["s1"],
        vec![
            ("sip", bt(BaseType::Ip)),
            ("event_time", bt(BaseType::Time)),
        ],
    )
}

pub(super) fn chars_sip_win() -> WindowSchema {
    make_window(
        "chars_sip",
        vec!["s2"],
        vec![
            ("sip", bt(BaseType::Chars)),
            ("event_time", bt(BaseType::Time)),
        ],
    )
}

/// A window whose field names collide with the step label used in tests.
pub(super) fn label_win() -> WindowSchema {
    make_window(
        "label_win",
        vec!["l_stream"],
        vec![
            ("fail", bt(BaseType::Digit)),
            ("event_time", bt(BaseType::Time)),
        ],
    )
}

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

#[test]
fn func_mv_family_validation() {
    let mvcount = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = mvcount())
}
"#;
    assert_has_error(
        mvcount,
        &[auth_events_window(), wide_output_window()],
        "mvcount() requires exactly 1 argument",
    );

    let mvjoin_count = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (y = mvjoin(split(e.user, ",")))
}
"#;
    assert_has_error(
        mvjoin_count,
        &[auth_events_window(), wide_output_window()],
        "mvjoin() requires exactly 2 arguments",
    );

    let mvjoin_sep = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (y = mvjoin(split(e.user, ","), e.sip))
}
"#;
    assert_has_error(
        mvjoin_sep,
        &[auth_events_window(), wide_output_window()],
        "mvjoin() second argument must be chars separator",
    );

    let split_type = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = mvcount(split(e.sip, ",")))
}
"#;
    assert_has_error(
        split_type,
        &[auth_events_window(), wide_output_window()],
        "split() first argument must be chars",
    );

    let mvsort_count = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (y = mvjoin(mvsort(), ","))
}
"#;
    assert_has_error(
        mvsort_count,
        &[auth_events_window(), wide_output_window()],
        "mvsort() requires exactly 1 argument",
    );

    let mvindex_count = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (y = mvindex(split(e.user, ",")))
}
"#;
    assert_has_error(
        mvindex_count,
        &[auth_events_window(), wide_output_window()],
        "mvindex() requires 2 or 3 arguments",
    );

    let mvindex_idx = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (y = mvindex(split(e.user, ","), e.sip))
}
"#;
    assert_has_error(
        mvindex_idx,
        &[auth_events_window(), wide_output_window()],
        "mvindex() second argument must be numeric index",
    );

    let mvindex_end = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (y = mvindex(split(e.user, ","), 1, e.sip))
}
"#;
    assert_has_error(
        mvindex_end,
        &[auth_events_window(), wide_output_window()],
        "mvindex() third argument must be numeric index",
    );
}

#[test]
fn func_mvappend_validation() {
    let empty = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = mvappend())
}
"#;
    assert_has_error(
        empty,
        &[auth_events_window(), wide_output_window()],
        "mvappend() requires at least 1 argument",
    );

    // split(...) is Array(Chars); e.sip is Ip — element types do not unify.
    let incompatible = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = mvcount(mvappend(split(e.user, ","), e.sip)))
}
"#;
    assert_has_error(
        incompatible,
        &[auth_events_window(), wide_output_window()],
        "mvappend() argument 2 type",
    );

    // Object arg is neither scalar nor array.
    let object_arg = r#"
rule r {
    events { e : obj_win }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = mvcount(mvappend(e.obj)))
}
"#;
    assert_has_error(
        object_arg,
        &[obj_win(), wide_output_window()],
        "mvappend() argument 1 must be scalar or array expression",
    );

    // Bool element + array-of-chars element is compatible-typed via Base(bool).
    let bool_ok = r#"
rule r {
    events { e : obj_win }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = mvcount(mvappend(e.active, e.active)))
}
"#;
    assert_no_errors(bool_ok, &[obj_win(), wide_output_window()]);
}

#[test]
fn func_aggregates_reject_set_level_alias() {
    let sum_alias = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = sum(e))
}
"#;
    assert_has_error(
        sum_alias,
        &[auth_events_window(), wide_output_window()],
        "sum() requires a field projection like alias.field; set-level alias `e` is not allowed",
    );

    let min_alias = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = min(e))
}
"#;
    assert_has_error(
        min_alias,
        &[auth_events_window(), wide_output_window()],
        "min() requires a field projection like alias.field; set-level alias `e` is not allowed",
    );

    // count() with a field projection (without distinct) is rejected.
    let count_field = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = count(e.sip))
}
"#;
    assert_has_error(
        count_field,
        &[auth_events_window(), wide_output_window()],
        "count() expects a set-level argument (alias), not a field projection",
    );

    // sum() over a nested path is rejected as non-column.
    let sum_path = r#"
rule r {
    events { e : obj_win }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = sum(e.obj.inner))
}
"#;
    assert_has_error(
        sum_path,
        &[obj_win(), wide_output_window()],
        "sum() argument must be a column projection (alias.field)",
    );

    // count(e) — a set-level alias — is valid.
    let count_ok = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = count(e))
}
"#;
    assert_no_errors(count_ok, &[auth_events_window(), wide_output_window()]);
}

// ===========================================================================
// check_funcs.rs — stat.* selector validation branches
// ===========================================================================

#[test]
fn stat_count_rejects_wrong_selector() {
    let input = r#"
rule r {
    events { auth : auth_events }
    match<sip:5m> { on event { fail: auth | count >= 1; } } -> score(50.0)
    entity(ip, auth.sip)
    yield out (n = stat.count(trigger(fail)))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), wide_output_window()],
        "stat.count() accepts window_event(...), match_event(...), or match_distinct(...), got trigger(...)",
    );
}

#[test]
fn stat_value_rejects_wrong_selector() {
    let input = r#"
rule r {
    events { auth : auth_events }
    match<sip:5m> { on event { fail: auth | count >= 1; } } -> score(50.0)
    entity(ip, auth.sip)
    yield out (n = stat.value(window_event(auth)))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), wide_output_window()],
        "stat.value() accepts trigger(...) or final(...), got window_event(...)",
    );
}

#[test]
fn stat_count_requires_one_selector_arg() {
    let input = r#"
rule r {
    events { auth : auth_events }
    match<sip:5m> { on event { fail: auth | count >= 1; } } -> score(50.0)
    entity(ip, auth.sip)
    yield out (n = stat.count())
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), wide_output_window()],
        "stat.count() requires exactly 1 stat selector argument",
    );
}

#[test]
fn stat_selector_parse_errors() {
    let unknown = r#"
rule r {
    events { auth : auth_events }
    match<sip:5m> { on event { fail: auth | count >= 1; } } -> score(50.0)
    entity(ip, auth.sip)
    yield out (n = stat.count(bogus(fail)))
}
"#;
    assert_has_error(
        unknown,
        &[auth_events_window(), wide_output_window()],
        "unknown stat selector `bogus(...)`",
    );

    let wrong_args = r#"
rule r {
    events { auth : auth_events }
    match<sip:5m> { on event { fail: auth | count >= 1; } } -> score(50.0)
    entity(ip, auth.sip)
    yield out (n = stat.count(window_event(auth, extra)))
}
"#;
    assert_has_error(
        wrong_args,
        &[auth_events_window(), wide_output_window()],
        "stat selector `window_event(...)` requires exactly 1 symbol argument",
    );

    let non_func = r#"
rule r {
    events { auth : auth_events }
    match<sip:5m> { on event { fail: auth | count >= 1; } } -> score(50.0)
    entity(ip, auth.sip)
    yield out (n = stat.count(auth))
}
"#;
    assert_has_error(
        non_func,
        &[auth_events_window(), wide_output_window()],
        "stat functions require a selector such as window_event(alias) or trigger(label)",
    );

    // stat.count(match_event(label)) requires the label measure to be count.
    let non_count = r#"
rule r {
    events { auth : auth_events }
    match<sip:5m> { on event { ports: auth.count | sum >= 2; } } -> score(50.0)
    entity(ip, auth.sip)
    yield out (n = stat.count(match_event(ports)))
}
"#;
    assert_has_error(
        non_count,
        &[auth_events_window(), wide_output_window()],
        "stat.count(match_event(ports)) requires step label `ports` to use count",
    );
}

// ===========================================================================
// rules/mod.rs — rule-level checks
// ===========================================================================

#[test]
fn where_without_join_rejected() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    where e.action == "failed"
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "`where` requires at least one `join` clause",
    );
}

#[test]
fn where_non_bool_rejected() {
    let schemas = vec![auth_events_window(), bid_win(), output_window()];
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    join bid_events on e.sip == bid_events.bidder
    where e.sip
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(input, &schemas, "`where` expression must be bool");
}

#[test]
fn on_each_filter_non_bool_rejected() {
    let input = r#"
rule r {
    events { e : auth_events }
    on each e where e.sip -> score(1.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "`on each where` expression must be bool",
    );
}

#[test]
fn on_each_rejects_set_level_alias_in_score() {
    let input = r#"
rule r {
    events { e : auth_events }
    on each e -> score(e)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "set-level alias references are not allowed in `on each` expressions",
    );
}

#[test]
fn on_each_rejects_qualified_close_reason() {
    let input = r#"
rule r {
    events { e : auth_events }
    on each e -> score(1.0)
    entity(ip, e.sip)
    yield out (y = e.close_reason)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "close_reason is not available in `on each`",
    );
}

#[test]
fn on_each_rejects_stat_funcs() {
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
}

#[test]
fn step_label_conflicts_with_match_key_field() {
    let input = r#"
rule r {
    events { e : label_win }
    match<fail:5m> { on event { fail: e | count >= 1; } } -> score(50.0)
    entity(ip, e.fail)
    yield out (n = e.fail)
}
"#;
    assert_has_error(
        input,
        &[label_win(), output_window()],
        "step label `fail` conflicts with match key field of the same name",
    );
}

// ===========================================================================
// rules/joins.rs — join validation branches
// ===========================================================================

#[test]
fn join_target_window_missing() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    join nonexistent on e.sip == nonexistent.sip
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "join target window `nonexistent` does not exist in schemas",
    );
}

#[test]
fn join_condition_left_side_unresolved() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    join bid_events on e.bogus == bid_events.bidder
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), bid_win(), output_window()],
        "join condition left side:",
    );
}

#[test]
fn join_condition_right_side_validation() {
    let schemas = vec![auth_events_window(), bid_win(), output_window()];

    let wrong_qualifier = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    join bid_events on e.sip == other.sip
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        wrong_qualifier,
        &schemas,
        "join condition right side `other.sip` must be qualified with target window `bid_events`",
    );

    let field_missing = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    join bid_events on e.sip == bid_events.bogus
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        field_missing,
        &schemas,
        "join condition: field `bogus` not found in window `bid_events`",
    );

    let unqualified = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    join bid_events on e.sip == bidder
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        unqualified,
        &schemas,
        "join condition right side must be qualified with window name",
    );
}

#[test]
fn join_key_must_be_scalar_base_type() {
    let float_join = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    join float_win on e.sip == float_win.f
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        float_join,
        &[auth_events_window(), float_win(), output_window()],
        "join key `float_win.f` must be a scalar base type",
    );

    let object_join = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    join obj_win on e.sip == obj_win.obj
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        object_join,
        &[auth_events_window(), obj_win(), output_window()],
        "join key `obj_win.obj` must be a scalar base type",
    );
}

#[test]
fn asof_join_requires_time_field_and_positive_within() {
    let no_time = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    join out asof on e.sip == out.x
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        no_time,
        &[auth_events_window(), output_window()],
        "join `out` uses asof mode but target window has no time field",
    );

    let zero_within = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    join bid_events asof within 0s on e.sip == bid_events.bidder
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        zero_within,
        &[auth_events_window(), bid_win(), output_window()],
        "join `bid_events` asof within must be > 0",
    );
}

#[test]
fn static_window_join_mode_restrictions() {
    // anti 对静态表**允许**（2026-08-24 放开）：纯键存在性否定不依赖时间，
    // 白名单排除是标准用例（Q21 形状）——不报错。
    let anti = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    join prov anti on e.sip == prov.sip
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_no_errors(anti, &[auth_events_window(), provider_win(), output_window()]);

    let within = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    join prov within 10s on e.sip == prov.sip
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        within,
        &[auth_events_window(), provider_win(), output_window()],
        "`within` interval 需要右窗 time 字段，静态表没有",
    );

    let emit = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    join prov on e.sip == prov.sip emit at e.sip
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        emit,
        &[auth_events_window(), provider_win(), output_window()],
        "`emit at` deferred 触发需要窗口生命周期，静态表没有",
    );

    let reduce = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    join prov reduce last(prov.sip) on e.sip == prov.sip
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        reduce,
        &[auth_events_window(), provider_win(), output_window()],
        "`reduce` 归约对静态表 v1 不支持",
    );
}

#[test]
fn reduce_field_validation() {
    let schemas = vec![auth_events_window(), bid_win(), output_window()];

    let wrong_qualifier = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    join bid_events reduce maxrow(other.price) on e.sip == bid_events.bidder
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        wrong_qualifier,
        &schemas,
        "join `bid_events` reduce measure field `other.price` must be qualified with target window `bid_events`",
    );

    let tie_missing = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    join bid_events reduce maxrow(price) tie(bogus) on e.sip == bid_events.bidder
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        tie_missing,
        &schemas,
        "join `bid_events` reduce tie field `bogus` not found in window `bid_events`",
    );
}

#[test]
fn reduce_measure_field_must_be_scalar() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    join obj_win reduce maxrow(obj_win.obj) on e.sip == obj_win.sip
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), obj_win(), output_window()],
        "join `obj_win` reduce measure field `obj` must be scalar (structured type cannot be ordered)",
    );
}

// ===========================================================================
// rules/keys.rs — match key validation branches
// ===========================================================================

#[test]
fn session_gap_must_be_positive() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:session(0s)> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "session(gap) gap must be > 0",
    );
}

#[test]
fn bracketed_and_qualified_key_unknown_alias() {
    let bracketed = r#"
rule r {
    events { e : auth_events }
    match<missing["dip"]:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        bracketed,
        &[auth_events_window(), output_window()],
        "match key `missing[\"dip\"]` references unknown alias `missing`",
    );

    let qualified = r#"
rule r {
    events { e : auth_events }
    match<missing.sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        qualified,
        &[auth_events_window(), output_window()],
        "match key `missing.sip` references unknown alias `missing`",
    );
}

#[test]
fn qualified_key_field_not_found() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<e.bogus:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "match key `e.bogus`: field `bogus` not found in window",
    );
}

#[test]
fn join_window_qualified_key_rejected() {
    let schemas = vec![auth_events_window(), bid_win(), output_window()];
    let input = r#"
rule r {
    events { e : auth_events }
    match<bid_events.price:5m> { on event { e | count >= 1; } } -> score(50.0)
    join bid_events on e.sip == bid_events.bidder
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &schemas,
        "match key `bid_events.price` references join window `bid_events`; join-side keys must be unqualified",
    );
}

#[test]
fn non_snapshot_join_key_source_rejected() {
    let schemas = vec![auth_events_window(), bid_win(), output_window()];
    let input = r#"
rule r {
    events { e : auth_events }
    match<price:5m> { on event { e | count >= 1; } } -> score(50.0)
    join bid_events asof on e.sip == bid_events.bidder
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &schemas,
        "match key `price` is only available on non-snapshot join window(s) (bid_events)",
    );
}

#[test]
fn key_mapping_source_field_validation() {
    let unqualified = r#"
rule r {
    events { e : auth_events }
    match<:5m> {
        key { user_id = sip; }
        on event { e | count >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        unqualified,
        &[auth_events_window(), output_window()],
        "key mapping `user_id`: source field must be qualified (alias.field)",
    );

    let unknown_alias = r#"
rule r {
    events { e : auth_events }
    match<:5m> {
        key { user_id = zzz.sip; }
        on event { e | count >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        unknown_alias,
        &[auth_events_window(), output_window()],
        "key mapping `user_id = zzz.sip`: alias `zzz` not declared in events",
    );

    let missing_field = r#"
rule r {
    events { e : auth_events }
    match<:5m> {
        key { user_id = e.bogus; }
        on event { e | count >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        missing_field,
        &[auth_events_window(), output_window()],
        "key mapping `user_id = e.bogus`: field `bogus` not found in window",
    );
}

#[test]
fn simple_key_type_mismatch_across_sources() {
    let schemas = vec![ip_sip_win(), chars_sip_win(), output_window()];
    let input = r#"
rule r {
    events { a : ip_sip  b : chars_sip }
    match<sip:5m> { on event { a | count >= 1; } } -> score(50.0)
    entity(ip, a.sip)
    yield out (x = a.sip)
}
"#;
    assert_has_error(input, &schemas, "match key `sip` type mismatch");
}

// ===========================================================================
// rules/limits.rs — limits validation branches
// ===========================================================================

#[test]
fn limits_unknown_key_rejected() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    limits { bogus = 1; }
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "unknown limits key `bogus`",
    );
}

#[test]
fn limits_on_exceed_invalid_value_rejected() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    limits { on_exceed = "bogus"; }
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "on_exceed value `bogus` invalid",
    );
}

#[test]
fn limits_max_throttle_invalid_formats() {
    let no_slash = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    limits { max_throttle = "1000"; }
}
"#;
    assert_has_error(
        no_slash,
        &[auth_events_window(), output_window()],
        "max_throttle value `1000` must be in format count/unit",
    );

    let bad_unit = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    limits { max_throttle = "1000/fortnight"; }
}
"#;
    assert_has_error(
        bad_unit,
        &[auth_events_window(), output_window()],
        "max_throttle unit `fortnight` invalid",
    );
}

#[test]
fn limits_max_memory_invalid_values() {
    let bad_suffix = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    limits { max_memory = "100"; }
}
"#;
    assert_has_error(
        bad_suffix,
        &[auth_events_window(), output_window()],
        "max_memory value `100` must end with KB, MB, or GB",
    );

    let bad_prefix = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    limits { max_memory = "abcMB"; }
}
"#;
    assert_has_error(
        bad_prefix,
        &[auth_events_window(), output_window()],
        "max_memory value `abcMB` must have a positive numeric prefix",
    );
}

#[test]
fn limits_valid_block_accepted() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    limits {
        max_memory = "512MB";
        max_instances = 2;
        max_throttle = "100/min";
        on_exceed = "drop_oldest";
    }
}
"#;
    assert_no_errors(input, &[auth_events_window(), output_window()]);
}

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
