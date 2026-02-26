use super::*;

#[test]
fn regex_match_valid() {
    let input = r#"
rule r {
    events { e : auth_events && regex_match(action, "fail.*") }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_no_errors(input, &[auth_events_window(), output_window()]);
}

#[test]
fn regex_match_invalid_pattern() {
    let input = r#"
rule r {
    events { e : auth_events && regex_match(action, "[invalid") }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "not valid regex",
    );
}

#[test]
fn regex_match_non_string_pattern() {
    let input = r#"
rule r {
    events { e : auth_events && regex_match(action, 42) }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "string literal pattern",
    );
}

#[test]
fn time_diff_valid() {
    let out = make_output_window(
        "out",
        vec![("x", bt(BaseType::Ip)), ("diff", bt(BaseType::Float))],
    );
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (diff = time_diff(e.event_time, e.event_time))
}
"#;
    assert_no_errors(input, &[auth_events_window(), out]);
}

#[test]
fn time_diff_wrong_arg_count() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = time_diff(e.event_time))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "requires exactly 2 arguments",
    );
}

#[test]
fn contains_valid() {
    let input = r#"
rule r {
    events { e : auth_events && contains(action, "fail") }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_no_errors(input, &[auth_events_window(), output_window()]);
}

#[test]
fn startswith_and_endswith_valid() {
    let input = r#"
rule r {
    events { e : auth_events && startswith(e.action, "fail") && endswith(e.action, "ed") }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_no_errors(input, &[auth_events_window(), output_window()]);
}

#[test]
fn substr_valid() {
    let out = make_output_window(
        "out",
        vec![("x", bt(BaseType::Ip)), ("part", bt(BaseType::Chars))],
    );
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (part = substr(e.action, 1, 4))
}
"#;
    assert_no_errors(input, &[auth_events_window(), out]);
}

#[test]
fn substr_wrong_index_type() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = substr(e.action, "1"))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "second argument must be numeric",
    );
}

#[test]
fn len_valid() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = len(e.action))
}
"#;
    assert_no_errors(input, &[auth_events_window(), output_window()]);
}

#[test]
fn len_wrong_type() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = len(e.count))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "must be chars",
    );
}

#[test]
fn abs_valid() {
    let out = make_output_window(
        "out",
        vec![("x", bt(BaseType::Ip)), ("n", bt(BaseType::Digit))],
    );
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = abs(e.count - 10))
}
"#;
    assert_no_errors(input, &[auth_events_window(), out]);
}

#[test]
fn abs_wrong_type() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = abs(e.action))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "abs() argument must be numeric",
    );
}

#[test]
fn round_valid_with_precision() {
    let out = make_output_window(
        "out",
        vec![("x", bt(BaseType::Ip)), ("n", bt(BaseType::Float))],
    );
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = round(12.3456, 2))
}
"#;
    assert_no_errors(input, &[auth_events_window(), out]);
}

#[test]
fn strftime_and_strptime_valid() {
    let out = make_output_window(
        "out",
        vec![
            ("x", bt(BaseType::Ip)),
            ("ts_text", bt(BaseType::Chars)),
            ("ts_parsed", bt(BaseType::Time)),
        ],
    );
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (
        ts_text = strftime(e.event_time, "%Y-%m-%d"),
        ts_parsed = strptime("2026-02-26", "%Y-%m-%d")
    )
}
"#;
    assert_no_errors(input, &[auth_events_window(), out]);
}

#[test]
fn top50_batch4_functions_valid() {
    use crate::schema::FieldType;
    let out = make_output_window(
        "out",
        vec![
            ("x", bt(BaseType::Ip)),
            ("sqrt_v", bt(BaseType::Float)),
            ("pow_v", bt(BaseType::Float)),
            ("log_v", bt(BaseType::Float)),
            ("exp_v", bt(BaseType::Float)),
            ("clamp_v", bt(BaseType::Float)),
            ("sign_v", bt(BaseType::Float)),
            ("trunc_v", bt(BaseType::Float)),
            ("finite_v", bt(BaseType::Bool)),
            ("ltrim_v", bt(BaseType::Chars)),
            ("rtrim_v", bt(BaseType::Chars)),
            ("concat_v", bt(BaseType::Chars)),
            ("index_v", bt(BaseType::Digit)),
            ("replace_plain_v", bt(BaseType::Chars)),
            ("sw_any_v", bt(BaseType::Bool)),
            ("ew_any_v", bt(BaseType::Bool)),
            ("coalesce_v", bt(BaseType::Chars)),
            ("isnull_v", bt(BaseType::Bool)),
            ("isnotnull_v", bt(BaseType::Bool)),
            ("sorted_v", FieldType::Array(BaseType::Chars)),
            ("reversed_v", FieldType::Array(BaseType::Chars)),
        ],
    );
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (
        sqrt_v = sqrt(16),
        pow_v = pow(2, 8),
        log_v = log(100, 10),
        exp_v = exp(1),
        clamp_v = clamp(e.count, 1, 100),
        sign_v = sign(e.count - 5),
        trunc_v = trunc(12.987),
        finite_v = is_finite(e.count),
        ltrim_v = ltrim("  hello"),
        rtrim_v = rtrim("hello  "),
        concat_v = concat("u=", e.user, "_c=", e.count),
        index_v = indexof(e.action, "fail"),
        replace_plain_v = replace_plain(e.action, "_", "-"),
        sw_any_v = startswith_any(e.action, "fail", "deny"),
        ew_any_v = endswith_any(e.action, "ed", "ied"),
        coalesce_v = coalesce(e.user, e.action, "unknown"),
        isnull_v = isnull(mvindex(split(e.action, "_"), 99)),
        isnotnull_v = isnotnull(e.action),
        sorted_v = mvsort(split(e.action, "_")),
        reversed_v = mvreverse(split(e.action, "_"))
    )
}
"#;
    assert_no_errors(input, &[auth_events_window(), out]);
}

#[test]
fn coalesce_incompatible_types_rejected() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = coalesce(e.action, e.count))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "not compatible",
    );
}

#[test]
fn mvsort_wrong_type() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = mvsort(e.action))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "must be an array expression",
    );
}

#[test]
fn replace_valid() {
    let out = make_output_window(
        "out",
        vec![("x", bt(BaseType::Ip)), ("msg", bt(BaseType::Chars))],
    );
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (msg = replace(e.action, "fail.*", "blocked"))
}
"#;
    assert_no_errors(input, &[auth_events_window(), out]);
}

#[test]
fn replace_invalid_pattern() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = replace(e.action, "[bad", "x"))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "not valid regex",
    );
}

#[test]
fn trim_valid() {
    let out = make_output_window(
        "out",
        vec![("x", bt(BaseType::Ip)), ("msg", bt(BaseType::Chars))],
    );
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (msg = trim(e.action))
}
"#;
    assert_no_errors(input, &[auth_events_window(), out]);
}

#[test]
fn trim_wrong_type() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = trim(e.count))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "trim() argument must be chars",
    );
}

#[test]
fn split_valid() {
    use crate::schema::FieldType;
    let out = make_output_window(
        "out",
        vec![
            ("x", bt(BaseType::Ip)),
            ("parts", FieldType::Array(BaseType::Chars)),
        ],
    );
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (parts = split(e.action, "_"))
}
"#;
    assert_no_errors(input, &[auth_events_window(), out]);
}

#[test]
fn split_wrong_type() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = split(e.action, 42))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "split() second argument must be chars",
    );
}

// L3 Collection functions (M28.2)

#[test]
fn collect_set_valid() {
    use crate::schema::FieldType;
    let out = make_output_window(
        "out",
        vec![
            ("x", bt(BaseType::Ip)),
            ("resources", FieldType::Array(BaseType::Chars)),
        ],
    );
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (resources = collect_set(e.action))
}
"#;
    assert_no_errors(input, &[auth_events_window(), out]);
}

#[test]
fn collect_set_wrong_arg() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = collect_set(42))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "column projection",
    );
}

#[test]
fn mvcount_valid() {
    let out = make_output_window(
        "out",
        vec![("x", bt(BaseType::Ip)), ("n", bt(BaseType::Digit))],
    );
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = mvcount(collect_set(e.action)))
}
"#;
    assert_no_errors(input, &[auth_events_window(), out]);
}

#[test]
fn mvcount_wrong_type() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = mvcount(e.action))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "must be an array expression",
    );
}

#[test]
fn mvjoin_valid() {
    let out = make_output_window(
        "out",
        vec![("x", bt(BaseType::Ip)), ("joined", bt(BaseType::Chars))],
    );
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (joined = mvjoin(collect_list(e.action), ","))
}
"#;
    assert_no_errors(input, &[auth_events_window(), out]);
}

#[test]
fn mvjoin_wrong_type() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = mvjoin(e.action, ","))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "first argument must be an array expression",
    );
}

#[test]
fn mvdedup_valid() {
    use crate::schema::FieldType;
    let out = make_output_window(
        "out",
        vec![
            ("x", bt(BaseType::Ip)),
            ("uniq", FieldType::Array(BaseType::Chars)),
        ],
    );
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (uniq = mvdedup(collect_list(e.action)))
}
"#;
    assert_no_errors(input, &[auth_events_window(), out]);
}

#[test]
fn mvdedup_wrong_type() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = mvdedup(e.action))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "mvdedup() argument must be an array expression",
    );
}

#[test]
fn mvindex_valid_scalar_and_range() {
    use crate::schema::FieldType;
    let out = make_output_window(
        "out",
        vec![
            ("x", bt(BaseType::Ip)),
            ("pick", bt(BaseType::Chars)),
            ("slice", FieldType::Array(BaseType::Chars)),
        ],
    );
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (
        pick = mvindex(collect_list(e.action), 0),
        slice = mvindex(collect_list(e.action), 0, 1)
    )
}
"#;
    assert_no_errors(input, &[auth_events_window(), out]);
}

#[test]
fn mvindex_wrong_first_arg_type() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = mvindex(e.action, 0))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "first argument must be an array expression",
    );
}

#[test]
fn mvappend_valid() {
    use crate::schema::FieldType;
    let out = make_output_window(
        "out",
        vec![
            ("x", bt(BaseType::Ip)),
            ("vals", FieldType::Array(BaseType::Chars)),
        ],
    );
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (vals = mvappend(collect_list(e.action), "tail"))
}
"#;
    assert_no_errors(input, &[auth_events_window(), out]);
}

#[test]
fn mvappend_mixed_type_rejected() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = mvappend(collect_list(e.action), e.count))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "not compatible",
    );
}

#[test]
fn first_valid() {
    let out = make_output_window(
        "out",
        vec![
            ("x", bt(BaseType::Ip)),
            ("first_action", bt(BaseType::Chars)),
        ],
    );
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (first_action = first(e.action))
}
"#;
    assert_no_errors(input, &[auth_events_window(), out]);
}

#[test]
fn first_wrong_arg() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = first("literal"))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "column projection",
    );
}

#[test]
fn l3_function_rejected_in_guard_expression() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> {
        on event { e && first(e.action) == "failed" | count >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "not allowed in guard expressions",
    );
}

// L3 Statistical functions (M28.3)

#[test]
fn stddev_valid() {
    let out = make_output_window(
        "out",
        vec![("x", bt(BaseType::Ip)), ("dev", bt(BaseType::Float))],
    );
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (dev = stddev(e.count))
}
"#;
    assert_no_errors(input, &[auth_events_window(), out]);
}

#[test]
fn stddev_wrong_type() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = stddev(e.action))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "requires a numeric field",
    );
}

#[test]
fn percentile_valid() {
    let out = make_output_window(
        "out",
        vec![("x", bt(BaseType::Ip)), ("p95", bt(BaseType::Float))],
    );
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (p95 = percentile(e.count, 95))
}
"#;
    assert_no_errors(input, &[auth_events_window(), out]);
}

#[test]
fn percentile_out_of_range() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = percentile(e.count, 150))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "must be a number literal 0-100",
    );
}

// L3 Enhanced baseline with method (M28.4)

#[test]
fn baseline_with_method_valid() {
    let out = make_output_window(
        "out",
        vec![("x", bt(BaseType::Ip)), ("base", bt(BaseType::Float))],
    );
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (base = baseline(count(e), 86400, "ewma"))
}
"#;
    assert_no_errors(input, &[auth_events_window(), out]);
}

#[test]
fn baseline_with_invalid_method() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = baseline(count(e), 86400, "invalid"))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "must be one of: mean, ewma, median",
    );
}

#[test]
fn baseline_with_non_string_method() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = baseline(count(e), 86400, 123))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "must be a string literal",
    );
}
