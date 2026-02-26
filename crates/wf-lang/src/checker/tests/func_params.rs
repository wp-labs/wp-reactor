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
