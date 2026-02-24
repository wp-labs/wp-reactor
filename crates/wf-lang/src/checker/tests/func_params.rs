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
