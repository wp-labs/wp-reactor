use super::*;

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
fn strftime_accepts_project_default_format() {
    let out = make_output_window("out", vec![("ts_text", bt(BaseType::Chars))]);
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (ts_text = strftime(e.event_time))
}
"#;
    assert_no_errors(input, &[auth_events_window(), out]);
}

#[test]
fn now_functions_valid() {
    let out = make_output_window(
        "out",
        vec![
            ("x", bt(BaseType::Ip)),
            ("created_time", bt(BaseType::Time)),
            ("created_s", bt(BaseType::Digit)),
            ("created_ms", bt(BaseType::Digit)),
            ("created_us", bt(BaseType::Digit)),
            ("created_ns", bt(BaseType::Digit)),
            ("created_day", bt(BaseType::Chars)),
        ],
    );
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (
        created_time = now(),
        created_s = now_s(),
        created_ms = now_ms(),
        created_us = now_us(),
        created_ns = now_ns(),
        created_day = strftime(now(), "%Y-%m-%d")
    )
}
"#;
    assert_no_errors(input, &[auth_events_window(), out]);
}

#[test]
fn now_functions_reject_arguments() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = now(1))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "now() requires no arguments",
    );
}

#[test]
fn time_to_ms_and_time_to_s_accept_time_values() {
    let out = make_output_window(
        "out",
        vec![("ms", bt(BaseType::Digit)), ("s", bt(BaseType::Digit))],
    );
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (
        ms = time_to_ms(e.event_time),
        s = time_to_s(e.event_time)
    )
}
"#;
    assert_no_errors(input, &[auth_events_window(), out]);
}

#[test]
fn time_to_ms_accepts_numeric_literal() {
    let out = make_output_window("out", vec![("ms", bt(BaseType::Digit))]);
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (ms = time_to_ms(1786501210000))
}
"#;
    assert_no_errors(input, &[auth_events_window(), out]);
}

#[test]
fn time_to_ms_rejects_non_time_argument() {
    let out = output_window();
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = time_to_ms("not-a-time"))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), out],
        "time_to_ms() argument must be time or numeric",
    );
}

#[test]
fn time_to_ms_requires_exactly_one_argument() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = time_to_ms())
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "time_to_ms() requires exactly 1 argument",
    );
}
