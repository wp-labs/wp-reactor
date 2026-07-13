use super::*;

#[test]
fn hash_and_id_functions_valid() {
    let out = make_output_window(
        "out",
        vec![
            ("x", bt(BaseType::Ip)),
            ("md5_v", bt(BaseType::Chars)),
            ("sha1_v", bt(BaseType::Chars)),
            ("sha256_v", bt(BaseType::Chars)),
            ("hex_v", bt(BaseType::Hex)),
            ("short_v", bt(BaseType::Chars)),
            ("stable_v", bt(BaseType::Chars)),
        ],
    );
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (
        md5_v = md5(e.action),
        sha1_v = sha1(e.action),
        sha256_v = sha256(e.action),
        hex_v = hex(e.action),
        short_v = substr(sha256(e.action), 1, 16),
        stable_v = stable_id("alert_", e.sip, e.count, e.event_time)
    )
}
"#;
    assert_no_errors(input, &[auth_events_window(), out]);
}

#[test]
fn hash_functions_reject_wrong_types() {
    let out = make_output_window(
        "out",
        vec![
            ("md5_v", bt(BaseType::Chars)),
            ("stable_v", bt(BaseType::Chars)),
        ],
    );
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (
        md5_v = md5(e.count),
        stable_v = stable_id(e.count, e.action)
    )
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), out.clone()],
        "md5() argument must be chars",
    );
    assert_has_error(
        input,
        &[auth_events_window(), out],
        "stable_id() prefix must be chars",
    );
}

#[test]
fn hash_functions_reject_wrong_arg_counts() {
    let out = make_output_window(
        "out",
        vec![
            ("md5_v", bt(BaseType::Chars)),
            ("hex_v", bt(BaseType::Hex)),
            ("stable_v", bt(BaseType::Chars)),
        ],
    );
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (
        md5_v = md5(),
        hex_v = hex(e.action, e.user),
        stable_v = stable_id("alert_")
    )
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), out.clone()],
        "md5() requires exactly 1 argument",
    );
    assert_has_error(
        input,
        &[auth_events_window(), out.clone()],
        "hex() requires exactly 1 argument",
    );
    assert_has_error(
        input,
        &[auth_events_window(), out],
        "stable_id() requires at least 2 arguments",
    );
}
