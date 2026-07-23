use super::*;

#[test]
fn hash_and_id_functions_valid() {
    let out = make_output_window(
        "out",
        vec![
            ("x", bt(BaseType::Ip)),
            ("md5_v", bt(BaseType::Chars)),
            ("sha1_v", bt(BaseType::Chars)),
            ("sha1_n_v", bt(BaseType::Chars)),
            ("sha256_v", bt(BaseType::Chars)),
            ("hex_v", bt(BaseType::Hex)),
            ("short_v", bt(BaseType::Chars)),
            ("join_v", bt(BaseType::Chars)),
            ("join_by_v", bt(BaseType::Chars)),
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
        sha1_n_v = sha1_n(e.action, 8),
        sha256_v = sha256(e.action),
        hex_v = hex(e.action),
        short_v = substr(sha256(e.action), 1, 16),
        join_v = join(e.action, "", e.count, e.sip),
        join_by_v = join_by("|", e.action, "", e.count, e.sip),
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
            ("sha1_n_v", bt(BaseType::Chars)),
            ("join_by_v", bt(BaseType::Chars)),
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
        sha1_n_v = sha1_n(e.count, e.action),
        join_by_v = join_by(e.count, e.action),
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
        &[auth_events_window(), out.clone()],
        "sha1_n() first argument must be chars",
    );
    assert_has_error(
        input,
        &[auth_events_window(), out.clone()],
        "sha1_n() second argument must be numeric",
    );
    assert_has_error(
        input,
        &[auth_events_window(), out.clone()],
        "join_by() separator must be chars",
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
            ("sha1_n_v", bt(BaseType::Chars)),
            ("join_v", bt(BaseType::Chars)),
            ("join_by_v", bt(BaseType::Chars)),
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
        sha1_n_v = sha1_n(e.action),
        join_v = join(),
        join_by_v = join_by("|"),
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
        "sha1_n() requires exactly 2 arguments",
    );
    assert_has_error(
        input,
        &[auth_events_window(), out.clone()],
        "join() requires at least 1 argument",
    );
    assert_has_error(
        input,
        &[auth_events_window(), out.clone()],
        "join_by() requires at least 2 arguments",
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

#[test]
fn sha1_n_rejects_invalid_literal_lengths() {
    let out = make_output_window(
        "out",
        vec![
            ("zero_v", bt(BaseType::Chars)),
            ("too_long_v", bt(BaseType::Chars)),
            ("fraction_v", bt(BaseType::Chars)),
        ],
    );
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (
        zero_v = sha1_n(e.action, 0),
        too_long_v = sha1_n(e.action, 41),
        fraction_v = sha1_n(e.action, 1.5)
    )
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), out],
        "sha1_n() length must be an integer from 1 to 40",
    );
}

#[test]
fn join_functions_reject_array_and_object_values() {
    let out = make_output_window(
        "out",
        vec![
            ("join_v", bt(BaseType::Chars)),
            ("join_by_v", bt(BaseType::Chars)),
        ],
    );
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (
        join_v = join(array ["ssh", e.action]),
        join_by_v = join_by("|", object { source = e.action; })
    )
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), out.clone()],
        "join() argument 1 must be scalar",
    );
    assert_has_error(
        input,
        &[auth_events_window(), out],
        "join_by() argument 2 must be scalar",
    );
}
