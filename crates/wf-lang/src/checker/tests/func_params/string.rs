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
