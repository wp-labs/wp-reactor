use super::*;

#[test]
fn yield_unknown_target() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield nonexistent (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window()],
        "yield target window `nonexistent` does not exist",
    );
}

#[test]
fn yield_target_has_stream() {
    // auth_events has streams, so it shouldn't be a yield target.
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield auth_events (sip = e.sip)
}
"#;
    assert_has_error(input, &[auth_events_window()], "has stream subscriptions");
}

#[test]
fn yield_system_field() {
    let out = make_output_window("out", vec![("score", bt(BaseType::Float))]);
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (score = 50.0)
}
"#;
    assert_has_error(input, &[auth_events_window(), out], "system field");
}

#[test]
fn yield_unknown_field() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (nonexistent = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "not a field in target window",
    );
}

#[test]
fn yield_type_mismatch() {
    // 'x' in out is ip, but we assign a digit
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = 42)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "type mismatch",
    );
}
