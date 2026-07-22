use super::*;

#[test]
fn coalesce_allows_mixed_args_for_target_coercion() {
    let out = make_output_window("out", vec![("x", bt(BaseType::Chars))]);
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = coalesce(e.user, e.action, e.sip, e.count))
}
"#;
    assert_no_errors(input, &[auth_events_window(), out]);
}

#[test]
fn coalesce_allows_mixed_args_when_each_arg_matches_yield_target() {
    let out = make_output_window("out", vec![("ts", bt(BaseType::Time))]);
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (ts = coalesce(e.event_time, e.count))
}
"#;
    assert_no_errors(input, &[auth_events_window(), out]);
}

#[test]
fn coalesce_rejects_mixed_args_not_assignable_to_yield_target() {
    let out = make_output_window("out", vec![("ts", bt(BaseType::Time))]);
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (ts = coalesce(e.event_time, e.user))
}
"#;
    assert_has_error(input, &[auth_events_window(), out], "coalesce() argument 2");
}

#[test]
fn coalesce_rejects_numeric_fallback_for_ip_yield_target() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = coalesce(e.sip, e.count))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "coalesce() argument 2",
    );
}

#[test]
fn coalesce_rejects_mixed_args_outside_yield_target_coercion() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(coalesce(e.count, e.action))
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "not compatible",
    );
}

#[test]
fn coalesce_rejects_mixed_args_when_nested_inside_yield_expression() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = coalesce(e.count, e.action) + 1)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "not compatible",
    );
}

#[test]
fn blank_functions_valid() {
    let out = make_output_window(
        "out",
        vec![
            ("x", bt(BaseType::Ip)),
            ("blank_v", bt(BaseType::Bool)),
            ("normalized_v", bt(BaseType::Chars)),
            ("default_v", bt(BaseType::Chars)),
            ("coalesced_v", bt(BaseType::Chars)),
            ("trimmed_v", bt(BaseType::Chars)),
        ],
    );
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (
        blank_v = is_blank(e.user),
        normalized_v = null_if_blank(e.user),
        default_v = default_if_blank(e.user, "unknown"),
        coalesced_v = coalesce(null_if_blank(e.user), e.action, "unknown"),
        trimmed_v = trim(e.action)
    )
}
"#;
    assert_no_errors(input, &[auth_events_window(), out]);
}

#[test]
fn blank_functions_reject_wrong_types() {
    let out = make_output_window(
        "out",
        vec![
            ("is_blank_v", bt(BaseType::Bool)),
            ("default_v", bt(BaseType::Chars)),
        ],
    );
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (
        is_blank_v = is_blank(e.count),
        default_v = default_if_blank(e.user, e.count)
    )
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), out.clone()],
        "is_blank() argument must be chars",
    );
    assert_has_error(
        input,
        &[auth_events_window(), out],
        "default_if_blank() argument 2 must be chars",
    );
}
