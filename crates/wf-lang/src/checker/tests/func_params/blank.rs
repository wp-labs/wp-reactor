use super::*;

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
