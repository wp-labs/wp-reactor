use super::*;

// =========================================================================
// T14: if-then-else type checks
// =========================================================================

#[test]
fn t14_if_then_else_valid() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } }
    -> score(if e.action == "failed" then 80.0 else 40.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_no_errors(input, &[auth_events_window(), output_window()]);
}

#[test]
fn t14_if_cond_not_bool() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } }
    -> score(if e.action then 80.0 else 40.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "condition must be bool",
    );
}

#[test]
fn t14_if_branch_type_mismatch() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (
        y = if e.action == "ok" then 42.0 else "text"
    )
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "incompatible types",
    );
}

// =========================================================================
// T51: yield version consistency
// =========================================================================

#[test]
fn t51_version_matches_meta() {
    let input = r#"
rule r {
    meta { contract_version = "2" }
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out@v2 (x = e.sip)
}
"#;
    assert_no_errors(input, &[auth_events_window(), output_window()]);
}

#[test]
fn t51_version_mismatch() {
    let input = r#"
rule r {
    meta { contract_version = "2" }
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out@v3 (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "does not match meta contract_version",
    );
}

#[test]
fn t51_version_no_meta() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out@v1 (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "no contract_version in meta",
    );
}
