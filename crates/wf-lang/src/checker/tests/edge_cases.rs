use super::*;

#[test]
fn valid_two_source_rule() {
    let input = r#"
rule brute_force_then_scan {
    events {
        fail : auth_events && action == "failed"
        scan : fw_events
    }
    match<sip:5m> {
        on event {
            fail | count >= 3;
            scan.dport | distinct | count > 10;
        }
    } -> score(80.0)
    entity(ip, fail.sip)
    yield security_alerts (
        sip = fail.sip,
        fail_count = count(fail)
    )
}
"#;
    assert_no_errors(
        input,
        &[
            auth_events_window(),
            fw_events_window(),
            security_alerts_window(),
        ],
    );
}

#[test]
fn field_selector_not_in_source_window() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> {
        on event { e.nonexistent | distinct | count >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "not found in source",
    );
}

#[test]
fn yield_numeric_to_digit_field_ok() {
    // Assigning a numeric literal to a digit field should be fine
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = 42)
}
"#;
    assert_no_errors(input, &[auth_events_window(), output_window()]);
}

#[test]
fn yield_multiple_wfu_reserved_fields() {
    let out = make_output_window(
        "out",
        vec![
            ("__wfu_entity_id", bt(BaseType::Chars)),
            ("__wfu_score", bt(BaseType::Float)),
        ],
    );
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (__wfu_entity_id = "foo", __wfu_score = 50.5)
}
"#;
    let errs = check_errors(input, &[auth_events_window(), out]);
    let system_errors: Vec<_> = errs
        .iter()
        .filter(|e| e.contains("reserved prefix"))
        .collect();
    assert_eq!(
        system_errors.len(),
        2,
        "expected 2 reserved prefix errors, got: {:?}",
        system_errors
    );
}

// ---------------------------------------------------------------------------
// on event<accu> — within-window accumulation constraints
// ---------------------------------------------------------------------------

#[test]
fn accu_bare_single_step_passes() {
    let input = r#"
rule r {
    events { s : auth_events }
    match<:5m> {
        on event<accu> { s | count >= 2; }
    } -> score(50.0)
    entity(ip, s.sip)
    yield out (x = s.sip)
}
"#;
    assert_no_errors(input, &[auth_events_window(), output_window()]);
}

#[test]
fn accu_with_close_block_rejected() {
    let input = r#"
rule r {
    events { s : auth_events }
    match<:5m> {
        on event<accu> { s | count >= 2; }
        on close { s | count >= 3; }
    } -> score(50.0)
    entity(ip, s.sip)
    yield out (x = s.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "on event<accu> is not supported together with an `on close` / `and close` block",
    );
}

#[test]
fn accu_with_seq_chain_rejected() {
    let input = r#"
rule r {
    events { s : auth_events }
    match<:5m> {
        on event<accu> seq { s | count >= 2; }
    } -> score(50.0)
    entity(ip, s.sip)
    yield out (x = s.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "on event<accu> is not supported with `on event seq { ... }` chain syntax",
    );
}

#[test]
fn accu_multi_step_rejected() {
    let input = r#"
rule r {
    events { a : auth_events  b : auth_events }
    match<:5m> {
        on event<accu> { a | count >= 1;  b | count >= 1; }
    } -> score(50.0)
    entity(ip, a.sip)
    yield out (x = a.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "on event<accu> requires exactly one step",
    );
}

#[test]
fn accu_multi_branch_rejected() {
    // A single step with multiple OR branches has undefined accu semantics.
    let input = r#"
rule r {
    events { s : auth_events }
    match<:5m> {
        on event<accu> { s | count >= 2 || s | count >= 3; }
    } -> score(50.0)
    entity(ip, s.sip)
    yield out (x = s.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "on event<accu> requires exactly one step with a single branch",
    );
}
