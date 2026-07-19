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
