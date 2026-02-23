use super::*;

#[test]
fn duplicate_event_alias() {
    let input = r#"
rule r {
    events { a : auth_events  a : fw_events }
    match<sip:5m> { on event { a | count >= 1; } } -> score(50.0)
    entity(ip, a.sip)
    yield out (x = a.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), fw_events_window(), output_window()],
        "duplicate event alias `a`",
    );
}

#[test]
fn unknown_event_window() {
    let input = r#"
rule r {
    events { a : nonexistent }
    match<:5m> { on event { a | count >= 1; } } -> score(50.0)
    entity(ip, a.sip)
    yield out (x = a.sip)
}
"#;
    assert_has_error(input, &[output_window()], "unknown window `nonexistent`");
}

#[test]
fn unknown_step_source() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { bad | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "not a declared event alias",
    );
}

#[test]
fn duplicate_step_label() {
    let input = r#"
rule r {
    events { a : auth_events  b : fw_events }
    match<sip:5m> {
        on event {
            lbl: a | count >= 1;
            lbl: b | count >= 1;
        }
    } -> score(50.0)
    entity(ip, a.sip)
    yield out (x = a.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), fw_events_window(), output_window()],
        "duplicate step label `lbl`",
    );
}

#[test]
fn valid_rule_passes() {
    let input = r#"
rule brute_force {
    events {
        fail : auth_events && action == "failed"
    }
    match<sip:5m> {
        on event {
            fail | count >= 3;
        }
    } -> score(70.0)
    entity(ip, fail.sip)
    yield security_alerts (
        sip = fail.sip,
        fail_count = count(fail),
        message = fmt("{} failed {} times", fail.sip, count(fail))
    )
}
"#;
    assert_no_errors(input, &[auth_events_window(), security_alerts_window()]);
}
