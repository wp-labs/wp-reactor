use super::*;

// =========================================================================
// Cross-segment label uniqueness
// =========================================================================

#[test]
fn duplicate_label_across_event_and_close() {
    let input = r#"
rule r {
    events { a : auth_events  b : fw_events }
    match<sip:5m> {
        on event {
            lbl: a | count >= 1;
        }
        on close {
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
fn distinct_labels_across_event_and_close() {
    let input = r#"
rule r {
    events { a : auth_events  b : fw_events }
    match<sip:5m> {
        on event {
            evt_lbl: a | count >= 1;
        }
        on close {
            cls_lbl: b | count >= 1;
        }
    } -> score(50.0)
    entity(ip, a.sip)
    yield out (x = a.sip)
}
"#;
    assert_no_errors(
        input,
        &[auth_events_window(), fw_events_window(), output_window()],
    );
}

// =========================================================================
// Label vs key name collision
// =========================================================================

#[test]
fn label_conflicts_with_match_key() {
    // Label "sip" shadows the match key "sip" -> should be rejected
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> {
        on event {
            sip: e | count >= 1;
        }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "conflicts with match key field",
    );
}

#[test]
fn label_does_not_conflict_with_unrelated_key() {
    // Label "fail" does not conflict with key "sip" -> should pass
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> {
        on event {
            fail: e | count >= 1;
        }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_no_errors(input, &[auth_events_window(), output_window()]);
}
