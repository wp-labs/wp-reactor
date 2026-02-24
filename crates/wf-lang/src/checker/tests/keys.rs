use super::*;

#[test]
fn key_field_not_in_all_sources() {
    // "dport" only exists in fw_events, not in auth_events
    let input = r#"
rule r {
    events { a : auth_events  b : fw_events }
    match<dport:5m> {
        on event {
            a | count >= 1;
        }
    } -> score(50.0)
    entity(ip, a.sip)
    yield out (x = a.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), fw_events_window(), output_window()],
        "match key `dport` not found in event source `a`",
    );
}

#[test]
fn key_type_mismatch() {
    // Create two windows where 'sip' has different types.
    let w1 = make_window("win1", vec!["s"], vec![("sip", bt(BaseType::Ip))]);
    let w2 = make_window("win2", vec!["s"], vec![("sip", bt(BaseType::Chars))]);
    let out = output_window();

    let input = r#"
rule r {
    events { a : win1  b : win2 }
    match<sip:5m> {
        on event { a | count >= 1; }
    } -> score(50.0)
    entity(ip, a.sip)
    yield out (x = a.sip)
}
"#;
    assert_has_error(input, &[w1, w2, out], "type mismatch");
}

#[test]
fn qualified_key_valid() {
    let input = r#"
rule r {
    events { fail : auth_events }
    match<fail.sip:5m> {
        on event { fail | count >= 1; }
    } -> score(50.0)
    entity(ip, fail.sip)
    yield out (x = fail.sip)
}
"#;
    assert_no_errors(input, &[auth_events_window(), output_window()]);
}

#[test]
fn key_field_not_in_window() {
    let input = r#"
rule r {
    events { fail : auth_events }
    match<fail.nonexistent:5m> {
        on event { fail | count >= 1; }
    } -> score(50.0)
    entity(ip, fail.sip)
    yield out (x = fail.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "field `nonexistent` not found",
    );
}
