use super::*;

#[test]
fn stat_context_functions_valid() {
    let out = make_output_window(
        "out",
        vec![
            ("x", bt(BaseType::Ip)),
            ("window_events", bt(BaseType::Digit)),
            ("matched_events", bt(BaseType::Digit)),
            ("distinct_ports", bt(BaseType::Digit)),
            ("trigger_count", bt(BaseType::Float)),
            ("final_count", bt(BaseType::Float)),
        ],
    );
    let input = r#"
rule r {
    events { auth : auth_events  net : fw_events }
    match<sip:5m> {
        on event {
            fail: auth | count >= 1;
            port_scan: net.dport | distinct | count >= 2;
        }
        and close {
            final_ports: net.dport | distinct | count >= 1;
        }
    } -> score(50.0)
    entity(ip, auth.sip)
    yield out (
        window_events = stat.count(window_event(auth)),
        matched_events = stat.count(match_event(fail)),
        distinct_ports = stat.count(match_distinct(port_scan)),
        trigger_count = stat.value(trigger(port_scan)),
        final_count = stat.value(final(final_ports))
    )
}
"#;
    assert_no_errors(input, &[auth_events_window(), fw_events_window(), out]);
}

#[test]
fn stat_context_rejects_unknown_alias() {
    let input = r#"
rule r {
    events { auth : auth_events }
    match<sip:5m> { on event { fail: auth | count >= 1; } } -> score(50.0)
    entity(ip, auth.sip)
    yield out (n = stat.count(window_event(missing)))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "unknown event alias `missing`",
    );
}

#[test]
fn stat_context_rejects_unknown_label() {
    let input = r#"
rule r {
    events { auth : auth_events }
    match<sip:5m> { on event { fail: auth | count >= 1; } } -> score(50.0)
    entity(ip, auth.sip)
    yield out (n = stat.count(match_event(missing)))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "unknown step label `missing`",
    );
}

#[test]
fn stat_context_rejects_distinct_on_non_distinct_label() {
    let input = r#"
rule r {
    events { auth : auth_events }
    match<sip:5m> { on event { fail: auth | count >= 1; } } -> score(50.0)
    entity(ip, auth.sip)
    yield out (n = stat.count(match_distinct(fail)))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "requires step label `fail` to use distinct",
    );
}

#[test]
fn stat_context_rejects_match_distinct_on_non_count_measure() {
    let input = r#"
rule r {
    events { auth : auth_events }
    match<sip:5m> { on event { ports: auth.count | distinct | sum >= 2; } } -> score(50.0)
    entity(ip, auth.sip)
    yield out (n = stat.count(match_distinct(ports)))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "requires step label `ports` to use distinct | count",
    );
}

#[test]
fn stat_context_rejects_match_event_on_close_label() {
    let input = r#"
rule r {
    events { auth : auth_events }
    match<sip:5m> {
        on event { fail: auth | count >= 1; }
        and close { final_fail: auth | count >= 1; }
    } -> score(50.0)
    entity(ip, auth.sip)
    yield out (n = stat.count(match_event(final_fail)))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "requires step label `final_fail` to come from on event",
    );
}

#[test]
fn stat_context_rejects_match_distinct_on_close_label() {
    let input = r#"
rule r {
    events { auth : auth_events }
    match<sip:5m> {
        on event { fail: auth | count >= 1; }
        and close { final_ports: auth.count | distinct | count >= 1; }
    } -> score(50.0)
    entity(ip, auth.sip)
    yield out (n = stat.count(match_distinct(final_ports)))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "requires step label `final_ports` to come from on event",
    );
}

#[test]
fn stat_context_rejects_trigger_on_close_label() {
    let input = r#"
rule r {
    events { auth : auth_events }
    match<sip:5m> {
        on event { fail: auth | count >= 1; }
        and close { final_fail: auth | count >= 1; }
    } -> score(50.0)
    entity(ip, auth.sip)
    yield out (n = stat.value(trigger(final_fail)))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "requires step label `final_fail` to come from on event",
    );
}

#[test]
fn stat_context_rejects_final_on_event_label() {
    let input = r#"
rule r {
    events { auth : auth_events }
    match<sip:5m> {
        on event { fail: auth | count >= 1; }
        and close { final_fail: auth | count >= 1; }
    } -> score(50.0)
    entity(ip, auth.sip)
    yield out (n = stat.value(final(fail)))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "requires step label `fail` to come from on close",
    );
}

#[test]
fn stat_context_rejects_final_with_or_close() {
    let input = r#"
rule r {
    events { auth : auth_events }
    match<sip:5m> {
        on event { fail: auth | count >= 1; }
        on close { final_fail: auth | count >= 1; }
    } -> score(50.0)
    entity(ip, auth.sip)
    yield out (n = stat.value(final(final_fail)))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "uses stat.value(final(...)) with `on close`; use `and close`",
    );
}

#[test]
fn stat_context_rejects_quoted_symbol() {
    let input = r#"
rule r {
    events { auth : auth_events }
    match<sip:5m> { on event { fail: auth | count >= 1; } } -> score(50.0)
    entity(ip, auth.sip)
    yield out (n = stat.count(window_event("auth")))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "requires a static symbol argument",
    );
}

#[test]
fn stat_selector_cannot_be_used_bare() {
    let input = r#"
rule r {
    events { auth : auth_events }
    match<sip:5m> { on event { fail: auth | count >= 1; } } -> score(50.0)
    entity(ip, auth.sip)
    yield out (n = window_event(auth))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "can only be used inside stat.count",
    );
}

#[test]
fn stat_context_rejects_outside_yield() {
    let input = r#"
rule r {
    events { auth : auth_events }
    match<sip:5m> { on event { fail: auth | count >= 1; } } -> score(stat.count(window_event(auth)))
    entity(ip, auth.sip)
    yield out (n = 1)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "stat functions are only allowed in `yield` expressions",
    );
}
