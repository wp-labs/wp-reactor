use super::*;

// ---------------------------------------------------------------------------
// Zero-value rejection
// ---------------------------------------------------------------------------

#[test]
fn check_limits_max_instances_zero_rejected() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    limits { max_instances = 0; on_exceed = throttle; }
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "max_instances",
    );
}

#[test]
fn check_limits_max_instances_positive_accepted() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    limits { max_instances = 1; on_exceed = throttle; }
}
"#;
    assert_no_errors(input, &[auth_events_window(), output_window()]);
}

#[test]
fn check_limits_max_memory_zero_rejected() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    limits { max_memory = "0MB"; on_exceed = throttle; }
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "max_memory",
    );
}

#[test]
fn check_limits_max_throttle_zero_count_rejected() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    limits { max_throttle = "0/min"; on_exceed = throttle; }
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "max_throttle",
    );
}

// ---------------------------------------------------------------------------
// Overflow rejection
// ---------------------------------------------------------------------------

#[test]
fn check_limits_max_memory_overflow_rejected() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    limits { max_memory = "18446744073709551615GB"; on_exceed = throttle; }
}
"#;
    assert_has_error(input, &[auth_events_window(), output_window()], "overflows");
}

// ---------------------------------------------------------------------------
// spill（M4）
// ---------------------------------------------------------------------------

#[test]
fn check_limits_spill_redb_accepted() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    limits { max_memory = "1GB"; spill = "redb"; max_spill_bytes = "8GB"; }
}
"#;
    assert_no_errors(input, &[auth_events_window(), output_window()]);
}

#[test]
fn check_limits_spill_unknown_value_rejected() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    limits { spill = "rocksdb"; }
}
"#;
    assert_has_error(input, &[auth_events_window(), output_window()], "spill");
}

#[test]
fn check_limits_spill_max_bytes_bad_format_rejected() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    limits { spill = "redb"; max_spill_bytes = "lots"; }
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "max_spill_bytes",
    );
}
