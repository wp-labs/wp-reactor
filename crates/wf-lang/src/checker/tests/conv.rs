use super::*;

// ---------------------------------------------------------------------------
// Conv requires fixed window
// ---------------------------------------------------------------------------

#[test]
fn conv_with_sliding_window_rejected() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    conv { sort(-count) | top(10) ; }
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "conv block requires fixed or hop window mode",
    );
}

#[test]
fn conv_with_hop_window_accepted() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:hop(10s, 2s)> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    conv { sort(-count) | top(10) ; }
}
"#;
    super::assert_no_errors(input, &[auth_events_window(), output_window()]);
}

#[test]
fn conv_top_ties_without_sort_rejected() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m:fixed> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    conv { top_ties(3) ; }
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "top_ties(N)` requires a preceding `sort`",
    );
}

#[test]
fn conv_with_fixed_window_accepted() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:1h:fixed> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    conv { sort(-count) | top(10) ; }
}
"#;
    assert_no_errors(input, &[auth_events_window(), output_window()]);
}

#[test]
fn no_conv_no_error() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    // Omitting conv is fine regardless of window mode — no conv error
    let errs = check_errors(input, &[auth_events_window(), output_window()]);
    assert!(
        !errs.iter().any(|e| e.contains("conv")),
        "unexpected conv error: {:?}",
        errs
    );
}
