use super::*;

use crate::wfl_parser::parse_wfl;

// =========================================================================
// T5: Threshold type compatibility — unit tests
// =========================================================================

#[test]
fn min_chars_vs_numeric_threshold() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> {
        on event { e.action | min >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "not compatible",
    );
}

#[test]
fn max_chars_vs_numeric_threshold() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> {
        on event { e.action | max >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "not compatible",
    );
}

#[test]
fn min_digit_vs_numeric_threshold() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> {
        on event { e.count | min >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_no_errors(input, &[auth_events_window(), output_window()]);
}

#[test]
fn min_chars_vs_string_threshold() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> {
        on event { e.action | min >= "abc"; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_no_errors(input, &[auth_events_window(), output_window()]);
}

#[test]
fn count_vs_string_threshold() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> {
        on event { e | count >= "abc"; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "not compatible",
    );
}

// =========================================================================
// T5: Threshold type compatibility — integration tests
// =========================================================================

/// Integration: T5 error propagates through compile_wfl, rejecting the rule.
#[test]
fn t5_compile_rejects_min_chars_numeric() {
    let input = r#"
rule hostname_anomaly {
    events { e : auth_events && action == "login" }
    match<sip:5m> {
        on event {
            e.user | min >= 1;
        }
    } -> score(60.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let errs = check_errors(input, &[auth_events_window(), output_window()]);
    assert!(
        errs.iter()
            .any(|e| e.contains("not compatible") && e.contains("min()")),
        "expected T5 error for min(Chars) vs Digit threshold, got: {:?}",
        errs
    );
}

/// Integration: a multi-branch rule where only one branch has a T5 mismatch.
#[test]
fn t5_multi_branch_only_bad_branch_errors() {
    let input = r#"
rule multi_measure {
    events { e : auth_events }
    match<sip:5m> {
        on event {
            e | count >= 3;
            e.count | sum >= 10;
            e.action | max >= 100;
        }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let errs = check_errors(input, &[auth_events_window(), output_window()]);
    let t5_errors: Vec<_> = errs
        .iter()
        .filter(|e| e.contains("not compatible"))
        .collect();
    assert_eq!(
        t5_errors.len(),
        1,
        "expected exactly 1 T5 error (from the max(Chars) branch), got: {:?}",
        t5_errors
    );
    assert!(
        t5_errors[0].contains("max()"),
        "T5 error should mention max(), got: {}",
        t5_errors[0]
    );
}

/// Integration: T5 catches mismatch when threshold is a float literal.
#[test]
fn t5_float_threshold_vs_chars_caught() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> {
        on event { e.action | min >= 1.5; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "not compatible",
    );
}

/// Integration: Time field with min — string literal threshold is Chars vs Time.
#[test]
fn t5_min_time_vs_string_threshold() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> {
        on event { e.event_time | min >= "2024-01-01"; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "not compatible",
    );
}

/// Integration: avg() always returns Float. A Digit threshold is compatible.
#[test]
fn t5_avg_digit_threshold_compatible() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> {
        on event { e.count | avg >= 5; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_no_errors(input, &[auth_events_window(), output_window()]);
}

/// Integration: avg() returns Float, but a Chars threshold is incompatible.
#[test]
fn t5_avg_string_threshold_rejected() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> {
        on event { e.count | avg >= "high"; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "not compatible",
    );
}

// =========================================================================
// T5: Illustrative example — walkthrough of the check data flow
// =========================================================================

#[test]
fn t5_example_walkthrough() {
    let input = r#"
rule bad_hostname_check {
    events { e : auth_events && action == "login" }
    match<sip:5m> {
        on event {
            e.action | min >= 1;
        }
    } -> score(40.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).expect("rule is syntactically valid");

    let schemas = &[auth_events_window(), output_window()];
    let errs = check_wfl(&file, schemas);

    let t5_errs: Vec<_> = errs
        .iter()
        .filter(|e| e.message.contains("not compatible"))
        .collect();
    assert_eq!(
        t5_errs.len(),
        1,
        "expected exactly 1 T5 error, got: {:?}",
        t5_errs
    );

    let msg = &t5_errs[0].message;
    assert!(
        msg.contains("min()"),
        "error should name the measure: {msg}"
    );
    assert!(
        msg.contains("Chars"),
        "error should mention the result type Chars: {msg}"
    );
    assert!(
        msg.contains("Digit"),
        "error should mention the threshold type Digit: {msg}"
    );

    assert_eq!(
        t5_errs[0].rule.as_deref(),
        Some("bad_hostname_check"),
        "error should be attributed to the rule"
    );
}
