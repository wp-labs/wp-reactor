use std::time::Duration;

use crate::check_wfl;
use crate::schema::{BaseType, FieldDef, FieldType, WindowSchema};
use crate::wfl_parser::parse_wfl;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Create a minimal WindowSchema with given name, streams, and fields.
fn make_window(name: &str, streams: Vec<&str>, fields: Vec<(&str, FieldType)>) -> WindowSchema {
    WindowSchema {
        name: name.to_string(),
        streams: streams.into_iter().map(String::from).collect(),
        time_field: Some("event_time".to_string()),
        over: Duration::from_secs(3600),
        fields: fields
            .into_iter()
            .map(|(n, ft)| FieldDef {
                name: n.to_string(),
                field_type: ft,
            })
            .collect(),
    }
}

/// Create an output-only window (no streams).
fn make_output_window(name: &str, fields: Vec<(&str, FieldType)>) -> WindowSchema {
    WindowSchema {
        name: name.to_string(),
        streams: vec![],
        time_field: None,
        over: Duration::from_secs(3600),
        fields: fields
            .into_iter()
            .map(|(n, ft)| FieldDef {
                name: n.to_string(),
                field_type: ft,
            })
            .collect(),
    }
}

fn bt(b: BaseType) -> FieldType {
    FieldType::Base(b)
}

/// Standard auth_events window for tests.
fn auth_events_window() -> WindowSchema {
    make_window(
        "auth_events",
        vec!["auth_stream"],
        vec![
            ("sip", bt(BaseType::Ip)),
            ("dip", bt(BaseType::Ip)),
            ("action", bt(BaseType::Chars)),
            ("user", bt(BaseType::Chars)),
            ("count", bt(BaseType::Digit)),
            ("event_time", bt(BaseType::Time)),
        ],
    )
}

/// Standard fw_events window for tests.
fn fw_events_window() -> WindowSchema {
    make_window(
        "fw_events",
        vec!["fw_stream"],
        vec![
            ("sip", bt(BaseType::Ip)),
            ("dip", bt(BaseType::Ip)),
            ("dport", bt(BaseType::Digit)),
            ("event_time", bt(BaseType::Time)),
        ],
    )
}

/// Standard output window for tests.
fn output_window() -> WindowSchema {
    make_output_window(
        "out",
        vec![
            ("x", bt(BaseType::Ip)),
            ("y", bt(BaseType::Chars)),
            ("n", bt(BaseType::Digit)),
        ],
    )
}

/// Standard security_alerts output window.
fn security_alerts_window() -> WindowSchema {
    make_output_window(
        "security_alerts",
        vec![
            ("sip", bt(BaseType::Ip)),
            ("fail_count", bt(BaseType::Digit)),
            ("port_count", bt(BaseType::Digit)),
            ("message", bt(BaseType::Chars)),
        ],
    )
}

/// Helper: check and return only the error messages for readability.
fn check_errors(input: &str, schemas: &[WindowSchema]) -> Vec<String> {
    let file = parse_wfl(input).expect("parse should succeed");
    let errs = check_wfl(&file, schemas);
    errs.into_iter().map(|e| e.message).collect()
}

/// Helper: assert that checking produces no errors.
fn assert_no_errors(input: &str, schemas: &[WindowSchema]) {
    let errs = check_errors(input, schemas);
    assert!(errs.is_empty(), "expected no errors, got: {:?}", errs);
}

/// Helper: assert that at least one error message contains the given substring.
fn assert_has_error(input: &str, schemas: &[WindowSchema], substring: &str) {
    let errs = check_errors(input, schemas);
    assert!(
        errs.iter().any(|e| e.contains(substring)),
        "expected an error containing {:?}, got: {:?}",
        substring,
        errs
    );
}

// =========================================================================
// Name/scope tests
// =========================================================================

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
    assert_has_error(input, &[auth_events_window(), fw_events_window(), output_window()], "duplicate event alias `a`");
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
    assert_no_errors(
        input,
        &[auth_events_window(), security_alerts_window()],
    );
}

// =========================================================================
// Match key tests
// =========================================================================

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

// =========================================================================
// Yield tests
// =========================================================================

#[test]
fn yield_unknown_target() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield nonexistent (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window()],
        "yield target window `nonexistent` does not exist",
    );
}

#[test]
fn yield_target_has_stream() {
    // auth_events has streams, so it shouldn't be a yield target.
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield auth_events (sip = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window()],
        "has stream subscriptions",
    );
}

#[test]
fn yield_system_field() {
    let out = make_output_window("out", vec![("score", bt(BaseType::Float))]);
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (score = 50.0)
}
"#;
    assert_has_error(input, &[auth_events_window(), out], "system field");
}

#[test]
fn yield_unknown_field() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (nonexistent = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "not a field in target window",
    );
}

#[test]
fn yield_type_mismatch() {
    // 'x' in out is ip, but we assign a digit
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = 42)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "type mismatch",
    );
}

// =========================================================================
// Type checking tests
// =========================================================================

#[test]
fn sum_on_chars_field() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> {
        on event { e.action | sum >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "requires a numeric field",
    );
}

#[test]
fn count_on_column() {
    // count with a field selector (without distinct) should error
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> {
        on event { e.action | count >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "use `distinct | count`",
    );
}

#[test]
fn distinct_on_set() {
    // distinct without a field selector should error
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> {
        on event { e | distinct | count >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "distinct requires a field selector",
    );
}

#[test]
fn score_non_numeric() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score("text")
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "score expression must be numeric",
    );
}

#[test]
fn entity_id_bool() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, true)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "scalar identity type",
    );
}

#[test]
fn compare_mismatched_types() {
    // sip is Ip, 42 is Digit — incompatible for ==
    let input = r#"
rule r {
    events { e : auth_events && sip == 42 }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "incompatible types",
    );
}

#[test]
fn logic_on_non_bool() {
    // sip || dip — both are Ip, not Bool
    let input = r#"
rule r {
    events { e : auth_events && sip || dip }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "requires bool operands",
    );
}

// =========================================================================
// Contract tests
// =========================================================================

#[test]
fn contract_unknown_rule() {
    let input = r#"
contract ct for nonexistent {
    given { row(e, x = 1); }
    expect { hits == 1; }
}
"#;
    let file = parse_wfl(input).unwrap();
    let errs = check_wfl(&file, &[]);
    assert!(
        errs.iter().any(|e| e.message.contains("not found")),
        "expected error about unknown rule, got: {:?}",
        errs
    );
}

#[test]
fn contract_unknown_alias() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
contract ct for r {
    given { row(bad, x = 1); }
    expect { hits == 1; }
}
"#;
    let file = parse_wfl(input).unwrap();
    let errs = check_wfl(&file, &[auth_events_window(), output_window()]);
    assert!(
        errs.iter().any(|e| e.message.contains("not declared")),
        "expected error about unknown alias in contract, got: {:?}",
        errs
    );
}

#[test]
fn contract_valid() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
contract ct for r {
    given { row(e, action = "failed"); }
    expect { hits == 1; }
}
"#;
    let file = parse_wfl(input).unwrap();
    let errs = check_wfl(&file, &[auth_events_window(), output_window()]);
    // The rule itself is valid and contract refs are valid
    assert!(
        errs.is_empty(),
        "expected no errors, got: {:?}",
        errs
    );
}

// =========================================================================
// Additional edge case tests
// =========================================================================

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
        &[auth_events_window(), fw_events_window(), security_alerts_window()],
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
fn yield_multiple_system_fields() {
    let out = make_output_window(
        "out",
        vec![
            ("entity_id", bt(BaseType::Chars)),
            ("score_contrib", bt(BaseType::Chars)),
        ],
    );
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (entity_id = "foo", score_contrib = "bar")
}
"#;
    let errs = check_errors(input, &[auth_events_window(), out]);
    let system_errors: Vec<_> = errs.iter().filter(|e| e.contains("system field")).collect();
    assert_eq!(
        system_errors.len(),
        2,
        "expected 2 system field errors, got: {:?}",
        system_errors
    );
}

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
//
// These tests verify T5 works correctly in realistic, multi-branch rules
// and through the full parse → check pipeline, covering edge cases beyond
// the basic unit tests above.

/// Integration: T5 error propagates through compile_wfl, rejecting the rule.
///
/// Demonstrates the original silent-failure scenario:
///   `min(hostname) > 1` — hostname is Chars, threshold is Digit.
/// Previously this passed the checker and silently never matched at runtime
/// (compare_value_threshold returned false for cross-type).
/// Now the checker catches it at compile time.
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
    // The checker must report the incompatibility, not silently pass.
    let errs = check_errors(input, &[auth_events_window(), output_window()]);
    assert!(
        errs.iter().any(|e| e.contains("not compatible") && e.contains("min()")),
        "expected T5 error for min(Chars) vs Digit threshold, got: {:?}",
        errs
    );
}

/// Integration: a multi-branch rule where only one branch has a T5 mismatch.
///
/// Schema recap (auth_events):
///   - action: Chars, count: Digit, event_time: Time, sip/dip: Ip
///
/// Branch 1: `e | count >= 3`        → count()→Digit vs 3(Digit)    ✓ compatible
/// Branch 2: `e.count | sum >= 10`    → sum(Digit)→Digit vs 10(Digit)✓ compatible
/// Branch 3: `e.action | max >= 100`  → max(Chars)→Chars vs 100(Digit)✗ T5 error
///
/// Only branch 3 should trigger a T5 error. The other branches remain valid.
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
    let t5_errors: Vec<_> = errs.iter().filter(|e| e.contains("not compatible")).collect();
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
///
/// `e.action | min >= 1.5` — threshold infers as Float,
/// but min(action) result type is Chars. The T5 check must still fire.
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
///
/// event_time is Time. min(event_time) result type → Time.
/// String literal "2024-01-01" infers as Chars.
/// Chars vs Time is incompatible → T5 error.
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

/// Integration: avg() always returns Float. A Digit threshold (e.g. 5)
/// is compatible because all numeric types (Digit, Float, Numeric) map to
/// Value::Number at runtime, so T5 allows numeric ↔ numeric pairings.
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
//
// This example demonstrates how T5 catches `min(hostname) > 1`, the
// original motivating bug. Read it as documentation of the check pipeline.
//
// Rule (simplified):
//   rule bad_hostname_check {
//       events { e : auth_events && action == "login" }
//       match<sip:5m> {
//           on event { e.action | min >= 1; }
//                      ^^^^^^^^   ^^^    ^
//                      field      measure threshold
//       } -> score(40.0)
//       ...
//   }
//
// Check data flow in check_pipe_chain():
//
//   1. Resolve field_val_type:
//      - branch.field = Dot("action")
//      - scope lookup → action is BaseType::Chars
//      - field_val_type = Some(ValType::Base(Chars))
//
//   2. Existing checks pass:
//      - T2: is_orderable(Chars) = true  ✓  (Chars is orderable)
//      - check_expr_type(threshold=1) → threshold is a Number literal ✓
//
//   3. NEW — T5 check:
//      - measure_result_type(Min, Some(Chars)) → Some(Chars)
//      - infer_type(threshold=1) → Some(Base(Digit))
//      - compatible(Chars, Digit) → false  ✗
//      - → Error: "threshold type Base(Digit) is not compatible with
//                  min() result type Base(Chars)"
//
//   Without T5, this rule would compile successfully and at runtime
//   compare_value_threshold() (match_engine.rs:644) would silently
//   return false for every event (Str vs Number), making the rule a no-op.

#[test]
fn t5_example_walkthrough() {
    // Step 1: Parse the rule — this always succeeds (syntactically valid)
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

    // Step 2: Run the semantic checker
    let schemas = &[auth_events_window(), output_window()];
    let errs = check_wfl(&file, schemas);

    // Step 3: Verify the T5 error is emitted
    let t5_errs: Vec<_> = errs
        .iter()
        .filter(|e| e.message.contains("not compatible"))
        .collect();
    assert_eq!(t5_errs.len(), 1, "expected exactly 1 T5 error, got: {:?}", t5_errs);

    let msg = &t5_errs[0].message;
    // Verify the error message contains the key diagnostic info
    assert!(msg.contains("min()"), "error should name the measure: {msg}");
    assert!(msg.contains("Chars"), "error should mention the result type Chars: {msg}");
    assert!(msg.contains("Digit"), "error should mention the threshold type Digit: {msg}");

    // Verify the error is attributed to the correct rule
    assert_eq!(
        t5_errs[0].rule.as_deref(),
        Some("bad_hostname_check"),
        "error should be attributed to the rule"
    );
}

// =========================================================================
// Cross-segment label uniqueness (Bug 2)
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
    // Label "sip" shadows the match key "sip" → should be rejected
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
    // Label "fail" does not conflict with key "sip" → should pass
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
