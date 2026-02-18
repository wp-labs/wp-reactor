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
