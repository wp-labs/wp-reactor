mod contracts;
mod edge_cases;
mod func_params;
mod keys;
mod labels;
mod scope;
mod t14_t51;
mod t5_threshold;
mod type_check;
mod yield_check;
mod yield_version;

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
