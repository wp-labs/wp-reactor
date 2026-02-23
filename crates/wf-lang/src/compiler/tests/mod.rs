use std::time::Duration;

use crate::ast::*;
use crate::compiler::compile_wfl;
use crate::plan::*;
use crate::schema::{BaseType, FieldDef, FieldType, WindowSchema};
use crate::wfl_parser::parse_wfl;

mod basic;
mod edge;
mod keys_entity;
mod yield_score;

// ---------------------------------------------------------------------------
// Schema helpers
// ---------------------------------------------------------------------------

pub(super) fn bt(b: BaseType) -> FieldType {
    FieldType::Base(b)
}

pub(super) fn make_window(
    name: &str,
    streams: Vec<&str>,
    fields: Vec<(&str, FieldType)>,
) -> WindowSchema {
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

pub(super) fn make_output_window(name: &str, fields: Vec<(&str, FieldType)>) -> WindowSchema {
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

pub(super) fn auth_events_window() -> WindowSchema {
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

pub(super) fn fw_events_window() -> WindowSchema {
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

/// Generic window used by many tests as "win".
pub(super) fn generic_window() -> WindowSchema {
    make_window(
        "win",
        vec!["stream"],
        vec![
            ("sip", bt(BaseType::Ip)),
            ("dip", bt(BaseType::Ip)),
            ("dport", bt(BaseType::Digit)),
            ("action", bt(BaseType::Chars)),
            ("host", bt(BaseType::Chars)),
            ("active", bt(BaseType::Bool)),
            ("detail.sha256", bt(BaseType::Hex)),
            ("event_time", bt(BaseType::Time)),
        ],
    )
}

/// Second generic window used by tests as "win2".
pub(super) fn generic_window2() -> WindowSchema {
    make_window(
        "win2",
        vec!["stream2"],
        vec![
            ("sip", bt(BaseType::Ip)),
            ("dport", bt(BaseType::Digit)),
            ("event_time", bt(BaseType::Time)),
        ],
    )
}

pub(super) fn dns_query_window() -> WindowSchema {
    make_window(
        "dns_query",
        vec!["dns_stream"],
        vec![
            ("query_id", bt(BaseType::Chars)),
            ("sip", bt(BaseType::Ip)),
            ("domain", bt(BaseType::Chars)),
            ("event_time", bt(BaseType::Time)),
        ],
    )
}

pub(super) fn dns_response_window() -> WindowSchema {
    make_window(
        "dns_response",
        vec!["dns_stream"],
        vec![
            ("query_id", bt(BaseType::Chars)),
            ("sip", bt(BaseType::Ip)),
            ("close_reason", bt(BaseType::Chars)),
            ("event_time", bt(BaseType::Time)),
        ],
    )
}

pub(super) fn output_window() -> WindowSchema {
    make_output_window(
        "out",
        vec![
            ("x", bt(BaseType::Ip)),
            ("y", bt(BaseType::Chars)),
            ("n", bt(BaseType::Digit)),
        ],
    )
}

pub(super) fn security_alerts_window() -> WindowSchema {
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

// ---------------------------------------------------------------------------
// Compile helper
// ---------------------------------------------------------------------------

/// Compile a WFL source string with given schemas, asserting parse + compile
/// both succeed.
pub(super) fn compile_with(src: &str, schemas: &[WindowSchema]) -> Vec<RulePlan> {
    let file = parse_wfl(src).expect("parse should succeed");
    compile_wfl(&file, schemas).expect("compile should succeed")
}
