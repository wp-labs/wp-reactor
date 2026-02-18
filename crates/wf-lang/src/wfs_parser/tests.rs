use std::time::Duration;

use winnow::prelude::*;

use super::parse_wfs;
use super::primitives::base_type_parser;
use crate::parse_utils::duration_value;
use crate::schema::{BaseType, FieldType};

// -----------------------------------------------------------------------
// Primitive parsers
// -----------------------------------------------------------------------

#[test]
fn parse_duration_seconds() {
    let d = duration_value.parse("30s").unwrap();
    assert_eq!(d, Duration::from_secs(30));
}

#[test]
fn parse_duration_minutes() {
    let d = duration_value.parse("5m").unwrap();
    assert_eq!(d, Duration::from_secs(300));
}

#[test]
fn parse_duration_hours() {
    let d = duration_value.parse("48h").unwrap();
    assert_eq!(d, Duration::from_secs(48 * 3600));
}

#[test]
fn parse_duration_days() {
    let d = duration_value.parse("7d").unwrap();
    assert_eq!(d, Duration::from_secs(7 * 86400));
}

#[test]
fn parse_duration_zero() {
    let d = duration_value.parse("0").unwrap();
    assert_eq!(d, Duration::ZERO);
}

#[test]
fn parse_duration_zero_with_suffix() {
    let d = duration_value.parse("0s").unwrap();
    assert_eq!(d, Duration::ZERO);
}

#[test]
fn parse_base_types() {
    assert_eq!(base_type_parser.parse("chars").unwrap(), BaseType::Chars);
    assert_eq!(base_type_parser.parse("digit").unwrap(), BaseType::Digit);
    assert_eq!(base_type_parser.parse("float").unwrap(), BaseType::Float);
    assert_eq!(base_type_parser.parse("bool").unwrap(), BaseType::Bool);
    assert_eq!(base_type_parser.parse("time").unwrap(), BaseType::Time);
    assert_eq!(base_type_parser.parse("ip").unwrap(), BaseType::Ip);
    assert_eq!(base_type_parser.parse("hex").unwrap(), BaseType::Hex);
}

#[test]
fn parse_array_type() {
    let ft = super::field_type.parse("array/digit").unwrap();
    assert_eq!(ft, FieldType::Array(BaseType::Digit));
}

#[test]
fn parse_field_decl_simple() {
    let fd = super::field_decl.parse("sip: ip").unwrap();
    assert_eq!(fd.name, "sip");
    assert_eq!(fd.field_type, FieldType::Base(BaseType::Ip));
}

#[test]
fn parse_field_decl_dotted() {
    let fd = super::field_decl.parse("detail.sha256: hex").unwrap();
    assert_eq!(fd.name, "detail.sha256");
    assert_eq!(fd.field_type, FieldType::Base(BaseType::Hex));
}

#[test]
fn parse_field_decl_backtick() {
    let fd = super::field_decl.parse("`src-ip`: ip").unwrap();
    assert_eq!(fd.name, "src-ip");
}

#[test]
fn parse_field_decl_array() {
    let fd = super::field_decl.parse("tags: array/chars").unwrap();
    assert_eq!(fd.name, "tags");
    assert_eq!(fd.field_type, FieldType::Array(BaseType::Chars));
}

// -----------------------------------------------------------------------
// Window declarations
// -----------------------------------------------------------------------

#[test]
fn parse_minimal_window() {
    let input = r#"
window ip_blocklist {
    over = 0
    fields {
        ip: ip
        category: chars
    }
}
"#;
    let schemas = parse_wfs(input).unwrap();
    assert_eq!(schemas.len(), 1);
    let w = &schemas[0];
    assert_eq!(w.name, "ip_blocklist");
    assert!(w.streams.is_empty());
    assert!(w.time_field.is_none());
    assert_eq!(w.over, Duration::ZERO);
    assert_eq!(w.fields.len(), 2);
}

#[test]
fn parse_window_with_stream_and_time() {
    let input = r#"
window auth_events {
    stream = "auth"
    time = event_time
    over = 30m

    fields {
        username: chars
        sip: ip
        event_time: time
    }
}
"#;
    let schemas = parse_wfs(input).unwrap();
    assert_eq!(schemas.len(), 1);
    let w = &schemas[0];
    assert_eq!(w.name, "auth_events");
    assert_eq!(w.streams, vec!["auth"]);
    assert_eq!(w.time_field.as_deref(), Some("event_time"));
    assert_eq!(w.over, Duration::from_secs(30 * 60));
    assert_eq!(w.fields.len(), 3);
}

#[test]
fn parse_window_multi_stream() {
    let input = r#"
window fw_events {
    stream = ["firewall", "netflow"]
    time = event_time
    over = 30m
    fields {
        sip: ip
        dip: ip
        event_time: time
    }
}
"#;
    let schemas = parse_wfs(input).unwrap();
    let w = &schemas[0];
    assert_eq!(w.streams, vec!["firewall", "netflow"]);
}

#[test]
fn parse_multiple_windows() {
    let input = r#"
window a {
    over = 0
    fields { x: chars }
}

window b {
    time = ts
    over = 5m
    fields { ts: time  v: digit }
}
"#;
    let schemas = parse_wfs(input).unwrap();
    assert_eq!(schemas.len(), 2);
    assert_eq!(schemas[0].name, "a");
    assert_eq!(schemas[1].name, "b");
}

#[test]
fn parse_with_comments() {
    let input = r#"
# Security window definitions
window alerts {
    # No stream — yield only
    over = 0
    fields {
        # Alert fields
        rule_name: chars
        score: float
    }
}
"#;
    let schemas = parse_wfs(input).unwrap();
    assert_eq!(schemas.len(), 1);
    assert_eq!(schemas[0].fields.len(), 2);
}

// -----------------------------------------------------------------------
// Full security.wfs example
// -----------------------------------------------------------------------

#[test]
fn parse_security_ws() {
    let input = r#"
window auth_events {
    stream = "auth"
    time = event_time
    over = 30m
    fields {
        username: chars
        uid: digit
        sip: ip
        dport: digit
        action: chars
        result: chars
        event_time: time
        process: chars
    }
}

window fw_events {
    stream = ["firewall", "netflow"]
    time = event_time
    over = 30m
    fields {
        sip: ip
        dip: ip
        dport: digit
        action: chars
        bytes: digit
        event_time: time
        protocol: chars
    }
}

window ip_blocklist {
    over = 0
    fields {
        ip: ip
        threat_level: chars
        category: chars
    }
}

window security_alerts {
    time = emit_time
    over = 48h
    fields {
        sip: ip
        rule_name: chars
        fail_count: digit
        port_count: digit
        threat_level: chars
        message: chars
        emit_time: time
        score: float
    }
}
"#;
    let schemas = parse_wfs(input).unwrap();
    assert_eq!(schemas.len(), 4);
    assert_eq!(schemas[0].name, "auth_events");
    assert_eq!(schemas[0].streams, vec!["auth"]);
    assert_eq!(schemas[0].fields.len(), 8);
    assert_eq!(schemas[1].name, "fw_events");
    assert_eq!(schemas[1].streams, vec!["firewall", "netflow"]);
    assert_eq!(schemas[1].fields.len(), 7);
    assert_eq!(schemas[2].name, "ip_blocklist");
    assert!(schemas[2].streams.is_empty());
    assert_eq!(schemas[2].over, Duration::ZERO);
    assert_eq!(schemas[3].name, "security_alerts");
    assert_eq!(schemas[3].over, Duration::from_secs(48 * 3600));
}

// -----------------------------------------------------------------------
// Semantic validation errors
// -----------------------------------------------------------------------

#[test]
fn reject_duplicate_window_names() {
    let input = r#"
window foo { over = 0  fields { x: chars } }
window foo { over = 0  fields { y: digit } }
"#;
    let err = parse_wfs(input).unwrap_err();
    assert!(err.to_string().contains("duplicate window name"));
}

#[test]
fn reject_over_without_time_attr() {
    let input = r#"
window bad {
    over = 5m
    fields {
        x: chars
    }
}
"#;
    let err = parse_wfs(input).unwrap_err();
    assert!(err.to_string().contains("requires a 'time' attribute"));
}

#[test]
fn reject_time_field_not_in_fields() {
    let input = r#"
window bad {
    time = ts
    over = 5m
    fields {
        x: chars
    }
}
"#;
    let err = parse_wfs(input).unwrap_err();
    assert!(err.to_string().contains("not found in fields"));
}

#[test]
fn reject_time_field_wrong_type() {
    let input = r#"
window bad {
    time = ts
    over = 5m
    fields {
        ts: chars
    }
}
"#;
    let err = parse_wfs(input).unwrap_err();
    assert!(err.to_string().contains("must have type 'time'"));
}

#[test]
fn accept_over_zero_without_time() {
    let input = r#"
window static_table {
    over = 0
    fields {
        key: chars
        val: digit
    }
}
"#;
    let schemas = parse_wfs(input).unwrap();
    assert_eq!(schemas.len(), 1);
    assert!(schemas[0].time_field.is_none());
}

// -----------------------------------------------------------------------
// Edge cases
// -----------------------------------------------------------------------

#[test]
fn parse_empty_file() {
    let schemas = parse_wfs("").unwrap();
    assert!(schemas.is_empty());
}

#[test]
fn parse_comment_only_file() {
    let schemas = parse_wfs("# just a comment\n# another\n").unwrap();
    assert!(schemas.is_empty());
}

#[test]
fn parse_dotted_field_names() {
    let input = r#"
window parsed_logs {
    time = timestamp
    over = 5m
    fields {
        detail.sha256: hex
        detail.severity: digit
        timestamp: time
    }
}
"#;
    let schemas = parse_wfs(input).unwrap();
    assert_eq!(schemas[0].fields[0].name, "detail.sha256");
    assert_eq!(schemas[0].fields[1].name, "detail.severity");
}

#[test]
fn parse_backtick_field_names() {
    let input = r#"
window special {
    over = 0
    fields {
        `src-ip`: ip
        `content type`: chars
    }
}
"#;
    let schemas = parse_wfs(input).unwrap();
    assert_eq!(schemas[0].fields[0].name, "src-ip");
    assert_eq!(schemas[0].fields[1].name, "content type");
}

#[test]
fn parse_all_array_types() {
    let input = r#"
window arrays {
    over = 0
    fields {
        a: array/chars
        b: array/digit
        c: array/float
        d: array/bool
        e: array/time
        f: array/ip
        g: array/hex
    }
}
"#;
    let schemas = parse_wfs(input).unwrap();
    assert_eq!(schemas[0].fields.len(), 7);
    assert_eq!(
        schemas[0].fields[0].field_type,
        FieldType::Array(BaseType::Chars)
    );
    assert_eq!(
        schemas[0].fields[6].field_type,
        FieldType::Array(BaseType::Hex)
    );
}

#[test]
fn reject_unknown_type() {
    let input = r#"
window bad {
    over = 0
    fields {
        x: unknown
    }
}
"#;
    assert!(parse_wfs(input).is_err());
}

#[test]
fn window_no_over_defaults_zero() {
    let input = r#"
window defaults {
    fields {
        v: chars
    }
}
"#;
    let schemas = parse_wfs(input).unwrap();
    assert_eq!(schemas[0].over, Duration::ZERO);
}

#[test]
fn reject_missing_fields_block() {
    let input = r#"
window bad {
    over = 0
}
"#;
    assert!(parse_wfs(input).is_err());
}

#[test]
fn reject_duplicate_stream_attr() {
    let input = r#"
window bad {
    stream = "a"
    stream = "b"
    over = 0
    fields { x: chars }
}
"#;
    assert!(parse_wfs(input).is_err());
}

#[test]
fn reject_duplicate_time_attr() {
    let input = r#"
window bad {
    time = ts
    time = ts2
    over = 5m
    fields { ts: time  ts2: time }
}
"#;
    assert!(parse_wfs(input).is_err());
}

#[test]
fn reject_duplicate_over_attr() {
    let input = r#"
window bad {
    over = 5m
    over = 10m
    time = ts
    fields { ts: time }
}
"#;
    assert!(parse_wfs(input).is_err());
}

#[test]
fn reject_duplicate_fields_block() {
    let input = r#"
window bad {
    over = 0
    fields { x: chars }
    fields { y: digit }
}
"#;
    assert!(parse_wfs(input).is_err());
}
