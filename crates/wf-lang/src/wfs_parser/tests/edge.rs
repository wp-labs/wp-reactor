use std::time::Duration;

use super::super::parse_wfs;
use crate::schema::{BaseType, FieldType};

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
