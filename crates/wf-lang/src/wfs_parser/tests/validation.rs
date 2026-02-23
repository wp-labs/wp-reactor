use std::time::Duration;

use super::super::parse_wfs;

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
