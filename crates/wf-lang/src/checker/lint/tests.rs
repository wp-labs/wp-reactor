use std::time::Duration;

use crate::schema::{BaseType, FieldDef, FieldType, WindowSchema};
use crate::wfl_parser::parse_wfl;

use super::*;

fn bt(b: BaseType) -> FieldType {
    FieldType::Base(b)
}

fn auth_events_window() -> WindowSchema {
    WindowSchema {
        name: "auth_events".to_string(),
        streams: vec!["auth_stream".to_string()],
        time_field: Some("event_time".to_string()),
        over: Duration::from_secs(3600),
        fields: vec![
            FieldDef {
                name: "sip".to_string(),
                field_type: bt(BaseType::Ip),
            },
            FieldDef {
                name: "dip".to_string(),
                field_type: bt(BaseType::Ip),
            },
            FieldDef {
                name: "action".to_string(),
                field_type: bt(BaseType::Chars),
            },
            FieldDef {
                name: "user".to_string(),
                field_type: bt(BaseType::Chars),
            },
            FieldDef {
                name: "count".to_string(),
                field_type: bt(BaseType::Digit),
            },
            FieldDef {
                name: "event_time".to_string(),
                field_type: bt(BaseType::Time),
            },
        ],
    }
}

fn output_window() -> WindowSchema {
    WindowSchema {
        name: "out".to_string(),
        streams: vec![],
        time_field: None,
        over: Duration::from_secs(3600),
        fields: vec![
            FieldDef {
                name: "x".to_string(),
                field_type: bt(BaseType::Ip),
            },
            FieldDef {
                name: "y".to_string(),
                field_type: bt(BaseType::Chars),
            },
            FieldDef {
                name: "n".to_string(),
                field_type: bt(BaseType::Digit),
            },
        ],
    }
}

fn lint_warnings(input: &str, schemas: &[WindowSchema]) -> Vec<String> {
    let file = parse_wfl(input).expect("parse should succeed");
    let warnings = lint_wfl(&file, schemas);
    warnings.into_iter().map(|e| e.message).collect()
}

fn assert_has_warning(input: &str, schemas: &[WindowSchema], substring: &str) {
    let warnings = lint_warnings(input, schemas);
    assert!(
        warnings.iter().any(|w| w.contains(substring)),
        "expected a warning containing {:?}, got: {:?}",
        substring,
        warnings
    );
}

fn assert_no_warning(input: &str, schemas: &[WindowSchema], substring: &str) {
    let warnings = lint_warnings(input, schemas);
    assert!(
        !warnings.iter().any(|w| w.contains(substring)),
        "expected no warning containing {:?}, got: {:?}",
        substring,
        warnings
    );
}

// W001: unused event alias
#[test]
fn w001_unused_alias_detected() {
    let input = r#"
rule r {
    events {
        e : auth_events && action == "failed"
        unused : auth_events
    }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_warning(input, &[auth_events_window(), output_window()], "W001");
}

#[test]
fn w001_all_used_no_warning() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_no_warning(input, &[auth_events_window(), output_window()], "W001");
}

// W002: missing on_close
#[test]
fn w002_no_on_close() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_warning(input, &[auth_events_window(), output_window()], "W002");
}

#[test]
fn w002_has_on_close_no_warning() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> {
        on event { e | count >= 1; }
        on close { e | count >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_no_warning(input, &[auth_events_window(), output_window()], "W002");
}

// W003: high cardinality key
#[test]
fn w003_four_keys() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip, dip, action, user:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_warning(input, &[auth_events_window(), output_window()], "W003");
}

#[test]
fn w003_three_keys_no_warning() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip, dip, action:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_no_warning(input, &[auth_events_window(), output_window()], "W003");
}

// W004: threshold zero with >= or >
#[test]
fn w004_threshold_zero_ge() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 0; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_warning(input, &[auth_events_window(), output_window()], "W004");
}

#[test]
fn w004_threshold_nonzero_no_warning() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_no_warning(input, &[auth_events_window(), output_window()], "W004");
}

// W005: score always zero
#[test]
fn w005_score_zero() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(0.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_warning(input, &[auth_events_window(), output_window()], "W005");
}

#[test]
fn w005_score_nonzero_no_warning() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(70.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_no_warning(input, &[auth_events_window(), output_window()], "W005");
}

// W006: yield field case collision with system field
#[test]
fn w006_case_collision() {
    let out = WindowSchema {
        name: "out".to_string(),
        streams: vec![],
        time_field: None,
        over: Duration::from_secs(3600),
        fields: vec![FieldDef {
            name: "Score".to_string(),
            field_type: bt(BaseType::Float),
        }],
    };
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (Score = 42.0)
}
"#;
    assert_has_warning(input, &[auth_events_window(), out], "W006");
}

#[test]
fn w006_exact_match_no_warning() {
    // Exact match is already an error in rules.rs, lint should skip it
    let out = WindowSchema {
        name: "out".to_string(),
        streams: vec![],
        time_field: None,
        over: Duration::from_secs(3600),
        fields: vec![FieldDef {
            name: "sip".to_string(),
            field_type: bt(BaseType::Ip),
        }],
    };
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (sip = e.sip)
}
"#;
    assert_no_warning(input, &[auth_events_window(), out], "W006");
}
