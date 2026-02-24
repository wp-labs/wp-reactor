use std::io::BufReader;
use std::time::Duration;

use wf_lang::{BaseType, FieldDef, FieldType, WindowSchema};
use wf_proj::cmd_replay::replay_events;

fn make_auth_events_schema() -> WindowSchema {
    WindowSchema {
        name: "auth_events".to_string(),
        streams: vec!["auth_stream".to_string()],
        time_field: Some("event_time".to_string()),
        over: Duration::from_secs(3600),
        fields: vec![
            FieldDef {
                name: "sip".to_string(),
                field_type: FieldType::Base(BaseType::Ip),
            },
            FieldDef {
                name: "action".to_string(),
                field_type: FieldType::Base(BaseType::Chars),
            },
            FieldDef {
                name: "user".to_string(),
                field_type: FieldType::Base(BaseType::Chars),
            },
            FieldDef {
                name: "event_time".to_string(),
                field_type: FieldType::Base(BaseType::Time),
            },
        ],
    }
}

fn make_security_alerts_schema() -> WindowSchema {
    WindowSchema {
        name: "security_alerts".to_string(),
        streams: vec![],
        time_field: None,
        over: Duration::from_secs(3600),
        fields: vec![
            FieldDef {
                name: "sip".to_string(),
                field_type: FieldType::Base(BaseType::Ip),
            },
            FieldDef {
                name: "fail_count".to_string(),
                field_type: FieldType::Base(BaseType::Digit),
            },
        ],
    }
}

const WFL_RULE: &str = r#"
rule brute_force {
    events { e : auth_events }
    match<sip:5m> {
        on event { e | count >= 5; }
    } -> score(70.0)
    entity(ip, e.sip)
    yield security_alerts (sip = e.sip, fail_count = 5)
}
"#;

fn make_ndjson_events(count: usize) -> String {
    let mut lines = Vec::with_capacity(count);
    for i in 0..count {
        lines.push(format!(
            r#"{{"sip":"10.0.0.1","action":"failed","user":"admin","event_time":{}}}"#,
            1_700_000_000_000_000_000i64 + (i as i64) * 1_000_000_000
        ));
    }
    lines.join("\n")
}

#[test]
fn replay_five_events_one_match() {
    let schemas = vec![make_auth_events_schema(), make_security_alerts_schema()];
    let ndjson = make_ndjson_events(5);
    let reader = BufReader::new(ndjson.as_bytes());

    let result = replay_events(WFL_RULE, &schemas, reader, "e").expect("replay should succeed");

    assert_eq!(result.event_count, 5);
    assert_eq!(result.match_count, 1);
    assert_eq!(result.error_count, 0);
    assert_eq!(result.alerts.len(), 1);

    let alert = &result.alerts[0];
    assert_eq!(alert.rule_name, "brute_force");
    assert!((alert.score - 70.0).abs() < f64::EPSILON);
    assert_eq!(alert.entity_type, "ip");
    assert_eq!(alert.entity_id, "10.0.0.1");
}

#[test]
fn replay_below_threshold_no_match() {
    let schemas = vec![make_auth_events_schema(), make_security_alerts_schema()];
    let ndjson = make_ndjson_events(3);
    let reader = BufReader::new(ndjson.as_bytes());

    let result = replay_events(WFL_RULE, &schemas, reader, "e").expect("replay should succeed");

    assert_eq!(result.event_count, 3);
    assert_eq!(result.match_count, 0);
    assert_eq!(result.error_count, 0);
    assert!(result.alerts.is_empty());
}

// ===========================================================================
// EOF close_all(Eos) with on_close steps
// ===========================================================================

/// Rule with on_close: the event step is satisfied, then EOF triggers close_all(Eos)
/// which evaluates on_close steps and produces an alert.
const WFL_CLOSE_RULE: &str = r#"
rule eos_close {
    events { e : auth_events }
    match<sip:5m> {
        on event { e | count >= 1; }
        on close { close_count: e | count >= 1; }
    } -> score(80.0)
    entity(ip, e.sip)
    yield security_alerts (sip = e.sip, fail_count = 1)
}
"#;

#[test]
fn replay_eof_close_all_fires_alert() {
    let schemas = vec![make_auth_events_schema(), make_security_alerts_schema()];
    // Send 2 events: enough to satisfy on_event (count >= 1) and on_close (count >= 1).
    // No on-event match is produced (close steps present → deferred to close path).
    // EOF close_all(Eos) evaluates close steps and emits the alert.
    let ndjson = make_ndjson_events(2);
    let reader = BufReader::new(ndjson.as_bytes());

    let result =
        replay_events(WFL_CLOSE_RULE, &schemas, reader, "e").expect("replay should succeed");

    assert_eq!(result.event_count, 2);
    assert_eq!(result.match_count, 1, "expected one alert from EOF close");
    assert_eq!(result.error_count, 0);
    assert_eq!(result.alerts.len(), 1);

    let alert = &result.alerts[0];
    assert_eq!(alert.rule_name, "eos_close");
    assert!((alert.score - 80.0).abs() < f64::EPSILON);
    assert_eq!(alert.entity_type, "ip");
    assert_eq!(alert.entity_id, "10.0.0.1");
}

// ===========================================================================
// Multi-source rule: time_field resolves from the alias-specific schema
// ===========================================================================

fn make_b_win_schema() -> WindowSchema {
    WindowSchema {
        name: "b_win".to_string(),
        streams: vec!["b_stream".to_string()],
        time_field: Some("tb".to_string()),
        over: Duration::from_secs(3600),
        fields: vec![
            FieldDef {
                name: "sip".to_string(),
                field_type: FieldType::Base(BaseType::Ip),
            },
            FieldDef {
                name: "tb".to_string(),
                field_type: FieldType::Base(BaseType::Time),
            },
        ],
    }
}

/// In a multi-source rule, the replay alias determines which schema's time_field
/// is used. With --alias b, the engine should use "tb" (b_win's time_field),
/// not "event_time" (auth_events' time_field). This test verifies fired_at
/// is in the expected range, not 1970.
#[test]
fn replay_multi_source_uses_alias_time_field() {
    let schemas = vec![
        make_auth_events_schema(),
        make_b_win_schema(),
        make_security_alerts_schema(),
    ];

    // Rule binds two sources; we replay on alias "b" which comes from b_win.
    // b_win's time_field is "tb".
    let wfl = r#"
rule multi_src {
    events {
        a : auth_events
        b : b_win
    }
    match<sip:5m> {
        on event { b | count >= 2; }
    } -> score(60.0)
    entity(ip, b.sip)
    yield security_alerts (sip = b.sip, fail_count = 2)
}
"#;

    let base_nanos = 1_700_000_000_000_000_000i64;
    let ndjson = format!(
        "{}\n{}",
        format!(r#"{{"sip":"10.0.0.1","tb":{}}}"#, base_nanos),
        format!(
            r#"{{"sip":"10.0.0.1","tb":{}}}"#,
            base_nanos + 1_000_000_000
        ),
    );
    let reader = BufReader::new(ndjson.as_bytes());

    let result = replay_events(wfl, &schemas, reader, "b").expect("replay should succeed");

    assert_eq!(result.event_count, 2);
    assert_eq!(result.match_count, 1);
    assert_eq!(result.alerts.len(), 1);

    let alert = &result.alerts[0];
    assert_eq!(alert.rule_name, "multi_src");
    // fired_at must be derived from the event time (tb), not default to 0 (1970).
    // The nanosecond timestamp 1_700_000_000_000_000_000 is ~2023-11-14.
    // Convert fired_at (ISO string) year to verify it's not 1970.
    assert!(
        !alert.fired_at.starts_with("1970"),
        "fired_at should not be 1970 (got {}); time_field was not resolved from alias schema",
        alert.fired_at
    );
}
