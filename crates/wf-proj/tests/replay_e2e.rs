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
