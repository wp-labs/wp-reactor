use wf_lang::{BaseType, FieldDef, FieldType, WindowSchema};

use crate::match_engine::RuleExecutor;

use super::super::helpers::*;

#[test]
fn compiled_field_tracking_supports_close_yield_and_l3_expressions() {
    use crate::match_engine::match_engine::{CepStateMachine, StepResult};

    let input_window = WindowSchema {
        name: "auth_events".to_string(),
        streams: vec!["auth_stream".to_string()],
        time_field: Some("event_time".to_string()),
        over: std::time::Duration::from_secs(3600),
        fields: vec![
            FieldDef {
                name: "sip".to_string(),
                field_type: FieldType::Base(BaseType::Ip),
            },
            FieldDef {
                name: "dip".to_string(),
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
                name: "count".to_string(),
                field_type: FieldType::Base(BaseType::Digit),
            },
            FieldDef {
                name: "dport".to_string(),
                field_type: FieldType::Base(BaseType::Digit),
            },
            FieldDef {
                name: "event_time".to_string(),
                field_type: FieldType::Base(BaseType::Time),
            },
        ],
    };
    let output_window = WindowSchema {
        name: "tracked_out".to_string(),
        streams: vec![],
        time_field: None,
        over: std::time::Duration::from_secs(3600),
        fields: vec![
            FieldDef {
                name: "sip".to_string(),
                field_type: FieldType::Base(BaseType::Ip),
            },
            FieldDef {
                name: "plain_sip".to_string(),
                field_type: FieldType::Base(BaseType::Ip),
            },
            FieldDef {
                name: "avg_count".to_string(),
                field_type: FieldType::Base(BaseType::Float),
            },
            FieldDef {
                name: "actions".to_string(),
                field_type: FieldType::Array(BaseType::Chars),
            },
        ],
    };
    let source = r#"
rule tracked_close {
    events { c : auth_events }
    match<sip:5m> {
        on event { c.dport | distinct | count >= 2; }
        and close { c.dport | distinct | count >= 2; }
    } -> score(avg(c.count))
    entity(user, last(c.user))
    yield tracked_out (
        sip = c.dip,
        plain_sip = sip,
        avg_count = avg(c.count),
        actions = collect_set(c.action)
    )
}

"#;
    let file = wf_lang::parse_wfl(source).expect("parse should succeed");
    let plan = wf_lang::compile_wfl(&file, &[input_window, output_window])
        .expect("compile should succeed")
        .into_iter()
        .next()
        .expect("rule plan should exist");

    let tracked_fields = plan
        .match_plan
        .tracked_bind_fields
        .get("c")
        .expect("compiler should track alias c fields");
    assert!(tracked_fields.contains("dip"));
    assert!(tracked_fields.contains("count"));
    assert!(tracked_fields.contains("action"));
    assert!(tracked_fields.contains("user"));
    assert!(plan.match_plan.tracked_plain_fields.contains("sip"));

    let exec = RuleExecutor::new(plan.clone());
    let mut sm = CepStateMachine::new(
        plan.name.clone(),
        plan.match_plan.clone(),
        Some("event_time".to_string()),
    );
    let base: i64 = 1_700_000_000_000_000_000;
    let e1 = event(vec![
        ("sip", str_val("10.0.0.1")),
        ("dip", str_val("10.0.0.10")),
        ("action", str_val("scan")),
        ("user", str_val("alice")),
        ("count", num(10.0)),
        ("dport", num(22.0)),
    ]);
    let e2 = event(vec![
        ("sip", str_val("10.0.0.1")),
        ("dip", str_val("10.0.0.11")),
        ("action", str_val("probe")),
        ("user", str_val("alice")),
        ("count", num(30.0)),
        ("dport", num(80.0)),
    ]);

    assert_eq!(sm.advance_at("c", &e1, base), StepResult::Accumulate);
    assert_eq!(
        sm.advance_at("c", &e2, base + 1_000_000_000),
        StepResult::Advance
    );

    let outputs = sm.close_all(CloseReason::Timeout);
    assert_eq!(outputs.len(), 1);
    let alert = exec
        .execute_close(&outputs[0])
        .expect("close execution should succeed")
        .expect("close should produce alert");

    assert_eq!(alert.entity_id, "alice");
    assert!((alert.score - 20.0).abs() < f64::EPSILON);
    assert_eq!(
        alert.yield_fields.iter().find(|(name, _)| &**name == "sip"),
        Some(&("sip".into(), Value::Str("10.0.0.11".into())))
    );
    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(name, _)| &**name == "plain_sip"),
        Some(&("plain_sip".into(), Value::Str("10.0.0.1".into())))
    );
    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(name, _)| &**name == "avg_count"),
        Some(&("avg_count".into(), Value::Number(20.0)))
    );
    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(name, _)| &**name == "actions"),
        Some(&(
            "actions".into(),
            Value::Array(vec![Value::Str("scan".into()), Value::Str("probe".into())])
        ))
    );
}

// Bring in CloseReason for the L3 test
use crate::match_engine::Value;
use crate::match_engine::match_engine::CloseReason;
