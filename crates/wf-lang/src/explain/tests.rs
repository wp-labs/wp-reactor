use std::time::Duration;

use crate::ast::{Expr, FieldRef};
use crate::compile_wfl;
use crate::schema::{BaseType, FieldDef, FieldType, WindowSchema};
use crate::wfl_parser::parse_wfl;

use super::format::format_expr;
use super::explain_rules;

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
                name: "action".to_string(),
                field_type: bt(BaseType::Chars),
            },
            FieldDef {
                name: "event_time".to_string(),
                field_type: bt(BaseType::Time),
            },
        ],
    }
}

fn security_alerts_window() -> WindowSchema {
    WindowSchema {
        name: "security_alerts".to_string(),
        streams: vec![],
        time_field: None,
        over: Duration::from_secs(3600),
        fields: vec![
            FieldDef {
                name: "sip".to_string(),
                field_type: bt(BaseType::Ip),
            },
            FieldDef {
                name: "fail_count".to_string(),
                field_type: bt(BaseType::Digit),
            },
            FieldDef {
                name: "message".to_string(),
                field_type: bt(BaseType::Chars),
            },
        ],
    }
}

#[test]
fn explain_brute_force_rule() {
    let input = r#"
rule brute_force_then_scan {
    events {
        fail : auth_events && action == "failed"
    }
    match<sip:5m> {
        on event {
            fail | count >= 3;
        }
        on close {
            fail | count >= 1;
        }
    } -> score(70.0)
    entity(ip, fail.sip)
    yield security_alerts (
        sip = fail.sip,
        fail_count = count(fail),
        message = fmt("{} brute force detected", fail.sip)
    )
}
"#;
    let schemas = &[auth_events_window(), security_alerts_window()];
    let file = parse_wfl(input).unwrap();
    let plans = compile_wfl(&file, schemas).unwrap();
    let explanations = explain_rules(&plans, schemas);

    assert_eq!(explanations.len(), 1);
    let expl = &explanations[0];
    assert_eq!(expl.name, "brute_force_then_scan");
    assert_eq!(expl.bindings.len(), 1);
    assert_eq!(expl.bindings[0].alias, "fail");
    assert_eq!(expl.bindings[0].window, "auth_events");
    assert!(expl.bindings[0].filter.is_some());

    assert_eq!(expl.match_expl.event_steps.len(), 1);
    assert_eq!(expl.match_expl.close_steps.len(), 1);
    assert_eq!(expl.score, "70.0");
    assert_eq!(expl.entity_type, "ip");
    assert_eq!(expl.entity_id, "fail.sip");
    assert_eq!(expl.yield_target, "security_alerts");
    assert_eq!(expl.yield_fields.len(), 3);

    // Verify Display output
    let output = format!("{}", expl);
    assert!(output.contains("Rule: brute_force_then_scan"));
    assert!(output.contains("fail -> auth_events"));
    assert!(output.contains("action == \"failed\""));
    assert!(output.contains("Score: 70.0"));
    assert!(output.contains("Entity: ip = fail.sip"));
    assert!(output.contains("sip"));
    assert!(output.contains("Field Lineage:"));
}

#[test]
fn format_expr_variants() {
    assert_eq!(format_expr(&Expr::Number(42.0)), "42.0");
    assert_eq!(format_expr(&Expr::Number(3.24)), "3.24");
    assert_eq!(format_expr(&Expr::StringLit("hello".into())), "\"hello\"");
    assert_eq!(format_expr(&Expr::Bool(true)), "true");
    assert_eq!(
        format_expr(&Expr::Field(FieldRef::Qualified("a".into(), "b".into()))),
        "a.b"
    );
    assert_eq!(
        format_expr(&Expr::FuncCall {
            qualifier: None,
            name: "count".into(),
            args: vec![Expr::Field(FieldRef::Simple("fail".into()))]
        }),
        "count(fail)"
    );
}
