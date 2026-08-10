// Tests for `rule_keep_fields` — the per-window field set used to avoid
// materializing unreferenced structured (object/array) fields (wp-reactor#19).
use super::*;
use crate::rule_keep_fields;

fn schemas() -> Vec<WindowSchema> {
    vec![
        make_window(
            "conn",
            vec!["c"],
            vec![
                ("sip", bt(BaseType::Ip)),
                ("bytes", bt(BaseType::Digit)),
                ("conn_info", FieldType::Object),
                ("event_time", bt(BaseType::Time)),
            ],
        ),
        make_output_window(
            "out",
            vec![("sip", bt(BaseType::Ip)), ("alert_type", bt(BaseType::Chars))],
        ),
    ]
}

#[test]
fn excludes_object_field_not_referenced() {
    let src = r#"
rule r {
    events { c : conn }
    match<sip:1m> { on event { c | count >= 10; } } -> score(10.0)
    entity(ip, c.sip)
    yield out (sip = c.sip, alert_type = "x")
}
"#;
    let file = parse_wfl(src).unwrap();
    let plans = compile_wfl(&file, &schemas()).unwrap();
    let keep = rule_keep_fields(&plans[0]).unwrap();
    let conn = keep.get("conn").unwrap();
    assert!(conn.contains("sip"), "entity/yield field must be kept");
    assert!(!conn.contains("conn_info"), "unreferenced object field must be dropped");
}

#[test]
fn includes_object_field_referenced_via_nested_path() {
    let src = r#"
rule r {
    events { c : conn }
    match<sip:1m> {
        on event { c && c.conn_info.geo.country == "CN" | count >= 5; }
    } -> score(10.0)
    entity(ip, c.sip)
    yield out (sip = c.sip, alert_type = "x")
}
"#;
    let file = parse_wfl(src).unwrap();
    let plans = compile_wfl(&file, &schemas()).unwrap();
    let keep = rule_keep_fields(&plans[0]).unwrap();
    assert!(
        keep.get("conn").unwrap().contains("conn_info"),
        "object field read via nested path must be kept"
    );
}

#[test]
fn none_for_each_rules() {
    let src = r#"
rule r {
    events { c : conn }
    on each c -> score(5.0)
    entity(ip, c.sip)
    yield out (sip = c.sip, alert_type = "x")
}
"#;
    let file = parse_wfl(src).unwrap();
    let plans = compile_wfl(&file, &schemas()).unwrap();
    assert!(rule_keep_fields(&plans[0]).is_none(), "each rules keep all fields");
}
