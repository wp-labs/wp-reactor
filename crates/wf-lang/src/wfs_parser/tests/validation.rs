use super::super::{parse_static_wfs, parse_wfs};

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

#[test]
fn accept_structured_object_field_in_stream_window() {
    let input = r#"
window events {
    stream_tag = "auth"
    over = 0
    fields {
        ctx: object
    }
}
"#;
    let schemas = parse_wfs(input).unwrap();
    assert_eq!(schemas[0].fields[0].field_type, crate::FieldType::Object);
}

#[test]
fn accept_structured_array_field_in_stream_window() {
    let input = r#"
window events {
    stream_tag = "auth"
    over = 0
    fields {
        tags: array
    }
}
"#;
    let schemas = parse_wfs(input).unwrap();
    assert_eq!(schemas[0].fields[0].field_type, crate::FieldType::ArrayAny);
}

#[test]
fn accept_typed_array_field_in_stream_window() {
    let input = r#"
window events {
    stream_tag = "auth"
    over = 0
    fields {
        ports: array/digit
    }
}
"#;
    let schemas = parse_wfs(input).unwrap();
    assert_eq!(
        schemas[0].fields[0].field_type,
        crate::FieldType::Array(crate::BaseType::Digit)
    );
}

#[test]
fn reject_structured_field_in_provider_window() {
    let input = r#"
window<provider> ip_reputation {
    fields {
        metadata: object
    }
}
"#;
    let err = parse_static_wfs(input).unwrap_err();
    assert!(err.to_string().contains("provider object/array fields"));
}

#[test]
fn reject_structured_provider_field_through_flow_schema_entrypoint() {
    let input = r#"
window<provider> ip_reputation {
    fields {
        metadata: array
    }
}
"#;
    let err = parse_wfs(input).unwrap_err();
    assert!(err.to_string().contains("provider object/array fields"));
}

/// P4：`parse_wfs`（flow 入口）必须把 `window<provider>` 合并进返回的
/// `WindowSchema` 列表（`StaticWindowSchema::to_flow_schema()`）——否则 checker
/// 的 `check_joins_list` 查不到 provider 目标窗口，side input join 无法编译。
#[test]
fn flow_entrypoint_merges_provider_as_flow_schema() {
    let input = r#"
window bid_events {
    stream_tag = "bid"
    time = event_time
    over = 10m
    fields {
        bidder: digit
        event_time: time
    }
}
window<provider> person_table {
    fields {
        id: digit
        state: chars
    }
}
"#;
    let schemas = parse_wfs(input).unwrap();
    assert_eq!(schemas.len(), 2);
    let person = schemas
        .iter()
        .find(|s| s.name == "person_table")
        .expect("provider window must be merged into flow schemas");
    // provider 投影：无 stream、无 time、over = 0
    assert!(person.streams.is_empty());
    assert!(person.time_field.is_none());
    assert!(person.over.is_zero());
    assert_eq!(person.fields.len(), 2);
    assert_eq!(person.fields[0].name, "id");
}

/// P4：provider 与 flow 窗口同名（重名覆盖）应报错——to_flow_schema 投影会与
/// 既有 flow 窗口撞名。
#[test]
fn flow_entrypoint_rejects_provider_colliding_with_flow_window() {
    let input = r#"
window person_table {
    stream_tag = "person"
    time = event_time
    over = 10m
    fields {
        id: digit
        event_time: time
    }
}
window<provider> person_table {
    fields {
        id: digit
    }
}
"#;
    let err = parse_wfs(input).unwrap_err();
    assert!(err.to_string().contains("duplicate window name"));
}

#[test]
fn accept_structured_fields_in_yield_only_window() {
    let input = r#"
window alerts {
    over = 0
    fields {
        ctx: object
        tags: array
        scores: array/float
    }
}
"#;
    let schemas = parse_wfs(input).unwrap();
    assert_eq!(schemas.len(), 1);
}
