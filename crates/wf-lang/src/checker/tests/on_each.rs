use super::*;
use crate::schema::FieldType;

#[test]
fn on_each_join_key_object_rejected() {
    // Hash-join index needs a scalar key; object/array join keys are rejected.
    let lookup = make_window(
        "lookup_table",
        vec!["lookup_stream"],
        vec![
            ("ip", bt(BaseType::Ip)),
            ("payload", FieldType::Object),
        ],
    );
    let input = r#"
rule r {
    events { e : auth_events }
    on each e -> score(1.0)
    join lookup_table snapshot on e.sip == lookup_table.payload
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), lookup, output_window()],
        "join key `lookup_table.payload` must be a scalar base type",
    );
}

#[test]
fn on_each_join_key_float_rejected() {
    // Float join keys would truncate in JoinKey::Int (42.5 → 42), false-matching
    // distinct values — so they are rejected like structured types.
    let lookup = make_window(
        "lookup_table",
        vec!["lookup_stream"],
        vec![("score", bt(BaseType::Float))],
    );
    let input = r#"
rule r {
    events { e : auth_events }
    on each e -> score(1.0)
    join lookup_table snapshot on e.count == lookup_table.score
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), lookup, output_window()],
        "float excluded",
    );
}

#[test]
fn on_each_join_key_scalar_ok() {
    let lookup = make_window(
        "lookup_table",
        vec!["lookup_stream"],
        vec![("ip", bt(BaseType::Ip))],
    );
    let input = r#"
rule r {
    events { e : auth_events }
    on each e -> score(1.0)
    join lookup_table snapshot on e.sip == lookup_table.ip
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_no_errors(input, &[auth_events_window(), lookup, output_window()]);
}

#[test]
fn on_each_allows_scalar_expressions() {
    let input = r#"
rule r {
    events { e : auth_events }
    on each e -> score(1.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_no_errors(input, &[auth_events_window(), output_window()]);
}

#[test]
fn on_each_rejects_set_functions_in_score() {
    let input = r#"
rule r {
    events { e : auth_events }
    on each e -> score(count(e))
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "function `count` is not allowed in `on each`",
    );
}

#[test]
fn on_each_rejects_close_reason_in_where() {
    let input = r#"
rule r {
    events { e : auth_events }
    on each e where close_reason == "timeout" -> score(1.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "close_reason is not available in `on each`",
    );
}

#[test]
fn on_each_checks_join_semantics() {
    let input = r#"
rule r {
    events { e : auth_events }
    on each e -> score(1.0)
    join missing snapshot on e.sip == missing.ip
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "join target window `missing` does not exist in schemas",
    );
}

#[test]
fn on_each_downstream_can_use_auto_wfu_fields() {
    let enriched = make_output_window(
        "enriched_events",
        vec![
            ("event_time", bt(BaseType::Time)),
            ("sip", bt(BaseType::Ip)),
            ("username", bt(BaseType::Chars)),
        ],
    );
    let final_out = make_output_window("final_out", vec![("sip", bt(BaseType::Ip))]);
    let input = r#"
rule enrich_each_event {
    events { e : auth_events }
    on each e -> score(1.0)
    entity(ip, e.sip)
    yield enriched_events (
        event_time = e.event_time,
        sip = e.sip,
        username = e.user
    )
}

rule final_risk {
    events { x : enriched_events }
    match<sip:5m> {
        on event {
            x | count >= 1;
        }
    } -> score(avg(x.__wfu_score) + 10.0)
    entity(ip, x.sip)
    yield final_out (sip = x.sip)
}
"#;
    assert_no_errors(input, &[auth_events_window(), enriched, final_out]);
}

#[test]
fn on_each_downstream_can_use_all_auto_intermediate_wfu_fields() {
    let enriched = make_output_window(
        "enriched_events",
        vec![
            ("event_time", bt(BaseType::Time)),
            ("sip", bt(BaseType::Ip)),
        ],
    );
    let final_out = make_output_window(
        "final_out",
        vec![
            ("rule_name", bt(BaseType::Chars)),
            ("score", bt(BaseType::Float)),
            ("entity_type", bt(BaseType::Chars)),
            ("entity_id", bt(BaseType::Chars)),
        ],
    );
    let input = r#"
rule enrich_each_event {
    events { e : auth_events }
    on each e -> score(1.0)
    entity(ip, e.sip)
    yield enriched_events (
        event_time = e.event_time,
        sip = e.sip
    )
}

rule final_risk {
    events { x : enriched_events }
    match<sip:5m> {
        on event {
            x | count >= 1;
        }
    } -> score(x.__wfu_score)
    entity(ip, x.sip)
    yield final_out (
        rule_name = x.__wfu_rule_name,
        score = x.__wfu_score,
        entity_type = x.__wfu_entity_type,
        entity_id = x.__wfu_entity_id
    )
}
"#;
    assert_no_errors(input, &[auth_events_window(), enriched, final_out]);
}

#[test]
fn on_each_downstream_rejects_wfu_fields_outside_intermediate_directory() {
    let enriched = make_output_window(
        "enriched_events",
        vec![
            ("event_time", bt(BaseType::Time)),
            ("sip", bt(BaseType::Ip)),
        ],
    );
    let final_out = make_output_window("final_out", vec![("wfu_id", bt(BaseType::Chars))]);
    let input = r#"
rule enrich_each_event {
    events { e : auth_events }
    on each e -> score(1.0)
    entity(ip, e.sip)
    yield enriched_events (
        event_time = e.event_time,
        sip = e.sip
    )
}

rule final_risk {
    events { x : enriched_events }
    match<sip:5m> {
        on event {
            x | count >= 1;
        }
    } -> score(1.0)
    entity(ip, x.sip)
    yield final_out (wfu_id = x.__wfu_id)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), enriched, final_out],
        "__wfu_id",
    );
}

#[test]
fn on_each_rejects_intermediate_window_cycles() {
    let enriched = make_output_window(
        "enriched_events",
        vec![
            ("event_time", bt(BaseType::Time)),
            ("sip", bt(BaseType::Ip)),
        ],
    );
    let input = r#"
rule enrich_each_event {
    events { e : enriched_events }
    on each e -> score(1.0)
    entity(ip, e.sip)
    yield enriched_events (
        event_time = e.event_time,
        sip = e.sip
    )
}
"#;
    assert_has_error(
        input,
        &[enriched],
        "must be acyclic; found cycle: enriched_events -> enriched_events",
    );
}
