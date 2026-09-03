//! yield_tests.rs 拆出的 stat 上下文 / 证据 / 缺失可选字段输出测试（2026-09-04；
//! `#[path]` 子模块，经父模块 `use super::*` 复用其导入）。
//!
//! 覆盖：stat 上下文函数与 stat 终值、window event_ids 证据收集/去重/空窗/采样
//! 上限、accu 运行计数证据累积、缺失可选字段（#62）省略与显式 NaN 失败、chars
//! 降级为空串。

use super::*;

// =========================================================================
// Stat context yield tests
// =========================================================================

#[test]
fn execute_match_yield_can_use_stat_context_functions() {
    use crate::match_engine::cep::{CepStateMachine, StepResult};
    use wf_lang::{BaseType, FieldDef, FieldType, WindowSchema};

    let input_window = WindowSchema {
        name: "auth_events".into(),
        streams: vec!["auth_stream".into()],
        time_field: Some("event_time".into()),
        over: std::time::Duration::from_secs(3600),
        fields: vec![
            FieldDef {
                name: "sip".into(),
                field_type: FieldType::Base(BaseType::Ip),
            },
            FieldDef {
                name: "event_time".into(),
                field_type: FieldType::Base(BaseType::Time),
            },
        ],
    };
    let output_window = WindowSchema {
        name: "out".into(),
        streams: vec![],
        time_field: None,
        over: std::time::Duration::from_secs(3600),
        fields: vec![
            FieldDef {
                name: "sip".into(),
                field_type: FieldType::Base(BaseType::Ip),
            },
            FieldDef {
                name: "window_events".into(),
                field_type: FieldType::Base(BaseType::Digit),
            },
            FieldDef {
                name: "matched_events".into(),
                field_type: FieldType::Base(BaseType::Digit),
            },
            FieldDef {
                name: "trigger_count".into(),
                field_type: FieldType::Base(BaseType::Float),
            },
        ],
    };
    let source = r#"
rule stat_rule {
    events { auth : auth_events }
    match<sip:5m> {
        on event { fail: auth | count >= 2; }
    } -> score(70.0)
    entity(ip, auth.sip)
    yield out (
        sip = auth.sip,
        window_events = stat.count(window_event(auth)),
        matched_events = stat.count(match_event(fail)),
        trigger_count = stat.value(trigger(fail))
    )
}
"#;
    let file = wf_lang::parse_wfl(source).expect("parse should succeed");
    let plan = wf_lang::compile_wfl(&file, &[input_window, output_window])
        .expect("compile should succeed")
        .into_iter()
        .next()
        .expect("rule plan should exist");
    assert!(plan.match_plan.tracked_bind_aliases.contains("auth"));

    let exec = RuleExecutor::new(plan.clone());
    let mut sm = CepStateMachine::new(plan.name.clone(), plan.match_plan.clone(), None);
    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    assert_eq!(
        sm.advance_at("auth", &e, 1_000_000_000),
        StepResult::Accumulate
    );
    let StepResult::Matched(matched) = sm.advance_at("auth", &e, 2_000_000_000) else {
        panic!("expected match");
    };

    let alert = exec.execute_match(&matched).expect("alert");
    let field = |name: &str| {
        alert
            .yield_fields
            .iter()
            .find(|(field_name, _)| &**field_name == name)
            .map(|(_, value)| value.clone())
    };

    assert_eq!(field("window_events"), Some(num(2.0)));
    assert_eq!(field("matched_events"), Some(num(2.0)));
    assert_eq!(field("trigger_count"), Some(num(2.0)));
}

fn evidence_input_window() -> WindowSchema {
    WindowSchema {
        name: "auth_events".into(),
        streams: vec!["auth_stream".into()],
        time_field: Some("event_time".into()),
        over: std::time::Duration::from_secs(3600),
        fields: vec![
            FieldDef {
                name: "sip".into(),
                field_type: FieldType::Base(BaseType::Ip),
            },
            FieldDef {
                name: "event_id".into(),
                field_type: FieldType::Base(BaseType::Chars),
            },
            FieldDef {
                name: "event_time".into(),
                field_type: FieldType::Base(BaseType::Time),
            },
            FieldDef {
                name: "weight".into(),
                field_type: FieldType::Base(BaseType::Digit),
            },
        ],
    }
}

fn evidence_output_window() -> WindowSchema {
    WindowSchema {
        name: "out".into(),
        streams: vec![],
        time_field: None,
        over: std::time::Duration::from_secs(3600),
        fields: vec![
            FieldDef {
                name: "sip".into(),
                field_type: FieldType::Base(BaseType::Ip),
            },
            FieldDef {
                name: "event_count".into(),
                field_type: FieldType::Base(BaseType::Digit),
            },
            FieldDef {
                name: "evidences".into(),
                field_type: FieldType::Array(BaseType::Chars),
            },
        ],
    }
}

fn evidence_event(event_id: &str) -> Value {
    str_val(event_id)
}

#[test]
fn execute_match_yield_collects_window_event_ids() {
    use crate::match_engine::cep::{CepStateMachine, StepResult};

    let source = r#"
rule evidence_rule {
    events { s : auth_events }
    match<sip:5m> {
        on event { hit: s | count >= 6; }
    } -> score(70.0)
    entity(ip, s.sip)
    yield out (
        sip = s.sip,
        event_count = stat.count(window_event(s)),
        evidences = collect_set(s.event_id)
    )
}
"#;
    let file = wf_lang::parse_wfl(source).expect("parse should succeed");
    let plan = wf_lang::compile_wfl(&file, &[evidence_input_window(), evidence_output_window()])
        .expect("compile should succeed")
        .into_iter()
        .next()
        .expect("rule plan should exist");
    assert!(
        plan.match_plan
            .tracked_bind_fields
            .get("s")
            .is_some_and(|fields| fields.contains("event_id"))
    );

    let exec = RuleExecutor::new(plan.clone());
    let mut sm = CepStateMachine::new(plan.name.clone(), plan.match_plan.clone(), None);
    let mut matched = None;
    for i in 0..6 {
        let event_id = format!("evt_{:03}", i + 1);
        let step = sm.advance_at(
            "s",
            &event(vec![
                ("sip", str_val("10.0.0.1")),
                ("event_id", evidence_event(&event_id)),
            ]),
            (i as i64 + 1) * 1_000_000_000,
        );
        if i < 5 {
            assert_eq!(step, StepResult::Accumulate);
        } else {
            let StepResult::Matched(ctx) = step else {
                panic!("sixth event should trigger");
            };
            matched = Some(ctx);
        }
    }

    let alert = exec
        .execute_match(&matched.expect("matched context"))
        .expect("alert");
    let field = |name: &str| {
        alert
            .yield_fields
            .iter()
            .find(|(field_name, _)| &**field_name == name)
            .map(|(_, value)| value.clone())
    };

    assert_eq!(field("event_count"), Some(num(6.0)));
    assert_eq!(
        field("evidences"),
        Some(Value::Array(vec![
            str_val("evt_001"),
            str_val("evt_002"),
            str_val("evt_003"),
            str_val("evt_004"),
            str_val("evt_005"),
            str_val("evt_006"),
        ]))
    );
}

#[test]
fn execute_match_accu_outputs_running_count_and_accumulating_evidence() {
    // `on event<accu>` end-to-end (wp-labs/warp-fusion#65): 5 events, threshold
    // 2 → 4 alerts with event_count 2,3,4,5 and evidence growing each fire.
    use crate::match_engine::cep::{CepStateMachine, StepResult};

    let source = r#"
rule accu_evidence {
    events { s : auth_events }
    match<sip:100s> {
        on event<accu> { hit: s | count >= 2; }
    } -> score(70.0)
    entity(ip, s.sip)
    yield out (
        sip = s.sip,
        event_count = stat.count(window_event(s)),
        evidences = collect_set(s.event_id)
    )
}
"#;
    let file = wf_lang::parse_wfl(source).expect("parse should succeed");
    let plan = wf_lang::compile_wfl(&file, &[evidence_input_window(), evidence_output_window()])
        .expect("compile should succeed")
        .into_iter()
        .next()
        .expect("rule plan should exist");
    assert!(plan.match_plan.accu, "on event<accu> must set plan.accu");

    let exec = RuleExecutor::new(plan.clone());
    let mut sm = CepStateMachine::new(plan.name.clone(), plan.match_plan.clone(), None);
    let mut counts = Vec::new();
    let mut evidences = Vec::new();
    for i in 0..5 {
        let event_id = format!("evt_{:03}", i + 1);
        let step = sm.advance_at(
            "s",
            &event(vec![
                ("sip", str_val("10.0.0.1")),
                ("event_id", evidence_event(&event_id)),
            ]),
            (i as i64 + 1) * 1_000_000_000,
        );
        if let StepResult::Matched(ctx) = step {
            let alert = exec.execute_match(&ctx).expect("alert");
            let field = |name: &str| {
                alert
                    .yield_fields
                    .iter()
                    .find(|(n, _)| &**n == name)
                    .map(|(_, v)| v.clone())
            };
            counts.push(field("event_count"));
            evidences.push(field("evidences"));
        }
    }

    assert_eq!(
        counts,
        vec![
            Some(num(2.0)),
            Some(num(3.0)),
            Some(num(4.0)),
            Some(num(5.0)),
        ],
        "accu must output the running cumulative count"
    );
    assert_eq!(
        evidences,
        vec![
            Some(Value::Array(vec![str_val("evt_001"), str_val("evt_002")])),
            Some(Value::Array(vec![
                str_val("evt_001"),
                str_val("evt_002"),
                str_val("evt_003"),
            ])),
            Some(Value::Array(vec![
                str_val("evt_001"),
                str_val("evt_002"),
                str_val("evt_003"),
                str_val("evt_004"),
            ])),
            Some(Value::Array(vec![
                str_val("evt_001"),
                str_val("evt_002"),
                str_val("evt_003"),
                str_val("evt_004"),
                str_val("evt_005"),
            ])),
        ],
        "accu evidence must accumulate across fires"
    );
}

#[test]
fn execute_match_yield_dedups_window_event_ids() {
    use crate::match_engine::cep::{CepStateMachine, StepResult};

    let source = r#"
rule evidence_rule {
    events { s : auth_events }
    match<sip:5m> {
        on event { hit: s | count >= 6; }
    } -> score(70.0)
    entity(ip, s.sip)
    yield out (
        sip = s.sip,
        event_count = stat.count(window_event(s)),
        evidences = collect_set(s.event_id)
    )
}
"#;
    let file = wf_lang::parse_wfl(source).expect("parse should succeed");
    let plan = wf_lang::compile_wfl(&file, &[evidence_input_window(), evidence_output_window()])
        .expect("compile should succeed")
        .into_iter()
        .next()
        .expect("rule plan should exist");

    let exec = RuleExecutor::new(plan.clone());
    let mut sm = CepStateMachine::new(plan.name.clone(), plan.match_plan.clone(), None);
    let ids = [
        "evt_001", "evt_002", "evt_002", "evt_003", "evt_001", "evt_004",
    ];
    let mut matched = None;
    for (i, event_id) in ids.iter().enumerate() {
        let step = sm.advance_at(
            "s",
            &event(vec![
                ("sip", str_val("10.0.0.1")),
                ("event_id", evidence_event(event_id)),
            ]),
            (i as i64 + 1) * 1_000_000_000,
        );
        if i < 5 {
            assert_eq!(step, StepResult::Accumulate);
        } else {
            let StepResult::Matched(ctx) = step else {
                panic!("sixth event should trigger");
            };
            matched = Some(ctx);
        }
    }

    let alert = exec
        .execute_match(&matched.expect("matched context"))
        .expect("alert");
    let field = |name: &str| {
        alert
            .yield_fields
            .iter()
            .find(|(field_name, _)| &**field_name == name)
            .map(|(_, value)| value.clone())
    };

    assert_eq!(field("event_count"), Some(num(6.0)));
    assert_eq!(
        field("evidences"),
        Some(Value::Array(vec![
            str_val("evt_001"),
            str_val("evt_002"),
            str_val("evt_003"),
            str_val("evt_004"),
        ]))
    );
}

#[test]
fn execute_match_yield_missing_window_event_ids_returns_empty_evidences() {
    use crate::match_engine::cep::{CepStateMachine, StepResult};

    let source = r#"
rule evidence_rule {
    events { s : auth_events }
    match<sip:5m> {
        on event { hit: s.weight | sum >= 6; }
    } -> score(70.0)
    entity(ip, s.sip)
    yield out (
        sip = s.sip,
        event_count = stat.count(window_event(s)),
        evidences = collect_set(s.event_id)
    )
}
"#;
    let file = wf_lang::parse_wfl(source).expect("parse should succeed");
    let plan = wf_lang::compile_wfl(&file, &[evidence_input_window(), evidence_output_window()])
        .expect("compile should succeed")
        .into_iter()
        .next()
        .expect("rule plan should exist");
    assert!(
        plan.match_plan
            .tracked_bind_fields
            .get("s")
            .is_some_and(|fields| fields.contains("event_id"))
    );

    let exec = RuleExecutor::new(plan.clone());
    let mut sm = CepStateMachine::new(plan.name.clone(), plan.match_plan.clone(), None);
    let mut matched = None;
    for i in 0..6 {
        let step = sm.advance_at(
            "s",
            &event(vec![("sip", str_val("10.0.0.1")), ("weight", num(1.0))]),
            (i as i64 + 1) * 1_000_000_000,
        );
        if i < 5 {
            assert_eq!(step, StepResult::Accumulate);
        } else {
            let StepResult::Matched(ctx) = step else {
                panic!("sixth event should trigger");
            };
            matched = Some(ctx);
        }
    }

    let alert = exec
        .execute_match(&matched.expect("matched context"))
        .expect("alert");
    let field = |name: &str| {
        alert
            .yield_fields
            .iter()
            .find(|(field_name, _)| &**field_name == name)
            .map(|(_, value)| value.clone())
    };

    assert_eq!(field("event_count"), Some(num(6.0)));
    assert_eq!(field("evidences"), Some(Value::Array(vec![])));
}

#[test]
fn execute_match_yield_caps_window_event_ids_to_recent_sample() {
    use crate::match_engine::cep::CepStateMachine;

    let source = r#"
rule evidence_rule {
    events { s : auth_events }
    match<sip:5m> {
        on event { hit: s | count >= 2065; }
    } -> score(70.0)
    entity(ip, s.sip)
    yield out (
        sip = s.sip,
        event_count = stat.count(window_event(s)),
        evidences = collect_set(s.event_id)
    )
}
"#;
    let file = wf_lang::parse_wfl(source).expect("parse should succeed");
    let plan = wf_lang::compile_wfl(&file, &[evidence_input_window(), evidence_output_window()])
        .expect("compile should succeed")
        .into_iter()
        .next()
        .expect("rule plan should exist");

    let exec = RuleExecutor::new(plan.clone());
    let mut sm = CepStateMachine::new(plan.name.clone(), plan.match_plan.clone(), None);
    let mut matched = None;
    for i in 0..2065 {
        let event_id = format!("evt_{:04}", i);
        if let crate::match_engine::cep::StepResult::Matched(ctx) = sm.advance_at(
            "s",
            &event(vec![
                ("sip", str_val("10.0.0.1")),
                ("event_id", evidence_event(&event_id)),
            ]),
            (i as i64 + 1) * 1_000_000,
        ) {
            matched = Some(ctx);
        }
    }

    let alert = exec
        .execute_match(&matched.expect("matched context"))
        .expect("alert");
    let field = |name: &str| {
        alert
            .yield_fields
            .iter()
            .find(|(field_name, _)| &**field_name == name)
            .map(|(_, value)| value.clone())
    };

    assert_eq!(field("event_count"), Some(num(2065.0)));
    let Some(Value::Array(evidences)) = field("evidences") else {
        panic!("evidences should be an array");
    };
    assert_eq!(evidences.len(), 1024);
    assert_eq!(evidences.first(), Some(&str_val("evt_1041")));
    assert_eq!(evidences.last(), Some(&str_val("evt_2064")));
}

#[test]
fn execute_close_yield_collects_window_event_ids() {
    use crate::match_engine::cep::{CepStateMachine, StepResult};

    let source = r#"
rule evidence_close_rule {
    events { s : auth_events }
    match<sip:5m> {
        on event { hit: s | count >= 6; }
        and close { final_hit: s | count >= 6; }
    } -> score(70.0)
    entity(ip, s.sip)
    yield out (
        sip = s.sip,
        event_count = stat.count(window_event(s)),
        evidences = collect_set(s.event_id)
    )
}
"#;
    let file = wf_lang::parse_wfl(source).expect("parse should succeed");
    let plan = wf_lang::compile_wfl(&file, &[evidence_input_window(), evidence_output_window()])
        .expect("compile should succeed")
        .into_iter()
        .next()
        .expect("rule plan should exist");

    let exec = RuleExecutor::new(plan.clone());
    let mut sm = CepStateMachine::new(plan.name.clone(), plan.match_plan.clone(), None);
    for i in 0..6 {
        let event_id = format!("evt_{:03}", i + 1);
        let step = sm.advance_at(
            "s",
            &event(vec![
                ("sip", str_val("10.0.0.1")),
                ("event_id", evidence_event(&event_id)),
            ]),
            (i as i64 + 1) * 1_000_000_000,
        );
        if i < 5 {
            assert_eq!(step, StepResult::Accumulate);
        } else {
            assert_eq!(step, StepResult::Advance);
        }
    }

    let outputs = sm.close_all(CloseReason::Timeout);
    assert_eq!(outputs.len(), 1);
    let alert = exec
        .execute_close(&outputs[0])
        .expect("close should execute")
        .expect("close should emit");
    let field = |name: &str| {
        alert
            .yield_fields
            .iter()
            .find(|(field_name, _)| &**field_name == name)
            .map(|(_, value)| value.clone())
    };

    assert_eq!(field("event_count"), Some(num(6.0)));
    assert_eq!(
        field("evidences"),
        Some(Value::Array(vec![
            str_val("evt_001"),
            str_val("evt_002"),
            str_val("evt_003"),
            str_val("evt_004"),
            str_val("evt_005"),
            str_val("evt_006"),
        ]))
    );
}

#[test]
fn execute_close_yield_can_use_stat_final_value() {
    use crate::match_engine::cep::{CepStateMachine, StepResult};
    use wf_lang::{BaseType, FieldDef, FieldType, WindowSchema};

    let input_window = WindowSchema {
        name: "auth_events".into(),
        streams: vec!["auth_stream".into()],
        time_field: Some("event_time".into()),
        over: std::time::Duration::from_secs(3600),
        fields: vec![
            FieldDef {
                name: "sip".into(),
                field_type: FieldType::Base(BaseType::Ip),
            },
            FieldDef {
                name: "event_time".into(),
                field_type: FieldType::Base(BaseType::Time),
            },
        ],
    };
    let output_window = WindowSchema {
        name: "out".into(),
        streams: vec![],
        time_field: None,
        over: std::time::Duration::from_secs(3600),
        fields: vec![
            FieldDef {
                name: "sip".into(),
                field_type: FieldType::Base(BaseType::Ip),
            },
            FieldDef {
                name: "final_hits".into(),
                field_type: FieldType::Base(BaseType::Float),
            },
        ],
    };
    let source = r#"
rule stat_close_rule {
    events { req : auth_events  resp : auth_events }
    match<sip:5m> {
        on event { start: req | count >= 1; }
        and close { final_hits: resp | count >= 2; }
    } -> score(70.0)
    entity(ip, req.sip)
    yield out (
        sip = req.sip,
        final_hits = stat.value(final(final_hits))
    )
}
"#;
    let file = wf_lang::parse_wfl(source).expect("parse should succeed");
    let plan = wf_lang::compile_wfl(&file, &[input_window, output_window])
        .expect("compile should succeed")
        .into_iter()
        .next()
        .expect("rule plan should exist");

    let exec = RuleExecutor::new(plan.clone());
    let mut sm = CepStateMachine::new(plan.name.clone(), plan.match_plan.clone(), None);
    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    assert_eq!(sm.advance_at("req", &e, 1_000_000_000), StepResult::Advance);
    assert_eq!(
        sm.advance_at("resp", &e, 2_000_000_000),
        StepResult::Accumulate
    );
    assert_eq!(
        sm.advance_at("resp", &e, 3_000_000_000),
        StepResult::Accumulate
    );

    let close = sm
        .close(&[str_val("10.0.0.1")], CloseReason::Flush)
        .expect("close output");
    assert!(close.event_ok);
    assert!(close.close_ok);

    let alert = exec
        .execute_close(&close)
        .expect("close should execute")
        .expect("close should emit alert");
    let final_hits = alert
        .yield_fields
        .iter()
        .find(|(field_name, _)| &**field_name == "final_hits")
        .map(|(_, value)| value.clone());

    assert_eq!(final_hits, Some(num(2.0)));
}

// =========================================================================
// Missing optional fields (wp-labs/warp-fusion#62)
// =========================================================================

#[test]
fn execute_each_missing_optional_float_field_is_omitted_not_fatal() {
    // A yield passthrough of an optional float field that is missing from the
    // input must omit the field from the output record instead of failing the
    // whole record. Explicit NaN/Infinity must still fail (handled in the
    // coercion branch), but "absent" is not a data-format error.
    let mut plan = simple_rule_plan(
        "r1",
        simple_plan(vec![], vec![]),
        Expr::Number(10.0),
        "ip",
        Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    plan.yield_plan.fields = vec![YieldField {
        name: "attacker_latitude".into(),
        value: Expr::Field(FieldRef::Qualified("e".into(), "attacker_latitude".into())),
    }];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([("attacker_latitude".into(), FieldType::Base(BaseType::Float))]),
    );

    // Input event has no `attacker_latitude` field at all.
    let alert = exec
        .execute_each(&event(vec![("sip", str_val("10.0.0.1"))]), 1_000_000)
        .expect("missing optional field must not fail the yield")
        .expect("on each should still emit an output record");

    assert!(
        !alert
            .yield_fields
            .iter()
            .any(|(name, _)| &**name == "attacker_latitude"),
        "missing optional float field should be omitted from output"
    );
}

// =========================================================================
// Missing optional fields — present / explicit-NaN / other-fields cases
// (wp-labs/warp-fusion#62)

#[test]
fn execute_each_present_float_field_outputs_normally() {
    let mut plan = simple_rule_plan(
        "r1",
        simple_plan(vec![], vec![]),
        Expr::Number(10.0),
        "ip",
        Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    plan.yield_plan.fields = vec![YieldField {
        name: "attacker_latitude".into(),
        value: Expr::Field(FieldRef::Qualified("e".into(), "attacker_latitude".into())),
    }];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([("attacker_latitude".into(), FieldType::Base(BaseType::Float))]),
    );

    // Present and finite → the field is output unchanged.
    let alert = exec
        .execute_each(
            &event(vec![
                ("sip", str_val("10.0.0.1")),
                ("attacker_latitude", num(37.7749)),
            ]),
            1_000_000,
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(n, _)| &**n == "attacker_latitude")
            .map(|(_, v)| v.clone()),
        Some(num(37.7749))
    );
}

#[test]
fn execute_each_explicit_nan_float_still_fails() {
    let mut plan = simple_rule_plan(
        "r1",
        simple_plan(vec![], vec![]),
        Expr::Number(10.0),
        "ip",
        Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    plan.yield_plan.fields = vec![YieldField {
        name: "attacker_latitude".into(),
        value: Expr::Number(f64::NAN),
    }];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([("attacker_latitude".into(), FieldType::Base(BaseType::Float))]),
    );

    // Explicit NaN is a genuine data-format error, not an absent value.
    let result = exec.execute_each(&event(vec![("sip", str_val("10.0.0.1"))]), 1_000_000);
    assert!(result.is_err(), "explicit NaN must still fail the yield");
}

#[test]
fn execute_each_missing_optional_field_keeps_other_fields() {
    let mut plan = simple_rule_plan(
        "r1",
        simple_plan(vec![], vec![]),
        Expr::Number(10.0),
        "ip",
        Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    plan.yield_plan.fields = vec![
        YieldField {
            name: "attacker_latitude".into(),
            value: Expr::Field(FieldRef::Qualified("e".into(), "attacker_latitude".into())),
        },
        YieldField {
            name: "sip".into(),
            value: Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
        },
    ];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([
            ("attacker_latitude".into(), FieldType::Base(BaseType::Float)),
            ("sip".into(), FieldType::Base(BaseType::Chars)),
        ]),
    );

    // `attacker_latitude` missing; `sip` present. Only the missing one is
    // omitted; `sip` still emits.
    let alert = exec
        .execute_each(&event(vec![("sip", str_val("10.0.0.1"))]), 1_000_000)
        .unwrap()
        .unwrap();
    assert!(
        !alert
            .yield_fields
            .iter()
            .any(|(n, _)| &**n == "attacker_latitude"),
        "missing float field omitted"
    );
    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(n, _)| &**n == "sip")
            .map(|(_, v)| v.clone()),
        Some(str_val("10.0.0.1")),
        "present sibling field still emitted"
    );
}

#[test]
fn execute_each_missing_optional_digit_field_is_omitted() {
    // The empty-string guard applies to every non-chars base type, not just float.
    let mut plan = simple_rule_plan(
        "r1",
        simple_plan(vec![], vec![]),
        Expr::Number(10.0),
        "ip",
        Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    plan.yield_plan.fields = vec![YieldField {
        name: "fail_count".into(),
        value: Expr::Field(FieldRef::Qualified("e".into(), "fail_count".into())),
    }];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([("fail_count".into(), FieldType::Base(BaseType::Digit))]),
    );

    let alert = exec
        .execute_each(&event(vec![("sip", str_val("10.0.0.1"))]), 1_000_000)
        .unwrap()
        .unwrap();
    assert!(!alert.yield_fields.iter().any(|(n, _)| &**n == "fail_count"));
}

#[test]
fn execute_each_missing_chars_field_degrades_to_empty_string() {
    // Chars is exempt from the omit guard: a missing chars field still degrades
    // to the empty-string fallback (unchanged behavior).
    let mut plan = simple_rule_plan(
        "r1",
        simple_plan(vec![], vec![]),
        Expr::Number(10.0),
        "ip",
        Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    plan.yield_plan.fields = vec![YieldField {
        name: "message".into(),
        value: Expr::Field(FieldRef::Qualified("e".into(), "message".into())),
    }];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([("message".into(), FieldType::Base(BaseType::Chars))]),
    );

    let alert = exec
        .execute_each(&event(vec![("sip", str_val("10.0.0.1"))]), 1_000_000)
        .unwrap()
        .unwrap();
    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(n, _)| &**n == "message")
            .map(|(_, v)| v.clone()),
        Some(Value::Str(String::new().into()))
    );
}

#[test]
fn execute_close_missing_optional_float_field_is_omitted_not_fatal() {
    let mut plan = simple_rule_plan(
        "r1",
        default_match_plan(),
        Expr::Number(70.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    plan.yield_plan.fields = vec![YieldField {
        name: "attacker_latitude".into(),
        value: Expr::Field(FieldRef::Qualified("e".into(), "attacker_latitude".into())),
    }];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([("attacker_latitude".into(), FieldType::Base(BaseType::Float))]),
    );
    let close = CloseOutput {
        rule_name: "r1".into(),
        scope_key: vec![str_val("10.0.0.1")],
        close_reason: CloseReason::Timeout,
        event_ok: true,
        close_ok: true,
        close_mode: wf_lang::ast::CloseMode::And,
        event_emitted: false,
        event_step_data: vec![StepData {
            satisfied_branch_index: 0,
            label: Some("fail".into()),
            measure_value: 3.0,
            event_first_time_nanos: None,
            event_last_time_nanos: None,
            collected_values: Vec::new(),
            field_values: EngineHashMap::default(),
        }],
        close_step_data: vec![],
        bind_data: vec![],
        watermark_nanos: 0,
        event_first_time_nanos: 0,
        event_last_time_nanos: 0,
        first_match_time_nanos: None,
        evidence_first_time_nanos: 0,
        evidence_last_time_nanos: 0,
        window_start_time_nanos: 0,
        window_end_time_nanos: 0,
        machine_id: String::new(),
        last_event_nanos: 123,
        row_fields: None,
        row_field_names: None,
    };

    let alert = exec
        .execute_close(&close)
        .expect("close yield must not fail on a missing optional field")
        .expect("close should emit an output record");
    assert!(
        !alert
            .yield_fields
            .iter()
            .any(|(n, _)| &**n == "attacker_latitude"),
        "missing typed float field should be omitted from close output"
    );
}
