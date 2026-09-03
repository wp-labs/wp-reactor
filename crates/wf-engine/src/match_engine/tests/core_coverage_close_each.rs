//! core_coverage 拆出的兄弟子模块（2026-09-04）：close / on-each 收口与
//! 输出后处理覆盖——close_exec.rs（execute_close 未限定/限定/带 join 抑制/
//! 列式安全门变体）、each_exec.rs（execute_each 过滤 / let / where）、
//! conv.rs（apply_conv sort / top / dedup / where 管道）与 contract.rs
//! （内联 harness 失败分支：hits 失配、hit 越界、引用 join 窗口拒绝）。
//! sample_close 独占 helper 随迁；JoinLookup / join_plan / eq_str_expr /
//! step_data 与 import 在父模块 core_coverage.rs，经 `use super::*` 复用。

use super::*;

// ===========================================================================
// executor/close_exec.rs — close 收口执行
// ===========================================================================

fn sample_close(close_mode: CloseMode, event_ok: bool, close_ok: bool) -> CloseOutput {
    CloseOutput {
        rule_name: "r_close".to_string(),
        scope_key: vec![str_val("10.0.0.1")],
        close_reason: CloseReason::Timeout,
        event_ok,
        close_ok,
        close_mode,
        event_emitted: false,
        event_step_data: vec![step_data(Some("fail"), 3.0, vec![])],
        close_step_data: vec![],
        bind_data: vec![],
        watermark_nanos: 1_000,
        machine_id: String::new(),
        event_first_time_nanos: 0,
        event_last_time_nanos: 1_000,
        first_match_time_nanos: None,
        evidence_first_time_nanos: 0,
        evidence_last_time_nanos: 1_000,
        window_start_time_nanos: 0,
        window_end_time_nanos: 1_000,
        last_event_nanos: 1_000,
        row_fields: None,
        row_field_names: None,
    }
}

#[test]
fn execute_close_unqualified_returns_none() {
    let exec = RuleExecutor::new(simple_rule_plan(
        "r_close",
        simple_plan(vec![simple_key("sip")], vec![]),
        Expr::Number(70.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    ));
    // Or mode with no close steps → never qualifies (event path owns output).
    let or_no_close = sample_close(CloseMode::Or, true, true);
    assert!(exec.execute_close(&or_no_close).unwrap().is_none());
    // And mode with event_ok=false → suppressed.
    let and_missing_event = sample_close(CloseMode::And, false, true);
    assert!(exec.execute_close(&and_missing_event).unwrap().is_none());
    // And mode with close_ok=false → suppressed.
    let and_missing_close = sample_close(CloseMode::And, true, false);
    assert!(exec.execute_close(&and_missing_close).unwrap().is_none());
}

#[test]
fn execute_close_qualified_builds_alert() {
    let exec = RuleExecutor::new(simple_rule_plan(
        "r_close",
        simple_plan(vec![simple_key("sip")], vec![]),
        Expr::Number(70.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    ));
    let close = sample_close(CloseMode::And, true, true);
    let alert = exec
        .execute_close(&close)
        .unwrap()
        .expect("qualified close fires");
    assert_eq!(&*alert.rule_name, "r_close");
    assert_eq!(alert.entity_id, "10.0.0.1");
    assert!((alert.score - 70.0).abs() < f64::EPSILON);
    assert_eq!(alert.origin.as_str(), "close:timeout");
    assert!(alert.summary.contains("fail=3.0"));
}

#[test]
fn execute_close_with_joins_suppressed_on_inner_miss() {
    let mut plan = simple_rule_plan(
        "r_close_join",
        simple_plan(vec![simple_key("sip")], vec![]),
        Expr::Number(70.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    );
    plan.joins = vec![join_plan(JoinMode::Inner, "geo", "sip", "ip")];
    let exec = RuleExecutor::new(plan);
    let close = sample_close(CloseMode::And, true, true);

    // Join miss → close output suppressed (D4 miss → drop).
    let lookup = JoinLookup::new();
    assert!(
        exec.execute_close_with_joins(&close, &lookup)
            .unwrap()
            .is_none()
    );

    // Join hit → enriched close alert, and where passes.
    let mut lookup = JoinLookup::new();
    lookup.add_row(
        "geo",
        vec![("ip", str_val("10.0.0.1")), ("country", str_val("US"))],
    );
    let alert = exec
        .execute_close_with_joins(&close, &lookup)
        .unwrap()
        .unwrap();
    assert_eq!(alert.entity_id, "10.0.0.1");

    // Post-join `where` suppresses when it fails.
    let mut plan2 = simple_rule_plan(
        "r_close_where",
        simple_plan(vec![simple_key("sip")], vec![]),
        Expr::Number(70.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    );
    plan2.joins = vec![join_plan(JoinMode::Inner, "geo", "sip", "ip")];
    plan2.r#where = Some(eq_str_expr("country", "US"));
    let exec2 = RuleExecutor::new(plan2);
    let mut lookup_bad = JoinLookup::new();
    lookup_bad.add_row(
        "geo",
        vec![("ip", str_val("10.0.0.1")), ("country", str_val("DE"))],
    );
    assert!(
        exec2
            .execute_close_with_joins(&close, &lookup_bad)
            .unwrap()
            .is_none()
    );
    let mut lookup_good = JoinLookup::new();
    lookup_good.add_row(
        "geo",
        vec![("ip", str_val("10.0.0.1")), ("country", str_val("US"))],
    );
    assert!(
        exec2
            .execute_close_with_joins(&close, &lookup_good)
            .unwrap()
            .is_some()
    );
}

#[test]
fn close_plan_columnar_safe_gate_variants() {
    let base = || {
        simple_rule_plan(
            "r_safe",
            simple_plan(vec![simple_key("sip")], vec![]),
            Expr::Number(70.0),
            "ip",
            Expr::Field(FieldRef::Simple("sip".to_string())),
        )
    };
    assert!(RuleExecutor::new(base()).close_plan_columnar_safe());

    // Non-constant score → unsafe.
    let mut p = base();
    p.score_plan.expr = Expr::Field(FieldRef::Simple("sip".into()));
    assert!(!RuleExecutor::new(p).close_plan_columnar_safe());

    // Entity path ref → unsafe.
    let mut p = base();
    p.entity_plan.entity_id_expr = Expr::Field(FieldRef::Path {
        alias: "e".into(),
        segments: vec![PathSegment::Field("roles_obj".into())],
    });
    assert!(!RuleExecutor::new(p).close_plan_columnar_safe());

    // Entity field with a synthetic `_` prefix → unsafe.
    let mut p = base();
    p.entity_plan.entity_id_expr = Expr::Field(FieldRef::Simple("_step_0_measure".into()));
    assert!(!RuleExecutor::new(p).close_plan_columnar_safe());

    // Yield with a general expression referencing plain fields → safe
    // （2026-08-25 扩展: 列式 close 对 General 走轻量 ctx 求值）。
    let mut p = base();
    p.yield_plan.fields = vec![YieldField {
        name: "f".into(),
        value: Expr::BinOp {
            op: BinOp::Add,
            left: Box::new(Expr::Field(FieldRef::Simple("sip".into()))),
            right: Box::new(Expr::Number(2.0)),
        },
    }];
    assert!(RuleExecutor::new(p).close_plan_columnar_safe());
    // General referencing a synthetic `_step_*` field → unsafe（Named 窄化不注入）。
    let mut p = base();
    p.yield_plan.fields = vec![YieldField {
        name: "f".into(),
        value: Expr::Field(FieldRef::Simple("_step_0_measure".into())),
    }];
    assert!(!RuleExecutor::new(p).close_plan_columnar_safe());

    // Joins present → unsafe.
    let mut p = base();
    p.joins = vec![join_plan(JoinMode::Snapshot, "geo", "sip", "ip")];
    assert!(!RuleExecutor::new(p).close_plan_columnar_safe());

    // Literal yields + StringLit entity → safe.
    let mut p = base();
    p.entity_plan.entity_id_expr = Expr::StringLit("fixed-entity".into());
    p.yield_plan.fields = vec![
        YieldField {
            name: "n".into(),
            value: Expr::Number(1.0),
        },
        YieldField {
            name: "s".into(),
            value: Expr::StringLit("x".into()),
        },
        YieldField {
            name: "b".into(),
            value: Expr::Bool(true),
        },
        YieldField {
            name: "f".into(),
            value: Expr::Field(FieldRef::Simple("sip".into())),
        },
    ];
    assert!(RuleExecutor::new(p).close_plan_columnar_safe());
}

// ===========================================================================
// executor/each_exec.rs — on-each 执行
// ===========================================================================

#[test]
fn execute_each_requires_each_plan() {
    let plain = RuleExecutor::new(simple_rule_plan(
        "r_plain",
        simple_plan(vec![], vec![]),
        Expr::Number(1.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    ));
    assert!(plain.execute_each(&event(vec![]), 0).is_err());
    assert!(
        plain
            .execute_each_with_joins(&event(vec![]), 0, &JoinLookup::new(), &[], 0)
            .is_err()
    );
}

#[test]
fn execute_each_filter_and_lets_and_where() {
    // Filter rejects non-matching events.
    let mut plan = simple_rule_plan(
        "r_each_f",
        simple_plan(vec![], vec![]),
        Expr::Number(42.5),
        "ip",
        Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
    );
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: Some(eq_str_expr("sip", "10.0.0.1")),
    });
    let exec = RuleExecutor::new(plan);
    assert!(
        exec.execute_each(&event(vec![("sip", str_val("10.0.0.2"))]), 0)
            .unwrap()
            .is_none()
    );
    let alert = exec
        .execute_each(&event(vec![("sip", str_val("10.0.0.1"))]), 0)
        .unwrap()
        .unwrap();
    assert!((alert.score - 42.5).abs() < f64::EPSILON);
    assert_eq!(alert.entity_id, "10.0.0.1");

    // `let` bindings inject computed values into the eval context.
    let mut plan = simple_rule_plan(
        "r_each_let",
        simple_plan(vec![], vec![]),
        Expr::Number(10.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    plan.lets = vec![LetPlan {
        name: "double".into(),
        expr: Expr::BinOp {
            op: BinOp::Mul,
            left: Box::new(Expr::Field(FieldRef::Simple("x".into()))),
            right: Box::new(Expr::Number(2.0)),
        },
    }];
    plan.yield_plan.fields = vec![YieldField {
        name: "double".into(),
        value: Expr::Field(FieldRef::Simple("double".into())),
    }];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([("double".into(), FieldType::Base(BaseType::Float))]),
    );
    let alert = exec
        .execute_each(&event(vec![("x", num(21.0))]), 0)
        .unwrap()
        .unwrap();
    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(n, _)| &**n == "double")
            .map(|(_, v)| v.clone()),
        Some(Value::Number(42.0))
    );

    // `where` after the ctx path: with a `let` present the where is evaluated.
    let mut plan = simple_rule_plan(
        "r_each_where",
        simple_plan(vec![], vec![]),
        Expr::Number(1.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    plan.lets = vec![LetPlan {
        name: "k".into(),
        expr: Expr::Number(1.0),
    }];
    plan.r#where = Some(eq_str_expr("sip", "10.0.0.1"));
    let exec = RuleExecutor::new(plan);
    assert!(
        exec.execute_each_with_joins(
            &event(vec![("sip", str_val("10.0.0.2"))]),
            0,
            &JoinLookup::new(),
            &[],
            0
        )
        .unwrap()
        .is_none()
    );
    assert!(
        exec.execute_each_with_joins(
            &event(vec![("sip", str_val("10.0.0.1"))]),
            0,
            &JoinLookup::new(),
            &[],
            0
        )
        .unwrap()
        .is_some()
    );
}

// ===========================================================================
// match_engine/conv.rs — apply_conv pipelines
// ===========================================================================

#[test]
fn apply_conv_sort_top_dedup_where_pipelines() {
    let keys = vec![simple_key("sip")];
    fn out(sip: &str, score: f64) -> CloseOutput {
        let mut c = sample_close(CloseMode::And, true, true);
        c.scope_key = vec![str_val(sip)];
        // Encode the score through the step label so conv exprs can read it.
        c.event_step_data = vec![step_data(Some("score"), score, vec![])];
        c
    }
    let a = out("a", 3.0);
    let b = out("b", 1.0);
    let c = out("c", 2.0);
    let dup = out("a", 3.0);

    let score_expr = || Expr::Field(FieldRef::Simple("score".into()));
    // sort(score desc) | top(2) | dedup(score) | where(score >= 2)
    let plan = wf_lang::plan::ConvPlan {
        chains: vec![ConvChainPlan {
            ops: vec![
                ConvOpPlan::Sort(vec![SortKeyPlan {
                    expr: score_expr(),
                    descending: true,
                }]),
                ConvOpPlan::Top(2),
                ConvOpPlan::Dedup(score_expr()),
                ConvOpPlan::Where(Expr::BinOp {
                    op: BinOp::Ge,
                    left: Box::new(score_expr()),
                    right: Box::new(Expr::Number(2.0)),
                }),
            ],
        }],
    };
    let out = apply_conv(&plan, &keys, vec![a, b, c, dup]);
    // sort desc → [3,3,2,1]; top 2 → [3,3]; dedup → [3]; where ≥2 keeps 3.
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].scope_key, vec![str_val("a")]);
    assert_eq!(out[0].event_step_data[0].label.as_deref(), Some("score"));
    assert_eq!(out[0].event_step_data[0].measure_value, 3.0);
}

// ===========================================================================
// contract.rs — inline test harness failure branches
// ===========================================================================

fn auth_events_schema() -> wf_lang::WindowSchema {
    wf_lang::WindowSchema {
        name: "auth_events".to_string(),
        streams: vec!["auth_stream".to_string()],
        time_field: Some("event_time".to_string()),
        over: Duration::from_secs(3600),
        fields: vec![
            wf_lang::FieldDef {
                name: "sip".to_string(),
                field_type: FieldType::Base(BaseType::Ip),
            },
            wf_lang::FieldDef {
                name: "action".to_string(),
                field_type: FieldType::Base(BaseType::Chars),
            },
            wf_lang::FieldDef {
                name: "count".to_string(),
                field_type: FieldType::Base(BaseType::Digit),
            },
            wf_lang::FieldDef {
                name: "event_time".to_string(),
                field_type: FieldType::Base(BaseType::Time),
            },
        ],
    }
}

fn security_alerts_schema() -> wf_lang::WindowSchema {
    wf_lang::WindowSchema {
        name: "security_alerts".to_string(),
        streams: vec![],
        time_field: None,
        over: Duration::from_secs(3600),
        fields: vec![
            wf_lang::FieldDef {
                name: "sip".to_string(),
                field_type: FieldType::Base(BaseType::Ip),
            },
            wf_lang::FieldDef {
                name: "fail_count".to_string(),
                field_type: FieldType::Base(BaseType::Digit),
            },
        ],
    }
}

fn run_contract_from_source(source: &str) -> crate::match_engine::contract::TestResult {
    run_contract_from_source_with_schemas(
        source,
        vec![auth_events_schema(), security_alerts_schema()],
    )
}

fn run_contract_from_source_with_schemas(
    source: &str,
    schemas: Vec<wf_lang::WindowSchema>,
) -> crate::match_engine::contract::TestResult {
    let wfl_file = wf_lang::parse_wfl(source).expect("parse should succeed");
    let plans = wf_lang::compile_wfl(&wfl_file, &schemas).expect("compile should succeed");
    let test = &wfl_file.tests[0];
    let plan = plans
        .iter()
        .find(|p| p.name == test.rule_name)
        .unwrap_or_else(|| panic!("rule `{}` not found", test.rule_name));
    let time_field = schemas
        .iter()
        .find(|s| plan.binds.iter().any(|b| b.window == s.name))
        .and_then(|s| s.time_field.clone());
    crate::match_engine::contract::run_test(test, plan, time_field).expect("run_test succeeds")
}

#[test]
fn contract_hits_mismatch_records_failure() {
    let source = r#"
rule brute_force {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 5; } } -> score(70.0)
    entity(ip, e.sip)
    yield security_alerts (sip = e.sip, fail_count = 5)
}

test below_threshold for brute_force {
    input {
        row(e, sip = "10.0.0.1", action = "failed");
        row(e, sip = "10.0.0.1", action = "failed");
        row(e, sip = "10.0.0.1", action = "failed");
    }
    expect {
        hits == 1;
    }
}
"#;
    let result = run_contract_from_source(source);
    assert!(!result.passed);
    assert!(!result.failures.is_empty());
    assert!(
        result
            .failures
            .iter()
            .any(|f| f.contains("hits") && f.contains("expected")),
        "failures: {:?}",
        result.failures
    );
    assert_eq!(result.output_count, 0);
}

#[test]
fn contract_hit_assert_out_of_range_records_failure() {
    let source = r#"
rule brute_force {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(70.0)
    entity(ip, e.sip)
    yield security_alerts (sip = e.sip, fail_count = 1)
}

test single_hit for brute_force {
    input {
        row(e, sip = "10.0.0.1", action = "failed");
    }
    expect {
        hits == 1;
        hit[5].score >= 70;
    }
}
"#;
    let result = run_contract_from_source(source);
    assert!(!result.passed);
    assert!(
        result
            .failures
            .iter()
            .any(|f| f.contains("hit[5]") && f.contains("index out of range")),
        "failures: {:?}",
        result.failures
    );
    assert_eq!(result.output_count, 1);
}

#[test]
fn contract_rejects_where_referencing_joined_window() {
    // The `where` references a field of the joined window (`geo_lookup.region`)
    // — the inline harness cannot populate joined windows, so the contract
    // harness must reject the rule loudly instead of passing vacuously.
    let geo_lookup = wf_lang::WindowSchema {
        name: "geo_lookup".to_string(),
        streams: vec![],
        time_field: None,
        over: Duration::from_secs(3600),
        fields: vec![
            wf_lang::FieldDef {
                name: "sip".to_string(),
                field_type: FieldType::Base(BaseType::Ip),
            },
            wf_lang::FieldDef {
                name: "region".to_string(),
                field_type: FieldType::Base(BaseType::Chars),
            },
        ],
    };
    let source = r#"
rule enriched {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(70.0)
    join geo_lookup snapshot on e.sip == geo_lookup.sip
    where geo_lookup.region == "US"
    entity(ip, e.sip)
    yield security_alerts (sip = e.sip, fail_count = 1)
}

test joined_where for enriched {
    input {
        row(e, sip = "10.0.0.1", action = "failed");
    }
    expect {
        hits == 1;
    }
}
"#;
    let result = run_contract_from_source_with_schemas(
        source,
        vec![auth_events_schema(), geo_lookup, security_alerts_schema()],
    );
    assert!(!result.passed);
    assert!(
        result
            .failures
            .iter()
            .any(|f| f.contains("references joined window")),
        "failures: {:?}",
        result.failures
    );
}
