//! coverage_r4 拆出的兄弟子模块（2026-09-04）：`executor/mod.rs` 的
//! match-alert 输出覆盖——行式 execute_match(_at/_with_joins) 语义、列式批
//! execute_match_direct_batch_columnar 与 ctx-free（build_match_alert_free）
//! 对拍、列式安全门控。共享 harness 在父模块 `coverage_r4.rs`，此处经
//! `use super::*` 复用。

use super::*;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field as ArrowField};

use crate::alert::AlertColumnBuilder;
use crate::match_engine::TriggerEvent;
use crate::match_engine::cep::MatchedContext;
use wf_lang::ast::SystemVar;
use wf_lang::plan::YieldField;

fn matched_context(scope_key: Value, step: StepData) -> MatchedContext {
    MatchedContext {
        rule_name: "r1".to_string(),
        scope_key: vec![scope_key],
        step_data: vec![step],
        bind_data: vec![],
        event_time_nanos: 1_700_000_000_000_000_000,
        event_first_time_nanos: 0,
        event_last_time_nanos: 0,
        first_match_time_nanos: None,
        evidence_first_time_nanos: 0,
        evidence_last_time_nanos: 0,
        window_start_time_nanos: 0,
        window_end_time_nanos: 0,
        machine_id: String::new(),
        trigger_event: None,
    }
}

// ---------------------------------------------------------------------------
// executor/mod.rs — match-alert path (YieldKind::Field / General / Lit)
// ---------------------------------------------------------------------------

#[test]
fn execute_match_yield_kinds_and_coercion_omission() {
    let mut plan = simple_rule_plan(
        "match_r",
        default_match_plan(),
        Expr::Number(55.0),
        "ip",
        field("sip"),
    );
    plan.yield_plan.fields = vec![
        YieldField {
            name: "lit_field".into(),
            value: Expr::Number(9.0),
        },
        YieldField {
            name: "str_field".into(),
            value: Expr::StringLit("const".into()),
        },
        YieldField {
            name: "flag_field".into(),
            value: Expr::Bool(true),
        },
        YieldField {
            name: "general_field".into(),
            value: Expr::SystemVar(SystemVar::Score),
        },
        YieldField {
            name: "missing_typed".into(),
            value: field("ghost"),
        },
    ];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([("missing_typed".into(), FieldType::Base(BaseType::Float))]),
    );
    let sd = step_data(Some("fail"), 1.0);
    let matched = matched_context(str_val("10.0.0.1"), sd);
    let rec = exec.execute_match(&matched).unwrap();
    assert_eq!(rec.score, 55.0);
    let fields: HashMap<&str, &Value> = rec
        .yield_fields
        .iter()
        .map(|(k, v)| (k.as_ref(), v))
        .collect();
    assert_eq!(fields.get("lit_field"), Some(&&num(9.0)));
    assert_eq!(fields.get("str_field"), Some(&&str_val("const")));
    assert_eq!(fields.get("flag_field"), Some(&&Value::Bool(true)));
    assert_eq!(fields.get("general_field"), Some(&&num(55.0)));
    // Missing typed field → omitted (empty-string + non-Chars).
    assert!(!fields.contains_key("missing_typed"));
    // Machine id from the matched context.
    let mut matched2 = matched;
    matched2.machine_id = "m".into();
    let rec2 = exec.execute_match_at(&matched2, 1234).unwrap();
    assert_eq!(rec2.machine_id.as_ref(), "m");
}

#[test]
fn execute_match_with_joins_hit_miss_and_where() {
    let mut plan = simple_rule_plan(
        "match_join",
        default_match_plan(),
        Expr::Number(55.0),
        "ip",
        field("sip"),
    );
    plan.joins = vec![one_cond_join(JoinMode::Inner)];
    plan.r#where = Some(Expr::BinOp {
        op: BinOp::Eq,
        left: Box::new(field("flag")),
        right: Box::new(Expr::Bool(true)),
    });
    let exec = RuleExecutor::new(plan);
    let lookup = RowsLookup::new(vec![join_row_event(vec![("id", num(1.0))])]);

    // Join miss (no right rows) → Ok(None).
    let mut matched = matched_context(str_val("10.0.0.1"), step_data(Some("fail"), 1.0));
    matched.trigger_event = Some(TriggerEvent::from_event(Arc::new(event(vec![(
        "bidder",
        num(1.0),
    )]))));
    assert!(
        exec.execute_match_with_joins(&matched, &RowsLookup::new(vec![]))
            .unwrap()
            .is_none()
    );

    // Join hit but `where` absent from the ctx → Ok(None).
    assert!(
        exec.execute_match_with_joins(&matched, &lookup)
            .unwrap()
            .is_none()
    );

    // with_joins_at with a trigger event and where true → record.
    let mut matched2 = matched_context(str_val("10.0.0.1"), step_data(Some("fail"), 1.0));
    matched2.trigger_event = Some(TriggerEvent::from_event(Arc::new(event(vec![
        ("bidder", num(1.0)),
        ("flag", Value::Bool(true)),
    ]))));
    let rec = exec
        .execute_match_with_joins_at(&matched2, &lookup, 1234)
        .unwrap()
        .expect("record");
    assert_eq!(rec.score, 55.0);
}

#[test]
fn columnar_mask_helpers() {
    // Build a batch with one column and a plan with a bind filter to exercise
    // bind_filter_columnar_mask / each_filter_columnar_mask / branch_guard_masks.
    let schema = Arc::new(Schema::new(vec![ArrowField::new(
        "v",
        DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2]))]).unwrap();

    let mut plan = simple_rule_plan(
        "col",
        default_match_plan(),
        Expr::Number(55.0),
        "ip",
        field("sip"),
    );
    plan.binds[0].filter = Some(Expr::BinOp {
        op: BinOp::Ge,
        left: Box::new(field("v")),
        right: Box::new(Expr::Number(1.0)),
    });
    let exec = RuleExecutor::new(plan);

    // Columnar bind filter mask present and evaluates.
    let mask = exec.bind_filter_columnar_mask("fail", &batch);
    assert!(mask.is_some());
    assert_eq!(mask.unwrap().len(), 2);

    // No each plan → each_filter_columnar_mask is None.
    assert!(exec.each_filter_columnar_mask(&batch).is_none());

    // Branch guards: no guard → empty masks.
    let masks = exec.branch_guard_masks(&batch);
    assert!(masks.is_empty());

    // bind_filters_columnar_safe with a columnar filter → true.
    assert!(exec.bind_filters_columnar_safe("w"));
}

// ---------------------------------------------------------------------------
// ctx-free match emit（F8.5）：build_match_alert_free 与 Full ctx 逐字段对拍
// ---------------------------------------------------------------------------

#[test]
fn ctx_free_match_output_matches_full_ctx_bytes() {
    // q6 形状：键 seller、无 label step、entity digit(b.auction)、yield
    // id=b.auction + 字面量、score 常量、无 where/join → match_ctx_free=true。
    let mut plan = simple_rule_plan(
        "ctx_free_r",
        simple_plan(
            vec![simple_key("seller")],
            vec![step(vec![branch("b", count_ge(1.0))])],
        ),
        Expr::Number(20.0),
        "digit",
        Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
    );
    plan.binds[0].alias = "b".into();
    plan.binds[0].window = "bid_events".into();
    plan.yield_plan.fields = vec![
        YieldField {
            name: "id".into(),
            value: Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
        },
        YieldField {
            name: "alert_type".into(),
            value: Expr::StringLit("q6_avg200".into()),
        },
    ];
    let exec = RuleExecutor::new(plan);
    assert!(
        exec.output_static().match_ctx_free,
        "q6 形状（score 常量 + entity/yield Field/Lit + 无 where/join）必须启用 ctx-free"
    );

    const NOW: i64 = 1_700_000_000_000_000_000;
    let trigger = event(vec![
        ("auction", num(1001.0)),
        ("price", num(300.0)),
        ("bidder", num(5.0)),
    ]);
    let matched = MatchedContext {
        rule_name: "ctx_free_r".into(),
        scope_key: vec![num(20.0)],
        step_data: vec![StepData {
            satisfied_branch_index: 0,
            label: None,
            measure_value: 300.0,
            event_first_time_nanos: Some(NOW),
            event_last_time_nanos: Some(NOW),
            collected_values: vec![],
            field_values: EngineHashMap::default(),
        }],
        bind_data: vec![],
        event_time_nanos: NOW,
        event_first_time_nanos: NOW,
        event_last_time_nanos: NOW,
        first_match_time_nanos: None,
        evidence_first_time_nanos: NOW,
        evidence_last_time_nanos: NOW,
        window_start_time_nanos: NOW - 600_000_000_000,
        window_end_time_nanos: NOW,
        machine_id: String::new(),
        trigger_event: Some(TriggerEvent::from_event(Arc::new(trigger.clone()))),
    };

    // Full 路径：手动构造窄化 ctx（键字段 + trigger_event 字段——同
    // build_eval_context 的 Named 注入语义）。
    let mut fields = EngineHashMap::default();
    fields.insert("seller".into(), num(20.0));
    for (k, v) in &trigger.fields {
        fields.insert(k.clone(), v.clone());
    }
    let ctx = Event { fields };
    let full = exec.build_match_alert(&matched, &ctx, NOW).unwrap();

    // ctx-free 路径：字段直读 scope_key + trigger_event。
    let free = exec.build_match_alert_free(&matched, NOW).unwrap();

    // 逐字段字节一致（OutputRecord 无 PartialEq——手工比较输出相关字段）。
    assert_eq!(full.wfx_id, free.wfx_id, "wfx_id");
    assert_eq!(full.score, free.score, "score");
    assert_eq!(full.entity_id, free.entity_id, "entity_id");
    assert_eq!(full.scope_key, free.scope_key, "scope_key");
    assert_eq!(full.summary, free.summary, "summary");
    assert_eq!(full.fired_at, free.fired_at, "fired_at");
    assert_eq!(full.machine_id, free.machine_id, "machine_id");
    assert_eq!(full.yield_fields, free.yield_fields, "yield_fields");
    assert_eq!(
        full.yield_field_types, free.yield_field_types,
        "yield_field_types"
    );
    assert_eq!(
        full.event_time_nanos, free.event_time_nanos,
        "event_time_nanos"
    );
    assert_eq!(full.origin, free.origin, "origin");

    // gate 反向：带 where 的规则必须走 Full（禁用 ctx-free）。
    let mut plan_where = simple_rule_plan(
        "ctx_free_where",
        simple_plan(
            vec![simple_key("seller")],
            vec![step(vec![branch("b", count_ge(1.0))])],
        ),
        Expr::Number(20.0),
        "digit",
        Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
    );
    plan_where.binds[0].alias = "b".into();
    plan_where.binds[0].window = "bid_events".into();
    plan_where.r#where = Some(Expr::BinOp {
        op: BinOp::Ge,
        left: Box::new(Expr::Field(FieldRef::Qualified("b".into(), "price".into()))),
        right: Box::new(Expr::Number(100.0)),
    });
    plan_where.yield_plan.fields = vec![YieldField {
        name: "id".into(),
        value: Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
    }];
    let exec_where = RuleExecutor::new(plan_where);
    assert!(
        !exec_where.output_static().match_ctx_free,
        "带 where 的规则必须禁用 ctx-free（where 需要完整 ctx）"
    );

    // gate 反向：yield General 表达式（函数调用）必须禁用 ctx-free。
    let mut plan_gen = simple_rule_plan(
        "ctx_free_gen",
        simple_plan(
            vec![simple_key("seller")],
            vec![step(vec![branch("b", count_ge(1.0))])],
        ),
        Expr::Number(20.0),
        "digit",
        Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
    );
    plan_gen.binds[0].alias = "b".into();
    plan_gen.binds[0].window = "bid_events".into();
    plan_gen.yield_plan.fields = vec![YieldField {
        name: "detail".into(),
        value: Expr::BinOp {
            op: BinOp::Add,
            left: Box::new(Expr::Number(1.0)),
            right: Box::new(Expr::Number(2.0)),
        },
    }];
    let exec_gen = RuleExecutor::new(plan_gen);
    assert!(
        !exec_gen.output_static().match_ctx_free,
        "yield 含 General 表达式的规则必须禁用 ctx-free"
    );
}

#[test]
fn columnar_match_output_matches_row_path() {
    // q6 形状（无 join 变体）：键 seller、entity digit(b.auction)、yield
    // id=b.auction + 字面量、score 常量、无 where —— match_plan_columnar_safe
    // 通过，列式批输出与行式 `execute_match_at` 逐字段字节一致。
    let mut plan = simple_rule_plan(
        "match_col",
        simple_plan(
            vec![simple_key("seller")],
            vec![step(vec![branch("b", count_ge(1.0))])],
        ),
        Expr::Number(20.0),
        "digit",
        Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
    );
    plan.binds[0].alias = "b".into();
    plan.binds[0].window = "bid_events".into();
    plan.yield_plan.fields = vec![
        YieldField {
            name: "id".into(),
            value: Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
        },
        YieldField {
            name: "alert_type".into(),
            value: Expr::StringLit("q6_avg200".into()),
        },
    ];
    let exec = RuleExecutor::new(plan);
    assert!(
        exec.match_plan_columnar_safe(),
        "q6 形状（无 join）必须通过 match 列式门控"
    );

    const NOW: i64 = 1_700_000_000_000_000_000;
    let mk = |auction: f64, seller: f64| MatchedContext {
        rule_name: "match_col".into(),
        scope_key: vec![num(seller)],
        step_data: vec![StepData {
            satisfied_branch_index: 0,
            label: None,
            measure_value: auction,
            event_first_time_nanos: Some(NOW),
            event_last_time_nanos: Some(NOW),
            collected_values: vec![],
            field_values: EngineHashMap::default(),
        }],
        bind_data: vec![],
        event_time_nanos: NOW,
        event_first_time_nanos: NOW,
        event_last_time_nanos: NOW,
        first_match_time_nanos: None,
        evidence_first_time_nanos: NOW,
        evidence_last_time_nanos: NOW,
        window_start_time_nanos: NOW - 600_000_000_000,
        window_end_time_nanos: NOW,
        machine_id: String::new(),
        trigger_event: Some(TriggerEvent::from_event(Arc::new(event(vec![
            ("auction", num(auction)),
            ("price", num(300.0)),
            ("bidder", num(5.0)),
        ])))),
    };
    let m1 = mk(1001.0, 20.0);
    let m2 = mk(1002.0, 21.0);

    // 行式路径：每命中 execute_match_at → OutputRecord → append_record。
    let mut b_row = AlertColumnBuilder::new(Arc::from("alerts"));
    for m in [&m1, &m2] {
        let record = exec.execute_match_at(m, NOW).unwrap();
        b_row.append_record(&record).unwrap();
    }
    let out_row: Vec<_> = b_row
        .finish()
        .iter_data_records()
        .map(|r| r.unwrap())
        .collect();

    // 列式路径：批量直写 builder。
    let mut b_col = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut appended = Vec::new();
    let stats =
        exec.execute_match_direct_batch_columnar(&[&m1, &m2], NOW, &mut b_col, &mut appended);
    assert_eq!(stats.appended, 2);
    assert_eq!(stats.failed, 0);
    assert_eq!(appended, vec![0, 1]);
    let out_col: Vec<_> = b_col
        .finish()
        .iter_data_records()
        .map(|r| r.unwrap())
        .collect();

    // 逐字段字节一致。
    assert_eq!(out_row, out_col);
    // entity_id = auction（字符串化），两行各一条。
    let entity_ids: Vec<String> = out_col
        .iter()
        .map(|r| {
            r.fields()
                .find(|f| f.get_name() == wf_lang::wfu_meta::WFU_ENTITY_ID)
                .and_then(|f| f.get_chars().map(str::to_string))
                .unwrap_or_default()
        })
        .collect();
    assert_eq!(entity_ids, vec!["1001", "1002"]);
}

#[test]
fn columnar_match_general_fmt_matches_row_path() {
    // 层 2 收口（2026-08-25）：match 列式批的 General yield（fmt detail，q6
    // 真实形态——此前门控排除 General → unreachable，全走行式）——列式 cell
    // vs 行式 Full ctx 逐字段字节一致（含 null 字段 → 空串）。
    let mut plan = simple_rule_plan(
        "match_fmt",
        simple_plan(
            vec![simple_key("seller")],
            vec![step(vec![branch("b", count_ge(1.0))])],
        ),
        Expr::Number(20.0),
        "digit",
        Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
    );
    plan.binds[0].alias = "b".into();
    plan.binds[0].window = "bid_events".into();
    plan.yield_plan.fields = vec![
        YieldField {
            name: "id".into(),
            value: Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
        },
        YieldField {
            name: "alert_type".into(),
            value: Expr::StringLit("q6_avg200".into()),
        },
        YieldField {
            name: "detail".into(),
            value: Expr::FuncCall {
                qualifier: None,
                name: "fmt".into(),
                args: vec![
                    Expr::StringLit("seller={} price={}".into()),
                    Expr::Field(FieldRef::Qualified("b".into(), "bidder".into())),
                    Expr::Field(FieldRef::Qualified("b".into(), "price".into())),
                ],
            },
        },
    ];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([
            ("id".into(), FieldType::Base(BaseType::Digit)),
            ("alert_type".into(), FieldType::Base(BaseType::Chars)),
            ("detail".into(), FieldType::Base(BaseType::Chars)),
        ]),
    );
    assert!(
        exec.match_plan_columnar_safe(),
        "match + fmt detail（普通字段）应过列式门控（层 2）"
    );

    const NOW: i64 = 1_700_000_000_000_000_000;
    let mk = |auction: f64, seller: f64, price: Option<f64>| MatchedContext {
        rule_name: "match_fmt".into(),
        scope_key: vec![num(seller)],
        step_data: vec![StepData {
            satisfied_branch_index: 0,
            label: None,
            measure_value: auction,
            event_first_time_nanos: Some(NOW),
            event_last_time_nanos: Some(NOW),
            collected_values: vec![],
            field_values: EngineHashMap::default(),
        }],
        bind_data: vec![],
        event_time_nanos: NOW,
        event_first_time_nanos: NOW,
        event_last_time_nanos: NOW,
        first_match_time_nanos: None,
        evidence_first_time_nanos: NOW,
        evidence_last_time_nanos: NOW,
        window_start_time_nanos: NOW - 600_000_000_000,
        window_end_time_nanos: NOW,
        machine_id: String::new(),
        trigger_event: Some(TriggerEvent::from_event(Arc::new(event(vec![
            ("auction", num(auction)),
            ("bidder", num(5.0)),
            (
                "price",
                match price {
                    Some(p) => num(p),
                    None => Value::Str("".into()),
                },
            ),
        ])))),
    };
    let m1 = mk(1001.0, 20.0, Some(300.0));
    let m2 = mk(1002.0, 21.0, None); // price 缺失 → fmt 空串（解释 None→""）

    // 行式路径：execute_match_at（General → Full ctx）+ append_record。
    let mut b_row = AlertColumnBuilder::new(Arc::from("alerts"));
    for m in [&m1, &m2] {
        let record = exec.execute_match_at(m, NOW).unwrap();
        b_row.append_record(&record).unwrap();
    }
    let out_row: Vec<_> = b_row
        .finish()
        .iter_data_records()
        .map(|r| r.unwrap())
        .collect();

    // 列式路径：match_batch_prepare 批级 cell。
    let mut b_col = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut appended = Vec::new();
    let stats =
        exec.execute_match_direct_batch_columnar(&[&m1, &m2], NOW, &mut b_col, &mut appended);
    assert_eq!(stats.appended, 2);
    assert_eq!(stats.failed, 0);
    assert_eq!(appended, vec![0, 1]);
    let out_col: Vec<_> = b_col
        .finish()
        .iter_data_records()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(out_row, out_col);
    let detail = |r: &wp_model_core::model::DataRecord| {
        r.fields()
            .find(|f| f.get_name() == "detail")
            .and_then(|f| f.get_chars().map(str::to_string))
            .unwrap_or_default()
    };
    assert_eq!(detail(&out_col[0]), "seller=5 price=300", "row 0 fmt");
    assert_eq!(
        detail(&out_col[1]),
        "seller=5 price=",
        "row 1 缺 price → 空串"
    );
}

#[test]
fn columnar_match_general_materialize_fail_falls_back_matches_row_path() {
    // 层 2 收口 review：物化失败（引用字段跨行类型不一致 → materialize_fields
    // None）→ 整批回退逐行——回退路径（build_eval_context Full ctx + eval）
    // 必须与解释路径（execute_match_at）逐位一致。
    let mut plan = simple_rule_plan(
        "match_fmt_fb",
        simple_plan(
            vec![simple_key("seller")],
            vec![step(vec![branch("b", count_ge(1.0))])],
        ),
        Expr::Number(20.0),
        "digit",
        Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
    );
    plan.binds[0].alias = "b".into();
    plan.binds[0].window = "bid_events".into();
    plan.yield_plan.fields = vec![YieldField {
        name: "detail".into(),
        value: Expr::FuncCall {
            qualifier: None,
            name: "fmt".into(),
            args: vec![
                Expr::StringLit("price={}".into()),
                Expr::Field(FieldRef::Qualified("b".into(), "price".into())),
            ],
        },
    }];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([("detail".into(), FieldType::Base(BaseType::Chars))]),
    );
    assert!(exec.match_plan_columnar_safe(), "fmt 普通字段应过门控");

    const NOW: i64 = 1_700_000_000_000_000_000;
    let mk = |auction: f64, seller: f64, price: Value| MatchedContext {
        rule_name: "match_fmt_fb".into(),
        scope_key: vec![num(seller)],
        step_data: vec![StepData {
            satisfied_branch_index: 0,
            label: None,
            measure_value: auction,
            event_first_time_nanos: Some(NOW),
            event_last_time_nanos: Some(NOW),
            collected_values: vec![],
            field_values: EngineHashMap::default(),
        }],
        bind_data: vec![],
        event_time_nanos: NOW,
        event_first_time_nanos: NOW,
        event_last_time_nanos: NOW,
        first_match_time_nanos: None,
        evidence_first_time_nanos: NOW,
        evidence_last_time_nanos: NOW,
        window_start_time_nanos: NOW - 600_000_000_000,
        window_end_time_nanos: NOW,
        machine_id: String::new(),
        trigger_event: Some(TriggerEvent::from_event(Arc::new(event(vec![
            ("auction", num(auction)),
            ("price", price),
        ])))),
    };
    // price 跨行类型不一致（Number vs Str）→ 物化失败 → 整批回退逐行。
    let m1 = mk(1001.0, 20.0, num(300.0));
    let m2 = mk(1002.0, 21.0, str_val("abc"));

    let mut b_row = AlertColumnBuilder::new(Arc::from("alerts"));
    for m in [&m1, &m2] {
        let record = exec.execute_match_at(m, NOW).unwrap();
        b_row.append_record(&record).unwrap();
    }
    let out_row: Vec<_> = b_row
        .finish()
        .iter_data_records()
        .map(|r| r.unwrap())
        .collect();

    let mut b_col = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut appended = Vec::new();
    let stats =
        exec.execute_match_direct_batch_columnar(&[&m1, &m2], NOW, &mut b_col, &mut appended);
    assert_eq!(stats.appended, 2);
    assert_eq!(stats.failed, 0);
    let out_col: Vec<_> = b_col
        .finish()
        .iter_data_records()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(out_row, out_col);
    let detail = |r: &wp_model_core::model::DataRecord| {
        r.fields()
            .find(|f| f.get_name() == "detail")
            .and_then(|f| f.get_chars().map(str::to_string))
            .unwrap_or_default()
    };
    assert_eq!(detail(&out_col[0]), "price=300", "row 0 Number");
    assert_eq!(
        detail(&out_col[1]),
        "price=abc",
        "row 1 Str（回退路径渲染）"
    );
}

#[test]
fn columnar_match_gate_rejects_right_window_refs() {
    // join 存在时，输出字段引用右窗字段（Qualified 右窗名）→ 门控回退行式。
    let mut plan = simple_rule_plan(
        "match_join_col",
        simple_plan(
            vec![simple_key("seller")],
            vec![step(vec![branch("b", count_ge(1.0))])],
        ),
        Expr::Number(20.0),
        "digit",
        Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
    );
    plan.binds[0].alias = "b".into();
    plan.binds[0].window = "bid_events".into();
    plan.joins = vec![JoinPlan {
        right_window: "auction_events".into(),
        mode: JoinMode::Inner,
        conds: vec![JoinCondPlan {
            left: FieldRef::Qualified("b".into(), "auction".into()),
            right: FieldRef::Qualified("auction_events".into(), "id".into()),
        }],
        within: None,
        reduce: None,
        emit_at: None,
    }];
    // 输出引用右窗字段（seller 在 auction_events）→ 拒绝。
    plan.yield_plan.fields = vec![YieldField {
        name: "seller".into(),
        value: Expr::Field(FieldRef::Qualified(
            "auction_events".into(),
            "seller".into(),
        )),
    }];
    let exec = RuleExecutor::new(plan);
    assert!(
        !exec.match_plan_columnar_safe(),
        "输出引用非键右窗字段必须回退行式（字节一致性）"
    );

    // 输出仅引用左窗字段（b.auction）+ 常量 → 即使有 join 也通过
    // （join 在上游完成，输出不依赖富化）。
    let mut plan2 = simple_rule_plan(
        "match_join_col2",
        simple_plan(
            vec![simple_key("seller")],
            vec![step(vec![branch("b", count_ge(1.0))])],
        ),
        Expr::Number(20.0),
        "digit",
        Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
    );
    plan2.binds[0].alias = "b".into();
    plan2.binds[0].window = "bid_events".into();
    plan2.joins = vec![JoinPlan {
        right_window: "auction_events".into(),
        mode: JoinMode::Inner,
        conds: vec![JoinCondPlan {
            left: FieldRef::Qualified("b".into(), "auction".into()),
            right: FieldRef::Qualified("auction_events".into(), "id".into()),
        }],
        within: None,
        reduce: None,
        emit_at: None,
    }];
    plan2.yield_plan.fields = vec![YieldField {
        name: "id".into(),
        value: Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
    }];
    let exec2 = RuleExecutor::new(plan2);
    assert!(
        exec2.match_plan_columnar_safe(),
        "输出仅左窗字段 + join 已上游完成 → 列式安全"
    );
}
