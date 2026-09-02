use wf_lang::ast::{Expr, FieldRef};

use crate::match_engine::EngineHashMap;
use crate::match_engine::RuleExecutor;
use crate::match_engine::match_engine::{CloseOutput, CloseReason};

use super::super::helpers::*;
use super::helpers::default_match_plan;

// =========================================================================
// Test 6: execute_close – both ok
// =========================================================================

#[test]
fn execute_close_both_ok() {
    use crate::match_engine::match_engine::StepData;
    use wf_lang::ast::CloseMode;

    let plan = simple_rule_plan(
        "r1",
        default_match_plan(),
        Expr::Number(70.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    );
    let exec = RuleExecutor::new(plan);
    let close = CloseOutput {
        rule_name: "r1".to_string(),
        scope_key: vec![str_val("10.0.0.1")],
        close_reason: CloseReason::Timeout,
        event_ok: true,
        close_ok: true,
        close_mode: CloseMode::And,
        event_emitted: false,
        event_step_data: vec![StepData {
            satisfied_branch_index: 0,
            label: Some("fail".to_string()),
            measure_value: 3.0,
            event_first_time_nanos: Some(1_000),
            event_last_time_nanos: Some(3_000),
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

    let alert = exec.execute_close(&close).unwrap().unwrap();
    assert_eq!(alert.origin.as_str(), "close:timeout");
    assert!((alert.score - 70.0).abs() < f64::EPSILON);
    assert_eq!(alert.entity_id, "10.0.0.1");
    assert_eq!(alert.event_time_nanos, 123);
}

// =========================================================================
// Test 7: execute_close – close_ok = false
// =========================================================================

#[test]
fn execute_close_close_not_ok() {
    use wf_lang::ast::CloseMode;

    let plan = simple_rule_plan(
        "r1",
        default_match_plan(),
        Expr::Number(70.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    );
    let exec = RuleExecutor::new(plan);
    let close = CloseOutput {
        rule_name: "r1".to_string(),
        scope_key: vec![str_val("10.0.0.1")],
        close_reason: CloseReason::Flush,
        event_ok: true,
        close_ok: false,
        close_mode: CloseMode::And,
        event_emitted: false,
        event_step_data: vec![],
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
        last_event_nanos: 0,
        row_fields: None,
        row_field_names: None,
    };

    let result = exec.execute_close(&close).unwrap();
    assert!(result.is_none());
}

// =========================================================================
// Test 8: execute_close – event_ok = false
// =========================================================================

#[test]
fn execute_close_event_not_ok() {
    use wf_lang::ast::CloseMode;

    let plan = simple_rule_plan(
        "r1",
        default_match_plan(),
        Expr::Number(70.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    );
    let exec = RuleExecutor::new(plan);
    let close = CloseOutput {
        rule_name: "r1".to_string(),
        scope_key: vec![str_val("10.0.0.1")],
        close_reason: CloseReason::Eos,
        event_ok: false,
        close_ok: true,
        close_mode: CloseMode::And,
        event_emitted: false,
        event_step_data: vec![],
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
        last_event_nanos: 0,
        row_fields: None,
        row_field_names: None,
    };

    let result = exec.execute_close(&close).unwrap();
    assert!(result.is_none());
}

#[test]
fn execute_close_score_can_use_count_alias() {
    use crate::match_engine::match_engine::StepData;
    use wf_lang::ast::CloseMode;

    let plan = simple_rule_plan(
        "r1",
        simple_plan(
            vec![simple_key("sip")],
            vec![step(vec![branch("x", count_ge(1.0))])],
        ),
        Expr::FuncCall {
            qualifier: None,
            name: "count".to_string(),
            args: vec![Expr::Field(FieldRef::Simple("x".to_string()))],
        },
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    );
    let exec = RuleExecutor::new(plan);
    let close = CloseOutput {
        rule_name: "r1".to_string(),
        scope_key: vec![str_val("10.0.0.1")],
        close_reason: CloseReason::Timeout,
        event_ok: true,
        close_ok: true,
        close_mode: CloseMode::And,
        event_emitted: false,
        event_step_data: vec![StepData {
            satisfied_branch_index: 0,
            label: None,
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

    let alert = exec.execute_close(&close).unwrap().unwrap();

    assert_eq!(alert.score, 3.0);
}

// =========================================================================
// Nested field paths (wp-labs/warp-fusion#64) — close yield path
// =========================================================================

#[test]
fn execute_close_yield_nested_path_via_bind_data() {
    use std::collections::HashMap;

    use wf_lang::ast::{CloseMode, PathSegment, SystemVar};
    use wf_lang::plan::YieldField;
    use wf_lang::{BaseType, FieldType};

    use crate::match_engine::Value;
    use crate::match_engine::match_engine::{BindData, StepData};

    let mut plan = simple_rule_plan(
        "r1",
        default_match_plan(),
        Expr::Number(70.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    );
    plan.binds[0].alias = "e".to_string();
    plan.yield_plan.fields = vec![
        YieldField {
            name: "uid".to_string(),
            value: Expr::Field(FieldRef::Path {
                alias: "e".to_string(),
                segments: vec![
                    PathSegment::Field("roles_obj".to_string()),
                    PathSegment::Field("source".to_string()),
                    PathSegment::Field("process".to_string()),
                    PathSegment::Field("uid".to_string()),
                ],
            }),
        },
        YieldField {
            name: "fail_count".to_string(),
            value: Expr::SystemVar(SystemVar::Score),
        },
    ];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([
            ("uid".to_string(), FieldType::Base(BaseType::Chars)),
            ("fail_count".to_string(), FieldType::Base(BaseType::Digit)),
        ]),
    );
    let close = CloseOutput {
        rule_name: "r1".to_string(),
        scope_key: vec![str_val("10.0.0.1")],
        close_reason: CloseReason::Timeout,
        event_ok: true,
        close_ok: true,
        close_mode: CloseMode::And,
        event_emitted: false,
        event_step_data: vec![StepData {
            satisfied_branch_index: 0,
            label: None,
            measure_value: 3.0,
            event_first_time_nanos: None,
            event_last_time_nanos: None,
            collected_values: Vec::new(),
            field_values: EngineHashMap::default(),
        }],
        close_step_data: vec![],
        bind_data: vec![BindData {
            alias: "e".to_string(),
            count: 3,
            field_values: EngineHashMap::from_iter([(
                "roles_obj".to_string(),
                vec![Value::Object(EngineHashMap::from_iter([(
                    "source".into(),
                    Value::Object(EngineHashMap::from_iter([(
                        "process".into(),
                        Value::Object(EngineHashMap::from_iter([(
                            "uid".into(),
                            str_val("d22b3fbcb9e77cb86834f6a18e2e0f68"),
                        )])),
                    )])),
                )]))],
            )]),
        }],
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

    let alert = exec.execute_close(&close).unwrap().unwrap();

    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(n, _)| &**n == "uid")
            .map(|(_, v)| v.clone()),
        Some(str_val("d22b3fbcb9e77cb86834f6a18e2e0f68")),
        "close-rule nested path leaf must be extracted from tracked bind field"
    );
    assert_eq!(alert.score, 70.0);
}

#[test]
fn execute_close_yield_nested_path_missing_bind_omits() {
    use std::collections::HashMap;

    use wf_lang::ast::{CloseMode, PathSegment};
    use wf_lang::plan::YieldField;
    use wf_lang::{BaseType, FieldType};

    use crate::match_engine::match_engine::StepData;

    let mut plan = simple_rule_plan(
        "r1",
        default_match_plan(),
        Expr::Number(70.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    );
    plan.binds[0].alias = "e".to_string();
    plan.yield_plan.fields = vec![YieldField {
        name: "risk_score".to_string(),
        value: Expr::Field(FieldRef::Path {
            alias: "e".to_string(),
            segments: vec![
                PathSegment::Field("roles_obj".to_string()),
                PathSegment::Field("risk".to_string()),
            ],
        }),
    }];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([("risk_score".to_string(), FieldType::Base(BaseType::Float))]),
    );
    let close = CloseOutput {
        rule_name: "r1".to_string(),
        scope_key: vec![str_val("10.0.0.1")],
        close_reason: CloseReason::Timeout,
        event_ok: true,
        close_ok: true,
        close_mode: CloseMode::And,
        event_emitted: false,
        event_step_data: vec![StepData {
            satisfied_branch_index: 0,
            label: None,
            measure_value: 3.0,
            event_first_time_nanos: None,
            event_last_time_nanos: None,
            collected_values: Vec::new(),
            field_values: EngineHashMap::default(),
        }],
        close_step_data: vec![],
        bind_data: vec![], // no roles_obj collected
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

    let alert = exec.execute_close(&close).unwrap().unwrap();

    assert!(
        !alert.yield_fields.iter().any(|(n, _)| &**n == "risk_score"),
        "missing nested path into a float target must be omitted in close yield"
    );
}

// =========================================================================
// L4: batched columnar close emit
// =========================================================================

/// q12-shaped rule: constant score, entity Field(bidder), yields = Lit×3 +
/// Field(bidder) — the shape the columnar close gate passes.
fn q12_like_plan() -> wf_lang::plan::RulePlan {
    use wf_lang::plan::{BindPlan, EntityPlan, ScorePlan, YieldField, YieldPlan};

    wf_lang::plan::RulePlan {
        conv_window: None,
        name: "q12_test".to_string(),
        binds: vec![BindPlan {
            alias: "b".to_string(),
            window: "bid_events".to_string(),
            filter: None,
        }],
        lets: Vec::new(),
        match_plan: simple_plan(
            vec![simple_key("bidder")],
            vec![step(vec![branch("b", count_ge(1.0))])],
        ),
        each_plan: None,
        stats_plan: None,
        joins: vec![],
        r#where: None,
        entity_plan: EntityPlan {
            entity_type: "digit".to_string(),
            entity_id_expr: Expr::Field(FieldRef::Simple("bidder".to_string())),
        },
        yield_plan: YieldPlan {
            target: "nexmark_alerts".to_string(),
            version: None,
            fields: vec![
                YieldField {
                    name: "id".to_string(),
                    value: Expr::Field(FieldRef::Simple("bidder".to_string())),
                },
                YieldField {
                    name: "alert_type".to_string(),
                    value: Expr::StringLit("q12_window".to_string()),
                },
                YieldField {
                    name: "detail".to_string(),
                    value: Expr::StringLit("bids in 10s window".to_string()),
                },
                YieldField {
                    name: "request_count".to_string(),
                    value: Expr::Number(1.0),
                },
            ],
        },
        score_plan: ScorePlan {
            expr: Expr::Number(10.0),
        },
        pattern_origin: None,
        conv_plan: None,
        limits_plan: None,
    }
}

fn q12_like_close() -> CloseOutput {
    use crate::match_engine::match_engine::StepData;
    use wf_lang::ast::CloseMode;

    CloseOutput {
        rule_name: "q12_test".to_string(),
        scope_key: vec![crate::match_engine::match_engine::Value::Number(42.0)],
        close_reason: CloseReason::Timeout,
        event_ok: true,
        close_ok: true,
        close_mode: CloseMode::And,
        event_emitted: false,
        event_step_data: vec![StepData {
            satisfied_branch_index: 0,
            label: Some("b".to_string()),
            measure_value: 7.0,
            event_first_time_nanos: Some(1_000),
            event_last_time_nanos: Some(9_000),
            collected_values: Vec::new(),
            field_values: EngineHashMap::default(),
        }],
        close_step_data: vec![StepData {
            satisfied_branch_index: 0,
            label: Some("n".to_string()),
            measure_value: 7.0,
            event_first_time_nanos: None,
            event_last_time_nanos: None,
            collected_values: Vec::new(),
            field_values: EngineHashMap::default(),
        }],
        bind_data: vec![],
        watermark_nanos: 10_000_000_000,
        event_first_time_nanos: 1_000,
        event_last_time_nanos: 9_000,
        first_match_time_nanos: None,
        evidence_first_time_nanos: 1_000,
        evidence_last_time_nanos: 9_000,
        window_start_time_nanos: 0,
        window_end_time_nanos: 10_000_000_000,
        machine_id: String::new(),
        last_event_nanos: 9_000,
        row_fields: None,
        row_field_names: None,
    }
}

#[test]
fn close_plan_columnar_safe_accepts_q12_shape() {
    let exec = RuleExecutor::new(q12_like_plan());
    assert!(exec.close_plan_columnar_safe());
}

#[test]
fn close_plan_columnar_safe_rejects_non_constant_score() {
    let mut plan = q12_like_plan();
    plan.score_plan.expr = Expr::BinOp {
        op: wf_lang::ast::BinOp::Add,
        left: Box::new(Expr::Number(1.0)),
        right: Box::new(Expr::Number(2.0)),
    };
    assert!(!RuleExecutor::new(plan).close_plan_columnar_safe());
}

#[test]
fn close_plan_columnar_safe_general_yield_plain_fields_only() {
    // 2026-08-25 扩展: General yield（fmt/strftime/count_char 等）只要引用
    // 普通字段/字面量就允许走列式 close（轻量 ctx 求值）; 引用合成字段
    // （`_step_*`/`_bind_*`, Named 窄化不注入）拒绝。
    let mut plan = q12_like_plan();
    plan.yield_plan.fields[0].value = Expr::BinOp {
        op: wf_lang::ast::BinOp::Add,
        left: Box::new(Expr::Number(1.0)),
        right: Box::new(Expr::Number(2.0)),
    };
    assert!(RuleExecutor::new(plan.clone()).close_plan_columnar_safe());
    plan.yield_plan.fields[0].value =
        Expr::Field(wf_lang::ast::FieldRef::Simple("_step_0_measure".into()));
    assert!(!RuleExecutor::new(plan).close_plan_columnar_safe());
}

/// Compare two records field-by-field, skipping `__wfu_emit_time` (the
/// columnar path uses a batch-level emit time; the per-record path stamps
/// each close with `now()`). Everything else — including yield cells and
/// their data types — must be byte-identical.
fn assert_records_equal_ignoring_emit_time(
    per_record: &wp_model_core::model::DataRecord,
    columnar: &wp_model_core::model::DataRecord,
) {
    use wf_lang::wfu_meta::WFU_EMIT_TIME;

    for field in &per_record.items {
        if field.get_name() == WFU_EMIT_TIME {
            continue;
        }
        let name = field.get_name();
        let other = columnar
            .items
            .iter()
            .find(|f| f.get_name() == name)
            .unwrap_or_else(|| panic!("columnar record missing field {name:?}"));
        assert_eq!(
            other.get_value(),
            field.get_value(),
            "field {name:?} value mismatch"
        );
    }
    // Same count of fields (both sides emit the same field set).
    assert_eq!(per_record.items.len(), columnar.items.len());
}

#[test]
fn columnar_close_matches_per_record_close() {
    use crate::alert::AlertColumnBuilder;
    use crate::error::CoreResult;
    use wp_model_core::model::DataRecord;

    let exec = RuleExecutor::new(q12_like_plan());
    let close = q12_like_close();

    // Per-record path: execute_close + OutputRecord::to_data_record.
    let record = exec.execute_close(&close).unwrap().unwrap();
    let per_record = record.to_data_record().unwrap();

    // Columnar path: batched builder + row view.
    let mut builder = AlertColumnBuilder::new(std::sync::Arc::from("nexmark_alerts"));
    let stats = exec.execute_close_direct_batch_columnar(&[close], &mut builder, 1_700_000_000_000);
    assert_eq!(stats.appended, 1);
    assert_eq!(stats.failed, 0);
    let batch = builder.finish();
    let columnar_rows: Vec<DataRecord> = batch
        .iter_data_records()
        .collect::<CoreResult<Vec<_>>>()
        .unwrap();
    assert_eq!(columnar_rows.len(), 1);

    assert_records_equal_ignoring_emit_time(&per_record, &columnar_rows[0]);
}

#[test]
fn columnar_close_general_fmt_yield_matches_per_record() {
    // 2026-08-25 扩展: General yield（fmt, q15-q19 detail 形态）过门控后,
    // 列式 close 的轻量 ctx 求值必须与逐条路径逐位一致（值 + 类型）。
    use crate::alert::AlertColumnBuilder;
    use crate::error::CoreResult;
    use wf_lang::plan::YieldField;
    use wp_model_core::model::DataRecord;

    let mut plan = q12_like_plan();
    plan.yield_plan.fields.push(YieldField {
        name: "fmt_detail".to_string(),
        value: Expr::FuncCall {
            qualifier: None,
            name: "fmt".to_string(),
            args: vec![
                Expr::StringLit("bidder={}".to_string()),
                Expr::Field(FieldRef::Simple("bidder".to_string())),
            ],
        },
    });
    let exec = RuleExecutor::new(plan);
    assert!(exec.close_plan_columnar_safe(), "fmt 引用普通字段应过门控");
    let close = q12_like_close();

    let record = exec.execute_close(&close).unwrap().unwrap();
    let per_record = record.to_data_record().unwrap();

    let mut builder = AlertColumnBuilder::new(std::sync::Arc::from("nexmark_alerts"));
    let stats = exec.execute_close_direct_batch_columnar(&[close], &mut builder, 1_700_000_000_000);
    assert_eq!(stats.appended, 1);
    assert_eq!(stats.failed, 0);
    let batch = builder.finish();
    let columnar_rows: Vec<DataRecord> = batch
        .iter_data_records()
        .collect::<CoreResult<Vec<_>>>()
        .unwrap();
    assert_eq!(columnar_rows.len(), 1);
    assert_records_equal_ignoring_emit_time(&per_record, &columnar_rows[0]);
}

#[test]
fn columnar_close_general_fmt_columnar_cell_matches_per_record() {
    // 层 1（2026-08-25）：close_batch_prepare 的列式批级 cell 求值——fmt 引用
    // 字段带真实值（键来源 + field_values 来源），输出必须与逐条解释路径
    // 逐位一致（值 + 类型）。
    use crate::alert::AlertColumnBuilder;
    use crate::error::CoreResult;
    use crate::match_engine::match_engine::Value;
    use wf_lang::plan::YieldField;
    use wp_model_core::model::DataRecord;

    let mut plan = q12_like_plan();
    plan.yield_plan.fields.push(YieldField {
        name: "fmt_detail".to_string(),
        value: Expr::FuncCall {
            qualifier: None,
            name: "fmt".to_string(),
            args: vec![
                Expr::StringLit("bidder={} price={}".to_string()),
                Expr::Field(FieldRef::Simple("bidder".to_string())),
                Expr::Field(FieldRef::Simple("price".to_string())),
            ],
        },
    });
    let exec = RuleExecutor::new(plan);
    assert!(exec.close_plan_columnar_safe(), "fmt 引用普通字段应过门控");
    let mut close = q12_like_close();
    // bidder 是键（scope_key[0]=42）——键来源；price 走 field_values（非键）
    // 来源；extra 是**未被引用**的字段——不应影响物化/求值。
    close.close_step_data[0]
        .field_values
        .insert("price".into(), vec![Value::Number(1234.5)]);
    close.close_step_data[0]
        .field_values
        .insert("extra".into(), vec![Value::Str("x".to_string().into())]);

    let record = exec.execute_close(&close).unwrap().unwrap();
    let per_record = record.to_data_record().unwrap();

    let mut builder = AlertColumnBuilder::new(std::sync::Arc::from("nexmark_alerts"));
    let stats = exec.execute_close_direct_batch_columnar(&[close], &mut builder, 1_700_000_000_000);
    assert_eq!(stats.appended, 1);
    assert_eq!(stats.failed, 0);
    let batch = builder.finish();
    let columnar_rows: Vec<DataRecord> = batch
        .iter_data_records()
        .collect::<CoreResult<Vec<_>>>()
        .unwrap();
    assert_eq!(columnar_rows.len(), 1);
    assert_records_equal_ignoring_emit_time(&per_record, &columnar_rows[0]);
}

#[test]
fn columnar_close_general_materialize_fail_falls_back_matches_per_record() {
    // 层 2 收口 review：物化失败（引用字段跨 close 类型不一致 → 整批回退）
    // 的回退路径（build_eval_context + eval）必须与逐条解释路径逐位一致。
    use crate::alert::AlertColumnBuilder;
    use crate::error::CoreResult;
    use crate::match_engine::match_engine::Value;
    use wf_lang::plan::YieldField;
    use wp_model_core::model::DataRecord;

    let mut plan = q12_like_plan();
    plan.yield_plan.fields.push(YieldField {
        name: "fmt_detail".into(),
        value: Expr::FuncCall {
            qualifier: None,
            name: "fmt".into(),
            args: vec![
                Expr::StringLit("v={}".into()),
                Expr::Field(FieldRef::Simple("v".into())),
            ],
        },
    });
    let exec = RuleExecutor::new(plan);
    assert!(exec.close_plan_columnar_safe());

    // 两个 close：v 分别为 Number 与 Str → 物化类型不一致 → 整批回退。
    let mut c1 = q12_like_close();
    c1.close_step_data[0]
        .field_values
        .insert("v".into(), vec![Value::Number(7.0)]);
    let mut c2 = q12_like_close();
    c2.close_step_data[0]
        .field_values
        .insert("v".into(), vec![Value::Str("x".to_string().into())]);

    let mut b_row = AlertColumnBuilder::new(std::sync::Arc::from("nexmark_alerts"));
    for c in [&c1, &c2] {
        let record = exec.execute_close(c).unwrap().unwrap();
        b_row.append_record(&record).unwrap();
    }
    let out_row: Vec<DataRecord> = b_row
        .finish()
        .iter_data_records()
        .collect::<CoreResult<Vec<_>>>()
        .unwrap();

    let mut b_col = AlertColumnBuilder::new(std::sync::Arc::from("nexmark_alerts"));
    let stats = exec.execute_close_direct_batch_columnar(&[c1, c2], &mut b_col, 1_700_000_000_000);
    assert_eq!(stats.appended, 2);
    assert_eq!(stats.failed, 0);
    let out_col: Vec<DataRecord> = b_col
        .finish()
        .iter_data_records()
        .collect::<CoreResult<Vec<_>>>()
        .unwrap();

    assert_eq!(out_row.len(), out_col.len());
    for (row, (ra, rb)) in out_row.iter().zip(out_col.iter()).enumerate() {
        assert_eq!(ra.items.len(), rb.items.len(), "row {row} field count");
        for (fa, fb) in ra.items.iter().zip(rb.items.iter()) {
            if fa.get_name() == wf_lang::wfu_meta::WFU_EMIT_TIME {
                continue;
            }
            assert_eq!(fa.get_name(), fb.get_name(), "row {row} field name");
            assert_eq!(
                fa.get_value(),
                fb.get_value(),
                "row {row} field {} value",
                fa.get_name()
            );
        }
    }
}

#[test]
fn columnar_close_resolves_field_from_step_label() {
    // entity = Field(label name) — the label's measure value is the ctx value
    // (build_eval_context inserts labels as Number(measure_value)).
    let mut plan = q12_like_plan();
    plan.entity_plan.entity_id_expr = Expr::Field(FieldRef::Simple("n".to_string()));
    let exec = RuleExecutor::new(plan);
    let close = q12_like_close();

    let mut builder = crate::alert::AlertColumnBuilder::new(std::sync::Arc::from("nexmark_alerts"));
    let stats = exec.execute_close_direct_batch_columnar(&[close], &mut builder, 1_700_000_000_000);
    assert_eq!(stats.appended, 1);
    let batch = builder.finish();
    let rows: Vec<_> = batch
        .iter_data_records()
        .collect::<crate::error::CoreResult<Vec<_>>>()
        .unwrap();
    let entity = rows[0]
        .items
        .iter()
        .find(|f| f.get_name() == wf_lang::wfu_meta::WFU_ENTITY_ID)
        .unwrap();
    // close step label "n" = 7.0 → "7" (f64 Display of an integral value).
    assert_eq!(entity.get_value(), &wp_model_core::model::Value::from("7"));
}

// -------------------------------------------------------------------------
// L4 edge cases (review 2026-08-21): entity fallback, rejection, missing
// yields, synthetic-field gate, keys-without-scope fallback.
// -------------------------------------------------------------------------

#[test]
fn columnar_close_entity_missing_falls_back_to_empty() {
    // entity = Field(absent): eval_yield_expr falls back to an empty string
    // on the per-record path — the columnar path must match (not fail the
    // close, which was the first implementation's bug).
    let mut plan = q12_like_plan();
    plan.entity_plan.entity_id_expr = Expr::Field(FieldRef::Simple("absent".to_string()));
    let exec = RuleExecutor::new(plan);
    let close = q12_like_close();

    let record = exec.execute_close(&close).unwrap().unwrap();
    assert_eq!(record.entity_id, "");

    let mut builder = crate::alert::AlertColumnBuilder::new(std::sync::Arc::from("nexmark_alerts"));
    let stats = exec.execute_close_direct_batch_columnar(&[close], &mut builder, 1_700_000_000_000);
    assert_eq!(stats.appended, 1, "absent entity must still append");
    assert_eq!(stats.failed, 0);
    let batch = builder.finish();
    let rows: Vec<_> = batch
        .iter_data_records()
        .collect::<crate::error::CoreResult<Vec<_>>>()
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_records_equal_ignoring_emit_time(&record.to_data_record().unwrap(), &rows[0]);
}

#[test]
fn columnar_close_rejects_unqualified_close() {
    let exec = RuleExecutor::new(q12_like_plan());
    let mut close = q12_like_close();
    close.close_ok = false;

    let mut builder = crate::alert::AlertColumnBuilder::new(std::sync::Arc::from("nexmark_alerts"));
    let stats = exec.execute_close_direct_batch_columnar(&[close], &mut builder, 1_700_000_000_000);
    assert_eq!(stats.rejected, 1);
    assert_eq!(stats.appended, 0);
    let batch = builder.finish();
    assert_eq!(batch.len(), 0);
}

#[test]
fn columnar_close_batch_mixes_qualified_and_unqualified() {
    let exec = RuleExecutor::new(q12_like_plan());
    let mut bad = q12_like_close();
    bad.close_ok = false;
    let good = q12_like_close();

    let mut builder = crate::alert::AlertColumnBuilder::new(std::sync::Arc::from("nexmark_alerts"));
    let stats = exec.execute_close_direct_batch_columnar(
        &[good.clone(), bad, good.clone()],
        &mut builder,
        1_700_000_000_000,
    );
    assert_eq!(stats.appended, 2);
    assert_eq!(stats.rejected, 1);
    let batch = builder.finish();
    assert_eq!(batch.len(), 2);
    // Both appended rows identical to the per-record path.
    let rows: Vec<_> = batch
        .iter_data_records()
        .collect::<crate::error::CoreResult<Vec<_>>>()
        .unwrap();
    let per_record = exec
        .execute_close(&good)
        .unwrap()
        .unwrap()
        .to_data_record()
        .unwrap();
    for row in &rows {
        assert_records_equal_ignoring_emit_time(&per_record, row);
    }
}

#[test]
fn columnar_close_missing_yield_field_falls_back_to_empty() {
    // yield id = Field(absent): eval_yield_expr_with_meta falls back to an
    // empty string — the columnar path must stage the same empty value.
    let mut plan = q12_like_plan();
    plan.yield_plan.fields[0].value = Expr::Field(FieldRef::Simple("absent".to_string()));
    let exec = RuleExecutor::new(plan);
    let close = q12_like_close();

    let record = exec.execute_close(&close).unwrap().unwrap();
    let (name, value) = record
        .yield_fields
        .iter()
        .find(|(n, _)| &**n == "id")
        .expect("id yield present");
    assert_eq!(
        value,
        &crate::match_engine::match_engine::Value::Str("".into())
    );
    let _ = name;

    let mut builder = crate::alert::AlertColumnBuilder::new(std::sync::Arc::from("nexmark_alerts"));
    let stats = exec.execute_close_direct_batch_columnar(&[close], &mut builder, 1_700_000_000_000);
    assert_eq!(stats.appended, 1);
    assert_eq!(stats.failed, 0);
    let batch = builder.finish();
    let rows: Vec<_> = batch
        .iter_data_records()
        .collect::<crate::error::CoreResult<Vec<_>>>()
        .unwrap();
    assert_records_equal_ignoring_emit_time(&record.to_data_record().unwrap(), &rows[0]);
}

#[test]
fn close_plan_columnar_safe_rejects_synthetic_field_refs() {
    // `_step_*` / `_bind_*` ctx fields can't be resolved columnarly — the
    // gate must reject them so those rules keep the per-record path.
    let mut plan = q12_like_plan();
    plan.yield_plan.fields[0].value = Expr::Field(FieldRef::Simple("_step_0_values".to_string()));
    assert!(!RuleExecutor::new(plan).close_plan_columnar_safe());

    let mut plan = q12_like_plan();
    plan.entity_plan.entity_id_expr = Expr::Field(FieldRef::Simple("_bind_b_count".to_string()));
    assert!(!RuleExecutor::new(plan).close_plan_columnar_safe());
}

#[test]
fn columnar_close_scope_key_short_falls_back_to_field_values() {
    // Two match keys but a scope_key of length 1: the second key has no value
    // in the ctx (keys.zip(scope_key) truncates), so a yield on it resolves
    // from field_values — both paths must agree.
    let mut plan = q12_like_plan();
    plan.match_plan = simple_plan(
        vec![simple_key("bidder"), simple_key("category")],
        vec![step(vec![branch("b", count_ge(1.0))])],
    );
    plan.yield_plan.fields[0].value = Expr::Field(FieldRef::Simple("category".to_string()));
    let exec = RuleExecutor::new(plan);

    let mut close = q12_like_close();
    // scope_key shorter than keys (truncated zip on the per-record path).
    close.scope_key = vec![crate::match_engine::match_engine::Value::Number(42.0)];
    close.event_step_data[0].field_values = {
        let mut m = EngineHashMap::default();
        m.insert(
            "category".to_string(),
            vec![crate::match_engine::match_engine::Value::Str("cars".into())],
        );
        m
    };

    let record = exec.execute_close(&close).unwrap().unwrap();
    let id_val = record
        .yield_fields
        .iter()
        .find(|(n, _)| &**n == "id")
        .map(|(_, v)| v.clone())
        .unwrap();
    assert_eq!(
        id_val,
        crate::match_engine::match_engine::Value::Str("cars".into())
    );

    let mut builder = crate::alert::AlertColumnBuilder::new(std::sync::Arc::from("nexmark_alerts"));
    let stats = exec.execute_close_direct_batch_columnar(&[close], &mut builder, 1_700_000_000_000);
    assert_eq!(stats.appended, 1);
    assert_eq!(stats.failed, 0);
    let batch = builder.finish();
    let rows: Vec<_> = batch
        .iter_data_records()
        .collect::<crate::error::CoreResult<Vec<_>>>()
        .unwrap();
    assert_records_equal_ignoring_emit_time(&record.to_data_record().unwrap(), &rows[0]);
}
