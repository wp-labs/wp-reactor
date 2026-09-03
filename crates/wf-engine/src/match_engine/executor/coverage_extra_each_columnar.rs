//! coverage_extra 拆出的兄弟子模块（2026-09-04）：each 列式路径 vs 行式的逐位对拍——
//! entity lanes / binop score / output funcs / q22 split-mvindex / direct-batch general
//! yield / close-ctx 字段窄化 / fmt 结构化参数回退 / q14 filter（逐位一致断言）。
//! 共享 harness 在父模块 `coverage_extra.rs`，此处经 `use super::*` 复用。

use super::*;

use crate::alert::AlertColumnBuilder;
use crate::match_engine::event_bridge::ColumnarEvent;
use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field as ArrowField, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use wf_lang::ast::BinOp;
use wf_lang::plan::LetPlan;

#[test]
fn columnar_each_entity_lanes_and_failure_paths() {
    // Schema: sip=Utf8, id=Int64, ts=Timestamp(Ns), price=Float64, note=structured Utf8.
    let note_field =
        ArrowField::new("note", DataType::Utf8, true).with_metadata(HashMap::from([(
            crate::match_engine::event_bridge::WFL_FIELD_TYPE_METADATA_KEY.to_string(),
            crate::match_engine::event_bridge::WFL_FIELD_TYPE_OBJECT.to_string(),
        )]));
    let schema = Arc::new(Schema::new(vec![
        ArrowField::new("sip", DataType::Utf8, true),
        ArrowField::new("id", DataType::Int64, true),
        ArrowField::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        ArrowField::new("price", DataType::Float64, true),
        note_field,
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![
                Some("10.0.0.1"),
                Some("10.0.0.2"),
                None,
            ])) as ArrayRef,
            Arc::new(Int64Array::from(vec![Some(1000), Some(1001), Some(1002)])) as ArrayRef,
            Arc::new(TimestampNanosecondArray::from(vec![
                Some(1_700_000_000_000_000_000),
                Some(1_700_000_000_001_000_000),
                Some(1_700_000_000_002_000_000),
            ])) as ArrayRef,
            Arc::new(Float64Array::from(vec![Some(1.5), Some(2.5), None])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some(r#"{"a":1}"#),
                Some(r#"{"b":2}"#),
                None,
            ])) as ArrayRef,
        ],
    )
    .unwrap();

    // Entity = e.id (Int64) → I64 lane; yield id (same column, Float type) →
    // numeric fast lane.
    let mut plan = simple_rule_plan(
        "i64_entity",
        simple_plan(vec![], vec![]),
        Expr::Number(1.0),
        "digit",
        Expr::Field(FieldRef::Qualified("e".into(), "id".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    plan.yield_plan.fields = vec![YieldField {
        name: "id_copy".into(),
        value: Expr::Field(FieldRef::Qualified("e".into(), "id".into())),
    }];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([("id_copy".into(), FieldType::Base(BaseType::Float))]),
    );
    assert!(exec.each_plan_columnar_safe());
    let col_events: Vec<ColumnarEvent<'_>> =
        (0..3).map(|r| ColumnarEvent::new(&batch, r)).collect();
    let rows: Vec<(&ColumnarEvent<'_>, i64)> = col_events
        .iter()
        .map(|ev| (ev, 1_700_000_000_000_000_000))
        .collect();
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut appended = Vec::new();
    let stats = exec.execute_each_direct_batch_columnar(&rows, 0, &mut builder, &mut appended);
    assert_eq!(stats.appended, 3);
    assert_eq!(stats.failed, 0);
    assert_eq!(appended, vec![0, 1, 2]);

    // Entity = e.ts (Timestamp Ns) → TsNanos lane.
    let mut plan = simple_rule_plan(
        "ts_entity",
        simple_plan(vec![], vec![]),
        Expr::Number(1.0),
        "time",
        Expr::Field(FieldRef::Qualified("e".into(), "ts".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    let exec = RuleExecutor::new(plan);
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut appended = Vec::new();
    let stats = exec.execute_each_direct_batch_columnar(&rows, 0, &mut builder, &mut appended);
    assert_eq!(stats.appended, 3);
    assert_eq!(stats.failed, 0);

    // Entity = e.sip (Utf8, with a null row) → Utf8 lane + empty-entity fallback.
    let exec = each_plan_rule();
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut appended = Vec::new();
    let stats = exec.execute_each_direct_batch_columnar(&rows, 0, &mut builder, &mut appended);
    assert_eq!(stats.appended, 3);
    assert_eq!(stats.failed, 0);

    // Entity = e.price (Float64) → Generic lane (value_at + value_to_string).
    let mut plan = simple_rule_plan(
        "f64_entity",
        simple_plan(vec![], vec![]),
        Expr::Number(1.0),
        "ip",
        Expr::Field(FieldRef::Qualified("e".into(), "price".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    let exec = RuleExecutor::new(plan);
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut appended = Vec::new();
    let stats = exec.execute_each_direct_batch_columnar(&rows, 0, &mut builder, &mut appended);
    assert_eq!(stats.appended, 3);
    assert_eq!(stats.failed, 0);

    // Entity = e.note (structured Utf8) → Generic lane (no fast lane).
    let mut plan = simple_rule_plan(
        "structured_entity",
        simple_plan(vec![], vec![]),
        Expr::Number(1.0),
        "ip",
        Expr::Field(FieldRef::Qualified("e".into(), "note".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    let exec = RuleExecutor::new(plan);
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut appended = Vec::new();
    let stats = exec.execute_each_direct_batch_columnar(&rows, 0, &mut builder, &mut appended);
    assert_eq!(stats.appended, 3);
    assert_eq!(stats.failed, 0);

    // Entity field missing from the batch schema → Generic None → empty pair.
    let mut plan = simple_rule_plan(
        "missing_entity",
        simple_plan(vec![], vec![]),
        Expr::Number(1.0),
        "ip",
        Expr::Field(FieldRef::Qualified("e".into(), "absent".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    let exec = RuleExecutor::new(plan);
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut appended = Vec::new();
    let stats = exec.execute_each_direct_batch_columnar(&rows, 0, &mut builder, &mut appended);
    assert_eq!(stats.appended, 3);
    assert_eq!(stats.failed, 0);

    // Const (literal) yield that fails to coerce → whole batch failed.
    let mut plan = simple_rule_plan(
        "nan_yield",
        simple_plan(vec![], vec![]),
        Expr::Number(1.0),
        "ip",
        Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    plan.yield_plan.fields = vec![YieldField {
        name: "lat".into(),
        value: Expr::Number(f64::NAN),
    }];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([("lat".into(), FieldType::Base(BaseType::Float))]),
    );
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut appended = Vec::new();
    let stats = exec.execute_each_direct_batch_columnar(&rows, 0, &mut builder, &mut appended);
    assert_eq!(stats.failed, 3);
    assert_eq!(stats.appended, 0);

    // Yield name with the reserved `__wfu_` prefix → register error → failed.
    let mut plan = simple_rule_plan(
        "reserved_yield",
        simple_plan(vec![], vec![]),
        Expr::Number(1.0),
        "ip",
        Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    plan.yield_plan.fields = vec![YieldField {
        name: "__wfu_evil".into(),
        value: Expr::Number(1.0),
    }];
    let exec = RuleExecutor::new(plan);
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut appended = Vec::new();
    let stats = exec.execute_each_direct_batch_columnar(&rows, 0, &mut builder, &mut appended);
    assert_eq!(stats.failed, 3);

    // Non-`on each` rule → all rows failed.
    let plan = simple_rule_plan(
        "not_each",
        default_match_plan(),
        Expr::Number(1.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    let exec = RuleExecutor::new(plan);
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut appended = Vec::new();
    let stats = exec.execute_each_direct_batch_columnar(&rows, 0, &mut builder, &mut appended);
    assert_eq!(stats.failed, 3);
    assert_eq!(stats.appended, 0);
}

#[test]
fn columnar_each_binop_score_matches_row_path() {
    // q1 形态：score(0.908 * e.price)、entity=e.id、yield 常量 + id 字段。
    // 对拍：行式（Event 物化 + eval_score 解释求值）vs 列式（ColumnarEvent
    // 零物化 + 列读 f64 × 常量）输出字节一致，且 score = clamp(0.908 × price)。
    use wp_model_core::model::Value as ModelValue;

    let schema = Arc::new(Schema::new(vec![
        ArrowField::new("id", DataType::Int64, true),
        ArrowField::new("price", DataType::Float64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3)])) as ArrayRef,
            Arc::new(Float64Array::from(vec![Some(1.5), Some(2.5), Some(100.0)])) as ArrayRef,
        ],
    )
    .unwrap();

    let mut plan = simple_rule_plan(
        "q1_binop_score",
        simple_plan(vec![], vec![]),
        Expr::BinOp {
            op: BinOp::Mul,
            left: Box::new(Expr::Number(0.908)),
            right: Box::new(Expr::Field(FieldRef::Qualified("e".into(), "price".into()))),
        },
        "digit",
        Expr::Field(FieldRef::Qualified("e".into(), "id".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    plan.yield_plan.fields = vec![
        YieldField {
            name: "alert_type".into(),
            value: Expr::StringLit("q1_passthrough".into()),
        },
        YieldField {
            name: "id".into(),
            value: Expr::Field(FieldRef::Qualified("e".into(), "id".into())),
        },
    ];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([("id".into(), FieldType::Base(BaseType::Float))]),
    );
    assert!(exec.each_plan_columnar_safe());

    let t = 1_700_000_000_000_000_000i64;

    // 行式路径（Event 物化 + eval_score 解释求值）。
    let events = crate::match_engine::event_bridge::batch_to_events(&batch);
    let row_refs: Vec<(&Event, i64)> = events.iter().map(|e| (e, t)).collect();
    let mut b_row = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut app_row = Vec::new();
    let sr =
        exec.execute_each_direct_batch(&row_refs, &EmptyLookup, &[], 0, &mut b_row, &mut app_row);
    assert_eq!(sr.appended, 3);
    let out_row: Vec<_> = b_row
        .finish()
        .iter_data_records()
        .map(|r| r.unwrap())
        .collect();

    // 列式路径（ColumnarEvent 零物化 + 列读 f64）。
    let col_events: Vec<ColumnarEvent> = (0..3).map(|r| ColumnarEvent::new(&batch, r)).collect();
    let col_refs: Vec<(&ColumnarEvent, i64)> = col_events.iter().map(|ev| (ev, t)).collect();
    let mut b_col = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut app_col = Vec::new();
    let sc = exec.execute_each_direct_batch_columnar(&col_refs, 0, &mut b_col, &mut app_col);
    assert_eq!(sc.appended, 3);
    assert_eq!(sc.failed, 0);
    let out_col: Vec<_> = b_col
        .finish()
        .iter_data_records()
        .map(|r| r.unwrap())
        .collect();

    // 对拍：两路径逐字段一致；score = clamp(0.908 × price)。
    assert_eq!(out_row, out_col);
    let scores: Vec<f64> = out_col
        .iter()
        .map(|r| {
            r.fields()
                .find(|f| f.get_name() == wf_lang::wfu_meta::WFU_SCORE)
                .and_then(|f| match f.get_value() {
                    ModelValue::Float(v) => Some(*v),
                    _ => None,
                })
                .expect("score field present")
        })
        .collect();
    assert_eq!(scores, vec![0.908 * 1.5, 0.908 * 2.5, 0.908 * 100.0]);
}

#[test]
fn columnar_each_binop_score_null_field_fails_row() {
    // 常量×字段的 score 字段为 null → 整行 failed（与解释路径 eval_score 的
    // None → Err 一致），其余行正常 appended。
    let schema = Arc::new(Schema::new(vec![
        ArrowField::new("id", DataType::Int64, true),
        ArrowField::new("price", DataType::Float64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3)])) as ArrayRef,
            Arc::new(Float64Array::from(vec![Some(1.5), None, Some(3.0)])) as ArrayRef,
        ],
    )
    .unwrap();

    let mut plan = simple_rule_plan(
        "null_score",
        simple_plan(vec![], vec![]),
        Expr::BinOp {
            op: BinOp::Mul,
            left: Box::new(Expr::Number(0.5)),
            right: Box::new(Expr::Field(FieldRef::Qualified("e".into(), "price".into()))),
        },
        "digit",
        Expr::Field(FieldRef::Qualified("e".into(), "id".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    let exec = RuleExecutor::new(plan);
    assert!(exec.each_plan_columnar_safe());

    let col_events: Vec<ColumnarEvent> = (0..3).map(|r| ColumnarEvent::new(&batch, r)).collect();
    let col_refs: Vec<(&ColumnarEvent, i64)> = col_events
        .iter()
        .map(|ev| (ev, 1_700_000_000_000_000_000))
        .collect();
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut appended = Vec::new();
    let stats = exec.execute_each_direct_batch_columnar(&col_refs, 0, &mut builder, &mut appended);
    assert_eq!(stats.appended, 2);
    assert_eq!(stats.failed, 1);
    assert_eq!(appended, vec![0, 2]);
}

/// 列式输出函数（fmt/strftime/count_char）yield：行式 vs 列式 each 输出逐字段
/// 对拍（含 null 参数 → 空串的 yield 包装语义）。
#[test]
fn each_columnar_output_funcs_match_row_path() {
    use wp_model_core::model::Value as ModelValue;

    let schema = Arc::new(Schema::new(vec![
        ArrowField::new("sip", DataType::Utf8, true),
        ArrowField::new("count", DataType::Int64, true),
        ArrowField::new("ts", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![
                Some("10.1.1.1"),
                None,
                Some("192.168.0.2"),
            ])) as ArrayRef,
            Arc::new(Int64Array::from(vec![Some(3), Some(7), None])) as ArrayRef,
            Arc::new(Int64Array::from(vec![
                Some(1_700_000_000_000_000_000),
                Some(1_700_000_000_000_000_000),
                None,
            ])) as ArrayRef,
        ],
    )
    .unwrap();

    let mut plan = simple_rule_plan(
        "out_funcs",
        simple_plan(vec![], vec![]),
        Expr::Number(50.0),
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
            name: "label".into(),
            value: Expr::FuncCall {
                qualifier: None,
                name: "fmt".into(),
                args: vec![
                    Expr::StringLit("ip={}|n={}".into()),
                    Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
                    Expr::Field(FieldRef::Qualified("e".into(), "count".into())),
                ],
            },
        },
        YieldField {
            name: "day".into(),
            value: Expr::FuncCall {
                qualifier: None,
                name: "strftime".into(),
                args: vec![
                    Expr::Field(FieldRef::Qualified("e".into(), "ts".into())),
                    Expr::StringLit("%Y-%m-%d".into()),
                ],
            },
        },
        YieldField {
            name: "dots".into(),
            value: Expr::FuncCall {
                qualifier: None,
                name: "count_char".into(),
                args: vec![
                    Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
                    Expr::StringLit(".".into()),
                ],
            },
        },
    ];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([
            ("label".into(), FieldType::Base(BaseType::Chars)),
            ("day".into(), FieldType::Base(BaseType::Chars)),
            ("dots".into(), FieldType::Base(BaseType::Digit)),
        ]),
    );
    assert!(
        exec.each_plan_columnar_safe(),
        "fmt/strftime/count_char yield 应列式"
    );

    let t = 1_700_000_000_000_000_000i64;
    let events = crate::match_engine::event_bridge::batch_to_events(&batch);
    let row_refs: Vec<(&Event, i64)> = events.iter().map(|e| (e, t)).collect();
    let mut b_row = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut app_row = Vec::new();
    let sr =
        exec.execute_each_direct_batch(&row_refs, &EmptyLookup, &[], 0, &mut b_row, &mut app_row);
    assert_eq!(sr.appended, 3);
    let out_row: Vec<_> = b_row
        .finish()
        .iter_data_records()
        .map(|r| r.unwrap())
        .collect();

    let col_events: Vec<ColumnarEvent> = (0..3).map(|r| ColumnarEvent::new(&batch, r)).collect();
    let col_refs: Vec<(&ColumnarEvent, i64)> = col_events.iter().map(|ev| (ev, t)).collect();
    let mut b_col = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut app_col = Vec::new();
    let sc = exec.execute_each_direct_batch_columnar(&col_refs, 0, &mut b_col, &mut app_col);
    assert_eq!(sc.appended, 3);
    assert_eq!(sc.failed, 0);
    let out_col: Vec<_> = b_col
        .finish()
        .iter_data_records()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(out_row, out_col);
    // 关键语义抽查：null sip（row 1）→ fmt 空串；null count（row 2）→ fmt
    // 空串；count_char 正常行返回数字。
    // 关键语义抽查：null sip（row 1）→ fmt 空串；null count（row 2）→ fmt
    // 空串；count_char 正常行返回数字。
    let label = |r: &wp_model_core::model::DataRecord| {
        r.fields()
            .find(|f| f.get_name() == "label")
            .and_then(|f| match f.get_value() {
                ModelValue::Chars(v) => Some(v.to_string()),
                _ => None,
            })
            .expect("label field")
    };
    assert_eq!(label(&out_col[0]), "ip=10.1.1.1|n=3", "row 0 fmt");
    assert_eq!(label(&out_col[1]), "", "row 1 fmt null sip → 空串");
    assert_eq!(label(&out_col[2]), "", "row 2 fmt null count → 空串");
}

/// 层 2（2026-08-25，q22 形态）：`let parts = split(e.url, "/")` + yield
/// `concat(mvindex(parts,3), "/", ...)`——列式 each（编译期内联 let + 融合
/// SplitIndex）与行式 each（apply_lets 逐行注入）输出逐字段对拍（含 null /
/// 越界 → 空串）。
#[test]
fn each_columnar_q22_split_mvindex_concat_matches_row_path() {
    let schema = Arc::new(Schema::new(vec![ArrowField::new(
        "url",
        DataType::Utf8,
        true,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(vec![
            Some("https://www.nexmark.com/aaaaa/bbbbb/ccccc/item.htm?query=1"),
            None,           // null 行
            Some("short"),  // mvindex 越界 → 空串
            Some("a/b//d"), // 空段
        ])) as ArrayRef],
    )
    .unwrap();

    let mut plan = simple_rule_plan(
        "q22_shape",
        simple_plan(vec![], vec![]),
        Expr::Number(50.0),
        "chars",
        Expr::Field(FieldRef::Qualified("e".into(), "url".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    plan.lets = vec![LetPlan {
        name: "parts".into(),
        expr: Expr::FuncCall {
            qualifier: None,
            name: "split".into(),
            args: vec![
                Expr::Field(FieldRef::Qualified("e".into(), "url".into())),
                Expr::StringLit("/".into()),
            ],
        },
    }];
    let mvindex = |idx: f64| Expr::FuncCall {
        qualifier: None,
        name: "mvindex".into(),
        args: vec![
            Expr::Field(FieldRef::Simple("parts".into())),
            Expr::Number(idx),
        ],
    };
    plan.yield_plan.fields = vec![YieldField {
        name: "detail".into(),
        value: Expr::FuncCall {
            qualifier: None,
            name: "concat".into(),
            args: vec![
                mvindex(3.0),
                Expr::StringLit("/".into()),
                mvindex(4.0),
                Expr::StringLit("/".into()),
                mvindex(5.0),
            ],
        },
    }];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([("detail".into(), FieldType::Base(BaseType::Chars))]),
    );
    assert!(
        exec.each_plan_columnar_safe(),
        "q22 let+split+mvindex+concat 应列式"
    );

    let t = 1_700_000_000_000_000_000i64;
    let events = crate::match_engine::event_bridge::batch_to_events(&batch);
    let row_refs: Vec<(&Event, i64)> = events.iter().map(|e| (e, t)).collect();
    let mut b_row = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut app_row = Vec::new();
    let sr =
        exec.execute_each_direct_batch(&row_refs, &EmptyLookup, &[], 0, &mut b_row, &mut app_row);
    assert_eq!(sr.appended, 4);
    let out_row: Vec<_> = b_row
        .finish()
        .iter_data_records()
        .map(|r| r.unwrap())
        .collect();

    let col_events: Vec<ColumnarEvent> = (0..4).map(|r| ColumnarEvent::new(&batch, r)).collect();
    let col_refs: Vec<(&ColumnarEvent, i64)> = col_events.iter().map(|ev| (ev, t)).collect();
    let mut b_col = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut app_col = Vec::new();
    let sc = exec.execute_each_direct_batch_columnar(&col_refs, 0, &mut b_col, &mut app_col);
    assert_eq!(sc.appended, 4);
    assert_eq!(sc.failed, 0);
    let out_col: Vec<_> = b_col
        .finish()
        .iter_data_records()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(out_row, out_col);
    // 语义抽查：row 0 三段拼接 aaaaa/bbbbb/ccccc；null row 1 与越界 row 2 → 空串。
    let detail = |r: &wp_model_core::model::DataRecord| {
        r.fields()
            .find(|f| f.get_name() == "detail")
            .and_then(|f| match f.get_value() {
                wp_model_core::model::Value::Chars(v) => Some(v.to_string()),
                _ => None,
            })
            .expect("detail field")
    };
    assert_eq!(detail(&out_col[0]), "aaaaa/bbbbb/ccccc", "row 0 concat");
    assert_eq!(detail(&out_col[1]), "", "row 1 null url → 空串");
    assert_eq!(detail(&out_col[2]), "", "row 2 mvindex 越界 → 空串");
    assert_eq!(detail(&out_col[3]), "", "row 3 段数不足 → 空串");
}

/// 层 2 收口（2026-08-25）：**行式批路径**（`execute_each_direct_batch`，Event
/// 数组——文件源 replay 等非 RecordBatch 源）的 General yield 走列式批级 cell
/// （Event 数组物化 + let 内联），与逐事件 `execute_each` 逐字段字节一致。
#[test]
fn each_direct_batch_general_yield_matches_per_event() {
    let mut plan = simple_rule_plan(
        "each_fmt",
        simple_plan(vec![], vec![]),
        Expr::Number(50.0),
        "digit",
        Expr::Field(FieldRef::Qualified("e".into(), "auction".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    plan.lets = vec![LetPlan {
        name: "parts".into(),
        expr: Expr::FuncCall {
            qualifier: None,
            name: "split".into(),
            args: vec![
                Expr::Field(FieldRef::Qualified("e".into(), "url".into())),
                Expr::StringLit("/".into()),
            ],
        },
    }];
    let mvindex = |idx: f64| Expr::FuncCall {
        qualifier: None,
        name: "mvindex".into(),
        args: vec![
            Expr::Field(FieldRef::Simple("parts".into())),
            Expr::Number(idx),
        ],
    };
    plan.yield_plan.fields = vec![
        YieldField {
            name: "id".into(),
            value: Expr::Field(FieldRef::Qualified("e".into(), "auction".into())),
        },
        YieldField {
            name: "detail".into(),
            value: Expr::FuncCall {
                qualifier: None,
                name: "concat".into(),
                args: vec![mvindex(3.0), Expr::StringLit("/".into()), mvindex(4.0)],
            },
        },
    ];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([
            ("id".into(), FieldType::Base(BaseType::Digit)),
            ("detail".into(), FieldType::Base(BaseType::Chars)),
        ]),
    );

    let t = 1_700_000_000_000_000_000i64;
    let events = vec![
        event(vec![
            ("auction", num(1001.0)),
            (
                "url",
                str_val("https://www.nexmark.com/aaaaa/bbbbb/ccccc/item.htm"),
            ),
        ]),
        event(vec![("auction", num(1002.0)), ("url", str_val("short"))]),
        event(vec![("auction", num(1003.0))]), // url 缺失 → mvindex null → 空串
    ];

    // 逐事件（解释路径，apply_lets 注入）。
    let mut b_row = AlertColumnBuilder::new(Arc::from("alerts"));
    for ev in &events {
        let record = exec.execute_each(ev, t).unwrap().unwrap();
        b_row.append_record(&record).unwrap();
    }
    let out_row: Vec<_> = b_row
        .finish()
        .iter_data_records()
        .map(|r| r.unwrap())
        .collect();

    // 行式批路径（Event 数组 → 列式 cell）。
    let rows: Vec<(&Event, i64)> = events.iter().map(|e| (e, t)).collect();
    let mut b_batch = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut app = Vec::new();
    let stats = exec.execute_each_direct_batch(&rows, &EmptyLookup, &[], 0, &mut b_batch, &mut app);
    assert_eq!(stats.appended, 3);
    assert_eq!(stats.failed, 0);
    assert_eq!(app, vec![0, 1, 2]);
    let out_batch: Vec<_> = b_batch
        .finish()
        .iter_data_records()
        .map(|r| r.unwrap())
        .collect();

    // 逐字段字节一致（`__wfu_emit_time` 除外——批路径用传入的 emit_time，
    // 逐事件用 now()，与列式批路径的文档化差异一致；emit_time 不喂语义）。
    assert_eq!(out_row.len(), out_batch.len());
    for (row, (ra, rb)) in out_row.iter().zip(out_batch.iter()).enumerate() {
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
    let detail = |r: &wp_model_core::model::DataRecord| {
        r.fields()
            .find(|f| f.get_name() == "detail")
            .and_then(|f| match f.get_value() {
                wp_model_core::model::Value::Chars(v) => Some(v.to_string()),
                _ => None,
            })
            .expect("detail field")
    };
    assert_eq!(detail(&out_batch[0]), "aaaaa/bbbbb", "row 0 concat");
    assert_eq!(detail(&out_batch[1]), "", "row 1 越界 → 空串");
    assert_eq!(detail(&out_batch[2]), "", "row 2 url 缺失 → 空串");
}

#[test]
fn close_ctx_fields_narrowed_for_output_funcs() {
    // 层 2 收口 review：列式输出函数（fmt 等）是纯参数函数——
    // `plan_close_ctx_fields` 应窄化为 Named（含引用的普通字段），而非
    // force_all（行式/回退路径的全量 ctx 构建）。合成字段引用仍 force_all。
    use crate::match_engine::executor::{CloseCtxFields, plan_close_ctx_fields};

    let base = || {
        let mut plan = simple_rule_plan(
            "narrow",
            simple_plan(vec![], vec![]),
            Expr::Number(10.0),
            "digit",
            Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
        );
        plan.binds[0].alias = "b".into();
        plan
    };
    let fmt = |args: Vec<Expr>| Expr::FuncCall {
        qualifier: None,
        name: "fmt".into(),
        args,
    };
    let f = |n: &str| Expr::Field(FieldRef::Qualified("b".into(), n.into()));

    // fmt detail（纯参数函数）→ Named，且含引用的字段。
    let mut plan = base();
    plan.yield_plan.fields = vec![YieldField {
        name: "detail".into(),
        value: fmt(vec![
            Expr::StringLit("{} {}".into()),
            f("bidder"),
            f("price"),
        ]),
    }];
    let fields = plan_close_ctx_fields(&plan);
    match &fields {
        CloseCtxFields::Named(set) => {
            assert!(set.contains("bidder"), "fmt 参数 bidder 应收集");
            assert!(set.contains("price"), "fmt 参数 price 应收集");
        }
        _ => panic!("fmt detail 应窄化为 Named，got {fields:?}"),
    }

    // L3 聚合（collect_set 读 `_step_*` 合成字段）→ 仍 force_all。
    let mut plan = base();
    plan.yield_plan.fields = vec![YieldField {
        name: "agg".into(),
        value: Expr::FuncCall {
            qualifier: None,
            name: "collect_set".into(),
            args: vec![f("bidder")],
        },
    }];
    assert!(
        matches!(plan_close_ctx_fields(&plan), CloseCtxFields::All),
        "L3 聚合必须 All（读合成字段）"
    );

    // fmt 引用合成字段 → 仍 force_all（Field 的 `_` 前缀检查）。
    let mut plan = base();
    plan.yield_plan.fields = vec![YieldField {
        name: "detail".into(),
        value: fmt(vec![
            Expr::StringLit("{}".into()),
            Expr::Field(FieldRef::Simple("_step_0_measure".into())),
        ]),
    }];
    assert!(
        matches!(plan_close_ctx_fields(&plan), CloseCtxFields::All),
        "fmt 引用合成字段必须 All"
    );
}

/// fmt 参数为结构化（object）字段：形状 gate 放行，但编译失败 → 行式回退，
/// 输出与纯行式路径逐字段一致（object 渲染 [object]）。
#[test]
fn each_columnar_fmt_structured_arg_falls_back_matches_row_path() {
    use crate::match_engine::WFL_FIELD_TYPE_METADATA_KEY;
    use crate::match_engine::WFL_FIELD_TYPE_OBJECT;
    use wp_model_core::model::Value as ModelValue;

    let schema = Arc::new(Schema::new(vec![
        ArrowField::new("ext", DataType::Utf8, true).with_metadata(
            std::collections::HashMap::from([(
                WFL_FIELD_TYPE_METADATA_KEY.to_string(),
                WFL_FIELD_TYPE_OBJECT.to_string(),
            )]),
        ),
        ArrowField::new("sip", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![
                Some(r#"{"k":1}"#),
                Some(r#"{"k":2}"#),
            ])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("10.0.0.1"), Some("10.0.0.2")])) as ArrayRef,
        ],
    )
    .unwrap();

    let mut plan = simple_rule_plan(
        "obj_fmt",
        simple_plan(vec![], vec![]),
        Expr::Number(50.0),
        "ip",
        Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    plan.yield_plan.fields = vec![YieldField {
        name: "label".into(),
        value: Expr::FuncCall {
            qualifier: None,
            name: "fmt".into(),
            args: vec![
                Expr::StringLit("x={}".into()),
                Expr::Field(FieldRef::Qualified("e".into(), "ext".into())),
            ],
        },
    }];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([("label".into(), FieldType::Base(BaseType::Chars))]),
    );
    // 形状 gate 放行；执行时结构化参数编译失败 → 行式回退（不 panic）。
    assert!(exec.each_plan_columnar_safe());

    let t = 1_700_000_000_000_000_000i64;
    let events = crate::match_engine::event_bridge::batch_to_events(&batch);
    let row_refs: Vec<(&Event, i64)> = events.iter().map(|e| (e, t)).collect();
    let mut b_row = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut app_row = Vec::new();
    let sr =
        exec.execute_each_direct_batch(&row_refs, &EmptyLookup, &[], 0, &mut b_row, &mut app_row);
    assert_eq!(sr.appended, 2);
    let out_row: Vec<_> = b_row
        .finish()
        .iter_data_records()
        .map(|r| r.unwrap())
        .collect();

    let col_events: Vec<ColumnarEvent> = (0..2).map(|r| ColumnarEvent::new(&batch, r)).collect();
    let col_refs: Vec<(&ColumnarEvent, i64)> = col_events.iter().map(|ev| (ev, t)).collect();
    let mut b_col = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut app_col = Vec::new();
    let sc = exec.execute_each_direct_batch_columnar(&col_refs, 0, &mut b_col, &mut app_col);
    assert_eq!(sc.appended, 2);
    assert_eq!(sc.failed, 0);
    let out_col: Vec<_> = b_col
        .finish()
        .iter_data_records()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(out_row, out_col);
    // 行式回退渲染 [object]。
    let label = |r: &wp_model_core::model::DataRecord| {
        r.fields()
            .find(|f| f.get_name() == "label")
            .and_then(|f| match f.get_value() {
                ModelValue::Chars(v) => Some(v.to_string()),
                _ => None,
            })
            .expect("label field")
    };
    assert_eq!(label(&out_col[0]), "x=[object]", "object 参数渲染 [object]");
}

/// Q14 形态的 on-each 规则：each filter（`0.908*price` 价格区间，列式算术
/// 比较）+ yield fmt（IfThenElse+InList+count_char 递归列式）。行式 vs 列式
/// 批路径统计与输出逐位对拍（含 each filter 拒绝行）。
#[test]
fn each_columnar_q14_filter_matches_row_path() {
    use wp_model_core::model::Value as ModelValue;

    let schema = Arc::new(Schema::new(vec![
        ArrowField::new("auction", DataType::Int64, true),
        ArrowField::new("price", DataType::Int64, true),
        ArrowField::new("dateTime", DataType::Int64, true),
        ArrowField::new("extra", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                Some(4),
                None,
                Some(6),
            ])) as ArrayRef,
            Arc::new(Int64Array::from(vec![
                // 0.908*price 需 ∈ (1M, 50M)：1M 不过、5M 过、60M 不过、10M 过、null 不过、20M 过。
                Some(1_000_000),
                Some(5_000_000),
                Some(60_000_000),
                Some(10_000_000),
                None,
                Some(20_000_000),
            ])) as ArrayRef,
            Arc::new(Int64Array::from(vec![
                // 真实 3 档 CASE：22 时 → nightTime；10 时（-12h）→ dayTime；07 时（-15h）→ otherTime。
                Some(1_700_000_000_000_000_000),
                Some(1_700_000_000_000_000_000),
                Some(1_700_000_000_000_000_000),
                Some(1_700_000_000_000_000_000 - 12 * 3_600_000_000_000),
                None,
                Some(1_700_000_000_000_000_000 - 15 * 3_600_000_000_000),
            ])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some("abc"),
                Some("abc c cc"),
                Some("x"),
                Some("no-c"),
                None,
                Some("zz"),
            ])) as ArrayRef,
        ],
    )
    .unwrap();

    let b_field = |n: &str| Expr::Field(FieldRef::Qualified("b".into(), n.into()));
    let call = |name: &str, args: Vec<Expr>| Expr::FuncCall {
        qualifier: None,
        name: name.into(),
        args,
    };
    let mut plan = simple_rule_plan(
        "q14_filter",
        simple_plan(vec![], vec![]),
        Expr::Number(5.0),
        "digit",
        b_field("auction"),
    );
    plan.binds[0].alias = "b".into();
    plan.binds[0].window = "bid_events".into();
    plan.each_plan = Some(EachPlan {
        alias: "b".into(),
        filter: Some(Expr::BinOp {
            op: BinOp::And,
            left: Box::new(Expr::BinOp {
                op: BinOp::Gt,
                left: Box::new(Expr::BinOp {
                    op: BinOp::Mul,
                    left: Box::new(Expr::Number(0.908)),
                    right: Box::new(b_field("price")),
                }),
                right: Box::new(Expr::Number(1_000_000.0)),
            }),
            right: Box::new(Expr::BinOp {
                op: BinOp::Lt,
                left: Box::new(Expr::BinOp {
                    op: BinOp::Mul,
                    left: Box::new(Expr::Number(0.908)),
                    right: Box::new(b_field("price")),
                }),
                right: Box::new(Expr::Number(50_000_000.0)),
            }),
        }),
    });
    // 真实 q14.wfl：嵌套 3 档 CASE（nightTime/dayTime/otherTime，10/9 项 InList）。
    let in_hours = |hours: &[&str]| Expr::InList {
        expr: Box::new(call(
            "strftime",
            vec![b_field("dateTime"), Expr::StringLit("%H".into())],
        )),
        list: hours.iter().map(|h| Expr::StringLit((*h).into())).collect(),
        negated: false,
    };
    plan.yield_plan.fields = vec![YieldField {
        name: "detail".into(),
        value: call(
            "fmt",
            vec![
                Expr::StringLit("{} c={}".into()),
                Expr::IfThenElse {
                    cond: Box::new(in_hours(&[
                        "00", "01", "02", "03", "04", "05", "06", "20", "21", "22", "23",
                    ])),
                    then_expr: Box::new(Expr::StringLit("nightTime".into())),
                    else_expr: Box::new(Expr::IfThenElse {
                        cond: Box::new(in_hours(&[
                            "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18",
                        ])),
                        then_expr: Box::new(Expr::StringLit("dayTime".into())),
                        else_expr: Box::new(Expr::StringLit("otherTime".into())),
                    }),
                },
                call(
                    "count_char",
                    vec![b_field("extra"), Expr::StringLit("c".into())],
                ),
            ],
        ),
    }];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([("detail".into(), FieldType::Base(BaseType::Chars))]),
    );
    assert!(
        exec.each_plan_columnar_safe(),
        "Q14 each filter + 递归输出函数应列式放行"
    );

    let t = 1_700_000_000_000_000_000i64;
    let events = crate::match_engine::event_bridge::batch_to_events(&batch);
    let row_refs: Vec<(&Event, i64)> = events.iter().map(|e| (e, t)).collect();
    let mut b_row = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut app_row = Vec::new();
    let sr =
        exec.execute_each_direct_batch(&row_refs, &EmptyLookup, &[], 0, &mut b_row, &mut app_row);
    let out_row: Vec<_> = b_row
        .finish()
        .iter_data_records()
        .map(|r| r.unwrap())
        .collect();

    let col_events: Vec<ColumnarEvent> = (0..6).map(|r| ColumnarEvent::new(&batch, r)).collect();
    let col_refs: Vec<(&ColumnarEvent, i64)> = col_events.iter().map(|ev| (ev, t)).collect();
    let mut b_col = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut app_col = Vec::new();
    let sc = exec.execute_each_direct_batch_columnar(&col_refs, 0, &mut b_col, &mut app_col);

    // 统计对拍：each filter 拒绝 3 行（1M 低于区间 / 60M 高于区间 / null），
    // 追加 3 行（5M / 10M / 20M）。
    assert_eq!(sr.appended, 3, "行式 appended");
    assert_eq!(sr.rejected, 3, "行式 rejected");
    assert_eq!(sc.appended, 3, "列式 appended");
    assert_eq!(sc.rejected, 3, "列式 rejected");
    assert_eq!(sc.failed, 0);
    assert_eq!(app_row, vec![1usize, 3, 5], "行式 appended 索引");
    assert_eq!(app_col, vec![1usize, 3, 5], "列式 appended 索引");

    let out_col: Vec<_> = b_col
        .finish()
        .iter_data_records()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(out_row, out_col, "输出逐位对拍");
    let label = |r: &wp_model_core::model::DataRecord| {
        r.fields()
            .find(|f| f.get_name() == "detail")
            .and_then(|f| match f.get_value() {
                ModelValue::Chars(v) => Some(v.to_string()),
                _ => None,
            })
            .expect("detail field")
    };
    assert_eq!(
        label(&out_col[0]),
        "nightTime c=4",
        "5M 行：22 时 → nightTime，\"abc c cc\" 含 4 个 c"
    );
    assert_eq!(
        label(&out_col[1]),
        "dayTime c=1",
        "10M 行：10 时 → dayTime，\"no-c\" 含 1 个 c"
    );
    assert_eq!(
        label(&out_col[2]),
        "otherTime c=0",
        "20M 行：07 时 → otherTime，\"zz\" 无 c"
    );
}
