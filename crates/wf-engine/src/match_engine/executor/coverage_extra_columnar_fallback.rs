//! coverage_extra 拆出的兄弟子模块（2026-09-04）：P4 gap-4 列式路径逐行解释回退对拍——
//! 非列式 each filter / bind filter、嵌套结构化参数、缺失列拒绝、空批 no-op、多个
//! general yield 交插。共享 harness 在父模块 `coverage_extra.rs`，此处经 `use super::*`
//! 复用。

use super::*;

use crate::alert::AlertColumnBuilder;
use crate::match_engine::event_bridge::ColumnarEvent;
use arrow::array::{ArrayRef, BooleanArray, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field as ArrowField, Schema};
use arrow::record_batch::RecordBatch;
use wf_lang::ast::BinOp;

// ---------------------------------------------------------------------------
// P4 gap-4（2026-09-02）：非列式 each filter / bind filter → 列式路径逐行
// 解释回退——行式/列式逐位对拍（filter 语义不丢：each filter 经 to_event +
// passes_each_filter；bind filter 经 process_batch 命中循环 event_matches_alias）。
// ---------------------------------------------------------------------------

/// gap-4 对拍夹具：批 + 非列式 each filter，行式/列式双路输出逐位一致。
#[test]
fn each_columnar_nonexpr_each_filter_matches_row_path() {
    let schema = Arc::new(Schema::new(vec![
        ArrowField::new("sip", DataType::Utf8, true),
        ArrowField::new("category", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec![
                Some("abc"),
                Some("AB"),
                Some("xyz"),
                None,
                Some("abc"),
            ])) as ArrayRef,
            Arc::new(Int64Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                Some(4),
                Some(5),
            ])) as ArrayRef,
        ],
    )
    .unwrap();
    let mut plan = simple_rule_plan(
        "gap4_each_filter",
        simple_plan(vec![], vec![]),
        Expr::Number(5.0),
        "ip",
        Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
    );
    plan.binds[0].alias = "e".into();
    // 非列式 each filter：upper(sip) == "ABC"（upper 不在守卫列式清单）。
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: Some(Expr::BinOp {
            op: BinOp::Eq,
            left: Box::new(Expr::FuncCall {
                qualifier: None,
                name: "upper".into(),
                args: vec![Expr::Field(FieldRef::Qualified("e".into(), "sip".into()))],
            }),
            right: Box::new(Expr::StringLit("ABC".into())),
        }),
    });
    plan.yield_plan.fields = vec![YieldField {
        name: "sip_out".into(),
        value: Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
    }];
    let exec = RuleExecutor::new(plan);
    assert!(
        exec.each_plan_columnar_safe(),
        "gap-4：非列式 each filter 必须放行（逐行解释回退）"
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

    let col_events: Vec<ColumnarEvent> = (0..batch.num_rows())
        .map(|r| ColumnarEvent::new(&batch, r))
        .collect();
    let col_refs: Vec<(&ColumnarEvent, i64)> = col_events.iter().map(|ev| (ev, t)).collect();
    let mut b_col = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut app_col = Vec::new();
    let sc = exec.execute_each_direct_batch_columnar(&col_refs, 0, &mut b_col, &mut app_col);
    let out_col: Vec<_> = b_col
        .finish()
        .iter_data_records()
        .map(|r| r.unwrap())
        .collect();

    // upper(sip)=="ABC" → 行 0/4（"abc"）过；行 1（"AB"）、行 2（"xyz"）、
    // 行 3（null → upper null → filter None）拒。
    assert_eq!(sr.appended, 2, "行式 appended");
    assert_eq!(sr.rejected, 3, "行式 rejected");
    assert_eq!(sc.appended, 2, "列式 appended");
    assert_eq!(sc.rejected, 3, "列式 rejected");
    assert_eq!(app_row, vec![0usize, 4], "行式 appended 索引");
    assert_eq!(app_col, vec![0usize, 4], "列式 appended 索引");
    assert_eq!(out_row, out_col, "非列式 each filter 输出逐位对拍");
}

/// gap-4 组合：非列式 each filter + 非列式 bind filter 同规则——列式路径
/// （each filter 逐行解释 + bind filter 命中循环解释）== 行式路径。
#[test]
fn each_columnar_nonexpr_filter_and_bind_matches_row_path() {
    let schema = Arc::new(Schema::new(vec![
        ArrowField::new("sip", DataType::Utf8, true),
        ArrowField::new("category", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec![
                Some("abc"),
                Some("AB"),
                Some("abc"),
            ])) as ArrayRef,
            Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3)])) as ArrayRef,
        ],
    )
    .unwrap();
    let mut plan = simple_rule_plan(
        "gap4_both",
        simple_plan(vec![], vec![]),
        Expr::Number(5.0),
        "ip",
        Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
    );
    plan.binds[0].alias = "e".into();
    // 非列式 bind filter：category 非偶数（函数不在守卫清单）。
    plan.binds[0].filter = Some(Expr::FuncCall {
        qualifier: None,
        name: "mod".into(),
        args: vec![
            Expr::Field(FieldRef::Qualified("e".into(), "category".into())),
            Expr::Number(2.0),
        ],
    });
    // 非列式 each filter：upper(sip) == "ABC"。
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: Some(Expr::BinOp {
            op: BinOp::Eq,
            left: Box::new(Expr::FuncCall {
                qualifier: None,
                name: "upper".into(),
                args: vec![Expr::Field(FieldRef::Qualified("e".into(), "sip".into()))],
            }),
            right: Box::new(Expr::StringLit("ABC".into())),
        }),
    });
    plan.yield_plan.fields = vec![YieldField {
        name: "sip_out".into(),
        value: Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
    }];
    let exec = RuleExecutor::new(plan);
    assert!(exec.each_plan_columnar_safe(), "gap-4 组合必须放行");
    let t = 1_700_000_000_000_000_000i64;
    let events = crate::match_engine::event_bridge::batch_to_events(&batch);
    let row_refs: Vec<(&Event, i64)> = events.iter().map(|e| (e, t)).collect();
    let mut b_row = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut app_row = Vec::new();
    let sr =
        exec.execute_each_direct_batch(&row_refs, &EmptyLookup, &[], 0, &mut b_row, &mut app_row);
    let col_events: Vec<ColumnarEvent> = (0..batch.num_rows())
        .map(|r| ColumnarEvent::new(&batch, r))
        .collect();
    let col_refs: Vec<(&ColumnarEvent, i64)> = col_events.iter().map(|ev| (ev, t)).collect();
    let mut b_col = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut app_col = Vec::new();
    let sc = exec.execute_each_direct_batch_columnar(&col_refs, 0, &mut b_col, &mut app_col);
    // bind filter 由调用方（process_batch 命中循环）应用——executor 层双路都
    // 全行进（bind 过滤不在此处）；断言 each filter 双路一致（bind 过滤的
    // 对拍由引擎层测试 each_noncolumnar_bind_filter_* 覆盖）。
    assert_eq!(sr.appended, sc.appended, "appended 一致");
    assert_eq!(sr.rejected, sc.rejected, "rejected 一致（each filter）");
    assert_eq!(app_row, app_col, "appended 索引一致（each filter）");
}

/// Q14 变体：fmt 的 IfThenElse 分支 / count_char 参数含 OBJECT 元数据字段。
/// gate 放行（flat FieldRef），但编译期递归 `arg_reads_structured` 拦截 →
/// 整个 yield 行式回退——行式/列式输出必须逐位一致（列式若不回退会渲染原始
/// JSON / 对 JSON 计数，字节分叉）。
#[test]
fn each_columnar_nested_structured_falls_back_matches_row_path() {
    use crate::match_engine::WFL_FIELD_TYPE_METADATA_KEY;
    use crate::match_engine::WFL_FIELD_TYPE_OBJECT;
    use wp_model_core::model::Value as ModelValue;

    let schema = Arc::new(Schema::new(vec![
        ArrowField::new("auction", DataType::Int64, true),
        ArrowField::new("flag", DataType::Boolean, true),
        ArrowField::new("ext", DataType::Utf8, true).with_metadata(
            std::collections::HashMap::from([(
                WFL_FIELD_TYPE_METADATA_KEY.to_string(),
                WFL_FIELD_TYPE_OBJECT.to_string(),
            )]),
        ),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![Some(1), Some(2), None])) as ArrayRef,
            Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(false),
                Some(true),
            ])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some(r#"{"k":1}"#),
                Some(r#"{"c":2}"#),
                None,
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
        "q14_obj",
        simple_plan(vec![], vec![]),
        Expr::Number(5.0),
        "digit",
        b_field("auction"),
    );
    plan.binds[0].alias = "b".into();
    plan.binds[0].window = "bid_events".into();
    plan.each_plan = Some(EachPlan {
        alias: "b".into(),
        filter: None,
    });
    // label = fmt("{} {}", if b.flag then b.ext else "x", "y")——结构化藏在分支。
    // cc    = count_char(b.ext, "c")——结构化作 text 参数（解释器 None → 空串）。
    plan.yield_plan.fields = vec![
        YieldField {
            name: "label".into(),
            value: call(
                "fmt",
                vec![
                    Expr::StringLit("{} {}".into()),
                    Expr::IfThenElse {
                        cond: Box::new(b_field("flag")),
                        then_expr: Box::new(b_field("ext")),
                        else_expr: Box::new(Expr::StringLit("x".into())),
                    },
                    Expr::StringLit("y".into()),
                ],
            ),
        },
        YieldField {
            name: "cc".into(),
            value: call(
                "count_char",
                vec![b_field("ext"), Expr::StringLit("c".into())],
            ),
        },
    ];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([
            ("label".into(), FieldType::Base(BaseType::Chars)),
            ("cc".into(), FieldType::Base(BaseType::Chars)),
        ]),
    );
    // 形状 gate 放行（分支/参数是 flat FieldRef）……
    assert!(exec.each_plan_columnar_safe());

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

    let col_events: Vec<ColumnarEvent> = (0..3).map(|r| ColumnarEvent::new(&batch, r)).collect();
    let col_refs: Vec<(&ColumnarEvent, i64)> = col_events.iter().map(|ev| (ev, t)).collect();
    let mut b_col = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut app_col = Vec::new();
    let sc = exec.execute_each_direct_batch_columnar(&col_refs, 0, &mut b_col, &mut app_col);

    assert_eq!(sr.appended, 3, "行式 appended");
    assert_eq!(
        sc.appended, 3,
        "列式 appended（结构化回退仍应产出全部 3 行）"
    );
    assert_eq!(sr.rejected, 0);
    assert_eq!(sc.rejected, 0);
    assert_eq!(sc.failed, 0);
    assert_eq!(app_row, vec![0usize, 1, 2]);
    assert_eq!(app_col, vec![0usize, 1, 2]);

    let out_col: Vec<_> = b_col
        .finish()
        .iter_data_records()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(out_row, out_col, "结构化嵌套必须行式回退且输出逐位一致");
    let get = |r: &wp_model_core::model::DataRecord, name: &str| {
        r.fields()
            .find(|f| f.get_name() == name)
            .and_then(|f| match f.get_value() {
                ModelValue::Chars(v) => Some(v.to_string()),
                _ => None,
            })
            .expect(name)
    };
    // label：row 0 true 分支 → [object]；row 1 false 分支 → "x"；row 2 null ext → 空串。
    assert_eq!(
        get(&out_col[0], "label"),
        "[object] y",
        "true 分支渲染 [object]（列式若未回退会渲染原始 JSON）"
    );
    assert_eq!(get(&out_col[1], "label"), "x y", "false 分支渲染 x");
    assert_eq!(
        get(&out_col[2], "label"),
        "",
        "null ext → fmt 参数 None → 空串"
    );
    // cc：count_char(Object) → None → 空串（列式若未回退会对原始 JSON 文本计数）。
    assert_eq!(
        get(&out_col[0], "cc"),
        "",
        "count_char(Object) → None → 空串"
    );
    assert_eq!(
        get(&out_col[1], "cc"),
        "",
        "count_char(Object) → None → 空串"
    );
    assert_eq!(get(&out_col[2], "cc"), "", "count_char(null) → None → 空串");
}

/// 空 rows：wrapper 与 `_with` 都应安全返回零统计（batch 级注册/预留对空批是
/// no-op，循环不执行；`emit_each_direct_batch_columnar` 的空行早退路径同源）。
#[test]
fn each_columnar_empty_rows_is_noop() {
    let b_field = |n: &str| Expr::Field(FieldRef::Qualified("b".into(), n.into()));
    let mut plan = simple_rule_plan(
        "empty",
        simple_plan(vec![], vec![]),
        Expr::Number(5.0),
        "digit",
        b_field("auction"),
    );
    plan.binds[0].alias = "b".into();
    plan.each_plan = Some(EachPlan {
        alias: "b".into(),
        // 列式 filter：即便有 filter，空 rows 也不该有任何求值/拒绝。
        filter: Some(Expr::Bool(true)),
    });
    plan.yield_plan.fields = vec![YieldField {
        name: "detail".into(),
        value: Expr::StringLit("x".into()),
    }];
    let exec = RuleExecutor::new(plan);
    assert!(exec.each_plan_columnar_safe());

    // wrapper（prepare default 路径）。
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut appended = Vec::new();
    let stats = exec.execute_each_direct_batch_columnar(&[], 0, &mut builder, &mut appended);
    assert_eq!(stats.appended, 0);
    assert_eq!(stats.rejected, 0);
    assert_eq!(stats.failed, 0);
    assert!(appended.is_empty());
    assert_eq!(builder.finish().len(), 0, "空批不得产出任何行");

    // _with（真实 prepared + 空 rows）：debug_assert 不得触发，统计为零。
    let schema = Arc::new(Schema::new(vec![ArrowField::new(
        "auction",
        DataType::Int64,
        true,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![Some(1)])) as ArrayRef],
    )
    .unwrap();
    let prepared = exec.each_batch_prepare(&batch);
    let mut b2 = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut app2 = Vec::new();
    let s2 = exec.execute_each_direct_batch_columnar_with(&[], 0, &prepared, &mut b2, &mut app2);
    assert_eq!(s2.appended, 0);
    assert_eq!(s2.rejected, 0);
    assert_eq!(s2.failed, 0);
    assert!(app2.is_empty());
}

/// each filter 引用批 schema 里不存在的列：gate 放行（形状可列式），列式编译
/// 解析成 `ColKind::Null` → 掩码全 None → 全拒绝；行式 `passes_each_filter`
/// 对缺字段求值 None → 同样全拒绝。两路统计与输出必须一致。
#[test]
fn each_columnar_filter_missing_column_rejects_all_parity() {
    let b_field = |n: &str| Expr::Field(FieldRef::Qualified("b".into(), n.into()));
    let mut plan = simple_rule_plan(
        "missing_filter",
        simple_plan(vec![], vec![]),
        Expr::Number(5.0),
        "digit",
        b_field("auction"),
    );
    plan.binds[0].alias = "b".into();
    plan.each_plan = Some(EachPlan {
        alias: "b".into(),
        // b.price 不在下面 batch 的 schema 里。
        filter: Some(Expr::BinOp {
            op: BinOp::Gt,
            left: Box::new(b_field("price")),
            right: Box::new(Expr::Number(1.0)),
        }),
    });
    plan.yield_plan.fields = vec![YieldField {
        name: "detail".into(),
        value: Expr::StringLit("x".into()),
    }];
    let exec = RuleExecutor::new(plan);
    assert!(exec.each_plan_columnar_safe());

    let schema = Arc::new(Schema::new(vec![ArrowField::new(
        "auction",
        DataType::Int64,
        true,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![Some(1), Some(2), None])) as ArrayRef],
    )
    .unwrap();
    let t = 1_700_000_000_000_000_000i64;

    let events = crate::match_engine::event_bridge::batch_to_events(&batch);
    let row_refs: Vec<(&Event, i64)> = events.iter().map(|e| (e, t)).collect();
    let mut b_row = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut app_row = Vec::new();
    let sr =
        exec.execute_each_direct_batch(&row_refs, &EmptyLookup, &[], 0, &mut b_row, &mut app_row);
    assert_eq!(sr.rejected, 3, "行式：缺字段 → None → 全拒绝");
    assert_eq!(sr.appended, 0);

    let col_events: Vec<ColumnarEvent> = (0..3).map(|r| ColumnarEvent::new(&batch, r)).collect();
    let col_refs: Vec<(&ColumnarEvent, i64)> = col_events.iter().map(|ev| (ev, t)).collect();
    let mut b_col = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut app_col = Vec::new();
    let sc = exec.execute_each_direct_batch_columnar(&col_refs, 0, &mut b_col, &mut app_col);
    assert_eq!(sc.rejected, 3, "列式：ColKind::Null → 全拒绝");
    assert_eq!(sc.appended, 0);
    assert_eq!(sc.failed, 0);
    assert_eq!(app_row, Vec::<usize>::new());
    assert_eq!(app_col, Vec::<usize>::new());
}

/// 回归：General（列式输出函数）yield **不在字段位 0**——前面有 Field/Lit
/// 字段（真实 q14：id=Field, alert_type=Lit, detail=fmt General, request_count=Lit）。
/// 此前 general_cvecs 用「只数 General 的游标」索引，错位取到 Field/Lit 槽位
/// （None）→ 误走行式回退 + yield_meta 悬空 panic。必须逐位对拍。
#[test]
fn each_columnar_general_yield_not_first_matches_row_path() {
    use wp_model_core::model::Value as ModelValue;

    let schema = Arc::new(Schema::new(vec![
        ArrowField::new("auction", DataType::Int64, true),
        ArrowField::new("price", DataType::Int64, true),
        ArrowField::new("extra", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![Some(11), Some(22), None])) as ArrayRef,
            Arc::new(Int64Array::from(vec![Some(7), Some(8), Some(9)])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("ab c"), Some("cc"), None])) as ArrayRef,
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
        "mixed_yield_order",
        simple_plan(vec![], vec![]),
        Expr::Number(5.0),
        "digit",
        b_field("auction"),
    );
    plan.binds[0].alias = "b".into();
    plan.each_plan = Some(EachPlan {
        alias: "b".into(),
        filter: None,
    });
    // 字段顺序刻意让 General 落在第 3 位（前面 Field + Lit）。
    plan.yield_plan.fields = vec![
        YieldField {
            name: "id".into(),
            value: b_field("auction"),
        },
        YieldField {
            name: "alert_type".into(),
            value: Expr::StringLit("q14_calc".into()),
        },
        YieldField {
            name: "detail".into(),
            value: call(
                "fmt",
                vec![
                    Expr::StringLit("c={} p={}".into()),
                    call(
                        "count_char",
                        vec![b_field("extra"), Expr::StringLit("c".into())],
                    ),
                    b_field("price"),
                ],
            ),
        },
        YieldField {
            name: "request_count".into(),
            value: Expr::Number(1.0),
        },
    ];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([
            ("id".into(), FieldType::Base(BaseType::Float)),
            ("alert_type".into(), FieldType::Base(BaseType::Chars)),
            ("detail".into(), FieldType::Base(BaseType::Chars)),
            ("request_count".into(), FieldType::Base(BaseType::Float)),
        ]),
    );
    assert!(exec.each_plan_columnar_safe());

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

    let col_events: Vec<ColumnarEvent> = (0..3).map(|r| ColumnarEvent::new(&batch, r)).collect();
    let col_refs: Vec<(&ColumnarEvent, i64)> = col_events.iter().map(|ev| (ev, t)).collect();
    let mut b_col = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut app_col = Vec::new();
    let sc = exec.execute_each_direct_batch_columnar(&col_refs, 0, &mut b_col, &mut app_col);

    assert_eq!(sr.appended, 3, "行式 appended");
    assert_eq!(sr.rejected, 0);
    assert_eq!(
        sc.appended, 3,
        "列式 appended（General 不在字段 0 也必须全编译）"
    );
    assert_eq!(sc.rejected, 0);
    assert_eq!(sc.failed, 0);
    assert_eq!(app_row, vec![0usize, 1, 2]);
    assert_eq!(app_col, vec![0usize, 1, 2]);

    let out_col: Vec<_> = b_col
        .finish()
        .iter_data_records()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(out_row, out_col, "混合 yield 顺序必须逐位一致");
    let detail = |r: &wp_model_core::model::DataRecord| {
        r.fields()
            .find(|f| f.get_name() == "detail")
            .and_then(|f| match f.get_value() {
                ModelValue::Chars(v) => Some(v.to_string()),
                _ => None,
            })
            .expect("detail field")
    };
    // 列式必须真的命中 fmt 槽位（错位取 None 会误回退成空串/悬空 panic）。
    assert_eq!(detail(&out_col[0]), "c=1 p=7");
    assert_eq!(detail(&out_col[1]), "c=2 p=8");
    assert_eq!(
        detail(&out_col[2]),
        "",
        "null extra → count_char None → fmt 参数 None → 空串"
    );
}

/// each filter 引用 OBJECT 元数据列：gate 放行（flat FieldRef 形状），但列式
/// 读原始 JSON 文本、解释器解析成 Value::Object——比较可分叉 → filter 槽位
/// 不编译，逐行 `passes_eval_filter` 解释回退。两路必须一致（Object 比较
/// 非 Bool → None → 全拒绝）。
#[test]
fn each_columnar_filter_structured_field_falls_back_parity() {
    use crate::match_engine::WFL_FIELD_TYPE_METADATA_KEY;
    use crate::match_engine::WFL_FIELD_TYPE_OBJECT;

    let b_field = |n: &str| Expr::Field(FieldRef::Qualified("b".into(), n.into()));
    let mut plan = simple_rule_plan(
        "obj_filter",
        simple_plan(vec![], vec![]),
        Expr::Number(5.0),
        "digit",
        b_field("auction"),
    );
    plan.binds[0].alias = "b".into();
    plan.each_plan = Some(EachPlan {
        alias: "b".into(),
        // 原始 JSON 文本恰好等于字面量时，列式会比较命中——解释器是 Object
        // 比较非 Bool → 拒绝；必须走解释回退保持一致。
        filter: Some(Expr::BinOp {
            op: BinOp::Eq,
            left: Box::new(b_field("ext")),
            right: Box::new(Expr::StringLit("{\"k\":1}".into())),
        }),
    });
    plan.yield_plan.fields = vec![YieldField {
        name: "detail".into(),
        value: Expr::StringLit("x".into()),
    }];
    let exec = RuleExecutor::new(plan);
    assert!(exec.each_plan_columnar_safe());

    let schema = Arc::new(Schema::new(vec![
        ArrowField::new("auction", DataType::Int64, true),
        ArrowField::new("ext", DataType::Utf8, true).with_metadata(
            std::collections::HashMap::from([(
                WFL_FIELD_TYPE_METADATA_KEY.to_string(),
                WFL_FIELD_TYPE_OBJECT.to_string(),
            )]),
        ),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![Some(1), Some(2)])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some(r#"{"k":1}"#),
                Some(r#"{"k":2}"#),
            ])) as ArrayRef,
        ],
    )
    .unwrap();
    let t = 1_700_000_000_000_000_000i64;

    let events = crate::match_engine::event_bridge::batch_to_events(&batch);
    let row_refs: Vec<(&Event, i64)> = events.iter().map(|e| (e, t)).collect();
    let mut b_row = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut app_row = Vec::new();
    let sr =
        exec.execute_each_direct_batch(&row_refs, &EmptyLookup, &[], 0, &mut b_row, &mut app_row);
    assert_eq!(sr.rejected, 2, "行式：Object 比较非 Bool → None → 全拒绝");
    assert_eq!(sr.appended, 0);

    let col_events: Vec<ColumnarEvent> = (0..2).map(|r| ColumnarEvent::new(&batch, r)).collect();
    let col_refs: Vec<(&ColumnarEvent, i64)> = col_events.iter().map(|ev| (ev, t)).collect();
    let mut b_col = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut app_col = Vec::new();
    let sc = exec.execute_each_direct_batch_columnar(&col_refs, 0, &mut b_col, &mut app_col);
    assert_eq!(
        sc.rejected, 2,
        "列式：结构化 filter 槽位不编译 → 解释回退 → 全拒绝"
    );
    assert_eq!(sc.appended, 0);
    assert_eq!(sc.failed, 0);
    assert_eq!(app_row, Vec::<usize>::new());
    assert_eq!(app_col, Vec::<usize>::new());
}

/// 形状矩阵收口：**多个 General 被 Field/Lit 隔开**（Field, General, Lit,
/// General）——每个 General 槽位按字段位置独立命中，`need_yield_meta` 与
/// 槽位映射必须对齐。若有人把位置索引改回「只数 General 的游标」，此形状
/// 会同时错位两个 General（修复前 general_cvecs 游标 bug 的完整触发面）。
#[test]
fn each_columnar_multiple_generals_interspersed_matches_row_path() {
    use wp_model_core::model::Value as ModelValue;

    let schema = Arc::new(Schema::new(vec![
        ArrowField::new("auction", DataType::Int64, true),
        ArrowField::new("ts", DataType::Int64, true),
        ArrowField::new("extra", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![Some(11), Some(22), None])) as ArrayRef,
            Arc::new(Int64Array::from(vec![
                Some(1_700_000_000_000_000_000),
                Some(1_700_000_000_000_000_000),
                None,
            ])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("ab c"), Some("cc"), None])) as ArrayRef,
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
        "mixed_interspersed",
        simple_plan(vec![], vec![]),
        Expr::Number(5.0),
        "digit",
        b_field("auction"),
    );
    plan.binds[0].alias = "b".into();
    plan.each_plan = Some(EachPlan {
        alias: "b".into(),
        filter: None,
    });
    // 刻意：Field, General, Lit, General——两个 General 都被非 General 隔开。
    plan.yield_plan.fields = vec![
        YieldField {
            name: "id".into(),
            value: b_field("auction"),
        },
        YieldField {
            name: "day".into(),
            value: call(
                "strftime",
                vec![b_field("ts"), Expr::StringLit("%Y".into())],
            ),
        },
        YieldField {
            name: "alert_type".into(),
            value: Expr::StringLit("q14_calc".into()),
        },
        YieldField {
            name: "dots".into(),
            value: call(
                "count_char",
                vec![b_field("extra"), Expr::StringLit("c".into())],
            ),
        },
    ];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([
            ("id".into(), FieldType::Base(BaseType::Float)),
            ("day".into(), FieldType::Base(BaseType::Chars)),
            ("alert_type".into(), FieldType::Base(BaseType::Chars)),
            ("dots".into(), FieldType::Base(BaseType::Chars)),
        ]),
    );
    assert!(exec.each_plan_columnar_safe());

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

    let col_events: Vec<ColumnarEvent> = (0..3).map(|r| ColumnarEvent::new(&batch, r)).collect();
    let col_refs: Vec<(&ColumnarEvent, i64)> = col_events.iter().map(|ev| (ev, t)).collect();
    let mut b_col = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut app_col = Vec::new();
    let sc = exec.execute_each_direct_batch_columnar(&col_refs, 0, &mut b_col, &mut app_col);

    assert_eq!(sr.appended, 3);
    assert_eq!(sr.rejected, 0);
    assert_eq!(sc.appended, 3, "两个 General 槽位都必须命中");
    assert_eq!(sc.rejected, 0);
    assert_eq!(sc.failed, 0);
    assert_eq!(app_row, vec![0usize, 1, 2]);
    assert_eq!(app_col, vec![0usize, 1, 2]);

    let out_col: Vec<_> = b_col
        .finish()
        .iter_data_records()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(out_row, out_col, "交错多 General 必须逐位一致");
    let get = |r: &wp_model_core::model::DataRecord, name: &str| {
        r.fields()
            .find(|f| f.get_name() == name)
            .and_then(|f| match f.get_value() {
                ModelValue::Chars(v) => Some(v.to_string()),
                _ => None,
            })
            .expect(name)
    };
    // 两个 General 都得真命中各自槽位（错位会取到 Field/Lit 的 None → 空串）。
    assert_eq!(get(&out_col[0], "day"), "2023", "strftime 槽位 1 命中");
    assert_eq!(get(&out_col[1], "day"), "2023");
    assert_eq!(
        get(&out_col[0], "dots"),
        "1",
        "count_char 槽位 3 命中（\"ab c\" 含 1 个 c）"
    );
    assert_eq!(get(&out_col[1], "dots"), "2", "\"cc\" 含 2 个 c");
    assert_eq!(get(&out_col[2], "dots"), "", "null extra → None → 空串");
}
