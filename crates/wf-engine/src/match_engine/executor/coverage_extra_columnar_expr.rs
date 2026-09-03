//! coverage_extra 拆出的兄弟子模块（2026-09-04）：P4 gap-3/5/6/7 列式化对拍——each
//! 后置 where 的列式掩码（严格语义：false / 缺失 → 抑制；null / 非布尔 → 拒绝）、
//! 无活 join 的 list-index 输出字段 cvec、general score / entity 批级 cvec（编译失败或
//! 读结构化列 → 逐行回退）。共享 harness 在父模块 `coverage_extra.rs`，此处经
//! `use super::*` 复用。

use super::*;

use crate::alert::AlertColumnBuilder;
use crate::match_engine::event_bridge::ColumnarEvent;
use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field as ArrowField, Schema};
use arrow::record_batch::RecordBatch;
use wf_lang::ast::{BinOp, JoinMode, PathSegment};
use wf_lang::plan::{JoinCondPlan, JoinPlan};
use wp_model_core::model::DataRecord;

// ---------------------------------------------------------------------------
// P4 gap-3（2026-09-02）：each + 后置 where（无 join）列式化——行式/列式
// 逐位对拍（where 严格语义：false/缺失 → 抑制；列式掩码 null/非布尔 → 拒绝）。
// ---------------------------------------------------------------------------

/// gap-3 对拍夹具：批 + 行式/列式双路输出 + 统计 + 索引逐位一致。
/// `plan_mut` 允许调用方加 where / filter。真实可达形状 = each + **死 join**
/// （checker 要求 where 必须有 ≥1 join 子句；where 只读驱动列 → join 死消除
/// → live_joins 空）——死 join 让行式路径走 join 分支、`where_ok` 真正生效。
#[track_caller]
fn assert_each_columnar_where_matches_row(
    mut plan: RulePlan,
    batch: &RecordBatch,
) -> (RuleExecutor, Vec<usize>, Vec<usize>) {
    // 死 Snapshot join（where 只读驱动列 → live_joins 空）：可到达 gap-3 形状。
    plan.joins.push(JoinPlan {
        right_window: "person_events".into(),
        mode: JoinMode::Snapshot,
        conds: vec![JoinCondPlan {
            left: FieldRef::Qualified("e".into(), "id".into()),
            right: FieldRef::Qualified("person_events".into(), "id".into()),
        }],
        within: None,
        reduce: None,
        emit_at: None,
    });
    let exec = RuleExecutor::new(plan.clone());
    assert!(
        exec.live_joins().is_empty(),
        "{}: where 只读驱动列 → 死 join 消除",
        plan.name
    );
    assert!(
        exec.each_plan_columnar_safe(),
        "{}: each + 列式 where 必须放行",
        plan.name
    );
    let t = 1_700_000_000_000_000_000i64;
    let events = crate::match_engine::event_bridge::batch_to_events(batch);
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
        .map(|r| ColumnarEvent::new(batch, r))
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

    assert_eq!(
        sr.appended, sc.appended,
        "{}: appended 计数一致（行 {} vs 列 {}）",
        plan.name, sr.appended, sc.appended
    );
    assert_eq!(
        sr.rejected, sc.rejected,
        "{}: rejected 计数一致（行 {} vs 列 {}）",
        plan.name, sr.rejected, sc.rejected
    );
    assert_eq!(sr.failed, sc.failed);
    assert_eq!(app_row, app_col, "{}: appended 索引一致", plan.name);
    assert_eq!(out_row, out_col, "{}: 输出逐位对拍", plan.name);
    (exec, app_row, app_col)
}

/// gap-3 基础：where = 驱动字段 <cmp> 字面量（含 null 行 → 严格抑制）。
#[test]
fn each_columnar_where_matches_row_path() {
    let schema = Arc::new(Schema::new(vec![
        ArrowField::new("sip", DataType::Utf8, true),
        ArrowField::new("category", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec![
                Some("10.0.0.1"),
                Some("10.0.0.2"),
                Some("10.0.0.3"),
                Some("10.0.0.4"),
                Some("10.0.0.5"),
                None,
            ])) as ArrayRef,
            Arc::new(Int64Array::from(vec![
                Some(10), // 过
                Some(20), // where false
                Some(10), // 过
                None,     // where null → 严格抑制
                Some(30), // where false
                Some(10), // 过（sip null 不影响 where）
            ])) as ArrayRef,
        ],
    )
    .unwrap();
    let mut plan = simple_rule_plan(
        "gap3_where",
        simple_plan(vec![], vec![]),
        Expr::Number(5.0),
        "ip",
        Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    plan.r#where = Some(Expr::BinOp {
        op: BinOp::Eq,
        left: Box::new(Expr::Field(FieldRef::Qualified(
            "e".into(),
            "category".into(),
        ))),
        right: Box::new(Expr::Number(10.0)),
    });
    plan.yield_plan.fields = vec![YieldField {
        name: "sip_out".into(),
        value: Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
    }];
    let (_, app_row, app_col) = assert_each_columnar_where_matches_row(plan, &batch);
    // 行 0/2/5 过 where（category=10），行 1/3/4 被拒（20 / null / 30）。
    assert_eq!(app_row, vec![0usize, 2, 5], "行式 appended 索引");
    assert_eq!(app_col, vec![0usize, 2, 5], "列式 appended 索引");
}

/// gap-3 + each filter 组合：filter 先于 where（AND 语义，顺序一致）。
#[test]
fn each_columnar_where_after_filter_matches_row_path() {
    let schema = Arc::new(Schema::new(vec![
        ArrowField::new("sip", DataType::Utf8, true),
        ArrowField::new("category", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec![
                Some("10.0.0.1"),
                Some("10.0.0.2"),
                Some("10.0.0.3"),
            ])) as ArrayRef,
            Arc::new(Int64Array::from(vec![Some(10), Some(10), Some(20)])) as ArrayRef,
        ],
    )
    .unwrap();
    let mut plan = simple_rule_plan(
        "gap3_filter_where",
        simple_plan(vec![], vec![]),
        Expr::Number(5.0),
        "ip",
        Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        // filter：sip 非 10.0.0.3（列式 BinOp）→ 行 2 被 filter 拒。
        filter: Some(Expr::BinOp {
            op: BinOp::Ne,
            left: Box::new(Expr::Field(FieldRef::Qualified("e".into(), "sip".into()))),
            right: Box::new(Expr::StringLit("10.0.0.3".into())),
        }),
    });
    // where：category == 10 → 行 0 过、行 1 被 where 拒。
    plan.r#where = Some(Expr::BinOp {
        op: BinOp::Eq,
        left: Box::new(Expr::Field(FieldRef::Qualified(
            "e".into(),
            "category".into(),
        ))),
        right: Box::new(Expr::Number(10.0)),
    });
    let (_, app_row, app_col) = assert_each_columnar_where_matches_row(plan, &batch);
    // 行 0/1 过（sip≠10.0.0.3 且 category=10）；行 2 被 filter 拒。
    assert_eq!(app_row, vec![0usize, 1], "行式 appended 索引");
    assert_eq!(app_col, vec![0usize, 1], "列式 appended 索引");
}

/// gap-3 编译失败兜底：where 引用**缺失列**（门控 expr_is_columnar 放行但
/// 编译/求值空）→ 列式掩码全假 vs 行式 where_ok 读缺失 → 全拒，逐位一致。
#[test]
fn each_columnar_where_missing_column_rejects_all_parity() {
    let schema = Arc::new(Schema::new(vec![ArrowField::new(
        "sip",
        DataType::Utf8,
        true,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(StringArray::from(vec![Some("10.0.0.1"), Some("10.0.0.2")])) as ArrayRef],
    )
    .unwrap();
    let mut plan = simple_rule_plan(
        "gap3_missing_col",
        simple_plan(vec![], vec![]),
        Expr::Number(5.0),
        "ip",
        Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    // where 引用批里没有的列 → 两路都读 None → 全拒（严格语义）。
    plan.r#where = Some(Expr::BinOp {
        op: BinOp::Eq,
        left: Box::new(Expr::Field(FieldRef::Qualified(
            "e".into(),
            "missing".into(),
        ))),
        right: Box::new(Expr::Number(10.0)),
    });
    let (_, app_row, app_col) = assert_each_columnar_where_matches_row(plan, &batch);
    assert_eq!(app_row, Vec::<usize>::new(), "缺失列 where → 全拒");
    assert_eq!(app_col, Vec::<usize>::new(), "缺失列 where → 全拒");
}

/// gap-3 形状矩阵：多种列式 where 形状（Utf8 Ne / Float Gt / 复合 And+Or 含
/// null 短路 / Not / Bool 字面量 / 算术比较（q14 形态）/ cidr_match /
/// startswith）与行式 where_ok 逐位对拍。
#[test]
fn each_columnar_where_shape_matrix_matches_row_path() {
    let schema = Arc::new(Schema::new(vec![
        ArrowField::new("sip", DataType::Utf8, true),
        ArrowField::new("price", DataType::Float64, true),
        ArrowField::new("category", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec![
                Some("10.0.0.1"),
                Some("10.0.0.2"),
                Some("10.0.0.3"),
                None,
                Some("192.168.1.1"),
            ])) as ArrayRef,
            Arc::new(Float64Array::from(vec![
                Some(5_000_000.0),
                Some(60_000_000.0),
                None,
                Some(100.0),
                Some(2_000_000.0),
            ])) as ArrayRef,
            Arc::new(Int64Array::from(vec![
                Some(10),
                Some(20),
                Some(30),
                None,
                Some(10),
            ])) as ArrayRef,
        ],
    )
    .unwrap();

    let ef = |n: &str| Expr::Field(FieldRef::Qualified("e".into(), n.into()));
    let cmp = |op: BinOp, l: Expr, r: Expr| Expr::BinOp {
        op,
        left: Box::new(l),
        right: Box::new(r),
    };
    // (形状名, where 表达式, 期望 appended 索引)。
    let cases: Vec<(&str, Expr, Vec<usize>)> = vec![
        (
            "utf8_ne",
            cmp(BinOp::Ne, ef("sip"), Expr::StringLit("10.0.0.3".into())),
            vec![0, 1, 4], // r2 相等拒；r3 null → 严格拒
        ),
        (
            "float_gt",
            cmp(BinOp::Gt, ef("price"), Expr::Number(100.5)),
            vec![0, 1, 4], // r2 null 拒；r3 100 <= 100.5 拒
        ),
        (
            "compound_and_or",
            cmp(
                BinOp::And,
                cmp(
                    BinOp::Or,
                    cmp(BinOp::Eq, ef("category"), Expr::Number(10.0)),
                    cmp(BinOp::Eq, ef("category"), Expr::Number(20.0)),
                ),
                cmp(BinOp::Ne, ef("sip"), Expr::StringLit("10.0.0.3".into())),
            ),
            vec![0, 1, 4], // r2: cat 30 + sip==3 → 拒；r3: cat null → Or null → 拒
        ),
        (
            "not",
            Expr::Not(Box::new(cmp(BinOp::Eq, ef("category"), Expr::Number(10.0)))),
            vec![1, 2], // r0/r4 cat=10 → Not true → false 拒；r3 cat null → Not null → 拒
        ),
        ("bool_lit", Expr::Bool(true), vec![0, 1, 2, 3, 4]),
        (
            "arith_cmp",
            cmp(
                BinOp::Gt,
                cmp(BinOp::Mul, ef("price"), Expr::Number(0.908)),
                Expr::Number(1_000_000.0),
            ),
            vec![0, 1, 4], // r2 null 拒；r3 100*0.908 拒
        ),
        (
            "cidr_match",
            Expr::FuncCall {
                qualifier: None,
                name: "cidr_match".into(),
                args: vec![ef("sip"), Expr::StringLit("10.0.0.0/8".into())],
            },
            vec![0, 1, 2], // r3 sip null → null 拒；r4 192.168 拒
        ),
        (
            "startswith",
            Expr::FuncCall {
                qualifier: None,
                name: "startswith".into(),
                args: vec![ef("sip"), Expr::StringLit("10.0.0.".into())],
            },
            vec![0, 1, 2],
        ),
    ];
    for (name, where_expr, expect) in cases {
        let mut plan = simple_rule_plan(
            &format!("gap3_{name}"),
            simple_plan(vec![], vec![]),
            Expr::Number(5.0),
            "ip",
            ef("sip"),
        );
        plan.binds[0].alias = "e".into();
        plan.each_plan = Some(EachPlan {
            alias: "e".into(),
            filter: None,
        });
        plan.r#where = Some(where_expr);
        plan.yield_plan.fields = vec![YieldField {
            name: "sip_out".into(),
            value: ef("sip"),
        }];
        let (_, app_row, app_col) = assert_each_columnar_where_matches_row(plan, &batch);
        assert_eq!(app_row, expect, "{name}: 行式 appended 索引");
        assert_eq!(app_col, expect, "{name}: 列式 appended 索引");
    }
}

/// gap-3 pipe 路径：each→pipe + 列式 where，行式（execute_each_with_joins
/// 记录路径）vs 列式（execute_each_pipe_batch_columnar → Vec<PipeEachRow>）
/// entity 顺序/计数一致。
#[test]
fn each_columnar_pipe_where_matches_row_path() {
    use crate::match_engine::executor::PipeEachRow;
    let schema = Arc::new(Schema::new(vec![
        ArrowField::new("sip", DataType::Utf8, true),
        ArrowField::new("category", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec![
                Some("10.0.0.1"),
                Some("10.0.0.2"),
                Some("10.0.0.3"),
                None,
            ])) as ArrayRef,
            Arc::new(Int64Array::from(vec![
                Some(10),
                Some(20),
                Some(10),
                Some(10),
            ])) as ArrayRef,
        ],
    )
    .unwrap();
    let mut plan = simple_rule_plan(
        "gap3_pipe",
        simple_plan(vec![], vec![]),
        Expr::Number(5.0),
        "ip",
        Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    plan.r#where = Some(Expr::BinOp {
        op: BinOp::Eq,
        left: Box::new(Expr::Field(FieldRef::Qualified(
            "e".into(),
            "category".into(),
        ))),
        right: Box::new(Expr::Number(10.0)),
    });
    plan.yield_plan = YieldPlan {
        target: "pipe_win".into(),
        version: None,
        fields: vec![YieldField {
            name: "sip_out".into(),
            value: Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
        }],
    };
    // 死 Snapshot join（checker 要求 where 有 join；where 只读驱动列 → 死）。
    plan.joins.push(JoinPlan {
        right_window: "person_events".into(),
        mode: JoinMode::Snapshot,
        conds: vec![JoinCondPlan {
            left: FieldRef::Qualified("e".into(), "id".into()),
            right: FieldRef::Qualified("person_events".into(), "id".into()),
        }],
        within: None,
        reduce: None,
        emit_at: None,
    });
    let exec = RuleExecutor::new(plan);
    assert!(
        exec.each_pipe_columnar_safe(),
        "gap-3 pipe：each→pipe + 列式 where 必须放行"
    );
    let t = 1_700_000_000_000_000_000i64;
    // 行式：逐事件 execute_each_with_joins（记录路径，where 生效）。
    let events = crate::match_engine::event_bridge::batch_to_events(&batch);
    let row_entities: Vec<String> = events
        .iter()
        .filter_map(|e| {
            exec.execute_each_with_joins(e, t, &EmptyLookup, &[], 0)
                .unwrap()
                .map(|rec| rec.entity_id)
        })
        .collect();
    // 列式：prepared + sink。
    let prepared = exec.each_batch_prepare(&batch);
    let col_events: Vec<ColumnarEvent> = (0..batch.num_rows())
        .map(|r| ColumnarEvent::new(&batch, r))
        .collect();
    let col_refs: Vec<(&ColumnarEvent, i64)> = col_events.iter().map(|ev| (ev, t)).collect();
    let mut sink: Vec<PipeEachRow> = Vec::new();
    let sc = exec.execute_each_pipe_batch_columnar(&col_refs, &prepared, &mut sink);
    let col_entities: Vec<&str> = sink.iter().map(|r| r.entity_id.as_str()).collect();
    // where category==10 → r0/r2 过；r1(20) 拒；r3(sip null 但 category 10) 过。
    assert_eq!(
        row_entities,
        vec!["10.0.0.1", "10.0.0.3", ""],
        "行式 entity 顺序"
    );
    assert_eq!(sc.appended, 3, "列式 appended");
    assert_eq!(sc.rejected, 1, "列式 rejected（r1）");
    assert_eq!(
        col_entities,
        vec!["10.0.0.1", "10.0.0.3", ""],
        "列式 entity 顺序"
    );
}

/// gap-4 gate 拒绝：InList / strftime 不在守卫列式清单（expr_is_columnar
/// false）→ 保持行式（这些形状行式 where_ok 语义不变）。
#[test]
fn each_columnar_where_inlist_strftime_stay_row_path() {
    let ef = |n: &str| Expr::Field(FieldRef::Qualified("e".into(), n.into()));
    let where_exprs: Vec<Expr> = vec![
        // InList（q15/q16/q17 形态）——守卫门控不列式。
        Expr::InList {
            expr: Box::new(ef("category")),
            list: vec![Expr::Number(10.0), Expr::Number(20.0)],
            negated: false,
        },
        // strftime 函数比较——守卫门控不列式。
        Expr::BinOp {
            op: BinOp::Eq,
            left: Box::new(Expr::FuncCall {
                qualifier: None,
                name: "strftime".into(),
                args: vec![ef("ts"), Expr::StringLit("%H".into())],
            }),
            right: Box::new(Expr::StringLit("12".into())),
        },
    ];
    for (i, where_expr) in where_exprs.into_iter().enumerate() {
        let mut plan = simple_rule_plan(
            &format!("gap3_reject_{i}"),
            simple_plan(vec![], vec![]),
            Expr::Number(5.0),
            "ip",
            ef("sip"),
        );
        plan.binds[0].alias = "e".into();
        plan.each_plan = Some(EachPlan {
            alias: "e".into(),
            filter: None,
        });
        plan.r#where = Some(where_expr);
        assert!(
            !RuleExecutor::new(plan).each_plan_columnar_safe(),
            "InList/strftime where 必须保持行式（{i}）"
        );
    }
}

// ---------------------------------------------------------------------------
// P4 gap-5（2026-09-02）：无活 join 的 list-index 输出字段（`c.tags[0]`）
// 列式化——编译 ListIndex cvec（Field 快通道只读 flat 列）vs 行式 Path
// 数组下标逐位对拍（含 null / 空列表 / 越界行 / null-drop）。
// ---------------------------------------------------------------------------

/// gap-5 对拍夹具：同一批上执行行式/列式 each 直接批路径，yield 唯一字段
/// `tags[i]`（alias=e，列名 tag0，Chars），返回 (行式输出, 列式输出)。
/// 内部断言两条路径逐位一致 + appended/rejected 相同。
fn gap5_list_index_parity(batch: &RecordBatch, index: usize) -> (Vec<DataRecord>, Vec<DataRecord>) {
    let mut plan = simple_rule_plan(
        "gap5_list_index",
        simple_plan(vec![], vec![]),
        Expr::Number(5.0),
        "ip",
        Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    plan.yield_plan.fields = vec![YieldField {
        name: "tag0".into(),
        value: Expr::Field(FieldRef::Path {
            alias: "e".into(),
            segments: vec![PathSegment::Field("tags".into()), PathSegment::Index(index)],
        }),
    }];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([("tag0".into(), FieldType::Base(BaseType::Chars))]),
    );
    assert!(
        exec.each_plan_columnar_safe(),
        "gap-5：无活 join + list-index 输出字段必须放行"
    );
    let t = 1_700_000_000_000_000_000i64;
    let events = crate::match_engine::event_bridge::batch_to_events(batch);
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
        .map(|r| ColumnarEvent::new(batch, r))
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

    assert_eq!(sr.appended, out_row.len(), "行式 appended");
    assert_eq!(sc.appended, out_col.len(), "列式 appended");
    assert_eq!(sr.rejected, sc.rejected, "拒绝数一致");
    assert_eq!(app_row, app_col, "appended 索引一致");
    assert_eq!(out_row, out_col, "list-index yield 输出逐位对拍");
    (out_row, out_col)
}

/// 读输出记录的 `tag0` 标签（Char 字段值或空串）。
fn gap5_tag0_label(r: &DataRecord) -> String {
    r.fields()
        .find(|f| f.get_name() == "tag0")
        .and_then(|f| match f.get_value() {
            wp_model_core::model::Value::Chars(v) => Some(v.to_string()),
            _ => None,
        })
        .expect("tag0 field")
}

/// gap-6/7 夹具：构造 on-each 规则（绑定 e、无 filter/where/let/yield）并返回
/// executor——score/entity 由参数给出。
fn gap67_plan(rule: &str, score: Expr, entity: Expr) -> RuleExecutor {
    let mut plan = simple_rule_plan(rule, simple_plan(vec![], vec![]), score, "ip", entity);
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    RuleExecutor::new(plan)
}

/// gap-6/7 对拍夹具：同一批上执行行式/列式 each 直接批路径，断言
/// appended/rejected/failed 与输出逐位一致，返回 (行式输出, 列式输出)。
fn gap67_parity(exec: &RuleExecutor, batch: &RecordBatch) -> (Vec<DataRecord>, Vec<DataRecord>) {
    let t = 1_700_000_000_000_000_000i64;
    let events = crate::match_engine::event_bridge::batch_to_events(batch);
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
        .map(|r| ColumnarEvent::new(batch, r))
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

    assert_eq!(sr.appended, sc.appended, "appended 一致");
    assert_eq!(sr.rejected, sc.rejected, "rejected 一致");
    assert_eq!(sr.failed, sc.failed, "failed 一致");
    assert_eq!(app_row, app_col, "appended 索引一致");
    assert_eq!(out_row, out_col, "gap-6/7 输出逐位对拍");
    (out_row, out_col)
}

/// 读输出记录的 score（Float）。
fn gap67_score(r: &DataRecord) -> f64 {
    r.fields()
        .find(|f| f.get_name() == wf_lang::wfu_meta::WFU_SCORE)
        .and_then(|f| match f.get_value() {
            wp_model_core::model::Value::Float(v) => Some(*v),
            _ => None,
        })
        .expect("score field")
}

/// 读输出记录的 entity_id（Char）。
fn gap67_entity(r: &DataRecord) -> String {
    r.fields()
        .find(|f| f.get_name() == wf_lang::wfu_meta::WFU_ENTITY_ID)
        .and_then(|f| match f.get_value() {
            wp_model_core::model::Value::Chars(v) => Some(v.to_string()),
            _ => None,
        })
        .expect("entity_id field")
}

#[test]
fn each_columnar_list_index_yield_matches_row_path() {
    use arrow::array::ListArray;
    use arrow::buffer::OffsetBuffer;
    // tags: 原生 List<Utf8>——行 0 = ["prod","edge"]；1 = [null]；2 = []；
    // 3 = ["dmz"]。
    let values = StringArray::from(vec![Some("prod"), Some("edge"), None, Some("dmz")]);
    let tags = ListArray::try_new(
        Arc::new(ArrowField::new("item", DataType::Utf8, true)),
        OffsetBuffer::new(vec![0i32, 2, 3, 3, 4].into()),
        Arc::new(values) as ArrayRef,
        None,
    )
    .unwrap();
    let schema = Arc::new(Schema::new(vec![
        ArrowField::new(
            "tags",
            DataType::List(Arc::new(ArrowField::new("item", DataType::Utf8, true))),
            true,
        ),
        ArrowField::new("sip", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(tags) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some("a"),
                Some("b"),
                Some("c"),
                Some("d"),
            ])) as ArrayRef,
        ],
    )
    .unwrap();
    let (_, out_col) = gap5_list_index_parity(&batch, 0);
    // tags[0]：行 0 → "prod"；行 1（[null]）→ null → 空串；行 2（[]）→ 越界 →
    // 空串；行 3 → "dmz"。全部 append（无 filter 拒绝）。
    assert_eq!(gap5_tag0_label(&out_col[0]), "prod");
    assert_eq!(gap5_tag0_label(&out_col[1]), "", "[null] 行索引 0 → 空串");
    assert_eq!(gap5_tag0_label(&out_col[2]), "", "空列表越界 → 空串");
    assert_eq!(gap5_tag0_label(&out_col[3]), "dmz");
}

#[test]
fn each_columnar_list_index_json_array_yield_matches_row_path() {
    // tags: JsonArray-metadata Utf8（`wf.wfl.field_type = "array"`——qradar 帧
    // 的 array/… 字段真实存储形态）——行 0 = ["a",null,"b"]（null-drop 探针：
    // 解释器 json_to_value 丢弃 null 元素，index 1 → "b"）；1 = ["edge"]；
    // 2 = []；3 = null cell。
    let schema = Arc::new(Schema::new(vec![
        ArrowField::new("tags", DataType::Utf8, true).with_metadata(
            std::collections::HashMap::from([(
                crate::match_engine::WFL_FIELD_TYPE_METADATA_KEY.to_string(),
                crate::match_engine::WFL_FIELD_TYPE_ARRAY.to_string(),
            )]),
        ),
        ArrowField::new("sip", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec![
                Some(r#"["a",null,"b"]"#),
                Some(r#"["edge"]"#),
                Some("[]"),
                None,
            ])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some("a"),
                Some("b"),
                Some("c"),
                Some("d"),
            ])) as ArrayRef,
        ],
    )
    .unwrap();
    let (_, out_col) = gap5_list_index_parity(&batch, 1);
    assert_eq!(
        gap5_tag0_label(&out_col[0]),
        "b",
        "null 元素被丢弃，[1] → b"
    );
    assert_eq!(
        gap5_tag0_label(&out_col[0]),
        "b",
        "null 元素被丢弃，[1] → b"
    );
    assert_eq!(gap5_tag0_label(&out_col[1]), "", "越界 → 空串");
    assert_eq!(gap5_tag0_label(&out_col[2]), "", "空数组越界 → 空串");
    assert_eq!(gap5_tag0_label(&out_col[3]), "", "null cell → 空串");
}

// ---------------------------------------------------------------------------
// P4 gap-6/7（2026-09-02）：score 非「常量 | 常量×flat」/ entity 非字面量 /
// flat 的可列式表达式 → 批级 score_cvec / entity_cvec（编译失败/读结构化列
// 逐行 eval_score / eval_entity_id 回退）——行式/列式逐位对拍。
// ---------------------------------------------------------------------------

#[test]
fn each_columnar_general_score_matches_row_path() {
    // gap-6：score = 字段×字段（`e.a * e.b`）→ 批级 score_cvec。
    // 语义抽查：2×3=6；10×10=100（clamp 上限）；-5×10=-50 → 0（clamp 下限）；
    // 100×2=200 → 100；b=null → 整行 failed（非数值与行式 eval_score 一致）。
    let schema = Arc::new(Schema::new(vec![
        ArrowField::new("id", DataType::Int64, true),
        ArrowField::new("a", DataType::Int64, true),
        ArrowField::new("b", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                Some(4),
                Some(5),
            ])) as ArrayRef,
            Arc::new(Int64Array::from(vec![
                Some(2),
                Some(10),
                Some(-5),
                Some(100),
                Some(7),
            ])) as ArrayRef,
            Arc::new(Int64Array::from(vec![
                Some(3),
                Some(10),
                Some(10),
                Some(2),
                None,
            ])) as ArrayRef,
        ],
    )
    .unwrap();
    let exec = gap67_plan(
        "gap6_score_fxf",
        Expr::BinOp {
            op: BinOp::Mul,
            left: Box::new(Expr::Field(FieldRef::Qualified("e".into(), "a".into()))),
            right: Box::new(Expr::Field(FieldRef::Qualified("e".into(), "b".into()))),
        },
        Expr::Field(FieldRef::Qualified("e".into(), "id".into())),
    );
    assert!(
        exec.each_plan_columnar_safe(),
        "gap-6：字段×字段 score 必须放行"
    );
    let (_, out_col) = gap67_parity(&exec, &batch);
    assert_eq!(out_col.len(), 4, "null 操作数行被 failed 丢弃");
    assert_eq!(gap67_score(&out_col[0]), 6.0);
    assert_eq!(gap67_score(&out_col[1]), 100.0);
    assert_eq!(gap67_score(&out_col[2]), 0.0);
    assert_eq!(gap67_score(&out_col[3]), 100.0);
}

#[test]
fn each_columnar_general_score_null_and_bad_cells_matches_row_path() {
    // gap-6 边界：裸 flat 字段 score（null cell → failed 行）；非数值列
    // （Utf8）score → 全行 failed；引用批 schema 外字段 → 编译槽位全 null →
    // 全行 failed（行式 eval_score 同语义）。
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            ArrowField::new("id", DataType::Int64, true),
            ArrowField::new("v", DataType::Int64, true),
            ArrowField::new("label", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3)])) as ArrayRef,
            Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("x"), Some("y"), Some("z")])) as ArrayRef,
        ],
    )
    .unwrap();
    // 1) 裸 flat 字段 score(`e.v`)：null 行 failed。
    let exec = gap67_plan(
        "gap6_score_bare",
        Expr::Field(FieldRef::Qualified("e".into(), "v".into())),
        Expr::Field(FieldRef::Qualified("e".into(), "id".into())),
    );
    assert!(exec.each_plan_columnar_safe());
    let (_, out_col) = gap67_parity(&exec, &batch);
    assert_eq!(out_col.len(), 2, "null score cell → failed 行");
    assert_eq!(gap67_score(&out_col[0]), 1.0);
    assert_eq!(gap67_score(&out_col[1]), 3.0);

    // 2) 非数值列（Utf8）score(`e.label`) → 全行 failed。
    let exec = gap67_plan(
        "gap6_score_utf8",
        Expr::Field(FieldRef::Qualified("e".into(), "label".into())),
        Expr::Field(FieldRef::Qualified("e".into(), "id".into())),
    );
    assert!(exec.each_plan_columnar_safe());
    let (out_row, out_col) = gap67_parity(&exec, &batch);
    assert!(
        out_row.is_empty() && out_col.is_empty(),
        "非数值 score 全行 failed"
    );

    // 3) score 引用 schema 外字段（`e.missing`）→ 编译槽位全 null → 全行
    // failed（列式 == 行式 eval_score 的 Err 语义）。
    let exec = gap67_plan(
        "gap6_score_missing",
        Expr::Field(FieldRef::Qualified("e".into(), "missing".into())),
        Expr::Field(FieldRef::Qualified("e".into(), "id".into())),
    );
    assert!(exec.each_plan_columnar_safe());
    let (out_row, out_col) = gap67_parity(&exec, &batch);
    assert!(
        out_row.is_empty() && out_col.is_empty(),
        "缺失字段 score 全行 failed"
    );
}

#[test]
fn each_columnar_general_entity_matches_row_path() {
    use arrow::array::ListArray;
    use arrow::buffer::OffsetBuffer;
    // gap-7：entity = 可列式表达式——list-index（`e.tags[0]`，原生 List）
    // 与复合 Add（`e.a + e.b`，数字渲染 number_to_string）。
    // 1) list-index entity：行 0 = ["prod","edge"] → "prod"；1 = [null] → ""；
    //    2 = [] → ""；3 = ["dmz"] → "dmz"——全部 append（entity 不拒绝行）。
    let values = StringArray::from(vec![Some("prod"), Some("edge"), None, Some("dmz")]);
    let tags = ListArray::try_new(
        Arc::new(ArrowField::new("item", DataType::Utf8, true)),
        OffsetBuffer::new(vec![0i32, 2, 3, 3, 4].into()),
        Arc::new(values) as ArrayRef,
        None,
    )
    .unwrap();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            ArrowField::new(
                "tags",
                DataType::List(Arc::new(ArrowField::new("item", DataType::Utf8, true))),
                true,
            ),
            ArrowField::new("id", DataType::Int64, true),
        ])),
        vec![
            Arc::new(tags) as ArrayRef,
            Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3), Some(4)])) as ArrayRef,
        ],
    )
    .unwrap();
    let exec = gap67_plan(
        "gap7_entity_list_index",
        Expr::Number(50.0),
        Expr::Field(FieldRef::Path {
            alias: "e".into(),
            segments: vec![PathSegment::Field("tags".into()), PathSegment::Index(0)],
        }),
    );
    assert!(
        exec.each_plan_columnar_safe(),
        "gap-7：list-index entity 必须放行"
    );
    let (_, out_col) = gap67_parity(&exec, &batch);
    assert_eq!(out_col.len(), 4);
    assert_eq!(gap67_entity(&out_col[0]), "prod");
    assert_eq!(gap67_entity(&out_col[1]), "", "[null] 元素 → 空串");
    assert_eq!(gap67_entity(&out_col[2]), "", "空列表越界 → 空串");
    assert_eq!(gap67_entity(&out_col[3]), "dmz");

    // 2) 复合 Add entity（`e.a + e.b`）→ number_to_string 渲染。
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            ArrowField::new("id", DataType::Int64, true),
            ArrowField::new("a", DataType::Int64, true),
            ArrowField::new("b", DataType::Int64, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3)])) as ArrayRef,
            Arc::new(Int64Array::from(vec![Some(2), Some(20), Some(5)])) as ArrayRef,
            Arc::new(Int64Array::from(vec![Some(3), Some(2), Some(-5)])) as ArrayRef,
        ],
    )
    .unwrap();
    let exec = gap67_plan(
        "gap7_entity_add",
        Expr::Number(50.0),
        Expr::BinOp {
            op: BinOp::Add,
            left: Box::new(Expr::Field(FieldRef::Qualified("e".into(), "a".into()))),
            right: Box::new(Expr::Field(FieldRef::Qualified("e".into(), "b".into()))),
        },
    );
    assert!(exec.each_plan_columnar_safe());
    let (_, out_col) = gap67_parity(&exec, &batch);
    assert_eq!(out_col.len(), 3);
    assert_eq!(gap67_entity(&out_col[0]), "5");
    assert_eq!(gap67_entity(&out_col[1]), "22");
    assert_eq!(gap67_entity(&out_col[2]), "0");
}

#[test]
fn each_columnar_score_const_times_list_index_matches_row_path() {
    use arrow::array::ListArray;
    use arrow::buffer::OffsetBuffer;
    // gap-6 review 补测（2026-09-02）：score = 常量×list-index 字段
    // （`2.0 * e.tags[0]`，原生 List<Int64>）——MulConst 快通道 value_at 只读
    // flat 列，索引元素归一般 cvec（ListIndex × 常量）。clamp + 空列表（越界
    // → failed 行）。
    let values = Int64Array::from(vec![Some(3), Some(50), Some(1000)]);
    let tags = ListArray::try_new(
        Arc::new(ArrowField::new("item", DataType::Int64, true)),
        OffsetBuffer::new(vec![0i32, 1, 2, 3, 3].into()),
        Arc::new(values) as ArrayRef,
        None,
    )
    .unwrap();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            ArrowField::new(
                "tags",
                DataType::List(Arc::new(ArrowField::new("item", DataType::Int64, true))),
                true,
            ),
            ArrowField::new("id", DataType::Int64, true),
        ])),
        vec![
            Arc::new(tags) as ArrayRef,
            Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3), Some(4)])) as ArrayRef,
        ],
    )
    .unwrap();
    let exec = gap67_plan(
        "gap6_score_const_x_list_index",
        Expr::BinOp {
            op: BinOp::Mul,
            left: Box::new(Expr::Number(2.0)),
            right: Box::new(Expr::Field(FieldRef::Path {
                alias: "e".into(),
                segments: vec![PathSegment::Field("tags".into()), PathSegment::Index(0)],
            })),
        },
        Expr::Field(FieldRef::Qualified("e".into(), "id".into())),
    );
    assert!(
        exec.each_plan_columnar_safe(),
        "常量×list-index score 必须放行（归一般 cvec）"
    );
    let (_, out_col) = gap67_parity(&exec, &batch);
    assert_eq!(out_col.len(), 3, "空列表行 failed 丢弃");
    assert_eq!(gap67_score(&out_col[0]), 6.0);
    assert_eq!(gap67_score(&out_col[1]), 100.0, "2×50 → 100");
    assert_eq!(gap67_score(&out_col[2]), 100.0, "2×1000 → 2000 clamp 100");
}

#[test]
fn each_columnar_entity_list_index_json_array_fallback_matches_row_path() {
    // gap-7 review 补测：list-index entity 读 **JsonArray-metadata** Utf8 列
    // （qradar 帧 array/… 形态）→ arg_reads_structured → entity_cvec = None →
    // 逐行 eval_entity_id 回退（行式语义，null-drop/越界/null cell → 空串）。
    let schema = Arc::new(Schema::new(vec![
        ArrowField::new("tags", DataType::Utf8, true).with_metadata(
            std::collections::HashMap::from([(
                crate::match_engine::WFL_FIELD_TYPE_METADATA_KEY.to_string(),
                crate::match_engine::WFL_FIELD_TYPE_ARRAY.to_string(),
            )]),
        ),
        ArrowField::new("id", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec![
                Some(r#"["prod","edge"]"#),
                Some(r#"["x"]"#),
                Some("[]"),
                None,
            ])) as ArrayRef,
            Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3), Some(4)])) as ArrayRef,
        ],
    )
    .unwrap();
    let exec = gap67_plan(
        "gap7_entity_json_array",
        Expr::Number(50.0),
        Expr::Field(FieldRef::Path {
            alias: "e".into(),
            segments: vec![PathSegment::Field("tags".into()), PathSegment::Index(0)],
        }),
    );
    assert!(exec.each_plan_columnar_safe());
    let (_, out_col) = gap67_parity(&exec, &batch);
    assert_eq!(out_col.len(), 4, "entity 不拒行（空串也 append）");
    assert_eq!(gap67_entity(&out_col[0]), "prod");
    assert_eq!(gap67_entity(&out_col[1]), "x");
    assert_eq!(gap67_entity(&out_col[2]), "", "空数组越界 → 空串");
    assert_eq!(gap67_entity(&out_col[3]), "", "null cell → 空串");
}

#[test]
fn each_columnar_general_score_nested_arith_matches_row_path() {
    // gap-6 review 补测：嵌套算数 score（`e.a * e.b + e.c`）→ 算数树编译 +
    // clamp——覆盖复合表达式（非单 BinOp）路径。
    let schema = Arc::new(Schema::new(vec![
        ArrowField::new("id", DataType::Int64, true),
        ArrowField::new("a", DataType::Int64, true),
        ArrowField::new("b", DataType::Int64, true),
        ArrowField::new("c", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3), Some(4)])) as ArrayRef,
            Arc::new(Int64Array::from(vec![
                Some(2),
                Some(10),
                Some(-5),
                Some(100),
            ])) as ArrayRef,
            Arc::new(Int64Array::from(vec![Some(3), Some(10), Some(2), Some(3)])) as ArrayRef,
            Arc::new(Int64Array::from(vec![Some(1), Some(0), Some(5), Some(2)])) as ArrayRef,
        ],
    )
    .unwrap();
    let exec = gap67_plan(
        "gap6_score_nested",
        Expr::BinOp {
            op: BinOp::Add,
            left: Box::new(Expr::BinOp {
                op: BinOp::Mul,
                left: Box::new(Expr::Field(FieldRef::Qualified("e".into(), "a".into()))),
                right: Box::new(Expr::Field(FieldRef::Qualified("e".into(), "b".into()))),
            }),
            right: Box::new(Expr::Field(FieldRef::Qualified("e".into(), "c".into()))),
        },
        Expr::Field(FieldRef::Qualified("e".into(), "id".into())),
    );
    assert!(exec.each_plan_columnar_safe());
    let (_, out_col) = gap67_parity(&exec, &batch);
    // 2*3+1=7；10*10+0=100（clamp 上限）；-5*2+5=-5 → 0（clamp 下限）；
    // 100*3+2=302 → 100。
    assert_eq!(out_col.len(), 4);
    assert_eq!(gap67_score(&out_col[0]), 7.0);
    assert_eq!(gap67_score(&out_col[1]), 100.0);
    assert_eq!(gap67_score(&out_col[2]), 0.0);
    assert_eq!(gap67_score(&out_col[3]), 100.0);
}
