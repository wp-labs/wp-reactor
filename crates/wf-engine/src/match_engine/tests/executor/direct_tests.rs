//! Plan C2 equivalence tests: the direct-write on-each emit
//! (`execute_each_direct` → `AlertColumnBuilder` staging) must produce
//! byte-equivalent rows to the record path
//! (`execute_each_with_joins` → `OutputRecord` → `append_record`).
use std::sync::Arc;

use std::collections::HashMap;

use wf_lang::ast::{BinOp, Expr, FieldRef, JoinMode};
use wf_lang::plan::{EachPlan, JoinCondPlan, JoinPlan, YieldField};
use wf_lang::{BaseType, FieldType};

use crate::alert::AlertColumnBuilder;
use crate::match_engine::match_engine::WindowLookup;
use crate::match_engine::{Event, RuleExecutor};

use super::super::helpers::*;

/// Empty lookup — the no-join plans used here never consult windows.
struct EmptyLookup;

impl WindowLookup for EmptyLookup {
    fn snapshot_field_values(
        &self,
        _window: &str,
        _field: &str,
    ) -> Option<std::collections::HashSet<String>> {
        None
    }
    fn snapshot(&self, _window: &str) -> Option<Vec<crate::match_engine::JoinRow>> {
        None
    }
    fn snapshot_with_timestamps(
        &self,
        _window: &str,
    ) -> Option<Vec<(i64, crate::match_engine::JoinRow)>> {
        None
    }
}

/// Snapshot join lookup stub: serves fixed right-window rows through the
/// trait's default `join_lookup` (snapshot scan + `values_equal`), which both
/// the row and columnar join paths consult — identical lookup semantics.
struct MockJoinLookup {
    rows: Vec<crate::match_engine::JoinRow>,
}

impl WindowLookup for MockJoinLookup {
    fn snapshot_field_values(
        &self,
        _window: &str,
        _field: &str,
    ) -> Option<std::collections::HashSet<String>> {
        None
    }
    fn snapshot(&self, _window: &str) -> Option<Vec<crate::match_engine::JoinRow>> {
        Some(self.rows.clone())
    }
    fn snapshot_with_timestamps(
        &self,
        _window: &str,
    ) -> Option<Vec<(i64, crate::match_engine::JoinRow)>> {
        None
    }
}

/// q20 形状的 each + Snapshot join + where 规则：`b.auction ==
/// auction_events.id`，where `auction_events.category == 10`，entity/yield 读
/// 左窗 `b.auction` + 右窗 `auction_events.category`。
fn each_join_plan_rule() -> RuleExecutor {
    let mut plan = simple_rule_plan(
        "q20_shape",
        simple_plan(vec![], vec![]),
        Expr::Number(10.0),
        "ip",
        Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
    );
    plan.binds[0].alias = "b".into();
    plan.each_plan = Some(EachPlan {
        alias: "b".into(),
        filter: None,
    });
    plan.joins = vec![JoinPlan {
        right_window: "auction_events".into(),
        mode: JoinMode::Snapshot,
        conds: vec![JoinCondPlan {
            left: FieldRef::Qualified("b".into(), "auction".into()),
            right: FieldRef::Qualified("auction_events".into(), "id".into()),
        }],
        within: None,
        reduce: None,
        emit_at: None,
    }];
    plan.r#where = Some(Expr::BinOp {
        op: BinOp::Eq,
        left: Box::new(Expr::Field(FieldRef::Qualified(
            "auction_events".into(),
            "category".into(),
        ))),
        right: Box::new(Expr::Number(10.0)),
    });
    plan.yield_plan.fields = vec![
        YieldField {
            name: "id".into(),
            value: Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
        },
        YieldField {
            name: "category".into(),
            value: Expr::Field(FieldRef::Qualified(
                "auction_events".into(),
                "category".into(),
            )),
        },
    ];
    RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([
            ("id".into(), FieldType::Base(BaseType::Digit)),
            ("category".into(), FieldType::Base(BaseType::Digit)),
        ]),
    )
}

/// q20 形状的驱动 batch：3 条 bid（auction=1/2/3）。右窗 auction 行：
/// id=1 category=10（命中）、id=2 category=20（where 拒绝）、id=3 category=10
/// （命中）。期望 appended=2 / rejected=1。
#[test]
fn execute_each_direct_batch_columnar_join_matches_event_path_rows() {
    use crate::match_engine::event_bridge::{ColumnarEvent, JoinRow, materialize_rows};
    use arrow::array::{ArrayRef, Float64Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    let exec = each_join_plan_rule();
    assert_eq!(
        exec.live_joins().len(),
        1,
        "where/yield 读右窗字段 → join 必须存活"
    );
    assert!(
        exec.each_join_columnar_ready(),
        "q20 形状必须解析出列式 join 计划"
    );
    assert!(exec.each_plan_columnar_safe(), "q20 形状必须列式安全");

    let lookup = MockJoinLookup {
        rows: vec![
            JoinRow::Event(Arc::new(event(vec![
                ("id", num(1.0)),
                ("category", num(10.0)),
            ]))),
            JoinRow::Event(Arc::new(event(vec![
                ("id", num(2.0)),
                ("category", num(20.0)),
            ]))),
            JoinRow::Event(Arc::new(event(vec![
                ("id", num(3.0)),
                ("category", num(10.0)),
            ]))),
        ],
    };

    const NANOS: i64 = 1_750_000_000_000_000_000;
    let schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, true),
        Field::new("bidder", DataType::Int64, true),
        Field::new("price", DataType::Float64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
            Arc::new(Int64Array::from(vec![100, 200, 300])),
            Arc::new(Float64Array::from(vec![10.5, 20.5, 30.5])),
        ],
    )
    .unwrap();

    // Reference：eager 物化 + Event 版 each+join 批路径（行式）。
    let events: Vec<Event> = materialize_rows(&batch, &[0, 1, 2]);
    let rows: Vec<(&Event, i64)> = events
        .iter()
        .enumerate()
        .map(|(i, ev)| (ev, NANOS + i as i64))
        .collect();
    let mut via_events = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut appended_idx = Vec::new();
    let stats = exec.execute_each_direct_batch(
        &rows,
        &lookup,
        &[],
        NANOS,
        &mut via_events,
        &mut appended_idx,
    );
    assert_eq!(stats.appended, 2, "行式：auction 1/3 命中且 category=10");
    assert_eq!(stats.rejected, 1, "行式：auction 2 where 拒绝");
    assert_eq!(stats.failed, 0);

    // 列式 join 路径。
    let col_events: Vec<ColumnarEvent<'_>> =
        (0..3).map(|r| ColumnarEvent::new(&batch, r)).collect();
    let col_rows: Vec<(&ColumnarEvent<'_>, i64)> = col_events
        .iter()
        .enumerate()
        .map(|(i, ev)| (ev, NANOS + i as i64))
        .collect();
    let mut via_columnar = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut appended_idx_c = Vec::new();
    let stats_c = exec.execute_each_direct_batch_columnar_join(
        &col_rows,
        &lookup,
        NANOS,
        &mut via_columnar,
        &mut appended_idx_c,
    );
    assert_eq!(stats_c, stats);
    assert_eq!(appended_idx_c, appended_idx);

    assert_batches_equal_rows(&via_events.finish(), &via_columnar.finish());
}

/// 列式 join 执行的补充语义测试（2026-08-23 review）：
/// 1. where 多谓词合取（`A == 1 && B == "x"`）；
/// 2. float 左 key（f64→Int 截断后桶内 values_equal 复核拒绝——1.5 不匹配 id=1）；
/// 3. 右窗字段 null → where 拒绝；
/// 4. 无 where + join miss → 输出该行（右窗 yield 字段空串）。
#[test]
fn columnar_join_semantics_edge_cases_match_event_path() {
    use crate::match_engine::event_bridge::{ColumnarEvent, JoinRow, materialize_rows};
    use arrow::array::{ArrayRef, Float64Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    // 规则：each + Snapshot join（b.auction == auction_events.id）+ where
    // `category == 10 && state == "CA"`（多谓词合取）+ yield 读右窗 category/state。
    let make_rule = |with_where: bool| {
        let mut plan = simple_rule_plan(
            "r",
            simple_plan(vec![], vec![]),
            Expr::Number(1.0),
            "ip",
            Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
        );
        plan.binds[0].alias = "b".into();
        plan.each_plan = Some(EachPlan {
            alias: "b".into(),
            filter: None,
        });
        plan.joins = vec![JoinPlan {
            right_window: "auction_events".into(),
            mode: JoinMode::Snapshot,
            conds: vec![JoinCondPlan {
                left: FieldRef::Qualified("b".into(), "auction".into()),
                right: FieldRef::Qualified("auction_events".into(), "id".into()),
            }],
            within: None,
            reduce: None,
            emit_at: None,
        }];
        if with_where {
            plan.r#where = Some(Expr::BinOp {
                op: BinOp::And,
                left: Box::new(Expr::BinOp {
                    op: BinOp::Eq,
                    left: Box::new(Expr::Field(FieldRef::Qualified(
                        "auction_events".into(),
                        "category".into(),
                    ))),
                    right: Box::new(Expr::Number(10.0)),
                }),
                right: Box::new(Expr::BinOp {
                    op: BinOp::Eq,
                    left: Box::new(Expr::Field(FieldRef::Qualified(
                        "auction_events".into(),
                        "state".into(),
                    ))),
                    right: Box::new(Expr::StringLit("CA".into())),
                }),
            });
        }
        plan.yield_plan.fields = vec![
            YieldField {
                name: "category".into(),
                value: Expr::Field(FieldRef::Qualified(
                    "auction_events".into(),
                    "category".into(),
                )),
            },
            YieldField {
                name: "state".into(),
                value: Expr::Field(FieldRef::Qualified("auction_events".into(), "state".into())),
            },
        ];
        RuleExecutor::new_with_yield_field_types(
            plan,
            HashMap::from([
                ("category".into(), FieldType::Base(BaseType::Digit)),
                ("state".into(), FieldType::Base(BaseType::Chars)),
            ]),
        )
    };

    // 右窗：id=1（cat=10, state=CA）、id=2（cat=20, state=CA）、id=3
    // （cat=10, state=null——null 右窗字段 → where state == "CA" 拒绝）。
    let rows_auc = vec![
        JoinRow::Event(Arc::new(event(vec![
            ("id", num(1.0)),
            ("category", num(10.0)),
            ("state", str_val("CA")),
        ]))),
        JoinRow::Event(Arc::new(event(vec![
            ("id", num(2.0)),
            ("category", num(20.0)),
            ("state", str_val("CA")),
        ]))),
        JoinRow::Event(Arc::new(event(vec![
            ("id", num(3.0)),
            ("category", num(10.0)),
        ]))), // state 缺失 = null
    ];
    let lookup = MockJoinLookup { rows: rows_auc };

    let run_both = |exec: &RuleExecutor, batch: &RecordBatch, expect: (usize, usize)| {
        const NANOS: i64 = 1_750_000_000_000_000_000;
        let all: Vec<u32> = (0..batch.num_rows() as u32).collect();
        let events: Vec<Event> = materialize_rows(batch, &all);
        let rows: Vec<(&Event, i64)> = events
            .iter()
            .enumerate()
            .map(|(i, ev)| (ev, NANOS + i as i64))
            .collect();
        let mut b1 = AlertColumnBuilder::new(Arc::from("alerts"));
        let mut idx1 = Vec::new();
        let s1 = exec.execute_each_direct_batch(&rows, &lookup, &[], NANOS, &mut b1, &mut idx1);

        let col: Vec<ColumnarEvent<'_>> = (0..batch.num_rows())
            .map(|r| ColumnarEvent::new(batch, r))
            .collect();
        let crows: Vec<(&ColumnarEvent<'_>, i64)> = col
            .iter()
            .enumerate()
            .map(|(i, ev)| (ev, NANOS + i as i64))
            .collect();
        let mut b2 = AlertColumnBuilder::new(Arc::from("alerts"));
        let mut idx2 = Vec::new();
        let s2 = exec
            .execute_each_direct_batch_columnar_join(&crows, &lookup, NANOS, &mut b2, &mut idx2);
        assert_eq!(s1, s2, "row={s1:?} col={s2:?}");
        assert_eq!(
            (s1.appended, s1.rejected),
            expect,
            "row path 期望 (appended, rejected)"
        );
        assert_eq!(idx1, idx2);
        assert_batches_equal_rows(&b1.finish(), &b2.finish());
    };

    // -- 场景 1：Int64 左 key + 多谓词 where --
    // bid auction=1（命中且双谓词过）、auction=2（cat≠10 拒绝）、auction=3
    // （cat=10 但 state null → 拒绝）、auction=99（miss → 拒绝）。
    let schema = Arc::new(Schema::new(vec![Field::new(
        "auction",
        DataType::Int64,
        true,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![1, 2, 3, 99])) as ArrayRef],
    )
    .unwrap();
    let exec = make_rule(true);
    assert!(exec.each_plan_columnar_safe());
    run_both(&exec, &batch, (1, 3));

    // -- 场景 2：float 左 key（f64→Int 截断假匹配，复核拒绝）--
    // auction=1.5 → JoinKey::Int(1) → 桶 id=1 → values_equal(1.5, 1) = false
    // → miss（行式 find_matching_row 同样拒绝）→ where 拒绝。
    let schema = Arc::new(Schema::new(vec![Field::new(
        "auction",
        DataType::Float64,
        true,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Float64Array::from(vec![1.5, 2.0])) as ArrayRef],
    )
    .unwrap();
    run_both(&exec, &batch, (0, 2));

    // -- 场景 3：无 where + miss 行 → 输出（右窗 yield 空串）--
    // 无 where 时 join miss 保留事件输出（Snapshot 语义），右窗 category/state
    // 读不到 → 空串。行式/列式必须一致。
    let exec_nowhere = make_rule(false);
    assert!(exec_nowhere.each_plan_columnar_safe());
    let schema = Arc::new(Schema::new(vec![Field::new(
        "auction",
        DataType::Int64,
        true,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![1, 99])) as ArrayRef],
    )
    .unwrap();
    run_both(&exec_nowhere, &batch, (2, 0));
}

/// 列式 join gate 分支：形状不支持 → 回退行式（each_plan_columnar_safe=false）。
#[test]
fn each_join_columnar_gate_rejects_unsupported_shapes() {
    let base = || {
        let mut plan = simple_rule_plan(
            "r",
            simple_plan(vec![], vec![]),
            Expr::Number(1.0),
            "ip",
            Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
        );
        plan.binds[0].alias = "b".into();
        plan.each_plan = Some(EachPlan {
            alias: "b".into(),
            filter: None,
        });
        plan.joins = vec![JoinPlan {
            right_window: "auction_events".into(),
            mode: JoinMode::Snapshot,
            conds: vec![JoinCondPlan {
                left: FieldRef::Qualified("b".into(), "auction".into()),
                right: FieldRef::Qualified("auction_events".into(), "id".into()),
            }],
            within: None,
            reduce: None,
            emit_at: None,
        }];
        plan
    };

    // where 引用**左窗**字段（非右窗）→ 不支持（列式 where 只吃右窗字段）。
    let mut plan = base();
    plan.r#where = Some(Expr::BinOp {
        op: BinOp::Gt,
        left: Box::new(Expr::Field(FieldRef::Qualified("b".into(), "price".into()))),
        right: Box::new(Expr::Number(5.0)),
    });
    assert!(!RuleExecutor::new(plan).each_plan_columnar_safe());

    // where 为复合表达式（函数）→ 不支持。
    let mut plan = base();
    plan.r#where = Some(Expr::FuncCall {
        qualifier: None,
        name: "upper".into(),
        args: vec![Expr::Field(FieldRef::Qualified(
            "auction_events".into(),
            "category".into(),
        ))],
    });
    assert!(!RuleExecutor::new(plan).each_plan_columnar_safe());

    // 多条件 join → 不支持（需让 join 存活：yield 读右窗字段，否则被死 join 消除）。
    let mut plan = base();
    plan.joins[0].conds.push(JoinCondPlan {
        left: FieldRef::Qualified("b".into(), "bidder".into()),
        right: FieldRef::Qualified("auction_events".into(), "seller".into()),
    });
    plan.yield_plan.fields = vec![YieldField {
        name: "cat".into(),
        value: Expr::Field(FieldRef::Qualified(
            "auction_events".into(),
            "category".into(),
        )),
    }];
    assert!(!RuleExecutor::new(plan).each_plan_columnar_safe());

    // Asof 模式 → 不支持（v1 仅 Snapshot；同样让 join 存活）。
    let mut plan = base();
    plan.joins[0].mode = JoinMode::Asof { within: None };
    plan.yield_plan.fields = vec![YieldField {
        name: "cat".into(),
        value: Expr::Field(FieldRef::Qualified(
            "auction_events".into(),
            "category".into(),
        )),
    }];
    assert!(!RuleExecutor::new(plan).each_plan_columnar_safe());

    // yield 读 Simple（裸名）字段 → 不支持（列式无法分辨裸名来源）。
    let mut plan = base();
    plan.yield_plan.fields = vec![YieldField {
        name: "cat".into(),
        value: Expr::Field(FieldRef::Simple("category".into())),
    }];
    assert!(!RuleExecutor::new(plan).each_plan_columnar_safe());

    // 无 where + 输出只读左窗（q13 死 join 消除后 live_joins 空，不适用）——
    // 这里 join 存活（无 where 也需右窗字段读才存活）；全左窗输出 → join 死。
    let plan = base();
    let exec = RuleExecutor::new(plan);
    // 无 where、yield/entity 全读左窗 → 死 join 消除 → live_joins 空 → 无 join 列式路径。
    assert!(
        exec.live_joins().is_empty(),
        "全左窗输出的 Snapshot join 必须被死 join 消除"
    );
    assert!(
        exec.each_plan_columnar_safe(),
        "死 join 消除后无 join 列式安全"
    );

    // 1 死 1 活 join：活 join 满足形状 → 仍列式支持（2026-08-23 review：
    // parse 基于 live_joins 而非 plan.joins，死 join 不阻塞活 join）。
    let mut plan = base();
    // 死 join（Snapshot，无任何输出引用）——插在活 join 前。
    plan.joins.insert(
        0,
        JoinPlan {
            right_window: "person_events".into(),
            mode: JoinMode::Snapshot,
            conds: vec![JoinCondPlan {
                left: FieldRef::Qualified("b".into(), "bidder".into()),
                right: FieldRef::Qualified("person_events".into(), "id".into()),
            }],
            within: None,
            reduce: None,
            emit_at: None,
        },
    );
    // 活 join（yield 读右窗 auction_events.category）。
    plan.yield_plan.fields = vec![YieldField {
        name: "cat".into(),
        value: Expr::Field(FieldRef::Qualified(
            "auction_events".into(),
            "category".into(),
        )),
    }];
    let exec = RuleExecutor::new(plan);
    assert_eq!(
        exec.live_joins().len(),
        1,
        "1 死 1 活 → live_joins 只剩活 join"
    );
    assert!(
        exec.each_join_columnar_ready() && exec.each_plan_columnar_safe(),
        "活 join 满足形状 → 死 join 不阻塞列式 join"
    );

    // join 条件左字段限定符非驱动别名 → 不支持（防御）。
    // 必须让 join 存活（yield 读右窗字段），否则死 join 消除 → live_joins 空 →
    // 走无 join 路径，防御逻辑（parse_each_join_columnar）不触发。
    let mut plan = base();
    plan.yield_plan.fields = vec![YieldField {
        name: "cat".into(),
        value: Expr::Field(FieldRef::Qualified(
            "auction_events".into(),
            "category".into(),
        )),
    }];
    plan.joins[0].conds[0].left = FieldRef::Qualified("other".into(), "auction".into());
    assert!(!RuleExecutor::new(plan).each_plan_columnar_safe());
}

fn each_plan_rule() -> RuleExecutor {
    let mut plan = simple_rule_plan(
        "q1_pass",
        simple_plan(vec![], vec![]),
        Expr::Number(42.5),
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
            name: "auction_id".into(),
            value: Expr::Field(FieldRef::Qualified("e".into(), "auction_id".into())),
        },
        YieldField {
            name: "price".into(),
            value: Expr::Field(FieldRef::Qualified("e".into(), "price".into())),
        },
    ];
    RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([
            ("auction_id".into(), FieldType::Base(BaseType::Float)),
            ("price".into(), FieldType::Base(BaseType::Float)),
        ]),
    )
}

fn sample_events() -> Vec<Event> {
    vec![
        event(vec![
            ("sip", str_val("10.0.0.1")),
            ("auction_id", num(1000.0)),
            ("price", num(99.5)),
        ]),
        event(vec![
            ("sip", str_val("10.0.0.2")),
            ("auction_id", num(1001.0)),
            ("price", num(79.25)),
        ]),
        // Missing optional `price` → the field must be omitted (#62),
        // exercising the sparse-column layout drift on the direct path.
        event(vec![
            ("sip", str_val("10.0.0.3")),
            ("auction_id", num(1002.0)),
        ]),
    ]
}

#[test]
fn execute_each_direct_matches_record_path_rows() {
    let exec = each_plan_rule();
    let events = sample_events();
    let lookup = EmptyLookup;
    const NANOS: i64 = 1_750_000_000_000_000_000;

    // Record path.
    let mut via_records = AlertColumnBuilder::new(Arc::from("alerts"));
    for ev in &events {
        let record = exec
            .execute_each_with_joins(ev, NANOS, &lookup, &[], NANOS + 1)
            .expect("record path must succeed")
            .expect("filter passes");
        via_records.append_record(&record).unwrap();
    }

    // Direct path.
    let mut via_direct = AlertColumnBuilder::new(Arc::from("alerts"));
    for ev in &events {
        let appended = exec
            .execute_each_direct(ev, NANOS, &lookup, &[], NANOS + 1, &mut via_direct)
            .expect("direct path must succeed");
        assert!(appended, "filter passes on the direct path too");
    }

    let record_batch = via_records.finish();
    let direct_batch = via_direct.finish();
    assert_eq!(record_batch.len(), direct_batch.len());
    for row in 0..record_batch.len() {
        let a = record_batch.iter_data_records().nth(row).unwrap().unwrap();
        let b = direct_batch.iter_data_records().nth(row).unwrap().unwrap();
        assert_eq!(a.items.len(), b.items.len(), "row {row} field count");
        for (fa, fb) in a.items.iter().zip(b.items.iter()) {
            assert_eq!(fa.get_name(), fb.get_name(), "row {row} field name");
            assert_eq!(fa.get_meta(), fb.get_meta(), "row {row} field meta");
            assert_eq!(fa.get_value(), fb.get_value(), "row {row} field value");
        }
    }
}

#[test]
fn execute_each_direct_filter_rejection_appends_nothing() {
    let mut plan = simple_rule_plan(
        "filtered",
        simple_plan(vec![], vec![]),
        Expr::Number(1.0),
        "ip",
        Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: Some(Expr::BinOp {
            op: wf_lang::ast::BinOp::Eq,
            left: Box::new(Expr::Field(FieldRef::Qualified("e".into(), "sip".into()))),
            right: Box::new(Expr::StringLit("10.0.0.1".into())),
        }),
    });
    let exec = RuleExecutor::new(plan);
    let lookup = EmptyLookup;
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let appended = exec
        .execute_each_direct(
            &event(vec![("sip", str_val("10.9.9.9"))]),
            1_000_000,
            &lookup,
            &[],
            1_000_001,
            &mut builder,
        )
        .unwrap();
    assert!(!appended, "where filter rejects the event");
    assert!(builder.is_empty());
}

#[test]
fn execute_each_direct_surfaces_eval_errors() {
    // Explicit NaN against a Float-typed yield must fail identically to the
    // record path (no partial row committed).
    let mut plan = simple_rule_plan(
        "nan_rule",
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
    let lookup = EmptyLookup;
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let result = exec.execute_each_direct(
        &event(vec![("sip", str_val("10.0.0.1"))]),
        1_000_000,
        &lookup,
        &[],
        1_000_001,
        &mut builder,
    );
    assert!(result.is_err(), "explicit NaN must fail the direct path");
    assert!(builder.is_empty(), "failed row must not touch columns");
}

#[test]
fn direct_path_wfx_id_matches_record_path() {
    // wfx_id depends on the event fields — the direct path must hash the
    // identical byte stream (spot-check via the row view).
    let exec = each_plan_rule();
    let ev = sample_events().remove(0);
    let lookup = EmptyLookup;
    const NANOS: i64 = 1_750_000_000_123_456_789;

    let record = exec
        .execute_each_with_joins(&ev, NANOS, &lookup, &[], NANOS + 7)
        .unwrap()
        .unwrap();
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    exec.execute_each_direct(&ev, NANOS, &lookup, &[], NANOS + 7, &mut builder)
        .unwrap();
    let batch = builder.finish();
    let direct_row = batch.iter_data_records().next().unwrap().unwrap();
    let record_row = record.to_data_record().unwrap();
    let direct_id = direct_row
        .items
        .iter()
        .find(|f| f.get_name() == "__wfu_id")
        .unwrap();
    let record_id = record_row
        .items
        .iter()
        .find(|f| f.get_name() == "__wfu_id")
        .unwrap();
    assert_eq!(direct_id.get_value(), record_id.get_value());
}

// -- Batched direct path (build_each_direct vectorization) ------------------

/// Row-view comparison helper: two finished batches must expose identical
/// `DataRecord` row views.
fn assert_batches_equal_rows(
    a: &crate::alert::AlertColumnBatch,
    b: &crate::alert::AlertColumnBatch,
) {
    assert_eq!(a.len(), b.len(), "row count");
    for row in 0..a.len() {
        let ra = a.iter_data_records().nth(row).unwrap().unwrap();
        let rb = b.iter_data_records().nth(row).unwrap().unwrap();
        assert_eq!(ra.items.len(), rb.items.len(), "row {row} field count");
        for (fa, fb) in ra.items.iter().zip(rb.items.iter()) {
            assert_eq!(fa.get_name(), fb.get_name(), "row {row} field name");
            assert_eq!(fa.get_meta(), fb.get_meta(), "row {row} field meta");
            assert_eq!(fa.get_value(), fb.get_value(), "row {row} field value");
        }
    }
}

#[test]
fn execute_each_direct_batch_matches_per_event_path_rows() {
    // Same mixed event batch (one with a missing optional field) through the
    // per-event direct path and the batched direct path must produce
    // identical rows, appended counts, and appended-index bookkeeping.
    let exec = each_plan_rule();
    let events = sample_events();
    let lookup = EmptyLookup;
    const NANOS: i64 = 1_750_000_000_000_000_000;

    // Per-event path.
    let mut via_per_event = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut per_event_appended = 0usize;
    for (i, ev) in events.iter().enumerate() {
        let appended = exec
            .execute_each_direct(
                ev,
                NANOS + i as i64,
                &lookup,
                &[],
                NANOS,
                &mut via_per_event,
            )
            .expect("per-event direct path must succeed");
        if appended {
            per_event_appended += 1;
        }
    }

    // Batched path.
    let rows: Vec<(&Event, i64)> = events
        .iter()
        .enumerate()
        .map(|(i, ev)| (ev, NANOS + i as i64))
        .collect();
    let mut via_batch = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut appended_idx = Vec::new();
    let stats = exec.execute_each_direct_batch(
        &rows,
        &lookup,
        &[],
        NANOS,
        &mut via_batch,
        &mut appended_idx,
    );
    assert_eq!(stats.appended, per_event_appended);
    assert_eq!(stats.rejected, 0);
    assert_eq!(stats.failed, 0);
    assert_eq!(appended_idx, (0..events.len()).collect::<Vec<_>>());

    assert_batches_equal_rows(&via_per_event.finish(), &via_batch.finish());
}

#[test]
fn execute_each_direct_batch_lit_and_general_specs_match_record_path() {
    // Const score + StringLit entity + literal/field/general (WfuMeta) yields:
    // every specialization lane must stay row-equivalent to the record path.
    use wf_lang::wfu_meta::WfuMetaField;

    let mut plan = simple_rule_plan(
        "lit_rule",
        simple_plan(vec![], vec![]),
        Expr::Number(7.5),
        "ip",
        Expr::StringLit("fixed-entity".into()),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    plan.yield_plan.fields = vec![
        YieldField {
            name: "const_str".into(),
            value: Expr::StringLit("const-value".into()),
        },
        YieldField {
            name: "const_num".into(),
            value: Expr::Number(1.25),
        },
        YieldField {
            name: "const_bool".into(),
            value: Expr::Bool(true),
        },
        YieldField {
            name: "sip".into(),
            value: Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
        },
        YieldField {
            name: "fired_at".into(),
            value: Expr::WfuMeta(WfuMetaField::FiredAt),
        },
    ];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([
            ("const_str".into(), FieldType::Base(BaseType::Chars)),
            ("const_num".into(), FieldType::Base(BaseType::Float)),
            ("const_bool".into(), FieldType::Base(BaseType::Bool)),
            ("sip".into(), FieldType::Base(BaseType::Chars)),
            ("fired_at".into(), FieldType::Base(BaseType::Chars)),
        ]),
    );
    let events = sample_events();
    let lookup = EmptyLookup;
    const NANOS: i64 = 1_750_000_000_000_000_000;

    // Record path.
    let mut via_records = AlertColumnBuilder::new(Arc::from("alerts"));
    for (i, ev) in events.iter().enumerate() {
        let record = exec
            .execute_each_with_joins(ev, NANOS + i as i64, &lookup, &[], NANOS)
            .unwrap()
            .unwrap();
        via_records.append_record(&record).unwrap();
    }

    // Batched path.
    let rows: Vec<(&Event, i64)> = events
        .iter()
        .enumerate()
        .map(|(i, ev)| (ev, NANOS + i as i64))
        .collect();
    let mut via_batch = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut appended_idx = Vec::new();
    let stats = exec.execute_each_direct_batch(
        &rows,
        &lookup,
        &[],
        NANOS,
        &mut via_batch,
        &mut appended_idx,
    );
    assert_eq!(stats.appended, events.len());
    assert_eq!(stats.failed, 0);
    assert_batches_equal_rows(&via_records.finish(), &via_batch.finish());
}

#[test]
fn execute_each_direct_batch_mid_batch_failure_skips_only_that_row() {
    // Row 2's sip is a non-empty string against a Float yield → conversion
    // error; rows 1/3 lack sip entirely → optional omission. The batch must
    // append 2 rows, fail 1, and match the per-event loop exactly.
    let mut plan = simple_rule_plan(
        "mixed_rule",
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
    plan.yield_plan.fields = vec![
        YieldField {
            name: "auction_id".into(),
            value: Expr::Field(FieldRef::Qualified("e".into(), "auction_id".into())),
        },
        YieldField {
            name: "sip_f".into(),
            value: Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
        },
    ];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([
            ("auction_id".into(), FieldType::Base(BaseType::Float)),
            ("sip_f".into(), FieldType::Base(BaseType::Float)),
        ]),
    );
    let events = [
        event(vec![("auction_id", num(1.0))]),
        event(vec![("sip", str_val("10.0.0.2")), ("auction_id", num(2.0))]),
        event(vec![("auction_id", num(3.0))]),
    ];
    let lookup = EmptyLookup;
    const NANOS: i64 = 1_750_000_000_000_000_000;

    // Per-event path: row 2 errors, rows 1/3 append.
    let mut via_per_event = AlertColumnBuilder::new(Arc::from("alerts"));
    for (i, ev) in events.iter().enumerate() {
        let _ = exec.execute_each_direct(
            ev,
            NANOS + i as i64,
            &lookup,
            &[],
            NANOS,
            &mut via_per_event,
        );
    }

    // Batched path.
    let rows: Vec<(&Event, i64)> = events
        .iter()
        .enumerate()
        .map(|(i, ev)| (ev, NANOS + i as i64))
        .collect();
    let mut via_batch = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut appended_idx = Vec::new();
    let stats = exec.execute_each_direct_batch(
        &rows,
        &lookup,
        &[],
        NANOS,
        &mut via_batch,
        &mut appended_idx,
    );
    assert_eq!(stats.appended, 2, "rows 1 and 3 append");
    assert_eq!(stats.failed, 1, "row 2 conversion error");
    assert_eq!(appended_idx, vec![0, 2]);
    assert_batches_equal_rows(&via_per_event.finish(), &via_batch.finish());
}

#[test]
fn execute_each_direct_batch_filter_rejections_match_per_event_path() {
    // Where-filter rejects must be counted as rejected and produce no rows —
    // identical to the per-event path.
    let mut plan = simple_rule_plan(
        "filtered_batch",
        simple_plan(vec![], vec![]),
        Expr::Number(1.0),
        "ip",
        Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: Some(Expr::BinOp {
            op: wf_lang::ast::BinOp::Eq,
            left: Box::new(Expr::Field(FieldRef::Qualified("e".into(), "sip".into()))),
            right: Box::new(Expr::StringLit("10.0.0.1".into())),
        }),
    });
    let exec = RuleExecutor::new(plan);
    let events = [
        event(vec![("sip", str_val("10.0.0.1")), ("auction_id", num(1.0))]),
        event(vec![("sip", str_val("10.9.9.9")), ("auction_id", num(2.0))]),
        event(vec![("sip", str_val("10.0.0.1")), ("auction_id", num(3.0))]),
    ];
    let lookup = EmptyLookup;
    const NANOS: i64 = 1_750_000_000_000_000_000;

    let rows: Vec<(&Event, i64)> = events.iter().map(|ev| (ev, NANOS)).collect();
    let mut via_batch = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut appended_idx = Vec::new();
    let stats = exec.execute_each_direct_batch(
        &rows,
        &lookup,
        &[],
        NANOS,
        &mut via_batch,
        &mut appended_idx,
    );
    assert_eq!(stats.appended, 2);
    assert_eq!(stats.rejected, 1);
    assert_eq!(appended_idx, vec![0, 2]);
    assert_eq!(via_batch.len(), 2);
}

#[test]
fn execute_each_direct_batch_columnar_matches_event_path_rows() {
    // Deferred-vs-columnar 对拍: the columnar fast path (no per-row `Event`
    // materialization, field reads straight from Arrow columns) must produce
    // byte-identical rows to the eager `materialize_rows` + batch path —
    // including the missing-field (null) lane and the 2^53 f64 round-trip
    // lane in the wfx_id hash.
    use crate::match_engine::event_bridge::{ColumnarEvent, materialize_rows};
    use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    let exec = each_plan_rule();
    assert!(
        exec.each_plan_columnar_safe(),
        "test rule must be columnar-safe"
    );
    const NANOS: i64 = 1_750_000_000_000_000_000;

    let schema = Arc::new(Schema::new(vec![
        Field::new("sip", DataType::Utf8, true),
        Field::new("auction_id", DataType::Float64, true),
        Field::new("price", DataType::Float64, true),
        // Int64 column with a 2^53+1 value: `extract_value` renders it through
        // the f64 round-trip — both paths must hash the identical bytes.
        Field::new("big", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["10.0.0.1", "10.0.0.2", "10.0.0.3"])) as ArrayRef,
            Arc::new(Float64Array::from(vec![1000.0, 1001.0, 1002.0])),
            Arc::new(Float64Array::from(vec![Some(99.5), Some(79.25), None])),
            Arc::new(Int64Array::from(vec![
                9007199254740993,
                -9007199254740993,
                42,
            ])),
        ],
    )
    .unwrap();

    // Reference: eager materialization + the Event-based batch path.
    let events: Vec<Event> = materialize_rows(&batch, &[0, 1, 2]);
    let rows: Vec<(&Event, i64)> = events
        .iter()
        .enumerate()
        .map(|(i, ev)| (ev, NANOS + i as i64))
        .collect();
    let mut via_events = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut appended_idx = Vec::new();
    let stats = exec.execute_each_direct_batch(
        &rows,
        &EmptyLookup,
        &[],
        NANOS,
        &mut via_events,
        &mut appended_idx,
    );
    assert_eq!(stats.appended, 3);
    assert_eq!(stats.rejected, 0);
    assert_eq!(stats.failed, 0);

    // Columnar fast path.
    let col_events: Vec<ColumnarEvent<'_>> =
        (0..3).map(|r| ColumnarEvent::new(&batch, r)).collect();
    let col_rows: Vec<(&ColumnarEvent<'_>, i64)> = col_events
        .iter()
        .enumerate()
        .map(|(i, ev)| (ev, NANOS + i as i64))
        .collect();
    let mut via_columnar = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut appended_idx_c = Vec::new();
    let stats_c = exec.execute_each_direct_batch_columnar(
        &col_rows,
        NANOS,
        &mut via_columnar,
        &mut appended_idx_c,
    );
    assert_eq!(stats_c, stats);
    assert_eq!(appended_idx_c, appended_idx);

    assert_batches_equal_rows(&via_events.finish(), &via_columnar.finish());
}

#[test]
fn columnar_numeric_entity_yield_fast_path_matches_event_path() {
    // last-materialization fast path: a numeric field-entity (Q1
    // `entity(digit, b.auction)`) whose yield field references the *same*
    // column (id=b.auction) stages the raw f64 directly via
    // `stage_yield_cell_f64` instead of constructing + coercing a `Value` per
    // row. Must be byte-identical to the Event path for every numeric declare
    // target (digit / float / chars / untyped), including a null-entity row.
    use crate::match_engine::event_bridge::{ColumnarEvent, materialize_rows};
    use arrow::array::{ArrayRef, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    const NANOS: i64 = 1_750_000_000_000_000_000;
    for declare in [
        Some(FieldType::Base(BaseType::Digit)),
        Some(FieldType::Base(BaseType::Float)),
        Some(FieldType::Base(BaseType::Chars)),
        None, // untyped
    ] {
        let mut plan = simple_rule_plan(
            "q_num_entity",
            simple_plan(vec![], vec![]),
            Expr::Number(5.0),
            "digit",
            Expr::Field(FieldRef::Qualified("e".into(), "auction".into())),
        );
        plan.binds[0].alias = "e".into();
        plan.each_plan = Some(EachPlan {
            alias: "e".into(),
            filter: None,
        });
        plan.yield_plan.fields = vec![
            YieldField {
                name: "id".into(),
                value: Expr::Field(FieldRef::Qualified("e".into(), "auction".into())),
            },
            YieldField {
                name: "c".into(),
                value: Expr::Number(1.0),
            },
        ];
        let types: HashMap<String, FieldType> = match &declare {
            Some(ft) => HashMap::from([("id".into(), ft.clone()), ("c".into(), ft.clone())]),
            None => HashMap::from([("c".into(), FieldType::Base(BaseType::Float))]),
        };
        let exec = RuleExecutor::new_with_yield_field_types(plan, types);
        assert!(exec.each_plan_columnar_safe());

        let schema = Arc::new(Schema::new(vec![Field::new(
            "auction",
            DataType::Int64,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![
                Some(123),
                Some(1_000_000),
                None, // null entity → same failure semantics both paths
                Some(7),
            ])) as ArrayRef],
        )
        .unwrap();

        let events: Vec<Event> = materialize_rows(&batch, &[0, 1, 2, 3]);
        let rows: Vec<(&Event, i64)> = events
            .iter()
            .enumerate()
            .map(|(i, ev)| (ev, NANOS + i as i64))
            .collect();
        let mut via_events = AlertColumnBuilder::new(Arc::from("alerts"));
        let mut appended_idx = Vec::new();
        let stats = exec.execute_each_direct_batch(
            &rows,
            &EmptyLookup,
            &[],
            NANOS,
            &mut via_events,
            &mut appended_idx,
        );

        let col_events: Vec<ColumnarEvent<'_>> =
            (0..4).map(|r| ColumnarEvent::new(&batch, r)).collect();
        let col_rows: Vec<(&ColumnarEvent<'_>, i64)> = col_events
            .iter()
            .enumerate()
            .map(|(i, ev)| (ev, NANOS + i as i64))
            .collect();
        let mut via_columnar = AlertColumnBuilder::new(Arc::from("alerts"));
        let mut appended_idx_c = Vec::new();
        let stats_c = exec.execute_each_direct_batch_columnar(
            &col_rows,
            NANOS,
            &mut via_columnar,
            &mut appended_idx_c,
        );
        assert_eq!(stats_c, stats, "declare={declare:?}");
        assert_eq!(appended_idx_c, appended_idx);
        assert_batches_equal_rows(&via_events.finish(), &via_columnar.finish());
    }
}

#[test]
fn columnar_utf8_entity_null_lane_matches_event_path_rows() {
    // P2 Utf8 entity fast lane (the qradar shape: sip / source_ip / user):
    // entity on a non-structured Utf8 column, a yield field sharing the same
    // column (entity_val reuse), and a null-entity row — the columnar path
    // must fail that row exactly like the Event path (same stats, same
    // appended indices, byte-identical rows).
    use crate::match_engine::event_bridge::{ColumnarEvent, materialize_rows};
    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    let mut plan = simple_rule_plan(
        "q_utf8_entity",
        simple_plan(vec![], vec![]),
        Expr::Number(10.0),
        "chars",
        Expr::Field(FieldRef::Qualified("e".into(), "user".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    plan.yield_plan.fields = vec![
        YieldField {
            name: "user".into(),
            value: Expr::Field(FieldRef::Qualified("e".into(), "user".into())),
        },
        YieldField {
            name: "cnt".into(),
            value: Expr::Number(1.0),
        },
    ];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([
            ("user".into(), FieldType::Base(BaseType::Chars)),
            ("cnt".into(), FieldType::Base(BaseType::Float)),
        ]),
    );
    assert!(exec.each_plan_columnar_safe());
    const NANOS: i64 = 1_750_000_000_000_000_000;

    let schema = Arc::new(Schema::new(vec![
        Field::new("user", DataType::Utf8, true),
        Field::new("n", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![
                Some("alice"),
                None, // null entity → both paths must fail this row identically
                Some("bob"),
            ])) as ArrayRef,
            Arc::new(Int64Array::from(vec![1, 2, 3])),
        ],
    )
    .unwrap();

    // Reference: eager materialization + the Event-based batch path.
    let events: Vec<Event> = materialize_rows(&batch, &[0, 1, 2]);
    let rows: Vec<(&Event, i64)> = events
        .iter()
        .enumerate()
        .map(|(i, ev)| (ev, NANOS + i as i64))
        .collect();
    let mut via_events = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut appended_idx = Vec::new();
    let stats = exec.execute_each_direct_batch(
        &rows,
        &EmptyLookup,
        &[],
        NANOS,
        &mut via_events,
        &mut appended_idx,
    );

    // Columnar fast path (Utf8 entity lane + shared-column yield reuse).
    let col_events: Vec<ColumnarEvent<'_>> =
        (0..3).map(|r| ColumnarEvent::new(&batch, r)).collect();
    let col_rows: Vec<(&ColumnarEvent<'_>, i64)> = col_events
        .iter()
        .enumerate()
        .map(|(i, ev)| (ev, NANOS + i as i64))
        .collect();
    let mut via_columnar = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut appended_idx_c = Vec::new();
    let stats_c = exec.execute_each_direct_batch_columnar(
        &col_rows,
        NANOS,
        &mut via_columnar,
        &mut appended_idx_c,
    );
    assert_eq!(stats_c, stats);
    assert_eq!(appended_idx_c, appended_idx);
    // The null-entity row appends on BOTH paths with entity_id = "" (the
    // yield missing-field fallback routes the entity read to Str("")) —
    // no failures on either side.
    assert_eq!(stats.appended, 3);
    assert_eq!(stats.failed, 0);
    assert_batches_equal_rows(&via_events.finish(), &via_columnar.finish());
}

/// Q1 形状的 plan：3 个字面量 yield（alert_type/detail/request_count，批级
/// 常量缓存路径）+ 1 个字段 yield（id = b.auction，逐行取列）。
fn q1_lit_shape_rule() -> RuleExecutor {
    use wf_lang::plan::{EachPlan, YieldField};
    let mut plan = simple_rule_plan(
        "q1_lit_shape",
        simple_plan(vec![], vec![]),
        Expr::Number(1.0),
        "digit",
        Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
    );
    plan.binds[0].alias = "b".into();
    plan.binds[0].window = "bid_events".into();
    plan.each_plan = Some(EachPlan {
        alias: "b".into(),
        filter: None,
    });
    plan.yield_plan.fields = vec![
        YieldField {
            name: "id".into(),
            value: Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
        },
        YieldField {
            name: "alert_type".into(),
            value: Expr::StringLit("q1_passthrough".into()),
        },
        YieldField {
            name: "detail".into(),
            value: Expr::StringLit("bid".into()),
        },
        YieldField {
            name: "request_count".into(),
            value: Expr::Number(1.0),
        },
    ];
    RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([
            ("id".into(), FieldType::Base(BaseType::Float)),
            ("alert_type".into(), FieldType::Base(BaseType::Chars)),
            ("detail".into(), FieldType::Base(BaseType::Chars)),
            ("request_count".into(), FieldType::Base(BaseType::Float)),
        ]),
    )
}

#[test]
fn columnar_const_yield_literals_match_event_path_rows() {
    // 常量 yield 批级缓存（register_yield_column + fill_row_gaps 填常量）
    // 必须与 eager 路径逐字节一致——含 null→字段缺失 lane 与 2^53 lane。
    use crate::match_engine::event_bridge::{ColumnarEvent, materialize_rows};
    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    let exec = q1_lit_shape_rule();
    assert!(
        exec.each_plan_columnar_safe(),
        "Q1 shape must be columnar-safe"
    );
    const NANOS: i64 = 1_750_000_000_000_000_000;

    let schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, true),
        Field::new("bidder", DataType::Int64, true),
        Field::new("price", DataType::Int64, true),
        Field::new("channel", DataType::Utf8, true),
        Field::new("url", DataType::Utf8, true),
        Field::new("dateTime", DataType::Int64, true),
        Field::new("extra", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1i64, 2, 3])) as ArrayRef,
            Arc::new(Int64Array::from(vec![1i64, 2, 3])),
            Arc::new(Int64Array::from(vec![7i64, 8, 9])),
            Arc::new(StringArray::from(vec!["mobile", "phone", "mobile"])),
            Arc::new(StringArray::from(vec![
                "http://example.com/a",
                "http://example.com/b",
                "http://example.com/c",
            ])),
            Arc::new(Int64Array::from(vec![1_700_000_000_000i64; 3])),
            Arc::new(StringArray::from(vec!["x"; 3])),
        ],
    )
    .unwrap();

    // Reference: eager materialization + Event-based batch path.
    let events: Vec<Event> = materialize_rows(&batch, &[0, 1, 2]);
    let rows: Vec<(&Event, i64)> = events
        .iter()
        .enumerate()
        .map(|(i, ev)| (ev, NANOS + i as i64))
        .collect();
    let mut via_events = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut appended_idx = Vec::new();
    let stats = exec.execute_each_direct_batch(
        &rows,
        &EmptyLookup,
        &[],
        NANOS,
        &mut via_events,
        &mut appended_idx,
    );
    assert_eq!(stats.appended, 3);
    assert_eq!(stats.failed, 0);

    // Columnar fast path (constant-yield caching active).
    let col_events: Vec<ColumnarEvent<'_>> =
        (0..3).map(|r| ColumnarEvent::new(&batch, r)).collect();
    let col_rows: Vec<(&ColumnarEvent<'_>, i64)> = col_events
        .iter()
        .enumerate()
        .map(|(i, ev)| (ev, NANOS + i as i64))
        .collect();
    let mut via_columnar = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut appended_idx_c = Vec::new();
    let stats_c = exec.execute_each_direct_batch_columnar(
        &col_rows,
        NANOS,
        &mut via_columnar,
        &mut appended_idx_c,
    );
    assert_eq!(stats_c, stats);
    assert_eq!(appended_idx_c, appended_idx);

    assert_batches_equal_rows(&via_events.finish(), &via_columnar.finish());
}

/// null/missing entity 字段：行式（`eval_entity_id` 缺失 → Err → failed+skip）
/// vs 列式必须逐位一致（2026-08-23 review 发现列式 join 版缺失 → 空串输出的
/// 不一致，此处先锁无 join 列式版行为）。
#[test]
fn columnar_null_entity_matches_event_path_failure_semantics() {
    use crate::match_engine::event_bridge::{ColumnarEvent, materialize_rows};
    use arrow::array::{ArrayRef, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    let mut plan = simple_rule_plan(
        "r",
        simple_plan(vec![], vec![]),
        Expr::Number(1.0),
        "ip",
        Expr::Field(FieldRef::Qualified("e".into(), "auction".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    plan.yield_plan.fields = vec![YieldField {
        name: "id".into(),
        value: Expr::Field(FieldRef::Qualified("e".into(), "auction".into())),
    }];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([("id".into(), FieldType::Base(BaseType::Digit))]),
    );
    let schema = Arc::new(Schema::new(vec![Field::new(
        "auction",
        DataType::Int64,
        true,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![Some(1), None])) as ArrayRef],
    )
    .unwrap();
    const NANOS: i64 = 1_750_000_000_000_000_000;

    let events: Vec<Event> = materialize_rows(&batch, &[0, 1]);
    let rows: Vec<(&Event, i64)> = events
        .iter()
        .enumerate()
        .map(|(i, e)| (e, NANOS + i as i64))
        .collect();
    let mut b1 = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut idx1 = Vec::new();
    let s1 = exec.execute_each_direct_batch(&rows, &EmptyLookup, &[], NANOS, &mut b1, &mut idx1);

    let col: Vec<ColumnarEvent<'_>> = (0..2).map(|r| ColumnarEvent::new(&batch, r)).collect();
    let crows: Vec<(&ColumnarEvent<'_>, i64)> = col
        .iter()
        .enumerate()
        .map(|(i, e)| (e, NANOS + i as i64))
        .collect();
    let mut b2 = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut idx2 = Vec::new();
    let s2 = exec.execute_each_direct_batch_columnar(&crows, NANOS, &mut b2, &mut idx2);

    assert_eq!(
        s1, s2,
        "null entity: 行式/列式必须一致 (row appended={} failed={} | col appended={} failed={})",
        s1.appended, s1.failed, s2.appended, s2.failed
    );
    assert_eq!(idx1, idx2);
    assert_batches_equal_rows(&b1.finish(), &b2.finish());
}
