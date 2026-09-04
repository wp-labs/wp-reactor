//! direct_tests.rs 拆出的列式 Snapshot join 直发测试（2026-09-04；`#[path]`
//! 子模块，经父模块 `use super::*` 复用其导入）。
//!
//! 覆盖：`execute_each_direct_batch_columnar_join` 与行式 Event 路径的字节级对拍
//! （含 where 多谓词合取 / float 左键截断复核 / 无 where miss 输出 / 热键同桶
//! Arc 共享 / recheck 救援 fill 快照 miss / null 右窗字段拒绝），以及
//! `each_join_columnar_gate` 对不支持形状（复合 where / 多条件 / Asof / 裸名
//! yield / 死 join 消除 / 左字段限定符）的放行-拒绝门控。

use super::*;

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

/// Stateful join lookup：每个 key 的**首次** lookup 返回空桶（fill 快照时
/// 桶仍空——模拟批处理开始时实体尚未 append），后续 lookup 返回行（模拟
/// 并行 ingest 在批处理期间补 append，行时 recheck 可见）。
/// 专测 `execute_each_direct_batch_columnar_join` 的 recheck 救援路径
/// （`miss_hold`：fill 快照 miss 的行在行循环时点实时复查）。
struct GrowJoinLookup {
    rows: Vec<crate::match_engine::JoinRow>,
    calls: Mutex<HashMap<crate::match_engine::JoinKey, usize>>,
}

impl WindowLookup for GrowJoinLookup {
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
    fn join_lookup(
        &self,
        _window: &str,
        key_field: &str,
        key: &crate::match_engine::Value,
    ) -> Option<Vec<crate::match_engine::JoinRow>> {
        let join_key = crate::match_engine::JoinKey::from_value(key)?;
        let mut calls = self.calls.lock().expect("GrowJoinLookup mutex poisoned");
        let n = calls.entry(join_key).or_insert(0);
        *n += 1;
        if *n == 1 {
            return Some(Vec::new()); // 首次（fill 快照）空桶 → 批级 miss
        }
        // 之后（行时 recheck）返回桶内行——与 trait 默认相同的按键过滤。
        Some(
            self.rows
                .iter()
                .filter(|r| {
                    r.field_value(key_field)
                        .is_some_and(|rv| crate::match_engine::values_equal(&rv, key))
                })
                .cloned()
                .collect(),
        )
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

/// 热键多行同桶（2026-08-24 Arc<JoinRow> 修复）：同一左 key 多行共享桶首行
/// JoinRow（每桶只建一个 Arc，每行 1 次 Arc clone）——输出必须与行式逐事件
/// 路径字节一致，且桶首行（而非桶内其它行）决定富化。
#[test]
fn columnar_join_hot_key_rows_match_event_path() {
    use crate::match_engine::event_bridge::{ColumnarEvent, JoinRow, materialize_rows};
    use arrow::array::{ArrayRef, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    let exec = each_join_plan_rule();
    // 右窗：id=1 有两行（cat=10 在前、cat=99 在后——桶首行 cat=10 必须赢），
    // id=2（cat=20 拒绝）、id=3（cat=10）。
    let lookup = MockJoinLookup {
        rows: vec![
            JoinRow::Event(Arc::new(event(vec![
                ("id", num(1.0)),
                ("category", num(10.0)),
            ]))),
            JoinRow::Event(Arc::new(event(vec![
                ("id", num(1.0)),
                ("category", num(99.0)),
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
    // 热键 auction=1 重复 3 行（同桶共享首行），auction=2/3 单行。
    let schema = Arc::new(Schema::new(vec![Field::new(
        "auction",
        DataType::Int64,
        true,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![1, 1, 1, 2, 3])) as ArrayRef],
    )
    .unwrap();

    // 行式参考路径。
    let events: Vec<Event> = materialize_rows(&batch, &[0, 1, 2, 3, 4]);
    let rows: Vec<(&Event, i64)> = events
        .iter()
        .enumerate()
        .map(|(i, ev)| (ev, NANOS + i as i64))
        .collect();
    let mut b_row = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut idx_row = Vec::new();
    let s_row =
        exec.execute_each_direct_batch(&rows, &lookup, &[], NANOS, &mut b_row, &mut idx_row);
    assert_eq!(
        s_row.appended, 4,
        "3×auction=1 + auction=3 命中（桶首行 cat=10）；auction=2 拒绝"
    );
    assert_eq!(s_row.rejected, 1);

    // 列式 join 路径（热键同桶 Arc 共享）。
    let col_events: Vec<ColumnarEvent<'_>> =
        (0..5).map(|r| ColumnarEvent::new(&batch, r)).collect();
    let col_rows: Vec<(&ColumnarEvent<'_>, i64)> = col_events
        .iter()
        .enumerate()
        .map(|(i, ev)| (ev, NANOS + i as i64))
        .collect();
    let mut b_col = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut idx_col = Vec::new();
    let s_col = exec.execute_each_direct_batch_columnar_join(
        &col_rows,
        &lookup,
        NANOS,
        &mut b_col,
        &mut idx_col,
    );
    assert_eq!(s_col, s_row);
    assert_eq!(idx_col, idx_row);
    assert_batches_equal_rows(&b_row.finish(), &b_col.finish());
}

/// recheck 救援（2026-08-24 `miss_hold` 新路径）：fill 快照时桶空（key 首次
/// lookup 空桶）→ 批级 miss；行循环时点实时复查（key 后续 lookup 非空）→
/// 救回并富化。用状态化 GrowJoinLookup 断言：救援后的输出与「快照即命中」
/// 的静态 mock **字节一致**——即 recheck 完整补回 fill 快照 miss。
#[test]
fn columnar_join_recheck_rescues_mid_batch_append() {
    use crate::match_engine::event_bridge::{ColumnarEvent, JoinRow};
    use arrow::array::{ArrayRef, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    let exec = each_join_plan_rule();
    let rows_auc = vec![
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
    ];

    const NANOS: i64 = 1_750_000_000_000_000_000;
    let schema = Arc::new(Schema::new(vec![Field::new(
        "auction",
        DataType::Int64,
        true,
    )]));
    // 热键 auction=1 重复 3 行：fill 首次 lookup 全 miss → recheck 逐行救援。
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![1, 1, 1, 2, 3])) as ArrayRef],
    )
    .unwrap();
    let col_events: Vec<ColumnarEvent<'_>> =
        (0..5).map(|r| ColumnarEvent::new(&batch, r)).collect();
    let col_rows: Vec<(&ColumnarEvent<'_>, i64)> = col_events
        .iter()
        .enumerate()
        .map(|(i, ev)| (ev, NANOS + i as i64))
        .collect();

    // 静态快照（fill 即命中）：
    let static_lookup = MockJoinLookup {
        rows: rows_auc.clone(),
    };
    let mut b_static = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut idx_static = Vec::new();
    let s_static = exec.execute_each_direct_batch_columnar_join(
        &col_rows,
        &static_lookup,
        NANOS,
        &mut b_static,
        &mut idx_static,
    );

    // 延迟 append（每 key 首次 lookup 空桶 → 批级 miss；recheck 救援）：
    let grow_lookup = GrowJoinLookup {
        rows: rows_auc,
        calls: Mutex::new(HashMap::new()),
    };
    let mut b_grow = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut idx_grow = Vec::new();
    let s_grow = exec.execute_each_direct_batch_columnar_join(
        &col_rows,
        &grow_lookup,
        NANOS,
        &mut b_grow,
        &mut idx_grow,
    );

    assert_eq!(
        s_grow, s_static,
        "recheck 必须完整补回 fill 快照 miss：grow={s_grow:?} static={s_static:?}"
    );
    assert_eq!(
        s_grow.appended, 4,
        "3×auction=1 + auction=3 被 recheck 救援；auction=2 where 拒绝"
    );
    assert_eq!(idx_grow, idx_static);
    assert_batches_equal_rows(&b_static.finish(), &b_grow.finish());
}

/// float 左键热键同桶（Arc 修复的浮点分支）：f64→Int 截断后桶内逐行
/// `values_equal` 复核——1.5 截断为 1 进桶但复核拒绝，1.0 复核通过。
/// 热键重复 + 浮点复核组合，列式与行式必须字节一致。
#[test]
fn columnar_join_float_hot_key_matches_event_path() {
    use crate::match_engine::event_bridge::{ColumnarEvent, JoinRow, materialize_rows};
    use arrow::array::{ArrayRef, Float64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    let exec = each_join_plan_rule();
    let lookup = MockJoinLookup {
        rows: vec![JoinRow::Event(Arc::new(event(vec![
            ("id", num(1.0)),
            ("category", num(10.0)),
        ])))],
    };

    const NANOS: i64 = 1_750_000_000_000_000_000;
    // auction=1.5 ×3（截断进 id=1 桶，复核拒绝）+ auction=1.0 ×2（复核通过）。
    let schema = Arc::new(Schema::new(vec![Field::new(
        "auction",
        DataType::Float64,
        true,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Float64Array::from(vec![1.5, 1.5, 1.5, 1.0, 1.0])) as ArrayRef],
    )
    .unwrap();

    let events: Vec<Event> = materialize_rows(&batch, &[0, 1, 2, 3, 4]);
    let rows: Vec<(&Event, i64)> = events
        .iter()
        .enumerate()
        .map(|(i, ev)| (ev, NANOS + i as i64))
        .collect();
    let mut b_row = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut idx_row = Vec::new();
    let s_row =
        exec.execute_each_direct_batch(&rows, &lookup, &[], NANOS, &mut b_row, &mut idx_row);
    assert_eq!(s_row.appended, 2, "1.5×3 复核拒绝；1.0×2 通过");

    let col_events: Vec<ColumnarEvent<'_>> =
        (0..5).map(|r| ColumnarEvent::new(&batch, r)).collect();
    let col_rows: Vec<(&ColumnarEvent<'_>, i64)> = col_events
        .iter()
        .enumerate()
        .map(|(i, ev)| (ev, NANOS + i as i64))
        .collect();
    let mut b_col = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut idx_col = Vec::new();
    let s_col = exec.execute_each_direct_batch_columnar_join(
        &col_rows,
        &lookup,
        NANOS,
        &mut b_col,
        &mut idx_col,
    );
    assert_eq!(s_col, s_row);
    assert_eq!(idx_col, idx_row);
    assert_batches_equal_rows(&b_row.finish(), &b_col.finish());
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

    // where 引用**左窗**字段（驱动列，gap-3 2026-09-02）→ 列式放行：死
    // join 消除（where 不引用右窗）→ live_joins 空 → where 走驱动列守卫掩码。
    let mut plan = base();
    plan.r#where = Some(Expr::BinOp {
        op: BinOp::Gt,
        left: Box::new(Expr::Field(FieldRef::Qualified("b".into(), "price".into()))),
        right: Box::new(Expr::Number(5.0)),
    });
    assert!(
        RuleExecutor::new(plan).each_plan_columnar_safe(),
        "gap-3：无活 join + 可列式驱动列 where 必须放行"
    );

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
