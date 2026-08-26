//! 列式 join 富化对拍（2026-08-23）：真实窗口 + 索引 + RegistryLookup 下，
//! 列式 each+join 路径（`execute_each_direct_batch_columnar_join`）必须与行式
//! 路径（`execute_each_direct_batch`）输出逐位一致——包括 join 命中/miss、
//! 后置 where 过滤、右窗字段读（yield/where）。
use std::sync::Arc;

use std::collections::HashMap;

use arrow::array::{ArrayRef, Int64Array, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;

use wf_engine::alert::AlertColumnBuilder;
use wf_engine::match_engine::{Event, RuleExecutor};
use wf_engine::window::{Router, WindowDef, WindowParams, WindowRegistry};
use wf_lang::ast::{BinOp, Expr, FieldRef, JoinMode};
use wf_lang::plan::{
    BindPlan, EachPlan, EntityPlan, JoinCondPlan, JoinPlan, MatchPlan, RulePlan, ScorePlan,
    YieldField, YieldPlan,
};

use super::tests::{empty_tracked_bind_fields, empty_tracked_plain_fields};
use crate::engine_task::window_lookup::RegistryLookup;

const T: i64 = 1_700_000_000_000_000_000;

fn schema(extra: &[(&str, DataType)]) -> Arc<Schema> {
    let mut fields = vec![
        Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new("id", DataType::Int64, true),
    ];
    for (name, dt) in extra {
        fields.push(Field::new(*name, dt.clone(), true));
    }
    Arc::new(Schema::new(fields))
}

fn window_def(name: &str, schema: &Arc<Schema>) -> WindowDef {
    let mut cfg = super::tests::test_window_config(usize::MAX);
    cfg.name = name.to_string();
    WindowDef {
        params: WindowParams {
            name: name.to_string(),
            schema: schema.clone(),
            time_col_index: Some(schema.index_of("event_time").unwrap()),
            over: std::time::Duration::from_secs(3600),
            materialize_fields: None,
            defer_materialization: false,
        },
        streams: vec![name.to_string()],
        config: cfg,
    }
}

/// 驱动事件时间列：所有行共享 T + 行号（事件时间递增）。
fn time_col(n: usize) -> ArrayRef {
    Arc::new(TimestampNanosecondArray::from(
        (0..n as i64).map(|i| T + i).collect::<Vec<_>>(),
    ))
}

fn assert_batches_equal_rows(
    a: &wf_engine::alert::AlertColumnBatch,
    b: &wf_engine::alert::AlertColumnBatch,
) {
    assert_eq!(a.len(), b.len(), "row count");
    for row in 0..a.len() {
        let ra = a.iter_data_records().nth(row).unwrap().unwrap();
        let rb = b.iter_data_records().nth(row).unwrap().unwrap();
        assert_eq!(ra.items.len(), rb.items.len(), "row {row} field count");
        for (fa, fb) in ra.items.iter().zip(rb.items.iter()) {
            assert_eq!(fa.get_name(), fb.get_name(), "row {row} field name");
            assert_eq!(fa.get_value(), fb.get_value(), "row {row} field value");
        }
    }
}

/// q20 形状规则：`on each b` + `join auction_events snapshot on b.auction ==
/// auction_events.id` + `where auction_events.category == 10`，entity/yield 读
/// 左窗 b.auction + 右窗 auction_events.category。
fn q20_shape_executor() -> RuleExecutor {
    let plan = RulePlan {
        conv_window: None,
        name: "q20_shape".into(),
        binds: vec![BindPlan {
            alias: "b".into(),
            window: "bid_events".into(),
            filter: None,
        }],
        lets: Vec::new(),
        match_plan: MatchPlan {
            keys: vec![],
            key_map: None,
            key_join: None,
            window_spec: wf_lang::plan::WindowSpec::Fixed(std::time::Duration::ZERO),
            event_steps: vec![],
            close_steps: vec![],
            close_mode: wf_lang::ast::CloseMode::Or,
            tracked_bind_aliases: std::collections::HashSet::new(),
            tracked_bind_fields: empty_tracked_bind_fields(),
            tracked_plain_fields: empty_tracked_plain_fields(),
            seq: None,
            match_mode: wf_lang::ast::MatchMode::Seq,
            accu: false,
            needs_field_history: false,
            trigger_event_needed: false,
        },
        each_plan: Some(EachPlan {
            alias: "b".into(),
            filter: None,
        }),
        stats_plan: None,
        joins: vec![JoinPlan {
            right_window: "auction_events".to_string(),
            mode: JoinMode::Snapshot,
            conds: vec![JoinCondPlan {
                left: FieldRef::Qualified("b".into(), "auction".into()),
                right: FieldRef::Qualified("auction_events".into(), "id".into()),
            }],
            within: None,
            reduce: None,
            emit_at: None,
        }],
        r#where: Some(Expr::BinOp {
            op: BinOp::Eq,
            left: Box::new(Expr::Field(FieldRef::Qualified(
                "auction_events".into(),
                "category".into(),
            ))),
            right: Box::new(Expr::Number(10.0)),
        }),
        entity_plan: EntityPlan {
            entity_type: "digit".into(),
            entity_id_expr: Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
        },
        yield_plan: YieldPlan {
            target: "alerts".into(),
            version: None,
            fields: vec![
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
            ],
        },
        score_plan: ScorePlan {
            expr: Expr::Number(10.0),
        },
        pattern_origin: None,
        conv_plan: None,
        limits_plan: None,
    };
    RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([
            (
                "id".into(),
                wf_lang::FieldType::Base(wf_lang::BaseType::Digit),
            ),
            (
                "category".into(),
                wf_lang::FieldType::Base(wf_lang::BaseType::Digit),
            ),
        ]),
    )
}

/// q13b 形状规则：`on each m` + `join side_input snapshot on m.mod_key ==
/// side_input.key`，yield `detail = fmt("{}", side_input.value)`（fmt 单参数
/// 恒等——2026-08-25 q13b 列式化）。
fn q13b_shape_executor() -> RuleExecutor {
    let plan = RulePlan {
        conv_window: None,
        name: "q13b_shape".into(),
        binds: vec![BindPlan {
            alias: "m".into(),
            window: "bid_mod".into(),
            filter: None,
        }],
        lets: Vec::new(),
        match_plan: MatchPlan {
            keys: vec![],
            key_map: None,
            key_join: None,
            window_spec: wf_lang::plan::WindowSpec::Fixed(std::time::Duration::ZERO),
            event_steps: vec![],
            close_steps: vec![],
            close_mode: wf_lang::ast::CloseMode::Or,
            tracked_bind_aliases: std::collections::HashSet::new(),
            tracked_bind_fields: empty_tracked_bind_fields(),
            tracked_plain_fields: empty_tracked_plain_fields(),
            seq: None,
            match_mode: wf_lang::ast::MatchMode::Seq,
            accu: false,
            needs_field_history: false,
            trigger_event_needed: false,
        },
        each_plan: Some(EachPlan {
            alias: "m".into(),
            filter: None,
        }),
        stats_plan: None,
        joins: vec![JoinPlan {
            right_window: "side_input".to_string(),
            mode: JoinMode::Snapshot,
            conds: vec![JoinCondPlan {
                left: FieldRef::Qualified("m".into(), "mod_key".into()),
                right: FieldRef::Qualified("side_input".into(), "key".into()),
            }],
            within: None,
            reduce: None,
            emit_at: None,
        }],
        r#where: None,
        entity_plan: EntityPlan {
            entity_type: "digit".into(),
            entity_id_expr: Expr::Field(FieldRef::Qualified("m".into(), "bidder".into())),
        },
        yield_plan: YieldPlan {
            target: "alerts".into(),
            version: None,
            fields: vec![
                YieldField {
                    name: "id".into(),
                    value: Expr::Field(FieldRef::Qualified("m".into(), "bidder".into())),
                },
                YieldField {
                    name: "alert_type".into(),
                    value: Expr::StringLit("q13_sidejoin".into()),
                },
                // fmt("{}", side_input.value)：Str 右窗字段恒等（q13b 列式化）。
                YieldField {
                    name: "detail".into(),
                    value: Expr::FuncCall {
                        qualifier: None,
                        name: "fmt".into(),
                        args: vec![
                            Expr::StringLit("{}".into()),
                            Expr::Field(FieldRef::Qualified("side_input".into(), "value".into())),
                        ],
                    },
                },
                YieldField {
                    name: "request_count".into(),
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
    };
    RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([
            (
                "id".into(),
                wf_lang::FieldType::Base(wf_lang::BaseType::Digit),
            ),
            (
                "alert_type".into(),
                wf_lang::FieldType::Base(wf_lang::BaseType::Chars),
            ),
            (
                "detail".into(),
                wf_lang::FieldType::Base(wf_lang::BaseType::Chars),
            ),
            (
                "request_count".into(),
                wf_lang::FieldType::Base(wf_lang::BaseType::Digit),
            ),
        ]),
    )
}

/// 集成对拍：真实 auction 窗口（索引）+ bid 驱动批 + RegistryLookup。
/// 场景：命中+category=10（输出）、命中+category≠10（where 拒绝）、miss
/// （bid 引用不存在的 auction → 无富化 → where 拒绝）。
#[test]
fn columnar_join_matches_row_path_with_real_window_index() {
    use wf_engine::match_engine::event_bridge::{ColumnarEvent, materialize_rows};

    // auction_events：id → category（索引 join 目标）。
    let auction_schema = schema(&[("category", DataType::Int64), ("price", DataType::Int64)]);
    let bid_schema = Arc::new(Schema::new(vec![
        Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new("auction", DataType::Int64, true),
        Field::new("bidder", DataType::Int64, true),
        Field::new("price", DataType::Int64, true),
    ]));

    let registry = WindowRegistry::build(vec![
        window_def("auction_events", &auction_schema),
        window_def("bid_events", &bid_schema),
    ])
    .unwrap();
    let auction_win = registry.get_window("auction_events").unwrap();
    // 建 join 索引（key = id）→ 存量 append 也入索引。
    auction_win.set_join_key("id".to_string());

    // auction 行：id=1 cat=10、id=2 cat=20、id=3 cat=10、id=4 cat=10。
    let auc_cols: Vec<ArrayRef> = vec![
        time_col(4),
        Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
        Arc::new(Int64Array::from(vec![10, 20, 10, 10])),
        Arc::new(Int64Array::from(vec![100, 200, 300, 400])),
    ];
    let auc_batch = RecordBatch::try_new(auction_schema, auc_cols).unwrap();
    auction_win
        .append_with_watermark_sized(auc_batch, 1024, None)
        .unwrap();

    // bid 驱动批：auction=1（命中 cat=10 输出）、auction=2（命中 cat≠10 拒绝）、
    // auction=3（命中 cat=10 输出）、auction=99（miss → 无富化 → where 拒绝）、
    // auction=4（命中 cat=10 输出）。
    let bid_batch = RecordBatch::try_new(
        bid_schema.clone(),
        vec![
            time_col(5),
            Arc::new(Int64Array::from(vec![1, 2, 3, 99, 4])),
            Arc::new(Int64Array::from(vec![100, 200, 300, 400, 500])),
            Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50])),
        ],
    )
    .unwrap();
    let (_, bid_seq) = bid_win_append(&registry, bid_batch.clone());

    let router = Arc::new(Router::new(registry));
    let lookup = RegistryLookup::with_source_watermark(&router, Some(bid_seq), "bid_events");
    let exec = q20_shape_executor();
    assert!(
        exec.each_plan_columnar_safe() && exec.each_join_columnar_ready(),
        "q20 形状必须列式 join 支持"
    );

    const NANOS: i64 = 1_750_000_000_000_000_000;
    // 行式参照。
    let events: Vec<Event> = materialize_rows(&bid_batch, &[0, 1, 2, 3, 4]);
    let rows: Vec<(&Event, i64)> = events
        .iter()
        .enumerate()
        .map(|(i, ev)| (ev, NANOS + i as i64))
        .collect();
    let mut via_events = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut idx = Vec::new();
    let stats =
        exec.execute_each_direct_batch(&rows, &lookup, &[], NANOS, &mut via_events, &mut idx);
    assert_eq!(stats.appended, 3, "行式：auction 1/3/4 命中且 category=10");
    assert_eq!(
        stats.rejected, 2,
        "行式：auction 2 where 拒绝 + auction 99 miss 拒绝"
    );

    // 列式 join 路径。
    let col_events: Vec<ColumnarEvent<'_>> =
        (0..5).map(|r| ColumnarEvent::new(&bid_batch, r)).collect();
    let col_rows: Vec<(&ColumnarEvent<'_>, i64)> = col_events
        .iter()
        .enumerate()
        .map(|(i, ev)| (ev, NANOS + i as i64))
        .collect();
    let mut via_columnar = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut idx_c = Vec::new();
    let stats_c = exec.execute_each_direct_batch_columnar_join(
        &col_rows,
        &lookup,
        NANOS,
        &mut via_columnar,
        &mut idx_c,
    );
    assert_eq!(stats_c, stats, "列式/行式 stats 一致");
    assert_eq!(idx_c, idx);
    assert_batches_equal_rows(&via_events.finish(), &via_columnar.finish());
}

/// q13b 列式化对拍（2026-08-25）：`detail = fmt("{}", side_input.value)`
/// 单参数恒等——列式 join 路径（读右窗 Str 字段透传）vs 行式路径（Event
/// clone + join + fmt 解释）输出**字节一致**。含 miss 行（无富化 → 空串）与
/// 非 Str 右窗值（fmt 语义 value_to_string 渲染）分支。
#[test]
fn columnar_join_fmt_identity_matches_row_path() {
    use wf_engine::match_engine::event_bridge::{ColumnarEvent, materialize_rows};

    // side_input：key（Int64 索引）+ value（Utf8 字符串）。
    let side_schema = Arc::new(Schema::new(vec![
        Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new("key", DataType::Int64, true),
        Field::new("value", DataType::Utf8, true),
    ]));
    let bid_mod_schema = Arc::new(Schema::new(vec![
        Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new("bidder", DataType::Int64, true),
        Field::new("mod_key", DataType::Int64, true),
    ]));

    let registry = WindowRegistry::build(vec![
        window_def("side_input", &side_schema),
        window_def("bid_mod", &bid_mod_schema),
    ])
    .unwrap();
    let side_win = registry.get_window("side_input").unwrap();
    side_win.set_join_key("key".to_string());

    // side_input 行：key=5 value="v5"、key=6 value="v6"（Str 右窗值）。
    let side_cols: Vec<ArrayRef> = vec![
        time_col(2),
        Arc::new(Int64Array::from(vec![5, 6])),
        Arc::new(arrow::array::StringArray::from(vec!["v5", "v6"])),
    ];
    let side_batch = RecordBatch::try_new(side_schema, side_cols).unwrap();
    side_win
        .append_with_watermark_sized(side_batch, 1024, None)
        .unwrap();

    // bid_mod 驱动批：mod_key=5（命中 v5）、mod_key=6（命中 v6）、mod_key=99
    // （miss → 空串 detail）、mod_key=5（重复命中）。
    let bid_mod_batch = RecordBatch::try_new(
        bid_mod_schema.clone(),
        vec![
            time_col(4),
            Arc::new(Int64Array::from(vec![100, 200, 300, 400])),
            Arc::new(Int64Array::from(vec![5, 6, 99, 5])),
        ],
    )
    .unwrap();
    let (_, bid_mod_seq) = bid_mod_win_append(&registry, bid_mod_batch.clone());

    let router = Arc::new(Router::new(registry));
    let lookup = RegistryLookup::with_source_watermark(&router, Some(bid_mod_seq), "bid_mod");
    let exec = q13b_shape_executor();
    assert!(
        exec.each_plan_columnar_safe() && exec.each_join_columnar_ready(),
        "q13b fmt 单参数形状必须列式 join 支持（gate 放开后）"
    );

    const NANOS: i64 = 1_750_000_000_000_000_000;
    // 行式参照。
    let events: Vec<Event> = materialize_rows(&bid_mod_batch, &[0, 1, 2, 3]);
    let rows: Vec<(&Event, i64)> = events
        .iter()
        .enumerate()
        .map(|(i, ev)| (ev, NANOS + i as i64))
        .collect();
    let mut via_events = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut idx = Vec::new();
    let stats =
        exec.execute_each_direct_batch(&rows, &lookup, &[], NANOS, &mut via_events, &mut idx);
    assert_eq!(
        stats.appended, 4,
        "行式：snapshot 外联——5/6 命中富化 + 99 miss 保留（detail 空串）"
    );

    // 列式 join 路径。
    let col_events: Vec<ColumnarEvent<'_>> = (0..4)
        .map(|r| ColumnarEvent::new(&bid_mod_batch, r))
        .collect();
    let col_rows: Vec<(&ColumnarEvent<'_>, i64)> = col_events
        .iter()
        .enumerate()
        .map(|(i, ev)| (ev, NANOS + i as i64))
        .collect();
    let mut via_columnar = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut idx_c = Vec::new();
    let stats_c = exec.execute_each_direct_batch_columnar_join(
        &col_rows,
        &lookup,
        NANOS,
        &mut via_columnar,
        &mut idx_c,
    );
    assert_eq!(stats_c, stats, "列式/行式 stats 一致（fmt 恒等）");
    assert_eq!(idx_c, idx);
    assert_batches_equal_rows(&via_events.finish(), &via_columnar.finish());
}

fn bid_mod_win_append(registry: &WindowRegistry, batch: RecordBatch) -> (u64, u64) {
    let win = registry.get_window("bid_mod").unwrap();
    let (outcome, seq) = win.append_with_watermark_sized(batch, 1024, None).unwrap();
    assert!(matches!(
        outcome,
        wf_engine::window::AppendOutcome::Appended
    ));
    (seq, seq)
}

fn bid_win_append(registry: &WindowRegistry, batch: RecordBatch) -> (u64, u64) {
    let win = registry.get_window("bid_events").unwrap();
    let (outcome, seq) = win.append_with_watermark_sized(batch, 1024, None).unwrap();
    assert!(matches!(
        outcome,
        wf_engine::window::AppendOutcome::Appended
    ));
    (seq, seq)
}
