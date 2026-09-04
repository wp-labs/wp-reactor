//! direct_tests.rs 拆出的列式无 join fast path 测试（2026-09-04；`#[path]`
//! 子模块，经父模块 `use super::*` 复用其导入）。
//!
//! 覆盖：`execute_each_direct_batch_columnar`（不经逐行 `Event` 物化、直接从
//! Arrow 列读字段）与 eager `materialize_rows` + 批路径的字节级对拍——含 2^53
//! f64 往返 lane、numeric/utf8 实体快车道与共享列 yield 复用、常量 yield 批级
//! 缓存（Q1 形状）、null/missing 实体失败的逐位一致语义。

use super::*;

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
