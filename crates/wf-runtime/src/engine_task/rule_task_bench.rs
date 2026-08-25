//! q13a 中间窗生产路径微基准（2026-08-25，数据驱动定位用）。
//!
//! 背景：q13a（`on each b` → yield 中间窗 bid_mod，含 `mod_key = auction % 10000`
//! BinOp）分片后仍是 100M q13 瓶颈——10 核总吞吐仅 ~692k/s（每行 ~14µs），
//! 远超合理值（q13b 的 row path join 才 ~2.5µs/行）。本基准在同一进程内直接
//! 测 q13a 生产路径的**逐段成本**（非猜测）：
//!
//!   ① per-record 求值（`execute_each_with_joins` → OutputRecord）——中间窗
//!      each 无批量路径，走每行 OutputRecord；
//!   ② 中间窗装载（`PipeBatchStager::push_record`，含 `record_window_fields`
//!      的字段查找）；
//!   ③ 对照：批量路径（`execute_each_direct_batch`，sink 形态）——量化
//!      「intermediate 无批量路径」的代价；
//!   ④ 对照：无 staging 的裸 on-each（`execute_each_direct`）。
//!
//! 运行：
//!   cargo test --release -p wf-runtime q13a_pipe_bench -- --ignored --nocapture
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use wf_lang::ast::{Expr, FieldRef};
use wf_lang::plan::{
    BindPlan, EachPlan, EntityPlan, MatchPlan, RulePlan, ScorePlan, YieldField, YieldPlan,
};

use wf_engine::match_engine::event_bridge::batch_to_events;
use wf_engine::match_engine::{RuleExecutor, WindowLookup};

use super::{OutputRecord, PipeBatchStager};
use crate::engine_task::tests::empty_tracked_bind_fields;

const N: usize = 100_000;
const NANOS: i64 = 1_750_000_000_000_000_000;

/// bid_events 形状批（q13a 读 auction/bidder/price/dateTime/channel/url）。
fn bid_batch(n: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, true),
        Field::new("bidder", DataType::Int64, true),
        Field::new("price", DataType::Int64, true),
        Field::new(
            "dateTime",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new("channel", DataType::Utf8, true),
        Field::new("url", DataType::Utf8, true),
    ]));
    let auction: Vec<i64> = (0..n as i64).map(|i| i * 7).collect();
    let bidder: Vec<i64> = (0..n as i64).map(|i| i % 100_000).collect();
    let price: Vec<i64> = (0..n as i64).map(|i| (i * 37) % 1_000_000).collect();
    let date_time: Vec<i64> = (0..n as i64).map(|i| NANOS + i).collect();
    let channel: Vec<&str> = vec!["G"; n];
    let url: Vec<&str> = vec!["https://x/y/z"; n];
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(auction)) as ArrayRef,
            Arc::new(Int64Array::from(bidder)),
            Arc::new(Int64Array::from(price)),
            Arc::new(arrow::array::TimestampNanosecondArray::from(date_time)),
            Arc::new(arrow::array::StringArray::from(channel)),
            Arc::new(arrow::array::StringArray::from(url)),
        ],
    )
    .unwrap()
}

/// q13a 形状的 RulePlan（`on each b` + entity(digit, b.bidder) + yield
/// bid_mod（5 个 Field + 1 个 mod BinOp））——executor 与边缘对拍共用。
fn q13a_plan_rule_plan() -> RulePlan {
    RulePlan {
        conv_window: None,
        name: "q13a_bench".into(),
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
            tracked_bind_fields: std::collections::HashMap::new(),
            tracked_plain_fields: std::collections::HashSet::new(),
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
        joins: vec![],
        r#where: None,
        entity_plan: EntityPlan {
            entity_type: "digit".into(),
            entity_id_expr: Expr::Field(FieldRef::Qualified("b".into(), "bidder".into())),
        },
        yield_plan: YieldPlan {
            target: "bid_mod".into(),
            version: None,
            fields: vec![
                YieldField {
                    name: "id".into(),
                    value: Expr::Field(FieldRef::Qualified("b".into(), "bidder".into())),
                },
                YieldField {
                    name: "bidder".into(),
                    value: Expr::Field(FieldRef::Qualified("b".into(), "bidder".into())),
                },
                YieldField {
                    name: "auction".into(),
                    value: Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
                },
                YieldField {
                    name: "price".into(),
                    value: Expr::Field(FieldRef::Qualified("b".into(), "price".into())),
                },
                YieldField {
                    name: "dateTime".into(),
                    value: Expr::Field(FieldRef::Qualified("b".into(), "dateTime".into())),
                },
                YieldField {
                    name: "mod_key".into(),
                    value: Expr::BinOp {
                        op: wf_lang::ast::BinOp::Mod,
                        left: Box::new(Expr::Field(FieldRef::Qualified(
                            "b".into(),
                            "auction".into(),
                        ))),
                        right: Box::new(Expr::Number(10000.0)),
                    },
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

/// q13a 形状的 RuleExecutor：`on each b` + entity(digit, b.bidder) + yield
/// bid_mod（5 个 Field + 1 个 mod BinOp）。
fn q13a_plan_rule() -> RuleExecutor {
    RuleExecutor::new(q13a_plan_rule_plan())
}

/// 空 WindowLookup（q13a 无 join，不查询窗口）。
struct NoLookup;
impl WindowLookup for NoLookup {
    fn snapshot_field_values(
        &self,
        _w: &str,
        _f: &str,
    ) -> Option<std::collections::HashSet<String>> {
        None
    }
    fn snapshot(&self, _w: &str) -> Option<Vec<wf_engine::match_engine::JoinRow>> {
        None
    }
}

/// bid_mod 中间窗 schema（q13a yield 目标：id/bidder/auction/price/dateTime/mod_key）。
fn bid_mod_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("bidder", DataType::Int64, true),
        Field::new("auction", DataType::Int64, true),
        Field::new("price", DataType::Int64, true),
        Field::new(
            "dateTime",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new("mod_key", DataType::Int64, true),
    ]))
}

fn report(name: &str, per_ns: f64, baseline_ns: f64) {
    let mps = 1e9 / per_ns / 1e6;
    eprintln!(
        "[q13a-pipe-bench] {:<34} {:>9.1} ns/row  ({:>7.2}M rows/s)  = {:>6.1}% of baseline",
        name,
        per_ns,
        mps,
        per_ns / baseline_ns * 100.0
    );
}

#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-runtime q13a_pipe_bench -- --ignored --nocapture"]
fn q13a_pipe_bench() {
    let exec = q13a_plan_rule();
    let lookup = NoLookup;

    let batch = bid_batch(N);
    let events: Vec<Arc<wf_engine::match_engine::Event>> =
        batch_to_events(&batch).into_iter().map(Arc::new).collect();

    // ---- ⓪ 物化：batch_to_events（process_batch 的 eager_events 构造，
    // 每批全量物化 Event HashMap——q13a 是 row path，必须物化） ----
    let start = Instant::now();
    for _ in 0..8 {
        let _ = batch_to_events(&batch);
    }
    let materialize_ns = start.elapsed().as_nanos() as f64 / (N as f64 * 8.0);

    // 每个字段的排序（wfx_id hashing 用）——模拟 rule_task 的 each_field_order。
    let first = &events[0];
    let mut field_order: Vec<&smol_str::SmolStr> = first.fields.keys().collect();
    field_order.sort_unstable();

    // ---- ① per-record 求值：execute_each_with_joins → OutputRecord ----
    // 每行产出 OutputRecord（q13a 实际路径：intermediate target 无批量路径）。
    let start = Instant::now();
    let mut records: Vec<OutputRecord> = Vec::with_capacity(N);
    for ev in &events {
        if let Ok(Some(record)) =
            exec.execute_each_with_joins(ev, NANOS, &lookup, &field_order, NANOS)
        {
            records.push(record);
        }
    }
    let per_record_ns = start.elapsed().as_nanos() as f64 / N as f64;

    // ---- ② 中间窗装载：PipeBatchStager::push_record ----
    let mut stager = PipeBatchStager::new(
        Arc::from("bid_mod"),
        bid_mod_schema(),
        Some(4), // dateTime 列
    );
    let start = Instant::now();
    for r in &records {
        stager.push_record(r).expect("stage row");
    }
    let stage_ns = start.elapsed().as_nanos() as f64 / N as f64;

    // ---- 组合 = q13a 每行总成本 ----
    let total_ns = per_record_ns + stage_ns;

    // ---- ③ 对照：execute_each_direct（直写 builder，无 OutputRecord）----
    let mut builder = wf_engine::alert::AlertColumnBuilder::new(Arc::from("alerts"));
    let start = Instant::now();
    for ev in &events {
        let _ = exec.execute_each_direct(ev, NANOS, &lookup, &field_order, NANOS, &mut builder);
    }
    let direct_ns = start.elapsed().as_nanos() as f64 / N as f64;

    // ---- ④ 对照：批量路径 execute_each_direct_batch ----
    let mut builder = wf_engine::alert::AlertColumnBuilder::new(Arc::from("alerts"));
    let mut appended = Vec::new();
    let rows: Vec<(&wf_engine::match_engine::Event, i64)> =
        events.iter().map(|e| (e.as_ref(), NANOS)).collect();
    let start = Instant::now();
    for chunk in rows.chunks(4096) {
        let _ = exec.execute_each_direct_batch(
            chunk,
            &lookup,
            &field_order,
            NANOS,
            &mut builder,
            &mut appended,
        );
    }
    let batch_ns = start.elapsed().as_nanos() as f64 / N as f64;

    eprintln!("[q13a-pipe-bench] N = {N}, records = {}", records.len());
    report("⓪ 物化 batch_to_events", materialize_ns, total_ns);
    report(
        "① per-record (execute_each_with_joins)",
        per_record_ns,
        total_ns,
    );
    report("② stage (PipeBatchStager::push_record)", stage_ns, total_ns);
    report(
        "q13a process_batch 每行合计 (⓪+①+②)",
        materialize_ns + total_ns,
        total_ns,
    );
    report("q13a 每行合计 (①+②)", total_ns, total_ns);
    report(
        "对照: execute_each_direct (无 OutputRecord)",
        direct_ns,
        total_ns,
    );
    report("对照: execute_each_direct_batch (批量)", batch_ns, total_ns);
    eprintln!(
        "[q13a-pipe-bench] 批量路径 vs q13a 生产路径(含物化) = {:.1}x",
        (materialize_ns + total_ns) / batch_ns
    );
    // 防御断言：批量路径应显著快于 per-record+stage（>1.5x）。
    assert!(
        total_ns > batch_ns * 1.5,
        "批量路径应显著快于 per-record+stage：total={total_ns:.1}ns batch={batch_ns:.1}ns"
    );
    let _ = empty_tracked_bind_fields();
}

/// q13a 中间窗装载的 **列式路径** 微基准（2026-08-25 q13a 列式化）：
/// `execute_each_pipe_batch_columnar`（零 Event/OutputRecord 物化）+ `push_row`
/// （预计算列来源计划）。与 ①② 对照：物化 246ns + per-record 526ns + stage 476ns
/// = 1248ns/行 的 row path，应降至几百 ns 量级。
#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-runtime q13a_pipe_bench -- --ignored --nocapture"]
fn q13a_pipe_columnar_bench() {
    let exec = q13a_plan_rule();
    assert!(
        exec.each_pipe_columnar_safe(),
        "q13a 形状必须满足 pipe 列式门控"
    );
    let batch = bid_batch(N);
    let col_events: Vec<wf_engine::match_engine::ColumnarEvent<'_>> = (0..N)
        .map(|i| wf_engine::match_engine::ColumnarEvent::new(&batch, i))
        .collect();
    let rows: Vec<(&wf_engine::match_engine::ColumnarEvent<'_>, i64)> =
        col_events.iter().map(|ev| (ev, NANOS)).collect();
    let yield_names: Vec<std::sync::Arc<str>> = exec
        .plan()
        .yield_plan
        .fields
        .iter()
        .map(|f| std::sync::Arc::from(f.name.as_str()))
        .collect();

    // ⑤ 求值 + 装载（列式全链）。
    let start = Instant::now();
    let mut total_appended = 0usize;
    for _ in 0..4 {
        let prepared = exec.each_batch_prepare(&batch);
        let mut out: Vec<wf_engine::match_engine::PipeEachRow> = Vec::with_capacity(N);
        let mut stager = PipeBatchStager::new_columnar(
            Arc::from("bid_mod"),
            bid_mod_schema(),
            Some(4),
            &yield_names,
        );
        let stats = exec.execute_each_pipe_batch_columnar(&rows, &prepared, &mut out);
        for row in &out {
            stager
                .push_row("q13a_bench", row, NANOS)
                .expect("stage row");
        }
        total_appended += stats.appended;
    }
    let columnar_ns = start.elapsed().as_nanos() as f64 / (N as f64 * 4.0);
    eprintln!(
        "[q13a-pipe-bench] N = {N}, columnar appended = {total_appended} (expect {})",
        N * 4
    );
    report("⑤ columnar pipe (eval+push_row)", columnar_ns, columnar_ns);
    eprintln!(
        "[q13a-pipe-bench] 列式路径 vs row path(含物化) = {:.1}x",
        (246.0 + 526.0 + 476.0) / columnar_ns
    );
    let _ = empty_tracked_bind_fields();
}

/// q13a 双路径 对拍（2026-08-25 q13a 列式化正确性锁）：row path
/// （`execute_each_with_joins` → `push_record`）与 columnar pipe path
/// （`execute_each_pipe_batch_columnar` → `push_row`）产出的中间窗批次必须
/// **字节一致**——含 meta 回退列（`__wfu_*`）与 `__wf_pipe_ts` 事件时间列。
#[test]
fn q13a_pipe_columnar_matches_row_path() {
    use arrow::array::Array;
    let exec = q13a_plan_rule();
    let batch = bid_batch(N);
    let events: Vec<std::sync::Arc<wf_engine::match_engine::Event>> = batch_to_events(&batch)
        .into_iter()
        .map(std::sync::Arc::new)
        .collect();
    let first = &events[0];
    let mut field_order: Vec<&smol_str::SmolStr> = first.fields.keys().collect();
    field_order.sort_unstable();
    let lookup = NoLookup;

    // 含 meta 回退列 + 事件时间列的中间窗 schema（bid_mod 字段 + __wfu_* + ts）。
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("bidder", DataType::Int64, true),
        Field::new("auction", DataType::Int64, true),
        Field::new("price", DataType::Int64, true),
        Field::new(
            "dateTime",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new("mod_key", DataType::Int64, true),
        Field::new("__wfu_rule_name", DataType::Utf8, true),
        Field::new("__wfu_score", DataType::Float64, true),
        Field::new("__wfu_entity_type", DataType::Utf8, true),
        Field::new("__wfu_entity_id", DataType::Utf8, true),
        Field::new(
            "__wf_pipe_ts",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            true,
        ),
    ]));

    // Row path.
    let mut row_stager = PipeBatchStager::new(Arc::from("bid_mod"), Arc::clone(&schema), Some(4));
    for ev in &events {
        let record = exec
            .execute_each_with_joins(ev, NANOS, &lookup, &field_order, NANOS)
            .expect("eval")
            .expect("q13a 无 filter → 必有输出");
        row_stager.push_record(&record).expect("stage");
    }
    let (_, _, row_batch) = row_stager.take_events().expect("build").expect("non-empty");

    // Columnar pipe path.
    let prepared = exec.each_batch_prepare(&batch);
    let col_events: Vec<wf_engine::match_engine::ColumnarEvent<'_>> = (0..N)
        .map(|i| wf_engine::match_engine::ColumnarEvent::new(&batch, i))
        .collect();
    let rows: Vec<(&wf_engine::match_engine::ColumnarEvent<'_>, i64)> =
        col_events.iter().map(|ev| (ev, NANOS)).collect();
    let mut out: Vec<wf_engine::match_engine::PipeEachRow> = Vec::with_capacity(N);
    let stats = exec.execute_each_pipe_batch_columnar(&rows, &prepared, &mut out);
    assert_eq!(stats.appended, N, "无 filter → 全行输出");
    let yield_names: Vec<std::sync::Arc<str>> = exec
        .plan()
        .yield_plan
        .fields
        .iter()
        .map(|f| std::sync::Arc::from(f.name.as_str()))
        .collect();
    let mut col_stager = PipeBatchStager::new_columnar(
        Arc::from("bid_mod"),
        Arc::clone(&schema),
        Some(4),
        &yield_names,
    );
    for row in &out {
        col_stager
            .push_row("q13a_bench", row, NANOS)
            .expect("stage");
    }
    let (_, _, col_batch) = col_stager.take_events().expect("build").expect("non-empty");

    assert_eq!(row_batch.num_rows(), col_batch.num_rows());
    assert_eq!(row_batch.num_columns(), col_batch.num_columns());
    for (i, (a, b)) in row_batch
        .columns()
        .iter()
        .zip(col_batch.columns().iter())
        .enumerate()
    {
        assert_eq!(
            a.len(),
            b.len(),
            "col {i} ({}): 长度一致",
            schema.field(i).name()
        );
        for row in 0..row_batch.num_rows() {
            assert_eq!(
                a.is_null(row),
                b.is_null(row),
                "col {i} ({}): row {row} null 位一致",
                schema.field(i).name()
            );
            if !a.is_null(row) {
                assert_eq!(
                    arrow::util::display::array_value_to_string(a, row).expect("display"),
                    arrow::util::display::array_value_to_string(b, row).expect("display"),
                    "col {i} ({}): row {row} 值一致",
                    schema.field(i).name()
                );
            }
        }
    }
    let _ = empty_tracked_bind_fields();
}

/// q13a 双路径 对拍 **边界用例**（2026-08-25 review R1/R4 补盲）：row path vs
/// pipe 列式路径在以下边界的字节一致性——
/// - **null 输入行**（auction/bidder 为 null → Field 缺失 → 空串→coerce→省略/空 cell）；
/// - **负值 mod**（auction=-7 → cvec `int_mod` i64 取模 vs 解释器 f64 取模）；
/// - **schema 时间列回退**（yield 未提供 time_fallback 列 → 回退 event_time_nanos）；
/// - **Missing 源列**（schema 列不在 yield/meta → null cell）；
/// - **meta 列**（`__wfu_entity_id`/`__wfu_score`——null entity 的渲染一致）。
#[test]
fn q13a_pipe_columnar_matches_row_path_edge_cases() {
    use arrow::array::Array;

    // 边缘输入批：auction 含 null 与负值，bidder 含 null。
    let schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, true),
        Field::new("bidder", DataType::Int64, true),
        Field::new("price", DataType::Int64, true),
        Field::new(
            "dateTime",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new("note", DataType::Utf8, true),
    ]));
    let auction = vec![Some(-7i64), Some(0), Some(12345), None, Some(8)];
    let bidder = vec![Some(5i64), None, Some(9), Some(2), Some(3)];
    let price = vec![Some(100i64), Some(200), Some(300), Some(400), Some(500)];
    let date_time: Vec<i64> = (0..5).map(|i| NANOS + i).collect();
    let note = vec!["a", "b", "c", "d", "e"];
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(arrow::array::Int64Array::from(auction)) as ArrayRef,
            Arc::new(arrow::array::Int64Array::from(bidder)),
            Arc::new(arrow::array::Int64Array::from(price)),
            Arc::new(arrow::array::TimestampNanosecondArray::from(date_time)),
            Arc::new(arrow::array::StringArray::from(note)),
        ],
    )
    .unwrap();

    // 边缘计划：id=bidder、mod_key=auction%10000、note=note（Field Utf8）。
    let mut plan = q13a_plan_rule_plan();
    plan.yield_plan.fields = vec![
        wf_lang::plan::YieldField {
            name: "id".into(),
            value: Expr::Field(FieldRef::Qualified("b".into(), "bidder".into())),
        },
        wf_lang::plan::YieldField {
            name: "mod_key".into(),
            value: Expr::BinOp {
                op: wf_lang::ast::BinOp::Mod,
                left: Box::new(Expr::Field(FieldRef::Qualified(
                    "b".into(),
                    "auction".into(),
                ))),
                right: Box::new(Expr::Number(10000.0)),
            },
        },
        wf_lang::plan::YieldField {
            name: "note".into(),
            value: Expr::Field(FieldRef::Qualified("b".into(), "note".into())),
        },
    ];
    let exec = RuleExecutor::new(plan);
    assert!(
        exec.each_pipe_columnar_safe(),
        "edge 形状必须过 pipe 列式门控"
    );

    // 边缘中间窗 schema：yield 三列 + 时间列回退（time_fallback，yield 未提供）
    // + Missing 源列（missing_col）+ meta 列（entity_id/score）。
    let pipe_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("mod_key", DataType::Int64, true),
        Field::new("note", DataType::Utf8, true),
        Field::new(
            "time_fallback",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new("missing_col", DataType::Int64, true),
        Field::new("__wfu_entity_id", DataType::Utf8, true),
        Field::new("__wfu_score", DataType::Float64, true),
    ]));
    let time_col_index = Some(3); // time_fallback 是 schema 时间列
    let events: Vec<std::sync::Arc<wf_engine::match_engine::Event>> = batch_to_events(&batch)
        .into_iter()
        .map(std::sync::Arc::new)
        .collect();
    let first = &events[0];
    let mut field_order: Vec<&smol_str::SmolStr> = first.fields.keys().collect();
    field_order.sort_unstable();
    let lookup = NoLookup;

    // Row path.
    let mut row_stager = PipeBatchStager::new(
        Arc::from("edge_pipe"),
        Arc::clone(&pipe_schema),
        time_col_index,
    );
    for (i, ev) in events.iter().enumerate() {
        let record = exec
            .execute_each_with_joins(ev, NANOS + i as i64, &lookup, &field_order, NANOS)
            .expect("eval")
            .expect("无 filter → 必有输出");
        row_stager.push_record(&record).expect("stage");
    }
    let (_, _, row_batch) = row_stager.take_events().expect("build").expect("non-empty");

    // Columnar pipe path.
    let prepared = exec.each_batch_prepare(&batch);
    let col_events: Vec<wf_engine::match_engine::ColumnarEvent<'_>> = (0..batch.num_rows())
        .map(|i| wf_engine::match_engine::ColumnarEvent::new(&batch, i))
        .collect();
    let rows: Vec<(&wf_engine::match_engine::ColumnarEvent<'_>, i64)> = col_events
        .iter()
        .enumerate()
        .map(|(i, ev)| (ev, NANOS + i as i64))
        .collect();
    let mut out: Vec<wf_engine::match_engine::PipeEachRow> = Vec::new();
    let stats = exec.execute_each_pipe_batch_columnar(&rows, &prepared, &mut out);
    assert_eq!(stats.appended, 5, "无 filter → 全行输出（含 null 行）");
    let yield_names: Vec<std::sync::Arc<str>> = exec
        .plan()
        .yield_plan
        .fields
        .iter()
        .map(|f| std::sync::Arc::from(f.name.as_str()))
        .collect();
    let mut col_stager = PipeBatchStager::new_columnar(
        Arc::from("edge_pipe"),
        Arc::clone(&pipe_schema),
        time_col_index,
        &yield_names,
    );
    for (row, (_, event_nanos)) in out.iter().zip(rows.iter()) {
        col_stager
            .push_row("edge_rule", row, *event_nanos)
            .expect("stage");
    }
    let (_, _, col_batch) = col_stager.take_events().expect("build").expect("non-empty");

    assert_eq!(row_batch.num_rows(), col_batch.num_rows());
    assert_eq!(row_batch.num_columns(), col_batch.num_columns());
    for (i, (a, b)) in row_batch
        .columns()
        .iter()
        .zip(col_batch.columns().iter())
        .enumerate()
    {
        assert_eq!(a.len(), b.len(), "col {i} 长度一致");
        for row in 0..row_batch.num_rows() {
            assert_eq!(
                a.is_null(row),
                b.is_null(row),
                "col {i} ({}): row {row} null 位一致",
                pipe_schema.field(i).name()
            );
            if !a.is_null(row) {
                assert_eq!(
                    arrow::util::display::array_value_to_string(a, row).expect("display"),
                    arrow::util::display::array_value_to_string(b, row).expect("display"),
                    "col {i} ({}): row {row} 值一致",
                    pipe_schema.field(i).name()
                );
            }
        }
    }
    let _ = empty_tracked_bind_fields();
}

/// q13b 生产真实路径微基准：`on each m` + `join side_input snapshot` +
/// `detail = fmt("{}", side_input.value)`。生产 q13b **不走列式 join 路径**——
/// yield 含 fmt 函数（live join 下 columnar gate 拒绝，回退 row path），每行
/// Event clone + join lookup + fmt 解释求值。q13b_join_bench 只测了列式
/// （462ns/行），本段补 row path 的真实成本（2026-08-25 生产 q13b 每行
/// ~14µs，q13a 被其反压）。
#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-runtime q13a_pipe_bench -- --ignored --nocapture"]
fn q13b_production_path_bench() {
    use std::collections::HashMap as StdMap;
    use wf_engine::match_engine::{JoinKey, JoinRow};
    use wf_lang::ast::JoinMode;
    use wf_lang::plan::{JoinCondPlan, JoinPlan};

    // q13b 形状：on-each + snapshot join（side_input）+ yield detail=fmt
    let mut plan = RulePlan {
        conv_window: None,
        name: "q13b_bench_row".into(),
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
            tracked_bind_fields: std::collections::HashMap::new(),
            tracked_plain_fields: std::collections::HashSet::new(),
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
            right_window: "side_input".into(),
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
            target: "nexmark_alerts".into(),
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
                // 生产真实形态：fmt("{}", side_input.value)——row path 元凶
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
    plan.binds[0].alias = "m".into();
    plan.binds[0].window = "bid_mod".into();
    let exec = RuleExecutor::new(plan);
    // 2026-08-25 q13b 列式化：`fmt("{}", 限定字段)` 单参数恒等**已被 gate 放行**
    // （列式 join 富化路径读字段后按 fmt 语义渲染，与解释器逐字节一致——见
    // `fmt_identity_field` 与列式对拍）。此前该形状被拒绝、只能走 row path，
    // 是 q13b 1.3µs/行的元凶。本 bench 仍**直接调用 row path 函数**测历史基线
    // （与 gate 无关），供列式路径对照。
    assert!(
        exec.each_plan_columnar_safe(),
        "fmt(\"{{}}\", 限定字段) 在 live join 下应走列式（q13b 列式化放行）"
    );

    // bid_mod 形状批（mod_key 均匀 0..9999）
    let bm_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("bidder", DataType::Int64, true),
        Field::new("auction", DataType::Int64, true),
        Field::new("price", DataType::Int64, true),
        Field::new(
            "dateTime",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new("mod_key", DataType::Int64, true),
    ]));
    let id: Vec<i64> = (0..N as i64).collect();
    let bidder: Vec<i64> = (0..N as i64).map(|i| i % 100_000).collect();
    let auction: Vec<i64> = (0..N as i64).map(|i| i * 7).collect();
    let price: Vec<i64> = (0..N as i64).map(|i| (i * 37) % 1_000_000).collect();
    let date_time: Vec<i64> = (0..N as i64).map(|i| NANOS + i).collect();
    let mod_key: Vec<i64> = (0..N as i64).map(|i| i % 10000).collect();
    let bm_batch = RecordBatch::try_new(
        bm_schema,
        vec![
            Arc::new(Int64Array::from(id)) as ArrayRef,
            Arc::new(Int64Array::from(bidder)),
            Arc::new(Int64Array::from(auction)),
            Arc::new(Int64Array::from(price)),
            Arc::new(arrow::array::TimestampNanosecondArray::from(date_time)),
            Arc::new(Int64Array::from(mod_key)),
        ],
    )
    .unwrap();
    let events: Vec<Arc<wf_engine::match_engine::Event>> = batch_to_events(&bm_batch)
        .into_iter()
        .map(Arc::new)
        .collect();
    let first = &events[0];
    let mut field_order: Vec<&smol_str::SmolStr> = first.fields.keys().collect();
    field_order.sort_unstable();

    // side_input 静态表 join 索引（O(1) lookup，与生产 set_join_key 一致）
    let mut index: StdMap<JoinKey, Vec<JoinRow>> = StdMap::new();
    for k in 0..10000i64 {
        let mut fields = wf_engine::match_engine::EngineHashMap::default();
        fields.insert(
            "key".into(),
            wf_engine::match_engine::Value::Number(k as f64),
        );
        fields.insert(
            "value".into(),
            wf_engine::match_engine::Value::Str(format!("value-{k}").into()),
        );
        let row = JoinRow::Event(Arc::new(wf_engine::match_engine::Event { fields }));
        index
            .entry(JoinKey::from_value(&wf_engine::match_engine::Value::Number(k as f64)).unwrap())
            .or_default()
            .push(row);
    }
    struct IndexedLookup(StdMap<JoinKey, Vec<JoinRow>>);
    impl WindowLookup for IndexedLookup {
        fn snapshot_field_values(
            &self,
            _w: &str,
            _f: &str,
        ) -> Option<std::collections::HashSet<String>> {
            None
        }
        fn snapshot(&self, _w: &str) -> Option<Vec<JoinRow>> {
            None
        }
        fn join_lookup(
            &self,
            _w: &str,
            _kf: &str,
            key: &wf_engine::match_engine::Value,
        ) -> Option<Vec<JoinRow>> {
            Some(self.0.get(&JoinKey::from_value(key)?)?.clone())
        }
    }
    let lookup = IndexedLookup(index.clone());

    // 诊断：lookup 命中率（bench 构造正确性检查）
    let mut hits = 0usize;
    for ev in &events {
        if let Some(v) = ev.fields.get("mod_key")
            && lookup.join_lookup("side_input", "key", v).is_some()
        {
            hits += 1;
        }
    }
    eprintln!(
        "[q13b-prod-bench] join_lookup 命中 = {hits}/{}",
        events.len()
    );

    // 生产 row path：execute_each_direct_batch（含 Event clone + join + fmt）
    let mut builder = wf_engine::alert::AlertColumnBuilder::new(Arc::from("nexmark_alerts"));
    let mut appended = Vec::new();
    let rows: Vec<(&wf_engine::match_engine::Event, i64)> =
        events.iter().map(|e| (e.as_ref(), NANOS)).collect();
    let start = Instant::now();
    let mut total_appended = 0usize;
    let mut total_failed = 0usize;
    let mut total_rejected = 0usize;
    for chunk in rows.chunks(4096) {
        let outcome = exec.execute_each_direct_batch(
            chunk,
            &lookup,
            &field_order,
            NANOS,
            &mut builder,
            &mut appended,
        );
        total_appended += outcome.appended;
        total_failed += outcome.failed;
        total_rejected += outcome.rejected;
    }
    let row_ns = start.elapsed().as_nanos() as f64 / N as f64;

    // 对照：同一 executor 走 columnar join（若可）——但 fmt 拒绝，仅作参照
    eprintln!(
        "[q13b-prod-bench] N = {N}, appended = {total_appended}, failed = {total_failed}, rejected = {total_rejected}\n  row path (Event clone + join + fmt): {:.1} ns/row ({:.2}M/s)",
        row_ns,
        1e9 / row_ns / 1e6
    );

    // ---- 对照：真实 provider 路径（RwLock + 每行 Event 构建，2026-08-25
    // q13 1.52M EPS 定位用）---- 生产 join_lookup 的 provider 分支每行
    // `pw.read()` 锁 + 行→JoinRow::Event 构建（HashMap 分配）；bench 的
    // IndexedLookup 是 Arc clone 零拷贝，低估生产成本。本段量化差距。
    use std::sync::RwLock as StdRwLock;
    struct LockedProviderLookup(StdRwLock<StdMap<JoinKey, Vec<wf_engine::match_engine::JoinRow>>>);
    impl WindowLookup for LockedProviderLookup {
        fn snapshot_field_values(
            &self,
            _w: &str,
            _f: &str,
        ) -> Option<std::collections::HashSet<String>> {
            None
        }
        fn snapshot(&self, _w: &str) -> Option<Vec<wf_engine::match_engine::JoinRow>> {
            None
        }
        fn join_lookup(
            &self,
            _w: &str,
            _kf: &str,
            key: &wf_engine::match_engine::Value,
        ) -> Option<Vec<wf_engine::match_engine::JoinRow>> {
            // 复刻 window_lookup.rs 的 provider 分支：锁 + 索引行→Event 构建。
            let locked = self.0.read().expect("provider lock");
            let rows = locked.get(&JoinKey::from_value(key)?)?;
            Some(
                rows.iter()
                    .map(|row| {
                        let fields: wf_engine::match_engine::EngineHashMap<
                            smol_str::SmolStr,
                            wf_engine::match_engine::Value,
                        > = row
                            .field_names()
                            .into_iter()
                            .map(|n| {
                                let n = n.to_string();
                                (
                                    n.clone().into(),
                                    row.field_value(&n).expect("field").clone(),
                                )
                            })
                            .collect();
                        wf_engine::match_engine::JoinRow::Event(std::sync::Arc::new(
                            wf_engine::match_engine::Event { fields },
                        ))
                    })
                    .collect(),
            )
        }
    }
    let locked_lookup = LockedProviderLookup(StdRwLock::new(index.clone()));
    let mut builder = wf_engine::alert::AlertColumnBuilder::new(Arc::from("nexmark_alerts"));
    let mut appended = Vec::new();
    let start = Instant::now();
    let mut total_appended = 0usize;
    let mut total_failed = 0usize;
    let mut total_rejected = 0usize;
    for chunk in rows.chunks(4096) {
        let outcome = exec.execute_each_direct_batch(
            chunk,
            &locked_lookup,
            &field_order,
            NANOS,
            &mut builder,
            &mut appended,
        );
        total_appended += outcome.appended;
        total_failed += outcome.failed;
        total_rejected += outcome.rejected;
    }
    let locked_ns = start.elapsed().as_nanos() as f64 / N as f64;
    // 计数纳入断言：对照路径必须真的处理完所有行（否则 ns/row 是假的）。
    assert_eq!(
        total_appended + total_failed + total_rejected,
        N,
        "provider 对照路径必须覆盖全部行（appended={total_appended} failed={total_failed} rejected={total_rejected}）"
    );
    eprintln!(
        "[q13b-prod-bench] 对照 provider 路径（RwLock + 每行 Event 构建）: {:.1} ns/row ({:.2}M/s) = {:.2}x of row path",
        locked_ns,
        1e9 / locked_ns / 1e6,
        locked_ns / row_ns
    );
    eprintln!(
        "[q13b-prod-bench] 生产 14µs/行 vs row path {row_ns:.1}ns → 剩余差距在 rule_task 层/并发"
    );
    let _ = empty_tracked_bind_fields();
}

/// q13b 并发对照（2026-08-25 q13 1.52M EPS 定位）：10 个 push worker 共享同一
/// provider 锁。单线程带锁只慢 16%（见 q13b_production_path_bench），若生产
/// 每 worker 6.6µs/行（10 worker 分摊 1.52M），大头顶在**并发锁竞争**——本段
/// 量化：T 线程共享一把 RwLock，各自处理 1/T 数据，总吞吐 vs 单线程×T。
#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-runtime q13b_concurrent_bench -- --ignored --nocapture"]
fn q13b_concurrent_lock_bench() {
    use std::collections::HashMap as StdMap;
    use std::sync::RwLock as StdRwLock;
    use wf_engine::match_engine::{JoinKey, JoinRow, Value as EValue};

    const THREADS: usize = 10;
    const PER_THREAD: usize = 100_000;
    // 每线程独立 executor（生产分片 worker 各持 executor），共享同一把锁。
    let mut execs = Vec::new();
    for _ in 0..THREADS {
        let mut plan = RulePlan {
            conv_window: None,
            name: "q13b_conc".into(),
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
                tracked_bind_fields: std::collections::HashMap::new(),
                tracked_plain_fields: std::collections::HashSet::new(),
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
            joins: vec![wf_lang::plan::JoinPlan {
                right_window: "side_input".into(),
                mode: wf_lang::ast::JoinMode::Snapshot,
                conds: vec![wf_lang::plan::JoinCondPlan {
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
                target: "nexmark_alerts".into(),
                version: None,
                fields: vec![
                    YieldField {
                        name: "id".into(),
                        value: Expr::Field(FieldRef::Qualified("m".into(), "bidder".into())),
                    },
                    YieldField {
                        name: "detail".into(),
                        value: Expr::FuncCall {
                            qualifier: None,
                            name: "fmt".into(),
                            args: vec![
                                Expr::StringLit("{}".into()),
                                Expr::Field(FieldRef::Qualified(
                                    "side_input".into(),
                                    "value".into(),
                                )),
                            ],
                        },
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
        plan.binds[0].alias = "m".into();
        plan.binds[0].window = "bid_mod".into();
        execs.push(RuleExecutor::new(plan));
    }

    // 共享 provider 锁（模拟生产 registry 里同一个 side_input ProviderWindow）。
    let shared: StdMap<JoinKey, Vec<JoinRow>> = {
        let mut index: StdMap<JoinKey, Vec<JoinRow>> = StdMap::new();
        for k in 0..10000i64 {
            let mut fields = wf_engine::match_engine::EngineHashMap::default();
            fields.insert("key".into(), EValue::Number(k as f64));
            fields.insert("value".into(), EValue::Str(format!("value-{k}").into()));
            let row = JoinRow::Event(Arc::new(wf_engine::match_engine::Event { fields }));
            index
                .entry(JoinKey::from_value(&EValue::Number(k as f64)).unwrap())
                .or_default()
                .push(row);
        }
        index
    };
    struct SharedLock(StdRwLock<StdMap<JoinKey, Vec<JoinRow>>>);
    impl WindowLookup for SharedLock {
        fn snapshot_field_values(
            &self,
            _w: &str,
            _f: &str,
        ) -> Option<std::collections::HashSet<String>> {
            None
        }
        fn snapshot(&self, _w: &str) -> Option<Vec<JoinRow>> {
            None
        }
        fn join_lookup(&self, _w: &str, _kf: &str, key: &EValue) -> Option<Vec<JoinRow>> {
            let locked = self.0.read().expect("provider lock");
            Some(locked.get(&JoinKey::from_value(key)?)?.clone())
        }
    }
    let lookup = Arc::new(SharedLock(StdRwLock::new(shared)));

    // 每线程一个 bid_mod 批（mod_key 均匀 0..9999）→ Event 物化。
    let batches: Vec<Vec<Arc<wf_engine::match_engine::Event>>> = (0..THREADS)
        .map(|t| {
            let n = PER_THREAD;
            let mut events = Vec::with_capacity(n);
            for i in 0..n {
                let mut fields = wf_engine::match_engine::EngineHashMap::default();
                fields.insert("id".into(), EValue::Number((t * n + i) as f64));
                fields.insert("bidder".into(), EValue::Number((t * n + i) as f64));
                fields.insert("auction".into(), EValue::Number((t * n + i) as f64));
                fields.insert("price".into(), EValue::Number((t * n + i) as f64));
                fields.insert("dateTime".into(), EValue::Number(NANOS as f64));
                fields.insert("mod_key".into(), EValue::Number((i % 10000) as f64));
                events.push(Arc::new(wf_engine::match_engine::Event { fields }));
            }
            events
        })
        .collect();

    let start = Instant::now();
    let mut handles = Vec::new();
    for t in 0..THREADS {
        let exec = execs[t].clone();
        let events = batches[t].clone();
        let lookup = Arc::clone(&lookup);
        handles.push(std::thread::spawn(move || {
            let mut field_order: Vec<&smol_str::SmolStr> = events[0].fields.keys().collect();
            field_order.sort_unstable();
            let rows: Vec<(&wf_engine::match_engine::Event, i64)> =
                events.iter().map(|e| (e.as_ref(), NANOS)).collect();
            let mut builder =
                wf_engine::alert::AlertColumnBuilder::new(Arc::from("nexmark_alerts"));
            let mut appended = Vec::new();
            for chunk in rows.chunks(4096) {
                let _ = exec.execute_each_direct_batch(
                    chunk,
                    lookup.as_ref(),
                    &field_order,
                    NANOS,
                    &mut builder,
                    &mut appended,
                );
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    let elapsed = start.elapsed().as_nanos() as f64;
    let per_row = elapsed / (THREADS * PER_THREAD) as f64;
    eprintln!(
        "[q13b-conc-bench] {THREADS} 线程共享 RwLock：{per_row:.1} ns/行 → 总 {:.2}M/s（单线程无锁 1.29µs/行 ≈ {:.1}M/s ×{THREADS} 理论）",
        1e9 / per_row / 1e6,
        1e9 / 1286.7 / 1e6
    );
    let _ = empty_tracked_bind_fields();
}

/// bid_mod 生产形状 schema（6 声明列 + 4 个 `__wfu_*` meta + `__wf_pipe_ts`）：
/// 与真实中间窗一致（实测 91B/行 vs 声明 6×int64=48B，差额就是 meta 列）。
fn bid_mod_prod_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("bidder", DataType::Int64, true),
        Field::new("auction", DataType::Int64, true),
        Field::new("price", DataType::Int64, true),
        Field::new(
            "dateTime",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new("mod_key", DataType::Int64, true),
        Field::new("__wfu_rule_name", DataType::Utf8, true),
        Field::new("__wfu_score", DataType::Float64, true),
        Field::new("__wfu_entity_type", DataType::Utf8, true),
        Field::new("__wfu_entity_id", DataType::Utf8, true),
        Field::new(
            "__wf_pipe_ts",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            true,
        ),
    ]))
}

/// **pipe 写入路径的分配足迹量化**（2026-08-25，回答「优化空间多大」）。
///
/// 背景：q13 内存缺口已实证与 **pipe 写入分配速率**成正比（非在途积压、非
/// 分配器、非窗口保留——见 `docs/issues/q13-memory-peak-scales-with-volume.md`）。
/// 本测量给出每批的三个数字，用来判断优化天花板：
/// - **暂存峰值**：`PipeEachRow` + `PipeCol` 的 `Vec<Option<T>>` 暂存
/// - **输出内容**：`content_bytes(batch)`（最终落窗的有效字节 = 理论下界）
/// - **放大倍数** = 暂存峰值 / 输出内容 —— 可优化空间就是这个倍数超出 1 的部分
///
/// 现实现的已知浪费（malloc_history 实证）：
/// 1. `Vec<Option<i64>>` 暂存 **16B/值**，Arrow 目标 8B/值 + null bitmap → 2×
/// 2. `take_batch` 的 `Int64Array::from(Vec<Option<_>>)` 是**全量拷贝**
/// 3. `PipeEachRow.values: Vec<Option<Value>>` + `entity_id: String` **每行各一次堆分配**；
///    `rule_name`/`entity_type` 每行重复渲染同一常量值
#[test]
#[ignore = "measurement: cargo test --release -p wf-runtime pipe_write_alloc_footprint -- --ignored --nocapture"]
fn pipe_write_alloc_footprint() {
    // 生产批规模（实测 bid_mod 35,360 行/批）。
    const ROWS: usize = 35_360;
    let exec = q13a_plan_rule();
    let batch = bid_batch(ROWS);
    let schema = bid_mod_prod_schema();
    let yield_names: Vec<Arc<str>> = exec
        .plan()
        .yield_plan
        .fields
        .iter()
        .map(|f| Arc::from(f.name.as_str()))
        .collect();
    let col_events: Vec<wf_engine::match_engine::ColumnarEvent<'_>> = (0..ROWS)
        .map(|i| wf_engine::match_engine::ColumnarEvent::new(&batch, i))
        .collect();
    let rows: Vec<(&wf_engine::match_engine::ColumnarEvent<'_>, i64)> =
        col_events.iter().map(|ev| (ev, NANOS)).collect();

    // 预热一轮（首次触碰的分配器页不计入测量）。
    {
        let prepared = exec.each_batch_prepare(&batch);
        let mut out = Vec::with_capacity(ROWS);
        exec.execute_each_pipe_batch_columnar(&rows, &prepared, &mut out);
        let mut stager =
            PipeBatchStager::new_columnar(Arc::from("bid_mod"), Arc::clone(&schema), Some(4), &yield_names);
        for row in &out {
            stager.push_row("q13a_bid_mod", row, NANOS).expect("stage");
        }
        let _ = stager.take_batch().expect("build");
    }

    let probe = crate::memory_probe::MemoryProbe::exclusive();
    let prepared = exec.each_batch_prepare(&batch);
    let mut out: Vec<wf_engine::match_engine::PipeEachRow> = Vec::with_capacity(ROWS);
    exec.execute_each_pipe_batch_columnar(&rows, &prepared, &mut out);
    let after_eval = probe.peak_growth();

    let mut stager =
        PipeBatchStager::new_columnar(Arc::from("bid_mod"), Arc::clone(&schema), Some(4), &yield_names);
    for row in &out {
        stager.push_row("q13a_bid_mod", row, NANOS).expect("stage");
    }
    let after_stage = probe.peak_growth();

    let built = stager.take_batch().expect("build").expect("non-empty");
    let peak = probe.peak_growth();
    let content = wf_engine::window::content_bytes(&built.1);

    eprintln!("[pipe-alloc] 批规模 = {ROWS} 行（生产实测批大小）");
    eprintln!(
        "[pipe-alloc] ① execute_each_pipe_batch_columnar（PipeEachRow）峰值 = {:.2} MB ({:.0} B/行)",
        after_eval as f64 / 1e6,
        after_eval as f64 / ROWS as f64
    );
    eprintln!(
        "[pipe-alloc] ② + push_row 暂存（PipeCol Vec<Option<T>>）峰值 = {:.2} MB ({:.0} B/行)",
        after_stage as f64 / 1e6,
        after_stage as f64 / ROWS as f64
    );
    eprintln!(
        "[pipe-alloc] ③ + take_batch（Arrow 数组拷贝）峰值 = {:.2} MB ({:.0} B/行)",
        peak as f64 / 1e6,
        peak as f64 / ROWS as f64
    );
    eprintln!(
        "[pipe-alloc] 输出 content_bytes = {:.2} MB ({:.0} B/行) ← 理论下界",
        content as f64 / 1e6,
        content as f64 / ROWS as f64
    );
    eprintln!(
        "[pipe-alloc] **放大倍数 = {:.2}×**（峰值/输出）→ 可优化空间 = {:.2} MB/批 ({:.0} B/行)",
        peak as f64 / content as f64,
        (peak.saturating_sub(content)) as f64 / 1e6,
        (peak.saturating_sub(content)) as f64 / ROWS as f64
    );
    assert!(content > 0, "输出批必须非空");
    let _ = empty_tracked_bind_fields();
}
