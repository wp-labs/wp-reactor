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

/// q4a deferred 中间窗 schema（`__wf_pipe_ts` + 4 yield 列 + 4 meta 列——
/// `record_window_fields` 行式路径会补的四个 `__wfu_meta_*`）。
fn q4a_stager_schema() -> Arc<arrow::datatypes::Schema> {
    use wf_lang::wfu_meta::WfuIntermediateMetaField;
    Arc::new(Schema::new(vec![
        Field::new(
            "__wf_pipe_ts",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("id", DataType::Int64, true),
        Field::new("category", DataType::Int64, true),
        Field::new("final", DataType::Float64, true),
        Field::new(
            "dateTime",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new(
            WfuIntermediateMetaField::RuleName.name(),
            DataType::Utf8,
            true,
        ),
        Field::new(WfuIntermediateMetaField::Score.name(), DataType::Utf8, true),
        Field::new(
            WfuIntermediateMetaField::EntityType.name(),
            DataType::Utf8,
            true,
        ),
        Field::new(
            WfuIntermediateMetaField::EntityId.name(),
            DataType::Utf8,
            true,
        ),
    ]))
}

/// q4a deferred emit 中间窗 staging 对比（2026-08-26）：deferred 到期评估的
/// `OutputRecord` → `push_record`（行式：`record_window_fields` 的
/// yield_fields clone + HashSet + meta 名 Arc::from 每行分配）vs
/// `push_record_columnar`（列式：col_sources 预计算 + SmolStr 内联 meta 值）。
///
/// 背景：q4 30M EPS 7.66M → 4.25M（回归），q4a 与 q9 deferred 部分同构但
/// yield 到中间窗（q9 直出 sink）——staging 是 q4 掉速主嫌疑。
#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-runtime q4a_stage_bench -- --ignored --nocapture"]
fn q4a_stage_bench() {
    use wf_engine::alert::AlertOrigin;
    use wf_engine::match_engine::Value;
    let schema = q4a_stager_schema();
    let yield_names: Vec<Arc<str>> = ["id", "category", "final", "dateTime"]
        .iter()
        .map(|s| Arc::from(*s))
        .collect();
    // q4a 产量形状的 OutputRecord（30M 数据 ≈ 1.67M 行，抽样 N）。
    let records: Vec<OutputRecord> = (0..N)
        .map(|i| OutputRecord {
            wfx_id: format!("id-{i}"),
            rule_name: Arc::from("q4a_auction_finals"),
            score: 20.0,
            entity_type: Arc::from("digit"),
            entity_id: (100_000 + i).to_string(),
            origin: AlertOrigin::Deferred,
            fired_at: "2026-08-26T00:00:00Z".to_string(),
            emit_time: Arc::from("2026-08-26T00:00:00Z"),
            matched_rows: Vec::new(),
            summary: Arc::from(""),
            yield_target: Arc::from("auction_finals"),
            yield_fields: vec![
                (Arc::from("id"), Value::Number(i as f64)),
                (Arc::from("category"), Value::Number((i % 5) as f64)),
                (Arc::from("final"), Value::Number(10.0 + i as f64)),
                // dateTime 缺失 → 时间列回退 event_time_nanos
            ],
            yield_field_types: Vec::new().into(),
            event_time_nanos: NANOS + i as i64,
            machine_id: Arc::from(""),
            scope_key: Arc::from(""),
        })
        .collect();

    // 行式 staging（旧路径）。
    let start = Instant::now();
    for _ in 0..4 {
        let mut stager =
            PipeBatchStager::new(Arc::from("auction_finals"), Arc::clone(&schema), Some(4));
        for r in &records {
            stager.push_record(r).expect("row stage");
        }
        let _ = stager.take_batch().expect("build").expect("rows");
    }
    let row_ns = start.elapsed().as_nanos() as f64 / (N as f64 * 4.0);

    // 列式 staging（2026-08-26 新路径）。
    let start = Instant::now();
    for _ in 0..4 {
        let mut stager = PipeBatchStager::new_columnar(
            Arc::from("auction_finals"),
            Arc::clone(&schema),
            Some(4),
            &yield_names,
        );
        for r in &records {
            stager.push_record_columnar(r).expect("col stage");
        }
        let _ = stager.take_batch().expect("build").expect("rows");
    }
    let col_ns = start.elapsed().as_nanos() as f64 / (N as f64 * 4.0);

    eprintln!(
        "[q4a-stage-bench] N={N} 行式 push_record    {:>9.1} ns/row  ({:>7.2}M rows/s)",
        row_ns,
        1e9 / row_ns / 1e6
    );
    eprintln!(
        "[q4a-stage-bench] N={N} 列式 push_record_columnar {:>9.1} ns/row  ({:>7.2}M rows/s)",
        col_ns,
        1e9 / col_ns / 1e6
    );
    eprintln!(
        "[q4a-stage-bench] 列式/行式 = {:.1}x（staging 每行省 record_window_fields 分配）",
        row_ns / col_ns
    );
}
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
        let mut stager = PipeBatchStager::new_columnar(
            Arc::from("bid_mod"),
            Arc::clone(&schema),
            Some(4),
            &yield_names,
        );
        let mut sink = TestStagerSink {
            stager: &mut stager,
        };
        exec.execute_each_pipe_batch_columnar(&rows, &prepared, &mut sink);
        let _ = stager.take_batch().expect("build");
    }

    // ① 旧路径（对照）：先物化整批 `Vec<PipeEachRow>`。
    let eval_only = {
        let probe = crate::memory_probe::MemoryProbe::exclusive();
        let prepared = exec.each_batch_prepare(&batch);
        let mut out: Vec<wf_engine::match_engine::PipeEachRow> = Vec::with_capacity(ROWS);
        exec.execute_each_pipe_batch_columnar(&rows, &prepared, &mut out);
        let peak = probe.peak_growth();
        assert_eq!(out.len(), ROWS, "对照路径应输出全部行");
        peak
    };

    // ② 生产路径（流式 sink）：executor 逐行直接装列。
    let probe = crate::memory_probe::MemoryProbe::exclusive();
    let prepared = exec.each_batch_prepare(&batch);
    let mut stager = PipeBatchStager::new_columnar(
        Arc::from("bid_mod"),
        Arc::clone(&schema),
        Some(4),
        &yield_names,
    );
    let mut sink = TestStagerSink {
        stager: &mut stager,
    };
    let stats = exec.execute_each_pipe_batch_columnar(&rows, &prepared, &mut sink);
    assert_eq!(stats.appended, ROWS, "流式路径应装载全部行");
    let after_stage = probe.peak_growth();

    let built = stager.take_batch().expect("build").expect("non-empty");
    let peak = probe.peak_growth();
    let content = wf_engine::window::content_bytes(&built.1);

    // ④ **窗口会计保真度**（2026-08-25 在途量分账后的新问题）：窗口用
    //   `content_bytes`（逻辑列内容）计账，但批次实际占的分配器字节还包括
    //   null bitmap / offsets / 容量舍入。若两者差很多，则 `Σwindow_bytes` 低估
    //   真实占用，"未归因"就含有假额度。测法：只保留 batch（stager 已经
    //   finish 并重置），重新建基线后看仅此批存活时的增量。
    drop(probe);
    let retained = {
        let probe2 = crate::memory_probe::MemoryProbe::exclusive();
        let base = probe2.current();
        // 重建一份同形批（上一份仍活着作对照，不影响增量）。
        let mut s2 = PipeBatchStager::new_columnar(
            Arc::from("bid_mod"),
            Arc::clone(&schema),
            Some(4),
            &yield_names,
        );
        {
            let prepared2 = exec.each_batch_prepare(&batch);
            let mut sink2 = TestStagerSink { stager: &mut s2 };
            exec.execute_each_pipe_batch_columnar(&rows, &prepared2, &mut sink2);
        }
        let b2 = s2.take_batch().expect("build").expect("non-empty");
        drop(s2); // builder 已重置；丢掉 stager 只留 batch
        let held = probe2.current().saturating_sub(base);
        drop(b2);
        held
    };

    eprintln!("[pipe-alloc] 批规模 = {ROWS} 行（生产实测批大小）");
    eprintln!(
        "[pipe-alloc] ① 旧路径对照：物化 Vec<PipeEachRow> 峰值 = {:.2} MB ({:.0} B/行)",
        eval_only as f64 / 1e6,
        eval_only as f64 / ROWS as f64
    );
    eprintln!(
        "[pipe-alloc] ② 生产路径：流式求值+装列峰值 = {:.2} MB ({:.0} B/行)",
        after_stage as f64 / 1e6,
        after_stage as f64 / ROWS as f64
    );
    eprintln!(
        "[pipe-alloc] ③ + take_batch（builder.finish 零拷贝）峰值 = {:.2} MB ({:.0} B/行)",
        peak as f64 / 1e6,
        peak as f64 / ROWS as f64
    );
    eprintln!(
        "[pipe-alloc] 输出 content_bytes = {:.2} MB ({:.0} B/行) ← 理论下界",
        content as f64 / 1e6,
        content as f64 / ROWS as f64
    );
    eprintln!(
        "[pipe-alloc] **放大倍数 = {:.2}×**（峰值/输出）→ 剩余可优化 = {:.2} MB/批 ({:.0} B/行)",
        peak as f64 / content as f64,
        (peak.saturating_sub(content)) as f64 / 1e6,
        (peak.saturating_sub(content)) as f64 / ROWS as f64
    );
    eprintln!(
        "[pipe-alloc] ④ 窗口会计保真度：批次**存活占用** = {:.2} MB vs content_bytes {:.2} MB → 低估 {:.2}×",
        retained as f64 / 1e6,
        content as f64 / 1e6,
        retained as f64 / content as f64
    );
    assert!(content > 0, "输出批必须非空");
    let _ = empty_tracked_bind_fields();
}

/// 测试用 sink：直接转发给 stager（与生产 `PipeStagerSink` 同构，但不需要
/// 错误聚合计数）。
struct TestStagerSink<'a> {
    stager: &'a mut PipeBatchStager,
}

impl wf_engine::match_engine::PipeRowSink for TestStagerSink<'_> {
    fn push_pipe_row(
        &mut self,
        score: f64,
        entity_type: &str,
        entity_id: &str,
        values: &[Option<wf_engine::match_engine::Value>],
        event_time_nanos: i64,
    ) -> Result<(), String> {
        self.stager
            .push_row_parts(
                "q13a_bid_mod",
                score,
                entity_type,
                entity_id,
                values,
                event_time_nanos,
            )
            .map_err(|e| e.to_string())
    }
}

// ---------------------------------------------------------------------------
// q18 close 装载分配足迹（2026-08-26，q18 100M close 期 42G 归因）
// ---------------------------------------------------------------------------
//
// 背景：q18 100M close flush 期 DIRTY 峰值 42G（状态 9.8G + 窗口 3.5G + 工作态
// ~29G）。CUT_ALERT 消融（WF_DIAG_CUT_ALERT=1）降到 23.5G → **close alert 装载
// 路径贡献 ~18.5G**。本测量量化 `close_buckets_to_rows` + `execute_stats_close_
// batch_columnar` 的分配峰值随批内桶数（100 万 / 300 万）的增长形态，判断是
// 「每批固有」还是「随批大小超线性」。
//
// 运行：cargo test --release -p wf-runtime q18_close_alloc_footprint -- --ignored --nocapture
use wf_engine::alert::AlertColumnBuilder;
use wf_engine::match_engine::StatsExecutor;
use wf_lang::plan::{StatsAggPlan, StatsMeasurePlan, StatsOutputShapePlan, StatsPlan, WindowSpec};

/// q18 形态 StatsPlan：4 个 last 度量（price/channel/url/dateTime），键
/// (bidder, auction)——与 `nexmark_hotpath_bench::q18_stats_last_plan` 同形。
fn q18_close_stats_plan() -> StatsPlan {
    fn last(label: &str, field: &str) -> StatsMeasurePlan {
        StatsMeasurePlan {
            label: label.into(),
            source_alias: "b".into(),
            where_expr: None,
            agg: StatsAggPlan::Last,
            field: Some(wf_lang::ast::FieldRef::Qualified("b".into(), field.into())),
            arg: None,
        }
    }
    StatsPlan {
        window_spec: WindowSpec::Fixed(std::time::Duration::from_secs(86400)),
        keys: vec![
            wf_lang::ast::Expr::Field(wf_lang::ast::FieldRef::Qualified(
                "b".into(),
                "bidder".into(),
            )),
            wf_lang::ast::Expr::Field(wf_lang::ast::FieldRef::Qualified(
                "b".into(),
                "auction".into(),
            )),
        ],
        output_shape: StatsOutputShapePlan::Rows,
        measures: vec![
            last("last_price", "price"),
            last("last_channel", "channel"),
            last("last_url", "url"),
            last("last_dateTime", "dateTime"),
        ],
        tracked_bind_fields: {
            let mut m = std::collections::HashMap::new();
            m.insert(
                "b".to_string(),
                std::collections::HashSet::from([
                    "auction".to_string(),
                    "bidder".to_string(),
                    "price".to_string(),
                    "channel".to_string(),
                    "url".to_string(),
                    "dateTime".to_string(),
                ]),
            );
            m
        },
    }
}

/// q18 形态批（键域 auction 放大 → 每行唯一，对齐 30M/100M 真实形态）。
fn q18_close_batch(n: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("bidder", DataType::Int64, false),
        Field::new("price", DataType::Int64, false),
        Field::new("channel", DataType::Utf8, false),
        Field::new("url", DataType::Utf8, false),
        Field::new("dateTime", DataType::Int64, false),
    ]));
    let mut rng: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut next = |range: u64| {
        rng = rng
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (rng >> 33) % range
    };
    let auctions: Vec<i64> = (0..n).map(|_| 1_000 + next(2_000_000) as i64).collect();
    let bidders: Vec<i64> = (0..n).map(|_| 1_000 + next(1010) as i64).collect();
    let prices: Vec<i64> = (0..n).map(|_| (next(10_000_000) + 1) as i64).collect();
    let channels: Vec<String> = (0..n).map(|_| "Google".to_string()).collect();
    let urls: Vec<String> = (0..n)
        .map(|_| "https://www.nexmark.com/aaaaa/bbbbb/ccccc/item.htm?query=1".to_string())
        .collect();
    let times: Vec<i64> = (0..n).map(|i| NANOS + i as i64 * 65_217).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(auctions)) as ArrayRef,
            Arc::new(Int64Array::from(bidders)),
            Arc::new(Int64Array::from(prices)),
            Arc::new(arrow::array::StringArray::from(channels)),
            Arc::new(arrow::array::StringArray::from(urls)),
            Arc::new(Int64Array::from(times)),
        ],
    )
    .unwrap()
}

/// 回归断言（非 ignore，常规测试）: 链 Vec `with_capacity(1)` 修复钉死——
/// q18 每键独立 hash（链均长 1.0）时，每链容量必须精确 1（不能退回
/// `or_default()` 的 capacity=4，否则 2935 万链 × 144B ≈ 4.2G 浪费）。
/// 用 CountingAlloc 实测状态持有 vs 期望上界（宽松断言防平台差异）。
/// ⚠ 测量必须在 `exclusive()` 段内建桶 + `current_growth()`（相对基线），
/// 不能用 `current()`（进程全局累计含其他测试残留，并行跑会虚高）。
#[test]
fn q18_state_chain_capacity_bounded() {
    const N: usize = 200_000;
    let row_fields: Arc<std::collections::HashSet<String>> = Arc::new(
        ["auction", "bidder", "price", "channel", "url", "dateTime"]
            .iter()
            .map(|s| s.to_string())
            .collect(),
    );
    let batch = q18_close_batch(N);

    // 状态持有（CountingAlloc 相对基线增量）：exclusive 段内建桶。
    let (n_chains, per_bucket) = {
        let probe = crate::memory_probe::MemoryProbe::exclusive();
        let mut exec = StatsExecutor::with_row_fields(q18_close_stats_plan(), Some(row_fields));
        assert!(exec.process_batch(&batch), "列式前置应满足");
        let n_chains = exec.window.buckets.len();
        let growth = probe.current_growth();
        (n_chains, growth as f64 / n_chains.max(1) as f64)
    };

    // 链容量断言：每条链 capacity == 1（无碰撞时，每链 1 桶）。
    // q18 键域 auction 200 万 + bidder 1010 → N=20 万行几乎无碰撞。
    // 需在 exclusive 段外重新建桶（段内 exec 已 drop）——或直接断言上面
    // 已建桶的形态：N=20 万 → 每链 1 桶，容量必为 1。重建一次独立验证。
    let mut exec2 = StatsExecutor::with_row_fields(q18_close_stats_plan(), None);
    let batch2 = q18_close_batch(N);
    assert!(exec2.process_batch(&batch2), "列式前置应满足");
    let max_cap = exec2
        .window
        .buckets
        .values()
        .map(|c| c.capacity())
        .max()
        .unwrap_or(0);
    assert!(
        max_cap <= 2,
        "链 Vec 容量应精确 1（或碰撞链 2），实测 max_capacity={max_cap}——若退回 or_default() 会到 4"
    );

    // 状态持有上界：每桶 ≤ 1000B（633B 实测 + 余量；CountingAlloc 口径
    // 含 HashMap 容器 + 分配器元数据）。
    assert!(n_chains > N / 2, "N=20 万应几乎每行一键，实际 {n_chains}");
    assert!(
        per_bucket < 1000.0,
        "每桶状态持有应 < 1000B，实测 {per_bucket:.0}B/桶（n_chains={n_chains}）"
    );
}

#[test]
#[ignore = "measurement: cargo test --release -p wf-runtime q18_close_alloc_footprint -- --ignored --nocapture"]
fn q18_close_alloc_footprint() {
    // 每批桶数：100 万（EMIT_CHUNK 默认）与 300 万（观测超线性）。
    for &n_buckets in &[1_000_000usize, 3_000_000] {
        let row_fields: Arc<std::collections::HashSet<String>> = Arc::new(
            ["auction", "bidder", "price", "channel", "url", "dateTime"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
        );
        let mut exec = StatsExecutor::with_row_fields(q18_close_stats_plan(), Some(row_fields));
        let batch = q18_close_batch(n_buckets);
        let ok = exec.process_batch(&batch);
        assert!(ok, "列式前置应满足");

        // 阶段 ①：状态建桶（进程基线内增量）——参考：100M 状态 9.8G。
        let state_hold = {
            let probe = crate::memory_probe::MemoryProbe::exclusive();
            // 预热后的新基线：probe 已重置。
            let _ = probe.peak_growth();
            // 读一次真实持有（current 已在 process_batch 后）
            probe.current()
        };

        // 状态拆分：size_of 求和口径 vs CountingAlloc 实测口径——量化 777B vs
        // 336B 差异（Vec 容量翻倍？HashMap 槽？RowFields 漏算？）。
        {
            let n_chains = exec.window.buckets.len();
            let n_buckets: usize = exec.window.buckets.values().map(|c| c.len()).sum();
            let mut sum_scopes = 0usize;
            let mut sum_accs_cap = 0usize;
            let mut sum_accs_len = 0usize;
            let mut sum_bucket_stack = 0usize;
            let mut sum_chain_cap = 0usize;
            let mut sum_rowfields = 0usize;
            for chain in exec.window.buckets.values() {
                sum_chain_cap += chain.capacity()
                    * size_of_val(chain.first().unwrap_or_else(|| {
                        // 空链不贡献桶内存; 用占位类型大小（不可能到达）
                        &exec.window.buckets.values().next().expect("非空")[0]
                    }));
                for b in chain {
                    sum_bucket_stack += size_of_val(b);
                    sum_scopes += size_of_val(&b.scope_key) + scope_key_heap_bytes(&b.scope_key);
                    sum_accs_cap += b.accs.capacity() * size_of_val(&b.accs[0]);
                    sum_accs_len += b.accs.len() * size_of_val(&b.accs[0]);
                    if b.accs.iter().any(|a| a.last().is_some()) {
                        let rf = b
                            .accs
                            .iter()
                            .find_map(|a| a.last().as_ref())
                            .expect("is_some");
                        sum_rowfields += 16 + row_fields_heap_bytes_test(rf);
                    }
                }
            }
            // HashMap<u64, Vec<StatsBucket>>：槽位 + 控制字（foldhash 87.5% 满）。
            let hashmap_slots = (n_chains as f64 / 0.875) as usize;
            let hashmap_bytes = hashmap_slots * (8 + 16) /* key + bucket ptr/ctrl */;
            let total_sum = sum_bucket_stack
                + sum_scopes
                + sum_accs_cap
                + sum_chain_cap
                + sum_rowfields
                + hashmap_bytes;
            eprintln!(
                "[q18-state-hold] n_buckets={} n_chains={} 链均长={:.1}",
                n_buckets,
                n_chains,
                n_buckets as f64 / n_chains.max(1) as f64,
            );
            eprintln!(
                "[q18-state-hold] 求和口径: StatsBucket栈={:.0}MB scopeKey={:.0}MB accs_cap={:.0}MB accs_len={:.0}MB chain_cap={:.0}MB rowfields={:.0}MB hashmap={:.0}MB 合计={:.0}MB ({:.0}B/桶)",
                sum_bucket_stack as f64 / 1e6,
                sum_scopes as f64 / 1e6,
                sum_accs_cap as f64 / 1e6,
                sum_accs_len as f64 / 1e6,
                sum_chain_cap as f64 / 1e6,
                sum_rowfields as f64 / 1e6,
                hashmap_bytes as f64 / 1e6,
                total_sum as f64 / 1e6,
                total_sum as f64 / n_buckets.max(1) as f64,
            );
            eprintln!(
                "[q18-state-hold] CountingAlloc 实测 state_hold={:.1}MB ({:.0}B/桶) vs 求和 {:.1}MB——差 {:.1}MB",
                state_hold as f64 / 1e6,
                state_hold as f64 / n_buckets.max(1) as f64,
                total_sum as f64 / 1e6,
                (state_hold as f64 - total_sum as f64) / 1e6,
            );
            eprintln!(
                "[q18-state-hold] accs 容量放大 = {:.2}×（len→cap），链 Vec 放大 = {:.2}×",
                sum_accs_cap as f64 / sum_accs_len.max(1) as f64,
                sum_chain_cap as f64
                    / (n_buckets * (sum_bucket_stack as f64 / n_buckets.max(1) as f64) as usize)
                        .max(1) as f64,
            );

            // HashMap 容器本身的开销（隔离测：同样的键数插空 Vec）——
            // CountingAlloc 实测每 entry 的槽位+控制字+对齐真实成本。
            let hm_overhead = {
                use std::collections::HashMap as StdHashMap;
                let probe = crate::memory_probe::MemoryProbe::exclusive();
                let mut m: StdHashMap<u64, Vec<u8>> = StdHashMap::new();
                for i in 0..n_chains {
                    m.entry(i as u64)
                        .or_insert_with(|| Vec::with_capacity(1))
                        .push(0);
                }
                let peak = probe.peak_growth();
                eprintln!(
                    "[q18-state-hold] HashMap<u64,Vec<u8>> {} 链容器开销 = {:.1}MB ({:.0}B/链)",
                    n_chains,
                    peak as f64 / 1e6,
                    peak as f64 / n_chains.max(1) as f64,
                );
                peak
            };

            // 扁平键对比：`HashMap<(i64,i64), ()>` 直接做键（无 ScopeKey 树/无中间
            // hash 层）——量化「q18 双 int 键专用扁平化」的上限收益。
            let flat_key_overhead = {
                use std::collections::HashMap as StdHashMap;
                let probe = crate::memory_probe::MemoryProbe::exclusive();
                let mut m: StdHashMap<(i64, i64), ()> = StdHashMap::new();
                for i in 0..n_buckets {
                    m.insert((i as i64, (i as i64) % 1010), ());
                }
                let peak = probe.peak_growth();
                eprintln!(
                    "[q18-state-hold] HashMap<(i64,i64),()> {} 桶容器开销 = {:.1}MB ({:.0}B/桶)【扁平键】",
                    n_buckets,
                    peak as f64 / 1e6,
                    peak as f64 / n_buckets.max(1) as f64,
                );
                peak
            };
            let state_flat_proj = state_hold as f64
                - (sum_bucket_stack + sum_scopes + sum_chain_cap + hashmap_bytes) as f64
                + flat_key_overhead as f64;
            eprintln!(
                "[q18-state-hold] 扁平键投影: 去掉 scopeKey树+StatsBucket包+中间hash层 → 预计 {:.1}MB ({:.0}B/桶) vs 当前 {:.1}MB ({:.0}B/桶)",
                state_flat_proj / 1e6,
                state_flat_proj / n_buckets.max(1) as f64,
                state_hold as f64 / 1e6,
                state_hold as f64 / n_buckets.max(1) as f64,
            );
            assert!(n_buckets > 0);
            eprintln!(
                "[q18-state-hold] 容器差 = CountingAlloc {} - 求和链 {} = {:.1}MB",
                hm_overhead as f64 / 1e6,
                (sum_chain_cap + hashmap_bytes) as f64 / 1e6,
                (hm_overhead as f64 - (sum_chain_cap + hashmap_bytes) as f64) / 1e6,
            );
            assert!(n_buckets > 0);
        }

        // 阶段 ②：close_buckets_to_rows 全量转换（StatsCloseBucket）。
        let buckets = exec.take_buckets_up_to(n_buckets);
        {
            let probe = crate::memory_probe::MemoryProbe::exclusive();
            let cb = exec.close_buckets_to_rows(buckets);
            let peak = probe.peak_growth();
            let cb_bytes: usize = cb
                .iter()
                .map(|b| b.measures.iter().map(Vec::capacity).sum::<usize>())
                .sum();
            drop(cb);
            eprintln!(
                "[q18-close] n_buckets={} state_hold={:.1}MB convert_peak={:.1}MB convert_measures_cap={:.1}MB",
                n_buckets,
                state_hold as f64 / 1e6,
                peak as f64 / 1e6,
                cb_bytes as f64 / 1e6,
            );
        }

        // 阶段 ③：execute_stats_close_batch_columnar 直装载（alert 列）。
        // 需 RuleExecutor（spawn 侧由同一 stats 计划装配）——此处用
        // `stats_close_rule_executor` 构造同形 RuleExecutor（yield 计划与
        // q18 一致：id/alert_type/detail/request_count）。
        // 重新建桶（阶段 ② 已取光状态），模拟独立 close 批。
        let exec3 = {
            let row_fields3: Arc<std::collections::HashSet<String>> = Arc::new(
                ["auction", "bidder", "price", "channel", "url", "dateTime"]
                    .iter()
                    .map(|s| s.to_string())
                    .collect(),
            );
            let mut e = StatsExecutor::with_row_fields(q18_close_stats_plan(), Some(row_fields3));
            let b3 = q18_close_batch(n_buckets);
            let ok = e.process_batch(&b3);
            assert!(ok, "列式前置应满足");
            e
        };
        let mut exec3 = exec3;
        let b3 = exec3.take_buckets_up_to(n_buckets);
        let cb = exec3.close_buckets_to_rows(b3);
        let labels: Vec<String> = exec.plan.measures.iter().map(|m| m.label.clone()).collect();
        let row_names = exec.row_field_names().cloned();
        let target: Arc<str> = Arc::from("nexmark_alerts");
        let load_peak = {
            let probe = crate::memory_probe::MemoryProbe::exclusive();
            let exec_r = stats_close_rule_executor();
            let mut builder = AlertColumnBuilder::new(Arc::clone(&target));
            let outcome = exec_r.execute_stats_close_batch_columnar(
                &cb,
                &labels,
                row_names.as_ref(),
                &mut builder,
                NANOS,
                NANOS + 86_400_000_000_000,
            );
            let peak = probe.peak_growth();
            let built = builder.finish();
            let built_bytes = built.len() as f64;
            eprintln!(
                "[q18-close] n_buckets={} load_peak={:.1}MB rows={} (avg {:.0}B/row)",
                n_buckets,
                peak as f64 / 1e6,
                outcome.appended,
                if outcome.appended > 0 {
                    peak as f64 / outcome.appended as f64
                } else {
                    0.0
                },
            );
            assert_eq!(built_bytes as usize, outcome.appended);
            peak
        };
        drop(cb);
        assert!(load_peak > 0);
    }
    eprintln!("[q18-close] 完成：对比 1M vs 3M 桶的 convert/load 峰值增长形态");
}

/// 消融对照：fmt detail（真实 q18）vs 常量 detail——量化 fmt 逐行物化在
/// load_peak 的占比（1094B/行的大头是否 fmt String）。
#[test]
#[ignore = "measurement: cargo test --release -p wf-runtime q18_close_fmt_vs_const -- --ignored --nocapture"]
fn q18_close_fmt_vs_const() {
    const N: usize = 1_000_000;
    // 完整装载（真实 q18 detail = fmt 5 字段）——对照基线。
    let full_peak = {
        let row_fields: Arc<std::collections::HashSet<String>> = Arc::new(
            ["auction", "bidder", "price", "channel", "url", "dateTime"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
        );
        let mut exec = StatsExecutor::with_row_fields(q18_close_stats_plan(), Some(row_fields));
        let batch = q18_close_batch(N);
        let _ = exec.process_batch(&batch);
        let b = exec.take_buckets_up_to(N);
        let cb = exec.close_buckets_to_rows(b);
        let labels: Vec<String> = exec.plan.measures.iter().map(|m| m.label.clone()).collect();
        let row_names = exec.row_field_names().cloned();
        let exec_r = stats_close_rule_executor();
        let target: Arc<str> = Arc::from("nexmark_alerts");
        let probe = crate::memory_probe::MemoryProbe::exclusive();
        let mut builder = AlertColumnBuilder::new(Arc::clone(&target));
        let outcome = exec_r.execute_stats_close_batch_columnar(
            &cb,
            &labels,
            row_names.as_ref(),
            &mut builder,
            NANOS,
            NANOS + 86_400_000_000_000,
        );
        let peak = probe.peak_growth();
        eprintln!(
            "[q18-fmt] fmt_detail load_peak={:.1}MB rows={} (avg {:.0}B/row)",
            peak as f64 / 1e6,
            outcome.appended,
            if outcome.appended > 0 {
                peak as f64 / outcome.appended as f64
            } else {
                0.0
            },
        );
        peak
    };

    // 常量 detail（fmt 替换为 StringLit）——量化 fmt 的增量。
    let const_peak = {
        let row_fields: Arc<std::collections::HashSet<String>> = Arc::new(
            ["auction", "bidder", "price", "channel", "url", "dateTime"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
        );
        let mut exec = StatsExecutor::with_row_fields(q18_close_stats_plan(), Some(row_fields));
        let batch = q18_close_batch(N);
        let _ = exec.process_batch(&batch);
        let b = exec.take_buckets_up_to(N);
        let cb = exec.close_buckets_to_rows(b);
        let labels: Vec<String> = exec.plan.measures.iter().map(|m| m.label.clone()).collect();
        let row_names = exec.row_field_names().cloned();
        // 同形 executor，detail 改常量（StringLit）——列式 gate 仍放行。
        let exec_r = stats_close_rule_executor_const_detail();
        let target: Arc<str> = Arc::from("nexmark_alerts");
        let probe = crate::memory_probe::MemoryProbe::exclusive();
        let mut builder = AlertColumnBuilder::new(Arc::clone(&target));
        let outcome = exec_r.execute_stats_close_batch_columnar(
            &cb,
            &labels,
            row_names.as_ref(),
            &mut builder,
            NANOS,
            NANOS + 86_400_000_000_000,
        );
        let peak = probe.peak_growth();
        eprintln!(
            "[q18-fmt] const_detail load_peak={:.1}MB rows={} (avg {:.0}B/row)",
            peak as f64 / 1e6,
            outcome.appended,
            if outcome.appended > 0 {
                peak as f64 / outcome.appended as f64
            } else {
                0.0
            },
        );
        peak
    };
    eprintln!(
        "[q18-fmt] fmt 增量 = {:.1}MB（{:.0}%）",
        (full_peak as f64 - const_peak as f64) / 1e6,
        (full_peak as f64 - const_peak as f64) / full_peak as f64 * 100.0,
    );
    assert!(full_peak >= const_peak);
}

/// RowFields 堆内存（Box 数组元素 + null_mask；layout Arc 全局共享不计）——
/// 与 nexmark_hotpath_bench 的 row_fields_heap_bytes 同口径。
fn row_fields_heap_bytes_test(rf: &wf_engine::match_engine::RowFields) -> usize {
    let l = rf.layout();
    l.n_numeric() * 8
        + l.n_strings() * 24 // SmolStr 24B 内联
        + l.n_others() * size_of::<Option<wf_engine::match_engine::Value>>()
        + l.n_fields().div_ceil(64) * 8 // null_mask
}

/// ScopeKey 堆内存（Box 子节点；Str 长串堆分配忽略——q18 键为数字）。
fn scope_key_heap_bytes(k: &wf_engine::match_engine::ScopeKey) -> usize {
    use wf_engine::match_engine::ScopeKey;
    match k {
        ScopeKey::Pair(a, b) => {
            size_of::<ScopeKey>() * 2 + scope_key_heap_bytes(a) + scope_key_heap_bytes(b)
        }
        ScopeKey::Str(s) if s.len() > 22 => s.len(),
        _ => 0,
    }
}

/// q18 同形 executor，detail 改 StringLit 常量（对照 fmt 增量）。
fn stats_close_rule_executor_const_detail() -> wf_engine::match_engine::RuleExecutor {
    use wf_lang::ast::CloseMode;
    use wf_lang::ast::Expr;
    use wf_lang::ast::MatchMode;
    use wf_lang::plan::{BindPlan, EntityPlan, MatchPlan, ScorePlan, YieldField, YieldPlan};
    let plan = wf_lang::plan::RulePlan {
        conv_window: None,
        name: "q18_last_bid_stats".into(),
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
            window_spec: wf_lang::plan::WindowSpec::Fixed(std::time::Duration::from_secs(86400)),
            event_steps: vec![],
            close_steps: vec![],
            close_mode: CloseMode::And,
            match_mode: MatchMode::Seq,
            accu: false,
            seq: None,
            tracked_bind_aliases: std::collections::HashSet::new(),
            tracked_bind_fields: std::collections::HashMap::new(),
            tracked_plain_fields: std::collections::HashSet::new(),
            needs_field_history: false,
            trigger_event_needed: false,
        },
        each_plan: None,
        stats_plan: Some(q18_close_stats_plan()),
        joins: vec![],
        r#where: None,
        entity_plan: EntityPlan {
            entity_type: "digit".into(),
            entity_id_expr: Expr::Field(wf_lang::ast::FieldRef::Qualified(
                "b".into(),
                "auction".into(),
            )),
        },
        yield_plan: YieldPlan {
            target: "nexmark_alerts".into(),
            version: None,
            fields: vec![
                YieldField {
                    name: "id".into(),
                    value: Expr::Field(wf_lang::ast::FieldRef::Qualified(
                        "b".into(),
                        "auction".into(),
                    )),
                },
                YieldField {
                    name: "alert_type".into(),
                    value: Expr::StringLit("q18_last_stats".into()),
                },
                YieldField {
                    name: "detail".into(),
                    value: Expr::StringLit("q18_detail".into()),
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
    wf_engine::match_engine::RuleExecutor::new_with_yield_field_types(
        plan,
        std::collections::HashMap::from([
            (
                "id".into(),
                wf_lang::FieldType::Base(wf_lang::BaseType::Float),
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
                wf_lang::FieldType::Base(wf_lang::BaseType::Float),
            ),
        ]),
    )
}

/// q18 close 直写用 RuleExecutor（yield: id=b.auction / alert_type 常量 /
/// detail=b.url / request_count=1——q18 输出形状）。
fn stats_close_rule_executor() -> wf_engine::match_engine::RuleExecutor {
    use wf_lang::ast::CloseMode;
    use wf_lang::ast::Expr;
    use wf_lang::ast::MatchMode;
    use wf_lang::plan::{BindPlan, EntityPlan, MatchPlan, ScorePlan, YieldField, YieldPlan};
    let plan = wf_lang::plan::RulePlan {
        conv_window: None,
        name: "q18_last_bid_stats".into(),
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
            window_spec: wf_lang::plan::WindowSpec::Fixed(std::time::Duration::from_secs(86400)),
            event_steps: vec![],
            close_steps: vec![],
            close_mode: CloseMode::And,
            match_mode: MatchMode::Seq,
            accu: false,
            seq: None,
            tracked_bind_aliases: std::collections::HashSet::new(),
            tracked_bind_fields: std::collections::HashMap::new(),
            tracked_plain_fields: std::collections::HashSet::new(),
            needs_field_history: false,
            trigger_event_needed: false,
        },
        each_plan: None,
        stats_plan: Some(q18_close_stats_plan()),
        joins: vec![],
        r#where: None,
        entity_plan: EntityPlan {
            entity_type: "digit".into(),
            entity_id_expr: Expr::Field(wf_lang::ast::FieldRef::Qualified(
                "b".into(),
                "auction".into(),
            )),
        },
        yield_plan: YieldPlan {
            target: "nexmark_alerts".into(),
            version: None,
            fields: vec![
                YieldField {
                    name: "id".into(),
                    value: Expr::Field(wf_lang::ast::FieldRef::Qualified(
                        "b".into(),
                        "auction".into(),
                    )),
                },
                YieldField {
                    name: "alert_type".into(),
                    value: Expr::StringLit("q18_last_bid_stats".into()),
                },
                YieldField {
                    name: "detail".into(),
                    value: Expr::Field(wf_lang::ast::FieldRef::Qualified("b".into(), "url".into())),
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
    wf_engine::match_engine::RuleExecutor::new_with_yield_field_types(
        plan,
        std::collections::HashMap::from([
            (
                "id".into(),
                wf_lang::FieldType::Base(wf_lang::BaseType::Float),
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
                wf_lang::FieldType::Base(wf_lang::BaseType::Float),
            ),
        ]),
    )
}
