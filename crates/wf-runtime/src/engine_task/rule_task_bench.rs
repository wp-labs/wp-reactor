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

/// q13a 形状的 RuleExecutor：`on each b` + entity(digit, b.bidder) + yield
/// bid_mod（5 个 Field + 1 个 mod BinOp）。
fn q13a_plan_rule() -> RuleExecutor {
    let plan = RulePlan {
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
    };
    RuleExecutor::new(plan)
}

/// 空 WindowLookup（q13a 无 join，不查询窗口）。
struct NoLookup;
impl WindowLookup for NoLookup {
    fn snapshot_field_values(&self, _w: &str, _f: &str) -> Option<std::collections::HashSet<String>> {
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
    let events: Vec<Arc<wf_engine::match_engine::Event>> = batch_to_events(&batch)
        .into_iter()
        .map(Arc::new)
        .collect();

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
        if let Ok(Some(record)) = exec.execute_each_with_joins(
            ev,
            NANOS,
            &lookup,
            &field_order,
            NANOS,
        ) {
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
    let mut builder =
        wf_engine::alert::AlertColumnBuilder::new(Arc::from("alerts"));
    let start = Instant::now();
    for ev in &events {
        let _ = exec.execute_each_direct(
            ev,
            NANOS,
            &lookup,
            &field_order,
            NANOS,
            &mut builder,
        );
    }
    let direct_ns = start.elapsed().as_nanos() as f64 / N as f64;

    // ---- ④ 对照：批量路径 execute_each_direct_batch ----
    let mut builder =
        wf_engine::alert::AlertColumnBuilder::new(Arc::from("alerts"));
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
    report("① per-record (execute_each_with_joins)", per_record_ns, total_ns);
    report("② stage (PipeBatchStager::push_record)", stage_ns, total_ns);
    report("q13a process_batch 每行合计 (⓪+①+②)", materialize_ns + total_ns, total_ns);
    report("q13a 每行合计 (①+②)", total_ns, total_ns);
    report("对照: execute_each_direct (无 OutputRecord)", direct_ns, total_ns);
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
    let rows: Vec<(&wf_engine::match_engine::ColumnarEvent<'_>, i64)> = col_events
        .iter()
        .map(|ev| (ev, NANOS))
        .collect();
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
            stager.push_row("q13a_bench", row, NANOS).expect("stage row");
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
    let events: Vec<std::sync::Arc<wf_engine::match_engine::Event>> =
        batch_to_events(&batch).into_iter().map(std::sync::Arc::new).collect();
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
        Field::new("__wf_pipe_ts", DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None), true),
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
    let rows: Vec<(&wf_engine::match_engine::ColumnarEvent<'_>, i64)> = col_events
        .iter()
        .map(|ev| (ev, NANOS))
        .collect();
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
    use wf_lang::ast::JoinMode;
    use wf_lang::plan::{JoinCondPlan, JoinPlan};
    use wf_engine::match_engine::{JoinKey, JoinRow};

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
                        args: vec![Expr::StringLit("{}".into()), Expr::Field(FieldRef::Qualified("side_input".into(), "value".into()))],
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
    // 断言：fmt + live join → columnar gate 拒绝（走 row path）
    assert!(
        !exec.each_plan_columnar_safe(),
        "fmt 在 live join 下必须回退 row path（gate 拒绝）"
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
        fields.insert("key".into(), wf_engine::match_engine::Value::Number(k as f64));
        fields.insert("value".into(), wf_engine::match_engine::Value::Str(format!("value-{k}").into()));
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
    let lookup = IndexedLookup(index);

    // 诊断：lookup 命中率（bench 构造正确性检查）
    let mut hits = 0usize;
    for ev in &events {
        if let Some(v) = ev.fields.get("mod_key")
            && lookup.join_lookup("side_input", "key", v).is_some()
        {
            hits += 1;
        }
    }
    eprintln!("[q13b-prod-bench] join_lookup 命中 = {hits}/{}", events.len());

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
    eprintln!(
        "[q13b-prod-bench] 生产 14µs/行 vs row path {row_ns:.1}ns → 剩余差距在 rule_task 层/并发"
    );
    let _ = empty_tracked_bind_fields();
}
