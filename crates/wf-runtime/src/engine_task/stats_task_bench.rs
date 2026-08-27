//! q17 stats 框架层归因 bench（2026-08-27，注册于 stats_task.rs 内 `#[path]`——
//! 需要访问 `StatsTask` 私有）。
//!
//! 背景: diag q17 rules 段每事件 587ns·核，stats 归并链（wf-engine bench）已
//! 量化到 ~83ns——剩余 ~86% 在规则执行框架层（push 投递/ack/多核协作）。本
//! bench 在同一进程内测 q17 生产入口 `process_push` 的**逐段成本**（参考
//! rule_task_bench 的 q13a 模式）：
//!
//!   ① process_batch_from 基线（归并链 + 时间扫描, wf-engine 83ns 的 wf-runtime 复刻）
//!   ② process_push 完整（+ push 包装 + progress ack 原子）
//!   ③ drain_push_channel（mpsc try_recv 循环, 模拟 fanout 投递消费）
//!   ④ 多核并行（spawn N 线程各处理独立分片——量化并行吞吐/每事件墙钟）
//!
//! 运行:
//!   cargo test --release -p wf-runtime q17_framework_bench -- --ignored --nocapture

use super::*;

use std::time::{Duration, Instant};

use arrow::array::{ArrayRef, Int64Array, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field as ArrowField, Schema};
use tokio_util::sync::CancellationToken;
use wf_engine::match_engine::RuleExecutor;
use wf_engine::pipe::PipeRegistry;
use wf_engine::window::{Router, Window, WindowParams, WindowRegistry};
use wf_lang::ast::Expr;
use wf_lang::plan::{
    BindPlan, EntityPlan, MatchPlan, ScorePlan, StatsAggPlan, StatsMeasurePlan,
    StatsOutputShapePlan, StatsPlan, WindowSpec, YieldField, YieldPlan,
};

use super::super::tests::test_window_config;

const N: usize = 1_000_000;
const AUCTIONS: i64 = 100;
const NANOS: i64 = 1_750_000_000_000_000_000;

// ---------------------------------------------------------------------------
// q17 形状构造
// ---------------------------------------------------------------------------

fn q17_stats_plan() -> StatsPlan {
    let mk =
        |label: &str, agg: StatsAggPlan, field: Option<&str>, w: Option<Expr>| StatsMeasurePlan {
            label: label.into(),
            source_alias: "b".into(),
            where_expr: w,
            agg,
            field: field.map(|f| wf_lang::ast::FieldRef::Qualified("b".into(), f.into())),
            arg: None,
        };
    let price = || {
        Expr::Field(wf_lang::ast::FieldRef::Qualified(
            "b".into(),
            "price".into(),
        ))
    };
    let lt = |v: f64| Expr::BinOp {
        op: wf_lang::ast::BinOp::Lt,
        left: Box::new(price()),
        right: Box::new(Expr::Number(v)),
    };
    StatsPlan {
        window_spec: WindowSpec::Fixed(Duration::from_secs(86_400)),
        keys: vec![Expr::Field(wf_lang::ast::FieldRef::Qualified(
            "b".into(),
            "auction".into(),
        ))],
        output_shape: StatsOutputShapePlan::Rows,
        measures: vec![
            mk("total", StatsAggPlan::Count, None, None),
            mk("r1", StatsAggPlan::Count, None, Some(lt(10_000.0))),
            mk("minp", StatsAggPlan::Min, Some("price"), None),
            mk("maxp", StatsAggPlan::Max, Some("price"), None),
            mk("avgp", StatsAggPlan::Avg, Some("price"), None),
            mk("sump", StatsAggPlan::Sum, Some("price"), None),
        ],
        tracked_bind_fields: HashMap::new(),
    }
}

fn q17_rule_plan() -> wf_lang::plan::RulePlan {
    wf_lang::plan::RulePlan {
        name: "q17_framework_bench".into(),
        binds: vec![BindPlan {
            alias: "b".into(),
            window: "bid_events".into(),
            filter: None,
        }],
        lets: vec![],
        match_plan: MatchPlan {
            keys: vec![],
            key_map: None,
            key_join: None,
            window_spec: WindowSpec::Fixed(Duration::from_secs(86_400)),
            event_steps: vec![],
            close_steps: vec![],
            close_mode: wf_lang::ast::CloseMode::And,
            match_mode: wf_lang::ast::MatchMode::Seq,
            accu: false,
            seq: None,
            tracked_bind_aliases: HashSet::new(),
            tracked_bind_fields: HashMap::new(),
            tracked_plain_fields: HashSet::new(),
            needs_field_history: false,
            trigger_event_needed: false,
        },
        each_plan: None,
        stats_plan: Some(q17_stats_plan()),
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
            target: "alerts".into(),
            version: None,
            fields: vec![YieldField {
                name: "id".into(),
                value: Expr::Field(wf_lang::ast::FieldRef::Qualified(
                    "b".into(),
                    "auction".into(),
                )),
            }],
        },
        score_plan: ScorePlan {
            expr: Expr::Number(10.0),
        },
        pattern_origin: None,
        conv_plan: None,
        limits_plan: None,
        conv_window: None,
    }
}

/// q17 真实形状批: auction（100 热点循环）+ price + dateTime（65.2µs 步长）。
fn q17_batch(n: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        ArrowField::new("auction", DataType::Int64, false),
        ArrowField::new("price", DataType::Int64, false),
        ArrowField::new(
            "dateTime",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            false,
        ),
    ]));
    let auction: Vec<i64> = (0..n).map(|i| (i as i64) % AUCTIONS).collect();
    let price: Vec<i64> = (0..n)
        .map(|i| {
            let t = i as f64 / n as f64;
            (100.0 * 1e6f64.powf(t)).round() as i64
        })
        .collect();
    let time: Vec<i64> = (0..n).map(|i| NANOS + i as i64 * 65_200).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(auction)) as ArrayRef,
            Arc::new(Int64Array::from(price)) as ArrayRef,
            Arc::new(TimestampNanosecondArray::from(time)) as ArrayRef,
        ],
    )
    .unwrap()
}

fn q17_config(
    time_field: Option<&str>,
    push_rx: Option<mpsc::Receiver<RulePush>>,
) -> (StatsTaskConfig, CancellationToken) {
    let config = StatsTaskConfig {
        stats: wf_engine::match_engine::StatsExecutor::with_row_fields(q17_stats_plan(), None),
        executor: RuleExecutor::new(q17_rule_plan()),
        window_sources: vec![],
        sink_fanout: SinkFanout::closed(),
        cancel: CancellationToken::new(),
        router: Arc::new(Router::new(
            WindowRegistry::build(vec![]).expect("registry"),
        )),
        metrics: None,
        time_field: time_field.map(|s| s.to_string()),
        timeout_scan_interval: Duration::from_secs(60),
        intermediate_targets: HashSet::new(),
        pipe_registry: Arc::new(PipeRegistry::new()),
        eos_flush: tokio::sync::watch::channel(0u64).1,
        push_rx,
        progress: HashMap::new(),
        shard_index: None,
        shard_count: 1,
        merge_rx: None,
        merge_tx: None,
        mask_cache: None,
    };
    let cancel = config.cancel.clone();
    (config, cancel)
}

// ---------------------------------------------------------------------------
// bench
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore]
async fn q17_framework_bench() {
    let batch = q17_batch(N);
    let n = batch.num_rows();
    let rounds = 5;

    // ① process_batch_from（归并链 + 时间扫描 + 段扫, 无 push 包装/ack）
    let (config, _cancel) = q17_config(Some("dateTime"), None);
    let (mut task, _) = StatsTask::new(config);
    let t0 = Instant::now();
    for _ in 0..rounds {
        task.process_batch_from("bid_events", &batch, None).await;
    }
    let from_ns = t0.elapsed().as_secs_f64() * 1e9 / (n as f64 * rounds as f64);

    // ② process_push 完整（+ push 包装 + progress ack）
    let (config, _cancel) = q17_config(Some("dateTime"), None);
    let (mut task, _) = StatsTask::new(config);
    let push = RulePush {
        window_name: Arc::from("bid_events"),
        events: None,
        batch: Some(Arc::new(batch.clone())),
        materialize_fields: None,
        seq: 1,
        shard_rows: None,
    };
    let t0 = Instant::now();
    for _ in 0..rounds {
        task.process_push(push.clone()).await;
    }
    let push_ns = t0.elapsed().as_secs_f64() * 1e9 / (n as f64 * rounds as f64);

    // ③ drain_push_channel（mpsc 投递 + 消费）
    let (tx, rx) = mpsc::channel::<RulePush>(64);
    let (config, _cancel) = q17_config(Some("dateTime"), Some(rx));
    let (mut task, _) = StatsTask::new(config);
    // 取出 push_rx（避免 task 双重 &mut 借用）; 测完放回。
    let mut rx = task.push_rx.take().expect("rx");
    let t0 = Instant::now();
    for _ in 0..rounds {
        tx.try_send(push.clone()).expect("queue");
        task.drain_push_channel(&mut rx).await;
    }
    task.push_rx = Some(rx);
    let drain_ns = t0.elapsed().as_secs_f64() * 1e9 / (n as f64 * rounds as f64);

    eprintln!("== q17 框架层归因（N={}, rounds={}）==", N, rounds);
    eprintln!(
        "  ① process_batch_from     : {:>6.2} ns/事件（归并链+时间扫描）",
        from_ns
    );
    eprintln!("  ② process_push 完整      : {:>6.2} ns/事件", push_ns);
    eprintln!(
        "  ③ drain_push_channel     : {:>6.2} ns/事件（含 mpsc 投递）",
        drain_ns
    );
    eprintln!("  → wf-engine 归并链基线     : ~83 ns/事件（stats_task_layer）");
    eprintln!("  → diag 577 ns·核 剩余      : 多核协作/投递/窗口 close/ack 外围");
}
