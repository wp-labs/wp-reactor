//! engine_task 模块级覆盖测试（注册于 engine_task/mod.rs）。
//!
//! 覆盖点:
//! - `register_notifications` / `wait_any`: 通知注册辅助函数。
//! - `run_rule_task` push 通道关闭路径（channel 关闭 → drain + flush → Ok）。

use super::*;

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Notify, mpsc, watch};
use tokio_util::sync::CancellationToken;

use wf_engine::match_engine::RuleExecutor;
use wf_engine::window::{Router, WindowRegistry};

use crate::alert_task::SinkFanout;
use crate::engine_task::task_types::RuleTaskConfig;

#[test]
fn register_notifications_empty() {
    let notified = register_notifications(&[]);
    assert!(notified.is_empty());
}

#[tokio::test]
async fn wait_any_resolves_on_notification() {
    let notify = Arc::new(Notify::new());
    let notifiers = vec![notify.clone()];
    let mut notified = register_notifications(&notifiers);

    // Notify *after* registration to prove no lost-wakeup.
    notify.notify_one();
    tokio::time::timeout(Duration::from_secs(1), wait_any(&mut notified))
        .await
        .expect("wait_any must resolve");
}

#[tokio::test]
async fn wait_any_multiple_registers_all() {
    let a = Arc::new(Notify::new());
    let b = Arc::new(Notify::new());
    let notifiers = vec![a.clone(), b.clone()];
    let mut notified = register_notifications(&notifiers);
    assert_eq!(notified.len(), 2);

    // Firing the second notifier resolves the wait.
    b.notify_one();
    tokio::time::timeout(Duration::from_secs(1), wait_any(&mut notified))
        .await
        .expect("wait_any must resolve");
}

#[tokio::test]
async fn run_rule_task_push_channel_close_returns_ok() {
    // A bare rule task over a push channel: dropping the sender closes the
    // channel, which drains + flushes and returns Ok(()) without cancel.
    let (tx, rx) = mpsc::channel::<RulePush>(8);
    let config = bare_config(rx);
    drop(tx); // close immediately → loop drains, flushes, breaks.

    let result = tokio::time::timeout(Duration::from_secs(5), run_rule_task(config))
        .await
        .expect("rule task must finish promptly");
    result.expect("run_rule_task returns Ok on channel close");
}

#[tokio::test]
async fn run_stats_task_push_channel_close_returns_ok() {
    use crate::engine_task::stats_task::run_stats_task;
    use crate::engine_task::task_types::StatsTaskConfig;

    let (tx, rx) = mpsc::channel::<RulePush>(8);
    let config = StatsTaskConfig {
        stats: wf_engine::match_engine::StatsExecutor::with_row_fields(stats_plan(), None),
        executor: RuleExecutor::new(stats_rule_plan()),
        window_sources: vec![],
        sink_fanout: SinkFanout::closed(),
        cancel: CancellationToken::new(),
        router: empty_router(),
        metrics: None,
        time_field: Some("event_time".into()),
        timeout_scan_interval: Duration::from_secs(60),
        intermediate_targets: std::collections::HashSet::new(),
        pipe_registry: Arc::new(wf_engine::pipe::PipeRegistry::new()),
        eos_flush: watch::channel(0u64).1,
        push_rx: Some(rx),
        progress: std::collections::HashMap::new(),
        shard_index: None,
        shard_count: 1,
    };
    drop(tx);

    let result = tokio::time::timeout(Duration::from_secs(5), run_stats_task(config))
        .await
        .expect("stats task must finish promptly");
    result.expect("run_stats_task returns Ok on channel close");
}

#[tokio::test]
async fn run_rule_task_cancel_drains_and_returns_ok() {
    let (tx, rx) = mpsc::channel::<RulePush>(8);
    let mut config = bare_config(rx);
    let cancel = CancellationToken::new();
    config.cancel = cancel.clone();
    // Push one batch into the channel before cancelling.
    tx.send(RulePush {
        window_name: Arc::from("w"),
        events: None,
        batch: None,
        materialize_fields: None,
        seq: 1,
        shard_rows: None,
    })
    .await
    .expect("send");
    cancel.cancel();

    let result = tokio::time::timeout(Duration::from_secs(5), run_rule_task(config))
        .await
        .expect("rule task must finish promptly");
    result.expect("run_rule_task returns Ok on cancel");
}

// -- helpers ------------------------------------------------------------

fn empty_router() -> Arc<Router> {
    Arc::new(Router::new(
        WindowRegistry::build(vec![]).expect("empty registry"),
    ))
}

fn bare_config(push_rx: mpsc::Receiver<RulePush>) -> RuleTaskConfig {
    RuleTaskConfig {
        machine: None,
        each_alias: None,
        each_time_field: None,
        executor: RuleExecutor::new(bare_plan()),
        window_sources: vec![],
        sink_fanout: SinkFanout::closed(),
        cancel: CancellationToken::new(),
        timeout_scan_interval: Duration::from_secs(60),
        router: empty_router(),
        metrics: None,
        intermediate_targets: std::collections::HashSet::new(),
        pipe_registry: Arc::new(wf_engine::pipe::PipeRegistry::new()),
        eos_flush: watch::channel(0u64).1,
        push_rx: Some(push_rx),
        shard_index: None,
        shard_count: 1,
        progress: std::collections::HashMap::new(),
        conv_sink: None,
    }
}

fn bare_plan() -> wf_lang::plan::RulePlan {
    wf_lang::plan::RulePlan {
        name: "bare_rule".into(),
        binds: vec![],
        lets: vec![],
        match_plan: wf_lang::plan::MatchPlan {
            keys: vec![],
            key_map: None,
            key_join: None,
            window_spec: wf_lang::plan::WindowSpec::Sliding(Duration::from_secs(60)),
            event_steps: vec![],
            close_steps: vec![],
            close_mode: wf_lang::ast::CloseMode::And,
            match_mode: wf_lang::ast::MatchMode::Seq,
            accu: false,
            seq: None,
            tracked_bind_aliases: std::collections::HashSet::new(),
            tracked_bind_fields: std::collections::HashMap::new(),
            tracked_plain_fields: std::collections::HashSet::new(),
            needs_field_history: false,
            trigger_event_needed: false,
        },
        each_plan: None,
        stats_plan: None,
        joins: vec![],
        r#where: None,
        entity_plan: wf_lang::plan::EntityPlan {
            entity_type: "e".into(),
            entity_id_expr: wf_lang::ast::Expr::Bool(false),
        },
        yield_plan: wf_lang::plan::YieldPlan {
            target: "alerts".into(),
            version: None,
            fields: vec![],
        },
        score_plan: wf_lang::plan::ScorePlan {
            expr: wf_lang::ast::Expr::Number(1.0),
        },
        pattern_origin: None,
        conv_plan: None,
        limits_plan: None,
        conv_window: None,
    }
}

fn stats_plan() -> wf_lang::plan::StatsPlan {
    wf_lang::plan::StatsPlan {
        window_spec: wf_lang::plan::WindowSpec::Fixed(Duration::from_secs(10)),
        keys: vec![],
        output_shape: wf_lang::plan::StatsOutputShapePlan::Rows,
        measures: vec![wf_lang::plan::StatsMeasurePlan {
            label: "cnt".into(),
            source_alias: "b".into(),
            where_expr: None,
            agg: wf_lang::plan::StatsAggPlan::Count,
            field: None,
            arg: None,
        }],
        tracked_bind_fields: std::collections::HashMap::new(),
    }
}

fn stats_rule_plan() -> wf_lang::plan::RulePlan {
    let mut plan = bare_plan();
    plan.name = "bare_stats".into();
    plan.stats_plan = Some(stats_plan());
    plan
}
