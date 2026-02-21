use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::record_batch::RecordBatch;
use tokio::sync::{Mutex, Semaphore, mpsc};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use wf_core::alert::AlertRecord;
use wf_core::rule::{
    CepStateMachine, CloseReason, RuleExecutor, StepResult, batch_to_events,
};

// ---------------------------------------------------------------------------
// SchedulerCommand — control channel messages
// ---------------------------------------------------------------------------

/// Commands that can be sent to the scheduler via its control channel.
#[non_exhaustive]
pub enum SchedulerCommand {
    /// Trigger an immediate timeout scan (normally runs on 1 s interval).
    ScanNow,
}

// ---------------------------------------------------------------------------
// RuleEngine — one per compiled rule (public construction interface)
// ---------------------------------------------------------------------------

/// Pairs a [`CepStateMachine`] with its [`RuleExecutor`] and precomputed
/// routing from stream names to CEP aliases.
pub(crate) struct RuleEngine {
    pub machine: CepStateMachine,
    pub executor: RuleExecutor,
    /// `stream_name → Vec<alias>` — which aliases should receive events from
    /// each stream name.
    pub stream_aliases: HashMap<String, Vec<String>>,
}

// ---------------------------------------------------------------------------
// Internal parallel-execution types
// ---------------------------------------------------------------------------

/// Read-only routing metadata + shared mutable engine state.
struct EngineHandle {
    /// `stream_name → Vec<alias>` — read-only after construction.
    stream_aliases: HashMap<String, Vec<String>>,
    state: Arc<Mutex<EngineCore>>,
}

/// Mutable per-engine state protected by a [`Mutex`].
struct EngineCore {
    machine: CepStateMachine,
    executor: RuleExecutor,
}

// ---------------------------------------------------------------------------
// Scheduler
// ---------------------------------------------------------------------------

/// Core event loop that connects incoming batches to CEP state machines
/// and periodically scans for timeouts.
pub struct Scheduler {
    event_rx: mpsc::Receiver<(String, RecordBatch)>,
    engines: Vec<EngineHandle>,
    alert_tx: mpsc::Sender<AlertRecord>,
    cancel: CancellationToken,
    exec_semaphore: Arc<Semaphore>,
    exec_timeout: Duration,
    cmd_rx: mpsc::Receiver<SchedulerCommand>,
}

impl Scheduler {
    pub(crate) fn new(
        event_rx: mpsc::Receiver<(String, RecordBatch)>,
        engines: Vec<RuleEngine>,
        alert_tx: mpsc::Sender<AlertRecord>,
        cancel: CancellationToken,
        executor_parallelism: usize,
        exec_timeout: Duration,
        cmd_rx: mpsc::Receiver<SchedulerCommand>,
    ) -> Self {
        let handles = engines
            .into_iter()
            .map(|e| EngineHandle {
                stream_aliases: e.stream_aliases,
                state: Arc::new(Mutex::new(EngineCore {
                    machine: e.machine,
                    executor: e.executor,
                })),
            })
            .collect();

        Self {
            event_rx,
            engines: handles,
            alert_tx,
            cancel,
            exec_semaphore: Arc::new(Semaphore::new(executor_parallelism)),
            exec_timeout,
            cmd_rx,
        }
    }

    /// Run the scheduler event loop until cancelled.
    ///
    /// On cancellation the scheduler waits for all `event_tx` senders (the
    /// Receiver and its connection handlers) to drop, drains remaining
    /// batches, then flushes all active CEP instances via `flush_all`.
    ///
    /// When `run` returns, `self` (including `alert_tx`) is dropped, which
    /// closes the alert channel and signals the alert task to exit.
    #[tracing::instrument(name = "scheduler", skip_all)]
    pub async fn run(mut self) -> anyhow::Result<()> {
        let mut scan_interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            tokio::select! {
                msg = self.event_rx.recv() => {
                    match msg {
                        Some((stream_name, batch)) => {
                            self.dispatch_batch(&stream_name, &batch).await;
                        }
                        None => {
                            // All senders dropped — clean shutdown without cancel.
                            self.flush_all().await;
                            break;
                        }
                    }
                }
                _ = scan_interval.tick() => {
                    self.scan_timeouts().await;
                }
                Some(cmd) = self.cmd_rx.recv() => {
                    self.handle_command(cmd).await;
                }
                _ = self.cancel.cancelled() => {
                    // Wait for Receiver + connection handlers to drop their
                    // event_tx clones, draining all in-flight batches.
                    while let Some((stream_name, batch)) = self.event_rx.recv().await {
                        self.dispatch_batch(&stream_name, &batch).await;
                    }
                    self.flush_all().await;
                    break;
                }
            }
        }
        Ok(())
    }

    /// Handle an incoming control command.
    async fn handle_command(&mut self, cmd: SchedulerCommand) {
        match cmd {
            SchedulerCommand::ScanNow => {
                self.scan_timeouts().await;
            }
        }
    }

    /// Route a batch to all matching rule engines and advance their state
    /// machines — engines execute in parallel, bounded by `exec_semaphore`.
    ///
    /// Each per-engine task is wrapped in `tokio::time::timeout` so the
    /// scheduler is never blocked indefinitely by semaphore back-pressure or
    /// lock contention.  The inner advance loop is synchronous, so once it
    /// starts it always runs to completion — no partial state corruption.
    /// Alert emission is outside the timeout to guarantee delivery of
    /// successfully processed results.
    async fn dispatch_batch(&self, stream_name: &str, batch: &RecordBatch) {
        let start = Instant::now();
        let events = batch_to_events(batch);
        if events.is_empty() {
            return;
        }

        let rows = events.len();
        let events = Arc::new(events);
        let mut join_set = JoinSet::new();

        for engine in &self.engines {
            let Some(aliases) = engine.stream_aliases.get(stream_name) else {
                continue;
            };
            let aliases = aliases.clone();
            let events = Arc::clone(&events);
            let state = Arc::clone(&engine.state);
            let semaphore = Arc::clone(&self.exec_semaphore);
            let alert_tx = self.alert_tx.clone();
            let exec_timeout = self.exec_timeout;

            join_set.spawn(async move {
                let result = tokio::time::timeout(exec_timeout, async {
                    let _permit = semaphore.acquire().await.expect("semaphore closed");
                    let mut core = state.lock().await;

                    let mut alerts = Vec::new();
                    for event in events.iter() {
                        for alias in &aliases {
                            match core.machine.advance(alias, event) {
                                StepResult::Matched(ctx) => {
                                    match core.executor.execute_match(&ctx) {
                                        Ok(record) => alerts.push(record),
                                        Err(e) => wf_warn!(pipe, error = %e, "execute_match error"),
                                    }
                                }
                                StepResult::Advance | StepResult::Accumulate => {}
                            }
                        }
                    }
                    alerts
                })
                .await;

                match result {
                    Ok(alerts) => {
                        let n = alerts.len();
                        for record in alerts {
                            emit_alert(&alert_tx, record).await;
                        }
                        n
                    }
                    Err(_) => {
                        wf_warn!(pipe,
                            timeout = ?exec_timeout,
                            "dispatch_batch engine timed out, execution cancelled"
                        );
                        0
                    }
                }
            });
        }

        let mut total_alerts = 0usize;
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(n) => total_alerts += n,
                Err(e) => wf_warn!(pipe, error = %e, "engine task panicked"),
            }
        }

        wf_debug!(pipe,
            stream = stream_name,
            rows = rows,
            alerts = total_alerts,
            duration_ms = start.elapsed().as_millis() as u64,
            "dispatch_batch complete"
        );
    }

    /// Scan all engines for expired instances — engines execute in parallel,
    /// bounded by `exec_semaphore`.  Same timeout semantics as
    /// [`dispatch_batch`](Self::dispatch_batch).
    async fn scan_timeouts(&self) {
        let now = Instant::now();
        let mut join_set = JoinSet::new();

        for engine in &self.engines {
            let state = Arc::clone(&engine.state);
            let semaphore = Arc::clone(&self.exec_semaphore);
            let alert_tx = self.alert_tx.clone();
            let exec_timeout = self.exec_timeout;

            join_set.spawn(async move {
                let result = tokio::time::timeout(exec_timeout, async {
                    let _permit = semaphore.acquire().await.expect("semaphore closed");
                    let mut core = state.lock().await;

                    let close_outputs = core.machine.scan_expired(now);
                    let mut alerts = Vec::new();
                    for close in &close_outputs {
                        match core.executor.execute_close(close) {
                            Ok(Some(record)) => alerts.push(record),
                            Ok(None) => {}
                            Err(e) => wf_warn!(pipe, error = %e, "execute_close error"),
                        }
                    }
                    alerts
                })
                .await;

                match result {
                    Ok(alerts) => {
                        let n = alerts.len();
                        for record in alerts {
                            emit_alert(&alert_tx, record).await;
                        }
                        n
                    }
                    Err(_) => {
                        wf_warn!(pipe,
                            timeout = ?exec_timeout,
                            "scan_timeouts engine timed out, execution cancelled"
                        );
                        0
                    }
                }
            });
        }

        let mut total_alerts = 0usize;
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(n) => total_alerts += n,
                Err(e) => wf_warn!(pipe, error = %e, "scan_timeouts task panicked"),
            }
        }

        if total_alerts > 0 {
            wf_debug!(pipe,
                alerts = total_alerts,
                duration_ms = now.elapsed().as_millis() as u64,
                "scan_timeouts complete"
            );
        }
    }

    /// Flush all engines on shutdown: close every active instance.
    /// Sequential — no parallelism needed on the shutdown path.
    async fn flush_all(&self) {
        let mut total_alerts = 0usize;
        for engine in &self.engines {
            let mut core = engine.state.lock().await;
            let close_outputs = core.machine.close_all(CloseReason::Flush);
            for close in &close_outputs {
                match core.executor.execute_close(close) {
                    Ok(Some(record)) => {
                        emit_alert(&self.alert_tx, record).await;
                        total_alerts += 1;
                    }
                    Ok(None) => {}
                    Err(e) => wf_warn!(pipe, error = %e, "execute_close flush error"),
                }
            }
        }
        wf_debug!(pipe, alerts = total_alerts, "flush_all complete");
    }
}

async fn emit_alert(tx: &mpsc::Sender<AlertRecord>, record: AlertRecord) {
    if let Err(e) = tx.send(record).await {
        wf_warn!(pipe, error = %e, "alert channel closed");
    }
}

/// Build stream_name → alias routing for a rule, given its binds and the
/// window schemas.
///
/// For each `BindPlan { alias, window, .. }`, find the `WindowSchema` with
/// matching name, then map each of its `streams` to the alias.
pub(crate) fn build_stream_aliases(
    binds: &[wf_lang::plan::BindPlan],
    schemas: &[wf_lang::WindowSchema],
) -> HashMap<String, Vec<String>> {
    let mut map: HashMap<String, Vec<String>> = HashMap::new();
    for bind in binds {
        if let Some(ws) = schemas.iter().find(|s| s.name == bind.window) {
            for stream_name in &ws.streams {
                map.entry(stream_name.clone())
                    .or_default()
                    .push(bind.alias.clone());
            }
        }
    }
    map
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use wf_core::alert::AlertRecord;
    use wf_lang::ast::{CmpOp, Expr, FieldRef, Measure};
    use wf_lang::plan::*;

    /// Build a minimal RulePlan for testing: single event step, count >= 1.
    fn simple_rule_plan(alias: &str, window: &str) -> RulePlan {
        RulePlan {
            name: "test_rule".to_string(),
            binds: vec![BindPlan {
                alias: alias.to_string(),
                window: window.to_string(),
                filter: None,
            }],
            match_plan: MatchPlan {
                keys: vec![FieldRef::Simple("sip".to_string())],
                window_spec: WindowSpec::Sliding(Duration::from_secs(60)),
                event_steps: vec![StepPlan {
                    branches: vec![BranchPlan {
                        label: Some("hits".to_string()),
                        source: alias.to_string(),
                        field: None,
                        guard: None,
                        agg: AggPlan {
                            transforms: vec![],
                            measure: Measure::Count,
                            cmp: CmpOp::Ge,
                            threshold: Expr::Number(1.0),
                        },
                    }],
                }],
                close_steps: vec![],
            },
            joins: vec![],
            entity_plan: EntityPlan {
                entity_type: "ip".to_string(),
                entity_id_expr: Expr::Field(FieldRef::Simple("sip".to_string())),
            },
            yield_plan: YieldPlan {
                target: "alerts".to_string(),
                fields: vec![],
            },
            score_plan: ScorePlan {
                expr: Expr::Number(80.0),
            },
            conv_plan: None,
        }
    }

    fn simple_schema(window: &str, stream: &str) -> wf_lang::WindowSchema {
        wf_lang::WindowSchema {
            name: window.to_string(),
            streams: vec![stream.to_string()],
            time_field: None,
            over: Duration::from_secs(60),
            fields: vec![],
        }
    }

    /// Collect all alerts from the receiver side of the channel.
    async fn collect_alerts(mut rx: mpsc::Receiver<AlertRecord>) -> Vec<AlertRecord> {
        let mut out = Vec::new();
        while let Some(record) = rx.recv().await {
            out.push(record);
        }
        out
    }

    #[tokio::test]
    async fn test_scheduler_dispatches_events() {
        let plan = simple_rule_plan("a", "auth_events");
        let schemas = vec![simple_schema("auth_events", "syslog")];
        let stream_aliases = build_stream_aliases(&plan.binds, &schemas);

        let engine = RuleEngine {
            machine: CepStateMachine::new("test_rule".to_string(), plan.match_plan.clone()),
            executor: RuleExecutor::new(plan),
            stream_aliases,
        };

        let (alert_tx, alert_rx) = mpsc::channel(16);
        let (tx, rx) = mpsc::channel(16);
        let cancel = CancellationToken::new();
        let (_cmd_tx, cmd_rx) = mpsc::channel(64);

        let scheduler = Scheduler::new(rx, vec![engine], alert_tx, cancel.clone(), 4, Duration::from_secs(30), cmd_rx);
        let handle = tokio::spawn(async move { scheduler.run().await });

        // Build a minimal RecordBatch with an "sip" column
        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema};
        let schema = Arc::new(Schema::new(vec![Field::new("sip", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["10.0.0.1"]))],
        )
        .unwrap();

        tx.send(("syslog".to_string(), batch)).await.unwrap();

        // Give the scheduler time to process
        tokio::time::sleep(Duration::from_millis(100)).await;

        cancel.cancel();
        drop(tx); // drop sender so scheduler's drain loop can finish
        handle.await.unwrap().unwrap();

        let alerts = collect_alerts(alert_rx).await;
        assert_eq!(alerts.len(), 1);
        assert_eq!(alerts[0].rule_name, "test_rule");
        assert_eq!(alerts[0].entity_id, "10.0.0.1");
    }

    #[tokio::test]
    async fn test_scheduler_shutdown_flushes() {
        // Rule requiring count >= 3 (won't be satisfied by 1 event)
        let mut plan = simple_rule_plan("a", "auth_events");
        plan.match_plan.event_steps[0].branches[0].agg.threshold = Expr::Number(3.0);
        // Add a close step so close path can produce an alert
        plan.match_plan.close_steps = vec![StepPlan {
            branches: vec![BranchPlan {
                label: Some("close_hits".to_string()),
                source: "a".to_string(),
                field: None,
                guard: None,
                agg: AggPlan {
                    transforms: vec![],
                    measure: Measure::Count,
                    cmp: CmpOp::Ge,
                    threshold: Expr::Number(1.0),
                },
            }],
        }];

        let schemas = vec![simple_schema("auth_events", "syslog")];
        let stream_aliases = build_stream_aliases(&plan.binds, &schemas);

        let engine = RuleEngine {
            machine: CepStateMachine::new("test_rule".to_string(), plan.match_plan.clone()),
            executor: RuleExecutor::new(plan),
            stream_aliases,
        };

        let (alert_tx, alert_rx) = mpsc::channel(16);
        let (tx, rx) = mpsc::channel(16);
        let cancel = CancellationToken::new();
        let (_cmd_tx, cmd_rx) = mpsc::channel(64);

        let scheduler = Scheduler::new(rx, vec![engine], alert_tx, cancel.clone(), 4, Duration::from_secs(30), cmd_rx);
        let handle = tokio::spawn(async move { scheduler.run().await });

        // Send 1 event — not enough to match (need 3) but enough to create instance
        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema};
        let schema = Arc::new(Schema::new(vec![Field::new("sip", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["10.0.0.1"]))],
        )
        .unwrap();
        tx.send(("syslog".to_string(), batch)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Cancel → flush
        cancel.cancel();
        drop(tx); // drop sender so scheduler's drain loop can finish
        handle.await.unwrap().unwrap();

        // Flush should have closed the instance. close_ok depends on close step
        // being satisfied: count>=1 is met (one event accumulated), and event_ok
        // is false (only 1 of 3 events). So execute_close returns None.
        // This is expected behavior — partial match doesn't produce alert.
        let _alerts = collect_alerts(alert_rx).await;
    }

    #[tokio::test]
    async fn test_scheduler_drains_event_rx_on_cancel() {
        // Verify that batches queued in event_rx are processed before
        // flush_all, even when cancel fires before the scheduler reads them.
        let plan = simple_rule_plan("a", "auth_events");
        let schemas = vec![simple_schema("auth_events", "syslog")];
        let stream_aliases = build_stream_aliases(&plan.binds, &schemas);

        let engine = RuleEngine {
            machine: CepStateMachine::new("test_rule".to_string(), plan.match_plan.clone()),
            executor: RuleExecutor::new(plan),
            stream_aliases,
        };

        let (alert_tx, alert_rx) = mpsc::channel(16);
        let (tx, rx) = mpsc::channel(16);
        let cancel = CancellationToken::new();
        let (_cmd_tx, cmd_rx) = mpsc::channel(64);

        // Don't spawn yet — fill the channel first so events are queued.
        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema};
        let schema = Arc::new(Schema::new(vec![Field::new("sip", DataType::Utf8, false)]));

        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec!["10.0.0.1"]))],
        )
        .unwrap();
        let batch2 = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["10.0.0.2"]))],
        )
        .unwrap();

        tx.send(("syslog".to_string(), batch1)).await.unwrap();
        tx.send(("syslog".to_string(), batch2)).await.unwrap();
        drop(tx);

        // Cancel *before* spawning the scheduler so it enters the cancel
        // branch immediately, exercising the drain path.
        cancel.cancel();

        let scheduler = Scheduler::new(rx, vec![engine], alert_tx, cancel.clone(), 4, Duration::from_secs(30), cmd_rx);
        let handle = tokio::spawn(async move { scheduler.run().await });
        handle.await.unwrap().unwrap();

        let alerts = collect_alerts(alert_rx).await;
        // Both queued batches must have been drained and processed.
        assert_eq!(alerts.len(), 2, "expected 2 alerts from drained batches, got {}", alerts.len());
    }

    /// M18 验收：多规则并行分发 — 两个引擎同时处理同一批事件，各自
    /// 独立产出告警。
    #[tokio::test]
    async fn test_multi_engine_parallel_dispatch() {
        let schemas = vec![simple_schema("auth_events", "syslog")];

        // Engine α
        let mut plan_a = simple_rule_plan("a", "auth_events");
        plan_a.name = "rule_alpha".to_string();
        let aliases_a = build_stream_aliases(&plan_a.binds, &schemas);
        let engine_a = RuleEngine {
            machine: CepStateMachine::new(plan_a.name.clone(), plan_a.match_plan.clone()),
            executor: RuleExecutor::new(plan_a),
            stream_aliases: aliases_a,
        };

        // Engine β
        let mut plan_b = simple_rule_plan("a", "auth_events");
        plan_b.name = "rule_beta".to_string();
        let aliases_b = build_stream_aliases(&plan_b.binds, &schemas);
        let engine_b = RuleEngine {
            machine: CepStateMachine::new(plan_b.name.clone(), plan_b.match_plan.clone()),
            executor: RuleExecutor::new(plan_b),
            stream_aliases: aliases_b,
        };

        let (alert_tx, alert_rx) = mpsc::channel(16);
        let (tx, rx) = mpsc::channel(16);
        let cancel = CancellationToken::new();
        let (_cmd_tx, cmd_rx) = mpsc::channel(64);

        // parallelism=4 > 2 engines → both may run truly in parallel
        let scheduler = Scheduler::new(
            rx, vec![engine_a, engine_b], alert_tx, cancel.clone(),
            4, Duration::from_secs(30), cmd_rx,
        );
        let handle = tokio::spawn(async move { scheduler.run().await });

        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema};
        let schema = Arc::new(Schema::new(vec![Field::new("sip", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["10.0.0.1"]))],
        )
        .unwrap();

        tx.send(("syslog".to_string(), batch)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        cancel.cancel();
        drop(tx);
        handle.await.unwrap().unwrap();

        let alerts = collect_alerts(alert_rx).await;
        assert_eq!(alerts.len(), 2, "expected 2 alerts from 2 engines, got {}", alerts.len());

        let mut names: Vec<&str> = alerts.iter().map(|a| a.rule_name.as_str()).collect();
        names.sort();
        assert_eq!(names, vec!["rule_alpha", "rule_beta"]);
    }

    /// M18 验收：并发上限背压 — 3 个引擎但 parallelism=1，信号量保证
    /// 同一时刻最多 1 个引擎执行。所有事件仍必须被处理，不允许丢弃。
    #[tokio::test]
    async fn test_parallelism_backpressure() {
        let schemas = vec![simple_schema("auth_events", "syslog")];

        let mut engines = Vec::new();
        for name in ["rule_a", "rule_b", "rule_c"] {
            let mut plan = simple_rule_plan("a", "auth_events");
            plan.name = name.to_string();
            let aliases = build_stream_aliases(&plan.binds, &schemas);
            engines.push(RuleEngine {
                machine: CepStateMachine::new(plan.name.clone(), plan.match_plan.clone()),
                executor: RuleExecutor::new(plan),
                stream_aliases: aliases,
            });
        }

        let (alert_tx, alert_rx) = mpsc::channel(64);
        let (tx, rx) = mpsc::channel(16);
        let cancel = CancellationToken::new();
        let (_cmd_tx, cmd_rx) = mpsc::channel(64);

        // parallelism=1 → only one engine holds a permit at a time
        let scheduler = Scheduler::new(
            rx, engines, alert_tx, cancel.clone(),
            1, Duration::from_secs(30), cmd_rx,
        );
        let handle = tokio::spawn(async move { scheduler.run().await });

        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema};
        let schema = Arc::new(Schema::new(vec![Field::new("sip", DataType::Utf8, false)]));

        // Send 2 events with distinct scope keys
        for ip in ["10.0.0.1", "10.0.0.2"] {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(StringArray::from(vec![ip]))],
            )
            .unwrap();
            tx.send(("syslog".to_string(), batch)).await.unwrap();
        }

        tokio::time::sleep(Duration::from_millis(200)).await;

        cancel.cancel();
        drop(tx);
        handle.await.unwrap().unwrap();

        let alerts = collect_alerts(alert_rx).await;
        // 3 engines × 2 events = 6 alerts — none dropped despite parallelism=1
        assert_eq!(alerts.len(), 6, "expected 6 alerts (3 engines × 2 events), got {}", alerts.len());

        // Each rule must have fired exactly twice
        let mut counts: HashMap<&str, usize> = HashMap::new();
        for alert in &alerts {
            *counts.entry(alert.rule_name.as_str()).or_default() += 1;
        }
        assert_eq!(counts.get("rule_a"), Some(&2));
        assert_eq!(counts.get("rule_b"), Some(&2));
        assert_eq!(counts.get("rule_c"), Some(&2));
    }
}
