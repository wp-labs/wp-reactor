use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::task::Poll;
use std::time::Duration;

static TASK_SEQ: AtomicU64 = AtomicU64::new(0);

use tokio::sync::{Notify, mpsc};
use tokio_util::sync::CancellationToken;

use wf_core::alert::AlertRecord;
use wf_core::rule::{CepStateMachine, CloseReason, RuleExecutor, StepResult, batch_to_events};
use wf_core::window::Window;

// ---------------------------------------------------------------------------
// WindowSource — one window a rule task reads from
// ---------------------------------------------------------------------------

pub(crate) struct WindowSource {
    pub window_name: String,
    pub window: Arc<RwLock<Window>>,
    pub notify: Arc<Notify>,
    /// Stream names that flow into this window.
    pub stream_names: Vec<String>,
}

// ---------------------------------------------------------------------------
// RuleTaskConfig — everything needed to construct a RuleTask
// ---------------------------------------------------------------------------

pub(crate) struct RuleTaskConfig {
    pub machine: CepStateMachine,
    pub executor: RuleExecutor,
    pub window_sources: Vec<WindowSource>,
    /// stream_name → Vec<alias>: which CEP aliases receive events from each stream.
    pub stream_aliases: HashMap<String, Vec<String>>,
    pub alert_tx: mpsc::Sender<AlertRecord>,
    pub cancel: CancellationToken,
    pub timeout_scan_interval: Duration,
}

// ---------------------------------------------------------------------------
// RuleTask — runtime state for a single rule
// ---------------------------------------------------------------------------

/// Holds all mutable state for one rule's processing loop.
///
/// Each `RuleTask` owns its `CepStateMachine` exclusively (no `Arc<Mutex>`).
struct RuleTask {
    task_id: String,
    machine: CepStateMachine,
    executor: RuleExecutor,
    sources: Vec<WindowSource>,
    /// window_name → Vec<alias>: pre-computed from stream_aliases + window sources.
    aliases: HashMap<String, Vec<String>>,
    alert_tx: mpsc::Sender<AlertRecord>,
    /// window_name → cursor: tracks read position per window.
    cursors: HashMap<String, u64>,
}

impl RuleTask {
    fn new(config: RuleTaskConfig) -> (Self, CancellationToken, Duration) {
        let RuleTaskConfig {
            machine,
            executor,
            window_sources,
            stream_aliases,
            alert_tx,
            cancel,
            timeout_scan_interval,
        } = config;

        // Pre-compute aliases per window: for each window, collect all
        // aliases from all streams that flow into it (deduplicated).
        let aliases: HashMap<String, Vec<String>> = window_sources
            .iter()
            .map(|src| {
                let window_aliases: Vec<String> = src
                    .stream_names
                    .iter()
                    .flat_map(|s| stream_aliases.get(s).into_iter().flatten())
                    .cloned()
                    .collect::<HashSet<_>>()
                    .into_iter()
                    .collect();
                (src.window_name.clone(), window_aliases)
            })
            .collect();

        // Initialize cursors to current position (skip historical data).
        let cursors: HashMap<String, u64> = window_sources
            .iter()
            .map(|src| {
                let seq = src.window.read().expect("lock poisoned").next_seq();
                (src.window_name.clone(), seq)
            })
            .collect();

        let seq = TASK_SEQ.fetch_add(1, Ordering::Relaxed);
        let task_id = format!("{}#{}", machine.rule_name(), seq);

        let task = Self {
            task_id,
            machine,
            executor,
            sources: window_sources,
            aliases,
            alert_tx,
            cursors,
        };
        (task, cancel, timeout_scan_interval)
    }

    // -- Data processing ----------------------------------------------------

    /// Read new batches from all windows, convert to events, and advance
    /// the state machine.
    async fn pull_and_advance(&mut self) {
        for source in &self.sources {
            let cursor = self.cursors.get(&source.window_name).copied().unwrap_or(0);
            let (batches, new_cursor, gap) = {
                let win = source.window.read().expect("lock poisoned");
                let result = win.read_since(cursor);
                wf_debug!(pipe,
                    task_id = %self.task_id,
                    window = %source.window_name,
                    cursor = cursor,
                    new_cursor = result.1,
                    batches = result.0.len(),
                    gap = result.2,
                    "read_since"
                );
                result
            };

            if gap {
                wf_warn!(pipe,
                    task_id = %self.task_id,
                    window = %source.window_name,
                    "cursor gap detected — some data was lost to eviction"
                );
            }
            self.cursors.insert(source.window_name.clone(), new_cursor);

            let Some(aliases) = self.aliases.get(&source.window_name) else {
                continue;
            };

            for batch in &batches {
                let events = batch_to_events(batch);
                for event in &events {
                    for alias in aliases {
                        if let StepResult::Matched(ctx) = self.machine.advance(alias, event) {
                            match self.executor.execute_match(&ctx) {
                                Ok(record) => self.emit(record).await,
                                Err(e) => wf_warn!(pipe, task_id = %self.task_id, error = %e, "execute_match error"),
                            }
                        }
                    }
                }
            }
        }
    }

    // -- Timeout & shutdown -------------------------------------------------

    /// Scan for expired state machine instances and emit alerts.
    async fn scan_timeouts(&mut self) {
        for close in &self.machine.scan_expired() {
            match self.executor.execute_close(close) {
                Ok(Some(record)) => self.emit(record).await,
                Ok(None) => {}
                Err(e) => wf_warn!(pipe, task_id = %self.task_id, error = %e, "execute_close error"),
            }
        }
    }

    /// Close all active instances (shutdown flush) and emit alerts.
    async fn flush(&mut self) {
        let mut emitted = 0usize;
        for close in &self.machine.close_all(CloseReason::Flush) {
            match self.executor.execute_close(close) {
                Ok(Some(record)) => {
                    self.emit(record).await;
                    emitted += 1;
                }
                Ok(None) => {}
                Err(e) => wf_warn!(pipe, task_id = %self.task_id, error = %e, "execute_close flush error"),
            }
        }
        if emitted > 0 {
            wf_debug!(pipe, task_id = %self.task_id, alerts = emitted, "flush complete");
        }
    }

    // -- Alert emission -----------------------------------------------------

    async fn emit(&self, record: AlertRecord) {
        if let Err(e) = self.alert_tx.send(record).await {
            wf_warn!(pipe, error = %e, "alert channel closed");
        }
    }
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

/// Run a single rule task until cancelled.
///
/// Wakes on window notifications, reads new batches via cursor-based
/// `read_since()`, converts them to events, and advances the state machine.
///
/// Uses `Notified::enable()` to register waiters before reading data,
/// ensuring no notifications are lost between data checks and waits.
pub(crate) async fn run_rule_task(config: RuleTaskConfig) -> anyhow::Result<()> {
    let (mut task, cancel, timeout_scan_interval) = RuleTask::new(config);
    let task_id = task.task_id.clone();
    let mut timeout_tick = tokio::time::interval(timeout_scan_interval);

    // Clone Arc<Notify> handles outside the struct so that notification
    // registration borrows `notifiers` (not `task`), allowing `&mut task`
    // for processing in the same loop iteration.
    let notifiers: Vec<Arc<Notify>> =
        task.sources.iter().map(|s| Arc::clone(&s.notify)).collect();

    loop {
        let mut notifications = register_notifications(&notifiers);
        task.pull_and_advance().await;

        tokio::select! {
            biased;
            _ = wait_any(&mut notifications) => {}
            _ = timeout_tick.tick() => task.scan_timeouts().await,
            _ = cancel.cancelled() => {
                task.pull_and_advance().await;
                task.flush().await;
                wf_debug!(pipe, task_id = %task_id, "rule task shutdown complete");
                break;
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Notification helpers
// ---------------------------------------------------------------------------

/// Register notification waiters and enable them immediately.
///
/// Must be called BEFORE [`RuleTask::pull_and_advance`] to avoid missing
/// notifications between data reads and waits.
fn register_notifications(
    notifiers: &[Arc<Notify>],
) -> Vec<Pin<Box<tokio::sync::futures::Notified<'_>>>> {
    let mut notified: Vec<_> = notifiers
        .iter()
        .map(|n| Box::pin(n.notified()))
        .collect();
    for n in &mut notified {
        n.as_mut().enable();
    }
    notified
}

/// Resolve when any pre-enabled Notified future fires.
async fn wait_any(notified: &mut [Pin<Box<tokio::sync::futures::Notified<'_>>>]) {
    if notified.is_empty() {
        std::future::pending::<()>().await;
        return;
    }
    std::future::poll_fn(|cx| {
        for n in notified.iter_mut() {
            if n.as_mut().poll(cx).is_ready() {
                return Poll::Ready(());
            }
        }
        Poll::Pending
    })
    .await;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::{StringArray, TimestampNanosecondArray};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
    use arrow::record_batch::RecordBatch;

    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    use tracing_subscriber::{EnvFilter, Layer, fmt};

    use wf_config::{DistMode, EvictPolicy, LatePolicy, WindowConfig};
    use wf_core::window::{Window, WindowParams};
    use wf_lang::ast::{CmpOp, Expr, FieldRef, Measure};
    use wf_lang::plan::{
        AggPlan, BindPlan, BranchPlan, EntityPlan, MatchPlan, RulePlan, ScorePlan, StepPlan,
        WindowSpec, YieldPlan,
    };

    use crate::tracing_init::DomainFormat;

    // -- helpers ------------------------------------------------------------

    /// Install a tracing subscriber that prints to the test harness.
    ///
    /// `cargo test` captures output by default; pass `--nocapture` to see it:
    /// ```sh
    /// cargo test -p wf-runtime -- engine_task::tests --nocapture
    /// ```
    /// Safe to call multiple times — subsequent calls are no-ops.
    fn init_tracing() {
        let _ = tracing_subscriber::registry()
            .with(
                fmt::layer()
                    .event_format(DomainFormat::new())
                    .with_test_writer()
                    .with_filter(EnvFilter::try_new("debug").unwrap()),
            )
            .try_init();
    }

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("sip", DataType::Utf8, true),
            Field::new(
                "event_time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
        ]))
    }

    fn test_window_config(max_bytes: usize) -> WindowConfig {
        WindowConfig {
            name: "auth_events".into(),
            mode: DistMode::Local,
            max_window_bytes: max_bytes.into(),
            over_cap: Duration::from_secs(3600).into(),
            evict_policy: EvictPolicy::TimeFirst,
            watermark: Duration::from_secs(0).into(),
            allowed_lateness: Duration::from_secs(3600).into(),
            late_policy: LatePolicy::Drop,
        }
    }

    fn make_window(
        name: &str,
        schema: &SchemaRef,
        max_bytes: usize,
    ) -> (Arc<RwLock<Window>>, Arc<Notify>) {
        let win = Window::new(
            WindowParams {
                name: name.into(),
                schema: schema.clone(),
                time_col_index: Some(1), // event_time is the second column
                over: Duration::from_secs(3600),
            },
            test_window_config(max_bytes),
        );
        (Arc::new(RwLock::new(win)), Arc::new(Notify::new()))
    }

    fn make_batch(schema: &SchemaRef, sips: &[&str], ts: i64) -> RecordBatch {
        let n = sips.len();
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(
                    sips.iter().map(|s| Some(*s)).collect::<Vec<_>>(),
                )),
                Arc::new(TimestampNanosecondArray::from(vec![ts; n])),
            ],
        )
        .unwrap()
    }

    /// Build a single-step count>=3 rule and return (task, alert_rx, window_arc, notify_arc).
    fn make_task() -> (
        RuleTask,
        mpsc::Receiver<AlertRecord>,
        Arc<RwLock<Window>>,
        Arc<Notify>,
    ) {
        make_task_with_window_bytes(usize::MAX)
    }

    /// Build a RuleTask for the following WFL rule:
    ///
    /// ```wfl
    /// rule test_rule {
    ///   events {
    ///     fail : auth_events           // stream "syslog"
    ///   }
    ///   match<sip:5m> {
    ///     on event {
    ///       fail | count >= 3;
    ///     }
    ///   } -> score(70.0)
    ///   entity(ip, fail.sip)
    ///   yield alerts ()
    /// }
    /// ```
    ///
    /// `max_bytes` controls the window's `max_window_bytes` for memory-pressure tests.
    fn make_task_with_window_bytes(
        max_bytes: usize,
    ) -> (
        RuleTask,
        mpsc::Receiver<AlertRecord>,
        Arc<RwLock<Window>>,
        Arc<Notify>,
    ) {
        let schema = test_schema();
        let (win_arc, notify_arc) = make_window("auth_events", &schema, max_bytes);

        let match_plan = MatchPlan {
            keys: vec![FieldRef::Simple("sip".into())],
            window_spec: WindowSpec::Sliding(Duration::from_secs(300)),
            event_steps: vec![StepPlan {
                branches: vec![BranchPlan {
                    label: Some("fail".into()),
                    source: "fail".into(),
                    field: None,
                    guard: None,
                    agg: AggPlan {
                        transforms: vec![],
                        measure: Measure::Count,
                        cmp: CmpOp::Ge,
                        threshold: Expr::Number(3.0),
                    },
                }],
            }],
            close_steps: vec![],
        };

        let rule_plan = RulePlan {
            name: "test_rule".into(),
            binds: vec![BindPlan {
                alias: "fail".into(),
                window: "auth_events".into(),
                filter: None,
            }],
            match_plan: match_plan.clone(),
            joins: vec![],
            entity_plan: EntityPlan {
                entity_type: "ip".into(),
                entity_id_expr: Expr::Field(FieldRef::Qualified("fail".into(), "sip".into())),
            },
            yield_plan: YieldPlan {
                target: "alerts".into(),
                fields: vec![],
            },
            score_plan: ScorePlan {
                expr: Expr::Number(70.0),
            },
            conv_plan: None,
        };

        let machine = CepStateMachine::new("test_rule".into(), match_plan, None);
        let executor = RuleExecutor::new(rule_plan);

        let (alert_tx, alert_rx) = mpsc::channel(64);

        let config = RuleTaskConfig {
            machine,
            executor,
            window_sources: vec![WindowSource {
                window_name: "auth_events".into(),
                window: Arc::clone(&win_arc),
                notify: Arc::clone(&notify_arc),
                stream_names: vec!["syslog".into()],
            }],
            stream_aliases: HashMap::from([("syslog".into(), vec!["fail".into()])]),
            alert_tx,
            cancel: CancellationToken::new(),
            timeout_scan_interval: Duration::from_secs(60),
        };

        let (task, _cancel, _interval) = RuleTask::new(config);
        (task, alert_rx, win_arc, notify_arc)
    }

    // -- test cases ---------------------------------------------------------

    /// 空窗口：pull_and_advance 不产出任何 alert，cursor 保持不变。
    #[tokio::test]
    async fn pull_empty_window() {
        init_tracing();
        let (mut task, mut alert_rx, _win, _notify) = make_task();
        task.pull_and_advance().await;
        assert!(
            alert_rx.try_recv().is_err(),
            "empty window should produce no alerts"
        );
    }

    /// 游标推进：写入 1 batch（2 行，不满阈值），cursor 从 0 → 1；
    /// 再次拉取无新数据时 cursor 不变。
    #[tokio::test]
    async fn pull_advances_cursor() {
        init_tracing();
        let schema = test_schema();
        let (mut task, _alert_rx, win, _notify) = make_task();

        let ts = 1_700_000_000_000_000_000i64;
        let batch = make_batch(&schema, &["10.0.0.1", "10.0.0.2"], ts);
        win.write().unwrap().append(batch).unwrap();

        task.pull_and_advance().await;
        let cursor = task.cursors["auth_events"];
        assert_eq!(cursor, 1, "cursor should advance to 1 after reading one batch");

        // 无新数据 — cursor 不变
        task.pull_and_advance().await;
        let cursor2 = task.cursors["auth_events"];
        assert_eq!(cursor2, 1, "cursor should remain 1 with no new data");
    }

    /// 触发告警：写入同一 sip 的 3 行（满足 count >= 3），
    /// 验证 alert 的 rule_name / entity_type / entity_id / score 正确。
    #[tokio::test]
    async fn pull_triggers_alert() {
        init_tracing();
        let schema = test_schema();
        let (mut task, mut alert_rx, win, _notify) = make_task();

        let ts = 1_700_000_000_000_000_000i64;
        let batch = make_batch(&schema, &["10.0.0.1", "10.0.0.1", "10.0.0.1"], ts);
        win.write().unwrap().append(batch).unwrap();

        task.pull_and_advance().await;

        let alert = alert_rx
            .try_recv()
            .expect("should have produced an alert");
        assert_eq!(alert.rule_name, "test_rule");
        assert_eq!(alert.entity_type, "ip");
        assert_eq!(alert.entity_id, "10.0.0.1");
        assert!((alert.score - 70.0).abs() < f64::EPSILON);
    }

    /// 多 key 隔离计数：
    ///   batch1: 2×A + 2×B → 各 count=2，均不满 3 → 0 alerts
    ///   batch2: 1×A       → A count=3 触发 → 1 alert，B 不受影响
    #[tokio::test]
    async fn pull_multiple_keys_isolated() {
        init_tracing();
        let schema = test_schema();
        let (mut task, mut alert_rx, win, _notify) = make_task();

        let ts = 1_700_000_000_000_000_000i64;
        let batch1 = make_batch(
            &schema,
            &["10.0.0.1", "10.0.0.1", "10.0.0.2", "10.0.0.2"],
            ts,
        );
        win.write().unwrap().append(batch1).unwrap();
        task.pull_and_advance().await;
        assert!(
            alert_rx.try_recv().is_err(),
            "neither key should trigger at count=2"
        );

        // A 再追加 1 行 → 达到 3
        let batch2 = make_batch(&schema, &["10.0.0.1"], ts + 1_000_000_000);
        win.write().unwrap().append(batch2).unwrap();
        task.pull_and_advance().await;

        let alert = alert_rx
            .try_recv()
            .expect("sip=10.0.0.1 should trigger at count=3");
        assert_eq!(alert.entity_id, "10.0.0.1");

        // B 仍为 2 — 不应触发
        assert!(
            alert_rx.try_recv().is_err(),
            "sip=10.0.0.2 should not trigger"
        );
    }

    /// 游标 gap 检测：
    ///   将 max_window_bytes 限制为仅容纳 1 个 batch，
    ///   写入 batch0（seq=0）再写入 batch1（seq=1）时 batch0 被淘汰。
    ///   cursor 从 0 开始，read_since 发现 oldest_seq=1 > cursor=0 → gap=true，
    ///   日志输出 WARN "cursor gap detected"，cursor 跳到 2。
    #[tokio::test]
    async fn pull_detects_gap() {
        init_tracing();
        let schema = test_schema();
        // 测量单个 batch 的内存占用，限制窗口只能容纳 1 个
        let batch_size = {
            let tmp = make_batch(&schema, &["10.0.0.1"], 1_000_000_000);
            tmp.get_array_memory_size()
        };
        let (mut task, _alert_rx, win, _notify) = make_task_with_window_bytes(batch_size);

        let ts = 1_700_000_000_000_000_000i64;

        // 强制 cursor=0（跳过 RuleTask::new 的 next_seq 初始化）以观察 gap
        task.cursors.insert("auth_events".into(), 0);

        // batch0 (seq=0)
        let batch0 = make_batch(&schema, &["10.0.0.1"], ts);
        win.write().unwrap().append(batch0).unwrap();

        // batch1 (seq=1) — 内存超限，batch0 被淘汰
        let batch1 = make_batch(&schema, &["10.0.0.2"], ts + 1_000_000_000);
        win.write().unwrap().append(batch1).unwrap();

        assert_eq!(
            win.read().unwrap().batch_count(),
            1,
            "only 1 batch should remain after eviction"
        );

        // pull: cursor=0 但 oldest_seq=1 → gap=true, cursor 跳到 2
        task.pull_and_advance().await;

        let cursor = task.cursors["auth_events"];
        assert_eq!(cursor, 2, "cursor should advance to 2 (past the surviving batch)");
    }

    /// 关闭刷新：
    ///   写入 2 行同 IP（count=2 < 阈值 3），instance 已创建但未满足。
    ///   flush() → close_all(Flush) → event_ok=false（事件步骤未完成）
    ///   → execute_close 返回 None → 不产出 alert。
    ///   验证 flush 正确清理未完成实例，不会产生误报。
    #[tokio::test]
    async fn flush_closes_active_instances() {
        init_tracing();
        let schema = test_schema();
        let (mut task, mut alert_rx, win, _notify) = make_task();

        let ts = 1_700_000_000_000_000_000i64;
        let batch = make_batch(&schema, &["10.0.0.1", "10.0.0.1"], ts);
        win.write().unwrap().append(batch).unwrap();
        task.pull_and_advance().await;

        assert!(
            alert_rx.try_recv().is_err(),
            "count=2 should not trigger alert"
        );

        task.flush().await;

        assert!(
            alert_rx.try_recv().is_err(),
            "flush of incomplete instance should not produce alert"
        );
    }
}
