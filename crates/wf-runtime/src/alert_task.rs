use std::sync::Arc;
use std::time::Instant;

use tokio::sync::mpsc;

use wf_engine::alert::OutputRecord;
use wf_engine::sink::SinkDispatcher;

use crate::metrics::RuntimeMetrics;

/// Bounded channel capacity for the alert pipeline.
/// Sized to absorb brief dispatch slowdowns without going unbounded; under
/// sustained backlog the sender applies backpressure (`send().await`) rather
/// than buffering infinitely.
pub const ALERT_CHANNEL_CAPACITY: usize = 2048;

/// Consume alert records from the channel and route them via the connector-based
/// `SinkDispatcher`.
///
/// Shutdown is driven by channel close: when the scheduler finishes
/// its drain + flush and drops its `Sender<OutputRecord>`, `rx.recv()` returns
/// `None` and this task exits. After all records are consumed, all sinks in
/// the dispatcher are gracefully stopped.
pub async fn run_alert_dispatcher(
    mut rx: mpsc::Receiver<OutputRecord>,
    dispatcher: Arc<SinkDispatcher>,
    metrics: Option<Arc<RuntimeMetrics>>,
) {
    // Targets already warned about having no sink — dedup so a high-volume
    // yield_target doesn't flood the log with one warn per alert.
    let mut warned_no_sink: std::collections::HashSet<String> = std::collections::HashSet::new();
    while let Some(record) = rx.recv().await {
        if let Some(metrics) = &metrics {
            metrics.set_alert_channel_depth(rx.len() as u64);
        }
        let data_record = match record.to_data_record() {
            Ok(data) => data,
            Err(e) => {
                if let Some(metrics) = &metrics {
                    metrics.inc_alert_serialize_failed();
                }
                log::warn!("alert export error: {e}");
                continue;
            }
        };
        let dispatch_started = Instant::now();
        let (matched, had_error) = dispatcher
            .dispatch(&record.yield_target, &data_record)
            .await;
        if had_error && let Some(metrics) = &metrics {
            metrics.inc_sink_dispatch_failed();
        }
        // Dispatch guard: warn once-per-target when a yield target has no sink
        // (route miss + no default sinks). Without this, alerts silently vanish.
        if matched == 0
            && dispatcher.has_no_default_sinks()
            && warned_no_sink.insert(record.yield_target.clone())
        {
            log::warn!(
                "alert for rule {:?} yield_target={:?} matched no sink \
                 (no route and no default sink configured) — further alerts \
                 to this target will be dropped silently",
                record.rule_name,
                record.yield_target
            );
        }
        if let Some(metrics) = &metrics {
            metrics.inc_alert_dispatch();
            metrics.observe_alert_dispatch(dispatch_started.elapsed());
        }
    }
    dispatcher.stop_all().await;
}
