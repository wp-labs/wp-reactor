use std::sync::Arc;

use tokio::sync::mpsc;

use wf_core::alert::AlertRecord;
use wf_core::sink::SinkDispatcher;

/// Bounded channel capacity for the alert pipeline.
pub const ALERT_CHANNEL_CAPACITY: usize = 64;

/// Consume alert records from the channel and route them via the connector-based
/// `SinkDispatcher`.
///
/// Shutdown is driven by channel close: when the scheduler finishes
/// its drain + flush and drops its `Sender<AlertRecord>`, `rx.recv()` returns
/// `None` and this task exits. After all records are consumed, all sinks in
/// the dispatcher are gracefully stopped.
pub async fn run_alert_dispatcher(
    mut rx: mpsc::Receiver<AlertRecord>,
    dispatcher: Arc<SinkDispatcher>,
) {
    while let Some(record) = rx.recv().await {
        let json = match serde_json::to_string(&record) {
            Ok(j) => j,
            Err(e) => {
                log::warn!("alert serialize error: {e}");
                continue;
            }
        };
        dispatcher.dispatch(&record.yield_target, &json).await;
    }
    dispatcher.stop_all().await;
}
