use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio_util::sync::CancellationToken;

use wf_core::window::{Evictor, Router};

/// Run the evictor periodically until cancelled.
#[tracing::instrument(name = "evictor", skip_all)]
pub async fn run_evictor(
    evictor: Evictor,
    router: Arc<Router>,
    interval: Duration,
    cancel: CancellationToken,
) {
    let mut tick = tokio::time::interval(interval);
    loop {
        tokio::select! {
            _ = tick.tick() => {
                let now_nanos = now_epoch_nanos();
                let report = evictor.run_once(router.registry(), now_nanos);
                if report.batches_time_evicted > 0 || report.batches_memory_evicted > 0 {
                    wf_debug!(res,
                        scanned = report.windows_scanned,
                        time_evicted = report.batches_time_evicted,
                        memory_evicted = report.batches_memory_evicted,
                        "evictor sweep"
                    );
                }
            }
            _ = cancel.cancelled() => break,
        }
    }
}

fn now_epoch_nanos() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64
}
