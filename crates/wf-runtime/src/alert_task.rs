use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use wf_core::alert::{AlertRecord, AlertSink};

/// Bounded channel capacity for the alert pipeline.
pub const ALERT_CHANNEL_CAPACITY: usize = 64;

/// Consume alert records from the channel and forward them to the sink.
///
/// Primary shutdown: channel close (all senders dropped after scheduler
/// `flush_all`). The cancel token is a backup to unblock `recv` if the
/// sender side hangs.
pub async fn run_alert_sink(
    mut rx: mpsc::Receiver<AlertRecord>,
    sink: Arc<dyn AlertSink>,
    cancel: CancellationToken,
) {
    loop {
        tokio::select! {
            msg = rx.recv() => {
                match msg {
                    Some(record) => {
                        if let Err(e) = sink.send(&record) {
                            log::warn!("alert sink error: {e}");
                        }
                    }
                    None => break, // all senders dropped
                }
            }
            _ = cancel.cancelled() => {
                // Drain remaining messages before exiting.
                rx.close();
                while let Some(record) = rx.recv().await {
                    if let Err(e) = sink.send(&record) {
                        log::warn!("alert sink drain error: {e}");
                    }
                }
                break;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;
    use std::time::Duration;

    fn sample_alert(id: &str) -> AlertRecord {
        AlertRecord {
            alert_id: id.to_string(),
            rule_name: "test_rule".to_string(),
            score: 50.0,
            entity_type: "ip".to_string(),
            entity_id: "10.0.0.1".to_string(),
            close_reason: None,
            fired_at: "2024-01-01T00:00:00Z".to_string(),
            matched_rows: vec![],
            summary: "test".to_string(),
        }
    }

    struct CollectorSink {
        alerts: Mutex<Vec<String>>,
    }

    impl CollectorSink {
        fn new() -> Self {
            Self {
                alerts: Mutex::new(Vec::new()),
            }
        }
        fn ids(&self) -> Vec<String> {
            self.alerts.lock().unwrap().clone()
        }
    }

    impl AlertSink for CollectorSink {
        fn send(&self, record: &AlertRecord) -> anyhow::Result<()> {
            self.alerts.lock().unwrap().push(record.alert_id.clone());
            Ok(())
        }
    }

    struct FailCountSink {
        call_count: AtomicUsize,
    }

    impl FailCountSink {
        fn new() -> Self {
            Self {
                call_count: AtomicUsize::new(0),
            }
        }
        fn count(&self) -> usize {
            self.call_count.load(Ordering::SeqCst)
        }
    }

    impl AlertSink for FailCountSink {
        fn send(&self, _record: &AlertRecord) -> anyhow::Result<()> {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            anyhow::bail!("intentional");
        }
    }

    #[tokio::test]
    async fn normal_delivery() {
        let (tx, rx) = mpsc::channel(16);
        let sink = Arc::new(CollectorSink::new());
        let cancel = CancellationToken::new();

        let task_sink = Arc::clone(&sink);
        let task_cancel = cancel.clone();
        let handle = tokio::spawn(async move {
            run_alert_sink(rx, task_sink, task_cancel).await;
        });

        tx.send(sample_alert("a1")).await.unwrap();
        tx.send(sample_alert("a2")).await.unwrap();
        drop(tx); // close channel

        handle.await.unwrap();
        assert_eq!(sink.ids(), vec!["a1", "a2"]);
    }

    #[tokio::test]
    async fn drain_on_cancel() {
        let (tx, rx) = mpsc::channel(16);
        let sink = Arc::new(CollectorSink::new());
        let cancel = CancellationToken::new();

        let task_sink = Arc::clone(&sink);
        let task_cancel = cancel.clone();
        let handle = tokio::spawn(async move {
            run_alert_sink(rx, task_sink, task_cancel).await;
        });

        // Send some alerts, then cancel before dropping tx
        tx.send(sample_alert("b1")).await.unwrap();
        tx.send(sample_alert("b2")).await.unwrap();

        // Give the task time to start processing
        tokio::time::sleep(Duration::from_millis(50)).await;

        cancel.cancel();
        // Drop tx after cancel so the drain loop can finish
        drop(tx);

        handle.await.unwrap();
        // Both alerts should have been delivered (either in the main loop or drain)
        assert_eq!(sink.ids().len(), 2);
    }

    #[tokio::test]
    async fn sink_error_continues() {
        let (tx, rx) = mpsc::channel(16);
        let sink = Arc::new(FailCountSink::new());
        let cancel = CancellationToken::new();

        let task_sink = Arc::clone(&sink);
        let task_cancel = cancel.clone();
        let handle = tokio::spawn(async move {
            run_alert_sink(rx, task_sink, task_cancel).await;
        });

        tx.send(sample_alert("c1")).await.unwrap();
        tx.send(sample_alert("c2")).await.unwrap();
        drop(tx);

        handle.await.unwrap();
        // Both alerts were attempted despite errors
        assert_eq!(sink.count(), 2);
    }
}
