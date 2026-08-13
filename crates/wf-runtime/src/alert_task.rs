use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

use tokio::sync::mpsc;

use wf_engine::alert::OutputRecord;
use wf_engine::sink::{SinkDispatcher, SinkRuntime};

use crate::metrics::RuntimeMetrics;

/// Bounded channel capacity for each sink's delivery channel.
/// Sized to absorb brief sink slowdowns; under sustained backlog the sender
/// blocks (backpressure) rather than buffering infinitely.
pub const SINK_CHANNEL_CAPACITY: usize = 2048;

/// Resolved delivery fanout: `yield_target → sink senders`.
///
/// Replaces the fixed two-consumer alert pipeline + per-alert wildcard routing:
/// each sink owns a bounded channel + a consumer task, and the wildcard routes
/// are resolved once per yield_target (cached) at delivery time.
pub struct SinkFanout {
    /// `Arc<SinkRuntime>` pointer identity → sender.
    pub(crate) by_sink: HashMap<usize, mpsc::Sender<Arc<OutputRecord>>>,
    /// yield_target → resolved senders (cache).
    cache: RwLock<HashMap<String, Arc<Vec<mpsc::Sender<Arc<OutputRecord>>>>>>,
    /// On-demand resolver (wildcard routes + default fallback). `None` for a
    /// closed/empty fanout (e.g. the reload-during-shutdown fallback).
    dispatcher: Option<Arc<SinkDispatcher>>,
    /// Targets already warned about having no sink (dedup).
    warned_no_sink: Mutex<HashSet<String>>,
}

impl SinkFanout {
    /// Build a fanout from the resolved sink→sender map.
    pub(crate) fn new(
        by_sink: HashMap<usize, mpsc::Sender<Arc<OutputRecord>>>,
        dispatcher: Arc<SinkDispatcher>,
    ) -> Self {
        Self {
            by_sink,
            cache: RwLock::new(HashMap::new()),
            dispatcher: Some(dispatcher),
            warned_no_sink: Mutex::new(HashSet::new()),
        }
    }

    /// A closed/empty fanout: resolves every target to no senders (drops).
    pub(crate) fn closed() -> Arc<Self> {
        Self::from_resolved(HashMap::new())
    }

    /// Build a fanout from a pre-resolved target→senders map (no on-demand
    /// resolver). Used by the reload fallback and by tests.
    pub(crate) fn from_resolved(
        cache: HashMap<String, Arc<Vec<mpsc::Sender<Arc<OutputRecord>>>>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            by_sink: HashMap::new(),
            cache: RwLock::new(cache),
            dispatcher: None,
            warned_no_sink: Mutex::new(HashSet::new()),
        })
    }

    /// Resolve the sink senders for a yield_target, caching the result.
    pub fn resolve(&self, window_name: &str) -> Arc<Vec<mpsc::Sender<Arc<OutputRecord>>>> {
        if let Some(senders) = self
            .cache
            .read()
            .expect("sink fanout cache lock poisoned")
            .get(window_name)
        {
            return Arc::clone(senders);
        }
        let sinks = match &self.dispatcher {
            Some(dispatcher) => dispatcher.resolve_sinks(window_name),
            None => Vec::new(),
        };
        let senders: Vec<_> = sinks
            .iter()
            .filter_map(|sink| self.by_sink.get(&(Arc::as_ptr(sink) as usize)).cloned())
            .collect();
        let senders = Arc::new(senders);
        self.cache
            .write()
            .expect("sink fanout cache lock poisoned")
            .insert(window_name.to_string(), Arc::clone(&senders));
        senders
    }

    /// Warn once-per-target when a yield_target has no sink at all.
    pub fn warn_if_no_sink(&self, window_name: &str) {
        let mut warned = self.warned_no_sink.lock().expect("warned lock poisoned");
        if warned.insert(window_name.to_string()) {
            wf_warn!(
                pipe,
                target = %window_name,
                reason = "no_matching_sink",
                "alert not dispatched"
            );
        }
    }
}

/// Consume alert records for a single sink: serialize, send, escalate on error.
///
/// Each sink owns one of these, so a slow sink only backpressures its own
/// channel (and the rules emitting to that target), not every other sink.
pub async fn run_sink_consumer(
    mut rx: mpsc::Receiver<Arc<OutputRecord>>,
    sink: Arc<SinkRuntime>,
    error_txs: Arc<Vec<mpsc::Sender<Arc<OutputRecord>>>>,
    metrics: Option<Arc<RuntimeMetrics>>,
) {
    while let Some(record) = rx.recv().await {
        if let Some(metrics) = &metrics {
            metrics.set_alert_channel_depth(rx.len() as u64);
        }
        let data = match record.to_data_record() {
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
        if let Err(e) = sink.send_record(&data).await {
            log::warn!("sink {:?} dispatch error: {e}", sink.name);
            if let Some(metrics) = &metrics {
                metrics.inc_sink_dispatch_failed();
            }
            // Escalate to error sinks (best-effort, no error-of-error loop).
            for tx in error_txs.iter() {
                if tx.send(Arc::clone(&record)).await.is_err() {
                    break;
                }
            }
        }
        if let Some(metrics) = &metrics {
            metrics.inc_alert_dispatch();
            metrics.observe_alert_dispatch(dispatch_started.elapsed());
        }
    }
    // Channel closed (all producers dropped): stop the sink.
    let _ = sink.stop().await;
}
