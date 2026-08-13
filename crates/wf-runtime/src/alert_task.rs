use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, RwLock};
use std::sync::atomic::Ordering;
use std::time::Instant;

use tokio::sync::mpsc;

use wp_model_core::model::DataRecord;
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
/// Resolved per-sink channel groups for a yield target: each entry is
/// `(sink_ptr, channels)` where `channels` are the sink's `parallel` writers.
type ResolvedChannels = Arc<Vec<(usize, Arc<Vec<mpsc::Sender<Arc<DataRecord>>>>)>>;

pub struct SinkFanout {
    /// `Arc<SinkRuntime>` pointer identity → its parallel writers.
    pub(crate) by_sink: HashMap<usize, Vec<mpsc::Sender<Arc<DataRecord>>>>,
    /// Per-sink round-robin index (across the sink's parallel writers).
    rr: HashMap<usize, std::sync::atomic::AtomicUsize>,
    /// yield_target → resolved channel groups (cache).
    cache: RwLock<HashMap<String, ResolvedChannels>>,
    /// On-demand resolver (wildcard routes + default fallback). `None` for a
    /// closed/empty fanout (e.g. the reload-during-shutdown fallback).
    dispatcher: Option<Arc<SinkDispatcher>>,
    /// Targets already warned about having no sink (dedup).
    warned_no_sink: Mutex<HashSet<String>>,
}

impl SinkFanout {
    /// Build a fanout from the resolved sink→writers map.
    pub(crate) fn new(
        by_sink: HashMap<usize, Vec<mpsc::Sender<Arc<DataRecord>>>>,
        dispatcher: Arc<SinkDispatcher>,
    ) -> Self {
        let rr = by_sink.keys().map(|&k| (k, std::sync::atomic::AtomicUsize::new(0))).collect();
        Self {
            by_sink,
            rr,
            cache: RwLock::new(HashMap::new()),
            dispatcher: Some(dispatcher),
            warned_no_sink: Mutex::new(HashSet::new()),
        }
    }

    /// A closed/empty fanout: resolves every target to no senders (drops).
    pub(crate) fn closed() -> Arc<Self> {
        Self::from_resolved(HashMap::new())
    }

    /// Build a fanout from a pre-resolved target→channel-groups map (no
    /// on-demand resolver). Used by the reload fallback and by tests.
    pub(crate) fn from_resolved(
        cache: HashMap<String, ResolvedChannels>,
    ) -> Arc<Self> {
        Arc::new(Self {
            by_sink: HashMap::new(),
            rr: HashMap::new(),
            cache: RwLock::new(cache),
            dispatcher: None,
            warned_no_sink: Mutex::new(HashSet::new()),
        })
    }

    /// Resolve the per-sink channel groups for a yield_target, caching.
    ///
    /// Each entry is `(sink_ptr, channels)` where `channels` are that sink's
    /// `parallel` writers — the emit path round-robins across them.
    pub fn resolve(&self, window_name: &str) -> ResolvedChannels {
        if let Some(groups) = self
            .cache
            .read()
            .expect("sink fanout cache lock poisoned")
            .get(window_name)
        {
            return Arc::clone(groups);
        }
        let sinks = match &self.dispatcher {
            Some(dispatcher) => dispatcher.resolve_sinks(window_name),
            None => Vec::new(),
        };
        let groups: Vec<_> = sinks
            .iter()
            .filter_map(|sink| {
                let ptr = Arc::as_ptr(sink) as usize;
                self.by_sink
                    .get(&ptr)
                    .map(|channels| (ptr, Arc::new(channels.clone())))
            })
            .collect();
        let groups = Arc::new(groups);
        self.cache
            .write()
            .expect("sink fanout cache lock poisoned")
            .insert(window_name.to_string(), Arc::clone(&groups));
        groups
    }

    /// Next round-robin writer index for a sink's parallel channels.
    pub fn next_index(&self, sink_ptr: usize, writer_count: usize) -> usize {
        if writer_count <= 1 {
            return 0;
        }
        self.rr
            .get(&sink_ptr)
            .map(|idx| idx.fetch_add(1, Ordering::Relaxed) % writer_count)
            .unwrap_or(0)
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
    mut rx: mpsc::Receiver<Arc<DataRecord>>,
    sink: Arc<SinkRuntime>,
    error_txs: Arc<Vec<mpsc::Sender<Arc<DataRecord>>>>,
    metrics: Option<Arc<RuntimeMetrics>>,
) {
    while let Some(data) = rx.recv().await {
        if let Some(metrics) = &metrics {
            metrics.set_alert_channel_depth(rx.len() as u64);
        }
        let dispatch_started = Instant::now();
        if let Err(e) = sink.send_record(&data).await {
            log::warn!("sink {:?} dispatch error: {e}", sink.name);
            if let Some(metrics) = &metrics {
                metrics.inc_sink_dispatch_failed();
            }
            // Escalate to error sinks (best-effort, no error-of-error loop).
            for tx in error_txs.iter() {
                if tx.send(Arc::clone(&data)).await.is_err() {
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
