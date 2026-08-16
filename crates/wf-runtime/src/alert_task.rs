use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use wf_engine::alert::AlertColumnBatch;
use wf_engine::sink::{SinkDispatcher, SinkRuntime};
use wp_model_core::model::DataRecord;

use crate::metrics::RuntimeMetrics;

/// Bounded channel capacity for each sink's delivery channel.
/// Sized to absorb brief sink slowdowns; under sustained backlog the sender
/// blocks (backpressure) rather than buffering infinitely.
pub const SINK_CHANNEL_CAPACITY: usize = 2048;

/// Max wall time the sink consumers may keep flushing a buffered alert
/// backlog after cancel before dropping the rest. Matches the rule-task
/// shutdown drain budget so graceful shutdown stays bounded.
const SINK_DRAIN_BUDGET: Duration = Duration::from_secs(1);

/// Resolved delivery fanout: `yield_target → sink senders`.
///
/// Replaces the fixed two-consumer alert pipeline + per-alert wildcard routing:
/// each sink owns a bounded channel + a consumer task, and the wildcard routes
/// are resolved once per yield_target (cached) at delivery time.
/// A batch of alert records delivered to a sink writer.
///
/// Two payload forms:
/// - `Columns` (the emit path): records stored as per-field columns (see
///   `AlertColumnBatch`). Row structs are never materialized on this path;
///   payload-blind sinks confirm without reading the payload and
///   row-oriented sinks reconstruct `DataRecord`s lazily via the row view.
/// - `Rows`: exported `DataRecord`s (escalation / tests / legacy callers).
///
/// Records were historically converted on the rule worker (same thread that
/// allocated them): sample profiling showed that handing OutputRecords to
/// the sink consumers — allocating on the rule thread, freeing on the sink
/// thread — drove mimalloc into its abandoned-page reclaim path (~2x
/// throughput loss). The columnar form keeps that property: dropping a
/// column batch frees a handful of contiguous buffers instead of millions
/// of small per-row allocations.
#[derive(Clone)]
pub enum AlertBatch {
    /// Row-oriented payload (escalation forwards whatever form it received;
    /// also the test/legacy call form). Not constructed by the emit path.
    #[allow(dead_code)]
    Rows(Arc<Vec<Arc<DataRecord>>>),
    Columns(Arc<AlertColumnBatch>),
}

impl AlertBatch {
    pub fn len(&self) -> usize {
        match self {
            AlertBatch::Rows(rows) => rows.len(),
            AlertBatch::Columns(cols) => cols.len(),
        }
    }

    #[allow(dead_code)]
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
/// Resolved per-sink channel groups for a yield target: each entry is
/// `(sink_ptr, channels)` where `channels` are the sink's `parallel` writers.
type ResolvedChannels = Arc<Vec<(usize, Arc<Vec<mpsc::Sender<AlertBatch>>>)>>;

pub struct SinkFanout {
    /// `Arc<SinkRuntime>` pointer identity → its parallel writers.
    pub(crate) by_sink: HashMap<usize, Vec<mpsc::Sender<AlertBatch>>>,
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
        by_sink: HashMap<usize, Vec<mpsc::Sender<AlertBatch>>>,
        dispatcher: Arc<SinkDispatcher>,
    ) -> Self {
        let rr = by_sink
            .keys()
            .map(|&k| (k, std::sync::atomic::AtomicUsize::new(0)))
            .collect();
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
    pub(crate) fn from_resolved(cache: HashMap<String, ResolvedChannels>) -> Arc<Self> {
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
///
/// Shutdown: on cancel the consumer flushes the buffered backlog for at most
/// [`SINK_DRAIN_BUDGET`], then drops the rest and stops the sink — a large
/// alert backlog can't extend graceful shutdown indefinitely (the
/// wait_grace_down_with_timeout pattern).
pub async fn run_sink_consumer(
    mut rx: mpsc::Receiver<AlertBatch>,
    sink: Arc<SinkRuntime>,
    error_txs: Arc<Vec<mpsc::Sender<AlertBatch>>>,
    metrics: Option<Arc<RuntimeMetrics>>,
    cancel: CancellationToken,
) {
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                // Shutdown: flush what's buffered within a budget, then stop.
                let deadline = Instant::now() + SINK_DRAIN_BUDGET;
                while let Ok(batch) = rx.try_recv() {
                    if Instant::now() >= deadline {
                        break;
                    }
                    dispatch_batch(&sink, &error_txs, &metrics, batch).await;
                }
                let mut dropped_batches = 0usize;
                let mut dropped_records = 0u64;
                while let Ok(batch) = rx.try_recv() {
                    dropped_batches += 1;
                    dropped_records += batch.len() as u64;
                }
                if dropped_batches > 0 {
                    if let Some(metrics) = &metrics {
                        metrics.add_sink_drain_dropped_records(dropped_records);
                    }
                    log::warn!(
                        "sink {:?} shutdown drain budget exceeded, dropped {dropped_batches} buffered alert batches ({dropped_records} records)",
                        sink.name
                    );
                }
                break;
            }
            batch = rx.recv() => match batch {
                Some(batch) => {
                    if let Some(metrics) = &metrics {
                        metrics.set_alert_channel_depth(rx.len() as u64);
                    }
                    dispatch_batch(&sink, &error_txs, &metrics, batch).await;
                }
                // Channel closed (all producers dropped): stop the sink.
                None => break,
            },
        }
    }
    let _ = sink.stop().await;
}

/// Send one alert batch to a sink, escalating failures to the error sinks.
/// Shared by the normal and shutdown-drain paths. The DataRecord conversion
/// stays on the rule worker (see [`AlertBatch`] — cross-thread record drops
/// cost more than the conversion itself under mimalloc).
async fn dispatch_batch(
    sink: &Arc<SinkRuntime>,
    error_txs: &Arc<Vec<mpsc::Sender<AlertBatch>>>,
    metrics: &Option<Arc<RuntimeMetrics>>,
    batch: AlertBatch,
) {
    let dispatch_started = Instant::now();
    let send_result = match &batch {
        AlertBatch::Rows(rows) => sink.send_records(rows).await,
        AlertBatch::Columns(cols) => sink.send_column_batch(cols).await,
    };
    if let Err(e) = send_result {
        log::warn!("sink {:?} dispatch error: {e}", sink.name);
        if let Some(metrics) = metrics {
            metrics.inc_sink_dispatch_failed();
        }
        // Escalate to error sinks (best-effort, no error-of-error loop).
        for tx in error_txs.iter() {
            if tx.send(batch.clone()).await.is_err() {
                if let Some(metrics) = metrics {
                    metrics.inc_alert_escalate_failed();
                }
                break;
            }
        }
    }
    if let Some(metrics) = metrics {
        metrics.inc_alert_dispatch();
        metrics.observe_alert_dispatch(dispatch_started.elapsed());
    }
}
