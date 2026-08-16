use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use arrow::record_batch::RecordBatch;
use tokio::sync::mpsc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use wf_engine::window::{ParsedRoute, Router};

use crate::metrics::RuntimeMetrics;

use super::types::TaskGroup;

/// Token-bucket ingest rate limiter (events/sec), learned from warp-parse's
/// `DynamicRateLimiter`. Tokens refill at `rate`; `acquire(events)` consumes
/// them and sleeps when exhausted, so the engine never ingests faster than the
/// configured cap even when a client sends flat-out — bounding the
/// allocation-throughput-driven RSS peak.
pub(crate) struct IngestLimiter {
    rate_per_sec: f64,
    tokens: Mutex<f64>,
    last_refill: Mutex<Instant>,
}

impl IngestLimiter {
    pub fn new(rate_per_sec: usize) -> Arc<Self> {
        Arc::new(Self {
            rate_per_sec: rate_per_sec as f64,
            tokens: Mutex::new(rate_per_sec as f64),
            last_refill: Mutex::new(Instant::now()),
        })
    }

    /// Consume `events` tokens, sleeping as needed to keep the long-run rate ≤
    /// `rate_per_sec`. Guards are scoped to a block so no `MutexGuard` crosses
    /// the `.await` (std guards are not `Send`).
    pub async fn acquire(&self, events: usize) {
        let n = events.max(1) as f64;
        let wait = {
            let mut tokens = self.tokens.lock().unwrap();
            let mut last = self.last_refill.lock().unwrap();
            let now = Instant::now();
            let elapsed = now.duration_since(*last).as_secs_f64();
            *tokens = (*tokens + elapsed * self.rate_per_sec).min(self.rate_per_sec);
            *last = now;
            if *tokens >= n {
                *tokens -= n;
                0.0
            } else {
                let need = n - *tokens;
                need / self.rate_per_sec
            }
        };
        if wait > 0.0 {
            tokio::time::sleep(Duration::from_secs_f64(wait)).await;
            let mut tokens = self.tokens.lock().unwrap();
            let mut last = self.last_refill.lock().unwrap();
            *tokens = 0.0;
            *last = Instant::now();
        }
    }
}

/// Bounded buffer for the source → parse-worker channel.
pub(super) const PARSE_CHANNEL_CAPACITY: usize = 1024;
/// Bounded buffer for the parse-worker → commit-worker channel.
const COMMIT_CHANNEL_CAPACITY: usize = 1024;

/// Default byte budget for in-flight decoded batches across the
/// source → parse → commit chain (see [`spawn_parse_pool_with_preread`]).
#[cfg(test)]
pub(crate) const DEFAULT_PARSE_BUFFER_BYTES: usize = 256 * 1024 * 1024;
/// Floor for the configured byte budget — smaller values would starve the
/// pipeline's pipelining depth for even modest batch sizes.
const MIN_PARSE_BUFFER_BYTES: usize = 16 * 1024 * 1024;
/// Largest number of semaphore permits acquired in one call. Batching the
/// acquisition in chunks keeps a single oversized batch from deadlocking a
/// budget smaller than itself (each chunk is ≤ budget, so it always
/// completes once earlier batches commit and release their permits).
const ACQUIRE_CHUNK_BYTES: usize = 8 * 1024 * 1024;

/// Byte budget shared by every source task pushing into the parse pool.
///
/// Each in-flight decoded batch holds permits equal to its arrow memory size
/// from the moment the source pushes it until the commit worker finishes
/// committing it, so total pipeline residency (parse channel + parse workers +
/// commit channel) is bounded in **bytes** regardless of batch/frame size —
/// the item-count channel caps alone scale with frame size and let a flat-out
/// client park multiple GiB in the channels with big frames.
pub(crate) type PrereadBudget = Arc<Semaphore>;

/// Acquire `bytes` permits from the preread budget, in chunks so any batch
/// size can be admitted once budget frees up (no deadlock when a single batch
/// exceeds the total budget). Permits are returned as owned handles; dropping
/// them (or the item carrying them) releases the budget.
pub(crate) async fn acquire_preread(
    budget: &PrereadBudget,
    bytes: usize,
) -> Vec<OwnedSemaphorePermit> {
    let mut permits = Vec::new();
    let mut remaining = bytes.max(1);
    while remaining > 0 {
        let chunk = remaining.min(ACQUIRE_CHUNK_BYTES);
        match budget.clone().acquire_many_owned(chunk as u32).await {
            Ok(permit) => permits.push(permit),
            // Semaphore closed (engine shutting down): stop waiting; the
            // following channel send will fail and unwind the source anyway.
            Err(_) => break,
        }
        remaining -= chunk;
    }
    permits
}

/// Blocking flavour of [`acquire_preread`] for `spawn_blocking` replay paths.
pub(crate) fn acquire_preread_blocking(
    budget: &PrereadBudget,
    bytes: usize,
) -> Vec<OwnedSemaphorePermit> {
    let mut permits = Vec::new();
    let mut remaining = bytes.max(1);
    while remaining > 0 {
        let chunk = remaining.min(ACQUIRE_CHUNK_BYTES);
        match budget.clone().try_acquire_many_owned(chunk as u32) {
            Ok(permit) => permits.push(permit),
            Err(_) => {
                if budget.is_closed() {
                    break;
                }
                std::thread::sleep(Duration::from_millis(5));
                continue;
            }
        }
        remaining -= chunk;
    }
    permits
}

/// A decoded batch handed from a source task to the parse worker pool.
///
/// `seq` is a monotonically increasing sequence assigned by the source, so the
/// single commit worker can re-assemble batches in source order even though N
/// parse workers finish out of order. `window_seqs` carries the per-(source,
/// window) contiguous sequences allocated in the same serialized step (actor
/// mode only) — see [`Router::next_window_seqs`].
pub(crate) struct ParseItem {
    pub seq: u64,
    pub source_name: String,
    pub stream_name: String,
    pub batch: RecordBatch,
    /// Per-(source, window) contiguous seqs for the actor dispatch path.
    /// Allocated here — the last point where the source's frames are still
    /// strictly ordered — so parallel parse workers cannot permute them.
    pub window_seqs: Vec<(String, u64)>,
    /// Arrow memory size of `batch`, charged against the preread budget.
    /// Budget permits held while this batch is in flight; released when the
    /// commit worker finishes (or the item is dropped on shutdown).
    pub permits: Vec<OwnedSemaphorePermit>,
}

/// Build a projected, sequenced `ParseItem` for one decoded batch (R2/R3).
///
/// Receiver frame metrics, projection (the parse pool's `route_parse`/
/// `route_commit` do **not** project, so the batch must be conformant before it
/// is pushed), then an ordered `ParseItem` on the shared seq. Synchronous so it
/// is reusable both from async push ([`push_decoded_batch`]) and from the
/// arrow-IPC replay's `spawn_blocking` closure ([`Sender::blocking_send`]).
pub(crate) fn build_parse_item(
    parse_seq: &AtomicU64,
    source_name: &str,
    stream_name: &str,
    batch: RecordBatch,
    router: &Router,
    metrics: Option<&Arc<RuntimeMetrics>>,
    permits: Vec<OwnedSemaphorePermit>,
) -> ParseItem {
    if let Some(metrics) = metrics {
        metrics.add_receiver_frame(batch.num_rows());
        metrics.add_receiver_source_frame(source_name, batch.num_rows());
        let machine_id = crate::receiver::batch_machine_id(&batch)
            .unwrap_or_else(|| source_name.to_string());
        metrics.add_receiver_source_machine_rows(source_name, &machine_id, batch.num_rows());
    }
    let projected = crate::receiver::prepare_batch(stream_name, &batch, router);
    // Per-(source, window) seq allocation MUST happen at this serialized
    // point (source order still guaranteed): the window actors' reorder
    // cursors expect a gap-free sequence per window, and parallel parse
    // workers would otherwise assign seqs in completion order.
    let window_seqs = router.next_window_seqs(source_name, stream_name);
    ParseItem {
        seq: parse_seq.fetch_add(1, Ordering::Relaxed),
        source_name: source_name.to_string(),
        stream_name: stream_name.to_string(),
        batch: projected,
        window_seqs,
        permits,
    }
}

/// Project + push one decoded batch to the parse worker pool.
///
/// Used by streaming sources and file-replay sources alike, so every source
/// flows through the same parse → ordered-commit → broadcast chain. Returns
/// `false` when the parse pool has shut down (channel closed).
///
/// When `limiter` is `Some`, the batch is token-bucketed first (engine-side
/// ingest rate cap), so a flat-out client cannot drive the engine's
/// allocation throughput (and RSS peak) beyond the configured rate.
pub(crate) async fn push_decoded_batch(
    parse_tx: &mpsc::Sender<ParseItem>,
    preread: &PrereadBudget,
    parse_seq: &AtomicU64,
    source_name: &str,
    stream_name: &str,
    batch: RecordBatch,
    router: &Router,
    metrics: Option<&Arc<RuntimeMetrics>>,
    limiter: Option<&IngestLimiter>,
) -> bool {
    if let Some(limiter) = limiter {
        limiter.acquire(batch.num_rows()).await;
    }
    // Charge the decoded batch against the byte budget *before* entering the
    // pipeline; permits travel with the item and are released after commit.
    let mem_bytes = batch.get_array_memory_size();
    let permits = acquire_preread(preread, mem_bytes).await;
    let item = build_parse_item(
        parse_seq,
        source_name,
        stream_name,
        batch,
        router,
        metrics,
        permits,
    );
    parse_tx.send(item).await.is_ok()
}

/// A parsed batch handed from a parse worker to the (ordered) commit worker.
struct ParsedItem {
    seq: u64,
    source_name: String,
    batch: RecordBatch,
    parsed: ParsedRoute,
    /// Budget permits held on behalf of this batch; released after commit.
    permits: Vec<OwnedSemaphorePermit>,
}

/// Spawn the parse worker pool into `group`: N parallel parsers plus either
/// (actor mode) direct dispatch to the per-window actors, or (sync mode) one
/// ordered commit worker. Returns the sender the source tasks push decoded
/// batches into, plus the shared preread byte budget sources must charge
/// batches against (see [`push_decoded_batch`]).
///
/// **Actor mode** (window mailboxes registered on the router before this
/// call — production boot): parse workers run [`Router::route_parse`] in
/// parallel and hand each parsed window batch directly to the window's actor
/// mailbox ([`Router::dispatch_parsed`]). Ordering is preserved without a
/// global serialization point: per-(source, window) seqs are allocated at
/// the source-side frame builder and the window actor re-orders arrivals.
///
/// **Sync mode** (no mailboxes — tests, embedded): parse workers forward to
/// a single commit worker that runs [`Router::route_commit`] in per-source
/// `seq` order so watermark advancement and rule broadcast stay in source
/// order.
/// Legacy entry point with the default budget (tests; the runtime passes the
/// configured budget via [`spawn_parse_pool_with_preread`]).
#[cfg(test)]
pub(crate) fn spawn_parse_pool(
    router: &Arc<Router>,
    metrics: Option<Arc<RuntimeMetrics>>,
    worker_count: usize,
    group: &mut TaskGroup,
) -> (mpsc::Sender<ParseItem>, PrereadBudget) {
    spawn_parse_pool_with_preread(
        router,
        metrics,
        worker_count,
        group,
        DEFAULT_PARSE_BUFFER_BYTES,
    )
}

/// [`spawn_parse_pool`] with an explicit preread byte budget (tests).
pub(crate) fn spawn_parse_pool_with_preread(
    router: &Arc<Router>,
    metrics: Option<Arc<RuntimeMetrics>>,
    worker_count: usize,
    group: &mut TaskGroup,
    preread_bytes: usize,
) -> (mpsc::Sender<ParseItem>, PrereadBudget) {
    let worker_count = worker_count.max(1);
    let preread_bytes = preread_bytes.max(MIN_PARSE_BUFFER_BYTES);
    let (parse_tx, parse_rx) = mpsc::channel::<ParseItem>(PARSE_CHANNEL_CAPACITY);
    let parse_rx = Arc::new(tokio::sync::Mutex::new(parse_rx));
    let preread: PrereadBudget = Arc::new(Semaphore::new(preread_bytes));

    let direct = router.has_mailboxes();

    // Sync mode only: ordered commit worker (single).
    let commit_tx = if direct {
        None
    } else {
        let (commit_tx, commit_rx) = mpsc::channel::<ParsedItem>(COMMIT_CHANNEL_CAPACITY);
        let commit_router = Arc::clone(router);
        let commit_metrics = metrics.clone();
        group.push(tokio::spawn(async move {
            run_commit_worker(commit_rx, commit_router, commit_metrics).await;
            Ok(())
        }));
        Some(commit_tx)
    };

    // Parallel parse workers.
    for _ in 0..worker_count {
        let parse_rx = Arc::clone(&parse_rx);
        let router = Arc::clone(router);
        let metrics = metrics.clone();
        let commit_tx = commit_tx.clone();
        group.push(tokio::spawn(async move {
            match commit_tx {
                Some(commit_tx) => {
                    run_parse_worker(parse_rx, commit_tx, router).await;
                }
                None => {
                    run_parse_worker_direct(parse_rx, router, metrics).await;
                }
            }
            Ok(())
        }));
    }
    // Drop our commit_tx clone so the commit channel closes once all parse
    // workers exit (letting the commit worker drain + stop).
    drop(commit_tx);

    (parse_tx, preread)
}

/// Pull decoded batches from the shared parse channel, parse them in parallel,
/// and forward the parsed result to the commit worker.
async fn run_parse_worker(
    rx: Arc<tokio::sync::Mutex<mpsc::Receiver<ParseItem>>>,
    commit_tx: mpsc::Sender<ParsedItem>,
    router: Arc<Router>,
) {
    loop {
        let item = {
            let mut guard = rx.lock().await;
            guard.recv().await
        };
        let Some(item) = item else {
            // Parse channel closed: all sources finished.
            break;
        };
        let parsed = router.route_parse(&item.stream_name, &item.batch);
        if commit_tx
            .send(ParsedItem {
                seq: item.seq,
                source_name: item.source_name,
                batch: item.batch,
                parsed,
                permits: item.permits,
            })
            .await
            .is_err()
        {
            // Commit worker gone (shutdown). Dropping the item releases its
            // preread permits.
            break;
        }
    }
}

/// Actor mode: parse in parallel, then hand each window's batch directly to
/// its window actor mailbox.
async fn run_parse_worker_direct(
    rx: Arc<tokio::sync::Mutex<mpsc::Receiver<ParseItem>>>,
    router: Arc<Router>,
    metrics: Option<Arc<RuntimeMetrics>>,
) {
    loop {
        let item = {
            let mut guard = rx.lock().await;
            guard.recv().await
        };
        let Some(item) = item else {
            break;
        };
        let ParseItem {
            seq,
            source_name,
            stream_name,
            batch,
            window_seqs,
            permits,
        } = item;
        if let Some(metrics) = &metrics {
            metrics.inc_router_route_call();
        }
        let parsed = router.route_parse(&stream_name, &batch);
        if let Some(metrics) = &metrics {
            metrics.add_router_skipped(parsed.skipped_non_local);
        }
        router
            .dispatch_parsed(Arc::from(source_name.as_str()), seq, window_seqs, batch, parsed)
            .await;
        // Every subscribed window's channel has received the batch: the
        // in-flight bytes are now accounted by the window byte budgets.
        // Release the preread permits (dropping them returns the budget).
        drop(permits);
    }
}

/// Re-assemble parsed batches in per-source `seq` order and commit them.
async fn run_commit_worker(
    mut rx: mpsc::Receiver<ParsedItem>,
    router: Arc<Router>,
    metrics: Option<Arc<RuntimeMetrics>>,
) {
    let mut next_seq: std::collections::HashMap<String, u64> =
        std::collections::HashMap::new();
    let mut pending: BTreeMap<(String, u64), ParsedItem> = BTreeMap::new();

    while let Some(item) = rx.recv().await {
        let key = (item.source_name.clone(), item.seq);
        pending.insert(key, item);
        drain_pending(&mut pending, &mut next_seq, &router, &metrics).await;
    }

    // Commit channel closed (all parse workers done): flush what remains, in
    // order. A missing `next_seq` would indicate a dropped batch (parse worker
    // aborted) — surface it rather than silently skipping.
    if !pending.is_empty() {
        for (source, next) in next_seq.iter_mut() {
            let first = pending
                .range((source.clone(), 0)..)
                .next()
                .map(|((_, seq), _)| *seq);
            if let Some(first) = first
                && first != *next
            {
                wf_warn!(
                    pipe,
                    source = %source,
                    next_seq = *next,
                    first_pending = first,
                    "parse commit sequence gap detected"
                );
            }
        }
        drain_pending(&mut pending, &mut next_seq, &router, &metrics).await;
    }
}

/// Commit every consecutively-sequenced pending item, per source.
async fn drain_pending(
    pending: &mut BTreeMap<(String, u64), ParsedItem>,
    next_seq: &mut std::collections::HashMap<String, u64>,
    router: &Arc<Router>,
    metrics: &Option<Arc<RuntimeMetrics>>,
) {
    loop {
        // Find any (source, seq) matching its cursor. A source not yet in the
        // map starts at seq 0 (per-source counters begin at 0).
        let hit = pending
            .keys()
            .find(|(source, seq)| {
                next_seq.get(source.as_str()).copied().unwrap_or(0) == *seq
            })
            .cloned();
        let Some((source, seq)) = hit else { break };
        let Some(item) = pending.remove(&(source.clone(), seq)) else { break };
        commit(router, metrics, item).await;
        next_seq.insert(source, seq.wrapping_add(1));
    }
}

async fn commit(router: &Router, metrics: &Option<Arc<RuntimeMetrics>>, item: ParsedItem) {
    let ParsedItem {
        seq: _,
        source_name,
        batch,
        parsed,
        permits,
    } = item;
    if let Some(metrics) = metrics {
        metrics.inc_router_route_call();
    }
    let result = router.route_commit(batch, parsed).await;
    // Commit finished (or failed): give the batch's byte budget back so
    // upstream sources can push more. Dropping the permits releases them.
    drop(permits);
    match result {
        Ok(report) => {
            if let Some(metrics) = metrics {
                metrics.add_route_report(&report);
            }
        }
        Err(e) => {
            // Preserve the per-source route_error telemetry that route_batch used
            // to report on the inline path.
            if let Some(metrics) = metrics {
                metrics.inc_route_error(&source_name);
            }
            wf_warn!(pipe, error = %e, "parse commit failed");
        }
    }
}
