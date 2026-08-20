//! Window actor: the single writer per window (subscription model).
//!
//! Each window owns one actor task that is the **only** writer of its data
//! plane; readers (`events_since`/`snapshot`, join-index setup, metrics
//! sampling) take the window's log **read lock** just long enough to clone
//! `Arc` handles out — and in the production push wiring no reader touches
//! the log on the hot path at all (rules receive broadcasts through their
//! channels). Upstream
//! producers (parse workers) hand batches to the actor over a bounded channel
//! with an explicit byte budget, so backpressure is structural instead of
//! relying on a write lock's implicit serialization (the LF regression root
//! cause: removing the lock removed its hidden flow shaping and let in-flight
//! batches balloon to ~8GB).
//!
//! Ordering: `seq` is a **per-(source, window)** contiguous sequence
//! allocated at the source-side frame builder (see
//! [`Router::next_window_seqs`](super::router::Router::next_window_seqs)) —
//! the last point where a source's frames are strictly ordered. Parallel
//! parse workers may then dispatch out of order; the actor re-orders per
//! source with a pending map, and a gap never blocks dequeue (missing
//! batches park in `pending`, the channel keeps draining), so upstream
//! sends always make progress — pending depth is transitively bounded by
//! the parse preread budget. A window must never receive a *global*
//! per-source frame counter as `seq`: a window only sees the subset of
//! frames carrying its stream, so that counter has permanent holes, every
//! frame after the first hole parks forever, and the parked permits pin
//! the window byte budget until the pipeline deadlocks.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use arrow::record_batch::RecordBatch;
use tokio::sync::mpsc;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

use super::EvictionGate;
use super::buffer::{AppendOutcome, Window};
use super::fanout::RuleFanout;
use crate::match_engine::Event;

/// Bounded depth of a window channel (messages). The byte budget is the
/// primary bound; depth just caps message-count overhead for tiny batches.
pub const WINDOW_CHANNEL_DEPTH: usize = 16;

/// A message for a window actor.
pub enum WindowMsg {
    /// One source batch dispatched directly by a parse worker after
    /// `route_parse`. `seq` is the per-(source, window) contiguous sequence;
    /// the actor re-orders per source before appending.
    Append {
        source: Arc<str>,
        seq: u64,
        batch: RecordBatch,
        /// Pre-parsed events. `Some` only when a rule subscribes to this
        /// window (otherwise materialization is skipped entirely — the
        /// dominant parse-side cost).
        events: Option<Arc<Vec<Arc<Event>>>>,
        /// Bytes charged to the window for this message
        /// (Arrow content + parsed-event footprint).
        byte_size: usize,
        /// Window byte-budget permits held on behalf of this message;
        /// released when the actor finishes with it (append or late drop).
        permits: Vec<OwnedSemaphorePermit>,
        /// Precomputed columnar shard partition of `batch` (per-shard row
        /// subsets), produced in the parallel parse stage so the single-writer
        /// actor does not repartition the whole batch on every commit. `None`
        /// for non-sharded windows / row-based pushes (defensive repartition
        /// in `broadcast_inner`).
        shard_rows: Option<Arc<[Vec<u32>]>>,
    },
}

/// Per-window mailbox: the bounded actor channel plus its byte budget.
#[derive(Clone)]
pub struct WindowMailbox {
    pub tx: mpsc::Sender<WindowMsg>,
    pub budget: Arc<Semaphore>,
    /// Total budget capacity in bytes (the semaphore's initial permit count).
    /// Kept alongside the semaphore so acquisition can clamp oversized
    /// requests (see [`acquire_window_budget`]).
    pub budget_bytes: usize,
}

/// Per-window append outcome reporter: `(window_name, rows, late)`. Wired by
/// the runtime to the existing route-report metrics so EPS accounting
/// (`window_append_total`) is unchanged by the actor path.
pub type WindowAppendReport = Arc<dyn Fn(&str, usize, bool) + Send + Sync>;

/// Acquire `bytes` permits from a window budget. Requests larger than the
/// budget capacity are clamped to the full capacity: the resulting message
/// exclusively owns the window budget until the actor consumes it, which
/// keeps acquisition terminating (without the clamp, a dispatcher holding
/// part of the budget while waiting for the rest would wait on the actor,
/// and the actor on the message that was never sent — a deterministic
/// deadlock for any batch bigger than the budget). The clamped amount is
/// acquired in a *single* semaphore call: chunked acquisition let several
/// concurrent oversized dispatchers each hold a fraction of the budget while
/// each waited for the rest — the dining-philosophers deadlock again.
/// Permits are returned as owned handles; dropping them releases the budget.
pub async fn acquire_window_budget(
    budget: &Arc<Semaphore>,
    capacity: usize,
    bytes: usize,
) -> Vec<OwnedSemaphorePermit> {
    let target = bytes.max(1).min(capacity.max(1)) as u32;
    match budget.clone().acquire_many_owned(target).await {
        Ok(permit) => vec![permit],
        // Semaphore closed (shutdown): stop waiting; the channel send
        // will fail and unwind the producer anyway.
        Err(_) => Vec::new(),
    }
}

/// Run the single-writer actor for one window until cancelled (or every
/// sender drops — embedded/test mode).
///
/// The actor is the only caller of append-path methods on `win`; all other
/// access is concurrent reading under the log read lock (see [`Window`]'s
/// concurrency contract). On cancellation it commits
/// whatever is already queued (bounded, non-blocking drain) so a graceful
/// shutdown does not lose the queued tail, then stops.
pub async fn run_window_actor(
    name: Arc<str>,
    win: Arc<Window>,
    gate: Arc<EvictionGate>,
    fanout: Arc<RuleFanout>,
    notify: Arc<Notify>,
    mut rx: mpsc::Receiver<WindowMsg>,
    cancel: CancellationToken,
    report: Option<WindowAppendReport>,
) {
    // Per-source expected-next-seq cursors (reorder state).
    let mut next_seq: HashMap<Arc<str>, u64> = HashMap::new();
    // Out-of-order arrivals waiting for their source cursor to catch up.
    let mut pending: BTreeMap<(Arc<str>, u64), WindowMsg> = BTreeMap::new();

    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                // Shutdown: commit what is already queued (non-blocking) so
                // the tail is not dropped, then stop. Messages still in
                // producer hands at this point are torn down with them (same
                // semantics as the old commit worker on a cancelled pool).
                while let Ok(msg) = rx.try_recv() {
                    commit_append(&name, &win, &gate, &fanout, &notify, &report, msg).await;
                }
                break;
            }
            msg = rx.recv() => {
                match msg {
                    None => {
                        // All senders dropped (embedded/test mode): exit loop
                        // and flush pending below.
                        log::warn!("window actor {:?}: mailbox closed (all senders dropped), exiting", name);
                        break;
                    }
                    Some(mut m) => {
                        let (source, seq) = match &m {
                            WindowMsg::Append { source, seq, .. } => (Arc::clone(source), *seq),
                        };
                        let cursor = next_seq.entry(Arc::clone(&source)).or_insert(0);
                        if seq == *cursor {
                            commit_append(&name, &win, &gate, &fanout, &notify, &report, m).await;
                            *cursor += 1;
                            // Drain this source's now-consecutive pending tail.
                            // The gap never blocked the channel — these
                            // arrived earlier and parked in `pending`.
                            while let Some(msg) =
                                pending.remove(&(Arc::clone(&source), *cursor))
                            {
                                commit_append(&name, &win, &gate, &fanout, &notify, &report, msg)
                                    .await;
                                *cursor += 1;
                            }
                        } else {
                            if seq < *cursor {
                                // Stale (already-processed) seq: a duplicate
                                // delivery, reachable only via the hot-add
                                // fallback seq in `dispatch_parsed`. Parking
                                // it would strand the message in `pending`
                                // forever — the cursor has moved past it, so
                                // no drain path can ever flush it (a pure
                                // memory leak). Drop it; dropping also
                                // releases its budget permits.
                                log::warn!(
                                    "window actor {:?}: dropping stale seq {} <= cursor {} (source {:?})",
                                    name, seq, cursor, source
                                );
                                drop(m);
                                continue;
                            }
                            // Future seq for this source: park it (gap or
                            // cross-source interleaving); keep dequeuing.
                            // Release the parked message's budget permits
                            // *before* parking: parse workers dispatch
                            // concurrently, so arrival order is not seq
                            // order, and the dispatcher holding the missing
                            // seq may itself be blocked in
                            // `acquire_window_budget` waiting for this very
                            // budget — parking with the permits held is a
                            // deterministic deadlock whenever a batch (or a
                            // batch sum in flight) exhausts the budget.
                            // Parked bytes stay bounded by the parse-side
                            // in-flight budget instead.
                            // `WindowMsg` has a single variant, so this
                            // destructures unconditionally.
                            let WindowMsg::Append { permits, .. } = &mut m;
                            permits.clear();
                            pending.insert((Arc::clone(&source), seq), m);
                        }
                    }
                }
            }
        }
    }

    // Channel closed with pending left (test/embedded mode): flush in order
    // per source, surfacing gaps instead of silently skipping (mirrors the
    // old commit worker's close-path behaviour).
    if !pending.is_empty() {
        let sources: Vec<Arc<str>> = pending.keys().map(|(s, _)| Arc::clone(s)).collect();
        for source in sources {
            let cursor = next_seq.entry(Arc::clone(&source)).or_insert(0);
            if let Some(first) = pending.range((Arc::clone(&source), 0)..).next() {
                let first_seq = first.0.1;
                if first_seq != *cursor {
                    log::warn!(
                        "window actor {:?}: source {:?} sequence gap at shutdown: next={}, first_pending={}",
                        name,
                        source,
                        cursor,
                        first_seq
                    );
                }
            }
            while let Some(msg) = pending.remove(&(Arc::clone(&source), *cursor)) {
                commit_append(&name, &win, &gate, &fanout, &notify, &report, msg).await;
                *cursor += 1;
            }
        }
        // Anything still pending sits on a real gap (a lost batch); dropping
        // the messages releases their budget permits.
    }
}

/// Commit one message's batch via the shared primitive (append + broadcast +
/// notify) and report the outcome. Consumes the message — dropping it
/// releases the window byte-budget permits whether the batch was appended or
/// dropped late. Entry-point specifics (report callback, error logging) stay
/// here; the ordering-sensitive core lives in
/// [`commit_appended_batch`](super::commit::commit_appended_batch).
async fn commit_append(
    name: &Arc<str>,
    win: &Arc<Window>,
    gate: &Arc<EvictionGate>,
    fanout: &Arc<RuleFanout>,
    notify: &Arc<Notify>,
    report: &Option<WindowAppendReport>,
    msg: WindowMsg,
) {
    // Memory backpressure (global cap): if the aggregate window memory across
    // all windows is still over `max_total_bytes` — and the evictor therefore
    // cannot reclaim space without dropping unacked pull data — stop this
    // conveyor (park) until the evictor's next sweep frees room. The actor
    // holds no locks while parked (only awaits the gate's `Notify`), so the
    // periodic evictor task keeps running and will wake it. This is the
    // "传送带得停" guarantee: under global memory pressure we back off instead
    // of letting the evictor lose unread pull batches.
    loop {
        if gate.current_bytes.load(Ordering::Relaxed) <= gate.max_total_bytes {
            break;
        }
        gate.freed.notified().await;
    }

    let WindowMsg::Append {
        source: _,
        seq: _,
        batch,
        events,
        byte_size,
        permits: _,
        shard_rows,
    } = msg;
    let rows = batch.num_rows();
    let result = super::commit::commit_appended_batch(
        win,
        fanout,
        Some(notify),
        name,
        batch,
        events,
        byte_size,
        shard_rows,
    )
    .await;
    match result {
        Ok((AppendOutcome::Appended, _)) => {
            if let Some(report) = report {
                report(name, rows, false);
            }
        }
        Ok((AppendOutcome::DroppedLate, _)) => {
            if let Some(report) = report {
                report(name, rows, true);
            }
        }
        Err(e) => {
            log::warn!("window actor {:?}: append failed: {}", name, e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::window::WindowParams;
    use arrow::array::{Int64Array, TimestampNanosecondArray};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
    use std::time::Duration;
    use wf_config::{DistMode, EvictPolicy, LatePolicy, WindowConfig};

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
            Field::new("value", DataType::Int64, false),
        ]))
    }

    fn make_batch(schema: &SchemaRef, time: i64, value: i64) -> RecordBatch {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![time])),
                Arc::new(Int64Array::from(vec![value])),
            ],
        )
        .unwrap()
    }

    fn test_config() -> WindowConfig {
        WindowConfig {
            name: "default".into(),
            mode: DistMode::Local,
            max_window_bytes: usize::MAX.into(),
            over_cap: Duration::from_secs(3600).into(),
            evict_policy: EvictPolicy::TimeFirst,
            watermark: Duration::from_secs(5).into(),
            allowed_lateness: Duration::from_secs(0).into(),
            late_policy: LatePolicy::Drop,
            table: None,
        }
    }

    fn make_window(name: &str) -> Arc<Window> {
        Arc::new(Window::new(
            WindowParams {
                name: name.into(),
                schema: test_schema(),
                time_col_index: Some(0),
                over: Duration::from_secs(3600),
                materialize_fields: None,
                defer_materialization: false,
            },
            test_config(),
        ))
    }

    fn msg(source: &str, seq: u64, time: i64, value: i64) -> WindowMsg {
        WindowMsg::Append {
            source: Arc::from(source),
            seq,
            batch: make_batch(&test_schema(), time, value),
            events: None,
            byte_size: 64,
            permits: Vec::new(),
            shard_rows: None,
        }
    }

    /// Read the appended values back out of the window, in append order.
    fn appended_values(win: &Arc<Window>) -> Vec<i64> {
        win.snapshot()
            .iter()
            .flat_map(|batch| {
                batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .iter()
                    .map(|v| v.unwrap())
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    async fn spawn_actor(win: Arc<Window>) -> (mpsc::Sender<WindowMsg>, CancellationToken) {
        let (tx, rx) = mpsc::channel::<WindowMsg>(WINDOW_CHANNEL_DEPTH);
        let cancel = CancellationToken::new();
        let name: Arc<str> = Arc::from("w");
        let fanout = RuleFanout::new();
        let notify = Arc::new(Notify::new());
        let cancel2 = cancel.clone();
        tokio::spawn(async move {
            run_window_actor(
                name,
                win,
                Arc::new(EvictionGate::new(usize::MAX)),
                fanout,
                notify,
                rx,
                cancel2,
                None,
            )
            .await;
        });
        (tx, cancel)
    }

    // -- 1. out-of-order arrivals are appended in source order ---------------

    #[tokio::test]
    async fn reorders_out_of_order_batches_per_source() {
        let win = make_window("w");
        let (tx, _cancel) = spawn_actor(Arc::clone(&win)).await;

        // Deliver 2, 0, 1: nothing may append out of order.
        tx.send(msg("s", 2, 30_000_000_000, 2)).await.unwrap();
        tx.send(msg("s", 0, 10_000_000_000, 0)).await.unwrap();
        // After seq 0 lands, 1 and 2 flush consecutively.
        tx.send(msg("s", 1, 20_000_000_000, 1)).await.unwrap();

        // Give the actor a moment to process (single-task test runtime).
        for _ in 0..50 {
            if win.total_rows() == 3 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(appended_values(&win), vec![0, 1, 2]);
    }

    // -- 2. a gap never blocks the channel ------------------------------------

    #[tokio::test]
    async fn gap_does_not_block_channel_or_lose_later_batches() {
        let win = make_window("w");
        let (tx, _cancel) = spawn_actor(Arc::clone(&win)).await;

        // seq 1..=5 arrive while 0 is missing: all park in pending, channel
        // keeps accepting (sends complete — this is the no-deadlock property).
        for seq in 1..=5u64 {
            tx.send(msg("s", seq, (seq as i64) * 10_000_000_000, seq as i64))
                .await
                .unwrap();
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(win.total_rows(), 0, "nothing appends while the gap is open");

        // Close the gap: everything flushes in order.
        tx.send(msg("s", 0, 0, 0)).await.unwrap();
        for _ in 0..50 {
            if win.total_rows() == 6 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(appended_values(&win), vec![0, 1, 2, 3, 4, 5]);
    }

    // -- 2b. pending tail latency: a large out-of-order window -----------------

    /// A large reverse-ordered burst reproduces the q5 pull-freeze tail: every
    /// seq parks in `pending` until seq 0 closes the gap, so nothing appends
    /// while the cursor is pinned (the tail-latency signature) and everything
    /// drains in order once it does. Guards completeness under a 100-batch
    /// reorder window (parse parallelism ~10 in production, but bursts can
    /// exceed it).
    #[tokio::test]
    async fn large_out_of_order_window_drains_completely() {
        let win = make_window("w");
        let (tx, _cancel) = spawn_actor(Arc::clone(&win)).await;
        const N: u64 = 100;

        // Reverse order (N..=1) parks everything; nothing may append yet.
        for seq in (1..=N).rev() {
            tx.send(msg("s", seq, seq as i64 * 10_000_000_000, seq as i64))
                .await
                .unwrap();
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            win.total_rows(),
            0,
            "cursor pinned by the missing seq 0 → tail latency (nothing appends)"
        );

        // Closing the gap flushes all N+1 batches in order.
        tx.send(msg("s", 0, 0, 0)).await.unwrap();
        for _ in 0..200 {
            if win.total_rows() == (N + 1) as usize {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(appended_values(&win), (0..=N as i64).collect::<Vec<i64>>());
    }

    // -- 3. independent per-source cursors -------------------------------------

    /// Stale (already-processed) seq must be dropped, not parked: parking a
    /// duplicate would strand it in `pending` forever (the cursor has moved
    /// past it, no drain path can flush it) — a pure leak. The duplicate must
    /// also not append a second time.
    #[tokio::test]
    async fn stale_seq_is_dropped_not_parked() {
        let win = make_window("w");
        let (tx, _cancel) = spawn_actor(Arc::clone(&win)).await;

        tx.send(msg("s", 0, 10_000_000_000, 0)).await.unwrap();
        for _ in 0..50 {
            if win.total_rows() == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(win.total_rows(), 1);

        // Duplicate seq 0 arrives after the cursor advanced to 1.
        tx.send(msg("s", 0, 10_000_000_000, 99)).await.unwrap();
        // A later in-order batch must still flow through (the stale entry
        // must not clog the reorder path).
        tx.send(msg("s", 1, 20_000_000_000, 1)).await.unwrap();

        for _ in 0..50 {
            if win.total_rows() == 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            appended_values(&win),
            vec![0, 1],
            "duplicate dropped, order kept"
        );
    }

    #[tokio::test]
    async fn two_sources_reorder_independently() {
        let win = make_window("w");
        let (tx, _cancel) = spawn_actor(Arc::clone(&win)).await;

        // Source B's gap must not hold back source A's in-order batches.
        // All batches share one event time so the watermark never makes an
        // out-of-order (but per-source in-order) delivery late.
        tx.send(msg("a", 0, 10_000_000_000, 100)).await.unwrap();
        tx.send(msg("b", 1, 10_000_000_000, 21)).await.unwrap(); // b parks
        tx.send(msg("a", 1, 10_000_000_000, 101)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            appended_values(&win),
            vec![100, 101],
            "source a appends despite source b's gap"
        );

        tx.send(msg("b", 0, 10_000_000_000, 20)).await.unwrap();
        for _ in 0..50 {
            if win.total_rows() == 4 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(appended_values(&win), vec![100, 101, 20, 21]);
    }

    // -- 4. budget permits are released after append ---------------------------

    /// Regression (q1p stall, 2026-08-16): a dispatch batch larger than the
    /// whole window budget used to deadlock — the dispatcher held part of
    /// the budget waiting for the rest, while the actor waited for a message
    /// that was never sent. The capacity clamp makes acquisition terminate:
    /// an oversized request is charged at most the full budget.
    #[tokio::test]
    async fn oversized_batch_acquisition_clamps_to_capacity() {
        let capacity = 64usize;
        let budget = Arc::new(Semaphore::new(capacity));

        // Request far larger than the budget: acquisition must complete on
        // its own (no actor consumption needed) and hold exactly the full
        // capacity, leaving zero permits for competing dispatchers until
        // the message is consumed and the permits dropped.
        let permits = tokio::time::timeout(
            Duration::from_millis(500),
            acquire_window_budget(&budget, capacity, 10 * capacity),
        )
        .await
        .expect("oversized acquisition must not deadlock");
        let held: usize = permits.iter().map(|p| p.num_permits()).sum();
        assert_eq!(held, capacity, "charge clamps to the full budget");
        assert_eq!(budget.available_permits(), 0);
        drop(permits);
        assert_eq!(budget.available_permits(), capacity);
    }

    /// Concurrent oversized dispatchers must not interleave partial
    /// acquisitions of the budget (the chunked variant let two dispatchers
    /// each hold a fraction while each waited for the rest — deadlock).
    /// Each request is served atomically: whole-budget or nothing; a
    /// burst of acquirers cycling acquire→consume→release must always
    /// make progress.
    #[tokio::test]
    async fn concurrent_oversized_acquisitions_do_not_interleave() {
        let capacity = 64usize;
        let budget = Arc::new(Semaphore::new(capacity));
        let mut handles = Vec::new();
        for _ in 0..4 {
            let b = Arc::clone(&budget);
            handles.push(tokio::spawn(async move {
                for _ in 0..50 {
                    let permits = tokio::time::timeout(
                        Duration::from_millis(2_000),
                        acquire_window_budget(&b, capacity, 10 * capacity),
                    )
                    .await
                    .expect("oversized acquisition must not deadlock");
                    for p in permits.iter() {
                        assert_eq!(p.num_permits(), capacity);
                    }
                    drop(permits);
                    tokio::task::yield_now().await;
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
    }

    /// Regression (q1p stall, 2026-08-16): a message that arrives out of seq
    /// order used to park in the reorder buffer holding its full-budget
    /// permits, while the dispatcher holding the missing seq was blocked in
    /// `acquire_window_budget` waiting for that budget — deadlock. Parking
    /// must release the permits so the gap-filling dispatcher can proceed.
    #[tokio::test]
    async fn parked_out_of_order_message_releases_budget() {
        let capacity = 4usize;
        let budget = Arc::new(Semaphore::new(capacity));
        let win = make_window("w");
        let (tx, _cancel) = spawn_actor(Arc::clone(&win)).await;

        // seq=1 arrives first and exhausts the whole budget; it must park
        // and give the permits back without waiting for seq=0.
        let permits = budget
            .clone()
            .acquire_many_owned(capacity as u32)
            .await
            .unwrap();
        tx.send(WindowMsg::Append {
            source: Arc::from("s"),
            seq: 1,
            batch: make_batch(&test_schema(), 20_000_000_000, 1),
            events: None,
            byte_size: capacity,
            permits: vec![permits],
            shard_rows: None,
        })
        .await
        .unwrap();

        // The dispatcher for the missing seq=0 can now acquire the budget
        // (this is exactly where the deadlock used to bite).
        let gap_permits = tokio::time::timeout(
            Duration::from_millis(500),
            budget.clone().acquire_many_owned(capacity as u32),
        )
        .await
        .expect("parked message must not hold the budget")
        .unwrap();
        tx.send(WindowMsg::Append {
            source: Arc::from("s"),
            seq: 0,
            batch: make_batch(&test_schema(), 10_000_000_000, 0),
            events: None,
            byte_size: capacity,
            permits: vec![gap_permits],
            shard_rows: None,
        })
        .await
        .unwrap();

        // Both rows land, in seq order (0 before 1).
        for _ in 0..100 {
            if win.total_rows() == 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(appended_values(&win), vec![0, 1]);
    }

    #[tokio::test]
    async fn budget_permits_release_after_append() {
        let budget = Arc::new(Semaphore::new(128));
        let win = make_window("w");
        let (tx, _cancel) = spawn_actor(Arc::clone(&win)).await;

        // Acquire, send, and verify the budget drains then refills as the
        // actor finishes each message.
        for seq in 0..4u64 {
            let permits = acquire_window_budget(&budget, 128, 64).await;
            assert_eq!(permits.len(), 1);
            tx.send(WindowMsg::Append {
                source: Arc::from("s"),
                seq,
                batch: make_batch(&test_schema(), (seq as i64) * 10_000_000_000, seq as i64),
                events: None,
                byte_size: 64,
                permits,
                shard_rows: None,
            })
            .await
            .unwrap();
        }
        for _ in 0..100 {
            if budget.available_permits() == 128 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            budget.available_permits(),
            128,
            "all permits return once every message is appended"
        );
        assert_eq!(win.total_rows(), 4);
    }

    // -- 5. appended batches broadcast to rule subscribers ---------------------

    // -- 6. batched inbox must not wait to fill -------------------------------

    /// Latency contract for the mailbox consumer, whatever receive strategy
    /// it uses (`recv` today, `recv_many` batching if ever revisited): a
    /// partial inbox — fewer messages than any batch limit — must still be
    /// processed immediately, never accumulated while waiting for more.
    /// Guards against a future "batch up then process" regression.
    #[tokio::test]
    async fn partial_inbox_processes_without_waiting_for_fill() {
        let win = make_window("w");
        let (tx, _cancel) = spawn_actor(Arc::clone(&win)).await;

        // Fewer messages than the inbox capacity (16): all must land promptly.
        for seq in 0..3u64 {
            tx.send(msg("s", seq, (seq as i64) * 10_000_000_000, seq as i64))
                .await
                .unwrap();
        }

        let deadline = tokio::time::timeout(Duration::from_millis(500), async {
            while win.total_rows() < 3 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        assert!(
            deadline.is_ok(),
            "partial inbox (3 < limit) must process without waiting for more messages"
        );
        assert_eq!(appended_values(&win), vec![0, 1, 2]);
    }

    #[tokio::test]
    async fn appended_batch_broadcasts_to_subscribers() {
        use crate::window::RulePush;

        let win = make_window("w");
        let (tx, rx) = mpsc::channel::<WindowMsg>(WINDOW_CHANNEL_DEPTH);
        let fanout = RuleFanout::new();
        let (rule_tx, mut rule_rx) = mpsc::channel::<RulePush>(8);
        fanout.register("w", rule_tx);
        let notify = Arc::new(Notify::new());
        let cancel = CancellationToken::new();
        let name: Arc<str> = Arc::from("w");
        let win2 = Arc::clone(&win);
        tokio::spawn(async move {
            run_window_actor(
                name,
                win2,
                Arc::new(EvictionGate::new(usize::MAX)),
                fanout,
                notify,
                rx,
                cancel,
                None,
            )
            .await;
        });

        let events: Arc<Vec<Arc<Event>>> = Arc::new(vec![Arc::new(Event {
            fields: Default::default(),
        })]);
        tx.send(WindowMsg::Append {
            source: Arc::from("s"),
            seq: 0,
            batch: make_batch(&test_schema(), 10_000_000_000, 7),
            events: Some(events),
            byte_size: 64,
            permits: Vec::new(),
            shard_rows: None,
        })
        .await
        .unwrap();

        let push = tokio::time::timeout(Duration::from_secs(2), rule_rx.recv())
            .await
            .expect("broadcast within timeout")
            .expect("rule channel open");
        assert_eq!(&*push.window_name, "w");
        assert_eq!(push.events.as_ref().map(|e| e.len()).unwrap_or(0), 1);
        assert_eq!(win.total_rows(), 1);
    }
}
