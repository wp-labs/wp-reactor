//! Window actor: the single writer per window (subscription model).
//!
//! Each window owns one actor task that is the **only** writer of its data
//! plane; readers (rule pull / join / snapshot / metrics sampling) access the
//! `Arc<Window>` concurrently and lock-free (SkipMap + atomics). Upstream
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

use arrow::record_batch::RecordBatch;
use tokio::sync::mpsc;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

use super::buffer::{AppendOutcome, Window};
use super::fanout::RuleFanout;
use crate::match_engine::Event;

/// Bounded depth of a window channel (messages). The byte budget is the
/// primary bound; depth just caps message-count overhead for tiny batches.
pub const WINDOW_CHANNEL_DEPTH: usize = 16;

/// Largest number of byte-budget permits acquired in one call. Chunking keeps
/// a single oversized batch from deadlocking a budget smaller than itself
/// (each chunk is ≤ budget, so acquisition completes once earlier messages
/// are appended and their permits released) — same argument as the parse
/// preread budget.
const WINDOW_BUDGET_CHUNK_BYTES: usize = 8 * 1024 * 1024;

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
    },
}

/// Per-window mailbox: the bounded actor channel plus its byte budget.
#[derive(Clone)]
pub struct WindowMailbox {
    pub tx: mpsc::Sender<WindowMsg>,
    pub budget: Arc<Semaphore>,
}

/// Per-window append outcome reporter: `(window_name, rows, late)`. Wired by
/// the runtime to the existing route-report metrics so EPS accounting
/// (`window_append_total`) is unchanged by the actor path.
pub type WindowAppendReport = Arc<dyn Fn(&str, usize, bool) + Send + Sync>;

/// Acquire `bytes` permits from a window budget, in chunks (see
/// [`WINDOW_BUDGET_CHUNK_BYTES`]). Permits are returned as owned handles;
/// dropping them releases the budget.
pub async fn acquire_window_budget(
    budget: &Arc<Semaphore>,
    bytes: usize,
) -> Vec<OwnedSemaphorePermit> {
    let mut permits = Vec::new();
    let mut remaining = bytes.max(1);
    while remaining > 0 {
        let chunk = remaining.min(WINDOW_BUDGET_CHUNK_BYTES);
        match budget.clone().acquire_many_owned(chunk as u32).await {
            Ok(permit) => permits.push(permit),
            // Semaphore closed (shutdown): stop waiting; the channel send
            // will fail and unwind the producer anyway.
            Err(_) => break,
        }
        remaining -= chunk;
    }
    permits
}

/// Run the single-writer actor for one window until cancelled (or every
/// sender drops — embedded/test mode).
///
/// The actor is the only caller of append-path methods on `win`; all other
/// access is concurrent lock-free reading. On cancellation it commits
/// whatever is already queued (bounded, non-blocking drain) so a graceful
/// shutdown does not lose the queued tail, then stops.
pub async fn run_window_actor(
    name: Arc<str>,
    win: Arc<Window>,
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
                    commit_append(&name, &win, &fanout, &notify, &report, msg).await;
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
                    Some(m) => {
                        let (source, seq) = match &m {
                            WindowMsg::Append { source, seq, .. } => (Arc::clone(source), *seq),
                        };
                        let cursor = next_seq.entry(Arc::clone(&source)).or_insert(0);
                        if seq == *cursor {
                            commit_append(&name, &win, &fanout, &notify, &report, m).await;
                            *cursor += 1;
                            // Drain this source's now-consecutive pending tail.
                            // The gap never blocked the channel — these
                            // arrived earlier and parked in `pending`.
                            while let Some(msg) =
                                pending.remove(&(Arc::clone(&source), *cursor))
                            {
                                commit_append(&name, &win, &fanout, &notify, &report, msg)
                                    .await;
                                *cursor += 1;
                            }
                        } else {
                            // Future seq for this source: park it (gap or
                            // cross-source interleaving); keep dequeuing.
                            if seq < *cursor {
                                log::warn!(
                                    "window actor {:?}: stale seq {} <= cursor {} (source {:?})",
                                    name, seq, cursor, source
                                );
                            }
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
                let first_seq = first.0 .1;
                if first_seq != *cursor {
                    log::warn!(
                        "window actor {:?}: source {:?} sequence gap at shutdown: next={}, first_pending={}",
                        name, source, cursor, first_seq
                    );
                }
            }
            while let Some(msg) = pending.remove(&(Arc::clone(&source), *cursor)) {
                commit_append(&name, &win, &fanout, &notify, &report, msg).await;
                *cursor += 1;
            }
        }
        // Anything still pending sits on a real gap (a lost batch); dropping
        // the messages releases their budget permits.
    }
}

/// Append one message's batch to the window (watermark-aware) and broadcast
/// to rule subscribers. Consumes the message — dropping it releases the
/// window byte-budget permits whether the batch was appended or dropped late.
async fn commit_append(
    name: &Arc<str>,
    win: &Arc<Window>,
    fanout: &Arc<RuleFanout>,
    notify: &Arc<Notify>,
    report: &Option<WindowAppendReport>,
    msg: WindowMsg,
) {
    let WindowMsg::Append {
        source: _,
        seq: _,
        batch,
        events,
        byte_size,
        permits: _,
    } = msg;
    let rows = batch.num_rows();
    let result = if let Some(events) = events.as_ref() {
        win.append_with_watermark_parsed_sized(batch, Arc::clone(events), byte_size)
    } else {
        win.append_with_watermark_sized(batch, byte_size)
    };
    match result {
        Ok((AppendOutcome::Appended, batch_seq)) => {
            // Fast-path windows (events == None) have no rule subscribers;
            // broadcast is skipped exactly as on the commit-worker path.
            if let Some(events) = &events {
                fanout.broadcast(name, events, batch_seq).await;
            }
            notify.notify_waiters();
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
    use crate::window::{WindowDef, WindowParams};
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

    async fn spawn_actor(
        win: Arc<Window>,
    ) -> (mpsc::Sender<WindowMsg>, CancellationToken) {
        let (tx, rx) = mpsc::channel::<WindowMsg>(WINDOW_CHANNEL_DEPTH);
        let cancel = CancellationToken::new();
        let name: Arc<str> = Arc::from("w");
        let fanout = RuleFanout::new();
        let notify = Arc::new(Notify::new());
        let cancel2 = cancel.clone();
        tokio::spawn(async move {
            run_window_actor(name, win, fanout, notify, rx, cancel2, None).await;
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

    // -- 3. independent per-source cursors -------------------------------------

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

    #[tokio::test]
    async fn budget_permits_release_after_append() {
        let budget = Arc::new(Semaphore::new(128));
        let win = make_window("w");
        let (tx, _cancel) = spawn_actor(Arc::clone(&win)).await;

        // Acquire, send, and verify the budget drains then refills as the
        // actor finishes each message.
        for seq in 0..4u64 {
            let permits = acquire_window_budget(&budget, 64).await;
            assert_eq!(permits.len(), 1);
            tx.send(WindowMsg::Append {
                source: Arc::from("s"),
                seq,
                batch: make_batch(&test_schema(), (seq as i64) * 10_000_000_000, seq as i64),
                events: None,
                byte_size: 64,
                permits,
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
            run_window_actor(name, win2, fanout, notify, rx, cancel, None).await;
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
        })
        .await
        .unwrap();

        let push = tokio::time::timeout(Duration::from_secs(2), rule_rx.recv())
            .await
            .expect("broadcast within timeout")
            .expect("rule channel open");
        assert_eq!(&*push.window_name, "w");
        assert_eq!(push.events.len(), 1);
        assert_eq!(win.total_rows(), 1);
    }
}
