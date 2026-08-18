use std::collections::HashMap;
use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll};

use tokio::sync::mpsc;
use wf_lang::ast::FieldRef;

use crate::match_engine::event_bridge::extract_field_value;
use crate::match_engine::{Event, Value, extract_key_simple, field_ref_name, shard_index};
use arrow::record_batch::RecordBatch;

/// A batch of parsed events pushed from one window to its subscribing rules.
///
/// The `window_name` tags which window the events were appended to, so a rule
/// subscribed to multiple windows can map the batch to the correct aliases.
/// `seq` is the window-assigned batch sequence number; consumers ack
/// `seq + 1` on the window's [`WindowProgress`](crate::window::WindowProgress)
/// slot after processing, which gates time-based eviction.
#[derive(Clone)]
pub struct RulePush {
    pub window_name: Arc<str>,
    /// Pre-parsed events, when the producer materialized them. `None` means the
    /// rule task defers materialization and parses only the rows its bind filter
    /// accepts (L2).
    pub events: Option<Arc<Vec<Arc<Event>>>>,
    /// The raw batch these events were parsed from, when the producer has it.
    /// Rule tasks use it for columnar guard evaluation (zero-copy); `None` for
    /// relay pushes (intermediate pipes) that only carry parsed events.
    pub batch: Option<Arc<RecordBatch>>,
    /// Per-event field whitelist the producer used (or would use) when
    /// materializing `events`. Deferred rule tasks use it to materialize the
    /// raw `batch` with the same field set as the eager path, keeping the
    /// event representation (and downstream wfx_id) stable.
    pub materialize_fields: Option<Arc<HashSet<String>>>,
    pub seq: u64,
    /// Only set by a **sharded** broadcast that defers materialization
    /// (`events` is `None`): the batch rows this shard owns (subset of the
    /// raw `batch`, already partitioned by the match key). Unsharded pushes and
    /// row-based (pre-materialized) pushes leave this `None`. The rule task
    /// applies its columnar bind filter over exactly these rows.
    pub shard_rows: Option<Arc<Vec<u32>>>,
}

/// A subscription for one window: a single (unsharded) rule channel, N shard
/// channels with a key partition (rule sharding, P2a), or N worker channels
/// with whole-batch round-robin (stateless `on each` sharding, R4).
///
/// Channels are **bounded** so a slow rule consumer backpressures the producer
/// (the window actor's broadcast awaits a full channel) instead of buffering
/// unboundedly — 50M sustained inject with unbounded channels let RSS grow to
/// ~13GB (wp-labs/wp-reactor long-run test, 2026-08-14).
enum Subscription {
    Single(mpsc::Sender<RulePush>),
    Sharded {
        shards: Vec<mpsc::Sender<RulePush>>,
        keys: Arc<[FieldRef]>,
    },
    RoundRobin {
        shards: Vec<mpsc::Sender<RulePush>>,
        /// Next shard index (wraps via modulo on take). Shared across clones
        /// of this subscription so every broadcast advances the same cursor.
        next: Arc<AtomicUsize>,
    },
}

// Manual impl: `AtomicUsize` is not `Clone`, the round-robin cursor is shared
// behind its `Arc` instead.
impl Clone for Subscription {
    fn clone(&self) -> Self {
        match self {
            Subscription::Single(tx) => Subscription::Single(tx.clone()),
            Subscription::Sharded { shards, keys } => Subscription::Sharded {
                shards: shards.clone(),
                keys: Arc::clone(keys),
            },
            Subscription::RoundRobin { shards, next } => Subscription::RoundRobin {
                shards: shards.clone(),
                next: Arc::clone(next),
            },
        }
    }
}

/// Fan-out table mapping window names to per-rule channels.
///
/// The window actor (producer) broadcasts each parsed `Arc<Vec<Arc<Event>>>`
/// to every channel registered for the window it was appended to; rule tasks
/// (consumers) receive those `Arc`s and advance their state machines without
/// taking the window log lock. Registration happens at rule-task spawn time;
/// closed channels (from a drained/cancelled rule) are pruned lazily on the
/// next broadcast.
#[derive(Default)]
pub struct RuleFanout {
    table: RwLock<HashMap<String, Vec<Subscription>>>,
}

impl RuleFanout {
    /// Create a fresh, empty fan-out table.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Whether any rule channel is registered for `window_name`.
    ///
    /// `route_parse` uses this to skip event materialization (and its
    /// accounting) for windows no rule consumes — the dominant parse-side
    /// cost when a window has no subscribers.
    pub fn has_subscribers(&self, window_name: &str) -> bool {
        self.table
            .read()
            .expect("fanout lock poisoned")
            .get(window_name)
            .is_some_and(|subs| !subs.is_empty())
    }

    /// Register a single (unsharded) rule channel for `window_name`.
    pub fn register(&self, window_name: &str, tx: mpsc::Sender<RulePush>) {
        let mut table = self.table.write().expect("fanout lock poisoned");
        table
            .entry(window_name.to_string())
            .or_default()
            .push(Subscription::Single(tx));
    }

    /// Register N shard channels for `window_name`, partitioned by `keys`.
    ///
    /// Each broadcast batch is split by `hash(extract_key(event)) % shards.len()`
    /// so every event with the same match key lands on the same shard.
    pub fn register_sharded(
        &self,
        window_name: &str,
        shards: Vec<mpsc::Sender<RulePush>>,
        keys: Arc<[FieldRef]>,
    ) {
        debug_assert!(!shards.is_empty());
        let mut table = self.table.write().expect("fanout lock poisoned");
        table
            .entry(window_name.to_string())
            .or_default()
            .push(Subscription::Sharded { shards, keys });
    }

    /// Register N worker channels for `window_name` served whole batches in
    /// round-robin order (stateless `on each` sharding).
    ///
    /// Each broadcast sends the **entire** `Arc<Vec<Arc<Event>>>` to the next
    /// worker — zero per-event partitioning cost, zero copies. This is only
    /// correct for stateless consumers (no cross-event rule state): event
    /// ordering across batches is no longer preserved.
    pub fn register_round_robin(&self, window_name: &str, shards: Vec<mpsc::Sender<RulePush>>) {
        debug_assert!(!shards.is_empty());
        let mut table = self.table.write().expect("fanout lock poisoned");
        table
            .entry(window_name.to_string())
            .or_default()
            .push(Subscription::RoundRobin {
                shards,
                next: Arc::new(AtomicUsize::new(0)),
            });
    }

    /// Broadcast `events` (window batch with sequence `seq`) to every rule
    /// channel registered for `window_name`.
    ///
    /// Unsharded subscriptions receive the whole batch; sharded subscriptions
    /// partition it by match key. Bounded channels: a full channel blocks the
    /// producer (`.await` on send) — backpressure instead of unbounded
    /// buffering, so a slow rule consumer stalls the ingest rather than
    /// growing RSS. Closed channels are pruned lazily here.
    ///
    /// Slow-consumer semantics: each rule's *compute* is independent (own
    /// channel, shared `Arc` batches — a fast rule is not slowed by a slow
    /// one), and two things are gated by the slowest subscriber: (a) the
    /// window's eviction floor (`WindowProgress::min_acked`, so retained
    /// memory waits for the slowest rule) and (b) this broadcast itself — a
    /// full channel blocks the batch's completion (and with it the window
    /// actor's next append) until it drains. Sends to *independent*
    /// subscriptions run concurrently, though, so one full channel only
    /// backpressures its own stream, never the deliveries to the other
    /// subscribers (P1-② head-of-line fix). Per-channel FIFO is unaffected:
    /// each channel receives from exactly one send future per broadcast, and
    /// broadcasts themselves are serialized by the single-writer commit path.
    pub async fn broadcast(&self, window_name: &str, events: &Arc<Vec<Arc<Event>>>, seq: u64) {
        self.broadcast_inner(window_name, Some(events), None, None, seq)
            .await;
    }

    /// Like [`Self::broadcast`], but also forwards the raw [`RecordBatch`] the
    /// events were parsed from, so rule tasks can evaluate columnar guards
    /// zero-copy instead of materializing every row first.
    pub async fn broadcast_with_batch(
        &self,
        window_name: &str,
        events: &Arc<Vec<Arc<Event>>>,
        batch: &RecordBatch,
        materialize_fields: Option<&Arc<HashSet<String>>>,
        seq: u64,
    ) {
        self.broadcast_inner(
            window_name,
            Some(events),
            Some(batch),
            materialize_fields,
            seq,
        )
        .await;
    }

    /// Broadcast only the raw [`RecordBatch`] (L2 deferred materialization):
    /// each rule task materializes only the rows its bind filter accepts.
    /// Only valid for non-sharded subscriptions (deferral excludes sharded).
    pub async fn broadcast_batch_only(
        &self,
        window_name: &str,
        batch: &RecordBatch,
        materialize_fields: Option<&Arc<HashSet<String>>>,
        seq: u64,
    ) {
        self.broadcast_inner(window_name, None, Some(batch), materialize_fields, seq)
            .await;
    }

    async fn broadcast_inner(
        &self,
        window_name: &str,
        events: Option<&Arc<Vec<Arc<Event>>>>,
        batch: Option<&RecordBatch>,
        materialize_fields: Option<&Arc<HashSet<String>>>,
        seq: u64,
    ) {
        let subs: Vec<Subscription> = {
            let table = self.table.read().expect("fanout lock poisoned");
            table.get(window_name).cloned().unwrap_or_default()
        };
        if subs.is_empty() {
            return;
        }
        // One shared allocation per broadcast (not per subscription × shard).
        let window_name: Arc<str> = window_name.into();
        // Raw batch shared by non-sharded subscriptions; sharded subscriptions
        // partition events (so their row indices no longer match the whole
        // batch) and get `None` (interpreted fallback).
        let batch_arc: Option<Arc<RecordBatch>> = batch.map(|b| Arc::new(b.clone()));

        let mut sends: Vec<Pin<Box<dyn Future<Output = bool> + Send>>> = Vec::new();
        for sub in &subs {
            match sub {
                Subscription::Single(tx) => {
                    let push = RulePush {
                        window_name: Arc::clone(&window_name),
                        events: events.map(Arc::clone),
                        batch: batch_arc.clone(),
                        materialize_fields: materialize_fields.map(Arc::clone),
                        shard_rows: None,
                        seq,
                    };
                    let tx = tx.clone();
                    sends.push(Box::pin(async move { tx.send(push).await.is_err() }));
                }
                Subscription::Sharded { shards, keys } => {
                    match (events, batch_arc.as_ref()) {
                        // Row-based (pre-materialized events): keep the existing
                        // per-event key partition.
                        (Some(events), _) => {
                            sharded_sends(shards, keys, &window_name, events, seq, &mut sends);
                        }
                        // Columnar deferred (events=None): partition the raw batch
                        // by key and send each shard the batch + its row subset.
                        (None, Some(batch)) => {
                            let per = partition_rows_by_key(batch, keys, shards.len());
                            let per = per.unwrap_or_else(|| {
                                // Key column absent from schema → every row
                                // missing → all shard 0 (matches row-based).
                                let mut v = vec![Vec::new(); shards.len()];
                                v[0] = (0..batch.num_rows()).map(|r| r as u32).collect();
                                v
                            });
                            for (i, rows) in per.into_iter().enumerate() {
                                if i >= shards.len() || rows.is_empty() {
                                    continue;
                                }
                                let push = RulePush {
                                    window_name: Arc::clone(&window_name),
                                    events: None,
                                    batch: batch_arc.clone(), // shared Arc (refcount, zero copy)
                                    materialize_fields: materialize_fields.map(Arc::clone),
                                    shard_rows: Some(Arc::new(rows)),
                                    seq,
                                };
                                let tx = shards[i].clone();
                                sends.push(Box::pin(async move { tx.send(push).await.is_err() }));
                            }
                        }
                        // Unreachable: a sharded broadcast with neither events nor
                        // batch (no producer sends a sharded batch-only-without-batch).
                        (None, None) => {
                            debug_assert!(false, "sharded broadcast without events or batch");
                        }
                    }
                }
                Subscription::RoundRobin { shards, next } => {
                    let n = shards.len();
                    let idx = next.fetch_add(1, Ordering::Relaxed) % n;
                    let push = RulePush {
                        window_name: Arc::clone(&window_name),
                        events: events.map(Arc::clone),
                        batch: batch_arc.clone(),
                        materialize_fields: materialize_fields.map(Arc::clone),
                        shard_rows: None,
                        seq,
                    };
                    let tx = shards[idx].clone();
                    sends.push(Box::pin(async move { tx.send(push).await.is_err() }));
                }
            }
        }

        let any_closed = join_sends(sends).await;
        if any_closed {
            let mut table = self.table.write().expect("fanout lock poisoned");
            if let Some(subs) = table.get_mut(window_name.as_ref()) {
                for sub in subs.iter_mut() {
                    match sub {
                        Subscription::Single(tx) => {
                            // handled by retain below
                            let _ = tx;
                        }
                        Subscription::Sharded { shards, .. } => {
                            shards.retain(|tx| !tx.is_closed());
                        }
                        Subscription::RoundRobin { shards, .. } => {
                            shards.retain(|tx| !tx.is_closed());
                        }
                    }
                }
                subs.retain(|sub| match sub {
                    Subscription::Single(tx) => !tx.is_closed(),
                    Subscription::Sharded { shards, .. } => !shards.is_empty(),
                    Subscription::RoundRobin { shards, .. } => !shards.is_empty(),
                });
                if subs.is_empty() {
                    table.remove(window_name.as_ref());
                }
            }
        }
    }
}

/// Poll every send future to completion, concurrently (each gets polled on
/// every wake; none waits for an earlier one to finish). Returns whether any
/// channel was closed. Hand-rolled instead of pulling in `futures` — this is
/// the only combinator needed.
async fn join_sends(mut sends: Vec<Pin<Box<dyn Future<Output = bool> + Send>>>) -> bool {
    let mut any_closed = false;
    if sends.is_empty() {
        return any_closed;
    }
    std::future::poll_fn(move |cx: &mut Context<'_>| {
        let mut still: Vec<Pin<Box<dyn Future<Output = bool> + Send>>> =
            Vec::with_capacity(sends.len());
        for mut fut in sends.drain(..) {
            match fut.as_mut().poll(cx) {
                Poll::Ready(closed) => any_closed |= closed,
                Poll::Pending => still.push(fut),
            }
        }
        sends = still;
        if sends.is_empty() {
            Poll::Ready(any_closed)
        } else {
            Poll::Pending
        }
    })
    .await
}

/// Extract a field from a batch at a pre-resolved column index, byte-identical
/// to the row-based `Event.fields.get(name)` used by [`extract_key_simple`].
fn column_scalar(batch: &RecordBatch, col_idx: usize, row: usize) -> Option<Value> {
    let col = batch.column(col_idx);
    if col.is_null(row) {
        return None;
    }
    extract_field_value(batch.schema().field(col_idx), col.as_ref(), row)
}

/// Extract the match-key fields of `row` from the batch at pre-resolved column
/// indices — the columnar twin of [`extract_key_simple`](crate::match_engine::extract_key_simple)
/// over a `&Event`. Returns `None` iff any key column is null / missing (row
/// lands shard 0, same as the row-based `fields.get()?`).
///
/// Each returned `Value` is produced by the exact same `extract_field_value`
/// the row-based path uses, so the two agree per-field byte-for-byte (including
/// Int64→f64 round-trip / Timestamp / Utf8 / structured-JSON / null lanes).
fn extract_key_columnar(batch: &RecordBatch, col_idx: &[usize], row: usize) -> Option<Vec<Value>> {
    let mut scope_key = Vec::with_capacity(col_idx.len());
    for &ci in col_idx {
        scope_key.push(column_scalar(batch, ci, row)?);
    }
    Some(scope_key)
}

/// Partition a batch's rows by the match key into per-shard row-index subsets,
/// so a sharded rule can be fed the raw batch + a row subset (zero per-event
/// materialization) instead of a fully materialized `Vec<Arc<Event>>`.
///
/// Byte-identical partition to the row-based [`sharded_sends`]: same
/// `extract_field_value` value, same `make_scope_key_str` → FNV `shard_index`.
/// A row whose key column is missing / null/ absent from the schema lands on
/// shard 0, exactly like the row-based missing-key `unwrap_or(0)`.
/// Returns `None` when a key field is absent from the whole schema (then every
/// row is missing → all shard 0).
fn partition_rows_by_key(
    batch: &RecordBatch,
    keys: &[FieldRef],
    shard_count: usize,
) -> Option<Vec<Vec<u32>>> {
    // Resolve each key field to its batch column index once (schema immutable).
    let col_idx: Vec<usize> = keys
        .iter()
        .map(field_ref_name)
        .map(|name| batch.schema().index_of(name).ok())
        .collect::<Option<_>>()?;
    let mut per: Vec<Vec<u32>> = (0..shard_count).map(|_| Vec::new()).collect();
    for row in 0..batch.num_rows() {
        // Missing key (any key column null/absent) → shard 0, same as the
        // row-based `extract_key_simple(...).unwrap_or(0)`.
        let idx = extract_key_columnar(batch, &col_idx, row)
            .map(|scope_key| shard_index(&scope_key, shard_count))
            .unwrap_or(0);
        per[idx].push(row as u32);
    }
    Some(per)
}

/// Partition a batch by match key and push one send future per non-empty
/// shard into `sends`. Awaits full shard channels via the caller's join
/// (backpressure).
fn sharded_sends(
    shards: &[mpsc::Sender<RulePush>],
    keys: &[FieldRef],
    window_name: &Arc<str>,
    events: &Arc<Vec<Arc<Event>>>,
    seq: u64,
    sends: &mut Vec<Pin<Box<dyn Future<Output = bool> + Send>>>,
) {
    let n = shards.len();
    let mut sub_batches: Vec<Vec<Arc<Event>>> = (0..n).map(|_| Vec::new()).collect();
    for event in events.iter() {
        // Missing key → shard 0; the rule's state machine skips it anyway.
        let idx = extract_key_simple(event, keys)
            .map(|scope_key| shard_index(&scope_key, n))
            .unwrap_or(0);
        sub_batches[idx].push(Arc::clone(event));
    }

    for (i, sub) in sub_batches.into_iter().enumerate() {
        if sub.is_empty() {
            continue;
        }
        let push = RulePush {
            window_name: Arc::clone(window_name),
            events: Some(Arc::new(sub)),
            batch: None,
            materialize_fields: None,
            shard_rows: None,
            seq,
        };
        let tx = shards[i].clone();
        sends.push(Box::pin(async move { tx.send(push).await.is_err() }));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::match_engine::{EngineHashMap, Value};

    fn event(id: &str) -> Event {
        let mut fields = EngineHashMap::default();
        fields.insert("id".into(), Value::Str(id.into()));
        Event { fields }
    }

    fn keys() -> Vec<FieldRef> {
        vec![FieldRef::Simple("id".into())]
    }

    #[tokio::test]
    async fn broadcast_delivers_same_arc_to_registered_channels() {
        let fanout = RuleFanout::new();
        let (tx, mut rx) = mpsc::channel(8);
        fanout.register("win_a", tx);

        let events: Arc<Vec<Arc<Event>>> = Arc::new(Vec::new());
        fanout.broadcast("win_a", &events, 0).await;

        let push = rx
            .try_recv()
            .expect("registered channel should receive a push");
        assert_eq!(&*push.window_name, "win_a");
        assert!(
            push.events
                .as_ref()
                .is_some_and(|e| Arc::ptr_eq(e, &events)),
            "should share the same Arc"
        );
    }

    #[tokio::test]
    async fn broadcast_prunes_closed_channels() {
        let fanout = RuleFanout::new();
        let (tx, rx) = mpsc::channel(8);
        fanout.register("win_a", tx);
        drop(rx); // close the channel

        let events: Arc<Vec<Arc<Event>>> = Arc::new(Vec::new());
        fanout.broadcast("win_a", &events, 0).await;

        let table = fanout.table.read().expect("fanout lock poisoned");
        assert!(
            !table.contains_key("win_a"),
            "closed channel should be pruned on broadcast"
        );
    }

    #[tokio::test]
    async fn sharded_broadcast_partitions_by_key_and_routes_same_key_together() {
        let fanout = RuleFanout::new();
        let (tx0, mut rx0) = mpsc::channel(8);
        let (tx1, mut rx1) = mpsc::channel(8);
        fanout.register_sharded(
            "win_a",
            vec![tx0, tx1],
            Arc::from(keys().into_boxed_slice()),
        );

        // Two distinct keys; each should land on a single (deterministic) shard.
        let events: Arc<Vec<Arc<Event>>> = Arc::new(vec![
            Arc::new(event("k1")),
            Arc::new(event("k2")),
            Arc::new(event("k1")),
        ]);
        fanout.broadcast("win_a", &events, 0).await;

        let mut received = Vec::new();
        while let Ok(push) = rx0.try_recv() {
            received.extend(
                push.events
                    .as_ref()
                    .unwrap()
                    .iter()
                    .map(|e| e.fields["id"].clone()),
            );
        }
        while let Ok(push) = rx1.try_recv() {
            received.extend(
                push.events
                    .as_ref()
                    .unwrap()
                    .iter()
                    .map(|e| e.fields["id"].clone()),
            );
        }

        // Union of the shards == the original batch (no loss, no dup).
        let mut ids: Vec<String> = received
            .into_iter()
            .map(|v| match v {
                Value::Str(s) => s.to_string(),
                _ => panic!("expected str"),
            })
            .collect();
        ids.sort();
        assert_eq!(ids, vec!["k1", "k1", "k2"]);

        // Same key (`k1`) must land on the SAME shard across broadcasts.
        let idx = shard_index(&[Value::Str("k1".into())], 2);
        let again: Arc<Vec<Arc<Event>>> = Arc::new(vec![Arc::new(event("k1"))]);
        fanout.broadcast("win_a", &again, 1).await;
        let got0 = rx0
            .try_recv()
            .map(|p| p.events.as_ref().map(|e| e.len()).unwrap_or(0))
            .unwrap_or(0);
        let got1 = rx1
            .try_recv()
            .map(|p| p.events.as_ref().map(|e| e.len()).unwrap_or(0))
            .unwrap_or(0);
        if idx == 0 {
            assert_eq!(got0, 1);
            assert_eq!(got1, 0);
        } else {
            assert_eq!(got0, 0);
            assert_eq!(got1, 1);
        }
    }

    #[test]
    fn shard_index_is_deterministic_and_in_range() {
        let n = 4;
        for id in ["a", "b", "c", "same", "same"] {
            let idx = shard_index(&[Value::Str(id.into())], n);
            assert!(idx < n);
        }
        // Same key → same index, across repeated calls.
        assert_eq!(
            shard_index(&[Value::Str("same".into())], n),
            shard_index(&[Value::Str("same".into())], n)
        );
    }

    #[test]
    fn shard_index_single_shard_is_zero() {
        assert_eq!(shard_index(&[Value::Str("anything".into())], 1), 0);
    }

    #[tokio::test]
    async fn round_robin_broadcast_delivers_whole_batches_and_shares_arcs() {
        let fanout = RuleFanout::new();
        let (tx0, mut rx0) = mpsc::channel(8);
        let (tx1, mut rx1) = mpsc::channel(8);
        fanout.register_round_robin("win_rr", vec![tx0, tx1]);

        // Four distinct batches; round-robin must send each WHOLE batch (same
        // Arc) to alternating workers with no loss / no duplication.
        let mut sent = Vec::new();
        for i in 0..4 {
            let events: Arc<Vec<Arc<Event>>> = Arc::new(vec![
                Arc::new(event(&format!("e{i}a"))),
                Arc::new(event(&format!("e{i}b"))),
            ]);
            sent.push(Arc::clone(&events));
            fanout.broadcast("win_rr", &events, 0).await;
        }

        let mut got0 = Vec::new();
        while let Ok(push) = rx0.try_recv() {
            got0.push(push);
        }
        let mut got1 = Vec::new();
        while let Ok(push) = rx1.try_recv() {
            got1.push(push);
        }

        // Exactly one worker per batch, alternating.
        assert_eq!(got0.len(), 2, "worker 0 receives 2 batches");
        assert_eq!(got1.len(), 2, "worker 1 receives 2 batches");
        // Whole batch preserved: 2 events per delivered push, same Arc as sent.
        let all: Vec<&RulePush> = got0.iter().chain(got1.iter()).collect();
        assert!(
            all.iter()
                .all(|p| p.events.as_ref().map(|e| e.len()).unwrap_or(0) == 2)
        );
        for push in &all {
            assert!(
                sent.iter()
                    .any(|s| push.events.as_ref().is_some_and(|e| Arc::ptr_eq(s, e))),
                "delivered batch must be one of the sent Arcs (zero copy)"
            );
        }
        assert_eq!(&*all[0].window_name, "win_rr");
    }

    #[tokio::test]
    async fn round_robin_broadcast_prunes_closed_shards() {
        let fanout = RuleFanout::new();
        let (tx0, mut rx0) = mpsc::channel(8);
        let (tx1, rx1) = mpsc::channel(8);
        fanout.register_round_robin("win_rr2", vec![tx0, tx1]);
        drop(rx1); // worker 1 shut down

        let events: Arc<Vec<Arc<Event>>> = Arc::new(vec![Arc::new(event("x"))]);
        // Broadcast enough times to hit the closed shard and trigger pruning.
        for _ in 0..2 {
            fanout.broadcast("win_rr2", &events, 0).await;
        }

        // Surviving shard still receives (at least) one delivery.
        let mut delivered = 0;
        while rx0.try_recv().is_ok() {
            delivered += 1;
        }
        assert!(delivered >= 1, "open shard must still receive batches");

        let table = fanout.table.read().expect("fanout lock poisoned");
        let subs = table.get("win_rr2").expect("subscription survives");
        assert!(
            !subs.is_empty(),
            "subscription with one open shard must not be pruned entirely"
        );
    }

    /// P1-② regression: a full (slow-consumer) channel must not head-of-line
    /// block the deliveries to the *other* subscriptions of the same window.
    /// The sends run concurrently, so the fast subscriber receives its copy
    /// immediately even while the slow one's send is still parked.
    #[tokio::test]
    async fn slow_consumer_does_not_block_other_subscribers() {
        let fanout = RuleFanout::new();
        // Slow consumer: capacity 1, never recv'd → its send parks after the
        // first broadcast fills the channel.
        let (slow_tx, _slow_rx_keep) = mpsc::channel::<RulePush>(1);
        // Fast consumer: capacity 8, drained immediately.
        let (fast_tx, mut fast_rx) = mpsc::channel::<RulePush>(8);
        fanout.register("win_hol", slow_tx);
        fanout.register("win_hol", fast_tx);

        let events: Arc<Vec<Arc<Event>>> = Arc::new(vec![Arc::new(event("e1"))]);
        fanout.broadcast("win_hol", &events, 0).await;

        // Second broadcast: the slow channel is full and would block a serial
        // send loop before the fast subscriber's delivery. Drive the
        // broadcast future concurrently with the fast recv — the broadcast
        // stays parked on the slow channel, the fast delivery must not wait.
        let events2: Arc<Vec<Arc<Event>>> = Arc::new(vec![Arc::new(event("e2"))]);
        let broadcast = fanout.broadcast("win_hol", &events2, 1);
        tokio::pin!(broadcast);
        let got = tokio::time::timeout(std::time::Duration::from_millis(500), async {
            tokio::select! {
                biased;
                r = fast_rx.recv() => r,
                // If the broadcast ever completes while the slow channel is
                // still full and undrained, something is wrong; ignore and
                // let the outer timeout fail the test.
                _ = &mut broadcast => fast_rx.try_recv().ok(),
            }
        })
        .await
        .expect("fast subscriber must receive within timeout")
        .expect("fast channel open");
        assert_eq!(
            got.events.as_ref().map(|e| e.len()).unwrap_or(0),
            1,
            "fast subscriber got the second batch"
        );
    }

    /// Same property for sharded subscriptions: a full shard must not block
    /// the other shards' deliveries of the same broadcast.
    #[tokio::test]
    async fn slow_shard_does_not_block_other_shards() {
        let fanout = RuleFanout::new();
        let (slow_tx, _slow_rx_keep) = mpsc::channel::<RulePush>(1);
        let (fast_tx, mut fast_rx) = mpsc::channel::<RulePush>(8);
        // Two shards partitioned by "id"; keys k1/k2 deterministically split.
        fanout.register_sharded(
            "win_sh",
            vec![slow_tx, fast_tx],
            Arc::from(keys().into_boxed_slice()),
        );

        let idx_k1 = shard_index(&[Value::Str("k1".into())], 2);
        let (slow_key, fast_key) = if idx_k1 == 0 {
            ("k1", "k2")
        } else {
            ("k2", "k1")
        };

        let events: Arc<Vec<Arc<Event>>> = Arc::new(vec![Arc::new(event(slow_key))]);
        fanout.broadcast("win_sh", &events, 0).await;

        // Second broadcast: the slow shard's channel is full; the fast shard
        // must still receive its sub-batch without waiting for it. Drive the
        // broadcast future concurrently with the fast shard's recv.
        let events2: Arc<Vec<Arc<Event>>> =
            Arc::new(vec![Arc::new(event(slow_key)), Arc::new(event(fast_key))]);
        let broadcast = fanout.broadcast("win_sh", &events2, 1);
        tokio::pin!(broadcast);
        let got = tokio::time::timeout(std::time::Duration::from_millis(500), async {
            tokio::select! {
                biased;
                r = fast_rx.recv() => r,
                _ = &mut broadcast => fast_rx.try_recv().ok(),
            }
        })
        .await
        .expect("fast shard must receive within timeout")
        .expect("fast shard channel open");
        assert_eq!(
            got.events.as_ref().map(|e| e.len()).unwrap_or(0),
            1,
            "fast shard got its sub-batch"
        );
    }

    #[test]
    fn partition_rows_matches_row_based_per_row() {
        // 列式分片（partition_rows_by_key，从 batch 列读 key）必须与行式分片
        // （batch_to_events + extract_key_simple + shard_index）逐行落在同一
        // shard —— Q2 键闭包 + 有状态安全的基础。含 null / UTF8 / 多行 key。
        use crate::match_engine::batch_to_events;
        use arrow::array::{ArrayRef, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use wf_lang::ast::FieldRef;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![
                Some("k1"),
                Some("k2"),
                None, // null key → both should land shard 0
                Some("k3"),
                Some("k1"),
            ])) as ArrayRef],
        )
        .unwrap();

        let keys = vec![FieldRef::Simple("id".into())];
        let shards = 3usize;

        // 列式：每行 → shard
        let per = partition_rows_by_key(&batch, &keys, shards).expect("key col present");
        let col_shard = |row: usize| -> usize {
            per.iter()
                .position(|rows| rows.contains(&(row as u32)))
                .unwrap()
        };

        // 行式：每行物化 Event → extract_key_simple → shard_index
        let events = batch_to_events(&batch);
        let row_shard = |row: usize| -> usize {
            extract_key_simple(&events[row], &keys)
                .map(|sk| shard_index(&sk, shards))
                .unwrap_or(0)
        };

        assert_eq!(batch.num_rows(), 5);
        for row in 0..batch.num_rows() {
            assert_eq!(
                col_shard(row),
                row_shard(row),
                "row {row} landed on different shard (columnar vs row-based)"
            );
        }

        // 无丢失、无重复：并集覆盖全部 5 行
        let flat: Vec<u32> = per.iter().flatten().copied().collect();
        let mut flat = flat;
        flat.sort_unstable();
        assert_eq!(flat, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn extract_key_columnar_matches_row_based() {
        // 2b 对拍：`extract_key_columnar`（从列读 key）必须与行式
        // `extract_key_simple`（Event.fields.get）逐行返回同一个 `Vec<Value>`——
        // 覆盖 Utf8、null（→ shard 0）、Int64 <2^53 与 >2^53（f64 往返）、
        // 多列 key。
        use crate::match_engine::batch_to_events;
        use arrow::array::{ArrayRef, Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use wf_lang::ast::FieldRef;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("n", DataType::Int64, true),
        ]));
        // Int64 值含 <2^53、>2^53（f64 往返丢精度 lane）、null。
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![
                    Some("k1"),
                    Some("k2"),
                    None,
                    Some("k3"),
                ])) as ArrayRef,
                Arc::new(Int64Array::from(vec![
                    Some(7),
                    Some(9007199254740993), // 2^53+1
                    Some(-3),
                    None,
                ])),
            ],
        )
        .unwrap();

        let keys = vec![FieldRef::Simple("id".into()), FieldRef::Simple("n".into())];
        let col_idx: Vec<usize> = keys
            .iter()
            .map(field_ref_name)
            .map(|name| batch.schema().index_of(name).unwrap())
            .collect();
        let events = batch_to_events(&batch);

        assert_eq!(batch.num_rows(), 4);
        for row in 0..batch.num_rows() {
            let col = extract_key_columnar(&batch, &col_idx, row);
            let rw = extract_key_simple(&events[row], &keys);
            assert_eq!(
                col,
                rw,
                "row {row}: columnar key {} != row-based key {:?}",
                format!("{:?}", col),
                rw
            );
        }
    }
}
