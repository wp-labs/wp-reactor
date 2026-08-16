use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use tokio::sync::mpsc;
use wf_lang::ast::FieldRef;

use crate::match_engine::{Event, extract_key_simple, shard_index};

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
    pub events: Arc<Vec<Arc<Event>>>,
    pub seq: u64,
}

/// A subscription for one window: a single (unsharded) rule channel, N shard
/// channels with a key partition (rule sharding, P2a), or N worker channels
/// with whole-batch round-robin (stateless `on each` sharding, R4).
///
/// Channels are **bounded** so a slow rule consumer backpressures the producer
/// (the commit worker's broadcast awaits a full channel) instead of buffering
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
/// The router (producer) broadcasts each parsed `Arc<Vec<Arc<Event>>>` to every
/// channel registered for the window it was appended to; rule tasks (consumers)
/// receive those `Arc`s and advance their state machines without taking the
/// window read lock. Registration happens at rule-task spawn time; closed
/// channels (from a drained/cancelled rule) are pruned lazily on the next
/// broadcast.
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
    pub fn register_round_robin(
        &self,
        window_name: &str,
        shards: Vec<mpsc::Sender<RulePush>>,
    ) {
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
    /// producer (`.await` on send) — backpressure instead of unbounded buffering,
    /// so a slow rule consumer stalls the ingest rather than growing RSS.
    /// Closed channels are pruned lazily here.
    pub async fn broadcast(&self, window_name: &str, events: &Arc<Vec<Arc<Event>>>, seq: u64) {
        let subs: Vec<Subscription> = {
            let table = self.table.read().expect("fanout lock poisoned");
            table.get(window_name).cloned().unwrap_or_default()
        };

        let mut any_closed = false;
        for sub in &subs {
            match sub {
                Subscription::Single(tx) => {
                    let push = RulePush {
                        window_name: window_name.into(),
                        events: Arc::clone(events),
                        seq,
                    };
                    if tx.send(push).await.is_err() {
                        any_closed = true;
                    }
                }
                Subscription::Sharded { shards, keys } => {
                    if broadcast_sharded(shards, keys, window_name, events, seq).await {
                        any_closed = true;
                    }
                }
                Subscription::RoundRobin { shards, next } => {
                    let n = shards.len();
                    let idx = next.fetch_add(1, Ordering::Relaxed) % n;
                    let push = RulePush {
                        window_name: window_name.into(),
                        events: Arc::clone(events),
                        seq,
                    };
                    if shards[idx].send(push).await.is_err() {
                        any_closed = true;
                    }
                }
            }
        }

        if any_closed {
            let mut table = self.table.write().expect("fanout lock poisoned");
            if let Some(subs) = table.get_mut(window_name) {
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
                    table.remove(window_name);
                }
            }
        }
    }
}

/// Partition a batch by match key and send each sub-batch to its shard.
/// Returns `true` if any shard channel was closed. Awaits full shard channels
/// (backpressure).
async fn broadcast_sharded(
    shards: &[mpsc::Sender<RulePush>],
    keys: &[FieldRef],
    window_name: &str,
    events: &Arc<Vec<Arc<Event>>>,
    seq: u64,
) -> bool {
    let n = shards.len();
    let mut sub_batches: Vec<Vec<Arc<Event>>> = (0..n).map(|_| Vec::new()).collect();
    for event in events.iter() {
        // Missing key → shard 0; the rule's state machine skips it anyway.
        let idx = extract_key_simple(event, keys)
            .map(|scope_key| shard_index(&scope_key, n))
            .unwrap_or(0);
        sub_batches[idx].push(Arc::clone(event));
    }

    let mut any_closed = false;
    for (i, sub) in sub_batches.into_iter().enumerate() {
        if sub.is_empty() {
            continue;
        }
        let push = RulePush {
            window_name: window_name.into(),
            events: Arc::new(sub),
            seq,
        };
        if shards[i].send(push).await.is_err() {
            any_closed = true;
        }
    }
    any_closed
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

        let push = rx.try_recv().expect("registered channel should receive a push");
        assert_eq!(&*push.window_name, "win_a");
        assert!(Arc::ptr_eq(&push.events, &events), "should share the same Arc");
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
        fanout.register_sharded("win_a", vec![tx0, tx1], Arc::from(keys().into_boxed_slice()));

        // Two distinct keys; each should land on a single (deterministic) shard.
        let events: Arc<Vec<Arc<Event>>> =
            Arc::new(vec![Arc::new(event("k1")), Arc::new(event("k2")), Arc::new(event("k1"))]);
        fanout.broadcast("win_a", &events, 0).await;

        let mut received = Vec::new();
        while let Ok(push) = rx0.try_recv() {
            received.extend(push.events.iter().map(|e| e.fields["id"].clone()));
        }
        while let Ok(push) = rx1.try_recv() {
            received.extend(push.events.iter().map(|e| e.fields["id"].clone()));
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
        let got0 = rx0.try_recv().map(|p| p.events.len()).unwrap_or(0);
        let got1 = rx1.try_recv().map(|p| p.events.len()).unwrap_or(0);
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
            let events: Arc<Vec<Arc<Event>>> =
                Arc::new(vec![Arc::new(event(&format!("e{i}a"))), Arc::new(event(&format!("e{i}b")))]);
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
        assert!(all.iter().all(|p| p.events.len() == 2));
        for push in &all {
            assert!(
                sent.iter().any(|s| Arc::ptr_eq(s, &push.events)),
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
}
