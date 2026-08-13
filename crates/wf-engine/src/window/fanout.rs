use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use tokio::sync::mpsc;

use crate::match_engine::Event;

/// A batch of parsed events pushed from one window to its subscribing rules.
///
/// The `window_name` tags which window the events were appended to, so a rule
/// subscribed to multiple windows can map the batch to the correct aliases.
#[derive(Clone)]
pub struct RulePush {
    pub window_name: Arc<str>,
    pub events: Arc<Vec<Event>>,
}

/// Fan-out table mapping window names to per-rule channels.
///
/// The router (producer) broadcasts each parsed `Arc<Vec<Event>>` to every
/// channel registered for the window it was appended to; rule tasks (consumers)
/// receive those `Arc`s and advance their state machines without taking the
/// window read lock. Registration happens at rule-task spawn time; closed
/// channels (from a drained/cancelled rule) are pruned lazily on the next
/// broadcast.
#[derive(Default)]
pub struct RuleFanout {
    table: RwLock<HashMap<String, Vec<mpsc::UnboundedSender<RulePush>>>>,
}

impl RuleFanout {
    /// Create a fresh, empty fan-out table.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Register a rule channel for `window_name`.
    pub fn register(&self, window_name: &str, tx: mpsc::UnboundedSender<RulePush>) {
        let mut table = self.table.write().expect("fanout lock poisoned");
        table.entry(window_name.to_string()).or_default().push(tx);
    }

    /// Broadcast `events` to every rule channel registered for `window_name`.
    ///
    /// Uses an unbounded channel so a slow consumer never blocks the router.
    /// This is R1's "block to preserve correctness" formulation expressed as
    /// "never drop" — dropping under backpressure (with an explicit gap metric)
    /// is deferred to a follow-up. Channels whose receiver has been dropped
    /// (cancelled/drained rule) are pruned lazily here.
    pub fn broadcast(&self, window_name: &str, events: &Arc<Vec<Event>>) {
        let push = RulePush {
            window_name: window_name.into(),
            events: Arc::clone(events),
        };

        let any_closed = {
            let table = self.table.read().expect("fanout lock poisoned");
            let Some(senders) = table.get(window_name) else {
                return;
            };
            let mut any_closed = false;
            for tx in senders {
                // Unbounded send never blocks; only fails once the receiver is
                // gone, which we then prune below.
                if tx.send(push.clone()).is_err() {
                    any_closed = true;
                }
            }
            any_closed
        };

        if any_closed {
            let mut table = self.table.write().expect("fanout lock poisoned");
            if let Some(senders) = table.get_mut(window_name) {
                senders.retain(|tx| !tx.is_closed());
                if senders.is_empty() {
                    table.remove(window_name);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn broadcast_delivers_same_arc_to_registered_channels() {
        let fanout = RuleFanout::new();
        let (tx, mut rx) = mpsc::unbounded_channel();
        fanout.register("win_a", tx);

        let events: Arc<Vec<Event>> = Arc::new(Vec::new());
        fanout.broadcast("win_a", &events);

        let push = rx
            .try_recv()
            .expect("registered channel should receive a push");
        assert_eq!(&*push.window_name, "win_a");
        assert!(
            Arc::ptr_eq(&push.events, &events),
            "should share the same Arc"
        );
    }

    #[test]
    fn broadcast_prunes_closed_channels() {
        let fanout = RuleFanout::new();
        let (tx, rx) = mpsc::unbounded_channel();
        fanout.register("win_a", tx);
        drop(rx); // close the channel

        let events: Arc<Vec<Event>> = Arc::new(Vec::new());
        fanout.broadcast("win_a", &events);

        let table = fanout.table.read().expect("fanout lock poisoned");
        assert!(
            !table.contains_key("win_a"),
            "closed channel should be pruned on broadcast"
        );
    }
}
