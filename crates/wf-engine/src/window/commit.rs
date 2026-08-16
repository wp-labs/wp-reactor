//! The shared append-commit primitive.
//!
//! Every path that appends a parsed batch to a window funnels through
//! [`commit_appended_batch`]: the window actor (production push path), the
//! ordered commit worker behind `Router::commit_window` (sync mode, tests,
//! embedded) and the inline fallback for windows without an actor mailbox
//! (hot-added windows). One primitive means the
//! "watermark append → rule broadcast → waiter notify" ordering cannot drift
//! between entry points.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use tokio::sync::Notify;

use crate::error::CoreResult;
use crate::match_engine::Event;

use super::buffer::{AppendOutcome, Window};
use super::fanout::RuleFanout;

/// Append `batch` to `win` (watermark-aware, byte-accounted) and, when the
/// batch was appended (not dropped late), broadcast the shared parsed events
/// to rule subscribers and wake window waiters.
///
/// The append → broadcast → notify ordering is part of the engine's contract
/// and lives here so all commit entry points execute it identically.
///
/// - `events` mirrors the caller's parse decision: `Some` when at least one
///   rule consumes this window (subscribers receive the shared `Arc`, zero
///   copy); `None` for fast-path windows with no rule subscriber (nothing to
///   broadcast, the batch's `parsed_events` stays lazily uninitialized).
/// - `notify` is the registry's waiter handle when one exists; the window
///   actor owns its notifier and always passes `Some`.
///
/// Returns the append outcome and the assigned batch seq (consumers ack
/// seq+1). Errors propagate to the caller — each entry point keeps its own
/// error policy (the actor logs and continues; the ordered path fails the
/// route).
pub(super) async fn commit_appended_batch(
    win: &Window,
    fanout: &RuleFanout,
    notify: Option<&Notify>,
    window_name: &str,
    batch: RecordBatch,
    events: Option<Arc<Vec<Arc<Event>>>>,
    byte_size: usize,
) -> CoreResult<(AppendOutcome, u64)> {
    let result = if let Some(events) = events.as_ref() {
        win.append_with_watermark_parsed_sized(batch, Arc::clone(events), byte_size)
    } else {
        win.append_with_watermark_sized(batch, byte_size)
    };
    let (outcome, batch_seq) = result?;
    if matches!(outcome, AppendOutcome::Appended) {
        if let Some(events) = &events {
            fanout.broadcast(window_name, events, batch_seq).await;
        }
        if let Some(notify) = notify {
            notify.notify_waiters();
        }
    }
    Ok((outcome, batch_seq))
}
