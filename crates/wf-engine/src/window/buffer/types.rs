use std::collections::HashSet;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use crate::match_engine::{Event, batch_to_events, batch_to_events_filtered};

/// Result of a watermark-aware append.
pub enum AppendOutcome {
    Appended,
    DroppedLate,
}

/// Parameters for constructing a [`Window`](super::Window).
#[derive(::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.WindowManager")]
pub struct WindowParams {
    pub name: String,
    pub schema: SchemaRef,
    /// Index of the time column in the schema, `None` for output windows.
    pub time_col_index: Option<usize>,
    /// Retention duration from the `.wfs` file.
    pub over: Duration,
    /// Optional whitelist of fields to materialize into per-event
    /// `HashMap<String, Value>` (wp-lang `field_usage`). `None` = all fields.
    /// Rules that wholesale-scan events keep `None`; everything else only
    /// materializes the fields rules actually read — the dominant peak RSS
    /// win on wide windows.
    pub materialize_fields: Option<Arc<HashSet<String>>>,
    /// L2 deferred materialization: every bound rule has a columnar bind filter,
    /// so rule tasks materialize only the rows their filter accepts (instead of
    /// the whole batch in `route_parse`).
    pub defer_materialization: bool,
}

#[derive(::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.WindowManager")]
pub(in crate::window) struct TimedBatch {
    pub(super) batch: RecordBatch,
    /// (min, max) event time in nanoseconds.
    pub(super) event_time_range: (i64, i64),
    #[allow(dead_code)]
    pub(super) ingested_at: Instant,
    pub(super) row_count: usize,
    pub(super) byte_size: usize,
    /// Monotonically increasing sequence number assigned on append.
    pub(super) seq: u64,
    /// Precomputed columnar shard partition of this batch's rows, produced in
    /// the parallel parse stage and stored once so every sharded rule task
    /// pulls *only* its own row subset on the single-writer critical path
    /// (P2: zero re-partition). `Some` only for deferred-materialization
    /// windows with a sharded (key-partitioned) subscription; the outer Vec
    /// is indexed by shard, each inner Vec the absolute batch-row indices that
    /// shard owns. `None` for unsharded windows / non-deferred batches — the
    /// pull path processes the whole batch.
    pub(super) shard_rows: Option<Arc<Vec<Vec<u32>>>>,
    /// Lazily parsed full events, shared by all consuming rules via `Arc`.
    ///
    /// Previously every rule parsed the batch independently (`batch_to_events`
    /// per rule), materializing the same `Value`s N times — the dominant RSS
    /// on object-heavy windows (wp-reactor#19). Parsing once here and sharing
    /// the `Arc` drops that to one copy for all rules.
    pub(super) parsed_events: OnceLock<Arc<Vec<Arc<Event>>>>,
}

impl TimedBatch {
    /// Full parsed events for this batch, parsed once and shared.
    ///
    /// `materialize` optionally restricts the field set per event (from
    /// `WindowParams::materialize_fields`); `None` materializes every schema
    /// field. The set is fixed per window, so the `OnceLock` cache stays
    /// consistent.
    pub(super) fn events(&self, materialize: Option<&HashSet<String>>) -> Arc<Vec<Arc<Event>>> {
        self.parsed_events
            .get_or_init(|| {
                let events = match materialize {
                    Some(fields) => batch_to_events_filtered(&self.batch, fields),
                    None => batch_to_events(&self.batch),
                };
                Arc::new(events.into_iter().map(Arc::new).collect())
            })
            .clone()
    }
}
