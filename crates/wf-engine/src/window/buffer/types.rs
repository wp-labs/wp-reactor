use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use crate::match_engine::{Event, batch_to_events};

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
    /// Lazily parsed full events, shared by all consuming rules via `Arc`.
    ///
    /// Previously every rule parsed the batch independently (`batch_to_events`
    /// per rule), materializing the same `Value`s N times — the dominant RSS
    /// on object-heavy windows (wp-reactor#19). Parsing once here and sharing
    /// the `Arc` drops that to one copy for all rules.
    pub(super) parsed_events: OnceLock<Arc<Vec<Event>>>,
}

impl TimedBatch {
    /// Full parsed events for this batch, parsed once and shared.
    pub(super) fn events(&self) -> Arc<Vec<Event>> {
        self.parsed_events
            .get_or_init(|| Arc::new(batch_to_events(&self.batch)))
            .clone()
    }
}
