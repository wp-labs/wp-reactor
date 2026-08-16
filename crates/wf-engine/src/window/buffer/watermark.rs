use std::sync::Arc;
use std::sync::atomic::Ordering;

use arrow::record_batch::RecordBatch;
use orion_error::conversion::ToStructError;
use wf_config::LatePolicy;

use crate::error::{CoreReason, CoreResult};
use crate::match_engine::Event;

use super::Window;
use super::types::AppendOutcome;

impl Window {
    /// Append a batch with watermark advancement and lateness checking.
    ///
    /// 1. Extracts the event-time range from the batch.
    /// 2. Advances the watermark: `max(current, max_event_time - watermark_delay)`.
    /// 3. If `min_event_time < watermark - allowed_lateness`, applies the late
    ///    policy (Drop/SideOutput → skip, Revise → append anyway).
    /// 4. Otherwise appends normally via [`Self::append`].
    ///
    /// Windows without a time column never advance the watermark and never
    /// reject data as late.
    pub fn append_with_watermark(&self, batch: RecordBatch) -> CoreResult<AppendOutcome> {
        self.append_with_watermark_inner(batch, None, None)
            .map(|(outcome, _)| outcome)
    }

    /// Like [`Self::append_with_watermark`], but stores already-parsed events
    /// (produced outside the window by the router) so rule reads never
    /// contend on the batch's `OnceLock`.
    pub fn append_with_watermark_parsed(
        &self,
        batch: RecordBatch,
        parsed_events: Arc<Vec<Arc<Event>>>,
    ) -> CoreResult<AppendOutcome> {
        self.append_with_watermark_inner(batch, Some(parsed_events), None)
            .map(|(outcome, _)| outcome)
    }

    /// Like [`Self::append_with_watermark_parsed`], but with a precomputed
    /// content byte size (R2: computed in the parallel parse worker, so the
    /// O(rows×cols) accounting stays off the ordered commit path). Returns
    /// the outcome plus the sequence number assigned to this batch (0 when
    /// not appended) — the caller uses it as the consumers' ack reference.
    pub fn append_with_watermark_parsed_sized(
        &self,
        batch: RecordBatch,
        parsed_events: Arc<Vec<Arc<Event>>>,
        byte_size: usize,
    ) -> CoreResult<(AppendOutcome, u64)> {
        self.append_with_watermark_inner(batch, Some(parsed_events), Some(byte_size))
    }

    /// Like [`Self::append_with_watermark_parsed_sized`], but without pre-parsed
    /// events: the batch is stored with an *uninitialized* `parsed_events`, so a
    /// consumer reading via `events_since()` still gets the lazily-parsed events.
    /// Used by the router's fast path for windows no rule currently consumes.
    pub fn append_with_watermark_sized(
        &self,
        batch: RecordBatch,
        byte_size: usize,
    ) -> CoreResult<(AppendOutcome, u64)> {
        self.append_with_watermark_inner(batch, None, Some(byte_size))
    }

    fn append_with_watermark_inner(
        &self,
        batch: RecordBatch,
        parsed_events: Option<Arc<Vec<Arc<Event>>>>,
        byte_size: Option<usize>,
    ) -> CoreResult<(AppendOutcome, u64)> {
        if batch.num_rows() == 0 {
            return Ok((AppendOutcome::Appended, 0));
        }

        // Accept batches that contain at least the window's fields (superset OK).
        // Extra metadata columns (e.g. machine_id) are allowed — they will be
        // carried through to events so rule executors can use them for labeling.
        if !self.schema.fields().iter().all(|f| {
            batch
                .schema()
                .field_with_name(f.name())
                .is_ok_and(|bf| bf.data_type() == f.data_type())
        }) {
            return CoreReason::DataFormat
                .to_err()
                .with_detail(format!(
                    "schema mismatch: window {:?} expects {:?}, got {:?}",
                    self.name,
                    self.schema,
                    batch.schema()
                ))
                .err();
        }

        let (min_event_time, max_event_time) = self.extract_time_range(&batch);

        // Lateness check FIRST against the current watermark (before this batch
        // advances it). This ensures a batch cannot be rejected by its own
        // watermark advancement — only by previously established watermarks.
        // The load is a snapshot: a concurrent appender may advance the
        // watermark after we read it, which only makes our check more lenient
        // (never falsely late) — source windows are single-writer (the ordered
        // commit task) in steady state anyway.
        if self.time_col_index.is_some() && min_event_time != i64::MIN {
            let allowed = self.config.allowed_lateness.as_duration().as_nanos() as i64;
            let cutoff = self
                .watermark_nanos
                .load(Ordering::Acquire)
                .saturating_sub(allowed);
            if min_event_time < cutoff {
                match self.config.late_policy {
                    // SideOutput not yet implemented — treated as Drop in M10.
                    LatePolicy::Drop | LatePolicy::SideOutput => {
                        return Ok((AppendOutcome::DroppedLate, 0));
                    }
                    LatePolicy::Revise => { /* fall through to append */ }
                }
            }
        }

        // Advance watermark AFTER lateness check (monotonic).
        if self.time_col_index.is_some() && max_event_time != i64::MAX {
            let delay = self.config.watermark.as_duration().as_nanos() as i64;
            let candidate = max_event_time.saturating_sub(delay);
            self.watermark_nanos.fetch_max(candidate, Ordering::AcqRel);
        }

        let seq = match (parsed_events, byte_size) {
            (Some(events), Some(size)) => self.append_parsed_sized(batch, events, size)?,
            (Some(events), None) => {
                self.append_parsed(batch, events)?;
                0
            }
            (None, _) => {
                self.append(batch)?;
                0
            }
        };
        Ok((AppendOutcome::Appended, seq))
    }

    /// Current watermark in nanoseconds.
    pub fn watermark_nanos(&self) -> i64 {
        self.watermark_nanos.load(Ordering::Acquire)
    }
}
