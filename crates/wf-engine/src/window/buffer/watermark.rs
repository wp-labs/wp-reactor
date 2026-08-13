use std::sync::Arc;

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
    pub fn append_with_watermark(&mut self, batch: RecordBatch) -> CoreResult<AppendOutcome> {
        self.append_with_watermark_inner(batch, None, None)
    }

    /// Like [`Self::append_with_watermark`], but stores already-parsed events
    /// (produced outside the window lock by the router) so rule reads never
    /// contend on the batch's `OnceLock`.
    pub fn append_with_watermark_parsed(
        &mut self,
        batch: RecordBatch,
        parsed_events: Arc<Vec<Arc<Event>>>,
    ) -> CoreResult<AppendOutcome> {
        self.append_with_watermark_inner(batch, Some(parsed_events), None)
    }

    /// Like [`Self::append_with_watermark_parsed`], but with a precomputed
    /// content byte size (R2: computed in the parallel parse worker, so the
    /// O(rows×cols) accounting stays off the ordered commit path).
    pub fn append_with_watermark_parsed_sized(
        &mut self,
        batch: RecordBatch,
        parsed_events: Arc<Vec<Arc<Event>>>,
        byte_size: usize,
    ) -> CoreResult<AppendOutcome> {
        self.append_with_watermark_inner(batch, Some(parsed_events), Some(byte_size))
    }

    fn append_with_watermark_inner(
        &mut self,
        batch: RecordBatch,
        parsed_events: Option<Arc<Vec<Arc<Event>>>>,
        byte_size: Option<usize>,
    ) -> CoreResult<AppendOutcome> {
        if batch.num_rows() == 0 {
            return Ok(AppendOutcome::Appended);
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
        if self.time_col_index.is_some() && min_event_time != i64::MIN {
            let allowed = self.config.allowed_lateness.as_duration().as_nanos() as i64;
            let cutoff = self.watermark_nanos.saturating_sub(allowed);
            if min_event_time < cutoff {
                match self.config.late_policy {
                    // SideOutput not yet implemented — treated as Drop in M10.
                    LatePolicy::Drop | LatePolicy::SideOutput => {
                        return Ok(AppendOutcome::DroppedLate);
                    }
                    LatePolicy::Revise => { /* fall through to append */ }
                }
            }
        }

        // Advance watermark AFTER lateness check.
        if self.time_col_index.is_some() && max_event_time != i64::MAX {
            let delay = self.config.watermark.as_duration().as_nanos() as i64;
            let candidate = max_event_time.saturating_sub(delay);
            self.watermark_nanos = self.watermark_nanos.max(candidate);
        }

        match (parsed_events, byte_size) {
            (Some(events), Some(size)) => self.append_parsed_sized(batch, events, size)?,
            (Some(events), None) => self.append_parsed(batch, events)?,
            (None, _) => self.append(batch)?,
        }
        Ok(AppendOutcome::Appended)
    }

    /// Current watermark in nanoseconds.
    pub fn watermark_nanos(&self) -> i64 {
        self.watermark_nanos
    }
}
