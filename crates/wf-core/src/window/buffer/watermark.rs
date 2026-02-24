use anyhow::{Result, bail};
use arrow::record_batch::RecordBatch;
use wf_config::LatePolicy;

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
    pub fn append_with_watermark(&mut self, batch: RecordBatch) -> Result<AppendOutcome> {
        if batch.num_rows() == 0 {
            return Ok(AppendOutcome::Appended);
        }

        if batch.schema() != self.schema {
            bail!(
                "schema mismatch: window {:?} expects {:?}, got {:?}",
                self.name,
                self.schema,
                batch.schema()
            );
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

        self.append(batch)?;
        Ok(AppendOutcome::Appended)
    }

    /// Current watermark in nanoseconds.
    pub fn watermark_nanos(&self) -> i64 {
        self.watermark_nanos
    }
}
