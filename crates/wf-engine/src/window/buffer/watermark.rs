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
        self.append_with_watermark_inner(batch, None, None, None, None)
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
        self.append_with_watermark_inner(batch, Some(parsed_events), None, None, None)
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
        shard_rows: Option<Arc<Vec<Vec<u32>>>>,
    ) -> CoreResult<(AppendOutcome, u64)> {
        self.append_with_watermark_inner(
            batch,
            Some(parsed_events),
            Some(byte_size),
            shard_rows,
            None,
        )
    }

    /// Like [`Self::append_with_watermark_parsed_sized`], but the caller records
    /// the batch's source so the window can track its per-source committed
    /// frontier (2026-08-25 cross-source reorder fix). Used by the window actor
    /// when a rule subscribes to this window (`events = Some`) — without this,
    /// a subscribed join target would silently fall back to the unsound global
    /// `max_event_time` in [`Self::committed_frontier_ns`].
    pub fn append_with_watermark_parsed_sized_from(
        &self,
        batch: RecordBatch,
        parsed_events: Arc<Vec<Arc<Event>>>,
        byte_size: usize,
        shard_rows: Option<Arc<Vec<Vec<u32>>>>,
        source: Arc<str>,
    ) -> CoreResult<(AppendOutcome, u64)> {
        self.append_with_watermark_inner(
            batch,
            Some(parsed_events),
            Some(byte_size),
            shard_rows,
            Some(source),
        )
    }

    /// Like [`Self::append_with_watermark_parsed_sized`], but without pre-parsed
    /// events: the batch is stored with an *uninitialized* `parsed_events`, so a
    /// consumer reading via `events_since()` still gets the lazily-parsed events.
    /// Used by the router's fast path for windows no rule currently consumes.
    pub fn append_with_watermark_sized(
        &self,
        batch: RecordBatch,
        byte_size: usize,
        shard_rows: Option<Arc<Vec<Vec<u32>>>>,
    ) -> CoreResult<(AppendOutcome, u64)> {
        self.append_with_watermark_inner(batch, None, Some(byte_size), shard_rows, None)
    }

    /// 2026-08-25（跨源提交乱序修复）：窗口 actor 专用入口——带提交来源。
    /// 与 [`Self::append_with_watermark_sized`] 等价，但会记录该 source 的
    /// 已提交最大事件时间（`committed_frontier_ns` 的输入）。deferred 评估
    /// gate 用健全前沿替代全局 max，避免跨源乱序下的假 miss。
    pub fn append_with_watermark_sized_from(
        &self,
        batch: RecordBatch,
        byte_size: usize,
        shard_rows: Option<Arc<Vec<Vec<u32>>>>,
        source: Arc<str>,
    ) -> CoreResult<(AppendOutcome, u64)> {
        self.append_with_watermark_inner(batch, None, Some(byte_size), shard_rows, Some(source))
    }

    fn append_with_watermark_inner(
        &self,
        batch: RecordBatch,
        parsed_events: Option<Arc<Vec<Arc<Event>>>>,
        byte_size: Option<usize>,
        shard_rows: Option<Arc<Vec<Vec<u32>>>>,
        source: Option<Arc<str>>,
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
            (Some(events), Some(size)) => {
                self.append_parsed_sized(batch, events, size, shard_rows)?
            }
            (Some(events), None) => {
                self.append_parsed(batch, events)?;
                0
            }
            // Columnar/deferred commit (pull-model sharded match rules):
            // events are `None` but `shard_rows` carries the precomputed
            // partition — must be persisted into the window log, not dropped.
            (None, Some(size)) => self.append_sized(batch, size, shard_rows)?,
            (None, None) => {
                self.append(batch)?;
                0
            }
        };
        // 2026-08-30 q3 根因（nexmark_pk）：`max_event_time_nanos` 必须在批次
        // **提交后**（append_inner 完成、join 索引已建）才推进——此前在
        // append_inner（含 index_batch）之前推进，而 `committed_frontier_ns` 对
        // per-source 为空（首个 actor append 进行中）回退到全局 max → 把未提交
        // 的 max 报为已提交 → eager join gate 提前放行 → join 与目标窗建索引并发
        // → snapshot join 静默 miss（buffer 有行、索引没有；q3 丢 0~16 早期
        // auction，oracle 对拍定位）。推进时序与 per-source 提交前沿一致：只在
        // 真正 append 后记录，提交前沿永远不会领先索引内容。
        if self.time_col_index.is_some() && max_event_time != i64::MAX {
            // Raw max event time (before the watermark delay) — the global data
            // tail the rule task needs at flush (see `max_event_time_nanos`).
            self.max_event_time_nanos
                .fetch_max(max_event_time, Ordering::AcqRel);
        }
        // 2026-08-25（跨源提交乱序修复）：记录该 source 的已提交最大事件时间。
        // 只在真正 append（非 DroppedLate）后记录；无时间列窗口不推进 max，
        // 不记录（源路径不产生时间语义）。
        if let Some(src) = source
            && self.time_col_index.is_some()
            && max_event_time != i64::MAX
        {
            self.per_source_max_event_time
                .lock()
                .expect("per-source max lock poisoned")
                .entry(src)
                .and_modify(|m| *m = (*m).max(max_event_time))
                .or_insert(max_event_time);
        }
        Ok((AppendOutcome::Appended, seq))
    }

    /// 2026-08-25（跨源提交乱序修复）：**健全提交前沿** = 各 source 已提交
    /// 最大事件时间的 min。`max_event_time_nanos`（全局 max）可能被任一 source
    /// 的晚 batch 提前推高（actor 只保证 source 内 seq 有序，跨 source 自由），
    /// deferred 评估 gate 用它会在右行未提交时提前评估 → 假 miss。
    /// `committed_frontier_ns` 是"右行完整性"的健全判据：所有 source 的行都
    /// 已提交到该水位。无记录（非 actor 路径 append）时回退全局 max（旧行为）。
    pub fn committed_frontier_ns(&self) -> i64 {
        let m = self
            .per_source_max_event_time
            .lock()
            .expect("per-source max lock poisoned");
        if m.is_empty() {
            return self.max_event_time_nanos();
        }
        // map 非空 → values 非空 → min 必为 Some（unwrap 安全）。
        m.values().copied().min().unwrap()
    }

    /// Current watermark in nanoseconds.
    pub fn watermark_nanos(&self) -> i64 {
        self.watermark_nanos.load(Ordering::Acquire)
    }

    /// Raw max event time seen on append, **before** the watermark delay is
    /// subtracted (i64::MIN when no time-stamped batch has been appended).
    /// This is the true global data tail — distinct from [`Self::watermark_nanos`]
    /// which lags it by the configured watermark delay.
    pub fn max_event_time_nanos(&self) -> i64 {
        self.max_event_time_nanos.load(Ordering::Acquire)
    }

    /// Test-only setter for the event-time watermark, so time-eviction tests
    /// can pin a cutoff without appending a watermark-advancing batch.
    #[cfg(test)]
    pub(crate) fn set_watermark_for_test(&self, watermark_nanos: i64) {
        self.watermark_nanos
            .store(watermark_nanos, Ordering::Release);
    }
}
