//! Columnar alert batches (plan C1).
//!
//! The rule→sink channel used to carry `Arc<Vec<Arc<DataRecord>>>` — one row
//! struct per alert, each with its own `Vec` of ~20 `FieldStorage` entries and
//! per-row `String` allocations. Profiling showed the build/drop churn of
//! those row structs is the single largest on-CPU cost (~11% drop glue alone),
//! and the channel backlog of row records is the main RSS high-water mark.
//!
//! A `AlertColumnBatch` stores the same records as per-field columns:
//! - The ten `__wfu_` system fields are fixed-layout columns (score is
//!   `Vec<f64>`, the string fields are `Vec<Arc<str>>` so plan-constant and
//!   batch-shared strings clone a refcount instead of copying bytes).
//! - `yield` fields are `YieldCol { metas, values }` columns; column layout
//!   is derived from the first appended record and extended on demand if a
//!   later record yields a new field (missing cells read back as
//!   `(Ignore, Null)`).
//!
//! Field order and value conversion are shared with
//! [`OutputRecord::to_data_record`](super::types::OutputRecord::to_data_record)
//! (`export_yield_value` + the same reserved-prefix / duplicate checks), and
//! [`AlertColumnBatch::iter_data_records`] reconstructs byte-equivalent
//! `DataRecord`s for sinks that still want rows — locked by unit test.

use std::sync::Arc;

use orion_error::conversion::ToStructError;
use wf_lang::FieldType;
use wp_model_core::model::{DataRecord, DataType, Field, FieldStorage, Value as ModelValue};

use crate::error::{CoreReason, CoreResult};
use crate::match_engine::Value;

use super::types as alert_types;
use super::types::{OutputRecord, WFU_PREFIX};
use wf_lang::wfu_meta::{
    WFU_CLOSE_REASON, WFU_EMIT_TIME, WFU_ENTITY_ID, WFU_ENTITY_TYPE, WFU_FIRED_AT, WFU_ID,
    WFU_ORIGIN, WFU_RULE_NAME, WFU_SCORE, WFU_SUMMARY,
};

/// One finished columnar alert batch, addressed to a single yield target.
pub struct AlertColumnBatch {
    target: Arc<str>,
    len: usize,
    /// Fixed system-field columns, in `to_data_record` order. `wfx_id` /
    /// `entity_id` / `fired_at` are per-row owned strings — stored as `String`
    /// so the columnar direct path moves them in with zero extra allocation
    /// (an `Arc` would pay a fresh allocation + memcpy per row for a value
    /// that is never shared). The remaining six are plan/batch constants and
    /// stay `Arc<str>` (refcount-shared).
    wfx_id: Vec<String>,
    rule_name: Vec<Arc<str>>,
    score: Vec<f64>,
    entity_type: Vec<Arc<str>>,
    entity_id: Vec<String>,
    origin: Vec<Arc<str>>,
    close_reason: Vec<Arc<str>>,
    fired_at: Vec<String>,
    emit_time: Vec<Arc<str>>,
    summary: Vec<Arc<str>>,
    /// Yield columns (layout follows the first appended record).
    yield_cols: Vec<YieldCol>,
}

struct YieldCol {
    name: Arc<str>,
    metas: Vec<DataType>,
    values: Vec<ModelValue>,
    /// Batch-constant default cell (e.g. `alert_type = "q1_passthrough"`):
    /// when set, `fill_row_gaps` fills missing cells with a clone of this
    /// value instead of `(Ignore, Null)` — the columnar fast path registers
    /// literal yield fields once per batch and skips their per-row staging.
    const_value: Option<(DataType, ModelValue)>,
}

impl AlertColumnBatch {
    pub fn target(&self) -> &Arc<str> {
        &self.target
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Row view: reconstruct one `DataRecord` per row, field-identical to
    /// `OutputRecord::to_data_record` output (used by non-columnar sinks).
    pub fn iter_data_records(&self) -> impl Iterator<Item = CoreResult<DataRecord>> + '_ {
        (0..self.len).map(|row| {
            let mut record = DataRecord::default();
            record.push(FieldStorage::from_owned(Field::new(
                DataType::Chars,
                WFU_ID,
                ModelValue::from(self.wfx_id[row].as_str()),
            )));
            record.push(FieldStorage::from_owned(Field::new(
                DataType::Chars,
                WFU_RULE_NAME,
                ModelValue::from(self.rule_name[row].as_ref()),
            )));
            record.push(FieldStorage::from_owned(Field::new(
                DataType::Float,
                WFU_SCORE,
                ModelValue::from(self.score[row]),
            )));
            record.push(FieldStorage::from_owned(Field::new(
                DataType::Chars,
                WFU_ENTITY_TYPE,
                ModelValue::from(self.entity_type[row].as_ref()),
            )));
            record.push(FieldStorage::from_owned(Field::new(
                DataType::Chars,
                WFU_ENTITY_ID,
                ModelValue::from(self.entity_id[row].as_str()),
            )));
            record.push(FieldStorage::from_owned(Field::new(
                DataType::Chars,
                WFU_ORIGIN,
                ModelValue::from(self.origin[row].as_ref()),
            )));
            record.push(FieldStorage::from_owned(Field::new(
                DataType::Chars,
                WFU_CLOSE_REASON,
                ModelValue::from(self.close_reason[row].as_ref()),
            )));
            record.push(FieldStorage::from_owned(Field::new(
                DataType::Chars,
                WFU_FIRED_AT,
                ModelValue::from(self.fired_at[row].as_str()),
            )));
            record.push(FieldStorage::from_owned(Field::new(
                DataType::Chars,
                WFU_EMIT_TIME,
                ModelValue::from(self.emit_time[row].as_ref()),
            )));
            record.push(FieldStorage::from_owned(Field::new(
                DataType::Chars,
                WFU_SUMMARY,
                ModelValue::from(self.summary[row].as_ref()),
            )));
            for col in &self.yield_cols {
                if row < col.values.len() {
                    record.push(FieldStorage::from_owned(Field::new(
                        col.metas[row].clone(),
                        col.name.as_ref(),
                        col.values[row].clone(),
                    )));
                } else {
                    // Sparse cell (column appeared after this row).
                    record.push(FieldStorage::from_owned(Field::new(
                        DataType::Ignore,
                        col.name.as_ref(),
                        ModelValue::Null,
                    )));
                }
            }
            Ok(record)
        })
    }
}

/// System-field values for one `on each` row on the direct-write path
/// ([`AlertColumnBuilder::commit_each_row`]). The `&Arc<str>` borrows are
/// plan constants (`OutputStatic`) or batch-shared (`emit_time`); the owned
/// `String`s are the per-record values whose `Arc` conversion happens once
/// at commit (instead of a `String` build followed by a second copy, as the
/// record-based path paid).
pub struct EachRowCells<'a> {
    pub wfx_id: String,
    pub score: f64,
    pub entity_id: String,
    pub fired_at: String,
    pub rule_name: &'a Arc<str>,
    pub entity_type: &'a Arc<str>,
    pub origin: &'a Arc<str>,
    pub close_reason: &'a Arc<str>,
    pub emit_time: &'a Arc<str>,
    pub summary: &'a Arc<str>,
}

/// Accumulates records into columns for one yield target; `finish()` produces
/// the immutable batch handed to the sink channel.
pub struct AlertColumnBuilder {
    target: Arc<str>,
    len: usize,
    wfx_id: Vec<String>,
    rule_name: Vec<Arc<str>>,
    score: Vec<f64>,
    entity_type: Vec<Arc<str>>,
    entity_id: Vec<String>,
    origin: Vec<Arc<str>>,
    close_reason: Vec<Arc<str>>,
    fired_at: Vec<String>,
    emit_time: Vec<Arc<str>>,
    summary: Vec<Arc<str>>,
    yield_cols: Vec<YieldCol>,
    /// Fast-path layout cache: the yield field name sequence of the last
    /// appended record, mapped to `(column index, resolved FieldType)`. A
    /// rule's yield layout is plan-constant and the name `Arc`s are clones
    /// of the same plan slots, so pointer equality matches without string
    /// comparison. The reserved-prefix / duplicate checks run on the slow
    /// path only: their outcome depends solely on the name sequence, which
    /// the pointer match proves identical to the already-validated one.
    layout_cache: Vec<(Arc<str>, usize, Option<wf_lang::FieldType>)>,
    /// Scratch buffer for the fallible yield-conversion pass, reused across
    /// appends (one allocation per builder instead of per record).
    scratch: Vec<(usize, DataType, ModelValue)>,
    /// Staging buffer for the C2 direct-write path (`begin_row` /
    /// `stage_yield_cell` / `commit_each_row`): converted yield cells of the
    /// row currently being built. Kept separate from `scratch` so the
    /// record-based `append_record` path stays independent.
    staged: Vec<(usize, DataType, ModelValue)>,
}
impl AlertColumnBuilder {
    pub fn new(target: Arc<str>) -> Self {
        Self {
            target,
            len: 0,
            wfx_id: Vec::new(),
            rule_name: Vec::new(),
            score: Vec::new(),
            entity_type: Vec::new(),
            entity_id: Vec::new(),
            origin: Vec::new(),
            close_reason: Vec::new(),
            fired_at: Vec::new(),
            emit_time: Vec::new(),
            summary: Vec::new(),
            yield_cols: Vec::new(),
            layout_cache: Vec::new(),
            scratch: Vec::new(),
            staged: Vec::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Pre-size every column for `additional` more rows (system columns and
    /// the yield columns created so far). The batched on-each direct path
    /// calls this once per event-batch segment so the per-row commits stop
    /// paying amortized vector growth.
    pub fn reserve_rows(&mut self, additional: usize) {
        self.wfx_id.reserve(additional);
        self.rule_name.reserve(additional);
        self.score.reserve(additional);
        self.entity_type.reserve(additional);
        self.entity_id.reserve(additional);
        self.origin.reserve(additional);
        self.close_reason.reserve(additional);
        self.fired_at.reserve(additional);
        self.emit_time.reserve(additional);
        self.summary.reserve(additional);
        for col in &mut self.yield_cols {
            col.metas.reserve(additional);
            col.values.reserve(additional);
        }
    }

    /// Append one record's exported fields to the columns. Applies exactly
    /// the validation and conversion of `to_data_record`; on error nothing
    /// is appended (all fallible work happens before any column push).
    pub fn append_record(&mut self, record: &OutputRecord) -> CoreResult<()> {
        // Fallible yield conversion first (order: reserved prefix, duplicate,
        // typed/untyped export — identical to to_data_record). Uses the
        // reusable scratch buffer; nothing is appended on the error paths.
        let mut scratch = std::mem::take(&mut self.scratch);
        scratch.clear();
        let converted = self.convert_yield(record, &mut scratch);
        if let Err(e) = converted {
            self.scratch = scratch;
            return Err(e);
        }

        /// Infallible column pushes (system fields, then yield cells).
        self.wfx_id.push(record.wfx_id.clone());
        self.rule_name.push(Arc::clone(&record.rule_name));
        self.score.push(record.score);
        self.entity_type.push(Arc::clone(&record.entity_type));
        self.entity_id.push(record.entity_id.clone());
        self.origin.push(Arc::from(record.origin.as_str()));
        self.close_reason.push(Arc::from(
            record
                .origin
                .close_reason()
                .map_or("", |reason| reason.as_str()),
        ));
        self.fired_at.push(record.fired_at.clone());
        self.emit_time.push(Arc::clone(&record.emit_time));
        self.summary.push(Arc::clone(&record.summary));
        for (col_idx, meta, value) in scratch.drain(..) {
            let col = &mut self.yield_cols[col_idx];
            col.metas.push(meta);
            col.values.push(value);
        }
        // Optional fields omitted by this record leave their columns one
        // cell short — backfill a gap cell so columns stay row-aligned.
        self.fill_row_gaps();
        self.len += 1;
        self.scratch = scratch;
        Ok(())
    }

    /// Fallible half of [`Self::append_record`]: validates and converts the
    /// record's yield fields into `scratch` as `(column index, meta, value)`,
    /// creating (and backfilling) yield columns on demand.
    fn convert_yield(
        &mut self,
        record: &OutputRecord,
        scratch: &mut Vec<(usize, DataType, ModelValue)>,
    ) -> CoreResult<()> {
        scratch.reserve(record.yield_fields.len());
        // Fast path: same yield layout as the previous record (plan-constant
        // per rule; names are Arc clones of the same plan slots, so pointer
        // equality matches without touching the string bytes).
        let mut fast_path = self.layout_cache.len() == record.yield_fields.len();
        if fast_path {
            for (cached, (name, _)) in self.layout_cache.iter().zip(record.yield_fields.iter()) {
                if !Arc::ptr_eq(&cached.0, name) {
                    fast_path = false;
                    break;
                }
            }
        }
        if fast_path {
            for ((_, col_idx, field_type), (_, value)) in
                self.layout_cache.iter().zip(record.yield_fields.iter())
            {
                let (meta, model_value) =
                    alert_types::export_yield_value(value, field_type.as_ref())?;
                scratch.push((*col_idx, meta, model_value));
            }
            return Ok(());
        }
        // Slow path (first append / layout drift): resolve each column by
        // name, then refresh the layout cache.
        self.layout_cache.clear();
        for (idx, (name, value)) in record.yield_fields.iter().enumerate() {
            if name.starts_with(WFU_PREFIX) {
                return CoreReason::DataFormat
                    .to_err()
                    .with_detail(format!(
                        "yield field {name:?} uses reserved prefix {WFU_PREFIX}"
                    ))
                    .err();
            }
            if record.yield_fields[..idx]
                .iter()
                .any(|(prev, _)| prev == name)
            {
                return CoreReason::DataFormat
                    .to_err()
                    .with_detail(format!("duplicate exported field {name:?}"))
                    .err();
            }
            let field_type = record
                .yield_field_types
                .iter()
                .find_map(|(field_name, field_type)| (field_name == name).then_some(field_type));
            let (meta, model_value) = alert_types::export_yield_value(value, field_type)?;
            // Resolve the column index (linear scan; yield layouts are tiny
            // and stable per rule).
            let col_idx = match self.yield_cols.iter().position(|c| c.name == *name) {
                Some(i) => i,
                None => {
                    self.yield_cols.push(YieldCol {
                        name: Arc::clone(name),
                        metas: Vec::new(),
                        values: Vec::new(),
                        const_value: None,
                    });
                    let col = self.yield_cols.last_mut().unwrap();
                    // Backfill rows that predate this column so every column
                    // stays row-aligned (sparse cells read back as Ignore).
                    for _ in 0..self.len {
                        col.metas.push(DataType::Ignore);
                        col.values.push(ModelValue::Null);
                    }
                    self.yield_cols.len() - 1
                }
            };
            self.layout_cache
                .push((Arc::clone(name), col_idx, field_type.cloned()));
            scratch.push((col_idx, meta, model_value));
        }
        Ok(())
    }

    // -- C2 direct-write staging API --------------------------------------

    /// Batch-level column registration for the columnar direct path: create
    /// the yield column up front (once per batch) and record a
    /// batch-constant cell when the field is a literal (`const_value = Some`).
    /// Literal fields then skip per-row staging — `fill_row_gaps` fills their
    /// cells with the constant. Non-constant (field) fields register with
    /// `None` and get a layout-cache entry (in register order — the same
    /// order the row loop stages them), so per-row `stage_yield_cell` hits
    /// the pointer-equality fast path.
    pub(crate) fn register_yield_column(
        &mut self,
        name: &Arc<str>,
        const_value: Option<(DataType, ModelValue)>,
    ) -> CoreResult<()> {
        if name.starts_with(WFU_PREFIX) {
            return CoreReason::DataFormat
                .to_err()
                .with_detail(format!(
                    "yield field {name:?} uses reserved prefix {WFU_PREFIX}"
                ))
                .err();
        }
        let is_const = const_value.is_some();
        let col_idx = match self.yield_cols.iter().position(|c| c.name == *name) {
            Some(i) => i,
            None => {
                self.yield_cols.push(YieldCol {
                    name: Arc::clone(name),
                    metas: Vec::new(),
                    values: Vec::new(),
                    const_value,
                });
                self.yield_cols.len() - 1
            }
        };
        // Constant fields are never staged per row, so they get no layout
        // cache entry (entries are indexed by staged position). Only
        // non-constant fields push one.
        if !is_const {
            self.layout_cache.push((Arc::clone(name), col_idx, None));
        }
        Ok(())
    }

    /// Begin staging a new row for the on-each direct path: clears the cells
    /// staged for the previous row (e.g. after a mid-row error).
    pub fn begin_row(&mut self) {
        self.staged.clear();
    }

    /// Drain the cells staged for the current row (batch-write path; L3).
    /// `stage_yield_cell`/`stage_yield_cell_f64` append here; the batched
    /// commit collects them via this instead of `commit_each_row` draining
    /// them per row.
    pub(crate) fn take_staged(&mut self) -> Vec<(usize, DataType, ModelValue)> {
        std::mem::take(&mut self.staged)
    }

    /// Stage one yield cell for the row being built (fallible: reserved-name
    /// validation, duplicate detection and typed conversion — identical rules
    /// to `append_record`). `field_type` is the plan-side spec of this field
    /// (from `output_static().yield_specs`), so no type lookup happens here.
    ///
    /// Column resolution uses the layout cache: the plan field names are
    /// `Arc` clones of the same slots on every record, so the common case is
    /// one pointer comparison per field. The slow path resolves by name and
    /// refreshes the cache entry at this row position.
    pub fn stage_yield_cell(
        &mut self,
        name: &Arc<str>,
        field_type: Option<&FieldType>,
        value: &Value,
    ) -> CoreResult<()> {
        let pos = self.staged.len();
        // Fast path: same plan slot as the last row at this position.
        if let Some((cached_name, col_idx, _cached_ft)) = self.layout_cache.get(pos)
            && Arc::ptr_eq(cached_name, name)
        {
            let (meta, model_value) = alert_types::export_yield_value(value, field_type)?;
            self.staged.push((*col_idx, meta, model_value));
            return Ok(());
        }
        // Slow path: validate, resolve the column, refresh the cache entry.
        if name.starts_with(WFU_PREFIX) {
            return CoreReason::DataFormat
                .to_err()
                .with_detail(format!(
                    "yield field {name:?} uses reserved prefix {WFU_PREFIX}"
                ))
                .err();
        }
        let col_idx = match self.yield_cols.iter().position(|c| c.name == *name) {
            Some(i) => i,
            None => {
                self.yield_cols.push(YieldCol {
                    name: Arc::clone(name),
                    metas: Vec::new(),
                    values: Vec::new(),
                    const_value: None,
                });
                let col = self.yield_cols.last_mut().unwrap();
                // Backfill rows that predate this column so every column
                // stays row-aligned (sparse cells read back as Ignore).
                for _ in 0..self.len {
                    col.metas.push(DataType::Ignore);
                    col.values.push(ModelValue::Null);
                }
                self.yield_cols.len() - 1
            }
        };
        // Duplicate within this row: equal names resolve to the same column,
        // so a repeated column index among the staged cells is the duplicate.
        if self.staged.iter().any(|(ci, _, _)| *ci == col_idx) {
            return CoreReason::DataFormat
                .to_err()
                .with_detail(format!("duplicate exported field {name:?}"))
                .err();
        }
        let (meta, model_value) = alert_types::export_yield_value(value, field_type)?;
        let cache_entry = (Arc::clone(name), col_idx, field_type.cloned());
        if pos < self.layout_cache.len() {
            self.layout_cache[pos] = cache_entry;
        } else {
            self.layout_cache.push(cache_entry);
        }
        self.staged.push((col_idx, meta, model_value));
        Ok(())
    }

    /// Percent of [`Self::stage_yield_cell`] for the numeric fast lane (Q1
    /// entity==yield `id=b.auction`): stages a raw `f64` without constructing a
    /// [`Value`] per cell. Column resolution / duplicate logic is identical;
    /// export uses [`export_yield_f64`] (byte-identical to the `Value` path,
    /// with a fallback for non-numeric target types).
    pub fn stage_yield_cell_f64(
        &mut self,
        name: &Arc<str>,
        field_type: Option<&FieldType>,
        n: f64,
    ) -> CoreResult<()> {
        let pos = self.staged.len();
        if let Some((cached_name, col_idx, _cached_ft)) = self.layout_cache.get(pos)
            && Arc::ptr_eq(cached_name, name)
        {
            let (meta, model_value) = crate::alert::export_yield_f64(n, field_type)?;
            self.staged.push((*col_idx, meta, model_value));
            return Ok(());
        }
        if name.starts_with(WFU_PREFIX) {
            return CoreReason::DataFormat
                .to_err()
                .with_detail(format!(
                    "yield field {name:?} uses reserved prefix {WFU_PREFIX}"
                ))
                .err();
        }
        let col_idx = match self.yield_cols.iter().position(|c| c.name == *name) {
            Some(i) => i,
            None => {
                self.yield_cols.push(YieldCol {
                    name: Arc::clone(name),
                    metas: Vec::new(),
                    values: Vec::new(),
                    const_value: None,
                });
                let col = self.yield_cols.last_mut().unwrap();
                for _ in 0..self.len {
                    col.metas.push(DataType::Ignore);
                    col.values.push(ModelValue::Null);
                }
                self.yield_cols.len() - 1
            }
        };
        if self.staged.iter().any(|(ci, _, _)| *ci == col_idx) {
            return CoreReason::DataFormat
                .to_err()
                .with_detail(format!("duplicate exported field {name:?}"))
                .err();
        }
        let (meta, model_value) = crate::alert::export_yield_f64(n, field_type)?;
        let cache_entry = (Arc::clone(name), col_idx, field_type.cloned());
        if pos < self.layout_cache.len() {
            self.layout_cache[pos] = cache_entry;
        } else {
            self.layout_cache.push(cache_entry);
        }
        self.staged.push((col_idx, meta, model_value));
        Ok(())
    }

    /// Commit the staged row with the system fields (infallible column
    /// pushes; all fallible work happened in `stage_yield_cell`).
    ///
    /// `origin` / `close_reason` are plan constants on the `on each` path
    /// (`"event"` / `""`) — pass the precomputed `Arc`s from `OutputStatic`
    /// so no per-record string copy happens for them.
    pub fn commit_each_row(&mut self, cells: EachRowCells<'_>) {
        self.wfx_id.push(cells.wfx_id);
        self.rule_name.push(Arc::clone(cells.rule_name));
        self.score.push(cells.score);
        self.entity_type.push(Arc::clone(cells.entity_type));
        self.entity_id.push(cells.entity_id);
        self.origin.push(Arc::clone(cells.origin));
        self.close_reason.push(Arc::clone(cells.close_reason));
        self.fired_at.push(cells.fired_at);
        self.emit_time.push(Arc::clone(cells.emit_time));
        self.summary.push(Arc::clone(cells.summary));
        for (col_idx, meta, value) in self.staged.drain(..) {
            let col = &mut self.yield_cols[col_idx];
            col.metas.push(meta);
            col.values.push(value);
        }
        self.fill_row_gaps();
        self.len += 1;
    }

    /// L3 batched commit: append a whole segment of rows **column-major**, so
    /// the per-row fill that dominated Q1's on-each output (10 `Vec::push` +
    /// a `fill_row_gaps` scan per row) becomes 10 bulk `extend`s + one block-
    /// level yield fill. Byte-identical to committing each row via
    /// [`Self::commit_each_row`].
    ///
    /// `staged_rows` is one entry per row, in order, each a slice of that
    /// row's staged yield cells `(col_idx, meta, value)` (same layout
    /// `stage_yield_cell`/`stage_yield_cell_f64` produce).
    #[allow(clippy::too_many_arguments)]
    pub fn commit_each_rows_batch(
        &mut self,
        wfx_id: &[String],
        score: &[f64],
        entity_id: &[String],
        fired_at: &[String],
        rule_name: &Arc<str>,
        entity_type: &Arc<str>,
        origin: &Arc<str>,
        close_reason: &Arc<str>,
        emit_time: &Arc<str>,
        summary: &Arc<str>,
        staged_rows: &[Vec<(usize, DataType, ModelValue)>],
    ) {
        let n = wfx_id.len();
        debug_assert_eq!(score.len(), n);
        debug_assert_eq!(entity_id.len(), n);
        debug_assert_eq!(fired_at.len(), n);
        debug_assert_eq!(staged_rows.len(), n);
        // Reserve once for the whole block (amortizes the per-row growth).
        self.wfx_id.reserve(n);
        self.score.reserve(n);
        self.entity_id.reserve(n);
        self.fired_at.reserve(n);
        self.rule_name.reserve(n);
        self.entity_type.reserve(n);
        self.origin.reserve(n);
        self.close_reason.reserve(n);
        self.emit_time.reserve(n);
        self.summary.reserve(n);
        // Bulk system columns.
        self.wfx_id.extend_from_slice(wfx_id);
        self.score.extend_from_slice(score);
        self.entity_id.extend_from_slice(entity_id);
        self.fired_at.extend_from_slice(fired_at);
        // Plan-constant columns: same `Arc` every row.
        self.rule_name
            .extend(std::iter::repeat(Arc::clone(rule_name)).take(n));
        self.entity_type
            .extend(std::iter::repeat(Arc::clone(entity_type)).take(n));
        self.origin
            .extend(std::iter::repeat(Arc::clone(origin)).take(n));
        self.close_reason
            .extend(std::iter::repeat(Arc::clone(close_reason)).take(n));
        self.emit_time
            .extend(std::iter::repeat(Arc::clone(emit_time)).take(n));
        self.summary
            .extend(std::iter::repeat(Arc::clone(summary)).take(n));
        // Yield cells, interleaved with per-row gap fills. `fill_row_gaps`
        // (per-row path) pushes one fill cell for every column that received no
        // staged cell that row — so a field that is present in rows {0, 2} but
        // absent in row 1 must land as [real0, fill, real2], NOT be topped up
        // to the block tail as a trailing run. To stay byte-identical we walk
        // the rows in order and, before pushing each real cell, pad that column
        // with the fill(s) for the rows since its previous real cell. Columns
        // with no staged cells (literal/idle) get the block-level top-up below.
        let target = self.len + n;
        for (row_idx, row) in staged_rows.iter().enumerate() {
            let row_pos = self.len + row_idx;
            for (col_idx, meta, value) in row {
                let col = &mut self.yield_cols[*col_idx];
                // Pad columns whose last real cell predates this row (field was
                // absent for intervening rows) — same fill value as
                // `fill_row_gaps` (batched constant, else Ignore/Null).
                while col.values.len() < row_pos {
                    match &col.const_value {
                        Some((meta, value)) => {
                            col.metas.push(meta.clone());
                            col.values.push(value.clone());
                        }
                        None => {
                            col.metas.push(DataType::Ignore);
                            col.values.push(ModelValue::Null);
                        }
                    }
                }
                col.metas.push(meta.clone());
                col.values.push(value.clone());
            }
        }
        // Top up columns that never got a staged cell this block (literal /
        // idle columns) to the block target — each fill contributes the column
        // constant, else Ignore/Null (byte-identical to their per-row fills).
        for col in &mut self.yield_cols {
            while col.values.len() < target {
                match &col.const_value {
                    Some((meta, value)) => {
                        col.metas.push(meta.clone());
                        col.values.push(value.clone());
                    }
                    None => {
                        col.metas.push(DataType::Ignore);
                        col.values.push(ModelValue::Null);
                    }
                }
            }
        }
        debug_assert!(self.yield_cols.iter().all(|c| c.values.len() == target));
        self.len += n;
    }

    /// Fill gap cells for yield columns that received no staged cell this
    /// row (optional input field missing → field omitted, wp-labs#62). Every
    /// column must stay row-aligned; gap cells read back as `(Ignore, Null)`
    /// unless the column was registered with a batch-constant value, in which
    /// case the constant is filled instead (literal yield fields on the
    /// columnar fast path skip per-row staging entirely).
    ///
    /// This also repairs the record-based `append_record` path, which had
    /// the same latent misalignment when a later record omitted a field an
    /// earlier record had yielded (caught by the C2 equivalence test).
    fn fill_row_gaps(&mut self) {
        for col in &mut self.yield_cols {
            if col.values.len() == self.len {
                match &col.const_value {
                    Some((meta, value)) => {
                        col.metas.push(meta.clone());
                        col.values.push(value.clone());
                    }
                    None => {
                        col.metas.push(DataType::Ignore);
                        col.values.push(ModelValue::Null);
                    }
                }
            }
        }
        debug_assert!(
            self.yield_cols
                .iter()
                .all(|col| col.values.len() == self.len + 1)
        );
    }

    /// Seal the builder into an immutable batch. The builder is left empty
    /// (capacities are not preserved; see flush call sites for reuse) and its
    /// layout cache is dropped — the yield columns moved out invalidate the
    /// cached column indices, so a reused builder must re-resolve them.
    pub fn finish(&mut self) -> AlertColumnBatch {
        self.layout_cache.clear();
        AlertColumnBatch {
            target: Arc::clone(&self.target),
            len: std::mem::take(&mut self.len),
            wfx_id: std::mem::take(&mut self.wfx_id),
            rule_name: std::mem::take(&mut self.rule_name),
            score: std::mem::take(&mut self.score),
            entity_type: std::mem::take(&mut self.entity_type),
            entity_id: std::mem::take(&mut self.entity_id),
            origin: std::mem::take(&mut self.origin),
            close_reason: std::mem::take(&mut self.close_reason),
            fired_at: std::mem::take(&mut self.fired_at),
            emit_time: std::mem::take(&mut self.emit_time),
            summary: std::mem::take(&mut self.summary),
            yield_cols: std::mem::take(&mut self.yield_cols),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::alert::AlertOrigin;
    use crate::match_engine::Value;
    use wf_lang::FieldType;

    fn sample_record(yield_fields: Vec<(Arc<str>, Value)>) -> OutputRecord {
        OutputRecord {
            wfx_id: "a1b2c3d4e5f60718".to_string(),
            rule_name: Arc::from("q1_pass"),
            score: 42.5,
            entity_type: Arc::from("ip"),
            entity_id: "10.0.0.1".to_string(),
            origin: AlertOrigin::Event,
            fired_at: "2026-08-16T00:00:00Z".to_string(),
            emit_time: Arc::from("2026-08-16T00:00:01Z"),
            matched_rows: Vec::new(),
            summary: Arc::from("summary text"),
            yield_target: Arc::from("alerts"),
            yield_fields,
            yield_field_types: Arc::from(vec![
                (
                    Arc::from("auction_id"),
                    FieldType::Base(wf_lang::BaseType::Float),
                ),
                (
                    Arc::from("price"),
                    FieldType::Base(wf_lang::BaseType::Float),
                ),
            ]),
            event_time_nanos: 0,
            machine_id: String::new(),
            scope_key: Arc::from(""),
        }
    }

    fn assert_records_equal(a: &DataRecord, b: &DataRecord) {
        assert_eq!(a.items.len(), b.items.len(), "field count mismatch");
        for (fa, fb) in a.items.iter().zip(b.items.iter()) {
            assert_eq!(fa.get_name(), fb.get_name());
            assert_eq!(fa.get_meta(), fb.get_meta());
            match (fa.get_value(), fb.get_value()) {
                (ModelValue::Float(x), ModelValue::Float(y)) => assert_eq!(x, y),
                (ModelValue::Digit(x), ModelValue::Digit(y)) => assert_eq!(x, y),
                _ => assert_eq!(fa.get_value(), fb.get_value()),
            }
        }
    }

    /// Assert the field `name` was filled as a **mid-segment gap** at `row`
    /// (meta `Ignore`, null) — i.e. it was absent that row rather than a
    /// trailing fill. Guards the sparse-in-the-middle placement fix for the
    /// batched fill (the pre-fix block-level top-up put such fills at the tail,
    /// which is not byte-identical to the per-row path).
    fn assert_mid_gap_at(builder: &AlertColumnBuilder, name: &Arc<str>, row: usize) {
        let col = builder.yield_cols.iter().find(|c| c.name == *name).unwrap();
        assert_eq!(
            col.metas[row],
            DataType::Ignore,
            "expected mid-segment fill gap for {name:?} at row {row}"
        );
        assert_eq!(col.values[row], ModelValue::Null);
    }

    #[test]
    fn column_batch_row_view_matches_to_data_record() {
        let records = vec![
            sample_record(vec![
                (Arc::from("auction_id"), Value::Number(1000.0)),
                (Arc::from("price"), Value::Number(99.5)),
            ]),
            sample_record(vec![
                (Arc::from("auction_id"), Value::Number(1001.0)),
                (Arc::from("price"), Value::Number(79.25)),
            ]),
            sample_record(vec![
                (Arc::from("auction_id"), Value::Number(1002.0)),
                (Arc::from("price"), Value::Number(10.0)),
            ]),
        ];
        let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
        for record in &records {
            builder.append_record(record).unwrap();
        }
        let batch = builder.finish();
        assert_eq!(batch.len(), 3);
        for (row, record) in records.iter().enumerate() {
            let via_columns = batch.iter_data_records().nth(row).unwrap().unwrap();
            let via_rows = record.to_data_record().unwrap();
            assert_records_equal(&via_columns, &via_rows);
        }
    }

    #[test]
    fn commit_each_rows_batch_matches_repeated_commit_each_row() {
        // L3 batched commit must produce byte-identical output to committing
        // the same rows one-by-one via `commit_each_row` (constant + field
        // yield columns, block-level gap fill, plan-constant system cols).
        use crate::alert::types::export_yield_value;
        use wf_lang::BaseType;
        let target = Arc::from("alerts");
        let ft_chars = FieldType::Base(BaseType::Chars);
        let ft_float = FieldType::Base(BaseType::Float);
        let rule_name = Arc::from("q1_pass");
        let entity_type = Arc::from("digit");
        let origin = Arc::from("event");
        let close_reason = Arc::from("");
        let emit_time = Arc::from("2026-08-16T00:00:01Z");
        let summary = Arc::from("summary");
        let n = 3usize;
        // `price` present in rows {0, 2} but absent in row 1 → the batched
        // fill must land [real0, fill, real2], not a trailing run; `idle` is a
        // registered column that never gets staged (idle/literal analog).
        let price_present = [true, false, true];

        // ---- row-by-row builder ----
        let mut via_row = AlertColumnBuilder::new(Arc::clone(&target));
        via_row
            .register_yield_column(
                &Arc::from("alert_type"),
                Some(export_yield_value(&Value::Str("q1".into()), Some(&ft_chars)).unwrap()),
            )
            .unwrap();
        via_row
            .register_yield_column(&Arc::from("auction_id"), None)
            .unwrap();
        via_row
            .register_yield_column(&Arc::from("price"), None)
            .unwrap();
        via_row
            .register_yield_column(&Arc::from("idle"), None)
            .unwrap();
        for i in 0..n {
            via_row.begin_row();
            via_row
                .stage_yield_cell(
                    &Arc::from("auction_id"),
                    Some(&ft_float),
                    &Value::Number((1000 + i) as f64),
                )
                .unwrap();
            if price_present[i] {
                via_row
                    .stage_yield_cell(
                        &Arc::from("price"),
                        Some(&ft_float),
                        &Value::Number(9.5 + i as f64 * 10.0),
                    )
                    .unwrap();
            }
            via_row.commit_each_row(EachRowCells {
                wfx_id: format!("id{i}"),
                score: 42.0 + i as f64,
                entity_id: format!("10.0.0.{}", i + 1),
                fired_at: format!("ts{i}"),
                rule_name: &rule_name,
                entity_type: &entity_type,
                origin: &origin,
                close_reason: &close_reason,
                emit_time: &emit_time,
                summary: &summary,
            });
        }
        // Sparse-mid-segment `price` placement: [real0, fill, real2].
        assert_mid_gap_at(&via_row, &Arc::from("price"), 1);

        // ---- batched builder ----
        let mut via_batch = AlertColumnBuilder::new(Arc::clone(&target));
        via_batch
            .register_yield_column(
                &Arc::from("alert_type"),
                Some(export_yield_value(&Value::Str("q1".into()), Some(&ft_chars)).unwrap()),
            )
            .unwrap();
        via_batch
            .register_yield_column(&Arc::from("auction_id"), None)
            .unwrap();
        via_batch
            .register_yield_column(&Arc::from("price"), None)
            .unwrap();
        via_batch
            .register_yield_column(&Arc::from("idle"), None)
            .unwrap();
        // Pre-export the field cells for the batch path (same export the
        // per-row `stage_yield_cell` performs), column-major per row.
        let auction_col = via_batch
            .yield_cols
            .iter()
            .position(|c| c.name.as_ref() == "auction_id")
            .unwrap();
        let price_col = via_batch
            .yield_cols
            .iter()
            .position(|c| c.name.as_ref() == "price")
            .unwrap();
        let wfx: Vec<String> = (0..n).map(|i| format!("id{i}")).collect();
        let scores: Vec<f64> = (0..n).map(|i| 42.0 + i as f64).collect();
        let eids: Vec<String> = (0..n).map(|i| format!("10.0.0.{}", i + 1)).collect();
        let fats: Vec<String> = (0..n).map(|i| format!("ts{i}")).collect();
        let mut staged_rows = Vec::with_capacity(n);
        for i in 0..n {
            let a = export_yield_value(&Value::Number((1000 + i) as f64), Some(&ft_float)).unwrap();
            let mut row_cells = vec![(auction_col, a.0, a.1)];
            if price_present[i] {
                let p = export_yield_value(&Value::Number(9.5 + i as f64 * 10.0), Some(&ft_float))
                    .unwrap();
                row_cells.push((price_col, p.0, p.1));
            }
            staged_rows.push(row_cells);
        }
        via_batch.commit_each_rows_batch(
            &wfx,
            &scores,
            &eids,
            &fats,
            &rule_name,
            &entity_type,
            &origin,
            &close_reason,
            &emit_time,
            &summary,
            &staged_rows,
        );
        assert_mid_gap_at(&via_batch, &Arc::from("price"), 1);

        let batch_row = via_row.finish();
        let batch_col = via_batch.finish();
        assert_eq!(batch_row.len(), batch_col.len());
        assert_eq!(batch_row.len(), n);
        for i in 0..batch_row.len() {
            let a = batch_row.iter_data_records().nth(i).unwrap().unwrap();
            let b = batch_col.iter_data_records().nth(i).unwrap().unwrap();
            assert_records_equal(&a, &b);
        }
    }

    #[test]
    fn commit_each_rows_batch_dense_all_present() {
        // Regression guard for the L3 default case (Q1: every yield field
        // present every row, no mid-segment gaps): batched commit stays
        // byte-identical to repeated `commit_each_row`.
        use crate::alert::types::export_yield_value;
        use wf_lang::BaseType;
        let target = Arc::from("alerts");
        let ft_chars = FieldType::Base(BaseType::Chars);
        let rule_name = Arc::from("q1_pass");
        let entity_type = Arc::from("digit");
        let origin = Arc::from("event");
        let close_reason = Arc::from("");
        let emit_time = Arc::from("2026-08-16T00:00:01Z");
        let summary = Arc::from("summary");
        let n = 3usize;

        let mut via_row = AlertColumnBuilder::new(Arc::clone(&target));
        via_row
            .register_yield_column(&Arc::from("alert_type"), None)
            .unwrap();
        for i in 0..n {
            via_row.begin_row();
            via_row
                .stage_yield_cell(
                    &Arc::from("alert_type"),
                    Some(&ft_chars),
                    &Value::Str(format!("type{i}").into()),
                )
                .unwrap();
            via_row.commit_each_row(EachRowCells {
                wfx_id: format!("id{i}"),
                score: 1.0 + i as f64,
                entity_id: format!("e{i}"),
                fired_at: format!("ts{i}"),
                rule_name: &rule_name,
                entity_type: &entity_type,
                origin: &origin,
                close_reason: &close_reason,
                emit_time: &emit_time,
                summary: &summary,
            });
        }

        let mut via_batch = AlertColumnBuilder::new(Arc::clone(&target));
        via_batch
            .register_yield_column(&Arc::from("alert_type"), None)
            .unwrap();
        let type_col = 0usize;
        let wfx: Vec<String> = (0..n).map(|i| format!("id{i}")).collect();
        let scores: Vec<f64> = (0..n).map(|i| 1.0 + i as f64).collect();
        let eids: Vec<String> = (0..n).map(|i| format!("e{i}")).collect();
        let fats: Vec<String> = (0..n).map(|i| format!("ts{i}")).collect();
        let staged_rows: Vec<Vec<(usize, DataType, ModelValue)>> = (0..n)
            .map(|i| {
                let t = export_yield_value(&Value::Str(format!("type{i}").into()), Some(&ft_chars))
                    .unwrap();
                vec![(type_col, t.0, t.1)]
            })
            .collect();
        via_batch.commit_each_rows_batch(
            &wfx,
            &scores,
            &eids,
            &fats,
            &rule_name,
            &entity_type,
            &origin,
            &close_reason,
            &emit_time,
            &summary,
            &staged_rows,
        );

        let batch_row = via_row.finish();
        let batch_col = via_batch.finish();
        assert_eq!(batch_row.len(), batch_col.len());
        for i in 0..batch_row.len() {
            let a = batch_row.iter_data_records().nth(i).unwrap().unwrap();
            let b = batch_col.iter_data_records().nth(i).unwrap().unwrap();
            assert_records_equal(&a, &b);
        }
    }

    #[test]
    fn append_rejects_reserved_prefix_and_duplicates() {
        let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));

        let bad_prefix = sample_record(vec![(Arc::from("__wfu_evil"), Value::Number(1.0))]);
        assert!(builder.append_record(&bad_prefix).is_err());
        assert_eq!(builder.len(), 0, "failed append must not touch columns");

        let dup = sample_record(vec![
            (Arc::from("price"), Value::Number(1.0)),
            (Arc::from("price"), Value::Number(2.0)),
        ]);
        assert!(builder.append_record(&dup).is_err());
        assert_eq!(builder.len(), 0);
    }

    #[test]
    fn sparse_yield_columns_read_back_as_ignore_null() {
        let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
        builder
            .append_record(&sample_record(vec![
                (Arc::from("auction_id"), Value::Number(1.0)),
                (Arc::from("price"), Value::Number(2.0)),
            ]))
            .unwrap();
        // A later record with an extra yield field extends the layout.
        builder
            .append_record(&sample_record(vec![
                (Arc::from("auction_id"), Value::Number(3.0)),
                (Arc::from("price"), Value::Number(4.0)),
                (Arc::from("extra"), Value::Str("x".into())),
            ]))
            .unwrap();
        let batch = builder.finish();
        let rows: Vec<_> = batch.iter_data_records().collect();
        let first = rows[0].as_ref().unwrap();
        let extra = first.field("extra").expect("sparse cell present");
        assert_eq!(extra.get_meta(), &DataType::Ignore);
    }

    #[test]
    fn finish_leaves_builder_empty() {
        let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
        builder
            .append_record(&sample_record(vec![(
                Arc::from("auction_id"),
                Value::Number(1.0),
            )]))
            .unwrap();
        let _ = builder.finish();
        assert!(builder.is_empty());
    }

    fn commit_staged(
        builder: &mut AlertColumnBuilder,
        wfx_id: &str,
        entity_id: &str,
        fired_at: &str,
    ) {
        builder.commit_each_row(EachRowCells {
            wfx_id: wfx_id.to_string(),
            score: 42.5,
            entity_id: entity_id.to_string(),
            fired_at: fired_at.to_string(),
            rule_name: &Arc::from("q1_pass"),
            entity_type: &Arc::from("ip"),
            origin: &Arc::from("event"),
            close_reason: &Arc::from(""),
            emit_time: &Arc::from("2026-08-16T00:00:01Z"),
            summary: &Arc::from("summary text"),
        });
    }

    #[test]
    fn staged_rows_match_record_appended_rows() {
        // Same three records through both paths must yield identical
        // DataRecord row views (system fields included).
        let rows_spec = [
            ("a1b2c3d4e5f60718", "10.0.0.1", "2026-08-16T00:00:00Z"),
            ("b2c3d4e5f60718a1", "10.0.0.2", "2026-08-16T00:00:01Z"),
            ("c3d4e5f60718a1b2", "10.0.0.3", "2026-08-16T00:00:02Z"),
        ];
        let values = [
            (Value::Number(1000.0), Value::Number(99.5)),
            (Value::Number(1001.0), Value::Number(79.25)),
            (Value::Number(1002.0), Value::Number(10.0)),
        ];

        // Record path: one OutputRecord per row, appended via append_record.
        let mut via_records = AlertColumnBuilder::new(Arc::from("alerts"));
        for ((wfx_id, entity_id, fired_at), vals) in rows_spec.iter().zip(values.iter()) {
            let mut record = sample_record(vec![
                (Arc::from("auction_id"), vals.0.clone()),
                (Arc::from("price"), vals.1.clone()),
            ]);
            record.wfx_id = wfx_id.to_string();
            record.entity_id = entity_id.to_string();
            record.fired_at = fired_at.to_string();
            via_records.append_record(&record).unwrap();
        }

        // Staging path (reuses one Arc per field name, like plan slots).
        let names: [Arc<str>; 2] = [Arc::from("auction_id"), Arc::from("price")];
        let ft = Some(FieldType::Base(wf_lang::BaseType::Float));
        let mut via_staging = AlertColumnBuilder::new(Arc::from("alerts"));
        for ((wfx_id, entity_id, fired_at), vals) in rows_spec.iter().zip(values.iter()) {
            via_staging.begin_row();
            via_staging
                .stage_yield_cell(&names[0], ft.as_ref(), &vals.0)
                .unwrap();
            via_staging
                .stage_yield_cell(&names[1], ft.as_ref(), &vals.1)
                .unwrap();
            commit_staged(&mut via_staging, wfx_id, entity_id, fired_at);
        }

        let record_batch = via_records.finish();
        let staged_batch = via_staging.finish();
        assert_eq!(record_batch.len(), staged_batch.len());
        for row in 0..record_batch.len() {
            let a = record_batch.iter_data_records().nth(row).unwrap().unwrap();
            let b = staged_batch.iter_data_records().nth(row).unwrap().unwrap();
            assert_records_equal(&a, &b);
        }
    }

    #[test]
    fn staged_optional_omission_creates_sparse_cells() {
        // Row 1 omits the middle field (optional input missing, #62): the
        // later column must backfill an Ignore/Null cell for that row.
        let names: [Arc<str>; 3] = [Arc::from("a"), Arc::from("b"), Arc::from("c")];
        let ft = Some(FieldType::Base(wf_lang::BaseType::Float));
        let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));

        // Row 0: a, c (b omitted).
        builder.begin_row();
        builder
            .stage_yield_cell(&names[0], ft.as_ref(), &Value::Number(1.0))
            .unwrap();
        builder
            .stage_yield_cell(&names[2], ft.as_ref(), &Value::Number(3.0))
            .unwrap();
        commit_staged(&mut builder, "id0", "e0", "t0");

        // Row 1: full a, b, c.
        builder.begin_row();
        builder
            .stage_yield_cell(&names[0], ft.as_ref(), &Value::Number(4.0))
            .unwrap();
        builder
            .stage_yield_cell(&names[1], ft.as_ref(), &Value::Number(5.0))
            .unwrap();
        builder
            .stage_yield_cell(&names[2], ft.as_ref(), &Value::Number(6.0))
            .unwrap();
        commit_staged(&mut builder, "id1", "e1", "t1");

        let batch = builder.finish();
        let rows: Vec<_> = batch.iter_data_records().collect();
        let row0 = rows[0].as_ref().unwrap();
        let b_cell = row0.field("b").expect("sparse cell present");
        assert_eq!(b_cell.get_meta(), &DataType::Ignore);
        let row1 = rows[1].as_ref().unwrap();
        match row1.field("b").unwrap().get_value() {
            ModelValue::Float(n) => assert_eq!(*n, 5.0),
            other => panic!("unexpected value for b: {other:?}"),
        }
    }

    #[test]
    fn stage_rejects_reserved_prefix_and_duplicates_and_keeps_row_clean() {
        let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
        let bad = Arc::from("__wfu_evil");
        builder.begin_row();
        assert!(
            builder
                .stage_yield_cell(&bad, None, &Value::Number(1.0))
                .is_err()
        );

        let a = Arc::from("dup");
        let a2 = Arc::from("dup");
        builder.begin_row();
        builder
            .stage_yield_cell(&a, None, &Value::Number(1.0))
            .unwrap();
        // Same name again (different Arc, equal string) → duplicate error.
        assert!(
            builder
                .stage_yield_cell(&a2, None, &Value::Number(2.0))
                .is_err()
        );
        assert_eq!(builder.len(), 0, "failed rows must not touch columns");
    }

    #[test]
    fn failed_staging_then_successful_row_is_consistent() {
        // A row that errors mid-staging leaves no partial state; the next
        // row commits cleanly.
        let n = Arc::from("x");
        let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
        builder.begin_row();
        builder
            .stage_yield_cell(&n, None, &Value::Number(1.0))
            .unwrap();
        let bad = Arc::from("__wfu_bad");
        assert!(
            builder
                .stage_yield_cell(&bad, None, &Value::Number(2.0))
                .is_err()
        );
        // begin_row clears the staged cells; commit must still be balanced.
        builder.begin_row();
        builder
            .stage_yield_cell(&n, None, &Value::Number(3.0))
            .unwrap();
        commit_staged(&mut builder, "id", "e", "t");
        let batch = builder.finish();
        assert_eq!(batch.len(), 1);
        let row = batch.iter_data_records().next().unwrap().unwrap();
        match row.field("x").unwrap().get_value() {
            ModelValue::Float(n) => assert_eq!(*n, 3.0),
            other => panic!("unexpected value for x: {other:?}"),
        }
    }
}
