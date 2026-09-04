//! `AlertColumnBuilder` 逐行装载与直写 staging 面（2026-09-04 自 column_batch.rs
//! 拆出；`#[path]` 兄弟子模块）：record 装载（`append_record`/`convert_yield`）与
//! C2 direct-write staging（`register_yield_column`/`begin_row`/`stage_yield_cell
//! [_f64]`/`commit_each_row`），及逐行私有 helper（`push_system_row`/`fill_row_gaps`）。
//! 批提交面（`commit_each_rows_batch`/`commit_close_rows_batch`/`finish`）见
//! `column_batch_commit.rs`；类型声明与收口在父 `column_batch.rs`。

use smol_str::SmolStr;
use std::sync::Arc;

use wf_lang::FieldType;
use wp_model_core::model::{DataType, Value as ModelValue};

use orion_error::conversion::ToStructError;

use crate::alert::types as alert_types;
use crate::alert::types::{OutputRecord, WFU_PREFIX};
use crate::error::{CoreReason, CoreResult};
use crate::match_engine::Value;

use super::*;

impl AlertColumnBuilder {
    pub fn new(target: Arc<str>) -> Self {
        Self {
            target,
            len: 0,
            wfx_id: Vec::new(),
            rule_name: None,
            score: Vec::new(),
            entity_type: None,
            entity_id: Vec::new(),
            origin: None,
            close_reason: None,
            fired_at: Vec::new(),
            emit_time: None,
            summary: None,
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

    /// record 路径（`append_record`）：系统字段可能 per-row 变化（close 的
    /// summary/close_reason 等）→ 逐行 push Rows（不做常量假设）。
    fn push_system_row(slot: &mut Option<ColumnData<Arc<str>>>, value: Arc<str>) {
        match slot {
            Some(ColumnData::Rows(v)) => v.push(value),
            _ => *slot = Some(ColumnData::Rows(vec![value])),
        }
    }

    /// Pre-size every column for `additional` more rows (system columns and
    /// the yield columns created so far). The batched on-each direct path
    /// calls this once per event-batch segment so the per-row commits stop
    /// paying amortized vector growth.
    pub fn reserve_rows(&mut self, additional: usize) {
        self.wfx_id.reserve(additional);
        self.score.reserve(additional);
        self.entity_id.reserve(additional);
        self.fired_at.reserve(additional);
        // 系统常量列（ColumnData::Const）免 per-row 数组——不 reserve。
        for col in &mut self.yield_cols {
            if !col.is_const_column() {
                col.metas.reserve(additional);
                col.values.reserve(additional);
            }
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

        // Infallible column pushes (system fields, then yield cells).
        // record 路径系统字段可能 per-row 变化 → Rows 逐行 push（不做常量假设）。
        self.wfx_id.push(SmolStr::from(record.wfx_id.clone()));
        Self::push_system_row(&mut self.rule_name, Arc::clone(&record.rule_name));
        self.score.push(record.score);
        Self::push_system_row(&mut self.entity_type, Arc::clone(&record.entity_type));
        self.entity_id.push(SmolStr::from(record.entity_id.clone()));
        Self::push_system_row(&mut self.origin, Arc::from(record.origin.as_str()));
        Self::push_system_row(
            &mut self.close_reason,
            Arc::from(
                record
                    .origin
                    .close_reason()
                    .map_or("", |reason| reason.as_str()),
            ),
        );
        self.fired_at.push(record.fired_at.clone());
        Self::push_system_row(&mut self.emit_time, Arc::clone(&record.emit_time));
        Self::push_system_row(&mut self.summary, Arc::clone(&record.summary));
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
        Self::set_system_const(&mut self.rule_name, cells.rule_name);
        self.score.push(cells.score);
        Self::set_system_const(&mut self.entity_type, cells.entity_type);
        self.entity_id.push(cells.entity_id);
        Self::set_system_const(&mut self.origin, cells.origin);
        Self::set_system_const(&mut self.close_reason, cells.close_reason);
        self.fired_at.push(cells.fired_at);
        // emit_time 跨批变化（cached_emit_time 按 nanos 缓存，不同批不同值）
        // → 不能常量折叠，逐行 Rows（R1 修复：Const 折叠会把后续批的
        // emit_time 错读成第一批的值）。
        Self::push_system_row(&mut self.emit_time, Arc::clone(cells.emit_time));
        // summary 可能 per-row（match 规则 scope 嵌入 build_summary）→ Rows。
        Self::push_system_row(&mut self.summary, Arc::clone(cells.summary));
        for (col_idx, meta, value) in self.staged.drain(..) {
            let col = &mut self.yield_cols[col_idx];
            col.metas.push(meta);
            col.values.push(value);
        }
        self.fill_row_gaps();
        self.len += 1;
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
            // 常量列（字面量 yield：alert_type/request_count 等）：免每行
            // cell——values/metas 不逐行 push，读时按行展开 `const_value`
            //（省 ~32B/行 × 常量列数；2026-08-26 列式化）。
            if col.is_const_column() {
                continue;
            }
            if col.values.len() == self.len {
                col.metas.push(DataType::Ignore);
                col.values.push(ModelValue::Null);
            }
        }
        debug_assert!(
            self.yield_cols
                .iter()
                .all(|col| col.is_const_column() || col.values.len() == self.len + 1)
        );
    }
}
