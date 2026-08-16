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
use wp_model_core::model::{DataRecord, DataType, Field, FieldStorage, Value as ModelValue};

use crate::error::{CoreReason, CoreResult};

use super::types::{OutputRecord, WFU_PREFIX};
use super::types as alert_types;
use wf_lang::wfu_meta::{
    WFU_CLOSE_REASON, WFU_EMIT_TIME, WFU_ENTITY_ID, WFU_ENTITY_TYPE, WFU_FIRED_AT, WFU_ID,
    WFU_ORIGIN, WFU_RULE_NAME, WFU_SCORE, WFU_SUMMARY,
};

/// One finished columnar alert batch, addressed to a single yield target.
pub struct AlertColumnBatch {
    target: Arc<str>,
    len: usize,
    /// Fixed system-field columns, in `to_data_record` order.
    wfx_id: Vec<Arc<str>>,
    rule_name: Vec<Arc<str>>,
    score: Vec<f64>,
    entity_type: Vec<Arc<str>>,
    entity_id: Vec<Arc<str>>,
    origin: Vec<Arc<str>>,
    close_reason: Vec<Arc<str>>,
    fired_at: Vec<Arc<str>>,
    emit_time: Vec<Arc<str>>,
    summary: Vec<Arc<str>>,
    /// Yield columns (layout follows the first appended record).
    yield_cols: Vec<YieldCol>,
}

struct YieldCol {
    name: Arc<str>,
    metas: Vec<DataType>,
    values: Vec<ModelValue>,
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
                ModelValue::from(self.wfx_id[row].as_ref()),
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
                ModelValue::from(self.entity_id[row].as_ref()),
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
                ModelValue::from(self.fired_at[row].as_ref()),
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

/// Accumulates records into columns for one yield target; `finish()` produces
/// the immutable batch handed to the sink channel.
pub struct AlertColumnBuilder {
    target: Arc<str>,
    len: usize,
    wfx_id: Vec<Arc<str>>,
    rule_name: Vec<Arc<str>>,
    score: Vec<f64>,
    entity_type: Vec<Arc<str>>,
    entity_id: Vec<Arc<str>>,
    origin: Vec<Arc<str>>,
    close_reason: Vec<Arc<str>>,
    fired_at: Vec<Arc<str>>,
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
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
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
        self.wfx_id.push(Arc::from(record.wfx_id.as_str()));
        self.rule_name.push(Arc::clone(&record.rule_name));
        self.score.push(record.score);
        self.entity_type.push(Arc::clone(&record.entity_type));
        self.entity_id.push(Arc::from(record.entity_id.as_str()));
        self.origin
            .push(Arc::from(record.origin.as_str()));
        self.close_reason.push(Arc::from(
            record
                .origin
                .close_reason()
                .map_or("", |reason| reason.as_str()),
        ));
        self.fired_at.push(Arc::from(record.fired_at.as_str()));
        self.emit_time.push(Arc::clone(&record.emit_time));
        self.summary.push(Arc::clone(&record.summary));
        for (col_idx, meta, value) in scratch.drain(..) {
            let col = &mut self.yield_cols[col_idx];
            col.metas.push(meta);
            col.values.push(value);
        }
        debug_assert!(self
            .yield_cols
            .iter()
            .all(|col| col.values.len() == self.len + 1));
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
            if record.yield_fields[..idx].iter().any(|(prev, _)| prev == name) {
                return CoreReason::DataFormat
                    .to_err()
                    .with_detail(format!("duplicate exported field {name:?}"))
                    .err();
            }
            let field_type = record
                .yield_field_types
                .iter()
                .find_map(|(field_name, field_type)| {
                    (field_name == name).then_some(field_type)
                });
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

    /// Seal the builder into an immutable batch. The builder is left empty
    /// (capacities are not preserved; see flush call sites for reuse).
    pub fn finish(&mut self) -> AlertColumnBatch {
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
                (Arc::from("auction_id"), FieldType::Base(wf_lang::BaseType::Float)),
                (Arc::from("price"), FieldType::Base(wf_lang::BaseType::Float)),
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
            let via_columns = batch
                .iter_data_records()
                .nth(row)
                .unwrap()
                .unwrap();
            let via_rows = record.to_data_record().unwrap();
            assert_records_equal(&via_columns, &via_rows);
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
}
