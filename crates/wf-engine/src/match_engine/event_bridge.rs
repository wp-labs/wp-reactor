use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::array::{
    Array, BooleanArray, FixedSizeListArray, Float64Array, Int64Array, LargeListArray, ListArray,
    StringArray, StructArray, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, Field, TimeUnit};
use arrow::record_batch::RecordBatch;
use smol_str::SmolStr;

use super::match_engine::{
    EngineHashMap, Event, FieldSource, ScopeKey, Value, extract_scope_key_from_row, field_ref_name,
};
use crate::window::scope_key_from_column;
use wf_lang::ast::FieldRef;
use wf_lang::plan::KeyMapPlan;

pub const WFL_FIELD_TYPE_METADATA_KEY: &str = "wf.wfl.field_type";
pub const WFL_FIELD_TYPE_OBJECT: &str = "object";
pub const WFL_FIELD_TYPE_ARRAY: &str = "array";

pub fn is_wfl_structured_field(field: &Field) -> bool {
    wfl_structured_field_kind(field).is_some()
}

pub fn wfl_structured_field_kind(field: &Field) -> Option<&str> {
    // 一次 metadata 查找（旧实现匹配命中后二次 get，纯浪费）。
    let kind = field
        .metadata()
        .get(WFL_FIELD_TYPE_METADATA_KEY)
        .map(String::as_str);
    match kind {
        Some(WFL_FIELD_TYPE_OBJECT | WFL_FIELD_TYPE_ARRAY) => kind,
        _ => None,
    }
}

/// Resolve the batch column index of the event-time field.
pub fn batch_time_col_index(batch: &RecordBatch, time_field: Option<&str>) -> Option<usize> {
    let tf = time_field?;
    batch.schema().fields().iter().position(|f| f.name() == tf)
}

/// Read event-time nanos from a resolved time column at `row`, mirroring
/// [`super::match_engine::CepStateMachine::event_time_nanos`] exactly (including
/// the f64 round-trip that the eager `extract_event_time` path uses).
///
/// Returns 0 when the column is null or non-numeric.
pub fn batch_event_time_nanos_at(batch: &RecordBatch, time_col_index: usize, row: usize) -> i64 {
    let col = batch.column(time_col_index);
    if col.is_null(row) {
        return 0;
    }
    match col.data_type() {
        DataType::Int64 => col
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|a| a.value(row) as f64 as i64)
            .unwrap_or(0),
        DataType::Float64 => col
            .as_any()
            .downcast_ref::<Float64Array>()
            .map(|a| a.value(row) as i64)
            .unwrap_or(0),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => col
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .map(|a| a.value(row) as f64 as i64)
            .unwrap_or(0),
        _ => 0,
    }
}

/// Extract the event-time nanos for `row` straight from the batch time column.
///
/// Returns 0 when the time field is absent, non-numeric, or null. Prefer
/// [`batch_time_col_index`] + [`batch_event_time_nanos_at`] when reading many
/// rows, to resolve the column only once.
pub fn batch_event_time_nanos(batch: &RecordBatch, time_field: Option<&str>, row: usize) -> i64 {
    match batch_time_col_index(batch, time_field) {
        Some(idx) => batch_event_time_nanos_at(batch, idx, row),
        None => 0,
    }
}

/// Convert an Arrow [`RecordBatch`] into a `Vec<Event>`, one per row.
///
/// Each column is mapped to an [`Event`] field by column name. Null values
/// are skipped (the field is simply absent from that row's event).
///
/// | Arrow Type           | → | CEP Value               |
/// |----------------------|---|-------------------------|
/// | Int64                | → | Value::Number(i as f64) |
/// | Float64              | → | Value::Number(f)        |
/// | Utf8                 | → | Value::Str(s)           |
/// | Boolean              | → | Value::Bool(b)          |
/// | Timestamp(Ns, _)     | → | Value::Number(ns as f64)|
/// | Struct               | → | Value::Object           |
/// | List/LargeList       | → | Value::Array            |
pub fn batch_to_events(batch: &RecordBatch) -> Vec<Event> {
    batch_to_events_with(batch, None)
}

/// Like [`batch_to_events`], but only materializes the listed field names.
///
/// Used by the window layer to avoid materializing schema fields no rule
/// reads (see wf-lang `field_usage`), the dominant peak RSS on wide windows.
/// Fields absent from the batch schema are ignored.
pub fn batch_to_events_filtered(
    batch: &RecordBatch,
    fields: &std::collections::HashSet<String>,
) -> Vec<Event> {
    batch_to_events_with(batch, Some(fields))
}

fn batch_to_events_with(
    batch: &RecordBatch,
    only_fields: Option<&std::collections::HashSet<String>>,
) -> Vec<Event> {
    let num_rows = batch.num_rows();
    let schema = batch.schema();
    let mut events = Vec::with_capacity(num_rows);

    for row in 0..num_rows {
        let mut fields = EngineHashMap::default();
        for (col_idx, field) in schema.fields().iter().enumerate() {
            if let Some(only) = only_fields
                && !only.contains(field.name())
            {
                continue;
            }
            let col = batch.column(col_idx);
            if col.is_null(row) {
                continue;
            }
            if let Some(val) = extract_field_value(field, col.as_ref(), row) {
                fields.insert(field.name().into(), val);
            }
        }
        events.push(Event { fields });
    }
    events
}

/// Materialize only the given row indices into events (L2 deferred
/// materialization primitive).
///
/// `indices` must be ascending (the columnar mask → indices step preserves
/// batch row order). Rows out of range are skipped. This is the counterpart to
/// [`batch_to_events`] that avoids materializing the ~99% of rows a columnar
/// guard rejects (Q2 hit 0.81%).
pub fn materialize_rows(batch: &RecordBatch, indices: &[u32]) -> Vec<Event> {
    materialize_rows_with(batch, indices, None)
}

/// Like [`materialize_rows`], but only materializes the listed field names.
pub fn materialize_rows_filtered(
    batch: &RecordBatch,
    indices: &[u32],
    fields: &std::collections::HashSet<String>,
) -> Vec<Event> {
    materialize_rows_with(batch, indices, Some(fields))
}

fn materialize_rows_with(
    batch: &RecordBatch,
    indices: &[u32],
    only_fields: Option<&std::collections::HashSet<String>>,
) -> Vec<Event> {
    let num_rows = batch.num_rows();
    let schema = batch.schema();
    let mut events = Vec::with_capacity(indices.len());

    for &row_u32 in indices {
        let row = row_u32 as usize;
        if row >= num_rows {
            continue;
        }
        let mut fields = EngineHashMap::default();
        for (col_idx, field) in schema.fields().iter().enumerate() {
            if let Some(only) = only_fields
                && !only.contains(field.name())
            {
                continue;
            }
            let col = batch.column(col_idx);
            if col.is_null(row) {
                continue;
            }
            if let Some(val) = extract_field_value(field, col.as_ref(), row) {
                fields.insert(field.name().into(), val);
            }
        }
        events.push(Event { fields });
    }
    events
}

/// Convert an Arrow [`RecordBatch`] into timestamped rows for asof join.
///
/// Each row becomes `(timestamp_nanos, fields)`. The timestamp is extracted
/// as a raw `i64` from the column at `time_col_index`. Rows with a null
/// timestamp are skipped. All columns (including the time column) are
/// included in the fields map via [`extract_value`], so the time field
/// remains available in the join context.
pub fn batch_to_timestamped_rows(
    batch: &RecordBatch,
    time_col_index: usize,
) -> Vec<(i64, HashMap<String, Value>)> {
    let num_rows = batch.num_rows();
    let schema = batch.schema();
    let ts_col = batch.column(time_col_index);
    let Some(ts_array) = ts_col.as_any().downcast_ref::<TimestampNanosecondArray>() else {
        return Vec::new();
    };

    let mut rows = Vec::with_capacity(num_rows);
    for row in 0..num_rows {
        if ts_array.is_null(row) {
            continue;
        }
        let ts = ts_array.value(row);
        let mut fields = HashMap::new();
        for (col_idx, field) in schema.fields().iter().enumerate() {
            let col = batch.column(col_idx);
            if col.is_null(row) {
                continue;
            }
            if let Some(val) = extract_field_value(field, col.as_ref(), row) {
                fields.insert(field.name().clone(), val);
            }
        }
        rows.push((ts, fields));
    }
    rows
}

pub(crate) fn extract_field_value(field: &Field, col: &dyn Array, row: usize) -> Option<Value> {
    // 先查列类型再查 metadata：只有 Utf8 列才可能是 structured JSON。旧实现先查
    // metadata（每次字段读取的纯开销）——q15 全 Int64 字段每事件 34 次白查，
    // 真实运行热点 wfl_structured_field_kind 312M 次（2026-08-22 实测）。
    if matches!(col.data_type(), DataType::Utf8)
        && let Some(kind) = wfl_structured_field_kind(field)
    {
        let arr = col.as_any().downcast_ref::<StringArray>()?;
        return serde_json::from_str::<serde_json::Value>(arr.value(row))
            .ok()
            .and_then(|value| json_to_structured_value(kind, value));
    }
    extract_value(col, row)
}

fn extract_value(col: &dyn Array, row: usize) -> Option<Value> {
    match col.data_type() {
        DataType::Int64 => {
            let arr = col.as_any().downcast_ref::<Int64Array>()?;
            Some(Value::Number(arr.value(row) as f64))
        }
        DataType::Float64 => {
            let arr = col.as_any().downcast_ref::<Float64Array>()?;
            Some(Value::Number(arr.value(row)))
        }
        DataType::Utf8 => {
            let arr = col.as_any().downcast_ref::<StringArray>()?;
            Some(Value::Str(arr.value(row).into()))
        }
        DataType::Boolean => {
            let arr = col.as_any().downcast_ref::<BooleanArray>()?;
            Some(Value::Bool(arr.value(row)))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let arr = col.as_any().downcast_ref::<TimestampNanosecondArray>()?;
            Some(Value::Number(arr.value(row) as f64))
        }
        DataType::Struct(_) => {
            let arr = col.as_any().downcast_ref::<StructArray>()?;
            let mut fields = EngineHashMap::default();
            for (field, child) in arr.fields().iter().zip(arr.columns()) {
                if child.is_null(row) {
                    continue;
                }
                if let Some(value) = extract_value(child.as_ref(), row) {
                    fields.insert(field.name().into(), value);
                }
            }
            Some(Value::Object(fields))
        }
        DataType::List(_) => {
            let arr = col.as_any().downcast_ref::<ListArray>()?;
            Some(Value::Array(extract_list_values(arr.value(row).as_ref())))
        }
        DataType::LargeList(_) => {
            let arr = col.as_any().downcast_ref::<LargeListArray>()?;
            Some(Value::Array(extract_list_values(arr.value(row).as_ref())))
        }
        DataType::FixedSizeList(_, _) => {
            let arr = col.as_any().downcast_ref::<FixedSizeListArray>()?;
            Some(Value::Array(extract_list_values(arr.value(row).as_ref())))
        }
        _ => None,
    }
}

fn json_to_value(value: serde_json::Value) -> Option<Value> {
    match value {
        serde_json::Value::Null => None,
        serde_json::Value::Bool(v) => Some(Value::Bool(v)),
        serde_json::Value::Number(v) => v.as_f64().map(Value::Number),
        serde_json::Value::String(v) => Some(Value::Str(v.into())),
        serde_json::Value::Array(values) => Some(Value::Array(
            values.into_iter().filter_map(json_to_value).collect(),
        )),
        serde_json::Value::Object(fields) => Some(Value::Object(
            fields
                .into_iter()
                .filter_map(|(key, value)| json_to_value(value).map(|value| (key.into(), value)))
                .collect(),
        )),
    }
}

fn json_to_structured_value(kind: &str, value: serde_json::Value) -> Option<Value> {
    match (kind, value) {
        (WFL_FIELD_TYPE_OBJECT, serde_json::Value::Object(fields)) => {
            json_to_value(serde_json::Value::Object(fields))
        }
        (WFL_FIELD_TYPE_ARRAY, serde_json::Value::Array(values)) => {
            json_to_value(serde_json::Value::Array(values))
        }
        _ => None,
    }
}

fn extract_list_values(values: &dyn Array) -> Vec<Value> {
    let mut out = Vec::with_capacity(values.len());
    for idx in 0..values.len() {
        if values.is_null(idx) {
            continue;
        }
        if let Some(value) = extract_value(values, idx) {
            out.push(value);
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Columnar event view (on-each fast path — "on-each 完全不物化")
// ---------------------------------------------------------------------------

/// Read one cell of `col_idx` as the same string `window.has()` membership
/// uses: the shared [`extract_field_value`] conversion (Int64/Timestamp →
/// `Number` with the f64 round-trip, Utf8 → `Str`, Boolean → `Bool`), then the
/// Event-path string form (Str → its text, Number → f64 Display, Bool →
/// `true`/`false`). Structured values (List / Struct / JSON object-array
/// columns) → `None`, matching the `Array`/`Object` skip in
/// [`WindowLookup::snapshot_field_values`]. Null cell → `None`.
///
/// Lets `window.has()` build its distinct-value `HashSet` from a single column
/// instead of materializing the whole referenced window into `Event` HashMaps.
pub fn column_scalar_string(batch: &RecordBatch, col_idx: usize, row: usize) -> Option<String> {
    let col = batch.column(col_idx);
    if col.is_null(row) {
        return None;
    }
    let value = extract_field_value(batch.schema_ref().field(col_idx), col.as_ref(), row)?;
    match value {
        Value::Str(s) => Some(s.to_string()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        Value::Array(_) | Value::Object(_) => None,
    }
}

/// Batch-level `field name → column index` map, so [`ColumnarEvent::field_value`]
/// is O(1) instead of a per-call `schema().index_of` linear scan. Built once per
/// batch ([`build_field_index`]) and Arc-shared across every row view.
pub type FieldIndex = EngineHashMap<SmolStr, usize>;

/// Build a `name → column index` map for a batch (one pass over its schema).
/// The schema is immutable for the batch's lifetime, so the index is stable.
pub fn build_field_index(batch: &RecordBatch) -> Arc<FieldIndex> {
    let mut index = EngineHashMap::default();
    for (i, field) in batch.schema().fields().iter().enumerate() {
        index.insert(SmolStr::new(field.name()), i);
    }
    Arc::new(index)
}

/// Field access straight from an Arrow row, byte-identical to the eager
/// `Event{fields: HashMap}` semantics: the same [`extract_field_value`]
/// conversions, null / failed extraction → field absent.
///
/// Used by the on-each columnar fast path to skip per-row `Event`
/// materialization entirely (design doc §3.5「`on each` 规则完全不物化」) and by
/// the match state machine's columnar entry (P3 FieldView) so hit rows never
/// build a HashMap.
pub struct ColumnarEvent<'a> {
    batch: &'a RecordBatch,
    row: usize,
    /// Optional batch-level name→column index for O(1) `field_value`.
    index: Option<Arc<FieldIndex>>,
    /// Optional `materialize_fields` projection: [`FieldSource::to_event`]
    /// materializes only these fields (byte-identical to
    /// `materialize_rows_filtered`); `None` = all schema columns
    /// (byte-identical to `batch_to_events`). Mirrors the eager deferred
    /// path's projected trigger event on emit.
    projection: Option<Arc<HashSet<String>>>,
}

impl<'a> ColumnarEvent<'a> {
    pub fn new(batch: &'a RecordBatch, row: usize) -> Self {
        Self {
            batch,
            row,
            index: None,
            projection: None,
        }
    }

    /// Same as [`Self::new`], but carry a batch-level field-name index so
    /// [`Self::field_value`] resolves names in O(1) instead of a per-call
    /// `schema().index_of` linear scan. Use on hot paths that read many rows.
    pub fn with_index(batch: &'a RecordBatch, row: usize, index: Arc<FieldIndex>) -> Self {
        Self {
            batch,
            row,
            index: Some(index),
            projection: None,
        }
    }

    /// [`Self::with_index`] plus the window's `materialize_fields` projection,
    /// so [`FieldSource::to_event`] reproduces the eager deferred path's
    /// projected trigger event exactly (extra schema columns stay out of the
    /// eval context instead of risking a label/name collision).
    pub fn with_index_projected(
        batch: &'a RecordBatch,
        row: usize,
        index: Arc<FieldIndex>,
        projection: Option<Arc<HashSet<String>>>,
    ) -> Self {
        Self {
            batch,
            row,
            index: Some(index),
            projection,
        }
    }

    /// The shared Arrow batch this view reads from (all events of one batch
    /// share it — batch-level indices resolve once per batch).
    pub fn batch(&self) -> &'a RecordBatch {
        self.batch
    }

    pub(crate) fn row(&self) -> usize {
        self.row
    }

    /// Field value by name, or `None` when the column is absent / null /
    /// fails extraction — mirrors `Event.fields.get(name)`.
    pub fn field_value(&self, name: &str) -> Option<Value> {
        let idx = match &self.index {
            Some(index) => index.get(name).copied()?,
            None => self.batch.schema().index_of(name).ok()?,
        };
        self.value_at(idx)
    }

    /// Field value by a **pre-resolved** column index — byte-identical to
    /// [`Self::field_value`] for the same column, but skips the per-call
    /// `schema().index_of(name)` lookup. The index is stable for the lifetime
    /// of a batch (the schema is `Arc`-shared and immutable), so callers
    /// resolve it once per batch (hot-path Q1 bisection).
    pub fn value_at(&self, idx: usize) -> Option<Value> {
        let col = self.batch.column(idx);
        if col.is_null(self.row) {
            return None;
        }
        extract_field_value(self.batch.schema().field(idx), col.as_ref(), self.row)
    }

    /// Mirrors `CepStateMachine::extract_event_str`: a `Str` field → its
    /// string, anything else (absent / non-str) → empty string.
    pub fn field_value_str(&self, name: &str) -> String {
        match self.field_value(name) {
            Some(Value::Str(s)) => s.to_string(),
            _ => String::new(),
        }
    }
}

impl FieldSource for ColumnarEvent<'_> {
    fn field_value(&self, name: &str) -> Option<Value> {
        // The inherent method resolves the same way (index-first, schema
        // fallback); call it directly rather than recursing into the trait.
        self.field_value(name)
    }

    fn extract_scope_key(
        &self,
        keys: &[FieldRef],
        key_map: Option<&[KeyMapPlan]>,
        alias: &str,
    ) -> Option<ScopeKey> {
        // 列式直读 fast path（qradar 单 key 字符串热路径）：key 字段从 Arrow
        // 列直接构 typed `ScopeKey`（`scope_key_from_column`），免
        // `field_value` → `Value`（String/JSON 物化）+ `Vec<Value>` 分配 +
        // `ScopeKey::from_value` 二次克隆。与行式路径逐类型同构（fanout 分片
        // 对拍测试锁定同一 canonicalization）。
        //
        // 回退行式路径（保持与 `extract_key` 字节一致）：
        // - `key_map` 别名映射（多事件规则）或空 key 列表；
        // - 非简单引用（`field_ref_name` 空，路径 key 编译期已拒绝）；
        // - object/array 结构化 Utf8 列：列式读原始 JSON 串，而行式
        //   `extract_field_value` 解析成 `Value::Object` →
        //   `ScopeKey::Str("[object]")`，语义不同必须回退。
        if key_map.is_none() && !keys.is_empty() {
            let mut acc: Option<ScopeKey> = None;
            let mut row_path = false;
            for key in keys {
                let name = field_ref_name(key);
                if name.is_empty() {
                    row_path = true;
                    break;
                }
                let idx = match &self.index {
                    Some(index) => *index.get(name)?, // 列缺失 → 同行式 key 缺失跳过
                    None => match self.batch.schema().index_of(name) {
                        Ok(i) => i,
                        Err(_) => return None,
                    },
                };
                let field = self.batch.schema_ref().field(idx);
                if matches!(field.data_type(), DataType::Utf8)
                    && wfl_structured_field_kind(field).is_some()
                {
                    row_path = true;
                    break;
                }
                let v = scope_key_from_column(self.batch, idx, self.row)?; // key 列 null → 跳过（同行式缺失语义）
                acc = Some(match acc {
                    None => v,
                    Some(prev) => ScopeKey::Pair(Box::new(prev), Box::new(v)),
                });
            }
            if !row_path {
                return Some(acc.unwrap_or(ScopeKey::Empty));
            }
        }
        extract_scope_key_from_row(self, keys, key_map, alias)
    }

    fn field_names(&self) -> Vec<&str> {
        // `schema_ref()` borrows from the batch; `schema()` would return an
        // owned `Arc<Schema>` temporary whose fields we cannot return. With a
        // `materialize_fields` projection the names match the eager Event's map
        // keys exactly (projected, non-null only), so the iterate-all evidence
        // collection and the memory estimate stay byte-identical.
        self.batch
            .schema_ref()
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .filter(|name| match &self.projection {
                Some(proj) => proj.contains(*name),
                None => true,
            })
            .collect()
    }

    fn to_event(&self) -> Event {
        let mut fields = EngineHashMap::default();
        // Iterate the precomputed field index (name → column) instead of
        // building a fresh `field_names` Vec — this runs per emit (the
        // trigger_event for every fired instance, e.g. Q7's per-event max rule).
        if let Some(index) = &self.index {
            for (name, &col_idx) in index.iter() {
                if let Some(proj) = &self.projection
                    && !proj.contains(name.as_str())
                {
                    continue;
                }
                let col = self.batch.column(col_idx);
                if col.is_null(self.row) {
                    continue;
                }
                if let Some(value) = extract_field_value(
                    self.batch.schema_ref().field(col_idx),
                    col.as_ref(),
                    self.row,
                ) {
                    fields.insert(name.clone(), value);
                }
            }
        } else {
            for name in self.field_names() {
                if let Some(value) = self.field_value(name) {
                    fields.insert(SmolStr::new(name), value);
                }
            }
        }
        Event { fields }
    }
}

// ---------------------------------------------------------------------------
// JoinRow — columnar join-candidate row (免物化: 按需读字段)
// ---------------------------------------------------------------------------

/// A join-candidate row: a columnar view over an owned batch row (zero HashMap
/// materialization — fields are read on demand) or a pre-materialized [`Event`]
/// (provider / join-index rows). Lets the join executor evaluate conditions and
/// enrich the eval context without materializing the whole referenced window.
#[derive(Clone)]
pub enum JoinRow {
    Columnar {
        batch: Arc<RecordBatch>,
        row: usize,
        index: Arc<FieldIndex>,
        /// Optional `materialize_fields` projection: [`Self::field_names`]
        /// exposes only these columns, so enrich reads just the fields rules
        /// actually use (byte-identical to the projected `Event` path). `None`
        /// = all schema columns.
        projection: Option<Arc<HashSet<String>>>,
    },
    Event(Arc<Event>),
}

impl JoinRow {
    /// Field value by name, or `None` when absent / null. The Columnar variant
    /// reads through the same [`extract_field_value`] conversion as
    /// `batch_to_events`, so the value is byte-identical to the eager path.
    pub fn field_value(&self, name: &str) -> Option<Value> {
        match self {
            JoinRow::Columnar {
                batch, row, index, ..
            } => {
                let idx = *index.get(name)?;
                let col = batch.column(idx);
                if col.is_null(*row) {
                    return None;
                }
                extract_field_value(batch.schema_ref().field(idx), col.as_ref(), *row)
            }
            JoinRow::Event(ev) => ev.fields.get(name).cloned(),
        }
    }

    /// Every field name this row exposes. The Columnar variant lists the
    /// (optionally projected) schema columns — null cells read `None` via
    /// [`Self::field_value`], matching the eager `batch_to_events` map which
    /// drops nulls; the Event variant lists its materialized (projected) map
    /// keys.
    pub fn field_names(&self) -> Vec<&str> {
        match self {
            JoinRow::Columnar {
                batch, projection, ..
            } => batch
                .schema_ref()
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .filter(|name| match projection {
                    Some(proj) => proj.contains(*name),
                    None => true,
                })
                .collect(),
            JoinRow::Event(ev) => ev.fields.keys().map(|k| k.as_str()).collect(),
        }
    }
}

// ---------------------------------------------------------------------------
// TriggerEvent — owned per-fire trigger row（P4 终态机制 M3 §11.6）
// ---------------------------------------------------------------------------

/// Owned per-fire trigger row: either a materialized [`Event`] (row-mode /
/// fallback capture) or an owned projected columnar view (Arc batch + row +
/// field index + projection). Deferred-match fires carry the columnar view —
/// no per-fire `to_event()` — and consumers read fields lazily through
/// [`FieldSource`] (`build_eval_context` / ctx-free `resolve_field`). Field
/// reads are byte-identical to the eager path (`extract_field_value`);
/// `field_names` respects the projection so ctx building sees exactly the M1
/// read-set columns.
#[derive(Debug, Clone, PartialEq)]
pub enum TriggerEvent {
    Event(Arc<Event>),
    Columnar {
        batch: Arc<RecordBatch>,
        row: usize,
        index: Arc<FieldIndex>,
        projection: Option<Arc<HashSet<String>>>,
    },
}

impl TriggerEvent {
    /// Wrap an already-owned materialized event (row-mode captures).
    pub fn from_event(event: Arc<Event>) -> Self {
        TriggerEvent::Event(event)
    }

    /// Owned projected columnar snapshot (deferred-match captures).
    pub fn columnar(
        batch: Arc<RecordBatch>,
        row: usize,
        index: Arc<FieldIndex>,
        projection: Option<Arc<HashSet<String>>>,
    ) -> Self {
        TriggerEvent::Columnar {
            batch,
            row,
            index,
            projection,
        }
    }
}

impl From<Arc<Event>> for TriggerEvent {
    fn from(event: Arc<Event>) -> Self {
        TriggerEvent::Event(event)
    }
}

impl From<Event> for TriggerEvent {
    fn from(event: Event) -> Self {
        TriggerEvent::Event(Arc::new(event))
    }
}

impl FieldSource for TriggerEvent {
    fn field_value(&self, name: &str) -> Option<Value> {
        match self {
            TriggerEvent::Event(ev) => ev.fields.get(name).cloned(),
            TriggerEvent::Columnar {
                batch, row, index, ..
            } => {
                let idx = *index.get(name)?;
                let col = batch.column(idx);
                if col.is_null(*row) {
                    return None;
                }
                extract_field_value(batch.schema_ref().field(idx), col.as_ref(), *row)
            }
        }
    }

    fn field_names(&self) -> Vec<&str> {
        match self {
            TriggerEvent::Event(ev) => ev.fields.keys().map(|k| k.as_str()).collect(),
            TriggerEvent::Columnar {
                batch, projection, ..
            } => batch
                .schema_ref()
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .filter(|name| match projection {
                    Some(proj) => proj.contains(*name),
                    None => true,
                })
                .collect(),
        }
    }

    fn to_event(&self) -> Event {
        match self {
            TriggerEvent::Event(ev) => (**ev).clone(),
            TriggerEvent::Columnar {
                batch,
                row,
                index,
                projection,
            } => ColumnarEvent::with_index_projected(
                batch.as_ref(),
                *row,
                Arc::clone(index),
                projection.clone(),
            )
            .to_event(),
        }
    }
}

/// Build columnar [`JoinRow`]s for every row of the given (cheaply Arc-cloned)
/// batches — the scan-fallback join path. No Event/HashMap materialization.
/// `projection` mirrors the window's `materialize_fields`: `None` = all columns.
pub fn columnar_join_rows(
    batches: Vec<RecordBatch>,
    projection: Option<Arc<HashSet<String>>>,
) -> Vec<JoinRow> {
    let mut rows = Vec::new();
    for batch in batches {
        let batch = Arc::new(batch);
        let index = build_field_index(&batch);
        for row in 0..batch.num_rows() {
            rows.push(JoinRow::Columnar {
                batch: Arc::clone(&batch),
                row,
                index: Arc::clone(&index),
                projection: projection.clone(),
            });
        }
    }
    rows
}

/// Read the **raw** `Timestamp(Ns)` i64 for a row from a resolved time column,
/// or `None` when the column is null / not a `Timestamp(Ns)` column. This is
/// the timestamp the asof-join path compares against (byte-identical to
/// [`columnar_timestamped_join_rows`]); it deliberately skips the f64
/// round-trip that [`batch_event_time_nanos_at`] applies on the eager
/// event-time path, so epoch-nanos values stay exact.
pub fn batch_raw_ts_nanos(batch: &RecordBatch, time_col_index: usize, row: usize) -> Option<i64> {
    batch
        .column(time_col_index)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .and_then(|a| (!a.is_null(row)).then(|| a.value(row)))
}

/// Build timestamped columnar [`JoinRow`]s for the asof-join path. The
/// timestamp is the **raw** `Timestamp(Ns)` i64 (byte-identical to
/// [`batch_to_timestamped_rows`]); rows with a null timestamp are skipped.
pub fn columnar_timestamped_join_rows(
    batches: Vec<RecordBatch>,
    time_col_index: usize,
    projection: Option<Arc<HashSet<String>>>,
) -> Vec<(i64, JoinRow)> {
    let mut rows = Vec::new();
    for batch in batches {
        let batch = Arc::new(batch);
        let index = build_field_index(&batch);
        for row in 0..batch.num_rows() {
            let Some(ts) = batch_raw_ts_nanos(&batch, time_col_index, row) else {
                continue;
            };
            rows.push((
                ts,
                JoinRow::Columnar {
                    batch: Arc::clone(&batch),
                    row,
                    index: Arc::clone(&index),
                    projection: projection.clone(),
                },
            ));
        }
    }
    rows
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::ArrayRef;
    use arrow::datatypes::{Field, Int64Type, Schema};
    use std::sync::Arc;

    fn make_schema(fields: Vec<Field>) -> Arc<Schema> {
        Arc::new(Schema::new(fields))
    }

    #[test]
    fn test_batch_to_events_basic() {
        let schema = make_schema(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("active", DataType::Boolean, false),
        ]);
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![42, 99])) as ArrayRef,
                Arc::new(StringArray::from(vec!["alice", "bob"])) as ArrayRef,
                Arc::new(BooleanArray::from(vec![true, false])) as ArrayRef,
            ],
        )
        .unwrap();

        let events = batch_to_events(&batch);
        assert_eq!(events.len(), 2);

        assert_eq!(events[0].fields["id"], Value::Number(42.0));
        assert_eq!(events[0].fields["name"], Value::Str("alice".into()));
        assert_eq!(events[0].fields["active"], Value::Bool(true));

        assert_eq!(events[1].fields["id"], Value::Number(99.0));
        assert_eq!(events[1].fields["name"], Value::Str("bob".into()));
        assert_eq!(events[1].fields["active"], Value::Bool(false));
    }

    #[test]
    fn test_batch_to_events_timestamp() {
        let schema = make_schema(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        )]);
        let nanos: i64 = 1_700_000_000_000_000_000;
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(TimestampNanosecondArray::from(vec![nanos])) as ArrayRef],
        )
        .unwrap();

        let events = batch_to_events(&batch);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].fields["ts"], Value::Number(nanos as f64));
    }

    #[test]
    fn test_batch_event_time_nanos_matches_extract_event_time_roundtrip() {
        // Int64 / Timestamp(Ns) go through an f64 round-trip exactly like the
        // eager `extract_event_time` (Value::Number(n as f64) → `as i64`); only
        // Float64 is a direct `as i64` cast. This is the correctness contract
        // for the L2 deferred scan reading time straight from the column.
        let schema = make_schema(vec![
            Field::new("i", DataType::Int64, true),
            Field::new("f", DataType::Float64, true),
            Field::new("t", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        ]);
        // 2^53 + 1 is not representable in f64 — the round-trip collapses it.
        let big: i64 = (1i64 << 53) + 1;
        let nanos: i64 = 1_700_000_000_000_000_000;
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![Some(big), None])) as ArrayRef,
                Arc::new(Float64Array::from(vec![Some(1.9), None])) as ArrayRef,
                Arc::new(TimestampNanosecondArray::from(vec![Some(nanos), None])) as ArrayRef,
            ],
        )
        .unwrap();

        let i_idx = batch_time_col_index(&batch, Some("i")).unwrap();
        let f_idx = batch_time_col_index(&batch, Some("f")).unwrap();
        let t_idx = batch_time_col_index(&batch, Some("t")).unwrap();

        // Int64: (value as f64) as i64.
        assert_eq!(
            batch_event_time_nanos_at(&batch, i_idx, 0),
            (big as f64) as i64
        );
        // Float64: direct cast.
        assert_eq!(batch_event_time_nanos_at(&batch, f_idx, 0), 1);
        // Timestamp(Ns): (value as f64) as i64.
        assert_eq!(
            batch_event_time_nanos_at(&batch, t_idx, 0),
            (nanos as f64) as i64
        );
        // Null time → 0 (matching `extract_event_time`'s missing-field fallback).
        assert_eq!(batch_event_time_nanos_at(&batch, i_idx, 1), 0);
        assert_eq!(batch_event_time_nanos_at(&batch, f_idx, 1), 0);
        assert_eq!(batch_event_time_nanos_at(&batch, t_idx, 1), 0);
        // Absent field → 0.
        assert_eq!(batch_event_time_nanos(&batch, Some("missing"), 0), 0);
        assert_eq!(batch_event_time_nanos(&batch, None, 0), 0);
    }

    #[test]
    fn test_batch_to_events_nulls() {
        let schema = make_schema(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]);
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![Some(1), None])) as ArrayRef,
                Arc::new(StringArray::from(vec![None, Some("bob")])) as ArrayRef,
            ],
        )
        .unwrap();

        let events = batch_to_events(&batch);
        assert_eq!(events.len(), 2);

        // Row 0: id=1, name is null (skipped)
        assert_eq!(events[0].fields["id"], Value::Number(1.0));
        assert!(!events[0].fields.contains_key("name"));

        // Row 1: id is null (skipped), name="bob"
        assert!(!events[1].fields.contains_key("id"));
        assert_eq!(events[1].fields["name"], Value::Str("bob".into()));
    }

    #[test]
    fn test_batch_to_events_empty() {
        let schema = make_schema(vec![Field::new("id", DataType::Int64, false)]);
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![] as Vec<i64>)) as ArrayRef],
        )
        .unwrap();

        let events = batch_to_events(&batch);
        assert!(events.is_empty());
    }

    #[test]
    fn test_batch_to_events_float64() {
        let schema = make_schema(vec![Field::new("score", DataType::Float64, false)]);
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Float64Array::from(vec![3.21, 9.87])) as ArrayRef],
        )
        .unwrap();

        let events = batch_to_events(&batch);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].fields["score"], Value::Number(3.21));
        assert_eq!(events[1].fields["score"], Value::Number(9.87));
    }

    #[test]
    fn test_batch_to_events_struct_and_list() {
        let tags =
            ListArray::from_iter_primitive::<Int64Type, _, _>(vec![Some(vec![Some(10), Some(20)])]);
        let detection = StructArray::from(vec![(
            Arc::new(Field::new("severity", DataType::Int64, false)),
            Arc::new(Int64Array::from(vec![10])) as ArrayRef,
        )]);
        let extension = StructArray::from(vec![
            (
                Arc::new(Field::new(
                    "detection",
                    detection.data_type().clone(),
                    false,
                )),
                Arc::new(detection) as ArrayRef,
            ),
            (
                Arc::new(Field::new("tags", tags.data_type().clone(), true)),
                Arc::new(tags) as ArrayRef,
            ),
            (
                Arc::new(Field::new("ignored", DataType::Utf8, true)),
                Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
            ),
        ]);
        let schema = make_schema(vec![Field::new(
            "extension",
            extension.data_type().clone(),
            false,
        )]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(extension) as ArrayRef]).unwrap();

        let events = batch_to_events(&batch);
        let Value::Object(extension) = &events[0].fields["extension"] else {
            panic!("expected extension object");
        };
        let Some(Value::Object(detection)) = extension.get("detection") else {
            panic!("expected nested detection object, got {extension:?}");
        };
        assert_eq!(detection.get("severity"), Some(&Value::Number(10.0)));
        assert_eq!(
            extension.get("tags"),
            Some(&Value::Array(vec![
                Value::Number(10.0),
                Value::Number(20.0)
            ]))
        );
        assert!(!extension.contains_key("ignored"));
    }

    #[test]
    fn test_batch_to_events_parses_structured_utf8_json_only_with_metadata() {
        let structured_field = Field::new("extension", DataType::Utf8, true).with_metadata(
            std::collections::HashMap::from([(
                WFL_FIELD_TYPE_METADATA_KEY.to_string(),
                WFL_FIELD_TYPE_OBJECT.to_string(),
            )]),
        );
        let schema = make_schema(vec![
            structured_field,
            Field::new("plain", DataType::Utf8, true),
        ]);
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![r#"{"severity":10,"tags":["ssh"]}"#])) as ArrayRef,
                Arc::new(StringArray::from(vec![r#"{"severity":10}"#])) as ArrayRef,
            ],
        )
        .unwrap();

        let events = batch_to_events(&batch);
        let Value::Object(extension) = &events[0].fields["extension"] else {
            panic!("expected extension object");
        };
        assert_eq!(extension.get("severity"), Some(&Value::Number(10.0)));
        assert_eq!(
            extension.get("tags"),
            Some(&Value::Array(vec![Value::Str("ssh".into())]))
        );
        assert_eq!(
            events[0].fields["plain"],
            Value::Str(r#"{"severity":10}"#.into())
        );
    }

    #[test]
    fn test_batch_to_events_parses_structured_array_utf8_json_with_metadata() {
        let structured_field = Field::new("ports", DataType::Utf8, true).with_metadata(
            std::collections::HashMap::from([(
                WFL_FIELD_TYPE_METADATA_KEY.to_string(),
                WFL_FIELD_TYPE_ARRAY.to_string(),
            )]),
        );
        let schema = make_schema(vec![
            structured_field,
            Field::new("plain", DataType::Utf8, true),
        ]);
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![r#"[22,2222]"#])) as ArrayRef,
                Arc::new(StringArray::from(vec![r#"[22,2222]"#])) as ArrayRef,
            ],
        )
        .unwrap();

        let events = batch_to_events(&batch);
        assert_eq!(
            events[0].fields["ports"],
            Value::Array(vec![Value::Number(22.0), Value::Number(2222.0)])
        );
        assert_eq!(events[0].fields["plain"], Value::Str(r#"[22,2222]"#.into()));
    }

    #[test]
    fn test_batch_raw_ts_nanos() {
        // Raw `Timestamp(Ns)` i64 must be preserved exactly (no `as f64 as i64`
        // collapse), null → None, and a non-Timestamp column → None.
        let schema = make_schema(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
            Field::new("i", DataType::Int64, true),
        ]);
        let epoch: i64 = 1_767_225_600_000_000_123;
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![Some(epoch), None])) as ArrayRef,
                Arc::new(Int64Array::from(vec![Some(1), Some(2)])) as ArrayRef,
            ],
        )
        .unwrap();

        // Raw value preserved exactly (f64 would round this).
        assert_eq!(batch_raw_ts_nanos(&batch, 0, 0), Some(epoch));
        // Null timestamp → None.
        assert_eq!(batch_raw_ts_nanos(&batch, 0, 1), None);
        // Non-Timestamp(Ns) column → None.
        assert_eq!(batch_raw_ts_nanos(&batch, 1, 0), None);
    }

    #[test]
    fn test_columnar_join_rows_projection() {
        let schema = make_schema(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        ]);
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![42, 99])) as ArrayRef,
                Arc::new(StringArray::from(vec!["alice", "bob"])) as ArrayRef,
                Arc::new(TimestampNanosecondArray::from(vec![1_000, 2_000])) as ArrayRef,
            ],
        )
        .unwrap();

        let proj: Arc<HashSet<String>> =
            Arc::new(HashSet::from(["id".to_string(), "ts".to_string()]));
        let rows = columnar_join_rows(vec![batch], Some(proj));

        assert_eq!(rows.len(), 2);
        // `field_names` exposes only the projected columns.
        let mut names: Vec<&str> = rows[0].field_names();
        names.sort_unstable();
        assert_eq!(names, vec!["id", "ts"]);
        // `field_value` still reads non-projected columns (join conditions).
        assert_eq!(
            rows[0].field_value("name"),
            Some(Value::Str("alice".into()))
        );
        assert_eq!(rows[0].field_value("id"), Some(Value::Number(42.0)));
    }

    #[test]
    fn test_columnar_timestamped_join_rows_projection() {
        let schema = make_schema(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]);
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![
                    Some(1_000),
                    None,
                    Some(3_000),
                ])) as ArrayRef,
                Arc::new(Int64Array::from(vec![42, 99, 7])) as ArrayRef,
                Arc::new(StringArray::from(vec!["alice", "bob", "carol"])) as ArrayRef,
            ],
        )
        .unwrap();

        let proj: Arc<HashSet<String>> = Arc::new(HashSet::from(["ts".to_string()]));
        let rows = columnar_timestamped_join_rows(vec![batch], 0, Some(proj));

        // Null-timestamp row (index 1) is skipped.
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, 1_000);
        assert_eq!(rows[1].0, 3_000);
        // `field_names` is projected to only "ts".
        assert_eq!(rows[0].1.field_names(), vec!["ts"]);
        // `field_value` still reads non-projected "id" (join conditions).
        assert_eq!(rows[0].1.field_value("id"), Some(Value::Number(42.0)));
        assert_eq!(rows[1].1.field_value("id"), Some(Value::Number(7.0)));
    }

    #[test]
    fn columnar_extract_scope_key_matches_row_based() {
        // 列式直读 `ColumnarEvent::extract_scope_key`（qradar 单 key 热路径）
        // 必须与行式 `extract_key_simple` + `scope_key_from_values` 逐行构造出
        // **同一个** `ScopeKey`——语义锁定（fanout 分片对拍同款 canonicalization）。
        use crate::match_engine::{extract_key_simple, scope_key_from_values};

        let schema = make_schema(vec![
            Field::new("sip", DataType::Utf8, true),
            Field::new("dport", DataType::Int64, true),
            Field::new("packet_rate", DataType::Float64, true),
            Field::new("blocked", DataType::Boolean, true),
        ]);
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![
                    Some("10.0.0.1"),
                    None,
                    Some("10.0.0.2"),
                ])) as ArrayRef,
                Arc::new(Int64Array::from(vec![Some(443), Some(80), Some(80)])) as ArrayRef,
                Arc::new(Float64Array::from(vec![Some(1.5), Some(2.0), Some(0.0)])) as ArrayRef,
                Arc::new(BooleanArray::from(vec![
                    Some(true),
                    Some(false),
                    Some(true),
                ])) as ArrayRef,
            ],
        )
        .unwrap();
        let index = build_field_index(&batch);
        let events = batch_to_events(&batch);

        // 单 key Utf8（含 null 行 → 双路径均 None）
        let keys = [FieldRef::Simple("sip".into())];
        for (row, row_ev) in events.iter().enumerate() {
            let col = ColumnarEvent::with_index(&batch, row, Arc::clone(&index));
            assert_eq!(
                col.extract_scope_key(&keys, None, "c"),
                extract_key_simple(row_ev, &keys).map(|v| scope_key_from_values(&v)),
                "sip row {row}"
            );
        }

        // 单 key Int64 / Float64 / Boolean 与行式一致
        for name in ["dport", "packet_rate", "blocked"] {
            let keys = [FieldRef::Simple(name.into())];
            for (row, row_ev) in events.iter().enumerate() {
                let col = ColumnarEvent::with_index(&batch, row, Arc::clone(&index));
                assert_eq!(
                    col.extract_scope_key(&keys, None, "c"),
                    extract_key_simple(row_ev, &keys).map(|v| scope_key_from_values(&v)),
                    "{name} row {row}"
                );
            }
        }

        // 多 key：Pair 顺序与行式 `scope_key_from_values` 一致
        let multi = [
            FieldRef::Simple("sip".into()),
            FieldRef::Simple("dport".into()),
        ];
        for (row, row_ev) in events.iter().enumerate() {
            let col = ColumnarEvent::with_index(&batch, row, Arc::clone(&index));
            assert_eq!(
                col.extract_scope_key(&multi, None, "c"),
                extract_key_simple(row_ev, &multi).map(|v| scope_key_from_values(&v)),
                "multi row {row}"
            );
        }
    }

    #[test]
    fn columnar_extract_scope_key_fallbacks() {
        // 结构化 object 列 / key_map / 无 index / 列缺失：回退或拒绝路径必须
        // 与行式 `extract_key_simple` 字节一致。
        use crate::match_engine::{extract_key_simple, scope_key_from_values};

        let obj_field = Field::new("conn_info", DataType::Utf8, true).with_metadata(
            std::collections::HashMap::from([(
                WFL_FIELD_TYPE_METADATA_KEY.to_string(),
                WFL_FIELD_TYPE_OBJECT.to_string(),
            )]),
        );
        let schema = make_schema(vec![obj_field, Field::new("sip", DataType::Utf8, true)]);
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some(r#"{"geo":"cn"}"#), None])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("10.0.0.1"), Some("10.0.0.2")])) as ArrayRef,
            ],
        )
        .unwrap();
        let index = build_field_index(&batch);
        let events = batch_to_events(&batch);

        // object 结构化 key：行式解析 JSON → Value::Object → Str("[object]")，
        // 列式直读会给原始 JSON 串——必须回退行式（语义不变）。
        let keys = [FieldRef::Simple("conn_info".into())];
        let col = ColumnarEvent::with_index(&batch, 0, Arc::clone(&index));
        let expected = extract_key_simple(&events[0], &keys).map(|v| scope_key_from_values(&v));
        assert_eq!(expected, Some(ScopeKey::Str("[object]".into())));
        assert_eq!(col.extract_scope_key(&keys, None, "c"), expected);

        // key_map 别名映射（多事件规则）→ 回退行式
        let keys_sip = [FieldRef::Simple("sip".into())];
        let km = [wf_lang::plan::KeyMapPlan {
            logical_name: "sip".into(),
            source_alias: "c".into(),
            source_field: "sip".into(),
        }];
        let col2 = ColumnarEvent::with_index(&batch, 1, Arc::clone(&index));
        assert_eq!(
            col2.extract_scope_key(&keys_sip, Some(&km), "c"),
            extract_key_simple(&events[1], &keys_sip).map(|v| scope_key_from_values(&v)),
        );

        // 无 index（ColumnarEvent::new）→ schema index_of 路径，结果一致
        let col3 = ColumnarEvent::new(&batch, 0);
        assert_eq!(
            col3.extract_scope_key(&keys_sip, None, "c"),
            extract_key_simple(&events[0], &keys_sip).map(|v| scope_key_from_values(&v)),
        );

        // 列缺失 → None（同行式 key 缺失跳过事件）
        let missing = [FieldRef::Simple("no_such_col".into())];
        assert_eq!(col3.extract_scope_key(&missing, None, "c"), None);
    }

    #[test]
    fn columnar_extract_scope_key_type_lanes() {
        // 类型车道锁定（2026-08-31 review 补）：
        // - Timestamp(Ns) / >2^53 Int64：列式直读 = ScopeKey::Int（精确 i64），
        //   行式 = Float（f64 舍入）——**已知分歧**（fanout 分片
        //   `scope_key_columnar_matches_row_based` 同款：>2^53 行式丢精度），
        //   列式与分片路由一致（本优化的正确方向）；
        // - Struct / List 列：双路径一致 → Str("[object]") / Str("[array]")
        //   （结构化键走 from_value 规范化）；
        // - 空 key 列表 → ScopeKey::Empty（shared instance）。
        use crate::match_engine::{extract_key_simple, scope_key_from_values};
        use arrow::array::TimestampNanosecondArray;
        use arrow::datatypes::TimeUnit;

        // --- Timestamp(Ns) key ---
        let schema = make_schema(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
            Field::new("sip", DataType::Utf8, false),
        ]);
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![
                    1_700_000_000_000_000_000,
                ])) as ArrayRef,
                Arc::new(StringArray::from(vec!["10.0.0.1"])) as ArrayRef,
            ],
        )
        .unwrap();
        let index = build_field_index(&batch);
        let keys = [FieldRef::Simple("ts".into())];
        let col = ColumnarEvent::with_index(&batch, 0, Arc::clone(&index));
        let col_key = col.extract_scope_key(&keys, None, "c").unwrap();
        assert_eq!(col_key, ScopeKey::Int(1_700_000_000_000_000_000));
        // 行式（旧路径）在 >2^53 处发散为 Float——分歧被锁定（fanout 同款）。
        let row_key = extract_key_simple(&col, &keys)
            .map(|v| scope_key_from_values(&v))
            .unwrap();
        assert_ne!(col_key, row_key);
        assert!(matches!(row_key, ScopeKey::Float(_)));

        // --- >2^53 Int64 key（同款分歧）---
        let schema = make_schema(vec![
            Field::new("big", DataType::Int64, false),
            Field::new("sip", DataType::Utf8, false),
        ]);
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![9_007_199_254_740_993])) as ArrayRef, // 2^53+1
                Arc::new(StringArray::from(vec!["10.0.0.1"])) as ArrayRef,
            ],
        )
        .unwrap();
        let index = build_field_index(&batch);
        let keys = [FieldRef::Simple("big".into())];
        let col = ColumnarEvent::with_index(&batch, 0, Arc::clone(&index));
        let col_key = col.extract_scope_key(&keys, None, "c").unwrap();
        assert_eq!(col_key, ScopeKey::Int(9_007_199_254_740_993));
        let row_key = extract_key_simple(&col, &keys)
            .map(|v| scope_key_from_values(&v))
            .unwrap();
        assert_ne!(col_key, row_key, ">2^53 Int64 列式精确 vs 行式 f64 舍入");

        // --- Struct 列 → 双路径均 Str("[object]") ---
        let inner_field = Field::new("geo", DataType::Utf8, false);
        let schema = make_schema(vec![Field::new(
            "obj",
            DataType::Struct(arrow::datatypes::Fields::from(vec![inner_field.clone()])),
            false,
        )]);
        let inner = StringArray::from(vec!["cn"]);
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StructArray::from(vec![(
                Arc::new(inner_field),
                Arc::new(inner) as ArrayRef,
            )])) as ArrayRef],
        )
        .unwrap();
        let index = build_field_index(&batch);
        let keys = [FieldRef::Simple("obj".into())];
        let col = ColumnarEvent::with_index(&batch, 0, Arc::clone(&index));
        assert_eq!(
            col.extract_scope_key(&keys, None, "c"),
            Some(ScopeKey::Str("[object]".into()))
        );

        // --- 空 key 列表 → Empty（shared instance）---
        assert_eq!(col.extract_scope_key(&[], None, "c"), Some(ScopeKey::Empty));
    }

    #[test]
    fn columnar_extract_scope_key_multi_key_gaps() {
        // 多 key 缺口（2026-08-31 review 补）：
        // - 第二个 key 列在批 schema 中缺失 → None（同行式跳过）；
        // - 第二个 key 列为 null → None（同行式缺失语义）。
        use crate::match_engine::{extract_key_simple, scope_key_from_values};
        let schema = make_schema(vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Utf8, true),
        ]);
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["k1", "k2"])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("v1"), None])) as ArrayRef,
            ],
        )
        .unwrap();
        let index = build_field_index(&batch);
        let events = batch_to_events(&batch);

        // 第二个 key 列缺失：a 存在、ghost 不存在 → None（快路径与行式一致）
        let keys = [
            FieldRef::Simple("a".into()),
            FieldRef::Simple("ghost".into()),
        ];
        for (row, row_ev) in events.iter().enumerate() {
            let col = ColumnarEvent::with_index(&batch, row, Arc::clone(&index));
            assert_eq!(
                col.extract_scope_key(&keys, None, "c"),
                extract_key_simple(row_ev, &keys).map(|v| scope_key_from_values(&v)),
                "missing second key col row {row}"
            );
        }

        // 第二个 key 列为 null（row 1）：快路径与行式均 None
        let keys = [FieldRef::Simple("a".into()), FieldRef::Simple("b".into())];
        for (row, row_ev) in events.iter().enumerate() {
            let col = ColumnarEvent::with_index(&batch, row, Arc::clone(&index));
            assert_eq!(
                col.extract_scope_key(&keys, None, "c"),
                extract_key_simple(row_ev, &keys).map(|v| scope_key_from_values(&v)),
                "null second key col row {row}"
            );
        }
    }
}
