//! 列式事件视图（on-each / join / trigger 免物化读面）——2026-09-04 自
//! `event_bridge.rs` 拆出（#[path] sibling）。[`ColumnarEvent`]（on-each 完全不
//! 物化）、[`JoinRow`]（join 候选行）与 [`TriggerEvent`]（P4 触发行）都按需从
//! Arc 批读字段（零 HashMap 物化）；`column_scalar_string` 供 `window.has()`
//! 单列建 set。公开面经 event_bridge 根 `pub use` re-export 保持原路径与可见级；
//! 行式值提取（`extract_field_value` / `wfl_structured_field_kind`）经 `use
//! super::*` 复用根层实现——与 eager 行式路径逐字节同构（对拍见 columnar_tests
//! / event_bridge_r4）。

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{Array, TimestampNanosecondArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use smol_str::SmolStr;

use super::*;
use crate::match_engine::cep::{
    EngineHashMap, Event, FieldSource, ScopeKey, Value, extract_scope_key_from_row, field_ref_name,
};
use crate::window::scope_key_from_column;
use wf_lang::ast::FieldRef;
use wf_lang::plan::KeyMapPlan;

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
#[derive(::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.ColumnarBatch")]
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
                // issue #83：嵌套路径 key 需要 walk 叶值，不能列直读 root（会把
                // 整个 root 列当 key）——统一回退行式提取。
                if matches!(key, FieldRef::Path { .. }) {
                    row_path = true;
                    break;
                }
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
#[derive(Clone, ::moju_derive::MoJu)]
#[moju(kind = "state", domain = "Engine", module = "Engine.ColumnarBatch")]
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
