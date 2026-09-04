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

use smol_str::SmolStr;
use std::sync::Arc;

use wf_lang::wfu_meta::{
    WFU_CLOSE_REASON, WFU_EMIT_TIME, WFU_ENTITY_ID, WFU_ENTITY_TYPE, WFU_FIRED_AT, WFU_ID,
    WFU_ORIGIN, WFU_RULE_NAME, WFU_SCORE, WFU_SUMMARY,
};
use wp_model_core::model::{DataRecord, DataType, Field, FieldStorage, Value as ModelValue};

use crate::error::CoreResult;

// 子模块 #[path] sibling（2026-09-04 拆件）：`stage` = record 装载 + C2 直写 staging
// （逐行路径），`commit` = L3 批提交 + finish 收口，`tests` = cfg(test) 行视图等价对拍。
// 类型声明（ColumnData/AlertColumnBatch/YieldCol/AlertColumnBuilder/EachRowCells）与收口
// 留本层——子模块经 `use super::*` 复用私有字段/方法（私有可见性只向下流，零提级）；
// pub 类型亦留本层，`alert/mod.rs` 的 `pub use column_batch::{...}` 与
// `crate::alert::column_batch::X` 双路径保持零变动，无需 re-export。
#[path = "column_batch_commit.rs"]
mod commit;
#[path = "column_batch_stage.rs"]
mod stage;

#[cfg(test)]
#[path = "column_batch_tests.rs"]
mod tests;

/// 常量列折叠（Const Column Folding，2026-08-26，q13 列式化）。
///
/// 列内全部行同值（run of identical values）时，按行冗余存储（每行一个
/// cell + 每行一次 clone）是纯浪费——折叠为 `Const(T)` 单值 + 外部行数，
/// 读取时按行展开（O(1)），内存从 30M 份 → 1 份。`Rows(Vec<T>)` 保持
/// 每行独立值（列内真不同，如 wfx_id / summary / close 的 origin）。
///
/// 覆盖两种列：
/// - 系统字段列（`ColumnData<Arc<str>>`）：on-each 的 rule_name/entity_type/
///   origin/close_reason（4 列计划常量）→ `Const`（免每行 cell 与 Arc clone）；
///   emit_time/summary 跨批/规则语义变化、close 路径的 origin 等 per-row
///   → `Rows`（emit_time 按 nanos 缓存跨批不同，R1 2026-08-26）。
/// - yield 字面量列（`YieldCol::const_value`）：同样语义，values/metas 免
///   每行 cell，读时展开 const_value。
#[derive(Clone, ::moju_derive::MoJu)]
#[moju(kind = "state", domain = "Engine", module = "Engine.AlertOutput")]
enum ColumnData<T> {
    Const(T),
    Rows(Vec<T>),
}

impl<T> ColumnData<T> {
    /// 按行取单元格：`Const` 返回唯一值（行号无关），`Rows` 返回数组元素。
    fn at(&self, row: usize) -> &T {
        match self {
            ColumnData::Const(v) => v,
            ColumnData::Rows(v) => &v[row],
        }
    }
}

/// One finished columnar alert batch, addressed to a single yield target.
#[derive(::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.AlertOutput")]
pub struct AlertColumnBatch {
    target: Arc<str>,
    len: usize,
    /// Fixed system-field columns, in `to_data_record` order. `wfx_id` /
    /// `entity_id` are per-row owned [`SmolStr`]（2026-08-26：内联 ≤22B，零堆
    /// 分配——q13b per-row churn 消减），`fired_at` 是 `String`（ISO 时间戳 24
    /// 字符超内联上限）。列式直接路径 move 进列、零额外分配（`Arc` 反而要每行
    /// 一次分配 + 拷贝，且值从不共享）。常量列折叠（见 [`ColumnData`]）：
    /// **4 列计划常量**（rule_name/entity_type/origin/close_reason）→
    /// [`ColumnData::Const`]（免每行 cell 与 Arc clone）；**emit_time/summary
    /// 跨批/规则语义变化 → [`ColumnData::Rows`]**（emit_time 按 nanos 缓存，
    /// 不同批不同值；summary 在 match 规则 per-row——R1 2026-08-26 修正）。
    /// close 路径的 origin 等 per-row → [`ColumnData::Rows`]。
    wfx_id: Vec<SmolStr>,
    rule_name: ColumnData<Arc<str>>,
    score: Vec<f64>,
    entity_type: ColumnData<Arc<str>>,
    entity_id: Vec<SmolStr>,
    origin: ColumnData<Arc<str>>,
    close_reason: ColumnData<Arc<str>>,
    fired_at: Vec<String>,
    emit_time: ColumnData<Arc<str>>,
    summary: ColumnData<Arc<str>>,
    /// Yield columns (layout follows the first appended record).
    yield_cols: Vec<YieldCol>,
}

#[derive(::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.AlertOutput")]
struct YieldCol {
    name: Arc<str>,
    metas: Vec<DataType>,
    values: Vec<ModelValue>,
    /// 常量列折叠：字面量 yield（`alert_type = "q1_passthrough"`）注册时带
    /// 批级常量 cell——`values`/`metas` 免每行 push（Const 形态），读时按行
    /// 展开；非字面量字段为 `None`（Rows 形态，逐行 stage）。
    const_value: Option<(DataType, ModelValue)>,
}

impl YieldCol {
    /// 常量列折叠判定：字面量 yield = Const 形态（免每行 cell）。
    fn is_const_column(&self) -> bool {
        self.const_value.is_some()
    }
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
                ModelValue::from(self.rule_name.at(row).as_ref()),
            )));
            record.push(FieldStorage::from_owned(Field::new(
                DataType::Float,
                WFU_SCORE,
                ModelValue::from(self.score[row]),
            )));
            record.push(FieldStorage::from_owned(Field::new(
                DataType::Chars,
                WFU_ENTITY_TYPE,
                ModelValue::from(self.entity_type.at(row).as_ref()),
            )));
            record.push(FieldStorage::from_owned(Field::new(
                DataType::Chars,
                WFU_ENTITY_ID,
                ModelValue::from(self.entity_id[row].as_str()),
            )));
            record.push(FieldStorage::from_owned(Field::new(
                DataType::Chars,
                WFU_ORIGIN,
                ModelValue::from(self.origin.at(row).as_ref()),
            )));
            record.push(FieldStorage::from_owned(Field::new(
                DataType::Chars,
                WFU_CLOSE_REASON,
                ModelValue::from(self.close_reason.at(row).as_ref()),
            )));
            record.push(FieldStorage::from_owned(Field::new(
                DataType::Chars,
                WFU_FIRED_AT,
                ModelValue::from(self.fired_at[row].as_str()),
            )));
            record.push(FieldStorage::from_owned(Field::new(
                DataType::Chars,
                WFU_EMIT_TIME,
                ModelValue::from(self.emit_time.at(row).as_ref()),
            )));
            record.push(FieldStorage::from_owned(Field::new(
                DataType::Chars,
                WFU_SUMMARY,
                ModelValue::from(self.summary.at(row).as_ref()),
            )));
            for col in &self.yield_cols {
                // 常量列（字面量 yield）：values 免每行 cell，按行展开
                // `const_value`（meta/value 恒定，读时 clone 一次）。
                if let Some((meta, value)) = &col.const_value {
                    record.push(FieldStorage::from_owned(Field::new(
                        meta.clone(),
                        col.name.as_ref(),
                        value.clone(),
                    )));
                    continue;
                }
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
    pub wfx_id: SmolStr,
    pub score: f64,
    pub entity_id: SmolStr,
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
#[derive(::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.AlertOutput")]
pub struct AlertColumnBuilder {
    target: Arc<str>,
    len: usize,
    wfx_id: Vec<SmolStr>,
    rule_name: Option<ColumnData<Arc<str>>>,
    score: Vec<f64>,
    entity_type: Option<ColumnData<Arc<str>>>,
    entity_id: Vec<SmolStr>,
    origin: Option<ColumnData<Arc<str>>>,
    close_reason: Option<ColumnData<Arc<str>>>,
    fired_at: Vec<String>,
    emit_time: Option<ColumnData<Arc<str>>>,
    summary: Option<ColumnData<Arc<str>>>,
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
    /// 设置系统常量列（计划/批常量）：首次 set，后续忽略——信任批级常量
    /// 语义（rule_name/entity_type 等恒为计划常量；emit_time 跨段 Arc 可能
    /// 不同但值相同，不做每行校验）。每行一次 O(1) 分支，无每行 cell 与
    /// Arc clone（2026-08-26 常量列折叠：ColumnData::Const 单值存储）。
    fn set_system_const(slot: &mut Option<ColumnData<Arc<str>>>, value: &Arc<str>) {
        if slot.is_none() {
            *slot = Some(ColumnData::Const(Arc::clone(value)));
        }
    }
}
