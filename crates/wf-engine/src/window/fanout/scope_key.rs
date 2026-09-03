//! columnar scope-key 直读（rule_shards）：从批列直读原生 Arrow 值构 typed
//! `ScopeKey`（不经 `Value` 舍入、与行式路径同 variant），fanout 分片
//! （`partition_rows_by_key`）与机器 / 归并键共用同一键序；`column_scalar`
//! 是其它类型列的逐行回退（与行式 `extract_field_value` 字节一致）。

use arrow::record_batch::RecordBatch;

use crate::match_engine::event_bridge::extract_field_value;
use crate::match_engine::{ScopeKey, Value};

/// Extract a field from a batch at a pre-resolved column index, byte-identical
/// to the row-based `Event.fields.get(name)` used by [`extract_key_simple`].
fn column_scalar(batch: &RecordBatch, col_idx: usize, row: usize) -> Option<Value> {
    let col = batch.column(col_idx);
    if col.is_null(row) {
        return None;
    }
    extract_field_value(batch.schema().field(col_idx), col.as_ref(), row)
}

/// Build a [`ScopeKey`] from a batch column at `row` (columnar key path), **without
/// rounding through [`Value`]** — reads the native Arrow value straight into the
/// typed key. Produces the **same** variant as `ScopeKey::from_value` on the
/// row-based `Value`, so both paths shard identically.
///
/// Returns `None` when the cell is null / missing (row → shard 0). Unsupported
/// column types fall back to reading via [`column_scalar`] → [`ScopeKey::from_value`]
/// so they still shard deterministically.
pub(crate) fn scope_key_from_column(
    batch: &RecordBatch,
    col_idx: usize,
    row: usize,
) -> Option<ScopeKey> {
    use arrow::datatypes::{DataType, TimeUnit};
    let col = batch.column(col_idx);
    if col.is_null(row) {
        return None;
    }
    match col.data_type() {
        DataType::Int64 => col
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .map(|a| ScopeKey::Int(a.value(row))),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => col
            .as_any()
            .downcast_ref::<arrow::array::TimestampNanosecondArray>()
            .map(|a| ScopeKey::Int(a.value(row))),
        DataType::Float64 => {
            let v = col
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .map(|a| a.value(row));
            v.map(|f| ScopeKey::from_value(&Value::Number(f)))
        }
        DataType::Utf8 => col
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .map(|a| ScopeKey::Str(a.value(row).into())),
        DataType::Boolean => col
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .map(|a| ScopeKey::Str(if a.value(row) { "true" } else { "false" }.into())),
        _ => column_scalar(batch, col_idx, row).map(|v| ScopeKey::from_value(&v)),
    }
}

/// Build a [`ScopeKey`] for a row's match-key fields, in plan field order. `None`
/// iff any key column is null / missing (row lands shard 0).
pub(crate) fn scope_key_columnar(
    batch: &RecordBatch,
    col_idx: &[usize],
    row: usize,
) -> Option<ScopeKey> {
    let mut acc: Option<ScopeKey> = None;
    for &ci in col_idx {
        let v = scope_key_from_column(batch, ci, row)?;
        acc = Some(match acc {
            None => v,
            Some(prev) => ScopeKey::Pair(Box::new(prev), Box::new(v)),
        });
    }
    Some(acc.unwrap_or(ScopeKey::Empty))
}
