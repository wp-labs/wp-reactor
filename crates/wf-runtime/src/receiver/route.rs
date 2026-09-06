use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBuilder, FixedSizeListArray, Float64Array, Int64Array,
    LargeListArray, ListArray, NullArray, PrimitiveBuilder, StringArray, StringBuilder,
    StructArray, TimestampNanosecondArray,
};
use arrow::datatypes::{
    DataType, Field, Float64Type, Int64Type, TimeUnit, TimestampNanosecondType,
};
use arrow::record_batch::RecordBatch;
use wf_data::time::parse_timestamp_str_nanos;
use wf_engine::match_engine::{
    self, WFL_FIELD_TYPE_ARRAY, WFL_FIELD_TYPE_OBJECT, wfl_structured_field_kind,
};
use wf_engine::window::Router;

pub(crate) fn batch_machine_id(batch: &RecordBatch) -> Option<String> {
    let idx = batch.schema().index_of(match_engine::MACHINE_ID).ok()?;
    let col = batch.column(idx);
    let arr = col.as_any().downcast_ref::<arrow::array::StringArray>()?;
    if arr.is_empty() {
        return None;
    }
    Some(arr.value(0).to_string())
}

/// Prepare a batch for routing: coerce columns to the target window schema when
/// the stream carries structured fields that need projection (otherwise a cheap
/// clone). Shared by the direct `route_batch` path and the R2 parse pool.
pub(crate) fn prepare_batch(
    stream_name: &str,
    batch: &RecordBatch,
    router: &Router,
) -> RecordBatch {
    if needs_projection_for_stream(stream_name, batch, router) {
        project_batch_for_stream(stream_name, batch, router)
    } else {
        batch.clone()
    }
}

/// Project a RecordBatch to match the first window's schema for the given stream.
/// Uses the window's actual schema (exact Field objects including metadata).
fn project_batch_for_stream(
    stream_name: &str,
    batch: &RecordBatch,
    router: &Router,
) -> RecordBatch {
    use arrow::array::NullArray;

    let subs = router.registry().subscribers_of(stream_name);
    if subs.is_empty() {
        return batch.clone();
    }

    // Use first window's exact schema as target
    let target_schema = subs.iter().find_map(|(window_name, _)| {
        router
            .registry()
            .get_window(window_name)
            .map(|win| win.schema().clone())
    });

    let Some(target_schema) = target_schema else {
        return batch.clone();
    };

    // Build columns matching the target schema order and types
    let mut fields: Vec<Arc<arrow::datatypes::Field>> =
        target_schema.fields().iter().cloned().collect();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(fields.len());
    for field in &fields {
        let col = match batch.column_by_name(field.name()) {
            Some(col) if col.data_type() == field.data_type() => col.clone(),
            Some(col) => coerce_column_for_field(col, field, batch.num_rows()),
            None => Arc::new(NullArray::new(batch.num_rows())),
        };
        columns.push(col);
    }
    // Preserve machine_id from source batch if present but not in target schema
    if target_schema.index_of(match_engine::MACHINE_ID).is_err()
        && let Some(col) = batch.column_by_name(match_engine::MACHINE_ID)
    {
        fields.push(Arc::new(arrow::datatypes::Field::new(
            match_engine::MACHINE_ID,
            col.data_type().clone(),
            true,
        )));
        columns.push(col.clone());
    }

    let schema = arrow::datatypes::Schema::new(fields);
    arrow::record_batch::RecordBatch::try_new(Arc::new(schema), columns)
        .unwrap_or_else(|_| batch.clone())
}

fn needs_projection_for_stream(stream_name: &str, batch: &RecordBatch, router: &Router) -> bool {
    let subs = router.registry().subscribers_of(stream_name);
    subs.iter().any(|(window_name, _)| {
        router
            .registry()
            .get_window(window_name)
            .map(|win| win.schema().clone())
            .is_some_and(|schema| {
                schema.fields().iter().any(|field| {
                    let target_kind = wfl_structured_field_kind(field);
                    target_kind.is_some()
                        && batch
                            .schema()
                            .field_with_name(field.name())
                            .is_ok_and(|source_field| {
                                source_field.data_type() != field.data_type()
                                    || wfl_structured_field_kind(source_field) != target_kind
                            })
                })
            })
    })
}

/// Coerce a column to the target Arrow type. Falls back to nulls if coercion fails.
pub(crate) fn coerce_column(col: &ArrayRef, target: &DataType, num_rows: usize) -> ArrayRef {
    if col.data_type() == target {
        // Same type — direct clone (should be handled by caller, but safe)
        return col.clone();
    }
    match col.data_type() {
        // Utf8 → numeric / boolean / timestamp
        DataType::Utf8 => coerce_utf8_to_target(col, target, num_rows),
        // Numeric → numeric / Utf8
        DataType::Int64 => coerce_int64_to_target(col, target, num_rows),
        DataType::Float64 => coerce_float64_to_target(col, target, num_rows),
        // Fallback — nulls
        _ => Arc::new(NullArray::new(num_rows)),
    }
}

/// Coerce a Utf8 column toward `target`; unparseable values become nulls and
/// unsupported targets degrade to an all-null column.
fn coerce_utf8_to_target(col: &ArrayRef, target: &DataType, num_rows: usize) -> ArrayRef {
    let strings = as_string_array(col);
    match target {
        DataType::Int64 => {
            fill_primitive::<Int64Type, _>(num_rows, |i| strings.value(i).parse().ok())
        }
        DataType::Float64 => {
            fill_primitive::<Float64Type, _>(num_rows, |i| strings.value(i).parse().ok())
        }
        DataType::Boolean => fill_boolean(num_rows, |i| {
            let value = strings.value(i);
            value.eq_ignore_ascii_case("true") || value == "1"
        }),
        DataType::Timestamp(TimeUnit::Nanosecond, None) => {
            fill_primitive::<TimestampNanosecondType, _>(num_rows, |i| {
                parse_timestamp_str_nanos(strings.value(i))
            })
        }
        _ => Arc::new(NullArray::new(num_rows)),
    }
}

/// Coerce an Int64 column toward `target`; unsupported targets degrade to nulls.
fn coerce_int64_to_target(col: &ArrayRef, target: &DataType, num_rows: usize) -> ArrayRef {
    let ints = as_primitive_array::<Int64Type>(col);
    match target {
        DataType::Float64 => {
            fill_primitive::<Float64Type, _>(num_rows, |i| Some(ints.value(i) as f64))
        }
        DataType::Utf8 => fill_utf8(num_rows, num_rows * 8, |i| ints.value(i).to_string()),
        _ => Arc::new(NullArray::new(num_rows)),
    }
}

/// Coerce a Float64 column toward `target`; unsupported targets degrade to nulls.
fn coerce_float64_to_target(col: &ArrayRef, target: &DataType, num_rows: usize) -> ArrayRef {
    let floats = as_primitive_array::<Float64Type>(col);
    match target {
        DataType::Int64 => {
            fill_primitive::<Int64Type, _>(num_rows, |i| Some(floats.value(i) as i64))
        }
        DataType::Utf8 => fill_utf8(num_rows, num_rows * 16, |i| floats.value(i).to_string()),
        _ => Arc::new(NullArray::new(num_rows)),
    }
}

/// Fill a primitive column from a per-row extractor; `None` rows become null.
fn fill_primitive<T, F>(num_rows: usize, value: F) -> ArrayRef
where
    T: arrow::datatypes::ArrowPrimitiveType,
    F: Fn(usize) -> Option<T::Native>,
{
    let mut builder = PrimitiveBuilder::<T>::with_capacity(num_rows);
    for i in 0..num_rows {
        match value(i) {
            Some(v) => builder.append_value(v),
            None => builder.append_null(),
        }
    }
    Arc::new(builder.finish())
}

/// Fill a Boolean column from a per-row extractor (every row carries a value).
fn fill_boolean(num_rows: usize, value: impl Fn(usize) -> bool) -> ArrayRef {
    let mut builder = BooleanBuilder::with_capacity(num_rows);
    for i in 0..num_rows {
        builder.append_value(value(i));
    }
    Arc::new(builder.finish())
}

/// Fill a Utf8 column from a per-row extractor.
fn fill_utf8(num_rows: usize, value_bytes: usize, value: impl Fn(usize) -> String) -> ArrayRef {
    let mut builder = StringBuilder::with_capacity(num_rows, value_bytes);
    for i in 0..num_rows {
        builder.append_value(value(i));
    }
    Arc::new(builder.finish())
}

pub(crate) fn coerce_column_for_field(col: &ArrayRef, target: &Field, num_rows: usize) -> ArrayRef {
    if matches!(target.data_type(), DataType::Utf8) {
        match (wfl_structured_field_kind(target), col.data_type()) {
            (Some(WFL_FIELD_TYPE_OBJECT), DataType::Struct(_))
            | (
                Some(WFL_FIELD_TYPE_ARRAY),
                DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _),
            ) => return structured_column_to_json_strings(col, num_rows),
            _ => {}
        }
    }
    coerce_column(col, target.data_type(), num_rows)
}

fn structured_column_to_json_strings(col: &ArrayRef, num_rows: usize) -> ArrayRef {
    use arrow::array::StringBuilder;

    let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 32);
    for row in 0..num_rows {
        if col.is_null(row) {
            builder.append_null();
            continue;
        }
        match arrow_value_to_json(col.as_ref(), row)
            .and_then(|value| serde_json::to_string(&value).ok())
        {
            Some(value) => builder.append_value(value),
            None => builder.append_null(),
        }
    }
    Arc::new(builder.finish())
}

/// Convert one cell of an Arrow column into a JSON value.
///
/// The dispatch stays flat: every arm delegates to a small per-type converter
/// that downcasts and formats a single row. Unsupported types yield `None`.
fn arrow_value_to_json(col: &dyn Array, row: usize) -> Option<serde_json::Value> {
    match col.data_type() {
        DataType::Int64 => int64_cell_to_json(col, row),
        DataType::Float64 => float64_cell_to_json(col, row),
        DataType::Utf8 => utf8_cell_to_json(col, row),
        DataType::Boolean => boolean_cell_to_json(col, row),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => timestamp_cell_to_json(col, row),
        DataType::Struct(_) => struct_cell_to_json(col, row),
        DataType::List(_) => list_cell_to_json(col, row),
        DataType::LargeList(_) => large_list_cell_to_json(col, row),
        DataType::FixedSizeList(_, _) => fixed_size_list_cell_to_json(col, row),
        _ => None,
    }
}

fn int64_cell_to_json(col: &dyn Array, row: usize) -> Option<serde_json::Value> {
    let arr = col.as_any().downcast_ref::<Int64Array>()?;
    Some(serde_json::Value::Number(arr.value(row).into()))
}

fn float64_cell_to_json(col: &dyn Array, row: usize) -> Option<serde_json::Value> {
    let arr = col.as_any().downcast_ref::<Float64Array>()?;
    serde_json::Number::from_f64(arr.value(row)).map(serde_json::Value::Number)
}

fn utf8_cell_to_json(col: &dyn Array, row: usize) -> Option<serde_json::Value> {
    let arr = col.as_any().downcast_ref::<StringArray>()?;
    Some(serde_json::Value::String(arr.value(row).to_string()))
}

fn boolean_cell_to_json(col: &dyn Array, row: usize) -> Option<serde_json::Value> {
    let arr = col.as_any().downcast_ref::<BooleanArray>()?;
    Some(serde_json::Value::Bool(arr.value(row)))
}

fn timestamp_cell_to_json(col: &dyn Array, row: usize) -> Option<serde_json::Value> {
    let arr = col.as_any().downcast_ref::<TimestampNanosecondArray>()?;
    Some(serde_json::Value::Number(arr.value(row).into()))
}

fn struct_cell_to_json(col: &dyn Array, row: usize) -> Option<serde_json::Value> {
    let arr = col.as_any().downcast_ref::<StructArray>()?;
    let mut object = serde_json::Map::new();
    for (field, child) in arr.fields().iter().zip(arr.columns()) {
        if child.is_null(row) {
            continue;
        }
        if let Some(value) = arrow_value_to_json(child.as_ref(), row) {
            object.insert(field.name().clone(), value);
        }
    }
    Some(serde_json::Value::Object(object))
}

fn list_cell_to_json(col: &dyn Array, row: usize) -> Option<serde_json::Value> {
    let arr = col.as_any().downcast_ref::<ListArray>()?;
    Some(serde_json::Value::Array(arrow_list_values_to_json(
        arr.value(row).as_ref(),
    )))
}

fn large_list_cell_to_json(col: &dyn Array, row: usize) -> Option<serde_json::Value> {
    let arr = col.as_any().downcast_ref::<LargeListArray>()?;
    Some(serde_json::Value::Array(arrow_list_values_to_json(
        arr.value(row).as_ref(),
    )))
}

fn fixed_size_list_cell_to_json(col: &dyn Array, row: usize) -> Option<serde_json::Value> {
    let arr = col.as_any().downcast_ref::<FixedSizeListArray>()?;
    Some(serde_json::Value::Array(arrow_list_values_to_json(
        arr.value(row).as_ref(),
    )))
}

fn arrow_list_values_to_json(values: &dyn Array) -> Vec<serde_json::Value> {
    let mut out = Vec::with_capacity(values.len());
    for idx in 0..values.len() {
        if values.is_null(idx) {
            continue;
        }
        if let Some(value) = arrow_value_to_json(values, idx) {
            out.push(value);
        }
    }
    out
}

#[allow(dead_code)]
fn as_string_array(col: &ArrayRef) -> &arrow::array::StringArray {
    col.as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .expect("expected StringArray")
}

#[allow(dead_code)]
fn as_primitive_array<T: arrow::datatypes::ArrowPrimitiveType>(
    col: &ArrayRef,
) -> &arrow::array::PrimitiveArray<T> {
    col.as_any()
        .downcast_ref::<arrow::array::PrimitiveArray<T>>()
        .expect("expected PrimitiveArray")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coerce_float64_to_utf8_and_bad_timestamp_null() {
        // Float64 → Utf8 字符串化（补上该转换方向）
        let floats: ArrayRef = Arc::new(Float64Array::from(vec![1.5, -2.0]));
        let text = coerce_column(&floats, &DataType::Utf8, 2);
        let text = text.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(text.value(0), "1.5");
        assert_eq!(text.value(1), "-2");

        // Utf8 → Timestamp：非法串落 null（合法值已由 receiver_tests 覆盖）
        let stamps: ArrayRef = Arc::new(StringArray::from(vec![
            "2023-11-14T22:13:20Z",
            "not-a-time",
        ]));
        let ts = coerce_column(&stamps, &DataType::Timestamp(TimeUnit::Nanosecond, None), 2);
        let ts = ts
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        assert_eq!(ts.value(0), 1_700_000_000_000_000_000);
        assert!(ts.is_null(1));
    }

    #[test]
    fn arrow_cell_to_json_covers_scalar_cells() {
        let ints = Int64Array::from(vec![7]);
        assert_eq!(
            arrow_value_to_json(&ints, 0),
            Some(serde_json::Value::from(7i64))
        );
        let floats = Float64Array::from(vec![1.5]);
        assert_eq!(
            arrow_value_to_json(&floats, 0),
            Some(serde_json::Value::from(1.5))
        );
        let bools = BooleanArray::from(vec![true]);
        assert_eq!(
            arrow_value_to_json(&bools, 0),
            Some(serde_json::Value::Bool(true))
        );
        let strings = StringArray::from(vec!["hello"]);
        assert_eq!(
            arrow_value_to_json(&strings, 0),
            Some(serde_json::Value::String("hello".to_string()))
        );
        let stamps = TimestampNanosecondArray::from(vec![1_700_000_000_000_000_000]);
        assert_eq!(
            arrow_value_to_json(&stamps, 0),
            Some(serde_json::Value::from(1_700_000_000_000_000_000i64))
        );
        // 不支持的类型 → None（不 panic、不误报）
        let nulls = NullArray::new(1);
        assert_eq!(arrow_value_to_json(&nulls, 0), None);
    }
}
