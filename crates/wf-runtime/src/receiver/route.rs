use std::sync::Arc;

use arrow::array::{Array, ArrayRef};
use arrow::datatypes::{DataType, Field, TimeUnit};
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
            .and_then(|w| w.read().ok().map(|win| win.schema().clone()))
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
            .and_then(|w| w.read().ok().map(|win| win.schema().clone()))
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
    use arrow::array::*;

    match (col.data_type(), target) {
        // Same type — direct clone (should be handled by caller, but safe)
        (src, tgt) if src == tgt => col.clone(),

        // Utf8 → numeric / boolean / timestamp
        (DataType::Utf8, DataType::Int64) => {
            let strings = as_string_array(col);
            let mut builder = Int64Builder::with_capacity(num_rows);
            for i in 0..num_rows {
                match strings.value(i).parse::<i64>() {
                    Ok(v) => builder.append_value(v),
                    Err(_) => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        (DataType::Utf8, DataType::Float64) => {
            let strings = as_string_array(col);
            let mut builder = Float64Builder::with_capacity(num_rows);
            for i in 0..num_rows {
                match strings.value(i).parse::<f64>() {
                    Ok(v) => builder.append_value(v),
                    Err(_) => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        (DataType::Utf8, DataType::Boolean) => {
            let strings = as_string_array(col);
            let mut builder = BooleanBuilder::with_capacity(num_rows);
            for i in 0..num_rows {
                let v = strings.value(i);
                builder.append_value(v.eq_ignore_ascii_case("true") || v == "1");
            }
            Arc::new(builder.finish())
        }
        (DataType::Utf8, DataType::Timestamp(TimeUnit::Nanosecond, None)) => {
            let strings = as_string_array(col);
            let mut builder = TimestampNanosecondBuilder::with_capacity(num_rows);
            for i in 0..num_rows {
                let v = strings.value(i);
                match parse_timestamp_str_nanos(v) {
                    Some(v) => builder.append_value(v),
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }

        // Numeric → numeric
        (DataType::Int64, DataType::Float64) => {
            let ints = as_primitive_array::<arrow::datatypes::Int64Type>(col);
            let mut builder = Float64Builder::with_capacity(num_rows);
            for i in 0..num_rows {
                builder.append_value(ints.value(i) as f64);
            }
            Arc::new(builder.finish())
        }
        (DataType::Float64, DataType::Int64) => {
            let floats = as_primitive_array::<arrow::datatypes::Float64Type>(col);
            let mut builder = Int64Builder::with_capacity(num_rows);
            for i in 0..num_rows {
                builder.append_value(floats.value(i) as i64);
            }
            Arc::new(builder.finish())
        }

        // Numeric → Utf8
        (DataType::Int64, DataType::Utf8) => {
            let ints = as_primitive_array::<arrow::datatypes::Int64Type>(col);
            let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 8);
            for i in 0..num_rows {
                builder.append_value(ints.value(i).to_string());
            }
            Arc::new(builder.finish()) as ArrayRef
        }
        (DataType::Float64, DataType::Utf8) => {
            let floats = as_primitive_array::<arrow::datatypes::Float64Type>(col);
            let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 16);
            for i in 0..num_rows {
                builder.append_value(floats.value(i).to_string());
            }
            Arc::new(builder.finish()) as ArrayRef
        }

        // Fallback — nulls
        _ => Arc::new(NullArray::new(num_rows)),
    }
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

fn arrow_value_to_json(col: &dyn Array, row: usize) -> Option<serde_json::Value> {
    use arrow::array::{
        BooleanArray, FixedSizeListArray, Float64Array, Int64Array, LargeListArray, ListArray,
        StringArray, StructArray, TimestampNanosecondArray,
    };

    match col.data_type() {
        DataType::Int64 => {
            let arr = col.as_any().downcast_ref::<Int64Array>()?;
            Some(serde_json::Value::Number(arr.value(row).into()))
        }
        DataType::Float64 => {
            let arr = col.as_any().downcast_ref::<Float64Array>()?;
            serde_json::Number::from_f64(arr.value(row)).map(serde_json::Value::Number)
        }
        DataType::Utf8 => {
            let arr = col.as_any().downcast_ref::<StringArray>()?;
            Some(serde_json::Value::String(arr.value(row).to_string()))
        }
        DataType::Boolean => {
            let arr = col.as_any().downcast_ref::<BooleanArray>()?;
            Some(serde_json::Value::Bool(arr.value(row)))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let arr = col.as_any().downcast_ref::<TimestampNanosecondArray>()?;
            Some(serde_json::Value::Number(arr.value(row).into()))
        }
        DataType::Struct(_) => {
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
        DataType::List(_) => {
            let arr = col.as_any().downcast_ref::<ListArray>()?;
            Some(serde_json::Value::Array(arrow_list_values_to_json(
                arr.value(row).as_ref(),
            )))
        }
        DataType::LargeList(_) => {
            let arr = col.as_any().downcast_ref::<LargeListArray>()?;
            Some(serde_json::Value::Array(arrow_list_values_to_json(
                arr.value(row).as_ref(),
            )))
        }
        DataType::FixedSizeList(_, _) => {
            let arr = col.as_any().downcast_ref::<FixedSizeListArray>()?;
            Some(serde_json::Value::Array(arrow_list_values_to_json(
                arr.value(row).as_ref(),
            )))
        }
        _ => None,
    }
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
