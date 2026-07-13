use std::sync::Arc;

use arrow::array::{Array, ArrayRef};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use wf_data::time::parse_timestamp_str_nanos;
use wf_engine::match_engine;
use wf_engine::window::Router;

use crate::error::{RuntimeReason, RuntimeResult};
use crate::metrics::RuntimeMetrics;
use orion_error::conversion::ToStructError;

pub(crate) fn route_batch(
    stream_name: &str,
    source_name: &str,
    batch: RecordBatch,
    router: &Router,
    metrics: Option<&Arc<RuntimeMetrics>>,
) -> RuntimeResult<()> {
    if let Some(metrics) = metrics {
        metrics.add_receiver_frame(batch.num_rows());
        metrics.add_receiver_source_frame(source_name, batch.num_rows());
        let machine_id = batch_machine_id(&batch).unwrap_or_else(|| source_name.to_string());
        metrics.add_receiver_source_machine_rows(source_name, &machine_id, batch.num_rows());
        metrics.inc_router_route_call();
    }
    wf_debug!(
        pipe,
        stream = stream_name,
        rows = batch.num_rows(),
        "frame decoded"
    );
    // Try routing directly; if schema mismatch, attempt projection
    let report = match router.route(stream_name, batch.clone()) {
        Ok(report) => report,
        Err(_) => {
            // Project batch to match window schemas for this stream
            let projected = project_batch_for_stream(stream_name, &batch, router);
            router
                .route(stream_name, projected)
                .map_err(|e| RuntimeReason::data_error().to_err().with_source(e))?
        }
    };
    if let Some(metrics) = metrics {
        metrics.add_route_report(&report);
    }
    wf_debug!(
        pipe,
        delivered = report.delivered,
        dropped_late = report.dropped_late,
        skipped = report.skipped_non_local,
        "route report"
    );
    Ok(())
}

pub(crate) fn batch_machine_id(batch: &RecordBatch) -> Option<String> {
    let idx = batch.schema().index_of(match_engine::MACHINE_ID).ok()?;
    let col = batch.column(idx);
    let arr = col.as_any().downcast_ref::<arrow::array::StringArray>()?;
    if arr.is_empty() {
        return None;
    }
    Some(arr.value(0).to_string())
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
            Some(col) => coerce_column(col, field.data_type(), batch.num_rows()),
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
