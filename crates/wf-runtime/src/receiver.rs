use std::io;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::ipc::reader::FileReader;
use arrow::record_batch::RecordBatch;
use orion_error::conversion::{SourceErr, SourceRawErr, ToStructError};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio_util::sync::CancellationToken;
use wf_engine::window::Router;
use wf_lang::{BaseType, FieldType, WindowSchema};

use crate::error::{RuntimeReason, RuntimeResult};
use crate::metrics::RuntimeMetrics;

/// Replay NDJSON events from file and route them into the runtime as one
/// configured stream.
///
/// Each line must be a JSON object whose field names match the subscribed
/// window schema for `stream_name`.
pub async fn replay_ndjson_file(
    path: &Path,
    stream_name: &str,
    source_name: &str,
    schemas: &[WindowSchema],
    router: Arc<Router>,
    metrics: Option<Arc<RuntimeMetrics>>,
    cancel: CancellationToken,
) -> RuntimeResult<()> {
    const FILE_BATCH_ROWS: usize = 2048;

    let schema = resolve_stream_schema(schemas, stream_name)?;
    let file = tokio::fs::File::open(path).await.source_err(
        RuntimeReason::system_error(),
        format!("open file source {}", path.display()),
    )?;
    let mut lines = BufReader::new(file).lines();
    let mut rows: Vec<serde_json::Map<String, serde_json::Value>> =
        Vec::with_capacity(FILE_BATCH_ROWS);
    let mut line_no = 0usize;
    let mut total_rows = 0usize;

    wf_info!(
        conn,
        source = %path.display(),
        stream = stream_name,
        "starting file source replay"
    );
    if let Some(metrics) = &metrics {
        metrics.inc_receiver_connection();
    }

    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            next = lines.next_line() => {
                let Some(line) = next
                    .source_err(RuntimeReason::system_error(), format!("read file source {}", path.display()))?
                else { break };
                line_no += 1;
                if line.trim().is_empty() {
                    continue;
                }
                let value: serde_json::Value = serde_json::from_str(&line).source_err(
                    RuntimeReason::data_error(),
                    format!("invalid NDJSON at {}:{}", path.display(), line_no),
                )?;
                let Some(obj) = value.as_object() else {
                    return RuntimeReason::data_error()
                        .to_err()
                        .with_detail(format!(
                            "invalid NDJSON at {}:{}: expected JSON object",
                            path.display(),
                            line_no
                        ))
                        .err();
                };
                rows.push(obj.clone());
                if rows.len() >= FILE_BATCH_ROWS {
                    let batch = build_record_batch_from_json(&schema, &rows)?;
                    total_rows += batch.num_rows();
                    if let Err(e) = route_batch(stream_name, source_name, batch, router.as_ref(), metrics.as_ref()) {
                        if let Some(metrics) = &metrics {
                            metrics.inc_route_error(source_name);
                        }
                        return Err(e);
                    }
                    rows.clear();
                }
            }
        }
    }

    if !rows.is_empty() {
        let batch = build_record_batch_from_json(&schema, &rows)?;
        total_rows += batch.num_rows();
        if let Err(e) = route_batch(
            stream_name,
            source_name,
            batch,
            router.as_ref(),
            metrics.as_ref(),
        ) {
            if let Some(metrics) = &metrics {
                metrics.inc_route_error(source_name);
            }
            return Err(e);
        }
    }

    wf_info!(
        conn,
        source = %path.display(),
        stream = stream_name,
        rows = total_rows,
        "file source replay complete"
    );
    Ok(())
}

/// Replay CSV data from file and route into the runtime as one stream.
///
/// CSV headers must match schema field names. Each row is converted to a
/// RecordBatch using the same column builder as NDJSON.
pub async fn replay_csv_file(
    path: &Path,
    stream_name: &str,
    source_name: &str,
    schemas: &[WindowSchema],
    router: Arc<Router>,
    metrics: Option<Arc<RuntimeMetrics>>,
    cancel: CancellationToken,
) -> RuntimeResult<()> {
    let schema = resolve_stream_schema(schemas, stream_name)?;
    let file_path = path.to_path_buf();
    let stream_name = stream_name.to_string();
    const FILE_BATCH_ROWS_CSV: usize = 2048;

    wf_info!(
        conn,
        source = %path.display(),
        stream = stream_name,
        "starting csv file replay"
    );

    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_path(&file_path)
        .map_err(|e| {
            RuntimeReason::system_error().to_err().with_detail(format!(
                "open csv source {}: {}",
                path.display(),
                e
            ))
        })?;

    let headers: Vec<String> = reader
        .headers()
        .map_err(|e| {
            RuntimeReason::data_error().to_err().with_detail(format!(
                "read csv headers from {}: {}",
                path.display(),
                e
            ))
        })?
        .iter()
        .map(|h| h.to_string())
        .collect();

    let mut total_rows = 0usize;
    let mut rows: Vec<serde_json::Map<String, serde_json::Value>> =
        Vec::with_capacity(FILE_BATCH_ROWS_CSV);

    for result in reader.records() {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = async {} => {}
        }
        let record = result.map_err(|e| {
            RuntimeReason::system_error().to_err().with_detail(format!(
                "read csv record from {}: {}",
                path.display(),
                e
            ))
        })?;

        let mut map = serde_json::Map::new();
        for (i, value) in record.iter().enumerate() {
            let field = headers
                .get(i)
                .cloned()
                .unwrap_or_else(|| format!("col_{}", i));
            map.insert(field, serde_json::Value::String(value.to_string()));
        }

        rows.push(map);
        total_rows += 1;

        if rows.len() >= FILE_BATCH_ROWS_CSV {
            let batch = build_record_batch_from_json(&schema, &rows)?;
            if let Err(e) = route_batch(
                &stream_name,
                source_name,
                batch,
                router.as_ref(),
                metrics.as_ref(),
            ) {
                if let Some(metrics) = &metrics {
                    metrics.inc_route_error(source_name);
                }
                return Err(e);
            }
            rows.clear();
        }
    }

    if !rows.is_empty() {
        let batch = build_record_batch_from_json(&schema, &rows)?;
        if let Err(e) = route_batch(
            &stream_name,
            source_name,
            batch,
            router.as_ref(),
            metrics.as_ref(),
        ) {
            if let Some(metrics) = &metrics {
                metrics.inc_route_error(source_name);
            }
            return Err(e);
        }
    }

    wf_info!(
        conn,
        source = %path.display(),
        stream = stream_name,
        rows = total_rows,
        "csv file replay complete"
    );
    Ok(())
}

/// Replay framed `wp_arrow` IPC records from file and route them into the
/// runtime.
pub async fn replay_arrow_framed_file(
    path: &Path,
    stream_name: &str,
    source_name: &str,
    schemas: &[WindowSchema],
    router: Arc<Router>,
    metrics: Option<Arc<RuntimeMetrics>>,
    cancel: CancellationToken,
) -> RuntimeResult<()> {
    let path = path.to_path_buf();
    let stream_override = (!stream_name.trim().is_empty()).then(|| stream_name.to_string());

    wf_info!(
        conn,
        source = %path.display(),
        stream = stream_name,
        "starting arrow file replay"
    );
    if let Some(metrics) = &metrics {
        metrics.inc_receiver_connection();
    }

    let mut file = tokio::fs::File::open(&path).await.source_err(
        RuntimeReason::system_error(),
        format!("open arrow source {}", path.display()),
    )?;
    let mut total_rows = 0usize;

    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            next = read_frame(&mut file) => {
                let Some(payload) = next.source_err(
                    RuntimeReason::system_error(),
                    format!("read arrow frame from {}", path.display()),
                )? else {
                    break;
                };

                let frame = wp_arrow::ipc::decode_ipc(&payload)
                    .source_raw_err(
                        RuntimeReason::data_error(),
                        format!("decode arrow frame from {}", path.display()),
                    )?;
                let stream = stream_override.as_deref().unwrap_or(frame.tag.as_str());
                validate_batch_schema_for_stream(schemas, stream, frame.batch.schema().as_ref())?;

                total_rows += frame.batch.num_rows();
                if let Err(e) = route_batch(stream, source_name, frame.batch, router.as_ref(), metrics.as_ref()) {
                    if let Some(metrics) = &metrics {
                        metrics.inc_route_error(source_name);
                    }
                    return Err(e);
                }
            }
        }
    }

    wf_info!(
        conn,
        source = %path.display(),
        stream = stream_name,
        rows = total_rows,
        "arrow file replay complete"
    );
    Ok(())
}

/// Replay standard Arrow IPC file batches and route them into the runtime as
/// one configured stream.
pub async fn replay_arrow_ipc_file(
    path: &Path,
    stream_name: &str,
    source_name: &str,
    schemas: &[WindowSchema],
    router: Arc<Router>,
    metrics: Option<Arc<RuntimeMetrics>>,
    cancel: CancellationToken,
) -> RuntimeResult<()> {
    let path = path.to_path_buf();
    let stream_name = stream_name.to_string();
    let expected_schema = resolve_stream_schema(schemas, &stream_name)?;

    wf_info!(
        conn,
        source = %path.display(),
        stream = stream_name,
        "starting arrow ipc file replay"
    );
    if let Some(metrics) = &metrics {
        metrics.inc_receiver_connection();
    }

    let path_for_read = path.clone();
    let stream_for_read = stream_name.clone();
    let source_for_read = source_name.to_string();
    let routed_rows = tokio::task::spawn_blocking(move || -> RuntimeResult<usize> {
        let file = std::fs::File::open(&path_for_read).source_err(
            RuntimeReason::system_error(),
            format!("open arrow ipc source {}", path_for_read.display()),
        )?;
        let mut reader = FileReader::try_new(file, None).source_raw_err(
            RuntimeReason::data_error(),
            format!("read arrow ipc source {}", path_for_read.display()),
        )?;

        let file_schema = reader.schema();
        if file_schema.as_ref() != expected_schema.as_ref() {
            return RuntimeReason::data_error()
                .to_err()
                .with_detail(format!(
                    "arrow ipc source {} schema mismatch for stream {:?}",
                    path_for_read.display(),
                    stream_for_read
                ))
                .err();
        }

        let mut total_rows = 0usize;
        for batch in &mut reader {
            if cancel.is_cancelled() {
                break;
            }
            let batch = batch.source_raw_err(
                RuntimeReason::data_error(),
                format!("read arrow ipc batch from {}", path_for_read.display()),
            )?;
            total_rows += batch.num_rows();
            if let Err(e) = route_batch(
                &stream_for_read,
                &source_for_read,
                batch,
                router.as_ref(),
                metrics.as_ref(),
            ) {
                if let Some(metrics) = &metrics {
                    metrics.inc_route_error(&source_for_read);
                }
                return Err(e);
            }
        }
        Ok(total_rows)
    })
    .await
    .source_raw_err(RuntimeReason::system_error(), "join arrow ipc replay task")??;

    wf_info!(
        conn,
        source = %path.display(),
        stream = stream_name,
        rows = routed_rows,
        "arrow ipc file replay complete"
    );
    Ok(())
}

fn validate_batch_schema_for_stream(
    schemas: &[WindowSchema],
    stream_name: &str,
    batch_schema: &Schema,
) -> RuntimeResult<()> {
    let expected = resolve_stream_schema(schemas, stream_name)?;
    if expected.as_ref() != batch_schema {
        return RuntimeReason::data_error()
            .to_err()
            .with_detail(format!(
                "arrow source schema mismatch for stream {:?}",
                stream_name
            ))
            .err();
    }
    Ok(())
}

pub(crate) fn resolve_stream_schema(
    schemas: &[WindowSchema],
    stream_name: &str,
) -> RuntimeResult<SchemaRef> {
    let mut schema: Option<SchemaRef> = None;
    for ws in schemas {
        if !ws.streams.iter().any(|s| s == stream_name) {
            continue;
        }
        let candidate = window_schema_to_arrow(ws)?;
        if let Some(existing) = &schema {
            if existing.as_ref() != candidate.as_ref() {
                return RuntimeReason::data_error()
                    .to_err()
                    .with_detail(format!(
                        "stream {:?} maps to inconsistent schemas (window {:?})",
                        stream_name, ws.name
                    ))
                    .err();
            }
        } else {
            schema = Some(candidate);
        }
    }
    schema.ok_or_else(|| {
        RuntimeReason::data_error()
            .to_err()
            .with_detail(format!("no schema subscribed for stream {:?}", stream_name))
    })
}

fn window_schema_to_arrow(ws: &WindowSchema) -> RuntimeResult<SchemaRef> {
    let mut fields = Vec::with_capacity(ws.fields.len());
    for field in &ws.fields {
        fields.push(Field::new(
            &field.name,
            field_type_to_arrow(&field.field_type),
            true,
        ));
    }
    Ok(Arc::new(Schema::new(fields)))
}

fn field_type_to_arrow(ft: &FieldType) -> DataType {
    match ft {
        FieldType::Base(base) => base_type_to_arrow(base),
        FieldType::Array(base) => {
            DataType::List(Arc::new(Field::new("item", base_type_to_arrow(base), true)))
        }
    }
}

fn base_type_to_arrow(base: &BaseType) -> DataType {
    match base {
        BaseType::Chars | BaseType::Ip | BaseType::Hex => DataType::Utf8,
        BaseType::Digit => DataType::Int64,
        BaseType::Float => DataType::Float64,
        BaseType::Bool => DataType::Boolean,
        BaseType::Time => DataType::Timestamp(TimeUnit::Nanosecond, None),
    }
}

fn batch_machine_id(batch: &RecordBatch) -> Option<String> {
    let idx = batch
        .schema()
        .index_of(wf_engine::match_engine::MACHINE_ID)
        .ok()?;
    let col = batch.column(idx);
    let arr = col.as_any().downcast_ref::<arrow::array::StringArray>()?;
    if arr.is_empty() {
        return None;
    }
    Some(arr.value(0).to_string())
}

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
    if target_schema
        .index_of(wf_engine::match_engine::MACHINE_ID)
        .is_err()
        && let Some(col) = batch.column_by_name(wf_engine::match_engine::MACHINE_ID)
    {
        fields.push(Arc::new(arrow::datatypes::Field::new(
            wf_engine::match_engine::MACHINE_ID,
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
fn coerce_column(col: &ArrayRef, target: &arrow::datatypes::DataType, num_rows: usize) -> ArrayRef {
    use arrow::array::*;
    use arrow::datatypes::DataType;

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
        (DataType::Utf8, DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None)) => {
            let strings = as_string_array(col);
            let mut builder = TimestampNanosecondBuilder::with_capacity(num_rows);
            for i in 0..num_rows {
                let v = strings.value(i);
                let ns = chrono::DateTime::parse_from_rfc3339(v)
                    .ok()
                    .or_else(|| {
                        chrono::NaiveDateTime::parse_from_str(v, "%Y-%m-%d %H:%M:%S")
                            .ok()
                            .map(|dt| dt.and_utc().fixed_offset())
                    })
                    .and_then(|dt| dt.timestamp_nanos_opt());
                match ns {
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
fn as_string_array(col: &ArrayRef) -> &StringArray {
    col.as_any()
        .downcast_ref::<StringArray>()
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

pub(crate) fn build_record_batch_from_json(
    schema: &SchemaRef,
    rows: &[serde_json::Map<String, serde_json::Value>],
) -> RuntimeResult<RecordBatch> {
    let mut builders: Vec<ColumnBuilder> = schema
        .fields()
        .iter()
        .map(|f| ColumnBuilder::new(f.data_type(), rows.len()))
        .collect::<RuntimeResult<Vec<_>>>()?;
    for row in rows {
        for (idx, field) in schema.fields().iter().enumerate() {
            builders[idx].push(row.get(field.name()))?;
        }
    }
    let columns: Vec<ArrayRef> = builders.into_iter().map(ColumnBuilder::finish).collect();
    RecordBatch::try_new(schema.clone(), columns).source_raw_err(
        RuntimeReason::data_error(),
        "build file source record batch",
    )
}

enum ColumnBuilder {
    Utf8(Vec<Option<String>>),
    Int64(Vec<Option<i64>>),
    Float64(Vec<Option<f64>>),
    Bool(Vec<Option<bool>>),
    TimeNanos(Vec<Option<i64>>),
}

impl ColumnBuilder {
    fn new(data_type: &DataType, cap: usize) -> RuntimeResult<Self> {
        Ok(match data_type {
            DataType::Utf8 => Self::Utf8(Vec::with_capacity(cap)),
            DataType::Int64 => Self::Int64(Vec::with_capacity(cap)),
            DataType::Float64 => Self::Float64(Vec::with_capacity(cap)),
            DataType::Boolean => Self::Bool(Vec::with_capacity(cap)),
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                Self::TimeNanos(Vec::with_capacity(cap))
            }
            _ => {
                return RuntimeReason::data_error()
                    .to_err()
                    .with_detail(format!("unsupported file-source field type: {data_type:?}"))
                    .err();
            }
        })
    }

    fn push(&mut self, value: Option<&serde_json::Value>) -> RuntimeResult<()> {
        match self {
            Self::Utf8(col) => col.push(parse_utf8(value)),
            Self::Int64(col) => col.push(parse_i64(value)),
            Self::Float64(col) => col.push(parse_f64(value)),
            Self::Bool(col) => col.push(parse_bool(value)),
            Self::TimeNanos(col) => col.push(parse_i64(value)),
        }
        Ok(())
    }

    fn finish(self) -> ArrayRef {
        match self {
            Self::Utf8(col) => Arc::new(StringArray::from(col)),
            Self::Int64(col) => Arc::new(Int64Array::from(col)),
            Self::Float64(col) => Arc::new(Float64Array::from(col)),
            Self::Bool(col) => Arc::new(BooleanArray::from(col)),
            Self::TimeNanos(col) => Arc::new(TimestampNanosecondArray::from(col)),
        }
    }
}

fn parse_utf8(v: Option<&serde_json::Value>) -> Option<String> {
    let v = v?;
    match v {
        serde_json::Value::Null => None,
        serde_json::Value::String(s) => Some(s.clone()),
        _ => Some(v.to_string()),
    }
}

fn parse_i64(v: Option<&serde_json::Value>) -> Option<i64> {
    let v = v?;
    match v {
        serde_json::Value::Number(n) => n.as_i64(),
        serde_json::Value::String(s) => s.parse::<i64>().ok(),
        _ => None,
    }
}

fn parse_f64(v: Option<&serde_json::Value>) -> Option<f64> {
    let v = v?;
    match v {
        serde_json::Value::Number(n) => n.as_f64(),
        serde_json::Value::String(s) => s.parse::<f64>().ok(),
        _ => None,
    }
}

fn parse_bool(v: Option<&serde_json::Value>) -> Option<bool> {
    let v = v?;
    match v {
        serde_json::Value::Bool(b) => Some(*b),
        serde_json::Value::String(s) => match s.trim().to_ascii_lowercase().as_str() {
            "true" | "1" => Some(true),
            "false" | "0" => Some(false),
            _ => None,
        },
        _ => None,
    }
}

/// Read a single length-prefixed frame: `[4B BE u32 len][payload]`.
///
/// Returns `Ok(None)` on clean EOF (connection closed).
async fn read_frame(reader: &mut (impl AsyncReadExt + Unpin)) -> io::Result<Option<Vec<u8>>> {
    let mut len_buf = [0u8; 4];
    match reader.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let frame_len = u32::from_be_bytes(len_buf) as usize;
    let mut payload = vec![0u8; frame_len];
    reader.read_exact(&mut payload).await?;
    Ok(Some(payload))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Array, BooleanArray, Float64Array, Int64Array, StringArray, TimestampNanosecondArray,
    };
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
    use arrow::ipc::writer::FileWriter;
    use std::time::Duration;
    use wf_config::{DistMode, EvictPolicy, LatePolicy, WindowConfig};
    use wf_engine::window::{WindowDef, WindowParams, WindowRegistry};

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
            Field::new("value", DataType::Int64, true),
        ]))
    }

    fn make_batch(
        schema: &SchemaRef,
        times: &[i64],
        values: &[i64],
    ) -> arrow::record_batch::RecordBatch {
        arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampNanosecondArray::from(times.to_vec())),
                Arc::new(Int64Array::from(values.to_vec())),
            ],
        )
        .unwrap()
    }

    fn test_config() -> WindowConfig {
        WindowConfig {
            name: "default".into(),
            mode: DistMode::Local,
            max_window_bytes: usize::MAX.into(),
            over_cap: Duration::from_secs(3600).into(),
            evict_policy: EvictPolicy::TimeFirst,
            watermark: Duration::from_secs(0).into(),
            allowed_lateness: Duration::from_secs(3600).into(),
            late_policy: LatePolicy::Drop,
            table: None,
        }
    }

    fn make_router(stream_name: &str) -> Arc<Router> {
        let reg = WindowRegistry::build(vec![WindowDef {
            params: WindowParams {
                name: "test_win".into(),
                schema: test_schema(),
                time_col_index: Some(0),
                over: Duration::from_secs(3600),
            },
            streams: vec![stream_name.to_string()],
            config: test_config(),
        }])
        .unwrap();
        Arc::new(Router::new(reg))
    }

    /// Count total rows across all batches in the test window snapshot.
    fn snapshot_row_count(router: &Router) -> usize {
        router
            .registry()
            .snapshot("test_win")
            .unwrap_or_default()
            .iter()
            .map(|b| b.num_rows())
            .sum()
    }

    #[tokio::test]
    async fn file_ndjson_replay_routes_rows() {
        let router = make_router("events");
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("events.ndjson");
        std::fs::write(
            &file_path,
            r#"{"ts":1000000000,"value":1}
{"ts":"2000000000","value":"2"}
"#,
        )
        .unwrap();

        replay_ndjson_file(
            &file_path,
            "events",
            "test_source",
            &[wf_lang::WindowSchema {
                name: "test_win".to_string(),
                streams: vec!["events".to_string()],
                time_field: Some("ts".to_string()),
                over: Duration::from_secs(3600),
                fields: vec![
                    wf_lang::FieldDef {
                        name: "ts".to_string(),
                        field_type: wf_lang::FieldType::Base(wf_lang::BaseType::Time),
                    },
                    wf_lang::FieldDef {
                        name: "value".to_string(),
                        field_type: wf_lang::FieldType::Base(wf_lang::BaseType::Digit),
                    },
                ],
            }],
            Arc::clone(&router),
            None,
            CancellationToken::new(),
        )
        .await
        .unwrap();

        assert_eq!(snapshot_row_count(&router), 2);
    }

    #[tokio::test]
    async fn file_arrow_framed_replay_routes_rows() {
        let router = make_router("events");
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("events.arrow_framed");
        let schema = test_schema();
        let batch_a = make_batch(&schema, &[1_000_000_000], &[1]);
        let batch_b = make_batch(&schema, &[2_000_000_000], &[2]);

        {
            let payload_a = wp_arrow::ipc::encode_ipc("events", &batch_a).unwrap();
            let payload_b = wp_arrow::ipc::encode_ipc("events", &batch_b).unwrap();
            let mut body = Vec::new();
            body.extend_from_slice(&(payload_a.len() as u32).to_be_bytes());
            body.extend_from_slice(&payload_a);
            body.extend_from_slice(&(payload_b.len() as u32).to_be_bytes());
            body.extend_from_slice(&payload_b);
            std::fs::write(&file_path, body).unwrap();
        }

        replay_arrow_framed_file(
            &file_path,
            "",
            "test_source",
            &[wf_lang::WindowSchema {
                name: "test_win".to_string(),
                streams: vec!["events".to_string()],
                time_field: Some("ts".to_string()),
                over: Duration::from_secs(3600),
                fields: vec![
                    wf_lang::FieldDef {
                        name: "ts".to_string(),
                        field_type: wf_lang::FieldType::Base(wf_lang::BaseType::Time),
                    },
                    wf_lang::FieldDef {
                        name: "value".to_string(),
                        field_type: wf_lang::FieldType::Base(wf_lang::BaseType::Digit),
                    },
                ],
            }],
            Arc::clone(&router),
            None,
            CancellationToken::new(),
        )
        .await
        .unwrap();

        assert_eq!(snapshot_row_count(&router), 2);
    }

    #[tokio::test]
    async fn file_arrow_ipc_replay_routes_rows() {
        let router = make_router("events");
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("events.arrow_ipc");
        let schema = test_schema();
        let batch_a = make_batch(&schema, &[1_000_000_000], &[1]);
        let batch_b = make_batch(&schema, &[2_000_000_000], &[2]);

        {
            let file = std::fs::File::create(&file_path).unwrap();
            let mut writer = FileWriter::try_new(file, &schema).unwrap();
            writer.write(&batch_a).unwrap();
            writer.write(&batch_b).unwrap();
            writer.finish().unwrap();
        }

        replay_arrow_ipc_file(
            &file_path,
            "events",
            "test_source",
            &[wf_lang::WindowSchema {
                name: "test_win".to_string(),
                streams: vec!["events".to_string()],
                time_field: Some("ts".to_string()),
                over: Duration::from_secs(3600),
                fields: vec![
                    wf_lang::FieldDef {
                        name: "ts".to_string(),
                        field_type: wf_lang::FieldType::Base(wf_lang::BaseType::Time),
                    },
                    wf_lang::FieldDef {
                        name: "value".to_string(),
                        field_type: wf_lang::FieldType::Base(wf_lang::BaseType::Digit),
                    },
                ],
            }],
            Arc::clone(&router),
            None,
            CancellationToken::new(),
        )
        .await
        .unwrap();

        assert_eq!(snapshot_row_count(&router), 2);
    }

    // ---- coerce_column ----

    use super::coerce_column;

    #[test]
    fn coerce_utf8_to_int64() {
        let arr: ArrayRef = Arc::new(StringArray::from(vec!["42", "99", "bad"]));
        let result = coerce_column(&arr, &DataType::Int64, 3);
        let ints = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(ints.value(0), 42);
        assert_eq!(ints.value(1), 99);
        assert!(ints.is_null(2));
    }

    #[test]
    fn coerce_utf8_to_float64() {
        let arr: ArrayRef = Arc::new(StringArray::from(vec!["1.5", "2.0", "x"]));
        let result = coerce_column(&arr, &DataType::Float64, 3);
        let floats = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((floats.value(0) - 1.5).abs() < 1e-10);
        assert!((floats.value(1) - 2.0).abs() < 1e-10);
        assert!(floats.is_null(2));
    }

    #[test]
    fn coerce_utf8_to_bool() {
        let arr: ArrayRef = Arc::new(StringArray::from(vec!["true", "false", "1", "0"]));
        let result = coerce_column(&arr, &DataType::Boolean, 4);
        let bools = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(bools.value(0));
        assert!(!bools.value(1));
        assert!(bools.value(2));
        assert!(!bools.value(3));
    }

    #[test]
    fn coerce_int64_to_float64() {
        let arr: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let result = coerce_column(&arr, &DataType::Float64, 3);
        let floats = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((floats.value(0) - 1.0).abs() < 1e-10);
        assert!((floats.value(1) - 2.0).abs() < 1e-10);
    }

    #[test]
    fn coerce_float64_to_int64() {
        let arr: ArrayRef = Arc::new(Float64Array::from(vec![1.0, 2.5, 3.9]));
        let result = coerce_column(&arr, &DataType::Int64, 3);
        let ints = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(ints.value(0), 1);
        assert_eq!(ints.value(1), 2);
        assert_eq!(ints.value(2), 3);
    }

    #[test]
    fn coerce_int64_to_utf8() {
        let arr: ArrayRef = Arc::new(Int64Array::from(vec![42, 99]));
        let result = coerce_column(&arr, &DataType::Utf8, 2);
        let strings = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(strings.value(0), "42");
        assert_eq!(strings.value(1), "99");
    }

    #[test]
    fn coerce_same_type_noop() {
        let arr: ArrayRef = Arc::new(StringArray::from(vec!["a", "b"]));
        let result = coerce_column(&arr, &DataType::Utf8, 2);
        // Same type should return the original column (clone)
        let strings = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(strings.value(0), "a");
        assert_eq!(strings.value(1), "b");
    }

    #[test]
    fn coerce_unmatched_falls_back_to_null() {
        let arr: ArrayRef = Arc::new(BooleanArray::from(vec![true, false]));
        let result = coerce_column(&arr, &DataType::Timestamp(TimeUnit::Nanosecond, None), 2);
        assert_eq!(result.len(), 2);
        // Unmatched type falls back to NullArray (null values for all rows)
    }

    fn machine_batch(cols: Vec<(&str, Vec<&str>)>) -> RecordBatch {
        let fields: Vec<_> = cols
            .iter()
            .map(|(n, _)| Field::new(*n, DataType::Utf8, true))
            .collect();
        let arrays: Vec<ArrayRef> = cols
            .iter()
            .map(|(_, v)| {
                Arc::new(StringArray::from(
                    v.iter().map(|s| Some(*s)).collect::<Vec<_>>(),
                )) as ArrayRef
            })
            .collect();
        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()
    }

    #[test]
    fn batch_machine_id() {
        let b = machine_batch(vec![("msg", vec!["hello"])]);
        assert_eq!(super::batch_machine_id(&b), None);

        let b = machine_batch(vec![(
            wf_engine::match_engine::MACHINE_ID,
            vec!["10.0.0.1"],
        )]);
        assert_eq!(super::batch_machine_id(&b), Some("10.0.0.1".to_string()));

        let b = machine_batch(vec![(
            wf_engine::match_engine::MACHINE_ID,
            vec!["10.0.0.1", "10.0.0.2"],
        )]);
        assert_eq!(super::batch_machine_id(&b), Some("10.0.0.1".to_string()));
    }
}
