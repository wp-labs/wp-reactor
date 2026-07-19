use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;

use orion_error::conversion::{SourceErr, ToStructError};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio_util::sync::CancellationToken;
use wf_engine::window::Router;
use wf_lang::WindowSchema;

use crate::error::{RuntimeReason, RuntimeResult};
use crate::metrics::RuntimeMetrics;
use crate::receiver::batch::build_record_batch_from_json;
use crate::receiver::miss::{WindowMiss, WindowMissReason, report_window_miss};
use crate::receiver::route::route_batch;
use crate::receiver::schema::{maybe_resolve_stream_schema, resolve_stream_schema};

use super::DEFAULT_STREAM_TAG_FIELD;
use super::ReplayRoute;

/// Replay NDJSON events from file and route them into the runtime.
///
/// If `stream_name` is set, all rows are routed as that configured stream.
/// If it is empty, each JSON object must carry `stream_tag_field`, and rows are
/// routed by that per-row logical stream.
pub async fn replay_ndjson_file(
    path: &Path,
    route: ReplayRoute<'_>,
    source_name: &str,
    schemas: &[WindowSchema],
    router: Arc<Router>,
    metrics: Option<Arc<RuntimeMetrics>>,
    cancel: CancellationToken,
) -> RuntimeResult<()> {
    const FILE_BATCH_ROWS: usize = 2048;

    let stream_name = route.stream_name;
    let fixed_stream = !stream_name.trim().is_empty();
    let fixed_schema = if fixed_stream {
        Some(resolve_stream_schema(schemas, stream_name)?)
    } else {
        None
    };
    let stream_tag_field = normalize_stream_tag_field(route.stream_tag_field);
    let mut schema_cache: HashMap<String, SchemaRef> = HashMap::new();
    let file = tokio::fs::File::open(path).await.source_err(
        RuntimeReason::system_error(),
        format!("open file source {}", path.display()),
    )?;
    let mut lines = BufReader::new(file).lines();
    let mut rows_by_stream: HashMap<String, Vec<serde_json::Map<String, serde_json::Value>>> =
        HashMap::new();
    let mut line_no = 0usize;
    let mut total_rows = 0usize;

    wf_info!(
        conn,
        source = %path.display(),
        stream = if fixed_stream { stream_name } else { "<row stream_tag_field>" },
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
                let route_stream = if fixed_stream {
                    stream_name.to_string()
                } else {
                    match obj.get(stream_tag_field)
                        .and_then(|v| v.as_str())
                        .filter(|s| !s.trim().is_empty())
                        .map(ToString::to_string)
                    {
                        Some(stream) => stream,
                        None => {
                            let miss = WindowMiss::new(
                                stream_tag_field,
                                None,
                                WindowMissReason::MissingStreamTagField,
                                &line,
                                1,
                            );
                            report_window_miss(
                                source_name,
                                "file",
                                &miss,
                                metrics.as_ref(),
                                Some(router.as_ref()),
                            );
                            continue;
                        }
                    }
                };
                let rows = rows_by_stream
                    .entry(route_stream.clone())
                    .or_insert_with(|| Vec::with_capacity(FILE_BATCH_ROWS));
                rows.push(obj.clone());
                if rows.len() >= FILE_BATCH_ROWS {
                    let rows = rows_by_stream
                        .get_mut(&route_stream)
                        .map(std::mem::take)
                        .unwrap_or_default();
                    total_rows += flush_ndjson_rows(
                        &route_stream,
                        source_name,
                        schemas,
                        fixed_schema.as_ref(),
                        &mut schema_cache,
                        rows,
                        router.as_ref(),
                        metrics.as_ref(),
                        stream_tag_field,
                        "file",
                    )?;
                }
            }
        }
    }

    for (route_stream, rows) in rows_by_stream {
        if rows.is_empty() {
            continue;
        }
        total_rows += flush_ndjson_rows(
            &route_stream,
            source_name,
            schemas,
            fixed_schema.as_ref(),
            &mut schema_cache,
            rows,
            router.as_ref(),
            metrics.as_ref(),
            stream_tag_field,
            "file",
        )?;
    }

    wf_info!(
        conn,
        source = %path.display(),
        stream = if fixed_stream { stream_name } else { "<row stream_tag_field>" },
        rows = total_rows,
        "file source replay complete"
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) fn flush_ndjson_rows(
    stream_name: &str,
    source_name: &str,
    schemas: &[WindowSchema],
    fixed_schema: Option<&SchemaRef>,
    schema_cache: &mut HashMap<String, SchemaRef>,
    rows: Vec<serde_json::Map<String, serde_json::Value>>,
    router: &Router,
    metrics: Option<&Arc<RuntimeMetrics>>,
    stream_tag_field: &str,
    source_kind: &str,
) -> RuntimeResult<usize> {
    if rows.is_empty() {
        return Ok(0);
    }
    let schema = match fixed_schema {
        Some(schema) => Arc::clone(schema),
        None => {
            if let Some(schema) = schema_cache.get(stream_name) {
                Arc::clone(schema)
            } else {
                let Some(schema) = maybe_resolve_stream_schema(schemas, stream_name)? else {
                    let sample = rows
                        .first()
                        .cloned()
                        .map(serde_json::Value::Object)
                        .map(|value| value.to_string())
                        .unwrap_or_default();
                    let miss = WindowMiss::new(
                        stream_tag_field,
                        Some(stream_name.to_string()),
                        WindowMissReason::UnknownStreamSchema,
                        sample,
                        rows.len(),
                    );
                    report_window_miss(source_name, source_kind, &miss, metrics, Some(router));
                    return Ok(0);
                };
                schema_cache.insert(stream_name.to_string(), Arc::clone(&schema));
                schema
            }
        }
    };
    let batch = build_record_batch_from_json(&schema, &rows)?;
    let row_count = batch.num_rows();
    if let Err(e) = route_batch(stream_name, source_name, batch, router, metrics) {
        if let Some(metrics) = metrics {
            metrics.inc_route_error(source_name);
        }
        return Err(e);
    }
    Ok(row_count)
}

pub fn normalize_stream_tag_field(value: &str) -> &str {
    let value = value.trim();
    if value.is_empty() {
        DEFAULT_STREAM_TAG_FIELD
    } else {
        value
    }
}
