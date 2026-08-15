use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use arrow::datatypes::SchemaRef;
use orion_error::conversion::ToStructError;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use wf_engine::window::Router;
use wf_lang::WindowSchema;

use crate::error::{RuntimeReason, RuntimeResult};
use crate::lifecycle::parse_pool::{ParseItem, PrereadBudget};
use crate::metrics::RuntimeMetrics;
use crate::receiver::miss::{WindowMiss, WindowMissReason, report_window_miss};
use crate::receiver::ndjson::{flush_ndjson_rows, normalize_stream_tag_field};
use crate::receiver::schema::resolve_stream_schema;

use super::ReplayRoute;

/// Replay CSV data from file and route into the runtime as one stream.
///
/// CSV headers must match schema field names. Each row is converted to a
/// RecordBatch using the same column builder as NDJSON.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn replay_csv_file(
    path: &Path,
    route: ReplayRoute<'_>,
    source_name: &str,
    schemas: &[WindowSchema],
    router: Arc<Router>,
    metrics: Option<Arc<RuntimeMetrics>>,
    parse_tx: mpsc::Sender<ParseItem>,
    preread: PrereadBudget,
    parse_seq: Arc<AtomicU64>,
    cancel: CancellationToken,
) -> RuntimeResult<()> {
    let stream_name = route.stream_name;
    let fixed_stream = !stream_name.trim().is_empty();
    let fixed_schema = if fixed_stream {
        Some(resolve_stream_schema(schemas, stream_name)?)
    } else {
        None
    };
    let stream_tag_field = normalize_stream_tag_field(route.stream_tag_field);
    let mut schema_cache: HashMap<String, SchemaRef> = HashMap::new();
    let file_path = path.to_path_buf();
    let stream_name = stream_name.to_string();
    const FILE_BATCH_ROWS_CSV: usize = 2048;

    wf_info!(
        conn,
        source = %path.display(),
        stream = if fixed_stream { stream_name.as_str() } else { "<row stream_tag_field>" },
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
    let mut rows_by_stream: HashMap<String, Vec<serde_json::Map<String, serde_json::Value>>> =
        HashMap::new();

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

        let route_stream = if fixed_stream {
            stream_name.clone()
        } else {
            match map
                .get(stream_tag_field)
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
                .map(ToString::to_string)
            {
                Some(stream) => stream,
                None => {
                    let sample = serde_json::Value::Object(map).to_string();
                    let miss = WindowMiss::new(
                        stream_tag_field,
                        None,
                        WindowMissReason::MissingStreamTagField,
                        sample,
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
            .or_insert_with(|| Vec::with_capacity(FILE_BATCH_ROWS_CSV));
        rows.push(map);
        if rows.len() >= FILE_BATCH_ROWS_CSV {
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
                &parse_tx,
                &preread,
                &parse_seq,
                stream_tag_field,
                "file",
            )
            .await?;
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
            &parse_tx,
            &preread,
            &parse_seq,
            stream_tag_field,
            "file",
        )
        .await?;
    }

    wf_info!(
        conn,
        source = %path.display(),
        stream = if fixed_stream { stream_name.as_str() } else { "<row stream_tag_field>" },
        rows = total_rows,
        "csv file replay complete"
    );
    Ok(())
}
