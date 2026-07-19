use std::io;
use std::path::Path;
use std::sync::Arc;

use arrow::ipc::reader::FileReader;
use orion_error::conversion::{SourceErr, SourceRawErr, ToStructError};
use tokio::io::AsyncReadExt;
use tokio_util::sync::CancellationToken;
use wf_engine::window::Router;
use wf_lang::WindowSchema;

use crate::error::{RuntimeReason, RuntimeResult};
use crate::metrics::RuntimeMetrics;
use crate::receiver::miss::record_batch_window_miss;
use crate::receiver::route::route_batch;
use crate::receiver::schema::{
    maybe_resolve_stream_schema, resolve_stream_schema, validate_batch_schema_for_stream,
};

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
                if stream_override.is_none()
                    && maybe_resolve_stream_schema(schemas, stream)?.is_none()
                {
                    record_batch_window_miss(
                        source_name,
                        "file",
                        "wp_arrow_tag",
                        stream,
                        frame.batch.num_rows(),
                        metrics.as_ref(),
                        Some(router.as_ref()),
                    );
                    continue;
                }
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
