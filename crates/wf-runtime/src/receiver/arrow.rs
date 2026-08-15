use std::io;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use arrow::ipc::reader::FileReader;
use orion_error::conversion::{SourceErr, SourceRawErr, ToStructError};
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use wf_engine::window::Router;
use wf_lang::WindowSchema;

use crate::error::{RuntimeReason, RuntimeResult};
use crate::lifecycle::parse_pool::{
    IngestLimiter, ParseItem, PrereadBudget, acquire_preread_blocking, build_parse_item,
    push_decoded_batch,
};
use crate::metrics::RuntimeMetrics;
use crate::receiver::miss::record_batch_window_miss;
use crate::receiver::schema::{
    maybe_resolve_stream_schema, resolve_stream_schema, schemas_are_compatible_for_stream,
    validate_batch_schema_for_stream,
};

/// Replay framed `wp_arrow` IPC records from file and route them into the
/// runtime.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn replay_arrow_framed_file(
    path: &Path,
    stream_name: &str,
    source_name: &str,
    schemas: &[WindowSchema],
    router: Arc<Router>,
    metrics: Option<Arc<RuntimeMetrics>>,
    parse_tx: mpsc::Sender<ParseItem>,
    preread: PrereadBudget,
    parse_seq: Arc<AtomicU64>,
    cancel: CancellationToken,
    limiter: Option<Arc<IngestLimiter>>,
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
                if !push_decoded_batch(
                    &parse_tx,
                    &preread,
                    &parse_seq,
                    source_name,
                    stream,
                    frame.batch,
                    router.as_ref(),
                    metrics.as_ref(),
                    limiter.as_deref(),
                )
                .await
                {
                    return RuntimeReason::system_error()
                        .to_err()
                        .with_detail("parse worker pool shut down during file replay")
                        .err();
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
#[allow(clippy::too_many_arguments)]
pub(crate) async fn replay_arrow_ipc_file(
    path: &Path,
    stream_name: &str,
    source_name: &str,
    schemas: &[WindowSchema],
    router: Arc<Router>,
    metrics: Option<Arc<RuntimeMetrics>>,
    parse_tx: mpsc::Sender<ParseItem>,
    preread: PrereadBudget,
    parse_seq: Arc<AtomicU64>,
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
        if !schemas_are_compatible_for_stream(expected_schema.as_ref(), file_schema.as_ref()) {
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
            let mem_bytes = batch.get_array_memory_size();
            let permits = acquire_preread_blocking(&preread, mem_bytes);
            let item = build_parse_item(
                &parse_seq,
                &source_for_read,
                &stream_for_read,
                batch,
                router.as_ref(),
                metrics.as_ref(),
                mem_bytes,
                permits,
            );
            if parse_tx.blocking_send(item).is_err() {
                return RuntimeReason::system_error()
                    .to_err()
                    .with_detail("parse worker pool shut down during file replay")
                    .err();
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

/// Max digits in the ASCII length prefix (covers any realistic byte count;
/// usize is ≤ 20 digits on 64-bit, but 16 is far beyond any valid frame).
const MAX_FRAME_PREFIX_DIGITS: usize = 16;
/// Safety cap on a single replayed frame, so a malformed length prefix cannot
/// trigger an unbounded allocation.
const MAX_FRAME_BYTES: usize = 1 << 30; // 1 GiB

/// Read a single length-prefixed frame: `<ascii digits> <payload>`.
///
/// This matches the wire framing produced by the TCP sink's `len` mode
/// (`build_payload_bytes`: decimal byte count + space + payload), so a file
/// captured by `wfgen dump-frames` / `send` can be replayed byte-for-byte.
/// Returns `Ok(None)` on clean EOF (end of file).
async fn read_frame(reader: &mut (impl AsyncReadExt + Unpin)) -> io::Result<Option<Vec<u8>>> {
    let mut len_buf: Vec<u8> = Vec::with_capacity(MAX_FRAME_PREFIX_DIGITS);
    loop {
        let mut byte = [0u8; 1];
        match reader.read_exact(&mut byte).await {
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                // EOF mid-prefix: clean only when nothing was read yet.
                return if len_buf.is_empty() { Ok(None) } else { Err(e) };
            }
            Err(e) => return Err(e),
        }
        if byte[0] == b' ' {
            break;
        }
        if !byte[0].is_ascii_digit() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid frame length prefix (expected '<digits> ')",
            ));
        }
        len_buf.push(byte[0]);
        if len_buf.len() > MAX_FRAME_PREFIX_DIGITS {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "frame length prefix too long",
            ));
        }
    }
    let len_str = std::str::from_utf8(&len_buf)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad frame length"))?;
    let frame_len: usize = len_str.parse().map_err(|_| {
        io::Error::new(io::ErrorKind::InvalidData, format!("bad frame length '{len_str}'"))
    })?;
    if frame_len == 0 || frame_len > MAX_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unreasonable frame length {frame_len}"),
        ));
    }
    let mut payload = vec![0u8; frame_len];
    reader.read_exact(&mut payload).await?;
    Ok(Some(payload))
}
