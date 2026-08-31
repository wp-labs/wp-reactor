use std::io;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use arrow::ipc::reader::FileReader;
use orion_error::conversion::{SourceErr, SourceRawErr, ToStructError};
use tokio::io::AsyncReadExt;
use tokio_util::sync::CancellationToken;
use wf_engine::window::Router;
use wf_lang::WindowSchema;

use crate::error::{RuntimeReason, RuntimeResult};
use crate::lifecycle::ingest::{IngestLimiter, route_and_dispatch, route_and_dispatch_blocking};
use crate::metrics::RuntimeMetrics;
use crate::receiver::miss::record_batch_window_miss;
use crate::receiver::schema::{
    maybe_resolve_stream_schema, resolve_stream_schema, schemas_are_compatible_for_stream,
    validate_batch_schema_for_stream,
};

/// Decode a trusted `wp_arrow` IPC frame: `[4B tag_len BE][tag][Arrow IPC stream]`.
///
/// Same wire format as `wp_arrow::ipc::decode_ipc`, but skips arrow-rs's
/// `ArrayData::validate_values` content validation (UTF-8 / offsets / null
/// bitmap). Frames are produced locally by `wfgen gen-nexmark`/`dump-frames`,
/// so the validation is pure overhead: on 30M bid events the string columns
/// (channel/url/extra) add up to ~4.5GB of bytes that get fully re-scanned per
/// frame (2026-08-23 measurement: `validate_utf8` is the decode hot spot and
/// the single-connection ~2.3GB/s byte-rate wall). Structural safety is
/// unchanged — offsets/lengths are still enforced by the decoder, only the
/// redundant content re-validation is skipped.
pub(crate) fn decode_ipc_trusted(
    data: &[u8],
) -> Result<(String, arrow::array::RecordBatch), arrow::error::ArrowError> {
    use arrow::ipc::reader::StreamReader;

    if data.len() < 4 {
        return Err(arrow::error::ArrowError::IpcError(
            "frame too short: {} bytes, minimum 4".to_string(),
        ));
    }
    let tag_len = u32::from_be_bytes(data[0..4].try_into().unwrap()) as usize;
    let tag_end = 4 + tag_len;
    if data.len() < tag_end {
        return Err(arrow::error::ArrowError::IpcError(format!(
            "frame truncated: tag_len={tag_len} but only {} bytes remain after header",
            data.len() - 4
        )));
    }
    let tag = String::from_utf8(data[4..tag_end].to_vec())
        .map_err(|e| arrow::error::ArrowError::IpcError(format!("invalid UTF-8 in tag: {e}")))?;
    let ipc_payload = &data[tag_end..];
    let reader = StreamReader::try_new(ipc_payload, None)?;
    // SAFETY: `with_skip_validation` asserts the IPC payload needs no content
    // re-validation; frames come from local `wfgen` producers (UTF-8/offsets
    // guaranteed by the generator). Byte-identical output to the validating
    // path for valid input — locked by the round-trip 对拍 test below.
    let mut reader = unsafe { reader.with_skip_validation(true) };
    let batch = reader.next().ok_or_else(|| {
        arrow::error::ArrowError::IpcError("no RecordBatch in IPC payload".to_string())
    })??;
    Ok((tag, batch))
}

/// 只读帧头 tag（前 4 字节长度 + tag 字节）——**不解码 body**。perf-diag
/// cut_recv 档用: 非哨兵帧只判流名即丢, 避免单线程 decode 的字节率墙
/// （validate_utf8 ~2.3GB/s 单连接上限）把「纯 TCP 接收」混进来。与
/// [`decode_ipc_trusted`] 的 tag 提取字节级一致。
pub(crate) fn frame_tag(data: &[u8]) -> Option<String> {
    if data.len() < 4 {
        return None;
    }
    let tag_len = u32::from_be_bytes(data[0..4].try_into().ok()?) as usize;
    let tag_end = 4 + tag_len;
    if data.len() < tag_end {
        return None;
    }
    String::from_utf8(data[4..tag_end].to_vec()).ok()
}

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

                let (tag, batch) = decode_ipc_trusted(&payload)
                    .source_raw_err(
                        RuntimeReason::data_error(),
                        format!("decode arrow frame from {}", path.display()),
                    )?;
                let stream = stream_override.as_deref().unwrap_or(tag.as_str());
                if stream_override.is_none()
                    && maybe_resolve_stream_schema(schemas, stream)?.is_none()
                {
                    record_batch_window_miss(
                        source_name,
                        "file",
                        "wp_arrow_tag",
                        stream,
                        batch.num_rows(),
                        metrics.as_ref(),
                        Some(router.as_ref()),
                    );
                    continue;
                }
                validate_batch_schema_for_stream(schemas, stream, batch.schema().as_ref())?;

                total_rows += batch.num_rows();
                route_and_dispatch(
                    &parse_seq,
                    source_name,
                    stream,
                    batch,
                    router.as_ref(),
                    metrics.as_ref(),
                    limiter.as_deref(),
                )
                .await;
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
            // decode-route-merge：内联 route + dispatch。blocking 变体在
            // blocking 池线程上用 runtime 句柄 block_on 驱动（mailbox 预算
            // 等待仍由 actor 在 runtime 线程推进，不会死锁）。
            route_and_dispatch_blocking(
                &parse_seq,
                &source_for_read,
                &stream_for_read,
                batch,
                router.as_ref(),
                metrics.as_ref(),
                None,
            );
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
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("bad frame length '{len_str}'"),
        )
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int32Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_batch(num_rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let ids: Vec<i32> = (0..num_rows as i32).collect();
        let names: Vec<Option<&str>> = (0..num_rows)
            .map(|i| if i % 2 == 0 { Some("even") } else { None })
            .collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids)) as ArrayRef,
                Arc::new(StringArray::from(names)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn trusted_decode_matches_wp_arrow_roundtrip() {
        // 对拍：trusted 解码结果与 wp_arrow 标准解码（含 validate）逐位一致。
        let batch = make_batch(5);
        let encoded = wp_arrow::ipc::encode_ipc("test-tag", &batch).unwrap();
        let (tag, decoded) = decode_ipc_trusted(&encoded).unwrap();
        assert_eq!(tag, "test-tag");
        assert_eq!(decoded, batch);
        let frame = wp_arrow::ipc::decode_ipc(&encoded).unwrap();
        assert_eq!(frame.tag, tag);
        assert_eq!(frame.batch, decoded);
    }

    #[test]
    fn trusted_decode_handles_utf8_tag_and_empty_tag() {
        let batch = make_batch(1);
        for tag in ["", "数据标签-🚀", "auction_events"] {
            let encoded = wp_arrow::ipc::encode_ipc(tag, &batch).unwrap();
            let (got, decoded) = decode_ipc_trusted(&encoded).unwrap();
            assert_eq!(got, tag);
            assert_eq!(decoded, batch);
        }
    }

    #[test]
    fn trusted_decode_large_batch() {
        let batch = make_batch(100_000);
        let encoded = wp_arrow::ipc::encode_ipc("large", &batch).unwrap();
        let (tag, decoded) = decode_ipc_trusted(&encoded).unwrap();
        assert_eq!(tag, "large");
        assert_eq!(decoded.num_rows(), 100_000);
        assert_eq!(decoded, batch);
    }

    #[test]
    fn trusted_decode_rejects_short_and_truncated_frames() {
        assert!(decode_ipc_trusted(&[0u8; 2]).is_err());
        let mut data = vec![0u8, 0, 0, 100]; // tag_len = 100 but no tag bytes
        assert!(decode_ipc_trusted(&data).is_err());
        data = vec![0u8, 0, 0, 1, b'x']; // tag ok, empty IPC payload
        assert!(decode_ipc_trusted(&data).is_err());
    }

    /// 手动微基准：对真实帧（从 bench_30m_v5.frames 提取到 /tmp/frame.bin）
    /// 对比 trusted（skip validate）与 wp_arrow 标准解码耗时。
    #[test]
    #[ignore = "manual: reads /tmp/frame.bin extracted from a real frames file"]
    fn compare_decode_trusted_vs_wp_arrow_real_frame() {
        use std::time::Instant;
        let payload = std::fs::read("/tmp/frame.bin").unwrap();
        let rows = wp_arrow::ipc::decode_ipc(&payload)
            .unwrap()
            .batch
            .num_rows();
        assert_eq!(decode_ipc_trusted(&payload).unwrap().1.num_rows(), rows);
        for _ in 0..50 {
            let _ = decode_ipc_trusted(&payload);
        }
        let t = Instant::now();
        for _ in 0..500 {
            let _ = decode_ipc_trusted(&payload);
        }
        let trusted = t.elapsed();
        for _ in 0..50 {
            let _ = wp_arrow::ipc::decode_ipc(&payload);
        }
        let t = Instant::now();
        for _ in 0..500 {
            let _ = wp_arrow::ipc::decode_ipc(&payload);
        }
        let wp = t.elapsed();
        println!(
            "rows={rows} trusted={trusted:?} wp_arrow={wp:?} speedup={:.2}x",
            wp.as_secs_f64() / trusted.as_secs_f64()
        );
    }
}
