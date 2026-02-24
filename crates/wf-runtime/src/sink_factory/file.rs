use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::io::AsyncWriteExt;
use wp_connector_api::*;
use wp_model_core::model::DataRecord;

// ---------------------------------------------------------------------------
// FileSinkFactory — built-in file sink for wp-reactor
// ---------------------------------------------------------------------------

/// Factory for the built-in `file` sink type.
///
/// Writes alert JSON lines to a file. The `path` parameter is resolved
/// relative to `SinkBuildCtx::work_root`.
pub struct FileSinkFactory;

impl SinkDefProvider for FileSinkFactory {
    fn sink_def(&self) -> ConnectorDef {
        ConnectorDef {
            id: "builtin_file".into(),
            kind: "file".into(),
            scope: ConnectorScope::Sink,
            allow_override: vec!["path".into()],
            default_params: ParamMap::new(),
            origin: None,
        }
    }
}

#[async_trait]
impl SinkFactory for FileSinkFactory {
    fn kind(&self) -> &'static str {
        "file"
    }

    fn validate_spec(&self, spec: &SinkSpec) -> SinkResult<()> {
        if !spec.params.contains_key("path") {
            return Err(SinkError::from(SinkReason::Sink(
                "file sink requires 'path' parameter".into(),
            )));
        }
        Ok(())
    }

    async fn build(&self, spec: &SinkSpec, ctx: &SinkBuildCtx) -> SinkResult<SinkHandle> {
        let path_val = spec
            .params
            .get("path")
            .ok_or_else(|| SinkError::from(SinkReason::Sink("missing 'path' parameter".into())))?;

        let path_str = path_val
            .as_str()
            .ok_or_else(|| SinkError::from(SinkReason::Sink("'path' must be a string".into())))?;

        let path = if PathBuf::from(path_str).is_relative() {
            ctx.work_root.join(path_str)
        } else {
            PathBuf::from(path_str)
        };

        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .owe_sink(format!("failed to create directory {}", parent.display()))?;
        }

        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await
            .owe_sink(format!("failed to open {}", path.display()))?;

        let writer = tokio::io::BufWriter::new(file);
        Ok(SinkHandle::new(Box::new(AsyncFileSink { writer })))
    }
}

// ---------------------------------------------------------------------------
// AsyncFileSink — async file writer implementing wp-connector-api traits
// ---------------------------------------------------------------------------

struct AsyncFileSink {
    writer: tokio::io::BufWriter<tokio::fs::File>,
}

#[async_trait]
impl AsyncCtrl for AsyncFileSink {
    async fn stop(&mut self) -> SinkResult<()> {
        self.writer.flush().await.owe_sink("flush on stop")?;
        self.writer.shutdown().await.owe_sink("shutdown")?;
        Ok(())
    }

    async fn reconnect(&mut self) -> SinkResult<()> {
        Ok(())
    }
}

#[async_trait]
impl AsyncRawDataSink for AsyncFileSink {
    async fn sink_str(&mut self, data: &str) -> SinkResult<()> {
        self.writer
            .write_all(data.as_bytes())
            .await
            .owe_sink("write data")?;
        self.writer
            .write_all(b"\n")
            .await
            .owe_sink("write newline")?;
        self.writer.flush().await.owe_sink("flush")?;
        Ok(())
    }

    async fn sink_bytes(&mut self, data: &[u8]) -> SinkResult<()> {
        self.writer.write_all(data).await.owe_sink("write bytes")?;
        self.writer.flush().await.owe_sink("flush")?;
        Ok(())
    }

    async fn sink_str_batch(&mut self, data: Vec<&str>) -> SinkResult<()> {
        for s in data {
            self.writer
                .write_all(s.as_bytes())
                .await
                .owe_sink("write batch")?;
            self.writer
                .write_all(b"\n")
                .await
                .owe_sink("write newline")?;
        }
        self.writer.flush().await.owe_sink("flush batch")?;
        Ok(())
    }

    async fn sink_bytes_batch(&mut self, data: Vec<&[u8]>) -> SinkResult<()> {
        for b in data {
            self.writer
                .write_all(b)
                .await
                .owe_sink("write bytes batch")?;
        }
        self.writer.flush().await.owe_sink("flush batch")?;
        Ok(())
    }
}

#[async_trait]
impl AsyncRecordSink for AsyncFileSink {
    // wp-reactor doesn't use DataRecord; provide no-op implementations.
    async fn sink_record(&mut self, _record: &DataRecord) -> SinkResult<()> {
        Ok(())
    }

    async fn sink_records(&mut self, _records: Vec<Arc<DataRecord>>) -> SinkResult<()> {
        Ok(())
    }
}

// AsyncSink is automatically implemented via blanket impl.
