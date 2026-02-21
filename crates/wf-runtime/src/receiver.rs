use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use tokio::io::{AsyncReadExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use wf_core::window::Router;

/// TCP receiver that accepts connections, reads length-prefixed Arrow IPC
/// frames, decodes them, and routes batches to the [`Router`].
///
/// Optionally sends `(stream_name, RecordBatch)` to the scheduler via
/// `event_tx` so the CEP engine can process each incoming batch.
pub struct Receiver {
    listener: TcpListener,
    router: Arc<Router>,
    cancel: CancellationToken,
    event_tx: Option<mpsc::Sender<(String, RecordBatch)>>,
}

impl Receiver {
    /// Parse `"tcp://host:port"` and bind a TCP listener.
    pub async fn bind(listen: &str, router: Arc<Router>) -> anyhow::Result<Self> {
        let addr = listen.strip_prefix("tcp://").unwrap_or(listen);
        let listener = TcpListener::bind(addr).await?;
        Ok(Self {
            listener,
            router,
            cancel: CancellationToken::new(),
            event_tx: None,
        })
    }

    /// Parse `"tcp://host:port"` and bind a TCP listener with an event channel
    /// for the scheduler.
    pub async fn bind_with_event_tx(
        listen: &str,
        router: Arc<Router>,
        event_tx: mpsc::Sender<(String, RecordBatch)>,
    ) -> anyhow::Result<Self> {
        let addr = listen.strip_prefix("tcp://").unwrap_or(listen);
        let listener = TcpListener::bind(addr).await?;
        Ok(Self {
            listener,
            router,
            cancel: CancellationToken::new(),
            event_tx: Some(event_tx),
        })
    }

    /// Returns the local address the listener is bound to.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Returns a clone of the cancellation token for external shutdown signaling.
    pub fn cancel_token(&self) -> CancellationToken {
        self.cancel.clone()
    }

    /// Start the accept loop. Blocks until the cancellation token is triggered.
    #[tracing::instrument(name = "receiver", skip_all)]
    pub async fn run(self) -> anyhow::Result<()> {
        loop {
            tokio::select! {
                result = self.listener.accept() => {
                    let (stream, peer) = result?;
                    wf_debug!(conn, peer = %peer, "accepted connection");
                    let router = Arc::clone(&self.router);
                    let cancel = self.cancel.child_token();
                    let event_tx = self.event_tx.clone();
                    tokio::spawn(handle_connection(stream, router, cancel, peer, event_tx));
                }
                _ = self.cancel.cancelled() => break,
            }
        }
        Ok(())
    }
}

#[tracing::instrument(skip_all, fields(peer = %peer))]
async fn handle_connection(
    stream: TcpStream,
    router: Arc<Router>,
    cancel: CancellationToken,
    peer: SocketAddr,
    event_tx: Option<mpsc::Sender<(String, RecordBatch)>>,
) {
    let (reader, _writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    loop {
        tokio::select! {
            result = read_frame(&mut reader) => {
                match result {
                    Ok(None) => break,
                    Ok(Some(payload)) => {
                        match wp_arrow::ipc::decode_ipc(&payload) {
                            Ok(frame) => {
                                wf_trace!(pipe, stream = &*frame.tag, rows = frame.batch.num_rows(), "frame decoded");
                                // Send to scheduler before routing to windows
                                if let Some(tx) = &event_tx {
                                    if tx.send((frame.tag.clone(), frame.batch.clone())).await.is_err() {
                                        wf_warn!(conn, peer = %peer, "event channel closed, dropping connection");
                                        break;
                                    }
                                }
                                if let Err(e) = router.route(&frame.tag, frame.batch) {
                                    wf_warn!(pipe, error = %e, "route error");
                                }
                            }
                            Err(e) => wf_warn!(conn, error = %e, "IPC decode error"),
                        }
                    }
                    Err(e) => {
                        wf_warn!(conn, error = %e, "connection read error");
                        break;
                    }
                }
            }
            _ = cancel.cancelled() => break,
        }
    }
    wf_debug!(conn, peer = %peer, "connection closed");
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
    use arrow::array::{Int64Array, TimestampNanosecondArray};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
    use std::time::Duration;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpStream;
    use wf_config::{DistMode, EvictPolicy, LatePolicy, WindowConfig};
    use wf_core::window::{WindowDef, WindowParams, WindowRegistry};

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
            Field::new("value", DataType::Int64, false),
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

    /// Encode a RecordBatch and wrap it in a length-prefixed outer frame.
    fn make_frame(stream_name: &str, batch: &arrow::record_batch::RecordBatch) -> Vec<u8> {
        let payload = wp_arrow::ipc::encode_ipc(stream_name, batch).unwrap();
        let mut frame = Vec::with_capacity(4 + payload.len());
        frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        frame.extend_from_slice(&payload);
        frame
    }

    async fn send_frame(stream: &mut TcpStream, frame: &[u8]) {
        stream.write_all(frame).await.unwrap();
        stream.flush().await.unwrap();
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

    // -- Test 1: multi_connection_concurrent -----------------------------------

    #[tokio::test]
    async fn multi_connection_concurrent() {
        let router = make_router("events");
        let receiver = Receiver::bind("tcp://127.0.0.1:0", Arc::clone(&router))
            .await
            .unwrap();
        let addr = receiver.local_addr().unwrap();
        let cancel = receiver.cancel_token();

        let server = tokio::spawn(async move { receiver.run().await });

        let schema = test_schema();
        let mut handles = Vec::new();
        for i in 0..3 {
            let schema = schema.clone();
            handles.push(tokio::spawn(async move {
                let mut conn = TcpStream::connect(addr).await.unwrap();
                let ts = (i + 1) * 10_000_000_000_i64;
                let batch = make_batch(&schema, &[ts], &[i]);
                let frame = make_frame("events", &batch);
                send_frame(&mut conn, &frame).await;
                // Small delay to ensure the frame is processed before we drop
                tokio::time::sleep(Duration::from_millis(50)).await;
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        // Allow processing time
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(snapshot_row_count(&router), 3);

        cancel.cancel();
        server.await.unwrap().unwrap();
    }

    // -- Test 2: continuous_reception ------------------------------------------

    #[tokio::test]
    async fn continuous_reception() {
        let router = make_router("stream");
        let receiver = Receiver::bind("tcp://127.0.0.1:0", Arc::clone(&router))
            .await
            .unwrap();
        let addr = receiver.local_addr().unwrap();
        let cancel = receiver.cancel_token();

        let server = tokio::spawn(async move { receiver.run().await });

        let schema = test_schema();
        let mut conn = TcpStream::connect(addr).await.unwrap();
        for i in 0..10 {
            let ts = (i + 1) * 10_000_000_000_i64;
            let batch = make_batch(&schema, &[ts], &[i]);
            let frame = make_frame("stream", &batch);
            send_frame(&mut conn, &frame).await;
        }

        // Allow processing time
        tokio::time::sleep(Duration::from_millis(200)).await;

        assert_eq!(snapshot_row_count(&router), 10);

        cancel.cancel();
        server.await.unwrap().unwrap();
    }

    // -- Test 3: connection_drop_no_impact -------------------------------------

    #[tokio::test]
    async fn connection_drop_no_impact() {
        let router = make_router("data");
        let receiver = Receiver::bind("tcp://127.0.0.1:0", Arc::clone(&router))
            .await
            .unwrap();
        let addr = receiver.local_addr().unwrap();
        let cancel = receiver.cancel_token();

        let server = tokio::spawn(async move { receiver.run().await });

        let schema = test_schema();

        // conn_a: send 1 frame then drop
        {
            let mut conn_a = TcpStream::connect(addr).await.unwrap();
            let batch = make_batch(&schema, &[10_000_000_000], &[1]);
            let frame = make_frame("data", &batch);
            send_frame(&mut conn_a, &frame).await;
            tokio::time::sleep(Duration::from_millis(50)).await;
            // conn_a dropped here
        }

        tokio::time::sleep(Duration::from_millis(50)).await;

        // conn_b: send 1 frame after conn_a is gone
        let mut conn_b = TcpStream::connect(addr).await.unwrap();
        let batch = make_batch(&schema, &[20_000_000_000], &[2]);
        let frame = make_frame("data", &batch);
        send_frame(&mut conn_b, &frame).await;

        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(snapshot_row_count(&router), 2);

        cancel.cancel();
        server.await.unwrap().unwrap();
    }

    // -- Test 4: event_tx_receives_batches ------------------------------------

    #[tokio::test]
    async fn event_tx_receives_batches() {
        let router = make_router("events");
        let (tx, mut rx) = mpsc::channel::<(String, RecordBatch)>(16);
        let receiver =
            Receiver::bind_with_event_tx("tcp://127.0.0.1:0", Arc::clone(&router), tx)
                .await
                .unwrap();
        let addr = receiver.local_addr().unwrap();
        let cancel = receiver.cancel_token();

        let server = tokio::spawn(async move { receiver.run().await });

        let schema = test_schema();
        let mut conn = TcpStream::connect(addr).await.unwrap();
        let batch = make_batch(&schema, &[10_000_000_000], &[42]);
        let frame = make_frame("events", &batch);
        send_frame(&mut conn, &frame).await;

        // Receive from the event channel
        let (stream_name, received_batch) = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stream_name, "events");
        assert_eq!(received_batch.num_rows(), 1);

        cancel.cancel();
        server.await.unwrap().unwrap();
    }
}
