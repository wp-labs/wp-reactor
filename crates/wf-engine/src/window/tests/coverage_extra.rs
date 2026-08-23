//! Coverage extras for `window/actor.rs`: the shutdown / sender-drop /
//! closed-semaphore / reporter branches the main actor suite drives only
//! indirectly (it always drops the sender, cancels nothing, and passes no
//! report closure).
use std::sync::Arc;

use std::sync::Mutex;
use std::time::Duration;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use tokio::sync::{Notify, Semaphore, mpsc};
use tokio_util::sync::CancellationToken;
use wf_config::{DistMode, EvictPolicy, LatePolicy, WindowConfig};

use crate::window::{
    EvictionGate, RuleFanout, WINDOW_CHANNEL_DEPTH, Window, WindowAppendReport, WindowMsg,
    WindowParams, acquire_window_budget, run_window_actor,
};

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("value", DataType::Int64, false),
    ]))
}

fn make_batch(schema: &SchemaRef, time: i64, value: i64) -> RecordBatch {
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(arrow::array::TimestampNanosecondArray::from(vec![time])),
            Arc::new(Int64Array::from(vec![value])),
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
        watermark: Duration::from_secs(5).into(),
        allowed_lateness: Duration::from_secs(0).into(),
        late_policy: LatePolicy::Drop,
        table: None,
    }
}

fn make_window(name: &str) -> Arc<Window> {
    Arc::new(Window::new(
        WindowParams {
            name: name.into(),
            schema: test_schema(),
            time_col_index: Some(0),
            over: Duration::from_secs(3600),
            materialize_fields: None,
            defer_materialization: false,
        },
        test_config(),
    ))
}

fn msg(source: &str, seq: u64, time: i64, value: i64) -> WindowMsg {
    WindowMsg::Append {
        source: Arc::from(source),
        seq,
        batch: make_batch(&test_schema(), time, value),
        events: None,
        byte_size: 64,
        permits: Vec::new(),
        shard_rows: None,
    }
}

fn appended_values(win: &Arc<Window>) -> Vec<i64> {
    win.snapshot()
        .iter()
        .flat_map(|batch| {
            batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .map(|v| v.unwrap())
                .collect::<Vec<_>>()
        })
        .collect()
}

fn spawn_actor(
    win: Arc<Window>,
    report: Option<WindowAppendReport>,
) -> (
    mpsc::Sender<WindowMsg>,
    CancellationToken,
    tokio::task::JoinHandle<()>,
) {
    let (tx, rx) = mpsc::channel::<WindowMsg>(WINDOW_CHANNEL_DEPTH);
    let cancel = CancellationToken::new();
    let name: Arc<str> = Arc::from("w");
    let fanout = RuleFanout::new();
    let notify = Arc::new(Notify::new());
    let cancel2 = cancel.clone();
    let handle = tokio::spawn(async move {
        run_window_actor(
            name,
            win,
            Arc::new(EvictionGate::new(usize::MAX)),
            fanout,
            notify,
            rx,
            cancel2,
            report,
        )
        .await;
    });
    (tx, cancel, handle)
}

async fn wait_for(win: &Arc<Window>, rows: usize) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while win.total_rows() < rows {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("actor makes progress within timeout");
}

// -- shutdown: cancel flushes the already-queued tail -------------------------

#[tokio::test]
async fn cancellation_flushes_queued_messages() {
    let win = make_window("w");
    let (tx, cancel, handle) = spawn_actor(Arc::clone(&win), None);
    tx.send(msg("s", 0, 10_000_000_000, 0)).await.unwrap();
    tx.send(msg("s", 1, 20_000_000_000, 1)).await.unwrap();
    // Cancel before the actor necessarily processed them: the queued tail must
    // still be committed (bounded, non-blocking drain).
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("actor exits after cancellation")
        .unwrap();
    assert_eq!(appended_values(&win), vec![0, 1], "queued tail flushed");
}

// -- all senders dropped: flush loop + sequence-gap warning -------------------

#[tokio::test]
async fn sender_drop_flushes_pending_and_warns_on_gap() {
    let win = make_window("w");
    let (tx, _cancel, handle) = spawn_actor(Arc::clone(&win), None);
    // seq 0 commits (cursor → 1); seq 2 parks (a real gap at seq 1).
    tx.send(msg("s", 0, 10_000_000_000, 0)).await.unwrap();
    wait_for(&win, 1).await;
    tx.send(msg("s", 2, 30_000_000_000, 2)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(win.total_rows(), 1, "gap keeps seq 2 parked");

    // Dropping the only sender closes the channel: the actor exits, surfaces
    // the gap (log warning), and the parked message is dropped (permits freed).
    drop(tx);
    tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("actor exits when all senders drop")
        .unwrap();
    assert_eq!(
        appended_values(&win),
        vec![0],
        "gap message is not appended"
    );
}

// -- closed semaphore: acquisition returns empty (no hang) --------------------

#[tokio::test]
async fn acquire_budget_on_closed_semaphore_returns_empty() {
    let budget = Arc::new(Semaphore::new(4));
    budget.close();
    let permits = acquire_window_budget(&budget, 4, 8).await;
    assert!(permits.is_empty(), "closed budget yields no permits");
}

// -- reporter: appended vs late-dropped outcomes ------------------------------

#[tokio::test]
async fn report_observes_appended_and_late_dropped() {
    let win = make_window("w");
    let reports: Arc<Mutex<Vec<(String, usize, bool)>>> = Arc::new(Mutex::new(Vec::new()));
    let reports2 = Arc::clone(&reports);
    let report = Arc::new(move |name: &str, rows: usize, late: bool| {
        reports2
            .lock()
            .unwrap()
            .push((name.to_string(), rows, late));
    }) as WindowAppendReport;
    let (tx, _cancel, handle) = spawn_actor(Arc::clone(&win), Some(report));

    // Batch at t=20s: appended (in order) → report (w, 1, late=false).
    tx.send(msg("s", 0, 20_000_000_000, 1)).await.unwrap();
    wait_for(&win, 1).await;
    // Batch at t=5s: below the watermark (20s - 5s = 15s) → late-dropped →
    // report (w, 1, late=true).
    tx.send(msg("s", 1, 5_000_000_000, 2)).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while reports.lock().unwrap().len() < 2 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("both reports arrive");
    assert_eq!(win.total_rows(), 1, "late batch not appended");

    let got = reports.lock().unwrap().clone();
    assert_eq!(got[0], ("w".to_string(), 1, false));
    assert_eq!(got[1], ("w".to_string(), 1, true));

    cancel_and_drain(handle, tx).await;
}

async fn cancel_and_drain(handle: tokio::task::JoinHandle<()>, tx: mpsc::Sender<WindowMsg>) {
    drop(tx);
    tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("actor exits")
        .unwrap();
}
