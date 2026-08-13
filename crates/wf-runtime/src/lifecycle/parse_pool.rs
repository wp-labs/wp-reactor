use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use tokio::sync::mpsc;

use wf_engine::window::{ParsedRoute, Router};

use crate::metrics::RuntimeMetrics;

use super::types::TaskGroup;

/// Bounded buffer for the source → parse-worker channel.
pub(super) const PARSE_CHANNEL_CAPACITY: usize = 1024;
/// Bounded buffer for the parse-worker → commit-worker channel.
const COMMIT_CHANNEL_CAPACITY: usize = 1024;

/// A decoded batch handed from a source task to the parse worker pool.
///
/// `seq` is a monotonically increasing sequence assigned by the source, so the
/// single commit worker can re-assemble batches in source order even though N
/// parse workers finish out of order.
pub(super) struct ParseItem {
    pub seq: u64,
    pub stream_name: String,
    pub batch: RecordBatch,
}

/// A parsed batch handed from a parse worker to the (ordered) commit worker.
struct ParsedItem {
    seq: u64,
    batch: RecordBatch,
    parsed: ParsedRoute,
}

/// Spawn the parse worker pool into `group`: N parallel parsers + one ordered
/// commit worker. Returns the sender the source tasks push decoded batches into.
///
/// Parse workers run [`Router::route_parse`] in parallel; the single commit
/// worker runs [`Router::route_commit`] in `seq` order so watermark advancement
/// and rule broadcast stay in source order.
pub(super) fn spawn_parse_pool(
    router: &Arc<Router>,
    metrics: Option<Arc<RuntimeMetrics>>,
    worker_count: usize,
    group: &mut TaskGroup,
) -> mpsc::Sender<ParseItem> {
    let worker_count = worker_count.max(1);
    let (parse_tx, parse_rx) = mpsc::channel::<ParseItem>(PARSE_CHANNEL_CAPACITY);
    let (commit_tx, commit_rx) = mpsc::channel::<ParsedItem>(COMMIT_CHANNEL_CAPACITY);
    let parse_rx = Arc::new(tokio::sync::Mutex::new(parse_rx));

    // Ordered commit worker (single).
    let commit_router = Arc::clone(router);
    let commit_metrics = metrics.clone();
    group.push(tokio::spawn(async move {
        run_commit_worker(commit_rx, commit_router, commit_metrics).await;
        Ok(())
    }));

    // Parallel parse workers.
    for _ in 0..worker_count {
        let parse_rx = Arc::clone(&parse_rx);
        let commit_tx = commit_tx.clone();
        let router = Arc::clone(router);
        group.push(tokio::spawn(async move {
            run_parse_worker(parse_rx, commit_tx, router).await;
            Ok(())
        }));
    }
    // Drop our commit_tx clone so the commit channel closes once all parse
    // workers exit (letting the commit worker drain + stop).
    drop(commit_tx);

    parse_tx
}

/// Pull decoded batches from the shared parse channel, parse them in parallel,
/// and forward the parsed result to the commit worker.
async fn run_parse_worker(
    rx: Arc<tokio::sync::Mutex<mpsc::Receiver<ParseItem>>>,
    commit_tx: mpsc::Sender<ParsedItem>,
    router: Arc<Router>,
) {
    loop {
        let item = {
            let mut guard = rx.lock().await;
            guard.recv().await
        };
        let Some(item) = item else {
            // Parse channel closed: all sources finished.
            break;
        };
        let parsed = router.route_parse(&item.stream_name, &item.batch);
        if commit_tx
            .send(ParsedItem {
                seq: item.seq,
                batch: item.batch,
                parsed,
            })
            .await
            .is_err()
        {
            // Commit worker gone (shutdown).
            break;
        }
    }
}

/// Re-assemble parsed batches in `seq` order and commit them.
async fn run_commit_worker(
    mut rx: mpsc::Receiver<ParsedItem>,
    router: Arc<Router>,
    metrics: Option<Arc<RuntimeMetrics>>,
) {
    let mut next_seq: u64 = 0;
    let mut pending: BTreeMap<u64, ParsedItem> = BTreeMap::new();

    while let Some(item) = rx.recv().await {
        let seq = item.seq;
        pending.insert(seq, item);
        while let Some(item) = pending.remove(&next_seq) {
            commit(&router, &metrics, item);
            next_seq += 1;
        }
    }

    // Commit channel closed (all parse workers done): flush what remains, in
    // order. A missing `next_seq` would indicate a dropped batch (parse worker
    // aborted) — surface it rather than silently skipping.
    if !pending.is_empty() {
        let first = *pending.keys().next().expect("pending non-empty");
        if first != next_seq {
            wf_warn!(
                pipe,
                next_seq = next_seq,
                first_pending = first,
                pending = pending.len(),
                "parse commit sequence gap detected"
            );
        }
        while let Some(item) = pending.remove(&next_seq) {
            commit(&router, &metrics, item);
            next_seq += 1;
        }
    }
}

fn commit(router: &Router, metrics: &Option<Arc<RuntimeMetrics>>, item: ParsedItem) {
    if let Some(metrics) = metrics {
        metrics.inc_router_route_call();
    }
    match router.route_commit(item.batch, item.parsed) {
        Ok(report) => {
            if let Some(metrics) = metrics {
                metrics.add_route_report(&report);
            }
        }
        Err(e) => {
            wf_warn!(pipe, error = %e, "parse commit failed");
        }
    }
}
