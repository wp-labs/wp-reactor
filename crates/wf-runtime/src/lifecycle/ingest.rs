//! Source-side ingest path: decode → gate → rate-limit → project → route → dispatch.
//!
//! 2026-08-31 decode-route-merge（design: `decode-route-merge-design.md`）：
//! 原 parse worker 池（`route_parse` 在池内并行 + 共享 `Arc<Mutex<Receiver>>` +
//! ordered commit worker）整体移除，`route_parse` + `dispatch_parsed` 内联进
//! 源任务单循环。保序由源任务的严格顺序提供（actor 的 (source, window) 重排
//! 保留为多 handle `fetch_add` 竞态的兜底）；背压由 `dispatch_parsed` 的
//! per-window mailbox 字节预算（`window_buffer_bytes`）直达 TCP 读——原
//! `PrereadBudget` 深度节流层随之消融。
//!
//! sync 模式（无 mailbox 的测试/embedded）不需要单独的 commit worker：
//! [`Router::dispatch_parsed`] 对无 mailbox 的窗口走 `commit_window` 内联提交，
//! 而内联后的源任务本身就是严格有序的，无需再重排。

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use arrow::record_batch::RecordBatch;

use wf_engine::window::Router;

use crate::metrics::RuntimeMetrics;

/// Token-bucket ingest rate limiter (events/sec), learned from warp-parse's
/// `DynamicRateLimiter`. Tokens refill at `rate`; `acquire(events)` consumes
/// them and sleeps when exhausted, so the engine never ingests faster than the
/// configured cap even when a client sends flat-out — bounding the
/// allocation-throughput-driven RSS peak.
pub(crate) struct IngestLimiter {
    rate_per_sec: f64,
    tokens: Mutex<f64>,
    last_refill: Mutex<Instant>,
}

impl IngestLimiter {
    pub fn new(rate_per_sec: usize) -> Arc<Self> {
        Arc::new(Self {
            rate_per_sec: rate_per_sec as f64,
            tokens: Mutex::new(rate_per_sec as f64),
            last_refill: Mutex::new(Instant::now()),
        })
    }

    /// Consume `events` tokens, sleeping as needed to keep the long-run rate ≤
    /// `rate_per_sec`. Guards are scoped to a block so no `MutexGuard` crosses
    /// the `.await` (std guards are not `Send`).
    pub async fn acquire(&self, events: usize) {
        let n = events.max(1) as f64;
        let wait = {
            let mut tokens = self.tokens.lock().unwrap();
            let mut last = self.last_refill.lock().unwrap();
            let now = Instant::now();
            let elapsed = now.duration_since(*last).as_secs_f64();
            *tokens = (*tokens + elapsed * self.rate_per_sec).min(self.rate_per_sec);
            *last = now;
            if *tokens >= n {
                *tokens -= n;
                0.0
            } else {
                let need = n - *tokens;
                need / self.rate_per_sec
            }
        };
        if wait > 0.0 {
            tokio::time::sleep(Duration::from_secs_f64(wait)).await;
            let mut tokens = self.tokens.lock().unwrap();
            let mut last = self.last_refill.lock().unwrap();
            *tokens = 0.0;
            *last = Instant::now();
        }
    }
}

/// Route one decoded batch from the source task's single loop.
///
/// Order (identical to the pre-merge `push_decoded_batch` + parse worker):
/// perf-diag gate → ingest rate limit → receiver frame metrics → projection
/// (`prepare_batch`) → per-(source, window) seq allocation → global frame
/// seq → `route_parse` → `dispatch_parsed` (mailbox send with byte-budget
/// backpressure, or inline `commit_window` fallback in sync mode).
///
/// The batch lands in every subscribed window (or is dropped by the gate /
/// logged on dead actors) before this returns — there is no intermediate
/// queue and no failure mode to report.
pub(crate) async fn route_and_dispatch(
    parse_seq: &AtomicU64,
    source_name: &str,
    stream_name: &str,
    batch: RecordBatch,
    router: &Router,
    metrics: Option<&Arc<RuntimeMetrics>>,
    limiter: Option<&IngestLimiter>,
) {
    // perf-diag cut_append 门控：解码后即丢（测「注入 + 解码」前序段——不含
    // 窗口 append / fanout / 引擎 / 输出）。哨兵流豁免——测量协议（档位切换 +
    // EPS 计算）必须活着。普通流被切时直接释放批次，不进入路由；
    // 同时**绕过限速与帧/行计数**（limiter/指标在门控之后）——
    // 诊断档语义: 测的是注入+解码的原始速率（2026-08-25 review 补注）。
    if crate::perf_diag::perf_cut_append() && stream_name != crate::perf_diag::PERF_SENTINEL_STREAM
    {
        return;
    }
    if let Some(limiter) = limiter {
        limiter.acquire(batch.num_rows()).await;
    }
    if let Some(metrics) = metrics {
        metrics.add_receiver_frame(batch.num_rows());
        metrics.add_receiver_source_frame(source_name, batch.num_rows());
        let machine_id =
            crate::receiver::batch_machine_id(&batch).unwrap_or_else(|| source_name.to_string());
        metrics.add_receiver_source_machine_rows(source_name, &machine_id, batch.num_rows());
    }
    let projected = crate::receiver::prepare_batch(stream_name, &batch, router);
    // Per-(source, window) seq allocation MUST happen at this serialized
    // point (source order still guaranteed): the window actors' reorder
    // cursors expect a gap-free sequence per window. Inlined into the source
    // loop, order is guaranteed by construction — the actor reorder stays as
    // the fallback for the multi-handle `fetch_add` race.
    let window_seqs = router.next_window_seqs(source_name, stream_name);
    let seq = parse_seq.fetch_add(1, Ordering::Relaxed);
    if let Some(metrics) = metrics {
        metrics.inc_router_route_call();
    }
    let parsed = router.route_parse(stream_name, &projected);
    if let Some(metrics) = metrics {
        metrics.add_router_skipped(parsed.skipped_non_local);
    }
    router
        .dispatch_parsed(Arc::from(source_name), seq, window_seqs, projected, parsed)
        .await;
}

/// Blocking flavour of [`route_and_dispatch`] for `spawn_blocking` replay
/// paths (arrow IPC file source): drives the async route+dispatch on the
/// current runtime handle. Must only be called from a **blocking** thread —
/// never from an async context (would panic).
pub(crate) fn route_and_dispatch_blocking(
    parse_seq: &AtomicU64,
    source_name: &str,
    stream_name: &str,
    batch: RecordBatch,
    router: &Router,
    metrics: Option<&Arc<RuntimeMetrics>>,
    limiter: Option<&IngestLimiter>,
) {
    let handle = tokio::runtime::Handle::try_current()
        .expect("route_and_dispatch_blocking must run inside a tokio runtime (spawn_blocking)");
    handle.block_on(route_and_dispatch(
        parse_seq,
        source_name,
        stream_name,
        batch,
        router,
        metrics,
        limiter,
    ));
}

#[cfg(test)]
mod tests {
    #![allow(clippy::await_holding_lock)] // perf-diag cut_append 测试跨 await 持全局锁
    use super::*;

    /// cut_append 门控：普通流解码后即丢（不进路由、不记帧指标）; 哨兵流豁免
    /// （测量协议必须活）。未切时普通流照常进入（帧指标计入 rows_total）。
    #[tokio::test]
    async fn route_and_dispatch_cut_append_drops_non_sentinel() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field as ArrowField, Schema};
        use wf_engine::window::{Router, WindowRegistry};

        let _g = crate::perf_diag::PERF_CUT_SERIAL
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let parse_seq = AtomicU64::new(0);
        let router = Router::new(WindowRegistry::build(vec![]).expect("registry"));
        let schema = Arc::new(Schema::new(vec![ArrowField::new(
            "v",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3])) as _],
        )
        .unwrap();
        let metrics = Arc::new(RuntimeMetrics::new(
            &[],
            &[],
            &["s1".to_string()],
            std::collections::BTreeMap::new(),
        ));
        // snapshot 按读取重置计数器 → 用增量断言（delta 语义对累加口径同样成立）。
        let rows_total = |metrics: &Arc<RuntimeMetrics>, source: &str| -> u64 {
            metrics
                .snapshot()
                .to_records()
                .into_iter()
                .find(|r| {
                    r.fields
                        .iter()
                        .any(|(k, v)| k == "name" && v == "rows_total")
                        && r.fields.iter().any(|(k, v)| k == "label" && v == source)
                })
                .and_then(|r| {
                    r.fields
                        .iter()
                        .find(|(k, _)| k == "value")
                        .and_then(|(_, v)| v.parse::<u64>().ok())
                })
                .unwrap_or(0)
        };

        // 未切: 普通流进入 ingest（rows_total 计入）。
        crate::perf_diag::set_perf_cuts(false, false, false, false, false);
        route_and_dispatch(
            &parse_seq,
            "s1",
            "bid_events",
            batch.clone(),
            &router,
            Some(&metrics),
            None,
        )
        .await;
        let after_uncut = rows_total(&metrics, "s1");
        assert_eq!(after_uncut, 3, "未切: 普通流进入 ingest");

        // cut_append: 普通流解码后即丢（不进路由、不记帧指标）; 哨兵流放行。
        crate::perf_diag::set_perf_cuts(false, false, true, false, false);
        route_and_dispatch(
            &parse_seq,
            "s1",
            "bid_events",
            batch.clone(),
            &router,
            Some(&metrics),
            None,
        )
        .await;
        let after_cut = rows_total(&metrics, "s1");
        assert_eq!(
            after_cut, 0,
            "cut_append: 普通流被丢，帧指标不增（snapshot 按读取重置，0 = 本区间无帧计入）"
        );

        route_and_dispatch(
            &parse_seq,
            "s1",
            crate::perf_diag::PERF_SENTINEL_STREAM,
            batch,
            &router,
            Some(&metrics),
            None,
        )
        .await;
        let after_sentinel = rows_total(&metrics, "s1");
        assert_eq!(
            after_sentinel, 3,
            "cut_append: 哨兵流豁免（测量协议必须活）"
        );
        crate::perf_diag::reset_perf_diag();
    }
}
