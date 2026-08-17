// ---------------------------------------------------------------------------
// ConvStage — the P2c "transform operator"
//
// A lightweight, stateless aggregation stage that turns a rule's shards into a
// cross-shard conv (sort / top / dedup / where) + emit pipeline.
//
// Design (rule-sharding-and-aggregation-window.md, P2c):
//   - A fixed-window conv rule is shardable. Each shard emits RAW qualifying
//     closes (no inline conv) to this stage via a bounded channel.
//   - This stage accumulates closes into fixed buckets (`over` = the rule's
//     match window duration; bucket key = close.window_start_time_nanos
//     floored to the bucket boundary).
//   - A bucket is sealed only when EVERY shard's watermark has passed the
//     bucket end (barrier protocol) — a slow shard never loses closes.
//   - On seal: apply `conv::apply_conv` over the whole aggregated batch, apply
//     the shared rate limit (P2b), then emit each result via SinkFanout.
//
// Shutdown / EOS coordination (design §0.1 #4): on flush a shard sends a
// `drained` batch and publishes `i64::MAX` to its barrier slot; when all slots
// are MAX the stage seals everything and resets the barrier to `i64::MIN` so a
// subsequent input burst starts fresh.
// ---------------------------------------------------------------------------

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use wf_engine::alert::OutputRecord;
use wf_engine::match_engine::{CloseOutput, SharedLimits, apply_conv, close_is_qualified};
use wf_engine::window::Router;
use wf_lang::ast::FieldRef;
use wf_lang::plan::{ConvPlan, ExceedAction, LimitsPlan};

use crate::alert_task::{AlertBatch, SinkFanout};
use crate::engine_task::window_lookup::RegistryLookup;
use crate::error::RuntimeResult;
use crate::metrics::RuntimeMetrics;

/// P2⑤: a barrier that has not advanced for this long (while buckets are
/// pending) is considered stalled — a shard died or is blocked. Drop the stuck
/// buckets to bound memory.
const STALE_BARRIER_AFTER: std::time::Duration = std::time::Duration::from_secs(30);
/// Max wall time the conv stage keeps draining its close channel during
/// shutdown. Shards flush their final closes as part of shutdown, so the stage
/// must keep consuming until all shard senders drop the channel — otherwise a
/// late flush batch is lost (same bug family as the sink consumer drain).
const CONV_DRAIN_BUDGET: std::time::Duration = std::time::Duration::from_secs(1);

/// A batch of raw qualifying closes sent from one shard to the conv stage.
pub(crate) struct ConvCloseBatch {
    /// Raw qualifying closes (already filtered by [`close_is_qualified`]).
    pub(crate) closes: Vec<CloseOutput>,
    /// This shard's machine watermark after emitting these closes.
    pub(crate) watermark: i64,
    /// True when this shard is draining (flush / EOS): no more closes will
    /// follow from this shard for the current input burst.
    pub(crate) drained: bool,
    /// This shard's index into the shared barrier.
    pub(crate) barrier_index: usize,
}

/// Per-shard sink handle handed to each shard's `RuleTask`.
#[derive(Clone)]
pub(crate) struct ConvShardSink {
    pub(crate) tx: mpsc::Sender<ConvCloseBatch>,
    /// This shard's slot in the shared barrier (`Arc<Vec<AtomicI64>>`).
    pub(crate) barrier_index: usize,
}

pub(crate) struct ConvStageConfig {
    pub(crate) executor: wf_engine::match_engine::RuleExecutor,
    pub(crate) conv_plan: Option<ConvPlan>,
    pub(crate) keys: Arc<[FieldRef]>,
    pub(crate) over: Duration,
    pub(crate) limits: Option<LimitsPlan>,
    pub(crate) shared_limits: Option<Arc<SharedLimits>>,
    pub(crate) barrier: Arc<Vec<AtomicI64>>,
    pub(crate) sink_fanout: Arc<SinkFanout>,
    pub(crate) router: Arc<Router>,
    pub(crate) metrics: Option<Arc<RuntimeMetrics>>,
    pub(crate) rx: mpsc::Receiver<ConvCloseBatch>,
    pub(crate) cancel: CancellationToken,
    pub(crate) eos: watch::Receiver<u64>,
    pub(crate) timeout_scan_interval: Duration,
}

pub(crate) async fn run_conv_stage_task(config: ConvStageConfig) -> RuntimeResult<()> {
    let mut stage = ConvStageTask {
        executor: config.executor,
        conv_plan: config.conv_plan,
        keys: config.keys,
        over: config.over,
        limits: config.limits,
        shared_limits: config.shared_limits,
        barrier: config.barrier,
        sink_fanout: config.sink_fanout,
        router: config.router,
        metrics: config.metrics,
        rx: config.rx,
        cancel: config.cancel,
        eos: config.eos,
        timeout_scan_interval: config.timeout_scan_interval,
        buckets: BTreeMap::new(),
        last_min_wm: i64::MIN,
        last_min_wm_at: std::time::Instant::now(),
    };
    stage.run().await
}

struct ConvStageTask {
    executor: wf_engine::match_engine::RuleExecutor,
    conv_plan: Option<ConvPlan>,
    keys: Arc<[FieldRef]>,
    over: Duration,
    limits: Option<LimitsPlan>,
    shared_limits: Option<Arc<SharedLimits>>,
    barrier: Arc<Vec<AtomicI64>>,
    sink_fanout: Arc<SinkFanout>,
    router: Arc<Router>,
    metrics: Option<Arc<RuntimeMetrics>>,
    rx: mpsc::Receiver<ConvCloseBatch>,
    cancel: CancellationToken,
    eos: watch::Receiver<u64>,
    timeout_scan_interval: Duration,
    /// Aggregated closes per bucket start (nanos).
    buckets: BTreeMap<i64, Vec<CloseOutput>>,
    /// Last observed barrier minimum (for P2⑤ stale detection).
    last_min_wm: i64,
    /// When `last_min_wm` was last observed to change.
    last_min_wm_at: std::time::Instant,
}

impl ConvStageTask {
    async fn run(&mut self) -> RuntimeResult<()> {
        let mut timeout_tick = tokio::time::interval(self.timeout_scan_interval);
        loop {
            tokio::select! {
                biased;
                _ = self.cancel.cancelled() => {
                    // P2④: shards flush their final closes as part of
                    // shutdown, so keep consuming until all shard senders drop
                    // the channel (channel closed) or the drain budget
                    // expires — otherwise a late flush ConvCloseBatch arrives
                    // at an exited stage and is lost. Unsealed buckets are
                    // still dropped afterwards (partial top(N)/sort results
                    // are never emitted).
                    let deadline = Instant::now() + CONV_DRAIN_BUDGET;
                    loop {
                        if Instant::now() >= deadline {
                            break;
                        }
                        match self.rx.recv().await {
                            Some(b) => self.on_batch(b).await,
                            // All shard senders dropped: flush done.
                            None => break,
                        }
                    }
                    self.drain_and_drop().await;
                    self.check_all_drained();
                    break;
                }
                batch = self.rx.recv() => {
                    match batch {
                        Some(b) => self.on_batch(b).await,
                        None => {
                            // All shard senders dropped (shutdown): drain then drop.
                            self.drain_and_drop().await;
                            break;
                        }
                    }
                }
                _ = self.eos.changed() => self.try_seal().await,
                _ = timeout_tick.tick() => {
                    self.try_seal().await;
                    self.check_stale();
                }
            }
        }
        Ok(())
    }

    /// P2⑤: a shard whose barrier stops advancing (panic / stuck task) would
    /// leave buckets above `min_wm` accumulating unboundedly. Detect a stale
    /// barrier and drop the stuck buckets (bounding memory) with a loud warning —
    /// dropping is safe because a stuck shard's data is incomplete anyway.
    fn check_stale(&mut self) {
        if !should_clear_stale(self.buckets.len(), self.last_min_wm_at.elapsed()) {
            return;
        }
        log::warn!(
            "conv stage: barrier stalled for {:?} at min watermark {} — dropping {} unsealed bucket(s) to bound memory",
            self.last_min_wm_at.elapsed(),
            self.last_min_wm,
            self.buckets.len(),
        );
        self.buckets.clear();
        // Reset the clock so the warning does not spam every tick.
        self.last_min_wm_at = std::time::Instant::now();
    }

    /// Drain any batches already in the channel (processing them seals complete
    /// buckets), then DROP every still-unsealed bucket. Never emits partial
    /// results — a bucket is only sealed when ALL shards' watermarks pass its end
    /// (P2④).
    async fn drain_and_drop(&mut self) {
        while let Ok(batch) = self.rx.try_recv() {
            self.on_batch(batch).await;
        }
        if !self.buckets.is_empty() {
            log::warn!(
                "conv stage: dropping {} unsealed bucket(s) on shutdown (partial results not emitted)",
                self.buckets.len()
            );
            self.buckets.clear();
        }
    }

    async fn on_batch(&mut self, batch: ConvCloseBatch) {
        let slot = &self.barrier[batch.barrier_index];
        if batch.drained {
            slot.store(i64::MAX, Ordering::Release);
        } else {
            let cur = slot.load(Ordering::Acquire);
            slot.store(cur.max(batch.watermark), Ordering::Release);
        }
        if !batch.closes.is_empty() {
            let over_nanos = self.over.as_nanos() as i64;
            for close in batch.closes {
                let bucket = (close.window_start_time_nanos.div_euclid(over_nanos)) * over_nanos;
                self.buckets.entry(bucket).or_default().push(close);
            }
        }
        self.try_seal().await;
        self.check_all_drained();
    }

    /// Seal every bucket whose end has been passed by all shards.
    async fn try_seal(&mut self) {
        let min_wm = self
            .barrier
            .iter()
            .map(|a| a.load(Ordering::Acquire))
            .min()
            .unwrap_or(i64::MAX);
        // P2⑤: track when the barrier last advanced (for stale detection).
        if min_wm != self.last_min_wm {
            self.last_min_wm = min_wm;
            self.last_min_wm_at = std::time::Instant::now();
        }
        let ready = seal_candidates(self.buckets.keys().copied(), min_wm, self.over);
        for b in ready {
            if let Some(closes) = self.buckets.remove(&b) {
                self.process_bucket(closes).await;
            }
        }
    }

    async fn process_bucket(&mut self, closes: Vec<CloseOutput>) {
        // P1①: a FailRule latch (from any shard or this stage) stops the rule.
        if let Some(shared) = &self.shared_limits
            && shared.is_failed()
        {
            return;
        }
        // Apply conv over the whole cross-shard batch (empty conv_plan is a
        // no-op passthrough).
        let closes = match &self.conv_plan {
            Some(plan) => apply_conv(plan, &self.keys, closes),
            None => closes,
        };
        let lookup = RegistryLookup(&self.router);
        let mut records: Vec<OutputRecord> = Vec::new();
        for close in closes {
            if !close_is_qualified(&close) {
                continue;
            }
            if let Some(shared) = &self.shared_limits
                && let Some(rate) = self.limits.as_ref().and_then(|l| l.max_throttle.clone())
            {
                // Use the close's event-time watermark (same clock as the shards'
                // on-event throttling and the legacy inline close throttle) so the
                // shared budget is consumed in one clock domain.
                if !shared.try_acquire_throttle(close.watermark_nanos, &rate) {
                    // P1①: mirror the inline close path — FailRule latches the
                    // whole rule; Throttle / DropOldest just suppress this emit.
                    let fail = self
                        .limits
                        .as_ref()
                        .map(|l| matches!(l.on_exceed, ExceedAction::FailRule))
                        .unwrap_or(false);
                    if fail {
                        // N3: FailRule latches the rule permanently — stop
                        // processing the rest of this bucket too. Without the
                        // break, a later close whose watermark falls into a
                        // fresh throttle window would pass try_acquire_throttle
                        // and emit after the latch.
                        shared.fail();
                        break;
                    }
                    continue;
                }
            }
            if let Ok(Some(record)) = self.executor.execute_close_with_joins(&close, &lookup) {
                records.push(record);
            }
        }
        for record in records {
            self.send_record(record).await;
        }
    }

    /// Deliver one `OutputRecord` to the sink writers (row form). Conv output
    /// is a cold, low-rate path, so per-record delivery is acceptable; the
    /// columnar batching optimization (RuleTask emit) is a follow-up.
    ///
    /// Delivery blocks on a full sink channel (design §10: backpressure
    /// preserves alert delivery) instead of dropping.
    async fn send_record(&self, record: OutputRecord) {
        if let Some(metrics) = &self.metrics {
            metrics.inc_alert_emitted_total(&record.rule_name);
        }
        let target = record.yield_target.clone();
        let data_record = match record.to_data_record() {
            Ok(d) => d,
            Err(e) => {
                if let Some(metrics) = &self.metrics {
                    metrics.inc_alert_serialize_failed();
                }
                log::warn!("conv stage alert export error: {e}");
                return;
            }
        };
        let batch = AlertBatch::Rows(Arc::new(vec![Arc::new(data_record)]));
        let sink_groups = self.sink_fanout.resolve(&target);
        let mut sent = false;
        for (sink_ptr, channels) in sink_groups.iter() {
            let idx = self.sink_fanout.next_index(*sink_ptr, channels.len());
            let tx = &channels[idx];
            // P3-C: `try_send` first is a micro-optimization — on the common
            // (non-full) path it avoids the extra scheduler poll that `await`
            // would cost; on `Full` we fall back to a blocking `.await` send
            // (design §10: backpressure preserves alert delivery).
            match tx.try_send(batch.clone()) {
                Ok(_) => sent = true,
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    // Blocking backpressure (design §10) — never drop alerts.
                    if tx.send(batch.clone()).await.is_ok() {
                        sent = true;
                    }
                }
                Err(_) => {} // sink channel closed
            }
        }
        if !sent {
            log::warn!("conv stage: no sink channel for yield target {target}");
        }
    }

    /// After a full drain (all shards at `i64::MAX`), reset the barrier so a
    /// subsequent input burst starts fresh and post-flush late closes for an
    /// already-drained bucket are not lost.
    fn check_all_drained(&mut self) {
        if self
            .barrier
            .iter()
            .all(|a| a.load(Ordering::Acquire) == i64::MAX)
        {
            for slot in self.barrier.iter() {
                slot.store(i64::MIN, Ordering::Release);
            }
        }
    }
}

/// Buckets whose end (`b + over`) is ≤ `min_wm` — i.e. every shard's watermark
/// has passed the bucket end — are complete and can be sealed.
fn seal_candidates(
    bucket_keys: impl Iterator<Item = i64>,
    min_wm: i64,
    over: Duration,
) -> Vec<i64> {
    let seal_upto = min_wm.saturating_sub(over.as_nanos() as i64);
    bucket_keys.filter(|b| *b <= seal_upto).collect()
}

/// P2⑤: a barrier that has not advanced for `elapsed` while buckets are pending
/// is considered stalled — drop the stuck buckets to bound memory.
fn should_clear_stale(bucket_count: usize, elapsed: Duration) -> bool {
    bucket_count > 0 && elapsed > STALE_BARRIER_AFTER
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stale_detection_requires_buckets_and_long_stall() {
        assert!(!should_clear_stale(
            0,
            STALE_BARRIER_AFTER + Duration::from_secs(5)
        ));
        assert!(!should_clear_stale(
            3,
            STALE_BARRIER_AFTER - Duration::from_secs(5)
        ));
        assert!(should_clear_stale(
            3,
            STALE_BARRIER_AFTER + Duration::from_secs(5)
        ));
    }

    #[test]
    fn seal_waits_for_all_shards_to_pass_bucket_end() {
        // 60-second buckets; keys are nanosecond bucket starts.
        let over = Duration::from_secs(60);
        let ns = |s: i64| s * 1_000_000_000;
        let keys = [ns(0), ns(60), ns(120), ns(180)];
        // min watermark = 100s: only bucket 0 (end 60s <= 100s) is ready.
        assert_eq!(
            seal_candidates(keys.iter().copied(), ns(100), over),
            vec![ns(0)]
        );
        // min watermark = 150s: buckets 0 and 60 (ends 60s, 120s <= 150s) ready.
        assert_eq!(
            seal_candidates(keys.iter().copied(), ns(150), over),
            vec![ns(0), ns(60)]
        );
        // min watermark = 240s: all ready.
        assert_eq!(
            seal_candidates(keys.iter().copied(), ns(240), over),
            vec![ns(0), ns(60), ns(120), ns(180)]
        );
    }
}
