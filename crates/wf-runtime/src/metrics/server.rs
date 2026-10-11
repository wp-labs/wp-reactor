use std::sync::Arc;
use std::time::Instant;

use crate::error::RuntimeResult;
use crate::metrics::{MonSend, MonitorFilter, RunSummary, RuntimeMetrics, TotalCounts};
use tokio_util::sync::CancellationToken;
use wf_config::MetricsConfig;
use wf_engine::window::Router;

pub async fn run_metrics_task(
    metrics: Arc<RuntimeMetrics>,
    config: MetricsConfig,
    router: Arc<Router>,
    cancel: CancellationToken,
    mon_send: Option<MonSend>,
) -> RuntimeResult<()> {
    wf_info!(
        sys,
        listen = %config.prometheus_listen,
        interval = %config.report_interval,
        "metrics exporter started"
    );

    metrics.sample_windows(&router);
    // 编译 [metrics.monitor] 筛选规则（#115）：在进入 monitor 通道前剔除被排除
    // 的记录——不会影响规则处理/告警输出，只裁剪 monitor fan-out。
    let monitor_filter = MonitorFilter::from_config(&config.monitor);
    // 告警（非拒绝）：若排除了 bench/诊断脚本会回读的指标，提示可能误配——
    // 这些指标的静默丢失会让相关校验读不到数据。
    for (stage, name) in monitor_filter.excluded_external_metrics() {
        wf_warn!(
            res,
            stage,
            name,
            "[metrics.monitor] 排除了被 bench/诊断脚本回读的指标；\
             相关校验（emitted_total/append_total/acked_lag 等）会读不到该指标"
        );
    }
    let mut tick = tokio::time::interval(config.report_interval.as_duration());
    tick.tick().await;
    let task_started = Instant::now();
    let mut prev = metrics.interval_snapshot(Instant::now());
    let start = prev;
    let mut run_summary = RunSummary::default();
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tick.tick() => {
                metrics.sample_windows(&router);
                // #61: console_output gates the periodic `res`-domain
                // summary log. prometheus export + Top-N run regardless.
                if config.console_output {
                    wf_info!(res, summary = %metrics.summary_line(), "metrics snapshot");
                }
                let curr = metrics.interval_snapshot(Instant::now());
                if let Some(rates) = metrics.interval_rates(prev, curr) {
                    run_summary.observe(rates);
                    if config.console_output {
                        wf_info!(res, "{}", metrics.interval_table(rates));
                    }
                }
                prev = curr;

                if let Some(ref sender) = mon_send {
                    let snap = metrics.snapshot();
                    let mut records = snap.to_records();
                    monitor_filter.retain_allowed(&mut records);
                    // Backpressure instead of drop: `snapshot()` above already
                    // drained (swap(0)) every counter, so a `try_send` on a full
                    // channel would silently lose that drained increment — the
                    // append/emit totals the bench relies on would undercount
                    // (q3 emitted_total / q5 append_total both drifted ~15-25%).
                    // `send().await` parks the metrics loop until the monitor
                    // consumer drains, so no drained batch is ever discarded.
                    if sender.send(records).await.is_err() {
                        wf_debug!(sys, "monitor channel closed, dropping metrics snapshot");
                    }
                }
            }
        }
    }

    // Include the last partial interval before shutdown in final stats.
    metrics.sample_windows(&router);
    let final_snap = metrics.interval_snapshot(Instant::now());
    if let Some(rates) = metrics.interval_rates(prev, final_snap) {
        run_summary.observe(rates);
    }
    let totals = TotalCounts {
        rows: final_snap.rx_rows.saturating_sub(start.rx_rows),
        late: final_snap.dropped_late.saturating_sub(start.dropped_late),
        rules: final_snap.rule_matches.saturating_sub(start.rule_matches),
        out: final_snap
            .alert_dispatch
            .saturating_sub(start.alert_dispatch),
        sm_delta: final_snap.rule_instances as i64 - start.rule_instances as i64,
    };

    if config.console_output
        && let Some(table) = run_summary.table(Some(totals))
    {
        wf_info!(
            res,
            runtime = ?task_started.elapsed(),
            intervals = run_summary.interval_count,
            "{}",
            table
        );
    }
    Ok(())
}
