use std::sync::Arc;
use std::time::Instant;

use crate::error::RuntimeResult;
use crate::metrics::{MonSend, RunSummary, RuntimeMetrics, TotalCounts};
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
                    let records = snap.to_records();
                    if sender.try_send(records).is_err() {
                        wf_debug!(sys, "monitor channel full, dropping metrics snapshot");
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
