//! 监控指标任务组构造（2026-09-03 自 spawn.rs 拆出）：`spawn_metrics_task` 按配置
//! 拉起 metrics 上报任务，`run_monitor_consumer` 消费监控帧并转发到 sink dispatcher，
//! `metrics_record_to_data_record` 转 DataRecord。生命周期编排见 `lifecycle/mod.rs`。

use super::*;

pub(crate) async fn spawn_metrics_task(
    config: &FusionConfig,
    router: &Arc<Router>,
    cancel: CancellationToken,
    metrics: Option<Arc<RuntimeMetrics>>,
    dispatcher: Option<Arc<SinkDispatcher>>,
) -> RuntimeResult<TaskGroup> {
    let mut group = TaskGroup::new("metrics");
    if !config.metrics.enabled {
        return Ok(group);
    }
    let Some(metrics) = metrics else {
        return Ok(group);
    };
    let router_clone = Arc::clone(router);
    let metrics_config = config.metrics.clone();

    // Create monitor channel if dispatcher is available
    let mon_send = match dispatcher {
        Some(ref d) if d.has_monitor_sinks() => {
            let (tx, rx) = mpsc::channel::<Vec<MetricsRecord>>(64);
            let d = Arc::clone(d);
            tokio::spawn(async move {
                run_monitor_consumer(rx, d).await;
            });
            Some(tx)
        }
        _ => None,
    };

    group.push(tokio::spawn(async move {
        run_metrics_task(metrics, metrics_config, router_clone, cancel, mon_send)
            .await
            .source_err(RuntimeReason::system_error(), "run metrics task")?;
        Ok(())
    }));
    Ok(group)
}

async fn run_monitor_consumer(mut rx: MonRecv, dispatcher: Arc<SinkDispatcher>) {
    while let Some(records) = rx.recv().await {
        for record in records {
            let data = metrics_record_to_data_record(&record);
            dispatcher.dispatch_to_monitor(&data).await;
        }
    }
    // Monitor channel closed: this consumer exits, but the monitor sinks are
    // stopped by `Reactor::wait` after the final metrics export (the shutdown
    // flush emits land after the metrics task's last tick, so stopping the
    // sinks here would drop the tail-of-stream counters from metrics.ndjson).
}

pub(crate) fn metrics_record_to_data_record(record: &MetricsRecord) -> DataRecord {
    let mut out = DataRecord::default();
    for (key, value) in &record.fields {
        let field = Field::new(DataType::Chars, key, Value::from(value.as_str()));
        out.push(FieldStorage::from_owned(field));
    }
    // 墙钟时间戳（RFC3339 UTC，与 daemon 日志同格式）：metrics.ndjson 每行可
    // 与墙梯档位时间线对齐（2026-08-31 排查——此前无时间戳，采样无法对应
    // recv/decode/floor/rules/full 各档）。
    let ts = chrono::Utc::now().to_rfc3339();
    out.push(FieldStorage::from_owned(Field::new(
        DataType::Chars,
        "time",
        Value::from(ts.as_str()),
    )));
    out
}
