//! 监控指标任务组构造（2026-09-03 自 spawn.rs 拆出）：`spawn_metrics_task` 按配置
//! 拉起 metrics 上报任务，`run_monitor_consumer` 消费监控帧并转发到 sink dispatcher，
//! `metrics_record_to_data_record` 转 DataRecord。生命周期编排见 `lifecycle/mod.rs`。

use super::*;
use crate::metrics::MonSend;

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
    let mon_send = spawn_monitor_channel(&dispatcher);

    group.push(tokio::spawn(async move {
        run_metrics_task(metrics, metrics_config, router_clone, cancel, mon_send)
            .await
            .source_err(RuntimeReason::system_error(), "run metrics task")?;
        Ok(())
    }));
    Ok(group)
}

/// dispatcher 配置了 monitor sink 时建立 64 槽监控通道并拉起消费者，供
/// metrics 任务转发监控帧；否则返回 `None`。
fn spawn_monitor_channel(dispatcher: &Option<Arc<SinkDispatcher>>) -> Option<MonSend> {
    let dispatcher = dispatcher.as_ref()?;
    if !dispatcher.has_monitor_sinks() {
        return None;
    }
    let (tx, rx) = mpsc::channel::<Vec<MetricsRecord>>(64);
    let dispatcher = Arc::clone(dispatcher);
    tokio::spawn(async move {
        run_monitor_consumer(rx, dispatcher).await;
    });
    Some(tx)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_dispatcher() -> Arc<SinkDispatcher> {
        // 无路由/默认/error/monitor sink 的裸 dispatcher：monitor 派发为 noop。
        Arc::new(SinkDispatcher::new(vec![], vec![], vec![], vec![]))
    }

    #[test]
    fn monitor_channel_absent_without_monitor_sinks() {
        // None dispatcher 与未配置 monitor sink 的 dispatcher 都不建监控通道。
        assert!(spawn_monitor_channel(&None).is_none());
        assert!(spawn_monitor_channel(&Some(empty_dispatcher())).is_none());
    }

    #[tokio::test]
    async fn monitor_consumer_drains_batches_until_channel_closes() {
        let dispatcher = empty_dispatcher();
        let (tx, rx) = mpsc::channel::<Vec<MetricsRecord>>(4);
        let d = Arc::clone(&dispatcher);
        let consumer = tokio::spawn(async move {
            run_monitor_consumer(rx, d).await;
        });
        tx.send(vec![MetricsRecord {
            fields: vec![("stage".to_string(), "receiver".to_string())],
        }])
        .await
        .expect("send batch");
        tx.send(vec![MetricsRecord { fields: vec![] }])
            .await
            .expect("send batch");
        drop(tx);
        // 通道关闭后消费者逐条转发完毕并退出（空 monitor sink 为 noop，不 panic）。
        consumer.await.expect("consumer joins cleanly");
    }

    #[test]
    fn empty_record_keeps_only_timestamp_field() {
        let record = MetricsRecord { fields: vec![] };
        let data = metrics_record_to_data_record(&record);
        assert!(data.field("stage").is_none());
        let time = data
            .field("time")
            .map(|f| f.get_value().to_string())
            .expect("time field must always exist");
        chrono::DateTime::parse_from_rfc3339(&time).expect("time must be RFC3339");
    }
}
