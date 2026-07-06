use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Once};
use std::time::Duration;

use orion_error::conversion::{SourceErr, ToStructError};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use wf_config::FusionConfig;
use wf_engine::alert::OutputRecord;
use wf_engine::sink::SinkDispatcher;
use wf_engine::window::{Evictor, Router, WindowRegistry};

use crate::alert_task;
use crate::engine_task::{RuleTaskConfig, WindowSource, run_rule_task};
use crate::error::{RuntimeReason, RuntimeResult};
use crate::evictor_task;
use crate::metrics::{MetricsRecord, MonRecv, RuntimeMetrics, run_metrics_task};
use crate::receiver::{
    replay_arrow_framed_file, replay_arrow_ipc_file, replay_csv_file, replay_ndjson_file,
    resolve_stream_schema,
};
use crate::source::DataSourceBatchSource;
use wf_connector_api::BatchSource;
use wp_core_connectors::sources::batch::arrow::WireFormat;
use wp_model_core::model::{DataRecord, DataType, Field, FieldStorage, Value};

use super::types::{RunRule, RunRuleKind, TaskGroup};

// ---------------------------------------------------------------------------
// Phase 2: task spawn helpers — each creates channel + spawns task
// ---------------------------------------------------------------------------

/// Spawn the alert pipeline: build channel, spawn consumer task.
/// Returns (alert_tx, task_group).
pub(super) fn spawn_alert_task(
    dispatcher: Arc<SinkDispatcher>,
    metrics: Option<Arc<RuntimeMetrics>>,
) -> (mpsc::Sender<OutputRecord>, TaskGroup) {
    let (alert_tx, alert_rx) = mpsc::channel(alert_task::ALERT_CHANNEL_CAPACITY);
    let mut group = TaskGroup::new("alert");
    group.push(tokio::spawn(async move {
        alert_task::run_alert_dispatcher(alert_rx, dispatcher, metrics).await;
        Ok(())
    }));
    (alert_tx, group)
}

/// Spawn the periodic window evictor task.
pub(super) fn spawn_evictor_task(
    config: &FusionConfig,
    router: &Arc<Router>,
    cancel: CancellationToken,
    metrics: Option<Arc<RuntimeMetrics>>,
) -> TaskGroup {
    let evictor = Evictor::new(config.window_defaults.max_total_bytes.as_bytes());
    let evict_interval = config.window_defaults.evict_interval.as_duration();
    let router = Arc::clone(router);
    let mut group = TaskGroup::new("evictor");
    group.push(tokio::spawn(async move {
        evictor_task::run_evictor(evictor, router, evict_interval, cancel, metrics).await;
        Ok(())
    }));
    group
}

/// Spawn one independent task per compiled rule.
///
/// Each rule task owns its `CepStateMachine` exclusively (no `Arc<Mutex>`).
/// It subscribes to window notifications and uses cursor-based `read_since()`
/// to pull new batches.
pub(super) fn spawn_rule_tasks(
    rules: Vec<RunRule>,
    router: &Arc<Router>,
    intermediate_targets: &HashSet<String>,
    alert_tx: mpsc::Sender<OutputRecord>,
    cancel: CancellationToken,
    metrics: Option<Arc<RuntimeMetrics>>,
) -> TaskGroup {
    let mut group = TaskGroup::new("rules");
    let timeout_scan_interval = Duration::from_secs(1);

    for rule in rules {
        let (machine, each_alias, each_time_field) = match rule.kind {
            RunRuleKind::Match(machine) => (Some(*machine), None, None),
            RunRuleKind::Each { alias, time_field } => (None, Some(alias), time_field),
        };
        let window_sources = resolve_window_sources(&rule.window_aliases, router.registry());

        let task_config = RuleTaskConfig {
            machine,
            each_alias,
            each_time_field,
            executor: rule.executor,
            window_sources,
            alert_tx: alert_tx.clone(),
            cancel: cancel.child_token(),
            timeout_scan_interval,
            router: Arc::clone(router),
            metrics: metrics.clone(),
            intermediate_targets: intermediate_targets.clone(),
        };

        group.push(tokio::spawn(
            async move { run_rule_task(task_config).await },
        ));
    }

    // Drop our copy of alert_tx so the alert channel closes when all rule
    // tasks finish.
    drop(alert_tx);

    group
}

/// Resolve which windows a rule needs to subscribe to, based on its direct
/// bind.window → alias mapping.
pub(super) fn resolve_window_sources(
    window_aliases: &HashMap<String, Vec<String>>,
    registry: &WindowRegistry,
) -> Vec<WindowSource> {
    let mut sources = Vec::new();

    for (window_name, aliases) in window_aliases {
        if let Some(window) = registry.get_window(window_name)
            && let Some(notify) = registry.get_notifier(window_name)
        {
            sources.push(WindowSource {
                window_name: window_name.clone(),
                window,
                notify,
                aliases: aliases.clone(),
            });
        }
    }

    sources
}

/// Bind the receiver and spawn its tasks.
/// Returns the receiver task group.
pub(super) async fn spawn_receiver_task(
    config: &FusionConfig,
    router: Arc<Router>,
    cancel: CancellationToken,
    metrics: Option<Arc<RuntimeMetrics>>,
    schemas: &[wf_lang::WindowSchema],
    base_dir: &Path,
) -> RuntimeResult<TaskGroup> {
    let mut group = TaskGroup::new("receiver");
    let mut spawned = 0usize;
    let schema_catalog = Arc::new(schemas.to_vec());
    register_builtin_external_sources();

    for (source_idx, source) in config.sources.iter().enumerate() {
        if !source.enabled {
            continue;
        }
        let source_name = source.effective_name(source_idx);
        // Resolve connect → kind if needed
        let kind = if let Some(ref conn) = source.connect {
            resolve_connector_kind(conn).unwrap_or_else(|| {
                // Fallback: try legacy source_type
                source.kind().to_string()
            })
        } else {
            source.kind().to_string()
        };
        match kind.as_str() {
            "file" => {
                let path_str = source.params.get("path").map(|s| s.as_str()).unwrap_or("");
                let path = resolve_source_path(base_dir, path_str);
                let stream = source.params.get("stream").cloned().unwrap_or_default();
                let router = Arc::clone(&router);
                let metrics = metrics.clone();
                let cancel = cancel.child_token();
                let format = source
                    .params
                    .get("data_format")
                    .cloned()
                    .unwrap_or_else(|| "ndjson".into());
                let schemas = Arc::clone(&schema_catalog);
                let source_name = source_name.clone();
                group.push(tokio::spawn(async move {
                    match format.as_str() {
                        "ndjson" => {
                            replay_ndjson_file(
                                &path,
                                &stream,
                                &source_name,
                                schemas.as_slice(),
                                router,
                                metrics,
                                cancel,
                            )
                            .await?
                        }
                        "csv" => {
                            replay_csv_file(
                                &path,
                                &stream,
                                &source_name,
                                schemas.as_slice(),
                                router,
                                metrics,
                                cancel,
                            )
                            .await?
                        }
                        "arrow_framed" => {
                            replay_arrow_framed_file(
                                &path,
                                &stream,
                                &source_name,
                                schemas.as_slice(),
                                router,
                                metrics,
                                cancel,
                            )
                            .await?
                        }
                        "arrow_ipc" => {
                            replay_arrow_ipc_file(
                                &path,
                                &stream,
                                &source_name,
                                schemas.as_slice(),
                                router,
                                metrics,
                                cancel,
                            )
                            .await?
                        }
                        _ => {
                            return Err(RuntimeReason::system_error()
                                .to_err()
                                .with_detail(format!("unsupported format: {format}")));
                        }
                    }
                    Ok(())
                }));
                spawned += 1;
            }
            _ => {
                spawned += spawn_external_source_tasks(
                    source,
                    &kind,
                    spawned,
                    base_dir,
                    &schema_catalog,
                    &router,
                    metrics.clone(),
                    cancel.child_token(),
                    &mut group,
                )
                .await?;
            }
        }
    }

    if spawned == 0 {
        return RuntimeReason::Bootstrap
            .to_err()
            .with_detail("no enabled sources configured")
            .err();
    }

    Ok(group)
}

fn resolve_source_path(base_dir: &Path, path: &str) -> PathBuf {
    let p = Path::new(path);
    if p.is_absolute() {
        p.to_path_buf()
    } else {
        base_dir.join(p)
    }
}

/// Resolve a connector id (e.g. `"kafka_src"`) to its kind (e.g. `"kafka"`)
/// via the global connector registry.
fn resolve_connector_kind(connector_id: &str) -> Option<String> {
    wp_core_connectors::registry::registered_source_defs()
        .into_iter()
        .find(|def| def.id == connector_id)
        .map(|def| def.kind)
}

fn register_builtin_external_sources() {
    static REGISTER: Once = Once::new();
    REGISTER.call_once(|| {
        wp_core_connectors::sources::register_file_factory();
        wp_core_connectors::sources::tcp::register_tcp_factory();
        wp_core_connectors::sources::syslog::register_syslog_factory();
    });
}

#[allow(clippy::too_many_arguments)]
async fn spawn_external_source_tasks(
    source: &wf_config::SourceConfig,
    source_kind: &str,
    source_idx: usize,
    base_dir: &Path,
    schemas: &Arc<Vec<wf_lang::WindowSchema>>,
    router: &Arc<Router>,
    metrics: Option<Arc<RuntimeMetrics>>,
    cancel: CancellationToken,
    group: &mut TaskGroup,
) -> RuntimeResult<usize> {
    let Some(factory) = wp_core_connectors::registry::get_source_factory(source_kind) else {
        return RuntimeReason::Bootstrap
            .to_err()
            .with_detail(format!(
                "no factory registered for source kind {source_kind:?}"
            ))
            .err();
    };

    let stream_name = source.params.get("stream").cloned().unwrap_or_default();
    let format = WireFormat::from_data_format(source.params.get("data_format").map(|s| s.as_str()));

    // Arrow formats carry their own schema in the IPC stream; only NDJSON
    // needs a pre-resolved window schema.
    let schema_needs_resolve = matches!(format, WireFormat::Ndjson) && !stream_name.is_empty();
    let schema = if schema_needs_resolve {
        resolve_stream_schema(schemas.as_slice(), &stream_name)?
    } else {
        // Empty schema placeholder — Arrow data carries its own schema.
        Arc::new(arrow::datatypes::Schema::empty())
    };
    let mut params = wp_connector_api::ParamMap::new();
    for (key, value) in &source.params {
        params.insert(key.clone(), source_param_to_json(value));
    }
    let source_spec = wp_connector_api::SourceSpec {
        name: source.effective_name(source_idx),
        kind: source_kind.to_string(),
        connector_id: source.connect.clone().unwrap_or_default(),
        params,
        tags: Vec::new(),
    };

    factory.validate_spec(&source_spec).source_err(
        RuntimeReason::Bootstrap,
        format!("validate source {:?}", source_spec.name),
    )?;

    let mut svc = factory
        .build(
            &source_spec,
            &wp_connector_api::SourceBuildCtx::new(base_dir.to_path_buf()),
        )
        .await
        .source_err(
            RuntimeReason::Bootstrap,
            format!("build source {:?}", source_spec.name),
        )?;

    let mut spawned = 0usize;
    if let Some(mut acceptor) = svc.acceptor.take() {
        let cancel = cancel.child_token();
        group.push(tokio::spawn(async move {
            let (ctrl_tx, ctrl_rx) = async_broadcast::broadcast(1);
            tokio::select! {
                result = acceptor.acceptor.accept_connection(ctrl_rx) => {
                    result.map_err(|e| RuntimeReason::system_error().to_err().with_source(e))
                }
                _ = cancel.cancelled() => {
                    let _ = ctrl_tx.broadcast(wp_connector_api::ControlEvent::Stop).await;
                    Ok(())
                },
            }
        }));
        spawned += 1;
    }

    for mut handle in svc.sources {
        let router = Arc::clone(router);
        let metrics = metrics.clone();
        let cancel = cancel.child_token();
        let stream_name = stream_name.clone();
        let source_name = source.effective_name(source_idx);
        let source_kind = source_kind.to_string();
        let schema = Arc::clone(&schema);
        group.push(tokio::spawn(async move {
            // Start the source if needed (e.g. TCP source checks started flag).
            let (_ctrl_tx, ctrl_rx) = async_broadcast::broadcast(1);
            let _ = handle.source.start(ctrl_rx).await;

            // Wrap the raw DataSource as a BatchSource — all Arrow IPC / NDJSON
            // decode happens inside the adapter, returning Vec<RecordBatch>.
            let mut batch_source = DataSourceBatchSource::new(
                handle.metadata.name.clone(),
                handle.source,
                schema,
                format,
            );

            let mut consecutive_errors: u32 = 0;
            loop {
                tokio::select! {
                    result = batch_source.receive_batch() => match result {
                        Ok(batches) if !batches.is_empty() => {
                            consecutive_errors = 0;
                            for rb in batches {
                                // For ArrowFramed, prefer the per-frame tag
                                // (stream name embedded in the wp_arrow IPC header)
                                // when no explicit stream is configured.
                                let route_stream =
                                    if stream_name.is_empty() {
                                        batch_source
                                            .last_stream_tag()
                                            .unwrap_or(&stream_name)
                                            .to_string()
                                    } else {
                                        stream_name.clone()
                                    };
                                if let Err(e) = crate::receiver::route_batch(
                                    &route_stream,
                                    &source_name,
                                    rb,
                                    router.as_ref(),
                                    metrics.as_ref(),
                                ) {
                                    if let Some(metrics) = &metrics {
                                        metrics.inc_route_error(&source_name);
                                    }
                                    wf_warn!(
                                        conn,
                                        kind = %source_kind,
                                        stream = %stream_name,
                                        error = %e,
                                        "external source route error"
                                    );
                                }
                            }
                        }
                        Ok(_) => {}
                        Err(e) => {
                            // EOF: source has ended — stop the task.
                            if e.reason() == &wf_connector_api::SourceReason::EOF {
                                wf_debug!(
                                    conn,
                                    kind = %source_kind,
                                    stream = %stream_name,
                                    "source reached EOF"
                                );
                                break;
                            }
                            if consecutive_errors == 0 {
                                wf_warn!(
                                    conn,
                                    kind = %source_kind,
                                    stream = %stream_name,
                                    error = %e,
                                    "source receive error, will retry"
                                );
                            }
                            if let Some(metrics) = &metrics {
                                metrics.inc_receiver_decode_error();
                                metrics.inc_receiver_source_decode_error(&source_name);
                            }
                            consecutive_errors = consecutive_errors.saturating_add(1);
                            let delay = if consecutive_errors <= 1 {
                                std::time::Duration::from_millis(500)
                            } else {
                                std::time::Duration::from_secs(5)
                            };
                            tokio::time::sleep(delay).await;
                        }
                    },
                    _ = cancel.cancelled() => break,
                }
            }
            Ok(())
        }));
        spawned += 1;
    }

    if spawned == 0 {
        return RuntimeReason::Bootstrap
            .to_err()
            .with_detail(format!(
                "source kind {:?} built no readable source handles",
                source_kind
            ))
            .err();
    }

    Ok(spawned)
}

fn source_param_to_json(value: &str) -> serde_json::Value {
    let trimmed = value.trim();
    match trimmed {
        "true" => return serde_json::Value::Bool(true),
        "false" => return serde_json::Value::Bool(false),
        _ => {}
    }
    if let Ok(parsed) = trimmed.parse::<i64>() {
        return serde_json::Value::Number(parsed.into());
    }
    if let Ok(parsed) = trimmed.parse::<f64>()
        && let Some(number) = serde_json::Number::from_f64(parsed)
    {
        return serde_json::Value::Number(number);
    }
    serde_json::Value::String(value.to_string())
}

pub(super) async fn spawn_metrics_task(
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
}

fn metrics_record_to_data_record(record: &MetricsRecord) -> DataRecord {
    let mut out = DataRecord::default();
    for (key, value) in &record.fields {
        let field = Field::new(DataType::Chars, key, Value::from(value.as_str()));
        out.push(FieldStorage::from_owned(field));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::source_param_to_json;

    #[test]
    fn source_param_to_json_preserves_connector_types() {
        assert_eq!(source_param_to_json("5514"), serde_json::json!(5514));
        assert_eq!(source_param_to_json("true"), serde_json::json!(true));
        assert_eq!(source_param_to_json("1.5"), serde_json::json!(1.5));
        assert_eq!(
            source_param_to_json("0.0.0.0"),
            serde_json::json!("0.0.0.0")
        );
    }
}
