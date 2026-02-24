use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use orion_error::prelude::*;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use wf_config::FusionConfig;
use wf_core::alert::OutputRecord;
use wf_core::sink::SinkDispatcher;
use wf_core::window::{Evictor, Router, WindowRegistry};

use crate::alert_task;
use crate::engine_task::{RuleTaskConfig, WindowSource, run_rule_task};
use crate::error::RuntimeResult;
use crate::evictor_task;
use crate::receiver::Receiver;

use super::types::{RunRule, TaskGroup};

// ---------------------------------------------------------------------------
// Phase 2: task spawn helpers — each creates channel + spawns task
// ---------------------------------------------------------------------------

/// Spawn the alert pipeline: build channel, spawn consumer task.
/// Returns (alert_tx, task_group).
pub(super) fn spawn_alert_task(
    dispatcher: Arc<SinkDispatcher>,
) -> (mpsc::Sender<OutputRecord>, TaskGroup) {
    let (alert_tx, alert_rx) = mpsc::channel(alert_task::ALERT_CHANNEL_CAPACITY);
    let mut group = TaskGroup::new("alert");
    group.push(tokio::spawn(async move {
        alert_task::run_alert_dispatcher(alert_rx, dispatcher).await;
        Ok(())
    }));
    (alert_tx, group)
}

/// Spawn the periodic window evictor task.
pub(super) fn spawn_evictor_task(
    config: &FusionConfig,
    router: &Arc<Router>,
    cancel: CancellationToken,
) -> TaskGroup {
    let evictor = Evictor::new(config.window_defaults.max_total_bytes.as_bytes());
    let evict_interval = config.window_defaults.evict_interval.as_duration();
    let router = Arc::clone(router);
    let mut group = TaskGroup::new("evictor");
    group.push(tokio::spawn(async move {
        evictor_task::run_evictor(evictor, router, evict_interval, cancel).await;
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
    schemas: &[wf_lang::WindowSchema],
    alert_tx: mpsc::Sender<OutputRecord>,
    _config: &FusionConfig,
    cancel: CancellationToken,
) -> TaskGroup {
    let mut group = TaskGroup::new("rules");
    let timeout_scan_interval = Duration::from_secs(1);

    for rule in rules {
        let window_sources =
            resolve_window_sources(&rule.stream_aliases, schemas, router.registry());

        let task_config = RuleTaskConfig {
            machine: rule.machine,
            executor: rule.executor,
            window_sources,
            stream_aliases: rule.stream_aliases,
            alert_tx: alert_tx.clone(),
            cancel: cancel.child_token(),
            timeout_scan_interval,
            router: Arc::clone(router),
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

/// Resolve which windows a rule needs to subscribe to, based on its
/// stream_aliases (stream → alias mapping) and the window schemas (which
/// define which streams flow into each window).
pub(super) fn resolve_window_sources(
    stream_aliases: &HashMap<String, Vec<String>>,
    schemas: &[wf_lang::WindowSchema],
    registry: &WindowRegistry,
) -> Vec<WindowSource> {
    // Collect all stream names this engine cares about.
    let interested_streams: std::collections::HashSet<&str> =
        stream_aliases.keys().map(|s| s.as_str()).collect();

    // For each window schema, check if any of its streams match.
    let mut seen_windows = std::collections::HashSet::new();
    let mut sources = Vec::new();

    for ws in schemas {
        if seen_windows.contains(&ws.name) {
            continue;
        }
        let matching_streams: Vec<String> = ws
            .streams
            .iter()
            .filter(|s| interested_streams.contains(s.as_str()))
            .cloned()
            .collect();
        if matching_streams.is_empty() {
            continue;
        }
        if let Some(window) = registry.get_window(&ws.name)
            && let Some(notify) = registry.get_notifier(&ws.name)
        {
            sources.push(WindowSource {
                window_name: ws.name.clone(),
                window: Arc::clone(window),
                notify: Arc::clone(notify),
                stream_names: matching_streams,
            });
            seen_windows.insert(ws.name.clone());
        }
    }

    sources
}

/// Bind the receiver and spawn its task.
/// Returns (listen_addr, task_group).
pub(super) async fn spawn_receiver_task(
    config: &FusionConfig,
    router: Arc<Router>,
    cancel: CancellationToken,
) -> RuntimeResult<(SocketAddr, TaskGroup)> {
    let receiver = Receiver::bind(&config.server.listen, router)
        .await
        .owe_sys()?;
    let listen_addr = receiver.local_addr().owe_sys()?;
    let receiver_cancel = receiver.cancel_token();
    tokio::spawn(async move {
        cancel.cancelled().await;
        receiver_cancel.cancel();
    });
    let mut group = TaskGroup::new("receiver");
    group.push(tokio::spawn(async move { receiver.run().await }));
    Ok((listen_addr, group))
}
