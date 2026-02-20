use std::path::Path;

use anyhow::Result;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::fmt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};
use wf_config::{LogFormat, LoggingConfig};

/// Initialise the `tracing` subscriber stack from [`LoggingConfig`].
///
/// Returns an optional [`WorkerGuard`] that **must** be held until the process
/// exits — dropping it flushes and closes the non-blocking file writer.
///
/// Precedence: `RUST_LOG` env-var overrides all config-driven directives.
///
/// The `log` → `tracing` bridge is set up automatically by
/// `tracing-subscriber`'s default `tracing-log` feature.
pub fn init_tracing(
    config: &LoggingConfig,
    base_dir: &Path,
) -> Result<Option<WorkerGuard>> {
    // 1. Build EnvFilter ------------------------------------------------
    let filter = if std::env::var("RUST_LOG").is_ok() {
        EnvFilter::from_default_env()
    } else {
        let mut directives = config.level.clone();
        for (module, level) in &config.modules {
            directives.push(',');
            directives.push_str(module);
            directives.push('=');
            directives.push_str(level);
        }
        EnvFilter::try_new(&directives)
            .map_err(|e| anyhow::anyhow!("invalid log filter '{directives}': {e}"))?
    };

    // 2. stderr layer ---------------------------------------------------
    let is_json = config.format == LogFormat::Json;

    // 3. Optional file layer --------------------------------------------
    let mut guard: Option<WorkerGuard> = None;

    if let Some(ref file_path) = config.file {
        let resolved = if file_path.is_relative() {
            base_dir.join(file_path)
        } else {
            file_path.clone()
        };
        if let Some(parent) = resolved.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file_name = resolved
            .file_name()
            .ok_or_else(|| anyhow::anyhow!("log file path has no file name"))?
            .to_os_string();
        let dir = resolved
            .parent()
            .ok_or_else(|| anyhow::anyhow!("log file path has no parent directory"))?;

        let file_appender = tracing_appender::rolling::never(dir, file_name);
        let (non_blocking, file_guard) = tracing_appender::non_blocking(file_appender);
        guard = Some(file_guard);

        if is_json {
            let stderr_layer = fmt::layer()
                .json()
                .with_target(true)
                .with_writer(std::io::stderr)
                .with_filter(filter);
            let file_layer = fmt::layer()
                .json()
                .with_target(true)
                .with_ansi(false)
                .with_writer(non_blocking);
            tracing_subscriber::registry()
                .with(stderr_layer)
                .with(file_layer)
                .init();
        } else {
            let stderr_layer = fmt::layer()
                .with_target(true)
                .with_writer(std::io::stderr)
                .with_filter(filter);
            let file_layer = fmt::layer()
                .with_target(true)
                .with_ansi(false)
                .with_writer(non_blocking);
            tracing_subscriber::registry()
                .with(stderr_layer)
                .with(file_layer)
                .init();
        }
    } else {
        // stderr only
        if is_json {
            tracing_subscriber::registry()
                .with(
                    fmt::layer()
                        .json()
                        .with_target(true)
                        .with_writer(std::io::stderr)
                        .with_filter(filter),
                )
                .init();
        } else {
            tracing_subscriber::registry()
                .with(
                    fmt::layer()
                        .with_target(true)
                        .with_writer(std::io::stderr)
                        .with_filter(filter),
                )
                .init();
        }
    }

    Ok(guard)
}
