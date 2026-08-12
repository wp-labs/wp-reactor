use std::fmt::{self as stdfmt, Write as _};
use std::path::Path;

use crate::error::{RuntimeReason, RuntimeResult};
use orion_error::conversion::{SourceErr, SourceRawErr};
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::fmt::format::DefaultFields;
use tracing_subscriber::fmt::time::{FormatTime, SystemTime};
use tracing_subscriber::fmt::{self, FmtContext, FormatEvent, FormattedFields};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};
use wf_config::{LogFormat, LoggingConfig};

// ---------------------------------------------------------------------------
// FileFields — newtype to isolate span field caching between layers
// ---------------------------------------------------------------------------

#[derive(Default)]
pub struct FileFields(DefaultFields);

impl<'writer> fmt::FormatFields<'writer> for FileFields {
    fn format_fields<R: tracing_subscriber::field::RecordFields>(
        &self,
        writer: fmt::format::Writer<'writer>,
        fields: R,
    ) -> stdfmt::Result {
        self.0.format_fields(writer, fields)
    }
}

// ---------------------------------------------------------------------------
// DomainFormat — promotes `domain` field to a `[domain]` prefix
// ---------------------------------------------------------------------------

/// Custom event formatter that renders the `domain` field as a prominent
/// `[domain]` prefix instead of burying it among key=value pairs.
///
/// Plain-text output:
/// ```text
/// 2026-02-21T01:17:14Z  INFO [sys] engine bootstrap complete schemas=1
/// ```
///
/// Events without a `domain` field (e.g. from dependencies) are rendered
/// without the prefix.  ANSI colouring adapts automatically based on the
/// writer.
pub struct DomainFormat {
    timer: SystemTime,
}

impl Default for DomainFormat {
    fn default() -> Self {
        Self::new()
    }
}

impl DomainFormat {
    pub fn new() -> Self {
        Self { timer: SystemTime }
    }
}

impl<S, N> FormatEvent<S, N> for DomainFormat
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'writer> fmt::FormatFields<'writer> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: fmt::format::Writer<'_>,
        event: &Event<'_>,
    ) -> stdfmt::Result {
        let ansi = writer.has_ansi_escapes();

        // 1. Timestamp
        if ansi {
            write!(writer, "\x1b[2m")?;
        }
        if self.timer.format_time(&mut writer).is_err() {
            write!(writer, "<unknown time>")?;
        }
        if ansi {
            write!(writer, "\x1b[0m")?;
        }

        // 2. Level
        let level = *event.metadata().level();
        if ansi {
            let color = match level {
                Level::ERROR => "31",
                Level::WARN => "33",
                Level::INFO => "32",
                Level::DEBUG => "34",
                Level::TRACE => "35",
            };
            write!(writer, " \x1b[{color}m{level:>5}\x1b[0m ")?;
        } else {
            write!(writer, " {level:>5} ")?;
        }

        // 3. Extract domain, message, and remaining fields from the event
        let mut visitor = DomainExtractor::default();
        event.record(&mut visitor);

        // 4. [domain] prefix
        if let Some(ref domain) = visitor.domain {
            if ansi {
                write!(writer, "\x1b[1;36m[{domain}]\x1b[0m ")?;
            } else {
                write!(writer, "[{domain}] ")?;
            }
        }

        // 4b. <task_id> prefix
        if let Some(ref task_id) = visitor.task_id {
            if ansi {
                write!(writer, "\x1b[1m<{task_id}>\x1b[0m ")?;
            } else {
                write!(writer, "<{task_id}> ")?;
            }
        }

        // 5. Span context
        if let Some(scope) = ctx.event_scope() {
            for span in scope.from_root() {
                let name = span.name();
                if ansi {
                    write!(writer, "\x1b[1m{name}\x1b[0m")?;
                } else {
                    write!(writer, "{name}")?;
                }
                write!(writer, "{{")?;
                let ext = span.extensions();
                if let Some(fields) = ext.get::<FormattedFields<N>>()
                    && !fields.is_empty()
                {
                    write!(writer, "{fields}")?;
                }
                write!(writer, "}}: ")?;
            }
        }

        // 6. Message
        write!(writer, "{}", visitor.message)?;

        // 7. Remaining fields
        if !visitor.other_fields.is_empty() {
            if ansi {
                write!(writer, " \x1b[3m{}\x1b[0m", visitor.other_fields)?;
            } else {
                write!(writer, " {}", visitor.other_fields)?;
            }
        }

        writeln!(writer)
    }
}

// ---------------------------------------------------------------------------
// DomainExtractor — visitor that separates domain/message from other fields
// ---------------------------------------------------------------------------

#[derive(Default)]
struct DomainExtractor {
    domain: Option<String>,
    task_id: Option<String>,
    message: String,
    other_fields: String,
}

impl DomainExtractor {
    fn push_separator(&mut self) {
        if !self.other_fields.is_empty() {
            self.other_fields.push(' ');
        }
    }
}

impl Visit for DomainExtractor {
    fn record_str(&mut self, field: &Field, value: &str) {
        match field.name() {
            "domain" => self.domain = Some(value.to_string()),
            "task_id" => self.task_id = Some(value.to_string()),
            "message" => self.message = value.to_string(),
            name => {
                self.push_separator();
                write!(&mut self.other_fields, "{name}={value:?}").ok();
            }
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn stdfmt::Debug) {
        match field.name() {
            "domain" => {
                let s = format!("{value:?}");
                self.domain = Some(s.trim_matches('"').to_string());
            }
            "task_id" => {
                let s = format!("{value:?}");
                self.task_id = Some(s.trim_matches('"').to_string());
            }
            "message" => {
                write!(&mut self.message, "{value:?}").ok();
            }
            name => {
                self.push_separator();
                write!(&mut self.other_fields, "{name}={value:?}").ok();
            }
        }
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.push_separator();
        write!(&mut self.other_fields, "{}={value}", field.name()).ok();
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.push_separator();
        write!(&mut self.other_fields, "{}={value}", field.name()).ok();
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        self.push_separator();
        write!(&mut self.other_fields, "{}={value}", field.name()).ok();
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.push_separator();
        write!(&mut self.other_fields, "{}={value}", field.name()).ok();
    }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Initialise the `tracing` subscriber stack from [`LoggingConfig`].
///
/// Returns an optional [`WorkerGuard`] that **must** be held until the process
/// exits — dropping it flushes and closes the non-blocking file writer.
///
/// The `[logging]` config is the single source of truth for log level:
/// `level` is the global floor and `modules` are per-target overrides.
/// `RUST_LOG` is intentionally **not** consulted — a leaked/stale env var
/// must not silently override the operator's configured level. Operators who
/// need finer control should use `[logging].modules` (e.g.
/// `wf_runtime::receiver = "debug"`).
///
/// The `log` → `tracing` bridge is set up automatically by
/// `tracing-subscriber`'s default `tracing-log` feature.
pub fn init_tracing(config: &LoggingConfig, base_dir: &Path) -> RuntimeResult<Option<WorkerGuard>> {
    // 1. Build EnvFilter from config (level + per-module overrides) --------
    let directives = filter_directives(config);
    let filter = EnvFilter::try_new(&directives).source_raw_err(
        RuntimeReason::Bootstrap,
        format!("invalid log filter '{directives}'"),
    )?;

    // 2. stderr + optional file layer -----------------------------------
    let mut guard: Option<WorkerGuard> = None;
    let is_json = config.format == LogFormat::Json;

    if let Some(ref file_path) = config.file {
        let resolved = if file_path.is_relative() {
            base_dir.join(file_path)
        } else {
            file_path.clone()
        };
        if let Some(parent) = resolved.parent() {
            std::fs::create_dir_all(parent).source_err(
                RuntimeReason::Bootstrap,
                format!("create log directory {}", parent.display()),
            )?;
        }
        let Some(file_name) = resolved.file_name() else {
            return RuntimeReason::Bootstrap.fail("log file path has no file name");
        };
        let file_name = file_name.to_os_string();
        let Some(dir) = resolved.parent() else {
            return RuntimeReason::Bootstrap.fail("log file path has no parent directory");
        };

        let file_appender = tracing_appender::rolling::never(dir, file_name);
        let (non_blocking, file_guard) = tracing_appender::non_blocking(file_appender);
        guard = Some(file_guard);

        if is_json {
            // JSON: keep domain as a regular field — consumers query by key
            let stderr_layer = fmt::layer()
                .json()
                .with_target(false)
                .with_writer(std::io::stderr)
                .with_filter(filter.clone());
            let file_layer = fmt::layer()
                .json()
                .fmt_fields(FileFields::default())
                .with_target(false)
                .with_ansi(false)
                .with_writer(non_blocking)
                .with_filter(filter);
            tracing_subscriber::registry()
                .with(stderr_layer)
                .with(file_layer)
                .init();
        } else {
            // Plain: domain as [domain] prefix via DomainFormat
            let stderr_layer = fmt::layer()
                .event_format(DomainFormat::new())
                .with_writer(std::io::stderr)
                .with_filter(filter.clone());
            let file_layer = fmt::layer()
                .event_format(DomainFormat::new())
                .fmt_fields(FileFields::default())
                .with_ansi(false)
                .with_writer(non_blocking)
                .with_filter(filter);
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
                        .with_target(false)
                        .with_writer(std::io::stderr)
                        .with_filter(filter),
                )
                .init();
        } else {
            tracing_subscriber::registry()
                .with(
                    fmt::layer()
                        .event_format(DomainFormat::new())
                        .with_writer(std::io::stderr)
                        .with_filter(filter),
                )
                .init();
        }
    }

    Ok(guard)
}

/// Build the `EnvFilter` directive string from [`LoggingConfig`].
///
/// `<global level>[,<module>=<level>...]` — the global `level` first, then
/// one `module=level` clause per `[logging].modules` entry. Pure so it can be
/// unit-tested without touching global subscriber state.
fn filter_directives(config: &LoggingConfig) -> String {
    let mut directives = config.level.clone();
    for (module, level) in &config.modules {
        directives.push(',');
        directives.push_str(module);
        directives.push('=');
        directives.push_str(level);
    }
    directives
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn cfg(level: &str, modules: &[(&str, &str)]) -> LoggingConfig {
        LoggingConfig {
            level: level.to_string(),
            modules: modules
                .iter()
                .map(|(m, l)| (m.to_string(), l.to_string()))
                .collect::<HashMap<_, _>>(),
            file: None,
            format: LogFormat::Plain,
        }
    }

    #[test]
    fn directives_global_level_only() {
        assert_eq!(filter_directives(&cfg("info", &[])), "info");
    }

    #[test]
    fn directives_append_module_overrides() {
        // HashMap order is unspecified, so check both possible orderings.
        let d = filter_directives(&cfg("info", &[("wf_runtime::receiver", "debug")]));
        assert_eq!(d, "info,wf_runtime::receiver=debug");
    }

    #[test]
    fn directives_multiple_modules_all_present() {
        let d = filter_directives(&cfg("warn", &[("a", "debug"), ("b", "trace")]));
        assert!(d.starts_with("warn,"));
        assert!(d.contains("a=debug"));
        assert!(d.contains("b=trace"));
    }

    /// Config is the authority: `init_tracing` must never read `RUST_LOG`.
    /// The helper is purely a function of config, so the directives are
    /// independent of the process environment.
    #[test]
    fn directives_ignore_rust_log_env() {
        let d = filter_directives(&cfg("info", &[]));
        assert_eq!(d, "info");
    }
}
