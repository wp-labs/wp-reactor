use std::fmt::{self as stdfmt, Write as _};
use std::path::Path;

use anyhow::Result;
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
struct FileFields(DefaultFields);

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
struct DomainFormat {
    timer: SystemTime,
}

impl DomainFormat {
    fn new() -> Self {
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
                if let Some(fields) = ext.get::<FormattedFields<N>>() {
                    if !fields.is_empty() {
                        write!(writer, "{fields}")?;
                    }
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
            // JSON: keep domain as a regular field — consumers query by key
            let stderr_layer = fmt::layer()
                .json()
                .with_target(false)
                .with_writer(std::io::stderr)
                .with_filter(filter);
            let file_layer = fmt::layer()
                .json()
                .fmt_fields(FileFields::default())
                .with_target(false)
                .with_ansi(false)
                .with_writer(non_blocking);
            tracing_subscriber::registry()
                .with(stderr_layer)
                .with(file_layer)
                .init();
        } else {
            // Plain: domain as [domain] prefix via DomainFormat
            let stderr_layer = fmt::layer()
                .event_format(DomainFormat::new())
                .with_writer(std::io::stderr)
                .with_filter(filter);
            let file_layer = fmt::layer()
                .event_format(DomainFormat::new())
                .fmt_fields(FileFields::default())
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
