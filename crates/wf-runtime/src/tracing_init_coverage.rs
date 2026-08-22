//! tracing_init.rs 覆盖测试（注册于 tracing_init.rs）。
//!
//! 覆盖点:
//! - `DomainFormat::format_event`: plain 输出、span 上下文、`task_id` 前缀、
//!   非 domain 事件。（ANSI 分支需要 tracing-subscriber 的 `ansi` feature,
//!   本 workspace 未启用, 无法在层中触发。）
//! - `DomainExtractor` 各 record_* 分支（str/debug/u64/i64/f64/bool）。
//! - `FileFields::format_fields` 层渲染。
//! - `init_tracing` 错误路径: 非法 filter、日志文件路径无文件名。
//!   （成功路径会设置全局 subscriber, 单进程内不可重复调用, 故不测。）

use super::*;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::{EnvFilter, Layer, fmt};

// ---------------------------------------------------------------------------
// 捕获写入器
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct VecWriter(Arc<Mutex<Vec<u8>>>);

impl<'a> fmt::MakeWriter<'a> for VecWriter {
    type Writer = VecWriter;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

impl std::io::Write for VecWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().write(buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.0.lock().unwrap().flush()
    }
}

fn rendered_with<F: FnOnce()>(ansi: bool, emit: F) -> String {
    let sink = Arc::new(Mutex::new(Vec::new()));
    let writer = VecWriter(Arc::clone(&sink));
    let layer = fmt::layer()
        .event_format(DomainFormat::new())
        .with_writer(writer)
        .with_ansi(ansi)
        .with_filter(EnvFilter::try_new("trace").expect("trace filter"));
    let subscriber = tracing_subscriber::registry().with(layer);
    let dispatch = tracing::Dispatch::new(subscriber);
    tracing::dispatcher::with_default(&dispatch, emit);
    String::from_utf8(sink.lock().unwrap().clone()).expect("utf8 output")
}

// ---------------------------------------------------------------------------
// DomainFormat / DomainExtractor
// ---------------------------------------------------------------------------

#[test]
fn domain_format_renders_plain_with_prefixes() {
    let out = rendered_with(false, || {
        tracing::info!(
            domain = "sys",
            task_id = "t-1",
            status = "ok",
            count = 3u64,
            ratio = 0.5f64,
            flag = true,
            signed = -7i64,
            detail = %"escaped",
            "bootstrap complete"
        );
    });

    assert!(out.contains(" INFO "), "got: {out}");
    assert!(out.contains("[sys]"), "got: {out}");
    assert!(out.contains("<t-1>"), "got: {out}");
    assert!(out.contains("bootstrap complete"), "got: {out}");
    // Other fields rendered as key="value".
    assert!(out.contains("status=\"ok\""), "got: {out}");
    assert!(out.contains("count=3"), "got: {out}");
    assert!(out.contains("ratio=0.5"), "got: {out}");
    assert!(out.contains("flag=true"), "got: {out}");
    assert!(out.contains("signed=-7"), "got: {out}");
    // `%value` (Display) renders without Debug quoting.
    assert!(out.contains("detail=escaped"), "got: {out}");
}

#[test]
fn domain_format_renders_without_domain_field() {
    // Events without a `domain` field (e.g. dependency logs) render fine.
    let out = rendered_with(false, || {
        tracing::error!("no domain here");
    });
    assert!(out.contains(" ERROR "), "got: {out}");
    assert!(out.contains("no domain here"), "got: {out}");
    assert!(!out.contains("[sys]"), "got: {out}");
}

#[test]
fn all_levels_render() {
    // Smoke: the level match arm renders every level.
    let out = rendered_with(false, || {
        tracing::trace!(domain = "sys", "t");
        tracing::debug!(domain = "sys", "d");
        tracing::info!(domain = "sys", "i");
        tracing::warn!(domain = "sys", "w");
        tracing::error!(domain = "sys", "e");
    });
    for needle in ["TRACE", "DEBUG", " INFO ", " WARN ", "ERROR"] {
        assert!(out.contains(needle), "missing {needle} in: {out}");
    }
}

#[test]
fn domain_format_renders_span_context() {
    let out = rendered_with(false, || {
        let span = tracing::info_span!("outer", field1 = "v1");
        let _enter = span.enter();
        tracing::info!(domain = "conf", "inside span");
    });
    assert!(out.contains("outer"), "got: {out}");
    // Span fields render through the FormattedFields layer (Debug-quoted).
    assert!(out.contains("field1=\"v1\""), "got: {out}");
    assert!(out.contains("inside span"), "got: {out}");
}

#[test]
fn domain_extractor_debug_field_paths() {
    // `?value` uses record_debug; the message itself uses record_str.
    let out = rendered_with(false, || {
        tracing::info!(
            domain = "conn",
            debugged = ?vec![1, 2],
            "debug field"
        );
    });
    assert!(out.contains("debugged=[1, 2]"), "got: {out}");
    assert!(out.contains("debug field"), "got: {out}");
}

// ---------------------------------------------------------------------------
// FileFields
// ---------------------------------------------------------------------------

#[test]
fn file_fields_layer_renders_events() {
    let sink = Arc::new(Mutex::new(Vec::new()));
    let writer = VecWriter(Arc::clone(&sink));
    let layer = fmt::layer()
        .fmt_fields(FileFields::default())
        .with_ansi(false)
        .with_writer(writer)
        .with_filter(EnvFilter::try_new("trace").expect("filter"));
    let subscriber = tracing_subscriber::registry().with(layer);
    let dispatch = tracing::Dispatch::new(subscriber);
    tracing::dispatcher::with_default(&dispatch, || {
        tracing::info!(domain = "sys", key = "value", "file layer event");
    });
    let out = String::from_utf8(sink.lock().unwrap().clone()).expect("utf8");
    assert!(out.contains("file layer event"), "got: {out}");
    assert!(out.contains("key=\"value\""), "got: {out}");
}

// ---------------------------------------------------------------------------
// init_tracing 错误路径
// ---------------------------------------------------------------------------

fn logging_cfg(level: &str, file: Option<std::path::PathBuf>) -> LoggingConfig {
    LoggingConfig {
        level: level.to_string(),
        modules: HashMap::new(),
        file,
        format: LogFormat::Plain,
    }
}

#[test]
fn init_tracing_invalid_filter_errors() {
    let base = std::path::Path::new("/tmp");
    // A per-module override with an invalid level makes EnvFilter parsing
    // fail → init_tracing errors before any subscriber is installed.
    let mut cfg = logging_cfg("info", None);
    cfg.modules
        .insert("wf_runtime".to_string(), "not-a-level".to_string());
    let err = init_tracing(&cfg, base).expect_err("invalid module level must fail");
    assert!(
        err.to_string().contains("invalid log filter"),
        "got: {err:?}"
    );
}

#[test]
fn init_tracing_file_without_name_errors() {
    let base = std::path::Path::new("/tmp");
    // "/" has no file name → error before any subscriber is installed.
    let err = init_tracing(
        &logging_cfg("info", Some(std::path::PathBuf::from("/"))),
        base,
    )
    .expect_err("path without file name must fail");
    assert!(
        err.to_string().contains("log file path has no file name"),
        "got: {err:?}"
    );
}
