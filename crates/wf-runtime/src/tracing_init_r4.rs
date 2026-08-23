//! tracing_init.rs 第四轮补测（注册于 tracing_init.rs 内, `#[path]` 方式）。
//!
//! 覆盖点（`tracing_init_coverage` 之外）:
//! - `DomainFormat::default()`（Default 实现）。
//! - span 上下文携带非空 `FormattedFields` 时的字段渲染（`if let ... && !empty`
//!   成功分支的闭合）。
//! - `DomainExtractor::record_debug` 的 `domain` 分支（`?` 形式传入 domain）。
//!
//! 记录的限制（本轮不可测）:
//! - `init_tracing` 的成功路径（相对文件路径 join / create_dir_all / 各层
//!   subscriber 安装 / `Ok(guard)`）会调用 `tracing_subscriber::registry().init()`
//!   安装**全局** subscriber; 测试二进制里 engine_task::tests::init_tracing()
//!   （`try_init`）已抢先安装全局 dispatch, 再次 `.init()` 必然 panic, 因此
//!   文件层 / JSON / plain 成功路径无法在单进程测试内触发。
//! - `format_time` 失败分支（`<unknown time>`）与 ANSI 颜色分支需要
//!   tracing-subscriber 的 `ansi` feature（workspace 未启用）。

use super::*;

use std::sync::{Arc, Mutex};

use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::{EnvFilter, Layer, fmt};

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

fn rendered_with<F: FnOnce()>(emit: F) -> String {
    let sink = Arc::new(Mutex::new(Vec::new()));
    let writer = VecWriter(Arc::clone(&sink));
    let layer = fmt::layer()
        .event_format(DomainFormat::default())
        .with_writer(writer)
        .with_ansi(false)
        .with_filter(EnvFilter::try_new("trace").expect("trace filter"));
    let subscriber = tracing_subscriber::registry().with(layer);
    let dispatch = tracing::Dispatch::new(subscriber);
    tracing::dispatcher::with_default(&dispatch, emit);
    String::from_utf8(sink.lock().unwrap().clone()).expect("utf8 output")
}

#[test]
fn domain_format_default_constructs() {
    // 覆盖 `Default for DomainFormat`（内部调用 `new()`）。
    let format = DomainFormat::default();
    let out = rendered_with(|| {
        // format 本身不直接渲染, 这里仅确认 Default 构造可用。
        let _ = &format;
        tracing::info!(domain = "sys", "default ctor ok");
    });
    assert!(out.contains("default ctor ok"), "got: {out}");
}

#[test]
fn domain_format_renders_span_with_fields() {
    // span 带字段 → FormattedFields 非空 → 字段渲染分支。
    let out = rendered_with(|| {
        let span = tracing::info_span!("outer", field1 = "v1", count = 3u64);
        let _enter = span.enter();
        tracing::info!(domain = "conf", "inside span with fields");
    });
    assert!(out.contains("outer"), "got: {out}");
    assert!(out.contains("field1=\"v1\""), "got: {out}");
    assert!(out.contains("count=3"), "got: {out}");
    assert!(out.contains("inside span with fields"), "got: {out}");
}

#[test]
fn domain_format_renders_span_without_fields() {
    // span 无字段 → `if let Some(fields) ... && !fields.is_empty()` 假分支。
    let out = rendered_with(|| {
        let span = tracing::info_span!("bare");
        let _enter = span.enter();
        tracing::info!(domain = "conf", "inside bare span");
    });
    assert!(out.contains("bare"), "got: {out}");
    assert!(out.contains("inside bare span"), "got: {out}");
}

#[test]
fn domain_extractor_record_debug_domain_branch() {
    // `domain = ?value` → record_debug 的 domain 分支（trim 引号）。
    let out = rendered_with(|| {
        tracing::info!(domain = ?"conn", task_id = ?"t-9", "debug domain");
    });
    assert!(out.contains("[conn]"), "got: {out}");
    assert!(out.contains("<t-9>"), "got: {out}");
    assert!(out.contains("debug domain"), "got: {out}");
}
