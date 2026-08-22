//! CLI 层第二轮补测（cli/mod.rs 可注入部分）。
//!
//! 覆盖点（第一轮 `coverage_extra` 之外）:
//! - `resolve_config_load` 包装函数: 成功 / 缺失配置文件 / 非法 --var /
//!   work-dir 非目录错误路径。
//! - `render_runtime_error`: RuntimeError → EngineError 转换。

use super::super::*;
use crate::error::{RuntimeReason, RuntimeResult};
use orion_error::conversion::ToStructError;
use tempfile::TempDir;

fn write_config(dir: &TempDir) -> std::path::PathBuf {
    let path = dir.path().join("wfusion.toml");
    std::fs::write(
        &path,
        r#"
        sinks = "sinks"
        [runtime]
        rule_exec_timeout = "30s"
        schemas = "models/schemas/*.wfs"
        rules = "models/rules/*.wfl"
        "#,
    )
    .expect("write wfusion.toml");
    path
}

fn load_args(config: std::path::PathBuf, work_dir: Option<std::path::PathBuf>) -> ConfigLoadArgs {
    ConfigLoadArgs {
        config,
        overlay: vec![],
        var: vec![],
        work_dir,
    }
}

fn resolve_err(args: ConfigLoadArgs) -> String {
    match resolve_config_load(args) {
        Err(e) => e.to_string(),
        Ok(_) => panic!("expected resolve_config_load to fail"),
    }
}

// ---------------------------------------------------------------------------
// resolve_config_load（run_cli_inner 的可注入组成部分）
// ---------------------------------------------------------------------------

#[test]
fn resolve_config_load_success() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path = write_config(&dir);
    let resolved =
        resolve_config_load(load_args(config_path.clone(), None)).expect("resolve succeeds");
    assert_eq!(resolved.config_path, config_path.canonicalize().unwrap());
    assert_eq!(
        resolved.runtime_base_dir,
        config_path.parent().unwrap().canonicalize().unwrap()
    );
    // work_dir 覆盖 base dir。
    let work = dir.path().join("work");
    std::fs::create_dir_all(&work).expect("work dir");
    let resolved = resolve_config_load(load_args(config_path, Some(work.clone())))
        .expect("resolve with work dir");
    assert_eq!(resolved.runtime_base_dir, work.canonicalize().unwrap());
}

#[test]
fn resolve_config_load_missing_config_errors() {
    let _dir = tempfile::tempdir().expect("tempdir");
    let err = resolve_err(load_args(
        std::path::PathBuf::from("/definitely/not/here.toml"),
        None,
    ));
    assert!(err.contains("config path"), "got: {err}");
}

#[test]
fn resolve_config_load_invalid_var_errors() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path = write_config(&dir);
    let err = resolve_err(ConfigLoadArgs {
        config: config_path,
        overlay: vec![],
        var: vec!["NO_EQUALS".to_string()],
        work_dir: None,
    });
    assert!(err.contains("invalid --var"), "got: {err}");
}

#[test]
fn resolve_config_load_work_dir_not_directory_errors() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path = write_config(&dir);
    let file = dir.path().join("plain.txt");
    std::fs::write(&file, "x").expect("write file");
    let err = resolve_err(load_args(config_path, Some(file)));
    assert!(err.contains("is not a directory"), "got: {err}");
}

#[test]
fn resolve_config_load_with_working_overlay() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path = write_config(&dir);
    let overlay = dir.path().join("overlay.toml");
    std::fs::write(&overlay, "").expect("write overlay");
    let resolved = resolve_config_load(ConfigLoadArgs {
        config: config_path,
        overlay: vec![overlay.clone()],
        var: vec![],
        work_dir: None,
    })
    .expect("resolve with overlay");
    assert_eq!(
        resolved.overlay_paths,
        vec![overlay.canonicalize().unwrap()]
    );
}

// ---------------------------------------------------------------------------
// render_runtime_error
// ---------------------------------------------------------------------------

#[test]
fn render_runtime_error_maps_to_engine_error() {
    let runtime_err: RuntimeResult<()> = RuntimeReason::Bootstrap
        .to_err()
        .with_detail("boom during bootstrap")
        .err();
    let engine = render_runtime_error(runtime_err.expect_err("constructed as err"));
    assert!(
        matches!(engine.reason(), EngineReason::Runtime(_)),
        "runtime reason must be preserved"
    );
    assert!(
        engine
            .detail()
            .as_deref()
            .is_some_and(|d| d.contains("bootstrap")),
        "detail preserved: {:?}",
        engine.detail()
    );
}
