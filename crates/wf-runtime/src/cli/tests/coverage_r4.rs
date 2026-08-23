//! CLI 层第四轮补测（注册于 cli/tests/mod.rs）。
//!
//! 覆盖点:
//! - `resolve_config_load_parts` 的 work-dir canonicalize 失败分支
//!   （work_dir 路径不存在 → `?` 早退; 现有测试只覆盖"存在但不是目录"）。
//!
//! 记录的限制（本轮不可测）:
//! - `run_cli_inner`（cli/mod.rs L272-468）的整个命令分发体依赖
//!   `Cli::parse()` 读取进程 argv; Rust 测试无法注入 `std::env::args`，
//!   且 wf-runtime 是纯库 crate（无 bin target），无法用子进程驱动。
//!   因此 Run/Config render/origins/vars/diff 的真实执行路径保持未覆盖。
//! - `run_cli`（L261-270）错误分支同样依赖上述 argv 入口。

use super::super::*;
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

#[test]
fn resolve_config_load_missing_work_dir_canonicalize_errors() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path = write_config(&dir);
    // work_dir 指向不存在的路径 → canonicalize 失败 → Cli 错误（覆盖 `?` 早退）。
    let missing = dir.path().join("does-not-exist");
    let err = match resolve_config_load(ConfigLoadArgs {
        config: config_path,
        overlay: vec![],
        var: vec![],
        work_dir: Some(missing),
    }) {
        Err(e) => e.to_string(),
        Ok(_) => panic!("expected resolve to fail for a missing work-dir"),
    };
    assert!(err.contains("work-dir path"), "got: {err}");
}

#[test]
fn resolve_config_load_missing_overlay_errors() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path = write_config(&dir);
    // overlay 指向不存在的路径 → canonicalize 失败 → 收集到 EngineError。
    let missing = dir.path().join("missing-overlay.toml");
    let err = match resolve_config_load(ConfigLoadArgs {
        config: config_path,
        overlay: vec![missing],
        var: vec![],
        work_dir: None,
    }) {
        Err(e) => e.to_string(),
        Ok(_) => panic!("expected resolve to fail for a missing overlay"),
    };
    assert!(err.contains("overlay path"), "got: {err}");
}
