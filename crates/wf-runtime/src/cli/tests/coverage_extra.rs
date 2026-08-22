//! CLI 层覆盖测试（cli/mod.rs 此前 0%）。
//!
//! 覆盖点:
//! - clap 派生的参数解析: 所有子命令（run / config render / origins / vars /
//!   diff）及错误/帮助/版本输出。
//! - 纯函数: `matches_any_prefix` / `path_matches_prefix` /
//!   `matches_any_var_prefix` / `format_value`。
//! - 配置解析: `resolve_config_load_parts` 的成功与错误路径（缺失文件、
//!   work-dir 非目录、非法 --var）; `resolve_compare_config_load` 的 base 回退。

use super::super::*;
use clap::{CommandFactory, Parser};
use tempfile::TempDir;

/// Parse argv as if `wfusion` was invoked.
fn parse(args: &[&str]) -> Result<Cli, clap::Error> {
    let mut argv = vec!["wfusion"];
    argv.extend_from_slice(args);
    Cli::try_parse_from(argv)
}

fn parse_ok(args: &[&str]) -> Cli {
    match parse(args) {
        Ok(cli) => cli,
        Err(e) => panic!("expected parse to succeed: {e}"),
    }
}

fn parse_err(args: &[&str]) -> String {
    match parse(args) {
        Err(e) => e.to_string(),
        Ok(_) => panic!("expected parse to fail"),
    }
}

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

// ---------------------------------------------------------------------------
// 子命令解析
// ---------------------------------------------------------------------------

#[test]
fn run_subcommand_defaults() {
    match parse_ok(&["run"]).command {
        Commands::Run {
            load,
            metrics,
            metrics_interval,
            metrics_listen,
        } => {
            assert_eq!(load.config, std::path::PathBuf::from("conf/wfusion.toml"));
            assert!(load.overlay.is_empty());
            assert!(load.var.is_empty());
            assert!(load.work_dir.is_none());
            assert!(!metrics);
            assert!(metrics_interval.is_none());
            assert!(metrics_listen.is_none());
        }
        _ => panic!("expected run subcommand"),
    }
}

#[test]
fn run_subcommand_with_all_flags() {
    match parse_ok(&[
        "run",
        "--config",
        "some/wfusion.toml",
        "--overlay",
        "a.toml",
        "--overlay",
        "b.toml",
        "--var",
        "THRESHOLD=5",
        "--var",
        "K=1",
        "--work-dir",
        "work",
        "--metrics",
        "--metrics-interval",
        "2s",
        "--metrics-listen",
        "127.0.0.1:9999",
    ])
    .command
    {
        Commands::Run {
            load,
            metrics,
            metrics_interval,
            metrics_listen,
        } => {
            assert_eq!(load.config, std::path::PathBuf::from("some/wfusion.toml"));
            assert_eq!(load.overlay.len(), 2);
            assert_eq!(load.var.len(), 2);
            assert_eq!(load.work_dir, Some(std::path::PathBuf::from("work")));
            assert!(metrics);
            assert_eq!(metrics_interval.as_deref(), Some("2s"));
            assert_eq!(metrics_listen.as_deref(), Some("127.0.0.1:9999"));
        }
        _ => panic!("expected run subcommand"),
    }
}

#[test]
fn config_render_flags() {
    match parse_ok(&["config", "render", "--raw", "--config", "x.toml"]).command {
        Commands::Config {
            command: ConfigCommands::Render { load, raw },
        } => {
            assert!(raw);
            assert_eq!(load.config, std::path::PathBuf::from("x.toml"));
        }
        _ => panic!("expected config render"),
    }

    // Default (no --raw) → expanded rendering path.
    match parse_ok(&["config", "render"]).command {
        Commands::Config {
            command: ConfigCommands::Render { raw, .. },
        } => assert!(!raw),
        _ => panic!("expected config render"),
    }
}

#[test]
fn config_origins_flags() {
    match parse_ok(&[
        "config",
        "origins",
        "--path-prefix",
        "runtime",
        "--path-prefix",
        "sources",
    ])
    .command
    {
        Commands::Config {
            command: ConfigCommands::Origins { load, filter },
        } => {
            assert_eq!(
                filter.path_prefix,
                vec!["runtime".to_string(), "sources".to_string()]
            );
            assert_eq!(load.config, std::path::PathBuf::from("conf/wfusion.toml"));
        }
        _ => panic!("expected config origins"),
    }
}

#[test]
fn config_vars_flags() {
    match parse_ok(&["config", "vars", "--var-prefix", "WORK"]).command {
        Commands::Config {
            command: ConfigCommands::Vars { load, filter },
        } => {
            assert_eq!(filter.var_prefix, vec!["WORK".to_string()]);
            assert_eq!(load.config, std::path::PathBuf::from("conf/wfusion.toml"));
        }
        _ => panic!("expected config vars"),
    }
}

#[test]
fn config_diff_flags() {
    match parse_ok(&[
        "config",
        "diff",
        "--config",
        "base.toml",
        "--to-config",
        "other.toml",
        "--to-overlay",
        "o1.toml",
        "--to-var",
        "K=2",
        "--to-work-dir",
        "wd",
        "--path-prefix",
        "runtime",
        "--expanded",
    ])
    .command
    {
        Commands::Config {
            command: ConfigCommands::Diff {
                load,
                compare,
                filter,
                expanded,
            },
        } => {
            assert_eq!(load.config, std::path::PathBuf::from("base.toml"));
            assert_eq!(
                compare.to_config,
                Some(std::path::PathBuf::from("other.toml"))
            );
            assert_eq!(compare.to_overlay.len(), 1);
            assert_eq!(compare.to_var.len(), 1);
            assert_eq!(compare.to_work_dir, Some(std::path::PathBuf::from("wd")));
            assert_eq!(filter.path_prefix, vec!["runtime".to_string()]);
            assert!(expanded);
        }
        _ => panic!("expected config diff"),
    }
}

// ---------------------------------------------------------------------------
// 错误 / 帮助 / 版本输出
// ---------------------------------------------------------------------------

#[test]
fn unknown_subcommand_rejected() {
    let text = parse_err(&["bogus"]);
    assert!(text.contains("unrecognized subcommand"), "got: {text}");
}

#[test]
fn missing_subcommand_rejected() {
    let text = parse_err(&[]);
    assert!(
        text.contains("subcommand") || text.contains("required"),
        "got: {text}"
    );
}

#[test]
fn help_output_lists_subcommands() {
    let help = Cli::command().render_help().to_string();
    assert!(help.contains("wfusion"), "got: {help}");
    assert!(help.contains("Start the WarpFusion engine"), "got: {help}");
    assert!(help.contains("Render merged configuration"), "got: {help}");
}

#[test]
fn version_output_contains_name() {
    let version = Cli::command().render_version().to_string();
    assert!(version.contains("wfusion"), "got: {version}");
}

// ---------------------------------------------------------------------------
// 纯辅助函数
// ---------------------------------------------------------------------------

#[test]
fn path_prefix_matching() {
    assert!(matches_any_prefix("runtime", &[]));
    assert!(matches_any_prefix("runtime.x", &["runtime".into()]));
    assert!(matches_any_prefix("runtime[0]", &["runtime".into()]));
    assert!(matches_any_prefix("runtime", &["runtime".into()]));
    assert!(!matches_any_prefix("runtime_x", &["runtime".into()]));
    assert!(!matches_any_prefix("other", &["runtime".into()]));

    assert!(path_matches_prefix("runtime", "runtime"));
    assert!(path_matches_prefix("runtime.exec", "runtime"));
    assert!(path_matches_prefix("runtime[0].a", "runtime"));
    assert!(!path_matches_prefix("runtimex", "runtime"));
    assert!(!path_matches_prefix("runtim", "runtime"));
}

#[test]
fn var_prefix_matching() {
    assert!(matches_any_var_prefix("ANY_KEY", &[]));
    assert!(matches_any_var_prefix("WORK_DIR", &["WORK".into()]));
    assert!(!matches_any_var_prefix("OTHER", &["WORK".into()]));
    // Multiple prefixes: any match wins.
    assert!(matches_any_var_prefix(
        "FAIL_THRESHOLD",
        &["WORK".into(), "FAIL".into()]
    ));
}

#[test]
fn format_value_displays() {
    assert_eq!(format_value(&42u64), "42");
    assert_eq!(format_value(&"hello"), "hello");
}

// ---------------------------------------------------------------------------
// 配置解析（resolve_config_load_parts）
// ---------------------------------------------------------------------------

fn resolve_ok(
    config: std::path::PathBuf,
    overlay: Vec<std::path::PathBuf>,
    var: Vec<String>,
    work_dir: Option<std::path::PathBuf>,
) -> ResolvedConfigLoad {
    match resolve_config_load_parts(config, overlay, var, work_dir) {
        Ok(resolved) => resolved,
        Err(e) => panic!("expected resolve to succeed: {e:?}"),
    }
}

fn resolve_err(
    config: std::path::PathBuf,
    overlay: Vec<std::path::PathBuf>,
    var: Vec<String>,
    work_dir: Option<std::path::PathBuf>,
) -> String {
    match resolve_config_load_parts(config, overlay, var, work_dir) {
        Err(e) => e.to_string(),
        Ok(_) => panic!("expected resolve to fail"),
    }
}

#[test]
fn resolve_config_load_parts_success() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path = write_config(&dir);
    let work_dir = dir.path().join("work");
    std::fs::create_dir_all(&work_dir).expect("create work dir");

    let resolved = resolve_ok(config_path.clone(), vec![], vec!["THRESHOLD=5".to_string()], Some(work_dir.clone()));

    assert_eq!(resolved.config_path, config_path.canonicalize().unwrap());
    assert!(resolved.overlay_paths.is_empty());
    assert_eq!(resolved.runtime_base_dir, work_dir.canonicalize().unwrap());
}

#[test]
fn resolve_config_load_parts_defaults_base_dir_to_config_parent() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path = write_config(&dir);

    let resolved = resolve_ok(config_path.clone(), vec![], vec![], None);

    assert_eq!(
        resolved.runtime_base_dir,
        config_path.parent().unwrap().canonicalize().unwrap()
    );
}

#[test]
fn resolve_config_load_parts_missing_config_errors() {
    let err = resolve_err(
        std::path::PathBuf::from("/definitely/not/here.toml"),
        vec![],
        vec![],
        None,
    );
    assert!(err.contains("config path"), "got: {err}");
}

#[test]
fn resolve_config_load_parts_overlay_missing_errors() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path = write_config(&dir);
    let err = resolve_err(
        config_path,
        vec![std::path::PathBuf::from("/nope/overlay.toml")],
        vec![],
        None,
    );
    assert!(err.contains("overlay path"), "got: {err}");
}

#[test]
fn resolve_config_load_parts_work_dir_not_directory_errors() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path = write_config(&dir);
    let file = dir.path().join("plain.txt");
    std::fs::write(&file, "x").expect("write file");

    let err = resolve_err(config_path, vec![], vec![], Some(file));
    assert!(err.contains("is not a directory"), "got: {err}");
}

#[test]
fn resolve_config_load_parts_invalid_var_errors() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path = write_config(&dir);
    let err = resolve_err(config_path, vec![], vec!["NO_EQUALS".to_string()], None);
    assert!(err.contains("invalid --var"), "got: {err}");
}

#[test]
fn resolve_compare_config_load_falls_back_to_base() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path = write_config(&dir);
    let base = resolve_ok(config_path.clone(), vec![], vec![], None);

    // No --to-config → the base config path is reused.
    let compare = match resolve_compare_config_load(&base, CompareConfigLoadArgs::default()) {
        Ok(compare) => compare,
        Err(e) => panic!("compare resolve failed: {e:?}"),
    };
    assert_eq!(compare.config_path, base.config_path);

    // Explicit --to-config wins.
    let other = dir.path().join("other.toml");
    std::fs::write(&other, "").expect("write other.toml");
    let compare = match resolve_compare_config_load(
        &base,
        CompareConfigLoadArgs {
            to_config: Some(other.clone()),
            ..CompareConfigLoadArgs::default()
        },
    ) {
        Ok(compare) => compare,
        Err(e) => panic!("compare resolve failed: {e:?}"),
    };
    assert_eq!(compare.config_path, other.canonicalize().unwrap());
}
