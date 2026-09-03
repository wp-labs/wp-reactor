use std::path::PathBuf;
use std::str::FromStr;

use clap::{Args, Parser, Subcommand};
use orion_error::conversion::{ConvErr, ConvStructError, SourceErr};
use orion_error::report::DiagnosticReport;

pub mod error;

#[cfg(test)]
mod tests;

use crate::error::RuntimeError;
use crate::lifecycle::Reactor;
use crate::tracing_init::init_tracing;
use error::{EngineReason, EngineResult};
use wf_config::ConfigVarContext;
use wf_config::{FusionConfigLoader, HumanDuration, parse_vars};

#[derive(Parser, ::moju_derive::MoJu)]
#[command(
    name = "wfusion",
    version,
    about = "WarpFusion CEP engine",
    propagate_version = true
)]
#[moju(kind = "struct", domain = "Runtime", module = "Runtime.CliEntry")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(::moju_derive::MoJu, Args, Clone)]
#[moju(kind = "struct", domain = "Runtime", module = "Runtime.CliEntry")]
struct ConfigLoadArgs {
    /// Path to wfusion.toml config file (default: conf/wfusion.toml)
    #[arg(short, long, default_value = "conf/wfusion.toml")]
    config: PathBuf,
    /// Apply one or more overlay config files after the base config
    #[arg(long)]
    overlay: Vec<PathBuf>,
    /// Override configuration variables, can be repeated: --var KEY=VALUE
    #[arg(long)]
    var: Vec<String>,
    /// Override the base directory used to resolve relative runtime paths
    #[arg(long)]
    work_dir: Option<PathBuf>,
}

#[derive(::moju_derive::MoJu, Args, Clone, Default)]
#[moju(kind = "struct", domain = "Runtime", module = "Runtime.CliEntry")]
struct CompareConfigLoadArgs {
    /// Compare against another config file; defaults to the primary --config
    #[arg(long = "to-config")]
    to_config: Option<PathBuf>,
    /// Apply one or more overlay files to the comparison side
    #[arg(long = "to-overlay")]
    to_overlay: Vec<PathBuf>,
    /// Override configuration variables on the comparison side
    #[arg(long = "to-var")]
    to_var: Vec<String>,
    /// Override the base directory used to resolve relative runtime paths on the comparison side
    #[arg(long = "to-work-dir")]
    to_work_dir: Option<PathBuf>,
}

#[derive(::moju_derive::MoJu, Args, Clone, Default)]
#[moju(kind = "struct", domain = "Runtime", module = "Runtime.CliEntry")]
struct PathFilterArgs {
    /// Limit output to one or more config path prefixes, e.g. runtime, sources, window.auth_events
    #[arg(long = "path-prefix")]
    path_prefix: Vec<String>,
}

#[derive(::moju_derive::MoJu, Args, Clone, Default)]
#[moju(kind = "struct", domain = "Runtime", module = "Runtime.CliEntry")]
struct VarFilterArgs {
    /// Limit output to one or more variable-name prefixes, e.g. WORK, CASE_, FAIL_
    #[arg(long = "var-prefix")]
    var_prefix: Vec<String>,
}

#[derive(Subcommand, ::moju_derive::MoJu)]
#[moju(kind = "state", domain = "Runtime", module = "Runtime.CliEntry")]
enum Commands {
    /// Start the WarpFusion engine
    Run {
        #[command(flatten)]
        load: ConfigLoadArgs,
        /// Enable runtime metrics and periodic snapshot output
        #[arg(long)]
        metrics: bool,
        /// Override metrics report interval (e.g. "2s", "30s", "1m")
        #[arg(long)]
        metrics_interval: Option<String>,
        /// Override metrics listen address for /metrics endpoint
        #[arg(long)]
        metrics_listen: Option<String>,
    },
    /// Render merged configuration after overlay/path rebasing and variable expansion
    Config {
        #[command(subcommand)]
        command: ConfigCommands,
    },
}

#[derive(::moju_derive::MoJu, Subcommand)]
#[moju(kind = "state", domain = "Runtime", module = "Runtime.CliEntry")]
enum ConfigCommands {
    /// Render the effective TOML after applying overlays and variable expansion
    Render {
        #[command(flatten)]
        load: ConfigLoadArgs,
        /// Print merged raw TOML before variable expansion/validation
        #[arg(long)]
        raw: bool,
    },
    /// Show the source file that supplied each final config path
    Origins {
        #[command(flatten)]
        load: ConfigLoadArgs,
        #[command(flatten)]
        filter: PathFilterArgs,
    },
    /// Show the final materialized config variables and their sources
    Vars {
        #[command(flatten)]
        load: ConfigLoadArgs,
        #[command(flatten)]
        filter: VarFilterArgs,
    },
    /// Show field-level differences between two loaded config states
    Diff {
        #[command(flatten)]
        load: ConfigLoadArgs,
        #[command(flatten)]
        compare: CompareConfigLoadArgs,
        #[command(flatten)]
        filter: PathFilterArgs,
        /// Compare expanded values after variable substitution instead of raw merged TOML
        #[arg(long)]
        expanded: bool,
    },
}

#[derive(::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Runtime", module = "Runtime.CliEntry")]
struct ResolvedConfigLoad {
    config_path: PathBuf,
    overlay_paths: Vec<PathBuf>,
    runtime_base_dir: PathBuf,
    config_ctx: ConfigVarContext,
}

fn resolve_config_load(load: ConfigLoadArgs) -> EngineResult<ResolvedConfigLoad> {
    resolve_config_load_parts(load.config, load.overlay, load.var, load.work_dir)
}

fn resolve_config_load_parts(
    config: PathBuf,
    overlay: Vec<PathBuf>,
    var: Vec<String>,
    work_dir: Option<PathBuf>,
) -> EngineResult<ResolvedConfigLoad> {
    let config_path = config.canonicalize().source_err(
        EngineReason::Cli,
        format!("config path '{}'", config.display()),
    )?;
    let overlay_paths: Vec<PathBuf> = overlay
        .into_iter()
        .map(|path| {
            path.canonicalize().source_err(
                EngineReason::Cli,
                format!("overlay path '{}'", path.display()),
            )
        })
        .collect::<EngineResult<_>>()?;
    let default_base_dir = config_path
        .parent()
        .expect("config path must have a parent directory");
    let runtime_base_dir = if let Some(work_dir) = work_dir {
        let path = work_dir.canonicalize().source_err(
            EngineReason::Cli,
            format!("work-dir path '{}'", work_dir.display()),
        )?;
        if !path.is_dir() {
            return EngineReason::Cli.fail(format!(
                "work-dir path '{}' is not a directory",
                path.display()
            ));
        }
        path
    } else {
        default_base_dir.to_path_buf()
    };
    let cli_vars = parse_vars(&var).conv_err()?;
    let config_ctx = ConfigVarContext::from_explicit_vars(cli_vars);

    Ok(ResolvedConfigLoad {
        config_path,
        overlay_paths,
        runtime_base_dir,
        config_ctx,
    })
}

fn resolve_compare_config_load(
    base: &ResolvedConfigLoad,
    compare: CompareConfigLoadArgs,
) -> EngineResult<ResolvedConfigLoad> {
    resolve_config_load_parts(
        compare
            .to_config
            .unwrap_or_else(|| base.config_path.clone()),
        compare.to_overlay,
        compare.to_var,
        compare.to_work_dir,
    )
}

fn format_value<T: std::fmt::Display>(value: &T) -> String {
    value.to_string()
}

fn matches_any_prefix(path: &str, prefixes: &[String]) -> bool {
    prefixes.is_empty()
        || prefixes
            .iter()
            .any(|prefix| path_matches_prefix(path, prefix))
}

fn path_matches_prefix(path: &str, prefix: &str) -> bool {
    path == prefix
        || path
            .strip_prefix(prefix)
            .is_some_and(|rest| rest.starts_with('.') || rest.starts_with('['))
}

fn matches_any_var_prefix(key: &str, prefixes: &[String]) -> bool {
    prefixes.is_empty() || prefixes.iter().any(|prefix| key.starts_with(prefix))
}

pub async fn run_cli() -> EngineResult<()> {
    match run_cli_inner().await {
        Ok(()) => Ok(()),
        Err(err) => {
            let report: DiagnosticReport = err.report();
            eprintln!("{}", report.render());
            Err(err)
        }
    }
}

async fn run_cli_inner() -> EngineResult<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Run {
            load,
            metrics,
            metrics_interval,
            metrics_listen,
        } => {
            let resolved = resolve_config_load(load)?;
            let loader = FusionConfigLoader::new(
                &resolved.config_path,
                &resolved.overlay_paths,
                &resolved.config_ctx,
                Some(&resolved.runtime_base_dir),
            );
            let raw = loader.load_raw().conv_err()?;
            let mut fusion_config = loader.load().conv_err()?;
            if metrics || metrics_interval.is_some() || metrics_listen.is_some() {
                fusion_config.metrics.enabled = true;
            }
            if let Some(interval) = metrics_interval {
                fusion_config.metrics.report_interval =
                    HumanDuration::from_str(&interval).conv_err()?;
            }
            if let Some(listen) = metrics_listen {
                fusion_config.metrics.prometheus_listen = listen;
            }
            let metrics_enabled = fusion_config.metrics.enabled;
            let metrics_interval = fusion_config.metrics.report_interval;
            let metrics_listen = fusion_config.metrics.prometheus_listen.clone();

            let _guard =
                init_tracing(&fusion_config.logging, &resolved.runtime_base_dir).conv_err()?;

            let reactor = match Reactor::start(fusion_config, raw, &resolved.runtime_base_dir).await
            {
                Ok(reactor) => reactor,
                Err(err) => return Err(render_runtime_error(err)),
            };
            tracing::info!(domain = "sys", "WarpFusion reactor started");
            if metrics_enabled {
                tracing::info!(
                    domain = "res",
                    interval = %metrics_interval,
                    listen = %metrics_listen,
                    "runtime metrics enabled"
                );
            }

            if let Err(err) = reactor.run().await.map(|_| ()) {
                return Err(render_runtime_error(err));
            }
        }
        Commands::Config { command } => match command {
            ConfigCommands::Render { load, raw } => {
                let resolved = resolve_config_load(load)?;
                let loader = FusionConfigLoader::new(
                    &resolved.config_path,
                    &resolved.overlay_paths,
                    &resolved.config_ctx,
                    Some(&resolved.runtime_base_dir),
                );
                let rendered = if raw {
                    loader.load_merged_toml().conv_err()?
                } else {
                    loader.load_expanded_toml().conv_err()?
                };
                print!("{rendered}");
            }
            ConfigCommands::Origins { load, filter } => {
                let resolved = resolve_config_load(load)?;
                let raw = FusionConfigLoader::new(
                    &resolved.config_path,
                    &resolved.overlay_paths,
                    &resolved.config_ctx,
                    Some(&resolved.runtime_base_dir),
                )
                .load_raw()
                .conv_err()?;
                let mut matched = 0usize;
                for (path, origin) in raw.origin_entries() {
                    if !matches_any_prefix(&path, &filter.path_prefix) {
                        continue;
                    }
                    matched += 1;
                    println!("{path}\t{}", origin.display());
                }
                if matched == 0 {
                    println!("no matching paths");
                }
            }
            ConfigCommands::Vars { load, filter } => {
                let resolved = resolve_config_load(load)?;
                let vars = FusionConfigLoader::new(
                    &resolved.config_path,
                    &resolved.overlay_paths,
                    &resolved.config_ctx,
                    Some(&resolved.runtime_base_dir),
                )
                .load_effective_vars()
                .conv_err()?;
                let mut matched = 0usize;
                for entry in vars {
                    if !matches_any_var_prefix(&entry.key, &filter.var_prefix) {
                        continue;
                    }
                    matched += 1;
                    println!("{}\t{}\t{}", entry.key, entry.value, entry.source);
                }
                if matched == 0 {
                    println!("no matching vars");
                }
            }
            ConfigCommands::Diff {
                load,
                compare,
                filter,
                expanded,
            } => {
                let resolved = resolve_config_load(load)?;
                let compare_resolved = resolve_compare_config_load(&resolved, compare)?;
                let left_loader = FusionConfigLoader::new(
                    &resolved.config_path,
                    &resolved.overlay_paths,
                    &resolved.config_ctx,
                    Some(&resolved.runtime_base_dir),
                );
                let right_loader = FusionConfigLoader::new(
                    &compare_resolved.config_path,
                    &compare_resolved.overlay_paths,
                    &compare_resolved.config_ctx,
                    Some(&compare_resolved.runtime_base_dir),
                );
                let left = if expanded {
                    left_loader.load_expanded_raw().conv_err()?
                } else {
                    left_loader.load_raw().conv_err()?
                };
                let right = if expanded {
                    right_loader.load_expanded_raw().conv_err()?
                } else {
                    right_loader.load_raw().conv_err()?
                };

                let changes: Vec<_> = left
                    .diff(&right)
                    .into_iter()
                    .filter(|change| matches_any_prefix(&change.path, &filter.path_prefix))
                    .collect();
                if changes.is_empty() {
                    println!("no changes");
                    return Ok(());
                }

                for change in changes {
                    println!("path: {}", change.path);
                    println!(
                        "  old: {}",
                        change
                            .old_value
                            .as_ref()
                            .map(format_value)
                            .unwrap_or_else(|| "<none>".to_string())
                    );
                    println!(
                        "  new: {}",
                        change
                            .new_value
                            .as_ref()
                            .map(format_value)
                            .unwrap_or_else(|| "<none>".to_string())
                    );
                    println!(
                        "  old_origin: {}",
                        change
                            .old_origin
                            .as_deref()
                            .map(|path| path.display().to_string())
                            .unwrap_or_else(|| "<none>".to_string())
                    );
                    println!(
                        "  new_origin: {}",
                        change
                            .new_origin
                            .as_deref()
                            .map(|path| path.display().to_string())
                            .unwrap_or_else(|| "<none>".to_string())
                    );
                }
            }
        },
    }

    Ok(())
}

fn render_runtime_error(err: RuntimeError) -> error::EngineError {
    err.conv()
}
