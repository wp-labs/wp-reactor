use std::path::PathBuf;
use std::str::FromStr;

use anyhow::Result;
use clap::{Parser, Subcommand};

use wf_config::{FusionConfig, HumanDuration};
use wf_runtime::lifecycle::{Reactor, wait_for_signal};
use wf_runtime::tracing_init::init_tracing;

#[derive(Parser)]
#[command(name = "wfusion", about = "WarpFusion CEP engine")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the WarpFusion engine
    Run {
        /// Path to wfusion.toml config file
        #[arg(short, long)]
        config: PathBuf,
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
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Run {
            config,
            metrics,
            metrics_interval,
            metrics_listen,
        } => {
            let config_path = config
                .canonicalize()
                .map_err(|e| anyhow::anyhow!("config path '{}': {e}", config.display()))?;
            let mut fusion_config = FusionConfig::load(&config_path)?;
            if metrics || metrics_interval.is_some() || metrics_listen.is_some() {
                fusion_config.metrics.enabled = true;
            }
            if let Some(interval) = metrics_interval {
                fusion_config.metrics.report_interval = HumanDuration::from_str(&interval)
                    .map_err(|e| anyhow::anyhow!("invalid --metrics-interval '{interval}': {e}"))?;
            }
            if let Some(listen) = metrics_listen {
                fusion_config.metrics.prometheus_listen = listen;
            }
            let metrics_enabled = fusion_config.metrics.enabled;
            let metrics_interval = fusion_config.metrics.report_interval;
            let metrics_listen = fusion_config.metrics.prometheus_listen.clone();
            let base_dir = config_path
                .parent()
                .expect("config path must have a parent directory");

            let _guard = init_tracing(&fusion_config.logging, base_dir)?;

            let reactor = Reactor::start(fusion_config, base_dir)
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            tracing::info!(domain = "sys", listen = %reactor.listen_addr(), "WarpFusion reactor started");
            if metrics_enabled {
                tracing::info!(
                    domain = "res",
                    interval = %metrics_interval,
                    listen = %metrics_listen,
                    "runtime metrics enabled"
                );
            }

            wait_for_signal(reactor.cancel_token()).await;
            reactor.shutdown();
            reactor.wait().await.map_err(|e| anyhow::anyhow!("{e}"))?;
        }
    }

    Ok(())
}
