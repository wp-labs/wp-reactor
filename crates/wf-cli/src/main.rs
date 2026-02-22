use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};

use wf_config::FusionConfig;
use wf_runtime::lifecycle::{Reactor, wait_for_signal};
use wf_runtime::tracing_init::init_tracing;

#[derive(Parser)]
#[command(name = "warp-fusion", about = "WarpFusion CEP engine")]
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
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Run { config } => {
            let config_path = config
                .canonicalize()
                .map_err(|e| anyhow::anyhow!("config path '{}': {e}", config.display()))?;
            let fusion_config = FusionConfig::load(&config_path)?;
            let base_dir = config_path
                .parent()
                .expect("config path must have a parent directory");

            let _guard = init_tracing(&fusion_config.logging, base_dir)?;

            let reactor = Reactor::start(fusion_config, base_dir)
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            tracing::info!(domain = "sys", listen = %reactor.listen_addr(), "WarpFusion reactor started");

            wait_for_signal(reactor.cancel_token()).await;
            reactor.shutdown();
            reactor.wait().await.map_err(|e| anyhow::anyhow!("{e}"))?;
        }
    }

    Ok(())
}
