use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};

use wf_config::FusionConfig;
use wf_runtime::lifecycle::{FusionEngine, wait_for_signal};

#[derive(Parser)]
#[command(name = "wf", about = "WarpFusion CEP engine")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the WarpFusion engine
    Run {
        /// Path to fusion.toml config file
        #[arg(short, long)]
        config: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let cli = Cli::parse();

    match cli.command {
        Commands::Run { config } => {
            let config_path = config.canonicalize().map_err(|e| {
                anyhow::anyhow!("config path '{}': {e}", config.display())
            })?;
            let fusion_config = FusionConfig::load(&config_path)?;
            let base_dir = config_path
                .parent()
                .expect("config path must have a parent directory");

            let engine = FusionEngine::start(fusion_config, base_dir).await?;
            log::info!("WarpFusion listening on {}", engine.listen_addr());

            wait_for_signal(engine.cancel_token()).await;
            engine.shutdown();
            engine.wait().await?;
        }
    }

    Ok(())
}
