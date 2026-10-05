mod config;
mod http_status;
mod logging;
mod service;
mod status;

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use config::AppConfig;
use tracing::{error, info};

#[derive(Debug, Parser)]
#[command(
    version,
    about = "Conflate Redis Pub/Sub messages and publish atomic batches"
)]
struct Args {
    #[arg(short, long, default_value = "config.json")]
    config: PathBuf,

    #[arg(long, help = "Validate the configuration and exit")]
    check_config: bool,
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("error: {error:#}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let args = Args::parse();
    let config = AppConfig::load(&args.config).with_context(|| {
        format!(
            "failed to load configuration from {}",
            args.config.display()
        )
    })?;
    config.validate()?;

    if args.check_config {
        println!("Configuration is valid.");
        return Ok(());
    }

    let _instance_lock = config.instance_lock.acquire()?;
    let _logging_guard = logging::init(&config.logging)?;
    let metrics = Arc::new(status::Metrics::new());

    info!(version = env!("CARGO_PKG_VERSION"), "service_started");
    if let Err(error) = service::run(config, metrics.clone()).await {
        error!(error = %error, "service_failed");
        return Err(error);
    }

    info!("service_stopped");
    Ok(())
}
