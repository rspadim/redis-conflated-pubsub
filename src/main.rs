//! Redis Pub/Sub is at-most-once: messages published while the input is disconnected cannot be
//! replayed. Queues and pending output batches are kept only in process memory, so a process crash
//! can discard them. No disk spool is used, and payload bytes are never written to status files or
//! logs. A failed PUBLISH or EXEC chunk is removed from pending, accounted as dropped or uncertain,
//! and never retried; the output continues with later chunks. Byte limits count the complete RESP
//! request, including MULTI/EXEC framing. Oversized messages default to `send`; `truncate` removes
//! only a payload suffix, and `drop` skips the affected output message. Status exposes dropped,
//! truncated, publish-error, and uncertain message counters plus per-output `pending_messages`.

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
    about = "Fan out Redis Pub/Sub messages to named outputs with optional conflation"
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
