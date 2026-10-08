//! Rust E2E proxies and drivers for the Compose integration suites.
//!
//! Subcommands replace the former Python helpers:
//!
//! * `fault-proxy` — `tests/redis_fault_proxy.py`: forwards RESP to Redis and
//!   closes the client socket after the configured Nth successful `EXEC`
//!   reply, exposing `accepted_connections`, `multi_exec_attempts`,
//!   `successful_execs`, `dropped_exec_replies`, `drop_publish_count`,
//!   `execs_after_drop` and `publishes_after_drop` on `STATS_PORT`.
//! * `counting-proxy` — `tests/redis_counting_proxy.py`: forwards RESP while
//!   counting `MULTI`/`EXEC`/`PUBLISH` per output database and reporting exact
//!   request sizes as JSON on `STATS_PORT`.
//! * `protocol-integration` — `tests/docker_protocol_integration.py`: drives
//!   the RESP protocol/oversized-policy E2E in `compose.protocol.test.yml`.
//! * `fault-integration` — `tests/docker_fault_integration.py`: drives the
//!   fault-injection E2E in the `fault` profile of `compose.test.yml`.
//!
//! Built by `Dockerfile.e2e` and installed as `redis-conflated-e2e`.

#[path = "e2e/integration.rs"]
mod integration;
#[path = "e2e/proxy.rs"]
mod proxy;
#[path = "e2e/resp.rs"]
mod resp;

use anyhow::Result;
use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(
    name = "redis-conflated-e2e",
    about = "Rust E2E proxies and drivers for the Compose integration suites"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Forward RESP to Redis, dropping the configured Nth successful EXEC reply.
    FaultProxy,
    /// Count MULTI/EXEC/PUBLISH per output database and expose JSON stats.
    CountingProxy,
    /// Drive the RESP protocol/oversized-policy E2E.
    ProtocolIntegration,
    /// Drive the fault-injection E2E.
    FaultIntegration,
}

#[tokio::main]
async fn main() -> Result<()> {
    match Cli::parse().command {
        Command::FaultProxy => proxy::run_fault_proxy().await,
        Command::CountingProxy => proxy::run_counting_proxy().await,
        Command::ProtocolIntegration => integration::run_protocol_integration().await,
        Command::FaultIntegration => integration::run_fault_integration().await,
    }
}
