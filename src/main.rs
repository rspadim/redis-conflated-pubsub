//! Redis Pub/Sub is at-most-once: messages published while the input is disconnected cannot be
//! replayed. Queues and pending output batches are kept only in process memory, so a process crash
//! can discard them. No disk spool is used, and payload bytes are never written to status files or
//! logs. A failed PUBLISH or EXEC chunk is removed from pending, accounted as dropped or uncertain,
//! and never retried; the output continues with later chunks. Byte limits count the complete RESP
//! request, including MULTI/EXEC framing. Oversized messages default to `send`; `truncate` removes
//! only a payload suffix, and `drop` skips the affected output message. Status exposes dropped,
//! truncated, deduplicated, publish-error, and uncertain message counters plus per-output
//! `pending_messages`. Per-output deduplication caches are memory-only and expire by TTL.

mod config;
mod http_status;
mod logging;
mod service;
mod status;

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::{Context, Result};
use clap::Parser;
use config::AppConfig;
#[cfg(unix)]
use tracing::warn;
use tracing::{error, info};

#[derive(Debug, Parser)]
#[command(
    version,
    about = "Forward raw Redis Pub/Sub messages to independently configured outputs",
    long_about = "Forward messages from Redis SUBSCRIBE/PSUBSCRIBE to named Redis outputs. Payloads remain raw bytes by default. Outputs can publish immediately or conflate to the latest value per mapped channel.\n\n\
For conflated outputs, each MULTI/EXEC request is chunked by both max_commands_per_exec and the exact RESP request byte target. The byte count includes the MULTI and EXEC frames and every PUBLISH array/bulk-string frame. max_bytes_per_exec is a client-side target, not discovery of the output Redis server's own limits.\n\n\
If one PUBLISH exceeds its output target, oversized_message_policy selects send (default, preserve the full payload), truncate (remove only a payload suffix), or drop (skip that output's copy). Failed publishes are not retried; the affected operation is counted and later messages/chunks continue.\n\n\
Each output can configure outputs.<name>.deduplication.ttl_ms as a signed integer (default 5000; nonpositive disables only deduplication; positive durations are limited to 365 days). Deduplication compares the exact mapped output channel and raw incoming payload bytes, including bytes removed by truncate. The cache remembers only the latest successfully or uncertainly published payload per channel: A->B->A publishes all three values, while consecutive repeats are suppressed until the TTL expires. In direct mode, changed values pass immediately; in conflated mode, only the final pending value per channel is compared immediately before each chunk is sent. Caches are independent, in-memory per output, and expired entries are periodically pruned.\n\n\
Output-local deduplication_groups set ttl_ms, round_ms, restart_on_change, max_members, and max_cache_bytes; round_ms floors the Unix-epoch TTL-start timestamp to a bucket (0 disables rounding). Shared expiry starts on the first successful or uncertain publication. With restart_on_change=true, a new member's first successful or uncertain publication or a changed final value successfully or uncertainly published restarts it; restart_on_change=false keeps it fixed. Each group cache defaults to 16384 members and 64 MiB of channel-name plus payload data (maximum 100000 members and 256 MiB); old entries are evicted at capacity, while a single item over the byte limit is not cached (but still published). Profiles contain partial conflation and TTL/group overrides.\n\n\
channel_policies are ordered rules for the final mapped output channel. Each rule has exactly one glob, prefix, or suffix selector, or default: true as the last catch-all rule, and chooses either a profile or inline conflation.interval_ms, deduplication.ttl_ms, or deduplication.group values. Only the first match applies; unmatched channels use output-level defaults. Runtime validation checks that the default is last and that round_ms does not exceed a positive group TTL. Positive intervals and TTLs are limited to 365 days.\n\n\
When input and output use the same Redis server, exclude_output_echoes defaults to true and filters mapped output channels before fan-out. exclude_sentinel_pubsub also defaults to true and filters Sentinel hello/notification channels. Both can be changed under input in the JSON configuration. On Unix, SIGHUP reloads a valid configuration; an invalid file leaves the current runtime active. An accepted reload drains pending output queues and restarts input/output workers, creating a brief Pub/Sub input gap and resetting status counters. Changes to logging or instance_lock still require a full systemd restart. Payloads and pending queues are process-local; there is no disk spool.",
    after_help = "Examples:\n  redis-conflated-pubsub --config config.json\n  redis-conflated-pubsub --config config.json --check-config\n  redis-conflated-pubsub --config-json-schema > config.schema.json"
)]
struct Args {
    #[arg(
        short,
        long,
        value_name = "FILE",
        default_value = "config.json",
        help = "Path to the JSON configuration file"
    )]
    config: PathBuf,

    #[arg(
        long,
        help = "Validate the selected JSON configuration and exit without connecting to Redis"
    )]
    check_config: bool,

    #[arg(
        long,
        help = "Print the JSON Schema for the configuration format and exit"
    )]
    config_json_schema: bool,
}

fn main() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name_fn(runtime_worker_name)
        .build()
        .expect("failed to initialize Tokio runtime");

    runtime.block_on(async {
        if let Err(error) = run().await {
            eprintln!("error: {error:#}");
            std::process::exit(1);
        }
    });
}

fn runtime_worker_name() -> String {
    static NEXT_THREAD_ID: AtomicUsize = AtomicUsize::new(0);
    format!(
        "conflate-{}",
        NEXT_THREAD_ID.fetch_add(1, Ordering::Relaxed)
    )
}

async fn run() -> Result<()> {
    let args = Args::parse();
    if args.config_json_schema {
        println!(
            "{}",
            serde_json::to_string_pretty(&AppConfig::json_schema())?
        );
        return Ok(());
    }
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
    let mut shutdown_signals = service::ShutdownSignals::new()?;
    #[cfg(unix)]
    {
        let mut active_config = config;
        let mut metrics = Arc::new(status::Metrics::new());
        let mut fallback_config = None;

        info!(version = env!("CARGO_PKG_VERSION"), "service_started");
        loop {
            let run_result = service::run(
                active_config.clone(),
                Arc::clone(&metrics),
                &args.config,
                &mut shutdown_signals,
            )
            .await;
            let outcome = match run_result {
                Ok(outcome) => outcome,
                Err(error) => {
                    if let Some(previous_config) = fallback_config.take() {
                        warn!(error = %error, "configuration_reload_start_failed; restoring previous configuration");
                        active_config = previous_config;
                        metrics = Arc::new(status::Metrics::new());
                        continue;
                    }
                    error!(error = %error, "service_failed");
                    return Err(error);
                }
            };
            match outcome {
                service::RunOutcome::Shutdown => break,
                service::RunOutcome::Reload(next_config) => {
                    fallback_config = Some(active_config.clone());
                    active_config = *next_config;
                    metrics = Arc::new(status::Metrics::new());
                    info!("service_configuration_reloaded");
                }
            }
        }
    }
    #[cfg(not(unix))]
    {
        let metrics = Arc::new(status::Metrics::new());
        info!(version = env!("CARGO_PKG_VERSION"), "service_started");
        if let Err(error) = service::run(config, metrics, &args.config, &mut shutdown_signals).await
        {
            error!(error = %error, "service_failed");
            return Err(error);
        }
    }

    info!("service_stopped");
    Ok(())
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory;

    use super::Args;
    use crate::config::AppConfig;

    #[test]
    fn long_help_documents_configuration_and_examples() {
        let help = Args::command().render_long_help().to_string();
        let help = help.split_whitespace().collect::<Vec<_>>().join(" ");
        for expected in [
            "max_bytes_per_exec",
            "oversized_message_policy",
            "exclude_output_echoes",
            "exclude_sentinel_pubsub",
            "outputs.<name>.deduplication.ttl_ms",
            "deduplication_groups",
            "round_ms",
            "floors the Unix-epoch TTL-start timestamp",
            "0 disables rounding",
            "restart_on_change",
            "Shared expiry starts",
            "new member's first successful or uncertain publication",
            "changed final value successfully or uncertainly published",
            "restart_on_change=false keeps it fixed",
            "Profiles contain partial",
            "max_members",
            "max_cache_bytes",
            "limited to 365 days",
            "channel_policies",
            "exactly one glob, prefix, or suffix selector",
            "default: true as the last catch-all rule",
            "Only the first match applies",
            "chooses either a profile or inline",
            "conflation.interval_ms",
            "deduplication.ttl_ms",
            "deduplication.group",
            "final mapped output channel",
            "unmatched channels use output-level defaults",
            "SIGHUP reloads a valid configuration",
            "invalid file leaves the current runtime active",
            "logging or instance_lock still require a full systemd restart",
            "brief Pub/Sub input gap",
            "resetting status counters",
            "--config-json-schema",
        ] {
            assert!(help.contains(expected), "missing {expected} in help");
        }
    }

    #[test]
    fn generated_config_schema_is_json_and_includes_filter_options() {
        let schema = AppConfig::json_schema();
        let serialized = serde_json::to_string(&schema).unwrap();
        assert!(serialized.contains("exclude_output_echoes"));
        assert!(serialized.contains("exclude_sentinel_pubsub"));
        assert!(serialized.contains("max_bytes_per_exec"));
        assert!(serialized.contains("oversized_message_policy"));
        assert!(serialized.contains("deduplication"));
        assert!(serialized.contains("ttl_ms"));
        assert_eq!(schema["type"], "object");
        assert_eq!(schema["properties"]["outputs"]["minProperties"], 1);
        assert_eq!(
            schema["$defs"]["InputConfig"]["properties"]["subscriptions"]["minItems"],
            1
        );
        let deduplication_ttl = &schema["$defs"]["DeduplicationConfig"]["properties"]["ttl_ms"];
        assert_eq!(deduplication_ttl["type"], "integer");
        assert_eq!(deduplication_ttl["default"], 5000);
        assert!(deduplication_ttl.get("minimum").is_none());
    }
}
