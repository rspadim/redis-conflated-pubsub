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
#[cfg(windows)]
mod service_host;
mod status;

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::time;

use anyhow::{Context, Result, bail};
use clap::Parser;
use config::{AppConfig, NatsConfig, RedisConfig};
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
Each output can configure outputs.<name>.deduplication.ttl_ms as a signed integer (default 0; nonpositive disables only deduplication; positive durations are limited to 365 days). Deduplication compares the exact mapped output channel and raw incoming payload bytes, including bytes removed by truncate. The cache remembers only the latest successfully or uncertainly published payload per channel: A->B->A publishes all three values, while consecutive repeats are suppressed until the TTL expires. In direct mode, changed values pass immediately; in conflated mode, only the final pending value per channel is compared immediately before each chunk is sent. Caches are independent, in-memory per output, and expired entries are periodically pruned.\n\n\
Output-local deduplication_groups set ttl_ms, round_ms, restart_on_change, max_members, and max_cache_bytes; round_ms floors the Unix-epoch TTL-start timestamp to a bucket (0 disables rounding). Shared expiry starts on the first successful or uncertain publication. With restart_on_change=true, a new member's first successful or uncertain publication or a changed final value successfully or uncertainly published restarts it; restart_on_change=false keeps it fixed. Each group cache defaults to 16384 members and 64 MiB of channel-name plus payload data (maximum 100000 members and 256 MiB); old entries are evicted at capacity, while a single item over the byte limit is not cached (but still published). Profiles contain partial conflation and TTL/group overrides.\n\n\
channel_policies are ordered rules for the final mapped output channel. Each rule has exactly one glob, prefix, or suffix selector, or default: true as the last catch-all rule, and chooses either a profile or inline conflation.interval_ms, deduplication.ttl_ms, or deduplication.group values. Only the first match applies; unmatched channels use output-level defaults. Runtime validation checks that the default is last and that round_ms does not exceed a positive group TTL. Positive intervals and TTLs are limited to 365 days.\n\n\
When input and output use the same Redis server, exclude_output_echoes defaults to true and filters mapped output channels before fan-out. exclude_sentinel_pubsub also defaults to true and filters Sentinel hello/notification channels. Channel filters use ordered `filters` with one `glob`, `regex`, `raw`, `single`, or `string` selector and an `accept`/`deny` action. `raw`, `single`, and `string` all compare the complete channel string literally. All rules are evaluated and the last match wins; `filter_default` defaults to `accept` if none match. Input filters see the source channel before subscription mappings; output filters see the subscription-mapped channel before that output's prefix/suffix. Selectors compile to internal enum variants and their source config is discarded after setup. `input.filter_cache_max_entries`, `outputs.<name>.filter_cache_max_entries`, and `outputs.<name>.channel_policy_cache_max_entries` independently size the local LRUs (default 16384 each, maximum 100000; zero disables that cache). `status.http.filters_endpoint_enabled` explicitly enables `GET /filters` with full cache contents; it is disabled by default. On Unix, SIGHUP reloads a valid configuration; an invalid file leaves the current runtime active. An accepted reload drains pending output queues and restarts input/output workers, creating a brief Pub/Sub input gap and resetting status counters. Changes to logging still require a full systemd restart; when instance_lock.path changes, the new lock is acquired before the previous lock is released, and the reload is rejected if the new lock is unavailable. Payloads and pending queues are process-local; there is no disk spool.",
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
        help = "Validate the configuration, then CONNECT to every configured backend and exit. \
                Uses the same client and credentials the relay uses at startup, so a wrong \
                address, password or TLS setting is reported before the service is installed"
    )]
    test_connection: bool,

    #[arg(
        long,
        help = "Print the JSON Schema for the configuration format and exit"
    )]
    config_json_schema: bool,
}

fn main() {
    // A process the service control manager started has to speak the service
    // protocol; a console run falls straight through.
    #[cfg(windows)]
    if let service_host::Dispatch::RanAsService = service_host::dispatch() {
        return;
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name_fn(runtime_worker_name)
        .build()
        .expect("failed to initialize Tokio runtime");

    runtime.block_on(async {
        if let Err(error) = run(None).await {
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

/// Read-only Redis commands the probe tries, in order.
///
/// The installer runs this against a server the user typed in, which often
/// belongs to somebody else, so the probe must never write, publish or create
/// anything: `DBSIZE` and `PUBSUB CHANNELS` are reads (the second is the
/// category the relay's input actually uses), and `PING` is the last resort.
///
/// A ladder, and not `PING` alone, because "did the server answer" and "was the
/// login accepted" are the only questions a probe may answer, and because a
/// single command can be unavailable for reasons that say nothing about the
/// installation: a server that fails its background save answers with
/// `-MISCONF` while still serving every read (`stop-writes-on-bgsave-error`),
/// and a read-only ACL user is denied `PING` (`@connection`) but allowed
/// `DBSIZE` (`@read`). A permission denial is *not* a failure here - see
/// [`probe_redis`].
const REDIS_PROBE_COMMANDS: &[&[&str]] = &[&["DBSIZE"], &["PUBSUB", "CHANNELS"], &["PING"]];

/// Connects to every configured backend and exits, so the installer can prove a
/// host/password/TLS setting before anything is installed or the service is
/// started. It reuses the very client configuration the relay uses
/// ([`RedisConfig::client`] and the NATS connection plan), never a parallel
/// implementation: a probe that disagreed with the runtime would be worse than
/// none. Read-only - it opens a connection (and, for Redis, sends one of the
/// [`REDIS_PROBE_COMMANDS`] reads) and drops it; nothing is published, no key is
/// touched and no NATS object is created.
async fn test_connection(config: &AppConfig) -> Result<()> {
    let mut failures = 0usize;

    // Input first: the relay cannot deliver anything without it.
    if let Some(redis) = &config.input.redis {
        match probe_redis(redis).await {
            Ok(()) => println!("input redis  {}:{}  OK", redis.host, redis.port),
            Err(error) => {
                failures += 1;
                eprintln!(
                    "input redis  {}:{}  FAILED: {error:#}",
                    redis.host, redis.port
                );
            }
        }
    }
    if let Some(nats) = &config.input.nats {
        match connect_nats(nats).await {
            Ok(()) => println!("input nats   {}  OK", nats.servers.join(",")),
            Err(error) => {
                failures += 1;
                eprintln!("input nats   {}  FAILED: {error:#}", nats.servers.join(","));
            }
        }
    }

    for (name, output) in &config.outputs {
        if let Some(redis) = &output.redis {
            match probe_redis(redis).await {
                Ok(()) => println!("output {name}  {}:{}  OK", redis.host, redis.port),
                Err(error) => {
                    failures += 1;
                    eprintln!(
                        "output {name}  {}:{}  FAILED: {error:#}",
                        redis.host, redis.port
                    );
                }
            }
        }
        if let Some(nats) = &output.nats {
            match connect_nats(nats).await {
                Ok(()) => println!("output {name}  {}  OK", nats.servers.join(",")),
                Err(error) => {
                    failures += 1;
                    eprintln!(
                        "output {name}  {}  FAILED: {error:#}",
                        nats.servers.join(",")
                    );
                }
            }
        }
    }

    if failures > 0 {
        anyhow::bail!("{failures} backend(s) did not answer; the service would fail the same way");
    }
    Ok(())
}

/// Opens (and immediately drops) a Redis connection, proving the address,
/// credentials, TLS and database selection with a READ.
///
/// The verdict is "connected and authenticated", never "allowed to run this
/// particular command": per-command authorization belongs to whoever issued the
/// credentials, so a `NOPERM`, `READONLY` or `MISCONF` reply still proves the
/// probe reached a server that accepted the login. Only a failure to connect or
/// a rejected login fails the probe.
async fn probe_redis(config: &RedisConfig) -> Result<()> {
    let client = config.client()?;
    let connection = time::timeout(
        config.connect_timeout(),
        client.get_multiplexed_async_connection(),
    )
    .await
    .context("timed out connecting")?;
    let mut connection = match connection {
        Ok(connection) => connection,
        // The handshake (AUTH, and SELECT for a non-zero database) is where a
        // rejected login surfaces, so it has to be classified here too: without
        // this a wrong password would be reported as an unreachable server and
        // the installer would show the wrong advice.
        Err(error) if is_auth_failure(&error) => return Err(authentication_error(&error)),
        Err(error) => bail!("could not connect: {error}"),
    };

    // The handshake was accepted; the reads below prove the server still
    // answers on this connection.
    let mut last_error: Option<redis::RedisError> = None;
    for command in REDIS_PROBE_COMMANDS {
        let mut cmd = redis::cmd(command[0]);
        for argument in &command[1..] {
            cmd.arg(*argument);
        }
        match cmd.query_async::<redis::Value>(&mut connection).await {
            Ok(_) => return Ok(()),
            Err(error) => {
                if is_auth_failure(&error) {
                    return Err(authentication_error(&error));
                }
                if is_server_reply(&error) {
                    // The server answered and the connection handshake (AUTH and
                    // SELECT) was accepted; it only refused this command. A
                    // denied command says nothing about the installation, so it
                    // is not the installer's verdict to make.
                    return Ok(());
                }
                // Connection-level failure (I/O, protocol): try the next read
                // once, then report the last reason.
                last_error = Some(error);
            }
        }
    }

    match last_error {
        Some(error) => bail!("the server did not answer a read: {error}"),
        None => bail!("the server did not answer a read"),
    }
}

/// True when the server rejected the credentials themselves, which is the one
/// answer a probe must report as a failure.
fn is_auth_failure(error: &redis::RedisError) -> bool {
    if error.kind() == redis::ErrorKind::AuthenticationFailed {
        return true;
    }
    if matches!(error.code(), Some("NOAUTH" | "WRONGPASS")) {
        return true;
    }
    // A SELECT against a server that demands a login is reported by the client
    // as a database-switch failure whose detail says the authentication is
    // missing; the kind alone would call that an unreachable server.
    error.detail().is_some_and(|detail| {
        let detail = detail.to_ascii_lowercase();
        detail.contains("authentication required")
            || detail.contains("noauth")
            || detail.contains("invalid password")
            || detail.contains("wrongpass")
    })
}

/// One clean line for a rejected login. `RedisError`'s own source chain repeats
/// the same detail, which made the installer's report carry it twice.
fn authentication_error(error: &redis::RedisError) -> anyhow::Error {
    let reason = error
        .detail()
        .unwrap_or("the server rejected the credentials");
    anyhow::anyhow!("authentication failed: {reason}")
}

/// True when the server itself answered with an error (`-MISCONF`, `-NOPERM`,
/// `-READONLY`, `-LOADING`, `-ERR ...`). That is proof of a live, authenticated
/// connection: the login was accepted at handshake time, and per-command
/// authorization belongs to whoever issued the credentials. The cases this
/// covers are real - a server with `stop-writes-on-bgsave-error yes` answers
/// `PING` with `-MISCONF` while serving every read, and a read-only ACL user is
/// denied a command it does not need.
fn is_server_reply(error: &redis::RedisError) -> bool {
    matches!(
        error.kind(),
        redis::ErrorKind::Extension | redis::ErrorKind::Server(_)
    )
}

/// Opens (and immediately drops) a NATS connection using the relay's own
/// connection plan, so user/password, token, NKey seed, creds file and TLS are
/// all exercised exactly as at startup.
async fn connect_nats(config: &NatsConfig) -> Result<()> {
    service::nats::test_connection(config).await
}

/// Runs the relay until it is told to stop.
///
/// `service_stop` is the Windows service host's stop trigger: the service control
/// manager, not a console signal, stops a service. Every other caller passes
/// `None`.
async fn run(service_stop: Option<tokio::sync::watch::Receiver<bool>>) -> Result<()> {
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

    if args.test_connection {
        test_connection(&config).await?;
        return Ok(());
    }

    #[cfg(unix)]
    let mut instance_lock = config.instance_lock.acquire()?;
    #[cfg(not(unix))]
    let _instance_lock = config.instance_lock.acquire()?;
    // `None` when logging is disabled; tracing events then have no subscriber and are no-ops.
    let _logging_guard = logging::init(&config.logging)?;
    #[cfg(windows)]
    let mut shutdown_signals = match service_stop {
        Some(receiver) => service::ShutdownSignals::with_service_stop(receiver)?,
        None => service::ShutdownSignals::new()?,
    };
    #[cfg(not(windows))]
    let mut shutdown_signals = {
        let _ = service_stop;
        service::ShutdownSignals::new()?
    };
    #[cfg(unix)]
    {
        let mut active_config = config;
        let mut metrics = Arc::new(status::Metrics::new());
        let mut fallback_config: Option<AppConfig> = None;

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
                        if active_config.instance_lock.path != previous_config.instance_lock.path {
                            // The failed start had already swapped in its own lock; hold the
                            // restored lock before releasing the failed one.
                            let restored_lock =
                                previous_config.instance_lock.acquire().with_context(|| {
                                    format!(
                                        "failed to reacquire instance lock {} while restoring the previous configuration",
                                        previous_config.instance_lock.path.display()
                                    )
                                })?;
                            drop(std::mem::replace(&mut instance_lock, restored_lock));
                        }
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
                service::RunOutcome::Reload {
                    config: next_config,
                    instance_lock: next_lock,
                } => {
                    fallback_config = Some(active_config.clone());
                    active_config = *next_config;
                    if let Some(next_lock) = next_lock {
                        // Acquired before the drain; the previous lock is dropped only now.
                        drop(std::mem::replace(&mut instance_lock, next_lock));
                        info!(
                            path = %active_config.instance_lock.path.display(),
                            "instance_lock_switched"
                        );
                    }
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
#[path = "../tests/unit/cli_help.rs"]
mod tests;

#[cfg(test)]
#[path = "../tests/unit/probe_read_only.rs"]
mod probe_read_only_tests;
