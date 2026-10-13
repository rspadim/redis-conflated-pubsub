use std::{str::FromStr, time::Duration};

use anyhow::{Context, Result};
use async_nats::{Client, ConnectOptions, ServerAddr};
use bytes::Bytes;
use futures_util::StreamExt;
use tokio::time::Instant;

const WAIT_TIMEOUT: Duration = Duration::from_secs(300);

/// Overall wait for a subscriber/verifier to reach the expected count. Tests
/// and the matrix driver set `COMPARE_WAIT_TIMEOUT_SECS` so a regression fails
/// fast instead of blocking for the full default.
fn wait_timeout() -> Duration {
    std::env::var("COMPARE_WAIT_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map_or(WAIT_TIMEOUT, Duration::from_secs)
}

/// Two roles, meant to run as **separate OS processes** so the publisher and the
/// subscriber do not contend for the same event loop or CPU.
///
/// Latency is meaningful only when the publisher is paced **below** the
/// subscriber's drain rate; otherwise the measured age is queue age. Use
/// `COMPARE_RATE` to sweep equal rates for both engines.
#[tokio::main]
async fn main() -> Result<()> {
    let role = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "publisher".to_owned());
    let engine = env_or("COMPARE_ENGINE", "nats");
    let total = env_u64("COMPARE_TOTAL", 50_000).max(1);
    let payload_bytes = env_usize("COMPARE_PAYLOAD_BYTES", 256).max(8);
    let batch = env_usize("COMPARE_BATCH", 1).max(1);
    // Target messages/second; a positive value paces the publisher. 0 = as fast as possible.
    let rate = env_u64("COMPARE_RATE", 0);
    let prefix = env_or("COMPARE_PREFIX", "compare");

    match role.as_str() {
        "publisher" => publish(&engine, &prefix, total, payload_bytes, batch, rate).await,
        "subscriber" => subscribe(&engine, &prefix, total).await,
        "e2e-publish" => e2e_publish(&engine, total, payload_bytes, rate).await,
        "e2e-verify" => e2e_verify(&engine, env_u64("COMPARE_EXPECTED", total)).await,
        other => anyhow::bail!(
            "role must be publisher, subscriber, e2e-publish, or e2e-verify, got {other:?}"
        ),
    }
}

/// Redis connection URL for an input-side role. `REDIS_URL` selects the input
/// server.
fn redis_url() -> String {
    env_or("REDIS_URL", "redis://127.0.0.1:16390")
}

/// Redis connection URL for the verifier (output side). `REDIS_VERIFY_URL`
/// overrides `REDIS_URL` so an input Redis server and an output Redis server can
/// be different processes in the same run.
fn redis_verify_url() -> String {
    std::env::var("REDIS_VERIFY_URL")
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(redis_url)
}

/// Publishes `total` messages to the relay's **input** address space and waits
/// for the broker to acknowledge them before returning. `engine` selects the
/// input backend, so the same publisher drives a Redis or a NATS relay input.
///
/// The input namespace is `COMPARE_INPUT_PREFIX` (default `in`); each message is
/// published on `{prefix}.<segment>.<index>` and the relay config subscribes to
/// `{prefix}.*` (Redis PSUBSCRIBE) or `{prefix}.>` (NATS). The driver substitutes
/// a unique prefix per run so output address spaces never collide across runs.
async fn e2e_publish(engine: &str, total: u64, payload_bytes: usize, rate: u64) -> Result<()> {
    // Wait for the verifier(s) to subscribe before the first publish, so the
    // output space is already being consumed.
    wait_for_ready().await?;
    // The relay configs subscribe to this namespace; a unique per-run value keeps
    // concurrent or repeated runs from observing each other's traffic.
    let input_prefix = env_or("COMPARE_INPUT_PREFIX", "in");
    // When positive, every Nth message is published on a `{prefix}.drop.*`
    // address so an output `deny` filter drops it; the rest go to
    // `{prefix}.keep.*`. The returned count is what the verifier should expect.
    let deny_every = env_u64("COMPARE_FILTER_DENY_EVERY", 0);
    let mut denied = 0u64;
    let started = Instant::now();
    let interval = (rate > 0).then(|| Duration::from_secs_f64(1.0 / rate as f64));
    let mut next_deadline = started;
    match engine {
        "nats" => {
            let client = connect_nats().await?;
            let mut since = 0usize;
            for index in 0..total {
                let subject = input_address(&input_prefix, index, deny_every, &mut denied);
                client
                    .publish(subject, Bytes::from(build_payload(payload_bytes)))
                    .await
                    .context("e2e nats publish")?;
                since += 1;
                if since == 256 {
                    client.flush().await.context("e2e nats flush")?;
                    since = 0;
                }
                pace(&mut next_deadline, interval).await;
            }
            if since > 0 {
                client.flush().await.context("e2e nats flush")?;
            }
            // `async-nats` `flush` only empties the client's local write buffer;
            // it is not a server round trip, so a process that exits immediately
            // can drop the tail of the stream. Barrier on a real round trip
            // before returning.
            nats_barrier(&client, &input_prefix).await?;
        }
        "redis" => {
            let client = redis::Client::open(redis_url())?;
            let mut connection = client.get_multiplexed_async_connection().await?;
            let mut pipe = redis::pipe();
            let mut pending = 0usize;
            for index in 0..total {
                let channel = input_address(&input_prefix, index, deny_every, &mut denied);
                pipe.cmd("PUBLISH")
                    .arg(channel)
                    .arg(build_payload(payload_bytes))
                    .ignore();
                pending += 1;
                if pending == 256 {
                    let _: () = pipe
                        .query_async(&mut connection)
                        .await
                        .context("e2e redis publish pipeline")?;
                    pipe = redis::pipe();
                    pending = 0;
                }
                pace(&mut next_deadline, interval).await;
            }
            if pending > 0 {
                let _: () = pipe
                    .query_async(&mut connection)
                    .await
                    .context("e2e redis publish pipeline")?;
            }
        }
        other => anyhow::bail!("engine must be nats or redis, got {other:?}"),
    }
    let elapsed = started.elapsed();
    println!(
        "role=e2e-publish engine={engine} prefix={input_prefix} total={total} \
         filter_denied_expected={denied} expected_forwarded={} seconds={:.3} \
         publish_rate_msg_s={:.0}",
        total - denied,
        elapsed.as_secs_f64(),
        total as f64 / elapsed.as_secs_f64().max(f64::MIN_POSITIVE)
    );
    Ok(())
}

/// Chooses the input address for one message: a `drop` token when this index is
/// subject to the configured filter test, otherwise a `keep` token.
fn input_address(prefix: &str, index: u64, deny_every: u64, denied: &mut u64) -> String {
    if deny_every > 0 && index.is_multiple_of(deny_every) {
        *denied += 1;
        format!("{prefix}.drop.{index}")
    } else {
        format!("{prefix}.keep.{index}")
    }
}

/// Forces a real client/server round trip after publishing.
///
/// `async-nats` `flush()` resolves as soon as the client's local write buffer is
/// empty and does **not** wait for the server to process the commands. A short
/// request/reply against the server is a genuine barrier: by the time the reply
/// (or the "no responders" reply) arrives, every command queued before it has
/// been handled, so the publisher can exit without dropping the tail of the
/// stream. The barrier subject is deliberately outside every relay input
/// namespace (`_barrier.<prefix>`), so it is never forwarded.
async fn nats_barrier(client: &Client, input_prefix: &str) -> Result<()> {
    let subject = format!("_barrier.{input_prefix}");
    match tokio::time::timeout(
        Duration::from_secs(10),
        client.request(subject, Bytes::new()),
    )
    .await
    {
        // Either a real reply or the server's immediate "no responders"
        // response is a completed round trip; both settle the earlier commands.
        Ok(_) => Ok(()),
        Err(_) => Err(anyhow::anyhow!(
            "NATS barrier round trip timed out; server did not confirm the tail of the publish stream"
        )),
    }
}

/// Verifier engine list: `COMPARE_VERIFY_ENGINES`, falling back to
/// `COMPARE_ENGINE` for single-output runs.
fn verify_engines() -> String {
    std::env::var("COMPARE_VERIFY_ENGINES")
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| env_or("COMPARE_ENGINE", "nats"))
}

/// Subscribes to the relay's output subject space and reports how many messages
/// arrived and the end-to-end age (publish -> observed here). The input prefix
/// is unused for verification because the output space is selected separately by
/// `COMPARE_VERIFY_PREFIX`.
async fn e2e_verify(_input_prefix: &str, expected: u64) -> Result<()> {
    let engines = verify_engines();
    e2e_verify_engines(&engines, expected).await
}

/// Verifies the relay's **output** address space. The verifier runs one
/// independent consume task per requested output, so a fan-out run with one
/// Redis and one NATS output is measured from both backends at once:
///
/// * `COMPARE_VERIFY_ENGINES` is a comma list (`redis`, `nats`, or
///   `redis,nats`).
/// * `COMPARE_VERIFY_PREFIX` is a comma list of output subject prefixes, one per
///   engine. A single value applies to every engine.
///
/// Each engine prints its own `role=e2e-verify engine=<engine> ...` line, then
/// the process joins all tasks. The ready file is written only after every
/// consumer is subscribed, so the publisher never races subscription setup.
async fn e2e_verify_engines(engines: &str, expected: u64) -> Result<()> {
    let engine_list = engines
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    anyhow::ensure!(!engine_list.is_empty(), "no verify engines selected");
    let prefixes = env_or("COMPARE_VERIFY_PREFIX", &format!("{expected}.out"))
        .split(',')
        .map(str::trim)
        .map(str::to_owned)
        .collect::<Vec<_>>();

    // Wait until every consumer has signalled readiness before the driver starts
    // the publisher.
    let (ready_tx, mut ready_rx) = tokio::sync::mpsc::channel::<()>(engine_list.len());
    let mut tasks = Vec::with_capacity(engine_list.len());
    for (index, engine) in engine_list.iter().enumerate() {
        let prefix = prefixes
            .get(index)
            .or_else(|| prefixes.first())
            .cloned()
            .unwrap_or_default();
        let engine = engine.clone();
        let ready = ready_tx.clone();
        tasks.push(tokio::spawn(async move {
            verify_engine(&engine, &prefix, expected, ready).await
        }));
    }
    drop(ready_tx);
    // A consumer signals readiness after it has subscribed; wait for all of them.
    for _ in 0..engine_list.len() {
        if ready_rx.recv().await.is_none() {
            break;
        }
    }
    signal_ready();
    for task in tasks {
        task.await.context("verify task join")??;
    }
    Ok(())
}

/// Consumes one output address space and reports counts and end-to-end age.
async fn verify_engine(
    engine: &str,
    prefix: &str,
    expected: u64,
    ready: tokio::sync::mpsc::Sender<()>,
) -> Result<()> {
    let started = Instant::now();
    let mut latencies: Vec<u64> = Vec::with_capacity(expected as usize);
    let mut received = 0u64;
    let consume = async {
        match engine {
            "nats" => {
                let client = connect_nats().await?;
                let mut subscription = client
                    .subscribe(format!("{prefix}.>"))
                    .await
                    .context("e2e verify nats subscribe")?;
                client.flush().await.ok();
                ready.send(()).await.ok();
                while received < expected {
                    match subscription.next().await {
                        Some(message) => {
                            latencies.push(age(&message.payload));
                            received += 1;
                        }
                        None => break,
                    }
                }
            }
            "redis" => {
                let client = redis::Client::open(redis_verify_url())?;
                let mut pubsub = client.get_async_pubsub().await?;
                pubsub
                    .psubscribe(format!("{prefix}.*"))
                    .await
                    .context("e2e verify redis psubscribe")?;
                ready.send(()).await.ok();
                let mut messages = pubsub.on_message();
                while received < expected {
                    match messages.next().await {
                        Some(message) => {
                            latencies.push(age(message.get_payload_bytes()));
                            received += 1;
                        }
                        None => break,
                    }
                }
            }
            other => anyhow::bail!("engine must be nats or redis, got {other:?}"),
        }
        Ok::<(), anyhow::Error>(())
    };
    let outcome = tokio::time::timeout(wait_timeout(), consume).await;
    let timed_out = outcome.is_err();
    if let Ok(Err(error)) = outcome {
        return Err(error);
    }
    let elapsed = started.elapsed();
    latencies.sort_unstable();
    println!(
        "role=e2e-verify engine={engine} prefix={prefix} received={received} expected={expected} \
         complete={} timed_out={timed_out} seconds={:.3} rate_msg_s={:.0}",
        received == expected,
        elapsed.as_secs_f64(),
        received as f64 / elapsed.as_secs_f64().max(f64::MIN_POSITIVE)
    );
    if !latencies.is_empty() {
        println!(
            "e2e_age_ms engine={engine} p50={:.3} p95={:.3} p99={:.3} max={:.3}",
            percentile_ms(&latencies, 50.0),
            percentile_ms(&latencies, 95.0),
            percentile_ms(&latencies, 99.0),
            latencies.last().copied().unwrap_or(0) as f64 / 1_000_000.0
        );
    }
    Ok(())
}

async fn publish(
    engine: &str,
    prefix: &str,
    total: u64,
    payload_bytes: usize,
    batch: usize,
    rate: u64,
) -> Result<()> {
    // Wait for the subscriber to signal readiness so no message is missed at startup.
    if let Ok(path) = std::env::var("COMPARE_READY_FILE") {
        let deadline = Instant::now() + Duration::from_secs(30);
        while !std::path::Path::new(&path).exists() {
            if Instant::now() > deadline {
                anyhow::bail!("subscriber did not signal readiness");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    let started = Instant::now();
    let interval = (rate > 0).then(|| Duration::from_secs_f64(1.0 / rate as f64));
    let mut next_deadline = started;

    match engine {
        "nats" => {
            let client = connect_nats().await?;
            let mut since = 0usize;
            for index in 0..total {
                let subject = format!("{prefix}.{index}");
                client
                    .publish(subject, Bytes::from(build_payload(payload_bytes)))
                    .await
                    .context("nats publish")?;
                since += 1;
                if since == batch {
                    client.flush().await.context("nats flush")?;
                    since = 0;
                }
                pace(&mut next_deadline, interval).await;
            }
            if since > 0 {
                client.flush().await.context("nats flush")?;
            }
            client.flush().await.context("nats final flush")?;
        }
        "redis" => {
            let url = env_or("REDIS_URL", "redis://127.0.0.1:16390");
            let client = redis::Client::open(url)?;
            let mut connection = client.get_multiplexed_async_connection().await?;
            let mut pipe = redis::pipe();
            let mut pending = 0usize;
            for index in 0..total {
                let channel = format!("{prefix}.{index}");
                pipe.cmd("PUBLISH")
                    .arg(channel)
                    .arg(build_payload(payload_bytes))
                    .ignore();
                pending += 1;
                if pending == batch {
                    let _: () = pipe
                        .query_async(&mut connection)
                        .await
                        .context("redis pipeline")?;
                    pipe = redis::pipe();
                    pending = 0;
                }
                pace(&mut next_deadline, interval).await;
            }
            if pending > 0 {
                let _: () = pipe
                    .query_async(&mut connection)
                    .await
                    .context("redis pipeline")?;
            }
        }
        other => anyhow::bail!("engine must be nats or redis, got {other:?}"),
    }

    let elapsed = started.elapsed();
    println!(
        "role=publisher engine={engine} total={total} payload_bytes={payload_bytes} \
         batch={batch} target_rate={rate} seconds={:.3} publish_rate_msg_s={:.0}",
        elapsed.as_secs_f64(),
        total as f64 / elapsed.as_secs_f64().max(f64::MIN_POSITIVE)
    );
    Ok(())
}

/// Absolute-deadline pacing: schedules against the run start, so drift does not
/// accumulate and the achieved rate stays close to the target regardless of the
/// host timer resolution.
async fn pace(next_deadline: &mut Instant, interval: Option<Duration>) {
    let Some(interval) = interval else {
        return;
    };
    *next_deadline += interval;
    let now = Instant::now();
    if *next_deadline > now {
        tokio::time::sleep_until(*next_deadline).await;
    }
}

async fn subscribe(engine: &str, prefix: &str, total: u64) -> Result<()> {
    // Preallocated, single task: no per-message lock, no Vec regrowth, no per-message timer.
    let mut latencies: Vec<u64> = Vec::with_capacity(total as usize);
    let mut received = 0u64;
    let started = Instant::now();

    // A single long timeout around the whole consume loop, not one timer per message.
    let consume = async {
        match engine {
            "nats" => {
                let client = connect_nats().await?;
                let mut subscription = client
                    .subscribe(format!("{prefix}.>"))
                    .await
                    .context("nats subscribe")?;
                client.flush().await.ok();
                signal_ready();
                while received < total {
                    match subscription.next().await {
                        Some(message) => {
                            latencies.push(age(&message.payload));
                            received += 1;
                        }
                        None => break,
                    }
                }
            }
            "redis" => {
                let url = env_or("REDIS_URL", "redis://127.0.0.1:16390");
                let client = redis::Client::open(url)?;
                let mut pubsub = client.get_async_pubsub().await?;
                pubsub
                    .psubscribe(format!("{prefix}.*"))
                    .await
                    .context("redis psubscribe")?;
                signal_ready();
                let mut messages = pubsub.on_message();
                while received < total {
                    match messages.next().await {
                        Some(message) => {
                            latencies.push(age(message.get_payload_bytes()));
                            received += 1;
                        }
                        None => break,
                    }
                }
            }
            other => anyhow::bail!("engine must be nats or redis, got {other:?}"),
        }
        Ok::<(), anyhow::Error>(())
    };

    let outcome = tokio::time::timeout(wait_timeout(), consume).await;
    let timed_out = outcome.is_err();
    if let Ok(Err(error)) = outcome {
        return Err(error);
    }

    let elapsed = started.elapsed();
    latencies.sort_unstable();
    println!(
        "role=subscriber engine={engine} received={received} expected={total} \
         complete={} timed_out={timed_out} seconds={:.3} e2e_rate_msg_s={:.0}",
        received == total,
        elapsed.as_secs_f64(),
        received as f64 / elapsed.as_secs_f64().max(f64::MIN_POSITIVE)
    );
    if !latencies.is_empty() {
        println!(
            "age_ms p50={:.3} p95={:.3} p99={:.3} max={:.3}",
            percentile_ms(&latencies, 50.0),
            percentile_ms(&latencies, 95.0),
            percentile_ms(&latencies, 99.0),
            latencies.last().copied().unwrap_or(0) as f64 / 1_000_000.0
        );
    }
    Ok(())
}

fn signal_ready() {
    if let Ok(path) = std::env::var("COMPARE_READY_FILE") {
        let _ = std::fs::write(path, b"ready");
    }
}

/// Blocks until `COMPARE_READY_FILE` exists, so the publisher starts only after
/// the verifier has subscribed. No-op when the variable is unset.
async fn wait_for_ready() -> Result<()> {
    let Ok(path) = std::env::var("COMPARE_READY_FILE") else {
        return Ok(());
    };
    let deadline = Instant::now() + Duration::from_secs(60);
    while !std::path::Path::new(&path).exists() {
        if Instant::now() > deadline {
            anyhow::bail!("verifier did not signal readiness");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Ok(())
}

async fn connect_nats() -> Result<Client> {
    let url = env_or("NATS_URL", "nats://127.0.0.1:14230");
    let addresses = url
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ServerAddr::from_str)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let mut options = ConnectOptions::new()
        .name("transport-compare")
        .max_reconnects(None)
        .subscription_capacity(1_048_576);
    if let Ok(root) = std::env::var("NATS_ROOT_CERT_FILE")
        && !root.is_empty()
    {
        options = options.add_root_certificates(root.into());
    }
    if let Ok(token) = std::env::var("NATS_TOKEN")
        && !token.is_empty()
    {
        options = options.token(token);
    }
    options
        .connect(addresses)
        .await
        .context("connecting to NATS")
}

fn build_payload(payload_bytes: usize) -> Vec<u8> {
    let mut bytes = vec![b'x'; payload_bytes];
    bytes[..8].copy_from_slice(&nanos().to_be_bytes());
    bytes
}

fn age(payload: &[u8]) -> u64 {
    if payload.len() >= 8
        && let Ok(bytes) = <[u8; 8]>::try_from(&payload[0..8])
    {
        return nanos().saturating_sub(u64::from_be_bytes(bytes));
    }
    0
}

fn percentile_ms(sorted: &[u64], percentile: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let rank = ((percentile / 100.0) * sorted.len() as f64).ceil() as usize;
    let index = rank.saturating_sub(1).min(sorted.len() - 1);
    sorted[index] as f64 / 1_000_000.0
}

fn nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_owned())
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}
