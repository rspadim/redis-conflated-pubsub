use std::{str::FromStr, time::Duration};

use anyhow::{Context, Result};
use async_nats::{Client, ConnectOptions, ServerAddr};
use bytes::Bytes;
use futures_util::StreamExt;
use tokio::time::Instant;

const WAIT_TIMEOUT: Duration = Duration::from_secs(300);

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
        "e2e-publish" => e2e_publish(&prefix, total, payload_bytes, rate).await,
        "e2e-verify" => e2e_verify(&prefix, total).await,
        other => anyhow::bail!("role must be publisher or subscriber, got {other:?}"),
    }
}

/// Publishes `total` messages to `{prefix}.{i}` on NATS and waits for every
/// message to be acknowledged by the server (flush) before returning. Used to
/// drive the relay's NATS input.
async fn e2e_publish(prefix: &str, total: u64, payload_bytes: usize, rate: u64) -> Result<()> {
    let client = connect_nats().await?;
    let started = Instant::now();
    let interval = (rate > 0).then(|| Duration::from_secs_f64(1.0 / rate as f64));
    let mut next_deadline = started;
    let mut since = 0usize;
    for index in 0..total {
        let subject = format!("{prefix}.{index}");
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
    client.flush().await.context("e2e nats final flush")?;
    let elapsed = started.elapsed();
    println!(
        "role=e2e-publish total={total} seconds={:.3} rate_msg_s={:.0}",
        elapsed.as_secs_f64(),
        total as f64 / elapsed.as_secs_f64().max(f64::MIN_POSITIVE)
    );
    Ok(())
}

/// Subscribes to the relay's output subject space `{prefix}.>` and reports how
/// many messages arrived and the end-to-end age (publish -> observed here).
async fn e2e_verify(prefix: &str, expected: u64) -> Result<()> {
    let client = connect_nats().await?;
    let mut subscription = client
        .subscribe(format!("{prefix}.>"))
        .await
        .context("e2e verify subscribe")?;
    client.flush().await.ok();
    signal_ready();

    let mut latencies: Vec<u64> = Vec::with_capacity(expected as usize);
    let mut received = 0u64;
    let started = Instant::now();
    let consume = async {
        while received < expected {
            match subscription.next().await {
                Some(message) => {
                    latencies.push(age(&message.payload));
                    received += 1;
                }
                None => break,
            }
        }
    };
    let timed_out = tokio::time::timeout(WAIT_TIMEOUT, consume).await.is_err();
    let elapsed = started.elapsed();
    latencies.sort_unstable();
    println!(
        "role=e2e-verify received={received} expected={expected} complete={} timed_out={timed_out} \
         seconds={:.3} rate_msg_s={:.0}",
        received == expected,
        elapsed.as_secs_f64(),
        received as f64 / elapsed.as_secs_f64().max(f64::MIN_POSITIVE)
    );
    if !latencies.is_empty() {
        println!(
            "e2e_age_ms p50={:.3} p95={:.3} p99={:.3} max={:.3}",
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

    let outcome = tokio::time::timeout(WAIT_TIMEOUT, consume).await;
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
