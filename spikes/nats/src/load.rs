use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use async_nats::Client;
use bytes::Bytes;
use futures_util::StreamExt;
use tokio::{sync::Notify, time};

/// NATS transport load harness. Not the relay: it measures the client path
/// (publish/flush and subscribe fan-in) so the design can record plateau
/// numbers before the adapters exist.
pub(crate) async fn run(client: &Client) -> Result<()> {
    let total = env_u64("NATS_LOAD_TOTAL", 200_000).max(1);
    let publishers = env_usize("NATS_LOAD_PUBLISHERS", 4).max(1) as u64;
    let payload_bytes = env_usize("NATS_LOAD_PAYLOAD_BYTES", 256).max(8);
    let flush_every = env_usize("NATS_LOAD_FLUSH_EVERY", 64).max(1);
    let prefix = env_or("NATS_LOAD_PREFIX", "load");
    let mode = env_or("NATS_LOAD_MODE", "roundtrip");
    let with_subscriber = mode != "publish";

    let received = Arc::new(AtomicU64::new(0));
    let latencies = Arc::new(Mutex::new(Vec::<u64>::new()));
    let done = Arc::new(Notify::new());

    let subscriber = if with_subscriber {
        let mut subscription = client.subscribe(format!("{prefix}.>")).await?;
        let received = Arc::clone(&received);
        let latencies = Arc::clone(&latencies);
        let done = Arc::clone(&done);
        Some(tokio::spawn(async move {
            while received.load(Ordering::Relaxed) < total {
                let Some(message) = subscription.next().await else {
                    break;
                };
                if message.payload.len() >= 8
                    && let Ok(bytes) = <[u8; 8]>::try_from(&message.payload[0..8])
                {
                    let sent = u64::from_be_bytes(bytes);
                    latencies
                        .lock()
                        .expect("latency buffer lock")
                        .push(now_nanos().saturating_sub(sent));
                }
                received.fetch_add(1, Ordering::Relaxed);
            }
            done.notify_waiters();
        }))
    } else {
        None
    };

    let started = Instant::now();
    let mut tasks = Vec::with_capacity(publishers as usize);
    for publisher in 0..publishers {
        let client = client.clone();
        let prefix = prefix.clone();
        let per_publisher = total / publishers + u64::from(publisher < total % publishers);
        tasks.push(tokio::spawn(async move {
            let mut since_flush = 0usize;
            for index in 0..per_publisher {
                let subject = format!("{prefix}.p{publisher}.{index}");
                let payload = build_payload(payload_bytes);
                client
                    .publish(subject, payload)
                    .await
                    .expect("load publish");
                since_flush += 1;
                if since_flush == flush_every {
                    client.flush().await.expect("load flush");
                    since_flush = 0;
                }
            }
            if since_flush > 0 {
                client.flush().await.expect("load flush");
            }
        }));
    }
    for task in tasks {
        task.await?;
    }
    let published_at = started.elapsed();

    let delivery = if let Some(subscriber) = subscriber {
        let waited = time::timeout(Duration::from_secs(120), async {
            loop {
                if received.load(Ordering::Relaxed) >= total {
                    break;
                }
                let notified = done.notified();
                if received.load(Ordering::Relaxed) >= total {
                    break;
                }
                let _ = time::timeout(Duration::from_millis(200), notified).await;
            }
        })
        .await;
        subscriber.abort();
        let _ = subscriber.await;
        waited.is_ok()
    } else {
        true
    };
    let elapsed = started.elapsed();
    let delivered = received.load(Ordering::Relaxed);

    let mut samples = std::mem::take(&mut *latencies.lock().expect("latency buffer lock"));
    samples.sort_unstable();

    let seconds = elapsed.as_secs_f64().max(f64::MIN_POSITIVE);
    println!(
        "load_mode={mode} publishers={publishers} total={total} payload_bytes={payload_bytes} \
         flush_every={flush_every}"
    );
    println!(
        "load_publish_seconds={:.3} load_publish_rate_msg_s={:.0}",
        published_at.as_secs_f64(),
        total as f64 / published_at.as_secs_f64().max(f64::MIN_POSITIVE)
    );
    if with_subscriber {
        println!(
            "load_delivered={delivered} load_delivery_complete={delivery} \
             load_end_to_end_seconds={:.3} load_rate_msg_s={:.0}",
            elapsed.as_secs_f64(),
            total as f64 / seconds
        );
        if !samples.is_empty() {
            println!(
                "load_rtt_ms p50={:.3} p95={:.3} p99={:.3} max={:.3}",
                percentile_ms(&samples, 50.0),
                percentile_ms(&samples, 95.0),
                percentile_ms(&samples, 99.0),
                samples.last().copied().unwrap_or(0) as f64 / 1_000_000.0
            );
        }
    } else {
        println!(
            "load_flush_seconds={:.3} load_publish_rate_msg_s={:.0}",
            elapsed.as_secs_f64(),
            total as f64 / seconds
        );
    }
    Ok(())
}

fn build_payload(payload_bytes: usize) -> Bytes {
    let mut bytes = vec![b'x'; payload_bytes];
    bytes[..8].copy_from_slice(&now_nanos().to_be_bytes());
    Bytes::from(bytes)
}

fn percentile_ms(sorted: &[u64], percentile: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let rank = ((percentile / 100.0) * sorted.len() as f64).ceil() as usize;
    let index = rank.saturating_sub(1).min(sorted.len() - 1);
    sorted[index] as f64 / 1_000_000.0
}

fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock before Unix epoch")
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
