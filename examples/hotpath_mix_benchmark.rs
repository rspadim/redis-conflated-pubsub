//! Mixed-policy hotpath benchmark for `compose.hotpath.test.yml`.
//!
//! Rust replacement for the former `tests/docker_hotpath_policy_mix.py`; the
//! workload is unchanged (about one-third direct, one-third 200 ms conflation
//! and one-third 200 ms conflation plus a 5 s deduplication TTL with one
//! repeated payload per channel), but publishers and both output subscribers
//! run in this one process, so unbuffered RESP reads under the Python GIL can
//! no longer distort the measurement. Channel names, warm-up, cohorts, latency
//! sampling and printed lines mirror the Python driver; outputs match
//! `tests/docker-hotpath-mix-config.json`.

use std::{
    collections::{BTreeMap, HashMap},
    env,
    io::{Read, Write},
    net::TcpStream,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result, ensure};
use futures_util::StreamExt;
use serde::Deserialize;

const OUTPUT_NAMES: [&str; 2] = ["out-a", "out-b"];
const OUTPUT_PREFIXES: [&str; 2] = ["replica-a:", "replica-b:"];
const OUTPUT_PATTERNS: [&str; 2] = ["replica-a:*", "replica-b:*"];
const COHORTS: [&str; 3] = ["direct", "conflate", "ttl"];
const CHANNELS_PER_COHORT: usize = 16;
const TTL_COHORT: usize = 2;

type EventStarts = Arc<Mutex<HashMap<Vec<u8>, u64>>>;

fn env_parse<T: std::str::FromStr>(name: &str, default: T) -> T {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn source_channel(cohort: &str, channel_index: usize) -> String {
    format!("mix:{cohort}:{channel_index:02}")
}

fn output_channel(output_index: usize, cohort: &str, channel_index: usize) -> String {
    format!(
        "{}mapped:mix:{cohort}:{channel_index:02}:source:dest",
        OUTPUT_PREFIXES[output_index]
    )
}

#[derive(Debug, Deserialize)]
struct Snapshot {
    input_messages_total: u64,
    outputs: BTreeMap<String, OutputSnapshot>,
}

#[derive(Debug, Deserialize)]
struct OutputSnapshot {
    input_messages_total: u64,
    output_messages_total: u64,
    pending_messages: u64,
    conflated_messages_total: u64,
    deduplicated_messages_total: u64,
}

#[derive(Debug, Deserialize)]
struct FiltersSnapshot {
    caches: BTreeMap<String, CacheSnapshot>,
}

#[derive(Debug, Deserialize)]
struct CacheSnapshot {
    capacity: u64,
    entry_count: u64,
    hits: u64,
    misses: u64,
    evictions: u64,
}

fn fetch_path(host: &str, port: u16, path: &str) -> Result<String> {
    let mut stream = TcpStream::connect((host, port)).context("connect to status endpoint")?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: hotpath\r\nConnection: close\r\n\r\n"
    )?;
    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    let status_line = response.lines().next().unwrap_or_default().to_owned();
    ensure!(
        status_line.contains(" 200 "),
        "status endpoint answered {status_line}"
    );
    let body = response
        .split("\r\n\r\n")
        .nth(1)
        .context("status response had no body")?;
    Ok(body.to_owned())
}

async fn fetch_json<T>(host: String, port: u16, path: &'static str) -> Result<T>
where
    T: serde::de::DeserializeOwned + Send + 'static,
{
    let body = tokio::task::spawn_blocking(move || fetch_path(&host, port, path)).await??;
    serde_json::from_str(&body).context("response was not valid JSON")
}

async fn wait_for_input_subscription(host: &str) -> Result<()> {
    let client = redis::Client::open(format!("redis://{host}:6379/0"))?;
    let mut connection = client.get_multiplexed_async_connection().await?;
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut observed = 0i64;
    while Instant::now() < deadline {
        observed = redis::cmd("PUBSUB")
            .arg("NUMPAT")
            .query_async(&mut connection)
            .await?;
        if observed == 1 {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    anyhow::bail!("Bridge input pattern did not become active; NUMPAT={observed}")
}

async fn publish(
    connection: &mut redis::aio::MultiplexedConnection,
    channel: &str,
    payload: &[u8],
) -> Result<()> {
    let subscribers: i64 = redis::cmd("PUBLISH")
        .arg(channel)
        .arg(payload)
        .query_async(connection)
        .await
        .context("input PUBLISH failed")?;
    ensure!(
        subscribers >= 1,
        "input PUBLISH to {channel} reached no subscribers"
    );
    Ok(())
}

fn percentile_ms(sorted: &[u64], percent: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let index = ((percent * sorted.len() as f64).ceil() as usize).saturating_sub(1);
    sorted[index] as f64 / 1_000_000.0
}

struct Received {
    count: AtomicU64,
    cohorts: [AtomicU64; COHORTS.len()],
    latencies: Mutex<[Vec<u64>; COHORTS.len()]>,
}

impl Received {
    fn new() -> Self {
        Self {
            count: AtomicU64::new(0),
            cohorts: [AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)],
            latencies: Mutex::new([Vec::new(), Vec::new(), Vec::new()]),
        }
    }
}

/// Parses `{prefix}mapped:mix:{cohort}:{index:02}:source:dest` and returns the
/// cohort index and channel index after validating the shape.
fn parse_output_channel(output_index: usize, channel: &str) -> Result<(usize, usize)> {
    let parts = channel.split(':').collect::<Vec<_>>();
    ensure!(
        parts.len() == 7 && parts[1] == "mapped" && parts[2] == "mix",
        "unexpected output channel {channel}"
    );
    let cohort_index = COHORTS
        .iter()
        .position(|candidate| *candidate == parts[3])
        .with_context(|| format!("unknown cohort in channel {channel}"))?;
    let channel_index: usize = parts[4]
        .parse()
        .with_context(|| format!("channel index was not a number: {channel}"))?;
    ensure!(
        channel_index < CHANNELS_PER_COHORT,
        "channel index out of range: {channel}"
    );
    ensure!(
        output_channel(output_index, COHORTS[cohort_index], channel_index) == channel,
        "unexpected output channel {channel}"
    );
    Ok((cohort_index, channel_index))
}

fn validate_pattern(message: &redis::Msg, pattern: &str) -> Result<()> {
    let observed: Option<String> = message.get_pattern()?;
    ensure!(
        observed.as_deref() == Some(pattern),
        "unexpected pattern {observed:?} (wanted {pattern})"
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn subscriber_task(
    output_host: String,
    output_index: usize,
    received: Arc<Received>,
    starts: EventStarts,
    base: Instant,
    ready: Arc<tokio::sync::Barrier>,
    warm_done: Arc<tokio::sync::Barrier>,
    stop: Arc<AtomicBool>,
) -> Result<()> {
    let client = redis::Client::open(format!("redis://{output_host}:6379/{output_index}"))?;
    let mut pubsub = client.get_async_pubsub().await?;
    let pattern = OUTPUT_PATTERNS[output_index];
    pubsub.psubscribe(pattern).await?;
    let mut stream = pubsub.on_message();
    ready.wait().await;

    // Warm-up: one unique payload per cohort/channel, delivered exactly once.
    let warm_messages = COHORTS.len() * CHANNELS_PER_COHORT;
    let warm_deadline = Instant::now() + Duration::from_secs(10);
    for _ in 0..warm_messages {
        let remaining = warm_deadline.saturating_duration_since(Instant::now());
        let message = tokio::time::timeout(remaining, stream.next())
            .await
            .context("timed out warming output channels")?
            .context("subscriber stream ended during warm-up")?;
        validate_pattern(&message, pattern)?;
        let channel = message.get_channel_name().to_owned();
        let (cohort_index, channel_index) = parse_output_channel(output_index, &channel)?;
        let expected = format!("warm:{}:{channel_index:02}", COHORTS[cohort_index]);
        ensure!(
            message.get_payload_bytes() == expected.as_bytes(),
            "unexpected warm payload {:?} (wanted {expected:?})",
            message.get_payload_bytes()
        );
    }
    received.count.store(warm_messages as u64, Ordering::SeqCst);
    warm_done.wait().await;

    loop {
        if stop.load(Ordering::Relaxed) {
            break;
        }
        match tokio::time::timeout(Duration::from_millis(100), stream.next()).await {
            Ok(Some(message)) => {
                validate_pattern(&message, pattern)?;
                let channel = message.get_channel_name().to_owned();
                let (cohort_index, _) = parse_output_channel(output_index, &channel)?;
                let payload = message.get_payload_bytes();
                let received_at = base.elapsed().as_nanos() as u64;
                received.count.fetch_add(1, Ordering::SeqCst);
                received.cohorts[cohort_index].fetch_add(1, Ordering::SeqCst);
                if cohort_index != TTL_COHORT {
                    let started = starts.lock().unwrap().get(payload).copied();
                    if let Some(started) = started {
                        received.latencies.lock().unwrap()[cohort_index]
                            .push(received_at.saturating_sub(started));
                    }
                }
            }
            Ok(None) => anyhow::bail!("subscriber stream ended"),
            Err(_) => {}
        }
    }
    Ok(())
}

async fn publisher_task(
    raw_host: String,
    publisher_id: usize,
    messages_per_publisher: usize,
    starts: EventStarts,
    base: Instant,
    barrier: Arc<tokio::sync::Barrier>,
) -> Result<()> {
    let client = redis::Client::open(format!("redis://{raw_host}:6379/0"))?;
    let mut connection = client.get_multiplexed_async_connection().await?;
    // The client defaults to a 500 ms response timeout; keep a generous bound.
    connection.set_response_timeout(Duration::from_secs(30));
    barrier.wait().await;
    for sequence in 0..messages_per_publisher {
        let event_index = publisher_id * messages_per_publisher + sequence;
        let cohort_index = event_index % COHORTS.len();
        let channel_index = (event_index / COHORTS.len()) % CHANNELS_PER_COHORT;
        let cohort = COHORTS[cohort_index];
        let channel = source_channel(cohort, channel_index);
        let payload = if cohort_index == TTL_COHORT {
            // Identical payload per channel: a positive TTL suppresses repeats.
            format!("ttl:{channel_index:02}").into_bytes()
        } else {
            format!("{cohort}:{publisher_id}:{sequence}").into_bytes()
        };
        if cohort_index != TTL_COHORT {
            starts
                .lock()
                .unwrap()
                .insert(payload.clone(), base.elapsed().as_nanos() as u64);
        }
        publish(&mut connection, &channel, &payload).await?;
    }
    Ok(())
}

async fn wait_for_drain(
    status_host: &str,
    status_port: u16,
    expected_input: u64,
    received: &[Arc<Received>; 2],
) -> Result<Snapshot> {
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut latest = None;
    while Instant::now() < deadline {
        if let Ok(snapshot) = fetch_json::<Snapshot>(status_host.to_owned(), status_port, "/").await
        {
            let outputs_drained = OUTPUT_NAMES.iter().enumerate().all(|(index, name)| {
                snapshot.outputs.get(*name).is_some_and(|output| {
                    output.input_messages_total == expected_input
                        && output.pending_messages == 0
                        && output.output_messages_total
                            == received[index].count.load(Ordering::SeqCst)
                })
            });
            if snapshot.input_messages_total == expected_input && outputs_drained {
                return Ok(snapshot);
            }
            latest = Some(snapshot);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    match latest {
        Some(snapshot) => {
            let summaries = OUTPUT_NAMES
                .iter()
                .enumerate()
                .map(|(index, name)| {
                    snapshot.outputs.get(*name).map_or_else(
                        || format!("{name}=missing"),
                        |output| {
                            format!(
                                "{name}=input:{}/published:{}/pending:{}/subscriber:{}",
                                output.input_messages_total,
                                output.output_messages_total,
                                output.pending_messages,
                                received[index].count.load(Ordering::SeqCst)
                            )
                        },
                    )
                })
                .collect::<Vec<_>>()
                .join(" ");
            anyhow::bail!(
                "output workers did not drain mixed policies: input={} {summaries}",
                snapshot.input_messages_total
            )
        }
        None => anyhow::bail!("status endpoint did not answer"),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let publisher_count: usize = env_parse("PUBLISHER_COUNT", 4);
    let messages_per_publisher: usize = env_parse("MESSAGES_PER_PUBLISHER", 2500);
    let raw_host = env::var("REDIS_HOST").unwrap_or_else(|_| "redis-raw".to_owned());
    let output_host = env::var("OUTPUT_REDIS_HOST").unwrap_or_else(|_| "redis-output".to_owned());
    let output_host_b = env::var("OUTPUT_REDIS_HOST_2").unwrap_or_else(|_| output_host.clone());
    let status_url =
        env::var("STATUS_URL").unwrap_or_else(|_| "http://service-hotpath:9090/".to_owned());
    let status_endpoint = status_url.trim_start_matches("http://");
    let (status_host, status_port) = status_endpoint
        .split_once(':')
        .context("STATUS_URL must look like http://host:port/")?;
    let status_host = status_host.to_owned();
    let status_port: u16 = status_port.trim_end_matches('/').parse()?;
    ensure!(publisher_count > 0, "PUBLISHER_COUNT must be positive");
    ensure!(
        messages_per_publisher > 0,
        "MESSAGES_PER_PUBLISHER must be positive"
    );

    let total_messages = publisher_count * messages_per_publisher;
    let warm_messages = COHORTS.len() * CHANNELS_PER_COHORT;
    let expected_input = (total_messages + warm_messages) as u64;

    wait_for_input_subscription(&raw_host).await?;

    let base = Instant::now();
    let starts: EventStarts = Arc::new(Mutex::new(HashMap::new()));
    let received = [Arc::new(Received::new()), Arc::new(Received::new())];
    let stop = Arc::new(AtomicBool::new(false));
    let ready = Arc::new(tokio::sync::Barrier::new(OUTPUT_NAMES.len() + 1));
    let warm_done = Arc::new(tokio::sync::Barrier::new(OUTPUT_NAMES.len() + 1));

    let mut subscribers = Vec::with_capacity(OUTPUT_NAMES.len());
    for (output_index, received) in received.iter().enumerate() {
        let host = if output_index == 0 {
            output_host.clone()
        } else {
            output_host_b.clone()
        };
        subscribers.push(tokio::spawn(subscriber_task(
            host,
            output_index,
            Arc::clone(received),
            Arc::clone(&starts),
            base,
            Arc::clone(&ready),
            Arc::clone(&warm_done),
            Arc::clone(&stop),
        )));
    }
    tokio::time::timeout(Duration::from_secs(30), ready.wait())
        .await
        .context("subscribers did not confirm PSUBSCRIBE in time")?;

    let raw_client = redis::Client::open(format!("redis://{raw_host}:6379/0"))?;
    let mut writer = raw_client.get_multiplexed_async_connection().await?;
    writer.set_response_timeout(Duration::from_secs(30));

    for cohort in COHORTS {
        for channel_index in 0..CHANNELS_PER_COHORT {
            let payload = format!("warm:{cohort}:{channel_index:02}");
            publish(
                &mut writer,
                &source_channel(cohort, channel_index),
                payload.as_bytes(),
            )
            .await?;
        }
    }
    tokio::time::timeout(Duration::from_secs(30), warm_done.wait())
        .await
        .context("warm-up did not complete in time")?;

    let barrier = Arc::new(tokio::sync::Barrier::new(publisher_count + 1));
    let mut publishers = Vec::with_capacity(publisher_count);
    for publisher_id in 0..publisher_count {
        publishers.push(tokio::spawn(publisher_task(
            raw_host.clone(),
            publisher_id,
            messages_per_publisher,
            Arc::clone(&starts),
            base,
            Arc::clone(&barrier),
        )));
    }

    let wall_started = Instant::now();
    barrier.wait().await;
    for publisher in publishers {
        let outcome = tokio::time::timeout(Duration::from_secs(60), publisher)
            .await
            .context("publisher task timed out")?;
        outcome??;
    }
    let publisher_phase = Instant::now();
    let status = wait_for_drain(&status_host, status_port, expected_input, &received).await?;
    let wall_finished = Instant::now();
    stop.store(true, Ordering::Relaxed);
    for subscriber in subscribers {
        let outcome = tokio::time::timeout(Duration::from_secs(5), subscriber)
            .await
            .context("subscriber task timed out")?;
        outcome??;
    }

    println!(
        "hotpath-policy-mix publishers={publisher_count} messages_per_publisher={messages_per_publisher} total_input={total_messages} direct~33% conflate_200ms~33% conflate_200ms_ttl5s~34% ttl_payload=repeated_per_channel publish_phase_s={:.3} output_drain_s={:.3} total_s={:.3}",
        publisher_phase.duration_since(wall_started).as_secs_f64(),
        wall_finished.duration_since(publisher_phase).as_secs_f64(),
        wall_finished.duration_since(wall_started).as_secs_f64()
    );

    for (output_index, name) in OUTPUT_NAMES.iter().enumerate() {
        let metrics = status
            .outputs
            .get(*name)
            .with_context(|| format!("status snapshot is missing {name}"))?;
        let delivered = received[output_index].count.load(Ordering::SeqCst) - warm_messages as u64;
        let cohorts = [
            received[output_index].cohorts[0].load(Ordering::SeqCst),
            received[output_index].cohorts[1].load(Ordering::SeqCst),
            received[output_index].cohorts[2].load(Ordering::SeqCst),
        ];
        let mut latencies = received[output_index].latencies.lock().unwrap().clone();
        latencies[0].sort_unstable();
        latencies[1].sort_unstable();
        println!(
            "output {name} delivered={delivered} by_cohort={{'direct': {}, 'conflate': {}, 'ttl': {}}} direct_p50_p95_ms={:.3}/{:.3} conflate_p50_p95_ms={:.3}/{:.3} conflated_total={} deduplicated_total={}",
            cohorts[0],
            cohorts[1],
            cohorts[2],
            percentile_ms(&latencies[0], 0.50),
            percentile_ms(&latencies[0], 0.95),
            percentile_ms(&latencies[1], 0.50),
            percentile_ms(&latencies[1], 0.95),
            metrics.conflated_messages_total,
            metrics.deduplicated_messages_total
        );
    }

    let filters: FiltersSnapshot = fetch_json(status_host, status_port, "/filters").await?;
    for (name, cache) in &filters.caches {
        println!(
            "cache {name} capacity={} entries={} hits={} misses={} evictions={}",
            cache.capacity, cache.entry_count, cache.hits, cache.misses, cache.evictions
        );
    }
    Ok(())
}
