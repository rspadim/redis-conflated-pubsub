//! End-to-end hotpath benchmark for `compose.hotpath.test.yml`.
//!
//! Publishers and both output subscribers run in this one Rust process, so the
//! earlier Python harness costs (unbuffered RESP reads under the GIL) can no
//! longer distort the measurement. The harness warms 64 channels, runs a serial
//! phase, then a concurrent phase and prints per-phase latency percentiles plus
//! the service's own queue/publish metrics from the status endpoint.
//!
//! `HOTPATH_MODE=closed-loop` (default) waits for each input PUBLISH
//! acknowledgement, measuring round-trip behaviour. `HOTPATH_MODE=open-loop`
//! sends non-atomic pipelines of `HOTPATH_PIPELINE` commands over
//! `HOTPATH_CONNECTIONS` connections per publisher without waiting for
//! individual acknowledgements, forcing input pressure to find the service's
//! saturation point; the achieved rate and queue growth are the signal.
//! `HOTPATH_OUTPUTS` selects one or two outputs (default 2).

use std::{
    collections::BTreeMap,
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

const HOT_CHANNEL_COUNT: usize = 64;
const OUTPUT_NAMES: [&str; 2] = ["out-a", "out-b"];

fn env_parse<T: std::str::FromStr>(name: &str, default: T) -> T {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn source_channel(channel_index: usize) -> String {
    format!(
        "hot:tenant-{:02}:item-{:04}",
        channel_index % 16,
        channel_index
    )
}

fn output_channel(output_index: usize, channel_index: usize) -> String {
    let prefix = ["replica-a:", "replica-b:"][output_index];
    format!(
        "{prefix}mapped:{}:source:dest",
        source_channel(channel_index)
    )
}

#[derive(Debug, Deserialize)]
struct Snapshot {
    input_messages_total: u64,
    outputs: BTreeMap<String, OutputSnapshot>,
}

#[derive(Clone, Debug, Deserialize)]
struct OutputSnapshot {
    output_messages_total: u64,
    output_batches_total: u64,
    pending_messages: u64,
    publish_errors_total: u64,
    queue_wait_samples: u64,
    queue_wait_total_ns: u64,
    queue_wait_max_ns: u64,
    publish_rtt_samples: u64,
    publish_rtt_total_ns: u64,
    publish_rtt_max_ns: u64,
}

fn fetch_status(host: &str, port: u16) -> Result<Snapshot> {
    let mut stream = TcpStream::connect((host, port)).context("connect to status endpoint")?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.write_all(b"GET / HTTP/1.1\r\nHost: hotpath\r\nConnection: close\r\n\r\n")?;
    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    let body = response
        .split("\r\n\r\n")
        .nth(1)
        .context("status response had no body")?;
    serde_json::from_str(body).context("status response was not valid JSON")
}

async fn fetch_status_async(host: String, port: u16) -> Result<Snapshot> {
    tokio::task::spawn_blocking(move || fetch_status(&host, port)).await?
}

fn percentile_ms(sorted: &[u64], percent: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let index = ((percent * sorted.len() as f64).ceil() as usize).saturating_sub(1);
    sorted[index] as f64 / 1_000_000.0
}

async fn wait_for_input_subscription(host: &str) -> Result<()> {
    let minimum: i64 = env_parse("HOTPATH_MIN_NUMPAT", 1);
    let client = redis::Client::open(format!("redis://{host}:6379/0"))?;
    let mut connection = client.get_multiplexed_async_connection().await?;
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut observed = 0i64;
    while Instant::now() < deadline {
        observed = redis::cmd("PUBSUB")
            .arg("NUMPAT")
            .query_async(&mut connection)
            .await?;
        if observed >= minimum {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    anyhow::bail!(
        "Bridge input pattern did not become active; NUMPAT={observed} (minimum {minimum})"
    )
}

async fn wait_for_status_totals(
    host: &str,
    port: u16,
    expected: u64,
    output_names: &[&str],
) -> Result<Snapshot> {
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut latest = None;
    while Instant::now() < deadline {
        if let Ok(snapshot) = fetch_status_async(host.to_owned(), port).await {
            if snapshot.input_messages_total == expected
                && output_names.iter().all(|name| {
                    snapshot.outputs.get(*name).is_some_and(|output| {
                        output.output_messages_total == expected
                            && output.pending_messages == 0
                            && output.publish_errors_total == 0
                    })
                })
            {
                return Ok(snapshot);
            }
            latest = Some(snapshot);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    match latest {
        Some(snapshot) => {
            let summaries = output_names
                .iter()
                .map(|name| {
                    snapshot.outputs.get(*name).map_or_else(
                        || format!("{name}=missing"),
                        |output| {
                            format!(
                                "{name}=published:{}/pending:{}/errors:{}",
                                output.output_messages_total,
                                output.pending_messages,
                                output.publish_errors_total
                            )
                        },
                    )
                })
                .collect::<Vec<_>>()
                .join(" ");
            anyhow::bail!(
                "output workers did not flush all messages: input={} {summaries}",
                snapshot.input_messages_total
            )
        }
        None => anyhow::bail!("status endpoint did not answer"),
    }
}

async fn publish(
    connection: &mut redis::aio::MultiplexedConnection,
    channel: &str,
    payload: &[u8],
) -> Result<i64> {
    let subscribers: i64 = redis::cmd("PUBLISH")
        .arg(channel)
        .arg(payload)
        .query_async(connection)
        .await
        .context("input PUBLISH failed")?;
    Ok(subscribers)
}

async fn expect_message<S>(stream: &mut S, channel: &str, payload: &[u8]) -> Result<()>
where
    S: futures_util::Stream<Item = redis::Msg> + Unpin,
{
    let message = stream.next().await.context("subscriber stream ended")?;
    ensure!(
        message.get_channel_name() == channel,
        "unexpected channel {} (wanted {channel})",
        message.get_channel_name()
    );
    ensure!(
        message.get_payload_bytes() == payload,
        "unexpected payload {:?} (wanted {payload:?})",
        message.get_payload_bytes()
    );
    Ok(())
}

async fn wait_for_barrier(barrier: &tokio::sync::Barrier) {
    barrier.wait().await;
}

async fn publisher_task_closed_loop(
    raw_host: String,
    publisher_id: usize,
    messages_per_publisher: usize,
    starts: Arc<Vec<AtomicU64>>,
    acks: Arc<Vec<AtomicU64>>,
    base: Instant,
    barrier: Arc<tokio::sync::Barrier>,
) -> Result<()> {
    let client = redis::Client::open(format!("redis://{raw_host}:6379/0"))?;
    let mut connection = client.get_multiplexed_async_connection().await?;
    // The client defaults to a 500 ms response timeout; a saturated pipeline
    // can exceed it, so keep a generous explicit bound.
    connection.set_response_timeout(Duration::from_secs(30));
    wait_for_barrier(&barrier).await;
    for sequence in 0..messages_per_publisher {
        let index = publisher_id * messages_per_publisher + sequence;
        starts[index].store(base.elapsed().as_nanos() as u64, Ordering::SeqCst);
        let payload = format!("{publisher_id}:{sequence}");
        let channel = source_channel(index % HOT_CHANNEL_COUNT);
        publish(&mut connection, &channel, payload.as_bytes()).await?;
        acks[index].store(base.elapsed().as_nanos() as u64, Ordering::SeqCst);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn publisher_task_open_loop(
    raw_host: String,
    publisher_id: usize,
    messages_per_publisher: usize,
    pipeline: usize,
    connections: usize,
    starts: Arc<Vec<AtomicU64>>,
    acks: Arc<Vec<AtomicU64>>,
    base: Instant,
    barrier: Arc<tokio::sync::Barrier>,
) -> Result<()> {
    wait_for_barrier(&barrier).await;
    let pipeline = pipeline.max(1);
    let connections = connections.max(1).min(messages_per_publisher);
    let stripe = messages_per_publisher.div_ceil(connections);
    let mut workers = Vec::with_capacity(connections);
    for connection_index in 0..connections {
        let stripe_start = (connection_index * stripe).min(messages_per_publisher);
        let stripe_end = ((connection_index + 1) * stripe).min(messages_per_publisher);
        if stripe_start >= stripe_end {
            continue;
        }
        let raw_host = raw_host.clone();
        let starts = Arc::clone(&starts);
        let acks = Arc::clone(&acks);
        workers.push(async move {
            let client = redis::Client::open(format!("redis://{raw_host}:6379/0"))?;
            let mut connection = client.get_multiplexed_async_connection().await?;
            connection.set_response_timeout(Duration::from_secs(30));
            let mut sequence = stripe_start;
            while sequence < stripe_end {
                let chunk_end = (sequence + pipeline).min(stripe_end);
                let mut pipe = redis::pipe();
                for current in sequence..chunk_end {
                    let index = publisher_id * messages_per_publisher + current;
                    starts[index].store(base.elapsed().as_nanos() as u64, Ordering::SeqCst);
                    pipe.cmd("PUBLISH")
                        .arg(source_channel(index % HOT_CHANNEL_COUNT))
                        .arg(format!("{publisher_id}:{current}"));
                }
                let _: Vec<i64> = pipe
                    .query_async(&mut connection)
                    .await
                    .context("input PUBLISH batch failed")?;
                let ack = base.elapsed().as_nanos() as u64;
                for current in sequence..chunk_end {
                    let index = publisher_id * messages_per_publisher + current;
                    acks[index].store(ack, Ordering::SeqCst);
                }
                sequence = chunk_end;
            }
            Ok::<(), anyhow::Error>(())
        });
    }
    for result in futures_util::future::join_all(workers).await {
        result?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn subscriber_task(
    output_host: String,
    output_index: usize,
    messages_per_publisher: usize,
    publisher_count: usize,
    recv: Arc<Vec<AtomicU64>>,
    base: Instant,
    ready: Arc<tokio::sync::Barrier>,
) -> Result<()> {
    let client = redis::Client::open(format!("redis://{output_host}:6379/{output_index}"))?;
    let mut pubsub = client.get_async_pubsub().await?;
    let pattern = ["replica-a:*", "replica-b:*"][output_index];
    pubsub.psubscribe(pattern).await?;
    // Confirm the subscription before publishers are released, otherwise the
    // first messages can be lost and the subscriber waits forever.
    ready.wait().await;
    let mut stream = pubsub.on_message();
    for _ in 0..(publisher_count * messages_per_publisher) {
        let message = stream.next().await.context("subscriber stream ended")?;
        let payload = std::str::from_utf8(message.get_payload_bytes())
            .context("payload was not valid UTF-8")?;
        let mut parts = payload.split(':');
        let publisher_id: usize = parts
            .next()
            .context("payload missing publisher id")?
            .parse()
            .context("publisher id was not a number")?;
        let sequence: usize = parts
            .next()
            .context("payload missing sequence")?
            .parse()
            .context("sequence was not a number")?;
        ensure!(parts.next().is_none(), "unexpected payload shape {payload}");
        ensure!(
            publisher_id < publisher_count && sequence < messages_per_publisher,
            "payload out of range: {payload}"
        );
        let index = publisher_id * messages_per_publisher + sequence;
        let expected_channel = output_channel(output_index, index % HOT_CHANNEL_COUNT);
        ensure!(
            message.get_channel_name() == expected_channel,
            "unexpected channel {} (wanted {expected_channel})",
            message.get_channel_name()
        );
        recv[index].store(base.elapsed().as_nanos() as u64, Ordering::SeqCst);
    }
    Ok(())
}

type TimelineSample = (u64, u64, u64, u64, u64);

async fn poller_task(
    status_host: String,
    status_port: u16,
    base: Instant,
    stop: Arc<AtomicBool>,
    timeline: Arc<Mutex<Vec<TimelineSample>>>,
) {
    loop {
        if let Ok(snapshot) = fetch_status_async(status_host.clone(), status_port).await {
            let out_a = snapshot.outputs.get("out-a");
            let out_b = snapshot.outputs.get("out-b");
            let sample = (
                base.elapsed().as_millis() as u64,
                snapshot.input_messages_total,
                out_a.map_or(0, |output| output.output_messages_total),
                out_b.map_or(0, |output| output.output_messages_total),
                out_a.map_or(0, |output| output.pending_messages)
                    + out_b.map_or(0, |output| output.pending_messages),
            );
            timeline.lock().unwrap().push(sample);
        }
        if stop.load(Ordering::Relaxed) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn output_line(name: &str, output: &OutputSnapshot) {
    let queue_wait_avg_ms = if output.queue_wait_samples > 0 {
        output.queue_wait_total_ns as f64 / output.queue_wait_samples as f64 / 1_000_000.0
    } else {
        0.0
    };
    let publish_rtt_avg_ms = if output.publish_rtt_samples > 0 {
        output.publish_rtt_total_ns as f64 / output.publish_rtt_samples as f64 / 1_000_000.0
    } else {
        0.0
    };
    println!(
        "output {name} published={} batches={} pending={} queue_wait_avg_max_ms={queue_wait_avg_ms:.3}/{:.3} publish_rtt_avg_max_ms={publish_rtt_avg_ms:.3}/{:.3}",
        output.output_messages_total,
        output.output_batches_total,
        output.pending_messages,
        output.queue_wait_max_ns as f64 / 1_000_000.0,
        output.publish_rtt_max_ns as f64 / 1_000_000.0
    );
}

#[tokio::main]
async fn main() -> Result<()> {
    let publisher_count: usize = env_parse("PUBLISHER_COUNT", 4);
    let messages_per_publisher: usize = env_parse("MESSAGES_PER_PUBLISHER", 2500);
    let serial_messages: usize = env_parse("SERIAL_MESSAGES", 250);
    let pipeline: usize = env_parse("HOTPATH_PIPELINE", 64);
    let connections: usize = env_parse("HOTPATH_CONNECTIONS", 1);
    let output_count: usize = env_parse("HOTPATH_OUTPUTS", 2);
    let open_loop =
        env::var("HOTPATH_MODE").is_ok_and(|mode| mode.eq_ignore_ascii_case("open-loop"));
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
    anyhow::ensure!(publisher_count > 0, "PUBLISHER_COUNT must be positive");
    anyhow::ensure!(
        messages_per_publisher > 0,
        "MESSAGES_PER_PUBLISHER must be positive"
    );
    anyhow::ensure!(
        (1..=2).contains(&output_count),
        "HOTPATH_OUTPUTS must be 1 or 2"
    );
    let output_names = OUTPUT_NAMES[..output_count].to_vec();

    let total_messages = publisher_count * messages_per_publisher;
    let total_expected = total_messages + HOT_CHANNEL_COUNT + serial_messages;

    wait_for_input_subscription(&raw_host).await?;

    let raw_client = redis::Client::open(format!("redis://{raw_host}:6379/0"))?;
    let out_a_client = redis::Client::open(format!("redis://{output_host}:6379/0"))?;
    let out_b_client = redis::Client::open(format!("redis://{output_host_b}:6379/1"))?;
    let mut writer = raw_client.get_multiplexed_async_connection().await?;

    // Warm-up and serial phases on dedicated subscriber connections; the
    // concurrent phase creates its own subscriptions afterwards.
    let mut warm_pubsub_a = out_a_client.get_async_pubsub().await?;
    warm_pubsub_a.psubscribe("replica-a:*").await?;
    let mut warm_stream_a = warm_pubsub_a.on_message();
    let mut warm_pubsub_b = if output_count == 2 {
        let mut pubsub = out_b_client.get_async_pubsub().await?;
        pubsub.psubscribe("replica-b:*").await?;
        Some(pubsub)
    } else {
        None
    };
    let mut warm_stream_b = warm_pubsub_b.as_mut().map(|pubsub| pubsub.on_message());

    for channel_index in 0..HOT_CHANNEL_COUNT {
        let payload = format!("warm:{channel_index}");
        publish(
            &mut writer,
            &source_channel(channel_index),
            payload.as_bytes(),
        )
        .await?;
        expect_message(
            &mut warm_stream_a,
            &output_channel(0, channel_index),
            payload.as_bytes(),
        )
        .await?;
        if let Some(stream_b) = warm_stream_b.as_mut() {
            expect_message(
                stream_b,
                &output_channel(1, channel_index),
                payload.as_bytes(),
            )
            .await?;
        }
    }

    let mut serial_latencies = Vec::with_capacity(serial_messages);
    for sequence in 0..serial_messages {
        let channel_index = sequence % HOT_CHANNEL_COUNT;
        let payload = format!("serial:{sequence}");
        let started = Instant::now();
        publish(
            &mut writer,
            &source_channel(channel_index),
            payload.as_bytes(),
        )
        .await?;
        expect_message(
            &mut warm_stream_a,
            &output_channel(0, channel_index),
            payload.as_bytes(),
        )
        .await?;
        if let Some(stream_b) = warm_stream_b.as_mut() {
            expect_message(
                stream_b,
                &output_channel(1, channel_index),
                payload.as_bytes(),
            )
            .await?;
        }
        serial_latencies.push(started.elapsed().as_nanos() as u64);
    }
    drop(warm_stream_b);
    drop(warm_stream_a);
    drop(warm_pubsub_b);
    drop(warm_pubsub_a);

    serial_latencies.sort_unstable();
    println!(
        "hotpath-serial messages={serial_messages} cache_mode=worker-local latency_ms_p50={:.3} p95={:.3} p99={:.3} max={:.3}",
        percentile_ms(&serial_latencies, 0.50),
        percentile_ms(&serial_latencies, 0.95),
        percentile_ms(&serial_latencies, 0.99),
        serial_latencies.last().copied().unwrap_or(0) as f64 / 1_000_000.0
    );

    let base = Instant::now();
    let starts = Arc::new(
        (0..total_messages)
            .map(|_| AtomicU64::new(0))
            .collect::<Vec<_>>(),
    );
    let acks = Arc::new(
        (0..total_messages)
            .map(|_| AtomicU64::new(0))
            .collect::<Vec<_>>(),
    );
    let recv_sets: Vec<Arc<Vec<AtomicU64>>> = (0..output_count)
        .map(|_| {
            Arc::new(
                (0..total_messages)
                    .map(|_| AtomicU64::new(0))
                    .collect::<Vec<_>>(),
            )
        })
        .collect();
    let stop_polling = Arc::new(AtomicBool::new(false));
    let timeline: Arc<Mutex<Vec<TimelineSample>>> = Arc::new(Mutex::new(Vec::new()));
    let barrier = Arc::new(tokio::sync::Barrier::new(publisher_count + 1));
    let subscriber_ready = Arc::new(tokio::sync::Barrier::new(output_count + 1));

    let mut subscriber_handles = Vec::with_capacity(output_count);
    for (output_index, recv) in recv_sets.iter().enumerate() {
        let host = if output_index == 0 {
            output_host.clone()
        } else {
            output_host_b.clone()
        };
        subscriber_handles.push(tokio::spawn(subscriber_task(
            host,
            output_index,
            messages_per_publisher,
            publisher_count,
            Arc::clone(recv),
            base,
            Arc::clone(&subscriber_ready),
        )));
    }
    tokio::time::timeout(Duration::from_secs(30), subscriber_ready.wait())
        .await
        .context("subscribers did not confirm PSUBSCRIBE in time")?;
    let poller = tokio::spawn(poller_task(
        status_host.clone(),
        status_port,
        base,
        Arc::clone(&stop_polling),
        Arc::clone(&timeline),
    ));

    let mut publishers = Vec::new();
    for publisher_id in 0..publisher_count {
        if open_loop {
            publishers.push(tokio::spawn(publisher_task_open_loop(
                raw_host.clone(),
                publisher_id,
                messages_per_publisher,
                pipeline,
                connections,
                Arc::clone(&starts),
                Arc::clone(&acks),
                base,
                Arc::clone(&barrier),
            )));
        } else {
            publishers.push(tokio::spawn(publisher_task_closed_loop(
                raw_host.clone(),
                publisher_id,
                messages_per_publisher,
                Arc::clone(&starts),
                Arc::clone(&acks),
                base,
                Arc::clone(&barrier),
            )));
        }
    }

    let wall_started = Instant::now();
    barrier.wait().await;
    for publisher in publishers {
        let outcome = tokio::time::timeout(Duration::from_secs(300), publisher)
            .await
            .context("publisher task timed out")?;
        outcome??;
    }
    let publishers_done = Instant::now();
    for handle in subscriber_handles {
        let outcome = tokio::time::timeout(Duration::from_secs(300), handle)
            .await
            .context("subscriber task timed out")?;
        outcome??;
    }
    let wall_finished = Instant::now();
    stop_polling.store(true, Ordering::Relaxed);
    let _ = poller.await;

    let mut input_rtt = Vec::with_capacity(total_messages);
    for index in 0..total_messages {
        let start = starts[index].load(Ordering::SeqCst);
        let ack = acks[index].load(Ordering::SeqCst);
        ensure!(start > 0 && ack > 0, "publisher {index} did not ack");
        input_rtt.push(ack - start);
    }
    input_rtt.sort_unstable();

    let mut e2e: Vec<Vec<u64>> = vec![Vec::new(); output_count];
    let mut post_ack: Vec<Vec<u64>> = vec![Vec::new(); output_count];
    for (output_index, recv) in recv_sets.iter().enumerate() {
        for index in 0..total_messages {
            let start = starts[index].load(Ordering::SeqCst);
            let ack = acks[index].load(Ordering::SeqCst);
            let received = recv[index].load(Ordering::SeqCst);
            ensure!(
                received > 0,
                "message {index} missing for output {}",
                output_names[output_index]
            );
            e2e[output_index].push(received - start);
            if ack > 0 {
                post_ack[output_index].push(received.saturating_sub(ack));
            }
        }
        e2e[output_index].sort_unstable();
        post_ack[output_index].sort_unstable();
    }

    let publisher_phase = publishers_done.duration_since(wall_started);
    let wall_elapsed = wall_finished.duration_since(wall_started);
    let drain = wall_finished.duration_since(publishers_done);
    let mode = if open_loop {
        "open-loop"
    } else {
        "closed-loop"
    };
    println!(
        "hotpath-e2e mode={mode} publishers={publisher_count} messages_per_publisher={messages_per_publisher} outputs={output_count} cache_mode=worker-local messages={total_messages} pipeline={} connections={} publish_phase_s={:.3} output_drain_s={:.3} elapsed_s={:.3} end_to_end_messages_per_second={:.0} input_ack_rtt_p50_p95_ms={:.3}/{:.3}",
        if open_loop { pipeline } else { 1 },
        if open_loop { connections } else { 1 },
        publisher_phase.as_secs_f64(),
        drain.as_secs_f64(),
        wall_elapsed.as_secs_f64(),
        total_messages as f64 / wall_elapsed.as_secs_f64(),
        percentile_ms(&input_rtt, 0.50),
        percentile_ms(&input_rtt, 0.95)
    );

    let markers = timeline.lock().unwrap().clone();
    let burst_offset = wall_started.duration_since(base).as_secs_f64();
    let reached = |check: &dyn Fn(u64, u64, u64) -> bool| -> f64 {
        for (elapsed_ms, input, out_a, out_b, _) in &markers {
            if check(*input, *out_a, *out_b) {
                return *elapsed_ms as f64 / 1000.0 - burst_offset;
            }
        }
        f64::NAN
    };
    let max_pending = markers.iter().map(|sample| sample.4).max().unwrap_or(0);
    let out_a_done = reached(&|_, out_a, _| out_a >= total_expected as u64);
    if output_count == 2 {
        println!(
            "hotpath-timeline subscribers_done_s={:.3} input_done_s={:.3} out-a_done_s={:.3} out-b_done_s={:.3} max_pending={max_pending}",
            wall_elapsed.as_secs_f64(),
            reached(&|input, _, _| input >= total_expected as u64),
            out_a_done,
            reached(&|_, _, out_b| out_b >= total_expected as u64)
        );
    } else {
        println!(
            "hotpath-timeline subscribers_done_s={:.3} input_done_s={:.3} out-a_done_s={:.3} max_pending={max_pending}",
            wall_elapsed.as_secs_f64(),
            reached(&|input, _, _| input >= total_expected as u64),
            out_a_done
        );
    }

    for output_index in 0..output_count {
        println!(
            "hotpath-phase {} e2e_p50_p95_p99_ms={:.3}/{:.3}/{:.3} post_ack_p50_p95_ms={:.3}/{:.3}",
            output_names[output_index],
            percentile_ms(&e2e[output_index], 0.50),
            percentile_ms(&e2e[output_index], 0.95),
            percentile_ms(&e2e[output_index], 0.99),
            percentile_ms(&post_ack[output_index], 0.50),
            percentile_ms(&post_ack[output_index], 0.95)
        );
    }

    let final_status = wait_for_status_totals(
        &status_host,
        status_port,
        total_expected as u64,
        &output_names,
    )
    .await?;
    for name in &output_names {
        if let Some(output) = final_status.outputs.get(*name) {
            output_line(name, output);
        }
    }
    Ok(())
}
