//! Rust replacements for `tests/docker_protocol_integration.py` and
//! `tests/docker_fault_integration.py`.
//!
//! Both drivers use raw RESP connections so the assertions keep the exact
//! behaviour of the Python versions: the protocol driver measures byte-sized
//! transactions through the counting proxy, and the fault driver observes the
//! uncertain transaction produced when the fault proxy drops an `EXEC` reply.

use std::{
    collections::BTreeMap,
    env, io,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail, ensure};
use serde_json::Value;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader},
    net::{
        TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    sync::mpsc,
    task::JoinHandle,
    time::{sleep, timeout},
};
use url::Url;

use crate::resp::{Frame, RespValue, encode_command, read_frame};

type Message = (Vec<u8>, Vec<u8>);
type Publication = (&'static str, Vec<u8>);
type Truncated = (Vec<u8>, usize);

const OUTPUT_NAMES: [&str; 5] = ["chunked", "send", "truncate", "drop", "immediate"];
const REDIS_QUERY_LIMIT: usize = 1024 * 1024;
const CHUNK_TARGET: usize = 700 * 1024;
const SMALL_TARGET: usize = 220 * 1024;
const SMALL_PAYLOAD_BYTES: usize = 100 * 1024;
const LARGE_PAYLOAD_BYTES: usize = 300 * 1024;

fn env_parse<T>(name: &str, default: T) -> Result<T>
where
    T: std::str::FromStr,
{
    match env::var(name) {
        Ok(value) => value
            .parse()
            .map_err(|_| anyhow::anyhow!("{name} must be a valid value")),
        Err(_) => Ok(default),
    }
}

// ---------------------------------------------------------------------------
// Redis connection with a cancellation-safe frame stream
// ---------------------------------------------------------------------------

/// Reads RESP frames in a background task so the drivers can apply short
/// timeouts without cancelling a partially-read frame (which would lose bytes).
struct FrameStream {
    receiver: mpsc::UnboundedReceiver<io::Result<Frame>>,
    task: JoinHandle<()>,
}

impl FrameStream {
    fn spawn(mut reader: BufReader<OwnedReadHalf>) -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            loop {
                match read_frame(&mut reader).await {
                    Ok(frame) => {
                        if sender.send(Ok(frame)).is_err() {
                            break;
                        }
                    }
                    Err(error) => {
                        let _ = sender.send(Err(error));
                        break;
                    }
                }
            }
        });
        Self { receiver, task }
    }

    async fn next(&mut self, wait: Duration) -> Option<io::Result<Frame>> {
        timeout(wait, self.receiver.recv())
            .await
            .unwrap_or_default()
    }
}

impl Drop for FrameStream {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct RedisConnection {
    writer: OwnedWriteHalf,
    frames: FrameStream,
}

impl RedisConnection {
    async fn connect(database: u32) -> Result<Self> {
        let host = env::var("REDIS_HOST").unwrap_or_else(|_| "redis".to_owned());
        let port = env_parse("REDIS_PORT", 6379)?;
        let stream = timeout(
            Duration::from_secs(5),
            TcpStream::connect((host.as_str(), port)),
        )
        .await
        .context("timed out connecting to Redis")?
        .with_context(|| format!("failed to connect to Redis at {host}:{port}"))?;
        stream.set_nodelay(true).ok();

        let (read_half, mut writer) = stream.into_split();
        let mut reader = BufReader::new(read_half);
        let select = encode_command(&[b"SELECT", database.to_string().as_bytes()]);
        writer
            .write_all(&select)
            .await
            .context("failed to send SELECT")?;
        let frame = timeout(Duration::from_secs(5), read_frame(&mut reader))
            .await
            .context("timed out waiting for the SELECT reply")?
            .context("failed to read the SELECT reply")?;
        match frame.value {
            RespValue::Simple(bytes) if bytes == b"OK" => {}
            other => bail!("Redis did not select database {database}: {other:?}"),
        }

        Ok(Self {
            writer,
            frames: FrameStream::spawn(reader),
        })
    }

    async fn send(&mut self, arguments: &[&[u8]]) -> Result<()> {
        self.writer
            .write_all(&encode_command(arguments))
            .await
            .context("failed to send command")
    }

    async fn next(&mut self, wait: Duration) -> Result<Option<Frame>> {
        match self.frames.next(wait).await {
            None => Ok(None),
            Some(Ok(frame)) => Ok(Some(frame)),
            Some(Err(error)) => Err(error).context("failed to read a Redis frame"),
        }
    }

    async fn command(&mut self, arguments: &[&[u8]]) -> Result<RespValue> {
        self.send(arguments).await?;
        let frame = self
            .next(Duration::from_secs(5))
            .await?
            .context("timed out waiting for a Redis reply")?;
        match frame.value {
            RespValue::Error(message) => {
                bail!("Redis error: {}", String::from_utf8_lossy(&message))
            }
            value => Ok(value),
        }
    }
}

fn parse_pmessage(value: &RespValue) -> Option<Message> {
    let items = match value {
        RespValue::Array(Some(items)) | RespValue::Push(items) => items,
        _ => return None,
    };
    if items.len() == 4 && items[0].as_bytes() == Some(b"pmessage") {
        Some((items[2].as_bytes()?.to_vec(), items[3].as_bytes()?.to_vec()))
    } else {
        None
    }
}

fn counter(messages: &[Message]) -> Vec<Message> {
    let mut sorted = messages.to_vec();
    sorted.sort();
    sorted
}

fn nested<'a>(value: &'a Value, path: &[&str]) -> Option<&'a Value> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    Some(current)
}

fn nested_u64(value: &Value, path: &[&str]) -> Option<u64> {
    nested(value, path)?.as_u64()
}

fn nested_str<'a>(value: &'a Value, path: &[&str]) -> Option<&'a str> {
    nested(value, path)?.as_str()
}

fn metric_u64(metrics: &Value, key: &str) -> Option<u64> {
    metrics.get(key).and_then(Value::as_u64)
}

fn metric_array<'a>(metrics: &'a Value, key: &str) -> Result<&'a Vec<Value>> {
    metrics
        .get(key)
        .and_then(Value::as_array)
        .with_context(|| format!("missing {key} array"))
}

// ---------------------------------------------------------------------------
// HTTP status and proxy stats helpers
// ---------------------------------------------------------------------------

async fn get_status(url: &str) -> Result<Value> {
    let parsed = Url::parse(url).context("invalid status URL")?;
    let host = parsed.host_str().context("status URL has no host")?;
    let port = parsed
        .port_or_known_default()
        .context("status URL has no port")?;
    let target = if parsed.path().is_empty() {
        "/"
    } else {
        parsed.path()
    };

    let mut stream = timeout(Duration::from_secs(3), TcpStream::connect((host, port)))
        .await
        .context("timed out connecting to the status endpoint")?
        .with_context(|| format!("failed to connect to the status endpoint at {host}:{port}"))?;
    let request =
        format!("GET {target} HTTP/1.1\r\nHost: {host}:{port}\r\nConnection: close\r\n\r\n");
    timeout(Duration::from_secs(3), stream.write_all(request.as_bytes()))
        .await
        .context("timed out sending the status request")??;
    let mut response = Vec::new();
    timeout(Duration::from_secs(3), stream.read_to_end(&mut response))
        .await
        .context("timed out reading the status response")??;

    let header_end = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .context("invalid HTTP status response")?;
    let status_line = response[..header_end]
        .split(|&byte| byte == b'\n')
        .next()
        .unwrap_or_default();
    let status_line = String::from_utf8_lossy(status_line);
    ensure!(
        status_line.contains(" 200 "),
        "unexpected HTTP status: {}",
        status_line.trim_end()
    );
    serde_json::from_slice(&response[header_end + 4..]).context("invalid status JSON")
}

async fn wait_for_status<F>(
    status_url: &str,
    predicate: F,
    description: &str,
    wait: Duration,
) -> Result<Value>
where
    F: Fn(&Value) -> bool,
{
    let deadline = Instant::now() + wait;
    let mut latest = Value::Null;
    while Instant::now() < deadline {
        latest = get_status(status_url).await?;
        if predicate(&latest) {
            return Ok(latest);
        }
        sleep(Duration::from_millis(50)).await;
    }
    bail!("Timed out waiting for {description}; last status: {latest}")
}

async fn fetch_until(host: &str, port: u16, terminator: &[u8]) -> Result<Vec<u8>> {
    let stream = timeout(Duration::from_secs(3), TcpStream::connect((host, port)))
        .await
        .context("timed out connecting to the proxy stats port")?
        .with_context(|| format!("failed to connect to proxy stats at {host}:{port}"))?;
    let (read_half, mut writer) = stream.into_split();
    writer
        .write_all(b"STATS\r\n")
        .await
        .context("failed to send STATS")?;
    let mut reader = BufReader::new(read_half);

    let read_response = async {
        let mut response = Vec::new();
        loop {
            let mut byte = [0_u8; 1];
            reader.read_exact(&mut byte).await?;
            response.push(byte[0]);
            if response.ends_with(terminator) {
                return Ok::<Vec<u8>, io::Error>(response);
            }
        }
    };
    timeout(Duration::from_secs(3), read_response)
        .await
        .context("timed out reading the proxy stats")?
        .context("failed to read the proxy stats")
}

async fn get_counting_proxy_stats(host: &str, port: u16) -> Result<Value> {
    let response = fetch_until(host, port, b"\n").await?;
    serde_json::from_slice(&response).context("invalid counting proxy stats JSON")
}

// ---------------------------------------------------------------------------
// Protocol/oversized-policy integration
// ---------------------------------------------------------------------------

struct ProtocolConfig {
    status_url: String,
    proxy_host: String,
    proxy_stats_port: u16,
}

impl ProtocolConfig {
    fn from_env() -> Result<Self> {
        Ok(Self {
            status_url: env::var("STATUS_URL")
                .unwrap_or_else(|_| "http://service-protocol:9090/".to_owned()),
            proxy_host: env::var("PROXY_HOST").unwrap_or_else(|_| "counting-proxy".to_owned()),
            proxy_stats_port: env_parse("PROXY_STATS_PORT", 9091)?,
        })
    }
}

fn make_payload(tag: &str, length: usize) -> Vec<u8> {
    let mut payload = vec![0x00, 0xff];
    payload.extend_from_slice(tag.as_bytes());
    payload.push(b':');
    if payload.len() < length {
        let fill = *tag.as_bytes().last().expect("payload tag is not empty");
        payload.resize(length, fill);
    }
    payload
}

fn publications() -> Vec<Publication> {
    vec![
        (
            "oversize-feed:a",
            make_payload("a-old", SMALL_PAYLOAD_BYTES),
        ),
        (
            "oversize-feed:a",
            make_payload("a-latest", SMALL_PAYLOAD_BYTES),
        ),
        (
            "oversize-feed:b",
            make_payload("b-large", LARGE_PAYLOAD_BYTES),
        ),
        (
            "oversize-feed:c",
            make_payload("c-large", LARGE_PAYLOAD_BYTES),
        ),
        (
            "oversize-feed:d",
            make_payload("d-large", LARGE_PAYLOAD_BYTES),
        ),
        (
            "oversize-feed:e",
            make_payload("e-large", LARGE_PAYLOAD_BYTES),
        ),
        (
            "oversize-feed:z",
            make_payload("z-large", LARGE_PAYLOAD_BYTES),
        ),
    ]
}

const SENTINEL_PUBLICATIONS: [(&str, &[u8]); 2] = [
    ("__sentinel__:hello", b"master,127.0.0.1,6379,runid"),
    ("+switch-master", b"master 127.0.0.1 6379 127.0.0.2 6380"),
];

fn latest_publications(publications: &[Publication]) -> Vec<(&'static str, &[u8])> {
    let mut latest: Vec<(&'static str, &[u8])> = Vec::new();
    for (channel, payload) in publications {
        match latest.iter().position(|(existing, _)| existing == channel) {
            Some(index) => latest[index].1 = payload.as_slice(),
            None => latest.push((channel, payload.as_slice())),
        }
    }
    latest
}

fn output_channel(output_name: &str, source_channel: &str) -> Vec<u8> {
    format!("out:{output_name}:{source_channel}").into_bytes()
}

fn bulk_resp_bytes(length: usize) -> usize {
    length + length.to_string().len() + 5
}

fn publish_resp_bytes(channel_length: usize, payload_length: usize) -> usize {
    4 + bulk_resp_bytes(7) + bulk_resp_bytes(channel_length) + bulk_resp_bytes(payload_length)
}

fn transaction_bytes(channel_length: usize, payload_length: usize) -> usize {
    15 + publish_resp_bytes(channel_length, payload_length) + 14
}

fn max_payload_for_transaction(channel: &[u8], target: usize, maximum: usize) -> Result<usize> {
    let mut low = 0_i64;
    let mut high = maximum as i64;
    let mut best = None;
    while low <= high {
        let candidate = (low + high) / 2;
        let candidate = candidate as usize;
        if transaction_bytes(channel.len(), candidate) <= target {
            best = Some(candidate);
            low = candidate as i64 + 1;
        } else {
            high = candidate as i64 - 1;
        }
    }
    best.context("the channel and RESP framing exceed the test target")
}

async fn wait_for_input_subscription(expected_patterns: i64) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut last_count = None;
    while Instant::now() < deadline {
        let mut connection = RedisConnection::connect(0).await?;
        let count = connection.command(&[b"PUBSUB", b"NUMPAT"]).await?.as_int();
        last_count = count;
        drop(connection);
        if count == Some(expected_patterns) {
            return Ok(());
        }
        sleep(Duration::from_millis(50)).await;
    }
    bail!("Input subscription did not become active; NUMPAT={last_count:?}")
}

async fn open_subscribers() -> Result<Vec<(&'static str, RedisConnection)>> {
    let mut subscribers = Vec::new();
    for (index, name) in OUTPUT_NAMES.iter().enumerate() {
        let mut connection = RedisConnection::connect(index as u32 + 1).await?;
        let pattern = format!("out:{name}:*");
        let acknowledgement = connection
            .command(&[b"PSUBSCRIBE", pattern.as_bytes()])
            .await?;
        ensure!(
            acknowledgement
                .as_array()
                .and_then(|items| items.first())
                .and_then(RespValue::as_bytes)
                == Some(b"psubscribe"),
            "unexpected PSUBSCRIBE acknowledgement: {acknowledgement:?}"
        );
        subscribers.push((*name, connection));
    }
    Ok(subscribers)
}

async fn collect_exact(
    connection: &mut RedisConnection,
    expected: &[Message],
    wait: Duration,
) -> Result<Vec<Message>> {
    let expected_counter = counter(expected);
    let expected_count = expected.len();
    let deadline = Instant::now() + wait;
    let mut received = Vec::new();
    while counter(&received) != expected_counter && Instant::now() < deadline {
        let Some(frame) = connection.next(Duration::from_millis(100)).await? else {
            continue;
        };
        let message = parse_pmessage(&frame.value)
            .with_context(|| format!("unexpected Redis Pub/Sub response: {:?}", frame.value))?;
        received.push(message);
        ensure!(
            received.len() <= expected_count,
            "received duplicate or unexpected output: {received:?}"
        );
    }
    ensure!(
        counter(&received) == expected_counter,
        "received_count: {}, expected_count: {expected_count}",
        received.len()
    );
    if let Some(frame) = connection.next(Duration::from_millis(500)).await? {
        bail!(
            "an output emitted duplicate Pub/Sub events: {:?}",
            frame.value
        );
    }
    Ok(received)
}

async fn publish_feed(publications: &[Publication]) -> Result<()> {
    let mut connection = RedisConnection::connect(0).await?;
    for (channel, payload) in publications {
        let count = connection
            .command(&[b"PUBLISH", channel.as_bytes(), payload])
            .await?;
        let count = count
            .as_int()
            .with_context(|| format!("PUBLISH {channel} did not return an integer"))?;
        ensure!(count >= 1, "{channel} subscriber count: {count}");
    }
    Ok(())
}

async fn publish_sentinel_messages() -> Result<()> {
    let mut connection = RedisConnection::connect(0).await?;
    for (channel, payload) in SENTINEL_PUBLICATIONS {
        let count = connection
            .command(&[b"PUBLISH", channel.as_bytes(), payload])
            .await?;
        let count = count
            .as_int()
            .with_context(|| format!("PUBLISH {channel} did not return an integer"))?;
        ensure!(count >= 1, "{channel} subscriber count: {count}");
    }
    Ok(())
}

fn protocol_outputs_match(status: &Value, expected_counts: &BTreeMap<&str, usize>) -> bool {
    let Some(outputs) = status.get("outputs").and_then(Value::as_object) else {
        return false;
    };
    if outputs.len() != OUTPUT_NAMES.len()
        || !OUTPUT_NAMES.iter().all(|name| outputs.contains_key(*name))
    {
        return false;
    }
    expected_counts.iter().all(|(name, count)| {
        outputs.get(*name).is_some_and(|metrics| {
            metrics.get("output_messages_total").and_then(Value::as_u64) == Some(*count as u64)
                && metrics.get("pending_messages").and_then(Value::as_u64) == Some(0)
                && metrics.get("publish_errors_total").and_then(Value::as_u64) == Some(0)
        })
    })
}

pub async fn run_protocol_integration() -> Result<()> {
    let config = ProtocolConfig::from_env()?;
    let publications = publications();
    let latest = latest_publications(&publications);

    wait_for_input_subscription(3).await?;
    let mut subscribers = open_subscribers().await?;

    // Let every positive interval reach its first tick before sending the burst.
    sleep(Duration::from_millis(1700)).await;
    publish_feed(&publications).await?;
    publish_sentinel_messages().await?;

    let mut expected_truncate: Vec<(&'static str, Truncated)> = Vec::new();
    for (source, payload) in &latest {
        let channel = output_channel("truncate", source);
        if payload.len() > SMALL_TARGET {
            let length = max_payload_for_transaction(&channel, SMALL_TARGET, payload.len())?;
            expected_truncate.push((source, (payload[..length].to_vec(), payload.len() - length)));
        } else {
            expected_truncate.push((source, (payload.to_vec(), 0)));
        }
    }
    let truncated_bytes: usize = expected_truncate
        .iter()
        .map(|(_, (_, removed))| removed)
        .sum();
    let expected_counts = BTreeMap::from([
        ("chunked", latest.len()),
        ("send", latest.len()),
        ("truncate", latest.len()),
        ("drop", 1),
        ("immediate", publications.len()),
    ]);

    let status = wait_for_status(
        &config.status_url,
        |current| {
            current.get("state").and_then(Value::as_str) == Some("running")
                && current.get("input_messages_total").and_then(Value::as_u64)
                    == Some(publications.len() as u64)
                && current
                    .get("excluded_messages_total")
                    .and_then(Value::as_u64)
                    == Some(SENTINEL_PUBLICATIONS.len() as u64)
                && protocol_outputs_match(current, &expected_counts)
        },
        "all policies and the immediate output to finish",
        Duration::from_secs(20),
    )
    .await?;

    let mut expected_messages: Vec<(&'static str, Vec<Message>)> = Vec::new();
    for name in ["chunked", "send"] {
        expected_messages.push((
            name,
            latest
                .iter()
                .map(|(source, payload)| (output_channel(name, source), payload.to_vec()))
                .collect(),
        ));
    }
    expected_messages.push((
        "truncate",
        expected_truncate
            .iter()
            .map(|(source, (payload, _))| (output_channel("truncate", source), payload.clone()))
            .collect(),
    ));
    let drop_latest = latest
        .iter()
        .find(|(source, _)| *source == "oversize-feed:a")
        .context("missing the oversize-feed:a latest payload")?;
    expected_messages.push((
        "drop",
        vec![(
            output_channel("drop", drop_latest.0),
            drop_latest.1.to_vec(),
        )],
    ));
    expected_messages.push((
        "immediate",
        publications
            .iter()
            .map(|(source, payload)| (output_channel("immediate", source), payload.clone()))
            .collect(),
    ));

    for (name, expected) in &expected_messages {
        let subscriber = subscribers
            .iter_mut()
            .find(|entry| entry.0 == *name)
            .with_context(|| format!("no subscriber for {name}"))?;
        collect_exact(&mut subscriber.1, expected, Duration::from_secs(10)).await?;
    }

    let outputs = status
        .get("outputs")
        .and_then(Value::as_object)
        .context("status has no outputs object")?;
    ensure!(
        outputs.len() == OUTPUT_NAMES.len()
            && OUTPUT_NAMES.iter().all(|name| outputs.contains_key(*name)),
        "unexpected output names: {status}"
    );

    let expected_batches = BTreeMap::from([
        ("chunked", 3_u64),
        ("send", 6),
        ("truncate", 6),
        ("drop", 1),
        ("immediate", 7),
    ]);
    let expected_conflated = BTreeMap::from([
        ("chunked", 1_u64),
        ("send", 1),
        ("truncate", 1),
        ("drop", 1),
        ("immediate", 0),
    ]);
    for name in OUTPUT_NAMES {
        let metrics = outputs
            .get(name)
            .with_context(|| format!("missing {name}"))?;
        ensure!(
            metric_u64(metrics, "input_messages_total") == Some(publications.len() as u64),
            "{name} metrics: {metrics}"
        );
        ensure!(
            metric_u64(metrics, "output_batches_total") == expected_batches.get(name).copied(),
            "{name} metrics: {metrics}"
        );
        ensure!(
            metric_u64(metrics, "conflated_messages_total")
                == expected_conflated.get(name).copied(),
            "{name} metrics: {metrics}"
        );
        ensure!(
            metric_u64(metrics, "publish_errors_total") == Some(0),
            "{name} metrics: {metrics}"
        );
        ensure!(
            metric_u64(metrics, "pending_payload_bytes") == Some(0),
            "{name} metrics: {metrics}"
        );
    }
    ensure!(
        status.get("output_batches_total").and_then(Value::as_u64)
            == Some(expected_batches.values().sum()),
        "{status}"
    );
    ensure!(
        status.get("output_messages_total").and_then(Value::as_u64)
            == Some(expected_counts.values().sum::<usize>() as u64),
        "{status}"
    );
    ensure!(
        status
            .get("conflated_messages_total")
            .and_then(Value::as_u64)
            == Some(expected_conflated.values().sum()),
        "{status}"
    );
    ensure!(
        status.get("publish_errors_total").and_then(Value::as_u64) == Some(0),
        "{status}"
    );
    ensure!(
        metric_u64(&outputs["truncate"], "truncated_messages_total") == Some(5),
        "truncate: {}",
        outputs["truncate"]
    );
    ensure!(
        metric_u64(&outputs["truncate"], "truncated_payload_bytes_total")
            == Some(truncated_bytes as u64),
        "truncate: {}",
        outputs["truncate"]
    );
    ensure!(
        metric_u64(&outputs["drop"], "dropped_messages_total") == Some(5),
        "drop: {}",
        outputs["drop"]
    );
    ensure!(
        metric_u64(&outputs["immediate"], "output_messages_total")
            == Some(publications.len() as u64),
        "immediate: {}",
        outputs["immediate"]
    );
    ensure!(
        status.get("dropped_messages_total").and_then(Value::as_u64) == Some(5),
        "{status}"
    );
    ensure!(
        status
            .get("truncated_messages_total")
            .and_then(Value::as_u64)
            == Some(5),
        "{status}"
    );
    ensure!(
        status
            .get("truncated_payload_bytes_total")
            .and_then(Value::as_u64)
            == Some(truncated_bytes as u64),
        "{status}"
    );
    ensure!(
        status
            .get("uncertain_transactions_total")
            .and_then(Value::as_u64)
            == Some(0),
        "{status}"
    );
    ensure!(
        status
            .get("excluded_messages_total")
            .and_then(Value::as_u64)
            == Some(SENTINEL_PUBLICATIONS.len() as u64),
        "{status}"
    );

    let expected_target_for_output = BTreeMap::from([
        ("chunked", CHUNK_TARGET),
        ("send", SMALL_TARGET),
        ("truncate", SMALL_TARGET),
        ("drop", SMALL_TARGET),
        ("immediate", SMALL_TARGET),
    ]);
    let proxy = get_counting_proxy_stats(&config.proxy_host, config.proxy_stats_port).await?;
    let expected_multi: u64 = expected_batches
        .iter()
        .filter(|(name, _)| **name != "immediate")
        .map(|(_, count)| *count)
        .sum();
    ensure!(
        nested_u64(&proxy, &["commands", "MULTI"]) == Some(expected_multi),
        "{proxy}"
    );
    ensure!(
        nested_u64(&proxy, &["commands", "EXEC"]) == nested_u64(&proxy, &["commands", "MULTI"]),
        "{proxy}"
    );
    ensure!(
        nested_u64(&proxy, &["commands", "PUBLISH"])
            == Some(expected_counts.values().sum::<usize>() as u64),
        "{proxy}"
    );
    for name in OUTPUT_NAMES {
        let metrics = proxy
            .get("outputs")
            .and_then(|outputs| outputs.get(name))
            .with_context(|| format!("proxy stats missing output {name}"))?;
        ensure!(
            metric_u64(metrics, "publish_count")
                == expected_counts.get(name).map(|count| *count as u64),
            "{name}: {metrics}"
        );
        let expected_transactions = if name == "immediate" {
            0
        } else {
            expected_batches[name]
        };
        ensure!(
            metric_u64(metrics, "transaction_count") == Some(expected_transactions),
            "{name}: {metrics}"
        );
        let expected_direct = if name == "immediate" {
            publications.len() as u64
        } else {
            0
        };
        ensure!(
            metric_u64(metrics, "direct_publish_count") == Some(expected_direct),
            "{name}: {metrics}"
        );
        let limit = expected_target_for_output[name];
        for request_bytes in metric_array(metrics, "transaction_request_bytes")? {
            let request_bytes = request_bytes
                .as_u64()
                .with_context(|| format!("{name}: non-integer transaction_request_bytes"))?
                as usize;
            ensure!(
                request_bytes <= REDIS_QUERY_LIMIT,
                "{name}: {request_bytes}"
            );
            if matches!(name, "chunked" | "truncate" | "drop") {
                ensure!(request_bytes <= limit, "{name}: {request_bytes} > {limit}");
            }
        }
        for request_bytes in metric_array(metrics, "direct_request_bytes")? {
            let request_bytes = request_bytes
                .as_u64()
                .with_context(|| format!("{name}: non-integer direct_request_bytes"))?
                as usize;
            ensure!(
                request_bytes <= REDIS_QUERY_LIMIT,
                "{name}: {request_bytes}"
            );
        }
    }
    let send_transactions = metric_array(&proxy["outputs"]["send"], "transaction_request_bytes")?;
    ensure!(send_transactions.len() == 6, "{proxy}");
    ensure!(
        send_transactions
            .iter()
            .filter(|value| value.as_u64().unwrap_or(0) as usize > SMALL_TARGET)
            .count()
            == 5,
        "send: {proxy}"
    );
    ensure!(
        metric_array(&proxy["outputs"]["immediate"], "transaction_request_bytes")?.is_empty(),
        "{proxy}"
    );

    println!(
        "Protocol E2E passed: Pub/Sub burst was conflated and split below Redis's \
         1 MiB query limit; send/truncate/drop policies behaved as configured; \
         interval_ms=0 emitted only direct PUBLISH commands, with no MULTI/EXEC."
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Fault-injection integration
// ---------------------------------------------------------------------------

const OUTPUT_NAME: &str = "output1";
const OUTPUT_PATTERN: &str = "fault-output:*";
const OUTPUT_PREFIX: &[u8] = b"fault-output:mapped:";
const OUTPUT_SUFFIX: &[u8] = b":source";
const MIN_INPUT_PATTERN_COUNT: i64 = 2;
const COMMITTED_CHUNK: [(&str, &[u8]); 1] = [("fault-feed:alpha", b"committed-alpha:\x00\xff")];
const FAILED_CHUNK: [(&str, &[u8]); 2] = [
    ("fault-feed:beta", b"uncertain-beta:\xfe\x00"),
    ("fault-feed:gamma", b"uncertain-gamma:\x00\xfd"),
];
const LATER_MESSAGE: (&str, &[u8]) = ("fault-feed:delta", b"later-delta:\x00\xfc");

struct FaultConfig {
    proxy_host: String,
    proxy_stats_port: u16,
    status_url: String,
}

impl FaultConfig {
    fn from_env() -> Result<Self> {
        let app_host = env::var("FAULT_APP_HOST").unwrap_or_else(|_| "fault-service".to_owned());
        Ok(Self {
            proxy_host: env::var("PROXY_HOST").unwrap_or_else(|_| "fault-proxy".to_owned()),
            proxy_stats_port: env_parse("PROXY_STATS_PORT", 9091)?,
            status_url: format!("http://{app_host}:9090/"),
        })
    }
}

fn expected_output_channel(source_channel: &str) -> Vec<u8> {
    let mut channel = OUTPUT_PREFIX.to_vec();
    channel.extend_from_slice(source_channel.as_bytes());
    channel.extend_from_slice(OUTPUT_SUFFIX);
    channel
}

fn fault_outputs_match(status: &Value) -> bool {
    status
        .get("outputs")
        .and_then(Value::as_object)
        .is_some_and(|outputs| outputs.len() == 1 && outputs.contains_key(OUTPUT_NAME))
}

async fn wait_for_input_subscriptions(minimum: i64) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        let mut connection = RedisConnection::connect(0).await?;
        let pattern_count = connection.command(&[b"PUBSUB", b"NUMPAT"]).await?.as_int();
        drop(connection);
        if pattern_count.is_some_and(|count| count >= minimum) {
            return Ok(());
        }
        sleep(Duration::from_millis(50)).await;
    }
    bail!("The fault-test app did not establish its input patterns")
}

async fn publish_many(publications: &[(&str, &[u8])]) -> Result<()> {
    let mut connection = RedisConnection::connect(0).await?;
    for (channel, payload) in publications {
        connection
            .send(&[b"PUBLISH", channel.as_bytes(), payload])
            .await?;
    }
    let mut counts = Vec::with_capacity(publications.len());
    for _ in publications {
        let frame = connection
            .next(Duration::from_secs(5))
            .await?
            .context("timed out waiting for a PUBLISH reply")?;
        match frame.value {
            RespValue::Error(message) => {
                bail!("Redis error: {}", String::from_utf8_lossy(&message))
            }
            RespValue::Int(count) => counts.push(count),
            value => bail!("unexpected PUBLISH reply: {value:?}"),
        }
    }
    ensure!(counts.iter().all(|count| *count >= 1), "{counts:?}");
    Ok(())
}

async fn read_chunk(
    connection: &mut RedisConnection,
    chunk: &[(&str, &[u8])],
    wait: Duration,
) -> Result<Vec<Message>> {
    let expected: Vec<Message> = chunk
        .iter()
        .map(|(channel, payload)| (expected_output_channel(channel), payload.to_vec()))
        .collect();
    let expected_sources: Vec<&str> = chunk.iter().map(|(channel, _)| *channel).collect();
    let mut received = Vec::new();
    let deadline = Instant::now() + wait;
    while Instant::now() < deadline && received.len() < chunk.len() {
        let Some(frame) = connection.next(Duration::from_millis(100)).await? else {
            continue;
        };
        let message = parse_pmessage(&frame.value)
            .with_context(|| format!("unexpected Redis Pub/Sub response: {:?}", frame.value))?;
        ensure!(
            expected_sources
                .iter()
                .any(|source| expected_output_channel(source) == message.0),
            "{:?}",
            message.0
        );
        received.push(message);
    }
    ensure!(
        counter(&received) == counter(&expected),
        "actual: {received:?}, expected: {expected:?}"
    );
    Ok(received)
}

async fn wait_for_uncertain_result(status_url: &str, expected_messages: usize) -> Result<Value> {
    wait_for_status(
        status_url,
        |status| {
            fault_outputs_match(status)
                && status.get("state").and_then(Value::as_str) == Some("running")
                && nested_str(status, &["outputs", OUTPUT_NAME, "state"]) == Some("running")
                && nested_u64(
                    status,
                    &["outputs", OUTPUT_NAME, "uncertain_transactions_total"],
                ) == Some(1)
                && nested_u64(
                    status,
                    &["outputs", OUTPUT_NAME, "uncertain_messages_total"],
                ) == Some(expected_messages as u64)
                && nested_u64(status, &["outputs", OUTPUT_NAME, "pending_messages"]) == Some(0)
        },
        "the uncertain transaction to be counted without pausing the output",
        Duration::from_secs(10),
    )
    .await
}

async fn assert_no_output(connection: &mut RedisConnection, wait: Duration) -> Result<()> {
    if let Some(frame) = connection.next(wait).await? {
        bail!(
            "the app duplicated an already-delivered output event: {:?}",
            frame.value
        );
    }
    Ok(())
}

async fn get_fault_proxy_stats(config: &FaultConfig) -> Result<BTreeMap<String, i64>> {
    let response = fetch_until(&config.proxy_host, config.proxy_stats_port, b"\n\n").await?;
    let mut stats = BTreeMap::new();
    for line in response.split(|&byte| byte == b'\n') {
        if line.is_empty() {
            continue;
        }
        let Some(position) = line.iter().position(|&byte| byte == b'=') else {
            continue;
        };
        let key = String::from_utf8_lossy(&line[..position]).into_owned();
        let value = std::str::from_utf8(&line[position + 1..])
            .ok()
            .and_then(|text| text.parse().ok())
            .with_context(|| format!("invalid proxy stats value for {key}"))?;
        stats.insert(key, value);
    }
    Ok(stats)
}

async fn wait_for_fault_proxy_stats<F>(
    config: &FaultConfig,
    predicate: F,
    description: &str,
    wait: Duration,
) -> Result<BTreeMap<String, i64>>
where
    F: Fn(&BTreeMap<String, i64>) -> bool,
{
    let deadline = Instant::now() + wait;
    let mut latest = BTreeMap::new();
    while Instant::now() < deadline {
        latest = get_fault_proxy_stats(config).await?;
        if predicate(&latest) {
            return Ok(latest);
        }
        sleep(Duration::from_millis(50)).await;
    }
    bail!("Timed out waiting for {description}; proxy stats: {latest:?}")
}

pub async fn run_fault_integration() -> Result<()> {
    let config = FaultConfig::from_env()?;
    wait_for_input_subscriptions(MIN_INPUT_PATTERN_COUNT).await?;

    let mut output = RedisConnection::connect(1).await?;
    let acknowledgement = output
        .command(&[b"PSUBSCRIBE", OUTPUT_PATTERN.as_bytes()])
        .await?;
    ensure!(
        acknowledgement
            .as_array()
            .and_then(|items| items.first())
            .and_then(RespValue::as_bytes)
            == Some(b"psubscribe"),
        "unexpected PSUBSCRIBE acknowledgement: {acknowledgement:?}"
    );

    // Allow the output interval to advance before producing the two test batches.
    sleep(Duration::from_millis(350)).await;

    publish_many(&COMMITTED_CHUNK).await?;
    let committed_messages =
        read_chunk(&mut output, &COMMITTED_CHUNK, Duration::from_secs(8)).await?;
    let committed_status = wait_for_status(
        &config.status_url,
        |status| {
            fault_outputs_match(status)
                && status.get("state").and_then(Value::as_str) == Some("running")
                && nested_str(status, &["outputs", OUTPUT_NAME, "state"]) == Some("running")
                && nested_u64(status, &["outputs", OUTPUT_NAME, "output_batches_total"]) == Some(1)
                && nested_u64(status, &["outputs", OUTPUT_NAME, "output_messages_total"])
                    == Some(COMMITTED_CHUNK.len() as u64)
                && nested_u64(status, &["outputs", OUTPUT_NAME, "pending_messages"]) == Some(0)
        },
        "the pre-fault chunk to commit successfully",
        Duration::from_secs(10),
    )
    .await?;
    ensure!(
        nested_u64(
            &committed_status,
            &["outputs", OUTPUT_NAME, "pending_payload_bytes"]
        ) == Some(0),
        "{committed_status}"
    );
    let proxy_stats = wait_for_fault_proxy_stats(
        &config,
        |stats| stats.get("successful_execs") == Some(&1),
        "the first successful MULTI/EXEC",
        Duration::from_secs(10),
    )
    .await?;
    ensure!(
        proxy_stats.get("multi_exec_attempts") == Some(&1),
        "{proxy_stats:?}"
    );
    ensure!(
        proxy_stats.get("dropped_exec_replies") == Some(&0),
        "{proxy_stats:?}"
    );

    publish_many(&FAILED_CHUNK).await?;
    let failed_messages = read_chunk(&mut output, &FAILED_CHUNK, Duration::from_secs(8)).await?;
    let uncertain_status =
        wait_for_uncertain_result(&config.status_url, FAILED_CHUNK.len()).await?;
    let proxy_stats = wait_for_fault_proxy_stats(
        &config,
        |stats| stats.get("dropped_exec_replies") == Some(&1),
        "the proxy to drop the second successful EXEC reply",
        Duration::from_secs(10),
    )
    .await?;
    ensure!(
        proxy_stats.get("multi_exec_attempts") == Some(&2),
        "{proxy_stats:?}"
    );
    ensure!(
        proxy_stats.get("successful_execs") == Some(&2),
        "{proxy_stats:?}"
    );
    ensure!(
        proxy_stats.get("drop_publish_count") == Some(&(FAILED_CHUNK.len() as i64)),
        "{proxy_stats:?}"
    );
    ensure!(
        nested_u64(
            &uncertain_status,
            &["outputs", OUTPUT_NAME, "publish_errors_total"]
        ) == Some(1),
        "{uncertain_status}"
    );
    ensure!(
        nested_u64(
            &uncertain_status,
            &["outputs", OUTPUT_NAME, "publish_error_messages_total"]
        ) == Some(FAILED_CHUNK.len() as u64),
        "{uncertain_status}"
    );

    publish_many(&[LATER_MESSAGE]).await?;
    let later_messages = read_chunk(&mut output, &[LATER_MESSAGE], Duration::from_secs(8)).await?;
    let final_status = wait_for_status(
        &config.status_url,
        |status| {
            fault_outputs_match(status)
                && status.get("state").and_then(Value::as_str) == Some("running")
                && nested_str(status, &["outputs", OUTPUT_NAME, "state"]) == Some("running")
                && nested_u64(status, &["outputs", OUTPUT_NAME, "output_messages_total"])
                    == Some((COMMITTED_CHUNK.len() + 1) as u64)
                && nested_u64(status, &["outputs", OUTPUT_NAME, "pending_messages"]) == Some(0)
                && status
                    .get("excluded_messages_total")
                    .and_then(Value::as_u64)
                    == Some((COMMITTED_CHUNK.len() + FAILED_CHUNK.len() + 1) as u64)
        },
        // The echo filter runs on a separate input task; wait for all executed PUBLISHes.
        "a later message to publish after the uncertain transaction",
        Duration::from_secs(10),
    )
    .await?;
    assert_no_output(&mut output, Duration::from_secs(1)).await?;
    let final_output = &final_status["outputs"][OUTPUT_NAME];
    ensure!(
        nested_str(final_output, &["state"]) == Some("running"),
        "{final_status}"
    );
    ensure!(
        metric_u64(final_output, "input_messages_total")
            == Some((COMMITTED_CHUNK.len() + FAILED_CHUNK.len() + 1) as u64),
        "{final_status}"
    );
    ensure!(
        metric_u64(final_output, "output_batches_total") == Some(2),
        "{final_status}"
    );
    ensure!(
        metric_u64(final_output, "output_messages_total")
            == Some((COMMITTED_CHUNK.len() + 1) as u64),
        "{final_status}"
    );
    ensure!(
        metric_u64(final_output, "pending_messages") == Some(0),
        "{final_status}"
    );
    ensure!(
        metric_u64(final_output, "pending_keys") == Some(0),
        "{final_status}"
    );
    ensure!(
        metric_u64(final_output, "pending_payload_bytes") == Some(0),
        "{final_status}"
    );
    ensure!(
        metric_u64(final_output, "uncertain_transactions_total") == Some(1),
        "{final_status}"
    );
    ensure!(
        metric_u64(final_output, "uncertain_messages_total") == Some(FAILED_CHUNK.len() as u64),
        "{final_status}"
    );
    ensure!(
        final_status
            .get("input_messages_total")
            .and_then(Value::as_u64)
            == Some((COMMITTED_CHUNK.len() + FAILED_CHUNK.len() + 1) as u64),
        "{final_status}"
    );
    ensure!(
        final_status
            .get("excluded_messages_total")
            .and_then(Value::as_u64)
            == Some((COMMITTED_CHUNK.len() + FAILED_CHUNK.len() + 1) as u64),
        "{final_status}"
    );
    ensure!(
        metric_u64(final_output, "publish_errors_total") == Some(1),
        "{final_status}"
    );
    ensure!(
        metric_u64(final_output, "publish_error_messages_total") == Some(FAILED_CHUNK.len() as u64),
        "{final_status}"
    );

    let proxy_stats = get_fault_proxy_stats(&config).await?;
    ensure!(
        proxy_stats.get("multi_exec_attempts") == Some(&3),
        "{proxy_stats:?}"
    );
    ensure!(
        proxy_stats.get("successful_execs") == Some(&3),
        "{proxy_stats:?}"
    );
    ensure!(
        proxy_stats.get("dropped_exec_replies") == Some(&1),
        "{proxy_stats:?}"
    );
    ensure!(
        proxy_stats.get("drop_publish_count") == Some(&(FAILED_CHUNK.len() as i64)),
        "{proxy_stats:?}"
    );
    ensure!(
        proxy_stats.get("execs_after_drop") == Some(&1),
        "{proxy_stats:?}"
    );
    ensure!(
        proxy_stats.get("publishes_after_drop") == Some(&1),
        "{proxy_stats:?}"
    );
    ensure!(
        committed_messages.len() == COMMITTED_CHUNK.len(),
        "{committed_messages:?}"
    );
    ensure!(
        failed_messages.len() == FAILED_CHUNK.len(),
        "{failed_messages:?}"
    );
    ensure!(later_messages.len() == 1, "{later_messages:?}");

    drop(output);
    println!(
        "Docker fault-injection test passed: Redis executed the ambiguous chunk once \
         before its reply was dropped; the app did not retry it, counted uncertainty, \
         and successfully published a later chunk without pausing the output."
    );
    Ok(())
}
