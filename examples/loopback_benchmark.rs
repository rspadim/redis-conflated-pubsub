//! Loopback benchmark for `compose.loopback.test.yml`.
//!
//! This example replaces Redis/Valkey with two in-process fake RESP brokers so
//! only the bridge's own input fan-out and output publish path are measured:
//!
//! * The input listener speaks just enough RESP2 for the service's async
//!   `PubSub` connection (setup `CLIENT SETINFO`, then `PSUBSCRIBE`). Once the
//!   subscription is acknowledged it warms up `LOOPBACK_WARMUP` channels,
//!   waits for the output listener to observe them, and then streams
//!   `LOOPBACK_MESSAGES` `pmessage` frames as fast as the socket accepts them
//!   in `LOOPBACK_CHUNK`-sized writes, timestamping every generated sequence.
//! * The output listener answers the service's multiplexed connection
//!   (`CLIENT SETINFO`, `MULTI`/`PUBLISH`/`EXEC`, standalone `PUBLISH`) and
//!   timestamps every publish it observes, validating the `replica-a:` channel
//!   prefix and parsing the payload sequence.
//!
//! When every burst message has come back through the bridge it prints a single
//! `loopback-e2e ...` summary line and exits 0. `LOOPBACK_TIMEOUT_S` guards the
//! whole run; sockets that close are treated as end-of-stream, never a panic.

use std::{
    env,
    io::{self, Write as _},
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result, ensure};
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream, tcp::OwnedWriteHalf},
    sync::Mutex,
    time,
};

/// Channel prefix the service maps every output publish to.
const OUTPUT_CHANNEL_PREFIX: &[u8] = b"replica-a:";
/// Warm-up payload prefix (`warm:<index>`); burst payloads are bare integers.
const WARMUP_PAYLOAD_PREFIX: &[u8] = b"warm:";
/// Channels cycle through 16 tenants exactly like the hotpath benchmark.
const HOT_CHANNEL_COUNT: usize = 16;

#[derive(Clone, Debug)]
struct Config {
    input_port: u16,
    output_port: u16,
    warmup: usize,
    messages: usize,
    chunk: usize,
    timeout: Duration,
}

impl Config {
    fn from_env() -> Result<Self> {
        let config = Self {
            input_port: env_parse("LOOPBACK_INPUT_PORT", 6379),
            output_port: env_parse("LOOPBACK_OUTPUT_PORT", 6380),
            warmup: env_parse("LOOPBACK_WARMUP", 64),
            messages: env_parse("LOOPBACK_MESSAGES", 1_000_000),
            chunk: env_parse("LOOPBACK_CHUNK", 256),
            timeout: Duration::from_secs(env_parse("LOOPBACK_TIMEOUT_S", 120)),
        };
        ensure!(config.messages > 0, "LOOPBACK_MESSAGES must be positive");
        ensure!(config.chunk > 0, "LOOPBACK_CHUNK must be positive");
        ensure!(
            config.timeout > Duration::ZERO,
            "LOOPBACK_TIMEOUT_S must be positive"
        );
        Ok(config)
    }
}

fn env_parse<T: std::str::FromStr>(name: &str, default: T) -> T {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn hot_channel(sequence: usize) -> String {
    format!(
        "hot:tenant-{:02}:item-{:04}",
        sequence % HOT_CHANNEL_COUNT,
        sequence
    )
}

fn percentile_ms(sorted: &[u64], percent: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let index = ((percent * sorted.len() as f64).ceil() as usize).saturating_sub(1);
    sorted[index] as f64 / 1_000_000.0
}

/// State shared by both fake brokers, the generator, and the main task.
struct Shared {
    base: Instant,
    warmup_expected: usize,
    warmup_received: AtomicUsize,
    received: AtomicUsize,
    t0: Vec<AtomicU64>,
    recv: Vec<AtomicU64>,
    batches: AtomicU64,
    publishes: AtomicU64,
    unknown_commands: AtomicU64,
    failure: StdMutex<Option<String>>,
}

impl Shared {
    fn new(messages: usize, warmup: usize, base: Instant) -> Self {
        Self {
            base,
            warmup_expected: warmup,
            warmup_received: AtomicUsize::new(0),
            received: AtomicUsize::new(0),
            t0: (0..messages).map(|_| AtomicU64::new(0)).collect(),
            recv: (0..messages).map(|_| AtomicU64::new(0)).collect(),
            batches: AtomicU64::new(0),
            publishes: AtomicU64::new(0),
            unknown_commands: AtomicU64::new(0),
            failure: StdMutex::new(None),
        }
    }

    fn fail(&self, message: impl Into<String>) {
        let mut failure = self.failure.lock().unwrap();
        if failure.is_none() {
            *failure = Some(message.into());
        }
    }

    fn failure(&self) -> Option<String> {
        self.failure.lock().unwrap().clone()
    }

    fn elapsed_nanos(&self) -> u64 {
        self.base.elapsed().as_nanos() as u64
    }
}

/// RESP2 command decoded from a client connection.
type Command = Vec<Vec<u8>>;

async fn read_line_value<R>(reader: &mut R) -> io::Result<Vec<u8>>
where
    R: AsyncBufRead + Unpin,
{
    let mut line = Vec::new();
    if reader.read_until(b'\n', &mut line).await? == 0 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "connection closed mid-line",
        ));
    }
    while matches!(line.last(), Some(b'\n' | b'\r')) {
        line.pop();
    }
    Ok(line)
}

fn parse_length(line: &[u8]) -> io::Result<usize> {
    std::str::from_utf8(line)
        .ok()
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid RESP length"))
}

/// Reads one `*N` array of bulk strings. `Ok(None)` means the peer closed the
/// connection or sent something unparsable; either way the handler stops.
async fn read_command<R>(reader: &mut R) -> io::Result<Option<Command>>
where
    R: AsyncBufRead + Unpin,
{
    let mut tag = [0u8; 1];
    if reader.read_exact(&mut tag).await.is_err() {
        return Ok(None);
    }
    if tag[0] != b'*' {
        return Ok(None);
    }
    let count = parse_length(&read_line_value(reader).await?)?;
    let mut args = Vec::with_capacity(count);
    for _ in 0..count {
        let mut bulk_tag = [0u8; 1];
        reader.read_exact(&mut bulk_tag).await?;
        if bulk_tag[0] != b'$' {
            return Ok(None);
        }
        let length = parse_length(&read_line_value(reader).await?)?;
        let mut data = vec![0u8; length];
        reader.read_exact(&mut data).await?;
        let mut crlf = [0u8; 2];
        reader.read_exact(&mut crlf).await?;
        args.push(data);
    }
    Ok(Some(args))
}

fn write_bulk(buffer: &mut Vec<u8>, data: &[u8]) {
    write!(buffer, "${}\r\n", data.len()).expect("writing to a Vec cannot fail");
    buffer.extend_from_slice(data);
    buffer.extend_from_slice(b"\r\n");
}

fn write_integer(buffer: &mut Vec<u8>, value: usize) {
    write!(buffer, ":{value}\r\n").expect("writing to a Vec cannot fail");
}

async fn run_input_listener(listener: TcpListener, config: Arc<Config>, shared: Arc<Shared>) {
    let started = Arc::new(AtomicBool::new(false));
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                tokio::spawn(handle_input_connection(
                    stream,
                    Arc::clone(&config),
                    Arc::clone(&shared),
                    Arc::clone(&started),
                ));
            }
            Err(_) => time::sleep(Duration::from_millis(50)).await,
        }
    }
}

async fn handle_input_connection(
    stream: TcpStream,
    config: Arc<Config>,
    shared: Arc<Shared>,
    started: Arc<AtomicBool>,
) {
    let _ = stream.set_nodelay(true);
    let (read_half, write_half) = stream.into_split();
    let writer = Arc::new(Mutex::new(write_half));
    let mut reader = BufReader::new(read_half);
    let mut subscription_count = 0usize;
    while let Ok(Some(args)) = read_command(&mut reader).await {
        let Some(command) = args.first() else {
            continue;
        };
        if command.eq_ignore_ascii_case(b"PSUBSCRIBE") || command.eq_ignore_ascii_case(b"SUBSCRIBE")
        {
            let is_pattern = command.eq_ignore_ascii_case(b"PSUBSCRIBE");
            for target in args.iter().skip(1) {
                subscription_count += 1;
                let ack = if is_pattern {
                    "psubscribe"
                } else {
                    "subscribe"
                };
                let mut response = Vec::with_capacity(target.len() + 32);
                response.extend_from_slice(b"*3\r\n");
                write_bulk(&mut response, ack.as_bytes());
                write_bulk(&mut response, target);
                write_integer(&mut response, subscription_count);
                if writer.lock().await.write_all(&response).await.is_err() {
                    return;
                }
                if !started.swap(true, Ordering::SeqCst) {
                    let subscription = if is_pattern {
                        Subscription::Pattern(target.clone())
                    } else {
                        Subscription::Channel(target.clone())
                    };
                    tokio::spawn(run_generator(
                        Arc::clone(&writer),
                        subscription,
                        Arc::clone(&config),
                        Arc::clone(&shared),
                    ));
                }
            }
        } else if command.eq_ignore_ascii_case(b"PING") {
            let _ = writer.lock().await.write_all(b"+PONG\r\n").await;
        } else if command.eq_ignore_ascii_case(b"CLIENT") || command.eq_ignore_ascii_case(b"SELECT")
        {
            let _ = writer.lock().await.write_all(b"+OK\r\n").await;
        } else {
            shared.unknown_commands.fetch_add(1, Ordering::Relaxed);
            let _ = writer.lock().await.write_all(b"+OK\r\n").await;
        }
    }
}

#[derive(Clone, Debug)]
enum Subscription {
    Pattern(Vec<u8>),
    Channel(Vec<u8>),
}

fn append_message(
    buffer: &mut Vec<u8>,
    subscription: &Subscription,
    channel: &str,
    payload: &[u8],
) {
    match subscription {
        Subscription::Pattern(pattern) => {
            buffer.extend_from_slice(b"*4\r\n$8\r\npmessage\r\n");
            write_bulk(buffer, pattern);
        }
        Subscription::Channel(channel_name) => {
            buffer.extend_from_slice(b"*3\r\n$7\r\nmessage\r\n");
            write_bulk(buffer, channel_name);
        }
    }
    write_bulk(buffer, channel.as_bytes());
    write_bulk(buffer, payload);
}

async fn run_generator(
    writer: Arc<Mutex<OwnedWriteHalf>>,
    subscription: Subscription,
    config: Arc<Config>,
    shared: Arc<Shared>,
) {
    if let Err(error) = generate_messages(writer, subscription, config, Arc::clone(&shared)).await {
        shared.fail(format!("message generator failed: {error:#}"));
    }
}

async fn generate_messages(
    writer: Arc<Mutex<OwnedWriteHalf>>,
    subscription: Subscription,
    config: Arc<Config>,
    shared: Arc<Shared>,
) -> Result<()> {
    let mut warmup = Vec::with_capacity(config.warmup * 96);
    for index in 0..config.warmup {
        let payload = format!("warm:{index}");
        append_message(
            &mut warmup,
            &subscription,
            &hot_channel(index),
            payload.as_bytes(),
        );
    }
    writer
        .lock()
        .await
        .write_all(&warmup)
        .await
        .context("failed to send warm-up messages")?;

    while shared.warmup_received.load(Ordering::SeqCst) < shared.warmup_expected {
        if let Some(failure) = shared.failure() {
            anyhow::bail!("{failure}");
        }
        time::sleep(Duration::from_millis(1)).await;
    }

    let mut sequence = 0usize;
    while sequence < config.messages {
        let chunk_end = (sequence + config.chunk).min(config.messages);
        let mut buffer = Vec::with_capacity(config.chunk * 96);
        for current in sequence..chunk_end {
            shared.t0[current].store(shared.elapsed_nanos(), Ordering::Relaxed);
            append_message(
                &mut buffer,
                &subscription,
                &hot_channel(current),
                current.to_string().as_bytes(),
            );
        }
        writer
            .lock()
            .await
            .write_all(&buffer)
            .await
            .context("failed to send burst chunk")?;
        sequence = chunk_end;
    }
    Ok(())
}

async fn run_output_listener(listener: TcpListener, config: Arc<Config>, shared: Arc<Shared>) {
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                tokio::spawn(handle_output_connection(
                    stream,
                    Arc::clone(&config),
                    Arc::clone(&shared),
                ));
            }
            Err(_) => time::sleep(Duration::from_millis(50)).await,
        }
    }
}

async fn handle_output_connection(stream: TcpStream, config: Arc<Config>, shared: Arc<Shared>) {
    let _ = stream.set_nodelay(true);
    let (read_half, mut writer) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let mut in_multi = false;
    let mut queued = 0usize;
    while let Ok(Some(args)) = read_command(&mut reader).await {
        let Some(command) = args.first() else {
            continue;
        };
        if command.eq_ignore_ascii_case(b"MULTI") {
            in_multi = true;
            queued = 0;
            if writer.write_all(b"+OK\r\n").await.is_err() {
                return;
            }
        } else if command.eq_ignore_ascii_case(b"EXEC") {
            let mut response = Vec::with_capacity(queued * 4 + 8);
            write!(response, "*{queued}\r\n").expect("writing to a Vec cannot fail");
            for _ in 0..queued {
                response.extend_from_slice(b":1\r\n");
            }
            if queued > 0 {
                shared.batches.fetch_add(1, Ordering::Relaxed);
            }
            in_multi = false;
            queued = 0;
            if writer.write_all(&response).await.is_err() {
                return;
            }
        } else if command.eq_ignore_ascii_case(b"DISCARD") {
            in_multi = false;
            queued = 0;
            if writer.write_all(b"+OK\r\n").await.is_err() {
                return;
            }
        } else if command.eq_ignore_ascii_case(b"PUBLISH") {
            if args.len() < 3 {
                if writer
                    .write_all(b"-ERR wrong number of arguments for 'publish' command\r\n")
                    .await
                    .is_err()
                {
                    return;
                }
                continue;
            }
            shared.publishes.fetch_add(1, Ordering::Relaxed);
            record_output_message(&args[1], &args[2], &config, &shared);
            if in_multi {
                queued += 1;
                if writer.write_all(b"+QUEUED\r\n").await.is_err() {
                    return;
                }
            } else {
                shared.batches.fetch_add(1, Ordering::Relaxed);
                if writer.write_all(b":1\r\n").await.is_err() {
                    return;
                }
            }
        } else if command.eq_ignore_ascii_case(b"PING") {
            if writer.write_all(b"+PONG\r\n").await.is_err() {
                return;
            }
        } else if command.eq_ignore_ascii_case(b"CLIENT") || command.eq_ignore_ascii_case(b"SELECT")
        {
            if writer.write_all(b"+OK\r\n").await.is_err() {
                return;
            }
        } else {
            shared.unknown_commands.fetch_add(1, Ordering::Relaxed);
            if writer.write_all(b"+OK\r\n").await.is_err() {
                return;
            }
        }
    }
}

/// Timestamps one output publish, validating the mapped channel prefix and
/// classifying the payload as warm-up or burst sequence.
fn record_output_message(channel: &[u8], payload: &[u8], config: &Config, shared: &Shared) {
    if !channel.starts_with(OUTPUT_CHANNEL_PREFIX) {
        shared.fail(format!(
            "unexpected output channel: {}",
            String::from_utf8_lossy(channel)
        ));
        return;
    }
    if let Some(rest) = payload.strip_prefix(WARMUP_PAYLOAD_PREFIX)
        && !rest.is_empty()
        && rest.iter().all(u8::is_ascii_digit)
    {
        shared.warmup_received.fetch_add(1, Ordering::SeqCst);
        return;
    }
    let sequence = match std::str::from_utf8(payload)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
    {
        Some(sequence) if sequence < config.messages => sequence,
        _ => {
            shared.fail(format!(
                "unexpected output payload: {}",
                String::from_utf8_lossy(payload)
            ));
            return;
        }
    };
    let received_at = shared.elapsed_nanos();
    if shared.recv[sequence].swap(received_at, Ordering::SeqCst) == 0 {
        shared.received.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = Arc::new(Config::from_env()?);
    let base = Instant::now();
    let shared = Arc::new(Shared::new(config.messages, config.warmup, base));

    let input_listener = TcpListener::bind(("0.0.0.0", config.input_port))
        .await
        .with_context(|| format!("failed to bind input port {}", config.input_port))?;
    let output_listener = TcpListener::bind(("0.0.0.0", config.output_port))
        .await
        .with_context(|| format!("failed to bind output port {}", config.output_port))?;
    tokio::spawn(run_input_listener(
        input_listener,
        Arc::clone(&config),
        Arc::clone(&shared),
    ));
    tokio::spawn(run_output_listener(
        output_listener,
        Arc::clone(&config),
        Arc::clone(&shared),
    ));

    let deadline = time::Instant::now() + config.timeout;
    loop {
        if let Some(failure) = shared.failure() {
            anyhow::bail!("{failure}");
        }
        let received = shared.received.load(Ordering::SeqCst);
        if received >= config.messages {
            break;
        }
        if time::Instant::now() >= deadline {
            anyhow::bail!(
                "loopback benchmark timed out after {:?}: received {received}/{} messages",
                config.timeout,
                config.messages
            );
        }
        time::sleep(Duration::from_millis(5)).await;
    }

    let mut latencies = Vec::with_capacity(config.messages);
    let mut first_started = u64::MAX;
    let mut last_received = 0u64;
    for sequence in 0..config.messages {
        let started = shared.t0[sequence].load(Ordering::SeqCst);
        let received = shared.recv[sequence].load(Ordering::SeqCst);
        ensure!(
            started > 0 && received >= started,
            "message {sequence} was not timed correctly"
        );
        first_started = first_started.min(started);
        last_received = last_received.max(received);
        latencies.push(received - started);
    }
    latencies.sort_unstable();

    let elapsed_s = (last_received - first_started) as f64 / 1_000_000_000.0;
    let messages_per_second = config.messages as f64 / elapsed_s;
    println!(
        "loopback-e2e messages={} elapsed_s={elapsed_s:.3} msg_per_s={messages_per_second:.0} e2e_p50_p95_p99_ms={:.3}/{:.3}/{:.3} batches={} publishes={}",
        config.messages,
        percentile_ms(&latencies, 0.50),
        percentile_ms(&latencies, 0.95),
        percentile_ms(&latencies, 0.99),
        shared.batches.load(Ordering::Relaxed),
        shared.publishes.load(Ordering::Relaxed),
    );
    let unknown_commands = shared.unknown_commands.load(Ordering::Relaxed);
    if unknown_commands > 0 {
        println!("loopback-unknown-commands n={unknown_commands}");
    }
    Ok(())
}
