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
//!   Every frame is built with direct byte writes into one reused buffer, so
//!   the steady state allocates nothing per message. `LOOPBACK_PAYLOAD_BYTES`
//!   (default 0) sizes each burst payload as `{seq}` or, when set,
//!   `{seq}:xxx...` padded to exactly that many bytes.
//! * The output listener answers the service's multiplexed connection
//!   (`CLIENT SETINFO`, `MULTI`/`PUBLISH`/`EXEC`, standalone `PUBLISH`) and
//!   timestamps every publish it observes, validating the `replica-a:` channel
//!   prefix and parsing the payload sequence. Its RESP reader reuses its line
//!   and argument buffers across commands.
//!
//! When every burst message has come back through the bridge it prints a single
//! `loopback-e2e ...` summary line and exits 0. `LOOPBACK_TIMEOUT_S` guards the
//! whole run; sockets that close are treated as end-of-stream, never a panic.
//! The main task is woken by a `Notify` when the last message arrives instead
//! of polling, and the generator wakes the same way once warm-up completes.

use std::{
    env, io,
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
    sync::{Mutex, Notify},
    time,
};

/// Channel prefix the service maps every output publish to.
const OUTPUT_CHANNEL_PREFIX: &[u8] = b"replica-a:";
/// Warm-up payload prefix (`warm:<index>`); burst payloads start with digits.
const WARMUP_PAYLOAD_PREFIX: &[u8] = b"warm:";
/// Channels cycle through 16 tenants exactly like the hotpath benchmark.
const HOT_CHANNEL_COUNT: usize = 16;
/// Padding byte appended when `LOOPBACK_PAYLOAD_BYTES` is set.
const PAYLOAD_PADDING: u8 = b'x';

#[derive(Clone, Debug)]
struct Config {
    input_port: u16,
    output_port: u16,
    warmup: usize,
    messages: usize,
    chunk: usize,
    payload_bytes: usize,
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
            payload_bytes: env_parse("LOOPBACK_PAYLOAD_BYTES", 0),
            timeout: Duration::from_secs(env_parse("LOOPBACK_TIMEOUT_S", 120)),
        };
        ensure!(config.messages > 0, "LOOPBACK_MESSAGES must be positive");
        ensure!(config.chunk > 0, "LOOPBACK_CHUNK must be positive");
        ensure!(
            config.timeout > Duration::ZERO,
            "LOOPBACK_TIMEOUT_S must be positive"
        );
        let longest_prefix = decimal_len(config.messages - 1) + 1;
        ensure!(
            config.payload_bytes == 0 || config.payload_bytes >= longest_prefix,
            "LOOPBACK_PAYLOAD_BYTES must be 0 or at least {longest_prefix} to fit the longest sequence prefix"
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
    total: usize,
    warmup_expected: usize,
    warmup_received: AtomicUsize,
    warmup_done: Notify,
    received: AtomicUsize,
    done: Notify,
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
            total: messages,
            warmup_expected: warmup,
            warmup_received: AtomicUsize::new(0),
            warmup_done: Notify::new(),
            received: AtomicUsize::new(0),
            done: Notify::new(),
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
        if failure.is_some() {
            return;
        }
        *failure = Some(message.into());
        drop(failure);
        // Wake the main task immediately instead of waiting for the timeout.
        self.done.notify_one();
    }

    fn failure(&self) -> Option<String> {
        self.failure.lock().unwrap().clone()
    }

    fn elapsed_nanos(&self) -> u64 {
        self.base.elapsed().as_nanos() as u64
    }
}

/// Decimal digits needed to print `value` (`0` needs one).
fn decimal_len(value: usize) -> usize {
    if value == 0 {
        1
    } else {
        value.ilog10() as usize + 1
    }
}

/// Appends the decimal digits of `value`, zero-padded to at least `min_width`,
/// without allocating a `String`.
fn write_usize_padded(buffer: &mut Vec<u8>, mut value: usize, min_width: usize) {
    let mut digits = [0u8; 20];
    let mut index = digits.len();
    loop {
        index -= 1;
        digits[index] = b'0' + (value % 10) as u8;
        value /= 10;
        if value == 0 {
            break;
        }
    }
    let width = (digits.len() - index).max(min_width);
    let start = digits.len() - width;
    digits[start..index].fill(b'0');
    buffer.extend_from_slice(&digits[start..]);
}

fn write_usize(buffer: &mut Vec<u8>, value: usize) {
    write_usize_padded(buffer, value, 0);
}

fn write_bulk(buffer: &mut Vec<u8>, data: &[u8]) {
    buffer.push(b'$');
    write_usize(buffer, data.len());
    buffer.extend_from_slice(b"\r\n");
    buffer.extend_from_slice(data);
    buffer.extend_from_slice(b"\r\n");
}

fn write_integer(buffer: &mut Vec<u8>, value: usize) {
    buffer.push(b':');
    write_usize(buffer, value);
    buffer.extend_from_slice(b"\r\n");
}

/// Reusable RESP2 command reader: the line and argument buffers survive across
/// commands, so steady-state parsing performs no per-command allocations.
struct CommandReader<R> {
    reader: R,
    line: Vec<u8>,
    args: Vec<Vec<u8>>,
    tag: [u8; 1],
    crlf: [u8; 2],
}

impl<R> CommandReader<R>
where
    R: AsyncBufRead + Unpin,
{
    fn new(reader: R) -> Self {
        Self {
            reader,
            line: Vec::new(),
            args: Vec::new(),
            tag: [0u8; 1],
            crlf: [0u8; 2],
        }
    }

    /// Reads one `*N` array of bulk strings. `Ok(None)` means the peer closed
    /// the connection or sent something unparsable; either way the handler
    /// stops. The returned slice stays valid until the next call.
    async fn read_command(&mut self) -> io::Result<Option<&[Vec<u8>]>> {
        if self.reader.read_exact(&mut self.tag).await.is_err() {
            return Ok(None);
        }
        if self.tag[0] != b'*' {
            return Ok(None);
        }
        read_line_into(&mut self.reader, &mut self.line).await?;
        let count = parse_length(&self.line)?;
        for index in 0..count {
            if index == self.args.len() {
                self.args.push(Vec::new());
            }
            self.reader.read_exact(&mut self.tag).await?;
            if self.tag[0] != b'$' {
                return Ok(None);
            }
            read_line_into(&mut self.reader, &mut self.line).await?;
            let length = parse_length(&self.line)?;
            let argument = &mut self.args[index];
            argument.clear();
            argument.resize(length, 0);
            self.reader.read_exact(argument).await?;
            self.reader.read_exact(&mut self.crlf).await?;
        }
        Ok(Some(&self.args[..count]))
    }
}

/// Reads a CRLF-terminated line into `buffer` (reused across calls) and strips
/// the terminator.
async fn read_line_into<R>(reader: &mut R, buffer: &mut Vec<u8>) -> io::Result<()>
where
    R: AsyncBufRead + Unpin,
{
    buffer.clear();
    if reader.read_until(b'\n', buffer).await? == 0 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "connection closed mid-line",
        ));
    }
    while matches!(buffer.last(), Some(b'\n' | b'\r')) {
        buffer.pop();
    }
    Ok(())
}

fn parse_length(line: &[u8]) -> io::Result<usize> {
    std::str::from_utf8(line)
        .ok()
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid RESP length"))
}

/// Appends the `hot:tenant-XX:item-NNNN` channel bulk string, matching the
/// hotpath benchmark's channel layout without allocating a `String`.
fn append_hot_channel(buffer: &mut Vec<u8>, sequence: usize) {
    const PREFIX: &[u8] = b"hot:tenant-";
    const MIDDLE: &[u8] = b":item-";
    let channel_len = PREFIX.len() + 2 + MIDDLE.len() + decimal_len(sequence).max(4);
    buffer.push(b'$');
    write_usize(buffer, channel_len);
    buffer.extend_from_slice(b"\r\n");
    buffer.extend_from_slice(PREFIX);
    write_usize_padded(buffer, sequence % HOT_CHANNEL_COUNT, 2);
    buffer.extend_from_slice(MIDDLE);
    write_usize_padded(buffer, sequence, 4);
    buffer.extend_from_slice(b"\r\n");
}

/// Appends the RESP array header plus the pattern/channel bulks of one
/// `pmessage`/`message` frame.
fn append_message_header(buffer: &mut Vec<u8>, subscription: &Subscription, sequence: usize) {
    match subscription {
        Subscription::Pattern(pattern) => {
            buffer.extend_from_slice(b"*4\r\n$8\r\npmessage\r\n");
            write_bulk(buffer, pattern);
        }
        Subscription::Channel(channel) => {
            buffer.extend_from_slice(b"*3\r\n$7\r\nmessage\r\n");
            write_bulk(buffer, channel);
        }
    }
    append_hot_channel(buffer, sequence);
}

/// Appends the warm-up payload bulk (`warm:<index>`).
fn append_warmup_payload(buffer: &mut Vec<u8>, index: usize) {
    let payload_len = WARMUP_PAYLOAD_PREFIX.len() + decimal_len(index);
    buffer.push(b'$');
    write_usize(buffer, payload_len);
    buffer.extend_from_slice(b"\r\n");
    buffer.extend_from_slice(WARMUP_PAYLOAD_PREFIX);
    write_usize(buffer, index);
    buffer.extend_from_slice(b"\r\n");
}

/// Appends the burst payload bulk: `{sequence}` when `payload_bytes` is 0, or
/// `{sequence}:xxx...` padded to exactly `payload_bytes` otherwise.
fn append_burst_payload(buffer: &mut Vec<u8>, sequence: usize, payload_bytes: usize) {
    buffer.push(b'$');
    if payload_bytes == 0 {
        write_usize(buffer, decimal_len(sequence));
        buffer.extend_from_slice(b"\r\n");
        write_usize(buffer, sequence);
    } else {
        write_usize(buffer, payload_bytes);
        buffer.extend_from_slice(b"\r\n");
        write_usize(buffer, sequence);
        buffer.push(b':');
        let padding = payload_bytes - decimal_len(sequence) - 1;
        buffer.resize(buffer.len() + padding, PAYLOAD_PADDING);
    }
    buffer.extend_from_slice(b"\r\n");
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
    let mut reader = CommandReader::new(BufReader::new(read_half));
    let mut subscription_count = 0usize;
    while let Ok(Some(args)) = reader.read_command().await {
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

/// Waits until the output listener has observed every warm-up message, waking
/// on `Notify` instead of polling.
async fn wait_for_warmup(shared: &Shared) -> Result<()> {
    loop {
        let notified = shared.warmup_done.notified();
        if shared.warmup_received.load(Ordering::SeqCst) >= shared.warmup_expected {
            return Ok(());
        }
        if let Some(failure) = shared.failure() {
            anyhow::bail!("{failure}");
        }
        notified.await;
    }
}

async fn generate_messages(
    writer: Arc<Mutex<OwnedWriteHalf>>,
    subscription: Subscription,
    config: Arc<Config>,
    shared: Arc<Shared>,
) -> Result<()> {
    let mut buffer = Vec::with_capacity(config.warmup.saturating_mul(96));
    for index in 0..config.warmup {
        append_message_header(&mut buffer, &subscription, index);
        append_warmup_payload(&mut buffer, index);
    }
    writer
        .lock()
        .await
        .write_all(&buffer)
        .await
        .context("failed to send warm-up messages")?;

    wait_for_warmup(&shared).await?;

    let frame_bytes = 96usize.saturating_add(config.payload_bytes);
    buffer.clear();
    buffer.reserve(config.chunk.saturating_mul(frame_bytes));
    let mut sequence = 0usize;
    while sequence < config.messages {
        let chunk_end = (sequence + config.chunk).min(config.messages);
        buffer.clear();
        for current in sequence..chunk_end {
            shared.t0[current].store(shared.elapsed_nanos(), Ordering::Relaxed);
            append_message_header(&mut buffer, &subscription, current);
            append_burst_payload(&mut buffer, current, config.payload_bytes);
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
    let mut reader = CommandReader::new(BufReader::new(read_half));
    let mut in_multi = false;
    let mut queued = 0usize;
    let mut exec_response = Vec::with_capacity(64);
    while let Ok(Some(args)) = reader.read_command().await {
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
            exec_response.clear();
            exec_response.push(b'*');
            write_usize(&mut exec_response, queued);
            exec_response.extend_from_slice(b"\r\n");
            for _ in 0..queued {
                exec_response.extend_from_slice(b":1\r\n");
            }
            if queued > 0 {
                shared.batches.fetch_add(1, Ordering::Relaxed);
            }
            in_multi = false;
            queued = 0;
            if writer.write_all(&exec_response).await.is_err() {
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

/// Parses the leading decimal sequence of a burst payload, requiring the
/// `:`-separated padding suffix of exactly `payload_bytes` bytes when
/// `LOOPBACK_PAYLOAD_BYTES` is configured.
fn parse_sequence(payload: &[u8], payload_bytes: usize) -> Option<usize> {
    let digits_end = payload
        .iter()
        .position(|byte| !byte.is_ascii_digit())
        .unwrap_or(payload.len());
    if digits_end == 0 {
        return None;
    }
    if payload_bytes == 0 {
        if digits_end != payload.len() {
            return None;
        }
    } else {
        if payload.len() != payload_bytes {
            return None;
        }
        let padding = &payload[digits_end..];
        if padding.first() != Some(&b':')
            || !padding[1..].iter().all(|&byte| byte == PAYLOAD_PADDING)
        {
            return None;
        }
    }
    let mut sequence = 0usize;
    for &byte in &payload[..digits_end] {
        sequence = sequence
            .checked_mul(10)?
            .checked_add(usize::from(byte - b'0'))?;
    }
    Some(sequence)
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
        let warmup_received = shared.warmup_received.fetch_add(1, Ordering::SeqCst) + 1;
        if warmup_received >= shared.warmup_expected {
            shared.warmup_done.notify_one();
        }
        return;
    }
    let sequence = match parse_sequence(payload, config.payload_bytes) {
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
        let received = shared.received.fetch_add(1, Ordering::SeqCst) + 1;
        // Wake the main task only when the final message arrives.
        if received >= shared.total {
            shared.done.notify_one();
        }
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
        // Register interest before re-checking so no wake-up can be lost.
        let notified = shared.done.notified();
        if let Some(failure) = shared.failure() {
            anyhow::bail!("{failure}");
        }
        let received = shared.received.load(Ordering::SeqCst);
        if received >= config.messages {
            break;
        }
        tokio::select! {
            () = notified => {}
            () = time::sleep_until(deadline) => {
                anyhow::bail!(
                    "loopback benchmark timed out after {:?}: received {received}/{} messages",
                    config.timeout,
                    config.messages
                );
            }
        }
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
