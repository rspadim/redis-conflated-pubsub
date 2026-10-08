//! Rust replacements for `tests/redis_fault_proxy.py` and
//! `tests/redis_counting_proxy.py`.
//!
//! Both proxies accept raw RESP on `PROXY_PORT`, forward every frame to Redis
//! unmodified, and expose their counters on `STATS_PORT` after a `STATS\r\n`
//! request. The fault proxy additionally closes the client socket right after
//! consuming the Nth successful `EXEC` reply, so the application cannot tell
//! whether Redis committed the transaction. The counting proxy tags traffic by
//! the database each output `SELECT`s and reports per-output command counts and
//! exact RESP request sizes as compact JSON.

use std::{
    collections::BTreeMap,
    env,
    future::Future,
    io,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, Result, anyhow};
use serde::Serialize;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, copy},
    net::{
        TcpListener, TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    time::timeout,
};

use crate::resp::{RespValue, read_command, read_frame};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const STATS_TIMEOUT: Duration = Duration::from_secs(3);
const OUTPUT_NAMES: [&str; 5] = ["chunked", "send", "truncate", "drop", "immediate"];

fn env_parse<T>(name: &str, default: T) -> Result<T>
where
    T: std::str::FromStr,
{
    match env::var(name) {
        Ok(value) => value
            .parse()
            .map_err(|_| anyhow!("{name} must be a valid value")),
        Err(_) => Ok(default),
    }
}

async fn serve_connections<H, F>(listener: TcpListener, handler: H) -> Result<()>
where
    H: Fn(TcpStream) -> F + Clone + Send + 'static,
    F: Future<Output = ()> + Send + 'static,
{
    loop {
        let (stream, _) = listener
            .accept()
            .await
            .context("failed to accept a proxy connection")?;
        let handler = handler.clone();
        tokio::spawn(handler(stream));
    }
}

fn is_disconnect(error: &anyhow::Error) -> bool {
    matches!(
        error.downcast_ref::<io::Error>().map(io::Error::kind),
        Some(
            io::ErrorKind::UnexpectedEof
                | io::ErrorKind::ConnectionReset
                | io::ErrorKind::ConnectionAborted
                | io::ErrorKind::BrokenPipe
        )
    )
}

// ---------------------------------------------------------------------------
// Fault proxy
// ---------------------------------------------------------------------------

struct FaultProxyConfig {
    redis_host: String,
    redis_port: u16,
    proxy_port: u16,
    stats_port: u16,
    fault_on_successful_exec_number: u64,
}

impl FaultProxyConfig {
    fn from_env() -> Result<Self> {
        Ok(Self {
            redis_host: env::var("REDIS_HOST").unwrap_or_else(|_| "redis".to_owned()),
            redis_port: env_parse("REDIS_PORT", 6379)?,
            proxy_port: env_parse("PROXY_PORT", 6379)?,
            stats_port: env_parse("STATS_PORT", 9091)?,
            fault_on_successful_exec_number: env_parse("FAULT_ON_SUCCESSFUL_EXEC_NUMBER", 2)?,
        })
    }
}

#[derive(Clone, Default)]
struct FaultStats {
    accepted_connections: u64,
    multi_exec_attempts: u64,
    successful_execs: u64,
    dropped_exec_replies: u64,
    drop_publish_count: u64,
    execs_after_drop: u64,
    publishes_after_drop: u64,
}

impl FaultStats {
    fn to_lines(&self) -> String {
        format!(
            "accepted_connections={}\n\
             multi_exec_attempts={}\n\
             successful_execs={}\n\
             dropped_exec_replies={}\n\
             drop_publish_count={}\n\
             execs_after_drop={}\n\
             publishes_after_drop={}\n\n",
            self.accepted_connections,
            self.multi_exec_attempts,
            self.successful_execs,
            self.dropped_exec_replies,
            self.drop_publish_count,
            self.execs_after_drop,
            self.publishes_after_drop,
        )
    }
}

pub async fn run_fault_proxy() -> Result<()> {
    let config = Arc::new(FaultProxyConfig::from_env()?);
    let state = Arc::new(Mutex::new(FaultStats::default()));

    let redis_listener = TcpListener::bind(("0.0.0.0", config.proxy_port))
        .await
        .with_context(|| format!("failed to bind fault proxy port {}", config.proxy_port))?;
    let stats_listener = TcpListener::bind(("0.0.0.0", config.stats_port))
        .await
        .with_context(|| format!("failed to bind fault stats port {}", config.stats_port))?;

    let proxy_handler = {
        let config = Arc::clone(&config);
        let state = Arc::clone(&state);
        move |stream: TcpStream| {
            let config = Arc::clone(&config);
            let state = Arc::clone(&state);
            async move {
                if let Err(error) = fault_connection(stream, &config, &state).await
                    && !is_disconnect(&error)
                {
                    eprintln!("fault-proxy connection error: {error:#}");
                }
            }
        }
    };
    let stats_handler = {
        let state = Arc::clone(&state);
        move |stream: TcpStream| {
            let state = Arc::clone(&state);
            async move {
                fault_stats_connection(stream, &state).await;
            }
        }
    };

    tokio::try_join!(
        serve_connections(redis_listener, proxy_handler),
        serve_connections(stats_listener, stats_handler),
    )?;
    Ok(())
}

async fn fault_connection(
    stream: TcpStream,
    config: &FaultProxyConfig,
    state: &Mutex<FaultStats>,
) -> Result<()> {
    let upstream = timeout(
        CONNECT_TIMEOUT,
        TcpStream::connect((config.redis_host.as_str(), config.redis_port)),
    )
    .await
    .context("timed out connecting to Redis")?
    .context("failed to connect to Redis")?;
    stream.set_nodelay(true).ok();
    upstream.set_nodelay(true).ok();

    state
        .lock()
        .expect("fault stats mutex poisoned")
        .accepted_connections += 1;

    let (client_read, mut client_write) = stream.into_split();
    let (upstream_read, mut upstream_write) = upstream.into_split();
    let mut client_reader = BufReader::new(client_read);
    let mut upstream_reader = BufReader::new(upstream_read);

    let mut in_multi = false;
    let mut transaction_publish_count = 0_u64;

    loop {
        let (raw_command, arguments) = read_command(&mut client_reader).await?;
        let command = arguments
            .first()
            .context("expected a RESP array command")?
            .to_ascii_uppercase();

        upstream_write.write_all(&raw_command).await?;
        if matches!(
            command.as_slice(),
            b"SUBSCRIBE" | b"PSUBSCRIBE" | b"SSUBSCRIBE"
        ) {
            relay_bidirectionally(
                &mut client_reader,
                &mut client_write,
                &mut upstream_reader,
                &mut upstream_write,
            )
            .await;
            return Ok(());
        }

        // Consume the complete EXEC reply before closing the app-side socket.
        let reply = read_frame(&mut upstream_reader).await?;

        if command == b"MULTI" {
            in_multi = true;
            transaction_publish_count = 0;
        } else if command == b"PUBLISH" && in_multi {
            transaction_publish_count += 1;
            let mut stats = state.lock().expect("fault stats mutex poisoned");
            if stats.dropped_exec_replies != 0 {
                stats.publishes_after_drop += 1;
            }
        }

        let mut should_drop_reply = false;
        if command == b"EXEC" {
            let successful_exec = matches!(reply.value, RespValue::Array(Some(_)));
            let mut stats = state.lock().expect("fault stats mutex poisoned");
            stats.multi_exec_attempts += 1;
            if successful_exec {
                stats.successful_execs += 1;
            }
            if stats.dropped_exec_replies != 0 {
                stats.execs_after_drop += 1;
            } else if successful_exec
                && stats.successful_execs == config.fault_on_successful_exec_number
            {
                stats.dropped_exec_replies = 1;
                stats.drop_publish_count = transaction_publish_count;
                should_drop_reply = true;
            }
            drop(stats);
            in_multi = false;
            transaction_publish_count = 0;
        }

        if should_drop_reply {
            return Ok(());
        }
        client_write.write_all(&reply.raw).await?;
    }
}

async fn relay_bidirectionally(
    client_reader: &mut BufReader<OwnedReadHalf>,
    client_write: &mut OwnedWriteHalf,
    upstream_reader: &mut BufReader<OwnedReadHalf>,
    upstream_write: &mut OwnedWriteHalf,
) {
    tokio::select! {
        _ = copy(client_reader, upstream_write) => {}
        _ = copy(upstream_reader, client_write) => {}
    }
}

async fn fault_stats_connection(stream: TcpStream, state: &Mutex<FaultStats>) {
    let (read, mut write) = stream.into_split();
    let mut reader = BufReader::new(read);
    let mut request = Vec::new();
    match timeout(STATS_TIMEOUT, reader.read_until(b'\n', &mut request)).await {
        Ok(Ok(_)) => {}
        _ => return,
    }
    let request = String::from_utf8_lossy(&request)
        .trim()
        .to_ascii_uppercase();
    if request != "STATS" {
        let _ = write.write_all(b"error=expected STATS\n\n").await;
        return;
    }
    let body = state.lock().expect("fault stats mutex poisoned").to_lines();
    let _ = write.write_all(body.as_bytes()).await;
}

// ---------------------------------------------------------------------------
// Counting proxy
// ---------------------------------------------------------------------------

struct CountingProxyConfig {
    redis_host: String,
    redis_port: u16,
    proxy_port: u16,
    stats_port: u16,
}

impl CountingProxyConfig {
    fn from_env() -> Result<Self> {
        Ok(Self {
            redis_host: env::var("REDIS_HOST").unwrap_or_else(|_| "redis-protocol".to_owned()),
            redis_port: env_parse("REDIS_PORT", 6379)?,
            proxy_port: env_parse("PROXY_PORT", 6379)?,
            stats_port: env_parse("STATS_PORT", 9091)?,
        })
    }
}

#[derive(Clone, Default, Serialize)]
struct CommandCounts {
    #[serde(rename = "MULTI")]
    multi: u64,
    #[serde(rename = "EXEC")]
    exec: u64,
    #[serde(rename = "PUBLISH")]
    publish: u64,
}

#[derive(Clone, Default, Serialize)]
struct OutputStats {
    publish_count: u64,
    direct_publish_count: u64,
    transaction_count: u64,
    transaction_request_bytes: Vec<usize>,
    direct_request_bytes: Vec<usize>,
}

#[derive(Clone, Serialize)]
struct CountingStats {
    commands: CommandCounts,
    outputs: BTreeMap<&'static str, OutputStats>,
}

impl CountingStats {
    fn new() -> Self {
        let outputs = OUTPUT_NAMES
            .iter()
            .map(|name| (*name, OutputStats::default()))
            .collect();
        Self {
            commands: CommandCounts::default(),
            outputs,
        }
    }
}

fn record_command(counts: &mut CommandCounts, command: &[u8]) {
    match command {
        b"MULTI" => counts.multi += 1,
        b"EXEC" => counts.exec += 1,
        b"PUBLISH" => counts.publish += 1,
        _ => {}
    }
}

fn database_output(argument: &[u8]) -> Option<&'static str> {
    let index: usize = std::str::from_utf8(argument).ok()?.parse().ok()?;
    index
        .checked_sub(1)
        .and_then(|index| OUTPUT_NAMES.get(index))
        .copied()
}

fn output_for_channel(channel: &[u8]) -> Option<&'static str> {
    OUTPUT_NAMES
        .iter()
        .copied()
        .find(|name| channel.starts_with(format!("out:{name}:").as_bytes()))
}

pub async fn run_counting_proxy() -> Result<()> {
    let config = Arc::new(CountingProxyConfig::from_env()?);
    let state = Arc::new(Mutex::new(CountingStats::new()));

    let redis_listener = TcpListener::bind(("0.0.0.0", config.proxy_port))
        .await
        .with_context(|| format!("failed to bind counting proxy port {}", config.proxy_port))?;
    let stats_listener = TcpListener::bind(("0.0.0.0", config.stats_port))
        .await
        .with_context(|| format!("failed to bind counting stats port {}", config.stats_port))?;

    let proxy_handler = {
        let config = Arc::clone(&config);
        let state = Arc::clone(&state);
        move |stream: TcpStream| {
            let config = Arc::clone(&config);
            let state = Arc::clone(&state);
            async move {
                if let Err(error) = counting_connection(stream, &config, &state).await
                    && !is_disconnect(&error)
                {
                    eprintln!("counting-proxy connection error: {error:#}");
                }
            }
        }
    };
    let stats_handler = {
        let state = Arc::clone(&state);
        move |stream: TcpStream| {
            let state = Arc::clone(&state);
            async move {
                counting_stats_connection(stream, &state).await;
            }
        }
    };

    tokio::try_join!(
        serve_connections(redis_listener, proxy_handler),
        serve_connections(stats_listener, stats_handler),
    )?;
    Ok(())
}

async fn counting_connection(
    stream: TcpStream,
    config: &CountingProxyConfig,
    state: &Mutex<CountingStats>,
) -> Result<()> {
    let upstream = timeout(
        CONNECT_TIMEOUT,
        TcpStream::connect((config.redis_host.as_str(), config.redis_port)),
    )
    .await
    .context("timed out connecting to Redis")?
    .context("failed to connect to Redis")?;
    stream.set_nodelay(true).ok();
    upstream.set_nodelay(true).ok();

    let (client_read, mut client_write) = stream.into_split();
    let (upstream_read, mut upstream_write) = upstream.into_split();
    let mut client_reader = BufReader::new(client_read);
    let mut upstream_reader = BufReader::new(upstream_read);

    let mut output_name: Option<&'static str> = None;
    let mut in_multi = false;
    let mut transaction_bytes = 0_usize;
    let mut transaction_output: Option<&'static str> = None;

    loop {
        let (raw_command, arguments) = read_command(&mut client_reader).await?;
        let command = arguments
            .first()
            .context("empty RESP command")?
            .to_ascii_uppercase();
        let publish_output = if command == b"PUBLISH" && arguments.len() > 1 {
            output_for_channel(&arguments[1])
        } else {
            None
        };

        match command.as_slice() {
            b"SELECT" if arguments.len() > 1 => {
                output_name = database_output(&arguments[1]);
            }
            b"MULTI" => {
                in_multi = true;
                transaction_bytes = raw_command.len();
                transaction_output = output_name;
            }
            b"PUBLISH" => {
                {
                    let mut stats = state.lock().expect("counting stats mutex poisoned");
                    record_command(&mut stats.commands, &command);
                    if let Some(name) = publish_output {
                        let output = stats.outputs.get_mut(name).expect("known output name");
                        output.publish_count += 1;
                        if in_multi {
                            if transaction_output.is_none() {
                                transaction_output = Some(name);
                            }
                        } else {
                            output.direct_publish_count += 1;
                            output.direct_request_bytes.push(raw_command.len());
                        }
                    }
                }
                if in_multi {
                    transaction_bytes += raw_command.len();
                }
            }
            b"EXEC" => {
                state
                    .lock()
                    .expect("counting stats mutex poisoned")
                    .commands
                    .exec += 1;
                if in_multi {
                    transaction_bytes += raw_command.len();
                }
            }
            _ => {}
        }

        upstream_write.write_all(&raw_command).await?;
        let reply = read_frame(&mut upstream_reader).await?;
        client_write.write_all(&reply.raw).await?;

        if command == b"MULTI" {
            state
                .lock()
                .expect("counting stats mutex poisoned")
                .commands
                .multi += 1;
        } else if command == b"EXEC" {
            if in_multi && let Some(name) = transaction_output {
                let mut stats = state.lock().expect("counting stats mutex poisoned");
                let output = stats.outputs.get_mut(name).expect("known output name");
                output.transaction_count += 1;
                output.transaction_request_bytes.push(transaction_bytes);
            }
            in_multi = false;
            transaction_bytes = 0;
            transaction_output = None;
        }
    }
}

async fn counting_stats_connection(stream: TcpStream, state: &Mutex<CountingStats>) {
    let (read, mut write) = stream.into_split();
    let mut reader = BufReader::new(read);
    let mut request = Vec::new();
    match timeout(STATS_TIMEOUT, reader.read_until(b'\n', &mut request)).await {
        Ok(Ok(_)) => {}
        _ => return,
    }
    let request = String::from_utf8_lossy(&request)
        .trim()
        .to_ascii_uppercase();
    if request != "STATS" {
        let _ = write.write_all(b"error=expected STATS\n").await;
        return;
    }
    let stats = state.lock().expect("counting stats mutex poisoned").clone();
    let Ok(body) = serde_json::to_vec(&stats) else {
        return;
    };
    let _ = write.write_all(&body).await;
    let _ = write.write_all(b"\n").await;
}
