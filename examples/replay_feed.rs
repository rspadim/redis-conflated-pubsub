//! Replays a capture produced by `capture_feed` against a Redis server.
//!
//! The capture stores timestamps, channels and payload lengths only; this tool
//! synthesizes payloads of the recorded length (never the original bytes) and
//! preserves the recorded inter-arrival timing (optionally rate-scaled). It is
//! meant for load tests against a local stack.

use std::{
    collections::HashMap,
    env,
    fs::File,
    io::{BufRead, BufReader, Read, Write},
    net::TcpStream,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};

fn env_parse<T: std::str::FromStr>(name: &str, default: T) -> T {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn synth_payload(
    index: u64,
    channel: &str,
    len: usize,
    duplicate_every: u64,
    last_payloads: &mut HashMap<String, Vec<u8>>,
) -> Vec<u8> {
    if duplicate_every > 0
        && !index.is_multiple_of(duplicate_every)
        && let Some(previous) = last_payloads.get(channel)
        && previous.len() == len
    {
        return previous.clone();
    }
    let digits = index.to_string();
    let mut payload = Vec::with_capacity(len);
    while payload.len() < len {
        payload.extend_from_slice(digits.as_bytes());
    }
    payload.truncate(len);
    if duplicate_every > 0 {
        last_payloads.insert(channel.to_owned(), payload.clone());
    }
    payload
}

fn fetch_status(url: &str) -> Result<serde_json::Value> {
    let endpoint = url.trim_start_matches("http://");
    let (host, port) = endpoint
        .split_once(':')
        .context("REPLAY_STATUS_URL must look like http://host:port/")?;
    let port: u16 = port.trim_end_matches('/').parse()?;
    let mut stream = TcpStream::connect((host, port)).context("connect to status endpoint")?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.write_all(b"GET / HTTP/1.1\r\nHost: replay\r\nConnection: close\r\n\r\n")?;
    let mut body = String::new();
    stream.read_to_string(&mut body)?;
    let json = body
        .split("\r\n\r\n")
        .nth(1)
        .context("status response had no body")?;
    serde_json::from_str(json).context("status response was not valid JSON")
}

fn print_service_state(label: &str, status: &serde_json::Value) {
    let output = status["outputs"]["out-a"].clone();
    println!(
        "replay-service {label} input={} published={} pending={} queue_wait_avg_max_ms={:.3}/{:.3} publish_rtt_avg_max_ms={:.3}/{:.3}",
        status["input_messages_total"],
        output["output_messages_total"],
        output["pending_messages"],
        output["queue_wait_total_ns"].as_u64().unwrap_or(0) as f64
            / output["queue_wait_samples"].as_u64().unwrap_or(1).max(1) as f64
            / 1_000_000.0,
        output["queue_wait_max_ns"].as_u64().unwrap_or(0) as f64 / 1_000_000.0,
        output["publish_rtt_total_ns"].as_u64().unwrap_or(0) as f64
            / output["publish_rtt_samples"].as_u64().unwrap_or(1).max(1) as f64
            / 1_000_000.0,
        output["publish_rtt_max_ns"].as_u64().unwrap_or(0) as f64 / 1_000_000.0,
    );
}

#[tokio::main]
async fn main() -> Result<()> {
    let in_path = env::var("REPLAY_IN").context("REPLAY_IN is required")?;
    let host = env::var("REPLAY_HOST").unwrap_or_else(|_| "127.0.0.1".to_owned());
    let port: u16 = env_parse("REPLAY_PORT", 6379);
    let database: i64 = env_parse("REPLAY_DB", 0);
    let rate_scale: f64 = env_parse("REPLAY_RATE_SCALE", 1.0);
    let pipeline: usize = env_parse("REPLAY_PIPELINE", 64).max(1);
    let max_messages: u64 = env_parse("REPLAY_MAX_MESSAGES", u64::MAX);
    let duplicate_every: u64 = env_parse("REPLAY_DUP_EVERY", 0);
    let status_url = env::var("REPLAY_STATUS_URL").ok();
    let drain_wait_ms: u64 = env_parse("REPLAY_DRAIN_WAIT_MS", 2000);
    anyhow::ensure!(rate_scale > 0.0, "REPLAY_RATE_SCALE must be positive");

    if let Some(url) = &status_url
        && let Ok(status) = tokio::task::spawn_blocking({
            let url = url.clone();
            move || fetch_status(&url)
        })
        .await?
    {
        print_service_state("before", &status);
    }

    // Wait for the service to be running before publishing so the first
    // capture lines are not dropped during startup/subscription.
    if status_url.is_some() {
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            let ready = tokio::task::spawn_blocking({
                let url = status_url.clone().expect("checked above");
                move || fetch_status(&url)
            })
            .await?
            .is_ok_and(|status| status["state"] == "running");
            if ready {
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
    let start_delay_ms: u64 = env_parse("REPLAY_START_DELAY_MS", 500);
    tokio::time::sleep(Duration::from_millis(start_delay_ms)).await;

    let client = redis::Client::open(format!("redis://{host}:{port}/{database}"))?;
    let mut connection = client.get_multiplexed_async_connection().await?;
    connection.set_response_timeout(Duration::from_secs(30));

    let reader = BufReader::new(
        File::open(&in_path).with_context(|| format!("failed to open capture {in_path}"))?,
    );
    let mut lines = reader.lines();
    let start = Instant::now();
    let mut first_t: Option<u64> = None;
    let mut sent = 0u64;
    let mut last_payloads = HashMap::new();
    let mut chunk: Vec<(u64, String, usize)> = Vec::with_capacity(pipeline);

    'outer: loop {
        chunk.clear();
        for line in lines.by_ref() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let value: serde_json::Value = serde_json::from_str(&line)
                .with_context(|| format!("invalid capture line: {line}"))?;
            chunk.push((
                value["t"].as_u64().context("capture line missing t")?,
                value["c"]
                    .as_str()
                    .context("capture line missing c")?
                    .to_owned(),
                value["n"].as_u64().unwrap_or(0) as usize,
            ));
            if chunk.len() == pipeline || sent + chunk.len() as u64 >= max_messages {
                break;
            }
        }
        if chunk.is_empty() {
            break 'outer;
        }
        let base = *first_t.get_or_insert(chunk[0].0);
        let target_ns = ((chunk[0].0.saturating_sub(base)) as f64 / rate_scale) as u64;
        let target = Duration::from_nanos(target_ns);
        let now = start.elapsed();
        if target > now {
            tokio::time::sleep(target - now).await;
        }
        let mut pipe = redis::pipe();
        for (offset, (_t, channel, len)) in chunk.iter().enumerate() {
            let payload = synth_payload(
                sent + offset as u64,
                channel,
                *len,
                duplicate_every,
                &mut last_payloads,
            );
            pipe.cmd("PUBLISH").arg(channel).arg(payload);
        }
        let _: Vec<i64> = pipe
            .query_async(&mut connection)
            .await
            .context("replay PUBLISH failed")?;
        sent += chunk.len() as u64;
        if sent % 500_000 < chunk.len() as u64 {
            eprintln!(
                "replay progress: {sent} messages ({:.0}/s)",
                sent as f64 / start.elapsed().as_secs_f64().max(0.001)
            );
        }
        if sent >= max_messages {
            break;
        }
    }

    let elapsed = start.elapsed();
    println!(
        "replay-done messages={sent} elapsed_s={:.3} rate_per_second={:.0}",
        elapsed.as_secs_f64(),
        sent as f64 / elapsed.as_secs_f64().max(0.001)
    );
    if let Some(url) = &status_url {
        tokio::time::sleep(Duration::from_millis(drain_wait_ms)).await;
        if let Ok(status) = tokio::task::spawn_blocking({
            let url = url.clone();
            move || fetch_status(&url)
        })
        .await?
        {
            print_service_state("after", &status);
        }
    }
    Ok(())
}
