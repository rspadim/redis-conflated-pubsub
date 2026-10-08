//! Read-only Pub/Sub capture for later replay.
//!
//! Records per-message metadata only: relative timestamp, channel name and
//! payload length. Payload contents are never written, so no application data
//! leaves the captured server. The NDJSON output and the summary stay local
//! (see `tests/.tmp-*` in `.gitignore`).

use std::{
    collections::HashMap,
    env,
    fs::File,
    io::{BufWriter, Write},
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use futures_util::StreamExt;
use serde_json::json;

const LENGTH_BUCKET_BOUNDS: [usize; 11] =
    [8, 16, 32, 64, 128, 256, 512, 1024, 4096, 65536, usize::MAX];

type ChannelStats = (u64, u64, Vec<usize>);

fn env_parse<T: std::str::FromStr>(name: &str, default: T) -> T {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn length_bucket(len: usize) -> usize {
    LENGTH_BUCKET_BOUNDS
        .iter()
        .position(|bound| len <= *bound)
        .unwrap_or(LENGTH_BUCKET_BOUNDS.len() - 1)
}

#[tokio::main]
async fn main() -> Result<()> {
    let host = env::var("CAPTURE_HOST").context("CAPTURE_HOST is required")?;
    let port: u16 = env_parse("CAPTURE_PORT", 6379);
    let database: i64 = env_parse("CAPTURE_DB", 0);
    let pattern = env::var("CAPTURE_PATTERN").unwrap_or_else(|_| "*".to_owned());
    let seconds: u64 = env_parse("CAPTURE_SECONDS", 1800);
    let max_messages: u64 = env_parse("CAPTURE_MAX_MESSAGES", 5_000_000);
    let out_path =
        env::var("CAPTURE_OUT").unwrap_or_else(|_| "tests/.tmp-capture.ndjson".to_owned());
    let summary_path = env::var("CAPTURE_SUMMARY")
        .unwrap_or_else(|_| "tests/.tmp-capture-summary.json".to_owned());

    let client = redis::Client::open(format!("redis://{host}:{port}/{database}"))?;
    let mut pubsub = client.get_async_pubsub().await?;
    pubsub.psubscribe(pattern.as_str()).await?;
    let mut stream = pubsub.on_message();
    println!(
        "capture started host={host}:{port} db={database} pattern={pattern} seconds={seconds} max_messages={max_messages}"
    );

    let start = Instant::now();
    let deadline = start + Duration::from_secs(seconds);
    let mut writer = BufWriter::new(
        File::create(&out_path)
            .with_context(|| format!("failed to create capture file {out_path}"))?,
    );
    let mut count = 0u64;
    let mut payload_bytes = 0u64;
    let mut truncated = false;
    let mut channels: HashMap<String, ChannelStats> = HashMap::new();
    let mut buckets = [0u64; LENGTH_BUCKET_BOUNDS.len()];
    let mut per_second: Vec<u64> = Vec::new();
    let mut next_progress = start + Duration::from_secs(30);

    loop {
        let now = Instant::now();
        if now >= deadline {
            break;
        }
        if now >= next_progress {
            let elapsed = start.elapsed().as_secs_f64();
            println!(
                "capture progress: {count} messages ({:.0}/s), {} channels, {:.1}s elapsed",
                count as f64 / elapsed.max(0.001),
                channels.len(),
                elapsed
            );
            next_progress = now + Duration::from_secs(30);
        }
        let remaining = deadline.saturating_duration_since(now);
        let message = match tokio::time::timeout(remaining, stream.next()).await {
            Err(_) => break,
            Ok(None) => break,
            Ok(Some(message)) => message,
        };
        let t = start.elapsed().as_nanos() as u64;
        let channel = message.get_channel_name();
        let len = message.get_payload_bytes().len();
        if count < max_messages {
            serde_json::to_writer(&mut writer, &json!({"t": t, "c": channel, "n": len}))?;
            writer.write_all(b"\n")?;
        } else if !truncated {
            truncated = true;
            println!(
                "capture reached max_messages={max_messages}; metadata continues in the summary"
            );
        }
        count += 1;
        payload_bytes = payload_bytes.saturating_add(len as u64);
        let entry = channels
            .entry(channel.to_owned())
            .or_insert_with(|| (0, 0, Vec::new()));
        entry.0 += 1;
        entry.1 = entry.1.saturating_add(len as u64);
        if entry.2.len() < 8 {
            entry.2.push(len);
        }
        buckets[length_bucket(len)] += 1;
        let second = (t / 1_000_000_000) as usize;
        if per_second.len() <= second {
            per_second.resize(second + 1, 0);
        }
        per_second[second] += 1;
    }
    writer.flush()?;

    let elapsed = start.elapsed();
    let unique_channels = channels.len();
    let mut top: Vec<(String, ChannelStats)> = channels.into_iter().collect();
    top.sort_by_key(|entry| std::cmp::Reverse(entry.1.0));
    let top_channels: Vec<_> = top
        .into_iter()
        .take(64)
        .map(|(channel, (messages, bytes, samples))| {
            json!({
                "channel": channel,
                "messages": messages,
                "payload_bytes": bytes,
                "length_samples": samples,
            })
        })
        .collect();
    let bucket_json: Vec<_> = LENGTH_BUCKET_BOUNDS
        .iter()
        .zip(buckets)
        .map(|(bound, count)| json!({"max_len": bound, "messages": count}))
        .collect();
    let summary = json!({
        "schema_version": 1,
        "privacy": {
            "payload_contents_recorded": false,
            "host_recorded": false,
            "exact_capture_time_recorded": false,
        },
        "capture": {"seconds": elapsed.as_secs_f64(), "pattern": pattern},
        "messages": {
            "count": count,
            "payload_bytes": payload_bytes,
            "rate_per_second": count as f64 / elapsed.as_secs_f64().max(0.001),
            "payload_size_buckets": bucket_json,
            "ndjson_truncated": truncated,
            "max_messages": max_messages,
        },
        "channels": {"unique": unique_channels, "top_64": top_channels},
        "per_second_rate": per_second,
    });
    std::fs::write(&summary_path, serde_json::to_string_pretty(&summary)?)
        .with_context(|| format!("failed to write summary {summary_path}"))?;
    println!(
        "capture done: {count} messages in {:.1}s ({:.0}/s), {unique_channels} unique channels; ndjson={out_path} summary={summary_path}",
        elapsed.as_secs_f64(),
        count as f64 / elapsed.as_secs_f64().max(0.001),
    );
    Ok(())
}
