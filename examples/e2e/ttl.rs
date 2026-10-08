//! Rust replacement for `tests/docker_ttl_integration.py`: drives the
//! TTL/profile/group/channel-policy E2E in `compose.ttl.test.yml`.
//!
//! The port keeps the Python driver's scenarios and tolerances: profile and
//! inline channel policies with ordered first-match selection, individual and
//! grouped TTL expiry (fixed windows, restart-on-change and `round_ms` floor
//! rounding), independent 100/300 ms conflation schedules, TTL 0 with
//! conflation and direct TTL 0 forwarding, a valid Unix `SIGHUP` config swap,
//! and an invalid-file reload that leaves the active configuration running.
//! The `SIGHUP` cases only run when `SERVICE_PID` is set; Compose shares the
//! service PID namespace so the driver can signal it with BusyBox `kill`.

use std::{
    collections::BTreeMap,
    env, fs, io,
    process::Command,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail, ensure};
use serde_json::Value;
use tokio::time::sleep;

use crate::integration::{
    RedisConnection, expect_subscription_ack, get_status, parse_pmessage,
    wait_for_input_subscription, wait_for_status,
};

const SOURCE_CHANNEL: &[u8] = b"ttl-feed:binary";
const TTL_MS: u64 = 600;
const CONFLATION_INTERVAL_MS: u64 = 50;
const FLOOR_GROUP_TTL_MS: u64 = 2600;
const FLOOR_GROUP_ROUND_MS: u64 = 1000;
const FLOOR_GROUP_GUARD_MS: u64 = 350;
const PAYLOAD_A: &[u8] = b"\x00\xffttl-A:\x80\r\n\x00";
const PAYLOAD_B: &[u8] = b"\xff\x00ttl-B:\xfe\r\n";
const PAYLOAD_C: &[u8] = b"\x7f\x00ttl-C:\xfd\r\n";

type Times = BTreeMap<Vec<u8>, Instant>;
type PolicyTimes = (Times, Times);

fn mapped_channel(output_name: &str, source_name: &str) -> Vec<u8> {
    format!("{output_name}-out:ttl-feed:{source_name}").into_bytes()
}

fn metric(value: &Value, key: &str) -> Option<u64> {
    value.get(key).and_then(Value::as_u64)
}

fn output_metric(status: &Value, name: &str, key: &str) -> Option<u64> {
    status.get("outputs")?.get(name)?.get(key)?.as_u64()
}

fn epoch_millis() -> Result<f64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("the system clock is before the Unix epoch")?
        .as_secs_f64()
        * 1000.0)
}

async fn sleep_until(deadline: Instant) {
    sleep(deadline.saturating_duration_since(Instant::now())).await;
}

// ---------------------------------------------------------------------------
// Subscribers, publishing and receiving
// ---------------------------------------------------------------------------

struct Subscribers {
    ttl: RedisConnection,
    window_zero: RedisConnection,
    direct_zero: RedisConnection,
}

impl Subscribers {
    fn all_mut(&mut self) -> [&mut RedisConnection; 3] {
        [&mut self.ttl, &mut self.window_zero, &mut self.direct_zero]
    }
}

async fn open_subscriber(database: u32, pattern: &str) -> Result<RedisConnection> {
    let mut connection = RedisConnection::connect(database).await?;
    let acknowledgement = connection
        .command(&[b"PSUBSCRIBE", pattern.as_bytes()])
        .await?;
    expect_subscription_ack(&acknowledgement, b"psubscribe")?;
    Ok(connection)
}

async fn open_subscribers() -> Result<Subscribers> {
    Ok(Subscribers {
        ttl: open_subscriber(1, "ttl-out:*").await?,
        window_zero: open_subscriber(2, "window-zero-out:*").await?,
        direct_zero: open_subscriber(3, "direct-zero-out:*").await?,
    })
}

async fn publish(publisher: &mut RedisConnection, payload: &[u8]) -> Result<Instant> {
    let count = publisher
        .command(&[b"PUBLISH", SOURCE_CHANNEL, payload])
        .await?
        .as_int()
        .context("PUBLISH did not return an integer")?;
    let channel = String::from_utf8_lossy(SOURCE_CHANNEL);
    ensure!(count >= 1, "{channel} subscriber count: {count}");
    Ok(Instant::now())
}

async fn receive_message(
    connection: &mut RedisConnection,
    wait: Duration,
) -> Result<(Vec<u8>, Vec<u8>)> {
    let deadline = Instant::now() + wait;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        ensure!(
            !remaining.is_zero(),
            "Timed out waiting for a Pub/Sub message"
        );
        let slice = remaining.min(Duration::from_millis(100));
        if let Some(frame) = connection.next(slice).await? {
            let (channel, payload) = parse_pmessage(&frame.value)
                .with_context(|| format!("unexpected Redis Pub/Sub response: {:?}", frame.value))?;
            return Ok((channel, payload));
        }
    }
}

async fn receive_expected(
    connection: &mut RedisConnection,
    channel: &[u8],
    payload: &[u8],
    wait: Duration,
) -> Result<()> {
    let (actual_channel, actual_payload) = receive_message(connection, wait).await?;
    ensure!(
        actual_channel == channel && actual_payload == payload,
        "expected: ({channel:?}, {payload:?}), actual: ({actual_channel:?}, {actual_payload:?})"
    );
    Ok(())
}

async fn receive_many(
    connection: &mut RedisConnection,
    output_name: &str,
    expected: &[(&str, &[u8])],
    wait: Duration,
) -> Result<Times> {
    let mut pending: BTreeMap<Vec<u8>, Vec<u8>> = expected
        .iter()
        .map(|(source_name, payload)| (mapped_channel(output_name, source_name), payload.to_vec()))
        .collect();
    let mut received_at = Times::new();
    let deadline = Instant::now() + wait;
    while !pending.is_empty() {
        let remaining = deadline.saturating_duration_since(Instant::now());
        ensure!(
            !remaining.is_zero(),
            "Timed out waiting for Pub/Sub messages {pending:?}"
        );
        let (channel, payload) = receive_message(connection, remaining).await?;
        let expected_payload = pending.get(&channel).with_context(|| {
            format!(
                "unexpected_channel: {channel:?}, expected_channels: {:?}",
                pending.keys().collect::<Vec<_>>()
            )
        })?;
        ensure!(
            payload == *expected_payload,
            "channel: {channel:?}, expected_payload: {expected_payload:?}, actual_payload: {payload:?}"
        );
        received_at.insert(channel.clone(), Instant::now());
        pending.remove(&channel);
    }
    Ok(received_at)
}

async fn assert_quiet(connections: &mut [&mut RedisConnection], duration: Duration) -> Result<()> {
    let deadline = Instant::now() + duration;
    while Instant::now() < deadline {
        for connection in connections.iter_mut() {
            let wait = deadline
                .saturating_duration_since(Instant::now())
                .min(Duration::from_millis(100));
            if let Some(frame) = connection.next(wait).await? {
                bail!("Unexpected duplicate Pub/Sub event: {:?}", frame.value);
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Policy batch helpers
// ---------------------------------------------------------------------------

async fn publish_policy_batch(
    publisher: &mut RedisConnection,
    subscribers: &mut Subscribers,
    items: &[(&str, &[u8])],
    ttl_items: &[(&str, &[u8])],
    ttl_timeout: Duration,
    assert_ttl_quiet: bool,
) -> Result<PolicyTimes> {
    for (source_name, payload) in items {
        let channel = format!("ttl-feed:{source_name}");
        let count = publisher
            .command(&[b"PUBLISH", channel.as_bytes(), payload])
            .await?
            .as_int()
            .context("PUBLISH did not return an integer")?;
        ensure!(count >= 1, "{channel} subscriber count: {count}");
    }

    for (source_name, payload) in items {
        receive_expected(
            &mut subscribers.direct_zero,
            &mapped_channel("direct-zero", source_name),
            payload,
            Duration::from_secs(10),
        )
        .await?;
    }

    let ttl_times = if ttl_items.is_empty() {
        Times::new()
    } else {
        receive_many(&mut subscribers.ttl, "ttl", ttl_items, ttl_timeout).await?
    };
    if assert_ttl_quiet {
        let mut quiet = [&mut subscribers.ttl];
        assert_quiet(&mut quiet, Duration::from_millis(120)).await?;
    }

    let window_zero_times = receive_many(
        &mut subscribers.window_zero,
        "window-zero",
        items,
        Duration::from_secs(10),
    )
    .await?;
    Ok((ttl_times, window_zero_times))
}

async fn publish_policy_event(
    publisher: &mut RedisConnection,
    subscribers: &mut Subscribers,
    source_name: &str,
    payload: &[u8],
    ttl_expected: bool,
) -> Result<Option<Instant>> {
    let ttl_items: Vec<(&str, &[u8])> = if ttl_expected {
        vec![(source_name, payload)]
    } else {
        Vec::new()
    };
    let (ttl_times, _) = publish_policy_batch(
        publisher,
        subscribers,
        &[(source_name, payload)],
        &ttl_items,
        Duration::from_secs(10),
        !ttl_expected,
    )
    .await?;
    if ttl_expected {
        Ok(Some(
            *ttl_times
                .get(&mapped_channel("ttl", source_name))
                .with_context(|| format!("no TTL output for {source_name}"))?,
        ))
    } else {
        Ok(None)
    }
}

// ---------------------------------------------------------------------------
// Conflation-window and epoch-phase timing helpers
// ---------------------------------------------------------------------------

fn window_target(
    now: Instant,
    last_flush_at: Instant,
    interval: Duration,
    guard: Duration,
) -> Instant {
    let elapsed = now.saturating_duration_since(last_flush_at).as_nanos();
    let periods = (elapsed / interval.as_nanos()) as u32 + 1;
    last_flush_at + interval * periods + guard
}

async fn next_window_start(last_flush_at: Instant, interval_ms: u64, guard_ms: u64) -> Instant {
    let target = window_target(
        Instant::now(),
        last_flush_at,
        Duration::from_millis(interval_ms),
        Duration::from_millis(guard_ms),
    );
    sleep_until(target).await;
    Instant::now()
}

async fn wait_for_next_windows(anchors: &[(Instant, u64)], guard_ms: u64) {
    let now = Instant::now();
    let target = anchors
        .iter()
        .map(|(last_flush_at, interval_ms)| {
            window_target(
                now,
                *last_flush_at,
                Duration::from_millis(*interval_ms),
                Duration::from_millis(guard_ms),
            )
        })
        .max()
        .expect("at least one window anchor");
    sleep_until(target).await;
}

async fn wait_for_epoch_phase(
    round_ms: u64,
    minimum_ms: f64,
    maximum_ms: f64,
    wait: Duration,
) -> Result<()> {
    let deadline = Instant::now() + wait;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        let now_epoch = epoch_millis()? / 1000.0;
        let phase_ms = (now_epoch * 1000.0) % round_ms as f64;
        if minimum_ms <= phase_ms && phase_ms <= maximum_ms {
            return Ok(());
        }
        let delay = if phase_ms < minimum_ms {
            (minimum_ms - phase_ms) / 1000.0
        } else {
            (round_ms as f64 - phase_ms + minimum_ms) / 1000.0
        };
        sleep(Duration::from_secs_f64(delay.min(remaining.as_secs_f64()))).await;
    }
    bail!("Could not reach {minimum_ms}-{maximum_ms} ms phase of {round_ms} ms")
}

// ---------------------------------------------------------------------------
// SIGHUP reload
// ---------------------------------------------------------------------------

fn run_kill(arguments: &[&str]) -> Result<()> {
    let status = Command::new("kill")
        .args(arguments)
        .status()
        .with_context(|| format!("failed to run kill {}", arguments.join(" ")))?;
    ensure!(
        status.success(),
        "kill {} exited with {status}",
        arguments.join(" ")
    );
    Ok(())
}

async fn wait_for_sighup_reload(
    publisher: &mut RedisConnection,
    status_url: &str,
    wait: Duration,
) -> Result<Value> {
    let deadline = Instant::now() + wait;
    let mut latest = Value::Null;
    loop {
        ensure!(
            Instant::now() < deadline,
            "Service did not finish SIGHUP reload; last status: {latest}"
        );
        let current = match get_status(status_url).await {
            Ok(current) => current,
            Err(_) => {
                sleep(Duration::from_millis(50)).await;
                continue;
            }
        };
        latest = current;
        let reloaded = latest.get("state").and_then(Value::as_str) == Some("running")
            && latest.get("input_messages_total").and_then(Value::as_u64) == Some(0);
        if reloaded {
            let count = publisher.command(&[b"PUBSUB", b"NUMPAT"]).await?;
            if count.as_int() == Some(4) {
                return Ok(latest);
            }
        }
        sleep(Duration::from_millis(50)).await;
    }
}

fn rejected_reload_in_logs(log_dir: &str) -> Result<bool> {
    let entries = match fs::read_dir(log_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => {
            return Err(error).with_context(|| format!("failed to list {log_dir}"));
        }
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let named_like_log = path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with("redis-conflated-pubsub."));
        if named_like_log
            && fs::read_to_string(&path)
                .is_ok_and(|contents| contents.contains("configuration_reload_rejected"))
        {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn wait_for_rejected_reload(log_dir: &str, wait: Duration) -> Result<()> {
    let deadline = Instant::now() + wait;
    while Instant::now() < deadline {
        if rejected_reload_in_logs(log_dir)? {
            return Ok(());
        }
        sleep(Duration::from_millis(50)).await;
    }
    bail!("Service did not log rejection of the invalid SIGHUP config")
}

async fn exercise_sighup_reload(
    publisher: &mut RedisConnection,
    subscribers: &mut Subscribers,
    status_url: &str,
    config_path: &str,
    log_dir: &str,
) -> Result<()> {
    let Ok(service_pid) = env::var("SERVICE_PID") else {
        return Ok(());
    };
    if service_pid.is_empty() {
        return Ok(());
    }

    let original_config =
        fs::read(config_path).with_context(|| format!("failed to read {config_path}"))?;
    let outcome = reload_sequence(
        publisher,
        subscribers,
        status_url,
        config_path,
        log_dir,
        &service_pid,
        &original_config,
    )
    .await;
    // Like the Python finally block, always restore the seeded configuration.
    let restore = fs::write(config_path, &original_config)
        .with_context(|| format!("failed to restore {config_path}"));
    outcome.and(restore)
}

async fn reload_sequence(
    publisher: &mut RedisConnection,
    subscribers: &mut Subscribers,
    status_url: &str,
    config_path: &str,
    log_dir: &str,
    service_pid: &str,
    original_config: &[u8],
) -> Result<()> {
    let mut updated: Value = serde_json::from_slice(original_config)
        .with_context(|| format!("invalid JSON in {config_path}"))?;
    let profile = updated
        .pointer_mut("/outputs/ttl/profiles/default")
        .context("missing outputs.ttl.profiles.default in the TTL config")?
        .as_object_mut()
        .context("outputs.ttl.profiles.default is not an object")?;
    profile.insert("deduplication.ttl_ms".to_owned(), Value::from(1200_u64));
    let updated_bytes =
        serde_json::to_vec(&updated).context("failed to serialize the updated TTL config")?;
    fs::write(config_path, updated_bytes)
        .with_context(|| format!("failed to write {config_path}"))?;

    run_kill(&["-HUP", service_pid])?;
    wait_for_sighup_reload(publisher, status_url, Duration::from_secs(15)).await?;

    let probe = "reload-probe";
    let _ = publish_policy_event(publisher, subscribers, probe, PAYLOAD_A, true).await?;
    sleep(Duration::from_millis(850)).await;
    publish_policy_batch(
        publisher,
        subscribers,
        &[(probe, PAYLOAD_A)],
        &[],
        Duration::from_secs(10),
        true,
    )
    .await?;

    let invalid_probe = "reload-invalid-probe";
    let _ = publish_policy_event(publisher, subscribers, invalid_probe, PAYLOAD_B, true).await?;
    sleep(Duration::from_millis(850)).await;
    fs::write(config_path, b"{ invalid json")
        .with_context(|| format!("failed to write {config_path}"))?;
    run_kill(&["-HUP", service_pid])?;
    wait_for_rejected_reload(log_dir, Duration::from_secs(5)).await?;
    let status = get_status(status_url).await?;
    ensure!(
        status.get("state").and_then(Value::as_str) == Some("running"),
        "{status}"
    );
    run_kill(&["-0", service_pid])?;
    // The valid 1200 ms TTL remains active. If the rejected reload had
    // replaced it with the original 600 ms config, this duplicate publishes.
    publish_policy_batch(
        publisher,
        subscribers,
        &[(invalid_probe, PAYLOAD_B)],
        &[],
        Duration::from_secs(10),
        true,
    )
    .await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Channel policies, groups and independent intervals
// ---------------------------------------------------------------------------

async fn exercise_channel_policies(
    publisher: &mut RedisConnection,
    subscribers: &mut Subscribers,
) -> Result<()> {
    let reuse_items = [
        ("profile-reuse-a", PAYLOAD_A),
        ("profile-reuse-b", PAYLOAD_A),
    ];
    let (reuse_initial, _) = publish_policy_batch(
        publisher,
        subscribers,
        &reuse_items,
        &reuse_items,
        Duration::from_secs(10),
        false,
    )
    .await?;
    publish_policy_batch(
        publisher,
        subscribers,
        &reuse_items,
        &[],
        Duration::from_secs(10),
        true,
    )
    .await?;
    let reuse_expiry = *reuse_initial
        .values()
        .max()
        .context("no reuse TTL timestamps")?
        + Duration::from_millis(350);
    sleep_until(reuse_expiry).await;
    publish_policy_batch(
        publisher,
        subscribers,
        &reuse_items,
        &reuse_items,
        Duration::from_secs(10),
        false,
    )
    .await?;

    let early_at = publish_policy_event(publisher, subscribers, "ungrouped-early", PAYLOAD_A, true)
        .await?
        .context("expected a TTL timestamp for ungrouped-early")?;
    sleep(Duration::from_millis(250)).await;
    let _ = publish_policy_event(publisher, subscribers, "ungrouped-late", PAYLOAD_A, true).await?;
    sleep_until(early_at + Duration::from_millis(500)).await;
    publish_policy_batch(
        publisher,
        subscribers,
        &[
            ("ungrouped-early", PAYLOAD_A),
            ("ungrouped-late", PAYLOAD_A),
        ],
        &[("ungrouped-early", PAYLOAD_A)],
        Duration::from_secs(10),
        true,
    )
    .await?;

    let precedence_at =
        publish_policy_event(publisher, subscribers, "precedence-suffix", PAYLOAD_A, true)
            .await?
            .context("expected a TTL timestamp for precedence-suffix")?;
    sleep_until(precedence_at + Duration::from_millis(350)).await;
    let _ =
        publish_policy_event(publisher, subscribers, "precedence-suffix", PAYLOAD_A, true).await?;

    let selector_items = [
        ("policy-prefix-case", PAYLOAD_A),
        ("namespaced:suffix-inline", PAYLOAD_A),
    ];
    let (selector_initial, _) = publish_policy_batch(
        publisher,
        subscribers,
        &selector_items,
        &selector_items,
        Duration::from_secs(10),
        false,
    )
    .await?;
    let selector_expiry = *selector_initial
        .values()
        .max()
        .context("no selector TTL timestamps")?
        + Duration::from_millis(350);
    sleep_until(selector_expiry).await;
    publish_policy_batch(
        publisher,
        subscribers,
        &selector_items,
        &selector_items,
        Duration::from_secs(10),
        false,
    )
    .await?;

    let fixed_initial_at =
        publish_policy_event(publisher, subscribers, "group-fixed-a", PAYLOAD_A, true)
            .await?
            .context("expected a TTL timestamp for group-fixed-a")?;
    sleep_until(fixed_initial_at + Duration::from_millis(250)).await;
    let _ = publish_policy_event(publisher, subscribers, "group-fixed-b", PAYLOAD_B, true).await?;
    sleep_until(fixed_initial_at + Duration::from_millis(700)).await;
    let _ = publish_policy_event(publisher, subscribers, "group-fixed-b", PAYLOAD_B, true).await?;

    let reset_initial_at =
        publish_policy_event(publisher, subscribers, "group-reset-a", PAYLOAD_A, true)
            .await?
            .context("expected a TTL timestamp for group-reset-a")?;
    sleep_until(reset_initial_at + Duration::from_millis(500)).await;
    let reset_changed_at =
        publish_policy_event(publisher, subscribers, "group-reset-b", PAYLOAD_B, true)
            .await?
            .context("expected a TTL timestamp for group-reset-b")?;
    sleep_until(reset_changed_at + Duration::from_millis(350)).await;
    let _ = publish_policy_event(publisher, subscribers, "group-reset-a", PAYLOAD_A, false).await?;
    sleep_until(reset_changed_at + Duration::from_millis(850)).await;
    let _ = publish_policy_event(publisher, subscribers, "group-reset-a", PAYLOAD_A, true).await?;

    wait_for_epoch_phase(FLOOR_GROUP_ROUND_MS, 550.0, 650.0, Duration::from_secs(4)).await?;
    let floor_name = "group-floor-a";
    let floor_first_at = publish_policy_event(publisher, subscribers, floor_name, PAYLOAD_A, true)
        .await?
        .context("expected a TTL timestamp for group-floor-a")?;
    let floor_phase_ms = epoch_millis()? % FLOOR_GROUP_ROUND_MS as f64;
    ensure!(
        (450.0..=750.0).contains(&floor_phase_ms),
        "floor_group_publish_phase_ms: {floor_phase_ms}"
    );
    let floor_expiry_at = floor_first_at
        + Duration::from_secs_f64((FLOOR_GROUP_TTL_MS as f64 - floor_phase_ms) / 1000.0);
    sleep_until(floor_expiry_at - Duration::from_millis(FLOOR_GROUP_GUARD_MS)).await;
    let _ = publish_policy_event(publisher, subscribers, floor_name, PAYLOAD_A, false).await?;
    sleep_until(floor_expiry_at + Duration::from_millis(FLOOR_GROUP_GUARD_MS)).await;
    let _ = publish_policy_event(publisher, subscribers, floor_name, PAYLOAD_A, true).await?;

    let interval_seed = [("interval-fast", PAYLOAD_A), ("interval-slow", PAYLOAD_A)];
    let (interval_initial_times, _) = publish_policy_batch(
        publisher,
        subscribers,
        &interval_seed,
        &interval_seed,
        Duration::from_secs(3),
        false,
    )
    .await?;
    let fast_channel = mapped_channel("ttl", "interval-fast");
    let slow_channel = mapped_channel("ttl", "interval-slow");
    let fast_initial = *interval_initial_times
        .get(&fast_channel)
        .context("missing interval-fast TTL time")?;
    let slow_initial = *interval_initial_times
        .get(&slow_channel)
        .context("missing interval-slow TTL time")?;
    wait_for_next_windows(&[(fast_initial, 100), (slow_initial, 300)], 20).await;

    let interval_followup = [("interval-fast", PAYLOAD_B), ("interval-slow", PAYLOAD_C)];
    publish_policy_batch(
        publisher,
        subscribers,
        &interval_followup,
        &[],
        Duration::from_secs(10),
        false,
    )
    .await?;
    let fast_message = receive_message(&mut subscribers.ttl, Duration::from_millis(300)).await?;
    ensure!(
        fast_message.0 == fast_channel && fast_message.1 == PAYLOAD_B,
        "expected_fast_interval_message: ({fast_channel:?}, {PAYLOAD_B:?}), actual: {fast_message:?}"
    );
    let fast_output_at = Instant::now();
    {
        let mut quiet = [&mut subscribers.ttl];
        assert_quiet(&mut quiet, Duration::from_millis(120)).await?;
    }
    let slow_message = receive_message(&mut subscribers.ttl, Duration::from_millis(500)).await?;
    ensure!(
        slow_message.0 == slow_channel && slow_message.1 == PAYLOAD_C,
        "expected_slow_interval_message: ({slow_channel:?}, {PAYLOAD_C:?}), actual: {slow_message:?}"
    );
    let slow_output_at = Instant::now();
    ensure!(
        slow_output_at.duration_since(fast_output_at) >= Duration::from_millis(120),
        "fast_output_at: {fast_output_at:?}, slow_output_at: {slow_output_at:?}"
    );
    let mut quiet = subscribers.all_mut();
    assert_quiet(&mut quiet, Duration::from_millis(450)).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Scenario driver
// ---------------------------------------------------------------------------

pub async fn run_ttl_integration() -> Result<()> {
    let status_url =
        env::var("STATUS_URL").unwrap_or_else(|_| "http://service-ttl:9090/".to_owned());
    let config_path =
        env::var("TTL_CONFIG_PATH").unwrap_or_else(|_| "/state/docker-ttl-config.json".to_owned());
    let log_dir = env::var("TTL_LOG_DIR").unwrap_or_else(|_| "/state/logs".to_owned());

    wait_for_input_subscription(1).await?;
    let mut subscribers = open_subscribers().await?;
    let mut publisher = RedisConnection::connect(0).await?;

    sleep(Duration::from_millis(100)).await;
    publish(&mut publisher, PAYLOAD_A).await?;
    receive_expected(
        &mut subscribers.ttl,
        &mapped_channel("ttl", "binary"),
        PAYLOAD_A,
        Duration::from_secs(10),
    )
    .await?;
    receive_expected(
        &mut subscribers.window_zero,
        &mapped_channel("window-zero", "binary"),
        PAYLOAD_A,
        Duration::from_secs(10),
    )
    .await?;
    receive_expected(
        &mut subscribers.direct_zero,
        &mapped_channel("direct-zero", "binary"),
        PAYLOAD_A,
        Duration::from_secs(10),
    )
    .await?;

    let first_a_output_at = Instant::now();
    let duplicate_a_sent_at = publish(&mut publisher, PAYLOAD_A).await?;
    ensure!(
        duplicate_a_sent_at.duration_since(first_a_output_at) < Duration::from_millis(TTL_MS),
        "Repeated A must be published within the configured TTL"
    );
    receive_expected(
        &mut subscribers.window_zero,
        &mapped_channel("window-zero", "binary"),
        PAYLOAD_A,
        Duration::from_secs(10),
    )
    .await?;
    receive_expected(
        &mut subscribers.direct_zero,
        &mapped_channel("direct-zero", "binary"),
        PAYLOAD_A,
        Duration::from_secs(10),
    )
    .await?;
    wait_for_status(
        &status_url,
        |current| {
            metric(current, "input_messages_total") == Some(2)
                && output_metric(current, "ttl", "deduplicated_messages_total") == Some(1)
                && output_metric(current, "ttl", "output_messages_total") == Some(1)
                && output_metric(current, "ttl", "pending_messages") == Some(0)
                && output_metric(current, "window-zero", "output_messages_total") == Some(2)
        },
        "the repeated A payload to be deduplicated",
        Duration::from_secs(15),
    )
    .await?;

    let changed_a_to_b_at = publish(&mut publisher, PAYLOAD_B).await?;
    ensure!(
        changed_a_to_b_at.duration_since(duplicate_a_sent_at) < Duration::from_millis(TTL_MS),
        "Changed B must be accepted within A's configured TTL"
    );
    receive_expected(
        &mut subscribers.ttl,
        &mapped_channel("ttl", "binary"),
        PAYLOAD_B,
        Duration::from_secs(10),
    )
    .await?;
    let first_b_output_at = Instant::now();
    receive_expected(
        &mut subscribers.window_zero,
        &mapped_channel("window-zero", "binary"),
        PAYLOAD_B,
        Duration::from_secs(10),
    )
    .await?;
    receive_expected(
        &mut subscribers.direct_zero,
        &mapped_channel("direct-zero", "binary"),
        PAYLOAD_B,
        Duration::from_secs(10),
    )
    .await?;

    let duplicate_b_sent_at = publish(&mut publisher, PAYLOAD_B).await?;
    ensure!(
        duplicate_b_sent_at.duration_since(first_b_output_at) < Duration::from_millis(TTL_MS),
        "Repeated B must be published within the configured TTL"
    );
    receive_expected(
        &mut subscribers.window_zero,
        &mapped_channel("window-zero", "binary"),
        PAYLOAD_B,
        Duration::from_secs(10),
    )
    .await?;
    receive_expected(
        &mut subscribers.direct_zero,
        &mapped_channel("direct-zero", "binary"),
        PAYLOAD_B,
        Duration::from_secs(10),
    )
    .await?;
    wait_for_status(
        &status_url,
        |current| {
            metric(current, "input_messages_total") == Some(4)
                && output_metric(current, "ttl", "deduplicated_messages_total") == Some(2)
                && output_metric(current, "ttl", "output_messages_total") == Some(2)
                && output_metric(current, "ttl", "pending_messages") == Some(0)
                && output_metric(current, "window-zero", "output_messages_total") == Some(4)
        },
        "the repeated B payload to be deduplicated",
        Duration::from_secs(15),
    )
    .await?;

    let expiry_deadline = first_b_output_at + Duration::from_millis(TTL_MS + 300);
    sleep_until(expiry_deadline).await;
    publish(&mut publisher, PAYLOAD_B).await?;
    receive_expected(
        &mut subscribers.ttl,
        &mapped_channel("ttl", "binary"),
        PAYLOAD_B,
        Duration::from_secs(10),
    )
    .await?;
    receive_expected(
        &mut subscribers.window_zero,
        &mapped_channel("window-zero", "binary"),
        PAYLOAD_B,
        Duration::from_secs(10),
    )
    .await?;
    receive_expected(
        &mut subscribers.direct_zero,
        &mapped_channel("direct-zero", "binary"),
        PAYLOAD_B,
        Duration::from_secs(10),
    )
    .await?;

    publish(&mut publisher, PAYLOAD_A).await?;
    receive_expected(
        &mut subscribers.ttl,
        &mapped_channel("ttl", "binary"),
        PAYLOAD_A,
        Duration::from_secs(10),
    )
    .await?;
    receive_expected(
        &mut subscribers.window_zero,
        &mapped_channel("window-zero", "binary"),
        PAYLOAD_A,
        Duration::from_secs(10),
    )
    .await?;
    receive_expected(
        &mut subscribers.direct_zero,
        &mapped_channel("direct-zero", "binary"),
        PAYLOAD_A,
        Duration::from_secs(10),
    )
    .await?;

    let same_window_anchor = Instant::now();
    let b_to_a_b_sent_at = publish(&mut publisher, PAYLOAD_B).await?;
    let b_to_a_a_sent_at = publish(&mut publisher, PAYLOAD_A).await?;
    ensure!(
        b_to_a_b_sent_at.duration_since(same_window_anchor)
            < Duration::from_millis(CONFLATION_INTERVAL_MS / 2),
        "The B/A burst must start within the new conflation window"
    );
    ensure!(
        b_to_a_a_sent_at.duration_since(b_to_a_b_sent_at)
            < Duration::from_millis(CONFLATION_INTERVAL_MS / 2),
        "The B/A burst must fit inside one conflation window"
    );
    receive_expected(
        &mut subscribers.direct_zero,
        &mapped_channel("direct-zero", "binary"),
        PAYLOAD_B,
        Duration::from_secs(10),
    )
    .await?;
    receive_expected(
        &mut subscribers.direct_zero,
        &mapped_channel("direct-zero", "binary"),
        PAYLOAD_A,
        Duration::from_secs(10),
    )
    .await?;
    receive_expected(
        &mut subscribers.window_zero,
        &mapped_channel("window-zero", "binary"),
        PAYLOAD_A,
        Duration::from_secs(10),
    )
    .await?;
    let window_zero_flush_at = Instant::now();

    wait_for_status(
        &status_url,
        |current| {
            output_metric(current, "ttl", "deduplicated_messages_total") == Some(3)
                && output_metric(current, "ttl", "output_messages_total") == Some(4)
                && output_metric(current, "ttl", "pending_messages") == Some(0)
                && output_metric(current, "window-zero", "output_messages_total") == Some(7)
                && output_metric(current, "window-zero", "pending_messages") == Some(0)
        },
        "the same-window B/A burst to flush as cached A only on TTL 0",
        Duration::from_secs(15),
    )
    .await?;
    {
        let mut quiet = [&mut subscribers.ttl];
        assert_quiet(
            &mut quiet,
            Duration::from_millis(CONFLATION_INTERVAL_MS + 10),
        )
        .await?;
    }
    let second_window_anchor =
        next_window_start(window_zero_flush_at, CONFLATION_INTERVAL_MS, 2).await;

    let b_to_c_b_sent_at = publish(&mut publisher, PAYLOAD_B).await?;
    let b_to_c_c_sent_at = publish(&mut publisher, PAYLOAD_C).await?;
    ensure!(
        b_to_c_b_sent_at.duration_since(second_window_anchor)
            < Duration::from_millis(CONFLATION_INTERVAL_MS / 2),
        "The B/C burst must start in the next conflation window"
    );
    ensure!(
        b_to_c_c_sent_at.duration_since(b_to_c_b_sent_at)
            < Duration::from_millis(CONFLATION_INTERVAL_MS / 2),
        "The B/C burst must fit inside one conflation window"
    );
    receive_expected(
        &mut subscribers.direct_zero,
        &mapped_channel("direct-zero", "binary"),
        PAYLOAD_B,
        Duration::from_secs(10),
    )
    .await?;
    receive_expected(
        &mut subscribers.direct_zero,
        &mapped_channel("direct-zero", "binary"),
        PAYLOAD_C,
        Duration::from_secs(10),
    )
    .await?;
    receive_expected(
        &mut subscribers.window_zero,
        &mapped_channel("window-zero", "binary"),
        PAYLOAD_C,
        Duration::from_secs(10),
    )
    .await?;
    receive_expected(
        &mut subscribers.ttl,
        &mapped_channel("ttl", "binary"),
        PAYLOAD_C,
        Duration::from_secs(10),
    )
    .await?;
    let mut quiet = subscribers.all_mut();
    assert_quiet(&mut quiet, Duration::from_millis(500)).await?;

    let status = wait_for_status(
        &status_url,
        |current| {
            if current.get("state").and_then(Value::as_str) != Some("running")
                || metric(current, "input_messages_total") != Some(10)
            {
                return false;
            }
            let Some(outputs) = current.get("outputs").and_then(Value::as_object) else {
                return false;
            };
            outputs.len() == 3
                && ["ttl", "window-zero", "direct-zero"]
                    .iter()
                    .all(|name| outputs.contains_key(*name))
                && [("ttl", 5_u64), ("window-zero", 8), ("direct-zero", 10)]
                    .iter()
                    .all(|(name, count)| {
                        outputs.get(*name).is_some_and(|metrics| {
                            metrics.get("output_messages_total").and_then(Value::as_u64)
                                == Some(*count)
                                && metrics.get("pending_messages").and_then(Value::as_u64)
                                    == Some(0)
                                && metrics.get("pending_payload_bytes").and_then(Value::as_u64)
                                    == Some(0)
                        })
                    })
        },
        "all TTL, conflated TTL-zero, and direct TTL-zero publications to finish",
        Duration::from_secs(15),
    )
    .await?;

    let outputs = status
        .get("outputs")
        .and_then(Value::as_object)
        .context("status has no outputs object")?;
    let ttl_metrics = outputs.get("ttl").context("status has no ttl output")?;
    let window_zero_metrics = outputs
        .get("window-zero")
        .context("status has no window-zero output")?;
    let direct_zero_metrics = outputs
        .get("direct-zero")
        .context("status has no direct-zero output")?;
    let payload_a = PAYLOAD_A.len() as u64;
    let payload_b = PAYLOAD_B.len() as u64;
    let payload_c = PAYLOAD_C.len() as u64;

    ensure!(
        metric(ttl_metrics, "deduplicated_messages_total") == Some(3),
        "{ttl_metrics}"
    );
    ensure!(
        metric(ttl_metrics, "deduplicated_payload_bytes_total") == Some(2 * payload_a + payload_b),
        "{ttl_metrics}"
    );
    for (name, metrics) in [
        ("window-zero", window_zero_metrics),
        ("direct-zero", direct_zero_metrics),
    ] {
        ensure!(
            metric(metrics, "deduplicated_messages_total") == Some(0),
            "{name}: {metrics}"
        );
        ensure!(
            metric(metrics, "deduplicated_payload_bytes_total") == Some(0),
            "{name}: {metrics}"
        );
    }
    ensure!(
        metric(ttl_metrics, "output_payload_bytes_total")
            == Some(2 * payload_a + 2 * payload_b + payload_c),
        "{ttl_metrics}"
    );
    ensure!(
        metric(window_zero_metrics, "output_payload_bytes_total")
            == Some(4 * payload_a + 3 * payload_b + payload_c),
        "{window_zero_metrics}"
    );
    ensure!(
        metric(direct_zero_metrics, "output_payload_bytes_total")
            == Some(4 * payload_a + 5 * payload_b + payload_c),
        "{direct_zero_metrics}"
    );
    for (name, metrics) in outputs {
        ensure!(
            metric(metrics, "publish_errors_total") == Some(0),
            "{name}: {metrics}"
        );
        ensure!(
            metric(metrics, "pending_messages") == Some(0),
            "{name}: {metrics}"
        );
        ensure!(
            metric(metrics, "pending_payload_bytes") == Some(0),
            "{name}: {metrics}"
        );
        ensure!(
            metric(metrics, "pending_keys") == Some(0),
            "{name}: {metrics}"
        );
    }

    exercise_channel_policies(&mut publisher, &mut subscribers).await?;
    let mut quiet = subscribers.all_mut();
    assert_quiet(&mut quiet, Duration::from_millis(450)).await?;
    let final_status = wait_for_status(
        &status_url,
        |current| {
            current.get("state").and_then(Value::as_str) == Some("running")
                && metric(current, "input_messages_total") == Some(40)
                && [("ttl", 30_u64), ("window-zero", 38), ("direct-zero", 40)]
                    .iter()
                    .all(|(name, count)| {
                        output_metric(current, name, "output_messages_total") == Some(*count)
                            && output_metric(current, name, "pending_messages") == Some(0)
                            && output_metric(current, name, "pending_payload_bytes") == Some(0)
                    })
        },
        "all channel-policy TTL and independently scheduled interval cases to finish",
        Duration::from_secs(15),
    )
    .await?;

    let final_outputs = final_status
        .get("outputs")
        .and_then(Value::as_object)
        .context("final status has no outputs object")?;
    let final_ttl_metrics = final_outputs
        .get("ttl")
        .context("final status has no ttl output")?;
    let final_window_zero = final_outputs
        .get("window-zero")
        .context("final status has no window-zero output")?;
    let final_direct_zero = final_outputs
        .get("direct-zero")
        .context("final status has no direct-zero output")?;
    ensure!(
        metric(final_ttl_metrics, "deduplicated_messages_total") == Some(8),
        "{final_ttl_metrics}"
    );
    ensure!(
        metric(final_ttl_metrics, "deduplicated_payload_bytes_total")
            == Some(7 * payload_a + payload_b),
        "{final_ttl_metrics}"
    );
    ensure!(
        metric(final_ttl_metrics, "output_payload_bytes_total")
            == Some(22 * payload_a + 6 * payload_b + 2 * payload_c),
        "{final_ttl_metrics}"
    );
    ensure!(
        metric(final_window_zero, "output_payload_bytes_total")
            == Some(29 * payload_a + 7 * payload_b + 2 * payload_c),
        "{final_window_zero}"
    );
    ensure!(
        metric(final_direct_zero, "output_payload_bytes_total")
            == Some(29 * payload_a + 9 * payload_b + 2 * payload_c),
        "{final_direct_zero}"
    );
    for (name, metrics) in final_outputs {
        ensure!(
            metric(metrics, "publish_errors_total") == Some(0),
            "{name}: {metrics}"
        );
        ensure!(
            metric(metrics, "pending_messages") == Some(0),
            "{name}: {metrics}"
        );
        ensure!(
            metric(metrics, "pending_payload_bytes") == Some(0),
            "{name}: {metrics}"
        );
        ensure!(
            metric(metrics, "pending_keys") == Some(0),
            "{name}: {metrics}"
        );
    }

    exercise_sighup_reload(
        &mut publisher,
        &mut subscribers,
        &status_url,
        &config_path,
        &log_dir,
    )
    .await?;

    println!(
        "TTL/profile/group E2E passed: profiles, ordered selectors and default, \
         individual expiry, floor-rounded and fixed/reset group expiry, valid SIGHUP reload, \
         and rejected invalid-file reload without replacing the active config; \
         independent 100/300 ms intervals behaved on mapped channels; TTL 0 \
         still conflated at 50 ms and direct TTL 0 forwarded every input."
    );
    Ok(())
}
