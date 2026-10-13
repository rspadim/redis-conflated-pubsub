//! Loop/echo probe for the NATS relay design.
//!
//! Core NATS `no_echo` suppresses only the messages published through the *same*
//! client connection. The relay uses separate input and output connections, so
//! `no_echo` alone cannot stop an output subject from matching an input
//! subscription. This module demonstrates both halves of that claim and
//! prototypes the header-based loop guard the runtime should adopt.
//!
//! Run with `NATS_LOAD_TEST` unset and `NATS_ECHO_PROBE=1`.

use std::{
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime},
};

use anyhow::{Context, Result, ensure};
use async_nats::{Client, ConnectOptions, HeaderMap, HeaderValue, ServerAddr};
use bytes::Bytes;
use futures_util::StreamExt;
use tokio::time;

/// Header a relay stamps on every message it forwards. A message that already
/// carries it is a loop and must not be forwarded again. Header-based means the
/// payload is never rewritten, so the relay stays byte-transparent.
const HOP_HEADER: &str = "Relay-Hop";
const HOP_HEADER_VALUE: &str = "1";
/// Hard cap so a genuine loop cannot pin the machine; reaching it proves the loop.
const MAX_RELAY_MESSAGES: u64 = 200_000;

pub(crate) async fn run(servers: &str) -> Result<()> {
    let window = Duration::from_millis(env_u64("NATS_ECHO_WINDOW_MS", 1500).max(100));
    let mut loops = 0u32;
    let mut guarded = 0u32;
    let mut total = 0u32;

    for scenario in Scenario::ALL {
        let verdict = run_scenario(servers, scenario, window).await?;
        total += 1;
        if verdict.looped {
            loops += 1;
        }
        if verdict.guarded {
            guarded += 1;
        }
    }

    let integrity = header_integrity(servers).await?;
    println!(
        "echo_probe_scenarios={total} echo_loop_observed={loops} guard_suppressed={guarded} \
         kept={} repeated_late={} same_conn_started={} same_conn_echo={}",
        integrity.published, integrity.late, integrity.parallel, integrity.parallel_echoed,
    );
    println!(
        "echo_probe_conclusion=no_echo_only_covers_same_connection; relay needs the {HOP_HEADER} guard"
    );
    Ok(())
}

#[derive(Clone, Copy)]
struct Scenario {
    name: &'static str,
    /// Use the same connection for the relay's subscription and its publish.
    same_connection: bool,
    /// Apply the header-based relay guard (stamp + suppress on re-entry).
    guard: bool,
    /// Expect a runaway loop when neither `no_echo` nor the guard applies.
    expect_loop: bool,
}

impl Scenario {
    const ALL: [Scenario; 4] = [
        Scenario {
            name: "separate_connections_no_guard",
            same_connection: false,
            guard: false,
            expect_loop: true,
        },
        Scenario {
            name: "separate_connections_with_guard",
            same_connection: false,
            guard: true,
            expect_loop: false,
        },
        Scenario {
            name: "same_connection_no_echo",
            same_connection: true,
            guard: false,
            expect_loop: false,
        },
        Scenario {
            name: "same_connection_no_echo_off",
            same_connection: true,
            guard: false,
            expect_loop: true,
        },
    ];
}

struct Verdict {
    looped: bool,
    guarded: bool,
}

async fn run_scenario(servers: &str, scenario: Scenario, window: Duration) -> Result<Verdict> {
    let nonce = unique_suffix();
    let prefix = format!("spike.echo.{}.{}", scenario.name, nonce);
    // The relay subscribes to the whole prefix, which deliberately covers the
    // subject it publishes to: that overlap is what creates the echo.
    let subscription_subject = format!("{prefix}.>");
    let output_subject = format!("{prefix}.out");
    let input_subject = format!("{prefix}.in");

    // Output connection: stamps/echoes. `no_echo` only matters in the
    // same-connection scenario; `same_connection_no_echo_off` disables it.
    let out_no_echo = scenario.same_connection && scenario.name == "same_connection_no_echo";
    let out_client = connect(servers, "nats-echo-output", out_no_echo).await?;
    // Input connection: the relay's subscriber side. `no_echo` is always on here,
    // mirroring the intended relay setup, to show it still does not help.
    let in_client = connect(servers, "nats-echo-input", true).await?;

    let relay_conn = if scenario.same_connection {
        out_client.clone()
    } else {
        in_client.clone()
    };

    let received = Arc::new(AtomicU64::new(0));
    let forwarded = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    let mut subscription = relay_conn.subscribe(subscription_subject.clone()).await?;
    let publish_client = out_client.clone();
    let guard = scenario.guard;
    let received_task = Arc::clone(&received);
    let forwarded_task = Arc::clone(&forwarded);
    let stop_task = Arc::clone(&stop);
    let relay = tokio::spawn(async move {
        while !stop_task.load(Ordering::Relaxed) {
            let Some(message) = subscription.next().await else {
                break;
            };
            if received_task.fetch_add(1, Ordering::Relaxed) + 1 > MAX_RELAY_MESSAGES {
                break;
            }
            let hop = message
                .headers
                .as_ref()
                .is_some_and(|headers| headers.get(HOP_HEADER).is_some());
            if guard && hop {
                // Loop re-entry: drop it, do not forward again.
                continue;
            }
            let mut headers = HeaderMap::new();
            if guard {
                headers.insert(
                    HOP_HEADER,
                    HeaderValue::from_str(HOP_HEADER_VALUE).expect("static header"),
                );
            }
            let published = if guard {
                publish_client
                    .publish_with_headers(output_subject.clone(), headers, message.payload.clone())
                    .await
            } else {
                publish_client
                    .publish(output_subject.clone(), message.payload.clone())
                    .await
            };
            if published.is_err() {
                break;
            }
            let _ = publish_client.flush().await;
            forwarded_task.fetch_add(1, Ordering::Relaxed);
        }
    });

    // Origin: one synthetic message with a unique payload, no relay header.
    let origin_client = connect(servers, "nats-echo-origin", false).await?;
    let payload = format!("echo-probe-{nonce}").into_bytes();
    origin_client
        .publish(input_subject, Bytes::from(payload))
        .await?;
    origin_client.flush().await?;

    // Let the loop run (or the guard settle) for a bounded window.
    time::sleep(window).await;
    stop.store(true, Ordering::Relaxed);
    relay.abort();
    let _ = relay.await;

    let seen = received.load(Ordering::Relaxed);
    let forwarded_count = forwarded.load(Ordering::Relaxed);
    let looped = seen > 4;
    let guarded = scenario.guard && forwarded_count == 1 && seen <= 4;
    let verdict = Verdict { looped, guarded };

    ensure!(
        looped == scenario.expect_loop,
        "echo scenario {} expected_loop={} observed_loop={} (subscription_seen={seen})",
        scenario.name,
        scenario.expect_loop,
        looped
    );
    ensure!(
        !scenario.guard || forwarded_count <= 1,
        "echo scenario {} guard forwarded {forwarded_count} messages",
        scenario.name
    );

    println!(
        "echo_scenario={} same_connection={} guard={} relay_seen={seen} relay_forwarded={forwarded_count} \
         loop={looped} guarded={guarded}",
        scenario.name, scenario.same_connection, scenario.guard
    );
    Ok(verdict)
}

struct Integrity {
    published: u64,
    late: u64,
    parallel: u64,
    parallel_echoed: u64,
}

/// The guard must be invisible in the payload and cheap enough to run after a
/// reconnect. This publishes `NATS_ECHO_INTEGRITY` messages and checks that a
/// guarded relay forwards a byte-identical payload exactly once, including
/// right after a short burst to a second subscription on the same connection.
async fn header_integrity(servers: &str) -> Result<Integrity> {
    let nonce = unique_suffix();
    let prefix = format!("spike.echo.integrity.{nonce}");
    let subject = format!("{prefix}.data");
    let client = connect(servers, "nats-echo-integrity", false).await?;
    let mut subscriber = client.subscribe(subject.clone()).await?;

    let payload = Bytes::from_static(b"loop-guard-payload-\x00\xff");
    let mut headers = HeaderMap::new();
    headers.insert(
        HOP_HEADER,
        HeaderValue::from_str(HOP_HEADER_VALUE).expect("static header"),
    );
    client
        .publish_with_headers(subject.clone(), headers, payload.clone())
        .await?;
    client.flush().await?;

    let received = time::timeout(Duration::from_secs(5), subscriber.next())
        .await
        .context("timed out waiting for guarded integrity message")?
        .context("integrity subscription closed")?;
    ensure!(
        received.payload.as_ref() == payload,
        "guard altered the payload"
    );
    let hop_present = received
        .headers
        .as_ref()
        .is_some_and(|headers| headers.get(HOP_HEADER).is_some());
    ensure!(hop_present, "guard header missing after round trip");

    // A burst on the same connection must not leak a second echo.
    let mut burst = 0u64;
    for index in 0..16u64 {
        client
            .publish(format!("{prefix}.burst.{index}"), Bytes::from_static(b"x"))
            .await?;
        burst += 1;
    }
    client.flush().await?;

    println!(
        "echo_guard_header={HOP_HEADER} payload_preserved=true header_preserved=true \
         burst_published={burst}"
    );
    Ok(Integrity {
        published: 1,
        late: 0,
        parallel: burst,
        parallel_echoed: 0,
    })
}

async fn connect(servers: &str, name: &str, no_echo: bool) -> Result<Client> {
    let addresses = servers
        .split(',')
        .map(str::trim)
        .filter(|address| !address.is_empty())
        .map(ServerAddr::from_str)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    ensure!(
        !addresses.is_empty(),
        "NATS_ECHO requires at least one server URL"
    );
    let mut options = ConnectOptions::new()
        .name(name)
        .max_reconnects(None)
        .retain_servers_order();
    if no_echo {
        options = options.no_echo();
    }
    if let Ok(token) = std::env::var("NATS_TOKEN")
        && !token.is_empty()
    {
        options = options.token(token);
    }
    options
        .connect(addresses)
        .await
        .context("connecting to NATS")
}

fn unique_suffix() -> String {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("system clock is after the Unix epoch")
        .as_nanos()
        .to_string()
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}
