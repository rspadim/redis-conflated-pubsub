use std::{path::PathBuf, str::FromStr, time::Duration};

use anyhow::{Context, Result, ensure};
use async_nats::{Client, ConnectOptions, ServerAddr};
use bytes::Bytes;
use futures_util::StreamExt;
use redis::AsyncCommands;
use tokio::time;

mod echo;
mod load;

const MESSAGE_TIMEOUT: Duration = Duration::from_secs(5);

#[tokio::main]
async fn main() -> Result<()> {
    let servers = env_or(
        "NATS_SERVERS",
        "nats://127.0.0.1:14222,nats://127.0.0.1:14223,nats://127.0.0.1:14224",
    );
    let redis_url = env_or("REDIS_URL", "redis://127.0.0.1:16380");
    let ws_url = env_or("NATS_WS_URL", "ws://127.0.0.1:18080");
    let wss_url = env_or("NATS_WSS_URL", "wss://localhost:18443");
    let wss_ca = env_or("NATS_WSS_CA_CERT", "spikes/nats/certs/ca-cert.pem");
    let payload = b"nats transport spike\0binary\xffpayload";

    let nats = connect_nats(&servers, "nats-cluster-spike").await?;
    let pool = nats.server_pool().await?;
    println!(
        "tcp_cluster_connected=true server_pool_entries={} max_payload_bytes={}",
        pool.len(),
        nats.max_payload()
    );
    if std::env::var("NATS_PROBE_ONLY")
        .is_ok_and(|value| matches!(value.as_str(), "1" | "true" | "yes"))
    {
        println!("nats_probe_only=true no_subscribe_or_publish=true");
        return Ok(());
    }
    if std::env::var("NATS_AUTH_ROUNDTRIP")
        .is_ok_and(|value| matches!(value.as_str(), "1" | "true" | "yes"))
    {
        auth_roundtrip_smoke(&nats).await?;
        return Ok(());
    }
    if std::env::var("NATS_LOAD_TEST")
        .is_ok_and(|value| matches!(value.as_str(), "1" | "true" | "yes"))
    {
        load::run(&nats, &servers).await?;
        return Ok(());
    }
    if std::env::var("NATS_ECHO_PROBE")
        .is_ok_and(|value| matches!(value.as_str(), "1" | "true" | "yes"))
    {
        echo::run(&servers).await?;
        return Ok(());
    }

    redis_to_nats_smoke(&nats, &redis_url, payload).await?;
    nats_to_redis_smoke(&nats, &redis_url, payload).await?;
    websocket_smoke(&ws_url, payload).await?;
    websocket_tls_smoke(&wss_url, &wss_ca, payload).await?;
    if let Ok(seconds) = std::env::var("NATS_FAILOVER_PROBE_SECS")
        && let Ok(seconds) = seconds.parse::<u64>()
        && seconds > 0
    {
        cluster_reconnect_smoke(&nats, seconds).await?;
    }

    println!("redis_to_nats=true nats_to_redis=true websocket=true");
    Ok(())
}

async fn connect_nats(servers: &str, name: &str) -> Result<Client> {
    connect_nats_with_ca(servers, name, None).await
}

async fn connect_nats_with_ca(
    servers: &str,
    name: &str,
    root_certificate: Option<PathBuf>,
) -> Result<Client> {
    let addresses = servers
        .split(',')
        .map(str::trim)
        .filter(|address| !address.is_empty())
        .map(ServerAddr::from_str)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    ensure!(
        !addresses.is_empty(),
        "NATS_SERVERS must contain at least one URL"
    );

    let mut options = ConnectOptions::new()
        .name(name)
        .max_reconnects(None)
        .retain_servers_order();
    let root_certificate = root_certificate.or_else(|| {
        std::env::var("NATS_ROOT_CERT_FILE")
            .ok()
            .filter(|path| !path.is_empty())
            .map(PathBuf::from)
    });
    if let Some(root_certificate) = root_certificate {
        options = options.add_root_certificates(root_certificate);
    }

    match env_or("NATS_AUTH_MODE", "auto").as_str() {
        "auto" => {
            if let Ok(token) = std::env::var("NATS_TOKEN")
                && !token.is_empty()
            {
                options = options.token(token);
            }
        }
        "none" => {}
        "user_password" => {
            let username = std::env::var("NATS_USERNAME")
                .context("NATS_USERNAME is required for user_password auth")?;
            let password = std::env::var("NATS_PASSWORD")
                .context("NATS_PASSWORD is required for user_password auth")?;
            options = options.user_and_password(username, password);
        }
        "token" => {
            let token =
                std::env::var("NATS_TOKEN").context("NATS_TOKEN is required for token auth")?;
            options = options.token(token);
        }
        "nkey" => {
            let path = std::env::var("NATS_NKEY_SEED_FILE")
                .context("NATS_NKEY_SEED_FILE is required for NKey auth")?;
            let seed = std::fs::read_to_string(path)?;
            options = options.nkey(seed.trim().to_owned());
        }
        "credentials_file" => {
            let path = std::env::var("NATS_CREDENTIALS_FILE")
                .context("NATS_CREDENTIALS_FILE is required for credentials-file auth")?;
            options = options.credentials_file(path).await?;
        }
        "mtls" => {
            let certificate = std::env::var("NATS_CLIENT_CERT_FILE")
                .context("NATS_CLIENT_CERT_FILE is required for mTLS")?;
            let key = std::env::var("NATS_CLIENT_KEY_FILE")
                .context("NATS_CLIENT_KEY_FILE is required for mTLS")?;
            options = options.add_client_certificate(certificate.into(), key.into());
        }
        mode => anyhow::bail!("unsupported NATS_AUTH_MODE {mode:?}"),
    }

    options
        .connect(addresses)
        .await
        .context("connecting to NATS")
}

async fn redis_to_nats_smoke(nats: &Client, redis_url: &str, payload: &[u8]) -> Result<()> {
    let redis = redis::Client::open(redis_url)?;
    let nonce = unique_suffix();
    let redis_channel = format!("spike.redis.input.{nonce}");
    let nats_subject = format!("spike.redis.output.{nonce}");

    let mut nats_subscriber = nats.subscribe(nats_subject.clone()).await?;
    let relay_redis = redis.clone();
    let relay_channel = redis_channel.clone();
    let relay_client = nats.clone();
    let relay_subject = nats_subject.clone();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let relay = tokio::spawn(async move {
        let mut redis_pubsub = relay_redis.get_async_pubsub().await?;
        redis_pubsub.subscribe(&relay_channel).await?;
        let _ = ready_tx.send(());
        let mut redis_messages = redis_pubsub.on_message();
        let message = time::timeout(MESSAGE_TIMEOUT, redis_messages.next())
            .await
            .context("timed out waiting for Redis input")?
            .context("Redis Pub/Sub stream closed")?;
        relay_client
            .publish(
                relay_subject,
                Bytes::copy_from_slice(message.get_payload_bytes()),
            )
            .await?;
        relay_client.flush().await?;
        Result::<()>::Ok(())
    });
    ready_rx
        .await
        .context("Redis input subscription did not start")?;

    let mut connection = redis.get_multiplexed_async_connection().await?;
    let _: i64 = connection.publish(&redis_channel, payload).await?;
    let received = time::timeout(MESSAGE_TIMEOUT, nats_subscriber.next())
        .await
        .context("timed out waiting for NATS output")?
        .context("NATS subscription closed")?;
    relay.await??;
    ensure!(
        received.payload.as_ref() == payload,
        "Redis→NATS payload mismatch"
    );
    println!("redis_to_nats_payload_bytes={}", received.payload.len());
    Ok(())
}

async fn nats_to_redis_smoke(nats: &Client, redis_url: &str, payload: &[u8]) -> Result<()> {
    let redis = redis::Client::open(redis_url)?;
    let nonce = unique_suffix();
    let nats_subject = format!("spike.nats.input.{nonce}");
    let redis_channel = format!("spike.nats.output.{nonce}");

    let nats_subscriber = nats.subscribe(nats_subject.clone()).await?;

    let relay_redis = redis.clone();
    let relay_channel = redis_channel.clone();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let relay = tokio::spawn(async move {
        let mut redis_pubsub = relay_redis.get_async_pubsub().await?;
        redis_pubsub.subscribe(&relay_channel).await?;
        let _ = ready_tx.send(());
        let mut redis_messages = redis_pubsub.on_message();
        let mut messages = nats_subscriber;
        let message = time::timeout(MESSAGE_TIMEOUT, messages.next())
            .await
            .context("timed out waiting for NATS input")?
            .context("NATS subscription closed")?;
        let mut connection = relay_redis.get_multiplexed_async_connection().await?;
        let _: i64 = connection
            .publish(relay_channel, message.payload.as_ref())
            .await?;
        let received = time::timeout(MESSAGE_TIMEOUT, redis_messages.next())
            .await
            .context("timed out waiting for Redis output")?
            .context("Redis Pub/Sub stream closed")?;
        Result::<Vec<u8>>::Ok(received.get_payload_bytes().to_vec())
    });
    ready_rx
        .await
        .context("Redis output subscription did not start")?;

    nats.publish(nats_subject, Bytes::copy_from_slice(payload))
        .await?;
    nats.flush().await?;
    let received_payload = relay.await??;
    ensure!(received_payload == payload, "NATS→Redis payload mismatch");
    println!("nats_to_redis_payload_bytes={}", received_payload.len());
    Ok(())
}

async fn websocket_smoke(ws_url: &str, payload: &[u8]) -> Result<()> {
    let client = connect_nats(ws_url, "nats-websocket-spike").await?;
    let subject = format!("spike.websocket.{}", unique_suffix());
    let mut subscriber = client.subscribe(subject.clone()).await?;
    client
        .publish(subject, Bytes::copy_from_slice(payload))
        .await?;
    client.flush().await?;

    let received = time::timeout(MESSAGE_TIMEOUT, subscriber.next())
        .await
        .context("timed out waiting for WebSocket NATS message")?
        .context("WebSocket NATS subscription closed")?;
    ensure!(
        received.payload.as_ref() == payload,
        "WebSocket payload mismatch"
    );
    println!("websocket_payload_bytes={}", received.payload.len());
    Ok(())
}

async fn websocket_tls_smoke(wss_url: &str, root_certificate: &str, payload: &[u8]) -> Result<()> {
    let client = connect_nats_with_ca(
        wss_url,
        "nats-websocket-tls-spike",
        Some(PathBuf::from(root_certificate)),
    )
    .await?;
    let subject = format!("spike.wss.{}", unique_suffix());
    let mut subscriber = client.subscribe(subject.clone()).await?;
    client
        .publish(subject, Bytes::copy_from_slice(payload))
        .await?;
    client.flush().await?;

    let received = time::timeout(MESSAGE_TIMEOUT, subscriber.next())
        .await
        .context("timed out waiting for WSS NATS message")?
        .context("WSS NATS subscription closed")?;
    ensure!(received.payload.as_ref() == payload, "WSS payload mismatch");
    println!("wss_payload_bytes={}", received.payload.len());
    Ok(())
}

async fn auth_roundtrip_smoke(client: &Client) -> Result<()> {
    let mode = env_or("NATS_AUTH_MODE", "auto");
    let subject = format!("spike.auth.{}", unique_suffix());
    let payload = format!("authenticated-{mode}").into_bytes();
    let mut subscriber = client.subscribe(subject.clone()).await?;
    client
        .publish(subject, Bytes::from(payload.clone()))
        .await?;
    client.flush().await?;

    let received = time::timeout(MESSAGE_TIMEOUT, subscriber.next())
        .await
        .context("timed out waiting for NATS auth smoke message")?
        .context("NATS auth smoke subscription closed")?;
    ensure!(
        received.payload.as_ref() == payload,
        "NATS auth payload mismatch"
    );
    println!("nats_auth_mode={mode} authenticated_roundtrip=true");
    Ok(())
}

async fn cluster_reconnect_smoke(client: &Client, seconds: u64) -> Result<()> {
    let subject = format!("spike.failover.{}", unique_suffix());
    let mut subscriber = client.subscribe(subject.clone()).await?;
    let deadline = time::Instant::now() + Duration::from_secs(seconds);
    let mut messages = 0u64;
    println!("cluster_reconnect_probe_started seconds={seconds}");

    while time::Instant::now() < deadline {
        let payload = format!("probe-{messages}").into_bytes();
        client
            .publish(subject.clone(), Bytes::from(payload.clone()))
            .await?;
        client.flush().await?;
        let received = time::timeout(MESSAGE_TIMEOUT, subscriber.next())
            .await
            .context("timed out waiting for failover probe message")?
            .context("failover probe subscription closed")?;
        ensure!(
            received.payload.as_ref() == payload,
            "failover probe payload mismatch"
        );
        messages += 1;
        time::sleep(Duration::from_millis(250)).await;
    }

    println!("cluster_reconnect_probe_messages={messages}");
    Ok(())
}

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_owned())
}

fn unique_suffix() -> String {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock is after the Unix epoch")
        .as_nanos()
        .to_string()
}
