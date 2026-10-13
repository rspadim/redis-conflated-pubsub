//! NATS output adapter: publishes an ordered chunk and flushes it.
//!
//! Core NATS has no per-message acknowledgement and no subscriber count.
//! `async-nats` `flush()` only drains the client's own write buffer down to the
//! socket; it does not wait for the server to process the commands. The sink
//! therefore publishes every message in order and calls `flush` to push them
//! onto the wire, but neither the returned value nor the flush promises
//! delivery: the count is always `0`, and completion means only "the commands
//! left the local buffer".

use std::time::Duration;

use async_nats::{Client, HeaderMap, HeaderValue, PublishErrorKind};
use bytes::Bytes;
use tokio::time;

use crate::config::OutputConfig;

use super::super::PendingMessage;
use super::super::backend::{NATS_HOP_HEADER, OutputSink};
use super::super::publish::PublishFailure;
use super::connect::{ConnectionPlan, connect_failure, connect_with_metrics, connection_plan};

/// Why a NATS publish step stopped before the flush barrier.
#[derive(Debug)]
pub(in crate::service) enum PublishStep {
    /// The client rejected the message before it entered the wire buffer, so it
    /// can never reach the server (`NotSent`).
    NotSent(String),
    /// The command may already be on the wire; treat it as ambiguous.
    Uncertain(String),
}

/// Core NATS never reports a subscriber count. The sink returns this value on
/// success so the worker's `subscribers` log field and status never imply one.
pub(in crate::service) const NATS_NO_SUBSCRIBER_COUNT: i64 = 0;

/// A cheap-to-clone publish/flush handle for one connected NATS client.
///
/// `Client` is internally reference counted, so cloning a handle (rather than
/// borrowing it) lets the direct lane own a connection handle inside its
/// in-flight future, mirroring how the Redis path clones its multiplexed
/// connection.
#[derive(Clone)]
pub(in crate::service) struct NatsSender {
    client: Client,
    max_payload: usize,
    timeout: Duration,
}

impl NatsSender {
    fn new(client: Client, timeout: Duration) -> Self {
        let max_payload = client.max_payload();
        Self {
            client,
            max_payload,
            timeout,
        }
    }
}

impl OutputSink for NatsSender {
    async fn publish(
        &mut self,
        messages: &[PendingMessage],
        atomic: bool,
    ) -> std::result::Result<i64, PublishFailure> {
        if messages.is_empty() {
            return Err(PublishFailure::NotSent(
                "output publish batch is empty".to_owned(),
            ));
        }
        // Core NATS is not transactional. The chunk is still published strictly
        // in order, and `flush` settles it, but no atomicity is claimed; the
        // `atomic` hint is intentionally ignored.
        let _ = atomic;
        match time::timeout(
            self.timeout,
            publish_chunk(&self.client, messages, self.max_payload),
        )
        .await
        {
            Err(_) => Err(PublishFailure::Uncertain(
                "timed out enqueueing NATS publish commands".to_owned(),
            )),
            Ok(Err(PublishStep::NotSent(error))) => Err(PublishFailure::NotSent(error)),
            Ok(Err(PublishStep::Uncertain(error))) => Err(PublishFailure::Uncertain(error)),
            // No acknowledgement or subscriber count exists in Core NATS.
            Ok(Ok(())) => Ok(NATS_NO_SUBSCRIBER_COUNT),
        }
    }

    async fn flush(&mut self) -> std::result::Result<(), PublishFailure> {
        match time::timeout(self.timeout, self.client.flush()).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(PublishFailure::Uncertain(format!(
                "NATS flush failed after publishing: {error}"
            ))),
            Err(_) => Err(PublishFailure::Uncertain(
                "timed out flushing NATS publish commands".to_owned(),
            )),
        }
    }
}

/// Connection-owning NATS output sink. `async-nats` reconnects internally, so
/// the worker keeps one client and only discards it after a failure so the next
/// publish re-runs the connection setup.
pub(in crate::service) struct NatsOutputSink {
    plan: ConnectionPlan,
    sender: Option<NatsSender>,
    /// Optional sink for NATS client connection events; `None` in unit tests.
    metrics: Option<std::sync::Arc<crate::status::Metrics>>,
}

impl NatsOutputSink {
    /// Builds the sink from an output configuration. Fails if the output is not
    /// a NATS endpoint or the server list is invalid.
    pub(in crate::service) fn new(config: &OutputConfig) -> anyhow::Result<Self> {
        let nats = config
            .nats
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("NatsOutputSink requires a `nats` output endpoint"))?;
        let plan = connection_plan(nats)?;
        Ok(Self {
            plan,
            sender: None,
            metrics: None,
        })
    }

    /// Attaches a metrics registry so NATS connect/disconnect events are counted.
    pub(in crate::service) fn with_metrics(
        mut self,
        metrics: std::sync::Arc<crate::status::Metrics>,
    ) -> Self {
        self.metrics = Some(metrics);
        self
    }

    pub(in crate::service) fn is_connected(&self) -> bool {
        self.sender.is_some()
    }

    /// Drops the client so the next publish reconnects through options.
    pub(in crate::service) fn disconnect(&mut self) {
        self.sender = None;
    }

    /// Connected handle for the direct lane to own inside its in-flight future.
    pub(in crate::service) fn sender(&self) -> Option<NatsSender> {
        self.sender.clone()
    }

    /// Connects if needed. A connection failure means nothing was sent on this
    /// connection, so it maps to `PublishFailure::NotSent`.
    pub(in crate::service) async fn ensure_connected(
        &mut self,
    ) -> std::result::Result<(), PublishFailure> {
        if self.sender.is_some() {
            return Ok(());
        }
        let client = connect_with_metrics(&self.plan, self.metrics.clone())
            .await
            .map_err(connect_failure)?;
        self.sender = Some(NatsSender::new(client, self.plan.connect_timeout));
        Ok(())
    }

    /// Flushes anything still buffered on the client at shutdown. `async-nats`
    /// `flush` pushes the locally buffered publish commands to the socket; it
    /// does not wait for the server to process them, but it is the most the
    /// client can do before exit. A disconnected sink has nothing buffered, so
    /// this is a no-op. Errors are intentionally ignored: shutdown must not
    /// block on an unreachable server.
    pub(in crate::service) async fn flush_before_shutdown(&mut self) {
        if let Some(sender) = self.sender.as_mut() {
            let _ = sender.flush().await;
        }
    }
}

impl OutputSink for NatsOutputSink {
    /// Connects on demand (like `RedisOutputSink::publish` does through its
    /// internal `ensure_output_connection`) and discards the client on failure.
    ///
    /// This is what keeps the conflated lane honest: the conflated flush path
    /// calls `publish`/`flush` directly through `OutputConnection` and never
    /// calls `ensure_connected` itself, so the sink must reconnect here.
    /// Discarding on failure makes the next attempt build a fresh client from
    /// `ConnectOptions`, mirroring the Redis path's reconnect-on-next-publish.
    async fn publish(
        &mut self,
        messages: &[PendingMessage],
        atomic: bool,
    ) -> std::result::Result<i64, PublishFailure> {
        // `ensure_connected` leaves the sink disconnected on failure, so the
        // next publish retries the connection.
        self.ensure_connected().await?;
        let result = match self.sender.as_mut() {
            Some(sender) => sender.publish(messages, atomic).await,
            None => Err(PublishFailure::NotSent(
                "NATS output is not connected".to_owned(),
            )),
        };
        // Discard the client when the connection may be broken (a send or flush
        // failure). A definitive pre-send rejection (invalid mapped subject,
        // oversized payload) leaves a healthy client in place, so a repeated
        // mapping error does not trigger a reconnect storm.
        if matches!(result, Err(PublishFailure::Uncertain(_))) {
            self.disconnect();
        }
        result
    }

    /// Settles the chunk through the client's flush barrier. A disconnected sink
    /// has no buffered command to settle, so this succeeds without attempting a
    /// new connection (the following publish is what reconnects). A flush
    /// failure discards the client so the next publish rebuilds it.
    async fn flush(&mut self) -> std::result::Result<(), PublishFailure> {
        let result = match self.sender.as_mut() {
            Some(sender) => sender.flush().await,
            None => Ok(()),
        };
        if result.is_err() {
            self.disconnect();
        }
        result
    }
}

async fn publish_chunk(
    client: &Client,
    messages: &[PendingMessage],
    max_payload: usize,
) -> std::result::Result<(), PublishStep> {
    // Stamp every forwarded message so a NATS input that also matches this
    // subject recognises it as a relay echo and drops it. The value is
    // constant, so the headers are built once per chunk.
    let mut headers = HeaderMap::new();
    headers.insert(NATS_HOP_HEADER, HeaderValue::from("1"));
    for message in messages {
        // Reject non-concrete or malformed publish subjects with the same
        // message `--check-config` uses, before the client would.
        if let Err(error) = crate::config::validate_nats_publish_subject(
            "NATS publish subject",
            &message.output_channel,
        ) {
            return Err(PublishStep::NotSent(error.to_string()));
        }
        check_payload_size(&message.output_channel, message.payload.len(), max_payload)?;
        // `Bytes::from_owner` shares the `Arc<[u8]>` payload without copying.
        let payload = Bytes::from_owner(message.payload.clone());
        match client
            .publish_with_headers(message.output_channel.clone(), headers.clone(), payload)
            .await
        {
            Ok(()) => {}
            Err(error) => {
                let kind = error.kind();
                let text = format!("{error}");
                return Err(classify_publish_step(&message.output_channel, kind, &text));
            }
        }
    }
    Ok(())
}

/// Maps a client publish failure to [`PublishStep`].
///
/// `MaxPayloadExceeded` and `InvalidSubject` are rejected by the client before
/// the command enters the wire buffer, so they are definitive (`NotSent`).
/// `Send` means the command may already be buffered on the connection, so it is
/// ambiguous (`Uncertain`) and the caller discards the client before retrying.
/// Extracted so the mapping is unit-testable without a live client.
pub(in crate::service) fn classify_publish_step(
    subject: &str,
    kind: PublishErrorKind,
    error: &str,
) -> PublishStep {
    let text = format!("NATS publish to subject {subject:?} failed: {error}");
    match kind {
        PublishErrorKind::MaxPayloadExceeded | PublishErrorKind::InvalidSubject => {
            PublishStep::NotSent(text)
        }
        PublishErrorKind::Send => PublishStep::Uncertain(text),
    }
}

/// Enforces the server-advertised payload limit before enqueueing, so the
/// classification is a definitive `NotSent` rather than relying on the client
/// error. Reuses the same policy the batch sizing does.
pub(in crate::service) fn check_payload_size(
    subject: &str,
    payload_len: usize,
    max_payload: usize,
) -> std::result::Result<(), PublishStep> {
    if payload_len > max_payload {
        return Err(PublishStep::NotSent(format!(
            "NATS subject {subject:?} payload of {payload_len} bytes exceeds the server maximum of {max_payload} bytes"
        )));
    }
    Ok(())
}
