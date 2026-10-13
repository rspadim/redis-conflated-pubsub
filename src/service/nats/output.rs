//! NATS output adapter: publishes an ordered chunk and flushes it.
//!
//! Core NATS has no per-message acknowledgement and no subscriber count. The
//! sink therefore publishes every message in order and then issues one
//! `flush`, which is a server round-trip barrier for the commands already
//! buffered on this connection. Completion means "the server processed our
//! publish commands", not "a subscriber received them": the returned count is
//! always `0` and the honest completion signal is the flush round trip.

use std::time::Duration;

use async_nats::{Client, HeaderMap, HeaderValue, PublishErrorKind};
use bytes::Bytes;
use tokio::time;

use crate::config::OutputConfig;

use super::super::PendingMessage;
use super::super::backend::{NATS_HOP_HEADER, OutputSink};
use super::super::publish::PublishFailure;
use super::connect::{ConnectionPlan, connect, connect_failure, connection_plan};

/// Why a NATS publish step stopped before the flush barrier.
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
        Ok(Self { plan, sender: None })
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
        let client = connect(&self.plan).await.map_err(connect_failure)?;
        self.sender = Some(NatsSender::new(client, self.plan.connect_timeout));
        Ok(())
    }
}

impl OutputSink for NatsOutputSink {
    async fn publish(
        &mut self,
        messages: &[PendingMessage],
        atomic: bool,
    ) -> std::result::Result<i64, PublishFailure> {
        match self.sender.as_mut() {
            Some(sender) => sender.publish(messages, atomic).await,
            None => Err(PublishFailure::NotSent(
                "NATS output is not connected".to_owned(),
            )),
        }
    }

    async fn flush(&mut self) -> std::result::Result<(), PublishFailure> {
        match self.sender.as_mut() {
            Some(sender) => sender.flush().await,
            None => Err(PublishFailure::NotSent(
                "NATS output is not connected".to_owned(),
            )),
        }
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
                let text = format!(
                    "NATS publish to subject {:?} failed: {error}",
                    message.output_channel
                );
                return Err(match error.kind() {
                    // Rejected before the command entered the wire buffer.
                    PublishErrorKind::MaxPayloadExceeded | PublishErrorKind::InvalidSubject => {
                        PublishStep::NotSent(text)
                    }
                    // The command may have been buffered and sent.
                    PublishErrorKind::Send => PublishStep::Uncertain(text),
                });
            }
        }
    }
    Ok(())
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
