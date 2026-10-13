//! NATS input adapter: subscribes to each configured `nats_subject` and yields
//! normalized [`InputEnvelope`]s.
//!
//! One `async-nats` client owns every subscription. `async-nats` performs
//! reconnect and server discovery internally and re-installs the subscriptions
//! after a reconnect, so the adapter only needs an outer reconnect loop for the
//! case where the client or every subscriber stream is closed outright.
//!
//! A Core NATS message is delivered once per matching subscription, so
//! overlapping filters each produce their own envelope. The envelope carries
//! both the concrete `source_subject` the message arrived on and the
//! `subscription_identity` (the filter that produced it), which is what keeps
//! per-subscription `output_prefix`/`output_suffix` mappings intact.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::Poll;
use std::time::Duration;

use anyhow::{Context, Result};
use async_nats::{Client, Subscriber};
use futures_util::{StreamExt, future::poll_fn};
use tokio::time;
use tracing::{info, warn};

use crate::{
    config::{InputConfig, Subscription},
    status::Metrics,
};

use super::super::MAX_RETRY_DELAY;
use super::super::OutputSender;
use super::super::backend::{InputEnvelope, InputSource};
use super::super::filter::ChannelFilterSet;
use super::super::input::{OutputChannelFilter, fan_out};
use super::connect::{ConnectionPlan, connect, connection_plan};

/// One configured NATS subscription: the filter, its output mapping, and the
/// concrete subscriber stream once connected.
struct NatsSubscription {
    subject: String,
    output_prefix: String,
    output_suffix: String,
    stream: Option<Subscriber>,
}

/// NATS implementation of [`InputSource`].
pub(in crate::service) struct NatsInput {
    plan: ConnectionPlan,
    subscriptions: Vec<NatsSubscription>,
    client: Option<Client>,
    /// Round-robin cursor so a busy first subscription cannot starve the rest.
    next_index: usize,
    /// Backend address reported in the envelope, for logs and metrics.
    source_address: String,
}

impl NatsInput {
    /// Builds the adapter from configuration. Fails if the input is not a NATS
    /// endpoint or holds a non-NATS subscription (validation already rejects
    /// this, but the adapter is also constructed directly in tests).
    pub(in crate::service) fn new(config: &InputConfig) -> Result<Self> {
        let nats = config
            .nats
            .as_ref()
            .context("NatsInput requires a `nats` input endpoint")?;
        let plan = connection_plan(nats)?;
        let mut subscriptions = Vec::with_capacity(config.subscriptions.len());
        for subscription in &config.subscriptions {
            let Subscription::NatsSubject {
                subject,
                output_prefix,
                output_suffix,
            } = subscription
            else {
                anyhow::bail!(
                    "NatsInput only accepts `nats_subject` subscriptions, found a Redis subscription"
                );
            };
            subscriptions.push(NatsSubscription {
                subject: subject.clone(),
                output_prefix: output_prefix.clone(),
                output_suffix: output_suffix.clone(),
                stream: None,
            });
        }
        anyhow::ensure!(
            !subscriptions.is_empty(),
            "NatsInput requires at least one `nats_subject` subscription"
        );
        let source_address = plan.servers.first().cloned().unwrap_or_default();
        Ok(Self {
            plan,
            subscriptions,
            client: None,
            next_index: 0,
            source_address,
        })
    }

    /// Number of configured subscriptions.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(in crate::service) fn subscription_count(&self) -> usize {
        self.subscriptions.len()
    }

    /// Owned `(prefix, suffix)` mapping for a subscription identity, or `None`
    /// when no configured subscription matches it.
    pub(in crate::service) fn mapping_for(&self, identity: &str) -> Option<(String, String)> {
        self.subscriptions
            .iter()
            .find(|subscription| subscription.subject == identity)
            .map(|subscription| {
                (
                    subscription.output_prefix.clone(),
                    subscription.output_suffix.clone(),
                )
            })
    }

    /// Maps one envelope to the relay's `(prefix, source_channel, suffix)`
    /// fan-out inputs. Returns `None` when the identity is unknown, which the
    /// caller counts as a configuration error.
    pub(in crate::service) fn map_envelope<'a>(
        &self,
        envelope: &'a InputEnvelope,
    ) -> Option<(String, &'a str, String)> {
        let (prefix, suffix) = self.mapping_for(&envelope.subscription_identity)?;
        Some((prefix, envelope.source_subject.as_str(), suffix))
    }
}

impl InputSource for NatsInput {
    /// Connects (or reconnects) and (re)installs every configured subscription.
    /// Subscriptions are flushed so the server has processed them before the
    /// first message is awaited.
    async fn connect(&mut self) -> Result<()> {
        let client = connect(&self.plan).await?;
        let mut streams = Vec::with_capacity(self.subscriptions.len());
        for subscription in &self.subscriptions {
            let subscriber = client
                .subscribe(subscription.subject.clone())
                .await
                .with_context(|| {
                    format!(
                        "failed to subscribe to NATS subject {:?}",
                        subscription.subject
                    )
                })?;
            streams.push(subscriber);
        }
        client
            .flush()
            .await
            .context("failed to flush NATS subscriptions")?;
        let server = client.server_info();
        if !server.host.is_empty() && server.port != 0 {
            self.source_address =
                format!("{}://{}:{}", self.plan.transport, server.host, server.port);
        }
        for (subscription, stream) in self.subscriptions.iter_mut().zip(streams) {
            subscription.stream = Some(stream);
        }
        self.next_index = 0;
        self.client = Some(client);
        Ok(())
    }

    /// Yields the next envelope in arrival order, rotating fairly across
    /// subscriptions. Returns `None` when every subscription stream is closed,
    /// which tells the caller to reconnect with backoff.
    async fn next_envelope(&mut self) -> anyhow::Result<Option<InputEnvelope>> {
        if self.subscriptions.is_empty() {
            return Ok(None);
        }
        let count = self.subscriptions.len();
        let polled = poll_fn(|cx| {
            let mut closed = 0usize;
            for offset in 0..count {
                let index = (self.next_index + offset) % count;
                let stream = self.subscriptions[index]
                    .stream
                    .as_mut()
                    .expect("connected streams are installed for every subscription");
                match stream.poll_next_unpin(cx) {
                    Poll::Ready(Some(message)) => {
                        self.next_index = (index + 1) % count;
                        return Poll::Ready(Some((index, message)));
                    }
                    Poll::Ready(None) => closed += 1,
                    Poll::Pending => {}
                }
            }
            if closed == count {
                Poll::Ready(None)
            } else {
                Poll::Pending
            }
        })
        .await;
        let Some((index, message)) = polled else {
            return Ok(None);
        };
        Ok(Some(InputEnvelope {
            source_address: self.source_address.clone(),
            source_subject: message.subject.as_str().to_owned(),
            payload: Arc::from(message.payload.as_ref()),
            subscription_identity: self.subscriptions[index].subject.clone(),
        }))
    }
}

/// Reads a NATS input forever, fanning each envelope out to the outputs. Models
/// the Redis `read_input` loop: the adapter reconnects with the shared backoff
/// and per-message mapping/filtering stays backend-agnostic.
///
/// Loop handling: Core NATS `no_echo` only suppresses messages published through
/// the same client connection, and the relay uses separate input/output
/// connections, so output echoes can reach the input. Loop prevention for
/// NATS-to-NATS forwarding is a 0.3 step-4 workstream; the Redis echo filters
/// encode Redis channel namespaces and never match NATS subjects, so they are
/// intentionally not consulted here.
pub(in crate::service) async fn read_nats_input(
    config: InputConfig,
    mut senders: Vec<OutputSender>,
    echo_filters: Vec<OutputChannelFilter>,
    mut channel_filter: ChannelFilterSet,
    metrics: Arc<Metrics>,
) {
    if !echo_filters.is_empty() {
        warn!("nats_input_ignores_redis_echo_filters");
    }
    let mut retry_delay = Duration::from_millis(250);
    let mut input = match NatsInput::new(&config) {
        Ok(input) => input,
        Err(error) => {
            metrics.record_error(&error);
            warn!(error = %error, "nats_input_invalid_configuration");
            return;
        }
    };
    loop {
        match input.connect().await {
            Ok(()) => {
                retry_delay = Duration::from_millis(250);
                info!("input_connected");
                loop {
                    let received_at = time::Instant::now();
                    let envelope = match input.next_envelope().await {
                        Ok(Some(envelope)) => envelope,
                        Ok(None) => break,
                        Err(error) => {
                            metrics.record_error(&error);
                            warn!(error = %error, "nats_input_stream_failed");
                            break;
                        }
                    };
                    let source_subject = envelope.source_subject.clone();
                    if !channel_filter.allows(&source_subject) {
                        metrics
                            .excluded_messages_total
                            .fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    let Some((prefix, _, suffix)) = input.map_envelope(&envelope) else {
                        metrics.record_error(
                            "NATS input message did not match a configured subscription",
                        );
                        warn!(
                            subject = %source_subject,
                            identity = %envelope.subscription_identity,
                            "input_message_subscription_not_found"
                        );
                        continue;
                    };
                    metrics.record_input(envelope.payload.len(), received_at.elapsed());
                    fan_out(
                        &mut senders,
                        &prefix,
                        &source_subject,
                        &suffix,
                        &envelope.payload,
                        &metrics,
                    );
                }
                metrics
                    .input_reconnects_total
                    .fetch_add(1, Ordering::Relaxed);
                metrics.record_error("NATS input subscription stream closed");
                warn!("input_connection_closed");
            }
            Err(error) => {
                metrics
                    .input_reconnects_total
                    .fetch_add(1, Ordering::Relaxed);
                metrics.record_error(&error);
                warn!(error = %error, "input_connection_failed");
            }
        }
        time::sleep(retry_delay).await;
        retry_delay = (retry_delay * 2).min(MAX_RETRY_DELAY);
    }
}
