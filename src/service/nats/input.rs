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

use anyhow::{Context, Result};
use async_nats::{Client, Subscriber};
use futures_util::{StreamExt, future::poll_fn};
use tokio::time;
use tracing::{debug, info, warn};

use crate::{
    config::{InputConfig, Subscription},
    status::Metrics,
};

use super::super::OutputSender;
use super::super::backend::{InputEnvelope, InputSource, NATS_HOP_HEADER};
use super::super::filter::ChannelFilterSet;
use super::super::input::{OutputChannelFilter, fan_out};
use super::super::{INITIAL_RETRY_DELAY, next_retry_delay};
use super::connect::{ConnectionPlan, connect_with_metrics, connection_plan};

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
    /// Optional sink for NATS client connection events. `None` in unit tests
    /// that construct the adapter without a metrics registry.
    metrics: Option<std::sync::Arc<Metrics>>,
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
            metrics: None,
        })
    }

    /// Attaches a metrics registry so NATS connect/disconnect events are counted.
    pub(in crate::service) fn with_metrics(mut self, metrics: std::sync::Arc<Metrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Number of configured subscriptions.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(in crate::service) fn subscription_count(&self) -> usize {
        self.subscriptions.len()
    }

    /// `true` once a client and all of its subscription streams are installed.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(in crate::service) fn is_connected(&self) -> bool {
        self.client.is_some()
            && self
                .subscriptions
                .iter()
                .all(|subscription| subscription.stream.is_some())
    }

    /// Clears the client and every installed stream. The reader's reconnect path
    /// uses this indirectly through `connect`; exposed for tests that assert the
    /// disconnected state is fully reset.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(in crate::service) fn force_disconnect(&mut self) {
        self.client = None;
        for subscription in &mut self.subscriptions {
            subscription.stream = None;
        }
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
    ///
    /// The relay builds `ConnectOptions` with the default `retry_on_initial_connect`
    /// disabled, so `async-nats` makes a single pass over the seed pool here.
    /// Retrying the initial connect (backoff, forever) is the reader's outer loop,
    /// exactly as it is for the Redis reader. On any failure the previous client
    /// and streams are dropped so the next attempt is clean.
    async fn connect(&mut self) -> Result<()> {
        match self.establish().await {
            Ok((client, streams, source_address)) => {
                for (subscription, stream) in self.subscriptions.iter_mut().zip(streams) {
                    subscription.stream = Some(stream);
                }
                self.source_address = source_address;
                self.next_index = 0;
                self.client = Some(client);
                Ok(())
            }
            Err(error) => {
                // Leave no half-installed state behind: a later reconnect must
                // start from a clean client and a fresh subscription set.
                self.client = None;
                for subscription in &mut self.subscriptions {
                    subscription.stream = None;
                }
                Err(error)
            }
        }
    }

    /// Yields the next envelope in arrival order, rotating fairly across
    /// subscriptions. Returns `None` when every subscription stream is closed,
    /// which tells the caller to reconnect with backoff.
    ///
    /// If the adapter is not connected (or a stream is missing because a
    /// previous connect failed part-way), this returns `None` instead of
    /// panicking so the reader re-runs `connect`.
    async fn next_envelope(&mut self) -> anyhow::Result<Option<InputEnvelope>> {
        if self.subscriptions.is_empty()
            || self.client.is_none()
            || self
                .subscriptions
                .iter()
                .any(|subscription| subscription.stream.is_none())
        {
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
        // Core NATS delivers the message to this subscription even when this relay
        // published it on another connection, so a loop is broken here by dropping
        // anything already stamped with the hop header.
        let already_forwarded = message
            .headers
            .as_ref()
            .is_some_and(|headers| headers.get(NATS_HOP_HEADER).is_some());
        Ok(Some(InputEnvelope {
            source_address: self.source_address.clone(),
            source_subject: message.subject.as_str().to_owned(),
            payload: Arc::from(message.payload.as_ref()),
            subscription_identity: self.subscriptions[index].subject.clone(),
            already_forwarded,
        }))
    }
}

impl NatsInput {
    /// Opens a fresh client and installs every configured subscription on it.
    /// Nothing is committed to `self` here, so a partial failure drops the new
    /// client and its subscriptions without disturbing the current state.
    async fn establish(&self) -> Result<(Client, Vec<Subscriber>, String)> {
        let client = connect_with_metrics(&self.plan, self.metrics.clone()).await?;
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
        let source_address = if !server.host.is_empty() && server.port != 0 {
            format!("{}://{}:{}", self.plan.transport, server.host, server.port)
        } else {
            self.source_address.clone()
        };
        Ok((client, streams, source_address))
    }
}

/// Reads a NATS input forever, fanning each envelope out to the outputs. Models
/// the Redis `read_input` loop: the adapter reconnects with the shared backoff
/// and per-message mapping/filtering stays backend-agnostic.
///
/// Loop handling: Core NATS `no_echo` only suppresses messages published through
/// the same client connection, and the relay uses separate input/output
/// connections, so output echoes can reach the input. Each NATS output stamps
/// the [`NATS_HOP_HEADER`] on the messages it forwards; this reader drops any
/// message that already carries it, which breaks NATS-to-NATS loops without
/// touching the payload. The Redis echo filters encode Redis channel namespaces
/// and never match NATS subjects, so they are intentionally not consulted here.
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
    let mut retry_delay = INITIAL_RETRY_DELAY;
    let mut input = match NatsInput::new(&config) {
        Ok(input) => input.with_metrics(Arc::clone(&metrics)),
        Err(error) => {
            metrics.record_error(&error);
            warn!(error = %error, "nats_input_invalid_configuration");
            return;
        }
    };
    loop {
        match input.connect().await {
            Ok(()) => {
                retry_delay = INITIAL_RETRY_DELAY;
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
                    if envelope.already_forwarded {
                        // Loop re-entry: this relay already published this message,
                        // so forwarding it again would cycle forever.
                        metrics
                            .loop_suppressed_messages_total
                            .fetch_add(1, Ordering::Relaxed);
                        debug!(subject = %source_subject, "nats_input_loop_suppressed");
                        continue;
                    }
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
        retry_delay = next_retry_delay(retry_delay);
    }
}
