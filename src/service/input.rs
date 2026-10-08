use std::{sync::Arc, sync::atomic::Ordering, time::Duration};

use anyhow::{Context, Result};
use futures_util::StreamExt;
use tokio::time;
use tracing::{info, warn};

use crate::{
    config::{
        InputConfig, OutputConfig, QueueOverflowPolicy, RedisConfig, Subscription,
        same_pubsub_server,
    },
    status::Metrics,
};

use super::{
    ChannelFilterSet, InboundMessage, MAX_RETRY_DELAY, OutputSender, QueuedInboundMessage,
};

#[derive(Clone, Debug)]
pub(super) struct OutputChannelFilter {
    pub(super) prefix: String,
    pub(super) suffix: String,
}

impl OutputChannelFilter {
    pub(super) fn matches(&self, channel: &str) -> bool {
        (!self.prefix.is_empty() && channel.starts_with(&self.prefix))
            || (!self.suffix.is_empty() && channel.ends_with(&self.suffix))
    }
}

pub(super) fn output_echo_filters(
    input: &InputConfig,
    outputs: &std::collections::BTreeMap<String, OutputConfig>,
) -> Vec<OutputChannelFilter> {
    outputs
        .values()
        .filter(|output| same_pubsub_server(&input.redis, &output.redis))
        .flat_map(|output| {
            input.subscriptions.iter().map(|subscription| {
                let (subscription_prefix, subscription_suffix) = subscription.output_mapping();
                OutputChannelFilter {
                    prefix: format!("{}{}", output.channel_prefix, subscription_prefix),
                    suffix: format!("{}{}", subscription_suffix, output.channel_suffix),
                }
            })
        })
        .collect()
}

const SENTINEL_PUBSUB_CHANNELS: &[&str] = &[
    "__sentinel__:hello",
    "+reset-master",
    "+slave",
    "+replica",
    "+sdown",
    "-sdown",
    "+odown",
    "-odown",
    "+new-epoch",
    "+try-failover",
    "+vote-for-leader",
    "+elected-leader",
    "+failover-state-select-slave",
    "+selected-slave",
    "+promoted-slave",
    "+failover-state-send-slaveof-noone",
    "+failover-state-wait-promotion",
    "+failover-state-reconf-slaves",
    "+slave-reconf-sent",
    "+slave-reconf-inprog",
    "+slave-reconf-done",
    "+failover-end-for-timeout",
    "+failover-end",
    "+switch-master",
    "+tilt",
    "-tilt",
    "+sentinel",
    "+dup-sentinel",
    "-dup-sentinel",
    "+monitor",
    "+reboot",
    "+config-update",
    "+sentinel-address-switch",
];

pub(super) fn is_sentinel_pubsub_channel(channel: &str) -> bool {
    SENTINEL_PUBSUB_CHANNELS.contains(&channel)
}

pub(super) async fn read_input(
    config: InputConfig,
    client: redis::Client,
    mut senders: Vec<OutputSender>,
    echo_filters: Vec<OutputChannelFilter>,
    mut channel_filter: ChannelFilterSet,
    metrics: Arc<Metrics>,
) {
    let mut retry_delay = Duration::from_millis(250);
    // With a single subscription the connection only delivers messages for it,
    // so the pattern reported by Redis can be ignored without allocating.
    let single_subscription = match config.subscriptions.as_slice() {
        [subscription] => Some(subscription),
        _ => None,
    };
    loop {
        match connect_input(&client, &config.redis, &config.subscriptions).await {
            Ok(mut pubsub) => {
                retry_delay = Duration::from_millis(250);
                info!("input_connected");
                let mut stream = pubsub.on_message();
                while let Some(message) = stream.next().await {
                    let received_at = time::Instant::now();
                    let source_channel = message.get_channel_name();
                    if (config.exclude_sentinel_pubsub
                        && is_sentinel_pubsub_channel(source_channel))
                        || echo_filters
                            .iter()
                            .any(|filter| filter.matches(source_channel))
                    {
                        metrics
                            .excluded_messages_total
                            .fetch_add(1, Ordering::Relaxed);
                        continue;
                    }

                    let subscription = match single_subscription {
                        Some(subscription) => single_subscription_match(
                            subscription,
                            source_channel,
                            message.from_pattern(),
                        ),
                        None => {
                            let pattern = message.get_pattern::<Option<String>>().unwrap_or(None);
                            matching_subscription(
                                &config.subscriptions,
                                source_channel,
                                pattern.as_deref(),
                            )
                        }
                    };
                    let Some(subscription) = subscription else {
                        metrics
                            .record_error("input message did not match a configured subscription");
                        warn!("input_message_subscription_not_found");
                        continue;
                    };
                    if !channel_filter.allows(source_channel) {
                        metrics
                            .excluded_messages_total
                            .fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    let (prefix, suffix) = subscription.output_mapping();
                    metrics.record_input(message.get_payload_bytes().len(), received_at.elapsed());
                    fan_out(
                        &mut senders,
                        prefix,
                        source_channel,
                        suffix,
                        message.get_payload_bytes(),
                        &metrics,
                    );
                }
                metrics
                    .input_reconnects_total
                    .fetch_add(1, Ordering::Relaxed);
                metrics.record_error("input Pub/Sub connection closed");
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

pub(super) fn fan_out(
    senders: &mut [OutputSender],
    subscription_prefix: &str,
    source_channel: &str,
    subscription_suffix: &str,
    payload: &[u8],
    metrics: &Metrics,
) {
    // One shared allocation per inbound message; every output clones the Arc.
    let payload: Arc<[u8]> = Arc::from(payload);
    let input_mapped_channel = senders
        .iter()
        .any(|output| output.channel_filter.can_deny())
        .then(|| {
            map_output_channel(
                "",
                subscription_prefix,
                source_channel,
                subscription_suffix,
                "",
            )
        });
    // One timestamp per inbound message instead of one per output.
    let enqueued_at = time::Instant::now();

    for output in senders.iter_mut() {
        if let Some(channel) = input_mapped_channel.as_deref()
            && !output.channel_filter.allows(channel)
        {
            continue;
        }
        let output_channel =
            output.mapped_channel(subscription_prefix, source_channel, subscription_suffix);
        let payload_bytes = payload.len();
        let queue_bytes = output_channel.len() + payload_bytes;
        // drop_newest checks the logical pending gauges before admission, so the
        // message is discarded without touching the queue or pending counters.
        if output.queue_policy == QueueOverflowPolicy::DropNewest
            && output.queue_limits.rejects_admission(
                output.output_metrics.pending_messages(),
                output.output_metrics.pending_queue_bytes(),
                queue_bytes as u64,
            )
        {
            metrics.record_output_shed(&output.output_metrics, 1, payload_bytes);
            output.queue_shed_log.report(&output.name, 1, payload_bytes);
            continue;
        }
        metrics.record_output_input(&output.output_metrics, payload_bytes, queue_bytes);
        match output.sender.send(
            QueuedInboundMessage {
                message: InboundMessage {
                    output_channel,
                    payload: Arc::clone(&payload),
                },
                enqueued_at,
            },
            output.queue_limits,
        ) {
            Ok(evicted) => {
                // Only drop_oldest returns evicted messages; release their
                // pending gauges and count them as shed.
                for evicted in evicted {
                    let evicted_payload_bytes = evicted.message.payload.len();
                    let evicted_queue_bytes =
                        evicted.message.output_channel.len() + evicted_payload_bytes;
                    metrics.record_output_shed_pending(
                        &output.output_metrics,
                        1,
                        evicted_payload_bytes,
                        evicted_queue_bytes,
                    );
                    output
                        .queue_shed_log
                        .report(&output.name, 1, evicted_payload_bytes);
                }
            }
            Err(()) => {
                metrics.rollback_output_input(&output.output_metrics, payload_bytes, queue_bytes);
                metrics.record_error(format!("output worker {} is unavailable", output.name));
                warn!(output = %output.name, "output_worker_unavailable");
            }
        }
    }
}

impl OutputSender {
    /// Builds the mapped channel without a per-message `format!`, reusing the
    /// precomputed single-subscription prefix/suffix when available.
    fn mapped_channel(
        &self,
        subscription_prefix: &str,
        source_channel: &str,
        subscription_suffix: &str,
    ) -> String {
        if let Some((prefix, suffix)) = &self.combined_channel_mapping {
            let mut channel =
                String::with_capacity(prefix.len() + source_channel.len() + suffix.len());
            channel.push_str(prefix);
            channel.push_str(source_channel);
            channel.push_str(suffix);
            channel
        } else {
            map_output_channel(
                &self.channel_prefix,
                subscription_prefix,
                source_channel,
                subscription_suffix,
                &self.channel_suffix,
            )
        }
    }
}

/// Fast path for a connection with exactly one subscription: Redis only
/// delivers messages for it, so the pattern reported by Redis is redundant and
/// `get_pattern` (which allocates) can be skipped.
pub(super) fn single_subscription_match<'a>(
    subscription: &'a Subscription,
    channel: &str,
    from_pattern: bool,
) -> Option<&'a Subscription> {
    match subscription {
        Subscription::Subscribe {
            channel: configured,
            ..
        } => (!from_pattern && configured == channel).then_some(subscription),
        Subscription::Psubscribe { .. } => from_pattern.then_some(subscription),
    }
}

pub(super) fn matching_subscription<'a>(
    subscriptions: &'a [Subscription],
    channel: &str,
    pattern: Option<&str>,
) -> Option<&'a Subscription> {
    match pattern {
        Some(pattern) => subscriptions.iter().find(|subscription| {
            matches!(subscription, Subscription::Psubscribe { pattern: configured, .. } if configured == pattern)
        }),
        None => subscriptions.iter().find(|subscription| {
            matches!(subscription, Subscription::Subscribe { channel: configured, .. } if configured == channel)
        }),
    }
}

pub(super) fn map_output_channel(
    output_prefix: &str,
    subscription_prefix: &str,
    source_channel: &str,
    subscription_suffix: &str,
    output_suffix: &str,
) -> String {
    let mut channel = String::with_capacity(
        output_prefix.len()
            + subscription_prefix.len()
            + source_channel.len()
            + subscription_suffix.len()
            + output_suffix.len(),
    );
    channel.push_str(output_prefix);
    channel.push_str(subscription_prefix);
    channel.push_str(source_channel);
    channel.push_str(subscription_suffix);
    channel.push_str(output_suffix);
    channel
}

async fn connect_input(
    client: &redis::Client,
    redis_config: &RedisConfig,
    subscriptions: &[Subscription],
) -> Result<redis::aio::PubSub> {
    let mut pubsub = time::timeout(redis_config.connect_timeout(), client.get_async_pubsub())
        .await
        .context("timed out connecting to input Redis")??;

    for subscription in subscriptions {
        match subscription {
            Subscription::Subscribe { channel, .. } => {
                time::timeout(
                    redis_config.connect_timeout(),
                    pubsub.subscribe(channel.as_str()),
                )
                .await
                .context("timed out subscribing to input channel")??;
            }
            Subscription::Psubscribe { pattern, .. } => {
                time::timeout(
                    redis_config.connect_timeout(),
                    pubsub.psubscribe(pattern.as_str()),
                )
                .await
                .context("timed out subscribing to input pattern")??;
            }
        }
    }
    Ok(pubsub)
}
