use std::{sync::Arc, sync::atomic::Ordering, time::Duration};

use anyhow::{Context, Result};
use futures_util::StreamExt;
use tokio::time;
use tracing::{info, warn};

use crate::{
    config::{InputConfig, OutputConfig, RedisConfig, Subscription, same_pubsub_server},
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

                    let pattern = message.get_pattern::<Option<String>>().unwrap_or(None);
                    let Some(subscription) = matching_subscription(
                        &config.subscriptions,
                        source_channel,
                        pattern.as_deref(),
                    ) else {
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

    for output in senders.iter_mut() {
        if let Some(channel) = input_mapped_channel.as_deref()
            && !output.channel_filter.allows(channel)
        {
            continue;
        }
        let output_channel = map_output_channel(
            &output.channel_prefix,
            subscription_prefix,
            source_channel,
            subscription_suffix,
            &output.channel_suffix,
        );
        let payload_bytes = payload.len();
        metrics.record_output_input(&output.output_metrics, payload_bytes);
        if output
            .sender
            .send(QueuedInboundMessage {
                message: InboundMessage {
                    output_channel,
                    payload: Arc::clone(&payload),
                },
                enqueued_at: time::Instant::now(),
            })
            .is_err()
        {
            metrics.rollback_output_input(&output.output_metrics, payload_bytes);
            metrics.record_error(format!("output worker {} is unavailable", output.name));
            warn!(output = %output.name, "output_worker_unavailable");
        }
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
    format!(
        "{output_prefix}{subscription_prefix}{source_channel}{subscription_suffix}{output_suffix}"
    )
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
