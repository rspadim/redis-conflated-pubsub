use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::Duration,
};

use redis::aio::MultiplexedConnection;
use tokio::{
    sync::mpsc,
    time::{self, MissedTickBehavior},
};
use tracing::error;

use crate::status::{self, Metrics, OutputMetrics};

use super::batch::OversizedPolicyLog;
use super::policy::{
    CompiledChannelPolicies, advance_due_schedules, flush_schedules, next_schedule_tick,
};
use super::publish::{OutputFailureLog, RedisBatchPublisher};
use super::{
    DeduplicationCache, InboundMessage, OutputMessageContext, OutputMessagePolicy,
    OutputPublishContext, OutputRuntimeSetup, PendingMessage, deduplication_prune_interval,
};

mod flush;

#[cfg(test)]
pub(super) use flush::clear_published_batch;
#[cfg(test)]
pub(super) use flush::publish_conflated_pending;
pub(super) use flush::{
    enqueue_message, publish_conflated_interval_pending, publish_passthrough_pending,
};

pub(super) async fn publish_output(
    setup: OutputRuntimeSetup,
    client: redis::Client,
    mut receiver: mpsc::UnboundedReceiver<InboundMessage>,
    metrics: Arc<Metrics>,
    output_metrics: Arc<OutputMetrics>,
) {
    let OutputRuntimeSetup {
        name,
        config,
        max_bytes_per_exec,
        oversized_policy,
        filters_endpoint_enabled,
        channel_policy_cache_max_entries,
    } = setup;
    let max_commands_per_exec = config.conflation.max_commands_per_exec;
    let compiled_channel_policies = CompiledChannelPolicies::new_with_cache(
        &config,
        channel_policy_cache_max_entries,
        filters_endpoint_enabled.then(|| (&*metrics, format!("outputs.{name}.channel_policies"))),
    );
    let mut deduplication_cache =
        DeduplicationCache::with_groups(config.deduplication.ttl_ms, &config.deduplication_groups);
    let mut deduplication_prune_interval = deduplication_prune_interval(&config).map(|interval| {
        let mut ticker = time::interval_at(time::Instant::now() + interval, interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        ticker
    });
    let mut flush_schedules = flush_schedules(
        std::iter::once(config.conflation.interval_ms)
            .chain(
                config
                    .profiles
                    .values()
                    .filter_map(|profile| profile.conflation_interval_ms),
            )
            .chain(
                config
                    .channel_policies
                    .iter()
                    .filter_map(|policy| policy.conflation_interval_ms),
            ),
        time::Instant::now(),
    );
    let mut pending = HashMap::<String, PendingMessage>::new();
    let mut passthrough = VecDeque::<PendingMessage>::new();
    let mut failure_log = OutputFailureLog::default();
    let mut policy_log = OversizedPolicyLog::default();
    let mut connection: Option<MultiplexedConnection> = None;
    let mut input_closed = false;
    let mut flush_due = false;
    let mut due_intervals = Vec::<i64>::new();

    metrics.set_output_state(&output_metrics, "running");
    loop {
        let has_pending = !pending.is_empty() || !passthrough.is_empty();
        if flush_due {
            flush_due = false;
            if !passthrough.is_empty() {
                metrics.set_output_state(&output_metrics, "publishing");
                if connection.is_none() {
                    metrics.set_output_state(&output_metrics, "connecting");
                }
                let mut publisher = RedisBatchPublisher {
                    config: &config,
                    client: &client,
                    connection: &mut connection,
                };
                let mut publish_context = OutputPublishContext {
                    name: &name,
                    failure_log: &mut failure_log,
                    deduplication_cache: &mut deduplication_cache,
                    metrics: &metrics,
                    output_metrics: &output_metrics,
                };
                publish_passthrough_pending(
                    &mut publisher,
                    &mut pending,
                    &mut passthrough,
                    &mut publish_context,
                )
                .await;
                metrics.set_output_state(&output_metrics, "running");
                continue;
            }
        }

        if !due_intervals.is_empty() {
            let intervals = std::mem::take(&mut due_intervals);
            for interval_ms in intervals {
                if !pending
                    .values()
                    .any(|message| message.conflation_interval_ms == interval_ms)
                {
                    continue;
                }
                metrics.set_output_state(&output_metrics, "publishing");
                if connection.is_none() {
                    metrics.set_output_state(&output_metrics, "connecting");
                }
                let mut publisher = RedisBatchPublisher {
                    config: &config,
                    client: &client,
                    connection: &mut connection,
                };
                let mut publish_context = OutputPublishContext {
                    name: &name,
                    failure_log: &mut failure_log,
                    deduplication_cache: &mut deduplication_cache,
                    metrics: &metrics,
                    output_metrics: &output_metrics,
                };
                publish_conflated_interval_pending(
                    &mut publisher,
                    &mut pending,
                    &mut passthrough,
                    Some(interval_ms),
                    max_commands_per_exec,
                    max_bytes_per_exec,
                    &mut publish_context,
                )
                .await;
                metrics.set_output_state(&output_metrics, "running");
            }
            continue;
        }

        if input_closed && !has_pending {
            break;
        }

        let next_flush_tick = next_schedule_tick(&flush_schedules);
        tokio::select! {
            message = receiver.recv(), if !input_closed => {
                match message {
                    Some(message) => {
                        let channel_policy = compiled_channel_policies
                            .resolve(&config, &message.output_channel);
                        let mut message_context = OutputMessageContext {
                            name: &name,
                            policy: OutputMessagePolicy {
                                interval_ms: channel_policy.interval_ms,
                                deduplication_ttl_ms: Some(channel_policy.deduplication_ttl_ms),
                                deduplication_group: channel_policy.deduplication_group,
                                max_bytes_per_exec,
                                oversized_policy,
                            },
                            metrics: &metrics,
                            output_metrics: &output_metrics,
                            policy_log: &mut policy_log,
                        };
                        enqueue_message(
                            message,
                            &mut pending,
                            &mut passthrough,
                            &mut message_context,
                            &mut deduplication_cache,
                            time::Instant::now(),
                        );
                        if channel_policy.interval_ms <= 0 {
                            flush_due = true;
                        }
                    }
                    None => {
                        input_closed = true;
                        flush_due = true;
                        for message in pending.values() {
                            let interval_ms = message.conflation_interval_ms;
                            if interval_ms > 0 && !due_intervals.contains(&interval_ms) {
                                due_intervals.push(interval_ms);
                            }
                        }
                    }
                }
            }
            _ = async {
                if let Some(next_tick) = next_flush_tick {
                    time::sleep_until(next_tick).await;
                } else {
                    std::future::pending::<()>().await;
                }
            }, if !flush_schedules.is_empty() => {
                for interval_ms in advance_due_schedules(&mut flush_schedules, time::Instant::now()) {
                    if pending
                        .values()
                        .any(|message| message.conflation_interval_ms == interval_ms)
                    {
                        due_intervals.push(interval_ms);
                    }
                }
            }
            _ = async {
                if let Some(interval) = deduplication_prune_interval.as_mut() {
                    interval.tick().await;
                } else {
                    std::future::pending::<()>().await;
                }
            }, if deduplication_prune_interval.is_some() => {
                deduplication_cache.prune_expired(time::Instant::now());
            }
        }
    }
    failure_log.flush_suppressed(&name);
    policy_log.flush_suppressed(&name);
    metrics.set_output_state(&output_metrics, "stopped");
}

pub(super) async fn write_status(
    path: std::path::PathBuf,
    update_interval_ms: u64,
    metrics: Arc<Metrics>,
) {
    let mut interval = time::interval(Duration::from_millis(update_interval_ms));
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        interval.tick().await;
        if let Err(error) = status::write_atomic(&path, &metrics.snapshot()) {
            error!(error = %error, "status_write_failed");
            metrics.record_error(&error);
        }
    }
}
