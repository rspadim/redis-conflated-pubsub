use std::{
    collections::{HashMap, VecDeque},
    future::poll_fn,
    sync::Arc,
    task::Poll,
    time::Duration,
};

use futures_util::{StreamExt, stream::FuturesOrdered};
use redis::aio::MultiplexedConnection;
use tokio::time::{self, MissedTickBehavior};
use tracing::{debug, error};

use crate::status::{self, Metrics, OutputMetrics};

use super::batch::{OversizedPolicyLog, passthrough_batch_length};
use super::policy::{
    CompiledChannelPolicies, advance_due_schedules, flush_schedules, next_schedule_tick,
};
use super::publish::{
    OutputFailureLog, PublishFailure, RedisBatchPublisher, ensure_output_connection,
    publish_batch_on_connection, settle_publish_result,
};
use super::queue::{QueueShedLog, TryRecv};
use super::{
    DeduplicationCache, InboundMessage, OutputMessageContext, OutputMessagePolicy,
    OutputPublishContext, OutputQueueReceiver, OutputRuntimeSetup, PendingByInterval,
    PendingMessage, QueuedInboundMessage, ResolvedChannelPolicy, deduplication_prune_interval,
    pending_message_count, pending_message_queue_bytes,
};

mod flush;

type DirectPublishResult = (
    Vec<PendingMessage>,
    std::result::Result<i64, PublishFailure>,
    Duration,
);
type DirectPublishFuture<'a> = futures_util::future::BoxFuture<'a, DirectPublishResult>;

pub(super) use flush::clear_published_batch;
#[cfg(test)]
pub(super) use flush::publish_conflated_pending;
#[cfg(test)]
pub(super) use flush::publish_passthrough_batch;
#[cfg(test)]
pub(super) use flush::publish_passthrough_pending;
pub(super) use flush::{enqueue_message, publish_conflated_interval_pending};

struct OutputEnqueueContext<'a> {
    name: &'a str,
    config: &'a crate::config::OutputConfig,
    channel_policies: &'a CompiledChannelPolicies,
    max_bytes_per_exec: usize,
    oversized_policy: crate::config::OversizedMessagePolicy,
    pending: &'a mut PendingByInterval,
    passthrough: &'a mut VecDeque<PendingMessage>,
    deduplication_cache: &'a mut DeduplicationCache,
    metrics: &'a Metrics,
    output_metrics: &'a OutputMetrics,
    policy_log: &'a mut OversizedPolicyLog,
}

impl OutputEnqueueContext<'_> {
    fn resolve(&self, message: &InboundMessage) -> ResolvedChannelPolicy {
        self.channel_policies
            .resolve(self.config, &message.output_channel)
    }

    fn enqueue(&mut self, message: InboundMessage, policy: ResolvedChannelPolicy) -> usize {
        let mut message_context = OutputMessageContext {
            name: self.name,
            policy: OutputMessagePolicy {
                interval_ms: policy.interval_ms,
                deduplication_ttl_ms: Some(policy.deduplication_ttl_ms),
                deduplication_group: policy.deduplication_group,
                max_bytes_per_exec: self.max_bytes_per_exec,
                oversized_policy: self.oversized_policy,
            },
            metrics: self.metrics,
            output_metrics: self.output_metrics,
            policy_log: self.policy_log,
        };
        enqueue_message(
            message,
            self.pending,
            self.passthrough,
            &mut message_context,
            self.deduplication_cache,
            time::Instant::now(),
        )
    }
}

pub(super) fn direct_batch_boundary_required(
    passthrough: &VecDeque<PendingMessage>,
    message: &InboundMessage,
    policy: &ResolvedChannelPolicy,
) -> bool {
    if policy.interval_ms > 0 || passthrough.is_empty() {
        return false;
    }
    let group = policy.deduplication_group.as_deref();
    if policy.deduplication_ttl_ms <= 0 && group.is_none() {
        return false;
    }
    passthrough.iter().any(|queued| {
        queued.output_channel == message.output_channel
            || group.is_some_and(|group| queued.deduplication_group.as_deref() == Some(group))
    })
}

/// Returns true when the intake message exceeded `queue_max_age_ms` and was
/// shed. Extracted so the drop_by_age decision and its metrics are testable
/// without a Redis connection.
#[allow(clippy::too_many_arguments)]
pub(super) fn shed_expired_message(
    name: &str,
    queued_message: &QueuedInboundMessage,
    queue_max_age_ms: Option<u64>,
    now: time::Instant,
    metrics: &Metrics,
    output_metrics: &OutputMetrics,
    shed_log: &mut QueueShedLog,
) -> bool {
    let Some(max_age_ms) = queue_max_age_ms else {
        return false;
    };
    if now
        .saturating_duration_since(queued_message.enqueued_at)
        .as_millis()
        <= u128::from(max_age_ms)
    {
        return false;
    }
    let payload_bytes = queued_message.message.payload.len();
    let queue_bytes = queued_message.message.output_channel.len() + payload_bytes;
    metrics.record_output_shed_pending(output_metrics, 1, payload_bytes, queue_bytes);
    shed_log.report(name, 1, payload_bytes);
    true
}

pub(super) async fn publish_output(
    setup: OutputRuntimeSetup,
    client: redis::Client,
    mut receiver: OutputQueueReceiver,
    metrics: Arc<Metrics>,
    output_metrics: Arc<OutputMetrics>,
) {
    let OutputRuntimeSetup {
        name,
        config,
        max_bytes_per_exec,
        max_in_flight_commands,
        max_in_flight_bytes,
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
    let mut pending = PendingByInterval::new();
    let mut passthrough = VecDeque::<PendingMessage>::new();
    let mut failure_log = OutputFailureLog::default();
    let mut policy_log = OversizedPolicyLog::default();
    let mut shed_log = QueueShedLog::default();
    let mut connection: Option<MultiplexedConnection> = None;
    let mut input_closed = false;
    let mut due_intervals = Vec::<i64>::new();
    let mut deferred_input = None;
    // Keep in-flight messages at the front of passthrough until their replies
    // settle, preserving FIFO accounting while later commands are in flight.
    let mut in_flight_direct = FuturesOrdered::<DirectPublishFuture<'_>>::new();
    let mut in_flight_batches = VecDeque::<(usize, usize)>::new();
    let mut in_flight_direct_messages = 0usize;
    let mut in_flight_direct_bytes = 0usize;

    metrics.set_output_state(&output_metrics, "running");
    loop {
        // Intake measures queued payload bytes, while the in-flight window
        // measures encoded request bytes; the two quotas are intentionally asymmetric.
        let input_command_capacity =
            max_in_flight_commands.saturating_sub(in_flight_direct_messages);
        let input_byte_capacity = max_in_flight_bytes.saturating_sub(in_flight_direct_bytes);
        if due_intervals.is_empty()
            && passthrough.len() == in_flight_direct_messages
            && input_command_capacity > 0
            && (input_byte_capacity > 0 || in_flight_direct_messages == 0)
            && (!input_closed || deferred_input.is_some())
        {
            let mut drained_messages = 0usize;
            let mut drained_payload_bytes = 0usize;
            let mut pending_keys_after_intake = None;
            let mut queue_wait_samples = 0u64;
            let mut queue_wait_total_ns = 0u64;
            let mut queue_wait_max_ns = 0u64;
            let mut enqueue_context = OutputEnqueueContext {
                name: &name,
                config: &config,
                channel_policies: &compiled_channel_policies,
                max_bytes_per_exec,
                oversized_policy,
                pending: &mut pending,
                passthrough: &mut passthrough,
                deduplication_cache: &mut deduplication_cache,
                metrics: &metrics,
                output_metrics: &output_metrics,
                policy_log: &mut policy_log,
            };
            loop {
                if drained_messages >= input_command_capacity
                    || (drained_messages > 0 && drained_payload_bytes >= input_byte_capacity)
                {
                    break;
                }
                let queued_message = if let Some(message) = deferred_input.take() {
                    message
                } else {
                    match receiver.try_recv() {
                        TryRecv::Message(message) => message,
                        TryRecv::Empty => break,
                        TryRecv::Disconnected => {
                            input_closed = true;
                            for (&interval_ms, messages) in enqueue_context.pending.iter() {
                                if !messages.is_empty() && !due_intervals.contains(&interval_ms) {
                                    due_intervals.push(interval_ms);
                                }
                            }
                            break;
                        }
                    }
                };
                // drop_by_age sheds at intake; the message was already admitted
                // by the fan-out, so its pending gauges are released here.
                if shed_expired_message(
                    &name,
                    &queued_message,
                    config.queue_max_age_ms,
                    time::Instant::now(),
                    &metrics,
                    &output_metrics,
                    &mut shed_log,
                ) {
                    continue;
                }
                let policy = enqueue_context.resolve(&queued_message.message);
                if direct_batch_boundary_required(
                    enqueue_context.passthrough,
                    &queued_message.message,
                    &policy,
                ) {
                    deferred_input = Some(queued_message);
                    break;
                }
                let queue_wait =
                    time::Instant::now().saturating_duration_since(queued_message.enqueued_at);
                let queue_wait_ns = queue_wait.as_nanos().min(u64::MAX as u128) as u64;
                queue_wait_samples += 1;
                queue_wait_total_ns = queue_wait_total_ns.saturating_add(queue_wait_ns);
                queue_wait_max_ns = queue_wait_max_ns.max(queue_wait_ns);
                drained_payload_bytes =
                    drained_payload_bytes.saturating_add(queued_message.message.payload.len());
                drained_messages += 1;
                pending_keys_after_intake =
                    Some(enqueue_context.enqueue(queued_message.message, policy));
            }
            // One coalesced metrics update per intake pass instead of per message.
            if queue_wait_samples > 0 {
                metrics.record_output_queue_wait_batch(
                    &output_metrics,
                    queue_wait_samples,
                    queue_wait_total_ns,
                    queue_wait_max_ns,
                );
            }
            if let Some(pending_keys) = pending_keys_after_intake {
                metrics.publish_output_pending_keys(&output_metrics, pending_keys);
            }
        }

        for interval_ms in advance_due_schedules(&mut flush_schedules, time::Instant::now()) {
            if pending
                .get(&interval_ms)
                .is_some_and(|messages| !messages.is_empty())
                && !due_intervals.contains(&interval_ms)
            {
                due_intervals.push(interval_ms);
            }
        }

        if !due_intervals.is_empty() && !(connection.is_none() && !in_flight_direct.is_empty()) {
            let intervals = std::mem::take(&mut due_intervals);
            let mut settled_pending_keys = None;
            for interval_ms in intervals {
                if pending.get(&interval_ms).is_none_or(HashMap::is_empty) {
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
                let (has_remaining, pending_keys) = publish_conflated_interval_pending(
                    &mut publisher,
                    &mut pending,
                    &mut passthrough,
                    interval_ms,
                    max_commands_per_exec,
                    max_bytes_per_exec,
                    &mut publish_context,
                )
                .await;
                settled_pending_keys = Some(pending_keys);
                metrics.set_output_state(&output_metrics, "running");
                if has_remaining {
                    // One chunk per turn: keep the interval due so the next turn
                    // drains it after completions, timers, and the receiver got a turn.
                    due_intervals.push(interval_ms);
                }
            }
            // One coalesced gauge update per settle pass instead of per chunk.
            if let Some(pending_keys) = settled_pending_keys {
                metrics.publish_output_pending_keys(&output_metrics, pending_keys);
            }
        }

        while passthrough.len() > in_flight_direct_messages
            && in_flight_direct_messages < max_in_flight_commands
            && !(connection.is_none() && !in_flight_direct.is_empty())
        {
            // A new batch is bounded by both the per-batch ceilings and the
            // remaining in-flight window.
            let available_commands =
                max_commands_per_exec.min(max_in_flight_commands - in_flight_direct_messages);
            let available_bytes =
                max_bytes_per_exec.min(max_in_flight_bytes.saturating_sub(in_flight_direct_bytes));
            let (batch_length, atomic, request_bytes) = passthrough_batch_length(
                available_commands,
                available_bytes,
                passthrough.iter().skip(in_flight_direct_messages),
            );
            if batch_length == 0
                || (in_flight_direct_messages > 0 && request_bytes > available_bytes)
            {
                break;
            }
            let batch = passthrough
                .iter()
                .skip(in_flight_direct_messages)
                .take(batch_length)
                .cloned()
                .collect::<Vec<_>>();

            if connection.is_none() {
                metrics.set_output_state(&output_metrics, "connecting");
                if let Err(failure) =
                    ensure_output_connection(&config, &client, &mut connection).await
                {
                    let mut publish_context = OutputPublishContext {
                        name: &name,
                        failure_log: &mut failure_log,
                        deduplication_cache: &mut deduplication_cache,
                        metrics: &metrics,
                        output_metrics: &output_metrics,
                    };
                    let result = settle_publish_result(&batch, Err(failure), &mut publish_context);
                    if let Err(failure) = result {
                        let pending_keys = flush::settle_failed_batch(
                            0,
                            &batch,
                            &mut pending,
                            &mut passthrough,
                            &failure,
                            &metrics,
                            &output_metrics,
                        );
                        metrics.publish_output_pending_keys(&output_metrics, pending_keys);
                    }
                    metrics.set_output_state(&output_metrics, "running");
                    continue;
                }
            }

            let connection_clone = connection
                .as_ref()
                .expect("output connection was initialized")
                .clone();
            // Polling this ordered stream submits direct commands without
            // awaiting each Redis reply before accepting the next message.
            let mut publish_future: DirectPublishFuture<'_> = Box::pin(
                publish_batch_on_connection(&config, connection_clone, batch, atomic),
            );
            // Poll once in FIFO order so the MultiplexedConnection receives
            // the command now; retain the future only to collect its reply.
            let initial_poll =
                poll_fn(|context| Poll::Ready(publish_future.as_mut().poll(context))).await;
            if let Poll::Ready(result) = initial_poll {
                publish_future = Box::pin(std::future::ready(result));
            }
            in_flight_direct.push_back(publish_future);
            in_flight_batches.push_back((batch_length, request_bytes));
            in_flight_direct_messages += batch_length;
            in_flight_direct_bytes = in_flight_direct_bytes.saturating_add(request_bytes);
            metrics.set_output_state(&output_metrics, "publishing");
        }

        let has_pending = pending_message_count(&pending) > 0 || !passthrough.is_empty();
        if input_closed && !has_pending && deferred_input.is_none() {
            break;
        }

        let next_flush_tick = next_schedule_tick(&flush_schedules);
        tokio::select! {
            biased;
            // Settle in-flight completions before the timer so a busy flush
            // schedule cannot starve them.
            completed = in_flight_direct.next(), if !in_flight_direct.is_empty() => {
                if let Some((batch, result, publish_rtt)) = completed {
                    let (message_count, request_bytes) = in_flight_batches
                        .pop_front()
                        .expect("in-flight batch accounting must stay aligned");
                    debug_assert_eq!(message_count, batch.len());
                    in_flight_direct_messages -= message_count;
                    in_flight_direct_bytes = in_flight_direct_bytes.saturating_sub(request_bytes);
                    let mut publish_context = OutputPublishContext {
                        name: &name,
                        failure_log: &mut failure_log,
                        deduplication_cache: &mut deduplication_cache,
                        metrics: &metrics,
                        output_metrics: &output_metrics,
                    };
                    metrics.record_output_publish_rtt(&output_metrics, publish_rtt);
                    let result = settle_publish_result(&batch, result, &mut publish_context);
                    let pending_keys = match result {
                        Ok(subscribers) => {
                            let payload_bytes =
                                batch.iter().map(|message| message.payload.len()).sum();
                            let queue_bytes = batch.iter().map(pending_message_queue_bytes).sum();
                            let pending_keys =
                                clear_published_batch(0, &mut pending, &mut passthrough, &batch);
                            metrics.record_output_flush(
                                &output_metrics,
                                batch.len(),
                                payload_bytes,
                                queue_bytes,
                            );
                            debug!(
                                output = name,
                                messages = batch.len(),
                                subscribers,
                                atomic = batch.len() > 1,
                                "output_batch_published"
                            );
                            pending_keys
                        }
                        Err(failure) => {
                            connection = None;
                            flush::settle_failed_batch(
                                0,
                                &batch,
                                &mut pending,
                                &mut passthrough,
                                &failure,
                                &metrics,
                                &output_metrics,
                            )
                        }
                    };
                    metrics.publish_output_pending_keys(&output_metrics, pending_keys);
                    if in_flight_direct.is_empty() {
                        metrics.set_output_state(&output_metrics, "running");
                    }
                }
            }
            _ = async {
                if let Some(next_tick) = next_flush_tick {
                    time::sleep_until(next_tick).await;
                } else {
                    std::future::pending::<()>().await;
                }
            }, if !flush_schedules.is_empty() => {}
            _ = async {
                if let Some(interval) = deduplication_prune_interval.as_mut() {
                    interval.tick().await;
                } else {
                    std::future::pending::<()>().await;
                }
            }, if deduplication_prune_interval.is_some() => {
                deduplication_cache.prune_expired(time::Instant::now());
            }
            message = receiver.recv(), if !input_closed
                && deferred_input.is_none()
                && due_intervals.is_empty()
                && passthrough.len() == in_flight_direct_messages
                && in_flight_direct_messages < max_in_flight_commands
                && in_flight_direct_bytes < max_in_flight_bytes => {
                match message {
                    Some(message) => {
                        deferred_input = Some(message);
                    }
                    None => {
                        input_closed = true;
                        for (&interval_ms, messages) in &pending {
                            if !messages.is_empty() && !due_intervals.contains(&interval_ms) {
                                due_intervals.push(interval_ms);
                            }
                        }
                    }
                }
            }
        }
    }
    failure_log.flush_suppressed(&name);
    policy_log.flush_suppressed(&name);
    shed_log.flush_suppressed(&name);
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
