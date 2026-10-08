use std::collections::{HashMap, VecDeque};

use tokio::time;
use tracing::debug;

use crate::status::{Metrics, OutputMetrics};

#[cfg(test)]
use super::super::batch::passthrough_batch_length;
use super::super::batch::{pending_batch_length, prepare_output_message};
use super::super::publish::{BatchPublisher, PublishFailure, publish_once};
use super::super::{
    DeduplicationCache, InboundMessage, OutputMessageContext, OutputPublishContext,
    PendingByInterval, PendingMessage, pending_message_count,
};

/// Number of pending output keys: conflated buckets plus the passthrough queue.
fn pending_key_count(pending: &PendingByInterval, passthrough: &VecDeque<PendingMessage>) -> usize {
    pending_message_count(pending) + passthrough.len()
}

pub(in crate::service) fn enqueue_message(
    message: InboundMessage,
    pending: &mut PendingByInterval,
    passthrough: &mut VecDeque<PendingMessage>,
    context: &mut OutputMessageContext<'_>,
    deduplication_cache: &mut DeduplicationCache,
    now: time::Instant,
) -> usize {
    let interval_ms = context.policy.interval_ms;
    let Some(pending_message) = prepare_output_message(message, context) else {
        return pending_key_count(pending, passthrough);
    };
    if interval_ms <= 0
        && deduplication_cache.is_active_for(&pending_message)
        && deduplication_cache.should_suppress(&pending_message, now)
    {
        context.metrics.record_output_deduplicated(
            context.output_metrics,
            pending_message.raw_payload().len(),
            pending_message.payload.len(),
        );
        return pending_key_count(pending, passthrough);
    }
    if interval_ms > 0 {
        let interval_pending = pending.entry(interval_ms).or_default();
        if let Some(replaced) =
            interval_pending.insert(pending_message.output_channel.clone(), pending_message)
        {
            context
                .metrics
                .record_output_conflated(context.output_metrics, replaced.payload.len());
        }
    } else {
        passthrough.push_back(pending_message);
    }
    pending_key_count(pending, passthrough)
}

pub(in crate::service) fn clear_published_batch(
    interval_ms: i64,
    pending: &mut PendingByInterval,
    passthrough: &mut VecDeque<PendingMessage>,
    published: &[PendingMessage],
) -> usize {
    if interval_ms > 0 {
        let remove_interval = if let Some(interval_pending) = pending.get_mut(&interval_ms) {
            for message in published {
                if interval_pending.get(&message.output_channel) == Some(message) {
                    interval_pending.remove(&message.output_channel);
                }
            }
            interval_pending.is_empty()
        } else {
            false
        };
        if remove_interval {
            pending.remove(&interval_ms);
        }
    } else {
        for _ in published {
            passthrough.pop_front();
        }
    }
    pending_key_count(pending, passthrough)
}

pub(super) fn settle_failed_batch(
    interval_ms: i64,
    batch: &[PendingMessage],
    pending: &mut PendingByInterval,
    passthrough: &mut VecDeque<PendingMessage>,
    failure: &PublishFailure,
    metrics: &Metrics,
    output_metrics: &OutputMetrics,
) -> usize {
    let payload_bytes = batch.iter().map(|message| message.payload.len()).sum();
    match failure {
        PublishFailure::NotSent(_) => {
            metrics.record_output_dropped(output_metrics, batch.len(), payload_bytes)
        }
        PublishFailure::Uncertain(_) => {
            metrics.record_output_abandoned(output_metrics, batch.len(), payload_bytes)
        }
    }
    if interval_ms > 0 {
        let remove_interval = if let Some(interval_pending) = pending.get_mut(&interval_ms) {
            for message in batch {
                if interval_pending.get(&message.output_channel) == Some(message) {
                    interval_pending.remove(&message.output_channel);
                }
            }
            interval_pending.is_empty()
        } else {
            false
        };
        if remove_interval {
            pending.remove(&interval_ms);
        }
    } else {
        for _ in batch {
            passthrough.pop_front();
        }
    }
    pending_key_count(pending, passthrough)
}

#[cfg(test)]
pub(in crate::service) async fn publish_conflated_pending<P: BatchPublisher>(
    publisher: &mut P,
    pending: &mut PendingByInterval,
    max_commands_per_exec: usize,
    max_bytes_per_exec: usize,
    context: &mut OutputPublishContext<'_>,
) {
    let mut passthrough = VecDeque::new();
    let mut intervals = pending.keys().copied().collect::<Vec<_>>();
    intervals.sort_unstable();
    let mut pending_keys = pending_key_count(pending, &passthrough);
    for interval_ms in intervals {
        loop {
            let (has_remaining, keys) = publish_conflated_interval_pending(
                publisher,
                pending,
                &mut passthrough,
                interval_ms,
                max_commands_per_exec,
                max_bytes_per_exec,
                context,
            )
            .await;
            pending_keys = keys;
            if !has_remaining {
                break;
            }
            tokio::task::yield_now().await;
        }
    }
    context
        .metrics
        .publish_output_pending_keys(context.output_metrics, pending_keys);
}

/// Publishes at most one command/byte-bounded chunk of `interval_ms` and reports
/// whether the bucket still has work plus the current pending-key count. One
/// chunk per call lets the worker turn return to completions, timers, and the
/// receiver between chunks.
pub(in crate::service) async fn publish_conflated_interval_pending<P: BatchPublisher>(
    publisher: &mut P,
    pending: &mut PendingByInterval,
    passthrough: &mut VecDeque<PendingMessage>,
    interval_ms: i64,
    max_commands_per_exec: usize,
    max_bytes_per_exec: usize,
    context: &mut OutputPublishContext<'_>,
) -> (bool, usize) {
    publish_interval_bucket(
        publisher,
        pending,
        passthrough,
        interval_ms,
        max_commands_per_exec,
        max_bytes_per_exec,
        context,
    )
    .await
}

async fn publish_interval_bucket<P: BatchPublisher>(
    publisher: &mut P,
    pending: &mut PendingByInterval,
    passthrough: &mut VecDeque<PendingMessage>,
    interval_ms: i64,
    max_commands_per_exec: usize,
    max_bytes_per_exec: usize,
    context: &mut OutputPublishContext<'_>,
) -> (bool, usize) {
    let Some(interval_pending) = pending.get(&interval_ms) else {
        return (false, pending_key_count(pending, passthrough));
    };
    let mut candidate_channels = interval_pending.keys().cloned().collect::<Vec<_>>();
    if candidate_channels.is_empty() {
        return (false, pending_key_count(pending, passthrough));
    }
    candidate_channels.sort();
    let candidate_batch_length = pending_batch_length(
        max_commands_per_exec,
        max_bytes_per_exec,
        interval_pending,
        &candidate_channels,
    );
    let candidate_batch = &candidate_channels[..candidate_batch_length];
    let now = time::Instant::now();
    let mut eligible_channels = Vec::with_capacity(candidate_batch_length);
    let mut deduplicated_channels = Vec::new();
    for channel in candidate_batch {
        let message = pending
            .get(&interval_ms)
            .and_then(|interval_pending| interval_pending.get(channel))
            .expect("pending channel snapshot must remain present");
        if context.deduplication_cache.is_active_for(message)
            && context.deduplication_cache.should_suppress(message, now)
        {
            deduplicated_channels.push(channel.clone());
        } else {
            eligible_channels.push(channel.clone());
        }
    }
    let mut pending_keys = pending_key_count(pending, passthrough);
    if !deduplicated_channels.is_empty() {
        if let Some(interval_pending) = pending.get_mut(&interval_ms) {
            for channel in deduplicated_channels {
                let message = interval_pending
                    .remove(&channel)
                    .expect("deduplicated pending channel must remain present");
                context.metrics.record_output_deduplicated(
                    context.output_metrics,
                    message.raw_payload().len(),
                    message.payload.len(),
                );
            }
        }
        let remove_interval = pending.get(&interval_ms).is_some_and(HashMap::is_empty);
        if remove_interval {
            pending.remove(&interval_ms);
        }
        pending_keys = pending_key_count(pending, passthrough);
    }

    if !eligible_channels.is_empty() {
        let batch = eligible_channels
            .iter()
            .map(|channel| {
                pending
                    .get(&interval_ms)
                    .and_then(|interval_pending| interval_pending.get(channel))
                    .expect("pending channel snapshot must remain present")
                    .clone()
            })
            .collect::<Vec<_>>();
        match publish_once(publisher, &batch, true, context).await {
            Ok(subscribers) => {
                let message_count = batch.len();
                let payload_bytes = batch.iter().map(|message| message.payload.len()).sum();
                pending_keys = clear_published_batch(interval_ms, pending, passthrough, &batch);
                context.metrics.record_output_flush(
                    context.output_metrics,
                    message_count,
                    payload_bytes,
                );
                debug!(
                    output = context.name,
                    interval_ms,
                    messages = message_count,
                    subscribers,
                    atomic = true,
                    "output_batch_published"
                );
            }
            Err(failure) => {
                pending_keys = settle_failed_batch(
                    interval_ms,
                    &batch,
                    pending,
                    passthrough,
                    &failure,
                    context.metrics,
                    context.output_metrics,
                );
            }
        }
    }

    let has_remaining = pending
        .get(&interval_ms)
        .is_some_and(|messages| !messages.is_empty());
    (has_remaining, pending_keys)
}

#[cfg(test)]
pub(in crate::service) async fn publish_passthrough_pending<P: BatchPublisher>(
    publisher: &mut P,
    pending: &mut PendingByInterval,
    passthrough: &mut VecDeque<PendingMessage>,
    context: &mut OutputPublishContext<'_>,
) {
    while publish_passthrough_one(publisher, pending, passthrough, context).await {
        tokio::task::yield_now().await;
    }
    context.metrics.publish_output_pending_keys(
        context.output_metrics,
        pending_key_count(pending, passthrough),
    );
}

#[cfg(test)]
pub(in crate::service) async fn publish_passthrough_one<P: BatchPublisher>(
    publisher: &mut P,
    pending: &mut PendingByInterval,
    passthrough: &mut VecDeque<PendingMessage>,
    context: &mut OutputPublishContext<'_>,
) -> bool {
    let Some(message) = passthrough.front().cloned() else {
        return false;
    };
    let batch = [message];
    match publish_once(publisher, &batch, false, context).await {
        Ok(subscribers) => {
            let payload_bytes = batch.iter().map(|message| message.payload.len()).sum();
            clear_published_batch(0, pending, passthrough, &batch);
            context
                .metrics
                .record_output_flush(context.output_metrics, batch.len(), payload_bytes);
            debug!(
                output = context.name,
                messages = batch.len(),
                subscribers,
                atomic = false,
                "output_batch_published"
            );
        }
        Err(failure) => {
            settle_failed_batch(
                0,
                &batch,
                pending,
                passthrough,
                &failure,
                context.metrics,
                context.output_metrics,
            );
        }
    }
    true
}

#[cfg(test)]
pub(in crate::service) async fn publish_passthrough_batch<P: BatchPublisher>(
    publisher: &mut P,
    pending: &mut PendingByInterval,
    passthrough: &mut VecDeque<PendingMessage>,
    max_commands_per_exec: usize,
    max_bytes_per_exec: usize,
    context: &mut OutputPublishContext<'_>,
) -> bool {
    let (batch_length, atomic, _) = passthrough_batch_length(
        max_commands_per_exec,
        max_bytes_per_exec,
        passthrough.iter(),
    );
    if batch_length == 0 {
        return false;
    }
    let batch = passthrough
        .iter()
        .take(batch_length)
        .cloned()
        .collect::<Vec<_>>();
    match publish_once(publisher, &batch, atomic, context).await {
        Ok(subscribers) => {
            let payload_bytes = batch.iter().map(|message| message.payload.len()).sum();
            clear_published_batch(0, pending, passthrough, &batch);
            context
                .metrics
                .record_output_flush(context.output_metrics, batch.len(), payload_bytes);
            debug!(
                output = context.name,
                messages = batch.len(),
                subscribers,
                atomic,
                "output_batch_published"
            );
        }
        Err(failure) => {
            settle_failed_batch(
                0,
                &batch,
                pending,
                passthrough,
                &failure,
                context.metrics,
                context.output_metrics,
            );
        }
    }
    true
}
