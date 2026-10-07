use std::collections::{HashMap, VecDeque};

use tokio::time;
use tracing::debug;

use crate::status::{Metrics, OutputMetrics};

use super::super::batch::{pending_batch, pending_batch_length, prepare_output_message};
use super::super::publish::{BatchPublisher, PublishFailure, publish_once};
use super::super::{
    DeduplicationCache, InboundMessage, OutputMessageContext, OutputPublishContext, PendingMessage,
};

pub(in crate::service) fn enqueue_message(
    message: InboundMessage,
    pending: &mut HashMap<String, PendingMessage>,
    passthrough: &mut VecDeque<PendingMessage>,
    context: &mut OutputMessageContext<'_>,
    deduplication_cache: &mut DeduplicationCache,
    now: time::Instant,
) {
    let interval_ms = context.policy.interval_ms;
    let Some(pending_message) = prepare_output_message(message, context) else {
        return;
    };
    if interval_ms <= 0 && deduplication_cache.should_suppress(&pending_message, now) {
        context.metrics.record_output_deduplicated(
            context.output_metrics,
            pending_message.raw_payload().len(),
            pending_message.payload.len(),
        );
        context
            .metrics
            .set_output_pending_keys(context.output_metrics, pending.len() + passthrough.len());
        return;
    }
    if interval_ms > 0 {
        if let Some(replaced) =
            pending.insert(pending_message.output_channel.clone(), pending_message)
        {
            context
                .metrics
                .record_output_conflated(context.output_metrics, replaced.payload.len());
        }
    } else {
        passthrough.push_back(pending_message);
    }
    context
        .metrics
        .set_output_pending_keys(context.output_metrics, pending.len() + passthrough.len());
}

pub(in crate::service) fn clear_published_batch(
    interval_ms: i64,
    pending: &mut HashMap<String, PendingMessage>,
    passthrough: &mut VecDeque<PendingMessage>,
    published: &[PendingMessage],
    metrics: &Metrics,
    output_metrics: &OutputMetrics,
) {
    if interval_ms > 0 {
        for message in published {
            if pending.get(&message.output_channel) == Some(message) {
                pending.remove(&message.output_channel);
            }
        }
    } else {
        passthrough.pop_front();
    }
    metrics.set_output_pending_keys(output_metrics, pending.len() + passthrough.len());
}

pub(super) fn settle_failed_batch(
    interval_ms: i64,
    batch: &[PendingMessage],
    pending: &mut HashMap<String, PendingMessage>,
    passthrough: &mut VecDeque<PendingMessage>,
    failure: &PublishFailure,
    metrics: &Metrics,
    output_metrics: &OutputMetrics,
) {
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
        for message in batch {
            if pending.get(&message.output_channel) == Some(message) {
                pending.remove(&message.output_channel);
            }
        }
    } else {
        for _ in batch {
            passthrough.pop_front();
        }
    }
    metrics.set_output_pending_keys(output_metrics, pending.len() + passthrough.len());
}

#[cfg(test)]
pub(in crate::service) async fn publish_conflated_pending<P: BatchPublisher>(
    publisher: &mut P,
    pending: &mut HashMap<String, PendingMessage>,
    max_commands_per_exec: usize,
    max_bytes_per_exec: usize,
    context: &mut OutputPublishContext<'_>,
) {
    let mut passthrough = VecDeque::new();
    publish_conflated_interval_pending(
        publisher,
        pending,
        &mut passthrough,
        None,
        max_commands_per_exec,
        max_bytes_per_exec,
        context,
    )
    .await;
}

pub(in crate::service) async fn publish_conflated_interval_pending<P: BatchPublisher>(
    publisher: &mut P,
    pending: &mut HashMap<String, PendingMessage>,
    passthrough: &mut VecDeque<PendingMessage>,
    interval_ms: Option<i64>,
    max_commands_per_exec: usize,
    max_bytes_per_exec: usize,
    context: &mut OutputPublishContext<'_>,
) {
    let mut candidate_channels = pending
        .iter()
        .filter(|(_, message)| {
            interval_ms.is_none_or(|interval_ms| message.conflation_interval_ms == interval_ms)
        })
        .map(|(channel, _)| channel.clone())
        .collect::<Vec<_>>();
    candidate_channels.sort();
    let mut offset = 0usize;
    while offset < candidate_channels.len() {
        let candidate_batch_length = pending_batch_length(
            max_commands_per_exec,
            max_bytes_per_exec,
            pending,
            &candidate_channels[offset..],
        );
        let candidate_batch = &candidate_channels[offset..offset + candidate_batch_length];
        let now = time::Instant::now();
        let mut eligible_channels = Vec::with_capacity(candidate_batch_length);
        let mut deduplicated_channels = Vec::new();
        for channel in candidate_batch {
            let message = pending
                .get(channel)
                .expect("pending channel snapshot must remain present");
            if context.deduplication_cache.should_suppress(message, now) {
                deduplicated_channels.push(channel.clone());
            } else {
                eligible_channels.push(channel.clone());
            }
        }
        let removed_deduplicated = !deduplicated_channels.is_empty();
        for channel in deduplicated_channels {
            let message = pending
                .remove(&channel)
                .expect("deduplicated pending channel must remain present");
            context.metrics.record_output_deduplicated(
                context.output_metrics,
                message.raw_payload().len(),
                message.payload.len(),
            );
        }
        if removed_deduplicated {
            context
                .metrics
                .set_output_pending_keys(context.output_metrics, pending.len() + passthrough.len());
        }
        if eligible_channels.is_empty() {
            offset += candidate_batch_length;
            tokio::task::yield_now().await;
            continue;
        }

        let batch = eligible_channels
            .iter()
            .map(|channel| {
                pending
                    .get(channel)
                    .expect("pending channel snapshot must remain present")
                    .clone()
            })
            .collect::<Vec<_>>();
        let last_candidate_index = offset + candidate_batch_length - 1;
        let publish_result = publish_once(publisher, &batch, true, context).await;
        let subscribers = match publish_result {
            Ok(subscribers) => subscribers,
            Err(failure) => {
                settle_failed_batch(
                    1,
                    &batch,
                    pending,
                    passthrough,
                    &failure,
                    context.metrics,
                    context.output_metrics,
                );
                offset = last_candidate_index + 1;
                tokio::task::yield_now().await;
                continue;
            }
        };
        let message_count = batch.len();
        let payload_bytes = batch.iter().map(|message| message.payload.len()).sum();
        clear_published_batch(
            1,
            pending,
            passthrough,
            &batch,
            context.metrics,
            context.output_metrics,
        );
        context
            .metrics
            .record_output_flush(context.output_metrics, message_count, payload_bytes);
        debug!(
            output = context.name,
            messages = message_count,
            subscribers,
            atomic = true,
            "output_batch_published"
        );
        offset = last_candidate_index + 1;
        tokio::task::yield_now().await;
    }
}

pub(in crate::service) async fn publish_passthrough_pending<P: BatchPublisher>(
    publisher: &mut P,
    pending: &mut HashMap<String, PendingMessage>,
    passthrough: &mut VecDeque<PendingMessage>,
    context: &mut OutputPublishContext<'_>,
) {
    while !passthrough.is_empty() {
        let batch = pending_batch(0, 1, 0, pending, passthrough);
        match publish_once(publisher, &batch, false, context).await {
            Ok(subscribers) => {
                let payload_bytes = batch.iter().map(|message| message.payload.len()).sum();
                clear_published_batch(
                    0,
                    pending,
                    passthrough,
                    &batch,
                    context.metrics,
                    context.output_metrics,
                );
                context.metrics.record_output_flush(
                    context.output_metrics,
                    batch.len(),
                    payload_bytes,
                );
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
                drop(batch);
            }
        }
        tokio::task::yield_now().await;
    }
}
