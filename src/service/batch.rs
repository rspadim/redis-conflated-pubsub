use std::{
    collections::{HashMap, VecDeque},
    time::Duration,
};

use tokio::time;
use tracing::warn;

use crate::config::OversizedMessagePolicy;

use super::{InboundMessage, OutputMessageContext, OutputMessagePolicy, PendingMessage};

pub(super) fn pending_batch(
    interval_ms: i64,
    max_commands_per_exec: usize,
    max_bytes_per_exec: usize,
    pending: &HashMap<String, PendingMessage>,
    passthrough: &VecDeque<PendingMessage>,
) -> Vec<PendingMessage> {
    if interval_ms > 0 {
        let mut candidates = pending.keys().cloned().collect::<Vec<_>>();
        candidates.sort();
        let batch_length = pending_batch_length(
            max_commands_per_exec,
            max_bytes_per_exec,
            pending,
            &candidates,
        );
        candidates
            .iter()
            .take(batch_length)
            .map(|channel| {
                pending
                    .get(channel)
                    .expect("pending channel snapshot must remain present")
                    .clone()
            })
            .collect()
    } else {
        passthrough.front().cloned().into_iter().collect()
    }
}

pub(super) fn pending_batch_length(
    max_commands_per_exec: usize,
    max_bytes_per_exec: usize,
    pending: &HashMap<String, PendingMessage>,
    candidate_channels: &[String],
) -> usize {
    let mut batch_length = 0usize;
    let mut batch_bytes = 0usize;
    for channel in candidate_channels.iter().take(max_commands_per_exec) {
        let message = pending
            .get(channel)
            .expect("pending channel snapshot must remain present");
        let message_bytes = publish_command_frame_bytes(message);
        let transaction_bytes = batch_bytes
            .saturating_add(message_bytes)
            .saturating_add(RESP_MULTI_FRAME_BYTES + RESP_EXEC_FRAME_BYTES);
        if batch_length > 0 && transaction_bytes > max_bytes_per_exec {
            break;
        }
        batch_bytes = batch_bytes.saturating_add(message_bytes);
        batch_length += 1;
    }
    batch_length
}

pub(super) const RESP_MULTI_FRAME_BYTES: usize = 15;
pub(super) const RESP_EXEC_FRAME_BYTES: usize = 14;
const RESP_COMMAND_ARRAY_HEADER_BYTES: usize = 4; // `*3\r\n`

pub(super) fn resp_bulk_frame_bytes(argument_len: usize) -> usize {
    1usize
        .saturating_add(decimal_digits(argument_len))
        .saturating_add(2)
        .saturating_add(argument_len)
        .saturating_add(2)
}

pub(super) fn decimal_digits(mut value: usize) -> usize {
    let mut digits = 1;
    while value >= 10 {
        value /= 10;
        digits += 1;
    }
    digits
}

pub(super) fn publish_command_frame_bytes(message: &PendingMessage) -> usize {
    publish_command_frame_bytes_for_lengths(message.output_channel.len(), message.payload.len())
}

pub(super) fn publish_command_frame_bytes_for_lengths(
    channel_len: usize,
    payload_len: usize,
) -> usize {
    RESP_COMMAND_ARRAY_HEADER_BYTES
        .saturating_add(resp_bulk_frame_bytes(b"PUBLISH".len()))
        .saturating_add(resp_bulk_frame_bytes(channel_len))
        .saturating_add(resp_bulk_frame_bytes(payload_len))
}

pub(super) fn publish_operation_frame_bytes(message: &PendingMessage, atomic: bool) -> usize {
    publish_command_frame_bytes(message).saturating_add(if atomic {
        RESP_MULTI_FRAME_BYTES + RESP_EXEC_FRAME_BYTES
    } else {
        0
    })
}

#[cfg(test)]
pub(super) fn exec_transaction_frame_bytes(messages: &[PendingMessage]) -> usize {
    messages.iter().fold(
        RESP_MULTI_FRAME_BYTES + RESP_EXEC_FRAME_BYTES,
        |total, message| total.saturating_add(publish_command_frame_bytes(message)),
    )
}

pub(super) fn max_payload_length_for_target(
    channel_len: usize,
    original_payload_len: usize,
    max_bytes: usize,
    atomic: bool,
) -> Option<usize> {
    let framing_bytes = if atomic {
        RESP_MULTI_FRAME_BYTES + RESP_EXEC_FRAME_BYTES
    } else {
        0
    };
    if publish_command_frame_bytes_for_lengths(channel_len, 0).saturating_add(framing_bytes)
        > max_bytes
    {
        return None;
    }

    let mut low = 0;
    let mut high = original_payload_len;
    while low < high {
        let middle = low + (high - low) / 2 + 1;
        if publish_command_frame_bytes_for_lengths(channel_len, middle)
            .saturating_add(framing_bytes)
            <= max_bytes
        {
            low = middle;
        } else {
            high = middle - 1;
        }
    }
    Some(low)
}

const OVERSIZED_LOG_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Default)]
pub(super) struct OversizedPolicyLog {
    last_report: Option<time::Instant>,
    dropped_messages: u64,
    truncated_messages: u64,
    truncated_payload_bytes: u64,
}

impl OversizedPolicyLog {
    pub(super) fn report(
        &mut self,
        output: &str,
        dropped_messages: usize,
        truncated_messages: usize,
        truncated_payload_bytes: usize,
    ) {
        self.dropped_messages = self
            .dropped_messages
            .saturating_add(dropped_messages as u64);
        self.truncated_messages = self
            .truncated_messages
            .saturating_add(truncated_messages as u64);
        self.truncated_payload_bytes = self
            .truncated_payload_bytes
            .saturating_add(truncated_payload_bytes as u64);

        let now = time::Instant::now();
        if self
            .last_report
            .is_none_or(|last| now.duration_since(last) >= OVERSIZED_LOG_INTERVAL)
        {
            warn!(
                output,
                dropped_messages = self.dropped_messages,
                truncated_messages = self.truncated_messages,
                truncated_payload_bytes = self.truncated_payload_bytes,
                "oversized_output_messages_handled"
            );
            self.last_report = Some(now);
            self.dropped_messages = 0;
            self.truncated_messages = 0;
            self.truncated_payload_bytes = 0;
        }
    }

    pub(super) fn flush_suppressed(&mut self, output: &str) {
        if self.dropped_messages > 0 || self.truncated_messages > 0 {
            warn!(
                output,
                dropped_messages = self.dropped_messages,
                truncated_messages = self.truncated_messages,
                truncated_payload_bytes = self.truncated_payload_bytes,
                "oversized_output_messages_suppressed"
            );
            self.dropped_messages = 0;
            self.truncated_messages = 0;
            self.truncated_payload_bytes = 0;
        }
    }
}

pub(super) fn prepare_output_message(
    message: InboundMessage,
    context: &mut OutputMessageContext<'_>,
) -> Option<PendingMessage> {
    let OutputMessagePolicy {
        interval_ms,
        deduplication_ttl_ms,
        deduplication_group,
        max_bytes_per_exec,
        oversized_policy,
    } = context.policy.clone();
    let mut pending_message = PendingMessage {
        output_channel: message.output_channel,
        conflation_interval_ms: interval_ms,
        deduplication_ttl_ms,
        deduplication_group,
        payload: message.payload,
        raw_payload: None,
    };
    let atomic = interval_ms > 0;
    if publish_operation_frame_bytes(&pending_message, atomic) <= max_bytes_per_exec
        || oversized_policy == OversizedMessagePolicy::Send
    {
        return Some(pending_message);
    }

    match oversized_policy {
        OversizedMessagePolicy::Send => Some(pending_message),
        OversizedMessagePolicy::Drop => {
            context.metrics.record_output_dropped(
                context.output_metrics,
                1,
                pending_message.payload.len(),
            );
            context.policy_log.report(context.name, 1, 0, 0);
            None
        }
        OversizedMessagePolicy::Truncate => {
            let Some(max_payload_len) = max_payload_length_for_target(
                pending_message.output_channel.len(),
                pending_message.payload.len(),
                max_bytes_per_exec,
                atomic,
            ) else {
                context.metrics.record_output_dropped(
                    context.output_metrics,
                    1,
                    pending_message.payload.len(),
                );
                context.policy_log.report(context.name, 1, 0, 0);
                return None;
            };
            let truncated_bytes = pending_message.payload.len() - max_payload_len;
            pending_message.raw_payload = Some(pending_message.payload.clone());
            pending_message.payload.truncate(max_payload_len);
            context
                .metrics
                .record_output_truncated(context.output_metrics, truncated_bytes);
            context
                .policy_log
                .report(context.name, 0, 1, truncated_bytes);
            Some(pending_message)
        }
    }
}
