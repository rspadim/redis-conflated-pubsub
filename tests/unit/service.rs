use std::collections::BTreeMap;
use std::sync::atomic::Ordering;

use crate::config::{
    ChannelFilterRule, ChannelPolicy, DeduplicationGroup, FilterAction, Subscription,
};

use super::*;

fn pending_message(output_channel: &str, payload: impl Into<Arc<[u8]>>) -> PendingMessage {
    PendingMessage {
        output_channel: output_channel.to_owned(),
        conflation_interval_ms: 0,
        deduplication_ttl_ms: None,
        deduplication_group: None,
        payload: payload.into(),
        raw_payload: None,
    }
}

fn pending_message_with_raw(
    output_channel: &str,
    payload: impl Into<Arc<[u8]>>,
    raw_payload: impl Into<Arc<[u8]>>,
) -> PendingMessage {
    PendingMessage {
        output_channel: output_channel.to_owned(),
        conflation_interval_ms: 0,
        deduplication_ttl_ms: None,
        deduplication_group: None,
        payload: payload.into(),
        raw_payload: Some(raw_payload.into()),
    }
}

fn pending_group_message(output_channel: &str, payload: &[u8], group: &str) -> PendingMessage {
    let mut message = pending_message(output_channel, payload.to_vec());
    message.deduplication_group = Some(group.to_owned());
    message
}

fn group_settings(ttl_ms: i64, restart_on_change: bool) -> BTreeMap<String, DeduplicationGroup> {
    group_settings_with_round(ttl_ms, restart_on_change, 0)
}

fn group_settings_with_round(
    ttl_ms: i64,
    restart_on_change: bool,
    round_ms: u64,
) -> BTreeMap<String, DeduplicationGroup> {
    group_settings_with_limits(
        ttl_ms,
        restart_on_change,
        round_ms,
        16_384,
        64 * 1024 * 1024,
    )
}

fn group_settings_with_limits(
    ttl_ms: i64,
    restart_on_change: bool,
    round_ms: u64,
    max_members: usize,
    max_cache_bytes: usize,
) -> BTreeMap<String, DeduplicationGroup> {
    BTreeMap::from([(
        "shared".to_owned(),
        DeduplicationGroup {
            ttl_ms,
            round_ms,
            restart_on_change,
            max_members,
            max_cache_bytes,
        },
    )])
}

fn channel_policy(
    glob: Option<&str>,
    prefix: Option<&str>,
    suffix: Option<&str>,
    interval_ms: Option<i64>,
    ttl_ms: Option<i64>,
) -> ChannelPolicy {
    ChannelPolicy {
        glob: glob.map(str::to_owned),
        prefix: prefix.map(str::to_owned),
        suffix: suffix.map(str::to_owned),
        default: false,
        profile: None,
        conflation_interval_ms: interval_ms,
        deduplication_ttl_ms: ttl_ms,
        deduplication_group: None,
    }
}

fn output_config_with_policies(
    interval_ms: i64,
    ttl_ms: i64,
    channel_policies: Vec<ChannelPolicy>,
) -> OutputConfig {
    serde_json::from_value(serde_json::json!({
        "redis": { "host": "localhost" },
        "conflation": { "interval_ms": interval_ms },
        "deduplication": { "ttl_ms": ttl_ms },
        "channel_policies": channel_policies,
    }))
    .expect("test output config should deserialize")
}

#[derive(Default)]
struct RecordingPublisher {
    not_sent_failures_remaining: usize,
    uncertain_failures_remaining: usize,
    attempts: Vec<(bool, Vec<PendingMessage>)>,
    possible_executions: Vec<Vec<PendingMessage>>,
    successful_batches: Vec<Vec<PendingMessage>>,
}

impl BatchPublisher for RecordingPublisher {
    async fn publish(
        &mut self,
        messages: &[PendingMessage],
        atomic: bool,
    ) -> std::result::Result<i64, PublishFailure> {
        self.attempts.push((atomic, messages.to_vec()));
        if self.not_sent_failures_remaining > 0 {
            self.not_sent_failures_remaining -= 1;
            return Err(PublishFailure::NotSent(
                "simulated connect failure".to_owned(),
            ));
        }
        if self.uncertain_failures_remaining > 0 {
            self.uncertain_failures_remaining -= 1;
            self.possible_executions.push(messages.to_vec());
            return Err(PublishFailure::Uncertain(
                "simulated lost acknowledgement".to_owned(),
            ));
        }
        self.successful_batches.push(messages.to_vec());
        Ok(messages.len() as i64)
    }
}

fn enqueue_for_test(
    interval_ms: i64,
    message: InboundMessage,
    pending: &mut PendingByInterval,
    passthrough: &mut VecDeque<PendingMessage>,
    metrics: &Metrics,
    output_metrics: &OutputMetrics,
) {
    let mut deduplication_cache = DeduplicationCache::new(0);
    enqueue_for_test_with_cache(
        interval_ms,
        message,
        pending,
        passthrough,
        metrics,
        output_metrics,
        &mut deduplication_cache,
    );
}

fn enqueue_for_test_with_cache(
    interval_ms: i64,
    message: InboundMessage,
    pending: &mut PendingByInterval,
    passthrough: &mut VecDeque<PendingMessage>,
    metrics: &Metrics,
    output_metrics: &OutputMetrics,
    deduplication_cache: &mut DeduplicationCache,
) {
    enqueue_for_test_with_cache_at(
        interval_ms,
        message,
        pending,
        passthrough,
        metrics,
        output_metrics,
        deduplication_cache,
        time::Instant::now(),
    );
}

#[allow(clippy::too_many_arguments)]
fn enqueue_for_test_with_cache_at(
    interval_ms: i64,
    message: InboundMessage,
    pending: &mut PendingByInterval,
    passthrough: &mut VecDeque<PendingMessage>,
    metrics: &Metrics,
    output_metrics: &OutputMetrics,
    deduplication_cache: &mut DeduplicationCache,
    now: time::Instant,
) {
    enqueue_for_test_with_policy_at(
        message,
        pending,
        passthrough,
        metrics,
        output_metrics,
        deduplication_cache,
        now,
        OutputMessagePolicy {
            interval_ms,
            deduplication_ttl_ms: None,
            deduplication_group: None,
            max_bytes_per_exec: 4 * 1024 * 1024,
            oversized_policy: OversizedMessagePolicy::Send,
        },
    );
}

#[allow(clippy::too_many_arguments)]
fn enqueue_for_test_with_policy_at(
    message: InboundMessage,
    pending: &mut PendingByInterval,
    passthrough: &mut VecDeque<PendingMessage>,
    metrics: &Metrics,
    output_metrics: &OutputMetrics,
    deduplication_cache: &mut DeduplicationCache,
    now: time::Instant,
    policy: OutputMessagePolicy,
) {
    let mut policy_log = OversizedPolicyLog::default();
    let mut context = OutputMessageContext {
        name: "test-output",
        policy,
        metrics,
        output_metrics,
        policy_log: &mut policy_log,
    };
    let pending_keys = enqueue_message(
        message,
        pending,
        passthrough,
        &mut context,
        deduplication_cache,
        now,
    );
    // The worker publishes the gauge once per intake pass; test helpers do the
    // same right after the single enqueue so existing assertions keep holding.
    metrics.publish_output_pending_keys(output_metrics, pending_keys);
}

fn publish_context_for_test<'a>(
    name: &'a str,
    failure_log: &'a mut OutputFailureLog,
    deduplication_cache: &'a mut DeduplicationCache,
    metrics: &'a Metrics,
    output_metrics: &'a OutputMetrics,
) -> OutputPublishContext<'a> {
    OutputPublishContext {
        name,
        failure_log,
        deduplication_cache,
        metrics,
        output_metrics,
    }
}

async fn flush_passthrough_for_test(
    name: &str,
    publisher: &mut RecordingPublisher,
    pending: &mut PendingByInterval,
    passthrough: &mut VecDeque<PendingMessage>,
    cache: &mut DeduplicationCache,
    metrics: &Metrics,
    output_metrics: &OutputMetrics,
) {
    let mut failure_log = OutputFailureLog::default();
    let mut context =
        publish_context_for_test(name, &mut failure_log, cache, metrics, output_metrics);
    publish_passthrough_pending(publisher, pending, passthrough, &mut context).await;
}

async fn flush_conflated_for_test(
    name: &str,
    publisher: &mut RecordingPublisher,
    pending: &mut PendingByInterval,
    cache: &mut DeduplicationCache,
    metrics: &Metrics,
    output_metrics: &OutputMetrics,
) {
    let mut failure_log = OutputFailureLog::default();
    let mut context =
        publish_context_for_test(name, &mut failure_log, cache, metrics, output_metrics);
    publish_conflated_pending(publisher, pending, 256, usize::MAX, &mut context).await;
}

fn prepare_for_test(
    name: &str,
    policy: OutputMessagePolicy,
    message: InboundMessage,
    metrics: &Metrics,
    output_metrics: &OutputMetrics,
    policy_log: &mut OversizedPolicyLog,
) -> Option<PendingMessage> {
    let mut context = OutputMessageContext {
        name,
        policy,
        metrics,
        output_metrics,
        policy_log,
    };
    prepare_output_message(message, &mut context)
}

#[path = "service/batching_publish.rs"]
mod batching_publish;
#[path = "service/dedup.rs"]
mod dedup;
#[path = "service/direct_lane_load.rs"]
mod direct_lane_load;
#[path = "service/filter_load.rs"]
mod filter_load;
#[path = "service/input_filtering.rs"]
mod input_filtering;
#[path = "service/policy_schedules.rs"]
mod policy_schedules;
#[path = "service/reload_runtime.rs"]
mod reload_runtime;
