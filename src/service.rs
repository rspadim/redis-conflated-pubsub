use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use anyhow::{Context, Result, anyhow};
use futures_util::StreamExt;
use redis::aio::MultiplexedConnection;
use tokio::{
    sync::mpsc,
    time::{self, MissedTickBehavior},
};
use tracing::{debug, error, info, warn};

use crate::{
    config::{
        AppConfig, HttpStatusConfig, InputConfig, OutputConfig, OversizedMessagePolicy,
        RedisConfig, Subscription, same_pubsub_server,
    },
    http_status, logging,
    status::{self, Metrics, OutputMetrics},
};

const MAX_RETRY_DELAY: Duration = Duration::from_secs(30);

#[derive(Clone, Debug)]
struct InboundMessage {
    output_channel: String,
    payload: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PendingMessage {
    output_channel: String,
    payload: Vec<u8>,
}

struct OutputSender {
    name: String,
    channel_prefix: String,
    channel_suffix: String,
    sender: mpsc::UnboundedSender<InboundMessage>,
    output_metrics: Arc<OutputMetrics>,
}

struct OutputRuntimeSetup {
    name: String,
    config: OutputConfig,
    max_bytes_per_exec: usize,
    oversized_policy: OversizedMessagePolicy,
}

#[derive(Clone, Copy)]
struct OutputMessagePolicy {
    interval_ms: i64,
    max_bytes_per_exec: usize,
    oversized_policy: OversizedMessagePolicy,
}

struct OutputMessageContext<'a> {
    name: &'a str,
    policy: OutputMessagePolicy,
    metrics: &'a Metrics,
    output_metrics: &'a OutputMetrics,
    policy_log: &'a mut OversizedPolicyLog,
}

struct OutputPublishContext<'a> {
    name: &'a str,
    failure_log: &'a mut OutputFailureLog,
    metrics: &'a Metrics,
    output_metrics: &'a OutputMetrics,
}

#[derive(Clone, Debug)]
struct OutputChannelFilter {
    prefix: String,
    suffix: String,
}

impl OutputChannelFilter {
    fn matches(&self, channel: &str) -> bool {
        (!self.prefix.is_empty() && channel.starts_with(&self.prefix))
            || (!self.suffix.is_empty() && channel.ends_with(&self.suffix))
    }
}

pub async fn run(config: AppConfig, metrics: Arc<Metrics>) -> Result<()> {
    let input_client = config.input.redis.client()?;
    let http_listener = bind_status_http(config.status.http.as_ref()).await?;
    let global_max_bytes_per_exec = config.max_bytes_per_exec;
    let global_oversized_message_policy = config.oversized_message_policy;
    let output_setups = config
        .outputs
        .iter()
        .map(|(name, output_config)| {
            Ok((
                name.clone(),
                output_config.clone(),
                output_config
                    .conflation
                    .max_bytes_per_exec
                    .unwrap_or(global_max_bytes_per_exec),
                output_config
                    .conflation
                    .oversized_message_policy
                    .unwrap_or(global_oversized_message_policy),
                output_config.redis.client()?,
                metrics.register_output(name),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let echo_filters = output_echo_filters(&config.input, &config.outputs);
    let (input_senders, mut output_tasks) = output_setups
        .into_iter()
        .map(
            |(
                name,
                output_config,
                max_bytes_per_exec,
                oversized_policy,
                client,
                output_metrics,
            )| {
                let (sender, receiver) = mpsc::unbounded_channel();
                let worker_metrics = Arc::clone(&metrics);
                let worker_output_metrics = Arc::clone(&output_metrics);
                let sender_name = name.clone();
                let channel_prefix = output_config.channel_prefix.clone();
                let channel_suffix = output_config.channel_suffix.clone();
                let runtime_setup = OutputRuntimeSetup {
                    name,
                    config: output_config,
                    max_bytes_per_exec,
                    oversized_policy,
                };
                let task = tokio::spawn(async move {
                    publish_output(
                        runtime_setup,
                        client,
                        receiver,
                        worker_metrics,
                        worker_output_metrics,
                    )
                    .await;
                });
                (
                    OutputSender {
                        name: sender_name,
                        channel_prefix,
                        channel_suffix,
                        sender,
                        output_metrics: Arc::clone(&output_metrics),
                    },
                    task,
                )
            },
        )
        .unzip::<_, _, Vec<_>, Vec<_>>();

    metrics.set_state("running");
    let input_config = config.input.clone();
    let input_metrics = Arc::clone(&metrics);
    let input_task = tokio::spawn(async move {
        read_input(
            input_config,
            input_client,
            input_senders,
            echo_filters,
            input_metrics,
        )
        .await;
    });

    let status_task = config.status.path.clone().map(|path| {
        let update_interval_ms = config.status.update_interval_ms;
        let status_metrics = Arc::clone(&metrics);
        tokio::spawn(async move {
            write_status(path, update_interval_ms, status_metrics).await;
        })
    });

    let http_task = http_listener.map(|listener| {
        let http_metrics = Arc::clone(&metrics);
        tokio::spawn(async move {
            if let Err(error) = http_status::serve(listener, http_metrics.clone()).await {
                http_metrics.record_error(&error);
                error!(error = %error, "http_status_server_failed");
            }
        })
    });

    let cleanup_config = config.logging.clone();
    let cleanup_task = tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(3600));
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            if let Err(error) = logging::cleanup(&cleanup_config) {
                warn!(error = %error, "log_cleanup_failed");
            }
        }
    });

    info!(outputs = config.outputs.len(), "service_ready");
    wait_for_shutdown_signal()
        .await
        .context("failed to listen for shutdown signal")?;

    info!("shutdown_signal_received");
    input_task.abort();
    let _ = input_task.await;

    // Workers drain their in-memory queues and retry retained publications before stopping.
    for task in output_tasks.drain(..) {
        if let Err(error) = task.await {
            metrics.record_error(&error);
            warn!(error = %error, "output_worker_stopped_unexpectedly");
        }
    }

    metrics.set_state("stopped");
    if let Some(path) = &config.status.path {
        let _ = status::write_atomic(path, &metrics.snapshot());
    }
    if let Some(status_task) = status_task {
        status_task.abort();
    }
    if let Some(http_task) = http_task {
        http_task.abort();
    }
    cleanup_task.abort();
    Ok(())
}

fn output_echo_filters(
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

async fn bind_status_http(
    config: Option<&HttpStatusConfig>,
) -> Result<Option<tokio::net::TcpListener>> {
    let Some(config) = config else {
        return Ok(None);
    };
    if config.port == 0 {
        return Err(anyhow!("status.http.port must be greater than zero"));
    }
    let listener = tokio::net::TcpListener::bind((config.bind.as_str(), config.port))
        .await
        .with_context(|| {
            format!(
                "failed to bind HTTP status server to {}:{}",
                config.bind, config.port
            )
        })?;
    info!(address = %listener.local_addr()?, "http_status_server_started");
    Ok(Some(listener))
}

async fn wait_for_shutdown_signal() -> Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .context("failed to install SIGTERM handler")?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                result.context("failed to listen for Ctrl-C")
            }
            _ = terminate.recv() => Ok(()),
        }
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .context("failed to listen for Ctrl-C")
    }
}

async fn read_input(
    config: InputConfig,
    client: redis::Client,
    senders: Vec<OutputSender>,
    echo_filters: Vec<OutputChannelFilter>,
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
                    let source_channel = message.get_channel_name();
                    if echo_filters
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
                    let (prefix, suffix) = subscription.output_mapping();
                    metrics.record_input(message.get_payload_bytes().len());
                    fan_out(
                        &senders,
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

fn fan_out(
    senders: &[OutputSender],
    subscription_prefix: &str,
    source_channel: &str,
    subscription_suffix: &str,
    payload: &[u8],
    metrics: &Metrics,
) {
    for output in senders {
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
            .send(InboundMessage {
                output_channel,
                payload: payload.to_vec(),
            })
            .is_err()
        {
            metrics.rollback_output_input(&output.output_metrics, payload_bytes);
            metrics.record_error(format!("output worker {} is unavailable", output.name));
            warn!(output = %output.name, "output_worker_unavailable");
        }
    }
}

fn matching_subscription<'a>(
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

fn map_output_channel(
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

fn enqueue_message(
    message: InboundMessage,
    pending: &mut HashMap<String, PendingMessage>,
    passthrough: &mut VecDeque<PendingMessage>,
    context: &mut OutputMessageContext<'_>,
) {
    let interval_ms = context.policy.interval_ms;
    let Some(pending_message) = prepare_output_message(message, context) else {
        return;
    };
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
    context.metrics.set_output_pending_keys(
        context.output_metrics,
        if interval_ms > 0 {
            pending.len()
        } else {
            passthrough.len()
        },
    );
}

fn pending_batch(
    interval_ms: i64,
    max_commands_per_exec: usize,
    max_bytes_per_exec: usize,
    pending: &HashMap<String, PendingMessage>,
    passthrough: &VecDeque<PendingMessage>,
) -> Vec<PendingMessage> {
    if interval_ms > 0 {
        let mut candidates = pending.values().cloned().collect::<Vec<_>>();
        candidates.sort_by(|left, right| left.output_channel.cmp(&right.output_channel));
        let mut batch = Vec::new();
        let mut batch_bytes = 0usize;
        for message in candidates.into_iter().take(max_commands_per_exec) {
            let message_bytes = publish_command_frame_bytes(&message);
            let transaction_bytes = batch_bytes
                .saturating_add(message_bytes)
                .saturating_add(RESP_MULTI_FRAME_BYTES + RESP_EXEC_FRAME_BYTES);
            if !batch.is_empty() && transaction_bytes > max_bytes_per_exec {
                break;
            }
            batch_bytes = batch_bytes.saturating_add(message_bytes);
            batch.push(message);
        }
        batch
    } else {
        passthrough.front().cloned().into_iter().collect()
    }
}

const RESP_MULTI_FRAME_BYTES: usize = 15;
const RESP_EXEC_FRAME_BYTES: usize = 14;
const RESP_COMMAND_ARRAY_HEADER_BYTES: usize = 4; // `*3\r\n`

fn resp_bulk_frame_bytes(argument_len: usize) -> usize {
    1usize
        .saturating_add(decimal_digits(argument_len))
        .saturating_add(2)
        .saturating_add(argument_len)
        .saturating_add(2)
}

fn decimal_digits(mut value: usize) -> usize {
    let mut digits = 1;
    while value >= 10 {
        value /= 10;
        digits += 1;
    }
    digits
}

fn publish_command_frame_bytes(message: &PendingMessage) -> usize {
    publish_command_frame_bytes_for_lengths(message.output_channel.len(), message.payload.len())
}

fn publish_command_frame_bytes_for_lengths(channel_len: usize, payload_len: usize) -> usize {
    RESP_COMMAND_ARRAY_HEADER_BYTES
        .saturating_add(resp_bulk_frame_bytes(b"PUBLISH".len()))
        .saturating_add(resp_bulk_frame_bytes(channel_len))
        .saturating_add(resp_bulk_frame_bytes(payload_len))
}

fn publish_operation_frame_bytes(message: &PendingMessage, atomic: bool) -> usize {
    publish_command_frame_bytes(message).saturating_add(if atomic {
        RESP_MULTI_FRAME_BYTES + RESP_EXEC_FRAME_BYTES
    } else {
        0
    })
}

#[cfg(test)]
fn exec_transaction_frame_bytes(messages: &[PendingMessage]) -> usize {
    messages.iter().fold(
        RESP_MULTI_FRAME_BYTES + RESP_EXEC_FRAME_BYTES,
        |total, message| total.saturating_add(publish_command_frame_bytes(message)),
    )
}

fn max_payload_length_for_target(
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
struct OversizedPolicyLog {
    last_report: Option<time::Instant>,
    dropped_messages: u64,
    truncated_messages: u64,
    truncated_payload_bytes: u64,
}

impl OversizedPolicyLog {
    fn report(
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

    fn flush_suppressed(&mut self, output: &str) {
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

fn prepare_output_message(
    message: InboundMessage,
    context: &mut OutputMessageContext<'_>,
) -> Option<PendingMessage> {
    let OutputMessagePolicy {
        interval_ms,
        max_bytes_per_exec,
        oversized_policy,
    } = context.policy;
    let mut pending_message = PendingMessage {
        output_channel: message.output_channel,
        payload: message.payload,
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

fn clear_published_batch(
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
    metrics.set_output_pending_keys(
        output_metrics,
        if interval_ms > 0 {
            pending.len()
        } else {
            passthrough.len()
        },
    );
}

fn settle_failed_batch(
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

#[derive(Clone, Debug, PartialEq, Eq)]
enum PublishFailure {
    NotSent(String),
    Uncertain(String),
}

const FAILURE_LOG_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Default)]
struct OutputFailureLog {
    last_report: Option<time::Instant>,
    suppressed_failures: u64,
    suppressed_messages: u64,
}

impl OutputFailureLog {
    fn report(&mut self, output: &str, error: &str, messages: usize) {
        let now = time::Instant::now();
        if self
            .last_report
            .is_none_or(|last| now.duration_since(last) >= FAILURE_LOG_INTERVAL)
        {
            warn!(
                output,
                error,
                messages,
                suppressed_failures = self.suppressed_failures,
                suppressed_messages = self.suppressed_messages,
                "output_publish_failed"
            );
            self.last_report = Some(now);
            self.suppressed_failures = 0;
            self.suppressed_messages = 0;
        } else {
            self.suppressed_failures = self.suppressed_failures.saturating_add(1);
            self.suppressed_messages = self.suppressed_messages.saturating_add(messages as u64);
        }
    }

    fn flush_suppressed(&mut self, output: &str) {
        if self.suppressed_failures > 0 {
            warn!(
                output,
                suppressed_failures = self.suppressed_failures,
                suppressed_messages = self.suppressed_messages,
                "output_publish_failures_suppressed"
            );
            self.suppressed_failures = 0;
            self.suppressed_messages = 0;
        }
    }
}

trait BatchPublisher {
    async fn publish(
        &mut self,
        messages: &[PendingMessage],
        atomic: bool,
    ) -> std::result::Result<i64, PublishFailure>;
}

struct RedisBatchPublisher<'a> {
    config: &'a OutputConfig,
    client: &'a redis::Client,
    connection: &'a mut Option<MultiplexedConnection>,
}

impl BatchPublisher for RedisBatchPublisher<'_> {
    async fn publish(
        &mut self,
        messages: &[PendingMessage],
        atomic: bool,
    ) -> std::result::Result<i64, PublishFailure> {
        publish_batch(self.config, self.client, self.connection, messages, atomic).await
    }
}

async fn publish_once<P: BatchPublisher>(
    name: &str,
    publisher: &mut P,
    batch: &[PendingMessage],
    atomic: bool,
    failure_log: &mut OutputFailureLog,
    metrics: &Metrics,
    output_metrics: &OutputMetrics,
) -> std::result::Result<i64, PublishFailure> {
    match publisher.publish(batch, atomic).await {
        Ok(subscribers) => Ok(subscribers),
        Err(failure) => {
            match &failure {
                PublishFailure::NotSent(error) => {
                    metrics.record_output_error(output_metrics, error, true);
                    metrics.record_output_error_messages(output_metrics, batch.len());
                    failure_log.report(name, error, batch.len());
                }
                PublishFailure::Uncertain(error) => {
                    metrics.record_output_uncertain(output_metrics, error, batch.len());
                    failure_log.report(name, error, batch.len());
                }
            }
            Err(failure)
        }
    }
}

async fn publish_conflated_pending<P: BatchPublisher>(
    publisher: &mut P,
    pending: &mut HashMap<String, PendingMessage>,
    max_commands_per_exec: usize,
    max_bytes_per_exec: usize,
    context: &mut OutputPublishContext<'_>,
) {
    let mut passthrough = VecDeque::new();
    while !pending.is_empty() {
        let batch = pending_batch(
            1,
            max_commands_per_exec,
            max_bytes_per_exec,
            pending,
            &passthrough,
        );
        let publish_result = publish_once(
            context.name,
            publisher,
            &batch,
            true,
            context.failure_log,
            context.metrics,
            context.output_metrics,
        )
        .await;
        let subscribers = match publish_result {
            Ok(subscribers) => subscribers,
            Err(failure) => {
                settle_failed_batch(
                    1,
                    &batch,
                    pending,
                    &mut passthrough,
                    &failure,
                    context.metrics,
                    context.output_metrics,
                );
                drop(batch);
                tokio::task::yield_now().await;
                continue;
            }
        };
        let message_count = batch.len();
        let payload_bytes = batch.iter().map(|message| message.payload.len()).sum();
        clear_published_batch(
            1,
            pending,
            &mut passthrough,
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
        tokio::task::yield_now().await;
    }
}

async fn publish_passthrough_pending<P: BatchPublisher>(
    name: &str,
    publisher: &mut P,
    pending: &mut HashMap<String, PendingMessage>,
    passthrough: &mut VecDeque<PendingMessage>,
    failure_log: &mut OutputFailureLog,
    metrics: &Metrics,
    output_metrics: &OutputMetrics,
) {
    while !passthrough.is_empty() {
        let batch = pending_batch(0, 1, 0, pending, passthrough);
        match publish_once(
            name,
            publisher,
            &batch,
            false,
            failure_log,
            metrics,
            output_metrics,
        )
        .await
        {
            Ok(subscribers) => {
                let payload_bytes = batch.iter().map(|message| message.payload.len()).sum();
                clear_published_batch(0, pending, passthrough, &batch, metrics, output_metrics);
                metrics.record_output_flush(output_metrics, batch.len(), payload_bytes);
                debug!(
                    output = name,
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
                    metrics,
                    output_metrics,
                );
                drop(batch);
            }
        }
        tokio::task::yield_now().await;
    }
}

async fn publish_output(
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
    } = setup;
    let interval_ms = config.conflation.interval_ms;
    let max_commands_per_exec = config.conflation.max_commands_per_exec;
    let mut flush_interval = if interval_ms > 0 {
        let interval = Duration::from_millis(interval_ms as u64);
        let mut ticker = time::interval_at(time::Instant::now() + interval, interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        Some(ticker)
    } else {
        None
    };
    let mut pending = HashMap::<String, PendingMessage>::new();
    let mut passthrough = VecDeque::<PendingMessage>::new();
    let mut failure_log = OutputFailureLog::default();
    let mut policy_log = OversizedPolicyLog::default();
    let mut connection: Option<MultiplexedConnection> = None;
    let mut input_closed = false;
    let mut flush_due = false;

    metrics.set_output_state(&output_metrics, "running");
    loop {
        let has_pending = if interval_ms > 0 {
            !pending.is_empty()
        } else {
            !passthrough.is_empty()
        };
        if flush_due && has_pending {
            flush_due = false;
            metrics.set_output_state(&output_metrics, "publishing");
            if connection.is_none() {
                metrics.set_output_state(&output_metrics, "connecting");
            }
            if interval_ms > 0 {
                let mut publisher = RedisBatchPublisher {
                    config: &config,
                    client: &client,
                    connection: &mut connection,
                };
                let mut publish_context = OutputPublishContext {
                    name: &name,
                    failure_log: &mut failure_log,
                    metrics: &metrics,
                    output_metrics: &output_metrics,
                };
                publish_conflated_pending(
                    &mut publisher,
                    &mut pending,
                    max_commands_per_exec,
                    max_bytes_per_exec,
                    &mut publish_context,
                )
                .await;
                metrics.set_output_state(&output_metrics, "running");
            } else {
                let mut publisher = RedisBatchPublisher {
                    config: &config,
                    client: &client,
                    connection: &mut connection,
                };
                publish_passthrough_pending(
                    &name,
                    &mut publisher,
                    &mut pending,
                    &mut passthrough,
                    &mut failure_log,
                    &metrics,
                    &output_metrics,
                )
                .await;
                metrics.set_output_state(&output_metrics, "running");
            }
            continue;
        }

        if input_closed && !has_pending {
            break;
        }

        tokio::select! {
            message = receiver.recv(), if !input_closed => {
                match message {
                    Some(message) => {
                        let mut message_context = OutputMessageContext {
                            name: &name,
                            policy: OutputMessagePolicy {
                                interval_ms,
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
                        );
                        if interval_ms <= 0 {
                            flush_due = true;
                        }
                    }
                    None => {
                        input_closed = true;
                        flush_due = true;
                    }
                }
            }
            _ = async {
                if let Some(interval) = flush_interval.as_mut() {
                    interval.tick().await;
                } else {
                    std::future::pending::<()>().await;
                }
            }, if interval_ms > 0 => {
                if has_pending {
                    flush_due = true;
                }
            }
        }
    }
    failure_log.flush_suppressed(&name);
    policy_log.flush_suppressed(&name);
    metrics.set_output_state(&output_metrics, "stopped");
}

async fn publish_batch(
    config: &OutputConfig,
    client: &redis::Client,
    connection: &mut Option<MultiplexedConnection>,
    messages: &[PendingMessage],
    atomic: bool,
) -> std::result::Result<i64, PublishFailure> {
    if messages.is_empty() {
        return Err(PublishFailure::NotSent(
            "output publish batch is empty".to_owned(),
        ));
    }
    if connection.is_none() {
        let connected = match time::timeout(
            config.redis.connect_timeout(),
            client.get_multiplexed_async_connection(),
        )
        .await
        {
            Ok(Ok(connection)) => connection,
            Ok(Err(error)) => {
                return Err(PublishFailure::NotSent(error.to_string()));
            }
            Err(_) => {
                return Err(PublishFailure::NotSent(
                    "timed out connecting to output Redis".to_owned(),
                ));
            }
        };
        *connection = Some(connected);
    }

    let result = time::timeout(config.redis.connect_timeout(), async {
        let connection = connection.as_mut().expect("connection was initialized");
        if atomic {
            let mut pipeline = redis::pipe();
            pipeline.atomic();
            for message in messages {
                pipeline
                    .cmd("PUBLISH")
                    .arg(&message.output_channel)
                    .arg(&message.payload);
            }
            pipeline
                .query_async::<Vec<i64>>(connection)
                .await
                .map(|counts| counts.into_iter().sum::<i64>())
        } else {
            let message = &messages[0];
            redis::cmd("PUBLISH")
                .arg(&message.output_channel)
                .arg(&message.payload)
                .query_async::<i64>(connection)
                .await
        }
    })
    .await;

    match result {
        Ok(Ok(subscribers)) => Ok(subscribers),
        Ok(Err(error)) => {
            *connection = None;
            Err(PublishFailure::Uncertain(format!(
                "Redis returned an error after a publish command was sent: {error}"
            )))
        }
        Err(_) => {
            *connection = None;
            Err(PublishFailure::Uncertain(
                "timed out waiting for the Redis publish response".to_owned(),
            ))
        }
    }
}

async fn write_status(path: std::path::PathBuf, update_interval_ms: u64, metrics: Arc<Metrics>) {
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

#[cfg(test)]
mod tests {
    use super::*;

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
        pending: &mut HashMap<String, PendingMessage>,
        passthrough: &mut VecDeque<PendingMessage>,
        metrics: &Metrics,
        output_metrics: &OutputMetrics,
    ) {
        let mut policy_log = OversizedPolicyLog::default();
        let mut context = OutputMessageContext {
            name: "test-output",
            policy: OutputMessagePolicy {
                interval_ms,
                max_bytes_per_exec: 4 * 1024 * 1024,
                oversized_policy: OversizedMessagePolicy::Send,
            },
            metrics,
            output_metrics,
            policy_log: &mut policy_log,
        };
        enqueue_message(message, pending, passthrough, &mut context);
    }

    fn publish_context_for_test<'a>(
        name: &'a str,
        failure_log: &'a mut OutputFailureLog,
        metrics: &'a Metrics,
        output_metrics: &'a OutputMetrics,
    ) -> OutputPublishContext<'a> {
        OutputPublishContext {
            name,
            failure_log,
            metrics,
            output_metrics,
        }
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

    #[test]
    fn matched_subscription_maps_to_the_output_channel() {
        let subscriptions = vec![Subscription::Psubscribe {
            pattern: "sensor:*".to_owned(),
            output_prefix: "replica:".to_owned(),
            output_suffix: ":copy".to_owned(),
        }];
        let subscription =
            matching_subscription(&subscriptions, "sensor:alpha", Some("sensor:*")).unwrap();
        let (prefix, suffix) = subscription.output_mapping();

        assert_eq!(
            map_output_channel("archive:", prefix, "sensor:alpha", suffix, ":dest"),
            "archive:replica:sensor:alpha:copy:dest"
        );
        assert!(
            matching_subscription(&subscriptions, "sensor:alpha", Some("sensor:other")).is_none()
        );
    }

    #[test]
    fn overlapping_patterns_use_the_pattern_reported_by_redis() {
        let subscriptions = vec![
            Subscription::Psubscribe {
                pattern: "sensor:*".to_owned(),
                output_prefix: "broad:".to_owned(),
                output_suffix: String::new(),
            },
            Subscription::Psubscribe {
                pattern: "sensor:a*".to_owned(),
                output_prefix: "specific:".to_owned(),
                output_suffix: String::new(),
            },
        ];
        let subscription =
            matching_subscription(&subscriptions, "sensor:alpha", Some("sensor:a*")).unwrap();
        let (prefix, suffix) = subscription.output_mapping();

        assert_eq!(
            map_output_channel("", prefix, "sensor:alpha", suffix, ""),
            "specific:sensor:alpha"
        );
    }

    #[test]
    fn different_outputs_compose_their_own_namespace_with_the_subscription_mapping() {
        let subscriptions = vec![Subscription::Subscribe {
            channel: "events".to_owned(),
            output_prefix: "input:".to_owned(),
            output_suffix: ":source".to_owned(),
        }];
        let subscription = matching_subscription(&subscriptions, "events", None).unwrap();
        let (subscription_prefix, subscription_suffix) = subscription.output_mapping();

        assert_eq!(
            map_output_channel(
                "first:",
                subscription_prefix,
                "events",
                subscription_suffix,
                ":a"
            ),
            "first:input:events:source:a"
        );
        assert_eq!(
            map_output_channel(
                "second:",
                subscription_prefix,
                "events",
                subscription_suffix,
                ":b"
            ),
            "second:input:events:source:b"
        );
    }

    #[test]
    fn each_output_conflates_by_mapped_channel_and_preserves_binary_payloads() {
        let metrics = Metrics::new();
        let output_metrics = metrics.register_output("output1");
        let mut pending = HashMap::new();
        let mut passthrough = VecDeque::new();

        for (channel, payload) in [
            ("replica:sensor:a", vec![0, 255]),
            ("replica:sensor:a", b"latest".to_vec()),
            ("replica:sensor:b", b"other".to_vec()),
        ] {
            metrics.record_output_input(&output_metrics, payload.len());
            enqueue_for_test(
                25,
                InboundMessage {
                    output_channel: channel.to_owned(),
                    payload,
                },
                &mut pending,
                &mut passthrough,
                &metrics,
                &output_metrics,
            );
        }

        assert_eq!(pending.len(), 2);
        assert_eq!(pending["replica:sensor:a"].payload, b"latest");
        assert_eq!(pending["replica:sensor:b"].payload, b"other");
        assert_eq!(metrics.conflated_messages_total.load(Ordering::Relaxed), 1);
        assert_eq!(
            metrics
                .conflated_payload_bytes_total
                .load(Ordering::Relaxed),
            2
        );
        let snapshot = metrics.snapshot();
        let output_snapshot = &snapshot.outputs["output1"];
        assert_eq!(output_snapshot.input_messages_total, 3);
        assert_eq!(output_snapshot.input_payload_bytes_total, 13);
        assert_eq!(output_snapshot.conflated_payload_bytes_total, 2);
        assert_eq!(output_snapshot.pending_messages, 2);
        assert_eq!(output_snapshot.pending_payload_bytes, 11);
        assert_eq!(metrics.pending_keys.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn nonpositive_interval_keeps_every_incoming_message_in_order() {
        let metrics = Metrics::new();
        let output_metrics = metrics.register_output("arbitrary-name");
        let mut pending = HashMap::new();
        let mut passthrough = VecDeque::new();

        for payload in [b"first".to_vec(), b"second".to_vec()] {
            metrics.record_output_input(&output_metrics, payload.len());
            enqueue_for_test(
                0,
                InboundMessage {
                    output_channel: "events".to_owned(),
                    payload,
                },
                &mut pending,
                &mut passthrough,
                &metrics,
                &output_metrics,
            );
        }

        assert_eq!(passthrough.len(), 2);
        let batch = pending_batch(0, 256, usize::MAX, &pending, &passthrough);
        assert_eq!(batch[0].payload, b"first");
        clear_published_batch(
            0,
            &mut pending,
            &mut passthrough,
            &batch,
            &metrics,
            &output_metrics,
        );
        assert_eq!(
            pending_batch(0, 256, usize::MAX, &pending, &passthrough)[0].payload,
            b"second"
        );
    }

    #[tokio::test]
    async fn failed_transaction_is_not_replayed_and_later_chunks_continue() {
        let metrics = Metrics::new();
        let output_metrics = metrics.register_output("custom-output");
        let mut pending = HashMap::new();
        let mut passthrough = VecDeque::new();

        for channel in ["events:d", "events:a", "events:e", "events:b", "events:c"] {
            let payload = vec![0, channel.as_bytes()[7]];
            metrics.record_output_input(&output_metrics, payload.len());
            enqueue_for_test(
                10,
                InboundMessage {
                    output_channel: channel.to_owned(),
                    payload,
                },
                &mut pending,
                &mut passthrough,
                &metrics,
                &output_metrics,
            );
        }

        let mut publisher = RecordingPublisher {
            uncertain_failures_remaining: 1,
            ..RecordingPublisher::default()
        };
        let mut failure_log = OutputFailureLog::default();
        let mut publish_context =
            publish_context_for_test("custom-output", &mut failure_log, &metrics, &output_metrics);
        publish_conflated_pending(
            &mut publisher,
            &mut pending,
            2,
            usize::MAX,
            &mut publish_context,
        )
        .await;

        let attempted_channels = publisher
            .attempts
            .iter()
            .map(|(atomic, batch)| {
                assert!(*atomic);
                assert!(batch.len() <= 2);
                batch
                    .iter()
                    .map(|message| message.output_channel.as_str())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            attempted_channels,
            vec![
                vec!["events:a", "events:b"],
                vec!["events:c", "events:d"],
                vec!["events:e"],
            ]
        );
        assert_eq!(publisher.possible_executions.len(), 1);
        assert_eq!(publisher.possible_executions[0].len(), 2);

        let published = publisher
            .successful_batches
            .iter()
            .flatten()
            .map(|message| (message.output_channel.as_str(), message.payload.clone()))
            .collect::<Vec<_>>();
        let expected = ["a", "b", "c", "d", "e"]
            .into_iter()
            .map(|suffix| (format!("events:{suffix}"), vec![0, suffix.as_bytes()[0]]))
            .collect::<Vec<_>>();
        assert_eq!(
            published,
            expected
                .iter()
                .skip(2)
                .map(|(channel, payload)| (channel.as_str(), payload.clone()))
                .collect::<Vec<_>>()
        );
        assert!(pending.is_empty());

        let snapshot = metrics.snapshot();
        let output = &snapshot.outputs["custom-output"];
        assert_eq!(output.output_batches_total, 2);
        assert_eq!(output.output_messages_total, 3);
        assert_eq!(output.publish_errors_total, 1);
        assert_eq!(output.publish_error_messages_total, 2);
        assert_eq!(output.uncertain_transactions_total, 1);
        assert_eq!(output.uncertain_messages_total, 2);
        assert_eq!(output.dropped_messages_total, 0);
        assert_eq!(output.pending_messages, 0);
        assert_eq!(output.pending_payload_bytes, 0);
        assert_eq!(output.pending_keys, 0);
    }

    #[tokio::test]
    async fn definitive_pre_send_chunk_failure_is_counted_as_dropped_and_continues() {
        let metrics = Metrics::new();
        let output_metrics = metrics.register_output("failed-output");
        let mut pending = HashMap::new();
        let mut passthrough = VecDeque::new();
        for suffix in ["a", "b", "c"] {
            let payload = suffix.as_bytes().to_vec();
            metrics.record_output_input(&output_metrics, payload.len());
            enqueue_for_test(
                10,
                InboundMessage {
                    output_channel: format!("events:{suffix}"),
                    payload,
                },
                &mut pending,
                &mut passthrough,
                &metrics,
                &output_metrics,
            );
        }
        let mut publisher = RecordingPublisher {
            not_sent_failures_remaining: 1,
            ..RecordingPublisher::default()
        };
        let mut failure_log = OutputFailureLog::default();
        let mut publish_context =
            publish_context_for_test("failed-output", &mut failure_log, &metrics, &output_metrics);
        publish_conflated_pending(
            &mut publisher,
            &mut pending,
            2,
            usize::MAX,
            &mut publish_context,
        )
        .await;

        assert_eq!(publisher.attempts.len(), 2);
        assert_eq!(publisher.attempts[0].1[0].output_channel, "events:a");
        assert_eq!(publisher.attempts[0].1[1].output_channel, "events:b");
        assert_eq!(publisher.attempts[1].1[0].output_channel, "events:c");
        assert_eq!(publisher.successful_batches.len(), 1);
        let output = &metrics.snapshot().outputs["failed-output"];
        assert_eq!(output.dropped_messages_total, 2);
        assert_eq!(output.dropped_payload_bytes_total, 2);
        assert_eq!(output.publish_errors_total, 1);
        assert_eq!(output.publish_error_messages_total, 2);
        assert_eq!(output.pending_messages, 0);
        assert_eq!(output.pending_payload_bytes, 0);
    }

    #[tokio::test]
    async fn direct_publish_failure_is_not_retried_and_next_item_continues() {
        let metrics = Metrics::new();
        let output_metrics = metrics.register_output("immediate-output");
        let mut pending = HashMap::new();
        let mut passthrough = VecDeque::new();
        for payload in [vec![0, 255], b"later".to_vec()] {
            metrics.record_output_input(&output_metrics, payload.len());
            enqueue_for_test(
                0,
                InboundMessage {
                    output_channel: "events".to_owned(),
                    payload,
                },
                &mut pending,
                &mut passthrough,
                &metrics,
                &output_metrics,
            );
        }
        let mut publisher = RecordingPublisher {
            uncertain_failures_remaining: 1,
            ..RecordingPublisher::default()
        };
        let mut failure_log = OutputFailureLog::default();
        publish_passthrough_pending(
            "immediate-output",
            &mut publisher,
            &mut pending,
            &mut passthrough,
            &mut failure_log,
            &metrics,
            &output_metrics,
        )
        .await;

        assert_eq!(publisher.attempts.len(), 2);
        assert!(publisher.attempts.iter().all(|(atomic, _)| !atomic));
        assert_eq!(publisher.possible_executions.len(), 1);
        assert_eq!(publisher.possible_executions[0][0].payload, [0, 255]);
        assert_eq!(publisher.successful_batches.len(), 1);
        assert_eq!(publisher.successful_batches[0][0].payload, b"later");
        let output = &metrics.snapshot().outputs["immediate-output"];
        assert_eq!(output.uncertain_transactions_total, 1);
        assert_eq!(output.uncertain_messages_total, 1);
        assert_eq!(output.publish_error_messages_total, 1);
        assert_eq!(output.output_messages_total, 1);
        assert_eq!(output.pending_messages, 0);
        assert_eq!(output.pending_payload_bytes, 0);
    }

    #[test]
    fn transaction_byte_limit_splits_at_the_exact_wire_size_boundary() {
        let first = PendingMessage {
            output_channel: "events:a".to_owned(),
            payload: vec![0; 8],
        };
        let second = PendingMessage {
            output_channel: "events:b".to_owned(),
            payload: vec![255; 12],
        };
        let pair = [first.clone(), second.clone()];
        let exact_limit = exec_transaction_frame_bytes(&pair);
        let pending = HashMap::from([
            (first.output_channel.clone(), first.clone()),
            (second.output_channel.clone(), second.clone()),
        ]);

        assert_eq!(publish_command_frame_bytes(&first), 45);
        assert_eq!(
            exact_limit,
            RESP_MULTI_FRAME_BYTES
                + RESP_EXEC_FRAME_BYTES
                + publish_command_frame_bytes(&first)
                + publish_command_frame_bytes(&second)
        );
        assert_eq!(
            pending_batch(10, 2, exact_limit, &pending, &VecDeque::new()),
            pair
        );
        assert_eq!(
            pending_batch(10, 2, exact_limit - 1, &pending, &VecDeque::new()),
            vec![first]
        );
    }

    #[test]
    fn oversized_send_keeps_the_message_as_a_singleton_chunk() {
        let message = PendingMessage {
            output_channel: "events:large".to_owned(),
            payload: vec![0xff; 128],
        };
        let later = PendingMessage {
            output_channel: "events:small".to_owned(),
            payload: b"later".to_vec(),
        };
        let limit = exec_transaction_frame_bytes(std::slice::from_ref(&later));
        assert!(exec_transaction_frame_bytes(std::slice::from_ref(&message)) > limit);
        let pending = HashMap::from([
            (message.output_channel.clone(), message.clone()),
            (later.output_channel.clone(), later.clone()),
        ]);

        assert_eq!(
            pending_batch(10, 256, limit, &pending, &VecDeque::new()),
            vec![message]
        );
    }

    #[test]
    fn truncate_preserves_payload_prefix_and_exact_exec_byte_target() {
        let metrics = Metrics::new();
        let output_metrics = metrics.register_output("truncate-output");
        let payload = vec![0, 255, 1, 254, 2, 253];
        metrics.record_output_input(&output_metrics, payload.len());
        let expected_message = PendingMessage {
            output_channel: "events:binary".to_owned(),
            payload: payload[..3].to_vec(),
        };
        let max_bytes = publish_operation_frame_bytes(&expected_message, true);
        let mut policy_log = OversizedPolicyLog::default();

        let prepared = prepare_for_test(
            "truncate-output",
            OutputMessagePolicy {
                interval_ms: 10,
                max_bytes_per_exec: max_bytes,
                oversized_policy: OversizedMessagePolicy::Truncate,
            },
            InboundMessage {
                output_channel: "events:binary".to_owned(),
                payload,
            },
            &metrics,
            &output_metrics,
            &mut policy_log,
        )
        .unwrap();

        assert_eq!(prepared, expected_message);
        assert_eq!(publish_operation_frame_bytes(&prepared, true), max_bytes);
        let output = &metrics.snapshot().outputs["truncate-output"];
        assert_eq!(output.truncated_messages_total, 1);
        assert_eq!(output.truncated_payload_bytes_total, 3);
        assert_eq!(output.pending_messages, 1);
        assert_eq!(output.pending_payload_bytes, 3);
    }

    #[test]
    fn truncate_drops_when_even_an_empty_publish_frame_exceeds_the_target() {
        let metrics = Metrics::new();
        let output_metrics = metrics.register_output("too-small-output");
        metrics.record_output_input(&output_metrics, 3);
        let channel = "events:long-channel";
        let max_bytes = publish_operation_frame_bytes(
            &PendingMessage {
                output_channel: channel.to_owned(),
                payload: Vec::new(),
            },
            true,
        ) - 1;
        let mut policy_log = OversizedPolicyLog::default();

        assert!(
            prepare_for_test(
                "too-small-output",
                OutputMessagePolicy {
                    interval_ms: 10,
                    max_bytes_per_exec: max_bytes,
                    oversized_policy: OversizedMessagePolicy::Truncate,
                },
                InboundMessage {
                    output_channel: channel.to_owned(),
                    payload: b"abc".to_vec(),
                },
                &metrics,
                &output_metrics,
                &mut policy_log,
            )
            .is_none()
        );
        let output = &metrics.snapshot().outputs["too-small-output"];
        assert_eq!(output.dropped_messages_total, 1);
        assert_eq!(output.pending_messages, 0);
        assert_eq!(output.pending_payload_bytes, 0);
    }

    #[test]
    fn direct_truncation_uses_publishes_frame_without_transaction_wrappers() {
        let metrics = Metrics::new();
        let output_metrics = metrics.register_output("direct-truncate-output");
        let channel = "events:direct";
        let payload = vec![0, 255, 1, 254, 2, 253];
        metrics.record_output_input(&output_metrics, payload.len());
        let max_bytes = publish_command_frame_bytes_for_lengths(channel.len(), 2);
        let mut policy_log = OversizedPolicyLog::default();

        let prepared = prepare_for_test(
            "direct-truncate-output",
            OutputMessagePolicy {
                interval_ms: 0,
                max_bytes_per_exec: max_bytes,
                oversized_policy: OversizedMessagePolicy::Truncate,
            },
            InboundMessage {
                output_channel: channel.to_owned(),
                payload,
            },
            &metrics,
            &output_metrics,
            &mut policy_log,
        )
        .unwrap();

        assert_eq!(prepared.payload, [0, 255]);
        assert_eq!(publish_command_frame_bytes(&prepared), max_bytes);
        assert_eq!(
            publish_operation_frame_bytes(&prepared, true),
            max_bytes + 29
        );
    }

    #[test]
    fn drop_policy_skips_one_oversized_message_and_counts_it() {
        let metrics = Metrics::new();
        let output_metrics = metrics.register_output("drop-output");
        let payload = vec![0xff; 10];
        metrics.record_output_input(&output_metrics, payload.len());
        let mut policy_log = OversizedPolicyLog::default();

        assert!(
            prepare_for_test(
                "drop-output",
                OutputMessagePolicy {
                    interval_ms: 0,
                    max_bytes_per_exec: 1,
                    oversized_policy: OversizedMessagePolicy::Drop,
                },
                InboundMessage {
                    output_channel: "events:too-large".to_owned(),
                    payload,
                },
                &metrics,
                &output_metrics,
                &mut policy_log,
            )
            .is_none()
        );
        let output = &metrics.snapshot().outputs["drop-output"];
        assert_eq!(output.dropped_messages_total, 1);
        assert_eq!(output.pending_messages, 0);
        assert_eq!(output.pending_payload_bytes, 0);
    }

    #[test]
    fn fan_out_counts_inputs_per_output_without_multiplying_global_input() {
        let metrics = Metrics::new();
        let first_metrics = metrics.register_output("first");
        let second_metrics = metrics.register_output("second");
        let (first_sender, mut first_receiver) = mpsc::unbounded_channel();
        let (second_sender, mut second_receiver) = mpsc::unbounded_channel();
        let unavailable_metrics = metrics.register_output("unavailable");
        let (unavailable_sender, unavailable_receiver) = mpsc::unbounded_channel();
        drop(unavailable_receiver);
        let senders = [
            OutputSender {
                name: "first".to_owned(),
                channel_prefix: "first:".to_owned(),
                channel_suffix: String::new(),
                sender: first_sender,
                output_metrics: Arc::clone(&first_metrics),
            },
            OutputSender {
                name: "second".to_owned(),
                channel_prefix: "second:".to_owned(),
                channel_suffix: String::new(),
                sender: second_sender,
                output_metrics: Arc::clone(&second_metrics),
            },
            OutputSender {
                name: "unavailable".to_owned(),
                channel_prefix: "unavailable:".to_owned(),
                channel_suffix: String::new(),
                sender: unavailable_sender,
                output_metrics: Arc::clone(&unavailable_metrics),
            },
        ];
        metrics.record_input(3);

        fan_out(&senders, "", "events", "", b"abc", &metrics);

        assert_eq!(metrics.input_messages_total.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.input_payload_bytes_total.load(Ordering::Relaxed), 3);
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.outputs["first"].input_messages_total, 1);
        assert_eq!(snapshot.outputs["second"].input_messages_total, 1);
        assert_eq!(snapshot.outputs["first"].input_payload_bytes_total, 3);
        assert_eq!(snapshot.outputs["second"].input_payload_bytes_total, 3);
        assert_eq!(snapshot.outputs["unavailable"].input_messages_total, 0);
        assert_eq!(snapshot.outputs["unavailable"].pending_messages, 0);
        assert_eq!(snapshot.outputs["unavailable"].pending_payload_bytes, 0);
        assert_eq!(first_receiver.try_recv().unwrap().payload, b"abc");
        assert_eq!(second_receiver.try_recv().unwrap().payload, b"abc");
    }

    #[test]
    fn echo_filter_uses_subscription_prefix_or_suffix_namespaces() {
        let filter = OutputChannelFilter {
            prefix: "replica:".to_owned(),
            suffix: ":copy".to_owned(),
        };

        assert!(filter.matches("replica:sensor:a"));
        assert!(filter.matches("sensor:a:copy"));
        assert!(!filter.matches("sensor:a"));
    }

    #[test]
    fn echo_filters_use_the_combined_namespace_for_each_same_server_output() {
        let config: AppConfig = serde_json::from_str(
            r#"{
                "input":{"redis":{"host":"localhost"},"subscriptions":[{"type":"psubscribe","pattern":"sensor:*","output_prefix":"sub:","output_suffix":":source"}]},
                "outputs":{
                    "one":{"redis":{"host":"localhost"},"channel_prefix":"out:","channel_suffix":":dest","conflation":{"interval_ms":10}},
                    "two":{"redis":{"host":"other.local"},"channel_prefix":"archive:","channel_suffix":":copy","conflation":{"interval_ms":10}}
                },
                "instance_lock":{"path":"lock"}
            }"#,
        )
        .unwrap();
        config.validate().unwrap();

        let filters = output_echo_filters(&config.input, &config.outputs);

        assert_eq!(filters.len(), 1);
        assert!(filters[0].matches("out:sub:sensor:a:source:dest"));
        assert!(!filters[0].matches("archive:sub:sensor:a:source:copy"));
    }

    #[test]
    fn output_batch_is_deterministic_and_retains_one_payload_per_channel() {
        let pending = HashMap::from([
            (
                "replica:b".to_owned(),
                PendingMessage {
                    output_channel: "replica:b".to_owned(),
                    payload: vec![0, 255],
                },
            ),
            (
                "replica:a".to_owned(),
                PendingMessage {
                    output_channel: "replica:a".to_owned(),
                    payload: b"value".to_vec(),
                },
            ),
        ]);
        let batch = pending_batch(10, 256, usize::MAX, &pending, &VecDeque::new());

        assert_eq!(batch.len(), 2);
        assert_eq!(batch[0].output_channel, "replica:a");
        assert_eq!(batch[1].payload, [0, 255]);
    }
}
