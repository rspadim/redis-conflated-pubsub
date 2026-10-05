use std::{
    collections::HashMap,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use anyhow::{Context, Result, anyhow};
use futures_util::StreamExt;
use redis::aio::MultiplexedConnection;
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot},
    time::{self, MissedTickBehavior},
};
use tracing::{error, info, warn};

use crate::{
    config::{AppConfig, HttpStatusConfig, RedisConfig, Subscription},
    http_status, logging,
    status::{self, Metrics},
};

const INPUT_QUEUE_CAPACITY: usize = 10_000;
const PUBLISH_QUEUE_CAPACITY: usize = 1;
const MAX_RETRY_DELAY: Duration = Duration::from_secs(30);

struct InboundMessage {
    channel: String,
    payload: Vec<u8>,
    _byte_permits: Vec<OwnedSemaphorePermit>,
}

#[derive(Clone, Debug)]
struct PendingMessage {
    sequence: u64,
    channel: String,
    payload: Vec<u8>,
}

struct PublishRequest {
    pending: HashMap<String, PendingMessage>,
    atomic: bool,
    response: oneshot::Sender<PublishResponse>,
}

struct PublishResponse {
    pending: HashMap<String, PendingMessage>,
    result: Result<i64, String>,
}

#[derive(Clone)]
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
    let output_client = config.output.redis.client()?;
    let http_listener = bind_status_http(config.status.http.as_ref()).await?;
    let input_config = config.input.clone();
    let same_pubsub_server = config
        .input
        .redis
        .host
        .eq_ignore_ascii_case(&config.output.redis.host)
        && config.input.redis.port == config.output.redis.port
        && config.input.redis.tls == config.output.redis.tls;
    let output_channel_filter = same_pubsub_server.then(|| OutputChannelFilter {
        prefix: config.output.channel_prefix.clone(),
        suffix: config.output.channel_suffix.clone(),
    });
    let (input_tx, mut input_rx) = mpsc::channel(INPUT_QUEUE_CAPACITY);
    let input_byte_semaphore = Arc::new(Semaphore::new(
        config
            .conflation
            .max_pending_bytes
            .min(Semaphore::MAX_PERMITS),
    ));
    let (publish_tx, publish_rx) = mpsc::channel(PUBLISH_QUEUE_CAPACITY);
    metrics.set_state("running");

    let input_max_message_bytes = config.conflation.max_message_bytes;
    let input_byte_semaphore = Arc::clone(&input_byte_semaphore);
    let input_metrics = Arc::clone(&metrics);
    let input_task = tokio::spawn(async move {
        read_input(
            input_config,
            input_client,
            input_tx,
            input_byte_semaphore,
            input_max_message_bytes,
            output_channel_filter,
            input_metrics,
        )
        .await;
    });

    let output_config = config.output.clone();
    let output_metrics = Arc::clone(&metrics);
    let output_task = tokio::spawn(async move {
        publish_output(output_config, output_client, publish_rx, output_metrics).await;
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

    let mut pending = HashMap::<String, PendingMessage>::new();
    let mut pending_bytes = 0_usize;
    let mut next_sequence = 0_u64;
    let mut next_flush = if config.conflation.interval_ms > 0 {
        let flush_interval = Duration::from_millis(config.conflation.interval_ms as u64);
        let mut interval = time::interval_at(time::Instant::now() + flush_interval, flush_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        Some(interval)
    } else {
        None
    };
    let mut publish_retry_at = None;
    let mut publish_retry_delay = Duration::from_millis(250);
    let mut in_flight: Option<oneshot::Receiver<PublishResponse>> = None;
    let mut shutdown = Box::pin(wait_for_shutdown_signal());

    info!(
        interval_ms = config.conflation.interval_ms,
        output_channel_prefix = %config.output.channel_prefix,
        output_channel_suffix = %config.output.channel_suffix,
        "service_ready"
    );

    loop {
        tokio::select! {
            signal = &mut shutdown => {
                signal.context("failed to listen for shutdown signal")?;
                info!("shutdown_signal_received");
                break;
            }
            message = input_rx.recv() => {
                let Some(message) = message else {
                    return Err(anyhow!("input message stream stopped"));
                };
                accept_input_message(
                    message,
                    &mut pending,
                    &mut pending_bytes,
                    &mut next_sequence,
                    InputMessageContext {
                        config: &config,
                        publish_sender: &publish_tx,
                        in_flight: &mut in_flight,
                        publish_retry_at: &mut publish_retry_at,
                        metrics: &metrics,
                    },
                );
                if config.conflation.interval_ms <= 0
                    && in_flight.is_none()
                    && publish_retry_at.is_none()
                    && !pending.is_empty()
                {
                    in_flight = start_publish(
                        &mut pending,
                        &mut pending_bytes,
                        &config,
                        &publish_tx,
                        &metrics,
                    );
                    if in_flight.is_none() && !pending.is_empty() {
                        schedule_publish_retry(&mut publish_retry_at, &mut publish_retry_delay);
                    }
                    metrics.pending_keys.store(pending.len() as u64, Ordering::Relaxed);
                }
            }
            _ = async {
                if let Some(interval) = next_flush.as_mut() {
                    interval.tick().await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => {
                if in_flight.is_none() && !pending.is_empty() {
                    in_flight = start_publish(
                        &mut pending,
                        &mut pending_bytes,
                        &config,
                        &publish_tx,
                        &metrics,
                    );
                    if in_flight.is_none() && !pending.is_empty() {
                        schedule_publish_retry(&mut publish_retry_at, &mut publish_retry_delay);
                    }
                    metrics.pending_keys.store(pending.len() as u64, Ordering::Relaxed);
                }
            }
            _ = async move {
                if let Some(retry_at) = publish_retry_at {
                    time::sleep_until(retry_at).await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => {
                publish_retry_at = None;
                if in_flight.is_none() && !pending.is_empty() {
                    in_flight = start_publish(
                        &mut pending,
                        &mut pending_bytes,
                        &config,
                        &publish_tx,
                        &metrics,
                    );
                    if in_flight.is_none() && !pending.is_empty() {
                        schedule_publish_retry(&mut publish_retry_at, &mut publish_retry_delay);
                    }
                    metrics.pending_keys.store(pending.len() as u64, Ordering::Relaxed);
                }
            }
            result = receive_publish_result(&mut in_flight), if in_flight.is_some() => {
                if let Some(response) = result {
                    let succeeded = response.result.is_ok();
                    handle_publish_response(
                        response,
                        &mut pending,
                        &mut pending_bytes,
                        &config,
                        &metrics,
                    );
                    if succeeded {
                        publish_retry_at = None;
                        publish_retry_delay = Duration::from_millis(250);
                        if config.conflation.interval_ms <= 0
                            && in_flight.is_none()
                            && !pending.is_empty()
                        {
                            in_flight = start_publish(
                                &mut pending,
                                &mut pending_bytes,
                                &config,
                                &publish_tx,
                                &metrics,
                            );
                            if in_flight.is_none() && !pending.is_empty() {
                                schedule_publish_retry(
                                    &mut publish_retry_at,
                                    &mut publish_retry_delay,
                                );
                            }
                        }
                    } else if !pending.is_empty() {
                        schedule_publish_retry(&mut publish_retry_at, &mut publish_retry_delay);
                    }
                }
            }
        }
    }

    input_task.abort();
    let _ = input_task.await;
    while let Some(message) = input_rx.recv().await {
        accept_input_message(
            message,
            &mut pending,
            &mut pending_bytes,
            &mut next_sequence,
            InputMessageContext {
                config: &config,
                publish_sender: &publish_tx,
                in_flight: &mut in_flight,
                publish_retry_at: &mut publish_retry_at,
                metrics: &metrics,
            },
        );
    }

    let mut shutdown_publish_failed = false;
    if in_flight.is_some() {
        if let Some(response) = receive_publish_result(&mut in_flight).await {
            shutdown_publish_failed = response.result.is_err();
            handle_publish_response(
                response,
                &mut pending,
                &mut pending_bytes,
                &config,
                &metrics,
            );
        } else {
            metrics.publish_errors_total.fetch_add(1, Ordering::Relaxed);
            metrics.record_error("publish response was dropped during shutdown");
            warn!("publish_response_lost_during_shutdown");
            shutdown_publish_failed = true;
        }
    }

    if config.conflation.interval_ms <= 0 {
        if shutdown_publish_failed {
            let count = pending.len();
            metrics
                .dropped_messages_total
                .fetch_add(count as u64, Ordering::Relaxed);
            pending.clear();
        }

        while !pending.is_empty() {
            let pending_before = pending.len();
            in_flight = start_publish(
                &mut pending,
                &mut pending_bytes,
                &config,
                &publish_tx,
                &metrics,
            );
            if in_flight.is_none() {
                if pending.len() == pending_before {
                    let count = pending.len();
                    metrics
                        .dropped_messages_total
                        .fetch_add(count as u64, Ordering::Relaxed);
                    pending.clear();
                    warn!(messages = count, "final_message_not_enqueued");
                }
                continue;
            }

            match receive_publish_result(&mut in_flight).await {
                Some(response) if response.result.is_ok() => {
                    let count = response.pending.len();
                    metrics.record_flush(count);
                    info!(messages = count, "final_message_published");
                }
                Some(response) => {
                    let count = response.pending.len() + pending.len();
                    metrics.publish_errors_total.fetch_add(1, Ordering::Relaxed);
                    metrics
                        .dropped_messages_total
                        .fetch_add(count as u64, Ordering::Relaxed);
                    metrics.record_error("final passthrough publish failed during shutdown");
                    warn!(messages = count, "final_passthrough_publish_failed");
                    pending.clear();
                }
                None => {
                    let count = pending_before;
                    metrics.publish_errors_total.fetch_add(1, Ordering::Relaxed);
                    metrics
                        .dropped_messages_total
                        .fetch_add(count as u64, Ordering::Relaxed);
                    metrics.record_error("final passthrough publish response was dropped");
                    warn!(messages = count, "final_passthrough_response_lost");
                    pending.clear();
                }
            }
        }
    } else if !pending.is_empty() {
        let final_batch_count = pending.len();
        in_flight = start_publish(
            &mut pending,
            &mut pending_bytes,
            &config,
            &publish_tx,
            &metrics,
        );
        metrics
            .pending_keys
            .store(pending.len() as u64, Ordering::Relaxed);

        if in_flight.is_some() {
            match receive_publish_result(&mut in_flight).await {
                Some(response) => match response.result {
                    Ok(subscriber_count) => {
                        let count = response.pending.len();
                        metrics.record_flush(count);
                        info!(
                            messages = count,
                            subscribers = subscriber_count,
                            "final_atomic_flush_published"
                        );
                    }
                    Err(error) => {
                        let count = response.pending.len();
                        metrics.publish_errors_total.fetch_add(1, Ordering::Relaxed);
                        metrics
                            .dropped_messages_total
                            .fetch_add(count as u64, Ordering::Relaxed);
                        metrics.record_error(&error);
                        warn!(error = %error, messages = count, "final_atomic_flush_failed");
                    }
                },
                None => {
                    metrics.publish_errors_total.fetch_add(1, Ordering::Relaxed);
                    metrics
                        .dropped_messages_total
                        .fetch_add(final_batch_count as u64, Ordering::Relaxed);
                    metrics.record_error("final atomic flush response was dropped during shutdown");
                    warn!(
                        messages = final_batch_count,
                        "final_atomic_flush_response_lost"
                    );
                }
            }
        } else if !pending.is_empty() {
            let count = pending.len();
            metrics
                .dropped_messages_total
                .fetch_add(count as u64, Ordering::Relaxed);
            warn!(messages = count, "final_atomic_flush_not_enqueued");
            pending.clear();
        }
        metrics
            .pending_keys
            .store(pending.len() as u64, Ordering::Relaxed);
    }

    metrics.set_state("stopped");
    if let Some(path) = &config.status.path {
        let _ = status::write_atomic(path, &metrics.snapshot());
    }
    drop(publish_tx);
    let _ = output_task.await;
    if let Some(status_task) = status_task {
        status_task.abort();
    }
    if let Some(http_task) = http_task {
        http_task.abort();
    }
    cleanup_task.abort();
    Ok(())
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

fn try_acquire_message_bytes(
    semaphore: &Arc<Semaphore>,
    byte_count: usize,
) -> Option<Vec<OwnedSemaphorePermit>> {
    if byte_count > semaphore.available_permits() {
        return None;
    }

    let mut remaining = byte_count;
    let mut permits = Vec::new();
    while remaining > 0 {
        let chunk = remaining.min(u32::MAX as usize) as u32;
        permits.push(Arc::clone(semaphore).try_acquire_many_owned(chunk).ok()?);
        remaining -= chunk as usize;
    }
    Some(permits)
}

fn schedule_publish_retry(retry_at: &mut Option<time::Instant>, retry_delay: &mut Duration) {
    *retry_at = Some(time::Instant::now() + *retry_delay);
    *retry_delay = (*retry_delay * 2).min(MAX_RETRY_DELAY);
}

async fn read_input(
    config: crate::config::InputConfig,
    client: redis::Client,
    sender: mpsc::Sender<InboundMessage>,
    byte_semaphore: Arc<Semaphore>,
    max_message_bytes: usize,
    output_channel_filter: Option<OutputChannelFilter>,
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
                    let channel = message.get_channel_name().to_owned();
                    if output_channel_filter
                        .as_ref()
                        .is_some_and(|filter| filter.matches(&channel))
                    {
                        metrics
                            .excluded_messages_total
                            .fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    metrics.record_input();

                    let payload = message.get_payload_bytes();
                    if payload.len() > max_message_bytes {
                        metrics
                            .dropped_messages_total
                            .fetch_add(1, Ordering::Relaxed);
                        warn!(
                            channel = %channel,
                            payload_bytes = payload.len(),
                            limit_bytes = max_message_bytes,
                            "input_message_too_large"
                        );
                        continue;
                    }

                    let queue_slot = match sender.reserve().await {
                        Ok(queue_slot) => queue_slot,
                        Err(_) => return,
                    };
                    let message_bytes = channel.len().saturating_add(payload.len());
                    let Some(byte_permits) =
                        try_acquire_message_bytes(&byte_semaphore, message_bytes)
                    else {
                        metrics
                            .dropped_messages_total
                            .fetch_add(1, Ordering::Relaxed);
                        warn!(
                            channel = %channel,
                            payload_bytes = payload.len(),
                            available_bytes = byte_semaphore.available_permits(),
                            "input_queue_byte_budget_reached"
                        );
                        continue;
                    };

                    let inbound = InboundMessage {
                        channel,
                        payload: payload.to_vec(),
                        _byte_permits: byte_permits,
                    };
                    queue_slot.send(inbound);
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

struct InputMessageContext<'a> {
    config: &'a AppConfig,
    publish_sender: &'a mpsc::Sender<PublishRequest>,
    in_flight: &'a mut Option<oneshot::Receiver<PublishResponse>>,
    publish_retry_at: &'a mut Option<time::Instant>,
    metrics: &'a Metrics,
}

fn accept_input_message(
    message: InboundMessage,
    pending: &mut HashMap<String, PendingMessage>,
    pending_bytes: &mut usize,
    next_sequence: &mut u64,
    context: InputMessageContext<'_>,
) {
    let InputMessageContext {
        config,
        publish_sender,
        in_flight,
        publish_retry_at,
        metrics,
    } = context;
    let InboundMessage {
        channel,
        payload,
        _byte_permits,
    } = message;
    drop(_byte_permits);
    *next_sequence = next_sequence.wrapping_add(1);

    let pending_key = if config.conflation.interval_ms <= 0 {
        format!("passthrough:{}", *next_sequence)
    } else {
        channel.clone()
    };
    let is_new_key = !pending.contains_key(&pending_key);
    let old_bytes = pending
        .get(&pending_key)
        .map_or(0, |value| pending_entry_bytes(&pending_key, value));
    let new_bytes = pending_entry_memory_bytes(&pending_key, &channel, payload.len());
    let would_exceed_keys = is_new_key && pending.len() >= config.conflation.max_pending_keys;
    let would_exceed_bytes = pending_bytes
        .saturating_sub(old_bytes)
        .saturating_add(new_bytes)
        > config.conflation.max_pending_bytes;

    if would_exceed_keys || would_exceed_bytes {
        if in_flight.is_none()
            && !pending.is_empty()
            && (config.conflation.interval_ms > 0 || publish_retry_at.is_none())
            && let Some(receiver) =
                start_publish(pending, pending_bytes, config, publish_sender, metrics)
        {
            *in_flight = Some(receiver);
        }

        let still_exceeds_keys = is_new_key && pending.len() >= config.conflation.max_pending_keys;
        let still_exceeds_bytes =
            pending_bytes.saturating_add(new_bytes) > config.conflation.max_pending_bytes;
        if in_flight.is_some() && (still_exceeds_keys || still_exceeds_bytes) {
            metrics
                .dropped_messages_total
                .fetch_add(1, Ordering::Relaxed);
            warn!(channel = %channel, "pending_capacity_reached");
            return;
        }
        if still_exceeds_keys || still_exceeds_bytes {
            metrics
                .dropped_messages_total
                .fetch_add(1, Ordering::Relaxed);
            warn!(channel = %channel, "pending_capacity_reached");
            return;
        }
    }

    if pending
        .insert(
            pending_key,
            PendingMessage {
                sequence: *next_sequence,
                channel,
                payload,
            },
        )
        .is_some()
    {
        *pending_bytes = pending_bytes.saturating_sub(old_bytes);
        metrics
            .conflated_messages_total
            .fetch_add(1, Ordering::Relaxed);
    }
    *pending_bytes = pending_bytes.saturating_add(new_bytes);
    metrics
        .pending_keys
        .store(pending.len() as u64, Ordering::Relaxed);
}

fn handle_publish_response(
    response: PublishResponse,
    pending: &mut HashMap<String, PendingMessage>,
    pending_bytes: &mut usize,
    config: &AppConfig,
    metrics: &Metrics,
) {
    match response.result {
        Ok(subscriber_count) => {
            let count = response.pending.len();
            metrics.record_flush(count);
            info!(
                messages = count,
                subscribers = subscriber_count,
                "batch_published"
            );
        }
        Err(error) => {
            metrics.publish_errors_total.fetch_add(1, Ordering::Relaxed);
            metrics.record_error(&error);
            warn!(error = %error, "batch_publish_failed");
            merge_pending(pending, pending_bytes, response.pending, config, metrics);
        }
    }
    metrics
        .pending_keys
        .store(pending.len() as u64, Ordering::Relaxed);
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
            Subscription::Subscribe { channel } => {
                time::timeout(
                    redis_config.connect_timeout(),
                    pubsub.subscribe(channel.as_str()),
                )
                .await
                .context("timed out subscribing to input channel")??;
            }
            Subscription::Psubscribe { pattern } => {
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

async fn publish_output(
    config: crate::config::OutputConfig,
    client: redis::Client,
    mut receiver: mpsc::Receiver<PublishRequest>,
    metrics: Arc<Metrics>,
) {
    let mut connection: Option<MultiplexedConnection> = None;
    while let Some(request) = receiver.recv().await {
        if connection.is_none() {
            match time::timeout(
                config.redis.connect_timeout(),
                client.get_multiplexed_async_connection(),
            )
            .await
            {
                Ok(Ok(connected)) => {
                    connection = Some(connected);
                    info!("output_connected");
                }
                Ok(Err(error)) => {
                    metrics
                        .output_reconnects_total
                        .fetch_add(1, Ordering::Relaxed);
                    let _ = request.response.send(PublishResponse {
                        pending: request.pending,
                        result: Err(error.to_string()),
                    });
                    continue;
                }
                Err(_) => {
                    metrics
                        .output_reconnects_total
                        .fetch_add(1, Ordering::Relaxed);
                    let _ = request.response.send(PublishResponse {
                        pending: request.pending,
                        result: Err("timed out connecting to output Redis".to_owned()),
                    });
                    continue;
                }
            }
        }

        let mut ordered: Vec<_> = request.pending.values().collect();
        ordered.sort_by_key(|message| message.sequence);
        let result = time::timeout(config.redis.connect_timeout(), async {
            let connection = connection.as_mut().expect("connection was initialized");
            if request.atomic {
                let mut pipeline = redis::pipe();
                pipeline.atomic();
                for message in ordered {
                    let channel = output_channel(&config, &message.channel);
                    pipeline.cmd("PUBLISH").arg(channel).arg(&message.payload);
                }
                pipeline
                    .query_async::<Vec<i64>>(connection)
                    .await
                    .map(|counts| counts.into_iter().sum::<i64>())
            } else {
                let message = ordered
                    .first()
                    .expect("immediate publish request contains one message");
                let channel = output_channel(&config, &message.channel);
                redis::cmd("PUBLISH")
                    .arg(channel)
                    .arg(&message.payload)
                    .query_async::<i64>(connection)
                    .await
            }
        })
        .await
        .map_err(|_| "timed out publishing output messages".to_owned())
        .and_then(|result| result.map_err(|error| error.to_string()));

        if result.is_err() {
            connection = None;
            metrics
                .output_reconnects_total
                .fetch_add(1, Ordering::Relaxed);
        }
        let _ = request.response.send(PublishResponse {
            pending: request.pending,
            result,
        });
    }
}

fn start_publish(
    pending: &mut HashMap<String, PendingMessage>,
    pending_bytes: &mut usize,
    config: &AppConfig,
    sender: &mpsc::Sender<PublishRequest>,
    metrics: &Metrics,
) -> Option<oneshot::Receiver<PublishResponse>> {
    let atomic = config.conflation.interval_ms > 0;
    let batch = if atomic {
        *pending_bytes = 0;
        std::mem::take(pending)
    } else {
        let next_key = pending
            .iter()
            .min_by_key(|(_, message)| message.sequence)
            .map(|(key, _)| key.clone())?;
        let message = pending.remove(&next_key)?;
        *pending_bytes = pending_bytes.saturating_sub(pending_entry_bytes(&next_key, &message));
        HashMap::from([(next_key, message)])
    };
    let response_batch = batch.clone();
    let batch_bytes = batch.values().fold(0_usize, |total, message| {
        total
            .saturating_add(config.output.channel_prefix.len())
            .saturating_add(message.channel.len())
            .saturating_add(config.output.channel_suffix.len())
            .saturating_add(message.payload.len())
            .saturating_add(64)
    });
    if batch_bytes > config.conflation.max_batch_bytes {
        warn!(
            bytes = batch_bytes,
            limit_bytes = config.conflation.max_batch_bytes,
            "output_batch_exceeds_configured_limit"
        );
        metrics
            .dropped_messages_total
            .fetch_add(response_batch.len() as u64, Ordering::Relaxed);
        metrics.record_error("output batch exceeded max_batch_bytes");
        return None;
    }

    let (response_tx, response_rx) = oneshot::channel();
    let request = PublishRequest {
        pending: batch,
        atomic,
        response: response_tx,
    };
    match sender.try_send(request) {
        Ok(()) => Some(response_rx),
        Err(error) => {
            warn!(error = %error, "publish_queue_unavailable");
            let request = error.into_inner();
            *pending_bytes = pending_bytes.saturating_add(
                request
                    .pending
                    .iter()
                    .fold(0_usize, |total, (key, message)| {
                        total.saturating_add(pending_entry_bytes(key, message))
                    }),
            );
            pending.extend(request.pending);
            None
        }
    }
}

fn output_channel(config: &crate::config::OutputConfig, input_channel: &str) -> String {
    format!(
        "{}{}{}",
        config.channel_prefix, input_channel, config.channel_suffix
    )
}

fn pending_entry_memory_bytes(key: &str, channel: &str, payload_bytes: usize) -> usize {
    key.len()
        .saturating_add(channel.len())
        .saturating_add(payload_bytes)
}

fn pending_entry_bytes(key: &str, message: &PendingMessage) -> usize {
    pending_entry_memory_bytes(key, &message.channel, message.payload.len())
}

fn merge_pending(
    current: &mut HashMap<String, PendingMessage>,
    current_bytes: &mut usize,
    failed: HashMap<String, PendingMessage>,
    config: &AppConfig,
    metrics: &Metrics,
) {
    for (key, old_message) in failed {
        if let Some(newer) = current.get(&key)
            && newer.sequence >= old_message.sequence
        {
            continue;
        }
        let previous_bytes = current
            .get(&key)
            .map_or(0, |message| pending_entry_bytes(&key, message));
        let would_exceed_keys =
            !current.contains_key(&key) && current.len() >= config.conflation.max_pending_keys;
        let old_message_bytes = pending_entry_bytes(&key, &old_message);
        let would_exceed_bytes = current_bytes
            .saturating_sub(previous_bytes)
            .saturating_add(old_message_bytes)
            > config.conflation.max_pending_bytes;
        if would_exceed_keys || would_exceed_bytes {
            metrics
                .dropped_messages_total
                .fetch_add(1, Ordering::Relaxed);
            continue;
        }
        *current_bytes = current_bytes.saturating_sub(previous_bytes);
        *current_bytes = current_bytes.saturating_add(old_message_bytes);
        current.insert(key, old_message);
    }
}

async fn receive_publish_result(
    receiver: &mut Option<oneshot::Receiver<PublishResponse>>,
) -> Option<PublishResponse> {
    let active = receiver.as_mut()?;
    let result = active.await.ok();
    *receiver = None;
    result
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

    #[test]
    fn atomic_publish_request_preserves_channel_and_binary_payload() {
        let config = test_config();
        let metrics = Metrics::new();
        let (publisher, mut publisher_rx) = mpsc::channel(1);
        let mut pending = HashMap::from([(
            "sensor:a".to_owned(),
            PendingMessage {
                sequence: 1,
                channel: "sensor:a".to_owned(),
                payload: vec![0, 255, 1],
            },
        )]);
        let mut pending_bytes = 3;

        assert!(
            start_publish(
                &mut pending,
                &mut pending_bytes,
                &config,
                &publisher,
                &metrics,
            )
            .is_some()
        );
        let request = publisher_rx.try_recv().unwrap();

        assert!(request.atomic);
        assert_eq!(request.pending.len(), 1);
        assert_eq!(request.pending["sensor:a"].channel, "sensor:a");
        assert_eq!(request.pending["sensor:a"].payload, [0, 255, 1]);
    }

    #[test]
    fn merge_keeps_the_newer_message_for_a_channel() {
        let mut current = HashMap::from([(
            "sensor:a".to_owned(),
            PendingMessage {
                sequence: 2,
                channel: "sensor:a".to_owned(),
                payload: b"new".to_vec(),
            },
        )]);
        let failed = HashMap::from([(
            "sensor:a".to_owned(),
            PendingMessage {
                sequence: 1,
                channel: "sensor:a".to_owned(),
                payload: b"old".to_vec(),
            },
        )]);
        let config = test_config();
        let metrics = Metrics::new();
        let mut bytes = pending_entry_bytes("sensor:a", current.get("sensor:a").unwrap());

        merge_pending(&mut current, &mut bytes, failed, &config, &metrics);

        assert_eq!(current["sensor:a"].payload, b"new");
        assert_eq!(bytes, pending_entry_bytes("sensor:a", &current["sensor:a"]));
    }

    #[test]
    fn non_positive_interval_preserves_repeated_messages_without_conflation() {
        let mut config = test_config();
        config.conflation.interval_ms = -1;
        let metrics = Metrics::new();
        let (publisher, mut publisher_rx) = mpsc::channel(1);
        let mut pending = HashMap::new();
        let mut pending_bytes = 0;
        let mut sequence = 0;
        let mut in_flight = None;
        let mut publish_retry_at = None;

        for payload in [b"first".to_vec(), b"second".to_vec()] {
            accept_input_message(
                InboundMessage {
                    channel: "test-feed:alpha".to_owned(),
                    payload,
                    _byte_permits: Vec::new(),
                },
                &mut pending,
                &mut pending_bytes,
                &mut sequence,
                InputMessageContext {
                    config: &config,
                    publish_sender: &publisher,
                    in_flight: &mut in_flight,
                    publish_retry_at: &mut publish_retry_at,
                    metrics: &metrics,
                },
            );
        }

        assert_eq!(pending.len(), 2);
        assert_eq!(metrics.conflated_messages_total.load(Ordering::Relaxed), 0);
        assert!(
            start_publish(
                &mut pending,
                &mut pending_bytes,
                &config,
                &publisher,
                &metrics,
            )
            .is_some()
        );
        let first_request = publisher_rx.try_recv().unwrap();
        assert!(!first_request.atomic);
        assert_eq!(first_request.pending.len(), 1);
        assert_eq!(
            first_request.pending.values().next().unwrap().channel,
            "test-feed:alpha"
        );
        assert_eq!(
            first_request.pending.values().next().unwrap().payload,
            b"first"
        );

        assert!(
            start_publish(
                &mut pending,
                &mut pending_bytes,
                &config,
                &publisher,
                &metrics,
            )
            .is_some()
        );
        let second_request = publisher_rx.try_recv().unwrap();
        assert!(!second_request.atomic);
        assert_eq!(second_request.pending.len(), 1);
        assert_eq!(
            second_request.pending.values().next().unwrap().channel,
            "test-feed:alpha"
        );
        assert_eq!(
            second_request.pending.values().next().unwrap().payload,
            b"second"
        );
    }

    #[test]
    fn conflation_keeps_only_the_latest_payload_per_concrete_channel() {
        let config = test_config();
        let metrics = Metrics::new();
        let (publisher, _publisher_rx) = mpsc::channel(1);
        let mut pending = HashMap::new();
        let mut pending_bytes = 0;
        let mut sequence = 0;
        let mut in_flight = None;
        let mut publish_retry_at = None;

        for payload in [vec![0, 255, 1], b"latest".to_vec()] {
            accept_input_message(
                InboundMessage {
                    channel: "sensor:a".to_owned(),
                    payload,
                    _byte_permits: Vec::new(),
                },
                &mut pending,
                &mut pending_bytes,
                &mut sequence,
                InputMessageContext {
                    config: &config,
                    publish_sender: &publisher,
                    in_flight: &mut in_flight,
                    publish_retry_at: &mut publish_retry_at,
                    metrics: &metrics,
                },
            );
        }

        assert_eq!(pending.len(), 1);
        assert_eq!(pending["sensor:a"].channel, "sensor:a");
        assert_eq!(pending["sensor:a"].payload, b"latest");
        assert_eq!(
            pending_bytes,
            pending_entry_bytes("sensor:a", &pending["sensor:a"])
        );
        assert_eq!(metrics.conflated_messages_total.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn pending_memory_budget_accounts_for_channel_and_map_key_bytes() {
        let mut config = test_config();
        config.conflation.max_pending_bytes = 7;
        let metrics = Metrics::new();
        let (publisher, _publisher_rx) = mpsc::channel(1);
        let mut pending = HashMap::new();
        let mut pending_bytes = 0;
        let mut sequence = 0;
        let mut in_flight = None;
        let mut publish_retry_at = None;

        accept_input_message(
            InboundMessage {
                channel: "long-channel".to_owned(),
                payload: Vec::new(),
                _byte_permits: Vec::new(),
            },
            &mut pending,
            &mut pending_bytes,
            &mut sequence,
            InputMessageContext {
                config: &config,
                publish_sender: &publisher,
                in_flight: &mut in_flight,
                publish_retry_at: &mut publish_retry_at,
                metrics: &metrics,
            },
        );

        assert!(pending.is_empty());
        assert_eq!(pending_bytes, 0);
        assert_eq!(metrics.dropped_messages_total.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn output_channels_apply_only_the_configured_prefix_and_suffix() {
        let mut config = test_config();
        assert_eq!(
            output_channel(&config.output, "sensor:a"),
            "replica:sensor:a"
        );

        config.output.channel_prefix.clear();
        config.output.channel_suffix = ":copy".to_owned();
        assert_eq!(output_channel(&config.output, "sensor:a"), "sensor:a:copy");
    }

    #[test]
    fn same_server_filter_excludes_the_transformed_output_namespace() {
        let filter = OutputChannelFilter {
            prefix: "replica:".to_owned(),
            suffix: ":copy".to_owned(),
        };

        assert!(filter.matches("replica:sensor:a"));
        assert!(filter.matches("sensor:a:copy"));
        assert!(!filter.matches("sensor:a"));
    }

    #[test]
    fn zero_interval_uses_passthrough_publish_mode() {
        let mut config = test_config();
        config.conflation.interval_ms = 0;
        let metrics = Metrics::new();
        let (publisher, mut publisher_rx) = mpsc::channel(1);
        let pending = HashMap::from([(
            "passthrough:1".to_owned(),
            PendingMessage {
                sequence: 1,
                channel: "sensor:a".to_owned(),
                payload: vec![0, 255],
            },
        )]);
        let mut pending_bytes = pending_entry_bytes("passthrough:1", &pending["passthrough:1"]);
        let mut pending = pending;

        assert!(
            start_publish(
                &mut pending,
                &mut pending_bytes,
                &config,
                &publisher,
                &metrics,
            )
            .is_some()
        );

        let request = publisher_rx.try_recv().unwrap();
        assert!(!request.atomic);
        assert_eq!(request.pending.len(), 1);
        assert_eq!(request.pending["passthrough:1"].payload, [0, 255]);
    }

    #[test]
    fn publish_retry_backoff_is_bounded() {
        let mut retry_at = None;
        let mut retry_delay = Duration::from_millis(250);

        schedule_publish_retry(&mut retry_at, &mut retry_delay);
        assert!(retry_at.is_some());
        assert_eq!(retry_delay, Duration::from_millis(500));

        retry_delay = MAX_RETRY_DELAY;
        schedule_publish_retry(&mut retry_at, &mut retry_delay);
        assert_eq!(retry_delay, MAX_RETRY_DELAY);
    }

    fn test_config() -> AppConfig {
        serde_json::from_str(
            r#"{
                "input":{"redis":{"host":"localhost"},"subscriptions":[{"type":"subscribe","channel":"x"}]},
                "output":{"redis":{"host":"localhost"},"channel_prefix":"replica:"},
                "instance_lock":{"path":"lock"},
                "status":{"path":"status.json"}
            }"#,
        )
        .unwrap()
    }
}
