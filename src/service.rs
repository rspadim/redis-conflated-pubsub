#[cfg(test)]
use std::collections::VecDeque;
use std::{collections::HashMap, path::Path, sync::Arc, time::Duration};

#[cfg(unix)]
use anyhow::bail;
use anyhow::{Context, Result, anyhow};
use tokio::{
    sync::mpsc,
    time::{self, MissedTickBehavior},
};
use tracing::{error, info, warn};

use crate::{
    config::{AppConfig, HttpStatusConfig, OutputConfig, OversizedMessagePolicy},
    http_status, logging,
    status::{self, Metrics, OutputMetrics},
};

mod batch;
mod channel_cache;
mod dedup;
mod filter;
mod input;
mod output;
mod policy;
mod publish;

use batch::OversizedPolicyLog;
#[cfg(test)]
use batch::{
    RESP_EXEC_FRAME_BYTES, RESP_MULTI_FRAME_BYTES, exec_transaction_frame_bytes,
    passthrough_batch_length, pending_batch, prepare_output_message, publish_command_frame_bytes,
    publish_command_frame_bytes_for_lengths, publish_operation_frame_bytes,
};
use dedup::{DeduplicationCache, deduplication_prune_interval};
use filter::ChannelFilterSet;
#[cfg(test)]
use input::{
    OutputChannelFilter, fan_out, is_sentinel_pubsub_channel, map_output_channel,
    matching_subscription,
};
use input::{output_echo_filters, read_input};
#[cfg(test)]
use output::{
    clear_published_batch, direct_batch_boundary_required, enqueue_message,
    publish_conflated_interval_pending, publish_conflated_pending, publish_passthrough_batch,
    publish_passthrough_pending,
};
use output::{publish_output, write_status};
#[cfg(test)]
use policy::{CompiledChannelPolicies, glob_matches};
#[cfg(test)]
use policy::{advance_due_schedules, flush_schedules, resolve_channel_policy};
use publish::OutputFailureLog;
#[cfg(test)]
use publish::{BatchPublisher, PublishFailure, publish_once};

const MAX_RETRY_DELAY: Duration = Duration::from_secs(30);

#[derive(Debug)]
pub enum RunOutcome {
    Shutdown,
    #[cfg(unix)]
    Reload(Box<AppConfig>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ServiceSignal {
    Shutdown,
    #[cfg(unix)]
    Reload,
}

pub struct ShutdownSignals {
    #[cfg(unix)]
    interrupt: tokio::signal::unix::Signal,
    #[cfg(unix)]
    terminate: tokio::signal::unix::Signal,
    #[cfg(unix)]
    hangup: tokio::signal::unix::Signal,
}

impl ShutdownSignals {
    pub fn new() -> Result<Self> {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};

            Ok(Self {
                interrupt: signal(SignalKind::interrupt())
                    .context("failed to install SIGINT handler")?,
                terminate: signal(SignalKind::terminate())
                    .context("failed to install SIGTERM handler")?,
                hangup: signal(SignalKind::hangup()).context("failed to install SIGHUP handler")?,
            })
        }
        #[cfg(not(unix))]
        {
            Ok(Self {})
        }
    }

    async fn recv(&mut self) -> Result<ServiceSignal> {
        #[cfg(unix)]
        {
            tokio::select! {
                _ = self.interrupt.recv() => Ok(ServiceSignal::Shutdown),
                _ = self.terminate.recv() => Ok(ServiceSignal::Shutdown),
                _ = self.hangup.recv() => Ok(ServiceSignal::Reload),
            }
        }
        #[cfg(not(unix))]
        {
            tokio::signal::ctrl_c()
                .await
                .context("failed to listen for Ctrl-C")?;
            Ok(ServiceSignal::Shutdown)
        }
    }
}

#[derive(Clone, Debug)]
struct InboundMessage {
    output_channel: String,
    payload: Arc<[u8]>,
}

#[derive(Clone, Debug)]
struct QueuedInboundMessage {
    message: InboundMessage,
    enqueued_at: time::Instant,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PendingMessage {
    output_channel: String,
    conflation_interval_ms: i64,
    deduplication_ttl_ms: Option<i64>,
    deduplication_group: Option<String>,
    /// Payload sent to Redis after applying the oversized-message policy.
    payload: Arc<[u8]>,
    /// Retained only when truncate changes `payload`.
    raw_payload: Option<Arc<[u8]>>,
}

impl PendingMessage {
    /// Shared handle to the untruncated payload, avoiding a byte copy.
    fn raw_payload_arc(&self) -> &Arc<[u8]> {
        self.raw_payload.as_ref().unwrap_or(&self.payload)
    }

    fn raw_payload(&self) -> &[u8] {
        self.raw_payload_arc()
    }
}

type PendingByInterval = HashMap<i64, HashMap<String, PendingMessage>>;

fn pending_message_count(pending: &PendingByInterval) -> usize {
    pending.values().map(HashMap::len).sum()
}

struct OutputSender {
    name: String,
    channel_prefix: String,
    channel_suffix: String,
    channel_filter: ChannelFilterSet,
    sender: mpsc::UnboundedSender<QueuedInboundMessage>,
    output_metrics: Arc<OutputMetrics>,
}

struct OutputRuntimeSetup {
    name: String,
    config: OutputConfig,
    max_bytes_per_exec: usize,
    oversized_policy: OversizedMessagePolicy,
    filters_endpoint_enabled: bool,
    channel_policy_cache_max_entries: usize,
}

#[derive(Clone)]
struct OutputMessagePolicy {
    interval_ms: i64,
    deduplication_ttl_ms: Option<i64>,
    deduplication_group: Option<String>,
    max_bytes_per_exec: usize,
    oversized_policy: OversizedMessagePolicy,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ResolvedChannelPolicy {
    interval_ms: i64,
    deduplication_ttl_ms: i64,
    deduplication_group: Option<String>,
}

struct FlushSchedule {
    interval_ms: i64,
    interval: Duration,
    next_tick: time::Instant,
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
    deduplication_cache: &'a mut DeduplicationCache,
    metrics: &'a Metrics,
    output_metrics: &'a OutputMetrics,
}

pub async fn run(
    mut config: AppConfig,
    metrics: Arc<Metrics>,
    config_path: &Path,
    shutdown_signals: &mut ShutdownSignals,
) -> Result<RunOutcome> {
    #[cfg(not(unix))]
    let _ = config_path;
    metrics.clear_channel_cache_inspectors();
    let input_client = config.input.redis.client()?;
    let filters_endpoint_enabled = config
        .status
        .http
        .as_ref()
        .is_some_and(|http| http.filters_endpoint_enabled);
    // This is the one source-channel filter and its private decision cache.
    let input_filter = ChannelFilterSet::compile_with_inspector(
        &config.input.filters,
        config.input.filter_default,
        config.input.filter_cache_max_entries,
        filters_endpoint_enabled.then(|| (&*metrics, "input.filters".to_owned())),
    )?;
    // The compiled selectors now own the runtime representation; discard the
    // deserialized string-selector structs rather than retaining a second copy.
    config.input.filters = Vec::new();
    let http_listener = bind_status_http(config.status.http.as_ref()).await?;
    let global_max_bytes_per_exec = config.max_bytes_per_exec;
    let global_oversized_message_policy = config.oversized_message_policy;
    let output_setups = config
        .outputs
        .iter_mut()
        .map(|(name, output_config)| {
            // Each output gets its own filter instance and decision cache.
            let channel_filter = ChannelFilterSet::compile_with_inspector(
                &output_config.filters,
                output_config.filter_default,
                output_config.filter_cache_max_entries,
                filters_endpoint_enabled.then(|| (&*metrics, format!("outputs.{name}.filters"))),
            )?;
            output_config.filters = Vec::new();
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
                channel_filter,
                output_config.channel_policy_cache_max_entries,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let echo_filters = if config.input.exclude_output_echoes {
        output_echo_filters(&config.input, &config.outputs)
    } else {
        Vec::new()
    };
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
                channel_filter,
                channel_policy_cache_max_entries,
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
                    filters_endpoint_enabled,
                    channel_policy_cache_max_entries,
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
                        channel_filter,
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
            input_filter,
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
        let filters_endpoint_enabled = config
            .status
            .http
            .as_ref()
            .is_some_and(|http| http.filters_endpoint_enabled);
        tokio::spawn(async move {
            if let Err(error) =
                http_status::serve(listener, http_metrics.clone(), filters_endpoint_enabled).await
            {
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
    #[cfg(unix)]
    let run_outcome = loop {
        match shutdown_signals.recv().await? {
            ServiceSignal::Shutdown => break RunOutcome::Shutdown,
            #[cfg(unix)]
            ServiceSignal::Reload => {
                let next_config = match load_reload_config(&config, config_path) {
                    Ok(next_config) => next_config,
                    Err(error) => {
                        warn!(error = %error, "configuration_reload_rejected");
                        continue;
                    }
                };
                info!("configuration_reload_accepted");
                metrics.set_state("reloading");
                break RunOutcome::Reload(Box::new(next_config));
            }
        }
    };
    #[cfg(not(unix))]
    let run_outcome = match shutdown_signals.recv().await? {
        ServiceSignal::Shutdown => RunOutcome::Shutdown,
    };

    match &run_outcome {
        RunOutcome::Shutdown => info!("shutdown_signal_received"),
        #[cfg(unix)]
        RunOutcome::Reload(_) => info!("configuration_reload_draining_outputs"),
    }
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
        let _ = status_task.await;
    }
    if let Some(http_task) = http_task {
        http_task.abort();
        let _ = http_task.await;
    }
    cleanup_task.abort();
    let _ = cleanup_task.await;
    Ok(run_outcome)
}

#[cfg(unix)]
fn reload_process_settings_unchanged(current: &AppConfig, next: &AppConfig) -> bool {
    current.instance_lock.path == next.instance_lock.path
        && current.logging.directory == next.logging.directory
        && current.logging.level == next.logging.level
        && current.logging.prefix == next.logging.prefix
        && current.logging.enabled == next.logging.enabled
        && current.logging.retention_days == next.logging.retention_days
        && current.logging.max_total_size_mb == next.logging.max_total_size_mb
}

#[cfg(unix)]
fn load_reload_config(current: &AppConfig, path: &Path) -> Result<AppConfig> {
    let next = AppConfig::load(path).context("failed to load configuration after SIGHUP")?;
    next.validate()?;
    if !reload_process_settings_unchanged(current, &next) {
        bail!("changes to instance_lock or logging require a full service restart");
    }
    Ok(next)
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

#[cfg(test)]
#[path = "../tests/unit/service.rs"]
mod tests;
