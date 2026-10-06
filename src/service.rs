use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    path::Path,
    sync::{Arc, atomic::Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[cfg(unix)]
use anyhow::bail;
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
        AppConfig, ChannelPolicy, DeduplicationGroup, HttpStatusConfig, InputConfig, OutputConfig,
        OversizedMessagePolicy, RedisConfig, Subscription, same_pubsub_server,
    },
    http_status, logging,
    status::{self, Metrics, OutputMetrics},
};

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
    payload: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PendingMessage {
    output_channel: String,
    conflation_interval_ms: i64,
    deduplication_ttl_ms: Option<i64>,
    deduplication_group: Option<String>,
    /// Payload sent to Redis after applying the oversized-message policy.
    payload: Vec<u8>,
    /// Retained only when truncate changes `payload`.
    raw_payload: Option<Vec<u8>>,
}

impl PendingMessage {
    fn raw_payload(&self) -> &[u8] {
        self.raw_payload.as_deref().unwrap_or(&self.payload)
    }
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

struct CachedPublishedValue {
    raw_payload: Vec<u8>,
    published_at: time::Instant,
    ttl: Duration,
}

#[derive(Clone, Copy)]
struct DeduplicationGroupSettings {
    ttl: Option<Duration>,
    round_ms: u64,
    restart_on_change: bool,
    max_members: usize,
    max_cache_bytes: usize,
}

struct CachedGroupEntry {
    raw_payload: Vec<u8>,
    last_used: time::Instant,
}

struct CachedDeduplicationGroup {
    expires_at: time::Instant,
    entries: HashMap<String, CachedGroupEntry>,
    cache_bytes: usize,
    warned_capacity: bool,
}

struct DeduplicationCache {
    ttl: Option<Duration>,
    entries: HashMap<String, CachedPublishedValue>,
    group_settings: HashMap<String, DeduplicationGroupSettings>,
    groups: HashMap<String, CachedDeduplicationGroup>,
}

impl DeduplicationCache {
    #[cfg(test)]
    fn new(ttl_ms: i64) -> Self {
        Self::with_groups(ttl_ms, &BTreeMap::new())
    }

    fn with_groups(
        ttl_ms: i64,
        deduplication_groups: &BTreeMap<String, DeduplicationGroup>,
    ) -> Self {
        Self {
            ttl: positive_ttl(ttl_ms),
            entries: HashMap::new(),
            group_settings: deduplication_groups
                .iter()
                .map(|(name, group)| {
                    (
                        name.clone(),
                        DeduplicationGroupSettings {
                            ttl: positive_ttl(group.ttl_ms),
                            round_ms: group.round_ms,
                            restart_on_change: group.restart_on_change,
                            max_members: group.max_members,
                            max_cache_bytes: group.max_cache_bytes,
                        },
                    )
                })
                .collect(),
            groups: HashMap::new(),
        }
    }

    fn should_suppress(&mut self, message: &PendingMessage, now: time::Instant) -> bool {
        if let Some(group_name) = message.deduplication_group.as_deref() {
            let Some(settings) = self.group_settings.get(group_name).copied() else {
                return false;
            };
            if settings.ttl.is_none() {
                self.groups.remove(group_name);
                return false;
            }
            if self
                .groups
                .get(group_name)
                .is_some_and(|group| now >= group.expires_at)
            {
                self.groups.remove(group_name);
                return false;
            }
            return self
                .groups
                .get_mut(group_name)
                .and_then(|group| group.entries.get_mut(&message.output_channel))
                .is_some_and(|entry| {
                    entry.last_used = now;
                    entry.raw_payload.as_slice() == message.raw_payload()
                });
        }

        let Some(cached) = self.entries.get(&message.output_channel) else {
            return false;
        };
        if message
            .deduplication_ttl_ms
            .is_some_and(|ttl_ms| ttl_ms <= 0)
            || now.saturating_duration_since(cached.published_at) >= cached.ttl
        {
            self.entries.remove(&message.output_channel);
            return false;
        }
        cached.raw_payload.as_slice() == message.raw_payload()
    }

    fn remember(&mut self, messages: &[PendingMessage], now: time::Instant) {
        self.remember_at(messages, now, epoch_millis());
    }

    fn remember_at(&mut self, messages: &[PendingMessage], now: time::Instant, now_epoch_ms: u128) {
        let mut grouped = HashMap::<String, Vec<&PendingMessage>>::new();
        for message in messages {
            if let Some(group_name) = message.deduplication_group.as_deref() {
                grouped
                    .entry(group_name.to_owned())
                    .or_default()
                    .push(message);
                continue;
            }

            let ttl = match message.deduplication_ttl_ms {
                Some(ttl_ms) => positive_ttl(ttl_ms),
                None => self.ttl,
            };
            let Some(ttl) = ttl.filter(|ttl| !ttl.is_zero()) else {
                self.entries.remove(&message.output_channel);
                continue;
            };
            self.entries.insert(
                message.output_channel.clone(),
                CachedPublishedValue {
                    raw_payload: message.raw_payload().to_vec(),
                    published_at: now,
                    ttl,
                },
            );
        }

        for (group_name, messages) in grouped {
            let Some(settings) = self.group_settings.get(&group_name).copied() else {
                continue;
            };
            let Some(ttl) = settings.ttl else {
                self.groups.remove(&group_name);
                continue;
            };
            if self
                .groups
                .get(&group_name)
                .is_some_and(|group| now >= group.expires_at)
            {
                self.groups.remove(&group_name);
            }

            let should_restart_deadline = settings.restart_on_change
                && self.groups.get(&group_name).is_some_and(|group| {
                    messages.iter().any(|message| {
                        group
                            .entries
                            .get(&message.output_channel)
                            .is_none_or(|previous| {
                                previous.raw_payload.as_slice() != message.raw_payload()
                            })
                    })
                });
            let group =
                self.groups
                    .entry(group_name.clone())
                    .or_insert_with(|| CachedDeduplicationGroup {
                        expires_at: group_expiration(now, now_epoch_ms, ttl, settings.round_ms),
                        entries: HashMap::new(),
                        cache_bytes: 0,
                        warned_capacity: false,
                    });
            if should_restart_deadline {
                group.expires_at = group_expiration(now, now_epoch_ms, ttl, settings.round_ms);
            }
            for message in messages {
                let channel = &message.output_channel;
                let payload = message.raw_payload();
                if let Some(previous) = group.entries.remove(channel) {
                    group.cache_bytes = group
                        .cache_bytes
                        .saturating_sub(group_entry_bytes(channel, &previous.raw_payload));
                }
                let member_bytes = group_entry_bytes(channel, payload);
                if member_bytes > settings.max_cache_bytes {
                    warn_group_cache_capacity(group, &group_name, settings);
                    continue;
                }
                while group.entries.len() >= settings.max_members
                    || group.cache_bytes.saturating_add(member_bytes) > settings.max_cache_bytes
                {
                    let oldest = group
                        .entries
                        .iter()
                        .min_by_key(|(_, entry)| entry.last_used)
                        .map(|(channel, _)| channel.clone());
                    let Some(oldest) = oldest else {
                        break;
                    };
                    if let Some(evicted) = group.entries.remove(&oldest) {
                        group.cache_bytes = group
                            .cache_bytes
                            .saturating_sub(group_entry_bytes(&oldest, &evicted.raw_payload));
                        warn_group_cache_capacity(group, &group_name, settings);
                    }
                }
                if group.entries.len() < settings.max_members
                    && group.cache_bytes.saturating_add(member_bytes) <= settings.max_cache_bytes
                {
                    group.entries.insert(
                        channel.clone(),
                        CachedGroupEntry {
                            raw_payload: payload.to_vec(),
                            last_used: now,
                        },
                    );
                    group.cache_bytes = group.cache_bytes.saturating_add(member_bytes);
                } else {
                    warn_group_cache_capacity(group, &group_name, settings);
                }
            }
        }
    }

    fn prune_expired(&mut self, now: time::Instant) {
        self.entries
            .retain(|_, cached| now.saturating_duration_since(cached.published_at) < cached.ttl);
        self.groups.retain(|_, group| now < group.expires_at);
    }
}

fn group_entry_bytes(channel: &str, payload: &[u8]) -> usize {
    channel.len().saturating_add(payload.len())
}

fn warn_group_cache_capacity(
    group: &mut CachedDeduplicationGroup,
    group_name: &str,
    settings: DeduplicationGroupSettings,
) {
    if !group.warned_capacity {
        warn!(
            deduplication_group = group_name,
            max_members = settings.max_members,
            max_cache_bytes = settings.max_cache_bytes,
            "deduplication_group_cache_capacity_reached; evicting an old member or bypassing cache for an oversized member"
        );
        group.warned_capacity = true;
    }
}

fn positive_ttl(ttl_ms: i64) -> Option<Duration> {
    u64::try_from(ttl_ms)
        .ok()
        .filter(|ttl_ms| *ttl_ms > 0)
        .map(Duration::from_millis)
}

fn epoch_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn group_expiration(
    now: time::Instant,
    now_epoch_ms: u128,
    ttl: Duration,
    round_ms: u64,
) -> time::Instant {
    let anchor_epoch_ms = if round_ms == 0 {
        now_epoch_ms
    } else {
        let round_ms = u128::from(round_ms);
        now_epoch_ms / round_ms * round_ms
    };
    let expiration_epoch_ms = anchor_epoch_ms.saturating_add(ttl.as_millis());
    let remaining_ms = expiration_epoch_ms.saturating_sub(now_epoch_ms);
    let remaining_ms = u64::try_from(remaining_ms).unwrap_or(u64::MAX);
    now + Duration::from_millis(remaining_ms)
}

fn resolve_channel_policy(config: &OutputConfig, channel: &str) -> ResolvedChannelPolicy {
    let policy = config
        .channel_policies
        .iter()
        .find(|policy| channel_policy_matches(policy, channel))
        .or_else(|| config.channel_policies.iter().find(|policy| policy.default));
    let profile = policy
        .and_then(|policy| policy.profile.as_deref())
        .and_then(|name| config.profiles.get(name));
    ResolvedChannelPolicy {
        interval_ms: profile
            .and_then(|profile| profile.conflation_interval_ms)
            .or_else(|| policy.and_then(|policy| policy.conflation_interval_ms))
            .unwrap_or(config.conflation.interval_ms),
        deduplication_ttl_ms: profile
            .and_then(|profile| profile.deduplication_ttl_ms)
            .or_else(|| policy.and_then(|policy| policy.deduplication_ttl_ms))
            .unwrap_or(config.deduplication.ttl_ms),
        deduplication_group: profile
            .and_then(|profile| profile.deduplication_group.clone())
            .or_else(|| policy.and_then(|policy| policy.deduplication_group.clone())),
    }
}

fn channel_policy_matches(policy: &ChannelPolicy, channel: &str) -> bool {
    if let Some(pattern) = policy.glob.as_deref() {
        glob_matches(pattern, channel)
    } else if let Some(prefix) = policy.prefix.as_deref() {
        channel.starts_with(prefix)
    } else if let Some(suffix) = policy.suffix.as_deref() {
        channel.ends_with(suffix)
    } else {
        false
    }
}

#[derive(Clone, Debug)]
enum GlobToken {
    Star,
    Any,
    Literal(u8),
    Class {
        negated: bool,
        ranges: Vec<(u8, u8)>,
    },
}

fn glob_matches(pattern: &str, value: &str) -> bool {
    let tokens = parse_glob(pattern);
    let value = value.as_bytes();
    let mut previous = vec![false; value.len() + 1];
    previous[0] = true;

    for token in tokens {
        let mut current = vec![false; value.len() + 1];
        match token {
            GlobToken::Star => {
                current[0] = previous[0];
                for index in 1..=value.len() {
                    current[index] = previous[index] || current[index - 1];
                }
            }
            GlobToken::Any => {
                current[1..].copy_from_slice(&previous[..value.len()]);
            }
            GlobToken::Literal(expected) => {
                for (index, actual) in value.iter().enumerate() {
                    current[index + 1] = previous[index] && *actual == expected;
                }
            }
            GlobToken::Class { negated, ranges } => {
                for (index, actual) in value.iter().enumerate() {
                    let included = ranges
                        .iter()
                        .any(|(start, end)| start <= actual && actual <= end);
                    current[index + 1] = previous[index] && (included != negated);
                }
            }
        }
        previous = current;
    }
    previous[value.len()]
}

fn parse_glob(pattern: &str) -> Vec<GlobToken> {
    let bytes = pattern.as_bytes();
    let mut tokens = Vec::new();
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'*' => {
                if !matches!(tokens.last(), Some(GlobToken::Star)) {
                    tokens.push(GlobToken::Star);
                }
                index += 1;
            }
            b'?' => {
                tokens.push(GlobToken::Any);
                index += 1;
            }
            b'\\' if index + 1 < bytes.len() => {
                tokens.push(GlobToken::Literal(bytes[index + 1]));
                index += 2;
            }
            b'[' => {
                if let Some((class, after)) = parse_glob_class(bytes, index) {
                    tokens.push(class);
                    index = after;
                } else {
                    tokens.push(GlobToken::Literal(b'['));
                    index += 1;
                }
            }
            literal => {
                tokens.push(GlobToken::Literal(literal));
                index += 1;
            }
        }
    }
    tokens
}

fn parse_glob_class(bytes: &[u8], start: usize) -> Option<(GlobToken, usize)> {
    let mut index = start + 1;
    let negated = bytes.get(index) == Some(&b'^');
    if negated {
        index += 1;
    }
    let mut elements = Vec::<(u8, bool)>::new();
    let mut closed = false;
    while index < bytes.len() {
        match bytes[index] {
            b']' if !elements.is_empty() => {
                closed = true;
                index += 1;
                break;
            }
            b'\\' if index + 1 < bytes.len() => {
                elements.push((bytes[index + 1], true));
                index += 2;
            }
            byte => {
                elements.push((byte, false));
                index += 1;
            }
        }
    }
    if !closed || elements.is_empty() {
        return None;
    }

    let mut ranges = Vec::new();
    let mut element_index = 0;
    while element_index < elements.len() {
        if element_index + 2 < elements.len() && elements[element_index + 1] == (b'-', false) {
            ranges.push((elements[element_index].0, elements[element_index + 2].0));
            element_index += 3;
        } else {
            let character = elements[element_index].0;
            ranges.push((character, character));
            element_index += 1;
        }
    }
    Some((GlobToken::Class { negated, ranges }, index))
}

fn flush_schedules(
    intervals_ms: impl IntoIterator<Item = i64>,
    now: time::Instant,
) -> Vec<FlushSchedule> {
    let mut intervals = intervals_ms
        .into_iter()
        .filter(|interval_ms| *interval_ms > 0)
        .collect::<Vec<_>>();
    intervals.sort_unstable();
    intervals.dedup();
    intervals
        .into_iter()
        .map(|interval_ms| {
            let interval = Duration::from_millis(interval_ms as u64);
            FlushSchedule {
                interval_ms,
                interval,
                next_tick: now + interval,
            }
        })
        .collect()
}

fn advance_due_schedules(schedules: &mut [FlushSchedule], now: time::Instant) -> Vec<i64> {
    let mut due = Vec::new();
    for schedule in schedules {
        if schedule.next_tick <= now {
            due.push(schedule.interval_ms);
            // Match MissedTickBehavior::Delay: after a delayed tick, start the
            // next period from the time this tick is observed.
            schedule.next_tick = now + schedule.interval;
        }
    }
    due
}

fn next_schedule_tick(schedules: &[FlushSchedule]) -> Option<time::Instant> {
    schedules.iter().map(|schedule| schedule.next_tick).min()
}

fn deduplication_prune_interval(config: &OutputConfig) -> Option<Duration> {
    std::iter::once(config.deduplication.ttl_ms)
        .chain(
            config
                .deduplication_groups
                .values()
                .map(|group| group.ttl_ms),
        )
        .chain(
            config
                .profiles
                .values()
                .filter_map(|profile| profile.deduplication_ttl_ms),
        )
        .chain(
            config
                .channel_policies
                .iter()
                .filter_map(|policy| policy.deduplication_ttl_ms),
        )
        .filter_map(|ttl_ms| u64::try_from(ttl_ms).ok())
        .filter(|ttl_ms| *ttl_ms > 0)
        .min()
        .map(Duration::from_millis)
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

pub async fn run(
    config: AppConfig,
    metrics: Arc<Metrics>,
    config_path: &Path,
    shutdown_signals: &mut ShutdownSignals,
) -> Result<RunOutcome> {
    #[cfg(not(unix))]
    let _ = config_path;
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

fn is_sentinel_pubsub_channel(channel: &str) -> bool {
    SENTINEL_PUBSUB_CHANNELS.contains(&channel)
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

fn pending_batch(
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

fn pending_batch_length(
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
    metrics.set_output_pending_keys(output_metrics, pending.len() + passthrough.len());
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
    publisher: &mut P,
    batch: &[PendingMessage],
    atomic: bool,
    context: &mut OutputPublishContext<'_>,
) -> std::result::Result<i64, PublishFailure> {
    match publisher.publish(batch, atomic).await {
        Ok(subscribers) => {
            context
                .deduplication_cache
                .remember(batch, time::Instant::now());
            Ok(subscribers)
        }
        Err(failure) => {
            if matches!(&failure, PublishFailure::Uncertain(_)) {
                context
                    .deduplication_cache
                    .remember(batch, time::Instant::now());
            }
            match &failure {
                PublishFailure::NotSent(error) => {
                    context
                        .metrics
                        .record_output_error(context.output_metrics, error, true);
                    context
                        .metrics
                        .record_output_error_messages(context.output_metrics, batch.len());
                    context.failure_log.report(context.name, error, batch.len());
                }
                PublishFailure::Uncertain(error) => {
                    context.metrics.record_output_uncertain(
                        context.output_metrics,
                        error,
                        batch.len(),
                    );
                    context.failure_log.report(context.name, error, batch.len());
                }
            }
            Err(failure)
        }
    }
}

#[cfg(test)]
async fn publish_conflated_pending<P: BatchPublisher>(
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

async fn publish_conflated_interval_pending<P: BatchPublisher>(
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

async fn publish_passthrough_pending<P: BatchPublisher>(
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
    let max_commands_per_exec = config.conflation.max_commands_per_exec;
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
                        let channel_policy =
                            resolve_channel_policy(&config, &message.output_channel);
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

    #[cfg(unix)]
    #[tokio::test]
    async fn sighup_is_reported_as_a_configuration_reload() {
        use std::process::Command;

        let mut signals = ShutdownSignals::new().unwrap();
        let waiting = tokio::spawn(async move { signals.recv().await.unwrap() });
        tokio::task::yield_now().await;
        let pid = std::process::id().to_string();
        let status = Command::new("kill")
            .args(["-HUP", pid.as_str()])
            .status()
            .expect("kill command should be available on Unix");
        assert!(status.success());
        assert_eq!(waiting.await.unwrap(), ServiceSignal::Reload);
    }

    #[cfg(unix)]
    #[test]
    fn reload_rejects_instance_lock_or_logging_changes() {
        let current: AppConfig = serde_json::from_str(include_str!("../config.example.json"))
            .expect("example config should deserialize");
        let mut next = current.clone();
        assert!(reload_process_settings_unchanged(&current, &next));

        next.logging.level = "debug".to_owned();
        assert!(!reload_process_settings_unchanged(&current, &next));

        next = current.clone();
        next.instance_lock.path.push(".new");
        assert!(!reload_process_settings_unchanged(&current, &next));
    }

    #[cfg(unix)]
    #[test]
    fn reload_config_is_parsed_and_validated_before_acceptance() {
        let current: AppConfig = serde_json::from_str(include_str!("../config.example.json"))
            .expect("example config should deserialize");
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("config.json");
        std::fs::write(&path, include_str!("../config.example.json")).unwrap();

        let loaded = load_reload_config(&current, &path).unwrap();
        assert_eq!(loaded.outputs.len(), current.outputs.len());

        std::fs::write(&path, "{ invalid json").unwrap();
        assert!(load_reload_config(&current, &path).is_err());

        let mut incompatible: serde_json::Value =
            serde_json::from_str(include_str!("../config.example.json")).unwrap();
        incompatible["logging"]["level"] = serde_json::json!("debug");
        std::fs::write(&path, serde_json::to_vec(&incompatible).unwrap()).unwrap();
        assert!(
            load_reload_config(&current, &path)
                .unwrap_err()
                .to_string()
                .contains("logging require a full service restart")
        );
    }

    fn pending_message(output_channel: &str, payload: Vec<u8>) -> PendingMessage {
        PendingMessage {
            output_channel: output_channel.to_owned(),
            conflation_interval_ms: 0,
            deduplication_ttl_ms: None,
            deduplication_group: None,
            payload,
            raw_payload: None,
        }
    }

    fn pending_message_with_raw(
        output_channel: &str,
        payload: Vec<u8>,
        raw_payload: Vec<u8>,
    ) -> PendingMessage {
        PendingMessage {
            output_channel: output_channel.to_owned(),
            conflation_interval_ms: 0,
            deduplication_ttl_ms: None,
            deduplication_group: None,
            payload,
            raw_payload: Some(raw_payload),
        }
    }

    fn pending_group_message(output_channel: &str, payload: &[u8], group: &str) -> PendingMessage {
        let mut message = pending_message(output_channel, payload.to_vec());
        message.deduplication_group = Some(group.to_owned());
        message
    }

    fn group_settings(
        ttl_ms: i64,
        restart_on_change: bool,
    ) -> BTreeMap<String, DeduplicationGroup> {
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
        pending: &mut HashMap<String, PendingMessage>,
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
        pending: &mut HashMap<String, PendingMessage>,
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
        pending: &mut HashMap<String, PendingMessage>,
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

    #[test]
    fn channel_policy_resolution_uses_first_match_and_output_fallbacks() {
        let config = output_config_with_policies(
            125,
            900,
            vec![
                channel_policy(Some("mapped:orders:*"), None, None, Some(200), Some(50)),
                channel_policy(None, Some("mapped:orders:"), None, Some(300), Some(75)),
                channel_policy(None, Some("mapped:direct:"), None, Some(0), None),
                channel_policy(None, None, Some(":disabled"), None, Some(0)),
            ],
        );

        let mapped_channel = map_output_channel("mapped:", "", "orders:42", "", "");
        assert_eq!(
            resolve_channel_policy(&config, &mapped_channel),
            ResolvedChannelPolicy {
                interval_ms: 200,
                deduplication_ttl_ms: 50,
                deduplication_group: None,
            }
        );
        assert_eq!(
            resolve_channel_policy(&config, "mapped:direct:now"),
            ResolvedChannelPolicy {
                interval_ms: 0,
                deduplication_ttl_ms: 900,
                deduplication_group: None,
            }
        );
        assert_eq!(
            resolve_channel_policy(&config, "mapped:history:disabled"),
            ResolvedChannelPolicy {
                interval_ms: 125,
                deduplication_ttl_ms: 0,
                deduplication_group: None,
            }
        );
        assert_eq!(
            resolve_channel_policy(&config, "unmatched"),
            ResolvedChannelPolicy {
                interval_ms: 125,
                deduplication_ttl_ms: 900,
                deduplication_group: None,
            }
        );

        let direct_policy = resolve_channel_policy(&config, "mapped:direct:now");
        let metrics = Metrics::new();
        let output_metrics = metrics.register_output("direct-policy-output");
        let mut pending = HashMap::new();
        let mut passthrough = VecDeque::new();
        let mut cache = DeduplicationCache::new(config.deduplication.ttl_ms);
        enqueue_for_test_with_policy_at(
            InboundMessage {
                output_channel: "mapped:direct:now".to_owned(),
                payload: b"direct".to_vec(),
            },
            &mut pending,
            &mut passthrough,
            &metrics,
            &output_metrics,
            &mut cache,
            time::Instant::now(),
            OutputMessagePolicy {
                interval_ms: direct_policy.interval_ms,
                deduplication_ttl_ms: Some(direct_policy.deduplication_ttl_ms),
                deduplication_group: direct_policy.deduplication_group,
                max_bytes_per_exec: usize::MAX,
                oversized_policy: OversizedMessagePolicy::Send,
            },
        );
        assert!(pending.is_empty());
        assert_eq!(passthrough.len(), 1);
    }

    #[test]
    fn profiles_and_inline_policies_resolve_selectors_precedence_and_schedules() {
        let config: OutputConfig = serde_json::from_value(serde_json::json!({
            "redis": { "host": "localhost" },
            "conflation": { "interval_ms": 125 },
            "deduplication": { "ttl_ms": 900 },
            "deduplication_groups": {
                "shared": {
                    "ttl_ms": 400,
                    "round_ms": 0,
                    "restart_on_change": false
                }
            },
            "profiles": {
                "reusable": {
                    "conflation.interval_ms": 100,
                    "deduplication.ttl_ms": 250
                },
                "grouped": {
                    "conflation.interval_ms": 300,
                    "deduplication.group": "shared"
                },
                "default-profile": {
                    "conflation.interval_ms": 75
                }
            },
            "channel_policies": [
                { "default": false, "glob": "mapped:reuse:*", "profile": "reusable" },
                { "default": false, "prefix": "mapped:group:", "profile": "grouped" },
                { "default": false, "glob": "mapped:precedence:*", "profile": "reusable" },
                {
                    "default": false,
                    "suffix": ":precedence",
                    "conflation.interval_ms": 0,
                    "deduplication.ttl_ms": 0
                },
                {
                    "default": false,
                    "suffix": ":inline",
                    "conflation.interval_ms": 50,
                    "deduplication.ttl_ms": 0
                },
                {
                    "default": false,
                    "prefix": "mapped:inline-group:",
                    "conflation.interval_ms": 250,
                    "deduplication.group": "shared"
                },
                { "default": true, "profile": "default-profile" }
            ]
        }))
        .expect("profile output config should deserialize");

        for source_channel in ["reuse:a", "reuse:b"] {
            let mapped = map_output_channel("mapped:", "", source_channel, "", "");
            assert_eq!(
                resolve_channel_policy(&config, &mapped),
                ResolvedChannelPolicy {
                    interval_ms: 100,
                    deduplication_ttl_ms: 250,
                    deduplication_group: None,
                }
            );
        }
        assert_eq!(
            resolve_channel_policy(&config, "mapped:group:symbol"),
            ResolvedChannelPolicy {
                interval_ms: 300,
                deduplication_ttl_ms: 900,
                deduplication_group: Some("shared".to_owned()),
            }
        );
        assert_eq!(
            resolve_channel_policy(&config, "mapped:precedence:tail:precedence"),
            ResolvedChannelPolicy {
                interval_ms: 100,
                deduplication_ttl_ms: 250,
                deduplication_group: None,
            }
        );
        assert_eq!(
            resolve_channel_policy(&config, "mapped:other:inline"),
            ResolvedChannelPolicy {
                interval_ms: 50,
                deduplication_ttl_ms: 0,
                deduplication_group: None,
            }
        );
        assert_eq!(
            resolve_channel_policy(&config, "mapped:inline-group:member"),
            ResolvedChannelPolicy {
                interval_ms: 250,
                deduplication_ttl_ms: 900,
                deduplication_group: Some("shared".to_owned()),
            }
        );
        assert_eq!(
            resolve_channel_policy(&config, "unmatched"),
            ResolvedChannelPolicy {
                interval_ms: 75,
                deduplication_ttl_ms: 900,
                deduplication_group: None,
            }
        );

        let schedules = flush_schedules(
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
        assert_eq!(
            schedules
                .iter()
                .map(|schedule| schedule.interval_ms)
                .collect::<Vec<_>>(),
            [50, 75, 100, 125, 250, 300]
        );
        assert_eq!(
            deduplication_prune_interval(&config),
            Some(Duration::from_millis(250))
        );
    }

    #[test]
    fn output_channel_glob_supports_wildcards_classes_and_escapes() {
        assert!(glob_matches("mapped:order:[a-c]?", "mapped:order:b7"));
        assert!(!glob_matches("mapped:order:[a-c]?", "mapped:order:d7"));
        assert!(glob_matches(r"literal:\*\?", "literal:*?"));
        assert!(glob_matches("mapped:*[^0-9]", "mapped:value-x"));
        assert!(!glob_matches("mapped:*[^0-9]", "mapped:value-7"));
    }

    #[tokio::test]
    async fn distinct_interval_groups_keep_cadence_and_flush_only_the_due_group() {
        let started = time::Instant::now();
        let mut schedules = flush_schedules([200, 250, 300, 200, 0], started);
        assert_eq!(
            schedules
                .iter()
                .map(|schedule| schedule.interval_ms)
                .collect::<Vec<_>>(),
            [200, 250, 300]
        );
        let metrics = Metrics::new();
        let output_metrics = metrics.register_output("interval-groups-output");
        let mut pending = HashMap::new();
        let mut passthrough = VecDeque::new();
        for (channel, interval_ms) in [
            ("events:200", 200),
            ("events:250", 250),
            ("events:300", 300),
        ] {
            let mut message = pending_message(channel, channel.as_bytes().to_vec());
            message.conflation_interval_ms = interval_ms;
            pending.insert(channel.to_owned(), message);
        }
        let mut cache = DeduplicationCache::new(0);
        let mut publisher = RecordingPublisher::default();

        for (elapsed_ms, due_interval_ms) in [(200, 200), (250, 250), (300, 300)] {
            let due =
                advance_due_schedules(&mut schedules, started + Duration::from_millis(elapsed_ms));
            assert_eq!(due, [due_interval_ms]);
            let mut failure_log = OutputFailureLog::default();
            let mut context = publish_context_for_test(
                "interval-groups-output",
                &mut failure_log,
                &mut cache,
                &metrics,
                &output_metrics,
            );
            publish_conflated_interval_pending(
                &mut publisher,
                &mut pending,
                &mut passthrough,
                Some(due_interval_ms),
                256,
                usize::MAX,
                &mut context,
            )
            .await;
            assert!(!pending.contains_key(&format!("events:{due_interval_ms}")));
            assert_eq!(pending.len(), (300 - due_interval_ms) as usize / 50);
        }

        assert!(pending.is_empty());
        assert_eq!(
            publisher
                .attempts
                .iter()
                .flat_map(|(_, batch)| batch.iter().map(|message| message.conflation_interval_ms))
                .collect::<Vec<_>>(),
            [200, 250, 300]
        );
        assert_eq!(
            advance_due_schedules(&mut schedules, started + Duration::from_millis(400)),
            [200]
        );
        assert_eq!(
            advance_due_schedules(&mut schedules, started + Duration::from_millis(500)),
            [250]
        );
        assert_eq!(
            advance_due_schedules(&mut schedules, started + Duration::from_millis(600)),
            [200, 300]
        );
    }

    #[test]
    fn channel_ttls_expire_independently() {
        let now = time::Instant::now();
        let short = pending_message("events:short", b"same".to_vec());
        let mut short = short;
        short.deduplication_ttl_ms = Some(10);
        let mut long = pending_message("events:long", b"same".to_vec());
        long.deduplication_ttl_ms = Some(100);
        let mut cache = DeduplicationCache::new(5000);
        cache.remember(&[short.clone(), long.clone()], now);

        assert!(!cache.should_suppress(&short, now + Duration::from_millis(10)));
        assert!(cache.should_suppress(&long, now + Duration::from_millis(10)));
        assert!(!cache.should_suppress(&long, now + Duration::from_millis(100)));
    }

    #[test]
    fn fixed_group_deadline_expires_all_sibling_members_together() {
        let now = time::Instant::now();
        let groups = group_settings(600, false);
        let mut cache = DeduplicationCache::with_groups(5000, &groups);
        let first_channel = pending_group_message("events:first", b"first", "shared");
        let sibling_channel = pending_group_message("events:sibling", b"sibling", "shared");
        let changed_first_channel = pending_group_message("events:first", b"changed", "shared");

        cache.remember(std::slice::from_ref(&first_channel), now);
        cache.remember(
            std::slice::from_ref(&sibling_channel),
            now + Duration::from_millis(100),
        );
        cache.remember(
            std::slice::from_ref(&changed_first_channel),
            now + Duration::from_millis(250),
        );

        assert!(cache.should_suppress(&changed_first_channel, now + Duration::from_millis(599)));
        assert!(cache.should_suppress(&sibling_channel, now + Duration::from_millis(599)));
        assert!(!cache.should_suppress(&changed_first_channel, now + Duration::from_millis(600)));
        assert!(!cache.should_suppress(&sibling_channel, now + Duration::from_millis(600)));
        assert!(!cache.groups.contains_key("shared"));
    }

    #[test]
    fn new_group_member_renews_shared_deadline_when_enabled() {
        let now = time::Instant::now();
        let groups = group_settings(600, true);
        let mut cache = DeduplicationCache::with_groups(5000, &groups);
        let first_channel = pending_group_message("events:first", b"first", "shared");
        let sibling_channel = pending_group_message("events:sibling", b"sibling", "shared");

        cache.remember(std::slice::from_ref(&first_channel), now);
        cache.remember(
            std::slice::from_ref(&sibling_channel),
            now + Duration::from_millis(100),
        );

        assert_eq!(
            cache.groups["shared"].expires_at,
            now + Duration::from_millis(700)
        );
        assert!(cache.should_suppress(&sibling_channel, now + Duration::from_millis(600)));
        assert!(!cache.should_suppress(&sibling_channel, now + Duration::from_millis(700)));
        assert!(!cache.groups.contains_key("shared"));
    }

    #[test]
    fn group_cache_evicts_least_recently_used_members_at_capacity() {
        let now = time::Instant::now();
        let groups = group_settings_with_limits(600, true, 0, 2, 4);
        let mut cache = DeduplicationCache::with_groups(5000, &groups);
        let first = pending_group_message("a", b"1", "shared");
        let second = pending_group_message("b", b"2", "shared");
        let third = pending_group_message("c", b"3", "shared");

        cache.remember_at(std::slice::from_ref(&first), now, 10_000);
        cache.remember_at(
            std::slice::from_ref(&second),
            now + Duration::from_millis(1),
            10_001,
        );
        assert!(cache.should_suppress(&first, now + Duration::from_millis(2)));
        cache.remember_at(
            std::slice::from_ref(&third),
            now + Duration::from_millis(3),
            10_003,
        );

        let group = &cache.groups["shared"];
        assert_eq!(group.entries.len(), 2);
        assert_eq!(group.cache_bytes, 4);
        assert!(cache.should_suppress(&first, now + Duration::from_millis(4)));
        assert!(!cache.should_suppress(&second, now + Duration::from_millis(4)));
        assert!(cache.should_suppress(&third, now + Duration::from_millis(4)));
    }

    #[test]
    fn group_cache_does_not_retain_a_member_larger_than_its_byte_budget() {
        let now = time::Instant::now();
        let groups = group_settings_with_limits(600, false, 0, 4, 5);
        let mut cache = DeduplicationCache::with_groups(5000, &groups);
        let large = pending_group_message("large", b"payload", "shared");

        cache.remember_at(std::slice::from_ref(&large), now, 10_000);

        let group = &cache.groups["shared"];
        assert!(group.entries.is_empty());
        assert_eq!(group.cache_bytes, 0);
        assert!(!cache.should_suppress(&large, now + Duration::from_millis(1)));
    }

    #[test]
    fn group_rounding_anchors_expiry_to_epoch_window() {
        let now = time::Instant::now();
        let groups = group_settings_with_round(300, false, 100);
        let mut cache = DeduplicationCache::with_groups(5000, &groups);
        let message = pending_group_message("events:rounded", b"payload", "shared");

        cache.remember_at(std::slice::from_ref(&message), now, 255);

        assert_eq!(
            cache.groups["shared"].expires_at,
            now + Duration::from_millis(245)
        );
        assert!(cache.should_suppress(&message, now + Duration::from_millis(244)));
        assert!(!cache.should_suppress(&message, now + Duration::from_millis(245)));
    }

    #[test]
    fn zero_group_rounding_keeps_exact_ttl_deadline() {
        let now = time::Instant::now();
        let groups = group_settings(300, false);
        let mut cache = DeduplicationCache::with_groups(5000, &groups);
        let message = pending_group_message("events:exact", b"payload", "shared");

        cache.remember_at(std::slice::from_ref(&message), now, 255);

        assert_eq!(
            cache.groups["shared"].expires_at,
            now + Duration::from_millis(300)
        );
        assert!(cache.should_suppress(&message, now + Duration::from_millis(299)));
        assert!(!cache.should_suppress(&message, now + Duration::from_millis(300)));
    }

    #[test]
    fn changed_existing_group_member_renews_shared_sibling_deadline() {
        let now = time::Instant::now();
        let groups = group_settings(600, true);
        let mut cache = DeduplicationCache::with_groups(5000, &groups);
        let first_channel = pending_group_message("events:first", b"first", "shared");
        let sibling_channel = pending_group_message("events:sibling", b"sibling", "shared");

        cache.remember(std::slice::from_ref(&first_channel), now);
        cache.remember(std::slice::from_ref(&sibling_channel), now);
        let changed_first_channel = pending_group_message("events:first", b"changed", "shared");
        assert!(!cache.should_suppress(&changed_first_channel, now + Duration::from_millis(300)));
        cache.remember(
            std::slice::from_ref(&changed_first_channel),
            now + Duration::from_millis(300),
        );

        assert!(cache.should_suppress(&sibling_channel, now + Duration::from_millis(700)));
        assert_eq!(
            cache.groups["shared"].expires_at,
            now + Duration::from_millis(900)
        );
        assert!(!cache.should_suppress(&sibling_channel, now + Duration::from_millis(900)));
        assert!(!cache.groups.contains_key("shared"));
    }

    #[test]
    fn channel_deduplication_disable_does_not_disable_conflation() {
        let config = output_config_with_policies(
            0,
            5000,
            vec![channel_policy(
                None,
                Some("events:"),
                None,
                Some(100),
                Some(0),
            )],
        );
        let resolved = resolve_channel_policy(&config, "events:live");
        assert_eq!(resolved.interval_ms, 100);
        assert_eq!(resolved.deduplication_ttl_ms, 0);

        let metrics = Metrics::new();
        let output_metrics = metrics.register_output("channel-no-dedup-output");
        let mut pending = HashMap::new();
        let mut passthrough = VecDeque::new();
        let mut cache = DeduplicationCache::new(config.deduplication.ttl_ms);
        for payload in [b"first".to_vec(), b"latest".to_vec()] {
            enqueue_for_test_with_policy_at(
                InboundMessage {
                    output_channel: "events:live".to_owned(),
                    payload,
                },
                &mut pending,
                &mut passthrough,
                &metrics,
                &output_metrics,
                &mut cache,
                time::Instant::now(),
                OutputMessagePolicy {
                    interval_ms: resolved.interval_ms,
                    deduplication_ttl_ms: Some(resolved.deduplication_ttl_ms),
                    deduplication_group: resolved.deduplication_group.clone(),
                    max_bytes_per_exec: usize::MAX,
                    oversized_policy: OversizedMessagePolicy::Send,
                },
            );
        }
        assert!(passthrough.is_empty());
        assert_eq!(pending.len(), 1);
        assert_eq!(pending["events:live"].payload, b"latest");
        assert!(cache.entries.is_empty());
        assert_eq!(metrics.conflated_messages_total.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn nonpositive_group_ttl_disables_deduplication_but_keeps_conflation() {
        let metrics = Metrics::new();
        let output_metrics = metrics.register_output("disabled-group-output");
        let groups = group_settings(0, true);
        let mut cache = DeduplicationCache::with_groups(5000, &groups);
        let mut pending = HashMap::new();
        let mut passthrough = VecDeque::new();

        for payload in [b"first".to_vec(), b"latest".to_vec()] {
            metrics.record_output_input(&output_metrics, payload.len());
            enqueue_for_test_with_policy_at(
                InboundMessage {
                    output_channel: "events:live".to_owned(),
                    payload,
                },
                &mut pending,
                &mut passthrough,
                &metrics,
                &output_metrics,
                &mut cache,
                time::Instant::now(),
                OutputMessagePolicy {
                    interval_ms: 100,
                    deduplication_ttl_ms: Some(5000),
                    deduplication_group: Some("shared".to_owned()),
                    max_bytes_per_exec: usize::MAX,
                    oversized_policy: OversizedMessagePolicy::Send,
                },
            );
        }

        assert_eq!(pending.len(), 1);
        assert_eq!(pending["events:live"].payload, b"latest");
        assert_eq!(metrics.conflated_messages_total.load(Ordering::Relaxed), 1);

        let mut publisher = RecordingPublisher::default();
        flush_conflated_for_test(
            "disabled-group-output",
            &mut publisher,
            &mut pending,
            &mut cache,
            &metrics,
            &output_metrics,
        )
        .await;

        assert_eq!(publisher.successful_batches.len(), 1);
        assert_eq!(publisher.successful_batches[0][0].payload, b"latest");
        assert!(cache.groups.is_empty());
        let output = &metrics.snapshot().outputs["disabled-group-output"];
        assert_eq!(output.deduplicated_messages_total, 0);
        assert_eq!(output.output_messages_total, 1);
    }

    #[allow(clippy::too_many_arguments)]
    fn enqueue_for_test_with_policy_at(
        message: InboundMessage,
        pending: &mut HashMap<String, PendingMessage>,
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
        enqueue_message(
            message,
            pending,
            passthrough,
            &mut context,
            deduplication_cache,
            now,
        );
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
        pending: &mut HashMap<String, PendingMessage>,
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
        pending: &mut HashMap<String, PendingMessage>,
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
    fn deduplication_compares_output_channel_and_payload_bytes_until_ttl_expiry() {
        let ttl = Duration::from_millis(5000);
        let started = time::Instant::now();
        let mut cache = DeduplicationCache::new(5000);
        let first = pending_message("events:a", vec![0, 255]);
        cache.remember(std::slice::from_ref(&first), started);

        assert!(cache.should_suppress(&first, started + Duration::from_millis(4999)));
        assert!(!cache.should_suppress(
            &pending_message("events:b", first.payload.clone()),
            started + Duration::from_millis(1),
        ));
        assert!(!cache.should_suppress(
            &pending_message(&first.output_channel, vec![0, 254]),
            started + Duration::from_millis(1),
        ));
        assert!(!cache.should_suppress(&first, started + ttl));
        assert!(cache.entries.is_empty());
    }

    #[test]
    fn per_channel_cache_remembers_only_the_latest_published_payload() {
        let now = time::Instant::now();
        let mut cache = DeduplicationCache::new(5000);
        let a = pending_message("events", b"A".to_vec());
        let b = pending_message("events", b"B".to_vec());

        cache.remember(std::slice::from_ref(&a), now);
        assert!(!cache.should_suppress(&b, now + Duration::from_millis(1)));
        cache.remember(std::slice::from_ref(&b), now + Duration::from_millis(1));
        assert!(!cache.should_suppress(&a, now + Duration::from_millis(2)));
        cache.remember(std::slice::from_ref(&a), now + Duration::from_millis(2));

        assert!(cache.should_suppress(&a, now + Duration::from_millis(3)));
        assert!(!cache.should_suppress(&b, now + Duration::from_millis(3)));
    }

    #[test]
    fn deduplication_isolated_per_output_and_zero_ttl_disables_it() {
        let now = time::Instant::now();
        let message = pending_message("events", b"same".to_vec());
        let mut first_output = DeduplicationCache::new(5000);
        let mut second_output = DeduplicationCache::new(5000);
        let mut disabled_output = DeduplicationCache::new(0);
        let mut negative_ttl_output = DeduplicationCache::new(-1);
        first_output.remember(std::slice::from_ref(&message), now);

        assert!(first_output.should_suppress(&message, now));
        assert!(!second_output.should_suppress(&message, now));
        assert!(!disabled_output.should_suppress(&message, now));
        assert!(!negative_ttl_output.should_suppress(&message, now));
        disabled_output.remember(std::slice::from_ref(&message), now);
        negative_ttl_output.remember(std::slice::from_ref(&message), now);
        assert!(disabled_output.entries.is_empty());
        assert!(negative_ttl_output.entries.is_empty());
    }

    #[tokio::test]
    async fn conflated_value_returning_to_cached_value_is_suppressed_at_flush() {
        let metrics = Metrics::new();
        let output_metrics = metrics.register_output("dedup-output");
        let mut pending = HashMap::new();
        let mut passthrough = VecDeque::new();
        let mut cache = DeduplicationCache::new(5000);
        let last_published = pending_message("events", b"published".to_vec());
        cache.remember(std::slice::from_ref(&last_published), time::Instant::now());

        let changed = b"changed".to_vec();
        metrics.record_output_input(&output_metrics, changed.len());
        enqueue_for_test_with_cache(
            100,
            InboundMessage {
                output_channel: "events".to_owned(),
                payload: changed,
            },
            &mut pending,
            &mut passthrough,
            &metrics,
            &output_metrics,
            &mut cache,
        );
        assert_eq!(pending["events"].raw_payload(), b"changed");

        let returned = b"published".to_vec();
        metrics.record_output_input(&output_metrics, returned.len());
        enqueue_for_test_with_cache(
            100,
            InboundMessage {
                output_channel: "events".to_owned(),
                payload: returned,
            },
            &mut pending,
            &mut passthrough,
            &metrics,
            &output_metrics,
            &mut cache,
        );

        assert_eq!(pending["events"].raw_payload(), b"published");
        assert!(passthrough.is_empty());
        assert_eq!(
            metrics.deduplicated_messages_total.load(Ordering::Relaxed),
            0
        );

        let mut publisher = RecordingPublisher::default();
        let mut failure_log = OutputFailureLog::default();
        let mut publish_context = publish_context_for_test(
            "dedup-output",
            &mut failure_log,
            &mut cache,
            &metrics,
            &output_metrics,
        );
        publish_conflated_pending(
            &mut publisher,
            &mut pending,
            256,
            usize::MAX,
            &mut publish_context,
        )
        .await;

        assert!(pending.is_empty());
        assert!(publisher.attempts.is_empty());
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.deduplicated_messages_total, 1);
        assert_eq!(snapshot.deduplicated_payload_bytes_total, 9);
        assert_eq!(snapshot.conflated_messages_total, 1);
        assert_eq!(snapshot.conflated_payload_bytes_total, 7);
        assert_eq!(
            snapshot.outputs["dedup-output"].deduplicated_messages_total,
            1
        );
        assert_eq!(
            snapshot.outputs["dedup-output"].deduplicated_payload_bytes_total,
            9
        );
        assert_eq!(snapshot.outputs["dedup-output"].pending_messages, 0);
        assert_eq!(snapshot.outputs["dedup-output"].pending_payload_bytes, 0);
        assert_eq!(snapshot.outputs["dedup-output"].pending_keys, 0);
        assert_eq!(snapshot.pending_keys, 0);
    }

    #[tokio::test]
    async fn conflation_publishes_only_the_final_changed_value_in_a_window() {
        let metrics = Metrics::new();
        let output_metrics = metrics.register_output("conflate-final-output");
        let mut pending = HashMap::new();
        let mut cache = DeduplicationCache::new(5000);
        let last_published = pending_message("events", b"A".to_vec());
        cache.remember(std::slice::from_ref(&last_published), time::Instant::now());

        for payload in [b"B".to_vec(), b"C".to_vec()] {
            metrics.record_output_input(&output_metrics, payload.len());
            enqueue_for_test_with_cache(
                100,
                InboundMessage {
                    output_channel: "events".to_owned(),
                    payload,
                },
                &mut pending,
                &mut VecDeque::new(),
                &metrics,
                &output_metrics,
                &mut cache,
            );
        }
        assert_eq!(pending["events"].raw_payload(), b"C");

        let mut publisher = RecordingPublisher::default();
        flush_conflated_for_test(
            "conflate-final-output",
            &mut publisher,
            &mut pending,
            &mut cache,
            &metrics,
            &output_metrics,
        )
        .await;

        assert_eq!(publisher.successful_batches.len(), 1);
        assert_eq!(publisher.successful_batches[0][0].payload, b"C");
        assert_eq!(publisher.attempts.len(), 1);
        let output = &metrics.snapshot().outputs["conflate-final-output"];
        assert_eq!(output.deduplicated_messages_total, 0);
        assert_eq!(output.conflated_messages_total, 1);
        assert_eq!(output.output_messages_total, 1);
        assert_eq!(output.pending_messages, 0);
        assert_eq!(output.pending_payload_bytes, 0);
    }

    #[tokio::test]
    async fn conflated_deduplication_skips_removed_channels_across_exec_chunks() {
        let metrics = Metrics::new();
        let output_metrics = metrics.register_output("chunked-dedup-output");
        let mut pending = HashMap::new();
        let mut passthrough = VecDeque::new();
        let mut cache = DeduplicationCache::new(5000);
        cache.remember(
            &[pending_message("events:b", b"cached".to_vec())],
            time::Instant::now(),
        );

        for (channel, payload) in [
            ("events:a", b"first".to_vec()),
            ("events:b", b"cached".to_vec()),
            ("events:c", b"last".to_vec()),
        ] {
            metrics.record_output_input(&output_metrics, payload.len());
            enqueue_for_test_with_cache(
                100,
                InboundMessage {
                    output_channel: channel.to_owned(),
                    payload,
                },
                &mut pending,
                &mut passthrough,
                &metrics,
                &output_metrics,
                &mut cache,
            );
        }

        let mut publisher = RecordingPublisher::default();
        let mut failure_log = OutputFailureLog::default();
        let mut publish_context = publish_context_for_test(
            "chunked-dedup-output",
            &mut failure_log,
            &mut cache,
            &metrics,
            &output_metrics,
        );
        publish_conflated_pending(
            &mut publisher,
            &mut pending,
            1,
            usize::MAX,
            &mut publish_context,
        )
        .await;

        let published_channels = publisher
            .attempts
            .iter()
            .flat_map(|(_, batch)| batch.iter().map(|message| message.output_channel.as_str()))
            .collect::<Vec<_>>();
        assert_eq!(published_channels, ["events:a", "events:c"]);
        assert!(pending.is_empty());
        let output = &metrics.snapshot().outputs["chunked-dedup-output"];
        assert_eq!(output.deduplicated_messages_total, 1);
        assert_eq!(output.deduplicated_payload_bytes_total, 6);
        assert_eq!(output.pending_messages, 0);
        assert_eq!(output.pending_payload_bytes, 0);
        assert_eq!(output.pending_keys, 0);
    }

    #[tokio::test]
    async fn nonpositive_ttl_keeps_normal_conflation_enabled() {
        for ttl_ms in [0, -1] {
            let metrics = Metrics::new();
            let output_metrics = metrics.register_output("disabled-dedup-output");
            let mut pending = HashMap::new();
            let mut cache = DeduplicationCache::new(ttl_ms);

            for payload in [b"first".to_vec(), b"latest".to_vec()] {
                metrics.record_output_input(&output_metrics, payload.len());
                enqueue_for_test_with_cache(
                    100,
                    InboundMessage {
                        output_channel: "events".to_owned(),
                        payload,
                    },
                    &mut pending,
                    &mut VecDeque::new(),
                    &metrics,
                    &output_metrics,
                    &mut cache,
                );
            }
            assert_eq!(pending["events"].payload, b"latest");

            let mut publisher = RecordingPublisher::default();
            flush_conflated_for_test(
                "disabled-dedup-output",
                &mut publisher,
                &mut pending,
                &mut cache,
                &metrics,
                &output_metrics,
            )
            .await;

            assert_eq!(publisher.successful_batches.len(), 1);
            assert_eq!(publisher.successful_batches[0][0].payload, b"latest");
            let output = &metrics.snapshot().outputs["disabled-dedup-output"];
            assert_eq!(output.deduplicated_messages_total, 0);
            assert_eq!(output.conflated_messages_total, 1);
            assert_eq!(output.pending_messages, 0);
            assert_eq!(output.pending_payload_bytes, 0);
        }
    }

    #[tokio::test]
    async fn direct_mode_publishes_changed_values_and_suppresses_repeats() {
        let metrics = Metrics::new();
        let output_metrics = metrics.register_output("direct-changes-output");
        let mut pending = HashMap::new();
        let mut passthrough = VecDeque::new();
        let mut cache = DeduplicationCache::new(5000);
        let mut publisher = RecordingPublisher::default();

        for payload in [b"A".to_vec(), b"B".to_vec(), b"B".to_vec()] {
            metrics.record_output_input(&output_metrics, payload.len());
            enqueue_for_test_with_cache(
                0,
                InboundMessage {
                    output_channel: "events".to_owned(),
                    payload,
                },
                &mut pending,
                &mut passthrough,
                &metrics,
                &output_metrics,
                &mut cache,
            );
            flush_passthrough_for_test(
                "direct-changes-output",
                &mut publisher,
                &mut pending,
                &mut passthrough,
                &mut cache,
                &metrics,
                &output_metrics,
            )
            .await;
        }

        assert_eq!(publisher.successful_batches.len(), 2);
        assert_eq!(publisher.successful_batches[0][0].payload, b"A");
        assert_eq!(publisher.successful_batches[1][0].payload, b"B");
        let output = &metrics.snapshot().outputs["direct-changes-output"];
        assert_eq!(output.deduplicated_messages_total, 1);
        assert_eq!(output.deduplicated_payload_bytes_total, 1);
        assert_eq!(output.pending_messages, 0);
        assert_eq!(output.pending_payload_bytes, 0);
    }

    #[tokio::test]
    async fn truncate_deduplication_compares_the_raw_input_payload() {
        let metrics = Metrics::new();
        let output_metrics = metrics.register_output("raw-truncate-dedup-output");
        let mut pending = HashMap::new();
        let mut passthrough = VecDeque::new();
        let mut cache = DeduplicationCache::new(5000);
        let mut publisher = RecordingPublisher::default();
        let channel = "events";
        let max_bytes = publish_command_frame_bytes_for_lengths(channel.len(), 2);
        let policy = OutputMessagePolicy {
            interval_ms: 0,
            deduplication_ttl_ms: None,
            deduplication_group: None,
            max_bytes_per_exec: max_bytes,
            oversized_policy: OversizedMessagePolicy::Truncate,
        };

        for payload in [vec![0, 1, 2], vec![0, 1, 3]] {
            metrics.record_output_input(&output_metrics, payload.len());
            enqueue_for_test_with_policy_at(
                InboundMessage {
                    output_channel: channel.to_owned(),
                    payload,
                },
                &mut pending,
                &mut passthrough,
                &metrics,
                &output_metrics,
                &mut cache,
                time::Instant::now(),
                policy.clone(),
            );
            flush_passthrough_for_test(
                "raw-truncate-dedup-output",
                &mut publisher,
                &mut pending,
                &mut passthrough,
                &mut cache,
                &metrics,
                &output_metrics,
            )
            .await;
        }

        assert_eq!(publisher.successful_batches.len(), 2);
        assert_eq!(publisher.successful_batches[0][0].payload, [0, 1]);
        assert_eq!(publisher.successful_batches[1][0].payload, [0, 1]);
        assert_eq!(
            cache.entries["events"].raw_payload,
            [0, 1, 3],
            "the TTL cache must retain the untruncated input"
        );

        let repeated = vec![0, 1, 3];
        metrics.record_output_input(&output_metrics, repeated.len());
        enqueue_for_test_with_policy_at(
            InboundMessage {
                output_channel: channel.to_owned(),
                payload: repeated,
            },
            &mut pending,
            &mut passthrough,
            &metrics,
            &output_metrics,
            &mut cache,
            time::Instant::now(),
            policy,
        );

        assert!(passthrough.is_empty());
        let output = &metrics.snapshot().outputs["raw-truncate-dedup-output"];
        assert_eq!(output.deduplicated_messages_total, 1);
        assert_eq!(output.deduplicated_payload_bytes_total, 3);
        assert_eq!(output.pending_messages, 0);
        assert_eq!(output.pending_payload_bytes, 0);
    }

    #[test]
    fn direct_queue_deduplicates_inside_ttl_and_accepts_same_value_after_expiry() {
        let metrics = Metrics::new();
        let output_metrics = metrics.register_output("direct-dedup-output");
        let mut pending = HashMap::new();
        let mut passthrough = VecDeque::new();
        let mut cache = DeduplicationCache::new(5000);
        let published = pending_message("events", vec![0, 255]);
        let now = time::Instant::now();
        cache.remember(std::slice::from_ref(&published), now);

        for (payload, enqueued_at) in [
            (published.payload.clone(), now + Duration::from_millis(1)),
            (published.payload.clone(), now + Duration::from_millis(2)),
        ] {
            metrics.record_output_input(&output_metrics, payload.len());
            enqueue_for_test_with_cache_at(
                0,
                InboundMessage {
                    output_channel: published.output_channel.clone(),
                    payload,
                },
                &mut pending,
                &mut passthrough,
                &metrics,
                &output_metrics,
                &mut cache,
                enqueued_at,
            );
        }
        assert!(passthrough.is_empty());

        metrics.record_output_input(&output_metrics, published.payload.len());
        enqueue_for_test_with_cache_at(
            0,
            InboundMessage {
                output_channel: published.output_channel,
                payload: published.payload,
            },
            &mut pending,
            &mut passthrough,
            &metrics,
            &output_metrics,
            &mut cache,
            now + Duration::from_millis(5000),
        );

        assert_eq!(passthrough.len(), 1);
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.deduplicated_messages_total, 2);
        assert_eq!(snapshot.deduplicated_payload_bytes_total, 4);
        assert_eq!(snapshot.outputs["direct-dedup-output"].pending_messages, 1);
        assert_eq!(
            snapshot.outputs["direct-dedup-output"].pending_payload_bytes,
            2
        );
        assert_eq!(snapshot.outputs["direct-dedup-output"].pending_keys, 1);
        assert_eq!(snapshot.pending_keys, 1);
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
        let mut deduplication_cache = DeduplicationCache::new(5000);
        let mut publish_context = publish_context_for_test(
            "custom-output",
            &mut failure_log,
            &mut deduplication_cache,
            &metrics,
            &output_metrics,
        );
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
        assert_eq!(
            deduplication_cache.entries["events:a"].raw_payload,
            [0, b'a']
        );
        assert_eq!(
            deduplication_cache.entries["events:b"].raw_payload,
            [0, b'b']
        );

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
        let mut deduplication_cache = DeduplicationCache::new(5000);
        let mut publish_context = publish_context_for_test(
            "failed-output",
            &mut failure_log,
            &mut deduplication_cache,
            &metrics,
            &output_metrics,
        );
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
        assert!(!deduplication_cache.entries.contains_key("events:a"));
        assert!(!deduplication_cache.entries.contains_key("events:b"));
        assert!(deduplication_cache.entries.contains_key("events:c"));
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
        let mut deduplication_cache = DeduplicationCache::new(5000);
        let mut publish_context = publish_context_for_test(
            "immediate-output",
            &mut failure_log,
            &mut deduplication_cache,
            &metrics,
            &output_metrics,
        );
        publish_passthrough_pending(
            &mut publisher,
            &mut pending,
            &mut passthrough,
            &mut publish_context,
        )
        .await;

        assert_eq!(publisher.attempts.len(), 2);
        assert!(publisher.attempts.iter().all(|(atomic, _)| !atomic));
        assert_eq!(publisher.possible_executions.len(), 1);
        assert_eq!(publisher.possible_executions[0][0].payload, [0, 255]);
        assert_eq!(publisher.successful_batches.len(), 1);
        assert_eq!(deduplication_cache.entries["events"].raw_payload, b"later");
        assert_eq!(publisher.successful_batches[0][0].payload, b"later");
        let output = &metrics.snapshot().outputs["immediate-output"];
        assert_eq!(output.uncertain_transactions_total, 1);
        assert_eq!(output.uncertain_messages_total, 1);
        assert_eq!(output.publish_error_messages_total, 1);
        assert_eq!(output.output_messages_total, 1);
        assert_eq!(output.pending_messages, 0);
        assert_eq!(output.pending_payload_bytes, 0);
    }

    #[tokio::test]
    async fn cache_records_acknowledged_and_ambiguous_sends_but_not_pre_send_failures() {
        let metrics = Metrics::new();
        let output_metrics = metrics.register_output("send-certainty-output");
        let groups = group_settings(5000, true);
        let message = pending_group_message("events", b"payload", "shared");

        let mut not_sent_cache = DeduplicationCache::with_groups(5000, &groups);
        let mut not_sent_publisher = RecordingPublisher {
            not_sent_failures_remaining: 1,
            ..RecordingPublisher::default()
        };
        let mut not_sent_log = OutputFailureLog::default();
        {
            let mut not_sent_context = publish_context_for_test(
                "send-certainty-output",
                &mut not_sent_log,
                &mut not_sent_cache,
                &metrics,
                &output_metrics,
            );
            assert!(
                publish_once(
                    &mut not_sent_publisher,
                    std::slice::from_ref(&message),
                    false,
                    &mut not_sent_context,
                )
                .await
                .is_err()
            );
        }
        assert!(not_sent_cache.entries.is_empty());
        assert!(not_sent_cache.groups.is_empty());
        assert!(!not_sent_cache.should_suppress(&message, time::Instant::now()));

        let mut uncertain_cache = DeduplicationCache::with_groups(5000, &groups);
        let mut uncertain_publisher = RecordingPublisher {
            uncertain_failures_remaining: 1,
            ..RecordingPublisher::default()
        };
        let mut uncertain_log = OutputFailureLog::default();
        {
            let mut uncertain_context = publish_context_for_test(
                "send-certainty-output",
                &mut uncertain_log,
                &mut uncertain_cache,
                &metrics,
                &output_metrics,
            );
            assert!(
                publish_once(
                    &mut uncertain_publisher,
                    std::slice::from_ref(&message),
                    false,
                    &mut uncertain_context,
                )
                .await
                .is_err()
            );
        }
        assert!(uncertain_cache.should_suppress(&message, time::Instant::now()));
        assert!(uncertain_cache.groups.contains_key("shared"));
    }

    #[test]
    fn transaction_byte_limit_splits_at_the_exact_wire_size_boundary() {
        let first = pending_message("events:a", vec![0; 8]);
        let second = pending_message("events:b", vec![255; 12]);
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
        let message = pending_message("events:large", vec![0xff; 128]);
        let later = pending_message("events:small", b"later".to_vec());
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
        let mut expected_message =
            pending_message_with_raw("events:binary", payload[..3].to_vec(), payload.clone());
        expected_message.conflation_interval_ms = 10;
        let max_bytes = publish_operation_frame_bytes(&expected_message, true);
        let mut policy_log = OversizedPolicyLog::default();

        let prepared = prepare_for_test(
            "truncate-output",
            OutputMessagePolicy {
                interval_ms: 10,
                deduplication_ttl_ms: None,
                deduplication_group: None,
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
        let max_bytes =
            publish_operation_frame_bytes(&pending_message(channel, Vec::new()), true) - 1;
        let mut policy_log = OversizedPolicyLog::default();

        assert!(
            prepare_for_test(
                "too-small-output",
                OutputMessagePolicy {
                    interval_ms: 10,
                    deduplication_ttl_ms: None,
                    deduplication_group: None,
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
                deduplication_ttl_ms: None,
                deduplication_group: None,
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
                    deduplication_ttl_ms: None,
                    deduplication_group: None,
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
    fn sentinel_filter_matches_hello_and_known_notification_channels_only() {
        for channel in [
            "__sentinel__:hello",
            "+sdown",
            "-odown",
            "+switch-master",
            "+failover-state-select-slave",
            "+sentinel-address-switch",
        ] {
            assert!(is_sentinel_pubsub_channel(channel), "{channel}");
        }

        for channel in ["metrics:cpu", "+custom-event", "__sentinel__:custom"] {
            assert!(!is_sentinel_pubsub_channel(channel), "{channel}");
        }
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
                pending_message("replica:b", vec![0, 255]),
            ),
            (
                "replica:a".to_owned(),
                pending_message("replica:a", b"value".to_vec()),
            ),
        ]);
        let batch = pending_batch(10, 256, usize::MAX, &pending, &VecDeque::new());

        assert_eq!(batch.len(), 2);
        assert_eq!(batch[0].output_channel, "replica:a");
        assert_eq!(batch[1].payload, [0, 255]);
    }
}
