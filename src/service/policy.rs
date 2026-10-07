use std::time::Duration;

use serde_json::{Value, json};
use tokio::time;

#[cfg(test)]
use crate::config::DEFAULT_CHANNEL_CACHE_MAX_ENTRIES;
use crate::config::{ChannelOverrides, ChannelPolicy, OutputConfig};
use crate::status::Metrics;

use super::channel_cache::ChannelCache;
use super::{FlushSchedule, ResolvedChannelPolicy};

pub(super) mod glob;

use glob::{GlobPattern, GlobWorkspace};

#[cfg(test)]
pub(super) fn glob_matches(pattern: &str, value: &str) -> bool {
    glob::glob_matches(pattern, value)
}

#[cfg(test)]
pub(super) fn resolve_channel_policy(
    config: &OutputConfig,
    channel: &str,
) -> ResolvedChannelPolicy {
    let mut workspace = GlobWorkspace::default();
    let policy = config
        .channel_policies
        .iter()
        .find(|policy| channel_policy_matches(policy, channel, &mut workspace))
        .or_else(|| config.channel_policies.iter().find(|policy| policy.default));
    resolve_policy(config, policy)
}

pub(super) struct CompiledChannelPolicies {
    explicit: Vec<(usize, CompiledChannelSelector)>,
    default_index: Option<usize>,
    cache: Option<ChannelCache<ResolvedChannelPolicy>>,
}

impl CompiledChannelPolicies {
    #[cfg(test)]
    pub(super) fn new(config: &OutputConfig) -> Self {
        Self::build(config, false, DEFAULT_CHANNEL_CACHE_MAX_ENTRIES, None)
    }

    pub(super) fn new_with_cache(
        config: &OutputConfig,
        cache_capacity: usize,
        inspector: Option<(&Metrics, String)>,
    ) -> Self {
        Self::build(config, true, cache_capacity, inspector)
    }

    fn build(
        config: &OutputConfig,
        cache_enabled: bool,
        cache_capacity: usize,
        inspector: Option<(&Metrics, String)>,
    ) -> Self {
        let mut explicit = Vec::new();
        let mut default_index = None;
        for (index, policy) in config.channel_policies.iter().enumerate() {
            if policy.default {
                default_index.get_or_insert(index);
            }
            let selector = if let Some(pattern) = policy.glob.as_deref() {
                Some(CompiledChannelSelector::Glob(GlobPattern::parse(pattern)))
            } else if let Some(prefix) = policy.prefix.as_deref() {
                Some(CompiledChannelSelector::Prefix(prefix.to_owned()))
            } else {
                policy
                    .suffix
                    .as_deref()
                    .map(|suffix| CompiledChannelSelector::Suffix(suffix.to_owned()))
            };
            if let Some(selector) = selector {
                explicit.push((index, selector));
            }
        }
        let cache = (!config.channel_policies.is_empty() && (cache_enabled || inspector.is_some()))
            .then(|| ChannelCache::new(cache_capacity, inspector.is_some()));
        if let (Some((metrics, name)), Some(cache)) = (inspector, cache.as_ref()) {
            metrics.register_channel_cache_inspector(
                name,
                cache.inspector(resolved_policy_json).unwrap(),
            );
        }
        Self {
            explicit,
            default_index,
            cache,
        }
    }

    pub(super) fn resolve(&self, config: &OutputConfig, channel: &str) -> ResolvedChannelPolicy {
        if let Some(cache) = &self.cache
            && cache.is_enabled()
            && let Some(policy) = cache.get(channel)
        {
            return policy;
        }
        let mut workspace = GlobWorkspace::default();
        let policy = self
            .explicit
            .iter()
            .find(|(_, selector)| selector.matches(channel, &mut workspace))
            .map(|(index, _)| &config.channel_policies[*index])
            .or_else(|| {
                self.default_index
                    .map(|index| &config.channel_policies[index])
            });
        let resolved = resolve_policy(config, policy);
        if let Some(cache) = &self.cache
            && cache.is_enabled()
        {
            cache.insert(channel, resolved.clone());
        }
        resolved
    }

    #[cfg(test)]
    pub(super) fn cached_channels(&self) -> usize {
        self.cache.as_ref().map_or(0, ChannelCache::entry_count)
    }

    #[cfg(test)]
    pub(super) fn cache_snapshot(&self) -> Option<Value> {
        self.cache
            .as_ref()
            .map(|cache| cache.snapshot_for_test(resolved_policy_json))
    }

    #[cfg(test)]
    pub(super) fn estimated_heap_bytes(&self) -> usize {
        self.explicit.capacity() * std::mem::size_of::<(usize, CompiledChannelSelector)>()
            + self
                .explicit
                .iter()
                .map(|(_, selector)| match selector {
                    CompiledChannelSelector::Glob(pattern) => pattern.estimated_heap_bytes(),
                    CompiledChannelSelector::Prefix(prefix)
                    | CompiledChannelSelector::Suffix(prefix) => prefix.capacity(),
                })
                .sum::<usize>()
    }
}

fn resolved_policy_json(policy: &ResolvedChannelPolicy) -> Value {
    json!({
        "conflation_interval_ms": policy.interval_ms,
        "deduplication_ttl_ms": policy.deduplication_ttl_ms,
        "deduplication_group": policy.deduplication_group,
    })
}

enum CompiledChannelSelector {
    Glob(GlobPattern),
    Prefix(String),
    Suffix(String),
}

impl CompiledChannelSelector {
    fn matches(&self, channel: &str, workspace: &mut GlobWorkspace) -> bool {
        match self {
            Self::Glob(pattern) => pattern.matches(channel, workspace),
            Self::Prefix(prefix) => channel.starts_with(prefix),
            Self::Suffix(suffix) => channel.ends_with(suffix),
        }
    }
}

fn resolve_policy(config: &OutputConfig, policy: Option<&ChannelPolicy>) -> ResolvedChannelPolicy {
    let profile = policy
        .and_then(|policy| policy.profile.as_deref())
        .and_then(|name| config.profiles.get(name));
    let profile_overrides = profile.map(ChannelOverrides::from);
    let policy_overrides = policy.map(ChannelOverrides::from);
    ResolvedChannelPolicy {
        interval_ms: profile_overrides
            .and_then(|overrides| overrides.conflation_interval_ms)
            .or_else(|| policy_overrides.and_then(|overrides| overrides.conflation_interval_ms))
            .unwrap_or(config.conflation.interval_ms),
        deduplication_ttl_ms: profile_overrides
            .and_then(|overrides| overrides.deduplication_ttl_ms)
            .or_else(|| policy_overrides.and_then(|overrides| overrides.deduplication_ttl_ms))
            .unwrap_or(config.deduplication.ttl_ms),
        deduplication_group: profile_overrides
            .and_then(|overrides| overrides.deduplication_group.map(str::to_owned))
            .or_else(|| {
                policy_overrides
                    .and_then(|overrides| overrides.deduplication_group.map(str::to_owned))
            }),
    }
}

#[cfg(test)]
fn channel_policy_matches(
    policy: &ChannelPolicy,
    channel: &str,
    workspace: &mut GlobWorkspace,
) -> bool {
    if let Some(pattern) = policy.glob.as_deref() {
        GlobPattern::parse(pattern).matches(channel, workspace)
    } else if let Some(prefix) = policy.prefix.as_deref() {
        channel.starts_with(prefix)
    } else if let Some(suffix) = policy.suffix.as_deref() {
        channel.ends_with(suffix)
    } else {
        false
    }
}

pub(super) fn flush_schedules(
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

pub(super) fn advance_due_schedules(
    schedules: &mut [FlushSchedule],
    now: time::Instant,
) -> Vec<i64> {
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

pub(super) fn next_schedule_tick(schedules: &[FlushSchedule]) -> Option<time::Instant> {
    schedules.iter().map(|schedule| schedule.next_tick).min()
}

#[cfg(test)]
#[path = "../../tests/unit/service_policy_bench.rs"]
mod bench;
