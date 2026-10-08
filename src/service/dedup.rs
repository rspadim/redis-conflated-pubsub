use std::{
    collections::{BTreeMap, HashMap},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use tokio::time;
use tracing::warn;

use crate::config::{DeduplicationGroup, OutputConfig};

use super::PendingMessage;

pub(super) struct CachedPublishedValue {
    pub(super) raw_payload: Vec<u8>,
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

pub(super) struct CachedGroupEntry {
    raw_payload: Vec<u8>,
    last_used: time::Instant,
}

pub(super) struct CachedDeduplicationGroup {
    pub(super) expires_at: time::Instant,
    pub(super) entries: HashMap<String, CachedGroupEntry>,
    pub(super) cache_bytes: usize,
    warned_capacity: bool,
}

pub(super) struct DeduplicationCache {
    ttl: Option<Duration>,
    pub(super) entries: HashMap<String, CachedPublishedValue>,
    group_settings: HashMap<String, DeduplicationGroupSettings>,
    pub(super) groups: HashMap<String, CachedDeduplicationGroup>,
}

impl DeduplicationCache {
    #[cfg(test)]
    pub(super) fn new(ttl_ms: i64) -> Self {
        Self::with_groups(ttl_ms, &BTreeMap::new())
    }

    pub(super) fn with_groups(
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

    pub(super) fn has_active_ttls(&self) -> bool {
        self.ttl.is_some()
            || self
                .group_settings
                .values()
                .any(|settings| settings.ttl.is_some())
    }

    pub(super) fn is_active_for(&self, message: &PendingMessage) -> bool {
        if message.deduplication_group.is_some() {
            return true;
        }
        match message.deduplication_ttl_ms {
            Some(ttl_ms) => ttl_ms > 0,
            None => self.ttl.is_some(),
        }
    }

    pub(super) fn should_suppress(&mut self, message: &PendingMessage, now: time::Instant) -> bool {
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

    pub(super) fn remember(&mut self, messages: &[PendingMessage], now: time::Instant) {
        self.remember_at(messages, now, epoch_millis());
    }

    pub(super) fn remember_at(
        &mut self,
        messages: &[PendingMessage],
        now: time::Instant,
        now_epoch_ms: u128,
    ) {
        if !self.has_active_ttls()
            && messages.iter().all(|message| {
                message.deduplication_group.is_none()
                    && !message
                        .deduplication_ttl_ms
                        .is_some_and(|ttl_ms| ttl_ms > 0)
            })
        {
            return;
        }

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

    pub(super) fn prune_expired(&mut self, now: time::Instant) {
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

pub(super) fn deduplication_prune_interval(config: &OutputConfig) -> Option<Duration> {
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
