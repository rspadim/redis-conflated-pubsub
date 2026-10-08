use anyhow::{Result, bail};

use super::{
    AppConfig, ChannelFilterRule, ChannelOverrides, MAX_CHANNEL_CACHE_MAX_ENTRIES,
    MAX_DEDUPLICATION_CACHE_BYTES, MAX_DEDUPLICATION_CACHE_ENTRIES,
    MAX_DEDUPLICATION_GROUP_CACHE_BYTES, MAX_DEDUPLICATION_GROUP_MEMBERS, MAX_RUNTIME_DURATION_MS,
    OutputConfig, RedisConfig, Subscription, same_pubsub_server,
};

pub(super) fn validate(config: &AppConfig) -> Result<()> {
    validate_status_and_input(config)?;
    validate_redis_and_outputs(config)?;
    if config.logging.retention_days == 0 || config.logging.max_total_size_mb == 0 {
        bail!("logging retention and size limits must be greater than zero");
    }
    if config.logging.prefix.is_empty() {
        bail!("logging.prefix must not be empty");
    }
    if config.logging.prefix.contains(['/', '\\']) {
        bail!("logging.prefix must not contain path separators");
    }
    validate_echo_loop_guard(config)?;
    Ok(())
}

fn validate_status_and_input(config: &AppConfig) -> Result<()> {
    if config.max_bytes_per_exec == 0 {
        bail!("max_bytes_per_exec must be greater than zero");
    }
    validate_channel_cache_capacity(
        "input.filter_cache_max_entries",
        config.input.filter_cache_max_entries,
    )?;
    if config.input.subscriptions.is_empty() {
        bail!("input.subscriptions must contain at least one subscription");
    }
    validate_filter_rules("input.filters", &config.input.filters)?;
    if config.outputs.is_empty() {
        bail!("outputs must contain at least one output");
    }
    if config.status.path.is_some() && config.status.update_interval_ms == 0 {
        bail!("status.update_interval_ms must be greater than zero");
    }
    if let Some(http) = &config.status.http {
        if http.bind.is_empty() {
            bail!("status.http.bind must not be empty");
        }
        if http.port == 0 {
            bail!("status.http.port must be greater than zero");
        }
    }
    if config.status.update_interval_ms > MAX_RUNTIME_DURATION_MS as u64 {
        bail!("status.update_interval_ms must not exceed {MAX_RUNTIME_DURATION_MS} ms");
    }

    Ok(())
}

fn validate_redis_and_outputs(config: &AppConfig) -> Result<()> {
    validate_redis(&config.input.redis)?;
    for (name, output) in &config.outputs {
        if name.trim().is_empty() {
            bail!("output names must not be empty");
        }
        validate_channel_cache_capacity(
            &format!("outputs.{name}.filter_cache_max_entries"),
            output.filter_cache_max_entries,
        )?;
        validate_channel_cache_capacity(
            &format!("outputs.{name}.channel_policy_cache_max_entries"),
            output.channel_policy_cache_max_entries,
        )?;
        validate_redis(&output.redis)?;
        if output.conflation.max_commands_per_exec == 0 {
            bail!("outputs.{name}.conflation.max_commands_per_exec must be greater than zero");
        }
        if output.conflation.max_bytes_per_exec == Some(0) {
            bail!("outputs.{name}.conflation.max_bytes_per_exec must be greater than zero");
        }
        if output.conflation.max_in_flight_commands == Some(0) {
            bail!("outputs.{name}.conflation.max_in_flight_commands must be greater than zero");
        }
        if output.conflation.max_in_flight_bytes == Some(0) {
            bail!("outputs.{name}.conflation.max_in_flight_bytes must be greater than zero");
        }
        validate_runtime_duration_ms(
            &format!("outputs.{name}.conflation.interval_ms"),
            output.conflation.interval_ms,
        )?;
        validate_runtime_duration_ms(
            &format!("outputs.{name}.deduplication.ttl_ms"),
            output.deduplication.ttl_ms,
        )?;
        validate_deduplication_cache_capacity(
            &format!("outputs.{name}.deduplication.max_entries"),
            output.deduplication.max_entries,
            MAX_DEDUPLICATION_CACHE_ENTRIES,
        )?;
        validate_deduplication_cache_capacity(
            &format!("outputs.{name}.deduplication.max_cache_bytes"),
            output.deduplication.max_cache_bytes,
            MAX_DEDUPLICATION_CACHE_BYTES,
        )?;
        validate_filter_rules(&format!("outputs.{name}.filters"), &output.filters)?;
        validate_groups(name, output)?;
        validate_profiles(name, output)?;
        validate_channel_policies(name, output)?;
    }
    Ok(())
}

fn validate_channel_cache_capacity(path: &str, capacity: usize) -> Result<()> {
    if capacity > MAX_CHANNEL_CACHE_MAX_ENTRIES {
        bail!("{path} must not exceed {MAX_CHANNEL_CACHE_MAX_ENTRIES}");
    }
    Ok(())
}

fn validate_deduplication_cache_capacity(
    path: &str,
    capacity: Option<usize>,
    maximum: usize,
) -> Result<()> {
    match capacity {
        Some(0) => bail!("{path} must be greater than zero"),
        Some(capacity) if capacity > maximum => {
            bail!("{path} must not exceed {maximum}");
        }
        _ => Ok(()),
    }
}

fn validate_groups(name: &str, output: &OutputConfig) -> Result<()> {
    for (group_name, group) in &output.deduplication_groups {
        if group_name.trim().is_empty() {
            bail!("outputs.{name}.deduplication_groups names must not be empty");
        }
        if group.ttl_ms > 0 && group.round_ms > group.ttl_ms as u64 {
            bail!(
                "outputs.{name}.deduplication_groups.{group_name}.round_ms must not exceed a positive ttl_ms"
            );
        }
        validate_runtime_duration_ms(
            &format!("outputs.{name}.deduplication_groups.{group_name}.ttl_ms"),
            group.ttl_ms,
        )?;
        if group.max_members == 0 {
            bail!(
                "outputs.{name}.deduplication_groups.{group_name}.max_members must be greater than zero"
            );
        }
        if group.max_members > MAX_DEDUPLICATION_GROUP_MEMBERS {
            bail!(
                "outputs.{name}.deduplication_groups.{group_name}.max_members must not exceed {MAX_DEDUPLICATION_GROUP_MEMBERS}"
            );
        }
        if group.max_cache_bytes == 0 {
            bail!(
                "outputs.{name}.deduplication_groups.{group_name}.max_cache_bytes must be greater than zero"
            );
        }
        if group.max_cache_bytes > MAX_DEDUPLICATION_GROUP_CACHE_BYTES {
            bail!(
                "outputs.{name}.deduplication_groups.{group_name}.max_cache_bytes must not exceed {MAX_DEDUPLICATION_GROUP_CACHE_BYTES}"
            );
        }
    }
    Ok(())
}

fn validate_profiles(name: &str, output: &OutputConfig) -> Result<()> {
    for (profile_name, profile) in &output.profiles {
        let path = format!("outputs.{name}.profiles.{profile_name}");
        if profile_name.trim().is_empty() {
            bail!("outputs.{name}.profiles names must not be empty");
        }
        validate_channel_overrides(output, &path, ChannelOverrides::from(profile))?;
    }
    Ok(())
}

fn validate_channel_policies(name: &str, output: &OutputConfig) -> Result<()> {
    if !output.channel_policies.is_empty() {
        let default_count = output
            .channel_policies
            .iter()
            .filter(|policy| policy.default)
            .count();
        if default_count != 1 {
            bail!("outputs.{name}.channel_policies must contain exactly one default rule");
        }
        if !output
            .channel_policies
            .last()
            .is_some_and(|policy| policy.default)
        {
            bail!("outputs.{name}.channel_policies default rule must be last");
        }
    }
    for (index, policy) in output.channel_policies.iter().enumerate() {
        let path = format!("outputs.{name}.channel_policies[{index}]");
        let selector_count = [
            policy.glob.as_deref(),
            policy.prefix.as_deref(),
            policy.suffix.as_deref(),
        ]
        .into_iter()
        .flatten()
        .count();
        if policy.default {
            if selector_count != 0 {
                bail!("{path} default rule must not specify glob, prefix, or suffix");
            }
        } else {
            if selector_count != 1 {
                bail!(
                    "{path} must specify exactly one of glob, prefix, or suffix, or default:true"
                );
            }
            let selector = policy
                .glob
                .as_deref()
                .or(policy.prefix.as_deref())
                .or(policy.suffix.as_deref())
                .expect("selector_count was checked");
            if selector.trim().is_empty() {
                bail!("{path} selector must not be empty");
            }
        }
        if let Some(profile_name) = policy.profile.as_deref() {
            if profile_name.trim().is_empty() {
                bail!("{path}.profile must not be empty");
            }
            if policy.has_inline_overrides() {
                bail!("{path} must use either profile or inline overrides, not both");
            }
            if !output.profiles.contains_key(profile_name) {
                bail!("{path} references unknown profile {profile_name:?}");
            }
        } else {
            if !policy.has_inline_overrides() {
                bail!("{path} must specify profile or at least one inline override");
            }
            validate_channel_overrides(output, &path, ChannelOverrides::from(policy))?;
        }
    }
    Ok(())
}

fn validate_channel_overrides(
    output: &OutputConfig,
    path: &str,
    overrides: ChannelOverrides<'_>,
) -> Result<()> {
    if let Some(interval_ms) = overrides.conflation_interval_ms {
        validate_runtime_duration_ms(&format!("{path}.conflation.interval_ms"), interval_ms)?;
    }
    validate_deduplication_override(
        output,
        path,
        overrides.deduplication_ttl_ms,
        overrides.deduplication_group,
    )
}

fn validate_echo_loop_guard(config: &AppConfig) -> Result<()> {
    for subscription in &config.input.subscriptions {
        match subscription {
            Subscription::Subscribe { channel, .. } if channel.is_empty() => {
                bail!("SUBSCRIBE channels must not be empty");
            }
            Subscription::Psubscribe { pattern, .. } if pattern.is_empty() => {
                bail!("PSUBSCRIBE patterns must not be empty");
            }
            _ => {}
        }
        let (subscription_prefix, subscription_suffix) = subscription.output_mapping();
        for (name, output) in &config.outputs {
            if same_pubsub_server(&config.input.redis, &output.redis)
                && config.input.exclude_output_echoes
                && output.channel_prefix.is_empty()
                && subscription_prefix.is_empty()
                && subscription_suffix.is_empty()
                && output.channel_suffix.is_empty()
            {
                bail!(
                    "outputs.{name} or input subscription must configure an output prefix or suffix when using the same Redis server, to avoid Pub/Sub echo loops"
                );
            }
        }
    }
    Ok(())
}

fn validate_redis(redis: &RedisConfig) -> Result<()> {
    if redis.database < 0 {
        bail!("Redis database IDs must not be negative");
    }
    if redis.host.is_empty() {
        bail!("Redis hosts must not be empty");
    }
    if redis.connect_timeout_ms == 0 {
        bail!("Redis connect_timeout_ms must be greater than zero");
    }
    if redis.connect_timeout_ms > MAX_RUNTIME_DURATION_MS as u64 {
        bail!("Redis connect_timeout_ms must not exceed {MAX_RUNTIME_DURATION_MS} ms");
    }
    Ok(())
}

fn validate_runtime_duration_ms(path: &str, value: i64) -> Result<()> {
    if value > MAX_RUNTIME_DURATION_MS {
        bail!("{path} must not exceed {MAX_RUNTIME_DURATION_MS} ms");
    }
    Ok(())
}

fn validate_deduplication_override(
    output: &OutputConfig,
    path: &str,
    ttl_ms: Option<i64>,
    group: Option<&str>,
) -> Result<()> {
    if ttl_ms.is_some() && group.is_some() {
        bail!("{path} must not specify both deduplication.ttl_ms and deduplication.group");
    }
    if let Some(ttl_ms) = ttl_ms {
        validate_runtime_duration_ms(&format!("{path}.deduplication.ttl_ms"), ttl_ms)?;
    }
    if let Some(group) = group {
        if group.trim().is_empty() {
            bail!("{path}.deduplication.group must not be empty");
        }
        if !output.deduplication_groups.contains_key(group) {
            bail!("{path} references unknown deduplication group {group:?}");
        }
    }
    Ok(())
}

fn validate_filter_rules(path: &str, filters: &[ChannelFilterRule]) -> Result<()> {
    for (index, filter) in filters.iter().enumerate() {
        let rule_path = format!("{path}[{index}]");
        let selectors = [
            ("glob", filter.glob.as_deref()),
            ("regex", filter.regex.as_deref()),
            ("raw", filter.raw.as_deref()),
            ("single", filter.single.as_deref()),
            ("string", filter.string.as_deref()),
        ];
        let selector_count = selectors
            .iter()
            .filter(|(_, selector)| selector.is_some())
            .count();
        if selector_count != 1 {
            bail!("{rule_path} must specify exactly one of glob, regex, raw, single, or string");
        }
        for (selector_name, selector) in selectors
            .into_iter()
            .filter_map(|(name, value)| value.map(|value| (name, value)))
        {
            if selector.trim().is_empty() {
                bail!("{rule_path}.{selector_name} must not be empty");
            }
            if selector_name == "regex" {
                regex_automata::meta::Regex::new(selector)
                    .map_err(|error| anyhow::anyhow!("{rule_path}.regex is invalid: {error}"))?;
            }
        }
    }
    Ok(())
}
