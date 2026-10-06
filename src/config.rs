use std::collections::BTreeMap;
use std::{env, fs, path::PathBuf, time::Duration};

use anyhow::{Context, Result, bail};
use redis::Client;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use url::Url;

pub const MAX_RUNTIME_DURATION_MS: i64 = 31_536_000_000;
pub const MAX_DEDUPLICATION_GROUP_MEMBERS: usize = 100_000;
pub const MAX_DEDUPLICATION_GROUP_CACHE_BYTES: usize = 256 * 1024 * 1024;

#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AppConfig {
    /// Default encoded RESP request size target for each output EXEC.
    #[serde(default = "default_max_bytes_per_exec")]
    #[schemars(range(min = 1))]
    pub max_bytes_per_exec: usize,
    /// Policy for single PUBLISH requests that exceed their output byte target.
    #[serde(default)]
    pub oversized_message_policy: OversizedMessagePolicy,
    pub input: InputConfig,
    #[schemars(length(min = 1))]
    pub outputs: BTreeMap<String, OutputConfig>,
    pub instance_lock: InstanceLockConfig,
    #[serde(default)]
    pub status: StatusConfig,
    #[serde(default)]
    pub logging: LoggingConfig,
}

impl AppConfig {
    pub fn json_schema() -> serde_json::Value {
        let mut schema = serde_json::to_value(schemars::schema_for!(AppConfig))
            .expect("the AppConfig JSON Schema must serialize");
        schema["properties"]["outputs"]["minProperties"] = serde_json::json!(1);
        schema["properties"]["outputs"]["propertyNames"]["pattern"] = serde_json::json!("\\S");
        schema["$defs"]["InputConfig"]["properties"]["subscriptions"]["minItems"] =
            serde_json::json!(1);
        schema["$defs"]["OutputConfig"]["properties"]["deduplication_groups"]["propertyNames"]["pattern"] =
            serde_json::json!("\\S");
        schema["$defs"]["OutputConfig"]["properties"]["profiles"]["propertyNames"]["pattern"] =
            serde_json::json!("\\S");
        schema["$defs"]["OutputConfig"]["properties"]["channel_policies"]["description"] = serde_json::json!(
            "Ordered explicit-match rules followed by exactly one default catch-all rule. The first explicit match wins; the default must be last and catches the rest without merging. JSON Schema checks that one default exists; runtime validation (`--check-config`) enforces its final position."
        );
        schema["$defs"]["OutputConfig"]["allOf"] = serde_json::json!([{
            "if": {
                "required": ["channel_policies"],
                "properties": {"channel_policies": {"minItems": 1}}
            },
            "then": {
                "properties": {
                    "channel_policies": {
                        "contains": {
                            "required": ["default"],
                            "properties": {"default": {"const": true}}
                        },
                        "minContains": 1,
                        "maxContains": 1
                    }
                }
            }
        }]);
        schema["$defs"]["ChannelProfile"]["description"] = serde_json::json!(
            "Optional overrides for an output channel. A profile cannot define both deduplication.ttl_ms and deduplication.group."
        );
        schema["$defs"]["ChannelProfile"]["not"] = serde_json::json!({
            "anyOf": [{
                "required": ["deduplication.ttl_ms", "deduplication.group"],
                "properties": {
                    "deduplication.ttl_ms": {"type": "integer"},
                    "deduplication.group": {"type": "string"}
                }
            }]
        });
        schema["$defs"]["ChannelProfile"]["allOf"] = serde_json::json!([{
            "if": {
                "required": ["deduplication.group"],
                "properties": {"deduplication.group": {"type": "string"}}
            },
            "then": {
                "properties": {"deduplication.group": {"pattern": "\\S"}}
            }
        }]);
        schema["$defs"]["ChannelPolicy"]["description"] = serde_json::json!(
            "Ordered rules matched against the final mapped Pub/Sub channel. Explicit selectors match first; the unique default:true rule must be last and catches the rest without merging. A rule uses either one selector or default:true, plus either a profile or inline overrides. Runtime validation (`--check-config`) enforces that default is last."
        );
        schema["$defs"]["ChannelPolicy"]["properties"]["default"]["default"] =
            serde_json::json!(false);
        for (definition, property) in [
            ("ConflationConfig", "interval_ms"),
            ("DeduplicationConfig", "ttl_ms"),
            ("DeduplicationGroup", "ttl_ms"),
            ("ChannelProfile", "conflation.interval_ms"),
            ("ChannelProfile", "deduplication.ttl_ms"),
            ("ChannelPolicy", "conflation.interval_ms"),
            ("ChannelPolicy", "deduplication.ttl_ms"),
        ] {
            schema["$defs"][definition]["properties"][property]["maximum"] =
                serde_json::json!(MAX_RUNTIME_DURATION_MS);
        }
        schema["$defs"]["RedisConfig"]["properties"]["connect_timeout_ms"]["maximum"] =
            serde_json::json!(MAX_RUNTIME_DURATION_MS as u64);
        schema["$defs"]["StatusConfig"]["properties"]["update_interval_ms"]["maximum"] =
            serde_json::json!(MAX_RUNTIME_DURATION_MS as u64);
        schema["$defs"]["DeduplicationGroup"]["properties"]["max_members"]["maximum"] =
            serde_json::json!(MAX_DEDUPLICATION_GROUP_MEMBERS);
        schema["$defs"]["DeduplicationGroup"]["properties"]["max_cache_bytes"]["maximum"] =
            serde_json::json!(MAX_DEDUPLICATION_GROUP_CACHE_BYTES);
        schema["$defs"]["ChannelPolicy"]["allOf"] = serde_json::json!([
            {
                "oneOf": [
                    {
                        "required": ["glob"],
                        "properties": {"glob": {"type": "string", "minLength": 1, "pattern": "\\S"}},
                        "not": {"anyOf": [
                            {"required": ["prefix"], "properties": {"prefix": {"type": "string"}}},
                            {"required": ["suffix"], "properties": {"suffix": {"type": "string"}}},
                            {"required": ["default"], "properties": {"default": {"const": true}}}
                        ]}
                    },
                    {
                        "required": ["prefix"],
                        "properties": {"prefix": {"type": "string", "minLength": 1, "pattern": "\\S"}},
                        "not": {"anyOf": [
                            {"required": ["glob"], "properties": {"glob": {"type": "string"}}},
                            {"required": ["suffix"], "properties": {"suffix": {"type": "string"}}},
                            {"required": ["default"], "properties": {"default": {"const": true}}}
                        ]}
                    },
                    {
                        "required": ["suffix"],
                        "properties": {"suffix": {"type": "string", "minLength": 1, "pattern": "\\S"}},
                        "not": {"anyOf": [
                            {"required": ["glob"], "properties": {"glob": {"type": "string"}}},
                            {"required": ["prefix"], "properties": {"prefix": {"type": "string"}}},
                            {"required": ["default"], "properties": {"default": {"const": true}}}
                        ]}
                    },
                    {
                        "required": ["default"],
                        "properties": {"default": {"const": true}},
                        "not": {"anyOf": [
                            {"required": ["glob"], "properties": {"glob": {"type": "string"}}},
                            {"required": ["prefix"], "properties": {"prefix": {"type": "string"}}},
                            {"required": ["suffix"], "properties": {"suffix": {"type": "string"}}}
                        ]}
                    }
                ]
            },
            {
                "oneOf": [
                    {
                        "required": ["profile"],
                        "properties": {"profile": {"type": "string", "minLength": 1, "pattern": "\\S"}},
                        "not": {"anyOf": [
                            {"required": ["conflation.interval_ms"], "properties": {"conflation.interval_ms": {"type": "integer"}}},
                            {"required": ["deduplication.ttl_ms"], "properties": {"deduplication.ttl_ms": {"type": "integer"}}},
                            {"required": ["deduplication.group"], "properties": {"deduplication.group": {"type": "string"}}}
                        ]}
                    },
                    {
                        "not": {"required": ["profile"], "properties": {"profile": {"type": "string"}}},
                        "anyOf": [
                            {"required": ["conflation.interval_ms"], "properties": {"conflation.interval_ms": {"type": "integer"}}},
                            {"required": ["deduplication.ttl_ms"], "properties": {"deduplication.ttl_ms": {"type": "integer"}}},
                            {"required": ["deduplication.group"], "properties": {"deduplication.group": {"type": "string", "minLength": 1, "pattern": "\\S"}}}
                        ]
                    }
                ]
            },
            {
                "not": {"anyOf": [{
                    "required": ["deduplication.ttl_ms", "deduplication.group"],
                    "properties": {
                        "deduplication.ttl_ms": {"type": "integer"},
                        "deduplication.group": {"type": "string"}
                    }
                }]}
            }
        ]);
        schema
    }

    pub fn load(path: &std::path::Path) -> Result<Self> {
        let text = fs::read_to_string(path)?;
        Ok(serde_json::from_str(&text)?)
    }

    pub fn validate(&self) -> Result<()> {
        if self.max_bytes_per_exec == 0 {
            bail!("max_bytes_per_exec must be greater than zero");
        }
        if self.input.subscriptions.is_empty() {
            bail!("input.subscriptions must contain at least one subscription");
        }
        if self.outputs.is_empty() {
            bail!("outputs must contain at least one output");
        }
        if self.status.path.is_some() && self.status.update_interval_ms == 0 {
            bail!("status.update_interval_ms must be greater than zero");
        }
        if let Some(http) = &self.status.http {
            if http.bind.is_empty() {
                bail!("status.http.bind must not be empty");
            }
            if http.port == 0 {
                bail!("status.http.port must be greater than zero");
            }
        }
        if self.status.update_interval_ms > MAX_RUNTIME_DURATION_MS as u64 {
            bail!("status.update_interval_ms must not exceed {MAX_RUNTIME_DURATION_MS} ms");
        }
        validate_redis(&self.input.redis)?;
        for (name, output) in &self.outputs {
            if name.trim().is_empty() {
                bail!("output names must not be empty");
            }
            validate_redis(&output.redis)?;
            if output.conflation.max_commands_per_exec == 0 {
                bail!("outputs.{name}.conflation.max_commands_per_exec must be greater than zero");
            }
            if output.conflation.max_bytes_per_exec == Some(0) {
                bail!("outputs.{name}.conflation.max_bytes_per_exec must be greater than zero");
            }
            validate_runtime_duration_ms(
                &format!("outputs.{name}.conflation.interval_ms"),
                output.conflation.interval_ms,
            )?;
            validate_runtime_duration_ms(
                &format!("outputs.{name}.deduplication.ttl_ms"),
                output.deduplication.ttl_ms,
            )?;
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
            for (profile_name, profile) in &output.profiles {
                let path = format!("outputs.{name}.profiles.{profile_name}");
                if profile_name.trim().is_empty() {
                    bail!("outputs.{name}.profiles names must not be empty");
                }
                if let Some(interval_ms) = profile.conflation_interval_ms {
                    validate_runtime_duration_ms(
                        &format!("{path}.conflation.interval_ms"),
                        interval_ms,
                    )?;
                }
                validate_deduplication_override(
                    output,
                    &path,
                    profile.deduplication_ttl_ms,
                    profile.deduplication_group.as_deref(),
                )?;
            }
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
                    validate_deduplication_override(
                        output,
                        &path,
                        policy.deduplication_ttl_ms,
                        policy.deduplication_group.as_deref(),
                    )?;
                    if let Some(interval_ms) = policy.conflation_interval_ms {
                        validate_runtime_duration_ms(
                            &format!("{path}.conflation.interval_ms"),
                            interval_ms,
                        )?;
                    }
                }
            }
        }
        if self.logging.retention_days == 0 || self.logging.max_total_size_mb == 0 {
            bail!("logging retention and size limits must be greater than zero");
        }
        for subscription in &self.input.subscriptions {
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
            for (name, output) in &self.outputs {
                if same_pubsub_server(&self.input.redis, &output.redis)
                    && self.input.exclude_output_echoes
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

pub fn same_pubsub_server(left: &RedisConfig, right: &RedisConfig) -> bool {
    left.host.eq_ignore_ascii_case(&right.host) && left.port == right.port && left.tls == right.tls
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct InputConfig {
    pub redis: RedisConfig,
    #[schemars(length(min = 1))]
    pub subscriptions: Vec<Subscription>,
    #[serde(default = "default_exclude_output_echoes")]
    pub exclude_output_echoes: bool,
    #[serde(default = "default_exclude_sentinel_pubsub")]
    pub exclude_sentinel_pubsub: bool,
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "lowercase", deny_unknown_fields)]
pub enum Subscription {
    Subscribe {
        #[schemars(length(min = 1))]
        channel: String,
        #[serde(default)]
        output_prefix: String,
        #[serde(default)]
        output_suffix: String,
    },
    Psubscribe {
        #[schemars(length(min = 1))]
        pattern: String,
        #[serde(default)]
        output_prefix: String,
        #[serde(default)]
        output_suffix: String,
    },
}

impl Subscription {
    pub fn output_mapping(&self) -> (&str, &str) {
        match self {
            Self::Subscribe {
                output_prefix,
                output_suffix,
                ..
            }
            | Self::Psubscribe {
                output_prefix,
                output_suffix,
                ..
            } => (output_prefix.as_str(), output_suffix.as_str()),
        }
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct OutputConfig {
    pub redis: RedisConfig,
    #[serde(default)]
    pub channel_prefix: String,
    #[serde(default)]
    pub channel_suffix: String,
    pub conflation: ConflationConfig,
    #[serde(default)]
    pub deduplication: DeduplicationConfig,
    /// Output-local named deduplication TTL groups.
    #[serde(default)]
    pub deduplication_groups: BTreeMap<String, DeduplicationGroup>,
    /// Named profiles reusable by channel policies.
    #[serde(default)]
    pub profiles: BTreeMap<String, ChannelProfile>,
    /// Ordered final-channel rules; explicit matches win first and the single default rule catches the rest.
    #[serde(default)]
    pub channel_policies: Vec<ChannelPolicy>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DeduplicationGroup {
    /// Deduplication TTL in milliseconds; values <= 0 disable deduplication for group members.
    pub ttl_ms: i64,
    /// Round the shared start time down to floor(unix_epoch_ms / round_ms) * round_ms. Zero disables clock rounding; positive values must not exceed a positive TTL.
    #[schemars(
        range(min = 0),
        description = "Clock rounding interval in milliseconds. Zero means no rounding; otherwise start_ms = floor(unix_epoch_ms / round_ms) * round_ms. For positive TTLs, round_ms must not exceed ttl_ms; runtime validation (`--check-config`) enforces this cross-field constraint."
    )]
    pub round_ms: u64,
    /// The shared deadline starts on the first successful or uncertain publication. When true (the default), it restarts if a changed payload is actually published or if a new group member makes its first successful or uncertain publication. Duplicate-suppressed and intermediate conflated values do not restart it. When false, the deadline remains fixed regardless of member additions or changes.
    #[serde(default = "default_restart_on_change")]
    #[schemars(
        default = "default_restart_on_change",
        description = "The shared deadline starts on the first successful or uncertain publication. When true (the default), it restarts if a changed payload is actually published or if a new group member makes its first successful or uncertain publication. Duplicate-suppressed and intermediate conflated values do not restart it. When false, the deadline remains fixed regardless of member additions or changes."
    )]
    pub restart_on_change: bool,
    /// Maximum number of cached channels in this group; least-recently-used entries are evicted at capacity.
    #[serde(default = "default_group_max_members")]
    #[schemars(
        default = "default_group_max_members",
        range(min = 1),
        description = "Maximum cached group channels. At capacity, the least-recently-used member is evicted; default is 16384."
    )]
    pub max_members: usize,
    /// Maximum combined bytes of cached channel names and raw payloads in this group.
    #[serde(default = "default_group_max_cache_bytes")]
    #[schemars(
        default = "default_group_max_cache_bytes",
        range(min = 1),
        description = "Maximum combined bytes of cached channel names and raw payloads; default is 67108864."
    )]
    pub max_cache_bytes: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(
    description = "Optional channel-policy overrides. A profile cannot specify both deduplication.ttl_ms and deduplication.group."
)]
pub struct ChannelProfile {
    /// Optional conflation interval override; nonpositive values use direct mode.
    #[serde(
        rename = "conflation.interval_ms",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(rename = "conflation.interval_ms")]
    pub conflation_interval_ms: Option<i64>,
    /// Optional per-channel TTL override; nonpositive values disable deduplication.
    #[serde(
        rename = "deduplication.ttl_ms",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(rename = "deduplication.ttl_ms")]
    pub deduplication_ttl_ms: Option<i64>,
    /// Optional output-local deduplication group; null or absent uses per-channel TTL behavior.
    #[serde(
        rename = "deduplication.group",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(rename = "deduplication.group", length(min = 1))]
    pub deduplication_group: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(
    description = "A rule has exactly one nonempty selector or default:true, plus either one profile reference or at least one inline override. The unique default rule must be last when policies exist."
)]
pub struct ChannelPolicy {
    /// Catch-all mode; true selects the sole default rule, which must be last when policies exist.
    #[serde(default, skip_serializing_if = "is_false")]
    #[schemars(default = "default_policy_is_default")]
    pub default: bool,
    /// Glob selector matched against the final mapped output Pub/Sub channel.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = 1))]
    pub glob: Option<String>,
    /// Prefix selector matched against the final mapped output Pub/Sub channel.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = 1))]
    pub prefix: Option<String>,
    /// Suffix selector matched against the final mapped output Pub/Sub channel.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = 1))]
    pub suffix: Option<String>,
    /// Named profile in this output's profiles map; cannot be combined with inline overrides.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = 1))]
    pub profile: Option<String>,
    /// Conflation interval override in milliseconds; nonpositive values use direct mode. Missing or null means no override.
    #[serde(
        rename = "conflation.interval_ms",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(rename = "conflation.interval_ms")]
    pub conflation_interval_ms: Option<i64>,
    /// Deduplication TTL override in milliseconds; nonpositive values disable only deduplication. Missing or null means no override.
    #[serde(
        rename = "deduplication.ttl_ms",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(rename = "deduplication.ttl_ms")]
    pub deduplication_ttl_ms: Option<i64>,
    /// Output-local deduplication group. Missing or null uses per-channel TTL behavior.
    #[serde(
        rename = "deduplication.group",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(rename = "deduplication.group", length(min = 1))]
    pub deduplication_group: Option<String>,
}

impl ChannelPolicy {
    fn has_inline_overrides(&self) -> bool {
        self.conflation_interval_ms.is_some()
            || self.deduplication_ttl_ms.is_some()
            || self.deduplication_group.is_some()
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct DeduplicationConfig {
    /// Suppress identical mapped output channels and raw incoming payload bytes for this many milliseconds; nonpositive values disable deduplication.
    #[serde(default = "default_deduplication_ttl_ms")]
    #[schemars(
        default = "default_deduplication_ttl_ms",
        description = "Deduplication TTL in milliseconds; 5000 by default and values <= 0 disable deduplication."
    )]
    pub ttl_ms: i64,
}

impl Default for DeduplicationConfig {
    fn default() -> Self {
        Self {
            ttl_ms: default_deduplication_ttl_ms(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RedisConfig {
    #[schemars(length(min = 1))]
    pub host: String,
    #[serde(default = "default_port")]
    #[schemars(range(min = 1))]
    pub port: u16,
    pub username: Option<String>,
    pub password_env: Option<String>,
    #[serde(default)]
    pub tls: bool,
    #[serde(default)]
    #[schemars(range(min = 0))]
    pub database: i64,
    #[serde(default = "default_connect_timeout_ms")]
    #[schemars(range(min = 1))]
    pub connect_timeout_ms: u64,
}

impl RedisConfig {
    pub fn client(&self) -> Result<Client> {
        let host = if self.host.contains(':') && !self.host.starts_with('[') {
            format!("[{}]", self.host)
        } else {
            self.host.clone()
        };
        let scheme = if self.tls { "rediss" } else { "redis" };
        let mut url = Url::parse(&format!(
            "{scheme}://{host}:{}/{}",
            self.port, self.database
        ))?;

        if let Some(username) = &self.username {
            url.set_username(username)
                .map_err(|()| anyhow::anyhow!("invalid Redis username"))?;
        }
        if let Some(variable) = &self.password_env {
            let password = env::var(variable)
                .with_context(|| format!("environment variable {variable} is not set"))?;
            url.set_password(Some(&password))
                .map_err(|()| anyhow::anyhow!("invalid Redis password"))?;
        }
        Client::open(url.as_str()).context("invalid Redis connection settings")
    }

    pub fn connect_timeout(&self) -> Duration {
        Duration::from_millis(self.connect_timeout_ms)
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ConflationConfig {
    pub interval_ms: i64,
    #[serde(default = "default_max_commands_per_exec")]
    #[schemars(range(min = 1))]
    pub max_commands_per_exec: usize,
    /// Optional override for the global encoded RESP request size target.
    #[serde(default)]
    #[schemars(range(min = 1))]
    pub max_bytes_per_exec: Option<usize>,
    #[serde(default)]
    pub oversized_message_policy: Option<OversizedMessagePolicy>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum OversizedMessagePolicy {
    /// Send the original PUBLISH as a singleton even if it exceeds the byte target.
    #[default]
    Send,
    /// Remove trailing payload bytes until the encoded request fits the byte target.
    Truncate,
    /// Drop this output's oversized message.
    Drop,
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct InstanceLockConfig {
    pub path: PathBuf,
}

impl InstanceLockConfig {
    pub fn acquire(&self) -> Result<fs::File> {
        use fs2::FileExt;
        use std::fs::OpenOptions;

        if let Some(parent) = self
            .path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create lock directory {}", parent.display()))?;
        }
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&self.path)
            .with_context(|| format!("failed to open lock file {}", self.path.display()))?;
        file.try_lock_exclusive().with_context(|| {
            format!(
                "another instance is already running (lock file: {})",
                self.path.display()
            )
        })?;
        Ok(file)
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StatusConfig {
    pub path: Option<PathBuf>,
    #[serde(default = "default_status_interval_ms")]
    pub update_interval_ms: u64,
    #[serde(default)]
    pub http: Option<HttpStatusConfig>,
}

impl Default for StatusConfig {
    fn default() -> Self {
        Self {
            path: None,
            update_interval_ms: default_status_interval_ms(),
            http: None,
        }
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct HttpStatusConfig {
    #[serde(default = "default_http_bind")]
    #[schemars(length(min = 1))]
    pub bind: String,
    #[serde(default = "default_http_port")]
    #[schemars(range(min = 1))]
    pub port: u16,
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct LoggingConfig {
    pub directory: PathBuf,
    pub level: String,
    #[schemars(range(min = 1))]
    pub retention_days: u64,
    #[schemars(range(min = 1))]
    pub max_total_size_mb: u64,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            directory: PathBuf::from("logs"),
            level: "info".to_owned(),
            retention_days: 14,
            max_total_size_mb: 1024,
        }
    }
}

fn default_port() -> u16 {
    6379
}

fn default_connect_timeout_ms() -> u64 {
    5000
}

fn default_status_interval_ms() -> u64 {
    1000
}

fn default_max_commands_per_exec() -> usize {
    256
}

fn default_max_bytes_per_exec() -> usize {
    4 * 1024 * 1024
}

fn default_deduplication_ttl_ms() -> i64 {
    5000
}

fn default_restart_on_change() -> bool {
    true
}

fn default_group_max_members() -> usize {
    16_384
}

fn default_group_max_cache_bytes() -> usize {
    64 * 1024 * 1024
}

fn default_policy_is_default() -> bool {
    false
}

fn is_false(value: &bool) -> bool {
    !*value
}

fn default_exclude_output_echoes() -> bool {
    true
}

fn default_exclude_sentinel_pubsub() -> bool {
    true
}

fn default_http_bind() -> String {
    "127.0.0.1".to_owned()
}

fn default_http_port() -> u16 {
    9090
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn example_config_uses_one_input_and_named_outputs() {
        let config: AppConfig =
            serde_json::from_str(include_str!("../config.example.json")).unwrap();
        config.validate().unwrap();

        assert_eq!(config.input.redis.host, "localhost");
        assert_eq!(config.input.redis.database, 0);
        assert!(config.input.exclude_output_echoes);
        assert!(config.input.exclude_sentinel_pubsub);
        assert!(matches!(
            config.input.subscriptions.as_slice(),
            [Subscription::Psubscribe { pattern, output_prefix, output_suffix }]
                if pattern == "*" && output_prefix.is_empty() && output_suffix.is_empty()
        ));
        assert_eq!(config.outputs.len(), 2);
        assert_eq!(config.outputs["output0"].redis.host, "localhost");
        assert_eq!(config.outputs["output0"].redis.database, 0);
        assert!(config.outputs["output0"].deduplication_groups.is_empty());
        assert!(config.outputs["output0"].profiles.is_empty());
        assert!(config.outputs["output0"].channel_policies.is_empty());
        assert_eq!(config.outputs["output0"].conflation.interval_ms, 0);
        assert_eq!(config.outputs["output0"].deduplication.ttl_ms, 5000);
        assert!(config.outputs["output0"].channel_policies.is_empty());
        assert_eq!(
            config.outputs["output0"].conflation.max_commands_per_exec,
            256
        );
        assert_eq!(config.max_bytes_per_exec, 4 * 1024 * 1024);
        assert_eq!(
            config.oversized_message_policy,
            OversizedMessagePolicy::Send
        );
        assert_eq!(
            config.outputs["output0"].conflation.max_bytes_per_exec,
            None
        );
        assert_eq!(config.outputs["output0"].channel_prefix, "db0:");
        assert_eq!(config.outputs["output1"].redis.host, "localhost");
        assert_eq!(config.outputs["output1"].redis.database, 1);
        assert_eq!(config.outputs["output1"].conflation.interval_ms, 250);
        assert_eq!(
            config.outputs["output1"].conflation.max_commands_per_exec,
            256
        );
        assert_eq!(
            config.outputs["output1"].conflation.max_bytes_per_exec,
            Some(2 * 1024 * 1024)
        );
        assert_eq!(
            config.outputs["output1"]
                .conflation
                .oversized_message_policy,
            Some(OversizedMessagePolicy::Send)
        );
        assert_eq!(config.outputs["output1"].channel_prefix, "db1:");
        assert!(config.status.path.is_none());
        assert!(config.status.http.is_some());
    }

    #[test]
    fn input_exclusion_options_default_safely_and_can_be_configured() {
        let mut config: AppConfig = serde_json::from_str(
            r#"{
                "input":{"redis":{"host":"input.example.net"},"subscriptions":[{"type":"psubscribe","pattern":"*"}]},
                "outputs":{"output1":{"redis":{"host":"output.example.net"},"conflation":{"interval_ms":10}}},
                "instance_lock":{"path":"lock"}
            }"#,
        )
        .unwrap();

        assert!(config.input.exclude_output_echoes);
        assert!(config.input.exclude_sentinel_pubsub);
        config.input.exclude_output_echoes = false;
        config.input.exclude_sentinel_pubsub = false;
        config.validate().unwrap();
    }

    #[test]
    fn disabling_output_echo_exclusion_allows_unmapped_same_server_output() {
        let mut config: AppConfig = serde_json::from_str(
            r#"{
                "input":{"redis":{"host":"localhost"},"subscriptions":[{"type":"psubscribe","pattern":"*"}]},
                "outputs":{"output1":{"redis":{"host":"localhost"},"conflation":{"interval_ms":0}}},
                "instance_lock":{"path":"lock"}
            }"#,
        )
        .unwrap();

        assert!(config.validate().is_err());
        config.input.exclude_output_echoes = false;
        config.validate().unwrap();
    }

    #[test]
    fn same_pubsub_endpoint_requires_namespace_per_subscription_across_database_ids() {
        let mut config: AppConfig = serde_json::from_str(
            r#"{
                "input":{"redis":{"host":"localhost","database":0},"subscriptions":[{"type":"subscribe","channel":"events","output_prefix":"","output_suffix":""}]},
                "outputs":{"output1":{"redis":{"host":"localhost","database":7},"conflation":{"interval_ms":10}}},
                "instance_lock":{"path":"lock"}
            }"#,
        )
        .unwrap();

        let error = config.validate().unwrap_err();
        assert!(error.to_string().contains("outputs.output1"));

        if let Subscription::Subscribe { output_suffix, .. } = &mut config.input.subscriptions[0] {
            *output_suffix = ":copy".to_owned();
        }
        config.validate().unwrap();

        if let Subscription::Subscribe { output_suffix, .. } = &mut config.input.subscriptions[0] {
            output_suffix.clear();
        }
        config.outputs.get_mut("output1").unwrap().channel_prefix = "copy:".to_owned();
        config.validate().unwrap();
    }

    #[test]
    fn same_server_outputs_each_require_a_composed_namespace() {
        let mut config: AppConfig = serde_json::from_str(
            r#"{
                "input":{"redis":{"host":"localhost"},"subscriptions":[{"type":"psubscribe","pattern":"*","output_prefix":"","output_suffix":""}]},
                "outputs":{
                    "output0":{"redis":{"host":"localhost","database":0},"channel_prefix":"db0:","conflation":{"interval_ms":0}},
                    "output1":{"redis":{"host":"localhost","database":1},"conflation":{"interval_ms":250}}
                },
                "instance_lock":{"path":"lock"}
            }"#,
        )
        .unwrap();

        let error = config.validate().unwrap_err();
        assert!(error.to_string().contains("outputs.output1"));
        config.outputs.get_mut("output1").unwrap().channel_prefix = "db1:".to_owned();
        config.validate().unwrap();
    }

    #[test]
    fn different_pubsub_endpoints_do_not_require_output_namespace() {
        let config: AppConfig = serde_json::from_str(
            r#"{
                "input":{"redis":{"host":"input.example.net"},"subscriptions":[{"type":"subscribe","channel":"events","output_prefix":"","output_suffix":""}]},
                "outputs":{"output1":{"redis":{"host":"output.example.net"},"conflation":{"interval_ms":10}}},
                "instance_lock":{"path":"lock"}
            }"#,
        )
        .unwrap();

        config.validate().unwrap();
        assert!(config.status.path.is_none());
        assert!(config.status.http.is_none());
        assert!(config.outputs["output1"].channel_prefix.is_empty());
        assert!(config.outputs["output1"].channel_suffix.is_empty());
    }

    #[test]
    fn output_conflation_interval_accepts_signed_values() {
        let config: AppConfig = serde_json::from_str(
            r#"{
                "input":{"redis":{"host":"input.example.net"},"subscriptions":[{"type":"subscribe","channel":"events","output_prefix":"","output_suffix":""}]},
                "outputs":{"custom-output":{"redis":{"host":"output.example.net"},"conflation":{"interval_ms":-25}}},
                "instance_lock":{"path":"lock"}
            }"#,
        )
        .unwrap();

        config.validate().unwrap();
        assert_eq!(config.outputs["custom-output"].conflation.interval_ms, -25);
    }

    #[test]
    fn output_deduplication_ttl_defaults_and_accepts_override_or_nonpositive_disable() {
        let default_config: AppConfig = serde_json::from_str(
            r#"{
                "input":{"redis":{"host":"input.example.net"},"subscriptions":[{"type":"subscribe","channel":"events"}]},
                "outputs":{"custom-output":{"redis":{"host":"output.example.net"},"conflation":{"interval_ms":0}}},
                "instance_lock":{"path":"lock"}
            }"#,
        )
        .unwrap();
        assert_eq!(
            default_config.outputs["custom-output"].deduplication.ttl_ms,
            5000
        );

        let override_config: AppConfig = serde_json::from_str(
            r#"{
                "input":{"redis":{"host":"input.example.net"},"subscriptions":[{"type":"subscribe","channel":"events"}]},
                "outputs":{
                    "enabled":{"redis":{"host":"output.example.net"},"conflation":{"interval_ms":0},"deduplication":{"ttl_ms":1250}},
                    "disabled":{"redis":{"host":"another.example.net"},"conflation":{"interval_ms":10},"deduplication":{"ttl_ms":0}},
                    "negative":{"redis":{"host":"third.example.net"},"conflation":{"interval_ms":10},"deduplication":{"ttl_ms":-25}}
                },
                "instance_lock":{"path":"lock"}
            }"#,
        )
        .unwrap();
        assert_eq!(
            override_config.outputs["enabled"].deduplication.ttl_ms,
            1250
        );
        assert_eq!(override_config.outputs["disabled"].deduplication.ttl_ms, 0);
        assert_eq!(
            override_config.outputs["negative"].deduplication.ttl_ms,
            -25
        );
    }

    #[test]
    fn channel_profiles_policies_and_groups_use_literal_dotted_keys() {
        let config: AppConfig = serde_json::from_str(
            r#"{
                "input":{"redis":{"host":"input.example.net"},"subscriptions":[{"type":"subscribe","channel":"events"}]},
                "outputs":{"custom-output":{
                    "redis":{"host":"output.example.net"},
                    "conflation":{"interval_ms":10},
                    "deduplication_groups":{
                        "workers":{"ttl_ms":3000,"round_ms":3000},
                        "disabled":{"ttl_ms":0,"round_ms":9000,"restart_on_change":false}
                    },
                    "profiles":{
                        "grouped":{"conflation.interval_ms":-25,"deduplication.group":"workers"},
                        "no-dedup":{"deduplication.ttl_ms":0}
                    },
                    "channel_policies":[
                        {"glob":"orders:*","profile":"grouped"},
                        {"prefix":"critical:","profile":null,"conflation.interval_ms":0,"deduplication.group":"workers"},
                        {"suffix":":raw","deduplication.ttl_ms":-10,"deduplication.group":null},
                        {"default":true,"profile":"no-dedup"}
                    ]
                }},
                "instance_lock":{"path":"lock"}
            }"#,
        )
        .unwrap();

        config.validate().unwrap();
        let output = &config.outputs["custom-output"];
        assert_eq!(output.deduplication_groups["workers"].ttl_ms, 3000);
        assert_eq!(output.deduplication_groups["workers"].round_ms, 3000);
        assert!(output.deduplication_groups["workers"].restart_on_change);
        assert_eq!(output.deduplication_groups["workers"].max_members, 16_384);
        assert_eq!(
            output.deduplication_groups["workers"].max_cache_bytes,
            64 * 1024 * 1024
        );
        assert_eq!(output.deduplication_groups["disabled"].ttl_ms, 0);
        assert_eq!(output.deduplication_groups["disabled"].round_ms, 9000);
        assert!(!output.deduplication_groups["disabled"].restart_on_change);
        assert_eq!(output.profiles["grouped"].conflation_interval_ms, Some(-25));
        assert_eq!(
            output.profiles["grouped"].deduplication_group.as_deref(),
            Some("workers")
        );
        assert_eq!(output.profiles["no-dedup"].deduplication_ttl_ms, Some(0));

        let policies = &output.channel_policies;
        assert_eq!(policies.len(), 4);
        assert_eq!(policies[0].glob.as_deref(), Some("orders:*"));
        assert_eq!(policies[0].profile.as_deref(), Some("grouped"));
        assert_eq!(policies[1].prefix.as_deref(), Some("critical:"));
        assert_eq!(policies[1].profile, None);
        assert_eq!(policies[1].conflation_interval_ms, Some(0));
        assert_eq!(policies[1].deduplication_group.as_deref(), Some("workers"));
        assert_eq!(policies[2].suffix.as_deref(), Some(":raw"));
        assert_eq!(policies[2].deduplication_ttl_ms, Some(-10));
        assert_eq!(policies[2].deduplication_group, None);
        assert!(policies[3].default);
        assert_eq!(policies[3].profile.as_deref(), Some("no-dedup"));

        assert_eq!(
            serde_json::to_value(&policies[0]).unwrap(),
            serde_json::json!({"glob": "orders:*", "profile": "grouped"})
        );
        assert_eq!(
            serde_json::to_value(&policies[3]).unwrap(),
            serde_json::json!({"default": true, "profile": "no-dedup"})
        );
        assert_eq!(
            serde_json::to_value(&output.profiles["grouped"]).unwrap(),
            serde_json::json!({
                "conflation.interval_ms": -25,
                "deduplication.group": "workers"
            })
        );

        assert!(
            serde_json::from_str::<ChannelProfile>(r#"{"conflation":{"interval_ms":25}}"#).is_err()
        );
        assert!(
            serde_json::from_str::<ChannelPolicy>(
                r#"{"glob":"orders:*","deduplication":{"ttl_ms":25}}"#
            )
            .is_err()
        );
    }

    #[test]
    fn deduplication_group_rounding_is_bounded_by_positive_ttl() {
        let mut config: AppConfig = serde_json::from_str(
            r#"{
                "input":{"redis":{"host":"input.example.net"},"subscriptions":[{"type":"subscribe","channel":"events"}]},
                "outputs":{"custom-output":{
                    "redis":{"host":"output.example.net"},
                    "conflation":{"interval_ms":10},
                    "deduplication_groups":{
                        "equal":{"ttl_ms":50,"round_ms":50},
                        "unrounded":{"ttl_ms":25,"round_ms":0},
                        "disabled-zero":{"ttl_ms":0,"round_ms":10000},
                        "disabled-negative":{"ttl_ms":-5,"round_ms":10000}
                    }
                }},
                "instance_lock":{"path":"lock"}
            }"#,
        )
        .unwrap();
        config.validate().unwrap();

        config
            .outputs
            .get_mut("custom-output")
            .unwrap()
            .deduplication_groups
            .get_mut("equal")
            .unwrap()
            .round_ms = 51;
        let error = config.validate().unwrap_err();
        assert!(
            error
                .to_string()
                .contains("round_ms must not exceed a positive ttl_ms")
        );

        config
            .outputs
            .get_mut("custom-output")
            .unwrap()
            .deduplication_groups
            .get_mut("equal")
            .unwrap()
            .ttl_ms = 0;
        config.validate().unwrap();

        config
            .outputs
            .get_mut("custom-output")
            .unwrap()
            .deduplication_groups
            .get_mut("equal")
            .unwrap()
            .ttl_ms = -1;
        config.validate().unwrap();

        config
            .outputs
            .get_mut("custom-output")
            .unwrap()
            .deduplication_groups
            .get_mut("equal")
            .unwrap()
            .max_members = 0;
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("max_members must be greater than zero")
        );
        config
            .outputs
            .get_mut("custom-output")
            .unwrap()
            .deduplication_groups
            .get_mut("equal")
            .unwrap()
            .max_members = MAX_DEDUPLICATION_GROUP_MEMBERS + 1;
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("max_members must not exceed")
        );
        let group = config
            .outputs
            .get_mut("custom-output")
            .unwrap()
            .deduplication_groups
            .get_mut("equal")
            .unwrap();
        group.max_members = default_group_max_members();
        group.max_cache_bytes = MAX_DEDUPLICATION_GROUP_CACHE_BYTES + 1;
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("max_cache_bytes must not exceed")
        );
        assert!(
            serde_json::from_str::<DeduplicationGroup>(r#"{"ttl_ms":25,"round_ms":-1}"#).is_err()
        );
    }

    #[test]
    fn configured_runtime_durations_are_limited_to_avoid_instant_overflow() {
        let mut config: AppConfig = serde_json::from_str(
            r#"{
                "input":{"redis":{"host":"input.example.net"},"subscriptions":[{"type":"subscribe","channel":"events"}]},
                "outputs":{"custom-output":{"redis":{"host":"output.example.net"},"conflation":{"interval_ms":0}}},
                "instance_lock":{"path":"lock"}
            }"#,
        )
        .unwrap();
        config.validate().unwrap();

        config
            .outputs
            .get_mut("custom-output")
            .unwrap()
            .conflation
            .interval_ms = MAX_RUNTIME_DURATION_MS + 1;
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("interval_ms")
        );
        config
            .outputs
            .get_mut("custom-output")
            .unwrap()
            .conflation
            .interval_ms = 0;
        config
            .outputs
            .get_mut("custom-output")
            .unwrap()
            .deduplication
            .ttl_ms = MAX_RUNTIME_DURATION_MS + 1;
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("ttl_ms")
        );
        config
            .outputs
            .get_mut("custom-output")
            .unwrap()
            .deduplication
            .ttl_ms = 5000;
        config.input.redis.connect_timeout_ms = MAX_RUNTIME_DURATION_MS as u64 + 1;
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("connect_timeout_ms")
        );
        config.input.redis.connect_timeout_ms = 5000;
        config.status.path = Some(PathBuf::from("status.json"));
        config.status.update_interval_ms = MAX_RUNTIME_DURATION_MS as u64 + 1;
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("update_interval_ms")
        );
    }

    #[test]
    fn channel_policy_validates_selectors_modes_and_output_local_references() {
        let mut config: AppConfig = serde_json::from_str(
            r#"{
                "input":{"redis":{"host":"input.example.net"},"subscriptions":[{"type":"subscribe","channel":"events"}]},
                "outputs":{"custom-output":{
                    "redis":{"host":"output.example.net"},
                    "conflation":{"interval_ms":10},
                    "deduplication_groups":{"workers":{"ttl_ms":-1,"round_ms":0}},
                    "profiles":{"grouped":{"deduplication.group":"workers"}},
                    "channel_policies":[
                        {"glob":"orders:*","profile":"grouped"},
                        {"default":true,"profile":"grouped"}
                    ]
                }},
                "instance_lock":{"path":"lock"}
            }"#,
        )
        .unwrap();
        config.validate().unwrap();

        config
            .outputs
            .get_mut("custom-output")
            .unwrap()
            .channel_policies[1]
            .default = false;
        let error = config.validate().unwrap_err();
        assert!(error.to_string().contains("exactly one default rule"));

        {
            let policies = &mut config
                .outputs
                .get_mut("custom-output")
                .unwrap()
                .channel_policies;
            policies[0].default = true;
            policies[1].default = false;
        }
        let error = config.validate().unwrap_err();
        assert!(error.to_string().contains("default rule must be last"));

        {
            let policies = &mut config
                .outputs
                .get_mut("custom-output")
                .unwrap()
                .channel_policies;
            policies[0].default = false;
            policies[1].default = true;
            policies[1].prefix = Some("catch-all:selector".to_owned());
        }
        let error = config.validate().unwrap_err();
        assert!(error.to_string().contains("default rule must not specify"));
        config
            .outputs
            .get_mut("custom-output")
            .unwrap()
            .channel_policies[1]
            .prefix = None;
        config
            .outputs
            .get_mut("custom-output")
            .unwrap()
            .channel_policies[0]
            .default = true;
        let error = config.validate().unwrap_err();
        assert!(error.to_string().contains("exactly one default rule"));
        config
            .outputs
            .get_mut("custom-output")
            .unwrap()
            .channel_policies[0]
            .default = false;

        let policy = &mut config
            .outputs
            .get_mut("custom-output")
            .unwrap()
            .channel_policies[0];
        policy.prefix = Some("orders:".to_owned());
        let error = config.validate().unwrap_err();
        assert!(error.to_string().contains("exactly one"));

        let policy = &mut config
            .outputs
            .get_mut("custom-output")
            .unwrap()
            .channel_policies[0];
        policy.prefix = None;
        policy.glob = Some("  ".to_owned());
        let error = config.validate().unwrap_err();
        assert!(error.to_string().contains("selector must not be empty"));

        let policy = &mut config
            .outputs
            .get_mut("custom-output")
            .unwrap()
            .channel_policies[0];
        policy.glob = Some("orders:*".to_owned());
        policy.profile = Some("grouped".to_owned());
        policy.glob = None;
        let error = config.validate().unwrap_err();
        assert!(error.to_string().contains("exactly one"));

        let policy = &mut config
            .outputs
            .get_mut("custom-output")
            .unwrap()
            .channel_policies[0];
        policy.glob = Some("orders:*".to_owned());
        policy.profile = None;
        let error = config.validate().unwrap_err();
        assert!(
            error
                .to_string()
                .contains("profile or at least one inline override")
        );

        let policy = &mut config
            .outputs
            .get_mut("custom-output")
            .unwrap()
            .channel_policies[0];
        policy.profile = Some("missing".to_owned());
        let error = config.validate().unwrap_err();
        assert!(error.to_string().contains("unknown profile"));

        let policy = &mut config
            .outputs
            .get_mut("custom-output")
            .unwrap()
            .channel_policies[0];
        policy.profile = Some("grouped".to_owned());
        policy.conflation_interval_ms = Some(0);
        let error = config.validate().unwrap_err();
        assert!(
            error
                .to_string()
                .contains("either profile or inline overrides")
        );

        let profile = config
            .outputs
            .get_mut("custom-output")
            .unwrap()
            .profiles
            .get_mut("grouped")
            .unwrap();
        profile.deduplication_ttl_ms = Some(0);
        let error = config.validate().unwrap_err();
        assert!(error.to_string().contains("both deduplication.ttl_ms"));

        let profile = config
            .outputs
            .get_mut("custom-output")
            .unwrap()
            .profiles
            .get_mut("grouped")
            .unwrap();
        profile.deduplication_ttl_ms = None;
        profile.deduplication_group = Some("unknown".to_owned());
        let error = config.validate().unwrap_err();
        assert!(error.to_string().contains("unknown deduplication group"));

        let profile = config
            .outputs
            .get_mut("custom-output")
            .unwrap()
            .profiles
            .get_mut("grouped")
            .unwrap();
        profile.deduplication_group = Some("workers".to_owned());
        {
            let policy = &mut config
                .outputs
                .get_mut("custom-output")
                .unwrap()
                .channel_policies[0];
            policy.profile = None;
            policy.conflation_interval_ms = None;
            policy.deduplication_ttl_ms = Some(0);
            policy.deduplication_group = Some("workers".to_owned());
        }
        let error = config.validate().unwrap_err();
        assert!(error.to_string().contains("both deduplication.ttl_ms"));

        {
            let policy = &mut config
                .outputs
                .get_mut("custom-output")
                .unwrap()
                .channel_policies[0];
            policy.deduplication_ttl_ms = None;
            policy.deduplication_group = Some("unknown".to_owned());
        }
        let error = config.validate().unwrap_err();
        assert!(error.to_string().contains("unknown deduplication group"));

        config
            .outputs
            .get_mut("custom-output")
            .unwrap()
            .channel_policies[0]
            .deduplication_group = Some("workers".to_owned());
        config.validate().unwrap();

        let output = config.outputs.get_mut("custom-output").unwrap();
        output.profiles.insert(
            "  ".to_owned(),
            ChannelProfile {
                conflation_interval_ms: None,
                deduplication_ttl_ms: None,
                deduplication_group: None,
            },
        );
        let error = config.validate().unwrap_err();
        assert!(
            error
                .to_string()
                .contains("profiles names must not be empty")
        );

        let output = config.outputs.get_mut("custom-output").unwrap();
        output.profiles.remove("  ");
        output.deduplication_groups.insert(
            "  ".to_owned(),
            DeduplicationGroup {
                ttl_ms: 0,
                round_ms: 0,
                restart_on_change: true,
                max_members: default_group_max_members(),
                max_cache_bytes: default_group_max_cache_bytes(),
            },
        );
        let error = config.validate().unwrap_err();
        assert!(
            error
                .to_string()
                .contains("deduplication_groups names must not be empty")
        );
    }

    #[test]
    fn channel_profiles_and_policy_schema_use_literal_dotted_keys_and_signed_values() {
        let schema = AppConfig::json_schema();
        let output_properties = &schema["$defs"]["OutputConfig"]["properties"];
        assert_eq!(
            output_properties["deduplication_groups"]["propertyNames"]["pattern"],
            "\\S"
        );
        assert_eq!(
            output_properties["profiles"]["propertyNames"]["pattern"],
            "\\S"
        );
        assert_eq!(
            schema["$defs"]["DeduplicationGroup"]["properties"]["restart_on_change"]["default"],
            true
        );
        assert_eq!(
            schema["$defs"]["DeduplicationGroup"]["properties"]["restart_on_change"]["description"],
            "The shared deadline starts on the first successful or uncertain publication. When true (the default), it restarts if a changed payload is actually published or if a new group member makes its first successful or uncertain publication. Duplicate-suppressed and intermediate conflated values do not restart it. When false, the deadline remains fixed regardless of member additions or changes."
        );
        assert_eq!(
            schema["$defs"]["DeduplicationGroup"]["properties"]["max_members"]["default"],
            16_384
        );
        assert_eq!(
            schema["$defs"]["DeduplicationGroup"]["properties"]["max_cache_bytes"]["default"],
            64 * 1024 * 1024
        );
        assert_eq!(
            schema["$defs"]["DeduplicationGroup"]["properties"]["max_members"]["maximum"],
            MAX_DEDUPLICATION_GROUP_MEMBERS
        );
        assert_eq!(
            schema["$defs"]["DeduplicationGroup"]["properties"]["max_cache_bytes"]["maximum"],
            MAX_DEDUPLICATION_GROUP_CACHE_BYTES
        );
        assert_eq!(
            schema["$defs"]["DeduplicationGroup"]["properties"]["round_ms"]["minimum"],
            0
        );
        assert!(
            schema["$defs"]["DeduplicationGroup"]["properties"]["round_ms"]["description"]
                .as_str()
                .unwrap()
                .contains("runtime validation")
        );

        let output_schema = &schema["$defs"]["OutputConfig"];
        assert_eq!(
            output_schema["allOf"][0]["then"]["properties"]["channel_policies"]["minContains"],
            1
        );
        assert_eq!(
            output_schema["allOf"][0]["then"]["properties"]["channel_policies"]["maxContains"],
            1
        );
        assert!(
            output_schema["properties"]["channel_policies"]["description"]
                .as_str()
                .unwrap()
                .contains("default must be last")
        );
        assert!(
            output_schema["properties"]["channel_policies"]["description"]
                .as_str()
                .unwrap()
                .contains("runtime validation")
        );

        let profile_schema = &schema["$defs"]["ChannelProfile"];
        let profile_properties = profile_schema["properties"].as_object().unwrap();
        for name in [
            "conflation.interval_ms",
            "deduplication.ttl_ms",
            "deduplication.group",
        ] {
            assert!(profile_properties.contains_key(name), "missing {name}");
        }
        assert!(!profile_properties.contains_key("conflation"));
        assert!(!profile_properties.contains_key("deduplication"));
        assert!(profile_schema.get("not").is_some());

        let policy_schema = &schema["$defs"]["ChannelPolicy"];
        let properties = policy_schema["properties"].as_object().unwrap();

        for name in [
            "default",
            "glob",
            "prefix",
            "suffix",
            "profile",
            "conflation.interval_ms",
            "deduplication.ttl_ms",
            "deduplication.group",
        ] {
            assert!(
                properties.contains_key(name),
                "missing schema property {name}"
            );
        }
        assert!(!properties.contains_key("conflation"));
        assert!(!properties.contains_key("deduplication"));
        assert_eq!(properties["default"]["default"], false);
        assert_eq!(
            policy_schema["allOf"][0]["oneOf"].as_array().unwrap().len(),
            4
        );
        assert_eq!(
            policy_schema["allOf"][1]["oneOf"].as_array().unwrap().len(),
            2
        );

        let signed_integer_schemas = [
            &profile_properties["conflation.interval_ms"],
            &profile_properties["deduplication.ttl_ms"],
            &properties["conflation.interval_ms"],
            &properties["deduplication.ttl_ms"],
        ];
        for value_schema in signed_integer_schemas {
            assert!(value_schema.get("minimum").is_none());
            assert_eq!(value_schema["maximum"], MAX_RUNTIME_DURATION_MS);
        }
    }

    #[test]
    fn conflation_command_limit_defaults_and_must_be_positive() {
        let mut config: AppConfig = serde_json::from_str(
            r#"{
                "input":{"redis":{"host":"input.example.net"},"subscriptions":[{"type":"subscribe","channel":"events"}]},
                "outputs":{"custom-output":{"redis":{"host":"output.example.net"},"conflation":{"interval_ms":10}}},
                "instance_lock":{"path":"lock"}
            }"#,
        )
        .unwrap();

        assert_eq!(
            config.outputs["custom-output"]
                .conflation
                .max_commands_per_exec,
            256
        );
        assert_eq!(config.max_bytes_per_exec, 4 * 1024 * 1024);
        assert_eq!(
            config.oversized_message_policy,
            OversizedMessagePolicy::Send
        );
        config.validate().unwrap();

        config
            .outputs
            .get_mut("custom-output")
            .unwrap()
            .conflation
            .max_commands_per_exec = 0;
        let error = config.validate().unwrap_err();
        assert!(
            error
                .to_string()
                .contains("outputs.custom-output.conflation.max_commands_per_exec")
        );

        config
            .outputs
            .get_mut("custom-output")
            .unwrap()
            .conflation
            .max_commands_per_exec = 256;
        config
            .outputs
            .get_mut("custom-output")
            .unwrap()
            .conflation
            .max_bytes_per_exec = Some(0);
        let error = config.validate().unwrap_err();
        assert!(
            error
                .to_string()
                .contains("outputs.custom-output.conflation.max_bytes_per_exec")
        );

        config
            .outputs
            .get_mut("custom-output")
            .unwrap()
            .conflation
            .max_bytes_per_exec = None;
        config.max_bytes_per_exec = 0;
        let error = config.validate().unwrap_err();
        assert!(error.to_string().contains("max_bytes_per_exec"));
    }

    #[test]
    fn oversized_message_policy_defaults_and_rejects_unknown_values() {
        let config: AppConfig = serde_json::from_str(
            r#"{
                "input":{"redis":{"host":"input.example.net"},"subscriptions":[{"type":"subscribe","channel":"events"}]},
                "outputs":{"custom-output":{"redis":{"host":"output.example.net"},"conflation":{"interval_ms":0,"oversized_message_policy":"drop"}}},
                "instance_lock":{"path":"lock"}
            }"#,
        )
        .unwrap();

        assert_eq!(
            config.oversized_message_policy,
            OversizedMessagePolicy::Send
        );
        assert_eq!(
            config.outputs["custom-output"]
                .conflation
                .oversized_message_policy,
            Some(OversizedMessagePolicy::Drop)
        );
        config.validate().unwrap();

        assert!(serde_json::from_str::<OversizedMessagePolicy>("\"compress\"").is_err());
    }

    #[test]
    fn instance_lock_rejects_a_second_process_and_releases_on_drop() {
        let directory = tempfile::tempdir().unwrap();
        let config = InstanceLockConfig {
            path: directory.path().join("service.lock"),
        };

        let first = config.acquire().unwrap();
        assert!(config.acquire().is_err());
        drop(first);
        assert!(config.acquire().is_ok());
    }
}
