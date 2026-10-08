use std::collections::BTreeMap;
use std::{env, fs, path::PathBuf, time::Duration};

use anyhow::{Context, Result};
use redis::Client;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use url::Url;

mod schema;
mod validate;

pub const MAX_RUNTIME_DURATION_MS: i64 = 31_536_000_000;
pub const MAX_DEDUPLICATION_GROUP_MEMBERS: usize = 100_000;
pub const MAX_DEDUPLICATION_GROUP_CACHE_BYTES: usize = 256 * 1024 * 1024;
pub const MAX_DEDUPLICATION_CACHE_ENTRIES: usize = 100_000;
pub const MAX_DEDUPLICATION_CACHE_BYTES: usize = 256 * 1024 * 1024;
pub const DEFAULT_CHANNEL_CACHE_MAX_ENTRIES: usize = 16_384;
pub const MAX_CHANNEL_CACHE_MAX_ENTRIES: usize = 100_000;

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
        schema::json_schema()
    }

    pub fn load(path: &std::path::Path) -> Result<Self> {
        let text = fs::read_to_string(path)?;
        Ok(serde_json::from_str(&text)?)
    }

    pub fn validate(&self) -> Result<()> {
        validate::validate(self)
    }
}

pub fn same_pubsub_server(left: &RedisConfig, right: &RedisConfig) -> bool {
    left.host.eq_ignore_ascii_case(&right.host) && left.port == right.port && left.tls == right.tls
}

#[derive(Clone, Copy, Debug, Default, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum FilterAction {
    #[default]
    Accept,
    Deny,
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ChannelFilterRule {
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = 1))]
    pub glob: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = 1))]
    pub regex: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = 1))]
    pub raw: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = 1))]
    pub single: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = 1))]
    pub string: Option<String>,
    pub action: FilterAction,
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct InputConfig {
    pub redis: RedisConfig,
    #[schemars(length(min = 1))]
    pub subscriptions: Vec<Subscription>,
    #[serde(default)]
    #[schemars(default)]
    pub filters: Vec<ChannelFilterRule>,
    /// Capacity of this input's filter-decision LRU; zero disables caching.
    #[serde(default = "default_channel_cache_max_entries")]
    #[schemars(range(min = 0, max = MAX_CHANNEL_CACHE_MAX_ENTRIES))]
    pub filter_cache_max_entries: usize,
    #[serde(default, alias = "default_filter")]
    #[schemars(default)]
    pub filter_default: FilterAction,
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
    #[serde(default)]
    #[schemars(default)]
    pub filters: Vec<ChannelFilterRule>,
    /// Capacity of this output's filter-decision LRU; zero disables caching.
    #[serde(default = "default_channel_cache_max_entries")]
    #[schemars(range(min = 0, max = MAX_CHANNEL_CACHE_MAX_ENTRIES))]
    pub filter_cache_max_entries: usize,
    /// Capacity of this output's channel-policy resolution LRU; zero disables caching.
    #[serde(default = "default_channel_cache_max_entries")]
    #[schemars(range(min = 0, max = MAX_CHANNEL_CACHE_MAX_ENTRIES))]
    pub channel_policy_cache_max_entries: usize,
    #[serde(default, alias = "default_filter")]
    #[schemars(default)]
    pub filter_default: FilterAction,
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

/// Borrowed common view of profile and inline-policy overrides. The public
/// JSON structs remain separate to preserve their flat, dotted-key schema.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ChannelOverrides<'a> {
    pub(crate) conflation_interval_ms: Option<i64>,
    pub(crate) deduplication_ttl_ms: Option<i64>,
    pub(crate) deduplication_group: Option<&'a str>,
}

impl<'a> From<&'a ChannelProfile> for ChannelOverrides<'a> {
    fn from(profile: &'a ChannelProfile) -> Self {
        Self {
            conflation_interval_ms: profile.conflation_interval_ms,
            deduplication_ttl_ms: profile.deduplication_ttl_ms,
            deduplication_group: profile.deduplication_group.as_deref(),
        }
    }
}

impl<'a> From<&'a ChannelPolicy> for ChannelOverrides<'a> {
    fn from(policy: &'a ChannelPolicy) -> Self {
        Self {
            conflation_interval_ms: policy.conflation_interval_ms,
            deduplication_ttl_ms: policy.deduplication_ttl_ms,
            deduplication_group: policy.deduplication_group.as_deref(),
        }
    }
}

impl ChannelOverrides<'_> {
    pub(crate) fn has_any(self) -> bool {
        self.conflation_interval_ms.is_some()
            || self.deduplication_ttl_ms.is_some()
            || self.deduplication_group.is_some()
    }
}

impl ChannelPolicy {
    fn has_inline_overrides(&self) -> bool {
        ChannelOverrides::from(self).has_any()
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct DeduplicationConfig {
    /// Suppress identical mapped output channels and raw incoming payload bytes for this many milliseconds; nonpositive values disable deduplication.
    #[serde(default = "default_deduplication_ttl_ms")]
    #[schemars(
        default = "default_deduplication_ttl_ms",
        description = "Deduplication TTL in milliseconds; 0 by default and values <= 0 disable deduplication."
    )]
    pub ttl_ms: i64,
    /// Treat a direct-mode batch as published as soon as it is dispatched instead of waiting for in-flight batches on the same channel or group to settle.
    #[serde(default)]
    #[schemars(
        default,
        description = "When true, direct-mode batches are remembered for deduplication at dispatch time instead of waiting for in-flight batches on the same channel or group to settle; a definitive pre-send failure (NotSent) rolls the value back. Defaults to false."
    )]
    pub in_flight_suppression: bool,
    /// Optional maximum number of per-channel deduplication entries; the least-recently-used entry is evicted at capacity.
    #[serde(default)]
    #[schemars(
        range(min = 1),
        description = "Maximum cached per-channel deduplication entries. At capacity, the least-recently-used entry is evicted; absent or null means unlimited."
    )]
    pub max_entries: Option<usize>,
    /// Optional maximum combined bytes of cached channel names and raw payloads for per-channel deduplication.
    #[serde(default)]
    #[schemars(
        range(min = 1),
        description = "Maximum combined bytes of cached channel names and raw payloads for per-channel deduplication. At capacity, the least-recently-used entry is evicted; absent or null means unlimited."
    )]
    pub max_cache_bytes: Option<usize>,
}

impl Default for DeduplicationConfig {
    fn default() -> Self {
        Self {
            ttl_ms: default_deduplication_ttl_ms(),
            in_flight_suppression: false,
            max_entries: None,
            max_cache_bytes: None,
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
    /// Optional in-flight command window; defaults to max_commands_per_exec.
    #[serde(default)]
    #[schemars(range(min = 1))]
    pub max_in_flight_commands: Option<usize>,
    /// Optional in-flight request-byte window; defaults to the resolved max_bytes_per_exec.
    #[serde(default)]
    #[schemars(range(min = 1))]
    pub max_in_flight_bytes: Option<usize>,
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
    /// Exposes channel names and cached filter/policy decisions at GET /filters.
    #[serde(default)]
    #[schemars(default)]
    pub filters_endpoint_enabled: bool,
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct LoggingConfig {
    pub directory: PathBuf,
    pub level: String,
    /// Base file name for this service's daily logs: `{prefix}.{YYYY-MM-DD}[.{n}]`.
    #[serde(default = "default_log_prefix")]
    #[schemars(
        default = "default_log_prefix",
        length(min = 1),
        description = "Base file name for this service's daily logs: `{prefix}.{YYYY-MM-DD}[.{n}]`. Must not be empty or contain `/` or `\\`; runtime validation (`--check-config`) enforces this."
    )]
    pub prefix: String,
    /// Enables file logging. When false, no log directory or files are created and no tracing subscriber is installed.
    #[serde(default = "default_log_enabled")]
    #[schemars(
        default = "default_log_enabled",
        description = "Enables file logging. When false, no log directory or files are created and no tracing subscriber is installed, so tracing events are discarded."
    )]
    pub enabled: bool,
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
            prefix: default_log_prefix(),
            enabled: default_log_enabled(),
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

fn default_channel_cache_max_entries() -> usize {
    DEFAULT_CHANNEL_CACHE_MAX_ENTRIES
}

fn default_deduplication_ttl_ms() -> i64 {
    0
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

fn default_log_prefix() -> String {
    "redis-conflated-pubsub".to_owned()
}

fn default_log_enabled() -> bool {
    true
}

#[cfg(test)]
#[path = "../tests/unit/config.rs"]
mod tests;
