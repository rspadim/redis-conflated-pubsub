use std::collections::BTreeMap;
use std::{env, fs, path::PathBuf, time::Duration};

use anyhow::{Context, Result, bail};
use redis::{Client, ConnectionInfo};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use url::Url;

mod schema;
mod validate;

// NATS subject helpers used by `--check-config` and by the NATS adapter.
pub(crate) use validate::validate_nats_publish_subject;
#[cfg(test)]
pub(crate) use validate::validate_nats_subject;

/// The wire protocol a NATS server URL selects. `async-nats` reads these from
/// the URL scheme; grouping the schemes lets validation reject a server list
/// that mixes WebSocket and raw-TCP transports before any connection is made.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NatsTransport {
    /// `nats://`: plain Core NATS over TCP.
    Tcp,
    /// `tls://`: Core NATS over TLS.
    Tls,
    /// `ws://`: Core NATS over WebSocket.
    WebSocket,
    /// `wss://`: Core NATS over WebSocket with TLS.
    WebSocketTls,
}

impl NatsTransport {
    fn from_scheme(scheme: &str) -> Option<Self> {
        match scheme {
            "nats" => Some(Self::Tcp),
            "tls" => Some(Self::Tls),
            "ws" => Some(Self::WebSocket),
            "wss" => Some(Self::WebSocketTls),
            _ => None,
        }
    }

    /// `true` for the WebSocket transports; `async-nats` refuses to mix these
    /// with `nats://`/`tls://` in one connection's server pool.
    pub fn is_websocket(self) -> bool {
        matches!(self, Self::WebSocket | Self::WebSocketTls)
    }

    /// The URL scheme that selects this transport.
    pub fn scheme(self) -> &'static str {
        match self {
            Self::Tcp => "nats",
            Self::Tls => "tls",
            Self::WebSocket => "ws",
            Self::WebSocketTls => "wss",
        }
    }
}

/// Redacts any `userinfo@` embedded in a URL so it is safe to place in an error
/// message or a log line. If the string contains an `@`, everything from just
/// after the scheme separator (`://`, or the start of the string when there is
/// none) through the **last** `@` is replaced with `***`, regardless of `/`,
/// `?`, or `#` in the password. This also covers malformed URLs that never
/// parse, and URLs with no scheme at all.
pub fn redact_url_userinfo(value: &str) -> String {
    let Some(at) = value.rfind('@') else {
        return value.to_owned();
    };
    let start = value.find("://").map(|index| index + 3).unwrap_or(0);
    // Nothing between the start of the authority and the last `@` to hide.
    if at <= start {
        return value.to_owned();
    }
    format!("{}***{}", &value[..start], &value[at..])
}

/// Decodes percent-encoded userinfo (`url` returns the encoded serialization).
/// Falls back to the raw value when the bytes are not valid UTF-8.
fn decode_userinfo(encoded: &str) -> String {
    percent_encoding::percent_decode_str(encoded)
        .decode_utf8()
        .map(|decoded| decoded.into_owned())
        .unwrap_or_else(|_| encoded.to_owned())
}

/// Credentials embedded directly in a NATS server URL (`nats://user:pass@host`).
///
/// Extracted so the URL stored in the connection plan can be credential-free:
/// `async-nats` does not consume URL userinfo, and its debug logging prints the
/// `ServerAddr` (including any password) verbatim.
#[derive(Clone, PartialEq, Eq)]
pub enum NatsInlineAuth {
    /// `nats://user:password@host`.
    UserPassword { username: String, password: String },
    /// `nats://token@host` (a lone userinfo value, no colon).
    Token(String),
}

// Manual `Debug` so the secret never reaches a log line even if a containing
// struct derives `Debug`.
impl std::fmt::Debug for NatsInlineAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UserPassword { username, .. } => f
                .debug_struct("NatsInlineAuth::UserPassword")
                .field("username", username)
                .field("password", &"***")
                .finish(),
            Self::Token(_) => f.write_str("NatsInlineAuth::Token(***)"),
        }
    }
}

/// Parses one NATS server URL into its credential-free form, transport, and any
/// inline credentials. The scheme is mandatory here (unlike `async-nats`, which
/// defaults a bare `host:port` to `nats://`) so the transport is never guessed.
///
/// Error text is redacted, so a malformed URL carrying a password never echoes
/// it through `--check-config`.
pub fn parse_nats_server(server: &str) -> Result<(String, NatsTransport, Option<NatsInlineAuth>)> {
    let url = Url::parse(server).with_context(|| {
        format!(
            "invalid NATS server URL {:?}: expected nats://, tls://, ws://, or wss://",
            redact_url_userinfo(server)
        )
    })?;
    let transport = NatsTransport::from_scheme(url.scheme()).ok_or_else(|| {
        anyhow::anyhow!(
            "invalid NATS server URL {:?}: unsupported scheme {:?}; use nats://, tls://, ws://, or wss://",
            redact_url_userinfo(server),
            url.scheme()
        )
    })?;
    // Userinfo presence is `username OR password`: `nats://:secret@host` has an
    // empty username but a real password, and must not fall through to the raw
    // URL (which would leak the password and drop the auth).
    let has_userinfo = !url.username().is_empty() || url.password().is_some();
    let inline = if has_userinfo {
        match url.password() {
            // A password is present: user + password (username may be empty).
            Some(password) => Some(NatsInlineAuth::UserPassword {
                username: decode_userinfo(url.username()),
                password: decode_userinfo(password),
            }),
            // A lone userinfo value with no colon is a token, matching the
            // common NATS convention.
            None => Some(NatsInlineAuth::Token(decode_userinfo(url.username()))),
        }
    } else {
        None
    };
    // Store a credential-free URL. `async-nats` does not consume URL userinfo,
    // and its debug logging would print any password in the `ServerAddr`.
    let clean = if inline.is_some() {
        let mut stripped = url.clone();
        stripped
            .set_username("")
            .expect("a host-backed URL accepts an empty username");
        stripped
            .set_password(None)
            .expect("a host-backed URL accepts a cleared password");
        stripped.to_string()
    } else {
        server.to_owned()
    };
    Ok((clean, transport, inline))
}

/// Result of normalizing a NATS `servers` list: credential-free URLs, the
/// transport family, and any credentials that were embedded in the URLs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NormalizedNatsServers {
    /// Server URLs with any inline credentials stripped.
    pub servers: Vec<String>,
    /// Transport scheme (`nats`, `tls`, `ws`, `wss`) shared by the whole list.
    pub transport: String,
    /// `true` for the WebSocket transports.
    pub websocket: bool,
    /// Credentials found inline in the URLs, if any. All entries must agree.
    pub inline_auth: Option<NatsInlineAuth>,
}

/// Validates and normalizes a NATS `servers` list.
///
/// Every URL must parse, use a supported scheme, and share a single transport
/// family (one connection cannot mix `ws`/`wss` with `nats`/`tls`). Credentials
/// embedded in the URL are extracted and returned; all entries in one list must
/// carry the same credentials (or none), because they form a single auth hand.
/// Error text is redacted so a password never leaks through `--check-config`.
pub fn normalize_nats_servers(path: &str, servers: &[String]) -> Result<NormalizedNatsServers> {
    if servers.is_empty() {
        bail!("{path}.servers must contain at least one NATS server URL");
    }
    let mut transport: Option<NatsTransport> = None;
    let mut inline_auth: Option<NatsInlineAuth> = None;
    let mut cleaned = Vec::with_capacity(servers.len());
    for (index, server) in servers.iter().enumerate() {
        if server.trim().is_empty() {
            bail!("{path}.servers[{index}] must not be empty");
        }
        let (clean, parsed, inline) = parse_nats_server(server)
            .map_err(|error| anyhow::anyhow!("{path}.servers[{index}]: {error}"))?;
        match transport {
            None => transport = Some(parsed),
            Some(first) if first == parsed => {}
            Some(first) => {
                bail!(
                    "{path}.servers mixes {} and {} transports ({:?} and {:?}); one NATS connection requires a single transport family",
                    first.scheme(),
                    parsed.scheme(),
                    first,
                    parsed
                );
            }
        }
        if let Some(inline) = inline {
            match &inline_auth {
                None => inline_auth = Some(inline),
                Some(existing) if *existing == inline => {}
                Some(_) => bail!(
                    "{path}.servers has inconsistent inline credentials; every seed must use the same credentials or none"
                ),
            }
        }
        cleaned.push(clean);
    }
    let transport = transport.expect("a non-empty server list has a transport");
    Ok(NormalizedNatsServers {
        servers: cleaned,
        transport: transport.scheme().to_owned(),
        websocket: transport.is_websocket(),
        inline_auth,
    })
}

pub const MAX_RUNTIME_DURATION_MS: i64 = 31_536_000_000;
pub const MAX_DEDUPLICATION_GROUP_MEMBERS: usize = 100_000;
pub const MAX_DEDUPLICATION_GROUP_CACHE_BYTES: usize = 256 * 1024 * 1024;
pub const MAX_DEDUPLICATION_CACHE_ENTRIES: usize = 100_000;
pub const MAX_DEDUPLICATION_CACHE_BYTES: usize = 256 * 1024 * 1024;
pub const DEFAULT_CHANNEL_CACHE_MAX_ENTRIES: usize = 16_384;
pub const MAX_CHANNEL_CACHE_MAX_ENTRIES: usize = 100_000;
pub const DEFAULT_CLIENT_NAME_TEMPLATE: &str = "ConflatedPS-{version}-{role}::{user}::{host}";
pub const MAX_CLIENT_NAME_TEMPLATE_LEN: usize = 128;

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
    /// Template for the connection names reported in `CLIENT LIST` (`CLIENT SETNAME`
    /// and `CLIENT SETINFO`). Placeholders: `{version}` (program version), `{role}`
    /// (`i` or `o-<name>`), `{user}` (`USERDOMAIN\USERNAME` or `USER`/`LOGNAME`)
    /// and `{host}` (`COMPUTERNAME`/`HOSTNAME`, falling back to the OS hostname).
    /// Missing values fall back to `unknown`; a template without `{role}` gets
    /// `-<role>` appended. An empty string disables naming.
    #[serde(default)]
    #[schemars(length(max = 128))]
    pub client_name: Option<String>,
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

    /// Resolves this config's `CLIENT LIST` name for a connection role, or
    /// `None` when naming is disabled.
    pub fn client_name_for(&self, role: &str) -> Option<String> {
        resolve_client_name(self.client_name.as_deref(), role)
    }
}

/// Expands a client-name template for a connection role, or `None` when
/// disabled. `{version}` is replaced by the program version and the role is
/// appended (for example `-input` or `-output-out-a`).
pub fn resolve_client_name(template: Option<&str>, role: &str) -> Option<String> {
    resolve_client_name_with(
        template,
        role,
        &default_client_user(),
        &default_client_host(),
    )
}

/// Testable core of [`resolve_client_name`]: `{version}`, `{user}` and `{host}`
/// are replaced with the given values; `{role}` is replaced with the connection
/// role, or appended after a dash when the template omits it.
pub fn resolve_client_name_with(
    template: Option<&str>,
    role: &str,
    user: &str,
    host: &str,
) -> Option<String> {
    let template = template.unwrap_or(DEFAULT_CLIENT_NAME_TEMPLATE);
    if template.is_empty() {
        return None;
    }
    let mut name = template
        .replace("{version}", env!("CARGO_PKG_VERSION"))
        .replace("{user}", user)
        .replace("{host}", host);
    if name.contains("{role}") {
        name = name.replace("{role}", role);
    } else {
        name.push('-');
        name.push_str(role);
    }
    Some(name)
}

fn first_env(keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| env::var(key).ok().filter(|value| !value.is_empty()))
}

/// Windows defaults to `DOMAIN\user`; otherwise the first non-empty of
/// `USERNAME`, `USER`, `LOGNAME`, falling back to `unknown`.
fn default_client_user() -> String {
    match (env::var("USERDOMAIN"), env::var("USERNAME")) {
        (Ok(domain), Ok(user)) if !domain.is_empty() && !user.is_empty() => {
            format!("{domain}\\{user}")
        }
        _ => first_env(&["USERNAME", "USER", "LOGNAME"]).unwrap_or_else(|| "unknown".to_owned()),
    }
}

/// The first non-empty of `COMPUTERNAME`, `HOSTNAME`; when neither is set
/// (systemd does not export `HOSTNAME` to services) it falls back to the OS
/// hostname and finally to `unknown`.
fn default_client_host() -> String {
    first_env(&["COMPUTERNAME", "HOSTNAME"])
        .or_else(system_hostname)
        .unwrap_or_else(|| "unknown".to_owned())
}

/// Reads the OS hostname from the files Linux exposes, returning `None` when
/// they are missing or empty.
fn system_hostname() -> Option<String> {
    ["/proc/sys/kernel/hostname", "/etc/hostname"]
        .iter()
        .find_map(|path| {
            std::fs::read_to_string(path)
                .ok()
                .map(|value| value.trim().to_owned())
                .filter(|value| !value.is_empty() && !value.chars().any(char::is_whitespace))
        })
}

pub fn same_pubsub_server(left: &RedisConfig, right: &RedisConfig) -> bool {
    left.host.eq_ignore_ascii_case(&right.host) && left.port == right.port && left.tls == right.tls
}

/// A single selected backend for an input or output.
///
/// The JSON keeps the Redis-compatible shape: exactly one of the keys `redis`
/// or `nats` is present on `input` and on each output. This enum is the runtime
/// view resolved from those keys rather than the serde representation, because a
/// literal untagged enum silently accepts the "both keys" case and cannot report
/// the "no backend" case with a useful message. See `InputConfig::endpoint` and
/// `OutputConfig::endpoint`.
#[derive(Clone, Copy, Debug)]
pub enum Endpoint<'a> {
    Redis(&'a RedisConfig),
    Nats(&'a NatsConfig),
}

impl<'a> Endpoint<'a> {
    /// Borrows the Redis settings, or `None` when NATS is selected.
    pub fn as_redis(self) -> Option<&'a RedisConfig> {
        match self {
            Endpoint::Redis(redis) => Some(redis),
            Endpoint::Nats(_) => None,
        }
    }

    /// Borrows the NATS settings, or `None` when Redis is selected.
    pub fn as_nats(self) -> Option<&'a NatsConfig> {
        match self {
            Endpoint::Nats(nats) => Some(nats),
            Endpoint::Redis(_) => None,
        }
    }
}

/// Resolves the exactly-one-backend selection shared by `input` and outputs.
/// `path` is the JSON path used in the error message.
pub(crate) fn select_endpoint<'a>(
    path: &str,
    redis: Option<&'a RedisConfig>,
    nats: Option<&'a NatsConfig>,
) -> Result<Endpoint<'a>> {
    match (redis, nats) {
        (Some(redis), None) => Ok(Endpoint::Redis(redis)),
        (None, Some(nats)) => Ok(Endpoint::Nats(nats)),
        (Some(_), Some(_)) => bail!(
            "{path} must select exactly one backend, but both `redis` and `nats` are configured"
        ),
        (None, None) => bail!("{path} must select one backend with `redis` or `nats`"),
    }
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
    /// Redis backend. Exactly one of `redis`/`nats` must be present; `redis`
    /// keeps its 0.2 fields and semantics unchanged.
    #[serde(default)]
    pub redis: Option<RedisConfig>,
    /// Core NATS backend. Exactly one of `redis`/`nats` must be present.
    #[serde(default)]
    pub nats: Option<NatsConfig>,
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

impl InputConfig {
    /// Resolves the single configured backend, rejecting zero or two. `path`
    /// names the JSON location for error messages.
    pub fn endpoint(&self, path: &str) -> Result<Endpoint<'_>> {
        select_endpoint(path, self.redis.as_ref(), self.nats.as_ref())
    }
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
    /// Native Core NATS subscription. `*` matches one token and `>` matches one
    /// or more trailing tokens; these use NATS semantics and are not translated
    /// from Redis globs.
    #[serde(rename = "nats_subject")]
    NatsSubject {
        #[schemars(length(min = 1))]
        subject: String,
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
            }
            | Self::NatsSubject {
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
    /// Redis backend. Exactly one of `redis`/`nats` must be present.
    #[serde(default)]
    pub redis: Option<RedisConfig>,
    /// Core NATS backend. Exactly one of `redis`/`nats` must be present.
    #[serde(default)]
    pub nats: Option<NatsConfig>,
    #[serde(default)]
    pub channel_prefix: String,
    #[serde(default)]
    pub channel_suffix: String,
    /// Maximum queued messages for this output; null means unlimited.
    #[serde(default)]
    #[schemars(range(min = 1))]
    pub queue_max_messages: Option<usize>,
    /// Maximum queued bytes per output, counting mapped channel plus payload; null means unlimited.
    #[serde(default)]
    #[schemars(range(min = 1))]
    pub queue_max_bytes: Option<usize>,
    /// Intake queue overflow policy: drop_newest drops the incoming message at fan-out, drop_oldest evicts the oldest queued message, drop_by_age drops at intake when older than queue_max_age_ms.
    #[serde(default)]
    pub queue_overflow_policy: QueueOverflowPolicy,
    /// Maximum intake queue age in milliseconds; required by drop_by_age and rejected by the other policies.
    #[serde(default)]
    #[schemars(range(min = 1))]
    pub queue_max_age_ms: Option<u64>,
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

impl OutputConfig {
    /// Resolves the single configured backend, rejecting zero or two. `path`
    /// names the JSON location for error messages.
    pub fn endpoint(&self, path: &str) -> Result<Endpoint<'_>> {
        select_endpoint(path, self.redis.as_ref(), self.nats.as_ref())
    }

    /// Redis settings for the runtime publish path, which is only reached after
    /// the endpoint selection has resolved to Redis.
    pub(crate) fn redis_endpoint(&self) -> &RedisConfig {
        self.redis
            .as_ref()
            .expect("runtime publish path requires a Redis output endpoint")
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DeduplicationGroup {
    /// Deduplication is a diff TTL in milliseconds shared by the group's channels: only
    /// consecutive identical payloads are suppressed, and only until the shared window
    /// expires; changed payloads always publish. Values <= 0 disable deduplication for
    /// group members.
    #[schemars(
        description = "Deduplication is a diff TTL in milliseconds shared by the group's channels: only consecutive identical payloads are suppressed, and only until the shared window expires; changed payloads always publish. Values <= 0 disable deduplication for group members."
    )]
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
    /// Optional per-channel diff TTL override; nonpositive values disable deduplication.
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
    /// Diff TTL for duplicate suppression in milliseconds: only consecutive identical payloads
    /// (same mapped output channel and raw incoming bytes) are suppressed, and only until the
    /// window expires. A changed payload always publishes (subject to conflation) and replaces
    /// the remembered value, so `A→B→A` publishes all three; after the window ends the current
    /// value is sent again even if unchanged. Nonpositive values disable deduplication.
    #[serde(default = "default_deduplication_ttl_ms")]
    #[schemars(
        default = "default_deduplication_ttl_ms",
        description = "Deduplication is a diff TTL: only consecutive identical payloads for the same mapped channel are suppressed, and only until this window expires. A changed payload always publishes (subject to conflation) and replaces the remembered value, so A→B→A publishes all three; after the window ends, the current value is sent again even if unchanged. 0 by default and values <= 0 disable deduplication."
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
    fn connection_url(&self) -> Result<Url> {
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
        Ok(url)
    }

    pub fn client(&self) -> Result<Client> {
        Client::open(self.connection_url()?.as_str()).context("invalid Redis connection settings")
    }

    /// Builds a client that identifies itself in `CLIENT LIST` through
    /// `CLIENT SETINFO` (`lib-name`/`lib-ver`). `RedisConfig::client` connections
    /// keep the redis-rs defaults.
    pub fn client_with_lib_name(&self, lib_name: &str) -> Result<Client> {
        let url = self.connection_url()?;
        let info: ConnectionInfo = url
            .as_str()
            .parse()
            .map_err(|_| anyhow::anyhow!("invalid Redis connection settings"))?;
        let redis = info
            .redis_settings()
            .clone()
            .set_lib_name(lib_name, env!("CARGO_PKG_VERSION"));
        Client::open(info.set_redis_settings(redis)).context("invalid Redis connection settings")
    }

    pub fn connect_timeout(&self) -> Duration {
        Duration::from_millis(self.connect_timeout_ms)
    }
}

/// Core NATS backend settings.
///
/// The `servers` list holds one or more seed URLs; the URL scheme selects the
/// transport (`nats://`, `tls://`, `ws://`, `wss://`). Every URL in one list
/// must use the same transport family. The runtime adapter in
/// `src/service/nats` consumes these settings: seed pool, connection
/// name/timeout, server discovery, authentication, and TLS.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct NatsConfig {
    /// One or more NATS server URLs used as cluster seeds.
    #[schemars(length(min = 1))]
    pub servers: Vec<String>,
    /// Optional connection name reported in NATS server `CONNZ` listings.
    #[serde(default)]
    #[schemars(length(min = 1))]
    pub name: Option<String>,
    /// Optional connect timeout in milliseconds.
    #[serde(default = "default_nats_connect_timeout_ms")]
    #[schemars(range(min = 1))]
    pub connect_timeout_ms: u64,
    /// When true, only the configured `servers` are used and servers advertised
    /// by the cluster are ignored. Useful behind proxies or WebSocket ingress.
    #[serde(default)]
    pub ignore_discovered_servers: bool,
    /// Authentication method; defaults to none.
    #[serde(default)]
    pub auth: NatsAuth,
    /// Optional TLS transport settings (trusted roots and client identity).
    #[serde(default)]
    #[schemars(default)]
    pub tls: Option<NatsTlsConfig>,
}

impl NatsConfig {
    pub fn connect_timeout(&self) -> Duration {
        Duration::from_millis(self.connect_timeout_ms)
    }
}

/// Secret reference for NATS authentication. Secrets are always referenced by
/// environment variable name or file path; values are never read from JSON.
#[derive(Clone, Debug, Default, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum NatsAuth {
    /// No authentication (development only).
    #[default]
    None,
    /// Username plus a password read from `password_env`.
    UserPassword {
        #[schemars(length(min = 1))]
        username: String,
        #[schemars(length(min = 1))]
        password_env: String,
    },
    /// Token read from `token_env`.
    Token {
        #[schemars(length(min = 1))]
        token_env: String,
    },
    /// NKey seed read from `seed_file`.
    Nkey {
        #[schemars(length(min = 1))]
        seed_file: PathBuf,
    },
    /// NATS credentials file (user JWT plus its NKey seed) at `path`.
    CredentialsFile {
        #[schemars(length(min = 1))]
        path: PathBuf,
    },
}

/// Optional NATS TLS settings. The server URL scheme selects TLS; these fields
/// extend it with a custom trusted root and/or a client identity for mTLS.
#[derive(Clone, Debug, Default, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct NatsTlsConfig {
    /// Additional trusted root certificate file.
    #[serde(default)]
    #[schemars(length(min = 1))]
    pub root_cert_file: Option<PathBuf>,
    /// Client certificate file for mTLS; requires `client_key_file`.
    #[serde(default)]
    #[schemars(length(min = 1))]
    pub client_cert_file: Option<PathBuf>,
    /// Client private key file for mTLS; requires `client_cert_file`.
    #[serde(default)]
    #[schemars(length(min = 1))]
    pub client_key_file: Option<PathBuf>,
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

// The variant names mirror the public `queue_overflow_policy` JSON strings.
#[allow(clippy::enum_variant_names)]
#[derive(Clone, Copy, Debug, Default, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum QueueOverflowPolicy {
    /// Drop the incoming message when the output queue is at capacity (default).
    #[default]
    DropNewest,
    /// Evict the oldest queued message to make room for the incoming one.
    DropOldest,
    /// Drop queued messages at intake once they are older than queue_max_age_ms.
    DropByAge,
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

fn default_nats_connect_timeout_ms() -> u64 {
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
