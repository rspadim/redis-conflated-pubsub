use std::collections::BTreeMap;
use std::{env, fs, path::PathBuf, time::Duration};

use anyhow::{Context, Result, bail};
use redis::Client;
use serde::Deserialize;
use url::Url;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppConfig {
    /// Default encoded RESP request size target for each output EXEC.
    #[serde(default = "default_max_bytes_per_exec")]
    pub max_bytes_per_exec: usize,
    /// Policy for single PUBLISH requests that exceed their output byte target.
    #[serde(default)]
    pub oversized_message_policy: OversizedMessagePolicy,
    pub input: InputConfig,
    pub outputs: BTreeMap<String, OutputConfig>,
    pub instance_lock: InstanceLockConfig,
    #[serde(default)]
    pub status: StatusConfig,
    #[serde(default)]
    pub logging: LoggingConfig,
}

impl AppConfig {
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
    Ok(())
}

pub fn same_pubsub_server(left: &RedisConfig, right: &RedisConfig) -> bool {
    left.host.eq_ignore_ascii_case(&right.host) && left.port == right.port && left.tls == right.tls
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InputConfig {
    pub redis: RedisConfig,
    pub subscriptions: Vec<Subscription>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase", deny_unknown_fields)]
pub enum Subscription {
    Subscribe {
        channel: String,
        #[serde(default)]
        output_prefix: String,
        #[serde(default)]
        output_suffix: String,
    },
    Psubscribe {
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

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutputConfig {
    pub redis: RedisConfig,
    #[serde(default)]
    pub channel_prefix: String,
    #[serde(default)]
    pub channel_suffix: String,
    pub conflation: ConflationConfig,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RedisConfig {
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    pub username: Option<String>,
    pub password_env: Option<String>,
    #[serde(default)]
    pub tls: bool,
    #[serde(default)]
    pub database: i64,
    #[serde(default = "default_connect_timeout_ms")]
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

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConflationConfig {
    pub interval_ms: i64,
    #[serde(default = "default_max_commands_per_exec")]
    pub max_commands_per_exec: usize,
    /// Optional override for the global encoded RESP request size target.
    #[serde(default)]
    pub max_bytes_per_exec: Option<usize>,
    #[serde(default)]
    pub oversized_message_policy: Option<OversizedMessagePolicy>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
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

#[derive(Clone, Debug, Deserialize)]
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

#[derive(Clone, Debug, Deserialize)]
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

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpStatusConfig {
    #[serde(default = "default_http_bind")]
    pub bind: String,
    #[serde(default = "default_http_port")]
    pub port: u16,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LoggingConfig {
    pub directory: PathBuf,
    pub level: String,
    pub retention_days: u64,
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
        assert!(matches!(
            config.input.subscriptions.as_slice(),
            [Subscription::Psubscribe { pattern, output_prefix, output_suffix }]
                if pattern == "*" && output_prefix.is_empty() && output_suffix.is_empty()
        ));
        assert_eq!(config.outputs.len(), 2);
        assert_eq!(config.outputs["output0"].redis.host, "localhost");
        assert_eq!(config.outputs["output0"].redis.database, 0);
        assert_eq!(config.outputs["output0"].conflation.interval_ms, 0);
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
            None
        );
        assert_eq!(config.outputs["output1"].channel_prefix, "db1:");
        assert!(config.status.path.is_none());
        assert!(config.status.http.is_some());
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
