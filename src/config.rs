use std::{env, fs, path::PathBuf, time::Duration};

use anyhow::{Context, Result, bail};
use redis::Client;
use serde::Deserialize;
use url::Url;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppConfig {
    pub input: InputConfig,
    pub output: OutputConfig,
    #[serde(default)]
    pub conflation: ConflationConfig,
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
        if self.input.subscriptions.is_empty() {
            bail!("input.subscriptions must contain at least one subscription");
        }
        if self.conflation.max_pending_keys == 0 {
            bail!("conflation.max_pending_keys must be greater than zero");
        }
        if self.conflation.max_message_bytes == 0 {
            bail!("conflation.max_message_bytes must be greater than zero");
        }
        if self.conflation.max_pending_bytes == 0 {
            bail!("conflation.max_pending_bytes must be greater than zero");
        }
        if self.conflation.max_batch_bytes == 0 {
            bail!("conflation.max_batch_bytes must be greater than zero");
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
        if self.input.redis.database < 0 || self.output.redis.database < 0 {
            bail!("Redis database IDs must not be negative");
        }
        for endpoint in [&self.input.redis, &self.output.redis] {
            if endpoint.host.is_empty() {
                bail!("Redis hosts must not be empty");
            }
            if endpoint.connect_timeout_ms == 0 {
                bail!("Redis connect_timeout_ms must be greater than zero");
            }
        }
        if self.logging.retention_days == 0 || self.logging.max_total_size_mb == 0 {
            bail!("logging retention and size limits must be greater than zero");
        }
        let same_output_server = self
            .input
            .redis
            .host
            .eq_ignore_ascii_case(&self.output.redis.host)
            && self.input.redis.port == self.output.redis.port
            && self.input.redis.tls == self.output.redis.tls;
        if same_output_server
            && self.output.channel_prefix.is_empty()
            && self.output.channel_suffix.is_empty()
        {
            bail!(
                "output.channel_prefix or output.channel_suffix is required when input and output use the same Redis server, to avoid Pub/Sub echo loops"
            );
        }
        for subscription in &self.input.subscriptions {
            match subscription {
                Subscription::Subscribe { channel } if channel.is_empty() => {
                    bail!("SUBSCRIBE channels must not be empty");
                }
                Subscription::Psubscribe { pattern } if pattern.is_empty() => {
                    bail!("PSUBSCRIBE patterns must not be empty");
                }
                _ => {}
            }
        }
        Ok(())
    }
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
    Subscribe { channel: String },
    Psubscribe { pattern: String },
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutputConfig {
    pub redis: RedisConfig,
    #[serde(default)]
    pub channel_prefix: String,
    #[serde(default)]
    pub channel_suffix: String,
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
#[serde(default, deny_unknown_fields)]
pub struct ConflationConfig {
    pub interval_ms: i64,
    pub max_pending_keys: usize,
    pub max_pending_bytes: usize,
    pub max_message_bytes: usize,
    pub max_batch_bytes: usize,
}

impl Default for ConflationConfig {
    fn default() -> Self {
        Self {
            interval_ms: 250,
            max_pending_keys: 10_000,
            max_pending_bytes: 50 * 1024 * 1024,
            max_message_bytes: 10 * 1024 * 1024,
            max_batch_bytes: 70 * 1024 * 1024,
        }
    }
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
    fn initial_test_config_uses_wildcard_input_and_database_one_output() {
        let config: AppConfig =
            serde_json::from_str(include_str!("../config.example.json")).unwrap();
        config.validate().unwrap();

        assert_eq!(config.input.redis.host, "redis.example.net");
        assert_eq!(config.input.redis.database, 0);
        assert!(matches!(
            config.input.subscriptions.as_slice(),
            [Subscription::Psubscribe { pattern }] if pattern == "*"
        ));
        assert_eq!(config.output.redis.database, 1);
        assert_eq!(config.conflation.interval_ms, 250);
        assert_eq!(config.output.channel_prefix, "replica:");
        assert!(config.status.path.is_none());
        assert!(config.status.http.is_some());
    }

    #[test]
    fn same_pubsub_endpoint_requires_output_namespace_across_database_ids() {
        let mut config: AppConfig = serde_json::from_str(
            r#"{
                "input":{"redis":{"host":"redis.example.net","database":0},"subscriptions":[{"type":"subscribe","channel":"events"}]},
                "output":{"redis":{"host":"redis.example.net","database":7}},
                "instance_lock":{"path":"lock"}
            }"#,
        )
        .unwrap();

        let error = config.validate().unwrap_err();
        assert!(error.to_string().contains("output.channel_prefix"));

        config.output.channel_suffix = ":copy".to_owned();
        config.validate().unwrap();
    }

    #[test]
    fn different_pubsub_endpoints_do_not_require_output_namespace() {
        let config: AppConfig = serde_json::from_str(
            r#"{
                "input":{"redis":{"host":"input.example.net"},"subscriptions":[{"type":"subscribe","channel":"events"}]},
                "output":{"redis":{"host":"output.example.net"}},
                "instance_lock":{"path":"lock"}
            }"#,
        )
        .unwrap();

        config.validate().unwrap();
        assert!(config.status.path.is_none());
        assert!(config.status.http.is_none());
    }

    #[test]
    fn conflation_interval_accepts_signed_values() {
        let config: AppConfig = serde_json::from_str(
            r#"{
                "input":{"redis":{"host":"input.example.net"},"subscriptions":[{"type":"subscribe","channel":"events"}]},
                "output":{"redis":{"host":"output.example.net"}},
                "conflation":{"interval_ms":-25},
                "instance_lock":{"path":"lock"}
            }"#,
        )
        .unwrap();

        config.validate().unwrap();
        assert_eq!(config.conflation.interval_ms, -25);
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
