//! One place that turns a [`NatsConfig`] into a connected `async_nats::Client`.
//!
//! The mapping is split in two so the configuration-to-options decision is a
//! pure function that unit tests can assert on without a network or secrets:
//!
//! * [`connection_plan`] normalizes `servers`, name, connect timeout, server
//!   discovery, the `auth` variant, and the TLS file references. It reads no
//!   environment variables and no files, and never carries a secret value.
//! * [`connect`] resolves the auth secret (environment variable or file),
//!   builds `ConnectOptions`, and connects.
//!
//! Secrets are only ever read here; they are never logged or included in error
//! messages, which name the environment variable or file path instead.

use std::{env, fs, path::PathBuf, str::FromStr, sync::Arc, time::Duration};

use anyhow::{Context, Result, bail};
use async_nats::{Client, ConnectOptions, ServerAddr};

use crate::config::{NatsAuth, NatsConfig};
use crate::status::Metrics;

use super::super::publish::PublishFailure;

/// Auth variant resolved from the configuration without reading the secret.
///
/// Only a reference (environment variable name, file path, or an inline value
/// from the server URL) is stored. The inline variants carry a secret, so
/// `NatsAuthPlan`'s `Debug` is hand-written to redact it.
#[derive(Clone, PartialEq, Eq)]
pub(in crate::service) enum NatsAuthPlan {
    None,
    UserPassword {
        username: String,
        password_env: String,
    },
    Token {
        token_env: String,
    },
    Nkey {
        seed_file: PathBuf,
    },
    CredentialsFile {
        path: PathBuf,
    },
    /// Credentials embedded in a server URL (`nats://user:pass@host`).
    InlineUserPassword {
        username: String,
        password: String,
    },
    /// A lone token embedded in a server URL (`nats://token@host`).
    InlineToken {
        token: String,
    },
}

impl std::fmt::Debug for NatsAuthPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => f.write_str("None"),
            Self::UserPassword {
                username,
                password_env,
            } => f
                .debug_struct("UserPassword")
                .field("username", username)
                .field("password_env", password_env)
                .finish(),
            Self::Token { token_env } => f
                .debug_struct("Token")
                .field("token_env", token_env)
                .finish(),
            Self::Nkey { seed_file } => f
                .debug_struct("Nkey")
                .field("seed_file", seed_file)
                .finish(),
            Self::CredentialsFile { path } => f
                .debug_struct("CredentialsFile")
                .field("path", path)
                .finish(),
            // Inline variants: redact the secret.
            Self::InlineUserPassword { username, .. } => f
                .debug_struct("InlineUserPassword")
                .field("username", username)
                .field("password", &"***")
                .finish(),
            Self::InlineToken { .. } => f.write_str("InlineToken(***)"),
        }
    }
}

impl From<&NatsAuth> for NatsAuthPlan {
    fn from(auth: &NatsAuth) -> Self {
        match auth {
            NatsAuth::None => Self::None,
            NatsAuth::UserPassword {
                username,
                password_env,
            } => Self::UserPassword {
                username: username.clone(),
                password_env: password_env.clone(),
            },
            NatsAuth::Token { token_env } => Self::Token {
                token_env: token_env.clone(),
            },
            NatsAuth::Nkey { seed_file } => Self::Nkey {
                seed_file: seed_file.clone(),
            },
            NatsAuth::CredentialsFile { path } => Self::CredentialsFile { path: path.clone() },
        }
    }
}

/// Backend-neutral, secret-free description of the resolved NATS connection.
///
/// This is the assertable surface for the configuration mapping: it captures
/// every `ConnectOptions` decision except the secret values themselves.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::service) struct ConnectionPlan {
    /// Seed/URL list, exactly as configured.
    pub(in crate::service) servers: Vec<String>,
    /// Transport scheme (`nats`, `tls`, `ws`, `wss`) shared by the whole list.
    pub(in crate::service) transport: String,
    /// `true` for the WebSocket transports.
    pub(in crate::service) websocket: bool,
    /// Optional connection name reported in the server's client listing.
    pub(in crate::service) name: Option<String>,
    /// Connect (and handshake) timeout.
    pub(in crate::service) connect_timeout: Duration,
    /// When true, servers advertised by the cluster are ignored.
    pub(in crate::service) ignore_discovered_servers: bool,
    /// Auth variant plus its environment/file reference.
    pub(in crate::service) auth: NatsAuthPlan,
    pub(in crate::service) root_cert_file: Option<PathBuf>,
    pub(in crate::service) client_cert_file: Option<PathBuf>,
    pub(in crate::service) client_key_file: Option<PathBuf>,
}

/// Resolves a [`NatsConfig`] into a [`ConnectionPlan`] without touching the
/// network, the environment, or the filesystem.
///
/// `servers` is normalized here (single transport family, inline credentials
/// extracted) so the adapter cannot be pointed at an unsupported list, and the
/// stored server URLs never carry a secret even when constructed directly
/// rather than through `--check-config`.
pub(in crate::service) fn connection_plan(config: &NatsConfig) -> Result<ConnectionPlan> {
    // Runtime validation already ran with a path-qualified message; the
    // "nats" path here is the adapter's belt-and-braces re-check.
    let normalized = crate::config::normalize_nats_servers("nats", &config.servers)?;
    let tls = config.tls.as_ref();
    let client_cert_file = tls.and_then(|tls| tls.client_cert_file.clone());
    let client_key_file = tls.and_then(|tls| tls.client_key_file.clone());
    if client_cert_file.is_some() != client_key_file.is_some() {
        bail!("NATS mTLS requires client_cert_file and client_key_file together");
    }
    // Inline URL credentials are a real auth method, so they cannot coexist with
    // an explicit `auth` block: two owners of one credential is ambiguous.
    let explicit_auth = NatsAuthPlan::from(&config.auth);
    let auth = match &normalized.inline_auth {
        Some(inline) => {
            if !matches!(config.auth, NatsAuth::None) {
                bail!(
                    "NATS server URL embeds credentials and `auth` is also configured; use one or the other"
                );
            }
            match inline {
                crate::config::NatsInlineAuth::UserPassword { username, password } => {
                    NatsAuthPlan::InlineUserPassword {
                        username: username.clone(),
                        password: password.clone(),
                    }
                }
                crate::config::NatsInlineAuth::Token(token) => NatsAuthPlan::InlineToken {
                    token: token.clone(),
                },
            }
        }
        None => explicit_auth,
    };
    Ok(ConnectionPlan {
        servers: normalized.servers,
        transport: normalized.transport,
        websocket: normalized.websocket,
        name: config.name.clone(),
        connect_timeout: config.connect_timeout(),
        ignore_discovered_servers: config.ignore_discovered_servers,
        auth,
        root_cert_file: tls.and_then(|tls| tls.root_cert_file.clone()),
        client_cert_file,
        client_key_file,
    })
}

/// Auth material resolved from a plan's references. This is the only place a
/// secret value exists in memory; it never enters a `ConnectionPlan`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::service) enum NatsAuthMaterial {
    None,
    UserPassword { username: String, password: String },
    Token(String),
    Nkey(String),
    Credentials(String),
}

/// Resolves the auth secret from its environment variable or file reference.
/// Error messages name the variable or path; the value is never logged.
pub(in crate::service) fn resolve_auth(auth: &NatsAuthPlan) -> Result<NatsAuthMaterial> {
    Ok(match auth {
        NatsAuthPlan::None => NatsAuthMaterial::None,
        NatsAuthPlan::UserPassword {
            username,
            password_env,
        } => {
            let password = env::var(password_env)
                .with_context(|| format!("environment variable {password_env} is not set"))?;
            NatsAuthMaterial::UserPassword {
                username: username.clone(),
                password,
            }
        }
        NatsAuthPlan::Token { token_env } => {
            let token = env::var(token_env)
                .with_context(|| format!("environment variable {token_env} is not set"))?;
            NatsAuthMaterial::Token(token)
        }
        NatsAuthPlan::Nkey { seed_file } => {
            let seed = fs::read_to_string(seed_file).with_context(|| {
                format!("failed to read NATS NKey seed file {}", seed_file.display())
            })?;
            NatsAuthMaterial::Nkey(seed.trim().to_owned())
        }
        NatsAuthPlan::CredentialsFile { path } => {
            let credentials = fs::read_to_string(path).with_context(|| {
                format!("failed to read NATS credentials file {}", path.display())
            })?;
            NatsAuthMaterial::Credentials(credentials)
        }
        NatsAuthPlan::InlineUserPassword { username, password } => NatsAuthMaterial::UserPassword {
            username: username.clone(),
            password: password.clone(),
        },
        NatsAuthPlan::InlineToken { token } => NatsAuthMaterial::Token(token.clone()),
    })
}

/// Builds `ConnectOptions` from a plan without connection-event counting. Only
/// the config-mapping tests use it; the runtime calls
/// [`connect_options_with_metrics`] so events reach the status counters.
#[cfg(test)]
pub(in crate::service) fn connect_options(plan: &ConnectionPlan) -> Result<ConnectOptions> {
    connect_options_with_metrics(plan, None)
}

/// Like [`connect_options`], but also registers an `event_callback` that records
/// NATS connect/disconnect events into `metrics`. Core NATS reconnects
/// internally, so this is the only honest signal that a node failover happened.
pub(in crate::service) fn connect_options_with_metrics(
    plan: &ConnectionPlan,
    metrics: Option<Arc<Metrics>>,
) -> Result<ConnectOptions> {
    let mut options = ConnectOptions::new()
        .connection_timeout(plan.connect_timeout)
        // Reconnect for the lifetime of the relay, matching the Redis input
        // retry loop and output reconnection behavior.
        .max_reconnects(None);
    if let Some(name) = &plan.name {
        options = options.name(name.clone());
    }
    if plan.ignore_discovered_servers {
        options = options.ignore_discovered_servers();
    }
    match resolve_auth(&plan.auth)? {
        NatsAuthMaterial::None => {}
        NatsAuthMaterial::UserPassword { username, password } => {
            options = options.user_and_password(username, password);
        }
        NatsAuthMaterial::Token(token) => {
            options = options.token(token);
        }
        NatsAuthMaterial::Nkey(seed) => {
            options = options.nkey(seed);
        }
        NatsAuthMaterial::Credentials(credentials) => {
            options = options
                .credentials(&credentials)
                .context("invalid NATS credentials file")?;
        }
    }
    if let Some(root_cert_file) = &plan.root_cert_file {
        options = options.add_root_certificates(root_cert_file.clone());
    }
    if let (Some(cert), Some(key)) = (&plan.client_cert_file, &plan.client_key_file) {
        options = options.add_client_certificate(cert.clone(), key.clone());
    }
    if let Some(metrics) = metrics {
        options = options.event_callback(move |event| {
            let metrics = Arc::clone(&metrics);
            async move {
                match event {
                    async_nats::Event::Connected => metrics.record_nats_connect(),
                    async_nats::Event::Disconnected => metrics.record_nats_disconnect(),
                    async_nats::Event::SlowConsumer(_) => metrics.record_nats_slow_consumer(),
                    _ => {}
                }
            }
        });
    }
    Ok(options)
}

/// Reads one NATS NKey seed through the credential path, for tests that assert
/// the file is opened and trimmed without building full `ConnectOptions`.
#[cfg(test)]
pub(in crate::service) fn connection_plan_nkey_seed(plan: &ConnectionPlan) -> Result<String> {
    match resolve_auth(&plan.auth)? {
        NatsAuthMaterial::Nkey(seed) => Ok(seed),
        other => bail!("plan does not use NKey auth: {other:?}"),
    }
}

/// Preflights a plan without opening a connection: resolves the auth secret
/// (environment variable or file) and confirms the TLS files exist, so a missing
/// secret or unreadable certificate fails at startup or reload rather than
/// looping in the reader. The resolved secret is discarded immediately.
pub(in crate::service) fn preflight(plan: &ConnectionPlan) -> Result<()> {
    // Building the options resolves the auth reference and reads the TLS paths.
    let _ = connect_options_with_metrics(plan, None)?;
    Ok(())
}

/// Connects a fresh client for a plan. The connect timeout is enforced by
/// `async-nats` through `ConnectOptions::connection_timeout`.
///
/// A failure to establish the connection means nothing was sent on it, which is
/// why the caller maps this to `PublishFailure::NotSent` for an output sink.
/// Like [`connect`], but registers connection-event counting when `metrics` is
/// provided. The initial connect also fires `Event::Connected`, so the counter
/// includes the first connection; callers compare it against the number of NATS
/// endpoints to detect reconnections.
pub(in crate::service) async fn connect_with_metrics(
    plan: &ConnectionPlan,
    metrics: Option<Arc<Metrics>>,
) -> Result<Client> {
    let options = connect_options_with_metrics(plan, metrics)?;
    let addresses = plan
        .servers
        .iter()
        .map(|server| ServerAddr::from_str(server))
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("invalid NATS server URL")?;
    options
        .connect(addresses)
        .await
        .context("failed to connect to NATS")
}

/// Maps any connection-setup failure to the output failure vocabulary. Nothing
/// reaches the broker when the connection cannot be established, so the failure
/// is definitive (`NotSent`).
pub(in crate::service) fn connect_failure(error: anyhow::Error) -> PublishFailure {
    PublishFailure::NotSent(format!("NATS output connection failed: {error:#}"))
}

#[cfg(test)]
#[path = "../../../tests/unit/service/nats_connect.rs"]
mod tests;
