//! Pure mapping tests: `NatsConfig` -> `ConnectionPlan` -> `ConnectOptions`.
//!
//! No network is used. `connect_options` is asserted by reading back the public
//! `ConnectOptions` surface and by reading the secret files from a temp
//! directory; secret env vars are read with unique names to avoid interfering
//! with other tests.

use std::env;
use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use crate::config::NatsConfig;
use crate::service::nats::connect::{
    NatsAuthPlan, connect_options, connection_plan, connection_plan_nkey_seed,
};

fn config(json: serde_json::Value) -> NatsConfig {
    serde_json::from_value(json).expect("NATS config should deserialize")
}

fn unique_env_name(prefix: &str) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    format!(
        "RCPS_TEST_{prefix}_{}_{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

#[test]
fn no_auth_plan_applies_servers_name_timeout_and_discovery() {
    let planned = connection_plan(&config(serde_json::json!({
        "servers": ["nats://one:4222", "nats://two:4222"],
        "name": "relay-input",
        "connect_timeout_ms": 2500,
        "ignore_discovered_servers": true,
    })))
    .unwrap();

    assert_eq!(planned.servers, vec!["nats://one:4222", "nats://two:4222"]);
    assert_eq!(planned.transport, "nats");
    assert!(!planned.websocket);
    assert_eq!(planned.name.as_deref(), Some("relay-input"));
    assert_eq!(planned.connect_timeout, Duration::from_millis(2500));
    assert!(planned.ignore_discovered_servers);
    assert_eq!(planned.auth, NatsAuthPlan::None);
    assert!(planned.root_cert_file.is_none());
    assert!(planned.client_cert_file.is_none());
    assert!(planned.client_key_file.is_none());

    let options = connect_options(&planned).unwrap();
    let debug = format!("{options:?}");
    assert!(debug.contains("relay-input"));
    // `ignore_discovered_servers` is not part of the Debug output, so assert it
    // through the plan field above and the connection timeout below.
    assert!(planned.connect_timeout == Duration::from_millis(2500));
}

#[test]
fn auth_variants_map_to_their_plan_without_reading_the_secret() {
    let user_password = connection_plan(&config(serde_json::json!({
        "servers": ["nats://localhost"],
        "auth": {"type": "user_password", "username": "relay", "password_env": "DOES_NOT_EXIST_YET"},
    })))
    .unwrap();
    assert_eq!(
        user_password.auth,
        NatsAuthPlan::UserPassword {
            username: "relay".to_owned(),
            password_env: "DOES_NOT_EXIST_YET".to_owned(),
        }
    );

    let token = connection_plan(&config(serde_json::json!({
        "servers": ["nats://localhost"],
        "auth": {"type": "token", "token_env": "TOKEN_ENV"},
    })))
    .unwrap();
    assert_eq!(
        token.auth,
        NatsAuthPlan::Token {
            token_env: "TOKEN_ENV".to_owned(),
        }
    );

    let nkey = connection_plan(&config(serde_json::json!({
        "servers": ["nats://localhost"],
        "auth": {"type": "nkey", "seed_file": "/run/secrets/user.nk"},
    })))
    .unwrap();
    assert_eq!(
        nkey.auth,
        NatsAuthPlan::Nkey {
            seed_file: PathBuf::from("/run/secrets/user.nk"),
        }
    );

    let credentials = connection_plan(&config(serde_json::json!({
        "servers": ["nats://localhost"],
        "auth": {"type": "credentials_file", "path": "/run/secrets/user.creds"},
    })))
    .unwrap();
    assert_eq!(
        credentials.auth,
        NatsAuthPlan::CredentialsFile {
            path: PathBuf::from("/run/secrets/user.creds"),
        }
    );
}

#[test]
fn user_password_options_read_the_password_from_the_environment() {
    let env_name = unique_env_name("PASSWORD");
    // SAFETY: the name is unique to this test and no other thread reads it.
    unsafe { env::set_var(&env_name, "s3cr3t") };
    let planned = connection_plan(&config(serde_json::json!({
        "servers": ["nats://localhost"],
        "auth": {"type": "user_password", "username": "relay", "password_env": env_name},
    })))
    .unwrap();

    let options = connect_options(&planned).unwrap();
    // The secret must never appear in the Debug output.
    assert!(!format!("{options:?}").contains("s3cr3t"));
    unsafe { env::remove_var(&env_name) };

    // A missing environment variable is a clear setup error, not a panic.
    let missing = connection_plan(&config(serde_json::json!({
        "servers": ["nats://localhost"],
        "auth": {"type": "user_password", "username": "relay", "password_env": "RCPS_TEST_UNSET_PASSWORD"},
    })))
    .unwrap();
    let error = connect_options(&missing).unwrap_err().to_string();
    assert!(error.contains("RCPS_TEST_UNSET_PASSWORD"), "{error}");
}

#[test]
fn nkey_seed_is_read_and_trimmed_from_the_seed_file() {
    let directory = tempfile::tempdir().unwrap();
    let seed_file = directory.path().join("user.nk");
    // A real NKey seed is not required to assert the file read + trim behavior.
    fs::write(
        &seed_file,
        "  SUAxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx  \n",
    )
    .unwrap();

    let planned = connection_plan(&config(serde_json::json!({
        "servers": ["nats://localhost"],
        "auth": {"type": "nkey", "seed_file": seed_file},
    })))
    .unwrap();
    assert_eq!(
        connection_plan_nkey_seed(&planned).unwrap(),
        "SUAxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
    );

    let missing = connection_plan(&config(serde_json::json!({
        "servers": ["nats://localhost"],
        "auth": {"type": "nkey", "seed_file": directory.path().join("absent.nk")},
    })))
    .unwrap();
    let error = connect_options(&missing).unwrap_err().to_string();
    assert!(error.contains("NKey seed file"), "{error}");
}

#[test]
fn credentials_file_errors_name_the_file_without_leaking_contents() {
    let directory = tempfile::tempdir().unwrap();
    let missing = directory.path().join("absent.creds");
    let planned = connection_plan(&config(serde_json::json!({
        "servers": ["nats://localhost"],
        "auth": {"type": "credentials_file", "path": missing},
    })))
    .unwrap();

    let error = connect_options(&planned).unwrap_err().to_string();
    assert!(error.contains("absent.creds"), "{error}");
}

#[test]
fn tls_files_and_transports_are_classified_from_the_scheme() {
    let wss = connection_plan(&config(serde_json::json!({
        "servers": ["wss://nats.example.net:443"],
        "tls": {
            "root_cert_file": "/run/secrets/ca.pem",
            "client_cert_file": "/run/secrets/cert.pem",
            "client_key_file": "/run/secrets/key.pem",
        },
    })))
    .unwrap();
    assert_eq!(wss.transport, "wss");
    assert!(wss.websocket);
    assert_eq!(
        wss.root_cert_file,
        Some(PathBuf::from("/run/secrets/ca.pem"))
    );
    assert_eq!(
        wss.client_cert_file,
        Some(PathBuf::from("/run/secrets/cert.pem"))
    );
    assert_eq!(
        wss.client_key_file,
        Some(PathBuf::from("/run/secrets/key.pem"))
    );

    let tls = connection_plan(&config(serde_json::json!({
        "servers": ["tls://nats.example.net:4222"],
    })))
    .unwrap();
    assert_eq!(tls.transport, "tls");
    assert!(!tls.websocket);

    let ws = connection_plan(&config(serde_json::json!({
        "servers": ["ws://nats.example.net:80"],
    })))
    .unwrap();
    assert_eq!(ws.transport, "ws");
    assert!(ws.websocket);
}

/// Inline URL credentials become real auth and are stripped from the stored
/// server list, so no secret reaches `ServerAddr` (and its debug logging).
#[test]
fn inline_url_credentials_become_auth_and_are_stripped_from_servers() {
    let planned = connection_plan(&config(serde_json::json!({
        "servers": ["nats://relay:s3cr3t@one:4222", "nats://relay:s3cr3t@two:4222"],
    })))
    .unwrap();

    assert_eq!(
        planned.auth,
        NatsAuthPlan::InlineUserPassword {
            username: "relay".to_owned(),
            password: "s3cr3t".to_owned(),
        }
    );
    // The stored URLs carry no userinfo.
    assert_eq!(planned.servers, vec!["nats://one:4222", "nats://two:4222"]);
    for server in &planned.servers {
        assert!(!server.contains("s3cr3t"), "{server}");
        assert!(!server.contains('@'), "{server}");
    }
    // The secret never appears in the plan's Debug output.
    let debug = format!("{planned:?}");
    assert!(!debug.contains("s3cr3t"), "{debug}");
    // ... nor in the built ConnectOptions Debug output.
    let options = connect_options(&planned).unwrap();
    assert!(!format!("{options:?}").contains("s3cr3t"));
}

/// A lone userinfo value (no colon) is a token.
#[test]
fn inline_url_token_auth_is_recognized() {
    let planned = connection_plan(&config(serde_json::json!({
        "servers": ["nats://t0k3n@one:4222"],
    })))
    .unwrap();
    assert_eq!(
        planned.auth,
        NatsAuthPlan::InlineToken {
            token: "t0k3n".to_owned(),
        }
    );
    assert_eq!(planned.servers, vec!["nats://one:4222"]);
    assert!(!format!("{:?}", planned.auth).contains("t0k3n"));
}

/// An empty username with a password is still userinfo: the password must be
/// extracted and the URL stripped, not left raw (`nats://:secret@host`).
#[test]
fn empty_username_still_extracts_and_strips_the_password() {
    let planned = connection_plan(&config(serde_json::json!({
        "servers": ["nats://:s3cr3t@one:4222"],
    })))
    .unwrap();
    assert_eq!(
        planned.auth,
        NatsAuthPlan::InlineUserPassword {
            username: String::new(),
            password: "s3cr3t".to_owned(),
        }
    );
    assert_eq!(planned.servers, vec!["nats://one:4222"]);
    assert!(!planned.servers[0].contains("s3cr3t"));
    assert!(!format!("{planned:?}").contains("s3cr3t"));
}

/// Percent-encoded userinfo is decoded before use, so a password containing a
/// reserved character is sent literally, not as its encoding.
#[test]
fn percent_encoded_userinfo_is_decoded() {
    let planned = connection_plan(&config(serde_json::json!({
        "servers": ["nats://user:p%3Aw@one:4222"],
    })))
    .unwrap();
    assert_eq!(
        planned.auth,
        NatsAuthPlan::InlineUserPassword {
            username: "user".to_owned(),
            password: "p:w".to_owned(),
        }
    );
    let token = connection_plan(&config(serde_json::json!({
        "servers": ["nats://us%40er@one:4222"],
    })))
    .unwrap();
    assert_eq!(
        token.auth,
        NatsAuthPlan::InlineToken {
            token: "us@er".to_owned(),
        }
    );
}

/// Redaction removes the secret even for malformed URLs the parser rejects,
/// including passwords containing `/`, `?`, `#`, and strings with no scheme.
#[test]
fn redaction_hides_secrets_in_malformed_urls() {
    for raw in [
        "nats://relay:YWJj/ZGVm@10.0.0.1:4222",
        "nats://relay:pa://ss@10.0.0.1:4222",
        "relay:s3cr3t@127.0.0.1:4222",
        "http://relay:s3cr3t@nats.example.net:4222",
    ] {
        let redacted = crate::config::redact_url_userinfo(raw);
        assert!(!redacted.contains("s3cr3t"), "{raw} -> {redacted}");
        assert!(!redacted.contains("YWJj"), "{raw} -> {redacted}");
        assert!(!redacted.contains("pa://ss"), "{raw} -> {redacted}");
        assert!(redacted.contains('@'), "{raw} -> {redacted}");
        assert!(redacted.contains("***"), "{raw} -> {redacted}");
    }
    // A plain URL with no userinfo is returned unchanged.
    assert_eq!(
        crate::config::redact_url_userinfo("nats://host:4222"),
        "nats://host:4222"
    );
}

/// Inline credentials and an explicit `auth` block cannot coexist.
#[test]
fn inline_credentials_conflict_with_explicit_auth() {
    let error = connection_plan(&config(serde_json::json!({
        "servers": ["nats://relay:s3cr3t@one:4222"],
        "auth": {"type": "token", "token_env": "RCPS_TEST_TOKEN"},
    })))
    .unwrap_err()
    .to_string();
    assert!(error.contains("use one or the other"), "{error}");
    assert!(!error.contains("s3cr3t"), "{error}");
}

/// Every seed must carry the same inline credentials (they are one auth hand).
#[test]
fn inconsistent_inline_credentials_are_rejected() {
    let error = connection_plan(&config(serde_json::json!({
        "servers": ["nats://relay:one@a:4222", "nats://relay:two@b:4222"],
    })))
    .unwrap_err()
    .to_string();
    assert!(error.contains("inconsistent inline credentials"), "{error}");
    assert!(!error.contains("one@"), "{error}");
    assert!(!error.contains("two@"), "{error}");
}

#[test]
fn mixed_websocket_and_tcp_server_lists_are_rejected_before_connecting() {
    let planned = connection_plan(&config(serde_json::json!({
        "servers": ["nats://one:4222", "wss://two:443"],
    })));
    let error = planned.unwrap_err().to_string();
    assert!(error.contains("mixes"), "{error}");

    // A bare host without a scheme is rejected: the transport must be explicit.
    let unschemed = connection_plan(&config(serde_json::json!({
        "servers": ["localhost:4222"],
    })));
    assert!(unschemed.is_err());
}

#[test]
fn mtls_requires_certificate_and_key_together() {
    let planned = connection_plan(&config(serde_json::json!({
        "servers": ["tls://localhost"],
        "tls": {"client_cert_file": "/run/secrets/cert.pem"},
    })));
    let error = planned.unwrap_err().to_string();
    assert!(error.contains("client_cert_file"), "{error}");
}

/// A URL the parser rejects before it can split userinfo from host must not
/// echo a credential: without a trustworthy `@`, `admin:s3cr3t` (or a password
/// containing `/`, `?`, `#`) is ambiguous, so the post-scheme part is masked.
#[test]
fn malformed_url_without_userinfo_does_not_leak_a_password() {
    for server in [
        "nats://admin:s3cr3t",
        "nats://admin:s3cr3t/path",
        // Credentials after a slash but with an empty authority, and with no
        // `://` at all: the parser still cannot split userinfo from host.
        "nats:///relay:s3cr3t@localhost:4222",
        "nats:/relay:s3cr3t@localhost:4222",
        "nats:///admin:s3cr3t",
        // An `@` with empty userinfo, and passwords carrying a raw `/`, `?` or
        // `#`: the whole post-scheme part must be masked, not just the first
        // authority segment.
        "nats://@admin:s3cr3t",
        "nats://relay:pa?s3cr3t",
        "nats://relay:pa/s3cr3t",
        "nats://relay:pa#s3cr3t",
        // A credential sitting *after* the `@` (the `@` is not a reliable split).
        "nats://host@user:s3cr3t",
        // A password that itself contains `://`: the prefix is not a scheme, so
        // nothing before it can be echoed.
        "nats:relay:p://s3cr3t@host:4222",
    ] {
        let error = connection_plan(&config(serde_json::json!({ "servers": [server] })))
            .unwrap_err()
            .to_string();
        assert!(!error.contains("s3cr3t"), "{server}: {error}");
        assert!(error.contains("***"), "{server}: {error}");
    }

    // A parseable URL with an unsupported scheme still redacts only the
    // userinfo, keeping the rest of the authority readable.
    let error = connection_plan(&config(serde_json::json!({
        "servers": ["ftp://relay:s3cr3t@host:4222"],
    })))
    .unwrap_err()
    .to_string();
    assert!(!error.contains("s3cr3t"), "{error}");
}

/// A URL with no authority (`nats:user:pass@host:port`) parses as a
/// cannot-be-a-base URL, so it has no host for the client to connect to and
/// would leak the raw credentials if stored verbatim. It is rejected instead.
#[test]
fn authority_less_url_is_rejected() {
    let error = connection_plan(&config(serde_json::json!({
        "servers": ["nats:relay:s3cr3t@localhost:4222"],
    })))
    .unwrap_err()
    .to_string();
    assert!(error.contains("missing host"), "{error}");
    assert!(!error.contains("s3cr3t"), "{error}");
    assert!(!error.contains("relay:"), "{error}");
}
