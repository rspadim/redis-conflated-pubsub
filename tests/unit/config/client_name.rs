use super::*;

fn minimal_config() -> AppConfig {
    serde_json::from_str(
        r#"{
            "input": {
                "redis": {"host": "input.example.net"},
                "subscriptions": [{"type": "psubscribe", "pattern": "*"}]
            },
            "outputs": {
                "out-a": {
                    "redis": {"host": "output.example.net"},
                    "conflation": {"interval_ms": 0}
                }
            },
            "instance_lock": {"path": "lock"}
        }"#,
    )
    .unwrap()
}

#[test]
fn client_name_defaults_include_version_role_user_and_host() {
    let version = env!("CARGO_PKG_VERSION");
    assert_eq!(
        resolve_client_name_with(None, "i", "ACME\\alice", "node-01"),
        Some(format!("ConflatedPS-{version}-i::ACME\\alice::node-01"))
    );
    assert_eq!(
        resolve_client_name_with(None, "o-out-a", "ACME\\alice", "node-01"),
        Some(format!(
            "ConflatedPS-{version}-o-out-a::ACME\\alice::node-01"
        ))
    );

    // The config-facing helper resolves the identity from the environment.
    let config = minimal_config();
    let name = config.client_name_for("i").unwrap();
    assert!(name.starts_with(&format!("ConflatedPS-{version}-i::")));
    assert!(name.contains("::"));
}

#[test]
fn client_name_template_expands_placeholders_and_empty_disables_naming() {
    let mut config = minimal_config();
    let version = env!("CARGO_PKG_VERSION");

    config.client_name = Some("my-bridge-{role}@{version}".to_owned());
    assert_eq!(
        config.client_name_for("i"),
        Some(format!("my-bridge-i@{version}"))
    );

    // Without {role} the role is appended after a dash.
    config.client_name = Some("my-bridge".to_owned());
    assert_eq!(config.client_name_for("i"), Some("my-bridge-i".to_owned()));

    config.client_name = Some(String::new());
    assert_eq!(config.client_name_for("i"), None);
    config.validate().unwrap();
}

#[test]
fn client_name_placeholders_use_the_injected_identity() {
    assert_eq!(
        resolve_client_name_with(Some("{version}/{role}::{user}::{host}"), "o-x", "R\\u", "H"),
        Some(format!("{}/o-x::R\\u::H", env!("CARGO_PKG_VERSION")))
    );
}

#[test]
fn client_name_validation_rejects_whitespace_and_oversized_templates() {
    let mut config = minimal_config();

    config.client_name = Some("has space".to_owned());
    assert!(
        config
            .validate()
            .unwrap_err()
            .to_string()
            .contains("whitespace")
    );

    config.client_name = Some("x".repeat(MAX_CLIENT_NAME_TEMPLATE_LEN + 1));
    assert!(
        config
            .validate()
            .unwrap_err()
            .to_string()
            .contains("must not exceed")
    );

    config.client_name = Some(String::new());
    config.validate().unwrap();
}

#[cfg(target_os = "linux")]
#[test]
fn system_hostname_falls_back_to_the_os_hostname_on_linux() {
    let hostname = system_hostname().expect("Linux must expose an OS hostname");
    assert!(!hostname.is_empty());
    assert!(!hostname.chars().any(char::is_whitespace));
}

#[test]
fn client_name_schema_exposes_default_and_length_limit() {
    let schema = AppConfig::json_schema();
    assert_eq!(
        schema["properties"]["client_name"]["default"],
        "ConflatedPS-{version}-{role}::{user}::{host}"
    );
    assert_eq!(
        schema["properties"]["client_name"]["maxLength"],
        MAX_CLIENT_NAME_TEMPLATE_LEN
    );
}
