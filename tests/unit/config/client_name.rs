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
fn client_name_defaults_to_program_version_and_role() {
    let config = minimal_config();
    let version = env!("CARGO_PKG_VERSION");

    assert_eq!(config.client_name, None);
    assert_eq!(
        config.client_name_for("input"),
        Some(format!("ConflatedPS-{version}-input"))
    );
    assert_eq!(
        config.client_name_for("output-out-a"),
        Some(format!("ConflatedPS-{version}-output-out-a"))
    );
}

#[test]
fn client_name_template_expands_version_and_empty_disables_naming() {
    let mut config = minimal_config();

    config.client_name = Some("my-bridge".to_owned());
    assert_eq!(
        config.client_name_for("input"),
        Some("my-bridge-input".to_owned())
    );

    config.client_name = Some("bridge/{version}/agent".to_owned());
    assert_eq!(
        config.client_name_for("input"),
        Some(format!("bridge/{}/agent-input", env!("CARGO_PKG_VERSION")))
    );

    config.client_name = Some(String::new());
    assert_eq!(config.client_name_for("input"), None);
    config.validate().unwrap();
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

#[test]
fn client_name_schema_exposes_default_and_length_limit() {
    let schema = AppConfig::json_schema();
    assert_eq!(
        schema["properties"]["client_name"]["default"],
        "ConflatedPS-{version}"
    );
    assert_eq!(
        schema["properties"]["client_name"]["maxLength"],
        MAX_CLIENT_NAME_TEMPLATE_LEN
    );
}
