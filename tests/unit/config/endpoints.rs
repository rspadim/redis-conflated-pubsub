use super::*;

fn base_json(input: &str, output: &str) -> String {
    format!(
        r#"{{
            "input": {input},
            "outputs": {{ "out": {output} }},
            "instance_lock": {{ "path": "lock" }}
        }}"#
    )
}

fn redis_input() -> String {
    r#"{"redis":{"host":"input.example.net"},"subscriptions":[{"type":"subscribe","channel":"events"}]}"#.to_owned()
}

fn redis_output() -> String {
    r#"{"redis":{"host":"output.example.net"},"conflation":{"interval_ms":0}}"#.to_owned()
}

#[test]
fn legacy_redis_json_still_parses_and_resolves_to_the_redis_backend() {
    // The 0.2 shape has no `type` discriminator and must keep working verbatim.
    let config: AppConfig =
        serde_json::from_str(include_str!("../../../config.example.json")).unwrap();
    config.validate().unwrap();
    assert!(matches!(
        config.input.endpoint("input").unwrap(),
        Endpoint::Redis(_)
    ));
    assert!(matches!(
        config.outputs["output0"]
            .endpoint("outputs.output0")
            .unwrap(),
        Endpoint::Redis(_)
    ));
    assert_eq!(config.input.redis.as_ref().unwrap().host, "localhost");
    assert_eq!(config.outputs["output0"].redis_endpoint().host, "localhost");

    // A minimal legacy config also keeps its exact Redis semantics.
    let minimal: AppConfig =
        serde_json::from_str(&base_json(&redis_input(), &redis_output())).unwrap();
    minimal.validate().unwrap();
    assert_eq!(minimal.input.redis.as_ref().unwrap().port, 6379);
    assert!(minimal.input.nats.is_none());
    assert!(minimal.outputs["out"].nats.is_none());
}

#[test]
fn exactly_one_backend_is_required_for_input_and_outputs() {
    // No backend on the input.
    let none_input = base_json(
        r#"{"subscriptions":[{"type":"subscribe","channel":"events"}]}"#,
        &redis_output(),
    );
    let config: AppConfig = serde_json::from_str(&none_input).unwrap();
    let error = config.validate().unwrap_err();
    assert!(
        error.to_string().contains("input must select one backend"),
        "{error}"
    );

    // Both backends on the input.
    let both_input = base_json(
        r#"{"redis":{"host":"input.example.net"},"nats":{"servers":["nats://localhost:4222"]},"subscriptions":[{"type":"subscribe","channel":"events"}]}"#,
        &redis_output(),
    );
    let config: AppConfig = serde_json::from_str(&both_input).unwrap();
    let error = config.validate().unwrap_err();
    assert!(
        error.to_string().contains("both `redis` and `nats`"),
        "{error}"
    );

    // No backend on an output.
    let none_output = base_json(&redis_input(), r#"{"conflation":{"interval_ms":0}}"#);
    let config: AppConfig = serde_json::from_str(&none_output).unwrap();
    let error = config.validate().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("outputs.out must select one backend"),
        "{error}"
    );

    // Both backends on an output.
    let both_output = base_json(
        &redis_input(),
        r#"{"redis":{"host":"output.example.net"},"nats":{"servers":["nats://localhost:4222"]},"conflation":{"interval_ms":0}}"#,
    );
    let config: AppConfig = serde_json::from_str(&both_output).unwrap();
    let error = config.validate().unwrap_err();
    assert!(
        error.to_string().contains("both `redis` and `nats`"),
        "{error}"
    );
}

#[test]
fn nats_backend_selects_and_parses_all_auth_variants() {
    for auth in [
        r#"{"type":"none"}"#,
        r#"{"type":"user_password","username":"relay","password_env":"NATS_PASSWORD"}"#,
        r#"{"type":"token","token_env":"NATS_TOKEN"}"#,
        r#"{"type":"nkey","seed_file":"/run/secrets/nats.nk"}"#,
        r#"{"type":"credentials_file","path":"/run/secrets/nats.creds"}"#,
    ] {
        let nats = format!(
            r#"{{"servers":["nats://localhost:4222","nats://other:4222"],"name":"relay","connect_timeout_ms":2500,"ignore_discovered_servers":true,"auth":{auth},"tls":{{"root_cert_file":"/run/secrets/ca.pem","client_cert_file":"/run/secrets/cert.pem","client_key_file":"/run/secrets/key.pem"}}}}"#
        );
        let input = format!(
            r#"{{"nats":{nats},"subscriptions":[{{"type":"nats_subject","subject":"prices.>"}}]}}"#
        );
        // The output stays Redis so the config remains otherwise valid.
        let config: AppConfig = serde_json::from_str(&base_json(&input, &redis_output())).unwrap();
        config.validate().unwrap();
        let nats = config.input.nats.as_ref().unwrap();
        assert_eq!(nats.servers.len(), 2);
        assert_eq!(nats.name.as_deref(), Some("relay"));
        assert_eq!(nats.connect_timeout_ms, 2500);
        assert!(nats.ignore_discovered_servers);
        assert!(nats.tls.as_ref().unwrap().root_cert_file.is_some());
        // The endpoint resolves to NATS, not Redis.
        assert!(matches!(
            config.input.endpoint("input").unwrap(),
            Endpoint::Nats(_)
        ));
    }
    assert!(matches!(NatsAuth::default(), NatsAuth::None));
}

#[test]
fn nats_auth_and_tls_reject_ambiguous_or_empty_settings() {
    // mTLS requires certificate and key together.
    let input = r#"{"nats":{"servers":["nats://localhost"],"tls":{"client_cert_file":"/c.pem"}},"subscriptions":[{"type":"nats_subject","subject":"a"}]}"#;
    let config: AppConfig = serde_json::from_str(&base_json(input, &redis_output())).unwrap();
    let error = config.validate().unwrap_err();
    assert!(
        error.to_string().contains("must be configured together"),
        "{error}"
    );

    // Unknown auth types are rejected at parse time.
    assert!(serde_json::from_str::<NatsAuth>(r#"{"type":"bearer","token_env":"T"}"#).is_err());
}

#[test]
fn nats_subject_validation_accepts_wildcards_and_rejects_malformed_subjects() {
    for subject in [
        "events",
        "events.orders",
        "*",
        "events.*",
        "events.>",
        ">",
        "a.*.b",
    ] {
        assert!(
            super::validate::validate_nats_subject("subject", subject).is_ok(),
            "expected {subject:?} to be valid"
        );
    }
    for (subject, needle) in [
        ("", "must not be empty"),
        ("events orders", "must not contain whitespace"),
        ("events.\torders", "must not contain whitespace"),
        ("events..orders", "empty tokens"),
        (".events", "empty tokens"),
        ("events.", "empty tokens"),
        ("events.>.orders", "`>` must be the final token"),
        ("ev*ents", "full token"),
        ("events.ord>ers", "full token"),
    ] {
        let error = super::validate::validate_nats_subject("subject", subject).unwrap_err();
        assert!(
            error.to_string().contains(needle),
            "{subject:?}: expected {needle:?}, got {error}"
        );
    }
}

#[test]
fn redis_and_nats_subscriptions_must_match_the_selected_input_backend() {
    // A NATS subject under a Redis input is rejected.
    let input = r#"{"redis":{"host":"input.example.net"},"subscriptions":[{"type":"nats_subject","subject":"events.>"}]}"#;
    let config: AppConfig = serde_json::from_str(&base_json(input, &redis_output())).unwrap();
    let error = config.validate().unwrap_err();
    assert!(
        error.to_string().contains("requires a `nats` input"),
        "{error}"
    );

    // A Redis subscription under a NATS input is rejected.
    let input = r#"{"nats":{"servers":["nats://localhost"]},"subscriptions":[{"type":"subscribe","channel":"events"}]}"#;
    let config: AppConfig = serde_json::from_str(&base_json(input, &redis_output())).unwrap();
    let error = config.validate().unwrap_err();
    assert!(
        error.to_string().contains("requires a `redis` input"),
        "{error}"
    );

    // A malformed NATS subject is reported against the subscription path.
    let input = r#"{"nats":{"servers":["nats://localhost"]},"subscriptions":[{"type":"nats_subject","subject":"events.>.x"}]}"#;
    let config: AppConfig = serde_json::from_str(&base_json(input, &redis_output())).unwrap();
    let error = config.validate().unwrap_err();
    assert!(
        error.to_string().contains("input.subscriptions[0].subject"),
        "{error}"
    );
}

#[test]
fn duplicate_nats_subjects_are_rejected() {
    // The subject is the mapping identity, so a duplicate would deliver twice
    // and only ever apply the first mapping. Reject it, and name the index.
    let input = r#"{"nats":{"servers":["nats://localhost"]},"subscriptions":[{"type":"nats_subject","subject":"events.>","output_prefix":"a."},{"type":"nats_subject","subject":"events.>","output_prefix":"b."}]}"#;
    let config: AppConfig = serde_json::from_str(&base_json(input, &redis_output())).unwrap();
    let error = config.validate().unwrap_err();
    let message = error.to_string();
    assert!(
        message.contains("input.subscriptions[1].subject"),
        "{message}"
    );
    assert!(message.contains("more than once"), "{message}");
}

#[test]
fn generated_schema_expresses_exactly_one_backend() {
    let schema = AppConfig::json_schema();
    for definition in ["InputConfig", "OutputConfig"] {
        let all_of = schema["$defs"][definition]["allOf"].as_array().unwrap();
        let rule = all_of
            .iter()
            .find(|rule| rule.get("oneOf").is_some())
            .unwrap_or_else(|| panic!("{definition} is missing the exactly-one-backend rule"));
        let options = rule["oneOf"].as_array().unwrap();
        assert_eq!(options.len(), 2);
        assert!(
            options
                .iter()
                .any(|o| o["required"] == serde_json::json!(["redis"]))
        );
        assert!(
            options
                .iter()
                .any(|o| o["required"] == serde_json::json!(["nats"]))
        );
    }
    // The NATS types and auth tag are part of the public schema.
    assert!(schema["$defs"]["NatsConfig"].is_object());
    assert_eq!(
        schema["$defs"]["NatsAuth"]["oneOf"]
            .as_array()
            .unwrap()
            .len(),
        5
    );
    // `redis` and `nats` are optional in the property list; the oneOf enforces
    // presence, so existing Redis JSON keeps validating without a discriminator.
    let input_properties = &schema["$defs"]["InputConfig"]["properties"];
    assert!(input_properties.get("redis").is_some());
    assert!(input_properties.get("nats").is_some());
    assert!(
        !schema["$defs"]["InputConfig"]["required"]
            .as_array()
            .unwrap()
            .iter()
            .any(|field| field == "redis" || field == "nats")
    );
}
