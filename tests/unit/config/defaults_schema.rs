use super::*;

#[test]
fn example_config_uses_one_input_and_named_outputs() {
    let config: AppConfig =
        serde_json::from_str(include_str!("../../../config.example.json")).unwrap();
    config.validate().unwrap();

    assert_eq!(config.input.redis.host, "localhost");
    assert_eq!(config.input.redis.database, 0);
    assert_eq!(
        config.input.filter_cache_max_entries,
        DEFAULT_CHANNEL_CACHE_MAX_ENTRIES
    );
    assert!(config.input.exclude_output_echoes);
    assert!(config.input.exclude_sentinel_pubsub);
    assert!(config.input.filters.is_empty());
    assert_eq!(config.input.filter_default, FilterAction::Accept);
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
    assert!(config.outputs["output0"].filters.is_empty());
    assert_eq!(
        config.outputs["output0"].filter_cache_max_entries,
        DEFAULT_CHANNEL_CACHE_MAX_ENTRIES
    );
    assert_eq!(
        config.outputs["output0"].channel_policy_cache_max_entries,
        DEFAULT_CHANNEL_CACHE_MAX_ENTRIES
    );
    assert_eq!(
        config.outputs["output0"].filter_default,
        FilterAction::Accept
    );
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
    assert!(
        !config
            .status
            .http
            .as_ref()
            .unwrap()
            .filters_endpoint_enabled
    );
    assert_eq!(config.logging.prefix, "redis-conflated-pubsub");
    assert!(config.logging.enabled);
}

#[test]
fn logging_prefix_defaults_and_must_not_be_empty_or_contain_path_separators() {
    let mut config: AppConfig = serde_json::from_str(
            r#"{
                "input":{"redis":{"host":"input.example.net"},"subscriptions":[{"type":"subscribe","channel":"events"}]},
                "outputs":{"custom-output":{"redis":{"host":"output.example.net"},"conflation":{"interval_ms":0}}},
                "instance_lock":{"path":"lock"}
            }"#,
        )
        .unwrap();
    assert_eq!(config.logging.prefix, "redis-conflated-pubsub");
    assert!(config.logging.enabled);
    config.validate().unwrap();

    config.logging.prefix = String::new();
    let error = config.validate().unwrap_err();
    assert!(error.to_string().contains("logging.prefix"));

    config.logging.prefix = "nested/logs".to_owned();
    let error = config.validate().unwrap_err();
    assert!(error.to_string().contains("logging.prefix"));

    config.logging.prefix = "nested\\logs".to_owned();
    let error = config.validate().unwrap_err();
    assert!(error.to_string().contains("logging.prefix"));

    config.logging.prefix = "service-a".to_owned();
    config.validate().unwrap();

    let schema = AppConfig::json_schema();
    let logging_properties = &schema["$defs"]["LoggingConfig"]["properties"];
    assert_eq!(
        logging_properties["prefix"]["default"],
        "redis-conflated-pubsub"
    );
    assert_eq!(logging_properties["prefix"]["minLength"], 1);
    assert_eq!(logging_properties["enabled"]["default"], true);
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
        0
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
fn deduplication_extras_default_to_disabled_and_unlimited() {
    let config: AppConfig = serde_json::from_str(
            r#"{
                "input":{"redis":{"host":"input.example.net"},"subscriptions":[{"type":"subscribe","channel":"events"}]},
                "outputs":{"custom-output":{"redis":{"host":"output.example.net"},"conflation":{"interval_ms":0}}},
                "instance_lock":{"path":"lock"}
            }"#,
        )
        .unwrap();
    let deduplication = &config.outputs["custom-output"].deduplication;
    assert!(!deduplication.in_flight_suppression);
    assert_eq!(deduplication.max_entries, None);
    assert_eq!(deduplication.max_cache_bytes, None);
    config.validate().unwrap();

    let configured: AppConfig = serde_json::from_str(
            r#"{
                "input":{"redis":{"host":"input.example.net"},"subscriptions":[{"type":"subscribe","channel":"events"}]},
                "outputs":{"custom-output":{"redis":{"host":"output.example.net"},"conflation":{"interval_ms":0},"deduplication":{"ttl_ms":5000,"in_flight_suppression":true,"max_entries":64,"max_cache_bytes":4096}}},
                "instance_lock":{"path":"lock"}
            }"#,
        )
        .unwrap();
    configured.validate().unwrap();
    let deduplication = &configured.outputs["custom-output"].deduplication;
    assert!(deduplication.in_flight_suppression);
    assert_eq!(deduplication.max_entries, Some(64));
    assert_eq!(deduplication.max_cache_bytes, Some(4096));

    let schema = AppConfig::json_schema();
    let properties = &schema["$defs"]["DeduplicationConfig"]["properties"];
    assert_eq!(properties["in_flight_suppression"]["default"], false);
    assert_eq!(
        properties["max_entries"]["default"],
        serde_json::Value::Null
    );
    assert_eq!(properties["max_entries"]["minimum"], 1);
    assert_eq!(
        properties["max_entries"]["maximum"],
        MAX_DEDUPLICATION_CACHE_ENTRIES
    );
    assert_eq!(
        properties["max_cache_bytes"]["default"],
        serde_json::Value::Null
    );
    assert_eq!(properties["max_cache_bytes"]["minimum"], 1);
    assert_eq!(
        properties["max_cache_bytes"]["maximum"],
        MAX_DEDUPLICATION_CACHE_BYTES
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
fn channel_filter_schema_defaults_and_selector_constraints_are_public() {
    let schema = AppConfig::json_schema();
    assert_eq!(schema["$defs"]["FilterAction"]["default"], "accept");
    assert_eq!(
        schema["$defs"]["ChannelFilterRule"]["oneOf"]
            .as_array()
            .unwrap()
            .len(),
        5
    );
    for definition in ["InputConfig", "OutputConfig"] {
        let properties = &schema["$defs"][definition]["properties"];
        assert_eq!(properties["filters"]["default"], serde_json::json!([]));
        assert_eq!(properties["filter_default"]["default"], "accept");
        assert_eq!(
            properties["filter_cache_max_entries"]["default"],
            DEFAULT_CHANNEL_CACHE_MAX_ENTRIES
        );
        assert_eq!(properties["filter_cache_max_entries"]["minimum"], 0);
        assert_eq!(
            properties["filter_cache_max_entries"]["maximum"],
            MAX_CHANNEL_CACHE_MAX_ENTRIES
        );
    }
    let output_properties = &schema["$defs"]["OutputConfig"]["properties"];
    assert_eq!(
        output_properties["channel_policy_cache_max_entries"]["default"],
        DEFAULT_CHANNEL_CACHE_MAX_ENTRIES
    );
    assert_eq!(
        output_properties["channel_policy_cache_max_entries"]["minimum"],
        0
    );
    assert_eq!(
        output_properties["channel_policy_cache_max_entries"]["maximum"],
        MAX_CHANNEL_CACHE_MAX_ENTRIES
    );
    assert_eq!(
        schema["$defs"]["HttpStatusConfig"]["properties"]["filters_endpoint_enabled"]["default"],
        false
    );
    for selector in ["glob", "regex", "raw", "single", "string"] {
        assert_eq!(
            schema["$defs"]["ChannelFilterRule"]["properties"][selector]["minLength"],
            1
        );
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
fn conflation_in_flight_limits_default_to_none_and_must_be_positive() {
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
            .max_in_flight_commands,
        None
    );
    assert_eq!(
        config.outputs["custom-output"]
            .conflation
            .max_in_flight_bytes,
        None
    );
    config.validate().unwrap();

    let schema = AppConfig::json_schema();
    let properties = &schema["$defs"]["ConflationConfig"]["properties"];
    assert_eq!(properties["max_in_flight_commands"]["minimum"], 1);
    assert_eq!(
        properties["max_in_flight_commands"]["default"],
        serde_json::Value::Null
    );
    assert_eq!(properties["max_in_flight_bytes"]["minimum"], 1);
    assert_eq!(
        properties["max_in_flight_bytes"]["default"],
        serde_json::Value::Null
    );

    let overridden: AppConfig = serde_json::from_str(
            r#"{
                "input":{"redis":{"host":"input.example.net"},"subscriptions":[{"type":"subscribe","channel":"events"}]},
                "outputs":{"custom-output":{"redis":{"host":"output.example.net"},"conflation":{"interval_ms":10,"max_in_flight_commands":8,"max_in_flight_bytes":4096}}},
                "instance_lock":{"path":"lock"}
            }"#,
        )
        .unwrap();
    assert_eq!(
        overridden.outputs["custom-output"]
            .conflation
            .max_in_flight_commands,
        Some(8)
    );
    assert_eq!(
        overridden.outputs["custom-output"]
            .conflation
            .max_in_flight_bytes,
        Some(4096)
    );
    overridden.validate().unwrap();

    config
        .outputs
        .get_mut("custom-output")
        .unwrap()
        .conflation
        .max_in_flight_commands = Some(0);
    let error = config.validate().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("outputs.custom-output.conflation.max_in_flight_commands")
    );

    config
        .outputs
        .get_mut("custom-output")
        .unwrap()
        .conflation
        .max_in_flight_commands = None;
    config
        .outputs
        .get_mut("custom-output")
        .unwrap()
        .conflation
        .max_in_flight_bytes = Some(0);
    let error = config.validate().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("outputs.custom-output.conflation.max_in_flight_bytes")
    );
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
