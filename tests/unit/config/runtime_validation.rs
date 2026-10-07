use super::*;

fn filter_test_config() -> AppConfig {
    serde_json::from_value(serde_json::json!({
        "input": {
            "redis": {"host": "localhost"},
            "exclude_output_echoes": false,
            "subscriptions": [{"type": "subscribe", "channel": "events"}]
        },
        "outputs": {
            "out": {
                "redis": {"host": "localhost"},
                "conflation": {"interval_ms": 0}
            }
        },
        "instance_lock": {"path": "lock"}
    }))
    .expect("filter test config should deserialize")
}

#[test]
fn channel_cache_capacity_defaults_is_configurable_and_zero_disables_it() {
    let mut config = filter_test_config();
    assert_eq!(
        config.input.filter_cache_max_entries,
        DEFAULT_CHANNEL_CACHE_MAX_ENTRIES
    );
    assert_eq!(
        config.outputs["out"].filter_cache_max_entries,
        DEFAULT_CHANNEL_CACHE_MAX_ENTRIES
    );
    assert_eq!(
        config.outputs["out"].channel_policy_cache_max_entries,
        DEFAULT_CHANNEL_CACHE_MAX_ENTRIES
    );

    config.input.filter_cache_max_entries = 4096;
    config
        .outputs
        .get_mut("out")
        .unwrap()
        .filter_cache_max_entries = 8192;
    config
        .outputs
        .get_mut("out")
        .unwrap()
        .channel_policy_cache_max_entries = 4096;
    config.validate().unwrap();
    config.input.filter_cache_max_entries = 0;
    config
        .outputs
        .get_mut("out")
        .unwrap()
        .filter_cache_max_entries = 0;
    config
        .outputs
        .get_mut("out")
        .unwrap()
        .channel_policy_cache_max_entries = 0;
    config.validate().unwrap();
    config.input.filter_cache_max_entries = MAX_CHANNEL_CACHE_MAX_ENTRIES;
    config
        .outputs
        .get_mut("out")
        .unwrap()
        .filter_cache_max_entries = MAX_CHANNEL_CACHE_MAX_ENTRIES;
    config
        .outputs
        .get_mut("out")
        .unwrap()
        .channel_policy_cache_max_entries = MAX_CHANNEL_CACHE_MAX_ENTRIES;
    config.validate().unwrap();
    config.input.filter_cache_max_entries = MAX_CHANNEL_CACHE_MAX_ENTRIES + 1;
    assert!(
        config
            .validate()
            .unwrap_err()
            .to_string()
            .contains("input.filter_cache_max_entries must not exceed")
    );

    config.input.filter_cache_max_entries = 0;
    config
        .outputs
        .get_mut("out")
        .unwrap()
        .channel_policy_cache_max_entries = MAX_CHANNEL_CACHE_MAX_ENTRIES + 1;
    assert!(
        config
            .validate()
            .unwrap_err()
            .to_string()
            .contains("outputs.out.channel_policy_cache_max_entries must not exceed")
    );
}

#[test]
fn configured_runtime_durations_are_limited_to_avoid_instant_overflow() {
    let mut config: AppConfig = serde_json::from_str(
            r#"{
                "input":{"redis":{"host":"input.example.net"},"subscriptions":[{"type":"subscribe","channel":"events"}]},
                "outputs":{"custom-output":{"redis":{"host":"output.example.net"},"conflation":{"interval_ms":0}}},
                "instance_lock":{"path":"lock"}
            }"#,
        )
        .unwrap();
    config.validate().unwrap();

    config
        .outputs
        .get_mut("custom-output")
        .unwrap()
        .conflation
        .interval_ms = MAX_RUNTIME_DURATION_MS + 1;
    assert!(
        config
            .validate()
            .unwrap_err()
            .to_string()
            .contains("interval_ms")
    );
    config
        .outputs
        .get_mut("custom-output")
        .unwrap()
        .conflation
        .interval_ms = 0;
    config
        .outputs
        .get_mut("custom-output")
        .unwrap()
        .deduplication
        .ttl_ms = MAX_RUNTIME_DURATION_MS + 1;
    assert!(
        config
            .validate()
            .unwrap_err()
            .to_string()
            .contains("ttl_ms")
    );
    config
        .outputs
        .get_mut("custom-output")
        .unwrap()
        .deduplication
        .ttl_ms = 5000;
    config.input.redis.connect_timeout_ms = MAX_RUNTIME_DURATION_MS as u64 + 1;
    assert!(
        config
            .validate()
            .unwrap_err()
            .to_string()
            .contains("connect_timeout_ms")
    );
    config.input.redis.connect_timeout_ms = 5000;
    config.status.path = Some(PathBuf::from("status.json"));
    config.status.update_interval_ms = MAX_RUNTIME_DURATION_MS as u64 + 1;
    assert!(
        config
            .validate()
            .unwrap_err()
            .to_string()
            .contains("update_interval_ms")
    );
}

#[test]
fn channel_filters_default_to_accept_and_validate_glob_and_regex_rules() {
    let mut config = filter_test_config();
    assert_eq!(config.input.filter_default, FilterAction::Accept);
    assert_eq!(config.outputs["out"].filter_default, FilterAction::Accept);
    config.input.filters.push(ChannelFilterRule {
        glob: Some("events:internal:*".to_owned()),
        regex: None,
        action: FilterAction::Deny,
        ..Default::default()
    });
    config
        .outputs
        .get_mut("out")
        .unwrap()
        .filters
        .push(ChannelFilterRule {
            glob: None,
            regex: Some(r"^events:public:[0-9]+$".to_owned()),
            action: FilterAction::Accept,
            ..Default::default()
        });
    config.validate().unwrap();
}

#[test]
fn filter_default_is_canonical_and_accepts_the_earlier_draft_alias() {
    let config: AppConfig = serde_json::from_value(serde_json::json!({
        "input": {
            "redis": {"host": "localhost"},
            "subscriptions": [{"type": "subscribe", "channel": "events"}],
            "filter_default": "deny"
        },
        "outputs": {
            "out": {
                "redis": {"host": "output.local"},
                "conflation": {"interval_ms": 0},
                "default_filter": "deny"
            }
        },
        "instance_lock": {"path": "lock"}
    }))
    .unwrap();

    assert_eq!(config.input.filter_default, FilterAction::Deny);
    assert_eq!(config.outputs["out"].filter_default, FilterAction::Deny);
    config.validate().unwrap();
}

#[test]
fn channel_filters_reject_ambiguous_empty_and_invalid_regex_rules() {
    let mut config = filter_test_config();
    config.input.filters.push(ChannelFilterRule {
        glob: Some("events:*".to_owned()),
        regex: Some("^events:.*$".to_owned()),
        action: FilterAction::Deny,
        ..Default::default()
    });
    assert!(
        config
            .validate()
            .unwrap_err()
            .to_string()
            .contains("input.filters[0] must specify exactly one")
    );

    config.input.filters.clear();
    config.input.filters.push(ChannelFilterRule {
        glob: None,
        regex: None,
        action: FilterAction::Deny,
        ..Default::default()
    });
    assert!(
        config
            .validate()
            .unwrap_err()
            .to_string()
            .contains("input.filters[0] must specify exactly one")
    );

    config.input.filters.clear();
    config.input.filters.push(ChannelFilterRule {
        glob: Some("  ".to_owned()),
        regex: None,
        action: FilterAction::Deny,
        ..Default::default()
    });
    assert!(
        config
            .validate()
            .unwrap_err()
            .to_string()
            .contains("input.filters[0].glob must not be empty")
    );

    config.input.filters.clear();
    config
        .outputs
        .get_mut("out")
        .unwrap()
        .filters
        .push(ChannelFilterRule {
            glob: None,
            regex: Some("[".to_owned()),
            action: FilterAction::Deny,
            ..Default::default()
        });
    assert!(
        config
            .validate()
            .unwrap_err()
            .to_string()
            .contains("outputs.out.filters[0].regex is invalid")
    );
}
