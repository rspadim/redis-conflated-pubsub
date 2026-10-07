use super::*;

#[test]
fn channel_profiles_and_inline_policies_share_the_internal_override_view() {
    let profile: ChannelProfile = serde_json::from_value(serde_json::json!({
        "conflation.interval_ms": 125,
        "deduplication.ttl_ms": 900,
        "deduplication.group": "workers"
    }))
    .unwrap();
    let policy: ChannelPolicy = serde_json::from_value(serde_json::json!({
        "default": true,
        "conflation.interval_ms": 125,
        "deduplication.ttl_ms": 900,
        "deduplication.group": "workers"
    }))
    .unwrap();

    assert_eq!(
        ChannelOverrides::from(&profile),
        ChannelOverrides::from(&policy)
    );
    assert!(ChannelOverrides::from(&profile).has_any());
}

#[test]
fn channel_profiles_policies_and_groups_use_literal_dotted_keys() {
    let config: AppConfig = serde_json::from_str(
            r#"{
                "input":{"redis":{"host":"input.example.net"},"subscriptions":[{"type":"subscribe","channel":"events"}]},
                "outputs":{"custom-output":{
                    "redis":{"host":"output.example.net"},
                    "conflation":{"interval_ms":10},
                    "deduplication_groups":{
                        "workers":{"ttl_ms":3000,"round_ms":3000},
                        "disabled":{"ttl_ms":0,"round_ms":9000,"restart_on_change":false}
                    },
                    "profiles":{
                        "grouped":{"conflation.interval_ms":-25,"deduplication.group":"workers"},
                        "no-dedup":{"deduplication.ttl_ms":0}
                    },
                    "channel_policies":[
                        {"glob":"orders:*","profile":"grouped"},
                        {"prefix":"critical:","profile":null,"conflation.interval_ms":0,"deduplication.group":"workers"},
                        {"suffix":":raw","deduplication.ttl_ms":-10,"deduplication.group":null},
                        {"default":true,"profile":"no-dedup"}
                    ]
                }},
                "instance_lock":{"path":"lock"}
            }"#,
        )
        .unwrap();

    config.validate().unwrap();
    let output = &config.outputs["custom-output"];
    assert_eq!(output.deduplication_groups["workers"].ttl_ms, 3000);
    assert_eq!(output.deduplication_groups["workers"].round_ms, 3000);
    assert!(output.deduplication_groups["workers"].restart_on_change);
    assert_eq!(output.deduplication_groups["workers"].max_members, 16_384);
    assert_eq!(
        output.deduplication_groups["workers"].max_cache_bytes,
        64 * 1024 * 1024
    );
    assert_eq!(output.deduplication_groups["disabled"].ttl_ms, 0);
    assert_eq!(output.deduplication_groups["disabled"].round_ms, 9000);
    assert!(!output.deduplication_groups["disabled"].restart_on_change);
    assert_eq!(output.profiles["grouped"].conflation_interval_ms, Some(-25));
    assert_eq!(
        output.profiles["grouped"].deduplication_group.as_deref(),
        Some("workers")
    );
    assert_eq!(output.profiles["no-dedup"].deduplication_ttl_ms, Some(0));

    let policies = &output.channel_policies;
    assert_eq!(policies.len(), 4);
    assert_eq!(policies[0].glob.as_deref(), Some("orders:*"));
    assert_eq!(policies[0].profile.as_deref(), Some("grouped"));
    assert_eq!(policies[1].prefix.as_deref(), Some("critical:"));
    assert_eq!(policies[1].profile, None);
    assert_eq!(policies[1].conflation_interval_ms, Some(0));
    assert_eq!(policies[1].deduplication_group.as_deref(), Some("workers"));
    assert_eq!(policies[2].suffix.as_deref(), Some(":raw"));
    assert_eq!(policies[2].deduplication_ttl_ms, Some(-10));
    assert_eq!(policies[2].deduplication_group, None);
    assert!(policies[3].default);
    assert_eq!(policies[3].profile.as_deref(), Some("no-dedup"));

    assert_eq!(
        serde_json::to_value(&policies[0]).unwrap(),
        serde_json::json!({"glob": "orders:*", "profile": "grouped"})
    );
    assert_eq!(
        serde_json::to_value(&policies[3]).unwrap(),
        serde_json::json!({"default": true, "profile": "no-dedup"})
    );
    assert_eq!(
        serde_json::to_value(&output.profiles["grouped"]).unwrap(),
        serde_json::json!({
            "conflation.interval_ms": -25,
            "deduplication.group": "workers"
        })
    );

    assert!(
        serde_json::from_str::<ChannelProfile>(r#"{"conflation":{"interval_ms":25}}"#).is_err()
    );
    assert!(
        serde_json::from_str::<ChannelPolicy>(
            r#"{"glob":"orders:*","deduplication":{"ttl_ms":25}}"#
        )
        .is_err()
    );
}
#[test]
fn deduplication_group_rounding_is_bounded_by_positive_ttl() {
    let mut config: AppConfig = serde_json::from_str(
            r#"{
                "input":{"redis":{"host":"input.example.net"},"subscriptions":[{"type":"subscribe","channel":"events"}]},
                "outputs":{"custom-output":{
                    "redis":{"host":"output.example.net"},
                    "conflation":{"interval_ms":10},
                    "deduplication_groups":{
                        "equal":{"ttl_ms":50,"round_ms":50},
                        "unrounded":{"ttl_ms":25,"round_ms":0},
                        "disabled-zero":{"ttl_ms":0,"round_ms":10000},
                        "disabled-negative":{"ttl_ms":-5,"round_ms":10000}
                    }
                }},
                "instance_lock":{"path":"lock"}
            }"#,
        )
        .unwrap();
    config.validate().unwrap();

    config
        .outputs
        .get_mut("custom-output")
        .unwrap()
        .deduplication_groups
        .get_mut("equal")
        .unwrap()
        .round_ms = 51;
    let error = config.validate().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("round_ms must not exceed a positive ttl_ms")
    );

    config
        .outputs
        .get_mut("custom-output")
        .unwrap()
        .deduplication_groups
        .get_mut("equal")
        .unwrap()
        .ttl_ms = 0;
    config.validate().unwrap();

    config
        .outputs
        .get_mut("custom-output")
        .unwrap()
        .deduplication_groups
        .get_mut("equal")
        .unwrap()
        .ttl_ms = -1;
    config.validate().unwrap();

    config
        .outputs
        .get_mut("custom-output")
        .unwrap()
        .deduplication_groups
        .get_mut("equal")
        .unwrap()
        .max_members = 0;
    assert!(
        config
            .validate()
            .unwrap_err()
            .to_string()
            .contains("max_members must be greater than zero")
    );
    config
        .outputs
        .get_mut("custom-output")
        .unwrap()
        .deduplication_groups
        .get_mut("equal")
        .unwrap()
        .max_members = MAX_DEDUPLICATION_GROUP_MEMBERS + 1;
    assert!(
        config
            .validate()
            .unwrap_err()
            .to_string()
            .contains("max_members must not exceed")
    );
    let group = config
        .outputs
        .get_mut("custom-output")
        .unwrap()
        .deduplication_groups
        .get_mut("equal")
        .unwrap();
    group.max_members = default_group_max_members();
    group.max_cache_bytes = MAX_DEDUPLICATION_GROUP_CACHE_BYTES + 1;
    assert!(
        config
            .validate()
            .unwrap_err()
            .to_string()
            .contains("max_cache_bytes must not exceed")
    );
    assert!(serde_json::from_str::<DeduplicationGroup>(r#"{"ttl_ms":25,"round_ms":-1}"#).is_err());
}
#[test]
fn channel_policy_validates_selectors_modes_and_output_local_references() {
    let mut config: AppConfig = serde_json::from_str(
            r#"{
                "input":{"redis":{"host":"input.example.net"},"subscriptions":[{"type":"subscribe","channel":"events"}]},
                "outputs":{"custom-output":{
                    "redis":{"host":"output.example.net"},
                    "conflation":{"interval_ms":10},
                    "deduplication_groups":{"workers":{"ttl_ms":-1,"round_ms":0}},
                    "profiles":{"grouped":{"deduplication.group":"workers"}},
                    "channel_policies":[
                        {"glob":"orders:*","profile":"grouped"},
                        {"default":true,"profile":"grouped"}
                    ]
                }},
                "instance_lock":{"path":"lock"}
            }"#,
        )
        .unwrap();
    config.validate().unwrap();

    config
        .outputs
        .get_mut("custom-output")
        .unwrap()
        .channel_policies[1]
        .default = false;
    let error = config.validate().unwrap_err();
    assert!(error.to_string().contains("exactly one default rule"));

    {
        let policies = &mut config
            .outputs
            .get_mut("custom-output")
            .unwrap()
            .channel_policies;
        policies[0].default = true;
        policies[1].default = false;
    }
    let error = config.validate().unwrap_err();
    assert!(error.to_string().contains("default rule must be last"));

    {
        let policies = &mut config
            .outputs
            .get_mut("custom-output")
            .unwrap()
            .channel_policies;
        policies[0].default = false;
        policies[1].default = true;
        policies[1].prefix = Some("catch-all:selector".to_owned());
    }
    let error = config.validate().unwrap_err();
    assert!(error.to_string().contains("default rule must not specify"));
    config
        .outputs
        .get_mut("custom-output")
        .unwrap()
        .channel_policies[1]
        .prefix = None;
    config
        .outputs
        .get_mut("custom-output")
        .unwrap()
        .channel_policies[0]
        .default = true;
    let error = config.validate().unwrap_err();
    assert!(error.to_string().contains("exactly one default rule"));
    config
        .outputs
        .get_mut("custom-output")
        .unwrap()
        .channel_policies[0]
        .default = false;

    let policy = &mut config
        .outputs
        .get_mut("custom-output")
        .unwrap()
        .channel_policies[0];
    policy.prefix = Some("orders:".to_owned());
    let error = config.validate().unwrap_err();
    assert!(error.to_string().contains("exactly one"));

    let policy = &mut config
        .outputs
        .get_mut("custom-output")
        .unwrap()
        .channel_policies[0];
    policy.prefix = None;
    policy.glob = Some("  ".to_owned());
    let error = config.validate().unwrap_err();
    assert!(error.to_string().contains("selector must not be empty"));

    let policy = &mut config
        .outputs
        .get_mut("custom-output")
        .unwrap()
        .channel_policies[0];
    policy.glob = Some("orders:*".to_owned());
    policy.profile = Some("grouped".to_owned());
    policy.glob = None;
    let error = config.validate().unwrap_err();
    assert!(error.to_string().contains("exactly one"));

    let policy = &mut config
        .outputs
        .get_mut("custom-output")
        .unwrap()
        .channel_policies[0];
    policy.glob = Some("orders:*".to_owned());
    policy.profile = None;
    let error = config.validate().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("profile or at least one inline override")
    );

    let policy = &mut config
        .outputs
        .get_mut("custom-output")
        .unwrap()
        .channel_policies[0];
    policy.profile = Some("missing".to_owned());
    let error = config.validate().unwrap_err();
    assert!(error.to_string().contains("unknown profile"));

    let policy = &mut config
        .outputs
        .get_mut("custom-output")
        .unwrap()
        .channel_policies[0];
    policy.profile = Some("grouped".to_owned());
    policy.conflation_interval_ms = Some(0);
    let error = config.validate().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("either profile or inline overrides")
    );

    let profile = config
        .outputs
        .get_mut("custom-output")
        .unwrap()
        .profiles
        .get_mut("grouped")
        .unwrap();
    profile.deduplication_ttl_ms = Some(0);
    let error = config.validate().unwrap_err();
    assert!(error.to_string().contains("both deduplication.ttl_ms"));

    let profile = config
        .outputs
        .get_mut("custom-output")
        .unwrap()
        .profiles
        .get_mut("grouped")
        .unwrap();
    profile.deduplication_ttl_ms = None;
    profile.deduplication_group = Some("unknown".to_owned());
    let error = config.validate().unwrap_err();
    assert!(error.to_string().contains("unknown deduplication group"));

    let profile = config
        .outputs
        .get_mut("custom-output")
        .unwrap()
        .profiles
        .get_mut("grouped")
        .unwrap();
    profile.deduplication_group = Some("workers".to_owned());
    {
        let policy = &mut config
            .outputs
            .get_mut("custom-output")
            .unwrap()
            .channel_policies[0];
        policy.profile = None;
        policy.conflation_interval_ms = None;
        policy.deduplication_ttl_ms = Some(0);
        policy.deduplication_group = Some("workers".to_owned());
    }
    let error = config.validate().unwrap_err();
    assert!(error.to_string().contains("both deduplication.ttl_ms"));

    {
        let policy = &mut config
            .outputs
            .get_mut("custom-output")
            .unwrap()
            .channel_policies[0];
        policy.deduplication_ttl_ms = None;
        policy.deduplication_group = Some("unknown".to_owned());
    }
    let error = config.validate().unwrap_err();
    assert!(error.to_string().contains("unknown deduplication group"));

    config
        .outputs
        .get_mut("custom-output")
        .unwrap()
        .channel_policies[0]
        .deduplication_group = Some("workers".to_owned());
    config.validate().unwrap();

    let output = config.outputs.get_mut("custom-output").unwrap();
    output.profiles.insert(
        "  ".to_owned(),
        ChannelProfile {
            conflation_interval_ms: None,
            deduplication_ttl_ms: None,
            deduplication_group: None,
        },
    );
    let error = config.validate().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("profiles names must not be empty")
    );

    let output = config.outputs.get_mut("custom-output").unwrap();
    output.profiles.remove("  ");
    output.deduplication_groups.insert(
        "  ".to_owned(),
        DeduplicationGroup {
            ttl_ms: 0,
            round_ms: 0,
            restart_on_change: true,
            max_members: default_group_max_members(),
            max_cache_bytes: default_group_max_cache_bytes(),
        },
    );
    let error = config.validate().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("deduplication_groups names must not be empty")
    );
}
