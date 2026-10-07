use super::*;

#[test]
fn input_exclusion_options_default_safely_and_can_be_configured() {
    let mut config: AppConfig = serde_json::from_str(
            r#"{
                "input":{"redis":{"host":"input.example.net"},"subscriptions":[{"type":"psubscribe","pattern":"*"}]},
                "outputs":{"output1":{"redis":{"host":"output.example.net"},"conflation":{"interval_ms":10}}},
                "instance_lock":{"path":"lock"}
            }"#,
        )
        .unwrap();

    assert!(config.input.exclude_output_echoes);
    assert!(config.input.exclude_sentinel_pubsub);
    config.input.exclude_output_echoes = false;
    config.input.exclude_sentinel_pubsub = false;
    config.validate().unwrap();
}
#[test]
fn disabling_output_echo_exclusion_allows_unmapped_same_server_output() {
    let mut config: AppConfig = serde_json::from_str(
            r#"{
                "input":{"redis":{"host":"localhost"},"subscriptions":[{"type":"psubscribe","pattern":"*"}]},
                "outputs":{"output1":{"redis":{"host":"localhost"},"conflation":{"interval_ms":0}}},
                "instance_lock":{"path":"lock"}
            }"#,
        )
        .unwrap();

    assert!(config.validate().is_err());
    config.input.exclude_output_echoes = false;
    config.validate().unwrap();
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
