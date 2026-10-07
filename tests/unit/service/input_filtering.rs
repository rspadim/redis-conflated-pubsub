use crate::config::DEFAULT_CHANNEL_CACHE_MAX_ENTRIES;

use super::*;

#[test]
fn matched_subscription_maps_to_the_output_channel() {
    let subscriptions = vec![Subscription::Psubscribe {
        pattern: "sensor:*".to_owned(),
        output_prefix: "replica:".to_owned(),
        output_suffix: ":copy".to_owned(),
    }];
    let subscription =
        matching_subscription(&subscriptions, "sensor:alpha", Some("sensor:*")).unwrap();
    let (prefix, suffix) = subscription.output_mapping();

    assert_eq!(
        map_output_channel("archive:", prefix, "sensor:alpha", suffix, ":dest"),
        "archive:replica:sensor:alpha:copy:dest"
    );
    assert!(matching_subscription(&subscriptions, "sensor:alpha", Some("sensor:other")).is_none());
}

#[test]
fn overlapping_patterns_use_the_pattern_reported_by_redis() {
    let subscriptions = vec![
        Subscription::Psubscribe {
            pattern: "sensor:*".to_owned(),
            output_prefix: "broad:".to_owned(),
            output_suffix: String::new(),
        },
        Subscription::Psubscribe {
            pattern: "sensor:a*".to_owned(),
            output_prefix: "specific:".to_owned(),
            output_suffix: String::new(),
        },
    ];
    let subscription =
        matching_subscription(&subscriptions, "sensor:alpha", Some("sensor:a*")).unwrap();
    let (prefix, suffix) = subscription.output_mapping();

    assert_eq!(
        map_output_channel("", prefix, "sensor:alpha", suffix, ""),
        "specific:sensor:alpha"
    );
}

#[test]
fn different_outputs_compose_their_own_namespace_with_the_subscription_mapping() {
    let subscriptions = vec![Subscription::Subscribe {
        channel: "events".to_owned(),
        output_prefix: "input:".to_owned(),
        output_suffix: ":source".to_owned(),
    }];
    let subscription = matching_subscription(&subscriptions, "events", None).unwrap();
    let (subscription_prefix, subscription_suffix) = subscription.output_mapping();

    assert_eq!(
        map_output_channel(
            "first:",
            subscription_prefix,
            "events",
            subscription_suffix,
            ":a"
        ),
        "first:input:events:source:a"
    );
    assert_eq!(
        map_output_channel(
            "second:",
            subscription_prefix,
            "events",
            subscription_suffix,
            ":b"
        ),
        "second:input:events:source:b"
    );
}

#[test]
fn each_output_conflates_by_mapped_channel_and_preserves_binary_payloads() {
    let metrics = Metrics::new();
    let output_metrics = metrics.register_output("output1");
    let mut pending = HashMap::new();
    let mut passthrough = VecDeque::new();

    for (channel, payload) in [
        ("replica:sensor:a", vec![0, 255]),
        ("replica:sensor:a", b"latest".to_vec()),
        ("replica:sensor:b", b"other".to_vec()),
    ] {
        metrics.record_output_input(&output_metrics, payload.len());
        enqueue_for_test(
            25,
            InboundMessage {
                output_channel: channel.to_owned(),
                payload,
            },
            &mut pending,
            &mut passthrough,
            &metrics,
            &output_metrics,
        );
    }

    assert_eq!(pending.len(), 2);
    assert_eq!(pending["replica:sensor:a"].payload, b"latest");
    assert_eq!(pending["replica:sensor:b"].payload, b"other");
    assert_eq!(metrics.conflated_messages_total.load(Ordering::Relaxed), 1);
    assert_eq!(
        metrics
            .conflated_payload_bytes_total
            .load(Ordering::Relaxed),
        2
    );
    let snapshot = metrics.snapshot();
    let output_snapshot = &snapshot.outputs["output1"];
    assert_eq!(output_snapshot.input_messages_total, 3);
    assert_eq!(output_snapshot.input_payload_bytes_total, 13);
    assert_eq!(output_snapshot.conflated_payload_bytes_total, 2);
    assert_eq!(output_snapshot.pending_messages, 2);
    assert_eq!(output_snapshot.pending_payload_bytes, 11);
    assert_eq!(metrics.pending_keys.load(Ordering::Relaxed), 2);
}

#[test]
fn fan_out_counts_inputs_per_output_without_multiplying_global_input() {
    let metrics = Metrics::new();
    let first_metrics = metrics.register_output("first");
    let second_metrics = metrics.register_output("second");
    let (first_sender, mut first_receiver) = mpsc::unbounded_channel();
    let (second_sender, mut second_receiver) = mpsc::unbounded_channel();
    let unavailable_metrics = metrics.register_output("unavailable");
    let (unavailable_sender, unavailable_receiver) = mpsc::unbounded_channel();
    drop(unavailable_receiver);
    let senders = [
        OutputSender {
            name: "first".to_owned(),
            channel_prefix: "first:".to_owned(),
            channel_suffix: String::new(),
            channel_filter: ChannelFilterSet::default(),
            sender: first_sender,
            output_metrics: Arc::clone(&first_metrics),
        },
        OutputSender {
            name: "second".to_owned(),
            channel_prefix: "second:".to_owned(),
            channel_suffix: String::new(),
            channel_filter: ChannelFilterSet::default(),
            sender: second_sender,
            output_metrics: Arc::clone(&second_metrics),
        },
        OutputSender {
            name: "unavailable".to_owned(),
            channel_prefix: "unavailable:".to_owned(),
            channel_suffix: String::new(),
            channel_filter: ChannelFilterSet::default(),
            sender: unavailable_sender,
            output_metrics: Arc::clone(&unavailable_metrics),
        },
    ];
    metrics.record_input(3);

    let mut senders = senders;
    fan_out(&mut senders, "", "events", "", b"abc", &metrics);

    assert_eq!(metrics.input_messages_total.load(Ordering::Relaxed), 1);
    assert_eq!(metrics.input_payload_bytes_total.load(Ordering::Relaxed), 3);
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.outputs["first"].input_messages_total, 1);
    assert_eq!(snapshot.outputs["second"].input_messages_total, 1);
    assert_eq!(snapshot.outputs["first"].input_payload_bytes_total, 3);
    assert_eq!(snapshot.outputs["second"].input_payload_bytes_total, 3);
    assert_eq!(snapshot.outputs["unavailable"].input_messages_total, 0);
    assert_eq!(snapshot.outputs["unavailable"].pending_messages, 0);
    assert_eq!(snapshot.outputs["unavailable"].pending_payload_bytes, 0);
    assert_eq!(first_receiver.try_recv().unwrap().payload, b"abc");
    assert_eq!(second_receiver.try_recv().unwrap().payload, b"abc");
}

#[test]
fn echo_filter_uses_subscription_prefix_or_suffix_namespaces() {
    let filter = OutputChannelFilter {
        prefix: "replica:".to_owned(),
        suffix: ":copy".to_owned(),
    };

    assert!(filter.matches("replica:sensor:a"));
    assert!(filter.matches("sensor:a:copy"));
    assert!(!filter.matches("sensor:a"));
}

#[test]
fn sentinel_filter_matches_hello_and_known_notification_channels_only() {
    for channel in [
        "__sentinel__:hello",
        "+sdown",
        "-odown",
        "+switch-master",
        "+failover-state-select-slave",
        "+sentinel-address-switch",
    ] {
        assert!(is_sentinel_pubsub_channel(channel), "{channel}");
    }

    for channel in ["metrics:cpu", "+custom-event", "__sentinel__:custom"] {
        assert!(!is_sentinel_pubsub_channel(channel), "{channel}");
    }
}

#[test]
fn echo_filters_use_the_combined_namespace_for_each_same_server_output() {
    let config: AppConfig = serde_json::from_str(
            r#"{
                "input":{"redis":{"host":"localhost"},"subscriptions":[{"type":"psubscribe","pattern":"sensor:*","output_prefix":"sub:","output_suffix":":source"}]},
                "outputs":{
                    "one":{"redis":{"host":"localhost"},"channel_prefix":"out:","channel_suffix":":dest","conflation":{"interval_ms":10}},
                    "two":{"redis":{"host":"other.local"},"channel_prefix":"archive:","channel_suffix":":copy","conflation":{"interval_ms":10}}
                },
                "instance_lock":{"path":"lock"}
            }"#,
        )
        .unwrap();
    config.validate().unwrap();

    let filters = output_echo_filters(&config.input, &config.outputs);

    assert_eq!(filters.len(), 1);
    assert!(filters[0].matches("out:sub:sensor:a:source:dest"));
    assert!(!filters[0].matches("archive:sub:sensor:a:source:copy"));
}

#[test]
fn channel_filters_are_ordered_cached_and_default_to_accept() {
    let mut filter = ChannelFilterSet::compile(
        &[
            ChannelFilterRule {
                glob: Some("events:*".to_owned()),
                regex: None,
                action: FilterAction::Deny,
                ..Default::default()
            },
            ChannelFilterRule {
                glob: None,
                regex: Some("^events:public$".to_owned()),
                action: FilterAction::Accept,
                ..Default::default()
            },
        ],
        FilterAction::Accept,
    )
    .unwrap();

    assert!(filter.allows("events:public"));
    assert!(!filter.allows("events:public:extra"));
    assert!(filter.allows("other:channel"));
    assert_eq!(filter.cached_channels(), 3);
    assert!(filter.allows("events:public"));
    assert!(!filter.allows("events:public:extra"));
    assert!(filter.allows("other:channel"));
    assert_eq!(filter.cached_channels(), 3);

    let mut deny_all = ChannelFilterSet::compile(&[], FilterAction::Deny).unwrap();
    assert!(!deny_all.allows("anything"));

    let mut regex_filter = ChannelFilterSet::compile(
        &[ChannelFilterRule {
            glob: None,
            regex: Some("^events:public$".to_owned()),
            action: FilterAction::Deny,
            ..Default::default()
        }],
        FilterAction::Accept,
    )
    .unwrap();
    assert!(!regex_filter.allows("events:public"));
    assert!(regex_filter.allows("events:public:extra"));

    let metrics = Metrics::new();
    let mut inspectable = ChannelFilterSet::compile_with_inspector(
        &[ChannelFilterRule {
            glob: Some("private:*".to_owned()),
            regex: None,
            action: FilterAction::Deny,
            ..Default::default()
        }],
        FilterAction::Accept,
        DEFAULT_CHANNEL_CACHE_MAX_ENTRIES,
        Some((&metrics, "input.filters".to_owned())),
    )
    .unwrap();
    assert!(!inspectable.allows("private:events"));
    assert!(!inspectable.allows("private:events"));
    let snapshot = metrics.channel_caches_snapshot();
    assert_eq!(
        snapshot["caches"]["input.filters"]["entries_most_recent_first"][0]["value"],
        "deny"
    );
    assert_eq!(snapshot["caches"]["input.filters"]["hits"], 1);
    assert_eq!(snapshot["caches"]["input.filters"]["misses"], 1);
}

#[test]
fn input_filter_checks_source_channel_before_subscription_mapping() {
    let mut filter = ChannelFilterSet::compile(
        &[ChannelFilterRule {
            glob: Some("input:*".to_owned()),
            regex: None,
            action: FilterAction::Deny,
            ..Default::default()
        }],
        FilterAction::Accept,
    )
    .unwrap();

    assert!(filter.allows("events:42"));
    assert!(!filter.allows("input:events:42"));
}

#[test]
fn raw_single_and_string_selectors_compare_the_channel_literally() {
    for selector in ["raw", "single", "string"] {
        let mut rule_json = serde_json::Map::new();
        rule_json.insert(selector.to_owned(), serde_json::json!("events:*"));
        rule_json.insert("action".to_owned(), serde_json::json!("deny"));
        let rule: ChannelFilterRule =
            serde_json::from_value(serde_json::Value::Object(rule_json)).unwrap();
        let mut filter = ChannelFilterSet::compile(&[rule], FilterAction::Accept).unwrap();

        assert!(!filter.allows("events:*"), "{selector} must match equality");
        assert!(
            filter.allows("events:alpha"),
            "{selector} must not interpret '*' as a wildcard"
        );
    }
}

#[test]
fn filter_decision_cache_stays_bounded_and_preserves_recent_channels() {
    let capacity = DEFAULT_CHANNEL_CACHE_MAX_ENTRIES;
    let mut filter = ChannelFilterSet::compile(
        &[ChannelFilterRule {
            glob: Some("events:*".to_owned()),
            action: FilterAction::Deny,
            ..Default::default()
        }],
        FilterAction::Accept,
    )
    .unwrap();

    for index in 0..capacity {
        let channel = format!("events:{index}");
        assert!(!filter.allows(&channel));
    }
    assert!(!filter.allows("events:0"));
    assert!(!filter.allows("events:new"));

    let snapshot = filter.cache_snapshot();
    let channels = snapshot["entries_most_recent_first"]
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| entry["channel"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(snapshot["capacity"], capacity);
    assert_eq!(snapshot["entry_count"], capacity);
    assert_eq!(snapshot["evictions"], 1);
    assert!(channels.contains(&"events:0"));
    assert!(!channels.contains(&"events:1"));
}

#[test]
fn zero_capacity_disables_decision_storage_but_keeps_filtering() {
    let mut filter = ChannelFilterSet::compile_with_inspector(
        &[ChannelFilterRule {
            raw: Some("events:private".to_owned()),
            action: FilterAction::Deny,
            ..Default::default()
        }],
        FilterAction::Accept,
        0,
        None,
    )
    .unwrap();

    assert!(!filter.allows("events:private"));
    assert!(!filter.allows("events:private"));
    assert_eq!(filter.cached_channels(), 0);
}

#[test]
fn output_filter_sees_input_mapping_but_not_output_namespace() {
    let metrics = Metrics::new();
    let filtered_metrics = metrics.register_output("filtered");
    let accepted_metrics = metrics.register_output("accepted");
    let (filtered_sender, mut filtered_receiver) = mpsc::unbounded_channel();
    let (accepted_sender, mut accepted_receiver) = mpsc::unbounded_channel();
    let mut senders = [
        OutputSender {
            name: "filtered".to_owned(),
            channel_prefix: "out:filtered:".to_owned(),
            channel_suffix: ":dest".to_owned(),
            channel_filter: ChannelFilterSet::compile(
                &[ChannelFilterRule {
                    glob: Some("input:events:*:source".to_owned()),
                    regex: None,
                    action: FilterAction::Deny,
                    ..Default::default()
                }],
                FilterAction::Accept,
            )
            .unwrap(),
            sender: filtered_sender,
            output_metrics: Arc::clone(&filtered_metrics),
        },
        OutputSender {
            name: "accepted".to_owned(),
            channel_prefix: "out:accepted:".to_owned(),
            channel_suffix: ":dest".to_owned(),
            channel_filter: ChannelFilterSet::compile(
                &[ChannelFilterRule {
                    glob: Some("out:accepted:input:events:*:source".to_owned()),
                    regex: None,
                    action: FilterAction::Deny,
                    ..Default::default()
                }],
                FilterAction::Accept,
            )
            .unwrap(),
            sender: accepted_sender,
            output_metrics: Arc::clone(&accepted_metrics),
        },
    ];

    fan_out(
        &mut senders,
        "input:",
        "events:42",
        ":source",
        b"payload",
        &metrics,
    );

    assert!(filtered_receiver.try_recv().is_err());
    assert_eq!(
        accepted_receiver.try_recv().unwrap().output_channel,
        "out:accepted:input:events:42:source:dest"
    );
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.outputs["filtered"].input_messages_total, 0);
    assert_eq!(snapshot.outputs["accepted"].input_messages_total, 1);
}
