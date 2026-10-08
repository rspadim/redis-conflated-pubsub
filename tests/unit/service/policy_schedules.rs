use crate::config::DEFAULT_CHANNEL_CACHE_MAX_ENTRIES;

use super::*;

#[test]
fn channel_policy_resolution_uses_first_match_and_output_fallbacks() {
    let config = output_config_with_policies(
        125,
        900,
        vec![
            channel_policy(Some("mapped:orders:*"), None, None, Some(200), Some(50)),
            channel_policy(None, Some("mapped:orders:"), None, Some(300), Some(75)),
            channel_policy(None, Some("mapped:direct:"), None, Some(0), None),
            channel_policy(None, None, Some(":disabled"), None, Some(0)),
        ],
    );
    let compiled = CompiledChannelPolicies::new(&config);

    for channel in [
        "mapped:orders:42",
        "mapped:direct:now",
        "mapped:history:disabled",
        "unmatched",
    ] {
        assert_eq!(
            compiled.resolve(&config, channel),
            resolve_channel_policy(&config, channel),
            "compiled policy resolution differed for {channel}"
        );
    }

    let mapped_channel = map_output_channel("mapped:", "", "orders:42", "", "");
    assert_eq!(
        resolve_channel_policy(&config, &mapped_channel),
        ResolvedChannelPolicy {
            interval_ms: 200,
            deduplication_ttl_ms: 50,
            deduplication_group: None,
        }
    );
    assert_eq!(
        resolve_channel_policy(&config, "mapped:direct:now"),
        ResolvedChannelPolicy {
            interval_ms: 0,
            deduplication_ttl_ms: 900,
            deduplication_group: None,
        }
    );
    assert_eq!(
        resolve_channel_policy(&config, "mapped:history:disabled"),
        ResolvedChannelPolicy {
            interval_ms: 125,
            deduplication_ttl_ms: 0,
            deduplication_group: None,
        }
    );
    assert_eq!(
        resolve_channel_policy(&config, "unmatched"),
        ResolvedChannelPolicy {
            interval_ms: 125,
            deduplication_ttl_ms: 900,
            deduplication_group: None,
        }
    );

    let direct_policy = resolve_channel_policy(&config, "mapped:direct:now");
    let metrics = Metrics::new();
    let output_metrics = metrics.register_output("direct-policy-output");
    let mut pending = PendingByInterval::new();
    let mut passthrough = VecDeque::new();
    let mut cache = DeduplicationCache::new(config.deduplication.ttl_ms);
    enqueue_for_test_with_policy_at(
        InboundMessage {
            output_channel: "mapped:direct:now".to_owned(),
            payload: b"direct".to_vec().into(),
        },
        &mut pending,
        &mut passthrough,
        &metrics,
        &output_metrics,
        &mut cache,
        time::Instant::now(),
        OutputMessagePolicy {
            interval_ms: direct_policy.interval_ms,
            deduplication_ttl_ms: Some(direct_policy.deduplication_ttl_ms),
            deduplication_group: direct_policy.deduplication_group,
            max_bytes_per_exec: usize::MAX,
            oversized_policy: OversizedMessagePolicy::Send,
        },
    );
    assert!(pending.is_empty());
    assert_eq!(passthrough.len(), 1);
}

#[test]
fn channel_policy_resolution_cache_reuses_per_channel_results_and_is_inspectable() {
    let config = output_config_with_policies(
        250,
        5000,
        vec![channel_policy(
            Some("mapped:orders:*"),
            None,
            None,
            Some(75),
            Some(800),
        )],
    );
    let metrics = Metrics::new();
    let compiled = CompiledChannelPolicies::new_with_cache(
        &config,
        DEFAULT_CHANNEL_CACHE_MAX_ENTRIES,
        Some((&metrics, "outputs.replica.channel_policies".to_owned())),
    );

    let first = compiled.resolve(&config, "mapped:orders:42");
    let second = compiled.resolve(&config, "mapped:orders:42");
    assert_eq!(first, second);
    assert_eq!(first.interval_ms, 75);
    assert_eq!(compiled.cached_channels(), 1);

    let snapshot = metrics.channel_caches_snapshot();
    assert_eq!(
        snapshot["caches"]["outputs.replica.channel_policies"]["entry_count"],
        1
    );
    assert_eq!(
        snapshot["caches"]["outputs.replica.channel_policies"]["entries_most_recent_first"][0]["channel"],
        "mapped:orders:42"
    );

    let uncached = CompiledChannelPolicies::new_with_cache(&config, 0, None);
    assert_eq!(uncached.resolve(&config, "mapped:orders:42"), first);
    assert_eq!(uncached.cached_channels(), 0);
}

#[test]
fn profiles_and_inline_policies_resolve_selectors_precedence_and_schedules() {
    let config: OutputConfig = serde_json::from_value(serde_json::json!({
        "redis": { "host": "localhost" },
        "conflation": { "interval_ms": 125 },
        "deduplication": { "ttl_ms": 900 },
        "deduplication_groups": {
            "shared": {
                "ttl_ms": 400,
                "round_ms": 0,
                "restart_on_change": false
            }
        },
        "profiles": {
            "reusable": {
                "conflation.interval_ms": 100,
                "deduplication.ttl_ms": 250
            },
            "grouped": {
                "conflation.interval_ms": 300,
                "deduplication.group": "shared"
            },
            "default-profile": {
                "conflation.interval_ms": 75
            }
        },
        "channel_policies": [
            { "default": false, "glob": "mapped:reuse:*", "profile": "reusable" },
            { "default": false, "prefix": "mapped:group:", "profile": "grouped" },
            { "default": false, "glob": "mapped:precedence:*", "profile": "reusable" },
            {
                "default": false,
                "suffix": ":precedence",
                "conflation.interval_ms": 0,
                "deduplication.ttl_ms": 0
            },
            {
                "default": false,
                "suffix": ":inline",
                "conflation.interval_ms": 50,
                "deduplication.ttl_ms": 0
            },
            {
                "default": false,
                "prefix": "mapped:inline-group:",
                "conflation.interval_ms": 250,
                "deduplication.group": "shared"
            },
            { "default": true, "profile": "default-profile" }
        ]
    }))
    .expect("profile output config should deserialize");
    let compiled = CompiledChannelPolicies::new(&config);

    for channel in [
        "mapped:reuse:a",
        "mapped:group:symbol",
        "mapped:precedence:tail:precedence",
        "mapped:other:inline",
        "mapped:inline-group:member",
        "unmatched",
    ] {
        assert_eq!(
            compiled.resolve(&config, channel),
            resolve_channel_policy(&config, channel),
            "compiled policy resolution differed for {channel}"
        );
    }

    for source_channel in ["reuse:a", "reuse:b"] {
        let mapped = map_output_channel("mapped:", "", source_channel, "", "");
        assert_eq!(
            resolve_channel_policy(&config, &mapped),
            ResolvedChannelPolicy {
                interval_ms: 100,
                deduplication_ttl_ms: 250,
                deduplication_group: None,
            }
        );
    }
    assert_eq!(
        resolve_channel_policy(&config, "mapped:group:symbol"),
        ResolvedChannelPolicy {
            interval_ms: 300,
            deduplication_ttl_ms: 900,
            deduplication_group: Some("shared".to_owned()),
        }
    );
    assert_eq!(
        resolve_channel_policy(&config, "mapped:precedence:tail:precedence"),
        ResolvedChannelPolicy {
            interval_ms: 100,
            deduplication_ttl_ms: 250,
            deduplication_group: None,
        }
    );
    assert_eq!(
        resolve_channel_policy(&config, "mapped:other:inline"),
        ResolvedChannelPolicy {
            interval_ms: 50,
            deduplication_ttl_ms: 0,
            deduplication_group: None,
        }
    );
    assert_eq!(
        resolve_channel_policy(&config, "mapped:inline-group:member"),
        ResolvedChannelPolicy {
            interval_ms: 250,
            deduplication_ttl_ms: 900,
            deduplication_group: Some("shared".to_owned()),
        }
    );
    assert_eq!(
        resolve_channel_policy(&config, "unmatched"),
        ResolvedChannelPolicy {
            interval_ms: 75,
            deduplication_ttl_ms: 900,
            deduplication_group: None,
        }
    );

    let schedules = flush_schedules(
        std::iter::once(config.conflation.interval_ms)
            .chain(
                config
                    .profiles
                    .values()
                    .filter_map(|profile| profile.conflation_interval_ms),
            )
            .chain(
                config
                    .channel_policies
                    .iter()
                    .filter_map(|policy| policy.conflation_interval_ms),
            ),
        time::Instant::now(),
    );
    assert_eq!(
        schedules
            .iter()
            .map(|schedule| schedule.interval_ms)
            .collect::<Vec<_>>(),
        [50, 75, 100, 125, 250, 300]
    );
    assert_eq!(
        deduplication_prune_interval(&config),
        Some(Duration::from_millis(250))
    );
}

#[test]
fn output_channel_glob_supports_wildcards_classes_and_escapes() {
    assert!(glob_matches("mapped:order:[a-c]?", "mapped:order:b7"));
    assert!(!glob_matches("mapped:order:[a-c]?", "mapped:order:d7"));
    assert!(glob_matches(r"literal:\*\?", "literal:*?"));
    assert!(glob_matches("mapped:*[^0-9]", "mapped:value-x"));
    assert!(!glob_matches("mapped:*[^0-9]", "mapped:value-7"));

    let config: OutputConfig = serde_json::from_value(serde_json::json!({
        "redis": {"host": "localhost"},
        "conflation": {"interval_ms": 100},
        "channel_policies": [
            {"glob": "mapped:order:[a-c]?", "conflation.interval_ms": 10},
            {"glob": r"literal:\*\?", "conflation.interval_ms": 20},
            {"prefix": "mapped:prefix:", "conflation.interval_ms": 30},
            {"suffix": ":suffix", "conflation.interval_ms": 40},
            {"default": true, "conflation.interval_ms": 50}
        ]
    }))
    .expect("compiled glob config should deserialize");
    let compiled = CompiledChannelPolicies::new(&config);
    for channel in [
        "mapped:order:b7",
        "mapped:order:d7",
        "literal:*?",
        "mapped:prefix:item",
        "other:suffix",
        "unmatched",
    ] {
        assert_eq!(
            compiled.resolve(&config, channel),
            resolve_channel_policy(&config, channel),
            "compiled selector differed for {channel}"
        );
    }
}

#[tokio::test]
async fn distinct_interval_groups_keep_cadence_and_flush_only_the_due_group() {
    let started = time::Instant::now();
    let mut schedules = flush_schedules([200, 250, 300, 200, 0], started);
    assert_eq!(
        schedules
            .iter()
            .map(|schedule| schedule.interval_ms)
            .collect::<Vec<_>>(),
        [200, 250, 300]
    );
    let metrics = Metrics::new();
    let output_metrics = metrics.register_output("interval-groups-output");
    let mut pending = HashMap::new();
    let mut passthrough = VecDeque::new();
    for (channel, interval_ms) in [
        ("events:200", 200),
        ("events:250", 250),
        ("events:300", 300),
    ] {
        let mut message = pending_message(channel, channel.as_bytes().to_vec());
        message.conflation_interval_ms = interval_ms;
        pending
            .entry(interval_ms)
            .or_insert_with(HashMap::new)
            .insert(channel.to_owned(), message);
    }
    let mut cache = DeduplicationCache::new(0);
    let mut publisher = RecordingPublisher::default();

    for (elapsed_ms, due_interval_ms) in [(200, 200), (250, 250), (300, 300)] {
        let due =
            advance_due_schedules(&mut schedules, started + Duration::from_millis(elapsed_ms));
        assert_eq!(due, [due_interval_ms]);
        let mut failure_log = OutputFailureLog::default();
        let mut context = publish_context_for_test(
            "interval-groups-output",
            &mut failure_log,
            &mut cache,
            &metrics,
            &output_metrics,
        );
        publish_conflated_interval_pending(
            &mut publisher,
            &mut pending,
            &mut passthrough,
            due_interval_ms,
            256,
            usize::MAX,
            &mut context,
        )
        .await;
        assert!(!pending.contains_key(&due_interval_ms));
        assert_eq!(
            pending_message_count(&pending),
            (300 - due_interval_ms) as usize / 50
        );
    }

    assert!(pending.is_empty());
    assert_eq!(
        publisher
            .attempts
            .iter()
            .flat_map(|(_, batch)| batch.iter().map(|message| message.conflation_interval_ms))
            .collect::<Vec<_>>(),
        [200, 250, 300]
    );
    assert_eq!(
        advance_due_schedules(&mut schedules, started + Duration::from_millis(400)),
        [200]
    );
    assert_eq!(
        advance_due_schedules(&mut schedules, started + Duration::from_millis(500)),
        [250]
    );
    assert_eq!(
        advance_due_schedules(&mut schedules, started + Duration::from_millis(600)),
        [200, 300]
    );
}

#[tokio::test]
async fn large_conflated_bucket_drains_one_chunk_per_turn_and_stays_due_until_empty() {
    let metrics = Metrics::new();
    let output_metrics = metrics.register_output("fairness-output");
    let mut pending = PendingByInterval::new();
    let mut passthrough = VecDeque::new();
    let mut cache = DeduplicationCache::new(0);
    for suffix in ["a", "b", "c", "d", "e"] {
        enqueue_for_test_with_cache(
            50,
            InboundMessage {
                output_channel: format!("events:{suffix}"),
                payload: suffix.as_bytes().to_vec().into(),
            },
            &mut pending,
            &mut passthrough,
            &metrics,
            &output_metrics,
            &mut cache,
        );
    }

    let mut publisher = RecordingPublisher::default();
    let mut failure_log = OutputFailureLog::default();
    let mut context = publish_context_for_test(
        "fairness-output",
        &mut failure_log,
        &mut cache,
        &metrics,
        &output_metrics,
    );

    // Mirrors the worker caller: the interval is re-added to `due_intervals`
    // while the bucket reports remaining work, so it stays due until drained.
    let mut due_intervals = vec![50];
    let mut turns = 0;
    while let Some(interval_ms) = due_intervals.pop() {
        turns += 1;
        let has_remaining = publish_conflated_interval_pending(
            &mut publisher,
            &mut pending,
            &mut passthrough,
            interval_ms,
            2,
            usize::MAX,
            &mut context,
        )
        .await;
        assert_eq!(
            publisher.successful_batches.len(),
            turns,
            "each turn must publish at most one chunk"
        );
        if has_remaining {
            assert!(pending.contains_key(&interval_ms));
            due_intervals.push(interval_ms);
        } else {
            assert!(!pending.contains_key(&interval_ms));
        }
    }

    assert_eq!(turns, 3);
    assert!(due_intervals.is_empty());
    assert!(pending.is_empty());
    assert_eq!(
        publisher
            .successful_batches
            .iter()
            .map(|batch| batch
                .iter()
                .map(|message| message.output_channel.as_str())
                .collect::<Vec<_>>())
            .collect::<Vec<_>>(),
        vec![
            vec!["events:a", "events:b"],
            vec!["events:c", "events:d"],
            vec!["events:e"],
        ]
    );
}
