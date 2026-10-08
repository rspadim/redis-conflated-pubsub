use super::*;

#[test]
fn channel_ttls_expire_independently() {
    let now = time::Instant::now();
    let short = pending_message("events:short", b"same".to_vec());
    let mut short = short;
    short.deduplication_ttl_ms = Some(10);
    let mut long = pending_message("events:long", b"same".to_vec());
    long.deduplication_ttl_ms = Some(100);
    let mut cache = DeduplicationCache::new(5000);
    cache.remember(&[short.clone(), long.clone()], now);

    assert!(!cache.should_suppress(&short, now + Duration::from_millis(10)));
    assert!(cache.should_suppress(&long, now + Duration::from_millis(10)));
    assert!(!cache.should_suppress(&long, now + Duration::from_millis(100)));
}

#[test]
fn fixed_group_deadline_expires_all_sibling_members_together() {
    let now = time::Instant::now();
    let groups = group_settings(600, false);
    let mut cache = DeduplicationCache::with_groups(5000, &groups);
    let first_channel = pending_group_message("events:first", b"first", "shared");
    let sibling_channel = pending_group_message("events:sibling", b"sibling", "shared");
    let changed_first_channel = pending_group_message("events:first", b"changed", "shared");

    cache.remember(std::slice::from_ref(&first_channel), now);
    cache.remember(
        std::slice::from_ref(&sibling_channel),
        now + Duration::from_millis(100),
    );
    cache.remember(
        std::slice::from_ref(&changed_first_channel),
        now + Duration::from_millis(250),
    );

    assert!(cache.should_suppress(&changed_first_channel, now + Duration::from_millis(599)));
    assert!(cache.should_suppress(&sibling_channel, now + Duration::from_millis(599)));
    assert!(!cache.should_suppress(&changed_first_channel, now + Duration::from_millis(600)));
    assert!(!cache.should_suppress(&sibling_channel, now + Duration::from_millis(600)));
    assert!(!cache.groups.contains_key("shared"));
}

#[test]
fn new_group_member_renews_shared_deadline_when_enabled() {
    let now = time::Instant::now();
    let groups = group_settings(600, true);
    let mut cache = DeduplicationCache::with_groups(5000, &groups);
    let first_channel = pending_group_message("events:first", b"first", "shared");
    let sibling_channel = pending_group_message("events:sibling", b"sibling", "shared");

    cache.remember(std::slice::from_ref(&first_channel), now);
    cache.remember(
        std::slice::from_ref(&sibling_channel),
        now + Duration::from_millis(100),
    );

    assert_eq!(
        cache.groups["shared"].expires_at,
        now + Duration::from_millis(700)
    );
    assert!(cache.should_suppress(&sibling_channel, now + Duration::from_millis(600)));
    assert!(!cache.should_suppress(&sibling_channel, now + Duration::from_millis(700)));
    assert!(!cache.groups.contains_key("shared"));
}

#[test]
fn group_cache_evicts_least_recently_used_members_at_capacity() {
    let now = time::Instant::now();
    let groups = group_settings_with_limits(600, true, 0, 2, 4);
    let mut cache = DeduplicationCache::with_groups(5000, &groups);
    let first = pending_group_message("a", b"1", "shared");
    let second = pending_group_message("b", b"2", "shared");
    let third = pending_group_message("c", b"3", "shared");

    cache.remember_at(std::slice::from_ref(&first), now, 10_000);
    cache.remember_at(
        std::slice::from_ref(&second),
        now + Duration::from_millis(1),
        10_001,
    );
    assert!(cache.should_suppress(&first, now + Duration::from_millis(2)));
    cache.remember_at(
        std::slice::from_ref(&third),
        now + Duration::from_millis(3),
        10_003,
    );

    let group = &cache.groups["shared"];
    assert_eq!(group.entries.len(), 2);
    assert_eq!(group.cache_bytes, 4);
    assert!(cache.should_suppress(&first, now + Duration::from_millis(4)));
    assert!(!cache.should_suppress(&second, now + Duration::from_millis(4)));
    assert!(cache.should_suppress(&third, now + Duration::from_millis(4)));
}

#[test]
fn group_cache_does_not_retain_a_member_larger_than_its_byte_budget() {
    let now = time::Instant::now();
    let groups = group_settings_with_limits(600, false, 0, 4, 5);
    let mut cache = DeduplicationCache::with_groups(5000, &groups);
    let large = pending_group_message("large", b"payload", "shared");

    cache.remember_at(std::slice::from_ref(&large), now, 10_000);

    let group = &cache.groups["shared"];
    assert!(group.entries.is_empty());
    assert_eq!(group.cache_bytes, 0);
    assert!(!cache.should_suppress(&large, now + Duration::from_millis(1)));
}

#[test]
fn group_rounding_anchors_expiry_to_epoch_window() {
    let now = time::Instant::now();
    let groups = group_settings_with_round(300, false, 100);
    let mut cache = DeduplicationCache::with_groups(5000, &groups);
    let message = pending_group_message("events:rounded", b"payload", "shared");

    cache.remember_at(std::slice::from_ref(&message), now, 255);

    assert_eq!(
        cache.groups["shared"].expires_at,
        now + Duration::from_millis(245)
    );
    assert!(cache.should_suppress(&message, now + Duration::from_millis(244)));
    assert!(!cache.should_suppress(&message, now + Duration::from_millis(245)));
}

#[test]
fn zero_group_rounding_keeps_exact_ttl_deadline() {
    let now = time::Instant::now();
    let groups = group_settings(300, false);
    let mut cache = DeduplicationCache::with_groups(5000, &groups);
    let message = pending_group_message("events:exact", b"payload", "shared");

    cache.remember_at(std::slice::from_ref(&message), now, 255);

    assert_eq!(
        cache.groups["shared"].expires_at,
        now + Duration::from_millis(300)
    );
    assert!(cache.should_suppress(&message, now + Duration::from_millis(299)));
    assert!(!cache.should_suppress(&message, now + Duration::from_millis(300)));
}

#[test]
fn changed_existing_group_member_renews_shared_sibling_deadline() {
    let now = time::Instant::now();
    let groups = group_settings(600, true);
    let mut cache = DeduplicationCache::with_groups(5000, &groups);
    let first_channel = pending_group_message("events:first", b"first", "shared");
    let sibling_channel = pending_group_message("events:sibling", b"sibling", "shared");

    cache.remember(std::slice::from_ref(&first_channel), now);
    cache.remember(std::slice::from_ref(&sibling_channel), now);
    let changed_first_channel = pending_group_message("events:first", b"changed", "shared");
    assert!(!cache.should_suppress(&changed_first_channel, now + Duration::from_millis(300)));
    cache.remember(
        std::slice::from_ref(&changed_first_channel),
        now + Duration::from_millis(300),
    );

    assert!(cache.should_suppress(&sibling_channel, now + Duration::from_millis(700)));
    assert_eq!(
        cache.groups["shared"].expires_at,
        now + Duration::from_millis(900)
    );
    assert!(!cache.should_suppress(&sibling_channel, now + Duration::from_millis(900)));
    assert!(!cache.groups.contains_key("shared"));
}

#[test]
fn channel_deduplication_disable_does_not_disable_conflation() {
    let config = output_config_with_policies(
        0,
        5000,
        vec![channel_policy(
            None,
            Some("events:"),
            None,
            Some(100),
            Some(0),
        )],
    );
    let resolved = resolve_channel_policy(&config, "events:live");
    assert_eq!(resolved.interval_ms, 100);
    assert_eq!(resolved.deduplication_ttl_ms, 0);

    let metrics = Metrics::new();
    let output_metrics = metrics.register_output("channel-no-dedup-output");
    let mut pending = HashMap::new();
    let mut passthrough = VecDeque::new();
    let mut cache = DeduplicationCache::new(config.deduplication.ttl_ms);
    for payload in [b"first".to_vec(), b"latest".to_vec()] {
        enqueue_for_test_with_policy_at(
            InboundMessage {
                output_channel: "events:live".to_owned(),
                payload: payload.into(),
            },
            &mut pending,
            &mut passthrough,
            &metrics,
            &output_metrics,
            &mut cache,
            time::Instant::now(),
            OutputMessagePolicy {
                interval_ms: resolved.interval_ms,
                deduplication_ttl_ms: Some(resolved.deduplication_ttl_ms),
                deduplication_group: resolved.deduplication_group.clone(),
                max_bytes_per_exec: usize::MAX,
                oversized_policy: OversizedMessagePolicy::Send,
            },
        );
    }
    assert!(passthrough.is_empty());
    assert_eq!(pending_message_count(&pending), 1);
    assert_eq!(pending[&100]["events:live"].payload.as_ref(), b"latest");
    assert!(cache.entries.is_empty());
    assert_eq!(metrics.conflated_messages_total.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn nonpositive_group_ttl_disables_deduplication_but_keeps_conflation() {
    let metrics = Metrics::new();
    let output_metrics = metrics.register_output("disabled-group-output");
    let groups = group_settings(0, true);
    let mut cache = DeduplicationCache::with_groups(5000, &groups);
    let mut pending = HashMap::new();
    let mut passthrough = VecDeque::new();

    for payload in [b"first".to_vec(), b"latest".to_vec()] {
        metrics.record_output_input(&output_metrics, payload.len(), payload.len());
        enqueue_for_test_with_policy_at(
            InboundMessage {
                output_channel: "events:live".to_owned(),
                payload: payload.into(),
            },
            &mut pending,
            &mut passthrough,
            &metrics,
            &output_metrics,
            &mut cache,
            time::Instant::now(),
            OutputMessagePolicy {
                interval_ms: 100,
                deduplication_ttl_ms: Some(5000),
                deduplication_group: Some("shared".to_owned()),
                max_bytes_per_exec: usize::MAX,
                oversized_policy: OversizedMessagePolicy::Send,
            },
        );
    }

    assert_eq!(pending_message_count(&pending), 1);
    assert_eq!(pending[&100]["events:live"].payload.as_ref(), b"latest");
    assert_eq!(metrics.conflated_messages_total.load(Ordering::Relaxed), 1);

    let mut publisher = RecordingPublisher::default();
    flush_conflated_for_test(
        "disabled-group-output",
        &mut publisher,
        &mut pending,
        &mut cache,
        &metrics,
        &output_metrics,
    )
    .await;

    assert_eq!(publisher.successful_batches.len(), 1);
    assert_eq!(
        publisher.successful_batches[0][0].payload.as_ref(),
        b"latest"
    );
    assert!(cache.groups.is_empty());
    let output = &metrics.snapshot().outputs["disabled-group-output"];
    assert_eq!(output.deduplicated_messages_total, 0);
    assert_eq!(output.output_messages_total, 1);
}

#[test]
fn deduplication_compares_output_channel_and_payload_bytes_until_ttl_expiry() {
    let ttl = Duration::from_millis(5000);
    let started = time::Instant::now();
    let mut cache = DeduplicationCache::new(5000);
    let first = pending_message("events:a", vec![0, 255]);
    cache.remember(std::slice::from_ref(&first), started);

    assert!(cache.should_suppress(&first, started + Duration::from_millis(4999)));
    assert!(!cache.should_suppress(
        &pending_message("events:b", first.payload.clone()),
        started + Duration::from_millis(1),
    ));
    assert!(!cache.should_suppress(
        &pending_message(&first.output_channel, vec![0, 254]),
        started + Duration::from_millis(1),
    ));
    assert!(!cache.should_suppress(&first, started + ttl));
    assert!(cache.entries.is_empty());
}

#[test]
fn per_channel_cache_remembers_only_the_latest_published_payload() {
    let now = time::Instant::now();
    let mut cache = DeduplicationCache::new(5000);
    let a = pending_message("events", b"A".to_vec());
    let b = pending_message("events", b"B".to_vec());

    cache.remember(std::slice::from_ref(&a), now);
    assert!(!cache.should_suppress(&b, now + Duration::from_millis(1)));
    cache.remember(std::slice::from_ref(&b), now + Duration::from_millis(1));
    assert!(!cache.should_suppress(&a, now + Duration::from_millis(2)));
    cache.remember(std::slice::from_ref(&a), now + Duration::from_millis(2));

    assert!(cache.should_suppress(&a, now + Duration::from_millis(3)));
    assert!(!cache.should_suppress(&b, now + Duration::from_millis(3)));
}

#[test]
fn deduplication_isolated_per_output_and_zero_ttl_disables_it() {
    let now = time::Instant::now();
    let message = pending_message("events", b"same".to_vec());
    let mut first_output = DeduplicationCache::new(5000);
    let mut second_output = DeduplicationCache::new(5000);
    let mut disabled_output = DeduplicationCache::new(0);
    let mut negative_ttl_output = DeduplicationCache::new(-1);
    first_output.remember(std::slice::from_ref(&message), now);

    assert!(first_output.should_suppress(&message, now));
    assert!(!second_output.should_suppress(&message, now));
    assert!(!disabled_output.should_suppress(&message, now));
    assert!(!negative_ttl_output.should_suppress(&message, now));
    disabled_output.remember(std::slice::from_ref(&message), now);
    negative_ttl_output.remember(std::slice::from_ref(&message), now);
    assert!(disabled_output.entries.is_empty());
    assert!(negative_ttl_output.entries.is_empty());
}

#[tokio::test]
async fn conflated_value_returning_to_cached_value_is_suppressed_at_flush() {
    let metrics = Metrics::new();
    let output_metrics = metrics.register_output("dedup-output");
    let mut pending = HashMap::new();
    let mut passthrough = VecDeque::new();
    let mut cache = DeduplicationCache::new(5000);
    let last_published = pending_message("events", b"published".to_vec());
    cache.remember(std::slice::from_ref(&last_published), time::Instant::now());

    let changed = b"changed".to_vec();
    metrics.record_output_input(&output_metrics, changed.len(), changed.len());
    enqueue_for_test_with_cache(
        100,
        InboundMessage {
            output_channel: "events".to_owned(),
            payload: changed.into(),
        },
        &mut pending,
        &mut passthrough,
        &metrics,
        &output_metrics,
        &mut cache,
    );
    assert_eq!(pending[&100]["events"].raw_payload(), b"changed");

    let returned = b"published".to_vec();
    metrics.record_output_input(&output_metrics, returned.len(), returned.len());
    enqueue_for_test_with_cache(
        100,
        InboundMessage {
            output_channel: "events".to_owned(),
            payload: returned.into(),
        },
        &mut pending,
        &mut passthrough,
        &metrics,
        &output_metrics,
        &mut cache,
    );

    assert_eq!(pending[&100]["events"].raw_payload(), b"published");
    assert!(passthrough.is_empty());
    assert_eq!(
        metrics.deduplicated_messages_total.load(Ordering::Relaxed),
        0
    );

    let mut publisher = RecordingPublisher::default();
    let mut failure_log = OutputFailureLog::default();
    let mut publish_context = publish_context_for_test(
        "dedup-output",
        &mut failure_log,
        &mut cache,
        &metrics,
        &output_metrics,
    );
    publish_conflated_pending(
        &mut publisher,
        &mut pending,
        256,
        usize::MAX,
        &mut publish_context,
    )
    .await;

    assert!(pending.is_empty());
    assert!(publisher.attempts.is_empty());
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.deduplicated_messages_total, 1);
    assert_eq!(snapshot.deduplicated_payload_bytes_total, 9);
    assert_eq!(snapshot.conflated_messages_total, 1);
    assert_eq!(snapshot.conflated_payload_bytes_total, 7);
    assert_eq!(
        snapshot.outputs["dedup-output"].deduplicated_messages_total,
        1
    );
    assert_eq!(
        snapshot.outputs["dedup-output"].deduplicated_payload_bytes_total,
        9
    );
    assert_eq!(snapshot.outputs["dedup-output"].pending_messages, 0);
    assert_eq!(snapshot.outputs["dedup-output"].pending_payload_bytes, 0);
    assert_eq!(snapshot.outputs["dedup-output"].pending_keys, 0);
    assert_eq!(snapshot.pending_keys, 0);
}

#[tokio::test]
async fn conflated_deduplication_skips_removed_channels_across_exec_chunks() {
    let metrics = Metrics::new();
    let output_metrics = metrics.register_output("chunked-dedup-output");
    let mut pending = HashMap::new();
    let mut passthrough = VecDeque::new();
    let mut cache = DeduplicationCache::new(5000);
    cache.remember(
        &[pending_message("events:b", b"cached".to_vec())],
        time::Instant::now(),
    );

    for (channel, payload) in [
        ("events:a", b"first".to_vec()),
        ("events:b", b"cached".to_vec()),
        ("events:c", b"last".to_vec()),
    ] {
        metrics.record_output_input(&output_metrics, payload.len(), payload.len());
        enqueue_for_test_with_cache(
            100,
            InboundMessage {
                output_channel: channel.to_owned(),
                payload: payload.into(),
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
    let mut publish_context = publish_context_for_test(
        "chunked-dedup-output",
        &mut failure_log,
        &mut cache,
        &metrics,
        &output_metrics,
    );
    publish_conflated_pending(
        &mut publisher,
        &mut pending,
        1,
        usize::MAX,
        &mut publish_context,
    )
    .await;

    let published_channels = publisher
        .attempts
        .iter()
        .flat_map(|(_, batch)| batch.iter().map(|message| message.output_channel.as_str()))
        .collect::<Vec<_>>();
    assert_eq!(published_channels, ["events:a", "events:c"]);
    assert!(pending.is_empty());
    let output = &metrics.snapshot().outputs["chunked-dedup-output"];
    assert_eq!(output.deduplicated_messages_total, 1);
    assert_eq!(output.deduplicated_payload_bytes_total, 6);
    assert_eq!(output.pending_messages, 0);
    assert_eq!(output.pending_payload_bytes, 0);
    assert_eq!(output.pending_keys, 0);
}

#[tokio::test]
async fn nonpositive_ttl_keeps_normal_conflation_enabled() {
    for ttl_ms in [0, -1] {
        let metrics = Metrics::new();
        let output_metrics = metrics.register_output("disabled-dedup-output");
        let mut pending = HashMap::new();
        let mut cache = DeduplicationCache::new(ttl_ms);

        for payload in [b"first".to_vec(), b"latest".to_vec()] {
            metrics.record_output_input(&output_metrics, payload.len(), payload.len());
            enqueue_for_test_with_cache(
                100,
                InboundMessage {
                    output_channel: "events".to_owned(),
                    payload: payload.into(),
                },
                &mut pending,
                &mut VecDeque::new(),
                &metrics,
                &output_metrics,
                &mut cache,
            );
        }
        assert_eq!(pending[&100]["events"].payload.as_ref(), b"latest");

        let mut publisher = RecordingPublisher::default();
        flush_conflated_for_test(
            "disabled-dedup-output",
            &mut publisher,
            &mut pending,
            &mut cache,
            &metrics,
            &output_metrics,
        )
        .await;

        assert_eq!(publisher.successful_batches.len(), 1);
        assert_eq!(
            publisher.successful_batches[0][0].payload.as_ref(),
            b"latest"
        );
        let output = &metrics.snapshot().outputs["disabled-dedup-output"];
        assert_eq!(output.deduplicated_messages_total, 0);
        assert_eq!(output.conflated_messages_total, 1);
        assert_eq!(output.pending_messages, 0);
        assert_eq!(output.pending_payload_bytes, 0);
    }
}

#[tokio::test]
async fn direct_mode_publishes_changed_values_and_suppresses_repeats() {
    let metrics = Metrics::new();
    let output_metrics = metrics.register_output("direct-changes-output");
    let mut pending = HashMap::new();
    let mut passthrough = VecDeque::new();
    let mut cache = DeduplicationCache::new(5000);
    let mut publisher = RecordingPublisher::default();

    for payload in [b"A".to_vec(), b"B".to_vec(), b"B".to_vec()] {
        metrics.record_output_input(&output_metrics, payload.len(), payload.len());
        enqueue_for_test_with_cache(
            0,
            InboundMessage {
                output_channel: "events".to_owned(),
                payload: payload.into(),
            },
            &mut pending,
            &mut passthrough,
            &metrics,
            &output_metrics,
            &mut cache,
        );
        flush_passthrough_for_test(
            "direct-changes-output",
            &mut publisher,
            &mut pending,
            &mut passthrough,
            &mut cache,
            &metrics,
            &output_metrics,
        )
        .await;
    }

    assert_eq!(publisher.successful_batches.len(), 2);
    assert_eq!(publisher.successful_batches[0][0].payload.as_ref(), b"A");
    assert_eq!(publisher.successful_batches[1][0].payload.as_ref(), b"B");
    let output = &metrics.snapshot().outputs["direct-changes-output"];
    assert_eq!(output.deduplicated_messages_total, 1);
    assert_eq!(output.deduplicated_payload_bytes_total, 1);
    assert_eq!(output.pending_messages, 0);
    assert_eq!(output.pending_payload_bytes, 0);
}

#[tokio::test]
async fn truncate_deduplication_compares_the_raw_input_payload() {
    let metrics = Metrics::new();
    let output_metrics = metrics.register_output("raw-truncate-dedup-output");
    let mut pending = HashMap::new();
    let mut passthrough = VecDeque::new();
    let mut cache = DeduplicationCache::new(5000);
    let mut publisher = RecordingPublisher::default();
    let channel = "events";
    let max_bytes = publish_command_frame_bytes_for_lengths(channel.len(), 2);
    let policy = OutputMessagePolicy {
        interval_ms: 0,
        deduplication_ttl_ms: None,
        deduplication_group: None,
        max_bytes_per_exec: max_bytes,
        oversized_policy: OversizedMessagePolicy::Truncate,
    };

    for payload in [vec![0, 1, 2], vec![0, 1, 3]] {
        metrics.record_output_input(&output_metrics, payload.len(), payload.len());
        enqueue_for_test_with_policy_at(
            InboundMessage {
                output_channel: channel.to_owned(),
                payload: payload.into(),
            },
            &mut pending,
            &mut passthrough,
            &metrics,
            &output_metrics,
            &mut cache,
            time::Instant::now(),
            policy.clone(),
        );
        flush_passthrough_for_test(
            "raw-truncate-dedup-output",
            &mut publisher,
            &mut pending,
            &mut passthrough,
            &mut cache,
            &metrics,
            &output_metrics,
        )
        .await;
    }

    assert_eq!(publisher.successful_batches.len(), 2);
    assert_eq!(publisher.successful_batches[0][0].payload.as_ref(), [0, 1]);
    assert_eq!(publisher.successful_batches[1][0].payload.as_ref(), [0, 1]);
    assert_eq!(
        cache.entries["events"].raw_payload.as_ref(),
        [0, 1, 3],
        "the TTL cache must retain the untruncated input"
    );

    let repeated = vec![0, 1, 3];
    metrics.record_output_input(&output_metrics, repeated.len(), repeated.len());
    enqueue_for_test_with_policy_at(
        InboundMessage {
            output_channel: channel.to_owned(),
            payload: repeated.into(),
        },
        &mut pending,
        &mut passthrough,
        &metrics,
        &output_metrics,
        &mut cache,
        time::Instant::now(),
        policy,
    );

    assert!(passthrough.is_empty());
    let output = &metrics.snapshot().outputs["raw-truncate-dedup-output"];
    assert_eq!(output.deduplicated_messages_total, 1);
    assert_eq!(output.deduplicated_payload_bytes_total, 3);
    assert_eq!(output.pending_messages, 0);
    assert_eq!(output.pending_payload_bytes, 0);
}

#[test]
fn direct_queue_deduplicates_inside_ttl_and_accepts_same_value_after_expiry() {
    let metrics = Metrics::new();
    let output_metrics = metrics.register_output("direct-dedup-output");
    let mut pending = HashMap::new();
    let mut passthrough = VecDeque::new();
    let mut cache = DeduplicationCache::new(5000);
    let published = pending_message("events", vec![0, 255]);
    let now = time::Instant::now();
    cache.remember(std::slice::from_ref(&published), now);

    for (payload, enqueued_at) in [
        (published.payload.clone(), now + Duration::from_millis(1)),
        (published.payload.clone(), now + Duration::from_millis(2)),
    ] {
        metrics.record_output_input(&output_metrics, payload.len(), payload.len());
        enqueue_for_test_with_cache_at(
            0,
            InboundMessage {
                output_channel: published.output_channel.clone(),
                payload,
            },
            &mut pending,
            &mut passthrough,
            &metrics,
            &output_metrics,
            &mut cache,
            enqueued_at,
        );
    }
    assert!(passthrough.is_empty());

    metrics.record_output_input(
        &output_metrics,
        published.payload.len(),
        published.payload.len(),
    );
    enqueue_for_test_with_cache_at(
        0,
        InboundMessage {
            output_channel: published.output_channel,
            payload: published.payload,
        },
        &mut pending,
        &mut passthrough,
        &metrics,
        &output_metrics,
        &mut cache,
        now + Duration::from_millis(5000),
    );

    assert_eq!(passthrough.len(), 1);
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.deduplicated_messages_total, 2);
    assert_eq!(snapshot.deduplicated_payload_bytes_total, 4);
    assert_eq!(snapshot.outputs["direct-dedup-output"].pending_messages, 1);
    assert_eq!(
        snapshot.outputs["direct-dedup-output"].pending_payload_bytes,
        2
    );
    assert_eq!(snapshot.outputs["direct-dedup-output"].pending_keys, 1);
    assert_eq!(snapshot.pending_keys, 1);
}
