use super::*;

#[tokio::test]
async fn conflation_publishes_only_the_final_changed_value_in_a_window() {
    let metrics = Metrics::new();
    let output_metrics = metrics.register_output("conflate-final-output");
    let mut pending = HashMap::new();
    let mut cache = DeduplicationCache::new(5000);
    let last_published = pending_message("events", b"A".to_vec());
    cache.remember(std::slice::from_ref(&last_published), time::Instant::now());

    for payload in [b"B".to_vec(), b"C".to_vec()] {
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
    assert_eq!(pending[&100]["events"].raw_payload(), b"C");

    let mut publisher = RecordingPublisher::default();
    flush_conflated_for_test(
        "conflate-final-output",
        &mut publisher,
        &mut pending,
        &mut cache,
        &metrics,
        &output_metrics,
    )
    .await;

    assert_eq!(publisher.successful_batches.len(), 1);
    assert_eq!(publisher.successful_batches[0][0].payload.as_ref(), b"C");
    assert_eq!(publisher.attempts.len(), 1);
    let output = &metrics.snapshot().outputs["conflate-final-output"];
    assert_eq!(output.deduplicated_messages_total, 0);
    assert_eq!(output.conflated_messages_total, 1);
    assert_eq!(output.output_messages_total, 1);
    assert_eq!(output.pending_messages, 0);
    assert_eq!(output.pending_payload_bytes, 0);
}

#[test]
fn nonpositive_interval_keeps_every_incoming_message_in_order() {
    let metrics = Metrics::new();
    let output_metrics = metrics.register_output("arbitrary-name");
    let mut pending = HashMap::new();
    let mut passthrough = VecDeque::new();

    for payload in [b"first".to_vec(), b"second".to_vec()] {
        metrics.record_output_input(&output_metrics, payload.len(), payload.len());
        enqueue_for_test(
            0,
            InboundMessage {
                output_channel: "events".to_owned(),
                payload: payload.into(),
            },
            &mut pending,
            &mut passthrough,
            &metrics,
            &output_metrics,
        );
    }

    assert_eq!(passthrough.len(), 2);
    let no_conflated_pending = HashMap::new();
    let batch = pending_batch(0, 256, usize::MAX, &no_conflated_pending, &passthrough);
    assert_eq!(batch[0].payload.as_ref(), b"first");
    clear_published_batch(0, &mut pending, &mut passthrough, &batch);
    assert_eq!(
        pending_batch(0, 256, usize::MAX, &no_conflated_pending, &passthrough,)[0]
            .payload
            .as_ref(),
        b"second"
    );
}

#[tokio::test]
async fn passthrough_batches_ready_items_in_fifo_order_and_respects_command_cap() {
    let metrics = Metrics::new();
    let output_metrics = metrics.register_output("paced-passthrough-output");
    let mut pending = PendingByInterval::new();
    let mut passthrough = VecDeque::new();
    let mut cache = DeduplicationCache::new(0);
    for payload in [b"first".to_vec(), b"second".to_vec(), b"third".to_vec()] {
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
    }
    let mut publisher = RecordingPublisher::default();
    let mut failure_log = OutputFailureLog::default();
    let mut context = publish_context_for_test(
        "paced-passthrough-output",
        &mut failure_log,
        &mut cache,
        &metrics,
        &output_metrics,
    );

    assert!(
        publish_passthrough_batch(
            &mut publisher,
            &mut pending,
            &mut passthrough,
            2,
            usize::MAX,
            &mut context,
        )
        .await
    );
    assert_eq!(publisher.successful_batches.len(), 1);
    assert!(publisher.attempts[0].0);
    assert_eq!(
        publisher.successful_batches[0]
            .iter()
            .map(|message| message.payload.as_ref())
            .collect::<Vec<_>>(),
        [b"first".as_slice(), b"second".as_slice()]
    );
    assert_eq!(passthrough.len(), 1);
    assert_eq!(passthrough.front().unwrap().payload.as_ref(), b"third");

    assert!(
        publish_passthrough_batch(
            &mut publisher,
            &mut pending,
            &mut passthrough,
            2,
            usize::MAX,
            &mut context,
        )
        .await
    );
    assert_eq!(publisher.successful_batches.len(), 2);
    assert!(!publisher.attempts[1].0);
    assert_eq!(
        publisher.successful_batches[1][0].payload.as_ref(),
        b"third"
    );
    assert!(passthrough.is_empty());
    assert!(
        !publish_passthrough_batch(
            &mut publisher,
            &mut pending,
            &mut passthrough,
            2,
            usize::MAX,
            &mut context,
        )
        .await
    );
}

#[test]
fn passthrough_exec_batch_respects_resp_byte_cap() {
    let mut passthrough = VecDeque::new();
    let first = pending_message("events", b"first".to_vec());
    let second = pending_message("events", b"second".to_vec());
    let one_command = publish_command_frame_bytes(&first);
    let two_command_exec = exec_transaction_frame_bytes(&[first.clone(), second.clone()]);
    passthrough.extend([first, second]);

    assert_eq!(
        passthrough_batch_length(10, two_command_exec - 1, passthrough.iter()),
        (1, false, one_command)
    );
    assert_eq!(
        passthrough_batch_length(10, two_command_exec, passthrough.iter()),
        (2, true, two_command_exec)
    );
    assert!(one_command < two_command_exec);
}

#[test]
fn direct_batch_boundary_preserves_per_channel_and_group_ttl_order() {
    let mut queued = pending_message("events:a", b"first".to_vec());
    queued.deduplication_ttl_ms = Some(5000);
    let mut passthrough = VecDeque::from([queued]);
    let same_channel = InboundMessage {
        output_channel: "events:a".to_owned(),
        payload: b"second".to_vec().into(),
    };
    let per_channel_policy = ResolvedChannelPolicy {
        interval_ms: 0,
        deduplication_ttl_ms: 5000,
        deduplication_group: None,
    };
    assert!(direct_batch_boundary_required(
        &passthrough,
        &same_channel,
        &per_channel_policy
    ));
    let different_channel = InboundMessage {
        output_channel: "events:b".to_owned(),
        payload: b"other".to_vec().into(),
    };
    assert!(!direct_batch_boundary_required(
        &passthrough,
        &different_channel,
        &per_channel_policy
    ));

    passthrough.front_mut().unwrap().deduplication_group = Some("shared".to_owned());
    let group_policy = ResolvedChannelPolicy {
        interval_ms: 0,
        deduplication_ttl_ms: 5000,
        deduplication_group: Some("shared".to_owned()),
    };
    assert!(direct_batch_boundary_required(
        &passthrough,
        &different_channel,
        &group_policy
    ));
}

#[tokio::test]
async fn failed_transaction_is_not_replayed_and_later_chunks_continue() {
    let metrics = Metrics::new();
    let output_metrics = metrics.register_output("custom-output");
    let mut pending = HashMap::new();
    let mut passthrough = VecDeque::new();

    for channel in ["events:d", "events:a", "events:e", "events:b", "events:c"] {
        let payload = vec![0, channel.as_bytes()[7]];
        metrics.record_output_input(&output_metrics, payload.len(), payload.len());
        enqueue_for_test(
            10,
            InboundMessage {
                output_channel: channel.to_owned(),
                payload: payload.into(),
            },
            &mut pending,
            &mut passthrough,
            &metrics,
            &output_metrics,
        );
    }

    let mut publisher = RecordingPublisher {
        uncertain_failures_remaining: 1,
        ..RecordingPublisher::default()
    };
    let mut failure_log = OutputFailureLog::default();
    let mut deduplication_cache = DeduplicationCache::new(5000);
    let mut publish_context = publish_context_for_test(
        "custom-output",
        &mut failure_log,
        &mut deduplication_cache,
        &metrics,
        &output_metrics,
    );
    publish_conflated_pending(
        &mut publisher,
        &mut pending,
        2,
        usize::MAX,
        &mut publish_context,
    )
    .await;

    let attempted_channels = publisher
        .attempts
        .iter()
        .map(|(atomic, batch)| {
            assert!(*atomic);
            assert!(batch.len() <= 2);
            batch
                .iter()
                .map(|message| message.output_channel.as_str())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        attempted_channels,
        vec![
            vec!["events:a", "events:b"],
            vec!["events:c", "events:d"],
            vec!["events:e"],
        ]
    );
    assert_eq!(publisher.possible_executions.len(), 1);
    assert_eq!(publisher.possible_executions[0].len(), 2);
    assert_eq!(
        deduplication_cache.entries["events:a"].raw_payload.as_ref(),
        [0, b'a']
    );
    assert_eq!(
        deduplication_cache.entries["events:b"].raw_payload.as_ref(),
        [0, b'b']
    );

    let published = publisher
        .successful_batches
        .iter()
        .flatten()
        .map(|message| (message.output_channel.as_str(), message.payload.as_ref()))
        .collect::<Vec<_>>();
    let expected = ["a", "b", "c", "d", "e"]
        .into_iter()
        .map(|suffix| (format!("events:{suffix}"), vec![0, suffix.as_bytes()[0]]))
        .collect::<Vec<_>>();
    assert_eq!(
        published,
        expected
            .iter()
            .skip(2)
            .map(|(channel, payload)| (channel.as_str(), payload.as_slice()))
            .collect::<Vec<_>>()
    );
    assert!(pending.is_empty());

    let snapshot = metrics.snapshot();
    let output = &snapshot.outputs["custom-output"];
    assert_eq!(output.output_batches_total, 2);
    assert_eq!(output.output_messages_total, 3);
    assert_eq!(output.publish_errors_total, 1);
    assert_eq!(output.publish_error_messages_total, 2);
    assert_eq!(output.uncertain_transactions_total, 1);
    assert_eq!(output.uncertain_messages_total, 2);
    assert_eq!(output.dropped_messages_total, 0);
    assert_eq!(output.pending_messages, 0);
    assert_eq!(output.pending_payload_bytes, 0);
    assert_eq!(output.pending_keys, 0);
}

#[tokio::test]
async fn definitive_pre_send_chunk_failure_is_counted_as_dropped_and_continues() {
    let metrics = Metrics::new();
    let output_metrics = metrics.register_output("failed-output");
    let mut pending = HashMap::new();
    let mut passthrough = VecDeque::new();
    for suffix in ["a", "b", "c"] {
        let payload = suffix.as_bytes().to_vec();
        metrics.record_output_input(&output_metrics, payload.len(), payload.len());
        enqueue_for_test(
            10,
            InboundMessage {
                output_channel: format!("events:{suffix}"),
                payload: payload.into(),
            },
            &mut pending,
            &mut passthrough,
            &metrics,
            &output_metrics,
        );
    }
    let mut publisher = RecordingPublisher {
        not_sent_failures_remaining: 1,
        ..RecordingPublisher::default()
    };
    let mut failure_log = OutputFailureLog::default();
    let mut deduplication_cache = DeduplicationCache::new(5000);
    let mut publish_context = publish_context_for_test(
        "failed-output",
        &mut failure_log,
        &mut deduplication_cache,
        &metrics,
        &output_metrics,
    );
    publish_conflated_pending(
        &mut publisher,
        &mut pending,
        2,
        usize::MAX,
        &mut publish_context,
    )
    .await;

    assert_eq!(publisher.attempts.len(), 2);
    assert_eq!(publisher.attempts[0].1[0].output_channel, "events:a");
    assert_eq!(publisher.attempts[0].1[1].output_channel, "events:b");
    assert_eq!(publisher.attempts[1].1[0].output_channel, "events:c");
    assert_eq!(publisher.successful_batches.len(), 1);
    assert!(!deduplication_cache.entries.contains_key("events:a"));
    assert!(!deduplication_cache.entries.contains_key("events:b"));
    assert!(deduplication_cache.entries.contains_key("events:c"));
    let output = &metrics.snapshot().outputs["failed-output"];
    assert_eq!(output.dropped_messages_total, 2);
    assert_eq!(output.dropped_payload_bytes_total, 2);
    assert_eq!(output.publish_errors_total, 1);
    assert_eq!(output.publish_error_messages_total, 2);
    assert_eq!(output.pending_messages, 0);
    assert_eq!(output.pending_payload_bytes, 0);
}

#[tokio::test]
async fn direct_publish_failure_is_not_retried_and_next_item_continues() {
    let metrics = Metrics::new();
    let output_metrics = metrics.register_output("immediate-output");
    let mut pending = HashMap::new();
    let mut passthrough = VecDeque::new();
    for payload in [vec![0, 255], b"later".to_vec()] {
        metrics.record_output_input(&output_metrics, payload.len(), payload.len());
        enqueue_for_test(
            0,
            InboundMessage {
                output_channel: "events".to_owned(),
                payload: payload.into(),
            },
            &mut pending,
            &mut passthrough,
            &metrics,
            &output_metrics,
        );
    }
    let mut publisher = RecordingPublisher {
        uncertain_failures_remaining: 1,
        ..RecordingPublisher::default()
    };
    let mut failure_log = OutputFailureLog::default();
    let mut deduplication_cache = DeduplicationCache::new(5000);
    let mut publish_context = publish_context_for_test(
        "immediate-output",
        &mut failure_log,
        &mut deduplication_cache,
        &metrics,
        &output_metrics,
    );
    publish_passthrough_pending(
        &mut publisher,
        &mut pending,
        &mut passthrough,
        &mut publish_context,
    )
    .await;

    assert_eq!(publisher.attempts.len(), 2);
    assert!(publisher.attempts.iter().all(|(atomic, _)| !atomic));
    assert_eq!(publisher.possible_executions.len(), 1);
    assert_eq!(
        publisher.possible_executions[0][0].payload.as_ref(),
        [0, 255]
    );
    assert_eq!(publisher.successful_batches.len(), 1);
    assert_eq!(
        deduplication_cache.entries["events"].raw_payload.as_ref(),
        b"later"
    );
    assert_eq!(
        publisher.successful_batches[0][0].payload.as_ref(),
        b"later"
    );
    let output = &metrics.snapshot().outputs["immediate-output"];
    assert_eq!(output.uncertain_transactions_total, 1);
    assert_eq!(output.uncertain_messages_total, 1);
    assert_eq!(output.publish_error_messages_total, 1);
    assert_eq!(output.output_messages_total, 1);
    assert_eq!(output.pending_messages, 0);
    assert_eq!(output.pending_payload_bytes, 0);
}

#[tokio::test]
async fn uncertain_direct_exec_marks_whole_bounded_batch_and_continues() {
    let metrics = Metrics::new();
    let output_metrics = metrics.register_output("direct-exec-failure-output");
    let mut pending = PendingByInterval::new();
    let mut passthrough = VecDeque::new();
    let mut cache = DeduplicationCache::new(0);
    for payload in [b"first".to_vec(), b"second".to_vec(), b"third".to_vec()] {
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
    }
    let mut publisher = RecordingPublisher {
        uncertain_failures_remaining: 1,
        ..RecordingPublisher::default()
    };
    let mut failure_log = OutputFailureLog::default();
    let mut context = publish_context_for_test(
        "direct-exec-failure-output",
        &mut failure_log,
        &mut cache,
        &metrics,
        &output_metrics,
    );

    assert!(
        publish_passthrough_batch(
            &mut publisher,
            &mut pending,
            &mut passthrough,
            2,
            usize::MAX,
            &mut context,
        )
        .await
    );
    assert!(publisher.attempts[0].0);
    assert_eq!(publisher.attempts[0].1.len(), 2);
    assert_eq!(publisher.possible_executions[0].len(), 2);
    assert_eq!(passthrough.len(), 1);

    assert!(
        publish_passthrough_batch(
            &mut publisher,
            &mut pending,
            &mut passthrough,
            2,
            usize::MAX,
            &mut context,
        )
        .await
    );
    assert!(!publisher.attempts[1].0);
    assert_eq!(
        publisher.successful_batches[0][0].payload.as_ref(),
        b"third"
    );
    assert!(passthrough.is_empty());
    let output = &metrics.snapshot().outputs["direct-exec-failure-output"];
    assert_eq!(output.uncertain_transactions_total, 1);
    assert_eq!(output.uncertain_messages_total, 2);
    assert_eq!(output.output_messages_total, 1);
    assert_eq!(output.pending_messages, 0);
}

#[tokio::test]
async fn cache_records_acknowledged_and_ambiguous_sends_but_not_pre_send_failures() {
    let metrics = Metrics::new();
    let output_metrics = metrics.register_output("send-certainty-output");
    let groups = group_settings(5000, true);
    let message = pending_group_message("events", b"payload", "shared");

    let mut not_sent_cache = DeduplicationCache::with_groups(5000, &groups);
    let mut not_sent_publisher = RecordingPublisher {
        not_sent_failures_remaining: 1,
        ..RecordingPublisher::default()
    };
    let mut not_sent_log = OutputFailureLog::default();
    {
        let mut not_sent_context = publish_context_for_test(
            "send-certainty-output",
            &mut not_sent_log,
            &mut not_sent_cache,
            &metrics,
            &output_metrics,
        );
        assert!(
            publish_once(
                &mut not_sent_publisher,
                std::slice::from_ref(&message),
                false,
                &mut not_sent_context,
            )
            .await
            .is_err()
        );
    }
    assert!(not_sent_cache.entries.is_empty());
    assert!(not_sent_cache.groups.is_empty());
    assert!(!not_sent_cache.should_suppress(&message, time::Instant::now()));

    let mut uncertain_cache = DeduplicationCache::with_groups(5000, &groups);
    let mut uncertain_publisher = RecordingPublisher {
        uncertain_failures_remaining: 1,
        ..RecordingPublisher::default()
    };
    let mut uncertain_log = OutputFailureLog::default();
    {
        let mut uncertain_context = publish_context_for_test(
            "send-certainty-output",
            &mut uncertain_log,
            &mut uncertain_cache,
            &metrics,
            &output_metrics,
        );
        assert!(
            publish_once(
                &mut uncertain_publisher,
                std::slice::from_ref(&message),
                false,
                &mut uncertain_context,
            )
            .await
            .is_err()
        );
    }
    assert!(uncertain_cache.should_suppress(&message, time::Instant::now()));
    assert!(uncertain_cache.groups.contains_key("shared"));
}

#[test]
fn transaction_byte_limit_splits_at_the_exact_wire_size_boundary() {
    let first = pending_message("events:a", vec![0; 8]);
    let second = pending_message("events:b", vec![255; 12]);
    let pair = [first.clone(), second.clone()];
    let exact_limit = exec_transaction_frame_bytes(&pair);
    let pending = HashMap::from([
        (first.output_channel.clone(), first.clone()),
        (second.output_channel.clone(), second.clone()),
    ]);

    assert_eq!(publish_command_frame_bytes(&first), 45);
    assert_eq!(
        exact_limit,
        RESP_MULTI_FRAME_BYTES
            + RESP_EXEC_FRAME_BYTES
            + publish_command_frame_bytes(&first)
            + publish_command_frame_bytes(&second)
    );
    assert_eq!(
        pending_batch(10, 2, exact_limit, &pending, &VecDeque::new()),
        pair
    );
    assert_eq!(
        pending_batch(10, 2, exact_limit - 1, &pending, &VecDeque::new()),
        vec![first]
    );
}

#[test]
fn oversized_send_keeps_the_message_as_a_singleton_chunk() {
    let message = pending_message("events:large", vec![0xff; 128]);
    let later = pending_message("events:small", b"later".to_vec());
    let limit = exec_transaction_frame_bytes(std::slice::from_ref(&later));
    assert!(exec_transaction_frame_bytes(std::slice::from_ref(&message)) > limit);
    let pending = HashMap::from([
        (message.output_channel.clone(), message.clone()),
        (later.output_channel.clone(), later.clone()),
    ]);

    assert_eq!(
        pending_batch(10, 256, limit, &pending, &VecDeque::new()),
        vec![message]
    );
}

#[test]
fn truncate_preserves_payload_prefix_and_exact_exec_byte_target() {
    let metrics = Metrics::new();
    let output_metrics = metrics.register_output("truncate-output");
    let payload = vec![0, 255, 1, 254, 2, 253];
    metrics.record_output_input(&output_metrics, payload.len(), payload.len());
    let mut expected_message =
        pending_message_with_raw("events:binary", payload[..3].to_vec(), payload.clone());
    expected_message.conflation_interval_ms = 10;
    let max_bytes = publish_operation_frame_bytes(&expected_message, true);
    let mut policy_log = OversizedPolicyLog::default();

    let prepared = prepare_for_test(
        "truncate-output",
        OutputMessagePolicy {
            interval_ms: 10,
            deduplication_ttl_ms: None,
            deduplication_group: None,
            max_bytes_per_exec: max_bytes,
            oversized_policy: OversizedMessagePolicy::Truncate,
        },
        InboundMessage {
            output_channel: "events:binary".to_owned(),
            payload: payload.into(),
        },
        &metrics,
        &output_metrics,
        &mut policy_log,
    )
    .unwrap();

    assert_eq!(prepared, expected_message);
    assert_eq!(publish_operation_frame_bytes(&prepared, true), max_bytes);
    let output = &metrics.snapshot().outputs["truncate-output"];
    assert_eq!(output.truncated_messages_total, 1);
    assert_eq!(output.truncated_payload_bytes_total, 3);
    assert_eq!(output.pending_messages, 1);
    assert_eq!(output.pending_payload_bytes, 3);
}

#[test]
fn truncate_drops_when_even_an_empty_publish_frame_exceeds_the_target() {
    let metrics = Metrics::new();
    let output_metrics = metrics.register_output("too-small-output");
    metrics.record_output_input(&output_metrics, 3, 3);
    let channel = "events:long-channel";
    let max_bytes = publish_operation_frame_bytes(&pending_message(channel, Vec::new()), true) - 1;
    let mut policy_log = OversizedPolicyLog::default();

    assert!(
        prepare_for_test(
            "too-small-output",
            OutputMessagePolicy {
                interval_ms: 10,
                deduplication_ttl_ms: None,
                deduplication_group: None,
                max_bytes_per_exec: max_bytes,
                oversized_policy: OversizedMessagePolicy::Truncate,
            },
            InboundMessage {
                output_channel: channel.to_owned(),
                payload: b"abc".to_vec().into(),
            },
            &metrics,
            &output_metrics,
            &mut policy_log,
        )
        .is_none()
    );
    let output = &metrics.snapshot().outputs["too-small-output"];
    assert_eq!(output.dropped_messages_total, 1);
    assert_eq!(output.pending_messages, 0);
    assert_eq!(output.pending_payload_bytes, 0);
}

#[test]
fn direct_truncation_uses_publishes_frame_without_transaction_wrappers() {
    let metrics = Metrics::new();
    let output_metrics = metrics.register_output("direct-truncate-output");
    let channel = "events:direct";
    let payload = vec![0, 255, 1, 254, 2, 253];
    metrics.record_output_input(&output_metrics, payload.len(), payload.len());
    let max_bytes = publish_command_frame_bytes_for_lengths(channel.len(), 2);
    let mut policy_log = OversizedPolicyLog::default();

    let prepared = prepare_for_test(
        "direct-truncate-output",
        OutputMessagePolicy {
            interval_ms: 0,
            deduplication_ttl_ms: None,
            deduplication_group: None,
            max_bytes_per_exec: max_bytes,
            oversized_policy: OversizedMessagePolicy::Truncate,
        },
        InboundMessage {
            output_channel: channel.to_owned(),
            payload: payload.into(),
        },
        &metrics,
        &output_metrics,
        &mut policy_log,
    )
    .unwrap();

    assert_eq!(prepared.payload.as_ref(), [0, 255]);
    assert_eq!(publish_command_frame_bytes(&prepared), max_bytes);
    assert_eq!(
        publish_operation_frame_bytes(&prepared, true),
        max_bytes + 29
    );
}

#[test]
fn drop_policy_skips_one_oversized_message_and_counts_it() {
    let metrics = Metrics::new();
    let output_metrics = metrics.register_output("drop-output");
    let payload = vec![0xff; 10];
    metrics.record_output_input(&output_metrics, payload.len(), payload.len());
    let mut policy_log = OversizedPolicyLog::default();

    assert!(
        prepare_for_test(
            "drop-output",
            OutputMessagePolicy {
                interval_ms: 0,
                deduplication_ttl_ms: None,
                deduplication_group: None,
                max_bytes_per_exec: 1,
                oversized_policy: OversizedMessagePolicy::Drop,
            },
            InboundMessage {
                output_channel: "events:too-large".to_owned(),
                payload: payload.into(),
            },
            &metrics,
            &output_metrics,
            &mut policy_log,
        )
        .is_none()
    );
    let output = &metrics.snapshot().outputs["drop-output"];
    assert_eq!(output.dropped_messages_total, 1);
    assert_eq!(output.pending_messages, 0);
    assert_eq!(output.pending_payload_bytes, 0);
}

#[test]
fn output_batch_is_deterministic_and_retains_one_payload_per_channel() {
    let pending = HashMap::from([
        (
            "replica:b".to_owned(),
            pending_message("replica:b", vec![0, 255]),
        ),
        (
            "replica:a".to_owned(),
            pending_message("replica:a", b"value".to_vec()),
        ),
    ]);
    let batch = pending_batch(10, 256, usize::MAX, &pending, &VecDeque::new());

    assert_eq!(batch.len(), 2);
    assert_eq!(batch[0].output_channel, "replica:a");
    assert_eq!(batch[1].payload.as_ref(), [0, 255]);
}
