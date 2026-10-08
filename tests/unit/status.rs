use super::*;

#[test]
fn status_file_is_replaced_with_a_complete_json_snapshot() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("status.json");
    let metrics = Metrics::new();
    metrics.set_state("running");
    metrics.record_input(7, std::time::Duration::from_micros(5));

    write_atomic(&path, &metrics.snapshot()).unwrap();
    metrics.record_input(11, std::time::Duration::from_micros(5));
    write_atomic(&path, &metrics.snapshot()).unwrap();

    let json: serde_json::Value = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
    assert_eq!(json["state"], "running");
    assert_eq!(json["schema_version"], 5);
    assert_eq!(json["input_messages_total"], 2);
    assert_eq!(json["input_payload_bytes_total"], 18);
}

#[test]
fn status_snapshot_exposes_metrics_by_arbitrary_output_name() {
    let metrics = Metrics::new();
    let output = metrics.register_output("custom-output");
    metrics.set_output_state(&output, "running");
    metrics.record_output_input(&output, 4);
    metrics.record_output_input(&output, 6);
    metrics.record_output_conflated(&output, 4);
    metrics.record_output_flush(&output, 1, 6);
    metrics.record_output_queue_wait(&output, std::time::Duration::from_micros(10));
    metrics.record_output_queue_wait(&output, std::time::Duration::from_micros(30));
    metrics.record_output_publish_rtt(&output, std::time::Duration::from_micros(20));
    metrics.set_output_pending_keys(&output, 0);

    let snapshot = serde_json::to_value(metrics.snapshot()).unwrap();

    assert_eq!(snapshot["outputs"]["custom-output"]["state"], "running");
    assert_eq!(
        snapshot["outputs"]["custom-output"]["output_messages_total"],
        1
    );
    assert_eq!(
        snapshot["outputs"]["custom-output"]["input_messages_total"],
        2
    );
    assert_eq!(
        snapshot["outputs"]["custom-output"]["input_payload_bytes_total"],
        10
    );
    assert_eq!(
        snapshot["outputs"]["custom-output"]["output_payload_bytes_total"],
        6
    );
    assert_eq!(
        snapshot["outputs"]["custom-output"]["conflated_messages_total"],
        1
    );
    assert_eq!(
        snapshot["outputs"]["custom-output"]["conflated_payload_bytes_total"],
        4
    );
    assert_eq!(
        snapshot["outputs"]["custom-output"]["message_reduction_percent"],
        50.0
    );
    assert_eq!(
        snapshot["outputs"]["custom-output"]["payload_reduction_percent"],
        40.0
    );
    assert_eq!(snapshot["outputs"]["custom-output"]["pending_messages"], 0);
    assert_eq!(
        snapshot["outputs"]["custom-output"]["queue_wait_samples"],
        2
    );
    assert_eq!(
        snapshot["outputs"]["custom-output"]["queue_wait_total_ns"],
        40_000
    );
    assert_eq!(
        snapshot["outputs"]["custom-output"]["queue_wait_max_ns"],
        30_000
    );
    assert_eq!(
        snapshot["outputs"]["custom-output"]["publish_rtt_samples"],
        1
    );
    assert_eq!(
        snapshot["outputs"]["custom-output"]["publish_rtt_total_ns"],
        20_000
    );
    assert_eq!(
        snapshot["outputs"]["custom-output"]["publish_rtt_max_ns"],
        20_000
    );
    assert_eq!(
        snapshot["outputs"]["custom-output"]["pending_payload_bytes"],
        0
    );
    assert_eq!(snapshot["pending_keys"], 0);
    assert_eq!(snapshot["output_payload_bytes_total"], 6);
    assert_eq!(snapshot["conflated_payload_bytes_total"], 4);
}

#[test]
fn batched_queue_wait_and_pending_keys_update_the_snapshot_once() {
    let metrics = Metrics::new();
    let output = metrics.register_output("batch-output");
    metrics.record_output_queue_wait_batch(&output, 3, 60, 30);
    metrics.record_output_queue_wait_batch(&output, 0, 0, 0);
    metrics.publish_output_pending_keys(&output, 2);
    metrics.publish_output_pending_keys(&output, 2);

    let snapshot = serde_json::to_value(metrics.snapshot()).unwrap();

    assert_eq!(snapshot["outputs"]["batch-output"]["queue_wait_samples"], 3);
    assert_eq!(
        snapshot["outputs"]["batch-output"]["queue_wait_total_ns"],
        60
    );
    assert_eq!(snapshot["outputs"]["batch-output"]["queue_wait_max_ns"], 30);
    assert_eq!(snapshot["outputs"]["batch-output"]["pending_keys"], 2);
    assert_eq!(snapshot["pending_keys"], 2);
}

#[test]
fn reductions_are_unavailable_before_the_first_output_input() {
    let metrics = Metrics::new();
    metrics.register_output("empty-output");

    let snapshot = serde_json::to_value(metrics.snapshot()).unwrap();

    assert!(snapshot["outputs"]["empty-output"]["message_reduction_percent"].is_null());
    assert!(snapshot["outputs"]["empty-output"]["payload_reduction_percent"].is_null());
}

#[test]
fn deduplication_metrics_count_raw_bytes_and_clear_truncated_pending_payload() {
    let metrics = Metrics::new();
    let output = metrics.register_output("dedup-output");
    metrics.record_output_input(&output, 5);
    metrics.record_output_truncated(&output, 3);
    metrics.record_output_deduplicated(&output, 5, 2);

    let snapshot = serde_json::to_value(metrics.snapshot()).unwrap();
    assert_eq!(snapshot["schema_version"], 5);
    assert_eq!(snapshot["deduplicated_messages_total"], 1);
    assert_eq!(snapshot["deduplicated_payload_bytes_total"], 5);
    assert_eq!(
        snapshot["outputs"]["dedup-output"]["deduplicated_messages_total"],
        1
    );
    assert_eq!(
        snapshot["outputs"]["dedup-output"]["deduplicated_payload_bytes_total"],
        5
    );
    assert_eq!(snapshot["outputs"]["dedup-output"]["pending_messages"], 0);
    assert_eq!(
        snapshot["outputs"]["dedup-output"]["pending_payload_bytes"],
        0
    );
}

#[test]
fn deduplication_evictions_are_counted_globally_and_per_output() {
    let metrics = Metrics::new();
    let output = metrics.register_output("evict-output");
    metrics.record_deduplication_evictions(&output, 3);
    metrics.record_deduplication_evictions(&output, 0);

    let snapshot = serde_json::to_value(metrics.snapshot()).unwrap();
    assert_eq!(snapshot["schema_version"], 5);
    assert_eq!(snapshot["deduplication_evictions_total"], 3);
    assert_eq!(
        snapshot["outputs"]["evict-output"]["deduplication_evictions_total"],
        3
    );
}

#[test]
fn oversized_policy_counters_and_pending_bytes_are_exposed_per_output() {
    let metrics = Metrics::new();
    let output = metrics.register_output("policy-output");
    metrics.record_output_input(&output, 10);
    metrics.record_output_truncated(&output, 4);
    metrics.record_output_dropped(&output, 1, 6);

    let snapshot = serde_json::to_value(metrics.snapshot()).unwrap();
    assert_eq!(snapshot["schema_version"], 5);
    assert_eq!(snapshot["dropped_messages_total"], 1);
    assert_eq!(snapshot["dropped_payload_bytes_total"], 6);
    assert_eq!(snapshot["truncated_messages_total"], 1);
    assert_eq!(snapshot["truncated_payload_bytes_total"], 4);
    assert_eq!(
        snapshot["outputs"]["policy-output"]["dropped_messages_total"],
        1
    );
    assert_eq!(
        snapshot["outputs"]["policy-output"]["dropped_payload_bytes_total"],
        6
    );
    assert_eq!(
        snapshot["outputs"]["policy-output"]["truncated_messages_total"],
        1
    );
    assert_eq!(
        snapshot["outputs"]["policy-output"]["truncated_payload_bytes_total"],
        4
    );
    assert_eq!(snapshot["outputs"]["policy-output"]["pending_messages"], 0);
    assert_eq!(
        snapshot["outputs"]["policy-output"]["pending_payload_bytes"],
        0
    );
}

#[test]
fn uncertain_transactions_are_counted_without_leaving_messages_pending() {
    let metrics = Metrics::new();
    let output = metrics.register_output("running-output");
    metrics.record_output_input(&output, 4);
    metrics.record_output_input(&output, 6);
    metrics.record_output_uncertain(&output, "publish acknowledgement was lost", 2);
    metrics.record_output_abandoned(&output, 2, 10);
    metrics.set_output_state(&output, "running");

    let snapshot = serde_json::to_value(metrics.snapshot()).unwrap();

    assert_eq!(snapshot["uncertain_transactions_total"], 1);
    assert_eq!(snapshot["uncertain_messages_total"], 2);
    assert_eq!(snapshot["publish_error_messages_total"], 2);
    assert_eq!(
        snapshot["outputs"]["running-output"]["uncertain_transactions_total"],
        1
    );
    assert_eq!(
        snapshot["outputs"]["running-output"]["uncertain_messages_total"],
        2
    );
    assert_eq!(
        snapshot["outputs"]["running-output"]["publish_error_messages_total"],
        2
    );
    assert_eq!(snapshot["outputs"]["running-output"]["state"], "running");
    assert_eq!(snapshot["outputs"]["running-output"]["pending_messages"], 0);
    assert_eq!(
        snapshot["outputs"]["running-output"]["pending_payload_bytes"],
        0
    );
    assert_eq!(
        snapshot["outputs"]["running-output"]["publish_errors_total"],
        1
    );
}
