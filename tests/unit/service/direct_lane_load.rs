//! Manual release-mode load probe for the direct lane (`conflation.interval_ms <= 0`).
//!
//! Purpose ("necessity gate"): the direct lane keeps every message in `passthrough`
//! and settles publishes strictly in FIFO order today. The worker submits batches
//! without awaiting each reply (`FuturesOrdered`) but only clears them from
//! `passthrough` head-first, so a slow head batch stalls accounting for every batch
//! behind it. This probe quantifies the cost of batching (`max_commands_per_exec`)
//! plus a simulated publish latency on that path, and verifies that the published
//! payload order stays strictly increasing. The drain loop awaits each batch before
//! submitting the next, so it is a conservative floor for the pipelined worker.
//!
//! This is *not* the saturation gate. The real end-to-end gate is the open-loop
//! E2E benchmark, which is the only place the direct lane can be shown to saturate
//! under production pressure. Use this probe as the supporting necessity gate: if
//! the direct lane cannot keep up under this order-preserving drain, relaxing FIFO
//! settlement becomes justified, and the relaxation must then be re-validated
//! against the open-loop benchmark.
//!
//! Run manually (ignored by default):
//! `cargo test --release --locked direct_lane_load_benchmark -- --ignored --nocapture`
//!
//! Optional tuning:
//! - `DIRECT_LANE_LOAD_MESSAGES` overrides the message count (default 10_000).
//! - `DIRECT_LANE_LOAD_LATENCY_US` overrides the simulated per-command latency
//!   (default 50 µs).
//!
//! Note: `tokio::time::sleep` cannot resolve below the platform timer floor
//! (~1 ms on Linux, ~15 ms on Windows), so small batches pay that floor per
//! publish call. Lower `DIRECT_LANE_LOAD_MESSAGES` if a Windows run is too slow;
//! the shape of the comparison is unchanged.

use std::time::Instant;

use super::*;

const DEFAULT_MESSAGE_COUNT: usize = 10_000;
const DEFAULT_LATENCY_US: u64 = 50;
const MAX_BYTES_PER_EXEC: usize = 4 * 1024 * 1024;
const BATCH_SIZES: [usize; 3] = [1, 32, 256];

/// In-memory [`BatchPublisher`] that sleeps a simulated per-command latency before
/// acknowledging each batch, records every payload in publication order (so FIFO
/// can be asserted without Redis), and can fail the batch that crosses a command
/// threshold so the failure path is exercised without a broker.
struct LatencyPublisher {
    latency_us: u64,
    fail_at_command: Option<usize>,
    commands_seen: usize,
    batches_published: usize,
    failed_batches: usize,
    published_payloads: Vec<Vec<u8>>,
}

impl LatencyPublisher {
    fn new(latency_us: u64) -> Self {
        Self {
            latency_us,
            fail_at_command: None,
            commands_seen: 0,
            batches_published: 0,
            failed_batches: 0,
            published_payloads: Vec::new(),
        }
    }

    fn failing_at(latency_us: u64, fail_at_command: usize) -> Self {
        Self {
            fail_at_command: Some(fail_at_command),
            ..Self::new(latency_us)
        }
    }
}

impl BatchPublisher for LatencyPublisher {
    async fn publish(
        &mut self,
        messages: &[PendingMessage],
        _atomic: bool,
    ) -> std::result::Result<i64, PublishFailure> {
        // One sleep per batch sized by the command count: the simulated server
        // work is charged per command, while the batching decision controls how
        // many commands share a single await (one simulated round trip).
        let simulated_us = self
            .latency_us
            .saturating_mul(u64::try_from(messages.len()).unwrap_or(u64::MAX));
        time::sleep(Duration::from_micros(simulated_us)).await;
        if self
            .fail_at_command
            .is_some_and(|limit| self.commands_seen + messages.len() > limit)
        {
            // One-shot failure: the batch that crosses the threshold is lost and
            // later batches keep publishing.
            self.fail_at_command = None;
            self.failed_batches += 1;
            return Err(PublishFailure::NotSent(
                "simulated direct-lane publish failure".to_owned(),
            ));
        }
        self.commands_seen += messages.len();
        self.batches_published += 1;
        self.published_payloads
            .extend(messages.iter().map(|message| message.payload.clone()));
        Ok(i64::try_from(messages.len()).unwrap_or(i64::MAX))
    }
}

#[tokio::test(flavor = "current_thread")]
async fn direct_lane_failure_drops_the_batch_and_keeps_publishing_in_order() {
    let metrics = Metrics::new();
    let output_metrics = metrics.register_output("direct-lane-failure");
    let mut pending = PendingByInterval::new();
    let mut passthrough = VecDeque::new();
    let mut deduplication_cache = DeduplicationCache::new(0);
    for sequence in 0..6u64 {
        enqueue_for_test_with_cache(
            0,
            InboundMessage {
                output_channel: "bench:direct".to_owned(),
                payload: sequence.to_be_bytes().to_vec(),
            },
            &mut pending,
            &mut passthrough,
            &metrics,
            &output_metrics,
            &mut deduplication_cache,
        );
    }
    let mut publisher = LatencyPublisher::failing_at(0, 3);
    let mut failure_log = OutputFailureLog::default();
    let mut context = publish_context_for_test(
        "direct-lane-failure",
        &mut failure_log,
        &mut deduplication_cache,
        &metrics,
        &output_metrics,
    );

    while publish_passthrough_batch(
        &mut publisher,
        &mut pending,
        &mut passthrough,
        2,
        MAX_BYTES_PER_EXEC,
        &mut context,
    )
    .await
    {
        tokio::task::yield_now().await;
    }

    assert_eq!(publisher.failed_batches, 1);
    assert!(passthrough.is_empty());
    assert_eq!(
        publisher
            .published_payloads
            .iter()
            .map(|payload| sequence_of(payload))
            .collect::<Vec<_>>(),
        [0, 1, 4, 5]
    );
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "manual release-mode direct-lane load benchmark; run with --ignored --nocapture"]
async fn direct_lane_load_benchmark() {
    let message_count = environment("DIRECT_LANE_LOAD_MESSAGES", DEFAULT_MESSAGE_COUNT);
    let latency_us = environment("DIRECT_LANE_LOAD_LATENCY_US", DEFAULT_LATENCY_US);
    assert!(message_count > 0, "message count must be greater than zero");
    if cfg!(debug_assertions) {
        println!(
            "direct-lane-load warning: debug build; re-run with --release for meaningful numbers"
        );
    }
    println!(
        "direct-lane-load messages={message_count} latency_us={latency_us} max_bytes_per_exec={MAX_BYTES_PER_EXEC} batch_sizes={BATCH_SIZES:?}"
    );
    println!(
        "{:<8} {:>9} {:>9} {:>12} {:>12} {:>6}",
        "batch", "batches", "seconds", "messages/s", "batches/s", "fifo"
    );

    for batch_size in BATCH_SIZES {
        let metrics = Metrics::new();
        let output_metrics = metrics.register_output(&format!("direct-lane-load-{batch_size}"));
        let mut pending = PendingByInterval::new();
        let mut passthrough = VecDeque::new();
        let mut deduplication_cache = DeduplicationCache::new(0);
        for sequence in 0..u64::try_from(message_count).unwrap_or(u64::MAX) {
            enqueue_for_test_with_cache(
                0,
                InboundMessage {
                    output_channel: "bench:direct".to_owned(),
                    payload: sequence.to_be_bytes().to_vec(),
                },
                &mut pending,
                &mut passthrough,
                &metrics,
                &output_metrics,
                &mut deduplication_cache,
            );
        }
        assert_eq!(passthrough.len(), message_count);

        let mut publisher = LatencyPublisher::new(latency_us);
        let mut failure_log = OutputFailureLog::default();
        let mut context = publish_context_for_test(
            "direct-lane-load",
            &mut failure_log,
            &mut deduplication_cache,
            &metrics,
            &output_metrics,
        );
        let started = Instant::now();
        while publish_passthrough_batch(
            &mut publisher,
            &mut pending,
            &mut passthrough,
            batch_size,
            MAX_BYTES_PER_EXEC,
            &mut context,
        )
        .await
        {
            tokio::task::yield_now().await;
        }
        let elapsed = started.elapsed();

        assert!(passthrough.is_empty(), "direct lane did not fully drain");
        assert_eq!(pending_message_count(&pending), 0);
        assert_eq!(publisher.commands_seen, message_count);
        assert_eq!(publisher.published_payloads.len(), message_count);
        assert_strictly_increasing(&publisher.published_payloads);
        println!(
            "{:<8} {:>9} {:>9.3} {:>12.0} {:>12.1} {:>6}",
            batch_size,
            publisher.batches_published,
            elapsed.as_secs_f64(),
            message_count as f64 / elapsed.as_secs_f64(),
            publisher.batches_published as f64 / elapsed.as_secs_f64(),
            "ok",
        );
    }
}

fn assert_strictly_increasing(payloads: &[Vec<u8>]) {
    let mut previous = None;
    for (position, payload) in payloads.iter().enumerate() {
        let sequence = sequence_of(payload);
        if let Some(previous) = previous {
            assert!(
                sequence > previous,
                "direct lane FIFO violation at publication {position}: {sequence} published after {previous}"
            );
        }
        previous = Some(sequence);
    }
}

fn sequence_of(payload: &[u8]) -> u64 {
    u64::from_be_bytes(
        payload
            .try_into()
            .expect("benchmark payloads carry an 8-byte big-endian sequence"),
    )
}

fn environment<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}
