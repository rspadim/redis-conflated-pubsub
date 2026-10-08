use std::time::Duration;

use super::super::queue::TryRecv;
use super::*;

fn queued_message(channel: &str, payload: &[u8]) -> QueuedInboundMessage {
    QueuedInboundMessage {
        message: InboundMessage {
            output_channel: channel.to_owned(),
            payload: Arc::from(payload),
        },
        enqueued_at: time::Instant::now(),
    }
}

fn next_payload(receiver: &mut OutputQueueReceiver) -> Option<Vec<u8>> {
    match receiver.try_recv() {
        TryRecv::Message(message) => Some(message.message.payload.to_vec()),
        TryRecv::Empty | TryRecv::Disconnected => None,
    }
}

fn sender_for_test(
    name: &str,
    sender: OutputQueueSender,
    queue_limits: QueueLimits,
    queue_policy: QueueOverflowPolicy,
    output_metrics: Arc<OutputMetrics>,
) -> OutputSender {
    OutputSender {
        name: name.to_owned(),
        channel_prefix: String::new(),
        channel_suffix: String::new(),
        combined_channel_mapping: None,
        channel_filter: ChannelFilterSet::default(),
        sender,
        queue_limits,
        queue_policy,
        queue_shed_log: QueueShedLog::default(),
        output_metrics,
    }
}

#[test]
fn drop_newest_sheds_messages_at_the_message_limit() {
    let metrics = Metrics::new();
    let output_metrics = metrics.register_output("bounded");
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let mut senders = [sender_for_test(
        "bounded",
        OutputQueueSender::Unbounded(sender),
        QueueLimits::new(Some(1), None),
        QueueOverflowPolicy::DropNewest,
        Arc::clone(&output_metrics),
    )];

    fan_out(&mut senders, "", "events", "", b"first", &metrics);
    fan_out(&mut senders, "", "events", "", b"second", &metrics);

    assert_eq!(
        receiver.try_recv().unwrap().message.payload.as_ref(),
        b"first"
    );
    assert!(receiver.try_recv().is_err());
    let snapshot = metrics.snapshot();
    let output = &snapshot.outputs["bounded"];
    // The shed message is rejected before input accounting and admission.
    assert_eq!(output.input_messages_total, 1);
    assert_eq!(output.pending_messages, 1);
    assert_eq!(output.pending_payload_bytes, 5);
    assert_eq!(output.pending_queue_bytes, 11);
    assert_eq!(output.shed_messages_total, 1);
    assert_eq!(output.shed_payload_bytes_total, 6);
    assert_eq!(snapshot.shed_messages_total, 1);
    assert_eq!(snapshot.shed_payload_bytes_total, 6);
}

#[test]
fn drop_newest_sheds_messages_at_the_byte_limit() {
    let metrics = Metrics::new();
    let output_metrics = metrics.register_output("bounded");
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let mut senders = [sender_for_test(
        "bounded",
        OutputQueueSender::Unbounded(sender),
        QueueLimits::new(None, Some(11)),
        QueueOverflowPolicy::DropNewest,
        Arc::clone(&output_metrics),
    )];

    // "events" (6) + 5 payload bytes fits exactly one message.
    fan_out(&mut senders, "", "events", "", b"first", &metrics);
    fan_out(&mut senders, "", "events", "", b"second", &metrics);

    assert_eq!(
        receiver.try_recv().unwrap().message.payload.as_ref(),
        b"first"
    );
    assert!(receiver.try_recv().is_err());
    let snapshot = metrics.snapshot();
    let output = &snapshot.outputs["bounded"];
    assert_eq!(output.input_messages_total, 1);
    assert_eq!(output.pending_queue_bytes, 11);
    assert_eq!(output.shed_messages_total, 1);
    assert_eq!(output.shed_payload_bytes_total, 6);
}

#[test]
fn unbounded_defaults_never_shed() {
    let metrics = Metrics::new();
    let output_metrics = metrics.register_output("unbounded");
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let mut senders = [sender_for_test(
        "unbounded",
        OutputQueueSender::Unbounded(sender),
        QueueLimits::default(),
        QueueOverflowPolicy::DropNewest,
        Arc::clone(&output_metrics),
    )];

    fan_out(&mut senders, "", "events", "", b"first", &metrics);
    fan_out(&mut senders, "", "events", "", b"second", &metrics);

    assert_eq!(
        receiver.try_recv().unwrap().message.payload.as_ref(),
        b"first"
    );
    assert_eq!(
        receiver.try_recv().unwrap().message.payload.as_ref(),
        b"second"
    );
    let snapshot = metrics.snapshot();
    let output = &snapshot.outputs["unbounded"];
    assert_eq!(output.input_messages_total, 2);
    assert_eq!(output.pending_messages, 2);
    assert_eq!(output.shed_messages_total, 0);
    assert_eq!(snapshot.shed_messages_total, 0);
}

#[test]
fn drop_oldest_evicts_the_oldest_queued_message() {
    let metrics = Metrics::new();
    let output_metrics = metrics.register_output("evicting");
    let (sender, mut receiver) = output_queue(QueueOverflowPolicy::DropOldest);
    let mut senders = [sender_for_test(
        "evicting",
        sender,
        QueueLimits::new(Some(1), None),
        QueueOverflowPolicy::DropOldest,
        Arc::clone(&output_metrics),
    )];

    fan_out(&mut senders, "", "events", "", b"first", &metrics);
    fan_out(&mut senders, "", "events", "", b"second", &metrics);

    assert_eq!(next_payload(&mut receiver).as_deref(), Some(&b"second"[..]));
    assert!(matches!(receiver.try_recv(), TryRecv::Empty));
    let snapshot = metrics.snapshot();
    let output = &snapshot.outputs["evicting"];
    assert_eq!(output.input_messages_total, 2);
    assert_eq!(output.pending_messages, 1);
    assert_eq!(output.pending_payload_bytes, 6);
    assert_eq!(output.pending_queue_bytes, 12);
    assert_eq!(output.shed_messages_total, 1);
    assert_eq!(output.shed_payload_bytes_total, 5);
}

#[test]
fn drop_oldest_evicts_an_oversized_message_immediately() {
    let metrics = Metrics::new();
    let output_metrics = metrics.register_output("oversized");
    let (sender, mut receiver) = output_queue(QueueOverflowPolicy::DropOldest);
    let mut senders = [sender_for_test(
        "oversized",
        sender,
        QueueLimits::new(None, Some(3)),
        QueueOverflowPolicy::DropOldest,
        Arc::clone(&output_metrics),
    )];

    fan_out(&mut senders, "", "events", "", b"first", &metrics);

    assert!(matches!(receiver.try_recv(), TryRecv::Empty));
    let snapshot = metrics.snapshot();
    let output = &snapshot.outputs["oversized"];
    assert_eq!(output.pending_messages, 0);
    assert_eq!(output.pending_queue_bytes, 0);
    assert_eq!(output.shed_messages_total, 1);
    assert_eq!(output.shed_payload_bytes_total, 5);
}

#[tokio::test]
async fn drop_oldest_queue_delivers_fifo_and_closes_after_sender_drop() {
    let (sender, mut receiver) = output_queue(QueueOverflowPolicy::DropOldest);
    sender
        .send(queued_message("events", b"first"), QueueLimits::default())
        .unwrap();
    sender
        .send(queued_message("events", b"second"), QueueLimits::default())
        .unwrap();
    drop(sender);

    assert_eq!(
        receiver.recv().await.unwrap().message.payload.as_ref(),
        b"first"
    );
    assert_eq!(
        receiver.recv().await.unwrap().message.payload.as_ref(),
        b"second"
    );
    assert!(receiver.recv().await.is_none());
}

#[test]
fn drop_by_age_sheds_expired_intake_messages_and_releases_pending_gauges() {
    let metrics = Metrics::new();
    let output_metrics = metrics.register_output("aged");
    metrics.record_output_input(&output_metrics, 5, 11);
    let queued = QueuedInboundMessage {
        message: InboundMessage {
            output_channel: "events".to_owned(),
            payload: Arc::from(&b"first"[..]),
        },
        enqueued_at: time::Instant::now()
            .checked_sub(Duration::from_millis(250))
            .unwrap(),
    };
    let mut shed_log = QueueShedLog::default();

    let shed = shed_expired_message(
        "aged",
        &queued,
        Some(100),
        time::Instant::now(),
        &metrics,
        &output_metrics,
        &mut shed_log,
    );

    assert!(shed);
    let snapshot = metrics.snapshot();
    let output = &snapshot.outputs["aged"];
    assert_eq!(output.pending_messages, 0);
    assert_eq!(output.pending_payload_bytes, 0);
    assert_eq!(output.pending_queue_bytes, 0);
    assert_eq!(output.shed_messages_total, 1);
    assert_eq!(output.shed_payload_bytes_total, 5);
    assert_eq!(snapshot.shed_messages_total, 1);
}

#[test]
fn drop_by_age_keeps_messages_within_the_age_limit() {
    let metrics = Metrics::new();
    let output_metrics = metrics.register_output("aged");
    metrics.record_output_input(&output_metrics, 5, 11);
    let queued = QueuedInboundMessage {
        message: InboundMessage {
            output_channel: "events".to_owned(),
            payload: Arc::from(&b"first"[..]),
        },
        enqueued_at: time::Instant::now()
            .checked_sub(Duration::from_millis(50))
            .unwrap(),
    };
    let mut shed_log = QueueShedLog::default();

    let shed = shed_expired_message(
        "aged",
        &queued,
        Some(100),
        time::Instant::now(),
        &metrics,
        &output_metrics,
        &mut shed_log,
    );

    assert!(!shed);
    let snapshot = metrics.snapshot();
    let output = &snapshot.outputs["aged"];
    assert_eq!(output.pending_messages, 1);
    assert_eq!(output.shed_messages_total, 0);
}

#[test]
fn oldest_pending_age_gauge_tracks_admissions_and_clears_when_empty() {
    let metrics = Metrics::new();
    let output = metrics.register_output("aging");
    metrics.record_output_input(&output, 1, 1);
    std::thread::sleep(Duration::from_millis(30));

    let snapshot = metrics.snapshot();
    assert!(snapshot.outputs["aging"].oldest_pending_age_ms >= 10);

    metrics.record_output_flush(&output, 1, 1, 1);
    assert_eq!(metrics.snapshot().outputs["aging"].oldest_pending_age_ms, 0);
}
