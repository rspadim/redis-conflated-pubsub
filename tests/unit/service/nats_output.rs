//! NATS output sink tests that need no live server.

use super::super::backend::{NATS_HOP_HEADER, NATS_HOP_HEADER_WIRE_BYTES};
use super::super::nats::output::{
    NATS_NO_SUBSCRIBER_COUNT, PublishStep, check_payload_size, hop_headers, preflight_chunk,
};
use crate::config::validate_nats_publish_subject;
use crate::service::tests::pending_message;

fn not_sent(step: PublishStep) -> String {
    match step {
        PublishStep::NotSent(message) => message,
        PublishStep::Uncertain(message) => panic!("expected NotSent, got Uncertain: {message}"),
    }
}

/// The NATS sink returns no subscriber count. This pins the honest status
/// choice: success means "the release's publish commands left the local client
/// buffer and the flush drained it", never "N subscribers received it". Core
/// NATS has no per-message acknowledgement.
#[test]
fn nats_success_carries_no_subscriber_count() {
    assert_eq!(NATS_NO_SUBSCRIBER_COUNT, 0);
}

/// The sink validates each mapped subject with the same rule set as
/// `--check-config` before handing it to the client, so a Redis-style mapping
/// that leaves a wildcard or a trailing dot is a definitive pre-send failure.
#[test]
fn concrete_subject_validation_matches_the_config_rule_set() {
    assert!(validate_nats_publish_subject("subject", "archive.prices.a.v1").is_ok());
    assert!(validate_nats_publish_subject("subject", "archive.prices.*.v1").is_err());
    assert!(validate_nats_publish_subject("subject", "archive.prices.>.v1").is_err());
    assert!(validate_nats_publish_subject("subject", "archive..v1").is_err());
    assert!(validate_nats_publish_subject("subject", "archive.prices.a.v1 ").is_err());
}

/// The hop header is the constant the output stamps and the input checks. Pinning
/// it stops the two sides from silently drifting apart.
#[test]
fn hop_header_name_is_stable() {
    assert_eq!(NATS_HOP_HEADER, "Relay-Hop");
}

/// A relayed message carries exactly the hop header and no application headers,
/// which keeps the per-message wire size constant for the ceiling accounting.
#[test]
fn only_the_hop_header_is_forwarded() {
    let debug = format!("{:?}", hop_headers());
    assert!(debug.contains(NATS_HOP_HEADER), "{debug}");
    // The only key present is the hop header: no application header leaks in.
    assert_eq!(debug.matches("Relay-Hop").count(), 1, "{debug}");
}

/// Payloads above the server-advertised maximum are a definitive pre-send
/// failure (`NotSent`), never an optimistic send that the server drops. The
/// relay's own hop header counts toward the server limit, so the usable payload
/// is `max_payload - header_wire_bytes`; a payload that fits only without the
/// header is rejected here rather than by the client.
#[test]
fn oversized_payloads_are_rejected_before_publishing() {
    // Pin the wire size of the relay's one-header publish; a header-format
    // change in async-nats would silently shift the usable ceiling otherwise.
    assert_eq!(NATS_HOP_HEADER_WIRE_BYTES, 26);
    let ceiling = 1024 - NATS_HOP_HEADER_WIRE_BYTES;
    assert!(check_payload_size("events", ceiling, 1024).is_ok());
    // Exactly the server max is too big once the header is counted.
    let error = not_sent(check_payload_size("events", 1024, 1024).unwrap_err());
    assert!(error.contains("exceeds the server maximum"), "{error}");
    let error = not_sent(check_payload_size("events", ceiling + 1, 1024).unwrap_err());
    assert!(error.contains("exceeds the server maximum"), "{error}");
}

/// `preflight_chunk` validates the whole chunk before anything is published, so
/// a chunk whose last message is bad is rejected as `NotSent` without a partial
/// send. This is what keeps the accounting honest.
#[test]
fn preflight_rejects_the_whole_chunk_before_any_publish() {
    let max = 1024;
    let ceiling = max - NATS_HOP_HEADER_WIRE_BYTES;
    let good = pending_message("events.a", vec![0u8; 32]);

    // All messages valid: the chunk is sendable.
    let ok = vec![good.clone(), pending_message("events.b", vec![0u8; 64])];
    assert!(preflight_chunk(&ok, max).is_ok());

    // Last message oversized: the whole chunk is rejected up front, so the
    // earlier (valid) messages are not published either.
    let oversized = vec![
        good.clone(),
        pending_message("events.b", vec![0u8; ceiling + 1]),
    ];
    assert!(matches!(
        preflight_chunk(&oversized, max),
        Err(PublishStep::NotSent(_))
    ));

    // Last message maps to an invalid (wildcard) subject: also rejected up front.
    let invalid = vec![good, pending_message("events.*.bad", vec![0u8; 8])];
    assert!(matches!(
        preflight_chunk(&invalid, max),
        Err(PublishStep::NotSent(_))
    ));
}
