//! NATS output sink tests that need no live server.

use super::super::backend::NATS_HOP_HEADER;
use super::super::nats::output::{NATS_NO_SUBSCRIBER_COUNT, PublishStep, check_payload_size};
use crate::config::validate_nats_publish_subject;

fn not_sent(step: PublishStep) -> String {
    match step {
        PublishStep::NotSent(message) => message,
        PublishStep::Uncertain(message) => panic!("expected NotSent, got Uncertain: {message}"),
    }
}

/// The NATS sink returns no subscriber count. This pins the honest status
/// choice: success means "the server processed our publish commands and the
/// flush round trip completed", never "N subscribers received it".
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

/// Payloads above the server-advertised maximum are a definitive pre-send
/// failure (`NotSent`), never an optimistic send that the server drops.
#[test]
fn oversized_payloads_are_rejected_before_publishing() {
    assert!(check_payload_size("events", 1024, 1024).is_ok());
    let error = not_sent(check_payload_size("events", 1025, 1024).unwrap_err());
    assert!(error.contains("exceeds the server maximum"), "{error}");
}
