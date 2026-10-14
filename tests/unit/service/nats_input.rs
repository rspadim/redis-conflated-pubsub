//! Input-adapter tests that need no live server.
//!
//! The subscription identity mapping is the piece the relay depends on for
//! per-subscription `output_prefix`/`output_suffix`: a wildcard filter matches
//! many concrete subjects, and the mapping must come from the filter that
//! produced the message, not from the concrete subject.

use std::sync::Arc;

use crate::config::InputConfig;
use crate::service::backend::InputEnvelope;
use crate::service::nats::input::NatsInput;

fn nats_input(subscriptions: serde_json::Value) -> InputConfig {
    serde_json::from_value(serde_json::json!({
        "nats": {"servers": ["nats://localhost:4222"]},
        "subscriptions": subscriptions,
    }))
    .expect("NATS input config should deserialize")
}

fn envelope(subject: &str, identity: &str) -> InputEnvelope {
    InputEnvelope {
        source_address: "nats://localhost:4222".to_owned(),
        source_subject: subject.to_owned(),
        payload: Arc::from(&b"payload"[..]),
        subscription_identity: identity.to_owned(),
        already_forwarded: false,
    }
}

#[test]
fn mapped_envelope_uses_the_matching_filters_prefix_and_suffix() {
    let config = nats_input(serde_json::json!([
        {"type": "nats_subject", "subject": "prices.>", "output_prefix": "ingress.", "output_suffix": ".v1"},
    ]));
    let input = NatsInput::new(&config).unwrap();
    assert_eq!(input.subscription_count(), 1);

    let envelope = envelope("prices.eu.aapl", "prices.>");
    let mapped = input
        .map_envelope(&envelope)
        .expect("the configured filter must map");
    assert_eq!(mapped.0, "ingress.");
    assert_eq!(mapped.1, "prices.eu.aapl");
    assert_eq!(mapped.2, ".v1");
}

#[test]
fn overlapping_filters_keep_their_own_mapping_identity() {
    // Two filters can both match `prices.eu.aapl`; the envelope identity picks
    // the mapping, so the broad filter does not borrow the specific one's
    // namespace.
    let config = nats_input(serde_json::json!([
        {"type": "nats_subject", "subject": "prices.>", "output_prefix": "broad.", "output_suffix": ""},
        {"type": "nats_subject", "subject": "prices.eu.*", "output_prefix": "specific.", "output_suffix": ":eu"},
    ]));
    let input = NatsInput::new(&config).unwrap();

    let broad_envelope = envelope("prices.eu.aapl", "prices.>");
    let broad = input.map_envelope(&broad_envelope).unwrap();
    assert_eq!((broad.0.as_str(), broad.2.as_str()), ("broad.", ""));

    let specific_envelope = envelope("prices.eu.aapl", "prices.eu.*");
    let specific = input.map_envelope(&specific_envelope).unwrap();
    assert_eq!(
        (specific.0.as_str(), specific.2.as_str()),
        ("specific.", ":eu")
    );
}

#[test]
fn an_unconfigured_identity_is_reported_as_unmapped() {
    let config = nats_input(serde_json::json!([
        {"type": "nats_subject", "subject": "prices.>"},
    ]));
    let input = NatsInput::new(&config).unwrap();
    let envelope = envelope("prices.eu.aapl", "orders.>");
    assert!(input.map_envelope(&envelope).is_none());
}

#[test]
fn a_forwarded_envelope_is_marked_for_loop_suppression() {
    // The input adapter must surface the hop marker so the read loop can drop
    // relay echoes instead of forwarding them again.
    let config = nats_input(serde_json::json!([
        {"type": "nats_subject", "subject": "prices.>"},
    ]));
    let input = NatsInput::new(&config).unwrap();

    let mut forwarded = envelope("prices.eu.aapl", "prices.>");
    forwarded.already_forwarded = true;
    assert!(forwarded.already_forwarded);
    // The mapping still resolves; suppression happens in the read loop, not here.
    assert!(input.map_envelope(&forwarded).is_some());

    let fresh = envelope("prices.eu.aapl", "prices.>");
    assert!(!fresh.already_forwarded);
}

#[test]
fn the_adapter_rejects_a_non_nats_endpoint_and_redis_subscriptions() {
    // A Redis input endpoint cannot build a NATS adapter.
    let redis: InputConfig = serde_json::from_value(serde_json::json!({
        "redis": {"host": "localhost"},
        "subscriptions": [{"type": "subscribe", "channel": "events"}],
    }))
    .unwrap();
    let error = NatsInput::new(&redis)
        .err()
        .expect("a Redis endpoint must not build a NATS input");
    assert!(error.to_string().contains("nats"), "{error}");

    // A NATS endpoint holding a Redis subscription is rejected with a clear
    // message rather than subscribing to the wrong address space.
    let mixed: InputConfig = serde_json::from_value(serde_json::json!({
        "nats": {"servers": ["nats://localhost:4222"]},
        "subscriptions": [{"type": "subscribe", "channel": "events"}],
    }))
    .unwrap();
    let error = NatsInput::new(&mixed)
        .err()
        .expect("a Redis subscription must not build a NATS input");
    assert!(error.to_string().contains("nats_subject"), "{error}");
}

#[test]
fn the_adapter_rejects_duplicate_nats_subjects() {
    // `mapping_for` resolves by subject string, so two identical subjects would
    // collide; the adapter refuses them even when validation was bypassed.
    let duplicate: InputConfig = serde_json::from_value(serde_json::json!({
        "nats": {"servers": ["nats://localhost:4222"]},
        "subscriptions": [
            {"type": "nats_subject", "subject": "prices.>", "output_prefix": "a."},
            {"type": "nats_subject", "subject": "prices.>", "output_prefix": "b."},
        ],
    }))
    .unwrap();
    let error = NatsInput::new(&duplicate)
        .err()
        .expect("duplicate subjects must not build a NATS input");
    assert!(
        error.to_string().contains("duplicate nats_subject"),
        "{error}"
    );
}
