//! NATS lifecycle tests that need no live server.
//!
//! These pin the state machine that the service lifecycle depends on:
//!
//! * a freshly built sink is disconnected, so the first publish connects;
//! * a failed connection/publish leaves the sink disconnected so the next
//!   publish reconnects, for both the conflated lane (which calls
//!   `OutputSink::publish`/`flush` directly) and the direct lane (which runs its
//!   own in-flight future through a cloned handle);
//! * a shutdown flush on a disconnected sink is a cheap no-op;
//! * a client publish failure maps to the same `NotSent`/`Uncertain` vocabulary
//!   the Redis path uses.
//!
//! A live server is the only way to prove an actual byte-level reconnect, so
//! that remains a Compose E2E; these tests prove the intent and mapping.

use std::time::Duration;

use crate::config::OutputConfig;
use crate::service::backend::{InputSource, OutputSink};
use crate::service::nats::input::NatsInput;
use crate::service::nats::output::{
    NATS_NO_SUBSCRIBER_COUNT, NatsOutputSink, PublishStep, classify_publish_step,
};
use crate::service::publish::{OutputConnection, PublishFailure};
use async_nats::PublishErrorKind;

use super::*;
use crate::service::INITIAL_RETRY_DELAY;
use crate::service::next_retry_delay;

/// A NATS output pointed at a port nothing listens on. Binding an ephemeral
/// port and dropping the listener yields a port that is (almost certainly)
/// closed, so the initial connect is a fast connection refusal. The relay does
/// not enable `retry_on_initial_connect`, so `async-nats` makes a single
/// seed-pool pass and returns instead of retrying forever.
fn unreachable_output_config() -> OutputConfig {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    serde_json::from_value(serde_json::json!({
        "nats": {
            "servers": [format!("nats://127.0.0.1:{port}")],
            "connect_timeout_ms": 250,
        },
        "conflation": { "interval_ms": 0 },
    }))
    .expect("NATS output config should deserialize")
}

#[test]
fn a_freshly_built_nats_sink_is_disconnected() {
    let sink = NatsOutputSink::new(&unreachable_output_config()).unwrap();
    assert!(!sink.is_connected());
    assert!(sink.sender().is_none());
}

#[test]
fn disconnect_is_idempotent_on_a_disconnected_sink() {
    let mut sink = NatsOutputSink::new(&unreachable_output_config()).unwrap();
    sink.disconnect();
    sink.disconnect();
    assert!(!sink.is_connected());
}

/// A failed connect through the sink's `publish` leaves the sink disconnected,
/// so the next publish attempts a fresh connection. This is the exact path the
/// conflated lane takes: it calls `OutputSink::publish` directly and never calls
/// `ensure_connected` itself.
#[tokio::test]
async fn conflated_lane_reconnect_intent_survives_a_connect_failure() {
    let mut connection =
        OutputConnection::Nats(NatsOutputSink::new(&unreachable_output_config()).unwrap());
    assert!(!connection.is_connected());

    let batch = vec![pending_message("events.a", &b"payload"[..])];

    // First attempt: connect fails before anything is sent, so it is NotSent.
    let first = OutputSink::publish(&mut connection, &batch, true).await;
    assert!(
        matches!(first, Err(PublishFailure::NotSent(_))),
        "a connect failure must be a definitive NotSent, got {first:?}"
    );
    // The sink must not keep a broken client: the next publish reconnects.
    assert!(!connection.is_connected());

    // Second attempt takes the same reconnect path rather than reporting a
    // permanently dead sink.
    let second = OutputSink::publish(&mut connection, &batch, true).await;
    assert!(
        matches!(second, Err(PublishFailure::NotSent(_))),
        "the retry must attempt a reconnect, got {second:?}"
    );
    assert!(!connection.is_connected());
}

/// The direct lane clones a connected handle into its in-flight future. When
/// the sink is disconnected there is no handle to clone, which is what forces
/// the worker to run `ensure_connected` before dispatching again.
#[test]
fn direct_lane_has_no_handle_while_disconnected() {
    let mut sink = NatsOutputSink::new(&unreachable_output_config()).unwrap();
    assert!(
        sink.sender().is_none(),
        "a disconnected sink must not hand out a publish handle"
    );
    sink.disconnect();
    assert!(sink.sender().is_none());
}

/// A flush on a disconnected sink settles nothing and must not fail the worker
/// or force a connection; the next publish is what reconnects.
#[tokio::test]
async fn flush_on_a_disconnected_sink_is_a_noop() {
    let mut sink = NatsOutputSink::new(&unreachable_output_config()).unwrap();
    assert!(OutputSink::flush(&mut sink).await.is_ok());
    // The same holds through the shutdown path.
    sink.flush_before_shutdown().await;
    assert!(!sink.is_connected());
}

/// A publish failure is discarded at the sink layer before it is classified, so
/// the shutdown flush must tolerate a disconnected sink without opening a new
/// connection.
#[tokio::test]
async fn shutdown_flush_after_failed_publish_is_a_noop() {
    let mut connection =
        OutputConnection::Nats(NatsOutputSink::new(&unreachable_output_config()).unwrap());
    let batch = vec![pending_message("events.a", &b"payload"[..])];
    let _ = OutputSink::publish(&mut connection, &batch, false).await;
    connection.flush_before_shutdown().await;
    assert!(!connection.is_connected());
}

#[test]
fn prefixed_and_oversized_subject_failures_are_definitive() {
    for (kind, expected_not_sent) in [
        (PublishErrorKind::InvalidSubject, true),
        (PublishErrorKind::MaxPayloadExceeded, true),
        (PublishErrorKind::Send, false),
    ] {
        let step = classify_publish_step("archive.prices.a.v1", kind, "some client error");
        match (step, expected_not_sent) {
            (PublishStep::NotSent(message), true) => {
                assert!(message.contains("archive.prices.a.v1"), "{message}");
            }
            (PublishStep::Uncertain(message), false) => {
                assert!(message.contains("archive.prices.a.v1"), "{message}");
            }
            (step, _) => panic!("unexpected classification for {kind:?}: {step:?}"),
        }
    }
}

/// The NATS sink never claims a subscriber count, including at the boundary
/// enum the worker drives.
#[test]
fn nats_subscriber_count_stays_zero() {
    assert_eq!(NATS_NO_SUBSCRIBER_COUNT, 0);
}

/// `async-nats` reports the connect timeout the relay configures; a tiny
/// timeout must not be silently ignored.
#[test]
fn connection_plan_carries_the_configured_connect_timeout() {
    let plan = crate::service::nats::connect::connection_plan(
        unreachable_output_config().nats.as_ref().unwrap(),
    )
    .unwrap();
    assert_eq!(plan.connect_timeout, Duration::from_millis(250));
}

/// The input reconnect backoff is shared with the Redis reader: start short,
/// double, and cap. This is the schedule `read_nats_input` uses between failed
/// or dropped connections.
#[test]
fn reconnect_backoff_doubles_and_caps_at_the_shared_maximum() {
    let mut delay = INITIAL_RETRY_DELAY;
    assert_eq!(delay, Duration::from_millis(250));
    let mut seen = Vec::new();
    for _ in 0..8 {
        delay = next_retry_delay(delay);
        seen.push(delay);
    }
    assert_eq!(seen[0], Duration::from_millis(500));
    assert_eq!(seen[1], Duration::from_millis(1000));
    assert_eq!(*seen.last().unwrap(), Duration::from_secs(30));
    // Once capped it stays capped.
    assert_eq!(
        next_retry_delay(Duration::from_secs(30)),
        Duration::from_secs(30)
    );
}

fn nats_input_config() -> crate::config::InputConfig {
    serde_json::from_value(serde_json::json!({
        "nats": {"servers": ["nats://127.0.0.1:4222"]},
        "subscriptions": [
            {"type": "nats_subject", "subject": "prices.>"},
            {"type": "nats_subject", "subject": "orders.*", "output_prefix": "o."},
        ],
    }))
    .expect("NATS input config should deserialize")
}

#[test]
fn a_freshly_built_nats_input_is_disconnected() {
    let input = NatsInput::new(&nats_input_config()).unwrap();
    assert!(!input.is_connected());
    assert_eq!(input.subscription_count(), 2);
}

/// Polling a never-connected (or force-disconnected) input must report
/// "reconnect" (`None`) rather than panic on a missing subscription stream. This
/// is the guard that keeps a partially failed connect from turning into a
/// worker panic.
#[tokio::test]
async fn polling_a_disconnected_input_reports_reconnect() {
    let mut input = NatsInput::new(&nats_input_config()).unwrap();
    assert!(input.next_envelope().await.unwrap().is_none());
    input.force_disconnect();
    assert!(input.next_envelope().await.unwrap().is_none());
    assert!(!input.is_connected());
}

/// A reload must be torn down and rebuilt with no `async-nats` subscription
/// leak. The adapter drops its client (and thus every `Subscriber`) on
/// `disconnect`, and a fresh `NatsInput` starts with no client and no streams,
/// so the rebuilt worker starts from a clean subscription set.
#[test]
fn a_rebuilt_input_starts_with_no_client_or_subscriptions() {
    let mut input = NatsInput::new(&nats_input_config()).unwrap();
    input.force_disconnect();
    assert!(!input.is_connected());

    let rebuilt = NatsInput::new(&nats_input_config()).unwrap();
    assert!(!rebuilt.is_connected());
    assert_eq!(rebuilt.subscription_count(), 2);
}

/// Reload (SIGHUP) from one valid config to another, and rejection of an
/// invalid file, both route through `load_reload_config`. A NATS config is the
/// new case: the backend preflight must accept it without opening a connection.
#[cfg(unix)]
mod reload {
    use super::*;
    use crate::service::{load_reload_config, validate_reload_backends};

    fn redis_example() -> AppConfig {
        serde_json::from_str(include_str!("../../../config.example.json"))
            .expect("example config should deserialize")
    }

    #[test]
    fn preflight_accepts_a_nats_config_without_connecting() {
        let config: AppConfig =
            serde_json::from_str(include_str!("../../../config.nats.example.json")).unwrap();
        config.validate().unwrap();
        validate_reload_backends(&config).unwrap();
    }

    #[test]
    fn reload_from_redis_to_nats_is_planned_and_invalid_files_are_rejected() {
        let current = redis_example();
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("config.json");

        // Redis -> NATS: the reload is planned (not executed) successfully.
        std::fs::write(&path, include_str!("../../../config.nats.example.json")).unwrap();
        let plan = load_reload_config(&current, &path).unwrap();
        assert!(plan.config.input.nats.is_some());
        assert!(plan.config.input.redis.is_none());

        // An invalid file leaves the current runtime running: it is rejected
        // before any tear-down.
        std::fs::write(&path, "{ this is not json").unwrap();
        assert!(load_reload_config(&current, &path).is_err());
    }

    #[test]
    fn preflight_resolves_redis_credentials_without_connecting() {
        let mut config = redis_example();
        // `validate()` does not resolve secrets; the preflight does, so a reload
        // pointing at a missing credential is rejected before tear-down instead
        // of half-starting.
        config
            .outputs
            .get_mut("output0")
            .unwrap()
            .redis
            .as_mut()
            .unwrap()
            .password_env = Some("RCPS_RELOAD_TEST_UNSET".to_owned());
        config.validate().unwrap();
        let error = validate_reload_backends(&config).unwrap_err();
        assert!(
            format!("{error:#}").contains("RCPS_RELOAD_TEST_UNSET"),
            "{error:#}"
        );
    }
}
