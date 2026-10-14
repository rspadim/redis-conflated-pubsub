//! Subject validation is shared by `--check-config` and the NATS sink.
//!
//! The adapter re-validates the mapped publish subject per message because a
//! Redis-style `channel_prefix`/`channel_suffix` mapping can otherwise turn a
//! subscription subject into something Core NATS cannot accept (a wildcard, a
//! trailing dot, or whitespace). These tests pin the exact acceptance rules.

use crate::config::{validate_nats_publish_subject, validate_nats_subject};

#[test]
fn subscription_validation_accepts_exact_and_native_wildcards() {
    for subject in [
        "events",
        "events.orders",
        "*",
        "events.*",
        "events.>",
        ">",
        "a.*.b",
    ] {
        assert!(
            validate_nats_subject("subject", subject).is_ok(),
            "expected {subject:?} to be a valid subscription filter"
        );
    }
}

#[test]
fn publish_validation_rejects_wildcards_that_subscriptions_accept() {
    for subject in ["events", "events.orders", "a.b.c", "_INBOX.abc"] {
        assert!(
            validate_nats_publish_subject("subject", subject).is_ok(),
            "expected {subject:?} to be a valid concrete publish subject"
        );
    }
    for subject in ["*", "events.*", "events.>", ">"] {
        let error = validate_nats_publish_subject("subject", subject).unwrap_err();
        assert!(
            error.to_string().contains("concrete subject"),
            "{subject:?}: {error}"
        );
    }
}

#[test]
fn both_validators_share_the_malformed_subject_rules() {
    for (subject, needle) in [
        ("", "must not be empty"),
        ("events orders", "must not contain whitespace"),
        ("events..orders", "empty tokens"),
        (".events", "empty tokens"),
        ("events.", "empty tokens"),
        ("ev*ents", "full token"),
        ("events.ord>ers", "full token"),
    ] {
        for validator in [
            validate_nats_subject as fn(&str, &str) -> anyhow::Result<()>,
            validate_nats_publish_subject as fn(&str, &str) -> anyhow::Result<()>,
        ] {
            let error = validator("subject", subject).unwrap_err();
            assert!(
                error.to_string().contains(needle),
                "{subject:?}: expected {needle:?}, got {error}"
            );
        }
    }

    // A misplaced `>` is only reachable through the subscription validator: the
    // publish validator rejects `>` outright before checking its position.
    let error = validate_nats_subject("subject", "events.>.orders").unwrap_err();
    assert!(
        error.to_string().contains("`>` must be the final token"),
        "{error}"
    );
}
