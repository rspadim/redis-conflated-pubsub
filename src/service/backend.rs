//! Backend boundary for the 0.3 input/output abstraction.
//!
//! This module defines the normalized message envelope and the two internal
//! traits that concrete Redis and Core NATS adapters implement. No backend I/O
//! lives here. The Redis input/output path predates this boundary and still
//! does not route through it, so the items are allowed to be unused by Redis.
#![allow(dead_code)]

use std::sync::Arc;

use super::PendingMessage;
use super::publish::PublishFailure;

/// Header a relay stamps on every message it forwards. A message that already
/// carries it is a loop and must not be forwarded again. Header-based means the
/// payload is never rewritten, so the relay stays byte-transparent.
pub(in crate::service) const NATS_HOP_HEADER: &str = "Relay-Hop";

/// Normalized inbound message envelope shared by every input backend.
///
/// The relay's filtering/conflation/deduplication path consumes this envelope
/// and stays backend-agnostic. `source_subject` is the concrete address the
/// message arrived on (Redis channel or NATS subject) and `subscription_identity`
/// is the identity of the subscription that matched: Redis reports the matching
/// channel (SUBSCRIBE) or pattern (PSUBSCRIBE), while a NATS subscription
/// reports the subject filter that produced the message. Keeping both is what
/// lets per-subscription output mappings survive overlapping filters, because a
/// wildcard filter (`prices.>`) matches many concrete subjects.
#[derive(Clone, Debug)]
pub(super) struct InputEnvelope {
    /// Backend address the message arrived from, for logs and metrics
    /// (for example `redis://host:6379` or `nats://host:4222`).
    pub(super) source_address: String,
    /// Concrete source address of the message (Redis channel or NATS subject).
    pub(super) source_subject: String,
    /// Raw payload bytes, shared through `Arc` where the backend allows it.
    pub(super) payload: Arc<[u8]>,
    /// Identity of the subscription that matched this message.
    pub(super) subscription_identity: String,
    /// `true` when the message already carries the relay hop header, meaning it
    /// was forwarded by this relay (or another) before. The input drops these to
    /// break NATS-to-NATS loops, because Core NATS `no_echo` only suppresses the
    /// publishing connection. Always `false` for Redis, which has no headers.
    pub(super) already_forwarded: bool,
}

/// An input backend: connect/reconnect, install the configured subscriptions,
/// and yield normalized envelopes in arrival order.
///
/// Implemented by the NATS input adapter (`service::nats::NatsInput`). The
/// Redis input path predates this boundary. `connect` must be idempotent enough
/// to be called again after a dropped stream establishes a fresh connection and
/// re-installs the same subscriptions.
pub(super) trait InputSource {
    /// Connects (or reconnects) and installs every configured subscription.
    async fn connect(&mut self) -> anyhow::Result<()>;

    /// Yields the next normalized message, or `None` when the source is closed
    /// and the caller should reconnect.
    async fn next_envelope(&mut self) -> anyhow::Result<Option<InputEnvelope>>;
}

/// An output backend: publish a bounded, ordered chunk and flush/settle it,
/// reporting the same `NotSent`/`Uncertain` outcomes the relay already handles.
///
/// `atomic` requests best-effort all-or-nothing where the backend supports it
/// (Redis `MULTI`/`EXEC`); Core NATS has no equivalent and may treat it as a
/// hint. `flush` is the backend's settlement barrier: Redis has none beyond the
/// pipeline replies, while NATS flushes its client buffer to the server.
pub(super) trait OutputSink {
    /// Publishes one ordered chunk. Returns the backend-specific completion
    /// signal where one exists. Redis returns the summed `PUBLISH` subscriber
    /// count; Core NATS has no acknowledgement or subscriber count, so the NATS
    /// sink returns `0` and `flush` carries the completion meaning.
    async fn publish(
        &mut self,
        chunk: &[PendingMessage],
        atomic: bool,
    ) -> std::result::Result<i64, PublishFailure>;

    /// Settles previously published chunks where the backend supports it.
    async fn flush(&mut self) -> std::result::Result<(), PublishFailure>;
}
