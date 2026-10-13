//! Backend boundary for the 0.3 input/output abstraction.
//!
//! This module is the frozen keystone for the NATS workstreams: it defines the
//! normalized message envelope and the two internal traits that concrete Redis
//! and Core NATS adapters will implement. No backend I/O lives here, and the
//! running Redis path is intentionally not rewired onto these traits yet, so the
//! items are allowed to be unused until the adapters land.
#![allow(dead_code)]

use std::sync::Arc;

use super::PendingMessage;
use super::publish::PublishFailure;

/// Normalized inbound message envelope shared by every input backend.
///
/// The relay's filtering/conflation/deduplication path consumes this envelope
/// and stays backend-agnostic. `subscription_identity` is required so that
/// per-subscription output mappings survive overlapping filters: Redis reports
/// the matching channel (SUBSCRIBE) or pattern (PSUBSCRIBE), while a NATS
/// subscription reports the subject filter that produced the message.
#[derive(Clone, Debug)]
pub(super) struct InputEnvelope {
    /// Backend address the message arrived from, for logs and metrics
    /// (for example `redis://host:6379` or `nats://host:4222`).
    pub(super) source_address: String,
    /// Raw payload bytes, shared through `Arc` where the backend allows it.
    pub(super) payload: Arc<[u8]>,
    /// Identity of the subscription that matched this message.
    pub(super) subscription_identity: String,
}

/// An input backend: connect/reconnect, install the configured subscriptions,
/// and yield normalized envelopes in arrival order.
///
/// Implemented later by the Redis and NATS input adapters. `connect` must be
/// idempotent enough to be called again after a dropped stream establishes a
/// fresh connection and re-installs the same subscriptions.
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
    /// signal where one exists (Redis subscriber count; Core NATS has none).
    async fn publish(
        &mut self,
        chunk: &[PendingMessage],
        atomic: bool,
    ) -> std::result::Result<i64, PublishFailure>;

    /// Settles previously published chunks where the backend supports it.
    async fn flush(&mut self) -> std::result::Result<(), PublishFailure>;
}
