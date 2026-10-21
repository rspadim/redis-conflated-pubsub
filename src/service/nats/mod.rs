//! Core NATS backend adapter: shared connection setup plus the input and output
//! implementations of the service backend boundary.
//!
//! Module layout:
//! * [`connect`] turns a `NatsConfig` into a connected `async_nats::Client`,
//!   with a pure configuration-to-plan mapping tests can assert on.
//! * [`input`] implements `InputSource` over `async_nats` subscriptions.
//! * [`output`] implements `OutputSink` with ordered publish + `flush`.
pub(in crate::service) mod connect;

/// Proves a NATS backend configuration by connecting once and dropping it.
pub(crate) use connect::test_connection;
pub(in crate::service) mod input;
pub(in crate::service) mod output;
