use std::time::Duration;

use redis::aio::MultiplexedConnection;
use tokio::time;
use tracing::warn;

use crate::config::OutputConfig;

use super::backend::OutputSink;
use super::nats::output::NatsOutputSink;
use super::{OutputPublishContext, PendingMessage};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum PublishFailure {
    NotSent(String),
    Uncertain(String),
}

const FAILURE_LOG_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Default)]
pub(super) struct OutputFailureLog {
    last_report: Option<time::Instant>,
    suppressed_failures: u64,
    suppressed_messages: u64,
}

impl OutputFailureLog {
    fn report(&mut self, output: &str, error: &str, messages: usize) {
        let now = time::Instant::now();
        if self
            .last_report
            .is_none_or(|last| now.duration_since(last) >= FAILURE_LOG_INTERVAL)
        {
            warn!(
                output,
                error,
                messages,
                suppressed_failures = self.suppressed_failures,
                suppressed_messages = self.suppressed_messages,
                "output_publish_failed"
            );
            self.last_report = Some(now);
            self.suppressed_failures = 0;
            self.suppressed_messages = 0;
        } else {
            self.suppressed_failures = self.suppressed_failures.saturating_add(1);
            self.suppressed_messages = self.suppressed_messages.saturating_add(messages as u64);
        }
    }

    pub(super) fn flush_suppressed(&mut self, output: &str) {
        if self.suppressed_failures > 0 {
            warn!(
                output,
                suppressed_failures = self.suppressed_failures,
                suppressed_messages = self.suppressed_messages,
                "output_publish_failures_suppressed"
            );
            self.suppressed_failures = 0;
            self.suppressed_messages = 0;
        }
    }
}

/// Redis output sink owning the multiplexed connection. Implements the shared
/// [`OutputSink`] boundary so the conflated and direct lanes can drive either
/// backend through one interface.
pub(crate) struct RedisOutputSink {
    config: OutputConfig,
    client: redis::Client,
    connection: Option<MultiplexedConnection>,
    /// Redis `CLIENT SETNAME` target, also carried in `OutputRuntimeSetup` for
    /// the runtime record; the sink is the value that actually applies it.
    client_name: Option<String>,
}

impl RedisOutputSink {
    pub(crate) fn new(
        config: &OutputConfig,
        client: redis::Client,
        client_name: Option<String>,
    ) -> Self {
        Self {
            config: config.clone(),
            client,
            connection: None,
            client_name,
        }
    }

    /// `true` once a connection has been established and not yet discarded.
    pub(super) fn is_connected(&self) -> bool {
        self.connection.is_some()
    }

    /// Drops the connection so the next publish reconnects.
    pub(crate) fn disconnect(&mut self) {
        self.connection = None;
    }

    /// Clone of the multiplexed connection for an in-flight direct batch, or
    /// `None` when the sink is currently disconnected.
    pub(super) fn connection(&self) -> Option<MultiplexedConnection> {
        self.connection.clone()
    }

    pub(crate) async fn ensure_connected(&mut self) -> std::result::Result<(), PublishFailure> {
        if self.connection.is_some() {
            return Ok(());
        }
        ensure_output_connection(
            &self.config,
            &self.client,
            &mut self.connection,
            self.client_name.as_deref(),
        )
        .await
    }
}

impl OutputSink for RedisOutputSink {
    async fn publish(
        &mut self,
        messages: &[PendingMessage],
        atomic: bool,
    ) -> std::result::Result<i64, PublishFailure> {
        publish_batch(
            &self.config,
            &self.client,
            &mut self.connection,
            messages,
            atomic,
            self.client_name.as_deref(),
        )
        .await
    }

    /// Redis settles per batch through the pipeline replies; there is no
    /// additional server-side barrier to await.
    async fn flush(&mut self) -> std::result::Result<(), PublishFailure> {
        Ok(())
    }
}

/// A connected output backend whose connection state the worker owns.
///
/// The Redis variant is larger because it owns a clone of the `OutputConfig`
/// needed for its byte/oversized policy; exactly one variant exists per output
/// for the process lifetime, so the size difference is not worth an indirection.
#[allow(clippy::large_enum_variant)]
pub(crate) enum OutputConnection {
    Redis(RedisOutputSink),
    Nats(NatsOutputSink),
}

impl OutputConnection {
    pub(crate) fn is_connected(&self) -> bool {
        match self {
            Self::Redis(sink) => sink.is_connected(),
            Self::Nats(sink) => sink.is_connected(),
        }
    }

    pub(crate) fn disconnect(&mut self) {
        match self {
            Self::Redis(sink) => sink.disconnect(),
            Self::Nats(sink) => sink.disconnect(),
        }
    }

    /// Reconnects every backend through its own sink. Not currently called by
    /// the worker (which performs the connect inline), kept for the shared
    /// boundary and future direct-lane use.
    #[allow(dead_code)]
    pub(crate) async fn ensure_connected(&mut self) -> std::result::Result<(), PublishFailure> {
        match self {
            Self::Redis(sink) => sink.ensure_connected().await,
            Self::Nats(sink) => sink.ensure_connected().await,
        }
    }
}

impl OutputSink for OutputConnection {
    async fn publish(
        &mut self,
        messages: &[PendingMessage],
        atomic: bool,
    ) -> std::result::Result<i64, PublishFailure> {
        match self {
            Self::Redis(sink) => sink.publish(messages, atomic).await,
            Self::Nats(sink) => sink.publish(messages, atomic).await,
        }
    }

    async fn flush(&mut self) -> std::result::Result<(), PublishFailure> {
        match self {
            Self::Redis(sink) => OutputSink::flush(sink).await,
            Self::Nats(sink) => OutputSink::flush(sink).await,
        }
    }
}

/// Publishes one chunk through any sink and settles it through the backend's
/// flush barrier. The subscriber count is backend-specific: Redis returns the
/// summed `PUBLISH` replies, Core NATS returns `0` because it has no
/// acknowledgement or subscriber count (`flush` is a server round-trip, not a
/// delivery guarantee).
pub(super) async fn publish_once<S: OutputSink>(
    publisher: &mut S,
    batch: &[PendingMessage],
    atomic: bool,
    context: &mut OutputPublishContext<'_>,
) -> std::result::Result<i64, PublishFailure> {
    let started_at = time::Instant::now();
    let result = match publisher.publish(batch, atomic).await {
        Ok(subscribers) => publisher.flush().await.map(|()| subscribers),
        Err(failure) => Err(failure),
    };
    context
        .metrics
        .record_output_publish_rtt(context.output_metrics, started_at.elapsed());
    settle_publish_result(batch, result, context)
}

/// Publishes an ordered chunk and flushes it without touching metrics, for the
/// in-flight direct lane which records its own RTT and settles on completion.
pub(super) async fn publish_chunk_and_flush<S: OutputSink>(
    publisher: &mut S,
    batch: &[PendingMessage],
) -> std::result::Result<i64, PublishFailure> {
    match publisher.publish(batch, false).await {
        Ok(subscribers) => publisher.flush().await.map(|()| subscribers),
        Err(failure) => Err(failure),
    }
}

pub(super) fn settle_publish_result(
    batch: &[PendingMessage],
    result: std::result::Result<i64, PublishFailure>,
    context: &mut OutputPublishContext<'_>,
) -> std::result::Result<i64, PublishFailure> {
    match result {
        Ok(subscribers) => {
            let evictions = context
                .deduplication_cache
                .remember(batch, time::Instant::now());
            context
                .metrics
                .record_deduplication_evictions(context.output_metrics, evictions);
            Ok(subscribers)
        }
        Err(failure) => {
            if matches!(&failure, PublishFailure::Uncertain(_)) {
                let evictions = context
                    .deduplication_cache
                    .remember(batch, time::Instant::now());
                context
                    .metrics
                    .record_deduplication_evictions(context.output_metrics, evictions);
            }
            match &failure {
                PublishFailure::NotSent(error) => {
                    context
                        .metrics
                        .record_output_error(context.output_metrics, error, true);
                    context
                        .metrics
                        .record_output_error_messages(context.output_metrics, batch.len());
                    context.failure_log.report(context.name, error, batch.len());
                }
                PublishFailure::Uncertain(error) => {
                    context.metrics.record_output_uncertain(
                        context.output_metrics,
                        error,
                        batch.len(),
                    );
                    context.failure_log.report(context.name, error, batch.len());
                }
            }
            Err(failure)
        }
    }
}

async fn publish_batch(
    config: &OutputConfig,
    client: &redis::Client,
    connection: &mut Option<MultiplexedConnection>,
    messages: &[PendingMessage],
    atomic: bool,
    client_name: Option<&str>,
) -> std::result::Result<i64, PublishFailure> {
    if messages.is_empty() {
        return Err(PublishFailure::NotSent(
            "output publish batch is empty".to_owned(),
        ));
    }
    ensure_output_connection(config, client, connection, client_name).await?;
    let mut publish_connection = connection
        .as_ref()
        .expect("output connection was initialized")
        .clone();
    let result = publish_on_connection(config, &mut publish_connection, messages, atomic).await;
    if result.is_err() {
        *connection = None;
    }
    result
}

pub(super) async fn ensure_output_connection(
    config: &OutputConfig,
    client: &redis::Client,
    connection: &mut Option<MultiplexedConnection>,
    client_name: Option<&str>,
) -> std::result::Result<(), PublishFailure> {
    if connection.is_some() {
        return Ok(());
    }
    let mut connected = match time::timeout(
        config.redis_endpoint().connect_timeout(),
        client.get_multiplexed_async_connection(),
    )
    .await
    {
        Ok(Ok(connection)) => connection,
        Ok(Err(error)) => return Err(PublishFailure::NotSent(error.to_string())),
        Err(_) => {
            return Err(PublishFailure::NotSent(
                "timed out connecting to output Redis".to_owned(),
            ));
        }
    };
    if let Some(name) = client_name {
        // Naming is best-effort: an ACL may forbid CLIENT SETNAME.
        let result: redis::RedisResult<()> = redis::cmd("CLIENT")
            .arg("SETNAME")
            .arg(name)
            .query_async(&mut connected)
            .await;
        if let Err(error) = result {
            warn!(client_name = name, error = %error, "output_client_setname_failed");
        }
    }
    *connection = Some(connected);
    Ok(())
}

pub(super) async fn publish_batch_on_connection(
    config: &OutputConfig,
    mut connection: MultiplexedConnection,
    messages: Vec<PendingMessage>,
    atomic: bool,
) -> (
    Vec<PendingMessage>,
    std::result::Result<i64, PublishFailure>,
    Duration,
) {
    let started_at = time::Instant::now();
    let result = publish_on_connection(config, &mut connection, &messages, atomic).await;
    (messages, result, started_at.elapsed())
}

async fn publish_on_connection(
    config: &OutputConfig,
    connection: &mut MultiplexedConnection,
    messages: &[PendingMessage],
    atomic: bool,
) -> std::result::Result<i64, PublishFailure> {
    if messages.is_empty() {
        return Err(PublishFailure::NotSent(
            "output publish batch is empty".to_owned(),
        ));
    }
    let result = time::timeout(config.redis_endpoint().connect_timeout(), async {
        if atomic {
            let mut pipeline = redis::pipe();
            pipeline.atomic();
            for message in messages {
                pipeline
                    .cmd("PUBLISH")
                    .arg(&message.output_channel)
                    .arg(&message.payload);
            }
            pipeline
                .query_async::<Vec<i64>>(connection)
                .await
                .map(|counts| counts.into_iter().sum::<i64>())
        } else {
            let message = &messages[0];
            redis::cmd("PUBLISH")
                .arg(&message.output_channel)
                .arg(&message.payload)
                .query_async::<i64>(connection)
                .await
        }
    })
    .await;

    match result {
        Ok(Ok(subscribers)) => Ok(subscribers),
        Ok(Err(error)) => Err(PublishFailure::Uncertain(format!(
            "Redis returned an error after a publish command was sent: {error}"
        ))),
        Err(_) => Err(PublishFailure::Uncertain(
            "timed out waiting for the Redis publish response".to_owned(),
        )),
    }
}
