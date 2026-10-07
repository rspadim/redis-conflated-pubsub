use std::time::Duration;

use redis::aio::MultiplexedConnection;
use tokio::time;
use tracing::warn;

use crate::config::OutputConfig;

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

pub(super) trait BatchPublisher {
    async fn publish(
        &mut self,
        messages: &[PendingMessage],
        atomic: bool,
    ) -> std::result::Result<i64, PublishFailure>;
}

pub(super) struct RedisBatchPublisher<'a> {
    pub(super) config: &'a OutputConfig,
    pub(super) client: &'a redis::Client,
    pub(super) connection: &'a mut Option<MultiplexedConnection>,
}

impl BatchPublisher for RedisBatchPublisher<'_> {
    async fn publish(
        &mut self,
        messages: &[PendingMessage],
        atomic: bool,
    ) -> std::result::Result<i64, PublishFailure> {
        publish_batch(self.config, self.client, self.connection, messages, atomic).await
    }
}

pub(super) async fn publish_once<P: BatchPublisher>(
    publisher: &mut P,
    batch: &[PendingMessage],
    atomic: bool,
    context: &mut OutputPublishContext<'_>,
) -> std::result::Result<i64, PublishFailure> {
    match publisher.publish(batch, atomic).await {
        Ok(subscribers) => {
            context
                .deduplication_cache
                .remember(batch, time::Instant::now());
            Ok(subscribers)
        }
        Err(failure) => {
            if matches!(&failure, PublishFailure::Uncertain(_)) {
                context
                    .deduplication_cache
                    .remember(batch, time::Instant::now());
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
) -> std::result::Result<i64, PublishFailure> {
    if messages.is_empty() {
        return Err(PublishFailure::NotSent(
            "output publish batch is empty".to_owned(),
        ));
    }
    if connection.is_none() {
        let connected = match time::timeout(
            config.redis.connect_timeout(),
            client.get_multiplexed_async_connection(),
        )
        .await
        {
            Ok(Ok(connection)) => connection,
            Ok(Err(error)) => {
                return Err(PublishFailure::NotSent(error.to_string()));
            }
            Err(_) => {
                return Err(PublishFailure::NotSent(
                    "timed out connecting to output Redis".to_owned(),
                ));
            }
        };
        *connection = Some(connected);
    }

    let result = time::timeout(config.redis.connect_timeout(), async {
        let connection = connection.as_mut().expect("connection was initialized");
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
        Ok(Err(error)) => {
            *connection = None;
            Err(PublishFailure::Uncertain(format!(
                "Redis returned an error after a publish command was sent: {error}"
            )))
        }
        Err(_) => {
            *connection = None;
            Err(PublishFailure::Uncertain(
                "timed out waiting for the Redis publish response".to_owned(),
            ))
        }
    }
}
