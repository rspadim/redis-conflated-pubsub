use std::{
    collections::BTreeMap,
    fs,
    io::Write,
    path::Path,
    sync::atomic::{AtomicU64, Ordering},
    sync::{Arc, Mutex},
    time::Instant,
};

use anyhow::{Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::Serialize;
use tempfile::NamedTempFile;

#[derive(Debug)]
pub struct Metrics {
    started_at: DateTime<Utc>,
    started_clock: Instant,
    pub input_messages_total: AtomicU64,
    pub input_payload_bytes_total: AtomicU64,
    pub output_batches_total: AtomicU64,
    pub output_messages_total: AtomicU64,
    pub output_payload_bytes_total: AtomicU64,
    pub conflated_messages_total: AtomicU64,
    pub conflated_payload_bytes_total: AtomicU64,
    pub excluded_messages_total: AtomicU64,
    pub dropped_messages_total: AtomicU64,
    pub dropped_payload_bytes_total: AtomicU64,
    pub truncated_messages_total: AtomicU64,
    pub truncated_payload_bytes_total: AtomicU64,
    pub publish_errors_total: AtomicU64,
    pub publish_error_messages_total: AtomicU64,
    pub uncertain_transactions_total: AtomicU64,
    pub uncertain_messages_total: AtomicU64,
    pub input_reconnects_total: AtomicU64,
    pub output_reconnects_total: AtomicU64,
    pub pending_keys: AtomicU64,
    last_input_at: Mutex<Option<String>>,
    last_flush_at: Mutex<Option<String>>,
    last_error: Mutex<Option<String>>,
    state: Mutex<String>,
    outputs: Mutex<BTreeMap<String, Arc<OutputMetrics>>>,
}

#[derive(Debug)]
pub struct OutputMetrics {
    input_messages_total: AtomicU64,
    input_payload_bytes_total: AtomicU64,
    output_batches_total: AtomicU64,
    output_messages_total: AtomicU64,
    output_payload_bytes_total: AtomicU64,
    conflated_messages_total: AtomicU64,
    conflated_payload_bytes_total: AtomicU64,
    dropped_messages_total: AtomicU64,
    dropped_payload_bytes_total: AtomicU64,
    truncated_messages_total: AtomicU64,
    truncated_payload_bytes_total: AtomicU64,
    publish_errors_total: AtomicU64,
    publish_error_messages_total: AtomicU64,
    uncertain_transactions_total: AtomicU64,
    uncertain_messages_total: AtomicU64,
    reconnects_total: AtomicU64,
    pending_keys: AtomicU64,
    pending_messages: AtomicU64,
    pending_payload_bytes: AtomicU64,
    last_flush_at: Mutex<Option<String>>,
    last_error: Mutex<Option<String>>,
    state: Mutex<String>,
}

impl OutputMetrics {
    fn new() -> Self {
        Self {
            input_messages_total: AtomicU64::new(0),
            input_payload_bytes_total: AtomicU64::new(0),
            output_batches_total: AtomicU64::new(0),
            output_messages_total: AtomicU64::new(0),
            output_payload_bytes_total: AtomicU64::new(0),
            conflated_messages_total: AtomicU64::new(0),
            conflated_payload_bytes_total: AtomicU64::new(0),
            dropped_messages_total: AtomicU64::new(0),
            dropped_payload_bytes_total: AtomicU64::new(0),
            truncated_messages_total: AtomicU64::new(0),
            truncated_payload_bytes_total: AtomicU64::new(0),
            publish_errors_total: AtomicU64::new(0),
            publish_error_messages_total: AtomicU64::new(0),
            uncertain_transactions_total: AtomicU64::new(0),
            uncertain_messages_total: AtomicU64::new(0),
            reconnects_total: AtomicU64::new(0),
            pending_keys: AtomicU64::new(0),
            pending_messages: AtomicU64::new(0),
            pending_payload_bytes: AtomicU64::new(0),
            last_flush_at: Mutex::new(None),
            last_error: Mutex::new(None),
            state: Mutex::new("starting".to_owned()),
        }
    }

    fn snapshot(&self) -> OutputStatusSnapshot {
        let input_messages_total = self.input_messages_total.load(Ordering::Relaxed);
        let input_payload_bytes_total = self.input_payload_bytes_total.load(Ordering::Relaxed);
        let output_messages_total = self.output_messages_total.load(Ordering::Relaxed);
        let output_payload_bytes_total = self.output_payload_bytes_total.load(Ordering::Relaxed);
        OutputStatusSnapshot {
            state: self.state.lock().unwrap().clone(),
            input_messages_total,
            input_payload_bytes_total,
            output_batches_total: self.output_batches_total.load(Ordering::Relaxed),
            output_messages_total,
            output_payload_bytes_total,
            conflated_messages_total: self.conflated_messages_total.load(Ordering::Relaxed),
            conflated_payload_bytes_total: self
                .conflated_payload_bytes_total
                .load(Ordering::Relaxed),
            dropped_messages_total: self.dropped_messages_total.load(Ordering::Relaxed),
            dropped_payload_bytes_total: self.dropped_payload_bytes_total.load(Ordering::Relaxed),
            truncated_messages_total: self.truncated_messages_total.load(Ordering::Relaxed),
            truncated_payload_bytes_total: self
                .truncated_payload_bytes_total
                .load(Ordering::Relaxed),
            message_reduction_percent: reduction_percent(
                input_messages_total,
                output_messages_total,
            ),
            payload_reduction_percent: reduction_percent(
                input_payload_bytes_total,
                output_payload_bytes_total,
            ),
            publish_errors_total: self.publish_errors_total.load(Ordering::Relaxed),
            publish_error_messages_total: self.publish_error_messages_total.load(Ordering::Relaxed),
            uncertain_transactions_total: self.uncertain_transactions_total.load(Ordering::Relaxed),
            uncertain_messages_total: self.uncertain_messages_total.load(Ordering::Relaxed),
            reconnects_total: self.reconnects_total.load(Ordering::Relaxed),
            pending_keys: self.pending_keys.load(Ordering::Relaxed),
            pending_messages: self.pending_messages.load(Ordering::Relaxed),
            pending_payload_bytes: self.pending_payload_bytes.load(Ordering::Relaxed),
            last_flush_at: self.last_flush_at.lock().unwrap().clone(),
            last_error: self.last_error.lock().unwrap().clone(),
        }
    }
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            started_at: Utc::now(),
            started_clock: Instant::now(),
            input_messages_total: AtomicU64::new(0),
            input_payload_bytes_total: AtomicU64::new(0),
            output_batches_total: AtomicU64::new(0),
            output_messages_total: AtomicU64::new(0),
            output_payload_bytes_total: AtomicU64::new(0),
            conflated_messages_total: AtomicU64::new(0),
            conflated_payload_bytes_total: AtomicU64::new(0),
            excluded_messages_total: AtomicU64::new(0),
            dropped_messages_total: AtomicU64::new(0),
            dropped_payload_bytes_total: AtomicU64::new(0),
            truncated_messages_total: AtomicU64::new(0),
            truncated_payload_bytes_total: AtomicU64::new(0),
            publish_errors_total: AtomicU64::new(0),
            publish_error_messages_total: AtomicU64::new(0),
            uncertain_transactions_total: AtomicU64::new(0),
            uncertain_messages_total: AtomicU64::new(0),
            input_reconnects_total: AtomicU64::new(0),
            output_reconnects_total: AtomicU64::new(0),
            pending_keys: AtomicU64::new(0),
            last_input_at: Mutex::new(None),
            last_flush_at: Mutex::new(None),
            last_error: Mutex::new(None),
            state: Mutex::new("starting".to_owned()),
            outputs: Mutex::new(BTreeMap::new()),
        }
    }

    pub fn record_input(&self, payload_bytes: usize) {
        self.input_messages_total.fetch_add(1, Ordering::Relaxed);
        self.input_payload_bytes_total
            .fetch_add(payload_bytes as u64, Ordering::Relaxed);
        *self.last_input_at.lock().unwrap() = Some(timestamp());
    }

    pub fn record_flush(&self, message_count: usize) {
        self.output_batches_total.fetch_add(1, Ordering::Relaxed);
        self.output_messages_total
            .fetch_add(message_count as u64, Ordering::Relaxed);
        *self.last_flush_at.lock().unwrap() = Some(timestamp());
    }

    pub fn record_error(&self, error: impl ToString) {
        *self.last_error.lock().unwrap() = Some(error.to_string());
    }

    pub fn set_state(&self, state: &str) {
        *self.state.lock().unwrap() = state.to_owned();
    }

    pub fn register_output(&self, name: &str) -> Arc<OutputMetrics> {
        let output = Arc::new(OutputMetrics::new());
        self.outputs
            .lock()
            .unwrap()
            .insert(name.to_owned(), Arc::clone(&output));
        output
    }

    pub fn set_output_state(&self, output: &OutputMetrics, state: &str) {
        *output.state.lock().unwrap() = state.to_owned();
    }

    pub fn set_output_pending_keys(&self, output: &OutputMetrics, pending_keys: usize) {
        let pending_keys = pending_keys as u64;
        let previous = output.pending_keys.swap(pending_keys, Ordering::Relaxed);
        if pending_keys >= previous {
            self.pending_keys
                .fetch_add(pending_keys - previous, Ordering::Relaxed);
        } else {
            self.pending_keys
                .fetch_sub(previous - pending_keys, Ordering::Relaxed);
        }
    }

    pub fn record_output_input(&self, output: &OutputMetrics, payload_bytes: usize) {
        let payload_bytes = payload_bytes as u64;
        output.input_messages_total.fetch_add(1, Ordering::Relaxed);
        output
            .input_payload_bytes_total
            .fetch_add(payload_bytes, Ordering::Relaxed);
        output.pending_messages.fetch_add(1, Ordering::Relaxed);
        output
            .pending_payload_bytes
            .fetch_add(payload_bytes, Ordering::Relaxed);
    }

    pub fn rollback_output_input(&self, output: &OutputMetrics, payload_bytes: usize) {
        let payload_bytes = payload_bytes as u64;
        output.input_messages_total.fetch_sub(1, Ordering::Relaxed);
        output
            .input_payload_bytes_total
            .fetch_sub(payload_bytes, Ordering::Relaxed);
        output.pending_messages.fetch_sub(1, Ordering::Relaxed);
        output
            .pending_payload_bytes
            .fetch_sub(payload_bytes, Ordering::Relaxed);
    }

    pub fn record_output_conflated(&self, output: &OutputMetrics, payload_bytes: usize) {
        let payload_bytes = payload_bytes as u64;
        self.conflated_messages_total
            .fetch_add(1, Ordering::Relaxed);
        self.conflated_payload_bytes_total
            .fetch_add(payload_bytes, Ordering::Relaxed);
        output
            .conflated_messages_total
            .fetch_add(1, Ordering::Relaxed);
        output
            .conflated_payload_bytes_total
            .fetch_add(payload_bytes, Ordering::Relaxed);
        output.pending_messages.fetch_sub(1, Ordering::Relaxed);
        output
            .pending_payload_bytes
            .fetch_sub(payload_bytes, Ordering::Relaxed);
    }

    pub fn record_output_dropped(
        &self,
        output: &OutputMetrics,
        message_count: usize,
        payload_bytes: usize,
    ) {
        let payload_bytes = payload_bytes as u64;
        self.dropped_messages_total
            .fetch_add(message_count as u64, Ordering::Relaxed);
        self.dropped_payload_bytes_total
            .fetch_add(payload_bytes, Ordering::Relaxed);
        output
            .dropped_messages_total
            .fetch_add(message_count as u64, Ordering::Relaxed);
        output
            .dropped_payload_bytes_total
            .fetch_add(payload_bytes, Ordering::Relaxed);
        output
            .pending_messages
            .fetch_sub(message_count as u64, Ordering::Relaxed);
        output
            .pending_payload_bytes
            .fetch_sub(payload_bytes, Ordering::Relaxed);
    }

    pub fn record_output_abandoned(
        &self,
        output: &OutputMetrics,
        message_count: usize,
        payload_bytes: usize,
    ) {
        output
            .pending_messages
            .fetch_sub(message_count as u64, Ordering::Relaxed);
        output
            .pending_payload_bytes
            .fetch_sub(payload_bytes as u64, Ordering::Relaxed);
    }

    pub fn record_output_truncated(&self, output: &OutputMetrics, payload_bytes: usize) {
        let payload_bytes = payload_bytes as u64;
        if payload_bytes == 0 {
            return;
        }
        self.truncated_messages_total
            .fetch_add(1, Ordering::Relaxed);
        output
            .truncated_messages_total
            .fetch_add(1, Ordering::Relaxed);
        self.truncated_payload_bytes_total
            .fetch_add(payload_bytes, Ordering::Relaxed);
        output
            .truncated_payload_bytes_total
            .fetch_add(payload_bytes, Ordering::Relaxed);
        output
            .pending_payload_bytes
            .fetch_sub(payload_bytes, Ordering::Relaxed);
    }

    pub fn record_output_flush(
        &self,
        output: &OutputMetrics,
        message_count: usize,
        payload_bytes: usize,
    ) {
        let payload_bytes = payload_bytes as u64;
        self.record_flush(message_count);
        self.output_payload_bytes_total
            .fetch_add(payload_bytes, Ordering::Relaxed);
        output.output_batches_total.fetch_add(1, Ordering::Relaxed);
        output
            .output_messages_total
            .fetch_add(message_count as u64, Ordering::Relaxed);
        output
            .output_payload_bytes_total
            .fetch_add(payload_bytes, Ordering::Relaxed);
        output
            .pending_messages
            .fetch_sub(message_count as u64, Ordering::Relaxed);
        output
            .pending_payload_bytes
            .fetch_sub(payload_bytes, Ordering::Relaxed);
        *output.last_flush_at.lock().unwrap() = Some(timestamp());
    }

    pub fn record_output_error(
        &self,
        output: &OutputMetrics,
        error: impl ToString,
        reconnect: bool,
    ) {
        let error = error.to_string();
        self.publish_errors_total.fetch_add(1, Ordering::Relaxed);
        output.publish_errors_total.fetch_add(1, Ordering::Relaxed);
        if reconnect {
            self.output_reconnects_total.fetch_add(1, Ordering::Relaxed);
            output.reconnects_total.fetch_add(1, Ordering::Relaxed);
        }
        self.record_error(&error);
        *output.last_error.lock().unwrap() = Some(error);
    }

    pub fn record_output_error_messages(&self, output: &OutputMetrics, message_count: usize) {
        self.publish_error_messages_total
            .fetch_add(message_count as u64, Ordering::Relaxed);
        output
            .publish_error_messages_total
            .fetch_add(message_count as u64, Ordering::Relaxed);
    }

    pub fn record_output_uncertain(
        &self,
        output: &OutputMetrics,
        error: impl ToString,
        message_count: usize,
    ) {
        self.uncertain_transactions_total
            .fetch_add(1, Ordering::Relaxed);
        output
            .uncertain_transactions_total
            .fetch_add(1, Ordering::Relaxed);
        self.uncertain_messages_total
            .fetch_add(message_count as u64, Ordering::Relaxed);
        output
            .uncertain_messages_total
            .fetch_add(message_count as u64, Ordering::Relaxed);
        self.record_output_error_messages(output, message_count);
        self.record_output_error(output, error, true);
    }

    pub fn snapshot(&self) -> StatusSnapshot {
        StatusSnapshot {
            schema_version: 4,
            state: self.state.lock().unwrap().clone(),
            updated_at: timestamp(),
            started_at: self.started_at.to_rfc3339_opts(SecondsFormat::Millis, true),
            uptime_seconds: self.started_clock.elapsed().as_secs(),
            input_messages_total: self.input_messages_total.load(Ordering::Relaxed),
            input_payload_bytes_total: self.input_payload_bytes_total.load(Ordering::Relaxed),
            output_batches_total: self.output_batches_total.load(Ordering::Relaxed),
            output_messages_total: self.output_messages_total.load(Ordering::Relaxed),
            output_payload_bytes_total: self.output_payload_bytes_total.load(Ordering::Relaxed),
            conflated_messages_total: self.conflated_messages_total.load(Ordering::Relaxed),
            conflated_payload_bytes_total: self
                .conflated_payload_bytes_total
                .load(Ordering::Relaxed),
            excluded_messages_total: self.excluded_messages_total.load(Ordering::Relaxed),
            dropped_messages_total: self.dropped_messages_total.load(Ordering::Relaxed),
            dropped_payload_bytes_total: self.dropped_payload_bytes_total.load(Ordering::Relaxed),
            truncated_messages_total: self.truncated_messages_total.load(Ordering::Relaxed),
            truncated_payload_bytes_total: self
                .truncated_payload_bytes_total
                .load(Ordering::Relaxed),
            publish_errors_total: self.publish_errors_total.load(Ordering::Relaxed),
            publish_error_messages_total: self.publish_error_messages_total.load(Ordering::Relaxed),
            uncertain_transactions_total: self.uncertain_transactions_total.load(Ordering::Relaxed),
            uncertain_messages_total: self.uncertain_messages_total.load(Ordering::Relaxed),
            input_reconnects_total: self.input_reconnects_total.load(Ordering::Relaxed),
            output_reconnects_total: self.output_reconnects_total.load(Ordering::Relaxed),
            pending_keys: self.pending_keys.load(Ordering::Relaxed),
            last_input_at: self.last_input_at.lock().unwrap().clone(),
            last_flush_at: self.last_flush_at.lock().unwrap().clone(),
            last_error: self.last_error.lock().unwrap().clone(),
            outputs: self
                .outputs
                .lock()
                .unwrap()
                .iter()
                .map(|(name, output)| (name.clone(), output.snapshot()))
                .collect(),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct StatusSnapshot {
    pub schema_version: u8,
    pub state: String,
    pub updated_at: String,
    pub started_at: String,
    pub uptime_seconds: u64,
    pub input_messages_total: u64,
    pub input_payload_bytes_total: u64,
    pub output_batches_total: u64,
    pub output_messages_total: u64,
    pub output_payload_bytes_total: u64,
    pub conflated_messages_total: u64,
    pub conflated_payload_bytes_total: u64,
    pub excluded_messages_total: u64,
    pub dropped_messages_total: u64,
    pub dropped_payload_bytes_total: u64,
    pub truncated_messages_total: u64,
    pub truncated_payload_bytes_total: u64,
    pub publish_errors_total: u64,
    /// Number of messages in failed output operations.
    pub publish_error_messages_total: u64,
    /// Ambiguous output publish operations (EXEC chunks or individual PUBLISH commands).
    pub uncertain_transactions_total: u64,
    /// Number of messages whose publish outcome is ambiguous.
    pub uncertain_messages_total: u64,
    pub input_reconnects_total: u64,
    pub output_reconnects_total: u64,
    pub pending_keys: u64,
    pub last_input_at: Option<String>,
    pub last_flush_at: Option<String>,
    pub last_error: Option<String>,
    pub outputs: BTreeMap<String, OutputStatusSnapshot>,
}

#[derive(Debug, Serialize)]
pub struct OutputStatusSnapshot {
    pub state: String,
    pub input_messages_total: u64,
    pub input_payload_bytes_total: u64,
    pub output_batches_total: u64,
    pub output_messages_total: u64,
    pub output_payload_bytes_total: u64,
    pub conflated_messages_total: u64,
    pub conflated_payload_bytes_total: u64,
    pub dropped_messages_total: u64,
    pub dropped_payload_bytes_total: u64,
    pub truncated_messages_total: u64,
    pub truncated_payload_bytes_total: u64,
    pub message_reduction_percent: Option<f64>,
    pub payload_reduction_percent: Option<f64>,
    pub publish_errors_total: u64,
    /// Number of messages in failed output operations.
    pub publish_error_messages_total: u64,
    /// Ambiguous output publish operations (EXEC chunks or individual PUBLISH commands).
    pub uncertain_transactions_total: u64,
    /// Number of messages whose publish outcome is ambiguous.
    pub uncertain_messages_total: u64,
    pub reconnects_total: u64,
    pub pending_keys: u64,
    /// Messages currently queued for output; given-up failed chunks are excluded.
    pub pending_messages: u64,
    pub pending_payload_bytes: u64,
    pub last_flush_at: Option<String>,
    pub last_error: Option<String>,
}

fn reduction_percent(input: u64, output: u64) -> Option<f64> {
    if input == 0 {
        return None;
    }

    Some((input.saturating_sub(output) as f64 / input as f64) * 100.0)
}

pub fn write_atomic(path: &Path, snapshot: &StatusSnapshot) -> Result<()> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create status directory {}", parent.display()))?;
    }
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let mut temporary = NamedTempFile::new_in(parent)?;
    serde_json::to_writer(&mut temporary, snapshot)?;
    temporary.write_all(b"\n")?;
    temporary
        .persist(path)
        .map_err(|error| error.error)
        .with_context(|| format!("failed to replace status file {}", path.display()))?;
    Ok(())
}

fn timestamp() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_file_is_replaced_with_a_complete_json_snapshot() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("status.json");
        let metrics = Metrics::new();
        metrics.set_state("running");
        metrics.record_input(7);

        write_atomic(&path, &metrics.snapshot()).unwrap();
        metrics.record_input(11);
        write_atomic(&path, &metrics.snapshot()).unwrap();

        let json: serde_json::Value = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
        assert_eq!(json["state"], "running");
        assert_eq!(json["schema_version"], 4);
        assert_eq!(json["input_messages_total"], 2);
        assert_eq!(json["input_payload_bytes_total"], 18);
    }

    #[test]
    fn status_snapshot_exposes_metrics_by_arbitrary_output_name() {
        let metrics = Metrics::new();
        let output = metrics.register_output("custom-output");
        metrics.set_output_state(&output, "running");
        metrics.record_output_input(&output, 4);
        metrics.record_output_input(&output, 6);
        metrics.record_output_conflated(&output, 4);
        metrics.record_output_flush(&output, 1, 6);
        metrics.set_output_pending_keys(&output, 0);

        let snapshot = serde_json::to_value(metrics.snapshot()).unwrap();

        assert_eq!(snapshot["outputs"]["custom-output"]["state"], "running");
        assert_eq!(
            snapshot["outputs"]["custom-output"]["output_messages_total"],
            1
        );
        assert_eq!(
            snapshot["outputs"]["custom-output"]["input_messages_total"],
            2
        );
        assert_eq!(
            snapshot["outputs"]["custom-output"]["input_payload_bytes_total"],
            10
        );
        assert_eq!(
            snapshot["outputs"]["custom-output"]["output_payload_bytes_total"],
            6
        );
        assert_eq!(
            snapshot["outputs"]["custom-output"]["conflated_messages_total"],
            1
        );
        assert_eq!(
            snapshot["outputs"]["custom-output"]["conflated_payload_bytes_total"],
            4
        );
        assert_eq!(
            snapshot["outputs"]["custom-output"]["message_reduction_percent"],
            50.0
        );
        assert_eq!(
            snapshot["outputs"]["custom-output"]["payload_reduction_percent"],
            40.0
        );
        assert_eq!(snapshot["outputs"]["custom-output"]["pending_messages"], 0);
        assert_eq!(
            snapshot["outputs"]["custom-output"]["pending_payload_bytes"],
            0
        );
        assert_eq!(snapshot["pending_keys"], 0);
        assert_eq!(snapshot["output_payload_bytes_total"], 6);
        assert_eq!(snapshot["conflated_payload_bytes_total"], 4);
    }

    #[test]
    fn reductions_are_unavailable_before_the_first_output_input() {
        let metrics = Metrics::new();
        metrics.register_output("empty-output");

        let snapshot = serde_json::to_value(metrics.snapshot()).unwrap();

        assert!(snapshot["outputs"]["empty-output"]["message_reduction_percent"].is_null());
        assert!(snapshot["outputs"]["empty-output"]["payload_reduction_percent"].is_null());
    }

    #[test]
    fn oversized_policy_counters_and_pending_bytes_are_exposed_per_output() {
        let metrics = Metrics::new();
        let output = metrics.register_output("policy-output");
        metrics.record_output_input(&output, 10);
        metrics.record_output_truncated(&output, 4);
        metrics.record_output_dropped(&output, 1, 6);

        let snapshot = serde_json::to_value(metrics.snapshot()).unwrap();
        assert_eq!(snapshot["schema_version"], 4);
        assert_eq!(snapshot["dropped_messages_total"], 1);
        assert_eq!(snapshot["dropped_payload_bytes_total"], 6);
        assert_eq!(snapshot["truncated_messages_total"], 1);
        assert_eq!(snapshot["truncated_payload_bytes_total"], 4);
        assert_eq!(
            snapshot["outputs"]["policy-output"]["dropped_messages_total"],
            1
        );
        assert_eq!(
            snapshot["outputs"]["policy-output"]["dropped_payload_bytes_total"],
            6
        );
        assert_eq!(
            snapshot["outputs"]["policy-output"]["truncated_messages_total"],
            1
        );
        assert_eq!(
            snapshot["outputs"]["policy-output"]["truncated_payload_bytes_total"],
            4
        );
        assert_eq!(snapshot["outputs"]["policy-output"]["pending_messages"], 0);
        assert_eq!(
            snapshot["outputs"]["policy-output"]["pending_payload_bytes"],
            0
        );
    }

    #[test]
    fn uncertain_transactions_are_counted_without_leaving_messages_pending() {
        let metrics = Metrics::new();
        let output = metrics.register_output("running-output");
        metrics.record_output_input(&output, 4);
        metrics.record_output_input(&output, 6);
        metrics.record_output_uncertain(&output, "publish acknowledgement was lost", 2);
        metrics.record_output_abandoned(&output, 2, 10);
        metrics.set_output_state(&output, "running");

        let snapshot = serde_json::to_value(metrics.snapshot()).unwrap();

        assert_eq!(snapshot["uncertain_transactions_total"], 1);
        assert_eq!(snapshot["uncertain_messages_total"], 2);
        assert_eq!(snapshot["publish_error_messages_total"], 2);
        assert_eq!(
            snapshot["outputs"]["running-output"]["uncertain_transactions_total"],
            1
        );
        assert_eq!(
            snapshot["outputs"]["running-output"]["uncertain_messages_total"],
            2
        );
        assert_eq!(
            snapshot["outputs"]["running-output"]["publish_error_messages_total"],
            2
        );
        assert_eq!(snapshot["outputs"]["running-output"]["state"], "running");
        assert_eq!(snapshot["outputs"]["running-output"]["pending_messages"], 0);
        assert_eq!(
            snapshot["outputs"]["running-output"]["pending_payload_bytes"],
            0
        );
        assert_eq!(
            snapshot["outputs"]["running-output"]["publish_errors_total"],
            1
        );
    }
}
