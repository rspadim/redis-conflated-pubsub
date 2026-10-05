use std::{
    fs,
    io::Write,
    path::Path,
    sync::Mutex,
    sync::atomic::{AtomicU64, Ordering},
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
    pub output_batches_total: AtomicU64,
    pub output_messages_total: AtomicU64,
    pub conflated_messages_total: AtomicU64,
    pub excluded_messages_total: AtomicU64,
    pub dropped_messages_total: AtomicU64,
    pub publish_errors_total: AtomicU64,
    pub input_reconnects_total: AtomicU64,
    pub output_reconnects_total: AtomicU64,
    pub pending_keys: AtomicU64,
    last_input_at: Mutex<Option<String>>,
    last_flush_at: Mutex<Option<String>>,
    last_error: Mutex<Option<String>>,
    state: Mutex<String>,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            started_at: Utc::now(),
            started_clock: Instant::now(),
            input_messages_total: AtomicU64::new(0),
            output_batches_total: AtomicU64::new(0),
            output_messages_total: AtomicU64::new(0),
            conflated_messages_total: AtomicU64::new(0),
            excluded_messages_total: AtomicU64::new(0),
            dropped_messages_total: AtomicU64::new(0),
            publish_errors_total: AtomicU64::new(0),
            input_reconnects_total: AtomicU64::new(0),
            output_reconnects_total: AtomicU64::new(0),
            pending_keys: AtomicU64::new(0),
            last_input_at: Mutex::new(None),
            last_flush_at: Mutex::new(None),
            last_error: Mutex::new(None),
            state: Mutex::new("starting".to_owned()),
        }
    }

    pub fn record_input(&self) {
        self.input_messages_total.fetch_add(1, Ordering::Relaxed);
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

    pub fn snapshot(&self) -> StatusSnapshot {
        StatusSnapshot {
            schema_version: 1,
            state: self.state.lock().unwrap().clone(),
            updated_at: timestamp(),
            started_at: self.started_at.to_rfc3339_opts(SecondsFormat::Millis, true),
            uptime_seconds: self.started_clock.elapsed().as_secs(),
            input_messages_total: self.input_messages_total.load(Ordering::Relaxed),
            output_batches_total: self.output_batches_total.load(Ordering::Relaxed),
            output_messages_total: self.output_messages_total.load(Ordering::Relaxed),
            conflated_messages_total: self.conflated_messages_total.load(Ordering::Relaxed),
            excluded_messages_total: self.excluded_messages_total.load(Ordering::Relaxed),
            dropped_messages_total: self.dropped_messages_total.load(Ordering::Relaxed),
            publish_errors_total: self.publish_errors_total.load(Ordering::Relaxed),
            input_reconnects_total: self.input_reconnects_total.load(Ordering::Relaxed),
            output_reconnects_total: self.output_reconnects_total.load(Ordering::Relaxed),
            pending_keys: self.pending_keys.load(Ordering::Relaxed),
            last_input_at: self.last_input_at.lock().unwrap().clone(),
            last_flush_at: self.last_flush_at.lock().unwrap().clone(),
            last_error: self.last_error.lock().unwrap().clone(),
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
    pub output_batches_total: u64,
    pub output_messages_total: u64,
    pub conflated_messages_total: u64,
    pub excluded_messages_total: u64,
    pub dropped_messages_total: u64,
    pub publish_errors_total: u64,
    pub input_reconnects_total: u64,
    pub output_reconnects_total: u64,
    pub pending_keys: u64,
    pub last_input_at: Option<String>,
    pub last_flush_at: Option<String>,
    pub last_error: Option<String>,
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
        metrics.input_messages_total.store(3, Ordering::Relaxed);

        write_atomic(&path, &metrics.snapshot()).unwrap();
        metrics.input_messages_total.store(7, Ordering::Relaxed);
        write_atomic(&path, &metrics.snapshot()).unwrap();

        let json: serde_json::Value = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
        assert_eq!(json["state"], "running");
        assert_eq!(json["input_messages_total"], 7);
    }
}
