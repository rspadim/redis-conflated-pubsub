use std::{
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
    sync::{Mutex, MutexGuard, OnceLock},
    time::SystemTime,
};

use anyhow::{Context, Result, anyhow};
use chrono::Utc;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{EnvFilter, fmt};

use crate::config::LoggingConfig;

const LOG_FILE_STEM: &str = "redis-conflated-pubsub.";

pub fn init(config: &LoggingConfig) -> Result<WorkerGuard> {
    fs::create_dir_all(&config.directory).with_context(|| {
        format!(
            "failed to create log directory {}",
            config.directory.display()
        )
    })?;
    cleanup(config)?;

    let filter = EnvFilter::try_new(&config.level)
        .with_context(|| format!("invalid logging level: {}", config.level))?;
    let max_total_bytes = config.max_total_size_mb.saturating_mul(1024 * 1024);
    let max_file_bytes = (max_total_bytes / config.retention_days.max(1)).max(1);
    let appender = SizeLimitedDailyWriter::new(
        &config.directory,
        max_file_bytes,
        config.retention_days,
        max_total_bytes,
    )
    .with_context(|| {
        format!(
            "failed to initialize log file in {}",
            config.directory.display()
        )
    })?;
    let previous_active = replace_active_log_path(Some(appender.active_path().to_owned()));
    let (writer, guard) = tracing_appender::non_blocking(appender);

    if let Err(error) = fmt()
        .json()
        .with_env_filter(filter)
        .with_writer(writer)
        .with_ansi(false)
        .try_init()
    {
        drop(guard);
        replace_active_log_path(previous_active);
        return Err(anyhow!("failed to initialize logging: {error}"));
    }

    Ok(guard)
}

pub fn cleanup(config: &LoggingConfig) -> Result<()> {
    cleanup_logs(
        &config.directory,
        config.retention_days,
        config.max_total_size_mb.saturating_mul(1024 * 1024),
    )
}

fn cleanup_logs(directory: &Path, retention_days: u64, max_bytes: u64) -> Result<()> {
    let active_path = lock_active_log_path();
    let now = SystemTime::now();
    let max_age = std::time::Duration::from_secs(retention_days.saturating_mul(86_400));
    let mut files = log_files(directory)?;

    for (path, modified, _) in &files {
        if active_path.as_ref() != Some(path)
            && now.duration_since(*modified).unwrap_or_default() > max_age
        {
            let _ = fs::remove_file(path);
        }
    }

    files = log_files(directory)?;
    let mut total = files
        .iter()
        .fold(0_u64, |total, (_, _, size)| total.saturating_add(*size));
    files.sort_by(|left, right| left.1.cmp(&right.1).then_with(|| left.0.cmp(&right.0)));
    for (path, _, size) in files {
        if total <= max_bytes {
            break;
        }
        if active_path.as_ref() == Some(&path) {
            continue;
        }
        if fs::remove_file(path).is_ok() {
            total = total.saturating_sub(size);
        }
    }
    Ok(())
}

fn log_files(directory: &Path) -> Result<Vec<(PathBuf, SystemTime, u64)>> {
    let mut files = Vec::new();
    for entry in fs::read_dir(directory)
        .with_context(|| format!("failed to read log directory {}", directory.display()))?
    {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if parse_daily_log_name(name).is_none() || !entry.file_type()?.is_file() {
            continue;
        }

        let metadata = entry.metadata()?;
        files.push((
            entry.path(),
            metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH),
            metadata.len(),
        ));
    }
    Ok(files)
}

fn parse_daily_log_name(name: &str) -> Option<(&str, u64)> {
    let suffix = name.strip_prefix(LOG_FILE_STEM)?;
    let (date, sequence) = match suffix.split_once('.') {
        Some((date, sequence)) => {
            let sequence = sequence.parse::<u64>().ok()?;
            if sequence == 0 {
                return None;
            }
            (date, sequence)
        }
        None => (suffix, 0),
    };

    let bytes = date.as_bytes();
    if bytes.len() != 10
        || !bytes.iter().enumerate().all(|(index, byte)| match index {
            4 | 7 => *byte == b'-',
            _ => byte.is_ascii_digit(),
        })
    {
        return None;
    }
    Some((date, sequence))
}

struct DailyLogFile {
    path: PathBuf,
    date: String,
    sequence: u64,
    file: File,
    size: u64,
}

struct SizeLimitedDailyWriter {
    directory: PathBuf,
    max_file_bytes: u64,
    retention_days: u64,
    max_total_bytes: u64,
    current: DailyLogFile,
}

impl SizeLimitedDailyWriter {
    fn new(
        directory: &Path,
        max_file_bytes: u64,
        retention_days: u64,
        max_total_bytes: u64,
    ) -> io::Result<Self> {
        let date = current_utc_date();
        let current = open_current_daily_file(directory, &date, max_file_bytes.max(1))?;
        Ok(Self {
            directory: directory.to_owned(),
            max_file_bytes: max_file_bytes.max(1),
            retention_days,
            max_total_bytes,
            current,
        })
    }

    fn active_path(&self) -> &Path {
        &self.current.path
    }
}

impl Write for SizeLimitedDailyWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }

        let buffer_size = u64::try_from(buffer.len()).unwrap_or(u64::MAX);
        let rotated = {
            let mut active_path = lock_active_log_path();
            let mut rotated = false;
            let date = current_utc_date();
            if self.current.date != date {
                self.current =
                    open_current_daily_file(&self.directory, &date, self.max_file_bytes)?;
                rotated = true;
            }

            if self.current.size > 0
                && self.current.size.saturating_add(buffer_size) > self.max_file_bytes
            {
                self.current = create_next_daily_file(
                    &self.directory,
                    &self.current.date,
                    self.current.sequence,
                )?;
                rotated = true;
            }

            *active_path = Some(self.current.path.clone());
            self.current.file.write_all(buffer)?;
            self.current.size = self.current.size.saturating_add(buffer_size);
            rotated
        };

        if rotated {
            let _ = cleanup_logs(&self.directory, self.retention_days, self.max_total_bytes);
        }
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.current.file.flush()
    }
}

impl Drop for SizeLimitedDailyWriter {
    fn drop(&mut self) {
        let mut active_path = lock_active_log_path();
        if active_path.as_ref() == Some(&self.current.path) {
            *active_path = None;
        }
    }
}

fn open_current_daily_file(
    directory: &Path,
    date: &str,
    max_file_bytes: u64,
) -> io::Result<DailyLogFile> {
    let mut segments = log_files(directory)
        .map_err(io::Error::other)?
        .into_iter()
        .filter_map(|(path, _, size)| {
            let name = path.file_name()?.to_str()?;
            let (file_date, sequence) = parse_daily_log_name(name)?;
            (file_date == date).then_some((sequence, path, size))
        })
        .collect::<Vec<_>>();
    segments.sort_by_key(|(sequence, _, _)| *sequence);

    if let Some((sequence, path, _)) = segments.last() {
        let file = OpenOptions::new().append(true).open(path)?;
        let size = file.metadata()?.len();
        if size < max_file_bytes {
            return Ok(DailyLogFile {
                path: path.clone(),
                date: date.to_owned(),
                sequence: *sequence,
                file,
                size,
            });
        }
    }

    let next_sequence = match segments.last() {
        Some((sequence, _, _)) => sequence
            .checked_add(1)
            .ok_or_else(|| io::Error::other("daily log rotation sequence is exhausted"))?,
        None => 0,
    };
    create_daily_file(directory, date, next_sequence)
}

fn create_next_daily_file(
    directory: &Path,
    date: &str,
    current_sequence: u64,
) -> io::Result<DailyLogFile> {
    let sequence = current_sequence
        .checked_add(1)
        .ok_or_else(|| io::Error::other("daily log rotation sequence is exhausted"))?;
    create_daily_file(directory, date, sequence)
}

fn create_daily_file(directory: &Path, date: &str, mut sequence: u64) -> io::Result<DailyLogFile> {
    loop {
        let path = daily_log_path(directory, date, sequence);
        match OpenOptions::new().append(true).create_new(true).open(&path) {
            Ok(file) => {
                return Ok(DailyLogFile {
                    path,
                    date: date.to_owned(),
                    sequence,
                    file,
                    size: 0,
                });
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                sequence = sequence
                    .checked_add(1)
                    .ok_or_else(|| io::Error::other("daily log rotation sequence is exhausted"))?;
            }
            Err(error) => return Err(error),
        }
    }
}

fn daily_log_path(directory: &Path, date: &str, sequence: u64) -> PathBuf {
    let name = if sequence == 0 {
        format!("{LOG_FILE_STEM}{date}")
    } else {
        format!("{LOG_FILE_STEM}{date}.{sequence:06}")
    };
    directory.join(name)
}

fn current_utc_date() -> String {
    Utc::now().format("%Y-%m-%d").to_string()
}

fn active_log_path() -> &'static Mutex<Option<PathBuf>> {
    static ACTIVE_LOG_PATH: OnceLock<Mutex<Option<PathBuf>>> = OnceLock::new();
    ACTIVE_LOG_PATH.get_or_init(|| Mutex::new(None))
}

fn lock_active_log_path() -> MutexGuard<'static, Option<PathBuf>> {
    active_log_path()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn replace_active_log_path(path: Option<PathBuf>) -> Option<PathBuf> {
    std::mem::replace(&mut *lock_active_log_path(), path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tempfile::tempdir;

    #[test]
    fn rotates_daily_logs_and_preserves_the_active_file_during_cleanup() {
        let directory = tempdir().unwrap();
        let previous_active = replace_active_log_path(None);
        let mut writer = SizeLimitedDailyWriter::new(directory.path(), 5, 14, 1024 * 1024).unwrap();

        writer.write_all(b"12345").unwrap();
        writer.write_all(b"6").unwrap();
        let active_path = writer.active_path().to_owned();
        assert_eq!(fs::metadata(&active_path).unwrap().len(), 1);
        assert_eq!(
            fs::metadata(daily_log_path(directory.path(), &writer.current.date, 0))
                .unwrap()
                .len(),
            5
        );

        let older_path = directory.path().join("redis-conflated-pubsub.2020-01-01");
        let newer_path = directory.path().join("redis-conflated-pubsub.2020-01-02");
        fs::write(&older_path, vec![b'a'; 600_000]).unwrap();
        fs::write(&newer_path, vec![b'b'; 600_000]).unwrap();
        let expired_path = directory.path().join("redis-conflated-pubsub.2019-01-01");
        fs::write(&expired_path, b"expired").unwrap();
        OpenOptions::new()
            .write(true)
            .open(&older_path)
            .unwrap()
            .set_times(
                std::fs::FileTimes::new().set_modified(SystemTime::now() - Duration::from_secs(10)),
            )
            .unwrap();
        OpenOptions::new()
            .write(true)
            .open(&newer_path)
            .unwrap()
            .set_times(
                std::fs::FileTimes::new().set_modified(SystemTime::now() - Duration::from_secs(5)),
            )
            .unwrap();
        OpenOptions::new()
            .write(true)
            .open(&expired_path)
            .unwrap()
            .set_times(
                std::fs::FileTimes::new()
                    .set_modified(SystemTime::now() - Duration::from_secs(15 * 86_400)),
            )
            .unwrap();

        let unrelated_path = directory.path().join("redis-conflated-pubsub-not-a-log");
        fs::write(&unrelated_path, vec![b'x'; 1_200_000]).unwrap();

        cleanup(&LoggingConfig {
            directory: directory.path().to_owned(),
            level: "info".to_owned(),
            retention_days: 14,
            max_total_size_mb: 1,
        })
        .unwrap();

        assert!(active_path.exists());
        assert!(!older_path.exists());
        assert!(!expired_path.exists());
        assert!(newer_path.exists());
        assert!(unrelated_path.exists());

        drop(writer);
        replace_active_log_path(previous_active);
    }
}
