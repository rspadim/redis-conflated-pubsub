use super::*;
use std::time::Duration;
use tempfile::tempdir;

const DEFAULT_PREFIX: &str = "redis-conflated-pubsub";

fn logging_config(directory: &Path, prefix: &str) -> LoggingConfig {
    LoggingConfig {
        directory: directory.to_owned(),
        level: "info".to_owned(),
        prefix: prefix.to_owned(),
        enabled: true,
        retention_days: 14,
        max_total_size_mb: 1,
    }
}

fn set_modified(path: &Path, age: Duration) {
    OpenOptions::new()
        .write(true)
        .open(path)
        .unwrap()
        .set_times(std::fs::FileTimes::new().set_modified(SystemTime::now() - age))
        .unwrap();
}

#[test]
fn rotates_daily_logs_and_preserves_the_active_file_during_cleanup() {
    let directory = tempdir().unwrap();
    let previous_active = replace_active_log_path(None);
    let mut writer =
        SizeLimitedDailyWriter::new(directory.path(), DEFAULT_PREFIX, 5, 14, 1024 * 1024).unwrap();

    writer.write_all(b"12345").unwrap();
    writer.write_all(b"6").unwrap();
    let active_path = writer.active_path().to_owned();
    assert_eq!(fs::metadata(&active_path).unwrap().len(), 1);
    assert_eq!(
        fs::metadata(daily_log_path(
            directory.path(),
            DEFAULT_PREFIX,
            &writer.current.date,
            0
        ))
        .unwrap()
        .len(),
        5
    );

    let older_path = directory
        .path()
        .join(format!("{DEFAULT_PREFIX}.2020-01-01"));
    let newer_path = directory
        .path()
        .join(format!("{DEFAULT_PREFIX}.2020-01-02"));
    fs::write(&older_path, vec![b'a'; 600_000]).unwrap();
    fs::write(&newer_path, vec![b'b'; 600_000]).unwrap();
    let expired_path = directory
        .path()
        .join(format!("{DEFAULT_PREFIX}.2019-01-01"));
    fs::write(&expired_path, b"expired").unwrap();
    set_modified(&older_path, Duration::from_secs(10));
    set_modified(&newer_path, Duration::from_secs(5));
    set_modified(&expired_path, Duration::from_secs(15 * 86_400));

    let unrelated_path = directory.path().join(format!("{DEFAULT_PREFIX}-not-a-log"));
    fs::write(&unrelated_path, vec![b'x'; 1_200_000]).unwrap();

    cleanup(&logging_config(directory.path(), DEFAULT_PREFIX)).unwrap();

    assert!(active_path.exists());
    assert!(!older_path.exists());
    assert!(!expired_path.exists());
    assert!(newer_path.exists());
    assert!(unrelated_path.exists());

    drop(writer);
    replace_active_log_path(previous_active);
}

#[test]
fn custom_prefix_names_and_parses_daily_log_files() {
    let directory = tempdir().unwrap();
    let previous_active = replace_active_log_path(None);
    let mut writer =
        SizeLimitedDailyWriter::new(directory.path(), "svc-a", 5, 14, 1024 * 1024).unwrap();

    writer.write_all(b"12345").unwrap();
    let first = daily_log_path(directory.path(), "svc-a", &writer.current.date, 0);
    assert_eq!(fs::metadata(&first).unwrap().len(), 5);
    assert_eq!(
        parse_daily_log_name(first.file_name().unwrap().to_str().unwrap(), "svc-a")
            .map(|(_, sequence)| sequence),
        Some(0)
    );
    assert!(parse_daily_log_name("redis-conflated-pubsub.2020-01-01", "svc-a").is_none());

    writer.write_all(b"6").unwrap();
    let second = daily_log_path(directory.path(), "svc-a", &writer.current.date, 1);
    assert_eq!(fs::metadata(&second).unwrap().len(), 1);

    drop(writer);
    replace_active_log_path(previous_active);
}

#[test]
fn cleanup_only_removes_daily_logs_of_the_configured_prefix() {
    let directory = tempdir().unwrap();
    let own_expired = directory.path().join("svc-a.2019-01-01");
    let own_current = directory.path().join("svc-a.2020-01-01");
    let other_current = directory.path().join("svc-b.2020-01-02");
    fs::write(&own_expired, b"expired").unwrap();
    fs::write(&own_current, vec![b'a'; 600_000]).unwrap();
    fs::write(&other_current, vec![b'b'; 1_200_000]).unwrap();
    set_modified(&own_expired, Duration::from_secs(15 * 86_400));

    cleanup(&logging_config(directory.path(), "svc-a")).unwrap();

    assert!(!own_expired.exists());
    assert!(own_current.exists());
    assert!(other_current.exists());
}

#[test]
fn disabled_logging_does_not_create_the_directory_or_install_a_subscriber() {
    let directory = tempdir().unwrap();
    let missing = directory.path().join("missing");
    let mut config = logging_config(&missing, "svc-a");
    config.enabled = false;

    let guard = init(&config).unwrap();

    assert!(guard.is_none());
    assert!(!missing.exists());
}
