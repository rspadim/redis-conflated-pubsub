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
