use super::*;

#[cfg(unix)]
fn acquire_after_release(config: &AppConfig) -> std::fs::File {
    // Lock release after dropping the handle is normally immediate, but a rare
    // filesystem/scheduler timing can delay it; retry briefly instead of
    // failing the test spuriously.
    for _ in 0..50 {
        if let Ok(lock) = config.instance_lock.acquire() {
            return lock;
        }
        std::thread::sleep(std::time::Duration::from_millis(4));
    }
    panic!("instance lock was not released in time");
}

#[cfg(unix)]
#[tokio::test]
async fn sighup_is_reported_as_a_configuration_reload() {
    use std::process::Command;

    let mut signals = ShutdownSignals::new().unwrap();
    let waiting = tokio::spawn(async move { signals.recv().await.unwrap() });
    tokio::task::yield_now().await;
    let pid = std::process::id().to_string();
    let status = Command::new("kill")
        .args(["-HUP", pid.as_str()])
        .status()
        .expect("kill command should be available on Unix");
    assert!(status.success());
    assert_eq!(waiting.await.unwrap(), ServiceSignal::Reload);
}

#[cfg(unix)]
#[test]
fn reload_rejects_logging_changes_but_accepts_instance_lock_path_changes() {
    let current: AppConfig = serde_json::from_str(include_str!("../../../config.example.json"))
        .expect("example config should deserialize");
    let mut next = current.clone();
    assert!(reload_logging_settings_unchanged(&current, &next));

    next.logging.level = "debug".to_owned();
    assert!(!reload_logging_settings_unchanged(&current, &next));

    next = current.clone();
    next.logging.prefix = "other-service".to_owned();
    assert!(!reload_logging_settings_unchanged(&current, &next));

    next = current.clone();
    next.logging.enabled = false;
    assert!(!reload_logging_settings_unchanged(&current, &next));

    next = current.clone();
    next.instance_lock.path.push(".new");
    assert!(reload_logging_settings_unchanged(&current, &next));
}

#[cfg(unix)]
#[test]
fn reload_config_is_parsed_and_validated_before_acceptance() {
    let current: AppConfig = serde_json::from_str(include_str!("../../../config.example.json"))
        .expect("example config should deserialize");
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("config.json");
    std::fs::write(&path, include_str!("../../../config.example.json")).unwrap();

    let plan = load_reload_config(&current, &path).unwrap();
    assert_eq!(plan.config.outputs.len(), current.outputs.len());
    assert!(plan.instance_lock.is_none());

    std::fs::write(&path, "{ invalid json").unwrap();
    assert!(load_reload_config(&current, &path).is_err());

    let mut incompatible: serde_json::Value =
        serde_json::from_str(include_str!("../../../config.example.json")).unwrap();
    incompatible["logging"]["level"] = serde_json::json!("debug");
    std::fs::write(&path, serde_json::to_vec(&incompatible).unwrap()).unwrap();
    assert!(
        load_reload_config(&current, &path)
            .unwrap_err()
            .to_string()
            .contains("logging require a full service restart")
    );
}

#[cfg(unix)]
fn example_config_with_lock_path(lock_path: &std::path::Path) -> serde_json::Value {
    let mut value: serde_json::Value =
        serde_json::from_str(include_str!("../../../config.example.json")).unwrap();
    value["instance_lock"]["path"] = serde_json::json!(lock_path.to_string_lossy());
    value
}

#[cfg(unix)]
fn write_reload_config(
    directory: &std::path::Path,
    value: &serde_json::Value,
) -> std::path::PathBuf {
    let path = directory.join("config.json");
    std::fs::write(&path, serde_json::to_vec(value).unwrap()).unwrap();
    path
}

#[cfg(unix)]
#[test]
fn reload_with_an_unchanged_lock_path_keeps_the_current_lock() {
    let directory = tempfile::tempdir().unwrap();
    let lock_path = directory.path().join("service.lock");
    let mut current: AppConfig =
        serde_json::from_str(include_str!("../../../config.example.json")).unwrap();
    current.instance_lock.path = lock_path.clone();
    let held = current.instance_lock.acquire().unwrap();

    let value = example_config_with_lock_path(&lock_path);
    let config_path = write_reload_config(directory.path(), &value);

    let plan = load_reload_config(&current, &config_path).unwrap();
    assert!(plan.instance_lock.is_none());
    assert_eq!(plan.config.instance_lock.path, lock_path);
    // The current lock is still held, so a second acquisition fails.
    assert!(current.instance_lock.acquire().is_err());

    drop(held);
    let _ = acquire_after_release(&current);
}

#[cfg(unix)]
#[test]
fn reload_with_a_changed_lock_path_acquires_the_new_lock_before_releasing_the_old() {
    let directory = tempfile::tempdir().unwrap();
    let old_path = directory.path().join("old.lock");
    let new_path = directory.path().join("new.lock");
    let mut current: AppConfig =
        serde_json::from_str(include_str!("../../../config.example.json")).unwrap();
    current.instance_lock.path = old_path.clone();
    let old_lock = current.instance_lock.acquire().unwrap();

    let value = example_config_with_lock_path(&new_path);
    let config_path = write_reload_config(directory.path(), &value);

    let plan = load_reload_config(&current, &config_path).unwrap();
    let new_lock = plan
        .instance_lock
        .expect("a changed lock path must acquire a replacement lock");

    // Both locks are held while the runtime drains and restarts.
    assert!(current.instance_lock.acquire().is_err());
    assert!(plan.config.instance_lock.acquire().is_err());

    drop(old_lock);
    // The old lock is released; the new one stays held.
    let _ = acquire_after_release(&current);
    assert!(plan.config.instance_lock.acquire().is_err());

    drop(new_lock);
    let _ = acquire_after_release(&plan.config);
}

#[cfg(unix)]
#[test]
fn reload_with_a_taken_lock_path_is_rejected_and_keeps_the_old_lock() {
    let directory = tempfile::tempdir().unwrap();
    let old_path = directory.path().join("old.lock");
    let new_path = directory.path().join("new.lock");
    let mut current: AppConfig =
        serde_json::from_str(include_str!("../../../config.example.json")).unwrap();
    current.instance_lock.path = old_path.clone();
    let old_lock = current.instance_lock.acquire().unwrap();

    let value = example_config_with_lock_path(&new_path);
    let config_path = write_reload_config(directory.path(), &value);
    let next: AppConfig = serde_json::from_value(value).unwrap();
    let blocker = next.instance_lock.acquire().unwrap();

    let error = load_reload_config(&current, &config_path).unwrap_err();
    assert!(error.to_string().contains("new instance lock"));

    // The rejected reload kept the old lock; the runtime continues with it.
    assert!(current.instance_lock.acquire().is_err());
    drop(old_lock);
    let _ = acquire_after_release(&current);

    drop(blocker);
    let _ = acquire_after_release(&next);
}
