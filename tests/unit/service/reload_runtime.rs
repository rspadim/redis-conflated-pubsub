use super::*;

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
fn reload_rejects_instance_lock_or_logging_changes() {
    let current: AppConfig = serde_json::from_str(include_str!("../../../config.example.json"))
        .expect("example config should deserialize");
    let mut next = current.clone();
    assert!(reload_process_settings_unchanged(&current, &next));

    next.logging.level = "debug".to_owned();
    assert!(!reload_process_settings_unchanged(&current, &next));

    next = current.clone();
    next.instance_lock.path.push(".new");
    assert!(!reload_process_settings_unchanged(&current, &next));
}

#[cfg(unix)]
#[test]
fn reload_config_is_parsed_and_validated_before_acceptance() {
    let current: AppConfig = serde_json::from_str(include_str!("../../../config.example.json"))
        .expect("example config should deserialize");
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("config.json");
    std::fs::write(&path, include_str!("../../../config.example.json")).unwrap();

    let loaded = load_reload_config(&current, &path).unwrap();
    assert_eq!(loaded.outputs.len(), current.outputs.len());

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
