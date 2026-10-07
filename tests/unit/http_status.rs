use super::route_request;
use crate::status::Metrics;

#[test]
fn root_routes_to_json_status_snapshot() {
    let metrics = Metrics::new();
    metrics.set_state("running");

    let response = route_request("GET", "/", &metrics, false);
    let snapshot: serde_json::Value = serde_json::from_slice(&response.body).unwrap();

    assert_eq!(response.status, 200);
    assert_eq!(response.content_type, "application/json");
    assert_eq!(snapshot["state"], "running");
    assert_eq!(snapshot["schema_version"], 5);
}

#[test]
fn serves_only_the_root_status_path_and_get_method() {
    let metrics = Metrics::new();

    assert_eq!(
        route_request("GET", "/missing", &metrics, false).status,
        404
    );
    assert_eq!(route_request("GET", "/status", &metrics, false).status, 404);
    assert_eq!(route_request("GET", "/health", &metrics, false).status, 404);
    assert_eq!(
        route_request("GET", "/filters", &metrics, false).status,
        404
    );
    let method_not_allowed = route_request("POST", "/", &metrics, false);
    assert_eq!(method_not_allowed.status, 405);
    assert!(method_not_allowed.allow_get);
}

#[test]
fn filter_cache_endpoint_is_opt_in_and_returns_registered_cache_snapshots() {
    use std::sync::Arc;

    let metrics = Metrics::new();
    metrics.register_channel_cache_inspector(
        "input.filters".to_owned(),
        Arc::new(|| serde_json::json!({"entries": [{"channel": "events", "value": "deny"}]})),
    );

    let response = route_request("GET", "/filters?detail=full", &metrics, true);
    let snapshot: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
    assert_eq!(response.status, 200);
    assert_eq!(
        snapshot["caches"]["input.filters"]["entries"][0]["channel"],
        "events"
    );

    let method_not_allowed = route_request("POST", "/filters", &metrics, true);
    assert_eq!(method_not_allowed.status, 405);
    assert!(method_not_allowed.allow_get);
}
