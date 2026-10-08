use clap::CommandFactory;

use super::Args;
use crate::config::AppConfig;

#[test]
fn long_help_documents_configuration_and_examples() {
    let help = Args::command().render_long_help().to_string();
    let help = help.split_whitespace().collect::<Vec<_>>().join(" ");
    for expected in [
        "max_bytes_per_exec",
        "oversized_message_policy",
        "exclude_output_echoes",
        "exclude_sentinel_pubsub",
        "outputs.<name>.deduplication.ttl_ms",
        "deduplication_groups",
        "round_ms",
        "floors the Unix-epoch TTL-start timestamp",
        "0 disables rounding",
        "restart_on_change",
        "Shared expiry starts",
        "new member's first successful or uncertain publication",
        "changed final value successfully or uncertainly published",
        "restart_on_change=false keeps it fixed",
        "Profiles contain partial",
        "max_members",
        "max_cache_bytes",
        "limited to 365 days",
        "channel_policies",
        "exactly one glob, prefix, or suffix selector",
        "default: true as the last catch-all rule",
        "Only the first match applies",
        "chooses either a profile or inline",
        "conflation.interval_ms",
        "deduplication.ttl_ms",
        "deduplication.group",
        "final mapped output channel",
        "unmatched channels use output-level defaults",
        "Channel filters use ordered `filters`",
        "one `glob`, `regex`, `raw`, `single`, or `string` selector",
        "`raw`, `single`, or `string` selector",
        "compare the complete channel string literally",
        "All rules are evaluated and the last match wins",
        "`filter_default` defaults to `accept`",
        "Input filters see the source channel",
        "output filters see the subscription-mapped channel",
        "`input.filter_cache_max_entries`, `outputs.<name>.filter_cache_max_entries`, and `outputs.<name>.channel_policy_cache_max_entries` independently size the local LRUs",
        "zero disables that cache",
        "maximum 100000",
        "filters_endpoint_enabled",
        "GET /filters",
        "disabled by default",
        "SIGHUP reloads a valid configuration",
        "invalid file leaves the current runtime active",
        "Changes to logging still require a full systemd restart",
        "the new lock is acquired before the previous lock is released",
        "the reload is rejected if the new lock is unavailable",
        "brief Pub/Sub input gap",
        "resetting status counters",
        "--config-json-schema",
    ] {
        assert!(help.contains(expected), "missing {expected} in help");
    }
}

#[test]
fn generated_config_schema_is_json_and_includes_filter_options() {
    let schema = AppConfig::json_schema();
    let serialized = serde_json::to_string(&schema).unwrap();
    assert!(serialized.contains("exclude_output_echoes"));
    assert!(serialized.contains("exclude_sentinel_pubsub"));
    assert!(serialized.contains("max_bytes_per_exec"));
    assert!(serialized.contains("oversized_message_policy"));
    assert!(serialized.contains("deduplication"));
    assert!(serialized.contains("ttl_ms"));
    assert_eq!(schema["type"], "object");
    assert_eq!(schema["properties"]["outputs"]["minProperties"], 1);
    assert_eq!(
        schema["$defs"]["InputConfig"]["properties"]["subscriptions"]["minItems"],
        1
    );
    let deduplication_ttl = &schema["$defs"]["DeduplicationConfig"]["properties"]["ttl_ms"];
    assert_eq!(deduplication_ttl["type"], "integer");
    assert_eq!(deduplication_ttl["default"], 0);
    assert!(deduplication_ttl.get("minimum").is_none());
}
