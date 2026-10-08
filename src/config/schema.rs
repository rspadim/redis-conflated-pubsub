use super::{
    AppConfig, DEFAULT_CHANNEL_CACHE_MAX_ENTRIES, MAX_CHANNEL_CACHE_MAX_ENTRIES,
    MAX_DEDUPLICATION_GROUP_CACHE_BYTES, MAX_DEDUPLICATION_GROUP_MEMBERS, MAX_RUNTIME_DURATION_MS,
};

pub(super) fn json_schema() -> serde_json::Value {
    let mut schema = serde_json::to_value(schemars::schema_for!(AppConfig))
        .expect("the AppConfig JSON Schema must serialize");
    schema["properties"]["outputs"]["minProperties"] = serde_json::json!(1);
    schema["properties"]["outputs"]["propertyNames"]["pattern"] = serde_json::json!("\\S");
    schema["$defs"]["InputConfig"]["properties"]["subscriptions"]["minItems"] =
        serde_json::json!(1);
    schema["$defs"]["ChannelFilterRule"]["description"] = serde_json::json!(
        "An ordered channel filter rule. Exactly one of glob, regex, raw, single, or string is required. raw/single/string are aliases for exact string equality. Every rule is evaluated and the last matching rule decides whether the channel is accepted or denied."
    );
    let filter_selectors = ["glob", "regex", "raw", "single", "string"];
    schema["$defs"]["ChannelFilterRule"]["oneOf"] = serde_json::Value::Array(
        filter_selectors
            .iter()
            .map(|selector| {
                let mut properties = serde_json::Map::new();
                properties.insert(
                    (*selector).to_owned(),
                    serde_json::json!({"type": "string", "minLength": 1}),
                );
                let other_selectors = filter_selectors
                    .iter()
                    .filter(|other| *other != selector)
                    .map(|other| serde_json::json!({"required": [other]}))
                    .collect::<Vec<_>>();
                serde_json::json!({
                    "required": [selector],
                    "properties": properties,
                    "not": {"anyOf": other_selectors}
                })
            })
            .collect(),
    );
    schema["$defs"]["FilterAction"]["default"] = serde_json::json!("accept");
    for definition in ["InputConfig", "OutputConfig"] {
        schema["$defs"][definition]["properties"]["filters"]["default"] = serde_json::json!([]);
        schema["$defs"][definition]["properties"]["filter_default"]["default"] =
            serde_json::json!("accept");
        schema["$defs"][definition]["properties"]["filter_cache_max_entries"]["default"] =
            serde_json::json!(DEFAULT_CHANNEL_CACHE_MAX_ENTRIES);
        schema["$defs"][definition]["properties"]["filter_cache_max_entries"]["minimum"] =
            serde_json::json!(0);
        schema["$defs"][definition]["properties"]["filter_cache_max_entries"]["maximum"] =
            serde_json::json!(MAX_CHANNEL_CACHE_MAX_ENTRIES);
    }
    schema["$defs"]["OutputConfig"]["properties"]["channel_policy_cache_max_entries"]["default"] =
        serde_json::json!(DEFAULT_CHANNEL_CACHE_MAX_ENTRIES);
    schema["$defs"]["OutputConfig"]["properties"]["channel_policy_cache_max_entries"]["minimum"] =
        serde_json::json!(0);
    schema["$defs"]["OutputConfig"]["properties"]["channel_policy_cache_max_entries"]["maximum"] =
        serde_json::json!(MAX_CHANNEL_CACHE_MAX_ENTRIES);
    schema["$defs"]["HttpStatusConfig"]["properties"]["filters_endpoint_enabled"]["default"] =
        serde_json::json!(false);
    schema["$defs"]["OutputConfig"]["properties"]["deduplication_groups"]["propertyNames"]["pattern"] =
        serde_json::json!("\\S");
    schema["$defs"]["OutputConfig"]["properties"]["profiles"]["propertyNames"]["pattern"] =
        serde_json::json!("\\S");
    schema["$defs"]["OutputConfig"]["properties"]["queue_overflow_policy"]["default"] =
        serde_json::json!("drop_newest");
    schema["$defs"]["OutputConfig"]["properties"]["queue_max_age_ms"]["maximum"] =
        serde_json::json!(MAX_RUNTIME_DURATION_MS as u64);
    schema["$defs"]["OutputConfig"]["properties"]["channel_policies"]["description"] = serde_json::json!(
        "Ordered explicit-match rules followed by exactly one default catch-all rule. The first explicit match wins; the default must be last and catches the rest without merging. JSON Schema checks that one default exists; runtime validation (`--check-config`) enforces its final position."
    );
    schema["$defs"]["OutputConfig"]["allOf"] = serde_json::json!([{
        "if": {
            "required": ["channel_policies"],
            "properties": {"channel_policies": {"minItems": 1}}
        },
        "then": {
            "properties": {
                "channel_policies": {
                    "contains": {
                        "required": ["default"],
                        "properties": {"default": {"const": true}}
                    },
                    "minContains": 1,
                    "maxContains": 1
                }
            }
        }
    }]);
    schema["$defs"]["ChannelProfile"]["description"] = serde_json::json!(
        "Optional overrides for an output channel. A profile cannot define both deduplication.ttl_ms and deduplication.group."
    );
    schema["$defs"]["ChannelProfile"]["not"] = serde_json::json!({
        "anyOf": [{
            "required": ["deduplication.ttl_ms", "deduplication.group"],
            "properties": {
                "deduplication.ttl_ms": {"type": "integer"},
                "deduplication.group": {"type": "string"}
            }
        }]
    });
    schema["$defs"]["ChannelProfile"]["allOf"] = serde_json::json!([{
        "if": {
            "required": ["deduplication.group"],
            "properties": {"deduplication.group": {"type": "string"}}
        },
        "then": {
            "properties": {"deduplication.group": {"pattern": "\\S"}}
        }
    }]);
    schema["$defs"]["ChannelPolicy"]["description"] = serde_json::json!(
        "Ordered rules matched against the final mapped Pub/Sub channel. Explicit selectors match first; the unique default:true rule must be last and catches the rest without merging. A rule uses either one selector or default:true, plus either a profile or inline overrides. Runtime validation (`--check-config`) enforces that default is last."
    );
    schema["$defs"]["ChannelPolicy"]["properties"]["default"]["default"] = serde_json::json!(false);
    for (definition, property) in [
        ("ConflationConfig", "interval_ms"),
        ("DeduplicationConfig", "ttl_ms"),
        ("DeduplicationGroup", "ttl_ms"),
        ("ChannelProfile", "conflation.interval_ms"),
        ("ChannelProfile", "deduplication.ttl_ms"),
        ("ChannelPolicy", "conflation.interval_ms"),
        ("ChannelPolicy", "deduplication.ttl_ms"),
    ] {
        schema["$defs"][definition]["properties"][property]["maximum"] =
            serde_json::json!(MAX_RUNTIME_DURATION_MS);
    }
    schema["$defs"]["RedisConfig"]["properties"]["connect_timeout_ms"]["maximum"] =
        serde_json::json!(MAX_RUNTIME_DURATION_MS as u64);
    schema["$defs"]["StatusConfig"]["properties"]["update_interval_ms"]["maximum"] =
        serde_json::json!(MAX_RUNTIME_DURATION_MS as u64);
    schema["$defs"]["DeduplicationGroup"]["properties"]["max_members"]["maximum"] =
        serde_json::json!(MAX_DEDUPLICATION_GROUP_MEMBERS);
    schema["$defs"]["DeduplicationGroup"]["properties"]["max_cache_bytes"]["maximum"] =
        serde_json::json!(MAX_DEDUPLICATION_GROUP_CACHE_BYTES);
    schema["$defs"]["ChannelPolicy"]["allOf"] = serde_json::json!([
        {
            "oneOf": [
                {
                    "required": ["glob"],
                    "properties": {"glob": {"type": "string", "minLength": 1, "pattern": "\\S"}},
                    "not": {"anyOf": [
                        {"required": ["prefix"], "properties": {"prefix": {"type": "string"}}},
                        {"required": ["suffix"], "properties": {"suffix": {"type": "string"}}},
                        {"required": ["default"], "properties": {"default": {"const": true}}}
                    ]}
                },
                {
                    "required": ["prefix"],
                    "properties": {"prefix": {"type": "string", "minLength": 1, "pattern": "\\S"}},
                    "not": {"anyOf": [
                        {"required": ["glob"], "properties": {"glob": {"type": "string"}}},
                        {"required": ["suffix"], "properties": {"suffix": {"type": "string"}}},
                        {"required": ["default"], "properties": {"default": {"const": true}}}
                    ]}
                },
                {
                    "required": ["suffix"],
                    "properties": {"suffix": {"type": "string", "minLength": 1, "pattern": "\\S"}},
                    "not": {"anyOf": [
                        {"required": ["glob"], "properties": {"glob": {"type": "string"}}},
                        {"required": ["prefix"], "properties": {"prefix": {"type": "string"}}},
                        {"required": ["default"], "properties": {"default": {"const": true}}}
                    ]}
                },
                {
                    "required": ["default"],
                    "properties": {"default": {"const": true}},
                    "not": {"anyOf": [
                        {"required": ["glob"], "properties": {"glob": {"type": "string"}}},
                        {"required": ["prefix"], "properties": {"prefix": {"type": "string"}}},
                        {"required": ["suffix"], "properties": {"suffix": {"type": "string"}}}
                    ]}
                }
            ]
        },
        {
            "oneOf": [
                {
                    "required": ["profile"],
                    "properties": {"profile": {"type": "string", "minLength": 1, "pattern": "\\S"}},
                    "not": {"anyOf": [
                        {"required": ["conflation.interval_ms"], "properties": {"conflation.interval_ms": {"type": "integer"}}},
                        {"required": ["deduplication.ttl_ms"], "properties": {"deduplication.ttl_ms": {"type": "integer"}}},
                        {"required": ["deduplication.group"], "properties": {"deduplication.group": {"type": "string"}}}
                    ]}
                },
                {
                    "not": {"required": ["profile"], "properties": {"profile": {"type": "string"}}},
                    "anyOf": [
                        {"required": ["conflation.interval_ms"], "properties": {"conflation.interval_ms": {"type": "integer"}}},
                        {"required": ["deduplication.ttl_ms"], "properties": {"deduplication.ttl_ms": {"type": "integer"}}},
                        {"required": ["deduplication.group"], "properties": {"deduplication.group": {"type": "string", "minLength": 1, "pattern": "\\S"}}}
                    ]
                }
            ]
        },
        {
            "not": {"anyOf": [{
                "required": ["deduplication.ttl_ms", "deduplication.group"],
                "properties": {
                    "deduplication.ttl_ms": {"type": "integer"},
                    "deduplication.group": {"type": "string"}
                }
            }]}
        }
    ]);
    schema
}
