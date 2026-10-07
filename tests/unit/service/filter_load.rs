use std::{hint::black_box, time::Instant};

use crate::{
    config::{ChannelFilterRule, DEFAULT_CHANNEL_CACHE_MAX_ENTRIES, FilterAction, OutputConfig},
    status::Metrics,
};

use super::*;

const FILTER_RULES: usize = 64;
const HOT_WORKING_SET: usize = 8_192;
const HOT_OPERATIONS: usize = 1_000_000;
const CHURN_CHANNELS: usize =
    DEFAULT_CHANNEL_CACHE_MAX_ENTRIES + DEFAULT_CHANNEL_CACHE_MAX_ENTRIES / 2;
const CHURN_PASSES: usize = 3;

#[test]
#[ignore = "manual release-mode filter-cache load benchmark; run with --ignored --nocapture"]
fn filter_cache_load_benchmark() {
    let rules = load_benchmark_rules();
    let hot_channels = hot_working_set();

    let mut no_filter = ChannelFilterSet::compile(&[], FilterAction::Accept).unwrap();
    let started = Instant::now();
    for _ in 0..HOT_OPERATIONS {
        black_box(no_filter.allows(black_box("feed:any-channel")));
    }
    let no_filter_elapsed = started.elapsed();
    println!(
        "filter-load mode=no-rules operations={HOT_OPERATIONS} ns_per_message={:.1} messages_per_second={:.0}",
        no_filter_elapsed.as_nanos() as f64 / HOT_OPERATIONS as f64,
        HOT_OPERATIONS as f64 / no_filter_elapsed.as_secs_f64(),
    );

    for expose_to_http in [false, true] {
        let metrics = Metrics::new();
        let inspector = expose_to_http.then(|| (&metrics, "bench.filters".to_owned()));
        let mut filter = ChannelFilterSet::compile_with_inspector(
            &rules,
            FilterAction::Accept,
            DEFAULT_CHANNEL_CACHE_MAX_ENTRIES,
            inspector,
        )
        .unwrap();

        let started = Instant::now();
        for channel in &hot_channels {
            black_box(filter.allows(black_box(channel)));
        }
        let cold_elapsed = started.elapsed();

        let started = Instant::now();
        for index in 0..HOT_OPERATIONS {
            let channel = &hot_channels[index % hot_channels.len()];
            black_box(filter.allows(black_box(channel)));
        }
        let hot_elapsed = started.elapsed();
        let hot_snapshot = filter.cache_snapshot();
        println!(
            "filter-load mode={} rules={FILTER_RULES} working_set={} cold_ns_per_message={:.1} hot_operations={HOT_OPERATIONS} hot_ns_per_message={:.1} hot_messages_per_second={:.0} hits={} misses={} evictions={} resident={}/{}",
            if expose_to_http {
                "shared-for-http"
            } else {
                "worker-local"
            },
            hot_channels.len(),
            cold_elapsed.as_nanos() as f64 / hot_channels.len() as f64,
            hot_elapsed.as_nanos() as f64 / HOT_OPERATIONS as f64,
            HOT_OPERATIONS as f64 / hot_elapsed.as_secs_f64(),
            hot_snapshot["hits"].as_u64().unwrap(),
            hot_snapshot["misses"].as_u64().unwrap(),
            hot_snapshot["evictions"].as_u64().unwrap(),
            hot_snapshot["entry_count"].as_u64().unwrap(),
            hot_snapshot["capacity"].as_u64().unwrap(),
        );
    }

    for cache_capacity in [0, 4_096, DEFAULT_CHANNEL_CACHE_MAX_ENTRIES] {
        let mut filter = ChannelFilterSet::compile_with_inspector(
            &rules,
            FilterAction::Accept,
            cache_capacity,
            None,
        )
        .unwrap();
        let started = Instant::now();
        for _ in 0..2 {
            for channel in &hot_channels {
                black_box(filter.allows(black_box(channel)));
            }
        }
        let elapsed = started.elapsed();
        let snapshot = filter.cache_snapshot();
        let operations = hot_channels.len() * 2;
        println!(
            "filter-load capacity-probe={} working_set={} operations={operations} ns_per_message={:.1} hits={} misses={} evictions={} resident={}",
            cache_capacity,
            hot_channels.len(),
            elapsed.as_nanos() as f64 / operations as f64,
            snapshot["hits"].as_u64().unwrap(),
            snapshot["misses"].as_u64().unwrap(),
            snapshot["evictions"].as_u64().unwrap(),
            snapshot["entry_count"].as_u64().unwrap(),
        );
    }

    let churn = churn_channels();
    for expose_to_http in [false, true] {
        let metrics = Metrics::new();
        let inspector = expose_to_http.then(|| (&metrics, "bench.filters".to_owned()));
        let mut filter = ChannelFilterSet::compile_with_inspector(
            &rules,
            FilterAction::Accept,
            DEFAULT_CHANNEL_CACHE_MAX_ENTRIES,
            inspector,
        )
        .unwrap();

        let started = Instant::now();
        for _ in 0..CHURN_PASSES {
            for channel in &churn {
                black_box(filter.allows(black_box(channel)));
            }
        }
        let elapsed = started.elapsed();
        let snapshot = filter.cache_snapshot();
        let operations = churn.len() * CHURN_PASSES;
        println!(
            "filter-load mode={} rules={FILTER_RULES} churn_working_set={} passes={CHURN_PASSES} operations={operations} ns_per_message={:.1} messages_per_second={:.0} hits={} misses={} evictions={} resident={}/{}",
            if expose_to_http {
                "shared-for-http"
            } else {
                "worker-local"
            },
            churn.len(),
            elapsed.as_nanos() as f64 / operations as f64,
            operations as f64 / elapsed.as_secs_f64(),
            snapshot["hits"].as_u64().unwrap(),
            snapshot["misses"].as_u64().unwrap(),
            snapshot["evictions"].as_u64().unwrap(),
            snapshot["entry_count"].as_u64().unwrap(),
            snapshot["capacity"].as_u64().unwrap(),
        );

        if expose_to_http {
            let started = Instant::now();
            let response = metrics.channel_caches_snapshot();
            let body = serde_json::to_vec(&response).unwrap();
            let snapshot_elapsed = started.elapsed();
            println!(
                "filter-load endpoint=GET_/filters resident={} response_bytes={} snapshot_and_json_ms={:.3}",
                snapshot["entry_count"].as_u64().unwrap(),
                body.len(),
                snapshot_elapsed.as_secs_f64() * 1_000.0,
            );
        }
    }
}

#[test]
#[ignore = "manual release-mode channel-policy-cache load benchmark; run with --ignored --nocapture"]
fn channel_policy_cache_load_benchmark() {
    let config = policy_load_config();
    let hot_channels = policy_hot_working_set();
    let churn = churn_channels();

    for expose_to_http in [false, true] {
        let metrics = Metrics::new();
        let inspector =
            expose_to_http.then(|| (&metrics, "bench.outputs.load.channel_policies".to_owned()));
        let compiled = CompiledChannelPolicies::new_with_cache(
            &config,
            DEFAULT_CHANNEL_CACHE_MAX_ENTRIES,
            inspector,
        );

        let started = Instant::now();
        for channel in &hot_channels {
            black_box(compiled.resolve(&config, black_box(channel)));
        }
        let cold_elapsed = started.elapsed();

        let started = Instant::now();
        for index in 0..HOT_OPERATIONS {
            let channel = &hot_channels[index % hot_channels.len()];
            black_box(compiled.resolve(&config, black_box(channel)));
        }
        let hot_elapsed = started.elapsed();
        let snapshot = compiled.cache_snapshot().unwrap();
        println!(
            "policy-load mode={} rules={FILTER_RULES} working_set={} cold_ns_per_message={:.1} hot_operations={HOT_OPERATIONS} hot_ns_per_message={:.1} hot_messages_per_second={:.0} hits={} misses={} evictions={} resident={}/{}",
            if expose_to_http {
                "shared-for-http"
            } else {
                "worker-local"
            },
            hot_channels.len(),
            cold_elapsed.as_nanos() as f64 / hot_channels.len() as f64,
            hot_elapsed.as_nanos() as f64 / HOT_OPERATIONS as f64,
            HOT_OPERATIONS as f64 / hot_elapsed.as_secs_f64(),
            snapshot["hits"].as_u64().unwrap(),
            snapshot["misses"].as_u64().unwrap(),
            snapshot["evictions"].as_u64().unwrap(),
            snapshot["entry_count"].as_u64().unwrap(),
            snapshot["capacity"].as_u64().unwrap(),
        );
    }

    for cache_capacity in [0, 4_096, DEFAULT_CHANNEL_CACHE_MAX_ENTRIES] {
        let compiled = CompiledChannelPolicies::new_with_cache(&config, cache_capacity, None);
        let started = Instant::now();
        for _ in 0..2 {
            for channel in &hot_channels {
                black_box(compiled.resolve(&config, black_box(channel)));
            }
        }
        let elapsed = started.elapsed();
        let snapshot = compiled.cache_snapshot().unwrap();
        let operations = hot_channels.len() * 2;
        println!(
            "policy-load capacity-probe={} working_set={} operations={operations} ns_per_message={:.1} hits={} misses={} evictions={} resident={}",
            cache_capacity,
            hot_channels.len(),
            elapsed.as_nanos() as f64 / operations as f64,
            snapshot["hits"].as_u64().unwrap(),
            snapshot["misses"].as_u64().unwrap(),
            snapshot["evictions"].as_u64().unwrap(),
            snapshot["entry_count"].as_u64().unwrap(),
        );
    }

    for expose_to_http in [false, true] {
        let metrics = Metrics::new();
        let inspector =
            expose_to_http.then(|| (&metrics, "bench.outputs.load.channel_policies".to_owned()));
        let compiled = CompiledChannelPolicies::new_with_cache(
            &config,
            DEFAULT_CHANNEL_CACHE_MAX_ENTRIES,
            inspector,
        );
        let started = Instant::now();
        for _ in 0..CHURN_PASSES {
            for channel in &churn {
                black_box(compiled.resolve(&config, black_box(channel)));
            }
        }
        let elapsed = started.elapsed();
        let snapshot = compiled.cache_snapshot().unwrap();
        let operations = churn.len() * CHURN_PASSES;
        println!(
            "policy-load mode={} rules={FILTER_RULES} churn_working_set={} passes={CHURN_PASSES} operations={operations} ns_per_message={:.1} messages_per_second={:.0} hits={} misses={} evictions={} resident={}/{}",
            if expose_to_http {
                "shared-for-http"
            } else {
                "worker-local"
            },
            churn.len(),
            elapsed.as_nanos() as f64 / operations as f64,
            operations as f64 / elapsed.as_secs_f64(),
            snapshot["hits"].as_u64().unwrap(),
            snapshot["misses"].as_u64().unwrap(),
            snapshot["evictions"].as_u64().unwrap(),
            snapshot["entry_count"].as_u64().unwrap(),
            snapshot["capacity"].as_u64().unwrap(),
        );
        if expose_to_http {
            let started = Instant::now();
            let response = metrics.channel_caches_snapshot();
            let body = serde_json::to_vec(&response).unwrap();
            println!(
                "policy-load endpoint=GET_/filters resident={} response_bytes={} snapshot_and_json_ms={:.3}",
                snapshot["entry_count"].as_u64().unwrap(),
                body.len(),
                started.elapsed().as_secs_f64() * 1_000.0,
            );
        }
    }
}

fn load_benchmark_rules() -> Vec<ChannelFilterRule> {
    let mut rules = Vec::with_capacity(FILTER_RULES);
    for tenant in 0..32 {
        rules.push(ChannelFilterRule {
            glob: Some(format!("feed:tenant-{tenant:04}:*")),
            action: FilterAction::Deny,
            ..Default::default()
        });
    }
    for namespace in 0..16 {
        rules.push(ChannelFilterRule {
            regex: Some(format!(r"^feed:regex-{namespace:02}:channel-[0-9]+$")),
            action: FilterAction::Accept,
            ..Default::default()
        });
    }
    for literal in 0..16 {
        rules.push(ChannelFilterRule {
            raw: Some(format!("feed:literal:{literal:02}")),
            action: FilterAction::Deny,
            ..Default::default()
        });
    }
    rules
}

fn hot_working_set() -> Vec<String> {
    let mut channels = Vec::with_capacity(HOT_WORKING_SET);
    for index in 0..4_096 {
        channels.push(format!("feed:tenant-{:04}:channel-{index:05}", index % 32));
    }
    for index in 0..2_048 {
        channels.push(format!("feed:regex-{:02}:channel-{index:05}", index % 16));
    }
    for literal in 0..16 {
        channels.push(format!("feed:literal:{literal:02}"));
    }
    while channels.len() < HOT_WORKING_SET {
        let index = channels.len();
        channels.push(format!("feed:other:channel-{index:05}"));
    }
    channels
}

fn churn_channels() -> Vec<String> {
    (0..CHURN_CHANNELS)
        .map(|index| format!("feed:unknown:channel-{index:05}"))
        .collect()
}

fn policy_load_config() -> OutputConfig {
    let mut policies = (0..FILTER_RULES)
        .map(|tenant| {
            serde_json::json!({
                "glob": format!("out:tenant-{tenant:04}:*"),
                "conflation.interval_ms": 50 + (tenant % 4) as i64 * 50,
                "deduplication.ttl_ms": 1000 + (tenant % 4) as i64 * 500,
            })
        })
        .collect::<Vec<_>>();
    policies.push(serde_json::json!({
        "default": true,
        "conflation.interval_ms": 250,
    }));
    serde_json::from_value(serde_json::json!({
        "redis": {"host": "localhost"},
        "conflation": {"interval_ms": 250},
        "channel_policies": policies,
    }))
    .expect("policy load-test config should deserialize")
}

fn policy_hot_working_set() -> Vec<String> {
    (0..HOT_WORKING_SET)
        .map(|index| format!("out:tenant-{:04}:channel-{index:05}", index % FILTER_RULES))
        .collect()
}
