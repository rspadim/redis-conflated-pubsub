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
const PAIRED_FIXTURE_REPLAY_MULTIPLIER: usize = 100;

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

#[test]
#[ignore = "manual 100x anonymized-feed cache replay; run with --ignored --nocapture"]
fn paired_fixture_100x_filter_and_policy_cache_load() {
    let (channels, event_order, raw_messages_per_capture) = paired_raw_fixture_workload();
    let input_filter =
        ChannelFilterSet::compile(&tenant_filter_rules("", Some(63)), FilterAction::Accept)
            .unwrap();
    let mut input_filter = input_filter;
    let mut output_filters = [
        ChannelFilterSet::compile(
            &tenant_filter_rules("mapped:", Some(62)),
            FilterAction::Accept,
        )
        .unwrap(),
        ChannelFilterSet::compile(
            &tenant_filter_rules("mapped:", Some(61)),
            FilterAction::Accept,
        )
        .unwrap(),
    ];
    let output_configs = [
        policy_load_config_for_prefix("out-a:"),
        policy_load_config_for_prefix("out-b:"),
    ];
    let policy_caches = [
        CompiledChannelPolicies::new_with_cache(
            &output_configs[0],
            DEFAULT_CHANNEL_CACHE_MAX_ENTRIES,
            None,
        ),
        CompiledChannelPolicies::new_with_cache(
            &output_configs[1],
            DEFAULT_CHANNEL_CACHE_MAX_ENTRIES,
            None,
        ),
    ];
    let mapped_channels = channels
        .iter()
        .map(|channel| map_output_channel("", "mapped:", channel, ":source", ""))
        .collect::<Vec<_>>();
    let output_channels = [
        channels
            .iter()
            .map(|channel| map_output_channel("out-a:", "mapped:", channel, ":source", ":dest"))
            .collect::<Vec<_>>(),
        channels
            .iter()
            .map(|channel| map_output_channel("out-b:", "mapped:", channel, ":source", ":dest"))
            .collect::<Vec<_>>(),
    ];

    let operations = raw_messages_per_capture * PAIRED_FIXTURE_REPLAY_MULTIPLIER;
    let mut accepted_input = 0u64;
    let mut output_filter_evaluations = [0u64; 2];
    let mut policy_resolutions = [0u64; 2];
    let started = Instant::now();
    for _ in 0..PAIRED_FIXTURE_REPLAY_MULTIPLIER {
        for &channel_index in &event_order {
            let source_channel = &channels[channel_index];
            if !input_filter.allows(black_box(source_channel)) {
                continue;
            }
            accepted_input += 1;
            for output_index in 0..2 {
                output_filter_evaluations[output_index] += 1;
                if !output_filters[output_index].allows(black_box(&mapped_channels[channel_index]))
                {
                    continue;
                }
                policy_resolutions[output_index] += 1;
                black_box(policy_caches[output_index].resolve(
                    &output_configs[output_index],
                    &output_channels[output_index][channel_index],
                ));
            }
        }
    }
    let elapsed = started.elapsed();
    let input_cache = input_filter.cache_snapshot();
    println!(
        "paired-100x fixture_raw_messages_per_capture={raw_messages_per_capture} replay_factor={PAIRED_FIXTURE_REPLAY_MULTIPLIER} synthetic_channels={} elapsed_s={:.3} raw_messages_per_second={:.0} accepted_input={} input_cache_hits={} input_cache_misses={} input_cache_evictions={} input_cache_entries={}/{}",
        channels.len(),
        elapsed.as_secs_f64(),
        operations as f64 / elapsed.as_secs_f64(),
        accepted_input,
        input_cache["hits"].as_u64().unwrap(),
        input_cache["misses"].as_u64().unwrap(),
        input_cache["evictions"].as_u64().unwrap(),
        input_cache["entry_count"].as_u64().unwrap(),
        input_cache["capacity"].as_u64().unwrap(),
    );
    for output_index in 0..2 {
        let filter_cache = output_filters[output_index].cache_snapshot();
        let policy_cache = policy_caches[output_index].cache_snapshot().unwrap();
        println!(
            "paired-100x output={} filter_evaluations={} policy_resolutions={} filter_hits={} filter_misses={} filter_evictions={} filter_entries={} policy_hits={} policy_misses={} policy_evictions={} policy_entries={}",
            output_index,
            output_filter_evaluations[output_index],
            policy_resolutions[output_index],
            filter_cache["hits"].as_u64().unwrap(),
            filter_cache["misses"].as_u64().unwrap(),
            filter_cache["evictions"].as_u64().unwrap(),
            filter_cache["entry_count"].as_u64().unwrap(),
            policy_cache["hits"].as_u64().unwrap(),
            policy_cache["misses"].as_u64().unwrap(),
            policy_cache["evictions"].as_u64().unwrap(),
            policy_cache["entry_count"].as_u64().unwrap(),
        );
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
    policy_load_config_for_prefix("out:")
}

fn policy_load_config_for_prefix(output_prefix: &str) -> OutputConfig {
    let mut policies = (0..FILTER_RULES)
        .map(|tenant| {
            serde_json::json!({
                "glob": format!("{output_prefix}mapped:g{tenant:02x}:*:source:dest"),
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
        .map(|index| {
            format!(
                "out:mapped:g{:02x}:channel-{index:05}:source:dest",
                index % FILTER_RULES
            )
        })
        .collect()
}

fn tenant_filter_rules(
    mapping_prefix: &str,
    denied_tenant: Option<usize>,
) -> Vec<ChannelFilterRule> {
    (0..FILTER_RULES)
        .map(|tenant| ChannelFilterRule {
            glob: Some(format!("{mapping_prefix}g{tenant:02x}:*")),
            action: if Some(tenant) == denied_tenant {
                FilterAction::Deny
            } else {
                FilterAction::Accept
            },
            ..Default::default()
        })
        .collect()
}

fn paired_raw_fixture_workload() -> (Vec<String>, Vec<usize>, usize) {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../fixtures/redis_feed_pair_profile_60s.json"
    ))
    .expect("paired feed fixture should parse");
    let channel_samples = fixture["channels"]["top_64_length_samples"]
        .as_array()
        .unwrap();
    let channel_count = fixture["channels"]["distinct_counted"].as_u64().unwrap() as usize;
    let raw_messages = fixture["ports"]["raw"]["messages"].as_u64().unwrap() as usize;
    let mut per_channel_counts = channel_samples
        .iter()
        .map(|sample| sample["raw_messages"].as_u64().unwrap() as usize)
        .collect::<Vec<_>>();
    let untracked_tail = channel_count - per_channel_counts.len();
    let tail_messages = raw_messages - per_channel_counts.iter().sum::<usize>();
    let tail_base = tail_messages / untracked_tail;
    let tail_remainder = tail_messages % untracked_tail;
    per_channel_counts
        .extend((0..untracked_tail).map(|index| tail_base + usize::from(index < tail_remainder)));

    let channels = (0..channel_count)
        .map(|index| {
            let length = if index < channel_samples.len() {
                channel_samples[index]["channel_length_bytes"]
                    .as_u64()
                    .unwrap() as usize
            } else {
                channel_samples[(index - channel_samples.len()) % channel_samples.len()]
                    ["channel_length_bytes"]
                    .as_u64()
                    .unwrap() as usize
            };
            let prefix = format!("g{:02x}:c{:04x}", index % FILTER_RULES, index);
            assert!(
                prefix.len() <= length,
                "synthetic channel prefix must fit its sample length"
            );
            format!("{prefix}{}", "x".repeat(length - prefix.len()))
        })
        .collect::<Vec<_>>();

    let mut event_order = Vec::with_capacity(raw_messages);
    for (channel_index, count) in per_channel_counts.into_iter().enumerate() {
        event_order.extend(std::iter::repeat_n(channel_index, count));
    }
    assert_eq!(event_order.len(), raw_messages);

    let mut state = 0xA076_1D64_78BD_642F_u64;
    for index in (1..event_order.len()).rev() {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        event_order.swap(index, (state as usize) % (index + 1));
    }
    (channels, event_order, raw_messages)
}
