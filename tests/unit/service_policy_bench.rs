use std::{hint::black_box, time::Instant};

use super::{CompiledChannelPolicies, resolve_channel_policy};
use crate::config::OutputConfig;

#[test]
#[ignore = "manual release-mode benchmark; run with --ignored --nocapture"]
fn policy_resolution_manual_microbenchmark() {
    for selector_count in [1usize, 8, 32, 64] {
        let config = output_with_globs(selector_count);
        let channel = format!("feed:tenant-{:04}:payload-0123456789", selector_count - 1);
        let compiled = CompiledChannelPolicies::new(&config);
        let iterations = (200_000 / selector_count).max(10_000);
        let mut dynamic_samples = Vec::with_capacity(3);
        let mut compiled_samples = Vec::with_capacity(3);

        for _ in 0..3 {
            let started = Instant::now();
            for _ in 0..iterations {
                black_box(resolve_channel_policy(
                    black_box(&config),
                    black_box(&channel),
                ));
            }
            dynamic_samples.push(started.elapsed());

            let started = Instant::now();
            for _ in 0..iterations {
                black_box(compiled.resolve(black_box(&config), black_box(&channel)));
            }
            compiled_samples.push(started.elapsed());
        }
        dynamic_samples.sort_unstable();
        compiled_samples.sort_unstable();
        let dynamic_median = dynamic_samples[1];
        let compiled_median = compiled_samples[1];
        println!(
            "policy-resolution selectors={selector_count} iterations={iterations} dynamic_ns_per_message={:.1} compiled_ns_per_message={:.1} speedup={:.2}x",
            dynamic_median.as_nanos() as f64 / iterations as f64,
            compiled_median.as_nanos() as f64 / iterations as f64,
            dynamic_median.as_secs_f64() / compiled_median.as_secs_f64(),
        );
    }
}

fn output_with_globs(selector_count: usize) -> OutputConfig {
    let patterns = (0..selector_count)
        .map(|index| format!("feed:tenant-{index:04}:*"))
        .collect::<Vec<_>>();
    output_with_patterns(&patterns)
}

fn output_with_patterns(patterns: &[String]) -> OutputConfig {
    let mut policies = patterns
        .iter()
        .map(|pattern| {
            serde_json::json!({
                "glob": pattern,
                "conflation.interval_ms": 50,
                "deduplication.group": "shared",
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
        "deduplication_groups": {
            "shared": {"ttl_ms": 60_000, "round_ms": 0},
        },
        "channel_policies": policies,
    }))
    .expect("benchmark output config should deserialize")
}

fn cost_config(patterns: &[String]) -> OutputConfig {
    let mut policies = patterns
        .iter()
        .map(|pattern| {
            serde_json::json!({
                "glob": pattern,
                "conflation.interval_ms": 50,
            })
        })
        .collect::<Vec<_>>();
    if !policies.is_empty() {
        policies.push(serde_json::json!({
            "default": true,
            "conflation.interval_ms": 250,
        }));
    }
    serde_json::from_value(serde_json::json!({
        "redis": {"host": "localhost"},
        "conflation": {"interval_ms": 250},
        "channel_policies": policies,
    }))
    .expect("policy cost config should deserialize")
}

#[test]
fn anonymized_feed_fixture_is_redacted_and_consistent() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../tests/fixtures/redis_feed_profile_60s.json"
    ))
    .expect("anonymized feed fixture should be valid JSON");
    assert_eq!(fixture["schema_version"], 1);
    assert!(
        !fixture["privacy"]["payload_contents_recorded"]
            .as_bool()
            .unwrap()
    );
    assert!(
        !fixture["privacy"]["raw_channel_names_recorded"]
            .as_bool()
            .unwrap()
    );

    let messages = fixture["messages"]["count"]
        .as_u64()
        .expect("message count should be an integer");
    let payload_buckets = &fixture["messages"]["payload_size_buckets"];
    assert_eq!(
        [
            "0_64_bytes",
            "65_256_bytes",
            "257_1024_bytes",
            "over_1024_bytes"
        ]
        .into_iter()
        .map(|key| {
            payload_buckets[key]
                .as_u64()
                .expect("bucket count should be an integer")
        })
        .sum::<u64>(),
        messages
    );

    let channels = &fixture["channels"];
    let distinct_counted = channels["distinct_counted"]
        .as_u64()
        .expect("channel count should be an integer");
    let length_buckets = &channels["length_buckets_bytes"];
    assert_eq!(
        ["1_16", "17_32", "33_64", "over_64"]
            .into_iter()
            .map(|key| {
                length_buckets[key]
                    .as_u64()
                    .expect("bucket count should be an integer")
            })
            .sum::<u64>(),
        distinct_counted
    );
    let samples = channels["top_64_anonymized_shapes"]
        .as_array()
        .expect("channel samples should be an array");
    assert!(samples.len() <= 64);
    for sample in samples {
        let fields = sample
            .as_object()
            .expect("channel sample should be an object");
        assert_eq!(
            fields.len(),
            5,
            "sample must not contain raw channel/payload data"
        );
        let shape = sample["shape"].as_str().expect("shape should be text");
        let channel_bytes = sample["channel_length_bytes"]
            .as_u64()
            .expect("channel length should be an integer") as usize;
        assert_eq!(shape.len(), channel_bytes);
        assert!(shape.chars().all(|character| character == 'x'));
    }
}

#[test]
fn paired_feed_fixture_is_redacted_and_consistent() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../tests/fixtures/redis_feed_pair_profile_60s.json"
    ))
    .expect("paired feed fixture should be valid JSON");
    assert!(
        !fixture["privacy"]["payload_contents_recorded"]
            .as_bool()
            .unwrap()
    );
    assert!(
        !fixture["privacy"]["raw_channel_names_recorded"]
            .as_bool()
            .unwrap()
    );
    assert!(!fixture["privacy"]["host_recorded"].as_bool().unwrap());
    assert!(
        !fixture["privacy"]["exact_capture_time_recorded"]
            .as_bool()
            .unwrap()
    );

    for role in ["raw", "conflated"] {
        let port = &fixture["ports"][role];
        let message_count = port["messages"]
            .as_u64()
            .expect("port message count should be an integer");
        let buckets = &port["payload_size_buckets"];
        assert_eq!(
            [
                "0_64_bytes",
                "65_256_bytes",
                "257_1024_bytes",
                "over_1024_bytes"
            ]
            .into_iter()
            .map(|key| buckets[key]
                .as_u64()
                .expect("bucket count should be an integer"))
            .sum::<u64>(),
            message_count
        );
    }

    let samples = fixture["channels"]["top_64_length_samples"]
        .as_array()
        .expect("paired channel samples should be an array");
    assert!(samples.len() <= 64);
    for sample in samples {
        let fields = sample.as_object().expect("sample should be an object");
        assert_eq!(
            fields.len(),
            7,
            "sample must not contain raw channel/payload data"
        );
        let shape = sample["shape"].as_str().expect("shape should be text");
        let channel_bytes = sample["channel_length_bytes"]
            .as_u64()
            .expect("channel length should be an integer") as usize;
        assert_eq!(shape.len(), channel_bytes);
        assert!(shape.chars().all(|character| character == 'x'));
    }
}

#[test]
#[ignore = "manual replay benchmark using the anonymized Redis fixture"]
fn anonymized_feed_fixture_policy_benchmark() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../tests/fixtures/redis_feed_profile_60s.json"
    ))
    .expect("anonymized feed fixture should be valid JSON");
    assert_eq!(fixture["privacy"]["raw_channel_names_recorded"], false);
    assert_eq!(fixture["privacy"]["payload_contents_recorded"], false);

    let samples = fixture["channels"]["top_64_anonymized_shapes"]
        .as_array()
        .expect("fixture channel sample should be an array")
        .iter()
        .enumerate()
        .map(|(index, sample)| {
            let shape = sample["shape"]
                .as_str()
                .expect("anonymized channel shape should be text");
            let messages = sample["messages"]
                .as_u64()
                .expect("sample message count should be an integer");
            (format!("{shape}:sample-{index:03}"), messages)
        })
        .collect::<Vec<_>>();
    let sample_messages = samples.iter().map(|(_, count)| count).sum::<u64>();
    assert!(!samples.is_empty() && sample_messages > 0);

    for selector_count in [1usize, 8, 32, 64] {
        let patterns = (0..selector_count)
            .map(|index| {
                samples
                    .get(index)
                    .map(|(channel, _)| channel.clone())
                    .unwrap_or_else(|| format!("never-match:{index:03}"))
            })
            .collect::<Vec<_>>();
        let config = output_with_patterns(&patterns);
        let compiled = CompiledChannelPolicies::new(&config);
        let weights = samples
            .iter()
            .map(|(_, count)| ((*count as u128 * 5_000 / sample_messages as u128) as usize).max(1))
            .collect::<Vec<_>>();
        let operations = weights.iter().sum::<usize>();
        let mut dynamic_samples = Vec::with_capacity(3);
        let mut compiled_samples = Vec::with_capacity(3);

        for _ in 0..3 {
            let started = Instant::now();
            for ((channel, _), weight) in samples.iter().zip(&weights) {
                for _ in 0..*weight {
                    black_box(resolve_channel_policy(&config, black_box(channel)));
                }
            }
            dynamic_samples.push(started.elapsed());

            let started = Instant::now();
            for ((channel, _), weight) in samples.iter().zip(&weights) {
                for _ in 0..*weight {
                    black_box(compiled.resolve(&config, black_box(channel)));
                }
            }
            compiled_samples.push(started.elapsed());
        }
        dynamic_samples.sort_unstable();
        compiled_samples.sort_unstable();
        let dynamic_median = dynamic_samples[1];
        let compiled_median = compiled_samples[1];
        println!(
            "anonymized-feed-replay selectors={selector_count} channels={} weighted_messages={operations} dynamic_ns_per_message={:.1} compiled_ns_per_message={:.1} speedup={:.2}x",
            samples.len(),
            dynamic_median.as_nanos() as f64 / operations as f64,
            compiled_median.as_nanos() as f64 / operations as f64,
            dynamic_median.as_secs_f64() / compiled_median.as_secs_f64(),
        );
    }
}

#[test]
#[ignore = "manual replay benchmark using the anonymized raw/conflated fixture"]
fn paired_feed_fixture_policy_benchmark() {
    let mode = std::env::var("POLICY_BENCH_MODE").unwrap_or_else(|_| "both".to_owned());
    assert!(matches!(mode.as_str(), "both" | "dynamic" | "compiled"));
    let budget = std::env::var("POLICY_BENCH_BUDGET")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(5_000);
    let selector_counts = std::env::var("POLICY_BENCH_SELECTOR_COUNT")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .map(|count| vec![count])
        .unwrap_or_else(|| vec![1, 8, 32, 64]);
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../tests/fixtures/redis_feed_pair_profile_60s.json"
    ))
    .expect("paired feed fixture should be valid JSON");
    let samples = fixture["channels"]["top_64_length_samples"]
        .as_array()
        .expect("paired channel samples should be an array")
        .iter()
        .enumerate()
        .map(|(index, sample)| {
            let shape = sample["shape"]
                .as_str()
                .expect("anonymized shape should be text");
            let raw_messages = sample["raw_messages"]
                .as_u64()
                .expect("raw count should be an integer");
            let conflated_messages = sample["conflated_messages"]
                .as_u64()
                .expect("conflated count should be an integer");
            (
                format!("{shape}:sample-{index:03}"),
                raw_messages + conflated_messages,
            )
        })
        .collect::<Vec<_>>();
    let total_sample_messages = samples.iter().map(|(_, count)| count).sum::<u64>();
    assert!(!samples.is_empty() && total_sample_messages > 0);

    for selector_count in selector_counts {
        let patterns = (0..selector_count)
            .map(|index| {
                samples
                    .get(index)
                    .map(|(channel, _)| channel.clone())
                    .unwrap_or_else(|| format!("never-match:{index:03}"))
            })
            .collect::<Vec<_>>();
        let config = output_with_patterns(&patterns);
        let compiled = (mode != "dynamic").then(|| CompiledChannelPolicies::new(&config));
        let weights = samples
            .iter()
            .map(|(_, count)| {
                ((*count as u128 * budget as u128 / total_sample_messages as u128) as usize).max(1)
            })
            .collect::<Vec<_>>();
        let operations = weights.iter().sum::<usize>();
        let mut dynamic_samples = Vec::with_capacity(3);
        let mut compiled_samples = Vec::with_capacity(3);

        for _ in 0..3 {
            if mode != "compiled" {
                let started = Instant::now();
                for ((channel, _), weight) in samples.iter().zip(&weights) {
                    for _ in 0..*weight {
                        black_box(resolve_channel_policy(&config, black_box(channel)));
                    }
                }
                dynamic_samples.push(started.elapsed());
            }

            if let Some(compiled) = &compiled {
                let started = Instant::now();
                for ((channel, _), weight) in samples.iter().zip(&weights) {
                    for _ in 0..*weight {
                        black_box(compiled.resolve(&config, black_box(channel)));
                    }
                }
                compiled_samples.push(started.elapsed());
            }
        }
        let dynamic_median = if dynamic_samples.is_empty() {
            None
        } else {
            dynamic_samples.sort_unstable();
            Some(dynamic_samples[1])
        };
        let compiled_median = if compiled_samples.is_empty() {
            None
        } else {
            compiled_samples.sort_unstable();
            Some(compiled_samples[1])
        };
        println!(
            "paired-feed-replay mode={mode} selectors={selector_count} channels={} weighted_messages={operations} dynamic_ns_per_message={:?} compiled_ns_per_message={:?} speedup={:?}",
            samples.len(),
            dynamic_median.map(|elapsed| elapsed.as_nanos() as f64 / operations as f64),
            compiled_median.map(|elapsed| elapsed.as_nanos() as f64 / operations as f64),
            dynamic_median
                .zip(compiled_median)
                .map(|(dynamic, compiled)| { dynamic.as_secs_f64() / compiled.as_secs_f64() }),
        );
    }
}

#[test]
#[ignore = "manual policy CPU/memory/latency estimate from the paired fixture"]
fn paired_feed_policy_cost_matrix() {
    let budget = std::env::var("POLICY_COST_BUDGET")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(5_000);
    let selector_counts = std::env::var("POLICY_COST_SELECTOR_COUNT")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .map(|count| vec![count])
        .unwrap_or_else(|| vec![0, 1, 8, 32, 64]);
    let selected_case = std::env::var("POLICY_COST_CASE").ok();
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../tests/fixtures/redis_feed_pair_profile_60s.json"
    ))
    .expect("paired feed fixture should be valid JSON");
    let raw_rate = fixture["ports"]["raw"]["messages_per_second"]
        .as_f64()
        .expect("raw rate should be numeric");
    let raw_messages = fixture["ports"]["raw"]["messages"]
        .as_u64()
        .expect("raw count should be an integer");
    let samples = fixture["channels"]["top_64_length_samples"]
        .as_array()
        .expect("paired channel samples should be an array")
        .iter()
        .enumerate()
        .map(|(index, sample)| {
            let shape = sample["shape"]
                .as_str()
                .expect("anonymized shape should be text");
            let messages = sample["raw_messages"]
                .as_u64()
                .expect("raw sample count should be an integer");
            (format!("{shape}:sample-{index:03}"), messages)
        })
        .collect::<Vec<_>>();
    let sample_messages = samples.iter().map(|(_, count)| count).sum::<u64>();
    let weights = samples
        .iter()
        .map(|(_, count)| {
            ((*count as u128 * budget as u128 / sample_messages as u128) as usize).max(1)
        })
        .collect::<Vec<_>>();
    let operations = weights.iter().sum::<usize>();
    let max_channel_length = samples
        .iter()
        .map(|(channel, _)| channel.len())
        .max()
        .expect("paired fixture has channel samples");
    let matcher_scratch_bound = 2 * (max_channel_length + 1) * std::mem::size_of::<bool>();
    let mut baseline_samples = Vec::with_capacity(3);
    let baseline_config = cost_config(&[]);
    let baseline_policies = CompiledChannelPolicies::new(&baseline_config);
    for _ in 0..3 {
        let started = Instant::now();
        for ((channel, _), weight) in samples.iter().zip(&weights) {
            for _ in 0..*weight {
                black_box(baseline_policies.resolve(&baseline_config, black_box(channel)));
            }
        }
        baseline_samples.push(started.elapsed());
    }
    baseline_samples.sort_unstable();
    let baseline_ns = baseline_samples[1].as_nanos() as f64 / operations as f64;

    for selector_count in selector_counts {
        let cases: &[&str] = if selector_count == 0 {
            &["no-policies"]
        } else {
            &["early-match", "last-match", "no-match"]
        };
        for case in cases {
            if selected_case
                .as_deref()
                .is_some_and(|selected| selected != *case)
            {
                continue;
            }
            let nonmatching_pattern = |index: usize| {
                format!(
                    "z{index:03}{}*",
                    "x".repeat(samples[index % samples.len()].0.len())
                )
            };
            let patterns = match *case {
                "no-policies" => Vec::new(),
                "early-match" => std::iter::once("x*".to_owned())
                    .chain((1..selector_count).map(nonmatching_pattern))
                    .collect::<Vec<_>>(),
                "last-match" => (0..selector_count.saturating_sub(1))
                    .map(nonmatching_pattern)
                    .chain(std::iter::once("x*".to_owned()))
                    .collect::<Vec<_>>(),
                "no-match" => (0..selector_count)
                    .map(nonmatching_pattern)
                    .collect::<Vec<_>>(),
                _ => unreachable!("benchmark case is enumerated above"),
            };
            let config = cost_config(&patterns);
            let compiled = CompiledChannelPolicies::new(&config);
            let compiled_heap_bytes = compiled.estimated_heap_bytes();
            let mut latency_samples = Vec::with_capacity(3);
            let mut compile_samples = Vec::with_capacity(3);

            for _ in 0..3 {
                let started = Instant::now();
                for ((channel, _), weight) in samples.iter().zip(&weights) {
                    for _ in 0..*weight {
                        black_box(compiled.resolve(&config, black_box(channel)));
                    }
                }
                latency_samples.push(started.elapsed());

                let started = Instant::now();
                for _ in 0..100 {
                    black_box(CompiledChannelPolicies::new(&config));
                }
                compile_samples.push(started.elapsed());
            }
            latency_samples.sort_unstable();
            compile_samples.sort_unstable();
            let latency_ns = latency_samples[1].as_nanos() as f64 / operations as f64;
            let compile_us = compile_samples[1].as_secs_f64() * 10_000.0;
            let cpu_one_output_pct = latency_ns * raw_rate / 10_000_000.0;
            println!(
                "policy-cost selectors={} case={case} latency_us={:.3} delta_vs_no_policies_us={:.3} cpu_pct_at_raw_rate_one_output={:.2} cpu_pct_two_outputs={:.2} compiled_heap_bytes={compiled_heap_bytes} matcher_scratch_logical_bytes={matcher_scratch_bound} compile_us={compile_us:.2}",
                patterns.len(),
                latency_ns / 1_000.0,
                (latency_ns - baseline_ns).max(0.0) / 1_000.0,
                cpu_one_output_pct,
                cpu_one_output_pct * 2.0,
            );
        }
    }
    println!(
        "paired policy cost workload: raw_messages={raw_messages} raw_rate={raw_rate:.1}/s top64_sampled_messages={sample_messages} top64_raw_coverage_pct={:.1} weighted_operations={operations} no_policy_baseline_us={:.3}",
        sample_messages as f64 * 100.0 / raw_messages as f64,
        baseline_ns / 1_000.0,
    );
}
