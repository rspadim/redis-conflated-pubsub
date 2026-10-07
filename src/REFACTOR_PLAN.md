# Internal refactor plan

Baseline: published `v0.1.4` (`a898c40`). The refactor is development work for the next release; do not rewrite the v0.1.4 tag.

## Goal and guardrails

Reduce the responsibilities and state a developer must understand when changing one feature. First make structural moves only; do not combine them with algorithm or semantics changes.

Preserve the JSON shape and generated schema, `--check-config`, selector/default precedence, conflation cadence, TTL/group rounding and renewal, cache limits/eviction, RESP sizing/chunking, oversized policies, failure/uncertain handling, status fields/metrics, echo/Sentinel filters, Unix SIGHUP behavior, and Windows shutdown behavior. Keep internal visibility narrow (`pub(super)` where needed); do not add public APIs just to split files.

## Baseline

- `src/service.rs`: 4,284 lines, with runtime orchestration, Redis I/O, policy/glob logic, cache, RESP batching, publishing and tests together.
- `src/config.rs`: 1,841 lines, with config types, validation, manual JSON Schema constraints and tests together.
- Baseline checks: 74 Rust tests, Clippy, release build, Compose E2Es, and monitoring tests pass. The TTL Compose test is the slowest one because it uses real expiry/scheduler waits; Rust unit tests run in a fraction of a second after compilation.
- Local ELF files and `dist/v0.1.2` are user artifacts and must remain untracked.

## Target layout

```text
src/
  config.rs                 # config types and public entrypoints
  config/
    schema.rs               # JSON Schema construction/constraints
    validate.rs             # semantic validation helpers
  service.rs                # lifecycle/orchestration facade initially
  service/
    dedup.rs                # per-channel/group cache, expiry and capacity
    policy.rs               # selector resolution and schedule helpers
    policy/glob.rs          # glob parser, tokens and matcher
    input.rs                # subscription, mapping, filters and fan-out
    output.rs               # worker lifecycle and scheduler
    output/flush.rs         # queue settlement and flush paths
    publish.rs              # Redis publisher and failure settlement
    batch.rs                # batching and exact RESP sizing
```

Current state: the service/config structural refactor is implemented. `service/` separates deduplication, selector/glob matching, scheduling, input, output flush/worker, batching and publishing; `service.rs` remains the lifecycle/reload facade. All unit-test modules are outside `src/`, included as children so private access is preserved. Channel filters are additive config fields; `filter_default` defaults to accept and the earlier draft's `default_filter` spelling remains an input alias. Input rules evaluate the source channel after subscription matching; output rules evaluate the subscription-mapped channel before the output namespace. Every filter rule is evaluated and the last match wins. Cache capacities are independently configurable for input filters, each output's filters, and each output's channel-policy resolution (default 16,384 per cache; 0 disables; max 100,000). `GET /filters` optionally exposes full channel names/cache entries and is disabled by default. The optional `Dockerfile.bundle` includes Redis and Valkey 9.1.2, selects one via `KV_ENGINE`, and runs raw:6379, conflated:6380, plus the relay. Profiles and policies share a borrowed internal `ChannelOverrides` view while retaining separate JSON structs/schema. Phase 5 includes anonymized feed fixtures and CPU/memory profiling. Latest checks: 97 Rust tests passed, seven manual benchmarks ignored, Clippy/fmt passed. Filter-cache `/filters` Docker E2E, TTL/SIGHUP valid/invalid reload E2E, and direct/mixed-policy hotpath Redis/Valkey Docker E2Es passed; bundle smoke tests passed.

### Output queues grouped by interval (implemented)

Each output worker stores conflated messages as `interval_ms -> output_channel -> latest message`; interval `<= 0` messages remain in an ordered passthrough queue. A due interval flush reads only that interval's map and sends its eligible channels in bounded `MULTI/EXEC` chunks. This avoids scanning channels belonging to other schedules, and each output owns its own set of interval buckets. Direct messages are sent through a bounded in-flight window on the multiplexed Redis connection, capped by command count and encoded RESP bytes. Already queued direct items share a MULTI/EXEC; singleton messages are submitted immediately and their reply is collected asynchronously. The queue remains ordered until replies settle, while same-channel/group TTL boundaries wait for earlier results before evaluating the next message.

Post-change verification: `cargo test --locked` passed 94 tests (7 manual benchmarks ignored) and Clippy passed. The TTL/profile/group E2E passed on Redis 7.4.11, including independent 100/300 ms intervals. The mixed-policy E2E passed on Valkey 9.1.2: per output, 3,334 direct, 726 conflated and 32 TTL-cohort events were delivered; 5,214/5,213 replacements and 694/695 suppressions were recorded. The 10,000-event burst took 9.05 s to publish and 4.31 s to drain. The direct hotpath E2E published all events with empty pending queues; Valkey measured 1.00 ms serial p50, 10.99 s concurrent-burst p50 and 462 end-to-end messages/s. These are small-sample Docker/WSL measurements, not a statistically sound engine or before/after comparison.

Follow-up E2E rerun on Valkey 9.1.2: the direct workload again published all 10,314 measured messages per output and ended with empty queues; serial p50 was 0.705 ms, concurrent p50 9.55 s (p95 10.24 s), publish phase 8.86 s, drain 9.57 s, and throughput 543 messages/s. This p50 is below the previously recorded pre-pacing 12.96–13.49 s range, but these are not controlled repeated samples. The mixed workload was similar to the prior post-change run: 8.68 s publish plus 4.79 s drain; each output delivered 3,334 direct, 699–700 conflated and 32 TTL-cohort events, with 668 deduplications and 48 entries per cache. Direct/conflated p50 was 6.31–6.38 s, reflecting burst queueing rather than the 200 ms timer.

The same follow-up E2Es on Redis 7.4.11 also passed. Direct traffic delivered all 10,314 messages per output with empty queues; serial p50 was 0.918 ms, concurrent p50 9.43 s (p95 9.84 s), publish 8.87 s, drain 9.95 s, and throughput 531 messages/s. In the mixed run, publish/drain/total were 8.63/4.54/13.17 s; each output delivered 3,334 direct, 691–692 conflated and 32 TTL events, with 660 deduplications, p50 6.08–6.12 s and 48 entries per cache. Compared with Valkey's same-run results (9.55 s direct p50, 543 messages/s; 13.47 s mixed total), the measurements are close and do not establish a consistent engine winner.

After removing per-message stop-and-wait, direct E2Es still delivered all messages with zero pending at completion. Latest Redis 7.4.11 run: 10,314 messages/output, p50 10.23 s, p95 12.26 s, publish phase 9.82 s, drain 11.87 s. Valkey 9.1.2 sample: p50 11.01 s and p95 12.60 s. The current closed-loop benchmark did not demonstrate a latency reduction; it does not directly measure queue age or in-flight depth, so these results are not evidence that the concurrency window is ineffective. TTL/profile/group E2E passed on Redis after the change; the Redis mixed-policy E2E also passed (13.33 s total, 3,334 direct, 720 conflated, and 32 TTL events per output).

To localize the remaining E2E delay, status now reports per-output worker queue wait (fan-out send to worker acceptance, including policy resolution) and Redis publish RTT aggregates. In the latest Redis 7.4.11 direct run, with 10,314 messages delivered per output and no pending work, queue-wait averages were 0.084/0.033 ms (max 1.10/0.92 ms); publish RTT averages were 0.280/0.301 ms (max 9.12/9.31 ms). Despite an end-to-end p50 of 11.11 s, these figures show the internal output MPSC queue is not taking seconds to read. The benchmark ends at Python subscriber receipt and starts before input Redis PUBLISH; input Redis-to-reader delay and subscriber/client processing are not yet separated.

The remaining gap was then found in the harness, not the service: its RESP reader used an unbuffered file object (one syscall per RESP fragment) and two subscriber threads shared one GIL with four publisher threads. The benchmark now embeds a send timestamp in the payload, samples the service status timeline, and reads subscribers through a buffered file object. Under the same Redis 7.4.11 workload (10,000 events, 4 publishers, 2 outputs), the run completed with publish phase 8.85 s, output drain 0.00 s, and service end-to-end latency from publisher send to subscriber receipt of p50 1.96 ms, p95 5.25 ms and p99 14.52 ms for out-a (out-b: 1.91/5.16/10.40 ms); end-to-end throughput was 1,130 messages/s and pending ended at zero. Publisher-side input PUBLISH acknowledgement stayed at p50 3.33 ms, dominated by the closed-loop Python publisher threads. Per-output worker queue wait averaged 0.095/0.038 ms and Redis publish RTT 0.416/0.430 ms. The earlier ~10 s p50 was an artifact of the unbuffered harness reader and GIL contention, not service backlog.

## Ordered phases

### Phase 1 — Low-risk extraction (implemented)

1. Extract `DeduplicationCache`, group expiry and cache helpers into `service/dedup.rs`.
2. Extract policy resolution/scheduling into `service/policy.rs` and glob parsing/matching into `service/policy/glob.rs`.
3. Keep behavior and tests unchanged; only adjust module visibility/imports.

### Phase 2 — Split remaining service responsibilities (implemented)

Move RESP batching to `batch.rs`, Redis publishing/failure settlement to `publish.rs`, subscription/fan-out code to `input.rs`, queue/flush paths to `output/flush.rs`, and the output worker loop to `output.rs`. Keep direct and conflated publish paths explicit; do not force them into a generic strategy abstraction.

### Phase 3 — Split config implementation (implemented)

Move manual schema construction to `config/schema.rs` and semantic validation helpers to `config/validate.rs`. Keep `AppConfig::json_schema()` and `AppConfig::validate()` as entrypoints. Compare generated schema before/after and preserve all runtime-only checks (including default-last and cross-field `round_ms <= ttl_ms`).

### Channel filtering — additive feature implemented

`filters` accepts ordered `glob`, `regex`, and literal `raw`/`single`/`string` rules with `action: accept|deny`; all rules run, the last match decides, and `filter_default` is `accept` when absent. Input filtering is after subscription matching and before subscription channel mapping. Output filtering sees that subscription-mapped channel, but not the output's own prefix/suffix. Regex syntax is checked by `--check-config`; selectors compile into enum variants and their deserialized string structs are dropped after setup. `input.filter_cache_max_entries`, `outputs.<name>.filter_cache_max_entries`, and `outputs.<name>.channel_policy_cache_max_entries` independently control cache limits (default 16,384, range 0–100,000); zero disables only the associated cache while retaining filtering/policy behavior. `status.http.filters_endpoint_enabled` explicitly enables `GET /filters` and defaults off because the endpoint exposes raw channel names. When inspection is enabled, cache operations use shared synchronization; default-off operation remains local to each worker.

The selector-cost matrix below measures uncached `channel_policies` resolution; runtime now also memoizes resolved policies in an LRU. The matrix does not include policy-cache hit/miss/eviction overhead, and the paired fixture has no configured input/output filters. Empty filters still take the default-accept fast path.

The manual release-mode load tests `service::tests::filter_load::{filter_cache_load_benchmark,channel_policy_cache_load_benchmark}` use 64 selectors, an 8,192-channel hot set, and a 24,576-channel churn set; run with `cargo test --release --locked cache_load_benchmark -- --ignored --nocapture`. The filter benchmark exercises mixed glob/regex/literal selectors; the policy benchmark builds 64 ordered globs with a final default. In a representative run, 8,192 warm channels generated 8,192 misses then 8,192 hits when replayed twice; a 16,384-entry cache had no evictions. A 4,096-entry cache with that working set had no hits across the second pass and evicted 12,288 entries. With cache capacity zero, filtering and policy resolution still worked without storing entries. A 24,576-channel set replayed three times caused 73,728 misses and 57,344 evictions at the default capacity.

On 64-rule misses, selector evaluation took about 30–41 microseconds/message for filters and 28–39 microseconds/message for policies in the sampled runs. Hot cache lookups were typically around 80–120 ns/message, although the policy benchmark varied more under WSL; treat these as microbenchmark ranges, not end-to-end Redis throughput. Cache hits are substantially cheaper when the active working set fits; if it exceeds capacity and is scanned sequentially, LRU churn can erase that benefit. A full filter cache serialized to about 950 KB in roughly 55 ms; a full policy cache with resolved values serialized to about 2.2 MB in roughly 90–140 ms. JSON generation happens after copying entries out from under each cache lock.

The full-stack `compose.hotpath.test.yml` benchmark measures before input `PUBLISH` to receipt on both output subscribers. It warms 64 channels, uses direct output (conflation/dedup TTL zero), and drives two outputs with 4 concurrent publishers. Across two local runs, Valkey 9.1.2 showed serial p50 1.10–1.67 ms and concurrent-burst p50 12.96–13.49 s; Redis 7.4.11 showed serial p50 0.91–1.23 ms and concurrent-burst p50 11.55–14.54 s. All messages were published, with pending queues empty at completion. The publisher phase was about 11–13 s and output drain another 13–16 s, indicating substantial queueing in this direct-PUBLISH workload; the large tail is queueing, not a cache lookup. These are small-sample local Docker/WSL results, not a statistically sound engine comparison. The benchmark defaults to Valkey 9.1.2 and accepts `HOTPATH_KV_IMAGE`/`HOTPATH_KV_CLI` overrides for Redis.

The mixed-policy E2E (`docker_hotpath_policy_mix.py`) sends 10,000 events split roughly 33/33/34 across direct, 200 ms conflation, and 200 ms conflation with a 5 s TTL. The TTL cohort deliberately repeats the same payload per channel. In a prior Valkey 9.1.2 run, each output published 3,334 direct, 800 conflated, and 32 TTL-cohort events; counters showed 5,066 conflated replacements and 768 TTL suppressions. The post-pacing run is recorded above. All five caches held 48 channels, with no evictions. Direct/conflated delivery latency under this burst reflects queueing, not the configured 200 ms timer. With unique payloads in the TTL cohort, TTL suppressions would instead be near zero.

`paired_fixture_100x_filter_and_policy_cache_load` replays 100 times the 60-second fixture's raw count: 12,448,500 synthetic message lookups across 7,428 synthetic channels, two outputs, input/output filters, and per-output policies. It ran in 18.8 s (about 662k raw lookups/s) in-process. Each cache recorded one miss per channel it saw, about 99.94% hits thereafter, and zero evictions at the 16,384 default. The fixture contains aggregates, not raw channel names, payloads or event order: top-64 message counts are preserved, remaining raw counts are distributed uniformly across the other channels, channel strings are synthetic, and the schedule is shuffled then repeated. This validates cache behavior under an approximate 100x CPU workload, not Redis/network throughput or payload/conflation behavior.

### Phase 4 — Internal model simplification (partial)

`CompiledChannelPolicies` compiles glob tokens once per output worker and preserves ordered matching/default behavior. `ChannelProfile` and `ChannelPolicy` use a shared borrowed `ChannelOverrides` view for validation and resolution, but retain separate serde structs and the existing dotted JSON keys/schema. No `flatten` or custom deserialization was introduced.

### Phase 5 — Measured CPU, memory and test-time work

Initial release-mode microbenchmark: `tests/unit/service_policy_bench.rs` runs 1/8/32/64 ordered glob selectors, targeting the last selector, with three samples per path. The pre-change medians were 1.74/14.22/57.34/114.69 µs per resolution; compiled selectors with reused matcher buffers measured 0.88/7.39/26.97/61.36 µs (roughly 1.9–2.1× in this synthetic workload). These separate-run numbers are indicative, not a traffic benchmark; same-run dynamic-vs-compiled comparisons showed an additional 1.1–1.6× from precompiling selectors. The benchmark process peaked at 3.8 MiB RSS; it is not a service-memory measurement. Scratch buffers are local to a resolution (O(channel length)); compiled tokens live for the worker lifetime.

The anonymized fixture `tests/fixtures/redis_feed_profile_60s.json` was captured read-only for 60 s using the existing `PSUBSCRIBE *`; no publisher was started and payload contents/raw channel names were never written. The fixture also omits the host and exact capture time; channel values are all `x` masks with byte lengths retained. In-memory channel accounting was capped at 8,192 (lower bound; 20 events fell beyond the cap). It records 118,727 messages (1,979/s), 2,355,634 payload bytes, 30 Sentinel-filtered messages, and 64 frequent channel-length samples. 118,439 messages (99.76%) were <=64 B. The local config has zero channel policies, so anonymized samples were replayed against synthetic exact-glob selectors weighted by their captured frequencies. Dynamic/compiled medians were 2.92/2.73, 17.15/13.62, 44.20/40.57 and 64.11/55.08 µs for 1/8/32/64 selectors (1.07–1.26× same-run speedup). The 60-second Redis subscription has ended; future replay is offline from the redacted fixture.

The paired fixture `tests/fixtures/redis_feed_pair_profile_60s.json` concurrently captured ports 6379 (raw) and 6380 (conflated) for 60 s on the same configured host; no output clients were started. It observed 124,485 raw messages (2,075/s) and 38,287 conflated messages (638/s), a 30.8% message ratio, with all 7,428 counted channel names present on both ports (no raw-only or conflated-only channels). Payload totals were 2,390,228 B raw and 1,631,254 B conflated. The fixture stores only aggregates and `x`-masked lengths; no payloads, host or exact timestamp. Replaying the top 64 anonymized channel rows, weighted by combined raw/conflated counts, gave dynamic/compiled medians of 3.82/3.13, 27.97/22.78, 72.12/51.94 and 60.84/46.04 µs for 1/8/32/64 synthetic selectors (1.22–1.39×). This checks channel/count correspondence, not payload-by-payload equivalence.

The policy-cost matrix uses the top-64 raw-channel rows (54.9% of captured raw events) at 2,075 input messages/s. Latency is per output-resolution:

| Explicit glob rules | First hit | Last hit | No match |
|---:|---:|---:|---:|
| 0 | 0.018 ?s | ? | ? |
| 1 | 0.204 ?s | 0.206 ?s | 2.541 ?s |
| 8 | 0.192 ?s | 20.682 ?s | 31.742 ?s |
| 32 | 0.280 ?s | 63.427 ?s | 62.093 ?s |
| 64 | 0.170 ?s | 105.633 ?s | 136.073 ?s |

At 64 selectors, that is about 0.04% of one CPU core per output for an early hit, 21.9% for a last hit and 28.2% for no match; two outputs with the same worst-case rules would use about 56.5% of one core. The compiled selector heap estimate is 107?109 KiB/output for 64 rules, excluding allocator metadata and source config strings; matcher scratch is 78 logical bytes for the sampled maximum channel length (capacity rounding excluded). Massif's whole benchmark-process peak rose from 107,072 B with no selectors to 344,696 B with 64 no-match selectors (+237,624 B, including fixture/config/harness allocations). Compilation of 64 selectors was about 25?33 ?s/output at worker setup/reload. These are synthetic glob rules; selector position dominates the result.

For 64 selectors on the paired fixture, Callgrind recorded 286.4M vs 238.2M instruction references for dynamic vs compiled lookup (-16.8%). After `perf` was installed in WSL, `/usr/bin/perf stat -r 3` measured 1.791B vs 1.396B user cycles (-22.0%), 5.709B vs 4.730B instructions (-17.1%), and 450K vs 542K cache misses (compiled had more; no locality gain claimed). Repeated test elapsed times were 0.833 s vs 0.610 s. `/usr/bin/perf` was 6.8.12 on WSL kernel 6.6. `llvm-mca` on isolated assembly reported 79.5 cycles block throughput for the DP matcher, 85.7 for the dynamic parser/call wrapper, and 1.2 for the compiled wrapper. MCA is static and omits allocator bodies/data-dependent paths, so it is not an end-to-end latency estimate.
Continue measuring realistic policy distributions, payload sizes and output rates before further optimization. In particular, an early selector match should be measured alongside the deliberate last-match case. Enums alone are not a performance goal for arbitrary channel/group names.

Warm Rust unit-test execution is about 0.03–0.05 s after compilation. The TTL/SIGHUP Compose E2E took 18.9 s with its image prebuilt, including Compose startup/shutdown. After the module splits, one run hit a B/C ordering assertion at the 50 ms window boundary; two subsequent runs passed without code changes. Keep this E2E timing-sensitive behavior visible and measure other scenarios with the same method before changing sleeps. Retain at least one full Redis E2E for SIGHUP with changed config.

### Follow-up simplification review

Moving test modules outside `src/` reduced `service.rs` from 2,674 to 424 lines and `config.rs` from 1,450 to 499 before adding filter config support; current sizes are about 433 and 531 lines. The glob matcher, output flush paths, config validation and large service/config test modules are now separated. The optional shared override type remains deferred: the anonymized production config currently has no channel policies, so there is no measured benefit to justify expanding that model. Keep direct and conflated publishing separate; no additional generic abstraction is justified by the feed data.

## Gate for every phase

```sh
cargo fmt --all -- --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --locked
cargo build --release --locked
```

Also run `--config-json-schema`, `--check-config` on examples, relevant Docker E2Es and Zabbix validation. Check `git diff --check`, compare external JSON/schema/metrics behavior, and stage paths explicitly—never add local ELF files or `dist/` artifacts.
