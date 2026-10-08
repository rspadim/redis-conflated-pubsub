# Design

Architecture, runtime behavior, defaults and reference measurements for `redis-conflated-pubsub` 0.2.0. This document describes the current version; see [`../CHANGELOG.md`](../CHANGELOG.md) for change history.

## Purpose

Forward raw Redis Pub/Sub messages from `SUBSCRIBE`/`PSUBSCRIBE` inputs to one or more named outputs. Each output independently maps channels, filters messages, conflates to the latest value per channel and interval, deduplicates identical payloads within a TTL, and publishes to its own Redis endpoint. Payloads stay raw bytes; there is no disk spool.

## Module map

| Path | Responsibility |
| --- | --- |
| `main.rs` | CLI, config loading, `--check-config`, schema printing, signal wiring. |
| `service.rs` | Lifecycle facade: compiles config into runtime pieces, spawns workers, supervises reload/shutdown. |
| `service/input.rs` | Pub/Sub reader, Sentinel/echo exclusion, input filters, subscription matching and fan-out. |
| `service/output.rs` | Output worker loop: intake, policy resolution, interval schedules, direct in-flight window. |
| `service/output/flush.rs` | Queue settlement, conflated bucket flush, direct batch publish path. |
| `service/batch.rs` | RESP frame accounting, batch sizing, oversized-message policy. |
| `service/publish.rs` | Redis connection handling, publish commands, failure classification. |
| `service/policy.rs`, `service/policy/glob.rs` | Compiled channel selectors, policy resolution cache, timer schedules. |
| `service/dedup.rs` | Per-channel TTL cache and named deduplication groups. |
| `service/filter.rs`, `service/channel_cache.rs` | Ordered filters and the bounded LRUs behind filters/policies, plus opt-in inspection. |
| `config.rs`, `config/schema.rs`, `config/validate.rs` | Serde config, generated JSON Schema, semantic validation. |
| `status.rs`, `http_status.rs`, `logging.rs` | Status snapshot, optional HTTP endpoints, size-limited JSON logs. |

## Runtime architecture

`service::run` compiles the configuration once per start/reload and then supervises:

- one **input task** that reads the Pub/Sub connection;
- one **output worker task** per configured output, each with its own Redis connection, policy cache, deduplication cache and interval schedules;
- optional status writer and HTTP listener.

### Input path

The single input reader processes messages in arrival order:

1. Drop Sentinel notification channels and output echoes when configured (both default on).
2. Match the message against the configured subscriptions.
3. Apply the **input filter** to the source channel.
4. For each output: apply that output's **filter** to the subscription-mapped channel (before the output's own prefix/suffix), build the final channel, and send a copy to the output's queue.

Filters are ordered rules with `glob`, `regex` or literal (`raw`/`single`/`string`) selectors and `accept`/`deny` actions. Every rule is evaluated and the last match wins; `filter_default` is `accept`.

### Output worker

Each worker is a single ordered consumer. It resolves the channel policy as messages are accepted, then routes by interval:

**Direct lane (`interval_ms <= 0`)** — when deduplication is disabled for the channel, messages go through a passthrough deque and are published through a bounded in-flight window:

- Window bounds are `max_commands_per_exec` commands and the encoded RESP byte target `max_bytes_per_exec`.
- Direct items already queued are sent together, ordered, inside `MULTI/EXEC`; a singleton is published immediately.
- Replies are collected in order; queued entries stay counted as pending until their command settles.
- Messages whose channel or deduplication group already has an in-flight entry wait for those results first, so TTL suppression compares against the final publication outcome.

**Conflated lane (`interval_ms > 0`)** — pending values are stored as `interval_ms → final channel → latest message`. One timer exists per distinct interval present in the configuration; on each tick the worker flushes that bucket only, in channel order, in bounded `MULTI/EXEC` chunks. Replacing a pending value counts as a conflation; deduplication is checked immediately before each chunk is sent.

Interval timers keep their configured cadence; a worker that is busy publishing does not accumulate missed ticks.

### Publishing and failures

- Command batching uses exact RESP frame sizes, including the `MULTI`/`EXEC` frames.
- Oversized messages follow `oversized_message_policy`: `send` (default), `truncate` (payload suffix) or `drop`; the decision is applied per output when the message is accepted.
- Failures are not retried. `NotSent` failures are counted as dropped; failures after sending (error reply, timeout) are counted as uncertain and the batch is abandoned. The connection is discarded on failure and re-established on the next publish.
- Uncertain publications are remembered by the deduplication cache, because they may have reached subscribers.

### Deduplication

Per-channel deduplication compares the exact mapped output channel and the raw incoming payload bytes (including bytes a truncate policy removed). The cache remembers only the latest successfully or uncertainly published value, so `A→B→A` publishes all three while consecutive repeats are suppressed until the TTL expires. `ttl_ms` values `<= 0` disable deduplication and now default to `0`.

Named `deduplication_groups` share one expiry across member channels: `round_ms` floors the Unix-epoch start, `restart_on_change` reanchors the deadline when a changed/new member publishes, and `max_members`/`max_cache_bytes` bound the cache. Suppressed duplicates and intermediate conflated values never renew the deadline.

### Channel policies

`channel_policies` are ordered rules matched against the final mapped channel: exactly one `glob`, `prefix` or `suffix` selector per rule, plus optionally one `default: true` catch-all that must be last. Each rule selects either a `profile` or inline `conflation.interval_ms`, `deduplication.ttl_ms`/`deduplication.group` overrides. Only the first match applies; unmatched channels use output defaults. Resolved policies are memoized per output.

### Reload and shutdown

On Unix, `SIGHUP` parses and validates the new file before switching; invalid files leave the running configuration in place. An accepted reload drains pending output queues and restarts input/output workers in-process, which creates a brief input gap and resets status counters. Logging and instance-lock changes still require a full restart. Shutdown stops input, drains workers, and flushes the remaining conflated buckets.

### Observability

The status snapshot (`schema_version` 5) exposes global and per-output counters for inputs, published/conflated/deduplicated/dropped/truncated messages and bytes, publish errors, uncertain operations, reconnects and pending messages/bytes/keys. Per output it also exposes:

- **queue wait** samples/total/max from fan-out enqueue to worker acceptance (includes policy resolution);
- **publish RTT** samples/total/max around the Redis publish round trip.

`GET /filters` optionally exposes full channel names and cache entries and is disabled by default because it reveals raw channel names; enabling it switches the filter/policy LRUs to shared, mutex-protected storage.

## Configuration defaults

| Setting | Default |
| --- | --- |
| `outputs.<name>.conflation.interval_ms` | required (example uses `0`/`250`) |
| `conflation.max_commands_per_exec` | `256` |
| `max_bytes_per_exec` / per-output override | `4 MiB` |
| `oversized_message_policy` | `send` |
| `outputs.<name>.deduplication.ttl_ms` | `0` (disabled) |
| Deduplication group limits | `16384` members / `64 MiB` |
| Filter/policy cache capacity (each) | `16384`, max `100000`, `0` disables |
| `exclude_output_echoes`, `exclude_sentinel_pubsub` | `true` |
| Status update interval | `1000 ms` |

Positive intervals and TTLs are capped at 365 days; group `round_ms` must not exceed a positive group TTL.

## Benchmarks

### End-to-end hot path

`examples/hotpath_benchmark.rs` (built by `Dockerfile.benchmark`, driven by `compose.hotpath.test.yml`) runs all publishers and both output subscribers in one Rust process. It warms 64 channels, measures 250 serial requests, then runs the concurrent burst and reports:

- serial p50/p95/p99/max;
- per-phase decomposition: publisher-side input ACK RTT, post-ACK service time, and per-output end-to-end percentiles;
- a status timeline (when input/output totals complete, peak pending);
- per-output queue wait and publish RTT from the status endpoint.

`HOTPATH_MODE=closed-loop` (default) waits for every input `PUBLISH` acknowledgement; `HOTPATH_MODE=open-loop` sends each publisher's messages as non-atomic pipelines of `HOTPATH_PIPELINE` commands (default 64) over `HOTPATH_CONNECTIONS` connections per publisher (default 1) without waiting for individual acknowledgements, using an explicit 30 s response timeout, to force sustained pressure. Publisher count and volume come from `HOTPATH_PUBLISHER_COUNT`, `HOTPATH_MESSAGES_PER_PUBLISHER` and `HOTPATH_SERIAL_MESSAGES`.

The mixed-policy workload (direct / 200 ms conflation / 200 ms conflation + 5 s TTL) still uses the Python driver selected with `HOTPATH_BENCHMARK_CMD='python /tests/docker_hotpath_policy_mix.py'`.

### Manual unit benchmarks

Run with `cargo test --release --locked <name> -- --ignored --nocapture`:

- `filter_cache_load_benchmark`, `channel_policy_cache_load_benchmark` (`tests/unit/service/filter_load.rs`) — 64 rules, 8,192-channel hot set, 24,576-channel churn set, plus the anonymized 100× fixture replay.
- `direct_lane_load_benchmark` (`tests/unit/service/direct_lane_load.rs`) — drains 100,000 queued direct messages with a simulated per-command latency (default 50 µs) at batch caps 1/32/256 and asserts FIFO order is preserved.

### Reference measurements

Local Docker/WSL runs with synthetic 64-channel names, worker-local caches and Pub/Sub-only server settings (no persistence, latency tracking, slowlog or subscriber buffer limits; `--save '' --appendonly no --disable-thp yes --latency-tracking no --slowlog-log-slower-than -1 --tcp-backlog 4096 --client-output-buffer-limit pubsub 0 0 0`). Treat them as indicative, not as controlled engine comparisons. The open-loop load generator uses non-atomic pipelines (one round trip per `HOTPATH_PIPELINE` commands), an explicit 30 s response timeout and `HOTPATH_CONNECTIONS` connections per publisher.

Window sweep (`max_commands_per_exec`, single output, 640k, Redis 7.4.11, open-loop pipeline 128): 256 → 99.8k msg/s, publish phase 2.87 s, queue wait 1135 ms, publish RTT avg 2.0 ms; 1024 → 85.6k msg/s, 3.47 s, 701 ms, 10.5 ms; 4096 → 93.8k msg/s, 3.11 s, 378 ms, 28.0 ms. The rate does not scale with the in-flight window while the publish RTT grows proportionally, so the plateau is server-side PUBLISH processing, not the client window; a larger window only redistributes latency (less queue wait, more per-batch RTT).

Closed-loop, Redis 7.4.11, 4×2,500: with two outputs serial p50 ~0.9 ms, e2e p50 ~1.0 ms / p95 ~1.7 ms / p99 ~3.0 ms and ACK RTT p50 0.30 ms; with one output serial p50 0.61 ms, e2e p50 0.64 ms / p95 0.97 ms / p99 1.43 ms and ACK RTT p50 0.24 ms.

Open-loop saturated runs (all drained to zero pending). "Outputs" is `HOTPATH_OUTPUTS`; every input message is published once per output.

| Engine | Outputs | Burst | Publishers × messages | Pipeline | End-to-end rate | Publish phase | Drain | Peak pending | ACK RTT p50/p95 | e2e p50/p95 | Queue wait avg | Publish RTT avg |
| --- | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Redis 7.4.11 | 2 | 80k | 16 × 5,000 | 64 | 35.8k msg/s | 0.61 s | 1.62 s | 11.9k | 4.4/12.2 ms | 0.88/1.48 s | 21/24 ms | 3.7 ms |
| Redis 7.4.11 | 2 | 160k | 32 × 5,000 | 128 | 48.9k msg/s | 1.41 s | 1.86 s | 4.3k | 9.3/35.5 ms | 1.45/1.82 s | 2.3/4.1 ms | 1.7 ms |
| Redis 7.4.11 | 2 | 320k | 64 × 5,000 | 128 | 46.7k msg/s | 1.69 s | 5.15 s | 6.8k | 35.8/62.4 ms | 3.72/5.06 s | 6.0/10.9 ms | 1.8 ms |
| Redis 7.4.11 | 2 | 640k | 128 × 5,000 | 128 | 48.3k msg/s | 3.37 s | 9.88 s | 36.2k | 75.0/135.4 ms | 7.20/9.73 s | 91/103 ms | 2.9 ms |
| Valkey 9.1.2 | 2 | 320k | 64 × 5,000 | 128 | 44.4k msg/s | 1.64 s | 5.57 s | 3.4k | 35.7/59.1 ms | 3.80/5.42 s | 2.6/5.2 ms | 2.0 ms |
| Valkey 9.1.2 | 2 | 640k | 128 × 5,000 | 128 | 40.3k msg/s | 3.96 s | 11.90 s | 93.4k | 86.0/153.7 ms | 8.78/11.86 s | 598/295 ms | 4.3 ms |
| Redis 7.4.11 | 1 | 320k | 64 × 5,000 | 128 | 82.7k msg/s | 1.52 s | 2.35 s | 66.2k | 32.3/56.8 ms | 2.29/2.42 s | 249 ms | 2.3 ms |
| Redis 7.4.11 | 1 | 640k | 128 × 5,000 | 128 | 79.9k msg/s | 3.14 s | 4.86 s | 95.5k | 70.4/123.0 ms | 3.99/4.87 s | 498 ms | 2.9 ms |
| Valkey 9.1.2 | 1 | 640k | 128 × 5,000 | 128 | 80.2k msg/s | 3.25 s | 4.73 s | 55.2k | 73.1/128.4 ms | 4.37/4.76 s | 253 ms | 2.7 ms |

Other reference points:

| Scenario | Result |
| --- | --- |
| Direct lane unit (100k messages, 50 µs/command) | batch 1/32/256 → 836 / 11,443 / 18,187 msg/s; FIFO preserved |
| Filter/policy caches | 8,192 warm channels: no evictions at 16,384; 64-rule misses ~30–41 µs (filters) and ~28–39 µs (policies); hits ~80–120 ns; 100× fixture replay 12.4M lookups in 18.8 s |

Observations:

- The end-to-end rate plateaus near 40–49k messages/s under this load generator; larger bursts raise the median delay roughly in proportion to the backlog rather than lowering throughput.
- Output lanes keep up: publish RTT stays at 1.7–4.4 ms even at 640k, and pending ends at zero. The term that grows at 640k is the intake queue wait (fan-out to worker acceptance), so the single input reader plus per-worker intake is the next concurrency lever.
- Redis and Valkey were close at 320k; at 640k the Valkey sample showed 3–6× higher queue wait and ~2.6× peak pending. This is one sample per engine, not a controlled comparison.
- Server-side configuration (persistence, I/O threading, client buffers) has not been explored yet and is the next tuning step.

## Test suite

- Unit and integration tests live in `tests/unit/`, included as crate modules; `${cargo test --locked}` currently runs 98 tests and lists 8 ignored manual benchmarks.
- Compose E2Es (see [`../docker/README.md`](../docker/README.md)): Pub/Sub integration, oversized request handling, RESP protocol/policies, TTL/groups/SIGHUP, hot-path latency (closed/open/mixed), and fault handling through a Redis proxy.
- `cargo fmt --all --check` and `cargo clippy --all-targets --locked -- -D warnings` are expected clean.
