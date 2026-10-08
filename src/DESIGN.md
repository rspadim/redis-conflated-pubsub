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

Per-channel deduplication compares the exact mapped output channel and the raw incoming payload bytes (including bytes a truncate policy removed). It is a diff TTL: only consecutive identical payloads are suppressed, and only until the window expires; a changed payload always publishes and replaces the remembered value, so `A→B→A` publishes all three, and after the window the current value is re-sent even if unchanged. `ttl_ms` values `<= 0` disable deduplication and now default to `0`.

Named `deduplication_groups` share one expiry across member channels: `round_ms` floors the Unix-epoch start, `restart_on_change` reanchors the deadline when a changed/new member publishes, and `max_members`/`max_cache_bytes` bound the cache. Suppressed duplicates and intermediate conflated values never renew the deadline.

`deduplication.in_flight_suppression` (default `false`) treats a value that was sent but not yet acknowledged as published for suppression purposes: identical repeats are dropped without waiting for the in-flight batch to settle. A definitive pre-send (`NotSent`) failure rolls the remembered value back; an ambiguous (`Uncertain`) outcome keeps it because the publish may have reached the broker. `deduplication.max_entries` and `deduplication.max_cache_bytes` (optional; unset means unbounded) bound the per-channel TTL cache with LRU eviction, counted in `deduplication_evictions_total`.

### Intake queue limits

Each output's intake queue is unbounded by default. `queue_max_messages`/`queue_max_bytes` bound it (bytes count channel name plus payload) with `queue_overflow_policy`:

- `drop_newest` (default): the fan-out refuses the new message when a limit is exceeded;
- `drop_oldest`: uses a shared queue so the oldest entry is evicted to admit the new one;
- `drop_by_age`: the worker sheds messages older than `queue_max_age_ms` at intake (no item/byte limit).

Shed messages are counted per output and globally (`shed_messages_total`/`shed_payload_bytes_total`), `pending_queue_bytes` is the queue byte gauge and `oldest_pending_age_ms` the oldest pending age. Pub/Sub has no replay, so any shedding policy loses events explicitly; blocking the reader is not offered because Redis would buffer and then disconnect the subscriber.

### Channel policies

`channel_policies` are ordered rules matched against the final mapped channel: exactly one `glob`, `prefix` or `suffix` selector per rule, plus optionally one `default: true` catch-all that must be last. Each rule selects either a `profile` or inline `conflation.interval_ms`, `deduplication.ttl_ms`/`deduplication.group` overrides. Only the first match applies; unmatched channels use output defaults. Resolved policies are memoized per output.

### Reload and shutdown

On Unix, `SIGHUP` parses and validates the new file before switching; invalid files leave the running configuration in place. An accepted reload drains pending output queues and restarts input/output workers in-process, which creates a brief input gap and resets status counters. Logging changes still require a full restart; a changed `instance_lock.path` is acquired during the reload (the new lock before the old one is released) and the reload is rejected when the new lock is unavailable. Shutdown stops input, drains workers, and flushes the remaining conflated buckets.

### Observability

The status snapshot (`schema_version` 5) exposes global and per-output counters for inputs, published/conflated/deduplicated/dropped/truncated/shed messages and bytes, deduplication evictions, publish errors, uncertain operations, reconnects and pending messages/bytes/keys. Per output it also exposes:

- **queue wait** samples/total/max from fan-out enqueue to worker acceptance (includes policy resolution);
- **publish RTT** samples/total/max around the Redis publish round trip;
- **pending queue bytes** and **oldest pending age** gauges for the bounded-queue policies.

`GET /filters` optionally exposes full channel names and cache entries and is disabled by default because it reveals raw channel names; enabling it switches the filter/policy LRUs to shared, mutex-protected storage.

`CLIENT LIST` identifies the service connections. Outputs send `CLIENT SETNAME` on every (re)connect; the Pub/Sub input does the same by connecting the plain TCP socket, authenticating (username/password when configured) and sending `CLIENT SETNAME` before subscribing, then handing the stream to `PubSub::new` — Redis keeps the connection name while subscribed. TLS inputs fall back to the unnamed connection because redis-rs keeps its TLS connector private. Names come from the `client_name` template (`{version}`, `{role}`, `{user}`, `{host}`). Naming is a best-effort cosmetic step: a forbidden `CLIENT SETNAME` logs one warning per connection and the connection keeps working.

## Configuration defaults

| Setting | Default |
| --- | --- |
| `outputs.<name>.conflation.interval_ms` | required (example uses `0`/`250`) |
| `conflation.max_commands_per_exec` | `256` |
| `max_bytes_per_exec` / per-output override | `4 MiB` |
| `oversized_message_policy` | `send` |
| `outputs.<name>.deduplication.ttl_ms` | `0` (disabled) |
| `deduplication.in_flight_suppression` | `false` |
| Per-channel TTL caps (`max_entries`/`max_cache_bytes`) | unset (unbounded) |
| Deduplication group limits | `16384` members / `64 MiB` |
| Intake queue limits (`queue_max_messages`/`queue_max_bytes`/`queue_max_age_ms`) | unset (unbounded), `drop_newest` |
| Filter/policy cache capacity (each) | `16384`, max `100000`, `0` disables |
| `exclude_output_echoes`, `exclude_sentinel_pubsub` | `true` |
| `logging.prefix` / `logging.enabled` | `redis-conflated-pubsub` / `true` |
| `client_name` template | `ConflatedPS-{version}-{role}::{user}::{host}` (empty disables naming) |
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

The mixed-policy workload (direct / 200 ms conflation / 200 ms conflation + 5 s TTL) uses its own Rust harness, `examples/hotpath_mix_benchmark.rs` (built into the benchmark image), selected with `HOTPATH_BENCHMARK_CMD='hotpath-mix-benchmark'`. It warms one payload per cohort/channel, runs the three cohorts in the same ~33/33/34 split as before and prints per-output delivered/by-cohort counts, direct/conflate p50/p95 receive latencies, the service's conflated/deduplicated totals and the `GET /filters` cache snapshot.

### Capture and replay

`capture-feed` records read-only Pub/Sub metadata (relative timestamp, channel, payload length; never payload contents) to local NDJSON plus a summary, and `replay-feed` republishes a capture with the recorded inter-arrival timing (rate-scalable, pipelined, synthetic payloads of the recorded length, optional repeated payloads for TTL) so realistic feed shapes can drive load tests without exporting application data.

A 30-minute capture of a market-data feed (6,054,668 messages, 3,364 msg/s average, 5,508 msg/s peak, 8,664 unique channels, 93 MB of metadata) replayed against the local stack with 200 ms conflation, 5 s TTL and the recommended window 1024/batch 64:

| Replay | Input | Published | Pending | Queue wait avg/max | Publish RTT avg/max |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1× (30 min) | 6,053,786 | 2,292,837 (37.9%) | 0 | 0.071/13.5 ms | 0.492/11.7 ms |
| 4× | 6,053,786 | 1,371,471 (22.7%) | 0 | 0.080/5.1 ms | 0.443/6.9 ms |
| 8× | 6,053,786 | 1,127,642 (18.6%) | 0 | 0.095/8.2 ms | 0.462/8.2 ms |
| 4×, window/batch 256/256 | 6,053,786 | 1,357,014 (22.4%) | 0 | 0.104/11.6 ms | 0.975/8.0 ms |
| 4×, repeated payloads | 6,053,786 | 979,805 (16.2%) | 0 | 0.090/9.2 ms | 0.448/8.0 ms |

Input excludes 882 Sentinel `__sentinel__:hello` messages per run; the repeated-payload variant reuses each channel's previous payload on three of every four messages to exercise TTL suppression. Conflation reduced publishing to 16–38% of the input depending on rate, with zero pending backlog and sub-millisecond average queue wait and publish RTT at every rate; the default 256/256 window/batch pair showed a similar published volume with roughly twice the average publish RTT. This replay harness also surfaced the intake starvation that 0.2.0 fixed (intake is now serviced every turn and the worker yields through pending buckets instead of draining one chunk per timer tick).

### Loopback (no Redis/Valkey)

`examples/loopback_benchmark.rs` (built into the same benchmark image) implements a minimal RESP2 broker in Rust: it accepts the service's `PSUBSCRIBE`, feeds messages as fast as the socket accepts them, and acts as the output server that receives the service's `PUBLISH`/`MULTI/EXEC` commands, reporting end-to-end percentiles. `compose.loopback.test.yml` runs it with no Redis containers, isolating the service and the wire protocol. A 200k-message smoke measured ~35k msg/s with the broker process sharing CPU with the service; `LOOPBACK_MESSAGES`, `LOOPBACK_CHUNK`, `LOOPBACK_WARMUP` and `LOOPBACK_TIMEOUT_S` tune the run.

### Manual unit benchmarks

Run with `cargo test --release --locked <name> -- --ignored --nocapture`:

- `filter_cache_load_benchmark`, `channel_policy_cache_load_benchmark` (`tests/unit/service/filter_load.rs`) — 64 rules, 8,192-channel hot set, 24,576-channel churn set, plus the anonymized 100× fixture replay.
- `direct_lane_load_benchmark` (`tests/unit/service/direct_lane_load.rs`) — drains 100,000 queued direct messages with a simulated per-command latency (default 50 µs) at batch caps 1/32/256 and asserts FIFO order is preserved.

### Reference measurements

Local Docker/WSL runs with synthetic 64-channel names, worker-local caches and Pub/Sub-only server settings (no persistence, latency tracking, slowlog or subscriber buffer limits; `--save '' --appendonly no --disable-thp yes --latency-tracking no --slowlog-log-slower-than -1 --tcp-backlog 4096 --client-output-buffer-limit pubsub 0 0 0`). Treat them as indicative, not as controlled engine comparisons. The open-loop load generator uses non-atomic pipelines (one round trip per `HOTPATH_PIPELINE` commands), an explicit 30 s response timeout and `HOTPATH_CONNECTIONS` connections per publisher.

Window sweep (`max_commands_per_exec`, single output, 640k, Redis 7.4.11, open-loop pipeline 128): 256 → 99.8k msg/s, publish phase 2.87 s, queue wait 1135 ms, publish RTT avg 2.0 ms; 1024 → 85.6k msg/s, 3.47 s, 701 ms, 10.5 ms; 4096 → 93.8k msg/s, 3.11 s, 378 ms, 28.0 ms. The rate does not scale with the in-flight window while the publish RTT grows proportionally, so the plateau is server-side PUBLISH processing, not the client window; a larger window only redistributes latency (less queue wait, more per-batch RTT).

2D grid after the decoupling (`max_commands_per_exec` × `max_in_flight_commands`, single output, 640k, open-loop, median of 3 reps):

| Batch | Window | Rate (msg/s) | Publish phase | Drain | Queue wait avg | Publish RTT avg |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 64 | 256 | 116.1k | 3.16 s | 2.63 s | 1566 ms | 2.0 ms |
| 64 | 1024 | 129.3k | 2.79 s | 2.17 s | 848 ms | 6.9 ms |
| 64 | 4096 | 114.8k | 3.15 s | 2.37 s | 800 ms | 30.8 ms |
| 256 | 256 | 130.5k | 2.79 s | 2.11 s | 1225 ms | 1.6 ms |
| 256 | 1024 | 129.3k | 2.98 s | 2.08 s | 898 ms | 6.6 ms |
| 256 | 4096 | 117.3k | 2.88 s | 2.43 s | 655 ms | 27.5 ms |
| 1024 | 256 | 130.6k | 2.80 s | 2.15 s | 1396 ms | 1.7 ms |
| 1024 | 1024 | 123.0k | 2.95 s | 2.27 s | 979 ms | 7.0 ms |
| 1024 | 4096 | 119.8k | 2.99 s | 2.14 s | 712 ms | 21.3 ms |

The window helps up to 1024 (rate up, queue wait roughly halved); at 4096 the publish RTT explodes because too many concurrent `MULTI/EXEC` requests queue at the output server. The batch cap helps mainly with a small window (64→256/1024 at window 256 is ~+12%); batch larger than the window is harmless (the dispatch chunks by window), while window much larger than batch underfills transactions and hurts. Best region: window 1024 with batch 64–256. Run-to-run variance is high (for example 256/1024 measured 91k and 138k msg/s), so rate differences under ~10% are noise; the latency signals are the robust ones.

Closed-loop, Redis 7.4.11, 4×2,500: with two outputs serial p50 ~0.9 ms, e2e p50 ~1.0 ms / p95 ~1.7 ms / p99 ~3.0 ms and ACK RTT p50 0.30 ms; with one output serial p50 0.61 ms, e2e p50 0.64 ms / p95 0.97 ms / p99 1.43 ms and ACK RTT p50 0.24 ms.

Open-loop saturated runs (all drained to zero pending). "Outputs" is `HOTPATH_OUTPUTS`; every input message is published once per output.

| Engine | Outputs | Burst | Publishers × messages | Pipeline | End-to-end rate | Publish phase | Drain | Peak pending | ACK RTT p50/p95 | e2e p50/p95 | Queue wait avg | Publish RTT avg |
| --- | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Redis 7.4.11 | 2 | 80k | 16 × 5,000 | 64 | 35.8k msg/s | 0.61 s | 1.62 s | 11.9k | 4.4/12.2 ms | 0.88/1.48 s | 21/24 ms | 3.7 ms |
| Redis 7.4.11 | 2 | 160k | 32 × 5,000 | 128 | 48.9k msg/s | 1.41 s | 1.86 s | 4.3k | 9.3/35.5 ms | 1.45/1.82 s | 2.3/4.1 ms | 1.7 ms |
| Redis 7.4.11 | 2 | 320k | 64 × 5,000 | 128 | 46.7k msg/s | 1.69 s | 5.15 s | 6.8k | 35.8/62.4 ms | 3.72/5.06 s | 6.0/10.9 ms | 1.8 ms |
| Redis 7.4.11 | 2 | 640k | 128 × 5,000 | 128 | 50.1k msg/s | 3.56 s | 9.20 s | 217k | 79.6/150.6 ms | 6.66/9.16 s | 620 ms | 4.0/4.6 ms |
| Redis 7.4.11 | 2, split endpoints | 640k | 128 × 5,000 | 128 | 57.6k msg/s | 2.82 s | 8.30 s | 179k | 61.9/114.5 ms | 5.42/8.20 s | 348/394 ms | 3.2/3.1 ms |
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

- Post-optimization runs (payload `Arc<[u8]>`, decoupled in-flight window, per-turn flush fairness, input/metric trims; Redis 7.4.11): single output 640k — **139.4k msg/s** with the default window and **149.1k** with `max_in_flight_commands: 1024`; two outputs 640k — **77.6k msg/s** (~155k publishes/s), identical whether both outputs share one server or use two; loopback (no Redis) 1M — **80k msg/s**. Before these changes the same runs measured 99.8k (single), 50.1k (two outputs) and 57.6k (split endpoints): single-output rose ~40%, two-output ~55%, and the split-endpoint advantage disappeared. The service publish path (~150–160k publishes/s) is now the limiter rather than the output server.
- The end-to-end rate plateaus near 40–49k messages/s under this load generator; larger bursts raise the median delay roughly in proportion to the backlog rather than lowering throughput.
- Output lanes keep up: publish RTT stays at 1.7–4.4 ms even at 640k, and pending ends at zero. The term that grows at 640k is the intake queue wait (fan-out to worker acceptance), so the single input reader plus per-worker intake is the next concurrency lever.
- Redis and Valkey were close at 320k; at 640k the Valkey sample showed 3–6× higher queue wait and ~2.6× peak pending. This is one sample per engine, not a controlled comparison.
- Splitting the two outputs across two Redis servers gained ~15% (50.1k → 57.6k input/s; ~100k → 115k publishes/s) rather than ~2×, so at two outputs the plateau is mixed: output-server processing plus a client/intake-side cap around 50–58k input/s. Single-output remains server-bound near 90–100k publishes/s.
- Run-to-run variance is high: the same single-output configuration measured 139.4k msg/s in one session, a 129k median across the grid and 106.9k in the verification run, with raw samples spanning 91k–138k for one grid cell. Treat throughput differences under roughly 20% as noise and prefer the latency signals (`queue_wait`, publish RTT), which were stable. The verification battery (closed-loop single 0.63 ms p50; two-output 64.3k msg/s; loopback 256-byte payloads 54.6k msg/s; mix-policy parity between both outputs) also showed zero publish errors, uncertain messages or reconnects, and the queue/dedup feature smokes passed: `drop_newest` shed 450,846 of 640,000 messages with counters and no errors, and `in_flight_suppression` suppressed 2 of 3 identical direct publishes.
- On a shared remote Redis (roughly 7 ms round trip, with other clients already holding Pub/Sub patterns), the mix workload (conflate 200 ms + TTL 5 s) is publisher-bound: 10k messages took ~17.4 s to publish regardless of service configuration. There, `max_in_flight_commands: 1024` with `max_commands_per_exec: 64` gave the lowest conflated-lane latency (p50 60.8 ms at 10k and 40.1 ms at 40k messages, p95 129–187 ms) versus 88.6 ms p50 with the defaults; the direct lane stayed at the network round trip (12.9–18.4 ms p50). The harness waits for `PUBSUB NUMPAT` to reach `HOTPATH_MIN_NUMPAT` (default 1) so it can run on shared servers.
- Server-side configuration (persistence, I/O threading, client buffers) has not been explored yet and is the next tuning step.

## Test suite

- Unit and integration tests live in `tests/unit/`, included as crate modules; `${cargo test --locked}` currently runs 130 tests and lists 8 ignored manual benchmarks.
- Compose E2Es (see [`../docker/README.md`](../docker/README.md)): Pub/Sub integration, oversized request handling, RESP protocol/policies, TTL/groups/SIGHUP, hot-path latency (closed/open/mixed), and fault handling. The Pub/Sub integration, filter, oversize, RESP protocol/policies, fault and TTL/groups/SIGHUP stacks run the Rust drivers and proxies from `examples/e2e.rs` (built by `Dockerfile.e2e`) instead of Python.
- `cargo fmt --all --check` and `cargo clippy --all-targets --locked -- -D warnings` are expected clean.
