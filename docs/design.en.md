# Architecture

This document describes the current runtime, the configuration defaults and the
reference measurements. For the change history see
[Changelog](changelog.md); the original NATS backend proposal is in
[`DESIGN-0.3.md`](https://github.com/rspadim/redis-conflated-pubsub/blob/main/DESIGN-0.3.md).

## Purpose

Forward raw Pub/Sub messages from **one** input to one or more named outputs. The
input is Redis (`SUBSCRIBE`/`PSUBSCRIBE`) **or** Core NATS (`nats_subject`); each
output independently selects Redis or Core NATS.

Each output maps addresses, filters messages, conflates to the latest value per
channel and interval, deduplicates identical payloads within a TTL and publishes
to its own endpoint. Payloads stay raw bytes; there is no disk spool.

## Backends

Backend selection is protocol-neutral: `input` and each output carry exactly one
of two optional keys, `redis` or `nats`. Existing Redis JSON is unchanged.

- `--check-config` and the generated JSON Schema enforce the exactly-one-backend
  rule (the schema via `oneOf` on `required: ["redis"]`/`["nats"]`).
- Cross-checks that only runtime validation performs: subscription kind compatible
  with the input backend, the single-transport-family-per-list rule and paired
  mTLS files.

Redis uses `redis-rs`; Core NATS uses `async-nats` (features `websockets`,
`nkeys`, `ring`). The two backends share the filtering/queue/conflation/
deduplication path and differ only at the input reader and the output sink.

## Module map

| Path | Responsibility |
| --- | --- |
| `main.rs` | CLI, config loading, `--check-config`, schema printing, signals. |
| `service.rs` | Lifecycle facade: compiles the config, spawns workers, supervises reload/shutdown. |
| `service/input.rs` | Pub/Sub reader, Sentinel/echo exclusion, input filters, subscription matching and fan-out. |
| `service/output.rs` | Output worker loop: intake, policy resolution, interval schedules, direct in-flight window. |
| `service/output/flush.rs` | Queue settlement, conflated bucket flush, direct batch publish path. |
| `service/batch.rs` | RESP frame accounting, batch sizing, oversized-message policy. |
| `service/publish.rs` | Redis connection, publish commands, failure classification; `OutputSink`/`OutputConnection` selection. |
| `service/backend.rs` | Backend boundary: `InputEnvelope` and the `InputSource`/`OutputSink` traits, plus the hop header constant. |
| `service/nats/connect.rs` | Turns a `NatsConfig` into a connected client; splits the secret-free `ConnectionPlan` from the secret-resolving `connect`. |
| `service/nats/input.rs` | NATS input reader: installs subscriptions, fans streams in, drops loop re-entry. |
| `service/nats/output.rs` | NATS output sink: ordered publish plus one `flush`; stamps the hop header. |
| `service/policy.rs`, `service/policy/glob.rs` | Compiled channel selectors, policy resolution cache, timer schedules. |
| `service/dedup.rs` | Per-channel TTL cache and named deduplication groups. |
| `service/filter.rs`, `service/channel_cache.rs` | Ordered filters and the bounded LRUs behind filters/policies, with opt-in inspection. |
| `config.rs`, `config/schema.rs`, `config/validate.rs` | Serde config, generated JSON Schema, semantic validation. |
| `status.rs`, `http_status.rs`, `logging.rs` | Status snapshot, optional HTTP endpoints, size-limited JSON logs. |

## Runtime

`service::run` compiles the configuration once per start/reload and then
supervises:

- one **input task** that reads the Pub/Sub connection;
- one **output worker task** per configured output, each with its own Redis
  connection, policy cache, deduplication cache and interval schedules;
- optional status writer and HTTP listener.

### Input path

The single input reader processes messages in arrival order:

1. Drop Sentinel notification channels and output echoes when configured (both
   default on; both Redis-specific).
2. Match the message against the configured subscriptions.
3. Apply the **input filter** to the source channel.
4. For each output: apply that output's **filter** to the subscription-mapped
   channel (before the output's own prefix/suffix), build the final channel and
   send a copy to the output's queue.

For a Redis input the reader is `service/input.rs`; for NATS it is
`service/nats/input.rs` (`read_nats_input`), which installs each `nats_subject`
subscription, fans the streams in round-robin, carries the matching
subscription's identity into the mapping and drops any message already stamped
with the hop header (loop guard). Sentinel/echo exclusion is Redis-only; the NATS
path relies on the hop header.

### Output worker

Each worker is a single ordered consumer. It resolves the channel policy as
messages are accepted and then routes by interval:

**Direct lane (`interval_ms <= 0`)** — when deduplication is disabled for the
channel, messages go through a passthrough deque and are published through a
bounded in-flight window:

- The bounds are `max_commands_per_exec` commands and the encoded RESP byte target
  `max_bytes_per_exec`.
- Direct items already queued are sent together, ordered, inside `MULTI`/`EXEC`;
  a singleton is published immediately.
- Replies are collected in order; queued entries stay counted as pending until
  their command settles.
- Messages whose channel or deduplication group already has an in-flight entry
  wait for those results first, so TTL suppression compares against the final
  publication outcome.

**Conflated lane (`interval_ms > 0`)** — pending values are stored as
`interval_ms → final channel → latest message`. One timer exists per distinct
interval present in the configuration; on each tick the worker flushes **only**
that bucket, in channel order, in bounded `MULTI`/`EXEC` chunks. Replacing a
pending value counts as a conflation; deduplication is checked immediately before
each chunk is sent.

Timers keep their configured cadence; a worker that is busy publishing does not
accumulate missed ticks.

### Publishing and failures

- Batching uses exact RESP frame sizes, including the `MULTI`/`EXEC` frames.
- Oversized messages follow `oversized_message_policy`: `send` (default),
  `truncate` (payload suffix) or `drop`; the decision is applied per output when
  the message is accepted.
- **Failures are not retried.** `NotSent` failures are counted as dropped;
  failures after sending (error reply, timeout) are counted as uncertain and the
  batch is abandoned. The connection is discarded on failure and re-established
  on the next publish.
- Uncertain publications are remembered by the deduplication cache, because they
  may have reached subscribers.

### Deduplication

Per-channel deduplication compares the exact mapped output channel and the raw
incoming payload bytes (including bytes a truncate policy removed). It is a diff
TTL: only consecutive identical payloads are suppressed, and only until the
window expires. See [Configuration](configuracao.md#diff-ttl) for the full rules,
groups and the `in_flight_suppression` mode.

### Queue limits

Each output's intake queue is unbounded by default.
`queue_max_messages`/`queue_max_bytes` bound it (bytes count channel name plus
payload) with `queue_overflow_policy`:

- `drop_newest` (default): the fan-out refuses the new message when a limit is
  exceeded;
- `drop_oldest`: uses a shared queue, evicting the oldest entry to admit the new
  one;
- `drop_by_age`: the worker sheds messages older than `queue_max_age_ms` at intake
  (no item/byte limit).

Blocking the reader is **not** offered because Redis would buffer and then
disconnect the subscriber. Because Pub/Sub has no replay, any shedding loses
events explicitly.

### Reload and shutdown

On Unix, `SIGHUP` parses and validates the new file before switching; an invalid
file leaves the running configuration intact. An accepted reload drains pending
queues and restarts the input/output workers in-process, which creates a brief
input gap and resets the status counters. Logging changes still require a full
restart; a changed `instance_lock.path` is acquired during the reload (the new
lock before the old one is released) and the reload is rejected if the new lock
is unavailable. Shutdown stops input, drains the workers and flushes the
remaining conflated buckets.

## Observability

The status snapshot (`schema_version` 5) exposes global and per-output counters
for input, published/conflated/deduplicated/dropped/truncated/shed messages and
their bytes, deduplication evictions, publish errors, uncertain operations,
reconnects, loop-guard-suppressed messages (NATS) and pending
messages/bytes/keys. Per output it also exposes:

- **queue wait** — samples/total/max from fan-out enqueue to worker acceptance
  (includes policy resolution);
- **publish RTT** — samples/total/max around the publish round trip;
- **pending queue bytes** and **oldest pending age** gauges.

NATS connection events are counted globally (`nats_client_connects_total`,
`nats_client_disconnects_total`) and slow-consumer drops
(`nats_client_slow_consumers_total`). Because Core NATS reconnects internally, a
node failover does **not** appear in the reconnect counters; these counters are
best-effort (`async-nats` can drop events in a reconnect storm, which
under-counts, never double-counts).

`GET /filters` exposes full channel names and cache entries and is disabled by
default because it reveals raw names; enabling it switches the filter/policy
LRUs to shared, mutex-protected storage.

## Configuration defaults

| Setting | Default |
| --- | --- |
| `outputs.<name>.conflation.interval_ms` | required |
| `conflation.max_commands_per_exec` | `256` |
| `max_bytes_per_exec` / per-output override | `4 MiB` |
| `oversized_message_policy` | `send` |
| `outputs.<name>.deduplication.ttl_ms` | `0` (disabled) |
| `deduplication.in_flight_suppression` | `false` |
| Per-channel TTL caps (`max_entries`/`max_cache_bytes`) | unset (unbounded) |
| Deduplication group limits | `16384` members / `64 MiB` |
| Queue limits (`queue_max_messages`/`queue_max_bytes`/`queue_max_age_ms`) | unset (unbounded), `drop_newest` |
| Filter/policy cache capacity (each) | `16384`, max `100000`, `0` disables |
| `exclude_output_echoes`, `exclude_sentinel_pubsub` | `true` |
| `logging.prefix` / `logging.enabled` | `redis-conflated-pubsub` / `true` |
| `client_name` template | `ConflatedPS-{version}-{role}::{user}::{host}` |
| Status update interval | `1000 ms` |
| NATS `connect_timeout_ms` / `auth` / `ignore_discovered_servers` | `5000` / `none` / `false` |

Positive intervals and TTLs are capped at 365 days; a group's `round_ms` cannot
exceed a positive group TTL.

## Benchmarks

### End-to-end hot path

`examples/hotpath_benchmark.rs` runs all publishers and both output subscribers
in a single Rust process. It warms 64 channels, measures 250 serial requests and
then runs the concurrent burst, reporting serial percentiles, a phase
decomposition (input ACK RTT, post-ACK service time and per-output end-to-end
percentiles) and the peak pending backlog.

`HOTPATH_MODE=closed-loop` (default) waits for the ack of every `PUBLISH`;
`HOTPATH_MODE=open-loop` sends the messages in non-atomic pipelines of
`HOTPATH_PIPELINE` commands (default 64) over `HOTPATH_CONNECTIONS` connections
per publisher, without waiting for individual acks, to force sustained pressure.
Full commands and measurement commands in [Docker and tests](docker.md).

### Capture and replay

`capture-feed` records read-only Pub/Sub metadata (relative timestamp, channel
and payload length; **never** the contents) to local NDJSON plus a summary.
`replay-feed` republishes a capture preserving the recorded inter-arrival timing,
with synthetic payloads of the recorded length and optional per-channel
repetition to exercise the TTL, so realistic feed shapes can drive load tests
without exporting application data.

A 30-minute capture of a market-data feed (6,054,668 messages, 3,364 msg/s
average, 5,508 msg/s peak, 8,664 unique channels) replayed against the local
stack with 200 ms conflation, 5 s TTL and the recommended window 1024/batch 64:

| Replay | Input | Published | Pending | Queue wait avg/max | Publish RTT avg/max |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1× (30 min) | 6,053,786 | 2,292,837 (37.9%) | 0 | 0.071/13.5 ms | 0.492/11.7 ms |
| 4× | 6,053,786 | 1,371,471 (22.7%) | 0 | 0.080/5.1 ms | 0.443/6.9 ms |
| 8× | 6,053,786 | 1,127,642 (18.6%) | 0 | 0.095/8.2 ms | 0.462/8.2 ms |
| 4×, window/batch 256/256 | 6,053,786 | 1,357,014 (22.4%) | 0 | 0.104/11.6 ms | 0.975/8.0 ms |
| 4×, repeated payloads | 6,053,786 | 979,805 (16.2%) | 0 | 0.090/9.2 ms | 0.448/8.0 ms |

The input excludes 882 `__sentinel__:hello` messages per run. Conflation reduced
publishing to 16–38% of the input depending on the rate, with zero backlog and
sub-millisecond average queue wait and publish RTT.

### Loopback (no Redis/Valkey)

`examples/loopback_benchmark.rs` implements a minimal RESP2 broker in Rust: it
accepts the service's `PSUBSCRIBE`, feeds messages as fast as the socket accepts
them and acts as the output server receiving `PUBLISH`/`MULTI`/`EXEC`, reporting
end-to-end percentiles. `compose.loopback.test.yml` runs it without Redis
containers, isolating the service and the protocol.

### Transport comparison and NATS e2e

`spikes/compare` is a separate Rust crate that measures the raw transport paths
(`async-nats` vs `redis-rs`) and drives the **real relay binary** end to end. Its
publisher and subscriber run as **separate processes** so the publisher's
buffering cannot inflate the measured latency; a single-process harness produces
queue-dominated numbers and must not be used for latency claims.

### Reference measurements

Local Docker/WSL runs with synthetic 64-channel names, worker-local caches and
Pub/Sub-only server settings. Treat them as indicative, not as controlled engine
comparisons.

Window sweep (`max_commands_per_exec`, single output, 640k, Redis 7.4.11,
pipeline 128): 256 → 99.8k msg/s; 1024 → 85.6k msg/s; 4096 → 93.8k msg/s. The
rate does not scale with the in-flight window while the publish RTT grows
proportionally, so the plateau is server-side `PUBLISH` processing, not the
client window.

2D grid after the decoupling (single output, 640k, open-loop, median of 3 reps):

| Batch | Window | Rate (msg/s) | Queue wait avg | Publish RTT avg |
| ---: | ---: | ---: | ---: | ---: |
| 64 | 256 | 116.1k | 1566 ms | 2.0 ms |
| 64 | 1024 | 129.3k | 848 ms | 6.9 ms |
| 64 | 4096 | 114.8k | 800 ms | 30.8 ms |
| 256 | 256 | 130.5k | 1225 ms | 1.6 ms |
| 256 | 1024 | 129.3k | 898 ms | 6.6 ms |
| 256 | 4096 | 117.3k | 655 ms | 27.5 ms |
| 1024 | 256 | 130.6k | 1396 ms | 1.7 ms |
| 1024 | 1024 | 123.0k | 979 ms | 7.0 ms |
| 1024 | 4096 | 119.8k | 712 ms | 21.3 ms |

The window helps up to 1024; at 4096 the RTT explodes because too many concurrent
`MULTI`/`EXEC` requests queue at the output server. The best region is window 1024
with batch 64–256. Run-to-run variance is high, so rate differences under ~10%
are noise; the latency signals are the robust ones.

After the optimizations (payload `Arc<[u8]>`, decoupled in-flight window, per-turn
flush fairness), runs with Redis 7.4.11: single output 640k — **139.4k msg/s**
with the default window and **149.1k** with `max_in_flight_commands: 1024`; two
outputs 640k — **77.6k msg/s** (~155k publishes/s), identical with one server or
two; loopback (no Redis) 1M — **80k msg/s**. Before these changes the same runs
measured 99.8k (single), 50.1k (two) and 57.6k (split endpoints).

Observations:

- The term that grows at 640k is the **intake queue wait** (fan-out to worker
  acceptance): the single input reader plus the per-worker intake is the next
  concurrency lever.
- Splitting endpoints: with one output the scenario is limited by the server near
  90–100k publishes/s; with two outputs the plateau mixes server processing and a
  client/intake cap around 50–58k input/s.
- Run-to-run variance is high: the same single-output configuration measured
  139.4k, a 129k median across the grid and 106.9k in the verification run, with
  raw samples spanning 91k–138k. Prefer the latency signals (`queue_wait`, publish
  RTT).

## Test suite

- Unit and integration tests live in `tests/unit/`, included as crate modules;
  `cargo test --locked` passes 187 tests on Windows and 198 on Unix (where the
  `#[cfg(unix)]` reload tests compile), and lists 8 ignored manual benchmarks.
- Compose E2Es (see [Docker and tests](docker.md)): Pub/Sub integration, oversized
  messages, RESP protocol/policies, TTL/groups/SIGHUP, hot-path latency and fault
  handling.
- `cargo fmt --all --check` and
  `cargo clippy --all-targets --locked -- -D warnings` are expected clean.

Manual unit benchmarks: run with
`cargo test --release --locked <name> -- --ignored --nocapture`.

- `filter_cache_load_benchmark`, `channel_policy_cache_load_benchmark`
  (`tests/unit/service/filter_load.rs`) — 64 rules, 8,192-channel hot set,
  24,576-channel churn set, plus the anonymized fixture replay 100×. Misses with
  64 rules: ~30–41 µs (filters) and ~28–39 µs (policies); hits ~80–120 ns.
- `direct_lane_load_benchmark` (`tests/unit/service/direct_lane_load.rs`) — drains
  100,000 queued direct messages with a simulated per-command latency (default
  50 µs) at batch caps 1/32/256 (836 / 11,443 / 18,187 msg/s) and asserts FIFO
  order.
