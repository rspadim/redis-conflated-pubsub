# Changelog

## 0.2.0 — 2026-10-08

- Changed the default `outputs.<name>.deduplication.ttl_ms` from 5000 to 0; deduplication is disabled unless an output, profile, or channel policy enables it explicitly.
- Added ordered input/output channel filters with glob, regex, and literal (`raw`/`single`/`string`) selectors; the last matching rule wins and `filter_default` defaults to `accept`.
- Added separate bounded LRU caches for input filters, each output's filters, and each output's channel-policy resolutions. Their capacities are independently configurable; default 16,384, zero disables. The opt-in HTTP `GET /filters` endpoint exposes full cache entries and channel names; disabled by default.
- Grouped pending conflated channel values by output interval, so each timer flushes only its own key/value bucket. Direct passthrough now submits a bounded window of Redis publishes without awaiting each response; queued messages may be sent together with MULTI/EXEC.
- Added per-output worker-queue wait and Redis publish RTT aggregate metrics (sample count, total, and maximum) to the status snapshot.
- Replaced the direct hotpath Python benchmark script with a Rust publisher/subscriber harness (`examples/hotpath_benchmark.rs`) that runs publishers and both output subscribers in one process; it decomposes latencies into publisher acknowledgement, post-ack service time, and per-output end-to-end, supports one or two outputs (`HOTPATH_OUTPUTS`), and offers `HOTPATH_MODE=closed-loop` (default) or `open-loop` with non-atomic pipelines of `HOTPATH_PIPELINE` commands (default 64) over `HOTPATH_CONNECTIONS` connections per publisher (default 1) and an explicit 30 s response timeout. The benchmark stack runs Redis/Valkey with Pub/Sub-only flags (no persistence, latency tracking, slowlog or subscriber buffer limits). Reference Redis/Valkey open-loop runs measured a 40–49k msg/s plateau with two outputs and ~80k msg/s with one output; earlier ~10 s figures were an artifact of the unbuffered Python harness reader under GIL contention.
- Replaced the mixed-policy Python benchmark with `examples/hotpath_mix_benchmark.rs` (installed as `hotpath-mix-benchmark` and selected with `HOTPATH_BENCHMARK_CMD`), keeping the same warm-up, ~33/33/34 direct / 200 ms conflation / 200 ms conflation + 5 s TTL workload split, the repeated per-channel TTL payload, per-output delivered/by-cohort counts, direct and conflate p50/p95 receive latencies, and the conflated/deduplicated totals plus `GET /filters` cache snapshot. The benchmark image no longer ships Python.
- Added an optional single-container Redis/Valkey bundle selected with `KV_ENGINE`, plus an example configuration and persistent data paths.
- Replaced the Python RESP counting/fault proxies and the protocol/fault E2E drivers with the `redis-conflated-e2e` Rust binary (`examples/e2e.rs`, built by `Dockerfile.e2e` and exposed through the `fault-proxy`, `counting-proxy`, `protocol-integration` and `fault-integration` subcommands). The proxies keep the same stats endpoints and counters, the fault trigger still drops the configured Nth successful `EXEC` reply, and both drivers keep the previous assertions; the protocol and fault Compose stacks no longer run Python or mount `tests/`.
- Migrated the remaining integration E2Es and their workload helpers to `redis-conflated-e2e`: `integration` and `random-publisher` (feed mapping, cross-database outputs, conflation and echo checks), `filter-integration` (ordered filters and `GET /filters` snapshots) and `oversize-integration` (Redis query-buffer limit). `tests/payloads.py` was ported to Rust, the five Python drivers were removed, and the integration/filter/oversize Compose stacks no longer mount `tests/`.
- Migrated the TTL/profile/group/channel-policy E2E to the `ttl-integration` subcommand of `redis-conflated-e2e`, keeping the previous scenarios and tolerances: ordered profile/inline selector precedence, individual and grouped TTL expiry (fixed, restart-on-change and `round_ms` floor windows), independent 100/300 ms conflation intervals, TTL 0 still conflating and direct TTL 0 forwarding every input, a valid `SIGHUP` config swap, and an invalid-file reload that leaves the active config running. `tests/docker_ttl_integration.py` and `tests/redis_resp.py` were removed, and the TTL Compose stack no longer runs Python or mounts `tests/`.
- Completed internal service/config module extraction and moved unit tests to `tests/unit/` without expanding public runtime APIs.
- Reworked the documentation to describe the current release: `src/DESIGN.md` covers architecture, defaults and reference measurements, and the README/Docker guides no longer narrate previous development steps.
- Added a loopback load test (`examples/loopback_benchmark.rs` plus `compose.loopback.test.yml`) with a minimal RESP2 broker in Rust, so the service can be measured without Redis or Valkey.
- Added `logging.prefix` (base log-file name; lets several services share one directory without touching each other's files) and `logging.enabled` (default `true`; `false` disables file logging).
- Shared message payloads with `Arc<[u8]>`, cutting the per-output copies in fan-out, batching and deduplication.
- Decoupled the direct in-flight window from the batch cap (`conflation.max_in_flight_commands` / `max_in_flight_bytes`) and made conflated flushes fair per turn (completions before timers, one chunk per bucket per turn).
- Trimmed input-path allocations (precomputed channel mappings, a single enqueue timestamp, no pattern allocation for a single subscription) and coalesced worker metric updates per intake/settlement pass.
- Sped up the loopback broker (reused RESP buffers, no per-message formatting, `LOOPBACK_PAYLOAD_BYTES`) and made the hotpath harness wait for subscriber readiness before releasing publishers.
- Added `HOTPATH_MIN_NUMPAT` to the hotpath and mix harnesses so they can run against shared servers where other clients already hold Pub/Sub patterns.
- Added `capture-feed` and `replay-feed` to the benchmark image: a read-only Pub/Sub capture of timestamps, channels and payload lengths (never payload contents) plus a timing-preserving, rate-scalable replayer that synthesizes payloads of the recorded length for realistic load tests.
- Fixed output intake starvation while a large conflated bucket drained: intake is now serviced every turn and the worker yields through pending buckets instead of waiting for the next timer tick, which previously capped bucket drains at one chunk per tick and backed up the intake queue.
- Added `deduplication.in_flight_suppression` (treat a sent-but-unacknowledged value as published for suppression, rolling back on a definitive pre-send failure) and optional per-channel TTL caps `deduplication.max_entries`/`max_cache_bytes` with LRU eviction counted in `deduplication_evictions_total`.
- Added per-output intake queue limits: `queue_max_messages`, `queue_max_bytes`, `queue_overflow_policy` (`drop_newest`/`drop_oldest`/`drop_by_age`) and `queue_max_age_ms`, with shedding counters and the `pending_queue_bytes`/`oldest_pending_age_ms` gauges. The default stays unbounded.
- `SIGHUP` reload can now change `instance_lock.path` in-process: the new lock is acquired before the old one is released, and the reload is rejected when the new lock is unavailable.
- Added `client_name` (default `ConflatedPS-{version}-{role}::{user}::{host}`; empty disables): every connection reports `lib-name`/`lib-ver` through `CLIENT SETINFO` and outputs additionally set `CLIENT SETNAME` with the resolved name (`{role}` is `i` for the input or `o-<output>`; `{user}`/`{host}` come from the environment) so `CLIENT LIST` shows which host, user, role and version is connected.

## 0.1.4 — 2026-10-06

- Added Unix `SIGHUP` configuration reload. The new file is parsed and validated before switching; invalid configuration leaves the current runtime active. Accepted reloads drain pending output queues and restart input/output workers in-process, briefly interrupting Pub/Sub input and resetting status counters. Logging and instance-lock changes still require a full process restart.

## 0.1.3 — 2026-10-06

- Added per-output profiles with partial conflation/TTL overrides. Ordered `channel_policies` select the first matching profile or inline rule, then the required final `default` rule; existing configurations without policies remain backward compatible.
- Channels with distinct conflation intervals are scheduled independently, each using its configured interval.
- Added named, output-local TTL groups with clock-rounded shared expiry, fixed or reanchored deadlines, and bounded caches (`max_members`, `max_cache_bytes`). Positive runtime durations are capped at 365 days to avoid timer overflow.
- Added existing global `output_payload_bytes_total` and `conflated_payload_bytes_total` counters to the Zabbix collector and 5.0/7.0 templates; `schema_version` remains 5.

## 0.1.2 — 2026-10-06

- Added per-output duplicate suppression with signed integer `outputs.<name>.deduplication.ttl_ms`, defaulting to 5000 ms; values `<= 0` disable deduplication without changing interval or conflation behavior. The memory-only cache remembers the latest successfully or outcome-ambiguous raw payload per mapped output channel: consecutive identical payloads are suppressed until the TTL expires, while a changed payload is published and replaces the remembered value (so A→B→A publishes all three). Each output has a separate cache, with no Redis keys or disk spool.
- Added global and per-output `deduplicated_messages_total` and `deduplicated_payload_bytes_total` metrics, plus collector and Zabbix 5.0/7.0 template support. The additive status fields retain `schema_version: 5`.
- Added a Docker end-to-end TTL test in `compose.ttl.test.yml`.

## 0.1.1 — 2026-10-06

- Added global and per-output RESP request-size targets and split conflated flushes by exact encoded transaction bytes and command count.
- Added configurable oversized-message policies: `send`, `truncate`, and `drop`.
- Made output-echo and Redis Sentinel Pub/Sub exclusions explicit input settings; both default to enabled.
- Failed publishes are not retried. The service counts failed or uncertain messages, rate-limits error logs, and continues with later messages and chunks.
- Added detailed CLI help and `--config-json-schema` output.
- Added Docker E2Es for Redis query-buffer limits, conflation and chunking, oversized-message policies, Sentinel filtering, ambiguous EXEC responses, and immediate PUBLISH output.
- Expanded status and Zabbix metrics for dropped, truncated, failed, and uncertain messages.
