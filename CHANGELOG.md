# Changelog

## 0.2.0 — unreleased

- Added ordered input/output channel filters with glob, regex, and literal (`raw`/`single`/`string`) selectors; the last matching rule wins and `filter_default` defaults to `accept`.
- Added separate bounded LRU caches for input filters, each output's filters, and each output's channel-policy resolutions. Their capacities are independently configurable; default 16,384, zero disables. The opt-in HTTP `GET /filters` endpoint exposes full cache entries and channel names; disabled by default.
- Grouped pending conflated channel values by output interval, so each timer flushes only its own key/value bucket. Direct passthrough now submits a bounded window of Redis publishes without awaiting each response; queued messages may be sent together with MULTI/EXEC.
- Added an optional single-container Redis/Valkey bundle selected with `KV_ENGINE`, plus an example configuration and persistent data paths.
- Completed internal service/config module extraction and moved unit tests to `tests/unit/` without expanding public runtime APIs.

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
