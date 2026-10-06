# Changelog

## 0.1.1 — 2026-10-06

- Added global and per-output RESP request-size targets and split conflated flushes by exact encoded transaction bytes and command count.
- Added configurable oversized-message policies: `send`, `truncate`, and `drop`.
- Made output-echo and Redis Sentinel Pub/Sub exclusions explicit input settings; both default to enabled.
- Failed publishes are not retried. The service counts failed or uncertain messages, rate-limits error logs, and continues with later messages and chunks.
- Added detailed CLI help and `--config-json-schema` output.
- Added Docker E2Es for Redis query-buffer limits, conflation and chunking, oversized-message policies, Sentinel filtering, ambiguous EXEC responses, and immediate PUBLISH output.
- Expanded status and Zabbix metrics for dropped, truncated, failed, and uncertain messages.
