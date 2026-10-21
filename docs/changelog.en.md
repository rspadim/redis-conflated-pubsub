# Changelog

The canonical file is [`CHANGELOG.md`](https://github.com/rspadim/redis-conflated-pubsub/blob/main/CHANGELOG.md)
at the repository root. Below is a summary of the versions.

## 0.4.0 — 2026-10-21

- The installer's "Test connection" button now runs the relay's own `--test-connection` against a draft of the configuration the wizard would write, instead of a PowerShell probe of the wire protocol. The new flag validates the configuration, then connects to every configured backend (Redis: connect + `PING`; NATS: the full connection plan) and exits non-zero when any of them did not answer. It reuses the runtime's client configuration, so the check cannot disagree with what the service does at startup - including TLS, ACL, cluster and the NATS credential styles. Read-only: it connects and drops, publishing nothing and touching no key.
- The probe is available outside the installer too: `redis-conflated-pubsub --config config.json --test-connection` reports one line per backend (`input redis  host:port  OK`, or `FAILED: <reason>`), which makes it usable from a deployment script or a support session.
- The wizard names the specific reason on failure (rejected credentials, timeout, refused) by reading the relay's own report instead of a bare "failed". Localized in all 10 wizard languages.
- Removed `installer/test-connection.ps1` and the messages only it used: a wire-level handshake proved reachability but not credentials, which is the case that actually fails. The endpoint pages keep their "Test connection" button, now backed by the real client.

## 0.3.1 — 2026-10-14

- The relay now implements the Windows service protocol: the service control
  manager starts, stops and restarts it like any other service. Before this the
  installer registered a console program, so every start request failed with
  error 1053 and the service never came up. Other platforms are unaffected.
- Parameterised silent install: `/CONFIG=<file>` uses a configuration that already
  exists, `/SERVICE=0|1` decides whether the service is registered, and
  `/SERVICESTART=auto|delayed-auto|demand` sets its startup type.
- Failures after the files are copied return exit code **9** and are recorded in
  `{app}\install-report.txt`, one line per step; nothing is shown in silent mode.
  Inno Setup returns 0 for a script failure, so that code is the only way to fail
  loudly.
- The setup confirms that the relay came up, not only that the service accepted
  the start request, and it requires a status file written after the check began.
- Fixed: `sc.exe` could not receive the default installation path (error 1639),
  and the service state was read from text that changes with the language
  (`ESTADO`), which made a running service look `unknown`.
- Fixed: a silent install generated NATS-to-NATS instead of Redis-to-Redis, a
  silent upgrade skipped the service, and the reason a configuration was rejected
  was lost.

## 0.3.0 — 2026-10-14

- Protocol-neutral backend selection: `input` and each output use exactly one of
  `redis` or `nats`.
- Core NATS input and output backend (Pub/Sub only, not JetStream): `servers`
  with the single-transport-family-per-list rule (`nats://`, `tls://`, `ws://`,
  `wss://`), `name`/`connect_timeout_ms`, optional `ignore_discovered_servers`
  and `nats_subject` subscriptions with native `*`/`>` wildcards.
- NATS authentication variants: `none` (default), `user_password`, `token`,
  `nkey` and `credentials_file`, with TLS/mTLS under `tls`. Secrets always by
  environment variable or file path, never inlined in JSON.
- NATS→NATS loop guard via the `ConflatedPS-Hop` header, with the
  `loop_suppressed_messages_total` counter. Application headers are not
  forwarded.
- Oversized-message policy applied against the server-advertised NATS
  `max_payload` (minus the hop-header bytes), with preflight of each chunk.
- NATS connection lifecycle in status (`nats_client_connects_total`,
  `nats_client_disconnects_total`, `nats_client_slow_consumers_total`) and
  fail-fast when a referenced secret is missing.

## 0.2.2 — 2026-10-08

- The Pub/Sub input sends `CLIENT SETNAME` before subscribing (plain TCP), so
  `CLIENT LIST` shows the `-i` name on the input server too. TLS inputs remain
  unnamed.

## 0.2.1 — 2026-10-08

- `client_name` `{host}` falls back to the OS hostname when
  `COMPUTERNAME`/`HOSTNAME` are unset, resolving the machine instead of
  `unknown` under systemd.
- `deduplication.ttl_ms` documented as a diff TTL.

## 0.2.0 — 2026-10-08

- The `deduplication.ttl_ms` default changed from 5000 to **0**: deduplication is
  only enabled explicitly by an output, profile or channel policy.
- Ordered input/output filters with `glob`, `regex` and literal selectors.
- Separate bounded LRU caches for input filters, each output's filters and
  channel policies; the opt-in `GET /filters` endpoint exposes the entries.
- Conflated channel buckets grouped by interval, each timer flushing only its own
  bucket; direct passthrough with a bounded in-flight window.
- Per-output queue-wait and publish-RTT metrics.
- Hot-path and mixed-policy benchmarks migrated to Rust.
- Single-container image with Redis/Valkey selectable through `KV_ENGINE`.
- E2E proxies and drivers migrated to the `redis-conflated-e2e` Rust binary.

## 0.1.4 — 2026-10-06

- Configuration reload by `SIGHUP` on Unix. The new file is validated before
  switching; invalid configuration keeps the current runtime. Logging and lock
  changes still require a full restart.

## 0.1.3 — 2026-10-06

- Per-output profiles with partial conflation/TTL overrides and ordered channel
  policies with a required final `default` rule.
- Named TTL groups with clock-rounded shared expiry and bounded caches. Positive
  durations capped at 365 days.

## 0.1.2 — 2026-10-06

- Per-output duplicate suppression with `deduplication.ttl_ms` (default 5000 ms
  at the time), with a memory-only cache.
- Global and per-output deduplication counters and support in the Zabbix
  templates.

## 0.1.1 — 2026-10-06

- RESP request-size targets and splitting of conflated flushes by exact bytes and
  command count.
- Oversized-message policies: `send`, `truncate` and `drop`.
- Explicit output-echo and Sentinel Pub/Sub exclusions.
- Failures are not retried: the service counts failed/uncertain messages,
  rate-limits error logs and continues.

## 0.1.0 — 2026-10-05

- First release: Redis Pub/Sub relay with conflation and channel mapping,
  transactional conflation and cross-platform builds.
