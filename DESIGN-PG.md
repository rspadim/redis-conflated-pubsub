# PostgreSQL LISTEN/NOTIFY backend — investigation record

**Status: not planned for implementation.** This document exists so the findings,
the decisions and the measurements are not redone. Nothing here is committed to a
version, and no code was written for it.

It was considered as a v0.4.0 candidate: accept PostgreSQL `LISTEN`/`NOTIFY` as an
input and/or output, alongside Redis and Core NATS. The investigation stopped at
design plus measurement.

Two rules from [`AGENTS.md`](AGENTS.md) shaped everything below: the payload is
byte-transparent (no content typing, no configurable encoding), and the relay only
consumes and writes against the user's services (no DDL, no conventions that
require new structure in their database).

## What the transport actually is

Quotes are from the PostgreSQL documentation, confirmed against a running server
(18.6) where noted.

| Property | Detail |
| --- | --- |
| Payload | A simple string literal, **shorter than 8000 bytes**, text only. "If binary data or large amounts of information need to be communicated, it's best to put it in a database table and send the key of the record." **The limit is not a setting** — unlike the NATS `max_payload`, which the relay already preflights and which a server can raise. |
| NUL / invalid bytes | Impossible: `text` cannot hold `0x00`, and the literal must be valid in the connection encoding. A 35-byte payload with NUL and `0xff` — which crosses Redis↔NATS byte-identically — cannot be written. |
| Channels | `LISTEN channel` takes an identifier. **No wildcards exist.** Names are subject to the identifier rules and to the 63-byte name limit. |
| Discovery | There is **no global catalog of channels**. `pg_listening_channels()` returns "the names of asynchronous notification channels that the **current session** is listening to". The set to listen to can only come from the operator's own data. |
| Delivery | Only at **COMMIT**. An aborted transaction loses its notifications. `LISTEN` itself also "takes effect at transaction commit". |
| Batching | All-or-nothing per transaction: one failing statement aborts the whole transaction and nothing is delivered. Redis `MULTI`/`EXEC` does **not** behave this way (a failing `PUBLISH` does not roll back its neighbours). |
| Folding | "If the same channel name is signaled multiple times with identical payload strings within the same transaction, only one instance of the notification event is delivered." Measured exactly — see below. |
| Queue | A queue holds notifications not yet processed by all listeners; 8 GB by default (`max_notify_queue_pages` = 1048576 on the measured server). "No cleanup can take place if a session executes `LISTEN` and then enters a transaction for a very long time." If it fills, `NOTIFY` fails at commit. |
| Subscribers | Nothing exposes how many sessions listen. An output must report `0`, exactly like NATS. |
| Self-notification | A notifying session that also listens gets its own event back; the documentation suggests ignoring it by comparing the notifying session's PID with one's own. **Useless for a relay**: input and output are different sessions, so the PID never matches, and the loop must be broken by channel namespace instead (the rule that already exists for Redis on the same server). |
| Startup race | "first execute (and commit!) `LISTEN`, then in a new transaction inspect the database state as needed by the application logic, then rely on notifications". The intuitive order — query, then listen — loses whatever was notified in between. |

## Measurements

Two throwaway probes, both outside the repository.

**Probe A — `psql` client running inside the container** (no host network hop).
PostgreSQL 18.6, `postgres:alpine`, all defaults, single client session, 5.000
notifications per case:

| Case | Result |
| --- | --- |
| 1 notification per transaction | **667 msg/s** (7.496 ms) |
| 500 per transaction | **1.594 msg/s** (3.137 ms) |
| payload of 7.900 bytes | **275 msg/s** (200 messages, 728 ms) |
| payload of 8.100 bytes | **refused by the server** |
| 1 per transaction, with a listener attached | 543 msg/s (vs 667 without) |
| 500 per transaction, with a listener attached | 1.546 msg/s (vs 1.594 without) |
| notification queue usage at the end | 0 |

Folding, measured rather than assumed: 1000 identical notifications in one
transaction plus 1000 with distinct payloads plus 2000 one-per-transaction
delivered **3001** — exactly `1 + 1000 + 2000`. Nothing else was folded.

**Probe B — `sqlx` client on the host, through Docker Desktop's published port.**
Not comparable with probe A: the published port adds roughly 2 ms per round trip,
and one notification per transaction is three round trips (BEGIN, notify, COMMIT),
so the numbers fell to 164 msg/s (1 per transaction) and 497 msg/s (batched);
4 concurrent tasks reached 354 and 1.125 msg/s (~2,3× one task). It did establish
that the `sqlx` API works as it would be used (`PgListener::listen`/`recv`,
`SELECT pg_notify($1, $2)` with bound parameters, batches inside
`begin`/`commit`), with no errors and the queue drained to zero.

**Probe C — `sqlx` client and server inside one Docker network**, no published
port, so no host proxy. 20.000 notifications per case, 20000 in each:

| Case | Without a listener | With a listener |
| --- | --- | --- |
| 1 per transaction, 1 publisher | **573 msg/s** (34,9 s) | 547 msg/s (36,5 s) |
| 250 per transaction, 1 publisher | **1.858 msg/s** (10,8 s) | 1.622 msg/s (12,3 s) |
| 1 per transaction, 4 publishers | **1.544 msg/s** (13,0 s) | 1.142 msg/s (17,5 s) |
| 250 per transaction, 4 publishers | **4.054 msg/s** (4,9 s) | 3.252 msg/s (6,1 s) |

Concurrency helps but does not scale linearly (4 publishers ≈ 2,2–2,7× one), which
is consistent with a serialization point in the notification machinery rather than
in the client.

Delivery was verified separately, and cleanly: **25.000 of 25.000 received, with no
error in the listener's full log**, p50 latency 51 ms and p95 148 ms. One larger
battery (80.000 notifications) reported only half received while the notification
queue still drained to zero; that was **not reproduced** in the controlled run and
its cause was never established — treat it as an unexplained orchestration artifact
of that probe, and re-verify it if this backend is ever picked up.

**The caveat that matters most, and it applies to every number above:** both
clients (psql and this harness) `await` each `pg_notify` individually, so a batch of
250 costs 250 round trips plus one commit. That is why batching only bought ~3,5×
instead of the near-free improvement it should be, and why the p50 latency is tens
of milliseconds rather than single digits: the first notification of a batch waits
for the whole loop to be sent. A real implementation **pipelines the batch** — send
all the `pg_notify` calls, then await the `COMMIT` — so the plateau measured here is
a floor imposed by the harness, not PostgreSQL's ceiling. Pipelining is the first
measurement to make if this is ever built; the numbers above bound the relay's cost
from below, not the server's capability.

**Not measured at all:** pipelined batches (see above), delivery latency from a
local client with a pipelined publisher, and a payload-size sweep other than the two
points in probe A.


For scale, the project's own Redis and Core NATS numbers are in the tens to low
hundreds of thousands of messages per second. **PostgreSQL is 50 to 100× below
that**, which is the finding that matters: it is an integration path (letting an
application that already speaks `LISTEN`/`NOTIFY` participate), not a throughput
path. The documentation's own advice about tables for large data points the same
way.

Two things the numbers do say about the design: **batching is worth 2,4× to 3,5×**
(667 → 1.594 with psql, 573 → 1.858 with the real client), so the commit is the
dominant cost and a transaction per chunk is the right shape — with the caveat that
those batches were not pipelined, so the true gain is larger; and **payload size
costs a lot** (7.900 bytes is ~6× slower per message), which is why a per-message
preflight against the ceiling is a requirement and not an optimization — with
all-or-nothing semantics, one bad message would take its whole batch down.

## Configuration shape that was agreed

For the record, since these were settled before the investigation stopped.

```json
{
  "type": "listen",
  "channels": ["control:reload"],
  "query": "SELECT DISTINCT channel FROM relay_channels WHERE active",
  "query_refresh_ms": 60000,
  "output_prefix": "",
  "output_suffix": ""
}
```

- **`listen`**, not `pg_channels`: the subscription types are already each
  backend's native verb (`subscribe` and `psubscribe` are Redis commands;
  `nats_subject` is the exception because NATS has no verb for it). `listen` is
  more consistent with the file than a prefixed name would be. It no longer names
  the backend, which is covered by the existing rule that a subscription kind must
  match its input backend — the same check `nats_subject` already goes through.
- **`postgres`** as the endpoint key, not `pg`: `redis` and `nats` are full names,
  and `pg` would be the only abbreviation in the schema.
- **libpq vocabulary inside the block**, deliberately not Redis's: `dbname` (not
  `database`, which in Redis is a number and here is a name) and `sslmode` (with
  libpq's own values: `disable`, `allow`, `prefer`, `require`, `verify-ca`,
  `verify-full`; omitted means `prefer`, the libpq default).
- **`query` absent, null or blank (after trimming) means no dynamic channels** —
  only `channels`. Not an error.
- **`query_refresh_ms: 0`** runs the query once and never again; a positive value
  re-runs it on that cadence.
- First execution order: **`LISTEN` with commit, then the query.**
- On refresh: `LISTEN` the new channels and `UNLISTEN` only the ones that came from
  the query. A channel listed statically is never removed.
- `SIGHUP` re-runs the query immediately, regardless of the interval.
- An empty query result is not an error — it is an empty dynamic set.
- **A failing query degrades, it does not abort startup**: keep the current set,
  log, count, retry on the next tick, and expose the failure in the status. A
  database blip at boot should not keep the service down.
- Status additions: a discovery-failure counter and a gauge of how many channels
  are being listened to, because "listening to zero" is otherwise
  indistinguishable from "there is nothing to listen to".

## How the output would publish

`SELECT pg_notify($1, $2)` — the function, never the `NOTIFY` command, because the
command's payload "must be specified as a simple string literal" and therefore
cannot take a parameter. The documentation recommends the function precisely "if
you need to work with non-constant channel names and payloads".

```sql
BEGIN;
SELECT pg_notify($1, $2);   -- mapped channel, raw payload
SELECT pg_notify($1, $2);
COMMIT;
```

| Existing mechanism | What it becomes |
| --- | --- |
| Batch in `MULTI`/`EXEC` | Batch in `BEGIN` … `COMMIT`, same `max_commands_per_exec` / `max_bytes_per_exec` |
| Waiting for the `EXEC` reply | Waiting for the `COMMIT` reply |
| Definitive pre-send failure | Invalid mapped channel, over 63 bytes, or payload over 8.000 → `NotSent`, counted as dropped |
| Connection discarded on failure | Same, one dedicated connection per output worker |
| `0` subscribers on a NATS output | Same: nothing exposes listeners |

Three consequences worth remembering:

1. **Delivery happens at `COMMIT`**, so "published" becomes "committed".
2. **All-or-nothing per batch** (see the table above), which is why the per-message
   preflight is mandatory.
3. **Folding is free deduplication** inside a batch, on top of the existing TTL
   deduplication.

The only place where the relay would build SQL text is the `LISTEN` statement, and
it is an identifier, not a value: it needs a quoting helper (double quotes,
`"` doubled) with a unit test. The channel and payload of a publish never enter SQL
text.

## Client library

Measured on the project's own lockfile, which already contains tokio, rustls,
bytes and serde:

| | total crates | **new for the project** | pulls a C library? |
| --- | --- | --- | --- |
| `tokio-postgres` 0.7.18 + `tokio-postgres-rustls` 0.14.0 | 120 | **48** | **yes** (`aws-lc-sys`, `cmake`, `jobserver`) |
| `sqlx` 0.8.6 (`default-features = false`, `runtime-tokio`, `tls-rustls`, `postgres`) | 147 | **39** | no |

The surprise, and it reversed an earlier assumption: the lighter-looking option is
the heavier one here. `rustls` 0.23 defaults to **aws-lc-rs**, so
`tokio-postgres-rustls` drags a C build into a project that deliberately chose
**ring** on its other two backends and ships a static musl binary. `sqlx` reuses
ring and adds no C dependency.

Also: **`tokio-postgres` has no TLS feature at all** (its features are `runtime`
and `with-*` for type mappings), so TLS means a companion crate plus a hand-wired
`TlsConnect`. `sqlx` has `tls-rustls` as a feature, and its `PgListener` does the
identifier quoting internally — the one piece of SQL text the relay would otherwise
own.

What `sqlx` brings that is actively unwanted is disabled with
`default-features = false`: a pool (wrong for `LISTEN`, since a pooled session can
be recycled and `LISTEN` is per-session), migrations (DDL, forbidden by the rules
in `AGENTS.md`) and the `query!` macros (they need a live database or an offline
cache at build time).

Provisional verdict: **start with `sqlx`**. The decisive test — release **musl**
binary size for both — was never run, and build cost is real: the throwaway harness
took 5m42s to compile with `sqlx` on Windows.

## Decisions taken while parking this

These were the open questions. They are recorded as answered so nobody has to
re-derive them, with the reasoning attached. Nothing is implemented.

1. **`channels: []` and a blank query is an error at `--check-config`.** Starting
   up listening to nothing is the worst failure mode: the service looks healthy and
   receives nothing. `validate.rs` already rejects an empty subscription list, so
   this is the same rule applied inside the subscription.
2. **`oversized_message_policy: "send"` is rejected on a PostgreSQL output.** Above
   8.000 bytes it can only fail, and with all-or-nothing transactions that failure
   would take its whole batch down. `truncate` and `drop` remain available.
3. **A channel name over 63 bytes is rejected, never truncated** — whether it comes
   from `channels`, from the discovery query, or from the output mapping. Truncating
   collides silently with another channel. Static and mapped names can be caught by
   `--check-config`; discovered ones only at runtime, with a log and a counter.
4. **`query_refresh_ms` has a documented floor of 1000 ms**, with `0` meaning "run
   once". It is polling against the database, and the relay's own status cadence is
   one second; anything faster would be abusive.
5. **Server-side identity reuses the existing `client_name` template as libpq's
   `application_name`** — no new field, and the relay identifies itself the same way
   it already does in Redis's `CLIENT LIST`.
6. **Certificates use libpq's own names**: `sslrootcert`, `sslcert`, `sslkey`, the
   same criterion that produced `dbname` and `sslmode`.

## Load test plan, if this is ever picked up

Baseline before relay: with `NOTIFY` the publisher and the listener are both
sessions of the same server, and the notification queue serializes them, so the
server's own ceiling has to be measured first — otherwise "the relay is slow" and
"PostgreSQL is slow" are indistinguishable. That is what `spikes/compare` exists
for (raw transport, publisher and subscriber in separate processes).

| Piece | Reuse or new |
| --- | --- |
| PostgreSQL input (`LISTEN`) | **new**: nothing in the project publishes `NOTIFY` today |
| PostgreSQL output, Redis→PostgreSQL direction | **pure reuse**: `replay-feed` already publishes to Redis; only the config changes |
| Raw transport baseline | new arm in `spikes/compare` |
| Hot-path latency with a PostgreSQL output | **reuse** `examples/hotpath_benchmark.rs` plus a `tests/docker-pg-hotpath-config.json`, like the existing `-single`/`-mix` configs |
| Realistic load | **reuse** `capture-feed` (payload-size distribution) and `replay-feed` |
| Functional E2E | the `compose.nats.hotpath.test.yml` pattern: `compose.pg.test.yml` plus a subcommand in `examples/e2e.rs` |
| Failure / uncertain | the `compose.test.yml --profile fault` pattern, with a PostgreSQL proxy in place of the Redis one |
| Auth / TLS | the `spikes/nats/test-auth.sh` pattern (disposable servers, authenticated round trip) |

Two behaviours need dedicated tests rather than benchmarks, because they differ
from every other backend:

1. **All-or-nothing per transaction**: a message over 8.000 bytes must be rejected
   *before* entering the batch, with the right counters, and its neighbours in the
   same chunk must still arrive. Otherwise a single bad payload takes a batch down.
2. **Delivery at commit**: a proxy that drops the `COMMIT` reply must produce
   `uncertain_*`, not `publish_errors_total` — the same classification the Redis
   fault proxy already validates.

New metrics for the report: **commit RTT** (the real barrier, unlike the NATS
`flush`, which is documented as not being one), `pg_notification_queue_usage()`,
and the **rejection rate from the ceiling** obtained by crossing a real capture's
payload-size distribution with the 8.000-byte limit — which answers with data
whether PostgreSQL could carry the actual feed or only control-plane signals.

Method conditions inherited from the documentation: the listening session must
never hold a long transaction (it blocks queue cleanup), and every measurement must
declare the payload size, because a rate without the size is meaningless under an
8.000-byte ceiling.
