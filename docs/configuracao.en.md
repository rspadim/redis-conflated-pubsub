# Configuration

The relay reads a single JSON file. The fields below are the ones that shape
behavior the most; exact reference and limits are validated by `--check-config`
and described by the generated JSON Schema (`--config-json-schema`).

## Profiles and channel policies

Per-output defaults live in `conflation.interval_ms` and `deduplication.ttl_ms`.

- Positive intervals and TTLs are limited to **365 days**.
- **Nonpositive** intervals select direct publishing.
- **Nonpositive** TTLs disable deduplication.

An output's own `deduplication_groups` maps names to
`{ "ttl_ms", "round_ms", "restart_on_change", "max_members", "max_cache_bytes" }`.

| Field | Effect |
| --- | --- |
| `round_ms` | Floors the Unix-epoch timestamp used for the group TTL to a bucket. `0` disables it. Flooring can shorten an active TTL by up to `round_ms`. |
| `restart_on_change` | Reanchors the deadline after a changed value or a newly published member. `false` keeps the expiry fixed. |
| `max_members` / `max_cache_bytes` | Group limits (default 16,384 channels and 64 MiB; maximum 100,000 members and 256 MiB). When full, the least recently used member is evicted. |

For positive TTLs, `round_ms` cannot exceed `ttl_ms` (checked by
`--check-config`). Suppressed duplicates and intermediate conflated values do
**not** renew the deadline. A single member larger than the byte limit is still
published but is not cached, with a warning.

### Diff TTL

`deduplication.ttl_ms` is a **diff TTL**: only **consecutive** identical payloads
(same mapped channel and same raw bytes) are suppressed, and only until the window
expires.

- A changed payload always publishes (subject to conflation) and replaces the
  remembered value — so `A→B→A` publishes all three.
- After the window ends, the current value is sent again even if unchanged, acting
  as a refresh.

`deduplication.in_flight_suppression` (default `false`) treats a value that was
sent but not yet acknowledged as published for suppression purposes, so identical
repeats are dropped without waiting for the in-flight batch to settle. A definitive
pre-send failure rolls the remembered value back; an ambiguous outcome keeps it.

Optional `max_entries` and `max_cache_bytes` bound the per-channel TTL cache with
LRU eviction (unset means unbounded) and count evictions in
`deduplication_evictions_total`.

### Intake queues

Each output can bound its intake queue with `queue_max_messages` and
`queue_max_bytes` (unset means unbounded) plus `queue_overflow_policy`:

| Policy | Behavior |
| --- | --- |
| `drop_newest` | Default. The fan-out refuses the message that arrives. |
| `drop_oldest` | Evicts the oldest entry to admit the new one. |
| `drop_by_age` | Sheds according to `queue_max_age_ms`. |

Shed messages are counted per output and globally in
`shed_messages_total`/`shed_payload_bytes_total`. The queue byte gauge is
`pending_queue_bytes` and the oldest pending age is `oldest_pending_age_ms`.

!!! warning "Pub/Sub has no replay"
    Any shedding policy drops events explicitly. The default configuration stays
    unbounded.

### Profiles

`profiles` are partial overrides: set `conflation.interval_ms`,
`deduplication.ttl_ms` or `deduplication.group` as needed; unset values inherit
the output defaults. A profile **cannot** set `deduplication.ttl_ms` and
`deduplication.group` at the same time.

```json
"profiles": {
  "fast_no_dedup": {
    "conflation.interval_ms": 100,
    "deduplication.ttl_ms": 0
  },
  "shared_symbols": {
    "conflation.interval_ms": 250,
    "deduplication.group": "symbols"
  }
}
```

### Channel policies

Each ordered `channel_policies` entry has **exactly one** selector: `glob`,
`prefix`, `suffix` or `"default": true` (put the default last). It selects either a
profile **or** inline overrides using literal dotted keys.

Policies match the **final mapped output channel**, after the subscription and
output prefixes and suffixes. Only the **first** matching policy is selected for
each channel. `--check-config` enforces that the default rule is last.

```json
"channel_policies": [
  { "glob": "db1:sub:test-feed:alpha:source", "profile": "fast_no_dedup" },
  { "prefix": "db1:sub:test-feed:beta:", "profile": "shared_symbols" },
  {
    "default": true,
    "conflation.interval_ms": 250,
    "deduplication.group": "symbols"
  }
]
```

Without a group, deduplication TTLs expire per channel; group members share a
deadline. Each output uses one scheduler cadence per distinct positive interval;
equal intervals share a cadence.

For local policy-load testing,
[`config.channel-policies-load.example.json`](https://github.com/rspadim/redis-conflated-pubsub/blob/main/config.channel-policies-load.example.json)
has 16 ordered tenant rules and a final default.

## Channel filters

`input.filters` evaluates the channel reported by Redis after a
`SUBSCRIBE`/`PSUBSCRIBE` matches, but **before** the subscription's
`output_prefix`/`output_suffix` is added. Each output may also have its own
`filters`; those evaluate the subscription-mapped channel (after the input
prefix/suffix, before that output's `channel_prefix`/`channel_suffix`).

Rules are ordered: **every** rule is evaluated and the `action` (`accept` or
`deny`) of the **last matching rule** decides. Non-matching rules do nothing. If
no rule matches, `filter_default` decides (default `accept`).

Each rule has exactly one selector:

| Selector | Semantics |
| --- | --- |
| `glob` | Supports `*`, `?`, character classes and backslash escapes. |
| `regex` | Rust syntax; searches for a match anywhere. Use `^`/`$` for a whole-channel match. |
| `raw`, `single`, `string` | Aliases for exact equality. `*` is literal. |

```json
{
  "filters": [
    {"glob": "events:*", "action": "deny"},
    {"regex": "^events:public:[0-9]+$", "action": "accept"},
    {"raw": "events:public:42", "action": "deny"}
  ],
  "filter_default": "accept"
}
```

In the example, the final literal rule overrides the earlier regex for that one
exact channel.

### Caches

Selectors are compiled once into internal variants and the original selector
strings are discarded after setup. Capacities are independent:

- `input.filter_cache_max_entries`
- `outputs.<name>.filter_cache_max_entries`
- `outputs.<name>.channel_policy_cache_max_entries`

Each defaults to **16,384**, accepts **0 to disable** that cache and is capped at
**100,000**. The limit applies per cache; total memory scales with the number of
configured filters and outputs. On eviction, only the least-recently-used channel
leaves the cache. An accepted reload recreates workers and their caches.

## Conflation and batch size

Positive intervals retain the latest message per mapped channel and flush in
`MULTI`/`EXEC` transactions.

| Field | Meaning |
| --- | --- |
| `max_bytes_per_exec` | Per-request byte target. |
| `max_commands_per_exec` | Command limit per transaction. |
| `oversized_message_policy` | `send` (default), `truncate` or `drop`. |

## Client name

`client_name` shows up in `CLIENT LIST`. The default template is
`ConflatedPS-{version}-{role}::{user}::{host}`:

- `{version}` — program version.
- `{role}` — `i` for the input or `o-<output>` for each output.
- `{user}` — `USERDOMAIN\USERNAME` on Windows or `USER`/`LOGNAME` elsewhere.
- `{host}` — `COMPUTERNAME`/`HOSTNAME`, falling back to the OS hostname.

Missing values fall back to `unknown`. A template without `{role}` gets
`-<role>` appended, and an empty `client_name` disables naming.

Output connections send `CLIENT SETNAME` on every (re)connect. The Pub/Sub input
does the same on plain TCP: it authenticates and sets the name on the socket
**before** subscribing, so input naming requires a non-TLS input. Every connection
also sends `CLIENT SETINFO` (`lib-name`/`lib-ver`) when the server is Redis 7.2 or
newer.

## Logging

```json
"logging": {
  "directory": "logs",
  "prefix": "redis-conflated-pubsub",
  "enabled": true,
  "level": "info",
  "retention_days": 14,
  "max_total_size_mb": 1024
}
```

`logging.prefix` names the log files (`{prefix}.{YYYY-MM-DD}[.{n}]`) and scopes
retention cleanup, so several services can share one directory without touching
each other's files. `logging.enabled: false` disables file logging entirely; the
service still runs, and `--check-config` still validates the other fields.

## Scope

JetStream (persistence, acknowledgement, durable replay) and Redis Streams are
**permanently out of scope**. The relay is an ephemeral Pub/Sub conflator; adding
a durable log would change its contract.
