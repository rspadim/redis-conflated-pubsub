# Redis Conflated Pub/Sub

A Rust relay that forwards Pub/Sub messages between Redis and Core NATS. The `input` selects one backend and every output independently selects Redis or Core NATS; mapping, filters, conflation and deduplication run the same way for either backend.

Current release: **v0.3.0**. [Download a platform binary](https://github.com/rspadim/redis-conflated-pubsub/releases) or see the [developer build instructions](src/README.md). The project site is at <https://rspadim.github.io/redis-conflated-pubsub/> (pt-BR).

## Quick start

Save one of the configurations below as `config.json`, then validate and run the downloaded binary:

```sh
./redis-conflated-pubsub --check-config --config config.json
./redis-conflated-pubsub --config config.json
```

On Windows, use `redis-conflated-pubsub.exe` with the same arguments. `--check-config` validates the JSON and semantic rules, including same-server echo-loop prevention.

## Local Redis example

This example maps `test-feed:alpha` to `db0:sub:test-feed:alpha:source` and `db1:sub:test-feed:alpha:source`. `output0` skips conflation and publishes directly, but still deduplicates identical payloads for 5 seconds; `output1` defaults to 250 ms conflation and a 5-second TTL, with channel-specific profiles and a shared deduplication group. Subscribe with `redis-cli PSUBSCRIBE 'db0:*'` or `redis-cli PSUBSCRIBE 'db1:*'`.

```json
{
  "max_bytes_per_exec": 4194304,
  "oversized_message_policy": "send",
  "client_name": "ConflatedPS-{version}-{role}::{user}::{host}",
  "input": {
    "redis": {
      "host": "localhost",
      "port": 6379,
      "username": null,
      "password_env": null,
      "tls": false,
      "database": 0,
      "connect_timeout_ms": 5000
    },
    "exclude_output_echoes": true,
    "exclude_sentinel_pubsub": true,
    "filters": [],
    "filter_cache_max_entries": 16384,
    "filter_default": "accept",
    "subscriptions": [
      {
        "type": "psubscribe",
        "pattern": "test-feed:*",
        "output_prefix": "sub:",
        "output_suffix": ":source"
      }
    ]
  },
  "outputs": {
    "output0": {
      "redis": {
        "host": "localhost",
        "port": 6379,
        "username": null,
        "password_env": null,
        "tls": false,
        "database": 0,
        "connect_timeout_ms": 5000
      },
      "channel_prefix": "db0:",
      "channel_suffix": "",
      "filters": [],
      "filter_cache_max_entries": 16384,
      "channel_policy_cache_max_entries": 16384,
      "filter_default": "accept",
      "conflation": {
        "interval_ms": 0,
        "max_commands_per_exec": 100,
        "oversized_message_policy": "send"
      },
      "deduplication": {
        "ttl_ms": 5000,
        "in_flight_suppression": false,
        "max_entries": 16384,
        "max_cache_bytes": 67108864
      },
      "queue_max_messages": 65536,
      "queue_max_bytes": 67108864,
      "queue_overflow_policy": "drop_newest"
    },
    "output1": {
      "redis": {
        "host": "localhost",
        "port": 6379,
        "username": null,
        "password_env": null,
        "tls": false,
        "database": 1,
        "connect_timeout_ms": 5000
      },
      "channel_prefix": "db1:",
      "channel_suffix": "",
      "filters": [],
      "filter_cache_max_entries": 16384,
      "channel_policy_cache_max_entries": 16384,
      "filter_default": "accept",
      "conflation": {
        "interval_ms": 250,
        "max_commands_per_exec": 2,
        "max_bytes_per_exec": 2097152,
        "oversized_message_policy": "send"
      },
      "deduplication": {
        "ttl_ms": 5000
      },
      "deduplication_groups": {
        "symbols": {
          "ttl_ms": 5000,
          "round_ms": 1000,
          "restart_on_change": true
        }
      },
      "profiles": {
        "fast_no_dedup": {
          "conflation.interval_ms": 100,
          "deduplication.ttl_ms": 0
        },
        "shared_symbols": {
          "conflation.interval_ms": 250,
          "deduplication.group": "symbols"
        }
      },
      "channel_policies": [
        {
          "glob": "db1:sub:test-feed:alpha:source",
          "profile": "fast_no_dedup"
        },
        {
          "prefix": "db1:sub:test-feed:beta:",
          "profile": "shared_symbols"
        },
        {
          "default": true,
          "conflation.interval_ms": 250,
          "deduplication.group": "symbols"
        }
      ]
    }
  },
  "instance_lock": {
    "path": "redis-conflated-pubsub.lock"
  },
  "status": {
    "http": {
      "bind": "127.0.0.1",
      "port": 9090
    }
  },
  "logging": {
    "directory": "logs",
    "prefix": "redis-conflated-pubsub",
    "enabled": true,
    "level": "info",
    "retention_days": 14,
    "max_total_size_mb": 1024
  }
}
```

`logging.prefix` names the log files (`{prefix}.{YYYY-MM-DD}[.{n}]`) and scopes retention cleanup, so several services can share one directory without touching each other's files. `logging.enabled: false` disables file logging entirely; the service still runs, and `--check-config` still validates the other fields.

## Profiles and channel policies

Per-output defaults stay in `conflation.interval_ms` and `deduplication.ttl_ms`. Positive intervals and TTLs are limited to 365 days; nonpositive intervals select direct publishing and nonpositive TTLs disable deduplication. Output-local `deduplication_groups` map names to `{ "ttl_ms", "round_ms", "restart_on_change", "max_members", "max_cache_bytes" }`. `round_ms` floors the Unix-epoch timestamp used for the group TTL to a bucket; `0` disables rounding, and flooring can shorten an active TTL by up to `round_ms`. For positive TTLs, `round_ms` cannot exceed `ttl_ms` (checked by `--check-config`). The group cache defaults to 16,384 channels and 64 MiB of combined channel-name/payload bytes; limits can be raised up to 100,000 members and 256 MiB. When full, it evicts the least recently used member. A single member larger than the byte limit is still published but is not cached for deduplication, with a warning.

`deduplication.ttl_ms` is a **diff TTL**: only consecutive identical payloads (same mapped channel and raw bytes) are suppressed, and only until the window expires. A changed payload always publishes (subject to conflation) and replaces the remembered value, so `A→B→A` publishes all three; after the window ends, the current value is sent again even if unchanged, acting as a refresh.

`deduplication.in_flight_suppression` (default `false`) treats a value that was sent but not yet acknowledged as published for suppression purposes, so identical repeats are dropped without waiting for the in-flight batch to settle; a definitive pre-send failure rolls the remembered value back, while an ambiguous outcome keeps it. `deduplication.max_entries` and `deduplication.max_cache_bytes` (optional; unset means unbounded) bound the per-channel TTL cache with LRU eviction and count evictions in `deduplication_evictions_total`.

Each output can bound its intake queue with `queue_max_messages` and `queue_max_bytes` (unset means unbounded) plus `queue_overflow_policy`: `drop_newest` (default), `drop_oldest`, or `drop_by_age` with `queue_max_age_ms`. Shed messages are counted per output and globally in `shed_messages_total`/`shed_payload_bytes_total`, the queue byte gauge is `pending_queue_bytes`, and the oldest pending age is `oldest_pending_age_ms`. Redis Pub/Sub has no replay, so any shedding policy drops events explicitly; the default configuration stays unbounded.

Named `profiles` are partial overrides: set `conflation.interval_ms`, `deduplication.ttl_ms`, or `deduplication.group` as needed; unset values inherit the output defaults. A profile cannot set both `deduplication.ttl_ms` and `deduplication.group`.

Each ordered `channel_policies` entry has exactly one selector: `glob`, `prefix`, `suffix`, or `"default": true` (put the default last). It selects either a profile or inline overrides using literal dotted keys. Policies match the final mapped output channel, after subscription/output prefixes and suffixes; only the first matching policy is selected for each channel. `--check-config` enforces that the default rule is last. Without a group, deduplication TTLs expire per channel; group members share a deadline. `restart_on_change: true` reanchors after a changed value or a new member is successfully or uncertainly published; `false` keeps the expiry fixed. Suppressed duplicates and intermediate conflated values do not renew it. Each output uses one scheduler cadence per distinct positive interval; equal intervals share a cadence.

For local policy-load testing, [`config.channel-policies-load.example.json`](config.channel-policies-load.example.json) has 16 ordered tenant rules and a final default. The ignored release benchmarks also stress 64 generated policies/filters, hot channels, and churn beyond the configured cache capacity:

```sh
cargo test --release --locked channel_policy_cache_load_benchmark -- --ignored --nocapture
cargo test --release --locked filter_cache_load_benchmark -- --ignored --nocapture
```

## Channel filters

`input.filters` evaluates the channel reported by Redis after a configured `SUBSCRIBE`/`PSUBSCRIBE` matches, but before the subscription's `output_prefix`/`output_suffix` is added. Each output may also have its own `filters`; those evaluate the subscription-mapped channel after the input prefix/suffix, but before that output's `channel_prefix`/`channel_suffix`.

Rules are ordered; every rule is evaluated and the last matching rule's `action` (`accept` or `deny`) decides. Non-matching rules do nothing. If no rule matches, `filter_default` decides and defaults to `accept`. A rule has exactly one `glob`, `regex`, `raw`, `single`, or `string` selector. `raw`, `single`, and `string` are aliases for exact equality (`channel == selector`): wildcard characters such as `*` are treated literally. Globs support `*`, `?`, character classes and backslash escapes. Regex uses Rust regex syntax and searches for a match anywhere; use `^` and `$` for a whole-channel match. For example, the final literal rule overrides the earlier regex for that one exact channel:

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

Selectors are compiled once into internal enum variants and the original selector strings are discarded after setup. Cache capacities are set independently: `input.filter_cache_max_entries`, `outputs.<name>.filter_cache_max_entries`, and `outputs.<name>.channel_policy_cache_max_entries`. Each defaults to 16,384, accepts 0 to disable that cache, and is capped at 100,000. The limit applies to each named cache; total memory scales with the number of configured filters and outputs. On eviction, only the least-recently-used channel leaves the cache. An accepted reload recreates workers and their caches.

## Single-container Docker bundle

The optional `Dockerfile.bundle` image contains the relay, Redis, and Valkey 9.1.2. At runtime, `KV_ENGINE` selects which implementation to start (`redis` by default, or `valkey`); the selected engine runs two instances, raw on container port `6379` and conflated on `6380`. The relay config is mounted at `/etc/redis-conflated/config.json`; `config.bundle.example.json` is a starting point. This bundle runs three processes in one container; the regular `Dockerfile` remains the relay-only image for separated deployments.

```sh
cp config.bundle.example.json config.bundle.json
docker build -f Dockerfile.bundle -t redis-conflated-pubsub:bundle .
docker run -d --name redis-conflated-bundle --restart unless-stopped \
  -e KV_ENGINE=redis \
  -e REDIS_PASSWORD='replace-with-a-secret' \
  -p 6379:6379 -p 6380:6380 \
  -v "$PWD/config.bundle.json:/etc/redis-conflated/config.json:ro" \
  -v redis-raw-data:/data/raw \
  -v redis-conflated-data:/data/conflated \
  -v redis-relay-state:/state \
  redis-conflated-pubsub:bundle
```

Set `KV_ENGINE=valkey` to use Valkey instead. `REDIS_PASSWORD` is required; alternatively mount a secret and set `REDIS_PASSWORD_FILE`. Published ports listen on all host interfaces by default; for host-local access use `-p 127.0.0.1:6379:6379 -p 127.0.0.1:6380:6380` or restrict access with the host firewall. Use separate data volumes when changing engine implementations unless you have verified data-file compatibility.

Positive intervals retain the latest message per mapped channel and flush in `MULTI`/`EXEC` transactions. `max_bytes_per_exec` is a per-request target; `max_commands_per_exec` limits commands per transaction. `oversized_message_policy` is `send` (default), `truncate`, or `drop`.

## Reload configuration on Unix (v0.1.4+)

On v0.1.4 and later, send `SIGHUP` to reload the configured JSON without restarting the systemd unit. Older versions require a normal service restart:

```sh
sudo systemctl kill --signal=HUP --kill-whom=main redis-conflated-pubsub.service
```

The service validates the file before switching; an invalid configuration is rejected and the current runtime keeps running. A valid reload drains pending output queues and restarts the Redis input/output workers, so there is a brief Pub/Sub input gap and messages may be missed (Pub/Sub has no replay). Status counters and uptime reset. A changed `instance_lock.path` is acquired during the reload before the old lock is released, and the reload is rejected if the new lock is unavailable. Changes to `logging` or updating the executable itself still require a full service restart.

## Remote input to local Redis

This example forwards `test-feed:*` from a remote Redis input to local Redis at `127.0.0.1:16379`, DB 1. Replace the input endpoint and adjust credentials and limits as needed.

```json
{
  "max_bytes_per_exec": 4194304,
  "oversized_message_policy": "send",
  "client_name": "ConflatedPS-{version}-{role}::{user}::{host}",
  "input": {
    "redis": {
      "host": "redis.example.net",
      "port": 6379,
      "username": null,
      "password_env": null,
      "tls": false,
      "database": 0,
      "connect_timeout_ms": 5000
    },
    "subscriptions": [
      {
        "type": "psubscribe",
        "pattern": "test-feed:*",
        "output_prefix": "remote:",
        "output_suffix": ""
      }
    ]
  },
  "outputs": {
    "output1": {
      "redis": {
        "host": "127.0.0.1",
        "port": 16379,
        "username": null,
        "password_env": null,
        "tls": false,
        "database": 1,
        "connect_timeout_ms": 5000
      },
      "channel_prefix": "local:",
      "channel_suffix": "",
      "conflation": {
        "interval_ms": 250,
        "max_commands_per_exec": 100,
        "max_bytes_per_exec": 1048576
      }
    }
  }
}
```

Here, `test-feed:alpha` becomes `local:remote:test-feed:alpha`. Verify with `redis-cli -h 127.0.0.1 -p 16379 -n 1 PSUBSCRIBE 'local:*'`. `subscribe` uses `channel`; `psubscribe` uses `pattern`. Credentials use `password_env`; set `tls` to `true` to enable TLS.

## Core NATS backend

Input and each output select exactly one backend. Add a `nats` object next to the existing `redis` one; existing Redis configurations are unchanged and still parse and validate verbatim, because the keys are additive.

- Either key alone is valid: `redis` keeps its 0.2 fields and semantics, `nats` selects Core NATS.
- Exactly one backend is required per side. Omitting both, or setting both, fails `--check-config` and startup with a path-qualified message:

  ```text
  input must select exactly one backend, but both `redis` and `nats` are configured
  input must select one backend with `redis` or `nats`
  ```

- The generated JSON Schema encodes the same rule with a `oneOf` on `required: ["redis"]` / `required: ["nats"]` for `input` and for each output.

Core NATS here means **Core NATS Pub/Sub only**, not JetStream: no persistence and no replay.

`nats` endpoint fields:

| Field | Meaning |
| --- | --- |
| `servers` | One or more seed URLs. Required, non-empty. The scheme is mandatory and selects the transport: `nats://` (TCP), `tls://` (TLS), `ws://`/`wss://` (WebSocket); a bare `host:port` is rejected. |
| `name` | Optional connection name reported in the NATS server client listing. |
| `connect_timeout_ms` | Connect and handshake timeout, default `5000`. |
| `ignore_discovered_servers` | When `true`, only the configured `servers` are used and cluster-advertised servers are ignored; useful behind a proxy or WebSocket ingress whose internal addresses are unreachable. |
| `auth` | Authentication variant; defaults to `none`. |
| `tls` | Optional trusted root and/or client identity; see below. |

### The one-transport-per-list rule

Every URL in a single `servers` list must use the same transport family, and `ws`/`wss` cannot be mixed with `nats`/`tls` in one list (one NATS connection requires one family). Different inputs and outputs may use different families, though. Validation rejects a mixed list, for example:

```sh
input.servers mixes nats and wss transports (Tcp and WebSocketTls); one NATS connection requires a single transport family
```

### Authentication

Secrets are always referenced by environment variable name or file path, never written into JSON. Provide the value through the environment or a mounted secret file.

| `auth.type` | Secret reference | Notes |
| --- | --- | --- |
| omitted or `none` | none | Development only; use a protected network. |
| `user_password` | `username` + `password_env` | Username plus the environment variable holding the password. |
| `token` | `token_env` | Environment variable holding the token. |
| `nkey` | `seed_file` | Path to the NKey seed file. |
| `credentials_file` | `path` | Path to the NATS `.creds` file (user JWT **plus its NKey seed**), not a bearer JWT. |

```json
"auth": { "type": "user_password", "username": "relay", "password_env": "NATS_PASSWORD" }
```

```json
"auth": { "type": "token", "token_env": "NATS_TOKEN" }
```

```json
"auth": { "type": "nkey", "seed_file": "/run/secrets/relay.nk" }
```

```json
"auth": { "type": "credentials_file", "path": "/run/secrets/relay.creds" }
```

TLS is separate from `auth` and is selected by the URL scheme; `tls` only extends it with trusted roots or a client identity. For remote password or token connections use `tls://` or `wss://` so the secret is not sent in cleartext.

```json
"tls": {
  "root_cert_file": "/run/secrets/nats-ca.pem",
  "client_cert_file": "/run/secrets/relay-cert.pem",
  "client_key_file": "/run/secrets/relay-key.pem"
}
```

mTLS requires `client_cert_file` and `client_key_file` together (`--check-config` rejects one without the other). Treat `.creds`, NKey seeds, passwords, tokens, and private keys as secrets.

> **Warning:** never inline a password, token, NKey seed, or key material in the JSON. Reference it through an environment variable name (`password_env`, `token_env`) or a file path (`seed_file`, `path`, and the `tls` files), and mount or inject the value at deployment time. Keep secret values out of Git, logs, and process arguments.

### Subjects and wildcards

NATS input subscriptions use the native `nats_subject` type and NATS semantics; Redis `SUBSCRIBE`/`PSUBSCRIBE` globs are **not** translated for you. Use the syntax the selected input actually needs:

- A Redis input uses `{"type": "subscribe", "channel": "prices:EURUSD"}` or `{"type": "psubscribe", "pattern": "prices:*"}` with Redis glob semantics.
- A NATS input uses `{"type": "nats_subject", "subject": "prices.EURUSD"}` or `{"type": "nats_subject", "subject": "prices.>"}`.

In a NATS subject, `.` separates tokens, `*` matches exactly one token, and `>` matches one or more trailing tokens and must be the last token. `prices:*` (Redis) and `prices.>` (NATS) are different patterns; the relay never rewrites one into the other. `--check-config` rejects empty subjects, whitespace, empty tokens, partial wildcards such as `pric*`, and a `>` that is not final.

For NATS outputs, channel mapping keeps its usual meaning: the subscription `output_prefix`/`output_suffix`, then the output `channel_prefix`/`channel_suffix`, are concatenated around the source subject, and the final mapped string is used directly as the publish subject. There is no automatic renaming or escaping, so the mapped result must be a concrete, valid subject (no `*` or `>`); an invalid mapped subject is rejected with a definitive pre-send error instead of being published.

Core NATS has **no per-message acknowledgement and no subscriber count**. A successful publish means the command was accepted by the local client buffer; the `flush` that follows each batch drains that **local write buffer** to the socket, and `async-nats` `flush` does **not** wait for the server to process the commands. So it is neither a server round-trip barrier nor a guarantee that a subscriber received anything. The relay therefore reports a subscriber count of `0` for NATS outputs, so the `subscribers` field in the `output_batch_published` log line is only meaningful for Redis, and the status snapshot exposes no subscriber count at all. Read `publish_errors_total`, `uncertain_messages_total`, `input_reconnects_total` and `output_reconnects_total` for a NATS output, and do not read `0` subscribers as "nobody is listening".

### Loop guard

A relay output published on one connection can match its own input subscription on another, so NATS-to-NATS loops are broken with a `ConflatedPS-Hop: 1` header (vendor-namespaced so it cannot collide with an application header): every NATS output stamps it on each forwarded message, and the NATS input drops any message that already carries it. The payload is never modified, so the guard is invisible to subscribers that ignore headers. **Only the payload is forwarded, not NATS application headers**: the output sets just `ConflatedPS-Hop`, so inbound headers (content-type, tracing, `Nats-Msg-Id`, …) are dropped when a message crosses the relay. Suppressed messages are counted in `loop_suppressed_messages_total`. The guard is NATS-specific: a Redis input has no headers to stamp or read and keeps its existing same-server echo handling, and a Redis input never receives NATS output traffic.

### Example: NATS input to NATS output

Forward `market.>` from a NATS cluster to a NATS archive endpoint, mapping `market.EURUSD` to `archive.market.EURUSD.v1`:

```json
{
  "max_bytes_per_exec": 4194304,
  "oversized_message_policy": "send",
  "client_name": "ConflatedPS-{version}-{role}::{user}::{host}",
  "input": {
    "nats": {
      "servers": ["nats://nats-1:4222", "nats://nats-2:4222"],
      "name": "conflated-input",
      "connect_timeout_ms": 5000,
      "ignore_discovered_servers": false,
      "auth": {
        "type": "user_password",
        "username": "relay",
        "password_env": "NATS_INPUT_PASSWORD"
      }
    },
    "subscriptions": [
      {
        "type": "nats_subject",
        "subject": "market.>",
        "output_prefix": "",
        "output_suffix": ""
      }
    ]
  },
  "outputs": {
    "nats-archive": {
      "nats": {
        "servers": ["tls://nats.example.net:4222"],
        "name": "conflated-archive",
        "auth": {
          "type": "credentials_file",
          "path": "/run/secrets/relay.creds"
        },
        "tls": {
          "root_cert_file": "/run/secrets/nats-ca.pem"
        }
      },
      "channel_prefix": "archive.",
      "channel_suffix": ".v1",
      "conflation": {
        "interval_ms": 250,
        "max_commands_per_exec": 256
      },
      "deduplication": {
        "ttl_ms": 5000
      }
    }
  },
  "instance_lock": {
    "path": "redis-conflated-pubsub.lock"
  },
  "status": {
    "http": {
      "bind": "127.0.0.1",
      "port": 9090
    }
  },
  "logging": {
    "directory": "logs",
    "prefix": "redis-conflated-pubsub",
    "enabled": true,
    "level": "info",
    "retention_days": 14,
    "max_total_size_mb": 1024
  }
}
```

### Example: Redis input to NATS output

Forward `feed:*` from a Redis input to NATS over WebSocket TLS, mapping `feed:alpha` to `nats.feed:alpha`, using the `token` auth variant:

```json
{
  "input": {
    "redis": {
      "host": "redis.example.net",
      "port": 6379,
      "username": null,
      "password_env": "REDIS_PASSWORD",
      "tls": false,
      "database": 0,
      "connect_timeout_ms": 5000
    },
    "subscriptions": [
      {
        "type": "psubscribe",
        "pattern": "feed:*",
        "output_prefix": "",
        "output_suffix": ""
      }
    ]
  },
  "outputs": {
    "nats-out": {
      "nats": {
        "servers": ["wss://nats.example.net:443"],
        "ignore_discovered_servers": true,
        "auth": {
          "type": "token",
          "token_env": "NATS_TOKEN"
        }
      },
      "channel_prefix": "nats.",
      "channel_suffix": "",
      "conflation": {
        "interval_ms": 100
      }
    }
  },
  "instance_lock": {
    "path": "redis-conflated-pubsub.lock"
  }
}
```

A complete NATS configuration example is in [`config.nats.example.json`](config.nats.example.json): a NATS input using `credentials_file`, a NATS output using a token, and a Redis copy output. Validate it before running:

```sh
./redis-conflated-pubsub --check-config --config config.nats.example.json
```

## Verify and monitor

In separate terminals, subscribe to an output and publish to an input channel:

```sh
# Terminal 1
redis-cli PSUBSCRIBE 'db1:*'
# Terminal 2
redis-cli PUBLISH 'test-feed:alpha' 'hello'
```

The optional `status.http` serves per-output metrics at `GET /`; check it with `curl http://127.0.0.1:9090/`. `GET /filters` exposes the full channel names and current filter/policy cache entries; it is disabled by default. Enable it explicitly with `status.http.filters_endpoint_enabled: true` only on a trusted/private interface because channel names can be sensitive. `status.path` writes an atomic JSON snapshot instead. See [Zabbix monitoring](monitoring/README.md) for the collector and templates.

Service connections identify themselves in `CLIENT LIST`. Output connections send `CLIENT SETNAME` on every (re)connect, and the Pub/Sub input does the same on plain TCP: it authenticates and sets the name on the socket before subscribing (Redis keeps the connection name while subscribed, the same approach StackExchange.Redis clients use), so input naming requires a non-TLS input. Every connection also sends `CLIENT SETINFO` (`lib-name`/`lib-ver`) when the server is Redis 7.2 or newer. The `client_name` template defaults to `ConflatedPS-{version}-{role}::{user}::{host}`: `{version}` is the program version, `{role}` is `i` for the input or `o-<output>` for each output, `{user}` is `USERDOMAIN\USERNAME` on Windows or `USER`/`LOGNAME` elsewhere, and `{host}` is `COMPUTERNAME`/`HOSTNAME` (falling back to the OS hostname, so it works under systemd); missing values fall back to `unknown`. A template without `{role}` gets `-<role>` appended, and an empty `client_name` disables naming.

## Observed reduction

An anonymized v0.1.2 snapshot after about 16 seconds: 28,482 input / 9,320 output messages (**67.28% fewer**) and 581,458 / 450,218 input/output payload bytes (**22.57% fewer**). Conflation accounted for 13,524 messages/64,622 bytes and deduplication for 5,636 messages/66,606 bytes; 2 messages/12 bytes were pending. This is workload-specific. An earlier anonymized v0.1.0 run (`interval_ms: 200`, no duplicate TTL) showed **48.2% fewer PUBLISH messages** and **12.0% fewer payload bytes**. Payload-byte difference is input minus output `*_payload_bytes_total`; channels and RESP framing are excluded.

## Deployment and testing

For Linux systemd, install the [unit template](redis-conflated-pubsub.service.example) and enable it:

```sh
sudo install -m 0644 redis-conflated-pubsub.service.example /etc/systemd/system/redis-conflated-pubsub.service
sudo systemctl daemon-reload
sudo systemctl enable --now redis-conflated-pubsub.service
sudo systemctl status redis-conflated-pubsub.service
```

### Linux packages

Releases also ship packages for the four families, all built with [nFPM](https://nfpm.goreleaser.com/) from the same static musl binary. None of them declares dependencies, so one package per family covers every supported release (CI installs and runs them on Ubuntu 20.04, 22.04, 24.04 and 26.04, plus Fedora, Alpine and Arch):

| Family | Package | Install |
| --- | --- | --- |
| Debian/Ubuntu | `redis-conflated-pubsub_<version>-1_amd64.deb` | `sudo apt install ./redis-conflated-pubsub_0.3.0-1_amd64.deb` |
| RHEL/Fedora/SUSE | `redis-conflated-pubsub-<version>-1.x86_64.rpm` | `sudo dnf install ./redis-conflated-pubsub-0.3.0-1.x86_64.rpm` |
| Alpine | `redis-conflated-pubsub_<version>-r1_x86_64.apk` | `sudo apk add --allow-untrusted ./redis-conflated-pubsub_0.3.0-r1_x86_64.apk` |
| Arch | `redis-conflated-pubsub-<version>-1-x86_64.pkg.tar.zst` | `sudo pacman -U redis-conflated-pubsub-0.3.0-1-x86_64.pkg.tar.zst` |

The packages are unsigned, like the release archives, so each package manager asks for confirmation (or needs `--allow-untrusted` on Alpine). Then edit the configuration and start the service:

```sh
sudo editor /etc/redis-conflated-pubsub/config.json
sudo systemctl enable --now redis-conflated-pubsub.service   # Debian, RPM family, Arch
sudo rc-update add redis-conflated-pubsub default            # Alpine (OpenRC)
sudo rc-service redis-conflated-pubsub start
```

Each package installs `/usr/bin/redis-conflated-pubsub`, the systemd unit at `/usr/lib/systemd/system/redis-conflated-pubsub.service` (Alpine gets an OpenRC script in `/etc/init.d` instead), a `sysusers.d` drop-in that defines the `redis-conflated-pubsub` system user, and the configuration files `/etc/redis-conflated-pubsub/config.json` plus `/etc/redis-conflated-pubsub/redis.env` (mode `0600`, for the `*_env` secret fields). Logs and state live in `/var/log/redis-conflated-pubsub` and `/var/lib/redis-conflated-pubsub`; installation never enables or starts the service, and a Debian `purge` removes the user, the state and the configuration. See [`nfpm.yaml`](nfpm.yaml) and [`packaging/`](packaging) for the layout.

See [Docker build, run, and integration-test instructions](docker/README.md) and [CI/release details](ci/README.md).
