# Redis Conflated Pub/Sub

A Rust relay that forwards Redis Pub/Sub messages to independently configured Redis outputs.

Latest published release: **v0.1.4**; next planned release: **v0.2.0**. [Download a platform binary](https://github.com/rspadim/redis-conflated-pubsub/releases) or see the [developer build instructions](src/README.md).

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
        "ttl_ms": 5000
      }
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
    "level": "info",
    "retention_days": 14,
    "max_total_size_mb": 1024
  }
}
```

## Profiles and channel policies

Per-output defaults stay in `conflation.interval_ms` and `deduplication.ttl_ms`. Positive intervals and TTLs are limited to 365 days; nonpositive intervals select direct publishing and nonpositive TTLs disable deduplication. Output-local `deduplication_groups` map names to `{ "ttl_ms", "round_ms", "restart_on_change", "max_members", "max_cache_bytes" }`. `round_ms` floors the Unix-epoch timestamp used for the group TTL to a bucket; `0` disables rounding, and flooring can shorten an active TTL by up to `round_ms`. For positive TTLs, `round_ms` cannot exceed `ttl_ms` (checked by `--check-config`). The group cache defaults to 16,384 channels and 64 MiB of combined channel-name/payload bytes; limits can be raised up to 100,000 members and 256 MiB. When full, it evicts the least recently used member. A single member larger than the byte limit is still published but is not cached for deduplication, with a warning.

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

The optional `Dockerfile.bundle` image contains the relay plus both Redis and Valkey server binaries. At runtime, `KV_ENGINE` selects which implementation to start (`redis` by default, or `valkey`); the selected engine runs two instances, raw on container port `6379` and conflated on `6380`. The relay config is mounted at `/etc/redis-conflated/config.json`; `config.bundle.example.json` is a starting point. This bundle runs three processes in one container; the regular `Dockerfile` remains the relay-only image for separated deployments.

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

The service validates the file before switching; an invalid configuration is rejected and the current runtime keeps running. A valid reload drains pending output queues and restarts the Redis input/output workers, so there is a brief Pub/Sub input gap and messages may be missed (Pub/Sub has no replay). Status counters and uptime reset. Changes to `logging` or `instance_lock`, or updating the executable itself, still require a full service restart.

## Remote input to local Redis

This example forwards `test-feed:*` from a remote Redis input to local Redis at `127.0.0.1:16379`, DB 1. Replace the input endpoint and adjust credentials and limits as needed.

```json
{
  "max_bytes_per_exec": 4194304,
  "oversized_message_policy": "send",
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

## Verify and monitor

In separate terminals, subscribe to an output and publish to an input channel:

```sh
# Terminal 1
redis-cli PSUBSCRIBE 'db1:*'
# Terminal 2
redis-cli PUBLISH 'test-feed:alpha' 'hello'
```

The optional `status.http` serves per-output metrics at `GET /`; check it with `curl http://127.0.0.1:9090/`. `GET /filters` exposes the full channel names and current filter/policy cache entries; it is disabled by default. Enable it explicitly with `status.http.filters_endpoint_enabled: true` only on a trusted/private interface because channel names can be sensitive. `status.path` writes an atomic JSON snapshot instead. See [Zabbix monitoring](monitoring/README.md) for the collector and templates.

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

See [Docker build, run, and integration-test instructions](docker/README.md) and [CI/release details](ci/README.md).
