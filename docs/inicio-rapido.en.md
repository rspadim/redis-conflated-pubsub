# Quick start

## 1. Download the binary

Grab the archive for your platform from the
[releases page](https://github.com/rspadim/redis-conflated-pubsub/releases): Linux
x86_64 (static musl), Windows x86_64, macOS Intel or macOS Apple Silicon. Every
release also publishes the matching `.sha256` next to the archive.

To build from source, see [Development](desenvolvimento.md).

## 2. Validate the configuration

Save one of the configurations below as `config.json` and run the validation.
`--check-config` checks the JSON and the semantic rules, including same-server
echo-loop prevention.

```sh
./redis-conflated-pubsub --check-config --config config.json
```

On Windows use `redis-conflated-pubsub.exe` with the same arguments.

## 3. Run it

```sh
./redis-conflated-pubsub --config config.json
```

## Local Redis example

This example maps `test-feed:alpha` to `db0:sub:test-feed:alpha:source` and
`db1:sub:test-feed:alpha:source`.

- `output0` does **not** conflate (it publishes directly) but still deduplicates
  identical payloads for 5 seconds.
- `output1` uses 250 ms conflation, a 5-second TTL, per-channel profiles and a
  shared deduplication group.

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

## Verify

In separate terminals, subscribe to an output and publish to the input:

```sh
# Terminal 1 — subscribe to the outputs
redis-cli PSUBSCRIBE 'db0:*'
# Terminal 2 — publish to the input
redis-cli PUBLISH 'test-feed:alpha' 'hello'
```

Optionally check the per-output metrics:

```sh
curl http://127.0.0.1:9090/
```

## Remote input to a local Redis

Forward `test-feed:*` from a remote Redis to a local Redis at `127.0.0.1:16379`,
DB 1. Here, `test-feed:alpha` becomes `local:remote:test-feed:alpha`.

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

```sh
redis-cli -h 127.0.0.1 -p 16379 -n 1 PSUBSCRIBE 'local:*'
```

`subscribe` uses `channel`; `psubscribe` uses `pattern`. Credentials always go
through `password_env`; set `tls: true` to enable TLS.

## Next step

- Tune profiles, policies and filters in [Configuration](configuracao.md).
- Move to NATS in [NATS backend](nats.md).
- Deploy with [Docker](docker.md) or [systemd](operacao.md).
