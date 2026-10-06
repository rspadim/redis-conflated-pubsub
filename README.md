# Redis Conflated Pub/Sub

A cross-platform Rust service that reads from one Redis Pub/Sub input and forwards messages to multiple independently configured Redis outputs.

The JSON file configures the service only. By default, message payloads remain opaque bytes and are forwarded unchanged; they are never parsed, wrapped in a JSON envelope, or re-encoded. Only an explicit `truncate` policy removes a payload suffix, while `drop` skips that output. Payloads and pending conflation queues stay in memory and are never written to disk; only status snapshots and logs are persisted, if configured.

## Features

- Subscribe to Redis channels with `SUBSCRIBE` or glob-style patterns with `PSUBSCRIBE`.
- Map matching input channels with the matching subscription rule, then apply each named output's optional channel prefix/suffix before publishing to its independent Redis endpoint/database.
- Forward messages immediately or retain the latest message per source channel for periodic publishing.
- Optionally write status snapshots to a file and/or expose them through a small HTTP endpoint.

## Message forwarding

Each individual `input.subscriptions[]` entry can define `output_prefix` and `output_suffix`, and each `outputs.<name>` entry can define its own `channel_prefix` and `channel_suffix`. A matching subscription rule routes the message to every configured output. For each output, the final channel is composed exactly as `outputs.<name>.channel_prefix + input.subscriptions[].output_prefix + source channel + input.subscriptions[].output_suffix + outputs.<name>.channel_suffix`. All four prefix/suffix fields are optional, default to empty strings, and may be omitted at either layer. With `oversized_message_policy: "send"`, Redis receives the original payload bytes unchanged.

Each named output has its own `conflation.interval_ms` and `conflation.max_commands_per_exec`, so it independently controls its forwarding cadence and command-count transaction limit. Top-level `max_bytes_per_exec` sets the default `MULTI`/`EXEC` byte target; `outputs.<name>.conflation.max_bytes_per_exec` can override that target for one output.

- When `interval_ms <= 0`, that output issues one direct `PUBLISH` for each message as it arrives; it does not use `MULTI`/`EXEC`.
- When `interval_ms > 0`, that output keeps the latest message for each concrete mapped channel in memory. A newer message for the same channel replaces its pending value. At each flush, retained messages are published in one or more Redis `MULTI`/`EXEC` transactions. The worker chunks by the exact encoded RESP request size and that output's `max_commands_per_exec`, whichever bound is reached first. The size includes RESP framing, channel bytes, payload bytes, `MULTI`, and `EXEC`.
- If one request exceeds the effective `max_bytes_per_exec` target, `oversized_message_policy` controls that output: `send` (default) sends it alone and preserves the payload, `truncate` trims only the payload suffix to fit the target, and `drop` skips that output's publication. Direct outputs compare the standalone `PUBLISH`; conflated outputs include the `MULTI`/`EXEC` wrappers. If the channel and RESP framing alone exceed the target, truncation cannot fit and the message is dropped. The global policy can be overridden by `outputs.<name>.conflation.oversized_message_policy`. `send` does not bypass Redis's own server limits.

Output failures are not retried. The failed chunk/message is counted and given up, error logs are rate-limited, and processing continues with the next chunk or incoming message. If a reply is lost after Redis may have executed a request, status records the result as uncertain; not retrying avoids duplicate publications but cannot guarantee delivery.

Redis logical database IDs do not isolate Pub/Sub. A subscriber on a Redis server receives matching publications from that server regardless of the database selected by the publisher or subscriber. Therefore, using DB 0 for input and DB 1 for output does not prevent an echo, and an output on DB 0 is not isolated from the input either. For outputs on the same Redis server as the input, echo suppression reserves the composed output namespace produced by the output-level and subscription-level prefixes/suffixes. Avoid mappings whose reserved composed namespaces overlap source channels that should be consumed.

Redis Pub/Sub does not retain messages for disconnected subscribers. Positive-interval conflation intentionally supersedes intermediate messages for a channel. Payloads and pending queues exist only in memory and are never persisted. `max_bytes_per_exec` is an application request-size target, not a Redis server limit or a cap on the complete flush; chunks continue until the flush is processed. `max_commands_per_exec` limits the number of `PUBLISH` commands per transaction, not the total number of messages in a flush. Pub/Sub is not a durable replay queue.

## Build and run

Install the Rust toolchain selected by [`rust-toolchain.toml`](rust-toolchain.toml), save the complete example below as `config.json`, then build and start the service:

```sh
cargo build --release --locked
./target/release/redis-conflated-pubsub --check-config --config config.json
./target/release/redis-conflated-pubsub --config config.json
```

On Windows, run `target\release\redis-conflated-pubsub.exe` with the same arguments.

This complete English `config.json` example uses generic local Redis settings; the input feed sends `test-feed:*` channels on DB 0. The matching rule adds `sub:` before and `:source` after each source channel; output `output0` adds `db0:` and publishes immediately to DB 0, while output `output1` adds `db1:` and independently conflates for 250 ms on DB 1. For example, `test-feed:alpha` becomes `db0:sub:test-feed:alpha:source` on DB 0 and `db1:sub:test-feed:alpha:source` on DB 1. The top-level 4 MiB `max_bytes_per_exec` is the default encoded-request target and the root oversized-message policy is `send`; `output1` overrides the target to 2 MiB and allows at most two `PUBLISH` commands per transaction. An output may explicitly override the root policy with `outputs.<name>.conflation.oversized_message_policy`. A flush beyond either bound is split into transactions. Consumers can use `PSUBSCRIBE db0:*` and `PSUBSCRIBE db1:*`; after each output prefix, the remaining channel retains the source channel mapped by the input subscription rule.

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
      "conflation": {
        "interval_ms": 0,
        "max_commands_per_exec": 100
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
      "conflation": {
        "interval_ms": 250,
        "max_commands_per_exec": 2,
        "max_bytes_per_exec": 2097152
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
    "level": "info",
    "retention_days": 14,
    "max_total_size_mb": 1024
  }
}
```

`outputs` is a JSON object keyed by arbitrary output names such as `output0` and `output1`. Each output entry independently defines its Redis connection, optional `channel_prefix`/`channel_suffix`, and nested conflation settings. Top-level `max_bytes_per_exec` is the default encoded-request target for every output transaction; a named output can override it with `outputs.<name>.conflation.max_bytes_per_exec`. `max_commands_per_exec` is configured per output and bounds the command count in each transaction. The root `oversized_message_policy` defaults to `send`; an output may override it under `conflation`. `truncate` limits only that output's payload bytes, and `drop` skips only that output's publication. In this example, `output0` inherits the 4 MiB target and has interval `0` for immediate PUBLISH on DB 0; `output1` has interval `250`, overrides the target to 2 MiB, and allows two `PUBLISH` commands per transaction. Every subscription can independently define optional `output_prefix`/`output_suffix`; these are composed with each output's own prefix/suffix, so the final channel can differ by output. Omitted prefix/suffix fields default to empty strings. Every output maintains its own conflation state, flush schedule, and transaction limit.

### Optional remote-input/local-output test

To test forwarding from a remote Redis input to a local Redis instance, use the following `input` and `outputs` sections in your ignored local `config.json`. This example listens for `test-feed:*` on input DB 0, then forwards to local Redis at `127.0.0.1:16379`, DB 1, with a 250 ms conflation interval:

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

`redis.example.net` is a documentation placeholder. Replace it only in the ignored local `config.json` with the Redis endpoint for your test; use generic sample values in committed documentation and tests. The optional output-level `channel_prefix`/`channel_suffix` fields can be omitted or left empty. Here, a source channel such as `test-feed:alpha` is published as `local:remote:test-feed:alpha`; a local consumer can listen with `redis-cli -h 127.0.0.1 -p 16379 -n 1 PSUBSCRIBE 'local:*'`. The top-level byte target is 4 MiB by default, while this output overrides it to 1 MiB; each `MULTI`/`EXEC` also contains at most 100 `PUBLISH` commands.

Redis credentials are optional. Set `username` as needed; `password_env` names an environment variable containing the password. Export that variable before starting the process. Set `tls` to `true` if an endpoint requires TLS. Subscriptions can use either `{ "type": "subscribe", "channel": "..." }` or `{ "type": "psubscribe", "pattern": "..." }`; either rule can also set `output_prefix` and `output_suffix`. Output-level `channel_prefix` and `channel_suffix` can be set separately on each named output.

The `status` section is optional and can be omitted entirely. Within it, `path` optionally writes an atomically replaced JSON status snapshot, and `http` optionally enables an HTTP listener. Both the status file and HTTP `GET /` snapshot include per-output metrics under an `outputs` object keyed by the same names as the configuration (for example, `outputs.output0` and `outputs.output1`). The HTTP listener exposes only `GET /`, which returns the status snapshot as JSON; there are no other routes. The example enables HTTP status on `127.0.0.1:9090` and does not write a status file. To enable one, add a `path` such as `"path": "status.json"`; to disable both status outputs, remove the whole `status` section.

For a positive `conflation.interval_ms`, an output flushes when its periodic timer fires and it has pending values. A flush may use multiple transactions according to that output's effective byte target and `max_commands_per_exec`; application chunking limits do not drop the remaining flush. A Redis publish error does discard and count its affected chunk, then the output proceeds to the next one. Later timer ticks while idle do not publish empty batches. Per-output `pending_keys` reports retained or queued entries for that output (not total messages received); the global `pending_keys` is the aggregate across outputs. Passthrough outputs count queued individual messages. If status includes per-output `message_reduction_percent`, it is the percentage of messages received by that output that conflation did not publish; `payload_reduction_percent` is the corresponding percentage of payload bytes not published. These reductions describe conflation, not durable delivery or recovery.

Configuration, lock, status, and log paths are relative to the process working directory unless given as absolute paths. The service acquires an exclusive OS file lock at `instance_lock.path` before connecting to Redis. Use the same lock path for invocations that must be mutually exclusive. The operating system releases the lock when the process exits; the lock file itself may remain and can be reused.

## Optional Linux systemd deployment

The root-level [`redis-conflated-pubsub.service.example`](redis-conflated-pubsub.service.example) is a systemd unit template. It uses `/opt/redis-conflated-pubsub/` as its working directory and expects the binary there. Put the JSON config at `/etc/redis-conflated-pubsub/config.json`; configure the lock and optional status paths under `/var/lib/redis-conflated-pubsub/` and the log directory under `/var/log/redis-conflated-pubsub/`. Ensure the service account can read the config and optional `redis.env` file. Then install and enable it:

```sh
sudo install -m 0644 redis-conflated-pubsub.service.example /etc/systemd/system/redis-conflated-pubsub.service
sudo systemctl daemon-reload
sudo systemctl enable --now redis-conflated-pubsub.service
sudo systemctl status redis-conflated-pubsub.service
```

## Docker

Build and run the Linux image with a mounted configuration and writable state directory:

```sh
docker build --tag redis-conflated-pubsub:local .
mkdir -p state
docker run --rm \
  --workdir /state \
  --volume "$PWD/config.json:/config.json:ro" \
  --volume "$PWD/state:/state" \
  redis-conflated-pubsub:local \
  --config /config.json
```

Relative lock, status, and log paths resolve from `/state` in this example. The image uses `scratch`, so it contains no shell or configuration file; mount the JSON configuration at runtime. To reach the optional status endpoint from outside the container, set `status.http.bind` to `0.0.0.0` and publish the configured port, for example with `--publish 9090:9090`.

### Local Redis integration test

The Docker Compose test starts a local Redis server, the service, a random publisher, and an integration-test client. It exercises the generic `test-feed:*` channels and verifies cross-database Pub/Sub behavior, output channel mapping, latest-per-channel conflation, byte-for-byte payload preservation, output-loop prevention, and the root status endpoint.

Run the test from the project root:

```sh
docker compose -f compose.test.yml up --build --abort-on-container-exit --exit-code-from integration-test
docker compose -f compose.test.yml down --volumes
```

The separate oversized-request test configures Redis with a 1 MiB `client-query-buffer-limit`. It verifies that a larger `MULTI`/`EXEC` chunk fails, is not retried, and does not prevent the next smaller chunk from publishing:

```sh
docker compose -f compose.oversize.test.yml up --build --abort-on-container-exit --exit-code-from oversize-integration-test
docker compose -f compose.oversize.test.yml down --volumes
```

The protocol behavior test feeds multiple Pub/Sub messages whose combined payload exceeds 1 MiB while Redis enforces a 1 MiB query-buffer limit. It verifies that byte-sized conflation keeps each transaction below the Redis limit, checks `send`/`truncate`/`drop` against actual subscriber payloads and proxy-measured RESP bytes, and confirms `interval_ms: 0` emits only individual `PUBLISH` commands with no `MULTI`/`EXEC`:

```sh
docker compose -f compose.protocol.test.yml up --build --abort-on-container-exit --exit-code-from protocol-integration-test
docker compose -f compose.protocol.test.yml down --volumes
```

## Monitoring

The [`monitoring/`](monitoring/) folder contains a Python `zabbix_sender` collector, a configuration example, and importable templates for Zabbix 5.0 and Zabbix 7.0. The collector reads the optional HTTP status JSON from `GET /` and sends the collected metrics to Zabbix. See [`monitoring/README.md`](monitoring/README.md) for setup instructions.

## Reliability and limits

- Redis Pub/Sub does not buffer messages while this service or a subscriber is disconnected.
- A positive conflation interval intentionally replaces intermediate messages with the latest payload for the same source channel; non-positive intervals forward messages individually.
- Each configured output uses its own Redis endpoint/database, channel prefix/suffix, interval, and maximum `PUBLISH` commands per transaction; top-level `max_bytes_per_exec` supplies the encoded-request target unless that output overrides it. RESP request accounting is exact: `MULTI` is 15 bytes, `EXEC` is 14 bytes, and each `PUBLISH` is `4 + bulk(7) + bulk(channel_bytes) + bulk(payload_bytes)`, where `bulk(n) = n + decimal_digits(n) + 5`. The `EXEC` response is separate server-to-client traffic and is not part of the request-byte target. Matching input subscription rules provide an additional channel prefix/suffix used by all outputs. Redis databases on the same server remain part of the same Pub/Sub namespace and do not isolate Pub/Sub traffic.
- `max_bytes_per_exec` is an application sizing target, not discovery of the remote server's hard limits. Redis settings such as `client-query-buffer-limit` and `proto-max-bulk-len` can differ. With `oversized_message_policy: "send"`, a single command over the target is sent alone and Redis may accept or reject it. The opt-in `truncate` and `drop` policies apply when one encoded `PUBLISH` exceeds the configured target.
- Output publishes are not retried after an error. A failed chunk is counted, rate-limited in logs, and processing continues with subsequent chunks; a failed immediate `PUBLISH` does not block later messages. If Redis executed `EXEC` but its reply was lost, status reports the outcome as uncertain. Not retrying avoids duplicates caused by this service but cannot guarantee delivery; error and affected-message counters are exposed in `GET /`.
- Redis Cluster supports only logical database 0; nonzero database IDs require a non-cluster Redis server.

## CI and releases

GitHub Actions runs formatting, Clippy, and tests on Ubuntu, Windows, and macOS. A static Linux `musl` binary is also smoke-tested in Ubuntu containers.

Pushing a `v*` tag builds release binaries for Linux x86_64, Windows x86_64, and macOS Intel and Apple Silicon, then publishes archives and SHA-256 checksums as a GitHub Release.
