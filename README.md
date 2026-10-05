# Redis Conflated Pub/Sub

A cross-platform Rust service for forwarding Redis Pub/Sub messages between channels or Redis endpoints, either immediately or with per-channel conflation.

The service reads a JSON configuration file. JSON is configuration only: message payloads are treated as opaque bytes and are never parsed, wrapped, or re-encoded.

## Features

- Subscribe to Redis channels with `SUBSCRIBE` or glob-style patterns with `PSUBSCRIBE`.
- Forward each message immediately, or retain the latest message per source channel for periodic publishing.
- Preserve each payload byte-for-byte and optionally add a prefix and/or suffix to the output channel.
- Bound pending message counts, memory use, individual message size, and batch size.
- Expose optional status snapshots as a file and/or through a small HTTP endpoint.

## Message forwarding

The output channel is the complete source channel with the configured `output.channel_prefix` prepended and `output.channel_suffix` appended. The payload bytes are passed to Redis `PUBLISH` unchanged.

- When `conflation.interval_ms <= 0`, each message is passed through immediately without conflation.
- When `conflation.interval_ms > 0`, the service stores the latest message for each concrete source channel in an in-memory `HashMap`. At each interval, it flushes the retained messages through a Redis `MULTI`/`EXEC` pipeline containing one `PUBLISH` command per message. Every command produces its own individual Redis Pub/Sub event; the messages are not combined into one event.

Redis logical database IDs do not isolate Pub/Sub. Subscribers receive matching publications across logical databases on the same Redis server. If input and output use the same Redis endpoint, configure a non-empty channel prefix or suffix. The service ignores incoming channels that start with the configured output prefix or end with the configured output suffix to prevent its own publications from creating an echo loop. Choose a prefix or suffix that does not unintentionally exclude source channels.

Pub/Sub is best-effort: Redis does not retain messages for disconnected subscribers. A network failure around `PUBLISH` or `EXEC` can also make a retry appear as a duplicate, so a transaction is not a durable queue.

## Build and run

Install the Rust toolchain selected by [`rust-toolchain.toml`](rust-toolchain.toml), then build and start the service:

```sh
cargo build --release --locked
cp config.example.json config.json
./target/release/redis-conflated-pubsub --check-config --config config.json
./target/release/redis-conflated-pubsub --config config.json
```

On Windows, run `target\release\redis-conflated-pubsub.exe` with the same arguments.

The following complete `config.json` example uses the same fields and structure as [`config.example.json`](config.example.json). It subscribes to all channels on DB 0, conflates for 250 ms, and publishes to DB 1 with a `replica:` channel prefix. Replace `localhost` with the Redis host reachable by the service in your environment.

```json
{
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
      { "type": "psubscribe", "pattern": "*" }
    ]
  },
  "output": {
    "redis": {
      "host": "localhost",
      "port": 6379,
      "username": null,
      "password_env": null,
      "tls": false,
      "database": 1,
      "connect_timeout_ms": 5000
    },
    "channel_prefix": "replica:",
    "channel_suffix": ""
  },
  "conflation": {
    "interval_ms": 250,
    "max_pending_keys": 10000,
    "max_pending_bytes": 52428800,
    "max_message_bytes": 10485760,
    "max_batch_bytes": 73400320
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

Set `conflation.interval_ms` to zero or a negative number for immediate passthrough. With a positive value, the other `conflation` limits bound pending channels, pending bytes, individual payload size, and each output batch.

Redis credentials are optional. Set `username` as needed; `password_env` names an environment variable containing the password. Export that variable before starting the process. Set `tls` to `true` if the Redis endpoint requires TLS. Subscriptions can use either `{ "type": "subscribe", "channel": "..." }` or `{ "type": "psubscribe", "pattern": "..." }`.

The `status` section is optional and can be omitted entirely. Within it, `path` is an optional destination for an atomically replaced JSON status snapshot, and `http` is an optional listener. When enabled, the HTTP listener exposes only `GET /`, which returns the status snapshot as JSON. The example enables HTTP status on `127.0.0.1:9090` and does not write a status file. To enable a status file, add a `path` such as `"path": "status.json"`; to disable both status outputs, remove the whole `status` section.

Configuration, lock, status, and log paths are relative to the process working directory unless given as absolute paths. The service acquires an exclusive OS file lock at `instance_lock.path` before connecting to Redis. Use the same lock path for invocations that must be mutually exclusive. The operating system releases the lock when the process exits; the lock file itself may remain and can be reused.

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

The Docker Compose test starts a local Redis server, the service, a random publisher, and an integration-test client. The service subscribes on logical DB 0 using `PSUBSCRIBE test-feed:*` and `PSUBSCRIBE replica:*`; the random publisher publishes from DB 2; and the service publishes output on DB 1 with the `replica:` prefix. The integration-test client uses `PSUBSCRIBE replica:*` to filter for output channels on DB 1. It verifies cross-database Pub/Sub behavior, channel-prefix filtering, conflation, byte-for-byte payload preservation, and output-loop prevention.

Run the test from the project root:

```sh
docker compose -f compose.test.yml up --build --abort-on-container-exit --exit-code-from integration-test
docker compose -f compose.test.yml down --volumes
```

## Monitoring

The [`monitoring/`](monitoring/) folder contains a Python `zabbix_sender` collector, a configuration example, and importable templates for Zabbix 5.0 and Zabbix 7.0. The collector reads the optional HTTP status JSON from `GET /` and sends the collected metrics to Zabbix. See [`monitoring/README.md`](monitoring/README.md) for setup instructions.

## Reliability and limits

- Redis Pub/Sub does not buffer messages while this service or a subscriber is disconnected.
- A positive conflation interval replaces intermediate messages with the latest payload for the same source channel. A non-positive interval passes messages through individually.
- Messages that exceed the configured queue or size limits are counted in enabled status outputs and logs.
- Output publishing is retried after errors. A timeout can occur after Redis accepted a command or transaction, so consumers should tolerate duplicate messages.
- Redis Cluster supports only logical database 0; nonzero database IDs require a non-cluster Redis server.

## CI and releases

GitHub Actions runs formatting, Clippy, and tests on Ubuntu, Windows, and macOS. A static Linux `musl` binary is also smoke-tested in Ubuntu containers.

Pushing a `v*` tag builds release binaries for Linux x86_64, Windows x86_64, and macOS Intel and Apple Silicon, then publishes archives and SHA-256 checksums as a GitHub Release.
