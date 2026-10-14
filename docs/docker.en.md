# Docker and tests

Run all commands **from the repository root**: `Dockerfile` and the
`compose.*.yml` files live there and the example paths are relative to it.

## Build and run

Build the Linux image and run it with a mounted configuration and a writable
state directory:

```sh
docker build --tag redis-conflated-pubsub:local .
cp config.example.json config.json
# Edit config.json so the Redis addresses are reachable from the container.
mkdir -p state
docker run --rm \
  --workdir /state \
  --volume "$PWD/config.json:/config.json:ro" \
  --volume "$PWD/state:/state" \
  redis-conflated-pubsub:local \
  --config /config.json
```

Mount the runtime config at `/config.json`; the image uses `scratch` and contains
neither a shell nor a config file. Relative lock, status and log paths resolve
from `/state` in this example. To expose the status endpoint, configure the bind
as `0.0.0.0` and publish the port, for example `--publish 9090:9090`.

## Single-container (bundle) image with Redis/Valkey

The optional `Dockerfile.bundle` contains the relay, Redis and Valkey 9.1.2. At
runtime, `KV_ENGINE` selects the implementation (`redis` by default, or `valkey`);
the selected engine runs **two** instances: raw on port `6379` and conflated on
`6380`. The relay config is mounted at `/etc/redis-conflated/config.json`;
`config.bundle.example.json` is the starting point. That is three processes in one
container; the regular `Dockerfile` remains the relay-only image.

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

Set `KV_ENGINE=valkey` to use Valkey. `REDIS_PASSWORD` is required; alternatively
mount a secret and set `REDIS_PASSWORD_FILE`. Published ports listen on all
interfaces; for host-local access use
`-p 127.0.0.1:6379:6379 -p 127.0.0.1:6380:6380`. Use separate data volumes when
changing engine, unless you have verified file compatibility.

## Integration tests (Compose)

Each test creates project-scoped containers, networks and volumes; there are no
fixed container names, IPs or subnets. Run `up` from the repository root and the
matching `down` afterwards. For parallel runs, pass a unique `-p <project>`.

| Stack | What it checks |
| --- | --- |
| `compose.test.yml` | Channel mapping, cross-database Pub/Sub, conflation, binary payloads, echo prevention and HTTP status. |
| `compose.filters.test.yml` | Ordered input/output filters, namespace mapping and the `GET /filters` cache snapshot. |
| `compose.oversize.test.yml` | Redis's 1 MiB query-buffer limit and failed-chunk handling. |
| `compose.protocol.test.yml` | Byte-sized transactions, `send`/`truncate`/`drop` policies and direct `PUBLISH` without a transaction. |
| `compose.ttl.test.yml` | Duplicate suppression, individual and grouped TTL, policy selection, valid and invalid `SIGHUP`. |
| `compose.test.yml --profile fault` | Uncertain/failed publish handling through the Rust fault proxy. |

```sh
docker compose -f compose.test.yml up --build --abort-on-container-exit --exit-code-from integration-test
docker compose -f compose.test.yml down --volumes --remove-orphans
```

The drivers and proxies come from `examples/e2e.rs` (the `redis-conflated-e2e`
binary, built by `Dockerfile.e2e`), replacing the old Python scripts.

## End-to-end hot path

The hot-path benchmark (`examples/hotpath_benchmark.rs`, image
`Dockerfile.benchmark`) runs publishers and both output subscribers in one Rust
process. The default image is Valkey 9.1.2.

```sh
docker compose -p hotpath-valkey -f compose.hotpath.test.yml up --build --abort-on-container-exit --exit-code-from hotpath-benchmark
docker compose -p hotpath-valkey -f compose.hotpath.test.yml down --volumes --remove-orphans
```

For Redis, select the image and CLI explicitly:

```sh
HOTPATH_KV_IMAGE=redis:7.4-alpine HOTPATH_KV_CLI=redis-cli \
docker compose -p hotpath-redis -f compose.hotpath.test.yml up --build --abort-on-container-exit --exit-code-from hotpath-benchmark
```

Tuning variables:

| Variable | Effect | Default |
| --- | --- | --- |
| `HOTPATH_PUBLISHER_COUNT` | Number of concurrent publishers. | — |
| `HOTPATH_MESSAGES_PER_PUBLISHER` | Messages per publisher. | — |
| `HOTPATH_SERIAL_MESSAGES` | Measured serial requests. | 250 |
| `HOTPATH_MODE` | `closed-loop` (waits for every ACK) or `open-loop` (non-atomic pipelines). | `closed-loop` |
| `HOTPATH_PIPELINE` | Commands per pipeline in open-loop. | 64 |
| `HOTPATH_CONNECTIONS` | Connections per publisher in open-loop. | 1 |
| `HOTPATH_OUTPUTS` | One or two outputs. | 2 |
| `HOTPATH_MIN_NUMPAT` | `PUBSUB NUMPAT` level the harness waits for before publishing (for shared servers). | 1 |

The mixed-policy workload (about one-third direct, one-third 200 ms conflation
and one-third 200 ms + 5 s TTL) uses another harness, selected with
`HOTPATH_BENCHMARK_CMD='hotpath-mix-benchmark'`:

```sh
HOTPATH_CONFIG_FILE=./tests/docker-hotpath-mix-config.json \
HOTPATH_BENCHMARK_CMD='hotpath-mix-benchmark' \
docker compose -p hotpath-mixed -f compose.hotpath.test.yml up --build --abort-on-container-exit --exit-code-from hotpath-benchmark
```

## Loopback (no Redis/Valkey)

Runs the service against a minimal RESP2 broker implemented in Rust, without
Redis containers:

```sh
docker compose -p loopback -f compose.loopback.test.yml up --build --abort-on-container-exit --exit-code-from loopback-benchmark
```

`LOOPBACK_MESSAGES` (default 1,000,000), `LOOPBACK_CHUNK`, `LOOPBACK_WARMUP` and
`LOOPBACK_TIMEOUT_S` tune the run. Because the broker shares CPU with the
service, treat the numbers as service + broker measurements.

## Feed capture and replay

The benchmark image also ships `capture-feed` and `replay-feed`:

- `capture-feed` subscribes read-only (`CAPTURE_PATTERN`, default `*`) and records
  **metadata only** per message: relative timestamp, channel and payload length.
  Contents are never written. It emits NDJSON plus a summary JSON (rate,
  per-second series, size buckets, top channels).
- `replay-feed` republishes a capture to a target Redis (`REPLAY_HOST`/`REPLAY_PORT`)
  preserving the inter-arrival timing, with rate scaling (`REPLAY_RATE_SCALE`),
  pipelined chunks (`REPLAY_PIPELINE`), synthetic payloads of the recorded length
  and optional per-channel repetition (`REPLAY_DUP_EVERY`) to exercise the TTL.
  With `REPLAY_STATUS_URL` it prints the queue/publish counters before and after.

Capture files stay local (`tests/.tmp-*` is gitignored); only anonymized
aggregates should be committed.

## CI and releases

The workflows are in `.github/workflows/`. `ci.yml` runs on pushes to `main`,
pull requests and manual dispatch: formatting, Clippy with warnings as errors,
unit tests and release builds on several platforms, plus the static Linux image,
Compose E2Es, spike harnesses and monitoring validation. `release.yml` triggers
on `v*` tags, checks that the tag matches the `Cargo.toml` version and publishes
the four targets with their `.sha256` files.
