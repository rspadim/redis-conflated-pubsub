# Docker

Run all commands in this guide from the repository root. `Dockerfile` and the `compose.*.yml` files remain at the root; paths in the examples are relative to it.

## Build and run

Build the Linux image and run it with a mounted configuration and writable state directory:

```sh
docker build --tag redis-conflated-pubsub:local .
cp config.example.json config.json
# Edit config.json so Redis addresses are reachable from the container.
mkdir -p state
docker run --rm \
  --workdir /state \
  --volume "$PWD/config.json:/config.json:ro" \
  --volume "$PWD/state:/state" \
  redis-conflated-pubsub:local \
  --config /config.json
```

Mount a runtime config at `/config.json`; the image uses `scratch` and contains neither a shell nor a config file. Relative lock, status, and log paths resolve from `/state` in this example. To expose the optional status endpoint, configure its bind address as `0.0.0.0` and publish its configured port, for example `--publish 9090:9090`.

## Compose integration tests

Each test starts project-scoped Redis/service containers, networks, and volumes; no fixed container names, IP addresses, or subnets are used. Run the `up` command from the repository root; run the matching `down` command afterward to remove its containers and test volumes. For parallel runs, pass a unique `-p <project>` to each Compose command.

### Pub/Sub integration

Checks channel mapping, cross-database Pub/Sub, conflation, binary payloads, echo prevention, and HTTP status. The random publisher and the driver are the `random-publisher` and `integration` subcommands of `examples/e2e.rs`, built into the `Dockerfile.e2e` image:

```sh
docker compose -f compose.test.yml up --build --abort-on-container-exit --exit-code-from integration-test
docker compose -f compose.test.yml down --volumes --remove-orphans
```

### Filter pipeline

Checks ordered input/output filters, the output namespace mapping, and the `GET /filters` cache snapshot with the `filter-integration` subcommand:

```sh
docker compose -f compose.filters.test.yml up --build --abort-on-container-exit --exit-code-from filter-integration-test
docker compose -f compose.filters.test.yml down --volumes --remove-orphans
```

### Oversized request handling

Uses Redis's 1 MiB query-buffer limit and checks failed-chunk handling with the `oversize-integration` subcommand:

```sh
docker compose -f compose.oversize.test.yml up --build --abort-on-container-exit --exit-code-from oversize-integration-test
docker compose -f compose.oversize.test.yml down --volumes --remove-orphans
```

### RESP protocol and oversized-message policies

Checks byte-sized transactions, `send`/`truncate`/`drop`, and direct `PUBLISH` without transaction wrappers. The counting proxy and the driver are Rust subcommands of `examples/e2e.rs`, built into the `Dockerfile.e2e` image:

```sh
docker compose -f compose.protocol.test.yml up --build --abort-on-container-exit --exit-code-from protocol-integration-test
docker compose -f compose.protocol.test.yml down --volumes --remove-orphans
```

### TTL and conflation

Checks duplicate suppression, changed payloads, individual and grouped TTL expiry/rounding, policy selection, independent schedules, Unix `SIGHUP` config reload, direct publishing, and conflation with deduplication disabled:

```sh
docker compose -f compose.ttl.test.yml up --build --abort-on-container-exit --exit-code-from ttl-integration-test
docker compose -f compose.ttl.test.yml down --volumes --remove-orphans
```

### End-to-end hot-path latency

The hot-path benchmark (`examples/hotpath_benchmark.rs`, built into the `Dockerfile.benchmark` image) runs publishers and both output subscribers in one Rust process, so harness buffering or GIL contention cannot distort the measurement. It warms 64 channels, times sequential requests, then runs concurrent publishers through Redis-compatible raw input to two output subscribers. It reports serial percentiles, a phase decomposition (input ACK round trip, post-ACK service time, per-output end-to-end) and the peak pending backlog. Outputs use direct mode (`conflation.interval_ms: 0`, dedup TTL 0), and cache inspection is disabled so the measured cache mode matches normal worker-local operation. The default image is Valkey 9.1.2.

```sh
docker compose -p hotpath-valkey -f compose.hotpath.test.yml up --build --abort-on-container-exit --exit-code-from hotpath-benchmark
docker compose -p hotpath-valkey -f compose.hotpath.test.yml down --volumes --remove-orphans
```

To run the identical test against Redis, select its image and CLI explicitly:

```sh
HOTPATH_KV_IMAGE=redis:7.4-alpine HOTPATH_KV_CLI=redis-cli docker compose -p hotpath-redis -f compose.hotpath.test.yml up --build --abort-on-container-exit --exit-code-from hotpath-benchmark
docker compose -p hotpath-redis -f compose.hotpath.test.yml down --volumes --remove-orphans
```

Publisher concurrency and volume can be varied with `HOTPATH_PUBLISHER_COUNT`, `HOTPATH_MESSAGES_PER_PUBLISHER`, and `HOTPATH_SERIAL_MESSAGES`. `HOTPATH_MODE=closed-loop` (default) waits for each input `PUBLISH` acknowledgement; `HOTPATH_MODE=open-loop` sends messages as non-atomic pipelines of `HOTPATH_PIPELINE` commands (default 64) over `HOTPATH_CONNECTIONS` connections per publisher (default 1), without waiting for individual acknowledgements, and uses an explicit 30 s response timeout. `HOTPATH_OUTPUTS` selects one or two outputs (default 2); the checked-in single-output config isolates one queue:

```sh
HOTPATH_CONFIG_FILE=./tests/docker-hotpath-single-config.json HOTPATH_OUTPUTS=1 \
docker compose -p hotpath-single -f compose.hotpath.test.yml up --build --abort-on-container-exit --exit-code-from hotpath-benchmark
docker compose -p hotpath-single -f compose.hotpath.test.yml down --volumes --remove-orphans
```

The config and workload use synthetic channel names and payload IDs; they do not replay raw production names or payloads.

To measure a mixed policy set (about one-third direct, one-third 200 ms conflation, and one-third 200 ms conflation with a 5 s deduplication TTL), switch to the mixed config and select the Rust mixed-policy harness (`examples/hotpath_mix_benchmark.rs`, built into the same image) through `HOTPATH_BENCHMARK_CMD`; `HOTPATH_KV_IMAGE`/`HOTPATH_KV_CLI` choose the engine as in the direct runs:

```sh
HOTPATH_CONFIG_FILE=./tests/docker-hotpath-mix-config.json \
HOTPATH_BENCHMARK_CMD='hotpath-mix-benchmark' \
docker compose -p hotpath-mixed -f compose.hotpath.test.yml up --build --abort-on-container-exit --exit-code-from hotpath-benchmark
docker compose -p hotpath-mixed -f compose.hotpath.test.yml down --volumes --remove-orphans
```

For the TTL cohort, this workload repeats one identical payload per channel; with unique payloads, a positive TTL would not suppress those publishes.

### Loopback (no Redis/Valkey)

The loopback benchmark runs the service against a minimal RESP2 broker implemented in Rust (same benchmark image), with no Redis containers:

```sh
docker compose -p loopback -f compose.loopback.test.yml up --build --abort-on-container-exit --exit-code-from loopback-benchmark
docker compose -p loopback -f compose.loopback.test.yml down --volumes --remove-orphans
```

`LOOPBACK_MESSAGES` (default 1,000,000), `LOOPBACK_CHUNK`, `LOOPBACK_WARMUP` and `LOOPBACK_TIMEOUT_S` tune the run. The broker shares CPU with the service, so treat the numbers as service-plus-broker measurements.

### Fault handling

The `fault` profile in `compose.test.yml` exercises uncertain/failed publish handling through the Rust fault proxy and driver from `examples/e2e.rs` (built into the `Dockerfile.e2e` image):

```sh
docker compose -f compose.test.yml --profile fault up --build --abort-on-container-exit --exit-code-from fault-integration fault-integration
docker compose -f compose.test.yml --profile fault down --volumes --remove-orphans
```
