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

Checks channel mapping, cross-database Pub/Sub, conflation, binary payloads, echo prevention, and HTTP status:

```sh
docker compose -f compose.test.yml up --build --abort-on-container-exit --exit-code-from integration-test
docker compose -f compose.test.yml down --volumes --remove-orphans
```

### Oversized request handling

Uses Redis's 1 MiB query-buffer limit and checks failed-chunk handling:

```sh
docker compose -f compose.oversize.test.yml up --build --abort-on-container-exit --exit-code-from oversize-integration-test
docker compose -f compose.oversize.test.yml down --volumes --remove-orphans
```

### RESP protocol and oversized-message policies

Checks byte-sized transactions, `send`/`truncate`/`drop`, and direct `PUBLISH` without transaction wrappers:

```sh
docker compose -f compose.protocol.test.yml up --build --abort-on-container-exit --exit-code-from protocol-integration-test
docker compose -f compose.protocol.test.yml down --volumes --remove-orphans
```

### TTL and conflation

Checks duplicate suppression, changed payloads, TTL expiry, direct publishing, and conflation with deduplication disabled:

```sh
docker compose -f compose.ttl.test.yml up --build --abort-on-container-exit --exit-code-from ttl-integration-test
docker compose -f compose.ttl.test.yml down --volumes --remove-orphans
```

### Fault handling

The `fault` profile in `compose.test.yml` exercises uncertain/failed publish handling through a Redis fault proxy:

```sh
docker compose -f compose.test.yml --profile fault up --build --abort-on-container-exit --exit-code-from fault-integration fault-integration
docker compose -f compose.test.yml --profile fault down --volumes --remove-orphans
```
