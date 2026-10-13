# NATS transport spike

Isolated experiment only; it does not modify the relay runtime or its dependencies. The smoke program forwards one synthetic binary message Redis→NATS and NATS→Redis, then checks NATS publish/subscribe over TCP, WS, and WSS. The NATS WebSocket listeners offer DEFLATE compression; the Rust client doesn't negotiate it, so its messages remain uncompressed. Other clients can negotiate it when supported. Compose provides a disposable Redis and a three-node NATS cluster; a local test CA and WSS certificate are generated into the gitignored `certs` directory.

## Run

From the repository root, start the local test services:

```powershell
docker compose -p nats-spike -f spikes/nats/compose.yml up -d
$env:CARGO_TARGET_DIR = (Resolve-Path target).Path
cargo run --locked --manifest-path spikes/nats/Cargo.toml
docker compose -p nats-spike -f spikes/nats/compose.yml down --remove-orphans
```

The binary defaults to the local ports declared in `compose.yml`. To use a different NATS server list, set `NATS_SERVERS` to comma-separated URLs; `NATS_TOKEN` is read from the environment and never printed. `NATS_WS_URL` and `REDIS_URL` can also be overridden.

For an authenticated connection check with no subscription or publish, set `NATS_PROBE_ONLY=1` and provide `NATS_SERVERS` plus `NATS_TOKEN` through the environment. This is the mode to use before testing a remote server.

To exercise reconnect while traffic is flowing, run `spikes/nats/test-failover.ps1`. It starts the smoke test, stops `nats1` during the probe, waits five seconds, then starts it again. The script only stops/starts the `nats1` service in the `nats-spike` Compose project.

To measure the raw client path (transport capacity, not the relay), run with `NATS_LOAD_TEST=1` and the `NATS_LOAD_*` knobs: `NATS_LOAD_TOTAL` (default 200000), `NATS_LOAD_PUBLISHERS` (4), `NATS_LOAD_PAYLOAD_BYTES` (256), `NATS_LOAD_FLUSH_EVERY` (64), `NATS_LOAD_MODE` (`roundtrip` or `publish`). It reports publish rate, end-to-end rate, delivery completeness, and RTT p50/p95/p99. Use `--release`. Baseline on the local three-node cluster over TCP: 4 publishers, 200k messages, 256 B, flush 64 → ~140k msg/s end-to-end, RTT p50 43 ms / p95 76 ms / p99 88 ms, zero loss.

To test local authentication with Docker, run `spikes/nats/test-auth.ps1`. It creates disposable random user/password and token values plus NKey, JWT credentials, and mTLS files under the ignored `certs/auth` directory. It checks authenticated publish/subscribe round trips for all five methods and rejects an incorrect test password. It does not use the production server or its token.

Do not point the smoke program at production subjects: it creates unique `spike.*` subjects/channels and publishes only one synthetic payload. The remote token shared for evaluation must not be copied into this directory or committed.
