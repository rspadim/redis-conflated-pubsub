# NATS transport spike

Isolated experiment only; it does not modify the relay runtime or its dependencies. The smoke program forwards one synthetic binary message Redis→NATS and NATS→Redis, then checks NATS publish/subscribe over TCP, WS, and WSS. The NATS WebSocket listeners offer DEFLATE compression; the Rust client doesn't negotiate it, so its messages remain uncompressed. Other clients can negotiate it when supported. Compose provides a disposable Redis and a three-node NATS cluster; a local test CA and WSS certificate are generated into the gitignored `certs` directory.

Everything here is synthetic (`spike.*` / `load.*` subjects, generated payloads). Never point any mode at production subjects: the remote token shared for evaluation must not be copied into this directory or committed.

## Build and start

The spike owns its own cargo target directory so it never contends with the repository's cargo lock. Always pass `CARGO_TARGET_DIR` (the helper scripts set it for you):

```powershell
docker compose -p nats-spike -f spikes/nats/compose.yml up -d
$env:CARGO_TARGET_DIR = Join-Path (Get-Location) "spikes/nats/.target"
cargo build --release --locked --manifest-path spikes/nats/Cargo.toml
# ... run modes ...
docker compose -p nats-spike -f spikes/nats/compose.yml down --remove-orphans
```

The binary defaults to the local ports declared in `compose.yml`. Overridable through the environment: `NATS_SERVERS` (comma-separated URLs), `NATS_WS_URL`, `NATS_WSS_URL`, `NATS_WSS_CA_CERT`, `REDIS_URL`, and the auth variables (`NATS_TOKEN`, `NATS_USERNAME`, `NATS_PASSWORD`, `NATS_AUTH_MODE`, `NATS_NKEY_SEED_FILE`, `NATS_CREDENTIALS_FILE`, `NATS_CLIENT_CERT_FILE`, `NATS_CLIENT_KEY_FILE`). Secrets are read from the environment and never printed.

## Modes and what each proves

Every mode is selected by one environment variable; the default (none set) runs the smoke path.

| Mode | Env | What it proves |
| --- | --- | --- |
| Smoke | *(default)* | Redis→NATS and NATS→Redis forward raw binary payloads; TCP, `ws://`, and `wss://` round trips work. |
| Probe only | `NATS_PROBE_ONLY=1` | The client connects and reports connection limits without subscribing or publishing. Use before testing a remote server. |
| Auth round trip | `NATS_AUTH_ROUNDTRIP=1` + `NATS_AUTH_MODE` | An authenticated publish/subscribe round trip. |
| Failover probe | `NATS_FAILOVER_PROBE_SECS=<n>` | Ordered publish/echo checks keep succeeding while a configured node is stopped. |
| Load | `NATS_LOAD_TEST=1` | Raw `async-nats` client capacity: publish rate, end-to-end rate, delivery completeness, RTT percentiles. |
| Loop/echo | `NATS_ECHO_PROBE=1` | `no_echo` only suppresses same-connection echoes; separate input/output connections loop, and a header-based guard stops it. |

### Smoke / probe / auth

```powershell
# Smoke round trips (uses the default local endpoints).
cargo run --release --locked --manifest-path spikes/nats/Cargo.toml

# Connection-only check against a remote server (no subscribe/publish).
$env:NATS_PROBE_ONLY = "1"; $env:NATS_SERVERS = "tls://host:4222"
cargo run --release --locked --manifest-path spikes/nats/Cargo.toml
```

`spikes/nats/test-auth.ps1` creates disposable random user/password and token values plus NKey, JWT credentials, and mTLS files under the ignored `certs/auth` directory, then checks authenticated publish/subscribe round trips for all five methods and rejects an incorrect test password.

### Failover probe (short, ordered)

```powershell
./spikes/nats/test-failover.ps1
```

Starts the smoke test, stops `nats1` during the 20 s probe, waits five seconds, then starts it again. It only stops/starts the `nats1` service in its Compose project.

### Failover under load

```powershell
./spikes/nats/test-failover-load.ps1                                  # defaults: 3M msgs, 6 s outage
./spikes/nats/test-failover-load.ps1 -Total 2000000 -OutageSeconds 8  # longer/heavier
```

Runs the load harness (`roundtrip` mode) while `nats1` is stopped mid-run, then restarts it and waits for completion. It reports delivered count, completeness, publish/flush errors, and rate, and fails if fewer than 98% of the messages arrive. This is evidence that traffic continues across a node outage; it is still the client path, not the relay.

### Load harness

```powershell
$env:NATS_LOAD_TEST = "1"
$env:NATS_SERVERS = "nats://127.0.0.1:14222,nats://127.0.0.1:14223,nats://127.0.0.1:14224"
cargo run --release --locked --manifest-path spikes/nats/Cargo.toml
```

Knobs: `NATS_LOAD_MODE` (`roundtrip` = subscriber + end-to-end latency, or `publish` = no subscriber, measures the publish+flush ceiling), `NATS_LOAD_TOTAL` (200000), `NATS_LOAD_PUBLISHERS` (4), `NATS_LOAD_PAYLOAD_BYTES` (256), `NATS_LOAD_FLUSH_EVERY` (64), `NATS_LOAD_WAIT_SECS` (120), `NATS_LOAD_SEPARATE_SUBSCRIBER` (1 = subscriber on its own connection, mirroring the relay's separate input/output connections; set 0 to reproduce the single-connection slow-consumer stalls), `NATS_LOAD_LABEL`, `NATS_LOAD_TRANSPORT`. The last line is a machine-readable `load_summary ...` row with rate, delivered, `complete`, publish/flush errors, and RTT p50/p95/p99/max. Use `--release`.

### Load matrix runner

`spikes/nats/load-matrix.ps1` loops the publishers × payload × flush matrix over TCP and/or WSS and writes CSV + Markdown tables to `spikes/nats/results/` (gitignored).

```powershell
# Local cluster must already be up (see Build and start), or let the runner manage it.
./spikes/nats/load-matrix.ps1 -Transports tcp -Publishers 1,4,16 -Payloads 64,256,1024 -Flushes 16,64,256 -Total 100000 -Repeats 2 -SkipComposeUp
./spikes/nats/load-matrix.ps1 -Transports tcp,wss -Mode publish            # publish-only ceiling
./spikes/nats/load-matrix.ps1 -Transports wss -WssServers "wss://localhost:18443"
```

Parameters are comma- or space-separated lists. `-SkipComposeUp`/`-SkipComposeDown` reuse an already-running cluster. The WSS cells use `certs/ca-cert.pem` automatically. `spikes/nats/run-measured-sweep.ps1` reproduces the recorded matrix.

Note: this harness runs the subscriber **in the same process** as the publisher, so its `RTT`/end-to-end latency is queue-inflated and is **not** a transport-latency figure — use it for relative cell comparison at a fixed shape. For process-separated, pacing-controlled latency and the relay end-to-end load, use `spikes/compare` instead.

## Loop/echo probe

```powershell
$env:NATS_ECHO_PROBE = "1"
$env:NATS_SERVERS = "nats://127.0.0.1:14222,nats://127.0.0.1:14223,nats://127.0.0.1:14224"
$env:NATS_ECHO_WINDOW_MS = "1200"
cargo run --release --locked --manifest-path spikes/nats/Cargo.toml
```

It runs four scenarios and prints `echo_scenario` lines plus a summary. Current result on the local three-node cluster:

| Scenario | Loop observed |
| --- | --- |
| separate connections, no guard | yes (~900 messages in 1.2 s) |
| separate connections, `Relay-Hop` guard | no (1 message forwarded) |
| same connection, `no_echo` on | no (1 message) |
| same connection, `no_echo` off | yes (~1000 messages) |

Conclusion: Core NATS `no_echo` only suppresses messages published through the *same* client connection. The relay uses separate input/output connections, so `no_echo` cannot stop an output subject from matching an input subscription; a backend-aware loop guard is required. The probe demonstrates the intended `Relay-Hop: 1` header guard: messages already stamped by the relay are not forwarded again, while the payload bytes are never rewritten (verified byte-identical, including NUL/`0xff`).

## Layout

- `src/main.rs` — mode dispatch, smoke tests, connection setup, failover probe.
- `src/load.rs` — load harness.
- `src/echo.rs` — loop/echo probe and header-guard prototype.
- `compose.yml` / `nats{1,2,3}.conf` — three-node cluster (TCP + WS on nats1/nats3, WSS on nats2) and a disposable Redis.
- `compose.auth.yml` / `auth-*.conf` / `generate-*.sh` — five local auth methods.
- `load-matrix.ps1`, `run-measured-sweep.ps1`, `test-failover.ps1`, `test-failover-load.ps1`, `test-auth.ps1` — runners.
- `results/`, `certs/` — gitignored outputs (synthetic data only).
