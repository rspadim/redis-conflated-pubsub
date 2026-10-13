# NATS vs Redis transport comparison

Isolated benchmark harness. It is **not** the relay: it measures the raw transport paths (`async-nats` vs `redis-rs`) with an identical workload so the 0.3 adapters can be compared against a known baseline.

Two roles run as **separate OS processes** so the publisher and the subscriber do not contend for the same event loop or CPU:

- `subscriber` — consumes into a preallocated `Vec`, no per-message lock; prints age percentiles.
- `publisher` — two modes:
  - **closed-loop** (`COMPARE_CLOSED_LOOP=1`, batch 1): waits for each command's round trip before the next. No queue, no pacing timer — this is wire latency.
  - **throughput** (`COMPARE_CLOSED_LOOP=0`): pipelines/flushes every `COMPARE_BATCH` messages; measures the ceiling.
- Optional `COMPARE_RATE` paces the publisher (0 = unpaced). Note: on Windows the timer resolution is coarse, so prefer closed-loop for latency.

Both roles run in Rust only; no Python. Each payload's first 8 bytes carry the send timestamp, so the subscriber measures true age.

## Run

```powershell
docker compose -p transport-compare -f spikes/compare/compose.yml up -d
$env:CARGO_TARGET_DIR = "$PWD/spikes/compare/.target"
cargo build --release --locked --manifest-path spikes/compare/Cargo.toml
$exe = "$PWD/spikes/compare/.target/release/transport-compare.exe"

# latency (closed loop)
$env:COMPARE_TOTAL='20000'; $env:COMPARE_PAYLOAD_BYTES='256'; $env:COMPARE_BATCH='1'; $env:COMPARE_CLOSED_LOOP='1'
$env:COMPARE_ENGINE='nats'; $env:NATS_URL='nats://127.0.0.1:14230'
Start-Process $exe subscriber -NoNewWindow -RedirectStandardOutput sub.txt
& $exe publisher; Get-Content sub.txt

# throughput (pipelined)
$env:COMPARE_TOTAL='200000'; $env:COMPARE_BATCH='64'; $env:COMPARE_CLOSED_LOOP='0'
$env:COMPARE_ENGINE='redis'; $env:REDIS_URL='redis://127.0.0.1:16390'
Start-Process $exe subscriber -NoNewWindow -RedirectStandardOutput sub.txt
& $exe publisher; Get-Content sub.txt

docker compose -p transport-compare -f spikes/compare/compose.yml down --remove-orphans
```

`NATS_ROOT_CERT_FILE` sets a trusted root for `wss://` (use the generated `certs/ca-cert.pem`). `NATS_TOKEN` sets a token if the server requires one.

## Measured — latency, closed loop (20k messages, 256 B, process-separated)

| Engine | Command | Rate | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| NATS | `PUB` + `flush` per message | 8.1k/s | 2.60 ms | 16.3 ms | 22.8 ms | 28.3 ms |
| Redis | `PUBLISH` + subscriber ack | 0.76k/s | 1.23 ms | 2.10 ms | 4.30 ms | 33.2 ms |

At one message per round trip, **Redis has lower and more stable latency**; NATS has ~10× the closed-loop throughput but a heavier tail.

## Measured — throughput, pipelined (200k messages, 256 B, batch 64, process-separated)

| Engine | Publish rate | End-to-end rate | p50 | p95 | p99 | max |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| NATS | 114.0k msg/s | 73.6k msg/s | 40.6 ms | 64.0 ms | 80.9 ms | 83.9 ms |
| Redis | 33.6k msg/s | 29.5k msg/s | 1.99 ms | 3.30 ms | 6.14 ms | 14.2 ms |

Under batching, **NATS sustains ~3.4× the publish rate**, but the subscriber-side age is queue-dominated for both engines; only the Redis age stays low because its publish rate is near the subscriber's drain rate. Use closed-loop for latency, throughput mode for ceiling.

## Measured — latency vs rate sweep (40k messages, 256 B, batch 1, process-separated)

Same target rate for both engines; `latency-sweep.ps1`. Achieved rate is the real publish rate.

| Engine | Target msg/s | Achieved msg/s | p50 ms | p95 ms | p99 ms | Complete |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| NATS | 500 | 500 | 1.42 | 2.57 | 5.11 | yes |
| NATS | 1000 | 1000 | 1.34 | 2.86 | 6.25 | yes |
| NATS | 2000 | 2000 | 1.32 | 4.12 | 8.20 | yes |
| NATS | 4000 | 3997 | 1.46 | 10.35 | 79.48 | yes |
| Redis | 500 | 488 | 1.36 | 3.61 | 9.14 | yes |
| Redis | 1000 | 572 | 1.53 | 3.28 | 6.68 | yes |
| Redis | 2000 | 557 | 1.36 | 4.02 | 12.33 | yes |
| Redis | 4000 | 753 | 1.22 | 2.18 | 4.49 | yes |

Reading:
- **NATS holds the target rate** up to 4000/s (it is paced, not saturated) and its latency stays ~1.3 ms p50 until p95/p99 start to climb at 4000/s — the classic queue buildup as the pace approaches capacity.
- **Redis does not hold the target rate**: asking for 1000/2000/4000 yields 570–750/s. The publisher is **ack-bound** — a per-message `PUBLISH` waits for the subscriber-count reply, so its ceiling is the round-trip rate (~700/s here), not the requested pace. Because it never actually reaches the high rates, its reported p99 stays low; higher targets do not add load.
- At the *same achieved* rate (~500/s), both engines are ~1.2–1.4 ms p50; Redis p99 is slightly higher here.

The earlier "NATS = 8.1k/s closed loop, Redis = 0.76k/s" pair was **not** a fair latency comparison: the NATS closed loop was `PUB`+`flush` (fast), the Redis one was a full `PUBLISH`+ack (slower), so they ran in different regimes. Pacing to a common target removes that.

## End-to-end relay load test (NATS input -> relay -> NATS output)

Drives the **actual relay binary** (not the transport harness): a Rust publisher writes to `<prefix>.in.>`, the relay maps to `<prefix>.out.>` and republishes on NATS, and a Rust verifier subscribes to `<prefix>.out.>` and counts/ages every message. Config: `e2e-nats-nats.json` (conflation off, dedup off, direct lane).

```powershell
docker compose -p transport-compare -f spikes/compare/compose.yml up -d
$bin = "target/release/redis-conflated-pubsub.exe"; $exe = "spikes/compare/.target/release/transport-compare.exe"
# rewrite e2e-nats-nats.json subjects to a unique prefix, start the relay, start e2e-verify, run e2e-publish
```

| Run | Published | Relay input | Relay output | Verify received | Errors | Uncertain | Shed | Age p50/p95/p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Smoke | 5,000 | 5,000 | 5,000 | 5,000 | 0 | 0 | 0 | 73.8 / 76.5 / 77.5 ms |
| Load | 100,000 | 100,000 | 100,000 | 100,000 | 0 | 0 | 0 | 85.5 / 168.8 / 199.1 ms |

The relay's own status HTTP endpoint confirms `input_messages_total == output_messages_total == published`, with zero publish errors, uncertain operations, shed messages, or reconnects. The verifier subscribes to a different subject space (`<prefix>.out.>`) than the publisher wrote (`<prefix>.in.>`), so it proves the relay's subject mapping is what carried the traffic.

The direct lane has no conflation/dedup, so this measures pure relay routing at full rate: the input publisher reached ~44.6k msg/s and the relay delivered ~31.9k msg/s end to end on this single-host Docker setup.

## Acceptance + load matrix (all four combinations)

`matrix-e2e.ps1` runs the whole 0.3 input/output matrix through the real relay
binary: the four combinations (Redis→Redis, Redis→NATS, NATS→Redis, NATS→NATS)
as acceptance runs plus four large load runs, an output-filter run, a mixed
Redis+NATS fan-out run, and the NATS loop guard. For every run it starts the
brokers, generates a config from `configs/*.json` (substituting a unique
`__NS__` namespace and status `__PORT__`), starts the relay, waits until the
relay's input subscription is installed on the broker, runs `e2e-publish` and
`e2e-verify` as **separate processes**, then asserts the relay's status counters.
It uses local Docker and synthetic data only, and tears its own compose project
down.

```powershell
# Full matrix (acceptance + 300k-message load runs at a paced 5k msg/s).
powershell -ExecutionPolicy Bypass -File spikes/compare/matrix-e2e.ps1

# Acceptance only, smaller messages, faster.
powershell -ExecutionPolicy Bypass -File spikes/compare/matrix-e2e.ps1 -SmokeTotal 3000 -SkipLoad
```

`Invoke-Run` in the driver checks, per run:

- `relay_in == published` (every published message reached the relay input);
- `relay_out == verify_received == expected` (every message reached every output);
- `errors == uncertain == shed == 0`;
- the loop-guard run reports `loop_suppressed >= 1`.

The `filter-redis-out` run publishes one in ten messages on a `...drop...`
address that an output `deny` glob filters out, so its expected count is 90% of
published. The `fanout-nats-mixed` run has one NATS input, one Redis output and
one NATS output; both verifiers must receive every message.

### Harness roles

`transport-compare` still runs one role per process:

| Role | Purpose |
| --- | --- |
| `publisher` / `subscriber` | raw transport comparison; one engine, one prefix |
| `e2e-publish` | drives a relay **input** (`COMPARE_ENGINE` = redis or nats); writes `COMPARE_INPUT_PREFIX.<i>` |
| `e2e-verify` | consumes one or more relay **outputs** |

`e2e-verify` takes `COMPARE_VERIFY_ENGINES` (comma list, e.g. `redis,nats`) and
`COMPARE_VERIFY_PREFIX` (comma list of output prefixes, one per engine) and
spawns one task per engine, so a mixed fan-out is measured from both backends at
once. It writes `COMPARE_READY_FILE` only after every engine has subscribed, so
the publisher never races subscription setup. `COMPARE_EXPECTED` overrides the
expected per-output count (default `COMPARE_TOTAL`). `COMPARE_WAIT_TIMEOUT_SECS`
bounds the overall wait so a regression fails fast.

`COMPARE_FILTER_DENY_EVERY=N` (default 0) sends every Nth message to a
`...drop...` address instead of `...keep...`, for the output-filter run.

### Measured — acceptance matrix (5,000 messages, 256 B, process-separated)

Generated by `matrix-e2e.ps1 -SmokeTotal 5000 -SkipLoad` on this workstation.
`deliver/s` is the verifier's end-to-end rate; `p50/p95/p99` are the age between
the publisher's send timestamp and the verifier's observation (one row per
verifier engine, the table shows the slower engine).

| Run | Input → Output | Published | Relay in | Relay out | Verify received | Errors | Uncertain | Shed |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| smoke-redis-redis | Redis → Redis | 5,000 | 5,000 | 5,000 | 5,000 | 0 | 0 | 0 |
| smoke-redis-nats | Redis → NATS | 5,000 | 5,000 | 5,000 | 5,000 | 0 | 0 | 0 |
| smoke-nats-redis | NATS → Redis | 5,000 | 5,000 | 5,000 | 5,000 | 0 | 0 | 0 |
| smoke-nats-nats | NATS → NATS | 5,000 | 5,000 | 5,000 | 5,000 | 0 | 0 | 0 |
| filter-redis-out | Redis → Redis (deny 1/10) | 5,000 | 5,000 | 4,500 | 4,500 | 0 | 0 | 0 |
| fanout-nats-mixed | NATS → Redis + NATS | 5,000 | 5,000 | 10,000 | 10,000 | 0 | 0 | 0 |
| loop-guard-nats | NATS → NATS (overlap) | 1 | 1 | 1 | 1 | 0 | 0 | 0 |

The filter run proves an output `deny` filter drops the mapped `drop` channels
while the `keep` channels pass, and the relay counts all 5,000 inputs. The
mixed-fan-out run proves a single input reaches two different backends. The
loop-guard run reports `loop_suppressed_messages_total = 1`, so the second
generation is dropped instead of cycling.

### Measured — load matrix (300,000 messages, 256 B, paced 5,000 msg/s)

`matrix-e2e.ps1 -LoadTotal 300000 -LoadRate 5000`. Paced so latency is not
queue-dominated; `deliver/s` is the verifier's end-to-end rate.

| Run | Input → Output | Published | Relay in | Relay out | Verify received | Errors | Uncertain | Shed | deliver/s | p50 | p95 | p99 |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| load-redis-redis | Redis → Redis | 300,000 | 300,000 | 300,000 | 300,000 | 0 | 0 | 0 | 4,983 | 40.6 | 91.0 | 274.1 |
| load-redis-nats | Redis → NATS | 300,000 | 300,000 | 300,000 | 300,000 | 0 | 0 | 0 | 4,996 | 42.6 | 87.1 | 135.6 |
| load-nats-redis | NATS → Redis | 300,000 | 300,000 | 300,000 | 300,000 | 0 | 0 | 0 | 4,994 | 3.7 | 20.3 | 42.1 |
| load-nats-nats | NATS → NATS | 300,000 | 300,000 | 300,000 | 300,000 | 0 | 0 | 0 | 4,994 | 3.4 | 16.1 | 30.8 |

Reading: at a common target rate every combination delivers every message with
zero errors, uncertain operations, or shed messages, and holds the pace
(`deliver/s ≈ 4,994–4,996`). NATS-input runs show lower age because a NATS
publisher paces against an absolute deadline with a real server round-trip
barrier at the end; the Redis publisher is ack-bound and its per-batch replies
add to the observed age. Timings are single-host Docker with broker and clients
sharing CPU, so they are indicative, not controlled engine comparisons.

Unpaced ceilings (`-LoadRate 0`) are higher (Redis input ~20k publish/s, NATS
input ~26k publish/s) but the verifier's drain becomes the bottleneck, so the
measured age is queue age, not transport latency; the paced table above is the
comparable one.



## WebSocket DEFLATE (permessage-deflate)

The servers run with `websocket { compress: true }`, so the extension is offered. `async-nats` 0.50 uses `tokio-websockets` 0.10.1, which documents WebSocket compression as unsupported, so no compression is negotiated and the connection still succeeds — the client simply does not offer `permessage-deflate`.

## Caveats

- Single local Docker host; server and clients share CPU. Numbers are indicative, not controlled engine comparisons.
- `wss://` must be dialed by IP on Windows here (`127.0.0.1`), because the published port is IPv4 while `localhost` may resolve to `::1`.
- An earlier in-process version of this harness showed inflated NATS latency; that was a slow single-process subscriber building a queue, not transport latency. The process-separated roles replaced it.
