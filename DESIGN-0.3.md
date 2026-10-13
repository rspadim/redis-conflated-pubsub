# Design proposal: 0.3

**Status:** proposal backed by an isolated transport spike; no NATS support has been added to the relay runtime yet.

The 0.2 runtime design is documented in [`src/DESIGN.md`](src/DESIGN.md). The experiment behind this proposal is in [`spikes/nats/`](spikes/nats/README.md).

## Goal and scope

Allow Redis or Core NATS as the single input backend, and independently select Redis or Core NATS for each output. The supported combinations are Redis→Redis, Redis→NATS, NATS→Redis, and NATS→NATS. Keep the existing message-processing behavior—mapping, filters, queues, conflation, deduplication, and status—common to both backends.

NATS scope is **Core NATS Pub/Sub**, not JetStream. JetStream persistence, consumer acknowledgements, and durable replay require a separate design because their delivery model differs from the relay's current ephemeral Pub/Sub contract.

Throughput is an explicit 0.3 goal: the relay must be measured under heavy sustained load for every input/output combination, not only smoke-tested. Correct routing, credentials, and reconnect behavior are prerequisites, but the release also needs reference numbers (rate, latency percentiles, publish reduction, backlog) with the same rigor as the Redis hot-path benchmarks.

## Spike evidence

The prototype is deliberately separate from the runtime and uses synthetic data only.

| Experiment | Result | What it establishes |
| --- | --- | --- |
| Dependency/toolchain | `async-nats` 0.50.0 declares Rust 1.88 as its minimum and compiled with the local Rust 1.99 toolchain | No MSRV blocker identified from crate metadata; the spike was not built using rustc 1.88 itself |
| Local cluster | Three NATS 2.15 nodes formed a cluster; a connection seeded with one node reported additional pool entries | Seed lists and server discovery are usable; pool entry counts vary with configured and discovered addresses and are not a cluster-size metric |
| Redis→NATS | A 35-byte payload containing NUL and `0xff` arrived byte-identically | Basic raw-byte forwarding works across the protocol boundary |
| NATS→Redis | The same binary payload arrived byte-identically | Basic NATS subscription and Redis publication work |
| Transports | TCP, `ws://`, and `wss://` publish/subscribe round trips passed; WSS used a generated local CA | The selected Rust client supports the required transports in this test setup |
| Node interruption | With a 20-second probe, `nats1` was stopped for five seconds; 77 ordered publish/echo checks completed | The client resumed traffic while a configured cluster node was down |
| Authentication | Local Docker servers accepted user/password, token, NKey seed, JWT credentials file, and mTLS client certificate; each passed a binary publish/subscribe round trip | These methods can be configured through the tested Rust client APIs |
| Negative authentication | A wrong local test password was rejected during connection setup | Authentication errors surface before relay publishing begins |
| Existing relay regression | `cargo test --locked`: 130 passed, 0 failed, 8 ignored | The isolated spike did not alter the existing runtime tests |

Not tested: the relay's actual worker/conflation/deduplication path with NATS, server-only TLS over raw TCP without a client certificate, mixed authentication methods on a single server, auth callouts, bearer-only JWTs, production authentication, proxy/load-balancer discovery behavior, maximum-payload edge cases, throughput comparisons, or JetStream. The remote server's TCP port was reachable, but no authenticated connection or production subscription was attempted. No production payloads or credentials are part of the spike artifacts.

The spike's `NATS_PROBE_ONLY=1` mode exists for a future connection-only check: it connects and reports basic connection limits without subscribing or publishing. Credentials must be supplied through the environment or an external secret source, never checked into the repository.

### Authentication configuration proposal

Use one explicit NATS authentication variant and keep TLS transport settings separate. The spike exercised the corresponding [`async-nats` 0.50 `ConnectOptions`](https://docs.rs/async-nats/0.50.0/async_nats/struct.ConnectOptions.html) methods: `user_and_password`, `token`, `nkey`, `credentials_file`, `add_root_certificates`, and `add_client_certificate`. See NATS documentation for [authentication basics](https://docs.nats.io/learn/security/authentication-basics), [operator/JWT mode](https://docs.nats.io/learn/security/operator-mode), and [TLS/mTLS](https://docs.nats.io/learn/security/encryption).

Proposed JSON shapes (illustrative; not yet part of the runtime schema):

```json
"auth": { "type": "user_password", "username": "relay", "password_env": "NATS_PASSWORD" }
```

```json
"auth": { "type": "token", "token_env": "NATS_TOKEN" }
```

```json
"auth": { "type": "nkey", "seed_file": "/run/secrets/nats-user.nk" }
```

```json
"auth": { "type": "credentials_file", "path": "/run/secrets/nats-user.creds" }
```

TLS remains an independent connection setting, selected by the server URL scheme and optionally extended with trusted roots or client identity:

```json
"tls": {
  "root_cert_file": "/run/secrets/nats-ca.pem",
  "client_cert_file": "/run/secrets/relay-cert.pem",
  "client_key_file": "/run/secrets/relay-key.pem"
}
```

| JSON `auth.type` | Secret reference | Rust client setup |
| --- | --- | --- |
| omitted / `none` | none; development only | `ConnectOptions::new()` |
| `user_password` | `username` + `password_env` | `.user_and_password(username, env(password_env))` |
| `token` | `token_env` | `.token(env(token_env))` |
| `nkey` | `seed_file` | `.nkey(read(seed_file))` |
| `credentials_file` | `path` to `.creds` | `.credentials_file(path).await?` |
| mTLS | `tls.client_cert_file` + `tls.client_key_file`; optional custom `root_cert_file` | `.add_client_certificate(cert, key)` and `.add_root_certificates(ca)` |

At startup, deserialize the tagged auth variant, resolve environment/file secrets, apply the optional TLS settings, then call `ConnectOptions::connect(servers)`. `credentials_file` and standalone NKey are alternatives; the server-side certificate mapping for mTLS is outside the relay's JSON. Validation should require both client certificate and key together and reject ambiguous auth variants.

The `credentials_file` variant is the NATS user JWT **plus its NKey seed** file, not a bearer JWT. Treat it and NKey seeds/private certificates as secrets. Passwords and tokens should be referenced by environment variable name; do not accept secret values inline in JSON or check them into Git. Remote password/token connections should use `tls://` or `wss://`; the local cleartext auth tests bind only to loopback. mTLS is expressed by client-certificate files under `tls`, with the NATS server configured to require/map client certificates. The spike used ephemeral random local credentials and a local CA under the gitignored `spikes/nats/certs/auth/` directory; it did not use the production token.

Auth callouts/custom auth callbacks and bearer-only JWTs are not in the proposed 0.3 minimum. Add them only if a deployment requires them.

## Architecture proposal

### Backend boundary

Keep message policy and queueing in the existing service layer. Extract backend-specific operations behind two internal boundaries:

- **Input source:** connect/reconnect, install subscriptions, and emit a normalized envelope containing source address, raw payload bytes, and the identity of the subscription that matched.
- **Output sink:** connect/reconnect, publish a bounded ordered chunk, flush/settle it, and report backend-specific outcomes.

The normalized envelope must retain subscription identity: Redis reports the matching PSUBSCRIBE pattern, while a NATS subscription stream is associated with the subject filter that created it. This preserves per-subscription mappings if filters overlap. Continue sharing payload storage with `Arc<[u8]>` where appropriate.

Redis remains implemented using redis-rs. Use the official Rust `async-nats` client for NATS rather than implementing the wire protocol. The spike verified that the client builds at the current MSRV and handled the tested transports and a node interruption.

### Configuration and addressing

- Preserve existing Redis configuration files. Add a NATS endpoint variant and validate that exactly one backend is selected for `input` and for each output; expose the same constraint in `--check-config` and generated JSON Schema.
- NATS endpoint configuration should accept one or more server URLs (cluster seeds), connection naming/timeouts, and secret references. Use the URL scheme to choose `nats://`, `tls://`, `ws://`, or `wss://`; provide an option to ignore advertised servers for deployments where discovered addresses are unreachable.
- Subscription syntax is backend-native for the selected input: Redis `SUBSCRIBE`/`PSUBSCRIBE` keeps Redis glob semantics, while NATS subscriptions use NATS subjects (`*` matches one token; `>` matches one or more trailing tokens and must be last). For example, Redis `prices:*` and NATS `prices.>` are distinct patterns; do not translate one into the other.
- Preserve existing channel names and `channel_prefix`/`channel_suffix` keys for NATS outputs too; the final mapped string is used as the NATS subject, with no automatic renaming or escaping. Validate that published subjects are valid and concrete. NATS input subscriptions use native NATS subjects/wildcards (`*`, `>`); Redis PSUBSCRIBE globs are not translated automatically.

### NATS behavior that must stay explicit

1. **Cluster and reconnect:** configure a seed pool and let `async-nats` perform reconnect and server discovery. The spike verifies one short node interruption, not every topology, DNS, proxy, or extended-outage case. Discovery must be configurable because advertised internal node addresses may not be reachable from a client behind a proxy or WebSocket ingress.
2. **Publish completion:** Core NATS `publish` does not return a subscriber count or an acknowledgement for each delivered message. A successful `flush` is a server round-trip barrier for earlier client-buffered commands; it is not a subscriber-delivery guarantee. Keep Redis subscriber counts specific to Redis and define what status counters mean for NATS.
3. **Batching and failures:** Redis `MULTI/EXEC` atomicity and RESP byte accounting do not transfer to NATS. Publish NATS chunks in order and flush them, but do not claim all-or-nothing batches. Model the ambiguity of connection loss and the NATS client's reconnect buffer before settling or retrying messages; test its bounded-queue/backpressure behavior.
4. **Payload limits:** RESP request limits remain Redis-specific. NATS provides a server-advertised maximum payload through the client; the local test server reported 1 MiB. Apply oversized-message policy against the active NATS limit and test servers with customized limits.
5. **WebSocket compression:** set `websocket.compress: true` on NATS listeners where compression is desired; permessage-deflate is negotiated independently on each WebSocket connection and only used when both peers offer support. `async-nats` 0.50.0 uses `tokio-websockets` 0.10.1, whose documented limitations say WebSocket compression is unsupported, so this relay currently connects uncompressed. Other clients can negotiate it if their WebSocket implementation supports the extension. The local Compose config enables the server option and confirms the Rust connection still works without compression.
6. **Echo/loop prevention:** NATS `no_echo` suppresses messages published through the same client connection only. Since the relay has separate input/output connections, it does not by itself prevent output messages from matching the input subscription.

   **Implemented (0.3 step 4).** Each NATS output stamps a `Relay-Hop: 1` header on every message it forwards; the NATS input drops any envelope whose message already carries that header. This breaks NATS-to-NATS loops without rewriting the payload (byte-transparent). The payload is never modified, so the guard is invisible to subscribers that ignore headers. Redis is unaffected (no headers; the Redis echo filters still handle the same-server channel case). Status exposes `loop_suppressed_messages_total`.

   Spike evidence (`spikes/nats/src/echo.rs`, `NATS_ECHO_PROBE=1`): with separate connections and no guard, an overlapping subscription loops (~900 messages in 1.2 s); with the `Relay-Hop` guard it forwards exactly one. `no_echo` alone only stops the same-connection case. Runtime e2e: a self-feeding config (input `caseloop.>` -> output `caseloop.out.`) reported relay `input=1 output=1 loop_suppressed=1`, and a normal non-overlapping run reported `loop_suppressed=0`.

## Implementation sequence

0. **Load harness plan:** decide the workload generator, metrics collection, and pass/fail gates before writing the adapters, so throughput is designed in rather than bolted on.
1. **Protocol-neutral config and types:** preserve old Redis JSON, add the exactly-one-backend validation/schema rules, normalize addresses and subscription identity, and add unit tests.

   **Shipped (foundation, 0.3 step 1).** Backend selection is expressed with two optional keys, `redis` and `nats`, on `input` and on each output; existing Redis JSON is unchanged because no discriminator is added.

   ```json
   // before (0.2, still valid) and after (0.3) are byte-identical for Redis:
   "input": { "redis": { "host": "localhost" }, "subscriptions": [ ... ] }

   // new NATS selection:
   "input": {
     "nats": {
       "servers": ["nats://localhost:4222", "nats://other:4222"],
       "name": "relay",
       "connect_timeout_ms": 5000,
       "ignore_discovered_servers": false,
       "auth": { "type": "user_password", "username": "relay", "password_env": "NATS_PASSWORD" },
       "tls": { "root_cert_file": "/run/secrets/ca.pem" }
     },
     "subscriptions": [ { "type": "nats_subject", "subject": "prices.>" } ]
   }
   ```

   *Representation chosen.* `InputConfig`/`OutputConfig` keep `redis: Option<RedisConfig>` and `nats: Option<NatsConfig>` side by side, and a `config::Endpoint<'a>` enum (`Redis`/`Nats`) is the resolved runtime view via `InputConfig::endpoint(path)`/`OutputConfig::endpoint(path)`. Serialization/deserialization stays on the two optional keys so 0.2 files parse verbatim. `--check-config` and the `input.endpoint(path)`/`output.endpoint(path)` accessors reject zero or two backends with a path-qualified message. The generated JSON Schema adds `allOf: [{ oneOf: [{ required: ["redis"] }, { required: ["nats"] }] }]` to `InputConfig` and `OutputConfig` (appended to `OutputConfig`'s existing channel-policy `allOf`), so a schema validator enforces the same constraint.

   A literal `#[serde(untagged)]` enum over `{redis: ...}`/`{nats: ...}` variants was rejected as the serde shape because a struct with two `Option` fields only errors when *both* keys are present if the variants are not `deny_unknown_fields`; the two-optional-key struct plus explicit resolution gives a clear error for both the zero and the two cases, and keeps the error message referencing the JSON path. Rust field access must now go through the endpoint accessors rather than a hard `redis` field.

   Also shipped: `Subscription::NatsSubject { subject, output_prefix, output_suffix }` with native `*`/`>` semantics (`nats_subject` JSON tag), `validate_nats_subject` (rejects empty, whitespace, empty tokens, partial `*`/`>` tokens, and a non-final `>`), cross-checks that Redis subscriptions require a Redis input and NATS subjects require a NATS input, and NATS endpoint validation for `servers`, timeout, auth variants, and paired mTLS client cert/key. A new backend boundary module (`src/service/backend.rs`) defines the normalized `InputEnvelope { source_address, payload, subscription_identity }` and the `InputSource`/`OutputSink` traits, but the running Redis path is not rewired onto them yet, so the traits are `#[allow(dead_code)]` until the adapters land. Selecting any NATS endpoint fails fast at startup with `NATS backend not implemented yet ...`. Redis behavior, defaults, and byte-for-byte subscription matching are unchanged; the suite is 137 passed / 8 ignored (130 existing plus 7 new).
2. **NATS input:** connect to seed URLs, subscribe using native subject syntax, fan in subscription streams, and test exact/wildcard and overlapping-subscription mapping.
3. **NATS output:** share the existing queue/policy worker where possible; implement ordered publish/flush chunks, payload-limit handling, backend-accurate metrics, and explicit reconnect/error settlement.
4. **Loop safety and lifecycle:** design output-echo prevention, connection-event metrics, reload/shutdown behavior, and initial-connect/reconnect policy.
5. **Acceptance tests/docs:** exercise all four backend combinations; run a multi-node NATS cluster with a node stopped during traffic; cover TCP, TLS, WS, and WSS; verify legacy Redis configs and the complete Redis suite; add configuration examples and release notes.

## Load testing plan for 0.3

The existing Redis hot-path benchmarks (`examples/hotpath_benchmark.rs`, `examples/hotpath_mix_benchmark.rs`, `compose.hotpath.test.yml`) stay the baseline. Extend the same harness style to NATS instead of inventing a second measurement methodology.

**Matrix** — every cell runs the same workload shape so numbers are comparable:

| Input | Output | What it isolates |
| --- | --- | --- |
| Redis | Redis | regression baseline for the refactor (must not regress vs 0.2) |
| Redis | NATS | output-side NATS cost, Redis input unchanged |
| NATS | Redis | input-side NATS cost, Redis output unchanged |
| NATS | NATS | full NATS path incl. fan-out and loop guard overhead |

**Transports:** run the NATS cells over TCP and WSS (and TLS where deployed) because framing cost differs; report them separately rather than averaging.

**Modes** — mirror the Redis harness:
- *closed-loop*: wait for every input publish ack; measures latency at controlled rate.
- *open-loop*: pipelined publishes without per-message acks; finds the saturation plateau and backlog behavior.

**Workload knobs:** publisher count, messages per publisher, payload size distribution, channel/subject fan-out (>1 output), and conflation interval (direct vs 100/200 ms vs +dedup). Include a mixed-policy config like the Redis mix benchmark.

**Metrics per run:** end-to-end rate (input msg/s and output publish/s), p50/p95/p99/max latency, conflation and deduplication reduction %, publish errors, uncertain operations, reconnects, queue wait, publish RTT, and peak pending backlog.

**Collection:** reuse the status HTTP endpoint (`GET /`) for service counters and the harness's own subscriber for end-to-end latency. Add NATS-side completion semantics explicitly (no subscriber count; `flush` round trip) so a NATS cell is never compared on a Redis-only metric.

**Environment discipline:** pin NATS server image/version and settings (max_payload, no JetStream, WebSocket `compress` on/off), pin payload generator seeds, and run each cell as a median of N repetitions. Record run-to-run variance; treat differences under the observed noise band as noise.

**Gates (fill numbers after the first baseline run):**
- Redis→Redis must stay within the 0.2 baseline noise band.
- NATS cells must sustain the target rate *T* msg/s per output with zero publish errors and bounded backlog, where *T* is set from the first measured plateau.
- WSS cells stay within an agreed factor of the TCP cells.
- Unknown/uncertain publishes stay at zero on a healthy local cluster.

**Artifacts:** add a `compose.nats.hotpath.test.yml` (or extend the existing benchmark compose) and a NATS publisher/subscriber harness, keeping production payloads and channel names out of the repo.

**First client-path baseline (spike, not the relay).** `spikes/nats/src/load.rs` (`NATS_LOAD_TEST=1`) against the local three-node Compose cluster over TCP, release build, on the development workstation:

| Publishers | Total | Payload | Flush every | Publish rate | End-to-end rate | Delivered | RTT p50/p95/p99 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 4 | 200,000 | 256 B | 64 | 143.8k msg/s | 140.2k msg/s | 200,000 | 43.5 / 76.3 / 87.6 ms |

This measures the raw `async-nats` client path with a loopback subscriber, not the relay. It only establishes that the transport itself is not the ceiling and gives a starting point for the target rate *T*. The relay's conflation/deduplication/fan-out overhead and the WSS framing cost still have to be measured on top of this. The harness stamps a per-message send timestamp (an earlier revision reused one payload and produced a meaningless whole-run "latency"; fixed before recording these numbers).

## Acceptance criteria for 0.3

- Existing Redis configurations continue to validate and existing Redis E2Es stay green.
- Each of the four input/output combinations forwards raw binary payloads with mappings intact.
- Existing filters, queue bounds, conflation, and deduplication behave consistently for either output backend, with documented NATS-specific completion semantics.
- NATS native subjects/wildcards validate correctly; overlapping subscriptions preserve their mapping identity; loop prevention is tested.
- A cluster failover test demonstrates continued operation with one node unavailable. TCP/TLS/WS/WSS are tested against supported configurations, including the address-discovery behavior used by deployment.
- Status and logs do not claim NATS per-message acknowledgement or subscriber counts that Core NATS does not provide. Secrets and production payloads are absent from committed artifacts.
- A documented load test covers all four input/output combinations and reports rate, p50/p95/p99 latency, conflation/deduplication reduction, publish errors, uncertain operations, and peak backlog. Results and the exact server/client tuning are recorded in this design.
