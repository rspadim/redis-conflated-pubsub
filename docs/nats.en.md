# NATS backend

The input and **each** output select exactly one backend. Add a `nats` object next
to the existing `redis` one: the keys are additive, so existing Redis
configurations stay valid unchanged.

- Either `redis` or `nats` alone is valid.
- Exactly one backend per side is required. Omitting both **or** setting both fails
  `--check-config` and startup:

  ```text
  input must select exactly one backend, but both `redis` and `nats` are configured
  input must select one backend with `redis` or `nats`
  ```

- The generated JSON Schema encodes the same rule with a `oneOf` on
  `required: ["redis"]` / `required: ["nats"]` for `input` and for each output.

!!! info "Core NATS Pub/Sub only"
    Not JetStream: no persistence and no replay.

## Endpoint fields

| Field | Meaning |
| --- | --- |
| `servers` | One or more seed URLs. Required, non-empty. The scheme is mandatory and selects the transport: `nats://` (TCP), `tls://` (TLS), `ws://`/`wss://` (WebSocket). A bare `host:port` is rejected. |
| `name` | Optional connection name reported in the NATS server client listing. |
| `connect_timeout_ms` | Connect and handshake timeout. Default `5000`. |
| `ignore_discovered_servers` | When `true`, only the configured `servers` are used and cluster-advertised servers are ignored. Useful behind a proxy or WebSocket ingress whose internal addresses are unreachable. |
| `auth` | Authentication variant. Defaults to `none`. |
| `tls` | Trusted root and/or client identity. See below. |

### One transport family per list

Every URL in a single `servers` list must use the same transport family, and
`ws`/`wss` cannot be mixed with `nats`/`tls` in one list (one NATS connection
requires one family). Different inputs and outputs may use different families. A
mixed list is rejected:

```sh
input.servers mixes nats and wss transports (Tcp and WebSocketTls); one NATS connection requires a single transport family
```

## Authentication

Secrets are always referenced by **environment variable name** or **file path**,
never written into JSON.

| `auth.type` | Secret reference | Notes |
| --- | --- | --- |
| omitted or `none` | none | Development only; use a protected network. |
| `user_password` | `username` + `password_env` | Username plus the environment variable holding the password. |
| `token` | `token_env` | Environment variable holding the token. |
| `nkey` | `seed_file` | Path to the NKey seed file. |
| `credentials_file` | `path` | Path to the `.creds` file (**user JWT plus its NKey seed**), not a bearer JWT. |

=== "user_password"

    ```json
    "auth": { "type": "user_password", "username": "relay", "password_env": "NATS_PASSWORD" }
    ```

=== "token"

    ```json
    "auth": { "type": "token", "token_env": "NATS_TOKEN" }
    ```

=== "nkey"

    ```json
    "auth": { "type": "nkey", "seed_file": "/run/secrets/relay.nk" }
    ```

=== "credentials_file"

    ```json
    "auth": { "type": "credentials_file", "path": "/run/secrets/relay.creds" }
    ```

TLS is separate from `auth` and is selected by the URL scheme; the `tls` block
only extends it with trusted roots or a client identity. For remote password or
token connections use `tls://` or `wss://` so the secret is not sent in cleartext.

```json
"tls": {
  "root_cert_file": "/run/secrets/nats-ca.pem",
  "client_cert_file": "/run/secrets/relay-cert.pem",
  "client_key_file": "/run/secrets/relay-key.pem"
}
```

mTLS requires `client_cert_file` and `client_key_file` together (`--check-config`
rejects one without the other).

!!! danger "Never inline secrets in JSON"
    A password, token, NKey seed or key material never goes inline. Reference it
    through an environment variable name (`password_env`, `token_env`) or a file
    path (`seed_file`, `path` and the `tls` files), and inject the value at
    deployment time. Keep secrets out of Git, logs and process arguments.

## Subjects and wildcards

NATS input subscriptions use the native `nats_subject` type and NATS semantics.
Redis `SUBSCRIBE`/`PSUBSCRIBE` globs are **not** translated.

- Redis input: `{"type": "subscribe", "channel": "prices:EURUSD"}` or
  `{"type": "psubscribe", "pattern": "prices:*"}` with Redis glob semantics.
- NATS input: `{"type": "nats_subject", "subject": "prices.EURUSD"}` or
  `{"type": "nats_subject", "subject": "prices.>"}`.

In a NATS subject, `.` separates tokens, `*` matches exactly one token and `>`
matches one or more trailing tokens and must be last. `prices:*` (Redis) and
`prices.>` (NATS) are different patterns; the relay never rewrites one into the
other. `--check-config` rejects empty subjects, whitespace, empty tokens, partial
wildcards such as `pric*`, and a `>` that is not final.

For NATS outputs, channel mapping keeps its usual meaning: the subscription's
`output_prefix`/`output_suffix`, then the output's
`channel_prefix`/`channel_suffix`, are concatenated around the source subject. The
final string is used **directly** as the publish subject. There is no automatic
renaming or escaping, so the mapped result must be a concrete, valid subject (no
`*` or `>`); an invalid mapped subject is rejected with a definitive pre-send error
instead of being published.

### No acknowledgement and no subscriber count

Core NATS has **no per-message acknowledgement and no subscriber count**. A
successful publish means the command was accepted by the local client buffer; the
`flush` that follows each batch drains the **local write buffer** to the socket,
and `async-nats` `flush` does **not** wait for the server to process the commands.

The relay therefore reports a subscriber count of `0` for NATS outputs — do not
read that as "nobody is listening". Read `publish_errors_total`,
`uncertain_messages_total`, `input_reconnects_total` and
`output_reconnects_total`.

## Loop guard

A relay output published on one connection can match its own input subscription on
another, so NATS-to-NATS loops are broken with a `ConflatedPS-Hop: 1` header
(vendor-namespaced so it cannot collide with an application header). Every NATS
output stamps it on each forwarded message, and the NATS input drops any message
that already carries it.

- The payload is **never** modified: the guard is invisible to subscribers that
  ignore headers.
- **Only the payload is forwarded, not application headers.** The output sets just
  `ConflatedPS-Hop`, so inbound headers (content-type, tracing, `Nats-Msg-Id`, …)
  are dropped when a message crosses the relay. This is intentional: propagating
  them would make the per-message wire size variable and complicate the
  payload-ceiling accounting.
- Suppressed messages are counted in `loop_suppressed_messages_total`.
- The guard is NATS-specific: a Redis input has no headers to stamp or read and
  keeps its same-server echo handling. A Redis input never receives NATS output
  traffic.

## Example: NATS → NATS

Forward `market.>` from a NATS cluster to an archive endpoint, mapping
`market.EURUSD` to `archive.market.EURUSD.v1`:

```json
{
  "max_bytes_per_exec": 4194304,
  "oversized_message_policy": "send",
  "client_name": "ConflatedPS-{version}-{role}::{user}::{host}",
  "input": {
    "nats": {
      "servers": ["nats://nats-1:4222", "nats://nats-2:4222"],
      "name": "conflated-input",
      "connect_timeout_ms": 5000,
      "ignore_discovered_servers": false,
      "auth": {
        "type": "user_password",
        "username": "relay",
        "password_env": "NATS_INPUT_PASSWORD"
      }
    },
    "subscriptions": [
      {
        "type": "nats_subject",
        "subject": "market.>",
        "output_prefix": "",
        "output_suffix": ""
      }
    ]
  },
  "outputs": {
    "nats-archive": {
      "nats": {
        "servers": ["tls://nats.example.net:4222"],
        "name": "conflated-archive",
        "auth": {
          "type": "credentials_file",
          "path": "/run/secrets/relay.creds"
        },
        "tls": {
          "root_cert_file": "/run/secrets/nats-ca.pem"
        }
      },
      "channel_prefix": "archive.",
      "channel_suffix": ".v1",
      "conflation": {
        "interval_ms": 250,
        "max_commands_per_exec": 256
      },
      "deduplication": {
        "ttl_ms": 5000
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
    "prefix": "redis-conflated-pubsub",
    "enabled": true,
    "level": "info",
    "retention_days": 14,
    "max_total_size_mb": 1024
  }
}
```

## Example: Redis → NATS

Forward `feed:*` from a Redis input to NATS over WebSocket TLS, mapping
`feed:alpha` to `nats.feed:alpha`, using the `token` variant:

```json
{
  "input": {
    "redis": {
      "host": "redis.example.net",
      "port": 6379,
      "username": null,
      "password_env": "REDIS_PASSWORD",
      "tls": false,
      "database": 0,
      "connect_timeout_ms": 5000
    },
    "subscriptions": [
      {
        "type": "psubscribe",
        "pattern": "feed:*",
        "output_prefix": "",
        "output_suffix": ""
      }
    ]
  },
  "outputs": {
    "nats-out": {
      "nats": {
        "servers": ["wss://nats.example.net:443"],
        "ignore_discovered_servers": true,
        "auth": {
          "type": "token",
          "token_env": "NATS_TOKEN"
        }
      },
      "channel_prefix": "nats.",
      "channel_suffix": "",
      "conflation": {
        "interval_ms": 100
      }
    }
  },
  "instance_lock": {
    "path": "redis-conflated-pubsub.lock"
  }
}
```

!!! tip "Complete example"
    The repository ships
    [`config.nats.example.json`](https://github.com/rspadim/redis-conflated-pubsub/blob/main/config.nats.example.json)
    with a NATS input using `credentials_file`, a NATS output using a token and a
    Redis copy output. Validate it before running:

    ```sh
    ./redis-conflated-pubsub --check-config --config config.nats.example.json
    ```
