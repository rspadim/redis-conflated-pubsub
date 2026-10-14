# Operations and monitoring

## systemd (Linux)

Install the unit template and enable the service:

```sh
sudo install -m 0644 redis-conflated-pubsub.service.example /etc/systemd/system/redis-conflated-pubsub.service
sudo systemctl daemon-reload
sudo systemctl enable --now redis-conflated-pubsub.service
sudo systemctl status redis-conflated-pubsub.service
```

The `redis-conflated-pubsub.service.example` file is at the repository root;
adjust `User`, `ExecStart` and the paths before installing it in production.

## Debian/Ubuntu (.deb), RPM, Alpine (.apk) and Arch packages

Each release publishes packages for the four families, all built with
[nFPM](https://nfpm.goreleaser.com/) from the same static musl binary:

```sh
sudo apt install ./redis-conflated-pubsub_0.3.0-1_amd64.deb                     # Debian/Ubuntu
sudo dnf install ./redis-conflated-pubsub-0.3.0-1.x86_64.rpm                    # RHEL/Fedora/SUSE
sudo apk add --allow-untrusted ./redis-conflated-pubsub_0.3.0-r1_x86_64.apk     # Alpine
sudo pacman -U redis-conflated-pubsub-0.3.0-1-x86_64.pkg.tar.zst                # Arch
```

The packages are unsigned, like the other release artifacts, so each package
manager asks for confirmation. Then edit the configuration and start the
service:

```sh
sudo editor /etc/redis-conflated-pubsub/config.json
sudo systemctl enable --now redis-conflated-pubsub.service   # Debian, RPM, Arch
sudo rc-update add redis-conflated-pubsub default            # Alpine (OpenRC)
sudo rc-service redis-conflated-pubsub start
```

No package declares dependencies (the binary is static) and all of them install
`/usr/bin/redis-conflated-pubsub`, the service account declared through
`sysusers.d`, `/etc/redis-conflated-pubsub/config.json` and
`/etc/redis-conflated-pubsub/redis.env` (mode `0600`, for the `*_env` fields).
The systemd families install the unit at
`/usr/lib/systemd/system/redis-conflated-pubsub.service`; Alpine uses an OpenRC
script in `/etc/init.d` instead. Logs and state live in
`/var/log/redis-conflated-pubsub` and `/var/lib/redis-conflated-pubsub`. The
service is not enabled or started during installation, and a Debian `purge`
removes the account, the state and the configuration.

## Reload configuration on Unix

Send `SIGHUP` to reload the configured JSON without restarting the unit:

```sh
sudo systemctl kill --signal=HUP --kill-whom=main redis-conflated-pubsub.service
```

The service validates the file **before** switching; an invalid configuration is
rejected and the current runtime keeps running. A valid reload drains pending
queues and restarts the input/output workers, so there is a brief Pub/Sub input
gap and messages may be missed (Pub/Sub has no replay). Status counters and
uptime reset.

A changed `instance_lock.path` is acquired during the reload **before** releasing
the old one, and the reload is rejected if the new lock is unavailable. Changes
to `logging` or updating the executable itself still require a full restart.

## Status and metrics

Enable the HTTP status endpoint:

```json
"status": {
  "http": {
    "bind": "127.0.0.1",
    "port": 9090
  }
}
```

`GET /` serves the per-output metrics. `GET /filters` exposes the full channel
names and the current filter/policy cache entries; it is **disabled by default**
and should only be enabled with
`status.http.filters_endpoint_enabled: true` on a trusted/private interface,
because channel names can be sensitive.

```sh
curl http://127.0.0.1:9090/
```

Alternatively, `status.path` writes an atomic JSON snapshot to a file, useful
when you do not want to expose a port.

## Zabbix monitoring

The Python 3 collector reads the status over HTTP and sends trapper values with
`zabbix_sender`, using only the standard library.

### Prerequisites

- Service running with the HTTP status listener enabled.
- Python 3 and `zabbix_sender` on the collector machine.
- Network access from the collector to the status and to the Zabbix server or
  proxy (default port `10051`).

### Configure and test

```sh
cp monitoring/config.example.json monitoring/config.json
curl --fail http://127.0.0.1:9090/
python3 monitoring/zabbix_sender.py --config monitoring/config.json
```

Set `base_url` to the status endpoint's root URL, `zabbix_server`/`zabbix_port`
to the server or proxy, and `host` to the host's **exact technical name** in
Zabbix. `key_prefix` must match the imported template (default
`redis_conflated_pubsub.`). Set `sender_path` to an absolute path if
`zabbix_sender` is not on `PATH`.

With a valid response, the collector sends **34 values** in one batch. If the
endpoint is unavailable or the response is invalid, it sends only `health=0` and
leaves the other values as they are.

If the host already has a Zabbix agent, `zabbix_config` lets you inherit
`ServerActive`, `Hostname` and TLS from the agent file (usually
`/etc/zabbix/zabbix_agentd.conf`). `host` is still required and must be the
agent's `Hostname`.

### Import the template

Import **one** XML file according to the server version:

| Zabbix version | File | Menu |
| --- | --- | --- |
| 5.0 | `monitoring/zabbix_template_5.0.xml` | Configuration → Templates → Import |
| 7.0 | `monitoring/zabbix_template_7.0.xml` | Data collection → Templates → Import |

Create or select a host, link **Template Redis Conflated Pub/Sub** and set the
collector's `host` to the host's technical name. The items are trapper items: a
Zabbix agent is not needed for the collector to run. Check `health = 1` and
`schema_version = 5`.

### Collected values

| Status field | Key suffix | Meaning |
| --- | --- | --- |
| derived from `state` | `health` | `1` when `state` is `running`, otherwise `0` |
| `schema_version` | `schema_version` | Integer (remains `5`) |
| `state` | `state` | Service state |
| `updated_at`, `started_at`, `last_input_at`, `last_flush_at` | same name | Timestamps; null becomes `never` |
| `last_error` | `last_error` | Text; null becomes `none` |
| `uptime_seconds` | `uptime_seconds` | Seconds |
| `input_messages_total`, `input_payload_bytes_total` | same name | Input message count and bytes |
| `output_batches_total`, `output_messages_total`, `output_payload_bytes_total` | same name | Published batches, messages and bytes |
| `conflated_messages_total`, `conflated_payload_bytes_total` | same name | Superseded messages and bytes |
| `deduplicated_messages_total`, `deduplicated_payload_bytes_total` | same name | TTL-suppressed messages and bytes |
| `excluded_messages_total` | `excluded_messages_total` | Excluded messages |
| `dropped_messages_total`, `dropped_payload_bytes_total` | same name | Policy drops and definitive pre-send failures |
| `truncated_messages_total`, `truncated_payload_bytes_total` | same name | Truncated messages and bytes removed |
| `publish_errors_total`, `publish_error_messages_total` | same name | Publish failures and affected messages |
| `uncertain_transactions_total`, `uncertain_messages_total` | same name | Uncertain transactions and messages |
| `input_reconnects_total`, `output_reconnects_total` | same name | Redis reconnects |
| `nats_client_connects_total`, `nats_client_disconnects_total` | same name | NATS client connections (Core NATS reconnects internally) |
| `loop_suppressed_messages_total` | `loop_suppressed_messages_total` | NATS loop-guard drops |
| `pending_keys` | `pending_keys` | Pending entries |
| `outputs` | `outputs_json` | Compact JSON with the per-output metrics |

The deduplication counters are additive; `schema_version` remains `5`. The
per-output JSON includes destination-level publish, conflation and deduplication
metrics, with the same raw-payload-byte semantics as the global counters. Error
text is limited to 2,048 characters and line breaks are normalized.

### Run on a schedule

For example, once per minute with cron:

```cron
* * * * * /usr/bin/python3 /opt/redis-conflated-pubsub/monitoring/zabbix_sender.py --config /etc/redis-conflated-pubsub/monitoring.json >> /var/log/redis-conflated-pubsub-monitoring.log 2>&1
```

Use absolute paths: cron has a limited `PATH` and working directory. Each run
sends the latest status snapshot. The Zabbix agent does not run the collector;
`zabbix_sender` connects directly to the server or proxy.

## Troubleshooting

- **Nothing arrives at the outputs** — check the subscription match and the
  channel mapping; in Redis, `PSUBSCRIBE` uses glob and is case-sensitive.
- **Publish failing with `0` subscribers in NATS** — that is expected in Core
  NATS. Read `publish_errors_total` and `uncertain_messages_total`.
- **Messages disappear in bursts** — check `shed_messages_total`,
  `pending_queue_bytes` and `oldest_pending_age_ms`. The default queue is
  unbounded, so shedding only happens with configured limits.
- **`--check-config` fails** — the message points at the field path; check that
  `default` is last in `channel_policies`, exactly one backend per side and
  paired mTLS files.
