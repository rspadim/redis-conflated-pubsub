# Zabbix monitoring (v0.1.3)

This guide configures the Python 3 collector and the Zabbix 5.0 or 7.0 template. The collector reads the service status over HTTP and sends trapper-item values with `zabbix_sender`; it uses only Python standard-library modules.

## Prerequisites

- Redis Conflated Pub/Sub v0.1.3 running with its HTTP status listener enabled. The project configuration example uses:

  ```json
  "status": {
    "http": {
      "bind": "127.0.0.1",
      "port": 9090
    }
  }
  ```

  The status JSON is at `GET /` (for example, `http://127.0.0.1:9090/`). If the collector runs on another machine, bind to an address it can reach and allow only trusted monitoring traffic.
- Python 3 and `zabbix_sender` installed on the machine that will run the collector.
- Network access from the collector to the status URL and to the configured Zabbix server or proxy (default port `10051`).

## Configure and test the collector

Copy the example and edit `monitoring/config.json` for your environment:

```sh
cp monitoring/config.example.json monitoring/config.json
```

Set `base_url` to the status endpoint's root URL, `zabbix_server` and `zabbix_port` to the server or proxy, and `host` to the Zabbix host's exact **technical name**. `key_prefix` must match the imported template (default: `redis_conflated_pubsub.`). Set `sender_path` to an absolute path if `zabbix_sender` is not on `PATH`.

Check the endpoint, then run the collector from the same account that will schedule it:

```sh
curl --fail http://127.0.0.1:9090/
python3 monitoring/zabbix_sender.py --config monitoring/config.json
```

With a valid status response, the collector sends 31 values in one batch. If the endpoint is unavailable or the response is invalid, it sends only `health=0` and leaves other values unchanged. The command exits with an error if `zabbix_sender` fails or Zabbix rejects values.

## Import and link a template

Import **one** XML template matching your Zabbix server version:

| Zabbix version | Template file | Import menu |
| --- | --- | --- |
| 5.0 | `monitoring/zabbix_template_5.0.xml` | Configuration → Templates → Import |
| 7.0 | `monitoring/zabbix_template_7.0.xml` | Data collection → Templates → Import |

Create or select a host, link **Template Redis Conflated Pub/Sub**, and set the collector's `host` value to that host's technical name. The template items are trapper items; a Zabbix agent is not needed to run the collector. In Latest data, check that `health` is `1` and `schema_version` is `5` after a successful collection. If you configure an item's **Allowed hosts**, include the collector's source address.

## Collected values

The status endpoint is `GET /`; configure `base_url` with the HTTP(S) origin, such as `http://127.0.0.1:9090/`. The collector requests `/` and sends the following item values:

| Status field or derived value | Zabbix key suffix | Type / meaning |
| --- | --- | --- |
| Derived from `state` | `health` | Unsigned integer: `1` when `state` is `running`, otherwise `0` |
| `schema_version` | `schema_version` | Unsigned integer; remains `5` in v0.1.3 |
| `state` | `state` | Text service state |
| `updated_at`, `started_at`, `last_input_at`, `last_flush_at` | Same field name | Text timestamps; null is sent as `never` |
| `last_error` | `last_error` | Text; null is sent as `none` |
| `uptime_seconds` | `uptime_seconds` | Unsigned integer, seconds |
| `input_messages_total`, `input_payload_bytes_total` | Same field name | Input message count and bytes |
| `output_batches_total`, `output_messages_total`, `output_payload_bytes_total` | Same field name | Published batch, message, and payload-byte totals |
| `conflated_messages_total`, `conflated_payload_bytes_total` | Same field name | Superseded message count and payload-byte total |
| `deduplicated_messages_total` | `deduplicated_messages_total` | Unsigned integer, TTL-suppressed messages across outputs |
| `deduplicated_payload_bytes_total` | `deduplicated_payload_bytes_total` | Unsigned integer, raw input payload bytes suppressed across outputs |
| `excluded_messages_total` | `excluded_messages_total` | Unsigned integer |
| `dropped_messages_total`, `dropped_payload_bytes_total` | Same field name | Oversized-policy drops and definitive pre-send failures; failed messages are not retried |
| `truncated_messages_total`, `truncated_payload_bytes_total` | Same field name | Messages truncated and payload bytes removed |
| `publish_errors_total`, `publish_error_messages_total` | Same field name | Publish failures and affected messages |
| `uncertain_transactions_total`, `uncertain_messages_total` | Same field name | Uncertain publish transactions and affected messages |
| `input_reconnects_total`, `output_reconnects_total` | Same field name | Redis reconnect counts |
| `pending_keys` | `pending_keys` | Unsigned integer |
| `outputs` | `outputs_json` | Compact JSON text map of per-output metrics |

The v0.1.3 deduplication counters are additive; **`schema_version` remains `5`**. The per-output JSON includes destination-level published, conflated, deduplicated, and payload-byte metrics. Its deduplication byte counts use the same raw-input-payload semantics as the global counters. The default item keys use the `redis_conflated_pubsub.` prefix; if you change `key_prefix`, update the imported template's item keys to match. Error text is limited to 2,048 characters and line breaks are normalized for sender input.

## Run on a schedule

For example, run the collector once per minute with cron:

```cron
* * * * * /usr/bin/python3 /opt/redis-conflated-pubsub/monitoring/zabbix_sender.py --config /etc/redis-conflated-pubsub/monitoring.json >> /var/log/redis-conflated-pubsub-monitoring.log 2>&1
```

Use absolute paths because cron has a limited `PATH` and working directory. Each run sends the latest status snapshot. The Zabbix agent does not run the collector; `zabbix_sender` connects directly to the configured server or proxy.
