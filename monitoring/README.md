# Redis Conflated Pub/Sub monitoring

This directory contains a standard-library Python 3 collector, a small JSON configuration example, and importable XML templates for Zabbix 5.0 and Zabbix 7.0. The collector reads the service status over HTTP and submits values to Zabbix trapper items with one `zabbix_sender` invocation.

## Collected values

The service exposes one HTTP endpoint: `GET /` returns the status JSON. Configure `base_url` with the HTTP(S) origin (for example, `http://127.0.0.1:9090/`); the collector makes one GET request to `/`. There are no `/status` or `/health` routes. The response must be a JSON object with every field below:

| Field | Zabbix item key suffix | Type |
| --- | --- | --- |
| `schema_version` | `schema_version` | Unsigned integer |
| `state` | `state` | Text |
| `updated_at` | `updated_at` | Text |
| `started_at` | `started_at` | Text |
| `uptime_seconds` | `uptime_seconds` | Unsigned integer, seconds |
| `input_messages_total` | `input_messages_total` | Unsigned integer |
| `output_batches_total` | `output_batches_total` | Unsigned integer |
| `output_messages_total` | `output_messages_total` | Unsigned integer |
| `conflated_messages_total` | `conflated_messages_total` | Unsigned integer |
| `excluded_messages_total` | `excluded_messages_total` | Unsigned integer |
| `dropped_messages_total` | `dropped_messages_total` | Unsigned integer |
| `publish_errors_total` | `publish_errors_total` | Unsigned integer |
| `input_reconnects_total` | `input_reconnects_total` | Unsigned integer |
| `output_reconnects_total` | `output_reconnects_total` | Unsigned integer |
| `pending_keys` | `pending_keys` | Unsigned integer |
| `last_input_at` | `last_input_at` | Text or null |
| `last_flush_at` | `last_flush_at` | Text or null |
| `last_error` | `last_error` | Text or null |

The Zabbix `health` item is derived from the same response: `state == "running"` produces `1`, otherwise `0`; it is not a separate HTTP endpoint. If the root request fails or its response is invalid, the collector sends only `health=0` and leaves all other item values untouched rather than inventing zero counters. Null timestamps are sent as `never`, and a null error is sent as `none`. Error text is limited to 2,048 characters and line breaks are normalized for sender input.

The default item keys use the `redis_conflated_pubsub.` prefix, for example `redis_conflated_pubsub.input_messages_total`. If `key_prefix` is changed in the collector configuration, update the item keys in the imported template to use that same prefix.

## Setup

1. Enable the service HTTP listener that serves the status JSON at `GET /`. The project example uses `status.http.bind` set to `127.0.0.1` and port `9090`; adjust `base_url` in the monitoring configuration if the listener uses another address or port. Keep the listener restricted to trusted monitoring clients.
2. Install Python 3 and the `zabbix_sender` utility on the machine that will run the collector. The script uses Python standard-library modules only.
3. Import exactly one template matching the Zabbix server version: `zabbix_template_5.0.xml` for Zabbix 5.0 or `zabbix_template_7.0.xml` for Zabbix 7.0.
4. Create or select the monitored host and link the imported template. Set the `host` value in the JSON configuration to the host's technical name in Zabbix (not its visible name).
5. Copy `config.example.json` to `config.json` and set `zabbix_server`, `zabbix_port`, `base_url`, and `host` for the environment. The `sender_path` value may be an absolute path if `zabbix_sender` is not in the collector's `PATH`.
6. Test the collector from the same account that will run it:

   ```sh
   cp monitoring/config.example.json monitoring/config.json
   python3 monitoring/zabbix_sender.py --config monitoring/config.json
   ```

   If the root request fails or its response is invalid, the collector sends only the health item with value `0` and leaves counters untouched. The command fails if `zabbix_sender` fails or reports rejected values.

## Cron example

Run the collector once per minute; it sends all 19 values in one batch when the root response is valid:

```cron
* * * * * /usr/bin/python3 /opt/redis-conflated-pubsub/monitoring/zabbix_sender.py --config /etc/redis-conflated-pubsub/monitoring.json >> /var/log/redis-conflated-pubsub-monitoring.log 2>&1
```

Use absolute paths because cron has a limited `PATH` and working directory. The service status snapshot may update more frequently than the collector schedule; each collection sends the latest available snapshot. If the root request fails or its response is invalid, the script submits a health value only rather than fabricated zero counters.

## Zabbix sender and agent notes

- Every template item is a Zabbix trapper item. `zabbix_sender` connects directly to the configured Zabbix server or proxy; the Zabbix agent does not execute the collector unless you explicitly arrange that separately.
- The account running the collector must be able to execute `zabbix_sender`, reach the status listener, and connect to the configured Zabbix server/proxy port (default `10051`).
- The host name in the configuration must match the Zabbix technical host name exactly. Trapper items must be enabled and the template must be linked to that host.
- Restrict each trapper item's **Allowed hosts** setting to the collector's source address where appropriate. The default template leaves it unset so the allowed sender address can be configured for each deployment.
- If using the Zabbix agent account to run a UserParameter or scheduled job, grant it read access to this directory and execute access to Python and `zabbix_sender`; keep `config.json` readable only by the intended account.
- Zabbix item history is retained for seven days. Numeric items also keep trends for 365 days; text state, timestamp, and error items do not create trends.
