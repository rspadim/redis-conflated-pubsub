# Inicio rápido

## 1. Descarga el binario

Toma el archivo de tu plataforma en la
[página de releases](https://github.com/rspadim/redis-conflated-pubsub/releases):
Linux x86_64 (musl estático), Windows x86_64, macOS Intel o macOS Apple Silicon.
Cada release publica también el `.sha256` junto al archivo.

Para compilar desde el código, consulta [Desarrollo](desenvolvimento.md).

## 2. Valida la configuración

Guarda una de las configuraciones de abajo como `config.json` y ejecuta la
validación. `--check-config` verifica el JSON y las reglas semánticas, incluida la
prevención de eco en el mismo servidor.

```sh
./redis-conflated-pubsub --check-config --config config.json
```

En Windows usa `redis-conflated-pubsub.exe` con los mismos argumentos.

## 3. Ejecútalo

```sh
./redis-conflated-pubsub --config config.json
```

## Ejemplo: Redis local

Este ejemplo mapea `test-feed:alpha` a `db0:sub:test-feed:alpha:source` y
`db1:sub:test-feed:alpha:source`.

- `output0` **no** aplica conflación (publica directo), pero igualmente deduplica
  payloads idénticos durante 5 segundos.
- `output1` usa conflación de 250 ms, TTL de 5 segundos, perfiles por canal y un
  grupo de deduplicación compartido.

```json
{
  "max_bytes_per_exec": 4194304,
  "oversized_message_policy": "send",
  "client_name": "ConflatedPS-{version}-{role}::{user}::{host}",
  "input": {
    "redis": {
      "host": "localhost",
      "port": 6379,
      "username": null,
      "password_env": null,
      "tls": false,
      "database": 0,
      "connect_timeout_ms": 5000
    },
    "exclude_output_echoes": true,
    "exclude_sentinel_pubsub": true,
    "filters": [],
    "filter_cache_max_entries": 16384,
    "filter_default": "accept",
    "subscriptions": [
      {
        "type": "psubscribe",
        "pattern": "test-feed:*",
        "output_prefix": "sub:",
        "output_suffix": ":source"
      }
    ]
  },
  "outputs": {
    "output0": {
      "redis": {
        "host": "localhost",
        "port": 6379,
        "username": null,
        "password_env": null,
        "tls": false,
        "database": 0,
        "connect_timeout_ms": 5000
      },
      "channel_prefix": "db0:",
      "channel_suffix": "",
      "filters": [],
      "filter_cache_max_entries": 16384,
      "channel_policy_cache_max_entries": 16384,
      "filter_default": "accept",
      "conflation": {
        "interval_ms": 0,
        "max_commands_per_exec": 100,
        "oversized_message_policy": "send"
      },
      "deduplication": {
        "ttl_ms": 5000,
        "in_flight_suppression": false,
        "max_entries": 16384,
        "max_cache_bytes": 67108864
      },
      "queue_max_messages": 65536,
      "queue_max_bytes": 67108864,
      "queue_overflow_policy": "drop_newest"
    },
    "output1": {
      "redis": {
        "host": "localhost",
        "port": 6379,
        "username": null,
        "password_env": null,
        "tls": false,
        "database": 1,
        "connect_timeout_ms": 5000
      },
      "channel_prefix": "db1:",
      "channel_suffix": "",
      "filters": [],
      "filter_cache_max_entries": 16384,
      "channel_policy_cache_max_entries": 16384,
      "filter_default": "accept",
      "conflation": {
        "interval_ms": 250,
        "max_commands_per_exec": 2,
        "max_bytes_per_exec": 2097152,
        "oversized_message_policy": "send"
      },
      "deduplication": {
        "ttl_ms": 5000
      },
      "deduplication_groups": {
        "symbols": {
          "ttl_ms": 5000,
          "round_ms": 1000,
          "restart_on_change": true
        }
      },
      "profiles": {
        "fast_no_dedup": {
          "conflation.interval_ms": 100,
          "deduplication.ttl_ms": 0
        },
        "shared_symbols": {
          "conflation.interval_ms": 250,
          "deduplication.group": "symbols"
        }
      },
      "channel_policies": [
        {
          "glob": "db1:sub:test-feed:alpha:source",
          "profile": "fast_no_dedup"
        },
        {
          "prefix": "db1:sub:test-feed:beta:",
          "profile": "shared_symbols"
        },
        {
          "default": true,
          "conflation.interval_ms": 250,
          "deduplication.group": "symbols"
        }
      ]
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

## Verifica

En terminales separadas, suscríbete a una salida y publica en la entrada:

```sh
# Terminal 1 — se suscribe a las salidas
redis-cli PSUBSCRIBE 'db0:*'
# Terminal 2 — publica en la entrada
redis-cli PUBLISH 'test-feed:alpha' 'hello'
```

Opcionalmente revisa las métricas por salida:

```sh
curl http://127.0.0.1:9090/
```

## Ejemplo: entrada remota hacia un Redis local

Reenvía `test-feed:*` desde un Redis remoto hacia un Redis local en
`127.0.0.1:16379`, DB 1. Aquí, `test-feed:alpha` se convierte en `local:remote:test-feed:alpha`.

```json
{
  "max_bytes_per_exec": 4194304,
  "oversized_message_policy": "send",
  "client_name": "ConflatedPS-{version}-{role}::{user}::{host}",
  "input": {
    "redis": {
      "host": "redis.example.net",
      "port": 6379,
      "username": null,
      "password_env": null,
      "tls": false,
      "database": 0,
      "connect_timeout_ms": 5000
    },
    "subscriptions": [
      {
        "type": "psubscribe",
        "pattern": "test-feed:*",
        "output_prefix": "remote:",
        "output_suffix": ""
      }
    ]
  },
  "outputs": {
    "output1": {
      "redis": {
        "host": "127.0.0.1",
        "port": 16379,
        "username": null,
        "password_env": null,
        "tls": false,
        "database": 1,
        "connect_timeout_ms": 5000
      },
      "channel_prefix": "local:",
      "channel_suffix": "",
      "conflation": {
        "interval_ms": 250,
        "max_commands_per_exec": 100,
        "max_bytes_per_exec": 1048576
      }
    }
  }
}
```

```sh
redis-cli -h 127.0.0.1 -p 16379 -n 1 PSUBSCRIBE 'local:*'
```

`subscribe` usa `channel`; `psubscribe` usa `pattern`. Las credenciales siempre
mediante `password_env`; usa `tls: true` para habilitar TLS.

## Siguiente paso

- Ajusta perfiles, políticas y filtros en [Configuración](configuracao.md).
- Pasa a NATS en [Backend NATS](nats.md).
- Despliega con [Docker](docker.md) o [systemd](operacao.md).
