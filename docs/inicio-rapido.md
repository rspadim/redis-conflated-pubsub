# Início rápido

## 1. Baixe o binário

Pegue o arquivo da sua plataforma na
[página de releases](https://github.com/rspadim/redis-conflated-pubsub/releases):
Linux x86_64 (musl estático), Windows x86_64, macOS Intel ou macOS Apple Silicon.
Cada release publica também o `.sha256` ao lado do arquivo.

Para compilar a partir do código, veja [Desenvolvimento](desenvolvimento.md).

## 2. Valide a configuração

Salve uma das configurações abaixo como `config.json` e rode a validação. O
`--check-config` confere o JSON e as regras semânticas, incluindo a prevenção de
eco no mesmo servidor.

```sh
./redis-conflated-pubsub --check-config --config config.json
```

No Windows use `redis-conflated-pubsub.exe` com os mesmos argumentos.

## 3. Execute

```sh
./redis-conflated-pubsub --config config.json
```

## Exemplo: Redis local

Este exemplo mapeia `test-feed:alpha` para `db0:sub:test-feed:alpha:source` e
`db1:sub:test-feed:alpha:source`.

- `output0` **não** faz conflation (publica direto), mas ainda deduplica payloads
  idênticos por 5 segundos.
- `output1` usa conflation de 250 ms, TTL de 5 segundos, perfis por canal e um
  grupo de deduplicação compartilhado.

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

## Verifique

Em terminais separados, assine uma saída e publique na entrada:

```sh
# Terminal 1 — assina as saídas
redis-cli PSUBSCRIBE 'db0:*'
# Terminal 2 — publica na entrada
redis-cli PUBLISH 'test-feed:alpha' 'hello'
```

Opcionalmente confira as métricas por saída:

```sh
curl http://127.0.0.1:9090/
```

## Exemplo: entrada remota para um Redis local

Encaminha `test-feed:*` de um Redis remoto para um Redis local em
`127.0.0.1:16379`, DB 1. Aqui, `test-feed:alpha` vira `local:remote:test-feed:alpha`.

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

`subscribe` usa `channel`; `psubscribe` usa `pattern`. Credenciais sempre via
`password_env`; use `tls: true` para habilitar TLS.

## Próximo passo

- Ajuste perfis, políticas e filtros em [Configuração](configuracao.md).
- Vá para NATS em [Backend NATS](nats.md).
- Implante com [Docker](docker.md) ou [systemd](operacao.md).
