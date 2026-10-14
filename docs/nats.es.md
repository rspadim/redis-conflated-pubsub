# Backend NATS

La entrada y **cada** salida seleccionan exactamente un backend. Basta con añadir
un objeto `nats` junto al `redis` existente: las claves son aditivas, así que las
configuraciones Redis siguen siendo válidas sin cambios.

- O bien `redis` o bien `nats` por sí solo es válido.
- Se exige exactamente un backend por lado. Omitir los dos **o** definir los dos
  falla en `--check-config` y en el arranque:

  ```text
  input must select exactly one backend, but both `redis` and `nats` are configured
  input must select one backend with `redis` or `nats`
  ```

- El JSON Schema generado codifica la misma regla con `oneOf` sobre
  `required: ["redis"]` / `required: ["nats"]` para `input` y para cada salida.

!!! info "Solo Core NATS Pub/Sub"
    No es JetStream: sin persistencia y sin replay.

## Campos del endpoint

| Campo | Significado |
| --- | --- |
| `servers` | Una o más URLs de seed. Obligatorio y no vacío. El esquema es obligatorio y selecciona el transporte: `nats://` (TCP), `tls://` (TLS), `ws://`/`wss://` (WebSocket). Un `host:port` puro se rechaza. |
| `name` | Nombre de conexión opcional, reportado en el listado de clientes del servidor NATS. |
| `connect_timeout_ms` | Timeout de conexión y handshake. Predeterminado `5000`. |
| `ignore_discovered_servers` | Cuando es `true`, solo se usan los `servers` configurados y los servidores anunciados por el clúster se ignoran. Útil detrás de un proxy o ingress WebSocket con direcciones internas inalcanzables. |
| `auth` | Variante de autenticación. Predeterminado `none`. |
| `tls` | Raíz de confianza y/o identidad del cliente. Ver abajo. |

### Una sola familia de transporte por lista

Toda URL de una misma lista `servers` debe usar la misma familia de transporte, y
`ws`/`wss` no pueden mezclarse con `nats`/`tls` en la misma lista (una conexión NATS
exige una única familia). Entradas y salidas diferentes pueden usar familias
diferentes. Una lista mixta se rechaza:

```sh
input.servers mixes nats and wss transports (Tcp and WebSocketTls); one NATS connection requires a single transport family
```

## Autenticación

Los secretos siempre se referencian por **nombre de variable de entorno** o **ruta
de archivo**, nunca se escriben en el JSON.

| `auth.type` | Referencia del secreto | Observaciones |
| --- | --- | --- |
| omitido o `none` | ninguna | Solo desarrollo; usa una red protegida. |
| `user_password` | `username` + `password_env` | Usuario más la variable de entorno con la contraseña. |
| `token` | `token_env` | Variable de entorno con el token. |
| `nkey` | `seed_file` | Ruta del archivo de seed NKey. |
| `credentials_file` | `path` | Ruta del archivo `.creds` (**JWT de usuario + su seed NKey**), no un JWT bearer. |

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

TLS es independiente de `auth` y se selecciona por el esquema de la URL; el bloque
`tls` solo lo extiende con raíces de confianza o identidad del cliente. Para
conexiones remotas con contraseña o token, usa `tls://` o `wss://` para que el
secreto no viaje en claro.

```json
"tls": {
  "root_cert_file": "/run/secrets/nats-ca.pem",
  "client_cert_file": "/run/secrets/relay-cert.pem",
  "client_key_file": "/run/secrets/relay-key.pem"
}
```

mTLS exige `client_cert_file` y `client_key_file` juntos (`--check-config` rechaza
uno sin el otro).

!!! danger "Nunca incrustes secretos en el JSON"
    Una contraseña, token, seed NKey o material de clave nunca van inline.
    Referéncialos por variable de entorno (`password_env`, `token_env`) o ruta de
    archivo (`seed_file`, `path` y los archivos de `tls`), e inyecta el valor en el
    despliegue. Mantén los secretos fuera de Git, de los logs y de los argumentos
    de proceso.

## Subjects y wildcards

Las suscripciones de entrada NATS usan el tipo nativo `nats_subject` y la semántica
NATS. Los globs de `SUBSCRIBE`/`PSUBSCRIBE` de Redis **no** se traducen.

- Entrada Redis: `{"type": "subscribe", "channel": "prices:EURUSD"}` o
  `{"type": "psubscribe", "pattern": "prices:*"}` con semántica glob de Redis.
- Entrada NATS: `{"type": "nats_subject", "subject": "prices.EURUSD"}` o
  `{"type": "nats_subject", "subject": "prices.>"}`.

En un subject NATS, `.` separa tokens, `*` coincide exactamente con un token y `>`
coincide con uno o más tokens finales, y debe ser el último. `prices:*` (Redis) y
`prices.>` (NATS) son patrones diferentes; el relay nunca convierte uno en el otro.
`--check-config` rechaza subjects vacíos, con espacios, con tokens vacíos, con
wildcard parcial (como `pric*`) o con un `>` que no sea final.

Para salidas NATS, el mapeo mantiene el significado habitual: `output_prefix`/
`output_suffix` de la suscripción, después `channel_prefix`/`channel_suffix` de la
salida, concatenados alrededor del subject de origen. La cadena final se usa
**directamente** como subject de publicación. No hay renombrado ni escape
automático, así que el resultado mapeado debe ser un subject concreto y válido (sin
`*` ni `>`); un subject mapeado inválido se rechaza con un error definitivo antes
del envío, en lugar de publicarse.

### Sin ack y sin conteo de suscriptores

Core NATS **no tiene ack por mensaje ni conteo de suscriptores**. Un publish exitoso
significa que el comando fue aceptado en el buffer local del cliente; el `flush` que
sigue a cada lote drena el **buffer local de escritura** hacia el socket, y el
`flush` de `async-nats` **no** espera a que el servidor procese los comandos.

Por lo tanto, la salida NATS reporta `0` suscriptores — no lo leas como "nadie está
escuchando". Lee `publish_errors_total`, `uncertain_messages_total`,
`input_reconnects_total` y `output_reconnects_total`.

## Guarda de bucle

Una salida publicada en una conexión puede coincidir con su propia suscripción de
entrada en otra, así que los ciclos NATS→NATS se rompen con el header
`ConflatedPS-Hop: 1` (con namespace para no colisionar con headers de la
aplicación). Cada salida NATS estampa ese header en cada mensaje reenviado, y la
entrada NATS descarta cualquier mensaje que ya lo lleve.

- El payload **nunca** se modifica: la guarda es invisible para los suscriptores que
  ignoran headers.
- **Solo se reenvía el payload, no los headers de la aplicación.** La salida define
  solo `ConflatedPS-Hop`, así que los headers de entrada (content-type, tracing,
  `Nats-Msg-Id`, …) se descartan cuando el mensaje cruza el relay. Esto es
  intencional: propagarlos haría variable el tamaño en la red y complicaría la
  contabilidad del techo de payload.
- Los mensajes suprimidos se cuentan en `loop_suppressed_messages_total`.
- La guarda es específica de NATS: una entrada Redis no tiene headers que estampar o
  leer y mantiene el tratamiento de eco en el mismo servidor. Una entrada Redis
  nunca recibe tráfico de salida NATS.

## Ejemplo: NATS → NATS

Reenvía `market.>` desde un clúster NATS hacia un endpoint de archivo, mapeando
`market.EURUSD` a `archive.market.EURUSD.v1`:

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

## Ejemplo: Redis → NATS

Reenvía `feed:*` desde una entrada Redis hacia NATS vía WebSocket TLS, mapeando
`feed:alpha` a `nats.feed:alpha`, con la variante `token`:

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

!!! tip "Ejemplo completo"
    El repositorio incluye
    [`config.nats.example.json`](https://github.com/rspadim/redis-conflated-pubsub/blob/main/config.nats.example.json)
    con entrada NATS vía `credentials_file`, salida NATS con token y una salida de
    copia en Redis. Valídalo antes de ejecutar:

    ```sh
    ./redis-conflated-pubsub --check-config --config config.nats.example.json
    ```
