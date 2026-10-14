# Configuración

El relay lee un único JSON. Los campos de abajo son los que más afectan el
comportamiento; la referencia exacta y los límites los valida `--check-config`
y los describe el JSON Schema generado (`--config-json-schema`).

## Perfiles y políticas de canal

Los valores predeterminados por salida están en `conflation.interval_ms` y
`deduplication.ttl_ms`.

- Los intervalos y TTL positivos están limitados a **365 días**.
- Los intervalos **no positivos** seleccionan la publicación directa.
- Los TTL **no positivos** desactivan la deduplicación.

`deduplication_groups` de la propia salida mapea nombres a
`{ "ttl_ms", "round_ms", "restart_on_change", "max_members", "max_cache_bytes" }`.

| Campo | Efecto |
| --- | --- |
| `round_ms` | Redondea hacia abajo el timestamp Unix usado en el TTL del grupo, formando un bucket. `0` lo desactiva. El redondeo puede acortar un TTL activo hasta `round_ms`. |
| `restart_on_change` | Reancla el plazo después de un valor cambiado o de un nuevo miembro publicado. `false` mantiene la expiración fija. |
| `max_members` / `max_cache_bytes` | Límites del grupo (predeterminado 16.384 canales y 64 MiB; máximo 100.000 miembros y 256 MiB). Al llenarse, elimina el miembro menos usado recientemente. |

Para TTL positivos, `round_ms` no puede exceder `ttl_ms` (verificado por
`--check-config`). Los duplicados suprimidos y los valores intermedios consumidos
por la conflación **no** renuevan el plazo. Un miembro individual mayor que el
límite de bytes igualmente se publica, pero no se cachea, con un aviso.

### TTL de diferencia

`deduplication.ttl_ms` es un **TTL de diferencia**: solo se suprimen payloads
idénticos **consecutivos** (mismo canal mapeado y mismos bytes), y solo hasta que
la ventana expire.

- Un payload cambiado siempre se publica (sujeto a la conflación) y reemplaza el
  valor recordado — por lo tanto `A→B→A` publica los tres.
- Después de la ventana, el valor actual se envía de nuevo aunque no haya cambiado,
  actuando como refresh.

`deduplication.in_flight_suppression` (predeterminado `false`) trata un valor
enviado pero aún no confirmado como ya publicado para fines de supresión: las
repeticiones idénticas se descartan sin esperar el lote en vuelo. Un fallo
definitivo antes del envío deshace el valor recordado; un resultado ambiguo lo
mantiene.

`max_entries` y `max_cache_bytes` opcionales limitan el caché TTL por canal con
expulsión LRU (sin valor definido = ilimitado) y cuentan las expulsiones en
`deduplication_evictions_total`.

### Colas de entrada

Cada salida puede limitar su cola con `queue_max_messages` y `queue_max_bytes`
(sin valor = ilimitado) y `queue_overflow_policy`:

| Política | Comportamiento |
| --- | --- |
| `drop_newest` | Predeterminada. Descarta el mensaje que llega. |
| `drop_oldest` | Descarta el más antiguo de la cola. |
| `drop_by_age` | Descarta según `queue_max_age_ms`. |

Los mensajes descartados se cuentan por salida y globalmente en
`shed_messages_total`/`shed_payload_bytes_total`. El gauge de bytes de la cola es
`pending_queue_bytes` y la edad del pendiente más antiguo es `oldest_pending_age_ms`.

!!! warning "Pub/Sub no tiene replay"
    Cualquier política de descarte tira eventos fuera de forma explícita. La
    configuración predeterminada permanece ilimitada.

### Perfiles

`profiles` son sobrescrituras parciales: define `conflation.interval_ms`,
`deduplication.ttl_ms` o `deduplication.group` según sea necesario; lo que no se
defina hereda los valores predeterminados de la salida. Un perfil **no puede**
definir `deduplication.ttl_ms` y `deduplication.group` al mismo tiempo.

```json
"profiles": {
  "fast_no_dedup": {
    "conflation.interval_ms": 100,
    "deduplication.ttl_ms": 0
  },
  "shared_symbols": {
    "conflation.interval_ms": 250,
    "deduplication.group": "symbols"
  }
}
```

### Políticas de canal

Cada entrada ordenada de `channel_policies` tiene **exactamente un** selector:
`glob`, `prefix`, `suffix` o `"default": true` (coloca el default al final). La
entrada selecciona un perfil **o** sobrescrituras inline usando claves punteadas
literales.

Las políticas coinciden con el **canal final mapeado de la salida**, después de
los prefijos y sufijos de la suscripción y de la salida. Solo la **primera**
política que coincide se selecciona para cada canal. `--check-config` exige que la
regla default sea la última.

```json
"channel_policies": [
  { "glob": "db1:sub:test-feed:alpha:source", "profile": "fast_no_dedup" },
  { "prefix": "db1:sub:test-feed:beta:", "profile": "shared_symbols" },
  {
    "default": true,
    "conflation.interval_ms": 250,
    "deduplication.group": "symbols"
  }
]
```

Sin grupo, los TTL de deduplicación expiran por canal; los miembros de un grupo
comparten el plazo. Cada salida usa una cadencia de planificador por intervalo
positivo distinto; intervalos iguales comparten la cadencia.

Para probar carga de políticas localmente,
[`config.channel-policies-load.example.json`](https://github.com/rspadim/redis-conflated-pubsub/blob/main/config.channel-policies-load.example.json)
trae 16 reglas de tenant ordenadas y un default final.

## Filtros de canal

`input.filters` evalúa el canal informado por Redis después de que un
`SUBSCRIBE`/`PSUBSCRIBE` coincide, pero **antes** del `output_prefix`/`output_suffix`
de la suscripción. Cada salida puede tener sus propios `filters`, evaluados en el
canal mapeado por la suscripción (después del prefijo/sufijo de entrada, antes del
`channel_prefix`/`channel_suffix` de la salida).

Las reglas están ordenadas: **todas** se evalúan y la `action` (`accept` o `deny`)
de la **última que coincida** decide. Las reglas que no coinciden no hacen nada. Si
ninguna coincide, decide `filter_default` (predeterminado `accept`).

Cada regla tiene exactamente un selector:

| Selector | Semántica |
| --- | --- |
| `glob` | Soporta `*`, `?`, clases de caracteres y escape con barra invertida. |
| `regex` | Sintaxis Rust; busca en cualquier posición. Usa `^`/`$` para el canal completo. |
| `raw`, `single`, `string` | Alias de igualdad exacta. `*` es literal. |

```json
{
  "filters": [
    {"glob": "events:*", "action": "deny"},
    {"regex": "^events:public:[0-9]+$", "action": "accept"},
    {"raw": "events:public:42", "action": "deny"}
  ],
  "filter_default": "accept"
}
```

En el ejemplo, la regla literal final sobrescribe la regex anterior para ese canal
exacto.

### Cachés

Los selectores se compilan una vez en variantes internas y las cadenas originales
se descartan tras el setup. Las capacidades son independientes:

- `input.filter_cache_max_entries`
- `outputs.<name>.filter_cache_max_entries`
- `outputs.<name>.channel_policy_cache_max_entries`

Cada una tiene predeterminado **16.384**, acepta **0 para desactivar** ese caché y
está limitada a **100.000**. El límite aplica por caché; la memoria total crece con
el número de filtros y salidas. En la expulsión, solo sale el canal menos usado
recientemente. Una recarga aceptada recrea los workers y sus cachés.

## Conflación y tamaño de lote

Los intervalos positivos mantienen el último mensaje por canal mapeado y descargan
en transacciones `MULTI`/`EXEC`.

| Campo | Significado |
| --- | --- |
| `max_bytes_per_exec` | Objetivo de bytes por petición. |
| `max_commands_per_exec` | Límite de comandos por transacción. |
| `oversized_message_policy` | `send` (predeterminado), `truncate` o `drop`. |

## Nombre del cliente

`client_name` aparece en `CLIENT LIST`. El template predeterminado es
`ConflatedPS-{version}-{role}::{user}::{host}`:

- `{version}` — versión del programa.
- `{role}` — `i` para la entrada o `o-<output>` para cada salida.
- `{user}` — `USERDOMAIN\USERNAME` en Windows o `USER`/`LOGNAME` en los demás.
- `{host}` — `COMPUTERNAME`/`HOSTNAME`, con fallback al hostname del SO.

Los valores ausentes caen a `unknown`. Un template sin `{role}` recibe `-<role>`
añadido, y un `client_name` vacío desactiva el nombre.

Las conexiones de salida envían `CLIENT SETNAME` en cada reconexión. La entrada
Pub/Sub hace lo mismo en TCP simple: autentica y nombra el socket **antes** de
suscribirse, así que el nombre de la entrada exige una entrada sin TLS. Toda
conexión también envía `CLIENT SETINFO` (`lib-name`/`lib-ver`) cuando el servidor es
Redis 7.2 o más nuevo.

## Logging

```json
"logging": {
  "directory": "logs",
  "prefix": "redis-conflated-pubsub",
  "enabled": true,
  "level": "info",
  "retention_days": 14,
  "max_total_size_mb": 1024
}
```

`logging.prefix` nombra los archivos (`{prefix}.{YYYY-MM-DD}[.{n}]`) y delimita la
limpieza por retención, permitiendo que varios servicios compartan un directorio sin
tocar los archivos de los demás. `logging.enabled: false` apaga el log en archivo por
completo; el servicio sigue funcionando y `--check-config` aún valida los demás
campos.

## Alcance

JetStream (persistencia, ack, replay duradero) y Redis Streams están **fuera de
alcance** de forma permanente. El relay es un conflator de Pub/Sub efímero; añadir un
log duradero cambiaría su contrato.
