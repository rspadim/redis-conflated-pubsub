# Arquitectura

Este documento describe el runtime actual, los valores predeterminados de
configuración y las mediciones de referencia. Para el historial de cambios consulta
[Historial de cambios](changelog.md); la propuesta original del backend NATS está en
[`DESIGN-0.3.md`](https://github.com/rspadim/redis-conflated-pubsub/blob/main/DESIGN-0.3.md).

## Propósito

Reenviar mensajes Pub/Sub crudos desde **una** entrada hacia una o más salidas
nombradas. La entrada es Redis (`SUBSCRIBE`/`PSUBSCRIBE`) **o** Core NATS
(`nats_subject`); cada salida elige Redis o Core NATS de forma independiente.

Cada salida mapea direcciones, filtra mensajes, aplica conflación al último valor por
canal e intervalo, deduplica payloads idénticos dentro de un TTL y publica en su
propio endpoint. Los payloads siguen siendo bytes crudos; no hay spool en disco.

## Backends

La selección de backend es agnóstica de protocolo: `input` y cada salida llevan
exactamente una de dos claves opcionales, `redis` o `nats`. El JSON existente de
Redis no cambia.

- `--check-config` y el JSON Schema generado imponen la regla de exactamente un
  backend (el schema vía `oneOf` sobre `required: ["redis"]`/`["nats"]`).
- Cruces que solo hace la validación de runtime: tipo de suscripción compatible con
  el backend de entrada, regla de una familia de transporte por lista y archivos
  mTLS emparejados.

Redis usa `redis-rs`; Core NATS usa `async-nats` (features `websockets`, `nkeys`,
`ring`). Ambos backends comparten el camino de filtrado/cola/conflación/deduplicación
y difieren solo en el lector de entrada y el destino de salida.

## Mapa de módulos

| Ruta | Responsabilidad |
| --- | --- |
| `main.rs` | CLI, carga de config, `--check-config`, impresión del schema, señales. |
| `service.rs` | Fachada del ciclo de vida: compila la config, levanta workers, supervisa reload/shutdown. |
| `service/input.rs` | Lector Pub/Sub, exclusión de Sentinel/eco, filtros de entrada, coincidencia de suscripciones y fan-out. |
| `service/output.rs` | Bucle del worker de salida: intake, resolución de política, planificadores por intervalo, ventana directa en vuelo. |
| `service/output/flush.rs` | Liquidación de cola, descarga de bucket conflado, camino de publicación directa en lote. |
| `service/batch.rs` | Contabilidad de frames RESP, tamaño de lote, política de mensaje grande. |
| `service/publish.rs` | Conexión Redis, comandos de publish, clasificación de fallos; selección de `OutputSink`/`OutputConnection`. |
| `service/backend.rs` | Frontera de backend: `InputEnvelope` y las traits `InputSource`/`OutputSink`, más la constante del header de hop. |
| `service/nats/connect.rs` | Transforma un `NatsConfig` en cliente conectado; separa el `ConnectionPlan` sin secretos del `connect` que resuelve secretos. |
| `service/nats/input.rs` | Lector de entrada NATS: instala suscripciones, hace fan-in de los streams, descarta reentrada de bucle. |
| `service/nats/output.rs` | Sink de salida NATS: publish ordenado más un `flush`; estampa el header de hop. |
| `service/policy.rs`, `service/policy/glob.rs` | Selectores de canal compilados, caché de resolución de política, planificadores. |
| `service/dedup.rs` | Caché TTL por canal y grupos de deduplicación nombrados. |
| `service/filter.rs`, `service/channel_cache.rs` | Filtros ordenados y las LRUs limitadas detrás de filtros/políticas, con inspección opt-in. |
| `config.rs`, `config/schema.rs`, `config/validate.rs` | Config Serde, JSON Schema generado, validación semántica. |
| `status.rs`, `http_status.rs`, `logging.rs` | Snapshot de estado, endpoints HTTP opcionales, logs JSON con límite de tamaño. |

## Runtime

`service::run` compila la configuración una vez por start/reload y luego supervisa:

- una **tarea de entrada** que lee la conexión Pub/Sub;
- una **tarea de worker de salida** por salida configurada, cada una con su conexión
  Redis, caché de política, caché de deduplicación y planificadores de intervalo;
- escritor de estado y listener HTTP opcionales.

### Camino de entrada

El único lector de entrada procesa mensajes en el orden de llegada:

1. Descarta canales de notificación de Sentinel y ecos de salida cuando está
   configurado (ambos predeterminados activados; ambos específicos de Redis).
2. Hace coincidir el mensaje con las suscripciones configuradas.
3. Aplica el **filtro de entrada** al canal de origen.
4. Para cada salida: aplica el **filtro** de esa salida al canal mapeado por la
   suscripción (antes del prefijo/sufijo de la propia salida), arma el canal final y
   envía una copia a la cola de la salida.

Para entrada Redis el lector es `service/input.rs`; para NATS es
`service/nats/input.rs` (`read_nats_input`), que instala cada suscripción
`nats_subject`, hace el fan-in de los streams en round-robin, carga la identidad de
la suscripción que coincidió dentro del mapeo y descarta cualquier mensaje ya
estampado con el header de hop (guarda de bucle). La exclusión de Sentinel/eco es
solo de Redis; el camino NATS cuenta con el header de hop.

### Worker de salida

Cada worker es un consumidor único y ordenado. Resuelve la política de canal a medida
que los mensajes se aceptan y luego enruta por intervalo:

**Lane directa (`interval_ms <= 0`)** — cuando la deduplicación está desactivada
para el canal, los mensajes pasan por un deque de passthrough y se publican por una
ventana limitada en vuelo:

- Los límites son `max_commands_per_exec` comandos y el objetivo de bytes RESP
  `max_bytes_per_exec`.
- Los ítems directos ya encolados se envían juntos, ordenados, dentro de
  `MULTI`/`EXEC`; un ítem solo se publica inmediatamente.
- Las respuestas se recolectan en orden; las entradas encoladas siguen contadas como
  pendientes hasta que su comando liquida.
- Los mensajes cuyo canal o grupo de deduplicación ya tiene una entrada en vuelo
  esperan esos resultados primero, para que la supresión por TTL compare con el
  desenlace final de la publicación.

**Lane conflada (`interval_ms > 0`)** — los valores pendientes quedan en
`interval_ms → canal final → último mensaje`. Hay un timer por intervalo distinto
presente en la configuración; en cada tick el worker descarga **solo** ese bucket, en
orden de canal, en chunks `MULTI`/`EXEC` limitados. Sustituir un valor pendiente
cuenta como conflación; la deduplicación se verifica inmediatamente antes de enviar
cada chunk.

Los timers mantienen la cadencia configurada; un worker ocupado publicando no acumula
ticks perdidos.

### Publicación y fallos

- El batching usa los tamaños exactos de los frames RESP, incluido `MULTI`/`EXEC`.
- Los mensajes grandes siguen `oversized_message_policy`: `send` (predeterminado),
  `truncate` (sufijo del payload) o `drop`; la decisión se aplica por salida cuando
  el mensaje se acepta.
- **Los fallos no se reintentan.** Los fallos `NotSent` cuentan como descartados; los
  fallos posteriores al envío (respuesta de error, timeout) cuentan como inciertos y
  el lote se abandona. La conexión se descarta en el fallo y se restablece en la
  próxima publicación.
- Las publicaciones inciertas se recuerdan en el caché de deduplicación, porque
  pueden haber alcanzado suscriptores.

### Deduplicación

La deduplicación por canal compara el canal de salida exacto y los bytes del payload
de entrada crudo (incluidos los bytes que una política de truncate eliminó). Es un
TTL de diferencia: solo se suprimen payloads idénticos consecutivos, y solo hasta que
la ventana expire. Consulta [Configuración](configuracao.md#ttl-de-diferencia) para
las reglas completas, grupos y el modo `in_flight_suppression`.

### Límites de cola

La cola de intake de cada salida es ilimitada por defecto. `queue_max_messages`/
`queue_max_bytes` la limitan (los bytes cuentan nombre de canal más payload) con
`queue_overflow_policy`:

- `drop_newest` (predeterminado): el fan-out rechaza el mensaje nuevo cuando se
  excede un límite;
- `drop_oldest`: usa una cola compartida, expulsando la entrada más antigua para
  admitir la nueva;
- `drop_by_age`: el worker descarta mensajes más viejos que `queue_max_age_ms` en el
  intake (sin límite de ítem/byte).

Bloquear el lector **no** se ofrece porque Redis bufferizaría y después desconectaría
al suscriptor. Como Pub/Sub no tiene replay, cualquier descarte pierde eventos de
forma explícita.

### Reload y shutdown

En Unix, `SIGHUP` hace parse y valida el archivo nuevo antes de cambiar; un archivo
inválido deja la configuración en ejecución intacta. Una recarga aceptada drena las
colas pendientes y reinicia los workers de entrada/salida en proceso, lo que crea una
breve brecha en la entrada y pone a cero los contadores de estado. Los cambios de
logging aún exigen un reinicio completo; un cambio de `instance_lock.path` se
adquiere durante la recarga (el nuevo lock antes de liberar el antiguo) y la recarga
se rechaza si el nuevo lock no está disponible. El shutdown detiene la entrada, drena
los workers y descarga los buckets conflados restantes.

## Observabilidad

El snapshot de estado (`schema_version` 5) expone contadores globales y por salida
para entrada, publicadas/confladas/deduplicadas/descartadas/truncadas/shed y sus
bytes, expulsiones de deduplicación, errores de publish, operaciones inciertas,
reconexiones, mensajes suprimidos por la guarda de bucle (NATS) y pendientes en
mensajes/bytes/claves. Por salida también expone:

- **espera en la cola** — muestras/total/máximo del encolado en el fan-out hasta la
  aceptación por el worker (incluye resolución de política);
- **RTT de publish** — muestras/total/máximo alrededor del round trip de publicación;
- gauges de **bytes de cola pendiente** y **edad del pendiente más antiguo**.

Los eventos de conexión NATS se cuentan globalmente (`nats_client_connects_total`,
`nats_client_disconnects_total`) y los descartes por consumidor lento
(`nats_client_slow_consumers_total`). Como Core NATS reconecta internamente, un
failover de nodo **no** aparece en los contadores de reconnect; esos contadores son
best-effort (`async-nats` puede perder eventos en una tormenta de reconexión, lo que
subcuenta, nunca cuenta dos veces).

`GET /filters` expone nombres de canal completos y entradas de caché y queda
desactivado por defecto, porque revela nombres crudos; habilitarlo cambia las LRUs de
filtro/política por almacenamiento compartido protegido por mutex.

## Valores predeterminados de configuración

| Configuración | Predeterminado |
| --- | --- |
| `outputs.<name>.conflation.interval_ms` | obligatorio |
| `conflation.max_commands_per_exec` | `256` |
| `max_bytes_per_exec` / override por salida | `4 MiB` |
| `oversized_message_policy` | `send` |
| `outputs.<name>.deduplication.ttl_ms` | `0` (desactivado) |
| `deduplication.in_flight_suppression` | `false` |
| Límites de TTL por canal (`max_entries`/`max_cache_bytes`) | no definido (ilimitado) |
| Límites de grupo de deduplicación | `16384` miembros / `64 MiB` |
| Límites de cola (`queue_max_messages`/`queue_max_bytes`/`queue_max_age_ms`) | no definido (ilimitado), `drop_newest` |
| Capacidad de caché de filtro/política (cada una) | `16384`, máx `100000`, `0` desactiva |
| `exclude_output_echoes`, `exclude_sentinel_pubsub` | `true` |
| `logging.prefix` / `logging.enabled` | `redis-conflated-pubsub` / `true` |
| Template de `client_name` | `ConflatedPS-{version}-{role}::{user}::{host}` |
| Intervalo de actualización de estado | `1000 ms` |
| NATS `connect_timeout_ms` / `auth` / `ignore_discovered_servers` | `5000` / `none` / `false` |

Los intervalos y TTL positivos están limitados a 365 días; el `round_ms` de un grupo
no puede exceder un TTL de grupo positivo.

## Benchmarks

### Hot path de extremo a extremo

`examples/hotpath_benchmark.rs` ejecuta todos los publishers y ambos suscriptores de
salida en un único proceso Rust. Calienta 64 canales, mide 250 peticiones seriales y
luego ejecuta la ráfaga concurrente, reportando percentiles seriales, una
descomposición por fase (RTT del ACK de entrada, tiempo de servicio post-ACK y
percentiles de extremo a extremo por salida) y el pico de backlog de pendientes.

`HOTPATH_MODE=closed-loop` (predeterminado) espera el ack de cada `PUBLISH`;
`HOTPATH_MODE=open-loop` envía los mensajes en pipelines no atómicos de
`HOTPATH_PIPELINE` comandos (predeterminado 64) por `HOTPATH_CONNECTIONS` conexiones
por publisher, sin esperar acks individuales, para forzar presión sostenida. Comandos
y comandos de medición completos en [Docker y pruebas](docker.md).

### Captura y replay

`capture-feed` graba metadatos de Pub/Sub solo lectura (timestamp relativo, canal y
tamaño del payload; **nunca** el contenido) en NDJSON local más un resumen.
`replay-feed` republica una captura preservando el timing de interllegada grabado,
con payloads sintéticos del tamaño registrado y repetición opcional por canal para
ejercitar el TTL, para que formatos realistas de feed dirijan pruebas de carga sin
exportar datos de la aplicación.

Una captura de 30 minutos de un feed de market data (6.054.668 mensajes, 3.364 msg/s
de promedio, 5.508 msg/s de pico, 8.664 canales únicos) reproducida contra el stack
local con conflación de 200 ms, TTL de 5 s y la ventana recomendada 1024/lote 64:

| Replay | Entrada | Publicadas | Pendientes | Espera en cola promedio/máx | RTT de publish promedio/máx |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1× (30 min) | 6.053.786 | 2.292.837 (37,9%) | 0 | 0,071/13,5 ms | 0,492/11,7 ms |
| 4× | 6.053.786 | 1.371.471 (22,7%) | 0 | 0,080/5,1 ms | 0,443/6,9 ms |
| 8× | 6.053.786 | 1.127.642 (18,6%) | 0 | 0,095/8,2 ms | 0,462/8,2 ms |
| 4×, ventana/lote 256/256 | 6.053.786 | 1.357.014 (22,4%) | 0 | 0,104/11,6 ms | 0,975/8,0 ms |
| 4×, payloads repetidos | 6.053.786 | 979.805 (16,2%) | 0 | 0,090/9,2 ms | 0,448/8,0 ms |

La entrada excluye 882 mensajes `__sentinel__:hello` por ejecución. La conflación
redujo la publicación a 16–38% de la entrada según la tasa, con cero backlog y espera
de cola y RTT de publish sub-milisegundo en promedio.

### Loopback (sin Redis/Valkey)

`examples/loopback_benchmark.rs` implementa un broker RESP2 mínimo en Rust: acepta el
`PSUBSCRIBE` del servicio, alimenta mensajes tan rápido como el socket acepta y actúa
como el servidor de salida recibiendo `PUBLISH`/`MULTI`/`EXEC`, reportando percentiles
de extremo a extremo. `compose.loopback.test.yml` ejecuta esto sin contenedores Redis,
aislando el servicio y el protocolo.

### Comparación de transporte y e2e NATS

`spikes/compare` es un crate Rust separado que mide los caminos de transporte crudos
(`async-nats` vs `redis-rs`) y dirige el **binario real del relay** de extremo a
extremo. Su publisher y subscriber corren como **procesos separados** para que el
buffering del publisher no infle la latencia medida; un harness de proceso único
produce números dominados por cola y no debe usarse para afirmaciones de latencia.

### Mediciones de referencia

Ejecuciones locales en Docker/WSL con nombres sintéticos de 64 canales, cachés locales
al worker y servidores en modo Pub/Sub puro. Trátalas como indicativas, no como
comparaciones controladas de engine.

Sweep de ventana (`max_commands_per_exec`, una salida, 640k, Redis 7.4.11, pipeline
128): 256 → 99,8k msg/s; 1024 → 85,6k msg/s; 4096 → 93,8k msg/s. La tasa no escala
con la ventana en vuelo mientras el RTT de publish crece proporcionalmente, así que
el plateau es el procesamiento de `PUBLISH` en el servidor, no la ventana del cliente.

Grilla 2D tras el desacoplamiento (una salida, 640k, open-loop, mediana de 3 reps):

| Lote | Ventana | Tasa (msg/s) | Espera en cola promedio | RTT de publish promedio |
| ---: | ---: | ---: | ---: | ---: |
| 64 | 256 | 116,1k | 1566 ms | 2,0 ms |
| 64 | 1024 | 129,3k | 848 ms | 6,9 ms |
| 64 | 4096 | 114,8k | 800 ms | 30,8 ms |
| 256 | 256 | 130,5k | 1225 ms | 1,6 ms |
| 256 | 1024 | 129,3k | 898 ms | 6,6 ms |
| 256 | 4096 | 117,3k | 655 ms | 27,5 ms |
| 1024 | 256 | 130,6k | 1396 ms | 1,7 ms |
| 1024 | 1024 | 123,0k | 979 ms | 7,0 ms |
| 1024 | 4096 | 119,8k | 712 ms | 21,3 ms |

La ventana ayuda hasta 1024; en 4096 el RTT explota porque muchos `MULTI`/`EXEC`
concurrentes se encolan en el servidor de salida. La mejor región es ventana 1024 con
lote 64–256. La varianza entre ejecuciones es alta, así que diferencias de tasa por
debajo de ~10% son ruido; las señales de latencia son las robustas.

Tras las optimizaciones (payload `Arc<[u8]>`, ventana en vuelo desacoplada, fairness
de flush por turno), ejecuciones con Redis 7.4.11: una salida 640k — **139,4k msg/s**
con la ventana predeterminada y **149,1k** con `max_in_flight_commands: 1024`; dos
salidas 640k — **77,6k msg/s** (~155k publishes/s), igual con un servidor o dos;
loopback (sin Redis) 1M — **80k msg/s**. Antes de esos cambios las mismas ejecuciones
medían 99,8k (una), 50,1k (dos) y 57,6k (endpoints separados).

Observaciones:

- El término que crece en 640k es la **espera en la cola de intake** (fan-out hasta la
  aceptación por el worker): el lector único de entrada más el intake por worker es
  la próxima palanca de concurrencia.
- Separación de endpoints: con una salida el escenario está limitado por el servidor
  cerca de 90–100k publishes/s; con dos salidas el plateau mezcla procesamiento del
  servidor y un techo de cliente/intake en torno a 50–58k entradas/s.
- La varianza entre ejecuciones es alta: la misma configuración de una salida midió
  139,4k, mediana de 129k en la grilla y 106,9k en la verificación, con muestras
  crudas de 91k–138k. Prefiere las señales de latencia (`queue_wait`, RTT de publish).

## Suite de pruebas

- Las pruebas unitarias y de integración están en `tests/unit/`, incluidas como
  módulos del crate; `cargo test --locked` pasa 187 pruebas en Windows y 198 en Unix
  (donde compilan las pruebas de reload `#[cfg(unix)]`), y lista 8 benchmarks manuales
  ignorados.
- E2Es Compose (ver [Docker y pruebas](docker.md)): integración Pub/Sub, mensajes
  grandes, protocolo RESP/políticas, TTL/grupos/SIGHUP, latencia de hot path y
  tratamiento de fallos.
- `cargo fmt --all --check` y
  `cargo clippy --all-targets --locked -- -D warnings` deben pasar limpios.

Benchmarks unitarios manuales: ejecútalos con
`cargo test --release --locked <nombre> -- --ignored --nocapture`.

- `filter_cache_load_benchmark`, `channel_policy_cache_load_benchmark`
  (`tests/unit/service/filter_load.rs`) — 64 reglas, conjunto caliente de 8.192
  canales, churn de 24.576 canales, más el replay del fixture anonimizado 100×.
  Misses con 64 reglas: ~30–41 µs (filtros) y ~28–39 µs (políticas); hits
  ~80–120 ns.
- `direct_lane_load_benchmark` (`tests/unit/service/direct_lane_load.rs`) — drena
  100.000 mensajes directos encolados con latencia simulada por comando
  (predeterminado 50 µs) en batch caps 1/32/256 (836 / 11.443 / 18.187 msg/s) y
  garantiza el orden FIFO.
