# Docker y pruebas

Ejecuta todos los comandos **desde la raíz del repositorio**: `Dockerfile` y los
`compose.*.yml` están allí y las rutas de los ejemplos son relativas a ella.

## Build y ejecución

Construye la imagen Linux y ejecútala con una configuración montada y un directorio
de estado escribible:

```sh
docker build --tag redis-conflated-pubsub:local .
cp config.example.json config.json
# Edita config.json para que las direcciones Redis sean alcanzables desde el contenedor.
mkdir -p state
docker run --rm \
  --workdir /state \
  --volume "$PWD/config.json:/config.json:ro" \
  --volume "$PWD/state:/state" \
  redis-conflated-pubsub:local \
  --config /config.json
```

Monta la config de runtime en `/config.json`; la imagen usa `scratch` y no contiene
shell ni archivo de configuración. Las rutas relativas de lock, estado y log se
resuelven desde `/state` en este ejemplo. Para exponer el endpoint de estado,
configura el bind como `0.0.0.0` y publica el puerto, por ejemplo `--publish 9090:9090`.

## Imagen única (bundle) con Redis/Valkey

El `Dockerfile.bundle` opcional contiene el relay, Redis y Valkey 9.1.2. En runtime,
`KV_ENGINE` selecciona la implementación (`redis` por defecto, o `valkey`); la engine
elegida ejecuta **dos** instancias: cruda en el puerto `6379` y conflada en el `6380`.
La config del relay se monta en `/etc/redis-conflated/config.json`;
`config.bundle.example.json` es el punto de partida. Son tres procesos en un
contenedor; el `Dockerfile` normal sigue siendo la imagen solo del relay.

```sh
cp config.bundle.example.json config.bundle.json
docker build -f Dockerfile.bundle -t redis-conflated-pubsub:bundle .
docker run -d --name redis-conflated-bundle --restart unless-stopped \
  -e KV_ENGINE=redis \
  -e REDIS_PASSWORD='replace-with-a-secret' \
  -p 6379:6379 -p 6380:6380 \
  -v "$PWD/config.bundle.json:/etc/redis-conflated/config.json:ro" \
  -v redis-raw-data:/data/raw \
  -v redis-conflated-data:/data/conflated \
  -v redis-relay-state:/state \
  redis-conflated-pubsub:bundle
```

Usa `KV_ENGINE=valkey` para Valkey. `REDIS_PASSWORD` es obligatorio;
alternativamente monta un secret y define `REDIS_PASSWORD_FILE`. Los puertos
publicados escuchan en todas las interfaces; para acceso solo del host usa
`-p 127.0.0.1:6379:6379 -p 127.0.0.1:6380:6380`. Usa volúmenes de datos separados al
cambiar de engine, a menos que hayas verificado la compatibilidad de los archivos.

## Pruebas de integración (Compose)

Cada prueba crea contenedores, redes y volúmenes con alcance de proyecto; no hay
nombres fijos de contenedor, IPs ni subredes. Ejecuta el `up` desde la raíz y el
`down` correspondiente después. Para ejecuciones en paralelo, pasa un único
`-p <proyecto>`.

| Stack | Qué verifica |
| --- | --- |
| `compose.test.yml` | Mapeo de canales, Pub/Sub entre databases, conflación, payloads binarios, prevención de eco y estado HTTP. |
| `compose.filters.test.yml` | Filtros ordenados de entrada/salida, mapeo de namespace y snapshot de caché de `GET /filters`. |
| `compose.oversize.test.yml` | Límite de 1 MiB del query-buffer de Redis y tratamiento de chunk fallido. |
| `compose.protocol.test.yml` | Transacciones por byte, políticas `send`/`truncate`/`drop` y `PUBLISH` directo sin transacción. |
| `compose.ttl.test.yml` | Supresión de duplicados, TTL individual y agrupado, selección de política, `SIGHUP` válido e inválido. |
| `compose.test.yml --profile fault` | Tratamiento de publish incierto/fallido vía proxy de fallo en Rust. |

```sh
docker compose -f compose.test.yml up --build --abort-on-container-exit --exit-code-from integration-test
docker compose -f compose.test.yml down --volumes --remove-orphans
```

Los drivers y proxies vienen de `examples/e2e.rs` (binario `redis-conflated-e2e`,
construido por `Dockerfile.e2e`), reemplazando los antiguos scripts Python.

## Hot path de extremo a extremo

El benchmark de hot path (`examples/hotpath_benchmark.rs`, imagen
`Dockerfile.benchmark`) ejecuta publishers y ambos suscriptores de salida en un
proceso Rust. La imagen predeterminada es Valkey 9.1.2.

```sh
docker compose -p hotpath-valkey -f compose.hotpath.test.yml up --build --abort-on-container-exit --exit-code-from hotpath-benchmark
docker compose -p hotpath-valkey -f compose.hotpath.test.yml down --volumes --remove-orphans
```

Para Redis, selecciona imagen y CLI explícitamente:

```sh
HOTPATH_KV_IMAGE=redis:7.4-alpine HOTPATH_KV_CLI=redis-cli \
docker compose -p hotpath-redis -f compose.hotpath.test.yml up --build --abort-on-container-exit --exit-code-from hotpath-benchmark
```

Variables de ajuste:

| Variable | Efecto | Predeterminado |
| --- | --- | --- |
| `HOTPATH_PUBLISHER_COUNT` | Número de publishers concurrentes. | — |
| `HOTPATH_MESSAGES_PER_PUBLISHER` | Mensajes por publisher. | — |
| `HOTPATH_SERIAL_MESSAGES` | Peticiones seriales medidas. | 250 |
| `HOTPATH_MODE` | `closed-loop` (espera cada ACK) u `open-loop` (pipelines no atómicos). | `closed-loop` |
| `HOTPATH_PIPELINE` | Comandos por pipeline en open-loop. | 64 |
| `HOTPATH_CONNECTIONS` | Conexiones por publisher en open-loop. | 1 |
| `HOTPATH_OUTPUTS` | Una o dos salidas. | 2 |
| `HOTPATH_MIN_NUMPAT` | Nivel de `PUBSUB NUMPAT` esperado antes de publicar (para servidores compartidos). | 1 |

El workload de política mixta (cerca de un tercio directo, un tercio conflación de
200 ms y un tercio 200 ms + TTL de 5 s) usa otro harness, seleccionado por
`HOTPATH_BENCHMARK_CMD='hotpath-mix-benchmark'`:

```sh
HOTPATH_CONFIG_FILE=./tests/docker-hotpath-mix-config.json \
HOTPATH_BENCHMARK_CMD='hotpath-mix-benchmark' \
docker compose -p hotpath-mixed -f compose.hotpath.test.yml up --build --abort-on-container-exit --exit-code-from hotpath-benchmark
```

## Loopback (sin Redis/Valkey)

Ejecuta el servicio contra un broker RESP2 mínimo implementado en Rust, sin
contenedores Redis:

```sh
docker compose -p loopback -f compose.loopback.test.yml up --build --abort-on-container-exit --exit-code-from loopback-benchmark
```

`LOOPBACK_MESSAGES` (predeterminado 1.000.000), `LOOPBACK_CHUNK`, `LOOPBACK_WARMUP` y
`LOOPBACK_TIMEOUT_S` ajustan la ejecución. Como el broker comparte CPU con el
servicio, trata los números como mediciones de servicio + broker.

## Captura y replay de feed

La imagen de benchmark también incluye `capture-feed` y `replay-feed`:

- `capture-feed` se suscribe en modo solo lectura (`CAPTURE_PATTERN`, predeterminado
  `*`) y graba **solo metadatos** por mensaje: timestamp relativo, canal y tamaño del
  payload. El contenido nunca se escribe. Emite NDJSON más un resumen JSON (tasa,
  serie por segundo, buckets de tamaño, top canales).
- `replay-feed` republica una captura en un Redis destino (`REPLAY_HOST`/`REPLAY_PORT`)
  preservando el timing de interllegada, con escala de tasa (`REPLAY_RATE_SCALE`),
  chunks en pipeline (`REPLAY_PIPELINE`), payloads sintéticos del tamaño grabado y
  repetición opcional por canal (`REPLAY_DUP_EVERY`) para ejercitar el TTL. Con
  `REPLAY_STATUS_URL` imprime los contadores de cola/publish antes y después.

Los archivos de captura quedan locales (`tests/.tmp-*` es ignorado por Git); solo
agregados anonimizados deben commitearse.

## CI y releases

Los workflows están en `.github/workflows/`. El `ci.yml` se ejecuta en pushes a
`main`, pull requests y dispatch manual: formato, Clippy con warnings como error,
pruebas unitarias y build de release en varias plataformas, además de imagen estática
Linux, E2Es Compose, harnesses de spike y validación del monitoreo. El `release.yml`
se dispara con tags `v*`, comprueba que el tag coincide con la versión del
`Cargo.toml` y publica los cuatro targets con sus `.sha256`.
