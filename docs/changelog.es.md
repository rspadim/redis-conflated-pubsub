# Historial de cambios

El archivo canónico es el [`CHANGELOG.md`](https://github.com/rspadim/redis-conflated-pubsub/blob/main/CHANGELOG.md)
en la raíz del repositorio. Abajo hay un resumen de las versiones.

## 0.4.0 — 2026-10-21

- The installer's "Test connection" button now runs the relay's own `--test-connection` against a draft of the configuration the wizard would write, instead of a PowerShell probe of the wire protocol. The new flag validates the configuration, then connects to every configured backend (Redis: a ladder of reads - `DBSIZE`, `PUBSUB CHANNELS`, `PING`; NATS: the full connection plan) and exits non-zero when any of them did not answer. It reuses the runtime's client configuration, so the check cannot disagree with what the service does at startup - including TLS, ACL, cluster and the NATS credential styles. Read-only: it connects and drops, publishing nothing and touching no key.
- The probe climbs a ladder of READ commands and treats any server reply as "connected". `PING` was the verdict and a Redis with `stop-writes-on-bgsave-error yes` answers it with `-MISCONF` while serving every read, so a usable server was reported unreachable. The ladder covers the ACL categories a restricted user may be granted separately (`@read`, `@pubsub`, `@connection`), and the verdict is "connected and authenticated": a server reply (MISCONF, NOPERM, READONLY, ERR) means the connection works, while an authentication failure is reported as such rather than as "unreachable". It never writes or publishes.
- The probe is available outside the installer too: `redis-conflated-pubsub --config config.json --test-connection` reports one line per backend (`input redis  host:port  OK`, or `FAILED: <reason>`), which makes it usable from a deployment script or a support session.
- The wizard names the specific reason on failure (rejected credentials, timeout, refused) by reading the relay's own report instead of a bare "failed". Localized in all 10 wizard languages.
- Removed `installer/test-connection.ps1` and the messages only it used: a wire-level handshake proved reachability but not credentials, which is the case that actually fails. The endpoint pages keep their "Test connection" button, now backed by the real client.

## 0.3.1 — 2026-10-14

- El relay ahora implementa el protocolo de servicio de Windows: el administrador
  de servicios lo inicia, detiene y reinicia en caso de fallo como cualquier otro
  servicio. Antes el instalador registraba un ejecutable de consola y toda
  solicitud de inicio fallaba con el error 1053 — el servicio nunca arrancaba. En
  las demás plataformas no cambia nada.
- Instalación silenciosa parametrizable: `/CONFIG=<archivo>` usa una configuración
  que ya existe, `/SERVICE=0|1` decide si se registra el servicio y
  `/SERVICESTART=auto|delayed-auto|demand` define el tipo de inicio.
- Los fallos posteriores a la copia de los archivos devuelven el código de salida
  **9** y quedan registrados en `{app}\install-report.txt`, una línea por paso; no
  se muestra nada en modo silencioso. Inno Setup devuelve 0 cuando falla un
  script, así que ese código es la única forma de fallar en voz alta.
- El instalador confirma que el relay arrancó, no solo que el servicio aceptó la
  solicitud, y exige un `status.json` escrito después de empezar la verificación.
- Corregido: `sc.exe` no aceptaba la ruta de instalación predeterminada (error
  1639) y el estado del servicio se leía de un texto que cambia con el idioma
  (`ESTADO`), lo que hacía parecer `unknown` un servicio en ejecución.
- Corregido: en modo silencioso la configuración generada salía NATS→NATS en vez
  de Redis→Redis, una actualización silenciosa omitía el servicio, y el motivo por
  el que el relay rechazó una configuración se perdía.

## 0.3.0 — 2026-10-14

- Selección de backend neutra de protocolo: `input` y cada salida usan exactamente
  uno de `redis` o `nats`.
- Backend Core NATS de entrada y salida (Pub/Sub únicamente, no JetStream): `servers`
  con regla de una familia de transporte por lista (`nats://`, `tls://`, `ws://`,
  `wss://`), `name`/`connect_timeout_ms`, `ignore_discovered_servers` opcional y
  suscripciones `nats_subject` con wildcards nativos `*`/`>`.
- Variantes de autenticación NATS: `none` (predeterminado), `user_password`, `token`,
  `nkey` y `credentials_file`, con TLS/mTLS bajo `tls`. Secretos siempre por variable
  de entorno o ruta de archivo, nunca inline en el JSON.
- Guarda de bucle NATS→NATS vía header `ConflatedPS-Hop`, con contador
  `loop_suppressed_messages_total`. Los headers de aplicación no se reenvían.
- Política de mensaje grande aplicada al `max_payload` anunciado por el servidor NATS
  (menos los bytes del header de hop), con preflight de cada chunk.
- Ciclo de vida de conexión NATS en el estado (`nats_client_connects_total`,
  `nats_client_disconnects_total`, `nats_client_slow_consumers_total`) y fallo rápido
  cuando falta un secreto referenciado.

## 0.2.2 — 2026-10-08

- La entrada Pub/Sub envía `CLIENT SETNAME` antes de suscribirse (TCP simple), así
  que `CLIENT LIST` muestra el nombre `-i` también en el servidor de entrada. Las
  entradas TLS siguen sin nombre.

## 0.2.1 — 2026-10-08

- `client_name` `{host}` cae al hostname del SO cuando `COMPUTERNAME`/`HOSTNAME` no
  están definidos, resolviendo la máquina en lugar de `unknown` bajo systemd.
- `deduplication.ttl_ms` documentado como TTL de diferencia.

## 0.2.0 — 2026-10-08

- El valor predeterminado de `deduplication.ttl_ms` cambió de 5000 a **0**: la
  deduplicación solo se activa explícitamente por salida, perfil o política de canal.
- Filtros ordenados de entrada/salida con selectores `glob`, `regex` y literal.
- Cachés LRU limitadas e independientes para filtros de entrada, filtros de cada
  salida y políticas de canal; el endpoint opt-in `GET /filters` expone las entradas.
- Buckets de canales conflados agrupados por intervalo, cada timer descargando solo
  su propio bucket; passthrough directo con ventana en vuelo limitada.
- Métricas de espera en la cola y RTT de publish por salida.
- Benchmarks de hot path y de política mixta migrados a Rust.
- Imagen única con Redis/Valkey seleccionable por `KV_ENGINE`.
- Proxies y drivers E2E migrados al binario Rust `redis-conflated-e2e`.

## 0.1.4 — 2026-10-06

- Recarga de configuración por `SIGHUP` en Unix. El archivo nuevo se valida antes del
  cambio; una configuración inválida mantiene el runtime actual. Los cambios de
  logging y de lock aún exigen un reinicio completo.

## 0.1.3 — 2026-10-06

- Perfiles por salida con sobrescrituras parciales de conflación/TTL y políticas de
  canal ordenadas con regla `default` final obligatoria.
- Grupos TTL nombrados con expiración compartida redondeada por reloj y cachés
  limitadas. Duraciones positivas limitadas a 365 días.

## 0.1.2 — 2026-10-06

- Supresión de duplicados por salida con `deduplication.ttl_ms` (por entonces
  predeterminado 5000 ms), con caché solo en memoria.
- Contadores globales y por salida de deduplicación y soporte en los templates
  Zabbix.

## 0.1.1 — 2026-10-06

- Objetivos de tamaño de petición RESP y división de descargas confladas por bytes
  exactos y conteo de comandos.
- Políticas de mensaje grande: `send`, `truncate` y `drop`.
- Exclusiones explícitas de eco de salida y de Pub/Sub del Sentinel.
- Los fallos no se reintentan: el servicio cuenta mensajes fallidos/inciertos, limita
  los logs de error y continúa.

## 0.1.0 — 2026-10-05

- Primera versión: relay de Pub/Sub Redis con conflación y mapeo de canales,
  conflación transaccional y builds multiplataforma.
