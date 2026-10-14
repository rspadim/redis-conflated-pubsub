# Historial de cambios

El archivo canónico es el [`CHANGELOG.md`](https://github.com/rspadim/redis-conflated-pubsub/blob/main/CHANGELOG.md)
en la raíz del repositorio. Abajo hay un resumen de las versiones.

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
