# Operación y monitoreo

## systemd (Linux)

Instala el template de unit y habilita el servicio:

```sh
sudo install -m 0644 redis-conflated-pubsub.service.example /etc/systemd/system/redis-conflated-pubsub.service
sudo systemctl daemon-reload
sudo systemctl enable --now redis-conflated-pubsub.service
sudo systemctl status redis-conflated-pubsub.service
```

El archivo `redis-conflated-pubsub.service.example` está en la raíz del repositorio;
ajusta `User`, `ExecStart` y las rutas antes de instalar en producción.

## Paquetes Debian/Ubuntu (.deb), RPM, Alpine (.apk) y Arch

Cada release publica paquetes para las cuatro familias, todos generados con
[nFPM](https://nfpm.goreleaser.com/) a partir del mismo binario estático (musl):

```sh
sudo apt install ./redis-conflated-pubsub_0.3.0-1_amd64.deb                     # Debian/Ubuntu
sudo dnf install ./redis-conflated-pubsub-0.3.0-1.x86_64.rpm                    # RHEL/Fedora/SUSE
sudo apk add --allow-untrusted ./redis-conflated-pubsub_0.3.0-r1_x86_64.apk     # Alpine
sudo pacman -U redis-conflated-pubsub-0.3.0-1-x86_64.pkg.tar.zst                # Arch
```

Los paquetes no están firmados, como el resto de los artefactos del release, así
que cada gestor pide confirmación. Después, edita la configuración e inicia el
servicio:

```sh
sudo editor /etc/redis-conflated-pubsub/config.json
sudo systemctl enable --now redis-conflated-pubsub.service   # Debian, RPM, Arch
sudo rc-update add redis-conflated-pubsub default            # Alpine (OpenRC)
sudo rc-service redis-conflated-pubsub start
```

Ningún paquete declara dependencias (el binario es estático) y todos instalan
`/usr/bin/redis-conflated-pubsub`, la cuenta de servicio definida vía
`sysusers.d`, `/etc/redis-conflated-pubsub/config.json` y
`/etc/redis-conflated-pubsub/redis.env` (modo `0600`, para los campos `*_env`).
Las familias con systemd instalan la unit en
`/usr/lib/systemd/system/redis-conflated-pubsub.service`; en Alpine un script
OpenRC ocupa `/etc/init.d` en su lugar. Los logs y el estado quedan en
`/var/log/redis-conflated-pubsub` y `/var/lib/redis-conflated-pubsub`. El
servicio no se habilita ni se inicia al instalar, y un `purge` en Debian elimina
la cuenta, el estado y la configuración.

## Recargar la configuración en Unix

Envía `SIGHUP` para recargar el JSON configurado sin reiniciar la unit:

```sh
sudo systemctl kill --signal=HUP --kill-whom=main redis-conflated-pubsub.service
```

El servicio valida el archivo **antes** de cambiar; una configuración inválida se
rechaza y el runtime actual sigue funcionando. Una recarga válida drena las colas
pendientes y reinicia los workers de entrada/salida, así que hay una breve brecha en
la entrada Pub/Sub y se pueden perder mensajes (Pub/Sub no tiene replay). Los
contadores de estado y el uptime se reinician a cero.

Un `instance_lock.path` cambiado se adquiere durante la recarga **antes** de liberar
el antiguo, y la recarga se rechaza si el nuevo lock no está disponible. Los cambios
en `logging` o el reemplazo del propio ejecutable aún exigen un reinicio completo.

## Estado y métricas

Habilita el endpoint HTTP de estado:

```json
"status": {
  "http": {
    "bind": "127.0.0.1",
    "port": 9090
  }
}
```

`GET /` sirve las métricas por salida. `GET /filters` expone los nombres de canal
completos y las entradas actuales de los cachés de filtro/política; queda
**desactivado por defecto** y solo debe habilitarse con
`status.http.filters_endpoint_enabled: true` en una interfaz de confianza/privada,
porque los nombres de canal pueden ser sensibles.

```sh
curl http://127.0.0.1:9090/
```

Alternativamente, `status.path` graba un snapshot JSON atómico en archivo, útil
cuando no se quiere exponer un puerto.

## Monitoreo Zabbix

El colector Python 3 lee el estado por HTTP y envía valores trapper con
`zabbix_sender`, usando solo la biblioteca estándar.

### Requisitos

- Servicio en ejecución con el listener HTTP de estado habilitado.
- Python 3 y `zabbix_sender` en la máquina del colector.
- Acceso de red del colector al estado y al servidor/proxy Zabbix (puerto
  predeterminado `10051`).

### Configurar y probar

```sh
cp monitoring/config.example.json monitoring/config.json
curl --fail http://127.0.0.1:9090/
python3 monitoring/zabbix_sender.py --config monitoring/config.json
```

Define `base_url` para la raíz del endpoint de estado, `zabbix_server`/`zabbix_port`
para el servidor o proxy, y `host` para el **nombre técnico exacto** del host en
Zabbix. `key_prefix` debe coincidir con el template importado (predeterminado
`redis_conflated_pubsub.`). Usa `sender_path` con ruta absoluta si `zabbix_sender` no
está en el `PATH`.

Con una respuesta válida, el colector envía **34 valores** en un lote. Si el endpoint
no está disponible o la respuesta es inválida, envía solo `health=0` y deja los otros
valores como están.

Si el host ya tiene un agente Zabbix, `zabbix_config` permite heredar `ServerActive`,
`Hostname` y TLS del archivo del agente (normalmente
`/etc/zabbix/zabbix_agentd.conf`). `host` sigue siendo obligatorio y debe ser el
`Hostname` del agente.

### Importar el template

Importa **un** XML según la versión del servidor:

| Versión Zabbix | Archivo | Menú |
| --- | --- | --- |
| 5.0 | `monitoring/zabbix_template_5.0.xml` | Configuration → Templates → Import |
| 7.0 | `monitoring/zabbix_template_7.0.xml` | Data collection → Templates → Import |

Crea o selecciona un host, vincula **Template Redis Conflated Pub/Sub** y define el
`host` del colector con el nombre técnico del host. Los items son trapper: no se
necesita un agente Zabbix para que el colector funcione. Verifica `health = 1` y
`schema_version = 5`.

### Valores recolectados

| Campo del estado | Sufijo de la clave | Significado |
| --- | --- | --- |
| derivado de `state` | `health` | `1` cuando `state` es `running`, si no `0` |
| `schema_version` | `schema_version` | Entero (permanece `5`) |
| `state` | `state` | Estado del servicio |
| `updated_at`, `started_at`, `last_input_at`, `last_flush_at` | mismo nombre | Timestamps; nulo se convierte en `never` |
| `last_error` | `last_error` | Texto; nulo se convierte en `none` |
| `uptime_seconds` | `uptime_seconds` | Segundos |
| `input_messages_total`, `input_payload_bytes_total` | mismo nombre | Conteo y bytes de entrada |
| `output_batches_total`, `output_messages_total`, `output_payload_bytes_total` | mismo nombre | Lotes, mensajes y bytes publicados |
| `conflated_messages_total`, `conflated_payload_bytes_total` | mismo nombre | Mensajes superados y bytes |
| `deduplicated_messages_total`, `deduplicated_payload_bytes_total` | mismo nombre | Suprimidos por TTL |
| `excluded_messages_total` | `excluded_messages_total` | Mensajes excluidos |
| `dropped_messages_total`, `dropped_payload_bytes_total` | mismo nombre | Descartes por política y fallos pre-envío definitivos |
| `truncated_messages_total`, `truncated_payload_bytes_total` | mismo nombre | Mensajes truncados y bytes removidos |
| `publish_errors_total`, `publish_error_messages_total` | mismo nombre | Fallos de publish y mensajes afectados |
| `uncertain_transactions_total`, `uncertain_messages_total` | mismo nombre | Transacciones y mensajes inciertos |
| `input_reconnects_total`, `output_reconnects_total` | mismo nombre | Reconexiones Redis |
| `nats_client_connects_total`, `nats_client_disconnects_total` | mismo nombre | Conexiones NATS (Core NATS reconecta internamente) |
| `loop_suppressed_messages_total` | `loop_suppressed_messages_total` | Descartes de la guarda de bucle NATS |
| `pending_keys` | `pending_keys` | Pendientes |
| `outputs` | `outputs_json` | JSON compacto con las métricas por salida |

Los contadores de deduplicación son aditivos; `schema_version` permanece `5`. El JSON
por salida incluye métricas de publicación, conflación y deduplicación por destino,
con la misma semántica de bytes de payload crudo del contador global. El texto de
error está limitado a 2.048 caracteres y los saltos de línea se normalizan.

### Ejecutar en agenda

Por ejemplo, una vez por minuto con cron:

```cron
* * * * * /usr/bin/python3 /opt/redis-conflated-pubsub/monitoring/zabbix_sender.py --config /etc/redis-conflated-pubsub/monitoring.json >> /var/log/redis-conflated-pubsub-monitoring.log 2>&1
```

Usa rutas absolutas: cron tiene `PATH` y directorio de trabajo limitados. Cada
ejecución envía el snapshot de estado más reciente. El agente Zabbix no ejecuta el
colector; el `zabbix_sender` conecta directo al servidor o proxy.

## Solución de problemas

- **No llega nada a las salidas** — confirma la coincidencia de suscripción y el
  mapeo de canal; en Redis, `PSUBSCRIBE` usa glob y distingue mayúsculas.
- **Publish fallando con `0` suscriptores en NATS** — esto es esperado en Core NATS.
  Lee `publish_errors_total` y `uncertain_messages_total`.
- **Los mensajes desaparecen en picos** — verifica `shed_messages_total`,
  `pending_queue_bytes` y `oldest_pending_age_ms`. La cola predeterminada es
  ilimitada, así que el descarte solo ocurre con límites configurados.
- **`--check-config` falla** — el mensaje indica la ruta del campo; valida `default`
  al final en `channel_policies`, exactamente un backend por lado y archivos mTLS
  emparejados.
