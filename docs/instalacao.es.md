# Instalación

Hay cuatro caminos. Esta página cubre el instalador de Windows y los paquetes de
Linux; para el binario directo mira [Inicio rápido](inicio-rapido.md) y para
contenedores mira [Docker y pruebas](docker.md).

## Windows — instalador

Descarga `ConflatedPubSubSetup-<versión>.exe` de la
[página de releases](https://github.com/rspadim/redis-conflated-pubsub/releases).
Requiere Windows 10 o superior y privilegios de administrador, porque registra un
servicio.

El asistente abre con un menú de seis opciones:

- configurar entrada Redis → salida Redis (predeterminada)
- entrada NATS → salida NATS
- entrada Redis → salida NATS
- entrada NATS → salida Redis
- usar un `config.json` que ya existe
- instalar solo el ejecutable, sin configurar

Las preguntas siguen a la elección: páginas de entrada y de salida (endpoint, base
de datos, nombre de la variable de entorno de la contraseña, patrón de suscripción o
subject, prefijo y sufijo del canal) y una de ajustes (intervalo de conflación, TTL
de deduplicación, puerto HTTP del estado, nivel de log). El prefijo de la salida ya
viene como `conflated:` porque el relay rechaza, con entrada y salida en el mismo
servidor Redis, una configuración sin prefijo o sufijo: su propia salida coincidiría
con la suscripción.

Al salir de la última página, el asistente arma el archivo y lo valida con el
`--check-config` del propio relay, en un borrador en el directorio temporal — antes
de escribir nada en `Program Files`. Si ya existe un archivo en el destino, se
conserva y se valida tal como está; si no existe, se genera a partir de las
respuestas. Después el instalador copia los archivos, escribe `config.json` con
rutas absolutas (un servicio de Windows se inicia con su directorio de trabajo en
`System32`), registra el servicio con reinicio automático en caso de fallo y, al
final, confirma si el relay **realmente arrancó** — `sc start` por sí solo no lo
probaría.

| Dónde | Qué |
| --- | --- |
| `C:\Program Files\Conflated Pub/Sub\` | ejecutable, `config.json`, los ejemplos y `scripts\` |
| `logs\` y `status.json` | dentro de esa carpeta, por rutas absolutas en `config.json` |
| Menú Inicio | validar la configuración, editar `config.json`, documentación, desinstalar |

Instalación silenciosa, para despliegue automatizado:

```powershell
.\ConflatedPubSubSetup-<versión>.exe /VERYSILENT /CONFIG=C:\relay\config.json /SERVICE=1 /SERVICESTART=auto
```

| Parámetro | Significado |
| --- | --- |
| `/CONFIG=<archivo>` | Usa una configuración que ya existe. Debe existir: si no, el instalador se niega a empezar y no se genera nada. |
| `/SERVICE=0\|1` | `1` (predeterminado) registra e inicia el servicio; `0` solo copia los archivos, para quien vaya a terminar a mano. |
| `/SERVICESTART=auto\|delayed-auto\|demand` | Tipo de inicio. Predeterminado `auto`, o `demand` cuando la tarea "iniciar automáticamente" está desmarcada en el asistente. |

Sin `/CONFIG` la configuración se genera con las respuestas predeterminadas del
asistente (entrada Redis a salida Redis); para cambiarlas, usa el asistente o edita
el `config.json` después.

Un valor inválido aborta antes de escribir nada, y **no se muestra nada** en modo
silencioso — un diálogo esperaría un clic que nunca llega. El código de salida
distinto de cero es la señal, y `/LOG=<archivo>` guarda el log del propio Inno.
Los fallos después de copiar los archivos (el relay rechazando la configuración, el
servicio sin arrancar) abortan la instalación igual, en lugar de mostrar un cuadro
de mensaje.

### Credenciales

El relay lee la contraseña de una variable de entorno, y un servicio de Windows
**no** ve las variables de usuario: una creada en tu perfil es invisible para el
servicio, que entonces sale al arrancar. El asistente lo resuelve — el campo de
contraseña guarda el valor en el **entorno del propio servicio**, una entrada
`Environment` en la clave del servicio que el gestor de servicios inyecta solo en
ese proceso, legible únicamente por administradores. Dejar el campo vacío está
soportado, pero el asistente avisa que la variable debe existir en ese entorno.

### Cuenta de servicio

El servicio corre como la cuenta virtual `NT SERVICE\redis-conflated-pubsub`, sin
contraseña, en lugar de `LocalSystem`: necesita la red y su propio directorio, nada
más. El instalador concede escritura solo en la carpeta de instalación. Si la
máquina rechaza la cuenta virtual, vuelve a `LocalSystem` y lo informa en pantalla.

### Desinstalar

El desinstalador detiene y elimina el servicio y pregunta si conserva el
`config.json` y los logs.

## Linux — paquetes

Cada release publica paquetes deb, rpm, apk y Arch, construidos a partir del binario
**musl estático**, así que no hay dependencias de runtime:

```sh
# Debian/Ubuntu
sudo dpkg -i redis-conflated-pubsub_<versión>_amd64.deb
# Fedora/RHEL
sudo rpm -i redis-conflated-pubsub-<versión>-1.x86_64.rpm
# Alpine
sudo apk add --allow-untrusted redis-conflated-pubsub_<versión>_x86_64.apk
```

El paquete **no** inicia el servicio por su cuenta: crea la cuenta de servicio,
recarga systemd e imprime el comando para que lo habilites.

```sh
sudo systemctl enable --now redis-conflated-pubsub.service
```

| Ruta | Qué |
| --- | --- |
| `/usr/bin/redis-conflated-pubsub` | el binario |
| `/etc/redis-conflated-pubsub/config.json` | configuración inicial; una actualización nunca la sobrescribe |
| `/etc/redis-conflated-pubsub/redis.env` | secretos de las variables `*_env`, modo `0600`, leído por systemd |
| `/usr/lib/systemd/system/redis-conflated-pubsub.service` | unidad systemd (Debian, RPM, Arch) |
| `/usr/lib/sysusers.d/redis-conflated-pubsub.conf` | cuenta de servicio dedicada, creada sin necesitar `shadow-utils` |
| `/etc/init.d/redis-conflated-pubsub` | script OpenRC (Alpine, que no tiene systemd) |

La unidad corre como el usuario `redis-conflated-pubsub`, con estado en
`/var/lib/redis-conflated-pubsub`, `EnvironmentFile` apuntando al `redis.env`,
`Restart=on-failure` y endurecimiento (`NoNewPrivileges`, `PrivateTmp`,
`ProtectSystem=strict`, `ProtectHome`). El log va al journal.

## Contenedor

La imagen del relay y el bundle con Redis/Valkey en un contenedor están en
[Docker y pruebas](docker.md).
