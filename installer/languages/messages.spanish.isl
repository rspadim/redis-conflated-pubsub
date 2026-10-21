; Spanish translation for the installer wizard.
; Keys not listed here fall back to messages.english.isl.
;
[CustomMessages]
GroupService=Servicio de Windows
GroupShortcuts=Accesos directos
TaskServiceDesc=Instalar e iniciar el servicio de Windows (recomendado)
TaskServiceAutoDesc=Iniciar el servicio automáticamente con Windows
TaskDesktopIconDesc=Crear un acceso directo en el escritorio para validar la configuración
PageConfigTitle=Archivo de configuración
PageConfigSubtitle=Dónde se escribirá config.json
PageConfigDesc=Si el archivo todavía no existe, se genera a partir de tus respuestas. Si ya existe, se conserva tal como está: apunta a un archivo existente para usar tu propia configuración.

; --- paginas del asistente de configuracion ---
PageInputTitle=Entrada
PageInputDesc=De dónde vienen los mensajes. El endpoint de Redis se escribe como host:puerto; usa el esquema rediss:// para habilitar TLS (rediss://redis.ejemplo.net:6380). El patrón usa la semántica glob de Redis, así que feed:* coincide con todo canal que empiece por feed:.
InEndpointLabel=Endpoint de Redis (host:puerto)
InDatabaseLabel=Base de datos
InPasswordEnvLabel=Nombre de la variable de entorno con la contraseña (opcional)
InPatternLabel=Patrón de suscripción
PageOutputTitle=Salida
PageOutputDesc=Dónde se publican los mensajes. El prefijo y el sufijo se colocan alrededor del nombre del canal. También evitan que el relay lea su propia salida: cuando la entrada y la salida son el mismo servidor Redis, al menos uno de ellos es obligatorio, si no el relay se niega a arrancar con un error de eco.
OutEndpointLabel=Endpoint de Redis (host:puerto)
OutDatabaseLabel=Base de datos
OutPasswordEnvLabel=Nombre de la variable de entorno con la contraseña (opcional)
OutChannelPrefixLabel=Prefijo del canal
OutChannelSuffixLabel=Sufijo del canal
PageTuningTitle=Ajustes
PageTuningDesc=Cuánto comprime el relay. Con un intervalo de conflación mayor que cero solo se publica el último valor por canal dentro de cada ventana; cero publica cada mensaje de inmediato. Con un TTL de deduplicación mayor que cero se suprimen los payloads idénticos consecutivos hasta que la ventana expire; cero lo desactiva.
TunIntervalLabel=Intervalo de conflación (ms, 0 publica directo)
TunTtlLabel=TTL de deduplicación (ms, 0 desactiva)
TunStatusLabel=Puerto HTTP de estado (0 desactiva el listener)
TunLogLevelLabel=Nivel de log (error, warn, info, debug, trace)
BadNumber=%1 debe ser un número entero de cero o más.
BadLogLevel=Nivel de log desconocido. Usa error, warn, info, debug o trace.
GenerateFailed=No se pudo generar la configuración:%n%n%1
ConfigGenerated=La configuración es válida y se escribirá en %1.

; --- menu de modo y paginas NATS ---
PageModeTitle=¿Qué debe hacer el asistente?
PageModeDesc=Elige cómo configurar el relay. "Usar un archivo existente" omite todas las preguntas y toma un config.json que ya tienes; las otras cuatro opciones generan uno para esa combinación de backends, preguntando solo lo que esa combinación necesita.
ModeExisting=Usar un config.json que ya existe
ModeRedisRedis=Configurar entrada Redis a salida Redis
ModeNatsNats=Configurar entrada NATS a salida NATS
ModeRedisNats=Configurar entrada Redis a salida NATS
ModeNatsRedis=Configurar entrada NATS a salida Redis
PageNatsInTitle=Entrada NATS
PageNatsInDesc=Servidores semilla separados por comas o espacios. Cada URL necesita esquema: nats:// para TCP, tls:// para TLS, ws:// o wss:// para WebSocket, y una misma lista no puede mezclar WebSocket con los demás. El subject usa los comodines de NATS: * coincide exactamente con un token, y > coincide con el resto y debe ser el último.
PageNatsOutTitle=Salida NATS
PageNatsOutDesc=Las mismas reglas de servidores y autenticación que la entrada. El prefijo y el sufijo se colocan alrededor del subject, y la cadena final debe ser un subject concreto: los comodines se rechazan.
NatsServersLabel=Servidores NATS (separados por comas)
NatsAuthLabel=Autenticación (none, user_password, token, credentials_file)
NatsUsernameLabel=Usuario (user_password)
NatsPasswordEnvLabel=Variable de entorno de la contraseña (user_password)
NatsTokenEnvLabel=Variable de entorno del token (token)
NatsCredentialsLabel=Ruta del archivo .creds (credentials_file)
NatsSubjectLabel=Subject de la suscripción
BadAuthType=Tipo de autenticación desconocido. Usa none, user_password, token o credentials_file.
SelectorMissing=Indica el patrón o el subject de esta suscripción. El relay rechaza un selector vacío.
SecretValueLabel=Valor de la contraseña (se guarda en el entorno del propio servicio; déjalo vacío para definir la variable tú mismo)
SecretsStored=Guardado en el entorno del servicio: %1
SecretsFailed=No se pudo actualizar el entorno del servicio:%n%n%1
SecretNotSetWarning=No se informó ningún valor para %1, así que el instalador no la guardará. El relay lee sus credenciales del entorno del propio servicio, donde esa variable todavía no existe: el servicio saldrá al arrancar hasta que la definas. ¿Continuar?
ModeInstallOnly=Instalar solo el ejecutable (sin configurar)
FinishedNoConfig=No se creó ninguna configuración y el servicio no se instaló: parte de config.example.json o config.nats.example.json y ejecuta scripts\service.ps1 en la carpeta de instalación cuando estés listo.
BadServiceParam=/SERVICE acepta solo 0 o 1, pero llegó "%1".
BadStartupParam=/SERVICESTART acepta solo auto, delayed-auto o demand, pero llegó "%1".
ConfigRejected=El relay rechazó esta configuración:%n%n%1
ServiceStopFailed=El servicio del relay no se pudo detener:%n%n%1%n%nDetenlo y vuelve a ejecutar el instalador.
ConfigPathLabel=Archivo de configuración
ValidateOk=La configuración es válida.
ValidateFailed=El relay rechazó esta configuración:%n%n%1%n%n¿Instalar de todos modos? El servicio no arrancará hasta que se corrija el archivo.
ValidateMissing=Elige la ruta de un archivo de configuración.
ConfigMissing=El archivo de configuración no existe:%n%n%1
ConfigCreated=config.json se escribió a partir de config.example.json con rutas absolutas.
ConfigKept=Se conservó el archivo de configuración existente.
ServiceInstallFailed=No se pudo instalar el servicio de Windows:%n%n%1
ServiceStartFailed=El servicio se instaló pero no se pudo iniciar:%n%n%1
ServiceReady=El servicio de Windows "{#ServiceDisplayName}" está instalado y en ejecución.
RunEditConfig=Abrir config.json en el Bloc de notas
RunDocs=Abrir la documentación del proyecto (3 idiomas)
IconValidate=Validar la configuración
IconConfig=Editar config.json
IconDocs=Documentación del proyecto
IconUninstall=Desinstalar {#AppName}
UninstKeepConfig=¿Conservar config.json y los archivos de log?%n%nElige No para eliminarlos junto con la aplicación.
UninstServiceRemoved=Se eliminó el servicio de Windows "{#ServiceDisplayName}".
UninstServiceFailed=No se pudo eliminar el servicio de Windows:%n%n%1
FinishedLine=Apunta tus productores al backend de entrada configurado y observa el directorio de logs para ver los primeros mensajes.
ServiceAlive=El relay arrancó y se reportó como en ejecución.
ServiceNotServing=El servicio está en ejecución, pero el relay no se reportó como en ejecución en %1 segundos. Revisa el directorio de logs para saber el motivo.

; --- boton "Probar conexión" de las páginas de endpoint ---
ProbeButton=Probar conexión
ProbeTesting=Probando... (espera hasta %1 segundos)
ProbeOk=Conectado: el servidor respondió en %1.
ProbeUnknownHost=No se pudo conectar: nombre de host desconocido. Revise si hay un error de escritura.
ProbeTimeout=No se pudo conectar: sin respuesta a tiempo. ¿El servidor está en marcha y accesible (cortafuegos, VPN)?
ProbeAuthFailed=El servidor rechazó las credenciales. Revise el usuario y el valor de la contraseña.
ProbeUnexpected=Algo respondió en esa dirección, pero no es el servidor esperado.
ProbeNoWebSocket=Esta dirección usa WebSocket, que esta comprobación no admite. Use el --check-config del relay, o pruébelo después de instalar.
ProbeEmpty=Escriba el endpoint antes de probar.
ProbeRefused=No se pudo conectar: se rechazó la conexión. ¿El servidor está en marcha en ese puerto?
