; Spanish translation for the installer wizard.
; Keys not listed here fall back to messages.english.isl.
;
[CustomMessages]
GroupService=Servicio de Windows
GroupShortcuts=Accesos directos
TaskServiceDesc=Instalar e iniciar el servicio de Windows (recomendado)
TaskServiceAutoDesc=Iniciar el servicio automáticamente con Windows
TaskDesktopIconDesc=Crear un acceso directo en el escritorio para validar la configuración
PageConfigTitle=Configuración
PageConfigSubtitle=¿Qué configuración debe usar el relay?
PageConfigDesc=El relay lee un único archivo JSON. La ruta predeterminada se crea a partir de config.example.json con rutas absolutas de log, lock y status, porque un servicio de Windows se inicia con su directorio de trabajo en System32. La validación usa el --check-config del propio relay.%n%nApunta a un archivo existente para conservar tu configuración.
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
