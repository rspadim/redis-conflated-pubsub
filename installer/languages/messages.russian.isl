; russian translation for the installer wizard.
; Keys not listed here fall back to messages.english.isl.

[CustomMessages]
GroupService=Служба Windows
GroupShortcuts=Ярлыки
TaskServiceDesc=Установить и запустить службу Windows (рекомендуется)
TaskServiceAutoDesc=Запускать службу автоматически при загрузке Windows
TaskDesktopIconDesc=Создать ярлык на рабочем столе для проверки конфигурации
PageConfigTitle=Конфигурация
PageConfigSubtitle=Какую конфигурацию должен использовать relay?
PageConfigDesc=Relay читает один файл JSON. Путь по умолчанию создаётся из config.example.json с абсолютными путями log, lock и status, потому что при запуске службы Windows рабочим каталогом становится System32. Проверка использует --check-config самого relay.%n%nУкажите существующий файл, чтобы сохранить собственную конфигурацию.
ConfigPathLabel=Файл конфигурации
ValidateOk=Конфигурация корректна.
ValidateFailed=Relay отклонил эту конфигурацию:%n%n%1%n%nВсё равно установить? Служба не запустится, пока файл не будет исправлен.
ValidateMissing=Выберите путь к файлу конфигурации.
ConfigMissing=Файл конфигурации не существует:%n%n%1
ConfigCreated=config.json записан на основе config.example.json с абсолютными путями.
ConfigKept=Существующий файл конфигурации оставлен без изменений.
ServiceInstallFailed=Не удалось установить службу Windows:%n%n%1
ServiceStartFailed=Служба установлена, но её не удалось запустить:%n%n%1
ServiceReady=Служба Windows "{#ServiceDisplayName}" установлена и запущена.
RunEditConfig=Открыть config.json в Блокноте
RunDocs=Открыть документацию проекта (на 3 языках)
IconValidate=Проверить конфигурацию
IconConfig=Редактировать config.json
IconDocs=Документация проекта
IconUninstall=Деинсталлировать {#AppName}
UninstKeepConfig=Сохранить config.json и логи?%n%nВыберите «Нет», чтобы удалить их вместе с приложением.
UninstServiceRemoved=Служба Windows "{#ServiceDisplayName}" удалена.
UninstServiceFailed=Не удалось удалить службу Windows:%n%n%1
FinishedLine=Направьте продюсеров на настроенный входной бэкенд, затем следите за каталогом логов, чтобы увидеть первые сообщения.
ServiceAlive=Relay запустился и сообщил, что работает.
ServiceNotServing=Служба запущена, но relay не сообщил, что работает, в течение %1 сек. Проверьте каталог логов, чтобы узнать причину.
