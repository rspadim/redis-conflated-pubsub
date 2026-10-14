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

; --- страницы мастера конфигурации ---
PageInputTitle=Вход
PageInputDesc=Откуда приходят сообщения. Эндпоинт Redis записывается как host:port; добавьте схему rediss://, чтобы включить TLS (rediss://redis.example.net:6380). Шаблон использует семантику glob в Redis, поэтому feed:* соответствует каждому каналу, начинающемуся с feed:.
InEndpointLabel=Эндпоинт Redis (host:port)
InDatabaseLabel=База данных
InPasswordEnvLabel=Имя переменной среды с паролем (необязательно)
InPatternLabel=Шаблон подписки
PageOutputTitle=Выход
PageOutputDesc=Куда публикуются сообщения. Префикс и суффикс добавляются вокруг имени канала. Они также не дают relay читать собственный вывод: если вход и выход — это один и тот же сервер Redis, требуется хотя бы один из них, иначе relay отказывается запускаться с ошибкой эхо-петли.
OutEndpointLabel=Эндпоинт Redis (host:port)
OutDatabaseLabel=База данных
OutPasswordEnvLabel=Имя переменной среды с паролем (необязательно)
OutChannelPrefixLabel=Префикс канала
OutChannelSuffixLabel=Суффикс канала
PageTuningTitle=Тонкая настройка
PageTuningDesc=Насколько relay сжимает поток. При интервале объединения больше нуля в каждом окне публикуется только последнее значение для канала; ноль публикует каждое сообщение сразу. TTL дедупликации больше нуля подавляет идущие подряд одинаковые полезные нагрузки, пока окно не истечёт; ноль отключает дедупликацию.
TunIntervalLabel=Интервал объединения (мс, 0 публикует сразу)
TunTtlLabel=TTL дедупликации (мс, 0 отключает)
TunStatusLabel=HTTP-порт статуса (0 отключает прослушиватель)
TunLogLevelLabel=Уровень логирования (error, warn, info, debug, trace)
BadNumber=%1 должно быть целым неотрицательным числом.
BadLogLevel=Неизвестный уровень логирования. Используйте error, warn, info, debug или trace.
GenerateFailed=Не удалось создать конфигурацию:%n%n%1
ConfigGenerated=Конфигурация корректна и будет записана в %1.

; --- меню режима и страницы NATS ---
PageModeTitle=Что должен сделать мастер?
PageModeDesc=Выберите, как настроить relay. «Использовать существующий файл» пропускает все вопросы и берёт config.json, который у вас уже есть; остальные четыре варианта создают его для выбранной комбинации бэкендов, задавая только необходимые для неё вопросы.
ModeExisting=Использовать существующий config.json
ModeRedisRedis=Настроить вход Redis — выход Redis
ModeNatsNats=Настроить вход NATS — выход NATS
ModeRedisNats=Настроить вход Redis — выход NATS
ModeNatsRedis=Настроить вход NATS — выход Redis
PageNatsInTitle=Вход NATS
PageNatsInDesc=Серверы начального подключения разделяются запятыми или пробелами. Каждый URL должен содержать схему: nats:// для TCP, tls:// для TLS, ws:// или wss:// для WebSocket, причём в одном списке нельзя смешивать WebSocket с остальными. Subject использует подстановочные знаки NATS: * соответствует ровно одному токену, а > — всему остальному и должен идти последним.
PageNatsOutTitle=Выход NATS
PageNatsOutDesc=Те же правила для серверов и аутентификации, что и на входе. Префикс и суффикс добавляются вокруг subject, и итоговая строка после преобразования должна быть конкретным subject: подстановочные знаки отклоняются.
NatsServersLabel=Серверы NATS (через запятую)
NatsAuthLabel=Аутентификация (none, user_password, token, credentials_file)
NatsUsernameLabel=Имя пользователя (user_password)
NatsPasswordEnvLabel=Переменная среды с паролем (user_password)
NatsTokenEnvLabel=Переменная среды с токеном (token)
NatsCredentialsLabel=Путь к файлу .creds (credentials_file)
NatsSubjectLabel=Subject подписки
BadAuthType=Неизвестный тип аутентификации. Используйте none, user_password, token или credentials_file.
SelectorMissing=Укажите шаблон или subject, который должна получать эта подписка. Relay отклоняет пустой селектор.
ModeInstallOnly=Установить только исполняемый файл (без настройки)
FinishedNoConfig=Конфигурация не создана, и служба не установлена: начните с config.example.json или config.nats.example.json и запустите scripts\service.ps1 из каталога установки, когда будете готовы.

; --- пароли в переменных среды службы ---
SecretValueLabel=Значение пароля (сохраняется в собственной среде службы; оставьте пустым, чтобы задать переменную самостоятельно)
SecretsStored=Сохранено в среде службы: %1
SecretsFailed=Не удалось обновить среду службы:%n%n%1
SecretNotSetWarning=Для %1 не введено значение, поэтому программа установки его не сохранит. Relay читает свои учётные данные из собственной среды службы, где этой переменной ещё нет: служба завершится при запуске, пока вы её не зададите. Продолжить?
