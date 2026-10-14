; Base wording for every custom message of the installer wizard.
; Unprefixed entries apply to whichever language reads this file, and every
; language lists this file first, so a language file that defines only some of
; these keys falls back to the English text here instead of showing a raw
; {cm:...} constant. Comments use ';' because '#' starts a preprocessor directive.
;
[CustomMessages]
GroupService=Windows service
GroupShortcuts=Shortcuts
TaskServiceDesc=Install and start the Windows service (recommended)
TaskServiceAutoDesc=Start the service automatically when Windows boots
TaskDesktopIconDesc=Create a desktop shortcut for validating the configuration
PageConfigTitle=Configuration
PageConfigSubtitle=Which configuration should the relay use?
PageConfigDesc=The relay reads a single JSON file. The default path is created for you from config.example.json with absolute log, lock and status paths, because a Windows service starts with its working directory set to System32. Validation uses the relay's own --check-config.%n%nPoint this at an existing file to keep your own configuration.
ConfigPathLabel=Configuration file
ValidateOk=The configuration is valid.
ValidateFailed=The relay rejected this configuration:%n%n%1%n%nInstall anyway? The service will fail to start until the file is fixed.
ValidateMissing=Choose a configuration file path.
ConfigMissing=The configuration file does not exist:%n%n%1
ConfigCreated=config.json was written from config.example.json with absolute paths.
ConfigKept=An existing configuration file was kept untouched.
ServiceInstallFailed=The Windows service could not be installed:%n%n%1
ServiceStartFailed=The service was installed but could not be started:%n%n%1
ServiceReady=The Windows service "{#ServiceDisplayName}" is installed and running.
RunEditConfig=Open config.json in Notepad
RunDocs=Open the project documentation (3 languages)
IconValidate=Validate the configuration
IconConfig=Edit config.json
IconDocs=Project documentation
IconUninstall=Uninstall {#AppName}
UninstKeepConfig=Keep config.json and the log files?%n%nChoose No to delete them with the application.
UninstServiceRemoved=The Windows service "{#ServiceDisplayName}" was removed.
UninstServiceFailed=The Windows service could not be removed:%n%n%1
FinishedLine=Point your producers at the configured input backend, then watch the logs directory for the first messages.
ServiceAlive=The relay started and reported itself as running.
ServiceNotServing=The service is running, but the relay did not report itself as running within %1 seconds. Check the log directory to see why.

; --- configuration wizard pages ---
PageInputTitle=Input
PageInputDesc=Where the messages come from. A Redis endpoint is written as host:port; add the rediss:// scheme to enable TLS (rediss://redis.example.net:6380). The pattern uses Redis glob semantics, so feed:* matches every channel that starts with feed:.
InEndpointLabel=Redis endpoint (host:port)
InDatabaseLabel=Database
InPasswordEnvLabel=Name of the environment variable holding the password (optional)
InPatternLabel=Subscription pattern
PageOutputTitle=Output
PageOutputDesc=Where the messages are published. The prefix and suffix are placed around the channel name. They also keep the relay from reading its own output: when the input and the output are the same Redis server, at least one of them is required, otherwise the relay refuses to start with an echo-loop error.
OutEndpointLabel=Redis endpoint (host:port)
OutDatabaseLabel=Database
OutPasswordEnvLabel=Name of the environment variable holding the password (optional)
OutChannelPrefixLabel=Channel prefix
OutChannelSuffixLabel=Channel suffix
PageTuningTitle=Tuning
PageTuningDesc=How much the relay compresses the feed. With a conflation interval above zero only the latest value per channel is published inside each window; zero publishes every message immediately. A deduplication TTL above zero suppresses consecutive identical payloads until the window expires; zero disables it.
TunIntervalLabel=Conflation interval (ms, 0 publishes directly)
TunTtlLabel=Deduplication TTL (ms, 0 disables)
TunStatusLabel=Status HTTP port (0 disables the listener)
TunLogLevelLabel=Log level (error, warn, info, debug, trace)
BadNumber=%1 must be a whole number of zero or more.
BadLogLevel=Unknown log level. Use error, warn, info, debug or trace.
GenerateFailed=The configuration could not be generated:%n%n%1
ConfigGenerated=The configuration is valid and will be written to %1.

; --- mode menu and NATS pages ---
PageModeTitle=What should the wizard do?
PageModeDesc=Pick how to configure the relay. "Use an existing file" skips every question and takes a config.json you already have; the other four options build one for that backend combination, asking only the questions that combination needs.
ModeExisting=Use a config.json that already exists
ModeRedisRedis=Configure Redis input to Redis output
ModeNatsNats=Configure NATS input to NATS output
ModeRedisNats=Configure Redis input to NATS output
ModeNatsRedis=Configure NATS input to Redis output
PageNatsInTitle=NATS input
PageNatsInDesc=Seed servers separated by commas or spaces. Every URL needs a scheme: nats:// for TCP, tls:// for TLS, ws:// or wss:// for WebSocket, and one list cannot mix WebSocket with the others. The subject uses NATS wildcards: * matches exactly one token, and > matches the rest and must be last.
PageNatsOutTitle=NATS output
PageNatsOutDesc=The same server and authentication rules as the input. The prefix and suffix are placed around the subject, and the final mapped string must be a concrete subject: wildcards are rejected.
NatsServersLabel=NATS servers (comma-separated)
NatsAuthLabel=Authentication (none, user_password, token, credentials_file)
NatsUsernameLabel=Username (user_password)
NatsPasswordEnvLabel=Password environment variable (user_password)
NatsTokenEnvLabel=Token environment variable (token)
NatsCredentialsLabel=Credentials file path (credentials_file)
NatsSubjectLabel=Subscription subject
BadAuthType=Unknown authentication type. Use none, user_password, token or credentials_file.
SelectorMissing=Enter the pattern or subject this subscription should receive. The relay rejects an empty selector.
SecretValueLabel=Password value (stored in the service's own environment; leave empty to set the variable yourself)
SecretsStored=Stored in the service environment: %1
SecretsFailed=The service environment could not be updated:%n%n%1
SecretNotSetWarning=No value was entered for %1, so the installer will not store it. The relay reads its credentials from the service's own environment, where that variable does not exist yet: the service will exit at startup until you set it. Continue?
ModeInstallOnly=Install the executable only (no configuration)
FinishedNoConfig=No configuration was created and the service was not installed: start from config.example.json or config.nats.example.json and run scripts\service.ps1 from the install directory when you are ready.
BadServiceParam=/SERVICE accepts only 0 or 1, but got "%1".
BadStartupParam=/SERVICESTART accepts only auto, delayed-auto or demand, but got "%1".
ConfigRejected=The relay rejected this configuration:%n%n%1
