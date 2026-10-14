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
