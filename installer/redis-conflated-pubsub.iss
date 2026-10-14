; Conflated Pub/Sub installer + uninstaller (Inno Setup 6).
;
; Build:  ISCC /DAppVersion=<version> .\installer\redis-conflated-pubsub.iss
; CI builds it on every run and attaches ConflatedPubSubSetup-<version>.exe to a
; v* tag release (see .github/workflows/release.yml).
;
; Flow (in this order, as the wizard runs):
;   1. asks which configuration file to use (default: <install dir>\config.json);
;   2. VALIDATES it with the relay's own `--check-config`, using a temporary copy
;      of the executable (the install directory does not exist yet);
;   3. copies the relay, writes config.json from the example with ABSOLUTE paths
;      (a Windows service runs with its working directory set to System32, so the
;      example's relative log/lock/status paths would land there), then keeps an
;      existing config.json untouched on upgrade;
;   4. registers and starts the Windows service (optional task).
;
; The uninstaller stops and removes the service, then asks whether to keep
; config.json and the logs.
;
; Silent mode:
;   ConflatedPubSubSetup.exe /VERYSILENT /DIR=... /CONFIG=<path> /SERVICE=<0|1>
;   /SERVICESTART=<auto|delayed-auto|demand>

#define AppName "Conflated Pub/Sub"
#define AppExeName "redis-conflated-pubsub"
#define AppPublisher "rspadim"
#define AppUrl "https://github.com/rspadim/redis-conflated-pubsub"
#define ServiceName "redis-conflated-pubsub"
#define ServiceDisplayName "Conflated Pub/Sub relay"
#ifndef AppVersion
  ; Must stay numeric (a.b.c or a.b.c.d): it feeds the file's fixed version
  ; resource, which rejects anything else. A local build is not a release, so it
  ; reports 0.0.0.0 and produces ConflatedPubSubSetup-0.0.0.exe; CI passes the
  ; release tag without the leading "v". Keep pre-release suffixes out of this
  ; value: they would make the compiler reject the version resource.
  #define AppVersion "0.0.0"
#endif
#ifndef BuildDir
  #define BuildDir "..\target\release"
#endif
#ifndef RepoRoot
  #define RepoRoot ".."
#endif
#define PowerShell "{sys}\WindowsPowerShell\v1.0\powershell.exe"

[Setup]
; A new AppId keeps this installer's upgrade/uninstall identity separate from
; any other Inno-based project. Never reuse another project's GUID.
AppId={{8C4A1F62-3D5B-4E19-9A77-2F0B6C1D4E88}
AppName={#AppName}
AppVersion={#AppVersion}
AppVerName={#AppName} {#AppVersion}
AppPublisher={#AppPublisher}
AppPublisherURL={#AppUrl}
AppSupportURL={#AppUrl}/issues
AppUpdatesURL={#AppUrl}/releases
DefaultDirName={autopf}\{#AppName}
DefaultGroupName={#AppName}
DisableDirPage=no
AllowNoIcons=yes
LicenseFile={#RepoRoot}\LICENSE
SetupIconFile={#RepoRoot}\installer\{#AppExeName}.ico
; Registering and controlling a service requires elevation.
PrivilegesRequired=admin
ArchitecturesAllowed=x64compatible
ArchitecturesInstallIn64BitMode=x64compatible
MinVersion=10.0
OutputDir={#RepoRoot}\installer\output
OutputBaseFilename=ConflatedPubSubSetup-{#AppVersion}
Compression=lzma2
SolidCompression=yes
WizardStyle=modern
SetupLogging=yes
UninstallDisplayName={#AppName} {#AppVersion}
UninstallDisplayIcon={app}\{#AppExeName}.exe
VersionInfoCompany={#AppPublisher}
VersionInfoDescription={#AppName} Setup
VersionInfoProductName={#AppName}
VersionInfoProductVersion={#AppVersion}
VersionInfoVersion={#AppVersion}

[Languages]
; english is the default (based on Default.isl); the rest ship with Inno Setup 6
; except the two Chinese files, vendored under installer\languages (the official
; build has no Chinese .isl).
Name: "english"; MessagesFile: "compiler:Default.isl"
Name: "brazilianportuguese"; MessagesFile: "compiler:Languages\BrazilianPortuguese.isl"
Name: "spanish"; MessagesFile: "compiler:Languages\Spanish.isl"
Name: "chinesesimplified"; MessagesFile: "languages\ChineseSimplified.isl"
Name: "chinesetraditional"; MessagesFile: "languages\ChineseTraditional.isl"
Name: "russian"; MessagesFile: "compiler:Languages\Russian.isl"
Name: "french"; MessagesFile: "compiler:Languages\French.isl"
Name: "korean"; MessagesFile: "compiler:Languages\Korean.isl"
Name: "german"; MessagesFile: "compiler:Languages\German.isl"
Name: "japanese"; MessagesFile: "compiler:Languages\Japanese.isl"

[CustomMessages]
; Messages without a language prefix apply to EVERY language, so a language that
; has no override below still shows English instead of a raw {cm:...} constant.
; Add a "<language>.Key=..." line to translate one language at a time.
;
; English (default for all languages)
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

; Brazilian Portuguese
brazilianportuguese.GroupService=Serviço do Windows
brazilianportuguese.GroupShortcuts=Atalhos
brazilianportuguese.TaskServiceDesc=Instalar e iniciar o serviço do Windows (recomendado)
brazilianportuguese.TaskServiceAutoDesc=Iniciar o serviço automaticamente com o Windows
brazilianportuguese.TaskDesktopIconDesc=Criar um atalho na área de trabalho para validar a configuração
brazilianportuguese.PageConfigTitle=Configuração
brazilianportuguese.PageConfigSubtitle=Qual configuração o relay deve usar?
brazilianportuguese.PageConfigDesc=O relay lê um único arquivo JSON. O caminho padrão é criado a partir de config.example.json com caminhos absolutos de log, lock e status, porque um serviço do Windows inicia com o diretório de trabalho em System32. A validação usa o --check-config do próprio relay.%n%nAponte para um arquivo existente para manter a sua configuração.
brazilianportuguese.ConfigPathLabel=Arquivo de configuração
brazilianportuguese.ValidateOk=A configuração é válida.
brazilianportuguese.ValidateFailed=O relay rejeitou esta configuração:%n%n%1%n%nInstalar mesmo assim? O serviço não vai iniciar até o arquivo ser corrigido.
brazilianportuguese.ValidateMissing=Escolha o caminho de um arquivo de configuração.
brazilianportuguese.ConfigMissing=O arquivo de configuração não existe:%n%n%1
brazilianportuguese.ConfigCreated=config.json foi gravado a partir de config.example.json com caminhos absolutos.
brazilianportuguese.ConfigKept=O arquivo de configuração existente foi mantido.
brazilianportuguese.ServiceInstallFailed=O serviço do Windows não pôde ser instalado:%n%n%1
brazilianportuguese.ServiceStartFailed=O serviço foi instalado mas não pôde ser iniciado:%n%n%1
brazilianportuguese.ServiceReady=O serviço do Windows "{#ServiceDisplayName}" está instalado e em execução.
brazilianportuguese.RunEditConfig=Abrir config.json no Bloco de Notas
brazilianportuguese.RunDocs=Abrir a documentação do projeto (3 idiomas)
brazilianportuguese.IconValidate=Validar a configuração
brazilianportuguese.IconConfig=Editar config.json
brazilianportuguese.IconDocs=Documentação do projeto
brazilianportuguese.IconUninstall=Desinstalar o {#AppName}
brazilianportuguese.UninstKeepConfig=Manter config.json e os arquivos de log?%n%nEscolha Não para apagá-los junto com a aplicação.
brazilianportuguese.UninstServiceRemoved=O serviço do Windows "{#ServiceDisplayName}" foi removido.
brazilianportuguese.UninstServiceFailed=O serviço do Windows não pôde ser removido:%n%n%1
brazilianportuguese.FinishedLine=Aponte seus produtores para o backend de entrada configurado e acompanhe o diretório de logs para ver as primeiras mensagens.

; Spanish
spanish.GroupService=Servicio de Windows
spanish.GroupShortcuts=Accesos directos
spanish.TaskServiceDesc=Instalar e iniciar el servicio de Windows (recomendado)
spanish.TaskServiceAutoDesc=Iniciar el servicio automáticamente con Windows
spanish.TaskDesktopIconDesc=Crear un acceso directo en el escritorio para validar la configuración
spanish.PageConfigTitle=Configuración
spanish.PageConfigSubtitle=¿Qué configuración debe usar el relay?
spanish.PageConfigDesc=El relay lee un único archivo JSON. La ruta predeterminada se crea a partir de config.example.json con rutas absolutas de log, lock y status, porque un servicio de Windows se inicia con su directorio de trabajo en System32. La validación usa el --check-config del propio relay.%n%nApunta a un archivo existente para conservar tu configuración.
spanish.ConfigPathLabel=Archivo de configuración
spanish.ValidateOk=La configuración es válida.
spanish.ValidateFailed=El relay rechazó esta configuración:%n%n%1%n%n¿Instalar de todos modos? El servicio no arrancará hasta que se corrija el archivo.
spanish.ValidateMissing=Elige la ruta de un archivo de configuración.
spanish.ConfigMissing=El archivo de configuración no existe:%n%n%1
spanish.ConfigCreated=config.json se escribió a partir de config.example.json con rutas absolutas.
spanish.ConfigKept=Se conservó el archivo de configuración existente.
spanish.ServiceInstallFailed=No se pudo instalar el servicio de Windows:%n%n%1
spanish.ServiceStartFailed=El servicio se instaló pero no se pudo iniciar:%n%n%1
spanish.ServiceReady=El servicio de Windows "{#ServiceDisplayName}" está instalado y en ejecución.
spanish.RunEditConfig=Abrir config.json en el Bloc de notas
spanish.RunDocs=Abrir la documentación del proyecto (3 idiomas)
spanish.IconValidate=Validar la configuración
spanish.IconConfig=Editar config.json
spanish.IconDocs=Documentación del proyecto
spanish.IconUninstall=Desinstalar {#AppName}
spanish.UninstKeepConfig=¿Conservar config.json y los archivos de log?%n%nElige No para eliminarlos junto con la aplicación.
spanish.UninstServiceRemoved=Se eliminó el servicio de Windows "{#ServiceDisplayName}".
spanish.UninstServiceFailed=No se pudo eliminar el servicio de Windows:%n%n%1
spanish.FinishedLine=Apunta tus productores al backend de entrada configurado y observa el directorio de logs para ver los primeros mensajes.

[Tasks]
Name: "service"; Description: "{cm:TaskServiceDesc}"; GroupDescription: "{cm:GroupService}"; Flags: checkedonce
Name: "serviceauto"; Description: "{cm:TaskServiceAutoDesc}"; GroupDescription: "{cm:GroupService}"; Flags: checkedonce
Name: "desktopicon"; Description: "{cm:TaskDesktopIconDesc}"; GroupDescription: "{cm:GroupShortcuts}"; Flags: unchecked

[Files]
Source: "{#BuildDir}\{#AppExeName}.exe"; DestDir: "{app}"; Flags: ignoreversion
Source: "{#RepoRoot}\config.example.json"; DestDir: "{app}"; Flags: ignoreversion
Source: "{#RepoRoot}\config.nats.example.json"; DestDir: "{app}"; Flags: ignoreversion
Source: "{#RepoRoot}\README.md"; DestDir: "{app}"; Flags: ignoreversion
Source: "{#RepoRoot}\LICENSE"; DestDir: "{app}"; Flags: ignoreversion
; Generated by `redis-conflated-pubsub --config-json-schema`; editors use it for
; completion. Not checked in, so a local build without it still compiles.
Source: "{#RepoRoot}\config.schema.json"; DestDir: "{app}"; Flags: ignoreversion skipifsourcedoesntexist
; Helper scripts: configuration bootstrap, service control, configuration check.
Source: "{#RepoRoot}\installer\install-config.ps1"; DestDir: "{app}\scripts"; Flags: ignoreversion
Source: "{#RepoRoot}\installer\service.ps1"; DestDir: "{app}\scripts"; Flags: ignoreversion
Source: "{#RepoRoot}\installer\validate-config.ps1"; DestDir: "{app}\scripts"; Flags: ignoreversion
Source: "{#RepoRoot}\installer\Validate-Config.cmd"; DestDir: "{app}"; Flags: ignoreversion
; Temporary copies so the wizard can validate BEFORE anything is installed.
Source: "{#BuildDir}\{#AppExeName}.exe"; DestDir: "{tmp}"; Flags: dontcopy
Source: "{#RepoRoot}\installer\validate-config.ps1"; DestDir: "{tmp}"; Flags: dontcopy

[Dirs]
Name: "{app}\logs"; Flags: uninsneveruninstall

[Icons]
; Shortcut FILE names stay English and stable: a localized file name created a
; second shortcut on a reinstall in another language and orphaned the first.
Name: "{group}\Validate configuration"; Filename: "{app}\Validate-Config.cmd"; Comment: "{cm:IconValidate}"; WorkingDir: "{app}"; IconFilename: "{app}\{#AppExeName}.exe"
Name: "{group}\Edit config.json"; Filename: "{sys}\notepad.exe"; Parameters: """{app}\config.json"""; Comment: "{cm:IconConfig}"
Name: "{group}\Project documentation"; Filename: "{#AppUrl}"; Comment: "{cm:IconDocs}"
Name: "{group}\Uninstall {#AppName}"; Filename: "{uninstallexe}"; Comment: "{cm:IconUninstall}"
Name: "{autodesktop}\{#AppName} - Validate configuration"; Filename: "{app}\Validate-Config.cmd"; Comment: "{cm:IconValidate}"; WorkingDir: "{app}"; IconFilename: "{app}\{#AppExeName}.exe"; Tasks: desktopicon

[Run]
Filename: "{sys}\notepad.exe"; Parameters: """{app}\config.json"""; Description: "{cm:RunEditConfig}"; Flags: postinstall nowait skipifsilent unchecked
Filename: "{#AppUrl}"; Description: "{cm:RunDocs}"; Flags: postinstall shellexec nowait skipifsilent unchecked

[UninstallDelete]
Type: filesandordirs; Name: "{app}\scripts"

[Code]
var
  ConfigPage: TInputFileWizardPage;
  KeepConfig: Boolean;
  ServiceWasInstalled: Boolean;
  ServiceWasStopped: Boolean;

function PowerShellExe(): String;
begin
  Result := ExpandConstant('{#PowerShell}');
  if not FileExists(Result) then
    Result := 'powershell.exe';
end;

function ReadTextFile(const Path: String): String;
var
  Lines: TArrayOfString;
  I: Integer;
begin
  Result := '';
  if not FileExists(Path) then
    exit;
  { The second parameter is a TArrayOfString, not a String: reading a report
    line by line keeps both the wizard and this helper free of AnsiString. }
  if not LoadStringsFromFile(Path, Lines) then
    exit;
  for I := 0 to GetArrayLength(Lines) - 1 do
  begin
    if I > 0 then
      Result := Result + #13#10;
    Result := Result + Lines[I];
  end;
  Result := Trim(Result);
end;

function RunScript(const ScriptPath, Params: String; var ExitCode: Integer): Boolean;
begin
  Result := Exec(PowerShellExe(),
    '-NoProfile -NonInteractive -ExecutionPolicy Bypass -File "' + ScriptPath + '" ' + Params,
    '', SW_HIDE, ewWaitUntilTerminated, ExitCode);
end;

{ Extracts the executable and the validator to the temporary directory on first
  use, so the wizard can run --check-config before anything is copied to the
  install directory. There is no nested brace here on purpose: a brace inside a
  Pascal comment would close it. }
function EnsureValidatorExtracted(): Boolean;
var
  ExePath: String;
begin
  ExePath := ExpandConstant('{tmp}\{#AppExeName}.exe');
  if not FileExists(ExePath) then
    ExtractTemporaryFile('{#AppExeName}.exe');
  if not FileExists(ExpandConstant('{tmp}\validate-config.ps1')) then
    ExtractTemporaryFile('validate-config.ps1');
  Result := FileExists(ExePath);
end;

function ValidateChosenConfig(): Boolean;
var
  ExePath, ScriptPath, ConfigPath, ReportPath, Params, Report: String;
  ExitCode: Integer;
begin
  Result := True;
  ConfigPath := ConfigPage.Values[0];

  if ConfigPath = '' then
  begin
    MsgBox(CustomMessage('ValidateMissing'), mbError, MB_OK);
    Result := False;
    exit;
  end;

  if not FileExists(ConfigPath) then
  begin
    MsgBox(FmtMessage(CustomMessage('ConfigMissing'), [ConfigPath]), mbError, MB_OK);
    Result := False;
    exit;
  end;

  if not EnsureValidatorExtracted() then
    exit; { Cannot validate here; the service still reports the real error. }

  ExePath := ExpandConstant('{tmp}\{#AppExeName}.exe');
  ScriptPath := ExpandConstant('{tmp}\validate-config.ps1');
  ReportPath := ExpandConstant('{tmp}\validate-report.txt');
  DeleteFile(ReportPath);

  Params := '-ConfigPath "' + ConfigPath + '" -ExePath "' + ExePath + '" -ReportFile "' + ReportPath + '"';
  if not RunScript(ScriptPath, Params, ExitCode) then
    exit; { PowerShell unavailable: do not block the install. }

  if ExitCode <> 0 then
  begin
    Report := ReadTextFile(ReportPath);
    if MsgBox(FmtMessage(CustomMessage('ValidateFailed'), [Report]), mbError, MB_YESNO) = IDNO then
      Result := False;
  end
  else
    MsgBox(CustomMessage('ValidateOk'), mbInformation, MB_OK);
end;

function PrepareToInstall(var NeedsRestart: Boolean): String;
var
  ScriptPath, Params, ReportPath: String;
  ExitCode: Integer;
begin
  Result := '';
  NeedsRestart := False;
  ServiceWasStopped := False;

  { An upgrade replaces the executable of a service that is still running.
    Windows lets a running image be renamed but not overwritten, so the copy
    would be deferred to the next reboot and the service would keep executing the
    old binary while the wizard reports success. Stop it before the files are
    copied; CurStepChanged starts it again. On a fresh install the scripts do not
    exist yet, which is exactly the case where there is nothing to stop. }
  ScriptPath := ExpandConstant('{app}\scripts\service.ps1');
  if not FileExists(ScriptPath) then
    exit;

  ReportPath := ExpandConstant('{tmp}\preinstall-report.txt');
  Params := '-Action status -ReportFile "' + ReportPath + '"';
  DeleteFile(ReportPath);
  if (not RunScript(ScriptPath, Params, ExitCode)) or (ExitCode <> 0) then
    exit;

  Params := '-Action stop -ReportFile "' + ReportPath + '"';
  DeleteFile(ReportPath);
  if RunScript(ScriptPath, Params, ExitCode) and (ExitCode = 0) then
    ServiceWasStopped := True;
end;

procedure InitializeWizard();
begin
  ConfigPage := CreateInputFilePage(wpSelectTasks,
    CustomMessage('PageConfigTitle'),
    CustomMessage('PageConfigSubtitle'),
    CustomMessage('PageConfigDesc'));
  ConfigPage.Add(CustomMessage('ConfigPathLabel'),
    'JSON configuration files|*.json|All files|*.*', '.json');
  ConfigPage.Values[0] := ExpandConstant('{app}\config.json');
end;

function NextButtonClick(CurPageID: Integer): Boolean;
begin
  Result := True;
  if CurPageID = ConfigPage.ID then
    Result := ValidateChosenConfig();
end;

procedure CurStepChanged(CurStep: TSetupStep);
var
  ScriptPath, Params, ReportPath, Report, StartupType, ExePath: String;
  ExitCode: Integer;
begin
  if CurStep <> ssPostInstall then
    exit;

  ReportPath := ExpandConstant('{tmp}\postinstall-report.txt');
  ExePath := ExpandConstant('{app}\{#AppExeName}.exe');

  { config.json: absolute paths, created only when missing. }
  ScriptPath := ExpandConstant('{app}\scripts\install-config.ps1');
  Params := '-AppDir "' + ExpandConstant('{app}') + '"' +
            ' -ExamplePath "' + ExpandConstant('{app}\config.example.json') + '"' +
            ' -TargetPath "' + ConfigPage.Values[0] + '"' +
            ' -ReportFile "' + ReportPath + '"';
  DeleteFile(ReportPath);
  RunScript(ScriptPath, Params, ExitCode);

  if not WizardIsTaskSelected('service') then
  begin
    { A service stopped for the upgrade is still registered, so put the machine
      back the way we found it instead of silently leaving the relay down. }
    if ServiceWasStopped then
    begin
      ScriptPath := ExpandConstant('{app}\scripts\service.ps1');
      Params := '-Action start -ReportFile "' + ReportPath + '"';
      DeleteFile(ReportPath);
      RunScript(ScriptPath, Params, ExitCode);
    end;
    exit;
  end;

  if WizardIsTaskSelected('serviceauto') then
    StartupType := 'auto'
  else
    StartupType := 'demand';

  ScriptPath := ExpandConstant('{app}\scripts\service.ps1');
  Params := '-Action install -ExePath "' + ExePath +
            '" -ConfigPath "' + ConfigPage.Values[0] +
            '" -StartupType ' + StartupType +
            ' -ReportFile "' + ReportPath + '"';
  DeleteFile(ReportPath);

  if (not RunScript(ScriptPath, Params, ExitCode)) or (ExitCode <> 0) then
  begin
    Report := ReadTextFile(ReportPath);
    MsgBox(FmtMessage(CustomMessage('ServiceInstallFailed'), [Report]), mbError, MB_OK);
    exit;
  end;

  Params := '-Action start -ReportFile "' + ReportPath + '"';
  DeleteFile(ReportPath);
  if (not RunScript(ScriptPath, Params, ExitCode)) or (ExitCode <> 0) then
  begin
    Report := ReadTextFile(ReportPath);
    MsgBox(FmtMessage(CustomMessage('ServiceStartFailed'), [Report]), mbError, MB_OK);
    exit;
  end;

  ServiceWasInstalled := True;
end;

procedure CurPageChanged(CurPageID: Integer);
var
  Extra: String;
begin
  if CurPageID <> wpFinished then
    exit;

  Extra := CustomMessage('FinishedLine');
  if ServiceWasInstalled then
    Extra := CustomMessage('ServiceReady') + #13#10 + #13#10 + Extra;
  WizardForm.FinishedLabel.Caption := Extra + #13#10#13#10 + WizardForm.FinishedLabel.Caption;
end;

function InitializeUninstall(): Boolean;
begin
  ServiceWasInstalled := False;
  KeepConfig := MsgBox(CustomMessage('UninstKeepConfig'), mbConfirmation, MB_YESNO) = IDYES;
  Result := True;
end;

procedure CurUninstallStepChanged(CurUninstallStep: TUninstallStep);
var
  ScriptPath, Params, ReportPath, Report, AppDir: String;
  ExitCode: Integer;
begin
  AppDir := ExpandConstant('{app}');
  ScriptPath := ExpandConstant('{app}\scripts\service.ps1');
  ReportPath := ExpandConstant('{tmp}\uninstall-report.txt');

  if CurUninstallStep = usUninstall then
  begin
    if not FileExists(ScriptPath) then
      exit;

    Params := '-Action uninstall -ReportFile "' + ReportPath + '"';
    DeleteFile(ReportPath);
    if (not RunScript(ScriptPath, Params, ExitCode)) or (ExitCode <> 0) then
    begin
      Report := ReadTextFile(ReportPath);
      if Report = '' then
        Report := 'service.ps1 could not be executed';
      MsgBox(FmtMessage(CustomMessage('UninstServiceFailed'), [Report]), mbError, MB_OK);
    end;
    exit;
  end;

  if CurUninstallStep = usPostUninstall then
  begin
    if KeepConfig then
      exit;
    DeleteFile(AppDir + '\config.json');
    DeleteFile(AppDir + '\redis-conflated-pubsub.lock');
    DeleteFile(AppDir + '\status.json');
    DelTree(AppDir + '\logs', True, True, True);
  end;
end;
