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
;
; Every language reads languages\messages.english.isl first: that file holds the
; base wording of every custom message this wizard uses. A language file listed
; after it overrides only the keys it translates, so a partially translated
; language shows English for the missing keys instead of a raw {cm:...}
; constant, and a language with no file at all still gets the English wording.
;
; Nothing is defined inline in this script on purpose: an entry in the main
; script is applied to every language and would shadow the per-language files.
Name: "english"; MessagesFile: "compiler:Default.isl,languages\messages.english.isl"
Name: "brazilianportuguese"; MessagesFile: "compiler:Languages\BrazilianPortuguese.isl,languages\messages.english.isl,languages\messages.brazilianportuguese.isl"
Name: "spanish"; MessagesFile: "compiler:Languages\Spanish.isl,languages\messages.english.isl,languages\messages.spanish.isl"
Name: "chinesesimplified"; MessagesFile: "languages\ChineseSimplified.isl,languages\messages.english.isl,languages\messages.chinesesimplified.isl"
Name: "chinesetraditional"; MessagesFile: "languages\ChineseTraditional.isl,languages\messages.english.isl,languages\messages.chinesetraditional.isl"
Name: "russian"; MessagesFile: "compiler:Languages\Russian.isl,languages\messages.english.isl,languages\messages.russian.isl"
Name: "french"; MessagesFile: "compiler:Languages\French.isl,languages\messages.english.isl,languages\messages.french.isl"
Name: "korean"; MessagesFile: "compiler:Languages\Korean.isl,languages\messages.english.isl,languages\messages.korean.isl"
Name: "german"; MessagesFile: "compiler:Languages\German.isl,languages\messages.english.isl,languages\messages.german.isl"
Name: "japanese"; MessagesFile: "compiler:Languages\Japanese.isl,languages\messages.english.isl,languages\messages.japanese.isl"
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
Source: "{#RepoRoot}\installer\verify-service.ps1"; DestDir: "{app}\scripts"; Flags: ignoreversion
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
  ServiceVerification: String;

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

  { `sc start` only proves that the service manager accepted the request. Ask the
    relay whether it actually came up: a service that exits during startup would
    otherwise be reported as a success. }
  ScriptPath := ExpandConstant('{app}\scripts\verify-service.ps1');
  Params := '-ConfigPath "' + ConfigPage.Values[0] +
            '" -TimeoutSeconds 20 -ReportFile "' + ReportPath + '"';
  DeleteFile(ReportPath);
  if RunScript(ScriptPath, Params, ExitCode) and (ExitCode = 0) then
    ServiceVerification := CustomMessage('ServiceAlive')
  else
    ServiceVerification := FmtMessage(CustomMessage('ServiceNotServing'), ['20']);
end;

procedure CurPageChanged(CurPageID: Integer);
var
  Extra: String;
begin
  if CurPageID <> wpFinished then
    exit;

  Extra := CustomMessage('FinishedLine');
  if ServiceVerification <> '' then
    Extra := ServiceVerification + #13#10 + #13#10 + Extra;
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
