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
; Silent mode: /VERYSILENT (plus Inno's own switches such as /DIR=...) runs the
; wizard's default answers — Redis input to Redis output, the service installed and
; started. Three parameters make it usable for an unattended deployment:
;
;   /CONFIG=<file>                  use a configuration that already exists
;   /SERVICE=0|1                    register the service, or only drop the files
;   /SERVICESTART=auto|delayed-auto|demand
;
; An invalid value aborts before anything is written, and nothing is shown in
; silent mode: the non-zero exit code is the signal. Failures after the files are
; copied (the relay rejecting the configuration, the service refusing to install or
; start, the relay not coming up) also show nothing: they set the exit code to 9
; through GetCustomSetupExitCode - Inno itself returns 0 for a script failure - and
; land in {app}\install-report.txt. Without /CONFIG the configuration is generated
; from the wizard defaults; to change those answers, run the wizard or edit
; config.json afterwards.

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
; The wizard ships ten languages, so the choice is presented instead of being
; guessed from the system locale; Inno pre-selects the matching one.
ShowLanguageDialog=yes
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
Source: "{#RepoRoot}\installer\New-Config.ps1"; DestDir: "{app}\scripts"; Flags: ignoreversion
Source: "{#RepoRoot}\installer\Set-ServiceSecrets.ps1"; DestDir: "{app}\scripts"; Flags: ignoreversion
Source: "{#RepoRoot}\installer\service.ps1"; DestDir: "{app}\scripts"; Flags: ignoreversion
Source: "{#RepoRoot}\installer\validate-config.ps1"; DestDir: "{app}\scripts"; Flags: ignoreversion
Source: "{#RepoRoot}\installer\verify-service.ps1"; DestDir: "{app}\scripts"; Flags: ignoreversion
Source: "{#RepoRoot}\installer\Validate-Config.cmd"; DestDir: "{app}"; Flags: ignoreversion
; Temporary copies so the wizard can validate BEFORE anything is installed.
Source: "{#BuildDir}\{#AppExeName}.exe"; DestDir: "{tmp}"; Flags: dontcopy
Source: "{#RepoRoot}\installer\validate-config.ps1"; DestDir: "{tmp}"; Flags: dontcopy
Source: "{#RepoRoot}\installer\New-Config.ps1"; DestDir: "{tmp}"; Flags: dontcopy

[Dirs]
Name: "{app}\logs"; Flags: uninsneveruninstall

[Icons]
; Shortcut FILE names stay English and stable: a localized file name created a
; second shortcut on a reinstall in another language and orphaned the first.
; The configuration shortcuts are skipped when the wizard was told to install the
; executable only, because there is no config.json to open then.
Name: "{group}\Validate configuration"; Filename: "{app}\Validate-Config.cmd"; Comment: "{cm:IconValidate}"; WorkingDir: "{app}"; IconFilename: "{app}\{#AppExeName}.exe"; Check: WantsConfiguration
Name: "{group}\Edit config.json"; Filename: "{sys}\notepad.exe"; Parameters: """{app}\config.json"""; Comment: "{cm:IconConfig}"; Check: WantsConfiguration
Name: "{group}\Project documentation"; Filename: "{#AppUrl}"; Comment: "{cm:IconDocs}"
Name: "{group}\Uninstall {#AppName}"; Filename: "{uninstallexe}"; Comment: "{cm:IconUninstall}"
Name: "{autodesktop}\{#AppName} - Validate configuration"; Filename: "{app}\Validate-Config.cmd"; Comment: "{cm:IconValidate}"; WorkingDir: "{app}"; IconFilename: "{app}\{#AppExeName}.exe"; Tasks: desktopicon; Check: WantsConfiguration

[Run]
Filename: "{sys}\notepad.exe"; Parameters: """{app}\config.json"""; Description: "{cm:RunEditConfig}"; Flags: postinstall nowait skipifsilent unchecked; Check: WantsConfiguration
Filename: "{#AppUrl}"; Description: "{cm:RunDocs}"; Flags: postinstall shellexec nowait skipifsilent unchecked

[UninstallDelete]
Type: filesandordirs; Name: "{app}\scripts"

[Code]
var
  ModePage: TInputOptionWizardPage;
  RedisInPage, RedisOutPage, NatsInPage, NatsOutPage, TuningPage: TInputQueryWizardPage;
  ConfigPage: TInputFileWizardPage;
  ConfigDefault: String;
  SecretsMessage: String;
  KeepConfig: Boolean;
  ServiceWasInstalled: Boolean;
  ServiceWasStopped: Boolean;
  ServiceVerification: String;
  SilentConfigPath: String;
  SilentServiceSet: Boolean;
  SilentInstallService: Boolean;
  SilentStartupType: String;
  PostInstallFailure: String;
  PostInstallReport: String;

{ --- mode helpers ------------------------------------------------------- }
{ Declared before the functions that use them: Pascal Script resolves
  identifiers in order. They read a page that exists by the time they run. }

{ The mode lives on a wizard page; a silent install may reach the code below
  before the pages exist, and the first entry is the default. }
function ModeIndex(): Integer;
begin
  { A silent install reaches this before anything has been selected, and an option
    page reports -1 until something is: both mean the default mode. }
  if (ModePage = nil) or (ModePage.SelectedValueIndex < 0) then
    Result := 0
  else
    Result := ModePage.SelectedValueIndex;
end;

function UsesExistingFile(): Boolean;
begin
  Result := ModeIndex() = 4;
end;

function InstallsOnly(): Boolean;
begin
  Result := ModeIndex() = 5;
end;

{ True when the wizard collects answers and writes a configuration file. }
function WantsConfiguration(): Boolean;
begin
  Result := ModeIndex() < 4;
end;

function InputIsRedis(): Boolean;
begin
  Result := (ModeIndex() = 0) or (ModeIndex() = 2);
end;

function OutputIsRedis(): Boolean;
begin
  Result := (ModeIndex() = 0) or (ModeIndex() = 3);
end;

{ --- unattended install --------------------------------------------------- }
{ /CONFIG=<file> uses a configuration that already exists, /SERVICE=0|1 decides
  whether the service is registered, and /SERVICESTART=auto|delayed-auto|demand
  overrides its startup type. An invalid value aborts before anything is written;
  in silent mode nothing is shown, because a dialog would wait for a click that
  never comes — the non-zero exit code is the signal automation can act on. }

function StripQuotes(const Value: String): String;
begin
  Result := Trim(Value);
  if (Length(Result) >= 2) and (Result[1] = '"') and (Result[Length(Result)] = '"') then
    Result := Copy(Result, 2, Length(Result) - 2);
end;

function ParamValue(const Name: String): String;
var
  I: Integer;
  Prefix: String;
begin
  Result := '';
  Prefix := '/' + Name + '=';
  for I := 1 to ParamCount do
    if CompareText(Copy(ParamStr(I), 1, Length(Prefix)), Prefix) = 0 then
    begin
      Result := StripQuotes(Copy(ParamStr(I), Length(Prefix) + 1, Length(ParamStr(I))));
      exit;
    end;
end;

function IsStartupType(const Value: String): Boolean;
var
  Normalized: String;
begin
  Normalized := LowerCase(Trim(Value));
  Result := (Normalized = 'auto') or (Normalized = 'delayed-auto') or
            (Normalized = 'demand');
end;

function InitializeSetup(): Boolean;
var
  Value: String;
begin
  Result := True;
  SilentConfigPath := '';
  SilentServiceSet := False;
  SilentInstallService := True;
  SilentStartupType := '';

  Value := ParamValue('CONFIG');
  if Value <> '' then
  begin
    if not FileExists(Value) then
    begin
      if not WizardSilent() then
        MsgBox(FmtMessage(CustomMessage('ConfigMissing'), [Value]), mbError, MB_OK);
      Result := False;
      exit;
    end;
    SilentConfigPath := Value;
  end;

  Value := ParamValue('SERVICE');
  if Value <> '' then
  begin
    if (Value <> '0') and (Value <> '1') then
    begin
      if not WizardSilent() then
        MsgBox(FmtMessage(CustomMessage('BadServiceParam'), [Value]), mbError, MB_OK);
      Result := False;
      exit;
    end;
    SilentServiceSet := True;
    SilentInstallService := Value = '1';
  end;

  Value := ParamValue('SERVICESTART');
  if Value <> '' then
  begin
    if not IsStartupType(Value) then
    begin
      if not WizardSilent() then
        MsgBox(FmtMessage(CustomMessage('BadStartupParam'), [Value]), mbError, MB_OK);
      Result := False;
      exit;
    end;
    SilentStartupType := LowerCase(Trim(Value));
  end;
end;

{ Where the configuration comes from, whichever way the setup was started. }
function EffectiveConfigPath(): String;
begin
  if SilentConfigPath <> '' then
    Result := SilentConfigPath
  else if ConfigPage.Values[0] <> '' then
    Result := ConfigPage.Values[0]
  else
    Result := ExpandConstant('{app}\config.json');
end;

{ The tasks belong to the wizard: on a later installation Inno's "checkedonce"
  leaves them unchecked, so reading them in silent mode would make an unattended
  upgrade silently skip the service. Silent mode has its own documented defaults,
  and /SERVICE and /SERVICESTART override them. }
function ShouldInstallService(): Boolean;
begin
  if SilentServiceSet then
    Result := SilentInstallService
  else if WizardSilent() then
    Result := True
  else
    Result := WizardIsTaskSelected('service');
end;

function ServiceStartupType(): String;
begin
  if SilentStartupType <> '' then
    Result := SilentStartupType
  else if WizardSilent() then
    Result := 'auto'
  else if WizardIsTaskSelected('serviceauto') then
    Result := 'auto'
  else
    Result := 'demand';
end;

{ /CONFIG points at a file that must already exist, so there is nothing to
  generate; otherwise the wizard's own mode decides. }
function ShouldGenerateConfig(): Boolean;
begin
  if SilentConfigPath <> '' then
    Result := False
  else
    Result := WantsConfiguration();
end;

procedure FailOrReport(const Message: String);
begin
  { Records a failure after the files are copied. A dialog in an unattended install
    would wait for a click that never comes, so in silent mode nothing is shown:
    the caller stops, the report written next to the installation says what
    happened, and GetCustomSetupExitCode turns it into a non-zero exit code. Inno
    returns 0 for a script failure - verified: both RaiseException and Abort() from
    ssPostInstall leave the exit code at 0 - so that hook is the only way to fail
    loudly. }
  if PostInstallFailure = '' then
    PostInstallFailure := Message;
  if not WizardSilent() then
    SuppressibleMsgBox(Message, mbError, MB_OK, IDOK);
end;

function GetCustomSetupExitCode(): Integer;
begin
  if PostInstallFailure <> '' then
    Result := 9
  else
    Result := 0;
end;

procedure ReportLine(const Line: String);
begin
  PostInstallReport := PostInstallReport + Line + #13#10;
end;

{ The exit code cannot carry a sentence and a silent install has no dialog to
  read, so the outcome of every step lands in a file next to the installation. }
procedure FlushReport();
begin
  if PostInstallFailure <> '' then
    ReportLine('result: failed - ' + PostInstallFailure)
  else
    ReportLine('result: ok');
  SaveStringToFile(ExpandConstant('{app}\install-report.txt'), PostInstallReport, False);
end;

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
  if not FileExists(ExpandConstant('{tmp}\New-Config.ps1')) then
    ExtractTemporaryFile('New-Config.ps1');
  Result := FileExists(ExePath);
end;

{ Wraps a value as a single command-line argument, doubling any embedded quote
  so a channel name containing one cannot break the argument list. }
function Quote(const Value: String): String;
var
  I: Integer;
  Builder: String;
begin
  Builder := '"';
  for I := 1 to Length(Value) do
  begin
    { A doubled quote is what the command-line parser turns back into one. }
    if Value[I] = '"' then
      Builder := Builder + '""'
    else
      Builder := Builder + Value[I];
  end;
  Result := Builder + '"';
end;

function IsDigits(const Value: String): Boolean;
var
  I: Integer;
begin
  Result := Value <> '';
  for I := 1 to Length(Value) do
    if (Value[I] < '0') or (Value[I] > '9') then
    begin
      Result := False;
      exit;
    end;
end;

function IsLogLevel(const Value: String): Boolean;
var
  Normalized: String;
begin
  Normalized := LowerCase(Trim(Value));
  Result := (Normalized = 'error') or (Normalized = 'warn') or
            (Normalized = 'info') or (Normalized = 'debug') or
            (Normalized = 'trace');
end;

function IsAuthType(const Value: String): Boolean;
var
  Normalized: String;
begin
  Normalized := LowerCase(Trim(Value));
  Result := (Normalized = 'none') or (Normalized = 'user_password') or
            (Normalized = 'token') or (Normalized = 'credentials_file');
end;

{ --- service secrets ---------------------------------------------------- }
{ The relay reads credentials from environment variables, and a service does not
  see the user variables of whoever installed it. The SCM supports injecting an
  environment block into one service, which is what these functions fill in. }

function SecretPair(const Name, Value: String; var Lines, Names: String): Integer;
begin
  Result := 0;
  if (Trim(Name) = '') or (Value = '') then
    exit;

  Lines := Lines + Trim(Name) + '=' + Value + #13#10;
  if Names <> '' then
    Names := Names + ', ';
  Names := Names + Trim(Name);
  Result := 1;
end;

{ Collects the secrets the wizard is going to store, as NAME=value lines. }
function CollectSecrets(var Lines, Names: String): Integer;
var
  Auth: String;
begin
  Lines := '';
  Names := '';
  Result := 0;

  if InputIsRedis() then
    Result := Result + SecretPair(RedisInPage.Values[2], RedisInPage.Values[4], Lines, Names)
  else
  begin
    Auth := LowerCase(Trim(NatsInPage.Values[1]));
    if Auth = 'user_password' then
      Result := Result + SecretPair(NatsInPage.Values[3], NatsInPage.Values[7], Lines, Names)
    else if Auth = 'token' then
      Result := Result + SecretPair(NatsInPage.Values[4], NatsInPage.Values[7], Lines, Names);
  end;

  if OutputIsRedis() then
    Result := Result + SecretPair(RedisOutPage.Values[2], RedisOutPage.Values[5], Lines, Names)
  else
  begin
    Auth := LowerCase(Trim(NatsOutPage.Values[1]));
    if Auth = 'user_password' then
      Result := Result + SecretPair(NatsOutPage.Values[3], NatsOutPage.Values[8], Lines, Names)
    else if Auth = 'token' then
      Result := Result + SecretPair(NatsOutPage.Values[4], NatsOutPage.Values[8], Lines, Names);
  end;
end;

{ Names the configuration will reference but the wizard is not storing: those
  have to exist in the service environment already. }
function ReferencedWithoutValue(): String;
var
  Auth: String;
begin
  Result := '';

  if InputIsRedis() then
  begin
    if (Trim(RedisInPage.Values[2]) <> '') and (RedisInPage.Values[4] = '') then
      Result := Result + Trim(RedisInPage.Values[2]) + ', ';
  end
  else
  begin
    Auth := LowerCase(Trim(NatsInPage.Values[1]));
    if (Auth = 'user_password') and (Trim(NatsInPage.Values[3]) <> '') and (NatsInPage.Values[7] = '') then
      Result := Result + Trim(NatsInPage.Values[3]) + ', ';
    if (Auth = 'token') and (Trim(NatsInPage.Values[4]) <> '') and (NatsInPage.Values[7] = '') then
      Result := Result + Trim(NatsInPage.Values[4]) + ', ';
  end;

  if OutputIsRedis() then
  begin
    if (Trim(RedisOutPage.Values[2]) <> '') and (RedisOutPage.Values[5] = '') then
      Result := Result + Trim(RedisOutPage.Values[2]) + ', ';
  end
  else
  begin
    Auth := LowerCase(Trim(NatsOutPage.Values[1]));
    if (Auth = 'user_password') and (Trim(NatsOutPage.Values[3]) <> '') and (NatsOutPage.Values[8] = '') then
      Result := Result + Trim(NatsOutPage.Values[3]) + ', ';
    if (Auth = 'token') and (Trim(NatsOutPage.Values[4]) <> '') and (NatsOutPage.Values[8] = '') then
      Result := Result + Trim(NatsOutPage.Values[4]) + ', ';
  end;

  Result := Trim(Result);
  if (Result <> '') and (Result[Length(Result)] = ',') then
    Result := Trim(Copy(Result, 1, Length(Result) - 1));
end;

function StoreSecrets(const ServiceName: String): Boolean;
var
  Lines, Names, SecretFile, ScriptPath, ReportPath, Params: String;
  Count, ExitCode: Integer;
begin
  Result := True;
  SecretsMessage := '';

  Count := CollectSecrets(Lines, Names);
  if Count = 0 then
    exit;

  SecretFile := ExpandConstant('{tmp}\service-secrets.txt');
  if not SaveStringToFile(SecretFile, Lines, False) then
  begin
    SecretsMessage := SecretFile;
    Result := False;
    exit;
  end;

  ScriptPath := ExpandConstant('{app}\scripts\Set-ServiceSecrets.ps1');
  ReportPath := ExpandConstant('{tmp}\secrets-report.txt');
  DeleteFile(ReportPath);
  Params := '-ServiceName ' + Quote(ServiceName) +
            ' -SecretFile ' + Quote(SecretFile) +
            ' -ReportFile ' + Quote(ReportPath);
  Result := RunScript(ScriptPath, Params, ExitCode) and (ExitCode = 0);

  { The file held plaintext secrets: remove it either way. }
  DeleteFile(SecretFile);

  if Result then
    SecretsMessage := FmtMessage(CustomMessage('SecretsStored'), [Names])
  else
    SecretsMessage := ReadTextFile(ReportPath);
end;

function NewConfigReportPath(): String;
begin
  Result := ExpandConstant('{tmp}\newconfig-report.txt');
end;

{ Turns the wizard answers into a configuration file. Force overwrites; without
  it an existing file is kept, which is what an upgrade wants. }
function BuildConfig(const TargetPath: String; Force: Boolean): Boolean;
var
  ScriptPath, Params, ReportPath: String;
  InBackend, OutBackend, InSelector, OutPrefix, OutSuffix: String;
  ExitCode: Integer;
begin
  ScriptPath := ExpandConstant('{tmp}\New-Config.ps1');
  ReportPath := NewConfigReportPath();
  DeleteFile(ReportPath);

  if InputIsRedis() then InBackend := 'redis' else InBackend := 'nats';
  if OutputIsRedis() then OutBackend := 'redis' else OutBackend := 'nats';

  { The selector and the channel mapping live on the page of the backend that is
    actually in use; the other page keeps its default and is ignored. }
  if InputIsRedis() then
    InSelector := RedisInPage.Values[3]
  else
    InSelector := NatsInPage.Values[6];

  if OutputIsRedis() then
  begin
    OutPrefix := RedisOutPage.Values[3];
    OutSuffix := RedisOutPage.Values[4];
  end
  else
  begin
    OutPrefix := NatsOutPage.Values[6];
    OutSuffix := NatsOutPage.Values[7];
  end;

  Params :=
    ' -AppDir ' + Quote(ExpandConstant('{app}')) +
    ' -TargetPath ' + Quote(TargetPath) +
    ' -InBackend ' + InBackend +
    ' -OutBackend ' + OutBackend +
    ' -InEndpoint ' + Quote(RedisInPage.Values[0]) +
    ' -InDatabase ' + RedisInPage.Values[1] +
    ' -InPasswordEnv ' + Quote(RedisInPage.Values[2]) +
    ' -OutEndpoint ' + Quote(RedisOutPage.Values[0]) +
    ' -OutDatabase ' + RedisOutPage.Values[1] +
    ' -OutPasswordEnv ' + Quote(RedisOutPage.Values[2]) +
    ' -OutChannelPrefix ' + Quote(OutPrefix) +
    ' -OutChannelSuffix ' + Quote(OutSuffix) +
    ' -InNatsServers ' + Quote(NatsInPage.Values[0]) +
    ' -InNatsAuth ' + LowerCase(Trim(NatsInPage.Values[1])) +
    ' -InNatsUsername ' + Quote(NatsInPage.Values[2]) +
    ' -InNatsPasswordEnv ' + Quote(NatsInPage.Values[3]) +
    ' -InNatsTokenEnv ' + Quote(NatsInPage.Values[4]) +
    ' -InNatsCredentials ' + Quote(NatsInPage.Values[5]) +
    ' -OutNatsServers ' + Quote(NatsOutPage.Values[0]) +
    ' -OutNatsAuth ' + LowerCase(Trim(NatsOutPage.Values[1])) +
    ' -OutNatsUsername ' + Quote(NatsOutPage.Values[2]) +
    ' -OutNatsPasswordEnv ' + Quote(NatsOutPage.Values[3]) +
    ' -OutNatsTokenEnv ' + Quote(NatsOutPage.Values[4]) +
    ' -OutNatsCredentials ' + Quote(NatsOutPage.Values[5]) +
    ' -InSelector ' + Quote(InSelector) +
    ' -IntervalMs ' + TuningPage.Values[0] +
    ' -DedupTtlMs ' + TuningPage.Values[1] +
    ' -StatusPort ' + TuningPage.Values[2] +
    ' -LogLevel ' + TuningPage.Values[3] +
    ' -ReportFile ' + Quote(ReportPath);

  if Force then
    Params := Params + ' -Force';

  Result := RunScript(ScriptPath, Params, ExitCode) and (ExitCode = 0);
end;

function ValidateChosenConfig(): Boolean;
var
  ExePath, ScriptPath, TargetPath, ValidatePath, ReportPath, Params, Report, Unstored: String;
  ExitCode: Integer;
  IsDraft: Boolean;
begin
  Result := True;
  { A silent install does not walk the pages the way a user does: this check is a
    conversation (it offers to install anyway) and a page that refuses to be left
    would keep an unattended install waiting forever. The file the service will
    actually use is checked after it is written instead. }
  if WizardSilent() then
    exit;
  TargetPath := ConfigPage.Values[0];

  if TargetPath = '' then
  begin
    MsgBox(CustomMessage('ValidateMissing'), mbError, MB_OK);
    Result := False;
    exit;
  end;

  { A referenced credential that the wizard is not going to store has to exist in
    the service environment already, otherwise the relay exits at startup. That
    is worth a stop, not a surprise in the event log. }
  if WantsConfiguration() then
  begin
    Unstored := ReferencedWithoutValue();
    if (Unstored <> '') and
       (MsgBox(FmtMessage(CustomMessage('SecretNotSetWarning'), [Unstored]), mbConfirmation, MB_YESNO) = IDNO) then
    begin
      Result := False;
      exit;
    end;
  end;

  if UsesExistingFile() and (not FileExists(TargetPath)) then
  begin
    { This mode exists to take a file the operator already has, so a missing one
      is a mistake worth stopping for. }
    MsgBox(FmtMessage(CustomMessage('ConfigMissing'), [TargetPath]), mbError, MB_OK);
    Result := False;
    exit;
  end;

  if not EnsureValidatorExtracted() then
    exit; { Cannot run anything here; the service still reports the real error. }

  IsDraft := WantsConfiguration() and (not FileExists(TargetPath));
  if IsDraft then
  begin
    { Nothing exists at the destination yet, which is the normal case: the wizard
      generates the file from the answers. Build a draft in the temporary
      directory, with the same content and the same absolute paths the real file
      will have, and let the relay judge that. }
    ValidatePath := ExpandConstant('{tmp}\config-draft.json');
    DeleteFile(ValidatePath);
    if not BuildConfig(ValidatePath, True) then
    begin
      Report := ReadTextFile(NewConfigReportPath());
      if Report = '' then
        Report := 'New-Config.ps1 produced no output';
      MsgBox(FmtMessage(CustomMessage('GenerateFailed'), [Report]), mbError, MB_OK);
      Result := False;
      exit;
    end;
  end
  else
    ValidatePath := TargetPath;

  ExePath := ExpandConstant('{tmp}\{#AppExeName}.exe');
  ScriptPath := ExpandConstant('{tmp}\validate-config.ps1');
  ReportPath := ExpandConstant('{tmp}\validate-report.txt');
  DeleteFile(ReportPath);

  Params := '-ConfigPath ' + Quote(ValidatePath) + ' -ExePath ' + Quote(ExePath) +
            ' -ReportFile ' + Quote(ReportPath);
  if not RunScript(ScriptPath, Params, ExitCode) then
    exit; { PowerShell unavailable: do not block the install. }

  if ExitCode <> 0 then
  begin
    Report := ReadTextFile(ReportPath);
    if MsgBox(FmtMessage(CustomMessage('ValidateFailed'), [Report]), mbError, MB_YESNO) = IDNO then
      Result := False;
  end
  else if IsDraft then
    MsgBox(FmtMessage(CustomMessage('ConfigGenerated'), [TargetPath]), mbInformation, MB_OK)
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
  if (not RunScript(ScriptPath, Params, ExitCode)) or (ExitCode <> 0) then
  begin
    { Without the stop the copy is deferred to the next reboot and the service
      keeps executing the old binary while the setup reports success, so this
      stops the installation instead of warning about it. }
    Result := FmtMessage(CustomMessage('ServiceStopFailed'), [ReadTextFile(ReportPath)]);
    exit;
  end;
  ServiceWasStopped := True;
end;

procedure InitializeWizard();
begin
  { The configuration wizard. A menu picks what to do, then only the pages that
    combination needs are shown (see ShouldSkipPage), and the answers are turned
    into config.json by New-Config.ps1. The result is validated by the relay
    itself before anything is written to Program Files. }

  ModePage := CreateInputOptionPage(wpSelectTasks,
    CustomMessage('PageModeTitle'), '', CustomMessage('PageModeDesc'), True, False);
  ModePage.Add(CustomMessage('ModeRedisRedis'));
  ModePage.Add(CustomMessage('ModeNatsNats'));
  ModePage.Add(CustomMessage('ModeRedisNats'));
  ModePage.Add(CustomMessage('ModeNatsRedis'));
  ModePage.Add(CustomMessage('ModeExisting'));
  ModePage.Add(CustomMessage('ModeInstallOnly'));
  { An option page reports -1 until something is selected, and a silent install
    never selects anything: the first entry has to be the explicit default. }
  ModePage.SelectedValueIndex := 0;

  RedisInPage := CreateInputQueryPage(ModePage.ID,
    CustomMessage('PageInputTitle'), '', CustomMessage('PageInputDesc'));
  RedisInPage.Add(CustomMessage('InEndpointLabel'), False);
  RedisInPage.Add(CustomMessage('InDatabaseLabel'), False);
  RedisInPage.Add(CustomMessage('InPasswordEnvLabel'), False);
  RedisInPage.Add(CustomMessage('InPatternLabel'), False);
  { Masked field: the value is stored in the service's own environment, not in
    the configuration file, and not on a command line. }
  RedisInPage.Add(CustomMessage('SecretValueLabel'), True);
  RedisInPage.Values[0] := 'localhost:6379';
  RedisInPage.Values[1] := '0';
  RedisInPage.Values[3] := '*';

  RedisOutPage := CreateInputQueryPage(RedisInPage.ID,
    CustomMessage('PageOutputTitle'), '', CustomMessage('PageOutputDesc'));
  RedisOutPage.Add(CustomMessage('OutEndpointLabel'), False);
  RedisOutPage.Add(CustomMessage('OutDatabaseLabel'), False);
  RedisOutPage.Add(CustomMessage('OutPasswordEnvLabel'), False);
  RedisOutPage.Add(CustomMessage('OutChannelPrefixLabel'), False);
  RedisOutPage.Add(CustomMessage('OutChannelSuffixLabel'), False);
  RedisOutPage.Add(CustomMessage('SecretValueLabel'), True);
  RedisOutPage.Values[0] := 'localhost:6379';
  RedisOutPage.Values[1] := '0';
  { Defaulted, not left empty: with the input and the output on the same Redis
    server the relay refuses a configuration without a prefix or a suffix, and
    the sensible default is to namespace the published channels. }
  RedisOutPage.Values[3] := 'conflated:';

  NatsInPage := CreateInputQueryPage(RedisOutPage.ID,
    CustomMessage('PageNatsInTitle'), '', CustomMessage('PageNatsInDesc'));
  NatsInPage.Add(CustomMessage('NatsServersLabel'), False);
  NatsInPage.Add(CustomMessage('NatsAuthLabel'), False);
  NatsInPage.Add(CustomMessage('NatsUsernameLabel'), False);
  NatsInPage.Add(CustomMessage('NatsPasswordEnvLabel'), False);
  NatsInPage.Add(CustomMessage('NatsTokenEnvLabel'), False);
  NatsInPage.Add(CustomMessage('NatsCredentialsLabel'), False);
  NatsInPage.Add(CustomMessage('NatsSubjectLabel'), False);
  NatsInPage.Add(CustomMessage('SecretValueLabel'), True);
  NatsInPage.Values[0] := 'nats://localhost:4222';
  NatsInPage.Values[1] := 'none';
  NatsInPage.Values[6] := '>';

  NatsOutPage := CreateInputQueryPage(NatsInPage.ID,
    CustomMessage('PageNatsOutTitle'), '', CustomMessage('PageNatsOutDesc'));
  NatsOutPage.Add(CustomMessage('NatsServersLabel'), False);
  NatsOutPage.Add(CustomMessage('NatsAuthLabel'), False);
  NatsOutPage.Add(CustomMessage('NatsUsernameLabel'), False);
  NatsOutPage.Add(CustomMessage('NatsPasswordEnvLabel'), False);
  NatsOutPage.Add(CustomMessage('NatsTokenEnvLabel'), False);
  NatsOutPage.Add(CustomMessage('NatsCredentialsLabel'), False);
  NatsOutPage.Add(CustomMessage('OutChannelPrefixLabel'), False);
  NatsOutPage.Add(CustomMessage('OutChannelSuffixLabel'), False);
  NatsOutPage.Add(CustomMessage('SecretValueLabel'), True);
  NatsOutPage.Values[0] := 'nats://localhost:4222';
  NatsOutPage.Values[1] := 'none';

  TuningPage := CreateInputQueryPage(NatsOutPage.ID,
    CustomMessage('PageTuningTitle'), '', CustomMessage('PageTuningDesc'));
  TuningPage.Add(CustomMessage('TunIntervalLabel'), False);
  TuningPage.Add(CustomMessage('TunTtlLabel'), False);
  TuningPage.Add(CustomMessage('TunStatusLabel'), False);
  TuningPage.Add(CustomMessage('TunLogLevelLabel'), False);
  TuningPage.Values[0] := '250';
  TuningPage.Values[1] := '5000';
  TuningPage.Values[2] := '9090';
  TuningPage.Values[3] := 'info';

  ConfigPage := CreateInputFilePage(TuningPage.ID,
    CustomMessage('PageConfigTitle'),
    CustomMessage('PageConfigSubtitle'),
    CustomMessage('PageConfigDesc'));
  ConfigPage.Add(CustomMessage('ConfigPathLabel'),
    'JSON configuration files|*.json|All files|*.*', '.json');
  { The default value is NOT set here: the app directory constant only exists
    after the user has passed the directory page, and expanding it this early
    aborts the wizard with a runtime error. CurPageChanged fills it in. }
end;

function ShouldSkipPage(PageID: Integer): Boolean;
begin
  Result := False;

  { Install the executable only: no questions, and no configuration file. }
  if InstallsOnly() then
  begin
    Result := (PageID = RedisInPage.ID) or (PageID = RedisOutPage.ID) or
              (PageID = NatsInPage.ID) or (PageID = NatsOutPage.ID) or
              (PageID = TuningPage.ID) or (PageID = ConfigPage.ID);
    exit;
  end;

  { An existing file: no questions, but the destination page is where it is
    chosen and checked. }
  if UsesExistingFile() then
  begin
    Result := (PageID = RedisInPage.ID) or (PageID = RedisOutPage.ID) or
              (PageID = NatsInPage.ID) or (PageID = NatsOutPage.ID) or
              (PageID = TuningPage.ID);
    exit;
  end;

  if PageID = RedisInPage.ID then
    Result := not InputIsRedis()
  else if PageID = RedisOutPage.ID then
    Result := not OutputIsRedis()
  else if PageID = NatsInPage.ID then
    Result := InputIsRedis()
  else if PageID = NatsOutPage.ID then
    Result := OutputIsRedis();
end;

function NextButtonClick(CurPageID: Integer): Boolean;
begin
  Result := True;

  if CurPageID = RedisInPage.ID then
  begin
    if not IsDigits(RedisInPage.Values[1]) then
    begin
      MsgBox(FmtMessage(CustomMessage('BadNumber'), [CustomMessage('InDatabaseLabel')]), mbError, MB_OK);
      Result := False;
      exit;
    end;
    { An empty selector would be rejected by the relay at startup, so it is
      caught here where the user can still fix it. }
    if Trim(RedisInPage.Values[3]) = '' then
    begin
      MsgBox(CustomMessage('SelectorMissing'), mbError, MB_OK);
      Result := False;
    end;
    exit;
  end;

  if CurPageID = RedisOutPage.ID then
  begin
    if not IsDigits(RedisOutPage.Values[1]) then
    begin
      MsgBox(FmtMessage(CustomMessage('BadNumber'), [CustomMessage('OutDatabaseLabel')]), mbError, MB_OK);
      Result := False;
    end;
    exit;
  end;

  if CurPageID = NatsInPage.ID then
  begin
    if not IsAuthType(NatsInPage.Values[1]) then
    begin
      MsgBox(CustomMessage('BadAuthType'), mbError, MB_OK);
      Result := False;
      exit;
    end;
    if Trim(NatsInPage.Values[6]) = '' then
    begin
      MsgBox(CustomMessage('SelectorMissing'), mbError, MB_OK);
      Result := False;
    end;
    exit;
  end;

  if CurPageID = NatsOutPage.ID then
  begin
    if not IsAuthType(NatsOutPage.Values[1]) then
    begin
      MsgBox(CustomMessage('BadAuthType'), mbError, MB_OK);
      Result := False;
    end;
    exit;
  end;

  if CurPageID = TuningPage.ID then
  begin
    if not IsDigits(TuningPage.Values[0]) then
    begin
      MsgBox(FmtMessage(CustomMessage('BadNumber'), [CustomMessage('TunIntervalLabel')]), mbError, MB_OK);
      Result := False;
      exit;
    end;
    if not IsDigits(TuningPage.Values[1]) then
    begin
      MsgBox(FmtMessage(CustomMessage('BadNumber'), [CustomMessage('TunTtlLabel')]), mbError, MB_OK);
      Result := False;
      exit;
    end;
    if not IsDigits(TuningPage.Values[2]) then
    begin
      MsgBox(FmtMessage(CustomMessage('BadNumber'), [CustomMessage('TunStatusLabel')]), mbError, MB_OK);
      Result := False;
      exit;
    end;
    if not IsLogLevel(TuningPage.Values[3]) then
    begin
      MsgBox(CustomMessage('BadLogLevel'), mbError, MB_OK);
      Result := False;
    end;
    exit;
  end;

  if CurPageID = ConfigPage.ID then
    Result := ValidateChosenConfig();
end;

procedure CurStepChanged(CurStep: TSetupStep);
var
  ScriptPath, Params, ReportPath, Report, StartupType, ExePath, ConfigPath: String;
  ExitCode: Integer;
begin
  if CurStep <> ssPostInstall then
    exit;

  ReportPath := ExpandConstant('{tmp}\postinstall-report.txt');
  ExePath := ExpandConstant('{app}\{#AppExeName}.exe');
  ConfigPath := EffectiveConfigPath();
  PostInstallFailure := '';
  PostInstallReport := '';

  { config.json from the wizard answers, with absolute paths. An existing file is
    kept, which is what an upgrade wants; the draft was already validated by the
    relay on the last wizard page. }
  EnsureValidatorExtracted();

  if InstallsOnly() then
  begin
    { No configuration was asked for, so there is nothing for a service to run:
      leave the examples and the helper scripts in place and let the operator
      finish by hand. }
    ReportLine('mode: files only, no configuration and no service');
    FlushReport();
    exit;
  end;

  ReportLine('configuration: ' + ConfigPath);

  if ShouldGenerateConfig() then
  begin
    if not BuildConfig(ConfigPath, False) then
    begin
      Report := ReadTextFile(NewConfigReportPath());
      if Report = '' then
        Report := 'New-Config.ps1 produced no output';
      FailOrReport(FmtMessage(CustomMessage('GenerateFailed'), [Report]));
      FlushReport();
      exit;
    end;
    ReportLine('configuration: generated');
  end
  else
    ReportLine('configuration: kept');

  { An unattended install never went through the wizard's validation, so the file
    the service is about to use is checked here instead. }
  if WizardSilent() then
  begin
    ScriptPath := ExpandConstant('{app}\scripts\validate-config.ps1');
    Params := '-ConfigPath ' + Quote(ConfigPath) + ' -ExePath ' + Quote(ExePath) +
              ' -ReportFile ' + Quote(ReportPath);
    DeleteFile(ReportPath);
    if (not RunScript(ScriptPath, Params, ExitCode)) or (ExitCode <> 0) then
    begin
      Report := ReadTextFile(ReportPath);
      FailOrReport(FmtMessage(CustomMessage('ConfigRejected'), [Report]));
      FlushReport();
      exit;
    end;
    ReportLine('configuration: accepted by the relay');
  end;

  if not ShouldInstallService() then
  begin
    ReportLine('service: not requested');
    { A service stopped for the upgrade is still registered, so put the machine
      back the way we found it instead of silently leaving the relay down. }
    if ServiceWasStopped then
    begin
      ScriptPath := ExpandConstant('{app}\scripts\service.ps1');
      Params := '-Action start -ReportFile "' + ReportPath + '"';
      DeleteFile(ReportPath);
      RunScript(ScriptPath, Params, ExitCode);
    end;
    FlushReport();
    exit;
  end;

  StartupType := ServiceStartupType();
  ReportLine('service: {#ServiceName} (' + StartupType + ')');

  ScriptPath := ExpandConstant('{app}\scripts\service.ps1');
  Params := '-Action install -ExePath ' + Quote(ExePath) +
            ' -ConfigPath ' + Quote(ConfigPath) +
            ' -StartupType ' + StartupType +
            ' -ReportFile ' + Quote(ReportPath);
  DeleteFile(ReportPath);

  if (not RunScript(ScriptPath, Params, ExitCode)) or (ExitCode <> 0) then
  begin
    Report := ReadTextFile(ReportPath);
    if Report = '' then
      Report := 'service.ps1 produced no report';
    FailOrReport(FmtMessage(CustomMessage('ServiceInstallFailed'), [Report]));
    FlushReport();
    exit;
  end;
  ReportLine('service: installed');

  Params := '-Action start -ReportFile "' + ReportPath + '"';
  DeleteFile(ReportPath);
  if (not RunScript(ScriptPath, Params, ExitCode)) or (ExitCode <> 0) then
  begin
    Report := ReadTextFile(ReportPath);
    if Report = '' then
      Report := 'service.ps1 produced no report';
    FailOrReport(FmtMessage(CustomMessage('ServiceStartFailed'), [Report]));
    FlushReport();
    exit;
  end;
  ReportLine('service: started');

  ServiceWasInstalled := True;

  { The credentials go into the service's own environment block, so they are
    injected when the service starts below — and are visible to nothing else on
    the machine. }
  if not StoreSecrets('{#ServiceName}') then
  begin
    FailOrReport(FmtMessage(CustomMessage('SecretsFailed'), [SecretsMessage]));
    FlushReport();
    exit;
  end;

  { `sc start` only proves that the service manager accepted the request. Ask the
    relay whether it actually came up: a service that exits during startup would
    otherwise be reported as a success. The service is registered either way, so
    this is a report and an exit code, not a rollback. }
  ScriptPath := ExpandConstant('{app}\scripts\verify-service.ps1');
  Params := '-ConfigPath ' + Quote(ConfigPath) +
            ' -TimeoutSeconds 20 -ReportFile ' + Quote(ReportPath);
  DeleteFile(ReportPath);
  if RunScript(ScriptPath, Params, ExitCode) and (ExitCode = 0) then
  begin
    ServiceVerification := CustomMessage('ServiceAlive');
    ReportLine('relay: running');
  end
  else
  begin
    ServiceVerification := FmtMessage(CustomMessage('ServiceNotServing'), ['20']);
    PostInstallFailure := ServiceVerification;
    ReportLine('relay: did not report itself as running within 20s');
  end;

  FlushReport();
end;

procedure CurPageChanged(CurPageID: Integer);
var
  Extra, Suggested: String;
begin
  if CurPageID = ConfigPage.ID then
  begin
    { Computed here, not in InitializeWizard: the app constant only exists once
      the directory page has been passed. Re-suggested when the directory
      changes, as long as the user has not typed a path of their own. }
    Suggested := ExpandConstant('{app}\config.json');
    { /CONFIG points at a file that already exists: show that one, so an
      interactive run and an unattended one agree on the source. }
    if SilentConfigPath <> '' then
      Suggested := SilentConfigPath;
    if (ConfigPage.Values[0] = '') or (ConfigPage.Values[0] = ConfigDefault) then
      ConfigPage.Values[0] := Suggested;
    ConfigDefault := Suggested;
    exit;
  end;

  if CurPageID <> wpFinished then
    exit;

  Extra := CustomMessage('FinishedLine');
  if InstallsOnly() then
    Extra := CustomMessage('FinishedNoConfig') + #13#10 + #13#10 + Extra;
  if SecretsMessage <> '' then
    Extra := SecretsMessage + #13#10 + #13#10 + Extra;
  if ServiceVerification <> '' then
    Extra := ServiceVerification + #13#10 + #13#10 + Extra;
  if ServiceWasInstalled then
    Extra := CustomMessage('ServiceReady') + #13#10 + #13#10 + Extra;
  WizardForm.FinishedLabel.Caption := Extra + #13#10#13#10 + WizardForm.FinishedLabel.Caption;
end;

function InitializeUninstall(): Boolean;
begin
  ServiceWasInstalled := False;
  { A silent uninstall has nobody to answer, and deleting the configuration is the
    destructive answer: keep it unless asked otherwise. }
  if UninstallSilent() then
    KeepConfig := True
  else
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
      SuppressibleMsgBox(FmtMessage(CustomMessage('UninstServiceFailed'), [Report]), mbError, MB_OK, IDOK);
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
