<#
.SYNOPSIS
    Runs the relay's --check-config against a configuration file.

.DESCRIPTION
    Wraps `redis-conflated-pubsub --check-config --config <file>` so the
    installer and the Start Menu shortcut can show the same answer the service
    will get at startup. Exit code is the relay's own exit code: 0 valid,
    non-zero rejected.

.PARAMETER ConfigPath
    Path to the JSON configuration to validate.

.PARAMETER ExePath
    Path to redis-conflated-pubsub.exe. Defaults to the copy next to the
    scripts directory, which is where the installer puts it.

.PARAMETER ReportFile
    Optional. Also writes the captured output to this file, so a GUI caller
    (the Inno Setup wizard) can display it.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$ConfigPath,
    [string]$ExePath,
    [string]$ReportFile
)

$ErrorActionPreference = 'Stop'

if (-not $ExePath) {
    $ExePath = Join-Path (Split-Path -Parent $PSScriptRoot) 'redis-conflated-pubsub.exe'
}

function Write-Report {
    param([string]$Text)
    # File.WriteAllText writes UTF-8 without a BOM. Set-Content -Encoding UTF8 in
    # Windows PowerShell 5.1 prepends one, so the wizard would show a stray
    # character at the start of every report.
    if ($ReportFile) {
        [System.IO.File]::WriteAllText($ReportFile, $Text)
    }
}

if (-not (Test-Path -LiteralPath $ConfigPath -PathType Leaf)) {
    $message = "Configuration file not found: $ConfigPath"
    Write-Host $message -ForegroundColor Red
    Write-Report $message
    exit 2
}

if (-not (Test-Path -LiteralPath $ExePath -PathType Leaf)) {
    $message = "Executable not found: $ExePath"
    Write-Host $message -ForegroundColor Red
    Write-Report $message
    exit 2
}

# The relay writes diagnostics to stderr; merge them with stdout so the report
# shows the full reason a configuration was rejected.
$output = & $ExePath --check-config --config $ConfigPath 2>&1
$code = $LASTEXITCODE
$text = ($output | Out-String).Trim()

if ($code -eq 0) {
    Write-Host "Configuration is valid: $ConfigPath" -ForegroundColor Green
    if ($text) { Write-Host $text }
    Write-Report "Configuration is valid: $ConfigPath"
} else {
    Write-Host "Configuration was rejected (exit $code): $ConfigPath" -ForegroundColor Red
    if ($text) { Write-Host $text }
    Write-Report ("Configuration was rejected (exit {0}): {1}`r`n{2}" -f $code, $ConfigPath, $text)
}

exit $code
