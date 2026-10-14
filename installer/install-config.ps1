<#
.SYNOPSIS
    Creates (or refreshes) the relay's config.json from a shipped example.

.DESCRIPTION
    The relay resolves relative paths (logging.directory, instance_lock.path,
    status.path) against the process working directory. A Windows service runs
    with its working directory set to System32, so a config.json installed with
    the example's relative paths would write its logs and lock file there.

    This script rewrites those three fields to absolute paths under the install
    directory. It never overwrites an existing config.json unless -Force is
    given, so an upgrade keeps whatever the operator configured.

.PARAMETER AppDir
    Install directory that receives config.json (and the logs/ subdirectory).

.PARAMETER ExamplePath
    The example configuration to start from.

.PARAMETER TargetPath
    Defaults to <AppDir>\config.json.

.PARAMETER Force
    Overwrite an existing config.json instead of leaving it untouched.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$AppDir,
    [Parameter(Mandatory = $true)][string]$ExamplePath,
    [string]$TargetPath,
    [string]$ReportFile,
    [switch]$Force
)

$ErrorActionPreference = 'Stop'

if (-not $TargetPath) { $TargetPath = Join-Path $AppDir 'config.json' }

function Write-Report {
    param([string]$Text)
    # UTF-8 without BOM; see the note in validate-config.ps1.
    if ($ReportFile) { [System.IO.File]::WriteAllText($ReportFile, $Text) }
}

if ((Test-Path -LiteralPath $TargetPath) -and -not $Force) {
    $message = "kept existing config.json: $TargetPath"
    Write-Host $message
    Write-Report $message
    exit 0
}

if (-not (Test-Path -LiteralPath $ExamplePath -PathType Leaf)) {
    $message = "example configuration not found: $ExamplePath"
    Write-Host $message -ForegroundColor Red
    Write-Report $message
    exit 2
}

$config = Get-Content -LiteralPath $ExamplePath -Raw -Encoding UTF8 | ConvertFrom-Json

# Absolute paths: the service's working directory is not the install directory.
$absolute = @{
    Directory   = Join-Path $AppDir 'logs'
    Lock        = Join-Path $AppDir 'redis-conflated-pubsub.lock'
    Status      = Join-Path $AppDir 'status.json'
}

if ($config.PSObject.Properties.Name -contains 'logging' -and $config.logging) {
    $config.logging.directory = $absolute.Directory
}
if ($config.PSObject.Properties.Name -contains 'instance_lock' -and $config.instance_lock) {
    $config.instance_lock.path = $absolute.Lock
}
if ($config.PSObject.Properties.Name -contains 'status' -and $config.status) {
    if ($config.status.PSObject.Properties.Name -contains 'path') {
        $config.status.path = $absolute.Status
    }
}

New-Item -ItemType Directory -Force -Path $absolute.Directory | Out-Null

# -Depth is required: PowerShell 5.1 truncates nested objects at depth 2.
# File.WriteAllText, not Set-Content: a UTF-8 BOM makes serde_json reject the file.
[System.IO.File]::WriteAllText($TargetPath, ($config | ConvertTo-Json -Depth 100))

$message = "config.json written: $TargetPath (logs: $($absolute.Directory))"
Write-Host $message -ForegroundColor Green
Write-Report $message
exit 0
