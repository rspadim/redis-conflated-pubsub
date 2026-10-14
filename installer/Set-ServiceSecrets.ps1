<#
.SYNOPSIS
    Gives the relay's service its own environment variables.

.DESCRIPTION
    The relay takes Redis and NATS credentials from environment variables
    (`password_env`, `token_env`), and a Windows service does not see the user
    variables of whoever installed it. Setting a machine-wide variable would
    work, but it would make the secret readable to every process on the box.

    The service control manager supports a third option, and it is the one used
    here: a REG_MULTI_SZ value named `Environment` under the service's own
    registry key, which the SCM injects into that service's process environment
    and nothing else. The secret is readable only by administrators, and only the
    relay sees it.

    The value is merged rather than replaced, so re-running the installer does
    not wipe variables an operator added by hand.

.PARAMETER SecretFile
    File with one `NAME=value` per line. Written by the installer to a temporary
    file instead of passing secrets on the command line, where any process could
    read them from the process list.

.PARAMETER Verify
    Report which of the given names are missing from the service environment
    instead of writing anything. Exit code 3 when at least one is missing.

.EXAMPLE
    .\Set-ServiceSecrets.ps1 -SecretFile secrets.txt -ServiceName redis-conflated-pubsub

.EXAMPLE
    .\Set-ServiceSecrets.ps1 -Verify -ServiceName redis-conflated-pubsub -Names 'REDIS_PASSWORD'
#>
[CmdletBinding(DefaultParameterSetName = 'Write')]
param(
    [Parameter(Mandatory = $true)][string]$ServiceName,

    [Parameter(ParameterSetName = 'Write', Mandatory = $true)][string]$SecretFile,
    [string]$ReportFile,

    [Parameter(ParameterSetName = 'Verify', Mandatory = $true)][switch]$Verify,
    [Parameter(ParameterSetName = 'Verify', Mandatory = $true)][string]$Names,

    # Test seam: point the script at any registry key instead of the service key.
    [string]$ServiceKey
)

$ErrorActionPreference = 'Stop'

function Write-Report {
    param([string]$Text)
    if ($ReportFile) { [System.IO.File]::WriteAllText($ReportFile, $Text) }
}

if (-not $ServiceKey) {
    $ServiceKey = "HKLM:\SYSTEM\CurrentControlSet\Services\$ServiceName"
}

if (-not (Test-Path -LiteralPath $ServiceKey)) {
    $message = "the service key does not exist yet: $ServiceKey"
    Write-Host $message -ForegroundColor Red
    Write-Report $message
    exit 2
}

function Get-ServiceEnvironment {
    $value = (Get-ItemProperty -LiteralPath $ServiceKey -Name 'Environment' -ErrorAction SilentlyContinue).Environment
    if ($null -eq $value) { return @() }
    # REG_MULTI_SZ comes back as a single string with embedded NULs in some
    # PowerShell versions and as an array in others.
    return @($value -split "`0" | Where-Object { $_ -ne '' })
}

if ($Verify) {
    $existing = Get-ServiceEnvironment
    $missing = @()
    foreach ($name in ($Names -split ',' | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne '' })) {
        $found = $false
        foreach ($entry in $existing) {
            if ($entry -like "$name=*") { $found = $true; break }
        }
        if (-not $found) { $missing += $name }
    }

    if ($missing.Count -eq 0) {
        $message = "service environment is complete: $($Names -split ',' -join ', ')"
        Write-Host $message -ForegroundColor Green
        Write-Report $message
        exit 0
    }

    $message = "missing from the service environment: $($missing -join ', ')"
    Write-Host $message -ForegroundColor Yellow
    Write-Report $message
    exit 3
}

if (-not (Test-Path -LiteralPath $SecretFile -PathType Leaf)) {
    $message = "the secrets file does not exist: $SecretFile"
    Write-Host $message -ForegroundColor Red
    Write-Report $message
    exit 4
}

$pairs = @()
foreach ($line in (Get-Content -LiteralPath $SecretFile -Encoding UTF8)) {
    $trimmed = $line.Trim()
    if ($trimmed -eq '' -or $trimmed.StartsWith(';')) { continue }
    $separator = $trimmed.IndexOf('=')
    if ($separator -lt 1) { continue }
    $pairs += [pscustomobject]@{
        Name  = $trimmed.Substring(0, $separator)
        Value = $trimmed.Substring($separator + 1)
    }
}

if ($pairs.Count -eq 0) {
    $message = 'nothing to store: the secrets file had no NAME=value lines'
    Write-Host $message
    Write-Report $message
    exit 0
}

# Merge: keep whatever the key already holds, replace the entries we are setting.
$environment = @(Get-ServiceEnvironment)
foreach ($pair in $pairs) {
    $environment = @($environment | Where-Object { $_ -notlike "$($pair.Name)=*" })
    $environment += "$($pair.Name)=$($pair.Value)"
}

try {
    New-ItemProperty -LiteralPath $ServiceKey -Name 'Environment' -PropertyType MultiString -Value $environment -Force | Out-Null
} catch {
    $message = "the service environment could not be written: $($_.Exception.Message)"
    Write-Host $message -ForegroundColor Red
    Write-Report $message
    exit 5
}

$stored = ($pairs | ForEach-Object { $_.Name }) -join ', '
$message = "stored in the service environment: $stored"
Write-Host $message -ForegroundColor Green
Write-Report $message
exit 0
