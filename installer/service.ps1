<#
.SYNOPSIS
    Installs, removes or controls the relay's Windows service.

.DESCRIPTION
    Registers redis-conflated-pubsub.exe as a Windows service through sc.exe.
    The service is created with the configuration file on the command line and
    restart-on-failure actions, because the relay is a daemon: if it dies, the
    Pub/Sub feed stops flowing and nothing else restarts it.

    The executable keeps its own working directory handling, so the configuration
    passed here must use absolute paths (installer\install-config.ps1 does that).

.PARAMETER Action
    install, uninstall, start, stop, restart or status.

.PARAMETER StartupType
    auto (default), delayed-auto or demand.

.EXAMPLE
    .\service.ps1 -Action install -ExePath 'C:\Program Files\Conflated Pub/Sub\redis-conflated-pubsub.exe' -ConfigPath 'C:\Program Files\Conflated Pub/Sub\config.json'
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('install', 'uninstall', 'start', 'stop', 'restart', 'status')]
    [string]$Action,

    [string]$Name = 'redis-conflated-pubsub',
    [string]$DisplayName = 'Conflated Pub/Sub relay',
    [string]$ExePath,
    [string]$ConfigPath,
    [ValidateSet('auto', 'delayed-auto', 'demand')][string]$StartupType = 'auto',
    [string]$ReportFile
)

$ErrorActionPreference = 'Stop'

function Write-Report {
    param([string]$Text)
    if ($ReportFile) { Set-Content -LiteralPath $ReportFile -Value $Text -Encoding UTF8 }
}

function Invoke-Sc {
    param([string[]]$Arguments)
    $output = & sc.exe @Arguments 2>&1
    return [pscustomobject]@{ Code = $LASTEXITCODE; Output = (($output | Out-String).Trim()) }
}

function Get-ServiceState {
    $result = Invoke-Sc @('query', $Name)
    if ($result.Code -ne 0) { return $null }
    foreach ($line in ($result.Output -split "`r?`n")) {
        if ($line -match 'STATE\s+:\s+\d+\s+(\w+)') { return $Matches[1] }
    }
    return 'unknown'
}

switch ($Action) {
    'install' {
        if (-not $ExePath) { throw '-ExePath is required for install' }
        if (-not $ConfigPath) { throw '-ConfigPath is required for install' }
        if (-not (Test-Path -LiteralPath $ExePath -PathType Leaf)) { throw "executable not found: $ExePath" }
        if (-not (Test-Path -LiteralPath $ConfigPath -PathType Leaf)) { throw "configuration not found: $ConfigPath" }

        $binaryPath = '"{0}" --config "{1}"' -f $ExePath, $ConfigPath
        $existing = Get-ServiceState

        if ($existing) {
            # Reinstall or upgrade: keep the service, point it at the current files.
            $result = Invoke-Sc @('config', $Name, 'binPath=', $binaryPath, 'start=', $StartupType, 'DisplayName=', "`"$DisplayName`"")
        } else {
            $result = Invoke-Sc @('create', $Name, 'binPath=', $binaryPath, 'start=', $StartupType, 'DisplayName=', "`"$DisplayName`"")
        }
        if ($result.Code -ne 0) {
            throw ("sc.exe failed ({0}): {1}" -f $result.Code, $result.Output)
        }

        # Cosmetic and operational extras: never fatal.
        Invoke-Sc @('description', $Name, "`"Conflated Pub/Sub relay: forwards Pub/Sub messages between Redis and Core NATS with per-output conflation.`"") | Out-Null
        Invoke-Sc @('failure', $Name, 'reset=', '86400', 'actions=', 'restart/5000/restart/15000/restart/60000') | Out-Null

        $message = "service installed: $Name ($StartupType) -> $binaryPath"
        Write-Host $message -ForegroundColor Green
        Write-Report $message
    }

    'uninstall' {
        $state = Get-ServiceState
        if (-not $state) {
            $message = "service not installed: $Name"
            Write-Host $message
            Write-Report $message
            exit 0
        }

        Invoke-Sc @('stop', $Name) | Out-Null
        for ($i = 0; $i -lt 30; $i++) {
            if ((Get-ServiceState) -eq 'STOPPED') { break }
            Start-Sleep -Milliseconds 500
        }

        $result = Invoke-Sc @('delete', $Name)
        if ($result.Code -ne 0) {
            throw ("sc.exe delete failed ({0}): {1}" -f $result.Code, $result.Output)
        }

        $message = "service removed: $Name"
        Write-Host $message -ForegroundColor Green
        Write-Report $message
    }

    'start' {
        if (-not (Get-ServiceState)) { throw "service not installed: $Name" }
        $result = Invoke-Sc @('start', $Name)
        if ($result.Code -ne 0) { throw ("sc.exe start failed ({0}): {1}" -f $result.Code, $result.Output) }
        Write-Host "service started: $Name" -ForegroundColor Green
    }

    'stop' {
        if (-not (Get-ServiceState)) { throw "service not installed: $Name" }
        $result = Invoke-Sc @('stop', $Name)
        if ($result.Code -ne 0) { throw ("sc.exe stop failed ({0}): {1}" -f $result.Code, $result.Output) }
        Write-Host "service stopped: $Name" -ForegroundColor Green
    }

    'restart' {
        if (-not (Get-ServiceState)) { throw "service not installed: $Name" }
        Invoke-Sc @('stop', $Name) | Out-Null
        Start-Sleep -Seconds 2
        $result = Invoke-Sc @('start', $Name)
        if ($result.Code -ne 0) { throw ("sc.exe start failed ({0}): {1}" -f $result.Code, $result.Output) }
        Write-Host "service restarted: $Name" -ForegroundColor Green
    }

    'status' {
        $state = Get-ServiceState
        if (-not $state) {
            Write-Host "service not installed: $Name"
            Write-Report "service not installed: $Name"
            exit 3
        }
        Write-Host "service $Name is $state"
        Write-Report "service $Name is $state"
    }
}
