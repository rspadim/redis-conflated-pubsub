<#
.SYNOPSIS
    Checks that the relay started, not only that the service was started.

.DESCRIPTION
    `sc start` succeeds as soon as the service control manager accepted the
    request. For a relay that is not the interesting question: the process can
    come up and exit immediately (a configuration the runtime rejects, an
    instance lock already held, an unreachable backend), and the wizard would
    still have reported success.

    Two signals are used, in order of strength:

    1. the status snapshot named by `status.path`. When the configuration sets it,
       the file appearing with `"state": "running"` is proof that the runtime is
       up, because the service writes it after its workers start. `last_error` is
       reported when present.
    2. otherwise, the service is re-queried after a settle delay. That is weaker,
       but the service control manager moves the service to STOPPED as soon as the
       process exits, so "still running a few seconds later" does rule out the
       instant-exit case.

.Exit codes
    0  the relay is running;  1  the service is not running;  2  running but no
    proof that the relay started;  3  usage problem.

.EXAMPLE
    .\verify-service.ps1 -ConfigPath 'C:\Program Files\Conflated Pub/Sub\config.json'
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$ConfigPath,
    [string]$ServiceName = 'redis-conflated-pubsub',
    [int]$TimeoutSeconds = 20,
    [int]$SettleSeconds = 5,
    [string]$ReportFile
)

$ErrorActionPreference = 'Stop'

function Write-Report {
    param([string]$Text)
    if ($ReportFile) { [System.IO.File]::WriteAllText($ReportFile, $Text) }
}

function Get-ServiceState {
    # Get-Service, not the text of "sc query": the field names are localized
    # ("ESTADO" instead of "STATE" on a Portuguese machine) and a service that
    # does not exist is $null either way.
    $service = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
    if (-not $service) { return $null }
    return $service.Status.ToString().ToUpper()
}

function Get-ConfigValue {
    param([string]$Name)
    try {
        $config = Get-Content -LiteralPath $ConfigPath -Raw -Encoding UTF8 | ConvertFrom-Json
    } catch {
        return $null
    }
    switch ($Name) {
        'status.path' {
            if ($config.PSObject.Properties.Name -contains 'status' -and $config.status) {
                if ($config.status.PSObject.Properties.Name -contains 'path') { return $config.status.path }
            }
        }
        'instance_lock.path' {
            if ($config.PSObject.Properties.Name -contains 'instance_lock' -and $config.instance_lock) {
                if ($config.instance_lock.PSObject.Properties.Name -contains 'path') { return $config.instance_lock.path }
            }
        }
    }
    return $null
}

$state = Get-ServiceState
if (-not $state) {
    $message = "the service '$ServiceName' is not installed"
    Write-Host $message -ForegroundColor Red
    Write-Report $message
    exit 1
}
if ($state -ne 'RUNNING') {
    $message = "the service '$ServiceName' is $state, not RUNNING"
    Write-Host $message -ForegroundColor Red
    Write-Report $message
    exit 1
}

# The strongest available signal: the runtime's own status snapshot.
$statusPath = Get-ConfigValue -Name 'status.path'
if ($statusPath) {
    # The file has to be written after this check began: a snapshot left behind by
    # an earlier run would otherwise pass for a relay that is up right now.
    $started = Get-Date
    $deadline = $started.AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        if (Test-Path -LiteralPath $statusPath -PathType Leaf) {
            try {
                $written = (Get-Item -LiteralPath $statusPath).LastWriteTime
                $status = Get-Content -LiteralPath $statusPath -Raw -Encoding UTF8 | ConvertFrom-Json
            } catch {
                $status = $null
                $written = $started.AddSeconds(-1)
            }
            if ($written -ge $started -and $status -and $status.state -eq 'running') {
                $message = "the relay is running (status: $statusPath)"
                if ($status.last_error) { $message += "; last error: $($status.last_error)" }
                Write-Host $message -ForegroundColor Green
                Write-Report $message
                exit 0
            }
        }
        Start-Sleep -Milliseconds 500
    }

    $detail = if (Test-Path -LiteralPath $statusPath -PathType Leaf) { "the status file exists but does not report a running state" } else { "no status file was written" }
    $message = "the service is running, but the relay did not report itself as running within $TimeoutSeconds seconds ($detail)"
    $logDirectory = Join-Path (Split-Path -Parent $ConfigPath) 'logs'
    if (Test-Path -LiteralPath $logDirectory) { $message += "; logs: $logDirectory" }

    Write-Host $message -ForegroundColor Yellow
    Write-Report $message
    exit 2
}

# No status file configured: the service surviving the settle delay is the only
# signal available, and it does rule out a process that exits at startup.
Start-Sleep -Seconds $SettleSeconds
$state = Get-ServiceState
if ($state -eq 'RUNNING') {
    $message = "the service is running (no status.path configured, so liveness was checked through the service state alone)"
    Write-Host $message -ForegroundColor Green
    Write-Report $message
    exit 0
}

$message = "the service is $state after $SettleSeconds seconds; the relay did not stay up"
Write-Host $message -ForegroundColor Yellow
Write-Report $message
exit 2
