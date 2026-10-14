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

# Native tools report through their exit code and their stderr text, not through
# PowerShell errors. Under $ErrorActionPreference = 'Stop' a tool that writes to
# stderr while its output is piped raises a RemoteException that kills the script
# before it can say what happened - and in this script one such failure is an
# expected step: icacls cannot resolve a virtual account until the service
# manager has registered it. So the calls below run with 'Continue' and the
# caller decides what a non-zero exit code means.
function Invoke-Native {
    param([string]$Tool, [string[]]$Arguments)
    # The arguments reach the tool as one raw command line. sc.exe and icacls parse
    # that line themselves, so a path with spaces has to arrive quoted the way they
    # expect it; PowerShell would escape those quotes as \" and the tool rejects the
    # result (sc.exe fails with 1639, "invalid command line"). The default install
    # directory has spaces, so this is the normal case rather than an edge.
    $start = New-Object System.Diagnostics.ProcessStartInfo
    $start.FileName = $Tool
    $start.Arguments = ($Arguments -join ' ')
    $start.UseShellExecute = $false
    $start.RedirectStandardOutput = $true
    $start.RedirectStandardError = $true
    $start.CreateNoWindow = $true
    $process = [System.Diagnostics.Process]::Start($start)
    $stdout = $process.StandardOutput.ReadToEndAsync()
    $stderr = $process.StandardError.ReadToEnd()
    $process.WaitForExit()
    return [pscustomobject]@{
        Code = $process.ExitCode
        Output = (($stdout.Result + $stderr).Trim())
    }
}

function Invoke-Sc {
    param([string[]]$Arguments)
    return Invoke-Native 'sc.exe' $Arguments
}

function Invoke-Icacls {
    param([string[]]$Arguments)
    return Invoke-Native 'icacls.exe' $Arguments
}

function Get-ServiceState {
    # Get-Service, not the text of "sc query": the field names are localized
    # ("ESTADO" instead of "STATE" on a Portuguese machine), and a service that
    # does not exist is $null either way.
    $service = Get-Service -Name $Name -ErrorAction SilentlyContinue
    if (-not $service) { return $null }
    return $service.Status.ToString().ToUpper()
}

function Get-ServiceBinaryPath {
    # Win32_Service carries the same field names on every locale; "sc qc" does not.
    $service = Get-CimInstance Win32_Service -Filter ("Name='{0}'" -f $Name) -ErrorAction SilentlyContinue
    if (-not $service) { return $null }
    return $service.PathName
}

function Grant-ServiceDirectoryAccess {
    param([string]$Path, [string]$Account, [string]$ServiceName)

    # icacls needs the account to exist in the local security authority, and a
    # virtual account is only registered by the service control manager - not by
    # icacls itself (verified: it fails with error 1332, "no mapping between
    # account names and security IDs"). So when the grant fails the service is
    # started once, which makes the SCM create the account, and the grant is
    # retried.
    $grant = Invoke-Icacls @("`"$Path`"", '/grant', "`"${Account}:(OI)(CI)M`"")
    if ($grant.Code -eq 0) { return $true }

    Invoke-Sc @('start', $ServiceName) | Out-Null
    Start-Sleep -Seconds 3
    $grant = Invoke-Icacls @("`"$Path`"", '/grant', "`"${Account}:(OI)(CI)M`"")
    return ($grant.Code -eq 0)
}

try {
switch ($Action) {
    'install' {
        if (-not $ExePath) { throw '-ExePath is required for install' }
        if (-not $ConfigPath) { throw '-ConfigPath is required for install' }
        if (-not (Test-Path -LiteralPath $ExePath -PathType Leaf)) { throw "executable not found: $ExePath" }
        if (-not (Test-Path -LiteralPath $ConfigPath -PathType Leaf)) { throw "configuration not found: $ConfigPath" }

        # The service manager runs the value of binPath as the process command
        # line, so the executable and its --config argument are one string with
        # their own quotes. It is written through New-Service / WMI rather than
        # sc.exe: sc.exe parses its command line itself and splits on spaces, so a
        # path with spaces - the default installation directory - cannot be handed
        # to it as a single value (it fails with 1639, "invalid command line").
        $binaryPath = '"{0}" --config "{1}"' -f $ExePath, $ConfigPath
        $startMode = if ($StartupType -eq 'demand') { 'Manual' } else { 'Automatic' }
        $existing = Get-ServiceState

        if ($existing) {
            # Reinstall or upgrade: keep the service, point it at the current files.
            $service = Get-CimInstance Win32_Service -Filter ("Name='{0}'" -f $Name) -ErrorAction Stop
            $changed = Invoke-CimMethod -InputObject $service -MethodName Change -Arguments @{
                PathName = $binaryPath
                DisplayName = $DisplayName
                StartMode = $startMode
            }
            if ($changed.ReturnValue -ne 0) {
                throw ("could not reconfigure service {0}: Win32_Service.Change returned {1}" -f $Name, $changed.ReturnValue)
            }
        } else {
            New-Service -Name $Name -BinaryPathName $binaryPath -DisplayName $DisplayName -StartupType $startMode | Out-Null
        }

        # Delayed auto-start has no New-Service equivalent, and every value here is
        # a single token without spaces, which is what sc.exe parses reliably.
        if ($StartupType -eq 'delayed-auto') {
            $result = Invoke-Sc @('config', $Name, 'start=', 'delayed-auto')
            if ($result.Code -ne 0) {
                throw ("sc.exe config failed ({0}): {1}" -f $result.Code, $result.Output)
            }
        }

        # Cosmetic: the description is the registry value sc.exe writes, set
        # directly to avoid the same command-line parsing.
        Set-ItemProperty -Path ("HKLM:\SYSTEM\CurrentControlSet\Services\{0}" -f $Name) -Name Description -Value 'Conflated Pub/Sub relay: forwards Pub/Sub messages between Redis and Core NATS with per-output conflation.' -ErrorAction SilentlyContinue

        # Least privilege. The relay needs the network and its own directory, not
        # LocalSystem: the SCM can run one service under the passwordless virtual
        # account "NT SERVICE\<name>", which exists nowhere else and holds no
        # rights beyond what is granted below. If the machine refuses that, the
        # service falls back to LocalSystem and says so, which beats leaving a
        # service that cannot start.
        $account = "NT SERVICE\$Name"
        $appDir = Split-Path -Parent $ExePath
        $accountOk = $false

        $configured = Invoke-Sc @('config', $Name, 'obj=', "`"$account`"")
        if ($configured.Code -eq 0) {
            $accountOk = Grant-ServiceDirectoryAccess -Path $appDir -Account $account -ServiceName $Name
        }

        if ($accountOk) {
            Write-Host "service account: $account (write access granted on $appDir)"
        }
        else {
            Invoke-Sc @('config', $Name, 'obj=', 'LocalSystem') | Out-Null
            $message = "the virtual account $account could not be used; the service will run as LocalSystem"
            Write-Host $message -ForegroundColor Yellow
            Write-Report $message
        }

        # Only now: with the account settled, a failure loop would be noise.
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

        # Best effort: drop the grant left on the install directory, so a kept
        # directory does not carry an access entry for an account that will not
        # exist anymore. The binary path is read back because the uninstaller
        # does not pass -ExePath.
        $binaryPath = Get-ServiceBinaryPath
        if ($binaryPath -match '^"([^"]+)"') {
            $appDir = Split-Path -Parent $Matches[1]
            if (Test-Path -LiteralPath $appDir) {
                Invoke-Icacls @("`"$appDir`"", '/remove:g', "`"NT SERVICE\$Name`"") | Out-Null
            }
        }

        $message = "service removed: $Name"
        Write-Host $message -ForegroundColor Green
        Write-Report $message
    }

    'start' {
        if (-not (Get-ServiceState)) { throw "service not installed: $Name" }
        $result = Invoke-Sc @('start', $Name)
        # 1056 is ERROR_SERVICE_ALREADY_RUNNING: the account-registration step may
        # have started the service already, and that is not a failure.
        if ($result.Code -ne 0 -and $result.Code -ne 1056) {
            throw ("sc.exe start failed ({0}): {1}" -f $result.Code, $result.Output)
        }
        Write-Host "service started: $Name" -ForegroundColor Green
    }

    'stop' {
        if (-not (Get-ServiceState)) { throw "service not installed: $Name" }
        $result = Invoke-Sc @('stop', $Name)
        # 1062 is ERROR_SERVICE_NOT_ACTIVE: already stopped is what we want.
        if ($result.Code -ne 0 -and $result.Code -ne 1062) {
            throw ("sc.exe stop failed ({0}): {1}" -f $result.Code, $result.Output)
        }
        # The caller replaces the executable next, and Windows refuses to overwrite
        # a running image, so wait for the process to be gone instead of trusting
        # the accepted request.
        for ($waited = 0; $waited -lt 30; $waited++) {
            if ((Get-ServiceState) -eq 'STOPPED') { break }
            Start-Sleep -Milliseconds 500
        }
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
}
catch {
    # The installer shows this text when the action fails, and in a silent install
    # it is the only thing that says what went wrong: an empty report would leave
    # the operator with nothing to act on.
    $message = $_.Exception.Message
    $position = $_.InvocationInfo.PositionMessage
    if ($position) { $message = $message + "`n" + $position.Trim() }
    Write-Report $message
    Write-Host $message -ForegroundColor Red
    exit 1
}
