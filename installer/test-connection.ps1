<#
.SYNOPSIS
    Probes an endpoint from the installer wizard, before anything is installed.

.DESCRIPTION
    The wizard validates the configuration semantically (the relay's own
    --check-config), but that runs BEFORE any connection: a wrong host, port or
    password is only discovered after the install, as the service timing out.
    This script answers the question the user actually asks on the page ("will
    this address work?") without leaving it.

    It speaks the real protocol of the chosen backend rather than only opening a
    socket, because a TCP connect proves reachability but says nothing about
    credentials - the case that fails in practice:

      redis  AUTH (when a password is given), then PING, expecting PONG.
      nats   reads the server INFO, sends CONNECT (+ auth) and PING, then
             expects PONG; an authorization failure answers -ERR and is reported
             as such.

    TLS is honoured for rediss://, tls:// and wss://. WebSocket URLs (ws/wss)
    are NOT probed: the handshake is an HTTP upgrade this script does not
    implement, so they report "not supported" instead of a false OK.

.PARAMETER Kind
    redis or nats.

.PARAMETER Endpoint
    The value as typed in the wizard: host:port, redis://host:port,
    rediss://host:port, nats://host:4222, tls://host:4222, or (NATS) a
    comma/space separated list whose first entry is probed.

.PARAMETER TimeoutMs
    Connect timeout. Defaults to 5000, which is the relay's own
    connect_timeout_ms default, so the probe waits as long as the service will.

.PARAMETER PasswordFile
    File holding the password/token. Used instead of a parameter so a value with
    spaces or quotes never has to survive command-line quoting (and never shows
    up in a process command line readable by other local users).

.PARAMETER User
    Username for Redis ACL or NATS user_password.

.OUTPUTS
    Exit codes, stable so the caller can show its own localized message
    instead of this script's text (a console redirect re-encodes it into the
    OEM codepage and would mangle accents):

      0  the server answered
      2  the host name does not resolve
      3  no answer within the timeout
      4  connection refused / other socket error
      5  the server rejected the credentials
      6  something answered, but not with the expected reply
      7  this probe does not support the URL (websocket)

.EXAMPLE
    powershell -File test-connection.ps1 -Kind redis -Endpoint localhost:6379
    powershell -File test-connection.ps1 -Kind nats -Endpoint nats://h:4222
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][ValidateSet('redis', 'nats')][string]$Kind,
    [Parameter(Mandatory = $true)][string]$Endpoint,
    [int]$TimeoutMs = 5000,
    [string]$User = '',
    [string]$PasswordFile = ''
)

$ErrorActionPreference = 'Stop'

$script:Password = ''
if ($PasswordFile -ne '' -and (Test-Path -LiteralPath $PasswordFile)) {
    # Verbatim (no Trim): a password may legitimately contain spaces.
    $script:Password = [System.IO.File]::ReadAllText($PasswordFile)
}

# --- endpoint parsing ------------------------------------------------------
# A Redis endpoint is host:port or scheme://host:port; the NATS field accepts a
# list, and only the first entry is probed (the others are the relay's failover
# seeds, not a different address to verify).
function Split-Endpoint {
    param([string]$Value)

    $text = $Value.Trim()
    $scheme = ''
    if ($text -match '^([a-zA-Z][a-zA-Z0-9+.-]*)://(.*)$') {
        $scheme = $Matches[1].ToLowerInvariant()
        $text = $Matches[2]
    }

    # A NATS field may hold several servers; the first one is enough to prove
    # the address and the credentials.
    $text = ($text -split '[,\s]+' | Where-Object { $_ -ne '' } | Select-Object -First 1)
    if (-not $text) { return $null }

    # Drop any path/query (a NATS URL may carry ?connect_timeout=..., a Redis
    # one is followed by nothing in practice but would break the host split).
    $text = ($text -split '[/?#]')[0]
    # Strip credentials embedded in the URL (user:pass@host): the password
    # belongs in PasswordFile, never on a command line.
    if ($text -match '^[^@]*@(.+)$') { $text = $Matches[1] }

    $hostName = $text
    $port = 0
    if ($text -match '^\[(.+)\](?::(\d+))?$') {
        # IPv6 literal: [::1]:6379
        $hostName = $Matches[1]
        if ($Matches[2]) { $port = [int]$Matches[2] }
    } elseif ($text -match '^(.+):(\d+)$') {
        $hostName = $Matches[1]
        $port = [int]$Matches[2]
    }

    if ($port -eq 0) {
        if ($Kind -eq 'nats') {
            if ($scheme -eq 'ws' -or $scheme -eq 'wss') { $port = 80 }
            else { $port = 4222 }
        } else {
            $port = 6379
        }
    }

    return [pscustomobject]@{
        Host   = $hostName
        Port   = $port
        Tls    = ($scheme -eq 'rediss' -or $scheme -eq 'tls' -or $scheme -eq 'wss')
        Scheme = $scheme
    }
}

function Test-TlsWanted {
    param($Target)
    if ($Target.Scheme -eq 'wss') { return $true }
    return $Target.Tls
}

# --- protocol probes -------------------------------------------------------
# RESP: an array of bulk strings is the only form that survives a password with
# spaces or binary bytes.
function Send-Resp {
    param($Stream, [string[]]$Parts)
    $builder = New-Object System.Text.StringBuilder
    [void]$builder.Append('*' + $Parts.Count + "`r`n")
    foreach ($part in $Parts) {
        $bytes = [System.Text.Encoding]::UTF8.GetByteCount($part)
        [void]$builder.Append('$' + $bytes + "`r`n")
        [void]$builder.Append($part + "`r`n")
    }
    $payload = [System.Text.Encoding]::UTF8.GetBytes($builder.ToString())
    $Stream.Write($payload, 0, $payload.Length)
    $Stream.Flush()
}

function Invoke-RedisProbe {
    param($Stream, $Reader)

    if ($script:Password -ne '') {
        if ($User -ne '') { Send-Resp $Stream @('AUTH', $User, $script:Password) }
        else { Send-Resp $Stream @('AUTH', $script:Password) }
        $reply = $Reader.ReadLine()
        if ($reply -notmatch '^\+OK') {
            if ($reply -match '^-?(WRONGPASS|NOAUTH|ERR .*[Aa]uth)') { return 5 }
            return 6
        }
    }

    Send-Resp $Stream @('PING')
    $reply = $Reader.ReadLine()
    if ($reply -match 'PONG') { return 0 }
    if ($reply -match 'NOAUTH|WRONGPASS') { return 5 }
    return 6
}

function Invoke-NatsProbe {
    param($Stream, $Reader)

    # The server speaks first: INFO {...}. Reading it proves this is really a
    # NATS server and not some other service on that port.
    $info = $Reader.ReadLine()
    if (-not $info) { return 6 }
    if ($info -notmatch '^INFO') { return 6 }

    $connect = 'CONNECT {"verbose":false,"pedantic":false,"lang":"powershell","version":"installer","protocol":1'
    if ($script:Password -ne '') {
        if ($User -ne '') {
            # JSON escaping by hand: the payload cannot be built with a JSON
            # serializer here without pulling a dependency into the wizard.
            $u = $User.Replace('\', '\\').Replace('"', '\"')
            $p = $script:Password.Replace('\', '\\').Replace('"', '\"')
            $connect = $connect + ',"user":"' + $u + '","pass":"' + $p + '"'
        } else {
            $t = $script:Password.Replace('\', '\\').Replace('"', '\"')
            $connect = $connect + ',"auth_token":"' + $t + '"'
        }
    }
    $connect = $connect + "}`r`n"

    $batch = [System.Text.Encoding]::UTF8.GetBytes($connect + "PING`r`n")
    $Stream.Write($batch, 0, $batch.Length)
    $Stream.Flush()

    # The server answers +OK (verbose off: nothing), PONG, or -ERR 'auth...'.
    $deadline = (Get-Date).AddMilliseconds($TimeoutMs)
    while ((Get-Date) -lt $deadline) {
        $line = $Reader.ReadLine()
        if ($null -eq $line) { return 6 }
        if ($line -match '^PONG') { return 0 }
        if ($line -match '^\+OK') { continue }
        if ($line -match '^-ERR') {
            if ($line -match 'Authorization|Authentication|auth') { return 5 }
            return 6
        }
    }
    return 3
}

# --- main ------------------------------------------------------------------
$target = Split-Endpoint $Endpoint
if (-not $target) {
    Write-Host 'empty endpoint'
    exit 4
}
if ($target.Scheme -eq 'ws' -or $target.Scheme -eq 'wss') {
    # An HTTP upgrade is out of scope for this probe; report it instead of
    # claiming success from a bare TCP connect.
    Write-Host 'websocket endpoints are not probed'
    exit 7
}

$client = $null
$stream = $null
try {
    $client = New-Object System.Net.Sockets.TcpClient
    $connect = $client.ConnectAsync($target.Host, $target.Port)
    if (-not $connect.Wait($TimeoutMs)) {
        Write-Host ('no answer within ' + [int]($TimeoutMs / 1000) + 's')
        exit 3
    }

    $netStream = $client.GetStream()
    if (Test-TlsWanted $target) {
        # Redis/NATS both use server-auth TLS with a trusted certificate by
        # default; a self-signed one fails here exactly as the service would.
        $ssl = New-Object System.Net.Security.SslStream($netStream, $false)
        $ssl.AuthenticateAsClient($target.Host)
        $stream = $ssl
    } else {
        $stream = $netStream
    }

    $reader = New-Object System.IO.StreamReader($stream)
    if ($Kind -eq 'nats') {
        $code = Invoke-NatsProbe $stream $reader
    } else {
        $code = Invoke-RedisProbe $stream $reader
    }

    if ($code -eq 0) { Write-Host 'OK' } else { Write-Host ('probe failed: ' + $code) }
    exit $code
} catch {
    $message = $_.Exception.Message
    Write-Host ('no answer: ' + $message)

    # Classify by SocketError CODE, never by the message text: the text is
    # localized by Windows and a pattern match on it breaks per locale.
    $hostNotFound = $false
    $e = $_.Exception
    while ($e -and -not $hostNotFound) {
        $se = $e -as [System.Net.Sockets.SocketException]
        if ($se -and ($se.SocketErrorCode -eq [System.Net.Sockets.SocketError]::HostNotFound -or
                      $se.SocketErrorCode -eq [System.Net.Sockets.SocketError]::NoData)) {
            $hostNotFound = $true
        }
        $e = $e.InnerException
    }
    if ($hostNotFound) { exit 2 }
    exit 4
} finally {
    if ($stream) { try { $stream.Dispose() } catch { } }
    if ($client) { try { $client.Close() } catch { } }
}
