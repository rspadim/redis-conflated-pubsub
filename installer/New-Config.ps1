<#
.SYNOPSIS
    Writes the relay's config.json from the answers collected by the installer.

.DESCRIPTION
    The wizard asks the questions and this script builds the file, for any of the
    four backend combinations: Redis or Core NATS on the input, Redis or Core NATS
    on the output.

    Everything it writes is validated by the relay itself right afterwards
    (`validate-config.ps1`), which is why the generated file can be trusted even
    though the structure is assembled here rather than copied from an example.

    Paths are absolute on purpose: a Windows service starts with System32 as its
    working directory, so relative log/lock/status paths would be written there.

    The file is written as UTF-8 without a BOM, because serde_json rejects a BOM.

.EXAMPLE
    .\New-Config.ps1 -AppDir 'C:\Program Files\Conflated Pub/Sub' -InSelector 'feed:*'

.EXAMPLE
    .\New-Config.ps1 -AppDir . -InBackend nats -InNatsServers 'nats://nats-1:4222' -InSelector 'market.>' -OutBackend redis
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$AppDir,
    [string]$TargetPath,
    [string]$ReportFile,
    [switch]$Force,

    [ValidateSet('redis', 'nats')][string]$InBackend = 'redis',
    [ValidateSet('redis', 'nats')][string]$OutBackend = 'redis',

    # Redis input and output
    [string]$InEndpoint = 'localhost:6379',
    [int]$InDatabase = 0,
    [string]$InPasswordEnv = '',
    [switch]$InTls,
    [string]$OutEndpoint = 'localhost:6379',
    [int]$OutDatabase = 0,
    [string]$OutPasswordEnv = '',
    [switch]$OutTls,

    # Core NATS input and output: seed URLs separated by comma, semicolon or space
    [string]$InNatsServers = '',
    [ValidateSet('none', 'user_password', 'token', 'credentials_file')][string]$InNatsAuth = 'none',
    [string]$InNatsUsername = '',
    [string]$InNatsPasswordEnv = '',
    [string]$InNatsTokenEnv = '',
    [string]$InNatsCredentials = '',
    [string]$OutNatsServers = '',
    [ValidateSet('none', 'user_password', 'token', 'credentials_file')][string]$OutNatsAuth = 'none',
    [string]$OutNatsUsername = '',
    [string]$OutNatsPasswordEnv = '',
    [string]$OutNatsTokenEnv = '',
    [string]$OutNatsCredentials = '',

    # One subscription: a Redis pattern or a NATS subject, depending on the backend
    [string]$InSelector = '',
    [int]$InFilterCache = 16384,

    # Output channel mapping
    [string]$OutChannelPrefix = '',
    [string]$OutChannelSuffix = '',
    [int]$OutFilterCache = 16384,
    [int]$OutPolicyCache = 16384,

    # Tuning
    [int]$IntervalMs = 250,
    [int]$DedupTtlMs = 5000,
    [int]$StatusPort = 9090,
    [ValidateSet('error', 'warn', 'info', 'debug', 'trace')][string]$LogLevel = 'info'
)

$ErrorActionPreference = 'Stop'

function Write-Report {
    param([string]$Text)
    if ($ReportFile) { [System.IO.File]::WriteAllText($ReportFile, $Text) }
}

if (-not $TargetPath) { $TargetPath = Join-Path $AppDir 'config.json' }

if ((Test-Path -LiteralPath $TargetPath) -and -not $Force) {
    $message = "kept the existing configuration: $TargetPath"
    Write-Host $message
    Write-Report $message
    exit 0
}

function Split-Endpoint {
    param([string]$Value, [string]$What)

    # An optional scheme avoids a separate TLS checkbox: "rediss://host:6379"
    # (or "tls://") means the same endpoint with TLS on. The relay's own field is
    # a boolean, so the scheme is translated into it here.
    $text = $Value.Trim()
    $tls = $false
    if ($text -match '^(?i)(redis|rediss|tls)://(.*)$') {
        $scheme = $Matches[1].ToLowerInvariant()
        if ($scheme -ne 'redis') { $tls = $true }
        $text = $Matches[2]
    }

    if ($text -notmatch '^\s*([^:\s/]+)\s*:\s*(\d+)\s*$') {
        throw "$What must be written as host:port (for example localhost:6379, or rediss://host:6380 for TLS); got '$Value'"
    }
    $port = [int]$Matches[2]
    if ($port -lt 1 -or $port -gt 65535) { throw "$What has an invalid port: $port" }

    return @{ Host = $Matches[1]; Port = $port; Tls = $tls }
}

function Split-Servers {
    param([string]$Value, [string]$What)

    $list = @($Value -split '[,;\s]+' | Where-Object { $_ -ne '' })
    if ($list.Count -eq 0) { throw "$What needs at least one seed URL, for example nats://localhost:4222" }
    foreach ($server in $list) {
        if ($server -notmatch '^(?i)(nats|tls|ws|wss)://') {
            # Braces are required: "$What:" would be read as a drive-qualified
            # variable reference, the same shape as $env:PATH.
            throw "${What}: '$server' needs a scheme. Use nats:// (TCP), tls:// (TLS), ws:// or wss:// (WebSocket)"
        }
    }
    return ,$list
}

function New-RedisEndpoint {
    param([hashtable]$Endpoint, [int]$Database, [string]$PasswordEnv, [bool]$Tls)

    return [ordered]@{
        host               = $Endpoint.Host
        port               = $Endpoint.Port
        username           = $null
        # A null environment variable name means "no authentication", which is
        # what the relay expects; an empty string would make it look for a
        # variable with no name.
        password_env       = if ([string]::IsNullOrWhiteSpace($PasswordEnv)) { $null } else { $PasswordEnv }
        tls                = $Tls
        database           = $Database
        connect_timeout_ms = 5000
    }
}

function New-NatsEndpoint {
    param([string]$Servers, [string]$Auth, [string]$Username, [string]$PasswordEnv,
          [string]$TokenEnv, [string]$Credentials, [string]$Name)

    $authObject = switch ($Auth) {
        'none' { [ordered]@{ type = 'none' } }
        'user_password' {
            if ([string]::IsNullOrWhiteSpace($Username)) { throw 'NATS user_password authentication needs a username' }
            if ([string]::IsNullOrWhiteSpace($PasswordEnv)) { throw 'NATS user_password authentication needs the name of the environment variable holding the password' }
            [ordered]@{ type = 'user_password'; username = $Username; password_env = $PasswordEnv }
        }
        'token' {
            if ([string]::IsNullOrWhiteSpace($TokenEnv)) { throw 'NATS token authentication needs the name of the environment variable holding the token' }
            [ordered]@{ type = 'token'; token_env = $TokenEnv }
        }
        'credentials_file' {
            if ([string]::IsNullOrWhiteSpace($Credentials)) { throw 'NATS credentials_file authentication needs the path to the .creds file' }
            [ordered]@{ type = 'credentials_file'; path = $Credentials }
        }
        default { throw "unknown NATS authentication type '$Auth'" }
    }

    return [ordered]@{
        # Split-Servers already returns an array; wrapping it in @() would nest it.
        servers                    = Split-Servers -Value $Servers -What 'The NATS servers list'
        name                       = $Name
        connect_timeout_ms         = 5000
        ignore_discovered_servers  = $false
        auth                       = $authObject
    }
}

function New-Subscription {
    param([string]$Backend, [string]$Selector, [string]$What)

    if ($Selector -notmatch '\S') {
        if ($Backend -eq 'nats') { throw "$What needs a NATS subject, for example prices.>" }
        throw "$What needs a Redis channel pattern, for example feed:*"
    }

    if ($Backend -eq 'nats') {
        return [ordered]@{
            type          = 'nats_subject'
            subject       = $Selector
            output_prefix = ''
            output_suffix = ''
        }
    }
    return [ordered]@{
        type          = 'psubscribe'
        pattern       = $Selector
        output_prefix = ''
        output_suffix = ''
    }
}

$logsDirectory = Join-Path $AppDir 'logs'
New-Item -ItemType Directory -Force -Path $logsDirectory | Out-Null

if ($InBackend -eq 'redis') {
    $inputEndpoint = Split-Endpoint -Value $InEndpoint -What 'The input endpoint'
    $input = [ordered]@{
        redis                    = New-RedisEndpoint -Endpoint $inputEndpoint -Database $InDatabase -PasswordEnv $InPasswordEnv -Tls ($InTls.IsPresent -or $inputEndpoint.Tls)
        exclude_output_echoes    = $true
        exclude_sentinel_pubsub  = $true
        filters                  = @()
        filter_cache_max_entries = $InFilterCache
        filter_default           = 'accept'
        subscriptions            = @(New-Subscription -Backend 'redis' -Selector $InSelector -What 'The subscription pattern')
    }
} else {
    $input = [ordered]@{
        nats                     = New-NatsEndpoint -Servers $InNatsServers -Auth $InNatsAuth -Username $InNatsUsername -PasswordEnv $InNatsPasswordEnv -TokenEnv $InNatsTokenEnv -Credentials $InNatsCredentials -Name 'conflated-input'
        filters                  = @()
        filter_cache_max_entries = $InFilterCache
        filter_default           = 'accept'
        subscriptions            = @(New-Subscription -Backend 'nats' -Selector $InSelector -What 'The subscription subject')
    }
}

if ($OutBackend -eq 'redis') {
    $outputEndpoint = Split-Endpoint -Value $OutEndpoint -What 'The output endpoint'
    $outputBackend = [ordered]@{
        redis = New-RedisEndpoint -Endpoint $outputEndpoint -Database $OutDatabase -PasswordEnv $OutPasswordEnv -Tls ($OutTls.IsPresent -or $outputEndpoint.Tls)
    }
} else {
    $outputBackend = [ordered]@{
        nats = New-NatsEndpoint -Servers $OutNatsServers -Auth $OutNatsAuth -Username $OutNatsUsername -PasswordEnv $OutNatsPasswordEnv -TokenEnv $OutNatsTokenEnv -Credentials $OutNatsCredentials -Name 'conflated-output'
    }
}

$output = [ordered]@{}
foreach ($key in $outputBackend.Keys) { $output[$key] = $outputBackend[$key] }
$output['channel_prefix'] = $OutChannelPrefix
$output['channel_suffix'] = $OutChannelSuffix
$output['filters'] = @()
$output['filter_cache_max_entries'] = $OutFilterCache
$output['channel_policy_cache_max_entries'] = $OutPolicyCache
$output['filter_default'] = 'accept'
$output['conflation'] = [ordered]@{
    interval_ms              = $IntervalMs
    max_commands_per_exec    = 256
    oversized_message_policy = 'send'
}
$output['deduplication'] = [ordered]@{
    ttl_ms = $DedupTtlMs
}

$config = [ordered]@{
    max_bytes_per_exec       = 4194304
    oversized_message_policy = 'send'
    client_name              = 'ConflatedPS-{version}-{role}::{user}::{host}'
    input                    = $input
    outputs                  = [ordered]@{ output1 = $output }
    instance_lock            = [ordered]@{
        path = Join-Path $AppDir 'redis-conflated-pubsub.lock'
    }
    status                   = [ordered]@{
        # The status snapshot is what the installer's post-install check reads to
        # tell "the service started" from "the relay is actually up", so it is
        # always written even when the HTTP listener is disabled.
        path = Join-Path $AppDir 'status.json'
    }
    logging                  = [ordered]@{
        directory         = $logsDirectory
        prefix            = 'redis-conflated-pubsub'
        enabled           = $true
        level             = $LogLevel
        retention_days    = 14
        max_total_size_mb = 1024
    }
}

if ($StatusPort -gt 0) {
    $config.status.http = [ordered]@{
        bind = '127.0.0.1'
        port = $StatusPort
    }
}

# -Depth is required: PowerShell 5.1 truncates nested objects at depth 2.
[System.IO.File]::WriteAllText($TargetPath, ($config | ConvertTo-Json -Depth 100))

$summary = "configuration written: $TargetPath (input $InBackend, output $OutBackend, conflation ${IntervalMs}ms, deduplication ${DedupTtlMs}ms)"
Write-Host $summary -ForegroundColor Green
Write-Report $summary
exit 0
