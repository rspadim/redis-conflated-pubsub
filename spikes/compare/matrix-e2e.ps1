# End-to-end acceptance + load matrix for the 0.3 Redis/NATS relay.
#
# Exercises all four input/output combinations through the REAL relay binary
# (target/release/redis-conflated-pubsub.exe), plus an output-filter run, a
# mixed Redis+NATS fan-out run, and the NATS loop guard. For every run it starts
# the broker(s), starts the relay with a generated config, drives a
# process-separated publisher, verifies the output address space with a
# process-separated verifier, and checks the relay's status HTTP counters.
#
# Local Docker only, synthetic data only. No production servers or secrets.
#
#   powershell -ExecutionPolicy Bypass -File spikes/compare/matrix-e2e.ps1
#
# Knobs:
#   -SmokeTotal   messages per acceptance run   (default 5000)
#   -LoadTotal    messages per load run         (default 300000)
#   -LoadRate     target msg/s for load runs, 0 = unpaced ceiling (default 5000)
#   -SkipLoad     acceptance only
#   -PayloadBytes payload size in bytes         (default 256)
#   -Project      compose project name          (default rc-e2e-<random>)

param(
    [int]$SmokeTotal = 5000,
    [int]$LoadTotal = 300000,
    [int]$LoadRate = 5000,
    [int]$PayloadBytes = 256,
    [switch]$SkipLoad,
    [switch]$SkipBuild,
    [string]$Project = "rc-e2e-$([guid]::NewGuid().ToString('N').Substring(0,8))"
)

$ErrorActionPreference = "Stop"
$repoRoot = (Resolve-Path "$PSScriptRoot/../..").Path
$composeFile = Join-Path $PSScriptRoot "compose.yml"
$configDir = Join-Path $PSScriptRoot "configs"
$harnessExe = Join-Path $PSScriptRoot ".target/release/transport-compare.exe"
$relayExe = Join-Path $repoRoot "target/release/redis-conflated-pubsub.exe"
$work = Join-Path $env:TEMP "rc-e2e-$Project"
New-Item -ItemType Directory -Force -Path $work | Out-Null

$redisUrl = "redis://127.0.0.1:16390"
$redisVerifyUrl = "redis://127.0.0.1:16390"
$natsUrl = "nats://127.0.0.1:14230"
$natsMonitor = "http://127.0.0.1:18222"
$statusBasePort = 19300

# Native commands (docker, cargo) can write to stderr, which PowerShell 5.1
# turns into a terminating error under $ErrorActionPreference=Stop. These
# helpers temporarily relax that and report only the exit code / captured
# stdout, so the driver controls failure handling explicitly.
function Invoke-Native {
    param([string]$File, [string[]]$Arguments, [string]$StdOutFile, [string]$StdErrFile)
    $previous = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        if ($StdOutFile -and $StdErrFile) {
            & $File @Arguments > $StdOutFile 2> $StdErrFile | Out-Null
        } elseif ($StdOutFile) {
            & $File @Arguments > $StdOutFile | Out-Null
        } elseif ($StdErrFile) {
            & $File @Arguments 2> $StdErrFile | Out-Null
        } else {
            & $File @Arguments | Out-Null
        }
        return $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $previous
    }
}

function Invoke-NativeCapture {
    param([string]$File, [string[]]$Arguments, [string]$StdErrFile)
    $previous = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        if ($StdErrFile) { $output = & $File @Arguments 2> $StdErrFile }
        else { $output = & $File @Arguments 2> $null }
        return [pscustomobject]@{ Code = $LASTEXITCODE; Output = ($output -join "`n") }
    } finally {
        $ErrorActionPreference = $previous
    }
}

function Write-Section([string]$Text) {
    Write-Host ""
    Write-Host ("=== " + $Text + " ===") -ForegroundColor Cyan
}

function Invoke-Compose {
    param([string[]]$Arguments)
    $errLog = Join-Path $work "compose.err"
    $code = Invoke-Native -File "docker" -Arguments (@("compose", "-p", $Project, "-f", $composeFile) + $Arguments) -StdErrFile $errLog
    if ($code -ne 0) {
        Get-Content $errLog -ErrorAction SilentlyContinue | Write-Host
        throw "docker compose $($Arguments -join ' ') failed (exit $code)"
    }
}

function Wait-ForRedis([int]$TimeoutSec = 60) {
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    while ((Get-Date) -lt $deadline) {
        $result = Invoke-NativeCapture -File "docker" -Arguments @("compose", "-p", $Project, "-f", $composeFile, "exec", "-T", "redis", "redis-cli", "ping")
        if ($result.Output -match "PONG") { return }
        Start-Sleep -Milliseconds 250
    }
    throw "redis did not become ready"
}

function Wait-ForNats([int]$TimeoutSec = 60) {
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    while ((Get-Date) -lt $deadline) {
        try {
            Invoke-RestMethod "$natsMonitor/varz" -TimeoutSec 2 | Out-Null
            return
        } catch {
            Start-Sleep -Milliseconds 250
        }
    }
    throw "nats did not become ready"
}

# Waits until the relay is up (status HTTP reachable, state running) and its
# input subscription is installed on the broker, so no published message can be
# lost to a late subscribe.
function Wait-RelayReady([int]$Port, [string]$InputBackend, [string]$InputPattern, [int]$TimeoutSec = 30) {
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    $httpReady = $false
    while ((Get-Date) -lt $deadline) {
        if (-not $httpReady) {
            try {
                $status = Invoke-RestMethod "http://127.0.0.1:$Port/" -TimeoutSec 2
                if ($status.state -eq "running") { $httpReady = $true }
            } catch { }
        }
        if ($httpReady) {
            if ($InputBackend -eq "redis") {
                $result = Invoke-NativeCapture -File "docker" -Arguments @("compose", "-p", $Project, "-f", $composeFile, "exec", "-T", "redis", "redis-cli", "PUBSUB", "NUMPAT")
                if ([int]$result.Output.Trim() -ge 1) { return }
            } else {
                try {
                    $subsz = Invoke-RestMethod "$natsMonitor/subsz?subs=1" -TimeoutSec 2
                    $found = @($subsz.subscriptions_list | Where-Object { $_.subject -eq $InputPattern }).Count
                    if ($found -ge 1) { return }
                } catch { }
            }
        }
        Start-Sleep -Milliseconds 200
    }
    throw "relay input $InputPattern did not become ready ($InputBackend)"
}

function New-RunConfig([string]$Template, [string]$Namespace, [int]$Port) {
    $path = Join-Path $work "$Namespace.json"
    $json = (Get-Content (Join-Path $configDir $Template) -Raw) -replace "__NS__", $Namespace -replace "__PORT__", "$Port"
    Set-Content -Path $path -Value $json -NoNewline
    return $path
}

$script:relayProcess = $null
function Start-Relay([string]$ConfigPath, [string]$Stdout, [string]$Stderr) {
    $script:relayProcess = Start-Process -FilePath $relayExe -ArgumentList "--config", $ConfigPath `
        -PassThru -NoNewWindow -RedirectStandardOutput $Stdout -RedirectStandardError $Stderr
}

function Stop-Relay {
    if ($script:relayProcess) {
        Stop-Process -Id $script:relayProcess.Id -Force -ErrorAction SilentlyContinue
        Start-Sleep -Milliseconds 400
        $script:relayProcess = $null
    }
}

function Read-Status([int]$Port) {
    return Invoke-RestMethod "http://127.0.0.1:$Port/" -TimeoutSec 5
}

function Stop-Harness {
    Get-Process transport-compare -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
}

# Runs one relay configuration end to end and returns the measured row.
function Invoke-Run {
    param(
        [string]$Name,
        [string]$Template,
        [string]$InputEngine,
        [string]$VerifyEngines,
        [int]$Total,
        [int]$ExpectedPerOutput,
        [int]$Port,
        [int]$DenyEvery = 0,
        [int]$ExpectedLoops = 0,
        [int]$Rate = 0,
        [string]$InPrefix = "in",
        [string]$OutPrefix = "out"
    )

    Stop-Harness
    $ns = "ns$([guid]::NewGuid().ToString('N').Substring(0,10))"
    $configPath = New-RunConfig $Template $ns $Port
    $readyFile = Join-Path $work "$ns.ready"
    Remove-Item $readyFile -ErrorAction SilentlyContinue

    $relayOut = Join-Path $work "$ns.relay.out"
    $relayErr = Join-Path $work "$ns.relay.err"
    Start-Relay $configPath $relayOut $relayErr

    $inputSubject = "$ns.$InPrefix"
    $inputPattern = if ($InputEngine -eq "redis") { "$inputSubject.*" } else { "$inputSubject.>" }
    Wait-RelayReady -Port $Port -InputBackend $InputEngine -InputPattern $inputPattern

    $verifyPrefix = "$ns.$OutPrefix"
    $verifyPrefixes = ($VerifyEngines.Split(",") | ForEach-Object { $verifyPrefix }) -join ","

    $env:COMPARE_ENGINE = $InputEngine
    $env:COMPARE_TOTAL = "$Total"
    $env:COMPARE_PAYLOAD_BYTES = "$PayloadBytes"
    $env:COMPARE_RATE = "$Rate"
    $env:COMPARE_READY_FILE = $readyFile
    $env:COMPARE_INPUT_PREFIX = $inputSubject
    $env:COMPARE_EXPECTED = "$ExpectedPerOutput"
    $env:COMPARE_VERIFY_ENGINES = $VerifyEngines
    $env:COMPARE_VERIFY_PREFIX = $verifyPrefixes
    $env:COMPARE_WAIT_TIMEOUT_SECS = "120"
    $env:COMPARE_FILTER_DENY_EVERY = "$DenyEvery"
    $env:REDIS_URL = $redisUrl
    $env:REDIS_VERIFY_URL = $redisVerifyUrl
    $env:NATS_URL = $natsUrl

    $verifyOut = Join-Path $work "$ns.verify"
    $verifyErr = Join-Path $work "$ns.verify.err"
    $publishOut = Join-Path $work "$ns.publish"
    $verifier = Start-Process -FilePath $harnessExe -ArgumentList "e2e-verify" `
        -PassThru -NoNewWindow -RedirectStandardOutput $verifyOut -RedirectStandardError $verifyErr

    $publishExit = Invoke-Native -File $harnessExe -Arguments @("e2e-publish") -StdOutFile $publishOut
    $verifier.WaitForExit(180000) | Out-Null

    $publishLine = (Get-Content $publishOut -ErrorAction SilentlyContinue | Select-String "role=e2e-publish").Line
    $verifyLines = @(Get-Content $verifyOut -ErrorAction SilentlyContinue | Select-String "role=e2e-verify")
    $ageLines = @(Get-Content $verifyOut -ErrorAction SilentlyContinue | Select-String "e2e_age_ms")

    # Settle before reading counters: the relay publishes asynchronously and the
    # status endpoint is a snapshot.
    $expectedOutput = $ExpectedPerOutput * [math]::Max($verifyLines.Count, 1)
    $status = $null
    $deadline = (Get-Date).AddSeconds(30)
    while ((Get-Date) -lt $deadline) {
        $status = Read-Status $Port
        $outputTotal = 0
        foreach ($property in $status.outputs.PSObject.Properties) { $outputTotal += $property.Value.output_messages_total }
        if ($status.input_messages_total -ge $Total -and $outputTotal -ge $expectedOutput) { break }
        Start-Sleep -Milliseconds 300
    }

    Stop-Relay

    $publishedRate = 0
    if ($publishLine -match "publish_rate_msg_s=([0-9.]+)") { $publishedRate = [double]$Matches[1] }

    $received = 0
    $complete = $true
    $p50 = $p95 = $p99 = 0.0
    $verifySummary = @()
    $e2eRate = 0.0
    for ($i = 0; $i -lt $verifyLines.Count; $i++) {
        $line = $verifyLines[$i]
        $r = 0
        if ($line -match "received=([0-9]+)") { $r = [int]$Matches[1] }
        if ($line -match "complete=(True|False)") { $complete = $complete -and ($Matches[1] -eq "True") }
        $received += $r
        $engine = "?"
        if ($line -match "engine=([a-z]+)") { $engine = $Matches[1] }
        $verifySummary += "$engine=$r"
        if ($line -match "rate_msg_s=([0-9.]+)") { $e2eRate += [double]$Matches[1] }
        if ($i -lt $ageLines.Count -and $ageLines[$i] -match "p50=([0-9.]+) p95=([0-9.]+) p99=([0-9.]+)") {
            $p50 = [math]::Max($p50, [double]$Matches[1])
            $p95 = [math]::Max($p95, [double]$Matches[2])
            $p99 = [math]::Max($p99, [double]$Matches[3])
        }
    }

    $errorText = @(Get-Content $relayErr -ErrorAction SilentlyContinue) -join "; "
    $outputTotal = 0
    foreach ($property in $status.outputs.PSObject.Properties) { $outputTotal += $property.Value.output_messages_total }

    $ok = ($publishExit -eq 0) -and $complete -and
        ($status.input_messages_total -eq $Total) -and
        ($outputTotal -eq $expectedOutput) -and
        ($received -eq $expectedOutput) -and
        ($status.publish_errors_total -eq 0) -and
        ($status.uncertain_transactions_total -eq 0) -and
        ($status.shed_messages_total -eq 0) -and
        ($status.loop_suppressed_messages_total -ge $ExpectedLoops)

    return [pscustomobject]@{
        Name       = $Name
        Input      = $InputEngine
        Output     = $VerifyEngines
        Published  = $Total
        RelayInput = $status.input_messages_total
        RelayOut   = $outputTotal
        Received   = $received
        Verify     = ($verifySummary -join " ")
        Errors     = $status.publish_errors_total
        Uncertain  = $status.uncertain_transactions_total
        Shed       = $status.shed_messages_total
        Loops      = $status.loop_suppressed_messages_total
        PubRate    = [math]::Round($publishedRate)
        E2eRate    = [math]::Round($e2eRate)
        p50        = [math]::Round($p50, 2)
        p95        = [math]::Round($p95, 2)
        p99        = [math]::Round($p99, 2)
        Seconds    = if ($publishLine -match "seconds=([0-9.]+)") { [double]$Matches[1] } else { 0 }
        OK         = $ok
        Error      = $errorText
    }
}

function Print-Table($Rows, [string]$Title) {
    Write-Section $Title
    $header = "{0,-20} {1,-16} {2,-14} {3,9} {4,9} {5,9} {6,9} {7,5} {8,5} {9,5} {10,9} {11,9} {12,7} {13,7} {14,7} {15,5}"
    Write-Host ($header -f "run", "input->output", "verify", "published", "relay_in", "relay_out", "received", "err", "unc", "shed", "publish/s", "deliver/s", "p50", "p95", "p99", "ok")
    foreach ($r in $Rows) {
        $io = "$($r.Input)->$($r.Output)"
        Write-Host ($header -f $r.Name, $io, $r.Verify, $r.Published, $r.RelayInput, $r.RelayOut, $r.Received, $r.Errors, $r.Uncertain, $r.Shed, $r.PubRate, $r.E2eRate, $r.p50, $r.p95, $r.p99, $r.OK)
    }
}

# ---------------------------------------------------------------------------

if (-not (Test-Path $relayExe)) {
    throw "relay binary not found: $relayExe (build with: cargo build --release --locked)"
}
if (-not $SkipBuild) {
    Write-Host "Building transport-compare harness..."
    $env:CARGO_TARGET_DIR = Join-Path $PSScriptRoot ".target"
    $buildLog = Join-Path $work "build.log"
    $code = Invoke-Native -File "cargo" -Arguments @("build", "--release", "--locked", "--manifest-path", (Join-Path $PSScriptRoot "Cargo.toml")) -StdOutFile $buildLog -StdErrFile (Join-Path $work "build.err")
    if ($code -ne 0) {
        Get-Content (Join-Path $work "build.err") -ErrorAction SilentlyContinue | Write-Host
        throw "harness build failed (exit $code)"
    }
}
if (-not (Test-Path $harnessExe)) {
    throw "harness binary not found: $harnessExe"
}

$acceptance = @()
$load = @()
$failures = @()

try {
    Write-Section "Starting brokers (compose project $Project)"
    Invoke-Compose @("up", "-d", "redis", "nats")
    Wait-ForRedis
    Wait-ForNats
    Write-Host "brokers ready"

    $port = $statusBasePort

    $acceptance += Invoke-Run -Name "smoke-redis-redis" -Template "redis-redis.json" -InputEngine "redis" `
        -VerifyEngines "redis" -Total $SmokeTotal -ExpectedPerOutput $SmokeTotal -Port $port
    $port++
    $acceptance += Invoke-Run -Name "smoke-redis-nats" -Template "redis-nats.json" -InputEngine "redis" `
        -VerifyEngines "nats" -Total $SmokeTotal -ExpectedPerOutput $SmokeTotal -Port $port
    $port++
    $acceptance += Invoke-Run -Name "smoke-nats-redis" -Template "nats-redis.json" -InputEngine "nats" `
        -VerifyEngines "redis" -Total $SmokeTotal -ExpectedPerOutput $SmokeTotal -Port $port
    $port++
    $acceptance += Invoke-Run -Name "smoke-nats-nats" -Template "nats-nats.json" -InputEngine "nats" `
        -VerifyEngines "nats" -Total $SmokeTotal -ExpectedPerOutput $SmokeTotal -Port $port
    $port++

    # Output filter: 1 in 10 messages is routed to a `drop` token the output
    # denies, so the rest must be forwarded and received.
    $expectedFilter = $SmokeTotal - [math]::Ceiling($SmokeTotal / 10)
    $acceptance += Invoke-Run -Name "filter-redis-out" -Template "redis-redis-filter.json" -InputEngine "redis" `
        -VerifyEngines "redis" -Total $SmokeTotal -ExpectedPerOutput $expectedFilter -Port $port -DenyEvery 10
    $port++

    # Mixed fan-out: one NATS input, one Redis output and one NATS output. Both
    # outputs must carry every message.
    $acceptance += Invoke-Run -Name "fanout-nats-mixed" -Template "mixed-fanout.json" -InputEngine "nats" `
        -VerifyEngines "redis,nats" -Total $SmokeTotal -ExpectedPerOutput $SmokeTotal -Port $port
    $port++

    # Loop guard: an overlapping NATS-to-NATS config must forward once and
    # suppress the re-entry.
    $acceptance += Invoke-Run -Name "loop-guard-nats" -Template "loop-guard.json" -InputEngine "nats" `
        -VerifyEngines "nats" -Total 1 -ExpectedPerOutput 1 -Port $port -ExpectedLoops 1 `
        -InPrefix "loop" -OutPrefix "loop.replay"
    $port++

    if (-not $SkipLoad) {
        Write-Section "Load runs ($LoadTotal messages each, target $LoadRate msg/s)"
        $load += Invoke-Run -Name "load-redis-redis" -Template "redis-redis.json" -InputEngine "redis" `
            -VerifyEngines "redis" -Total $LoadTotal -ExpectedPerOutput $LoadTotal -Port $port -Rate $LoadRate
        $port++
        $load += Invoke-Run -Name "load-redis-nats" -Template "redis-nats.json" -InputEngine "redis" `
            -VerifyEngines "nats" -Total $LoadTotal -ExpectedPerOutput $LoadTotal -Port $port -Rate $LoadRate
        $port++
        $load += Invoke-Run -Name "load-nats-redis" -Template "nats-redis.json" -InputEngine "nats" `
            -VerifyEngines "redis" -Total $LoadTotal -ExpectedPerOutput $LoadTotal -Port $port -Rate $LoadRate
        $port++
        $load += Invoke-Run -Name "load-nats-nats" -Template "nats-nats.json" -InputEngine "nats" `
            -VerifyEngines "nats" -Total $LoadTotal -ExpectedPerOutput $LoadTotal -Port $port -Rate $LoadRate
        $port++
    }
} finally {
    Stop-Harness
    Stop-Relay
    Write-Section "Tearing down compose project $Project"
    Invoke-Native -File "docker" -Arguments @("compose", "-p", $Project, "-f", $composeFile, "down", "--remove-orphans") -StdErrFile (Join-Path $work "teardown.err") | Out-Null
    foreach ($name in @("COMPARE_ENGINE", "COMPARE_TOTAL", "COMPARE_PAYLOAD_BYTES", "COMPARE_RATE",
            "COMPARE_READY_FILE", "COMPARE_INPUT_PREFIX", "COMPARE_EXPECTED", "COMPARE_VERIFY_ENGINES",
            "COMPARE_VERIFY_PREFIX", "COMPARE_WAIT_TIMEOUT_SECS", "COMPARE_FILTER_DENY_EVERY",
            "REDIS_URL", "REDIS_VERIFY_URL", "NATS_URL", "CARGO_TARGET_DIR")) {
        Remove-Item "Env:$name" -ErrorAction SilentlyContinue
    }
}

Print-Table $acceptance "Acceptance matrix"
if ($load.Count -gt 0) { Print-Table $load "Load matrix" }

foreach ($row in @($acceptance) + @($load)) {
    if (-not $row.OK) { $failures += $row }
}

if ($failures.Count -gt 0) {
    Write-Host ""
    Write-Host "FAILED runs:" -ForegroundColor Red
    foreach ($row in $failures) {
        Write-Host ("  {0}: relay_err={1}" -f $row.Name, $row.Error) -ForegroundColor Red
    }
    exit 1
}

Write-Host ""
Write-Host "All runs green." -ForegroundColor Green
exit 0
