<#
.SYNOPSIS
Runs the NATS spike load harness while stopping one cluster node, to show that
traffic continues across a node outage.

.DESCRIPTION
Starts the release load harness (`NATS_LOAD_TEST=1`, `roundtrip` mode) with a
total large enough to span the outage window, stops `nats1` after a warm-up,
waits for the configured outage, starts `nats1` again, then waits for the run to
finish. The harness summary (`load_summary ...`) is parsed to report delivered
count, completeness, publish/flush errors, and rate.

This reuses the Compose project/stop/start pattern from
`spikes/nats/test-failover.ps1`, but keeps load flowing instead of running the
short ordered probe.

.EXAMPLE
./test-failover-load.ps1

.EXAMPLE
./test-failover-load.ps1 -Total 2000000 -OutageSeconds 8 -Flush 128
#>
param(
    [int]$Total = 3000000,
    [int]$Publishers = 4,
    [int]$PayloadBytes = 256,
    [int]$Flush = 64,
    [int]$WarmupSeconds = 5,
    [int]$OutageSeconds = 6,
    [int]$FinishTimeoutSeconds = 180,
    [string]$ComposeProject = "nats-spike-failover-load",
    [string]$Servers = "nats://127.0.0.1:14222,nats://127.0.0.1:14223,nats://127.0.0.1:14224"
)

$ErrorActionPreference = "Stop"
$repositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$spikeRoot = $PSScriptRoot
$composeArgs = @("-p", $ComposeProject, "-f", "spikes/nats/compose.yml")
$logId = [guid]::NewGuid().ToString("N")
$stdoutPath = Join-Path $env:TEMP "nats-failover-load-$logId.stdout.log"
$stderrPath = Join-Path $env:TEMP "nats-failover-load-$logId.stderr.log"
$process = $null
$nats1Stopped = $false
$composeUp = $false

Push-Location $repositoryRoot
try {
    $env:CARGO_TARGET_DIR = Join-Path $spikeRoot ".target"
    & cargo build --release --locked --manifest-path "spikes/nats/Cargo.toml"
    if ($LASTEXITCODE -ne 0) { throw "cargo build failed" }
    $exe = Join-Path $spikeRoot ".target\release\nats-transport-spike.exe"

    & docker compose @composeArgs up -d
    if ($LASTEXITCODE -ne 0) { throw "docker compose up failed" }
    $composeUp = $true
    Start-Sleep -Seconds 5

    $env:NATS_LOAD_TEST = "1"
    $env:NATS_LOAD_MODE = "roundtrip"
    $env:NATS_LOAD_TOTAL = "$Total"
    $env:NATS_LOAD_PUBLISHERS = "$Publishers"
    $env:NATS_LOAD_PAYLOAD_BYTES = "$PayloadBytes"
    $env:NATS_LOAD_FLUSH_EVERY = "$Flush"
    $env:NATS_LOAD_LABEL = "failover-load"
    $env:NATS_LOAD_TRANSPORT = "tcp"
    $env:NATS_LOAD_WAIT_SECS = "30"
    $env:NATS_SERVERS = $Servers
    $env:NATS_ECHO_PROBE = "0"

    Write-Host "Starting load: total=$Total publishers=$Publishers payload=$PayloadBytes flush=$Flush"
    $process = Start-Process -FilePath $exe -PassThru -NoNewWindow `
        -RedirectStandardOutput $stdoutPath -RedirectStandardError $stderrPath

    Start-Sleep -Seconds $WarmupSeconds
    if ($process.HasExited) {
        Get-Content $stdoutPath, $stderrPath -ErrorAction SilentlyContinue
        throw "load harness exited before the failover window (total too small?)"
    }

    Write-Host "Stopping nats1 for $OutageSeconds s (load still flowing)..."
    & docker compose @composeArgs stop nats1
    $nats1Stopped = $true
    Start-Sleep -Seconds $OutageSeconds
    & docker compose @composeArgs start nats1
    $nats1Stopped = $false
    Write-Host "nats1 restarted; waiting for load to finish..."

    $deadline = (Get-Date).AddSeconds($FinishTimeoutSeconds)
    while (-not $process.HasExited -and (Get-Date) -lt $deadline) {
        Start-Sleep -Milliseconds 500
    }
    if (-not $process.HasExited) {
        Stop-Process -Id $process.Id -Force
        throw "load harness did not finish within $FinishTimeoutSeconds s after the outage"
    }

    $output = @(Get-Content $stdoutPath -ErrorAction SilentlyContinue)
    $errors = Get-Content $stderrPath -ErrorAction SilentlyContinue
    $output | ForEach-Object { Write-Host $_ }
    if ($errors) { Write-Host "stderr: $($errors -join ' ')" }

    $summaryLine = $output | Where-Object { $_ -like "load_summary *" } | Select-Object -Last 1
    if (-not $summaryLine) { throw "no load_summary line; stderr: $($errors -join ' ')" }
    $fields = @{}
    foreach ($token in $summaryLine.Substring("load_summary ".Length).Split(" ")) {
        $pair = $token.Split("=", 2)
        if ($pair.Length -eq 2) { $fields[$pair[0]] = $pair[1] }
    }

    $delivered = [int]$fields["delivered"]
    $complete = $fields["complete"]
    $publishErrors = [int]$fields["publish_errors"]
    $flushErrors = [int]$fields["flush_errors"]
    $rate = [double]$fields["publish_rate_msg_s"]

    Write-Host ""
    Write-Host ("failover_load node_down={0}s total={1} delivered={2} complete={3} publish_errors={4} flush_errors={5} publish_rate_msg_s={6:N0}" -f `
        $OutageSeconds, $Total, $delivered, $complete, $publishErrors, $flushErrors, $rate)

    if ($delivered -lt ($Total * 0.98)) {
        throw "traffic did not continue: delivered $delivered of $Total after a $OutageSeconds s node outage"
    }
    Write-Host "traffic_continued=true"
}
finally {
    if ($nats1Stopped) {
        & docker compose @composeArgs start nats1
    }
    if ($process -and -not $process.HasExited) {
        Stop-Process -Id $process.Id -Force
    }
    if ($composeUp) {
        & docker compose @composeArgs down --remove-orphans
    }
    Remove-Item $stdoutPath, $stderrPath -ErrorAction SilentlyContinue
    Pop-Location
}
