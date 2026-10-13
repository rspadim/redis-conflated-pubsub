<#
.SYNOPSIS
Runs the NATS spike load harness across a publishers x payload x flush matrix,
over TCP and/or WSS, and writes CSV + Markdown result tables.

.DESCRIPTION
Each cell runs the release `nats-transport-spike` binary with
`NATS_LOAD_TEST=1` and the `NATS_LOAD_*` knobs, capturing the harness's
machine-readable `load_summary` line. The harness measures the raw async-nats
client path, not the relay.

The cluster is the three-node Compose stack in spikes/nats/compose.yml. TCP
targets all three nodes; WSS targets the TLS WebSocket listener on nats2 using
the locally generated CA in spikes/nats/certs.

.EXAMPLE
# Full default sweep over TCP only.
./load-matrix.ps1 -Transports tcp

.EXAMPLE
# Smaller TCP+WSS sweep with three repeats to expose run-to-run variance.
./load-matrix.ps1 -Transports tcp,wss -Publishers 1,4,16 -Payloads 64,1024 `
  -Flushes 16,256 -Total 100000 -Repeats 3

.NOTES
Synthetic data only. Never point this at production subjects or servers.
#>
param(
    [string]$Transports = "tcp",
    [string]$Publishers = "1,4,16",
    [string]$Payloads = "64,256,1024",
    [string]$Flushes = "16,64,256",
    [int]$Total = 200000,
    [int]$Repeats = 1,
    [int]$WaitSecs = 20,
    [int]$CellTimeoutSeconds = 150,
    [ValidateSet("roundtrip", "publish")]
    [string]$Mode = "roundtrip",
    [string]$Label = "matrix",
    [string]$OutCsv = "",
    [string]$OutMd = "",
    [string]$ComposeProject = "nats-spike-load",
    [string]$TcpServers = "nats://127.0.0.1:14222,nats://127.0.0.1:14223,nats://127.0.0.1:14224",
    [string]$WssServers = "wss://localhost:18443",
    [switch]$SkipComposeUp,
    [switch]$SkipComposeDown
)

$ErrorActionPreference = "Stop"
# Force invariant number formatting so CSV/Markdown are stable regardless of host locale.
[System.Threading.Thread]::CurrentThread.CurrentCulture = [System.Globalization.CultureInfo]::InvariantCulture
# Accept comma/space-separated lists so `-File` invocations (which coerce arrays
# to "1 4 16") and `-Command` invocations both work.
$transportList = @($Transports -split '[,;\s]+' | Where-Object { $_ })
$publisherList = @($Publishers -split '[,;\s]+' | Where-Object { $_ } | ForEach-Object { [int]$_ } | Where-Object { $_ -gt 0 })
$payloadList = @($Payloads -split '[,;\s]+' | Where-Object { $_ } | ForEach-Object { [int]$_ } | Where-Object { $_ -gt 0 })
$flushList = @($Flushes -split '[,;\s]+' | Where-Object { $_ } | ForEach-Object { [int]$_ } | Where-Object { $_ -gt 0 })
$totalCells = $transportList.Count * $publisherList.Count * $payloadList.Count * $flushList.Count * $Repeats
$repositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$spikeRoot = $PSScriptRoot
$composeFile = "spikes/nats/compose.yml"
$composeArgs = @("-p", $ComposeProject, "-f", $composeFile)
$wssCa = Join-Path $spikeRoot "certs\ca-cert.pem"
$resultsDir = Join-Path $spikeRoot "results"
if (-not (Test-Path $resultsDir)) { New-Item -ItemType Directory -Path $resultsDir -Force | Out-Null }
$stamp = Get-Date -Format "yyyyMMdd-HHmmss"
if (-not $OutCsv) { $OutCsv = Join-Path $resultsDir "load-matrix-$stamp.csv" }
if (-not $OutMd) { $OutMd = Join-Path $resultsDir "load-matrix-$stamp.md" }

function Get-TransportConfig([string]$transport) {
    switch ($transport.ToLowerInvariant()) {
        "tcp" { return @{ Servers = $TcpServers; Ca = ""; Tag = "tcp" } }
        "wss" { return @{ Servers = $WssServers; Ca = $wssCa; Tag = "wss" } }
        default { throw "Unknown transport '$transport' (expected tcp or wss)" }
    }
}

function Invoke-Cell([string]$transport, [int]$publishers, [int]$payload, [int]$flush, [int]$repeat) {
    $cfg = Get-TransportConfig $transport
    $exe = Join-Path $spikeRoot ".target\release\nats-transport-spike.exe"

    $env:NATS_LOAD_TEST = "1"
    $env:NATS_LOAD_MODE = $Mode
    $env:NATS_LOAD_TOTAL = "$Total"
    $env:NATS_LOAD_PUBLISHERS = "$publishers"
    $env:NATS_LOAD_PAYLOAD_BYTES = "$payload"
    $env:NATS_LOAD_FLUSH_EVERY = "$flush"
    $env:NATS_LOAD_LABEL = $Label
    $env:NATS_LOAD_TRANSPORT = $cfg.Tag
    $env:NATS_LOAD_WAIT_SECS = "$WaitSecs"
    $env:NATS_SERVERS = $config.Servers
    $env:NATS_ECHO_PROBE = "0"
    if ($config.Ca) { $env:NATS_ROOT_CERT_FILE = $config.Ca } else { Remove-Item Env:NATS_ROOT_CERT_FILE -ErrorAction SilentlyContinue }

    # System.Diagnostics.Process gives a reliable ExitCode plus a hard timeout so a
    # stalled transport (e.g. an unread WebSocket connection) cannot hang the sweep.
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = $exe
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError = $true
    $psi.UseShellExecute = $false
    $proc = [System.Diagnostics.Process]::Start($psi)
    $stdoutTask = $proc.StandardOutput.ReadToEndAsync()
    $proc.WaitForExit($CellTimeoutSeconds * 1000) | Out-Null
    $timedOut = $false
    if (-not $proc.HasExited) {
        $timedOut = $true
        try { $proc.Kill($true) } catch { $proc.Kill() }
        $proc.WaitForExit(10000) | Out-Null
    }
    $output = @()
    try { $output = @(($stdoutTask.GetAwaiter().GetResult()) -split "`r?`n" | Where-Object { $_ }) } catch { }
    $exitCode = if ($proc.HasExited) { $proc.ExitCode } else { $null }

    $summaryLine = $output | Where-Object { $_ -like "load_summary *" } | Select-Object -Last 1
    if ($timedOut -or $exitCode -ne 0 -or -not $summaryLine) {
        $reason = if ($timedOut) { "timed out after $CellTimeoutSeconds s" } else { "exited ${exitCode}" }
        Write-Warning "cell $transport pub=$publishers payload=$payload flush=$flush $reason; marking incomplete"
        $row = [pscustomobject]@{
            transport          = $transport.ToLowerInvariant()
            publishers         = $publishers
            payload_bytes      = $payload
            flush_every        = $flush
            repeat             = $repeat
            mode               = $Mode
            total              = $Total
            publish_rate_msg_s = 0
            e2e_rate_msg_s     = 0
            delivered          = 0
            complete           = "false"
            publish_errors     = 0
            flush_errors       = 0
            elapsed_s          = 0
            rtt_p50_ms         = $null
            rtt_p95_ms         = $null
            rtt_p99_ms         = $null
            rtt_max_ms         = $null
        }
        return [pscustomobject]$row
    }
    Write-Host "  $transport pub=$publishers payload=$payload flush=$flush rep=$repeat ok"
    $fields = @{}
    foreach ($token in $summaryLine.Substring("load_summary ".Length).Split(" ")) {
        $pair = $token.Split("=", 2)
        if ($pair.Length -eq 2) { $fields[$pair[0]] = $pair[1] }
    }
    $row = [ordered]@{
        transport           = $transport.ToLowerInvariant()
        publishers          = $publishers
        payload_bytes       = $payload
        flush_every         = $flush
        repeat              = $repeat
        mode                = $fields["mode"]
        total               = [int]$fields["total"]
        publish_rate_msg_s  = [double]$fields["publish_rate_msg_s"]
        e2e_rate_msg_s      = [double]$fields["end_to_end_rate_msg_s"]
        delivered           = [int]$fields["delivered"]
        complete            = $fields["complete"]
        publish_errors      = [int]$fields["publish_errors"]
        flush_errors        = [int]$fields["flush_errors"]
        elapsed_s           = [double]$fields["elapsed_s"]
        rtt_p50_ms          = if ($fields.ContainsKey("rtt_p50_ms")) { [double]$fields["rtt_p50_ms"] } else { $null }
        rtt_p95_ms          = if ($fields.ContainsKey("rtt_p95_ms")) { [double]$fields["rtt_p95_ms"] } else { $null }
        rtt_p99_ms          = if ($fields.ContainsKey("rtt_p99_ms")) { [double]$fields["rtt_p99_ms"] } else { $null }
        rtt_max_ms          = if ($fields.ContainsKey("rtt_max_ms")) { [double]$fields["rtt_max_ms"] } else { $null }
    }
    $rate = "{0:N0}" -f $row.publish_rate_msg_s
    Write-Host ("  {0,-3} pub={1,-2} payload={2,-5} flush={3,-4} rep={4} rate={5} msg/s complete={6}" -f `
        $row.transport, $row.publishers, $row.payload_bytes, $row.flush_every, $row.repeat, $rate, $row.complete)
    return [pscustomobject]$row
}

function Get-Median([double[]]$values) {
    if (-not $values -or $values.Count -eq 0) { return $null }
    $sorted = $values | Sort-Object
    $mid = [int][math]::Floor($sorted.Count / 2)
    if ($sorted.Count % 2 -eq 1) { return [double]$sorted[$mid] }
    return ([double]$sorted[$mid - 1] + [double]$sorted[$mid]) / 2.0
}

Push-Location $repositoryRoot
$composeUp = $false
try {
    $env:CARGO_TARGET_DIR = Join-Path $spikeRoot ".target"
    Write-Host "Building release binary (CARGO_TARGET_DIR=$($env:CARGO_TARGET_DIR))..."
    & cargo build --release --locked --manifest-path "spikes/nats/Cargo.toml"
    if ($LASTEXITCODE -ne 0) { throw "cargo build failed" }

    if (-not $SkipComposeUp) {
        Write-Host "Starting Compose project '$ComposeProject'..."
        & docker compose @composeArgs up -d
        if ($LASTEXITCODE -ne 0) { throw "docker compose up failed" }
        $composeUp = $true
        Start-Sleep -Seconds 5
    }

    $rows = @()
    $cellIndex = 0
    foreach ($transportName in $transportList) {
        Write-Host "Transport: $transportName"
        foreach ($pubCount in $publisherList) {
            foreach ($payloadBytes in $payloadList) {
                foreach ($flushEvery in $flushList) {
                    for ($repeatIndex = 1; $repeatIndex -le $Repeats; $repeatIndex++) {
                        $cellIndex++
                        try {
                            $rows += Invoke-Cell $transportName $pubCount $payloadBytes $flushEvery $repeatIndex
                        }
                        catch {
                            Write-Warning "cell $transportName pub=$pubCount payload=$payloadBytes flush=$flushEvery rep=$repeatIndex failed: $($_.Exception.Message)"
                        }
                        # Write after every cell so a later timeout/crash still leaves
                        # a usable CSV (and a partial Markdown rendering).
                        $rows | Export-Csv -Path $OutCsv -NoTypeInformation -Encoding UTF8
                        if ($cellIndex % 6 -eq 0 -or $cellIndex -eq $totalCells) {
                            Write-Progress -Activity "load matrix" -Status "$cellIndex / $totalCells cells" -PercentComplete (100 * $cellIndex / $totalCells)
                        }
                    }
                }
            }
        }
    }
    $rows | Export-Csv -Path $OutCsv -NoTypeInformation -Encoding UTF8
    Write-Host "Wrote $OutCsv"

    # Markdown: median across repeats per cell.
    $lines = New-Object System.Collections.Generic.List[string]
    $lines.Add("# NATS spike load matrix")
    $lines.Add("")
    $lines.Add("Harness: ``spikes/nats/src/load.rs`` (``NATS_LOAD_TEST=1``), mode ``$Mode``, total ``$Total`` msgs/cell, repeats ``$Repeats``.")
    $lines.Add("Synthetic loopback subscriber, release build. Raw async-nats client path; not the relay.")
    $lines.Add("")
    foreach ($transportTag in ($rows.transport | Select-Object -Unique)) {
        $lines.Add("## $transportTag")
        $lines.Add("")
        $lines.Add("| Publishers | Payload B | Flush | Publish msg/s | E2E msg/s | RTT p50 ms | RTT p95 ms | RTT p99 ms | Delivered | Complete |")
        $lines.Add("| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |")
        foreach ($cell in ($rows | Where-Object { $_.transport -eq $transportTag } | Group-Object "publishers", "payload_bytes", "flush_every" | Sort-Object { [int]($_.Group[0].publishers); [int]($_.Group[0].payload_bytes); [int]($_.Group[0].flush_every) })) {
            $group = $cell.Group
            $first = $group[0]
            $pubRate = Get-Median ([double[]]($group.publish_rate_msg_s))
            $e2eRate = Get-Median ([double[]]($group.e2e_rate_msg_s))
            $p50 = Get-Median ([double[]]($group.rtt_p50_ms))
            $p95 = Get-Median ([double[]]($group.rtt_p95_ms))
            $p99 = Get-Median ([double[]]($group.rtt_p99_ms))
            $delivered = Get-Median ([double[]]($group.delivered))
            $complete = ($group | Where-Object { $_.complete -ne "true" }).Count -eq 0
            $completeText = if ($complete) { "true" } else { "false" }
            $fmt = { param($v) if ($null -eq $v) { "-" } else { "{0:N1}" -f $v } }
            $lines.Add(("| {0} | {1} | {2} | {3} | {4} | {5} | {6} | {7} | {8:N0} | {9} |" -f `
                $first.publishers, $first.payload_bytes, $first.flush_every, `
                (& $fmt $pubRate), (& $fmt $e2eRate), (& $fmt $p50), (& $fmt $p95), (& $fmt $p99), `
                $delivered, $completeText))
        }
        $lines.Add("")
        if ($Repeats -gt 1) {
            $spread = $rows | Where-Object { $_.transport -eq $transportTag } | Group-Object "publishers", "payload_bytes", "flush_every" | ForEach-Object {
                $rates = [double[]]($_.Group.publish_rate_msg_s)
                $median = Get-Median $rates
                if ($median -gt 0) { (($rates | ForEach-Object { [math]::Abs($_ - $median) }) | Measure-Object -Maximum).Maximum / $median * 100.0 }
            }
            $spreadValues = @($spread | Where-Object { $null -ne $_ })
            if ($spreadValues.Count -gt 0) {
                $worst = ($spreadValues | Measure-Object -Maximum).Maximum
                $lines.Add(("Run-to-run spread vs median: worst {0:N1}% across cells (repeats={1})." -f $worst, $Repeats))
            }
        }
    }
    [System.IO.File]::WriteAllLines($OutMd, $lines, [System.Text.UTF8Encoding]::new($false))
    Write-Host "Wrote $OutMd"
}
finally {
    if ($composeUp -and -not $SkipComposeDown) {
        Write-Host "Tearing down Compose project '$ComposeProject'..."
        & docker compose @composeArgs down --remove-orphans
    }
    Pop-Location
}
