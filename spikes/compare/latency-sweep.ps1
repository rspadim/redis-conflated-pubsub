# Latency-vs-rate sweep for the transport comparison harness.
# Runs the SAME target rate for both engines, in separate processes, and prints
# one line per (engine, rate) so the two engines are directly comparable.
param(
    [string]$Engines = "nats,redis",
    [string]$Rates = "500,1000,2000,4000,8000",
    [int]$Total = 40000,
    [int]$PayloadBytes = 256
)

$ErrorActionPreference = "Stop"
$repoRoot = (Resolve-Path "$PSScriptRoot/../..").Path
$targetDir = Join-Path $PSScriptRoot ".target"
$exe = Join-Path $targetDir "release/transport-compare.exe"
if (-not (Test-Path $exe)) { throw "build first: cargo build --release --manifest-path spikes/compare/Cargo.toml" }

$env:CARGO_TARGET_DIR = $targetDir
$env:COMPARE_TOTAL = "$Total"
$env:COMPARE_PAYLOAD_BYTES = "$PayloadBytes"
$env:COMPARE_BATCH = "1"
$env:NATS_URL = "nats://127.0.0.1:14230"
$env:REDIS_URL = "redis://127.0.0.1:16390"

$results = @()
foreach ($engine in $Engines.Split(",")) {
    $env:COMPARE_ENGINE = $engine
    foreach ($rate in $Rates.Split(",")) {
        $env:COMPARE_RATE = $rate
        $prefix = "s$([guid]::NewGuid().ToString('N').Substring(0,10))"
        $env:COMPARE_PREFIX = $prefix
        $ready = Join-Path $env:TEMP "cmp_ready_$prefix"
        Remove-Item $ready -ErrorAction SilentlyContinue
        $env:COMPARE_READY_FILE = $ready
        $subOut = Join-Path $env:TEMP "cmp_sub_$prefix.txt"
        $pubOut = Join-Path $env:TEMP "cmp_pub_$prefix.txt"

        $sub = Start-Process -FilePath $exe -ArgumentList "subscriber" -PassThru -NoNewWindow -RedirectStandardOutput $subOut
        Start-Sleep -Milliseconds 500
        & $exe publisher *> $pubOut
        $sub.WaitForExit(300000) | Out-Null

        $pubLine = (Get-Content $pubOut -ErrorAction SilentlyContinue | Select-String "publish_rate_msg_s").Line
        $ageLine = (Get-Content $subOut -ErrorAction SilentlyContinue | Select-String "age_ms").Line
        $recvLine = (Get-Content $subOut -ErrorAction SilentlyContinue | Select-String "received=").Line
        $achieved = if ($pubLine -match "seconds=([0-9.]+)") { [math]::Round($Total / [double]$Matches[1]) } else { 0 }

        $p50 = $p95 = $p99 = 0
        if ($ageLine -match "p50=([0-9.]+)") { $p50 = [double]$Matches[1] }
        if ($ageLine -match "p95=([0-9.]+)") { $p95 = [double]$Matches[1] }
        if ($ageLine -match "p99=([0-9.]+)") { $p99 = [double]$Matches[1] }
        $complete = $recvLine -match "complete=true"

        $results += [pscustomobject]@{
            Engine = $engine; TargetRate = [int]$rate; Achieved = $achieved
            p50 = $p50; p95 = $p95; p99 = $p99; Complete = $complete
        }
        Write-Host ("{0,-6} target={1,-6} achieved={2,-7} p50={3,-8} p95={4,-8} p99={5,-8} complete={6}" -f $engine, $rate, $achieved, $p50, $p95, $p99, $complete)
    }
}

""
"| Engine | Target msg/s | Achieved msg/s | p50 ms | p95 ms | p99 ms | Complete |"
"| --- | ---: | ---: | ---: | ---: | ---: | --- |"
foreach ($r in $results) {
    "| $($r.Engine) | $($r.TargetRate) | $($r.Achieved) | $($r.p50) | $($r.p95) | $($r.p99) | $($r.Complete) |"
}
