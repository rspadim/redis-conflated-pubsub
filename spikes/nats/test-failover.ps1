$ErrorActionPreference = "Stop"
$repositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$composeArgs = @("-p", "nats-spike", "-f", "spikes/nats/compose.yml")
$logId = [guid]::NewGuid().ToString("N")
$stdoutPath = Join-Path $env:TEMP "nats-spike-$logId.stdout.log"
$stderrPath = Join-Path $env:TEMP "nats-spike-$logId.stderr.log"
$process = $null
$nats1Stopped = $false

Push-Location $repositoryRoot
try {
    $env:CARGO_TARGET_DIR = Join-Path $repositoryRoot "target"
    $env:NATS_SERVERS = "nats://127.0.0.1:14222,nats://127.0.0.1:14223,nats://127.0.0.1:14224"
    $env:NATS_WS_URL = "ws://127.0.0.1:18080"
    $env:REDIS_URL = "redis://127.0.0.1:16380"
    $env:NATS_FAILOVER_PROBE_SECS = "20"

    & docker compose @composeArgs up -d
    $cargo = (Get-Command cargo).Source
    $process = Start-Process -FilePath $cargo `
        -ArgumentList @("run", "--locked", "--manifest-path", "spikes/nats/Cargo.toml") `
        -WorkingDirectory $repositoryRoot `
        -PassThru `
        -RedirectStandardOutput $stdoutPath `
        -RedirectStandardError $stderrPath

    Start-Sleep -Seconds 4
    if ($process.HasExited) {
        Get-Content $stdoutPath, $stderrPath -ErrorAction SilentlyContinue
        throw "NATS spike exited before the failover window started"
    }

    & docker compose @composeArgs stop nats1
    $nats1Stopped = $true
    Start-Sleep -Seconds 5
    & docker compose @composeArgs start nats1
    $nats1Stopped = $false

    if (-not $process.WaitForExit(90000)) {
        throw "NATS failover spike did not finish within 90 seconds"
    }
    Get-Content $stdoutPath, $stderrPath -ErrorAction SilentlyContinue
    $process.Refresh()
    $exitCode = $process.ExitCode
    if ($null -ne $exitCode -and $exitCode -ne 0) {
        throw "NATS failover spike exited with code $exitCode"
    }
}
finally {
    if ($nats1Stopped) {
        & docker compose @composeArgs start nats1
    }
    if ($process -and -not $process.HasExited) {
        Stop-Process -Id $process.Id -Force
    }
    Pop-Location
}
