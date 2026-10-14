$ErrorActionPreference = "Stop"
$repositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$authDirectory = Join-Path $PSScriptRoot "certs\auth"
$runtimeEnv = Join-Path $authDirectory "runtime.env"
$composeArgs = @("--env-file", $runtimeEnv, "-p", "nats-auth", "-f", "spikes/nats/compose.auth.yml")

if (-not (Test-Path $runtimeEnv)) {
    New-Item -ItemType Directory -Path $authDirectory -Force | Out-Null
    $runtimeSettings = @(
        "NATS_SPIKE_USER=relay-spike",
        "NATS_SPIKE_PASSWORD=$([guid]::NewGuid().ToString('N'))",
        "NATS_SPIKE_TOKEN=$([guid]::NewGuid().ToString('N'))"
    )
    [System.IO.File]::WriteAllLines($runtimeEnv, $runtimeSettings, [System.Text.UTF8Encoding]::new($false))
}

$settings = @{}
foreach ($line in Get-Content $runtimeEnv) {
    if ($line -match '^([^=]+)=(.*)$') {
        $settings[$matches[1]] = $matches[2]
    }
}

Push-Location $repositoryRoot
try {
    $env:CARGO_TARGET_DIR = Join-Path $repositoryRoot "target"
    & docker compose @composeArgs run --rm auth-material
    & docker compose @composeArgs run --rm auth-certs
    & docker compose @composeArgs up -d nats-userpass nats-token nats-nkey nats-credentials nats-mtls
    Start-Sleep -Seconds 2

    function Invoke-AuthRoundtrip([string]$mode, [string]$server) {
        $env:NATS_AUTH_MODE = $mode
        $env:NATS_SERVERS = $server
        $env:NATS_AUTH_ROUNDTRIP = "1"
        $env:NATS_PROBE_ONLY = "0"
        & cargo run --quiet --locked --manifest-path spikes/nats/Cargo.toml
        if ($LASTEXITCODE -ne 0) {
            throw "NATS $mode authentication roundtrip failed"
        }
    }

    $env:NATS_USERNAME = $settings["NATS_SPIKE_USER"]
    $env:NATS_PASSWORD = $settings["NATS_SPIKE_PASSWORD"]
    Invoke-AuthRoundtrip "user_password" "nats://127.0.0.1:14401"

    $env:NATS_TOKEN = $settings["NATS_SPIKE_TOKEN"]
    Invoke-AuthRoundtrip "token" "nats://127.0.0.1:14402"

    $env:NATS_NKEY_SEED_FILE = Join-Path $authDirectory "nkey.seed"
    Invoke-AuthRoundtrip "nkey" "nats://127.0.0.1:14403"

    $env:NATS_CREDENTIALS_FILE = Join-Path $authDirectory "nkeys\creds\SPIKE\SPIKE\SPIKE.creds"
    Invoke-AuthRoundtrip "credentials_file" "nats://127.0.0.1:14404"

    $env:NATS_ROOT_CERT_FILE = Join-Path $authDirectory "certs\ca-cert.pem"
    $env:NATS_CLIENT_CERT_FILE = Join-Path $authDirectory "certs\client-cert.pem"
    $env:NATS_CLIENT_KEY_FILE = Join-Path $authDirectory "certs\client-key.pem"
    Invoke-AuthRoundtrip "mtls" "tls://localhost:14405"

    $env:NATS_PASSWORD = "deliberately-invalid-local-test-password"
    $env:NATS_AUTH_MODE = "user_password"
    $env:NATS_SERVERS = "nats://127.0.0.1:14401"
    $env:NATS_AUTH_ROUNDTRIP = "0"
    $previousErrorActionPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $null = & cargo run --quiet --locked --manifest-path spikes/nats/Cargo.toml 2>&1
        $invalidPasswordExitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }
    if ($invalidPasswordExitCode -eq 0) {
        throw "NATS accepted an invalid local test password"
    }
    "wrong_user_password_rejected=true"
}
finally {
    & docker compose @composeArgs down --remove-orphans
    Pop-Location
}
