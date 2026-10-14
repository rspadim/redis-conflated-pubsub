#!/bin/sh
# Local NATS authentication matrix (Linux/macOS). Windows uses test-auth.ps1.
# Creates disposable random credentials under the gitignored certs/auth dir;
# never uses production credentials.
set -eu

root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$root/spikes/nats"

env_file="certs/auth/runtime.env"
mkdir -p certs/auth certs/auth/certs
if [ ! -f "$env_file" ]; then
  {
    echo "NATS_SPIKE_USER=relay-spike"
    echo "NATS_SPIKE_PASSWORD=$(head -c 16 /dev/urandom | od -An -tx1 | tr -d ' \n')"
    echo "NATS_SPIKE_TOKEN=$(head -c 16 /dev/urandom | od -An -tx1 | tr -d ' \n')"
  } > "$env_file"
fi

compose="docker compose --env-file $env_file -p nats-auth -f compose.auth.yml"
cleanup() { $compose down --remove-orphans || true; }
trap cleanup EXIT

$compose run --rm auth-material
$compose run --rm auth-certs
$compose up -d nats-userpass nats-token nats-nkey nats-credentials nats-mtls
sleep 2

settings() { grep -E "^$1=" "$env_file" | cut -d= -f2; }
run_mode() {
  NATS_AUTH_MODE="$1" NATS_SERVERS="$2" NATS_AUTH_ROUNDTRIP=1 NATS_PROBE_ONLY=0 \
    cargo run --quiet --locked --manifest-path Cargo.toml
}

NATS_USERNAME="$(settings NATS_SPIKE_USER)" NATS_PASSWORD="$(settings NATS_SPIKE_PASSWORD)" \
  run_mode user_password "nats://127.0.0.1:14401"
NATS_TOKEN="$(settings NATS_SPIKE_TOKEN)" run_mode token "nats://127.0.0.1:14402"
NATS_NKEY_SEED_FILE="$PWD/certs/auth/nkey.seed" run_mode nkey "nats://127.0.0.1:14403"
NATS_CREDENTIALS_FILE="$PWD/certs/auth/nkeys/creds/SPIKE/SPIKE/SPIKE.creds" \
  run_mode credentials_file "nats://127.0.0.1:14404"
NATS_ROOT_CERT_FILE="$PWD/certs/auth/certs/ca-cert.pem" \
  NATS_CLIENT_CERT_FILE="$PWD/certs/auth/certs/client-cert.pem" \
  NATS_CLIENT_KEY_FILE="$PWD/certs/auth/certs/client-key.pem" \
  run_mode mtls "tls://localhost:14405"

echo "nats_auth_matrix=ok"
