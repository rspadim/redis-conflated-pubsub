#!/bin/bash
set -Eeuo pipefail

case "${KV_ENGINE,,}" in
    redis)
        SERVER_BIN=/usr/bin/redis-server
        ;;
    valkey)
        SERVER_BIN=/usr/local/bin/valkey-server
        ;;
    *)
        echo "KV_ENGINE must be either 'redis' or 'valkey'." >&2
        exit 64
        ;;
esac

if [[ -n "${REDIS_PASSWORD_FILE:-}" ]]; then
    if [[ ! -r "$REDIS_PASSWORD_FILE" ]]; then
        echo "REDIS_PASSWORD_FILE is not readable." >&2
        exit 78
    fi
    REDIS_PASSWORD="$(<"$REDIS_PASSWORD_FILE")"
    export REDIS_PASSWORD
fi

if [[ -z "${REDIS_PASSWORD:-}" ]]; then
    echo "Set REDIS_PASSWORD or REDIS_PASSWORD_FILE before starting the bundle." >&2
    exit 78
fi
if [[ ! -r "$CONFIG_FILE" ]]; then
    echo "Config file is not readable: $CONFIG_FILE" >&2
    exit 78
fi

mkdir -p /data/raw /data/conflated /state

"$SERVER_BIN" \
    --bind 0.0.0.0 \
    --port 6379 \
    --protected-mode yes \
    --requirepass "$REDIS_PASSWORD" \
    --dir /data/raw \
    --appendonly yes \
    --appendfilename appendonly.aof \
    --save "" &
raw_pid=$!

"$SERVER_BIN" \
    --bind 0.0.0.0 \
    --port 6380 \
    --protected-mode yes \
    --requirepass "$REDIS_PASSWORD" \
    --dir /data/conflated \
    --appendonly yes \
    --appendfilename appendonly.aof \
    --save "" &
conflated_pid=$!

/usr/local/bin/redis-conflated-pubsub --config "$CONFIG_FILE" &
relay_pid=$!
printf '%s\n' "$relay_pid" > /tmp/relay.pid

shutdown() {
    trap - EXIT TERM INT
    kill "$relay_pid" "$raw_pid" "$conflated_pid" 2>/dev/null || true
    wait "$relay_pid" 2>/dev/null || true
    wait "$raw_pid" 2>/dev/null || true
    wait "$conflated_pid" 2>/dev/null || true
    rm -f /tmp/relay.pid
}
trap shutdown EXIT
trap 'exit 143' TERM
trap 'exit 130' INT

set +e
wait -n "$raw_pid" "$conflated_pid" "$relay_pid"
exit_code=$?
set -e
exit "$exit_code"
