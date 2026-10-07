#!/bin/bash
set -euo pipefail

case "${KV_ENGINE,,}" in
    redis) CLI=/usr/bin/redis-cli ;;
    valkey) CLI=/usr/local/bin/valkey-cli ;;
    *) exit 1 ;;
esac

[[ -s /tmp/relay.pid ]] || exit 1
kill -0 "$(</tmp/relay.pid)" 2>/dev/null || exit 1

export REDISCLI_AUTH="${REDIS_PASSWORD:-}"
"$CLI" -h 127.0.0.1 -p 6379 ping 2>/dev/null | grep -qx PONG
"$CLI" -h 127.0.0.1 -p 6380 ping 2>/dev/null | grep -qx PONG
