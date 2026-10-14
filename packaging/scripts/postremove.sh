#!/bin/sh
# Reload systemd after the unit disappears and, on a Debian purge, drop the
# service user and the runtime state this package created.
#
# RPM erasures keep the user and the state directories so that a reinstall
# reuses the same IDs and keeps its logs.

case "${1:-}" in
  purge)
    rmdir /etc/redis-conflated-pubsub 2>/dev/null || true

    if getent passwd redis-conflated-pubsub >/dev/null 2>&1; then
      userdel redis-conflated-pubsub >/dev/null 2>&1 || true
    fi

    if getent group redis-conflated-pubsub >/dev/null 2>&1; then
      groupdel redis-conflated-pubsub >/dev/null 2>&1 || true
    fi

    rm -rf /var/lib/redis-conflated-pubsub /var/log/redis-conflated-pubsub
    ;;
esac

if command -v systemctl >/dev/null 2>&1; then
  systemctl daemon-reload >/dev/null 2>&1 || true
fi

exit 0
