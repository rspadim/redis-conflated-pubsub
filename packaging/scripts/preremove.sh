#!/bin/sh
# Stop and disable the service before its files are removed.
#
# Debian passes "remove" (and "upgrade" while upgrading); RPM passes 0 on
# erasure (and 1 while upgrading). Upgrades keep the running service.

case "${1:-}" in
  remove | 0)
    if command -v systemctl >/dev/null 2>&1; then
      systemctl stop redis-conflated-pubsub.service >/dev/null 2>&1 || true
      systemctl disable redis-conflated-pubsub.service >/dev/null 2>&1 || true
    fi
    ;;
esac

exit 0
