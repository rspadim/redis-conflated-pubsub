#!/bin/sh
# Alpine apk pre-deinstall: stop the service before its binary is removed.
#
# The user and the runtime directories are kept, so a reinstall reuses them.

if command -v rc-service >/dev/null 2>&1; then
  rc-service redis-conflated-pubsub stop >/dev/null 2>&1 || true
fi

if command -v rc-update >/dev/null 2>&1; then
  rc-update del redis-conflated-pubsub default >/dev/null 2>&1 || true
fi

exit 0
