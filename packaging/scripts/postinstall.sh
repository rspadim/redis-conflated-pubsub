#!/bin/sh
# Create the system user and group the unit runs as, then reload systemd.
#
# Shared by the Debian, RPM and Arch packages and executed on installs and
# upgrades, so every step is idempotent. It never fails the package transaction:
# a problem is reported on stderr instead of aborting an upgrade mid-way, since
# the service cannot start anyway without the unit or the user.

if ! getent passwd redis-conflated-pubsub >/dev/null 2>&1; then
  # systemd-sysusers first: it needs no shadow-utils, so the account is also
  # created on minimal images that ship systemd but not useradd/groupadd.
  if command -v systemd-sysusers >/dev/null 2>&1; then
    systemd-sysusers /usr/lib/sysusers.d/redis-conflated-pubsub.conf 2>/dev/null ||
      echo "redis-conflated-pubsub: systemd-sysusers could not create the system user" >&2
  fi
fi

if ! getent passwd redis-conflated-pubsub >/dev/null 2>&1; then
  if ! getent group redis-conflated-pubsub >/dev/null 2>&1; then
    groupadd -r redis-conflated-pubsub 2>/dev/null ||
      echo "redis-conflated-pubsub: could not create the system group" >&2
  fi

  shell=/usr/sbin/nologin
  [ -x "$shell" ] || shell=/sbin/nologin
  [ -x "$shell" ] || shell=/bin/false

  useradd -r -g redis-conflated-pubsub -d /var/lib/redis-conflated-pubsub \
    -s "$shell" -M redis-conflated-pubsub 2>/dev/null ||
    echo "redis-conflated-pubsub: could not create the system user" >&2
fi

if command -v systemctl >/dev/null 2>&1; then
  systemctl daemon-reload >/dev/null 2>&1 || true
fi

# Debian passes "configure", RPM passes 1 (install) or 2 (upgrade), Arch passes
# the package version. Only RPM upgrades stay quiet.
if [ "${1:-}" != "2" ]; then
  echo "Redis Conflated Pub/Sub installed. Edit /etc/redis-conflated-pubsub/config.json, then:"
  echo "  sudo systemctl enable --now redis-conflated-pubsub.service"
fi

exit 0
