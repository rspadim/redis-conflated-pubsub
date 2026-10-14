#!/bin/sh
set -eu
umask 077

# Runs as root in the container and writes into the bind-mounted /auth. The
# host-side test runs as an unprivileged user, so make the generated materials
# group/other readable (0600 -> 0644 files, 0700 -> 0755 dirs). These are
# disposable local test credentials (gitignored), never production secrets.
relax_permissions() {
  chmod -R a+rX /auth 2>/dev/null || true
}

if [ -f /auth/.auth-ready ]; then
  relax_permissions
  exit 0
fi

nsc init --name SPIKE >/dev/null 2>&1
nsc env --operator SPIKE --store /nsc/nats/nsc/stores >/dev/null 2>&1
nsc generate config --mem-resolver --config-file /auth/nats-jwt.conf >/dev/null 2>&1
nats auth nkey gen user --output /auth/nkey.seed >/dev/null 2>&1
nats auth nkey show /auth/nkey.seed > /auth/nkey.public
relax_permissions
touch /auth/.auth-ready
