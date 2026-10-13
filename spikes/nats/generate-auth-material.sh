#!/bin/sh
set -eu
umask 077

if [ -f /auth/.auth-ready ]; then
  exit 0
fi

nsc init --name SPIKE >/dev/null 2>&1
nsc env --operator SPIKE --store /nsc/nats/nsc/stores >/dev/null 2>&1
nsc generate config --mem-resolver --config-file /auth/nats-jwt.conf >/dev/null 2>&1
nats auth nkey gen user --output /auth/nkey.seed >/dev/null 2>&1
nats auth nkey show /auth/nkey.seed > /auth/nkey.public
touch /auth/.auth-ready
