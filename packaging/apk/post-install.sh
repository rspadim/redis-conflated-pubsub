#!/bin/sh
# Alpine apk post-install: create the service user and the runtime directories.
#
# OpenRC has no equivalent of systemd's StateDirectory/LogsDirectory, so the
# directories the shipped configuration points at are created here. The
# lifecycle of the service itself belongs to OpenRC.

if ! grep -q '^redis-conflated-pubsub:' /etc/passwd 2>/dev/null; then
  addgroup -S redis-conflated-pubsub 2>/dev/null ||
    echo "redis-conflated-pubsub: could not create the system group" >&2
  adduser -S -D -H -h /var/lib/redis-conflated-pubsub -s /sbin/nologin \
    -G redis-conflated-pubsub redis-conflated-pubsub 2>/dev/null ||
    echo "redis-conflated-pubsub: could not create the system user" >&2
fi

for dir in /var/lib/redis-conflated-pubsub /var/log/redis-conflated-pubsub; do
  mkdir -p "$dir"
  chown -R redis-conflated-pubsub:redis-conflated-pubsub "$dir" 2>/dev/null || true
  chmod 0750 "$dir"
done

echo "Redis Conflated Pub/Sub installed. Edit /etc/redis-conflated-pubsub/config.json, then:"
echo "  rc-update add redis-conflated-pubsub default"
echo "  rc-service redis-conflated-pubsub start"

exit 0
