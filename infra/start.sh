#!/bin/sh
set -eu
case "$MODE" in
  redis)
    file=/data/redis.conf
    if [ ! -f "$file" ]; then
      cp /config/redis.conf "$file"
      printf '\nport %s\nreplica-announce-ip %s\nreplica-announce-port %s\n' "$PORT" "$NODE" "$PORT" >> "$file"
      if [ -n "${REPLICA_OF:-}" ]; then
        printf 'replicaof %s 6379\n' "$REPLICA_OF" >> "$file"
      fi
    fi
    ;;
  sentinel)
    file=/data/sentinel.conf
    if [ ! -f "$file" ]; then
      cp /config/sentinel.conf "$file"
      printf '\nport %s\nsentinel announce-ip %s\nsentinel announce-port %s\n' "$PORT" "$NODE" "$PORT" >> "$file"
    fi
    ;;
  *) echo "Unknown MODE: $MODE" >&2; exit 1 ;;
esac
chown -R redis:redis /data
if [ "$MODE" = sentinel ]; then
  exec /usr/local/bin/docker-entrypoint.sh redis-server "$file" --sentinel
fi
exec /usr/local/bin/docker-entrypoint.sh redis-server "$file"