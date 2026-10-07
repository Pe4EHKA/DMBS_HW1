#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
for endpoint in sentinel-1:26379 sentinel-2:26380 sentinel-3:26381; do
  node="${endpoint%:*}"
  port="${endpoint#*:}"
  address=$(docker compose exec -T "$node" redis-cli --raw -p "$port" \
    SENTINEL get-master-addr-by-name mymaster 2>/dev/null | tr -d '\r') || continue
  host=$(printf '%s\n' "$address" | sed -n '1p')
  master_port=$(printf '%s\n' "$address" | sed -n '2p')
  if [[ -n "$host" && "$master_port" =~ ^[0-9]+$ ]]; then
    if [[ "${1:-}" == --address ]]; then
      printf '%s %s\n' "$host" "$master_port"
      exit 0
    fi
    exec docker compose exec -T "$node" redis-cli -e --raw -h "$host" -p "$master_port" "$@"
  fi
done
echo "No Sentinel returned a master address" >&2
exit 1
