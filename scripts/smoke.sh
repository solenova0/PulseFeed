#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose up --build -d
wait_http() {
  local address="$1"
  for attempt in $(seq 1 60); do
    if curl --fail --silent --max-time 2 "$address" >/dev/null; then
      return 0
    fi
    sleep 1
  done
  docker compose logs --tail=50
  echo "Readiness timed out: $address" >&2
  return 1
}
wait_http "http://127.0.0.1:${GATEWAY_PORT:-8080}/readyz"
wait_http "http://127.0.0.1:${PROMETHEUS_PORT:-9090}/-/ready"
wait_http "http://127.0.0.1:${GRAFANA_PORT:-3000}/api/health"
docker compose exec -T gateway subscriber -url ws://127.0.0.1:8080/ws \
  -clients 20 -duration 3s -quiet
echo 'Stack smoke check passed. Services remain running; stop with docker compose down.'
