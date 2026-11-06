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
# Ask Compose for the actual bindings so .env and exported port overrides agree.
gateway_address="$(docker compose port gateway 8080)"
prometheus_address="$(docker compose port prometheus 9090)"
grafana_address="$(docker compose port grafana 3000)"
wait_http "http://${gateway_address}/readyz"
wait_http "http://${prometheus_address}/-/ready"
wait_http "http://${grafana_address}/api/health"
docker compose exec -T gateway subscriber -url ws://127.0.0.1:8080/ws \
  -clients 20 -duration 3s -quiet
echo 'Stack smoke check passed. Services remain running; stop with docker compose down.'
