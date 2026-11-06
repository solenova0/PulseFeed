#!/usr/bin/env bash
set -Eeuo pipefail

# All three services share one free web container. Only HTTP listens publicly.
export ENGINE_ADDR=127.0.0.1:50051 ENGINE_METRICS_ADDR=127.0.0.1 REDIS_ADDR=127.0.0.1:6379 INGEST=true
unset REDIS_URL
pids=()
cleanup() {
  trap - EXIT INT TERM
  if (( ${#pids[@]} )); then
    kill -TERM "${pids[@]}" 2>/dev/null || true
    for pid in "${pids[@]}"; do
      wait "$pid" 2>/dev/null || true
    done
  fi
}
trap cleanup EXIT
trap 'exit 0' INT TERM

redis-server --bind 127.0.0.1 --protected-mode yes --port 6379 \
  --save '' --appendonly no --dir /tmp --daemonize no --supervised no \
  --maxmemory 24mb --maxmemory-policy noeviction --maxclients 32 \
  --client-output-buffer-limit pubsub 8mb 2mb 10 --loglevel warning &
pids+=("$!")
pulsefeed_engine &
pids+=("$!")
gateway &
pids+=("$!")

# An unexpected exit stops the whole container so the host can restart it.
if wait -n "${pids[@]}"; then
  echo 'A PulseFeed service exited unexpectedly.' >&2
  exit 1
else
  status=$?
  echo "A PulseFeed service failed (status $status)." >&2
  exit "$status"
fi
