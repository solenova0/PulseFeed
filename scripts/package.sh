#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
destination="${1:-/tmp/PulseFeed-source.tar.gz}"
# Select only project sources, never hidden workspace metadata or credentials.
tar -czf "$destination" --exclude='*.out' \
  OrderBook.hpp OrderBook.cpp SpscRing.hpp benchmarks.cpp CMakeLists.txt Makefile \
  README.md LICENSE Dockerfile.engine Dockerfile.gateway docker-compose.yml \
  .gitignore .dockerignore .github proto cpp tests gateway/main.go gateway/main_test.go \
  gateway/go.mod gateway/go.sum gateway/gen gateway/cmd observability scripts
echo "$destination"
