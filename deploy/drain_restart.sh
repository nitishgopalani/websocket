#!/bin/bash
# W4-2 — SIGTERM drain then recreate. In-flight calls get DRAIN_CAP_S (default 180).
set -euo pipefail
cd "$(dirname "$0")"
CAP="${DRAIN_CAP_S:-180}"
echo "drain_restart: SIGTERM brain + go-server (cap ${CAP}s)"
docker compose kill -s SIGTERM brain go-server || true
# compose stop waits stop_grace_period; kill already sent TERM.
docker compose stop -t "$CAP" brain go-server
docker compose up -d brain go-server
echo "drain_restart: done"
