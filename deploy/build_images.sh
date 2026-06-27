#!/usr/bin/env bash
# Build all images (dev machine with repo + internet). Run from deploy/.
set -euo pipefail

DEPLOY="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DEPLOY"

if [[ ! -d ../../Collection ]]; then
  echo "FAIL: Collection repo not found at ../../Collection (expected Main/Collection sibling of Websocket)"
  exit 1
fi

echo "=== docker compose build (models baked into images) ==="
docker compose -f docker-compose.build.yml build

echo "=== tagging redis ==="
docker pull redis:7-alpine

echo ""
echo "Build complete. Images:"
docker images --format '  {{.Repository}}:{{.Tag}}' | grep -E 'fonada/voice-|redis:7-alpine' || true
