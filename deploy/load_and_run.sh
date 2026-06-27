#!/usr/bin/env bash
# Server-side: load offline images, verify .env, start stack, preflight. Idempotent.
set -euo pipefail

DEPLOY="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DEPLOY"

TAR="${1:-fonada-voice-images.tar.gz}"

if [[ ! -f "$TAR" ]]; then
  echo "FAIL: missing $TAR — copy bundle from build machine first"
  exit 1
fi

if [[ ! -f .env ]]; then
  echo "FAIL: .env missing — copy .env.example to .env ON THIS SERVER and paste API keys"
  echo "      NEVER copy .env from your laptop."
  exit 1
fi

# Fail loudly if required secrets empty
missing=0
for key in SARVAM_API_KEY ELEVENLABS_API_KEY; do
  val="$(grep -E "^${key}=" .env 2>/dev/null | head -1 | cut -d= -f2- || true)"
  if [[ -z "${val// /}" ]]; then
    echo "FAIL: $key is empty in .env"
    missing=1
  fi
done
if (( missing )); then
  exit 1
fi

echo "=== docker load -i $TAR ==="
docker load -i "$TAR"

echo "=== docker compose up -d ==="
docker compose up -d

echo "=== waiting for health (up to 120s) ==="
deadline=$((SECONDS + 120))
while (( SECONDS < deadline )); do
  if docker compose ps --format json 2>/dev/null | grep -q '"Health":"healthy"'; then
    healthy="$(docker compose ps --format '{{.Name}} {{.Health}}' | grep -c healthy || true)"
    if (( healthy >= 2 )); then
      break
    fi
  fi
  sleep 3
done

echo ""
bash "$DEPLOY/preflight_compose.sh"
