#!/usr/bin/env bash
# Local packaging test (free): build, up, asterisk smoke (ASR/TTS off), down.
set -euo pipefail

DEPLOY="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DEPLOY"
ROOT="$(cd "$DEPLOY/.." && pwd)"

PASS=1

echo "======== LOCAL PACKAGING TEST ========"
echo "deploy dir: $DEPLOY"
echo ""

# Packaging test overrides — no paid Sarvam/ElevenLabs calls
PACK_ENV="$DEPLOY/.env.packtest"
cat > "$PACK_ENV" <<'EOF'
SARVAM_API_KEY=packtest-not-used
ELEVENLABS_API_KEY=packtest-not-used
CARRIER=asterisk
ASR_ENABLED=false
TTS_ENABLED=false
BRAIN_WS_ENABLED=true
BRAIN_WS_URL=ws://brain:8000/ws/brain
DENOISE_ENABLED=false
AMD_ENABLED=false
SEMANTIC_TURN_ENABLED=true
SEMANTIC_TURN_ADDR=semantic-turn:9093
TARGET_SAMPLE_RATE=16000
LISTEN_ADDR=:8080
METRICS_ENABLED=true
EOF

cleanup() {
  echo ""
  echo "--- docker compose down ---"
  docker compose --env-file "$PACK_ENV" down -v --remove-orphans 2>/dev/null || true
  rm -f "$PACK_ENV" "$DEPLOY/.env"
}
trap cleanup EXIT

cp "$PACK_ENV" "$DEPLOY/.env"

echo "--- Step 1: build images (minimal asterisk stack) ---"
if ! docker compose -f docker-compose.build.yml build brain semantic-turn go-server smoke; then
  echo "FAIL: build"
  exit 1
fi
docker pull redis:7-alpine

echo ""
echo "--- Step 2: compose up ---"
docker compose --env-file "$PACK_ENV" up -d

echo "--- Step 3: wait health (120s max) ---"
deadline=$((SECONDS + 120))
while (( SECONDS < deadline )); do
  if curl -sf --max-time 2 http://127.0.0.1:8080/healthz 2>/dev/null | grep -qx ok; then
    echo "go-server healthy"
    break
  fi
  sleep 3
done
if ! curl -sf http://127.0.0.1:8080/healthz | grep -qx ok; then
  echo "FAIL: go-server not healthy"
  docker compose --env-file "$PACK_ENV" logs --tail=30 go-server || true
  PASS=0
fi

COMPOSE_ENV="${COMPOSE_ENV_FILE:-.env}"

echo ""
echo "--- Step 4: compose preflight ---"
if ! COMPOSE_ENV_FILE="$PACK_ENV" bash "$DEPLOY/preflight_compose.sh"; then
  PASS=0
fi

echo ""
echo "--- Step 5: Asterisk-protocol smoke (ASR/TTS off) ---"
if docker compose --env-file "$PACK_ENV" --profile test run --rm smoke; then
  echo "smoke: PASS"
else
  echo "smoke: FAIL"
  PASS=0
fi

echo ""
if (( PASS == 1 )); then
  echo "======== OVERALL: PASS ========"
else
  echo "======== OVERALL: FAIL ========"
  exit 1
fi
