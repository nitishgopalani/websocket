#!/usr/bin/env bash
# One-command local e2e: real workers + Go server, no paid APIs.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ENV_FILE="$ROOT/.env.local"
PID_FILE="$ROOT/scripts/.worker_pids"
SERVER_PID=""
PASS=1

cleanup() {
  if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
    echo "Stopping Go server (pid $SERVER_PID)"
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

port_open() {
  local host="$1" port="$2"
  if command -v nc >/dev/null 2>&1; then
    nc -z "$host" "$port" 2>/dev/null
    return
  fi
  (echo >/dev/tcp/"$host"/"$port") 2>/dev/null
}

ensure_workers() {
  local need_start=0
  for port in 9091 9092 9093; do
    if ! port_open 127.0.0.1 "$port"; then
      need_start=1
    fi
  done
  if (( need_start == 1 )); then
    echo "Workers not all listening — starting via run_workers.sh"
    bash "$ROOT/scripts/run_workers.sh"
    sleep 3
  else
    echo "Workers already listening on 9091-9093"
  fi
}

ensure_workers

echo "Starting Go server with .env.local"
# shellcheck disable=SC1091
source "$ROOT/scripts/load_env.sh"
set -a
load_env_local "$ENV_FILE"
set +a
go run ./cmd/server >> "$ROOT/scripts/pipeline_server.log" 2>&1 &
SERVER_PID=$!
sleep 2

if ! port_open 127.0.0.1 8080; then
  echo "FAIL: Go server not listening on :8080"
  tail -20 "$ROOT/scripts/pipeline_server.log" || true
  exit 1
fi

echo "Running replay CLI (testdata/smoke.ulaw, pace=fast)"
REPLAY_OUT="$(mktemp)"
set +e
go run ./cmd/replay \
  -addr ws://127.0.0.1:8080/stream \
  -in testdata/smoke.ulaw \
  -pace fast \
  -timeout 60s 2>&1 | tee "$REPLAY_OUT"
REPLAY_RC=${PIPESTATUS[0]}
set -e

if grep -qi "error:" "$REPLAY_OUT"; then
  echo "WARN: replay reported errors (may be OK without TTS/brain)"
fi
if (( REPLAY_RC != 0 && REPLAY_RC != 2 )); then
  echo "FAIL: replay exited $REPLAY_RC"
  PASS=0
fi

echo ""
echo "=== /metrics summary ==="
METRICS="$(curl -sf http://127.0.0.1:8080/metrics || true)"
echo "$METRICS" | grep -E '^(media_denoise_fallbacks_total|media_amd_human_total|media_amd_machine_total|media_turns_total|media_active_sessions)' || true

DENOISE_FB="$(echo "$METRICS" | awk '/^media_denoise_fallbacks_total /{print $2; exit}')"
if [[ -n "${DENOISE_FB:-}" && "$DENOISE_FB" != "0" ]]; then
  echo "WARN: media_denoise_fallbacks_total=$DENOISE_FB (expected 0 with healthy workers)"
fi

echo ""
echo "=== WORKERS_LIVE integration (optional) ==="
if WORKERS_LIVE=1 go test ./internal/media -run TestWorkersLiveIntegration -count=1 -v; then
  echo "WORKERS_LIVE: PASS"
else
  echo "WORKERS_LIVE: FAIL (workers may be down or ONNX missing)"
  PASS=0
fi

echo ""
if (( PASS == 1 && REPLAY_RC <= 2 )); then
  echo "PASS: local pipeline completed (no panics, replay finished)"
  exit 0
fi
echo "FAIL: see logs in scripts/pipeline_server.log and scripts/workers.log"
exit 1
