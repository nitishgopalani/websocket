#!/usr/bin/env bash
# Full live stack test: real Sarvam + ElevenLabs + brain, simulated carrier (no Fonada).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
# shellcheck disable=SC1091
source "$ROOT/scripts/load_env.sh"

LOG="$ROOT/scripts/live_callflow.log"
exec > >(tee -a "$LOG") 2>&1

echo "======== LIVE CALL-FLOW TEST — $(date -Iseconds) ========"

load_env_stack "$ROOT"
if [[ ! -f "$ROOT/.env.live" && -f "$ROOT/.env.live.example" ]]; then
  load_env_local "$ROOT/.env.live.example"
fi
export WHISPER_MODEL="${WHISPER_MODEL:-tiny}"

print_key_status
require_keys || exit 1
print_live_config
echo ""

echo "--- Step 1: preflight ---"
if ! bash "$ROOT/scripts/preflight_live.sh"; then
  echo "STOP: preflight failed — fix keys/services before call-flow test"
  exit 1
fi
echo ""

SPOKEN="$(bash "$ROOT/scripts/ensure_spoken_fixture.sh")"
echo "Speech fixture: $SPOKEN"

port_open() {
  ss -ltn 2>/dev/null | grep -q ":$1 " || (echo >/dev/tcp/127.0.0.1/"$1") 2>/dev/null
}

wait_ports() {
  local deadline=$((SECONDS + 180))
  while (( SECONDS < deadline )); do
    port_open 9091 && port_open 9092 && port_open 9093 && return 0
    sleep 3
  done
  return 1
}

SERVER_PID=""
cleanup() {
  [[ -n "$SERVER_PID" ]] && kill "$SERVER_PID" 2>/dev/null || true
  wait "$SERVER_PID" 2>/dev/null || true
}
trap cleanup EXIT

echo "--- Step 2: workers ---"
bash "$ROOT/scripts/stop_workers.sh" 2>/dev/null || true
bash "$ROOT/scripts/run_workers.sh"
wait_ports || echo "WARN: worker ports not all open"

echo "--- Step 3: brain (Collection) ---"
BRAIN_URL="${BRAIN_WS_URL:-ws://127.0.0.1:8000/ws/brain}"
BRAIN_HTTP="${BRAIN_URL/ws:\/\//http:\/\/}"
BRAIN_HTTP="${BRAIN_HTTP/\/ws\/brain/\/healthz}"
if curl -sf --max-time 3 "$BRAIN_HTTP" >/dev/null 2>&1; then
  echo "Brain health: OK ($BRAIN_HTTP)"
else
  echo "WARN: brain not reachable at $BRAIN_HTTP"
  echo "Start Collection brain (separate terminal):"
  echo "  cd $(dirname "$ROOT")/Collection"
  echo "  source .venv/bin/activate   # or: python3 -m venv .venv && pip install -e '.[dev]'"
  echo "  STUB_MODE=true uvicorn app.main:app --host 0.0.0.0 --port 8000"
  echo "Re-run preflight after brain is up."
fi

echo "--- Step 4: Go server (live env) ---"
: >>"$ROOT/scripts/pipeline_server.log"
BEFORE_LOG=$(wc -l <"$ROOT/scripts/pipeline_server.log")
go run ./cmd/server >>"$ROOT/scripts/pipeline_server.log" 2>&1 &
SERVER_PID=$!
deadline=$((SECONDS + 90))
while (( SECONDS < deadline )); do
  port_open 8080 && break
  sleep 2
done
port_open 8080 || { echo "FAIL: server not on :8080"; tail -15 "$ROOT/scripts/pipeline_server.log"; exit 1; }

WORKERS_BEFORE=$(wc -l <"$ROOT/scripts/workers.log" 2>/dev/null || echo 0)

echo "--- Step 5: replay (realtime, full live loop) ---"
go run ./cmd/replay \
  -addr ws://127.0.0.1:8080/stream \
  -in "$SPOKEN" \
  -pace realtime \
  -stream-sid MZ-LIVE-TEST \
  -call-sid CA-LIVE-TEST \
  -timeout 180s || true

echo "Waiting for pipeline to settle..."
sleep 15

echo ""
echo "======== RESULTS ========"
echo "--- AMD (workers.log) ---"
tail -n +"$((WORKERS_BEFORE + 1))" "$ROOT/scripts/workers.log" 2>/dev/null | grep 'amd classify' | tail -3 || echo "(none)"

echo "--- Sarvam ASR (pipeline_server.log) ---"
tail -n +"$BEFORE_LOG" "$ROOT/scripts/pipeline_server.log" | grep -E '"msg":"asr (partial|final|speech)' | tail -8 || echo "(none)"

echo "--- Brain / reply (pipeline_server.log) ---"
tail -n +"$BEFORE_LOG" "$ROOT/scripts/pipeline_server.log" | grep -E '"msg":"(reply chunk|brain chunk|egress audio)"' | tail -8 || echo "(none — check brain WS + opener-on-AMD-human)"

echo "--- ElevenLabs egress ---"
EGRESS_BYTES=$(tail -n +"$BEFORE_LOG" "$ROOT/scripts/pipeline_server.log" | grep '"msg":"egress audio"' | wc -l || true)
echo "egress audio log lines: $EGRESS_BYTES"

echo "--- Fallbacks ---"
tail -n +"$BEFORE_LOG" "$ROOT/scripts/pipeline_server.log" | grep -iE 'fallback|fail-open|asr event error|tts.*failed' | tail -5 || echo "(none)"

echo "--- /metrics ---"
METRICS="$(curl -sf http://127.0.0.1:8080/metrics || true)"
echo "$METRICS" | grep -E '^(media_mouth_to_ear_ms_count|media_mouth_to_ear_ms_sum|media_turns_total|media_denoise_fallbacks_total|media_amd_human_total|media_asr_reconnects_total|media_tts_fallbacks_total)' || true

M2E_COUNT=$(echo "$METRICS" | awk '/^media_mouth_to_ear_ms_count /{print $2; exit}')
M2E_SUM=$(echo "$METRICS" | awk '/^media_mouth_to_ear_ms_sum /{print $2; exit}')
if [[ -n "${M2E_COUNT:-}" && "$M2E_COUNT" != "0" ]]; then
  echo "mouth_to_ear_ms: count=$M2E_COUNT sum=$M2E_SUM avg=$((M2E_SUM / M2E_COUNT))ms"
else
  echo "mouth_to_ear_ms: (no samples — turn may not have completed)"
fi

echo ""
echo "Full log: $LOG"
echo "See docs/FONADA_STAGING.md for staging phone call runbook."
