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
apply_conversation_test_env

print_key_status
require_keys || exit 1
print_live_config
echo ""

FIXTURE="${LIVE_CALLFLOW_FIXTURE:-$ROOT/testdata/calls/human_long.ulaw}"
REF_TEXT="${FIXTURE%.ulaw}.ref.txt"
if [[ -f "$FIXTURE" ]]; then
  SPOKEN="$FIXTURE"
else
  SPOKEN="$(bash "$ROOT/scripts/ensure_spoken_fixture.sh")"
fi
echo "Speech fixture: $SPOKEN"
if [[ -f "$REF_TEXT" ]]; then
  echo "Reference text: $(head -1 "$REF_TEXT" | cut -c1-80)..."
fi

pad_ulaw_min_secs() {
  local src="$1" min_secs="$2"
  local min_bytes=$((min_secs * 8000))
  local sz
  sz=$(wc -c <"$src")
  if (( sz >= min_bytes )); then
    echo "$src"
    return
  fi
  local tmp
  tmp=$(mktemp --suffix=.ulaw)
  cp "$src" "$tmp"
  local pad=$((min_bytes - sz))
  dd if=/dev/zero bs=1 count="$pad" 2>/dev/null | tr '\000' '\377' >>"$tmp"
  echo "$tmp"
}
SPOKEN_PADDED="$(pad_ulaw_min_secs "$SPOKEN" 10)"
if [[ "$SPOKEN_PADDED" != "$SPOKEN" ]]; then
  echo "Padded fixture for ASR window: $SPOKEN_PADDED"
  SPOKEN="$SPOKEN_PADDED"
  PADDED_TMP="$SPOKEN"
fi

port_open() {
  ss -ltn 2>/dev/null | grep -q ":$1 " || (echo >/dev/tcp/127.0.0.1/"$1") 2>/dev/null
}

SERVER_PID=""
PADDED_TMP=""
cleanup() {
  [[ -n "$SERVER_PID" ]] && kill "$SERVER_PID" 2>/dev/null || true
  wait "$SERVER_PID" 2>/dev/null || true
  [[ -n "${PADDED_TMP:-}" && -f "${PADDED_TMP:-}" ]] && rm -f "$PADDED_TMP"
}
trap cleanup EXIT

echo "--- Step 1: workers ---"
bash "$ROOT/scripts/stop_workers.sh" 2>/dev/null || true
bash "$ROOT/scripts/run_workers.sh"

echo "--- Step 2: brain stub ---"
bash "$ROOT/scripts/start_brain_stub.sh"

echo "--- Step 3: Go server (live env) ---"
load_env_stack "$ROOT"
apply_conversation_test_env
RUN_MARK="======== CALLFLOW RUN $(date -Iseconds) ========"
: >>"$ROOT/scripts/pipeline_server.log"
echo "$RUN_MARK" >>"$ROOT/scripts/pipeline_server.log"
BEFORE_LOG=$(wc -l <"$ROOT/scripts/pipeline_server.log")
go run ./cmd/server >>"$ROOT/scripts/pipeline_server.log" 2>&1 &
SERVER_PID=$!
deadline=$((SECONDS + 90))
while (( SECONDS < deadline )); do
  port_open 8080 && break
  sleep 2
done
port_open 8080 || { echo "FAIL: server not on :8080"; tail -15 "$ROOT/scripts/pipeline_server.log"; exit 1; }

echo "--- Step 4: local preflight (no Sarvam call) ---"
if ! bash "$ROOT/scripts/preflight_local.sh"; then
  echo "STOP: preflight not green — fix startup before callflow (protects Sarvam quota)"
  exit 1
fi
echo ""

WORKERS_BEFORE=$(wc -l <"$ROOT/scripts/workers.log" 2>/dev/null || echo 0)

echo "--- Step 5: replay (realtime, ONE live loop) ---"
go run ./cmd/replay \
  -addr ws://127.0.0.1:8080/stream \
  -in "$SPOKEN" \
  -pace realtime \
  -stream-sid MZ-LIVE-TEST \
  -call-sid CA-LIVE-TEST \
  -timeout 180s || true

echo "Waiting for pipeline to settle..."
sleep 45

PIPE_LOG="$(tail -n +"$BEFORE_LOG" "$ROOT/scripts/pipeline_server.log")"

echo ""
echo "======== RESULTS ========"

# Sarvam rate limit check (avoid matching unrelated log fields like target_sample_rate)
if echo "$PIPE_LOG" | grep -qiE 'Rate limit exceeded|HTTP 429|"status":429'; then
  echo "Sarvam: STILL COOLING DOWN (rate limited) — STOP, no retry"
  exit 2
fi

echo "--- Sarvam ASR (pipeline_server.log) ---"
echo "$PIPE_LOG" | grep -E '"msg":"asr (partial|final|speech)' | tail -12 || echo "(none)"
SARVAM_FINAL="$(echo "$PIPE_LOG" | grep '"msg":"asr final"' | tail -1 || true)"
SARVAM_PARTIAL="$(echo "$PIPE_LOG" | grep '"msg":"asr partial"' | tail -3 || true)"

echo "--- Turn-taking ---"
echo "$PIPE_LOG" | grep '"msg":"turn event"' | grep 'end_of_turn' | tail -5 || echo "(no end_of_turn logged)"

echo "--- Brain / reply (pipeline_server.log) ---"
echo "$PIPE_LOG" | grep -E '"msg":"(reply chunk|reply done|reply error)"' | tail -10 || echo "(none)"

echo "--- ElevenLabs egress ---"
EGRESS_LINES=$(echo "$PIPE_LOG" | grep -c '"msg":"egress audio"' || true)
echo "egress audio log lines: $EGRESS_LINES"
echo "$PIPE_LOG" | grep '"msg":"egress audio"' | tail -3 || true

echo "--- Fallbacks ---"
echo "$PIPE_LOG" | grep -iE 'fallback|fail-open|asr event error|tts.*failed|brain.*fail' | tail -8 || echo "(none)"

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

# PASS/FAIL heuristics
PASS=1
if [[ -z "$SARVAM_FINAL" ]]; then
  echo "CHECK Sarvam final: MISSING"
  PASS=0
else
  echo "CHECK Sarvam final: $SARVAM_FINAL"
fi
if ! echo "$PIPE_LOG" | grep -q '"msg":"reply chunk"'; then
  echo "CHECK Brain reply: MISSING"
  PASS=0
else
  echo "CHECK Brain reply: present"
fi
if [[ -z "${M2E_COUNT:-}" || "$M2E_COUNT" == "0" ]]; then
  echo "CHECK mouth_to_ear_ms: MISSING"
  PASS=0
else
  echo "CHECK mouth_to_ear_ms: present"
fi

echo ""
if (( PASS == 1 )); then
  echo "OVERALL: PASS"
else
  echo "OVERALL: FAIL"
fi

echo ""
echo "Full log: $LOG"
echo "See docs/FONADA_STAGING.md for staging phone call runbook."
