#!/usr/bin/env bash
# Part A: zero-cost audio-path verification (ASR/TTS off, no Sarvam/ElevenLabs).
# Part B: single live callflow (only if Part A clean). See test_live_callflow.sh.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
# shellcheck disable=SC1091
source "$ROOT/scripts/load_env.sh"

LOG="$ROOT/scripts/verify_clean_path.log"
SERVER_PID=""
PADDED_TMP=""

cleanup() {
  [[ -n "$SERVER_PID" ]] && kill "$SERVER_PID" 2>/dev/null || true
  wait "$SERVER_PID" 2>/dev/null || true
  [[ -n "${PADDED_TMP:-}" && -f "${PADDED_TMP:-}" ]] && rm -f "$PADDED_TMP"
}
trap cleanup EXIT

exec > >(tee -a "$LOG") 2>&1

echo "======== VERIFY CLEAN AUDIO PATH — $(date -Iseconds) ========"

load_env_stack "$ROOT"
apply_conversation_test_env false false

echo "=== Part A env (masked) ==="
print_live_config
echo ""

port_open() {
  ss -ltn 2>/dev/null | grep -q ":$1 " || (echo >/dev/tcp/127.0.0.1/"$1") 2>/dev/null
}

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

FIXTURE="${VERIFY_FIXTURE:-$ROOT/testdata/calls/human_long.ulaw}"
SPOKEN="$(pad_ulaw_min_secs "$FIXTURE" 10)"
[[ "$SPOKEN" != "$FIXTURE" ]] && PADDED_TMP="$SPOKEN"

echo "--- Part A.1: brain stub ---"
bash "$ROOT/scripts/start_brain_stub.sh"

echo "--- Part A.2: workers ---"
bash "$ROOT/scripts/stop_workers.sh" 2>/dev/null || true
bash "$ROOT/scripts/run_workers.sh"

echo "--- Part A.3: Go server (ASR/TTS off) ---"
PART_A_LOG="$ROOT/scripts/verify_part_a_server.log"
: >"$PART_A_LOG"
RUN_MARK="======== PART A $(date -Iseconds) ========"
echo "$RUN_MARK" >>"$PART_A_LOG"
go run ./cmd/server >>"$PART_A_LOG" 2>&1 &
SERVER_PID=$!
deadline=$((SECONDS + 90))
while (( SECONDS < deadline )); do
  if grep -q '"msg":"audio pipeline ready"' "$PART_A_LOG" 2>/dev/null; then
    break
  fi
  sleep 1
done
port_open 8080 || { echo "FAIL: server not on :8080"; exit 1; }

echo "--- Part A.4: assert boot flags ---"
if ! assert_boot_flags "$PART_A_LOG" false false false false; then
  echo ""
  echo "PART A: STOP — env overlay wrong (boot flags). Do NOT call Sarvam."
  exit 1
fi

echo "--- Part A.5: preflight_local ---"
if ! bash "$ROOT/scripts/preflight_local.sh"; then
  echo "PART A: STOP — preflight not green."
  exit 1
fi

echo "--- Part A.6: replay (ASR/TTS off, zero quota) ---"
BEFORE_LINES=$(wc -l <"$PART_A_LOG")
go run ./cmd/replay \
  -addr ws://127.0.0.1:8080/stream \
  -in "$SPOKEN" \
  -pace realtime \
  -stream-sid MZ-VERIFY-A \
  -call-sid CA-VERIFY-A \
  -timeout 120s || true
sleep 8

SESSION_LOG="$(tail -n +"$((BEFORE_LINES + 1))" "$PART_A_LOG")"

DENOISE_FB="$(echo "$SESSION_LOG" | grep -c 'denoise fail-open' || true)"
DROPS="$(echo "$SESSION_LOG" | grep 'dropping oldest audio frame' | tail -1 | grep -oE 'dropped_total":[0-9]+' | grep -oE '[0-9]+' || echo 0)"
ASR_COMPLETE="$(echo "$SESSION_LOG" | grep -c '"msg":"asr session complete"' || true)"
DENOISE_COMPLETE="$(echo "$SESSION_LOG" | grep '"msg":"denoise session complete"' | tail -1 || true)"
ASR_COMPLETE_LINE="$(echo "$SESSION_LOG" | grep '"msg":"asr session complete"' | tail -1 || true)"

METRICS="$(curl -sf http://127.0.0.1:8080/metrics || true)"
MET_DENOISE_FB="$(echo "$METRICS" | awk '/^media_denoise_fallbacks_total /{print $2; exit}')"

echo ""
echo "======== PART A RESULTS ========"
echo "denoise fail-open log lines: $DENOISE_FB"
echo "media_denoise_fallbacks_total: ${MET_DENOISE_FB:-0}"
echo "backpressure dropped_total (max): ${DROPS:-0}"
echo "asr session complete events: $ASR_COMPLETE"
echo "denoise session complete: ${DENOISE_COMPLETE:-(none)}"
echo "asr session complete: ${ASR_COMPLETE_LINE:-(none)}"

PART_A_OK=1
if (( DENOISE_FB > 0 )); then PART_A_OK=0; fi
if [[ "${MET_DENOISE_FB:-0}" != "0" && -n "${MET_DENOISE_FB:-}" ]]; then PART_A_OK=0; fi
if [[ "${DROPS:-0}" != "0" && "${DROPS:-0}" -gt 5 ]]; then PART_A_OK=0; fi
if (( ASR_COMPLETE < 1 )); then PART_A_OK=0; fi

if (( PART_A_OK == 0 )); then
  echo ""
  echo "PART A: FAIL — audio path not clean. Do NOT call Sarvam."
  exit 1
fi

echo ""
echo "PART A: PASS — clean audio path verified (zero quota spent)."

# Stop Part A server before Part B
kill "$SERVER_PID" 2>/dev/null || true
wait "$SERVER_PID" 2>/dev/null || true
SERVER_PID=""

if [[ "${RUN_PART_B:-true}" != "true" ]]; then
  echo "Part B skipped (RUN_PART_B=false)."
  exit 0
fi

echo ""
echo "======== PART B: live callflow (ONE Sarvam run) ========"
exec bash "$ROOT/scripts/test_live_callflow.sh"
