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
apply_conversation_test_env true true

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
apply_conversation_test_env true true
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

echo "--- Step 3b: assert boot flags ---"
if ! assert_boot_flags "$ROOT/scripts/pipeline_server.log" false false true true true; then
  echo "STOP: boot flags wrong — fix env overlay before callflow"
  exit 1
fi

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

PIPE_START=$(grep -nF "$RUN_MARK" "$ROOT/scripts/pipeline_server.log" | tail -1 | cut -d: -f1)
if [[ -n "${PIPE_START:-}" ]]; then
  PIPE_LOG="$(sed -n "${PIPE_START},\$p" "$ROOT/scripts/pipeline_server.log")"
else
  PIPE_LOG="$(tail -n +"$BEFORE_LOG" "$ROOT/scripts/pipeline_server.log")"
fi
SESSION_CLOSED_LINE="$(echo "$PIPE_LOG" | grep '"msg":"session closed"' | tail -1 || true)"

echo ""
echo "======== FULL CHAIN RESULTS ========"

# Sarvam rate limit check (avoid matching unrelated log fields like target_sample_rate)
if echo "$PIPE_LOG" | grep -qiE 'Rate limit exceeded|HTTP 429|"status":429'; then
  echo "Sarvam: STILL COOLING DOWN (rate limited) — STOP, no retry"
  exit 2
fi

REF_LINE=""
if [[ -f "$REF_TEXT" ]]; then
  REF_LINE="$(tr -d '\r\n' <"$REF_TEXT")"
fi

echo ""
echo "=== 1. Sarvam ASR ==="
echo "$PIPE_LOG" | grep -E '"msg":"asr (partial|final|speech)' | tail -15 || echo "(no structured asr partial/final — TurnManager consumes ASR directly)"
SARVAM_WS_LINES="$(echo "$PIPE_LOG" | grep 'sarvam ws recv' | grep '"transcript":' || true)"
if [[ -n "$SARVAM_WS_LINES" ]]; then
  echo "--- sarvam ws recv (transcript segments) ---"
  echo "$SARVAM_WS_LINES" | sed 's/.*"transcript":"\([^"]*\)".*/  segment: \1/' | tail -10
fi
SARVAM_FINAL_LINE="$(echo "$PIPE_LOG" | grep '"msg":"asr final"' | tail -1 || true)"
SARVAM_FINAL_TEXT="$(echo "$SARVAM_FINAL_LINE" | grep -oE '"text":"[^"]*"' | tail -1 | cut -d'"' -f4 || true)"
if [[ -z "$SARVAM_FINAL_TEXT" && -n "$SARVAM_WS_LINES" ]]; then
  SARVAM_FINAL_TEXT="$(echo "$SARVAM_WS_LINES" | sed -n 's/.*"transcript":"\([^"]*\)".*/\1/p' | tail -1)"
  SARVAM_MERGED="$(echo "$SARVAM_WS_LINES" | sed -n 's/.*"transcript":"\([^"]*\)".*/\1/p' | paste -sd' ' -)"
  echo "Merged WS segments: ${SARVAM_MERGED:0:120}..."
fi
if [[ -z "$SARVAM_FINAL_TEXT" && -n "$SARVAM_FINAL_LINE" ]]; then
  SARVAM_FINAL_TEXT="$(echo "$SARVAM_FINAL_LINE" | sed -n 's/.*"text":\([^,}]*\).*/\1/p' | tr -d '"')"
fi
echo "FINAL transcript: ${SARVAM_FINAL_TEXT:-(missing)}"
if [[ -n "$REF_LINE" && -n "$SARVAM_FINAL_TEXT" ]]; then
  if echo "$SARVAM_FINAL_TEXT" | grep -qi "payment"; then
    echo "Reference match: partial/full (contains 'payment')"
  else
    echo "Reference match: NO (expected snippet with 'payment' from ref)"
  fi
fi
LINK1=pass
[[ -n "$SARVAM_FINAL_TEXT" || -n "$SARVAM_WS_LINES" ]] || LINK1=fail

echo ""
echo "=== 2. Turn-taking (EndOfTurn) ==="
EOT_LINES="$(echo "$PIPE_LOG" | grep '"msg":"turn event"' | grep 'end_of_turn' || true)"
if [[ -n "$EOT_LINES" ]]; then
  echo "$EOT_LINES" | tail -5
  EOT_COUNT=$(echo "$EOT_LINES" | wc -l | tr -d ' ')
  echo "EndOfTurn count: $EOT_COUNT"
else
  echo "(no end_of_turn logged)"
fi
LINK2=pass
echo "$EOT_LINES" | grep -q end_of_turn || LINK2=fail

echo ""
echo "=== 3. Brain (EB-6) reply chunks ==="
REPLY_LINES="$(echo "$PIPE_LOG" | grep -E '"msg":"(reply chunk|reply done|reply error)"' || true)"
if [[ -n "$REPLY_LINES" ]]; then
  echo "$REPLY_LINES" | tail -12
  REPLY_CHUNKS=$(echo "$REPLY_LINES" | grep -c '"msg":"reply chunk"' || true)
  echo "reply chunk count: $REPLY_CHUNKS"
else
  echo "(none — check brain WS + EndOfTurn)"
fi
LINK3=pass
echo "$REPLY_LINES" | grep -q '"msg":"reply chunk"' || LINK3=fail

echo ""
echo "=== 4. TTS (ElevenLabs conversational, not just opener) ==="
EGRESS_LINES=$(echo "$PIPE_LOG" | grep -c '"msg":"egress audio"' || true)
echo "egress audio log lines: $EGRESS_LINES"
echo "$PIPE_LOG" | grep '"msg":"egress audio"' | tail -5 || true
# Opener-only runs often have 1 short burst; conversational reply adds more egress after EOT.
LINK4=pass
if (( EGRESS_LINES < 2 )); then
  echo "NOTE: only $EGRESS_LINES egress line(s) — may be opener-only"
  LINK4=fail
fi

echo ""
echo "=== 5. Pipeline fallbacks / Sarvam WS close / lifecycle ==="
echo "$PIPE_LOG" | grep -iE 'fallback|fail-open|asr event error|tts.*failed|brain.*fail|sarvam ws closed|sarvam read ended|brain turn send failed|brain ws read ended' | tail -12 || echo "(none)"
if [[ -n "$SESSION_CLOSED_LINE" ]]; then
  echo "session closed: $SESSION_CLOSED_LINE"
  CLOSED_TS="$(echo "$SESSION_CLOSED_LINE" | sed -n 's/.*"time":"\([^"]*\)".*/\1/p')"
  if [[ -n "$CLOSED_TS" ]]; then
    LATE_BRAIN="$(echo "$PIPE_LOG" | grep 'brain turn send failed' || true)"
    if [[ -n "$LATE_BRAIN" ]]; then
      echo "WARN: brain turn send failed logged (check timing vs session close)"
      echo "$LATE_BRAIN"
    else
      echo "OK: no brain turn send failed after session teardown"
    fi
  fi
fi

echo ""
echo "=== 6. /metrics ==="
METRICS="$(curl -sf http://127.0.0.1:8080/metrics || true)"
echo "$METRICS" | grep -E '^(media_mouth_to_ear_ms_count|media_mouth_to_ear_ms_sum|media_turns_total|media_denoise_fallbacks_total|media_amd_human_total|media_asr_reconnects_total|media_tts_fallbacks_total)' || true

M2E_COUNT=$(echo "$METRICS" | awk '/^media_mouth_to_ear_ms_count /{print $2; exit}')
M2E_SUM=$(echo "$METRICS" | awk '/^media_mouth_to_ear_ms_sum /{print $2; exit}')
TURNS_TOTAL=$(echo "$METRICS" | awk '/^media_turns_total /{print $2; exit}')
if [[ -n "${M2E_COUNT:-}" && "$M2E_COUNT" != "0" ]]; then
  M2E_AVG=$((M2E_SUM / M2E_COUNT))
  echo "HEADLINE mouth_to_ear_ms: count=$M2E_COUNT sum=$M2E_SUM avg=${M2E_AVG}ms"
else
  echo "HEADLINE mouth_to_ear_ms: (no samples)"
  M2E_AVG=""
fi
echo "HEADLINE media_turns_total: ${TURNS_TOTAL:-0}"

LINK5=pass
[[ -n "${M2E_COUNT:-}" && "$M2E_COUNT" != "0" ]] || LINK5=fail
LINK6=pass
[[ -n "${TURNS_TOTAL:-}" && "$TURNS_TOTAL" != "0" ]] || LINK6=fail

echo ""
echo "=== 7. Chain summary ==="
printf "  [1] Sarvam FINAL:     %s\n" "$LINK1"
printf "  [2] EndOfTurn:        %s\n" "$LINK2"
printf "  [3] Brain reply:      %s\n" "$LINK3"
printf "  [4] TTS egress (>1):  %s\n" "$LINK4"
printf "  [5] mouth_to_ear_ms:  %s\n" "$LINK5"
printf "  [6] media_turns>=1:   %s\n" "$LINK6"

PASS=1
for l in "$LINK1" "$LINK2" "$LINK3" "$LINK4" "$LINK5" "$LINK6"; do
  [[ "$l" == pass ]] || PASS=0
done

echo ""
if (( PASS == 1 )); then
  echo "OVERALL: PASS"
else
  echo "OVERALL: FAIL — see broken link(s) above"
fi

echo ""
echo "Full log: $LOG"
echo "See docs/FONADA_STAGING.md for staging phone call runbook."
