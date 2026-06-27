#!/usr/bin/env bash
# Finish local verify: L-6..L-9 (core workers already installed; SILERO_SETUP=0).
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
# shellcheck disable=SC1091
source "$ROOT/scripts/load_env.sh"
LOG="$ROOT/scripts/local_setup_run.log"
export SILERO_SETUP=0

exec >>"$LOG" 2>&1

echo ""
echo "================================================================"
echo "RESUME L-6..L-9 — $(date -Iseconds)"
echo "================================================================"

declare -A STEP_STATUS STEP_SIGNAL

mark() {
  STEP_STATUS["$1"]="$2"
  STEP_SIGNAL["$1"]="$3"
}

port_open() {
  ss -ltn 2>/dev/null | grep -q ":$1 " || (echo >/dev/tcp/127.0.0.1/"$1") 2>/dev/null
}

# L-0 recap
echo ""
echo "======== L-0 PREREQS (recap) ========"
if nvidia-smi >/tmp/nvidia_smi.out 2>&1; then
  GPU_LINE="$(grep -E 'GeForce|RTX|CUDA Version' /tmp/nvidia_smi.out | tr '\n' ' ' | head -c 120)"
  mark L-0 PASS "$GPU_LINE"
  head -12 /tmp/nvidia_smi.out
else
  mark L-0 DEGRADED "GPU NOT visible in WSL2"
fi

# L-1..L-3 from installed venvs
echo ""
echo "======== L-1..L-3 WORKER INSTALL (recap) ========"
DENOISE_IMPORT=FAIL
if workers/denoise/.venv/bin/python -c "from df.enhance import enhance, init_df" 2>/dev/null; then
  DENOISE_IMPORT=OK
fi
WHISPER_CACHE=unknown
if workers/amd/.venv/bin/python -c "from faster_whisper import WhisperModel; WhisperModel('base', device='cpu', compute_type='int8')" 2>/dev/null; then
  WHISPER_CACHE=OK
fi
ONNX_PRESENT=no
if [[ -f workers/semantic_turn/smart-turn-v3.1.onnx ]]; then ONNX_PRESENT=yes
elif [[ -f workers/semantic_turn/smart-turn-v3.0.onnx ]]; then ONNX_PRESENT=yes
fi
L13="denoise=$DENOISE_IMPORT whisper=$WHISPER_CACHE ONNX=$ONNX_PRESENT"
if [[ "$DENOISE_IMPORT" == OK && "$WHISPER_CACHE" == OK ]]; then
  mark L-1 PASS "$L13"
  mark L-2 PASS "$L13"
  mark L-3 PASS "$L13"
else
  mark L-1 DEGRADED "$L13"
  mark L-2 DEGRADED "$L13"
  mark L-3 DEGRADED "$L13"
fi
echo "$L13"

mark L-4 DEGRADED "SKIPPED (SILERO_SETUP=0); EnergyVAD fallback in Go"

# L-6 smokes
echo ""
echo "======== L-6 PER-WORKER SMOKE ========"
SMOKE_PASS=0 SMOKE_FAIL=0
for name in denoise amd semantic_turn; do
  echo "--- $name test_smoke.py ---"
  if (cd "workers/$name" && .venv/bin/python -m pytest test_smoke.py -v --tb=short 2>&1); then
    echo "PASS: $name"
    SMOKE_PASS=$((SMOKE_PASS + 1))
  else
    echo "FAIL: $name"
    SMOKE_FAIL=$((SMOKE_FAIL + 1))
  fi
done
if [[ $SMOKE_FAIL -eq 0 ]]; then
  mark L-6 PASS "smokes $SMOKE_PASS/3; $L13"
else
  mark L-6 DEGRADED "smokes pass=$SMOKE_PASS fail=$SMOKE_FAIL"
fi

wait_for_ports() {
  local deadline=$((SECONDS + 180))
  while (( SECONDS < deadline )); do
    local ok=1
    for p in 9091 9092 9093; do
      port_open "$p" || ok=0
    done
    if (( ok == 1 )); then
      return 0
    fi
    sleep 3
  done
  return 1
}

# L-5 env
echo ""
echo "======== L-5 ENV ========"
if [[ -f .env.local ]]; then
  grep -v '^#' .env.local | grep -v '^$' || true
  mark L-5 PASS "workers ON, ASR/TTS/BRAIN OFF"
else
  mark L-5 FAIL ".env.local missing"
fi

# Start workers
echo ""
echo "======== START WORKERS ========"
bash scripts/stop_workers.sh 2>/dev/null || true
sleep 1
bash scripts/run_workers.sh || true
echo "Waiting up to 180s for worker ports..."
if wait_for_ports; then
  echo "All worker ports open"
else
  echo "WARN: not all worker ports open after 180s"
fi
sleep 2
PORTS=""
for p in 9091 9092 9093; do
  port_open "$p" && PORTS="${PORTS}${p},"
done
PORTS="${PORTS%,}"
AMD_DEVICE="unknown"
ST_MODE="unknown"
sleep 2
if [[ -f scripts/workers.log ]]; then
  AMD_DEVICE="$(grep -E 'faster-whisper|CUDA init|device=' scripts/workers.log | tail -3 | tr '\n' '; ')"
  ST_MODE="$(grep -E 'smart-turn ONNX|heuristic fallback|ONNX not found' scripts/workers.log | tail -2 | tr '\n' '; ')"
  tail -15 scripts/workers.log || true
fi
echo "Ports: ${PORTS:-none}; AMD: $AMD_DEVICE; ST: $ST_MODE"
if [[ "$PORTS" == *9091* && "$PORTS" == *9092* && "$PORTS" == *9093* ]]; then
  mark L-5 PASS "env OK; ports $PORTS; AMD: ${AMD_DEVICE:0:60}"
else
  mark L-5 DEGRADED "ports ${PORTS:-none}; AMD: ${AMD_DEVICE:0:60}"
fi

# L-7
echo ""
echo "======== L-7 GO<->WORKER INTEGRATION ========"
load_env_local "$ROOT/.env.local"
export DENOISE_TIMEOUT_MS="${DENOISE_TIMEOUT_MS:-5000}"
export AMD_TIMEOUT_MS="${AMD_TIMEOUT_MS:-30000}"
export SEMANTIC_TURN_TIMEOUT_MS="${SEMANTIC_TURN_TIMEOUT_MS:-5000}"

if WORKERS_LIVE=1 go test ./internal/media -run TestWorkersLiveIntegration -v -count=1 2>&1 | tee /tmp/l7.out; then
  mark L-7 PASS "$(grep -E 'RemoteDenoiser|RemoteAMDClassifier|RemoteSemanticTurn|complete=|result=' /tmp/l7.out | tr '\n' ' ' | head -c 180)"
else
  mark L-7 FAIL "$(tail -5 /tmp/l7.out | tr '\n' ' ')"
fi

# L-8
echo ""
echo "======== L-8 PIPELINE E2E ========"
SERVER_PID=""
cleanup_all() {
  if [[ -n "${SERVER_PID:-}" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  bash scripts/stop_workers.sh 2>/dev/null || true
}
trap cleanup_all EXIT

set -a
load_env_local "$ROOT/.env.local"
set +a
go run ./cmd/server >>scripts/pipeline_server.log 2>&1 &
SERVER_PID=$!
sleep 8

L8_OK=0
if port_open 8080; then
  if go run ./cmd/replay -addr ws://127.0.0.1:8080/stream -in testdata/smoke.ulaw -pace fast -timeout 90s 2>&1 | tee /tmp/l8_replay.out; then
    L8_OK=1
  fi
  METRICS="$(curl -sf http://127.0.0.1:8080/metrics 2>/dev/null || echo '')"
  echo "--- /metrics ---"
  echo "$METRICS" | grep -E '^(media_denoise_fallbacks_total|media_amd_human_total|media_amd_machine_total|media_turns_total|media_mouth_to_ear)' || true
  DFB="$(echo "$METRICS" | awk '/^media_denoise_fallbacks_total /{print $2; exit}')"
  TURNS="$(echo "$METRICS" | awk '/^media_turns_total /{print $2; exit}')"
  M2E="$(echo "$METRICS" | awk '/^media_mouth_to_ear_ms_count /{print $2; exit}')"
  L8SIG="denoise_fb=${DFB:-0} turns=${TURNS:-0} mouth_to_ear_count=${M2E:-0}"
  if [[ $L8_OK -eq 1 ]]; then mark L-8 PASS "$L8SIG"; else mark L-8 DEGRADED "$L8SIG replay had errors"; fi
else
  mark L-8 FAIL "server not on :8080"
  tail -20 scripts/pipeline_server.log 2>/dev/null || true
fi

# L-9 AMD samples
echo ""
echo "======== L-9 AMD REAL-SAMPLE ========"
mkdir -p testdata/calls
HUMAN_ULAW="" VM_ULAW=""
for f in testdata/calls/human*.ulaw testdata/calls/*human*.ulaw; do [[ -f "$f" ]] && HUMAN_ULAW="$f" && break; done
for f in testdata/calls/voicemail*.ulaw testdata/calls/*voicemail*.ulaw; do [[ -f "$f" ]] && VM_ULAW="$f" && break; done

if [[ -z "$HUMAN_ULAW" || -z "$VM_ULAW" ]]; then
  echo "SYNTHETIC: generating with espeak-ng + ffmpeg"
  espeak-ng -v en "Hello? Who is this speaking?" -w testdata/calls/human_synthetic.wav 2>/dev/null || true
  espeak-ng -v en "The person you have called is not available. Please leave your message after the tone." -w testdata/calls/voicemail_synthetic.wav 2>/dev/null || true
  ffmpeg -y -i testdata/calls/human_synthetic.wav -ar 8000 -ac 1 -f mulaw testdata/calls/human_synthetic.ulaw 2>/dev/null || true
  ffmpeg -y -i testdata/calls/voicemail_synthetic.wav -ar 8000 -ac 1 -f mulaw testdata/calls/voicemail_synthetic.ulaw 2>/dev/null || true
  HUMAN_ULAW="testdata/calls/human_synthetic.ulaw"
  VM_ULAW="testdata/calls/voicemail_synthetic.ulaw"
  echo "NOTE: SYNTHETIC placeholders for sign-off"
fi

amd_replay_decision() {
  local file="$1"
  local before=0
  [[ -f scripts/pipeline_server.log ]] && before=$(wc -l <scripts/pipeline_server.log)
  go run ./cmd/replay -addr ws://127.0.0.1:8080/stream -in "$file" -pace fast -timeout 120s >/dev/null 2>&1 || true
  sleep 3
  if [[ -f scripts/pipeline_server.log ]]; then
    tail -n +"$((before + 1))" scripts/pipeline_server.log \
      | grep -E '"msg":"amd (human|machine)' \
      | tail -3 \
      | tr '\n' '; '
  fi
}

HUMAN_DEC="" VM_DEC=""
if [[ -f "$HUMAN_ULAW" ]]; then
  echo "Replay human: $HUMAN_ULAW"
  HUMAN_DEC="$(amd_replay_decision "$HUMAN_ULAW")"
  echo "Human: $HUMAN_DEC"
fi
if [[ -f "$VM_ULAW" ]]; then
  echo "Replay voicemail: $VM_ULAW"
  VM_DEC="$(amd_replay_decision "$VM_ULAW")"
  echo "Voicemail: $VM_DEC"
fi

L9SIG="human->${HUMAN_DEC:-skip}; vm->${VM_DEC:-skip}"
if echo "$HUMAN_DEC" | grep -q 'amd human' && echo "$VM_DEC" | grep -q 'amd machine'; then
  mark L-9 PASS "$L9SIG"
elif echo "$HUMAN_DEC" | grep -q 'amd human' && [[ -n "$VM_DEC" ]]; then
  mark L-9 DEGRADED "$L9SIG (voicemail not machine — check transcript in workers.log)"
elif [[ -n "$HUMAN_DEC" && -n "$VM_DEC" ]]; then
  mark L-9 DEGRADED "$L9SIG"
else
  mark L-9 FAIL "$L9SIG"
fi

cleanup_all
trap - EXIT

echo ""
echo "================================================================"
echo "STEP SUMMARY"
echo "================================================================"
printf "%-6s %-10s %s\n" "STEP" "STATUS" "KEY SIGNAL"
printf "%-6s %-10s %s\n" "----" "------" "-----------"
for step in L-0 L-1 L-2 L-3 L-4 L-5 L-6 L-7 L-8 L-9; do
  [[ -z "${STEP_STATUS[$step]:-}" ]] && continue
  sig="${STEP_SIGNAL[$step]:-}"
  sig="${sig:0:120}"
  printf "%-6s %-10s %s\n" "$step" "${STEP_STATUS[$step]}" "$sig"
done
echo "AMD device recap: $AMD_DEVICE"
echo "Semantic recap: $ST_MODE"
echo "Full log: $LOG"
echo "================================================================"
