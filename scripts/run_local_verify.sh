#!/usr/bin/env bash
# L-0..L-9 local worker verification — fail-open, log everything.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
# shellcheck disable=SC1091
source "$ROOT/scripts/load_env.sh"
LOG="$ROOT/scripts/local_setup_run.log"
mkdir -p "$ROOT/scripts"

exec > >(tee -a "$LOG") 2>&1

echo "================================================================"
echo "LOCAL WORKER VERIFY — $(date -Iseconds)"
echo "ROOT=$ROOT"
echo "================================================================"

declare -A STEP_STATUS
declare -A STEP_SIGNAL

mark() {
  local step="$1" status="$2" signal="$3"
  STEP_STATUS["$step"]="$status"
  STEP_SIGNAL["$step"]="$signal"
}

port_open() {
  local port="$1"
  if command -v ss >/dev/null 2>&1; then
    ss -ltn 2>/dev/null | grep -q ":${port} "
    return
  fi
  (echo >/dev/tcp/127.0.0.1/"$port") 2>/dev/null
}

# --- L-0 PREREQS ---
echo ""
echo "======== L-0 PREREQS ========"
GPU_LINE="GPU NOT visible in WSL2"
if nvidia-smi >/tmp/nvidia_smi.out 2>&1; then
  GPU_NAME="$(grep -E 'GeForce|RTX|GTX|Tesla|Quadro' /tmp/nvidia_smi.out | head -1 | sed 's/^[[:space:]]*|//' | xargs || true)"
  CUDA_VER="$(grep 'CUDA Version' /tmp/nvidia_smi.out | head -1 | sed 's/.*CUDA Version: //' | awk '{print $1}')"
  GPU_LINE="${GPU_NAME:-NVIDIA GPU}, CUDA ${CUDA_VER:-unknown}"
  echo "nvidia-smi OK: $GPU_LINE"
  cat /tmp/nvidia_smi.out | head -12
  mark L-0 PASS "$GPU_LINE"
else
  echo "nvidia-smi FAILED — AMD will fall back to CPU"
  cat /tmp/nvidia_smi.out 2>/dev/null || true
  mark L-0 DEGRADED "GPU NOT visible in WSL2; AMD CPU fallback"
fi

PYVER="$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}")' 2>/dev/null || echo missing)"
echo "python3: $PYVER"
if ! python3 -c 'import sys; exit(0 if sys.version_info >= (3,10) else 1)' 2>/dev/null; then
  mark L-0 FAIL "python3 < 3.10 ($PYVER)"
else
  if ! python3 -m pip --version >/dev/null 2>&1; then
    echo "Installing python3-pip python3-venv via apt..."
    apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq python3-pip python3-venv >/dev/null 2>&1 || true
  fi
  PIPVER="$(python3 -m pip --version 2>/dev/null || echo missing)"
  echo "pip: $PIPVER"
  if [[ "$PIPVER" == missing ]]; then
    [[ "${STEP_STATUS[L-0]:-}" != FAIL ]] && mark L-0 DEGRADED "python OK, pip missing"
  elif [[ "${STEP_STATUS[L-0]:-}" != FAIL && "${STEP_STATUS[L-0]:-}" != DEGRADED ]]; then
    mark L-0 PASS "python $PYVER, $PIPVER"
  fi
fi

if ! command -v go >/dev/null 2>&1; then
  echo "Installing golang-go via apt..."
  apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq golang-go curl wget ffmpeg espeak-ng netcat-openbsd >/dev/null 2>&1 || true
fi
GOVER="$(go version 2>/dev/null || echo 'go missing')"
echo "go: $GOVER"

# --- L-1..L-3, L-6 SETUP WORKERS ---
echo ""
echo "======== L-1..L-3, L-6 SETUP WORKERS + SMOKE ========"
chmod +x scripts/*.sh 2>/dev/null || true
SETUP_OK=1
ONNX_PRESENT=no
DENOISE_SMOKE=unknown
AMD_SMOKE=unknown
ST_SMOKE=unknown

if bash scripts/setup_workers.sh; then
  echo "setup_workers.sh: OK"
else
  echo "setup_workers.sh: FAILED (continuing)"
  SETUP_OK=0
fi

for f in workers/semantic_turn/smart-turn-v3.1.onnx workers/semantic_turn/smart-turn-v3.0.onnx; do
  if [[ -f "$f" ]]; then ONNX_PRESENT=yes; ONNX_FILE="$f"; break; fi
done

# denoise import
if workers/denoise/.venv/bin/python -c "from df.enhance import init_df" 2>/dev/null; then
  DENOISE_IMPORT=OK
else
  DENOISE_IMPORT=FAIL
fi

# whisper cache check
if workers/amd/.venv/bin/python -c "from faster_whisper import WhisperModel; WhisperModel('small', device='cpu', compute_type='int8')" 2>/dev/null; then
  WHISPER_CACHE=OK
else
  WHISPER_CACHE=unknown
fi

for name in denoise amd semantic_turn; do
  dir="workers/$name"
  if (cd "$dir" && ../denoise/.venv/bin/true 2>/dev/null); then :; fi
  if (cd "$dir" && .venv/bin/python -m pytest test_smoke.py -q 2>/tmp/smoke_${name}.out); then
    eval "${name^^}_SMOKE=PASS" 2>/dev/null || true
    case $name in denoise) DENOISE_SMOKE=PASS;; amd) AMD_SMOKE=PASS;; semantic_turn) ST_SMOKE=PASS;; esac
  else
    case $name in denoise) DENOISE_SMOKE=FAIL;; amd) AMD_SMOKE=FAIL;; semantic_turn) ST_SMOKE=FAIL;; esac
    cat /tmp/smoke_${name}.out 2>/dev/null || true
  fi
done

SETUP_SIGNAL="denoise=$DENOISE_IMPORT smoke=$DENOISE_SMOKE; whisper=$WHISPER_CACHE amd_smoke=$AMD_SMOKE; ONNX=$ONNX_PRESENT st_smoke=$ST_SMOKE"
if [[ $SETUP_OK -eq 1 && "$DENOISE_SMOKE" == PASS && "$AMD_SMOKE" == PASS && "$ST_SMOKE" == PASS ]]; then
  mark L-1 PASS "$SETUP_SIGNAL"
  mark L-2 PASS "$SETUP_SIGNAL"
  mark L-3 PASS "$SETUP_SIGNAL"
  mark L-6 PASS "$SETUP_SIGNAL"
elif [[ $SETUP_OK -eq 1 ]]; then
  mark L-1 DEGRADED "$SETUP_SIGNAL"
  mark L-2 DEGRADED "$SETUP_SIGNAL"
  mark L-3 DEGRADED "$SETUP_SIGNAL"
  mark L-6 DEGRADED "$SETUP_SIGNAL"
else
  mark L-1 FAIL "$SETUP_SIGNAL"
  mark L-2 FAIL "$SETUP_SIGNAL"
  mark L-3 FAIL "$SETUP_SIGNAL"
  mark L-6 FAIL "$SETUP_SIGNAL"
fi

# --- L-4 SILERO ---
echo ""
echo "======== L-4 SILERO (optional) ========"
SILERO_STATUS=skipped
if [[ "${SILERO_SETUP:-0}" == "1" && -f workers/requirements-silero.txt ]]; then
  SILERO_VENV="$ROOT/workers/.venv-silero"
  python3 -m venv "$SILERO_VENV" 2>/dev/null || true
  if "$SILERO_VENV/bin/pip" install -q -r workers/requirements-silero.txt 2>/tmp/silero_install.out \
    && "$SILERO_VENV/bin/python" -c "import torch; m, _ = torch.hub.load('snakers4/silero-vad', 'silero_vad', trust_repo=True); print('silero ok')" 2>/tmp/silero_load.out; then
    SILERO_STATUS=PASS
    mark L-4 PASS "silero-vad load OK"
  else
    SILERO_STATUS=FAIL
    cat /tmp/silero_install.out /tmp/silero_load.out 2>/dev/null || true
    mark L-4 DEGRADED "silero optional/skipped"
  fi
else
  mark L-4 DEGRADED "skipped (SILERO_SETUP=0); ONNX at Another_testing LiveKit venv if needed later"
fi

# --- L-5 ENV ---
echo ""
echo "======== L-5 ENV + RUN SCRIPTS ========"
if [[ -f .env.local ]]; then
  echo "--- .env.local ---"
  grep -v '^#' .env.local | grep -v '^$' || true
  mark L-5 PASS "workers ON, ASR/TTS/BRAIN OFF"
else
  mark L-5 FAIL ".env.local missing"
fi

# --- START WORKERS ---
echo ""
echo "======== START WORKERS ========"
bash scripts/stop_workers.sh 2>/dev/null || true
sleep 1
bash scripts/run_workers.sh || true
sleep 4

PORTS_UP=""
for p in 9091 9092 9093; do
  if port_open "$p"; then PORTS_UP="${PORTS_UP}${p},"; fi
done
PORTS_UP="${PORTS_UP%,}"
echo "Ports listening: ${PORTS_UP:-none}"

AMD_DEVICE="unknown"
ST_MODE="unknown"
if [[ -f scripts/workers.log ]]; then
  AMD_DEVICE="$(grep -E 'faster-whisper small loaded|CUDA init failed|device=' scripts/workers.log | tail -1 || echo unknown)"
  ST_MODE="$(grep -E 'smart-turn ONNX loaded|heuristic fallback|ONNX not found' scripts/workers.log | tail -1 || echo unknown)"
  echo "AMD log: $AMD_DEVICE"
  echo "Semantic log: $ST_MODE"
  tail -20 scripts/workers.log || true
fi

if [[ "$PORTS_UP" == *9091* && "$PORTS_UP" == *9092* && "$PORTS_UP" == *9093* ]]; then
  WORKER_START="ports $PORTS_UP; AMD: $AMD_DEVICE; ST: $ST_MODE"
else
  WORKER_START="ports ${PORTS_UP:-none} DEGRADED; AMD: $AMD_DEVICE"
fi
echo "Worker start: $WORKER_START"

# --- L-7 GO INTEGRATION ---
echo ""
echo "======== L-7 GO <-> WORKER INTEGRATION ========"
set -a
load_env_local "$ROOT/.env.local"
set +a
export DENOISE_TIMEOUT_MS="${DENOISE_TIMEOUT_MS:-2000}"
export AMD_TIMEOUT_MS="${AMD_TIMEOUT_MS:-10000}"
export SEMANTIC_TURN_TIMEOUT_MS="${SEMANTIC_TURN_TIMEOUT_MS:-2000}"

L7_OUT=/tmp/workers_live.out
if WORKERS_LIVE=1 go test ./internal/media -run TestWorkersLiveIntegration -v -count=1 2>&1 | tee "$L7_OUT"; then
  L7_SIGNAL="$(grep -E 'RemoteDenoiser|RemoteAMDClassifier|RemoteSemanticTurn|complete=|result=' "$L7_OUT" | tr '\n' ' ' | head -c 200)"
  mark L-7 PASS "${L7_SIGNAL:-all round-trips OK}; $WORKER_START"
else
  mark L-7 FAIL "$(tail -3 "$L7_OUT" | tr '\n' ' '); $WORKER_START"
fi

# --- L-8 PIPELINE E2E ---
echo ""
echo "======== L-8 PIPELINE E2E ========"
SERVER_PID=""
cleanup_server() {
  if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
}
bash scripts/stop_workers.sh 2>/dev/null || true
sleep 1
bash scripts/run_workers.sh || true
sleep 3

set -a
load_env_local "$ROOT/.env.local"
set +a
go run ./cmd/server >> scripts/pipeline_server.log 2>&1 &
SERVER_PID=$!
sleep 3

L8_OK=0
if port_open 8080; then
  REPLAY_OUT=/tmp/replay.out
  if go run ./cmd/replay -addr ws://127.0.0.1:8080/stream -in testdata/smoke.ulaw -pace fast -timeout 60s 2>&1 | tee "$REPLAY_OUT"; then
    L8_OK=1
  fi
  METRICS="$(curl -sf http://127.0.0.1:8080/metrics 2>/dev/null || echo '')"
  echo "--- /metrics ---"
  echo "$METRICS" | grep -E '^(media_denoise_fallbacks_total|media_amd_human_total|media_amd_machine_total|media_turns_total|media_mouth_to_ear)' || true
  DFB="$(echo "$METRICS" | awk '/^media_denoise_fallbacks_total /{print $2; exit}')"
  TURNS="$(echo "$METRICS" | awk '/^media_turns_total /{print $2; exit}')"
  L8_SIGNAL="denoise_fb=${DFB:-?} turns=${TURNS:-0}"
  if [[ $L8_OK -eq 1 ]]; then
    mark L-8 PASS "$L8_SIGNAL"
  else
    mark L-8 DEGRADED "$L8_SIGNAL replay errors"
  fi
else
  mark L-8 FAIL "server not on :8080"
fi
cleanup_server

# --- L-9 AMD REAL-SAMPLE ---
echo ""
echo "======== L-9 AMD REAL-SAMPLE TEST ========"
mkdir -p testdata/calls
HUMAN_ULAW=""
VM_ULAW=""
for f in testdata/calls/human*.ulaw testdata/calls/*human*.ulaw; do
  [[ -f "$f" ]] && HUMAN_ULAW="$f" && break
done
for f in testdata/calls/voicemail*.ulaw testdata/calls/*voicemail*.ulaw; do
  [[ -f "$f" ]] && VM_ULAW="$f" && break
done

if [[ -z "$HUMAN_ULAW" || -z "$VM_ULAW" ]]; then
  echo "Synthesizing SYNTHETIC placeholder greetings (offline TTS)..."
  SYNTH_DIR="$ROOT/testdata/calls"
  PIPER_OK=0
  if pip3 install -q piper-tts 2>/tmp/piper_install.out || python3 -m pip install -q piper-tts 2>/tmp/piper_install.out; then
    PIPER_OK=1
  fi
  gen_wav() {
    local text="$1" out="$2"
    if [[ $PIPER_OK -eq 1 ]] && piper --help >/dev/null 2>&1; then
      echo "$text" | piper --model en_US-lessac-medium --output_file "$out" 2>/dev/null && return 0
    fi
    espeak-ng -v en "$text" -w "$out" 2>/dev/null
  }
  gen_wav "Hello? Who is this?" "$SYNTH_DIR/human_synthetic.wav" || true
  gen_wav "The person you have called is not available. Please leave your message after the tone. Beep." "$SYNTH_DIR/voicemail_synthetic.wav" || true
  if [[ -f "$SYNTH_DIR/human_synthetic.wav" ]]; then
    ffmpeg -y -i "$SYNTH_DIR/human_synthetic.wav" -ar 8000 -ac 1 -f mulaw "$SYNTH_DIR/human_synthetic.ulaw" 2>/dev/null || true
    HUMAN_ULAW="$SYNTH_DIR/human_synthetic.ulaw"
  fi
  if [[ -f "$SYNTH_DIR/voicemail_synthetic.wav" ]]; then
    ffmpeg -y -i "$SYNTH_DIR/voicemail_synthetic.wav" -ar 8000 -ac 1 -f mulaw "$SYNTH_DIR/voicemail_synthetic.ulaw" 2>/dev/null || true
    VM_ULAW="$SYNTH_DIR/voicemail_synthetic.ulaw"
  fi
  echo "NOTE: SYNTHETIC placeholders — replace with real recordings for final sign-off"
fi

bash scripts/stop_workers.sh 2>/dev/null || true
sleep 1
: > scripts/workers.log
bash scripts/run_workers.sh || true
sleep 4
go run ./cmd/server >> scripts/pipeline_server.log 2>&1 &
SERVER_PID=$!
sleep 3

amd_decision() {
  local file="$1"
  : > scripts/workers.log
  go run ./cmd/replay -addr ws://127.0.0.1:8080/stream -in "$file" -pace fast -timeout 90s >/dev/null 2>&1 || true
  sleep 2
  grep -E 'result.*human|result.*machine|voicemail|no_voicemail|AMD|amd_worker' scripts/workers.log scripts/pipeline_server.log 2>/dev/null | tail -5
}

HUMAN_DEC="unknown"
VM_DEC="unknown"
if [[ -n "$HUMAN_ULAW" && -f "$HUMAN_ULAW" ]]; then
  echo "Replay human: $HUMAN_ULAW"
  HUMAN_DEC="$(amd_decision "$HUMAN_ULAW" | tr '\n' '; ')"
  echo "Human AMD signals: $HUMAN_DEC"
fi
if [[ -n "$VM_ULAW" && -f "$VM_ULAW" ]]; then
  echo "Replay voicemail: $VM_ULAW"
  VM_DEC="$(amd_decision "$VM_ULAW" | tr '\n' '; ')"
  echo "Voicemail AMD signals: $VM_DEC"
fi

cleanup_server
L9_SIGNAL="human->${HUMAN_DEC:-skip}; vm->${VM_DEC:-skip}"
if echo "$HUMAN_DEC" | grep -qi human && echo "$VM_DEC" | grep -qiE 'machine|voicemail'; then
  mark L-9 PASS "$L9_SIGNAL"
elif [[ -n "$HUMAN_ULAW" && -n "$VM_ULAW" ]]; then
  mark L-9 DEGRADED "$L9_SIGNAL"
else
  mark L-9 FAIL "samples missing"
fi

# --- CLEANUP ---
echo ""
echo "======== CLEANUP ========"
bash scripts/stop_workers.sh 2>/dev/null || true
cleanup_server 2>/dev/null || true

# --- STEP SUMMARY ---
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
echo "================================================================"
echo "Full log: $LOG"
