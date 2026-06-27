#!/usr/bin/env bash
# Start denoise, AMD, and semantic-turn workers in the background.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_FILE="$ROOT/scripts/.worker_pids"
LOG_FILE="$ROOT/scripts/workers.log"

mkdir -p "$ROOT/scripts"
: > "$LOG_FILE"

start_worker() {
  local name="$1"
  local addr="$2"
  local extra_env="${3:-}"
  local dir="$ROOT/workers/$name"
  # shellcheck disable=SC1091
  source "$dir/.venv/bin/activate"
  echo "[$(date -Iseconds)] starting $name on $addr" | tee -a "$LOG_FILE"
  if [[ -n "$extra_env" ]]; then
    eval "$extra_env python \"$dir/server.py\" --addr \"$addr\" >> \"$LOG_FILE\" 2>&1 &"
  else
    python "$dir/server.py" --addr "$addr" >> "$LOG_FILE" 2>&1 &
  fi
  local pid=$!
  echo "$name=$pid" >> "$PID_FILE"
  deactivate
  echo "  pid=$pid"
}

if [[ -f "$PID_FILE" ]]; then
  echo "WARN: $PID_FILE exists — run scripts/stop_workers.sh first or workers may duplicate"
fi

: > "$PID_FILE"

start_worker denoise "127.0.0.1:9091"

# AMD: default CPU int8 — reliable in WSL2. CUDA 13.0 + CTranslate2 needs libcublas.so.12 (cuBLAS 12).
# GPU path (optional, when cuBLAS 12 is available):
#   pip install nvidia-cublas-cu12 nvidia-cudnn-cu12
#   export LD_LIBRARY_PATH="$(
#     python3 -c \"import os,nvidia.cublas.lib,nvidia.cudnn.lib as c; print(':'.join([os.path.dirname(nvidia.cublas.lib.__file__), os.path.dirname(c.lib.__file__)]))\"
#   ):\$LD_LIBRARY_PATH"
#   start_worker amd "127.0.0.1:9092" "WHISPER_DEVICE=cuda WHISPER_MODEL=base"
# Production default: tiny (~745ms p50 on WSL CPU). Override with WHISPER_MODEL=base|small if needed.
start_worker amd "127.0.0.1:9092" "WHISPER_DEVICE=cpu WHISPER_MODEL=${WHISPER_MODEL:-tiny}"

start_worker semantic_turn "127.0.0.1:9093"

sleep 2
echo ""
echo "Workers started. PIDs in $PID_FILE"
echo "Combined log: $LOG_FILE"
echo "Tail: tail -f $LOG_FILE"
