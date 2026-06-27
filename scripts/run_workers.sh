#!/usr/bin/env bash
# Start pipeline workers; wait until required ports are listening before returning.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_FILE="$ROOT/scripts/.worker_pids"
LOG_FILE="$ROOT/scripts/workers.log"

# shellcheck disable=SC1091
source "$ROOT/scripts/load_env.sh"
load_env_stack "$ROOT"

mkdir -p "$ROOT/scripts"
: > "$LOG_FILE"

env_true() {
  case "${1:-}" in
    1 | true | TRUE | yes | YES) return 0 ;;
    *) return 1 ;;
  esac
}

port_open() {
  local port="$1"
  ss -ltn 2>/dev/null | grep -q ":${port} " || (echo >/dev/tcp/127.0.0.1/"$port") 2>/dev/null
}

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

wait_workers_ready() {
  local need_denoise=0 need_amd=0 need_semantic=0
  env_true "${DENOISE_ENABLED:-false}" && need_denoise=1
  env_true "${AMD_ENABLED:-false}" && need_amd=1
  env_true "${SEMANTIC_TURN_ENABLED:-true}" && need_semantic=1

  local deadline=$((SECONDS + 180))
  echo "Waiting for workers (denoise=$need_denoise amd=$need_amd semantic=$need_semantic)..."
  while (( SECONDS < deadline )); do
    local ports_ok=1 logs_ok=1

    if (( need_denoise == 1 )); then
      port_open 9091 || ports_ok=0
      grep -q 'denoise worker listening' "$LOG_FILE" 2>/dev/null || logs_ok=0
    fi
    if (( need_amd == 1 )); then
      port_open 9092 || ports_ok=0
      grep -q 'amd worker listening' "$LOG_FILE" 2>/dev/null || logs_ok=0
      grep -q "faster-whisper ${WHISPER_MODEL:-base} loaded" "$LOG_FILE" 2>/dev/null || logs_ok=0
    fi
    if (( need_semantic == 1 )); then
      port_open 9093 || ports_ok=0
      grep -q 'semantic turn worker listening' "$LOG_FILE" 2>/dev/null || logs_ok=0
    fi

    if (( ports_ok == 1 && logs_ok == 1 )); then
      echo "Workers ready."
      return 0
    fi
    sleep 2
  done
  echo "FAIL: workers not ready within 180s"
  tail -30 "$LOG_FILE" || true
  return 1
}

if [[ -f "$PID_FILE" ]]; then
  echo "WARN: $PID_FILE exists — run scripts/stop_workers.sh first or workers may duplicate"
fi

: > "$PID_FILE"

if env_true "${DENOISE_ENABLED:-false}"; then
  start_worker denoise "127.0.0.1:9091"
else
  echo "[$(date -Iseconds)] skipping denoise worker (DENOISE_ENABLED=false)" | tee -a "$LOG_FILE"
fi

if env_true "${AMD_ENABLED:-false}"; then
  start_worker amd "127.0.0.1:9092" "WHISPER_DEVICE=cpu WHISPER_MODEL=${WHISPER_MODEL:-base}"
else
  echo "[$(date -Iseconds)] skipping amd worker (AMD_ENABLED=false)" | tee -a "$LOG_FILE"
fi

start_worker semantic_turn "127.0.0.1:9093"

wait_workers_ready

echo ""
echo "Workers started. PIDs in $PID_FILE"
echo "Combined log: $LOG_FILE"
echo "Tail: tail -f $LOG_FILE"
