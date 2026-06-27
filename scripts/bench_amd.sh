#!/usr/bin/env bash
# Benchmark AMD worker latency per WHISPER_MODEL (tiny/base/small) on CPU.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

RUNS="${BENCH_RUNS:-5}"
ADDR="${AMD_BENCH_ADDR:-127.0.0.1:9092}"
MODELS="${BENCH_MODELS:-tiny base small}"
AMD_DIR="$ROOT/workers/amd"
VENV_PY="$AMD_DIR/.venv/bin/python"

port_open() {
  (echo >/dev/tcp/127.0.0.1/9092) 2>/dev/null
}

wait_amd() {
  local deadline=$((SECONDS + 180))
  while (( SECONDS < deadline )); do
    port_open && grep -q 'amd worker listening' scripts/workers.log 2>/dev/null && return 0
    sleep 2
  done
  return 1
}

stop_amd() {
  if [[ -f scripts/.worker_pids ]]; then
    local pid
    pid=$(grep '^amd=' scripts/.worker_pids 2>/dev/null | cut -d= -f2 || true)
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
    fi
  fi
  pkill -f "workers/amd/server.py" 2>/dev/null || true
  sleep 1
}

start_amd_model() {
  local model="$1"
  stop_amd
  : > scripts/workers.log
  # shellcheck disable=SC1091
  source "$AMD_DIR/.venv/bin/activate"
  echo "[$(date -Iseconds)] bench amd model=$model on $ADDR" | tee -a scripts/workers.log
  WHISPER_DEVICE=cpu WHISPER_MODEL="$model" python "$AMD_DIR/server.py" --addr "$ADDR" >>scripts/workers.log 2>&1 &
  local pid=$!
  echo "amd=$pid" > scripts/.worker_pids
  deactivate
  wait_amd || { echo "FAIL: AMD worker did not start for model=$model"; tail -10 scripts/workers.log; exit 1; }
  # Warm-up classify (model load / mmap on /mnt/c) — excluded from stats.
  "$VENV_PY" - testdata/calls/human_synthetic.ulaw "$ADDR" <<'PY' >/dev/null 2>&1 || true
import audioop, socket, struct, sys
path, addr = sys.argv[1], sys.argv[2]
host, port = addr.rsplit(":", 1)
pcm = audioop.ulaw2lin(open(path, "rb").read(), 2)[:32000]
body = struct.pack("<H", 8000) + pcm
req = struct.pack("<I", len(body)) + body
s = socket.create_connection((host, int(port)), timeout=120)
s.sendall(req)
n = struct.unpack("<I", s.recv(4))[0]
s.recv(n)
s.close()
PY
  echo "AMD ready: model=$model pid=$pid (warmed up)"
}

collect_samples() {
  local -n _out=$1
  _out=()
  declare -A seen=()
  local f base
  for f in testdata/calls/human_synthetic.ulaw testdata/calls/voicemail_synthetic.ulaw testdata/calls/*.ulaw; do
    [[ -f "$f" ]] || continue
    base=$(basename "$f")
    [[ -n "${seen[$base]:-}" ]] && continue
    seen[$base]=1
    _out+=("$f")
  done
  if ((${#_out[@]} == 0)); then
    echo "FAIL: no .ulaw samples under testdata/calls/"
    exit 1
  fi
}

percentile() {
  local p="$1"
  shift
  "$VENV_PY" - "$p" "$@" <<'PY'
import sys
vals = sorted(float(x) for x in sys.argv[2:])
if not vals:
    print("0")
    sys.exit(0)
p = float(sys.argv[1])
idx = min(len(vals) - 1, max(0, int(round((p / 100.0) * (len(vals) - 1)))))
print(int(vals[idx]))
PY
}

declare -a SAMPLES=()
collect_samples SAMPLES
echo "Samples (${#SAMPLES[@]}): ${SAMPLES[*]}"
echo "Runs per sample: $RUNS"
echo ""

declare -A P50 P95

for model in $MODELS; do
  echo "======== model=$model ========"
  start_amd_model "$model"
  latencies=()
  for sample in "${SAMPLES[@]}"; do
    for ((i = 1; i <= RUNS; i++)); do
      ms=$("$VENV_PY" - "$sample" "$ADDR" <<'PY'
import audioop
import json
import socket
import struct
import sys
import time

path, addr = sys.argv[1], sys.argv[2]
host, port = addr.rsplit(":", 1)
ulaw = open(path, "rb").read()
pcm = audioop.ulaw2lin(ulaw, 2)
rate = 8000
window_ms = 2000
max_bytes = rate * 2 * window_ms // 1000
pcm = pcm[:max_bytes]
body = struct.pack("<H", rate) + pcm
req = struct.pack("<I", len(body)) + body
t0 = time.perf_counter()
s = socket.create_connection((host, int(port)), timeout=120)
s.sendall(req)
hdr = s.recv(4)
n = struct.unpack("<I", hdr)[0]
resp = s.recv(n)
s.close()
elapsed = int((time.perf_counter() - t0) * 1000)
print(elapsed)
PY
)
      latencies+=("$ms")
      echo "  $sample run $i: ${ms}ms (client RTT)"
    done
  done
  p50=$(percentile 50 "${latencies[@]}")
  p95=$(percentile 95 "${latencies[@]}")
  P50[$model]=$p50
  P95[$model]=$p95
  echo "model=$model p50=${p50}ms p95=${p95}ms (n=${#latencies[@]} client RTT)"
  echo ""
done

stop_amd

echo "================================================================"
echo "AMD LATENCY COMPARISON (CPU int8, ${RUNS} runs x ${#SAMPLES[@]} samples, 2s window)"
echo "================================================================"
printf "%-8s %8s %8s\n" "MODEL" "P50_ms" "P95_ms"
printf "%-8s %8s %8s\n" "-----" "------" "------"
for model in $MODELS; do
  printf "%-8s %8s %8s\n" "$model" "${P50[$model]}" "${P95[$model]}"
done
echo "================================================================"
echo "Default: WHISPER_MODEL=tiny in run_workers.sh (override env); code fallback WHISPER_MODEL=base"
echo "Worker logs latency_ms= in scripts/workers.log per classify"
