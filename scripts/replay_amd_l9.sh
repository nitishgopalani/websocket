#!/usr/bin/env bash
# L-9 AMD replay: human vs voicemail samples through the live pipeline (no paid APIs).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
# shellcheck disable=SC1091
source "$ROOT/scripts/load_env.sh"

HUMAN=""
VM=""
for f in testdata/calls/human_real.ulaw testdata/calls/human*.ulaw; do
  [[ -f "$f" ]] && HUMAN="$f" && break
done
for f in testdata/calls/voicemail_real.ulaw testdata/calls/voicemail*.ulaw; do
  [[ -f "$f" ]] && VM="$f" && break
done

if [[ -z "$HUMAN" || -z "$VM" ]]; then
  echo "FAIL: need human + voicemail ulaw under testdata/calls/"
  echo "  e.g. human_synthetic.ulaw + voicemail_synthetic.ulaw (SYNTHETIC)"
  echo "  or human_real.ulaw + voicemail_real.ulaw (real L-9 sign-off)"
  exit 1
fi

port_open() {
  ss -ltn 2>/dev/null | grep -q ":$1 " || (echo >/dev/tcp/127.0.0.1/"$1") 2>/dev/null
}

# AMD gate buffers 2s before classify; pad short ulaw with silence (0xFF) so session_stop does not fail-open early.
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

wait_ports() {
  local deadline=$((SECONDS + 180))
  while (( SECONDS < deadline )); do
    port_open 9091 && port_open 9092 && port_open 9093 && return 0
    sleep 3
  done
  return 1
}

wait_amd_ready() {
  local deadline=$((SECONDS + 120))
  while (( SECONDS < deadline )); do
    grep -q 'amd worker listening' scripts/workers.log 2>/dev/null && return 0
    sleep 2
  done
  return 1
}

SERVER_PID=""
cleanup() {
  [[ -n "$SERVER_PID" ]] && kill "$SERVER_PID" 2>/dev/null || true
  wait "$SERVER_PID" 2>/dev/null || true
}
trap cleanup EXIT

echo "=== L-9 AMD replay ==="
bash scripts/stop_workers.sh 2>/dev/null || true
bash scripts/run_workers.sh
echo "Waiting for worker ports..."
wait_ports || echo "WARN: not all worker ports open"
wait_amd_ready || echo "WARN: AMD worker not listening yet"

AMD_DEVICE="$(grep -E 'faster-whisper .* loaded' scripts/workers.log | tail -1 || true)"
echo "AMD startup: ${AMD_DEVICE:-unknown}"

set -a
load_env_local "$ROOT/.env.local"
set +a
: >>scripts/pipeline_server.log
go run ./cmd/server >>scripts/pipeline_server.log 2>&1 &
SERVER_PID=$!
echo "Waiting for Go server on :8080 (go run compile on /mnt/c can take ~30s)..."
deadline=$((SECONDS + 60))
while (( SECONDS < deadline )); do
  port_open 8080 && break
  sleep 2
done
port_open 8080 || { echo "FAIL: server not on :8080"; tail -20 scripts/pipeline_server.log; exit 1; }

replay_one() {
  local label="$1" file="$2" sid="$3"
  local padded
  padded=$(pad_ulaw_min_secs "$file" 3)
  local before=0
  [[ -f scripts/workers.log ]] && before=$(wc -l <scripts/workers.log)
  echo ""
  echo "--- Replay $label: $file (padded for 3s AMD window) ---"
  go run ./cmd/replay \
    -addr ws://127.0.0.1:8080/stream \
    -in "$padded" \
    -pace realtime \
    -stream-sid "$sid" \
    -call-sid "$sid" \
    -timeout 120s || true
  [[ "$padded" != "$file" ]] && rm -f "$padded"
  echo "Waiting for AMD classify..."
  local deadline=$((SECONDS + 30))
  while (( SECONDS < deadline )); do
    if tail -n +"$((before + 1))" scripts/workers.log 2>/dev/null | grep -q 'amd classify'; then
      break
    fi
    sleep 2
  done
  echo "AMD worker lines:"
  tail -n +"$((before + 1))" scripts/workers.log | grep -E 'amd classify|error_fail_open|libcublas' || true
  echo "Pipeline AMD lines:"
  grep -E '"msg":"amd (human|machine)' scripts/pipeline_server.log | tail -2 || true
}

replay_one "human" "$HUMAN" "MZ-L9-HUMAN"
replay_one "voicemail" "$VM" "MZ-L9-VM"

echo ""
echo "=== Summary ==="
echo "AMD device: ${AMD_DEVICE:-see workers.log}"
echo "Transcripts + decisions (workers.log):"
grep 'amd classify' scripts/workers.log | tail -4 || true
echo ""
echo "Real L-9 sign-off: drop 8 kHz mono ulaw/wav into testdata/calls/ as:"
echo "  testdata/calls/human_real.ulaw      — live hello pickup"
echo "  testdata/calls/voicemail_real.ulaw  — voicemail greeting"
echo "Then re-run: bash scripts/replay_amd_l9.sh"
