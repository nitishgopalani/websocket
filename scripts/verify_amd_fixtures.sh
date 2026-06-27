#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
bash scripts/start_brain_stub.sh
bash scripts/stop_workers.sh 2>/dev/null || true
WHISPER_MODEL=base bash scripts/run_workers.sh
deadline=$((SECONDS + 180))
while (( SECONDS < deadline )); do
  if grep -q 'faster-whisper base loaded' scripts/workers.log 2>/dev/null && ss -tlnp | grep -q ':9092 '; then
    break
  fi
  sleep 3
done
grep 'faster-whisper.*loaded' scripts/workers.log | tail -1
echo "=== AMD fixture verify (Whisper base) ==="
python3 scripts/amd_classify_fixture.py testdata/calls/human_real.ulaw testdata/calls/voicemail_real.ulaw
