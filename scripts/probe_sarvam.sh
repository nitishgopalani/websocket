#!/usr/bin/env bash
# Minimal Sarvam STT WebSocket probe (~1.5s PCM, one session). Keys never printed.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
# shellcheck disable=SC1091
source "$ROOT/scripts/load_env.sh"
load_env_stack "$ROOT"

export ASR_LANGUAGE="${ASR_LANGUAGE:-en-IN}"
export PROBE_FIXTURE="${PROBE_FIXTURE:-$ROOT/testdata/calls/human_long.ulaw}"

echo "======== SARVAM PROBE — $(date -Iseconds) ========"
print_key_status
if [[ -z "${SARVAM_API_KEY:-}" ]]; then
  echo "FAIL: SARVAM_API_KEY empty"
  exit 1
fi
echo "fixture: $PROBE_FIXTURE"
echo "language: $ASR_LANGUAGE"
echo ""

go run ./cmd/probe_sarvam
