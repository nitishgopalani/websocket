#!/usr/bin/env bash
# Cheap connectivity check for Sarvam, ElevenLabs, and Collection brain (EB-6).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
# shellcheck disable=SC1091
source "$ROOT/scripts/load_env.sh"

echo "======== LIVE PREFLIGHT ========"
load_env_stack "$ROOT"
if [[ ! -f "$ROOT/.env.live" && -f "$ROOT/.env.live.example" ]]; then
  load_env_local "$ROOT/.env.live.example"
fi

print_key_status
echo ""
if ! require_keys; then
  exit 1
fi

print_live_config
echo ""

go run ./cmd/preflight
