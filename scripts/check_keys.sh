#!/usr/bin/env bash
# Report set/empty only — never print key values.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck disable=SC1091
source "$ROOT/scripts/load_env.sh"
load_env_stack "$ROOT"
if [[ ! -f "$ROOT/.env.live" && -f "$ROOT/.env.live.example" ]]; then
  load_env_local "$ROOT/.env.live.example"
fi
print_key_status
for k in SARVAM_API_KEY ELEVENLABS_API_KEY; do
  line=$(grep -m1 "^${k}=" "$ROOT/.env.local" 2>/dev/null || true)
  if [[ -n "$line" ]]; then
    val="${line#*=}"
    val="${val//$'\r'/}"
    echo "  ${k} file_len=${#val}"
  fi
done
