#!/usr/bin/env bash
# Load .env.local and run the Go media server.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ENV_FILE="$ROOT/.env.local"
if [[ -f "$ENV_FILE" ]] || [[ -f "$ROOT/.env" ]] || [[ -f "$ROOT/.env.live" ]]; then
  echo "Loading env stack (.env / .env.local / .env.live)"
  # shellcheck disable=SC1091
  source "$ROOT/scripts/load_env.sh"
  load_env_stack "$ROOT"
else
  echo "WARN: $ENV_FILE not found — using process environment only"
fi

exec go run ./cmd/server
