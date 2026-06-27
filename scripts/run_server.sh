#!/usr/bin/env bash
# Load .env.local and run the Go media server.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ENV_FILE="$ROOT/.env.local"
if [[ -f "$ENV_FILE" ]]; then
  echo "Loading $ENV_FILE"
  # shellcheck disable=SC1091
  source "$ROOT/scripts/load_env.sh"
  set -a
  load_env_local "$ENV_FILE"
  set +a
else
  echo "WARN: $ENV_FILE not found — using process environment only"
fi

exec go run ./cmd/server
