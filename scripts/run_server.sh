#!/usr/bin/env bash
# Load .env.local and run the Go media server.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ENV_FILE="$ROOT/.env.local"
if [[ -f "$ENV_FILE" ]]; then
  echo "Loading $ENV_FILE"
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
else
  echo "WARN: $ENV_FILE not found — using process environment only"
fi

exec go run ./cmd/server
