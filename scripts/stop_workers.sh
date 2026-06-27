#!/usr/bin/env bash
# Stop background workers started by run_workers.sh.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_FILE="$ROOT/scripts/.worker_pids"

if [[ ! -f "$PID_FILE" ]]; then
  echo "No worker PID file at $PID_FILE"
  exit 0
fi

while IFS='=' read -r name pid; do
  [[ -z "${name:-}" || -z "${pid:-}" ]] && continue
  if kill -0 "$pid" 2>/dev/null; then
    echo "Stopping $name (pid $pid)"
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  else
    echo "$name (pid $pid) not running"
  fi
done < "$PID_FILE"

rm -f "$PID_FILE"
echo "Workers stopped."
