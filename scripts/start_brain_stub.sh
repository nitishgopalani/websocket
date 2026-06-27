#!/usr/bin/env bash
# Start Collection brain in stub mode (no Vertex creds). Poll healthz until confirmed.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COLLECTION="$(cd "$ROOT/../Collection" && pwd)"
BRAIN_HTTP="${BRAIN_HEALTH_URL:-http://127.0.0.1:8000/healthz}"
BRAIN_LOG="$ROOT/scripts/brain.log"

brain_stub_ok() {
  curl -sf --max-time 3 "$BRAIN_HTTP" 2>/dev/null | python3 -c '
import json, sys
try:
    d = json.load(sys.stdin)
    ok = d.get("llm_stub_mode") is True and d.get("stub_mode") is True
    sys.exit(0 if ok else 1)
except Exception:
    sys.exit(1)
'
}

pkill -f 'uvicorn app.main:app' 2>/dev/null || true
sleep 1

if [[ ! -d "$COLLECTION/.venv" ]]; then
  echo "Brain stub health: FAIL — Collection venv missing at $COLLECTION/.venv"
  exit 1
fi

cd "$COLLECTION"
# shellcheck disable=SC1091
source .venv/bin/activate
# Force stub over Collection/.env (may set LLM_STUB=false).
# Force full stub stack over Collection/.env (may set LLM_STUB/KB_STUB=false).
nohup env STUB_MODE=true LLM_STUB=true KB_STUB=true TOOLS_MODE=stub \
  uvicorn app.main:app --host 0.0.0.0 --port 8000 \
  >>"$BRAIN_LOG" 2>&1 &
brain_pid=$!
deactivate 2>/dev/null || true
echo "Brain stub starting (pid $brain_pid) — log: $BRAIN_LOG"

deadline=$((SECONDS + 60))
while (( SECONDS < deadline )); do
  if brain_stub_ok; then
    echo "Brain stub health: PASS ($BRAIN_HTTP stub_mode=true llm_stub_mode=true)"
    exit 0
  fi
  sleep 2
done

echo "Brain stub health: FAIL — stub mode not confirmed after 60s"
echo "Last 25 lines of $BRAIN_LOG:"
tail -25 "$BRAIN_LOG" 2>/dev/null || true
exit 1
