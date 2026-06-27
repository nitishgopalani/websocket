#!/usr/bin/env bash
# Local readiness: brain stub, worker ports, Go /healthz — no paid API calls.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
# shellcheck disable=SC1091
source "$ROOT/scripts/load_env.sh"
load_env_stack "$ROOT"

GO_HTTP="${GO_HEALTH_URL:-http://127.0.0.1:8080/healthz}"
BRAIN_HTTP="${BRAIN_HEALTH_URL:-http://127.0.0.1:8000/healthz}"

declare -A STATUS
declare -A DETAIL
FAIL=0

port_open() {
  local port="$1"
  ss -ltn 2>/dev/null | grep -q ":${port} " || (echo >/dev/tcp/127.0.0.1/"$port") 2>/dev/null
}

env_true() {
  case "${1:-}" in
    1 | true | TRUE | yes | YES) return 0 ;;
    *) return 1 ;;
  esac
}

# Brain stub
if curl -sf --max-time 3 "$BRAIN_HTTP" 2>/dev/null | python3 -c '
import json, sys
try:
    d = json.load(sys.stdin)
    ok = d.get("llm_stub_mode") is True and d.get("stub_mode") is True
    print("stub_mode=%s llm_stub_mode=%s" % (d.get("stub_mode"), d.get("llm_stub_mode")))
    sys.exit(0 if ok else 1)
except Exception as e:
    print("parse error:", e)
    sys.exit(1)
' > /tmp/brain_health_detail 2>&1; then
  STATUS[brain]=PASS
  DETAIL[brain]="$(tr -d '\n' < /tmp/brain_health_detail)"
else
  STATUS[brain]=FAIL
  DETAIL[brain]="not stub or unreachable at $BRAIN_HTTP"
  FAIL=1
fi

# Denoise worker (skip when passthrough)
if env_true "${DENOISE_ENABLED:-false}"; then
  if port_open 9091 && grep -q 'denoise worker listening' "$ROOT/scripts/workers.log" 2>/dev/null; then
    STATUS[denoise]=PASS
    DETAIL[denoise]="9091 listening"
  else
    STATUS[denoise]=FAIL
    DETAIL[denoise]="9091 not ready"
    FAIL=1
  fi
else
  STATUS[denoise]=SKIP
  DETAIL[denoise]="DENOISE_ENABLED=false (passthrough)"
fi

# Semantic turn worker
if env_true "${SEMANTIC_TURN_ENABLED:-true}"; then
  if port_open 9093 && grep -q 'semantic turn worker listening' "$ROOT/scripts/workers.log" 2>/dev/null; then
    STATUS[semantic_turn]=PASS
    DETAIL[semantic_turn]="9093 listening"
  else
    STATUS[semantic_turn]=FAIL
    DETAIL[semantic_turn]="9093 not ready"
    FAIL=1
  fi
else
  STATUS[semantic_turn]=SKIP
  DETAIL[semantic_turn]="SEMANTIC_TURN_ENABLED=false"
fi

# AMD worker (only when enabled)
if env_true "${AMD_ENABLED:-false}"; then
  if port_open 9092 && grep -q 'amd worker listening' "$ROOT/scripts/workers.log" 2>/dev/null; then
    STATUS[amd]=PASS
    DETAIL[amd]="9092 listening"
  else
    STATUS[amd]=FAIL
    DETAIL[amd]="9092 not ready"
    FAIL=1
  fi
else
  STATUS[amd]=SKIP
  DETAIL[amd]="AMD_ENABLED=false"
fi

# Go media server
if body="$(curl -sf --max-time 3 "$GO_HTTP" 2>/dev/null)" && [[ "$body" == "ok" ]]; then
  STATUS[go_server]=PASS
  DETAIL[go_server]="$GO_HTTP -> ok"
else
  STATUS[go_server]=FAIL
  DETAIL[go_server]="$GO_HTTP not ok"
  FAIL=1
fi

echo "======== LOCAL PREFLIGHT READINESS ========"
printf "%-18s %-6s %s\n" "CHECK" "STATUS" "DETAIL"
printf "%-18s %-6s %s\n" "-----" "------" "------"
for name in brain denoise semantic_turn amd go_server; do
  printf "%-18s %-6s %s\n" "$name" "${STATUS[$name]:-?}" "${DETAIL[$name]:-}"
done
echo ""

if (( FAIL == 0 )); then
  echo "ALL GREEN — ready for live callflow"
  exit 0
fi
echo "NOT READY — fix failures above before callflow"
exit 1
