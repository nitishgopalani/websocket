#!/usr/bin/env bash
# Preflight for docker-compose stack (no paid API calls unless live preflight invoked separately).
set -euo pipefail

DEPLOY="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DEPLOY"
ENV_FILE="${COMPOSE_ENV_FILE:-.env}"

FAIL=0
check() {
  local name="$1" status="$2" detail="$3"
  printf "%-18s %-6s %s\n" "$name" "$status" "$detail"
  [[ "$status" == PASS ]] || FAIL=1
}
skip() {
  printf "%-18s %-6s %s\n" "$1" "SKIP" "$2"
}

echo "======== COMPOSE PREFLIGHT ========"
printf "%-18s %-6s %s\n" "CHECK" "STATUS" "DETAIL"
printf "%-18s %-6s %s\n" "-----" "------" "------"

# go-server
if docker compose --env-file "$ENV_FILE" exec -T go-server curl -sf http://127.0.0.1:8080/healthz 2>/dev/null | grep -qx ok; then
  check go_server PASS "8080/healthz ok"
else
  if curl -sf --max-time 3 http://127.0.0.1:8080/healthz 2>/dev/null | grep -qx ok; then
    check go_server PASS "host:8080/healthz ok"
  else
    check go_server FAIL "8080 not ok"
  fi
fi

# brain stub
if docker compose --env-file "$ENV_FILE" exec -T brain curl -sf http://127.0.0.1:8000/healthz 2>/dev/null | python3 -c '
import json,sys
d=json.load(sys.stdin)
sys.exit(0 if d.get("stub_mode") and d.get("llm_stub_mode") else 1)
' 2>/dev/null; then
  check brain PASS "stub_mode=true"
else
  check brain FAIL "brain healthz"
fi

# semantic-turn TCP
if docker compose --env-file "$ENV_FILE" exec -T semantic-turn python3 -c "import socket;s=socket.create_connection(('127.0.0.1',9093),3);s.close()" 2>/dev/null; then
  check semantic_turn PASS "9093 listening"
else
  check semantic_turn FAIL "9093 not ready"
fi

# redis
if docker compose --env-file "$ENV_FILE" exec -T redis redis-cli ping 2>/dev/null | grep -qx PONG; then
  check redis PASS "PONG"
else
  check redis FAIL "redis ping"
fi

# optional workers (profile full)
if docker compose --env-file "$ENV_FILE" ps denoise 2>/dev/null | grep -q Up; then
  if docker compose --env-file "$ENV_FILE" exec -T denoise python3 -c "import socket;s=socket.create_connection(('127.0.0.1',9091),3);s.close()" 2>/dev/null; then
    check denoise PASS "9091 listening"
  else
    check denoise FAIL "9091 not ready"
  fi
else
  skip denoise "profile full not active"
fi

if docker compose --env-file "$ENV_FILE" ps amd 2>/dev/null | grep -q Up; then
  if docker compose --env-file "$ENV_FILE" exec -T amd python3 -c "import socket;s=socket.create_connection(('127.0.0.1',9092),3);s.close()" 2>/dev/null; then
    check amd PASS "9092 listening"
  else
    check amd FAIL "9092 not ready"
  fi
else
  skip amd "profile full not active"
fi

echo ""
docker compose --env-file "$ENV_FILE" ps
echo ""

if (( FAIL == 0 )); then
  echo "ALL GREEN — stack ready"
  echo "Asterisk WS: ws://<server-ip>:8080/stream"
  exit 0
fi
echo "NOT READY — see failures above"
exit 1
