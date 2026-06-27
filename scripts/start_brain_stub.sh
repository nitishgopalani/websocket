#!/usr/bin/env bash
set -euo pipefail
pkill -f 'uvicorn app.main' 2>/dev/null || true
sleep 1
cd /mnt/c/Users/nitis/source/repos/Main/Collection
source .venv/bin/activate
export STUB_MODE=true
export LLM_STUB=true
nohup uvicorn app.main:app --host 0.0.0.0 --port 8000 > /tmp/brain.log 2>&1 &
sleep 3
curl -sf http://127.0.0.1:8000/healthz | grep -o '"llm_stub_mode":[^,]*'
