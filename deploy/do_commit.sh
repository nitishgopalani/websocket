#!/usr/bin/env bash
set -euo pipefail
cd /mnt/c/Users/nitis/source/repos/Main/Websocket
git add deploy/ .dockerignore
git add deploy/cmd/smoke/main.go
git status --short deploy/ .dockerignore
git commit -m 'deploy: offline image bundle + jump-host runbook + load_and_run script (manual deploy)'
