# Fonada voice go-server

Carrier media path: Asterisk / Fonada / Exotel -> denoise -> ASR -> brain WS -> TTS -> carrier.

[![CI](https://github.com/nitishgopalani/websocket/actions/workflows/ci.yml/badge.svg)](https://github.com/nitishgopalani/websocket/actions/workflows/ci.yml)

## Verify

```bash
go test ./internal/media/ ./internal/brain/ -count=1
curl -sS http://127.0.0.1:8080/version
curl -sS http://127.0.0.1:8080/healthz
```

`GET /version` is stamped at image build (`GIT_SHA` / `GIT_BRANCH` ldflags).

Deploy: `deploy/DEPLOY_RUNBOOK.md`. Drain restart: `deploy/drain_restart.sh`.
