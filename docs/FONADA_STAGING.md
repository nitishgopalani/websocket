# Fonada staging call runbook

Use this after `scripts/preflight_live.sh` and `scripts/test_live_callflow.sh` pass locally.

## Prerequisites

| Component | Check |
|-----------|--------|
| Workers | `bash scripts/run_workers.sh` — ports 9091–9093 |
| Collection brain | `cd ../Collection && STUB_MODE=true uvicorn app.main:app --host 0.0.0.0 --port 8000` |
| Go media server | `bash scripts/run_server.sh` with `.env` keys + `.env.live` |
| Secrets | `SARVAM_API_KEY` and `ELEVENLABS_API_KEY` in `.env` only (never commit) |

Copy live config:

```bash
cp .env.live.example .env.live
# Keys stay in .env — edit .env.live for BRAIN_WS_URL / ports if needed
```

## Point Fonada staging media WebSocket at this server

Fonada sends bidirectional μ-law audio over WebSocket (same shape as CT-13 replay).

1. **Expose the Go server** on a URL Fonada can reach:
   - **WSL2 local:** `ws://127.0.0.1:8080/stream` (replay/simulator only).
   - **Staging:** use your public/tunnel URL, e.g. `wss://<your-host>/stream`.

2. **Configure Fonada staging** media WebSocket URL:
   ```
   ws://<host>:8080/stream
   ```

3. **Carrier:** `CARRIER=fonada` in `.env.live`.

4. **Egress:** `EGRESS_PACING=realtime` for live calls.

## Opener gated on AMD-human (CT-14)

With `AMD_ENABLED=true`, the bot **must not speak** until AMD confirms human:

- Log: `"msg":"amd human confirmed; conversation opened"`.
- **Test A (human):** answer “hello?” → AMD human → opener TTS.
- **Test B (voicemail):** voicemail greeting → AMD machine, no conversational opener.

## What to watch

```bash
tail -f scripts/pipeline_server.log | grep -E 'amd human|amd machine|asr final|reply chunk|egress audio'
tail -f scripts/workers.log | grep 'amd classify'
curl -s http://127.0.0.1:8080/metrics | grep -E 'mouth_to_ear|turns_total|amd_human|denoise_fallback'
```

## Start order

```bash
# Terminal A — Collection brain
cd ../Collection && source .venv/bin/activate
STUB_MODE=true uvicorn app.main:app --host 0.0.0.0 --port 8000

# Terminal B — Websocket
cd Websocket
bash scripts/preflight_live.sh
bash scripts/run_workers.sh
bash scripts/run_server.sh
```

## Local verification before staging

```bash
bash scripts/preflight_live.sh
bash scripts/test_live_callflow.sh
```
