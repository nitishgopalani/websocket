# Semantic Turn (EOU) Worker — smart-turn-v3 ONNX

Out-of-process end-of-utterance classifier for CT-7. Uses **audio-based** smart-turn-v3 ONNX inference with transcript heuristic fallback when weights are absent.

## Wire protocol

Length-prefixed JSON (matches Go `internal/media/semantic_turn.go`):

**Request:** `[uint32 len LE][json]`

```json
{"transcript":"I want to pay my bill","rate_hz":8000,"audio_b64":"<optional pcm16 mono base64>"}
```

**Response:** `[uint32 len LE][json]`

```json
{"complete":true,"confidence":0.92}
```

## Model download

Download the ONNX file from [pipecat-ai/smart-turn-v3](https://huggingface.co/pipecat-ai/smart-turn-v3) into this directory:

```bash
cd workers/semantic_turn
curl -L -o smart-turn-v3.0.onnx \
  https://huggingface.co/pipecat-ai/smart-turn-v3/resolve/main/smart-turn-v3.0.onnx
```

Prefer `smart-turn-v3.1.onnx` if available (auto-detected before v3.0). Or set `SMART_TURN_ONNX=/path/to/model.onnx`.

The repo setup script (`scripts/setup_workers.sh`) downloads the model automatically when missing.

## Setup (WSL2)

```bash
# From Websocket/ — see docs/SETUP_LOCAL.md for full runbook
bash scripts/setup_workers.sh
```

## Run

```bash
source .venv/bin/activate   # or workers/semantic_turn/.venv after setup_workers.sh
python server.py --addr 127.0.0.1:9093
```

Go server env:

| Variable | Example |
|----------|---------|
| `SEMANTIC_TURN_ENABLED` | `true` |
| `SEMANTIC_TURN_ADDR` | `127.0.0.1:9093` |
| `SEMANTIC_TURN_TIMEOUT_MS` | `500` (local; default 100 ms is tight) |

## Smoke test

```bash
python test_smoke.py          # via pytest
pytest test_smoke.py -v
```

Protocol tests always run; model-inference tests skip when ONNX is absent.

## Audio processing

- Input: PCM16 mono at 8 kHz or 16 kHz (from `rate_hz` + decoded `audio_b64`)
- Resample 8 kHz → 16 kHz when needed
- Truncate to last 8 s (pad at beginning if shorter)
- Whisper-style log-mel features → ONNX → `{complete, confidence}`

When no audio is sent, a lightweight transcript heuristic is used (fail-safe for v1 callers).
