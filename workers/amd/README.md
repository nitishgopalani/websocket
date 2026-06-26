# Whisper-small AMD Worker

Self-hosted answering-machine detection for the Call Testing Pipeline (CT-5). Classifies the first ~2 seconds of outbound call audio **before** audio reaches Sarvam ASR, keeping voicemail off the paid meter.

## Protocol

Same request framing as the denoise worker; JSON response:

| Direction | Format |
|-----------|--------|
| Request   | `[uint32 len LE][uint16 rate_hz LE][pcm16 bytes]` |
| Response  | `[uint32 len LE][json]` → `{"result":"human"\|"machine","proba_human":0.9,"reason":"..."}` |

Supported rates: **8000**, **16000** Hz mono PCM16 little-endian.

## Detection logic (v1)

1. **faster-whisper `small`** (int8) transcribes the buffered window
2. Keyword/regex classifier over Hindi+English voicemail phrases (`leave a message`, `after the tone`, `uplabdh`, `sandesh`, …)
3. **Human-precision default**: `proba_human < 0.4` (configurable) required to call `machine`; otherwise `human`

Worker errors always return `human` (fail-open).

## Setup

```bash
cd workers/amd
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

First run downloads Whisper `small` weights.

## Run

**Linux (UDS):**

```bash
export AMD_SOCKET=/tmp/fonada-amd.sock
python server.py --socket "$AMD_SOCKET"
```

**Windows / TCP:**

```bash
python server.py --addr 127.0.0.1:9092
```

## Point Go at the worker

```bash
export AMD_ENABLED=true
export AMD_ADDR=127.0.0.1:9092          # or AMD_SOCKET on Linux
export AMD_WINDOW_MS=2000
export AMD_TIMEOUT_MS=500
export AMD_PROBA_HUMAN_THRESHOLD=0.4
```

With `AMD_ENABLED=false` (default), Go uses `NoopAMDClassifier` (always human) — no worker required.

## Expected latency

~200–800 ms for 2s of 8 kHz audio on CPU (Whisper small int8). Go timeout defaults to 500 ms; on timeout the gate **fail-opens to human**.

## Tests

```bash
pytest test_smoke.py -v
```

Whisper inference test skips when model weights are absent (typical CI).

## Pipeline position

```
ingress → Transcode → Denoise → AMDGate → ASRSink → TranscriptConsumer
```

Machine outcome stops forwarding audio to Sarvam; CT-12/13 handles hangup/branching.
