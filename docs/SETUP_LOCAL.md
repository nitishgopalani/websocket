# Local development setup — WSL2 + GPU laptop

Run the Go data-plane against **real Python workers** without Sarvam, ElevenLabs, or the brain WebSocket. Workers live under WSL2/Ubuntu; the Go server can run in WSL2 or native Windows (TCP `127.0.0.1` bridges both).

## Prerequisites

### WSL2 (Ubuntu 22.04+)

```bash
wsl --install   # Windows host, if not already installed
```

Inside WSL2:

- **Python 3.10+**
- **Go 1.23+** (for server/tests in WSL2) or run Go on Windows
- **CUDA (optional, for AMD GPU):** `nvidia-smi` must work inside WSL2
  - Default AMD path is **CPU int8** (reliable; CUDA 13 / cuBLAS 12 gap in WSL2)
  - Optional GPU: install `nvidia-cublas-cu12` + `nvidia-cudnn-cu12`, set `WHISPER_DEVICE=cuda`

### GPU vs CPU placement (one laptop)

| Component | Device | Notes |
|-----------|--------|-------|
| AMD (faster-whisper small) | **CPU (default)** | `WHISPER_DEVICE=cpu` in `run_workers.sh`; optional CUDA with cuBLAS 12 |
| Denoise (DeepFilterNet3) | **CPU** | Installed via PyTorch CPU wheels |
| Semantic turn (smart-turn ONNX) | **CPU** | `onnxruntime` CPU; optional `onnxruntime-gpu` |
| Silero VAD (optional) | **CPU** | `workers/requirements-silero.txt` |

## Quick start

```bash
cd Websocket

# 1) Install worker venvs, download ONNX, run smoke tests
bash scripts/setup_workers.sh

# 2) Start workers (background, ports 9091-9093)
bash scripts/run_workers.sh
tail -f scripts/workers.log

# 3) Start Go server (loads .env.local — no paid APIs)
bash scripts/run_server.sh
# Windows native Go:
#   powershell -File scripts/run_server.ps1

# 4) Replay test audio
go run ./cmd/replay -addr ws://127.0.0.1:8080/stream -in testdata/smoke.ulaw -pace fast

# 5) One-command e2e (starts workers if needed, server, replay, metrics)
bash scripts/test_pipeline_local.sh
```

Stop workers:

```bash
bash scripts/stop_workers.sh
```

## Environment (`.env.local`)

Committed template with **no secrets**. Key settings:

| Variable | Purpose |
|----------|---------|
| `DENOISE_ENABLED` / `DENOISE_ADDR` | DeepFilterNet worker @ `:9091` |
| `AMD_ENABLED` / `AMD_ADDR` | Whisper AMD @ `:9092` |
| `SEMANTIC_TURN_ENABLED` / `SEMANTIC_TURN_ADDR` | smart-turn @ `:9093` |
| `ASR_ENABLED=false` | No Sarvam |
| `TTS_ENABLED=false` | No ElevenLabs |
| `BRAIN_WS_ENABLED=false` | No EB-6 brain |
| `DENOISE_TIMEOUT_MS=500` | Local-friendly worker timeout |
| `EGRESS_PACING=burst` | Faster replay without TTS pacing |

## Go ↔ worker live test

With workers running:

```bash
WORKERS_LIVE=1 go test ./internal/media -run TestWorkersLiveIntegration -v
```

Skipped in CI (`WORKERS_LIVE` unset). Exercises:

- `RemoteDenoiser.Process` — same-length PCM16, zero fallbacks
- `RemoteAMDClassifier.Classify` — human/machine decision on ~2 s sample
- `RemoteSemanticTurn.Predict` — `{complete, confidence}` JSON

## L-9 AMD sign-off (real recordings)

Synthetic espeak samples (`human_synthetic.ulaw`, `voicemail_synthetic.ulaw`) check plumbing only.
**Real L-9 sign-off** uses recorded call audio:

1. Record or export two **8 kHz mono** samples (μ-law `.ulaw` preferred; `.wav` OK if you convert with ffmpeg):

   | File | Content |
   |------|---------|
   | `testdata/calls/human_real.ulaw` | Live “hello?” pickup (first ~2 s used for AMD) |
   | `testdata/calls/voicemail_real.ulaw` | Voicemail greeting (“not available… leave a message after the tone”) |

   Convert WAV → μ-law:

   ```bash
   ffmpeg -y -i your_recording.wav -ar 8000 -ac 1 -f mulaw testdata/calls/human_real.ulaw
   ```

2. Start workers and replay (no paid APIs):

   ```bash
   bash scripts/replay_amd_l9.sh
   ```

   Or manually:

   ```bash
   bash scripts/run_workers.sh
   bash scripts/run_server.sh   # separate terminal; uses .env.local
   go run ./cmd/replay -addr ws://127.0.0.1:8080/stream -in testdata/calls/human_real.ulaw -pace fast
   go run ./cmd/replay -addr ws://127.0.0.1:8080/stream -in testdata/calls/voicemail_real.ulaw -pace fast
   tail -f scripts/workers.log | grep 'amd classify'
   ```

3. Expect **human** vs **machine** in `scripts/workers.log` (`amd classify transcript=... result=...`).
   Empty transcript → fail-open human (check audio level / codec).

## Manual AMD check (synthetic or real)

Validate AMD **without any paid API**:

1. Place samples under `testdata/calls/` (see L-9 table above, or use `human_synthetic.ulaw` / `voicemail_synthetic.ulaw`).

2. Ensure workers are up and watch AMD logs:

   ```bash
   bash scripts/run_workers.sh
   tail -f scripts/workers.log | grep -E 'amd classify|faster-whisper'
   ```

3. Start server with `.env.local`, replay:

   ```bash
   bash scripts/replay_amd_l9.sh
   ```

4. Confirm AMD logs **human** vs **machine** and Go metrics `media_amd_human_total` / `media_amd_machine_total`.

See also [`testdata/calls/README.md`](../testdata/calls/README.md).

## Troubleshooting

### DeepFilterNet / torch wheel issues on Windows

Run workers **only in WSL2** (`bash scripts/setup_workers.sh`). DeepFilterNet + CPU torch wheels are tested on Linux.

### faster-whisper CUDA / libcublas.so.12

- **Default:** `WHISPER_DEVICE=cpu` in `run_workers.sh` (int8, first ~2 s AMD only).
- CUDA 13.0 in WSL2 often lacks matching **cuBLAS 12** for CTranslate2 → inference fail-open.
- Optional GPU: `pip install nvidia-cublas-cu12 nvidia-cudnn-cu12`, set `LD_LIBRARY_PATH`, then `WHISPER_DEVICE=cuda` in `run_workers.sh`.
- Worker auto-falls back to CPU int8 on CUDA init failure and logs `CUDA init failed`.

### smart-turn ONNX download fails

Manual download:

```bash
curl -L -o workers/semantic_turn/smart-turn-v3.0.onnx \
  https://huggingface.co/pipecat-ai/smart-turn-v3/resolve/main/smart-turn-v3.0.onnx
```

Or set `SMART_TURN_ONNX=/path/to/model.onnx`. Protocol smoke tests pass without ONNX; inference tests skip.

### Worker connection refused from Windows Go

Ensure workers bind `127.0.0.1:909x` (default). WSL2 localhost forwarding usually maps to Windows `127.0.0.1`. If not, use WSL2 IP from `hostname -I` and update `.env.local` addresses.

### Denoise fallbacks > 0

Increase `DENOISE_TIMEOUT_MS` in `.env.local` (default local: 500 ms). Check `scripts/workers.log` for worker errors.

## Related docs

- [`IMPLEMENTATION.md`](../IMPLEMENTATION.md) — full pipeline architecture
- [`workers/semantic_turn/README.md`](../workers/semantic_turn/README.md) — EOU worker protocol
- [`workers/amd/README.md`](../workers/amd/README.md) — AMD worker
- [`workers/denoise/README.md`](../workers/denoise/README.md) — Denoise worker
