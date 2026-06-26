# DeepFilterNet3 Denoise Worker

Out-of-process denoise worker for the Go media data-plane (CT-3). The Go service sends 20 ms PCM16 mono frames; this worker denoises them and returns PCM16 of the **same length and sample rate**.

## Protocol

Length-prefixed binary over **Unix domain socket** (Linux) or **TCP** (Windows/dev):

| Direction | Format |
|-----------|--------|
| Request   | `[uint32 len LE][uint16 rate_hz LE][pcm16 bytes]` where `len = 2 + len(pcm16)` |
| Response  | `[uint32 len LE][pcm16 bytes]` |

Supported rates: **8000**, **16000** Hz mono PCM16 little-endian.

DeepFilterNet runs internally at **48 kHz**; the worker resamples in/out transparently.

## Expected latency

Target per-frame budget on the Go side: **15 ms** (`DENOISE_TIMEOUT_MS`, fail-open).

For a 20 ms frame at 8 kHz (160 samples):

- Resample 8k→48k→8k: ~1–3 ms (NumPy)
- DeepFilterNet3 inference: ~5–20 ms depending on CPU/GPU
- **Total**: aim for **< 15 ms** on a warm GPU; CPU-only may exceed budget and trigger fail-open (original audio forwarded)

## Setup

```bash
cd workers/denoise
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

First run downloads DeepFilterNet3 weights via the `deepfilternet` package.

## Run

**Linux (UDS, preferred):**

```bash
export DENOISE_SOCKET=/tmp/fonada-denoise.sock
python server.py --socket "$DENOISE_SOCKET"
```

**Windows / TCP fallback:**

```bash
python server.py --addr 127.0.0.1:9091
```

## Point Go at the worker

```bash
export DENOISE_ENABLED=true
export DENOISE_SOCKET=/tmp/fonada-denoise.sock   # Linux
# or
export DENOISE_ADDR=127.0.0.1:9091               # TCP
export DENOISE_TIMEOUT_MS=15
```

With `DENOISE_ENABLED=false` (default), Go uses `NoopDenoiser` and no worker is required.

## Tests

```bash
pytest test_smoke.py -v
```

The DeepFilterNet inference test is skipped when model weights are not present (typical CI).

## Swapping denoisers

Go depends only on this wire protocol via `media.Denoiser`. Replace the Python worker with a FonadaLabs in-house service by implementing the same frame format; no Go code changes beyond config.
