#!/usr/bin/env bash
# Idempotent WSL2/Ubuntu setup for all three Python workers.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

need_python() {
  if ! command -v python3 >/dev/null 2>&1; then
    echo "ERROR: python3 not found" >&2
    exit 1
  fi
  local ver
  ver="$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
  local major minor
  major="${ver%%.*}"
  minor="${ver#*.}"
  if (( major < 3 || (major == 3 && minor < 10) )); then
    echo "ERROR: python3.10+ required, found $ver" >&2
    exit 1
  fi
  echo "python3 version: $ver"
}

# deepfilterlib (DeepFilterNet) builds a Rust extension — need cargo on first install.
ensure_rust() {
  if command -v cargo >/dev/null 2>&1; then
    echo "rust/cargo: $(cargo --version)"
    return 0
  fi
  echo "=== Installing Rust + build tools (required for deepfilterlib) ==="
  if command -v apt-get >/dev/null 2>&1; then
    DEBIAN_FRONTEND=noninteractive apt-get update -qq
    DEBIAN_FRONTEND=noninteractive apt-get install -y -qq curl build-essential pkg-config
  fi
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable
  # shellcheck disable=SC1091
  source "${HOME}/.cargo/env"
  echo "rust/cargo: $(cargo --version)"
}

setup_worker() {
  local name="$1"
  shift
  local dir="$ROOT/workers/$name"
  echo ""
  echo "=== Setting up worker: $name ==="
  if [[ ! -d "$dir" ]]; then
    echo "ERROR: missing $dir" >&2
    exit 1
  fi
  python3 -m venv "$dir/.venv"
  # shellcheck disable=SC1091
  source "$dir/.venv/bin/activate"
  pip install -U pip wheel
  "$@"
  deactivate
}

need_python
ensure_rust
# shellcheck disable=SC1091
[[ -f "${HOME}/.cargo/env" ]] && source "${HOME}/.cargo/env"

setup_worker denoise \
  pip install -r "$ROOT/workers/denoise/requirements.txt" \
    --index-url https://download.pytorch.org/whl/cpu \
    --extra-index-url https://pypi.org/simple

setup_worker amd \
  pip install -r "$ROOT/workers/amd/requirements.txt"

echo ""
echo "=== Pre-caching faster-whisper small (CPU int8 for build-time cache) ==="
# shellcheck disable=SC1091
source "$ROOT/workers/amd/.venv/bin/activate"
python -c "from faster_whisper import WhisperModel; WhisperModel('small', device='cpu', compute_type='int8')"
deactivate
echo "NOTE: For GPU at runtime set WHISPER_DEVICE=cuda in run_workers.sh."
echo "      Requires nvidia-smi working in WSL2 and CUDA/cuDNN for CTranslate2."

setup_worker semantic_turn \
  pip install -r "$ROOT/workers/semantic_turn/requirements.txt"

ST_DIR="$ROOT/workers/semantic_turn"
ONNX_V31="$ST_DIR/smart-turn-v3.1.onnx"
ONNX_V30="$ST_DIR/smart-turn-v3.0.onnx"
if [[ -f "$ONNX_V31" ]]; then
  echo "smart-turn ONNX already present: $ONNX_V31"
elif [[ -f "$ONNX_V30" ]]; then
  echo "smart-turn ONNX already present: $ONNX_V30"
else
  echo "=== Downloading smart-turn-v3.0.onnx from HuggingFace ==="
  if command -v curl >/dev/null 2>&1; then
    curl -L -o "$ONNX_V30" \
      "https://huggingface.co/pipecat-ai/smart-turn-v3/resolve/main/smart-turn-v3.0.onnx"
  elif command -v wget >/dev/null 2>&1; then
    wget -O "$ONNX_V30" \
      "https://huggingface.co/pipecat-ai/smart-turn-v3/resolve/main/smart-turn-v3.0.onnx"
  else
    echo "WARN: curl/wget not found; download ONNX manually (see workers/semantic_turn/README.md)"
  fi
fi

if [[ -f "$ROOT/workers/requirements-silero.txt" && "${SILERO_SETUP:-0}" == "1" ]]; then
  echo ""
  echo "=== Optional: Silero VAD (set SILERO_SETUP=1; skipped by default) ==="
  SILERO_VENV="$ROOT/workers/.venv-silero"
  python3 -m venv "$SILERO_VENV"
  # shellcheck disable=SC1091
  source "$SILERO_VENV/bin/activate"
  pip install -U pip wheel
  pip install -r "$ROOT/workers/requirements-silero.txt"
  deactivate
  echo "Silero installed in $SILERO_VENV"
else
  echo ""
  echo "=== Optional: Silero VAD — skipped (SILERO_SETUP=0). Reuse existing ONNX e.g."
  echo "    /mnt/c/Users/nitis/source/repos/Another_testing/venv/Lib/site-packages/livekit/plugins/silero/resources/silero_vad.onnx"
fi

echo ""
echo "=== Running smoke tests ==="
PASS=0
FAIL=0
for name in denoise amd semantic_turn; do
  dir="$ROOT/workers/$name"
  echo "--- $name test_smoke.py ---"
  # shellcheck disable=SC1091
  source "$dir/.venv/bin/activate"
  if (cd "$dir" && python -m pytest test_smoke.py -q); then
    echo "PASS: $name"
    PASS=$((PASS + 1))
  else
    echo "FAIL: $name"
    FAIL=$((FAIL + 1))
  fi
  deactivate
done

echo ""
echo "Setup complete: $PASS passed, $FAIL failed"
if (( FAIL > 0 )); then
  exit 1
fi
