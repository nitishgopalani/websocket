#!/usr/bin/env bash
# Ensure a real-speech ulaw fixture exists (not espeak). Prefer human_real.* from user.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CALLS="$ROOT/testdata/calls"
mkdir -p "$CALLS"

resolve_real() {
  local f
  for f in "$CALLS"/human_real.ulaw "$CALLS"/human_real.wav "$CALLS"/human_real.*.ulaw; do
    [[ -f "$f" ]] || continue
    echo "$f"
    return 0
  done
  return 1
}

to_ulaw() {
  local src="$1" dst="$2"
  ffmpeg -y -loglevel error -i "$src" -ar 8000 -ac 1 -f mulaw "$dst"
}

if real=$(resolve_real); then
  if [[ "$real" == *.ulaw ]]; then
    echo "$real"
    exit 0
  fi
  out="$CALLS/human_real_converted.ulaw"
  to_ulaw "$real" "$out"
  echo "$out"
  exit 0
fi

BUNDLED="$CALLS/human_bundled.ulaw"
if [[ -f "$BUNDLED" ]]; then
  echo "$BUNDLED"
  exit 0
fi

WAV="$CALLS/human_bundled.wav"
TEXT="Hello, yes I am here. I can talk about my payment today."

if command -v piper >/dev/null 2>&1; then
  piper --model en_US-lessac-medium --output_file "$WAV" <<<"$TEXT" 2>/dev/null || \
    echo "$TEXT" | piper --model en_US-lessac-medium --output_file "$WAV"
elif command -v piper-tts >/dev/null 2>&1; then
  echo "$TEXT" | piper-tts --model en_US-lessac-medium --output_file "$WAV"
else
  echo "FAIL: no human_real.* under testdata/calls/ and piper not installed." >&2
  echo "  Drop testdata/calls/human_real.ulaw (8 kHz mono recorded hello), OR install piper:" >&2
  echo "    sudo apt install piper-tts   # or download a voice model for piper" >&2
  exit 1
fi

to_ulaw "$WAV" "$BUNDLED"
echo "$BUNDLED"
