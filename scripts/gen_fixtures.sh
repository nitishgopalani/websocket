#!/usr/bin/env bash
# Generate 8 kHz mono ulaw call fixtures via ElevenLabs TTS (uses ELEVENLABS_API_KEY from env).
# Never prints key values.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
# shellcheck disable=SC1091
source "$ROOT/scripts/load_env.sh"

CALLS="$ROOT/testdata/calls"
mkdir -p "$CALLS"

load_env_stack "$ROOT"

echo "=== ElevenLabs fixture generation ==="
print_key_status

if [[ -z "${ELEVENLABS_API_KEY:-}" ]]; then
  echo "FAIL: ELEVENLABS_API_KEY is empty — add to .env (never commit)"
  exit 1
fi

VOICE_ID="${ELEVENLABS_VOICE_ID:-21m00Tcm4TlvDq8ikWAM}"
MODEL="${ELEVENLABS_MODEL:-eleven_flash_v2_5}"
OUTPUT_FORMAT="${TTS_OUTPUT_FORMAT:-ulaw_8000}"
API_BASE="${ELEVENLABS_API_BASE:-https://api.elevenlabs.io}"

mask_err() {
  local msg="$1"
  if [[ -n "${ELEVENLABS_API_KEY:-}" ]]; then
    msg="${msg//${ELEVENLABS_API_KEY}/***}"
  fi
  echo "$msg"
}

ulaw_duration_secs() {
  local bytes
  bytes=$(wc -c <"$1")
  awk -v b="$bytes" 'BEGIN { printf "%.2f", b / 8000 }'
}

print_fixture_stats() {
  local path="$1"
  local bytes dur
  bytes=$(wc -c <"$path")
  dur=$(ulaw_duration_secs "$path")
  echo "  $path — ${bytes} bytes, ${dur}s @ 8 kHz ulaw"
}

tts_to_ulaw() {
  local out_ulaw="$1"
  local text="$2"
  local tmp_http tmp_body http_code

  tmp_http=$(mktemp)
  tmp_body=$(mktemp)

  http_code=$(curl -sS -w "%{http_code}" -o "$tmp_body" \
    -X POST "${API_BASE}/v1/text-to-speech/${VOICE_ID}?output_format=${OUTPUT_FORMAT}" \
    -H "xi-api-key: ${ELEVENLABS_API_KEY}" \
    -H "Content-Type: application/json" \
    -H "Accept: audio/*" \
    -d "{\"text\":$(python3 -c 'import json,sys; print(json.dumps(sys.argv[1]))' "$text"),\"model_id\":\"${MODEL}\"}" \
    2>"$tmp_http" || true)

  if [[ "$http_code" == "200" ]] && [[ -s "$tmp_body" ]]; then
    mv "$tmp_body" "$out_ulaw"
    rm -f "$tmp_http"
    return 0
  fi

  # Fallback: mp3 then ffmpeg -> ulaw
  local err_hint=""
  if [[ -s "$tmp_body" ]]; then
    err_hint=$(head -c 400 "$tmp_body" | tr -d '\n')
  fi
  if [[ -s "$tmp_http" ]]; then
    err_hint="${err_hint} $(head -c 200 "$tmp_http")"
  fi

  local tmp_mp3
  tmp_mp3=$(mktemp --suffix=.mp3)
  http_code=$(curl -sS -w "%{http_code}" -o "$tmp_mp3" \
    -X POST "${API_BASE}/v1/text-to-speech/${VOICE_ID}" \
    -H "xi-api-key: ${ELEVENLABS_API_KEY}" \
    -H "Content-Type: application/json" \
    -H "Accept: audio/mpeg" \
    -d "{\"text\":$(python3 -c 'import json,sys; print(json.dumps(sys.argv[1]))' "$text"),\"model_id\":\"${MODEL}\"}" \
    2>/dev/null || true)

  rm -f "$tmp_body" "$tmp_http"

  if [[ "$http_code" != "200" ]] || [[ ! -s "$tmp_mp3" ]]; then
    echo "FAIL: ElevenLabs TTS HTTP ${http_code:-???} — $(mask_err "$err_hint")" >&2
    rm -f "$tmp_mp3"
    return 1
  fi

  if ! command -v ffmpeg >/dev/null 2>&1; then
    echo "FAIL: ulaw_8000 direct TTS failed and ffmpeg not installed for mp3 fallback" >&2
    rm -f "$tmp_mp3"
    return 1
  fi

  ffmpeg -y -loglevel error -i "$tmp_mp3" -ar 8000 -ac 1 -f mulaw "$out_ulaw"
  rm -f "$tmp_mp3"
}

declare -A FIXTURES=(
  ["human_real.ulaw"]="Hello? Yes, who is this calling please?"
  ["voicemail_real.ulaw"]="The person you are trying to reach is not available. Please leave your message after the tone."
  ["human_long.ulaw"]="Hi yes this is speaking, I got your message about the payment, can you tell me how much is due and by when."
)

echo ""
echo "Voice: ${VOICE_ID}  Model: ${MODEL}  Format: ${OUTPUT_FORMAT}"
echo ""

for name in human_real.ulaw voicemail_real.ulaw human_long.ulaw; do
  text="${FIXTURES[$name]}"
  out="$CALLS/$name"
  ref="${out%.ulaw}.ref.txt"
  echo "Generating $name ..."
  if ! tts_to_ulaw "$out" "$text"; then
    exit 1
  fi
  printf '%s\n' "$text" >"$ref"
  print_fixture_stats "$out"
done

echo ""
echo "Done — ELEVENLABS-SYNTHESIZED fixtures in testdata/calls/"
