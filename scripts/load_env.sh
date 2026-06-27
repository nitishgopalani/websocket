# Load env files with CRLF-safe parsing. Never echo values.
load_env_local() {
  local env_file="${1:-.env.local}"
  if [[ ! -f "$env_file" ]]; then
    return 0
  fi
  set -a
  while IFS= read -r line || [[ -n "$line" ]]; do
    line="${line%$'\r'}"
    line="${line//$'\r'/}"
    [[ -z "$line" || "$line" =~ ^[[:space:]]*# ]] && continue
    local key="${line%%=*}"
    local val="${line#*=}"
    [[ -z "$key" ]] && continue
    export "${key}=${val}"
  done <"$env_file"
  set +a
}

# Stack order: .env (secrets) → .env.local → .env.live (live overrides).
load_env_stack() {
  local root="${1:-.}"
  local f
  for f in "$root/.env" "$root/.env.local" "$root/.env.live"; do
    if [[ -f "$f" ]]; then
      load_env_local "$f"
    fi
  done
}

key_status() {
  local name="$1"
  if [[ -n "${!name:-}" ]]; then
    echo "set"
  else
    echo "empty"
  fi
}

print_key_status() {
  echo "SARVAM_API_KEY: $(key_status SARVAM_API_KEY)"
  echo "ELEVENLABS_API_KEY: $(key_status ELEVENLABS_API_KEY)"
}

require_keys() {
  local missing=0
  if [[ -z "${SARVAM_API_KEY:-}" ]]; then
    echo "FAIL: SARVAM_API_KEY is empty — add it to .env (never commit)"
    missing=1
  fi
  if [[ -z "${ELEVENLABS_API_KEY:-}" ]]; then
    echo "FAIL: ELEVENLABS_API_KEY is empty — add it to .env (never commit)"
    missing=1
  fi
  if [[ -n "$missing" ]]; then
    return 1
  fi
  return 0
}

mask_env_value() {
  local val="${1:-}"
  if [[ -z "$val" ]]; then
    echo "(empty)"
  elif [[ "$val" == "true" || "$val" == "false" || "$val" =~ ^[0-9]+$ || "$val" =~ ^:[0-9]+$ || "$val" =~ ^wss?:// ]]; then
    echo "$val"
  else
    echo "***"
  fi
}

print_live_config() {
  echo "=== Effective live config (secrets masked) ==="
  local vars=(
    ASR_ENABLED TTS_ENABLED BRAIN_WS_ENABLED BRAIN_WS_URL
    AMD_ENABLED DENOISE_ENABLED SEMANTIC_TURN_ENABLED
    CARRIER EGRESS_PACING METRICS_ENABLED LISTEN_ADDR
    AMD_TIMEOUT_MS WHISPER_MODEL SARVAM_ENDPOINT ELEVENLABS_VOICE_ID
  )
  local v
  for v in "${vars[@]}"; do
    printf "  %s=%s\n" "$v" "$(mask_env_value "${!v:-}")"
  done
  echo "  SARVAM_API_KEY=$(key_status SARVAM_API_KEY)"
  echo "  ELEVENLABS_API_KEY=$(key_status ELEVENLABS_API_KEY)"
}
