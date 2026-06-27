# Load .env.local with CRLF-safe parsing (Windows editors often add \r).
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
    export "$line"
  done <"$env_file"
  set +a
}
