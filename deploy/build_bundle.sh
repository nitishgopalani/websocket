#!/usr/bin/env bash
# Save offline image bundle for scp to internal server. Run from deploy/ after build_images.sh.
set -euo pipefail

DEPLOY="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DEPLOY"

OUT="${1:-fonada-voice-images.tar.gz}"
IMAGES=(
  fonada/voice-go-server:latest
  fonada/voice-brain:latest
  fonada/voice-semantic-turn:latest
  fonada/voice-denoise:latest
  fonada/voice-amd:latest
  fonada/voice-smoke:latest
  redis:7-alpine
)

echo "=== docker save -> $OUT ==="
for img in "${IMAGES[@]}"; do
  if ! docker image inspect "$img" >/dev/null 2>&1; then
    echo "FAIL: missing image $img — run ./build_images.sh first"
    exit 1
  fi
done

docker save "${IMAGES[@]}" | gzip -c > "$OUT"
ls -lh "$OUT"
echo ""
echo "Bundle files to transfer to server (/opt/fonada/):"
echo "  $OUT"
echo "  docker-compose.yml"
echo "  .env.example"
echo "  load_and_run.sh"
echo "  preflight_compose.sh"
echo "  DEPLOY_RUNBOOK.md"
