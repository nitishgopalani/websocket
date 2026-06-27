# Manual deploy runbook — Fonada voice stack (offline bundle)

Cursor **does not** SSH or deploy. You build the bundle on a machine with Docker + internet,
transfer via jump host, then run scripts on the internal server by hand.

## What is in the bundle

After `bash build_bundle.sh` on your laptop, copy these files to the server (e.g. `/opt/fonada/`):

| File | Purpose |
|------|---------|
| `fonada-voice-images.tar.gz` | Pre-built Docker images (models baked in) |
| `docker-compose.yml` | Runtime stack (image tags only, no build) |
| `.env.example` | Template — copy to `.env` on server |
| `load_and_run.sh` | `docker load` + `compose up` + preflight |
| `preflight_compose.sh` | Health checks (no paid API calls) |
| `DEPLOY_RUNBOOK.md` | This document |

## Server prerequisites

- Docker Engine 24+ and **Compose v2** (`docker compose version`)
- Port **8080** reachable from Dinesh's Asterisk/Go server on the internal LAN
- **Outbound HTTPS** from the server to:
  - `api.sarvam.ai` (ASR)
  - `api.elevenlabs.io` (TTS)

> **Air-gap warning:** If the internal server has **no outbound internet**, live ASR/TTS **will not work**
> even with loaded images. You must open egress to those hosts (or run a forward proxy). The bundle
> only removes the need to download **models** at runtime — API calls still need network.

Optional GPU (commented in build Dockerfiles / `docker compose` profile `full`):

- `nvidia-container-toolkit` on host
- Uncomment GPU `deploy.resources` blocks for `amd` / `denoise` / `semantic-turn`

## 1. Build bundle (laptop — you run this)

```bash
cd Websocket/deploy
bash build_images.sh          # all images, models baked in (needs internet)
bash build_bundle.sh          # writes fonada-voice-images.tar.gz
bash test_local_packaging.sh  # optional: free local smoke (ASR/TTS off)
```

## 2. Transfer via jump host (you run this)

Confirm paths/ports with your ops team. Example pattern:

```bash
# From laptop — copy deploy/ contents to bastion, then to internal server
scp -o ProxyJump=root@1.7.48.205:9734 -P 9751 \
  fonada-voice-images.tar.gz docker-compose.yml .env.example \
  load_and_run.sh preflight_compose.sh DEPLOY_RUNBOOK.md \
  root@192.168.1.74:/opt/fonada/
```

Alternative: `scp` to bastion, then `ssh -J root@1.7.48.205:9734 -p 9751 root@192.168.1.74` and pull from bastion.

Large tarball: use `rsync -avP --partial` if the transfer drops.

## 3. Create `.env` ON THE SERVER (never copy from laptop)

```bash
ssh -J root@1.7.48.205:9734 -p 9751 root@192.168.1.74
cd /opt/fonada
cp .env.example .env
chmod 600 .env
nano .env   # paste SARVAM_API_KEY and ELEVENLABS_API_KEY only here
```

### Required variables

| Variable | Example / notes |
|----------|-----------------|
| `SARVAM_API_KEY` | Sarvam STT key |
| `ELEVENLABS_API_KEY` | ElevenLabs TTS key |
| `CARRIER` | `asterisk` |
| `BRAIN_WS_URL` | `ws://brain:8000/ws/brain` (compose DNS) |
| `SEMANTIC_TURN_ADDR` | `semantic-turn:9093` |
| `ASR_ENABLED` | `true` |
| `TTS_ENABLED` | `true` |
| `TARGET_SAMPLE_RATE` | `16000` |
| `TTS_OUTPUT_FORMAT` | `pcm_24000` |

Defaults in `.env.example` set `DENOISE_ENABLED=false`, `AMD_ENABLED=false` (CPU-friendly asterisk path).
Enable full workers: `docker compose --profile full up -d` and set `DENOISE_ENABLED=true` / `AMD_ENABLED=true`.

## 4. Load and start (on server)

```bash
cd /opt/fonada
chmod +x load_and_run.sh preflight_compose.sh
./load_and_run.sh fonada-voice-images.tar.gz
```

This runs `docker load`, `docker compose up -d`, waits for health, and runs `preflight_compose.sh`.

## 5. Verify

```bash
./preflight_compose.sh
curl -sf http://127.0.0.1:8080/healthz   # expect: ok
```

Optional Asterisk smoke (no paid APIs if `ASR_ENABLED=false` in a test `.env`):

```bash
docker compose --profile test run --rm smoke
```

Give Dinesh the internal WebSocket URL:

```text
ws://192.168.1.74:8080/stream
```

Confirm **his** Go/Asterisk server can TCP-connect to `192.168.1.74:8080` and speaks the binary-PCM16
protocol (`session_start` → binary audio → `session_end`). See `docs/DINESH_PROTOCOL.md`.

## 6. Teardown / rollback

```bash
cd /opt/fonada
docker compose down -v
# Previous tag: docker load older tar, edit compose image tags, compose up -d
docker images | grep fonada/voice
```

To revert: keep the previous `fonada-voice-images.tar.gz`, `docker compose down`, `docker load` old tar,
`docker compose up -d`.

## Architecture (compose network)

```text
Dinesh Asterisk WS ──► go-server:8080 ──► brain:8000
                          │    ├── semantic-turn:9093
                          │    ├── denoise:9091 (optional, profile full)
                          │    └── amd:9092 (optional, WHISPER_MODEL=tiny CPU)
                          └── redis:6379 (reserved)
```

## Troubleshooting

| Symptom | Check |
|---------|--------|
| `load_and_run.sh` fails on `.env` | Create `.env` on server from `.env.example` |
| go-server unhealthy | `docker compose logs go-server` — brain WS URL |
| No ASR/TTS | Outbound to Sarvam/ElevenLabs; keys in `.env` |
| Brain not stub | Should show `stub_mode=true` at `:8000/healthz` |
| AMD slow start | First boot loads tiny model from image cache (~1–2 min health start) |
