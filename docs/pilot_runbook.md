# Live Pilot Dry-Run Runbook (CT-14)

Manual validation on Fonada staging media before production traffic.

## Prerequisites

| Component | Requirement |
|-----------|-------------|
| Go data-plane | This server listening on `LISTEN_ADDR` (default `:8080`), WS path `/stream` |
| Denoise worker | Running (CT-3) |
| AMD worker | Whisper-small worker reachable (`AMD_ADDR` or `AMD_SOCKET`) |
| Semantic turn | Optional (`SEMANTIC_TURN_ENABLED=1`) |
| Sarvam ASR | `ASR_ENABLED=1`, `SARVAM_API_KEY` set |
| ElevenLabs TTS | `TTS_ENABLED=1`, `ELEVENLABS_API_KEY` set |
| Brain EB-6 | `BRAIN_WS_ENABLED=1`, `BRAIN_WS_URL` reachable |
| Fonada staging | Bidirectional media WS pointed at `wss://<host>/stream` |
| Test number | One human-answered and one voicemail-answered line |

## Pilot environment (suggested defaults)

```bash
# Carrier + AMD
export CARRIER=fonada
export AMD_ENABLED=1
export AMD_WINDOW_MS=2000
export AMD_PROBA_HUMAN_THRESHOLD=0.4
export VOICEMAIL_ACTION=hangup   # or leave_message for compliant VM drop

# ASR / TTS / Brain
export ASR_ENABLED=1
export TTS_ENABLED=1
export BRAIN_WS_ENABLED=1
export BRAIN_WS_URL=wss://brain-staging.example/ws

# Endpointing (per-flow silence ms)
export SILENCE_MS_YES_NO=400
export SILENCE_MS_DEFAULT=600
export SILENCE_MS_SPELLED=1200

# Barge-in + egress
export BARGEIN_ENABLED=1
export BARGEIN_CLASSIFY_TIMEOUT_MS=300
export EGRESS_JITTER_MS=300
export EGRESS_PACING=realtime

# Semantic turn (optional)
export SEMANTIC_TURN_ENABLED=0
export SEMANTIC_COMPLETE_SILENCE_MS=200

# Observability
export METRICS_ENABLED=1
export MOUTH_TO_EAR_TARGET_MS=1200
```

## Test A — Human-answered call

1. Place call to test number; Fonada connects WS → `connected` → `start`.
2. **Verify AMD human:** logs show `amd human`; opener plays **after** human detection (not during ring/VM greeting).
3. **Verify loop:** ASR partials/finals in logs; brain replies; TTS audible on carrier.
4. **Barge-in:** interrupt mid-reply; agent audio stops within ~300 ms (`BARGEIN_CLASSIFY_TIMEOUT_MS`).
5. **Hangup:** carrier `stop`; session closes cleanly; no goroutine leak.

## Test B — Voicemail-answered call

1. Place call that hits voicemail/IVR greeting.
2. **Verify AMD machine:** logs show `amd machine`; `media_amd_machine_total` increments.
3. **Verify branch:** with `VOICEMAIL_ACTION=hangup`, no opener, no ASR/engine conversation, session closes.
4. With `VOICEMAIL_ACTION=leave_message`, one compliant `VOICEMAIL_MESSAGE` line plays, mark echo, then hangup.

## Observe

| Signal | Where |
|--------|--------|
| Mouth-to-ear p50/p95 | `GET /metrics` → `media_mouth_to_ear_ms` vs 1200 ms target |
| Fallbacks / barge-ins | `media_fallbacks_total`, `media_bargeins_committed_total` |
| AMD outcomes | `media_amd_human_total`, `media_amd_machine_total` |
| Per-turn timing | Structured `turn timing complete` JSON logs |

### Live eval (optional)

```bash
export RUN_LIVE_EVAL=1
# Record calls under testdata/calls/ with .ref.txt transcripts
go test ./internal/media/sim/ -run TestLiveEvalGated -v
```

Produces WER, AMD accuracy, latency percentiles report.

## AEC decision

During Test A, watch for:

- ASR finals containing the bot's own TTS text
- Self-triggered barge-in without caller speech
- Echo/delay artifacts on transcripts

| Observation | Action |
|-------------|--------|
| No echo / self-trigger | **Keep AEC stubbed** (`NoopAEC`); record decision in pilot notes |
| Clear echo or self-ASR | Enable WebRTC AEC3 in denoise worker; re-run Test A |

## Tuning guide

| Symptom | Knob |
|---------|------|
| Endpoints too eager (cuts caller off) | Increase `SILENCE_MS_*` |
| Endpoints too slow | Decrease `SILENCE_MS_*` or enable semantic turn |
| Barge-in false fires | Increase `BARGEIN_CLASSIFY_TIMEOUT_MS`; tune backchannel |
| AMD human→machine errors | Adjust `AMD_PROBA_HUMAN_THRESHOLD`, `AMD_WINDOW_MS` |
| Outbound choppy / bursty | Tune `EGRESS_JITTER_MS`; try `EGRESS_PACING=burst` for debug only |
| Mouth-to-ear high | Check brain/TTS latency; `FALLBACK_NO_AUDIO_MS`, dead-air watchdog logs |

## Exit criteria

- [ ] One clean human call end-to-end (opener → turn → reply → barge-in → hangup)
- [ ] One correct voicemail branch (hangup or leave_message)
- [ ] Mouth-to-ear p50 within 1200 ms target on test calls
- [ ] AEC decision recorded (stub vs enable)
- [ ] `/metrics` and timing logs reviewed; knobs documented for production

## Replay tool (debug)

Against a running server:

```bash
go run ./cmd/replay -addr ws://localhost:8080/stream -in testdata/smoke.ulaw -pace fast
```
