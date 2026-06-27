# Dinesh Asterisk Binary-PCM16 Protocol

Edge adapter for Dinesh's Asterisk WebSocket integration. Selected with `CARRIER=asterisk`.
The core pipeline (ASR → turn-taking → brain → TTS) is unchanged; only CT-1 ingress and CT-10
egress framing differ from the Exotel/Fonada base64-JSON path.

## Wire format

| Direction | Frame type | Content |
|-----------|------------|---------|
| Carrier → Go | TEXT JSON | `session_start`, `session_end` |
| Carrier → Go | BINARY | Raw PCM16 LE caller audio @ `input_sample_rate` (default 16 kHz) |
| Go → Carrier | TEXT JSON | `ready`, `end_of_call`, `error` |
| Go → Carrier | BINARY | Raw PCM16 LE TTS audio @ `output_sample_rate` (default 24 kHz) |

### `session_start` (carrier → Go)

```json
{
  "type": "session_start",
  "session_id": "...",
  "client_id": "...",
  "customer_phone": "...",
  "business_phone": "...",
  "audio": {
    "codec": "pcm16",
    "input_sample_rate": 16000,
    "output_sample_rate": 24000,
    "channels": 1
  },
  "metadata": {
    "language": "en-IN",
    "agent_id": "..."
  }
}
```

Mapped to internal session:

| Dinesh field | Internal |
|--------------|----------|
| `session_id` | `stream_sid` |
| `customer_phone` | `call_sid` / borrower context |
| `metadata.language` | `custom_parameters.language`, `asr_language` |
| `metadata.agent_id` | `custom_parameters.agent_id` |
| `audio.input_sample_rate` | `media_format.sample_rate` (PCM16 / `audio/x-l16`) |
| `audio.output_sample_rate` | `custom_parameters.output_sample_rate` |

Go responds with `{"type":"ready"}` after the session is created.

### Audio

- **Ingress:** BINARY frames are fed directly into the transcode/ASR chain (no base64, no JSON).
- **Egress:** TTS uses `pcm_24000` (ElevenLabs) and is sent as BINARY WS frames.

With `CARRIER=asterisk`, `TARGET_SAMPLE_RATE` defaults to **16000** when unset (Sarvam / workers
prefer 16 kHz). Set `TTS_OUTPUT_FORMAT=pcm_24000` automatically when unset.

## Playback completion (no mark/clear)

Exotel/Fonada use `mark` echo to know when playback finished. This protocol has **no mark/clear**.

- Playback-complete is derived locally: after all TTS frames are paced out, the pipeline fires
  `OnPlaybackComplete` without waiting for a carrier echo.
- `end_of_call` is sent when the brain sets `done.end_call=true` (or on AMD/voicemail hangup).

## Barge-in gap

There is **no flush/clear** command on this protocol.

On barge-in commit we:

1. Pause/stop sending new binary frames from our egress pacer
2. Cancel in-flight TTS and brain turn (CT-11)
3. Log a **WARN**: buffered audio already sent to Dinesh's edge may still play
   (`BARGEIN_FLUSH_SUPPORTED=false`)

**Open question for Dinesh:** Is there an interrupt/flush control frame we should send when the
caller barges in? A seam exists in `AsteriskSerializer.Clear` (currently no-op) to add one later.

## Local testing

```bash
CARRIER=asterisk go test ./internal/media/sim/... -run TestAsteriskProtocolSmoke -count=1
```

Uses `sim.AsteriskSimulator`: sends `session_start`, binary PCM16, expects `ready` + binary TTS
back (fake ASR/brain/TTS — no paid APIs).

## Config summary

| Env | Asterisk default |
|-----|------------------|
| `CARRIER` | `asterisk` |
| `TARGET_SAMPLE_RATE` | `16000` |
| `TTS_OUTPUT_FORMAT` | `pcm_24000` |

`CARRIER=fonada` or `exotel` keeps the legacy JSON/base64 path unchanged.
