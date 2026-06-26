# Semantic Turn (EOU) Worker

Out-of-process end-of-utterance classifier for CT-7. v1 is **transcript-based**; the Go client also sends optional recent PCM (base64) for a future **smart-turn-v3 / LiveKit turn-detector** audio upgrade.

## Wire protocol

Length-prefixed JSON (same family as AMD/denoise response framing):

**Request:** `[uint32 len LE][json]`

```json
{"transcript":"I want to pay my bill","rate_hz":8000,"audio_b64":"<optional pcm16 mono base64>"}
```

**Response:** `[uint32 len LE][json]`

```json
{"complete":true,"confidence":0.92}
```

## Environment (Go server)

| Variable | Default | Description |
|----------|---------|-------------|
| `SEMANTIC_TURN_ENABLED` | `false` | Enable remote EOU refinement |
| `SEMANTIC_TURN_SOCKET` | — | Unix socket path |
| `SEMANTIC_TURN_ADDR` | — | TCP `host:port` |
| `SEMANTIC_TURN_TIMEOUT_MS` | `100` | Per-predict timeout |
| `SEMANTIC_COMPLETE_SILENCE_MS` | `280` | Shorter wait when model says complete |

When disabled, `NoopSemanticTurn` preserves CT-6 endpointing.

## v1 heuristic (placeholder)

Until a model worker is deployed, implement a simple transcript heuristic:

- Complete when the utterance ends with sentence punctuation or matches common closing phrases.
- Incomplete when trailing conjunctions / open phrases (`"account number is"`, `"my date of birth is"`).

## Audio upgrade path

Replace the classifier with smart-turn-v3 or LiveKit turn-detector ONNX:

1. Decode `audio_b64` to PCM16 mono at `rate_hz`.
2. Run the audio model; ignore or blend with transcript features.
3. Return the same JSON response shape.

Go `TurnManager` already passes `recentAudio` from `ObserveAudio` through `Predict`.

## Running a worker (future)

```bash
cd workers/semantic_turn
pip install -r requirements.txt
python server.py --socket /tmp/semantic_turn.sock
```

Set `SEMANTIC_TURN_ENABLED=true` and `SEMANTIC_TURN_SOCKET=/tmp/semantic_turn.sock` on the Go server.
