# Call Testing testdata layout

This directory holds recorded calls and labels for **live eval** (`RUN_LIVE_EVAL=1`).
Large audio files are **not** committed; add them locally or via your artifact store.

## CI smoke (committed)

| Path | Purpose |
|------|---------|
| `../smoke.ulaw` | Synthetic 50-frame μ-law silence sequence for carrier-simulator smoke |

## L-9 AMD samples

| File | Purpose |
|------|---------|
| `human_synthetic.ulaw` | espeak-ng “Hello?” — **plumbing only** (SYNTHETIC) |
| `voicemail_synthetic.ulaw` | espeak-ng voicemail phrase — **plumbing only** (SYNTHETIC) |
| `human_real.ulaw` | **ELEVENLABS-SYNTHESIZED** — clean studio hello pickup (8 kHz mono μ-law) |
| `voicemail_real.ulaw` | **ELEVENLABS-SYNTHESIZED** — clean studio voicemail greeting |
| `human_long.ulaw` | **ELEVENLABS-SYNTHESIZED** — longer human utterance (>2 s, AMD/turn window) |

Regenerate ElevenLabs fixtures (uses `ELEVENLABS_API_KEY` from `.env`, never committed):

```bash
bash scripts/gen_fixtures.sh
```

**Note:** These are clean TTS samples — they validate ASR + AMD logic end-to-end but are **not** a substitute for a real phone-line recording. Final pilot sign-off still needs one genuine recorded call.

Companion `.ref.txt` files hold the exact spoken text for WER / transcript checks.

### Replay (one command)

```bash
# Workers + server + both samples; prints Whisper transcripts and AMD decisions
bash scripts/replay_amd_l9.sh
```

Prefers `human_real.ulaw` / `voicemail_real.ulaw` when present; otherwise uses `human_synthetic.ulaw` / `voicemail_synthetic.ulaw`.

Convert WAV → μ-law:

```bash
ffmpeg -y -i recording.wav -ar 8000 -ac 1 -f mulaw testdata/calls/human_real.ulaw
```

Watch classification:

```bash
tail -f scripts/workers.log | grep 'amd classify'
```

Expect `result=human` vs `result=machine` with `transcript=...` in the log line.
Empty transcript → fail-open human (weak audio / wrong codec).

## Live eval layout (local / artifact)

```
testdata/calls/
  <call_id>.ulaw          # 8 kHz μ-law raw audio (20 ms = 160 bytes per frame)
  <call_id>.ref.txt       # Reference transcript (one line or full text)
amd_labels.csv            # AMD greeting labels (see format below)
```

### Reference transcript (`.ref.txt`)

Plain UTF-8 text — the ground-truth caller speech for WER.

### AMD labels (`amd_labels.csv`)

```csv
sample_id,label,path
greeting_human_01,human,calls/greeting_human_01.ulaw
greeting_vm_01,voicemail,calls/greeting_vm_01.ulaw
```

- `label`: `human` or `voicemail`
- `path`: relative to `testdata/`

## Running live eval

Requires `RUN_LIVE_EVAL=1`, real API keys (`SARVAM_API_KEY`, `ELEVENLABS_API_KEY`), brain WS, and AMD worker.
See `internal/media/sim/live_eval_test.go`.
