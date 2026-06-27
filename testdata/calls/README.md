# Call Testing testdata layout

This directory holds recorded calls and labels for **live eval** (`RUN_LIVE_EVAL=1`).
Large audio files are **not** committed; add them locally or via your artifact store.

## CI smoke (committed)

| Path | Purpose |
|------|---------|
| `smoke.ulaw` | Synthetic 50-frame μ-law silence sequence for carrier-simulator smoke |

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
