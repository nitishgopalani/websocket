# Phase 3b-ii: Bidirectional Translation Bridge — Design

**Branch:** `feature/cross-city-ai-bridge`  
**Status:** Design review — **no code until approved**  
**Baseline:** Phase 3b-i proven one-way (A Hindi → B English, 16 kHz ASR upsample, Mayura, ElevenLabs, ~0.8 s e2e warm, Party B confirmed clean, `fail_open=0`)

---

## 1. Goal

Extend the translator bridge so **both parties speak their own language** and hear the other party **translated**:

| Party | Speaks | Hears |
|-------|--------|-------|
| A (BLR, leg=a) | Hindi (`hi-IN`) | Hindi TTS (translation of B's English) |
| B (Mohali, leg=b) | English (`en-IN`) | English TTS (translation of A's Hindi) |

The **second lane** (B→A) reuses the proven 3b-i pipeline. The **hard part** is running **both lanes concurrently** without acoustic echo loops, cross-talk, or self-hearing.

---

## 2. Current 3b-i Architecture (reference)

Today each `room` holds one `aToBLane` only:

```
Leg A uplink ──► [Transcode→ASR→TurnMgr→Mayura→TTS→legEgress] ──► Leg B downlink (paced)
Leg A uplink ──X raw relay muted (unless fail-open)
Leg B uplink ──► raw relay (always) ──► Leg A downlink (unpaced)
Leg A downlink ◄── comfort noise when B quiet
```

Key bindings (`leg.go`, `lane.go`, `pairing.go`):

- ASR ingest: **Leg A binary only** → `room.lane.ingestAudio`
- TTS egress: **Leg B only** via `legEgress` → `targetLeg.enqueueBinaryPaced`
- Raw relay: A→B **muted** when lane active; B→A **always on**
- TurnManager: one instance per lane; semantic/backchannel/barge-in **disabled**
- `fail_open`: one sticky flag on `aToBLane`; restores raw A→B relay only

**3b-ii replaces B→A raw relay with a symmetric translated lane** and adds a **room-level coordinator** for echo suppression and turn arbitration.

---

## 3. Target Architecture (3b-ii)

```
                    ┌─────────────────────────────────────┐
                    │         roomCoordinator             │
                    │  floor state + echo-suppress gates  │
                    └──────────┬──────────────┬───────────┘
                               │              │
         Lane A→B              │              │         Lane B→A
  Leg A uplink ──► pipeline ──► Leg B downlink │  Leg B uplink ──► pipeline ──► Leg A downlink
  (hi ASR→Mayura→en TTS)       │              │  (en ASR→Mayura→hi TTS)
                               │              │
  Raw A→B: OFF                 │              │  Raw B→A: OFF
  Comfort noise on A when idle │              │  Comfort noise on B when idle
```

Each lane is an independent instance of the proven chain. The coordinator gates **ASR ingest** on each leg based on **who holds the floor** and **whether TTS is playing on that leg's phone**.

---

## Problem 1 — Acoustic Echo / Feedback Loop (#1 risk)

### 4.1 Failure mode

When Lane A→B injects **English TTS into B's earpiece/speaker**, B's phone microphone picks it up. That audio enters **Leg B uplink** → Lane B→A ASR → Mayura en→hi → Hindi TTS to A → A's mic may re-enter Lane A→B → **infinite or long-lived loop**.

Mirror risk when Lane B→A plays Hindi TTS on A's phone while Lane A→B listens to A's mic.

This is **not** solved by today's code: B→A raw relay is always on, and there is no reference-aware AEC. Carrier echo cancellation on PSTN/mobile is **unreliable** for TTS-like wideband speech.

### 4.2 Options evaluated

| Option | Mechanism | Pros | Cons |
|--------|-----------|------|------|
| **(a) Half-duplex ASR gating** | While playing TTS **to leg X**, suppress ASR ingest **from leg X** | Deterministic, low CPU, matches interpreter model | Brief deaf window on each side during peer TTS; needs tail timer |
| **(b) Energy / echo detection** | VAD on uplink during peer TTS; discard frames above threshold | Could shorten deaf window | False positives/negatives on mobile; tuning per handset; hard in production |
| **(c) Rely on carrier AEC** | Assume PSTN removes far-end echo | Zero app logic | **Proven insufficient** for app-injected TTS; different path than normal call audio |

### 4.3 Recommendation: **(a) TTS-aware half-duplex ASR gating** (primary)

**Rule:** A leg's microphone is **deaf** to its own translation pipeline while **either**:

1. **This leg is receiving TTS** from the opposite lane (`*_TTS_PLAYING` on that leg), or  
2. **This leg's uplink is not the floor holder** during simultaneous capture (Problem 2).

Implementation hook (conceptual — no code yet):

```go
// leg.readLoop, before lane.ingestAudio:
if room.coordinator.AllowASRIngest(s.role) {
    lane.ingestAudio(ctx, data)
}
// raw relay remains OFF in both directions (3b-ii)
```

**`AllowASRIngest(leg)` returns false when:**

| Condition | Rationale |
|-----------|-----------|
| Opposite lane TTS actively playing **to this leg** | Block acoustic echo of translated speech |
| Echo tail window after that TTS ends (configurable, default **350 ms**) | Handset speaker tail + room reverberation |
| Room floor owned by **other** speaker in `CAPTURING` or `PLAYING` | Half-duplex interpreter (Problem 2) |

**Does not suppress** when this leg is the floor holder and no TTS is playing **to this leg** — user speech flows normally.

### 4.4 Echo-guard state machine (per leg, driven by coordinator)

Each leg tracks `echoSuppress` derived from room state:

```
                    ┌──────────────────────────────────┐
                    │  legEchoGuard (per leg A / B)    │
                    └──────────────────────────────────┘

  OPEN ──(peer TTS first frame to this leg)──► MUTED
  MUTED ──(peer TTS finished + tail_ms elapsed)──► OPEN

  OPEN ──(floor lost to peer: peer CAPTURING/PLAYING)──► MUTED
  MUTED ──(floor granted back: IDLE or self CAPTURING)──► OPEN
```

**Signals:**

- `OnTTSStart(targetLeg)` → mute **targetLeg** ASR ingest immediately (before first frame hits ear)
- `OnTTSComplete(targetLeg)` → start `echo_tail_timer` (default 350 ms), then unmute **targetLeg** if floor allows
- `OnSpeechStart(sourceLeg)` → floor arbitration (Problem 2); may mute **other** leg ASR

**Why not mute during self-TTS?** In 3b-ii the speaker never receives their own TTS on their leg (Problem 3). Self-echo is not a path. We mute the **listener** leg while **they** hear translated audio — exactly when their mic would pick up the earpiece.

### 4.5 Fallback (future, not v1)

If field tests show tail too long/short, add **optional** energy gate as a secondary filter only during `MUTED` (discard frames > N dB above comfort-noise baseline). Not required for initial build.

---

## Problem 2 — Turn-Taking / Simultaneous Speech

### 5.1 Failure mode

If A and B speak at once, **both lanes** emit `TurnEndOfTurn` → two TTS streams → overlapping translations, coordinator fights, perceived cross-talk.

Human interpreters are **half-duplex**: one direction at a time.

### 5.2 Room-level floor state machine

Single `roomCoordinator` per bridge (new component), states:

| State | Meaning |
|-------|---------|
| `IDLE` | No capture or playback active |
| `A_CAPTURING` | A's speech being ASR'd on Lane A→B |
| `B_CAPTURING` | B's speech being ASR'd on Lane B→A |
| `A_PLAYING` | Lane A→B TTS playing to Leg B |
| `B_PLAYING` | Lane B→A TTS playing to Leg A |

**Allowed transitions:**

```
IDLE ──A:OnSpeechStart──► A_CAPTURING
IDLE ──B:OnSpeechStart──► B_CAPTURING

A_CAPTURING ──A:TurnEndOfTurn + pipeline start──► A_PLAYING
B_CAPTURING ──B:TurnEndOfTurn + pipeline start──► B_PLAYING

A_PLAYING ──TTS complete + echo tail──► IDLE
B_PLAYING ──TTS complete + echo tail──► IDLE

A_CAPTURING ──silence timeout / cancel──► IDLE
B_CAPTURING ──silence timeout / cancel──► IDLE
```

**Illegal / deferred:** `A_CAPTURING` → `B_CAPTURING` without passing through `IDLE` (unless pre-empt — see below).

### 5.3 Simultaneous start arbitration

When `IDLE` and both `OnSpeechStart` within **debounce window** (default **150 ms**):

- **First arrival wins (FAW)** — standard for v1
- Second leg: ASR ingest **blocked** at coordinator; partials ignored
- Log metric: `translator_floor_contention_total`

When **not IDLE** and the non-floor leg starts speaking:

| Current state | Behavior |
|---------------|----------|
| `A_CAPTURING` / `A_PLAYING` | Ignore B capture until A cycle completes (B may hear comfort noise only) |
| `B_CAPTURING` / `B_PLAYING` | Symmetric |
| Optional v1.1: **barge-in pre-empt** | If B speaks > **800 ms** during `A_PLAYING`, cancel A TTS (`ttsPlayer.cancelPlayback`), transition to `B_CAPTURING` — **disabled in v1** |

### 5.4 Interaction with per-lane TurnManager

**Reuse** existing `media.TurnManager` per lane unchanged (300 ms silence, 8 s max utterance, no semantic/backchannel).

Coordinator listens to:

- `OnSpeechStart` / `OnSpeechEnd` from each lane's `latencyBridge` (new callbacks)
- `ttsPlayer` start/complete hooks per lane

TurnManager on the **non-floor** lane still receives audio frames only if `AllowASRIngest` is true — when false, frames are dropped **before** `ingestAudio`, so no phantom turns.

### 5.5 Playback serialization within a lane

Existing `ttsPlayer.speakMu` (**last utterance wins** within one lane) stays. Coordinator ensures **only one lane** reaches `*_PLAYING` at a time in v1.

---

## Problem 3 — Lane Isolation (no self-hearing)

### 6.1 Invariants (must hold)

| ID | Invariant |
|----|-----------|
| L1 | Lane A→B ASR reads **only** Leg A uplink binary |
| L2 | Lane A→B TTS writes **only** to Leg B downlink (paced egress) |
| L3 | Lane B→A ASR reads **only** Leg B uplink binary |
| L4 | Lane B→A TTS writes **only** to Leg A downlink (paced egress) |
| L5 | Party A **never** hears their own Hindi translated back (no A→B TTS to A) |
| L6 | Party B **never** hears their own English translated back |
| L7 | **No raw relay** in either direction when translation active (both muted symmetrically) |

### 6.2 Binding spec

**Leg readLoop (both legs symmetric):**

```
binary from connector
  ├─ if translationEnabled && coordinator.AllowASRIngest(thisRole):
  │     └─ thisRole's lane.ingestAudio(data)   // A→ laneAToB, B→ laneBToA
  └─ relayToPeer(data)  // gated by shouldRelayToPeer()
```

**`shouldRelayToPeer()` (3b-ii):**

| Leg | Relay when |
|-----|------------|
| A | `!translationEnabled` OR `laneAToB.failOpen()` OR no lane |
| B | `!translationEnabled` OR `laneBToA.failOpen()` OR no lane |

Default translated mode: **both false** → **zero raw relay**.

**TTS egress:** unchanged — each lane's `legEgress` holds `targetLeg` pointer set at lane construction (`sourceLeg` / `targetLeg` fixed).

### 6.3 Raw relay retirement

3b-i kept B→A raw relay so Party A heard **something** while B was silent. In 3b-ii:

- B→A raw relay **removed** when both lanes healthy
- **Comfort noise** injected on **both** legs when outbound queue empty and peer not playing (extend `maybeInjectComfortNoise` to Leg B)
- On **per-lane fail-open**, restore **that direction's raw relay only** (A→B fail-open → A raw to B; B→A fail-open → B raw to A)

### 6.4 Self-hearing check

| Path | Blocked by |
|------|------------|
| A speaks → A→B TTS → B hears (OK) | — |
| B hears → B mic → B→A (echo) | Echo guard mutes B ASR during A_PLAYING |
| A hears → A mic → A→B while B TTS to A | Echo guard mutes A ASR during B_PLAYING |
| A speaks → A→B TTS routed to A | **Impossible** — egress bound to Leg B only (L2) |

---

## 7. Per-Lane Configuration

Global env vars are **insufficient** for bidirectional (single `ASR_LANGUAGE`, single `ELEVENLABS_LANGUAGE`). Introduce **per-lane config** on `LaneDeps` (construction time, not new env sprawl for v1 — hardcode table below, override via env later).

### Lane A→B (Hindi → English)

| Parameter | Value | Notes |
|-----------|-------|-------|
| Source leg | A | uplink @ 8 kHz leg → 16 kHz ASR |
| Target leg | B | paced egress @ leg output rate |
| ASR model | Sarvam `saaras:v3` | proven 3b-i |
| ASR language | `hi-IN` | session param `asr_language` |
| ASR sample rate | 16000 | `TRANSLATOR_ASR_SAMPLE_RATE` |
| Mayura | `hi-IN` → `en-IN` | direct, proven |
| TTS provider | ElevenLabs | proven |
| TTS model | `eleven_flash_v2_5` | proven |
| TTS voice | `21m00Tcm4TlvDq8ikWAM` (Rachel) | proven English @ `language_code=en` |
| TTS language | `en` | target language for flash v2.5 |
| Silence / endpoint | 300 ms default | reuse `translatorEndpointConfig` |

### Lane B→A (English → Hindi)

| Parameter | Value | Notes |
|-----------|-------|-------|
| Source leg | B | same upsample path |
| Target leg | A | paced egress |
| ASR model | Sarvam `saaras:v3` | **en-IN** confirmed supported |
| ASR language | `en-IN` | |
| ASR sample rate | 16000 | same |
| Mayura | `en-IN` → `hi-IN` | direct, confirmed |
| TTS provider | ElevenLabs | |
| TTS model | `eleven_flash_v2_5` | |
| TTS voice | **`21m00Tcm4TlvDq8ikWAM` (Rachel)** | **Recommend reuse for v1** — Hindi echo test (3b-i echo mode) showed clean Devanagari with `ELEVENLABS_LANGUAGE=hi`; same voice reduces tuning surface |
| TTS language | `hi` | |
| Silence / endpoint | 300 ms default | consider 600 ms for spelled Hindi numbers if needed |

**Voice choice rationale:** Rachel (`21m00Tcm4TlvDq8ikWAM`) is the SOT voice; echo test proved Hindi quality. Alternative `kiaJRdXJzloFWi6AtFBf` exists in deploy scripts but is **not** live-proven on this bridge — defer unless Rachel Hindi fails Party A listening test.

**Implementation note:** `BuildDepsFromEnv` today builds one ASR/TTS provider. 3b-ii needs **`LaneDeps` factory** that clones provider config with per-lane language/voice overrides (same API keys, different session params).

---

## 8. Reuse vs New

### Reuse as-is (proven 3b-i)

| Component | File(s) |
|-----------|---------|
| Ingress pipeline | `buildIngressPipeline`, `TranscodeSink` → `ASRSink` |
| Turn endpoint tuning | `translatorEndpointConfig` (300 ms silence) |
| Translation handler | `TranslateListener` (parameterized langs) |
| TTS playback + cancel | `ttsPlayer`, `legEgress` real-time pacing |
| Latency instrumentation | `LatencyTracker`, `latencyBridge`, `translator_utterance_latency` logs |
| Outbound queue / pacing | `queue.dequeueReady`, `enqueueBinaryPaced` |
| Media auth / pairing | `auth.go`, `pairing.go` (extend room struct) |
| Fail-open pattern | per-lane sticky flag + relay restore |

### Refactor (generalize, minimal)

| Change | From → To |
|--------|-----------|
| Lane type | `aToBLane` → `translationLane{direction, sourceLeg, targetLeg, deps}` |
| Room lanes | `lane *aToBLane` → `laneAToB`, `laneBToA *translationLane` |
| `leg.readLoop` ingest | A-only → role→lane map |
| `shouldRelayToPeer` | A-only mute → symmetric per-direction fail-open |
| Comfort noise | Leg A only → both legs |
| `onRoomPaired` | one lane → two lanes + coordinator init |
| Metrics | add direction labels `a_to_b`, `b_to_a` for floor/echo events |

### New components

| Component | Responsibility |
|-----------|----------------|
| **`roomCoordinator`** | Floor state machine, `AllowASRIngest`, echo tail timers |
| **`LaneDeps` per direction** | Distinct ASR lang, Mayura pair, TTS voice/lang |
| **TTS lifecycle hooks** | `OnTTSStart(targetLeg)`, `OnTTSComplete(targetLeg)` → coordinator |
| **Speech lifecycle hooks** | `OnSpeechStart(sourceLeg)` → coordinator |
| **Config flag** | `TRANSLATOR_BIDIRECTIONAL=true` (3b-i one-way remains fallback) |

---

## 9. Latency

**Per-utterance latency should not materially increase** vs 3b-i:

- Active lane runs the same sequential pipeline: silence → ASR final → Mayura → TTS first frame → paced egress
- Coordinator gates are **in-memory checks** on ingest (~nanoseconds); no extra network hops
- Inactive lane dropped frames incur **no Mayura/TTS cost**

**Added latency only when:**

- User on non-floor side starts speaking during floor hold — speech ignored until `IDLE` (interpreter-accurate, not a pipeline slowdown)
- Echo tail (350 ms default) after peer TTS before mic re-opens — **by design**, not processing delay

Expect warm e2e **~0.8–1.0 s** per direction, same as 3b-i A→B.

---

## 10. Failure Handling

### Per-lane fail-open (independent)

Each `translationLane` owns `failOpen atomic.Bool` (sticky, same semantics as today).

| Failure | Lane behavior | Other lane |
|---------|---------------|------------|
| Mayura error on A→B | A→B fail-open → raw A audio to B | B→A **continues translating** |
| TTS error on B→A | B→A fail-open → raw B audio to A | A→B **continues** |
| ASR stream death | treat as lane fail-open after N reconnect failures | unaffected |
| Lane setup failure at pair | that direction raw relay; other lane if configured | partial bridge |

Coordinator on fail-open:

- Stop muting raw relay for **that direction only**
- Echo guard for that leg may **release** (raw Hindi/English passthrough is acceptable degrade)
- Metric: `translator_fail_open_total{direction="a_to_b|b_to_a"}`

**Room teardown:** unchanged — either leg disconnect tears down both lanes + coordinator.

---

## 11. Config Summary (deploy target)

```bash
TRANSLATION_ENABLED=true
TRANSLATOR_BIDIRECTIONAL=true          # NEW — false preserves 3b-i one-way
TRANSLATOR_MODE=translate
TRANSLATOR_ASR_SAMPLE_RATE=16000
TRANSLATOR_TTS_SYNTH_RATE=16000
TRANSLATOR_SILENCE_MS_DEFAULT=300
TRANSLATOR_ECHO_TAIL_MS=350            # NEW — post-TTS deaf window on listener leg

# Lane A→B TTS (env until per-lane config file)
ELEVENLABS_VOICE_ID=21m00Tcm4TlvDq8ikWAM
ELEVENLABS_LANGUAGE=en                 # used by A→B lane factory
ELEVENLABS_MODEL=eleven_flash_v2_5

# Lane B→A TTS overrides (NEW env pair)
TRANSLATOR_TTS_VOICE_ID_B=21m00Tcm4TlvDq8ikWAM
TRANSLATOR_TTS_LANGUAGE_B=hi

# Languages from WS query unchanged
# lang_a=hi-IN  lang_b=en-IN
```

---

## 12. Test Plan (post-implementation, not in this doc)

1. **Echo loop test:** A speaks continuously; verify B's translated TTS does **not** re-enter as Hindi on A (log + Party A ear)
2. **Hard phrases both directions:** JP West Town / 1202 / Genesis — Hindi→English and English→Hindi back-translation
3. **Simultaneous speech:** both talk over each other — verify FAW, no dual TTS
4. **Fail-open isolation:** kill Mayura on one lane; other lane still translates
5. **Latency table:** per-direction e2e, `fail_open=0` both lanes

---

## 13. Approved Decisions (2026-07-17)

1. **Echo tail 350 ms** — approved; env `TRANSLATOR_ECHO_TAIL_MS=350`.
2. **First-arrival-wins** (150 ms debounce) — approved for v1.
3. **Barge-in pre-empt** — deferred to v1.1.
4. **Rachel for Hindi output to A** — approved for v1 (reuse). Evaluate native Hindi ElevenLabs voice as polish; Party A verdict decides.
5. **`TRANSLATOR_BIDIRECTIONAL=false`** — keep as 3b-i one-way fallback for staged rollout.

### Addition A — Loop-breaker safety net

Independent of half-duplex gating. Per-room circuit breaker trips when:

- More than **N translations in T seconds** (default **>6 in 10 s**), **or**
- Same/near-same text detected translating back-and-forth (ping-pong).

On trip: fall back to **raw relay for the entire room**, log loud alert, stop translating until call ends.

Env: `TRANSLATOR_LOOP_BREAKER_MAX=6`, `TRANSLATOR_LOOP_BREAKER_WINDOW_MS=10000`.

### Addition B — Floor-release timing

Floor returns to `IDLE` **only after**:

1. Speaker's speech ends (turn emitted),
2. Full translation finishes playing to peer (last paced egress frame),
3. Echo tail elapses on the listener leg.

**Not before.** Unit tests must assert non-floor leg ASR stays `MUTED` until floor is truly released.

---

## 14. Implementation Status

**Approved — implementation in progress on `feature/cross-city-ai-bridge`.**
