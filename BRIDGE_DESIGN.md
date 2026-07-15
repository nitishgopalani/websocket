# Cross-City AI Bridge — Design (Phase 0)

**Branch:** `feature/cross-city-ai-bridge`  
**Repo:** `github.com/nitishgopalani/websocket`  
**Status:** Implemented — **PAUSE before Mohali deploy**

## Goal

Glue two **independent** Asterisk calls (eventually Mohali ↔ Bangalore, no SIP trunk between them) by relaying **audio only** through a small central bridge service. Each call’s asterisk-connector opens a media WebSocket to the bridge; the bridge swaps PCM16 frames (A hears B, B hears A). Later, an AI leg can sit in the middle.

**Phase 0** proves the bridge on the **Mohali box alone** with two local calls looped through the bridge.

**Constraint:** Do **not** modify `asterisk-connector`. Connectors already speak the Dinesh binary-PCM16 protocol (`session_start` → binary audio → `session_end`; server replies `ready`, `end_of_call`).

---

## Architecture

```
  Call A (Asterisk)                    Call B (Asterisk)
       │                                      │
  AudioSocket                              AudioSocket
       │                                      │
  connector (unchanged)                  connector (unchanged)
       │  WSS + signature                     │
       └──────────┬───────────────────────────┘
                  │
           ┌──────▼──────┐
           │  callbridge │  cmd/callbridge + internal/callbridge
           │  (relay)    │
           └─────────────┘
```

The existing `cmd/server` AI media pipeline (ASR → brain → TTS) is **untouched**. The bridge is a **separate binary**, not a new `CARRIER` mode.

---

## Components

| Path | Role |
|------|------|
| `cmd/callbridge/main.go` | Standalone TLS/HTTP server |
| `internal/callbridge/server.go` | Upgrade, auth, handshake |
| `internal/callbridge/pair.go` | Room registry, pairing, timeout |
| `internal/callbridge/leg.go` | Per-leg reader + single writer |
| `internal/callbridge/queue.go` | Bounded queue + age drop |
| `internal/callbridge/auth.go` | HMAC media signature |
| `internal/callbridge/metrics.go` | Prometheus |

Protocol constants imported from `internal/media` only (`ParseAsteriskControl`, `AsteriskReadyMessage`, etc.) — no SessionManager / ASR / brain.

---

## Wire protocol

Same as [DINESH_PROTOCOL.md](docs/DINESH_PROTOCOL.md):

| Direction | Type | Payload |
|-----------|------|---------|
| Connector → Bridge | TEXT | `session_start`, `session_end` |
| Connector → Bridge | BINARY | PCM16 LE @ input rate |
| Bridge → Connector | TEXT | `ready`, `end_of_call`, `error` |
| Bridge → Connector | BINARY | PCM16 LE relayed audio |

`ready` is deferred until **both** legs are paired. On peer drop: `end_of_call` to survivor, then close.

---

## Pairing

| Source | Fields | Precedence |
|--------|--------|------------|
| URL query | `bridge=<id>` (required), `leg=a\|b` (optional) | default |
| `session_start.metadata` | `bridge_id`, `bridge_leg` | overrides query |

- **Auto leg assign** (no `leg=`): first `session_start` → A, second → B.
- **Third** connection while pair active: `error` (`bridge_full`).
- **Rate mismatch** at pair time: hard reject with `error` (no relay).

### Pairing timeout

| Setting | Default |
|---------|---------|
| `BRIDGE_PAIRING_TIMEOUT_MS` | **60000** (60 s) |

- Unpaired room at expiry → `error` (`pairing_timeout`) to waiting leg, close, delete room.
- Metric: `bridge_pairings_total{result="timeout"}`.
- Lone waiting leg disconnects → delete room immediately.

---

## Connector pre-ready behavior (verified)

Read-only review of `asterisk-connector/internal/wsclient/wsclient.go` + `internal/config/config.go`:

1. **`ConnectWith` blocks on `awaitReady`** until `{"type":"ready"}` or timeout — `readLoop` and audio bridging **do not start** until `ready` is received.
2. **No uplink audio** is sent before `ready` (connector `bridge.Run` only proceeds after successful `Connect`).
3. **Default ready timeout:** `WS_READY_TIMEOUT_SECONDS` = **5 seconds** (`defaultReadyTimeout` in wsclient if unset).
4. **`awaitReady` ignores binary frames** while waiting (only processes TEXT control).
5. Server `error` before `ready` fails the connect with an error (call fails loudly).

### Max safe pairing window

| Limit | Value | Notes |
|-------|-------|-------|
| Connector `WS_READY_TIMEOUT` | **5 s default** | Hard abort if `ready` not received |
| Bridge `BRIDGE_PAIRING_TIMEOUT_MS` | **60 s default** | Waiting leg gets `pairing_timeout` error |

**Effective window for defer-ready:** bounded by **connector ready timeout**, not bridge timeout, unless Mohali connector env sets `WS_READY_TIMEOUT_SECONDS` higher (config-only, no code change). For Phase 0 loopback, **leg B must connect within ~5 s of leg A** with default connector config, or increase `WS_READY_TIMEOUT_SECONDS` on the box before deploy.

Defer-ready is acceptable when the pairing window is understood and connector timeout is aligned.

---

## Relay

```
Leg A reader ──► B.outQueue ──► B writer ──► WS ──► Connector B
Leg B reader ──► A.outQueue ──► A writer ──► WS ──► Connector A
```

| Parameter | Default | Env |
|-----------|---------|-----|
| Queue depth | 100 frames | `BRIDGE_QUEUE_FRAMES` |
| Max frame age | 200 ms | `BRIDGE_MAX_FRAME_AGE_MS` |

**Age drop (enqueue):** drop head while age > max; drop oldest on overflow.

**Age drop (dequeue):** writer re-checks age immediately before `WriteMessage`; stale frames never delivered even if writer stalled. Counter: `bridge_frames_dropped_age_total`.

**Single-writer rule:** one `writeLoop` goroutine per leg; control frames (`ready`, `error`, `end_of_call`) go through the same queue with synchronous ack for teardown paths.

### Relay latency

`bridge_relay_latency_ms` measured at **WriteMessage time**: `now - frame.arrived_at` in the writer, per direction (`a_to_b`, `b_to_a`). Includes queue wait — this is the real relay latency.

---

## Lifecycle

```
leg A connect ──► WAITING (no ready)
leg B connect ──► PAIRED ──► ready sent to both ──► relay on
one leg ends  ──► end_of_call to peer ──► close both ──► room deleted
timeout       ──► error to waiting leg ──► close ──► room deleted
```

---

## Auth & TLS

- `X-Fonada-Session-Id` + `X-Fonada-Media-Signature` (`t=<unix>,v1=<hmac>`)
- `FONADA_MEDIA_SECRET` (≥ 16 chars)
- Default listen: `:18445`, path `/bridge`
- TLS: `TLS_CERT` / `TLS_KEY` (same cert paths as loadtest echobot on Mohali)

### Port check (Mohali, read-only)

`ss -tlnp` on 103.132.145.55: **no listeners on 18443–18445** at check time (box idle post-teardown). **18445 is free** for callbridge.

---

## Metrics

| Metric | Type | Labels |
|--------|------|--------|
| `bridge_active_pairs` | Gauge | — |
| `bridge_legs_connected` | Gauge | `state=waiting` |
| `bridge_frames_relayed_total` | Counter | `direction` |
| `bridge_frames_dropped_age_total` | Counter | `direction` |
| `bridge_frames_dropped_depth_total` | Counter | `direction` |
| `bridge_relay_latency_ms` | Histogram | `direction` (at write) |
| `bridge_pairings_total` | Counter | `result=ok\|rejected\|timeout` |
| `bridge_teardowns_total` | Counter | `reason` |

---

## Unit tests

| Test | Status |
|------|--------|
| `TestPairing_autoAssign` | pass |
| `TestPairing_explicitLeg` | pass |
| `TestPairing_metadataOverride` | pass |
| `TestPairing_rateMismatch` | pass |
| `TestPairingTimeout` | pass |
| `TestRelay_bidirectional` | pass |
| `TestAgeDrop_enqueue` | pass |
| `TestDequeueAgeDrop` | pass |
| `TestTeardown_oneLeg` | pass |

Gate: `go build ./... && go vet ./... && go test ./internal/callbridge/...`

---

## Phase 0 deploy (NEXT — after review)

1. Build `callbridge`; deploy to Mohali (`PAUSE` before deploy).
2. Run TLS on `:18445`, register test media-stream URL with `?bridge=phase0-1`.
3. Two local SIPp/calls to DID 1725617001; verify bidirectional relay ~50 fps, drops ≈ 0.
4. **Align `WS_READY_TIMEOUT_SECONDS`** on connector if pairing window > 5 s.
5. Teardown: stop bridge, channels→0, unregister test media-stream, sites 200/403/403.

**No merge to `main`. No connector code changes.**
