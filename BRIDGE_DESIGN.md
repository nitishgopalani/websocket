# Cross-City AI Bridge — Design (Phase 0)

**Branch:** `feature/cross-city-ai-bridge`  
**Repo:** `github.com/nitishgopalani/websocket`  
**Status:** Design review — **no implementation code yet**

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
           │  callbridge │  NEW binary: cmd/callbridge
           │  (relay)    │  internal/callbridge
           └─────────────┘
```

The existing `cmd/server` AI media pipeline (ASR → brain → TTS) is **untouched**. The bridge is a **separate binary and code path**, not a new `CARRIER` mode inside the AI server.

---

## New components

| Path | Role |
|------|------|
| `cmd/callbridge/main.go` | Standalone process: TLS HTTP server, `/healthz`, `/metrics`, bridge WebSocket mount |
| `internal/callbridge/server.go` | HTTP upgrade, auth, session registry |
| `internal/callbridge/pair.go` | Bridge room: match leg A ↔ leg B |
| `internal/callbridge/leg.go` | One connector session: reader + single writer goroutine |
| `internal/callbridge/queue.go` | Bounded per-leg outbound queue with age-based drop |
| `internal/callbridge/auth.go` | `X-Fonada-Media-Signature` validation (same HMAC scheme as echobot) |
| `internal/callbridge/metrics.go` | Prometheus counters/histograms |

Shared protocol constants can **import** from `internal/media` (`AsteriskMsgSessionStart`, `AsteriskReadyMessage`, etc.) without pulling in SessionManager / ASR / brain.

---

## Wire protocol (connector ↔ bridge)

Identical to [DINESH_PROTOCOL.md](docs/DINESH_PROTOCOL.md) / connector `wsclient`:

| Direction | Type | Payload |
|-----------|------|---------|
| Connector → Bridge | TEXT JSON | `session_start`, `session_end` |
| Connector → Bridge | BINARY | PCM16 LE @ `input_sample_rate` (typically 16 kHz, 20 ms ≈ 640 B) |
| Bridge → Connector | TEXT JSON | `ready`, `end_of_call`, `error` |
| Bridge → Connector | BINARY | PCM16 LE @ negotiated `output_sample_rate` (match input for Phase 0: 16 kHz) |

Bridge responds with `{"type":"ready"}` after pairing succeeds (both legs present) or after `session_start` is accepted for the second leg that completes the pair.

On peer teardown, the bridge sends `{"type":"end_of_call"}` to the surviving leg (connector’s `wsclient` already handles `end_of_call` → hangup path). The bridge then closes both sockets cleanly.

---

## Pairing mechanism

### Requirements

- Two connector sessions must join the **same logical bridge** before audio relay starts.
- Each session has a distinct `session_id` (AudioSocket UUID in `X-Fonada-Session-Id` and `session_start.session_id`).
- **No connector changes** — pairing data must come from the WebSocket URL and/or `session_start` fields the connector already sends.

### Chosen design: **dual source, one precedence**

| Source | Fields | When used |
|--------|--------|-----------|
| **1. URL query (primary for Phase 0)** | `bridge=<id>` required; `leg=a\|b` optional | Registered tenant `media_ws_url` or per-session orchestrator `media-meta` URL |
| **2. `session_start.metadata` (override)** | `bridge_id`, `bridge_leg` (`a`/`b`) | If present, overrides query params |

**Precedence:** metadata wins over query if both set.

### Why query param is primary for Phase 0 loopback

- Tenant `salary-on-time` has **one** registered `media_ws_url` — both local calls dial the same URL.
- Connector **does** support **per-session** URL overrides via orchestrator `/internal/media-meta/<uuid>` (BYO dial) without code changes — useful later for explicit `leg=a` / `leg=b`.
- For the simplest Phase 0 test (two SIPp calls, same tenant registration): register  
  `wss://loadtest-mohali.fonada.ai:<port>/bridge?bridge=loop1`  
  Both legs hit the same URL → bridge pairs by **auto leg assignment** (below).

### Leg assignment

| `leg` provided? | Behavior |
|-----------------|----------|
| `leg=a` or `leg=b` | Fixed role |
| Omitted | **Auto-assign:** first `session_start` on `bridge=<id>` → leg **A**; second → leg **B** |
| Third connection same `bridge=<id>` while pair active | Reject with `error` (`bridge_full`); do not disturb active pair |

Waiting room: leg A may connect and receive `ready` only after leg B completes `session_start` (or vice versa). Until paired, inbound binary audio is discarded (no relay). Optional: hold leg A in `waiting` without `ready` until B arrives — **chosen behavior: defer `ready` until both legs paired** so connectors don’t pump audio into a void.

### Future cross-city

- Mohali call: orchestrator media-meta URL `wss://bridge-central/bridge?bridge=<call-id>&leg=a`
- Bangalore call: `...&leg=b`
- Same `bridge=<call-id>`, explicit legs, no auto-assign ambiguity.

---

## Relay path

```
Leg A reader ──► enqueue to B.outQueue ──► B writer ──► WS binary ──► Connector B ──► Asterisk B
Leg B reader ──► enqueue to A.outQueue ──► A writer ──► WS binary ──► Connector A ──► Asterisk A
```

### Frame handling

- Treat each inbound **binary** WS message as one relay unit (connector typically sends 20 ms PCM16 chunks).
- **No transcoding** in Phase 0: input rate from `session_start.audio` must match peer’s expected output rate (both 16 kHz slin16). Mismatch → `error` at pair time.
- Copy frame bytes before enqueue (slice reuse safety).

### Per-leg outbound queue

| Parameter | Default | Env override |
|-----------|---------|--------------|
| Max depth | **100 frames** | `BRIDGE_QUEUE_FRAMES` |
| Max age | **200 ms** | `BRIDGE_MAX_FRAME_AGE_MS` |

**Enqueue (writer goroutine’s queue):**

1. Stamp `enqueued_at = now` on each frame.
2. While queue head frame age `> MAX_AGE`, drop head, increment `bridge_frames_dropped_age_total`.
3. If `len(queue) >= MAX_DEPTH`, drop **oldest**, increment `bridge_frames_dropped_depth_total`.
4. Never deliver stale audio — prefer silence/gap over delayed speech.

### Single-writer rule

- Gorilla WebSocket: one writer per connection.
- Each `leg` has exactly **one `writeLoop` goroutine** draining its outbound queue.
- Readers never call `WriteMessage` directly.

### Relay latency measurement

- Histogram `bridge_relay_latency_ms`: `now - frame.arrived_at` at enqueue to peer (not at write), tagged `direction` (`a_to_b`, `b_to_a`).

---

## Lifecycle

```
                    ┌─────────────┐
     leg A connect  │   WAITING   │  leg B connect
        ──────────►│  (1 leg)    │◄──────────
                    └──────┬──────┘
                           │ both session_start
                    ┌──────▼──────┐
                    │   PAIRED    │◄── ready sent to both
                    │  (relay on) │
                    └──────┬──────┘
           ┌───────────────┼───────────────┐
    leg A session_end/     │          leg B drop
    disconnect             │          session_end
           ▼               ▼               ▼
    end_of_call ──► B     PAIR         end_of_call ──► A
    close B               DEAD          close A
```

| Event | Action |
|-------|--------|
| `session_start` (1st leg) | Create room `bridge=<id>`, assign leg, wait |
| `session_start` (2nd leg) | Assign leg, validate rates, send `ready` to **both**, start relay |
| Binary from A | Enqueue to B (if paired) |
| `session_end` or read error | Send `end_of_call` to peer, close peer, remove room, log both `session_id`s |
| Auth failure | Close with WS 1008, no room created |

**Logging (structured):**

- `bridge_paired bridge_id leg_a_session leg_b_session`
- `bridge_teardown bridge_id reason survivor_session departed_session`

---

## Authentication & TLS

Reuse connector ↔ echobot pattern (see `asterisk-connector/scripts/voice_loadtest/fast_discard_echobot.go`):

- Header `X-Fonada-Session-Id`: session UUID
- Header `X-Fonada-Media-Signature`: `t=<unix>,v1=<hmac_sha256(t.session_id, secret)>`
- Secret: `FONADA_MEDIA_SECRET` env (≥ 16 chars)
- Reject stale timestamps (> 5 min skew)

**TLS (Mohali Phase 0):**

- Listen `LISTEN_ADDR` (default `:18445`)
- Cert/key: same paths as loadtest echobot (`/etc/ssl/certs/Newfonada/fullchain.pem`, `fonada_ai.key`)
- Public name: `loadtest-mohali.fonada.ai` + DNAT (SSRF-safe; orchestrator already allows this host)
- Mount: `BRIDGE_WS_PATH` default `/bridge`

**Health:** `GET /healthz` → 200  
**Metrics:** `GET /metrics` (Prometheus)

---

## Metrics

| Metric | Type | Labels |
|--------|------|--------|
| `bridge_active_pairs` | Gauge | — |
| `bridge_legs_connected` | Gauge | `state=waiting\|paired` |
| `bridge_frames_relayed_total` | Counter | `direction=a_to_b\|b_to_a` |
| `bridge_frames_dropped_age_total` | Counter | `direction` |
| `bridge_frames_dropped_depth_total` | Counter | `direction` |
| `bridge_relay_latency_ms` | Histogram | `direction` |
| `bridge_pairings_total` | Counter | `result=ok\|rejected` |
| `bridge_teardowns_total` | Counter | `reason=leg_a_end\|leg_b_end\|error` |

---

## Configuration

| Env | Default | Description |
|-----|---------|-------------|
| `LISTEN_ADDR` | `:18445` | HTTP(S) listen |
| `BRIDGE_WS_PATH` | `/bridge` | WebSocket mount (query params preserved) |
| `FONADA_MEDIA_SECRET` | — | Required HMAC secret |
| `BRIDGE_QUEUE_FRAMES` | `100` | Per-leg outbound queue depth |
| `BRIDGE_MAX_FRAME_AGE_MS` | `200` | Max frame age before drop |
| `TLS_CERT` / `TLS_KEY` | — | If both set, TLS enabled |
| `METRICS_ENABLED` | `true` | Prometheus |

---

## Phase 0 loopback test plan (Mohali box)

**Prerequisites (operator PC, box has no egress):**

1. Build `callbridge` locally; deploy binary to Mohali (PAUSE before deploy).
2. Run bridge: TLS on `:18445`, path `/bridge`, `FONADA_MEDIA_SECRET` = loadtest secret.
3. Register media-stream (voice-api): tenant `salary-on-time`, URL  
   `wss://loadtest-mohali.fonada.ai:18445/bridge?bridge=phase0-1`, test key only.
4. Originate **two** local calls (SIPp × 2 or sequential test calls) to DID `1725617001` with continuous PCAP scenario.
5. Connector unchanged (sync `d3be2df4`); both calls hit registered bridge URL.

**Pass criteria:**

| Check | Target |
|-------|--------|
| Pairing | `bridge_pairings_total{result="ok"}` increments; logs show both session UUIDs |
| Relay rate | `bridge_frames_relayed_total` grows **both** directions ~50 fps each |
| Drops | age + depth drops ≈ 0 under steady load |
| Relay latency | p99 `bridge_relay_latency_ms` < few ms (local box) |
| Teardown | Hang up one leg → peer gets `end_of_call`, both sessions close, `bridge_active_pairs` → 0 |

**Optional latency baseline:** correlate SIPp/connector timestamps with bridge histogram for Asterisk-in A → Asterisk-out B (best-effort via logs + metrics).

**Teardown:**

- Stop bridge + SIPp
- Asterisk channels → 0
- DELETE test media-stream registration
- Sites `200/403/403`
- Connector untouched

---

## Unit tests (Step 3 gate)

| Test | Assert |
|------|--------|
| `TestPairing_autoAssign` | Two WS clients, same `?bridge=x`, both get `ready`, paired |
| `TestPairing_explicitLeg` | `leg=a` + `leg=b` via query |
| `TestPairing_metadataOverride` | `session_start.metadata.bridge_id/bridge_leg` |
| `TestRelay_bidirectional` | Frame sent on A appears on B read, and reverse |
| `TestAgeDrop` | Frame backdated > 200 ms dropped, counter incremented |
| `TestTeardown_oneLeg` | A closes → B receives `end_of_call` |

Run: `go build ./...`, `go vet ./...`, `go test -race ./internal/callbridge/...`

---

## Explicit non-goals (Phase 0)

- No AI / ASR / TTS in bridge path
- No changes to `cmd/server`, `internal/media` session pipeline, or `asterisk-connector`
- No cross-box deployment (Bangalore) until Phase 0 passes
- No merge to `main`

---

## Open questions for review

1. **Defer `ready` until paired** — acceptable latency on leg A (~seconds until leg B dials)?
2. **Auto leg assignment** — OK for Phase 0, with explicit `leg=` via orchestrator media-meta for production cross-city?
3. **Bridge port `18445`** — confirm no conflict with echobot `18443` / `18444` on Mohali box.
4. **Rate mismatch policy** — hard reject pair if A is 8 kHz and B is 16 kHz (recommended: reject).

---

*Next step after approval: implement `internal/callbridge` + `cmd/callbridge` per this doc.*
