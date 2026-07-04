# Tenant forwarding — connector client_id → brain tenant_id

Branch: `feature/tenant-forwarding` (from `main`)

## Why

A live inbound call to the booking DID (1725617002) must reach the
booking-confirm bot in the brain. The asterisk-connector now stamps a
per-listener tenant into `session_start.client_id` (its
`feature/per-tenant-ports` branch), and the brain routes tenants from
`session_start.tenant_id` — but this go-server dropped `client_id` on the
floor when opening the brain session. `SessionStartPayload.TenantID` was only
ever filled from a `tenant_id` session param or the static `BRAIN_TENANT_ID`
env, so every call looked like the default tenant.

## What changed

`internal/brain/client.go` — mapping fix only, no contract change:

- `session_start.tenant_id` now resolves in order:
  1. explicit `tenant_id` session param (unchanged, still wins),
  2. the connector's `client_id` (already present in session params via
     `AsteriskStartToStartEvent`; this is the new hop),
  3. `BRAIN_TENANT_ID` env fallback (unchanged).
- `agent_id`: `metadata.agent_id` from the connector (merged into session
  params by `AsteriskStartToStartEvent`) is forwarded; the literal
  `"default"` is used only when no agent id was supplied anywhere. This was
  already the effective behaviour via `AgentIDParam="agent_id"`; it is now an
  explicit, named resolver (`resolveBrainAgentID`) so it can't regress
  silently.

## Tests (real output)

- `TestClientForwardsClientIDAsTenant` — drives a REAL Asterisk
  `session_start` wire frame (`client_id":"booking-confirm"`,
  `metadata.agent_id":"persona_customer"`) through `ParseAsteriskControl` →
  `AsteriskStartToStartEvent` → `brain.Client.Connect` against an httptest WS
  server, and asserts the brain receives `tenant_id=booking-confirm` and
  `agent_id=persona_customer`.
- `TestClientTenantPrecedence` — table test: explicit `tenant_id` beats
  `client_id`; `client_id` used when no `tenant_id`; `BRAIN_TENANT_ID`
  fallback when neither.

```
go vet ./internal/brain/...   -> clean
go test ./...                 -> PASS
  ok  websocket/cmd/replay             5.111s
  ok  websocket/internal/brain         1.294s
  ok  websocket/internal/media         2.638s
  ok  websocket/internal/media/eval    0.570s
  ok  websocket/internal/media/sim     2.858s
```

(`gofmt -l` flags `client.go`/`contract.go`/`client_borrower_context_test.go`
— pre-existing on `main` before this branch; not introduced here.)

## Assumed, not verified

- End-to-end live path (telco → Asterisk → connector:9093 → this server →
  brain) is exercised only by unit/integration tests against fakes. The live
  smoke test on 1725617002 is a separate, coordinated step after review.
- The brain (`collections`) honours `tenant_id=booking-confirm` per its
  `feature/booking-confirm-bot` branch; verified there, not from this repo.
