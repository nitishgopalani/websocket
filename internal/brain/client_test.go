package brain_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"websocket/internal/brain"
	"websocket/internal/media"
)

type recordingReplyConsumer struct {
	chunks []string
	done   bool
}

func (r *recordingReplyConsumer) OnReplyChunk(_ context.Context, _ *media.Session, _ string, _ int, text string) {
	r.chunks = append(r.chunks, text)
}

func (r *recordingReplyConsumer) OnReplyDone(_ context.Context, _ *media.Session, _ string, _ bool, _ string) {
	r.done = true
}

func (r *recordingReplyConsumer) OnReplyError(_ context.Context, _ *media.Session, _ string, _ string) {
}

func TestClientSessionStartAndTurn(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	var gotStart brain.SessionStartPayload
	var gotTurn brain.TurnPayload

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade: %v", err)
		}
		defer conn.Close()
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var header struct {
				Type string `json:"type"`
			}
			if err := json.Unmarshal(data, &header); err != nil {
				t.Fatalf("decode: %v", err)
			}
			switch header.Type {
			case brain.TypeSessionStart:
				if err := json.Unmarshal(data, &gotStart); err != nil {
					t.Fatalf("session_start: %v", err)
				}
				_ = conn.WriteJSON(brain.SessionReadyPayload{
					Type:        brain.TypeSessionReady,
					SessionID:   gotStart.SessionID,
					BorrowerID:  gotStart.BorrowerID,
					AsrLanguage: "hi-IN",
				})
			case brain.TypeTurn:
				if err := json.Unmarshal(data, &gotTurn); err != nil {
					t.Fatalf("turn: %v", err)
				}
				_ = conn.WriteJSON(brain.ChunkMessage{Type: brain.TypeChunk, TurnID: gotTurn.TurnID, Seq: 0, Text: "Namaste."})
				_ = conn.WriteJSON(brain.FlowClassMessage{Type: brain.TypeFlowClass, TurnID: gotTurn.TurnID, Next: "YesNo"})
				_ = conn.WriteJSON(brain.DoneMessage{Type: brain.TypeDone, TurnID: gotTurn.TurnID, AuditID: "audit-1"})
			case brain.TypeSessionEnd:
				return
			}
		}
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	reply := &recordingReplyConsumer{}
	tm := media.NewTurnManager(nil, media.DefaultEndpointConfig(), media.NewFakeClock(time.Now()), media.NoopVAD{}, nil, media.SemanticTurnConfig{}, nil, nil)
	client := brain.NewClient(brain.Config{Enabled: true, URL: wsURL, BorrowerIDParam: "borrower_id", AgentIDParam: "agent_id"}, reply, tm, nil)

	session := &media.Session{
		StreamSID: "MZ-EB6",
		Params:    map[string]string{"borrower_id": "bor-1", "agent_id": "agent-1"},
	}
	if err := client.Connect(context.Background(), session); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Close()

	client.OnTurnEvent(context.Background(), session, media.TurnEvent{
		Kind:       media.TurnEndOfTurn,
		Transcript: "haan",
		FlowClass:  media.FlowYesNo,
	})

	deadline := time.Now().Add(2 * time.Second)
	for !reply.done && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !reply.done {
		t.Fatal("expected done from brain")
	}
	if gotStart.SessionID != "MZ-EB6" || gotStart.BorrowerID != "bor-1" {
		t.Fatalf("session_start = %+v", gotStart)
	}
	if session.Params["asr_language"] != "hi-IN" {
		t.Fatalf("asr_language = %q, want hi-IN", session.Params["asr_language"])
	}
	if gotTurn.Transcript != "haan" || gotTurn.FlowClass != "YesNo" {
		t.Fatalf("turn = %+v", gotTurn)
	}
	if len(reply.chunks) != 1 {
		t.Fatalf("chunks = %v", reply.chunks)
	}
}

func TestParseFlowClassHint(t *testing.T) {
	if brain.ParseFlowClassHint("SpelledInput") != media.FlowSpelledInput {
		t.Fatal("SpelledInput mapping")
	}
}

// TestClientForwardsClientIDAsTenant proves the connector's per-DID client_id
// (e.g. "booking-confirm") reaches the brain as session_start.tenant_id, and
// metadata.agent_id reaches agent_id — driven from a real Asterisk
// session_start frame through AsteriskStartToStartEvent, not synthetic params.
func TestClientForwardsClientIDAsTenant(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	startCh := make(chan brain.SessionStartPayload, 1)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var header struct {
				Type string `json:"type"`
			}
			_ = json.Unmarshal(data, &header)
			if header.Type == brain.TypeSessionStart {
				var start brain.SessionStartPayload
				_ = json.Unmarshal(data, &start)
				startCh <- start
				_ = conn.WriteJSON(brain.SessionReadyPayload{
					Type:        brain.TypeSessionReady,
					SessionID:   start.SessionID,
					AsrLanguage: "hi-IN",
				})
			}
		}
	}))
	defer srv.Close()

	// The wire frame the asterisk-connector sends for the booking DID.
	control, err := media.ParseAsteriskControl([]byte(`{
		"type": "session_start",
		"session_id": "ast-booking-1",
		"client_id": "booking-confirm",
		"metadata": {"agent_id": "persona_customer"}
	}`))
	if err != nil {
		t.Fatalf("parse asterisk session_start: %v", err)
	}
	startEvent := media.AsteriskStartToStartEvent(*control.Start)
	session := &media.Session{
		StreamSID: startEvent.StreamSID,
		Params:    startEvent.CustomParameters,
	}

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	tm := media.NewTurnManager(nil, media.DefaultEndpointConfig(), media.NewFakeClock(time.Now()), media.NoopVAD{}, nil, media.SemanticTurnConfig{}, nil, nil)
	client := brain.NewClient(brain.Config{Enabled: true, URL: wsURL}, &recordingReplyConsumer{}, tm, nil)
	if err := client.Connect(context.Background(), session); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Close()

	select {
	case got := <-startCh:
		if got.TenantID != "booking-confirm" {
			t.Fatalf("tenant_id = %q, want booking-confirm (client_id must map to tenant)", got.TenantID)
		}
		if got.AgentID != "persona_customer" {
			t.Fatalf("agent_id = %q, want persona_customer (metadata.agent_id must forward)", got.AgentID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("brain never received session_start")
	}
}

// TestClientForwardsClientIDVerbatim (HARDEN-1 F2, G-A3-03): the connector's
// per-DID client_id must reach the brain as session_start.client_id verbatim,
// AND tenant_id must still be injected (explicit param or BRAIN_TENANT_ID) for
// one more release so older brain builds keep working. The brain resolves
// tenant as client_id > session_tenant_id > reject, so on the BYO path a stale
// tenant_id must NOT overwrite client_id — this test proves the go-server hands
// the brain both fields so the brain can make that call.
func TestClientForwardsClientIDVerbatim(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	startCh := make(chan brain.SessionStartPayload, 1)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var header struct {
				Type string `json:"type"`
			}
			_ = json.Unmarshal(data, &header)
			if header.Type == brain.TypeSessionStart {
				var start brain.SessionStartPayload
				_ = json.Unmarshal(data, &start)
				startCh <- start
				_ = conn.WriteJSON(brain.SessionReadyPayload{
					Type:        brain.TypeSessionReady,
					SessionID:   start.SessionID,
					AsrLanguage: "hi-IN",
				})
			}
		}
	}))
	defer srv.Close()

	// BYO/media-meta path: connector stamps client_id=paisalo in metadata, and
	// a stale tenant_id=salary_on_time is still present in params (one more release).
	control, err := media.ParseAsteriskControl([]byte(`{
		"type": "session_start",
		"session_id": "ast-paisalo-1",
		"client_id": "paisalo",
		"metadata": {"agent_id": "paisalo-test", "tenant_id": "salary_on_time"}
	}`))
	if err != nil {
		t.Fatalf("parse asterisk session_start: %v", err)
	}
	startEvent := media.AsteriskStartToStartEvent(*control.Start)
	session := &media.Session{
		StreamSID: startEvent.StreamSID,
		Params:    startEvent.CustomParameters,
	}

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	tm := media.NewTurnManager(nil, media.DefaultEndpointConfig(), media.NewFakeClock(time.Now()), media.NoopVAD{}, nil, media.SemanticTurnConfig{}, nil, nil)
	client := brain.NewClient(brain.Config{Enabled: true, URL: wsURL}, &recordingReplyConsumer{}, tm, nil)
	if err := client.Connect(context.Background(), session); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Close()

	select {
	case got := <-startCh:
		// F2: client_id forwarded verbatim — the brain uses this as the tenant
		// source of truth (client_id > session_tenant_id > reject).
		if got.ClientID != "paisalo" {
			t.Fatalf("client_id = %q, want paisalo (must forward verbatim)", got.ClientID)
		}
		// tenant_id is still injected (explicit metadata.tenant_id wins over the
		// BRAIN_TENANT_ID fallback) so older brain builds keep working one more
		// release. The brain must NOT let this stale value override client_id.
		if got.TenantID != "salary_on_time" {
			t.Fatalf("tenant_id = %q, want salary_on_time (injected for one more release)", got.TenantID)
		}
		if got.AgentID != "paisalo-test" {
			t.Fatalf("agent_id = %q, want paisalo-test", got.AgentID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("brain never received session_start")
	}
}

// TestClientTenantPrecedence: an explicit tenant_id param beats client_id, and
// BRAIN_TENANT_ID remains the fallback when neither is present.
func TestClientTenantPrecedence(t *testing.T) {
	cases := []struct {
		name   string
		params map[string]string
		cfg    string
		want   string
	}{
		{"explicit tenant_id wins", map[string]string{"tenant_id": "acme", "client_id": "booking-confirm"}, "envdefault", "acme"},
		{"client_id used when no tenant_id", map[string]string{"client_id": "booking-confirm"}, "envdefault", "booking-confirm"},
		{"config fallback when neither", map[string]string{}, "envdefault", "envdefault"},
	}
	for _, tc := range cases {
		session := &media.Session{StreamSID: "s", Params: tc.params}
		if got := brain.ResolveBrainTenantForTest(session, tc.cfg); got != tc.want {
			t.Fatalf("%s: got %q, want %q", tc.name, got, tc.want)
		}
	}
}
