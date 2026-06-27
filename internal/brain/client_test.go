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

func (r *recordingReplyConsumer) OnReplyError(_ context.Context, _ *media.Session, _ string, _ string) {}

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
