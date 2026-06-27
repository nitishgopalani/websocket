package brain

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"websocket/internal/media"
)

type recordingReplyHandler struct {
	chunks      []ChunkMessage
	flowClass   media.FlowClass
	done        bool
	errFallback string
}

func (r *recordingReplyHandler) OnReplyChunk(_ context.Context, _ *media.Session, turnID string, seq int, text string) {
	r.chunks = append(r.chunks, ChunkMessage{TurnID: turnID, Seq: seq, Text: text})
}

func (r *recordingReplyHandler) OnFlowClassHint(_ context.Context, _ *media.Session, _ string, class media.FlowClass) {
	r.flowClass = class
}

func (r *recordingReplyHandler) OnTurnDone(_ context.Context, _ *media.Session, _ DoneMessage) {
	r.done = true
}

func (r *recordingReplyHandler) OnTurnError(_ context.Context, _ *media.Session, _ string, fallback string) {
	r.errFallback = fallback
}

func TestClientSessionStartAndTurn(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	var gotStart SessionStartPayload
	var gotTurn TurnPayload

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
			case TypeSessionStart:
				if err := json.Unmarshal(data, &gotStart); err != nil {
					t.Fatalf("session_start: %v", err)
				}
			case TypeTurn:
				if err := json.Unmarshal(data, &gotTurn); err != nil {
					t.Fatalf("turn: %v", err)
				}
				_ = conn.WriteJSON(ChunkMessage{Type: TypeChunk, TurnID: gotTurn.TurnID, Seq: 0, Text: "Namaste."})
				_ = conn.WriteJSON(FlowClassMessage{Type: TypeFlowClass, TurnID: gotTurn.TurnID, Next: "YesNo"})
				_ = conn.WriteJSON(DoneMessage{Type: TypeDone, TurnID: gotTurn.TurnID, AuditID: "audit-1"})
			case TypeSessionEnd:
				return
			}
		}
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	reply := &recordingReplyHandler{}
	tm := media.NewTurnManager(nil, media.DefaultEndpointConfig(), media.NewFakeClock(time.Now()), media.NoopVAD{}, nil, media.SemanticTurnConfig{}, nil, nil)
	client := NewClient(Config{Enabled: true, URL: wsURL, BorrowerIDParam: "borrower_id", AgentIDParam: "agent_id"}, reply, tm, nil)

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
	if len(reply.chunks) != 1 || reply.flowClass != media.FlowYesNo {
		t.Fatalf("reply chunks=%v flow=%v", reply.chunks, reply.flowClass)
	}
}

func TestParseFlowClassHint(t *testing.T) {
	if ParseFlowClassHint("SpelledInput") != media.FlowSpelledInput {
		t.Fatal("SpelledInput mapping")
	}
}
