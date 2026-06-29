package brain_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"websocket/internal/brain"
	"websocket/internal/media"
)

type turnRecordingReply struct {
	mu     sync.Mutex
	chunks map[string][]string
	done   map[string]bool
}

func newTurnRecordingReply() *turnRecordingReply {
	return &turnRecordingReply{
		chunks: make(map[string][]string),
		done:   make(map[string]bool),
	}
}

func (r *turnRecordingReply) OnReplyChunk(_ context.Context, _ *media.Session, turnID string, _ int, text string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.chunks[turnID] = append(r.chunks[turnID], text)
}

func (r *turnRecordingReply) OnReplyDone(_ context.Context, _ *media.Session, turnID string, _ bool, _ string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.done[turnID] = true
}

func (r *turnRecordingReply) OnReplyError(_ context.Context, _ *media.Session, turnID, fallback string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.chunks[turnID] = append(r.chunks[turnID], fallback)
	r.done[turnID] = true
}

func TestClientSupersedesInflightTurnOnOverlap(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	var mu sync.Mutex
	var cancels []string
	var turns []brain.TurnPayload

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
			if err := json.Unmarshal(data, &header); err != nil {
				return
			}
			switch header.Type {
			case brain.TypeSessionStart:
				var start brain.SessionStartPayload
				_ = json.Unmarshal(data, &start)
				_ = conn.WriteJSON(brain.SessionReadyPayload{
					Type:        brain.TypeSessionReady,
					SessionID:   start.SessionID,
					BorrowerID:  start.BorrowerID,
					AsrLanguage: "hi-IN",
				})
			case brain.TypeCancel:
				var cancel brain.CancelPayload
				_ = json.Unmarshal(data, &cancel)
				mu.Lock()
				cancels = append(cancels, cancel.TurnID)
				mu.Unlock()
			case brain.TypeTurn:
				var turn brain.TurnPayload
				_ = json.Unmarshal(data, &turn)
				mu.Lock()
				turns = append(turns, turn)
				mu.Unlock()
				if len(turns) == 1 {
					time.Sleep(200 * time.Millisecond)
				}
				_ = conn.WriteJSON(brain.ChunkMessage{Type: brain.TypeChunk, TurnID: turn.TurnID, Seq: 0, Text: "reply-" + turn.TurnID})
				_ = conn.WriteJSON(brain.DoneMessage{Type: brain.TypeDone, TurnID: turn.TurnID})
			case brain.TypeSessionEnd:
				return
			}
		}
	}))
	defer srv.Close()

	reply := newTurnRecordingReply()
	tm := media.NewTurnManager(nil, media.DefaultEndpointConfig(), media.NewFakeClock(time.Now()), media.NoopVAD{}, nil, media.SemanticTurnConfig{}, nil, nil)
	client := brain.NewClient(brain.Config{Enabled: true, URL: "ws" + strings.TrimPrefix(srv.URL, "http")}, reply, tm, nil)
	session := &media.Session{StreamSID: "MZ-OVERLAP"}

	if err := client.Connect(context.Background(), session); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Close()

	client.OnTurnEvent(context.Background(), session, media.TurnEvent{
		Kind:       media.TurnEndOfTurn,
		Transcript: "date of birth is",
	})
	client.OnTurnEvent(context.Background(), session, media.TurnEvent{
		Kind:       media.TurnEndOfTurn,
		Transcript: "Bhakti",
	})

	deadline := time.Now().Add(3 * time.Second)
	for {
		reply.mu.Lock()
		done := len(reply.done) >= 1
		reply.mu.Unlock()
		if done || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(cancels) != 1 {
		t.Fatalf("cancels = %v, want 1 stale turn cancelled", cancels)
	}
	if len(turns) != 2 {
		t.Fatalf("turns = %d, want 2", len(turns))
	}
	if !strings.Contains(turns[1].Transcript, "Bhakti") {
		t.Fatalf("merged second turn transcript = %q", turns[1].Transcript)
	}
	if strings.Contains(turns[0].Transcript, "Bhakti") {
		t.Fatal("first turn should not include superseding fragment")
	}

	reply.mu.Lock()
	defer reply.mu.Unlock()
	if reply.chunks[cancels[0]] != nil {
		t.Fatalf("superseded turn should not produce reply chunks: %v", reply.chunks)
	}
	if !reply.done[turns[1].TurnID] {
		t.Fatalf("latest turn should complete: done=%v", reply.done)
	}
}
