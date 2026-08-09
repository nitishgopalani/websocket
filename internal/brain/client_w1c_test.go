package brain

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"websocket/internal/media"
)

// W1-C C0 (DEBT-026): session_ready carries the tenant apology line; the
// brain client wires it into the TTSReplyConsumer; on a later ASR-kill the
// DeadAirHandler speaks the apology via TTS (audio frames reach egress) and
// clean-closes the session. Completes invariant #10.

// capturingEgress records every audio chunk sent to the carrier egress.
type w1cCapturingEgress struct {
	mu     sync.Mutex
	chunks []media.TTSAudioChunk
}

func (e *w1cCapturingEgress) SendAudio(_ context.Context, _ *media.Session, chunk media.TTSAudioChunk) error {
	e.mu.Lock()
	e.chunks = append(e.chunks, chunk)
	e.mu.Unlock()
	return nil
}
func (e *w1cCapturingEgress) Mark(_ context.Context, _ *media.Session, _ string) error { return nil }
func (e *w1cCapturingEgress) ClearPlayback(_ context.Context, _ *media.Session) error  { return nil }
func (e *w1cCapturingEgress) DropPending() int                                   { return 0 }

// synthTTS emits one mu-law chunk + Final per non-empty Speak.
type w1cSynthTTS struct {
	audio  chan media.TTSAudioChunk
	closed bool
	mu     sync.Mutex
}

func newW1CSynthTTS() *w1cSynthTTS {
	return &w1cSynthTTS{audio: make(chan media.TTSAudioChunk, 8)}
}

func (s *w1cSynthTTS) Speak(turnID string, text string) error {
	if text == "" {
		return nil
	}
	s.audio <- media.TTSAudioChunk{TurnID: turnID, Seq: 1, MuLaw: []byte{0x80}, Final: false}
	s.audio <- media.TTSAudioChunk{TurnID: turnID, Seq: 2, MuLaw: []byte{0x80}, Final: true}
	return nil
}
func (s *w1cSynthTTS) Cancel(_ string) error { return nil }
func (s *w1cSynthTTS) Audio() <-chan media.TTSAudioChunk {
	return s.audio
}
func (s *w1cSynthTTS) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		close(s.audio)
	}
	return nil
}

func TestSessionReadyWiresApologyLineThenASRKillSpeaksItAndCloses(t *testing.T) {
	apology := "माफ़ कीजिए, लाइन में तकनीकी समस्या आ रही है। हम आपसे थोड़ी देर में दोबारा संपर्क करेंगे। धन्यवाद।"
	voice := "abhilash"

	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	var gotStart SessionStartPayload

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade: %v", err)
		}
		defer conn.Close()
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
		if header.Type != TypeSessionStart {
			t.Fatalf("first msg = %q, want session_start", header.Type)
		}
		if err := json.Unmarshal(data, &gotStart); err != nil {
			t.Fatalf("session_start: %v", err)
		}
		// Ack session_start WITH the apology line + voice (W1-C C0).
		_ = conn.WriteJSON(SessionReadyPayload{
			Type:           TypeSessionReady,
			SessionID:      gotStart.SessionID,
			BorrowerID:     gotStart.BorrowerID,
			AsrLanguage:    "hi-IN",
			ApologyText:    apology,
			ApologyVoiceID: voice,
		})
		// Hold the conn open so the brain client stays connected.
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	// TTS reply consumer with synth TTS + capturing egress.
	synth := newW1CSynthTTS()
	egress := &w1cCapturingEgress{}
	var endCallCalls atomic.Int32
	ttsConsumer := media.NewTTSReplyConsumer(synth, egress, nil, func(_ context.Context, _ *media.Session) {
		endCallCalls.Add(1)
	}, nil)
	tm := media.NewTurnManager(nil, media.DefaultEndpointConfig(), media.NewFakeClock(time.Now()), media.NoopVAD{}, nil, media.SemanticTurnConfig{}, nil, nil)
	client := NewClient(Config{Enabled: true, URL: wsURL, BorrowerIDParam: "borrower_id", AgentIDParam: "agent_id"}, ttsConsumer, tm, nil)

	session := &media.Session{
		StreamSID: "MZ-C0",
		Params:    map[string]string{"borrower_id": "bor-1", "agent_id": "agent-1"},
	}
	if err := client.Connect(context.Background(), session); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Close()

	// Give readSessionReady a moment to land + wire SetApologyLine.
	time.Sleep(150 * time.Millisecond)

	// Simulate ASR-kill: the DeadAirHandler shares the same ttsConsumer.
	handler := media.NewDeadAirHandler(ttsConsumer, nil)
	handler.OnASRDead(context.Background(), session)

	// Apology Speak → synth emits 2 chunks → routeAudio egresses them →
	// OnReplyDone(end_call=true) → final-fallback → onEndCall.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		egress.mu.Lock()
		n := len(egress.chunks)
		egress.mu.Unlock()
		if n >= 2 && endCallCalls.Load() >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	egress.mu.Lock()
	got := egress.chunks
	egress.mu.Unlock()
	if len(got) < 2 {
		t.Fatalf("apology audio frames not produced after ASR-kill (chunks=%d)", len(got))
	}
	if got[0].TurnID != "apology-dead-air" {
		t.Errorf("first apology chunk turn_id=%q, want apology-dead-air", got[0].TurnID)
	}
	if endCallCalls.Load() == 0 {
		t.Errorf("session should have been closed after the apology (endCallCalls=%d)", endCallCalls.Load())
	}
	_ = ttsConsumer.Close()
}
