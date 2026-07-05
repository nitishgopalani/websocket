package media_test

import (
	"context"
	"encoding/base64"
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

type recordingEgress struct {
	mu     sync.Mutex
	chunks []media.TTSAudioChunk
	marks  []string
	clears int
}

func (e *recordingEgress) SendAudio(_ context.Context, _ *media.Session, chunk media.TTSAudioChunk) error {
	e.mu.Lock()
	e.chunks = append(e.chunks, chunk)
	e.mu.Unlock()
	return nil
}

func (e *recordingEgress) Mark(_ context.Context, _ *media.Session, turnID string) error {
	e.mu.Lock()
	e.marks = append(e.marks, turnID)
	e.mu.Unlock()
	return nil
}

func (e *recordingEgress) ClearPlayback(_ context.Context, _ *media.Session) error {
	e.mu.Lock()
	e.clears++
	e.mu.Unlock()
	return nil
}

type speakRecorder struct {
	mu    sync.Mutex
	calls []string
}

func startFakeElevenLabs(t *testing.T, rec *speakRecorder, blockTurn *string) (string, func()) {
	t.Helper()
	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
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
			var msg struct {
				Text string `json:"text"`
			}
			if err := json.Unmarshal(data, &msg); err != nil {
				continue
			}
			if strings.TrimSpace(msg.Text) == "" {
				_ = conn.WriteJSON(map[string]any{"isFinal": true})
				continue
			}
			if blockTurn != nil && *blockTurn != "" {
				continue
			}
			rec.mu.Lock()
			rec.calls = append(rec.calls, msg.Text)
			rec.mu.Unlock()

			for i := 0; i < 3; i++ {
				if blockTurn != nil && *blockTurn != "" {
					break
				}
				payload := base64.StdEncoding.EncodeToString([]byte{byte(i + 1), byte(i + 2)})
				_ = conn.WriteJSON(map[string]any{"audio": payload})
			}
			_ = conn.WriteJSON(map[string]any{"isFinal": true})
		}
	}))
	return "ws" + strings.TrimPrefix(srv.URL, "http"), srv.Close
}

func testTTSConfig(wsURL string) media.TTSConfig {
	cfg := media.DefaultTTSConfig()
	cfg.Enabled = true
	cfg.APIKey = "test-key"
	cfg.BaseURL = wsURL
	cfg.VoiceID = "voice-test"
	return cfg
}

func TestTTSReplyConsumerRoutesAudioAndMark(t *testing.T) {
	speakRec := &speakRecorder{}
	wsURL, cleanup := startFakeElevenLabs(t, speakRec, nil)
	defer cleanup()

	provider, err := media.NewElevenLabsTTSProvider(testTTSConfig(wsURL))
	if err != nil {
		t.Fatalf("provider: %v", err)
	}

	stream, err := provider.Open(context.Background(), media.TTSSessionMeta{StreamSID: "MZ-TTS"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer stream.Close()

	egress := &recordingEgress{}
	var endCall atomic.Bool
	consumer := media.NewTTSReplyConsumer(stream, egress, nil, func(_ context.Context, _ *media.Session) {
		endCall.Store(true)
	}, nil)

	session := &media.Session{StreamSID: "MZ-TTS"}
	consumer.BindSession(session)
	ctx := context.Background()

	consumer.OnReplyChunk(ctx, session, "turn-1", 0, "Namaste.")
	consumer.OnReplyDone(ctx, session, "turn-1", true, "resolved")

	waitUntil(t, 3*time.Second, func() bool {
		egress.mu.Lock()
		defer egress.mu.Unlock()
		return len(egress.marks) >= 1 && len(egress.chunks) >= 3
	})
	waitUntil(t, 3*time.Second, func() bool { return endCall.Load() })

	egress.mu.Lock()
	defer egress.mu.Unlock()
	if len(egress.marks) != 1 || egress.marks[0] != "turn-1" {
		t.Fatalf("marks = %v", egress.marks)
	}
	for i, c := range egress.chunks {
		if c.TurnID != "turn-1" {
			t.Fatalf("chunk %d turnID = %q", i, c.TurnID)
		}
		if c.Final {
			t.Fatalf("chunk %d should not be final marker in egress", i)
		}
	}
}

func TestTTSReplyConsumerCancelSuppressesAudio(t *testing.T) {
	speakRec := &speakRecorder{}
	blocked := ""
	wsURL, cleanup := startFakeElevenLabs(t, speakRec, &blocked)
	defer cleanup()

	provider, _ := media.NewElevenLabsTTSProvider(testTTSConfig(wsURL))
	stream, err := provider.Open(context.Background(), media.TTSSessionMeta{StreamSID: "MZ-CAN"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer stream.Close()

	egress := &recordingEgress{}
	consumer := media.NewTTSReplyConsumer(stream, egress, nil, nil, nil)
	session := &media.Session{StreamSID: "MZ-CAN"}
	consumer.BindSession(session)

	consumer.OnReplyChunk(context.Background(), session, "turn-cancel", 0, "hello")
	blocked = "turn-cancel"
	consumer.CancelPlayback(context.Background(), "turn-cancel")

	time.Sleep(300 * time.Millisecond)
	egress.mu.Lock()
	n := len(egress.chunks)
	clears := egress.clears
	egress.mu.Unlock()
	if n != 0 {
		t.Fatalf("expected no audio after cancel, got %d chunks", n)
	}
	if clears != 1 {
		t.Fatalf("clears = %d, want 1", clears)
	}
}

func TestTTSReplyConsumerErrorSpeaksFallback(t *testing.T) {
	speakRec := &speakRecorder{}
	wsURL, cleanup := startFakeElevenLabs(t, speakRec, nil)
	defer cleanup()

	provider, _ := media.NewElevenLabsTTSProvider(testTTSConfig(wsURL))
	stream, _ := provider.Open(context.Background(), media.TTSSessionMeta{StreamSID: "MZ-ERR"})
	defer stream.Close()

	consumer := media.NewTTSReplyConsumer(stream, &recordingEgress{}, nil, nil, nil)
	session := &media.Session{StreamSID: "MZ-ERR"}
	consumer.OnReplyError(context.Background(), session, "turn-err", "Please hold.")

	waitUntil(t, 2*time.Second, func() bool {
		speakRec.mu.Lock()
		defer speakRec.mu.Unlock()
		return len(speakRec.calls) > 0
	})
}

func TestTTSReplyConsumerSpeaksFirstChunkBeforeDone(t *testing.T) {
	speakRec := &speakRecorder{}
	wsURL, cleanup := startFakeElevenLabs(t, speakRec, nil)
	defer cleanup()

	provider, err := media.NewElevenLabsTTSProvider(testTTSConfig(wsURL))
	if err != nil {
		t.Fatalf("provider: %v", err)
	}
	stream, err := provider.Open(context.Background(), media.TTSSessionMeta{StreamSID: "MZ-STREAM"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer stream.Close()

	egress := &recordingEgress{}
	consumer := media.NewTTSReplyConsumer(stream, egress, nil, nil, nil)
	session := &media.Session{StreamSID: "MZ-STREAM"}
	ctx := context.Background()

	consumer.OnReplyChunk(ctx, session, "turn-stream", 0, "Pehla vakya.")
	waitUntil(t, 2*time.Second, func() bool {
		speakRec.mu.Lock()
		defer speakRec.mu.Unlock()
		return len(speakRec.calls) >= 1
	})

	consumer.OnReplyChunk(ctx, session, "turn-stream", 1, "Doosra vakya.")
	consumer.OnReplyDone(ctx, session, "turn-stream", false, "")

	waitUntil(t, 2*time.Second, func() bool {
		speakRec.mu.Lock()
		defer speakRec.mu.Unlock()
		return len(speakRec.calls) >= 2
	})
}

type recordingPlaybackDone struct {
	mu    sync.Mutex
	turns []string
}

func (r *recordingPlaybackDone) NotifyPlaybackDone(_ *media.Session, turnID string) {
	r.mu.Lock()
	r.turns = append(r.turns, turnID)
	r.mu.Unlock()
}

func TestTTSReplyConsumerNotifiesPlaybackDoneAndDelaysEndCall(t *testing.T) {
	speakRec := &speakRecorder{}
	wsURL, cleanup := startFakeElevenLabs(t, speakRec, nil)
	defer cleanup()

	provider, err := media.NewElevenLabsTTSProvider(testTTSConfig(wsURL))
	if err != nil {
		t.Fatalf("provider: %v", err)
	}
	stream, err := provider.Open(context.Background(), media.TTSSessionMeta{StreamSID: "MZ-PBD"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer stream.Close()

	egress := &recordingEgress{}
	var endCallAt atomic.Int64
	consumer := media.NewTTSReplyConsumer(stream, egress, nil, func(_ context.Context, _ *media.Session) {
		endCallAt.Store(time.Now().UnixNano())
	}, nil)
	notifier := &recordingPlaybackDone{}
	consumer.SetPlaybackDoneNotifier(notifier)

	session := &media.Session{StreamSID: "MZ-PBD"}
	consumer.BindSession(session)
	ctx := context.Background()

	// end_call with a 150ms grace: playback_done fires immediately at playback
	// completion; the hangup lands only after the delay.
	consumer.SetEndCallDelay("turn-bye", 150*time.Millisecond)
	start := time.Now()
	consumer.OnReplyChunk(ctx, session, "turn-bye", 0, "Dhanyavaad, namaste.")
	consumer.OnReplyDone(ctx, session, "turn-bye", true, "COMPLETED")

	waitUntil(t, 3*time.Second, func() bool {
		notifier.mu.Lock()
		defer notifier.mu.Unlock()
		return len(notifier.turns) >= 1
	})
	notifier.mu.Lock()
	if notifier.turns[0] != "turn-bye" {
		t.Fatalf("playback_done turns = %v", notifier.turns)
	}
	notifier.mu.Unlock()
	if endCallAt.Load() != 0 {
		t.Fatal("end call must not fire before the grace delay")
	}

	waitUntil(t, 3*time.Second, func() bool { return endCallAt.Load() != 0 })
	elapsed := time.Duration(endCallAt.Load() - start.UnixNano())
	if elapsed < 150*time.Millisecond {
		t.Fatalf("end call fired after %v, want >= 150ms", elapsed)
	}
}

// startFakeElevenLabsNoFinal mimics the REAL ElevenLabs stream-input behavior
// on a persistent connection: audio chunks per flush but never an isFinal
// event (isFinal only comes when the whole socket-level generation closes).
func startFakeElevenLabsNoFinal(t *testing.T) (string, func()) {
	t.Helper()
	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
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
			var msg struct {
				Text string `json:"text"`
			}
			if err := json.Unmarshal(data, &msg); err != nil {
				continue
			}
			if strings.TrimSpace(msg.Text) == "" {
				continue // flush/keepalive: no isFinal on a live connection
			}
			for i := 0; i < 3; i++ {
				payload := base64.StdEncoding.EncodeToString([]byte{byte(i + 1), byte(i + 2)})
				_ = conn.WriteJSON(map[string]any{"audio": payload})
			}
		}
	}))
	return "ws" + strings.TrimPrefix(srv.URL, "http"), srv.Close
}

func TestTTSReplyConsumerFinalFallbackWithoutIsFinal(t *testing.T) {
	wsURL, cleanup := startFakeElevenLabsNoFinal(t)
	defer cleanup()

	provider, err := media.NewElevenLabsTTSProvider(testTTSConfig(wsURL))
	if err != nil {
		t.Fatalf("provider: %v", err)
	}
	stream, err := provider.Open(context.Background(), media.TTSSessionMeta{StreamSID: "MZ-NOFIN"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer stream.Close()

	egress := &recordingEgress{}
	var endCallAt atomic.Int64
	consumer := media.NewTTSReplyConsumer(stream, egress, nil, func(_ context.Context, _ *media.Session) {
		endCallAt.Store(time.Now().UnixNano())
	}, nil)
	notifier := &recordingPlaybackDone{}
	consumer.SetPlaybackDoneNotifier(notifier)

	session := &media.Session{StreamSID: "MZ-NOFIN"}
	consumer.BindSession(session)
	ctx := context.Background()

	consumer.OnReplyChunk(ctx, session, "turn-hold", 0, "Please hold the line.")
	consumer.OnReplyDone(ctx, session, "turn-hold", true, "COMPLETED")

	// No isFinal ever arrives; the local fallback must still mark the turn,
	// notify playback_done, and run end_call.
	waitUntil(t, 3*time.Second, func() bool {
		egress.mu.Lock()
		defer egress.mu.Unlock()
		return len(egress.marks) >= 1
	})
	egress.mu.Lock()
	if egress.marks[0] != "turn-hold" {
		t.Fatalf("marks = %v", egress.marks)
	}
	egress.mu.Unlock()

	waitUntil(t, 3*time.Second, func() bool {
		notifier.mu.Lock()
		defer notifier.mu.Unlock()
		return len(notifier.turns) == 1 && notifier.turns[0] == "turn-hold"
	})
	waitUntil(t, 3*time.Second, func() bool { return endCallAt.Load() != 0 })
}

func TestNoopTTSProviderRegression(t *testing.T) {
	stream, err := media.NoopTTSProvider{}.Open(context.Background(), media.TTSSessionMeta{StreamSID: "MZ-NOOP"})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()

	egress := &recordingEgress{}
	consumer := media.NewTTSReplyConsumer(stream, egress, nil, nil, nil)
	session := &media.Session{StreamSID: "MZ-NOOP"}
	consumer.OnReplyChunk(context.Background(), session, "t1", 0, "hello")
	consumer.OnReplyDone(context.Background(), session, "t1", false, "")

	time.Sleep(50 * time.Millisecond)
	egress.mu.Lock()
	defer egress.mu.Unlock()
	if len(egress.chunks) != 0 {
		t.Fatalf("noop should emit no audio, got %d chunks", len(egress.chunks))
	}
}

func TestElevenLabsReconnectCounter(t *testing.T) {
	speakRec := &speakRecorder{}
	var dials atomic.Int32
	wsURL, cleanup := startFakeElevenLabs(t, speakRec, nil)
	defer cleanup()

	cfg := testTTSConfig(wsURL)
	provider, _ := media.NewElevenLabsTTSProvider(cfg)

	var firstConn *websocket.Conn
	provider.SetTTSDialer(func(ctx context.Context, url string, header http.Header) (*websocket.Conn, *http.Response, error) {
		n := dials.Add(1)
		d := websocket.Dialer{}
		conn, resp, err := d.DialContext(ctx, url, header)
		if err != nil {
			return nil, resp, err
		}
		if n == 1 {
			firstConn = conn
		}
		return conn, resp, nil
	})

	stream, err := provider.Open(context.Background(), media.TTSSessionMeta{StreamSID: "MZ-REC"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer stream.Close()

	_ = stream.Speak("turn-r", "first")
	time.Sleep(100 * time.Millisecond)
	if firstConn != nil {
		_ = firstConn.Close()
	}
	_ = stream.Speak("turn-r2", "second")

	waitUntil(t, 3*time.Second, func() bool {
		return dials.Load() >= 2
	})
}

func waitUntil(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition timeout")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
