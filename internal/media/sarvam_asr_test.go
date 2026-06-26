package media

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestNoopASRProvider(t *testing.T) {
	provider := NoopASRProvider{}
	sess, err := provider.Open(context.Background(), ASRSessionMeta{StreamSID: "MZ"})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := sess.SendAudio([]byte{0, 0, 1, 0}); err != nil {
		t.Fatalf("SendAudio: %v", err)
	}
	select {
	case _, ok := <-sess.Events():
		if ok {
			t.Fatal("expected no events from noop asr")
		}
	default:
	}
	if err := sess.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := sess.SendAudio([]byte{0, 0}); err != ErrASRSessionClosed {
		t.Fatalf("SendAudio after close = %v, want ErrASRSessionClosed", err)
	}
}

func TestNewASRProviderDisabled(t *testing.T) {
	p, err := NewASRProvider(DefaultASRConfig())
	if err != nil {
		t.Fatalf("NewASRProvider: %v", err)
	}
	if _, ok := p.(NoopASRProvider); !ok {
		t.Fatalf("expected NoopASRProvider, got %T", p)
	}
}

func TestNewASRProviderEnabledRequiresKey(t *testing.T) {
	cfg := DefaultASRConfig()
	cfg.Enabled = true
	_, err := NewASRProvider(cfg)
	if err != ErrASRNotConfigured {
		t.Fatalf("err = %v, want ErrASRNotConfigured", err)
	}
}

func TestSarvamSessionWithFakeServer(t *testing.T) {
	var mu sync.Mutex
	var gotQuery url.Values
	audioFrames := 0

	wsURL, cleanup := startFakeSarvamServer(t, func(t *testing.T, conn *websocket.Conn, r *http.Request) {
		mu.Lock()
		gotQuery = r.URL.Query()
		mu.Unlock()

		_, data, err := conn.ReadMessage()
		if err != nil {
			return
		}
		var msg sarvamAudioMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			t.Errorf("audio message: %v", err)
			return
		}
		if msg.Encoding != "pcm_s16le" || msg.SampleRate != 8000 {
			t.Errorf("unexpected audio message: %+v", msg)
		}
		audioFrames++

		sendSarvamJSON(conn, map[string]any{
			"type": "events",
			"data": map[string]string{"signal_type": "START_SPEECH"},
		})
		sendSarvamJSON(conn, map[string]any{
			"type": "data",
			"data": map[string]any{"transcript": "namaste", "is_final": false},
		})
		sendSarvamJSON(conn, map[string]any{
			"type": "data",
			"data": map[string]any{"transcript": "namaste kaise ho", "is_final": true},
		})
		sendSarvamJSON(conn, map[string]any{
			"type": "events",
			"data": map[string]string{"signal_type": "END_SPEECH"},
		})
	})
	defer cleanup()

	provider := NewSarvamASRProvider("test-key", SarvamConfig{
		Endpoint:           wsURL,
		Model:              "saaras:v3",
		Mode:               "transcribe",
		Language:           "hi-IN",
		HighVADSensitivity: true,
		VADSignals:         true,
		KeepalivePeriod:    0,
	}, nil)

	sess, err := provider.Open(context.Background(), ASRSessionMeta{
		StreamSID:  "MZ-ASR",
		SampleRate: 8000,
		Language:   "hi-IN",
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer sess.Close()

	if err := sess.SendAudio([]byte{0x01, 0x00, 0x02, 0x00}); err != nil {
		t.Fatalf("SendAudio: %v", err)
	}

	events := collectASREvents(t, sess, 4)
	if events[0].Type != ASREventSpeechStart {
		t.Fatalf("event[0] = %v, want speech start", events[0].Type)
	}
	if events[1].Type != ASREventPartial || events[1].Transcript.Text != "namaste" {
		t.Fatalf("event[1] = %+v", events[1])
	}
	if events[2].Type != ASREventFinal || events[2].Transcript.Text != "namaste kaise ho" {
		t.Fatalf("event[2] = %+v", events[2])
	}
	if events[3].Type != ASREventSpeechEnd {
		t.Fatalf("event[3] = %v, want speech end", events[3].Type)
	}

	mu.Lock()
	q := gotQuery
	mu.Unlock()
	if q.Get("sample_rate") != "8000" {
		t.Fatalf("sample_rate = %q, want 8000", q.Get("sample_rate"))
	}
	if q.Get("input_audio_codec") != "pcm_s16le" {
		t.Fatalf("input_audio_codec = %q, want pcm_s16le", q.Get("input_audio_codec"))
	}
	if q.Get("model") != "saaras:v3" {
		t.Fatalf("model = %q, want saaras:v3", q.Get("model"))
	}
	if q.Get("vad_signals") != "true" {
		t.Fatalf("vad_signals = %q, want true", q.Get("vad_signals"))
	}
	if audioFrames != 1 {
		t.Fatalf("audioFrames = %d, want 1", audioFrames)
	}
}

func TestSarvamReconnectOnDrop(t *testing.T) {
	var connCount atomicInt
	wsURL, cleanup := startFakeSarvamServer(t, func(t *testing.T, conn *websocket.Conn, _ *http.Request) {
		n := connCount.Add(1)
		if n == 1 {
			_, _, _ = conn.ReadMessage()
			_ = conn.Close()
			return
		}
		time.Sleep(200 * time.Millisecond)
	})
	defer cleanup()

	provider := NewSarvamASRProvider("test-key", SarvamConfig{
		Endpoint:           wsURL,
		Model:              "saaras:v3",
		Mode:               "transcribe",
		VADSignals:         true,
		HighVADSensitivity: true,
		KeepalivePeriod:    0,
		ReconnectBaseDelay: 50 * time.Millisecond,
		ReconnectMaxDelay:  200 * time.Millisecond,
	}, nil)

	sess, err := provider.Open(context.Background(), ASRSessionMeta{
		StreamSID:  "MZ-REC",
		SampleRate: 8000,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer sess.Close()

	ss, ok := sess.(*sarvamSession)
	if !ok {
		t.Fatalf("expected *sarvamSession, got %T", sess)
	}

	if err := sess.SendAudio([]byte{0x01, 0x00}); err != nil {
		t.Fatalf("first SendAudio: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if ss.Reconnects() >= 1 && connCount.Load() >= 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if ss.Reconnects() < 1 {
		t.Fatalf("reconnects = %d, want >= 1", ss.Reconnects())
	}
	if err := sess.SendAudio([]byte{0x02, 0x00}); err != nil {
		t.Fatalf("SendAudio after reconnect: %v", err)
	}
}

func TestSarvamLiveSmoke(t *testing.T) {
	if os.Getenv("SARVAM_LIVE_TEST") != "1" {
		t.Skip("set SARVAM_LIVE_TEST=1 to run live Sarvam smoke test")
	}
	if os.Getenv("SARVAM_API_KEY") == "" {
		t.Skip("SARVAM_API_KEY not set")
	}

	provider, err := NewASRProvider(ASRConfig{
		Enabled: true,
		APIKey:  os.Getenv("SARVAM_API_KEY"),
	})
	if err != nil {
		t.Fatalf("NewASRProvider: %v", err)
	}

	sess, err := provider.Open(context.Background(), ASRSessionMeta{
		StreamSID:  "live-smoke",
		SampleRate: 8000,
		Language:   "hi-IN",
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer sess.Close()

	silence := make([]byte, 320)
	if err := sess.SendAudio(silence); err != nil {
		t.Fatalf("SendAudio: %v", err)
	}
	time.Sleep(2 * time.Second)
}

func TestParseSarvamMessages(t *testing.T) {
	partials := parseSarvamMessages([]byte(`{"type":"data","data":{"transcript":"hi","is_final":false}}`))
	if len(partials) != 1 || partials[0].Type != ASREventPartial {
		t.Fatalf("partial = %+v", partials)
	}
	finals := parseSarvamMessages([]byte(`{"type":"transcript","data":{"transcript":"done","is_final":true}}`))
	if len(finals) != 1 || finals[0].Type != ASREventFinal {
		t.Fatalf("final = %+v", finals)
	}
}

type fakeSarvamHandler func(t *testing.T, conn *websocket.Conn, r *http.Request)

func startFakeSarvamServer(t *testing.T, handler fakeSarvamHandler) (string, func()) {
	t.Helper()
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		handler(t, conn, r)
	})
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/"
	return wsURL, ts.Close
}

func sendSarvamJSON(conn *websocket.Conn, payload map[string]any) {
	data, _ := json.Marshal(payload)
	_ = conn.WriteMessage(websocket.TextMessage, data)
}

func collectASREvents(t *testing.T, sess ASRSession, want int) []ASREvent {
	t.Helper()
	var out []ASREvent
	deadline := time.Now().Add(2 * time.Second)
	for len(out) < want && time.Now().Before(deadline) {
		select {
		case evt, ok := <-sess.Events():
			if !ok {
				t.Fatalf("events closed with %d/%d", len(out), want)
			}
			out = append(out, evt)
		case <-time.After(time.Until(deadline)):
			t.Fatalf("got %d events, want %d", len(out), want)
		}
	}
	return out
}

type atomicInt struct {
	mu sync.Mutex
	n  int
}

func (a *atomicInt) Add(v int) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.n += v
	return a.n
}

func (a *atomicInt) Load() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.n
}
