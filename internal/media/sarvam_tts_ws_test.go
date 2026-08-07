package media

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestSarvamWSStream_AudioChunksArrive(t *testing.T) {
	configReceived := make(chan map[string]any, 1)
	textReceived := make(chan string, 10)

	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Logf("upgrade error: %v", err)
			return
		}
		defer conn.Close()

		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var msg sarvamWSMessage
			if err := json.Unmarshal(data, &msg); err != nil {
				continue
			}

			switch msg.Type {
			case "config":
				configReceived <- msg.Data
			case "text":
				if txt, ok := msg.Data["text"].(string); ok {
					textReceived <- txt
					pcm := make([]byte, 640)
					for i := range pcm {
						pcm[i] = byte(i % 256)
					}
					audioResp := map[string]any{
						"type": "audio",
						"data": map[string]any{
							"audio":        base64.StdEncoding.EncodeToString(pcm),
							"content_type": "audio/raw",
						},
					}
					payload, _ := json.Marshal(audioResp)
					_ = conn.WriteMessage(websocket.TextMessage, payload)
				}
			case "flush":
				finalResp := map[string]any{
					"type": "event",
					"data": map[string]any{
						"event_type": "final",
					},
				}
				payload, _ := json.Marshal(finalResp)
				_ = conn.WriteMessage(websocket.TextMessage, payload)
			}
		}
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	t.Setenv("SARVAM_TTS_WS_URL", wsURL)
	t.Setenv("SARVAM_TTS_STREAMING", "true")

	provider := &SarvamTTSProvider{
		apiKey:  "test-key",
		baseURL: srv.URL,
		model:   "bulbul:v3",
		speaker: "amit",
		lang:    "hi-IN",
		logger:  slog.Default(),
	}

	meta := TTSSessionMeta{
		StreamSID:        "test-stream",
		CallSID:          "test-call",
		OutputSampleRate: 8000,
	}

	ws := newSarvamTTSWSStream(provider, meta, 8000)
	ws.wsURL = wsURL

	ctx := context.Background()
	if err := ws.Open(ctx); err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer ws.Close()

	select {
	case cfg := <-configReceived:
		if cfg["speaker"] != "amit" {
			t.Errorf("config speaker = %v, want amit", cfg["speaker"])
		}
		if cfg["speech_sample_rate"] != float64(8000) {
			t.Errorf("config sample_rate = %v, want 8000", cfg["speech_sample_rate"])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for config")
	}

	if err := ws.Speak("turn-1", "नमस्ते"); err != nil {
		t.Fatalf("Speak failed: %v", err)
	}

	select {
	case txt := <-textReceived:
		if txt != "नमस्ते" {
			t.Errorf("text = %q, want नमस्ते", txt)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for text")
	}

	if err := ws.Speak("turn-1", ""); err != nil {
		t.Fatalf("Speak flush failed: %v", err)
	}

	audioCh := ws.Audio()
	var chunks []TTSAudioChunk
	timeout := time.After(2 * time.Second)
loop:
	for {
		select {
		case chunk := <-audioCh:
			chunks = append(chunks, chunk)
			if chunk.Final {
				break loop
			}
		case <-timeout:
			break loop
		}
	}

	if len(chunks) == 0 {
		t.Fatal("no audio chunks received")
	}
	hasAudio := false
	hasFinal := false
	for _, c := range chunks {
		if len(c.MuLaw) > 0 {
			hasAudio = true
		}
		if c.Final {
			hasFinal = true
		}
	}
	if !hasAudio {
		t.Error("no audio data in chunks")
	}
	if !hasFinal {
		t.Error("no final chunk received")
	}
}

func TestSarvamWSStream_ReconnectThenRESTFallback(t *testing.T) {
	var connectCount atomic.Int32
	var mu sync.Mutex
	shouldFail := true

	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	wsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		fail := shouldFail
		mu.Unlock()

		if fail {
			connectCount.Add(1)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		for {
			_, _, err := conn.ReadMessage()
			if err != nil {
				return
			}
		}
	}))
	defer wsSrv.Close()

	var restCalls atomic.Int32
	restSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		restCalls.Add(1)
		pcm := make([]byte, 320)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"audios": []string{base64.StdEncoding.EncodeToString(pcm)},
		})
	}))
	defer restSrv.Close()

	cap := &warnCapture{}

	provider := &SarvamTTSProvider{
		apiKey:  "test-key",
		baseURL: restSrv.URL,
		model:   "bulbul:v3",
		speaker: "amit",
		lang:    "hi-IN",
		client:  restSrv.Client(),
		logger:  slog.New(cap),
	}

	meta := TTSSessionMeta{
		StreamSID: "fallback-test",
		CallSID:   "call-fallback",
	}

	wsURL := "ws" + strings.TrimPrefix(wsSrv.URL, "http")
	ws := newSarvamTTSWSStream(provider, meta, 8000)
	ws.wsURL = wsURL

	ctx := context.Background()
	err := ws.Open(ctx)
	if err == nil {
		t.Log("Open succeeded unexpectedly, server may have upgraded; checking fallback path")
	}

	if err := ws.Speak("turn-1", "hello"); err != nil {
		t.Fatalf("Speak failed: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	if restCalls.Load() == 0 {
		t.Error("expected REST fallback to be called")
	}

	cap.mu.Lock()
	found := false
	for _, msg := range cap.msgs {
		if strings.Contains(msg, "tts_ws_fallback") {
			found = true
			break
		}
	}
	cap.mu.Unlock()

	if !found {
		t.Error("expected WARN tts_ws_fallback in logs")
	}

	if ws.Path() != "rest" {
		t.Errorf("Path = %q, want rest", ws.Path())
	}
}

func TestSarvamWSStream_VoiceChangeReconnects(t *testing.T) {
	var connectCount atomic.Int32
	var lastConfig atomic.Value

	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		connectCount.Add(1)
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
			var msg sarvamWSMessage
			if err := json.Unmarshal(data, &msg); err != nil {
				continue
			}

			switch msg.Type {
			case "config":
				lastConfig.Store(msg.Data)
			case "text":
				pcm := make([]byte, 320)
				audioResp := map[string]any{
					"type": "audio",
					"data": map[string]any{
						"audio":        base64.StdEncoding.EncodeToString(pcm),
						"content_type": "audio/raw",
					},
				}
				payload, _ := json.Marshal(audioResp)
				_ = conn.WriteMessage(websocket.TextMessage, payload)
			case "flush":
				finalResp := map[string]any{
					"type": "event",
					"data": map[string]any{"event_type": "final"},
				}
				payload, _ := json.Marshal(finalResp)
				_ = conn.WriteMessage(websocket.TextMessage, payload)
			}
		}
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	provider := &SarvamTTSProvider{
		apiKey:  "test-key",
		baseURL: srv.URL,
		model:   "bulbul:v3",
		speaker: "amit",
		lang:    "hi-IN",
		logger:  slog.Default(),
	}

	meta := TTSSessionMeta{
		StreamSID: "voice-change-test",
		CallSID:   "call-voice",
	}

	ws := newSarvamTTSWSStream(provider, meta, 8000)
	ws.wsURL = wsURL

	ctx := context.Background()
	if err := ws.Open(ctx); err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer ws.Close()

	time.Sleep(100 * time.Millisecond)
	initialConnects := connectCount.Load()
	if initialConnects < 1 {
		t.Fatal("expected at least 1 initial connection")
	}

	if err := ws.Speak("turn-1", "hello"); err != nil {
		t.Fatalf("Speak 1 failed: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	ws.SetTurnVoice("turn-2", "neha", "bulbul:v3", nil)
	if err := ws.Speak("turn-2", "world"); err != nil {
		t.Fatalf("Speak 2 failed: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	finalConnects := connectCount.Load()
	if finalConnects <= initialConnects {
		t.Errorf("expected reconnect on voice change: initial=%d, final=%d", initialConnects, finalConnects)
	}

	cfg := lastConfig.Load()
	if cfg == nil {
		t.Fatal("no config received")
	}
	cfgMap := cfg.(map[string]any)
	if cfgMap["speaker"] != "neha" {
		t.Errorf("speaker = %v, want neha", cfgMap["speaker"])
	}
}

func TestSarvamWSStream_EmptySpeakNoFinalWhileInFlight(t *testing.T) {
	textReceived := make(chan string, 10)
	respondCh := make(chan struct{})

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
			var msg sarvamWSMessage
			if err := json.Unmarshal(data, &msg); err != nil {
				continue
			}

			switch msg.Type {
			case "config":
			case "text":
				if txt, ok := msg.Data["text"].(string); ok {
					textReceived <- txt
					<-respondCh
					pcm := make([]byte, 320)
					audioResp := map[string]any{
						"type": "audio",
						"data": map[string]any{
							"audio":        base64.StdEncoding.EncodeToString(pcm),
							"content_type": "audio/raw",
						},
					}
					payload, _ := json.Marshal(audioResp)
					_ = conn.WriteMessage(websocket.TextMessage, payload)
				}
			case "flush":
				finalResp := map[string]any{
					"type": "event",
					"data": map[string]any{"event_type": "final"},
				}
				payload, _ := json.Marshal(finalResp)
				_ = conn.WriteMessage(websocket.TextMessage, payload)
			}
		}
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	provider := &SarvamTTSProvider{
		apiKey:  "test-key",
		baseURL: srv.URL,
		model:   "bulbul:v3",
		speaker: "amit",
		lang:    "hi-IN",
		logger:  slog.Default(),
	}

	meta := TTSSessionMeta{StreamSID: "race-test"}

	ws := newSarvamTTSWSStream(provider, meta, 8000)
	ws.wsURL = wsURL

	ctx := context.Background()
	if err := ws.Open(ctx); err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer ws.Close()

	if err := ws.Speak("turn-1", "test text"); err != nil {
		t.Fatalf("Speak text failed: %v", err)
	}

	select {
	case <-textReceived:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for text")
	}

	if err := ws.Speak("turn-1", ""); err != nil {
		t.Fatalf("Speak empty failed: %v", err)
	}

	audioCh := ws.Audio()
	gotFinalBeforeAudio := false
	select {
	case chunk := <-audioCh:
		if chunk.Final && len(chunk.MuLaw) == 0 {
			gotFinalBeforeAudio = true
		}
	case <-time.After(100 * time.Millisecond):
	}

	if gotFinalBeforeAudio {
		t.Error("got Final chunk before audio was sent - race condition!")
	}

	close(respondCh)

	timeout := time.After(2 * time.Second)
	var gotFinal bool
loop:
	for {
		select {
		case chunk := <-audioCh:
			if chunk.Final {
				gotFinal = true
				break loop
			}
		case <-timeout:
			break loop
		}
	}

	if !gotFinal {
		t.Error("expected Final chunk after audio completes")
	}
}

func TestSarvamRESTStream_EmptySpeakWaitsForInFlight(t *testing.T) {
	var callCount atomic.Int32
	respondCh := make(chan struct{})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount.Add(1)
		<-respondCh
		pcm := make([]byte, 320)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"audios": []string{base64.StdEncoding.EncodeToString(pcm)},
		})
	}))
	defer srv.Close()

	provider := &SarvamTTSProvider{
		apiKey:  "test",
		baseURL: srv.URL,
		model:   "bulbul:v3",
		speaker: "amit",
		lang:    "hi-IN",
		client:  srv.Client(),
		logger:  slog.Default(),
	}

	s := &sarvamTTSStream{
		provider:   provider,
		meta:       TTSSessionMeta{StreamSID: "rest-race-test"},
		sampleRate: 8000,
		audio:      make(chan TTSAudioChunk, defaultTTSAudioBuffer),
		done:       make(chan struct{}),
		cancelled:  make(map[string]struct{}),
		turnSeq:    make(map[string]int),
		turnVoice:  make(map[string]sarvamTurnVoice),
		inFlight:   make(map[string]int),
		pendingEnd: make(map[string]bool),
	}

	if err := s.Speak("turn-1", "hello world"); err != nil {
		t.Fatalf("Speak text failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	if callCount.Load() < 1 {
		t.Fatal("REST call not started")
	}

	if err := s.Speak("turn-1", ""); err != nil {
		t.Fatalf("Speak empty failed: %v", err)
	}

	gotFinalEarly := false
	select {
	case chunk := <-s.audio:
		if chunk.Final && len(chunk.MuLaw) == 0 {
			gotFinalEarly = true
		}
	case <-time.After(50 * time.Millisecond):
	}

	if gotFinalEarly {
		t.Error("got Final before synthesis completed - race condition!")
	}

	close(respondCh)

	timeout := time.After(2 * time.Second)
	var hasAudio, hasFinal bool
loop:
	for {
		select {
		case chunk := <-s.audio:
			if len(chunk.MuLaw) > 0 {
				hasAudio = true
			}
			if chunk.Final {
				hasFinal = true
				break loop
			}
		case <-timeout:
			break loop
		}
	}

	if !hasAudio {
		t.Error("no audio chunks received")
	}
	if !hasFinal {
		t.Error("no final chunk after synthesis completed")
	}
}

func TestSarvamTTSPaceFromEnv(t *testing.T) {
	t.Setenv("SARVAM_TTS_PACE", "1.5")
	pace := SarvamTTSDefaultPace()
	if pace == nil || *pace != 1.5 {
		t.Errorf("pace = %v, want 1.5", pace)
	}

	t.Setenv("SARVAM_TTS_PACE", "")
	pace = SarvamTTSDefaultPace()
	if pace != nil {
		t.Errorf("pace = %v, want nil", pace)
	}
}

func TestSarvamTTSStreamingEnabled(t *testing.T) {
	t.Setenv("SARVAM_TTS_STREAMING", "")
	if !SarvamTTSStreamingEnabled() {
		t.Error("default should be enabled")
	}

	t.Setenv("SARVAM_TTS_STREAMING", "false")
	if SarvamTTSStreamingEnabled() {
		t.Error("should be disabled when false")
	}

	t.Setenv("SARVAM_TTS_STREAMING", "1")
	if !SarvamTTSStreamingEnabled() {
		t.Error("should be enabled when 1")
	}
}
