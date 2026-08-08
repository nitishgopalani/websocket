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

	// R3-TTS: Open no longer dials — the first Speak dials and sends the config
	// frame with the resolved voice. So config arrives AFTER Speak, not Open.
	if err := ws.Speak("turn-1", "नमस्ते"); err != nil {
		t.Fatalf("Speak failed: %v", err)
	}

	select {
	case cfg := <-configReceived:
		if cfg["speaker"] != "amit" {
			t.Errorf("config speaker = %v, want amit", cfg["speaker"])
		}
		if cfg["speech_sample_rate"] != float64(8000) {
			t.Errorf("config sample_rate = %v, want 8000", cfg["speech_sample_rate"])
		}
		if cfg["output_audio_codec"] != "linear16" {
			t.Errorf("output_audio_codec = %v, want linear16", cfg["output_audio_codec"])
		}
		if _, ok := cfg["enable_preprocessing"]; ok {
			t.Fatal("enable_preprocessing must not be sent for bulbul:v3 WS")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for config")
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

func TestSarvamWSStream_ErrorFrameTriggersRESTFallbackAndAudio(t *testing.T) {
	// F1: fake WS emits type:error after text → REST must synthesize; audio must egress (never silence).
	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	wsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
				// accept
			case "text":
				errFrame := map[string]any{
					"type": "error",
					"data": map[string]any{
						"message": "Input parameters has to be a valid dictionary",
						"code":    422,
					},
				}
				payload, _ := json.Marshal(errFrame)
				_ = conn.WriteMessage(websocket.TextMessage, payload)
			}
		}
	}))
	defer wsSrv.Close()

	var restCalls atomic.Int32
	restPCM := make([]byte, 640)
	for i := range restPCM {
		restPCM[i] = byte(i % 256)
	}
	restSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		restCalls.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"audios": []string{base64.StdEncoding.EncodeToString(restPCM)},
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

	wsURL := "ws" + strings.TrimPrefix(wsSrv.URL, "http")
	ws := newSarvamTTSWSStream(provider, TTSSessionMeta{StreamSID: "err-fb", CallSID: "c1"}, 8000)
	ws.wsURL = wsURL
	ws.logger = slog.New(cap)

	if err := ws.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer ws.Close()

	done := make(chan struct{})
	var chunks []TTSAudioChunk
	go func() {
		defer close(done)
		for c := range ws.Audio() {
			chunks = append(chunks, c)
		}
	}()

	if err := ws.Speak("turn-err", "नमस्ते"); err != nil {
		t.Fatalf("Speak: %v", err)
	}
	_ = ws.Speak("turn-err", "") // flush

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if restCalls.Load() >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if restCalls.Load() == 0 {
		t.Fatal("expected REST fallback after type:error frame")
	}

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		hasAudio, hasFinal := false, false
		for _, c := range chunks {
			if len(c.MuLaw) > 0 {
				hasAudio = true
			}
			if c.Final {
				hasFinal = true
			}
		}
		if hasAudio && hasFinal {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	hasAudio, hasFinal := false, false
	for _, c := range chunks {
		if len(c.MuLaw) > 0 {
			hasAudio = true
		}
		if c.Final {
			hasFinal = true
		}
	}
	if !hasAudio {
		t.Fatal("never-silence: expected audio chunks from REST fallback")
	}
	if !hasFinal {
		t.Fatal("expected Final chunk after REST fallback")
	}
	if ws.Path() != "rest" {
		t.Errorf("Path = %q, want rest", ws.Path())
	}

	cap.mu.Lock()
	found := false
	reasonOK := false
	for i, msg := range cap.msgs {
		if msg == "tts_ws_fallback" || strings.Contains(msg, "tts_ws_fallback") {
			found = true
			if a := cap.attrs[i]; a != nil {
				if r, ok := a["reason"].(string); ok && strings.Contains(r, "valid dictionary") {
					reasonOK = true
				}
			}
		}
	}
	cap.mu.Unlock()
	if !found {
		t.Error("expected WARN tts_ws_fallback")
	}
	if !reasonOK {
		t.Error("expected tts_ws_fallback reason to include error message")
	}

	_ = ws.Close()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
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

	// R3-TTS: Open no longer dials — the first Speak dials with the resolved
	// voice (here the provider default amit). So the initial connection happens
	// on Speak, not Open.
	if err := ws.Speak("turn-1", "hello"); err != nil {
		t.Fatalf("Speak 1 failed: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	initialConnects := connectCount.Load()
	if initialConnects < 1 {
		t.Fatal("expected at least 1 connection after first Speak")
	}

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
	if SarvamTTSStreamingEnabled() {
		t.Error("default should be disabled (REST primary until WS proven)")
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

// R3-TTS(b): the first Speak carrying a voice override != the env default must
// synthesize with the override directly — no voice-change event, no second
// connection. (This was broken independent of eager open: the eager open used
// the env default, then the first Speak's override triggered a reconnect.)
func TestSarvamWSStream_FirstSpeakVoiceOverrideNoReconnect(t *testing.T) {
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
		speaker: "amit", // env default
		lang:    "hi-IN",
		logger:  slog.Default(),
	}
	ws := newSarvamTTSWSStream(provider, TTSSessionMeta{StreamSID: "override-test", CallSID: "c-override"}, 8000)
	ws.wsURL = wsURL

	if err := ws.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer ws.Close()

	// Override the voice for turn-1 to priya (!= env default amit) BEFORE Speak.
	ws.SetTurnVoice("turn-1", "priya", "bulbul:v3", nil)
	if err := ws.Speak("turn-1", "नमस्ते"); err != nil {
		t.Fatalf("Speak: %v", err)
	}
	_ = ws.Speak("turn-1", "") // flush

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if lastConfig.Load() != nil && connectCount.Load() >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Exactly ONE connection — the override must NOT trigger a voice-change
	// reconnect (a reconnect here would mean the eager-open default was used
	// first, which is exactly the bug R3-TTS fixes).
	if got := connectCount.Load(); got != 1 {
		t.Errorf("connectCount = %d, want 1 (override must not trigger reconnect)", got)
	}
	cfg, ok := lastConfig.Load().(map[string]any)
	if !ok {
		t.Fatal("no config received")
	}
	if cfg["speaker"] != "priya" {
		t.Errorf("config speaker = %v, want priya (the override)", cfg["speaker"])
	}
}

// R3-TTS(c): a session with zero Speaks must not open a Sarvam connection at all.
func TestSarvamWSStream_ZeroSpeaksNoConnection(t *testing.T) {
	var connectCount atomic.Int32
	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		connectCount.Add(1)
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
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
	ws := newSarvamTTSWSStream(provider, TTSSessionMeta{StreamSID: "zero-speak"}, 8000)
	ws.wsURL = wsURL

	if err := ws.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}
	// No Speak calls at all — give the (would-be) dial time to misbehave.
	time.Sleep(250 * time.Millisecond)
	if got := connectCount.Load(); got != 0 {
		t.Errorf("connectCount = %d, want 0 (zero Speaks must not connect)", got)
	}
	_ = ws.Close()
	time.Sleep(100 * time.Millisecond)
	if got := connectCount.Load(); got != 0 {
		t.Errorf("connectCount after close = %d, want 0", got)
	}
}

// TestSarvamWSStream_HoldTurnInheritsParentVoice verifies that a dead-air
// watchdog holding turn (turnID + ":hold") inherits the parent turn's
// resolved voice instead of falling back to the env default speaker. Without
// this, the holding line ("ek minute") switches the voice mid-call (e.g.
// priya -> amit), which is exactly the R3-TTS regression we saw on UAT.
func TestSarvamWSStream_HoldTurnInheritsParentVoice(t *testing.T) {
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
		speaker: "amit", // env default
		lang:    "hi-IN",
		logger:  slog.Default(),
	}
	ws := newSarvamTTSWSStream(provider, TTSSessionMeta{StreamSID: "hold-inherit"}, 8000)
	ws.wsURL = wsURL

	if err := ws.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer ws.Close()

	// Parent turn uses priya (override != env default).
	ws.SetTurnVoice("turn-3", "priya", "bulbul:v3", nil)
	if err := ws.Speak("turn-3", "नमस्ते"); err != nil {
		t.Fatalf("Speak turn-3: %v", err)
	}
	// Holding turn (turn-3:hold) — no SetTurnVoice of its own. It must inherit
	// priya from turn-3 so the holding line doesn't switch to amit.
	if err := ws.Speak("turn-3:hold", "ek minute"); err != nil {
		t.Fatalf("Speak turn-3:hold: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if lastConfig.Load() != nil && connectCount.Load() >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	// The hold turn inherits priya, so the speaker is UNCHANGED from turn-3.
	// A cancel/reopen on the new Speak is expected (Sarvam WS has no cancel
	// message), but a VOICE-CHANGE reconnect must NOT fire — connectCount
	// stays at 2 (turn-3 + cancel/reopen for turn-3:hold), not 3.
	if got := connectCount.Load(); got > 2 {
		t.Errorf("connectCount = %d, want <= 2 (hold turn must not trigger a voice-change reconnect)", got)
	}
	cfg, ok := lastConfig.Load().(map[string]any)
	if !ok {
		t.Fatal("no config received")
	}
	if cfg["speaker"] != "priya" {
		t.Errorf("hold turn speaker = %v, want priya (inherited from parent)", cfg["speaker"])
	}
}
