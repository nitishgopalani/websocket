package media

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

type warnCapture struct {
	mu    sync.Mutex
	msgs  []string
	attrs []map[string]any
}

func (w *warnCapture) Handle(_ context.Context, r slog.Record) error {
	if r.Level == slog.LevelWarn {
		w.mu.Lock()
		w.msgs = append(w.msgs, r.Message)
		m := map[string]any{}
		r.Attrs(func(a slog.Attr) bool {
			m[a.Key] = a.Value.Any()
			return true
		})
		w.attrs = append(w.attrs, m)
		w.mu.Unlock()
	}
	return nil
}
func (w *warnCapture) Enabled(context.Context, slog.Level) bool { return true }
func (w *warnCapture) WithAttrs([]slog.Attr) slog.Handler       { return w }
func (w *warnCapture) WithGroup(string) slog.Handler            { return w }

func fmtAny(v any) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	b, _ := json.Marshal(v)
	return string(b)
}

func TestRemapSpeakerV2(t *testing.T) {
	cases := map[string]string{
		"priya":         "anushka",
		"neha":          "manisha",
		"kabir":         "hitesh",
		"amit":          "karun",
		"Priya":         "anushka",
		"unknown-voice": FallbackSpeakerV2Default,
		"":              FallbackSpeakerV2Default,
	}
	for in, want := range cases {
		if got := RemapSpeakerV2(in); got != want {
			t.Fatalf("RemapSpeakerV2(%q)=%q want %q", in, got, want)
		}
	}
}

func TestSarvamTTS_v3FailureRetriesOnceWithRemappedV2(t *testing.T) {
	var mu sync.Mutex
	var calls []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var body map[string]any
		_ = json.Unmarshal(raw, &body)
		model, _ := body["model"].(string)
		speaker, _ := body["speaker"].(string)
		mu.Lock()
		calls = append(calls, model+"|"+speaker)
		n := len(calls)
		mu.Unlock()
		if n == 1 {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":{"message":"v3 boom"}}`))
			return
		}
		pcm := make([]byte, 320)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"audios": []string{base64.StdEncoding.EncodeToString(pcm)},
		})
	}))
	defer srv.Close()

	cap := &warnCapture{}
	p := &SarvamTTSProvider{
		apiKey:  "test",
		baseURL: srv.URL,
		model:   "bulbul:v3",
		speaker: "amit",
		lang:    "hi-IN",
		client:  srv.Client(),
		logger:  slog.New(cap),
	}
	s := &sarvamTTSStream{
		provider:   p,
		meta:       TTSSessionMeta{StreamSID: "sid-1", CallSID: "call-abc"},
		sampleRate: 8000,
		turnVoice:  map[string]sarvamTurnVoice{},
	}
	pcm, err := s.requestPCM("नमस्ते", "priya", "bulbul:v3", nil)
	if err != nil {
		t.Fatalf("requestPCM: %v", err)
	}
	if len(pcm) == 0 {
		t.Fatal("expected pcm from v2 fallback")
	}
	mu.Lock()
	got := append([]string{}, calls...)
	mu.Unlock()
	if len(got) != 2 {
		t.Fatalf("want exactly 2 HTTP calls, got %v", got)
	}
	if got[0] != "bulbul:v3|priya" {
		t.Fatalf("first call = %q", got[0])
	}
	if got[1] != "bulbul:v2|anushka" {
		t.Fatalf("fallback call = %q want bulbul:v2|anushka", got[1])
	}
	cap.mu.Lock()
	defer cap.mu.Unlock()
	if len(cap.msgs) == 0 || !strings.Contains(cap.msgs[0], "v3→v2 fallback") {
		t.Fatalf("expected fallback WARN, got %v", cap.msgs)
	}
	a := cap.attrs[0]
	if a["call_id"] != "call-abc" || a["original_speaker"] != "priya" || a["fallback_speaker"] != "anushka" {
		t.Fatalf("WARN attrs = %#v", a)
	}
	if !strings.Contains(fmtAny(a["v3_error"]), "v3 boom") {
		t.Fatalf("v3_error missing body: %#v", a["v3_error"])
	}
}

func TestSarvamTTS_v2FallbackFailureSurfacesNoLoop(t *testing.T) {
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n.Add(1)
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"still bad"}`))
	}))
	defer srv.Close()

	p := &SarvamTTSProvider{
		apiKey:  "test",
		baseURL: srv.URL,
		model:   "bulbul:v3",
		speaker: "amit",
		lang:    "hi-IN",
		client:  srv.Client(),
		logger:  slog.Default(),
	}
	s := &sarvamTTSStream{
		provider:   p,
		meta:       TTSSessionMeta{StreamSID: "sid-2"},
		sampleRate: 8000,
		turnVoice:  map[string]sarvamTurnVoice{},
	}
	_, err := s.requestPCM("hi", "neha", "bulbul:v3", nil)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "v2 fallback failed") {
		t.Fatalf("error = %v", err)
	}
	if n.Load() != 2 {
		t.Fatalf("want exactly 2 attempts (v3+v2), got %d", n.Load())
	}
}

func TestSarvamTTS_unknownSpeakerFallsBackToDefault(t *testing.T) {
	if RemapSpeakerV2("totally-unknown") != FallbackSpeakerV2Default {
		t.Fatal("unknown must map to _default abhilash")
	}
	var calls []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var body map[string]any
		_ = json.Unmarshal(raw, &body)
		calls = append(calls, body["model"].(string)+"|"+body["speaker"].(string))
		if len(calls) == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`rate`))
			return
		}
		pcm := make([]byte, 160)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"audios": []string{base64.StdEncoding.EncodeToString(pcm)},
		})
	}))
	defer srv.Close()

	p := &SarvamTTSProvider{
		apiKey: "t", baseURL: srv.URL, model: "bulbul:v3", speaker: "amit",
		lang: "hi-IN", client: srv.Client(), logger: slog.Default(),
	}
	s := &sarvamTTSStream{provider: p, meta: TTSSessionMeta{}, sampleRate: 8000, turnVoice: map[string]sarvamTurnVoice{}}
	if _, err := s.requestPCM("x", "mystery", "bulbul:v3", nil); err != nil {
		t.Fatal(err)
	}
	if len(calls) != 2 || calls[1] != "bulbul:v2|"+FallbackSpeakerV2Default {
		t.Fatalf("calls=%v", calls)
	}
}

func TestTTSCacheKeyIncludesModelAndVoice(t *testing.T) {
	prefix := strings.Join([]string{"sarvam", "amit", "bulbul:v3", "hi-IN", "pcm_8000", "8000"}, "|")
	k1 := ttsCacheKey(prefix+"|neha|bulbul:v3|", "hello")
	k2 := ttsCacheKey(prefix+"|manisha|bulbul:v2|", "hello")
	if k1 == k2 {
		t.Fatal("v3 and v2 voice/model must not share a cache key")
	}
}
