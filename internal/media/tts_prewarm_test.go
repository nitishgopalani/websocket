package media

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakePreWarmStream emits one audio chunk then a Final on flush, so the
// caching wrapper records the segment.
type fakePreWarmStream struct {
	mu       sync.Mutex
	voice    string
	text     string
	out      chan TTSAudioChunk
	flushed  bool
	closed   bool
}

func newFakePreWarmStream() *fakePreWarmStream {
	return &fakePreWarmStream{out: make(chan TTSAudioChunk, 8)}
}

func (s *fakePreWarmStream) Speak(turnID string, text string) error {
	text = strings.TrimSpace(text)
	if text != "" {
		s.mu.Lock()
		s.text = text
		s.mu.Unlock()
		return nil
	}
	// flush
	s.mu.Lock()
	if !s.flushed {
		s.flushed = true
		s.out <- TTSAudioChunk{TurnID: turnID, Seq: 0, MuLaw: []byte{0x01, 0x02}, Final: false}
		s.out <- TTSAudioChunk{TurnID: turnID, Seq: 1, MuLaw: []byte{0x03, 0x04}, Final: true}
	}
	s.mu.Unlock()
	return nil
}
func (s *fakePreWarmStream) Cancel(turnID string) error { return nil }
func (s *fakePreWarmStream) Audio() <-chan TTSAudioChunk { return s.out }
func (s *fakePreWarmStream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		close(s.out)
	}
	return nil
}
func (s *fakePreWarmStream) SetTurnVoice(turnID, voiceID, model string, pace *float64) {
	s.mu.Lock()
	s.voice = voiceID
	s.mu.Unlock()
}

type fakePreWarmProvider struct {
	mu      sync.Mutex
	opens   int
	streams []*fakePreWarmStream
}

func (p *fakePreWarmProvider) Open(_ context.Context, _ TTSSessionMeta) (TTSStream, error) {
	s := newFakePreWarmStream()
	p.mu.Lock()
	p.opens++
	p.streams = append(p.streams, s)
	p.mu.Unlock()
	return s, nil
}

// Item 3 (DEBT-034): PreWarmTTS synthesizes each line into the global TTS cache
// so the first live call hits the cache. Verify the helper warms 3 lines and
// the global cache gains 3 entries (one per distinct text+voice key).
func TestPreWarmTTS_WarmsCache(t *testing.T) {
	provider := &fakePreWarmProvider{}
	base := TTSConfig{
		Enabled:      true,
		Provider:     "fake",
		VoiceID:      "amit",
		Model:        "bulbul:v3",
		Language:     "hi-IN",
		OutputFormat: "ulaw_8000",
	}
	lines := []TTSPreWarmLine{
		{Voice: "simran", Model: "bulbul:v3", Language: "hi-IN", Text: "नमस्ते, मैं अंजली पैसालो से बोल रही हूँ।"},
		{Voice: "simran", Model: "bulbul:v3", Language: "hi-IN", Text: "क्या मेरी बात रमेश जी से हो रही है?"},
		{Voice: "neha", Model: "bulbul:v3", Language: "hi-IN", Text: "apology line"},
	}

	cache := GlobalTTSCache()
	_, _, entriesBefore := cache.Stats()

	logger := slog.New(slog.NewTextHandler(&strings.Builder{}, nil))
	warmed, warmMs := PreWarmTTS(context.Background(), provider, base, lines, logger)
	if warmed != 3 {
		t.Errorf("warmed = %d, want 3", warmed)
	}
	if warmMs < 0 {
		t.Errorf("warmMs = %d, want >= 0", warmMs)
	}
	// The global cache should have gained 3 entries (one per distinct
	// text+voice key). Other tests may have added entries, so check the delta.
	_, _, entriesAfter := cache.Stats()
	if got := entriesAfter - entriesBefore; got != 3 {
		t.Errorf("cache entries delta = %d, want 3 (got %d before, %d after)", got, entriesBefore, entriesAfter)
	}
	// The provider was opened once per line.
	if got := provider.opens; got != 3 {
		t.Errorf("provider opens = %d, want 3", got)
	}
}

// Item 3 (DEBT-034): PreWarmTTS with no lines or nil provider is a no-op.
func TestPreWarmTTS_NoOpWhenEmpty(t *testing.T) {
	provider := &fakePreWarmProvider{}
	base := TTSConfig{Enabled: true, Provider: "fake", VoiceID: "amit", Model: "bulbul:v3", Language: "hi-IN"}
	warmed, _ := PreWarmTTS(context.Background(), provider, base, nil, slog.Default())
	if warmed != 0 {
		t.Errorf("warmed = %d, want 0 for empty lines", warmed)
	}
	warmed, _ = PreWarmTTS(context.Background(), nil, base, []TTSPreWarmLine{{Text: "x"}}, slog.Default())
	if warmed != 0 {
		t.Errorf("warmed = %d, want 0 for nil provider", warmed)
	}
}

// drain to avoid goroutine leak warnings on the fake stream's closed channel.
var _ = func() { _ = time.Second }
