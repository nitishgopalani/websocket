package translator_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"websocket/internal/media"
	"websocket/internal/translator"
)

type slowMockTTS struct {
	mu        sync.Mutex
	audio     chan media.TTSAudioChunk
	cancelled map[string]struct{}
	speakN    atomic.Int32
	active    atomic.Int32
}

func newSlowMockTTS() *slowMockTTS {
	return &slowMockTTS{
		audio:     make(chan media.TTSAudioChunk, 32),
		cancelled: make(map[string]struct{}),
	}
}

func (m *slowMockTTS) Speak(turnID, _ string) error {
	m.speakN.Add(1)
	m.active.Add(1)
	go func() {
		defer m.active.Add(-1)
		for i := 0; i < 5; i++ {
			if m.isCancelled(turnID) {
				return
			}
			frame := []byte{byte(turnID[0]), byte(i), 0, 0}
			select {
			case m.audio <- media.TTSAudioChunk{TurnID: turnID, Seq: i + 1, MuLaw: frame}:
			default:
			}
			time.Sleep(15 * time.Millisecond)
		}
		if !m.isCancelled(turnID) {
			m.audio <- media.TTSAudioChunk{TurnID: turnID, Seq: 99, Final: true}
		}
	}()
	return nil
}

func (m *slowMockTTS) isCancelled(turnID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.cancelled[turnID]
	return ok
}

func (m *slowMockTTS) Cancel(turnID string) error {
	m.mu.Lock()
	m.cancelled[turnID] = struct{}{}
	m.mu.Unlock()
	return nil
}

func (m *slowMockTTS) Audio() <-chan media.TTSAudioChunk { return m.audio }
func (m *slowMockTTS) Close() error                   { close(m.audio); return nil }

type trackingEgress struct {
	mu           sync.Mutex
	chunks       []media.TTSAudioChunk
	clearCount   int
	lastClearHad int
}

func (e *trackingEgress) SendAudio(_ context.Context, _ *media.Session, chunk media.TTSAudioChunk) error {
	e.mu.Lock()
	e.chunks = append(e.chunks, chunk)
	e.mu.Unlock()
	return nil
}

func (e *trackingEgress) Mark(context.Context, *media.Session, string) error { return nil }

func (e *trackingEgress) ClearPlayback(context.Context, *media.Session) error {
	e.mu.Lock()
	e.clearCount++
	e.lastClearHad = len(e.chunks)
	e.chunks = nil
	e.mu.Unlock()
	return nil
}

func (e *trackingEgress) snapshot() (chunks []media.TTSAudioChunk, clears int, turnIDs map[string]int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	chunks = append([]media.TTSAudioChunk(nil), e.chunks...)
	clears = e.clearCount
	turnIDs = make(map[string]int)
	for _, c := range e.chunks {
		turnIDs[c.TurnID]++
	}
	return chunks, clears, turnIDs
}

func TestTTSPlayer_overlappingSpeak_cancelsPreviousAndClearsEgress(t *testing.T) {
	tts := newSlowMockTTS()
	egress := &trackingEgress{}
	target := &media.Session{StreamSID: "leg-b"}
	player := translator.NewTTSPlayerForTest(tts, egress, target, nil)

	ctx := context.Background()
	if err := player.Speak(ctx, "turn-a", "hello"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if err := player.Speak(ctx, "turn-b", "world"); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		_, _, ids := egress.snapshot()
		if ids["turn-b"] > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	chunks, clears, ids := egress.snapshot()
	if clears < 1 {
		t.Fatalf("clearCount=%d want >=1", clears)
	}
	if ids["turn-a"] > 0 {
		t.Fatalf("stale turn-a chunks=%d want 0", ids["turn-a"])
	}
	if ids["turn-b"] == 0 {
		t.Fatalf("turn-b chunks=%v", chunks)
	}
	if tts.speakN.Load() != 2 {
		t.Fatalf("speakN=%d want 2", tts.speakN.Load())
	}
	tts.mu.Lock()
	if _, ok := tts.cancelled["turn-a"]; !ok {
		t.Fatal("expected turn-a cancelled")
	}
	tts.mu.Unlock()
}

func TestTTSPlayer_rapidSpeak_clearsBetweenUtterances(t *testing.T) {
	tts := newSlowMockTTS()
	egress := &trackingEgress{}
	player := translator.NewTTSPlayerForTest(tts, egress, &media.Session{StreamSID: "b"}, nil)
	ctx := context.Background()
	_ = player.Speak(ctx, "t1", "one")
	_ = player.Speak(ctx, "t2", "two")
	_ = player.Speak(ctx, "t3", "three")
	_, clears, _ := egress.snapshot()
	if clears < 2 {
		t.Fatalf("clearCount=%d want >=2 between utterances", clears)
	}
}
