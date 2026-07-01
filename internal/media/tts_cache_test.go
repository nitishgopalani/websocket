package media

import (
	"sync"
	"testing"
	"time"
)

// fakeInnerStream synthesizes deterministic frames per Speak and records calls,
// so we can assert the cache serves repeats without hitting the inner stream.
type fakeInnerStream struct {
	audio chan TTSAudioChunk
	mu    sync.Mutex
	spoke []string
}

func newFakeInnerStream() *fakeInnerStream {
	return &fakeInnerStream{audio: make(chan TTSAudioChunk, 16)}
}

func (f *fakeInnerStream) Speak(turnID, text string) error {
	f.mu.Lock()
	f.spoke = append(f.spoke, text)
	f.mu.Unlock()
	go func() {
		f.audio <- TTSAudioChunk{TurnID: turnID, Seq: 0, MuLaw: []byte{1, 2, 3}}
		f.audio <- TTSAudioChunk{TurnID: turnID, Seq: 1, MuLaw: []byte{4, 5, 6}}
		f.audio <- TTSAudioChunk{TurnID: turnID, Seq: 2, Final: true}
	}()
	return nil
}
func (f *fakeInnerStream) Cancel(string) error         { return nil }
func (f *fakeInnerStream) Audio() <-chan TTSAudioChunk { return f.audio }
func (f *fakeInnerStream) Close() error                { close(f.audio); return nil }

func (f *fakeInnerStream) speakCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.spoke)
}

// drainTurn collects audio frames for a turn until Final (or timeout).
func drainTurn(t *testing.T, ch <-chan TTSAudioChunk) [][]byte {
	t.Helper()
	var frames [][]byte
	deadline := time.After(2 * time.Second)
	for {
		select {
		case c := <-ch:
			if c.Final {
				return frames
			}
			if len(c.MuLaw) > 0 {
				frames = append(frames, c.MuLaw)
			}
		case <-deadline:
			t.Fatal("timed out waiting for Final chunk")
		}
	}
}

func TestCachingTTSStream_MissThenHit(t *testing.T) {
	inner := newFakeInnerStream()
	cache := NewTTSCache(16)
	cs := newCachingTTSStream(inner, "eleven|voice|hi|pcm_16000|16000", cache, nil, "sid1")
	out := cs.Audio()

	// First utterance: cache miss -> inner is used, audio flows through.
	if err := cs.Speak("t1", "aaj payment karein"); err != nil {
		t.Fatalf("speak1: %v", err)
	}
	f1 := drainTurn(t, out)
	if len(f1) != 2 {
		t.Fatalf("miss: expected 2 frames, got %d", len(f1))
	}
	if inner.speakCount() != 1 {
		t.Fatalf("miss: inner should have spoken once, got %d", inner.speakCount())
	}

	// Second identical utterance: cache hit -> inner NOT called again, same audio.
	if err := cs.Speak("t2", "aaj payment karein"); err != nil {
		t.Fatalf("speak2: %v", err)
	}
	f2 := drainTurn(t, out)
	if len(f2) != 2 {
		t.Fatalf("hit: expected 2 frames, got %d", len(f2))
	}
	if inner.speakCount() != 1 {
		t.Fatalf("hit: inner should still be 1 (served from cache), got %d", inner.speakCount())
	}

	hits, miss, entries := cache.Stats()
	if hits != 1 || miss != 1 || entries != 1 {
		t.Fatalf("stats: hits=%d miss=%d entries=%d (want 1/1/1)", hits, miss, entries)
	}
	_ = cs.Close()
}

func TestCachingTTSStream_CancelNotCached(t *testing.T) {
	inner := newFakeInnerStream()
	cache := NewTTSCache(16)
	cs := newCachingTTSStream(inner, "p", cache, nil, "sid2")

	cs.mu.Lock()
	cs.pending["tX"] = &pendingTurn{key: ttsCacheKey("p", "partial"), frames: [][]byte{{9}}}
	cs.mu.Unlock()
	if err := cs.Cancel("tX"); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	cs.mu.Lock()
	_, still := cs.pending["tX"]
	cs.mu.Unlock()
	if still {
		t.Fatal("cancel should drop the pending (partial) turn")
	}
	if _, ok := cache.Get(ttsCacheKey("p", "partial")); ok {
		t.Fatal("partial/cancelled audio must not be cached")
	}
	_ = cs.Close()
}
