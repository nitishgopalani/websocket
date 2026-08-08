package media

import (
	"sync"
	"testing"
	"time"
)

// fakeInnerStream synthesizes deterministic frames per non-empty Speak and records
// calls, so we can assert the cache serves repeats without hitting the inner stream.
// An empty Speak is a no-op flush (the batch already self-finalized on the prior
// non-empty Speak), mirroring the real Sarvam WS provider's flush semantics.
type fakeInnerStream struct {
	audio chan TTSAudioChunk
	mu    sync.Mutex
	spoke []string
}

func newFakeInnerStream() *fakeInnerStream {
	return &fakeInnerStream{audio: make(chan TTSAudioChunk, 64)}
}

func (f *fakeInnerStream) Speak(turnID, text string) error {
	if text == "" {
		return nil
	}
	f.mu.Lock()
	f.spoke = append(f.spoke, text)
	f.mu.Unlock()
	go func() {
		f.audio <- TTSAudioChunk{TurnID: turnID, Seq: 1, MuLaw: []byte{1, 2, 3}}
		f.audio <- TTSAudioChunk{TurnID: turnID, Seq: 2, MuLaw: []byte{4, 5, 6}}
		f.audio <- TTSAudioChunk{TurnID: turnID, Seq: 3, Final: true}
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
	deadline := time.After(3 * time.Second)
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

// drainTurnSeqs collects (seq) values and frames until Final, so tests can assert
// monotonic seq with no reset.
func drainTurnSeqs(t *testing.T, ch <-chan TTSAudioChunk) (seqs []int, frames [][]byte) {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		select {
		case c := <-ch:
			seqs = append(seqs, c.Seq)
			if c.Final {
				return seqs, frames
			}
			if len(c.MuLaw) > 0 {
				frames = append(frames, c.MuLaw)
			}
		case <-deadline:
			t.Fatal("timed out waiting for Final chunk")
		}
	}
}

func isMonotonicFrom1(seq []int) bool {
	if len(seq) == 0 {
		return false
	}
	for i, v := range seq {
		if v != i+1 {
			return false
		}
	}
	return true
}

// (c) single-chunk turn still caches+replays with seq from 1.
func TestCachingTTSStream_MissThenHit(t *testing.T) {
	inner := newFakeInnerStream()
	cache := NewTTSCache(16)
	cs := newCachingTTSStream(inner, "eleven|voice|hi|pcm_16000|16000", cache, nil, "sid1")
	out := cs.Audio()

	if err := cs.Speak("t1", "aaj payment karein"); err != nil {
		t.Fatalf("speak1: %v", err)
	}
	if err := cs.Speak("t1", ""); err != nil {
		t.Fatalf("flush1: %v", err)
	}
	seqs, f1 := drainTurnSeqs(t, out)
	if len(f1) != 2 {
		t.Fatalf("miss: expected 2 frames, got %d", len(f1))
	}
	if !isMonotonicFrom1(seqs) {
		t.Fatalf("miss: seq not monotonic from 1: %v", seqs)
	}
	if inner.speakCount() != 1 {
		t.Fatalf("miss: inner should have spoken once, got %d", inner.speakCount())
	}

	if err := cs.Speak("t2", "aaj payment karein"); err != nil {
		t.Fatalf("speak2: %v", err)
	}
	if err := cs.Speak("t2", ""); err != nil {
		t.Fatalf("flush2: %v", err)
	}
	seqs2, f2 := drainTurnSeqs(t, out)
	if len(f2) != 2 {
		t.Fatalf("hit: expected 2 frames, got %d", len(f2))
	}
	if !isMonotonicFrom1(seqs2) {
		t.Fatalf("hit: seq not monotonic from 1: %v", seqs2)
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

// (a) 4-chunk turn, chunk1 cached -> single monotonic seq stream, no reset, no dup audio.
func TestCachingTTSStream_MultiChunkChunk1Cached(t *testing.T) {
	inner := newFakeInnerStream()
	cache := NewTTSCache(16)
	cs := newCachingTTSStream(inner, "p", cache, nil, "sid-mc")
	out := cs.Audio()

	// Prime the cache for chunk1 by doing a single-chunk turn first.
	if err := cs.Speak("t0", "chunk1 text"); err != nil {
		t.Fatalf("prime speak: %v", err)
	}
	if err := cs.Speak("t0", ""); err != nil {
		t.Fatalf("prime flush: %v", err)
	}
	drainTurn(t, out)

	// Now a 4-chunk turn where chunk1 is cached, chunks 2-4 are live.
	if err := cs.Speak("t1", "chunk1 text"); err != nil {
		t.Fatalf("speak1: %v", err)
	}
	if err := cs.Speak("t1", "chunk2 text"); err != nil {
		t.Fatalf("speak2: %v", err)
	}
	if err := cs.Speak("t1", "chunk3 text"); err != nil {
		t.Fatalf("speak3: %v", err)
	}
	if err := cs.Speak("t1", "chunk4 text"); err != nil {
		t.Fatalf("speak4: %v", err)
	}
	if err := cs.Speak("t1", ""); err != nil {
		t.Fatalf("flush: %v", err)
	}
	seqs, frames := drainTurnSeqs(t, out)

	if len(frames) != 8 {
		t.Fatalf("expected 8 frames (2 cached + 6 live), got %d", len(frames))
	}
	if !isMonotonicFrom1(seqs) {
		t.Fatalf("seq not monotonic from 1 (reset/dup detected): %v", seqs)
	}
	if inner.speakCount() != 4 { // 1 prime + 3 live
		t.Fatalf("inner should synthesize 1 prime + 3 live = 4, got %d", inner.speakCount())
	}
	_ = cs.Close()
}

// (b) multi-chunk turn NOT recorded.
func TestCachingTTSStream_MultiChunkNotRecorded(t *testing.T) {
	inner := newFakeInnerStream()
	cache := NewTTSCache(16)
	cs := newCachingTTSStream(inner, "p", cache, nil, "sid-nr")
	out := cs.Audio()

	if err := cs.Speak("t1", "alpha"); err != nil {
		t.Fatalf("speak1: %v", err)
	}
	if err := cs.Speak("t1", "beta"); err != nil {
		t.Fatalf("speak2: %v", err)
	}
	if err := cs.Speak("t1", ""); err != nil {
		t.Fatalf("flush: %v", err)
	}
	drainTurn(t, out)

	hits, miss, entries := cache.Stats()
	if hits != 0 || miss == 0 || entries != 0 {
		t.Fatalf("multi-chunk should not be recorded: hits=%d miss=%d entries=%d", hits, miss, entries)
	}
	_ = cs.Close()
}

// (d) poisoned-entry regression: a pre-existing bad cache entry (whole-turn audio
// recorded under one chunk's key) must NOT cause double production. Serialization
// still yields one producer per turn — the poisoned entry is replayed once, then
// live chunks follow with continuing wrapper seq (no reset, no duplicate).
func TestCachingTTSStream_PoisonedEntryRegression(t *testing.T) {
	inner := newFakeInnerStream()
	cache := NewTTSCache(16)
	cs := newCachingTTSStream(inner, "p", cache, nil, "sid-poison")
	out := cs.Audio()

	// Simulate the OLD bug: poison the cache with a 4-frame entry keyed by chunk1
	// (as if a prior multi-chunk turn had recorded whole-turn audio under chunk1).
	poisonKey := ttsCacheKey("p", "chunk1 text")
	poisonFrames := [][]byte{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}, {10, 11, 12}}
	cache.Put(poisonKey, poisonFrames)

	// A 2-chunk turn: chunk1 hits the poisoned entry (replay 4 frames), chunk2 is live.
	if err := cs.Speak("t1", "chunk1 text"); err != nil {
		t.Fatalf("speak1: %v", err)
	}
	if err := cs.Speak("t1", "chunk2 text"); err != nil {
		t.Fatalf("speak2: %v", err)
	}
	if err := cs.Speak("t1", ""); err != nil {
		t.Fatalf("flush: %v", err)
	}
	seqs, frames := drainTurnSeqs(t, out)

	// Expect 4 (poisoned replay) + 2 (live chunk2) = 6 frames, single monotonic seq.
	if len(frames) != 6 {
		t.Fatalf("expected 6 frames (4 replay + 2 live), got %d", len(frames))
	}
	if !isMonotonicFrom1(seqs) {
		t.Fatalf("seq not monotonic from 1 (reset/dup detected): %v", seqs)
	}
	if inner.speakCount() != 1 {
		t.Fatalf("inner should synthesize only chunk2 (1 call), got %d", inner.speakCount())
	}
	_ = cs.Close()
}

// Cancel drops the in-flight turn session so partial audio is never recorded/replayed.
func TestCachingTTSStream_CancelDropsTurn(t *testing.T) {
	inner := newFakeInnerStream()
	cache := NewTTSCache(16)
	cs := newCachingTTSStream(inner, "p", cache, nil, "sid-cancel")

	// Enqueue a content segment (worker not started yet — no flush).
	if err := cs.Speak("tX", "partial text"); err != nil {
		t.Fatalf("speak: %v", err)
	}
	if err := cs.Cancel("tX"); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	cs.mu.Lock()
	_, still := cs.turns["tX"]
	cs.mu.Unlock()
	if still {
		t.Fatal("cancel should drop the in-flight turn session")
	}
	// Nothing was recorded (worker never ran; no flush).
	hits, miss, entries := cache.Stats()
	if hits != 0 || miss == 0 || entries != 0 {
		t.Fatalf("partial/cancelled audio must not be cached: hits=%d miss=%d entries=%d", hits, miss, entries)
	}
	_ = cs.Close()
}

