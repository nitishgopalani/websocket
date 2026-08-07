package media

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// delayedTTSStream emits audio after a delay so OnReplyDone cannot finalize
// before first audio (W1 delayed-first-audio fixture).
type delayedTTSStream struct {
	audio    chan TTSAudioChunk
	delay    time.Duration
	mu       sync.Mutex
	closed   bool
	inFlight map[string]bool
}

func newDelayedTTSStream(delay time.Duration) *delayedTTSStream {
	return &delayedTTSStream{
		audio:    make(chan TTSAudioChunk, 8),
		delay:    delay,
		inFlight: make(map[string]bool),
	}
}

func (d *delayedTTSStream) Speak(turnID, text string) error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil
	}
	if trimEmpty(text) {
		if d.inFlight[turnID] {
			d.mu.Unlock()
			return nil
		}
		d.mu.Unlock()
		d.audio <- TTSAudioChunk{TurnID: turnID, Final: true}
		return nil
	}
	d.inFlight[turnID] = true
	d.mu.Unlock()
	go func() {
		time.Sleep(d.delay)
		d.audio <- TTSAudioChunk{TurnID: turnID, Seq: 0, MuLaw: []byte{0xFF, 0x7F}}
		d.mu.Lock()
		delete(d.inFlight, turnID)
		d.mu.Unlock()
		d.audio <- TTSAudioChunk{TurnID: turnID, Final: true}
	}()
	return nil
}

func trimEmpty(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] != ' ' && s[i] != '\t' && s[i] != '\n' && s[i] != '\r' {
			return false
		}
	}
	return true
}

func (d *delayedTTSStream) Cancel(_ string) error { return nil }
func (d *delayedTTSStream) Audio() <-chan TTSAudioChunk {
	return d.audio
}
func (d *delayedTTSStream) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.closed {
		d.closed = true
		close(d.audio)
	}
	return nil
}

type w1RecordingEgress struct {
	mu     sync.Mutex
	chunks []TTSAudioChunk
	marks  []string
}

func (e *w1RecordingEgress) SendAudio(_ context.Context, _ *Session, chunk TTSAudioChunk) error {
	e.mu.Lock()
	e.chunks = append(e.chunks, chunk)
	e.mu.Unlock()
	return nil
}
func (e *w1RecordingEgress) Mark(_ context.Context, _ *Session, turnID string) error {
	e.mu.Lock()
	e.marks = append(e.marks, turnID)
	e.mu.Unlock()
	return nil
}
func (e *w1RecordingEgress) ClearPlayback(context.Context, *Session) error { return nil }

type w1PlaybackDone struct {
	n atomic.Int32
}

func (p *w1PlaybackDone) NotifyPlaybackDone(_ *Session, _ string) { p.n.Add(1) }

func TestTTSReplyConsumer_DelayedFirstAudioNoEarlyFinalize(t *testing.T) {
	stream := newDelayedTTSStream(600 * time.Millisecond)
	defer stream.Close()

	egress := &w1RecordingEgress{}
	done := &w1PlaybackDone{}
	consumer := NewTTSReplyConsumer(stream, egress, nil, nil, nil)
	consumer.SetPlaybackDoneNotifier(done)
	hub := NewTurnTimingHub("sess-w1", nil, nil, nil, LatencyBudget{})
	consumer.SetObservability(hub, nil)

	session := &Session{StreamSID: "MZ-W1"}
	consumer.BindSession(session)
	ctx := context.Background()

	turnID := "turn-delayed"
	hub.BeginCallerTurn()
	turn := hub.BindEngineTurn(turnID, false)
	if turn == nil {
		t.Fatal("expected turn timing")
	}
	hub.MarkTurn(turnID, StageEngineFirstChunk)
	consumer.OnReplyChunk(ctx, session, turnID, 0, "Namaste ji.")
	consumer.OnReplyDone(ctx, session, turnID, false, "resolved")

	time.Sleep(200 * time.Millisecond)
	egress.mu.Lock()
	earlyMarks, earlyChunks := len(egress.marks), len(egress.chunks)
	egress.mu.Unlock()
	if earlyMarks != 0 {
		t.Fatalf("early egress marks = %d, want 0", earlyMarks)
	}
	if earlyChunks != 0 {
		t.Fatalf("early audio chunks = %d, want 0", earlyChunks)
	}
	if done.n.Load() != 0 {
		t.Fatal("playback_done fired before first audio")
	}

	deadline := time.Now().Add(3 * time.Second)
	for {
		egress.mu.Lock()
		ok := len(egress.marks) >= 1 && len(egress.chunks) >= 1
		egress.mu.Unlock()
		if ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timeout waiting for audio + mark")
		}
		time.Sleep(20 * time.Millisecond)
	}
	deadline = time.Now().Add(2 * time.Second)
	for done.n.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("timeout waiting for playback_done")
		}
		time.Sleep(20 * time.Millisecond)
	}

	d := turn.durations()
	if d.TTSMS <= 0 {
		t.Fatalf("tts_ms = %d, want > 0 (no early finalize before audio)", d.TTSMS)
	}
}
