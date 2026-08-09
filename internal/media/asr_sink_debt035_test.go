package media_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"websocket/internal/media"
)

// recordingASRSession records every SendAudio frame in order.
type recordingASRSession struct {
	mu     sync.Mutex
	frames [][]byte
	events chan media.ASREvent
	closed bool
}

func (s *recordingASRSession) SendAudio(pcm16 []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return media.ErrASRSessionClosed
	}
	// Copy so the test owns the slice (caller may reuse it).
	cp := make([]byte, len(pcm16))
	copy(cp, pcm16)
	s.frames = append(s.frames, cp)
	return nil
}
func (s *recordingASRSession) Events() <-chan media.ASREvent { return s.events }
func (s *recordingASRSession) Close() error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	if s.events != nil {
		close(s.events)
	}
	return nil
}

func (s *recordingASRSession) snapshot() [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([][]byte, len(s.frames))
	copy(out, s.frames)
	return out
}

// delayedOpenASRProvider hands out a recordingASRSession on Open. Open can be
// deferred until the test calls openReady (simulating the ASR WS setup window).
type delayedOpenASRProvider struct {
	mu       sync.Mutex
	session  *recordingASRSession
	openDone bool
}

func (p *delayedOpenASRProvider) Open(_ context.Context, _ media.ASRSessionMeta) (media.ASRSession, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.session = &recordingASRSession{events: make(chan media.ASREvent)}
	p.openDone = true
	return p.session, nil
}

// Item 4 (DEBT-035): frames arriving during the setup window (before OnStart
// completes / ASR WS ready) must be BUFFERED and drained to ASR in order once
// ready — not dropped. Zero backpressure drops.
func TestASRSinkDEBT035_SetupBufferingDrainsOnReady(t *testing.T) {
	provider := &delayedOpenASRProvider{}
	sink := media.NewASRSink(provider, &recordingConsumer{}, 8000, nil)
	session := &media.Session{StreamSID: "MZ-DEBT035", CallSID: "CA-DEBT035"}
	ctx := context.Background()

	// Send N frames BEFORE OnStart (ASR WS not ready). These must be buffered.
	const nSetup = 25
	setupFrames := make([][]byte, nSetup)
	for i := 0; i < nSetup; i++ {
		f := make([]byte, 320)
		f[0] = byte(i)
		f[1] = byte(i >> 8)
		setupFrames[i] = f
		if err := sink.OnAudio(ctx, session, f); err != nil {
			t.Fatalf("OnAudio[%d] pre-start: %v", i, err)
		}
	}

	// Now OnStart — ASR WS becomes ready; the setup buffer must drain in order.
	if err := sink.OnStart(ctx, session); err != nil {
		t.Fatalf("OnStart: %v", err)
	}

	// Send a live frame after ready — must append after the drained setup frames.
	liveFrame := make([]byte, 320)
	liveFrame[0] = 0xFF
	if err := sink.OnAudio(ctx, session, liveFrame); err != nil {
		t.Fatalf("OnAudio live: %v", err)
	}

	// Wait briefly for the drain + live send to settle.
	deadline := time.Now().Add(2 * time.Second)
	var got [][]byte
	for time.Now().Before(deadline) {
		got = provider.session.snapshot()
		if len(got) == nSetup+1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if len(got) != nSetup+1 {
		t.Fatalf("ASR received %d frames, want %d (setup buffer + 1 live)", len(got), nSetup+1)
	}

	// Verify order: setup frames first (in order), then the live frame.
	for i := 0; i < nSetup; i++ {
		if got[i][0] != byte(i) || got[i][1] != byte(i>>8) {
			t.Errorf("setup frame[%d] = [%d,%d], want [%d,%d] (order must be preserved)",
				i, got[i][0], got[i][1], byte(i), byte(i>>8))
		}
	}
	if got[nSetup][0] != 0xFF {
		t.Errorf("live frame = %v, want first byte 0xFF", got[nSetup][:1])
	}

	// Zero backpressure drops on the session.
	if dropped := session.FramesDropped(); dropped != 0 {
		t.Errorf("session FramesDropped = %d, want 0", dropped)
	}

	_ = sink.OnStop(ctx, session)
}

// Item 4 (DEBT-035) guard: if ASR never opens, the setup buffer must stay
// bounded (cap = asrSetupBufferMaxFrames). Oldest frames are dropped with a
// log, never an unbounded memory grow.
func TestASRSinkDEBT035_SetupBufferBoundedWhenASRFails(t *testing.T) {
	// A provider whose Open returns a session but we never call OnStart — so
	// asrSession stays nil and the setup buffer grows until the cap.
	provider := &delayedOpenASRProvider{}
	sink := media.NewASRSink(provider, &recordingConsumer{}, 8000, nil)
	session := &media.Session{StreamSID: "MZ-DEBT035-CAP", CallSID: "CA-DEBT035-CAP"}
	ctx := context.Background()

	// Send well over the cap (500) — the buffer must cap and drop oldest.
	const total = 700
	for i := 0; i < total; i++ {
		f := make([]byte, 32)
		f[0] = byte(i)
		if err := sink.OnAudio(ctx, session, f); err != nil {
			t.Fatalf("OnAudio[%d]: %v", i, err)
		}
	}

	// Now OnStart — drain whatever survived (the last cap frames).
	if err := sink.OnStart(ctx, session); err != nil {
		t.Fatalf("OnStart: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	var got [][]byte
	for time.Now().Before(deadline) {
		got = provider.session.snapshot()
		if len(got) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// The cap is 500 — at most 500 frames should have been drained.
	if len(got) > 500 {
		t.Errorf("drained frames = %d, want <= 500 (cap)", len(got))
	}
	// The first drained frame should be the (total-500)th sent frame, i.e. byte
	// (total-500) mod 256 = 200.
	if len(got) == 500 && got[0][0] != byte(total-500) {
		t.Errorf("first drained frame = %d, want %d (oldest dropped)", got[0][0], byte(total-500))
	}

	_ = sink.OnStop(ctx, session)
}
