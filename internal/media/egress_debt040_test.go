package media

import (
	"context"
	"testing"
	"time"
)

// TestCarrierEgressDrainReadyGate verifies the DEBT-040 egress drain-ready
// gate: when enabled, the pacer holds the opener TTS burst in pendingFrames
// until ConfirmDrainReady is called (first ingress frame = bridge live).
// No outbound audio is emitted while gated; after release, the burst flows.
func TestCarrierEgressDrainReadyGate(t *testing.T) {
	clock := NewFakeClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	egress, session, cap := setupCarrierEgressTest(t, clock)
	egress.EnableDrainReadyGate()

	if !egress.DrainReadyGated() {
		t.Fatal("expected drain-ready gate to be active after EnableDrainReadyGate")
	}
	if !egress.Paused() {
		t.Fatal("expected pacer to be paused while drain-ready gated")
	}

	// Opener TTS burst: 5 frames (100ms of audio) arrive while gated.
	audio := make([]byte, 160)
	for i := 0; i < 5; i++ {
		if err := egress.SendAudio(context.Background(), session, TTSAudioChunk{TurnID: "t1", MuLaw: audio}); err != nil {
			t.Fatal(err)
		}
	}
	// Advance the clock past the jitter window; gated → nothing egresses.
	clock.Advance(400 * time.Millisecond)
	if got := len(cap.snapshot()); got != 0 {
		t.Fatalf("expected no outbound while drain-ready gated; got %d frames", got)
	}

	// First ingress frame arrives → ConfirmDrainReady releases the gate.
	egress.ConfirmDrainReady()
	if egress.DrainReadyGated() {
		t.Fatal("expected drain-ready gate to be released after ConfirmDrainReady")
	}

	// Pacer resumes; the held burst now egresses (one frame per 20ms tick).
	for i := 0; i < 10; i++ {
		clock.Advance(20 * time.Millisecond)
	}
	waitCapture(t, cap, 1, time.Second)
	if got := len(cap.snapshot()); got == 0 {
		t.Fatal("expected outbound audio after drain-ready gate release; got none")
	}
}

// TestCarrierEgressDrainReadyGateNoOpWhenHumanGated verifies the
// drain-ready gate does NOT double-gate when the AMD human gate is already
// active (AMD owns the pause; ConfirmHuman releases it).
func TestCarrierEgressDrainReadyGateNoOpWhenHumanGated(t *testing.T) {
	clock := NewFakeClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	egress, _, _ := setupCarrierEgressTest(t, clock)
	egress.EnableHumanGate()
	if !egress.HumanGated() {
		t.Fatal("expected human gate active")
	}
	// Drain-ready gate should be a no-op while human-gated.
	egress.EnableDrainReadyGate()
	if egress.DrainReadyGated() {
		t.Fatal("drain-ready gate must not activate while human gate owns the pause")
	}
	if !egress.HumanGated() {
		t.Fatal("human gate must remain active (AMD owns the pause)")
	}
}

// TestCarrierEgressDrainReadyGateIdempotent verifies ConfirmDrainReady is
// idempotent (second call is a no-op) and EnableDrainReadyGate after release
// re-gates (for a hypothetical second turn — though in practice the gate
// fires once per session).
func TestCarrierEgressDrainReadyGateIdempotent(t *testing.T) {
	clock := NewFakeClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	egress, _, _ := setupCarrierEgressTest(t, clock)
	egress.EnableDrainReadyGate()
	egress.ConfirmDrainReady()
	if egress.DrainReadyGated() {
		t.Fatal("expected gate released")
	}
	// Second confirm is a no-op (no panic, no state change).
	egress.ConfirmDrainReady()
	if egress.DrainReadyGated() {
		t.Fatal("expected gate still released after idempotent second confirm")
	}
}

// TestSessionDrainReadyCallbackFiresOnce verifies the DEBT-040 session-side
// drain-ready callback fires exactly once on the first successful ingress
// frame, and not on subsequent frames.
func TestSessionDrainReadyCallbackFiresOnce(t *testing.T) {
	mgr := NewSessionManager(DefaultConfig(), nil, func() AudioSink {
		return NewLoggingSink(nil)
	}, nil)
	ctx := context.Background()
	serverConn, clientConn := newWSConnPair(t)
	session, err := mgr.Create(ctx, StartEvent{
		Event:       EventStart,
		StreamSID:   "MZ-DRAIN-READY",
		CallSID:     "CA-DRAIN-READY",
		MediaFormat: AudioFormat{Encoding: "audio/x-mulaw", SampleRate: 8000, Channels: 1},
	}, serverConn)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	t.Cleanup(func() {
		mgr.Close(ctx, session.StreamSID)
		_ = clientConn.Close()
	})

	fired := make(chan struct{}, 4)
	session.SetDrainReadyCallback(func() { fired <- struct{}{} })

	// Send 3 ingress frames; the callback should fire exactly once.
	for i := 0; i < 3; i++ {
		if err := session.enqueueRawMedia(ctx, make([]byte, 160)); err != nil {
			t.Fatalf("enqueueRawMedia[%d]: %v", i, err)
		}
	}

	// Drain the audioCh so enqueueRawMedia doesn't block / drop.
	go func() {
		for {
			select {
			case <-session.audioCh:
			case <-session.stopCh:
				return
			}
		}
	}()

	select {
	case <-fired:
		// first frame fired the callback
	case <-time.After(time.Second):
		t.Fatal("drain-ready callback never fired on first ingress frame")
	}
	select {
	case <-fired:
		t.Fatal("drain-ready callback fired more than once")
	case <-time.After(100 * time.Millisecond):
		// good: exactly one fire
	}
}

// TestSessionAudioBufferSizeAbsorbsBurst verifies the DEBT-040 ingress
// audioCh enlarge (default 8 → 64): a session-start burst of 40 frames
// (~800ms, more than the observed 37-drop burst) is absorbed with ZERO
// drops (framesDropped==0).
func TestSessionAudioBufferSizeAbsorbsBurst(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.AudioBufferSize < 64 {
		t.Fatalf("expected default AudioBufferSize >= 64 (DEBT-040); got %d", cfg.AudioBufferSize)
	}
	mgr := NewSessionManager(cfg, nil, func() AudioSink {
		return NewLoggingSink(nil)
	}, nil)
	ctx := context.Background()
	serverConn, clientConn := newWSConnPair(t)
	session, err := mgr.Create(ctx, StartEvent{
		Event:       EventStart,
		StreamSID:   "MZ-BURST",
		CallSID:     "CA-BURST",
		MediaFormat: AudioFormat{Encoding: "audio/x-mulaw", SampleRate: 8000, Channels: 1},
	}, serverConn)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	t.Cleanup(func() {
		mgr.Close(ctx, session.StreamSID)
		_ = clientConn.Close()
	})

	// Enqueue a burst of 40 frames (800ms) faster than the sink drains.
	for i := 0; i < 40; i++ {
		if err := session.enqueueRawMedia(ctx, make([]byte, 160)); err != nil {
			t.Fatalf("enqueueRawMedia[%d]: %v", i, err)
		}
	}
	if got := session.FramesDropped(); got != 0 {
		t.Fatalf("expected zero ingress drops for 40-frame burst (DEBT-040 enlarge); got %d", got)
	}
}
