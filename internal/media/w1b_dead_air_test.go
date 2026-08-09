package media

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// W1-B.1 / W1-B.2 / W1-B.5 — H2 dead-air defense tests.

// fakeASRProvider pipes canned events into a session's Events channel.
type fakeASRProvider struct {
	events chan ASREvent
}

func (f *fakeASRProvider) Open(_ context.Context, _ ASRSessionMeta) (ASRSession, error) {
	return &fakeASRSession{events: f.events}, nil
}

type fakeASRSession struct {
	events chan ASREvent
}

func (s *fakeASRSession) SendAudio(_ []byte) error { return nil }
func (s *fakeASRSession) Events() <-chan ASREvent  { return s.events }
func (s *fakeASRSession) Close() error {
	close(s.events)
	return nil
}

// W1-B.1: ASR reconnect exhausted emits ASREventDead (terminal, not just error).

func TestSarvamReconnectExhaustedEmitsDeadEvent(t *testing.T) {
	// Use a provider whose dial always fails → connectLocked fails on every
	// attempt → reconnectFails increments → after MaxReconnects fails, giveUp
	// and emit ASREventDead (the failN path). This is faster and more robust
	// than a dropping-conn server (which lets dial succeed, hitting the
	// dialCount path with a 500ms readLoop sleep between cycles).
	provider := NewSarvamASRProvider("test-key", SarvamConfig{
		Endpoint:           "ws://127.0.0.1:1/unreachable",
		Model:              "saaras:v3",
		Mode:               "transcribe",
		ReconnectBaseDelay: 5 * time.Millisecond,
		ReconnectMaxDelay:  20 * time.Millisecond,
		MaxReconnects:     2,
	}, nil)
	// Override the dial to fail immediately (no real server needed).
	provider.dial = func(_ context.Context, _ string, _ http.Header) (*websocket.Conn, *http.Response, error) {
		return nil, nil, errors.New("dial refused")
	}

	sess, err := provider.Open(context.Background(), ASRSessionMeta{
		StreamSID:  "DEAD-REC",
		SampleRate: 8000,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer sess.Close()

	// Kick the lazy dial + reconnect loop. The first SendAudio returns the
	// dial error (buffered while disconnected); that's expected — ignore it.
	_ = sess.SendAudio([]byte{0x01, 0x00})

	deadline := time.Now().Add(4 * time.Second)
	var sawDead bool
	ch := sess.Events()
	for time.Now().Before(deadline) && !sawDead {
		select {
		case evt, ok := <-ch:
			if !ok {
				break
			}
			if evt.Type == ASREventDead {
				sawDead = true
				if evt.Err == nil {
					t.Error("ASREventDead carried nil error")
				}
			}
		case <-time.After(20 * time.Millisecond):
		}
	}
	if !sawDead {
		t.Fatalf("ASREventDead not emitted within deadline")
	}
}

// W1-B.1: ASRSink on ASREventDead logs asr_dead=true and invokes the listener.

type fakeDeadAirListener struct {
	called atomic.Int32
	mu     sync.Mutex
	got    *Session
}

func (f *fakeDeadAirListener) OnASRDead(_ context.Context, session *Session) {
	f.mu.Lock()
	f.got = session
	f.mu.Unlock()
	f.called.Add(1)
}

func TestASRSinkHandlesDeadEventAndInvokesListener(t *testing.T) {
	provider := &fakeASRProvider{events: make(chan ASREvent, 4)}
	sink := NewASRSink(provider, NewLoggingTranscriptConsumer(slog.Default()), 8000, nil)
	listener := &fakeDeadAirListener{}
	sink.SetDeadAirListener(listener)

	session := &Session{StreamSID: "DEAD-SINK", CallSID: "CALL-1"}
	if err := sink.OnStart(context.Background(), session); err != nil {
		t.Fatalf("OnStart: %v", err)
	}

	provider.events <- ASREvent{Type: ASREventDead, Err: errors.New("exhausted")}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && listener.called.Load() == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if listener.called.Load() != 1 {
		t.Fatalf("dead-air listener not invoked (called=%d)", listener.called.Load())
	}
	listener.mu.Lock()
	got := listener.got
	listener.mu.Unlock()
	if got == nil || got.StreamSID != "DEAD-SINK" {
		t.Errorf("listener got wrong session: %+v", got)
	}
	_ = sink.OnStop(context.Background(), session)
}

func TestASRSinkDeadEventLoggedWithoutListener(t *testing.T) {
	provider := &fakeASRProvider{events: make(chan ASREvent, 2)}
	sink := NewASRSink(provider, NewLoggingTranscriptConsumer(slog.Default()), 8000, nil)
	session := &Session{StreamSID: "DEAD-NOOP"}
	_ = sink.OnStart(context.Background(), session)
	provider.events <- ASREvent{Type: ASREventDead, Err: errors.New("exhausted")}
	time.Sleep(40 * time.Millisecond)
	_ = sink.OnStop(context.Background(), session)
	// No panic = pass.
}

// ---------------------------------------------------------------------------
// W1-B.2: TTS speak-fail ×2 → apology + graceful close
// ---------------------------------------------------------------------------

type failingTTSStream struct {
	mu          sync.Mutex
	speakCalls  atomic.Int32
	audio       chan TTSAudioChunk
	closed      bool
	// failFirstN: fail the first N non-empty Speaks, succeed after. 0 = never
	// fail (succeed always); -1 = always fail. Lets a test simulate "1st
	// Speak fails, holding-line Speak succeeds" without escalating to close.
	failFirstN int32
}

func newFailingTTSStream() *failingTTSStream {
	return &failingTTSStream{audio: make(chan TTSAudioChunk), failFirstN: -1}
}

func (f *failingTTSStream) Speak(_ string, text string) error {
	f.speakCalls.Add(1)
	if text == "" {
		return nil
	}
	n := f.speakCalls.Load()
	if f.failFirstN < 0 || n <= f.failFirstN {
		return errors.New("synthesis unavailable")
	}
	return nil
}
func (f *failingTTSStream) Cancel(_ string) error { return nil }
func (f *failingTTSStream) Audio() <-chan TTSAudioChunk {
	return f.audio
}
func (f *failingTTSStream) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.closed {
		f.closed = true
		close(f.audio)
	}
	return nil
}

func TestTTSConsecutiveSpeakFailTriggersApologyAndClose(t *testing.T) {
	tts := newFailingTTSStream() // failFirstN=-1 → always fail
	var endCallCalls atomic.Int32
	c := NewTTSReplyConsumer(tts, NewLoggingEgress(nil), nil, func(_ context.Context, _ *Session) {
		endCallCalls.Add(1)
	}, nil)
	c.SetHoldingLine("कृपया बनी रहिए।")
	c.SetApologyLine("माफ़ कीजिए, लाइन में तकनीकी समस्या आ रही है।", "abhilash")

	session := &Session{StreamSID: "FAIL-SINK", CallSID: "CALL-2"}
	ctx := context.Background()

	// 1st non-empty Speak fails → holding-line Speak also fails (always-fail
	// stream) → apology path → apology Speak fails (recursion guard) → close.
	// On a fully-dead TTS the call closes immediately; the recursion guard
	// prevents infinite apology retries.
	c.OnReplyChunk(ctx, session, "turn-1", 0, "आपका भुगतान 4500 रुपये है।")
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && endCallCalls.Load() == 0 {
		time.Sleep(20 * time.Millisecond)
	}
	if endCallCalls.Load() == 0 {
		t.Fatalf("fully-dead TTS should close the call via the apology path")
	}
	_ = c.Close()
}

func TestTTSFirstSpeakFailHoldingLineNoClose(t *testing.T) {
	// failFirstN=1: the 1st non-empty Speak fails, the holding-line Speak
	// (2nd call) succeeds → no escalation, no close. Counter resets on success.
	tts := newFailingTTSStream()
	tts.failFirstN = 1
	var endCallCalls atomic.Int32
	c := NewTTSReplyConsumer(tts, NewLoggingEgress(nil), nil, func(_ context.Context, _ *Session) {
		endCallCalls.Add(1)
	}, nil)
	c.SetHoldingLine("कृपया बनी रहिए।")
	c.SetApologyLine("माफ़ कीजिए।", "")

	session := &Session{StreamSID: "FAIL-1"}
	c.OnReplyChunk(context.Background(), session, "turn-1", 0, "नमस्ते।")
	time.Sleep(80 * time.Millisecond)
	if endCallCalls.Load() != 0 {
		t.Fatalf("1st failure + successful holding line should NOT close (closes=%d)", endCallCalls.Load())
	}
	c.mu.Lock()
	fails := c.consecutiveSpeakFails
	c.mu.Unlock()
	if fails != 0 {
		t.Errorf("counter should have reset to 0 after successful holding Speak, got %d", fails)
	}
	_ = c.Close()
}

// ---------------------------------------------------------------------------
// W1-B.5: noop-with-text — a noop TTS stream returns nil from Speak; no escalation
// ---------------------------------------------------------------------------

func TestTTSNoopWithTextNoEscalation(t *testing.T) {
	noop := NoopTTSProvider{}
	stream, err := noop.Open(context.Background(), TTSSessionMeta{})
	if err != nil {
		t.Fatalf("noop Open: %v", err)
	}
	c := NewTTSReplyConsumer(stream, NewLoggingEgress(nil), nil, func(_ context.Context, _ *Session) {
		t.Error("noop-with-text must NOT close the call")
	}, nil)
	c.SetHoldingLine("कृपया बनी रहिए।")
	c.SetApologyLine("माफ़ कीजिए।", "")

	session := &Session{StreamSID: "NOOP-SINK"}
	c.OnReplyChunk(context.Background(), session, "turn-1", 0, "यह एक noop परीक्षण है।")
	c.OnReplyDone(context.Background(), session, "turn-1", false, "noop")
	time.Sleep(500 * time.Millisecond)
	c.mu.Lock()
	fails := c.consecutiveSpeakFails
	c.mu.Unlock()
	if fails != 0 {
		t.Errorf("noop-with-text should keep consecutiveSpeakFails=0, got %d", fails)
	}
	// c.Close() calls stream.Close(); do NOT also defer stream.Close() (double-close panics).
	_ = c.Close()
}

// ---------------------------------------------------------------------------
// W1-B.5: simulated ASR-WS-kill → apology audio frames are produced
// ---------------------------------------------------------------------------

type capturingEgress struct {
	mu     sync.Mutex
	chunks []TTSAudioChunk
}

func (e *capturingEgress) SendAudio(_ context.Context, _ *Session, chunk TTSAudioChunk) error {
	e.mu.Lock()
	e.chunks = append(e.chunks, chunk)
	e.mu.Unlock()
	return nil
}
func (e *capturingEgress) Mark(_ context.Context, _ *Session, _ string) error { return nil }
func (e *capturingEgress) ClearPlayback(_ context.Context, _ *Session) error  { return nil }
func (e *capturingEgress) DropPending() int                                  { return 0 }

type synthesizingTTSStream struct {
	mu       sync.Mutex
	audio    chan TTSAudioChunk
	speakCnt atomic.Int32
	closed   bool
}

func newSynthTTSStream() *synthesizingTTSStream {
	return &synthesizingTTSStream{audio: make(chan TTSAudioChunk, 8)}
}

func (s *synthesizingTTSStream) Speak(turnID string, text string) error {
	if text == "" {
		return nil
	}
	s.speakCnt.Add(1)
	s.audio <- TTSAudioChunk{TurnID: turnID, Seq: 1, MuLaw: []byte{0x80}, Final: false}
	s.audio <- TTSAudioChunk{TurnID: turnID, Seq: 2, MuLaw: []byte{0x80}, Final: true}
	return nil
}
func (s *synthesizingTTSStream) Cancel(_ string) error { return nil }
func (s *synthesizingTTSStream) Audio() <-chan TTSAudioChunk {
	return s.audio
}
func (s *synthesizingTTSStream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		close(s.audio)
	}
	return nil
}

func TestSimulatedASRWSKillProducesApologyAudioFrames(t *testing.T) {
	tts := newSynthTTSStream()
	egress := &capturingEgress{}
	var endCallCalls atomic.Int32
	c := NewTTSReplyConsumer(tts, egress, nil, func(_ context.Context, _ *Session) {
		endCallCalls.Add(1)
	}, nil)
	apology := "माफ़ कीजिए, लाइन में तकनीकी समस्या आ रही है। हम आपसे थोड़ी देर में दोबारा संपर्क करेंगे। धन्यवाद।"
	c.SetApologyLine(apology, "abhilash")

	handler := NewDeadAirHandler(c, nil)

	provider := &fakeASRProvider{events: make(chan ASREvent, 2)}
	sink := NewASRSink(provider, NewLoggingTranscriptConsumer(nil), 8000, nil)
	sink.SetDeadAirListener(handler)

	session := &Session{StreamSID: "KILL-SINK", CallSID: "CALL-KILL"}
	_ = sink.OnStart(context.Background(), session)
	// Simulate the ASR WebSocket being killed → reconnect exhausted → Dead.
	provider.events <- ASREvent{Type: ASREventDead, Err: errors.New("ws killed")}

	// The apology Speak synthesizes 2 chunks → routeAudio egresses them.
	// Then OnReplyDone(end_call=true) → final-fallback → onEndCall.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		egress.mu.Lock()
		n := len(egress.chunks)
		egress.mu.Unlock()
		if n >= 2 && endCallCalls.Load() >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	egress.mu.Lock()
	got := egress.chunks
	egress.mu.Unlock()
	if len(got) < 2 {
		t.Fatalf("apology audio frames not produced (chunks=%d)", len(got))
	}
	if got[0].TurnID != "apology-dead-air" {
		t.Errorf("first apology chunk turn_id=%q, want apology-dead-air", got[0].TurnID)
	}
	if endCallCalls.Load() == 0 {
		t.Errorf("session should have been closed after the apology (endCallCalls=%d)", endCallCalls.Load())
	}
	_ = c.Close()
	_ = sink.OnStop(context.Background(), session)
}
