package media

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeBargeInEgress struct {
	mu      sync.Mutex
	paused  bool
	clears  int
	order   []string
	onPause func()
}

func (f *fakeBargeInEgress) Pause() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.onPause != nil {
		f.onPause()
	}
	f.paused = true
	f.order = append(f.order, "pause")
}

func (f *fakeBargeInEgress) Resume() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.paused = false
	f.order = append(f.order, "resume")
}

func (f *fakeBargeInEgress) ClearPlayback(_ context.Context, _ *Session) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.clears++
	f.order = append(f.order, "clear")
	return nil
}

type fakeBargeInTTS struct {
	mu      sync.Mutex
	cancels []string
	order   []string
}

func (f *fakeBargeInTTS) CancelTTS(turnID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cancels = append(f.cancels, turnID)
	f.order = append(f.order, "tts")
}

type fakeEngineSession struct {
	mu      sync.Mutex
	cancels []string
	order   []string
}

func (f *fakeEngineSession) Cancel(turnID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cancels = append(f.cancels, turnID)
	f.order = append(f.order, "engine")
	return nil
}

type onsetSpeechVAD struct {
	speech bool
}

func (v *onsetSpeechVAD) IsSpeech(_ []byte, _ int) bool { return v.speech }
func (v *onsetSpeechVAD) Close() error                  { return nil }

func newBargeInTestHarness(t *testing.T) (*TurnManager, *BargeInHandler, *fakeBargeInEgress, *fakeBargeInTTS, *fakeEngineSession, *Session, *FakeClock) {
	t.Helper()
	clock := NewFakeClock(time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC))
	listener := NewLoggingTurnListener(nil)
	tm := NewTurnManager(listener, DefaultEndpointConfig(), clock, NoopVAD{}, nil, SemanticTurnConfig{}, NoopBackchannel{}, nil)
	egress := &fakeBargeInEgress{}
	tts := &fakeBargeInTTS{}
	engine := &fakeEngineSession{}
	handler := NewBargeInHandler(DefaultBargeInConfig(), egress, tts, engine, tm, clock, nil)
	tm.SetBargeInHandler(handler)
	session := &Session{StreamSID: "MZ-BI"}
	return tm, handler, egress, tts, engine, session, clock
}

func TestBargeInFastPauseOnLocalVADOnset(t *testing.T) {
	clock := NewFakeClock(time.Now())
	listener := NewLoggingTurnListener(nil)
	vad := &onsetSpeechVAD{speech: true}
	tm := NewTurnManager(listener, DefaultEndpointConfig(), clock, vad, nil, SemanticTurnConfig{}, NoopBackchannel{}, nil)

	var engineCalled atomic.Bool
	engine := &fakeEngineSession{}
	egress := &fakeBargeInEgress{
		onPause: func() {
			if engineCalled.Load() {
				t.Fatal("engine cancel before pause")
			}
		},
	}
	tts := &fakeBargeInTTS{}
	handler := NewBargeInHandler(DefaultBargeInConfig(), egress, tts, engine, tm, clock, nil)
	tm.SetBargeInHandler(handler)

	session := &Session{StreamSID: "MZ-FAST"}
	tm.SetAgentTurn(session, "agent-turn-1", true)

	tm.ObserveAudio(context.Background(), session, []byte{0, 1, 0, 1}, 8000)

	egress.mu.Lock()
	paused := egress.paused
	order := append([]string(nil), egress.order...)
	egress.mu.Unlock()
	if !paused {
		t.Fatal("expected egress paused synchronously on VAD onset")
	}
	if len(order) != 1 || order[0] != "pause" {
		t.Fatalf("order = %v, want [pause]", order)
	}
	engine.mu.Lock()
	n := len(engine.cancels)
	engine.mu.Unlock()
	if n != 0 {
		t.Fatalf("engine cancel = %d before classification", n)
	}
}

func TestBargeInBackchannelResumesWithoutCommit(t *testing.T) {
	tm, handler, egress, tts, engine, session, _ := newBargeInTestHarness(t)
	ctx := context.Background()
	lexicon := NewLexiconBackchannel(DefaultBackchannelConfig())

	tm.mu.Lock()
	tm.backchannel = lexicon
	tm.mu.Unlock()

	tm.SetAgentTurn(session, "agent-turn-2", true)
	handler.OnSpeechOnset(ctx, session, "agent-turn-2")
	tm.OnPartial(ctx, session, Transcript{Text: "haan ji"})
	tm.OnSpeechStart(ctx, session)

	egress.mu.Lock()
	clears := egress.clears
	resumed := egress.order
	egress.mu.Unlock()
	if clears != 0 {
		t.Fatalf("clears = %d, want 0", clears)
	}
	if handler.BackchannelsResumed() != 1 {
		t.Fatalf("resumed = %d, want 1", handler.BackchannelsResumed())
	}
	if len(resumed) < 2 || resumed[len(resumed)-1] != "resume" {
		t.Fatalf("egress order = %v, want resume", resumed)
	}
	tts.mu.Lock()
	defer tts.mu.Unlock()
	if len(tts.cancels) != 0 {
		t.Fatal("tts should not cancel on backchannel")
	}
	engine.mu.Lock()
	defer engine.mu.Unlock()
	if len(engine.cancels) != 0 {
		t.Fatal("engine should not cancel on backchannel")
	}
	if !tm.isAgentSpeaking() {
		t.Fatal("agent should still be speaking after backchannel resume")
	}
}

func TestBargeInRealContentCommits(t *testing.T) {
	tm, handler, egress, tts, engine, session, _ := newBargeInTestHarness(t)
	ctx := context.Background()
	lexicon := NewLexiconBackchannel(DefaultBackchannelConfig())
	tm.mu.Lock()
	tm.backchannel = lexicon
	tm.mu.Unlock()

	tm.SetAgentTurn(session, "agent-turn-3", true)
	handler.OnSpeechOnset(ctx, session, "agent-turn-3")
	tm.OnPartial(ctx, session, Transcript{Text: "wait I need to ask something"})
	tm.OnSpeechStart(ctx, session)

	if handler.BargeInsCommitted() != 1 {
		t.Fatalf("committed = %d, want 1", handler.BargeInsCommitted())
	}
	egress.mu.Lock()
	if egress.clears != 1 {
		t.Fatalf("clears = %d, want 1", egress.clears)
	}
	egress.mu.Unlock()
	tts.mu.Lock()
	if len(tts.cancels) != 1 || tts.cancels[0] != "agent-turn-3" {
		t.Fatalf("tts cancels = %v", tts.cancels)
	}
	tts.mu.Unlock()
	engine.mu.Lock()
	if len(engine.cancels) != 1 || engine.cancels[0] != "agent-turn-3" {
		t.Fatalf("engine cancels = %v", engine.cancels)
	}
	engine.mu.Unlock()
	if tm.isAgentSpeaking() {
		t.Fatal("agent should not be speaking after commit")
	}
}

func TestBargeInClassifyTimeoutCommits(t *testing.T) {
	tm, handler, egress, tts, engine, session, clock := newBargeInTestHarness(t)
	ctx := context.Background()
	tm.SetAgentTurn(session, "agent-turn-4", true)
	handler.OnSpeechOnset(ctx, session, "agent-turn-4")

	clock.Advance(301 * time.Millisecond)

	if handler.BargeInsCommitted() != 1 {
		t.Fatalf("committed = %d, want 1 on timeout", handler.BargeInsCommitted())
	}
	egress.mu.Lock()
	defer egress.mu.Unlock()
	if egress.clears != 1 {
		t.Fatalf("clears = %d, want 1", egress.clears)
	}
	tts.mu.Lock()
	defer tts.mu.Unlock()
	if len(tts.cancels) != 1 {
		t.Fatalf("tts cancels = %v", tts.cancels)
	}
	engine.mu.Lock()
	defer engine.mu.Unlock()
	if len(engine.cancels) != 1 {
		t.Fatalf("engine cancels = %v", engine.cancels)
	}
}

func TestBargeInNoActionWhenAgentNotSpeaking(t *testing.T) {
	tm, _, egress, tts, engine, session, _ := newBargeInTestHarness(t)
	vad := &onsetSpeechVAD{speech: true}
	tm.localVAD = vad

	tm.ObserveAudio(context.Background(), session, []byte{0, 1}, 8000)

	egress.mu.Lock()
	defer egress.mu.Unlock()
	if len(egress.order) != 0 {
		t.Fatalf("unexpected egress calls: %v", egress.order)
	}
	tts.mu.Lock()
	defer tts.mu.Unlock()
	engine.mu.Lock()
	defer engine.mu.Unlock()
}

func TestBargeInCommitIdempotent(t *testing.T) {
	tm, handler, egress, tts, engine, session, _ := newBargeInTestHarness(t)
	ctx := context.Background()
	tm.SetAgentTurn(session, "agent-turn-5", true)
	handler.OnSpeechOnset(ctx, session, "agent-turn-5")
	handler.OnClassified(ctx, session, "agent-turn-5", false)
	handler.OnClassified(ctx, session, "agent-turn-5", false)
	handler.commit(ctx, session, "agent-turn-5")

	if handler.BargeInsCommitted() != 1 {
		t.Fatalf("committed = %d, want 1", handler.BargeInsCommitted())
	}
	egress.mu.Lock()
	defer egress.mu.Unlock()
	if egress.clears != 1 {
		t.Fatalf("clears = %d, want 1", egress.clears)
	}
	tts.mu.Lock()
	defer tts.mu.Unlock()
	if len(tts.cancels) != 1 {
		t.Fatalf("tts cancels = %d, want 1", len(tts.cancels))
	}
	engine.mu.Lock()
	defer engine.mu.Unlock()
	if len(engine.cancels) != 1 {
		t.Fatalf("engine cancels = %d, want 1", len(engine.cancels))
	}
}

func TestBargeInDisabledRegressionUsesInterrupt(t *testing.T) {
	clock := NewFakeClock(time.Now())
	listener := &recordingTurnListener{}
	lexicon := NewLexiconBackchannel(DefaultBackchannelConfig())
	tm := NewTurnManager(listener, DefaultEndpointConfig(), clock, NoopVAD{}, nil, SemanticTurnConfig{}, lexicon, nil)
	cfg := DefaultBargeInConfig()
	cfg.Enabled = false
	handler := NewBargeInHandler(cfg, &fakeBargeInEgress{}, &fakeBargeInTTS{}, &fakeEngineSession{}, tm, clock, nil)
	tm.SetBargeInHandler(handler)

	session := &Session{StreamSID: "MZ-REG"}
	ctx := context.Background()
	tm.SetAgentTurn(session, "agent-turn-6", true)
	tm.OnPartial(ctx, session, Transcript{Text: "wait I have a question"})
	tm.OnSpeechStart(ctx, session)

	if len(filterEvents(listener.events, TurnInterrupt)) != 1 {
		t.Fatal("expected TurnInterrupt when barge-in disabled")
	}
}

func TestBargeInBackchannelErrorCommitsFailSafe(t *testing.T) {
	tm, handler, _, _, _, session, _ := newBargeInTestHarness(t)
	ctx := context.Background()
	bc := &fakeBackchannelClassifier{err: errors.New("down")}
	tm.mu.Lock()
	tm.backchannel = bc
	tm.mu.Unlock()

	tm.SetAgentTurn(session, "agent-turn-7", true)
	handler.OnSpeechOnset(ctx, session, "agent-turn-7")
	tm.OnPartial(ctx, session, Transcript{Text: "haan ji"})
	tm.OnSpeechStart(ctx, session)

	if handler.BargeInsCommitted() != 1 {
		t.Fatal("classifier error should fail-safe to commit")
	}
}

type fakeBackchannelClassifier struct {
	err error
}

func (f *fakeBackchannelClassifier) IsBackchannel(context.Context, string, []byte, int) (bool, error) {
	return false, f.err
}
func (f *fakeBackchannelClassifier) Close() error { return nil }

type recordingTurnListener struct {
	events []TurnEvent
}

func (l *recordingTurnListener) OnTurnEvent(_ context.Context, _ *Session, event TurnEvent) {
	l.events = append(l.events, event)
}

func filterEvents(events []TurnEvent, kind TurnKind) []TurnEvent {
	out := make([]TurnEvent, 0)
	for _, e := range events {
		if e.Kind == kind {
			out = append(out, e)
		}
	}
	return out
}

func TestCarrierEgressPauseResume(t *testing.T) {
	clock := NewFakeClock(time.Now())
	egress := NewCarrierEgress(DefaultEgressConfig(), 20, clock, nil, nil)
	egress.Pause()
	if !egress.Paused() {
		t.Fatal("expected paused")
	}
	egress.Pause()
	if !egress.Paused() {
		t.Fatal("pause should be idempotent")
	}
	egress.Resume()
	if egress.Paused() {
		t.Fatal("expected resumed")
	}
}

var _ BargeInEgress = (*fakeBargeInEgress)(nil)
var _ BargeInTTS = (*fakeBargeInTTS)(nil)
var _ EngineSession = (*fakeEngineSession)(nil)
