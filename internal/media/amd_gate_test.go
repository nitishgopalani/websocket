package media_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"websocket/internal/media"
)

type recordingAMDListener struct {
	humanCount   int
	machineCount int
	lastMachine  media.AMDDecision
}

func (l *recordingAMDListener) OnHuman(_ context.Context, _ *media.Session) {
	l.humanCount++
}

func (l *recordingAMDListener) OnMachine(_ context.Context, _ *media.Session, decision media.AMDDecision) {
	l.machineCount++
	l.lastMachine = decision
}

type stubAMDClassifier struct {
	decision media.AMDDecision
	err      error
	delay    time.Duration
	calls    int
}

func (s *stubAMDClassifier) Classify(_ context.Context, _ []byte, _ int) (media.AMDDecision, error) {
	s.calls++
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	if s.err != nil {
		return media.AMDDecision{}, s.err
	}
	d := s.decision
	d.Final = true
	return d, nil
}

func (s *stubAMDClassifier) Close() error { return nil }

func makeAMDFrame(seed byte, size int) []byte {
	b := make([]byte, size)
	for i := range b {
		b[i] = seed + byte(i)
	}
	return b
}

func TestAMDGateNoopPassesThrough(t *testing.T) {
	collector := &collectSink{}
	gate := media.NewAMDGateSink(collector, media.NoopAMDClassifier{}, &recordingAMDListener{}, 8000, media.DefaultAMDConfig(), nil)
	session := &media.Session{StreamSID: "MZ-NOOP"}
	ctx := context.Background()

	if err := gate.OnStart(ctx, session); err != nil {
		t.Fatalf("OnStart: %v", err)
	}
	f1 := makeAMDFrame(1, 320)
	f2 := makeAMDFrame(2, 320)
	if err := gate.OnAudio(ctx, session, f1); err != nil {
		t.Fatalf("OnAudio1: %v", err)
	}
	if err := gate.OnAudio(ctx, session, f2); err != nil {
		t.Fatalf("OnAudio2: %v", err)
	}
	if err := gate.OnStop(ctx, session); err != nil {
		t.Fatalf("OnStop: %v", err)
	}
	if len(collector.frames) != 2 {
		t.Fatalf("frames = %d, want 2 immediate pass-through", len(collector.frames))
	}
	if string(collector.frames[0]) != string(f1) || string(collector.frames[1]) != string(f2) {
		t.Fatal("frames not byte-exact")
	}
}

func TestAMDGateHumanFlushesBufferedInOrder(t *testing.T) {
	collector := &collectSink{}
	listener := &recordingAMDListener{}
	clf := &stubAMDClassifier{decision: media.AMDDecision{Result: media.AMDHuman, ProbaHuman: 0.95, Reason: "test"}}
	cfg := media.DefaultAMDConfig()
	cfg.WindowMs = 40
	gate := media.NewAMDGateSink(collector, clf, listener, 8000, cfg, nil)
	session := &media.Session{StreamSID: "MZ-HUM"}
	ctx := context.Background()

	if err := gate.OnStart(ctx, session); err != nil {
		t.Fatalf("OnStart: %v", err)
	}

	f1 := makeAMDFrame(10, 320)
	f2 := makeAMDFrame(20, 320)
	if err := gate.OnAudio(ctx, session, f1); err != nil {
		t.Fatalf("OnAudio1: %v", err)
	}
	if err := gate.OnAudio(ctx, session, f2); err != nil {
		t.Fatalf("OnAudio2: %v", err)
	}
	if listener.humanCount != 1 {
		t.Fatalf("humanCount = %d, want 1", listener.humanCount)
	}
	if len(collector.frames) != 2 {
		t.Fatalf("flushed frames = %d, want 2", len(collector.frames))
	}
	if string(collector.frames[0]) != string(f1) || string(collector.frames[1]) != string(f2) {
		t.Fatal("flushed frames not byte-exact or out of order")
	}

	f3 := makeAMDFrame(30, 320)
	if err := gate.OnAudio(ctx, session, f3); err != nil {
		t.Fatalf("OnAudio3: %v", err)
	}
	if len(collector.frames) != 3 || string(collector.frames[2]) != string(f3) {
		t.Fatal("post-human frame not passed through")
	}
}

func TestAMDGateMachineDropsAndDoesNotForward(t *testing.T) {
	collector := &collectSink{}
	listener := &recordingAMDListener{}
	clf := &stubAMDClassifier{decision: media.AMDDecision{Result: media.AMDMachine, ProbaHuman: 0.1, Reason: "voicemail"}}
	cfg := media.DefaultAMDConfig()
	cfg.WindowMs = 40
	gate := media.NewAMDGateSink(collector, clf, listener, 8000, cfg, nil)
	session := &media.Session{StreamSID: "MZ-MAC"}
	ctx := context.Background()

	if err := gate.OnStart(ctx, session); err != nil {
		t.Fatalf("OnStart: %v", err)
	}
	if err := gate.OnAudio(ctx, session, makeAMDFrame(1, 320)); err != nil {
		t.Fatalf("OnAudio1: %v", err)
	}
	if err := gate.OnAudio(ctx, session, makeAMDFrame(2, 320)); err != nil {
		t.Fatalf("OnAudio2: %v", err)
	}
	if listener.machineCount != 1 {
		t.Fatalf("machineCount = %d, want 1", listener.machineCount)
	}
	if len(collector.frames) != 0 {
		t.Fatalf("expected no forwarded frames, got %d", len(collector.frames))
	}
	if err := gate.OnAudio(ctx, session, makeAMDFrame(3, 320)); err != nil {
		t.Fatalf("OnAudio3: %v", err)
	}
	if gate.DroppedDuringMachine() != 1 {
		t.Fatalf("dropped = %d, want 1", gate.DroppedDuringMachine())
	}
}

func TestAMDGateClassifierErrorFailOpen(t *testing.T) {
	collector := &collectSink{}
	listener := &recordingAMDListener{}
	clf := &stubAMDClassifier{err: errors.New("worker down")}
	cfg := media.DefaultAMDConfig()
	cfg.WindowMs = 40
	gate := media.NewAMDGateSink(collector, clf, listener, 8000, cfg, nil)
	session := &media.Session{StreamSID: "MZ-ERR"}
	ctx := context.Background()

	if err := gate.OnStart(ctx, session); err != nil {
		t.Fatalf("OnStart: %v", err)
	}
	if err := gate.OnAudio(ctx, session, makeAMDFrame(5, 320)); err != nil {
		t.Fatalf("OnAudio1: %v", err)
	}
	if err := gate.OnAudio(ctx, session, makeAMDFrame(6, 320)); err != nil {
		t.Fatalf("OnAudio2: %v", err)
	}
	if listener.humanCount != 1 {
		t.Fatalf("humanCount = %d, want fail-open human", listener.humanCount)
	}
	if len(collector.frames) != 2 {
		t.Fatalf("frames = %d, want flushed buffer", len(collector.frames))
	}
}

func TestAMDGateStopDuringDetectingFlushes(t *testing.T) {
	collector := &collectSink{}
	listener := &recordingAMDListener{}
	clf := &stubAMDClassifier{decision: media.AMDDecision{Result: media.AMDMachine, ProbaHuman: 0.1}}
	cfg := media.DefaultAMDConfig()
	cfg.WindowMs = 2000
	gate := media.NewAMDGateSink(collector, clf, listener, 8000, cfg, nil)
	session := &media.Session{StreamSID: "MZ-STOP"}
	ctx := context.Background()

	if err := gate.OnStart(ctx, session); err != nil {
		t.Fatalf("OnStart: %v", err)
	}
	f1 := makeAMDFrame(7, 320)
	if err := gate.OnAudio(ctx, session, f1); err != nil {
		t.Fatalf("OnAudio: %v", err)
	}
	if err := gate.OnStop(ctx, session); err != nil {
		t.Fatalf("OnStop: %v", err)
	}
	if clf.calls != 0 {
		t.Fatalf("classify calls = %d, want 0 on short stop fail-open", clf.calls)
	}
	if len(collector.frames) != 1 {
		t.Fatalf("frames = %d, want 1 flushed on stop decision", len(collector.frames))
	}
}
