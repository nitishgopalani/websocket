package media

import (
	"context"
	"testing"
)

type amdCollectSink struct {
	frames [][]byte
}

func (c *amdCollectSink) OnStart(_ context.Context, _ *Session) error { return nil }
func (c *amdCollectSink) OnDTMF(_ context.Context, _ *Session, _ string) error {
	return nil
}
func (c *amdCollectSink) OnStop(_ context.Context, _ *Session) error { return nil }
func (c *amdCollectSink) OnAudio(_ context.Context, _ *Session, frame []byte) error {
	copied := make([]byte, len(frame))
	copy(copied, frame)
	c.frames = append(c.frames, copied)
	return nil
}

type amdTestListener struct {
	human int
}

func (l *amdTestListener) OnHuman(_ context.Context, _ *Session) { l.human++ }
func (l *amdTestListener) OnMachine(_ context.Context, _ *Session, _ AMDDecision) {
}

type stubAMDClassifierInternal struct {
	decision AMDDecision
}

func (s *stubAMDClassifierInternal) Classify(_ context.Context, _ []byte, _ int) (AMDDecision, error) {
	return s.decision, nil
}
func (s *stubAMDClassifierInternal) Close() error { return nil }

func TestAMDGateBufferCapFailOpenInternal(t *testing.T) {
	collector := &amdCollectSink{}
	listener := &amdTestListener{}
	cfg := DefaultAMDConfig()
	cfg.WindowMs = 2000
	cfg.BufferMarginMs = 40
	gate := NewAMDGateSink(collector, &stubAMDClassifierInternal{
		decision: AMDDecision{Result: AMDMachine, ProbaHuman: 0.1},
	}, listener, 8000, cfg, nil)
	gate.maxBufferBytes = 700
	session := &Session{StreamSID: "MZ-CAP-INT"}
	ctx := context.Background()

	if err := gate.OnStart(ctx, session); err != nil {
		t.Fatalf("OnStart: %v", err)
	}
	frame := make([]byte, 320)
	for i := 0; i < 3; i++ {
		if err := gate.OnAudio(ctx, session, frame); err != nil {
			t.Fatalf("OnAudio[%d]: %v", i, err)
		}
	}
	if listener.human != 1 {
		t.Fatalf("humanCount = %d, want fail-open on cap", listener.human)
	}
	if len(collector.frames) == 0 {
		t.Fatal("expected flushed frames")
	}
}
