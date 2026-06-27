package media_test

import (
	"context"
	"sync/atomic"
	"testing"

	"websocket/internal/media"
)

type asrStartTracker struct {
	starts atomic.Int32
	inner  media.AudioSink
}

func (a *asrStartTracker) OnStart(_ context.Context, _ *media.Session) error {
	a.starts.Add(1)
	if a.inner != nil {
		return a.inner.OnStart(context.Background(), nil)
	}
	return nil
}

func (a *asrStartTracker) OnAudio(_ context.Context, _ *media.Session, _ []byte) error { return nil }
func (a *asrStartTracker) OnDTMF(_ context.Context, _ *media.Session, _ string) error  { return nil }
func (a *asrStartTracker) OnStop(_ context.Context, _ *media.Session) error            { return nil }

func TestAMDGateDefersDownstreamUntilHuman(t *testing.T) {
	tracker := &asrStartTracker{}
	listener := &recordingAMDListener{}
	clf := &stubAMDClassifier{decision: media.AMDDecision{Result: media.AMDHuman, ProbaHuman: 0.9}}
	cfg := media.DefaultAMDConfig()
	cfg.WindowMs = 40
	cfg.Enabled = true
	gate := media.NewAMDGateSink(tracker, clf, listener, 8000, cfg, nil)
	session := &media.Session{StreamSID: "MZ-DEFER"}
	ctx := context.Background()

	if err := gate.OnStart(ctx, session); err != nil {
		t.Fatal(err)
	}
	if tracker.starts.Load() != 0 {
		t.Fatalf("downstream started early: %d", tracker.starts.Load())
	}
	if err := gate.OnAudio(ctx, session, makeAMDFrame(1, 320)); err != nil {
		t.Fatal(err)
	}
	if err := gate.OnAudio(ctx, session, makeAMDFrame(2, 320)); err != nil {
		t.Fatal(err)
	}
	if tracker.starts.Load() != 1 {
		t.Fatalf("downstream starts = %d, want 1 after human", tracker.starts.Load())
	}
	if listener.humanCount != 1 {
		t.Fatalf("humanCount = %d", listener.humanCount)
	}
}

func TestAMDGateMachineNeverStartsDownstream(t *testing.T) {
	tracker := &asrStartTracker{}
	listener := &recordingAMDListener{}
	clf := &stubAMDClassifier{decision: media.AMDDecision{Result: media.AMDMachine, ProbaHuman: 0.05}}
	cfg := media.DefaultAMDConfig()
	cfg.WindowMs = 40
	gate := media.NewAMDGateSink(tracker, clf, listener, 8000, cfg, nil)
	session := &media.Session{StreamSID: "MZ-NODS"}
	ctx := context.Background()

	_ = gate.OnStart(ctx, session)
	_ = gate.OnAudio(ctx, session, makeAMDFrame(1, 320))
	_ = gate.OnAudio(ctx, session, makeAMDFrame(2, 320))
	if tracker.starts.Load() != 0 {
		t.Fatalf("expected no downstream start on machine, got %d", tracker.starts.Load())
	}
	if listener.machineCount != 1 {
		t.Fatalf("machineCount = %d", listener.machineCount)
	}
}

func TestCarrierSerializerRoundTrip(t *testing.T) {
	payload := []byte{0xFF, 0xD5, 0x80}
	for _, ser := range []media.CarrierSerializer{
		media.FonadaSerializer{},
		media.ExotelSerializer{},
	} {
		mediaJSON, err := ser.Media("MZ1", payload)
		if err != nil {
			t.Fatal(err)
		}
		markJSON, err := ser.Mark("MZ1", "turn-1")
		if err != nil {
			t.Fatal(err)
		}
		clearJSON, err := ser.Clear("MZ1")
		if err != nil {
			t.Fatal(err)
		}
		if len(mediaJSON) == 0 || len(markJSON) == 0 || len(clearJSON) == 0 {
			t.Fatal("empty serializer output")
		}
	}
}

func TestNewCarrierSerializerSelect(t *testing.T) {
	if _, ok := media.NewCarrierSerializer(media.CarrierConfig{Variant: "fonada"}).(media.FonadaSerializer); !ok {
		t.Fatal("fonada")
	}
	if _, ok := media.NewCarrierSerializer(media.CarrierConfig{Variant: "exotel"}).(media.ExotelSerializer); !ok {
		t.Fatal("exotel")
	}
}
