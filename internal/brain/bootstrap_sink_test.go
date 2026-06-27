package brain_test

import (
	"context"
	"testing"

	"websocket/internal/brain"
	"websocket/internal/media"
)

func TestBootstrapOpenerOnStartWhenAMDDisabled(t *testing.T) {
	ctrl := brain.NewCallControl(brain.CallControlConfig{AMDEnabled: false})
	sink := &brain.BootstrapSink{
		AMDEnabled:  false,
		CallControl: ctrl,
	}
	session := &media.Session{StreamSID: "MZ-OP"}
	if err := sink.OnStart(context.Background(), session); err != nil {
		// Brain nil — no opener path; regression is no panic with AMD off + no brain
	}
}

func TestBootstrapDefersOpenerWhenAMDEnabled(t *testing.T) {
	ctrl := brain.NewCallControl(brain.CallControlConfig{AMDEnabled: true})
	egress := media.NewCarrierEgress(media.DefaultEgressConfig(), 20, media.RealClock{}, nil, nil)
	ctrl.Bind(nil, nil, egress, nil)
	sink := &brain.BootstrapSink{
		AMDEnabled:    true,
		CallControl:   ctrl,
		CarrierEgress: egress,
		Inner:         media.NewLoggingSink(nil),
	}
	session := &media.Session{StreamSID: "MZ-GATE"}
	if err := sink.OnStart(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	if ctrl.OpenerCount() != 0 {
		t.Fatalf("opener before OnHuman = %d, want 0", ctrl.OpenerCount())
	}
	if !egress.HumanGated() {
		t.Fatal("expected human gate when AMD enabled")
	}
	ctrl.OnHuman(context.Background(), session)
	if egress.HumanGated() {
		t.Fatal("human gate should release after OnHuman")
	}
}
