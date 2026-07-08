package brain

import (
	"context"
	"testing"

	"websocket/internal/media"
)

func TestIsTapOnlySession(t *testing.T) {
	if isTapOnlySession(nil) {
		t.Fatal("nil session")
	}
	if isTapOnlySession(&media.Session{}) {
		t.Fatal("empty params")
	}
	if isTapOnlySession(&media.Session{Params: map[string]string{"tap_only": "false"}}) {
		t.Fatal("tap_only=false")
	}
	if !isTapOnlySession(&media.Session{Params: map[string]string{"tap_only": "true"}}) {
		t.Fatal("tap_only=true")
	}
}

func TestBootstrapSkipsOpenerForTapOnly(t *testing.T) {
	ctrl := NewCallControl(CallControlConfig{AMDEnabled: false})
	sink := &BootstrapSink{
		AMDEnabled:  false,
		CallControl: ctrl,
		// Brain nil — tap_only path must not panic and must not record opener.
	}
	session := &media.Session{
		StreamSID: "tap-only-1",
		Params:    map[string]string{"tap_only": "true"},
	}
	if err := sink.OnStart(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	if ctrl.OpenerCount() != 0 {
		t.Fatalf("opener count = %d, want 0 for tap_only", ctrl.OpenerCount())
	}
}
