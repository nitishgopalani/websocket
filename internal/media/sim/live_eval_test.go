package sim_test

import (
	"os"
	"testing"
)

func TestLiveEvalGated(t *testing.T) {
	if os.Getenv("RUN_LIVE_EVAL") != "1" {
		t.Skip("live eval disabled (set RUN_LIVE_EVAL=1)")
	}
	if os.Getenv("SARVAM_API_KEY") == "" {
		t.Skip("SARVAM_API_KEY not set")
	}
	if os.Getenv("ELEVENLABS_API_KEY") == "" {
		t.Skip("ELEVENLABS_API_KEY not set")
	}
	if os.Getenv("BRAIN_WS_URL") == "" {
		t.Skip("BRAIN_WS_URL not set")
	}
	t.Skip("live eval harness: add recorded calls under testdata/calls/ and wire real providers")
}
