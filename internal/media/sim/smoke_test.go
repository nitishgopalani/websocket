package sim_test

import (
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"websocket/internal/media/sim"
)

func TestSmokeEndToEnd(t *testing.T) {
	h, err := sim.NewSmokeHarness(sim.SmokeHarnessConfig{
		ASR: sim.FakeASRConfig{
			FinalText:         "haan ji",
			FramesBeforeFinal: 5,
			EmitSpeechStart:   true,
		},
		MetricsEnabled: true,
		SilenceMs:      50,
	})
	if err != nil {
		t.Fatalf("harness: %v", err)
	}
	defer h.Close()

	source := sim.NewSilenceGenerator(160, 30)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	result, err := h.RunSmoke(ctx, source, 30)
	if err != nil && err != context.DeadlineExceeded {
		t.Fatalf("run: %v", err)
	}
	if result == nil {
		t.Fatal("nil result")
	}
	if len(result.Errors) > 0 {
		t.Fatalf("run errors: %v", result.Errors)
	}
	if result.OutboundAudioBytes <= 0 {
		t.Fatalf("expected outbound audio, got %d bytes", result.OutboundAudioBytes)
	}
	if len(result.Marks) == 0 {
		t.Fatal("expected at least one mark echo round-trip")
	}
	for _, m := range result.Marks {
		if m.Echoed.IsZero() {
			t.Fatalf("mark %q not echoed", m.Name)
		}
	}
	if h.Turns.EndOfTurnCount() < 1 {
		t.Fatalf("expected >=1 end_of_turn, got %d", h.Turns.EndOfTurnCount())
	}

	resp, err := http.Get(h.HTTPSrv.URL + "/metrics")
	if err != nil {
		t.Fatalf("metrics: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "media_turns_total") {
		t.Fatalf("metrics missing media_turns_total: %q", string(body))
	}
}

func TestCarrierSimulatorWithRecordingSink(t *testing.T) {
	h, err := sim.NewSmokeHarness(sim.SmokeHarnessConfig{SilenceMs: 50})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	result, err := h.RunSmoke(context.Background(), sim.NewToneGenerator(160, 20), 20)
	if err != nil && err != context.DeadlineExceeded {
		t.Fatal(err)
	}
	if result.OutboundAudioBytes == 0 {
		t.Fatal("expected outbound audio from opener")
	}
}

func TestSyntheticFrameSource(t *testing.T) {
	gen := sim.NewSilenceGenerator(160, 3)
	for i := 0; i < 3; i++ {
		frame, err := gen.NextFrame()
		if err != nil {
			t.Fatal(err)
		}
		if len(frame) != 160 {
			t.Fatalf("frame len = %d", len(frame))
		}
	}
	if _, err := gen.NextFrame(); err == nil {
		t.Fatal("expected EOF")
	}
}

func TestSmokeULawFile(t *testing.T) {
	path := filepath.Join("..", "..", "..", "testdata", "smoke.ulaw")
	if _, err := os.Stat(path); err != nil {
		t.Skip("testdata/smoke.ulaw not present")
	}
	src, err := sim.OpenFrameSource(path, 160)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for {
		_, err := src.NextFrame()
		if err != nil {
			break
		}
		n++
	}
	if n == 0 {
		t.Fatal("expected frames from smoke.ulaw")
	}
}
