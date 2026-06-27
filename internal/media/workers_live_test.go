package media_test

import (
	"context"
	"os"
	"testing"
	"time"

	"websocket/internal/media"
)

func TestWorkersLiveIntegration(t *testing.T) {
	if os.Getenv("WORKERS_LIVE") != "1" {
		t.Skip("set WORKERS_LIVE=1 with workers running on 127.0.0.1:9091-9093")
	}

	ctx := context.Background()
	timeout := 10 * time.Second

	t.Run("RemoteDenoiser", func(t *testing.T) {
		addr := envOr("DENOISE_ADDR", "127.0.0.1:9091")
		d, err := media.NewRemoteDenoiser(media.DenoiseConfig{
			Enabled: true,
			Addr:    addr,
			Timeout: timeout,
		})
		if err != nil {
			t.Fatalf("NewRemoteDenoiser: %v", err)
		}
		t.Cleanup(func() { _ = d.Close() })

		in := make([]byte, 320)
		for i := range in {
			in[i] = byte(i % 256)
		}
		out, err := d.Process(ctx, in, 8000)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		if len(out) != len(in) {
			t.Fatalf("len(out)=%d, want %d", len(out), len(in))
		}
		if d.Fallbacks() != 0 {
			t.Fatalf("denoise fallbacks = %d, want 0", d.Fallbacks())
		}
	})

	t.Run("RemoteAMDClassifier", func(t *testing.T) {
		addr := envOr("AMD_ADDR", "127.0.0.1:9092")
		c, err := media.NewRemoteAMDClassifier(media.AMDConfig{
			Enabled:             true,
			Addr:                addr,
			Timeout:             timeout,
			ProbaHumanThreshold: 0.4,
		})
		if err != nil {
			t.Fatalf("NewRemoteAMDClassifier: %v", err)
		}
		t.Cleanup(func() { _ = c.Close() })

		pcm := bundledAMDPCM16(t)
		decision, err := c.Classify(ctx, pcm, 8000)
		if err != nil {
			t.Fatalf("Classify: %v", err)
		}
		switch decision.Result {
		case media.AMDHuman, media.AMDMachine, media.AMDUnknown:
		default:
			t.Fatalf("unexpected AMD result: %v", decision.Result)
		}
		t.Logf("AMD result=%s proba_human=%.2f reason=%s", decision.Result, decision.ProbaHuman, decision.Reason)
	})

	t.Run("RemoteSemanticTurn", func(t *testing.T) {
		addr := envOr("SEMANTIC_TURN_ADDR", "127.0.0.1:9093")
		st, err := media.NewRemoteSemanticTurn(media.SemanticTurnConfig{
			Enabled: true,
			Addr:    addr,
			Timeout: timeout,
		})
		if err != nil {
			t.Fatalf("NewRemoteSemanticTurn: %v", err)
		}
		t.Cleanup(func() { _ = st.Close() })

		audio := bundledSemanticPCM16()
		pred, err := st.Predict(ctx, "I want to pay my bill.", audio, 8000)
		if err != nil {
			t.Fatalf("Predict: %v", err)
		}
		t.Logf("semantic complete=%v confidence=%.2f", pred.Complete, pred.Confidence)
	})
}

// bundledAMDPCM16 returns ~2 s of 8 kHz mono PCM16 (silence) for wire-protocol smoke.
func bundledAMDPCM16(t *testing.T) []byte {
	t.Helper()
	const sampleRate = 8000
	const durationMs = 2000
	n := sampleRate * durationMs / 1000 * 2
	return make([]byte, n)
}

func bundledSemanticPCM16() []byte {
	const samples = 8000 * 2 // 2 s @ 8 kHz
	pcm := make([]byte, samples*2)
	for i := 0; i < samples; i++ {
		v := int16((i % 400) - 200)
		pcm[i*2] = byte(v)
		pcm[i*2+1] = byte(v >> 8)
	}
	return pcm
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
