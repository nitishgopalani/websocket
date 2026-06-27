// deploy-smoke: Asterisk-protocol connectivity test against a running go-server (no paid APIs).
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"websocket/internal/media/sim"
)

func main() {
	wsURL := os.Getenv("WS_URL")
	if wsURL == "" {
		wsURL = "ws://go-server:8080/stream"
	}
	expectTTS := os.Getenv("SMOKE_EXPECT_TTS") == "true"

	cfg := sim.AsteriskRunConfig{
		WSURL:      wsURL,
		SessionID:  "DEPLOY-SMOKE",
		PCM16:      sim.PCM16Silence(6400),
		ChunkBytes: 3200,
		Pace:       sim.PaceFast,
		RunTimeout: 30 * time.Second,
		Language:   "en-IN",
		AgentID:    "deploy-smoke",
	}
	result, err := sim.NewAsteriskSimulator(cfg).Run(context.Background())
	if err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: asterisk smoke: %v\n", err)
		os.Exit(1)
	}
	if !result.ReadyReceived {
		fmt.Fprintln(os.Stderr, "FAIL: ready control frame not received")
		os.Exit(1)
	}
	if expectTTS && result.OutboundBinaryBytes == 0 {
		fmt.Fprintln(os.Stderr, "FAIL: expected outbound TTS binary audio")
		os.Exit(1)
	}
	fmt.Printf("PASS: ready=yes outbound_binary_bytes=%d outbound_binary_frames=%d\n",
		result.OutboundBinaryBytes, result.OutboundBinaryFrames)
}
