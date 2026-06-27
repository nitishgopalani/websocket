package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"websocket/internal/media"
	"websocket/internal/media/sim"
)

func main() {
	addr := flag.String("addr", "ws://localhost:8080/stream", "WebSocket server URL")
	in := flag.String("in", "", "Input audio file (.ulaw or .wav)")
	streamSID := flag.String("stream-sid", "MZ-REPLAY", "Stream SID")
	callSID := flag.String("call-sid", "CA-REPLAY", "Call SID")
	pace := flag.String("pace", "realtime", "Pacing: realtime or fast")
	timeout := flag.Duration("timeout", 120*time.Second, "Overall run timeout")
	flag.Parse()

	source, err := sim.OpenFrameSource(*in, 160)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open input: %v\n", err)
		os.Exit(1)
	}

	pacing := sim.PaceRealtime
	if strings.EqualFold(*pace, "fast") {
		pacing = sim.PaceFast
	}

	simulator := sim.NewCarrierSimulator(sim.RunConfig{
		WSURL:           *addr,
		StreamSID:       *streamSID,
		CallSID:         *callSID,
		MediaFormat:     media.AudioFormat{Encoding: "audio/x-mulaw", SampleRate: 8000, Channels: 1},
		Source:          source,
		Pace:            pacing,
		FrameDurationMs: 20,
		RunTimeout:      *timeout,
	})

	result, err := simulator.Run(context.Background())
	if err != nil {
		fmt.Fprintf(os.Stderr, "run failed: %v\n", err)
	}
	printResult(result)
	if err != nil {
		os.Exit(1)
	}
	if len(result.Errors) > 0 {
		os.Exit(2)
	}
}

func printResult(r *sim.RunResult) {
	if r == nil {
		fmt.Println("no result")
		return
	}
	durationMs := float64(r.OutboundAudioBytes) / 8.0
	fmt.Printf("turns (marks):     %d\n", r.Turns)
	fmt.Printf("outbound audio:    %d bytes (~%.0f ms)\n", r.OutboundAudioBytes, durationMs)
	fmt.Printf("outbound frames:   %d\n", r.OutboundFrames)
	fmt.Printf("first audio:       %v\n", r.FirstAudioLatency)
	fmt.Printf("marks echoed:      %d\n", len(r.Marks))
	fmt.Printf("clear events:      %d\n", len(r.Clears))
	for _, e := range r.Errors {
		fmt.Printf("error: %v\n", e)
	}
}
