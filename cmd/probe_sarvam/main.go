package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"websocket/internal/media"
)

func main() {
	os.Exit(run())
}

func run() int {
	// shellcheck disable=SC1091
	// Loaded via probe_sarvam.sh
	key := strings.TrimSpace(os.Getenv("SARVAM_API_KEY"))
	if key == "" {
		fmt.Println("FAIL: SARVAM_API_KEY not set")
		return 1
	}

	fixture := os.Getenv("PROBE_FIXTURE")
	if fixture == "" {
		fixture = "testdata/calls/human_long.ulaw"
	}
	lang := os.Getenv("ASR_LANGUAGE")
	if lang == "" {
		lang = "en-IN"
	}

	data, err := os.ReadFile(fixture)
	if err != nil {
		fmt.Printf("FAIL: read fixture %s: %v\n", fixture, err)
		return 1
	}
	pcm := media.MuLawToPCM16(data)
	if len(pcm) < 320 {
		fmt.Println("FAIL: fixture too short")
		return 1
	}
	// ~1.5s of speech at 8kHz (minimal quota).
	maxBytes := 8000 * 2 * 3 / 2
	if len(pcm) > maxBytes {
		pcm = pcm[:maxBytes]
	}

	cfg := media.DefaultASRConfig()
	cfg.Enabled = true
	cfg.APIKey = key
	cfg.Language = lang

	provider, err := media.NewASRProvider(cfg)
	if err != nil {
		fmt.Printf("FAIL: provider: %v\n", err)
		return 1
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sess, err := provider.Open(ctx, media.ASRSessionMeta{
		StreamSID:  "probe-sarvam",
		SampleRate: 8000,
		Language:   lang,
	})
	if err != nil {
		fmt.Printf("FAIL: open: %v\n", err)
		return 1
	}
	defer sess.Close()

	fmt.Println("=== Sarvam probe: connect deferred until first audio ===")
	frameSize := 320
	var partials, finals int
	var lastText string

	done := make(chan struct{})
	go func() {
		defer close(done)
		for evt := range sess.Events() {
			switch evt.Type {
			case media.ASREventPartial:
				partials++
				lastText = evt.Transcript.Text
				fmt.Printf("PARTIAL: %q\n", evt.Transcript.Text)
			case media.ASREventFinal:
				finals++
				lastText = evt.Transcript.Text
				fmt.Printf("FINAL: %q\n", evt.Transcript.Text)
			case media.ASREventSpeechStart:
				fmt.Println("EVENT: speech_start")
			case media.ASREventSpeechEnd:
				fmt.Println("EVENT: speech_end")
			case media.ASREventError:
				fmt.Printf("ERROR: %v\n", evt.Err)
			}
		}
	}()

	for i := 0; i < len(pcm); i += frameSize {
		end := i + frameSize
		if end > len(pcm) {
			end = len(pcm)
		}
		if err := sess.SendAudio(pcm[i:end]); err != nil {
			fmt.Printf("FAIL: send audio: %v\n", err)
			return 1
		}
		time.Sleep(20 * time.Millisecond)
	}

	time.Sleep(3 * time.Second)
	_ = sess.Close()
	<-done

	fmt.Println("")
	fmt.Println("=== PROBE RESULT ===")
	fmt.Printf("fixture_bytes=%d pcm_bytes=%d partials=%d finals=%d\n", len(data), len(pcm), partials, finals)
	if lastText != "" {
		fmt.Printf("last_transcript=%q\n", lastText)
	}
	if partials+finals > 0 {
		fmt.Println("PASS: Sarvam returned transcript(s)")
		return 0
	}
	fmt.Println("FAIL: no transcript (see logs above for close code / recv)")
	return 1
}
