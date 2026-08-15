package media

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"
)

// TTSPreWarmLine is one utterance to synthesize into the TTS cache at boot
// (Item 3, DEBT-034). Voice/Model/Language override the base TTSConfig for
// this line; Text is the exact string the live call will Speak.
type TTSPreWarmLine struct {
	Voice    string `json:"voice"`
	Model    string `json:"model"`
	Language string `json:"language"`
	Text     string `json:"text"`
}

// preWarmLineTimeout is the per-line drain cap. A line that takes longer is
// abandoned (the live call will synthesize on demand).
const preWarmLineTimeout = 10 * time.Second

// PreWarmTTS synthesizes each line into the global TTS cache at boot so the
// first live call that Speaks the same line (same voice/model/language/format/
// rate) hits the cache with zero synthesis latency (Item 3, DEBT-034). Best-
// effort: a line that fails to synth is skipped (the live call will synth on
// demand). Returns the count of lines warmed and the total elapsed ms.
//
// The pipeline matches OpenSessionTTSStream: provider.Open → resample (if
// needed) → caching wrapper. The synthetic session uses the carrier default
// format (ulaw_8000 @ 8 kHz) so the cache key prefix matches a real 8 kHz
// paisalo/sot call.
func PreWarmTTS(
	ctx context.Context,
	provider TTSProvider,
	base TTSConfig,
	lines []TTSPreWarmLine,
	logger *slog.Logger,
) (warmed int, warmMs int64) {
	if provider == nil || len(lines) == 0 {
		return 0, 0
	}
	if logger == nil {
		logger = slog.Default()
	}
	if !ttsCacheEnabled() {
		logger.Info("tts prewarm skipped (cache disabled)")
		return 0, 0
	}
	start := time.Now()

	// Synthetic session matching a real 8 kHz μ-law call so the cache key
	// prefix (provider|voice|model|language|format|rate) matches live calls.
	// output_sample_rate=8000 in Params makes OpenSessionTTSStream pick
	// pcm_8000 @ 8 kHz (no resampling) — the same prefix a real paisalo/sot
	// 8 kHz call produces.
	synthSession := &Session{
		StreamSID: "prewarm",
		CallSID:   "prewarm",
		Params: map[string]string{
			"output_sample_rate": "8000",
		},
		Format: AudioFormat{
			Encoding:   "ulaw",
			SampleRate: 8000,
			Channels:   1,
		},
	}

	for i, ln := range lines {
		text := strings.TrimSpace(ln.Text)
		if text == "" {
			continue
		}
		lineCtx, cancel := context.WithTimeout(ctx, preWarmLineTimeout)
		stream, err := OpenSessionTTSStream(lineCtx, provider, base, synthSession, logger)
		if err != nil {
			logger.Warn("tts prewarm open failed",
				"line", i, "voice", ln.Voice, "error", err)
			cancel()
			continue
		}

		turnID := fmt.Sprintf("prewarm-%d", i)
		if ln.Voice != "" || ln.Model != "" {
			ApplyTTSTurnVoice(stream, turnID, ln.Voice, ln.Model, nil)
		}
		// DEBT-038: a template with {customer_name} warms the static
		// prefix/suffix as separate cache keys (exact live Speak match).
		speakTexts := []string{text}
		if pfx, sfx, ok := SplitTemplateSegments(text); ok {
			speakTexts = nil
			if strings.TrimSpace(pfx) != "" {
				speakTexts = append(speakTexts, pfx)
			}
			if strings.TrimSpace(sfx) != "" {
				speakTexts = append(speakTexts, sfx)
			}
		}
		var speakErr error
		for _, part := range speakTexts {
			if err := stream.Speak(turnID, part); err != nil {
				speakErr = err
				break
			}
		}
		if speakErr != nil {
			logger.Warn("tts prewarm speak failed",
				"line", i, "voice", ln.Voice, "error", speakErr)
			_ = stream.Close()
			cancel()
			continue
		}
		// Flush — triggers the caching wrapper to record the segment.
		_ = stream.Speak(turnID, "")

		// Drain audio until we see the Final chunk (recording is complete) or
		// the per-line timeout fires. We must NOT block forever — the caching
		// wrapper's out channel only closes on Close(), so we break on Final.
		drainDone := make(chan struct{})
		go func() {
			for chunk := range stream.Audio() {
				if chunk.Final {
					break
				}
			}
			close(drainDone)
		}()
		select {
		case <-drainDone:
		case <-lineCtx.Done():
			logger.Warn("tts prewarm drain timeout",
				"line", i, "voice", ln.Voice, "text_len", len(text))
		}
		_ = stream.Close()
		cancel()
		warmed++
		logger.Info("tts prewarm line synthesized",
			"line", i, "voice", ln.Voice, "model", ln.Model, "text_len", len(text))
	}
	warmMs = time.Since(start).Milliseconds()
	logger.Info("tts prewarm complete",
		"lines_requested", len(lines),
		"lines_warmed", warmed,
		"warm_ms", warmMs,
	)
	return warmed, warmMs
}

// PreWarmLinesFromEnv reads TTS_PREWARM_LINES (JSON array of TTSPreWarmLine)
// or TTS_PREWARM_FILE (path to a JSON file with the same shape). Returns nil
// if neither is set or parsing fails (warned).
func PreWarmLinesFromEnv(logger *slog.Logger) []TTSPreWarmLine {
	if logger == nil {
		logger = slog.Default()
	}
	if v := strings.TrimSpace(os.Getenv("TTS_PREWARM_LINES")); v != "" {
		var lines []TTSPreWarmLine
		if err := json.Unmarshal([]byte(v), &lines); err != nil {
			logger.Warn("tts prewarm parse failed (TTS_PREWARM_LINES)", "error", err)
			return nil
		}
		return lines
	}
	if path := strings.TrimSpace(os.Getenv("TTS_PREWARM_FILE")); path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			logger.Warn("tts prewarm file read failed", "path", path, "error", err)
			return nil
		}
		var lines []TTSPreWarmLine
		if err := json.Unmarshal(data, &lines); err != nil {
			logger.Warn("tts prewarm file parse failed", "path", path, "error", err)
			return nil
		}
		return lines
	}
	return nil
}
