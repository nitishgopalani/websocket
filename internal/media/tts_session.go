package media

import (
	"context"
	"log/slog"
	"strconv"
	"strings"
)

// LogSessionAudioRates emits one line per call with session/sarvam/asr rates.
// WARNs when any mismatch survives so diagnosis is immediate.
func LogSessionAudioRates(logger *slog.Logger, streamSID string, sessionRate, sarvamRate, asrRate int) {
	if logger == nil {
		return
	}
	mismatch := sessionRate > 0 && sarvamRate > 0 && asrRate > 0 &&
		(sessionRate != sarvamRate || sessionRate != asrRate || sarvamRate != asrRate)
	attrs := []any{
		"stream_sid", streamSID,
		"session_rate", sessionRate,
		"sarvam_rate", sarvamRate,
		"asr_rate", asrRate,
	}
	if mismatch {
		logger.Warn("audio rate mismatch", attrs...)
		return
	}
	logger.Info("audio rates", attrs...)
}

// OutputSampleRateFromParams reads Dinesh session_start output_sample_rate (default 0 = use carrier default).
func OutputSampleRateFromParams(params map[string]string) int {
	if params == nil {
		return 0
	}
	for _, key := range []string{"output_sample_rate", "outputSampleRate"} {
		if v := strings.TrimSpace(params[key]); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				return n
			}
		}
	}
	return 0
}

// ElevenLabsPCMFormatForRate picks the closest streaming PCM format for a target Hz.
// 8 kHz sessions stay on pcm_8000 (G.711 / Asterisk slin) — do not snap up to 16 kHz.
func ElevenLabsPCMFormatForRate(hz int) string {
	switch {
	case hz <= 0:
		return "pcm_24000"
	case hz <= 8000:
		return "pcm_8000"
	case hz <= 16000:
		return "pcm_16000"
	case hz <= 22050:
		return "pcm_22050"
	case hz <= 24000:
		return "pcm_24000"
	default:
		return "pcm_44100"
	}
}

// SampleRateFromPCMFormat parses Hz from ElevenLabs output_format strings.
func SampleRateFromPCMFormat(format string) int {
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "pcm_8000", "ulaw_8000":
		return 8000
	case "pcm_16000":
		return 16000
	case "pcm_22050":
		return 22050
	case "pcm_24000":
		return 24000
	case "pcm_44100":
		return 44100
	default:
		return 24000
	}
}

// OpenSessionTTSStream opens ElevenLabs for one call using session_start output_sample_rate.
func OpenSessionTTSStream(
	ctx context.Context,
	provider TTSProvider,
	base TTSConfig,
	session *Session,
	logger *slog.Logger,
) (TTSStream, error) {
	declared := OutputSampleRateFromParams(session.Params)
	targetRate := declared
	if targetRate <= 0 {
		targetRate = 24000
	}
	format := ElevenLabsPCMFormatForRate(targetRate)
	sourceRate := SampleRateFromPCMFormat(format)

	meta := TTSSessionMeta{
		StreamSID:        session.StreamSID,
		CallSID:          session.CallSID,
		Params:           session.Params,
		OutputSampleRate: targetRate,
		OutputFormat:     format,
	}
	asrRate := session.Format.SampleRate
	if asrRate <= 0 {
		asrRate = targetRate
	}
	LogSessionAudioRates(logger, session.StreamSID, targetRate, sourceRate, asrRate)
	if logger != nil {
		logger.Info("tts session output rate",
			"stream_sid", session.StreamSID,
			"declared_output_sample_rate", declared,
			"effective_output_sample_rate", targetRate,
			"elevenlabs_format", format,
			"elevenlabs_pcm_rate", sourceRate,
		)
	}
	stream, err := provider.Open(ctx, meta)
	if err != nil {
		return nil, err
	}
	if sourceRate != targetRate {
		stream = NewResamplingTTSStream(stream, sourceRate, targetRate, logger, session.StreamSID)
	}
	// Outermost layer: cache the final (post-resample) audio so a repeat of the exact
	// same spoken line skips both the TTS network call and resampling. Key includes
	// provider/voice/model/language/format/rate so any of those changing invalidates it.
	if ttsCacheEnabled() {
		prefix := strings.Join([]string{
			base.Provider, base.VoiceID, base.Model, base.Language,
			format, strconv.Itoa(targetRate),
		}, "|")
		stream = newCachingTTSStream(stream, prefix, GlobalTTSCache(), logger, session.StreamSID)
	}
	return stream, nil
}

// NewResamplingTTSStream wraps a TTS stream and resamples PCM16 chunks to targetRate.
func NewResamplingTTSStream(inner TTSStream, sourceRate, targetRate int, logger *slog.Logger, streamSID string) TTSStream {
	if inner == nil || sourceRate <= 0 || targetRate <= 0 || sourceRate == targetRate {
		return inner
	}
	return &resamplingTTSStream{
		inner:      inner,
		sourceRate: sourceRate,
		targetRate: targetRate,
		logger:     logger,
		streamSID:  streamSID,
	}
}

type resamplingTTSStream struct {
	inner                        TTSStream
	sourceRate, targetRate       int
	logger                       *slog.Logger
	streamSID                    string
}

func (r *resamplingTTSStream) SetTurnVoice(turnID, voiceID, model string, pace *float64) {
	ApplyTTSTurnVoice(r.inner, turnID, voiceID, model, pace)
}

func (r *resamplingTTSStream) Speak(turnID string, text string) error {
	return r.inner.Speak(turnID, text)
}

func (r *resamplingTTSStream) Cancel(turnID string) error {
	return r.inner.Cancel(turnID)
}

func (r *resamplingTTSStream) Close() error {
	return r.inner.Close()
}

func (r *resamplingTTSStream) Audio() <-chan TTSAudioChunk {
	out := make(chan TTSAudioChunk, defaultTTSAudioBuffer)
	go func() {
		defer close(out)
		for chunk := range r.inner.Audio() {
			if len(chunk.MuLaw) > 0 && r.sourceRate != r.targetRate {
				resampled, err := ResamplePCM16Linear(chunk.MuLaw, r.sourceRate, r.targetRate)
				if err != nil {
					if r.logger != nil {
						r.logger.Warn("tts resample failed",
							"stream_sid", r.streamSID,
							"from_hz", r.sourceRate,
							"to_hz", r.targetRate,
							"error", err,
						)
					}
				} else {
					chunk.MuLaw = resampled
				}
			}
			out <- chunk
		}
	}()
	return out
}
