package media

import (
	"context"
	"log/slog"
	"strconv"
	"strings"
)

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

// ElevenLabsPCMFormatForRate picks the closest ElevenLabs streaming PCM format for a target Hz.
func ElevenLabsPCMFormatForRate(hz int) string {
	switch {
	case hz <= 0:
		return "pcm_24000"
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
	case "pcm_16000":
		return 16000
	case "pcm_22050":
		return 22050
	case "pcm_24000":
		return 24000
	case "pcm_44100":
		return 44100
	case "ulaw_8000":
		return 8000
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
		stream = &resamplingTTSStream{
			inner:      stream,
			sourceRate: sourceRate,
			targetRate: targetRate,
			logger:     logger,
			streamSID:  session.StreamSID,
		}
	}
	return stream, nil
}

type resamplingTTSStream struct {
	inner                        TTSStream
	sourceRate, targetRate       int
	logger                       *slog.Logger
	streamSID                    string
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
