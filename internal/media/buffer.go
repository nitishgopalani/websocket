package media

import (
	"context"
	"errors"
	"log/slog"
)

// TranscodeSink decodes carrier audio into canonical PCM16, reframes to fixed durations,
// and forwards to the next sink in the chain.
//
// CT-1 drains each session's audio on a single worker goroutine, so per-session decoder
// and remainder buffer state on this sink does not require locking.
type TranscodeSink struct {
	next            AudioSink
	target          TargetFormat
	frameDurationMs int
	frameSizeBytes  int
	logger          *slog.Logger

	decoder   Decoder
	remainder []byte
}

// NewTranscodeSink wraps next with carrier-to-PCM16 transcoding and fixed-size reframing.
func NewTranscodeSink(next AudioSink, target TargetFormat, frameDurationMs int, logger *slog.Logger) *TranscodeSink {
	if next == nil {
		next = NewLoggingSink(logger)
	}
	if frameDurationMs <= 0 {
		frameDurationMs = defaultFrameDurationMs
	}
	if target.Channels <= 0 {
		target.Channels = 1
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &TranscodeSink{
		next:            next,
		target:          target,
		frameDurationMs: frameDurationMs,
		frameSizeBytes:  target.FrameSizeBytes(frameDurationMs),
		logger:          logger,
	}
}

func (t *TranscodeSink) OnStart(ctx context.Context, session *Session) error {
	decoder, err := NewDecoder(session.Format, t.target)
	if err != nil {
		t.logger.Warn("transcode decoder unavailable; audio will be skipped for session",
			"stream_sid", session.StreamSID,
			"encoding", session.Format.Encoding,
			"error", err,
		)
	} else {
		t.decoder = decoder
	}
	return t.next.OnStart(ctx, session)
}

func (t *TranscodeSink) OnAudio(ctx context.Context, session *Session, frame []byte) error {
	if t.decoder == nil {
		return nil
	}

	pcm, err := t.decoder.Decode(frame)
	if err != nil {
		if !errors.Is(err, ErrInvalidPCM16Length) {
			t.logger.Warn("transcode decode failed",
				"stream_sid", session.StreamSID,
				"error", err,
			)
		}
		return nil
	}

	t.remainder = append(t.remainder, pcm...)
	for len(t.remainder) >= t.frameSizeBytes {
		chunk := t.remainder[:t.frameSizeBytes]
		t.remainder = t.remainder[t.frameSizeBytes:]
		if err := t.next.OnAudio(ctx, session, chunk); err != nil {
			return err
		}
	}
	return nil
}

func (t *TranscodeSink) OnDTMF(ctx context.Context, session *Session, digit string) error {
	return t.next.OnDTMF(ctx, session, digit)
}

func (t *TranscodeSink) OnStop(ctx context.Context, session *Session) error {
	if len(t.remainder) > 0 && t.decoder != nil {
		tail := make([]byte, len(t.remainder))
		copy(tail, t.remainder)
		t.remainder = nil
		if err := t.next.OnAudio(ctx, session, tail); err != nil {
			return err
		}
	}
	t.decoder = nil
	t.remainder = nil
	return t.next.OnStop(ctx, session)
}
