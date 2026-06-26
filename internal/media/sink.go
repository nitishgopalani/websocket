package media

import (
	"context"
	"log/slog"
	"sync/atomic"
)

// AudioSink is the downstream seam for decoded inbound audio and stream lifecycle.
// CT-2/CT-3 implement real sinks behind this interface.
type AudioSink interface {
	OnStart(ctx context.Context, session *Session) error
	OnAudio(ctx context.Context, session *Session, frame []byte) error
	OnDTMF(ctx context.Context, session *Session, digit string) error
	OnStop(ctx context.Context, session *Session) error
}

// LoggingSink is the default CT-1 sink: lifecycle logs plus frame counting.
type LoggingSink struct {
	logger *slog.Logger
}

// NewLoggingSink returns a sink that logs stream lifecycle and counts audio frames.
func NewLoggingSink(logger *slog.Logger) *LoggingSink {
	if logger == nil {
		logger = slog.Default()
	}
	return &LoggingSink{logger: logger}
}

func (s *LoggingSink) OnStart(ctx context.Context, session *Session) error {
	s.logger.Info("stream started",
		"stream_sid", session.StreamSID,
		"call_sid", session.CallSID,
		"encoding", session.Format.Encoding,
		"sample_rate", session.Format.SampleRate,
		"channels", session.Format.Channels,
	)
	return nil
}

func (s *LoggingSink) OnAudio(ctx context.Context, session *Session, frame []byte) error {
	atomic.AddInt64(&session.framesDelivered, 1)
	s.logger.Debug("audio frame",
		"stream_sid", session.StreamSID,
		"bytes", len(frame),
		"frames_in", session.FramesIn,
	)
	return nil
}

func (s *LoggingSink) OnDTMF(ctx context.Context, session *Session, digit string) error {
	s.logger.Info("dtmf received",
		"stream_sid", session.StreamSID,
		"digit", digit,
	)
	return nil
}

func (s *LoggingSink) OnStop(ctx context.Context, session *Session) error {
	s.logger.Info("stream stopped",
		"stream_sid", session.StreamSID,
		"frames_in", session.FramesIn,
		"frames_delivered", session.framesDelivered,
		"frames_dropped", session.FramesDropped(),
	)
	return nil
}
